  // -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
  /***********************************************************************
 *
 * ir/client.cc:
 *   Inconsistent replication client
 *
 * Copyright 2013-2015 Dan R. K. Ports  <drkp@cs.washington.edu>
 *                     Irene Zhang Ports  <iyzhang@cs.washington.edu>
 *                2018 Adriana Szekeres <aaasz@cs.washington.edu>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/

#include "lib/assert.h"
#include "lib/message.h"
#include "replication/meerkatir/client.h"

#include <sys/time.h>
#include <math.h>

#include <random>

namespace replication {
namespace meerkatir {

using namespace std;

Client::Client(const transport::Configuration &config,
                   Transport *transport,
                   uint64_t clientid)
    : config(config),
      lastReqId(0),
      transport(transport) {

    this->clientid = clientid;
    // Randomly generate a client ID
    // This is surely not the fastest way to get a random 64-bit int,
    // but it should be fine for this purpose.
    while (this->clientid == 0) {
        std::random_device rd;
        std::mt19937_64 gen(rd());
        std::uniform_int_distribution<uint64_t> dis;
        this->clientid = dis(gen);
        Debug("Client ID: %lu", this->clientid);
    }

    transport->Register(this, -1);
}

Client::~Client()
{
}

// TODO: make this more general -- the replication layer must not do the app
// message serialization as well
void Client::InvokeInconsistent(uint64_t txn_nr,
                             uint8_t core_id,
                             bool commit,
                             inconsistent_continuation_t continuation,
                             error_continuation_t error_continuation) {
    // TODO: Use error_continuation.
    (void) error_continuation;

    // Bump the request ID
    uint64_t reqId = ++lastReqId;

    auto *reqBuf = reinterpret_cast<inconsistent_request_t *>(
      transport->GetRequestBuf(
        sizeof(inconsistent_request_t),
        sizeof(inconsistent_response_t)
      )
    );
    reqBuf->req_nr = reqId;
    reqBuf->txn_nr = txn_nr;
    reqBuf->client_id = clientid;
    reqBuf->commit = commit;
    transport->SendRequestToAll(this,
                                inconsistentReqType,
                                core_id,
                                sizeof(inconsistent_request_t));
}

void Client::InvokeConsensus(uint64_t txn_nr,
                          uint8_t core_id,
                          const Transaction &txn,
                          const Timestamp &timestamp,
                          decide_t decide,
                          consensus_continuation_t continuation,
                          error_continuation_t error_continuation) {
    uint64_t reqId = ++lastReqId;
    //auto timer = std::unique_ptr<Timeout>(new Timeout(
    //    transport, 500, [this, reqId]() { ResendConsensusRequest(reqId); }));
    //auto transition_to_slow_path_timer =
    //    std::unique_ptr<Timeout>(new Timeout(transport, 500, [this, reqId]() {
    //        // TODO: new way to deal with this
    //        //TransitionToConsensusSlowPath(reqId);
    //    }));

    crtConsensusReq =
        PendingConsensusRequest(reqId,
                                  txn_nr,
                                  core_id,
                                  continuation,
                                  //nullptr,
                                  //nullptr,
                                  //std::move(timer),
                                  //std::move(transition_to_slow_path_timer),
                                  decide,
                                  error_continuation);
    // TODO: how do we deal with timeouts? (do we need to patch eRPC?)
    //req->transition_to_slow_path_timer->Start();
    //SendConsensus(req);
    size_t txnLen = txn.getReadSet().size() * sizeof(read_t) +
                    txn.getWriteSet().size() * sizeof(write_t);
    size_t reqLen = sizeof(consensus_request_header_t) + txnLen;
    auto *reqBuf = reinterpret_cast<consensus_request_header_t *>(
      transport->GetRequestBuf(
        reqLen,
        sizeof(consensus_response_t)
      )
    );
    reqBuf->req_nr = reqId;
    reqBuf->txn_nr = txn_nr;
    reqBuf->id = timestamp.getID();
    reqBuf->timestamp = timestamp.getTimestamp();
    reqBuf->client_id = clientid;
    reqBuf->nr_reads = txn.getReadSet().size();
    reqBuf->nr_writes = txn.getWriteSet().size();

    txn.serialize(reinterpret_cast<char *>(reqBuf + 1));
    blocked = true;
    transport->SendRequestToAll(this,
                                consensusReqType,
                                core_id, reqLen);
}

void Client::InvokeUnlogged(uint64_t txn_nr,
                         uint8_t core_id,
                         int replicaIdx,
                         const string &request,
                         unlogged_continuation_t continuation,
                         error_continuation_t error_continuation,
                         uint32_t timeout) {
    uint64_t reqId = ++lastReqId;
    //auto timer = std::unique_ptr<Timeout>(new Timeout(
    //    transport, timeout,
    //    [this, reqId]() { UnloggedRequestTimeoutCallback(reqId); }));

    crtUnloggedReq =
        PendingUnloggedRequest(request,
                                 reqId,
                                 txn_nr,
                                 core_id,
                                 continuation,
                                 error_continuation);
                                 //nullptr,
                                 //std::move(timer));

    // TODO: find a way to get sending errors (the eRPC's enqueue_request
    // function does not return errors)
    // TODO: deal with timeouts?
    auto *reqBuf = reinterpret_cast<unlogged_request_t *>(
      transport->GetRequestBuf(
        sizeof(unlogged_request_t),
        sizeof(unlogged_response_t)
      )
    );
    reqBuf->req_nr = reqId;
    memcpy(reqBuf->key, request.c_str(), request.size());
    blocked = true;
    transport->SendRequestToReplica(this,
                                    unloggedReqType,
                                    replicaIdx, core_id,
                                    sizeof(unlogged_request_t));
}


// void IRClient::TransitionToConsensusSlowPath(const uint64_t reqId) {
//     Warning("Client timeout; taking consensus slow path: reqId=%lu", reqId);
//     PendingConsensusRequest *req =
//         dynamic_cast<PendingConsensusRequest *>(pendingReqs[reqId]);
//     ASSERT(req != NULL);
//     req->on_slow_path = true;

//     // We've already transitioned into the slow path, so don't transition into
//     // the slow-path again.
//     //ASSERT(req->transition_to_slow_path_timer);
//     //req->transition_to_slow_path_timer.reset();

//     // It's possible that we already have a quorum of responses (but not a
//     // super quorum).
//     const std::map<int, consensus_response_t> *quorum =
//         req->consensusReplyQuorum.CheckForQuorum();
//     if (quorum != nullptr) {
//         HandleSlowPathConsensus(reqId, *quorum, false, req);
//     }
// }

void Client::HandleSlowPathConsensus(const bool finalized_result_found) {
    ASSERT(finalized_result_found || consensusReplyQuorum.size() >= config.QuorumSize());

    // If a finalized result wasn't found, call decide to determine the
    // finalized result.
    if (!finalized_result_found) {
        uint64_t view = 0;
        boost::unordered_map<int, std::size_t> results;
        for (const auto &p : consensusReplyQuorum) {
            const consensus_response_t *r = &p.second;
            results[r->status] += 1;

            // All messages should have the same view.
            if (view == 0) {
                view = r->view;
            }
            ASSERT(r->view == view);
        }

        // Upcall into the application, and put the result in the request
        // to store for later retries.
        ASSERT(crtConsensusReq.decide != NULL);
        crtConsensusReq.decidedStatus = crtConsensusReq.decide(results);
        crtConsensusReq.reply_consensus_view = view;
    }

    // Set up a new timer for the finalize phase.
    //req->timer = std::unique_ptr<Timeout>(
    //    new Timeout(transport, 500, [this, req_nr]() {  //
    //        ResendFinalizeConsensusRequest(req_nr, true);
    //    }));

    // Send finalize message.
    auto *reqBuf = reinterpret_cast<finalize_consensus_request_t *>(
      transport->GetRequestBuf(
        sizeof(finalize_consensus_request_t),
        sizeof(finalize_consensus_response_t)
      )
    );
    reqBuf->req_nr = crtConsensusReq.req_nr;
    reqBuf->client_id = clientid;
    reqBuf->status = crtConsensusReq.decidedStatus;
    reqBuf->txn_nr = crtConsensusReq.txn_nr;

    crtConsensusReq.sent_confirms = true;
    //req->timer->Start();
    transport->SendRequestToAll(this,
                                finalizeConsensusReqType,
                                crtConsensusReq.core_id,
                                sizeof(finalize_consensus_request_t));
}

void Client::HandleFastPathConsensus() {
    ASSERT(consensusReplyQuorum.size() >= config.FastQuorumSize());
    Debug("Handling fast path for request %lu.", crtConsensusReq.req_nr);

    // We've received a super quorum of responses. Now, we have to check to see
    // if we have a super quorum of _matching_ responses.
    boost::unordered_map<int, std::size_t> results;
    for (const auto &m : consensusReplyQuorum) {
        const int result = m.second.status;
        results[result]++;
    }

    for (const auto &result : results) {
        if (result.second < config.FastQuorumSize()) {
            continue;
        }

        // A super quorum of matching requests was found!
        Debug("A super quorum of matching requests was found for request %lu.",
              crtConsensusReq.req_nr);
        crtConsensusReq.decidedStatus = result.first;

        // Stop the transition to slow path timer
        //req->transition_to_slow_path_timer->Stop();

        // aaasz: we don't need to send finalize consensus on fast path anymore;
        // the client will immediately send the inconsistent request to commit/abort

        // Return to the client.
        if (!crtConsensusReq.continuationInvoked) {
            crtConsensusReq.consensus_continuation(crtConsensusReq.decidedStatus);
            crtConsensusReq.continuationInvoked = true;
        }

        blocked = false;
        consensusReplyQuorum.clear();
        crtConsensusReq.req_nr = 0;
        return;
    }

    // There was not a super quorum of matching results, so we transition into
    // the slow path.
    Debug("A super quorum of matching requests was NOT found for request %lu.",
          crtConsensusReq.req_nr);
    crtConsensusReq.on_slow_path = true;
    //if (req->transition_to_slow_path_timer) {
    //    req->transition_to_slow_path_timer.reset();
    //}
    HandleSlowPathConsensus(false);
}

void Client::ReceiveResponse(uint8_t reqType, char *respBuf) {
    Debug("[%lu] received response", clientid);
    switch(reqType){
        case unloggedReqType:
            HandleUnloggedReply(respBuf);
            break;
        case inconsistentReqType:
            HandleInconsistentReply(respBuf);
            break;
        case consensusReqType:
            HandleConsensusReply(respBuf);
            break;
        case finalizeConsensusReqType:
            HandleFinalizeConsensusReply(respBuf);
            break;
        default:
            Warning("Unrecognized request type: %d\n", reqType);
    }
}

void Client::HandleUnloggedReply(char *respBuf) {
    auto *resp = reinterpret_cast<unlogged_response_t *>(respBuf);
    if (resp->req_nr != crtUnloggedReq.req_nr) {
        Warning("Received unlogged reply when no request was pending; req_nr = %lu", resp->req_nr);
        return;
    }

    Debug("[%lu] Received unlogged reply", clientid);

    // delete timer event
    //req->timer->Stop();
    // invoke application callback
    crtUnloggedReq.get_continuation(respBuf);
    // remove from pending list
    blocked = false;
    crtUnloggedReq.req_nr = 0;
}

void Client::HandleInconsistentReply(char *respBuf) {
    // auto *resp = reinterpret_cast<inconsistent_response_t *>(respBuf);
    // if (lastReqId == resp->req_nr)
}

void Client::HandleConsensusReply(char *respBuf) {
    auto *resp = reinterpret_cast<consensus_response_t *>(respBuf);

    Debug(
        "Client received ReplyConsensusMessage from replica %lu in view %lu for "
        "request %lu.",
        resp->replicaid, resp->view, resp->req_nr);

    if (resp->req_nr != crtConsensusReq.req_nr) {
        Warning(
            "Client was not expecting a ReplyConsensusMessage for request %lu, "
            "so it is ignoring the request.",
            resp->req_nr);
        return;
    }

    //ASSERT(req != nullptr);

    if (crtConsensusReq.sent_confirms) {
        Debug(
            "Client has already received a quorum or super quorum of "
            "HandleConsensusReply for request %lu and has already sent out "
            "ConfirmMessages.",
            resp->req_nr);
        return;
    }

    // save the response
    //req->consensusReplyQuorum.Add(resp->view, resp->replicaid, *resp);
    // TODO: check view number
    consensusReplyQuorum[resp->replicaid] = *resp;

    if (resp->finalized) {
        Debug("The HandleConsensusReply for request %lu was finalized.", resp->req_nr);
        // If we receive a finalized message, then we immediately transition
        // into the slow path.
        crtConsensusReq.on_slow_path = true;
        //if (req->transition_to_slow_path_timer) {
        //    req->transition_to_slow_path_timer.reset();
        //}

        crtConsensusReq.decidedStatus = resp->status;
        crtConsensusReq.reply_consensus_view = resp->view;
        // TODO: what if finalize in a different view?
        HandleSlowPathConsensus(true);
    } else if (crtConsensusReq.on_slow_path && consensusReplyQuorum.size() >= config.QuorumSize()) {
        HandleSlowPathConsensus(false);
    } else if (!crtConsensusReq.on_slow_path && consensusReplyQuorum.size() >= config.FastQuorumSize()) {
        HandleFastPathConsensus();
    }
}

void Client::HandleFinalizeConsensusReply(char *respBuf) {
    auto *resp = reinterpret_cast<finalize_consensus_response_t *>(respBuf);
    if (resp->req_nr != crtConsensusReq.req_nr) {
        Debug(
            "We received a FinalizeConsensusReply for operation %lu, but we weren't "
            "waiting for any FinalizeConsensusReply. We are ignoring the message.",
            resp->req_nr);
        return;
    }

    Debug(
        "Client received FinalizeConsensusReply from replica %lu in view %lu for "
        "request %lu.",
        resp->replicaid, resp->view, resp->req_nr);

    // TODO: check view
    finalizeReplyQuorum[resp->replicaid] = *resp;
    if (finalizeReplyQuorum.size() >= config.QuorumSize()) {
        //req->timer->Stop();
        if (!crtConsensusReq.continuationInvoked) {
            // Return to the client.
            if (resp->view == crtConsensusReq.reply_consensus_view) {
                crtConsensusReq.consensus_continuation(crtConsensusReq.decidedStatus);
            } else {
                Debug(
                    "We received a majority of ConfirmMessages for request %lu "
                    "with view %lu, but the view from ReplyConsensusMessages "
                    "was %lu.",
                    resp->req_nr, resp->view, crtConsensusReq.reply_consensus_view);
                if (crtConsensusReq.error_continuation) {
                    crtConsensusReq.error_continuation(
                        crtConsensusReq.request, ErrorCode::MISMATCHED_CONSENSUS_VIEWS);
                }
            }
        }
        blocked = false;
        finalizeReplyQuorum.clear();
        consensusReplyQuorum.clear();
        crtConsensusReq.req_nr = 0;
    }
}

} // namespace ir
} // namespace replication
