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
#include "replication/ir/client.h"

#include <sys/time.h>
#include <math.h>

#include <random>

namespace replication {
namespace ir {

using namespace std;

IRClient::IRClient(const transport::Configuration &config,
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
        Debug("IRClient ID: %lu", this->clientid);
    }

    transport->Register(this, -1);
}

IRClient::~IRClient()
{
    for (auto kv : pendingReqs) {
	delete kv.second;
    }
}

// TODO: make this more general -- the replication layer must not do the app
// message serialization as well
void IRClient::InvokeInconsistent(uint64_t txn_nr,
                             uint32_t core_id,
                             bool commit,
                             inconsistent_continuation_t continuation,
                             error_continuation_t error_continuation) {
    // TODO: Use error_continuation.
    (void) error_continuation;

    // Bump the request ID
    uint64_t reqId = ++lastReqId;

    auto *reqBuf = reinterpret_cast<inconsistent_request_t *>(transport->GetRequestBuf());
    reqBuf->req_nr = reqId;
    reqBuf->txn_nr = txn_nr;
    reqBuf->client_id = clientid;
    reqBuf->commit = commit;

    transport->SendRequestToAll(inconsistentReqType, sizeof(inconsistent_request_t), false);
}

void
IRClient::InvokeConsensus(uint64_t txn_nr,
                          uint32_t core_id,
                          const Transaction &txn,
                          const Timestamp &timestamp,
                          decide_t decide,
                          consensus_continuation_t continuation,
                          error_continuation_t error_continuation) {
    uint64_t reqId = ++lastReqId;
    auto timer = std::unique_ptr<Timeout>(new Timeout(
        transport, 500, [this, reqId]() { ResendConsensusRequest(reqId); }));
    auto transition_to_slow_path_timer =
        std::unique_ptr<Timeout>(new Timeout(transport, 500, [this, reqId]() {
            TransitionToConsensusSlowPath(reqId);
        }));

    PendingConsensusRequest *req =
      new PendingConsensusRequest(reqId,
                                  txn_nr,
                                  core_id,
                                  continuation,
                                  std::move(timer),
                                  std::move(transition_to_slow_path_timer),
                                  config.QuorumSize(),
                                  config.FastQuorumSize(),
                                  decide,
                                  error_continuation);
    pendingReqs[reqId] = req;
    // TODO: how do we deal with timeouts? (do we need to patch eRPC?)
    req->transition_to_slow_path_timer->Start();
    //SendConsensus(req);
    auto *reqBuf = reinterpret_cast<consensus_request_header_t *>(transport->GetRequestBuf());
    reqBuf->req_nr = reqId;
    reqBuf->txn_nr = txn_nr;
    reqBuf->id = timestamp.getID();
    reqBuf->timestamp = timestamp.getTimestamp();
    reqBuf->client_id = clientid;
    reqBuf->nr_reads = txn.getReadSet().size();
    reqBuf->nr_writes = txn.getWriteSet().size();

    txn.serialize(reinterpret_cast<char *>(reqBuf + 1));

    transport->SendRequestToAll(consensusReqType, sizeof(consensus_request_header_t) +
                                                    reqBuf->nr_reads * sizeof(read_t) +
                                                    reqBuf->nr_writes * sizeof(write_t), true);
}

void IRClient::InvokeUnlogged(uint64_t txn_nr,
                         uint32_t core_id,
                         int replicaIdx,
                         const string &request,
                         unlogged_continuation_t continuation,
                         error_continuation_t error_continuation,
                         uint32_t timeout) {
    uint64_t reqId = ++lastReqId;
    auto timer = std::unique_ptr<Timeout>(new Timeout(
        transport, timeout,
        [this, reqId]() { UnloggedRequestTimeoutCallback(reqId); }));

    PendingUnloggedRequest *req =
      new PendingUnloggedRequest(request,
                                 reqId,
                                 txn_nr,
                                 core_id,
                                 continuation,
                                 error_continuation,
                                 std::move(timer));

    // TODO: find a way to get sending errors (the eRPC's enqueue_request
    // function does not return errors)
    // TODO: deal with timeouts?
    pendingReqs[reqId] = req;
    auto *reqBuf = reinterpret_cast<unlogged_request_t *>(transport->GetRequestBuf());
    reqBuf->req_nr = reqId;
    memcpy(reqBuf->key, request.c_str(), request.size());
    transport->SendRequestToReplica(unloggedReqType, replicaIdx, sizeof(unlogged_request_t), true);
}

void IRClient::ResendConsensusRequest(const uint64_t reqId) {
    Warning("Client timeout; resending consensus request: %lu", reqId);
    auto *reqBuf = reinterpret_cast<consensus_request_header_t *>(transport->GetRequestBuf());
    // reqBuf must already have all field filled in
        // TODO: send to all replicas
    transport->SendRequestToReplica(consensusReqType, 0, sizeof(consensus_request_header_t) +
                                                    reqBuf->nr_reads * sizeof(read_t) +
                                                    reqBuf->nr_writes * sizeof(write_t), true);
}

void IRClient::TransitionToConsensusSlowPath(const uint64_t reqId) {
    Warning("Client timeout; taking consensus slow path: reqId=%lu", reqId);
    PendingConsensusRequest *req =
        dynamic_cast<PendingConsensusRequest *>(pendingReqs[reqId]);
    ASSERT(req != NULL);
    req->on_slow_path = true;

    // We've already transitioned into the slow path, so don't transition into
    // the slow-path again.
    ASSERT(req->transition_to_slow_path_timer);
    req->transition_to_slow_path_timer.reset();

    // It's possible that we already have a quorum of responses (but not a
    // super quorum).
    const std::map<int, consensus_response_t> *quorum =
        req->consensusReplyQuorum.CheckForQuorum();
    if (quorum != nullptr) {
        HandleSlowPathConsensus(reqId, *quorum, false, req);
    }
}

void IRClient::HandleSlowPathConsensus(
            const uint64_t req_nr,
            const std::map<int, consensus_response_t> &msgs,
            const bool finalized_result_found,
            PendingConsensusRequest *req) {
    ASSERT(finalized_result_found || msgs.size() >= req->quorumSize);
    Debug("Handling slow path for request %lu.", req_nr);

    // If a finalized result wasn't found, call decide to determine the
    // finalized result.
    if (!finalized_result_found) {
        uint64_t view = 0;
        std::map<int, std::size_t> results;
        for (const auto &p : msgs) {
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
        ASSERT(req->decide != NULL);
        req->decidedStatus = req->decide(results);
        req->reply_consensus_view = view;
    }

    // Set up a new timer for the finalize phase.
    req->timer = std::unique_ptr<Timeout>(
        new Timeout(transport, 500, [this, req_nr]() {  //
            ResendFinalizeConsensusRequest(req_nr, true);
        }));

    // Send finalize message.
    auto *reqBuf = reinterpret_cast<finalize_consensus_request_t *>(transport->GetRequestBuf());
    reqBuf->req_nr = req_nr;
    reqBuf->client_id = clientid;
    reqBuf->status = req->decidedStatus;
    reqBuf->txn_nr = req->clienttxn_nr;

    transport->SendRequestToReplica(finalizeConsensusReqType, 0, sizeof(finalize_consensus_request_t), true);
    req->sent_confirms = true;
    req->timer->Start();
}

void IRClient::HandleFastPathConsensus(
            const uint64_t req_nr,
            const std::map<int, consensus_response_t> &msgs,
            PendingConsensusRequest *req,
            bool &unblock) {
    ASSERT(msgs.size() >= req->superQuorumSize);
    Debug("Handling fast path for request %lu.", req_nr);

    // We've received a super quorum of responses. Now, we have to check to see
    // if we have a super quorum of _matching_ responses.
    std::map<int, std::size_t> results;
    for (const auto &m : msgs) {
        const int result = m.second.status;
        results[result]++;
    }

    for (const auto &result : results) {
        if (result.second < req->superQuorumSize) {
            continue;
        }

        // A super quorum of matching requests was found!
        Debug("A super quorum of matching requests was found for request %lu.",
              req_nr);
        req->decidedStatus = result.first;

        // Stop the transition to slow path timer
        req->transition_to_slow_path_timer->Stop();

        // aaasz: we don't need to send finalize consensus on fast path anymore;
        // the client will immediately send the inconsistent request to commit/abort

        // Return to the client.
        if (!req->continuationInvoked) {
            req->consensus_continuation(req->decidedStatus);
            req->continuationInvoked = true;
            unblock = true;
        }
        delete req;
        return;
    }

    // There was not a super quorum of matching results, so we transition into
    // the slow path.
    Debug("A super quorum of matching requests was NOT found for request %lu.",
          req_nr);
    req->on_slow_path = true;
    if (req->transition_to_slow_path_timer) {
        req->transition_to_slow_path_timer.reset();
    }
    HandleSlowPathConsensus(req_nr, msgs, false, req);
}

void IRClient::ResendFinalizeConsensusRequest(const uint64_t req_nr, bool isConsensus) {
    if (pendingReqs.find(req_nr) == pendingReqs.end()) {
        Warning("Received resend request when no request was pending");
        return;
    }

    if (isConsensus) {
        Warning("Client timeout; resending finalize consensus request: %lu", req_nr);

        PendingConsensusRequest *req = static_cast<PendingConsensusRequest *>(pendingReqs[req_nr]);
        ASSERT(req != NULL);

        // TODO: the reqBuf should already have the necessary information
        // (of the last request)
        // TODO: send to all
        transport->SendRequestToReplica(finalizeConsensusReqType, 0, sizeof(finalize_consensus_request_t), true);
        req->timer->Reset();
    } else {
    	// aaasz: We don't need this anymore -- we only finalize consensus operations
	    Panic("Not implemented!");
    }
}

// TOOD: for now, just the GETs go through this
void IRClient::ReceiveResponse(uint8_t reqType, char *respBuf, bool &unblock) {
    switch(reqType){
        case unloggedReqType:
            HandleUnloggedReply(respBuf, unblock);
            break;
        case inconsistentReqType:
            HandleInconsistentReply(respBuf, unblock);
            break;
        case consensusReqType:
            HandleConsensusReply(respBuf, unblock);
            break;
        case finalizeConsensusReqType:
            HandleFinalizeConsensusReply(respBuf, unblock);
            break;
        default:
            Warning("Unrecognized request type: %d\n", reqType);
    }
}

void IRClient::HandleUnloggedReply(char *respBuf, bool &unblock) {
    auto *resp = reinterpret_cast<unlogged_response_t *>(respBuf);
    auto it = pendingReqs.find(resp->req_nr);
    if (it == pendingReqs.end()) {
        Warning("Received unlogged reply when no request was pending; req_nr = %lu", resp->req_nr);
        return;
    }
    PendingUnloggedRequest *req = (PendingUnloggedRequest *)it->second;
    // delete timer event
    req->timer->Stop();
    // remove from pending list
    pendingReqs.erase(it);
    // invoke application callback
    req->get_continuation(respBuf);
    delete req;
    unblock = true;
}

void IRClient::HandleInconsistentReply(char *respBuf, bool &unblock) {
    // just check if we need to unblock
    // auto *resp = reinterpret_cast<inconsistent_response_t *>(respBuf);
    // if (lastReqId == resp->req_nr)
    //     unblock = true;
}

void IRClient::HandleConsensusReply(char *respBuf, bool &unblock) {
    auto *resp = reinterpret_cast<consensus_response_t *>(respBuf);

    Debug(
        "Client received ReplyConsensusMessage from replica %i in view %lu for "
        "request %lu.",
        resp->replicaid, resp->view, resp->req_nr);

    auto it = pendingReqs.find(resp->req_nr);
    if (it == pendingReqs.end()) {
        Debug(
            "Client was not expecting a ReplyConsensusMessage for request %lu, "
            "so it is ignoring the request.",
            resp->req_nr);
        return;
    }

    PendingConsensusRequest *req =
        dynamic_cast<PendingConsensusRequest *>(it->second);
    ASSERT(req != nullptr);

    if (req->sent_confirms) {
        Debug(
            "Client has already received a quorum or super quorum of "
            "HandleConsensusReply for request %lu and has already sent out "
            "ConfirmMessages.",
            resp->req_nr);
        return;
    }

    // save the response
    req->consensusReplyQuorum.Add(resp->view, resp->replicaid, *resp);
    const std::map<int, consensus_response_t> &msgs =
        req->consensusReplyQuorum.GetMessages(resp->view);

    if (resp->finalized) {
        Debug("The HandleConsensusReply for request %lu was finalized.", resp->req_nr);
        // If we receive a finalized message, then we immediately transition
        // into the slow path.
        req->on_slow_path = true;
        if (req->transition_to_slow_path_timer) {
            req->transition_to_slow_path_timer.reset();
        }

        req->decidedStatus = resp->status;
        req->reply_consensus_view = resp->view;
        // TODO: what if finalize in a different view?
        HandleSlowPathConsensus(resp->req_nr, msgs, true, req);
    } else if (req->on_slow_path && msgs.size() >= req->quorumSize) {
        HandleSlowPathConsensus(resp->req_nr, msgs, false, req);
    } else if (!req->on_slow_path && msgs.size() >= req->superQuorumSize) {
        HandleFastPathConsensus(resp->req_nr, msgs, req, unblock);
    }
}

void IRClient::HandleFinalizeConsensusReply(char *respBuf, bool &unblock) {
    auto *resp = reinterpret_cast<finalize_consensus_response_t *>(respBuf);
    auto it = pendingReqs.find(resp->req_nr);
    if (it == pendingReqs.end()) {
        Debug(
            "We received a ConfirmMessage for operation %lu, but we weren't "
            "waiting for any ConfirmMessages. We are ignoring the message.",
            resp->req_nr);
        return;
    }

    Debug(
        "Client received ConfirmMessage from replica %i in view %lu for "
        "request %lu.",
        resp->replicaid, resp->view, resp->req_nr);

    PendingRequest *req = it->second;

    viewstamp_t vs = { resp->view, resp->req_nr };
    if (req->confirmQuorum.AddAndCheckForQuorum(vs, resp->replicaid, *resp)) {
        req->timer->Stop();
        pendingReqs.erase(it);
        if (!req->continuationInvoked) {
            // Return to the client. ConfirmMessages are sent by replicas in
            // response to FinalizeInconsistentMessages and
            // FinalizeConsensusMessage, but inconsistent operations are
            // invoked before FinalizeInconsistentMessages are ever sent. Thus,
            // req->continuationInvoked can only be false if req is a
            // PendingConsensusRequest, so it's safe to cast it here.
            PendingConsensusRequest *r2 =
                dynamic_cast<PendingConsensusRequest *>(req);
            ASSERT(r2 != nullptr);
            if (vs.view == r2->reply_consensus_view) {
                r2->consensus_continuation(r2->decidedStatus);
            } else {
                Debug(
                    "We received a majority of ConfirmMessages for request %lu "
                    "with view %lu, but the view from ReplyConsensusMessages "
                    "was %lu.",
                    resp->req_nr, vs.view, r2->reply_consensus_view);
                if (r2->error_continuation) {
                    r2->error_continuation(
                        r2->request, ErrorCode::MISMATCHED_CONSENSUS_VIEWS);
                }
            }
        }
        delete req;
    }
}

void IRClient::UnloggedRequestTimeoutCallback(const uint64_t req_nr) {
    auto it = pendingReqs.find(req_nr);
    if (it == pendingReqs.end()) {
        Warning("Received unlogged request timeout when no request was pending");
        return;
    }

    PendingUnloggedRequest *req = static_cast<PendingUnloggedRequest *>(it->second);
    ASSERT(req != NULL);

    Warning("Unlogged request timed out: %lu", req_nr);

    // delete timer event
    req->timer->Stop();
    // remove from pending list
    pendingReqs.erase(it);
    // invoke application callback
    if (req->error_continuation) {
        req->error_continuation(req->request, ErrorCode::TIMEOUT);
    }
    delete req;
}

} // namespace ir
} // namespace replication
