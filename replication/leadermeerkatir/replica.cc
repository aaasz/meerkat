// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * leadermeerkatir/replica.cc:
 *   Leader based inconsistent replication
 *
 * Copyright 2013 Dan R. K. Ports  <drkp@cs.washington.edu>
 *           2019 Adriana Szekeres <aaasz@cs.washington.edu>
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

#include "replication/leadermeerkatir/replica.h"
#include "store/common/transaction.h"

#include <sys/time.h>
#include <algorithm>
#include <thread>

#define RDebug(fmt, ...) Debug("[%d] " fmt, myIdx, ##__VA_ARGS__)
#define RNotice(fmt, ...) Notice("[%d] " fmt, myIdx, ##__VA_ARGS__)
#define RWarning(fmt, ...) Warning("[%d] " fmt, myIdx, ##__VA_ARGS__)
#define RPanic(fmt, ...) Panic("[%d] " fmt, myIdx, ##__VA_ARGS__)

namespace replication {
namespace leadermeerkatir {

using namespace std;

IRReplica::IRReplica(transport::Configuration config, int myIdx,
                     Transport *transport, //unsigned int batchSize,
                     IRAppReplica *app)
    : config(std::move(config)), myIdx(myIdx), transport(transport), app(app)
{
    this->status = STATUS_NORMAL;
    this->view = 0;
    prepareResponseQuorum.SetNumRequired(config.QuorumSize() - 1);
    transport->Register(this, myIdx);

    if (AmLeader()) {
        // start transport sessions to the other replicas
        for (int i = 0; i < config.n; i++) {
            // skip the leader
            if (this->myIdx == i) continue;
            // open session to the same rpcIdx as our transport's,
            // consequently, the same core
            transport->GetSession(this, i, transport->GetID());
        }
    }
}

IRReplica::~IRReplica() {}

bool IRReplica::AmLeader() const {
    return (config.GetLeaderIndex(view) == myIdx);
}

void IRReplica::ReceiveRequest(uint64_t reqHandleIdx, uint8_t reqType, char *reqBuf, char *respBuf) {
    switch(reqType) {
        case clientReqType:
            HandleRequest(reqHandleIdx, reqBuf, respBuf);
            break;
        case unloggedReqType:
            HandleUnloggedRequest(reqHandleIdx, reqBuf, respBuf);
            break;
        case prepareReqType:
            HandlePrepare(reqHandleIdx, reqBuf, respBuf);
            break;
        case commitReqType:
            HandleCommit(reqHandleIdx, reqBuf, respBuf);
            break;
        default:
            Warning("Unrecognized rquest type: %d", reqType);
    }
}

void IRReplica::ReceiveResponse(uint8_t reqType, char *respBuf) {
    switch(reqType){
        case prepareReqType:
            HandlePrepareReply(respBuf);
            break;
        case commitReqType:
            HandleCommitReply(respBuf);
            break;
        default:
            Warning("Unrecognized request type: %d\n", reqType);
    }
}

void IRReplica::HandleRequest(uint64_t reqHandleIdx, char *reqBuf, char *respBuf) {
    viewstamp_t v;

    if (status != STATUS_NORMAL) {
        RNotice("Ignoring request due to abnormal status");
        return;
    }

    if (!AmLeader()) {
        RDebug("Ignoring request because I'm not the leader");
        return;
    }

    auto *req = reinterpret_cast<request_header_t *>(reqBuf);

    Debug("[%lu - %lu] Received Request, req_nr =  %lu",
        req->client_id, req->txn_nr, req->req_nr);

    txnid_t txnid = std::make_pair(req->client_id, req->txn_nr);

    // Check record if we've already handled this request
   RecordEntry *entry = record.Find(txnid);
   if (entry == NULL) {
       // It's the first prepare request we've seen for this transaction,
       // save it as tentative, in initial view, with initial status
       entry = &record.Add(0, txnid, req->req_nr, NOT_PREPARED,
                  RECORD_STATE_TENTATIVE, "");
   }

    // Leader Upcall
    bool replicate = false;
    // note: LeaderUpcall saves the transaction and timestamp in the entry
    app->LeaderUpcall(txnid, entry, reqBuf, replicate);

    // Check whether this request should be committed to replicas
    if (!replicate) {
        RDebug("Executing request failed. Not committing to replicas");
        auto *resp = reinterpret_cast<request_response_t *>(respBuf);
        resp->req_nr = req->req_nr;
        resp->view = this->view;
        resp->status = entry->txn_status == PREPARED_OK ? REPLY_OK : REPLY_FAIL;

        // ClientTableEntry &cte = clientTable[req->client_id];
        // cte.replied = true;
        // cte.reply = reply;
        transport->SendResponse(reqHandleIdx, sizeof(request_response_t));
    } else {
        // TODO: we can't commit immediately at the leader

        if (prepareResponseQuorum.NumRequired() == 0) {
            // No other replicas
        	RDebug("No other replicas");

            // Execute the operation at the leader
            app->LeaderUpcallPostPrepare(txnid, entry);

            // Send reply to client
            Debug("Sending reply to client for req_nr = %lu", req->req_nr);
            auto *resp = reinterpret_cast<request_response_t *>(respBuf);
            resp->req_nr = req->req_nr;
            resp->view = this->view;
            resp->status = entry->txn_status == PREPARED_OK ? REPLY_OK : REPLY_FAIL;
            transport->SendResponse(reqHandleIdx, sizeof(request_response_t));
        } else {
            // Save the request handle index and the response buffer
            entry->reqHandleIdx = reqHandleIdx;
            entry->respBuf = respBuf;

            // Send prepare record to the other replicas
            size_t txnLen = req->nr_reads * sizeof(read_t) + req->nr_writes * sizeof(write_t);
            size_t reqLen = sizeof(prepare_request_header_t) + txnLen;
            auto *prepareReq = reinterpret_cast<prepare_request_header_t *>(
              transport->GetRequestBuf(
                reqLen,
                sizeof(prepare_response_t)
              )
            );
            prepareReq->request_header = *req;
            prepareReq->view = this->view;
            prepareReq->timestamp = entry->ts.getTimestamp();
            prepareReq->id = entry->ts.getID();
            memcpy(prepareReq + 1, req + 1, txnLen);
            transport->SendRequestToAll(this, prepareReqType, transport->GetID(), reqLen);
        }
    }
}

void IRReplica::HandleUnloggedRequest(uint64_t reqHandleIdx, char *reqBuf, char *respBuf) {
    // TODO: Ignore requests from the past
    size_t respLen;
    app->UnloggedUpcall(reqBuf, respBuf, respLen);
    transport->SendResponse(reqHandleIdx, respLen);
}

void IRReplica::HandlePrepare(uint64_t reqHandleIdx, char *reqBuf, char *respBuf) {
    auto *req = reinterpret_cast<prepare_request_header_t *>(reqBuf);

    RDebug("[%lu - %lu] Received prepare clientreq_nr %lu:\n",
          req->request_header.client_id, req->request_header.txn_nr, req->request_header.req_nr);

    if (this->status != STATUS_NORMAL) {
        RDebug("Ignoring PREPARE due to abnormal status");
        return;
    }

    if (req->view < this->view) {
        RDebug("Ignoring PREPARE due to stale view");
        return;
    }
//
//    if (msg.view() > this->view) {
//        RequestStateTransfer();
//        pendingPrepares.push_back(std::pair<TransportAddress *, PrepareMessage>(remote.clone(), msg));
//        return;
//    }
//
    if (AmLeader()) {
        RPanic("Unexpected PREPARE: I'm the leader of this view");
    }
    
    txnid_t txnid = std::make_pair(req->request_header.client_id, req->request_header.txn_nr);

    // Check record if we've already handled this prepare
    RecordEntry *entry = record.Find(txnid);
    if (entry == NULL) {
        // It's the first prepare request we've seen for this transaction,
        // save it as tentative, in initial view, with initial status
        entry = &record.Add(0, txnid, req->request_header.req_nr, PREPARED_OK, // TODO: set txn status from application
                            RECORD_STATE_TENTATIVE, "");
        // Save the transaction
        entry->txn = Transaction(req->request_header.nr_reads, req->request_header.nr_writes, (char *)(req + 1));
        entry->ts = Timestamp(req->timestamp, req->id);
    }

    /* Build reply and send it to the leader */
    auto *resp = reinterpret_cast<prepare_response_t *>(respBuf);
    resp->client_id = req->request_header.client_id;
    resp->req_nr = req->request_header.req_nr;
    resp->txn_nr = req->request_header.txn_nr;
    resp->view = this->view;
    resp->replica_idx = this->myIdx;
    transport->SendResponse(reqHandleIdx, sizeof(prepare_response_t));
}

void IRReplica::HandleCommit(uint64_t reqHandleIdx, char *reqBuf, char *respBuf) {
    auto *req = reinterpret_cast<commit_request_t *>(reqBuf);

    RDebug("Received COMMIT " FMT_VIEWSTAMP, req->view, req->txn_nr);

   if (this->status != STATUS_NORMAL) {
       RDebug("Ignoring COMMIT due to abnormal status");
       return;
   }

   if (req->view < this->view) {
       RDebug("Ignoring COMMIT due to stale view");
       return;
   }

//    if (msg.view() > this->view) {
//        RequestStateTransfer();
//        return;
//    }

    if (AmLeader()) {
        RPanic("Unexpected COMMIT: I'm the leader of this view");
    }

//    viewChangeTimeout->Reset();

    txnid_t txnid = std::make_pair(req->client_id, req->txn_nr);
    RecordEntry *entry = record.Find(txnid);
    if (entry == NULL) {
        // TODO: this happens if messages are being dropped
        // (I've seen it happening even in our cluster but very very rarely,
        // but don't Panic in those cases cause it stops the experiment batch)
        // TODO: get transaction from a different replica
        RWarning("Received commit but replica does not have the entry.");
        return;
    }

    string res;
    app->ReplicaUpcall(txnid, entry);

    // send response to the leader
    // TODO: this should not be necessary but eRPC expects a response to every request
    // (otherwise it will try to re-send the request)
    auto *resp = reinterpret_cast<commit_response_t *>(respBuf);
    resp->req_nr = req->txn_nr;
    transport->SendResponse(reqHandleIdx, sizeof(commit_response_t));

    // TODO: for now just trim the log here, as soon as we know the transaction was acked at a majority
    // Not entirely safe
    record.Remove(txnid);
}

// TODO: The check for a quorum happens only after the first
// prepareOK is received. This breaks the case when we have
// just 1 replica. Yes, there's no point in starting with less than
// 3 replicas, except for testing reasons.
void IRReplica::HandlePrepareReply(char *respBuf) {
    auto *resp = reinterpret_cast<prepare_response_t *>(respBuf);

    RDebug("[%lu - %lu] Received prepareResponse \n",
          resp->client_id, resp->txn_nr);

    if (this->status != STATUS_NORMAL) {
        RDebug("Ignoring PREPAREOK due to abnormal status");
        return;
    }

    if (resp->view < this->view) {
        RDebug("Ignoring PREPAREOK due to stale view");
        return;
    }

//    if (msg.view() > this->view) {
//        RequestStateTransfer();
//        return;
//    }

    if (!AmLeader()) {
        RWarning("Ignoring PREPAREOK because I'm not the leader");
        return;        
    }

    txnid_t txnid = std::make_pair(resp->client_id, resp->txn_nr);
    // TODO: viewstamp must contain txn id
    viewstamp_t vs = { resp->view, txnid };
    if (auto msgs =
        (prepareResponseQuorum.AddAndCheckForQuorum(vs, resp->replica_idx, *resp))) {

        if (msgs->size() >= (unsigned int)config.QuorumSize()) {
            // TODO: clear this if we received replies from all replicas
            if (msgs->size() == (unsigned int)config.n - 1) {
                prepareResponseQuorum.Clear(vs);
            }
            RDebug("Already enough messages\n ");
            return;
        }

    	/*
         * We have a quorum of PrepareOK messages for this
         * transaction.
         *
         * (Note that we might have already executed it. That's fine,
         * we just won't do anything.)
         *
         * We also notify the client of the result.
         */
        RecordEntry *entry = record.Find(txnid);
        if (entry == NULL) {
            RWarning("Received prepare OKs but leader does not have the entry.");
            return;
        }

        app->LeaderUpcallPostPrepare(txnid, entry);

        /*
         * Send COMMIT message to the other replicas.
         *
         * This can be done asynchronously, so it really ought to be
         * piggybacked on the next PREPARE or something.
         */
        auto *commitReq = reinterpret_cast<commit_request_t *>(
          transport->GetRequestBuf(
            sizeof(commit_request_t),
            sizeof(commit_response_t)
          )
        );
        commitReq->client_id = resp->client_id;
        commitReq->txn_nr = resp->txn_nr;
        commitReq->view = this->view;
        transport->SendRequestToAll(this, commitReqType, transport->GetID(), sizeof(commit_request_t));

        /* Send reply to client */
        auto *clientResp = reinterpret_cast<request_response_t *>(entry->respBuf);
        clientResp->req_nr = resp->req_nr;
        clientResp->view = this->view;
        clientResp->status =  entry->txn_status == PREPARED_OK ? REPLY_OK : REPLY_FAIL;
        transport->SendResponse(entry->reqHandleIdx, sizeof(request_response_t));

        // TODO: for now just trim the log here, as soon as we know the transaction was acked at a majority
        // Not entirely safe
        record.Remove(txnid);
    } else {
        RDebug("Not enough messages; size = %lu\n ", prepareResponseQuorum.GetMessages(vs).size());
    }
}

void IRReplica::HandleCommitReply(char *respBuf) {
}

} // namespace leadermeerkatir
} // namespace replication
