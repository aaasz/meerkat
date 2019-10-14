  // -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
  /***********************************************************************
 *
 * pb/client.cc:
 *   Viewstamped Replication clinet
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

#include "replication/leadermeerkatir/client.h"
#include "lib/assert.h"
#include "lib/message.h"
#include "lib/fasttransport.h"
#include "store/common/transaction.h"

namespace replication {
namespace leadermeerkatir {

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
        Debug("replication::leadermeerkatir::Client ID: %lu", this->clientid);
    }

    transport->Register(this, -1);
}

Client::~Client()
{
}

// TODO: intentionally not general enough to eliminate double copying
void Client::Invoke(uint64_t txn_nr,
                  uint32_t core_id,
                  const Timestamp &ts,
                  const Transaction &txn,
                  continuation_t continuation,
                  error_continuation_t error_continuation) {
    // TODO: Currently, invocations never timeout and error_continuation is
    // never called. It may make sense to set a timeout on the invocation.
    (void) error_continuation;

    uint64_t reqId = ++lastReqId;
    // TODO: for not, let eRPC handle request timeouts
    // auto timer = std::unique_ptr<Timeout>(new Timeout(transport, 500, [this, reqId]() {
    //     ResendRequest(reqId);
    // }));
    PendingRequest req = PendingRequest(reqId, txn_nr, core_id, continuation);
    pendingReqs[reqId] = req;

    Debug("Invoke for req_nr = %lu", reqId);
    size_t txnLen = txn.getReadSet().size() * sizeof(read_t) +
                    txn.getWriteSet().size() * sizeof(write_t);
    size_t reqLen = sizeof(request_header_t) + txnLen;
    auto *reqBuf = reinterpret_cast<request_header_t *>(
      transport->GetRequestBuf(
        reqLen,
        sizeof(request_response_t)
      )
    );
    reqBuf->req_nr = reqId;
    reqBuf->txn_nr = txn_nr;
    reqBuf->client_id = clientid;
    reqBuf->timestamp = ts.getTimestamp();
    reqBuf->id = ts.getID();
    reqBuf->nr_reads = txn.getReadSet().size();
    reqBuf->nr_writes = txn.getWriteSet().size();
    txn.serialize(reinterpret_cast<char *>(reqBuf + 1));
    blocked = true;
    // TODO: Send to the leader; for now just assume replica 0 is the leader
    transport->SendRequestToReplica(this,
                                clientReqType, 0,
                                core_id, reqLen);
}

void Client::InvokeUnlogged(uint64_t txn_nr,
                          uint32_t core_id,
                          int replicaIdx,
                          const string &request,
                          unlogged_continuation_t continuation,
                          error_continuation_t error_continuation,
                          uint32_t timeout) {
    uint64_t reqId = ++lastReqId;
    //auto timer = std::unique_ptr<Timeout>(new Timeout(
    //    transport, timeout,
    //    [this, reqId]() { UnloggedRequestTimeoutCallback(reqId); }));

    Debug("Invoke unlogged");
    PendingUnloggedRequest req =
        PendingUnloggedRequest(reqId,
                               txn_nr,
                               core_id,
                               continuation,
                               error_continuation);
                               //nullptr,
                               //std::move(timer));

    // TODO: find a way to get sending errors (the eRPC's enqueue_request
    // function does not return errors)
    // TODO: deal with timeouts?
    pendingUnloggedReqs[reqId] = req;
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

void Client::ReceiveResponse(uint8_t reqType, char *respBuf) {
    Debug("[%lu] received response", clientid);
    switch(reqType){
        case clientReqType:
            HandleReply(respBuf);
            break;
        case unloggedReqType:
            HandleUnloggedReply(respBuf);
            break;
        default:
            Warning("Unrecognized request type: %d\n", reqType);
    }
}

void Client::HandleReply(char *respBuf) {
    auto *resp = reinterpret_cast<request_response_t *>(respBuf);
    Debug("Received reply for req_nr = %lu", resp->req_nr);
    auto it = pendingReqs.find(resp->req_nr);
    if (it == pendingReqs.end()) {
        Warning("Received reply when no request was pending");
        return;
    }

    // invoke application callback
    ((PendingRequest)it->second).continuation(resp->status);
    // remove from pending list
    pendingReqs.erase(resp->req_nr);
    // unblock the call
    blocked = false;
}

void Client::HandleUnloggedReply(char *respBuf) {
    auto *resp = reinterpret_cast<unlogged_response_t *>(respBuf);
    auto it = pendingUnloggedReqs.find(resp->req_nr);
    if (it == pendingUnloggedReqs.end()) {
        Warning("Received unlogged reply when no request was pending; req_nr = %lu", resp->req_nr);
        return;
    }

    Debug("[%lu] Received unlogged reply", clientid);

    // invoke application callback
    ((PendingUnloggedRequest)it->second).unlogged_request_continuation(respBuf);
    // remove from pending list
    pendingUnloggedReqs.erase(resp->req_nr);
    // unblock the call
    blocked = false;
}

} // namespace leadermeerkatir
} // namespace replication
