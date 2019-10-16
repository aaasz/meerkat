// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/multitapirstore/server.cc:
 *   Implementation of a single transactional key-value server.
 *
 * Copyright 2015 Irene Zhang <iyzhang@cs.washington.edu>
 *                Naveen Kr. Sharma <naveenks@cs.washington.edu>
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

#include <pthread.h>
#include <sched.h>
#include <iostream>
#include <thread>

#include "store/meerkatstore/meerkatir/server.h"

namespace meerkatstore {
namespace meerkatir {

using namespace std;

void Server::ExecInconsistentUpcall(txnid_t txn_id,
                            replication::RecordEntry *crt_txn_state,
                            bool commit) {

    if (crt_txn_state->txn_status == NOT_PREPARED) {
        // TODO: get state from other replicas
        Warning("Trying to abort an un-prepared transaction.");
    }

    if (commit) {
        if (crt_txn_state->txn_status != COMMITTED)
            store->Commit(txn_id, crt_txn_state->ts, crt_txn_state->txn);
        crt_txn_state->txn_status = COMMITTED;
    } else {
        if (crt_txn_state->txn_status != ABORTED)
            store->Abort(txn_id, crt_txn_state->txn);
        crt_txn_state->txn_status = ABORTED;
    }
}

void Server::ExecConsensusUpcall(txnid_t txn_id,
                            replication::RecordEntry *crt_txn_state,
                            uint8_t nr_reads,
                            uint8_t nr_writes,
                            uint64_t timestamp,
                            uint64_t id,
                            char *reqBuf,
                            char *respBuf, size_t &respLen) {
    Debug("Received Consensus Request");
    int status;
    Timestamp proposed;

    auto *resp = reinterpret_cast<replication::meerkatir::consensus_response_t *>(respBuf);

    if (crt_txn_state->txn_status == NOT_PREPARED) {
        // TODO: make sure this creates a copy
        crt_txn_state->txn = Transaction(nr_reads, nr_writes, reqBuf);
        crt_txn_state->ts = Timestamp(timestamp, id);
        //Debug("Prepare at timestamp: %lu", crt_txn_state->ts.getTimestamp());
        status = store->Prepare(txn_id,
                                crt_txn_state->txn,
                                crt_txn_state->ts,
                                proposed);
        resp->status = status;

        // TODO: merge status with transaction status
        if (status == REPLY_OK) {
            crt_txn_state->txn_status = PREPARED_OK;
        } else {
            crt_txn_state->txn_status = PREPARED_ABORT;
        }
    } else {
        // TODO:
    }
}

void Server::UnloggedUpcall(char *reqBuf, char *respBuf, size_t &respLen) {
    Debug("Received Unlogged Request: %s", reqBuf);
    std::pair<Timestamp, string> val;

    auto *req = reinterpret_cast<replication::meerkatir::unlogged_request_t *>(reqBuf);
    std::string key = string(req->key, 64);
    int status = store->Get(key, val);

    auto *resp = reinterpret_cast<replication::meerkatir::unlogged_response_t *>(respBuf);
    respLen = sizeof(replication::meerkatir::unlogged_response_t);
    resp->status = status;
    resp->req_nr = req->req_nr;
    resp->timestamp = val.first.getTimestamp();
    resp->id = val.first.getID();
    memcpy(resp->value, val.second.c_str(), 64);
}

void
Server::Load(const string &key, const string &value, const Timestamp timestamp) {
    store->Load(key, value, timestamp);
}

void
Server::PrintStats() {
    // fprintf(stderr, "%lu\n", store->fake_counter[10].load());
}

} // namespace meerkatir
} // namespace meerkatrstore