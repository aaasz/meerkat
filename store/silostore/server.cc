// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/tapirstore/server.cc:
 *   Implementation of a single transactional key-value server.
 *
 * Copyright 2015 Irene Zhang <iyzhang@cs.washington.edu>
 *                Naveen Kr. Sharma <naveenks@cs.washington.edu>
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

#include "store/silostore/server.h"

#include <pthread.h>
#include <sched.h>
#include <cstdlib>
#include <iostream>
#include <thread>

#include <tbb/scalable_allocator.h>

namespace silostore {

using namespace std;

void ServerIR::LeaderUpcall(txnid_t txn_id,
                            replication::RecordEntry *crt_txn_state,
                            char *reqBuf, bool &replicate) {
    auto *req = reinterpret_cast<replication::leadermeerkatir::request_header_t *>(reqBuf);

    // validating the transaction
    if (crt_txn_state->txn_status != NOT_PREPARED) {
        Warning("Trying to prepare an already prepared transaction.");
        replicate = false;
        return;
    }

    // TODO: merge status with transaction status
    crt_txn_state->txn = Transaction(req->nr_reads, req->nr_writes, (char *)(req + 1));

    Timestamp proposed_write_ts;
    Timestamp proposed;
    int status = store->PrepareWrite(
        txn_id, crt_txn_state->txn,
        proposed_write_ts);

    if (status == REPLY_OK) {
        status = store->PrepareRead(
         txn_id, crt_txn_state->txn,
         proposed_write_ts, proposed);
    }

    if (status == REPLY_OK) {
        crt_txn_state->txn_status = PREPARED_OK;
        crt_txn_state->ts = proposed;
        replicate = true;
    } else {
        crt_txn_state->txn_status = PREPARED_ABORT;
        replicate = false;
    }
}

void ServerIR::LeaderUpcallPostPrepare(txnid_t txn_id,
                            replication::RecordEntry *crt_txn_state) {
    store->Commit(txn_id, crt_txn_state->ts, crt_txn_state->txn);
}

void ServerIR::ReplicaUpcall(txnid_t txn_id,
                             replication::RecordEntry *crt_txn_state) {
    // apply commit in store; without assuming we hold any locks
    if (crt_txn_state->txn_status == NOT_PREPARED) {
        // TODO: get state from other replicas
        Warning("Trying to commit an un-prepared transaction.");
        return;
    }
    store->ForceCommit(txn_id, crt_txn_state->ts, crt_txn_state->txn);
}

void ServerIR::UnloggedUpcall(char *reqBuf, char *respBuf, size_t &respLen) {
    Debug("Received Unlogged Request: %s", reqBuf);
    pair<Timestamp, string> val;

    auto *req = reinterpret_cast<replication::leadermeerkatir::unlogged_request_t *>(reqBuf);
    std::string key = string(req->key, 64);
    int status = store->Get(key, val);

    auto *resp = reinterpret_cast<replication::leadermeerkatir::unlogged_response_t *>(respBuf);
    respLen = sizeof(replication::leadermeerkatir::unlogged_response_t);
    resp->status = status;
    resp->req_nr = req->req_nr;
    resp->timestamp = val.first.getTimestamp();
    resp->id = val.first.getID();
    memcpy(resp->value, val.second.c_str(), 64);
}

void ServerIR::Load(const string &key, const string &value, const Timestamp timestamp) {
    store->Load(key, value, timestamp);
}

} // namespace silostore
