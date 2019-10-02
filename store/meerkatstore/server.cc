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

#include "store/meerkatstore/server.h"


#define MULTITAPIRSTORE_MEASURE__TIMES false

namespace meerkatstore {

using namespace std;

/*******************************************************
 IR App calls
 *******************************************************/

void ServerIR::ExecInconsistentUpcall(txnid_t txn_id,
                            RecordEntryIR *crt_txn_state,
                            bool commit) {
#if  MULTITAPIRSTORE_MEASURE__TIMES
    struct timespec s, e;
#endif
    if (commit) {
        if (crt_txn_state->txn_status == NOT_PREPARED) {
            // TODO: get state from other replicas
            Warning("Trying to commit an un-prepared transaction.");
        }
        if (crt_txn_state->txn_status != COMMITTED)
#if  MULTITAPIRSTORE_MEASURE__TIMES
            { clock_gettime(CLOCK_REALTIME, &s);
#endif
            store->Commit(txn_id, crt_txn_state->ts, crt_txn_state->txn);
#if  MULTITAPIRSTORE_MEASURE__TIMES
            clock_gettime(CLOCK_REALTIME, &e);
            long lat_ns = e.tv_nsec-s.tv_nsec;
            if (e.tv_nsec < s.tv_nsec) { //clock underflow
                lat_ns += 1000000000;
            }
            latency_commit.push_back(lat_ns); }
#endif
        crt_txn_state->txn_status = COMMITTED;
    } else {
        if (crt_txn_state->txn_status == NOT_PREPARED) {
            // TODO: get state from other replicas
            Warning("Trying to abort an un-prepared transaction.");
        }
        if (crt_txn_state->txn_status != ABORTED)
            store->Abort(txn_id, crt_txn_state->txn);
        crt_txn_state->txn_status = ABORTED;
    }
}

void ServerIR::ExecConsensusUpcall(txnid_t txn_id,
                            RecordEntryIR *crt_txn_state,
                            uint8_t nr_reads,
                            uint8_t nr_writes,
                            uint64_t timestamp,
                            uint64_t id,
                            char *reqBuf,
                            char *respBuf, size_t &respLen) {
#if  MULTITAPIRSTORE_MEASURE__TIMES
    struct timespec s, e;
#endif
    Debug("Received Consensus Request");
    int status;
    Timestamp proposed;

    auto *resp = reinterpret_cast<consensus_response_t *>(respBuf);

    if (crt_txn_state->txn_status == NOT_PREPARED) {
        // TODO: make sure this creates a copy
        crt_txn_state->txn = Transaction(nr_reads, nr_writes, reqBuf);
        crt_txn_state->ts = Timestamp(timestamp, id);
        //Debug("Prepare at timestamp: %lu", crt_txn_state->ts.getTimestamp());
#if  MULTITAPIRSTORE_MEASURE__TIMES
        clock_gettime(CLOCK_REALTIME, &s);
#endif
        status = store->Prepare(txn_id,
                                crt_txn_state->txn,
                                crt_txn_state->ts,
                                proposed);

#if  MULTITAPIRSTORE_MEASURE__TIMES
        clock_gettime(CLOCK_REALTIME, &e);
        long lat_ns = e.tv_nsec-s.tv_nsec;
        if (e.tv_nsec < s.tv_nsec) { //clock underflow
            lat_ns += 1000000000;
        }
        latency_prepare.push_back(lat_ns);
#endif
        resp->status = status;
        if (proposed.isValid()) {
            resp->id = proposed.getID();
            resp->timestamp = proposed.getTimestamp();
        }

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

void ServerIR::UnloggedUpcall(char *reqBuf, char *respBuf, size_t &respLen) {
    Debug("Received Unlogged Request: %s", reqBuf);
    pair<Timestamp, string> val;

    auto *req = reinterpret_cast<unlogged_request_t *>(reqBuf);
    std::string key = string(req->key, 64);
    int status = store->Get(key, val);

    auto *resp = reinterpret_cast<unlogged_response_t *>(respBuf);
    respLen = sizeof(unlogged_response_t);
    resp->status = status;
    resp->req_nr = req->req_nr;
    resp->timestamp = val.first.getTimestamp();
    resp->id = val.first.getID();
    memcpy(resp->value, val.second.c_str(), 64);
}

void
ServerIR::Load(const string &key, const string &value, const Timestamp timestamp)
{
    store->Load(key, value, timestamp);
}

void
ServerIR::PrintStats() {
    // fprintf(stderr, "%lu\n", store->fake_counter[10].load());
#if  MULTITAPIRSTORE_MEASURE__TIMES
    // Discard first third of collected results

    std::sort(latency_get.begin() + latency_get.size()/3, latency_get.end() - latency_get.size()/3);
    std::sort(latency_prepare.begin() + latency_prepare.size()/3, latency_prepare.end() - latency_prepare.size()/3);
    std::sort(latency_commit.begin() + latency_commit.size()/3, latency_commit.end() - latency_prepare.size()/3);

    double latency_get_50 = latency_get[latency_get.size()/3 + ((latency_get.size() - 2 * latency_get.size()/3)*50)/100] / 1000.0;
    double latency_get_99 = latency_get[latency_get.size()/3 + ((latency_get.size() - 2 * latency_get.size()/3)*99)/100] / 1000.0;

    double latency_prepare_50 = latency_prepare[latency_get.size()/3 + ((latency_prepare.size() - 2 * latency_prepare.size()/3)*50)/100] / 1000.0;
    double latency_prepare_99 = latency_prepare[latency_get.size()/3 + ((latency_prepare.size() - 2 * latency_prepare.size()/3)*99)/100] / 1000.0;

    double latency_commit_50 = latency_commit[latency_get.size()/3 + ((latency_commit.size() - 2 * latency_commit.size()/3)*50)/100] / 1000.0;
    double latency_commit_99 = latency_commit[latency_get.size()/3 + ((latency_commit.size() - 2 * latency_commit.size()/3)*99)/100] / 1000.0;

    uint64_t latency_get_sum = std::accumulate(latency_get.begin() + latency_get.size()/3, latency_get.end() - latency_get.size()/3, 0);
    uint64_t latency_prepare_sum = std::accumulate(latency_prepare.begin() + latency_prepare.size()/3, latency_prepare.end() - latency_prepare.size()/3, 0);
    uint64_t latency_commit_sum = std::accumulate(latency_commit.begin() + latency_commit.size()/3, latency_commit.end() - latency_commit.size()/3, 0);

    double latency_get_avg = latency_get_sum/(latency_get.size() - 2 * latency_get.size()/3)/1000.0;
    double latency_prepare_avg = latency_prepare_sum/(latency_prepare.size() - 2 * latency_prepare.size()/3)/1000.0;
    double latency_commit_avg = latency_commit_sum/(latency_commit.size() - 2 * latency_commit.size()/3)/1000.0;

    fprintf(stderr, "Get latency (size = %lu) [avg: %.2f; 50 percentile: %.2f; 99 percentile: %.2f] us \n",
            latency_get.size(),
            latency_get_avg,
            latency_get_50,
            latency_get_99);

    fprintf(stderr, "Prepare latency (size = %lu) [avg: %.2f; 50 percentile: %.2f; 99 percentile: %.2f] us \n",
            latency_prepare.size(),
            latency_prepare_avg,
            latency_prepare_50,
            latency_prepare_99);

    fprintf(stderr, "Commit latency (size = %lu) [avg: %.2f; 50 percentile: %.2f; 99 percentile: %.2f] us \n",
            latency_commit.size(),
            latency_commit_avg,
            latency_commit_50,
            latency_commit_99);

#endif
}

} // namespace meerkatrstore