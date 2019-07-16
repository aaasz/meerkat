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

#include "store/multitapirstore/server.h"

#include <pthread.h>
#include <sched.h>
#include <iostream>
#include <thread>

#define MULTITAPIRSTORE_MEASURE__TIMES true

namespace multitapirstore {

using namespace std;
using namespace proto;

#if  MULTITAPIRSTORE_MEASURE__TIMES
static constexpr double kAppLatFac = 100.0;        // Precision factor for latency
#endif

/*******************************************************
 IR App calls
 *******************************************************/
void
ServerIR::ExecInconsistentUpcall(txnid_t txn_id,
                               RecordEntryIR *crt_txn_state,
                               const string &str1)
{
#if  MULTITAPIRSTORE_MEASURE__TIMES
    struct timespec s, e;
#endif
    Debug("Received Inconsistent Request: %s",  str1.c_str());

    Request request;

    request.ParseFromString(str1);

    switch (request.op()) {
    case multitapirstore::proto::Request::COMMIT:
        if (crt_txn_state->txn_status == NOT_PREPARED) {
            // TODO: get state from other replicas
            Warning("Trying to commit an un-prepared transaction.");
            break;
        }
        if (crt_txn_state->txn_status != COMMITTED)
#if  MULTITAPIRSTORE_MEASURE__TIMES
            { clock_gettime(CLOCK_REALTIME, &s);
#endif
            store->Commit(txn_id, crt_txn_state->ts, crt_txn_state->txn);
#if  MULTITAPIRSTORE_MEASURE__TIMES
            clock_gettime(CLOCK_REALTIME, &e);
            double lat_us = (e.tv_nsec-s.tv_nsec)/1000.0 * kAppLatFac;
            latency_commit.push_back(static_cast<uint64_t>(lat_us)); }
#endif
        crt_txn_state->txn_status = COMMITTED;
        break;
    case multitapirstore::proto::Request::ABORT:
        if (crt_txn_state->txn_status == NOT_PREPARED) {
            // TODO: get state from other replicas
            Warning("Trying to abort an un-prepared transaction.");
            break;
        }
        if (crt_txn_state->txn_status != ABORTED)
            store->Abort(txn_id, crt_txn_state->txn);
        crt_txn_state->txn_status = ABORTED;
        break;
    default:
        Panic("Unrecognized inconsistent operation.");
    }
}

void
ServerIR::ExecConsensusUpcall(txnid_t txn_id,
                            RecordEntryIR *crt_txn_state,
                            const string &str1, string &str2)
{
#if  MULTITAPIRSTORE_MEASURE__TIMES
    struct timespec s, e;
#endif
    Debug("Received Consensus Request: %s", str1.c_str());

    Request request;
    Reply reply;
    int status;
    Timestamp proposed;

    request.ParseFromString(str1);

    switch (request.op()) {
    case multitapirstore::proto::Request::PREPARE:
        if (crt_txn_state->txn_status == NOT_PREPARED) {
            // TODO: make sure this creates a copy
            crt_txn_state->txn = Transaction(request.prepare().txn());
            crt_txn_state->ts = Timestamp(request.prepare().timestamp());
            //Debug("Prepare at timestamp: %lu", crt_txn_state->ts.getTimestamp());
#if  MULTITAPIRSTORE_MEASURE__TIMES
            clock_gettime(CLOCK_REALTIME, &s);
#endif
            status = store->Prepare(txn_id,
                                    crt_txn_state->txn,
                                    Timestamp(request.prepare().timestamp()),
                                    proposed);

#if  MULTITAPIRSTORE_MEASURE__TIMES
            clock_gettime(CLOCK_REALTIME, &e);
            double lat_us = (e.tv_nsec-s.tv_nsec)/1000.0 * kAppLatFac;
            latency_prepare.push_back(static_cast<uint64_t>(lat_us));
#endif
            reply.set_status(status);
            if (proposed.isValid()) {
                proposed.serialize(reply.mutable_timestamp());
            }
            reply.SerializeToString(&str2);

            // TODO: merge status with transaction status
            if (status == REPLY_OK) {
                crt_txn_state->txn_status = PREPARED_OK;
            } else {
                crt_txn_state->txn_status = PREPARED_ABORT;
            }
        }
        break;
    default:
        Panic("Unrecognized consensus operation.");
    }

}

void
ServerIR::UnloggedUpcall(txnid_t txn_id, const string &str1, string &str2)
{
#if  MULTITAPIRSTORE_MEASURE__TIMES
    struct timespec s, e;
#endif
    Debug("Received Unlogged Request: %s", str1.c_str());

    Request request;
    Reply reply;
    int status;

    request.ParseFromString(str1);

    //TODO: add the other operations

    switch (request.op()) {
    case multitapirstore::proto::Request::GET: {
        if (request.get().has_timestamp()) {
            pair<Timestamp, string> val;
            status = store->Get(txn_id, request.get().key(),
                                request.get().timestamp(), val);
            if (status == 0) {
                reply.set_value(val.second);
            }
        } else {
            pair<Timestamp, string> val;
#if  MULTITAPIRSTORE_MEASURE__TIMES
            clock_gettime(CLOCK_REALTIME, &s);
#endif
            status = store->Get(txn_id, request.get().key(), val);
#if  MULTITAPIRSTORE_MEASURE__TIMES
            clock_gettime(CLOCK_REALTIME, &e);
            double lat_us = (e.tv_nsec-s.tv_nsec)/1000.0 * kAppLatFac;
            latency_get.push_back(static_cast<uint64_t>(lat_us));
#endif
            if (status == 0) {
                reply.set_value(val.second);
                val.first.serialize(reply.mutable_timestamp());
            }
        }
        reply.set_status(status);
        reply.SerializeToString(&str2);
        break;
    }
    case multitapirstore::proto::Request::PREPARE: {
        Timestamp proposed;
        status = store->Prepare(
            txn_id, Transaction(request.prepare().txn()),
            Timestamp(request.prepare().timestamp()), proposed);
        proposed.serialize(reply.mutable_timestamp());

        // TODO: we don't support twopc over non-replicated for now
//        if (!twopc) {
        if (status == 0) {
            store->Commit(txn_id, Timestamp(request.prepare().timestamp()), Transaction(request.prepare().txn()));
             // Not necessary
//               } else {
//                   store->Abort(txn_id, Transaction(request.prepare().txn()));
//            }
        }
        reply.set_status(status);
        reply.SerializeToString(&str2);
        break;
    }
    default:
        Panic("Unrecognized Unlogged request.");
    }
}

//void
//ServerIR::Sync(const std::map<txnid_t, RecordEntry>& record)
//{
//    Panic("Unimplemented!");
//}
//
//std::map<txnid_t, std::string>
//ServerIR::Merge(const std::map<txnid_t, std::vector<RecordEntry>> &d,
//              const std::map<txnid_t, std::vector<RecordEntry>> &u,
//              const std::map<txnid_t, std::string> &majority_results_in_d)
//{
//    Panic("Unimplemented!");
//}

void
ServerIR::Load(const string &key, const string &value, const Timestamp timestamp)
{
    store->Load(key, value, timestamp);
}

void
ServerIR::PrintStats() {
#if  MULTITAPIRSTORE_MEASURE__TIMES
    std::sort(latency_get.begin(), latency_get.end());
    std::sort(latency_prepare.begin(), latency_prepare.end());
    std::sort(latency_commit.begin(), latency_commit.end());

    uint64_t latency_get_size = latency_get.size();
    uint64_t latency_prepare_size = latency_prepare.size();
    uint64_t latency_commit_size = latency_prepare.size();

    double latency_get_50 = latency_get[(latency_get_size*50)/100] / kAppLatFac;
    double latency_get_99 = latency_get[(latency_get_size*99)/100] / kAppLatFac;

    double latency_prepare_50 = latency_prepare[(latency_prepare_size*50)/100] / kAppLatFac;
    double latency_prepare_99 = latency_prepare[(latency_prepare_size*99)/100] / kAppLatFac;

    double latency_commit_50 = latency_commit[(latency_commit_size*50)/100] / kAppLatFac;
    double latency_commit_99 = latency_commit[(latency_commit_size*99)/100] / kAppLatFac;

    uint64_t latency_get_sum = std::accumulate(latency_get.begin(), latency_get.end(), 0);
    uint64_t latency_prepare_sum = std::accumulate(latency_prepare.begin(), latency_prepare.end(), 0);
    uint64_t latency_commit_sum = std::accumulate(latency_commit.begin(), latency_commit.end(), 0);

    double latency_get_avg = latency_get_sum/latency_get_size/kAppLatFac;
    double latency_prepare_avg = latency_prepare_sum/latency_prepare_size/kAppLatFac;
    double latency_commit_avg = latency_commit_sum/latency_commit_size/kAppLatFac;

    fprintf(stderr, "Get latency (size = %d) [avg: %.2f; 50 percentile: %.2f; 99 percentile: %.2f] us \n",
            latency_get_size,
            latency_get_avg,
            latency_get_50,
            latency_get_99);

    fprintf(stderr, "Prepare latency (size = %d) [avg: %.2f; 50 percentile: %.2f; 99 percentile: %.2f] us \n",
            latency_prepare_size,
            latency_prepare_avg,
            latency_prepare_50,
            latency_prepare_99);

    fprintf(stderr, "Commit latency (size = %d) [avg: %.2f; 50 percentile: %.2f; 99 percentile: %.2f] us \n",
            latency_commit_size,
            latency_commit_avg,
            latency_commit_50,
            latency_commit_99);
#endif
}

} // namespace multitapirstore