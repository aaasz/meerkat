// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 * Copyright 2019 Dan R. K. Ports <drkp@cs.washington.edu>
 *                Irene Zhang Ports <iyzhang@cs.washington.edu>
 *                Adriana Szekeres <aaasz@cs.washington.edu>
 *                Naveen Sharma <naveenks@cs.washington.edu>
 *                Michael Whittaker <mjwhittaker@berkeley.edu>
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

#include <numeric>

#include "store/tapirstore/server.h"

#define TAPIRSTORE_MEASURE_TIMES false

namespace tapirstore {

void Server::InconsistentUpcall(
        const replication::tapirir::inconsistent_request_t& request) {
#if TAPIRIR_MEASURE_APP_TIMES
    struct timespec s, e;
    clock_gettime(CLOCK_REALTIME, &s);
#endif

    if (request.commit) {
        store.Commit(
            {request.operation_id.client_id, request.transaction_number},
            Timestamp(request.timestamp.timestamp, request.timestamp.client_id));
#if TAPIRIR_MEASURE_APP_TIMES
    clock_gettime(CLOCK_REALTIME, &e);
    double latency_us = (e.tv_nsec - s.tv_nsec) / 1000.0 * kAppLatFac;
    latency_commit_us.push_back(static_cast<uint64_t>(latency_us));
#endif
    } else {
        store.Abort(
            {request.operation_id.client_id, request.transaction_number});
#if TAPIRIR_MEASURE_APP_TIMES
    clock_gettime(CLOCK_REALTIME, &e);
    double latency_us = (e.tv_nsec - s.tv_nsec) / 1000.0 * kAppLatFac;
    latency_abort_us.push_back(static_cast<uint64_t>(latency_us));
#endif
    }
}

void Server::ConsensusUpcall(
        txnid_t txn_id,
        replication::tapirir::timestamp_t timestamp,
        Transaction transaction,
        replication::tapirir::consensus_response_t* response) {
#if TAPIRIR_MEASURE_APP_TIMES
    struct timespec s, e;
    clock_gettime(CLOCK_REALTIME, &s);
#endif

    Timestamp proposed;
    int status = store.Prepare(
        txn_id, transaction, Timestamp(timestamp.timestamp, timestamp.client_id),
        proposed);

    response->status = status;
    if (proposed.isValid()) {
        response->timestamp.timestamp = proposed.getTimestamp();
        response->timestamp.client_id = proposed.getID();
    }

#if TAPIRIR_MEASURE_APP_TIMES
    clock_gettime(CLOCK_REALTIME, &e);
    double latency_us = (e.tv_nsec - s.tv_nsec) / 1000.0 * kAppLatFac;
    latency_prepare_us.push_back(static_cast<uint64_t>(latency_us));
#endif
}

void
Server::UnloggedUpcall(char *request_buffer, char *response_buffer,
                       size_t &response_length) {
#if TAPIRIR_MEASURE_APP_TIMES
    struct timespec s, e;
    clock_gettime(CLOCK_REALTIME, &s);
#endif

    // Parse the request.
    auto *request = reinterpret_cast<
        replication::tapirir::unlogged_request_t*>(request_buffer);
    std::string key = string(request->key, 64);

    // Issue the get to the underlying store.
    std::pair<Timestamp, std::string> value;
    int status = store.Get(key, value);

    // Form the response.
    auto *response = reinterpret_cast<
        replication::tapirir::unlogged_response_t *>(response_buffer);
    response->operation_id = request->operation_id;
    response->status = status;
    if (status == REPLY_OK) {
        response->timestamp.timestamp = value.first.getTimestamp();
        response->timestamp.client_id = value.first.getID();
        memcpy(response->value, value.second.c_str(), 64);
    }
    response_length = sizeof(replication::tapirir::unlogged_response_t);

#if TAPIRIR_MEASURE_APP_TIMES
    clock_gettime(CLOCK_REALTIME, &e);
    double latency_us = (e.tv_nsec - s.tv_nsec) / 1000.0 * kAppLatFac;
    latency_get_us.push_back(static_cast<uint64_t>(latency_us));
#endif
}

void
Server::Load(const string &key, const string &value, const Timestamp timestamp)
{
    store.Load(key, value, timestamp);
}

void PrintStat(std::vector<uint64_t>* measurements) {
    std::sort(measurements->begin(), measurements->end());
    double p50 = (*measurements)[measurements->size() * 50 / 100] / kAppLatFac;
    double p99 = (*measurements)[measurements->size() * 99 / 100] / kAppLatFac;
    double avg =
        std::accumulate(measurements->begin(), measurements->end(), 0) /
        measurements->size() / kAppLatFac;
    std::cerr << "  size = " << measurements->size() << std::endl
              << "  avg  = " << avg << std::endl
              << "  p50  = " << p50 << std::endl
              << "  p99  = " << p99 << std::endl;
}

void
Server::PrintStats() {
#if  IRREPLICA_MEASURE_APP_TIMES
    std::cerr << "Get latency." << std::endl;
    PrintStat(&latency_get_us);
    std::cerr << "Prepare latency." << std::endl;
    PrintStat(&latency_prepare_us);
    std::cerr << "Commit latency." << std::endl;
    PrintStat(&latency_commit_us);
    std::cerr << "Abort latency." << std::endl;
    PrintStat(&latency_abort_us);
#endif
}

} // namespace tapirstore
