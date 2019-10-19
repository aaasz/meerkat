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

#include <cstdint>

#include <numeric>
#include <set>

#include "lib/configuration.h"
#include "replication/tapirir/replica.h"

#define TAPIRIR_MEASURE_APP_TIMES false

namespace replication {
namespace tapirir {

// using namespace std;
// using namespace proto;

IRReplica::IRReplica(transport::Configuration config, int my_index,
                     Transport *transport, IRAppReplica *app)
    : config(std::move(config)), my_index(my_index), transport(transport),
      app(app) {
    if (transport != nullptr) {
        transport->Register(this, my_index);
    } else {
        // we use this for micorbenchmarking, but still issue a warning
        Warning("NULL transport provided to the replication layer");
    }
}

void IRReplica::ReceiveRequest(uint8_t request_type, char *request_buffer,
                               char *response_buffer) {
    size_t response_length;
    switch(request_type) {
        case UNLOGGED_REQUEST:
            HandleUnloggedRequest(request_buffer, response_buffer,
                                  response_length);
            break;
        case INCONSISTENT_REQUEST:
            HandleInconsistentRequest(request_buffer, response_buffer,
                                      response_length);
            break;
        case FINALIZE_INCONSISTENT_REQUEST:
            HandleFinalizeInconsistentRequest(request_buffer, response_buffer,
                                              response_length);
            break;
        case CONSENSUS_REQUEST:
            HandleConsensusRequest(request_buffer, response_buffer,
                                   response_length);
            break;
        case FINALIZE_CONSENSUS_REQUEST:
            HandleFinalizeConsensusRequest(request_buffer, response_buffer,
                                           response_length);
            break;
        default:
            Warning("Unrecognized rquest type: %d", request_type);
            break;
    }

    if (!(transport->SendResponse(response_length))) {
        Warning("Failed to send reply message");
    }
}

void IRReplica::HandleUnloggedRequest(char *request_buffer,
                                      char *response_buffer,
                                      size_t &response_length) {
#if TAPIRIR_MEASURE_APP_TIMES
    struct timespec s, e;
    clock_gettime(CLOCK_REALTIME, &s);
#endif

    app->UnloggedUpcall(request_buffer, response_buffer, response_length);

#if TAPIRIR_MEASURE_APP_TIMES
    clock_gettime(CLOCK_REALTIME, &e);
    double latency_us = (e.tv_nsec - s.tv_nsec) / 1000.0 * kAppLatFac;
    latency_unlogged_us.push_back(static_cast<uint64_t>(latency_us));
#endif
}

void IRReplica::HandleInconsistentRequest(char *request_buffer,
                                          char *response_buffer,
                                          size_t &response_length) {
#if TAPIRIR_MEASURE_APP_TIMES
    struct timespec s, e;
    clock_gettime(CLOCK_REALTIME, &s);
#endif

    auto *request = reinterpret_cast<inconsistent_request_t *>(request_buffer);

    // If we haven't processed this request before, we put it in our record.
    auto it = record.Entries().find(request->operation_id);
    if (it == record.Entries().end()) {
        // We haven't processed this request before.
        record.MutableEntries().insert({request->operation_id, RecordEntry {
            /*view=*/ 0,
            /*opid=*/ request->operation_id,
            /*type=*/ EntryType::INCONSISTENT,
            /*status=*/ EntryStatus::TENTATIVE,
            /*request=*/ std::vector<char>(
                    request_buffer,
                    request_buffer + sizeof(inconsistent_request_t)),
            /*result=*/ std::vector<char>()
        }});
    }

    // Whether or not we've seen the request before, we return a response.
    auto *response = reinterpret_cast<inconsistent_response_t *>(
            response_buffer);
    response->operation_id = request->operation_id;
    response->replica_index = my_index;
    response_length = sizeof(inconsistent_response_t);

#if TAPIRIR_MEASURE_APP_TIMES
    clock_gettime(CLOCK_REALTIME, &e);
    double latency_us = (e.tv_nsec - s.tv_nsec) / 1000.0 * kAppLatFac;
    latency_inconsistent_us.push_back(static_cast<uint64_t>(latency_us));
#endif
}

void IRReplica::HandleFinalizeInconsistentRequest(
        char *request_buffer, char *response_buffer, size_t &response_length) {
#if TAPIRIR_MEASURE_APP_TIMES
    struct timespec s, e;
    clock_gettime(CLOCK_REALTIME, &s);
#endif

    auto *request = reinterpret_cast<finalize_inconsistent_request_t *>(
            request_buffer);
    auto it = record.MutableEntries().find(request->operation_id);
    if (it == record.MutableEntries().end()) {
        // We received a finalize inconsistent request for an operation that we
        // never received! We're ignoring it.
        //
        // TODO(mwhittaker): Here, we don't want to return a reply, but ERPC
        // forces us to. We should hvae a field in the reply which indicates
        // whether the reply should be ignored. For now, we don't.
    } else {
        // Execute the operation and mark it as finalized, if we haven't
        // already.
        RecordEntry& entry = it->second;
        ASSERT(entry.type == EntryType::INCONSISTENT);
        if (entry.status == EntryStatus::TENTATIVE) {
            auto *inconsistent_request =
                reinterpret_cast<inconsistent_request_t*>(entry.request.data());
            app->InconsistentUpcall(*inconsistent_request);
            entry.status = EntryStatus::FINALIZED;
        }
    }

    response_length = sizeof(finalize_inconsistent_response_t);

#if TAPIRIR_MEASURE_APP_TIMES
    clock_gettime(CLOCK_REALTIME, &e);
    double latency_us = (e.tv_nsec - s.tv_nsec) / 1000.0 * kAppLatFac;
    latency_finalize_inconsistent_us.push_back(
            static_cast<uint64_t>(latency_us));
#endif
}

void IRReplica::HandleConsensusRequest(char *request_buffer,
                                       char *response_buffer,
                                       size_t &response_length) {
#if TAPIRIR_MEASURE_APP_TIMES
    struct timespec s, e;
    clock_gettime(CLOCK_REALTIME, &s);
#endif

    auto *request = reinterpret_cast<consensus_request_t *>(request_buffer);
    auto it = record.MutableEntries().find(request->operation_id);
    if (it == record.MutableEntries().end()) {
        // We have not processed this operation yet. Invoke the upcall and form
        // a response.
        auto *response = reinterpret_cast<consensus_response_t *>(
                response_buffer);
        app->ConsensusUpcall(
                {request->operation_id.client_id, request->transaction_number},
                request->timestamp,
                Transaction(request->num_reads, request->num_writes,
                            request_buffer + sizeof(consensus_request_t)),
                response);
        response->operation_id = request->operation_id;
        response->transaction_number = request->transaction_number;
        response->replica_index = my_index;
        response->finalized = false;
        response_length = sizeof(consensus_response_t);

        // Update the record.
        record.MutableEntries().insert({request->operation_id, RecordEntry {
            /*view=*/ 0,
            /*opid=*/ request->operation_id,
            /*type=*/ EntryType::CONSENSUS,
            /*status=*/ EntryStatus::TENTATIVE,
            /*request=*/ std::vector<char>(),
            /*result=*/ std::vector<char>(response_buffer,
                                          response_buffer +
                                          sizeof(consensus_response_t))
        }});
    } else {
        // We've already processed this request. Copy over our saved response
        // into the response buffer.
        RecordEntry& entry = it->second;
        std::copy(entry.result.begin(), entry.result.end(), response_buffer);
        response_length = sizeof(consensus_response_t);
    }

#if TAPIRIR_MEASURE_APP_TIMES
    clock_gettime(CLOCK_REALTIME, &e);
    double latency_us = (e.tv_nsec - s.tv_nsec) / 1000.0 * kAppLatFac;
    latency_consensus_us.push_back(static_cast<uint64_t>(latency_us));
#endif
}

void IRReplica::HandleFinalizeConsensusRequest(char *request_buffer,
                                               char *response_buffer,
                                               size_t &response_length) {
#if TAPIRIR_MEASURE_APP_TIMES
    struct timespec s, e;
    clock_gettime(CLOCK_REALTIME, &s);
#endif

    auto *request = reinterpret_cast<finalize_consensus_request_t *>(
            request_buffer);
    auto it = record.MutableEntries().find(request->operation_id);
    if (it == record.MutableEntries().end()) {
        // TODO(mwhittaker): Here, we don't want to return a reply, but ERPC
        // forces us to. We should hvae a field in the reply which indicates
        // whether the reply should be ignored. For now, we don't.
    } else {
        RecordEntry& entry = it->second;

        // Update our response.
        auto *response = reinterpret_cast<consensus_response_t *>(
            entry.result.data());
        response->status = request->status;
        response->timestamp = request->timestamp;
        response->finalized = true;

        // Update our record.
        entry.status = EntryStatus::FINALIZED;
    }

    auto *response = reinterpret_cast<finalize_consensus_response_t *>(
            response_buffer);
    response->operation_id = request->operation_id;
    response->replica_index = my_index;
    response_length = sizeof(finalize_consensus_response_t);

#if TAPIRIR_MEASURE_APP_TIMES
    clock_gettime(CLOCK_REALTIME, &e);
    double latency_us = (e.tv_nsec - s.tv_nsec) / 1000.0 * kAppLatFac;
    latency_finalize_consensus_us.push_back(static_cast<uint64_t>(latency_us));
#endif
}

void PrintStat(std::vector<uint64_t>* measurements) {
#if IRREPLICA_MEASURE_APP_TIMES
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
#endif
}

void
IRReplica::PrintStats() {
#if IRREPLICA_MEASURE_APP_TIMES
    std::cerr << "Unlogged latency." << std::endl;
    PrintStat(&latency_unlogged_us);
    std::cerr << "Inconsistent latency." << std::endl;
    PrintStat(&latency_inconsistent_us);
    std::cerr << "Finalize inconsistent latency." << std::endl;
    PrintStat(&latency_finalize_inconsistent_us);
    std::cerr << "Consensus latency." << std::endl;
    PrintStat(&latency_consensus_us);
    std::cerr << "Finalize consensus latency." << std::endl;
    PrintStat(&latency_finalize_consensus_us);
#endif
}

} // namespace tapirir
} // namespace replication
