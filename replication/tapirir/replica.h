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

#ifndef _TAPIRIR_REPLICA_H_
#define _TAPIRIR_REPLICA_H_

#include <memory>

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "replication/common/quorumset.h"
#include "replication/tapirir/record.h"
#include "store/common/transaction.h"

namespace replication {
namespace tapirir {


class IRAppReplica
{
public:
    virtual ~IRAppReplica() = default;
    virtual void UnloggedUpcall(char *reqBuf, char *respBuf, size_t &respLen);
    virtual void InconsistentUpcall(const inconsistent_request_t& request);
    virtual void ConsensusUpcall(txnid_t txn_id, timestamp_t timestamp,
                                 Transaction transaction,
                                 consensus_response_t* response);
};

class IRReplica : TransportReceiver
{
public:
    IRReplica(transport::Configuration config, int myIdx,
              Transport *transport, IRAppReplica *app);

    // TransportReceiver API. //////////////////////////////////////////////////
    void ReceiveRequest(uint8_t reqType, char *request_buffer,
                        char *response_buffer) override;

    // TODO: For now, replicas do not need to communicate with eachother; they
    // will need to for synchronization. Because they do send any requests,
    // they never receive responses.
    void ReceiveResponse(uint8_t reqType, char *response_buffer) override {
        // Do nothing.
    }

    // IR replicas are never blocked. Only clients
    bool Blocked() override {
        return false;
    }

    // Helper to print statistics about various requests.
    void PrintStats();

private:
    // Message handlers. ///////////////////////////////////////////////////////
    void HandleUnloggedRequest(char *request_buffer, char *response_buffer,
                               size_t &response_length);
    void HandleInconsistentRequest(char *request_buffer, char *response_buffer,
                                   size_t &response_length);
    void HandleFinalizeInconsistentRequest(char *request_buffer,
                                           char *response_buffer,
                                           size_t &response_length);
    void HandleConsensusRequest(char *request_buffer,
                                char *response_buffer, size_t &response_length);
    void HandleFinalizeConsensusRequest(char *request_buffer,
                                        char *response_buffer,
                                        size_t &response_length);

    transport::Configuration config;

    // This replica's index into `config`.
    int my_index;

    Transport *transport;

    IRAppReplica *app;

    Record record;

    // Latencies of various requests, measured in microseconds. These latencies
    // are used to compute statistics when the PrintStats() function is called.
    std::vector<uint64_t> latency_unlogged_us;
    std::vector<uint64_t> latency_inconsistent_us;
    std::vector<uint64_t> latency_finalize_inconsistent_us;
    std::vector<uint64_t> latency_consensus_us;
    std::vector<uint64_t> latency_finalize_consensus_us;
};

} // namespace tapirir
} // namespace replication

#endif // _TAPIRIR_REPLICA_H_
