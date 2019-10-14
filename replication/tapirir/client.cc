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

#include <math.h>

#include "lib/assert.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "replication/tapirir/client.h"
#include "store/common/timestamp.h"

namespace replication {
namespace tapirir {

IRClient::IRClient(const transport::Configuration &config,
                   Transport *transport,
                   uint64_t clientid)
    : config(config),
      transport(transport),
      clientid(clientid),
      lastReqId(0),
      blocked(false) {
    // Randomly generate a client id if necessary. This is surely not the
    // fastest way to get a random 64-bit int, but it should be fine for this
    // purpose.
    while (this->clientid == 0) {
        std::random_device rd;
        std::mt19937_64 gen(rd());
        std::uniform_int_distribution<uint64_t> dis;
        this->clientid = dis(gen);
        Debug("TapirIR client id: %lu", this->clientid);
    }

    transport->Register(this, /* replicaIdx= */ -1);
}

// Sending /////////////////////////////////////////////////////////////////////
void IRClient::InvokeUnlogged(
        uint8_t core_id,
        int replica_index,
        const string &key,
        unlogged_continuation_t continuation,
        error_continuation_t error_continuation,
        uint32_t timeout) {
    // Update our metadata.
    uint64_t request_id = ++lastReqId;
    PendingUnloggedRequest req{continuation, error_continuation};
    pending_unlogged_requests.insert({request_id, req});
    blocked = true;

    // Create and send the request.
    auto *request_buffer = reinterpret_cast<unlogged_request_t *>(
            transport->GetRequestBuf());
    request_buffer->operation_id.client_id = clientid;
    request_buffer->operation_id.client_request_number = request_id;
    memcpy(request_buffer->key, key.c_str(), key.size());
    transport->SendRequestToReplica(this,
                                    UNLOGGED_REQUEST,
                                    replica_index, core_id,
                                    sizeof(unlogged_request_t));

    // TODO: Find a way to get sending errors (the eRPC's enqueue_request
    // function does not return errors)
    // TODO: deal with timeouts?
}

void IRClient::InvokeInconsistent(
        uint8_t core_id,
        uint64_t transaction_number,
        bool commit,
        inconsistent_continuation_t continuation,
        error_continuation_t error_continuation) {
    // Update our metadata.
    uint64_t request_id = ++lastReqId;
    PendingInconsistentRequest req(core_id, continuation, error_continuation);
    pending_inconsistent_requests.insert({request_id, req});
    blocked = true;

    // Create and send the request.
    auto *request_buffer = reinterpret_cast<inconsistent_request_t *>(
            transport->GetRequestBuf());
    request_buffer->operation_id.client_id = clientid;
    request_buffer->operation_id.client_request_number = request_id;
    request_buffer->transaction_number = transaction_number;
    request_buffer->commit = commit;
    transport->SendRequestToAll(this,
                                INCONSISTENT_REQUEST,
                                core_id,
                                sizeof(inconsistent_request_t));
}

void IRClient::InvokeConsensus(
        uint8_t core_id,
        uint64_t transaction_number,
        const Transaction &txn,
        const Timestamp &timestamp,
        decide_t decide,
        consensus_continuation_t continuation,
        error_continuation_t error_continuation) {
    // Update our metadata.
    uint64_t request_id = ++lastReqId;
    PendingConsensusRequest req(decide, continuation, error_continuation);
    pending_consensus_requests.insert({request_id, req});
    blocked = true;

    // Create and send the request.
    auto *request_buffer = reinterpret_cast<consensus_request_t *>(
            transport->GetRequestBuf());
    request_buffer->operation_id.client_id = clientid;
    request_buffer->operation_id.client_request_number = request_id;
    request_buffer->transaction_number = transaction_number;
    request_buffer->timestamp.timestamp = timestamp.getTimestamp();
    request_buffer->timestamp.client_id = timestamp.getID();
    request_buffer->num_reads = txn.getReadSet().size();
    request_buffer->num_writes = txn.getWriteSet().size();
    txn.serialize(reinterpret_cast<char *>(request_buffer + 1));
    transport->SendRequestToAll(this,
                                CONSENSUS_REQUEST,
                                core_id,
                                sizeof(consensus_request_t) +
                                  request_buffer->num_reads * sizeof(read_t) +
                                  request_buffer->num_writes * sizeof(write_t));
}

// Receiving ///////////////////////////////////////////////////////////////////
void IRClient::ReceiveResponse(uint8_t reqType, char *respBuf) {
    Debug("Client %lu received a response.", clientid);
    switch(reqType){
        case UNLOGGED_REQUEST:
            HandleUnloggedReply(respBuf);
            break;
        case INCONSISTENT_REQUEST:
            HandleInconsistentReply(respBuf);
            break;
        case CONSENSUS_REQUEST:
            HandleConsensusReply(respBuf);
            break;
        case FINALIZE_INCONSISTENT_REQUEST:
            HandleFinalizeConsensusReply(respBuf);
            break;
        default:
            Warning("Unexpected request type: %d", reqType);
    }
}

void IRClient::HandleUnloggedReply(char *respBuf) {
    auto *response = reinterpret_cast<unlogged_response_t *>(respBuf);

    // Ignore unexpected responses.
    auto it = pending_unlogged_requests.find(
            response->operation_id.client_request_number);
    if (it == pending_unlogged_requests.end()) {
        Warning("Received unlogged response when no request was pending; "
                "req_nr = %lu", response->operation_id.client_request_number);
        return;
    }

    // Invoke the continuation and unblock.
    it->second.continuation(respBuf);
    ASSERT(blocked);
    blocked = false;
    pending_unlogged_requests.erase(it);
}

void IRClient::HandleInconsistentReply(char *respBuf) {
    auto *response = reinterpret_cast<inconsistent_response_t *>(respBuf);

    // Ignore unexpected responses.
    auto it = pending_inconsistent_requests.find(
            response->operation_id.client_request_number);
    if (it == pending_inconsistent_requests.end()) {
        Warning("Received inconsistent response when no request was pending; "
                "req_nr = %lu", response->operation_id.client_request_number);
        return;
    }

    // Wait until we have a quorum of responses.
    PendingInconsistentRequest& pending = it->second;
    if (pending.quorum_set.AddAndCheckForQuorum(
                monostate{}, response->replica_index, monostate{}) == nullptr) {
        return;
    }

    // Send finalize messages to replicas.
    auto *request_buffer = reinterpret_cast<finalize_inconsistent_request_t *>(
            transport->GetRequestBuf());
    request_buffer->operation_id = response->operation_id;
    transport->SendRequestToAll(this,
                                FINALIZE_INCONSISTENT_REQUEST,
                                pending.core_id,
                                sizeof(finalize_inconsistent_request_t));

    // Invoke the continuation and unblock.
    pending.continuation();
    ASSERT(blocked);
    blocked = false;
    pending_inconsistent_requests.erase(it);
}

void IRClient::HandleFinalizeInconsistentReply(char *respBuf) {
    // Do nothing.
    (void) respBuf;
}

void IRClient::HandleConsensusReply(char *respBuf) {
    auto *response = reinterpret_cast<consensus_response_t *>(respBuf);

    // Ignore unexpected responses.
    auto it = pending_consensus_requests.find(
            response->operation_id.client_request_number);
    if (it == pending_consensus_requests.end()) {
        Warning("Received consensus response when no request was pending; "
                "req_nr = %lu", response->operation_id.client_request_number);
        return;
    }

    // If the result is finalized, we enter the slow path immediately.
    // Otherwise, we try and take the fast path, defaulting to the slow path if
    // not possible.
    PendingConsensusRequest& pending_consensus = it->second;
    if (response->finalized) {
        // Update our metadata.
        pending_slow_paths.insert({
            response->operation_id.client_request_number,
            PendingSlowPath(response->status, pending_consensus.continuation,
                            pending_consensus.error_continuation)
        });

        // Create and send a finalize consensus request.
        auto *request_buffer = reinterpret_cast<finalize_consensus_request_t *>(
                transport->GetRequestBuf());
        request_buffer->operation_id = response->operation_id;
        request_buffer->transaction_number = response->transaction_number;
        request_buffer->status = response->status;
        transport->SendRequestToAll(this,
                                    FINALIZE_CONSENSUS_REQUEST,
                                    pending_consensus.core_id,
                                    sizeof(finalize_consensus_request_t));
    } else {
        // Wait until we have a fast quorum of responses. In a real IR
        // implementation, we would either be waiting for a classic quorum or a
        // fast quorum depending on a timeout, but we don't have timeouts here.
        pending_consensus.quorum_set.Add(monostate{}, response->replica_index,
                                         *response);
        const std::map<int, consensus_response_t>& quorum =
            pending_consensus.quorum_set.GetMessages(monostate{});
        if (quorum.size() < config.FastQuorumSize()) {
            return;
        }

        // We've received a super quorum of responses. Now, we have to check to
        // see if we have a super quorum of _matching_ responses. `statuses` is
        // a histogram of the returned statuses.
        std::map<int, std::size_t> statuses;
        for (const auto& kv : quorum) {
            statuses[kv.second.status]++;
        }

        for (const auto &kv : statuses) {
            if (kv.second < config.FastQuorumSize()) {
                continue;
            }

            // A super quorum of matching requests was found! Create and
            // asynchronously send a finalize consensus request.
            auto *request_buffer =
                reinterpret_cast<finalize_consensus_request_t *>(
                transport->GetRequestBuf());
            request_buffer->operation_id = response->operation_id;
            request_buffer->transaction_number = response->transaction_number;
            request_buffer->status = kv.first;
            transport->SendRequestToAll(this,
                                        FINALIZE_CONSENSUS_REQUEST,
                                        pending_consensus.core_id,
                                        sizeof(finalize_consensus_request_t));

            // Invoke the continuation and unblock.
            pending_consensus.continuation(kv.first);
            ASSERT(blocked);
            blocked = false;
            pending_consensus_requests.erase(it);
            return;
        }

        // There was not a super quorum of matching results, so we transition
        // into the slow path.
        const int decided_status = pending_consensus.decide(statuses);
        pending_slow_paths.insert({
            response->operation_id.client_request_number,
            PendingSlowPath(decided_status, pending_consensus.continuation,
                            pending_consensus.error_continuation)
        });

        // Create and send a finalize consensus request.
        auto *request_buffer = reinterpret_cast<finalize_consensus_request_t *>(
                transport->GetRequestBuf());
        request_buffer->operation_id = response->operation_id;
        request_buffer->transaction_number = response->transaction_number;
        request_buffer->status = decided_status;
        transport->SendRequestToAll(this,
                                    FINALIZE_CONSENSUS_REQUEST,
                                    pending_consensus.core_id,
                                    sizeof(finalize_consensus_request_t));
    }
}

void IRClient::HandleFinalizeConsensusReply(char *respBuf) {
    auto *response = reinterpret_cast<finalize_consensus_response_t *>(respBuf);

    // Ignore unexpected responses.
    auto it = pending_slow_paths.find(
            response->operation_id.client_request_number);
    if (it == pending_slow_paths.end()) {
        Warning("Received finalize consensus response when no request was "
                "pending; req_nr = %lu",
                response->operation_id.client_request_number);
        return;
    }

    // Wait until we have a quorum of responses.
    PendingSlowPath& pending = it->second;
    if (pending.quorum_set.AddAndCheckForQuorum(
                monostate{}, response->replica_index, monostate{}) == nullptr) {
        return;
    }

    // Invoke the continuation and unblock.
    pending.continuation(pending.decided_status);
    ASSERT(blocked);
    blocked = false;
    pending_slow_paths.erase(it);
}

} // namespace tapirir
} // namespace replication
