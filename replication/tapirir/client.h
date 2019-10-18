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

#ifndef _TAPIRIR_CLIENT_H_
#define _TAPIRIR_CLIENT_H_

#include <functional>
#include <map>
#include <memory>
#include <set>
#include <unordered_map>

#include <boost/functional/hash.hpp>
#include <boost/unordered_map.hpp>

#include "lib/configuration.h"
#include "lib/fasttransport.h"
#include "replication/common/quorumset.h"
#include "replication/tapirir/messages.h"
#include "store/common/transaction.h"

namespace replication {
namespace tapirir {

// A backport of C++17 monostate, a unit type [1].
//
// [1]: https://en.cppreference.com/w/cpp/utility/variant/monostate.
struct monostate{};
bool operator<(monostate, monostate) { return false; }
bool operator<=(monostate, monostate) { return true; }
bool operator>(monostate, monostate) { return false; }
bool operator>=(monostate, monostate) { return true; }
bool operator==(monostate, monostate) { return true; }
bool operator!=(monostate, monostate) { return false; }

// A client's request may fail for various reasons. For example, if enough
// replicas are down, a client's request may time out. An ErrorCode indicates
// the reason that a client's request failed.
enum class ErrorCode {
    // For whatever reason (failed replicas, slow network), the request took
    // too long and timed out.
    TIMEOUT,

    // For IR, if a client issues a consensus operation and receives a majority
    // of replies and confirms in different views, then the operation fails.
    MISMATCHED_CONSENSUS_VIEWS
};

// TODO(mwhittaker): Need to decide not only on the status but also potentially
// the new timestamp if the status is RETRY.
using result_set_t = std::map<std::pair<int, Timestamp>, std::size_t>;
using decide_t = std::function<std::pair<int, Timestamp>(const result_set_t &)>;
using unlogged_continuation_t = std::function<void(char *respBuf)>;
using inconsistent_continuation_t = std::function<void()>;
using consensus_continuation_t =
    std::function<void(int decided_status, const Timestamp& timestamp)>;
using error_continuation_t =
    std::function<void(const string &request, ErrorCode err)>;


class IRClient : public TransportReceiver
{
public:
    // Constructor and destructor.
    IRClient(const transport::Configuration &config,
             Transport *transport,
             uint64_t clientid = 0);

    // API.
    // DO_NOT_SUBMIT(mwhittaker): I removed the core_id argument here. Does
    // that break everything?
    virtual void InvokeUnlogged(
        uint8_t core_id,
        int replica_index,
        const string &key,
        unlogged_continuation_t continuation,
        error_continuation_t error_continuation = nullptr,
        uint32_t timeout = 1000 /* milliseconds */);
    virtual void InvokeInconsistent(
        uint8_t core_id,
        uint64_t transaction_number,
        bool commit,
        inconsistent_continuation_t continuation,
        error_continuation_t error_continuation = nullptr);
    virtual void InvokeConsensus(
        uint8_t core_id,
        uint64_t transaction_number,
        const Transaction &txn,
        const Timestamp &timestamp,
        decide_t decide,
        consensus_continuation_t continuation,
        error_continuation_t error_continuation = nullptr);

protected:
    // TODO(mwhittaker): Implement timeouts. Meerkat doesn't implement
    // timeouts, so neither do we :).
    struct PendingUnloggedRequest {
        unlogged_continuation_t continuation;
        error_continuation_t error_continuation;
    };

    struct PendingInconsistentRequest {
        // The core id that the inconsistent request was sent to. We need to
        // know this because we'll send the finalize inconsistent message to
        // the same core.
        uint8_t core_id;

        // Callbacks.
        inconsistent_continuation_t continuation;
        error_continuation_t error_continuation;

        // QuorumSet is parameterized by an IDTYPE (typically viewstamp) and a
        // MSGTYPE (typically something like a Phase2b response). Here, we
        // don't have views. We also don't need to record any messages. So, we
        // plug in unit types for both parameters.
        QuorumSet<monostate, monostate> quorum_set;

        PendingInconsistentRequest(uint8_t core_id,
                                   inconsistent_continuation_t continuation,
                                   error_continuation_t error_continuation)
            : core_id(core_id),
              continuation(continuation),
              error_continuation(error_continuation) {}
    };

    // TODO(mwhittaker): Implement timeouts. Meerkat doesn't implement
    // timeouts, so neither do we :).
    struct PendingConsensusRequest {
        // The core id that the consensus request was sent to. We need to know
        // this because we'll send the finalize consensus message to the same
        // core.
        uint8_t core_id;

        // Callbacks.
        decide_t decide;
        consensus_continuation_t continuation;
        error_continuation_t error_continuation;

        // Reponses. See above for why monostate.
        QuorumSet<monostate, consensus_response_t> quorum_set;

        PendingConsensusRequest(decide_t decide,
                                consensus_continuation_t continuation,
                                error_continuation_t error_continuation)
            : decide(decide),
              continuation(continuation),
              error_continuation(error_continuation) {}
    };

    // TODO(mwhittaker): Implement timeouts. Meerkat doesn't implement
    // timeouts, so neither do we :).
    struct PendingSlowPath {
        // The final status and timestsamp of the request.
        int decided_status;
        Timestamp timestamp;

        // Callbacks.
        consensus_continuation_t continuation;
        error_continuation_t error_continuation;

        // Reponses.
        QuorumSet<monostate, monostate> quorum_set;

        PendingSlowPath(int decided_status,
                        Timestamp timestamp,
                        consensus_continuation_t continuation,
                        error_continuation_t error_continuation)
            : decided_status(decided_status),
              timestamp(timestamp),
              continuation(continuation),
              error_continuation(error_continuation) {}
    };

    // TransportReceiver interface.
    void ReceiveRequest(uint8_t reqType, char *reqBuf, char *respBuf) override {
        PPanic("Not implemented.");
    }
    void ReceiveResponse(uint8_t reqType, char *respBuf) override;
    bool Blocked() override {
        return blocked;
    };

    // Reply handlers.
    void HandleUnloggedReply(char *respBuf);
    void HandleInconsistentReply(char *respBuf);
    void HandleFinalizeInconsistentReply(char *respBuf);
    void HandleConsensusReply(char *respBuf);
    void HandleFinalizeConsensusReply(char *respBuf);

    transport::Configuration config;
    Transport *transport;

    // A globally unique client id.
    uint64_t clientid;

    // A monotonically increasing request number.
    uint64_t lastReqId;

    // Whether this client is currently blocked waiting for a response. For
    // example, after a client issues an inconsistent request, it is blocked
    // waiting for the reply.
    bool blocked;

    // Pending requests, keyed by request number.
    boost::unordered_map<uint64_t, PendingUnloggedRequest>
        pending_unlogged_requests;
    boost::unordered_map<uint64_t, PendingInconsistentRequest>
        pending_inconsistent_requests;
    boost::unordered_map<uint64_t, PendingConsensusRequest>
        pending_consensus_requests;
    boost::unordered_map<uint64_t, PendingSlowPath> pending_slow_paths;
};

} // namespace tapirir
} // namespace replication

#endif  // _TAPIRIR_CLIENT_H_
