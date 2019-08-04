// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * replication/ir/client.h:
 *   Inconsistent replication client
 *
 * Copyright 2013-2015 Dan R. K. Ports  <drkp@cs.washington.edu>
 *                     Irene Zhang Ports  <iyzhang@cs.washington.edu>
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

#ifndef _IR_CLIENT_H_
#define _IR_CLIENT_H_

#include "replication/common/quorumset.h"
#include "lib/transport.h"
#include "lib/configuration.h"
#include "store/common/transaction.h"

#include <functional>
#include <map>
#include <memory>
#include <set>
#include <unordered_map>

namespace replication {
namespace ir {

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

using result_set_t = std::map<int, std::size_t>;
using decide_t = std::function<int(const result_set_t &)>;
using unlogged_continuation_t =
    std::function<void(char *respBuf)>;
using inconsistent_continuation_t =
    std::function<void(char *respBuf)>;
using consensus_continuation_t =
    std::function<void(int decidedStatus)>;
using continuation_t =
    std::function<void(const string &request, const string &reply)>;
using error_continuation_t =
    std::function<void(const string &request, ErrorCode err)>;


class IRClient : public TransportReceiver
{
public:
    static const uint32_t DEFAULT_UNLOGGED_OP_TIMEOUT = 1000; // milliseconds

    IRClient(const transport::Configuration &config,
             Transport *transport,
             uint64_t clientid = 0);
    virtual ~IRClient();

    virtual void InvokeUnlogged(
        uint64_t txn_nr,
        uint8_t core_id,
        int replicaIdx,
        const string &request,
        unlogged_continuation_t continuation,
        error_continuation_t error_continuation = nullptr,
        uint32_t timeout = DEFAULT_UNLOGGED_OP_TIMEOUT);
    virtual void InvokeInconsistent(
        uint64_t txn_nr,
        uint8_t core_id,
        bool commit,
        inconsistent_continuation_t continuation,
        error_continuation_t error_continuation = nullptr);
    virtual void InvokeConsensus(
        uint64_t txn_nr,
        uint8_t core_id,
        const Transaction &txn,
        const Timestamp &timestamp,
        decide_t decide,
        consensus_continuation_t continuation,
        error_continuation_t error_continuation = nullptr);
    void ReceiveRequest(uint8_t reqType, char *reqBuf, char *respBuf) override {};
    void ReceiveResponse(uint8_t reqType, char *respBuf, bool &unblock) override;
protected:
    struct PendingRequest {
        string request;
        uint64_t clientReqId;
        uint64_t clienttxn_nr;
        uint8_t core_id;
        continuation_t continuation;
        bool continuationInvoked = false;
        std::unique_ptr<Timeout> timer;
        QuorumSet<viewstamp_t, finalize_consensus_response_t> confirmQuorum;

        inline PendingRequest(string request, uint64_t clientReqId,
                              uint64_t clienttxn_nr, uint8_t core_id,
                              continuation_t continuation,
                              std::unique_ptr<Timeout> timer, int quorumSize)
            : request(request),
              clientReqId(clientReqId),
              clienttxn_nr(clienttxn_nr),
              core_id(core_id),
              continuation(continuation),
              timer(std::move(timer)),
              confirmQuorum(quorumSize){};
        virtual ~PendingRequest(){};
    };

    struct PendingUnloggedRequest : public PendingRequest {
        error_continuation_t error_continuation;
        unlogged_continuation_t get_continuation;

        inline PendingUnloggedRequest(
            string request, uint64_t clientReqId, uint64_t clienttxn_nr,
            uint8_t core_id,
            unlogged_continuation_t get_continuation,
            error_continuation_t error_continuation,
            std::unique_ptr<Timeout> timer)
            : PendingRequest(request, clientReqId, clienttxn_nr, core_id, nullptr,
                             std::move(timer), 1),
              error_continuation(error_continuation),
              get_continuation(get_continuation){};
    };

    struct PendingInconsistentRequest : public PendingRequest {
        inconsistent_continuation_t inconsistent_continuation;
        QuorumSet<viewstamp_t, inconsistent_response_t> inconsistentReplyQuorum;

        inline PendingInconsistentRequest(uint64_t clientReqId,
                                          uint64_t clienttxn_nr, uint8_t core_id,
                                          inconsistent_continuation_t inconsistent_continuation,
                                          std::unique_ptr<Timeout> timer,
                                          int quorumSize)
            : PendingRequest("", clientReqId, clienttxn_nr, core_id, nullptr,
                             std::move(timer), quorumSize),
              inconsistent_continuation(inconsistent_continuation),
              inconsistentReplyQuorum(quorumSize){};
    };

    struct PendingConsensusRequest : public PendingRequest {
        QuorumSet<opnum_t, consensus_response_t> consensusReplyQuorum;
        decide_t decide;
        //string decideResult;
        int decidedStatus;
        const std::size_t quorumSize;
        const std::size_t superQuorumSize;
        bool on_slow_path;
        error_continuation_t error_continuation;
        consensus_continuation_t consensus_continuation;

        // The timer to give up on the fast path and transition to the slow
        // path. After this timer is run for the first time, it is nulled.
        std::unique_ptr<Timeout> transition_to_slow_path_timer;

        // The view for which a majority result (or finalized result) was
        // found. The view of a majority of confirms must match this view.
        uint64_t reply_consensus_view = 0;

        // True when a consensus request has already received a quorum or super
        // quorum of replies and has already transitioned into the confirm
        // phase.
        bool sent_confirms = false;

        inline PendingConsensusRequest(
            uint64_t clientReqId, uint64_t clienttxn_nr,
            uint8_t core_id,
            consensus_continuation_t consensus_continuation,
            std::unique_ptr<Timeout> timer,
            std::unique_ptr<Timeout> transition_to_slow_path_timer,
            int quorumSize, int superQuorum, decide_t decide,
            error_continuation_t error_continuation)
            : PendingRequest("", clientReqId, clienttxn_nr, core_id, nullptr,
                             std::move(timer), quorumSize),
              consensusReplyQuorum(quorumSize),
              decide(decide),
              quorumSize(quorumSize),
              superQuorumSize(superQuorum),
              on_slow_path(false),
              error_continuation(error_continuation),
              consensus_continuation(consensus_continuation),
              transition_to_slow_path_timer(
                  std::move(transition_to_slow_path_timer)){};
    };

    transport::Configuration config;
    uint64_t lastReqId;
    std::unordered_map<uint64_t, PendingRequest *> pendingReqs;
    Transport *transport;
    uint64_t clientid;

    // `TransitionToConsensusSlowPath` is called after a timeout to end the
    // possibility of taking the fast path and transition into taking the slow
    // path.
    void TransitionToConsensusSlowPath(const uint64_t reqId);

    // HandleSlowPathConsensus is called in one of two scenarios:
    //
    //   1. A finalized ReplyConsensusMessage was received. In this case, we
    //      immediately enter the slow path and use the finalized result. If
    //      finalized is true, req has already been populated with the
    //      finalized result.
    //   2. We're in the slow path and receive a majority of
    //      ReplyConsensusMessages in the same view. In this case, we call
    //      decide to determine the final result.
    //
    // In either case, HandleSlowPathConsensus intitiates the finalize phase of
    // a consensus request.
    void HandleSlowPathConsensus(
        const uint64_t reqid,
        const std::map<int, consensus_response_t> &msgs,
        const bool finalized_result_found,
        PendingConsensusRequest *req);

    // HandleFastPathConsensus is called when we're on the fast path and
    // receive a super quorum of responses from the same view.
    // HandleFastPathConsensus will check to see if there is a superquorum of
    // matching responses. If there is, it will return to the user and
    // asynchronously intitiate the finalize phase of a consensus request.
    // Otherwise, it transitions into the slow path which will also initiate
    // the finalize phase of a consensus request, but not yet return to the
    // user.
    void HandleFastPathConsensus(
        const uint64_t reqid,
        const std::map<int, consensus_response_t> &msgs,
        PendingConsensusRequest *req,
        bool &unblock);

    void ResendConsensusRequest(const uint64_t reqId);
    void ResendFinalizeConsensusRequest(const uint64_t reqId, bool isConsensus);

    void UnloggedRequestTimeoutCallback(const uint64_t reqId);

    // new handlers
    void HandleInconsistentReply(char *respBuf, bool &unblock);
    void HandleUnloggedReply(char *respBuf, bool &unblock);
    void HandleConsensusReply(char *respBuf, bool &unblock);
    void HandleFinalizeConsensusReply(char *respBuf, bool &unblock);
};

} // namespace replication::ir
} // namespace replication

#endif  /* _IR_CLIENT_H_ */
