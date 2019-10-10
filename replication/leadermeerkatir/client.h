// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * replication/pb/client.h:
 *   dummy implementation of replication interface that just uses a
 *   single replica and passes updates directly to it
 *
 * Copyright 2013 Dan R. K. Ports  <drkp@cs.washington.edu>
 *           2018 Adriana Szekeres  <aaasz@cs.washington.edu>
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

#ifndef _LEADERMEERKATIR_CLIENT_H_
#define _LEADERMEERKATIR_CLIENT_H_

#include "lib/configuration.h"
#include "lib/fasttransport.h"
#include "store/common/transaction.h"
#include "replication/leadermeerkatir/messages.h"

#include <functional>
#include <map>
#include <memory>
#include <set>
#include <unordered_map>
#include <boost/functional/hash.hpp>
#include <boost/unordered_map.hpp>

namespace replication {
namespace leadermeerkatir {

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

using continuation_t =
    std::function<void(int status)>;
using unlogged_continuation_t =
    std::function<void(char *respBuf)>;
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
    virtual void Invoke(uint64_t txn_nr, uint32_t core_id, const Transaction &txn,
                        continuation_t continuation,
                        error_continuation_t error_continuation = nullptr);
    virtual void InvokeUnlogged(uint64_t txn_nr, uint32_t core_id, int replicaIdx,
                                const string &request,
                                unlogged_continuation_t continuation,
                                error_continuation_t error_continuation = nullptr,
                                uint32_t timeout = DEFAULT_UNLOGGED_OP_TIMEOUT);
    void ReceiveRequest(uint8_t reqType, char *reqBuf, char *respBuf) override { PPanic("Not implemented."); };
    void ReceiveResponse(uint8_t reqType, char *respBuf) override;
    bool Blocked() override { return blocked; };

protected:
    transport::Configuration config;
    uint64_t lastReqId;
    Transport *transport;
    uint64_t clientid;
    bool blocked;

    struct PendingRequest
    {
        uint64_t clientReqId;
        uint64_t clienttxn_nr;
        uint32_t core_id;
        continuation_t continuation;
        // std::unique_ptr<Timeout> timer;

        inline PendingRequest() {};
        inline PendingRequest(uint64_t clientReqId,
                              uint64_t clienttxn_nr,
                              uint32_t core_id,
                              continuation_t continuation)
                              //std::unique_ptr<Timeout> timer)
            : clientReqId(clientReqId),
              clienttxn_nr(clienttxn_nr),
              core_id(core_id),
              continuation(continuation) { };
              // timer(std::move(timer)) { };
        virtual ~PendingRequest(){};
    };

    struct PendingUnloggedRequest : public PendingRequest
    {
        unlogged_continuation_t unlogged_request_continuation;
        error_continuation_t error_continuation;

        inline PendingUnloggedRequest() {};
        inline PendingUnloggedRequest(uint64_t clientReqId,
                                      uint64_t clienttxn_nr,
                                      uint32_t core_id,
                                      unlogged_continuation_t continuation,
                                      // std::unique_ptr<Timeout> timer,
                                      error_continuation_t error_continuation)
            : PendingRequest(clientReqId, clienttxn_nr, core_id, nullptr), //, std::move(timer)),
              unlogged_request_continuation(continuation),
              error_continuation(error_continuation) { };
    };

    boost::unordered_map<uint64_t, PendingRequest> pendingReqs;
    boost::unordered_map<uint64_t, PendingUnloggedRequest> pendingUnloggedReqs;

    void HandleReply(char *respBuf);
    void HandleUnloggedReply(char *respBuf);
};

} // namespace leadermeerkatir
} // namespace replication

#endif  /* _LEADERMEERKATIR_CLIENT_H_ */
