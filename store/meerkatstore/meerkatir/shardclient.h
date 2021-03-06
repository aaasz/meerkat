// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/tapir/shardclient.h:
 *   Single shard tapir transactional client interface.
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

#ifndef _MEERKATSTORE_SHARDCLIENT_H_
#define _MEERKATSTORE_SHARDCLIENT_H_

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "replication/meerkatir/client.h"
#include "store/common/timestamp.h"
#include "store/common/transaction.h"
#include "store/common/frontend/txnclient.h"

#include <map>
#include <string>

namespace meerkatstore {

class ShardClient : public TxnClient
{
public:
    /* Constructor needs path to shard config. */
    ShardClient(const transport::Configuration &config,
        Transport *transport,
        uint64_t client_id,
        int shard,
        int closestReplica,
        bool replicated);
    ~ShardClient();

    // Overriding from TxnClient
    void Begin(uint64_t txn_nr) override;
    void Get(uint64_t txn_nr,
             uint8_t core_id,
             const std::string &key,
             Promise *promise = NULL) override;
    void Prepare(uint64_t txn_nr,
                 uint8_t core_id,
                 const Transaction &txn,
                 const Timestamp &timestamp = Timestamp(),
                 Promise *promise = NULL) override;
    void Commit(uint64_t txn_nr,
                uint8_t core_id,
                const Transaction &txn,
                const Timestamp &timestamp = Timestamp(),
                Promise *promise = NULL) override;
    void Abort(uint64_t txn_nr,
               uint8_t core_id,
               const Transaction &txn,
               Promise *promise = NULL) override;

private:
    transport::Configuration config;
    uint64_t client_id; // Unique ID for this client.
    Transport *transport; // Transport layer (shared by multiple client fibers).
    int shard; // which shard this client accesses
    int replica; // which replica to use for reads
    bool replicated; // Is the database replicated?

    replication::meerkatir::Client *client; // Client proxy.
    Promise *waiting; // waiting thread
    Promise *blockingBegin; // don't start a new transaction until current one
                            // until finished (limitation on transport --
                            // can't have more than one outstanding req,
                            // cause it uses the same send buffer)

    void SendUnreplicated(uint64_t txn_nr,
                          uint8_t core_id,
                          Promise *promise, const std::string &request_str,
                          replication::meerkatir::unlogged_continuation_t callback,
                          replication::meerkatir::error_continuation_t error_callback);
    void SendInconsistent(uint64_t txn_nr,
                          uint8_t core_id,
                          bool commit,
                          replication::meerkatir::inconsistent_continuation_t callback,
                          replication::meerkatir::error_continuation_t error_callback);
    void SendConsensus(uint64_t txn_nr,
                       uint8_t core_id,
                       Promise *promise,
                       const Transaction &txn,
                       const Timestamp &timestamp,
                       replication::meerkatir::decide_t decide,
                       replication::meerkatir::consensus_continuation_t callback,
                       replication::meerkatir::error_continuation_t error_callback);

    /* Meerkat's Decide Function. */
    int MeerkatDecide(const boost::unordered_map<int, std::size_t> &results);

    /* Timeout for Get requests, which only go to one replica. */
    void GetTimeout();
    /* Timeout for all the other requests that go to one
     * replica when replicated is false */
    void GiveUpTimeout();

    /* Callbacks for hearing back from a shard for an operation. */
    void GetCallback(char *respBuf);
    void PrepareCallback(int decidedStatus);
    void CommitCallback(char *respBuf);

    /* Helper Functions for starting and finishing requests */
    void StartRequest();
    void WaitForResponse();
    void FinishRequest(const std::string &reply_str);
    void FinishRequest();
    int SendGet(const std::string &request_str);
};

} // namespace meerkatstore

#endif /* _MEERKATSTORE_SHARDCLIENT_H_ */
