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

#ifndef _MEERKATSTORE_LEADERMEERKATIR_SHARDCLIENT_H_
#define _MEERKATSTORE_LEADERMEERKATIR_SHARDCLIENT_H_

#include <memory>
#include <string>

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "replication/leadermeerkatir/client.h"
#include "store/common/frontend/txnclient.h"
#include "store/common/timestamp.h"
#include "store/common/transaction.h"
#include "store/meerkatstore/config.h"

namespace meerkatstore {
namespace leadermeerkatir {

class ShardClient : public TxnClient
{
public:
    // Constructor needs path to shard config.
    ShardClient(const transport::Configuration &config,
                  Transport *transport,
                  uint64_t client_id, int shard,
                  int closestReplica, bool replicated);

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
                Promise *promise = NULL) override { Panic("Not implemented!"); };
    void Abort(uint64_t txn_nr,
               uint8_t core_id,
               const Transaction &txn,
               Promise *promise = NULL) override { Panic("Not implemented!"); };

private:
    transport::Configuration config;
    uint64_t client_id; // Unique ID for this client.
    int shard; // which shard this client accesses
    int replica; // which replica to use for reads

    replication::leadermeerkatir::Client *client; // Client proxy.
    Promise *waiting; // waiting thread

    // Callbacks for hearing back from a shard for an operation.
    void GetCallback(char *respBuf);
    void PrepareCallback(int status);

    // A timeout that gives up on a request and returns an error to the user.
    void GiveUpTimeout();
};

} // namespace leadermeerkatir
} // namespace meerkatstore

#endif /* _MEERKATSTORE_LEADERMEERKATIR_SHARDCLIENT_H_ */
