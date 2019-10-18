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

#ifndef _TAPIRSTORE_SHARDCLIENT_H_
#define _TAPIRSTORE_SHARDCLIENT_H_

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "replication/tapirir/client.h"
#include "store/common/timestamp.h"
#include "store/common/transaction.h"
#include "store/common/frontend/txnclient.h"

#include <map>
#include <string>

namespace tapirstore {

class ShardClient : public TxnClient
{
public:
    /* Constructor needs path to shard config. */
    ShardClient(const transport::Configuration &config,
                Transport *transport,
                uint64_t client_id,
                int shard,
                int closestReplica);

    // TxnClient interface.
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
                const Transaction &txn = Transaction(),
                const Timestamp &timestamp = Timestamp(),
                Promise *promise = NULL) override;
    void Abort(uint64_t txn_nr,
               uint8_t core_id,
               const Transaction &txn = Transaction(),
               Promise *promise = NULL) override;

private:

    // Callbacks for hearing back from a shard for an operation.
    void GetCallback(char *response_buffer);
    void PrepareCallback(int decided_status);
    void CommitCallback();
    void AbortCallback();

    // Decide function.
    std::pair<int, Timestamp> TapirDecide(
            const std::map<std::pair<int, Timestamp>, std::size_t>& results);

    // Error timeouts.
    void GetTimeout();

    transport::Configuration config;
    int shard; // which shard this client accesses
    int closest_replica; // which replica to use for reads
    std::unique_ptr<replication::tapirir::IRClient> client; // Client proxy.
    Promise *waiting; // waiting thread
};

} // namespace tapirstore

#endif /* _TAPIRSTORE_SHARDCLIENT_H_ */
