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

#include "replication/tapirir/messages.h"
#include "store/tapirstore/shardclient.h"

namespace tapirstore {

ShardClient::ShardClient(const transport::Configuration &config,
                         Transport *transport, uint64_t client_id, int shard,
                         int closestReplica)
    : config(config),
      shard(shard),
      closest_replica(closestReplica == -1 ?
                      client_id % config.n :
                      closestReplica),
      client(std::unique_ptr<replication::tapirir::IRClient>(
            new replication::tapirir::IRClient(config, transport, client_id))),
      waiting(nullptr) { }

// TxnClient API ///////////////////////////////////////////////////////////////
void
ShardClient::Begin(uint64_t txn_nr)
{
    Debug("[shard %i] BEGIN(txn_nr=%lu)", shard, txn_nr);
}

void
ShardClient::Get(uint64_t txn_nr, uint8_t core_id, const std::string &key,
                 Promise *promise)
{
    Debug("[shard %i] GET(txn_nr=%lu, key=%s)", shard, txn_nr, key.c_str());

    using namespace std::placeholders;
    waiting = promise;
    client->InvokeUnlogged(
        /*core_id=*/core_id,
        /*replica_index=*/closest_replica,
        /*key=*/key,
        /*continuation=*/std::bind(&ShardClient::GetCallback, this, _1),
        /*error_continuation=*/std::bind(&ShardClient::GetTimeout, this),
        /*timeout=*/(promise != NULL) ? promise->GetTimeout() : 1000);
}

void
ShardClient::Prepare(uint64_t txn_nr, uint8_t core_id,
                       const Transaction &txn, const Timestamp &timestamp,
                       Promise *promise)
{
    Debug("[shard %i] PREPARE(%lu)", shard, txn_nr);

    using namespace std::placeholders;
    waiting = promise;
    client->InvokeConsensus(
        /*core_id=*/core_id,
        /*transaction_number=*/txn_nr,
        /*txn=*/txn,
        /*timestamp=*/timestamp,
        /*decide=*/std::bind(&ShardClient::TapirDecide, this, _1),
        /*continuation=*/std::bind(&ShardClient::PrepareCallback, this, _1),
        /*error_continuation=*/nullptr);
}

void
ShardClient::Commit(uint64_t txn_nr, uint8_t core_id, const Transaction &txn,
                      const Timestamp &timestamp, Promise *promise)
{
    Debug("[shard %i] COMMIT(txn_nr=%lu)", shard, txn_nr);

    using namespace std::placeholders;
    waiting = promise;
    client->InvokeInconsistent(
        /*core_id=*/core_id,
        /*transaction_number=*/txn_nr,
        /*commit=*/true,
        /*continuation=*/std::bind(&ShardClient::CommitCallback, this),
        /*error_continuation=*/nullptr);
}

void
ShardClient::Abort(uint64_t txn_nr, uint8_t core_id, const Transaction &txn,
                   Promise *promise)
{
    Debug("[shard %i] ABORT(txn_nr=%lu)", shard, txn_nr);

    using namespace std::placeholders;
    waiting = promise;
    client->InvokeInconsistent(
        /*core_id=*/core_id,
        /*transaction_number=*/txn_nr,
        /*commit=*/false,
        /*continuation=*/std::bind(&ShardClient::AbortCallback, this),
        /*error_continuation=*/nullptr);
}

// Callbacks ///////////////////////////////////////////////////////////////////
void
ShardClient::GetCallback(char *response_buffer)
{
    auto *response =
        reinterpret_cast<replication::tapirir::unlogged_response_t *>(
            response_buffer);
    if (waiting == nullptr) {
        Warning("waiting is null!");
    } else {
        Promise *w = waiting;
        waiting = nullptr;
        w->Reply(response->status,
                 Timestamp(response->timestamp.timestamp,
                           response->timestamp.client_id),
                 std::string(response->value, 64));
    }

}
void
ShardClient::GetTimeout()
{
    if (waiting != nullptr) {
        Promise *w = waiting;
        waiting = nullptr;
        w->Reply(REPLY_TIMEOUT);
    }
}

std::pair<int, Timestamp>
ShardClient::TapirDecide(
        const std::map<std::pair<int, Timestamp>, std::size_t>& results)
{
    // TODO(mwhittaker): We implement the decide function like meerkat does,
    // not like TAPIR does.

    Timestamp timestamp;
    int ok_count = 0;

    for (const auto& result_and_count : results) {
        const std::pair<int, Timestamp> result = result_and_count.first;
        const std::size_t count = result_and_count.second;

        if (result.first == REPLY_OK) {
            ok_count += count;
        } else if (result.first == REPLY_FAIL) {
            return {REPLY_FAIL, Timestamp()};
        } else if (result.first == REPLY_RETRY) {
            if (result.second > timestamp) {
                timestamp = result.second;
            }
        }
    }

    if (ok_count >= config.QuorumSize()) {
        return {REPLY_OK, Timestamp()};
    } else {
        return {REPLY_RETRY, timestamp};
    }
}

void
ShardClient::PrepareCallback(int decided_status)
{
    if (waiting == nullptr) {
        Warning("waiting is null");
    } else {
        Promise *w = waiting;
        waiting = NULL;
        // TODO: As with meerkat, we don't do retries.
        w->Reply(decided_status, Timestamp());
    }
}

void
ShardClient::CommitCallback()
{
    // Commits always succeed.
    if (waiting != nullptr) {
        waiting = nullptr;
    }
}

void
ShardClient::AbortCallback()
{
    // Abort callback.
    if (waiting != nullptr) {
        waiting = nullptr;
    }
}

} // namespace tapir
