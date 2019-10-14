// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/tapirstore/shardclient.cc:
 *   Single shard tapir transactional client.
 *
 * Copyright 2015 Irene Zhang <iyzhang@cs.washington.edu>
 *                Naveen Kr. Sharma <naveenks@cs.washington.edu>
 * Copyright 2018 Michael Whittaker <mjwhittaker@berkeley.edu>
 *                Adriana Szekeres <aaasz@cs.washington.edu>
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

#include "store/meerkatstore/leadermeerkatir/shardclient.h"

namespace meerkatstore {
namespace leadermeerkatir {

using namespace std;
using placeholders::_1;
using placeholders::_2;

ShardClient::ShardClient(const transport::Configuration &config, Transport *transport,
                             uint64_t client_id, int shard, int closestReplica,
                             bool replicated)
    : config(config),
      client_id(client_id),
      shard(shard),
      waiting(nullptr) {

    client = new replication::leadermeerkatir::Client(config, transport, client_id);

    if (closestReplica == -1) {
        replica = client_id % config.n;
    } else {
        replica = closestReplica;
    }
    Debug("Sending unlogged requests to replica %i.", replica);
}

void ShardClient::Begin(uint64_t txn_nr) {
    Debug("LeaderMeerkatIR shard client beginning transaction %lu for shard %d.", txn_nr,
          shard);
}

void ShardClient::Get(uint64_t txn_nr,
                         uint8_t core_id,
                         const string &key,
                         Promise *promise) {
    Debug("Shard client getting key %s for transaction %lu for shard %d.",
          key.c_str(), txn_nr, shard);
    const int timeout = (promise != nullptr) ? promise->GetTimeout() : 1000;
    waiting = promise;
    client->InvokeUnlogged(txn_nr, core_id, replica, key,
                bind(&ShardClient::GetCallback, this,
                        placeholders::_1),
                bind(&ShardClient::GiveUpTimeout, this),
                timeout);
}

void ShardClient::Prepare(uint64_t txn_nr,
                             uint8_t core_id,
                             const Transaction &txn,
                             const Timestamp &timestamp,
                             Promise *promise) {
    Debug("Shard client prepare transaction %lu for shard %d.", txn_nr, shard);
    waiting = promise;
    client->Invoke(txn_nr, core_id, timestamp, txn,
                bind(&ShardClient::PrepareCallback, this,
                        placeholders::_1),
                bind(&ShardClient::GiveUpTimeout, this));
}

void ShardClient::GiveUpTimeout() {
    Debug("GiveupTimeout called.");
    if (waiting != nullptr) {
        Promise *w = waiting;
        waiting = nullptr;
        w->Reply(REPLY_TIMEOUT);
    }
}

void ShardClient::GetCallback(char *respBuf) {
   /* Replies back from a replica. */
    auto *resp = reinterpret_cast<replication::leadermeerkatir::unlogged_response_t *>(respBuf);

    // Debug("[shard %lu:%i] GET callback [%d]", client_id, shard, reply.status());
    if (waiting != NULL) {
        Promise *w = waiting;
        waiting = NULL;
        w->Reply(resp->status, Timestamp(resp->timestamp, resp->id), std::string(resp->value, 64));
    } else {
        Warning("Waiting is null!");
    }
}

void ShardClient::PrepareCallback(int decidedStatus) {
    Debug("[shard %lu:%i] PREPARE callback [%d]", client_id, shard, decidedStatus);

    if (waiting != NULL) {
        Promise *w = waiting;
        waiting = NULL;
        // TODO: for now no optimization with RETRY
        //if (reply.has_timestamp()) {
        //    w->Reply(reply.status(), Timestamp(reply.timestamp()));
        //} else {
            w->Reply(decidedStatus, Timestamp());
        //}
    }
}

} // namespace leadermeerkatir
} // namespace meerkatstore
