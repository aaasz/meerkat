// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/tapirstore/shardclient.cc:
 *   Single shard tapir transactional client.
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

#include "store/multitapirstore/shardclient.h"

#include <sys/time.h>

namespace multitapirstore {

using namespace std;
using namespace proto;

struct timeval t0, t1;

namespace {

void ReplyToPromise(const Reply &reply, Promise *promise) {
    if (reply.has_value() && reply.has_timestamp()) {
        promise->Reply(reply.status(), reply.timestamp(), reply.value());
    } else if (reply.has_value()) {
        promise->Reply(reply.status(), reply.value());
    } else if (reply.has_timestamp()) {
        promise->Reply(reply.status(), Timestamp(reply.timestamp()));
    } else {
        promise->Reply(reply.status());
    }
}

}  // namespace


/*******************************************************
 IR Client calls
 *******************************************************/

ShardClientIR::ShardClientIR(const string &configPath,
                       Transport *transport, uint64_t client_id, int
                       shard, int closestReplica, bool replicated)
    : client_id(client_id), transport(transport), shard(shard), replicated(replicated)
{
    ifstream configStream(configPath);
    if (configStream.fail()) {
        Panic("Unable to read configuration file: %s\n", configPath.c_str());
    }

    transport::Configuration config(configStream);
    this->config = &config;

    client = new replication::ir::IRClient(config, transport, client_id);

    if (closestReplica == -1) {
        replica = client_id % config.n;
    } else {
        replica = closestReplica;
    }
    Debug("Sending unlogged to replica %i", replica);

    waiting = NULL;
    blockingBegin = NULL;
}

ShardClientIR::~ShardClientIR()
{
    delete client;
}

void
ShardClientIR::Begin(uint64_t txn_nr)
{
    Debug("[shard %i] BEGIN: %lu", shard, txn_nr);
}

void ShardClientIR::SendUnreplicated(uint64_t txn_nr,
                                     uint32_t core_id,
                                     Promise *promise,
                                     const std::string &request_str,
                                     replication::unlogged_continuation_t callback,
                                     replication::error_continuation_t error_callback) {

    Debug("Sending unlogged request to replica %d.", replica);
    const int timeout = (promise != nullptr) ? promise->GetTimeout() : 1000;
    waiting = promise;
    client->InvokeUnlogged(txn_nr, core_id, replica, request_str, callback,
                           error_callback, timeout);
}

void ShardClientIR::SendConsensus(uint64_t txn_nr, uint32_t core_id, Promise *promise,
                                  const Transaction &txn, const Timestamp &timestamp,
                                  replication::ir::decide_t decide,
                                  replication::consensus_continuation_t callback,
                                  replication::error_continuation_t error_callback) {

    Debug("Sending consensus request to replica %d.", replica);
    waiting = promise;
    client->InvokeConsensus(txn_nr, core_id, txn, timestamp, decide,
                            callback, error_callback);
}

void ShardClientIR::SendInconsistent(uint64_t txn_nr, uint32_t core_id,
                                     bool commit,
                                     replication::inconsistent_continuation_t callback,
                                     replication::error_continuation_t error_callback) {

    client->InvokeInconsistent(txn_nr, core_id, commit,
                               callback, error_callback);
}

void
ShardClientIR::Get(uint64_t txn_nr, const string &key, Promise *promise)
{
    // Send the GET operation to appropriate shard.
    Panic("Not implemented!");
}

void
ShardClientIR::Get(uint64_t txn_nr, uint32_t core_id,
                   const string &key, Promise *promise)
{
    // Send the GET operation to appropriate shard.
    Debug("[shard %i] Sending GET [%lu : %s]", shard, txn_nr, key.c_str());

    SendUnreplicated(txn_nr, core_id, promise, key,
      bind(&ShardClientIR::GetCallback, this,
           placeholders::_1),
      bind(&ShardClientIR::GetTimeout, this));
}

void
ShardClientIR::Get(uint64_t txn_nr, const string &key,
                   const Timestamp &timestamp, Promise *promise)
{
    // Send the GET operation to appropriate shard.
    Panic("Not implemented!");
}

void
ShardClientIR::Put(uint64_t txn_nr,
               const string &key,
               const string &value,
               Promise *promise)
{
    Panic("Not implemented!");
}

void
ShardClientIR::Prepare(uint64_t txn_nr,
                       const Transaction &txn,
                       const Timestamp &timestamp, Promise *promise)
{
    Panic("Not implemented!");
}


void
ShardClientIR::Prepare(uint64_t txn_nr,
                       uint32_t core_id, const Transaction &txn,
                       const Timestamp &timestamp, Promise *promise)
{
    Debug("[shard %i] Sending PREPARE [%lu]", shard, txn_nr);

    SendConsensus(txn_nr, core_id, promise, txn, timestamp,
          bind(&ShardClientIR::MultiTapirDecide, this,
               placeholders::_1),
          bind(&ShardClientIR::PrepareCallback, this,
               placeholders::_1), nullptr);
}

int ShardClientIR::MultiTapirDecide(const std::map<int, std::size_t> &results) {
    // TODO: re-introduce the retry?

    // If a majority say prepare_ok,
    int ok_count = 0;
    // Timestamp ts = 0;
    // string final_reply_str;
    // Reply final_reply;

    for (const auto& r : results) {
        const int status = r.first;
        const std::size_t count = r.second;

        // Reply reply;
        //reply.ParseFromString(s);

        if (status == REPLY_OK) {
            ok_count += count;
        } else if (status == REPLY_FAIL) {
            return REPLY_FAIL;
        //} else if (reply.status() == REPLY_RETRY) {
        //    Timestamp t(reply.timestamp());
        //    if (t > ts) {
        //        ts = t;
        //    }
        }
    }

    ASSERT(ok_count >= config->QuorumSize());
    // {
    //    final_reply.set_status(REPLY_OK);
    // } else {
    //     final_reply.set_status(REPLY_RETRY);
    //     ts.serialize(final_reply.mutable_timestamp());
    // }
    // final_reply.SerializeToString(&final_reply_str);
    // return final_reply_str;
    return REPLY_OK;
}

void
ShardClientIR::Commit(uint64_t txn_nr,
                      const Transaction &txn,
                      const Timestamp &timestamp, Promise *promise)
{
    Panic("Not implemented!");
}

void
ShardClientIR::Commit(uint64_t txn_nr, uint32_t core_id,
                      const Transaction &txn,
                      const Timestamp &timestamp, Promise *promise)
{

    Debug("[shard %i] Sending COMMIT [%lu]", shard, txn_nr);

    SendInconsistent(txn_nr, core_id, true,
          bind(&ShardClientIR::CommitCallback, this,
               placeholders::_1), nullptr);
}

void
ShardClientIR::Abort(uint64_t txn_nr,
                     const Transaction &txn, Promise *promise)
{
    Panic("Not implemented!");
}

void
ShardClientIR::Abort(uint64_t txn_nr, uint32_t core_id,
                     const Transaction &txn, Promise *promise)
{
    Debug("[shard %i] Sending ABORT [%lu]", shard, txn_nr);

    SendInconsistent(txn_nr, core_id, false,
          bind(&ShardClientIR::CommitCallback, this,
               placeholders::_1), nullptr);
}

void
ShardClientIR::GetTimeout()
{
    if (waiting != NULL) {
        Promise *w = waiting;
        waiting = NULL;
        w->Reply(REPLY_TIMEOUT);
    }
}

void
ShardClientIR::GiveUpTimeout() {
    Debug("GiveupTimeout called.");
    if (waiting != nullptr) {
        Promise *w = waiting;
        waiting = nullptr;
        w->Reply(REPLY_TIMEOUT);
    }
}

/* Callback from a shard replica on get operation completion. */
void ShardClientIR::GetCallback(char *respBuf) {
    /* Replies back from a replica. */
    auto *resp = reinterpret_cast<unlogged_response_t *>(respBuf);

    // Debug("[shard %lu:%i] GET callback [%d]", client_id, shard, reply.status());
    if (waiting != NULL) {
        Promise *w = waiting;
        waiting = NULL;
        w->Reply(resp->status, Timestamp(resp->timestamp, resp->id), std::string(resp->value, 64));
    } else {
        Warning("Waiting is null!");
    }
}

/* Callback from a shard replica on prepare operation completion. */
void ShardClientIR::PrepareCallback(int decidedStatus) {
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

/* Callback from a shard replica on commit operation completion. */
void ShardClientIR::CommitCallback(char *respBuf) {
    // COMMITs always succeed.
    Debug("[shard %lu:%i] COMMIT callback", client_id, shard);

    if (waiting != NULL) {
        waiting = NULL;
    }
}

} // namespace multitapirstore
