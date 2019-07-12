// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/tapirstore/client.cc:
 *   Client to TAPIR transactional storage system.
 *
 * Copyright 2015 Irene Zhang <iyzhang@cs.washington.edu>
 *                Naveen Kr. Sharma <naveenks@cs.washington.edu>
 *           2018 Adriana Szekeres <aaasz@cs.washington.edu>
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

#include "store/multitapirstore/client.h"

#include <random>
#include <list>
#include <limits.h>
#include <thread>

namespace multitapirstore {

using namespace std;

void client_thread_func(std::string replScheme, Transport *transport) {
    ((FastTransport *)transport)->Run();
}

Client::Client(const string configFile, int nsthreads, int nShards,
               int closestReplica,
               bool twopc, bool replicated, TrueTime timeServer,
               const string replScheme)
    : t_id(0), core_id(0), nsthreads(nsthreads), nshards(nShards), replicated(replicated), twopc(twopc),
      timeServer(timeServer), replScheme(replScheme), core_dis(0, nsthreads -1)
{
    // Initialize all state here;
    srand(time(NULL));

    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dis(1, ULLONG_MAX);

    while (client_id == 0) {
        client_id = dis(gen);
    }

    // Standard mersenne_twister_engine seeded with rd()
    core_gen = std::mt19937(rd());

    bclient.reserve(nshards);

    Debug("Initializing Tapir client with id [%lu] %lu", client_id, nshards);

    /* Start a client for each shard. */

    shard_clients.reserve(nshards);
    bclient.reserve(nshards);
    // for (uint64_t i = 0; i < nshards; i++) {
    //     string shardConfigPath = configPath + to_string(i) + ".config";
    //TODO: Assume a single shard for now!
    if (replScheme == "ir") {
        // TODO: this is hardcoded
        transport = new FastTransport("10.100.1.16:31850", nsthreads, 4);
        shard_clients.push_back(std::unique_ptr<TxnClient>(
          new ShardClientIR(configFile, transport, client_id, 0,
                                 closestReplica, replicated)));
// //        } else if (replScheme == "lir") {
// //        	transport = new UDPSTransport(nsthreads, 0.0, 0.0);
// //            shard_clients.push_back(std::unique_ptr<TxnClient>(
// //              new ShardClientLIR(shardConfigPath, transport, client_id, i,
// //                                closestReplica, replicated)));
//         } else if (replScheme == "vr") {
//             transport = new UDPTransport(nsthreads, 0.0, 0.0);
//             shard_clients.push_back(std::unique_ptr<TxnClient>(
//               new ShardClientVR(shardConfigPath, transport, client_id, i,
//                                 closestReplica, replicated)));
    } else
    NOT_REACHABLE();

    bclient.push_back(std::unique_ptr<BufferClient>(
      new BufferClient(shard_clients[0].get())));

    Debug("Tapir client [%lu] created! %lu %lu", client_id, nshards, bclient.size());

    /* Run the transport in a new thread. */
    transport_thread = std::thread(client_thread_func, replScheme, transport);
}

Client::~Client()
{
    // if (replScheme == "ir" || replScheme == "lir") {
    //     UDPSTransport *t = (UDPSTransport *)transport;
    //     t->Stop();
    // } else if (replScheme == "ir") {
    //     UDPTransport *t = (UDPTransport *)transport;
    //     t->Stop();
	// } else
    //     NOT_REACHABLE();

    bclient.clear();
//    transport->Wait();
}

/* Begins a transaction. All subsequent operations before a commit() or
 * abort() are part of this transaction.
 *
 * Return a TID for the transaction.
 */
void
Client::Begin()
{
    Debug("BEGIN [%lu]", t_id + 1);
    t_id++;
    if (replScheme == "vr")
        core_id = -1;
    else
        core_id = core_dis(core_gen);
    participants.clear();
}

/* Returns the value corresponding to the supplied key. */
int
Client::Get(const string &key, string &value)
{
    Debug("GET [%lu : %s]", t_id, key.c_str());

    // Contact the appropriate shard to get the value.
    int i = key_to_shard(key, nshards);

    // If needed, add this shard to set of participants and send BEGIN.
    if (participants.find(i) == participants.end()) {
        participants.insert(i);
        bclient[i]->Begin(t_id, core_id);
    }

    // Send the GET operation to appropriate shard.
    Promise promise(GET_TIMEOUT);

    bclient[i]->Get(key, &promise);
    value = promise.GetValue();
    return promise.GetReply();
}

string
Client::Get(const string &key)
{
    string value;
    Get(key, value);
    return value;
}

/* Sets the value corresponding to the supplied key. */
int
Client::Put(const string &key, const string &value)
{
    Debug("PUT [%lu : %s]", t_id, key.c_str());

    // Contact the appropriate shard to set the value.
    int i = key_to_shard(key, nshards);

    // If needed, add this shard to set of participants and send BEGIN.
    if (participants.find(i) == participants.end()) {
        participants.insert(i);
        bclient[i]->Begin(t_id, core_id);
    }

    Promise promise(PUT_TIMEOUT);

    // Buffering, so no need to wait.
    bclient[i]->Put(key, value, &promise);
    return promise.GetReply();
}

int
Client::Prepare(Timestamp &timestamp)
{
    // 1. Send commit-prepare to all shards.
    uint64_t proposed = 0;
    list<Promise *> promises;

    Debug("PREPARE [%lu] at %lu", t_id, timestamp.getTimestamp());
    ASSERT(participants.size() > 0);

    for (auto p : participants) {
        promises.push_back(new Promise(PREPARE_TIMEOUT));
        bclient[p]->Prepare(timestamp, promises.back());
    }

    int status = REPLY_OK;
    uint64_t ts;
    // 3. If all votes YES, send commit to all shards.
    // If any abort, then abort. Collect any retry timestamps.
    for (auto p : promises) {
        uint64_t proposed = p->GetTimestamp().getTimestamp();

        switch(p->GetReply()) {
        case REPLY_OK:
            Debug("PREPARE [%lu] OK", t_id);
            continue;
        case REPLY_FAIL:
            // abort!
            Debug("PREPARE [%lu] ABORT", t_id);
            return REPLY_FAIL;
        case REPLY_RETRY:
            status = REPLY_RETRY;
                if (proposed > ts) {
                    ts = proposed;
                }
                break;
        case REPLY_TIMEOUT:
            status = REPLY_RETRY;
            break;
        case REPLY_ABSTAIN:
            // just ignore abstains
            break;
        default:
            break;
        }
        delete p;
    }

    if (status == REPLY_RETRY) {
        uint64_t now = timeServer.GetTime();
        if (now > proposed) {
            timestamp.setTimestamp(now);
        } else {
            timestamp.setTimestamp(proposed);
        }
        Debug("RETRY [%lu] at [%lu]", t_id, timestamp.getTimestamp());
    }

    Debug("All PREPARE's [%lu] received", t_id);
    return status;
}

/* Attempts to commit the ongoing transaction. */
bool
Client::Commit()
{
    // Implementing 2 Phase Commit
    Timestamp timestamp(timeServer.GetTime(), client_id);
    int status;

    for (retries = 0; retries < COMMIT_RETRIES; retries++) {
        status = Prepare(timestamp);
        if (status == REPLY_RETRY) {
            continue;
        } else {
            break;
        }
    }

    // If we're not performing 2PC, then there's no need for another round
    // after prepare.
    // aaasz: for IR it is, unless it's not replicated
    if ((!twopc && replScheme == "vr") ||
      (!replicated && replScheme == "ir")) {
        participants.clear();
        return status == REPLY_OK;
    }

    if (status == REPLY_OK) {
        Debug("COMMIT [%lu]", t_id);

        for (auto p : participants) {
             bclient[p]->Commit(timestamp);
        }
        return true;
    }

    // 4. If not, send abort to all shards.
    Abort();
    return false;
}

/* Aborts the ongoing transaction. */
void
Client::Abort()
{
    Debug("ABORT [%lu]", t_id);

    for (auto p : participants) {
        bclient[p]->Abort();
    }
}

/* Return statistics of most recent transaction. */
vector<int>
Client::Stats()
{
    vector<int> v;
    return v;
}

} // namespace multitapirstore
