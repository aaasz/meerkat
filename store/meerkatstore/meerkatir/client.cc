// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/meerkatstore/meerkatir/client.cc:
 *   Meerkatir client interface (uses meerkatir for replcation and the
 *   meerkatstore transactional storage system).
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

#include "store/meerkatstore/meerkatir/client.h"

#include <random>
#include <list>
#include <limits.h>
#include <thread>

namespace meerkatstore {
namespace meerkatir {

using namespace std;

Client::Client(const transport::Configuration &config,
                Transport *transport,
                int nsthreads, int nShards,
                uint8_t closestReplica,
                uint8_t preferred_thread_id,
                uint8_t preferred_read_thread_id,
                bool twopc, bool replicated, TrueTime timeServer)
    : t_id(0), preferred_thread_id(preferred_thread_id),
      preferred_read_thread_id(preferred_read_thread_id),
      timeServer(timeServer), core_dis(0, nsthreads -1)
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

    Warning("Initializing Meerkatstore client with id [%lu]; preferred_thread = %d,"
          "read_replica = %d, preferred_read_thread = %d",
          client_id, preferred_thread_id,
          closestReplica, preferred_read_thread_id);

    /* Start a client for each shard. */
    // TODO: assume just one shard for now!
    bclient = new BufferClient(new ShardClient(config, transport, client_id, 0,
                                 closestReplica, replicated));

    Debug("Meerkatstore client [%lu] created!", client_id);
}

Client::~Client()
{
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
    bclient->Begin(t_id, preferred_thread_id, preferred_read_thread_id);
}

/* Returns the value corresponding to the supplied key. */
int
Client::Get(const string &key, string &value)
{
    Debug("GET [%lu : %s]", t_id, key.c_str());

    // Send the GET operation.
    Promise promise(GET_TIMEOUT);
    bclient->Get(key, &promise);
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

    Promise promise(PUT_TIMEOUT);

    // Buffering, so no need to wait.
    bclient->Put(key, value, &promise);
    return promise.GetReply();
}

int
Client::Prepare(Timestamp &timestamp)
{
    Debug("PREPARE [%lu] at %lu", t_id, timestamp.getTimestamp());
    Promise *promise = new Promise(PREPARE_TIMEOUT);
    bclient->Prepare(timestamp, promise);
    int status = promise->GetReply();
    delete promise;
    return status;
}

/* Attempts to commit the ongoing transaction. */
bool
Client::Commit()
{
    Timestamp timestamp(timeServer.GetTime(), client_id);
    int status = Prepare(timestamp);

    if (status == REPLY_OK) {
        Debug("COMMIT [%lu]", t_id);
        bclient->Commit(timestamp);
        return true;
    }

    Abort();
    return false;
}

/* Aborts the ongoing transaction. */
void
Client::Abort()
{
    Debug("ABORT [%lu]", t_id);
    bclient->Abort();
}

/* Return statistics of most recent transaction. */
vector<int>
Client::Stats()
{
    vector<int> v;
    return v;
}

} // namespace meerkatir
} // namespace meerkatstore