// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/tapirstore/client.cc:
 *   Client to TAPIR transactional storage system.
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

#include "store/silostore/client.h"

namespace silostore {

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
      nshards(nShards),
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

    bclient.reserve(nshards);

    Warning("Initializing Silostore client with id [%lu]; preferred_thread = %d,"
          "read_replica = %d, preferred_read_thread = %d",
          client_id, preferred_thread_id,
          closestReplica, preferred_read_thread_id);

    /* Start a client for each shard. */
    // TODO: assume just one shard for now!
    shard_clients.reserve(nshards);
    bclient.reserve(nshards);
    // for (uint64_t i = 0; i < nshards; i++) {
    //     string shardConfigPath = configPath + to_string(i) + ".config";
    shard_clients.push_back(std::unique_ptr<TxnClient>(
        new ShardClientIR(config, transport, client_id, 0,
                          closestReplica, replicated)));

    bclient.push_back(std::unique_ptr<BufferClient>(new BufferClient(shard_clients[0].get())));

    Debug("Silostore client [%lu] created! %lu %lu", client_id, nshards, bclient.size());
}

Client::~Client() {
    bclient.clear();
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
    participants.clear();
}

int Client::Get(const string &key, string &value) {
    Debug("GET [%lu : %s]", t_id, key.c_str());

    // Contact the appropriate shard to get the value.
    int i = key_to_shard(key, nshards);

    // If needed, add this shard to set of participants and send BEGIN.
    if (participants.find(i) == participants.end()) {
        participants.insert(i);
        bclient[i]->Begin(t_id, preferred_thread_id, preferred_read_thread_id);
    }

    // Send the GET operation to appropriate shard.
    Promise promise(GET_TIMEOUT);

    bclient[i]->Get(key, &promise);
    value = promise.GetValue();
    return promise.GetReply();
}

string Client::Get(const string &key) {
    string value;
    Get(key, value);
    return value;
}

int Client::Put(const string &key, const string &value) {
    Debug("PUT [%lu : %s]", t_id, key.c_str());

    // Contact the appropriate shard to set the value.
    int i = key_to_shard(key, nshards);

    // If needed, add this shard to set of participants and send BEGIN.
    if (participants.find(i) == participants.end()) {
        participants.insert(i);
        bclient[i]->Begin(t_id, preferred_thread_id, preferred_read_thread_id);
    }

    Promise promise(PUT_TIMEOUT);

    // Buffering, so no need to wait.
    bclient[i]->Put(key, value, &promise);
    return promise.GetReply();
}

int Client::Prepare(Timestamp &timestamp) {
    Debug("Client is preparing transaction %lu.", t_id);

    // At least one participant should have been contacted.
	ASSERT(participants.size() > 0);

	// This is called when no twopc, so just one participant involved
	auto promise = std::unique_ptr<Promise>(new Promise(PREPARE_TIMEOUT));
	int p = *participants.begin();
	Debug("Client is sending prepare to shard %d.", p);
	bclient[p]->Prepare(timestamp, promise.get());

	int status = REPLY_OK;

	// Wait until we get response from the replica group
	timestamp = promise->GetTimestamp();

	switch (promise->GetReply()) {
	    case REPLY_OK: {
	        Debug(
	              "Prepare response for transaction %lu is "
	              "REPLY_OK.",
	              t_id);
	              break;
	    }
	    case REPLY_FAIL: {
	        Debug(
	              "Prepare response for transaction %lu is "
	              "REPLY_FAIL.",
	              t_id);
	        return REPLY_FAIL;
	    }
	    case REPLY_TIMEOUT: {
	        Debug(
	              "Prepare response for transaction %lu is "
	              "REPLY_RETRY.",
	              t_id);
	              status = REPLY_RETRY;
	              break;
	    }
	    default: { break; }
	}
	return status;
}

bool Client::Commit() {
    Debug("Client is commiting transaction %lu.", t_id);
    Timestamp timestamp;
    int status;

    status = Prepare(timestamp);

    return status == REPLY_OK;
}

void Client::Abort() {
    Debug("Client is aborting transaction %lu.", t_id);
    // Send an abort message to every shard that was written to. We don't need
    // to send an abort  message to the read-only shards because they don't
    // hold any locks.
    for (int p : participants) {
        if (!bclient[p]->GetTransaction().getWriteSet().empty()) {
            Debug("Client is sending abort to shard %d.", p);
            bclient[p]->Abort();
        }
    }
    participants.clear();
}

vector<int> Client::Stats() {
    return vector<int>();
}

} // namespace silostore
