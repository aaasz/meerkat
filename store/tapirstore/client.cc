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

#include "store/tapirstore/client.h"

namespace tapirstore {

Client::Client(const std::string& config_path_prefix,
                Transport *transport,
                int nsthreads, int nShards,
                uint8_t closestReplica,
                TrueTime timeServer)
    : t_id(0), nshards(nShards), timeServer(timeServer),
      core_gen(std::random_device()()), core_dis(0, nsthreads - 1)
{
    // Generate a randon non-zero client id.
    srand(time(NULL));
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dis(1, ULLONG_MAX);
    while (client_id == 0) {
        client_id = dis(gen);
    }

    Warning("Initializing TAPIR client with "
            "id = %lu, "
            "read_replica = %d, ",
            client_id, closestReplica);

    // Start a shard client and corresponding buffer client for every shard.
    for (int i = 0; i < nshards; ++i) {
        // Load the shard's configuration.
        std::string config_path = config_path_prefix + std::to_string(i) +
                                  ".config";
        std::ifstream config_stream(config_path);
        if (config_stream.fail()) {
            Panic("Unable to read configuration file: %s\n",
                  config_path.c_str());
        }
        transport::Configuration config(config_stream);

        // Construct the clients.
        auto shard_client = std::unique_ptr<ShardClient>(new ShardClient(
                config, transport, client_id, i, closestReplica));
        auto buffer_client = std::unique_ptr<BufferClient>(
                new BufferClient(shard_client.get()));
        shard_clients.push_back(std::move(shard_client));
        buffer_clients.push_back(std::move(buffer_client));
    }
}

void
Client::Begin()
{
    Debug("BEGIN [%lu]", t_id + 1);
    t_id++;
    participants.clear();
}

int
Client::Get(const string &key, string &value)
{
    Debug("GET [%lu : %s]", t_id, key.c_str());

    // Contact the appropriate shard to get the value.
    int i = key_to_shard(key, nshards);

    // If needed, add this shard to set of participants and send BEGIN.
    if (participants.find(i) == participants.end()) {
        participants.insert(i);
        buffer_clients[i]->Begin(t_id, /*core_id=*/i,
                                 /*preferred_read_core_id=*/i);
    }

    // Send the GET operation to appropriate shard.
    Promise promise(GET_TIMEOUT);

    buffer_clients[i]->Get(key, &promise);
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

int
Client::Put(const string &key, const string &value)
{
    Debug("PUT [%lu : %s]", t_id, key.c_str());

    // Contact the appropriate shard to set the value.
    int i = key_to_shard(key, nshards);

    // If needed, add this shard to set of participants and send BEGIN.
    if (participants.find(i) == participants.end()) {
        participants.insert(i);
        buffer_clients[i]->Begin(t_id, /*core_id=*/i,
                                 /*preferred_read_core_id=*/i);
    }

    Promise promise(PUT_TIMEOUT);

    // Buffering, so no need to wait.
    buffer_clients[i]->Put(key, value, &promise);
    return promise.GetReply();
}

int
Client::Prepare(Timestamp &timestamp)
{
    // 1. Send commit-prepare to all shards.
    uint64_t proposed = 0;
    std::vector<std::unique_ptr<Promise>> promises;

    Debug("PREPARE [%lu] at %lu", t_id, timestamp.getTimestamp());
    ASSERT(participants.size() > 0);

    for (auto p : participants) {
        std::unique_ptr<Promise> promise(new Promise(PREPARE_TIMEOUT));
        buffer_clients[p]->Prepare(timestamp, promise.get());
        promises.push_back(std::move(promise));
    }

    int status = REPLY_OK;
    uint64_t ts;
    // 3. If all votes YES, send commit to all shards.
    // If any abort, then abort. Collect any retry timestamps.
    for (auto& p : promises) {
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

    if (status == REPLY_OK) {
        Debug("COMMIT [%lu]", t_id);

        for (auto p : participants) {
            buffer_clients[p]->Commit(timestamp);
        }
        return true;
    }

    // 4. If not, send abort to all shards.
    Abort();
    return false;
}

void
Client::Abort()
{
    Debug("ABORT [%lu]", t_id);

    for (auto p : participants) {
        buffer_clients[p]->Abort();
    }
}

std::vector<int>
Client::Stats()
{
    return std::vector<int>();
}

} // namespace tapirstore
