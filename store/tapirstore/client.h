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

#ifndef _TAPIRSTORE_CLIENT_H_
#define _TAPIRSTORE_CLIENT_H_

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/transport.h"
// #include "replication/tapirir/client.h"
#include "store/common/frontend/bufferclient.h"
#include "store/common/frontend/client.h"
#include "store/common/timestamp.h"
#include "store/common/truetime.h"
#include "store/tapirstore/shardclient.h"

namespace tapirstore {

class Client : public ::Client
{
public:
    Client(const std::string& config_path_prefix, Transport *transport,
           int nThreads, int nShards, uint8_t closestReplica,
           TrueTime timeserver = TrueTime(0,0));

    // Overriding functions from ::Client.
    void Begin() override;
    int Get(const std::string &key, std::string &value) override;
    std::string Get(const std::string &key);
    int Put(const std::string &key, const std::string &value) override;
    bool Commit() override;
    void Abort() override;
    std::vector<int> Stats() override;

private:
    // Prepare function
    int Prepare(Timestamp &timestamp);

    // Unique ID for this client.
    uint64_t client_id;

    // Ongoing transaction ID.
    uint64_t t_id;

    // Number of shards.
    uint64_t nshards;

    // Number of retries for current transaction.
    long retries;

    // List of participants in the ongoing transaction.
    std::set<int> participants;

    // Shard client and corresponding Buffering client for each shard.
    std::vector<std::unique_ptr<TxnClient>> shard_clients;
    std::vector<std::unique_ptr<BufferClient>> buffer_clients;

    // TrueTime server.
    TrueTime timeServer;

    // select core_id for the current transaction from a uniform distribution
    std::mt19937 core_gen;
    std::uniform_int_distribution<uint32_t> core_dis;
};

} // namespace tapirstore

#endif /* _TAPIRSTORE_CLIENT_H_ */
