// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/meerkatstore/meerkatir/client.h:
 *   Meerkatir client interface (uses meerkatir for replcation and the
 *   meerkatstore transactional storage system).
 *
 * Copyright 2015 Irene Zhang  <iyzhang@cs.washington.edu>
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
 
#ifndef _MEERKATSTORE_MEERKATIR_CLIENT_H_
#define _MEERKATSTORE_MEERKATIR_CLIENT_H_

#include "lib/assert.h"
#include "lib/message.h"
#include "lib/configuration.h"
#include "lib/fasttransport.h"
#include "replication/meerkatir/client.h"
#include "store/common/timestamp.h"
#include "store/common/truetime.h"
#include "store/common/frontend/client.h"
#include "store/common/frontend/bufferclient.h"
#include "store/meerkatstore/meerkatir/shardclient.h"

#include <thread>

namespace meerkatstore {
namespace meerkatir {

class Client : public ::Client
{
public:
    Client(const transport::Configuration &config,
        Transport *transport,
        int nThreads,  int nShards,
        uint8_t closestReplica,
        uint8_t preferred_core_id,
        uint8_t preferred_read_core_id,
        bool twopc, bool replicated,
        TrueTime timeserver = TrueTime(0,0));
    virtual ~Client();

    // Overriding functions from ::Client.
    void Begin();
    int Get(const std::string &key, std::string &value);
    // Interface added for Java bindings
    std::string Get(const std::string &key);
    int Put(const std::string &key, const std::string &value);
    bool Commit();
    void Abort();
    std::vector<int> Stats();

private:
    // Unique ID for this client.
    uint64_t client_id;

    // Ongoing transaction ID.
    uint64_t t_id;

    // Ongoing transaction's mapping to a core.
    uint8_t preferred_thread_id;
    uint8_t preferred_read_thread_id;

    // Buffering client.
    BufferClient *bclient;

    // TrueTime server.
    TrueTime timeServer;

    // select core_id for the current transaction from a uniform distribution
    std::mt19937 core_gen;
    std::uniform_int_distribution<uint32_t> core_dis;

    // Prepare function
    int Prepare(Timestamp &timestamp);
};

} // namespace meerkatir
} // namespace meerkatstore

#endif /* _MEERKATSTORE_MEERKATIR_CLIENT_H_ */
