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

#include <csignal>
#include <cstdlib>
#include <numa.h>
#include <pthread.h>
#include <sched.h>

#include <thread>
#include <iostream>

#include <boost/thread/thread.hpp>

#include "lib/assert.h"
#include "lib/message.h"
#include "lib/fasttransport.h"
#include "lib/transport.h"
#include "store/common/flags.h"
#include "store/tapirstore/server.h"

// TODO: Find a better way to print stats.
static FastTransport *last_transport;
static replication::tapirir::IRReplica *last_ir_replica;

void server_thread_func(uint8_t numa_node, uint8_t shard_id) {
    // Load the shard's configuration. Note that every shard has a different
    // configuration.
    const std::string config_path = FLAGS_configFile +
                                    std::to_string(shard_id) +
                                    ".config";
    std::ifstream config_stream(config_path);
    if (config_stream.fail()) {
        fprintf(stderr, "unable to read configuration file: %s\n",
                config_path.c_str());
        std::exit(EXIT_FAILURE);
    }
    transport::Configuration config(config_stream);

    // Construct the server and load the keys.
    tapirstore::Server server(/*linearizable=*/true);
    if (FLAGS_keysFile != "") {
        std::ifstream in;
        in.open(FLAGS_keysFile);
        if (!in) {
            fprintf(stderr, "Could not read keys from: %s\n",
                    FLAGS_keysFile.c_str());
            std::exit(EXIT_FAILURE);
        }

        for (unsigned int i = 0; i < FLAGS_numKeys; ++i) {
            std::string key;
            getline(in, key);

            uint64_t hash = 5381;
            const char* str = key.c_str();
            for (unsigned int j = 0; j < key.length(); j++) {
                hash = ((hash << 5) + hash) + (uint64_t)str[j];
            }

            if (hash % FLAGS_numShards == shard_id) {
                server.Load(key, "null", Timestamp());
            }
        }
        in.close();
    }

    // Construct the transport and IR replica.
    std::string ip = config.replica(FLAGS_replicaIndex).host;
    FastTransport transport(
        /*config=*/config,
        /*ip=*/ip,
        /*nthreads=*/FLAGS_numShards,
        /*nr_req_types=*/5,
        /*phy_port=*/0,
        /*numa_node=*/numa_node,
        /*id=*/shard_id);
    replication::tapirir::IRReplica replica(config, FLAGS_replicaIndex,
                                            &transport, &server);
    if (shard_id == FLAGS_numShards - 1) {
        last_ir_replica = &replica;
    }

    // Run the server.
    transport.Run();
}

void signal_handler(int signal_num) {
   last_transport->Stop();
   last_ir_replica->PrintStats();
   std::exit(signal_num);
}

int
main(int argc, char **argv)
{
    signal(SIGINT, signal_handler);

    gflags::ParseCommandLineFlags(&argc, &argv, true);

    if (FLAGS_configFile == "") {
        fprintf(stderr, "option --configFile is required\n");
        return EXIT_FAILURE;
    }

    if (FLAGS_keysFile == "") {
        fprintf(stderr, "option --keysFile is required\n");
        return EXIT_FAILURE;
    }

    if (FLAGS_replicaIndex == -1) {
        fprintf(stderr, "option replicaIndex is required\n");
        return EXIT_FAILURE;
    }

    if (numa_available() == -1) {
        PPanic("NUMA library not available.");
    }

    std::vector<std::thread> threads;
    for (int shard_id = 0; shard_id < FLAGS_numShards; ++shard_id) {
        uint8_t numa_node = (shard_id % 4 < 2) ? 0 : 1;
        uint8_t idx = shard_id / 4 + (shard_id % 2) * 20;
        std::thread server_thread(server_thread_func, numa_node, shard_id);
        erpc::bind_to_core(server_thread, numa_node, idx);
        threads.push_back(std::move(server_thread));
    }
    for (std::thread& thread : threads) {
        thread.join();
    }
    return 0;
}
