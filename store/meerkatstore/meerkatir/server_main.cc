// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/tapirstore/server.cc:
 *   Implementation of a single transactional key-value server.
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

#include <pthread.h>
#include <sched.h>
#include <cstdlib>
#include <iostream>
#include <thread>
#include <csignal>
#include <numa.h>

#include "store/common/flags.h"
#include "store/meerkatstore/meerkatir/server.h"

#include <boost/thread/thread.hpp>

using namespace std;

// TODO: better way to print stats
static FastTransport *last_transport;
static replication::meerkatir::Replica *last_replica;
static meerkatstore::meerkatir::Server *global_server;

void server_thread_func(meerkatstore::meerkatir::Server *server,
      transport::Configuration config,
      uint8_t numa_node, uint8_t thread_id) {
    std::string local_uri = config.replica(FLAGS_replicaIndex).host;
    // TODO: provide mapping function from thread_id to numa_node
    // for now assume it's round robin
    // TODO: get rid of the hardcoded number of request types
    int ht_ct = boost::thread::hardware_concurrency();
    FastTransport *transport = new FastTransport(config,
                                                local_uri,
                                                //FLAGS_numServerThreads,
                                                ht_ct,
                                                4,
                                                0,
                                                numa_node,
                                                thread_id);
    last_transport = transport;

    replication::meerkatir::Replica *replica = new replication::meerkatir::Replica(
      config, FLAGS_replicaIndex,
      (FastTransport *)transport,
      server);

    last_replica = replica;
    global_server = server;

    transport->Run();
}

void signal_handler( int signal_num ) {
   last_transport->Stop();
   last_replica->PrintStats();
   global_server->PrintStats();

   // terminate program
   exit(signal_num);
}

int
main(int argc, char **argv)
{
    signal(SIGINT, signal_handler);

    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // TODO(mwhittaker): Make command line flags.
    // bool twopc = false;
    // bool replicated = true;

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

    // Load configuration
    std::ifstream configStream(FLAGS_configFile);
    if (configStream.fail()) {
        fprintf(stderr, "unable to read configuration file: %s\n", FLAGS_configFile.c_str());
    }
    transport::Configuration config(configStream);

    if (FLAGS_replicaIndex >= config.n) {
        fprintf(stderr, "replica index %d is out of bounds; "
                "only %d replicas defined\n", FLAGS_replicaIndex, config.n);
    }

    meerkatstore::meerkatir::Server *server = new meerkatstore::meerkatir::Server();

    // Load keys in memory
    if (FLAGS_keysFile != "") {
        string key;
        std::ifstream in;
        in.open(FLAGS_keysFile);
        if (!in) {
            fprintf(stderr, "Could not read keys from: %s\n", FLAGS_keysFile.c_str());
            exit(0);
        }

        for (unsigned int i = 0; i < FLAGS_numKeys; i++) {
            getline(in, key);

            uint64_t hash = 5381;
            const char* str = key.c_str();
            for (unsigned int j = 0; j < key.length(); j++) {
                hash = ((hash << 5) + hash) + (uint64_t)str[j];
            }

            if (hash % FLAGS_numShards == FLAGS_shardIndex) {
                server->Load(key, "null", Timestamp());
            }
        }
        in.close();
    }

    // create replica threads
    // bind round robin on the availlable numa nodes
    if (numa_available() == -1) {
        PPanic("NUMA library not available.");
    }

    //int nn_ct = numa_max_node() + 1;
    //int ht_ct = boost::thread::hardware_concurrency()/boost::thread::physical_concurrency(); // number of hyperthreads

    // start the app on all available cores to regulate frequency boosting
    int ht_ct = boost::thread::hardware_concurrency();
    //std::vector<std::thread> thread_arr(FLAGS_numServerThreads);
    std::vector<std::thread> thread_arr(ht_ct);
    //for (uint8_t i = 0; i < FLAGS_numServerThreads; i++) {
    for (uint8_t i = 0; i < ht_ct; i++) {
        // thread_arr[i] = std::thread(server_thread_func, server, config, i%nn_ct, i);
        // erpc::bind_to_core(thread_arr[i], i%nn_ct, i/nn_ct);
        uint8_t numa_node = (i % 4 < 2)?0:1;
        uint8_t idx = i/4 + (i % 2) * 20;
        thread_arr[i] = std::thread(server_thread_func, server, config, numa_node, i);
        erpc::bind_to_core(thread_arr[i], numa_node, idx);
    }

    for (auto &thread : thread_arr) thread.join();

    return 0;
}