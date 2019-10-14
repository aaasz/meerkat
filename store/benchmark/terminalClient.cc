// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * bench/terminal.cc:
 *   A terminal client for a distributed transactional store.
 *
 **********************************************************************/

#include <iostream>
#include <string>

#include "store/common/frontend/client.h"
#include "store/common/truetime.h"
#include "store/meerkatstore/meerkatir/client.h"
#include "store/meerkatstore/leadermeerkatir/client.h"
#include "lib/fasttransport.h"
#include "store/common/flags.h"

using namespace std;

namespace {

std::string Usage() {
    return "Usage: ./terminalClient\n"
           "  -configFile <config path prefix>\n"
           "  -numShards <num shards>\n"
           "  -mode <meerkatstore|silostore>\n"
           "  -closestReplica <closest replica index>\n"
           "  -numServerThreads <num_server_threads>\n"
           "  -ip <client_ip_address>"
           "  -physPort <NIC port>";
}

}  // namespace

int
main(int argc, char **argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    Client *client;

    // Load configuration
    std::ifstream configStream(FLAGS_configFile);
    if (configStream.fail()) {
        fprintf(stderr, "unable to read configuration file: %s\n",
                FLAGS_configFile.c_str());
    }
    transport::Configuration config(configStream);

    // Create the transport
    FastTransport *transport = new FastTransport(config,
                                                FLAGS_ip,
                                                FLAGS_numServerThreads,
                                                0,
                                                FLAGS_physPort,
                                                0,
                                                0);

    // Create client store object
    if (FLAGS_mode == "meerkatstore") {
        client = new meerkatstore::meerkatir::Client(config,
                                            transport,
                                            FLAGS_numServerThreads,
                                            FLAGS_numShards,
                                            FLAGS_closestReplica,
                                            0,
                                            0,
                                            false, true,
                                            TrueTime(FLAGS_skew, FLAGS_error));
    } else if (FLAGS_mode == "meerkatstore-leader") {
        client = new meerkatstore::leadermeerkatir::Client(config,
                                            transport,
                                            FLAGS_numServerThreads,
                                            FLAGS_numShards,
                                            FLAGS_closestReplica,
                                            0,
                                            0,
                                            false, true,
                                            TrueTime(FLAGS_skew, FLAGS_error));
    } else {
        fprintf(stderr, "option --mode is required\n");
        exit(0);
    }

    char c, cmd[2048], *tok;
    int clen, status;
    string key, value;

    while (1) {
        printf(">> ");
        fflush(stdout);

        clen = 0;
        while ((c = getchar()) != '\n')
            cmd[clen++] = c;
        cmd[clen] = '\0';

        if (clen == 0) continue;

        tok = strtok(cmd, " ,.-");

        if (strcasecmp(tok, "exit") == 0 || strcasecmp(tok, "q") == 0) {
            printf("Exiting..\n");
            break;
        } else if (strcasecmp(tok, "get") == 0) {
            tok = strtok(NULL, " ,.-");
            key = string(tok);

            status = client->Get(key, value);

            if (status == 0) {
                printf("%s -> %s\n", key.c_str(), value.c_str());
            } else {
                printf("Error in retrieving value\n");
            }
        } else if (strcasecmp(tok, "put") == 0) {
            tok = strtok(NULL, " ,.-");
            key = string(tok);
            tok = strtok(NULL, " ,.-");
            value = string(tok);
            client->Put(key, value);
        } else if (strcasecmp(tok, "begin") == 0) {
            client->Begin();
        } else if (strcasecmp(tok, "commit") == 0) {
            bool status = client->Commit();
            if (status) {
                printf("Commit succeeded..\n");
            } else {
                printf("Commit failed..\n");
            }
        } else {
            printf("Unknown command.. Try again!\n");
        }
        fflush(stdout);
    }

    exit(0);
    return 0;
}
