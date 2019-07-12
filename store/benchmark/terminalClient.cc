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
#include "store/multitapirstore/client.h"

using namespace std;

namespace {

std::string Usage() {
    return "Usage: ./terminalClient\n"
           "  -c <config path prefix>\n"
           "  -N <num shards>\n"
           "  -m <drsilo|mtapir>\n"
           "  -r <closest replica index>\n"
           "  -t <num_server_threads>\n"
           "  -R <vr|ir>";
}

}  // namespace

int
main(int argc, char **argv)
{
    const char *configPath = NULL;
    const char *replScheme = NULL;
    int nShards = 1, nsthreads = 1;
    int closestReplica = -1; // Closest replica id.

    // TODO(mwhittaker): Make command line flags.
    bool twopc = false;
    bool replicated = true;

    Client *client;
    enum {
        MODE_UNKNOWN,
        MODE_MTAPIR,
        MODE_DRSILO
    } mode = MODE_UNKNOWN;

    int opt;
    while ((opt = getopt(argc, argv, "c:N:m:r:R:t:")) != -1) {
        switch (opt) {
        case 'c': // Configuration path
        {
            configPath = optarg;
            break;
        }

        case 'N': // Number of shards.
        {
            char *strtolPtr;
            nShards = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                (nShards <= 0)) {
                fprintf(stderr, "option -N requires a numeric arg\n");
            }
            break;
        }

        case 't': // Number of shards.
        {
            char *strtolPtr;
            nsthreads = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                (nShards <= 0)) {
                fprintf(stderr, "option -t requires a numeric arg\n");
            }
            break;
        }

        case 'm': // Mode to run in [occ/lock/...]
        {
            if (strcasecmp(optarg, "mtapir") == 0) {
                mode = MODE_MTAPIR;
            } else if (strcasecmp(optarg, "drsilo") == 0) {
                mode = MODE_DRSILO;
            } else {
                fprintf(stderr, "unknown mode '%s'\n", optarg);
                exit(0);
            }
            break;
        }

        case 'r':
        {
            char *strtolPtr;
            closestReplica = strtod(optarg, &strtolPtr);
            if ((*optarg == '\0') || (*strtolPtr != '\0'))
            {
                fprintf(stderr,
                        "option -r requires a numeric arg\n");
            }
            break;
        }

        case 'R': // Preferred replication scheme.
        {
            replScheme = optarg;
            break;
        }

        default:
            fprintf(stderr, "Unknown argument %s\n", argv[optind]);
            break;
        }
    }

    if (mode == MODE_MTAPIR) {
        client = new multitapirstore::Client(configPath, nsthreads, nShards,
                                             closestReplica,
                                             twopc, replicated,
                                             TrueTime(0, 0), replScheme);
    } else if (mode == MODE_DRSILO) {
        // TODO(mwhittaker): Make these command line flags.
        // client = new silostore::Client(configPath, nsthreads, nShards,
        //                                closestReplica,
        //                                twopc, replicated,
        //                                TrueTime(0, 0), replScheme);
    } else {
        fprintf(stderr, "option -m is required\n");
        std::cerr << Usage() << std::endl;
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
