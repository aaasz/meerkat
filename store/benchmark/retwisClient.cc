// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/benchmark/retwisClient.cc:
 *   Retwis benchmarking client for a distributed transactional store.
 *
 **********************************************************************/

#include "store/common/truetime.h"
#include "store/common/frontend/client.h"
#include "store/multitapirstore/client.h"

#include <signal.h>
#include <random>
#include <algorithm>

using namespace std;

// Function to pick a random key according to some distribution.
int rand_key();

bool ready = false;
double alpha = -1;
double *zipf;

// TODO: send this in as a parameter
string logPath = "/mnt/log";

vector<string> keys;
uint32_t keysPerCore = 1000000;
int nKeys = 100;
const char *configPath = NULL;
const char *keysPath = NULL;
const char *replScheme = NULL;
long int seconds_from_epoch = 0;
int duration = 10;
int warmup = duration/3;
int nShards = 1, ipcthreads = 0, nsthreads = 1, ncthreads = 1;
int tLen = 10;
int wPer = 50; // Out of 100
int closestReplica = -1; // Closest replica id.
int nReplicas = 3; // Number of replicas TODO:get this from command line
int skew = 0; // difference between real clock and TrueTime
int error = 0; // error bars
int ncpu = 0; // on which processor to pin this process and its threads
int nhost = 0; // monotonic id of the host we are running on

std::mt19937 key_gen;
vector<std::uniform_int_distribution<uint32_t>> keys_distributions;
std::uniform_int_distribution<uint32_t> key_dis;


// TODO(mwhittaker): Make command line flags.
bool twopc = false;
bool replicated = true;

std::mutex mtx;

enum {
    MODE_UNKNOWN,
    MODE_DRSILO,
    MODE_MTAPIR
} mode = MODE_UNKNOWN;

thread_local Client* client;
thread_local vector<string> results;

void* run_client(void *arg) {

    std::mt19937 core_gen;
    std::mt19937 replica_gen;
    std::uniform_int_distribution<uint32_t> core_dis(0, nsthreads - 1);
    std::uniform_int_distribution<uint32_t> replica_dis(0, nReplicas - 1);
    std::random_device rd;
    //uint32_t core_id;
    uint32_t preffered_core_id;
    uint32_t localReplica = -1;

    vector<int> keyIdx;
    int ret;

    core_gen = std::mt19937(rd());
    replica_gen = std::mt19937(rd());

    int thread_id = *((int*) arg);
    delete (int*)arg;

    // Open file to dump results
    FILE* fp = fopen((logPath + "/client." + std::to_string(nhost * 1000 + ncpu * ncthreads + thread_id) + ".log").c_str(), "w");
    uint32_t global_thread_id = nhost * ncthreads + thread_id;

    // Trying to distribute as equally as possible the clients on the
    // replica cores.

    //preffered_core_id = core_dis(core_gen);
    preffered_core_id = global_thread_id % nsthreads;

    if (closestReplica == -1) {
        //localReplica =  (global_thread_id / nsthreads) % nReplicas;
        localReplica = replica_dis(replica_gen);
    } else {
        localReplica = closestReplica;
    }

    //fprintf(stderr, "global_thread_id = %d; localReplica = %d\n", global_thread_id, localReplica);

    if (mode == MODE_DRSILO) {
        {
            // std::lock_guard<std::mutex> lck (mtx); //guard the libevent setup
	        // client = new silostore::Client(configPath, nsthreads, nShards,
            //                                localReplica,
            //                                twopc, replicated,
            //                                TrueTime(skew, error), replScheme);
        }
    } else if (mode == MODE_MTAPIR) {
        {
            std::lock_guard<std::mutex> lck (mtx); //guard the libevent setup
            client = new multitapirstore::Client(configPath, nsthreads, nShards,
                                             localReplica,
                                             twopc, replicated,
                                             TrueTime(skew, error), replScheme);
        }
    } else {
        fprintf(fp, "option -m is required\n");
        exit(0);
    }

    struct timeval t0, t1, t2, t3;

    int nTransactions = 0;
    int tCount = 0;
    double tLatency = 0.0;
    int getCount = 0;
    double getLatency = 0.0;
    int putCount = 0;
    double putLatency = 0.0;
    int beginCount = 0;
    double beginLatency = 0.0;
    int commitCount = 0;
    double commitLatency = 0.0;
    string key, value;
    char buffer[100];
    bool status = 0;
    int ttype; // Transaction type.
    int retries = 0;

    gettimeofday(&t0, NULL);
    srand(t0.tv_sec + t0.tv_usec);
    //string v="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"; //56 bytes
    string v="v";


    while (1) {
        keyIdx.clear();

        // Begin a transaction.
        //core_id = core_dis(core_gen);
        client->Begin();
        //((silostore::Client *)client)->Begin(preffered_core_id);
        gettimeofday(&t1, NULL);
        status = true;

        // Decide which type of retwis transaction it is going to be.
        ttype = rand() % 100;

        if (ttype < 5) {
            // 5% - Add user transaction. 1,3
            keyIdx.push_back(rand_key());
            keyIdx.push_back(rand_key());
            keyIdx.push_back(rand_key());
            sort(keyIdx.begin(), keyIdx.end());

            if ((ret = client->Get(keys[keyIdx[0]], value))) {
                Warning("Aborting due to %s %d", keys[keyIdx[0]].c_str(), ret);
                status = false;
            }

            for (int i = 0; i < 3 && status; i++) {
                client->Put(keys[keyIdx[i]], v);
            }
            ttype = 1;
        } else if (ttype < 20) {
            // 15% - Follow/Unfollow transaction. 2,2
            keyIdx.push_back(rand_key());
            keyIdx.push_back(rand_key());
            sort(keyIdx.begin(), keyIdx.end());

            for (int i = 0; i < 2 && status; i++) {
                if ((ret = client->Get(keys[keyIdx[i]], value))) {
                    Warning("Aborting due to %s %d", keys[keyIdx[i]].c_str(), ret);
                    status = false;
                }
                client->Put(keys[keyIdx[i]], v);
            }
            ttype = 2;
        } else if (ttype < 50) {
            // 30% - Post tweet transaction. 3,5
            keyIdx.push_back(rand_key());
            keyIdx.push_back(rand_key());
            keyIdx.push_back(rand_key());
            keyIdx.push_back(rand_key());
            keyIdx.push_back(rand_key());
            sort(keyIdx.begin(), keyIdx.end());

            for (int i = 0; i < 3 && status; i++) {
                if ((ret = client->Get(keys[keyIdx[i]], value))) {
                    Warning("Aborting due to %d %s %d", keyIdx[i], keys[keyIdx[i]].c_str(), ret);
                    status = false;
                }
                client->Put(keys[keyIdx[i]], v);
            }
            for (int i = 0; i < 2; i++) {
                client->Put(keys[keyIdx[i+3]], v);
            }
            ttype = 3;
        } else {
            // 50% - Get followers/timeline transaction. rand(1,10),0
            int nGets = 1 + rand() % 10;
            for (int i = 0; i < nGets; i++) {
                keyIdx.push_back(rand_key());
            }

            sort(keyIdx.begin(), keyIdx.end());
            for (int i = 0; i < nGets && status; i++) {
                if ((ret = client->Get(keys[keyIdx[i]], value))) {
                    Warning("Aborting due to %s %d", keys[keyIdx[i]].c_str(), ret);
                    status = false;
                }
            }
            ttype = 4;
        }

        gettimeofday(&t3, NULL);
        if (status)
            status = client->Commit();
        else {
            gettimeofday(&t1, NULL);
            if ( ((t1.tv_sec-t0.tv_sec)*1000000 + (t1.tv_usec-t0.tv_usec)) > duration*1000000)
                break;
            else continue;
        }
        //status = true;
        gettimeofday(&t2, NULL);

        commitCount++;
        commitLatency += ((t2.tv_sec - t3.tv_sec)*1000000 + (t2.tv_usec - t3.tv_usec));

        long latency = (t2.tv_sec - t1.tv_sec)*1000000 + (t2.tv_usec - t1.tv_usec);

//        fprintf(fp, "%d %ld.%06ld %ld.%06ld %ld %d\n", nTransactions+1, t1.tv_sec,
//                t1.tv_usec, t2.tv_sec, t2.tv_usec, latency, status?1:0);

        // log only the transactions that finished in the interval we actually measure
        if ((t2.tv_sec > seconds_from_epoch + warmup) &&
            (t2.tv_sec < seconds_from_epoch + duration - warmup)) {
            sprintf(buffer, "%d %ld.%06ld %ld.%06ld %ld %d %d %d\n", ++nTransactions, t1.tv_sec,
                    t1.tv_usec, t2.tv_sec, t2.tv_usec, latency, status?1:0, ttype, retries);
            results.push_back(string(buffer));

            if (status) {
                tCount++;
                tLatency += latency;
            }
        }

        gettimeofday(&t1, NULL);
        if ( ((t1.tv_sec-t0.tv_sec)*1000000 + (t1.tv_usec-t0.tv_usec)) > duration*1000000)
            break;
    }

    for (auto line : results) {
        fprintf(fp, "%s", line.c_str());
    }

    fprintf(fp, "# Commit_Ratio: %lf\n", (double)tCount/nTransactions);
    fprintf(fp, "# Overall_Latency: %lf\n", tLatency/tCount);
    fprintf(fp, "# Begin: %d, %lf\n", beginCount, beginLatency/beginCount);
    fprintf(fp, "# Get: %d, %lf\n", getCount, getLatency/getCount);
    fprintf(fp, "# Put: %d, %lf\n", putCount, putLatency/putCount);
    fprintf(fp, "# Commit: %d, %lf\n", commitCount, commitLatency/commitCount);

    fclose(fp);

    return NULL;
}

void segfault_sigaction(int signal, siginfo_t *si, void *arg)
{
    fprintf(stderr, "Caught segfault at address %p, code = %d\n", si->si_addr, si->si_code);
    exit(0);
}

int
main(int argc, char **argv)
{

    int opt;
    while ((opt = getopt(argc, argv, "a:c:d:N:l:t:T:w:k:f:m:e:s:z:r:R:p:h:S:")) != -1) {
        switch (opt) {
        case 'c': // Configuration path
        {
            configPath = optarg;
            break;
        }

        case 'f': // Generated keys path
        {
            keysPath = optarg;
            break;
        }

        case 'N': // Number of shards.
        {
            char *strtolPtr;
            nShards = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                (nShards <= 0)) {
                fprintf(stderr, "option -n requires a numeric arg\n");
            }
            break;
        }

        case 'd': // Duration in seconds to run.
        {
            char *strtolPtr;
            duration = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                (duration <= 0)) {
                fprintf(stderr, "option -n requires a numeric arg\n");
            }
            break;
        }

        case 'a': // Duration in seconds to run.
        {
            char *strtolPtr;
            warmup = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                (warmup <= 0)) {
                fprintf(stderr, "option -a requires a numeric arg\n");
            }
            break;
        }

        case 'l': // Length of each transaction (deterministic!)
        {
            char *strtolPtr;
            tLen = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                (tLen <= 0)) {
                fprintf(stderr, "option -l requires a numeric arg\n");
            }
            break;
        }

        case 'w': // Percentage of writes (out of 100)
        {
            char *strtolPtr;
            wPer = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                (wPer < 0 || wPer > 100)) {
                fprintf(stderr, "option -w requires a arg b/w 0-100\n");
            }
            break;
        }

        case 'k': // Number of keys to operate on.
        {
            char *strtolPtr;
            nKeys = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                (nKeys <= 0)) {
                fprintf(stderr, "option -k requires a numeric arg\n");
            }
            break;
        }

        case 's': // Simulated clock skew.
        {
            char *strtolPtr;
            skew = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') || (skew < 0))
            {
                fprintf(stderr,
                        "option -s requires a numeric arg\n");
            }
            break;
        }

        case 'e': // Simulated clock error.
        {
            char *strtolPtr;
            error = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') || (error < 0))
            {
                fprintf(stderr,
                        "option -e requires a numeric arg\n");
            }
            break;
        }

        case 't':
        {
            char *strtolPtr;
            nsthreads = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0'))
            {
                fprintf(stderr, "option -t requires a numeric arg\n");
            }
            break;
        }

        case 'p':
        {
            char *strtolPtr;
            ncpu = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0'))
            {
                fprintf(stderr, "option -p requires a numeric arg\n");
            }
            break;
        }

        case 'h':
        {
            char *strtolPtr;
            nhost = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0'))
            {
                fprintf(stderr, "option -h requires a numeric arg\n");
            }
            break;
        }

        case 'T':
        {
            char *strtolPtr;
            ncthreads = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0'))
            {
                fprintf(stderr, "option -T requires a numeric arg\n");
            }
            break;
        }

        case 'z': // Zipf coefficient for key selection.
        {
            char *strtolPtr;
            alpha = strtod(optarg, &strtolPtr);
            if ((*optarg == '\0') || (*strtolPtr != '\0'))
            {
                fprintf(stderr,
                        "option -z requires a numeric arg\n");
            }
            break;
        }

        case 'r': // Preferred closest replica.
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

        case 'm': // Mode to run in [occ/lock/...]
        {
            if (strcasecmp(optarg, "drsilo") == 0) {
                mode = MODE_DRSILO;
            } else if (strcasecmp(optarg, "mtapir") == 0) {
                mode = MODE_MTAPIR;
            } else {
                fprintf(stderr, "unknown mode '%s'\n", optarg);
                exit(0);
            }
            break;
        }

        case 'S': // number of seconds used to compute the start and end of the actual experiment
        {
            char *strtolPtr;
            seconds_from_epoch = strtoul(optarg, &strtolPtr, 10);
            if ((*optarg == '\0') || (*strtolPtr != '\0') ||
                (seconds_from_epoch <= 0)) {
                fprintf(stderr, "option -S requires a numeric arg\n");
            }
            break;
        }

        default:
            fprintf(stderr, "Unknown argument %s\n", argv[optind]);
            break;
        }
    }

    struct sigaction sa;

    memset(&sa, 0, sizeof(struct sigaction));
    sigemptyset(&sa.sa_mask);
    sa.sa_sigaction = segfault_sigaction;
    sa.sa_flags   = SA_SIGINFO;

    sigaction(SIGSEGV, &sa, NULL);

    // initialize the uniform distribution
    std::random_device rd;
    key_gen = std::mt19937(rd());
    key_dis = std::uniform_int_distribution<uint32_t>(0, nKeys - 1);

    // create key distributions per core
    //for (int i = 0; i < nsthreads; i++) {
    //    key_dis = std::uniform_int_distribution<uint32_t>(i * nKeys / keysPerCore, (i + 1) * nKeys / keysPerCore - 1);
    //    keys_distributions.push_back(std::uniform_int_distribution<uint32_t>(i * keysPerCore, (i + 1) * keysPerCore - 1));
    //    keys_distributions.push_back(std::uniform_int_distribution<uint32_t>(i * keysPerCore, i*keysPerCore + 10));
    //}

    // Read in the keys from a file.
    string key, value;
    ifstream in;
    in.open(keysPath);
    if (!in) {
        fprintf(stderr, "Could not read keys from: %s\n", keysPath);
        exit(0);
    }
    for (int i = 0; i < nKeys; i++) {
        getline(in, key);
        keys.push_back(key);
    }
    in.close();

    // Create client threads
    pthread_t* t;
    std::vector<pthread_t *> threads;
    int* tid;

    for (int i = 0; i < ncthreads; i++) {
        t = new pthread_t;
        tid = new int;
        // TODO: pass host id (given as incremental number times the
        //       number of client processes we start on the host)
        // Unique thread id
        //*tid = nhost + ncpu * ncthreads + i;
        *tid = i;
        pthread_create(t, NULL, run_client, (void*) tid);
        threads.push_back(t);
    }

	for (auto t : threads){
        pthread_join(*t, NULL);
    }

    return 0;
}

int rand_key()
{
    if (alpha <= 0) {
        // Uniform selection of keys.
        return key_dis(key_gen);
    } else {
        // Zipf-like selection of keys.
        if (!ready) {
            zipf = new double[nKeys];

            double c = 0.0;
            for (int i = 1; i <= nKeys; i++) {
                c = c + (1.0 / pow((double) i, alpha));
            }
            c = 1.0 / c;

            double sum = 0.0;
            for (int i = 1; i <= nKeys; i++) {
                sum += (c / pow((double) i, alpha));
                zipf[i-1] = sum;
            }
            ready = true;
        }

        double random = 0.0;
        while (random == 0.0 || random == 1.0) {
            random = (1.0 + rand())/RAND_MAX;
        }

        // binary search to find key;
        int l = 0, r = nKeys, mid;
        while (l < r) {
            mid = (l + r) / 2;
            if (random > zipf[mid]) {
                l = mid + 1;
            } else if (random < zipf[mid]) {
                r = mid - 1;
            } else {
                break;
            }
        }
        return mid;
    }
}
