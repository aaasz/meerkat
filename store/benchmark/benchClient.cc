// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/benchmark/benchClient.cc:
 *   Benchmarking client for a distributed transactional store.
 *
 **********************************************************************/

#include "store/common/truetime.h"
#include "store/common/frontend/client.h"
#include "store/multitapirstore/client.h"
#include "store/common/flags.h"

#include <signal.h>
#include <random>

using namespace std;

// Function to pick a random key according to some distribution.
int rand_key();

bool ready = false;
double *zipf;
vector<string> keys;
int nReplicas = 1; // Number of replicas TODO:get this from command line
std::mt19937 key_gen;
vector<std::uniform_int_distribution<uint32_t>> keys_distributions;
thread_local std::uniform_int_distribution<uint32_t> key_dis;


// TODO(mwhittaker): Make command line flags.
bool twopc = false;
bool replicated = true;

std::mutex mtx;

thread_local Client* client;
thread_local vector<string> results;

void* client_thread_func(int thread_id, transport::Configuration config) {

    std::mt19937 core_gen;
    std::mt19937 replica_gen;
    std::uniform_int_distribution<uint32_t> core_dis(0, FLAGS_numServerThreads - 1);
    std::uniform_int_distribution<uint32_t> replica_dis(0, nReplicas - 1);
    std::random_device rd;
    uint8_t preferred_core_id;
    uint32_t localReplica = -1;

    core_gen = std::mt19937(rd());
    replica_gen = std::mt19937(rd());
    key_dis = std::uniform_int_distribution<uint32_t>(0, FLAGS_numKeys - 1);

    // Open file to dump results
    uint32_t global_client_id = FLAGS_nhost * 1000 + FLAGS_ncpu * FLAGS_numClientThreads + thread_id;
    FILE* fp = fopen((FLAGS_logPath + "/client." + std::to_string(global_client_id) + ".log").c_str(), "w");
    uint32_t global_thread_id = FLAGS_nhost * FLAGS_numClientThreads + thread_id;

    // Trying to distribute as equally as possible the clients on the
    // replica cores.

    //preferred_core_id = core_dis(core_gen);
    preferred_core_id = global_thread_id % FLAGS_numServerThreads;

    if (FLAGS_closestReplica == -1) {
        //localReplica =  (global_thread_id / nsthreads) % nReplicas;
        localReplica = replica_dis(replica_gen);
    } else {
        localReplica = FLAGS_closestReplica;
    }

    //fprintf(stderr, "global_thread_id = %d; localReplica = %d\n", global_thread_id, localReplica);
    if (FLAGS_mode == "mtapir") {
        client = new multitapirstore::Client(config,
                                            FLAGS_ip,
                                            FLAGS_physPort,
                                            FLAGS_numServerThreads,
                                            FLAGS_numShards,
                                            localReplica,
                                            preferred_core_id,
                                            twopc, replicated,
                                            TrueTime(FLAGS_skew, FLAGS_error),
                                            FLAGS_replScheme);
    } else {
        fprintf(fp, "option --mode is required\n");
        exit(0);
    }

    struct timeval t0, t1, t2, t3, t4;

    int nTransactions = 0;
    int tCount = 0;
    double tLatency = 0.0;
    int getCount = 0;
    double getLatency = 0.0;
    int commitCount = 0;
    double commitLatency = 0.0;
    string key, value;
    char buffer[100];
    bool status;
    string v (56, 'x'); //56 bytes

    gettimeofday(&t0, NULL);
    srand(t0.tv_sec + t0.tv_usec);

    // Eliminate randomness from the number of reads and writes we perform
    // but keep randomness in when the operations are performed
    int nr_writes = (FLAGS_wPer * FLAGS_tLen / 100);
    int nr_reads = FLAGS_tLen - nr_writes;
    while (1) {
        status = true;

        gettimeofday(&t1, NULL);
        client->Begin();

        int r = 0;
        int w = 0;

        for (int j = 0; j < FLAGS_tLen; j++) {
            key = keys[rand_key()];
        
            int coin = rand() % 2;
            if (coin == 0) {
                // write priority
                if (w < nr_writes) {
                    client->Put(key, v);
                    w++;
                } else {
                    gettimeofday(&t3, NULL);
                    status = client->Get(key, value) == REPLY_OK;
                    gettimeofday(&t4, NULL);

                    // the INC workload
                    client->Put(key, v);

                    getCount++;
                    getLatency += ((t4.tv_sec - t3.tv_sec)*1000000 + (t4.tv_usec - t3.tv_usec));
                    r++;
                }
            } else {
                // read priority
                if (r < nr_reads) {
                    gettimeofday(&t3, NULL);
                    status = client->Get(key, value) == REPLY_OK;
                    gettimeofday(&t4, NULL);

                    // the INC workload
                    client->Put(key, v);

                    getCount++;
                    getLatency += ((t4.tv_sec - t3.tv_sec)*1000000 + (t4.tv_usec - t3.tv_usec));
                    r++;
                } else {
                    client->Put(key, v);
                    w++;
                }
            }
        }

        gettimeofday(&t3, NULL);
        if (status) {
            status = client->Commit();
        }
        gettimeofday(&t2, NULL);

        commitCount++;
        commitLatency += ((t2.tv_sec - t3.tv_sec)*1000000 + (t2.tv_usec - t3.tv_usec));

        long latency = (t2.tv_sec - t1.tv_sec)*1000000 + (t2.tv_usec - t1.tv_usec);

        // log only the transactions that finished in the interval we actually measure
        if ((t2.tv_sec > FLAGS_secondsFromEpoch + FLAGS_warmup) &&
            (t2.tv_sec < FLAGS_secondsFromEpoch + FLAGS_duration - FLAGS_warmup)) {
            sprintf(buffer, "%d %ld.%06ld %ld.%06ld %ld %d\n", ++nTransactions, t1.tv_sec,
                    t1.tv_usec, t2.tv_sec, t2.tv_usec, latency, status?1:0);
            results.push_back(string(buffer));

            if (status) {
                tCount++;
                tLatency += latency;
            }
        }

        gettimeofday(&t1, NULL);
        if ( ((t1.tv_sec-t0.tv_sec)*1000000 + (t1.tv_usec-t0.tv_usec)) > FLAGS_duration*1000000)
            break;
    }
  
    for (auto line : results) {
        fprintf(fp, "%s", line.c_str());
    }

    fprintf(fp, "# Commit_Ratio: %lf\n", (double)tCount/nTransactions);
    fprintf(fp, "# Overall_Latency: %lf\n", tLatency/tCount);
    fprintf(fp, "# Get: %d, %lf\n", getCount, getLatency/getCount);
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
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    struct sigaction sa;

    memset(&sa, 0, sizeof(struct sigaction));
    sigemptyset(&sa.sa_mask);
    sa.sa_sigaction = segfault_sigaction;
    sa.sa_flags   = SA_SIGINFO;

    sigaction(SIGSEGV, &sa, NULL);

    // initialize the uniform distribution
    std::random_device rd;
    key_gen = std::mt19937(rd());

    // Read in the keys from a file.
    string key, value;
    ifstream in;
    in.open(FLAGS_keysFile);
    if (!in) {
        fprintf(stderr, "Could not read keys from: %s\n", FLAGS_keysFile.c_str());
        exit(0);
    }
    for (int i = 0; i < FLAGS_numKeys; i++) {
        getline(in, key);
        keys.push_back(key);
    }
    in.close();

    // Load configuration
    std::ifstream configStream(FLAGS_configFile);
    if (configStream.fail()) {
        fprintf(stderr, "unable to read configuration file: %s\n", FLAGS_configFile.c_str());
    }
    transport::Configuration config(configStream);

    // Create client threads
    std::vector<std::thread> thread_arr(FLAGS_numClientThreads);
    for (size_t i = 0; i < FLAGS_numClientThreads; i++) {
        // TODO: pass host id (given as incremental number times the
        //       number of client processes we start on the host)
        // Unique thread id
        //*tid = nhost + ncpu * ncthreads + i;
        thread_arr[i] = std::thread(client_thread_func, i, config);
        erpc::bind_to_core(thread_arr[i], 0, i);
    }

    for (auto &thread : thread_arr) thread.join();

    return 0;
}

int rand_key()
{
    if (FLAGS_zipf <= 0) {
        // Uniform selection of keys.
        return key_dis(key_gen);
    } else {
        // Zipf-like selection of keys.
        if (!ready) {
            zipf = new double[FLAGS_numKeys];

            double c = 0.0;
            for (int i = 1; i <= FLAGS_numKeys; i++) {
                c = c + (1.0 / pow((double) i, FLAGS_zipf));
            }
            c = 1.0 / c;

            double sum = 0.0;
            for (int i = 1; i <= FLAGS_numKeys; i++) {
                sum += (c / pow((double) i, FLAGS_zipf));
                zipf[i-1] = sum;
            }
            ready = true;
        }

        double random = 0.0;
        while (random == 0.0 || random == 1.0) {
            random = (1.0 + rand())/RAND_MAX;
        }

        // binary search to find key;
        int l = 0, r = FLAGS_numKeys, mid;
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
