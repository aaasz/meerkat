#ifndef _FLAGS_H_
#define _FLAGS_H_

#include <gflags/gflags.h>

// Defines all the command line flags shared between both
// replica severs and clients
// Server side flags
DEFINE_string(configFile, "", "Path to the configuration file");
DEFINE_int32(replicaIndex, -1, "Index of the replica in the config file");
DEFINE_string(keysFile, "", "Path to the keys file");
DEFINE_uint32(shardIndex, 0, "Index of the shard this replica is replicating");
DEFINE_uint64(numKeys, 1000000, "Number of keys in the store");
DEFINE_uint32(numShards, 1, "Number of shards");
DEFINE_uint32(numServerThreads, 1, "Number of server replica threads");
DEFINE_string(replScheme, "ir", "Replication scheme <ir|vr|lir>");

DEFINE_string(logPath, "/mnt/log", "Path to the log files");
DEFINE_uint32(numClientThreads, 1, "Number of client threads");
DEFINE_uint64(secondsFromEpoch, 0, "Synchronization point (start clock) for all clients");
DEFINE_uint32(duration, 10, "Number of seconds to run the experiment");
DEFINE_uint32(warmup, 3, "Number of seconds to warmup the experiment");
DEFINE_uint32(tLen, 10, "Length of the transaction");
DEFINE_uint32(wPer, 50, "Percentage of writes");
DEFINE_int32(closestReplica, -1, "Replica where to send the reads");
DEFINE_double(zipf, -1, "Zipf coefficient");
DEFINE_uint32(ncpu, 0, "On which processor to pin this process and its threads");
DEFINE_uint32(nhost, 0, "Monotonic id of the host we are running on");
DEFINE_uint32(skew, 0, "Difference between real clock and TrueTime");
DEFINE_uint32(error, 0, "Error bars");
DEFINE_string(mode, "mtapir", "What store client to run");

#endif /* _FLAGS_H_ */