// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/silostore/store.cc:
 *   Key-value store with support for transactions using SILO.
 *
 * Copyright 2015 Irene Zhang <iyzhang@cs.washington.edu>
 *                Naveen Kr. Sharma <naveenks@cs.washington.edu>
 * Copyright 2018 Adriana Szekeres <aaasz@cs.washington.edu>
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

#include "store/silostore/store.h"

#include <thread>

#include <semaphore.h>
#include <sys/time.h>

namespace silostore {

constexpr bool fake_visible_reads = false;

using namespace std;


int Store::Get(const string &key, pair<Timestamp,string> &value) {
    Debug("GET %s", key.c_str());

    // for (int i = 0; i < 1000; i++) { waste_cpu++; }
    // return REPLY_OK;

    if (store->Get(key, &value)) {
//    store->GetWithLock(key, &value);
        Debug("Value: \"%s\" at <%lu, %lu>",
              value.second.c_str(), value.first.getTimestamp(),
              value.first.getID());
        return REPLY_OK;
    } else {
        Debug("Key \"%s\" not found.", key.c_str());
        return REPLY_FAIL;
    }
}

int Store::Get(txnid_t txn_id, const string &key,
               pair<Timestamp, string> &value) {
    Debug("GET %s", key.c_str());
    if (store->Get(key, &value)) {
        Debug("Value: %s at <%lu, %lu>",
              value.second.c_str(), value.first.getTimestamp(),
              value.first.getID());
        return REPLY_OK;
    } else {
        Debug("[%lu - %lu] Key %s not found.",
              txn_id.first, txn_id.second, key.c_str());
        return REPLY_FAIL;
    }
}

int Store::Get(txnid_t txn_id, const string &key, const Timestamp &timestamp,
               pair<Timestamp, string> &value) {
    Panic("Gets at a particular timestamp are not supported by Silo.");
    return REPLY_FAIL;
}

int Store::Prepare(txnid_t id, const Transaction &txn,
                   const Timestamp &timestamp, Timestamp &proposedTimestamp) {
    Panic("Unimplemented");
    return REPLY_FAIL;
}

int Store::PrepareWrite(txnid_t txn_id, const Transaction &txn,
                        Timestamp &proposed) {
    // The set of keys for which we have acquired a lock.
    std::set<std::string> locked_keys;

    // Whether or not the prepare is successful.
    bool prepare_successful = true;

    // A timestamp bigger than the timestamp of any value written.
    Timestamp max_tid;

    // Try to acquire the write locks on every item in the write set of txn. In
    // a non-distributed and non-replicated setting, we perpetually try to
    // acquire locks. In a distributed setting, we conservatively abort to
    // prevent distributed deadlock.
    for (const pair<const string, string> &write : txn.getWriteSet()) {
        const string &key = write.first;
        bool flag_acquired = false;
        Timestamp timestamp;

        if (!twopc && !replicated) {
            // We use the short lock in the store for all concurrency control
            // purposes.
            //
            // TODO: The write set is already ordered in retwisClient. We need
            // to order it if not to avoid deadlocks even in the non-2PC mode.
            store->WriteLock(key, &timestamp);
            flag_acquired = true;
        } else {
            flag_acquired = TryLongLock(key);
        }

        if (flag_acquired) {
            locked_keys.insert(key);

            // We need to compute the commit timestamp, which must be bigger
            // than the written value's current timestamp.
            max_tid = std::max(max_tid, timestamp);
        } else {
            // Another concurrent transaction wants to write this key.
            Debug("[%lu - %lu] Could not acquire write lock on %s", txn_id.first,
                  txn_id.second,
                  key.c_str());
            prepare_successful = false;
            break;
        }
    }

    // If we couldn't acquire _any_ of the write locks, release _all_ the write
    // locks.
    if (!prepare_successful) {
        for (const string &key : locked_keys) {
            if (!twopc && !replicated) {
                store->WriteUnlock(key);
            } else {
                LongUnlock(key);
            }
        }
        return REPLY_FAIL;
    }

    proposed = max_tid;
    Debug("[%lu - %lu] PREPARED_WRITE at timestamp %lu",
          txn_id.first, txn_id.second,
          proposed.getTimestamp());

    return REPLY_OK;
}

int Store::PrepareRead(txnid_t txn_id, const Transaction &txn,
                       const Timestamp &write_timestamp, Timestamp &proposed) {
    // Whether the read-prepare phase is successful.
    bool prepare_successful = true;

    // A timestamp larger than any value read or written by this transaction.
    Timestamp max_tid = write_timestamp;

    for (const pair<const string, Timestamp> &read : txn.getReadSet()) {
        const string &key = read.first;
        const Timestamp &read_timestamp = read.second;

        // // Simulate a visible read.
        // if (fake_visible_reads) {
        //     if(fake_visible_read_atomics[key] & 1)
        //         fake_visible_read_atomics[key] =  fake_visible_read_atomics[key] + 1;
        //     else
        //         fake_visible_read_atomics[key] =  fake_visible_read_atomics[key] + 3;
        // }

        // get the current version from the store
        std::pair<Timestamp, string> timestamped_value;
        if (txn.getWriteSet().find(key) == txn.getWriteSet().end()) {
            if ((replicated && IsLongLocked(key)) || (!replicated && store->IsWriteLocked(key))) {
                prepare_successful = false;
                Debug("[%lu - %lu] Key %s is locked by another transaction.",
                      txn_id.first, txn_id.second, key.c_str());
                break;
            }
            store->Get(key, &timestamped_value);
        } else {
            store->GetWithLock(key, &timestamped_value);
        }

        if (timestamped_value.first != read_timestamp) {
            prepare_successful = false;
            Debug(
                  "[%lu - %lu] Check failed due to modified read key; ts "
                  "last wrote= %lu; ts read = %lu",
                  txn_id.first, txn_id.second, timestamped_value.first.getTimestamp(),
                  read_timestamp.getTimestamp());
            break;
        }

        max_tid = std::max(max_tid, read_timestamp);
    //    } else {
    //        Debug("[%lu - %lu] Either key %s doesn't exist or it is locked.",
    //              txn_id.first, txn_id.second, key.c_str());
    //        prepare_successful = false;
    //        break;
    //    }
    }

    // If read validation failed, then release all the write locks and clear
    // write_prepared.
    if (!prepare_successful) {
        for (const pair<const string, string> &p : txn.getWriteSet()) {
            const string &key = p.first;
            if (!twopc && !replicated) {
                // We use the short lock in the store for all concurrency control
                // purposes.
                store->WriteUnlock(key);
            } else {
                LongUnlock(key);
            }
        }
        Debug("[%lu - %lu] PREPARE READ failed", txn_id.first, txn_id.second);
        return REPLY_FAIL;
    }

    // TODO: not sure why we need a local timestamp and preserve
    // the invariant that transactions timestamps match the prepare order
    // (probably still to make sure their mechanism to provide
    //  serializable read-only transactions works)
    // Will take this out as it might cause many slow paths
//    silo_thread.ts = std::max(max_tid, silo_thread.ts);
//    ++silo_thread.ts;
//    proposed = silo_thread.ts;
    proposed = ++max_tid;

    Debug("[%lu - %lu] PREPARED_READ at timestamp %lu.",
          txn_id.first, txn_id.second,
          proposed.getTimestamp());
    return REPLY_OK;
}

// Assumes we prepared the transaction before and hold the locks
void Store::Commit(txnid_t txn_id, const Timestamp &timestamp,
                   const Transaction &txn) {
    Debug("[%lu - %lu] COMMIT at timestamp <%lu, %lu>",
          txn_id.first, txn_id.second, timestamp.getID(), timestamp.getTimestamp());

    // Insert writes into versioned key-value store and release all write locks
    for (const std::pair<const std::string, std::string> &write :
         txn.getWriteSet()) {
        const std::string &key = write.first;
        const std::string &value = write.second;
        if (!twopc && !replicated) {
            // We use the short lock in the store for all concurrency control
            // purposes.
            store->PutWithLock(key, value, timestamp);
            store->WriteUnlock(key);
        } else {
            store->Put(key, value, timestamp);
            Debug("Wrote key: %s", key.c_str());
            LongUnlock(key);
        }
    }
}

// Applies updates without having done a prepare before
// (i.e., not integrated with the concurrency control mechanism)
void Store::ForceCommit(txnid_t txn_id, const Timestamp &timestamp,
                   const Transaction &txn) {
    Debug("[%lu - %lu] FORCE COMMIT at timestamp <%lu, %lu>",
          txn_id.first, txn_id.second, timestamp.getID(), timestamp.getTimestamp());

    // Insert writes into versioned key-value store
    for (const std::pair<const std::string, std::string> &write :
         txn.getWriteSet()) {
        const std::string &key = write.first;
        const std::string &value = write.second;
        store->Put(key, value, timestamp);
        Debug("Wrote key: %s", key.c_str());
    }
}

// Assumes we prepared the transaction before and we hold the locks
void Store::Abort(txnid_t txn_id, const Transaction &txn) {
    Debug("[%lu - %lu] ABORT", txn_id.first, txn_id.second);

    // Release all the write locks.
    for (const std::pair<const std::string, std::string> &write :
         txn.getWriteSet()) {
        const std::string &key = write.first;
        if (!twopc && !replicated) {
            // We use the short lock in the store for all concurrency control
            // purposes.
            store->WriteUnlock(key);
        } else {
            LongUnlock(key);
        }
    }
}

void Store::Load(const string &key, const string &value,
                 const Timestamp &timestamp) {
    // Store the value.
    store->Put(key, value, timestamp);

    // Initialize long locks
    LongUnlock(key);

    // Initialize the fake visible read locks, if we're faking visible reads.
    if (fake_visible_reads) {
      fake_visible_read_atomics[key] = 0;
    }
}

}  // namespace silostore
