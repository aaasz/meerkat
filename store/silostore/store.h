// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/silostore/store.h:
 *   Key-value store with support for transactions using TAPIR.
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

#ifndef _SILO_STORE_H_
#define _SILO_STORE_H_

#include <atomic>
#include <memory>
#include <set>
#include <unordered_map>

#include <pthread.h>
#include <semaphore.h>

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/backend/thread_safe_kvs.h"
#include "store/common/backend/txnstore.h"
#include "store/common/backend/versionstore.h"
#include "store/common/timestamp.h"
#include "store/common/transaction.h"

namespace silostore {

class Store : public TxnStore {
public:
    Store(bool twopc, bool replicated, ThreadSafeKvs *store)
        : twopc(twopc), replicated(replicated), store(store) {}

    // Overriding from TxnStore.
    void Begin(txnid_t txn_id);
    int Get(const std::string &key, std::pair<Timestamp,
            std::string> &value) override;
    int Get(txnid_t txn_id, const std::string &key,
            std::pair<Timestamp, std::string> &value) override;
    int Get(txnid_t txn_id, const std::string &key, const Timestamp &timestamp,
            std::pair<Timestamp, std::string> &value) override;
    int Prepare(txnid_t txn_id, const Transaction &txn,
                const Timestamp &timestamp, Timestamp &proposed) override;
    void Commit(txnid_t txn_id, const Timestamp &timestamp = Timestamp(),
                const Transaction &txn = Transaction()) override;
    void ForceCommit(txnid_t txn_id, const Timestamp &timestamp = Timestamp(),
                   const Transaction &txn = Transaction());
    void Abort(txnid_t txn_id,
               const Transaction &txn = Transaction()) override;
    void Load(const std::string &key, const std::string &value,
              const Timestamp &timestamp) override;

    // PerpareWrite performs the write phase of Silo's concurrency control. It
    // acquires write locks (or at least tries to) on the write set of txn. If
    // PrepareWrite returns successfully, it returns (via proposed) a propsed
    // timestamp that is larger than the timestamp of any written value.
    int PrepareWrite(txnid_t txn_id, const Transaction &txn,
                     Timestamp &proposed);

    // PrepareRead performs the read phase of Silo's concurrency control. It
    // checks to see if the read set is unmodified. If PrepareRead returns
    // successfully, it returns (via proposed) a timestamp larger than
    // write_timestamp and larger than any timestamp of any read value.
    int PrepareRead(txnid_t txn_id, const Transaction &txn,
                    const Timestamp &write_timestamp, Timestamp &proposed);

private:
    std::atomic<uint64_t> fake_counter;

    // Is our data sharded?
    const bool twopc;

    // Is our data replicated?
    const bool replicated;

    // Data store.
    ThreadSafeKvs* store;

    // Silo is designed so that read operations do not write to any shared
    // memory, a property known as _invisible reads_. To evaluate the benefits
    // of invisible reads, we want to compare the performance of Silo with and
    // without invisible reads. To evaluate the performance of Silo without
    // invisible reads, we have reads increment an atomic int. These atomic
    // ints are stored per-key in this map. The map is only accessed if the
    // `fake_visible_reads` flag inside store.cc is set to true. Otherwise, the
    // map is left completely untouched.
    std::map<std::string, std::atomic<int>> fake_visible_read_atomics;

    // We use these as long locks taken by preparing writers
    // and checked by preparing readers
    //
    // Note: std::atomic<bool> ensures natural-alignment
    // which makes loads atomic without any other
    // synchronization instructions; this enables invisible reads.
    std::unordered_map<std::string, std::atomic<bool>> long_locks;

    bool IsLongLocked(const std::string &key) {
        return long_locks[key].load();
    }

    bool TryLongLock(const std::string &key) {
        bool expected = false;
        return long_locks[key].compare_exchange_weak(expected, true);
    }

    void LongUnlock(const std::string &key) {
        long_locks[key].store(0);
    }
};

}  // namespace silostore

#endif  // _SILO_STORE_H_
