// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/tapirstore/store.h:
 *   Key-value store with support for transactions using TAPIR.
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

#ifndef _MULTITAPIR_STORE_H_
#define _MULTITAPIR_STORE_H_

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/timestamp.h"
#include "store/common/transaction.h"
#include "store/common/backend/txnstore.h"
#include "store/common/backend/thread_safe_kvs.h"
#include "store/common/backend/atomic_kvs.h"
#include "store/common/backend/pthread_kvs.h"
#include "store/common/backend/versionstore.h"
#include "store/multitapirstore/dlinkedlist.h"
#include "replication/ir/replica.h"

#include <set>
#include <unordered_map>
#include <pthread.h>
#include <mutex>

namespace multitapirstore {

class Store : public TxnStore
{
    struct PreparingTransaction
    {
        txnid_t txn_id;
        Timestamp ts;
        std::unordered_map<std::string, DLinkedList<PreparingTransaction>::Node*> readNodes;
        std::unordered_map<std::string, DLinkedList<PreparingTransaction>::Node*> writeNodes;

        friend bool operator< (const PreparingTransaction &t1, const PreparingTransaction &t2) {
            return t1.ts < t2.ts;
        };

        friend bool operator== (const PreparingTransaction &t1, const PreparingTransaction &t2) {
            return t1.txn_id == t2.txn_id;
        };

        friend bool operator!= (const PreparingTransaction &t1, const PreparingTransaction &t2) {
           return !(t1 == t2);
        };

    };
public:
    Store(bool twopc, bool replicated, ThreadSafeKvs *store)
        : twopc(twopc), replicated(replicated), store(store) {}

    // Overriding from TxnStore
    void Begin(txnid_t txn_id);
    int Get(txnid_t txn_id, const std::string &key, std::pair<Timestamp, std::string> &value);
    int Get(txnid_t txn_id, const std::string &key, const Timestamp &timestamp, std::pair<Timestamp, std::string> &value);
    int Prepare(txnid_t txn_id, const Transaction &txn, const Timestamp &timestamp, Timestamp &proposed);
    void Commit(txnid_t txn_id, const Timestamp &timestamp, const Transaction &txn);
    void Abort(txnid_t txn_id, const Transaction &txn = Transaction());
    void Load(const std::string &key, const std::string &value, const Timestamp &timestamp);

private:

    // Is our data sharded?
    const bool twopc;

    // Is our data replicated?
    const bool replicated;

    // Data store.
    ThreadSafeKvs* store;

    // Ordered list of active readers of per key
    std::unordered_map<std::string, DLinkedList<PreparingTransaction>*> readers;
    // Ordered list of active writers of per key
    std::unordered_map<std::string, DLinkedList<PreparingTransaction>*> writers;

    void clean_transaction(txnid_t id, const Transaction &txn);
    void clean_preparing_transaction(PreparingTransaction *p);
};

} // namespace tapirstore

#endif /* _MULTITAPIR_STORE_H_ */
