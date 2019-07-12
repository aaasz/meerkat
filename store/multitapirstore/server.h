// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/tapirstore/server.h:
 *   A single transactional server replica.
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

#ifndef _MULTITAPIR_SERVER_H_
#define _MULTITAPIR_SERVER_H_

#include <memory>

#include "replication/ir/replica.h"
#include "store/common/backend/atomic_kvs.h"
#include "store/common/backend/pthread_kvs.h"
#include "store/common/backend/thread_safe_kvs.h"
#include "store/common/timestamp.h"
#include "store/common/truetime.h"
#include "store/multitapirstore/config.h"
#include "store/multitapirstore/multitapir-proto.pb.h"
#include "store/multitapirstore/store.h"

namespace multitapirstore {

using RecordEntryIR = replication::ir::RecordEntry;
//using RecordEntryLIR = replication::lir::RecordEntry;

class Server {
public:
    virtual ~Server() = default;
    virtual void Load(const string &key, const string &value,
                      const Timestamp timestamp) = 0;
};

class ServerIR : public Server, public replication::ir::IRAppReplica
{
public:
    ServerIR()
        : kvs(new PthreadKvs()),
          store(new Store(/*twopc=*/true, /*replicated=*/true, kvs.get())) {}

    // Invoke inconsistent operation, no return value
    void ExecInconsistentUpcall(txnid_t txn_id,
                                RecordEntryIR *crt_txn_state,
                                const string &str1) override;

    // Invoke consensus operation
    void ExecConsensusUpcall(txnid_t txn_id,
                             RecordEntryIR *crt_txn_state,
                             const string &str1, string &str2) override;

    // Invoke unreplicated operation
    void UnloggedUpcall(txnid_t txn_id,
                        const string &str1,
                        string &str2) override;

    void Load(const string &key, const string &value, const Timestamp timestamp) override;

private:
    std::unique_ptr<ThreadSafeKvs> kvs;
    std::unique_ptr<Store> store;
};

} // namespace multitapirstore

#endif /* _MULTITAPIR_SERVER_H_ */
