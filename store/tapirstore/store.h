// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 * Copyright 2019 Dan R. K. Ports <drkp@cs.washington.edu>
 *                Irene Zhang Ports <iyzhang@cs.washington.edu>
 *                Adriana Szekeres <aaasz@cs.washington.edu>
 *                Naveen Sharma <naveenks@cs.washington.edu>
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

#ifndef _TAPIRSTORE_STORE_H_
#define _TAPIRSTORE_STORE_H_

#include <set>
#include <unordered_map>

#include <boost/functional/hash.hpp>
#include <boost/unordered_map.hpp>

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/backend/txnstore.h"
#include "store/common/backend/versionstore.h"
#include "store/common/timestamp.h"
#include "store/common/transaction.h"

namespace tapirstore {

class Store : public TxnStore
{

public:
    Store(bool linearizable) : linearizable(linearizable) {}

    int Get(const std::string &key, std::pair<Timestamp, std::string> &value)
        override;
    int Prepare(txnid_t txn_id, const Transaction &txn,
                const Timestamp &timestamp, Timestamp &proposed) override;
    // The TxnStore interface has us pass in a timestamp and transaction,
    // though TAPIR actually ignores the transaction. The transaction was
    // received in the Prepare.
    void Commit(txnid_t txn_id, const Timestamp &timestamp,
                const Transaction &txn) override;
    void Abort(txnid_t txn_id, const Transaction &txn) override;
    void Load(const std::string &key, const std::string &value,
              const Timestamp &timestamp) override;

private:
    // Are we running in linearizable (vs serializable) mode?
    bool linearizable;

    VersionedKVStore store;

    // TODO: comment this.
    boost::unordered_map<txnid_t, std::pair<Timestamp, Transaction>> prepared;

    void GetPreparedWrites(std::unordered_map< std::string,
                           std::set<Timestamp> > &writes);
    void GetPreparedReads(std::unordered_map<std::string,
                          std::set<Timestamp>> &reads);
    void Commit(const Timestamp &timestamp, const Transaction &txn);
};

} // namespace tapirstore

#endif /* _TAPIRSTORE_STORE_H_ */
