// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * record.h:
 *   a replica's log of pending and committed operations
 *
 * Copyright 2013 Dan R. K. Ports  <drkp@cs.washington.edu>
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

#ifndef _MEERKATIR_RECORD_H_
#define _MEERKATIR_RECORD_H_

#include <map>
#include <unordered_map>
#include <string>
#include <utility>

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/transaction.h"
#include "replication/common/viewstamp.h"

#include <boost/functional/hash.hpp>

#include <boost/unordered_map.hpp>

namespace replication {
namespace meerkatir {

enum RecordEntryState {
    RECORD_STATE_TENTATIVE,
    RECORD_STATE_FINALIZED
};

// Each record entry maintains information about
// a single, uniquely identified, transaction
struct RecordEntry
{
    // unique id of this transaction
    txnid_t txn_id;
	// current view for this transaction
    view_t view;
    // most recent request number
    uint64_t req_nr;
    // latest status
    TransactionStatus txn_status;
    // latest request for this transaction
    // TODO: do we need this?
    //Request request;
    // Read and write sets
    Transaction txn;
    // Commit timestamp
    Timestamp ts;
    // replication state of the latest request (FINALIZED if we know
    // that at least a majority agree on accepting the operation, and,
    // if it's the case, its result)
    RecordEntryState state;
    // latest result
    std::string result;

    RecordEntry() { result = "";
                    txn_status = NOT_PREPARED;
                    state = RECORD_STATE_TENTATIVE; }

    RecordEntry(const RecordEntry &x)
        : txn_id(x.txn_id),
          view(x.view),
          req_nr(x.req_nr),
          txn_status(x.txn_status),
          //request(x.request),
          state(x.state),
          result(x.result) {}
    RecordEntry(view_t view, txnid_t txn_id,
                uint64_t req_nr,
                TransactionStatus txn_status,
                RecordEntryState state,
                //const Request &request,
                const std::string &result)
        : txn_id(txn_id),
          view(view),
          req_nr(req_nr),
          txn_status(txn_status),
          //request(request),
          state(state),
          result(result) {}
    virtual ~RecordEntry() {}
};

typedef boost::unordered_map
    <txnid_t, RecordEntry,
     boost::hash<std::pair<uint64_t, uint64_t>>>
RecordMap;

class Record
{
public:
    // Use the copy-and-swap idiom to make Record movable but not copyable
    // [1]. We make it non-copyable to avoid unnecessary copies.
    //
    // [1]: https://stackoverflow.com/a/3279550/3187068
    Record(){};
    Record(Record &&other) : Record() { swap(*this, other); }
    Record(const Record &) = delete;
    Record &operator=(const Record &) = delete;
    Record &operator=(Record &&other) {
        swap(*this, other);
        return *this;
    }
    friend void swap(Record &x, Record &y) {
        std::swap(x.entries, y.entries);
    }

    //void Reserve(size_t count) { entries.reserve(count); };
    RecordEntry &Add(const RecordEntry& entry);
    RecordEntry &Add(view_t view,
                     txnid_t txn_id,
                     uint64_t req_nr,
                     TransactionStatus txn_status,
                     RecordEntryState state);
                     // const Request &request);
    RecordEntry &Add(view_t view,
                     txnid_t txn_id,
                     uint64_t req_nr,
                     TransactionStatus txn_status,
                     RecordEntryState state,
                     // const Request &request,
                     const std::string &result);
    RecordEntry *Find(txnid_t txn_id);
    bool SetStatus(txnid_t txn_id, RecordEntryState state);
    bool SetResult(txnid_t txn_id, const std::string &result);
    bool SetTxnStatus(txnid_t txn_id, TransactionStatus txn_status);
    bool SetReqNr(txnid_t txn_id, uint64_t req_nr);
    // bool SetRequest(txnid_t txn_id, const Request &req);
    void Remove(txnid_t txn_id);
    bool Empty() const;

    const RecordMap &Entries() const;

private:
    RecordMap entries;
};

}      // namespace meerkatir
}      // namespace replication
#endif  /* _MEERKATIR_RECORD_H_ */
