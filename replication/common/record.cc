// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * record.cc:
 *   a replica's log of pending and committed operations
 *
 * Copyright 2013 Dan R. K. Ports  <drkp@cs.washington.edu>
 *           2019 Adriana Szekeres <aaasz@cs.washington.edu>      
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

#include "replication/common/record.h"

#include <utility>

#include "lib/assert.h"

namespace replication {

RecordEntry &
Record::Add(const RecordEntry& entry) {
    // Make sure this isn't a duplicate
    ASSERT(entries.count(entry.txn_id) == 0);
    entries[entry.txn_id] = entry;
    return entries[entry.txn_id];
}

RecordEntry &
Record::Add(view_t view,
            txnid_t txn_id,
            uint64_t req_nr,
            TransactionStatus txn_status,
            RecordEntryState state)
            // const Request &request)
{
    return Add(RecordEntry(view, txn_id, req_nr, txn_status, state, ""));
}

RecordEntry &
Record::Add(view_t view,
            txnid_t txn_id,
            uint64_t req_nr,
            TransactionStatus txn_status,
            RecordEntryState state,
            // const Request &request,
            const std::string &result)
{
    RecordEntry &entry = Add(view, txn_id, req_nr, txn_status, state);
    entry.result = result;
    return entries[txn_id];
}

// This really ought to be const
RecordEntry *
Record::Find(txnid_t txn_id)
{
    if (entries.empty() || entries.count(txn_id) == 0) {
        return NULL;
    }

    RecordEntry *entry = &entries[txn_id];
    ASSERT(entry->txn_id == txn_id);
    return entry;
}

bool
Record::SetStatus(txnid_t txn_id, RecordEntryState state)
{
    RecordEntry *entry = Find(txn_id);
    if (entry == NULL) {
        return false;
    }

    entry->state = state;
    return true;
}

bool
Record::SetTxnStatus(txnid_t txn_id, TransactionStatus txn_status)
{
    RecordEntry *entry = Find(txn_id);
    if (entry == NULL) {
        return false;
    }

    entry->txn_status = txn_status;
    return true;
}

bool
Record::SetResult(txnid_t txn_id, const std::string &result)
{
    RecordEntry *entry = Find(txn_id);
    if (entry == NULL) {
        return false;
    }

    entry->result = result;
    return true;
}

// bool
// Record::SetRequest(txnid_t txn_id, const Request &req)
// {
//     RecordEntry *entry = Find(txn_id);
//     if (entry == NULL) {
//         return false;
//     }

//     entry->request = req;
//     return true;
// }

bool
Record::SetReqNr(txnid_t txn_id, const uint64_t req_nr)
{
    RecordEntry *entry = Find(txn_id);
    if (entry == NULL) {
        return false;
    }

    entry->req_nr = req_nr;
    return true;
}

void
Record::Remove(txnid_t txn_id)
{
    entries.erase(txn_id);
}

bool
Record::Empty() const
{
    return entries.empty();
}

const RecordMap &Record::Entries() const {
    return entries;
}

} // namespace replication
