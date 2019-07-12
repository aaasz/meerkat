// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * log.h:
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

#include "replication/ir/record.h"

#include <utility>

#include "lib/assert.h"

namespace replication {
namespace ir {

Record::Record(const proto::RecordProto &record_proto) {
    for (const proto::RecordEntryProto &entry_proto : record_proto.entry()) {
        const view_t view = entry_proto.view();
        const txnid_t txn_id = std::make_pair(entry_proto.clientid(),
                                     entry_proto.clienttxn_nr());
        const TransactionStatus txn_status = (TransactionStatus) entry_proto.txn_status();
        const uint64_t req_nr = entry_proto.clientreq_nr();

        Request request;
        request.set_op(entry_proto.op());
        request.set_clientid(entry_proto.clientid());
        proto::RecordEntryState state = entry_proto.state();
        const std::string& result = entry_proto.result();
        Add(view, txn_id, req_nr, txn_status, state, request, result);
    }
}

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
            proto::RecordEntryState state,
            const Request &request)
{
    return Add(RecordEntry(view, txn_id, req_nr, txn_status, state, request, ""));
}

RecordEntry &
Record::Add(view_t view,
            txnid_t txn_id,
            uint64_t req_nr,
            TransactionStatus txn_status,
            proto::RecordEntryState state,
            const Request &request,
            const std::string &result)
{
    RecordEntry &entry = Add(view, txn_id, req_nr, txn_status, state, request);
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
Record::SetStatus(txnid_t txn_id, proto::RecordEntryState state)
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

bool
Record::SetRequest(txnid_t txn_id, const Request &req)
{
    RecordEntry *entry = Find(txn_id);
    if (entry == NULL) {
        return false;
    }

    entry->request = req;
    return true;
}

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

void
Record::ToProto(proto::RecordProto *proto) const
{
    for (const std::pair<const txnid_t, RecordEntry> &p : entries) {
        const RecordEntry &entry = p.second;
        proto::RecordEntryProto *entry_proto = proto->add_entry();

        entry_proto->set_view(entry.view);
        entry_proto->set_clientid(entry.txn_id.first);
        entry_proto->set_clienttxn_nr(entry.txn_id.second);
        entry_proto->set_clientreq_nr(entry.req_nr);
        entry_proto->set_txn_status(entry.txn_status);
        entry_proto->set_state(entry.state);
        entry_proto->set_op(entry.request.op());
        entry_proto->set_result(entry.result);
    }
}

const RecordMap &Record::Entries() const {
    return entries;
}

} // namespace ir
} // namespace replication
