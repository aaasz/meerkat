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

#include "store/tapirstore/store.h"

namespace tapirstore {

int
Store::Get(const std::string &key, std::pair<Timestamp, std::string> &value)
{
    Debug("GET(%s)", key.c_str());
    bool ret = store.get(key, value);
    if (ret) {
        return REPLY_OK;
    } else {
        return REPLY_FAIL;
    }
}

int
Store::Prepare(txnid_t txn_id, const Transaction &txn,
                const Timestamp &timestamp, Timestamp &proposed)
{
    Debug("Prepare(txn_id=(%lu, %lu))", txn_id.first, txn_id.second);

    auto it = prepared.find(txn_id);
    if (it != prepared.end()) {
        if (it->second.first == timestamp) {
            Warning("Transaction (%lu, %lu) has already been prepared!",
                    txn_id.first, txn_id.second);
            return REPLY_OK;
        } else {
            // Run the checks again for a new timestamp.
            prepared.erase(it);
        }
    }

    // Do OCC checks.
    std::unordered_map<std::string, std::set<Timestamp>> p_writes;
    GetPreparedWrites(p_writes);
    std::unordered_map<std::string, std::set<Timestamp>> p_reads;
    GetPreparedReads(p_reads);

    // Check for conflicts with the read set.
    for (const std::pair<std::string, Timestamp>& read : txn.getReadSet()) {
        std::pair<Timestamp, Timestamp> range;
        bool ret = store.getRange(read.first, read.second, range);

        // If we don't have this key then no conflicts for read.
        if (!ret) {
            continue;
        }

        // If we don't have this version then no conflicts for read.
        if (range.first != read.second) {
            continue;
        }

        // If the value is still valid.
        if (!range.second.isValid()) {
            // Check pending writes.
            if (p_writes.find(read.first) != p_writes.end() &&
                 (linearizable ||
                  p_writes[read.first].upper_bound(timestamp) != p_writes[read.first].begin()) ) {
                Debug("[(%lu,%lu)] ABSTAIN rw conflict w/ prepared key:%s",
                      txn_id.first, txn_id.second, read.first.c_str());
                return REPLY_ABSTAIN;
            }

        } else if (linearizable || timestamp > range.second) {
            /* if value is not still valid, if we are running linearizable, then abort.
             *  Else check validity range. if
             * proposed timestamp not within validity range, then
             * conflict and abort
             */
            ASSERT(timestamp > range.first);
            Debug("[(%lu,%lu)] ABORT rw conflict key:%s",
                  txn_id.first, txn_id.second, read.first.c_str());
            return REPLY_FAIL;
        } else {
            /* there may be a pending write in the past.  check
             * pending writes again.  If proposed transaction is
             * earlier, abstain
             */
            if (p_writes.find(read.first) != p_writes.end()) {
                for (auto &writeTime : p_writes[read.first]) {
                    if (writeTime > range.first &&
                        writeTime < timestamp) {
                        Debug("[(%lu,%lu)] ABSTAIN rw conflict w/ prepared key:%s",
                              txn_id.first, txn_id.second, read.first.c_str());
                        return REPLY_ABSTAIN;
                    }
                }
            }
        }
    }

    // check for conflicts with the write set
    for (auto &write : txn.getWriteSet()) {
        std::pair<Timestamp, std::string> val;
        // if this key is in the store
        if ( store.get(write.first, val) ) {
            Timestamp lastRead;
            bool ret;

            // if the last committed write is bigger than the timestamp,
            // then can't accept in linearizable
            if ( linearizable && val.first > timestamp ) {
                Debug("[(%lu,%lu)] RETRY ww conflict w/ prepared key:%s",
                      txn_id.first, txn_id.second, write.first.c_str());
                proposed = val.first;
                return REPLY_RETRY;
            }

            // if last committed read is bigger than the timestamp, can't
            // accept this transaction, but can propose a retry timestamp

            // if linearizable mode, then we get the timestamp of the last
            // read ever on this object
            if (linearizable) {
                ret = store.getLastRead(write.first, lastRead);
            } else {
                // otherwise, we get the last read for the version that is being written
                ret = store.getLastRead(write.first, timestamp, lastRead);
            }

            // if this key is in the store and has been read before
            if (ret && lastRead > timestamp) {
                Debug("[(%lu,%lu)] RETRY wr conflict w/ prepared key:%s",
                      txn_id.first, txn_id.second, write.first.c_str());
                proposed = lastRead;
                return REPLY_RETRY;
            }
        }


        // if there is a pending write for this key, greater than the
        // proposed timestamp, retry
        if ( linearizable &&
             p_writes.find(write.first) != p_writes.end()) {
            std::set<Timestamp>::iterator it = p_writes[write.first].upper_bound(timestamp);
            if ( it != p_writes[write.first].end() ) {
                Debug("[(%lu,%lu)] RETRY ww conflict w/ prepared key:%s",
                      txn_id.first, txn_id.second, write.first.c_str());
                proposed = *it;
                return REPLY_RETRY;
            }
        }


        //if there is a pending read for this key, greater than the
        //propsed timestamp, abstain
        if ( p_reads.find(write.first) != p_reads.end() &&
             p_reads[write.first].upper_bound(timestamp) != p_reads[write.first].end() ) {
            Debug("[(%lu,%lu)] ABSTAIN wr conflict w/ prepared key:%s",
                  txn_id.first, txn_id.second, write.first.c_str());
            return REPLY_ABSTAIN;
        }
    }

    // Otherwise, prepare this transaction for commit
    prepared[txn_id] = std::make_pair(timestamp, txn);
    Debug("[(%lu,%lu)] PREPARED TO COMMIT", txn_id.first, txn_id.second);

    return REPLY_OK;
}

void
Store::Commit(txnid_t txn_id, const Timestamp& timestamp, const Transaction&) {
    Debug("Commit(txn_id=(%lu, %lu))", txn_id.first, txn_id.second);

    // Ignore transactions that we haven't previously prepared.
    //
    // TODO(mwhittaker): This is what TAPIR does; not sure if it's safe.
    auto it = prepared.find(txn_id);
    if (it == prepared.end()) {
        return;
    }

    // Commit our latest reads. Every element of the read set is a pair (key,
    // timestamp).
    const Transaction& txn = it->second.second;
    for (const auto& read : txn.getReadSet()) {
        store.commitGet(read.first, // key
                        read.second, // timestamp of read version
                        timestamp); // commit timestamp
    }

    // Commit our writes. Every element of the write set is a pair (key, value).
    for (const auto& write : txn.getWriteSet()) {
        store.put(write.first, // key
                  write.second, // value
                  timestamp); // timestamp
    }
}

void
Store::Abort(txnid_t txn_id, const Transaction &txn) {
    Debug("Abort(txn_id=(%lu, %lu))", txn_id.first, txn_id.second);

    auto it = prepared.find(txn_id);
    if (it != prepared.end()) {
        prepared.erase(it);
    }
}

void
Store::Load(const std::string &key, const std::string &value,
            const Timestamp &timestamp)
{
    store.put(key, value, timestamp);
}

void
Store::GetPreparedWrites(
    std::unordered_map<std::string, std::set<Timestamp>> &writes)
{
    // Gather up the set of all writes that are currently prepared.
    for (const auto& t : prepared) {
        const std::pair<Timestamp, Transaction>& v = t.second;
        for (const auto& write : v.second.getWriteSet()) {
            writes[write.first].insert(v.first);
        }
    }
}

void
Store::GetPreparedReads(
    std::unordered_map<std::string, std::set<Timestamp>> &reads)
{
    // Gather up the set of all writes that are currently prepared.
    for (const auto& t : prepared) {
        const std::pair<Timestamp, Transaction>& v = t.second;
        for (const auto& read : v.second.getReadSet()) {
            reads[read.first].insert(v.first);
        }
    }
}

} // namespace tapirstore
