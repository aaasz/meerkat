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

#ifndef _TAPIRIR_RECORD_H_
#define _TAPIRIR_RECORD_H_

#include <map>
#include <string>
#include <utility>

#include <boost/functional/hash.hpp>
#include <boost/unordered_map.hpp>

#include "lib/assert.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "replication/common/viewstamp.h"
#include "replication/tapirir/messages.h"

namespace replication {
namespace tapirir {

enum class EntryType {
    INCONSISTENT,
    CONSENSUS
};

enum class EntryStatus {
    TENTATIVE,
    FINALIZED
};

struct RecordEntry
{
    view_t view;
    operation_id_t opid;
    EntryType type;
    EntryStatus status;
    std::vector<char> request;
    std::vector<char> result;
};

class Record
{
public:
    // Use the copy-and-swap idiom to make Record moveable but not copyable
    // [1]. We make it non-copyable to avoid unnecessary copies.
    //
    // [1]: https://stackoverflow.com/a/3279550/3187068
    Record() {}
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

    const boost::unordered_map<operation_id_t, RecordEntry> &Entries() const {
        return entries;
    }

    boost::unordered_map<operation_id_t, RecordEntry> &MutableEntries() {
        return entries;
    }

private:
    boost::unordered_map<operation_id_t, RecordEntry> entries;
};

} // namespace tapirir
} // namespace replication

#endif // _TAPIRIR_RECORD_H_
