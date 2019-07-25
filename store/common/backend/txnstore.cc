// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/txnstore/lib/txnstore.h:
 *   Interface for a single node transactional store serving as a
 *   server-side backend
 *
 * Copyright 2013-2015 Irene Zhang <iyzhang@cs.washington.edu>
 *                     Naveen Kr. Sharma <naveenks@cs.washington.edu>
 *                     Dan R. K. Ports  <drkp@cs.washington.edu>
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

#include "store/common/backend/txnstore.h"

using namespace std;

TxnStore::TxnStore() {}
TxnStore::~TxnStore() {}

int
TxnStore::Get(const string &key, pair<Timestamp, string> &value)
{
    Panic("Unimplemented GET");
    return 0;
}

int
TxnStore::Get(txnid_t txn_id, const string &key, pair<Timestamp, string> &value)
{
    Panic("Unimplemented GET");
    return 0;
}

int
TxnStore::Get(txnid_t txn_id, const string &key, const Timestamp &timestamp,
    pair<Timestamp, string> &value)
{
    Panic("Unimplemented GET");
    return 0;
}

int
TxnStore::Put(txnid_t txn_id, const string &key, const string &value)
{
    Panic("Unimplemented PUT");
    return 0;
}

int
TxnStore::Prepare(txnid_t txn_id, const Transaction &txn)
{
    Panic("Unimplemented PREPARE");
    return 0;
}

int
TxnStore::Prepare(txnid_t txn_id, const Transaction &txn,
    const Timestamp &timestamp, Timestamp &proposed)
{
    Panic("Unimplemented PREPARE");
    return 0;
}

void
TxnStore::Commit(txnid_t txn_id, const Timestamp &timestamp, const Transaction &txn)
{
    Panic("Unimplemented COMMIT");
}

void
TxnStore::Abort(txnid_t txn_id, const Transaction &txn)
{
    Panic("Unimplemented ABORT");
}

void
TxnStore::Load(const string &key, const string &value, const Timestamp &timestamp)
{
    Panic("Unimplemented LOAD");
}
