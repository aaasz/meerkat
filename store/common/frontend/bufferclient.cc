// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/common/frontend/bufferclient.cc:
 *   Single shard buffering client implementation.
 *
 * Copyright 2015 Irene Zhang <iyzhang@cs.washington.edu>
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

#include "store/common/frontend/bufferclient.h"

using namespace std;

BufferClient::BufferClient(TxnClient* txnclient) : txn()
{
    this->txnclient = txnclient;
}

BufferClient::~BufferClient() { }

/* Begins a transaction. */
void
BufferClient::Begin(uint64_t tid, uint32_t core_id)
{
    // Initialize data structures.
    txn = Transaction();
    this->tid = tid;
    this->core_id = core_id;
    txnclient->Begin(tid);
}

/* Get value for a key.
 * Returns 0 on success, else -1. */
void
BufferClient::Get(const string &key, Promise *promise)
{
    // Read your own writes, check the write set first.
    if (txn.getWriteSet().find(key) != txn.getWriteSet().end()) {
        promise->Reply(REPLY_OK, (txn.getWriteSet().find(key))->second);
        return;
    }

    // TODO: we do not support multi-versioning;
    // for now, just always take the latest value from server,
    // it will abort if not consistent anyways (key was written in the meantime)
    // Consistent reads, check the read set.
//    if (txn.getReadSet().find(key) != txn.getReadSet().end()) {
//        // read from the server at same timestamp.
//        txnclient->Get(tid, key, (txn.getReadSet().find(key))->second, promise);
//        return;
//    }

    // Otherwise, get latest value from server.
    Promise p(GET_TIMEOUT);
    Promise *pp = (promise != NULL) ? promise : &p;

    if (core_id == -1)
        txnclient->Get(tid, key, pp);
    else
        txnclient->Get(tid, core_id, key, pp);
    if (pp->GetReply() == REPLY_OK) {
        Debug("Adding [%s] with ts %lu", key.c_str(), pp->GetTimestamp().getTimestamp());
        txn.addReadSet(key, pp->GetTimestamp());
    }
    // TODO: do we just ignore a REPLY_TIMEOUT?
}

/* Set value for a key. (Always succeeds).
 * Returns 0 on success, else -1. */
void
BufferClient::Put(const string &key, const string &value, Promise *promise)
{
    // Update the write set.
    txn.addWriteSet(key, value);
    promise->Reply(REPLY_OK);
}

/* Prepare the transaction. */
void
BufferClient::Prepare(const Timestamp &timestamp, Promise *promise)
{
    if (core_id == -1)
        txnclient->Prepare(tid, txn, timestamp, promise);
    else
        txnclient->Prepare(tid, core_id, txn, timestamp, promise);
}

void
BufferClient::Commit(const Timestamp &timestamp, Promise *promise)
{
    if (core_id == -1)
        txnclient->Commit(tid, txn, timestamp, promise);
    else
        txnclient->Commit(tid, core_id, txn, timestamp, promise);
}

/* Aborts the ongoing transaction. */
void
BufferClient::Abort(Promise *promise)
{
    if (core_id == -1)
        txnclient->Abort(tid, txn, promise);
    else
        txnclient->Abort(tid, core_id, txn, promise);
}

void
BufferClient::PrepareWrite(Promise *promise)
{
    txnclient->PrepareWrite(tid, txn, promise);
}

void
BufferClient::PrepareRead(const Timestamp& timestamp, Promise *promise)
{
    txnclient->PrepareRead(tid, txn, timestamp, promise);
}
