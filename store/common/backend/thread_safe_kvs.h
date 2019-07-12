// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/common/backend/thread_safe_kvs.h:
 *   Thread-safe key-value store.
 *
 * Copyright 2018 Michael Whittaker <mjwhittaker@berkeley.edu>
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

#ifndef _THREAD_SAFE_KVS_H_
#define _THREAD_SAFE_KVS_H_

#include <string>
#include <utility>

#include "store/common/timestamp.h"

// A ThreadSafeKvs is a key-value store that can be read from and written to
// safely by multiple concurrently executing threads. A ThreadSafeKvs is not
// multi-versioned---it's single-versioned---but all values in the key-value
// store are annotated with a Timestamp.
//
// NOTE that this class makes a vital assumption that all keys are initially
// loaded by calling Put on a single thread. In other words, all concurrent
// Puts must be to existing keys.
class ThreadSafeKvs {
public:
    virtual ~ThreadSafeKvs() = default;

    // Get the timestamp and value for a particular key. Get returns true if
    // the key exists in the key-value store and false otherwise. Get is a
    // blocking call.
    virtual bool Get(const std::string& key,
                     std::pair<Timestamp, std::string>* timestamped_value) = 0;

    // Get the timestamp and value for a particular key, assuming a write lock
    // has already been acquired on the key. This call is non-blocking.
    virtual void GetWithLock(
        const std::string& key,
        std::pair<Timestamp, std::string>* timestamped_value) = 0;

    // TryGet is a non-blocking alternative to Get. TryGet returns false if the
    // key doesn't exist in the key-value store or if the executing thread was
    // unable to obtain exclusive access to the key.
    virtual bool TryGet(
        const std::string& key,
        std::pair<Timestamp, std::string>* timestamped_value) = 0;

    // Obtain a write lock on a particular key and return the timestamp of the
    // key at the time the lock is obtained. WriteLock is a blocking call.
    virtual void WriteLock(const std::string& key, Timestamp* timestamp) = 0;

    // TryWriteLock is a non-blocking alternative to WriteLock. It returns true
    // (and populates timestamp) if the lock is successfully acquired.
    virtual bool TryWriteLock(const std::string& key, Timestamp* timestamp) = 0;

    // Insert a key, value, and timestamp into the key-value store. `value` is
    // only written if `timestamp` is larger than or equal to the current
    // timestamp of the key. Before Put is called, a write lock must be
    // obtained on the key. This is a non-blocking call. Don't forget to call
    // WriteUnlock afterwards.
    virtual void PutWithLock(const std::string& key, const std::string& value,
                             const Timestamp& timestamp) = 0;

    // Unlocks a lock obtained with WriteLock. This call is non-blocking.
    virtual void WriteUnlock(const std::string& key) = 0;

    // Checks wheher the key is locked with a lock obtained through WriteLock.
    virtual bool IsWriteLocked(const std::string& key) = 0;

    // Insert a key, value, and timestamp into the key-value store. Put is a
    // blocking call and is more or less equivalent to calling WriteLock,
    // PutWithLock, and WriteUnlock.
    virtual void Put(const std::string& key, const std::string& value,
                     const Timestamp& timestamp) = 0;
};

#endif  //  _THREAD_SAFE_KVS_H_
