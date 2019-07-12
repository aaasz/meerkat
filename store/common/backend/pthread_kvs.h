// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/common/backend/pthread_kvs.h
 *   Thread-safe key-value store implemented with pthread locks.
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

#ifndef _PTHREAD_KVS_H_
#define _PTHREAD_KVS_H_

#include <unordered_map>

#include "pthread.h"

#include "store/common/backend/thread_safe_kvs.h"

// PthreadKvs implements the ThreadSafeKvs interface using pthread_rwlock_ts.
class PthreadKvs : public ThreadSafeKvs {
public:
    bool Get(const std::string& key,
             std::pair<Timestamp, std::string>* timestamped_value) override;
    void GetWithLock(
        const std::string& key,
        std::pair<Timestamp, std::string>* timestamped_value) override;
    bool TryGet(const std::string& key,
                std::pair<Timestamp, std::string>* timestamped_value) override;
    void WriteLock(const std::string& key, Timestamp* timestamp) override;
    bool TryWriteLock(const std::string& key, Timestamp* timestamp) override;
    bool IsWriteLocked(const std::string& key) override;
    void PutWithLock(const std::string& key, const std::string& value,
                     const Timestamp& timestamp) override;
    void WriteUnlock(const std::string& key) override;
    void Put(const std::string& key, const std::string& value,
             const Timestamp& timestamp) override;

private:
    struct Entry {
        Entry() {
            int err = pthread_rwlock_init(&lock, nullptr);
            ASSERT(err == 0);
        }
        ~Entry() {
            int err = pthread_rwlock_destroy(&lock);
            ASSERT(err == 0);
        }

        std::string value;
        Timestamp timestamp;
        pthread_rwlock_t lock;
    };

    std::unordered_map<std::string, Entry> kvs_;
};

#endif  //  _PTHREAD_KVS_H_
