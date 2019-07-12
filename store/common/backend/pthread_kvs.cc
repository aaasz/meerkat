// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/common/backend/pthread_kvs.cc
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

#include "store/common/backend/pthread_kvs.h"

bool PthreadKvs::Get(const std::string& key,
                     std::pair<Timestamp, std::string>* timestamped_value) {
    ASSERT(timestamped_value != nullptr);

    const auto iter = kvs_.find(key);
    if (iter == kvs_.end()) {
        return false;
    }

    Entry& entry = iter->second;
    int lock_err = pthread_rwlock_rdlock(&entry.lock);
    ASSERT(lock_err == 0);
    timestamped_value->first = entry.timestamp;
    timestamped_value->second = entry.value;
    int unlock_err = pthread_rwlock_unlock(&entry.lock);
    ASSERT(unlock_err == 0);
    return true;
}

void PthreadKvs::GetWithLock(
    const std::string& key,
    std::pair<Timestamp, std::string>* timestamped_value) {
    ASSERT(timestamped_value != nullptr);
    const auto iter = kvs_.find(key);
    ASSERT(iter != kvs_.end());
    Entry& entry = iter->second;
    timestamped_value->first = entry.timestamp;
    timestamped_value->second = entry.value;
}

bool PthreadKvs::TryGet(const std::string& key,
                        std::pair<Timestamp, std::string>* timestamped_value) {
    ASSERT(timestamped_value != nullptr);

    const auto iter = kvs_.find(key);
    if (iter == kvs_.end()) {
        return false;
    }

    Entry& entry = iter->second;
    if (pthread_rwlock_tryrdlock(&entry.lock) != 0) {
        return false;
    }

    timestamped_value->first = entry.timestamp;
    timestamped_value->second = entry.value;
    int unlock_err = pthread_rwlock_unlock(&entry.lock);
    ASSERT(unlock_err == 0);
    return true;
}

void PthreadKvs::WriteLock(const std::string& key, Timestamp* timestamp) {
    Entry& entry = kvs_[key];
    int lock_err = pthread_rwlock_wrlock(&entry.lock);
    ASSERT(lock_err == 0);
    *timestamp = entry.timestamp;
}

bool PthreadKvs::TryWriteLock(const std::string& key, Timestamp* timestamp) {
    Entry& entry = kvs_[key];
    if (pthread_rwlock_trywrlock(&entry.lock) == 0) {
        *timestamp = entry.timestamp;
        return true;
    } else {
        return false;
    }
}

void PthreadKvs::PutWithLock(const std::string& key, const std::string& value,
                             const Timestamp& timestamp) {
    Entry& entry = kvs_[key];
    if (timestamp >= entry.timestamp) {
        entry.value = value;
        entry.timestamp = timestamp;
    }
}

void PthreadKvs::WriteUnlock(const std::string& key) {
    Entry& entry = kvs_[key];
    int unlocked_err = pthread_rwlock_unlock(&entry.lock);
    ASSERT(unlocked_err == 0);
}

void PthreadKvs::Put(const std::string& key, const std::string& value,
                     const Timestamp& timestamp) {
    Entry& entry = kvs_[key];
    int lock_err = pthread_rwlock_wrlock(&entry.lock);
    ASSERT(lock_err == 0);
    if (timestamp >= entry.timestamp) {
        entry.value = value;
        entry.timestamp = timestamp;
    }
    int unlocked_err = pthread_rwlock_unlock(&entry.lock);
    ASSERT(unlocked_err == 0);
}

bool PthreadKvs::IsWriteLocked(const std::string& key) {
    Entry& entry = kvs_[key];
    if (pthread_rwlock_tryrdlock(&entry.lock) != 0) {
        return true;
    } else {
        pthread_rwlock_unlock(&entry.lock);
        return false;
    }
}