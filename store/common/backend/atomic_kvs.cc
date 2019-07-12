// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/common/backend/atomic_kvs.cc
 *   Thread-safe key-value store implemented with atomics.
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

#include "store/common/backend/atomic_kvs.h"

#include <cstring>

namespace {

constexpr uint64_t locked_mask = 0x8000000000000000;
constexpr uint64_t timestamp_mask = 0x7FFFFFFFFFFF8000;
constexpr uint64_t id_mask = 0x0000000000007FFF;

constexpr uint64_t max_timestamp = 0xFFFFFFFFFFFF;
constexpr uint64_t max_id = 0x7FFF;

}  // namespace

AtomicKvs::TimestampWord::TimestampWord(bool locked,
                                        const Timestamp& timestamp) {
    ASSERT(timestamp.getTimestamp() <= max_timestamp);
    ASSERT(timestamp.getID() <= max_id);
    locked_ = locked;
    timestamp_ = timestamp;
}

AtomicKvs::TimestampWord::TimestampWord(uint64_t word)
    : locked_((word & locked_mask) != 0),
      timestamp_((word & timestamp_mask) >> 15, (word & id_mask)) {}

uint64_t AtomicKvs::TimestampWord::ToWord() const {
    const uint64_t locked_part = locked_ ? locked_mask : 0;
    const uint64_t timestamp_part = (timestamp_.getTimestamp() & max_timestamp)
                                    << 15;
    const uint64_t id_part = timestamp_.getID() & max_id;
    return locked_part | timestamp_part | id_part;
}

bool AtomicKvs::Get(const std::string& key,
                    std::pair<Timestamp, std::string>* timestamped_value) {
    ASSERT(timestamped_value != nullptr);

    const auto iter = kvs_.find(key);
    if (iter == kvs_.end()) {
        return false;
    }

    Entry& entry = iter->second;
    bool done = false;
    while (!done) {
        TimestampWord timestamp_word_before(entry.word.load());
        timestamped_value->first = timestamp_word_before.timestamp();
        timestamped_value->second = entry.value;
        //TODO: put compiler barrier here (check if still necessary if we have an std::atomic access)
        TimestampWord timestamp_word_after(entry.word.load());
        done = !timestamp_word_before.locked() &&
               timestamp_word_before.ToWord() == timestamp_word_after.ToWord();
    }
    return true;
}

void AtomicKvs::GetWithLock(
    const std::string& key,
    std::pair<Timestamp, std::string>* timestamped_value) {
    ASSERT(timestamped_value != nullptr);
    const auto iter = kvs_.find(key);
    ASSERT(iter != kvs_.end());

    Entry& entry = iter->second;
    timestamped_value->first = TimestampWord(entry.word.load()).timestamp();
    timestamped_value->second = entry.value;
}

bool AtomicKvs::TryGet(const std::string& key,
                       std::pair<Timestamp, std::string>* timestamped_value) {
    ASSERT(timestamped_value != nullptr);

    const auto iter = kvs_.find(key);
    if (iter == kvs_.end()) {
        return false;
    }

    Entry& entry = iter->second;
    TimestampWord timestamp_word_before(entry.word.load());
    timestamped_value->first = timestamp_word_before.timestamp();
    timestamped_value->second = entry.value;
    //TODO: put compiler barrier here (check if still necessary if we have an std::atomic access)
    TimestampWord timestamp_word_after(entry.word.load());
    return !timestamp_word_before.locked() &&
           timestamp_word_before.ToWord() == timestamp_word_after.ToWord();
}

void AtomicKvs::WriteLock(const std::string& key, Timestamp* timestamp) {
    Entry& entry = kvs_[key];
    while (true) {
        uint64_t word_before = entry.word.load();
        const TimestampWord timestamp_word_before(word_before);
        if (timestamp_word_before.locked()) {
            continue;
        }

        const TimestampWord timestamp_word(true,
                                           timestamp_word_before.timestamp());
        const bool lock_acquired = entry.word.compare_exchange_weak(
            word_before, timestamp_word.ToWord());
        if (lock_acquired) {
            *timestamp = timestamp_word.timestamp();
            return;
        }
    }
}

bool AtomicKvs::TryWriteLock(const std::string& key, Timestamp* timestamp) {
    Entry& entry = kvs_[key];
    uint64_t word_before = entry.word.load();
    const TimestampWord timestamp_word_before(word_before);
    if (timestamp_word_before.locked()) {
        return false;
    }

    const TimestampWord timestamp_word(true, timestamp_word_before.timestamp());
    const bool lock_acquired =
        entry.word.compare_exchange_weak(word_before, timestamp_word.ToWord());
    if (lock_acquired) {
        *timestamp = timestamp_word.timestamp();
        return true;
    } else {
        return false;
    }
}

void AtomicKvs::PutWithLock(const std::string& key, const std::string& value,
                            const Timestamp& timestamp) {
    // value.size() has to be less than the max_value_size, as opposed to less
    // than or equal to the max_value_size, because we need one character in
    // value for the null terminator.
    ASSERT(value.size() < AtomicKvs::Entry::max_value_size);

    Entry& entry = kvs_[key];
    if (timestamp >= TimestampWord(entry.word.load()).timestamp()) {
        std::strcpy(entry.value, value.c_str());
        entry.word.store(TimestampWord(true, timestamp).ToWord());
    }
}

void AtomicKvs::WriteUnlock(const std::string& key) {
    Entry& entry = kvs_[key];
    const TimestampWord word(entry.word.load());
    ASSERT(word.locked() == true);
    entry.word.store(TimestampWord(false, word.timestamp()).ToWord());
}

void AtomicKvs::Put(const std::string& key, const std::string& value,
                    const Timestamp& timestamp) {
    // value.size() has to be less than the max_value_size, as opposed to less
    // than or equal to the max_value_size, because we need one character in
    // value for the null terminator.
    ASSERT(value.size() < AtomicKvs::Entry::max_value_size);

    Entry& entry = kvs_[key];

    bool lock_acquired = false;
    while (!lock_acquired) {
        uint64_t word_before = entry.word.load();
        TimestampWord timestamp_word_before(word_before);

        // We only write `value` if `timestamp` is larger than or equal to the
        // current timestamp. Because of this, timestamps increase
        // monotonically. Thus, if we have a timestamp smaller than the current
        // timestamp, we'll never have a timestamp larger than the current
        // timestamp, so we'll never end up writing our value. Thus, it's safe
        // to return early and not write anything.
        if (timestamp < timestamp_word_before.timestamp()) {
            return;
        }

        if (timestamp_word_before.locked()) {
            continue;
        }

        TimestampWord timestamp_word(true, timestamp);
        lock_acquired = entry.word.compare_exchange_weak(
            word_before, timestamp_word.ToWord());
    }

    std::strcpy(entry.value, value.c_str());
    entry.word.store(TimestampWord(false, timestamp).ToWord());
}

bool AtomicKvs::IsWriteLocked(const std::string& key) {
    Entry& entry = kvs_[key];
    const TimestampWord word(entry.word.load());
    return word.locked();
}
