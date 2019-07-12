// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/common/backend/atomic_kvs.h
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

#ifndef _ATOMIC_KVS_H_
#define _ATOMIC_KVS_H_

#include <atomic>
#include <unordered_map>

#include "store/common/backend/thread_safe_kvs.h"

#define CACHE_LINE_SIZE 64

// AtomicKvs implements the ThreadSafeKvs using atomic reads and writes. Most
// notably, AtomicKvs implements invisible reads like Silo [1] and Tictoc [2].
// That is, reads do not write to shared-memory.
//
// # Timestamp Words
// AtomicKvs associates a timestamp word with every key-value pair. A timestamp
// word is a 64-bit word with the following format.
//
//   bit 63                                                            bit 0
//   |                                                                     |
//   ABBBBBBB BBBBBBBB BBBBBBBB BBBBBBBB BBBBBBBB BBBBBBBB BCCCCCCC CCCCCCCC
//
//   - The most significant bit (bit A) is a 1 if the word is locked or 0 if
//     the word is unlocked.
//   - The remaining 63 bits encode a Timestamp. The next 48 bits (B bits)
//     store the timestamp portion of a Timestamp.
//   - The final 15 bits (C bits) store the user id portion of a Timestamp.
//
// # Writes
// Writes use a compare-and-swap operation to safely set the locked bit of a
// timestamp word. Consider a thread trying to write a value v to key k with
// timestamp t. The code to do so looks like this.
//
//   while True:
//     old_timestamp_word = atomically read the timestamp word for k;
//     if the locked bit in old_timestamp_word is set:
//       continue;
//
//     new_timestamp_word = a timestamp word with locked bit set and timestamp
//     t;
//     use a compare-and-swap to atomically replace the timestamp word with
//     new_timestamp_word if the timestamp_word is equal to old_timestamp_word;
//     if the compare-and-swap succeeded:
//       write v
//       atomically store new_timestamp_word but with the locked bit unset
//
// This logic is taken from Algorithm 5 of [2].
//
// # Reads
// Reads are taken from Algorithm 4 of [2] and descriptions from [1]. To read
// the value for key k, a thread reads the timestamp word for k, then reads the
// value of k, then reads the timestamp word for k again. If the two words are
// equal and the locked bit is not set, then the read was successful.
// Otherwise, the thread keeps trying.
//
// [1]: https://scholar.google.com/scholar?cluster=1808818331949135820
// [2]: https://scholar.google.com/scholar?cluster=7246772973103959497
class AtomicKvs : public ThreadSafeKvs {
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
    // See above for documentation. tl;dr:
    //   - 1 locked bit
    //   - 48 timestamp bits
    //   - 15 user id bits
    class TimestampWord {
    public:
        // Construct a TimestampWord with a particular locked flag and
        // Timestamp.
        TimestampWord(bool locked, const Timestamp& timestamp);

        // Parse a TimestampWord from a 64-bit timestmap word.
        explicit TimestampWord(uint64_t word);

        // Convert a TimestampWord into a 64-bit timestamp word.
        uint64_t ToWord() const;

        // Return whether the locked bit is set.
        bool locked() const { return locked_; }

        // Return the Timestamp of the TimestampWord.
        const Timestamp& timestamp() const { return timestamp_; }

    private:
        bool locked_;
        Timestamp timestamp_;
    };

    struct Entry {
        Entry() : word(0) {
            for (int i = 0; i < max_value_size; ++i) {
                value[i] = '\0';
            }
        }

        // Note: std::atomic<uint64_t> ensures word is naturally-aligned
        // (on a 64-bit boundary), which makes loads atomic on 64-bit
        // architectures without any other synchronization instructions;
        // this enables invisible reads.
        //
        // Used as a short lock to ensure the consistency of value
        std::atomic<uint64_t> word;

        // TODO(mwhittaker): Previously, this was a string. This led to a
        // concurrency bug. A Get reads the word, then reads the value, then
        // reads the word. If the two words are the same, the read is
        // successful. The problem is during the read, another thread can be
        // writing the value. When two threads concurrently read and write the
        // same string, things can crash.
        //
        // Instead of that, we use a char[]. Does that fix things? It seems to,
        // but I'm not 100% sure that it's guaranteed to work. We use a size of
        // enough bytes to pad to the cache line size. If the strings you write
        // are bigger than this, things might crash.
        //
        // value should always end in a null terminator.
        static constexpr int max_value_size =
                    CACHE_LINE_SIZE - sizeof(word);
        char value[max_value_size];
    } __attribute__((__aligned__(CACHE_LINE_SIZE)));

    std::unordered_map<std::string, Entry> kvs_;
};

#endif  //  _ATOMIC_KVS_H_
