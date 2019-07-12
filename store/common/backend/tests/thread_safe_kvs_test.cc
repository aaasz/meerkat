// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/common/backend/thread_safe_kvs_test.cc
 *   Test cases for thread safe key-value stores.
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

#include <random>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

#include "store/common/backend/atomic_kvs.h"
#include "store/common/backend/pthread_kvs.h"
#include "store/common/backend/thread_safe_kvs.h"

namespace {

TEST(ThreadSafeKvsTest, SmokeTest) {
    PthreadKvs pthread_kvs;
    AtomicKvs atomic_kvs;
    std::vector<ThreadSafeKvs*> kvss = {&pthread_kvs, &atomic_kvs};

    constexpr int num_items = 10;
    for (ThreadSafeKvs* kvs : kvss) {
        // Load the database.
        for (int i = 0; i < num_items; ++i) {
            kvs->Put(std::to_string(i), std::to_string(i), Timestamp(i, i));
        }

        // Launch a set of readers.
        std::vector<std::thread> readers;
        for (int i = 0; i < 5; ++i) {
            readers.push_back(std::thread([&kvs]() {
                std::random_device rd;
                std::mt19937 gen(rd());
                std::uniform_int_distribution<> dis(0, num_items - 1);

                for (int i = 0; i < 1000; ++i) {
                    std::pair<Timestamp, std::string> timestamped_value;
                    kvs->TryGet(std::to_string(dis(gen)), &timestamped_value);
                    kvs->Get(std::to_string(dis(gen)), &timestamped_value);
                }
            }));
        }

        // Launch a set of writers.
        std::vector<std::thread> writers;
        for (int i = 0; i < 5; ++i) {
            readers.push_back(std::thread([&kvs]() {
                std::random_device rd;
                std::mt19937 gen(rd());
                std::uniform_int_distribution<> dis(0, num_items - 1);

                for (int i = 0; i < 1000; ++i) {
                    std::pair<Timestamp, std::string> timestamped_value;
                    kvs->Put(std::to_string(dis(gen)), std::to_string(dis(gen)),
                             Timestamp(dis(gen), dis(gen)));

                    Timestamp timestamp;
                    const std::string key = std::to_string(dis(gen));
                    kvs->WriteLock(key, &timestamp);
                    kvs->PutWithLock(key, key, Timestamp(dis(gen), dis(gen)));
                    kvs->GetWithLock(key, &timestamped_value);
                    kvs->WriteUnlock(key);

                    if (kvs->TryWriteLock(key, &timestamp)) {
                        kvs->GetWithLock(key, &timestamped_value);
                        kvs->PutWithLock(key, key, Timestamp(dis(gen), dis(gen)));
                        kvs->WriteUnlock(key);
                    }
                }
            }));
        }

        // Join the readers and writers.
        for (std::thread& reader : readers) {
            reader.join();
        }
        for (std::thread& writer : writers) {
            writer.join();
        }
    }
}

TEST(ThreadSafeKvsTest, ReadAndWriteTest) {
    PthreadKvs pthread_kvs;
    AtomicKvs atomic_kvs;
    std::vector<ThreadSafeKvs*> kvss = {&pthread_kvs, &atomic_kvs};

    constexpr int num_items = 10;
    for (ThreadSafeKvs* kvs : kvss) {
        // Load the database at timestamp 0.
        for (int i = 0; i < num_items; ++i) {
            kvs->Put(std::to_string(i), std::to_string(i), Timestamp(0, 0));
        }

        // Read the database.
        for (int i = 0; i < num_items; ++i) {
            std::pair<Timestamp, std::string> timestamped_value;
            kvs->Get(std::to_string(i), &timestamped_value);
            EXPECT_EQ(timestamped_value.first, Timestamp(0, 0));
            EXPECT_EQ(timestamped_value.second, std::to_string(i));
        }

        // Load the database at timestamp 1.
        for (int i = 0; i < num_items; ++i) {
            kvs->Put(std::to_string(i), std::to_string(2 * i), Timestamp(0, 1));
        }

        // Read the database.
        for (int i = 0; i < num_items; ++i) {
            std::pair<Timestamp, std::string> timestamped_value;
            kvs->Get(std::to_string(i), &timestamped_value);
            EXPECT_EQ(timestamped_value.first, Timestamp(0, 1));
            EXPECT_EQ(timestamped_value.second, std::to_string(2 * i));
        }

        // Load the database at timestamp 0. This should not overwrite any
        // values because the timestamp is lower than the existing timestamp.
        for (int i = 0; i < num_items; ++i) {
            kvs->Put(std::to_string(i), std::to_string(i), Timestamp(0, 0));
        }

        // Read the database.
        for (int i = 0; i < num_items; ++i) {
            std::pair<Timestamp, std::string> timestamped_value;
            kvs->Get(std::to_string(i), &timestamped_value);
            EXPECT_EQ(timestamped_value.first, Timestamp(0, 1));
            EXPECT_EQ(timestamped_value.second, std::to_string(2 * i));
        }

        // Load the database at timestamp 1.
        for (int i = 0; i < num_items; ++i) {
            Timestamp old_timestamp;
            kvs->WriteLock(std::to_string(i), &old_timestamp);

            // This put should succeed.
            kvs->PutWithLock(std::to_string(i), std::to_string(3 * i),
                             Timestamp(0, 1));

            // This put should fail.
            kvs->PutWithLock(std::to_string(i), std::to_string(4 * i),
                             Timestamp(0, 0));

            kvs->WriteUnlock(std::to_string(i));
        }

        // Read the database.
        for (int i = 0; i < num_items; ++i) {
            std::pair<Timestamp, std::string> timestamped_value;
            kvs->Get(std::to_string(i), &timestamped_value);
            EXPECT_EQ(timestamped_value.first, Timestamp(0, 1));
            EXPECT_EQ(timestamped_value.second, std::to_string(3 * i));
        }
    }
}

}  // namespace
