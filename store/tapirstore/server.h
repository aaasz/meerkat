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

#ifndef _TAPIRSTORE_SERVER_H_
#define _TAPIRSTORE_SERVER_H_

#include "replication/tapirir/replica.h"
#include "store/common/timestamp.h"
#include "store/common/truetime.h"
#include "store/tapirstore/store.h"

namespace tapirstore {

class Server : public replication::tapirir::IRAppReplica
{
public:
    Server(bool linearizable) : store(linearizable) {}

    void UnloggedUpcall(char *request_buffer, char *response_buffer,
                        size_t &response_length) override;
    void InconsistentUpcall(
        const replication::tapirir::inconsistent_request_t& request) override;
    void ConsensusUpcall(txnid_t txn_id,
                         replication::tapirir::timestamp_t timestamp,
                         Transaction transaction,
                         replication::tapirir::consensus_response_t* response)
                         override;
    void Load(const std::string &key, const std::string &value,
              const Timestamp timestamp);
    void PrintStats();

private:
    Store store;

    // Latencies in microseconds.
    std::vector<long> latency_get_us;
    std::vector<long> latency_prepare_us;
    std::vector<long> latency_commit_us;
    std::vector<long> latency_abort_us;
};

} // namespace tapirstore

#endif /* _TAPIRSTORE_SERVER_H_ */
