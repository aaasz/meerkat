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

#ifndef _TAPIRIR_MESSAGES_H_
#define _TAPIRIR_MESSAGES_H_

#include <functional>
#include <utility>

#include <boost/functional/hash.hpp>

namespace replication {
namespace tapirir {

// Request types; TODO: make more general and add replication
// headers with union of structs
static constexpr uint8_t UNLOGGED_REQUEST = 1;
static constexpr uint8_t CONSENSUS_REQUEST = 2;
static constexpr uint8_t FINALIZE_CONSENSUS_REQUEST = 3; //slow path prepare
static constexpr uint8_t INCONSISTENT_REQUEST = 4;
static constexpr uint8_t FINALIZE_INCONSISTENT_REQUEST = 5;

struct operation_id_t {
    uint64_t client_id;
    uint64_t client_request_number;
};

struct timestamp_t {
    uint64_t timestamp;
    uint64_t client_id;
};

struct unlogged_request_t {
    operation_id_t operation_id;
    char key[64];
};

struct unlogged_response_t {
    operation_id_t operation_id;
    timestamp_t timestamp;
    char value[64];
    int status;
};

struct inconsistent_request_t {
    operation_id_t operation_id;
    uint64_t transaction_number;
    bool commit;
    timestamp_t timestamp;
};

// TODO(mwhittaker): Add view?
struct inconsistent_response_t {
    operation_id_t operation_id;
    uint64_t replica_index;
};

struct finalize_inconsistent_request_t {
    operation_id_t operation_id;
};

struct finalize_inconsistent_response_t {
};

struct consensus_request_t {
    operation_id_t operation_id;
    uint64_t transaction_number;
    timestamp_t timestamp;
    uint8_t num_reads;
    uint8_t num_writes;
};

// TODO(mwhittaker): Add view?
struct consensus_response_t {
    operation_id_t operation_id;
    uint64_t transaction_number;
    uint64_t replica_index;
    timestamp_t timestamp;
    int status;
    bool finalized;
};

struct finalize_consensus_request_t {
    operation_id_t operation_id;
    uint64_t transaction_number;
    int status;
};

// TODO(mwhittaker): Add view?
struct finalize_consensus_response_t {
    operation_id_t operation_id;
    uint64_t replica_index;
};


} // namespace tapirir
} // namespace replication


namespace boost {

template <>
struct hash<replication::tapirir::operation_id_t>
{
    size_t operator()(
            const replication::tapirir::operation_id_t operation_id) const {
        return boost::hash<std::pair<uint64_t, uint64_t>>()({
            operation_id.client_id, operation_id.client_request_number
        });
    }
};

} // namespace boost

namespace std {

template <>
struct equal_to<replication::tapirir::operation_id_t>
{
    size_t operator()(const replication::tapirir::operation_id_t lhs,
                      const replication::tapirir::operation_id_t rhs) const {
        return lhs.client_id == rhs.client_id &&
               lhs.client_request_number == rhs.client_request_number;
    }
};

} // namespace std

#endif  /* _TAPIRIR_MESSAGES_H_ */
