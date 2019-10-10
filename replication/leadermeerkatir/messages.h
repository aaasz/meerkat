#ifndef _LEADERMEERKATIR_MESSAGES_H_
#define _LEADERMEERKATIR_MESSAGES_H_

namespace replication {
namespace leadermeerkatir {

// Request types;
const uint8_t clientReqType = 1;
const uint8_t unloggedReqType = 2;
const uint8_t prepareReqType = 3;
const uint8_t commitReqType = 4;

struct request_header_t {
    uint64_t client_id;
    uint64_t req_nr;
    uint64_t txn_nr;
    uint8_t nr_reads;
    uint8_t nr_writes;
};

struct request_response_t {
    uint64_t req_nr;
    uint64_t view;
    int status;
};

struct unlogged_request_t {
    uint64_t req_nr;
    char key[64];
};

struct unlogged_response_t {
    uint64_t req_nr;
    uint64_t timestamp;
    uint64_t id;
    char value[64];
    int status;
};

struct prepare_request_header_t {
    request_header_t request_header;
    uint64_t view;
    uint64_t timestamp;
    uint64_t id;
};

struct commit_request_t {
    uint64_t client_id;
    uint64_t req_nr;
    uint64_t view;
    uint64_t txn_nr;
};

struct prepare_response_t {
    uint64_t client_id;
    uint64_t req_nr;
    uint64_t view;
    uint64_t txn_nr;
    uint32_t replica_idx;
};

struct commit_response_t {
    uint64_t req_nr;
};

}      // namespace leadermeerkatir
}      // namespace replication


#endif  /* _LEADERMEERKATIR_MESSAGES_H_ */