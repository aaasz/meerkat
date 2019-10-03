#ifndef _MEERKATIR_MESSAGES_H_
#define _MEERKATIR_MESSAGES_H_

namespace replication {
namespace meerkatir {

// Request types; TODO: make more general and add replication
// headers with union of structs
static constexpr uint8_t unloggedReqType = 1;
static constexpr uint8_t consensusReqType = 2;
static constexpr uint8_t finalizeConsensusReqType = 3; //slow path prepare
static constexpr uint8_t inconsistentReqType = 4;

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

struct inconsistent_request_t {
    uint64_t client_id;
    uint64_t req_nr;
    uint64_t txn_nr;
    bool commit;
};

struct inconsistent_response_t {
    uint64_t req_nr;
};

struct consensus_request_header_t {
    uint64_t client_id;
    uint64_t req_nr;
    uint64_t txn_nr;
    uint64_t timestamp;
    uint64_t id;
    uint8_t nr_reads;
    uint8_t nr_writes;
};

struct consensus_response_t {
    uint64_t req_nr;
    uint64_t txn_nr;
    uint64_t replicaid;
    uint64_t view;
    uint64_t timestamp;
    uint64_t id;
    int status;
    bool finalized;
};

struct finalize_consensus_request_t {
    uint64_t client_id;
    uint64_t req_nr;
    uint64_t txn_nr;
    int status;
};

struct finalize_consensus_response_t {
    uint64_t client_id;
    uint64_t req_nr;
    uint64_t view;
    uint64_t replicaid;
};


} // namespace meerkatir
} // namespace replication


#endif  /* _MEERKATIR_MESSAGES_H_ */