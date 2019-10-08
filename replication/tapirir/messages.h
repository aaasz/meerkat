#ifndef _TAPIRIR_MESSAGES_H_
#define _TAPIRIR_MESSAGES_H_

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


#endif  /* _TAPIRIR_MESSAGES_H_ */
