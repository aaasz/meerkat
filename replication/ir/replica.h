// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * replication/ir/replica.h:
 *   IR Replica server
 *
 **********************************************************************/

#ifndef _IR_REPLICA_H_
#define _IR_REPLICA_H_

#include <memory>

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/fasttransport.h"
#include "replication/common/quorumset.h"
#include "replication/ir/record.h"


namespace replication {
namespace ir {


class IRAppReplica
{
public:
    IRAppReplica() { };
    virtual ~IRAppReplica() { };
    // Invoke inconsistent operation, no return value
    virtual void ExecInconsistentUpcall(txnid_t txn_id,
                                        RecordEntry *crt_txn_state,
                                        bool commit) { };

    // Invoke consensus operation
    virtual void ExecConsensusUpcall(txnid_t txn_id,
                            RecordEntry *crt_txn_state,
                            uint8_t nr_reads,
                            uint8_t nr_writes,
                            uint64_t timestamp,
                            uint64_t id,
                            char *reqBuf,
                            char *respBuf, size_t &respLen) { };

    // Invoke unreplicated operation
    virtual void UnloggedUpcall(char *reqBuf, char *respBuf, size_t &respLen) { };

    // Sync
    virtual void Sync(const std::map<txnid_t, RecordEntry>& record) { };
    // Merge
    virtual std::map<txnid_t, std::string> Merge(
        const std::map<txnid_t, std::vector<RecordEntry>> &d,
        const std::map<txnid_t, std::vector<RecordEntry>> &u,
        const std::map<txnid_t, std::string> &majority_results_in_d) {
        return {};
    };
};


class IRReplica : TransportReceiver
{
public:
    IRReplica(transport::Configuration config, int myIdx,
              Transport *transport, IRAppReplica *app);
    ~IRReplica();

    // Message handlers.
    void ReceiveRequest(uint8_t reqType, char *reqBuf, char *respBuf) override;
    void ReceiveResponse(uint8_t reqType, char *respBuf, bool &unblock) override {}; // TODO: for now, replicas
                                            // do not need to communicate
                                            // with eachother; they will need
                                            // to for synchronization

    // new handlers
    void HandleUnloggedRequest(char *reqBuf, char *respBuf, size_t &respLen);
    void HandleInconsistentRequest(char *reqBuf, char *respBuf, size_t &respLen);
    void HandleConsensusRequest(char *reqBuf, char *respBuf, size_t &respLen);
    void HandleFinalizeConsensusRequest(char *reqBuf, char *respBuf, size_t &respLen);

    void PrintStats();

private:
    transport::Configuration config;
    int myIdx; // Replica index into config.
    Transport *transport;
    IRAppReplica *app;

    std::vector<uint64_t> latency_get;
    std::vector<uint64_t> latency_prepare;
    std::vector<uint64_t> latency_commit;

    // Transactions are fully partitioned across cores => no synchronization needed;
    // The record now maintains just one entry per transaction (as opposed to
    // one entry per operation, i.e., consensus, inconsistent);
    // The upcalls into the application now provide the old state of the
    // transaction and the app computes its next state;
    Record record;
};

} // namespace ir
} // namespace replication

#endif /* _IR_REPLICA_H_ */
