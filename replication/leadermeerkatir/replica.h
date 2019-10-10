// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * pb/replica.h:
 *   Primary backup replication for transaction updates
 *
 * Copyright 2013 Dan R. K. Ports  <drkp@cs.washington.edu>
 *           2018 Adriana Szekeres  <aaasz@cs.washington.edu>
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

#ifndef _LEADERMEERKATIR_REPLICA_H_
#define _LEADERMEERKATIR_REPLICA_H_

#include "lib/message.h"
#include "lib/fasttransport.h"
#include "lib/configuration.h"
#include "replication/common/record.h"
#include "replication/common/quorumset.h"
#include "store/common/transaction.h"
#include "replication/common/record.h"
#include "replication/leadermeerkatir/messages.h"
#include "replication/leadermeerkatir/viewstamp.h"

#include <map>
#include <memory>
#include <list>
#include <mutex>
#include <atomic>

namespace replication {
namespace leadermeerkatir {

enum ReplicaStatus {
    STATUS_NORMAL,
    STATUS_VIEW_CHANGE,
    STATUS_RECOVERING
};

class IRAppReplica
{
public:
    IRAppReplica() { };
    virtual ~IRAppReplica() { };

    // Invoke callback on the leader, with the option to replicate on success
    virtual void LeaderUpcall(txnid_t txn_id,
                      replication::RecordEntry *crt_txn_state,
                      char *reqBuf, bool &replicate) { };
    virtual void LeaderUpcallPostPrepare(txnid_t txn_id,
                                 replication::RecordEntry *crt_txn_state) { };
    // Invoke callback on all replicas
    virtual void ReplicaUpcall(txnid_t txn_id,
                       replication::RecordEntry *crt_txn_state) { };
    // Invoke call back for unreplicated operations run on only one replica
    virtual void UnloggedUpcall(char *reqBuf, char *respBuf, size_t &respLen) { };
};


class IRReplica : public TransportReceiver
{
public:
    IRReplica(transport::Configuration config, int myIdx,
              Transport *transport, //unsigned int batchSize,
              IRAppReplica *app);
    ~IRReplica();

    // Message handlers.
    void ReceiveRequest(uint64_t reqHandleIdx, uint8_t reqType, char *reqBuf, char *respBuf) override;
    void ReceiveResponse(uint8_t reqType, char *respBuf) override;

    bool Blocked() override { return false; };
    void PrintStats();

private:
    view_t view;
    transport::Configuration config;
    int myIdx; // Replica index into config.
    Transport *transport;
    IRAppReplica *app;
    ReplicaStatus status;

    // Transactions are fully partitioned across cores => no synchronization needed;
    // The record now maintains just one entry per transaction (as opposed to
    // one entry per operation, i.e., consensus, inconsistent);
    // The upcalls into the application now provide the old state of the
    // transaction and the app computes its next state;
    Record record;

    // Set containing responses to prepare messages
    QuorumSet<viewstamp_t, prepare_response_t> prepareResponseQuorum;

//    std::map<uint64_t, std::unique_ptr<TransportAddress> > clientAddresses;
//    struct ClientTableEntry
//    {
//        uint64_t lastReqId;
//        bool replied;
//        proto::ReplyMessage reply;
//    };
//    std::map<uint64_t, ClientTableEntry> clientTable;

//    QuorumSet<view_t, proto::StartViewChangeMessage> startViewChangeQuorum;
//    QuorumSet<view_t, proto::DoViewChangeMessage> doViewChangeQuorum;

//    Timeout *viewChangeTimeout;
//    Timeout *nullCommitTimeout;

    bool AmLeader() const;

    void HandleRequest(uint64_t reqHandleIdx, char *reqBuf, char *respBuf);
    void HandleUnloggedRequest(uint64_t reqHandleIdx, char *reqBuf, char *respBuf);
    void HandlePrepare(uint64_t reqHandleIdx, char *reqBuf, char *respBuf);
    void HandlePrepareReply(char *respBuf);
    void HandleCommit(uint64_t reqHandleIdx, char *reqBuf, char *respBuf);
    void HandleCommitReply(char *respBuf);
};

} // namespace leadermeerkatir
} // namespace replication

#endif  /* _LEADERMEERKATIR_REPLICA_H_ */
