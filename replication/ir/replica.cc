// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * replication/ir/replica.cc:
 *   IR Replica server
 *
 **********************************************************************/

#include "replication/ir/replica.h"

#include <cstdint>

#include <set>

#define IRREPLICA_MEASURE_APP_TIMES false

namespace replication {
namespace ir {

using namespace std;
using namespace proto;

#if  IRREPLICA_MEASURE_APP_TIMES
static constexpr double kAppLatFac = 100.0;        // Precision factor for latency
#endif


// TODO: Check if recovery and synchronization still works

IRReplica::IRReplica(transport::Configuration config, int myIdx,
                     Transport *transport, IRAppReplica *app)
    : config(std::move(config)), myIdx(myIdx), transport(transport), app(app)
{
    if (transport != NULL) {
        transport->Register(this, config, myIdx);
    } else {
        // we use this for micorbenchmarking, but still issue a warning
        Warning("NULL transport provided to the replication layer");
    }
}

IRReplica::~IRReplica() { }

void
IRReplica::ReceiveMessage(const string &type, const string &data,
                          bool &unblock)
{
    HandleMessage(type, data);
}

void
IRReplica::HandleMessage(const string &type, const string &data)
{
    ProposeInconsistentMessage proposeInconsistent;
    FinalizeInconsistentMessage finalizeInconsistent;
    ProposeConsensusMessage proposeConsensus;
    FinalizeConsensusMessage finalizeConsensus;
    UnloggedRequestMessage unloggedRequest;

    if (type == proposeInconsistent.GetTypeName()) {
        proposeInconsistent.ParseFromString(data);
        HandleProposeInconsistent(proposeInconsistent);
    } else if (type == finalizeInconsistent.GetTypeName()) {
        finalizeInconsistent.ParseFromString(data);
        HandleFinalizeInconsistent(finalizeInconsistent);
    } else if (type == proposeConsensus.GetTypeName()) {
        proposeConsensus.ParseFromString(data);
        HandleProposeConsensus(proposeConsensus);
    } else if (type == finalizeConsensus.GetTypeName()) {
        finalizeConsensus.ParseFromString(data);
        HandleFinalizeConsensus(finalizeConsensus);
    } else if (type == unloggedRequest.GetTypeName()) {
        unloggedRequest.ParseFromString(data);
        HandleUnlogged(unloggedRequest);
    } else {
        Panic("Received unexpected message type in IR proto: %s",
              type.c_str());
    }
}

void
IRReplica::HandleProposeInconsistent(const ProposeInconsistentMessage &msg)
{
#if  IRREPLICA_MEASURE_APP_TIMES
    struct timespec s, e;
#endif
    uint64_t clientid = msg.req().clientid();
    uint64_t clientreq_nr = msg.req().clientreqid();
    uint64_t clienttxn_nr = msg.req().clienttxn_nr();

    Debug("[%lu - %lu] Received inconsistent op nr %lu:\n%s",
          clientid, clienttxn_nr, clientreq_nr,
          (char *)msg.DebugString().c_str());

    txnid_t txnid = make_pair(clientid, clienttxn_nr);

    // Check record if we've already handled this request
    RecordEntry *entry = record.Find(txnid);
    TransactionStatus crt_txn_status;
    view_t crt_txn_view;
    proto::RecordEntryState crt_txn_state;
    string crt_txn_result;
    if (entry != NULL) {
        if (clientreq_nr <= entry->req_nr) {
            Warning("Client request from the past.");
            // If a client request number from the past, ignore it
            return;
        }

        // If we already have this transaction in our record,
        // save the txn's current state
        crt_txn_view = entry->view;
        // TODO: check the view? If request in lower
        // view just reply with the new view and new state
        crt_txn_status = entry->txn_status;
        crt_txn_state = entry->state;
        crt_txn_result = entry->result;
    } else {
        // We've never seen this transaction before,
        // save it as tentative, in initial view,
        // with initial status
        entry = &record.Add(0, txnid, clientreq_nr, NOT_PREPARED,
                   RECORD_STATE_FINALIZED, msg.req(), "");
        crt_txn_status = NOT_PREPARED;
        crt_txn_view = 0;
    }

    // Call in the application with the current transaction state;
    // the app will decide whether to execute this commit/abort request
    // or not; if yes, it will specify the updated transaction status
    // and the result to return to the coordinator/client.
#if  IRREPLICA_MEASURE_APP_TIMES
    clock_gettime(CLOCK_REALTIME, &s);
#endif
    app->ExecInconsistentUpcall(txnid, entry, msg.req().op());
#if  IRREPLICA_MEASURE_APP_TIMES
    clock_gettime(CLOCK_REALTIME, &e);
    double lat_us = (e.tv_nsec-s.tv_nsec)/1000.0 * kAppLatFac;
    latency_commit.push_back(static_cast<uint64_t>(lat_us));
#endif

    // TODO: we use eRPC and it expects replies in order so we need
    // to send replies to all requests

    // We should not even submit a reply if
    // app returns NOT_PREPARED
    // if (entry->txn_status != NOT_PREPARED) {
    //     if (entry->txn_status != crt_txn_status) {
    //         // Update record
    //         record.SetStatus(txnid, RECORD_STATE_TENTATIVE);
    //         record.SetRequest(txnid, msg.req());
    //         record.SetReqNr(txnid, clientreq_nr);
    //         crt_txn_state = RECORD_STATE_TENTATIVE;
    //     }

    ReplyInconsistentMessage reply;
    reply.set_view(crt_txn_view);
    reply.set_replicaidx(myIdx);
    reply.mutable_opid()->set_clientid(clientid);
    reply.mutable_opid()->set_clientreq_nr(clientreq_nr);
    //     // TODO: should set this to true for MTapir -- no need for
    //     // inconsistent finalize messages
    reply.set_finalized(crt_txn_state == RECORD_STATE_FINALIZED);

    //      // Send the reply
    transport->SendMessage(this, reply);
    // }

    // TODO: for now just trim the log as soon as the transaction was finalized
    // this is not safe for a complete checkpoint
    record.Remove(txnid);
}

void
IRReplica::HandleFinalizeInconsistent(const FinalizeInconsistentMessage &msg)
{
	// TODO: Not needed for MultiTapir
	// because the decision to abort or commit will not change (here we
	// assume that the coordinator is not allowed to vote if it is not
	// a participant) so we can consider the ProposeInconsistent to be
	// the finalized decision.
    Panic("Not implemented");
}

void
IRReplica::HandleProposeConsensus(const ProposeConsensusMessage &msg)
{
#if  IRREPLICA_MEASURE_APP_TIMES
    struct timespec s, e;
#endif
    uint64_t clientid = msg.req().clientid();
    uint64_t clientreq_nr = msg.req().clientreqid();
    uint64_t clienttxn_nr = msg.req().clienttxn_nr();

    Debug("[%lu - %lu] Received consensus op number %lu:\n%s",
          clientid, clienttxn_nr, clientreq_nr,
          (char *)msg.DebugString().c_str());

    txnid_t txnid = make_pair(clientid, clienttxn_nr);

    // Check record if we've already handled this request
    RecordEntry *entry = record.Find(txnid);
    TransactionStatus crt_txn_status;
    view_t crt_txn_view;
    proto::RecordEntryState crt_txn_state;
    string crt_txn_result;
    if (entry != NULL) {
        //if (clientreq_nr <= entry->req_nr) {
            // If a client request number from the past, ignore it
        //    return;
        //}
        // If we already have this transaction in our record,
        // save the txn's current state
        crt_txn_view = entry->view;
        // TODO: check the view? If request in lower
        // view just reply with the new view and new state
        crt_txn_status = entry->txn_status;
        crt_txn_state = entry->state;
        crt_txn_result = entry->result;
    } else {
        // It's the first prepare request we've seen for this transaction,
        // save it as tentative, in initial view, with initial status
        entry = &record.Add(0, txnid, clientreq_nr, NOT_PREPARED,
                   RECORD_STATE_TENTATIVE, msg.req(), "");
        crt_txn_status = NOT_PREPARED;
        crt_txn_view = 0;
    }

    // Call in the application with the current transaction status;
    // the app will decide whether to execute this prepare request
    // or not; if yes, it will specify the updated transaction status
    // and the result to return to the coordinator/client.
    string result;
#if  IRREPLICA_MEASURE_APP_TIMES
    clock_gettime(CLOCK_REALTIME, &s);
#endif
    app->ExecConsensusUpcall(txnid, entry, msg.req().op(), result);
#if  IRREPLICA_MEASURE_APP_TIMES
    clock_gettime(CLOCK_REALTIME, &e);
    double lat_us = (e.tv_nsec-s.tv_nsec)/1000.0 * kAppLatFac;
    latency_prepare.push_back(static_cast<uint64_t>(lat_us));
#endif
    
    // We should not even submit a reply if
    // app returns NOT_PREPARED
    if (entry->txn_status != NOT_PREPARED) {
        if (entry->txn_status != crt_txn_status) {
            // Update record
            record.SetResult(txnid, result);
            record.SetStatus(txnid, RECORD_STATE_TENTATIVE);
            crt_txn_result = result;
            crt_txn_state = RECORD_STATE_TENTATIVE;
        }

        ReplyConsensusMessage reply;
        reply.set_view(crt_txn_view);
        reply.set_replicaidx(myIdx);
        reply.mutable_opid()->set_clientid(clientid);
        reply.mutable_opid()->set_clientreq_nr(clientreq_nr);
        reply.set_result(crt_txn_result);
        reply.set_finalized(crt_txn_state == RECORD_STATE_FINALIZED);

         // Send the reply
         transport->SendMessage(this, reply);
    }
}

void
IRReplica::HandleFinalizeConsensus(const FinalizeConsensusMessage &msg)
{
    uint64_t clientid = msg.opid().clientid();
    uint64_t clientreq_nr = msg.opid().clientreq_nr();
    uint64_t clienttxn_nr = msg.clienttxn_nr();

    Debug("[%lu - %lu] Received finalize consensus for req %lu",
          clientid, clienttxn_nr, clientreq_nr);

    txnid_t txnid = make_pair(clientid, clienttxn_nr);

    // Check record for the request
    RecordEntry *entry = record.Find(txnid);
    if (entry != NULL) {
        //if (clientreq_nr < entry->req_nr) {
            // If finalize for a different operation number, then ignore
            // TODO: what if we missed a prepare phase, thus clientreq_nr > entry->req_nr?
        //    return;
        //}

        // TODO: check the view? If request in lower
        // view just reply with the new view and new state

        // Mark entry as finalized
        record.SetStatus(txnid, RECORD_STATE_FINALIZED);

        if (msg.result() != entry->result) {
            // Update the result
            // TODO: set the timestamp and status of the transaction
            entry->result = msg.result();
        }

        // Send the reply
        ConfirmMessage reply;
        reply.set_view(entry->view);
        reply.set_replicaidx(myIdx);
        *reply.mutable_opid() = msg.opid();

        // TODO: SendMessage appears not to require concurrency protection
        if (!transport->SendMessage(this, reply)) {
            Warning("Failed to send reply message");
        }

        Debug("[%lu - %lu] Operation found and consensus finalized", clientid, clienttxn_nr);
    } else {
        // Ignore?
        // Send the reply
        ConfirmMessage reply;
        reply.set_view(0);
        reply.set_replicaidx(myIdx);
        *reply.mutable_opid() = msg.opid();

        // TODO: SendMessage appears not to require concurrency protection
        if (!transport->SendMessage(this, reply)) {
            Warning("Failed to send reply message");
        }

        //TODO:
        //Warning("Finalize request for unknown consensus operation");
    }
}

void
IRReplica::HandleUnlogged(const UnloggedRequestMessage &msg)
{
#if  IRREPLICA_MEASURE_APP_TIMES
    struct timespec s, e;
#endif
    uint64_t clientid = msg.req().clientid();
    uint64_t clientreq_nr = msg.req().clientreqid();
    uint64_t clienttxn_nr = msg.req().clienttxn_nr();

    UnloggedReplyMessage reply;
    string res;


    Debug("[%lu - %lu] Received unlogged request op number %lu:\n%s",
          clientid, clienttxn_nr, clientreq_nr,
          (char *)msg.DebugString().c_str());

    txnid_t txnid = make_pair(clientid, clienttxn_nr);

#if  IRREPLICA_MEASURE_APP_TIMES
    clock_gettime(CLOCK_REALTIME, &s);
#endif
    app->UnloggedUpcall(txnid, msg.req().op(), res);
#if  IRREPLICA_MEASURE_APP_TIMES
    clock_gettime(CLOCK_REALTIME, &e);
    double lat_us = (e.tv_nsec-s.tv_nsec)/1000.0 * kAppLatFac;
    latency_get.push_back(static_cast<uint64_t>(lat_us));
#endif

    reply.set_reply(res);
    reply.set_clientreq_nr(clientreq_nr);
    if (!(transport->SendMessage(this, reply)))
        Warning("Failed to send reply message");
}

void
IRReplica::PrintStats() {
#if  IRREPLICA_MEASURE_APP_TIMES
    std::sort(latency_get.begin(), latency_get.end());
    std::sort(latency_prepare.begin(), latency_prepare.end());
    std::sort(latency_commit.begin(), latency_commit.end());

    uint64_t latency_get_size = latency_get.size();
    uint64_t latency_prepare_size = latency_prepare.size();
    uint64_t latency_commit_size = latency_prepare.size();

    double latency_get_50 = latency_get[(latency_get_size*50)/100] / kAppLatFac;
    double latency_get_99 = latency_get[(latency_get_size*99)/100] / kAppLatFac;

    double latency_prepare_50 = latency_prepare[(latency_prepare_size*50)/100] / kAppLatFac;
    double latency_prepare_99 = latency_prepare[(latency_prepare_size*99)/100] / kAppLatFac;

    double latency_commit_50 = latency_commit[(latency_commit_size*50)/100] / kAppLatFac;
    double latency_commit_99 = latency_commit[(latency_commit_size*99)/100] / kAppLatFac;

    uint64_t latency_get_sum = std::accumulate(latency_get.begin(), latency_get.end(), 0);
    uint64_t latency_prepare_sum = std::accumulate(latency_prepare.begin(), latency_prepare.end(), 0);
    uint64_t latency_commit_sum = std::accumulate(latency_commit.begin(), latency_commit.end(), 0);

    double latency_get_avg = latency_get_sum/latency_get_size/kAppLatFac;
    double latency_prepare_avg = latency_prepare_sum/latency_prepare_size/kAppLatFac;
    double latency_commit_avg = latency_commit_sum/latency_commit_size/kAppLatFac;

    fprintf(stderr, "Get latency (size = %d) [avg: %.2f; 50 percentile: %.2f; 99 percentile: %.2f] us \n",
            latency_get_size,
            latency_get_avg,
            latency_get_50,
            latency_get_99);

    fprintf(stderr, "Prepare latency (size = %d) [avg: %.2f; 50 percentile: %.2f; 99 percentile: %.2f] us \n",
            latency_prepare_size,
            latency_prepare_avg,
            latency_prepare_50,
            latency_prepare_99);

    fprintf(stderr, "Commit latency (size = %d) [avg: %.2f; 50 percentile: %.2f; 99 percentile: %.2f] us \n",
            latency_commit_size,
            latency_commit_avg,
            latency_commit_50,
            latency_commit_99);
#endif
}

} // namespace ir
} // namespace replication
