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

namespace replication {
namespace ir {

using namespace std;
using namespace proto;

// TODO: Check if recovery and synchronization still works

IRReplica::IRReplica(transport::Configuration config, int myIdx,
                     Transport *transport, IRAppReplica *app)
    : config(std::move(config)), myIdx(myIdx), transport(transport), app(app)
//      status(STATUS_NORMAL), view(0), latest_normal_view(0),
//      // TODO: Take these filenames in via the command line?
//      persistent_view_info(config.replica(myIdx).host + ":" +
//                           config.replica(myIdx).port + "_" +
//                           std::to_string(myIdx) + ".bin"),
//      // Note that a leader waits for DO-VIEW-CHANGE messages from f other
//      // replicas (as opposed to f + 1) for a total of f + 1 replicas.
//      do_view_change_quorum(config.f)
{
    if (transport != NULL) {
        transport->Register(this, config, myIdx);
    } else {
        // we use this for micorbenchmarking, but still issue a warning
        Warning("NULL transport provided to the replication layer");
    }

//    // If our view info was previously initialized, then we are being started
//    // in recovery mode. If our view info has never been initialized, then this
//    // is the first time we are being run.
//    if (persistent_view_info.Initialized()) {
//        Debug("View information found in %s. Starting recovery.",
//              persistent_view_info.Filename().c_str());
//        status = STATUS_RECOVERING;
//        RecoverViewInfo();
//        Debug("Recovered view = %" PRIu64 " latest_normal_view = %" PRIu64 ".",
//              view, latest_normal_view);
//        ++view;
//        if (myIdx == config.GetLeaderIndex(view)) {
//            // A recoverying replica should not be the leader.
//            ++view;
//        }
//        PersistViewInfo();
//        BroadcastDoViewChangeMessages();
//    } else {
//        PersistViewInfo();
//    }

//    // TODO: Figure out a good view change timeout.
//    const uint64_t view_change_timeout_ms = 10 * 1000;
//    view_change_timeout = std::unique_ptr<Timeout>(
//        new Timeout(transport, view_change_timeout_ms,
//                    [this]() { this->HandleViewChangeTimeout(); }));
//    view_change_timeout->Start();
}

IRReplica::~IRReplica() { }

void
IRReplica::ReceiveMessage(const TransportAddress &remote,
                          const string &type, const string &data)
{
    HandleMessage(remote, type, data);
}

void
IRReplica::HandleMessage(const TransportAddress &remote,
                         const string &type, const string &data)
{
    ProposeInconsistentMessage proposeInconsistent;
    FinalizeInconsistentMessage finalizeInconsistent;
    ProposeConsensusMessage proposeConsensus;
    FinalizeConsensusMessage finalizeConsensus;
    UnloggedRequestMessage unloggedRequest;
//    DoViewChangeMessage doViewChange;
//    StartViewMessage startView;

    if (type == proposeInconsistent.GetTypeName()) {
        proposeInconsistent.ParseFromString(data);
        HandleProposeInconsistent(remote, proposeInconsistent);
    } else if (type == finalizeInconsistent.GetTypeName()) {
        finalizeInconsistent.ParseFromString(data);
        HandleFinalizeInconsistent(remote, finalizeInconsistent);
    } else if (type == proposeConsensus.GetTypeName()) {
        proposeConsensus.ParseFromString(data);
        HandleProposeConsensus(remote, proposeConsensus);
    } else if (type == finalizeConsensus.GetTypeName()) {
        finalizeConsensus.ParseFromString(data);
        HandleFinalizeConsensus(remote, finalizeConsensus);
    } else if (type == unloggedRequest.GetTypeName()) {
        unloggedRequest.ParseFromString(data);
        HandleUnlogged(remote, unloggedRequest);
//    } else if (type == doViewChange.GetTypeName()) {
//        doViewChange.ParseFromString(data);
//        HandleDoViewChange(remote, doViewChange);
//    } else if (type == startView.GetTypeName()) {
//        startView.ParseFromString(data);
//        HandleStartView(remote, startView);
    } else {
        Panic("Received unexpected message type in IR proto: %s",
              type.c_str());
    }
}

void
IRReplica::HandleProposeInconsistent(const TransportAddress &remote,
                                     const ProposeInconsistentMessage &msg)
{
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
    app->ExecInconsistentUpcall(txnid, entry, msg.req().op());


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
    transport->SendMessage(this, remote, reply);
    // }

    // TODO: for now just trim the log as soon as the transaction was finalized
    // this is not safe for a complete checkpoint
    record.Remove(txnid);
}

void
IRReplica::HandleFinalizeInconsistent(const TransportAddress &remote,
                                      const FinalizeInconsistentMessage &msg)
{
	// TODO: Not needed for MultiTapir
	// because the decision to abort or commit will not change (here we
	// assume that the coordinator is not allowed to vote if it is not
	// a participant) so we can consider the ProposeInconsistent to be
	// the finalized decision.
    Panic("Not implemented");

//    uint64_t clientid = msg.opid().clientid();
//    uint64_t clientreq_nr = msg.opid().clientreq_nr();
//    uint64_t clienttxn_nr = msg.clienttxn_nr();
//
//    Debug("[%lu - %lu] Received finalize inconsistent op nr %lu",
//          clientid, clienttxn_nr, clientreq_nr);
//
//    txnid_t txnid = make_pair(clientid, clienttxn_nr);
//
//    // Check record for the request
//    RecordEntry *entry = record.Find(txnid);
//    if (entry != NULL) {
//        if (clientreq_nr < entry->req_nr) {
//            // If finalize for a different operation number, then ignore
//            return;
//        }
//
//        // TODO: check the view? If request in lower
//        // view just reply with the new view and new state
//
//
//        // Mark entry as finalized
//        record.SetStatus(txnid, RECORD_STATE_FINALIZED);
//        // Execute the operation
//        TransactionStatus next_txn_status;
//        app->ExecInconsistentUpcall(txnid, entry->txn_status, next_txn_status, entry->request.op(), false);
//
//        // Send the reply
//        ConfirmMessage reply;
//        reply.set_view(0);
//        reply.set_replicaidx(myIdx);
//        *reply.mutable_opid() = msg.opid();
//
//        // TODO: SendMessage appears not to require concurrency protection
//        transport->SendMessage(this, remote, reply);
//    } else {
//        // Ignore?
//        // TODO: just send a reply if we have it
//        // TODO: if we don't have it just add it?
//    }
}

void
IRReplica::HandleProposeConsensus(const TransportAddress &remote,
                                  const ProposeConsensusMessage &msg)
{
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
    app->ExecConsensusUpcall(txnid, entry, msg.req().op(), result);

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
         transport->SendMessage(this, remote, reply);
    }
}

void
IRReplica::HandleFinalizeConsensus(const TransportAddress &remote,
                                   const FinalizeConsensusMessage &msg)
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
        if (!transport->SendMessage(this, remote, reply)) {
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
        if (!transport->SendMessage(this, remote, reply)) {
            Warning("Failed to send reply message");
        }

        //TODO:
        //Warning("Finalize request for unknown consensus operation");
    }
}

void
IRReplica::HandleUnlogged(const TransportAddress &remote,
                    const UnloggedRequestMessage &msg)
{
    uint64_t clientid = msg.req().clientid();
    uint64_t clientreq_nr = msg.req().clientreqid();
    uint64_t clienttxn_nr = msg.req().clienttxn_nr();

    UnloggedReplyMessage reply;
    string res;


    Debug("[%lu - %lu] Received unlogged request op number %lu:\n%s",
          clientid, clienttxn_nr, clientreq_nr,
          (char *)msg.DebugString().c_str());

    txnid_t txnid = make_pair(clientid, clienttxn_nr);

    app->UnloggedUpcall(txnid, msg.req().op(), res);
    reply.set_reply(res);
    reply.set_clientreq_nr(clientreq_nr);
    if (!(transport->SendMessage(this, remote, reply)))
        Warning("Failed to send reply message");
}

//void
//IRReplica::HandleDoViewChange(const TransportAddress &remote,
//                              const proto::DoViewChangeMessage &msg)
//{
//    Debug(
//        "Received DoViewChangeMessage from replica %d with new_view = %" PRIu64
//        ", latest_normal_view = %" PRIu64 ", has_record = %d.",
//        msg.replicaidx(), msg.new_view(), msg.latest_normal_view(),
//        msg.has_record());
//
//    if (msg.new_view() < view) {
//        Debug("Ignoring DO-VIEW-CHANGE for view %" PRIu64 " < %" PRIu64 ". ",
//              msg.new_view(), view);
//        return;
//    } else if (msg.new_view() == view) {
//        // If we're NORMAL, then we've already completed this view change.
//        if (status == STATUS_NORMAL) {
//            Debug("Ignoring DO-VIEW-CHANGE for view %" PRIu64
//                  " because our status is NORMAL.",
//                  view);
//            return;
//        }
//
//        // If we're a recovering node, we don't want to be the leader.
//        if (status == STATUS_NORMAL) {
//            Debug("Ignoring DO-VIEW-CHANGE for view %" PRIu64
//                  " because our status is RECOVERING.",
//                  view);
//            return;
//        }
//    } else {
//        ASSERT(msg.new_view() > view);
//
//        // Update and persist our view.
//        view = msg.new_view();
//        PersistViewInfo();
//
//        // Update our status. If we're NORMAL, then we transition into
//        // VIEW_CHANGE.  If we're VIEW_CHANGE or RECOVERING, we want to stay in
//        // VIEW_CHANGE or RECOVERING. Note that it would be a bug to transition
//        // from RECOVERING to VIEW_CHANGE before we finish recovering.
//        if (status == STATUS_NORMAL) {
//            status = STATUS_VIEW_CHANGE;
//
//        }
//
//        // We just began a new view change, so we need to broadcast
//        // DO-VIEW-CHANGE messages to everyone.
//        BroadcastDoViewChangeMessages();
//
//        // Restart our view change timer. We don't to perform a view change
//        // right after we just performed a view change.
//        view_change_timeout->Reset();
//    }
//
//    ASSERT(msg.new_view() == view);
//
//    // If we're not the leader of this view change, then we have nothing to do.
//    if (myIdx != config.GetLeaderIndex(view)) {
//        return;
//    }
//
//    // Replicas should send their records to the leader.
//    ASSERT(msg.has_record());
//    const std::map<int, DoViewChangeMessage> *quorum =
//        do_view_change_quorum.AddAndCheckForQuorum(msg.new_view(),
//                                                   msg.replicaidx(), msg);
//    if (quorum == nullptr) {
//        // There is no quorum yet.
//        return;
//    }
//    Debug("Received a quourum of DoViewChangeMessages. Initiating "
//          "IR-MERGE-RECORDS.");
//
//    // Update our record, status, and view.
//    record = IrMergeRecords(*quorum);
//    status = STATUS_NORMAL;
//    view = msg.new_view();
//    latest_normal_view = view;
//    PersistViewInfo();
//
//    // Notify all replicas of the new view.
//    StartViewMessage start_view_msg;
//    record.ToProto(start_view_msg.mutable_record());
//    start_view_msg.set_new_view(view);
//    // TODO: Don't send this message to myself. It's not incorrect, but it's
//    // unnecessary.
//    // TODO: Acknowledge StartViewMessage messages, and rebroadcast them after
//    // a timeout.
//    Debug("Sending StartViewMessages to all replicas.");
//    bool success = transport->SendMessageToAll(this, start_view_msg);
//    if (!success) {
//        Warning("Could not send StartViewMessage.");
//    }
//}

//void
//IRReplica::HandleStartView(const TransportAddress &remote,
//                           const proto::StartViewMessage &msg)
//{
//    Debug("Received StartViewMessage with new_view = %" PRIu64 ".",
//          msg.new_view());
//
//    // A leader should not be sending START-VIEW messages to themselves.
//    ASSERT(myIdx != config.GetLeaderIndex(msg.new_view()));
//
//    if (msg.new_view() < view) {
//        Debug("Ignoring START-VIEW for view %" PRIu64 " < %" PRIu64 ". ",
//              msg.new_view(), view);
//        return;
//    }
//
//    // If new_view == view and we're NORMAL, then we've already completed this
//    // view change, and we don't want to do it again.
//    if (msg.new_view() == view && status == STATUS_NORMAL) {
//        Debug("Ignoring START-VIEW for view %" PRIu64
//              " because our status is NORMAL.",
//              view);
//        return;
//    }
//
//    ASSERT((msg.new_view() >= view) ||
//           (msg.new_view() == view && status != STATUS_NORMAL));
//
//    // Throw away our record for the new master record and call sync.
//    record = Record(msg.record());
//    app->Sync(record.Entries());
//
//    status = STATUS_NORMAL;
//    view = msg.new_view();
//    latest_normal_view = view;
//    PersistViewInfo();
//}

//void IRReplica::HandleViewChangeTimeout() {
//    Debug("HandleViewChangeTimeout fired.");
//    if (status == STATUS_NORMAL) {
//        status = STATUS_VIEW_CHANGE;
//    }
//    ++view;
//    PersistViewInfo();
//    BroadcastDoViewChangeMessages();
//}

//void IRReplica::PersistViewInfo() {
//    PersistedViewInfo view_info;
//    view_info.set_view(view);
//    view_info.set_latest_normal_view(latest_normal_view);
//    std::string output;
//    ASSERT(view_info.SerializeToString(&output));
//    persistent_view_info.Write(output);
//}
//
//void IRReplica::RecoverViewInfo() {
//    PersistedViewInfo view_info;
//    view_info.ParseFromString(persistent_view_info.Read());
//    view = view_info.view();
//    latest_normal_view = view_info.latest_normal_view();
//}

//void IRReplica::BroadcastDoViewChangeMessages() {
//    // Send a DoViewChangeMessage _without_ our record to all replicas except
//    // ourselves and the leader.
//    proto::DoViewChangeMessage msg;
//    msg.set_replicaidx(myIdx);
//    msg.clear_record();
//    msg.set_new_view(view);
//    msg.set_latest_normal_view(latest_normal_view);
//
//    const int leader_idx = config.GetLeaderIndex(view);
//    Debug(
//        "Broadcasting DoViewChangeMessages to replicas with leader id = %d, "
//        "view = %" PRIu64 ", latest_normal_view = %" PRIu64 ".",
//        leader_idx, view, latest_normal_view);
//
//    for (int i = 0; i < config.n; ++i) {
//        if (i == myIdx || i == leader_idx) {
//            continue;
//        }
//
//        bool success = transport->SendMessageToReplica(this, i, msg);
//        if (!success) {
//            Warning("Could not send DoViewChangeMessage to replica %d.", i);
//        }
//    }
//
//    // Send a DoViewChangeMessage _with_ our record to the leader (unless we
//    // are the leader).
//    record.ToProto(msg.mutable_record());
//    if (leader_idx != myIdx) {
//        bool success = transport->SendMessageToReplica(this, leader_idx, msg);
//        if (!success) {
//            Warning("Could not send DoViewChangeMessage to leader %d.",
//                    leader_idx);
//        }
//    }
//}

//Record
//IRReplica::IrMergeRecords(const std::map<int, DoViewChangeMessage>& records) {
//    // TODO: This implementation of IrMergeRecords is not the most efficient in
//    // the world. It could be optimized a bit if it happens to be a bottleneck.
//    // For example, Merge could take in pointers to the record entry vectors.
//
//    // Create a type alias to save some typing.
//    using RecordEntryVec = std::vector<RecordEntry>;
//
//    // Find the largest latest_normal_view.
//    view_t max_latest_normal_view = latest_normal_view;
//    for (const std::pair<const int, DoViewChangeMessage>& p : records) {
//        const DoViewChangeMessage& msg = p.second;
//        max_latest_normal_view =
//            std::max(max_latest_normal_view, msg.latest_normal_view());
//    }
//
//    // Collect the records with largest latest_normal_view.
//    std::vector<Record> latest_records;
//    for (const std::pair<const int, DoViewChangeMessage>& p : records) {
//        const DoViewChangeMessage& msg = p.second;
//        if (msg.latest_normal_view() == max_latest_normal_view) {
//            ASSERT(msg.has_record());
//            latest_records.push_back(Record(msg.record()));
//        }
//    }
//    if (latest_normal_view == max_latest_normal_view) {
//        latest_records.push_back(std::move(record));
//    }
//
//    // Group together all the entries from all the records in latest_records.
//    // We'll use this to build d and u. Simultaneously populate R.
//    // TODO: Avoid redundant copies.
//    Record R;
//    std::map<opid_t, RecordEntryVec> entries_by_opid;
//    for (const Record &r : latest_records) {
//        for (const std::pair<const opid_t, RecordEntry> &p : r.Entries()) {
//            const opid_t &opid = p.first;
//            const RecordEntry &entry = p.second;
//            ASSERT(opid == entry.opid);
//
//            if (entry.type == RECORD_TYPE_INCONSISTENT) {
//                // TODO: Do we have to update the view here?
//                if (R.Find(opid) == nullptr) {
//                    R.Add(entry);
//                }
//            } else if (entry.state == RECORD_STATE_FINALIZED) {
//                // TODO: Do we have to update the view here?
//                if (R.Find(opid) == nullptr) {
//                    R.Add(entry);
//                }
//                entries_by_opid.erase(opid);
//            } else {
//                ASSERT(entry.type == RECORD_TYPE_CONSENSUS &&
//                       entry.state == RECORD_STATE_TENTATIVE);
//                // If R already contains this operation, then we don't group
//                // it.
//                if (R.Entries().count(entry.opid) == 0) {
//                    entries_by_opid[entry.opid].push_back(entry);
//                }
//            }
//        }
//    }
//
//    // Build d and u.
//    std::map<opid_t, RecordEntryVec> d;
//    std::map<opid_t, RecordEntryVec> u;
//    std::map<opid_t, std::string> majority_results_in_d;
//    for (const std::pair<const opid_t, RecordEntryVec> &p : entries_by_opid) {
//        const opid_t &opid = p.first;
//        const RecordEntryVec &entries = p.second;
//
//        // Count the frequency of each response.
//        std::map<std::string, std::size_t> result_counts;
//        for (const RecordEntry& entry : entries) {
//            result_counts[entry.result] += 1;
//        }
//
//        // Check if any response occurs ceil(f/2) + 1 times or more.
//        bool in_d = false;
//        std::string majority_result_in_d = "";
//        for (const std::pair<const std::string, std::size_t> &c :
//             result_counts) {
//            if (c.second >= ceil(0.5 * config.f) + 1) {
//                majority_result_in_d = c.first;
//                in_d = true;
//                break;
//            }
//        }
//
//        // TODO: Avoid redundant copies.
//        if (in_d) {
//            d[opid] = entries;
//            majority_results_in_d[opid] = majority_result_in_d;
//        } else {
//            u[opid] = entries;
//        }
//    }
//
//    // Sync.
//    app->Sync(R.Entries());
//
//    // Merge.
//    std::map<opid_t, std::string> results_by_opid =
//        app->Merge(d, u, majority_results_in_d);
//
//    // Sanity check Merge results. Every opid should be present.
//    ASSERT(results_by_opid.size() == d.size() + u.size());
//    for (const std::pair<const opid_t, std::string> &p : results_by_opid) {
//        const opid_t &opid = p.first;
//        ASSERT(d.count(opid) + u.count(opid) == 1);
//    }
//
//    // Convert Merge results into a Record.
//    Record merged;
//    for (std::pair<const opid_t, std::string> &p : results_by_opid) {
//        const opid_t &opid = p.first;
//        std::string &result = p.second;
//
//        const std::vector<RecordEntry> entries = entries_by_opid[opid];
//        ASSERT(records.size() > 0);
//        const RecordEntry &entry = entries[0];
//
//        // TODO: Is this view correct?
//        merged.Add(view, opid, entry.request, RECORD_STATE_FINALIZED,
//                   entry.type, std::move(result));
//    }
//
//    // R = R cup merged.
//    for (const std::pair<const opid_t, RecordEntry> &r : merged.Entries()) {
//        // TODO: Avoid copy.
//        R.Add(r.second);
//    }
//    return R;
//}

} // namespace ir
} // namespace replication
