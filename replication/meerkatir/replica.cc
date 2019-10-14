// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * replication/ir/replica.cc:
 *   IR Replica server
 *
 **********************************************************************/

#include "replication/meerkatir/replica.h"

#include <cstdint>

#include <set>

namespace replication {
namespace meerkatir {

using namespace std;

Replica::Replica(transport::Configuration config, int myIdx,
                     Transport *transport, AppReplica *app)
    : config(std::move(config)), myIdx(myIdx), transport(transport), app(app)
{
    if (transport != NULL) {
        transport->Register(this, myIdx);
    } else {
        // we use this for micorbenchmarking, but still issue a warning
        Warning("NULL transport provided to the replication layer");
    }
}

Replica::~Replica() { }

void Replica::ReceiveRequest(uint8_t reqType, char *reqBuf, char *respBuf) {
    size_t respLen;
    switch(reqType) {
        case unloggedReqType:
            HandleUnloggedRequest(reqBuf, respBuf, respLen);
            break;
        case inconsistentReqType:
            HandleInconsistentRequest(reqBuf, respBuf, respLen);
            break;
        case consensusReqType:
            HandleConsensusRequest(reqBuf, respBuf, respLen);
            break;
        case finalizeConsensusReqType:
            HandleFinalizeConsensusRequest(reqBuf, respBuf, respLen);
            break;
        default:
            Warning("Unrecognized rquest type: %d", reqType);
    }

    // For every request, we need to send a response (because we use eRPC)
    if (!(transport->SendResponse(respLen)))
        Warning("Failed to send reply message");
}

void Replica::HandleUnloggedRequest(char *reqBuf, char *respBuf, size_t &respLen) {
    // ignore requests from the past
    app->UnloggedUpcall(reqBuf, respBuf, respLen);
}

void Replica::HandleInconsistentRequest(char *reqBuf, char *respBuf, size_t &respLen) {
    auto *req = reinterpret_cast<inconsistent_request_t *>(reqBuf);

    Debug("[%lu - %lu] Received inconsistent op nr %lu\n",
          req->client_id, req->txn_nr, req->req_nr);

    txnid_t txnid = make_pair(req->client_id, req->txn_nr);

    // Check record if we've already handled this request
    RecordEntry *entry = record.Find(txnid);
    TransactionStatus crt_txn_status;
    view_t crt_txn_view;
    RecordEntryState crt_txn_state;
    string crt_txn_result;
    if (entry != NULL) {
        if (req->req_nr <= entry->req_nr) {
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
        entry = &record.Add(0, txnid, req->req_nr, NOT_PREPARED,
                   RECORD_STATE_FINALIZED, "");
        crt_txn_status = NOT_PREPARED;
        crt_txn_view = 0;
    }

    // Call in the application with the current transaction state;
    // the app will decide whether to execute this commit/abort request
    // or not; if yes, it will specify the updated transaction status
    // and the result to return to the coordinator/client.
    app->ExecInconsistentUpcall(txnid, entry, req->commit);

    // TODO: we use eRPC and it expects replies in order so we need
    // to send replies to all requests
    auto *resp = reinterpret_cast<inconsistent_response_t *>(respBuf);
    resp->req_nr = req->req_nr;
    respLen = sizeof(inconsistent_response_t);

    // TODO: for now just trim the log as soon as the transaction was finalized
    // this is not safe for a complete checkpoint
    record.Remove(txnid);
}

void Replica::HandleConsensusRequest(char *reqBuf, char *respBuf, size_t &respLen) {
    auto *req = reinterpret_cast<consensus_request_header_t *>(reqBuf);

    Debug("[%lu - %lu] Received consensus op number %lu:\n",
          req->client_id, req->txn_nr, req->req_nr);

    txnid_t txnid = make_pair(req->client_id, req->txn_nr);

    // Check record if we've already handled this request
    RecordEntry *entry = record.Find(txnid);
    TransactionStatus crt_txn_status;
    view_t crt_txn_view;
    RecordEntryState crt_txn_state;
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
        entry = &record.Add(0, txnid, req->req_nr, NOT_PREPARED,
                   RECORD_STATE_TENTATIVE, "");
        crt_txn_status = NOT_PREPARED;
        crt_txn_view = 0;
    }

    // Call in the application with the current transaction status;
    // the app will decide whether to execute this prepare request
    // or not; if yes, it will specify the updated transaction status
    // and the result to return to the coordinator/client.
    // string result;
    app->ExecConsensusUpcall(txnid, entry, req->nr_reads,
                             req->nr_writes, req->timestamp, req->id,
                             reqBuf + sizeof(consensus_request_header_t),
                             respBuf, respLen);

    if (entry->txn_status != crt_txn_status) {
        // Update record
        //record.SetResult(txnid, result);
        record.SetStatus(txnid, RECORD_STATE_TENTATIVE);
        //crt_txn_result = result;
        crt_txn_state = RECORD_STATE_TENTATIVE;
    }

    // fill in the replication layer specific fields
    // TODO: make this more general
    auto *resp = reinterpret_cast<consensus_response_t *>(respBuf);
    resp->view = crt_txn_view;
    resp->replicaid = myIdx;
    resp->req_nr = req->req_nr;
    resp->finalized = (crt_txn_state == RECORD_STATE_FINALIZED);

    respLen = sizeof(consensus_response_t);
}

void Replica::HandleFinalizeConsensusRequest(char *reqBuf, char *respBuf, size_t &respLen) {
    auto *req = reinterpret_cast<finalize_consensus_request_t *>(reqBuf);

    Debug("[%lu - %lu] Received finalize consensus for req %lu",
          req->client_id, req->txn_nr, req->req_nr);

    txnid_t txnid = make_pair(req->client_id, req->txn_nr);

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

        // if (msg.status() != entry->result) {
        //     // Update the result
        //     // TODO: set the timestamp and status of the transaction
        //     entry->result = msg.result();
        // }

        // Send the reply
        auto *resp = reinterpret_cast<finalize_consensus_response_t *>(respBuf);
        resp->view = entry->view;
        resp->replicaid = myIdx;
        resp->req_nr = req->req_nr;
        respLen = sizeof(finalize_consensus_response_t);

        Debug("[%lu - %lu] Operation found and consensus finalized", req->client_id, req->txn_nr);
    } else {
        // Ignore?
        // Send the reply
        auto *resp = reinterpret_cast<finalize_consensus_response_t *>(respBuf);
        resp->view = 0;
        resp->replicaid = myIdx;
        resp->req_nr = req->req_nr;
        respLen = sizeof(finalize_consensus_response_t);
    }
}

void Replica::PrintStats() {
}

} // namespace ir
} // namespace replication
