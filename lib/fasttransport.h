// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * udptransport.h:
 *   message-passing network interface that uses UDP message delivery
 *   and libasync
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

#ifndef _LIB_FASTTRANSPORT_H_
#define _LIB_FASTTRANSPORT_H_

#include "lib/configuration.h"
#include "lib/transport.h"

#include "rpc.h"
#include "util/numautils.h"
#include <gflags/gflags.h>

#include <event2/event.h>

#include <map>
#include <list>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <random>
#include <mutex>
#include <atomic>
#include <netinet/in.h>

/*
 * Class FastTransport implements a multi-threaded DPDK
 * transport layer based on eRPC which works with
 * a client - server configuration, where the server
 * may have multiple replicas.
 *
 * The Register function is used to register a transport
 * receiver. The transport is responsible for sending and
 * dispatching messages from/to its receivers accordingly.
 * A transport receiver can either be a client or a server
 * replica. A transport instance's receivers must be
 * of the same type.
 */

// Use just one function, TransportRceiver will do the multiplexing
static constexpr uint8_t reqType = 2;

// eRPC context passed between request and responses
class AppContext {
    public:
        struct {
            // TODO: these should be vectors
            // (connection details to every replica)
            int session_num;
            erpc::MsgBuffer req_msgbuf;
            erpc::MsgBuffer resp_msgbuf;
        } client;

        struct {
            // current req_handle
            erpc::ReqHandle *req_handle;
        } server;

        // common to both servers and clients
        bool unblock; // used by the application to unblock the latest request
        TransportReceiver *receiver = nullptr;
        erpc::Rpc<erpc::CTransport> *rpc = nullptr;
};

class FastTransport : public Transport
{
public:
    FastTransport(std::string local_uri, int nthreads, uint8_t phy_port, bool blocking);
    virtual ~FastTransport();
    void Register(TransportReceiver *receiver,
                  const transport::Configuration &config,
                  int replicaIdx) override;
    void Run();
    void Wait();
    void Stop();
    int Timer(uint64_t ms, timer_callback_t cb) override;
    bool CancelTimer(int id) override;
    void CancelAllTimers() override;
    
    bool SendMessageToReplica(TransportReceiver *src, int replicaIdx,
                         const Message &m) override;

    // SendMessage is actuall a reply on the same channel we got the request
    bool SendMessage(TransportReceiver *src, const Message &m) override;
    bool SendMessageToAll(TransportReceiver *src, const Message &m) override;

private:
    // The port of the fast NIC
    uint8_t phy_port;

    // Number of server threads
    int nthreads;

    // Index of the replica server
    int replicaIdx;

    // The "blocking" variable puts the transport in a blocking mode:
    // after sending a request, it block until the application unblocks it
    bool blocking;

    transport::Configuration *config;

    struct FastTransportTimerInfo
    {
        FastTransport *transport;
        timer_callback_t cb;
        event *ev;
        int id;
    };

    event_base *eventBase;
    std::vector<event *> signalEvents;
    AppContext *c;
    bool stop = false;

    // TODO: find some other method to deal with timeouts (hidden in eRPC?)
    std::atomic<int> lastTimerId;
    using timers_map = std::map<int, FastTransportTimerInfo *>;
    timers_map timers;
    std::mutex timers_lock;

    bool SendMessageInternal(TransportReceiver *src, int replicaIdx,
                         const Message &m);
    void OnTimer(FastTransportTimerInfo *info);
    static void SocketCallback(evutil_socket_t fd,
                               short what, void *arg);
    static void TimerCallback(evutil_socket_t fd,
                              short what, void *arg);
    static void LogCallback(int severity, const char *msg);
    static void FatalCallback(int err);
    static void SignalCallback(evutil_socket_t fd,
                               short what, void *arg);
};

// A basic session management handler that expects successful responses
static void basic_sm_handler(int session_num, erpc::SmEventType sm_event_type,
                      erpc::SmErrType sm_err_type, void *_context) {

    auto *c = static_cast<AppContext *>(_context);

    ASSERT(sm_err_type == erpc::SmErrType::kNoError);
    //  "SM response with error " + erpc::sm_err_type_str(sm_err_type));

    if (!(sm_event_type == erpc::SmEventType::kConnected ||
          sm_event_type == erpc::SmEventType::kDisconnected)) {
        throw std::runtime_error("Received unexpected SM event.");
    }

    Debug("Rpc %u: Session number %d %s. Error %s. "
            "Time elapsed = %.3f s.\n",
            c->rpc->get_rpc_id(), session_num,
            erpc::sm_event_type_str(sm_event_type).c_str(),
            erpc::sm_err_type_str(sm_err_type).c_str(),
            c->rpc->sec_since_creation());
}

#endif  // _LIB_FASTTRANSPORT_H_
