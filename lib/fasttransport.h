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
#include "rpc_constants.h"

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

#include <boost/unordered_map.hpp>

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

// A tag attached to every request we send;
// it is passed to the response function
struct req_tag_t {
    erpc::MsgBuffer req_msgbuf;
    erpc::MsgBuffer resp_msgbuf;
    uint8_t reqType;
    //uint8_t sessionIdx;
};

// A basic mempool for preallocated objects of type T. eRPC has a faster,
// hugepage-backed one.
template <class T> class AppMemPool {
    public:
        size_t num_to_alloc = 1;
        std::vector<T *> backing_ptr_vec;
        std::vector<T *> pool;

    void extend_pool() {
        T *backing_ptr = new T[num_to_alloc];
        for (size_t i = 0; i < num_to_alloc; i++) pool.push_back(&backing_ptr[i]);
        backing_ptr_vec.push_back(backing_ptr);
        num_to_alloc *= 2;
    }

    T *alloc() {
        if (pool.empty()) extend_pool();
        T *ret = pool.back();
        pool.pop_back();
        return ret;
    }

    void free(T *t) { pool.push_back(t); }

    AppMemPool() {}
    ~AppMemPool() {
        for (T *ptr : backing_ptr_vec) delete[] ptr;
    }
};

// eRPC context passed between request and responses
class AppContext {
    public:
        struct {
            std::vector<int> session_num_vec;
            // TODO: seems like it's not safe to share a msg buffer among more requests -> don't know
            // when eRPC stops urequiring it
            // erpc::MsgBuffer req_msgbuf;

            // TODO: seems like it might not be safe to share the response message
            // erpc::MsgBuffer resp_msgbuf;

            // This is maintained between calls to GetReqBuf and SendRequest
            // to reduce copying
            req_tag_t *crt_req_tag;
            // Request tags used for RPCs exchanged with the servers
            // TODO: get rid of this and put info in the packet itself?
            AppMemPool<req_tag_t> req_tag_pool;
        } client;

        struct {
            // current req_handle
            erpc::ReqHandle *req_handle;
            std::vector<long> latency_get;
            std::vector<long> latency_prepare;
            std::vector<long> latency_commit;
        } server;

        // common to both servers and clients
        bool unblock; // used by the application to unblock the latest request
        TransportReceiver *receiver = nullptr;
        erpc::Rpc<erpc::CTransport> *rpc = nullptr;
        boost::unordered_map<std::pair<uint8_t, uint8_t>, int> sessions;
};

class FastTransport : public Transport
{
public:
    FastTransport(const transport::Configuration &config,
                  std::string &ip,
                  int nthreads,
                  uint8_t phy_port);
    virtual ~FastTransport();
    void Register(TransportReceiver *receiver,
                  int replicaIdx) override;
    void Run();
    void Wait();
    void Stop();
    int Timer(uint64_t ms, timer_callback_t cb) override;
    bool CancelTimer(int id) override;
    void CancelAllTimers() override;

    bool SendRequestToReplica(uint8_t reqType, uint8_t replicaIdx, uint8_t coreIdx, size_t msgLen, bool blocking) override;
    // TODO: implement this
    bool SendRequestToAll(uint8_t reqType, uint8_t coreIdx, size_t msgLen, bool blocking) override;
    bool SendResponse(size_t msgLen) override;

    char *GetRequestBuf() override;
private:
    // Configuration of the replicas
    transport::Configuration config;

    // The port of the fast NIC
    uint8_t phy_port;

    // Number of server threads
    int nthreads;

    // Index of the replica server
    int replicaIdx;

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

    int GetSession(uint8_t replicaIdx, uint8_t coreIdx);
    bool SendMessageInternal(TransportReceiver *src, int replicaIdx, const Message &m);
    void OnTimer(FastTransportTimerInfo *info);
    static void SocketCallback(evutil_socket_t fd, short what, void *arg);
    static void TimerCallback(evutil_socket_t fd, short what, void *arg);
    static void LogCallback(int severity, const char *msg);
    static void FatalCallback(int err);
    static void SignalCallback(evutil_socket_t fd, short what, void *arg);
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
