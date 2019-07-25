// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * udptransport.cc:
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

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/fasttransport.h"

#include <google/protobuf/message.h>
#include <event2/event.h>
#include <event2/thread.h>

#include <memory>
#include <random>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <signal.h>
#include <thread>
#include <sched.h>

using std::pair;

#define FASTTRANSPORT_MEASURE_TIMES false

erpc::Nexus *nexus;
static std::mutex fasttransport_lock;
static volatile bool fasttransport_initialized = false;
// Used to assign increasing thread_ids, used as rpc object IDs
static uint8_t fasttransport_thread_counter;

// Function called when we received a response to a
// request we sent on this transport (TODO: for now, just GETs)
static void fasttransport_response(void *_context, void *reqType) {
#if FASTTRANSPORT_MEASURE_TIMES
    struct timespec s, e;
    clock_gettime(CLOCK_REALTIME, &s);
#endif
    Debug("Received respose, reqType = %d", *reinterpret_cast<uint8_t *>(reqType));
    auto *c = static_cast<AppContext *>(_context);
    c->receiver->ReceiveResponse(*reinterpret_cast<uint8_t *>(reqType),
                                 reinterpret_cast<char *>(c->client.resp_msgbuf.buf),
                                 c->unblock);
#if FASTTRANSPORT_MEASURE_TIMES
    clock_gettime(CLOCK_REALTIME, &e);
    //printf("fasttransport_rpc_response cost; size =  %d, latency = %.02f us\n", sz, (e.tv_nsec-s.tv_nsec)/1000.0);
#endif
}

// Function called when we received a request (TODO: for now just GETs)
static void fasttransport_request(erpc::ReqHandle *req_handle, void *_context) {
#if FASTTRANSPORT_MEASURE_TIMES
    struct timespec s, e;
    clock_gettime(CLOCK_REALTIME, &s);
#endif
    // save the req_handle for when we are in the SendMessage function
    auto *c = static_cast<AppContext *>(_context);
    c->server.req_handle = req_handle;
    // upcall to the app
    c->receiver->ReceiveRequest(req_handle->get_req_type(),
                                reinterpret_cast<char *>(req_handle->get_req_msgbuf()->buf),
                                reinterpret_cast<char *>(req_handle->pre_resp_msgbuf.buf));
#if FASTTRANSPORT_MEASURE_TIMES
    clock_gettime(CLOCK_REALTIME, &e);
    // update latency vector
    long lat_ns = e.tv_nsec-s.tv_nsec;
    if (e.tv_nsec < s.tv_nsec) { //clock underflow
        lat_ns += 1000000000;
    };
    c->server.latency_get.push_back(lat_ns);
    //printf("fasttransport_rpc_request cost; size =  %d, latency = %.02f us\n", sz, (e.tv_nsec-s.tv_nsec)/1000.0);
#endif
}

FastTransport::FastTransport(std::string local_uri, int nthreads, uint8_t phy_port, bool blocking)
    : phy_port(phy_port),
      nthreads(nthreads),
      blocking(blocking) {

    // The first thread to grab the lock initializes the transport
    fasttransport_lock.lock();

    if (fasttransport_initialized) {
        // Create the event_base to schedule requests
        eventBase = event_base_new();
        evthread_make_base_notifiable(eventBase);
    } else {
        // Setup libevent
        //evthread_use_pthreads(); // TODO: do we really need this even
                                 // when we manipulate one eventbase
                                 // per thread?
        event_set_log_callback(LogCallback);
        event_set_fatal_callback(FatalCallback);

        // Create the event_base to schedule requests
        eventBase = event_base_new();
        evthread_make_base_notifiable(eventBase);

        // signals must be registered only on one eventBase
        // signalEvents.push_back(evsignal_new(eventBase, SIGTERM,
        //         SignalCallback, this));
        // signalEvents.push_back(evsignal_new(eventBase, SIGINT,
        //         SignalCallback, this));

        for (event *x : signalEvents) {
            event_add(x, NULL);
        }

        // Setup eRPC
        Debug("Creating nexus objects with local_uri = %s", local_uri.c_str());
        nexus = new erpc::Nexus(local_uri, 0, 0);
        // TODO: give a list of reqType numbers and register them to
        // the same handler
        nexus->register_req_func(unloggedReqType, fasttransport_request,
                            erpc::ReqFuncType::kForeground);
        nexus->register_req_func(inconsistentReqType, fasttransport_request,
                            erpc::ReqFuncType::kForeground);
        nexus->register_req_func(consensusReqType, fasttransport_request,
                            erpc::ReqFuncType::kForeground);
        nexus->register_req_func(finalizeConsensusReqType, fasttransport_request,
                            erpc::ReqFuncType::kForeground);
        fasttransport_thread_counter = 0;
        fasttransport_initialized = true;
    }

    fasttransport_lock.unlock();
}

FastTransport::~FastTransport() {
}

void FastTransport::Register(TransportReceiver *receiver,
      const transport::Configuration &config, int replicaIdx) {

	ASSERT(replicaIdx < config.n);

    //const transport::Configuration *canonicalConfig =
    //     RegisterConfiguration(receiver, config, replicaIdx);

    // If it is a client, we open a session to the same
    // server thread id on every replica server.
    // If it is a server, we just create an rpc object
    // and wait for sessions.

    c = new AppContext();
    c->receiver = receiver;
    c->unblock = false;
    this->replicaIdx = replicaIdx;
    this->config = new transport::Configuration(config);
}

inline char *FastTransport::GetRequestBuf() {
    return reinterpret_cast<char *>(c->client.req_msgbuf.buf);
}

// This function assumes the message has already been copied to the
// req_msgbuf
bool FastTransport::SendMessageToReplica(uint8_t reqType, int replicaIdx, size_t msgLen) {

    c->rpc->resize_msg_buffer(&c->client.req_msgbuf, msgLen);
    // TODO: select the corresponding session for replicaIdx
    c->rpc->enqueue_request(c->client.session_num, reqType, &c->client.req_msgbuf,
                          &c->client.resp_msgbuf, fasttransport_response,
                          reinterpret_cast<uint8_t *>(&reqType));
    if (blocking) {
        while (!c->unblock) {
            c->rpc->run_event_loop_once();
        }
        c->unblock = false;
    }

    return true;
}

// Assumes we already put the response in c->server.req_handle->pre_resp_msgbuf
bool FastTransport::SendResponse(size_t msgLen) {
    // we get here from fasttransport_rpc_request
    auto &resp = c->server.req_handle->pre_resp_msgbuf;
    c->rpc->resize_msg_buffer(&resp, msgLen);
    c->rpc->enqueue_response(c->server.req_handle, &resp);
    return true;
}

void FastTransport::Run() {

    // Get an increasing id
    fasttransport_lock.lock();
    uint8_t  id = fasttransport_thread_counter;
    fasttransport_thread_counter++;
    fasttransport_lock.unlock();

    // Create one rpc object per thread
    erpc::Rpc<erpc::CTransport> *rpc = new erpc::Rpc<erpc::CTransport> (nexus, static_cast<void *>(c),
                                           static_cast<uint8_t>(id),
                                           basic_sm_handler, phy_port);
    rpc->retry_connect_on_invalid_rpc_id = true;

    c->rpc = rpc;

    Debug("rpc object created for this transport");

    if (replicaIdx == -1) {
        // TODO: Open a session to every replica
        const string &host = config->replica(0).host;
        const string &port = config->replica(0).port;
        Debug("Openning eRPC session to %s", (host + ":" + port).c_str());
        c->client.session_num = c->rpc->create_session(host + ":" + port, id % nthreads);

        while (!c->rpc->is_connected(c->client.session_num)) c->rpc->run_event_loop_once();
    }

    // Pre-allocate MsgBuffers (for now, maximum one packet per RPC)
    c->client.req_msgbuf = c->rpc->alloc_msg_buffer_or_die(c->rpc->get_max_data_per_pkt());
    c->client.resp_msgbuf = c->rpc->alloc_msg_buffer_or_die(c->rpc->get_max_data_per_pkt());

    Debug("Starting the fast transport!");

    if (!blocking) {
        while(!stop)
            c->rpc->run_event_loop(500);
    }
}

void FastTransport::Stop() {
    Debug("Stopping transport!");
#if FASTTRANSPORT_MEASURE_TIMES
    // Discard first third of collected results

    fprintf(stderr, "Printing stats:\n");
    std::vector<long> latency_get = c->server.latency_get;
    std::vector<long> latency_prepare = c->server.latency_prepare;
    std::vector<long> latency_commit = c->server.latency_commit;

    std::sort(latency_get.begin() + latency_get.size()/3, latency_get.end() - latency_get.size()/3);
    std::sort(latency_prepare.begin() + latency_prepare.size()/3, latency_prepare.end() - latency_prepare.size()/3);
    std::sort(latency_commit.begin() + latency_commit.size()/3, latency_commit.end() - latency_prepare.size()/3);

    double latency_get_50 = latency_get[latency_get.size()/3 + ((latency_get.size() - 2 * latency_get.size()/3)*50)/100] / 1000.0;
    double latency_get_99 = latency_get[latency_get.size()/3 + ((latency_get.size() - 2 * latency_get.size()/3)*99)/100] / 1000.0;

    double latency_prepare_50 = latency_prepare[latency_get.size()/3 + ((latency_prepare.size() - 2 * latency_prepare.size()/3)*50)/100] / 1000.0;
    double latency_prepare_99 = latency_prepare[latency_get.size()/3 + ((latency_prepare.size() - 2 * latency_prepare.size()/3)*99)/100] / 1000.0;

    double latency_commit_50 = latency_commit[latency_get.size()/3 + ((latency_commit.size() - 2 * latency_commit.size()/3)*50)/100] / 1000.0;
    double latency_commit_99 = latency_commit[latency_get.size()/3 + ((latency_commit.size() - 2 * latency_commit.size()/3)*99)/100] / 1000.0;

    long latency_get_sum = std::accumulate(latency_get.begin() + latency_get.size()/3, latency_get.end() - latency_get.size()/3, 0);
    long latency_prepare_sum = std::accumulate(latency_prepare.begin() + latency_prepare.size()/3, latency_prepare.end() - latency_prepare.size()/3, 0);
    long latency_commit_sum = std::accumulate(latency_commit.begin() + latency_commit.size()/3, latency_commit.end() - latency_commit.size()/3, 0);

    double latency_get_avg = latency_get_sum/(latency_get.size() - 2 * latency_get.size()/3)/1000.0;
    double latency_prepare_avg = latency_prepare_sum/(latency_prepare.size() - 2 * latency_prepare.size()/3)/1000.0;
    double latency_commit_avg = latency_commit_sum/(latency_commit.size() - 2 * latency_commit.size()/3)/1000.0;

    fprintf(stderr, "Get latency (size = %lu) [avg: %.2f; 50 percentile: %.2f; 99 percentile: %.2f] us \n",
            latency_get.size(),
            latency_get_avg,
            latency_get_50,
            latency_get_99);

    fprintf(stderr, "Prepare latency (size = %lu) [avg: %.2f; 50 percentile: %.2f; 99 percentile: %.2f] us \n",
            latency_prepare.size(),
            latency_prepare_avg,
            latency_prepare_50,
            latency_prepare_99);

    fprintf(stderr, "Commit latency (size = %lu) [avg: %.2f; 50 percentile: %.2f; 99 percentile: %.2f] us \n",
            latency_commit.size(),
            latency_commit_avg,
            latency_commit_50,
            latency_commit_99);
#endif
    stop = true;
}

int FastTransport::Timer(uint64_t ms, timer_callback_t cb) {
    FastTransportTimerInfo *info = new FastTransportTimerInfo();

    struct timeval tv;
    tv.tv_sec = ms/1000;
    tv.tv_usec = (ms % 1000) * 1000;
    
    ++lastTimerId;
    
    info->transport = this;
    info->id = lastTimerId;
    info->cb = cb;
    info->ev = event_new(eventBase, -1, 0,
                         TimerCallback, info);

    if (info->ev == NULL) {
        Debug("Error creating new Timer event : %d", lastTimerId.load());
    }

    timers_lock.lock();
    timers[info->id] = info;
    timers_lock.unlock();

    int ret = event_add(info->ev, &tv);
    if (ret != 0) {
        Debug("Error adding new Timer event to eventbase %d", lastTimerId.load());
    }
    
    return info->id;
}

bool FastTransport::CancelTimer(int id)
{
    FastTransportTimerInfo *info = timers[id];

    if (info == NULL) {
         return false;
    }

    event_del(info->ev);
    event_free(info->ev);

    timers_lock.lock();
    timers.erase(info->id);
    timers_lock.unlock();

    delete info;
    
    return true;
}

void FastTransport::CancelAllTimers() {
    Debug("Cancelling all Timers");
    while (!timers.empty()) {
        auto kv = timers.begin();
        CancelTimer(kv->first);
    }
}

void FastTransport::OnTimer(FastTransportTimerInfo *info) {
    timers_lock.lock();
    timers.erase(info->id);
    timers_lock.unlock();

    event_del(info->ev);
    event_free(info->ev);

    info->cb();

    delete info;
}

void FastTransport::TimerCallback(evutil_socket_t fd, short what, void *arg) {
    FastTransport::FastTransportTimerInfo *info =
        (FastTransport::FastTransportTimerInfo *)arg;

    ASSERT(what & EV_TIMEOUT);

    info->transport->OnTimer(info);
}

void FastTransport::LogCallback(int severity, const char *msg) {
    Message_Type msgType;
    switch (severity) {
    case _EVENT_LOG_DEBUG:
        msgType = MSG_DEBUG;
        break;
    case _EVENT_LOG_MSG:
        msgType = MSG_NOTICE;
        break;
    case _EVENT_LOG_WARN:
        msgType = MSG_WARNING;
        break;
    case _EVENT_LOG_ERR:
        msgType = MSG_WARNING;
        break;
    default:
        NOT_REACHABLE();
    }

    _Message(msgType, "libevent", 0, NULL, "%s", msg);
}

void FastTransport::FatalCallback(int err) {
    Panic("Fatal libevent error: %d", err);
}

void FastTransport::SignalCallback(evutil_socket_t fd,
      short what, void *arg) {
    Notice("Terminating on SIGTERM/SIGINT");
    FastTransport *transport = (FastTransport *)arg;
    //event_base_loopbreak(libeventBase);
    transport->Stop();
}
