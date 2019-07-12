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

erpc::Nexus *nexus;
static std::mutex fasttransport_lock;
static volatile bool fasttransport_initialized = false;
// Used to assign increasing thread_ids, used as rpc object IDs
static uint8_t fasttransport_thread_counter;

struct timeval t0, t1;

static size_t SerializeMessage(const ::google::protobuf::Message &m,
      char *out) {
    string data = m.SerializeAsString();
    string type = m.GetTypeName();
    size_t typeLen = type.length();
    size_t dataLen = data.length();
    ssize_t totalLen = (typeLen + sizeof(typeLen) +
                       dataLen + sizeof(dataLen));

    char *ptr = out;
    *((size_t *) ptr) = typeLen;
    ptr += sizeof(size_t);
    ASSERT(ptr-buf < totalLen);
    ASSERT(ptr+typeLen-buf < totalLen);
    memcpy(ptr, type.c_str(), typeLen);
    ptr += typeLen;
    *((size_t *) ptr) = dataLen;
    ptr += sizeof(size_t);
    ASSERT(ptr-buf < totalLen);
    ASSERT(ptr+dataLen-buf == totalLen);
    memcpy(ptr, data.c_str(), dataLen);
    ptr += dataLen;

    return totalLen;
}

static void DecodePacket(const char *buf, size_t sz, string &type, string &msg) {
    const char *ptr = buf;
    size_t typeLen = *((size_t *)ptr);
    ptr += sizeof(size_t);

    ASSERT(ptr-buf < (int)sz);
    ASSERT(ptr+typeLen-buf < (int)sz);

    type = string(ptr, typeLen);
    ptr += typeLen;

    size_t msgLen = *((size_t *)ptr);
    ptr += sizeof(size_t);

    ASSERT(ptr-buf < (int)sz);
    ASSERT(ptr+msgLen-buf <= (int)sz);

    msg = string(ptr, msgLen);
    ptr += msgLen;
 
}

// Function called when we received a response to an
// RPC we sent on this transport
static void fasttransport_rpc_response(void *_context, void *) { 
    auto *c = static_cast<AppContext *>(_context);
    const auto &resp_msgbuf = c->client.resp_msgbuf;

    std::string msgType, msg;
    size_t sz = resp_msgbuf.get_data_size();

    gettimeofday(&t0, NULL);
    DecodePacket((char*)resp_msgbuf.buf, sz, msgType, msg);
    gettimeofday(&t1, NULL);

    fprintf(stderr, "Decode message, size =  %d, latency = %lu us\n", sz, (t1.tv_sec - t0.tv_sec)*1000000 + (t1.tv_usec - t0.tv_usec));

    FastTransportAddress a;
    gettimeofday(&t0, NULL);
    c->receiver->ReceiveMessage(a, msgType, msg);
    gettimeofday(&t1, NULL);

    fprintf(stderr, "ReceiveMessage cost; size =  %d, latency = %lu us\n", sz, (t1.tv_sec - t0.tv_sec)*1000000 + (t1.tv_usec - t0.tv_usec));

}

// Function called when we received an RPC request 
static void fasttransport_rpc_request(erpc::ReqHandle *req_handle, void *_context) {

    auto *c = static_cast<AppContext *>(_context);
    const auto *req_msgbuf = req_handle->get_req_msgbuf();

    // save the req_handle for when we are in the SendMessage function
    c->server.req_handle = req_handle;

    std::string msgType, msg;
    size_t sz = req_msgbuf->get_data_size();
    char *req = reinterpret_cast<char *>(req_msgbuf->buf);

    gettimeofday(&t0, NULL);
    DecodePacket(req, sz, msgType, msg);
    gettimeofday(&t1, NULL);

    fprintf(stderr, "Decode message, size =  %d, latency = %lu us\n", sz, (t1.tv_sec - t0.tv_sec)*1000000 + (t1.tv_usec - t0.tv_usec));

    Debug("Received message, msgType = %s", msgType.c_str());

    FastTransportAddress a;

    gettimeofday(&t0, NULL);
    c->receiver->ReceiveMessage(a, msgType, msg);
    gettimeofday(&t1, NULL);

    fprintf(stderr, "ReceiveMessage cost; size =  %d, latency = %lu us\n", sz, (t1.tv_sec - t0.tv_sec)*1000000 + (t1.tv_usec - t0.tv_usec));
}

FastTransport::FastTransport(std::string local_uri, int nthreads, uint8_t phy_port)
    : nthreads(nthreads),
      phy_port(phy_port) {

    // The first thread to grab the lock initializes the transport
    fasttransport_lock.lock();

    if (fasttransport_initialized) {
        // Create the event_base to schedule requests
        eventBase = event_base_new();
        evthread_make_base_notifiable(eventBase);
    } else {
        // Setup libevent
        evthread_use_pthreads(); // TODO: do we really need this even
                                 // when we manipulate one eventbase
                                 // per thread?
        event_set_log_callback(LogCallback);
        event_set_fatal_callback(FatalCallback);

        // Create the event_base to schedule requests
        eventBase = event_base_new();
        evthread_make_base_notifiable(eventBase);

        // signals must be registered only on one eventBase
        signalEvents.push_back(evsignal_new(eventBase, SIGTERM,
                SignalCallback, this));
        signalEvents.push_back(evsignal_new(eventBase, SIGINT,
                SignalCallback, this));

        for (event *x : signalEvents) {
            event_add(x, NULL);
        }

        // Setup eRPC
        Debug("Creating nexus objects with local_uri = %s", local_uri.c_str());
        nexus = new erpc::Nexus(local_uri, 0, 0);
        fasttransport_thread_counter = 0;
        fasttransport_initialized = true;

        nexus->register_req_func(reqType, fasttransport_rpc_request,
                            erpc::ReqFuncType::kForeground);
    }

    fasttransport_lock.unlock();
}

FastTransport::~FastTransport() {
    // event_base_loopbreak(libeventBase);

    // for (auto kv : timers) {
    //     delete kv.second;
    // }

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
    this->replicaIdx = replicaIdx;
    this->config = new transport::Configuration(config);
}

bool FastTransport::SendMessageToReplica(TransportReceiver *src,
      int replicaIdx, const Message &m) {

    erpc::MsgBuffer &req_msgbuf = c->client.req_msgbuf;
    ASSERT(req_msgbuf.get_data_size() == c->rpc->get_max_data_per_pkt());

    // Serialize message
    //std::unique_ptr<char[]> unique_buf;
    

    size_t msgLen = SerializeMessage(m, reinterpret_cast<char *>(req_msgbuf.buf));

    Debug("SendMessageToReplica msgLen = %d", msgLen);
    c->rpc->resize_msg_buffer(&req_msgbuf, msgLen);

    c->rpc->enqueue_request(c->client.session_num, reqType, &c->client.req_msgbuf,
                          &c->client.resp_msgbuf, fasttransport_rpc_response,
                          nullptr);

    Debug("SendMessageToReplica request enqueued", msgLen);
    return true;
}

bool FastTransport::SendMessageToAll(TransportReceiver *src,
      const Message &m) {

    // TODO: send to all, not just replica 0
    SendMessageToReplica(src, 0, m);
    return true;
}

// Assume we use this only to send replies
bool FastTransport::SendMessage(TransportReceiver *src,
      const TransportAddress &dst, const Message &m) {

    // we get here from fasttransport_rpc_request
    auto &resp = c->server.req_handle->pre_resp_msgbuf;

    gettimeofday(&t0, NULL);
    size_t msgLen = SerializeMessage(m, reinterpret_cast<char *>(resp.buf));
    gettimeofday(&t1, NULL);
    c->rpc->resize_msg_buffer(&resp, msgLen);

    fprintf(stderr, "SendMessage, Serialize cost: size =  %d, latency = %lu us\n", msgLen, (t1.tv_sec - t0.tv_sec)*1000000 + (t1.tv_usec - t0.tv_usec));


    Debug("SendMessage %s, len = %d", m.GetTypeName().c_str(), msgLen);

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

    if (replicaIdx == -1) {
        while(!stop) {
            event_base_loop(eventBase, EVLOOP_ONCE|EVLOOP_NONBLOCK);
            c->rpc->run_event_loop_once();
            //c->rpc->run_event_loop(50);
        }
    } else {
        while(!stop)
            c->rpc->run_event_loop(500);
    }
}

void FastTransport::Stop() {
    Debug("Stopping transport!");
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

bool
FastTransport::CancelTimer(int id)
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

FastTransportAddress *FastTransportAddress::clone() const {
    FastTransportAddress *c = new FastTransportAddress(*this);
    return c;
}

bool operator==(const FastTransportAddress &a, const FastTransportAddress &b) {
    return (memcmp(&a.addr, &b.addr, sizeof(a.addr)) == 0);
}

bool operator!=(const FastTransportAddress &a, const FastTransportAddress &b) {
    return !(a == b);
}

bool operator<(const FastTransportAddress &a, const FastTransportAddress &b) {
    return (memcmp(&a.addr, &b.addr, sizeof(a.addr)) < 0);
}
