// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * transport.h:
 *   message-passing network interface definition
 *
 * Copyright 2013 Dan R. K. Ports  <drkp@cs.washington.edu>
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

#ifndef _LIB_TRANSPORT_H_
#define _LIB_TRANSPORT_H_

#include "lib/message.h"
#include "lib/configuration.h"
#include <functional>

#define CLIENT_NETWORK_DELAY 0
#define REPLICA_NETWORK_DELAY 0
#define READ_AT_LEADER 1

class TransportReceiver
{
public:
    virtual ~TransportReceiver();
    virtual void ReceiveRequest(uint8_t reqType, char *reqBuf, char *respBuf) {
        PPanic("Not implemented; did you forget to set the MULTIPLE_ACTIVE_REQUESTS flag accordingly?");
    };
    virtual void ReceiveRequest(uint64_t reqHandleIdx, uint8_t reqType, char *reqBuf, char *respBuf) {
        PPanic("Not implemented; did you forget to set the MULTIPLE_ACTIVE_REQUESTS flag accordingly?");
    };
    virtual void ReceiveResponse(uint8_t reqType, char *respBuf) = 0;
    virtual bool Blocked() = 0;
};

typedef std::function<void (void)> timer_callback_t;

class Transport
{
public:
    virtual ~Transport() {}
    virtual void Register(TransportReceiver *receiver,
                          int replicaIdx) = 0;
    virtual bool SendResponse(size_t msgLen) = 0;
    virtual bool SendResponse(uint64_t bufIdx, size_t msgLen) = 0;
    virtual bool SendRequestToReplica(TransportReceiver *src, uint8_t reqType, uint8_t replicaIdx, uint8_t coreIdx, size_t msgLen) = 0;
    virtual bool SendRequestToAll(TransportReceiver *src, uint8_t reqType, uint8_t coreIdx, size_t msgLen) = 0;
    virtual int Timer(uint64_t ms, timer_callback_t cb) = 0;
    virtual bool CancelTimer(int id) = 0;
    virtual void CancelAllTimers() = 0;

    virtual char *GetRequestBuf(size_t reqLen, size_t respLen) = 0;
    virtual int GetSession(TransportReceiver *src, uint8_t replicaIdx, uint8_t dstRpcIdx) = 0;

    virtual uint8_t GetID() = 0;
};

class Timeout
{
public:
    Timeout(Transport *transport, uint64_t ms, timer_callback_t cb);
    virtual ~Timeout();
    virtual void SetTimeout(uint64_t ms);
    virtual uint64_t Start();
    virtual uint64_t Reset();
    virtual void Stop();
    virtual bool Active() const;
    
private:
    Transport *transport;
    uint64_t ms;
    timer_callback_t cb;
    int timerId;
};

#endif  // _LIB_TRANSPORT_H_
