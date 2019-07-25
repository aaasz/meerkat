// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * common/transaction.h:
 *   A Transaction representation.
 *
 **********************************************************************/

#ifndef _TRANSACTION_H_
#define _TRANSACTION_H_

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/timestamp.h"

#include <unordered_map>

// Reply types
#define REPLY_OK 0
#define REPLY_FAIL 1
#define REPLY_RETRY 2
#define REPLY_ABSTAIN 3
#define REPLY_TIMEOUT 4
#define REPLY_NETWORK_FAILURE 5
#define REPLY_MAX 6

// <clientd, clienttxn_nr>
typedef std::pair<uint64_t, uint64_t> txnid_t;

// Statuses maintained in the TransactionSet,
// by the replication layer
enum TransactionStatus {
    NOT_PREPARED,
    PREPARED_WRITES,
    PREPARED_RETRY,
    PREPARED_OK,
    PREPARED_ABORT,
    COMMITTED,
    ABORTED
};

typedef std::unordered_map<std::string, Timestamp> ReadSetMap;
typedef std::unordered_map<std::string, std::string> WriteSetMap;

class Transaction {
private:
    // map between key and timestamp at
    // which the read happened and how
    // many times this key has been read
    ReadSetMap readSet;

    // map between key and value(s)
    //std::unordered_map<std::string, std::string> writeSet;
    WriteSetMap writeSet;

public:
    Transaction();
    Transaction(uint8_t nr_reads, uint8_t nr_writes, char* buf);
    ~Transaction();

    //const std::unordered_map<std::string, Timestamp>& getReadSet() const;
    const ReadSetMap& getReadSet() const;
    //const std::unordered_map<std::string, std::string>& getWriteSet() const;
    const WriteSetMap& getWriteSet() const;

    void addReadSet(const std::string &key, const Timestamp &readTime);
    void addWriteSet(const std::string &key, const std::string &value);
    void serialize(char *reqBuf) const;
    void clear();
};

#endif /* _TRANSACTION_H_ */
