// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * viewstamp.h:
 *   definition of types and utility functions for viewstamps and
 *   related types
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

#ifndef _LEADERMEERKATIR_VIEWSTAMP_H_
#define _LEADERMEERKATIR_VIEWSTAMP_H_

#include "store/common/transaction.h"

namespace replication {
namespace leadermeerkatir {

typedef uint64_t view_t;

struct viewstamp_t {
    view_t view;
    txnid_t txnid;

    viewstamp_t() : view(0), txnid(0, 0) {}
    viewstamp_t(view_t view, txnid_t txnid) : view(view), txnid(txnid) {}
};

static inline int Viewstamp_Compare(viewstamp_t a, viewstamp_t b) {
    if (a.view < b.view) return -1;
    if (a.view > b.view) return 1;
    if (a.txnid.second < b.txnid.second) return -1;
    if (a.txnid.second > b.txnid.second) return 1;
    if (a.txnid.first < b.txnid.first) return -1;
    if (a.txnid.first > b.txnid.first) return 1;
    return 0;
}

inline bool operator==(const viewstamp_t& lhs, const viewstamp_t& rhs){ return Viewstamp_Compare(lhs,rhs) == 0; }
inline bool operator!=(const viewstamp_t& lhs, const viewstamp_t& rhs){return !operator==(lhs,rhs);}
inline bool operator< (const viewstamp_t& lhs, const viewstamp_t& rhs){ return Viewstamp_Compare(lhs,rhs) < 0; }
inline bool operator> (const viewstamp_t& lhs, const viewstamp_t& rhs){return  operator< (rhs,lhs);}
inline bool operator<=(const viewstamp_t& lhs, const viewstamp_t& rhs){return !operator> (lhs,rhs);}
inline bool operator>=(const viewstamp_t& lhs, const viewstamp_t& rhs){return !operator< (lhs,rhs);}

} // namespace leadermeerkatir
} // namespace replication

#endif  /* _LEADERMEERKATIR_VIEWSTAMP_H_ */
