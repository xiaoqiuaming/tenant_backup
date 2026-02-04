/*
 * Copyright (C) 2012-2013 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Description here
 *
 * Version: $Id$
 *
 * Authors:
 *   Fusheng Han <yanran.hfs@taobao.com>
 *     - Thread ID encapsulation
 */
#ifndef OCEANBASE_COMMON_CMBTREE_BTREE_TID_H_
#define OCEANBASE_COMMON_CMBTREE_BTREE_TID_H_

#include <sys/syscall.h>
#include <unistd.h>

namespace oceanbase
{
  namespace common
  {
    namespace cmbtree
    {
      class BtreeTID
      {
        public:
          //mod qiuhm[fix arm bug]20210822:b
          /*
          static inline int gettid()
          {
            static __thread int tid = -1;
            if (UNLIKELY(tid == -1))
            {
              tid = static_cast<int>(syscall(__NR_gettid));
            }
            return tid;
          }
          */
          static inline int32_t gettid()
          {
            static __thread int32_t tid = -1;
            if (UNLIKELY(tid == -1))
            {
              tid = static_cast<int32_t>(syscall(__NR_gettid));
            }
            return tid;
          }
          //mod:e
      };

    } // end namespace cmbtree
  } // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_CMBTREE_BTREE_TID_H_
