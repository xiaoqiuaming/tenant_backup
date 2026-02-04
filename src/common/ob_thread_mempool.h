////===================================================================
//
// ob_thread_mempool.h / common / Oceanbase
//
// Copyright (C) 2010 Taobao.com, Inc.
//
// Created on 2011-01-13 by Yubai (yubai.lk@taobao.com)
//
// -------------------------------------------------------------------
//
// Description
//
//
// -------------------------------------------------------------------
//
// Change Log
//
////====================================================================

#ifndef  OCEANBASE_COMMON_THREAD_MEMPOOL_H_
#define  OCEANBASE_COMMON_THREAD_MEMPOOL_H_
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
//#include <pthread.h>
#include <new>
#include <algorithm>
#include "ob_define.h"
#include "ob_malloc.h"
//mod hongchen [TSD_USER_IMPL] 20170916:b
#include "pthread_key_impl.h"
//mod hongchen [TSD_USER_IMPL] 20170916:e

namespace oceanbase
{
  namespace common
  {
    struct DefaultAllocator
    {
        void *malloc(const int32_t nbyte)
        { return ob_malloc(nbyte, ObModIds::OB_THREAD_MEM_POOL); };
        void free(void *ptr)
        { ob_free(ptr); };
    };
    class ObMemList
    {
        static const int64_t WARN_ALLOC_NUM = 1024;
        typedef DefaultAllocator MemAllocator;
        struct MemBlock
        {
            MemBlock *next;
        };
      public:
        explicit ObMemList(const int32_t fixed_size);
        ~ObMemList();
      public:
        void *get();
        void put(void *ptr, const int32_t max_free_num);
        int64_t inc_ref_cnt();
        int64_t dec_ref_cnt();
        int64_t get_ref_cnt();
      private:
        MemAllocator alloc_;
        MemBlock *header_;
        int32_t size_;
        const int32_t fixed_size_;
        int64_t ref_cnt_;
//add support for arm platform by wangd 202106:b
#if defined(__aarch64__)
        pthread_spinlock_t spin_  __attribute__((__aligned__(8)));
#else
//add support for arm platform by wangd 202106:e
        pthread_spinlock_t spin_;
#endif //add support for arm platform by wangd 202106
    };
    class ObThreadMempool
    {
        //static const pthread_key_t INVALID_THREAD_KEY = INT32_MAX;  //del hongchen [TSD_USER_IMPL] 20170916
      public:
        static const int32_t DEFAULT_MAX_FREE_NUM = 0;
      public:
        ObThreadMempool();
        ~ObThreadMempool();
      public:
        int init(const int32_t fixed_size, const int32_t max_free_num);
        int destroy();
        void *alloc();
        void free(void *ptr);
        void set_max_free_num(const int32_t max_free_num);
      private:
        static void destroy_thread_data_(void *ptr);
      private:
        //mod hongchen [TSD_USER_IMPL] 20170916:b
        //pthread_key_t key_;
        itemkey key_;
        //mod hongchen [TSD_USER_IMPL] 20170916:e
        int32_t fixed_size_;
        int32_t max_free_num_;
    };

    extern void thread_mempool_init();
    extern void thread_mempool_destroy();
    extern void thread_mempool_set_max_free_num(const int32_t max_free_num);
    extern void *thread_mempool_alloc();
    extern void thread_mempool_free(void *ptr);
  }
}

#endif //OCEANBASE_COMMON_THREAD_MEMPOOL_H_

