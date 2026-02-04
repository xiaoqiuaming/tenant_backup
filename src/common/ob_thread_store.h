/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *     - some work details if you want
 *
 */

#ifndef _THREAD_STORE_H__
#define _THREAD_STORE_H__

//mod hongchen [TSD_USER_IMPL] 20170916:b
//#include <pthread.h>
#include "pthread_key_impl.h"
//mod hongchen [TSD_USER_IMPL] 20170916:e

namespace oceanbase
{
  namespace common
  {
    template <int ModId = ObModIds::OB_THREAD_STORE>
    class DfltThreadStoreAlloc
    {
      public:
        void *malloc(const int64_t sz)
        { return ob_malloc(sz, ModId); }
        void free(void *p)
        { ob_free(p, ModId); }
    };

    template <class Type>
    class DfltInitType
    {
      public:
        void operator()(void *ptr)
        {
          new (ptr) Type();
        }
    };

    template <class Type, class InitType = DfltInitType<Type>,
              class Alloc = DfltThreadStoreAlloc<> >
    class thread_store
    {
        //mod hongchen [TSD_USER_IMPL] 20170916:b
        //public:
        //  static const pthread_key_t INVALID_THREAD_KEY = UINT32_MAX;
      public:
        thread_store() //: key_(INVALID_THREAD_KEY)
        {
          create_store();
        }
        //mod hongchen [TSD_USER_IMPL] 20170916:e

        virtual ~thread_store()
        {
          delete_store();
        }

        static void destroy_object(void *mem)
        {
          if (NULL != mem)
          {
            thread_store<Type, InitType, Alloc> **h =
                reinterpret_cast<thread_store<Type, InitType, Alloc> **>(mem) - 1;
            (reinterpret_cast<Type *>(mem))->~Type();
            (*h)->alloc_.free(h);
          }
        }

        void create_store()
        {
          //mod hongchen [TSD_USER_IMPL] 20170916:b
          /*
          if (INVALID_THREAD_KEY == key_)
          {
            pthread_key_create(&key_, destroy_object);
          }
          */
          if (!key_.is_valid())
          {
            PTHREAD_KEY_MGR->create_thread_key(key_);
          }
          //mod hongchen [TSD_USER_IMPL] 20170916:e
        }

        void delete_store()
        {
          //mod hongchen [TSD_USER_IMPL] 20170916:b
          /*
          if (INVALID_THREAD_KEY != key_)
          {
            void* mem = pthread_getspecific(key_);
            if (NULL != mem) destroy_object(mem);
            pthread_key_delete(key_);
            key_ = INVALID_THREAD_KEY;
          }
          */
          if (key_.is_valid())
          {
            void* mem = PTHREAD_KEY_MGR->get_thread_specifi(key_);
            if (NULL != mem) destroy_object(mem);
            PTHREAD_KEY_MGR->delete_thread_key(key_);
            key_.reset();
          }
          //mod hongchen [TSD_USER_IMPL] 20170916:e
        }

        Type* get()
        {
          //mod hongchen [TSD_USER_IMPL] 20170916:b
          //if (INVALID_THREAD_KEY == key_) return NULL;
          if (!key_.is_valid()) return NULL;
          else
          {
            void* ptr = NULL;
            //void* mem = pthread_getspecific(key_);
            void* mem = PTHREAD_KEY_MGR->get_thread_specifi(key_);
            if (NULL == mem)
            {
              mem = alloc_.malloc(sizeof(thread_store*) + sizeof(Type));
              if (NULL != mem)
              {
                ptr = reinterpret_cast<void*>(
                        (reinterpret_cast<
                         thread_store<Type, InitType, Alloc> **
                         >(mem)) + 1
                        );
                //if (0 != pthread_setspecific(key_, ptr))
                if (0 != PTHREAD_KEY_MGR->set_thread_specifi(key_, ptr))
                {
                  alloc_.free(mem);
                  mem = NULL;
                  ptr = NULL;
                }
                else
                {
                  InitType init_type_func;
                  init_type_func(ptr);
                  *reinterpret_cast<thread_store<Type, InitType, Alloc> **>(mem)
                      = this;
                }
              }
            }
            else
            {
              ptr = mem;
            }
            //mod hongchen [TSD_USER_IMPL] 20170916:e
            return reinterpret_cast<Type*>(ptr);
          }
        }

      private:
        //mod hongchen [TSD_USER_IMPL] 20170916:b
        //pthread_key_t key_;
        itemkey key_;
        //mod hongchen [TSD_USER_IMPL] 20170916:e
        Alloc alloc_;
    };
  }
}

#endif // _THREAD_STORE_H__
