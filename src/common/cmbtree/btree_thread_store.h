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
 *     - A thread-local object store
 */
#ifndef OCEANBASE_COMMON_CMBTREE_BTREE_THREAD_STORE_H__
#define OCEANBASE_COMMON_CMBTREE_BTREE_THREAD_STORE_H__

//#include <pthread.h>
#include <vector>
#include "btree_define.h"
#include "btree_mutex.h"
#include "btree_tid.h"
//mod hongchen [TSD_USER_IMPL] 20170916:b
#include "common/pthread_key_impl.h"
//mod hongchen [TSD_USER_IMPL] 20170916:e

# define UINT32_MAX		(4294967295U)

namespace oceanbase
{
  namespace common
  {
    namespace cmbtree
    {
      class DefaultThreadStoreAlloc
      {
        public:
          inline void * alloc(const int64_t sz)
          { return ::malloc(sz); }
          inline void free(void *p)
          { ::free(p); }
      };

      template <class Type>
      class DefaultInitializer
      {
        public:
          void operator()(void *ptr)
          {
            new (ptr) Type();
          }
      };

      template <class Type, class Initializer = DefaultInitializer<Type>,
                class Alloc = DefaultThreadStoreAlloc>
      class BtreeThreadStore
      {
        public:
          typedef BtreeThreadStore<Type, Initializer, Alloc> TSelf;
        public:
          struct Item
          {
            TSelf * self;
            int     thread_id;
            Type    obj;
          };

          class SyncVector
          {
            public:
              typedef std::vector<Item *>        PtrArray;
            public:
              inline int      push_back          (Item * ptr);

              template<class Function>
              inline int      for_each           (Function & f) const;
              inline void     destroy            ();
            private:
              PtrArray ptr_array_;
              BtreeMutex mutex_;
          };

          template<class Function>
          class ObjPtrAdapter
          {
            public:
              ObjPtrAdapter                      (Function & f)
                : f_(f)
              {
              }
              void operator()                    (const Item * item)
              {
                if (NULL != item)
                {
                  f_(&item->obj);
                }
              }
              void operator()                    (Item * item)
              {
                if (NULL != item)
                {
                  f_(&item->obj);
                }
              }
            protected:
              Function & f_;
          };

        //mod hongchen [TSD_USER_IMPL] 20170916:b
        /*
        public:
          static const pthread_key_t INVALID_THREAD_KEY = UINT32_MAX;
        */
        //mod hongchen [TSD_USER_IMPL] 20170916:e
        public:
          BtreeThreadStore         (Alloc &alloc);

          BtreeThreadStore         (Initializer & initializer, Alloc &alloc);

          BtreeThreadStore         (Initializer & initializer);

          BtreeThreadStore         ();

          virtual ~BtreeThreadStore();

          static void destroy_object(Item * item);

          int32_t  init             ();

          void     destroy          ();

          Type *   get              ();

          template<class Function>
          int      for_each_obj_ptr (Function & f) const;

          template<class Function>
          int      for_each_item_ptr(Function & f) const;

        private:
          //mod hongchen [TSD_USER_IMPL] 20170916:b
          //pthread_key_t key_;
          itemkey key_;
          //mod hongchen [TSD_USER_IMPL] 20170916:e
          DefaultInitializer<Type> default_initializer_;
          DefaultThreadStoreAlloc default_alloc_;
          Initializer & initializer_;
          SyncVector ptr_array_;
          Alloc &alloc_;
          bool init_;
      };

      template <class Type, class Initializer, class Alloc>
      int BtreeThreadStore<Type, Initializer, Alloc>::SyncVector::push_back(Item * ptr)
      {
        int ret = ERROR_CODE_OK;
        int err = ERROR_CODE_OK;
        err = mutex_.lock();
        if (UNLIKELY(ERROR_CODE_OK != err))
        {
          CMBTREE_LOG(ERROR, "mutex lock error, err = %d", err);
          ret = ERROR_CODE_NOT_EXPECTED;
        }
        else
        {
          try
          {
            ptr_array_.push_back(ptr);
          }
          catch (std::bad_alloc)
          {
            CMBTREE_LOG(ERROR, "memory is not enough when push_back");
            ret = ERROR_CODE_NOT_EXPECTED;
          }
          err = mutex_.unlock();
          if (UNLIKELY(ERROR_CODE_OK != err))
          {
            CMBTREE_LOG(ERROR, "exclusive_unlock error, err = %d", err);
            ret = ERROR_CODE_NOT_EXPECTED;
          }
        }
        return ret;
      }

      template <class Type, class Initializer, class Alloc>
      template<class Function>
      int BtreeThreadStore<Type, Initializer, Alloc>::SyncVector::for_each(Function & f) const
      {
        int ret = ERROR_CODE_OK;
        int err = ERROR_CODE_OK;
        err = mutex_.lock();
        if (UNLIKELY(ERROR_CODE_OK != err))
        {
          CMBTREE_LOG(ERROR, "exclusive_lock error, err = %d", err);
          ret = ERROR_CODE_NOT_EXPECTED;
        }
        else
        {
          std::for_each(ptr_array_.begin(), ptr_array_.end(), f);
          err = mutex_.unlock();
          if (UNLIKELY(ERROR_CODE_OK != err))
          {
            CMBTREE_LOG(ERROR, "exclusive_unlock error, err = %d", err);
            ret = ERROR_CODE_NOT_EXPECTED;
          }
        }
        return ret;
      }

      template <class Type, class Initializer, class Alloc>
      void BtreeThreadStore<Type, Initializer, Alloc>::SyncVector::destroy()
      {
        int ret = ERROR_CODE_OK;
        int err = ERROR_CODE_OK;
        err = mutex_.lock();
        if (UNLIKELY(ERROR_CODE_OK != err))
        {
          CMBTREE_LOG(ERROR, "exclusive_lock error, err = %d", err);
          ret = ERROR_CODE_NOT_EXPECTED;
        }
        else
        {
          ptr_array_.clear();
          err = mutex_.unlock();
          if (UNLIKELY(ERROR_CODE_OK != err))
          {
            CMBTREE_LOG(ERROR, "exclusive_unlock error, err = %d", err);
            ret = ERROR_CODE_NOT_EXPECTED;
          }
        }
        (void)ret;//add for build warning
      }

      template <class Type, class Initializer, class Alloc>
      BtreeThreadStore<Type, Initializer, Alloc>::BtreeThreadStore(Alloc &alloc)
        //mod hongchen [TSD_USER_IMPL] 20170916:b
        //: key_(INVALID_THREAD_KEY), initializer_(default_initializer_),
        : initializer_(default_initializer_),
        //mod hongchen [TSD_USER_IMPL] 20170916:e
          alloc_(alloc), init_(false)
      {
      }

      template <class Type, class Initializer, class Alloc>
      BtreeThreadStore<Type, Initializer, Alloc>::BtreeThreadStore(
          Initializer & initializer, Alloc &alloc)
        //mod hongchen [TSD_USER_IMPL] 20170916:b
        //: key_(INVALID_THREAD_KEY), initializer_(initializer),
        : initializer_(initializer),
        //mod hongchen [TSD_USER_IMPL] 20170916:e
          alloc_(alloc), init_(false)
      {
      }

      template <class Type, class Initializer, class Alloc>
      BtreeThreadStore<Type, Initializer, Alloc>::BtreeThreadStore(
          Initializer & initializer)
        //mod hongchen [TSD_USER_IMPL] 20170916:b
        //: key_(INVALID_THREAD_KEY), initializer_(initializer),
        : initializer_(initializer),
        //mod hongchen [TSD_USER_IMPL] 20170916:e
          alloc_(default_alloc_), init_(false)
      {
      }

      template <class Type, class Initializer, class Alloc>
      BtreeThreadStore<Type, Initializer, Alloc>::BtreeThreadStore()
        //mod hongchen [TSD_USER_IMPL] 20170916:b
        //: key_(INVALID_THREAD_KEY), initializer_(default_initializer_),
        : initializer_(default_initializer_),
        //mod hongchen [TSD_USER_IMPL] 20170916:e
          alloc_(default_alloc_), init_(false)
      {
      }

      template <class Type, class Initializer, class Alloc>
      BtreeThreadStore<Type, Initializer, Alloc>::~BtreeThreadStore()
      {
        destroy();
      }

      template <class Type, class Initializer, class Alloc>
      void BtreeThreadStore<Type, Initializer, Alloc>::destroy_object(Item * item)
      {
        if (NULL != item)
        {
          item->obj.~Type();
          item->self->alloc_.free(item);
        }
      }

      template <class Type, class Initializer, class Alloc>
      int32_t BtreeThreadStore<Type, Initializer, Alloc>::init()
      {
        int32_t ret = ERROR_CODE_OK;
        if (init_)
        {
          CMBTREE_LOG(ERROR, "BtreeThreadStore has already initialized.");
          ret = ERROR_CODE_NOT_EXPECTED;
        }
        else
        {
          //mod hongchen [TSD_USER_IMPL] 20170916:b
          //if (INVALID_THREAD_KEY == key_
          if (!key_.is_valid())
          //mod hongchen [TSD_USER_IMPL] 20170916:e
          {
            int retry_times = 3;
            while (--retry_times >= 0 )
            {
              //mod hongchen [TSD_USER_IMPL] 20170916:b
              //int err = pthread_key_create(&key_, NULL);
              int err = PTHREAD_KEY_MGR->create_thread_key(key_);
              //mod hongchen [TSD_USER_IMPL] 20170916:e
              if (0 != err)
              {
                CMBTREE_LOG(ERROR, "pthread_key_create error: %s,err=%d",
                    strerror(errno),err);
                if (errno == ENOMEM)
                {
                  ret = ERROR_CODE_ALLOC_FAIL;
                }
                else if (errno == EAGAIN)
                {
                  usleep(10 * 1000); //10ms
                  continue;
                }
                else
                {
                  ret = ERROR_CODE_NOT_EXPECTED;
                }
              }
              else
              {
                init_ = true;
              }
              break;
            }
            if (!init_ && ERROR_CODE_OK != ret)
            {
              YYSYS_LOG(ERROR,"fail to init BtreeThreadStore. UPS will kill itself.");
              kill(getpid(), SIGTERM);
            }
          }
          else
          {
            CMBTREE_LOG(ERROR, "key_ should be INVALID_THREAD_KEY");
            ret = ERROR_CODE_NOT_EXPECTED;
          }
        }
        return ret;
      }

      template <class Type, class Initializer, class Alloc>
      void BtreeThreadStore<Type, Initializer, Alloc>::destroy()
      {
        if (init_)
        {
          //mod hongchen [TSD_USER_IMPL] 20170916:b
          //if (INVALID_THREAD_KEY != key_)
          //{
          //  //void* mem = pthread_getspecific(key_);
          //  //if (NULL != mem) destroy_object(mem);
          //  pthread_key_delete(key_);
          //  key_ = INVALID_THREAD_KEY;
          //
          if (key_.is_valid())
          {
            PTHREAD_KEY_MGR->delete_thread_key(key_);
            key_.reset();
          }
          //mod hongchen [TSD_USER_IMPL] 20170916:e
          for_each_item_ptr(destroy_object);
          ptr_array_.destroy();
          init_ = false;
        }
      }

      template <class Type, class Initializer, class Alloc>
      Type * BtreeThreadStore<Type, Initializer, Alloc>::get()
      {
        Type * ret = NULL;
        if (UNLIKELY(!init_))
        {
          CMBTREE_LOG(ERROR, "BtreeThreadStore has not been initialized");
        }
        //mod hongchen [TSD_USER_IMPL] 20170916:b
        //else if (INVALID_THREAD_KEY == key_)
        else if (!key_.is_valid())
        {
          CMBTREE_LOG(ERROR, "BtreeThreadStore thread key is invalid");
        }
        else
        {
          //Item * item = reinterpret_cast<Item *>(pthread_getspecific(key_));
          Item * item = reinterpret_cast<Item *>(PTHREAD_KEY_MGR->get_thread_specifi(key_));
          if (NULL == item)
          {
            item = reinterpret_cast<Item *>(alloc_.alloc(sizeof(Item)));
            if (NULL != item)
            {
              //if (0 != pthread_setspecific(key_, item))
              if (0 != PTHREAD_KEY_MGR->set_thread_specifi(key_, item))
              {
                alloc_.free(item);
                item = NULL;
              }
              else
              {
                initializer_(&item->obj);
                item->self = this;
                item->thread_id = BtreeTID::gettid();
                ptr_array_.push_back(item);
              }
            }
          }
          if (NULL != item)
          {
            ret = &item->obj;
          }
        }
        //mod hongchen [TSD_USER_IMPL] 20170916:e
        return ret;
      }

      template <class Type, class Initializer, class Alloc>
      template<class Function>
      int BtreeThreadStore<Type, Initializer, Alloc>::for_each_obj_ptr(Function & f) const
      {
        if (UNLIKELY(!init_))
        {
          CMBTREE_LOG(ERROR, "BtreeThreadStore has not been initialized");
          return ERROR_CODE_NOT_EXPECTED;
        }
        else
        {
          ObjPtrAdapter<Function> opa(f);
          return ptr_array_.for_each(opa);
        }
      }

      template <class Type, class Initializer, class Alloc>
      template<class Function>
      int BtreeThreadStore<Type, Initializer, Alloc>::for_each_item_ptr(Function & f) const
      {
        if (UNLIKELY(!init_))
        {
          CMBTREE_LOG(ERROR, "BtreeThreadStore has not been initialized");
          return ERROR_CODE_NOT_EXPECTED;
        }
        else
        {
          return ptr_array_.for_each(f);
        }
      }

    } // end namespace cmbtree
  } // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_CMBTREE_BTREE_THREAD_STORE_H__
