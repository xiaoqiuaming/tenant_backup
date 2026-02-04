////===================================================================
//
// pthread_key_impl.h common / Oceanbase
//
// Created on 2017-09-17 by hongchen (peiouya2013@mail.nwpu.edu.cn)
//
// -------------------------------------------------------------------
//
// Description:
//   This class pthread_key_impl is made to replace posix TSD.
//   please use it carefully!!!!!
//   pthread_key_impl has the following two limitations:
//      first.  it only manage key and memory-ptr, without holding memory
//      second. it has no callback func. So, user must process by hand.
// -------------------------------------------------------------------
//
// Change Log
//
////====================================================================
#include "pthread_key_impl.h"
#include "yysys.h"
#include "utility.h"

namespace oceanbase
{
  namespace common
  {
    pthread_key_impl* pthread_key_impl::instance_ = NULL;
    volatile item1level pthread_key_impl::process_key_items_[MAX_KEY_ITEMS_NUM] = {{0, 0}};
    __thread item2level pthread_key_impl::thread_key_items_[MAX_KEY_ITEMS_NUM] = {{0, 0}};
    volatile bool pthread_key_impl::mutex_  = false;
    volatile int64_t pthread_key_impl::seq_ = 0;
    volatile int64_t pthread_key_impl::cur_usable_pos_ = 0;
    int64_t  pthread_key_impl::free_items_arr_[MAX_KEY_ITEMS_NUM] = {-1};


    //singleton, global unique instance
    pthread_key_impl* pthread_key_impl::get_instance()
    {
      if (NULL == instance_)
      {
        while (!__sync_bool_compare_and_swap(&mutex_, false, true))
        {
//add support for arm platform by wangd 202106:b
#if defined(__aarch64__)
          asm("yield");
#else
//add support for arm platform by wangd 202106:e
          asm("pause");
#endif //add support for arm platform by wangd 202106
        }
        if (NULL == instance_)
        {
          instance_ = new (std::nothrow) pthread_key_impl();
        }
        mutex_ = false;
      }
      return dynamic_cast<pthread_key_impl*>(instance_);
    }

    pthread_key_impl::pthread_key_impl()
    {
      for (int64_t index = 0; index < MAX_KEY_ITEMS_NUM; ++ index)
      {
        free_items_arr_[index] = index;
      }
    }

    /*expr:The create_thread_key() function shall create a thread-specific
           data key visible to all threads in the process. Key values provided
           by create_thread_key() are used to locate thread-specific data.
           if successful, the create_thread_key shall store the newly created
           key value at key and shall return zero. Otherwise, an error number
           shall be returned to indicate the error.
           ERRORS:
                 OB_EAGAIN:the pre-defined limit on the total number of keys
                           per process has been exceeded.
                 OB_ERROR :the pre-defined non-loop seq is exhausted.
    */
    int   pthread_key_impl::create_thread_key(itemkey& key)
    {
      int ret = OB_SUCCESS;
      if (MAX_KEY_ITEMS_NUM <= cur_usable_pos_)
      {
        ret = OB_EAGAIN;
      }
      else
      {
        while (!__sync_bool_compare_and_swap(&mutex_, false, true))
        {
//add support for arm platform by wangd 202106:b
#if defined(__aarch64__)
          asm("yield");
#else
//add support for arm platform by wangd 202106:e
          asm("pause");
#endif //add support for arm platform by wangd 202106
        }
        seq_ += 1;
        if (0 > seq_)
        {
          ret = OB_ERROR;
        }
        else if (MAX_KEY_ITEMS_NUM <= cur_usable_pos_)
        {
          ret = OB_EAGAIN;
        }
        else
        {
          int64_t free_item_index = free_items_arr_[cur_usable_pos_++];
          process_key_items_[free_item_index].seq_ = seq_;
          __sync_synchronize();
          process_key_items_[free_item_index].used_ = 1;
          key.index_ = free_item_index;
          key.seq_   = process_key_items_[free_item_index].seq_;
          //modify for [450-mod log in phtread key impl]-b
          //        YYSYS_LOG(INFO, "CREATE PTHREAD KEY seq_ %ld", seq_);
          YYSYS_LOG(INFO, "CREATE PTHREAD KEY seq_ %ld, cur_usable_pos_ %ld", seq_, cur_usable_pos_);
          //modify for [450-mod log in phtread key impl]-e
        }
        mutex_ = false;
      }
      return ret;
    }
    /*expr:The delete_thread_key() function shall delete a thread-specific data
           key previously returned by create_thread_key().
           Whether succeed or not, The delete_thread_key() shall return mem-ptr.
           Return NULL if failed.
    */
    void* pthread_key_impl::delete_thread_key(const itemkey key)
    {
      void * ret = NULL;
      if (MAX_KEY_ITEMS_NUM <= key.index_ || 0 > key.index_ || 0 >= key.seq_)
      {
        YYSYS_LOG(WARN," input parameter key[%ld] not in [0, %ld), and key.seq_[%ld] must > 0", key.index_, MAX_KEY_ITEMS_NUM, key.seq_);
      }
      else
      {
        while (!__sync_bool_compare_and_swap(&mutex_, false, true))
        {
//add support for arm platform by wangd 202106:b
#if defined(__aarch64__)
          asm("yield");
#else
//add support for arm platform by wangd 202106:e
          asm("pause");
#endif //add support for arm platform by wangd 202106
        }
        if (0 == process_key_items_[key.index_].used_
            || process_key_items_[key.index_].seq_ != key.seq_
            || (0 != thread_key_items_[key.index_].seq_ && thread_key_items_[key.index_].seq_ != key.seq_))
        {
          //nothing todo
        }
        else
        {
          process_key_items_[key.index_].seq_    = 0;
          thread_key_items_[key.index_].seq_     = 0;
          __sync_synchronize();
          process_key_items_[key.index_].used_   = 0;
          ret = reinterpret_cast<void*>(thread_key_items_[key.index_].ptr_);
          thread_key_items_[key.index_].ptr_ = 0;
          free_items_arr_[--cur_usable_pos_] = key.index_;
          //        YYSYS_LOG(INFO, "DELETE PTHREAD KEY seq_ %ld", key.seq_);
          YYSYS_LOG(INFO, "DELETE PTHREAD KEY seq_ %ld, cur_usable_pos_ %ld", key.seq_, cur_usable_pos_);
        }
        mutex_ = false;
      }
      return ret;
    }
    /*expr:The get_thread_specifi() function shall associate a thread-specific
           value with a key obtained via a  previous call to create_thread_key().
           Different threads may bind different values to the same key.
           Whether succeed or not, The get_thread_specifi() shall return mem-ptr.
           Return NULL if failed.
    */
    void* pthread_key_impl::get_thread_specifi(const itemkey key)
    {
      void * ret = NULL;
      if (MAX_KEY_ITEMS_NUM <= key.index_ || 0 > key.index_ || 0 >= key.seq_)
      {
        YYSYS_LOG(WARN," input parameter key[%ld] not in [0, %ld), and key.seq_[%ld] must > 0", key.index_, MAX_KEY_ITEMS_NUM, key.seq_);
      }
      else
      {
        item2level thread_item;
        LOAD128(thread_item, &(thread_key_items_[key.index_]));
        __sync_synchronize();
        item1level process_item;
        LOAD128(process_item, &(process_key_items_[key.index_]));
        if (0 != process_item.used_
            && process_item.seq_ == thread_item.seq_
            && process_item.seq_ == key.seq_)
        {
          ret = reinterpret_cast<void*>(thread_item.ptr_);
        }
      }
      return ret;
    }
    /*expr:The set_thread_specifi() function shall associate a thread-specific
           value with a key obtained via a previous call to create_thread_key().
           Different threads  may  bind different values to the same key.
           If successful, the set_thread_specifi() function shall return zero;
           otherwise, an error number shall be returned to indicate the error.
           ERRORS:
                 OB_INVALID_ARGUMENT:key is invalid, means key's slot is invalid.
                 OB_ERROR           :all errors except for OB_INVALID_ARGUMENT.
    */
    int   pthread_key_impl::set_thread_specifi(const itemkey key, const void* ptr)
    {
      int ret = OB_SUCCESS;
      if (MAX_KEY_ITEMS_NUM <= key.index_ || 0 > key.index_ || 0 >= key.seq_)
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(WARN," input parameter key[%ld] not in [0, %ld), and key.seq_[%ld] must > 0, ret=%d", key.index_, MAX_KEY_ITEMS_NUM, key.seq_, ret);
      }
      else
      {
        item2level tmp_thread_item;
        tmp_thread_item.seq_ = key.seq_;
        tmp_thread_item.ptr_ = reinterpret_cast<int64_t>(ptr);
        item2level thread_item;
        LOAD128(thread_item, &(thread_key_items_[key.index_]));
        __sync_synchronize();
        item1level process_item;
        LOAD128(process_item, &(process_key_items_[key.index_]));
        if (0 != process_item.used_
            && process_item.seq_ == key.seq_)
        {
          if (!CAS128(&(thread_key_items_[key.index_]), thread_item, tmp_thread_item))
          {
            ret = OB_ERROR;
            YYSYS_LOG(WARN, "cur slot[%ld]'s content changed!, ret=%d",
                      key.index_, ret);
          }
        }
        else
        {
          ret = OB_ERROR;
          YYSYS_LOG(WARN, "cur slot[%ld] use flag is %ld, its seq_ is %ld, key.seq_ is %ld, ret=%d",
                    key.index_, process_item.used_, process_item.seq_, key.seq_, ret);
        }
      }
      return ret;
    }
  }
}
