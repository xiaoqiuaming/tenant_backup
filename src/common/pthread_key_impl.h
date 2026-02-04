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
#ifndef PTHREAD_KEY_IMPL_H
#define PTHREAD_KEY_IMPL_H

#include "ob_define.h"

namespace oceanbase
{
  namespace common
  {
    namespace types
    {
      struct uint128_t
      {
          uint64_t lo;
          uint64_t hi;
      }
      __attribute__ (( __aligned__( 16 ) ));
    }
    inline bool cas128( volatile types::uint128_t * src, types::uint128_t cmp, types::uint128_t with )
    {
      bool result;
//add support for arm platform by wangd 202106:b
#ifdef __aarch64__
      uint64_t tmp = 0;
      types::uint128_t psrc;
      memset(&psrc, 0, sizeof (types::uint128_t));
      __asm__ __volatile__(
          "1: ldxp %2,%3, [%1]    \n\t"
          "eor %4, %5, %2         \n\t"
          "cbnz %4, 2f            \n\t"
          "eor %4, %6, %3         \n\t"
          "cbnz %4, 2f            \n\t"
          "stxp %w4, %7, %8, [%1] \n\t"
          "cbnz %4, 1b            \n\t"
          "mov %0, #1             \n\t"
          "b 3f                   \n\t"
          "2:                     \n\t"
          "mov %5, %2             \n\t"
          "mov %6, %3             \n\t"
          "mov %0, #0             \n\t"
          "3:"
          : "+r"( result ), "+r"(src), "+&r"(psrc.lo), "+&r"(psrc.hi), "=&r"(tmp), "+r"(cmp.lo), "+r"(cmp.hi)
          : "r"(with.lo), "r"(with.hi)
          : "cc");
#else
//add support for arm platform by wangd 202106:e
      __asm__ __volatile__
          (
            "\n\tlock cmpxchg16b %1"
            "\n\tsetz %0\n"
            : "=q" ( result ), "+m" ( *src ), "+d" ( cmp.hi ), "+a" ( cmp.lo )
            : "c" ( with.hi ), "b" ( with.lo )
            : "cc"
            );
#endif //add support for arm platform by wangd 202106
      return result;
    }
    inline void load128 (__uint128_t& dest, types::uint128_t *src)
    {
//add support for arm platform by wangd 202106:b
#ifdef __aarch64__
      __asm__ __volatile__  (
                            "1:                           \n\t"
                            "ldxp x15, x16, %1            \n\t"
//                           "ldp x15, x16, %1            \n\t"
                            "stp x15, x16, %0             \n\t"
                            "stxp w17, x15, x16, %1       \n\t"
                            "cbnz w17, 1b                 \n\t"
                            "dmb ish                      \n\t"
                            : "+Q"(dest)
                            : "m"(*src)
                            : "x15", "x16", "w17", "cc", "memory");
#else
//add support for arm platform by wangd 202106:e
      __asm__ __volatile__ ("\n\txor %%rax, %%rax;"
                            "\n\txor %%rbx, %%rbx;"
                            "\n\txor %%rcx, %%rcx;"
                            "\n\txor %%rdx, %%rdx;"
                            "\n\tlock cmpxchg16b %1;\n"
                            : "=&A"(dest)
                            : "m"(*src)
                            : "%rbx", "%rcx", "cc");
#endif //add support for arm platform by wangd 202106
    }
#define CAS128(src, cmp, with) cas128((types::uint128_t*)(src), *((types::uint128_t*)&(cmp)), *((types::uint128_t*)&(with)))
#define LOAD128(dest, src) load128((__uint128_t&)(dest), (types::uint128_t*)(src))
    struct itemkey
    {
        itemkey()
        {
          reset();
        }
        void reset()
        {
          index_ = -1;
          seq_    = 0;
        }
        bool is_valid() const
        {
          return (-1 != index_);
        }
        int64_t  index_;
        int64_t  seq_;
    };
    struct item1level
    {
        int64_t  used_;
        int64_t  seq_;
    }__attribute__ (( __aligned__( 16 ) ));
    struct item2level
    {
        int64_t  seq_;
        int64_t  ptr_;  //convenient, ptr<->int64_t
    }__attribute__ (( __aligned__( 16 ) ));
    static const int64_t MAX_KEY_ITEMS_NUM = 4096;
    class pthread_key_impl
    {
      public:
        pthread_key_impl();
        int   create_thread_key(itemkey& key);
        void* delete_thread_key(const itemkey key);
        void* get_thread_specifi(const itemkey key);
        int   set_thread_specifi(const itemkey key, const void*ptr);
        static pthread_key_impl* get_instance();
      private:
        static pthread_key_impl*   instance_;
        static volatile bool       mutex_;
        static volatile int64_t    seq_;
        static volatile int64_t    cur_usable_pos_;
        static          int64_t    free_items_arr_[MAX_KEY_ITEMS_NUM];
        static volatile item1level process_key_items_[MAX_KEY_ITEMS_NUM] CACHE_ALIGNED;
        static __thread item2level thread_key_items_[MAX_KEY_ITEMS_NUM]  CACHE_ALIGNED;
    };
#define PTHREAD_KEY_MGR pthread_key_impl::get_instance()
  }
}
#endif // PTHREAD_KEY_IMPL_H
