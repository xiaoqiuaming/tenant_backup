#ifndef __OB_COMMON_TEST_BASE_H__
#define __OB_COMMON_TEST_BASE_H__

#include "gtest/gtest.h"
#include "rwt.h"
#include "ob_allocator.h"
#include "ob_malloc.h"

#define _cfg(k, v) getenv(k)?:v
#define _cfgi(k, v) atoll(getenv(k)?:v)
struct BaseConfig
{
  int64_t duration;
  char* schema;
  int64_t table_id;
  BaseConfig()
  {
    duration = _cfgi("duration", "5000000");
    schema = "./test.schema";
    table_id = 1002;
  }
};

using namespace oceanbase::common;
struct BufHolder
{
  BufHolder(int64_t limit) { buf_ = (char*)ob_malloc(limit); }
  ~BufHolder() { ob_free((void*)buf_); }
  char* buf_;
};

class FixedAllocator: public ObIAllocator
{
  public:
    FixedAllocator(char* buf, int64_t limit): buf_(buf), limit_(limit), pos_(0) {}
    virtual ~FixedAllocator() {}
  public:
    void reset() { pos_ = 0; }
    virtual void *alloc(const int64_t sz){
      void* ptr = NULL;
      int64_t pos = 0;
//add support for arm platform by wangd 202106:b
#if defined(__aarch64__)
      int64_t sz_align = (sz + 7) & ~(7);
#else
      int64_t sz_align = sz;
#endif
//add support for arm platform by wangd 202106:e
      if ((pos = __sync_add_and_fetch(&pos_, sz_align)) > limit_)//mod support for arm platform by wangd 202106
      {
        __sync_add_and_fetch(&pos_, -sz_align);//mod support for arm platform by wangd 202106
      }
      else
      {
        ptr = buf_ + pos;
      }
      return ptr;
    }
    virtual void free(void *ptr){ UNUSED(ptr); }
  private:
    char* buf_;
    int64_t limit_;
    int64_t pos_;
};

#define RWT_def(base) \
  TEST_F(base, Rand){ \
    ASSERT_EQ(0, PARDO(get_thread_num(), this, duration));      \
    ASSERT_EQ(0, check_error());                                \
  }
#endif /* __OB_COMMON_TEST_BASE_H__ */
