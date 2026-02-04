#ifndef __OB_COMMON_OB_THD_NAME_H__
#define __OB_COMMON_OB_THD_NAME_H__

#include <sys/prctl.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/syscall.h>

typedef struct thd_name_ {
    int64_t   threadid;
    pid_t     pid_tid;
    char      threadname[24];
}thd_name_t;

extern   int g_curthreadno;
extern   thd_name_t  thdname[oceanbase::common::OB_MAX_THREAD_NUM];

namespace oceanbase
{
   namespace common
   {

#define CP_THD_NAME(THD_NAME) \
    do{\
    __sync_fetch_and_add(&g_curthreadno, 1);\
    thdname[g_curthreadno].threadid=pthread_self();\
    thdname[g_curthreadno].pid_tid=(pid_t)syscall(__NR_gettid);\
    strncpy(thdname[g_curthreadno].threadname, THD_NAME, 16);\
   }while(0)

#define SET_THD_NAME(THD_NAME) \
    do{\
    char threadname[16];\
    static int64_t thd_no;\
    __sync_fetch_and_add(&thd_no, 1);\
    snprintf(threadname, 16, THD_NAME "%ld", thd_no);\
    prctl(PR_SET_NAME, (unsigned long)threadname);\
    CP_THD_NAME(threadname);\
   }while(0)

#define SET_THD_NAME_ONCE(THD_NAME) \
    do{\
    char threadname[16];\
    strncpy(threadname, THD_NAME, 16);\
    prctl(PR_SET_NAME, (unsigned long)threadname);\
    CP_THD_NAME(threadname);\
    }while(0)
   }
}

#endif
