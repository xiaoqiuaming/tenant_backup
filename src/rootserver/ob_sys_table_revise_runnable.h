/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 * 
 * Version: $Id$
 *
 * ob_sys_table_revise_runnable.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_SYS_TABLE_REVISE_RUNNABLE_H
#define _OB_SYS_TABLE_REVISE_RUNNABLE_H 1

#include "yysys.h"
#include "rootserver/ob_ups_manager.h"
#include "rootserver/ob_chunk_server_manager.h"
#include "rootserver/ob_root_server2.h"
#include "rootserver/ob_root_worker.h"

namespace oceanbase
{
  namespace rootserver
  {
    class ObSysTableReviseRunnable: public yysys::CDefaultRunnable
    {
      public:
        ObSysTableReviseRunnable(ObUpsManager& ups_manager, ObChunkServerManager &server_manager,
                               ObRootServer2 *root_server, ObRootWorker *root_work_, const int64_t &sys_table_revise_interval);
        virtual ~ObSysTableReviseRunnable();
        virtual void run(yysys::CThread* thread, void* arg);              
      private:
        // disallow copy
        ObSysTableReviseRunnable(const ObSysTableReviseRunnable &other);
        ObSysTableReviseRunnable& operator=(const ObSysTableReviseRunnable &other);
      private:
        int modify_all_server_table(const ObRootAsyncTaskQueue::ObSeqTask & task);
        int modify_all_cluster_table(const ObRootAsyncTaskQueue::ObSeqTask & task);
        int check_cluster_alive(const int32_t cluster_id);
        int process_offline_cluster(int32_t cluster_id);
        int process_cluster_while_all_mscs_offline(int32_t cluster_id);
      private:
        //static const int64_t CHECK_INTERVAL_US = 60 * 1000 * 1000LL; // 60s
        const static int64_t RETRY_TIMES = 1;
        const static int64_t TIMEOUT = 1000000; // 1s
        const static int64_t DELETE_SERVER_TIMES_AGO = 900 * 1000 * 1000;// 900s
        // data members
        ObUpsManager &ups_manager_;
        ObChunkServerManager &server_manager_;
        ObRootServer2 *root_server_;
        ObRootWorker *root_work_;
        const int64_t& check_time_interval_;
    };
  } // end namespace rootserver
} // end namespace oceanbase
#endif /* _OB_SYS_TABLE_REVISE_RUNNABLE_H */

