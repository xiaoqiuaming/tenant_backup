/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ms_list.h
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_MS_LIST_H
#define _OB_MS_LIST_H

#include "common/ob_define.h"
#include "common/ob_server.h"
#include "common/ob_client_manager.h"
#include "common/ob_timer.h"
#include "common/ob_buffer.h"
#include "common/ob_thread_store.h"
#include "common/ob_atomic.h"
#include "common/ob_result.h"
// #include "rootserver/ob_chunk_server_manager.h"

namespace oceanbase
{
  namespace common
  {

    /**
     * MsList��¼������Mergeserver�ĵ�ַ
     * update�������RS����ȡ�µ�MS�б�������б��б仯��������Լ���
     * get_one�������MS�б���ѡ��һ������
     * �̳���ObTimerTask�������ڸ���MS�б�
     */
    class MsList : public ObTimerTask
    {
      public:
        MsList();
        ~MsList();
        //mod zhaoqiong [MultiUPS] [MS_Manage_Function] 20150611:b
        //int init(const ObServer &rs, const ObClientManager *client, bool do_update=true);
//        int init(const ObServer &rs, const ObClientManager *client, int32_t cluster_id = OB_ALL_CLUSTER_FLAG, bool do_update=true);
        //modify for [get local ms]-b
        int init(const ObServer &rs, const ObClientManager *client, int32_t cluster_id = OB_ALL_CLUSTER_FLAG, bool do_update=true, int64_t local_cluster_id = OB_ALL_CLUSTER_FLAG);
        //modify for [get local ms]-e
        //mod:e
        void clear();
        int update();
        const ObServer get_one();
        const ObServer get_cluster_one(); //add for [582-get local cluster ms]
        bool is_local_ms_alive(const ObServer &cs, ObServer &ms); //add for [secondary index get old cchecksum opt]
        //add jinty [Paxos Cluster.Balance]20160708:b
        void get_list( std::vector<ObServer> &list);
        //int get_list_push(std::vector<ObServer> &list);
        //add e
        //add pangtianze [Paxos rs_election] 20150717:b
        inline void set_rs(const ObServer server)
        {
          rs_ = server;
        }
        //add:e
        virtual void runTimerTask();
      public:
        static const int64_t SCHEDULE_PERIOD = 10 * 1000L * 1000L;
      protected:
        bool list_equal_(const std::vector<ObServer> &list);
        void list_copy_(const std::vector<ObServer> &list);
      protected:
        ObServer rs_;
        //add zhaoqiong [MultiUPS] [MS_Manage_Function] 20150611:b
        int32_t cluster_id_;
        //add:e
        std::vector<ObServer> ms_list_;
        //add for [582-get local cluster ms]-b
        std::vector<ObServer>cluster_ms_list_;
        uint64_t cluster_ms_iter_;
        int64_t local_cluster_id_;
        //add for [582-get local cluster ms]-e
        uint64_t ms_iter_;
        uint64_t ms_iter_step_;
        const ObClientManager *client_;
        buffer buff_;
        yysys::CRWSimpleLock rwlock_;
        yyutil::Mutex update_mutex_;
        bool initialized_;
        //add lbzhong [Paxos Cluster.Flow.MS] 201607027:b
        int32_t master_cluster_id_;
        //add:e
    };
  }
}

#endif /* _OB_MS_LIST_H */


