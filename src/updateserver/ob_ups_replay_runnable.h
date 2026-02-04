/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *     - some work details if you want
 */

#ifndef OCEANBASE_UPDATESERVER_OB_UPS_REPLAY_RUNNABLE_H_
#define OCEANBASE_UPDATESERVER_OB_UPS_REPLAY_RUNNABLE_H_

#include "common/ob_obi_role.h"
#include "common/ob_log_replay_runnable.h"
#include "common/ob_log_entry.h"
#include "common/ob_log_reader.h"
#include "common/ob_repeated_log_reader.h"
#include "ob_log_sync_delay_stat.h"
#include "ob_ups_role_mgr.h"
#include "common/ob_obi_role.h"
#include "common/ob_switch.h"
//add wangdonghui [ups replication] 20170526:b
#include "common/ob_fifo_allocator.h"
#include "ob_trans_executor.h"
#include "common/ob_single_pop_queue.h"
//add :e
namespace oceanbase
{
  namespace tests
  {
    namespace updateserver
    {
      //forward decleration
      class TestObUpsReplayRunnable_test_init_Test;
    }
  }
  namespace updateserver
  {
    class ObUpsLogMgr;
    class ObUpsReplayRunnable : public yysys::CDefaultRunnable 
    {
      const static int64_t DEFAULT_REPLAY_WAIT_TIME_US = 50 * 1000;
      const static int64_t DEFAULT_FETCH_LOG_WAIT_TIME_US = 500 * 1000;
      friend class tests::updateserver::TestObUpsReplayRunnable_test_init_Test;
      public:
        ObUpsReplayRunnable();
        virtual ~ObUpsReplayRunnable();
        //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
        //virtual int init(ObUpsLogMgr* log_mgr, common::ObiRole *obi_role, ObUpsRoleMgr *role_mgr);
        virtual int init(ObUpsLogMgr* log_mgr, ObUpsRoleMgr *role_mgr);
        //mod 20150701:e
        virtual void run(yysys::CThread* thread, void* arg);
        // 不停线程等待开始和结束
        bool wait_stop();
        bool wait_start();
        // 等待线程结束
        void stop();
        virtual void clear();
        void set_replay_wait_time_us(const int64_t wait_time)
        { replay_wait_time_us_ = wait_time; }
        void set_fetch_log_wait_time_us(const int64_t wait_time)
        { fetch_log_wait_time_us_ = wait_time; }
        ObSwitch& get_switch()
        { return switch_; }
      private:
        ObSwitch switch_;
        ObUpsLogMgr* log_mgr_;
        //del peiouya [MultiUPS] [DELETE_OBI] 20150701:b
        //common::ObiRole* obi_role_;
        //del 20150701:e
        ObUpsRoleMgr* role_mgr_;
        bool is_initialized_;
        int64_t replay_wait_time_us_;
        int64_t fetch_log_wait_time_us_;
    };
    //add wangdonghui [Ups_replication] 20161009 :b
    class ObUpdateServer;
    class ObUpsWaitFlushRunnable : public yysys::CDefaultRunnable
    {
      static const int64_t ALLOCATOR_TOTAL_LIMIT = 10L * 1024L * 1024L * 1024L;
      static const int64_t ALLOCATOR_HOLD_LIMIT = ALLOCATOR_TOTAL_LIMIT / 2;
      static const int64_t ALLOCATOR_PAGE_SIZE = 4L * 1024L * 1024L;
      public:
        ObUpsWaitFlushRunnable();
        virtual ~ObUpsWaitFlushRunnable();
        virtual int init(ObUpsLogMgr* log_mgr, ObUpsRoleMgr *role_mgr,
                         ObUpdateServer* updateserver, ObLogReplayWorker* replay_worker_);
        virtual void run(yysys::CThread* thread, void* arg);
        // 不停线程等待开始和结束
        bool wait_stop();
        bool wait_start();
        // 等待线程结束
        void stop();
        virtual void clear();
        int process_head_task();
        int push(TransExecutor::Task* packet);
      private:
        ObSwitch switch_;
        common::ObSinglePopQueue<TransExecutor::Task *> task_queue_;
        yysys::CThreadCond _cond;
        bool is_initialized_;
        ObUpsLogMgr* log_mgr_;
        common::ObiRole* obi_role_;
        ObUpsRoleMgr* role_mgr_;
        ObUpdateServer* updateserver_;
        ObLogReplayWorker* replay_worker_;
        common::FIFOAllocator allocator_;
      private:
        static const int MY_VERSION = 1;
    };
    //add 20161009 :e

    //add lxb [slave_ups_optimizer] 20180109:b
    //��ObUpsReplayRunnable������ObUpsReplayLogRunnable����־д��طŶ��У�ObUpsReplayRunnable����־д�����
    class ObUpsReplayLogRunnable : public yysys::CDefaultRunnable
    {
      static const int64_t DEFAULT_REPLAY_TO_QUEUE_WAIT_TIME_US = 20*1000;
      static const int64_t DEFAULT_REPLAY_TO_DISK_WAIT_TIME_US = 20*1000;

      public:
        ObUpsReplayLogRunnable();
        virtual ~ObUpsReplayLogRunnable();
        virtual int init(ObUpsLogMgr *log_mgr);
        virtual void run(yysys::CThread *thread, void *arg);

        //��ͣ�̵߳ȴ���ʼ�ͽ���
        bool wait_stop();
        bool wait_start();

        //�ȴ��߳̽���
        void stop();

        virtual void clear();

        void set_replay_to_queue_wait_time_us(const int64_t wait_time)
        { replay_to_queue_wait_time_us_ = wait_time;}

        void set_replay_to_disk_wait_time_us(const int64_t wait_time)
        { replay_to_disk_wait_time_us_ = wait_time;}

      private:
        ObSwitch switch_;
        yysys::CThreadCond _cond;
        bool  is_initialized_;
        ObUpsLogMgr *log_mgr_;
      private:
        static const int MY_VERSION = 1;
        int64_t replay_to_queue_wait_time_us_;
        int64_t replay_to_disk_wait_time_us_;

    };

    //add:e


    //add hxlog [asyn_load_log] �첽�����߳�
    class ObUpsLoadLogRunnable : public yysys::CDefaultRunnable
    {
      static const int64_t DEFAULT_LOAD_LOG_TO_BUFF_WAIT_TIME_US = 20*1000;

      public:
        ObUpsLoadLogRunnable();
        virtual ~ObUpsLoadLogRunnable();
        virtual int init(ObUpsLogMgr *log_mgr);
        virtual void run(yysys::CThread *thread, void *arg);

        bool wait_stop();
        bool wait_start();

        void stop();
        virtual void clear();

        void set_load_log_to_buff_wait_time_us(const int64_t wait_time)
        {
            load_log_to_buff_wait_time_us_ = wait_time;
        }


      private:
        ObSwitch switch_;
        bool  is_initialized_;
        ObUpsLogMgr *log_mgr_;
        static const int MY_VERSION = 1;
        int64_t load_log_to_buff_wait_time_us_;
        bool is_append_;
    };

    //add:e

  } // end namespace updateserver
} // end namespace oceanbase


#endif // OCEANBASE_UPDATESERVER_OB_UPS_REPLAY_RUNNABLE_H_

