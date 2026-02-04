/*
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 *
 *
 * Version: 0.1: ob_update_server.cpp,v 0.1 2010/09/28 13:52:43 chuanhui Exp $
 *
 * Authors:
 *   chuanhui <rizhao.ych@taobao.com>
 *     - some work details if you want
 *
 */
#include <math.h>
#include "common/ob_trace_log.h"
#include "common/serialization.h"
#include "common/utility.h"
#include "common/ob_log_dir_scanner.h"
#include "common/ob_tsi_factory.h"
#include "common/ob_rs_ups_message.h"
#include "common/ob_token.h"
#include "common/ob_version.h"
#include "common/ob_log_cursor.h"
#include "sstable/ob_aio_buffer_mgr.h"
#include "ob_update_server.h"
#include "ob_ups_utils.h"
#include "ob_update_server_main.h"
#include "ob_ups_clog_status.h"
#include <pthread.h>
//#include "ob_client_wrapper.h"
#include "common/ob_tbnet_callback.h"
#include "ob_update_callback.h"
#define __ups_debug__
#include "common/debug.h"
#include "common/ob_trace_id.h"
#include "common/ob_profile_log.h"
#include "common/ob_profile_type.h"
#include "common/gperf.h"
#include "common/ob_pcap.h"

//add lbzhong [Clog Monitor] 20151218:b
#include "clog_status.h"
//add:e


using namespace oceanbase::common;

#define RPC_CALL_WITH_RETRY(function, retry_times, timeout, server, args...) \
  ({ \
    int err = OB_RESPONSE_TIME_OUT; \
    for (int64_t i = 0; ObUpsRoleMgr::STOP != role_mgr_.get_state() \
        && ObUpsRoleMgr::FATAL != role_mgr_.get_state() \
        && OB_SUCCESS != err && i < retry_times; ++i) \
    { \
      int64_t timeu = yysys::CTimeUtil::getMonotonicTime(); \
      err = ups_rpc_stub_.function(server, args, timeout);               \
      YYSYS_LOG(INFO, "%s, server=%s, retry_times=%ld, err=%d", #function, server.to_cstring(), i, err); \
      timeu = yysys::CTimeUtil::getMonotonicTime() - timeu; \
      if (OB_SUCCESS != err \
          && timeu < timeout) \
      { \
        YYSYS_LOG(INFO, "timeu=%ld not match timeout=%ld, will sleep %ld", timeu, timeout, timeout - timeu); \
        int sleep_ret = precise_sleep(timeout - timeu); \
        if (OB_SUCCESS != sleep_ret) \
        { \
          YYSYS_LOG(ERROR, "precise_sleep ret=%d", sleep_ret); \
        } \
      } \
    } \
    err; \
  })

namespace oceanbase
{
  namespace updateserver
  {
    static const int32_t ADDR_BUF_LEN = 64;

    ObUpdateServer::ObUpdateServer(ObConfigManager &config_mgr,
                                   ObUpdateServerConfig& config, common::ObShadowServer& shadow_server)
      : config_(config),
        config_mgr_(config_mgr),
        rpc_buffer_(RPC_BUFFER_SIZE),
        read_task_queue_size_(DEFAULT_TASK_READ_QUEUE_SIZE),
        write_task_queue_size_(DEFAULT_TASK_WRITE_QUEUE_SIZE),
        lease_task_queue_size_(DEFAULT_TASK_LEASE_QUEUE_SIZE),
        log_task_queue_size_(DEFAULT_TASK_LOG_QUEUE_SIZE),
        store_thread_queue_size_(DEFAULT_STORE_THREAD_QUEUE_SIZE),
        preprocess_task_queue_size_(DEFAULT_TASK_PREPROCESS_QUEUE_SIZE),
        //add peiouya [MultiUPS] [UPS_SYNC_Frozen] 20150521:b  20150615
        last_frozen_version_(0),
        to_be_frozen_version_(0),  //add peiouya [MultiUPS] [UPS_Manage_Function] 20150615
        //add 20150521:e
        log_mgr_(),
        table_mgr_(log_mgr_),
        //add chujiajia [Paxos rs_election] 20151229:b
        slave_mgr_(config_.quorum_scale),
        //add:e
        sstable_query_(sstable_mgr_),
        grant_keep_alive_guard_(DEFAULT_RESET_ASYNC_TASK_COUNT_TIMEOUT),
        check_keep_alive_guard_(DEFAULT_RESET_ASYNC_TASK_COUNT_TIMEOUT),
        check_lease_guard_(DEFAULT_RESET_ASYNC_TASK_COUNT_TIMEOUT),
        //add peiouya [MultiUPS] [DELETE_OBI] 20150701:b
        get_sys_ups_master_guard_(DEFAULT_RESET_ASYNC_TASK_COUNT_TIMEOUT),
        //add 20150701:e
        //add peiouya [MultiUPS] [UPS_SYNC_Frozen] 20150810:b
        //interval DEFAULT_RESET_ASYNC_TASK_COUNT_TIMEOUT
        major_freeze_accord_hb_guard_(DEFAULT_RESET_ASYNC_TASK_COUNT_TIMEOUT),
        //add 20150810:e
        schema_version_(0),
        schema_lock_(0),
      trans_executor_(*this),
        shadow_server_(shadow_server) ,ms_(),scan_helper_(NULL),schema_service_(NULL) //add zhaoqiong [Schema Manager] 20150327
    //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160226:b
    ,offline_version_(OB_DEFAULT_OFFLINE_VERSION)
    //add 20160226:e
    {
    }

    ObUpdateServer::~ObUpdateServer()
    {
    }

    int ObUpdateServer::initialize()
    {
      int err = OB_SUCCESS;
      __debug_init__();
      // do not handle batch packet.
      // process packet one by one.
      set_batch_process(false);
      is_log_mgr_start_ = false;
      lease_expire_time_us_ = 0;
      //last_keep_alive_time_ = 0;
      can_receive_log_ = true;
      is_set_term_ = false;//add wangdonghui [ups replication] 20170717 :b:e
      minor_freeze_done_ = 0;
      ups_renew_reserved_us_ = 0;
      if (OB_SUCCESS == err)
      {
        read_task_queue_size_ = static_cast<int32_t>(config_.read_queue_size);
        write_task_queue_size_ = static_cast<int32_t>(config_.write_queue_size);
        // TODO (rizhao)
        lease_task_queue_size_ = static_cast<int32_t>(config_.lease_queue_size);
        log_task_queue_size_ = static_cast<int32_t>(config_.log_queue_size);
        store_thread_queue_size_ = static_cast<int32_t>(config_.store_thread_count);
      }

      if (OB_SUCCESS == err)
      {
        lease_timeout_in_advance_ = config_.lease_timeout_in_advance;
        keep_alive_valid_interval_ = config_.keep_alive_timeout;
        YYSYS_LOG(INFO, "load param: keep_alive_valid_interval=%s",
                  config_.keep_alive_timeout.str());
      }

      if (OB_SUCCESS == err)
      {
        memset(&server_handler_, 0, sizeof(easy_io_handler_pt));
        server_handler_.encode = ObTbnetCallback::encode;
        server_handler_.decode = ObTbnetCallback::decode;
        server_handler_.process = ObUpdateCallback::process;
        //server_handler_.process
        server_handler_.get_packet_id = ObTbnetCallback::get_packet_id;
        server_handler_.on_disconnect = ObTbnetCallback::on_disconnect;
        server_handler_.on_connect = ObTbnetCallback::on_connect;
        server_handler_.cleanup = ObTbnetCallback::clean_up;
        server_handler_.user_data = this;
      }

      if (OB_SUCCESS == err)
      {
        err = client_manager_.initialize(eio_, &server_handler_);
        if (OB_SUCCESS != err)
        {
          YYSYS_LOG(WARN, "failed to init client manager, err=%d", err);
        }
        else if (OB_SUCCESS != (err = client_manager_.set_dedicate_thread_num(1)))
        {
          YYSYS_LOG(ERROR, "client_manager.set_dedicate_thread_num(1)=>%d", err);
        }
      }

      if (OB_SUCCESS == err)
      {
        root_server_.set_ipv4_addr(config_.root_server_ip,
                                   (int32_t)config_.root_server_port);
        //add pangtianze [Paxos rs_election] 20150710:b
        rs_mgr_.set_master_rs(root_server_);
        //add:e
        YYSYS_LOG(INFO, "load param: root_server addr=%s", root_server_.to_cstring());
      }

      if (OB_SUCCESS == err)
      {
        //mod zhaoqiong [fixed for Backup]:20150811:b
        //err = ups_rpc_stub_.init(&client_manager_);
        err = ups_rpc_stub_.init(&client_manager_, &rpc_buffer_);
        //mod:e
        if (OB_SUCCESS != err)
        {
          YYSYS_LOG(WARN, "fail to init rpc stub, err=%d", err);
        }
      }

      //add shili [LONG_TRANSACTION_LOG]  20160926:b
      if (OB_SUCCESS == err)
      {
        err = big_log_writer_.init();
        if (OB_SUCCESS != err)
        {
          YYSYS_LOG(WARN, "fail to init big log writer, err=%d", err);
        }
      };
      //add e

      if (OB_SUCCESS == err)
      {
        err = set_listen_port((int)config_.port);
      }

      if (OB_SUCCESS == err)
      {
        err = set_self_(config_.devname, (int32_t)config_.port);
        if (OB_SUCCESS != err)
        {
          YYSYS_LOG(WARN, "fail to set self.");
        }
      }
      if (OB_SUCCESS == err)
      {
        //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//        if (strlen(config_.lsync_ip) > 0
//            && 0 != strcmp("0.0.0.0", config_.lsync_ip))
//        {
//          lsync_server_.set_ipv4_addr(config_.lsync_ip, (int32_t)config_.lsync_port);
//          obi_slave_stat_ = STANDALONE_SLAVE;

//          YYSYS_LOG(INFO, "Slave stat set to STANDALONE_SLAVE, lsync_server=%s", to_cstring(lsync_server_));
//        }
//        else
//        {
//          //obi_slave_stat_ = UNKNOWN_SLAVE;
//          obi_slave_stat_ = FOLLOWED_SLAVE; // ȥHA֮��ups����������instance ups�ĵ�ַ
//        }
        obi_slave_stat_ = FOLLOWED_SLAVE;
        //mod 20150701:E
      }

      // init ups cache
      if (OB_SUCCESS == err)
      {
        err = ups_cache_.init();
        if (OB_SUCCESS != err)
        {
          YYSYS_LOG(WARN, "failed to init ups cache, err=%d", err);
        }
      }

      // init mgr
      if (OB_SUCCESS == err)
      {
        //mod peiouya [MultiUPS] [UPS_Manage_Function] 20150521:b
        //err = table_mgr_.init();
        err = table_mgr_.init(static_cast<uint64_t>(config_.start_major_version));
        //mod 20150521:e
        if (OB_SUCCESS != err)
        {
          YYSYS_LOG(WARN, "failed to init table mgr, err=%d", err);
        }
        else
        {
          table_mgr_.set_replay_checksum_flag(config_.replay_checksum_flag);
          ob_set_memory_size_limit(config_.total_memory_limit);
          MemTableAttr memtable_attr;
          if (OB_SUCCESS == table_mgr_.get_memtable_attr(memtable_attr))
          {
            memtable_attr.total_memlimit = config_.table_memory_limit;
            table_mgr_.set_memtable_attr(memtable_attr);
          }
          else
          {
            YYSYS_LOG(WARN, "fail in table_mgr");
          }
          table_mgr_.set_paxos_id(config_.paxos_id);//[588]
        }
      }

      if (OB_SUCCESS == err)
      {
        err = sstable_query_.init(config_.blockcache_size,
                                  config_.blockindex_cache_size);
        if (OB_SUCCESS != err)
        {
          YYSYS_LOG(WARN, "failed to init sstable query, err=%d", err);
        }
        else
        {
          YYSYS_LOG(INFO, "init sstable query success");
        }
      }

      if (OB_SUCCESS == err)
      {
        err = slave_mgr_.init(&trans_executor_, &role_mgr_, &ups_rpc_stub_,
                              config_.log_sync_timeout);
        if (OB_SUCCESS != err)
        {
          YYSYS_LOG(WARN, "failed to init slave mgr, err=%d", err);
        }
        else
        {
          YYSYS_LOG(INFO, "slave_mgr init");
        }
      }
      if (OB_SUCCESS == err)
      {
        int64_t timeout_delta = 50 * 1000;
        int64_t n_blocks = config_.log_cache_n_block;
        int64_t block_size_shift =  __builtin_ffsl(config_.log_cache_block_size) - 1;
        int64_t log_file_max_size = config_.commit_log_size;
        int64_t n_replay_worker = config_.replay_worker_num;
        int64_t replay_log_buf_len = config_.replay_log_buf_size;
        int64_t replay_queue_len = config_.replay_queue_len;

        bool replay_queue_rebalance = config_.replay_queue_rebalance;

        if (OB_SUCCESS != (err = ups_log_server_getter_.init(this)))
        {
          YYSYS_LOG(ERROR, "master_getter.init()=>%d", err);
        }
        else if (OB_SUCCESS != (err = prefetch_log_task_submitter_.init(config_.lsync_fetch_timeout + timeout_delta, this)))
        {
          YYSYS_LOG(ERROR, "prefetch_log_task_submitter_.init(this)=>%d", err);
        }
        else if (OB_SUCCESS != (err = trigger_handler_.init(root_server_, &ups_rpc_stub_, &role_mgr_)))
        {
          YYSYS_LOG(WARN, "failed to init trigger handler, err=%d", err);
        }
        else if (OB_SUCCESS != (err = log_applier_.init(&trans_executor_, &trans_executor_.get_session_mgr(),
                                                        &trans_executor_.get_lock_mgr(), &table_mgr_, &log_mgr_)))
        {
          YYSYS_LOG(ERROR, "log_applier.init(n_replay_worker=%ld)=>%d", n_replay_worker, err);
        }
        else if (OB_SUCCESS != (err = replay_worker_.init(&log_applier_,
                                                          (int32_t)n_replay_worker,
                                                          replay_log_buf_len,
                                                          replay_queue_len,
                                                          replay_queue_rebalance,
                                                          config_.replay_thread_start_cpu,
                                                          config_.replay_thread_end_cpu)))
        {
          YYSYS_LOG(ERROR, "replay_worker.init(n_replay_worker=%ld)=>%d", n_replay_worker, err);
        }
        else if (OB_SUCCESS != (err = replay_log_src_.init(&log_mgr_.get_log_buffer(), &prefetch_log_task_submitter_,
                &ups_log_server_getter_, &ups_rpc_stub_,
                config_.lsync_fetch_timeout,
                n_blocks, block_size_shift)))
        {
          YYSYS_LOG(ERROR, "replay_log_src.init()=>%d", err);
        }  
		//mod shili [LONG_TRANSACTION_LOG]  20160926:b
        //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//        else if (OB_SUCCESS != (err = log_mgr_.init(config_.commit_log_dir,
//                                                    log_file_max_size,
//                                                    &replay_worker_,
//                                                    &replay_log_src_,
//                                                    &table_mgr_,
//                                                    &slave_mgr_,
//                                                    &obi_role_,
//                                                    &role_mgr_,
//                                                    config_.log_sync_type)))
       else if (OB_SUCCESS != (err = log_mgr_.init(config_.commit_log_dir,
                                                    log_file_max_size,
                                                    &replay_worker_,
                                                    &replay_log_src_,
                                                    &table_mgr_,
                                                    &slave_mgr_,
                                                    &role_mgr_,
                                                    &big_log_writer_,
                                                    config_.log_sync_type)))
        {
          YYSYS_LOG(WARN, "failed to init log mgr, path=%s, log_file_size=%ld, err=%d",
                    config_.commit_log_dir.str(), log_file_max_size, err);
        }
      }
      if (OB_SUCCESS == err)
      {
        set_log_sync_delay_stat_param();
      }

      if (OB_SUCCESS == err)
      {
        int64_t read_thread_count = config_.read_thread_count;
        int64_t store_thread_count = config_.store_thread_count;
        IpPort ip_port;
        ip_port.ip_ = static_cast<uint16_t>(local_ip_ & 0x0000FFFF);
        ip_port.port_ = static_cast<uint16_t>(port_);
        read_thread_queue_.set_ip_port(ip_port);
        read_thread_queue_.setThreadParameter(static_cast<int32_t>(read_thread_count), this, NULL);
        //add lbzhong [Clog Monitor] 20151218:b
        clog_thread_queue_.setThreadParameter(1, this, NULL);
        //add:e
        write_thread_queue_.setThreadParameter(1, this, NULL);
        lease_thread_queue_.setThreadParameter(1, this, NULL);
        store_thread_.setThreadParameter(static_cast<int32_t>(store_thread_count), this, NULL);
      }

      if (OB_SUCCESS == err)
      {
        err = set_io_thread_count((int32_t)config_.io_thread_count);
        if (OB_SUCCESS != err)
        {
          YYSYS_LOG(ERROR, "set io thread count faild");
        }
        else
        {
          YYSYS_LOG(INFO, "set io thread count %s", config_.io_thread_count.str());
        }
      }

      if (OB_SUCCESS == err)
      {
        log_mgr_.set_load_log_config(config_.replay_log_cache_block_num, config_.used_block_num);
        //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
        //err = log_replay_thread_.init(&log_mgr_, &obi_role_, &role_mgr_);
        err = log_replay_thread_.init(&log_mgr_, &role_mgr_);
        //mod 20150701:e
        if (OB_SUCCESS != err)
        {
          YYSYS_LOG(WARN, "failed to start log replay thread, err=%d", err);
        }
        else
        {
          set_log_replay_thread_param();
        }
      }

      //add wangdonghui [Ups_replication] 20161009 :b
      if (OB_SUCCESS == err)
      {
        err = wait_flush_thread_.init(&log_mgr_, &role_mgr_, this, &replay_worker_);
        if (OB_SUCCESS != err)
        {
          YYSYS_LOG(WARN, "failed to start wait flush thread, err=%d", err);
        }
        else
        {
          YYSYS_LOG(INFO, "wait_flush_thread init succ: %p", &wait_flush_thread_);
        }
      }
      //add :e


      //add lxb [slave ups optimizer:b
      if (OB_SUCCESS == err)
      {
          err = replay_log_to_queue_thread_.init(&log_mgr_);
          if(OB_SUCCESS != err)
          {
              YYSYS_LOG(WARN,"failed to replay_log_to_queue_thread_,err=%d",err);
          }
          else
          {
              err = async_load_log_thread_.init(&log_mgr_);
              if(OB_SUCCESS != err)
              {
                  YYSYS_LOG(WARN,"failed to async_load_log_thread_,err=%d",err);
              }
              else
              {
                  set_replay_log_to_queue_param();
                  YYSYS_LOG(INFO,"replay_log_to_queue_thread_ init succ: %p", &replay_log_to_queue_thread_);
              }
          }
      }
      //add:e
      if (OB_SUCCESS == err)
      {
        err = trans_executor_.init(config_.trans_thread_num,
                                  config_.trans_thread_start_cpu,
                                  config_.trans_thread_end_cpu,
                                  config_.commit_bind_core_id,
                                  config_.commit_end_thread_num,
                                   config_.distributed_trans_thread_num
                                   );
        if (OB_SUCCESS != err)
        {
          YYSYS_LOG(WARN, "trans executor init fail, err=%d", err);
        }
      }

      if (OB_SUCCESS == err)
      {
        //mod zhaoqiong [MultiUPS] [MS_Manage_Function] 20150611:b
//        if (OB_SUCCESS != (err = ms_list_task_.init(
//                             config_.get_root_server(),
//                             &client_manager_,
//                             false)))
        if (OB_SUCCESS != (err = ms_list_task_.init(
                             config_.get_root_server(),
                             &client_manager_,OB_MAX_CLUSTER_COUNT,
                             false, config_.cluster_id))) //mod [582]
        //mod:e
        {
          YYSYS_LOG(ERROR, "init ms list failt, ret: [%d]", err);
        }
      }

      if (OB_SUCCESS == err)
      {
        stat_mgr_.init(get_self());
        ObStatSingleton::init(&stat_mgr_);
      }

      if (OB_SUCCESS == err)
      {
        const char *store_root = config_.store_root;
        const char *raid_regex = config_.raid_regex;
        const char *dir_regex = config_.dir_regex;
        err = sstable_mgr_.init(store_root, raid_regex, dir_regex);
        if (OB_SUCCESS != err)
        {
          YYSYS_LOG(WARN, "failed to init sstable mgr, err=%d", err);
        }
        else
        {
          table_mgr_.reg_table_mgr(sstable_mgr_);
          if (!sstable_mgr_.load_new())
          {
            YYSYS_LOG(WARN, "sstable mgr load new fail");
            err = OB_ERROR;
          }
          else if (OB_SUCCESS != (err = table_mgr_.check_sstable_id()))
          {
            YYSYS_LOG(WARN, "check sstable id fail err=%d", err);
          }
          else
          {
            table_mgr_.log_table_info();
          }

          TableItem *table_item = table_mgr_.get_table_mgr()->get_active_memtable();
          if(NULL == table_item)
          {
              err = OB_ERROR;
          }
          else
          {
              table_item->set_last_clog_id(sstable_mgr_.get_max_clog_id());
              table_mgr_.get_table_mgr()->revert_active_memtable(table_item);
          }
        }
      }
	  //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
      if (OB_SUCCESS == err)
      {
        err = rpc_stub_.init(&rpc_buffer_, &client_manager_);
      }
      //add 20150701:e
     
		
      //add zhaoqiong [Schema Manager] 20150327:b
      if (OB_SUCCESS == err)
      {
        if (NULL == (scan_helper_ = new(std::nothrow)ObScanHelperImpl))
        {
          err = OB_ALLOCATE_MEMORY_FAILED;
        }
        else if (NULL == (schema_service_ = new(std::nothrow)ObSchemaServiceImpl))
        {
          err = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          ms_.init(&ms_list_task_);
          scan_helper_->set_rpc_stub(&ups_rpc_stub_);
          scan_helper_->set_scan_timeout(config_.fetch_schema_timeout);
          scan_helper_->set_mutate_timeout(config_.fetch_schema_timeout);
          scan_helper_->set_scan_retry_times(config_.fetch_schema_times);
          scan_helper_->set_ms_provider(&ms_);
          if (OB_SUCCESS != (err = schema_service_->init(scan_helper_, false)))
          {
            YYSYS_LOG(ERROR,"init schema_service_ error");
          }
        }

        if (OB_SUCCESS != err)
        {
          if (scan_helper_)
          {
            delete scan_helper_;
            scan_helper_ = NULL;
          }

          if (schema_service_)
          {
            delete schema_service_;
            schema_service_ = NULL;
          }
        }
      }
      fetch_schema_timestamp_ = 0;
      //add:e

      return err;
    }

    int ObUpdateServer::set_io_thread_count(int io_thread_count)
    {
      int ret = OB_SUCCESS;
      if (io_thread_count < 1)
      {
        YYSYS_LOG(WARN, "invalid argument io thread count is %d", io_thread_count);
        ret = OB_ERROR;
      }
      else
      {
        io_thread_count_ = io_thread_count;
      }
      return ret;
    }

    void ObUpdateServer::wait_for_queue()
    {
      /*
         read_thread_queue_.wait();
         write_thread_queue_.wait();
       */
    }

    void ObUpdateServer::destroy()
    {
      role_mgr_.set_state(ObUpsRoleMgr::STOP);
      log_mgr_.signal_stop();
      replay_worker_.stop();
      cleanup();
      ObBaseServer::destroy();
    }

    void ObUpdateServer::cleanup()
    {
      YYSYS_LOG(WARN, "req stop threads");
      /// д�߳�
      write_thread_queue_.stop();

      /// ���߳�
      read_thread_queue_.stop();

      //add lbzhong [Clog Monitor] 20151218:b
      clog_thread_queue_.stop();
      //add:e

      //add wangdonghui [Ups_replication] 20161009 :b
      ///�ȴ���־ˢ���߳�
      wait_flush_thread_.stop();
      //add :e

      replay_log_to_queue_thread_.stop();
      async_load_log_thread_.stop();


      /// Lease�߳�
      lease_thread_queue_.stop();

      /// Check�߳�
      //check_thread_.stop();

      /// ת���߳�
      store_thread_.stop();

      ///��־�ط��߳�
      log_replay_thread_.stop();

      replay_worker_.wait();

      /// д�߳�
      write_thread_queue_.wait();

      /// ���߳�
      read_thread_queue_.wait();

      //add lbzhong [Clog Monitor] 20151218:b
      clog_thread_queue_.wait();
      //add:e

      /// Lease�߳�
      lease_thread_queue_.wait();

      /// ת���߳�
      store_thread_.wait();

      ///��־�ط��߳�
      log_replay_thread_.wait();

      //add wangdonghui [Ups_replication] 20161009 :b
      ///��־ˢ���߳�
      wait_flush_thread_.wait();
      //add :e

      async_load_log_thread_.wait();
      replay_log_to_queue_thread_.wait();


      timer_.destroy();
      config_timer_.destroy();
      //TODO stop network
      log_mgr_.wait();
      trans_executor_.destroy();
      YYSYS_LOG(WARN, "stop threads succ.");

      //add zhaoqiong [Schema Manager] 20150327:b
      delete scan_helper_;
      scan_helper_ = NULL;
      delete schema_service_;
      schema_service_ = NULL;
      //add:e
    }

    int ObUpdateServer::start_timer_schedule()
    {
      int err = OB_SUCCESS;
      err = set_timer_check_keep_alive();
      if (OB_SUCCESS != err)
      {
        YYSYS_LOG(WARN, "fail to set timer to check_keep_alive. err=%d", err);
      }
      if (OB_SUCCESS == err)
      {
        if (OB_SUCCESS != (err = set_timer_check_lease()))
        {
          YYSYS_LOG(WARN, "fail to set timer to check lease. err=%d", err);
        }
      }
      if (OB_SUCCESS == err)
      {
        if (OB_SUCCESS != (err = set_timer_grant_keep_alive()))
        {
          YYSYS_LOG(WARN, "fail to set timer to grant keep_alive, err=%d", err);
        }
      }
      if (OB_SUCCESS == err)
      {
        if (OB_SUCCESS != (err = set_timer_handle_fronzen()))
        {
          YYSYS_LOG(WARN, "fail to set timer to handle frozen, err=%d", err);
        }
      }
      if (OB_SUCCESS == err)
      {
        if (OB_SUCCESS != (err = set_timer_switch_skey()))
        {
          YYSYS_LOG(WARN, "fail to set timer to switch skey. err=%d", err);
        }
      }
      if (OB_SUCCESS == err)
      {
        if (OB_SUCCESS != (err = set_timer_major_freeze()))
        {
          YYSYS_LOG(WARN, "fail to set timer to major freeze. err=%d", err);
        }
      }
      if (OB_SUCCESS == err)
      {
        if (OB_SUCCESS != (err = set_timer_kill_zombie()))
        {
          YYSYS_LOG(WARN, "fail to set timer to kill zombie. err=%d", err);
        }
      }
      if (OB_SUCCESS == err)
      {
        if (OB_SUCCESS != (err = set_timer_time_update()))
        {
          YYSYS_LOG(WARN, "fail to set timer to time update. err=%d", err);
        }
      }
      //add peiouya [MultiUPS] [DELETE_OBI] 20150810:b
      //note: CDI_multiUPS_duht.xls:sheet 1
      if (OB_SUCCESS == err)
      {
        if (OB_SUCCESS != (err = set_timer_get_sys_table_master_ups()))
        {
          YYSYS_LOG(WARN, "fail to set timer to get sys table master ups. err=%d", err);
        }
      }
      //add 20150810:e
      if (OB_SUCCESS == err)
      {
        err = timer_.schedule(ms_list_task_, 10000000, true);
      }

      if (OB_SUCCESS == err)
      {
        if (OB_SUCCESS !=
            (err = config_mgr_.init(ms_list_task_, client_manager_, config_timer_)))
        {
          YYSYS_LOG(ERROR, "init config manager error, ret: [%d]", err);
        }
      }

      return err;
    }

    int ObUpdateServer::start_service()
    {
      int err = OB_SUCCESS;
      //del peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//      settled_obi_role_.set_role(ObiRole::SLAVE);
//      obi_role_.set_role(ObiRole::SLAVE);
      //del 20150701:e
      PerfGuard perf_guard("GPERF_FILE");
      err = start_threads();
      perf_guard.register_threads();
      shadow_server_.start(false);
      if (OB_SUCCESS == err)
      {
        err = timer_.init();
        if (OB_SUCCESS != err)
        {
          YYSYS_LOG(ERROR, "ObTimer init error, err=%d", err);
        }
        else
        {
          YYSYS_LOG(INFO, "init timer success");
        }
      }

      if (OB_SUCCESS == err)
      {
        err = config_timer_.init();
        if (OB_SUCCESS != err)
        {
          YYSYS_LOG(ERROR, "ObTimer init error, err=%d", err);
        }
        else
        {
          YYSYS_LOG(INFO, "init timer success");
        }
      }

      //�ύһ�α�����־�ط�����
      if (OB_SUCCESS == err)
      {
        err = submit_replay_commit_log();
      }

      //��ȡ���������־��ȥ��RSע��
      int64_t log_id = 0;
      //add zhaoqiong [Schema Manager] 20150327:b
      //if first start, fetch schema from rs
      //else replay log get schema, do not fetch schema from rs
      //used to avoid write full schema log
      bool need_fetch_schema = false;
      //add:e
      if (OB_SUCCESS == err)
      {
        err = log_mgr_.get_max_log_seq_replayable(log_id);
        if (OB_SUCCESS != err)
        {
          YYSYS_LOG(WARN, "fail to get max log seq replayable. err=%d", err);
        }
        //add wangjiahao [Paxos ups_replication] 20150717 :b
        else if(OB_SUCCESS != (err = log_mgr_.restore_log_term_from_log(log_id)))
        {
          YYSYS_LOG(WARN, "fail to restore log term from log. err=%d", err);
        }
        //add :e
        //add zhaoqiong [Schema Manager] 20150327:b
        else if (log_id <= 0)
        {
          need_fetch_schema = true;
        }
        //add:e
      }
      if (OB_SUCCESS == err)
      {
        //mod pangtianze [Paoxs] 20160928:b
        //register_to_rootserver(log_id);
        is_set_term_ = true;
        log_mgr_.set_log_term_inited(is_set_term_);
        YYSYS_LOG(INFO, "register to rootserver(log_id: %ld, term: %ld)", log_id, log_mgr_.get_current_log_term());
        err = register_to_rootserver(log_id);
        if (OB_SERVER_COUNT_ENOUGH == err)
        {
            role_mgr_.set_state(ObUpsRoleMgr::STOP);
            YYSYS_LOG(ERROR, "first register to rootserver failed, all ups online count is enough. err=%d", err);
        }
        else
        {
            err = OB_SUCCESS;
        }
        //mod:e
        /*
        err = set_schema();
        if (OB_SUCCESS != err)
        {
          YYSYS_LOG(WARN, "fail to set schema. err=%d", err);
        }
        */
      }

      if (OB_SUCCESS == err)
      {
        err = start_timer_schedule();
      }
      int64_t last = 0;
      int64_t now = 0;
      //add wangdonghui [ups_replication] 20170822 :b
      if (OB_SUCCESS != err)
      {
        YYSYS_LOG(ERROR, "start service failed and then stop, err=%d", err);
        role_mgr_.set_state(ObUpsRoleMgr::STOP);
      }
      //add :e
      while (ObUpsRoleMgr::STOP != role_mgr_.get_state()
             && ObUpsRoleMgr::FATAL != role_mgr_.get_state())
      {
        if (!log_mgr_.is_log_replay_started())
        {
          submit_replay_commit_log();
        }
        {
          yysys::CThreadGuard guard(&mutex_);
          //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//          if (ObiRole::MASTER == obi_role_.get_role()
//              && ObUpsRoleMgr::MASTER == role_mgr_.get_role()
//              && ObUpsRoleMgr::REPLAYING_LOG == role_mgr_.get_state()
//              && log_mgr_.is_log_replay_finished())
          if (ObUpsRoleMgr::MASTER == role_mgr_.get_role()
              && ObUpsRoleMgr::REPLAYING_LOG == role_mgr_.get_state()
              && log_mgr_.is_log_replay_finished())
           {

              slave_mgr_.set_my_log_seq(log_mgr_.get_tmp_cursor_log_id());//add lxb [bugfix_get_multi_logid]

            //add wangjiahao [Paxos ups_replication] 20150817 :b
            //mod wangdonghui [ups_replication] 20170315 :b
            //if (log_mgr_.get_flushed_clog_id() < log_mgr_.get_tmp_cursor_log_id())
            if (log_mgr_.get_flushed_clog_id() < log_mgr_.get_tmp_cursor_log_id() && config_.quorum_scale != 1)
            //mod 20170315 :e
            {
              if ((now = yysys::CTimeUtil::getTime()) - last > 500L * 1000L)
              {
                YYSYS_LOG(INFO, "waiting slave catching up to master, flushed_clog_id(%ld),tmp_cursor_log_id(%ld)", log_mgr_.get_flushed_clog_id(), log_mgr_.get_tmp_cursor_log_id());
                last = now;
              }
            }
            else if (!log_mgr_.is_tmp_log_replay_started())
            {
              if (OB_SUCCESS != (err =submit_replay_tmp_log()))
              {
                  YYSYS_LOG(ERROR, "submit_replay_tmp_log()=>%d", err);
              }
            }
            else if (log_mgr_.is_tmp_log_replay_finished())
            //add :e
            {
              YYSYS_LOG(INFO, "log_replay finished, log_mgr_start=%s", is_log_mgr_start_ ? "true" : "false");
              if (!is_log_mgr_start_)
              {
                is_log_mgr_start_ = true;
                if (OB_SUCCESS != (err = log_mgr_.start_log_for_master_write()))
                {
                  YYSYS_LOG(INFO, "log_mgr.start_log_for_master_write()=>%d", err);
                }
                else if (OB_SUCCESS!= (err = table_mgr_.write_start_log()))
                {
                  YYSYS_LOG(WARN, "fail to start log");
                }
                if (OB_SUCCESS == err)
                {
                  if (OB_SUCCESS != (err = table_mgr_.sstable_scan_finished(config_.minor_num_limit)))
                  {
                    YYSYS_LOG(ERROR, "sstable_scan_finished error, err=%d", err);
                  }
                  else
                  {
                    table_mgr_.log_table_info();
                  }
                }
                //mod zhaoqiong [Schema Manager] 20150327:b
                //if (OB_SUCCESS == err)
                if (OB_SUCCESS == err && need_fetch_schema)
                //mod e
                 {
                  bool write_log = false;
                  bool only_core = true;
                  // fetch sys schema
                  if (OB_SUCCESS != (err = sync_update_schema(true, write_log, only_core)))
                  {
                    YYSYS_LOG(WARN, "update sys schema fail ,err=%d", err);
                  }
                  else
                  {
                    YYSYS_LOG(INFO, "successfully update sys schema");
                  }
                }
              }

              if (OB_SUCCESS != err)
              {
                YYSYS_LOG(ERROR, "error=%d, stop master_master, %s", err, to_cstring(log_mgr_));
                role_mgr_.set_state(ObUpsRoleMgr::STOP);
              }
              else
              {
                int tmp_err = OB_SUCCESS;
                YYSYS_LOG(INFO, "set master_master to ACTIVE, %s", to_cstring(log_mgr_));
                YYSYS_LOG(INFO, "switch_to_master_master: switch_elapse: %ldms begin(%ld) end(%ld)",
                          (yysys::CTimeUtil::getTime()-switch_time_)/1000, switch_time_, yysys::CTimeUtil::getTime());
                //mod wangdonghui [ups_replication] 20170816 :b
                log_mgr_.set_state_as_active();
                //role_mgr_.set_state(ObUpsRoleMgr::ACTIVE);
                //mod :e
                // fetch user schema
                //mod zhaoqiong [Schema Manager] 20150327:b
  //              if (OB_SUCCESS != (tmp_err = do_async_update_whole_schema()))
  //              {
  //                YYSYS_LOG(WARN, "do_async_update_whole_schema to fetch user schema fail, err=%d", tmp_err);
  //              }
  //              else
  //              {
  //                YYSYS_LOG(INFO, "successfully do_async_update_whole_schema to fetch user schema");
  //              }
                if (need_fetch_schema)
                {
                  if (OB_SUCCESS != (tmp_err = do_async_update_whole_schema()))
                  {
                    YYSYS_LOG(WARN, "do_async_update_whole_schema to fetch user schema fail, err=%d", tmp_err);
                  }
                  else
                  {
                    YYSYS_LOG(INFO, "successfully do_async_update_whole_schema to fetch user schema");
                  }
                }
          //mod :e
              }
            }
          }
        }
        usleep(10 * 1000);
      }

      YYSYS_LOG(INFO, "mainloop finished.");
      return err;
    }

    void ObUpdateServer::req_stop()
    {
      role_mgr_.set_state(ObUpsRoleMgr::STOP);
    }

    void ObUpdateServer::stop()
    {
      ObBaseServer::stop();
    }

    int ObUpdateServer::start_threads()
    {
      int ret = OB_SUCCESS;

      /// д�߳�
      write_thread_queue_.start();

      /// ���߳�
      read_thread_queue_.start();

      //add lbzhong [Clog Monitor] 20151218:b
      clog_thread_queue_.start();
      //add:e

      /// Lease�߳�
      lease_thread_queue_.start();

      /// ת���߳�
      store_thread_.start();

      ///��־�ط��߳�
      log_replay_thread_.start();

      ///�ȴ���־ˢ���߳�
      wait_flush_thread_.start();

      replay_log_to_queue_thread_.start();
      async_load_log_thread_.start();

      return ret;
    }

    int ObUpdateServer::check_frozen_version()
    {
      int err = OB_SUCCESS;
      int64_t rs_last_frozen_version = -1;
      int64_t ups_last_frozen_version = -1;
      err = RPC_CALL_WITH_RETRY(get_rs_last_frozen_version, INT64_MAX, RPC_TIMEOUT, root_server_, rs_last_frozen_version);
      if (err != OB_SUCCESS)
      {
        YYSYS_LOG(ERROR, "get_last_frozen_version from root_server[%s] failed, err=%d", to_cstring(root_server_), err);
      }
      else if (rs_last_frozen_version < 0)
      {
        YYSYS_LOG(WARN, "get_last_frozen_version(rootserver=%s)=>%ld", to_cstring(root_server_), rs_last_frozen_version);
      }
      else if (OB_SUCCESS != (err = table_mgr_.get_last_frozen_memtable_version((uint64_t&)ups_last_frozen_version)))
      {
        YYSYS_LOG(ERROR, "table_mgr.get_last_frozen_version()=>%d", err);
      }
      else if (rs_last_frozen_version == ups_last_frozen_version)
      {
        YYSYS_LOG(INFO, "check_frozen_version(rs_last_frozen_version[%ld] == ups_last_frozen_version)",
                  rs_last_frozen_version);
      }
      else if (rs_last_frozen_version < ups_last_frozen_version)
      {
        YYSYS_LOG(WARN, "rs_last_frozen_version[%ld] < ups_last_frozen_version[%ld]",
                  rs_last_frozen_version, ups_last_frozen_version);
      }
      else
      {
        err = OB_ERROR;
        YYSYS_LOG(ERROR, "rootserver[%s]'s last_frozen_version[%ld] > ups_frozen_version[%ld].",
                  to_cstring(root_server_), rs_last_frozen_version, ups_last_frozen_version);
      }
      return err;
    }

    //mod peiouya [MultiUPS] [DELETE_OBI] 20150723:b
    //int ObUpdateServer::switch_to_master_master()
    int ObUpdateServer::switch_to_master()
    //mod 20150723:e
    {
      int err = OB_SUCCESS;
      const int64_t report_interval_us = 1000000;
      const int64_t wait_us = 10000;

      YYSYS_LOG(INFO, "SWITCHING state happen");
      yysys::CThreadGuard guard(&mutex_);
      //�ȴ�log_replay_thread�����
      //add wangdonghui [for statistics unaviable time of system] 20170303 :b:e
      switch_time_ = yysys::CTimeUtil::getTime();
      YYSYS_LOG(INFO, "wait replay thread to stop.");
      for(int64_t last_report_ts = 0, cur_ts = report_interval_us;
          !stoped_ && (!log_replay_thread_.wait_stop()||!replay_log_to_queue_thread_.wait_stop()||!async_load_log_thread_.wait_stop());
          cur_ts += wait_us)
      {
        if (cur_ts - last_report_ts >= report_interval_us)
        {
          last_report_ts = cur_ts;
          YYSYS_LOG(INFO, "wait replay_thread: %s", to_cstring(log_mgr_));
        }
        usleep(static_cast<useconds_t>(wait_us));
      }
      log_mgr_.set_async_load_thread_to_stop();
      if (stoped_)
      {
        err = OB_CANCELED;
      }
      if (OB_SUCCESS != err)
      {
        YYSYS_LOG(ERROR, "wait replay thread, err=%d, %s", err, to_cstring(log_mgr_));
      }
      else
      {
        //mod wangjiahao [Paxos ups_replication] 20150817 :b
        /*
        ObLogCursor replayed_cursor;
        int64_t replayable_log_id = 0;
        if (OB_SUCCESS != (err = log_mgr_.get_replayed_cursor(replayed_cursor)))
        {
          YYSYS_LOG(ERROR, "get_replayed_cursor()=>%d", err);
        }
        else if (OB_SUCCESS != (err = log_mgr_.get_max_log_seq_replayable(replayable_log_id)))
        {
          YYSYS_LOG(ERROR, "get_max_log_seq_replayable()=>%d", err);
        }

        else if (log_mgr_.is_log_replay_finished() && replayed_cursor.log_id_ != replayable_log_id)
        {
          //mod peiouya [MultiUPS] [DELETE_OBI] 20150810:b
          //note: CDI_multiUPS_duht.xls:sheet 1
          //expr:replace "master_master" with "master"
          //YYSYS_LOG(ERROR, "after switch to master_master: replayed_cursor[%s] != replayable_log_id[%ld]",
          //          to_cstring(replayed_cursor), replayable_log_id);
          YYSYS_LOG(ERROR, "after switch to master: replayed_cursor[%s] != replayable_log_id[%ld]",
                    to_cstring(replayed_cursor), replayable_log_id);
          //mod 20150810:e
        }
        */
        //mod :e
        if (OB_SUCCESS == err)
        {
          //del wangdonghui [ups_replication] 20170306 :b
          /*
          //add wangjiahao [Paxos ups_replication_tmplog] 20150719 :b
          log_mgr_.reset_replay_tmp_log_task();
          //add chujiajia [Paxos ups_replication] 20160104:b
          if(OB_SUCCESS != (err = submit_replay_tmp_log()))
          {
            YYSYS_LOG(WARN, "submit_replay_tmp_log error! err = %d.", err);
          }
          else
          {
            while(!log_mgr_.is_tmp_log_replay_finished())
            {
              YYSYS_LOG(INFO, "tmp_log_replay did not finish! need wait.");
              usleep(20 * 1000);
            }
            YYSYS_LOG(INFO, "tmp_log_replay_finished!");
            log_mgr_.reset();
            if (OB_SUCCESS != (err = log_mgr_.start_log_for_master_write()))
            {
              YYSYS_LOG(WARN, "submit_replay_tmp_log error! err = %d.", err);
            }
            else
            {
          //add:e
          */
          //del :e
          //add wangdonghui [ups_replication] 20170711 :b
          while(log_mgr_.is_log_replay_started() && !log_mgr_.is_log_replay_finished())
          {
            YYSYS_LOG(INFO, "local_log_replay did not finish! need wait.");
            usleep(20 * 1000);
          }
          //add :e
          log_mgr_.set_current_log_term(yysys::CTimeUtil::getTime());
          //add :e

          err = log_mgr_.set_replay_cursor();
          if (OB_SUCCESS != err)
          {
              YYSYS_LOG(ERROR,"set replay cursor fail, switch to master_master fail, %s",to_cstring(log_mgr_));
              role_mgr_.set_state(ObUpsRoleMgr::FATAL);
          }
          else
          {
              YYSYS_LOG(INFO, "switch to master succ, %s", to_cstring(log_mgr_));

              role_mgr_.set_role(ObUpsRoleMgr::MASTER);
              can_receive_log_ = true;
              log_mgr_.set_state_as_replaying_log();
              trans_executor_.get_session_mgr().enable_start_write_session();
              if(OB_SUCCESS != get_log_mgr().set_fill_log_max_log_id(get_log_mgr().get_tmp_cursor_log_id()))
              {
                  YYSYS_LOG(ERROR,"set fill_log_max_log_id failed, fill_log_max_log_id=%ld, tmp_cursor=%ld",
                            get_log_mgr().get_fill_log_max_log_id(),get_log_mgr().get_tmp_cursor_log_id());
                  err = OB_DISCONTINUOUS_LOG;
              }
              else
              {
                  YYSYS_LOG(INFO,"set fill_log_max_log_id to tmp_cursor=%ld",
                            get_log_mgr().get_fill_log_max_log_id());
              }
          }

        }
      }
      return err;
    }
    //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
    //int ObUpdateServer::master_switch_to_slave(const bool is_obi_change, const bool is_role_change)
    int ObUpdateServer::master_switch_to_slave(const bool is_role_change)
    //mod 20150701:e
    {
      int err = OB_SUCCESS;
      int64_t wait_write_session_end_timeout_us = 2000000;
      int64_t wait_us = 10000;
      log_mgr_.reset_switch();
      //add wangdonghui [ups_replication] 20170711 :b
      while(log_mgr_.is_tmp_log_replay_started() && !log_mgr_.is_tmp_log_replay_finished())
      {
        YYSYS_LOG(INFO, "tmp_log_replay did not finish! need wait.");
        usleep(20 * 1000);
      }
      //add :e
      yysys::CThreadGuard guard(&mutex_);
      YYSYS_LOG(INFO, "begin switch to slave");
      //del peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//      if (is_obi_change)
//      {
//        obi_role_.set_role(ObiRole::SLAVE);
//      }
      //del 20150701:e
      if (is_role_change)
      {
        role_mgr_.set_role(ObUpsRoleMgr::SLAVE);
        //role_mgr_.set_state(ObUpsRoleMgr::REPLAYING_LOG);
        log_mgr_.set_state_as_replaying_log();
        slave_mgr_.reset_slave_list();
        log_mgr_.reset();
      }
      //log_mgr_.reset();
      write_thread_queue_.notify_state_change();
      while(!stoped_ && !write_thread_queue_.wait_state_change_ack())
      {
        usleep(static_cast<useconds_t>(wait_us));
      }
      //add chujiajia [Paxos ups_replication]
      //trans_executor_.clean_flush_queue();
      //add:e
      while(!stoped_ && OB_SUCCESS != (err = trans_executor_.get_session_mgr().wait_write_session_end_and_lock(wait_write_session_end_timeout_us)))
      {
        YYSYS_LOG(INFO, "master_switch_to_slave wait session end.");
      }
      if (OB_SUCCESS != err)
      {
        YYSYS_LOG(ERROR, "wait_write_session_end_and_lock(timeout=%ld)=>%d, log_mgr=%s, switch_to_slave fail, will kill self",
                  wait_write_session_end_timeout_us, err, to_cstring(log_mgr_));
        kill(getpid(), SIGTERM);
      }
      else
      {
        YYSYS_LOG(INFO, "wait_write_session_end_and_lock succ, %s.", to_cstring(log_mgr_));

        //[]
        trans_executor_.clean_flush_queue();
        YYSYS_LOG(INFO, "clean flush queue succ.");

        log_mgr_.clear();//clear log_generator and log_write
        ObLogCursor cursor;
        log_mgr_.reset_replayed_cursor();
        log_mgr_.reset_real_replay_cursor();
        if (OB_SUCCESS != (err = log_mgr_.get_replay_cursor_for_disk(cursor)))
        {
        YYSYS_LOG(ERROR, "get_replay_cursor_for_disk=>%d", err);
        }
        else if (OB_SUCCESS != (err = log_mgr_.start_log(cursor)))
        {
          YYSYS_LOG(ERROR, "start_log(cursor=%s)=>%d", cursor.to_str(), err);
        }
        else if (OB_SUCCESS != (err = replay_log_src_.reset_prefetch_log_buffer2()))
        {
            YYSYS_LOG(ERROR,"reset prefetch log buffer2=>%d",err);
        }
        else
        {
            log_mgr_.set_real_replay_cursor();
            YYSYS_LOG(INFO, "MM->MS  start_log from (%s)", to_cstring(log_mgr_));
        }
        trans_executor_.get_session_mgr().disable_start_write_session();
        trans_executor_.get_session_mgr().unlock_write_session();
        while(!stoped_ && (!log_replay_thread_.wait_start()||!replay_log_to_queue_thread_.wait_start()))
        {
          YYSYS_LOG(INFO, "wait replay_thread start");
          usleep(static_cast<useconds_t>(wait_us));
        }
        YYSYS_LOG(INFO, "wait log_replay_thread start to work succ.");
      }
      if (stoped_)
      {
        err = OB_CANCELED;
        YYSYS_LOG(WARN, "UPS stoped while trying to switch to slave");
      }
      return err;
    }
    //mod :e
    //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//    int ObUpdateServer::slave_change_role(const bool is_obi_change, const bool is_role_change)
//    {
//      int err= OB_SUCCESS;
//      yysys::CThreadGuard guard(&mutex_);
//      if (ObiRole::MASTER == obi_role_.get_role()
//          && ObUpsRoleMgr::SLAVE == role_mgr_.get_role()
//          && is_obi_change && is_role_change)
//      {
//        YYSYS_LOG(INFO, "switch happen, master_slave ====> slave_master");
//        obi_role_.set_role(ObiRole::SLAVE);
//        role_mgr_.set_role(ObUpsRoleMgr::MASTER);
//      }
//      else if (ObiRole::MASTER == obi_role_.get_role()
//          && ObUpsRoleMgr::SLAVE == role_mgr_.get_role()
//          && is_obi_change && (!is_role_change))
//      {
//        YYSYS_LOG(INFO, "switch happen, master_slave ====> slave_salve");
//        obi_role_.set_role(ObiRole::SLAVE);
//      }
//      else if (ObiRole::SLAVE == obi_role_.get_role()
//          && ObUpsRoleMgr::MASTER == role_mgr_.get_role()
//          && is_obi_change && is_role_change)
//      {
//        YYSYS_LOG(INFO, "switch happen, slave_master ====> master_slave");
//        slave_report_quit();
//        slave_mgr_.reset_slave_list();
//        obi_role_.set_role(ObiRole::MASTER);
//        role_mgr_.set_role(ObUpsRoleMgr::SLAVE);
//      }
//      else if (ObiRole::SLAVE == obi_role_.get_role()
//          && ObUpsRoleMgr::MASTER == role_mgr_.get_role()
//          && !is_obi_change && is_role_change)
//      {
//        YYSYS_LOG(INFO, "switch happen, slave_master ====> slave_slave");
//        slave_report_quit();
//        slave_mgr_.reset_slave_list();
//        role_mgr_.set_role(ObUpsRoleMgr::SLAVE);
//      }
//      else if (ObiRole::SLAVE == obi_role_.get_role()
//          && ObUpsRoleMgr::SLAVE == role_mgr_.get_role()
//          && is_obi_change && !is_role_change)
//      {
//        YYSYS_LOG(INFO, "switch happen, slave_slave ====> master_slave");
//        obi_role_.set_role(ObiRole::MASTER);
//      }
//      else if (ObiRole::SLAVE == obi_role_.get_role()
//          && ObUpsRoleMgr::SLAVE == role_mgr_.get_role()
//          && !is_obi_change && is_role_change)
//      {
//        YYSYS_LOG(INFO, "switch happen, slave_slave ====> slave_master");
//        role_mgr_.set_role(ObUpsRoleMgr::MASTER);
//      }
//      else
//      {
//        YYSYS_LOG(WARN, "invalid switch case. cur_obi_role=%s, cur_role=%s, is_obi_change=%s, is_role_change=%s",
//            obi_role_.get_role_str(), role_mgr_.get_role_str(),
//            is_obi_change ? "TRUE" : "FALSE", is_role_change ? "TRUE" : "FALSE");
//      }

//      YYSYS_LOG(INFO, "slave change role, %s", to_cstring(log_mgr_));
//      return err;
//    }

    //del peiouya [MultiUPS] [DELETE_OBI] 20150723:b
//    int ObUpdateServer::slave_change_role(const bool is_role_change)
//    {
//      int err = OB_SUCCESS;
//      yysys::CThreadGuard guard(&mutex_);
//      if (ObUpsRoleMgr::SLAVE == role_mgr_.get_role()
//          && is_role_change)
//      {
//        YYSYS_LOG(INFO, "switch happen, slave ====> master");
//        role_mgr_.set_role(ObUpsRoleMgr::MASTER);
//      }
//      else
//      {
//        YYSYS_LOG(WARN, "invalid switch case. cur_role=%s, is_role_change=%s",
//            role_mgr_.get_role_str(), is_role_change ? "TRUE" : "FALSE");
//      }
//      YYSYS_LOG(INFO, "slave change role, %s", to_cstring(log_mgr_));
//      return err;
//    }
    //del 20150723:e
    //mod 20150701:e

    //int ObUpdateServer::reregister_standalone()
    //{
    //  int err = OB_SUCCESS;

    //  YYSYS_LOG(INFO, "reregister STANDALONE SLAVE");
    //  is_registered_to_ups_ = false;

    //  // slave register
    //  if (OB_SUCCESS == err)
    //  {
    //    uint64_t log_id_start = 0;
    //    uint64_t log_seq_start = 0;
    //    err = slave_register_standalone(log_id_start, log_seq_start);
    //    if (OB_SUCCESS != err)
    //    {
    //      YYSYS_LOG(WARN, "failed to register, err=%d", err);
    //    }
    //  }

    //  if (OB_SUCCESS != err)
    //  {
    //    YYSYS_LOG(INFO, "REREGISTER err");
    //  }
    //  else
    //  {
    //    YYSYS_LOG(INFO, "REREGISTER succ");
    //  }
    //  return err;
    //}

    int ObUpdateServer::set_timer_switch_skey()
    {
      int err = OB_SUCCESS;

      bool repeat = true;
      err = timer_.schedule(switch_skey_duty_, SKEY_UPDATE_PERIOD, repeat);
      if (OB_SUCCESS != err)
      {
        YYSYS_LOG(WARN, "schedule switch_skey_duty fail err=%d", err);
      }
      else
      {
        submit_switch_skey();
      }
      return err;
    }

    int ObUpdateServer::set_timer_check_keep_alive()
    {
      int err = OB_SUCCESS;
      bool repeat = true;
      err = timer_.schedule(check_keep_alive_duty_, config_.state_check_period, repeat);
      if (OB_SUCCESS != err)
      {
        YYSYS_LOG(WARN, "schedule check_keep_alive_duty_ fail .err=%d", err);
      }
      else
      {
        submit_check_keep_alive();
      }
      return err;
    }

    int ObUpdateServer::set_timer_time_update()
    {
      int err = OB_SUCCESS;
      bool repeat = true;
      err = timer_.schedule(time_update_duty_, TimeUpdateDuty::SCHEDULE_PERIOD, repeat);
      if (OB_SUCCESS != err)
      {
        YYSYS_LOG(WARN, "schedule time_update_duty_ fail .err=%d", err);
      }
      return err;
    }

    int ObUpdateServer::set_timer_check_lease()
    {
      int err = OB_SUCCESS;
      bool repeat = true;
      err = timer_.schedule(ups_lease_task_, DEFAULT_CHECK_LEASE_INTERVAL, repeat);
      if (OB_SUCCESS != err)
      {
        YYSYS_LOG(WARN, "schedule check_keep_alive_duty_ fail .err=%d", err);
      }
      else
      {
        submit_lease_task();
      }
      return err;
    }

    //add peiouya [MultiUPS] [DELETE_OBI] 20150810:b
    //note: CDI_multiUPS_duht.xls:sheet 1
    int ObUpdateServer::set_timer_get_sys_table_master_ups()
    {
      int err = OB_SUCCESS;
      bool repeat = true;
      //also use DEFAULT_CHECK_LEASE_INTERVAL
      err = timer_.schedule(get_sys_ups_master_duty_, DEFAULT_CHECK_LEASE_INTERVAL, repeat);
      if (OB_SUCCESS != err)
      {
        YYSYS_LOG(WARN, "schedule get_sys_ups_master_duty_ fail .err=%d", err);
      }
      else
      {
        submit_get_sys_ups_master ();
      }
      return err;
    }
    //add 20150810:e

    int ObUpdateServer::set_timer_grant_keep_alive()
    {
      int err = OB_SUCCESS;
      bool repeat = true;
      err = timer_.schedule(grant_keep_alive_duty_, GRANT_KEEP_ALIVE_PERIOD, repeat);
      if (OB_SUCCESS != err)
      {
        YYSYS_LOG(WARN, "schedule check_keep_alive_duty_ fail .err=%d", err);
      }
      else
      {
        submit_grant_keep_alive();
      }
      return err;
    }

    int ObUpdateServer::set_timer_major_freeze()
    {
      int err = OB_SUCCESS;

      bool repeat = true;
      //mod peiouya [DUTY_FREEZE_BUG_FIX] 20160418:b
      //multi-servers' times are not in synch, so we only choose the group SYS_TABLE_PAXOS_ID to execute duty freeze tasks.
      //err = timer_.schedule(major_freeze_duty_, MajorFreezeDuty::SCHEDULE_PERIOD, repeat);
      if (SYS_TABLE_PAXOS_ID == (int64_t)config_.paxos_id)
      {
        err = timer_.schedule(major_freeze_duty_, MajorFreezeDuty::SCHEDULE_PERIOD, repeat);
      }
      //mod 20160418:e

      if (OB_SUCCESS != err)
      {
        YYSYS_LOG(WARN, "schedule major_freeze_duty fail err=%d", err);
      }

      return err;
    }

    int ObUpdateServer::set_timer_kill_zombie()
    {
      int err = OB_SUCCESS;

      bool repeat = true;
      //[620] mod
      //err = timer_.schedule(kill_zombie_duty_, KillZombieDuty::SCHEDULE_PERIOD, repeat);
      err = timer_.schedule(kill_zombie_duty_, config_.kill_zombie_schedule_interval, repeat);

      if (OB_SUCCESS != err)
      {
        YYSYS_LOG(WARN, "schedule kill_zombie_duty fail err=%d", err);
      }

      return err;
    }

    int ObUpdateServer::set_timer_handle_fronzen()
    {
      int err = OB_SUCCESS;

      bool repeat = true;
      err = timer_.schedule(handle_frozen_duty_, HandleFrozenDuty::SCHEDULE_PERIOD, repeat);
      if (OB_SUCCESS != err)
      {
        YYSYS_LOG(WARN, "schedule handle_frozen_duty fail err=%d", err);
      }

      return err;
    }

    int ObUpdateServer::set_timer_minor_freeze()
    {
        int err = OB_SUCCESS;
        bool repeat = false;
        err = timer_.schedule(minor_freeze_duty_, 0, repeat);
        if(OB_SUCCESS != err)
        {
            YYSYS_LOG(WARN,"schedule minor_freeze_duty_ failed err=%d", err);
        }
        return err;
    }

    //[588]
    int ObUpdateServer::set_timer_check_memtable_lock()
    {
        int err= OB_SUCCESS;
        bool repeat = true;
        err = timer_.schedule(memtable_lock_duty_, MemtableLockDuty::SCHEDULE_PERIOD, repeat);
        if(OB_SUCCESS != err)
        {
            YYSYS_LOG(WARN, "schedule memtable_lock_duty fail err=%d", err);
        }
        return err;
    }
    void ObUpdateServer::cancel_timer_check_memtable_lock()
    {
        timer_.cancel(memtable_lock_duty_);
    }

    int ObUpdateServer::register_to_master_ups(const ObServer &master)
    {
      int err = OB_SUCCESS;
      ObUpsFetchParam fetch_param;
      uint64_t max_log_seq;
      ObServer null_server;
      if (null_server == master)
      {
        err = OB_INVALID_ARGUMENT;
        YYSYS_LOG(INFO, "master ups is %s, need wait new master ups", null_server.to_cstring());
      }
      //add wangdonghui [ups_replication] 20170821 :b
      else if(log_mgr_.is_log_replay_finished())
      //add :e
      {
        if (OB_SUCCESS == err)
        {
          err = slave_register_followed(master, fetch_param, max_log_seq);
          if (OB_SUCCESS != err)
          {
            YYSYS_LOG(WARN, "failed to register, err=%d, master_addr=%s", err, master.to_cstring());
          }
          else
          {
            YYSYS_LOG(INFO, "register to master ups succ. master_ups=%s", master.to_cstring());
          }
        }
        if (OB_SUCCESS == err)
        {
          log_mgr_.set_master_log_id(max_log_seq);
          YYSYS_LOG(INFO, "log_mgr_.set_master_log_id=%ld", max_log_seq);
        }
      }
      return err;
    }

    //int ObUpdateServer::register_and_start_fetch(const ObServer &master, uint64_t &replay_point)
    //{
    //  int err = OB_SUCCESS;
    //  ObUpsFetchParam fetch_param;
    //  if (OB_SUCCESS == err)
    //  {
    //    err = slave_register_followed(master, fetch_param);
    //    if (OB_SUCCESS != err)
    //    {
    //      YYSYS_LOG(WARN, "failed to register");
    //    }
    //  }
    //  if (OB_SUCCESS == err)
    //  {
    //    if (fetch_param.max_log_id_ < static_cast<uint64_t>(replay_start_log_id_))
    //    {
    //      err = OB_ERROR;
    //      YYSYS_LOG(ERROR, "slave ups has bigger log id than master ups, fetch_param.max_log_id=%ld, replay_start_log_id=%ld", fetch_param.max_log_id_, replay_start_log_id_);
    //    }
    //  }
    //  if (OB_SUCCESS == err)
    //  {
    //    //fetch_thread_.clear();
    //    //fetch_thread_.set_fetch_param(fetch_param);
    //    //fetch_thread_.set_master(master);
    //    //fetch_thread_.start();
    //  }
    //  if (OB_SUCCESS == err)
    //  {
    //    replay_point = fetch_param.min_log_id_;
    //    // compare the max log file and the fetch replay_point
    //    if (replay_point > static_cast<uint64_t>(replay_start_log_id_))
    //    {
    //      //slave' log too old. destory the memetable.
    //      TableMgr *table_mgr = table_mgr_.get_table_mgr();
    //      table_mgr->destroy();
    //      table_mgr->init();
    //      replay_start_log_id_ = replay_point;
    //      replay_start_log_seq_ = 0;
    //    }
    //    YYSYS_LOG(INFO, "set log replay point to %lu", replay_start_log_id_);
    //  }
    //  if (OB_SUCCESS != err)
    //  {
    //    YYSYS_LOG(WARN, "fail to register and start fetch, err=%d", err);
    //  }
    //  else
    //  {
    //    YYSYS_LOG(INFO, "register to master ups succ.");
    //  }
    //  return err;
    //}

    //del peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//    int ObUpdateServer::renew_master_inst_ups()
//    {
//      int err = OB_SUCCESS;
//      //mod peiouya [MultiUPS] [UPS_Manage_Function] 20150504:b
//      err = ups_rpc_stub_.get_inst_master_ups(root_server_, ups_inst_master_, config_.paxos_id, DEFAULT_NETWORK_TIMEOUT);
//      //mod 20150504:e
//      if (OB_SUCCESS == err)
//      {
//        YYSYS_LOG(INFO, "renew master inst ups =%s", ups_inst_master_.to_cstring());
//      }
//      else
//      {
//        YYSYS_LOG(WARN, "fail to get inst master ups. err=%d", err);
//      }
//      return err;
//    }
    //del 20150701:e

    int ObUpdateServer::set_self_(const char* dev_name, const int32_t port)
    {
      int ret = OB_SUCCESS;
      int32_t ip = yysys::CNetUtil::getLocalAddr(dev_name);
      if (0 == ip)
      {
        YYSYS_LOG(ERROR, "cannot get valid local addr on dev:%s.", dev_name);
        ret = OB_ERROR;
      }
      if (OB_SUCCESS == ret)
      {
        bool res = self_addr_.set_ipv4_addr(ip, port);
        //add lbzhong [Paxos Cluster.Balance] 20160707:b
        self_addr_.cluster_id_ = (int32_t) config_.cluster_id;
        //add:e
        if (!res)
        {
          YYSYS_LOG(ERROR, "chunk server dev:%s, port:%d is invalid.",
              dev_name, port);
          ret = OB_ERROR;
        }
        else
        {
          YYSYS_LOG(INFO, "update server addr =%s", self_addr_.to_cstring());
        }
      }

      return ret;
    }
    //del peiouya [MultiUPS] [UPS_SYNC_Frozen] 20150528:b
    /*
    int ObUpdateServer::report_frozen_version_()
    {
      int ret = OB_SUCCESS;
      int64_t num_times = config_.resp_root_times;
      int64_t timeout = config_.resp_root_timeout;
      uint64_t last_frozen_version = 0;
      ret = table_mgr_.get_last_frozen_memtable_version(last_frozen_version);
      if (OB_SUCCESS == ret)
      {
        ret = RPC_CALL_WITH_RETRY(report_freeze, num_times, timeout, root_server_, ups_master_, last_frozen_version);
      }
      if (OB_RESPONSE_TIME_OUT == ret)
      {
        YYSYS_LOG(ERROR, "report fronzen version timeout, num_times=%ld, timeout=%ldus", num_times, timeout);
      }
      else if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "error occurs when report frozen version, ret=%d", ret);
      }
      else
      {
        YYSYS_LOG(INFO, "report succ frozen version=%ld", last_frozen_version);
      }
      OB_STAT_SET(UPDATESERVER, UPS_STAT_FROZEN_VERSION, last_frozen_version);
      return ret;
    }
    */
    //del 20150528:e

    int ObUpdateServer::replay_commit_log_()
    {
      int err = OB_SUCCESS;
      if (log_mgr_.is_log_replay_started())
      {
        YYSYS_LOG(WARN, "replay process already started, refuse to replay twice");
      }
      else
      {
        err = log_mgr_.do_replay_local_log_task();
        //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
        if(OB_SUCCESS == err && (role_mgr_.get_role() == ObUpsRoleMgr::MASTER))//[520]
        {
          err = log_mgr_.do_process_unsettled_trans();
        }
        //add 20150701:e
      }
      return err;
    }
    //add wangjiahao [Paxos ups_replication] 20150719 :b
    int ObUpdateServer::replay_tmp_log_()
    {
        int err = OB_SUCCESS;
        if (!log_mgr_.is_log_replay_finished())
        {
          YYSYS_LOG(WARN, "replay local log process is running, try again");
        }
        else if (log_mgr_.is_tmp_log_replay_started())
        {
          YYSYS_LOG(WARN, "replay tmp log process already started, refuse to replay twice");
        }
        else
        {
          err = log_mgr_.do_replay_tmp_log_task();
          if(OB_SUCCESS == err && (role_mgr_.get_role() == ObUpsRoleMgr::MASTER))//[520]
          {
              err = log_mgr_.do_process_unsettled_trans();
          }
        }
        return err;
    }
    //add :e

    int ObUpdateServer::prefetch_remote_log_(ObDataBuffer& in_buf)
    {
      int err = OB_SUCCESS;
      ObPrefetchLogTaskSubmitter::Task task;
      if (in_buf.get_position() + (int64_t)sizeof(task) > in_buf.get_capacity())
      {
        err = OB_ERR_UNEXPECTED;
        YYSYS_LOG(ERROR, "pos[%ld] + sizeof(task)[%ld] > capacity[%ld]",
                  in_buf.get_position(), sizeof(task), in_buf.get_capacity());
      }
      else
      {
        task = *((typeof(task)*)(in_buf.get_data() + in_buf.get_position()));
      }
      if (OB_SUCCESS != err)
      {}
      else if (OB_SUCCESS != (err = replay_log_src_.prefetch_log())
          && OB_NEED_RETRY != err)
      {
        YYSYS_LOG(WARN, "replay_log_src.prefetch_log(%s)=>%d", to_cstring(replay_log_src_), err);
      }
      else if (OB_NEED_RETRY == err)
      {
        YYSYS_LOG(WARN, "fetch_log_from_master(%s): DATA_NOTE_SERVE, need wait master: %dus", to_cstring(replay_log_src_), int(config_.lsync_fetch_timeout));
        usleep((useconds_t)(config_.lsync_fetch_timeout));
      }
      else
      {
        err = OB_SUCCESS;
      }
      prefetch_log_task_submitter_.done(task);
      return err;
    }

    int ObUpdateServer::sync_update_schema(const bool always_try, const bool write_log, bool only_core_tables)
    {
      int err = OB_SUCCESS;
      int64_t num_times = always_try ? INT64_MAX : config_.fetch_schema_times;
      int64_t timeout = config_.fetch_schema_timeout;

      CommonSchemaManagerWrapper schema_mgr;
      err = RPC_CALL_WITH_RETRY(fetch_schema, num_times, timeout, root_server_, 0, schema_mgr, only_core_tables);

      if (OB_RESPONSE_TIME_OUT == err)
      {
        YYSYS_LOG(ERROR, "fetch schema timeout, num_times=%ld, timeout=%ldus",
            num_times, timeout);
        err = OB_RESPONSE_TIME_OUT;
      }
      else if (OB_SUCCESS != err)
      {
        YYSYS_LOG(WARN, "Error occurs when fetching schema, err=%d", err);
      }
      else
      {
        YYSYS_LOG(INFO, "Fetching schema succeed version=%ld", schema_mgr.get_version());
      }

      if (OB_SUCCESS == err)
      {
        if (write_log)
        {
          YYSYS_LOG(INFO, "start to switch schemas");
          err = table_mgr_.switch_schemas(schema_mgr);
        }
        else
        {
          err = table_mgr_.set_schemas(schema_mgr);
        }
        if (OB_SUCCESS != err)
        {
          YYSYS_LOG(ERROR, "failed to set schema, err=%d", err);
        }
      }

      return err;
    }
    int ObUpdateServer::slave_register_followed(const ObServer &master, ObUpsFetchParam & fetch_param, uint64_t &max_log_seq)
    {
      int err = OB_SUCCESS;
      const ObServer& self_addr = get_self();
      ObSlaveSyncType slave_type;
      ObSlaveInfo slave_info;
      slave_type.set_sync_type(config_.real_time_slave
                               ? ObSlaveSyncType::REAL_TIME_SLAVE
                               : ObSlaveSyncType::NON_REAL_TIME_SLAVE);
      slave_info.self = self_addr;
      slave_info.min_sstable_id = sstable_mgr_.get_min_sstable_id();
      slave_info.max_sstable_id = sstable_mgr_.get_max_sstable_id();
      //add wangjiahao [Paxos ups_replication] 20150817 :b
      slave_info.log_id = log_mgr_.get_flushed_cursor_log_id();
      //add :e
      int64_t timeout = config_.register_timeout;
      err = ups_rpc_stub_.slave_register_followed(master, slave_info, slave_type, fetch_param, max_log_seq, timeout);
      if (OB_SUCCESS != err && OB_ALREADY_REGISTERED != err)
      {
        YYSYS_LOG(WARN, "fail to register to master ups[%s]. err=%d", master.to_cstring(), err);
      }
      return err;
    }

    int ObUpdateServer::handlePacket(ObPacket* packet)
    {
      static volatile uint64_t cpu = 0;
      rebind_cpu(config_.io_thread_start_cpu, config_.io_thread_end_cpu, cpu, __FILE__, __LINE__);
      static ObPCap pcap(getenv("pcap_cmd"));
      pcap.handle_packet(packet);
      // ����ǰ�����ֶ� trace_id��chid
      PROFILE_LOG(DEBUG, TRACE_ID
                  SOURCE_CHANNEL_ID
                  UPS_REQ_START_TIME, packet->get_trace_id(), packet->get_channel_id(), packet->get_receive_ts());
      int rc = OB_SUCCESS;
      ObPacket* req = (ObPacket*) packet;
      int64_t id = req->get_trace_id();
      if (0 == id)
      {
        TraceId *trace_id = reinterpret_cast<TraceId*>(&id);
        trace_id->id.seq_ = atomic_inc(&(SeqGenerator::seq_generator_));
        trace_id->id.ip_ = static_cast<uint16_t>(local_ip_ & 0x0000FFFF);
        trace_id->id.port_ = static_cast<uint16_t>(port_);
        req->set_trace_id(trace_id->uval_);
      }
      bool ps = true;
      int packet_code = req->get_packet_code();
      int32_t priority = req->get_packet_priority();
      YYSYS_LOG(DEBUG,"get packet code is %d, priority=%d", packet_code, priority);
      switch (packet_code)
      {
      //del peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//      case OB_SET_OBI_ROLE:
//        ps = read_thread_queue_.push(req, read_task_queue_size_, false,
//                                     (NORMAL_PRI == priority)
//                                     ? PriorityPacketQueueThread::NORMAL_PRIV
//                                     : PriorityPacketQueueThread::LOW_PRIV);
//        break;
      //del 20150701:e
      case OB_UPS_CLEAR_FATAL_STATUS:
        ps = lease_thread_queue_.push(req, lease_task_queue_size_, false);
        break;
      case OB_SEND_LOG:
        //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//        if ((ObUpsRoleMgr::SLAVE == role_mgr_.get_role()
//             || ObiRole::SLAVE == obi_role_.get_role())
//            && ObUpsRoleMgr::FATAL != role_mgr_.get_state())
        if (ObUpsRoleMgr::SLAVE == role_mgr_.get_role()
            && ObUpsRoleMgr::FATAL != role_mgr_.get_state())
        //mod 20150701:e
        {
            //add wangjiahao [Paxos ups_replication_tmplog] 20150609 :e
            //del chujiajia [Paxos ups_replication] 20160107:b
            //log_mgr_.set_master_max_commit_log_id(req->get_cmt_log_seq());
            //del:e
            //add :e
          //mod wangdonghui [ups_replication] 20170717 :b
          //trans_executor_.handle_packet(*req);
          //ps = true;
          if (can_receive_log_)
          {
            //add pangtianze [Paxos ups_relication] 20170808:b
            char ip_buf[32];
            ups_master_.ip_to_string(ip_buf, sizeof(ip_buf));
            if(ups_master_.is_valid() && strncmp(get_peer_ip(req->get_request()), ip_buf, strlen(ip_buf)) != 0)
            {
              ps = false;
              YYSYS_LOG(WARN, "receive log from ups[%s], not master, ignore it", get_peer_ip(req->get_request()));
            }
            else
            {
              //add:e
              trans_executor_.handle_packet(*req);
              ps = true;
            }
          }
          else
          {
            char ip_buf[32];
            ups_master_.ip_to_string(ip_buf, sizeof(ip_buf));
            if(strncmp(get_peer_ip(req->get_request()), ip_buf, strlen(ip_buf)) == 0)
            {
              YYSYS_LOG(INFO, "master ups has been changed(%s), can_receive_log=>true", to_cstring(ups_master_));
              can_receive_log_ = true;
              trans_executor_.handle_packet(*req);
              ps = true;
            }
            else
            {
              ps = false;
            }
          }
          //YYSYS_LOG(INFO, "(%s)<=>(%s)", get_peer_ip(req->get_request()), to_cstring(ups_master_));
          //mod :e
        }
        break;
      case OB_FETCH_LOG:
        ps = read_thread_queue_.push(req, read_task_queue_size_, false,
                                     (NORMAL_PRI == priority)
                                     ? PriorityPacketQueueThread::NORMAL_PRIV
                                     : PriorityPacketQueueThread::LOW_PRIV);
        break;
      case OB_FILL_LOG_CURSOR:
        ps = read_thread_queue_.push(req, read_task_queue_size_, false,
                                     (NORMAL_PRI == priority)
                                     ? PriorityPacketQueueThread::NORMAL_PRIV
                                     : PriorityPacketQueueThread::LOW_PRIV);
        break;
      case OB_GET_CLOG_STATUS:
        ps = read_thread_queue_.push(req, read_task_queue_size_, false,
                                     (NORMAL_PRI == priority)
                                     ? PriorityPacketQueueThread::NORMAL_PRIV
                                     : PriorityPacketQueueThread::LOW_PRIV);
        break;
        //add lbzhong [Clog Monitor] 20151218:b
      case OB_CLOG_MONITOR_GET_UPS_LIST:
      case OB_CLOG_MONITOR_GET_CLOG_STAT:
        //add:e
      case OB_UPS_GET_COMMIT_LOG_STAT:
        ps = clog_thread_queue_.push(req, lease_task_queue_size_, false);
        break;
      case OB_WRITE:
      case OB_TRUNCATE_TABLE:
      case OB_INTERNAL_WRITE:
      case OB_MS_MUTATE:
      case OB_FAKE_WRITE_FOR_KEEP_ALIVE:
      case OB_PHY_PLAN_EXECUTE:
      case OB_START_TRANSACTION:
      case OB_END_TRANSACTION:
      //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
      //for distributed transaction
      case OB_WRITE_TRANS_STAT:
      case OB_END_DISTRIBUTED_TRANS:
      case OB_TRANS_PREPARE_ACK:
      case OB_ROLLBACK_TRANSACTION:
      case OB_PREPARE_TRANSACTION:
      case OB_COMMIT_TRANSACTION:
      case OB_WRITE_TRANS_STAT_ACK:
      case OB_GET_PUBLICED_TRANCEID://add by maosy [MultiUps 1.0] [batch_iud_snapshot] 20170622 
      //add 20150701:e
        if (ObUpsRoleMgr::FATAL != role_mgr_.get_state())
        {
          //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//          if (ObiRole::MASTER != obi_role_.get_role()
//              || ObUpsRoleMgr::MASTER != role_mgr_.get_role())
//          {
//            if (REACH_TIME_INTERVAL(10 * 1000 * 1000))
//            {
//              YYSYS_LOG(WARN, "ups not master.obi_role=%s, role=%s", obi_role_.get_role_str(), role_mgr_.get_role_str());
//            }
//            response_result_(OB_NOT_MASTER, OB_WRITE_RES, MY_VERSION, packet->get_request(), packet->get_channel_id());
//            rc = OB_ERROR;
//          }
          if (ObUpsRoleMgr::MASTER != role_mgr_.get_role())
          {
            if (REACH_TIME_INTERVAL(10 * 1000 * 1000))
            {
              YYSYS_LOG(WARN, "ups not master, role=%s", role_mgr_.get_role_str());
            }
            response_result_(OB_NOT_MASTER, OB_WRITE_RES, MY_VERSION, packet->get_request(), packet->get_channel_id());
            rc = OB_ERROR;
          }
          //mod 20150701:e
          else if (ObUpsRoleMgr::ACTIVE != role_mgr_.get_state())
          {
            if (REACH_TIME_INTERVAL(10 * 1000 * 1000))
            {
              YYSYS_LOG(WARN, "master ups state not ACTIVE. refuse write. role_state=%s", role_mgr_.get_state_str());
            }
            response_result_(OB_NOT_MASTER, OB_WRITE_RES, MY_VERSION, packet->get_request(), packet->get_channel_id());
            rc = OB_ERROR;
          }
          else if (!is_lease_valid())
          {
            YYSYS_LOG(WARN, "master ups lease is nearly to timeout. refuse write.");
            response_result_(OB_NOT_MASTER, OB_WRITE_RES, MY_VERSION, packet->get_request(), packet->get_channel_id());
            rc = OB_ERROR;
          }
          else if (OB_INTERNAL_WRITE == packet_code || OB_FAKE_WRITE_FOR_KEEP_ALIVE == packet_code)
          {
            //ps = write_thread_queue_.push(req, write_task_queue_size_, false);
            trans_executor_.handle_packet(*req);
            ps = true;
          }
          //add by wangdonghui [paxos ups_replication] 20160929 :b
          else if (config_.quorum_scale/2 > slave_mgr_.get_num())
          {
              if(REACH_TIME_INTERVAL(10*1000*1000))
              {
                  YYSYS_LOG(WARN, "cluster has no enough ups slaves: quorum_scale:[%d], slave_num:[%d]",
                      (int32_t)config_.quorum_scale, slave_mgr_.get_num());
              }
            response_result_(OB_NOT_ENOUGH_SLAVE, OB_WRITE_RES, MY_VERSION, packet->get_request(), packet->get_channel_id());
            rc = OB_ERROR;
          }
          //add wangdonghui :e
          else
          {
            //ps = write_thread_queue_.push(req, write_task_queue_size_, false);
            trans_executor_.handle_packet(*req);
            ps = true;
          }
        }
        else
        {
          YYSYS_LOG(DEBUG, "ups state is FATAL, refuse write. state=%s", role_mgr_.get_state_str());
          response_result_(OB_NOT_MASTER, OB_WRITE_RES, MY_VERSION, packet->get_request(), packet->get_channel_id());
          rc = OB_ERROR;
        }
        break;
      case OB_SLAVE_REG:
        if (ObUpsRoleMgr::MASTER == role_mgr_.get_role())
        {
          //ps = write_thread_queue_.push(req, write_task_queue_size_, false);
          ps = lease_thread_queue_.push(req, lease_task_queue_size_, false);
        }
        else
        {
          response_result_(OB_NOT_MASTER, OB_SLAVE_REG_RES, MY_VERSION, packet->get_request(), packet->get_channel_id());
          rc = OB_ERROR;
        }
        break;
      //add zhaoqiong [fixed for Backup]:20150811:b
      case OB_BACKUP_REG:
        if (ObUpsRoleMgr::MASTER == role_mgr_.get_role())
        {
          ps = write_thread_queue_.push(req, write_task_queue_size_, false);
        }
        else
        {
          response_result_(OB_NOT_MASTER, OB_BACKUP_REG_RES, MY_VERSION, packet->get_request(), packet->get_channel_id());
          rc = OB_ERROR;
        }
        break;
      //add:e
      case OB_FREEZE_MEM_TABLE:
      case OB_UPS_MINOR_FREEZE_MEMTABLE:
      case OB_UPS_MINOR_LOAD_BYPASS:
      case OB_UPS_MAJOR_LOAD_BYPASS:
      case OB_UPS_CLEAR_ACTIVE_MEMTABLE:
      //case OB_TRUNCATE_TABLE: //add zhaoqiong [Truncate Table] 20160127
      case OB_SWITCH_SCHEMA:
      case OB_SWITCH_SCHEMA_MUTATOR://add zhaoqiong [Schema Manager] 20150327
      case OB_SWITCH_TMP_SCHEMA://add zhaoqiong [Schema Manager] 20150420
      case OB_UPS_FORCE_FETCH_SCHEMA:
      case OB_UPS_SWITCH_COMMIT_LOG:
      case OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE:
      //add peiouya [MultiUPS] [UPS_SYNC_Frozen] 20150521:b
      case OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE_ACCORD_HB:
      //add 20150521:e
      case OB_UPS_ASYNC_AUTO_FREEZE_MEMTABLE:
      case OB_UPS_ASYNC_CHECK_CUR_VERSION:
      case OB_UPS_ASYNC_CHECK_SSTABLE_CHECKSUM:
        //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//        if (ObUpsRoleMgr::MASTER == role_mgr_.get_role()
//            && ObiRole::MASTER == obi_role_.get_role()
//            && ObUpsRoleMgr::FATAL != role_mgr_.get_state())
        if (ObUpsRoleMgr::MASTER == role_mgr_.get_role()
            && ObUpsRoleMgr::FATAL != role_mgr_.get_state())
        //mod 20150701:e
        {
          ps = write_thread_queue_.push(req, write_task_queue_size_, false);
        }
        else
        {
          return_not_master(MY_VERSION, packet->get_request(), packet->get_channel_id(), packet_code);
          rc = OB_ERROR;
        }
        break;
      case OB_GET_REQUEST:
      case OB_SCAN_REQUEST:
      case OB_NEW_GET_REQUEST:
      case OB_NEW_SCAN_REQUEST:
      case OB_UPS_SHOW_SESSIONS:
      case OB_UPS_KILL_SESSION:
      case OB_CHECK_SESSION_EXIST://add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150727
      case OB_SWITCH_PARTITION_STAT: //[449]
        trans_executor_.handle_packet(*req);
        break;
        //if (!get_service_state())
        //{
        //  ps = false;
        //}
        //else
        //{
        //  ps = read_thread_queue_.push(req, read_task_queue_size_, false,
        //                               (NORMAL_PRI == priority)
        //                               ? PriorityPacketQueueThread::NORMAL_PRIV
        //                               : PriorityPacketQueueThread::LOW_PRIV);
        //}
        //break;
      case OB_UPS_GET_BLOOM_FILTER:
      case OB_UPS_DUMP_TEXT_MEMTABLE:
      case OB_UPS_DUMP_TEXT_SCHEMAS:
      case OB_UPS_MEMORY_WATCH:
      case OB_UPS_MEMORY_LIMIT_SET:
      case OB_UPS_PRIV_QUEUE_CONF_SET:
      case OB_UPS_RELOAD_CONF:
      case OB_UPS_GET_LAST_FROZEN_VERSION:
      case OB_UPS_GET_TABLE_TIME_STAMP:
      case OB_UPS_ENABLE_MEMTABLE_CHECKSUM:
      case OB_UPS_DISABLE_MEMTABLE_CHECKSUM:
      case OB_FETCH_STATS:
      case OB_FETCH_SCHEMA:
      //add wenghaixing [secondary index.cluster]20150630
      case OB_FETCH_INIT_INDEX:
      //add e
      case OB_RS_FETCH_SPLIT_RANGE:
      case OB_UPS_STORE_MEM_TABLE:
      case OB_UPS_DROP_MEM_TABLE:
      case OB_UPS_ASYNC_FORCE_DROP_MEMTABLE:
      case OB_UPS_ASYNC_LOAD_BYPASS:
      case OB_UPS_ERASE_SSTABLE:
      case OB_UPS_LOAD_NEW_STORE:
      case OB_UPS_RELOAD_ALL_STORE:
      case OB_UPS_RELOAD_STORE:
      case OB_UPS_UMOUNT_STORE:
      case OB_UPS_FORCE_REPORT_FROZEN_VERSION:
      case OB_GET_CLOG_CURSOR:
      case OB_GET_CLOG_MASTER:
      case OB_GET_LOG_SYNC_DELAY_STAT:
      case OB_RS_GET_MAX_LOG_SEQ:
      //add pangtianze [Paxos ups_replication] 20150604
      case OB_RS_GET_MAX_LOG_SEQ_AND_TERM:
      //add:e
      //add pangtianze [Paxos rs_election] 20160919:b
      case OB_RS_GET_UPS_ROLE:
      //add:e
      //add pangtianze [Paxos rs_election] 20170228:b
      case OB_REFRESH_RS_LIST:
      //add:e
      case OB_GET_CLOG_STAT:
      case OB_SQL_SCAN_REQUEST:
      case OB_SET_CONFIG:
      case OB_GET_CONFIG:

      case OB_CHECK_INCREMENTAL_RANGE: /*add zhaoqiong [Truncate Table]:20160318*/
      case OB_FETCH_SCHEMA_NEXT://add zhaoqiong [Schema Manager] 20150327

      case OB_GET_UPS_MEMORY:
      case OB_GET_UPS_TMPLOG_REPLAY_FINISHED:
      case OB_UPS_GET_MEM_TABLE_INFO:
        ps = read_thread_queue_.push(req, read_task_queue_size_, false,
                                     (NORMAL_PRI == priority)
                                     ? PriorityPacketQueueThread::NORMAL_PRIV
                                     : PriorityPacketQueueThread::LOW_PRIV);
        break;
      //del peiouya [MultiUPS] [DELETE_OBI] 20150701:b
      //case OB_SLAVE_QUIT:
      //del 20150701:e
      case OB_UPS_GET_SLAVE_INFO:
        if (ObUpsRoleMgr::MASTER != role_mgr_.get_role())
        {
          YYSYS_LOG(WARN, "server is not master, refuse to get slave info");
          response_result_(OB_NOT_MASTER, OB_UPS_GET_SLAVE_INFO_RESPONSE, 1, packet->get_request(), packet->get_channel_id());
          rc = OB_ERROR;
        }
        else
        {
          ps = read_thread_queue_.push(req, read_task_queue_size_, false,
                                       (NORMAL_PRI == priority)
                                       ? PriorityPacketQueueThread::NORMAL_PRIV
                                       : PriorityPacketQueueThread::LOW_PRIV);
        }
        break;
        //master send to slave, to keep alive
      case OB_UPS_KEEP_ALIVE:
        YYSYS_LOG(WARN, "not need receive keep_alive anymore");
        ps = false;
        break;
      case OB_RS_UPS_REVOKE_LEASE:
        if (ObUpsRoleMgr::MASTER == role_mgr_.get_role())
        {
          ps = lease_thread_queue_.push(req, lease_task_queue_size_, false);
        }
        else
        {
          YYSYS_LOG(WARN, "server not master, refuse to revoke lease.");
          response_result_(OB_NOT_MASTER, OB_RENEW_LEASE_RESPONSE, MY_VERSION, packet->get_request(), packet->get_channel_id());
          rc = OB_ERROR;
        }
        break;
      case OB_DIRECT_PING:
        response_result_(OB_SUCCESS, OB_DIRECT_PING_RESPONSE, MY_VERSION, packet->get_request(), packet->get_channel_id());
        ps = true;
        static CountReporter ping_count_reporter("direct_ping", 100000);
        ping_count_reporter.inc();
        break;
      case OB_SET_SYNC_LIMIT_REQUEST:
      case OB_PING_REQUEST:
        //case OB_UPS_CHANGE_VIP_REQUEST:
        if (ObUpsRoleMgr::MASTER == role_mgr_.get_role())
        {
          ps = lease_thread_queue_.push(req, lease_task_queue_size_, false);
        }
        else if (ObUpsRoleMgr::SLAVE == role_mgr_.get_role())
        {
          ps = lease_thread_queue_.push(req, log_task_queue_size_, false);
        }
        break;
      case OB_RS_UPS_HEARTBEAT:
        if (ObUpsRoleMgr::FATAL != role_mgr_.get_state())
        {
          ps = lease_thread_queue_.push(req, lease_task_queue_size_, false);
        }
        else
        {
          YYSYS_LOG(DEBUG, "UPS become FATAL, refuse to receive heartbeat from rs");
        }
        break;
      case OB_CHANGE_LOG_LEVEL:
        ps = read_thread_queue_.push(req, read_task_queue_size_, false);
        break;
      case OB_STOP_SERVER:
        ps = read_thread_queue_.push(req, read_task_queue_size_, false);
        break;
      case OB_MALLOC_STRESS:
        ps = read_thread_queue_.push(req, read_task_queue_size_, false);
        break;
      default:
        if (NULL != req->get_request()
            && NULL != req->get_request()->ms
            && NULL != req->get_request()->ms->c)
        {
          YYSYS_LOG(ERROR, "UNKNOWN packet %d src=%s, ignore this", packet_code,
                    get_peer_ip(req->get_request()));
        }
        else
        {
          YYSYS_LOG(ERROR, "UNKNOWN packet %d from UNKNOWN src, ignore this", packet_code);
        }
        break;
      }
      if (!ps)
      {
        YYSYS_LOG(WARN, "can not push packet(pcode is %u) to packet queue", req->get_packet_code());
        rc = OB_ENQUEUE_FAILED;
      }
      return rc;
    }

    int ObUpdateServer::handleBatchPacket(ObPacketQueue &packetQueue)
    {
      UNUSED(packetQueue);
      YYSYS_LOG(ERROR, "you should not reach this, not supported");
      return OB_SUCCESS;
    }

    bool ObUpdateServer::handlePacketQueue(ObPacket *packet, void *args)
    {
      UNUSED(args);
      bool ret = true;
      int return_code = OB_SUCCESS;

      ObPacket* ob_packet = packet;
      int packet_code = ob_packet->get_packet_code();
      int version = ob_packet->get_api_version();
      int32_t priority = ob_packet->get_packet_priority();
      return_code = ob_packet->deserialize();
      uint32_t channel_id = ob_packet->get_channel_id();//yynet need this
      if (OB_SUCCESS != return_code)
      {
        YYSYS_LOG(ERROR, "packet deserialize error packet code is %d", packet_code);
      }
      else
      {
        int64_t packet_timewait = ob_packet->get_source_timeout() ?: config_.packet_max_wait_time;
        ObDataBuffer* in_buf = ob_packet->get_buffer();
        in_buf->get_limit() = in_buf->get_position() + ob_packet->get_data_length();
        if ((ob_packet->get_receive_ts() + packet_timewait) < yysys::CTimeUtil::getTime())
        {
          OB_STAT_INC(UPDATESERVER, UPS_STAT_PACKET_LONG_WAIT_COUNT, 1);
          YYSYS_LOG(WARN, "packet wait too long time, receive_time=%ld cur_time=%ld packet_max_timewait=%ld packet_code=%d "
              "priority=%d last_log_network_elapse=%ld last_log_disk_elapse=%ld "
              "read_task_queue_size=%zu write_task_queue_size=%zu lease_task_queue_size=%zu",
              ob_packet->get_receive_ts(), yysys::CTimeUtil::getTime(), packet_timewait, packet_code, priority,
              log_mgr_.get_last_net_elapse(), log_mgr_.get_last_disk_elapse(),
              read_thread_queue_.size(), write_thread_queue_.size(), lease_thread_queue_.size());
        }
        else if (in_buf == NULL)
        {
          YYSYS_LOG(ERROR, "in_buff is NUll should not reach this");
        }
        else
        {
          packet_timewait -= DEFAULT_REQUEST_TIMEOUT_RESERVE;
          easy_request_t* req = ob_packet->get_request();
          if (OB_SELF_FLAG != ob_packet->get_target_id() &&
            (NULL == req || NULL == req->ms || NULL == req->ms->c))
          {
            YYSYS_LOG(ERROR, "req or req->ms or req->ms->c is NULL, shoule not reach here");
          }
          else
          {
            ThreadSpecificBuffer::Buffer* my_buffer = my_thread_buffer_.get_buffer();
            if (my_buffer != NULL)
            {
              my_buffer->reset();
              ObDataBuffer thread_buff(my_buffer->current(), my_buffer->remain());
              YYSYS_LOG(DEBUG, "handle packet, packe code is %d", packet_code);
              switch(packet_code)
              {
              //del peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//              case OB_SET_OBI_ROLE:
//                return_code = set_obi_role(version, *in_buf, req, channel_id, thread_buff);
//                break;
              //del 20150701:e
              case OB_FETCH_LOG:
                return_code = ups_fetch_log_for_slave(version, *in_buf, req, channel_id, thread_buff, packet);
                break;
              case OB_FILL_LOG_CURSOR:
                return_code = ups_fill_log_cursor_for_slave(version, *in_buf, req, channel_id, thread_buff);
                break;
              case OB_GET_CLOG_STATUS:
                return_code = ups_get_clog_status(version, *in_buf, req, channel_id, thread_buff);
                break;
                //add lbzhong [Clog Monitor] 20151218:b
              case OB_CLOG_MONITOR_GET_UPS_LIST:
                return_code = clog_monitor_get_ups_list(version, *in_buf, req, channel_id, thread_buff);
                break;
              case OB_CLOG_MONITOR_GET_CLOG_STAT:
                return_code = clog_monitor_get_clog_status(version, *in_buf, req, channel_id, thread_buff);
                break;
                //add:e
              case OB_SEND_LOG:
                return_code = ups_slave_write_log(version, *in_buf, req, channel_id,
                //add chujiajia [Paxos ups_replication] 20160107:b
                packet->get_cmt_log_seq(),
                //add:e
                thread_buff);
                break;
              case OB_SET_SYNC_LIMIT_REQUEST:
                return_code = ups_set_sync_limit(version, *in_buf, req, channel_id, thread_buff);
                break;
              case OB_PING_REQUEST:
                return_code = ups_ping(version, req, channel_id);
                break;
              case OB_LOGIN:
                return_code = ob_login(version, *in_buf, req, channel_id, thread_buff);
                break;
              case OB_UPS_ASYNC_SWITCH_SKEY:
                return_code = ups_switch_skey();
                break;
              case OB_GET_CLOG_CURSOR:
                return_code = ups_get_clog_cursor(version, req, channel_id, thread_buff);
                break;
              case OB_GET_CLOG_MASTER:
                return_code = ups_get_clog_master(version, req, channel_id, thread_buff);
                break;
              case OB_GET_CLOG_STAT:
                return_code = ups_get_clog_stat(version, req, channel_id, thread_buff);
                break;
              case OB_SQL_SCAN_REQUEST:
                return_code = ups_sql_scan(version, *in_buf, req, channel_id, thread_buff);
                break;
              case OB_GET_LOG_SYNC_DELAY_STAT:
                return_code = ups_get_log_sync_delay_stat(version, req, channel_id, thread_buff);
                break;
              case OB_NEW_GET_REQUEST:
                return_code = ups_new_get(version, *in_buf, req, channel_id, thread_buff, ob_packet->get_receive_ts(), packet_timewait, priority);
                break;
              case OB_GET_REQUEST:
                CLEAR_TRACE_LOG();
                FILL_TRACE_LOG("start handle get, packet wait=%ld start_time=%ld timeout=%ld src=%s priority=%d",
                               yysys::CTimeUtil::getTime() - ob_packet->get_receive_ts(),
                               ob_packet->get_receive_ts(), packet_timewait, NULL == req ? NULL :
                               get_peer_ip(ob_packet->get_request()), priority);
                return_code = ups_get(version, *in_buf, req, channel_id, thread_buff, ob_packet->get_receive_ts(), packet_timewait, priority);
                break;
              case OB_NEW_SCAN_REQUEST:
                CLEAR_TRACE_LOG();
                return_code = ups_new_scan(version, *in_buf, req, channel_id, thread_buff, ob_packet->get_receive_ts(), packet_timewait, priority);
                break;
              case OB_SCAN_REQUEST:
                CLEAR_TRACE_LOG();
                FILL_TRACE_LOG("start handle scan, packet wait=%ld start_time=%ld timeout=%ld src=%s priority=%d",
                               yysys::CTimeUtil::getTime() - ob_packet->get_receive_ts(),
                               ob_packet->get_receive_ts(), packet_timewait, NULL == req ? NULL :
                               get_peer_ip(ob_packet->get_request()), priority);
                return_code = ups_scan(version, *in_buf, req, channel_id, thread_buff, ob_packet->get_receive_ts(), packet_timewait, priority);
                break;
              case OB_WRITE:
              case OB_MS_MUTATE:
              case OB_PHY_PLAN_EXECUTE:
              case OB_WRITE_TRANS_STAT:  //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701
                CLEAR_TRACE_LOG();
                FILL_TRACE_LOG("start preprocess write, packet wait=%ld, start_time=%ld, timeout=%ld, src=%s, packet_code=%d",
                               yysys::CTimeUtil::getTime() - ob_packet->get_receive_ts(),
                               ob_packet->get_receive_ts(), packet_timewait, NULL == req ? NULL :
                               get_peer_ip(ob_packet->get_request()), packet_code);
                return_code = ups_preprocess(version, packet_code, *in_buf, req, channel_id, thread_buff, ob_packet->get_receive_ts(), packet_timewait);

                break;
              case OB_UPS_GET_BLOOM_FILTER:
                return_code = ups_get_bloomfilter(version, *in_buf, req, channel_id, thread_buff);
                break;
              case OB_CREATE_MEMTABLE_INDEX:
                return_code = ups_create_memtable_index();
                break;
              case OB_UPS_DUMP_TEXT_MEMTABLE:
                return_code = ups_dump_text_memtable(version, *in_buf, req, channel_id);
                break;
              case OB_UPS_DUMP_TEXT_SCHEMAS:
                return_code = ups_dump_text_schemas(version, req, channel_id);
                break;
              case OB_UPS_MEMORY_WATCH:
                return_code = ups_memory_watch(version, req, channel_id, thread_buff);
                break;
              case OB_UPS_MEMORY_LIMIT_SET:
                return_code = ups_memory_limit_set(version, *in_buf, req, channel_id, thread_buff);
                break;
              case OB_UPS_PRIV_QUEUE_CONF_SET:
                return_code = ups_priv_queue_conf_set(version, *in_buf, req, channel_id, thread_buff);
                break;
              case OB_UPS_RELOAD_CONF:
                return_code = ups_reload_conf(version, req, channel_id);
                break;
              //del peiouya [MultiUPS] [DELETE_OBI] 20150701:b
              //case OB_SLAVE_QUIT:
              //  return_code = ups_slave_quit(version, *in_buf, req, channel_id, thread_buff);
              //  break;
              //del 20150701:e
                //case OB_RENEW_LEASE_REQUEST:
                //  return_code = ups_renew_lease(version, *in_buf, req, channel_id, thread_buff);
                //  break;
                //case OB_GRANT_LEASE_REQUEST:
                //  return_code = ups_grant_lease(version, *in_buf, req, channel_id, thread_buff);
                //  break;
                //add ��������������
              case OB_RS_UPS_HEARTBEAT:
                return_code = ups_rs_lease(version, *in_buf, req, channel_id, thread_buff);
                break;
              case OB_RS_UPS_REVOKE_LEASE:
                return_code = ups_rs_revoke_lease(version, *in_buf, req, channel_id, thread_buff);
                break;
              case OB_UPS_GET_LAST_FROZEN_VERSION:
                return_code = ups_get_last_frozen_version(version, req, channel_id, thread_buff);
                break;
              case OB_UPS_GET_TABLE_TIME_STAMP:
                return_code = ups_get_table_time_stamp(version, *in_buf, req, channel_id, thread_buff);
                break;
              case OB_UPS_GET_SLAVE_INFO:
                return_code = ups_get_slave_info(version, req, channel_id, thread_buff);
                break;
              case OB_UPS_ENABLE_MEMTABLE_CHECKSUM:
                return_code = ups_enable_memtable_checksum(version, req, channel_id);
                break;
              case OB_UPS_DISABLE_MEMTABLE_CHECKSUM:
                return_code = ups_disable_memtable_checksum(version, req, channel_id);
                break;
              case OB_FETCH_STATS:
                return_code = ups_fetch_stat_info(version, req, channel_id, thread_buff);
                break;
              //add zhaoqiong [Truncate Table]:20160318:b
              case OB_CHECK_INCREMENTAL_RANGE:
                return_code = ups_check_incremental_data_range(version, *in_buf, req, channel_id, thread_buff);
                break;
              //add:e
              case OB_FETCH_SCHEMA:
                return_code = ups_get_schema(version, *in_buf, req, channel_id, thread_buff);
                break;
              //add zhaoqiong [Schema Manager] 20150327:b
              case OB_FETCH_SCHEMA_NEXT:
                return_code = ups_get_schema_next(version, *in_buf, req, channel_id, thread_buff);
                break;
              //add :e
               //add wenghaixing [secondary index.cluster]20150630
              case OB_FETCH_INIT_INDEX:
                return_code = ups_get_init_index(version, *in_buf, req, channel_id, thread_buff);
                break;
              //add e
              case OB_RS_FETCH_SPLIT_RANGE:
                return_code = ups_get_sstable_range_list(version, *in_buf, req, channel_id, thread_buff);
                break;
                /* case OB_UPS_CHANGE_VIP_REQUEST:
                   return_code = ups_change_vip(version, *in_buf, req, channel_id);
                   break; */
              case OB_UPS_STORE_MEM_TABLE:
                return_code = ups_store_memtable(version, *in_buf, req, channel_id);
                break;
              case OB_UPS_DROP_MEM_TABLE:
                return_code = ups_drop_memtable(version, req, channel_id);
                break;
              case OB_UPS_DELAY_DROP_MEMTABLE:
                return_code = ups_delay_drop_memtable(version, req, channel_id);
                break;
              case OB_UPS_IMMEDIATELY_DROP_MEMTABLE:
                return_code = ups_immediately_drop_memtable(version, req, channel_id);
                break;
              case OB_UPS_ASYNC_FORCE_DROP_MEMTABLE:
                return_code = ups_drop_memtable();
                break;

              case OB_UPS_GET_MEM_TABLE_INFO:
                  return_code = ups_get_memtable_info(version, req, channel_id,thread_buff);
                  break;
              case OB_UPS_GET_COMMIT_LOG_STAT:
                  return_code = ups_get_commit_log_status(version, *in_buf, req, channel_id, thread_buff);
                  break;

              case OB_UPS_ASYNC_LOAD_BYPASS:
                return_code = ups_load_bypass(version, req, channel_id, thread_buff, packet_code);
                break;
              case OB_UPS_ERASE_SSTABLE:
                return_code = ups_erase_sstable(version, req, channel_id);
                break;
              case OB_UPS_ASYNC_HANDLE_FROZEN:
                return_code = ups_handle_frozen();
                break;
              //del peiouya [MultiUPS] [UPS_SYNC_Frozen] 20150528:b
              //case OB_UPS_ASYNC_REPORT_FREEZE:
              //  return_code = report_frozen_version_();
              //  break;
              //del 20150528:e
              case OB_UPS_ASYNC_REPLAY_LOG:
                return_code = replay_commit_log_();
                break;
              //add wangjiahao [Paxos ups_replication_tmplog]
              case OB_UPS_ASYNC_REPLAY_TMP_LOG:
                return_code = replay_tmp_log_();
                break;
              //add :e
              case OB_PREFETCH_LOG:
                return_code = prefetch_remote_log_(*in_buf);
                break;
              case OB_UPS_ASYNC_CHECK_KEEP_ALIVE:
                return_code = check_keep_alive_();
                break;
              //add peiouya [MultiUPS] [DELETE_OBI] 20150701:b
              case OB_UPS_ASYNC_GET_SYS_UPS_MASTER:
                return_code = get_sys_ups_master_();
                break;
              //add 20150701:e
              case OB_UPS_ASYNC_GRANT_KEEP_ALIVE:
                return_code = grant_keep_alive_();
                break;
              case OB_UPS_ASYNC_CHECK_LEASE:
                return_code = check_lease_();
                break;
              case OB_UPS_LOAD_NEW_STORE:
                return_code = ups_load_new_store(version, req, channel_id);
                break;
              case OB_UPS_RELOAD_ALL_STORE:
                return_code = ups_reload_all_store(version, req, channel_id);
                break;
              case OB_UPS_RELOAD_STORE:
                return_code = ups_reload_store(version, *in_buf, req, channel_id);
                break;
              case OB_UPS_UMOUNT_STORE:
                return_code = ups_umount_store(version, *in_buf, req, channel_id);
                break;
              case OB_UPS_FORCE_REPORT_FROZEN_VERSION:
                return_code = ups_froce_report_frozen_version(version, req, channel_id);
                break;
              case OB_UPS_KEEP_ALIVE:
                return_code = slave_ups_receive_keep_alive(version, req, channel_id);
                break;
              case OB_UPS_CLEAR_FATAL_STATUS:
                return_code = ups_clear_fatal_status(version, req, channel_id);
                break;
              case OB_RS_GET_MAX_LOG_SEQ:
                return_code = ups_rs_get_max_log_seq(version, req, channel_id, thread_buff);
                break;

              case OB_GET_UPS_MEMORY:
                  return_code = ups_rs_get_memory(version, *in_buf, req, channel_id, thread_buff);
                  break;
              case OB_GET_UPS_TMPLOG_REPLAY_FINISHED:
                  return_code = get_ups_tmplog_replay_finished(version, *in_buf, req, channel_id, thread_buff);
                  break;

              //add pangtianze [Paxos ups_replication] 20150604:b
              case OB_RS_GET_MAX_LOG_SEQ_AND_TERM:
                return_code = ups_rs_get_max_log_seq_and_term(version, req, channel_id, thread_buff);
                break;
              //add:e
              //add pangtianze [Paxos rs_election] 20160919:b
              case OB_RS_GET_UPS_ROLE:
                return_code = ups_rs_get_ups_role(version, req, channel_id, thread_buff);
                break;
              //add:e
              //add pangtianze [Paxos rs_election] 20170228:b
              case OB_REFRESH_RS_LIST:
                return_code = ups_refresh_rs_list(version, *in_buf, req, channel_id);
                break;
              //add:e

              case OB_SLAVE_REG:
                  return_code = ups_slave_register(version, *in_buf, req, channel_id, thread_buff);
                  break;

              case OB_CHANGE_LOG_LEVEL:
                return_code = ups_change_log_level(version, *in_buf, req, channel_id);
                break;
              case OB_STOP_SERVER:
                return_code = ups_stop_server(version, *in_buf, req, channel_id);
                break;
              case OB_SET_CONFIG:
                return_code = ups_set_config(version, *in_buf, req, channel_id);
                break;
              case OB_GET_CONFIG:
                return_code = ups_get_config(version, req, channel_id, thread_buff);
                break;
              case OB_UPS_ASYNC_UPDATE_SCHEMA:
                //mod zhaoqiong [Schema Manager] 20150327:b
                //return_code = do_async_update_whole_schema();
                return_code = do_async_update_schema();
                //mod:e
                break;
              case OB_MALLOC_STRESS:
                return_code = ob_malloc_stress(version, *in_buf, req, channel_id);
                break;
              default:
                return_code = OB_ERROR;
                break;
              }

              if (OB_SUCCESS != return_code)
              {
                YYSYS_LOG(WARN, "call func error packet_code is %d return code is %d", packet_code, return_code);
              }
            }
            else
            {
              YYSYS_LOG(ERROR, "get thread buffer error, ignore this packet");
            }
          }
        }
      }
      return ret;//if return true packet will be deleted.
    }

    int ObUpdateServer::ups_preprocess(const int32_t version, const int32_t packet_code, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff,
        const int64_t start_time, const int64_t timeout)
    {
      UNUSED(out_buff);

      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }

      ObMutator *mutator_ptr = GET_TSI_MULT(ObMutator, TSI_UPS_MUTATOR_1);
      ObToken token;
      ObToken *token_ptr = NULL;
      easy_addr_t addr = get_easy_addr(req);
      if (NULL == mutator_ptr)
      {
        YYSYS_LOG(WARN, "GET_TSI ObMutator or ObScanner fail");
        ret = OB_ERROR;
      }

      ObDataBuffer ori_in_buff = in_buff;
      if (OB_SUCCESS == ret)
      {
        UpsSchemaMgrGuard guard;
        const ObSchemaManagerV2* schema_mgr = table_mgr_.get_schema_mgr().get_schema_mgr(guard);
        if (NULL == schema_mgr)
        {
          YYSYS_LOG(WARN, "failed to get schema");
          ret = OB_SCHEMA_ERROR;
        }
        else
        {
          mutator_ptr->set_compatible_schema(schema_mgr);
        }
        ret = mutator_ptr->deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        FILL_TRACE_LOG("mutator deserialize ret=%d buff_size=%d buff_pos=%d", ret, in_buff.get_capacity(), in_buff.get_position());
      }

      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "deserialize token or mutator fail ret=%d", ret);
      }
      else
      {
        ret = table_mgr_.pre_process(OB_MS_MUTATE == packet_code, *mutator_ptr, token_ptr);
        if (OB_SUCCESS == ret)
        {
          int64_t apply_timeout = timeout - (yysys::CTimeUtil::getTime() - start_time);
          ThreadSpecificBuffer::Buffer *my_buffer = my_thread_buffer_.get_buffer();
          if (apply_timeout <= 0)
          {
            YYSYS_LOG(WARN, "response timeout, timeout=%ld, start_time=%ld, now=%ld",
                timeout, start_time, yysys::CTimeUtil::getTime());
            ret = OB_RESPONSE_TIME_OUT;
          }
          else if (NULL == my_buffer)
          {
            YYSYS_LOG(ERROR, "alloc thread buffer fail");
            ret = OB_MEM_OVERFLOW;
          }
          else
          {
            ObDataBuffer mutator_buff(my_buffer->current(), my_buffer->remain());
            if (OB_SUCCESS != (ret = mutator_ptr->serialize(mutator_buff.get_data(), mutator_buff.get_capacity(),
                                                            mutator_buff.get_position())))
            {
              YYSYS_LOG(WARN, "failed to serialize mutator, ret=%d", ret);
            }
            else if (OB_SUCCESS != (ret = submit_async_task_((PacketCode)OB_MS_MUTATE, write_thread_queue_, write_task_queue_size_,
                                                            version, mutator_buff, req, channel_id, apply_timeout)))
            {
              YYSYS_LOG(WARN, "failed to add apply task, ret=%d", ret);
            }
          }
        }
      }

      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "failed to pre process, ret=%d src=%s", ret,
                  inet_ntoa_r(addr));
        int res_code = (OB_MS_MUTATE == packet_code) ? OB_MS_MUTATE_RESPONSE : OB_WRITE_RES;
        int tmp_ret = response_result_(ret, res_code, MY_VERSION, req, channel_id);
        if (OB_SUCCESS != tmp_ret)
        {
          YYSYS_LOG(WARN, "failed to send response, tmp_ret=%d", tmp_ret);
          ret = OB_ERROR;
        }
        // TODO (rizhao.ych) modify stat info
      }
      FILL_TRACE_LOG("preprocess ret=%d src=%s", ret,
                     inet_ntoa_r(addr));
      PRINT_TRACE_LOG();

      return ret;
    }

    bool ObUpdateServer::handleBatchPacketQueue(const int64_t batch_num, ObPacket** packets, void *args)
    {
      UNUSED(args);
      enum __trans_status__
      {
        TRANS_NOT_START = 0,
        TRANS_STARTED = 1,
      };
      bool ret = true;
      int return_code = OB_SUCCESS;
      /*
      int64_t trans_status = TRANS_NOT_START;
      int64_t first_trans_idx = 0;
      */
      UpsTableMgrTransHandle handle;
      ObPacket packet_repl;
      ObPacket* ob_packet = &packet_repl;
      ScannerArray *scanner_array = GET_TSI_MULT(ScannerArray, TSI_UPS_SCANNER_ARRAY_1);
      //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
      // �ж��Ƿ���master_master��Ȼ�з��� master slave -> slave master
//      bool is_master_master = ObUpsRoleMgr::MASTER == role_mgr_.get_role() && ObiRole::MASTER == obi_role_.get_role();
//      if (is_master_master && !is_lease_valid())
//      {
//        return_code = OB_NOT_MASTER;
//        YYSYS_LOG(WARN, "is master_master But lease is invalid");
//      }
      bool is_master =  ObUpsRoleMgr::MASTER == role_mgr_.get_role();
      if (is_master && !is_lease_valid())
      {
        return_code = OB_NOT_MASTER;
        //mod peiouya [MultiUPS] [DELETE_OBI] 20150810:b
        //note: CDI_multiUPS_duht.xls:sheet 1
        //expr:replace "master_master" with "master"
        //YYSYS_LOG(WARN, "is master_master But lease is invalid");
        YYSYS_LOG(WARN, "is master But lease is invalid");
        //mod 20150810:e
      }
      if (NULL == scanner_array)
      {
        YYSYS_LOG(WARN, "get tsi scanner_array fail");
        return_code = OB_ERROR;
      }
      for (int64_t i = 0; OB_SUCCESS == return_code && i < batch_num; ++i)
      {
        ObPacket* packet_orig = packets[i];
        packet_repl = *packet_orig;
        int packet_code = ob_packet->get_packet_code();
        /*
        int version = ob_packet->get_api_version();
        */
        return_code = ob_packet->deserialize();
        /*
        uint32_t channel_id = ob_packet->get_channel_id();//yynet need this
        */
        //YYSYS_LOG(DEBUG, "packet i=%ld batch_num=%ld %s", i, batch_num, ob_packet->print_self());
        if (OB_SUCCESS != return_code)
        {
          YYSYS_LOG(ERROR, "packet deserialize error packet code is %d", packet_code);
        }
        else
        {
          trans_executor_.handle_packet(*ob_packet);
          /*
          int64_t packet_timewait = (0 == ob_packet->get_source_timeout()) ?
            config_.get_packet_max_timewait() : ob_packet->get_source_timeout();
          ObDataBuffer* in_buf = ob_packet->get_buffer();
          in_buf->get_limit() = in_buf->get_position() + ob_packet->get_data_length();
          if ((ob_packet->get_receive_ts() + packet_timewait)< yysys::CTimeUtil::getTime())
          {
            OB_STAT_INC(UPDATESERVER, UPS_STAT_PACKET_LONG_WAIT_COUNT, 1);
            YYSYS_LOG(WARN, "packet wait too long time, receive_time=%ld cur_time=%ld packet_max_timewait=%ld packet_code=%d "
                "read_task_queue_size=%zu write_task_queue_size=%zu lease_task_queue_size=%zu log_task_queue_size=%zu",
                ob_packet->get_receive_ts(), yysys::CTimeUtil::getTime(), packet_timewait, packet_code,
                read_thread_queue_.size(), write_thread_queue_.size(), lease_thread_queue_.size(), log_thread_queue_.size());
            return_code = OB_RESPONSE_TIME_OUT;
          }
          else if (in_buf == NULL)
          {
            YYSYS_LOG(ERROR, "in_buff is NUll should not reach this");
            return_code = OB_ERROR;
          }
          else
          {
            easy_request_t* req = ob_packet->get_request();
            if (OB_SELF_FLAG != ob_packet->get_target_id() &&
                (NULL == req || NULL == req->ms || NULL == req->ms->c))
            {
              YYSYS_LOG(ERROR, "req or req->ms or req->ms->c is NUll should not reach this");
              return_code = OB_ERROR;
            }
            else
            {
              ThreadSpecificBuffer::Buffer* my_buffer = my_thread_buffer_.get_buffer();
              if (my_buffer == NULL)
              {
                YYSYS_LOG(ERROR, "get thread buffer error, ignore this packet");
                return_code = OB_ERROR;
              }
              else
              {
                my_buffer->reset();
                ObDataBuffer thread_buff(my_buffer->current(), my_buffer->remain());
                YYSYS_LOG(DEBUG, "handle packet, packe code is %d", packet_code);
                if (!is_master_master)
                {
                  if (OB_SEND_LOG == packet_code)
                  {
                    return_code = ups_slave_write_log(version, *in_buf, req, channel_id, thread_buff);
                  }
                  else if (OB_SLAVE_REG == packet_code)
                  {
                    return_code = ups_slave_register(version, *in_buf, req, channel_id, thread_buff);
                  }
                  else
                  {
                    return_code = return_not_master(version, req, channel_id, packet_code);
                  }
                }
                else
                {
                  switch (packet_code)
                  {
                  case OB_FREEZE_MEM_TABLE:
                  case OB_UPS_MINOR_FREEZE_MEMTABLE:
                  case OB_UPS_MINOR_LOAD_BYPASS:
                  case OB_UPS_MAJOR_LOAD_BYPASS:
                  case OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE:
                  case OB_UPS_ASYNC_AUTO_FREEZE_MEMTABLE:
                  case OB_SLAVE_REG:
                  case OB_UPS_CLEAR_ACTIVE_MEMTABLE:
                  case OB_SWITCH_SCHEMA:
                  case OB_UPS_FORCE_FETCH_SCHEMA:
                  case OB_UPS_SWITCH_COMMIT_LOG:
                  case OB_UPS_ASYNC_CHECK_CUR_VERSION:
                  case OB_UPS_ASYNC_WRITE_SCHEMA:
                  case OB_FAKE_WRITE_FOR_KEEP_ALIVE:
                    if (TRANS_STARTED == trans_status)
                    {
                      return_code = ups_end_transaction(packets, *scanner_array, first_trans_idx, i-1, handle, OB_SUCCESS);
                      trans_status = TRANS_NOT_START;
                      if (OB_SUCCESS != return_code)
                      {
                        YYSYS_LOG(WARN, "failed to end transaction, err=%d", return_code);
                        break;
                      }
                    }
                    break;
                  case OB_SEND_LOG:
                  case OB_WRITE:
                  case OB_INTERNAL_WRITE:
                  case OB_MS_MUTATE:
                    break;
                  default:
                    YYSYS_LOG(WARN, "unexpected packet_code %d", packet_code);
                    return_code = OB_ERR_UNEXPECTED;
                    break;
                  }
                  if (OB_SUCCESS == return_code)
                  {
                    switch (packet_code)
                    {
                    case OB_FREEZE_MEM_TABLE:
                    case OB_UPS_MINOR_FREEZE_MEMTABLE:
                    case OB_UPS_MINOR_LOAD_BYPASS:
                    case OB_UPS_MAJOR_LOAD_BYPASS:
                    case OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE:
                    case OB_UPS_ASYNC_AUTO_FREEZE_MEMTABLE:
                      return_code = ups_freeze_memtable(version, packet_orig, thread_buff, packet_code);
                      break;
                    case OB_SWITCH_SCHEMA:
                      return_code = ups_switch_schema(version, packet_orig, *in_buf);
                      break;
                    case OB_UPS_FORCE_FETCH_SCHEMA:
                      return_code = ups_force_fetch_schema(version, req, channel_id);
                      break;
                    case OB_UPS_SWITCH_COMMIT_LOG:
                      return_code = ups_switch_commit_log(version, req, channel_id, thread_buff);
                      break;
                    case OB_SLAVE_REG:
                      return_code = ups_slave_register(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    case OB_UPS_CLEAR_ACTIVE_MEMTABLE:
                      return_code = ups_clear_active_memtable(version, req, channel_id);
                      break;
                    case OB_UPS_ASYNC_CHECK_CUR_VERSION:
                      return_code = ups_check_cur_version();
                      break;
                    case OB_FAKE_WRITE_FOR_KEEP_ALIVE:
                      return_code = ups_handle_fake_write_for_keep_alive();
                      break;
                    case OB_WRITE:
                    case OB_INTERNAL_WRITE:
                    case OB_MS_MUTATE:
                      if (TRANS_NOT_START == trans_status)
                      {
                        return_code = ups_start_transaction(WRITE_TRANSACTION, handle);
                        if (OB_SUCCESS != return_code)
                        {
                          YYSYS_LOG(ERROR, "failed to start transaction, err=%d", return_code);
                        }
                        else
                        {
                          trans_status = TRANS_STARTED;
                          first_trans_idx = i;
                        }
                      }

                      if (OB_SUCCESS == return_code)
                      {
                        return_code = ups_apply(OB_MS_MUTATE == packet_code, handle, *in_buf, (*scanner_array)[i]);
                        if (OB_EAGAIN == return_code)
                        {
                          if (first_trans_idx < i)
                          {
                            YYSYS_LOG(INFO, "exceeds memory limit, should retry");
                          }
                          else
                          {
                            YYSYS_LOG(WARN, "mutator too large more than log_buffer, cannot apply");
                            return_code = OB_BUF_NOT_ENOUGH;
                          }
                        }
                        else if (OB_SUCCESS != return_code)
                        {
                          if (OB_COND_CHECK_FAIL != return_code)
                          {
                            YYSYS_LOG(WARN, "failed to apply mutation, err=%d", return_code);
                          }
                        }
                        FILL_TRACE_LOG("ups_apply src=%s ret=%d batch_num=%ld cur_trans_idx=%ld last_trans_idx=%ld",
                                       inet_ntoa_r(convert_addr_to_server(req->ms->c->addr)),
                                       return_code, batch_num, first_trans_idx, i);
                      }

                      if (OB_EAGAIN == return_code)
                      {
                        return_code = ups_end_transaction(packets, *scanner_array, first_trans_idx, i-1, handle, OB_SUCCESS);
                        --i; // re-execute the last mutation
                        trans_status = TRANS_NOT_START;
                        if (OB_SUCCESS != return_code)
                        {
                          YYSYS_LOG(WARN, "failed to end transaction, err=%d", return_code);
                          break;
                        }
                      }
                      else if (OB_SUCCESS != return_code)
                      {
                        return_code = ups_end_transaction(packets, *scanner_array, first_trans_idx, i, handle, return_code);
                        trans_status = TRANS_NOT_START;
                        if (OB_SUCCESS != return_code)
                        {
                          YYSYS_LOG(WARN, "failed to end transaction, err=%d", return_code);
                          break;
                        }
                      }
                      else if (i == batch_num - 1)
                      {
                        return_code = ups_end_transaction(packets, *scanner_array, first_trans_idx, i, handle, OB_SUCCESS);
                        trans_status = TRANS_NOT_START;
                        if (OB_SUCCESS != return_code)
                        {
                          YYSYS_LOG(WARN, "failed to end transaction, err=%d", return_code);
                          break;
                        }
                      }
                      break;

                    case OB_SEND_LOG:
                      return_code = ups_slave_write_log(version, *in_buf, req, channel_id, thread_buff);
                      break;
                    default:
                      YYSYS_LOG(WARN, "unexpected packet_code %d", packet_code);
                      return_code = OB_ERR_UNEXPECTED;
                      break;
                    }
                  }
                }

                if (OB_SUCCESS != return_code)
                {
                  YYSYS_LOG(WARN, "call func error packet_code is %d return code is %d", packet_code, return_code);
                }
              }
            }
          }
          */
        }
        /*
        // do something after every loop
        if (OB_SUCCESS != return_code)
        {
          if (TRANS_STARTED == trans_status)
          {
            return_code = ups_end_transaction(packets, *scanner_array, first_trans_idx, i, handle, return_code);
            trans_status = TRANS_NOT_START;
            if (OB_SUCCESS != return_code)
            {
              YYSYS_LOG(WARN, "failed to end transaction, err=%d", return_code);
              return_code = OB_SUCCESS;
            }
          }
          else
          {
            return_code = OB_SUCCESS;
          }
        }
        */
      }
      /*
      packet_repl.get_buffer()->reset();

      if (ObiRole::MASTER == obi_role_.get_role()
          && ObUpsRoleMgr::MASTER == role_mgr_.get_role()
          && ObUpsRoleMgr::ACTIVE == role_mgr_.get_state()
          && is_lease_valid())
      {
        TableMgr::FreezeType freeze_type = TableMgr::AUTO_TRIG;
        uint64_t frozen_version = 0;
        bool report_version_changed = false;
        if (OB_SUCCESS == table_mgr_.freeze_memtable(freeze_type, frozen_version, report_version_changed))
        {
          if (report_version_changed)
          {
            submit_report_freeze();
          }
          submit_handle_frozen();
        }
      }
      */

      return ret;//if return true packet will be deleted.
    }

    int ObUpdateServer::ups_handle_fake_write_for_keep_alive()
    {
      int err = OB_SUCCESS;
      int64_t end_log_id = 0;
      //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//      if (!(ObiRole::MASTER == obi_role_.get_role()
//            && ObUpsRoleMgr::MASTER == role_mgr_.get_role()
//            && ObUpsRoleMgr::ACTIVE == role_mgr_.get_state()
//            && is_lease_valid()))
      if (!(ObUpsRoleMgr::MASTER == role_mgr_.get_role()
            && ObUpsRoleMgr::ACTIVE == role_mgr_.get_state()
            && is_lease_valid()))
      //mod 20150701:e
      {
        err = OB_NOT_MASTER;
        YYSYS_LOG(WARN, "not master:is_lease_valid=%s", STR_BOOL(is_lease_valid()));
      }
      else if (OB_SUCCESS != (err = log_mgr_.write_keep_alive_log()))
      {
        YYSYS_LOG(ERROR, "log_mgr.write_keep_alive_log()=>%d", err);
      }
      else if (OB_SUCCESS != (err = log_mgr_.async_flush_log(end_log_id, TraceLog::get_logbuffer())))
      {
        YYSYS_LOG(ERROR, "log_mgr.flush_log()=>%d", err);
      }
      if (OB_SUCCESS != err)
      {
        if(OB_NOT_MASTER != err)
        {
          role_mgr_.set_state(ObUpsRoleMgr::STOP);
          YYSYS_LOG(ERROR, "write keep_alive log fill, err=%d, will kill self", err);
          kill(getpid(), SIGTERM);
        }
        YYSYS_LOG(ERROR, "write_keep_alive_log: err=%d", err);
      }
      return err;
    }

    //mod zhaoqiong [Schema Manager] 20150520:b
    //int ObUpdateServer::do_async_update_whole_schema();
    int ObUpdateServer::do_async_update_whole_schema(bool force_fetch_schema)
    //mod:e
    {
      int err = OB_SUCCESS;
      //fetch schema from system table directly, do not fetch schema from RS any more
      //int64_t num_times = config_.fetch_schema_times;//del zhaoqiong [Schema Manager] 20150421
      //int64_t timeout = config_.fetch_schema_timeout;//del zhaoqiong [Schema Manager] 20150421
      bool only_core_tables = false;
      //CommonSchemaManagerWrapper schema_mgr;//del zhaoqiong [Schema Manager] 20150421
      OnceGuard once_guard(schema_lock_);
      if (!once_guard.try_lock())
      {}
      //mod zhaoqiong [Schema Manager] 20150420:b
//      else
//      {
//        YYSYS_LOG(INFO, "handle_update_schema(%ld->%ld)", table_mgr_.get_schema_mgr().get_version(), schema_version_);
//        err = RPC_CALL_WITH_RETRY(fetch_schema, num_times, timeout, root_server_, 0, schema_mgr, only_core_tables);
//        if (OB_RESPONSE_TIME_OUT == err)
//        {
//          YYSYS_LOG(ERROR, "fetch schema timeout, num_times=%ld, timeout=%ldus",
//                    num_times, timeout);
//          err = OB_RESPONSE_TIME_OUT;
//        }
//        else if (OB_SUCCESS != err)
//        {
//          YYSYS_LOG(WARN, "Error occurs when fetching schema, err=%d", err);
//        }
//        else if (OB_SUCCESS != (err = submit_switch_schema(schema_mgr)))
//        {
//          YYSYS_LOG(ERROR, "submit_switch_schema(version=%ld)=>%d", schema_mgr.get_version(), err);
//        }
//        else
//        {
//          YYSYS_LOG(INFO, "submit swtich_schema(version=%ld)", schema_mgr.get_version());
//        }
      else if (table_mgr_.lock_tmp_schema())
      {
        YYSYS_LOG(INFO, "handle_update_schema(%ld->%ld), get schema from system table", table_mgr_.get_schema_mgr().get_version(), schema_version_);
        CommonSchemaManager &schema_mgr = table_mgr_.get_tmp_schema().get_impl_ref();
        schema_mgr.reset();
        if (force_fetch_schema)
        {
          schema_mgr.set_version(schema_version_ + 1);
          YYSYS_LOG(WARN, "force fetch shchema, set version:%ld", schema_version_ + 1);
        }
        else
        {
          schema_mgr.set_version(schema_version_);
        }
        if (OB_SUCCESS != (err = schema_service_->get_schema(only_core_tables,schema_mgr)))
        {
          YYSYS_LOG(WARN, "Error occurs when get schema, err=%d", err);
        }
        else if (OB_SUCCESS != (err = submit_async_task_(OB_SWITCH_TMP_SCHEMA, write_thread_queue_, write_task_queue_size_)))
        {
          YYSYS_LOG(WARN, "submit_async_task(SWITCH_SCHEMA)=>%d", err);
        }
        else
        {
          YYSYS_LOG(INFO, "submit swtich_schema(version=%ld)", schema_mgr.get_version());
        }

        if (OB_SUCCESS != err)
        {
          if (!table_mgr_.unlock_tmp_schema())
          {
            YYSYS_LOG(ERROR, "unexpected error");
          }
        }
        //mod:e
      }
      return err;
    }

    int ObUpdateServer::return_not_master(const int32_t version, easy_request_t* req,
                                          const uint32_t channel_id, const int32_t packet_code)
    {
      int return_code = OB_SUCCESS;
      UNUSED(version);
      switch (packet_code)
      {
        case OB_FREEZE_MEM_TABLE:
        case OB_UPS_MINOR_FREEZE_MEMTABLE:
        case OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE:
        case OB_UPS_ASYNC_AUTO_FREEZE_MEMTABLE:
        case OB_TRUNCATE_TABLE: //add zhaoqiong [Truncate Table] 20160127
        case OB_SWITCH_SCHEMA:
        //add zhaoqiong [Schema Manager] 20150420:b
        case OB_SWITCH_SCHEMA_MUTATOR:
        case OB_SWITCH_TMP_SCHEMA:
        //add:e
          YYSYS_LOG(INFO, "no longer master. refuse the order");
          break;
		  //add peiouya [MultiUPS] [UPS_SYNC_Frozen] 20150810:b
        case OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE_ACCORD_HB:
          {
            major_freeze_accord_hb_guard_.done ();
            YYSYS_LOG(INFO, "no longer master. refuse the order");
            break;
          }
        //add 20150810:e
        case OB_UPS_FORCE_FETCH_SCHEMA:
          return_code = response_result_(OB_NOT_MASTER, OB_UPS_FORCE_FETCH_SCHEMA_RESPONSE, MY_VERSION, req, channel_id);
          break;
        case OB_UPS_SWITCH_COMMIT_LOG:
          return_code = response_result_(OB_NOT_MASTER, OB_UPS_SWITCH_COMMIT_LOG_RESPONSE, MY_VERSION, req, channel_id);
          break;
        case OB_SLAVE_REG:
          return_code = response_result_(OB_NOT_MASTER, OB_SLAVE_REG_RES, MY_VERSION, req, channel_id);
          break;
        //add zhaoqiong [fixed for Backup]:20150811:b
        case OB_BACKUP_REG:
          return_code = response_result_(OB_NOT_MASTER, OB_BACKUP_REG_RES, MY_VERSION, req, channel_id);
          break;
        //add:e
        case OB_UPS_CLEAR_ACTIVE_MEMTABLE:
          return_code = response_result_(OB_NOT_MASTER, OB_UPS_CLEAR_ACTIVE_MEMTABLE_RESPONSE, MY_VERSION, req, channel_id);
          break;
        case OB_UPS_ASYNC_CHECK_CUR_VERSION:
          YYSYS_LOG(INFO, "not master now, need not check cur version");
          break;
        case OB_UPS_ASYNC_CHECK_SSTABLE_CHECKSUM:
          YYSYS_LOG(INFO, "not master now, need not check sstable version");
          break;
        case OB_WRITE:
        case OB_INTERNAL_WRITE:
        case OB_MS_MUTATE:
        case OB_PHY_PLAN_EXECUTE:
        case OB_START_TRANSACTION:
        case OB_END_TRANSACTION:
        //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
        case OB_WRITE_TRANS_STAT:
        case OB_END_DISTRIBUTED_TRANS:
        case OB_TRANS_PREPARE_ACK:
        case OB_ROLLBACK_TRANSACTION:
        case OB_PREPARE_TRANSACTION:
        case OB_COMMIT_TRANSACTION:
        case OB_WRITE_TRANS_STAT_ACK:
        //add 20150701:e
      case OB_GET_PUBLICED_TRANCEID://add by maosy [MultiUps 1.0] [batch_iud_snapshot] 20170622 
          return_code = response_result_(OB_NOT_MASTER, packet_code, MY_VERSION, req, channel_id);
          break;
        default:
          YYSYS_LOG(ERROR, "UNEXPECT packet %d, ignore this", packet_code);
          return_code = OB_ERROR;
          break;
      }
      return return_code;
    }

    common::ThreadSpecificBuffer::Buffer* ObUpdateServer::get_rpc_buffer() const
    {
      return rpc_buffer_.get_buffer();
    }

    ObUpsRpcStub& ObUpdateServer::get_ups_rpc_stub()
      //MockUpsRpcStub& ObUpdateServer::get_ups_rpc_stub()
    {
      return ups_rpc_stub_;
    }

    //del peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//    int ObUpdateServer::set_obi_role(const int32_t version, common::ObDataBuffer& in_buff,
//        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
//    {
//      UNUSED(out_buff);

//      int err = OB_SUCCESS;

//      if (version != MY_VERSION)
//      {
//        err = OB_ERROR_FUNC_VERSION;
//      }

//      ObiRole obi_role;
//      err = obi_role.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
//      if (OB_SUCCESS != err)
//      {
//        YYSYS_LOG(ERROR, "ObiRole deserialize error, err=%d", err);
//      }
//      else
//      {
//        if (ObiRole::MASTER == obi_role.get_role())
//        {
//          if (ObiRole::MASTER == obi_role_.get_role())
//          {
//            YYSYS_LOG(INFO, "ObiRole is already MASTER");
//          }
//          else if (ObiRole::SLAVE == obi_role_.get_role())
//          {
//            obi_role_.set_role(ObiRole::MASTER);
//            YYSYS_LOG(INFO, "ObiRole is set to MASTER");
//          }
//          else if (ObiRole::INIT == obi_role_.get_role())
//          {
//            obi_role_.set_role(ObiRole::MASTER);
//            YYSYS_LOG(INFO, "ObiRole is set to MASTER");
//          }
//        }
//        else if (ObiRole::SLAVE == obi_role.get_role())
//        {
//          if (ObiRole::MASTER == obi_role_.get_role())
//          {
//            obi_role_.set_role(ObiRole::SLAVE);
//            YYSYS_LOG(INFO, "ObiRole is set to SLAVE");
//          }
//          else if (ObiRole::SLAVE == obi_role_.get_role())
//          {
//            YYSYS_LOG(INFO, "ObiRole is already SLAVE");
//          }
//          else if (ObiRole::INIT == obi_role_.get_role())
//          {
//            obi_role_.set_role(ObiRole::SLAVE);
//            YYSYS_LOG(INFO, "ObiRole is set to SLAVE");
//          }
//        }
//        else
//        {
//          YYSYS_LOG(ERROR, "Unknown ObiRole: %d", obi_role.get_role());
//          err = OB_ERROR;
//        }
//      }

//      // send response to MASTER before writing to disk
//      err = response_result_(err, OB_SET_OBI_ROLE_RESPONSE, MY_VERSION, req, channel_id);

//      return err;
//    }
    //del 20150701:e

    //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160306:b
    void ObUpdateServer::kill_self()
    {
      kill(getpid(), SIGTERM);
    }
    //add 20160306:e

    bool ObUpdateServer::is_lease_valid() const
    {
      bool ret = true;
      int64_t lease = lease_expire_time_us_;
      int64_t lease_time_us = yysys::CTimeUtil::getTime();
      YYSYS_LOG(DEBUG, "lease =%ld, cur_time = %ld, lease-cur_time=%ld", lease, lease_time_us, lease-lease_time_us);
      if (lease_time_us  + lease_timeout_in_advance_ > lease)
      {
        YYSYS_LOG(DEBUG, "lease timeout. lease=%ld, cur_time =%ld", lease, lease_time_us);
        ret = false;
      }
      return ret;
    }

    bool ObUpdateServer::is_master_lease_valid() const
    {
      //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//      return ObiRole::MASTER == obi_role_.get_role()
//        && ObUpsRoleMgr::MASTER == role_mgr_.get_role()
//        && ObUpsRoleMgr::ACTIVE == role_mgr_.get_state()
//        && is_lease_valid();
      return ObUpsRoleMgr::MASTER == role_mgr_.get_role()
        && ObUpsRoleMgr::ACTIVE == role_mgr_.get_state()
        && is_lease_valid();
      //mod 20150701:e
    }

    void ObUpdateServer::on_ioth_start()
    {
      int64_t affinity_start_cpu = config_.io_thread_start_cpu;
      int64_t affinity_end_cpu = config_.io_thread_end_cpu;
      if (0 <= affinity_start_cpu
          && affinity_start_cpu <= affinity_end_cpu)
      {
        static volatile uint64_t cpu = 0;
        uint64_t local_cpu = __sync_fetch_and_add(&cpu, 1) % (affinity_end_cpu - affinity_start_cpu + 1) + affinity_start_cpu;
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(local_cpu, &cpuset);
        int ret = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
        YYSYS_LOG(INFO, "io thread setaffinity tid=%ld ret=%d cpu=%ld start=%ld end=%ld",
                  GETTID(), ret, local_cpu, affinity_start_cpu, affinity_end_cpu);
      }
      SET_THD_NAME("ups-io");
    }

    /*int ObUpdateServer::slave_set_fetch_param(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int err = OB_SUCCESS;
      ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        YYSYS_LOG(WARN, "version not equal. version=%d, MY_VERSION=%d", version, MY_VERSION);
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      ObUpsFetchParam fetch_param;
      if (OB_SUCCESS == err && result_msg.result_code_ == OB_SUCCESS)
      {
        err = fetch_param.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (OB_SUCCESS != err)
        {
          YYSYS_LOG(WARN, "fail to deserialize fetch param. err=%d", err);
        }
      }
      if (OB_SUCCESS == err && result_msg.result_code_ == OB_SUCCESS)
      {
        result_msg.result_code_ = set_fetch_thread(fetch_param);
        if (OB_SUCCESS != result_msg.result_code_)
        {
          YYSYS_LOG(WARN, "fail to set_fetch_thread, err = %d", err);
        }
      }
      if (OB_SUCCESS == err)
      {
        err = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (OB_SUCCESS != err)
        {
          YYSYS_LOG(WARN, "fail to serialize result_msg,err=%d", err);
        }
      }
      if (OB_SUCCESS == err)
      {
        err = send_response(OB_SEND_FETCH_CONFIG_RES, MY_VERSION, out_buff, req, channel_id);
        if (OB_SUCCESS != err)
        {
          YYSYS_LOG(WARN, "fail to send response. err=%d", err);
        }
      }
      return err;
    }
*/
    int ObUpdateServer::ups_fetch_log_for_slave(const int32_t version, common::ObDataBuffer& in_buff,
                                                easy_request_t* request, const uint32_t channel_id, common::ObDataBuffer& out_buff,
                                                ObPacket* packet)
    {
      int err = OB_SUCCESS;
      int ret_err = OB_SUCCESS;
      ObFetchLogReq req;
      ObFetchedLog result;
      const char* src_addr = inet_ntoa_r(get_easy_addr(request));
      if (version != MY_VERSION)
      {
        err = OB_ERROR_FUNC_VERSION;
      }
      else if (OB_SUCCESS != (err = req.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position())))
      {
        YYSYS_LOG(ERROR, "req.deserialize(buf=%p[%ld], pos=%ld)=>%d",
                  in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), err);
      }
      else if (OB_SUCCESS != (err = log_mgr_.get_log_for_slave_fetch(req, result))
               && OB_DATA_NOT_SERVE != err)
      {
        YYSYS_LOG(ERROR, "log_mgr_.fetch_log_for_slave(srv=%s, req=%s, result=%s,master_log_id=%ld)=>%d", src_addr, to_cstring(req), to_cstring(result), log_mgr_.get_master_log_seq(), err);
      }
      else if (OB_DATA_NOT_SERVE == err)
      {
        ret_err = OB_NEED_RETRY;
        err = OB_SUCCESS;
        if (REACH_TIME_INTERVAL(10 * 1000000))
        {
          YYSYS_LOG(WARN, "log_mgr_.fetch_log_for_slave(srv=%s, req=%s, result=%s, master_log_id=%ld): DATA_NOT_SERVE", src_addr, to_cstring(req), to_cstring(result), log_mgr_.get_master_log_seq());
        }
      }
      if (packet->get_receive_ts() + (packet->get_source_timeout()?: config_.packet_max_wait_time)
          < yysys::CTimeUtil::getTime())
      {
        err = OB_RESPONSE_TIME_OUT;
        YYSYS_LOG(ERROR, "get_log_for_slave_fetch() too slow[receive_ts[%ld] + timeout[%ld] < curTime[%ld]]",
                  packet->get_receive_ts(), packet->get_source_timeout()?: config_.packet_max_wait_time,
                  yysys::CTimeUtil::getTime());
      }
      else if (OB_SUCCESS != (err = response_data_(OB_SUCCESS == err? ret_err: err, result, OB_FETCH_LOG_RESPONSE, MY_VERSION, request, channel_id, out_buff)))
      {
        YYSYS_LOG(ERROR, "response_data()=>%d", err);
      }
      return err;
    }

    int ObUpdateServer::ups_fill_log_cursor_for_slave(const int32_t version, common::ObDataBuffer& in_buff,
                                                      easy_request_t* req, const uint32_t channel_id,
                                                      common::ObDataBuffer& out_buff)
    {
      int err = OB_SUCCESS;
      ObLogCursor cursor;
      if (version != MY_VERSION)
      {
        err = OB_ERROR_FUNC_VERSION;
      }
      else if (OB_SUCCESS != (err = cursor.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position())))
      {
        YYSYS_LOG(ERROR, "cursor.deserialize(buf=%p[%ld], pos=%ld)=>%d",
                  in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), err);
      }
      else if (OB_SUCCESS != (err = log_mgr_.fill_log_cursor(cursor)))
      {
        YYSYS_LOG(ERROR, "log_mgr_.fill_log_cursor(cursor)=>%d", err);
      }
      else if (OB_SUCCESS !=
          (err = response_data_(err, cursor, OB_FILL_LOG_CURSOR_RESPONSE, MY_VERSION, req, channel_id, out_buff)))
      {
        YYSYS_LOG(ERROR, "response_data()=>%d", err);
      }
      return err;
    }
    //add lbzhong [Clog Monitor] 20151218:b
    int ObUpdateServer::clog_monitor_get_ups_list(const int32_t version, common::ObDataBuffer& in_buff,
                                                      easy_request_t* req, const uint32_t channel_id,
                                                      common::ObDataBuffer& out_buff)
    {
      int err = OB_SUCCESS;
      UNUSED(in_buff);
      if (version != MY_VERSION)
      {
        err = OB_ERROR_FUNC_VERSION;
      }
      else
      {
        ObString ups_list;
        char ups_buf[1024];
        int64_t pos = 0;
        if (ObUpsRoleMgr::MASTER == role_mgr_.get_role())
        {
          ObServer slaves[ObUpsSlaveMgr::MAX_SLAVE_NUM];
          int64_t slave_num = 0;
          if (OB_SUCCESS != (err = slave_mgr_.get_slaves(slaves, ObUpsSlaveMgr::MAX_SLAVE_NUM, slave_num)))
          {
            YYSYS_LOG(ERROR, "get_slaves(limit=%ld)=>%d", ObUpsSlaveMgr::MAX_SLAVE_NUM, err);
          }
          else
          {
            databuff_printf(ups_buf, sizeof(ups_buf), pos, "s:");
            for(int64_t i = 0; i < slave_num; i++)
            {
              char ip_buf[32];
              slaves[i].ip_to_string(ip_buf, sizeof(ip_buf));
              databuff_printf(ups_buf, sizeof(ups_buf), pos, "%s:%d,", ip_buf, slaves[i].get_port());
            }
          }
        }
        else
        {
          char ip_buf[32];
          char inst_ip_buf[32];
          ups_master_.ip_to_string(ip_buf, sizeof(ip_buf));
          sys_ups_master_.ip_to_string(inst_ip_buf, sizeof(ip_buf));
          databuff_printf(ups_buf, sizeof(ups_buf), pos, "m:%s:%d", ip_buf, ups_master_.get_port());
        }

        if(OB_SUCCESS == err)
        {
          ups_list.assign_ptr(ups_buf, static_cast<int32_t>(pos));
          if (OB_SUCCESS !=
              (err = response_data_(err, ups_list, OB_CLOG_MONITOR_GET_UPS_LIST_RESPONSE, MY_VERSION, req, channel_id, out_buff)))
          {
            YYSYS_LOG(ERROR, "response_data()=>%d", err);
          }
        }
      }
      return err;
    }

    int ObUpdateServer::clog_monitor_get_clog_status(const int32_t version, common::ObDataBuffer& in_buff,
                                                      easy_request_t* req, const uint32_t channel_id,
                                                      common::ObDataBuffer& out_buff)
    {
      int err = OB_SUCCESS;
      ClogStatus stat;
      UNUSED(in_buff);

      if (version != MY_VERSION)
      {
        err = OB_ERROR_FUNC_VERSION;
      }
      else
      {
        stat.self_ip_ = self_addr_.get_ipv4();
        stat.self_port_ = self_addr_.get_port();
        stat.state_ = role_mgr_.get_state();
        stat.master_rs_ip_ = root_server_.get_ipv4();
        stat.master_rs_port_ = root_server_.get_port();
        stat.master_ups_ip_ = ups_master_.get_ipv4();
        stat.master_ups_port_ = ups_master_.get_port();
        stat.lease_ = lease_expire_time_us_;
        stat.set_lease_remain(lease_expire_time_us_ - yysys::CTimeUtil::getTime());

        stat.role_ = role_mgr_.get_role();
        if(ObUpsRoleMgr::MASTER == role_mgr_.get_role())
        {
          stat.send_receive_ = slave_mgr_.get_last_send_log_id() - 1;
        }
        else if(ObUpsRoleMgr::SLAVE == role_mgr_.get_role())
        {
          stat.send_receive_ = log_mgr_.get_last_receive_log_id() - 1;
        }
        stat.log_term_ = log_mgr_.get_current_log_term();
        stat.commit_replay_ = replay_worker_.get_next_commit_log_id() - 1;
        stat.flush_ = replay_worker_.get_next_flush_log_id() - 1;
        stat.commit_point_ = log_mgr_.get_last_wrote_commit_point();

        if (OB_SUCCESS !=
            (err = response_data_(err, stat, OB_CLOG_MONITOR_GET_CLOG_STAT_RESPONSE, MY_VERSION, req, channel_id, out_buff)))
        {
          YYSYS_LOG(ERROR, "response_data()=>%d", err);
        }
      }
      return err;
    }
    //add:e

    int ObUpdateServer::ups_get_commit_log_status(const int32_t version, common::ObDataBuffer &in_buff,
                                                  easy_request_t *req, const uint32_t channel_id,
                                                  common::ObDataBuffer &out_buff)
    {
        int ret = OB_SUCCESS;
        common::ObResultCode rc;
        ClogStatus stat;
        int64_t commit_file_id = 0;
        int64_t replay_point = 0;
        ObLogCursor cursor;
        UNUSED(in_buff);

        if(version != MY_VERSION)
        {
            ret = OB_ERROR_FUNC_VERSION;
        }
        else
        {
            stat.self_ip_ = self_addr_.get_ipv4();
            stat.self_port_ = self_addr_.get_port();
            stat.state_ = role_mgr_.get_state();
            stat.master_rs_ip_ = root_server_.get_ipv4();
            stat.master_rs_port_ = root_server_.get_port();
            stat.master_ups_ip_ = ups_master_.get_ipv4();
            stat.master_ups_port_ = ups_master_.get_port();
            stat.lease_ = lease_expire_time_us_;
            stat.set_lease_remain(lease_expire_time_us_ - yysys::CTimeUtil::getTime());

            stat.role_ = role_mgr_.get_role();
            if(ObUpsRoleMgr::MASTER == role_mgr_.get_role())
            {
                stat.send_receive_ = slave_mgr_.get_last_send_log_id();
                stat.commit_replay_ = log_mgr_.get_flushed_clog_id();
                stat.flush_ = log_mgr_.get_flushed_cursor_log_id();
            }
            else if (ObUpsRoleMgr::SLAVE == role_mgr_.get_role())
            {
                stat.send_receive_ = log_mgr_.get_last_receive_log_id();
                stat.commit_replay_ = replay_worker_.get_next_commit_log_id();
                stat.flush_ = replay_worker_.get_next_flush_log_id();
            }
            stat.log_term_ = log_mgr_.get_current_log_term();
            stat.commit_point_ = log_mgr_.get_current_max_commit_log_id();

            ret = log_mgr_.get_replay_point().get(replay_point);
            if(OB_SUCCESS != ret)
            {
                YYSYS_LOG(WARN,"get replay point failed, ret=%d",ret);
            }

            ret = log_mgr_.get_cmt_log_cursor(cursor);
            if(OB_SUCCESS != ret)
            {
                YYSYS_LOG(WARN,"get cmmmit log cursor failed, ret=%d",ret);
            }
            else
            {
                commit_file_id = cursor.file_id_;
            }

            ret = rc.serialize(out_buff.get_data(),out_buff.get_capacity(),out_buff.get_position());
            if(OB_SUCCESS != ret)
            {
                YYSYS_LOG(WARN,"serialize the rc failed, ret=%d", ret);
            }
            else
            {
                if(OB_SUCCESS != (ret = stat.serialize(out_buff.get_data(),out_buff.get_capacity(),out_buff.get_position())))
                {
                    YYSYS_LOG(WARN,"serialize ClogStatus failed, ret=%d", ret);
                }
                else
                {

                    ret = serialization::encode_vi64(out_buff.get_data(),out_buff.get_capacity(),out_buff.get_position(),commit_file_id);
                    if(OB_SUCCESS != ret)
                    {
                        YYSYS_LOG(WARN,"serialize commit_file_id failed, ret=%d", ret);
                    }
                    else
                    {
                        ret = serialization::encode_vi64(out_buff.get_data(),out_buff.get_capacity(),out_buff.get_position(),replay_point);
                        if (OB_SUCCESS != ret)
                        {
                            YYSYS_LOG(WARN,"serialize replay_point failed, ret=%d", ret);
                        }
                    }
                }
            }
        }
        if(OB_SUCCESS == ret )
        {
            ret = send_response(OB_UPS_GET_COMMIT_LOG_STAT_RESPONSE, MY_VERSION, out_buff, req, channel_id);
            if (OB_SUCCESS != ret)
            {
                YYSYS_LOG(WARN, "fail to send response to get commit log status, ret=%d",ret);
            }
        }
        return ret;
    }

    //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
    int ObUpdateServer::ups_get_clog_status(const int32_t version, common::ObDataBuffer& in_buff,
                                                      easy_request_t* req, const uint32_t channel_id,
                                                      common::ObDataBuffer& out_buff)
    {
      int err = OB_SUCCESS;
      int tmp_ret = OB_SUCCESS;
      ObLogCursor replayed_cursor;
      uint64_t frozen_version = 0;
      int64_t max_log_id_replayable = 0;
      ObUpsCLogStatus stat;
      UNUSED(in_buff);

      if (version != MY_VERSION)
      {
        err = OB_ERROR_FUNC_VERSION;
      }
      else
      {
        if (OB_SUCCESS != (tmp_ret = log_mgr_.get_replayed_cursor(replayed_cursor)))
        {
          YYSYS_LOG(WARN, "log_mgr.get_replayed_cursor()=>%d", tmp_ret);
        }
        if (OB_SUCCESS != (tmp_ret = log_mgr_.get_max_log_seq_replayable(max_log_id_replayable)))
        {
          YYSYS_LOG(WARN, "log_mgr.get_max_log_id_replayable()=>%d", tmp_ret);
        }
        if (OB_SUCCESS != (tmp_ret = table_mgr_.get_last_frozen_memtable_version(frozen_version)))
        {
          YYSYS_LOG(WARN, "table_mgr.get_last_frozen_memtable_version()=>%d", tmp_ret);
        }
        stat.obi_slave_stat_ = obi_slave_stat_;
        stat.slave_sync_type_.set_sync_type(config_.real_time_slave ? ObSlaveSyncType::REAL_TIME_SLAVE : ObSlaveSyncType::NON_REAL_TIME_SLAVE);
        //stat.obi_role_ = obi_role_;
        stat.role_mgr_ = role_mgr_;
        stat.rs_ = root_server_;
        stat.self_ = self_addr_;
        stat.ups_master_ = ups_master_;
        //stat.inst_ups_master_ = ups_inst_master_;
        //stat.lsync_ = lsync_server_;
        //add peiouya [MultiUPS] [DELETE_OBI] 20150810:b
        //note: CDI_multiUPS_duht.xls:sheet 1
        stat.sys_table_master_ups_ = sys_ups_master_;
        //add 20150810:e
        stat.last_frozen_version_ = frozen_version;
        stat.replay_switch_ = log_replay_thread_.get_switch();
        stat.replayed_cursor_ = replayed_cursor;
        stat.max_log_id_replayable_ = max_log_id_replayable;
        stat.master_log_id_ = log_mgr_.get_master_log_seq();
        stat.next_submit_log_id_ = replay_worker_.get_next_submit_log_id();
        stat.next_commit_log_id_ = replay_worker_.get_next_commit_log_id();
        stat.next_flush_log_id_ = replay_worker_.get_next_flush_log_id();
        stat.last_barrier_log_id_ = replay_worker_.get_last_barrier_log_id();
        stat.wait_trans_ = trans_executor_.TransHandlePool::get_queued_num();
        stat.wait_commit_ = trans_executor_.get_commit_queue_len();
        stat.wait_response_ = trans_executor_.CommitEndHandlePool::get_queued_num();
      }
      if (OB_SUCCESS !=
          (err = response_data_(err, stat, OB_GET_CLOG_STATUS_RESPONSE, MY_VERSION, req, channel_id, out_buff)))
      {
        YYSYS_LOG(ERROR, "response_data()=>%d", err);
      }
      return err;
    }
    //mod 20150701:e

    int ObUpdateServer::ups_slave_write_log(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id,
        //add chujiajia [Paxos ups_replication] 20160107:b
        const int64_t cmt_log_id,
        //add:e
        common::ObDataBuffer& out_buff)
    {
      UNUSED(out_buff);
      int err = OB_SUCCESS;
      int response_err = OB_SUCCESS;

      if (version != MY_VERSION)
      {
        err = OB_ERROR_FUNC_VERSION;
      }
      if (OB_SUCCESS != err)
      {}
      else if (OB_SUCCESS != (err = (log_mgr_.slave_receive_log(in_buff.get_data() + in_buff.get_position(),
                                                                in_buff.get_capacity() - in_buff.get_position(),
                                                                config_.wait_slave_sync_time, //100ms
                                                                //add chujiajia [Paxos ups_replication] 20160107:b
                                                                cmt_log_id,
                                                                //add:e
                                                                (ObUpsLogMgr::WAIT_SYNC_TYPE)static_cast<int>(config_.wait_slave_sync_type)))))
      {
        //add wangjiahao [Paxos ups_replication] 20150716 :b
        if (err == OB_ERR_UNEXPECTED)
        {
          YYSYS_LOG(ERROR, "slave_receive_log(buf=%p[%ld:%ld])=>%d",
                    in_buff.get_data(), in_buff.get_position(), in_buff.get_capacity(), err);
        }
        else
        {
         YYSYS_LOG(WARN, "slave_receive_log(buf=%p[%ld:%ld])=>%d",
                   in_buff.get_data(), in_buff.get_position(), in_buff.get_capacity(), err);
        }
      }
      //add :e
      //mod wangdonghui [ups_log_replication_optimize] 20161009 :b
      //response only if err != OB_SUCCESS and will response in replay runnable
      if (OB_SUCCESS != err && OB_SUCCESS != (response_err = response_result1_(err, OB_SEND_LOG_RES, MY_VERSION, req, channel_id, -1)))
      {
        err = response_err;
        YYSYS_LOG(ERROR, "response_result_()=>%d", err);
      }
      //mod :e
      return err;
    }
    //add wangdonghui [ups_log_replication_optimize] 20161009 :b
    int ObUpdateServer::ups_push_wait_flush(TransExecutor::Task* task)
    {
      int ret = OB_SUCCESS;
      ret = wait_flush_thread_.push(task);
      return ret;
    }

    int ObUpdateServer::ups_change_log_level(const int32_t version, common::ObDataBuffer& in_buff,
                                         easy_request_t* req, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      UNUSED(version);
      int32_t log_level = -1;
      if (OB_SUCCESS != (ret = serialization::decode_vi32(in_buff.get_data(),
                                                          in_buff.get_capacity(),
                                                          in_buff.get_position(),
                                                          &log_level)))
      {
        YYSYS_LOG(WARN, "deserialize error, err=%d", ret);
      }
      else
      {
        if (YYSYS_LOG_LEVEL_ERROR <= log_level
            && YYSYS_LOG_LEVEL_DEBUG >= log_level)
        {
          YYSYS_LOG(INFO, "change log level. From: %d, To: %d.", YYSYS_LOGGER._level, log_level);
          YYSYS_LOGGER._level = log_level;
        }
        else
        {
          YYSYS_LOG(WARN, "invalid log level, level=%d", log_level);
          ret = OB_INVALID_ARGUMENT;
        }
        if (OB_SUCCESS == ret)
          ret = response_result_(ret, OB_CHANGE_LOG_LEVEL_RESPONSE, MY_VERSION, req, channel_id);
      }
      return ret;
    }

    int ObUpdateServer::ups_stop_server(const int32_t version, common::ObDataBuffer& in_buff,
                                         easy_request_t* req, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      UNUSED(version);

      int64_t server_id = root_server_.get_ipv4_server_id();
      int64_t peer_id = convert_addr_to_server(req->ms->c->addr);
      if (server_id != peer_id)
      {
        YYSYS_LOG(WARN, "*stop server* WARNNING coz packet from unrecongnized address "
                  "which is [%ld], should be [%ld] as rootserver.", peer_id, server_id);
        // comment follow line not to strict packet from rs.
        // rc.result_code_ = OB_ERROR;
      }

      int32_t restart = 0;
      if (OB_SUCCESS != (ret = serialization::decode_i32(in_buff.get_data(),
                                                         in_buff.get_capacity(),
                                                         in_buff.get_position(),
                                                         &restart)))
      {
        YYSYS_LOG(WARN, "deserialize error, err=%d", ret);
      }
      else
      {
        ret = response_result_(ret, OB_SET_OBI_ROLE_RESPONSE, MY_VERSION, req, channel_id);
        if (restart != 0)
        {
          YYSYS_LOG(WARN, "set restart server flag, ready to restart server!");
          BaseMain::set_restart_flag();
        }
        stop();
      }
      return ret;
    }

    int ObUpdateServer::ob_malloc_stress(const int32_t version, common::ObDataBuffer& in_buff,
                                         easy_request_t* req, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      int64_t malloc_limit = 0;
      UNUSED(version);

      if (OB_SUCCESS != (ret = serialization::decode_vi64(in_buff.get_data(),
                                                         in_buff.get_capacity(),
                                                         in_buff.get_position(),
                                                         &malloc_limit)))
      {
        YYSYS_LOG(WARN, "deserialize error, err=%d, buf=%p[%ld:%ld]", ret, in_buff.get_data(), in_buff.get_position(), in_buff.get_capacity());
      }
      else
      {
        for(int64_t i = 0; i < 100; i++)
        {
          int64_t block_size = 1<<21;
          DefaultBlockAllocator block_allocator;
          StackAllocator stack_allocator;
          malloc_limit <<= 20;
          if (OB_SUCCESS != (ret = stack_allocator.init(&block_allocator, 1<<16)))
          {
            YYSYS_LOG(ERROR, "stack_allocator.init()=>%d", ret);
          }
          for(int64_t malloc_size = 0; OB_SUCCESS == ret && malloc_size < malloc_limit; malloc_size += block_size)
          {
            if (NULL == stack_allocator.alloc(block_size))
            {
              YYSYS_LOG(ERROR, "stack_allocator.alloc(%ld) FAIL", block_size);
            }
          }
        }
      }
      ret = response_result_(ret, OB_MALLOC_STRESS_RESPONSE, MY_VERSION, req, channel_id);
      return ret;
    }

    int ObUpdateServer::ob_login(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      int proc_ret = OB_SUCCESS;
      ret = OB_NOT_SUPPORTED;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }

      ObLoginInfo login_info;
      easy_addr_t addr = get_easy_addr(req);
      ret = login_info.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "deserialize login info fail ret=%d src=%s", ret, NULL == req ? NULL :
                  inet_ntoa_r(addr));
      }
      else
      {
        ObToken token;
        if (OB_SUCCESS == ret)
        {
          proc_ret = response_data_(ret, token, OB_LOGIN_RES, MY_VERSION, req, channel_id, out_buff);
        }
        else
        {
          proc_ret = response_result_(ret, OB_LOGIN_RES, MY_VERSION, req, channel_id);
        }
      }
      YYSYS_LOG(INFO, "ob_login ret=%d src=%s", ret, NULL == req ? NULL :
                inet_ntoa_r(addr));

      return proc_ret;
    }

    int ObUpdateServer::ups_set_sync_limit(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(out_buff);
      int ret = OB_SUCCESS;

      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }

      int64_t new_limit = 0;
      ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &new_limit);
      if (OB_SUCCESS == ret)
      {
        //fetch_thread_.set_limit_rate(new_limit);
        //YYSYS_LOG(INFO, "update sync limit=%ld", fetch_thread_.get_limit_rate());
      }

      ret = response_result_(ret, OB_SET_SYNC_LIMIT_RESPONSE, MY_VERSION, req, channel_id);

      return ret;
    }

    int ObUpdateServer::ups_ping(const int32_t version, easy_request_t* req, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;

      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }

      ret = response_result_(ret, OB_PING_RESPONSE, MY_VERSION, req, channel_id);

      return ret;
    }

    int ObUpdateServer::ups_get_clog_master(const int32_t version, easy_request_t* req,
                                            const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int err = OB_SUCCESS;
      common::ObServer* master = NULL;
      if (MY_VERSION != version)
      {
        err = OB_ERROR_FUNC_VERSION;
        YYSYS_LOG(ERROR, "MY_VERSION[%d] != version[%d]", MY_VERSION, version);
      }
      //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//      else if (ObiRole::MASTER == obi_role_.get_role())
//      {
//        if (ObUpsRoleMgr::MASTER == role_mgr_.get_role())
//        {
//          master = &self_addr_;
//        }
//        else if (ObUpsRoleMgr::SLAVE == role_mgr_.get_role())
//        {
//          master = &ups_master_;
//        }
//        else
//        {
//          err = OB_ERROR;
//          YYSYS_LOG(ERROR, "ob_role != MASTER && obi_role != SLAVE");
//        }
//      }
//      else if (ObiRole::SLAVE == obi_role_.get_role())
//      {
//        if (ObUpsRoleMgr::MASTER == role_mgr_.get_role())
//        {
//          if (STANDALONE_SLAVE == obi_slave_stat_)
//          {
//            master = &lsync_server_;
//          }
//          else if (FOLLOWED_SLAVE == obi_slave_stat_)
//          {
//            master = &ups_inst_master_;
//          }
//        }
//        else if (ObUpsRoleMgr::SLAVE == role_mgr_.get_role())
//        {
//          if (STANDALONE_SLAVE == obi_slave_stat_)
//          {
//              //master = &lsync_server_;
//            err = OB_NOT_SUPPORTED;
//            YYSYS_LOG(ERROR, "slave slave ups not allowed to connect lsyncserver");
//          }
//          else if (FOLLOWED_SLAVE == obi_slave_stat_)
//          {
//            master = &ups_master_;
//          }
//        }
//        else
//        {
//          err = OB_ERROR;
//          YYSYS_LOG(ERROR, "ob_role != MASTER && obi_role != SLAVE");
//        }
//      }
//      else
//      {
//        err = OB_ERROR;
//        YYSYS_LOG(ERROR, "obi_role != MASTER && obi_role != SLAVE");
//      }
      else if (ObUpsRoleMgr::MASTER == role_mgr_.get_role())
      {
        master = &self_addr_;
      }
      else if (ObUpsRoleMgr::SLAVE == role_mgr_.get_role())
      {
        master = &ups_master_;
      }
      //mod 20150701:e
      //add peiouya [MultiUPS] [DELETE_OBI] 20150723:b
      //expr:add exception(else) handling branch
      else
      {
        err = OB_ERROR;
        YYSYS_LOG(ERROR, "cur ups role != MASTER && cur ups role != SLAVE");
      }
      //add 20150723:e

      if (OB_SUCCESS != err)
      {}
      else if (NULL == master)
      {
        err = OB_ERR_UNEXPECTED;
        YYSYS_LOG(ERROR, "NULL == master");
      }
      else if(OB_SUCCESS != (err = response_data_(err, *master, OB_GET_CLOG_MASTER_RESPONSE, MY_VERSION, req, channel_id, out_buff)))
      {
        YYSYS_LOG(ERROR, "response_data()=>%d", err);
      }

      return err;
    }

    int ObUpdateServer::ups_get_clog_cursor(const int32_t version, easy_request_t* req,
                                            const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int err = OB_SUCCESS;
      ObLogCursor log_cursor;
      if (MY_VERSION != version)
      {
        err = OB_ERROR_FUNC_VERSION;
        YYSYS_LOG(ERROR, "MY_VERSION[%d] != version[%d]", MY_VERSION, version);
      }
      else if (OB_SUCCESS != (err = log_mgr_.get_replayed_cursor(log_cursor)))
      {
        YYSYS_LOG(ERROR, "log_mgr.get_replayed_cursor()=>%d", err);
      }

      if(OB_SUCCESS != (err = response_data_(err, log_cursor, OB_GET_CLOG_CURSOR_RESPONSE, MY_VERSION, req, channel_id, out_buff)))
      {
        YYSYS_LOG(ERROR, "response_data()=>%d", err);
      }

      return err;
    }

    int ObUpdateServer::ups_get_log_sync_delay_stat(const int32_t version, easy_request_t* req,
                                            const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int err = OB_SUCCESS;
      if (MY_VERSION != version)
      {
        err = OB_ERROR_FUNC_VERSION;
        YYSYS_LOG(ERROR, "MY_VERSION[%d] != version[%d]", MY_VERSION, version);
      }
      else if(OB_SUCCESS != (err = response_data_(err, log_mgr_.get_delay_stat(), OB_GET_LOG_SYNC_DELAY_STAT_RESPONSE, MY_VERSION, req, channel_id, out_buff)))
      {
       YYSYS_LOG(ERROR, "response_data()=>%d", err);
      }
      else if(OB_SUCCESS != (err = log_mgr_.get_delay_stat().reset_max_delay()))
      {
       YYSYS_LOG(ERROR, "reset_max_delay()=>%d", err);
      }
      return err;
    }

    int ObUpdateServer::ups_get_clog_stat(const int32_t version, easy_request_t* req,
                                            const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int err = OB_SUCCESS;
      ObLogCursor log_cursor;
      if (MY_VERSION != version)
      {
        err = OB_ERROR_FUNC_VERSION;
        YYSYS_LOG(ERROR, "MY_VERSION[%d] != version[%d]", MY_VERSION, version);
      }

      if(OB_SUCCESS != (err = response_data_(err, log_mgr_.get_clog_stat(), OB_GET_CLOG_STAT_RESPONSE, MY_VERSION, req, channel_id, out_buff)))
      {
        YYSYS_LOG(ERROR, "response_data()=>%d", err);
      }
      else
      {
        log_mgr_.get_clog_stat().clear();
      }

      return err;
    }

    int ObUpdateServer::ups_sql_scan(const int32_t version, common::ObDataBuffer& in_buff,
                                     easy_request_t* req, const uint32_t channel_id,
                                     common::ObDataBuffer& out_buff)
    {
      static const int32_t UPS_SCAN_VERSION = 1;
      int ret = OB_SUCCESS;
      ObNewScanner *new_scanner = GET_TSI_MULT(ObNewScanner, TSI_UPS_NEW_SCANNER_1);
      sql::ObSqlScanParam *sql_scan_param_ptr = GET_TSI_MULT(sql::ObSqlScanParam,
                                                             TSI_UPS_SQL_SCAN_PARAM_1);
      if (version != UPS_SCAN_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      else if (NULL == new_scanner || NULL == sql_scan_param_ptr)
      {
        YYSYS_LOG(ERROR, "failed to get thread local scan_param or new scanner, "
                  "new_scanner=%p, sql_scan_param_ptr=%p", new_scanner, sql_scan_param_ptr);
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        new_scanner->reuse();
        sql_scan_param_ptr->reset();
      }

      if (OB_SUCCESS == ret)
      {
        ret = sql_scan_param_ptr->deserialize(
          in_buff.get_data(), in_buff.get_capacity(),
          in_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          YYSYS_LOG(ERROR, "parse cs_sql_scan input scan param error.");
        }
      }

      if (OB_SUCCESS == ret)
      {
        new_scanner->set_range(*sql_scan_param_ptr->get_range());

        OB_STAT_SET(UPDATESERVER, UPS_STAT_MEMORY_TOTAL, ob_get_memory_size_handled());
        OB_STAT_SET(UPDATESERVER, UPS_STAT_MEMORY_LIMIT, ob_get_memory_size_limit());
        OB_STAT_SET(UPDATESERVER, UPS_STAT_HL_QUEUED_COUNT, trans_executor_.TransHandlePool::get_high_prio_queued_num());
        OB_STAT_SET(UPDATESERVER, UPS_STAT_NL_QUEUED_COUNT, trans_executor_.TransHandlePool::get_normal_prio_queued_num());
        OB_STAT_SET(UPDATESERVER, UPS_STAT_LL_QUEUED_COUNT, trans_executor_.TransHandlePool::get_low_prio_queued_num());
        OB_STAT_SET(UPDATESERVER, UPS_STAT_HT_QUEUED_COUNT, trans_executor_.TransHandlePool::get_hotspot_queued_num());
        OB_STAT_SET(UPDATESERVER, UPS_STAT_TRANS_COMMIT_QUEUED_COUNT, trans_executor_.get_commit_queue_len());
        OB_STAT_SET(UPDATESERVER, UPS_STAT_TRANS_RESPONSE_QUEUED_COUNT, trans_executor_.CommitEndHandlePool::get_queued_num());
        table_mgr_.update_memtable_stat_info();
        OB_STAT_SET(UPDATESERVER, UPS_STAT_GET_COUNT, OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_LL_GET_COUNT)
                                                    + OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_NL_GET_COUNT)
                                                    + OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_HL_GET_COUNT));
        OB_STAT_SET(UPDATESERVER, UPS_STAT_SCAN_COUNT, OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_LL_SCAN_COUNT)
                                                     + OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_NL_SCAN_COUNT)
                                                     + OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_HL_SCAN_COUNT));
        OB_STAT_SET(UPDATESERVER, UPS_STAT_APPLY_COUNT, OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_LL_APPLY_COUNT)
                                                      + OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_NL_APPLY_COUNT)
                                                      + OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_HL_APPLY_COUNT));
        OB_STAT_SET(UPDATESERVER, UPS_STAT_APPLY_QTIME, OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_LL_APPLY_QTIME)
                                                      + OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_NL_APPLY_QTIME)
                                                      + OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_HL_APPLY_QTIME));
        OB_STAT_SET(UPDATESERVER, UPS_STAT_TRANS_COUNT, OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_LL_TRANS_COUNT)
                                                      + OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_NL_TRANS_COUNT)
                                                      + OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_HL_TRANS_COUNT));
        OB_STAT_SET(UPDATESERVER, UPS_STAT_GET_TIMEU, OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_LL_GET_TIMEU)
                                                    + OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_NL_GET_TIMEU)
                                                    + OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_HL_GET_TIMEU));
        OB_STAT_SET(UPDATESERVER, UPS_STAT_SCAN_TIMEU, OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_LL_SCAN_TIMEU)
                                                     + OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_NL_SCAN_TIMEU)
                                                     + OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_HL_SCAN_TIMEU));
        OB_STAT_SET(UPDATESERVER, UPS_STAT_APPLY_TIMEU, OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_LL_APPLY_TIMEU)
                                                      + OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_NL_APPLY_TIMEU)
                                                      + OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_HL_APPLY_TIMEU));
        OB_STAT_SET(UPDATESERVER, UPS_STAT_TRANS_TIMEU, OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_LL_TRANS_TIMEU)
                                                      + OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_NL_TRANS_TIMEU)
                                                      + OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_HL_TRANS_TIMEU));

        ret = stat_mgr_.get_scanner(*new_scanner);
        if(OB_SUCCESS != ret)
        {
          YYSYS_LOG(WARN, "open query service fail:err[%d]", ret);
        }
        else if (OB_SUCCESS != (ret = response_data_(ret, *new_scanner,
                                                     OB_SQL_SCAN_RESPONSE, UPS_SCAN_VERSION,
                                                     req, channel_id, out_buff)))
        {
          YYSYS_LOG(WARN, "send new sql scan result fail, ret: [%d]", ret);
        }
      }

      return ret;
    }

    int ObUpdateServer::ups_new_get(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff,
        const int64_t start_time, const int64_t timeout, const int32_t priority)
    {
      int ret = OB_SUCCESS;
      ObGetParam get_param_stack;
      ObGetParam *get_param_ptr = GET_TSI_MULT(ObGetParam, TSI_UPS_GET_PARAM_1);
      ObGetParam &get_param = (NULL == get_param_ptr) ? get_param_stack : *get_param_ptr;
      common::ObRowDesc row_desc;
      common::ObCellNewScanner *new_scanner = GET_TSI_MULT(ObCellNewScanner, TSI_UPS_NEW_SCANNER_1);

      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }

      if (OB_SUCCESS == ret)
      {
        if (NULL == new_scanner)
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          YYSYS_LOG(WARN, "fail to allocate mem for new scanner");
        }
        else
        {
          new_scanner->reuse();
        }
      }

      //CLEAR_TRACE_LOG();
      if (OB_SUCCESS == ret)
      {
        ret = get_param.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          YYSYS_LOG(WARN, "deserialize get param retor, ret=%d", ret);
        }
        FILL_TRACE_LOG("get param deserialize ret=%d", ret);
      }

      if (OB_SUCCESS == ret)
      {
        if (get_param.get_is_read_consistency())
        {
          //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//          if ( !(ObiRole::MASTER == obi_role_.get_role() && ObUpsRoleMgr::MASTER == role_mgr_.get_role()) )
//          {
//            YYSYS_LOG(DEBUG, "The Get Request require consistency, ObiRole:%s RoleMgr:%s",
//                obi_role_.get_role_str(), role_mgr_.get_role_str());
//            ret = OB_NOT_MASTER;
//          }
          if ( !(ObUpsRoleMgr::MASTER == role_mgr_.get_role()) )
          {
            YYSYS_LOG(DEBUG, "The Get Request require consistency, RoleMgr:%s",
                role_mgr_.get_role_str());
            ret = OB_NOT_MASTER;
          }
          //mod 20150701:e
        }
      }

      if(OB_SUCCESS == ret)
      {
        if(OB_SUCCESS != (ret = ObNewScannerHelper::get_row_desc(get_param, true, row_desc)))
        {
          YYSYS_LOG(WARN, "get row desc fail:ret[%d]", ret);
        }
        else
        {
          new_scanner->set_row_desc(row_desc);
        }
      }

      if (OB_SUCCESS == ret)
      {
        thread_read_prepare();
        ret = table_mgr_.new_get(get_param, *new_scanner, start_time, timeout);
        if (OB_SUCCESS != ret)
        {
          YYSYS_LOG(WARN, "failed to get, ret=%d", ret);
        }
        FILL_TRACE_LOG("get from table mgr ret=%d", ret);
      }

      ret = response_data_(ret, *new_scanner, OB_NEW_GET_RESPONSE, MY_VERSION, req, channel_id, out_buff, start_time, &priority);
      OB_STAT_INC(UPDATESERVER, UPS_STAT_NL_GET_COUNT, 1);
      OB_STAT_INC(UPDATESERVER, UPS_STAT_NL_GET_TIMEU, GET_TRACE_TIMEU());
      FILL_TRACE_LOG("response scanner ret=%d", ret);
      PRINT_TRACE_LOG();

      thread_read_complete();

      return ret;
    }

    int ObUpdateServer::ups_get(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff,
        const int64_t start_time, const int64_t timeout, const int32_t priority)
    {
      int ret = OB_SUCCESS;
      ObGetParam *get_param_ptr = GET_TSI_MULT(ObGetParam, TSI_UPS_GET_PARAM_1);
      ObScanner *scanner_ptr = GET_TSI_MULT(ObScanner, TSI_UPS_SCANNER_1);

      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }

      if (OB_SUCCESS == ret)
      {
        if (NULL == get_param_ptr
            || NULL == scanner_ptr)
        {
          YYSYS_LOG(WARN, "get tsi fail, get_param=%p scanner=%p", get_param_ptr, scanner_ptr);
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          get_param_ptr->reset();
          scanner_ptr->reset();
        }
      }

      //CLEAR_TRACE_LOG();
      if (OB_SUCCESS == ret)
      {
        ret = get_param_ptr->deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          YYSYS_LOG(WARN, "deserialize get param error, ret=%d", ret);
        }
        FILL_TRACE_LOG("get param deserialize ret=%d", ret);
      }

      if (OB_SUCCESS == ret)
      {
        if (get_param_ptr->get_is_read_consistency())
        {
          //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//          if ( !(ObiRole::MASTER == obi_role_.get_role() && ObUpsRoleMgr::MASTER == role_mgr_.get_role()) )
//          {
//            YYSYS_LOG(DEBUG, "The Get Request require consistency, ObiRole:%s RoleMgr:%s",
//                obi_role_.get_role_str(), role_mgr_.get_role_str());
//            ret = OB_NOT_MASTER;
//          }
          if ( !(ObUpsRoleMgr::MASTER == role_mgr_.get_role()) )
          {
            YYSYS_LOG(DEBUG, "The Get Request require consistency, RoleMgr:%s",
                role_mgr_.get_role_str());
            ret = OB_NOT_MASTER;
          }
          //mod 20150701:e
        }
      }

      if (OB_SUCCESS == ret)
      {
        thread_read_prepare();
        ret = table_mgr_.get(*get_param_ptr, *scanner_ptr, start_time, timeout);
        if (OB_SUCCESS != ret)
        {
          YYSYS_LOG(WARN, "failed to get, err=%d", ret);
        }
        FILL_TRACE_LOG("get from table mgr ret=%d", ret);
      }

      if (NULL != scanner_ptr)
      {
        ret = response_data_(ret, *scanner_ptr, OB_GET_RESPONSE, MY_VERSION, req, channel_id, out_buff, start_time, &priority);
      }
      else
      {
        ret = response_result_(ret, OB_GET_RESPONSE, MY_VERSION, req, channel_id);
      }
      OB_STAT_INC(UPDATESERVER, UPS_STAT_NL_GET_COUNT, 1);
      OB_STAT_INC(UPDATESERVER, UPS_STAT_NL_GET_TIMEU, GET_TRACE_TIMEU());
      FILL_TRACE_LOG("response scanner ret=%d", ret);
      PRINT_TRACE_LOG();

      thread_read_complete();

      return ret;
    }

    template <class T>
    int ObUpdateServer::response_data_(int32_t ret_code, const T &data,
                                          int32_t cmd_type, int32_t func_version,
                                          easy_request_t* req, const uint32_t channel_id,
                                       common::ObDataBuffer& out_buff, const int64_t receive_ts, const int32_t* priority, const char *ret_string)
    {
      int ret = OB_SUCCESS;
      common::ObResultCode result_msg;
      result_msg.result_code_ = ret_code;
      if (NULL != ret_string)
      {
        result_msg.message_.assign_ptr(const_cast<char*>(ret_string), static_cast<int32_t>(strlen(ret_string) + 1));
      }
      ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "serialize result msg error, ret=%d", ret);
      }
      else if (NULL == eio_)
      {
        ret = OB_NOT_INIT;
      }
      else if (receive_ts > 0 && eio_->force_destroy_second > 0 && yysys::CTimeUtil::getTime() - receive_ts + RESPONSE_RESERVED_US > eio_->force_destroy_second * 1000000)
      {
        ret = OB_RESPONSE_TIME_OUT;
        YYSYS_LOG(ERROR, "pkt wait too long time, not send response: pkt_receive_ts=%ld, pkt_code=%d", receive_ts, cmd_type);
      }
      else
      {
        common::ObDataBuffer tmp_buffer = out_buff;
        ret = ups_serialize(data, out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          YYSYS_LOG(WARN, "serialize data error, ret=%d", ret);
          ret = OB_ERROR;
        }
        else
        {
          if (OB_NEW_GET_REQUEST == cmd_type || OB_GET_REQUEST == cmd_type)
          {
            OB_STAT_INC(UPDATESERVER, UPS_STAT_GET_BYTES, out_buff.get_position());
          }
          else if (OB_NEW_SCAN_REQUEST == cmd_type || OB_SCAN_REQUEST == cmd_type)
          {
            OB_STAT_INC(UPDATESERVER, UPS_STAT_SCAN_BYTES, out_buff.get_position());
          }
          ret = send_response(cmd_type, func_version, out_buff, req, channel_id);
          if (OB_SUCCESS != ret)
          {
            YYSYS_LOG(WARN, "failed to send scan response, ret=%d", ret);
            ret = OB_ERROR;
          }
          if (OB_SUCCESS == ret && NULL != priority && PriorityPacketQueueThread::LOW_PRIV == *priority)
          {
            low_priv_speed_control_(out_buff.get_position());
          }
        }
      }
      return ret;
    }

    int ObUpdateServer::response_fetch_param_(int32_t ret_code, const ObUpsFetchParam& fetch_param,
        const int64_t log_id, int32_t cmd_type, int32_t func_version,
        easy_request_t* req, const uint32_t channel_id,
        common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      common::ObResultCode result_msg;
      result_msg.result_code_ = ret_code;
      ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "serialize result msg error, ret=%d", ret);
        ret = OB_ERROR;
      }
      else
      {
        ret = fetch_param.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          YYSYS_LOG(WARN, "serialize fetch param error, ret=%d", ret);
          ret = OB_ERROR;
        }
        else
        {
          if (OB_SUCCESS == (ret = serialization::encode_i64(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position(), log_id)))
          {
            ret = send_response(cmd_type, func_version, out_buff, req, channel_id);
            if (OB_SUCCESS != ret)
            {
              YYSYS_LOG(WARN, "failed to send scan response, ret=%d", ret);
              ret = OB_ERROR;
            }
          }
          else
          {
            YYSYS_LOG(WARN, "fail to encode max log id . err=%d", ret);
          }
        }

      }
      return ret;
    }

    int ObUpdateServer::response_lease_(int32_t ret_code, const ObLease& lease,
        int32_t cmd_type, int32_t func_version,
        easy_request_t* req, const uint32_t channel_id,
        common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      common::ObResultCode result_msg;
      result_msg.result_code_ = ret_code;
      ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "serialize result msg error, ret=%d", ret);
        ret = OB_ERROR;
      }
      else
      {
        ret = lease.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          YYSYS_LOG(WARN, "serialize lease error, ret=%d", ret);
          ret = OB_ERROR;
        }
        else
        {
          ret = send_response(cmd_type, func_version, out_buff, req, channel_id);
          if (OB_SUCCESS != ret)
          {
            YYSYS_LOG(WARN, "failed to send scan response, ret=%d", ret);
            ret = OB_ERROR;
          }
        }
      }
      return ret;
    }

    int ObUpdateServer::ups_new_scan(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff,
        const int64_t start_time, const int64_t timeout, const int32_t priority)
    {
      int ret = OB_SUCCESS;
      ObScanParam scan_param_stack;
      ObScanParam *scan_param_ptr = GET_TSI_MULT(ObScanParam, TSI_UPS_SCAN_PARAM_1);
      ObScanParam &scan_param = (NULL == scan_param_ptr) ? scan_param_stack : *scan_param_ptr;
      common::ObResultCode result_msg;
      common::ObRowDesc row_desc;
      common::ObCellNewScanner *new_scanner = GET_TSI_MULT(ObCellNewScanner, TSI_UPS_NEW_SCANNER_1);


      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }

      if (OB_SUCCESS == ret)
      {
        if (NULL == new_scanner)
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          YYSYS_LOG(WARN, "fail to allocate mem for new scanner");
        }
        else
        {
          new_scanner->reuse();
        }
      }

      //CLEAR_TRACE_LOG();
      if (OB_SUCCESS == ret)
      {
        ret = scan_param.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          YYSYS_LOG(WARN, "deserialize scan param retor, ret=%d", ret);
        }
        FILL_TRACE_LOG("scan param deserialize ret=%d", ret);
      }

      if (OB_SUCCESS == ret)
      {
        if (scan_param.get_is_read_consistency())
        {
          //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//          if ( !(ObiRole::MASTER == obi_role_.get_role() && ObUpsRoleMgr::MASTER == role_mgr_.get_role()) )
//          {
//            YYSYS_LOG(INFO, "The Scan Request require consistency, ObiRole:%s RoleMgr:%s",
//                obi_role_.get_role_str(), role_mgr_.get_role_str());
//            ret = OB_NOT_MASTER;
//          }
          if ( !(ObUpsRoleMgr::MASTER == role_mgr_.get_role()) )
          {
            YYSYS_LOG(INFO, "The Scan Request require consistency, RoleMgr:%s",
                role_mgr_.get_role_str());
            ret = OB_NOT_MASTER;
          }
          //mod 20150701:e
        }
      }

      if(OB_SUCCESS == ret)
      {
        if(OB_SUCCESS != (ret = ObNewScannerHelper::get_row_desc(scan_param, row_desc)))
        {
          YYSYS_LOG(WARN, "get row desc fail:ret[%d]", ret);
        }
        else
        {
          new_scanner->set_row_desc(row_desc);
        }
      }

      if (OB_SUCCESS == ret)
      {
        thread_read_prepare();
        //���new_scanne��tsi����Ҫreset
        ret = table_mgr_.new_scan(scan_param, *new_scanner, start_time, timeout);
        if (OB_SUCCESS != ret)
        {
          YYSYS_LOG(WARN, "failed to scan, ret=%d", ret);
        }
        FILL_TRACE_LOG("scan from table mgr ret=%d", ret);
      }

      ret = response_data_(ret, *new_scanner, OB_NEW_SCAN_RESPONSE, MY_VERSION, req, channel_id, out_buff, start_time, &priority);
      OB_STAT_INC(UPDATESERVER, UPS_STAT_NL_SCAN_COUNT, 1);
      OB_STAT_INC(UPDATESERVER, UPS_STAT_NL_SCAN_TIMEU, GET_TRACE_TIMEU());
      FILL_TRACE_LOG("response scanner ret=%d", ret);
      PRINT_TRACE_LOG();

      thread_read_complete();

      return ret;
    }

    int ObUpdateServer::ups_scan(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff,
        const int64_t start_time, const int64_t timeout, const int32_t priority)
    {
      int err = OB_SUCCESS;
      ObScanParam *scan_param_ptr = GET_TSI_MULT(ObScanParam, TSI_UPS_SCAN_PARAM_1);
      ObScanner *scanner_ptr = GET_TSI_MULT(ObScanner, TSI_UPS_SCANNER_1);
      common::ObResultCode result_msg;

      if (version != MY_VERSION)
      {
        err = OB_ERROR_FUNC_VERSION;
      }

      if (OB_SUCCESS == err)
      {
        if (NULL == scan_param_ptr
            || NULL == scanner_ptr)
        {
          YYSYS_LOG(WARN, "get tsi fail, scan_param=%p scanner=%p", scan_param_ptr, scanner_ptr);
          err = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          scan_param_ptr->reset();
          scanner_ptr->reset();
        }
      }

      //CLEAR_TRACE_LOG();
      if (OB_SUCCESS == err)
      {
        err = scan_param_ptr->deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (OB_SUCCESS != err)
        {
          YYSYS_LOG(WARN, "deserialize scan param error, err=%d", err);
        }
        FILL_TRACE_LOG("scan param deserialize ret=%d", err);
      }

      if (OB_SUCCESS == err)
      {
        if (scan_param_ptr->get_is_read_consistency())
        {
          //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//          if ( !(ObiRole::MASTER == obi_role_.get_role() && ObUpsRoleMgr::MASTER == role_mgr_.get_role()) )
//          {
//            YYSYS_LOG(INFO, "The Scan Request require consistency, ObiRole:%s RoleMgr:%s",
//                obi_role_.get_role_str(), role_mgr_.get_role_str());
//            err = OB_NOT_MASTER;
//          }
          if ( !(ObUpsRoleMgr::MASTER == role_mgr_.get_role()) )
          {
            YYSYS_LOG(INFO, "The Scan Request require consistency, RoleMgr:%s",
                role_mgr_.get_role_str());
            err = OB_NOT_MASTER;
          }
          //mod 20150701:e
        }
      }

      if (OB_SUCCESS == err)
      {
        thread_read_prepare();
        err = table_mgr_.scan(*scan_param_ptr, *scanner_ptr, start_time, timeout);
        if (OB_SUCCESS != err)
        {
          YYSYS_LOG(WARN, "failed to scan, err=%d", err);
        }
        FILL_TRACE_LOG("scan from table mgr ret=%d", err);
      }
      if (NULL != scanner_ptr)
      {
        err = response_data_(err, *scanner_ptr, OB_SCAN_RESPONSE, MY_VERSION, req, channel_id, out_buff, start_time, &priority);
      }
      else
      {
        err = response_result_(err,  OB_SCAN_RESPONSE, MY_VERSION, req, channel_id);
      }
      OB_STAT_INC(UPDATESERVER, UPS_STAT_NL_SCAN_COUNT, 1);
      OB_STAT_INC(UPDATESERVER, UPS_STAT_NL_SCAN_TIMEU, GET_TRACE_TIMEU());
      FILL_TRACE_LOG("response scanner ret=%d", err);
      PRINT_TRACE_LOG();

      thread_read_complete();

      return err;
    }

    int ObUpdateServer::ups_get_bloomfilter(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      int64_t frozen_version = 0;
      TableBloomFilter table_bf;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      else if (OB_SUCCESS != (ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &frozen_version)))
      {
        YYSYS_LOG(WARN, "decode cur version fail ret=%d", ret);
      }
      else
      {
        ret = table_mgr_.get_frozen_bloomfilter(frozen_version, table_bf);
      }
      ret = response_data_(ret, table_bf, OB_UPS_GET_BLOOM_FILTER_RESPONSE, MY_VERSION, req, channel_id, out_buff);
      return ret;
    }

    int ObUpdateServer::ups_switch_skey()
    {
      int ret = OB_SUCCESS;
      int proc_ret = OB_SUCCESS;
      ObMutator mutator;
      if (OB_SUCCESS != (ret = skey_table_cache_.update_cur_skey(mutator)))
      {
        YYSYS_LOG(WARN, "build switch skey mutator fail ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = ups_rpc_stub_.send_mutator_apply(ups_master_, mutator, RPC_TIMEOUT)))
      {
        YYSYS_LOG(WARN, "send switch skey mutator apply fail ret=%d", ret);
      }
      else
      {
        YYSYS_LOG(INFO, "send switch skey mutator apply succ");
      }
      return proc_ret;
    }

    int ObUpdateServer::ups_freeze_memtable(const int32_t version, ObPacket *packet_orig, common::ObDataBuffer& out_buff, const int pcode)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      TableMgr::FreezeType freeze_type = TableMgr::AUTO_TRIG;
      //add peiouya [MultiUPS] [UPS_SYNC_Frozen] 20150521:b
      int64_t limit_version = 0;
      //add 20150521:e
      if (OB_UPS_MINOR_FREEZE_MEMTABLE == pcode)
      {
        freeze_type = TableMgr::FORCE_MINOR;
      }
      else if (OB_FREEZE_MEM_TABLE == pcode
          || OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE == pcode)
      {
        freeze_type = TableMgr::FORCE_MAJOR;
      }
      else if (OB_UPS_ASYNC_AUTO_FREEZE_MEMTABLE == pcode)
      {
        freeze_type = TableMgr::AUTO_TRIG;
      }
      else if (OB_UPS_MINOR_LOAD_BYPASS == pcode)
      {
        freeze_type = TableMgr::MINOR_LOAD_BYPASS;
      }
      else if (OB_UPS_MAJOR_LOAD_BYPASS == pcode)
      {
        freeze_type = TableMgr::MAJOR_LOAD_BYPASS;
      }
      //add peiouya [MultiUPS] [UPS_SYNC_Frozen] 20150521:b
      else if (OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE_ACCORD_HB == pcode)
      {
        freeze_type = TableMgr::FORCE_MAJOR;
        //mod peiouya [MultiUPS] [UPS_Manage_Function] 20150615:b
        //limit_version = last_frozen_version_;
        limit_version = to_be_frozen_version_;
        //mod 20150615:e
      }
      //add 20150521:e
      uint64_t frozen_version = 0;
      bool report_version_changed = false;
      //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//      if (!(ObiRole::MASTER == obi_role_.get_role()
//            && ObUpsRoleMgr::MASTER == role_mgr_.get_role()
//            && ObUpsRoleMgr::ACTIVE == role_mgr_.get_state()
//            && is_lease_valid()))
      if (!(ObUpsRoleMgr::MASTER == role_mgr_.get_role()
            && ObUpsRoleMgr::ACTIVE == role_mgr_.get_state()
            && is_lease_valid()))
      //mod 20150701:e
      {
        ret = OB_NOT_MASTER;
        YYSYS_LOG(WARN, "not master:is_lease_valie=%s", STR_BOOL(is_lease_valid()));
      }
      //mod peiouya [MultiUPS] [UPS_SYNC_Frozen] 20150521:b
      //else if (OB_SUCCESS != (ret = table_mgr_.freeze_memtable(freeze_type, frozen_version, report_version_changed, packet_orig)))
      else if (OB_SUCCESS != (ret = table_mgr_.freeze_memtable(freeze_type, frozen_version, report_version_changed, packet_orig, limit_version)))
      {
        minor_freeze_done_ = UPS_FROZEN_FAIL;
        if (OB_NOT_NEED_FROZEN != ret)
        {
          YYSYS_LOG(WARN, "freeze memtable fail ret=%d", ret);
        }
        //add peiouya [MultiUPS] [UPS_SYNC_Frozen] 20150810:b
        else
        {
          major_freeze_accord_hb_guard_.done ();
        }
        //add 20150810:e
      }
      //mod 20150521:e
      else
      {
          //add[570] use heartbeat report new freezed version
          //to fast sync RS/MS/CS's last_freezed_version
          if(freeze_type == TableMgr::FORCE_MAJOR || freeze_type == TableMgr::AUTO_TRIG){
              ObMsgUpsHeartbeatResp hb_res;
              set_heartbeat_res(hb_res);
              ret = ups_rpc_stub_.renew_lease(root_server_, hb_res);
              if(OB_SUCCESS != ret)
              {
                  YYSYS_LOG(WARN, "fail to send freezed version to rootserver. ret=%d", ret);
              }
          }


        if (report_version_changed
            && OB_UPS_MINOR_LOAD_BYPASS != pcode
            && OB_UPS_MAJOR_LOAD_BYPASS != pcode) // ��·����freeze���������㱨 ��Ҫ�ȼ�������ٻ㱨
        {
          //submit_report_freeze();  //del peiouya [UPS_SYNC_Frozen] 20150528
        }
        submit_handle_frozen();
        minor_freeze_done_ = UPS_FROZEN;
        //add peiouya [MultiUPS] [UPS_SYNC_Frozen] 20150810:b
        major_freeze_accord_hb_guard_.done ();
        //add 20150810:e
      }
      if ((OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE != pcode
            && OB_UPS_ASYNC_AUTO_FREEZE_MEMTABLE != pcode
            && OB_UPS_MINOR_LOAD_BYPASS != pcode
            && OB_UPS_MAJOR_LOAD_BYPASS != pcode
            //add peiouya [MultiUPS] [UPS_SYNC_Frozen] 20150521:b
            && OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE_ACCORD_HB != pcode)
            //add 20150521:e
          || (OB_UPS_MINOR_LOAD_BYPASS == pcode && OB_SUCCESS != ret)
          || (OB_UPS_MAJOR_LOAD_BYPASS == pcode && OB_SUCCESS != ret))
      {
        // �����첽������ҪӦ��
        ret = response_data_(ret, frozen_version, pcode + 1, MY_VERSION,
                             packet_orig->get_request(), packet_orig->get_channel_id(), out_buff);
      }
      return ret;
    }

    int ObUpdateServer::ups_store_memtable(const int32_t version, common::ObDataBuffer &in_buf,
        easy_request_t* req, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      int64_t store_all = 0;
      if (OB_SUCCESS != (ret = serialization::decode_vi64(in_buf.get_data(), in_buf.get_capacity(), in_buf.get_position(), &store_all)))
      {
        YYSYS_LOG(WARN, "decode store_all flag fail ret=%d", ret);
      }
      else
      {
        YYSYS_LOG(INFO, "store memtable store_all=%ld", store_all);
      }
      response_result_(ret, OB_UPS_STORE_MEM_TABLE_RESPONSE, MY_VERSION, req, channel_id);
      if (OB_SUCCESS == ret)
      {
        table_mgr_.store_memtable(0 != store_all);
      }
      return ret;
    }

    int ObUpdateServer::ups_handle_frozen()
    {
      int ret = OB_SUCCESS;
      table_mgr_.update_merged_version(ups_rpc_stub_, root_server_, config_.resp_root_timeout);
      bool force = false;
      table_mgr_.erase_sstable(force);
      bool store_all = false;
      table_mgr_.store_memtable(store_all);
      table_mgr_.log_table_info();
      return ret;
    }

    //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//    int ObUpdateServer::check_keep_alive_()
//    {
//      int err = OB_SUCCESS;
//      if (ObUpsRoleMgr::FATAL == role_mgr_.get_state())
//      {
//        YYSYS_LOG(DEBUG, "enter fatal state");
//      }
//      else if (ObiRole::SLAVE == obi_role_.get_role()
//          || ObUpsRoleMgr::SLAVE == role_mgr_.get_role())
//      {
//        if (ObUpsRoleMgr::MASTER == role_mgr_.get_role()
//            && STANDALONE_SLAVE == obi_slave_stat_)
//        {
//          YYSYS_LOG(DEBUG, "STANDALONE slave_master, need not check keep alive.");
//        }
//        else
//        {
//          ObServer null_server;
//          int64_t cur_time_us = yysys::CTimeUtil::getTime();
//          int64_t last_keep_alive_time = log_mgr_.get_last_receive_log_time();
//          int64_t last_replay_time = log_mgr_.get_delay_stat().get_last_replay_time_us();
//          YYSYS_LOG(DEBUG, "slave check keep_alive, cur_time=%ld, last_keep_alive_time=%ld",
//                    cur_time_us, last_keep_alive_time);
//          if (last_keep_alive_time + keep_alive_valid_interval_ < cur_time_us)
//          {
//            if (last_keep_alive_time != 0)
//            {
//              YYSYS_LOG(WARN, "keep_alive msg timeout, last_time = %ld, cur_time = %ld, duration_time = %ld", last_keep_alive_time,  cur_time_us, keep_alive_valid_interval_);
//              if (REACH_TIME_INTERVAL_RANGE(5L * 60L * 1000000L, 60L * 60L * 1000000L))
//              {
//                YYSYS_LOG(ERROR, "[PER_5_MIN] keep_alive msg timeout, last_time = %ld, cur_time = %ld, duration_time = %ld", last_keep_alive_time,  cur_time_us, keep_alive_valid_interval_);
//              }
//              if (ObUpsRoleMgr::ACTIVE == role_mgr_.get_state())
//              {
//                YYSYS_LOG(WARN, "slave_ups can't connect with master_ups. set state to REPLAYING_LOG");
//                role_mgr_.set_state(ObUpsRoleMgr::REPLAYING_LOG);
//              }
//            }
//            else
//            {
//              YYSYS_LOG(DEBUG, "ups neet register to master_ups.");
//            }
//          }
//          if (last_keep_alive_time + config_.keep_alive_reregister_timeout < cur_time_us)
//          {
//            //register to master_ups
//            if (last_keep_alive_time + keep_alive_valid_interval_ >= cur_time_us)
//            {
//              YYSYS_LOG(WARN, "keep_alive msg timeout, last_time = %ld, cur_time = %ld, duration_time = %ld", last_keep_alive_time,  cur_time_us, keep_alive_valid_interval_);
//            }
//            if (ObUpsRoleMgr::MASTER == role_mgr_.get_role())
//            {
//              err = ups_rpc_stub_.get_inst_master_ups(root_server_, ups_inst_master_, config_.paxos_id, DEFAULT_NETWORK_TIMEOUT);
//              if (OB_SUCCESS != err)
//              {
//                YYSYS_LOG(WARN, "fail to get inst_master_ups. err=%d", err);
//              }
//              else
//              {
//                YYSYS_LOG(INFO, "get master_master_ups addr = %s", ups_inst_master_.to_cstring());
//                if (ups_inst_master_ == self_addr_)
//                {
//                  YYSYS_LOG(WARN, "inst_master == self_addr[%s], maybe NOT set_obi_role", to_cstring(self_addr_));
//                }
//                else
//                {
//                  err = register_to_master_ups(ups_inst_master_);
//                  if (OB_SUCCESS != err)
//                  {
//                    YYSYS_LOG(WARN, "fail to register to inst_master_ups. err=%d, inst_master_ups addr=%s", err, ups_inst_master_.to_cstring());
//                  }
//                  else
//                  {
//                    YYSYS_LOG(INFO, "register to inst_master_ups succ. inst_master_ups=%s", ups_inst_master_.to_cstring());
//                  }
//                }
//              }
//            }
//            else
//            {
//              if (null_server == ups_master_)
//              {}
//              else if (self_addr_ == ups_master_)
//              {}
//              else
//              {
//                err = register_to_master_ups(ups_master_);
//                if (OB_SUCCESS != err)
//                {
//                  YYSYS_LOG(WARN, "fail to register to master_ups. err=%d", err);
//                }
//                else
//                {
//                  YYSYS_LOG(INFO, "register to master_ups succ. master_ups=%s", ups_master_.to_cstring());
//                }
//              }
//            }
//          }
//          else if (last_replay_time + keep_alive_valid_interval_ < cur_time_us
//                   && ObUpsRoleMgr::MASTER == role_mgr_.get_role())
//          {
//            if (REACH_TIME_INTERVAL_RANGE(5L * 60L * 1000000L, 60L * 60L * 1000000L))
//            {
//              YYSYS_LOG(ERROR, "[PER_5_MIN] keep_alive msg timeout, last_replay_time = %ld, cur_time = %ld, duration_time = %ld", last_replay_time,  cur_time_us, keep_alive_valid_interval_);
//            }
//            if (OB_SUCCESS != (err = ups_rpc_stub_.get_inst_master_ups(root_server_, ups_inst_master_, config_.paxos_id, DEFAULT_NETWORK_TIMEOUT)))
//            {
//              YYSYS_LOG(WARN, "get inst_master_ups failed, err=%d", err);
//            }
//            else
//            {
//              YYSYS_LOG(INFO, "get inst_master_ups: %s", to_cstring(ups_inst_master_));
//            }
//          }
//        }
//      }
//      else
//      {
//        //do nothing;
//      }
//      check_keep_alive_guard_.done();
//      return err;
//    }
    int ObUpdateServer::check_keep_alive_()
    {
      int err = OB_SUCCESS;
      if (ObUpsRoleMgr::FATAL == role_mgr_.get_state())
      {
        YYSYS_LOG(DEBUG, "enter fatal state");
      }
      else if (ObUpsRoleMgr::SLAVE == role_mgr_.get_role())
      {
        ObServer null_server;
        int64_t cur_time_us = yysys::CTimeUtil::getTime();
        int64_t last_keep_alive_time = log_mgr_.get_last_receive_log_time();
        YYSYS_LOG(DEBUG, "slave check keep_alive, cur_time=%ld, last_keep_alive_time=%ld",
                  cur_time_us, last_keep_alive_time);
        if (last_keep_alive_time + keep_alive_valid_interval_ < cur_time_us)
        {
          if (last_keep_alive_time != 0)
          {
            YYSYS_LOG(WARN, "keep_alive msg timeout, last_time = %ld, cur_time = %ld, duration_time = %ld", last_keep_alive_time,  cur_time_us, keep_alive_valid_interval_);
            if (REACH_TIME_INTERVAL_RANGE(5L * 60L * 1000000L, 60L * 60L * 1000000L))
            {
              YYSYS_LOG(ERROR, "[PER_5_MIN] keep_alive msg timeout, last_time = %ld, cur_time = %ld, duration_time = %ld", last_keep_alive_time,  cur_time_us, keep_alive_valid_interval_);
            }
            if (ObUpsRoleMgr::ACTIVE == role_mgr_.get_state())
            {
              YYSYS_LOG(WARN, "slave_ups can't connect with master_ups. set state to REPLAYING_LOG");
              //role_mgr_.set_state(ObUpsRoleMgr::REPLAYING_LOG);
              log_mgr_.set_state_as_replaying_log();
            }
          }
          else
          {
            YYSYS_LOG(DEBUG, "ups neet register to master_ups.");
          }
        }
        if (last_keep_alive_time + config_.keep_alive_reregister_timeout < cur_time_us)
        {
          //register to master_ups
          if (last_keep_alive_time + keep_alive_valid_interval_ >= cur_time_us)
          {
            YYSYS_LOG(WARN, "keep_alive msg timeout, last_time = %ld, cur_time = %ld, duration_time = %ld", last_keep_alive_time,  cur_time_us, keep_alive_valid_interval_);
          }
          if (null_server == ups_master_)
          {}
          else if (self_addr_ == ups_master_)
          {}
          else
          {
            err = register_to_master_ups(ups_master_);
            if (OB_SUCCESS != err)
            {
              //mod peiouya [MultiUPS] [DELETE_OBI] 20150810:b
              //note: CDI_multiUPS_duht.xls:sheet 1
              //YYSYS_LOG(WARN, "fail to register to master_ups. err=%d", err);
              YYSYS_LOG(WARN, "fail to register to master_ups. err=%d, master_ups=%s", err, ups_master_.to_cstring());
              //mod 20150810:e
            }
            else
            {
              YYSYS_LOG(INFO, "register to master_ups succ. master_ups=%s", ups_master_.to_cstring());
            }
          }
        }
      }
      else
      {
        //do nothing
      }
      check_keep_alive_guard_.done();
      return err;
    }
    //mod 20150701:e

    //add peiouya [MultiUPS] [DELETE_OBI] 20150701:b
    int ObUpdateServer::get_sys_ups_master_()
    {
      int ret =OB_SUCCESS;

      if (ObUpsRoleMgr::MASTER == role_mgr_.get_role())
      {
        //mod peiouya [MultiUPS] [DELETE_OBI] 20150810:b
        //note: CDI_multiUPS_duht.xls:sheet 1
        //if (OB_SUCCESS != (ret = ups_rpc_stub_.get_inst_master_ups(root_server_, sys_ups_master_, 0, DEFAULT_NETWORK_TIMEOUT)))
        //{
        //  sys_ups_master_.reset();
        //  YYSYS_LOG(WARN, "fail to get sys_table_master_ups. ret=%d", ret);  //add peiouya [MultiUPS] [DELETE_OBI] 20150810
        //}
        if (0 == config_.paxos_id)
        {
          sys_ups_master_ = self_addr_;
        }
        else if (OB_SUCCESS != (ret = ups_rpc_stub_.get_inst_master_ups(root_server_, sys_ups_master_, 0, DEFAULT_NETWORK_TIMEOUT)))
        {
          sys_ups_master_.reset();
          YYSYS_LOG(WARN, "fail to get sys_table_master_ups from rs:%s. ret=%d", root_server_.to_cstring(), ret);  //add peiouya [MultiUPS] [DELETE_OBI] 20150810
        }
        else
        {
          //do nothing
        }
        //mod 20150810:e
      }
      else
      {
        //mod peiouya [MultiUPS] [DELETE_OBI] 20150810:b
        //note: CDI_multiUPS_duht.xls:sheet 1
        ////do nothing
        sys_ups_master_.reset ();
        //mod 20150810:e
      }
      get_sys_ups_master_guard_.done();
      return ret;
    }
    //add 20150701:e

    int ObUpdateServer::grant_keep_alive_()
    {
      int err = OB_SUCCESS;
      int64_t log_seq_id = 0;
      if (ObUpsRoleMgr::FATAL == role_mgr_.get_state())
      {
        YYSYS_LOG(DEBUG, "enter FATAL state.");
      }
      else
      {
        //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//        if (ObiRole::MASTER != obi_role_.get_role()
//            || ObUpsRoleMgr::MASTER != role_mgr_.get_role()
//            || ObUpsRoleMgr::ACTIVE != role_mgr_.get_state())
//        {
//          YYSYS_LOG(DEBUG, "ups not master.obi_role=%s, role=%s, state=%s", obi_role_.get_role_str(), role_mgr_.get_role_str(), role_mgr_.get_state_str());
//        }
        if (ObUpsRoleMgr::MASTER != role_mgr_.get_role()
            || ObUpsRoleMgr::ACTIVE != role_mgr_.get_state())
        {
          YYSYS_LOG(DEBUG, "ups not master, role=%s, state=%s", role_mgr_.get_role_str(), role_mgr_.get_state_str());
        }
        //mod 20150701:e
        else if (!is_lease_valid())
        {
          YYSYS_LOG(DEBUG, "lease is invalid");
        }
        else if (log_mgr_.get_last_flush_log_time() + config_.keep_alive_interval > yysys::CTimeUtil::getTime())
        {
          YYSYS_LOG(DEBUG, "log_mgr.last_flush_log_time=%ld, keep_alive_interval=%ld, no need write NOP again",
                    log_mgr_.get_last_flush_log_time(), (int64_t)config_.keep_alive_interval);
        }
        else if (OB_SUCCESS != (err = submit_fake_write_for_keep_alive()))
        {
          YYSYS_LOG(WARN, "submit_fake_write()=>%d", err);
        }

        if (OB_SUCCESS != err)
        {}
        else if (OB_SUCCESS != (err = log_mgr_.get_max_log_seq_replayable(log_seq_id)))
        {
          YYSYS_LOG(WARN, "fail to get max log seq. err = %d", err);
        }
        else if (OB_SUCCESS != (err = register_to_rootserver(log_seq_id)))
        {
          YYSYS_LOG(WARN, "fail to send keep_alive to rs. err=%d", err);
        }
      }
      grant_keep_alive_guard_.done();
      return err;
    }

    int ObUpdateServer::register_to_rootserver(const uint64_t log_seq_id)
    {
      int err = OB_SUCCESS;
      ObMsgUpsRegister msg_register;
      set_register_msg(log_seq_id, msg_register);
      int64_t renew_reserved_us = 0;
      //add shili [MUTIUPS] [START��UPS]  20150427:b
	  msg_register.paxos_id_ = (int64_t)config_.paxos_id;
	  //add:e
      int64_t cluster_id = OB_ALL_CLUSTER_FLAG;

      //add pangtianze [Paxos bugfix:ups may regist failed after all rs rstart] 20170811:b
      yysys::CThreadGuard guard(&regist_rs_mutex_);
      //add:e
      //add pangtianze [Paxos rs_election] 20150708:b
      common::ObServer current_rs; //current rs for register
      rs_mgr_.init_current_idx();
      current_rs = rs_mgr_.get_current_rs();
      //add:e
      //mod pangtianze [Paxos rs_electon] 20170228:b
      /*err = ups_rpc_stub_.ups_register(root_server_, msg_register,
                                       renew_reserved_us, cluster_id,
                                       DEFAULT_NETWORK_TIMEOUT);*/
      err = ups_rpc_stub_.ups_register(current_rs, msg_register,
                                       renew_reserved_us, cluster_id,
                                       DEFAULT_NETWORK_TIMEOUT);
      //mod:e
      if (OB_SUCCESS != err && OB_ALREADY_REGISTERED != err)
      {
        //mod pangtianze [Paxos rs_election] 20170329:b
        //YYSYS_LOG(WARN, "fail to register to rootserver. err = %d", err);
        YYSYS_LOG(WARN, "fail to register to rootserver=%s. err = %d", current_rs.to_cstring(), err);
        //mod:e
        //add pangtianze[paxos rs_election]
        if(OB_UPS_MASTER_EXISTS == err)
        {
            int64_t cur_time_us = yysys::CTimeUtil::getTime();
            lease_expire_time_us_ = cur_time_us;
            YYSYS_LOG(ERROR,"ups master has been changed, err=%d", err);
        }
        //add:e
		 //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160306:b
        /*Exp: paxos group, current ups belongs to, is offline, so current ups should be killed.
               But there is some inc date unmerged perhaps. Be carefully.*/
        if (OB_CURRENT_PAXOS_GROUP_OFFLINE == err)
        {
          YYSYS_LOG(ERROR, "paxos group:[%ld], current ups register to, has been taken offline, kill self, ret=%d",
                    (int64_t)config_.paxos_id, err);
          timer_.destroy();
          kill_self();
        }
        else if (OB_CURRENT_PAXOS_HAS_ENOUGH_UPS == err)
        {
          YYSYS_LOG(ERROR, "paxos group:[%ld] has enough ups, kill self, ret=%d",
                    (int64_t)config_.paxos_id, err);
          timer_.destroy();
          kill_self();
        }
        //add 20160306:e
        //add liuzy [MultiUPS] [take_cluster_offline_interface] 20160325:b
        else if (OB_CURRENT_CLUSTER_OFFLINE == err)
        {
          YYSYS_LOG(ERROR, "cluster:[%ld], current ups register to, has been taken offline, kill self, ret=%d",
                    (int64_t)config_.cluster_id, err);
          timer_.destroy();
          kill_self();
        }
        //add 20160325:e
		else		
        {
		  //add pangtianze [Paxos rs_election] 20170329:b
          current_rs = rs_mgr_.next_rs();
          rs_mgr_.set_master_rs(current_rs);
          YYSYS_LOG(INFO, "will regist to next rootserver=%s", current_rs.to_cstring());        
          //add:e
		}
      }
      else
      {
        //add pangtianze [Paxos rs_election] 20170228:b
        if (OB_ALREADY_REGISTERED != err)
        {
           set_root_server(current_rs);
           rs_mgr_.set_master_rs(current_rs);
        }
        //add:e
        //del lbzhong [Paxos Cluster.Balance] 201607011:b
        /*if (cluster_id > 0)
        {
          config_.cluster_id = cluster_id;
        }*/
        //del:e
        if (0 == ups_renew_reserved_us_)
        {
          ups_renew_reserved_us_ = renew_reserved_us;
        }
        err = OB_SUCCESS;
      }
      return err;
    }

    void ObUpdateServer::set_register_msg(const uint64_t log_seq_id, ObMsgUpsRegister &msg_register)
    {
      msg_register.log_seq_num_ = log_seq_id;
      msg_register.inner_port_ = static_cast<int32_t>(config_.inner_port);
      msg_register.addr_.set_ipv4_addr(self_addr_.get_ipv4(), self_addr_.get_port());
      //add lbzhong [Paxos Cluster.Balance] 201607021:b
      msg_register.addr_.cluster_id_ = static_cast<int32_t>(config_.cluster_id);
      //add:e

      get_package_and_git(msg_register.server_version_, sizeof(msg_register.server_version_));
      if (ObUpsRoleMgr::MASTER == role_mgr_.get_role())
      {
        msg_register.lease_ = lease_expire_time_us_;
      }
      else
      {
         msg_register.lease_ = 0;
      }
      //mod liuzy [MultiUPS] [take_paxos_offline_interface] 20160229:b
//      msg_register.last_frozen_version_ = static_cast<int64_t>(last_frozen_version);
      msg_register.last_frozen_version_ = static_cast<int64_t>(table_mgr_.get_cur_major_version()) - 1;
      //mod 20160229:e
      //add 20150521:e
    }

    int ObUpdateServer::check_lease_()
    {
      int err = OB_SUCCESS;
      int64_t cur_time_us = yysys::CTimeUtil::getTime();
      if (ObUpsRoleMgr::FATAL == role_mgr_.get_state())
      {
        YYSYS_LOG(DEBUG, "enter fatal state");
      }
      else if (lease_expire_time_us_ < cur_time_us)
      {
        if (0 != lease_expire_time_us_)
        {
          YYSYS_LOG(ERROR, "lease timeout, need reregister to rootserver. lease=%ld, cur_time=%ld",
              lease_expire_time_us_, cur_time_us);

          //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//          if (ObUpsRoleMgr::MASTER == role_mgr_.get_role()
//              && ObiRole::MASTER == obi_role_.get_role())
//          {
//            err = master_switch_to_slave(false, true);
//            if (OB_SUCCESS == err)
//            {
//              YYSYS_LOG(WARN, "master_master_ups lease timeout, change to master_slave");
//            }
//            else
//            {
//              YYSYS_LOG(ERROR, "master_master_ups lease timeout, change to master_slave failed!, err=%d", err);
//            }
//          }
          if (ObUpsRoleMgr::MASTER == role_mgr_.get_role())
          {
            err = master_switch_to_slave(true);
            if (OB_SUCCESS == err)
            {
              YYSYS_LOG(WARN, "master_ups lease timeout, change to slave");
            }
            else
            {
              YYSYS_LOG(ERROR, "master_ups lease timeout, change to slave failed!, err=%d", err);
            }
          }
          //mod 20150701:e
          //del peiouya [MultiUPS] [DELETE_OBI] 20150810
          //UNUSED branch.delete it.
          /*
          else if (ObUpsRoleMgr::MASTER == role_mgr_.get_role())
          {
            //mod peiouya [MultiUPS] [DELETE_OBI] 20150723:b
            ////mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
            ////err = slave_change_role(false, true);
            //err = slave_change_role(true);
            ////mod 20150701:e
            err = switch_to_master();
            //mod 20150723:e
            if (OB_SUCCESS != err)
            {
              YYSYS_LOG(WARN, "ups lease timetout, change role failed!");
            }
          }
          */
          //del 20150810:e
        }


        //mod wangjiahao [Paxos ups_replication_tmplog] 20150721 :b
        int64_t log_id = 0;
        if(OB_SUCCESS != (err = log_mgr_.get_max_log_seq_replayable(log_id)))
        {
          YYSYS_LOG(WARN, "fail to get max_log_seq_replayable. err=%d", err);
        }
        else if(OB_SUCCESS != (err = register_to_rootserver(log_id)))
        {
          YYSYS_LOG(WARN, "fail to register to rootserver. err=%d", err);
        }
        //mod :e
      }
      else if (lease_expire_time_us_ - cur_time_us < ups_renew_reserved_us_)
      {
        YYSYS_LOG(WARN, "lease will be timeout, retry to send renew lease to rootserver. \
            lease_time:%ld, cur_time:%ld, remain lease[%ld] should big than %ld",
            lease_expire_time_us_, cur_time_us, lease_expire_time_us_ - cur_time_us, ups_renew_reserved_us_);
        ObMsgUpsHeartbeatResp hb_res;
        set_heartbeat_res(hb_res);
        err = ups_rpc_stub_.renew_lease(root_server_, hb_res);
        if (OB_SUCCESS != err)
        {
          YYSYS_LOG(WARN, "fail to send renew_lease to rootserver. err=%d", err);
        }
      }
      //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//      else if (ObiRole::MASTER != obi_role_.get_role()
//          || ObUpsRoleMgr::MASTER != role_mgr_.get_role()
//          || ObUpsRoleMgr::ACTIVE != role_mgr_.get_state())
//      {
//        //YYSYS_LOG(DEBUG, "ups not master.obi_role=%s, role=%s, state=%s", obi_role_.get_role_str(), role_mgr_.get_role_str(), role_mgr_.get_state_str());
//      }
      else if (ObUpsRoleMgr::MASTER != role_mgr_.get_role()
          || ObUpsRoleMgr::ACTIVE != role_mgr_.get_state())
      {
        //YYSYS_LOG(DEBUG, "ups not master.obi_role=%s, role=%s, state=%s", obi_role_.get_role_str(), role_mgr_.get_role_str(), role_mgr_.get_state_str());
      }
      //mod 20150701:e
      else if (schema_version_ > table_mgr_.get_schema_mgr().get_version()
               && OB_SUCCESS != (err = submit_update_schema()))
      {
        YYSYS_LOG(ERROR, "submit_udpate_schema()=>%d", err);
      }
      check_lease_guard_.done();
      return err;
    }
    void ObUpdateServer::set_heartbeat_res(ObMsgUpsHeartbeatResp &hb_res)
    {
      hb_res.addr_.set_ipv4_addr(self_addr_.get_ipv4(), self_addr_.get_port());
      //add lbzhong [Paxos Cluster.Balance] 201607011:b
      hb_res.addr_.cluster_id_ = self_addr_.cluster_id_;
      //add:e
      bool sync = false;
      if (ObUpsRoleMgr::ACTIVE == role_mgr_.get_state())
      {
        sync = true;
      }
      hb_res.status_ = (true == sync) ? ObMsgUpsHeartbeatResp::SYNC : ObMsgUpsHeartbeatResp::NOTSYNC;
      //del peiouya [MultiUPS] [DELETE_OBI] 20150701:b
      //hb_res.obi_role_.set_role(settled_obi_role_.get_role());
      //del 20150701:e
      //add peiouya [MultiUPS] [UPS_Manage_Function] 20150521:b
      //mod peiouya [MultiUPS] [UPS_RESTART_BUG_FIX] 20150703:b
      //uint64_t last_frozen_version = 0;
      uint64_t last_frozen_version = OB_INVALID_VERSION;
      //mod 20150703:e
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = table_mgr_.get_last_frozen_memtable_version(last_frozen_version)))
      {
        YYSYS_LOG(WARN, "fail to get last frozen version from table_mgr. ret = %d", ret);
        hb_res.is_active_ = false;
        //mod peiouya [MultiUPS] [UPS_RESTART_BUG_FIX] 20150703:b
        //hb_res.last_frozen_version_ = 0;
        hb_res.last_frozen_version_ = OB_INVALID_VERSION;
        //mod 20150703:e
      }
      else
      {
        hb_res.last_frozen_version_ = static_cast<int64_t>(last_frozen_version);
        if (ObUpsRoleMgr::MASTER == role_mgr_.get_role() && ObUpsRoleMgr::ACTIVE == role_mgr_.get_state())
        {
          hb_res.is_active_ = true;
          //add peiouya [MultiUPS] [PARTITION_LOCK_FUCTION] 20150601:b
          if (table_mgr_.need_lock_partition())
          {
            hb_res.partition_lock_flag_ = true;
            //del hongchen 20170819:b
            //this branch is too strictly, so delete it because other logical hold it
            ////add peiouya [MultiUPS] [UPS_SYNC_Frozen] 20150810:b
            //trans_executor_.get_session_mgr().disable_start_write_session();
            ////add 20150810:e
            //del hongchen 20170819:e
          }
          else
          {
            hb_res.partition_lock_flag_ = false;
          }
          //add 20150601:e
        }
        else
        {
          hb_res.is_active_ = false;
          hb_res.partition_lock_flag_ = false;  //bug fix
        }
      }
      //add 20150521:e
	  //add chujiajia [Paxos rs_election] 20151229:b
      hb_res.quorum_scale_ = (int64_t)(config_.quorum_scale);
      //add:e
      hb_res.minor_freeze_stat_ = minor_freeze_done_;
    }

    //add peiouya [MultiUPS] [DELETE_OBI] 20150701:b
    int ObUpdateServer::submit_get_sys_ups_master()
    {
      return submit_async_task_once_(get_sys_ups_master_guard_, OB_UPS_ASYNC_GET_SYS_UPS_MASTER, read_thread_queue_, read_task_queue_size_);
    }
    //add 20150701:e

    int ObUpdateServer::submit_check_keep_alive()
    {
      return submit_async_task_once_(check_keep_alive_guard_, OB_UPS_ASYNC_CHECK_KEEP_ALIVE, read_thread_queue_, read_task_queue_size_);
    }

    int ObUpdateServer::submit_lease_task()
    {
      return submit_async_task_once_(check_lease_guard_, OB_UPS_ASYNC_CHECK_LEASE, lease_thread_queue_, lease_task_queue_size_);
    }

    int ObUpdateServer::submit_grant_keep_alive()
    {
      return submit_async_task_once_(grant_keep_alive_guard_, OB_UPS_ASYNC_GRANT_KEEP_ALIVE, read_thread_queue_, read_task_queue_size_);
    }

    int ObUpdateServer::submit_fake_write_for_keep_alive()
    {
      return submit_async_task_(OB_FAKE_WRITE_FOR_KEEP_ALIVE, write_thread_queue_, write_task_queue_size_);
    }

    int ObUpdateServer::submit_handle_frozen()
    {
      return submit_async_task_(OB_UPS_ASYNC_HANDLE_FROZEN, store_thread_, store_thread_queue_size_);
    }

    //del peiouya [MultiUPS] [UPS_SYNC_Frozen] 20150528:b
    /*
    int ObUpdateServer::submit_report_freeze()
    {
      return submit_async_task_(OB_UPS_ASYNC_REPORT_FREEZE, read_thread_queue_, read_task_queue_size_);
    }
    */
    //del 20150528:e

    int ObUpdateServer::submit_replay_commit_log()
    {
      YYSYS_LOG(INFO, "submit replay commit log task");
      return submit_async_task_(OB_UPS_ASYNC_REPLAY_LOG, read_thread_queue_, read_task_queue_size_);
    }
    //add wangjiahao [Paxos] 20150719 :b
    int ObUpdateServer::submit_replay_tmp_log()
    {
      YYSYS_LOG(INFO, "submit replay tmp log task");
      return submit_async_task_(OB_UPS_ASYNC_REPLAY_TMP_LOG, read_thread_queue_, read_task_queue_size_);
    }
    //add :e
    int ObUpdateServer::submit_prefetch_remote_log(ObPrefetchLogTaskSubmitter::Task& task)
    {
      ObDataBuffer in_buf;
      in_buf.set_data((char*)&task, sizeof(task));
      in_buf.get_position() = sizeof(task);
      return submit_async_task_(OB_PREFETCH_LOG, read_thread_queue_, read_task_queue_size_, &in_buf);
    }

    int ObUpdateServer::submit_switch_schema(CommonSchemaManagerWrapper& schema_mgr)
    {
      int err = OB_SUCCESS;
      ThreadSpecificBuffer::Buffer *my_buffer = my_thread_buffer_.get_buffer();
      ObDataBuffer in_buf;
      if (NULL == my_buffer)
      {
        err = OB_ALLOCATE_MEMORY_FAILED;
        YYSYS_LOG(ERROR, "async_task_serialize_buffer == NULL");
      }
      else
      {
        in_buf.set_data(my_buffer->current(), my_buffer->remain());
      }
      if (OB_SUCCESS != err)
      {}
      else if (OB_SUCCESS != (err = schema_mgr.serialize(in_buf.get_data(), in_buf.get_capacity(), in_buf.get_position())))
      {
        YYSYS_LOG(ERROR, "schema_mgr.serialize(buf=%p[%ld-%ld])=>%d",
                  in_buf.get_data(), in_buf.get_position(), in_buf.get_capacity(), err);
      }
      else if (OB_SUCCESS != (err = submit_async_task_(OB_SWITCH_SCHEMA, write_thread_queue_, write_task_queue_size_, &in_buf)))
      {
        YYSYS_LOG(WARN, "submit_async_task(SWITCH_SCHEMA)=>%d", err);
      }
      return err;
    }

    int ObUpdateServer::submit_check_sstable_checksum(const uint64_t sstable_id, const uint64_t checksum)
    {
      int err = OB_SUCCESS;
      //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//      if (ObUpsRoleMgr::MASTER == role_mgr_.get_role()
//          && ObiRole::MASTER == obi_role_.get_role())
      if (ObUpsRoleMgr::MASTER == role_mgr_.get_role())
      //mod 20150701:e
      {
        ThreadSpecificBuffer::Buffer *my_buffer = my_thread_buffer_.get_buffer();
        ObDataBuffer in_buf;
        if (NULL == my_buffer)
        {
          err = OB_ALLOCATE_MEMORY_FAILED;
          YYSYS_LOG(ERROR, "async_task_serialize_buffer == NULL");
        }
        else
        {
          in_buf.set_data(my_buffer->current(), my_buffer->remain());
        }
        if (OB_SUCCESS != err)
        {}
        else if (OB_SUCCESS != (err = ups_serialize(sstable_id,
                in_buf.get_data(), in_buf.get_capacity(), in_buf.get_position())))
        {
          YYSYS_LOG(ERROR, "sstable_id=%lu serialize(buf=%p[%ld-%ld])=>%d",
                    sstable_id, in_buf.get_data(), in_buf.get_position(), in_buf.get_capacity(), err);
        }
        else if (OB_SUCCESS != (err = ups_serialize(checksum,
                in_buf.get_data(), in_buf.get_capacity(), in_buf.get_position())))
        {
          YYSYS_LOG(ERROR, "checksum=%lu serialize(buf=%p[%ld-%ld])=>%d",
                    checksum, in_buf.get_data(), in_buf.get_position(), in_buf.get_capacity(), err);
        }
        else if (OB_SUCCESS != (err = submit_async_task_(OB_UPS_ASYNC_CHECK_SSTABLE_CHECKSUM,
                write_thread_queue_, write_task_queue_size_, &in_buf)))
        {
          YYSYS_LOG(WARN, "submit_async_task(OB_UPS_ASYNC_CHECK_SSTABLE_CHECKSUM)=>%d", err);
        }
        else
        {
          YYSYS_LOG(INFO, "submit_async_task(OB_UPS_ASYNC_CHECK_SSTABLE_CHECKSUM) succ");
        }
      }
      return err;
    }

    int ObUpdateServer::submit_major_freeze()
    {
      int err = OB_SUCCESS;
      //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//      if (ObUpsRoleMgr::MASTER == role_mgr_.get_role()
//          && ObiRole::MASTER == obi_role_.get_role())
      if (ObUpsRoleMgr::MASTER == role_mgr_.get_role())
      //mod 20150701:e
      {
        //modify hongchen [FREEZED_PROCESS_FIX] 20170710:b
        //err = submit_async_task_(OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE, write_thread_queue_, write_task_queue_size_);
        static const int32_t MY_VERSION = 1;
        const int buff_size = static_cast<int>(sizeof(ObPacket) + 32);
        char buff[buff_size];
        ObDataBuffer msgbuf(buff, buff_size);
        if (OB_SUCCESS != (err = client_manager_.send_request(root_server_,OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE,MY_VERSION,2 * 1000 * 1000, msgbuf)))
        {
          printf("failed to send major freeze signal to rs(master), err=%d\n", err);
        }
        else
        {
          ObResultCode result_code;
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (err = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            printf("failed to deserialize response, err=%d\n", err);
          }
          else if (OB_SUCCESS != (err = result_code.result_code_))
          {
            YYSYS_LOG(WARN,"some error make rs fail to process major_freeze!, err=%d", result_code.result_code_);
          }
          else
          {
            YYSYS_LOG(INFO,"ups major freeze duty process successly!");
          }
        }
        //modify hongchen [FREEZED_PROCESS_FIX] 20170710:e
      }
      else
      {
      }
      return err;
    }

    //add peiouya [MultiUPS] [UPS_SYNC_Frozen] 20150521:b
    int ObUpdateServer::submit_major_freeze_accord_hb()
    {
      int err = OB_SUCCESS;
      //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//      if (ObUpsRoleMgr::MASTER == role_mgr_.get_role()
//          && ObiRole::MASTER == obi_role_.get_role())
      if (ObUpsRoleMgr::MASTER == role_mgr_.get_role())
      //mod 20150701:e
      {
        //mod peiouya [MultiUPS] [UPS_SYNC_Frozen] 20150810:b
        //err = submit_async_task_(OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE_ACCORD_HB, write_thread_queue_, write_task_queue_size_);
        err = submit_async_task_once_(major_freeze_accord_hb_guard_, OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE_ACCORD_HB, write_thread_queue_, write_task_queue_size_);
        //mod 20150810:e
      }
      else
      {
      }
      return err;
    }
    //add 20150521:e

    int ObUpdateServer::submit_minor_freeze()
    {
        int err = OB_SUCCESS;
        if(ObUpsRoleMgr::MASTER == role_mgr_.get_role())
        {
            err = submit_async_task_(OB_UPS_MINOR_FREEZE_MEMTABLE, write_thread_queue_,write_task_queue_size_);
        }
        else
        {

        }
        return err;
    }

    int ObUpdateServer::submit_auto_freeze()
    {
      int err = OB_SUCCESS;
      //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//      if (ObUpsRoleMgr::MASTER == role_mgr_.get_role()
//          && ObiRole::MASTER == obi_role_.get_role())
      if (ObUpsRoleMgr::MASTER == role_mgr_.get_role())
      //mod 20150701:e
      {
        err = submit_async_task_(OB_UPS_ASYNC_AUTO_FREEZE_MEMTABLE, write_thread_queue_, write_task_queue_size_);
      }
      else
      {
      }
      return err;
    }

    int ObUpdateServer::submit_switch_skey()
    {
      int err = OB_SUCCESS;
      //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//      if (ObUpsRoleMgr::MASTER == role_mgr_.get_role()
//          && ObiRole::MASTER == obi_role_.get_role())
      if (ObUpsRoleMgr::MASTER == role_mgr_.get_role())
      //mod 20150701:e
      {
        err = submit_async_task_(OB_UPS_ASYNC_SWITCH_SKEY, read_thread_queue_, read_task_queue_size_);
      }
      else
      {
      }
      return err;
    }

    int ObUpdateServer::submit_force_drop()
    {
      return submit_async_task_(OB_UPS_ASYNC_FORCE_DROP_MEMTABLE, read_thread_queue_, read_task_queue_size_);
    }

    int ObUpdateServer::submit_load_bypass(const common::ObPacket *packet)
    {
      return submit_async_task_(OB_UPS_ASYNC_LOAD_BYPASS, read_thread_queue_, read_task_queue_size_, NULL, packet);
    }

    int ObUpdateServer::submit_check_cur_version()
    {
      return submit_async_task_(OB_UPS_ASYNC_CHECK_CUR_VERSION, write_thread_queue_, write_task_queue_size_, NULL, NULL);
    }

    int ObUpdateServer::submit_immediately_drop()
    {
      int ret = OB_SUCCESS;
      bool for_immediately = true;
      submit_delay_drop(for_immediately);
      warm_up_duty_.finish_immediately();
      return ret;
    }

    int ObUpdateServer::submit_delay_drop(const bool for_immediately)
    {
      int ret = OB_SUCCESS;
      if (warm_up_duty_.drop_start())
      {
        schedule_warm_up_duty(for_immediately);
      }
      else
      {
        static int64_t last_log_time = 0;
        int64_t cur_time = yysys::CTimeUtil::getTime();
        int64_t old_time = last_log_time;
        if ((1000000 + last_log_time) < cur_time
            && old_time == ATOMIC_CAS(&last_log_time, old_time, cur_time))
        {
          YYSYS_LOG(INFO, "there is still a warm up duty running, will not schedule another");
        }
      }
      return ret;
    }

    int ObUpdateServer::submit_update_schema()
    {
      //mod zhaoqiong [Schema Manager] 20150423:b
      //return submit_async_task_(OB_UPS_ASYNC_UPDATE_SCHEMA, read_thread_queue_, read_task_queue_size_);
      int ret = OB_SUCCESS;
      if (yysys::CTimeUtil::getTime() - fetch_schema_timestamp_ < DEFAULT_CHECK_LEASE_INTERVAL * 10)
      {
        YYSYS_LOG(DEBUG, "check last fetch schema timestamp is too nearby");
      }
      else if (OB_SUCCESS == (ret = submit_async_task_(OB_UPS_ASYNC_UPDATE_SCHEMA, read_thread_queue_, read_task_queue_size_)))
      {
        fetch_schema_timestamp_ = yysys::CTimeUtil::getTime();
      }
      return ret;
      //mod:e
    }

    int ObUpdateServer::submit_kill_zombie()
    {
      return submit_async_task_(OB_UPS_ASYNC_KILL_ZOMBIE, write_thread_queue_, write_task_queue_size_);
    }

    void ObUpdateServer::schedule_warm_up_duty(const bool for_immediately)
    {
      int ret = OB_SUCCESS;
      int64_t warm_up_step_interval = 10000;
      if (!for_immediately)
      {
        warm_up_step_interval = warm_up_duty_.get_warm_up_step_interval();
      }
      bool repeat = false;
      if (OB_SUCCESS != (ret = timer_.schedule(warm_up_duty_, warm_up_step_interval, repeat)))
      {
        YYSYS_LOG(WARN, "schedule warm_up_duty fail ret=%d, will force drop", ret);
        submit_force_drop();
      }
      else
      {
        YYSYS_LOG(INFO, "warm up scheduled interval=%ld", warm_up_step_interval);
      }
    }

    template <class Queue>
    int ObUpdateServer::submit_async_task_once_(ObSafeOnceGuard& guard, const PacketCode pcode, Queue& qthread, int32_t task_queue_size)
    {
      int err = OB_SUCCESS;
      if (!guard.launch_authorize())
      {}
      else if (OB_SUCCESS != (err = submit_async_task_(pcode, qthread, task_queue_size)))
      {
        guard.launch_fail();
        YYSYS_LOG(WARN, "submit_async_task fail: pcode=%d", pcode);
      }
      else
      {
        guard.launch_success();
      }
      return err;
    }

    template <class Queue>
    int ObUpdateServer::submit_async_task_(const PacketCode pcode, Queue& qthread, int32_t task_queue_size,
        const int32_t version, common::ObDataBuffer& in_buff, easy_request_t* req,
        const uint32_t channel_id, const int64_t timeout)
    {
      int ret = OB_SUCCESS;
      ObPacket *ob_packet = NULL;
      if (NULL == (ob_packet = dynamic_cast<ObPacket*>(packet_factory_.createPacket(pcode))))
      {
        YYSYS_LOG(WARN, "create packet fail");
        ret = OB_ERROR;
      }
      else
      {
        ob_packet->set_packet_code(pcode);
        ob_packet->set_api_version(version);
        ob_packet->set_request(req);
        ob_packet->set_channel_id(channel_id);
        ob_packet->set_target_id(OB_SELF_FLAG);
        ob_packet->set_receive_ts(yysys::CTimeUtil::getTime());
        ob_packet->set_source_timeout(timeout);
        ob_packet->set_data(in_buff);
        if (OB_SUCCESS != (ret = ob_packet->serialize()))
        {
          YYSYS_LOG(WARN, "ob_packet serialize fail ret=%d", ret);
        }
        else if (!qthread.push(ob_packet, task_queue_size, false))
        {
          YYSYS_LOG(WARN, "submit async task to thread queue fail task_queue_size=%d, pcode=%d", task_queue_size, pcode);
          ret = OB_ERROR;
        }
        else
        {
          YYSYS_LOG(DEBUG, "submit async task succ pcode=%d", pcode);
        }
        if (OB_SUCCESS != ret)
        {
          packet_factory_.destroyPacket(ob_packet);
          ob_packet = NULL;
        }
      }
      return ret;
    }

    template <class Queue>
    int ObUpdateServer::submit_async_task_(const PacketCode pcode, Queue &qthread, int32_t &task_queue_size, const ObDataBuffer *data_buffer,
                                          const common::ObPacket *packet)
    {
      int ret = OB_SUCCESS;
      ObPacket *ob_packet = NULL;
      if (NULL == (ob_packet = dynamic_cast<ObPacket*>(packet_factory_.createPacket(pcode))))
      {
        YYSYS_LOG(WARN, "create packet fail");
        ret = OB_ERROR;
      }
      else
      {
        if (NULL != packet)
        {
          ob_packet->set_api_version(packet->get_api_version());
          ob_packet->set_request(packet->get_request());
          ob_packet->set_channel_id(packet->get_channel_id());
          ob_packet->set_source_timeout(packet->get_source_timeout());
        }

        uint32_t new_chid = atomic_inc(&ObPacket::global_chid);
        ob_packet->set_channel_id(new_chid);
        uint32_t *chid = GET_TSI_MULT(uint32_t, TSI_COMMON_PACKET_CHID_1);
        *chid = new_chid;
        ob_packet->set_api_version(MY_VERSION);
        ob_packet->set_packet_code(pcode);
        ob_packet->set_target_id(OB_SELF_FLAG);
        ob_packet->set_receive_ts(yysys::CTimeUtil::getTime());
        ob_packet->set_source_timeout(INT32_MAX);
        if (NULL != data_buffer)
        {
          ob_packet->set_data(*data_buffer);
        }
        if (OB_SUCCESS != (ret = ob_packet->serialize()))
        {
          YYSYS_LOG(WARN, "ob_packet serialize fail ret=%d", ret);
        }
        else if (!qthread.push(ob_packet, task_queue_size, false))
        {
          YYSYS_LOG(WARN, "submit async task to thread queue fail task_queue_size=%d, pcode=%d",
              task_queue_size, pcode);
          ret = OB_ERROR;
        }
        else
        {
          //YYSYS_LOG(INFO, "submit async task succ pcode=%d", pcode);
        }
        if (OB_SUCCESS != ret)
        {
          packet_factory_.destroyPacket(ob_packet);
          ob_packet = NULL;
        }
      }
      return ret;
    }

    int ObUpdateServer::ups_switch_schema(const int32_t version, ObPacket *packet_orig, common::ObDataBuffer &in_buf)
    {
      int ret = OB_SUCCESS;
      CommonSchemaManagerWrapper new_schema;

      //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//      if (!(ObiRole::MASTER == obi_role_.get_role()
//            && ObUpsRoleMgr::MASTER == role_mgr_.get_role()
//            && ObUpsRoleMgr::ACTIVE == role_mgr_.get_state()
//            && is_lease_valid()))
      if (!(ObUpsRoleMgr::MASTER == role_mgr_.get_role()
            && ObUpsRoleMgr::ACTIVE == role_mgr_.get_state()
            && is_lease_valid()))
      //mod 20150701:e
      {
        ret = OB_NOT_MASTER;
        YYSYS_LOG(WARN, "not master:is_lease_valid=%s", STR_BOOL(is_lease_valid()));
      }

      if (version != MY_VERSION)
      {
        YYSYS_LOG(ERROR, "Version do not match, MY_VERSION=%d version= %d",
            MY_VERSION, version);
        ret = OB_ERROR_FUNC_VERSION;
      }

      if (OB_SUCCESS == ret)
      {
        ret = new_schema.deserialize(in_buf.get_data(), in_buf.get_capacity(), in_buf.get_position());
        if (OB_SUCCESS != ret)
        {
          YYSYS_LOG(ERROR, "deserialize schema from packet error, ret=%d buf=%p pos=%ld cap=%ld", ret, in_buf.get_data(), in_buf.get_position(), in_buf.get_capacity());
        }
      }

      //add zhaoqiong [Schema Manager] 20150327:b

      //avoid fetch followed schema make response timeout
      if (NULL != packet_orig->get_request())
      {
        ret = response_result_(ret, OB_SWITCH_SCHEMA_RESPONSE, MY_VERSION,
            packet_orig->get_request(), packet_orig->get_channel_id());
      }

      //if old schema, do not need to fetch followed schema
      if (OB_SUCCESS == ret && new_schema.get_version() <= table_mgr_.get_schema_version())
      {
        ret = OB_OLD_SCHEMA_VERSION;
        YYSYS_LOG(WARN, "old schema, local schema version=%ld, switch version=%ld",table_mgr_.get_schema_version(),new_schema.get_version());
      }

      if (OB_SUCCESS == ret && !new_schema.get_impl()->is_completion())
      {
        ret = new_schema.get_impl_ref().fetch_schema_followed(ups_rpc_stub_,config_.fetch_schema_timeout*2,get_root_server(),new_schema.get_impl()->get_version());
      }
      //add:e

      if (OB_SUCCESS == ret)
      {
        YYSYS_LOG(INFO, "switch schemas");
        ret = table_mgr_.switch_schemas(new_schema);
        if (OB_SUCCESS != ret)
        {
          YYSYS_LOG(ERROR, "set_schemas failed, ret=%d", ret);
        }
      }
      //add zhaoqiong [Schema Manager] 20150327:b
      else if (OB_OLD_SCHEMA_VERSION == ret)
      {
        ret = OB_SUCCESS;
      }
      //add:e

      if (OB_SUCCESS == ret)
      {
        YYSYS_LOG(INFO, "switch schema succ");
      }
      else
      {
        YYSYS_LOG(ERROR, "switch schema err, ret=%d schema_version=%ld", ret, new_schema.get_version());
        //del zhaoqiong [Schema Manager] 20150421:b
        //hex_dump(in_buf.get_data(), static_cast<int32_t>(in_buf.get_capacity()), false, YYSYS_LOG_LEVEL_ERROR);
        //del:e
      }
	 //del zhaoqiong [Schema Manager] 20150327:b
//      if (NULL != packet_orig->get_request())
//      {
//        ret = response_result_(ret, OB_SWITCH_SCHEMA_RESPONSE, MY_VERSION,
//            packet_orig->get_request(), packet_orig->get_channel_id());
//      }
	  //del:e
      return ret;
    }

    int ObUpdateServer::ups_create_memtable_index()
    {
      int ret = OB_SUCCESS;

      // memtable������ �ظ�����ֱ�ӷ���OB_SUCCESS
      ret = table_mgr_.create_index();
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "create index fail ret=%d", ret);
      }

      // �����л���timestamp
      uint64_t new_version = 0;
      if (OB_SUCCESS == ret)
      {
        ret = table_mgr_.get_active_memtable_version(new_version);
        if (OB_SUCCESS != ret)
        {
          YYSYS_LOG(WARN, "decode new version fail ret=%d", ret);
        }
      }

      // ���ͷ�����Ϣ��rootserver
      int64_t retry_times = config_.resp_root_times;
      int64_t timeout = config_.resp_root_timeout;
      ret = RPC_CALL_WITH_RETRY(send_freeze_memtable_resp, retry_times, timeout, root_server_, ups_master_, new_version);
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "send freeze memtable resp fail ret_code=%d schema_version=%ld", ret, new_version);
      }
      return ret;
    }

    int ObUpdateServer::ups_drop_memtable(const int32_t version, easy_request_t* req, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      bool force = true;
      table_mgr_.drop_memtable(force);
      ret = response_result_(ret, OB_DROP_OLD_TABLETS_RESPONSE, MY_VERSION, req, channel_id);
      return ret;
    }

    int ObUpdateServer::ups_delay_drop_memtable(const int32_t version, easy_request_t* req, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      submit_delay_drop();
      ret = response_result_(ret, OB_UPS_DELAY_DROP_MEMTABLE_RESPONSE, MY_VERSION, req, channel_id);
      return ret;
    }

    int ObUpdateServer::ups_immediately_drop_memtable(const int32_t version, easy_request_t* req, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      submit_immediately_drop();
      ret = response_result_(ret, OB_UPS_IMMEDIATELY_DROP_MEMTABLE_RESPONSE, MY_VERSION, req, channel_id);
      return ret;
    }

    int ObUpdateServer::ups_drop_memtable()
    {
      bool force = true;
      table_mgr_.drop_memtable(force);
      warm_up_duty_.drop_end();
      force = false;
      table_mgr_.erase_sstable(force);
      return OB_SUCCESS;
    }

    int ObUpdateServer::ups_get_memtable_info(const int32_t version, easy_request_t *req,
                                              const uint32_t channel_id, ObDataBuffer &out_buffer)
    {
        UNUSED(channel_id);
        int ret = OB_SUCCESS;
        common::ObResultCode rc;

        if(version != MY_VERSION)
        {
            YYSYS_LOG(WARN,"version not equal. version=%d, MY_VERSION=%d",version, MY_VERSION);
            ret = OB_ERROR_FUNC_VERSION;
        }
        else
        {
            ret = rc.serialize(out_buffer.get_data(), out_buffer.get_capacity(), out_buffer.get_position());
            if(OB_SUCCESS != ret)
            {
                YYSYS_LOG(WARN,"serialize the rc failed, ret=%d",ret);
            }
            else
            {
                ret = table_mgr_.get_table_mgr()->serialize_ups_memtable_info(sstable_mgr_, out_buffer.get_data(),out_buffer.get_capacity(), out_buffer.get_position());
                if(OB_SUCCESS != ret)
                {
                    YYSYS_LOG(WARN,"serialize ups memtable info failed, ret=%d",ret);
                }
            }
        }
        if(OB_SUCCESS == ret)
        {
            ret = send_response(OB_UPS_GET_MEM_TABLE_INFO_RESPONSE, MY_VERSION, out_buffer, req, channel_id);
            if(OB_SUCCESS != ret)
            {
                YYSYS_LOG(WARN,"fail to send response to get memtable info, ret=%d",ret);
            }
        }
        return ret;
    }

    int ObUpdateServer::ups_load_bypass(const int32_t version, easy_request_t* req, const uint32_t channel_id,
                                        common::ObDataBuffer& out_buff, const int packet_code)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      int64_t loaded_num = 0;
      if (OB_SUCCESS == ret)
      {
        ret = table_mgr_.load_sstable_bypass(sstable_mgr_, loaded_num);
        //submit_report_freeze();  //del peiouya [UPS_SYNC_Frozen] 20150528
        submit_check_cur_version();
        table_mgr_.log_table_info();
      }

      ret = response_data_(ret, loaded_num, packet_code + 1, MY_VERSION, req, channel_id, out_buff);
      return ret;
    }

    int ObUpdateServer::ups_check_cur_version()
    {
      return table_mgr_.check_cur_version();
    }

    int ObUpdateServer::ups_commit_check_sstable_checksum(ObDataBuffer &buffer)
    {
      int ret = OB_SUCCESS;
      uint64_t sstable_id = 0;
      uint64_t checksum = 0;
      //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//      if (!(ObiRole::MASTER == obi_role_.get_role()
//            && ObUpsRoleMgr::MASTER == role_mgr_.get_role()
//            && ObUpsRoleMgr::ACTIVE == role_mgr_.get_state()
//            && is_lease_valid()))
      if (!(ObUpsRoleMgr::MASTER == role_mgr_.get_role()
            && ObUpsRoleMgr::ACTIVE == role_mgr_.get_state()
            && is_lease_valid()))
      //mod 20150701:e
      {
        ret = OB_NOT_MASTER;
        YYSYS_LOG(WARN, "not master:is_lease_valid=%s", STR_BOOL(is_lease_valid()));
      }
      else if (OB_SUCCESS != (ret = ups_deserialize(sstable_id,
              buffer.get_data(), buffer.get_capacity(), buffer.get_position())))
      {
        YYSYS_LOG(WARN, "deserialize sstable_id fail, data=%p capacity=%ld position=%ld",
                  buffer.get_data(), buffer.get_capacity(), buffer.get_position());
      }
      else if (OB_SUCCESS != (ret = ups_deserialize(checksum,
              buffer.get_data(), buffer.get_capacity(), buffer.get_position())))
      {
        YYSYS_LOG(WARN, "deserialize checksum fail, data=%p capacity=%ld position=%ld",
                  buffer.get_data(), buffer.get_capacity(), buffer.get_position());
      }
      else
      {
        YYSYS_LOG(INFO, "write to commitlog to check sstable checksum, %s checksum=%lu",
                  SSTableID::log_str(sstable_id), checksum);
        ret = table_mgr_.commit_check_sstable_checksum(sstable_id, checksum);
      }
      return ret;
    }

    int ObUpdateServer::ups_erase_sstable(const int32_t version, easy_request_t* req, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      bool force = true;
      table_mgr_.erase_sstable(force);
      ret = response_result_(ret, OB_DROP_OLD_TABLETS_RESPONSE, MY_VERSION, req, channel_id);
      return ret;
    }

    int ObUpdateServer::ups_load_new_store(const int32_t version, easy_request_t* req, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      ret = sstable_mgr_.load_new() ? OB_SUCCESS : OB_ERROR;
      ret = response_result_(ret, OB_UPS_LOAD_NEW_STORE_RESPONSE, MY_VERSION, req, channel_id);
      return ret;
    }

    int ObUpdateServer::ups_reload_all_store(const int32_t version, easy_request_t* req, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      sstable_mgr_.reload_all();
      ret = response_result_(ret, OB_UPS_RELOAD_ALL_STORE_RESPONSE, MY_VERSION, req, channel_id);
      return ret;
    }
    //add wangjiahao [Paxos] 20150810 :b
    int ObUpdateServer::ups_rs_get_max_log_seq_and_term(const int32_t version, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer &out_buff)
    {
      int err = OB_SUCCESS;
      int RETRY_TIMES = 2;//retry time
      if (MY_VERSION != version)
      {
        YYSYS_LOG(WARN, "version not equal. version=%d, MY_VERSION=%d", version, MY_VERSION);
        err = OB_ERROR_FUNC_VERSION;
      }
      int64_t log_seq = 0;
      int64_t log_term = 0;
      if (OB_SUCCESS == err)
      {
        ups_master_.reset();
        //add wangdonghui [ups_replication] 20170717 :b
        can_receive_log_ = false;
        //ups_master_.set_ipv4_addr("0.0.0.0", 0);
        YYSYS_LOG(INFO, "receive OB_RS_GET_MAX_LOG_SEQ_AND_TERM, need to change can_receive_log->false");
        for (int retry = 0; retry < RETRY_TIMES; retry ++)
        {
          int64_t max_log_seq_in_buffer = 0;
          if (OB_SUCCESS != (err = log_mgr_.get_max_log_seq_in_buffer(max_log_seq_in_buffer)))
          {
            YYSYS_LOG(ERROR, "get_max_log_seq_in_file()=>%d", err);
          }
          else if(max_log_seq_in_buffer > log_mgr_.get_tmp_cursor_log_id() || !is_set_term_)
          {
            YYSYS_LOG(WARN, "wait buffer(%ld) flush to disk(%ld) before responsing to RS. is_set_term(%d). retry_times(%d)",
                       max_log_seq_in_buffer, log_mgr_.get_tmp_cursor_log_id(), is_set_term_, retry);
            usleep(50 * 1000);
            err = OB_GET_TERM_TIMEOUT;
          }
          else
          {
            err = OB_SUCCESS;
            break;
          }
        }
        if(OB_SUCCESS != err)
        {}
        else
        //add :e
        //add :e
        if (OB_SUCCESS != (err = log_mgr_.get_max_log_seq_replayable(log_seq)))
        {
          YYSYS_LOG(ERROR, "log_mgr.get_max_log_seq_replayable(log_seq)=>%d", err);
        }
        else
        {
          log_term = log_mgr_.get_current_log_term();
          YYSYS_LOG(INFO, "ups_rs_get_max_log_seq_and_term succ. (log_id: %ld, term: %ld)",
                    log_seq, log_term);
          common::ObResultCode result_msg;
          result_msg.result_code_ = err;
          err = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
          if (OB_SUCCESS != err)
          {
            YYSYS_LOG(WARN, "fail to serialize, err=%d", err);
          }
        }
        //if (OB_SUCCESS == err)
        {
           if (OB_SUCCESS != (err = serialization::encode_vi64(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position(), log_seq)))
           {
              YYSYS_LOG(WARN, "fail to serialize log_seq, err=%d", err);
           }
           else if (OB_SUCCESS != (err = serialization::encode_vi64(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position(), log_term)))
           {
              YYSYS_LOG(WARN, "fail to serialize ups_term, err=%d", err);
           }
           else
           {
             err = send_response(OB_RS_GET_MAX_LOG_SEQ_AND_TERM_RESPONSE, version, out_buff, req, channel_id);
             if (OB_SUCCESS != err)
             {
                YYSYS_LOG(WARN, "fail to send response, err = %d", err);
             }
           }
        }
      }
      return err;
    }
    //add :e
    //add pangtianze [Paxos rs_election] 20160919:b
    int ObUpdateServer::ups_rs_get_ups_role(const int32_t version, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
        int err = OB_SUCCESS;
        if (MY_VERSION != version)
        {
          YYSYS_LOG(WARN, "version not equal. version=%d, MY_VERSION=%d", version, MY_VERSION);
          err = OB_ERROR_FUNC_VERSION;
        }
        int32_t role = (int32_t)role_mgr_.get_role();
        if (OB_SUCCESS != (err = response_data_(err, role, OB_RS_GET_UPS_ROLE_RESPONSE, MY_VERSION, req, channel_id, out_buff)))
        {
          YYSYS_LOG(WARN, "fail to send response, err = %d", err);
        }
        return err;
    }
    //add:e
    //add pangtianze [Paxos rs_election] 20170228:b
    int ObUpdateServer::ups_refresh_rs_list(const int32_t version, common::ObDataBuffer& in_buff,
                       easy_request_t* req, const uint32_t channel_id)
    {
        UNUSED(channel_id);

        int err = OB_SUCCESS;
        if (MY_VERSION != version)
        {
          YYSYS_LOG(WARN, "version not equal. version=%d, MY_VERSION=%d", version, MY_VERSION);
          err = OB_ERROR_FUNC_VERSION;
        }
        else
        {
          int32_t servers_count = 0;
          if (OB_SUCCESS != (err = serialization::decode_vi32(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &servers_count)))
          {
            YYSYS_LOG(WARN, "failed to deserialize server_count. err=%d", err);
          }
          if (OB_SUCCESS == err)
          {
            if (servers_count > 0) //count cannot be 0
            {
                common::ObServer servers[servers_count];
                for (int32_t i = 0; i < servers_count; i++)
                {
                  if (OB_SUCCESS != (err = servers[i].deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position())))
                  {
                     YYSYS_LOG(WARN, "failed to deserialize rootserver. err=%d", err);
                     break;
                  }
                }
                //mod chujiajia[Paxos rs_election]20151021:b
                if(OB_SUCCESS == err)
                {
                  if (OB_SUCCESS != (err = rs_mgr_.update_all_rs(servers, servers_count)))
                  {
                    YYSYS_LOG(WARN, "failed update all rootservers. err=%d", err);
                  }
                }
                //mod:e
            }
            else
            {
                err = OB_INVALID_VALUE;
                YYSYS_LOG(ERROR, "rs server count is invalid, count=%d, err=%d", servers_count, err);
            }
            //mod:e
          }
        }

        easy_request_wakeup(req);
        return err;
    }
    //add:e

    int ObUpdateServer::get_ups_tmplog_replay_finished(const int32_t version, common::ObDataBuffer &in_buff,
        easy_request_t *req, const uint32_t channel_id, common::ObDataBuffer &out_buff)
    {
        int err = OB_SUCCESS;
        common::ObResultCode result_msg;
        bool is_tmplog_replay_finished = false;
        common::ObServer ups_addr;
        result_msg.result_code_ = OB_SUCCESS;

        if(MY_VERSION != version)
        {
            YYSYS_LOG(WARN,"version not equal. version=%d, MY_VERSION=%d",version, MY_VERSION);
            result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
        }
        else
        {
            if(OB_SUCCESS != (err = ups_addr.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position())))
            {
                YYSYS_LOG(ERROR,"failed to deserialize ups_addr, err=%d", err);
            }
        }

        if(OB_SUCCESS == err)
        {
            if(ups_addr == self_addr_ && log_mgr_.is_tmp_log_replay_finished())
            {
                is_tmplog_replay_finished = true;
            }
        }

        if(OB_SUCCESS == err)
        {
            err = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
            if(OB_SUCCESS != err)
            {
                YYSYS_LOG(WARN, "fail to serialize result_msg, err=%d",err);
            }
            else
            {
                if(OB_SUCCESS != (err = serialization::encode_bool(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position(), is_tmplog_replay_finished)))
                {
                    YYSYS_LOG(ERROR,"failed to serialize is_tmplog_replay_finished, err=%d", err);
                }
            }
        }
        if(OB_SUCCESS == err)
        {
            err = send_response(OB_GET_UPS_TMPLOG_REPLAY_FINISHED_RESPONSE, MY_VERSION, out_buff, req, channel_id);
            if(OB_SUCCESS != err)
            {
                YYSYS_LOG(WARN, "fail to send response. err = %d",err);
            }
        }
        return err;
    }


    int ObUpdateServer::ups_rs_get_memory(const int32_t version, common::ObDataBuffer &in_buff,
            easy_request_t *req, const uint32_t channel_id, common::ObDataBuffer &out_buff)
    {
        int err = OB_SUCCESS;
        common::ObResultCode result_msg;
        int64_t total_mem_size = 0;
        common::ObServer ups_addr;
        result_msg.result_code_ = OB_SUCCESS;

        if(MY_VERSION != version)
        {
            YYSYS_LOG(WARN,"version not equal. version=%d, MY_VERSION=%d",version, MY_VERSION);
            result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
        }
        else
        {
            if(OB_SUCCESS != (err = ups_addr.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position())))
            {
                YYSYS_LOG(ERROR,"failed to deserialize ups_addr, err=%d", err);
            }
        }

        if(OB_SUCCESS == err)
        {
            err = get_ups_mem_size(ups_addr, total_mem_size);
            if(OB_SUCCESS != err)
            {
                YYSYS_LOG(WARN,"fail to get ups used memory, err = %d", err);
            }
        }
        if(OB_SUCCESS == err)
        {
            err = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
            if(OB_SUCCESS != err)
            {
                YYSYS_LOG(WARN, "fail to serialize result_msg, err=%d",err);
            }
            else
            {
                if(OB_SUCCESS != (err = serialization::encode_vi64(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position(), total_mem_size)))
                {
                    YYSYS_LOG(ERROR,"failed to serialize total_mem_size, err=%d", err);
                }
            }
        }
        if(OB_SUCCESS == err)
        {
            err = send_response(OB_GET_UPS_MEMORY_RESPONSE, MY_VERSION, out_buff, req, channel_id);
            if(OB_SUCCESS != err)
            {
                YYSYS_LOG(WARN, "fail to send response. err = %d",err);
            }
        }
        return err;
    }

    int ObUpdateServer::get_ups_mem_size(const common::ObServer &addr, int64_t &mem_size)
    {
        int err = OB_SUCCESS;
        if(self_addr_ == addr)
        {
            table_mgr_.get_ups_used_mem(mem_size);
        }
        else
        {
            YYSYS_LOG(ERROR,"the ups addr not equal");
            err = OB_ERROR;
        }
        return err;
    }


    int ObUpdateServer::ups_rs_get_max_log_seq(const int32_t version, easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer &out_buff)
    {
      int err = OB_SUCCESS;
      if (MY_VERSION != version)
      {
        err = OB_ERROR_FUNC_VERSION;
      }
      int64_t log_seq = 0;
      if (OB_SUCCESS != (err = log_mgr_.get_max_log_seq_replayable(log_seq)))
      {
        YYSYS_LOG(ERROR, "log_mgr.get_max_log_seq_replayable(log_seq)=>%d", err);
      }

      if (OB_SUCCESS != (err = response_data_(err, log_seq, OB_RS_GET_MAX_LOG_SEQ_RESPONSE, MY_VERSION, req, channel_id, out_buff)))
      {
        YYSYS_LOG(WARN, "fail to send response, err = %d", err);
      }
      return err;
    }
    int ObUpdateServer::slave_ups_receive_keep_alive(const int32_t version, easy_request_t* req, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      UNUSED(req);
      UNUSED(channel_id);
      if (MY_VERSION != version)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      else
      {
        ret = OB_NOT_SUPPORTED;
        YYSYS_LOG(WARN, "ups_receive_keep_alive(): NOT NEED anymore");
      }

      easy_request_wakeup(req);
      return ret;
    }

    int ObUpdateServer::ups_clear_fatal_status(const int32_t version, easy_request_t* req, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      if (MY_VERSION != version)
      {
        ret = OB_ERROR_FUNC_VERSION;
        YYSYS_LOG(ERROR, "MY_VERSION[%d] != version[%d]", MY_VERSION, version);
      }
      else
      {
        if (ObUpsRoleMgr::FATAL == role_mgr_.get_state())
        {
          role_mgr_.set_state(ObUpsRoleMgr::ACTIVE);
          YYSYS_LOG(INFO, "clear ups FATAL status succ.");
        }
        ret = response_result_(ret, OB_UPS_CLEAR_FATAL_STATUS_RESPONSE, MY_VERSION, req, channel_id);
      }
      return ret;
    }
    int ObUpdateServer::ups_froce_report_frozen_version(const int32_t version, easy_request_t* req, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      //submit_report_freeze();  //del peiouya [UPS_SYNC_Frozen] 20150528
      ret = response_result_(ret, OB_UPS_FORCE_REPORT_FROZEN_VERSION_RESPONSE, MY_VERSION, req, channel_id);
      return ret;
    }

    int ObUpdateServer::ups_reload_store(const int32_t version, common::ObDataBuffer& in_buf,
        easy_request_t* req, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      StoreMgr::Handle store_handle = StoreMgr::INVALID_HANDLE;
      if (OB_SUCCESS != (ret = serialization::decode_vi64(in_buf.get_data(), in_buf.get_capacity(), in_buf.get_position(), (int64_t*)&store_handle)))
      {
        YYSYS_LOG(WARN, "decode store_handle fail ret=%d", ret);
      }
      else
      {
        YYSYS_LOG(INFO, "reload store handle=%lu", store_handle);
        sstable_mgr_.reload(store_handle);
      }
      response_result_(ret, OB_UPS_RELOAD_STORE_RESPONSE, MY_VERSION, req, channel_id);
      return ret;
    }

    int ObUpdateServer::ups_umount_store(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      ObString umount_dir;
      if (OB_SUCCESS == ret)
      {
        ret = umount_dir.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          YYSYS_LOG(WARN, "deserialize umount dir error, ret=%d", ret);
        }
      }
      if (OB_SUCCESS == ret)
      {
        YYSYS_LOG(INFO, "umount store=[%s]", umount_dir.ptr());
        sstable_mgr_.umount_store(umount_dir.ptr());
        sstable_mgr_.check_broken();
      }
      ret = response_result_(ret, OB_UPS_UMOUNT_STORE_RESPONSE, MY_VERSION, req, channel_id);
      return ret;
    }

    int ObUpdateServer::ups_slave_register(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int err = OB_SUCCESS;

      if (version != MY_VERSION)
      {
        err = OB_ERROR_FUNC_VERSION;
      }

      uint64_t new_log_file_id = 0;
      // deserialize ups_slave
      ObSlaveInfo slave_info;
      err = slave_info.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
      if (OB_SUCCESS != err)
      {
        YYSYS_LOG(WARN, "deserialize ObSlaveInfo failed, err=%d", err);
      }

      if (OB_SUCCESS == err)
      {
        //add peiouya [MultiUPS] [DELETE_OBI] 20150723:b
        //expr:following original processing logic, replace the output field.
        //role_mgr_.get_role_str(), instead of obi_role_.get_role_str()
        YYSYS_LOG(INFO, "start log mgr add slave, role=%s", role_mgr_.get_role_str());
        //add 20150723:e
        //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
        //YYSYS_LOG(INFO, "start log mgr add slave, obi_role=%s", obi_role_.get_role_str());
        //err = log_mgr_.add_slave(slave_info.self, new_log_file_id, ObiRole::MASTER == obi_role_.get_role());
        err = log_mgr_.add_slave(slave_info.self, new_log_file_id, true,
		                         //add wangjiahao [Paxos ups_replication] 20150817 :b
                             slave_info.log_id //add:e
                             );
        //mod 20150701:e
        if (OB_SUCCESS != err)
        {
          YYSYS_LOG(WARN, "ObUpsLogMgr add_slave error, err=%d", err);
        }

        YYSYS_LOG(INFO, "add slave, slave_addr=%s, log_id=%ld, err=%d", to_cstring(slave_info.self), slave_info.log_id, err);
      }

      // reply ups slave with related info
      if (OB_SUCCESS == err)
      {
        // fetch_param��ʱû�á�
        ObUpsFetchParam fetch_param;
        fetch_param.fetch_log_ = true;
        fetch_param.fetch_ckpt_ = false;
        fetch_param.min_log_id_ = 0;
        fetch_param.max_log_id_ = new_log_file_id - 1;
        err = sstable_mgr_.fill_fetch_param(slave_info.min_sstable_id,
            slave_info.max_sstable_id, config_.slave_sync_sstable_num, fetch_param);
        int64_t log_id;
        log_mgr_.get_max_log_seq_replayable(log_id);
        YYSYS_LOG(INFO, "receive slave register, max_log_id=%ld", log_id);

        if (OB_SUCCESS != err)
        {
          YYSYS_LOG(WARN, "ObSSTableMgr fill_fetch_param error, err=%d", err);
        }
        else
        {
          err = response_fetch_param_(err, fetch_param, log_id, OB_SLAVE_REG_RES, MY_VERSION,
              req, channel_id, out_buff);
          if (OB_SUCCESS != err)
          {
            YYSYS_LOG(WARN, "failed to response fetch param, err=%d", err);
          }
        }
      }

      return err;
    }

    //add zhaoqiong [fixed for Backup]:20150811:b
    int ObUpdateServer::ups_backup_register(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int err = OB_SUCCESS;

      if (version != MY_VERSION)
      {
        err = OB_ERROR_FUNC_VERSION;
      }

      uint64_t new_log_file_id = 0;
      // deserialize ups_slave
      ObSlaveInfo slave_info;
      err = slave_info.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
      if (OB_SUCCESS != err)
      {
        YYSYS_LOG(WARN, "deserialize ObSlaveInfo failed, err=%d", err);
      }

      if (OB_SUCCESS == err)
      {
	   // YYSYS_LOG(INFO, "start log mgr switch log, obi_role=%s", obi_role_.get_role_str());
        YYSYS_LOG(INFO, "start log mgr switch log, obi_role=%s", role_mgr_.get_role_str());
        err = log_mgr_.switch_log_file(new_log_file_id);
        if (OB_SUCCESS != err)
        {
          YYSYS_LOG(WARN, "ObUpsLogMgr add_backup error, err=%d", err);
        }

        YYSYS_LOG(INFO, "add backup, backup_addr=%s, err=%d", to_cstring(slave_info.self), err);
      }

      // reply ups slave with related info
      if (OB_SUCCESS == err)
      {
        ObUpsFetchParam fetch_param;
        fetch_param.fetch_log_ = true;
        fetch_param.fetch_ckpt_ = false;
        fetch_param.max_log_id_ = new_log_file_id - 1;
        err = sstable_mgr_.fill_fetch_param(slave_info.min_sstable_id,
            slave_info.max_sstable_id, config_.slave_sync_sstable_num, fetch_param);
        int64_t log_seq;
        log_mgr_.get_max_log_seq_replayable(log_seq);
        YYSYS_LOG(INFO, "receive slave register, max_log_seq=%ld", log_seq);
        if (OB_SUCCESS != err)
        {
          YYSYS_LOG(WARN, "ObSSTableMgr fill_fetch_param error, err=%d", err);
        }
        else
        {
          err = response_fetch_param_(err, fetch_param, log_seq, OB_BACKUP_REG_RES, MY_VERSION,
              req, channel_id, out_buff);
          if (OB_SUCCESS != err)
          {
            YYSYS_LOG(WARN, "failed to response fetch param, err=%d", err);
          }
        }
      }

      return err;
    }
    //add:e
	
 //del peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//    int ObUpdateServer::ups_slave_quit(const int32_t version, common::ObDataBuffer& in_buff,
//        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
//    {
//      int err = OB_SUCCESS;

//      UNUSED(out_buff);

//      if (version != MY_VERSION)
//      {
//        err = OB_ERROR_FUNC_VERSION;
//      }

//      // deserialize ups_slave
//      ObServer ups_slave;
//      err = ups_slave.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
//      if (OB_SUCCESS != err)
//      {
//        YYSYS_LOG(WARN, "deserialize ups_slave failed, err=%d", err);
//      }

//      if (OB_SUCCESS == err)
//      {
//        err = slave_mgr_.delete_server(ups_slave);
//        if (OB_SUCCESS != err)
//        {
//          YYSYS_LOG(WARN, "ObSlaveMgr delete_slave error, err=%d", err);
//        }

//        YYSYS_LOG(INFO, "slave quit, slave_addr=%s, err=%d", to_cstring(ups_slave), err);
//      }

//      // reply ups slave
//      if (OB_SUCCESS == err)
//      {
//        err = response_result_(err, OB_SLAVE_QUIT_RES, MY_VERSION, req, channel_id);
//        if (OB_SUCCESS != err)
//        {
//          YYSYS_LOG(WARN, "failed to response slave quit, err=%d", err);
//        }
//      }

//      return err;
//    }
    //del 20150701:e

    int ObUpdateServer::ups_apply(const bool using_id, UpsTableMgrTransHandle& handle, common::ObDataBuffer& in_buff, ObScanner *scanner)
    {
      int ret = OB_SUCCESS;
      ObUpsMutator ups_mutator_stack;
      ObUpsMutator *ups_mutator_ptr = GET_TSI_MULT(ObUpsMutator, TSI_UPS_UPS_MUTATOR_1);
      ObUpsMutator &ups_mutator = (NULL == ups_mutator_ptr) ? ups_mutator_stack : *ups_mutator_ptr;
      {
        UpsSchemaMgrGuard guard;
        const ObSchemaManagerV2* schema_mgr = table_mgr_.get_schema_mgr().get_schema_mgr(guard);
        if (NULL == schema_mgr)
        {
          YYSYS_LOG(WARN, "failed to get schema");
          ret = OB_SCHEMA_ERROR;
        }
        else
        {
          ups_mutator.get_mutator().set_compatible_schema(schema_mgr);
          ret = ups_mutator.get_mutator().deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        }
      }
      FILL_TRACE_LOG("mutator deserialize ret=%d", ret);
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "deserialize mutator fail ret=%d", ret);
      }
      else if (NULL == scanner)
      {
        YYSYS_LOG(WARN, "scanner null pointer");
        ret = OB_ERROR;
      }

      if (OB_SUCCESS == ret)
      {
        ret = table_mgr_.apply(using_id, handle, ups_mutator, scanner);
      }
      OB_STAT_INC(UPDATESERVER, UPS_STAT_NL_APPLY_COUNT, 1);
      OB_STAT_INC(UPDATESERVER, UPS_STAT_NL_APPLY_TIMEU, GET_TRACE_TIMEU());
      FILL_TRACE_LOG("ret=%d", ret);
      PRINT_TRACE_LOG();
      if (OB_SUCCESS != ret)
      {
        OB_STAT_INC(UPDATESERVER, UPS_STAT_APPLY_FAIL_COUNT, 1);
      }
      return ret;
    }

    //add by maosy [MultiUps 1.0] [batch_iud_snapshot] 20170622 b:
    int ObUpdateServer::response_published_transid(int32_t ret_code, ObPacket &pkt, int64_t published_transid, ObDataBuffer &out_buffer)
    {
        int ret = OB_SUCCESS ;
        ret = response_data_(ret_code,published_transid,pkt.get_packet_code(), pkt.get_api_version(),
                               pkt.get_request(), pkt.get_channel_id(), out_buffer, pkt.get_receive_ts());
        return ret ;
    }
    // add by e

    int ObUpdateServer::ups_start_transaction(const MemTableTransType type, UpsTableMgrTransHandle& handle)
    {
      int ret = OB_SUCCESS;
      start_trans_timestamp_ = yysys::CTimeUtil::getTime();
      ret = table_mgr_.start_transaction(type, handle);
      CLEAR_TRACE_LOG();
      FILL_TRACE_LOG("ret=%d", ret);
      return ret;
    }

    int ObUpdateServer::response_result(int32_t ret_code, ObPacket &pkt)
    {
      int ret = OB_SUCCESS;
      ret = response_result_(ret_code, pkt.get_packet_code(), pkt.get_api_version(),
                             pkt.get_request(), pkt.get_channel_id(), pkt.get_receive_ts());
      // ����ǰ�����ֶ� trace_id��chid
      PROFILE_LOG(DEBUG, TRACE_ID
                  SOURCE_CHANNEL_ID
                  UPS_REQ_END_TIME, pkt.get_trace_id(), pkt.get_channel_id(), yysys::CTimeUtil::getTime());
      return ret;
    }
    int ObUpdateServer::response_result(int32_t ret_code, const char *ret_string, ObPacket &pkt)
    {
      return response_result_(ret_code, pkt.get_packet_code(), pkt.get_api_version(),
                              pkt.get_request(), pkt.get_channel_id(), pkt.get_receive_ts(), ret_string);
    }

    int ObUpdateServer::response_trans_id(int32_t ret_code, ObPacket &pkt, common::ObTransID &id, ObDataBuffer &out_buffer)
    {
      int ret = OB_SUCCESS;
      ret =  response_data_(ret_code, id, pkt.get_packet_code(), pkt.get_api_version(),
                            pkt.get_request(), pkt.get_channel_id(), out_buffer, pkt.get_receive_ts());
      return ret;
    }

    int ObUpdateServer::response_scanner(int32_t ret_code, ObPacket &pkt, common::ObScanner &scanner, ObDataBuffer &out_buffer)
    {
      int ret = OB_SUCCESS;
      ret =  response_data_(ret_code, scanner, pkt.get_packet_code(), pkt.get_api_version(),
                            pkt.get_request(), pkt.get_channel_id(), out_buffer, pkt.get_receive_ts());
      // ����ǰ�����ֶ� trace_id��chid
      PROFILE_LOG(DEBUG, TRACE_ID
                  SOURCE_CHANNEL_ID
                  UPS_REQ_END_TIME
                  SCANNER_SIZE_BYTES, pkt.get_trace_id(), pkt.get_channel_id(), yysys::CTimeUtil::getTime(), scanner.get_size());
      return ret;
    }

    int ObUpdateServer::response_scanner(int32_t ret_code, ObPacket &pkt, common::ObNewScanner &new_scanner, ObDataBuffer &out_buffer)
    {
      int ret = OB_SUCCESS;
      ret =  response_data_(ret_code, new_scanner, pkt.get_packet_code(), pkt.get_api_version(),
                            pkt.get_request(), pkt.get_channel_id(), out_buffer, pkt.get_receive_ts());
      // ����ǰ�����ֶ� trace_id��chid
      PROFILE_LOG(DEBUG, TRACE_ID
                  SOURCE_CHANNEL_ID
                  UPS_REQ_END_TIME
                  SCANNER_SIZE_BYTES, pkt.get_trace_id(), pkt.get_channel_id(), yysys::CTimeUtil::getTime(), new_scanner.get_size());
      return ret;
    }

    int ObUpdateServer::response_scanner(int32_t ret_code, ObPacket &pkt, common::ObCellNewScanner &new_scanner, ObDataBuffer &out_buffer)
    {
      int ret = OB_SUCCESS;
      ret =  response_data_(ret_code, new_scanner, pkt.get_packet_code(), pkt.get_api_version(),
                            pkt.get_request(), pkt.get_channel_id(), out_buffer, pkt.get_receive_ts());
      // ����ǰ�����ֶ� trace_id��chid
      PROFILE_LOG(DEBUG, TRACE_ID
                  SOURCE_CHANNEL_ID
                  UPS_REQ_END_TIME
                  SCANNER_SIZE_BYTES, pkt.get_trace_id(), pkt.get_channel_id(), yysys::CTimeUtil::getTime(), new_scanner.get_size());
      return ret;
    }

    int ObUpdateServer::response_buffer(int32_t ret_code, ObPacket &pkt, common::ObDataBuffer &buffer, const char *ret_string/* = NULL*/)
    {
      int ret = OB_SUCCESS;
      ThreadSpecificBuffer::Buffer *my_buffer = my_thread_buffer_.get_buffer();
      if (NULL == my_buffer)
      {
        YYSYS_LOG(ERROR, "alloc thread buffer fail");
        easy_request_wakeup(pkt.get_request());
        ret = OB_MEM_OVERFLOW;
      }
      else
      {
        ObDataBuffer out_buffer(my_buffer->current(), my_buffer->remain());
        ret = response_data_(ret_code, buffer, pkt.get_packet_code(), pkt.get_api_version(),
                             pkt.get_request(), pkt.get_channel_id(), out_buffer, pkt.get_receive_ts(), NULL, ret_string);
      }
      return ret;
    }

    int ObUpdateServer::response_result_(int32_t ret_code, int32_t cmd_type, int32_t func_version,
                                         easy_request_t* req, const uint32_t channel_id, int64_t receive_ts, const char *ret_string/* = NULL*/)
    {
      int ret = OB_SUCCESS;
      common::ObResultCode result_msg;
      ThreadSpecificBuffer::Buffer *my_buffer = my_thread_buffer_.get_buffer();
      if (NULL == eio_)
      {
        ret = OB_NOT_INIT;
      }
      else if (receive_ts > 0 && eio_->force_destroy_second > 0 && yysys::CTimeUtil::getTime() - receive_ts + RESPONSE_RESERVED_US > eio_->force_destroy_second * 1000000)
      {
        ret = OB_RESPONSE_TIME_OUT;
        YYSYS_LOG(ERROR, "pkt wait too long time, not send response: pkt_receive_ts=%ld, pkt_code=%d", receive_ts, cmd_type);
      }
      else if (NULL == my_buffer)
      {
        YYSYS_LOG(ERROR, "alloc thread buffer fail");
        easy_request_wakeup(req);
        ret = OB_MEM_OVERFLOW;
      }
      else
      {
        ObDataBuffer out_buff(my_buffer->current(), my_buffer->remain());
        result_msg.result_code_ = ret_code;
        if (NULL != ret_string)
        {
          result_msg.message_.assign_ptr(const_cast<char*>(ret_string), static_cast<int32_t>(strlen(ret_string) + 1));
        }
        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if (OB_SUCCESS == ret)
        {
          ret = send_response(cmd_type, func_version, out_buff, req, channel_id);
          if (OB_SUCCESS != ret)
          {
            YYSYS_LOG(WARN, "send response fail ret=%d conn=%p channel_id=%u result_msg=%d cmd_type=%d func_version=%d",
                ret, req, channel_id, ret_code, cmd_type, func_version);
          }
        }
        else
        {
          easy_request_wakeup(req);
          YYSYS_LOG(WARN, "send response fail ret=%d conn=%p channel_id=%u result_msg=%d cmd_type=%d func_version=%d",
              ret, req, channel_id, ret_code, cmd_type, func_version);
        }
      }
      return ret;
    }

    //add wangdonghui [ups_replication] 20170323 :b
    int ObUpdateServer::response_result1_(int32_t ret_code, int32_t cmd_type, int32_t func_version, easy_request_t* req,
                                          const uint32_t channel_id, int64_t message_residence_time_us, const char *ret_string/* = NULL*/)
    {
      int ret = OB_SUCCESS;
      common::ObResultCode result_msg;
      ThreadSpecificBuffer::Buffer *my_buffer = my_thread_buffer_.get_buffer();
      if (NULL == eio_)
      {
        ret = OB_NOT_INIT;
      }
      else if (NULL == my_buffer)
      {
        YYSYS_LOG(ERROR, "alloc thread buffer fail");
        easy_request_wakeup(req);
        ret = OB_MEM_OVERFLOW;
      }
      else
      {
        ObDataBuffer out_buff(my_buffer->current(), my_buffer->remain());
        result_msg.result_code_ = ret_code;
        if (NULL != ret_string)
        {
          result_msg.message_.assign_ptr(const_cast<char*>(ret_string), static_cast<int32_t>(strlen(ret_string) + 1));
        }
        ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if(OB_SUCCESS != ret)
        {}
        else
        {
            ret = serialization::encode_vi64(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position(), message_residence_time_us);
        }
        if (OB_SUCCESS == ret)
        {
          ret = send_response(cmd_type, func_version, out_buff, req, channel_id);
          if (OB_SUCCESS != ret)
          {
            YYSYS_LOG(WARN, "send response fail ret=%d conn=%p channel_id=%u result_msg=%d cmd_type=%d func_version=%d",
                ret, req, channel_id, ret_code, cmd_type, func_version);
          }
        }
        else
        {
          easy_request_wakeup(req);
          YYSYS_LOG(WARN, "send response fail ret=%d conn=%p channel_id=%u result_msg=%d cmd_type=%d func_version=%d",
              ret, req, channel_id, ret_code, cmd_type, func_version);
        }
      }
      return ret;
    }
    //add:e

    int ObUpdateServer::ups_end_transaction(ObPacket** packets, ScannerArray &scanner_array, const int64_t start_idx,
        const int64_t last_idx, UpsTableMgrTransHandle& handle, int32_t last_err_code)
    {
      bool rollback = false;
      int proc_ret = table_mgr_.end_transaction(handle, rollback);
      int resp_ret = (OB_SUCCESS == proc_ret) ? proc_ret : OB_RESPONSE_TIME_OUT;
      ObPacket **ob_packets = reinterpret_cast<ObPacket**>(packets);
      ThreadSpecificBuffer::Buffer *my_buffer = my_thread_buffer_.get_buffer();

      if (NULL == ob_packets
          || NULL == my_buffer)
      {
        YYSYS_LOG(WARN, "ob_packet or my_buffer null pointer start_idx=%ld last_idx=%ld", start_idx, last_idx);
      }
      else
      {
        int tmp_ret = OB_SUCCESS;
        for (int64_t i = start_idx; i < last_idx; i++)
        {
          if (NULL == ob_packets[i])
          {
            YYSYS_LOG(WARN, "ob_packet[%ld] null pointer, start_idx=%ld last_idx=%ld", i, start_idx, last_idx);
          }
          else if (NULL != scanner_array[i]
                  && 0 != scanner_array[i]->get_size())
          {
            ObDataBuffer out_buff(my_buffer->current(), my_buffer->remain());
            tmp_ret = response_data_(resp_ret, *(scanner_array[i]), OB_WRITE_RES, MY_VERSION,
                                      ob_packets[i]->get_request(), ob_packets[i]->get_channel_id(), out_buff);
            YYSYS_LOG(DEBUG, "response result index=%ld send_ret=%d resp_ret=%d proc_ret=%d", i, tmp_ret, resp_ret, proc_ret);
          }
          else
          {
            tmp_ret = response_result_(resp_ret, OB_WRITE_RES, MY_VERSION,
                ob_packets[i]->get_request(), ob_packets[i]->get_channel_id());
            YYSYS_LOG(DEBUG, "response result index=%ld send_ret=%d resp_ret=%d proc_ret=%d", i, tmp_ret, resp_ret, proc_ret);
          }
        }

        if (NULL == ob_packets[last_idx])
        {
          YYSYS_LOG(WARN, "last ob_packet[%ld] null pointer, start_idx=%ld", last_idx, start_idx);
        }
        else if (NULL != scanner_array[last_idx]
                && 0 != scanner_array[last_idx]->get_size())
        {
          ObDataBuffer out_buff(my_buffer->current(), my_buffer->remain());
          tmp_ret = response_data_(resp_ret, *(scanner_array[last_idx]), OB_WRITE_RES, MY_VERSION,
                                    ob_packets[last_idx]->get_request(), ob_packets[last_idx]->get_channel_id(), out_buff);
          YYSYS_LOG(DEBUG, "response result index=%ld send_ret=%d resp_ret=%d proc_ret=%d", last_idx, tmp_ret, resp_ret, proc_ret);
        }
        else
        {
          resp_ret = (OB_SUCCESS == last_err_code) ? resp_ret : last_err_code;
          // ���һ������ɹ��򷵻��ύ�Ľ�� ���ʧ���򷵻�apply�Ľ��
          tmp_ret = response_result_(resp_ret, OB_WRITE_RES, MY_VERSION,
              ob_packets[last_idx]->get_request(), ob_packets[last_idx]->get_channel_id());
          YYSYS_LOG(DEBUG, "response result index=%ld send_ret=%d resp_ret=%d proc_ret=%d", last_idx, tmp_ret, resp_ret, proc_ret);
        }
      }

      int64_t trans_proc_time = yysys::CTimeUtil::getTime() - start_trans_timestamp_;
      static __thread int64_t counter = 0;
      counter++;
      if (trans_proc_time > config_.trans_proc_time_warn)
      {
        YYSYS_LOG(WARN, "transaction process time is too long, process_time=%ld cur_time=%ld response_num=%ld "
            "last_log_network_elapse=%ld last_log_disk_elapse=%ld trans_counter=%ld "
            "read_task_queue_size=%zu write_task_queue_size=%zu lease_task_queue_size=%zu",
            trans_proc_time, yysys::CTimeUtil::getTime(), last_idx - start_idx + 1,
            log_mgr_.get_last_net_elapse(), log_mgr_.get_last_disk_elapse(), counter,
            read_thread_queue_.size(), write_thread_queue_.size(), lease_thread_queue_.size());
        counter = 0;
      }
      FILL_TRACE_LOG("process_time=%ld cur_time=%ld response_num=%ld "
                "last_log_network_elapse=%ld last_log_disk_elapse=%ld trans_counter=%ld "
                "read_task_queue_size=%zu write_task_queue_size=%zu lease_task_queue_size=%zu",
                trans_proc_time, yysys::CTimeUtil::getTime(), last_idx - start_idx + 1,
                log_mgr_.get_last_net_elapse(), log_mgr_.get_last_disk_elapse(), counter,
                read_thread_queue_.size(), write_thread_queue_.size(), lease_thread_queue_.size());
      OB_STAT_INC(UPDATESERVER, UPS_STAT_BATCH_COUNT, 1);
      OB_STAT_INC(UPDATESERVER, UPS_STAT_BATCH_TIMEU, GET_TRACE_TIMEU());
      FILL_TRACE_LOG("resp_ret=%d proc_ret=%d", resp_ret, proc_ret);
      PRINT_TRACE_LOG();
      return proc_ret;
    }

    //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
    const common::ObServer& ObUpdateServer::get_sys_ups()
    {
      int retry_time = 3;
      if(!sys_ups_master_.is_valid())
      {
        for(int i = 0; i< retry_time; i++)
        {
          if(OB_SUCCESS != ups_rpc_stub_.get_inst_master_ups(root_server_,sys_ups_master_ ,0,DEFAULT_NETWORK_TIMEOUT))
          {
            YYSYS_LOG(ERROR, "get_inst_master_ups fail,retry:%d", i);
          }
          else
          {
            break;
          }
        }
      }

      return sys_ups_master_;
    }
    //add 20150701:e

    void ObUpdateServer::set_log_sync_delay_stat_param()
    {
      YYSYS_LOG(INFO, "set_log_sync_delay_stat_param log_sync_delay_warn_time_threshold=%s "
                "log_sync_delay_tolerable_time_threshold=%s "
                "log_sync_delay_warn_report_interval=%s "
                "max_n_lagged_log_allowed=%s "
                "disk_warn_threshold=%s "
                "net_warn_threshold=%s",
                config_.log_sync_delay_warn_time_threshold.str(),
                config_.log_sync_delay_tolerable_time_threshold.str(),
                config_.log_sync_delay_warn_report_interval.str(),
                config_.max_n_lagged_log_allowed.str(),
                config_.disk_warn_threshold.str(),
                config_.net_warn_threshold.str());
      log_mgr_.get_delay_stat().set_delay_warn_time_us(config_.log_sync_delay_warn_time_threshold);
      log_mgr_.get_delay_stat().set_delay_tolerable_time_us(config_.log_sync_delay_tolerable_time_threshold);
      log_mgr_.get_delay_stat().set_report_interval_us(config_.log_sync_delay_warn_report_interval);
      log_mgr_.get_delay_stat().set_max_n_lagged_log_allowed(config_.max_n_lagged_log_allowed);
      log_mgr_.set_disk_warn_threshold_us(config_.disk_warn_threshold);
      log_mgr_.set_net_warn_threshold_us(config_.net_warn_threshold);
      log_mgr_.get_clog_stat().set_disk_warn_us(config_.disk_warn_threshold);
      log_mgr_.get_clog_stat().set_net_warn_us(config_.net_warn_threshold);
    }

    void ObUpdateServer::set_log_replay_thread_param()
    {
      YYSYS_LOG(INFO, "set_log_replay_thread_param replay_wait_time=%s fetch_log_wait_time=%s",
                config_.replay_wait_time.str(), config_.fetch_log_wait_time.str());
      log_replay_thread_.set_replay_wait_time_us(config_.replay_wait_time);
      log_replay_thread_.set_fetch_log_wait_time_us(config_.fetch_log_wait_time);
    }

    void ObUpdateServer::set_replay_log_to_queue_param()
    {
        YYSYS_LOG(INFO,"set_replay_log_to_queue_param replay_to_queue_wait_time=%s replay_to_disk_wait_time=%s",
                  config_.replay_to_queue_wait_time.str(),config_.replay_to_disk_wait_time.str());
        replay_log_to_queue_thread_.set_replay_to_queue_wait_time_us(config_.replay_to_queue_wait_time);
        replay_log_to_queue_thread_.set_replay_to_disk_wait_time_us(config_.replay_to_disk_wait_time);
    }

    void ObUpdateServer::apply_conf()
    {
      YYSYS_LOG(INFO, "==========apply conf begin==========");

      set_log_replay_thread_param();
      set_log_sync_delay_stat_param();

      set_replay_log_to_queue_param();
      //[337]
      log_mgr_.set_async_load_log_gap(config_.async_load_log_gap);

      table_mgr_.set_replay_checksum_flag(0 != config_.replay_checksum_flag);
      YYSYS_LOG(INFO, "set_replay_checksum_flag replay_checksum_flag=%s",
                config_.replay_checksum_flag.str());

      slave_mgr_.set_log_sync_timeout_us(config_.log_sync_timeout);
      YYSYS_LOG(INFO, "set_log_sync_timeout log_sync_timeout=%s",
                config_.log_sync_timeout.str());

      ob_set_memory_size_limit(config_.total_memory_limit);
      YYSYS_LOG(INFO, "set_memory_size_limit total_memory_limit=%s",
                config_.total_memory_limit.str());

      //del peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//      set_lsync_server(config_.lsync_ip, (int32_t)config_.lsync_port);
//      YYSYS_LOG(INFO, "set_lsync_server lsync_ip=%s lsync_port=%s",
//                config_.lsync_ip.str(), config_.lsync_port.str());
      //del 20150701:e

      g_conf.using_static_cm_column_id = (0 != config_.using_static_cm_column_id);
      YYSYS_LOG(INFO, "set using_static_cm_column_id=%s", STR_BOOL(g_conf.using_static_cm_column_id));

      g_conf.using_hash_index = (0 != config_.using_hash_index);
      YYSYS_LOG(INFO, "set using_hash_index=%s", STR_BOOL(g_conf.using_hash_index));

      MemTableAttr memtable_attr;
      if (OB_SUCCESS == table_mgr_.get_memtable_attr(memtable_attr))
      {
        memtable_attr.total_memlimit = config_.table_memory_limit;
        table_mgr_.set_memtable_attr(memtable_attr);
        YYSYS_LOG(INFO, "set_memtable_attr table_memory_limit=%s",
                  config_.table_memory_limit.str());
      }

      int64_t high_prio = config_.high_prio_quota_percent;
      int64_t low_prio = config_.low_prio_quota_percent;
      int64_t normal_prio = 100 - high_prio - low_prio;
      v4si prio = {high_prio, normal_prio/2, normal_prio/2, low_prio};
      if (normal_prio <= 0)
      {
        YYSYS_LOG(ERROR, "high_prio_percent[%ld] + low_prio_percent[%ld] >= 100", high_prio, low_prio);
      }
      else
      {
        trans_executor_.TransHandlePool::set_prio_quota(prio);
        //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
        trans_executor_.DistributedTransHandlePool::set_prio_quota(prio);
        //add 20150701:e
      }
      if (static_cast<int64_t>(config_.low_priv_cur_percent) >= 0)
      {
        read_thread_queue_.set_low_priv_cur_percent(config_.low_priv_cur_percent);
        YYSYS_LOG(INFO, "set_low_priv_cur_percent low_priv_cur_percent=%s",
                  config_.low_priv_cur_percent.str());
      }

      sstable_query_.enlarge_cache_size(config_.blockcache_size, config_.blockindex_cache_size);
      YYSYS_LOG(INFO, "enlarge_cache_size, blockcache_size=%s, blockindex_cache_size=%s",
                config_.blockcache_size.str(), config_.blockindex_cache_size.str());

      const char *disk_delay_warn_param = config_.disk_delay_warn_param;
      if (NULL != disk_delay_warn_param)
      {
        const int64_t PARAM_NUM = 4;
        int64_t params[PARAM_NUM] = {0};
        str_to_int_pt callbacks[PARAM_NUM] = {get_time, get_time, ob_atoll, ob_atoll};
        if (PARAM_NUM == split_string(disk_delay_warn_param, params, callbacks, PARAM_NUM))
        {
          log_mgr_.get_clog_stat().disk_warn_stat_.set_max_value_limit(params[0]);
          log_mgr_.get_clog_stat().disk_warn_stat_.set_stb_value_limit(params[1]);
          log_mgr_.get_clog_stat().disk_warn_stat_.set_gt_stb_count_limit(params[2]);
          log_mgr_.get_clog_stat().disk_warn_stat_.set_window_count(params[3]);
        }
      }

      const char *net_delay_warn_param = config_.net_delay_warn_param;
      if (NULL != net_delay_warn_param)
      {
        const int64_t PARAM_NUM = 4;
        int64_t params[PARAM_NUM] = {0};
        str_to_int_pt callbacks[PARAM_NUM] = {get_time, get_time, ob_atoll, ob_atoll};
        if (PARAM_NUM == split_string(net_delay_warn_param, params, callbacks, PARAM_NUM))
        {
          log_mgr_.get_clog_stat().net_warn_stat_.set_max_value_limit(params[0]);
          log_mgr_.get_clog_stat().net_warn_stat_.set_stb_value_limit(params[1]);
          log_mgr_.get_clog_stat().net_warn_stat_.set_gt_stb_count_limit(params[2]);
          log_mgr_.get_clog_stat().net_warn_stat_.set_window_count(params[3]);
        }
      }

      trans_executor_.TransHandlePool::set_cpu_affinity(config_.trans_thread_start_cpu, config_.trans_thread_end_cpu);
      //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
      trans_executor_.DistributedTransHandlePool::set_cpu_affinity(config_.trans_thread_start_cpu, config_.trans_thread_end_cpu);
      //add 20150701:e
      trans_executor_.TransCommitThread::set_cpu_affinity(config_.commit_bind_core_id);

      replay_worker_.set_cpu_affinity(config_.replay_thread_start_cpu, config_.replay_thread_end_cpu);

      YYSYS_LOG(INFO, "==========apply conf end==========");
    }

    //add peiouya [MultiUPS] [UPS_SYNC_Frozen] 20150521:b
    bool ObUpdateServer::check_if_permit_freeze()
    {
      bool ret = false;
      uint64_t last_frozen_version = 0;
      if (OB_SUCCESS != (ret = table_mgr_.get_last_frozen_memtable_version(last_frozen_version)))
      {
        YYSYS_LOG(WARN, "fail to get last frozen version from table_mgr. ret = %d", ret);
      }
      else if (-1 != static_cast<int64_t>(last_frozen_version_)
               && -1 != static_cast<int64_t>(to_be_frozen_version_)
               && last_frozen_version_ == last_frozen_version)
      {
        ret = true;
      }
      return ret;
    }
    //add 20150615:e

    //add peiouya [MultiUPS] [UPS_SYNC_Frozen] 20150521:b
    bool ObUpdateServer::check_if_need_freeze_for_hb_major()
    {
      bool ret = false;
      uint64_t last_frozen_version = 0;
      if (OB_SUCCESS != (ret = table_mgr_.get_last_frozen_memtable_version(last_frozen_version)))
      {
        YYSYS_LOG(WARN, "fail to get last frozen version from table_mgr. ret = %d", ret);
      }
      else if (-1 != static_cast<int64_t>(last_frozen_version_)
               && -1 != static_cast<int64_t>(to_be_frozen_version_)
               && last_frozen_version == to_be_frozen_version_ - 1)
      {
        ret = true;
      }
      return ret;
    }
    //add 20150615:e

    //add peiouya [MultiUPS] [UPS_SYNC_Frozen] 20150810:b
    void ObUpdateServer::major_freeze_accord_hb_guard_unlock()
    {
      major_freeze_accord_hb_guard_.done ();
    }
    //add 20150810:e

    //add peiouya [MultiUPS] [UPS_SYNC_Frozen] 20150811:b
    //expr:when disable_start_write_session && role=master, some packet can still process
    bool ObUpdateServer::can_process_with_disable_sws(const int pcode)
    {
      return ((OB_FAKE_WRITE_FOR_KEEP_ALIVE == pcode
               || OB_SWITCH_SCHEMA == pcode
               || OB_UPS_FORCE_FETCH_SCHEMA == pcode
               || OB_UPS_SWITCH_COMMIT_LOG == pcode
               || OB_WRITE_TRANS_STAT == pcode)
              && (ObUpsRoleMgr::MASTER == role_mgr_.get_role ())
              );
    }
    //add 20150811:e

    int ObUpdateServer::ups_reload_conf(const int32_t version,
        easy_request_t* req, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;

      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }

      if (OB_SUCCESS == ret)
      {
        ret = config_mgr_.reload_config();
      }
      ret = response_result_(ret, OB_UPS_RELOAD_CONF_RESPONSE, MY_VERSION, req, channel_id);

      return ret;
    }


    //del peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//    int ObUpdateServer::slave_report_quit()
//    {
//      int err = OB_SUCCESS;
//      //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
////      if (ObUpsRoleMgr::MASTER == role_mgr_.get_role()
////          && ObiRole::MASTER == obi_role_.get_role())
////      {
////        //do nothing
////      }
//      if (ObUpsRoleMgr::MASTER == role_mgr_.get_role())
//      {
//        //do nothing
//      }
//      //mod 20150701:e
//      else if (ObUpsRoleMgr::SLAVE == role_mgr_.get_role())
//      {
//        if (OB_SUCCESS != (err = ups_rpc_stub_.slave_quit(ups_master_, get_self(), DEFAULT_SLAVE_QUIT_TIMEOUT)))
//        {
//          YYSYS_LOG(WARN, "fail to send slave quit to master. err = %d, master = %s", err, ups_master_.to_cstring());
//        }
//      }
//      //del peiouya [MultiUPS] [DELETE_OBI] 20150701:b
////      else
////      {
////        if (OB_SUCCESS != (err = ups_rpc_stub_.slave_quit(ups_inst_master_, get_self(), DEFAULT_SLAVE_QUIT_TIMEOUT)))
////        {
////          YYSYS_LOG(WARN, "fail to send slave quit to master. err = %d, master = %s", err, ups_inst_master_.to_cstring());
////        }
////      }
//      //del 20150701:e

//      return err;
//    }
    //del 20150701:e

    int ObUpdateServer::ups_update_lease(const common::ObMsgUpsHeartbeat &hb)
    {
      int err = OB_SUCCESS;
      //add wangjiahao [Paxos ups_replication] 20150718 :b
      bool is_master_changed = false;
      //add :e
      //add lbzhong [Paxos ups_replication] 20151030:b
      //mod chujiajia [Paxos rs_election] 20151229
      //slave_mgr_.set_quorum_scale(hb.ups_quorum_);
      if(hb.ups_quorum_ != config_.quorum_scale)
      {
        config_.quorum_scale = hb.ups_quorum_;
        config_mgr_.dump2file();
        config_mgr_.get_update_task().write2stat();
        YYSYS_LOG(INFO, "receive hb, new quorum scale = %ld.", (int64_t)config_.quorum_scale);
      }
      //mod:e
      //add:e
      int64_t cur_time_us = yysys::CTimeUtil::getTime();
      //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
      //YYSYS_LOG(DEBUG, "receive hb, master_addr_=%s, self_lease=%ld, obi_role=%s, cur_time=%ld, lease_time=%ld",
      //    hb.ups_master_.to_cstring(), hb.self_lease_, hb.obi_role_.get_role_str(), cur_time_us, hb.self_lease_ - cur_time_us);
      YYSYS_LOG(DEBUG, "receive hb, master_addr_=%s, self_lease=%ld, cur_time=%ld, lease_time=%ld",
          hb.ups_master_.to_cstring(), hb.self_lease_, cur_time_us, hb.self_lease_ - cur_time_us);
      //mod 20150701:e

      //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160318:b
      /*Exp: renew offline_version_ of current ups base on */
      if (static_cast<uint64_t>(hb.offline_version_) != offline_version_)
      {
        offline_version_ = static_cast<uint64_t>(hb.offline_version_);
      }
      //add 20160318:e

      if (hb.schema_version_ > schema_version_)
      {
        schema_version_ = hb.schema_version_;
      }
      if (hb.self_lease_ == OB_MAX_UPS_LEASE_DURATION_US)
      {
        lease_expire_time_us_ = hb.self_lease_;
        YYSYS_LOG(INFO, "rootserver down, receive the overlength lease. lease_time=%ld", OB_MAX_UPS_LEASE_DURATION_US);
      }
      if (OB_SUCCESS != (err = config_mgr_.got_version(hb.config_version_)))
      {
        YYSYS_LOG(WARN, "Process config failed, ret: [%d]", err);
      }
      else if (hb.self_lease_ >= lease_expire_time_us_)
      {
          int64_t cur_time_us = yysys::CTimeUtil::getTime();
          if(hb.self_lease_ < cur_time_us)
          {
              err = OB_ERROR;
              YYSYS_LOG(WARN,"Receive timeout lease[%ld], cur_time[%ld], ignore it.",
                        hb.self_lease_, cur_time_us);
          }
          else
          {
              lease_expire_time_us_ = hb.self_lease_;
          }
      }
      else if (OB_MAX_UPS_LEASE_DURATION_US == lease_expire_time_us_)
      {
        YYSYS_LOG(INFO, "maybe rootserver restarted, receive normal lease.lease=%ld", hb.self_lease_);
        lease_expire_time_us_ = hb.self_lease_;
      }
      else
      {
        err = OB_ERROR;
        YYSYS_LOG(WARN, "lease should not rollback, self_lease=%ld, new_lease=%ld",
            lease_expire_time_us_, hb.self_lease_);
      }

      bool is_role_change = false;
      //del peiouya [MultiUPS] [DELETE_OBI] 20150701:b
      //bool is_obi_change = false;
      //del 20150701:e

      if (OB_SUCCESS == err)
      {
        //del peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//        if ((ObiRole::INIT != hb.obi_role_.get_role())
//            && obi_role_.get_role() != hb.obi_role_.get_role())
//        {
//          YYSYS_LOG(INFO, "UPS obi_role change. obi_role=%s", hb.obi_role_.get_role_str());
//          is_obi_change = true;
//        }
        //del 20150701:e
        if (!(hb.ups_master_ == ups_master_))
        {
          YYSYS_LOG(INFO, "master_ups addr has been change. old_master=%s", ups_master_.to_cstring());
          //add wangjiahao [Paxos] 20150718 :b
          is_master_changed = true;
          //add :e
          YYSYS_LOG(INFO, "new master addr =%s", hb.ups_master_.to_cstring());
          ups_master_.set_ipv4_addr(hb.ups_master_.get_ipv4(), hb.ups_master_.get_port());
          //add lbzhong [Paxos Cluster.Balance] 201607011:b
          ups_master_.cluster_id_ = hb.ups_master_.cluster_id_;
          //add:e

        }

        if (ObUpsRoleMgr::MASTER == role_mgr_.get_role() && !(hb.ups_master_ == self_addr_))
        {
          YYSYS_LOG(INFO, "UPS role change, master ups need change to slave. new_master=%s",
              hb.ups_master_.to_cstring());
          is_role_change = true;
        }
        else if (ObUpsRoleMgr::SLAVE == role_mgr_.get_role() && (hb.ups_master_ == self_addr_))
        {
          YYSYS_LOG(INFO, "UPS role change, slave ups need change to master.");
          is_role_change = true;
        }
      }

      if(err == OB_SUCCESS)
      {
          if(!hb.minor_freeze_flag_)
          {
              minor_freeze_done_ = UPS_NOT_FROZEN;
          }
      }

      //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//      if (OB_SUCCESS == err)
//      {
//       if (is_obi_change && is_role_change)
//       {
//         if (ObiRole::MASTER == obi_role_.get_role()
//             && ObUpsRoleMgr::MASTER == role_mgr_.get_role())
//         {
//           YYSYS_LOG(INFO, "switch happen. master_master ====> slave_slave");
//           err = master_switch_to_slave(true, true);
//         }
//         else if (ObiRole::SLAVE == obi_role_.get_role()
//             && ObUpsRoleMgr::SLAVE == role_mgr_.get_role())
//         {
//           YYSYS_LOG(INFO, "switch happen. slave_slave ===> master_master");
//           err = switch_to_master_master();
//         }
//         else
//         {
//           YYSYS_LOG(INFO, "switch happen.");
//           err = slave_change_role(true, true);
//           if (OB_SUCCESS != err)
//           {
//             YYSYS_LOG(INFO, "change obi_role and role_mgr_ fail.");
//           }
//         }
//       }
//       else if (true == is_obi_change && false == is_role_change)
//       {
//         if (ObUpsRoleMgr::MASTER == role_mgr_.get_role())
//         {
//           if (ObiRole::MASTER == obi_role_.get_role())
//           {
//             YYSYS_LOG(INFO, "switch happen. master_master ====> slave_master");
//             err = master_switch_to_slave(true, false);
//           }
//           else
//           {
//             YYSYS_LOG(INFO, "switch happen. slave_master ===> master_master");
//             err = switch_to_master_master();
//           }
//         }
//         else
//         {
//           YYSYS_LOG(INFO, "switch happen.");
//           err = slave_change_role(true, false);
//           if (OB_SUCCESS != err)
//           {
//             YYSYS_LOG(INFO, "change obi_role and role_mgr_ fail.");
//           }
//         }
//       }
//       else if (false == is_obi_change && true == is_role_change)
//       {
//         if (ObiRole::MASTER == obi_role_.get_role())
//         {
//           if (ObUpsRoleMgr::MASTER == role_mgr_.get_role())
//           {
//             YYSYS_LOG(INFO, "switch happen. master_master ===> master_slave");
//             err = master_switch_to_slave(false, true);
//           }
//           else
//           {
//             YYSYS_LOG(INFO, "switch happen. master_slave ===> master_master");
//             err = switch_to_master_master();
//           }
//         }
//         else
//         {
//           err = slave_change_role(false, true);
//         }
//       }
//     }

      if (OB_SUCCESS == err)
      {
        if (is_role_change)
        {
          if (ObUpsRoleMgr::MASTER == role_mgr_.get_role())
          {
            YYSYS_LOG(INFO, "switch happen. master ====> slave");
            err = master_switch_to_slave(true);
          }
          else if (ObUpsRoleMgr::SLAVE == role_mgr_.get_role())
          {
            YYSYS_LOG(INFO, "switch happen. slave ====> master");
            //mod peiouya [MultiUPS] [DELETE_OBI] 20150723:b
            //err = slave_change_role(true);
            err = switch_to_master();
            //mod 20150723:e
          }
        }
       //modify wagndonghui [ups_replication] 20170220 :b
       //add wangjiahao [Paxos ups_replication_tmplog] :b
       //else if (is_master_changed) //obi not changed && role not changed
       else if (is_master_changed && !(hb.ups_master_ == self_addr_))
       // modify :e
       {
           const int64_t report_interval_us = 1000000;
           const int64_t wait_us = 10000;
           YYSYS_LOG(INFO, "change master ups.");
           YYSYS_LOG(INFO, "wait replay thread to stop.");
           //for(int64_t last_report_ts = 0, cur_ts = report_interval_us; !stoped_ && !log_replay_thread_.wait_stop(); cur_ts += wait_us)
           for(int64_t last_report_ts = 0, cur_ts = report_interval_us;
               !stoped_ && (!log_replay_thread_.wait_stop() || !replay_log_to_queue_thread_.wait_stop() || !async_load_log_thread_.wait_stop());
               cur_ts += wait_us)
           {
             if (cur_ts - last_report_ts >= report_interval_us)
             {
               last_report_ts = cur_ts;
               YYSYS_LOG(INFO, "wait replay_thread: %s", to_cstring(log_mgr_));
             }
             usleep(static_cast<useconds_t>(wait_us));
           }
           if (stoped_)
           {
             err = OB_CANCELED;
           }
           if (OB_SUCCESS != err)
           {
             YYSYS_LOG(ERROR, "wait replay thread, err=%d, %s", err, to_cstring(log_mgr_));
           }
           else
           {
               ObLogCursor cursor;
               log_mgr_.get_replayed_cursor(cursor);
               if (OB_SUCCESS != (err = log_mgr_.start_log(cursor)))
               {
                 YYSYS_LOG(ERROR, "start_log(cursor=%s)=>%d", cursor.to_str(), err);
               }
               else
               {
                 YYSYS_LOG(INFO, "master changed. start_log from flushed_cursor to replayed_cursor=(%s)", cursor.to_str());
               }

               if(OB_SUCCESS != (err = replay_log_src_.reset_prefetch_log_buffer2()))
               {
                   YYSYS_LOG(ERROR,"reset prefetch log buffer2=>%d",err);
               }
               else
               {
                   YYSYS_LOG(INFO,"master changed. prefetch_log_cursor_ reset");
               }
           }
           //while(!stoped_ && !log_replay_thread_.wait_start())
           while(!stoped_ && (!log_replay_thread_.wait_start() || !replay_log_to_queue_thread_.wait_start() || !async_load_log_thread_.wait_start()))
           {
             YYSYS_LOG(INFO, "wait replay_thread start");
             usleep(static_cast<useconds_t>(wait_us));
           }
           YYSYS_LOG(INFO, "wait log_replay_thread start to work succ.");
        }
        //add :e
      }
     //mod 20150701:e
     //del peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//      if (is_obi_change)
//      {
//        settled_obi_role_.set_role(obi_role_.get_role());
//      }
      //del 20150701:e

      //add peiouya [MultiUPS] [UPS_SYNC_Frozen] 20150521:b
      uint64_t last_frozen_version = 0;
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = table_mgr_.get_last_frozen_memtable_version(last_frozen_version)))
      {
        YYSYS_LOG(WARN, "fail to get last frozen version from table_mgr. ret = %d", ret);
      }
      else if (OB_SUCCESS == ret && ObUpsRoleMgr::MASTER == role_mgr_.get_role()
          && ObUpsRoleMgr::ACTIVE == role_mgr_.get_state())
      {
        if (-1 == hb.last_frozen_version_)
        {
          //NOTHING TODO
        }
        //mod peiouya [MultiUPS] [UPS_SYNC_Frozen] 20150521:b
        //else if (hb.last_frozen_version_ == static_cast<int64_t>(last_frozen_version) + 1)
        //{
        //  atomic_exchange(&last_frozen_version_, hb.last_frozen_version_);
        //  submit_major_freeze_accord_hb();
        //}
        //else if (hb.last_frozen_version_ == static_cast<int64_t>(last_frozen_version))
        //{
        //  //NOTHING TODO
        //}
        //mod peiouya [MultiUPS] [UPS_SYNC_Frozen] 20150811:b
        //else if (-1 == hb.to_be_frozen_version_
        //         || (hb.to_be_frozen_version_ != hb.last_frozen_version_
        //              && hb.to_be_frozen_version_ != hb.last_frozen_version_ + 1))
        else if (hb.to_be_frozen_version_ != hb.last_frozen_version_
                 && hb.to_be_frozen_version_ != hb.last_frozen_version_ + 1)
        //mod 20150811:e
        {
          YYSYS_LOG(ERROR, "will kill self. becase of cur sys heartbeat error. first to_be_frozen_version_:%ld must greater than zero.\
          second to_be_frozen_version_:%ld must in [%ld, %ld]",
                            hb.to_be_frozen_version_, hb.to_be_frozen_version_, hb.last_frozen_version_, hb.last_frozen_version_ + 1);
          kill(getpid(), SIGTERM);
        }
        else if (hb.to_be_frozen_version_ == static_cast<int64_t>(last_frozen_version) + 1)
        {
          if (hb.last_frozen_version_ > static_cast<int64_t>(last_frozen_version_))
          {
            atomic_exchange(&last_frozen_version_, hb.last_frozen_version_);
          }
          atomic_exchange(&to_be_frozen_version_, hb.to_be_frozen_version_);
          submit_major_freeze_accord_hb();
        }
        else if (hb.to_be_frozen_version_ == static_cast<int64_t>(last_frozen_version))
        {
          if (hb.last_frozen_version_ > static_cast<int64_t>(last_frozen_version_))
          {
            atomic_exchange(&last_frozen_version_, hb.last_frozen_version_);
          }
          if (hb.last_frozen_version_ == static_cast<int64_t>(last_frozen_version))
          {
            trans_executor_.get_session_mgr().enable_start_write_session();
            //add peiouya [MultiUPS] [UPS_SYNC_Frozen] 20150810:b
            trans_executor_.get_session_mgr ().unlock_write_session_for_sync_freeze ();
            //add 20150811:e
          }
        }
        //mod 20150615:e
        //mod hongchen [FREEZE_BUG_fix] 20170705:b
        //BUG fix
        //this case process ups active freeze, means freezed by ups itself
        else if (hb.to_be_frozen_version_ == static_cast<int64_t>(last_frozen_version) - 1)
        {
          YYSYS_LOG(DEBUG, "UPS freezed, BUT RS has not received UPS's Freeze signal! we think it's ok!");
          //nothing todo
        }
        else
        {
          //YYSYS_LOG(ERROR, "will kill self. cur ups is valid becase cur ups last_frozen_version:%ld not in [%ld, %ld]",
          //                  last_frozen_version, hb.last_frozen_version_ - 1, hb.last_frozen_version_);
          YYSYS_LOG(ERROR, "will kill self. cur ups is valid becase cur ups last_frozen_version:%ld,\
                            BUT in RS's hb, to_be_frozen_version_:%ld,last_frozen_version_:%ld",
                            last_frozen_version, hb.to_be_frozen_version_, hb.last_frozen_version_);
          kill(getpid(), SIGTERM);
        }
        //hongchen [FREEZE_BUG_fix] 20170705:e
        if(err == OB_SUCCESS)
        {
            if(hb.minor_freeze_flag_ && UPS_NOT_FROZEN == minor_freeze_done_)
            {
                set_timer_minor_freeze();
            }
        }
      }
      //add 20150521:e
      return err;
    }

     
    int ObUpdateServer::ups_revoke_lease(const ObMsgRevokeLease &revoke_info)
    {
      int err = OB_SUCCESS;
      if (revoke_info.lease_ == lease_expire_time_us_ && self_addr_ == revoke_info.ups_master_)
      {
        YYSYS_LOG(INFO, "revoke info check succ. translate to slave");
        ups_master_.reset();

        int64_t wait_write_session_end_timeout_us = config_.switch_master_wait_write_session_end_timeout;
        trans_executor_.get_session_mgr().disable_start_write_session();

        int64_t before_wait_session_ts = yysys::CTimeUtil::getTime();
        if(OB_SUCCESS != trans_executor_.get_session_mgr().wait_write_session_end_and_lock(wait_write_session_end_timeout_us))
        {
            YYSYS_LOG(ERROR,"wait_write_session_end_and_lock failed, write session still running, can not switch master");
            err = OB_UPS_TRANS_RUNNING;
            trans_executor_.get_session_mgr().enable_start_write_session(); //switch failed, open session switch
        }
        else
        {
            trans_executor_.get_session_mgr().unlock_write_session();

            //the left time for waiting commit_point
            int64_t left_wait_ts = wait_write_session_end_timeout_us - (yysys::CTimeUtil::getTime() - before_wait_session_ts);
            left_wait_ts = left_wait_ts >= 100*1000 ? left_wait_ts:100*1000;
            YYSYS_LOG(INFO,"wait_write_session_end_and_lock success, wait_ts=%ld, left_ts=%ld",
                      wait_write_session_end_timeout_us - left_wait_ts, left_wait_ts);

            //wait for commit_log_id > fill_log_max_log_id
            int64_t commit_log_id = get_log_mgr().get_last_wrote_commit_point();
            int64_t fill_log_max_log_id = get_log_mgr().get_fill_log_max_log_id();
            int64_t flush_queue_size = trans_executor_.get_flush_queue_size();
            int wait_interval = 30*1000;
            int64_t start_time = yysys::CTimeUtil::getTime();
            while(start_time + left_wait_ts > yysys::CTimeUtil::getTime())
            {
                if(flush_queue_size == 0 && commit_log_id > fill_log_max_log_id)
                {
                    break;
                }
                else
                {
                    usleep(wait_interval);
                    fill_log_max_log_id = get_log_mgr().get_fill_log_max_log_id();
                    commit_log_id = get_log_mgr().get_last_wrote_commit_point();
                    flush_queue_size = trans_executor_.get_flush_queue_size();
                    YYSYS_LOG(INFO,"wait for log commit,flush_queue_size=%ld,commit_log_id=%ld,fill_log_max_log_id=%ld",
                             flush_queue_size, commit_log_id, fill_log_max_log_id);
                }
            }

            //judge the result is OK or timeout?
            if(flush_queue_size >0 || commit_log_id <= fill_log_max_log_id)
            {
                YYSYS_LOG(ERROR,"wait for log commit failed,flush_queue_size=%ld,commit_log_id=%ld,fill_log_max_log_id=%ld",
                          flush_queue_size, commit_log_id, fill_log_max_log_id);
                err = OB_UPS_TRANS_RUNNING;
                trans_executor_.get_session_mgr().enable_start_write_session(); // switch failed.
            }
            else
            {
                YYSYS_LOG(INFO, "wait transaction commit succ, flush_queue_size=%ld,commit_log_id=%ld,fill_log_max_log_id=%ld",
                          flush_queue_size, commit_log_id, fill_log_max_log_id);
                YYSYS_LOG(INFO, "switch happen. master ====> slave");
                err = master_switch_to_slave(true);

            }

        }

      }
      else
      {
        YYSYS_LOG(INFO, "revoke info check false, refuse to translate to slave");
        err = OB_ERROR;
      }
      return err;
    }

    int ObUpdateServer::ups_rs_revoke_lease(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int err = OB_SUCCESS;
      common::ObResultCode result_msg;
      result_msg.result_code_ = OB_SUCCESS;
      if (MY_VERSION != version)
      {
        YYSYS_LOG(WARN, "version not equal. version=%d, MY_VERSION=%d", version, MY_VERSION);
        result_msg.result_code_ = OB_ERROR_FUNC_VERSION;
      }
      ObMsgRevokeLease revoke_info;
      if (OB_SUCCESS == err && OB_SUCCESS == result_msg.result_code_)
      {
        err = revoke_info.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (OB_SUCCESS != err)
        {
          YYSYS_LOG(WARN, "failed to serialize hb_info, err=%d", err);
        }
      }
      //����
      if (OB_SUCCESS == err && OB_SUCCESS == result_msg.result_code_)
      {
        result_msg.result_code_ = ups_revoke_lease(revoke_info);
        if (OB_SUCCESS != result_msg.result_code_)
        {
          YYSYS_LOG(WARN, "fail to revoke lease, err = %d", result_msg.result_code_);
        }
      }
      if (OB_SUCCESS == err)
      {
        result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if(OB_SUCCESS != err)
        {
          YYSYS_LOG(WARN, "fail to serialize, err=%d", err);
        }
      }

      if (OB_SUCCESS == err)
      {
        err = send_response(OB_RS_UPS_REVOKE_LEASE_RESPONSE, MY_VERSION, out_buff, req, channel_id);
        if (OB_SUCCESS != err)
        {
          YYSYS_LOG(WARN, "fail to send response. err = %d", err);
        }
      }

      return err;
    }

    bool ObUpdateServer::can_serve_read_req(const bool is_consistency_read, const int64_t query_version)
    {
      bool is_provide_service = false;
      ObConsistencyType::Type consistency_type = static_cast<ObConsistencyType::Type>(
        static_cast<int64_t>(config_.consistency_type));
      //���ڲ�����ȡ��һ���Լ���

      if (is_consistency_read)
      {
        //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//        if (ObiRole::MASTER == obi_role_.get_role()
//            && ObUpsRoleMgr::MASTER == role_mgr_.get_role()
//            && ObUpsRoleMgr::ACTIVE == role_mgr_.get_state())
        if (ObUpsRoleMgr::MASTER == role_mgr_.get_role()
            && ObUpsRoleMgr::ACTIVE == role_mgr_.get_state())
        {
          is_provide_service = true;
        }
      }
      else
      {
        if (ObConsistencyType::WEAK_CONSISTENCY == consistency_type || query_version > 0)
        {
          is_provide_service = true;
        }
        else
        {
          if (ObUpsRoleMgr::ACTIVE == role_mgr_.get_state())
          {
            is_provide_service = true;
          }
        }
      }

      if (!is_provide_service)
      {
        if (REACH_TIME_INTERVAL(10 * 1000 * 1000))
        {
          //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//          YYSYS_LOG(WARN, "can not serve read req, server_consistency=%s,"
//                    " req_consistency=%s, obi_role=%s, role=%s, state=%s",
//                    config_.consistency_type.str(),
//                    STR_BOOL(is_consistency_read), obi_role_.get_role_str(),
//                    role_mgr_.get_role_str(), role_mgr_.get_state_str());
          YYSYS_LOG(WARN, "can not serve read req, server_consistency=%s,"
                    " req_consistency=%s, role=%s, state=%s",
                    config_.consistency_type.str(),
                    STR_BOOL(is_consistency_read),
                    role_mgr_.get_role_str(),
                    role_mgr_.get_state_str());
          //mod 20150701:e
        }
      }
      return is_provide_service;
    }

    //add :rs ups hb
    int ObUpdateServer::ups_rs_lease(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int err = OB_SUCCESS;
      UNUSED(channel_id);
      UNUSED(req);

      ObMsgUpsHeartbeat hb;
      if (hb.MY_VERSION != version)
      {
        YYSYS_LOG(WARN, "version not equal. version=%d, MY_VERSION=%d", version, hb.MY_VERSION);
        err = OB_ERROR_FUNC_VERSION;
      }
//      if (OB_SUCCESS == err)
//      {
//        err = hb.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
//        if (OB_SUCCESS != err)
//        {
//          YYSYS_LOG(WARN, "failed to deserialize hb_info, err=%d", err);
//        }
//      }
      if(4 == version)
      {
          hb.minor_freeze_flag_ = false;
          err = hb.deserialize_v4(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
      }
      else if(5 == version)
      {
          err = hb.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
      }
      if (OB_SUCCESS != err)
      {
          YYSYS_LOG(WARN, "failed to deserialize hb_info, err=%d", err);
      }

      //�����������ݣ����д���
      if (OB_SUCCESS == err)
      {
        err = ups_update_lease(hb);
        if (OB_SUCCESS != err)
        {
          YYSYS_LOG(WARN, "fail to update lease. err=%d", err);
        }
      }

      if (OB_ERROR_FUNC_VERSION != err)
      {
        ObMsgUpsHeartbeatResp hb_res;
        set_heartbeat_res(hb_res);
        //err = hb_res.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        if(4 == version)
        {
            err = hb_res.serialize_v2(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        }
        else if(5 == version)
        {
            err = hb_res.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
        }

        if (OB_SUCCESS != err)
        {
          YYSYS_LOG(WARN, "fail to serialize hb_res");
        }
        if (OB_SUCCESS == err)
        {
            if(4 == version)
            {
                int OLD_VERSION = 2;
                err = client_manager_.post_request(root_server_, OB_RS_UPS_HEARTBEAT_RESPONSE, OLD_VERSION, out_buff);
            }
            else if (5 == version)
            {
                err = client_manager_.post_request(root_server_, OB_RS_UPS_HEARTBEAT_RESPONSE, hb_res.MY_VERSION, out_buff);
            }

          if (OB_SUCCESS != err)
          {
            YYSYS_LOG(WARN, "fail to send response. err = %d", err);
          }
        }
      }
      easy_request_wakeup(req);
      return err;
    }

    /*  int ObUpdateServer::ups_change_vip(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id)
        {
        int ret = OB_SUCCESS;

        if (version != MY_VERSION)
        {
        ret = OB_ERROR_FUNC_VERSION;
        }

        int32_t new_vip = 0;
        if (OB_SUCCESS == ret)
        {
        ret = serialization::decode_i32(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &new_vip);
        if (OB_SUCCESS != ret)
        {
        YYSYS_LOG(WARN, "failed to decode vip, ret=%d", ret);
        }
        }

        if (OB_SUCCESS == ret)
        {
        check_thread_.reset_vip(new_vip);
        slave_mgr_.reset_vip(new_vip);
        }

        ret = response_result_(ret, OB_UPS_CHANGE_VIP_RESPONSE, MY_VERSION, req, channel_id);

        return ret;
        }
     */
    int ObUpdateServer::ups_dump_text_memtable(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      ObString dump_dir;
      if (OB_SUCCESS == ret)
      {
        ret = dump_dir.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (OB_SUCCESS != ret)
        {
          YYSYS_LOG(WARN, "deserialize dump dir error, ret=%d", ret);
        }
        else
        {
          YYSYS_LOG(INFO, "dumping memtables to dir=[%.*s]", dump_dir.length(), dump_dir.ptr());
        }
      }
      response_result_(ret, OB_UPS_DUMP_TEXT_MEMTABLE_RESPONSE, MY_VERSION, req, channel_id);
      if (OB_SUCCESS == ret)
      {
        table_mgr_.dump_memtable(dump_dir);
      }
      return ret;
    }

    int ObUpdateServer::ups_dump_text_schemas(const int32_t version,
        easy_request_t* req, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      if (OB_SUCCESS == ret)
      {
        table_mgr_.dump_schemas();
      }

      if (OB_SUCCESS == ret)
      {
        ob_print_mod_memory_usage();
      }

      ret = response_result_(ret, OB_UPS_DUMP_TEXT_SCHEMAS_RESPONSE, MY_VERSION, req, channel_id);
      return ret;
    }

    int ObUpdateServer::ups_force_fetch_schema(const int32_t version,
        easy_request_t* req, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      if (OB_SUCCESS == ret)
      {
        //mod zhaoqiong [Schema Manager] 20150520:b
        //ret = do_async_update_whole_schema();
        ret = do_async_update_whole_schema(true);
        //mod:e
      }
      ret = response_result_(ret, OB_UPS_FORCE_FETCH_SCHEMA_RESPONSE, MY_VERSION, req, channel_id);
      return ret;
    }

    int ObUpdateServer::ups_memory_watch(const int32_t version,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      UpsMemoryInfo memory_info;
      if (OB_SUCCESS == ret)
      {
        memory_info.total_size = ob_get_memory_size_handled();
        memory_info.cur_limit_size = ob_get_memory_size_limit();
        table_mgr_.get_memtable_memory_info(memory_info.table_mem_info);
        table_mgr_.log_table_info();
        trans_executor_.log_trans_info();
        ob_print_mod_memory_usage();
      }
      ret = response_data_(ret, memory_info, OB_UPS_MEMORY_WATCH_RESPONSE, MY_VERSION,
          req, channel_id, out_buff);
      return ret;
    }

    int ObUpdateServer::ups_memory_limit_set(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      UpsMemoryInfo memory_info;
      if (OB_SUCCESS == ret)
      {
        ret = memory_info.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (OB_SUCCESS == ret)
        {
          ob_set_memory_size_limit(memory_info.cur_limit_size);
          MemTableAttr memtable_attr;
          if (OB_SUCCESS == table_mgr_.get_memtable_attr(memtable_attr))
          {
            memtable_attr.total_memlimit = memory_info.table_mem_info.memtable_limit;
            table_mgr_.set_memtable_attr(memtable_attr);
          }
        }
        memory_info.total_size = ob_get_memory_size_handled();
        memory_info.cur_limit_size = ob_get_memory_size_limit();
        table_mgr_.get_memtable_memory_info(memory_info.table_mem_info);
        table_mgr_.log_table_info();
      }
      ret = response_data_(ret, memory_info, OB_UPS_MEMORY_LIMIT_SET_RESPONSE, MY_VERSION,
          req, channel_id, out_buff);
      return ret;
    }

    int ObUpdateServer::ups_priv_queue_conf_set(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }

      UpsPrivQueueConf priv_queue_conf;
      if (OB_SUCCESS == ret)
      {
        ret = priv_queue_conf.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position());
        if (OB_SUCCESS == ret)
        {
          if (priv_queue_conf.low_priv_network_lower_limit != 0)
          {
            config_.low_priv_network_lower_limit = priv_queue_conf.low_priv_network_lower_limit;
          }

          if (priv_queue_conf.low_priv_network_upper_limit != 0)
          {
            config_.low_priv_network_upper_limit = priv_queue_conf.low_priv_network_upper_limit;
          }

          if (priv_queue_conf.low_priv_adjust_flag != 0)
          {
            config_.low_priv_adjust_flag = priv_queue_conf.low_priv_adjust_flag;
          }

          if (priv_queue_conf.low_priv_cur_percent != 0)
          {
            config_.low_priv_cur_percent = priv_queue_conf.low_priv_cur_percent;
          }

          if (static_cast<int64_t>(config_.low_priv_cur_percent) >= 0)
          {
            read_thread_queue_.set_low_priv_cur_percent(config_.low_priv_cur_percent);
          }
        }
        priv_queue_conf.low_priv_network_lower_limit = config_.low_priv_network_lower_limit;
        priv_queue_conf.low_priv_network_upper_limit = config_.low_priv_network_upper_limit;
        priv_queue_conf.low_priv_adjust_flag = config_.low_priv_adjust_flag;
        priv_queue_conf.low_priv_cur_percent = config_.low_priv_cur_percent;
      }

      ret = response_data_(ret, priv_queue_conf, OB_UPS_PRIV_QUEUE_CONF_SET_RESPONSE, MY_VERSION,
          req, channel_id, out_buff);
      return ret;
    }

    int ObUpdateServer::ups_clear_active_memtable(const int32_t version,
        easy_request_t* req, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//      if (!(ObiRole::MASTER == obi_role_.get_role()
//            && ObUpsRoleMgr::MASTER == role_mgr_.get_role()
//            && ObUpsRoleMgr::ACTIVE == role_mgr_.get_state()
//            && is_lease_valid()))
      if (!(ObUpsRoleMgr::MASTER == role_mgr_.get_role()
            && ObUpsRoleMgr::ACTIVE == role_mgr_.get_state()
            && is_lease_valid()))
      //mod 20150701:e
      {
        ret = OB_NOT_MASTER;
        YYSYS_LOG(WARN, "not master:is_lease_valid=%s", STR_BOOL(is_lease_valid()));
      }
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      if (OB_SUCCESS == ret)
      {
        ret = table_mgr_.clear_active_memtable();
      }
      ret = response_result_(ret, OB_UPS_CLEAR_ACTIVE_MEMTABLE_RESPONSE, MY_VERSION, req, channel_id);
      return ret;
    }

    int ObUpdateServer::ups_switch_commit_log(const int32_t version,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//      if (!(ObiRole::MASTER == obi_role_.get_role()
//            && ObUpsRoleMgr::MASTER == role_mgr_.get_role()
//            && ObUpsRoleMgr::ACTIVE == role_mgr_.get_state()
//            && is_lease_valid()))
      if (!(ObUpsRoleMgr::MASTER == role_mgr_.get_role()
            && ObUpsRoleMgr::ACTIVE == role_mgr_.get_state()
            && is_lease_valid()))
      //mod 20150701:e
      {
        ret = OB_NOT_MASTER;
        YYSYS_LOG(WARN, "not master:is_lease_valid=%s", STR_BOOL(is_lease_valid()));
      }
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      uint64_t new_log_file_id = 0;
      int proc_ret = OB_SUCCESS;
      if (OB_SUCCESS == ret)
      {
        proc_ret = log_mgr_.switch_log_file(new_log_file_id);
        YYSYS_LOG(INFO, "switch log file id ret=%d new_log_file_id=%lu", ret, new_log_file_id);
      }
      ret = response_data_(proc_ret, new_log_file_id, OB_UPS_SWITCH_COMMIT_LOG_RESPONSE, MY_VERSION,
          req, channel_id, out_buff);
      return ret;
    }
    int ObUpdateServer::ups_get_slave_info(const int32_t version,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      common::ObResultCode result;
      result.result_code_ = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        result.result_code_ = OB_ERROR_FUNC_VERSION;
      }

      if (OB_SUCCESS == ret)
      {
        ret = result.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
      }
      if (OB_SUCCESS == ret && OB_SUCCESS == result.result_code_)
      {
        slave_mgr_.print(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position());
      }
      if (OB_SUCCESS == ret)
      {
        ret = send_response(OB_UPS_GET_SLAVE_INFO_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      }
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "get ups slave info fail .err=%d", ret);
      }
      return ret;
    }
    //*/

    int ObUpdateServer::ups_get_last_frozen_version(const int32_t version,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      uint64_t last_frozen_memtable_version = 0;
      if (OB_SUCCESS == ret)
      {
        ret = table_mgr_.get_last_frozen_memtable_version(last_frozen_memtable_version);
        YYSYS_LOG(INFO, "ups get last frozen version[%ld] ret=%d", last_frozen_memtable_version, ret);
      }
      ret = response_data_(ret, last_frozen_memtable_version, OB_UPS_GET_LAST_FROZEN_VERSION_RESPONSE, MY_VERSION,
          req, channel_id, out_buff);
      YYSYS_LOG(INFO, "rs get last frozeon version, version=%lu ret=%d",
                last_frozen_memtable_version, ret);
      return ret;
    }

    int ObUpdateServer::ups_get_table_time_stamp(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      int proc_ret = OB_SUCCESS;
      uint64_t major_version = 0;
      int64_t time_stamp = 0;
      if (OB_SUCCESS != (proc_ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), (int64_t*)&major_version)))
      {
        YYSYS_LOG(WARN, "decode major_version fail ret=%d", ret);
      }
      else
      {
        proc_ret = table_mgr_.get_table_time_stamp(major_version, time_stamp);
      }
      YYSYS_LOG(INFO, "get_table_time_stamp ret=%d major_version=%lu time_stamp=%ld src=%s",
                proc_ret, major_version, time_stamp, NULL == req ? NULL :
                get_peer_ip(req));
      ret = response_data_(proc_ret, time_stamp, OB_UPS_GET_TABLE_TIME_STAMP_RESPONSE, MY_VERSION,
          req, channel_id, out_buff);
      return ret;
    }

    int ObUpdateServer::ups_enable_memtable_checksum(const int32_t version, easy_request_t* req, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      if (OB_SUCCESS == ret)
      {
        table_mgr_.set_replay_checksum_flag(true);
      }
      ret = response_result_(ret, OB_UPS_ENABLE_MEMTABLE_CHECKSUM_RESPONSE, MY_VERSION, req, channel_id);
      return ret;
    }

    int ObUpdateServer::ups_disable_memtable_checksum(const int32_t version, easy_request_t* req, const uint32_t channel_id)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      if (OB_SUCCESS == ret)
      {
        table_mgr_.set_replay_checksum_flag(false);
      }
      ret = response_result_(ret, OB_UPS_ENABLE_MEMTABLE_CHECKSUM_RESPONSE, MY_VERSION, req, channel_id);
      return ret;
    }

    int ObUpdateServer::ups_fetch_stat_info(const int32_t version,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      uint64_t last_frozen_version = 0;
      if (OB_SUCCESS == ret)
      {
        ret = table_mgr_.get_last_frozen_memtable_version(last_frozen_version);
      }
      if (OB_SUCCESS == ret)
      {
        OB_STAT_SET(UPDATESERVER, UPS_STAT_FROZEN_VERSION, last_frozen_version);
        OB_STAT_SET(UPDATESERVER, UPS_STAT_MEMORY_TOTAL, ob_get_memory_size_handled());
        OB_STAT_SET(UPDATESERVER, UPS_STAT_MEMORY_LIMIT, ob_get_memory_size_limit());
        table_mgr_.update_memtable_stat_info();
        OB_STAT_SET(UPDATESERVER, UPS_STAT_GET_COUNT, OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_LL_GET_COUNT)
                                                    + OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_NL_GET_COUNT)
                                                    + OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_HL_GET_COUNT));
        OB_STAT_SET(UPDATESERVER, UPS_STAT_SCAN_COUNT, OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_LL_SCAN_COUNT)
                                                     + OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_NL_SCAN_COUNT)
                                                     + OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_HL_SCAN_COUNT));
        OB_STAT_SET(UPDATESERVER, UPS_STAT_APPLY_COUNT, OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_LL_APPLY_COUNT)
                                                      + OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_NL_APPLY_COUNT)
                                                      + OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_HL_APPLY_COUNT));
        OB_STAT_SET(UPDATESERVER, UPS_STAT_GET_TIMEU, OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_LL_GET_TIMEU)
                                                    + OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_NL_GET_TIMEU)
                                                    + OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_HL_GET_TIMEU));
        OB_STAT_SET(UPDATESERVER, UPS_STAT_SCAN_TIMEU, OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_LL_SCAN_TIMEU)
                                                     + OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_NL_SCAN_TIMEU)
                                                     + OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_HL_SCAN_TIMEU));
        OB_STAT_SET(UPDATESERVER, UPS_STAT_APPLY_TIMEU, OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_LL_APPLY_TIMEU)
                                                      + OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_NL_APPLY_TIMEU)
                                                      + OB_STAT_GET_VALUE(UPDATESERVER, UPS_STAT_HL_APPLY_TIMEU));
      }
      ret = response_data_(ret, stat_mgr_, OB_FETCH_STATS_RESPONSE, MY_VERSION,
          req, channel_id, out_buff);
      return ret;
    }

    //add wenghaixing [secondary index.cluster]20150630
    int ObUpdateServer::ups_get_init_index(const int32_t version, ObDataBuffer &in_buff, easy_request_t *req,
                                           const uint32_t channel_id, ObDataBuffer &out_buff)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      int proc_ret = OB_SUCCESS;
      int64_t major_version = 0;
      CommonSchemaManagerWrapper sm;
      if (OB_SUCCESS != (proc_ret = serialization::decode_i64(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), &major_version)))
      {
        YYSYS_LOG(WARN, "decode major_version fail ret=%d", proc_ret);
      }
      else
      {
        proc_ret = table_mgr_.get_schema(major_version, sm);
      }
      YYSYS_LOG(INFO, "get_schema ret=%d major_version=%lu src=%s",
                proc_ret, major_version, NULL == req ? NULL :
                get_peer_ip(req));
      if(NULL == sm.schema_mgr_impl_)
      {
        YYSYS_LOG(ERROR, "should not be here, null pointer");
        ret = OB_INVALID_ARGUMENT;
      }
      if(OB_SUCCESS == ret)
      {
        ObArray<uint64_t> list;
        if(OB_SUCCESS != (ret = sm.schema_mgr_impl_->get_all_init_index_tid(list)))
        {
          YYSYS_LOG(WARN, "failed to get init index tid");
        }
        common::ObResultCode result_msg;
        result_msg.result_code_ = ret;
        if(OB_SUCCESS != (ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position())))
        {
          YYSYS_LOG(WARN, "failed to encode result buffer, ret[%d]", ret);
        }
        else if(OB_SUCCESS != (ret = serialization::encode_i64(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position(), list.count())))
        {
          YYSYS_LOG(WARN, "failed to encode list count,ret[%d]", ret);
        }
        else
        {
          uint64_t index_id = OB_INVALID_ID;
          //YYSYS_LOG(ERROR, "test::whx init index count[%ld], version[%ld]", list.count(),major_version);
          for(int64_t i = 0; i < list.count(); i++)
          {
            list.at(i,index_id);
            if(OB_SUCCESS != (ret = serialization::encode_i64(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position(), (int64_t)index_id)))
            {
              YYSYS_LOG(WARN, "failed to encode index id ,i =[%ld], index = [%ld]", i, index_id);
              break;
            }
          }
        }
      }
      ret = send_response(OB_FETCH_INIT_INDEX_RESPONSE, MY_VERSION, out_buff, req, channel_id);
      //YYSYS_LOG(ERROR, "test::whx response:ret[%d]", ret);
      return ret;
    }
    //add e
    int ObUpdateServer::ups_get_schema(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      int proc_ret = OB_SUCCESS;
      uint64_t major_version = 0;
      CommonSchemaManagerWrapper sm;
      if (OB_SUCCESS != (proc_ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), (int64_t*)&major_version)))
      {
        YYSYS_LOG(WARN, "decode major_version fail ret=%d", proc_ret);
      }
      else
      {
        proc_ret = table_mgr_.get_schema(major_version, sm);
      }
      YYSYS_LOG(INFO, "get_schema ret=%d major_version=%lu src=%s",
                proc_ret, major_version, NULL == req ? NULL :
                get_peer_ip(req));
      ret = response_data_(proc_ret, sm, OB_FETCH_SCHEMA_RESPONSE, MY_VERSION,
          req, channel_id, out_buff);
      return ret;
    }

    //add zhaoqiong [Truncate Table]:20160318:b
    int ObUpdateServer::ups_check_incremental_data_range(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      int proc_ret = OB_SUCCESS;
      int64_t table_id = 0;
      ObVersionRange range;
      ObVersionRange new_range;

      if (OB_SUCCESS != (proc_ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), (int64_t*)&table_id)))
      {
        YYSYS_LOG(WARN, "decode table_id fail ret=%d", proc_ret);
      }
      else if (OB_SUCCESS != (proc_ret = range.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position())))
      {
        YYSYS_LOG(WARN, "decode range fail ret=%d", proc_ret);
      }
      else
      {
        proc_ret = table_mgr_.get_table_mgr()->check_table_range(range,new_range,table_id);
      }
      YYSYS_LOG(DEBUG, "check tablet range ret=%d table_id=%ld src=%s",
                proc_ret, table_id, NULL == req ? NULL :
                get_peer_ip(req));
      ret = response_data_(proc_ret, new_range, OB_CHECK_INCREMENTAL_RANGE_RESPONSE, MY_VERSION,
          req, channel_id, out_buff);
      return ret;
    }
    //add:e
    //add zhaoqiong [Schema Manager] 20150327:b
    int ObUpdateServer::do_async_update_schema()
    {
      int err = OB_SUCCESS;
      bool need_refresh_schema = false;

      {
        ObSchemaMutator schema_mutator;
        OnceGuard once_guard(schema_lock_);
        if (!once_guard.try_lock())
        {}
        else
        {
          YYSYS_LOG(INFO, "fetch_schema_mutator(%ld->%ld)", table_mgr_.get_schema_mgr().get_version(), schema_version_);

          err = schema_service_->fetch_schema_mutator(table_mgr_.get_schema_mgr().get_version(),schema_version_,schema_mutator);

          if (OB_SUCCESS != err)
          {
            YYSYS_LOG(WARN, "Error occurs when fetch_schema_mutator, err=%d", err);
          }
          else if (!schema_mutator.get_refresh_schema())
          {
            if (OB_SUCCESS != (err = submit_switch_schema_mutator(schema_mutator)))
            {
              YYSYS_LOG(ERROR, "submit_switch_schema mutator(version=%ld)=>%d", schema_version_, err);
            }
            else
            {
              YYSYS_LOG(INFO, "submit swtich_schema mutator(version=%ld)", schema_version_);
            }
          }
          else
          {
            need_refresh_schema = true;
            YYSYS_LOG(INFO, "mutator contain refresh schema, need update whole schema");
          }
        }
      }

      if (OB_SUCCESS == err && need_refresh_schema)
      {
        err = do_async_update_whole_schema();
      }

      return err;
    }

    int ObUpdateServer::submit_switch_schema_mutator(ObSchemaMutator& schema_mutator)
    {
      int err = OB_SUCCESS;
      ThreadSpecificBuffer::Buffer *my_buffer = my_thread_buffer_.get_buffer();
      ObDataBuffer in_buf;
      if (NULL == my_buffer)
      {
        err = OB_ALLOCATE_MEMORY_FAILED;
        YYSYS_LOG(ERROR, "async_task_serialize_buffer == NULL");
      }
      else
      {
        in_buf.set_data(my_buffer->current(), my_buffer->remain());
      }
      if (OB_SUCCESS != err)
      {}
      else if (OB_SUCCESS != (err = schema_mutator.serialize(in_buf.get_data(), in_buf.get_capacity(), in_buf.get_position())))
      {
        YYSYS_LOG(ERROR, "schema_mutator.serialize(buf=%p[%ld-%ld])=>%d",
                  in_buf.get_data(), in_buf.get_position(), in_buf.get_capacity(), err);
      }
      else if (OB_SUCCESS != (err = submit_async_task_(OB_SWITCH_SCHEMA_MUTATOR, write_thread_queue_, write_task_queue_size_, &in_buf)))
      {
        YYSYS_LOG(WARN, "submit_async_task(SWITCH_SCHEMA_MUTATOR)=>%d", err);
      }
      return err;
    }

    int ObUpdateServer::ups_switch_schema_mutator(const int32_t version, ObPacket *packet_orig, common::ObDataBuffer &in_buf)
    {
      int ret = OB_SUCCESS;

      ObSchemaMutator schema_mutator;

//      if (!(ObiRole::MASTER == obi_role_.get_role()
//            && ObUpsRoleMgr::MASTER == role_mgr_.get_role()
//            && ObUpsRoleMgr::ACTIVE == role_mgr_.get_state()
//            && is_lease_valid()))   //uncertainty  ups ��д
      if(!(ObUpsRoleMgr::MASTER == role_mgr_.get_role()
                      && ObUpsRoleMgr::ACTIVE == role_mgr_.get_state()
                      && is_lease_valid()))
      {
        ret = OB_NOT_MASTER;
        YYSYS_LOG(WARN, "not master:is_lease_valid=%s", STR_BOOL(is_lease_valid()));
      }

      if (version != MY_VERSION)
      {
        YYSYS_LOG(ERROR, "Version do not match, MY_VERSION=%d version= %d",
            MY_VERSION, version);
        ret = OB_ERROR_FUNC_VERSION;
      }

      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = schema_mutator.deserialize(in_buf.get_data(), in_buf.get_capacity(), in_buf.get_position())))
        {
          YYSYS_LOG(ERROR, "deserialize schema from packet error, ret=%d buf=%p pos=%ld cap=%ld",
                    ret, in_buf.get_data(), in_buf.get_position(), in_buf.get_capacity());
        }
        else if (OB_SUCCESS != (ret = table_mgr_.switch_schema_mutator(schema_mutator)))
        {
          YYSYS_LOG(ERROR, "switch schema err, ret=%d schema_version=%ld", ret, schema_mutator.get_end_version());
        }
        else
        {
           YYSYS_LOG(INFO, "switch schema mutator succ");
        }
      }

      if (NULL != packet_orig->get_request())
      {
        ret = response_result_(ret, OB_SWITCH_SCHEMA_MUTATOR_RESPONSE, MY_VERSION,
            packet_orig->get_request(), packet_orig->get_channel_id());
      }
      return ret;
    }
	
    int ObUpdateServer::ups_get_schema_next(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      uint64_t major_version = 0;
      int64_t start_table_pos = -1;
      int64_t start_column_pos = -1;

      common::ObResultCode result_msg;
      CommonSchemaManagerWrapper sm;

      if (OB_SUCCESS != (ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position(), (int64_t*)&major_version)))
      {
        YYSYS_LOG(WARN, "decode major_version fail ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(),in_buff.get_position(), &start_table_pos)))
      {
        YYSYS_LOG(WARN, "failed to decode table id, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(),in_buff.get_position(), &start_column_pos)))
      {
        YYSYS_LOG(WARN, "failed to decode table id, err=%d", ret);
      }
      else
      {
        YYSYS_LOG(INFO, "next table start:%ld, column start:%ld", start_table_pos, start_column_pos);
        ret = table_mgr_.get_schema(major_version, sm);
      }

      YYSYS_LOG(INFO, "get_schema ret=%d major_version=%lu src=%s",
                ret, major_version, NULL == req ? NULL :
                get_peer_ip(req));

      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "serialize result msg error, ret=%d", ret);
      }
      else if (NULL == eio_)
      {
        ret = OB_NOT_INIT;
      }
      else
      {
        if (OB_SUCCESS != (ret=sm.get_impl()->determine_serialize_pos(start_table_pos,start_column_pos)))
        {
            result_msg.result_code_ = ret;
            YYSYS_LOG(ERROR, "failed to determine_serialize_pos, err=%d", ret);
            ret = OB_SUCCESS;
        }
        if (OB_SUCCESS != (ret = result_msg.serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position())))
        {
          YYSYS_LOG(ERROR, "result_msg.serialize error");
        }
        else if (OB_SUCCESS !=(ret = sm.get_impl()->serialize(out_buff.get_data(), out_buff.get_capacity(), out_buff.get_position())))
        {
          YYSYS_LOG(ERROR, "schema.serialize error");
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = send_response(OB_FETCH_SCHEMA_NEXT_RESPONSE, MY_VERSION, out_buff, req, channel_id);
        if (OB_SUCCESS != ret)
        {
          YYSYS_LOG(WARN, "failed to send scan response, ret=%d", ret);
          ret = OB_ERROR;
        }
      }
      return ret;
    }
    int ObUpdateServer::ups_switch_tmp_schema()
    {
      int ret = OB_SUCCESS;
//      if (!(ObiRole::MASTER == obi_role_.get_role()
//            && ObUpsRoleMgr::MASTER == role_mgr_.get_role()
//            && ObUpsRoleMgr::ACTIVE == role_mgr_.get_state()
//            && is_lease_valid()))                  // uncertianty ups��ȥ��obi
      if(!(ObUpsRoleMgr::MASTER == role_mgr_.get_role()
                      && ObUpsRoleMgr::ACTIVE == role_mgr_.get_state()
                      && is_lease_valid()))
      {
        ret = OB_NOT_MASTER;
        YYSYS_LOG(WARN, "not master:is_lease_valid=%s", STR_BOOL(is_lease_valid()));
        if (!table_mgr_.unlock_tmp_schema())
        {
          YYSYS_LOG(ERROR, "unexpected error");
        }
      }
      else if (OB_SUCCESS != (ret = table_mgr_.switch_tmp_schemas()))
      {
        YYSYS_LOG(ERROR, "switch_tmp_schemas failed, ret=%d", ret);
      }
      return ret;
    }

    //add:e
    int ObUpdateServer::ups_get_sstable_range_list(const int32_t version, common::ObDataBuffer& in_buff,
        easy_request_t* req, const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      int ret = OB_SUCCESS;
      if (version != MY_VERSION)
      {
        ret = OB_ERROR_FUNC_VERSION;
      }
      int proc_ret = OB_SUCCESS;
      uint64_t major_version = 0;
      uint64_t table_id = 0;
      TabletInfoList ti_list;
      if (OB_SUCCESS != (proc_ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(),
                        in_buff.get_position(), (int64_t*)&major_version)))
      {
        YYSYS_LOG(WARN, "decode major_version fail ret=%d", ret);
      }
      else if (OB_SUCCESS != (proc_ret = serialization::decode_vi64(in_buff.get_data(), in_buff.get_capacity(),
                              in_buff.get_position(), (int64_t*)&table_id)))
      {
        YYSYS_LOG(WARN, "decode major_version fail ret=%d", ret);
      }
      else
      {
        proc_ret = table_mgr_.get_sstable_range_list(major_version, table_id, ti_list);
      }
      YYSYS_LOG(INFO, "get_sstable_range_list ret=%d major_version=%lu table_id=%lu src=%s",
                proc_ret, major_version, table_id, NULL == req ? NULL :
                get_peer_ip(req));
      ret = response_data_(proc_ret, ti_list.inst, OB_RS_FETCH_SPLIT_RANGE_RESPONSE, MY_VERSION,
          req, channel_id, out_buff);
      return ret;
    }

    int ObUpdateServer::ups_set_config(const int32_t version, common::ObDataBuffer& in_buff,
                                       easy_request_t* req, const uint32_t channel_id)
    {
      UNUSED(version);
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;
      common::ObString config_str;
      if (OB_SUCCESS != (ret = config_str.deserialize(in_buff.get_data(), in_buff.get_capacity(), in_buff.get_position())))
      {
        YYSYS_LOG(ERROR, "Deserialize config string failed! ret: [%d]", ret);
      }
      else if (OB_SUCCESS != (ret = config_.add_extra_config(config_str.ptr(), true)))
      {
        YYSYS_LOG(ERROR, "Set config failed! ret: [%d]", ret);
      }
      else if (OB_SUCCESS != (ret = config_mgr_.reload_config()))
      {
        YYSYS_LOG(ERROR, "Reload config failed! ret: [%d]", ret);
      }
      else
      {
        YYSYS_LOG(INFO, "Set config successfully! str: [%s]", config_str.ptr());
        config_.print();
      }

      if (OB_SUCCESS != (ret = response_result_(ret, OB_SET_CONFIG_RESPONSE, MY_VERSION, req, channel_id)))
      {
        YYSYS_LOG(ERROR, "send response fail, ret: [%d]", ret);
      }

      return ret;
    }

    int ObUpdateServer::ups_get_config(const int32_t version, easy_request_t* req,
                                       const uint32_t channel_id, common::ObDataBuffer& out_buff)
    {
      UNUSED(version);
      int ret = OB_SUCCESS;
      static const int MY_VERSION = 1;

      if (OB_SUCCESS != (ret = response_data_(ret, config_, OB_GET_CONFIG_RESPONSE, MY_VERSION, req, channel_id, out_buff)))
      {
        YYSYS_LOG(ERROR, "send response fail, ret: [%d]", ret);
      }

      return ret;
    }

    int ObUpdateServer::low_priv_speed_control_(const int64_t scanner_size)
    {
      int ret = OB_SUCCESS;
      static volatile int64_t s_stat_times = 0;
      static volatile int64_t s_stat_size = 0;
      static volatile int64_t s_last_stat_time_ms = yysys::CTimeUtil::getTime() / 1000L;
      static volatile int32_t flag = 0;

      if (scanner_size < 0)
      {
        YYSYS_LOG(WARN, "invalid param, scanner_size=%ld", scanner_size);
        ret = OB_ERROR;
      }
      else
      {
        atomic_inc((volatile uint64_t*) &s_stat_times);
        atomic_add((volatile uint64_t*) &s_stat_size, scanner_size);

        if (s_stat_times >= SEG_STAT_TIMES || s_stat_size >= SEG_STAT_SIZE * 1024L * 1024L)
        {
          if (atomic_compare_exchange((volatile uint32_t*) &flag, 1, 0) == 0)
          {
            // only one thread is allowed to adjust network limit
            int64_t cur_time_ms = yysys::CTimeUtil::getTime() / 1000L;

            YYSYS_LOG(DEBUG, "stat_size=%ld cur_time_ms=%ld last_stat_time_ms=%ld", s_stat_size,
                cur_time_ms, s_last_stat_time_ms);

            int64_t adjust_flag = config_.low_priv_adjust_flag;

            if (1 == adjust_flag) // auto adjust low priv percent
            {
              int64_t lower_limit = config_.low_priv_network_lower_limit;
              int64_t upper_limit = config_.low_priv_network_upper_limit;

              int64_t low_priv_percent = read_thread_queue_.get_low_priv_cur_percent();

              if (s_stat_size * 1000L < lower_limit * (cur_time_ms - s_last_stat_time_ms))
              {
                if (low_priv_percent < PriorityPacketQueueThread::LOW_PRIV_MAX_PERCENT)
                {
                  ++low_priv_percent;
                  read_thread_queue_.set_low_priv_cur_percent(low_priv_percent);

                  YYSYS_LOG(INFO, "network lower limit, lower_limit=%ld, low_priv_percent=%ld",
                      lower_limit, low_priv_percent);
                }
              }
              else if (s_stat_size * 1000L > upper_limit * (cur_time_ms - s_last_stat_time_ms))
              {
                if (low_priv_percent > PriorityPacketQueueThread::LOW_PRIV_MIN_PERCENT)
                {
                  --low_priv_percent;
                  read_thread_queue_.set_low_priv_cur_percent(low_priv_percent);

                  YYSYS_LOG(INFO, "network upper limit, upper_limit=%ld, low_priv_percent=%ld",
                      upper_limit, low_priv_percent);
                }
              }
            }

            // reset stat_times, stat_size and last_stat_time
            s_stat_times = 0;
            s_stat_size = 0;
            s_last_stat_time_ms = cur_time_ms;

            flag = 0;
          }
        }
      }
      return ret;
    }

    //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//    const ObServer& ObUpdateServer::get_ups_log_master()
//    {
//      return ObUpsRoleMgr::MASTER == role_mgr_.get_role()? ups_inst_master_: ups_master_;
//    }
    const ObServer& ObUpdateServer::get_ups_log_master()
    {
      return ups_master_;
    }
    //mod 20150701:e
    ////////////////////////////////////////////////////////////////////////////////////////////////////

    UpsWarmUpDuty::UpsWarmUpDuty() : duty_start_time_(0),
                                     cur_warm_up_percent_(0),
                                     duty_waiting_(0)
    {
    }

    UpsWarmUpDuty::~UpsWarmUpDuty()
    {
    }

    bool UpsWarmUpDuty::drop_start()
    {
      bool bret = false;
      if (0 == atomic_compare_exchange(&duty_waiting_, 1, 0))
      {
        bret = true;
      }
      else
      {
        if (0 != duty_start_time_
            && yysys::CTimeUtil::getTime() > (MAX_DUTY_IDLE_TIME + duty_start_time_))
        {
          YYSYS_LOG(WARN, "duty has run too long and will be rescheduled, duty_start_time=%ld",
                    duty_start_time_);
          bret = true;
        }
      }
      if (bret)
      {
        duty_start_time_ = yysys::CTimeUtil::getTime();
        cur_warm_up_percent_ = 0;
        YYSYS_LOG(INFO, "warm up start duty_start_time=%ld", duty_start_time_);
      }
      return bret;
    }

    void UpsWarmUpDuty::drop_end()
    {
      atomic_exchange(&duty_waiting_, 0);
      duty_start_time_ = 0;
      cur_warm_up_percent_ = 0;
    }

    void UpsWarmUpDuty::finish_immediately()
    {
      duty_start_time_ = 0;
    }

    void UpsWarmUpDuty::runTimerTask()
    {
      int64_t warm_up_time = get_warm_up_time();
      if (yysys::CTimeUtil::getTime() > (duty_start_time_ + warm_up_time)
          || CacheWarmUpConf::STOP_PERCENT <= cur_warm_up_percent_)
      {
        submit_force_drop();
        YYSYS_LOG(INFO, "warm up finished, will drop memtable, cur_warm_up_percent=%ld cur_time=%ld duty_start_time=%ld warm_time=%ld",
                  cur_warm_up_percent_, yysys::CTimeUtil::getTime(), duty_start_time_, warm_up_time);
      }
      else
      {
        if (CacheWarmUpConf::STOP_PERCENT > cur_warm_up_percent_)
        {
          cur_warm_up_percent_++;
          YYSYS_LOG(INFO, "warm up percent update to %ld", cur_warm_up_percent_);
          set_warm_up_percent(cur_warm_up_percent_);
        }
        schedule_warm_up_duty();
      }
    }

    int64_t UpsWarmUpDuty::get_warm_up_time()
    {
      int64_t ret = get_conf_warm_up_time();
      int64_t active_mem_limit = get_active_mem_limit();
      int64_t oldest_memtable_size = get_oldest_memtable_size();
      if (0 != active_mem_limit
          && 0 != oldest_memtable_size)
      {
        double warm_up_time = ceil(static_cast<double>(ret) * static_cast<double>(oldest_memtable_size) / static_cast<double>(active_mem_limit));
        ret = std::min(ret, static_cast<int64_t>(warm_up_time));
      }
      return ret;
    }

    int64_t UpsWarmUpDuty::get_warm_up_step_interval()
    {
      double warm_up_step_interval = ceil(static_cast<double>(get_warm_up_time()) / (CacheWarmUpConf::STOP_PERCENT / CacheWarmUpConf::STEP_PERCENT));
      return static_cast<int64_t>(warm_up_step_interval);
    }

    int ObUpsLogServerGetter::init(ObUpdateServer* ups)
    {
      int err = OB_SUCCESS;
      if (NULL != ups_)
      {
        err = OB_INIT_TWICE;
      }
      else if (NULL == ups)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        ups_ = ups;
      }
      return err;
    }

    int64_t ObUpsLogServerGetter::get_type() const
    {
      int64_t server_type = ANY_SERVER;
      if (NULL == ups_)
      {}
      //mod peiouya [MultiUPS] [DELETE_OBI] 20150810
//      //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
////      else if (STANDALONE_SLAVE == ups_->get_obi_slave_stat()
////          && ObiRole::SLAVE == ups_->get_obi_role().get_role()
////          && ObUpsRoleMgr::MASTER == ups_->get_role_mgr().get_role())
////      {
////        server_type = LSYNC_SERVER;
////      }
////      else
////      {
////        server_type = FETCH_SERVER;
////      }
//      server_type = FETCH_SERVER;
//      //mod 20150701:e
       else
       {
         server_type = FETCH_SERVER;
       }
      //mod 20150810:e
      return server_type;
    }

    int ObUpsLogServerGetter::get_server(ObServer& server) const
    {
      int err = OB_SUCCESS;
      ObServer null_server;
      if (NULL == ups_)
      {
        err = OB_NOT_INIT;
      }
      //del peiouya [MultiUPS] [DELETE_OBI] 20150701:b
      //else if (LSYNC_SERVER == get_type())
      //{
      //  server = ups_->get_lsync_server();
      //}
      //del 20150701:e
      else if (FETCH_SERVER == get_type())
      {
        server = ups_->get_ups_log_master();
      }
      else
      {
        server = null_server;
        YYSYS_LOG(WARN, "get_server(): unknown slave type");
      }
      return err;
    }

    int ObPrefetchLogTaskSubmitter::init(int64_t prefetch_timeout, ObUpdateServer* ups)
    {
      int err = OB_SUCCESS;
      if (NULL != ups_)
      {
        err = OB_INIT_TWICE;
      }
      else if (NULL == ups)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        prefetch_timeout_ = prefetch_timeout;
        ups_ = ups;
      }
      return err;
    }

    int ObPrefetchLogTaskSubmitter::done(Task& task)
    {
      int err = OB_SUCCESS;
      int64_t now_us = yysys::CTimeUtil::getTime();
      if (task.launch_time_ + prefetch_timeout_ < now_us)
      {
        YYSYS_LOG(WARN, "task finished, but timeout: lauch_time[%ld] + timeout[%ld] <  now_us[%ld]",
                  task.launch_time_, prefetch_timeout_, now_us);
      }
      else
      {
        last_done_time_ = now_us;
      }
      __sync_add_and_fetch(&running_task_num_, -1);
      return err;
    }

    int ObPrefetchLogTaskSubmitter::submit_task(void* arg)
    {
      UNUSED(arg);
      int err = OB_SUCCESS;
      int64_t now_us = yysys::CTimeUtil::getTime();
      Task task;
      if (NULL == ups_)
      {
        err = OB_NOT_INIT;
      }
      else if (running_task_num_ > 0)
      {}
      else
      {
        task.launch_time_ = now_us;
        if (last_launch_time_ + prefetch_timeout_ <= now_us)
        {
          YYSYS_LOG(WARN, "last task timeout, last_launch_time_[%ld] timeout[%ld]", last_launch_time_, prefetch_timeout_);
        }
        if (OB_SUCCESS != (err = ups_->submit_prefetch_remote_log(task)))
        {
          YYSYS_LOG(ERROR, "submit_prefetch_remote_log()=>%d", err);
        }
        else
        {
          last_launch_time_ = now_us;
          __sync_add_and_fetch(&running_task_num_, +1);
        }
      }
      return err;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    int ObUpdateServer::ui_deserialize_mutator(ObDataBuffer& buffer, ObMutator &mutator)
    {
      int ret = OB_SUCCESS;
      UpsSchemaMgrGuard guard;
      const ObSchemaManagerV2* schema_mgr = table_mgr_.get_schema_mgr().get_schema_mgr(guard);
      if (NULL == schema_mgr)
      {
        YYSYS_LOG(WARN, "failed to get schema");
        ret = OB_SCHEMA_ERROR;
      }
      else
      {
        mutator.set_compatible_schema(schema_mgr);
        ret = mutator.deserialize(buffer.get_data(), buffer.get_capacity(), buffer.get_position());
      }
      return ret;
    }
    //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150709:b
    int ObUpdateServer::ui_set_mutator(ObMutator &mutator, const ObTransID &trans_id, const bool commit)
    {
      int ret = OB_SUCCESS;
      UpsSchemaMgrGuard guard;
      ObDataBuffer data_buff;
      ThreadSpecificBuffer::Buffer* rpc_buffer = NULL;
      const ObSchemaManagerV2* schema_mgr = table_mgr_.get_schema_mgr().get_schema_mgr(guard);
      if (NULL == schema_mgr)
      {
        YYSYS_LOG(WARN, "failed to get schema");
        ret = OB_SCHEMA_ERROR;
      }
      else if(OB_SUCCESS != (ret = mutator.reuse()))
      {
        YYSYS_LOG(WARN, "reuse mutator error:%d", ret);
      }
      else if (NULL == (rpc_buffer = get_rpc_buffer()))
      {
        YYSYS_LOG(ERROR, "get thread rpc buff failed:buffer[%p].", rpc_buffer);
        ret = OB_ERROR;
      }
      else
      {
        rpc_buffer->reset();
        data_buff.set_data(rpc_buffer->current(), rpc_buffer->remain());
        data_buff.get_position() = 0;
        //��mutator�е������� ���뱣֤�����˷����л�����������ȡis_row_changed��is_row_finished��� ����᷵��not support
        mutator.set_compatible_schema(schema_mgr);
        if(OB_SUCCESS != (ret =  construct_trans_record(trans_id,commit, &mutator)))
        {
          YYSYS_LOG(WARN, "construct trans record mutator fail:ret[%d]", ret);
        }
        else if (OB_SUCCESS != (ret = mutator.serialize(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position())))
        {
          YYSYS_LOG(WARN, "serialize mutator fail:ret[%d]", ret);
        }
        else
        {
          int64_t pos = 0;
          if(OB_SUCCESS != (ret = mutator.deserialize(data_buff.get_data(), data_buff.get_position(), pos)))
          {
            YYSYS_LOG(WARN, "deserialize mutator fail:ret[%d]", ret);
          }
        }
      }
      return ret;
    }

    int ObUpdateServer::construct_trans_record(const ObTransID& trans_id, bool commit, ObMutator* mutator)
    {
      int ret = OB_SUCCESS;
      static const char cname[] = "commit_stat\0";
      static const char tname[] = "__ts_session_info\0";
      //mod linhx 20211208:b
      //before debug: static const char tname[] = "__ups_session_info\0";
      //after debug: static const char tname[] = "__ts_session_info\0";
      //mod linhx 20211208:e
      
      static ObString column_name;
      static ObString table_name;
      column_name.assign_ptr(const_cast<char*>(cname),static_cast<int32_t>(strlen(cname)));
      table_name.assign_ptr(const_cast<char*>(tname),static_cast<int32_t>(strlen(tname)));

      ObString ups;
      char buf[OB_MAX_SERVER_ADDR_SIZE];
      memset(buf, 0 , sizeof(buf));
      if (trans_id.ups_.ip_to_string(buf, sizeof(buf)) != true)
      {
        ret = OB_CONVERT_ERROR;
        YYSYS_LOG(ERROR, "server ip is invalid, ret=%d", ret);
      }
      else
      {
        ups.assign_ptr(buf,static_cast<int32_t>(strlen(buf)));

        //mod peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20160304:b
        //ObObj trans_info[3];
        //trans_info[0].set_int(trans_id.start_time_us_);
        //trans_info[1].set_varchar(ups);
        //trans_info[2].set_int(trans_id.ups_.get_port());

        //ObRowkey rowkey;
        //rowkey.assign(trans_info, 4);
        ObObj trans_info[4];
        trans_info[0].set_int(trans_id.start_time_us_);
        trans_info[1].set_int (static_cast<int64_t>(trans_id.descriptor_));
        trans_info[2].set_varchar(ups);
        trans_info[3].set_int(trans_id.ups_.get_port());

        ObRowkey rowkey;
        rowkey.assign(trans_info, 4);
        //mod peiouya 20160304:e

        ObObj commit_stat;
        commit_stat.set_bool(commit);

        ret = mutator->insert(table_name, rowkey, column_name, commit_stat);
        YYSYS_LOG(DEBUG, "insert value to mutator:t_name[%s],column_name[%s], ret[%d]", to_cstring(table_name),to_cstring(column_name), ret);
        if(OB_SUCCESS != ret)
        {
          YYSYS_LOG(WARN, "insert value to mutator fail:t_name[%s],column_name[%s], ret[%d]", to_cstring(table_name),to_cstring(column_name), ret);
        }
      }
      return ret;
    }
    //add 20150709:e
    //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
    common::ObGeneralRpcStub & ObUpdateServer::get_rpc_stub()
    {
      return rpc_stub_;
    }

    //add 20150701:e
    //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160226:b
    bool ObUpdateServer::is_ups_usable(const uint64_t cur_version)
    {
      bool is_usable = false;
      if (OB_DEFAULT_OFFLINE_VERSION == offline_version_ || cur_version < offline_version_)
      {
        is_usable = true;
      }
      return is_usable;
    }
    //add 20160226:e
  }
}
