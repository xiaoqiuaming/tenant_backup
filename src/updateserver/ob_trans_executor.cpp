////===================================================================
 //
 // ob_trans_executor.cpp updateserver / Oceanbase
 //
 // Copyright (C) 2010, 2012, 2013 Taobao.com, Inc.
 //
 // Created on 2012-08-30 by Yubai (yubai.lk@taobao.com)
 //
 // -------------------------------------------------------------------
 //
 // Description
 //
 //
 // -------------------------------------------------------------------
 //
 // Change Log
 //
////====================================================================

#include "common/ob_common_param.h"
#include "common/ob_profile_log.h"
#include "common/ob_profile_type.h"
#include "common/ob_trace_id.h"
#include "common/base_main.h"
#include "sql/ob_lock_filter.h"
#include "sql/ob_inc_scan.h"
#include "sql/ob_ups_modify.h"
#include "ob_ups_lock_filter.h"
#include "ob_ups_inc_scan.h"
#include "ob_memtable_modify.h"
#include "ob_update_server_main.h"
#include "ob_trans_executor.h"
#include "ob_session_guard.h"
#include "stress.h"
#include "sql/ob_sql_result_set.h"  //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701
//add hongchen [LOCK_WAIT_TIMEOUT] 20161209:b
#include "yysys.h"
//add hongchen [LOCK_WAIT_TIMEOUT] 20161209:e

#define UPS ObUpdateServerMain::get_instance()->get_update_server()

namespace oceanbase
{
  namespace updateserver
  {
    static int64_t get_slow_query_time_limit(int priority)
    {
      return PriorityPacketQueueThread::LOW_PRIV == priority? (int64_t)UPS.get_param().low_prio_slow_query_time_limit: (int64_t)UPS.get_param().normal_prio_slow_query_time_limit;
    }
#define BEGIN_SAMPLE_SLOW_QUERY(session_ctx) if (REACH_TIME_INTERVAL(100 * 1000)) { session_ctx->get_tlog_buffer().force_log_ = true; }
#define END_SAMPLE_SLOW_QUERY(session_ctx, pkt) if (session_ctx->is_slow_query(get_slow_query_time_limit(pkt.get_packet_priority()))) { session_ctx->get_tlog_buffer().force_print_ = true; }
    class FakeWriteGuard
    {
      public:
        //mod peiouya [MultiUPS] [UPS_SYNC_Frozen] 20150811:b
        //FakeWriteGuard(SessionMgr& session_mgr):
        FakeWriteGuard(SessionMgr& session_mgr, common::PacketCode pcode = common::OB_PACKET_NUM):
          session_mgr_(session_mgr), session_descriptor_(INVALID_SESSION_DESCRIPTOR), fake_write_granted_(false)
        {
          int ret = OB_SUCCESS;
          RWSessionCtx *session_ctx = NULL;
          if (OB_SUCCESS != (ret = session_mgr.begin_session(ST_READ_WRITE, 0, -1, -1, session_descriptor_, pcode)))
          {
            YYSYS_LOG(WARN, "begin fake write session fail ret=%d", ret);
          }
          else if (NULL == (session_ctx = session_mgr.fetch_ctx<RWSessionCtx>(session_descriptor_)))
          {
            YYSYS_LOG(WARN, "fetch ctx fail session_descriptor=%u", session_descriptor_);
            ret = OB_TRANS_ROLLBACKED;
          }
          else
          {
            session_ctx->set_frozen();
            session_mgr.revert_ctx(session_descriptor_);
            fake_write_granted_ = true;
          }
        }
        ~FakeWriteGuard()
        {
          if (INVALID_SESSION_DESCRIPTOR == session_descriptor_)
          {} // do nothing
          else
          {
            session_mgr_.end_session(session_descriptor_, /*rollback*/ true);
          }
        }
        bool is_granted()
        { return fake_write_granted_; }
        //add chujiajia [Paxos ups_replication] 20160113:b
        void set_fake_write_granted(bool fake_write_granted)
        {
          fake_write_granted_ = fake_write_granted;
        }
        //add:e
      private:
        SessionMgr& session_mgr_;
        uint32_t session_descriptor_;
        bool fake_write_granted_;
    };
    TransExecutor::TransExecutor(ObUtilInterface &ui) : TransHandlePool(),
                                                        TransCommitThread(),
                                                        DistributedTransHandlePool(),//add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701
                                                        ui_(ui),
                                                        allocator_(),
                                                        session_ctx_factory_(),
                                                        session_mgr_(),
                                                        lock_mgr_(),
                                                        uncommited_session_list_(),
                                                        ups_result_buffer_(ups_result_memory_, OB_MAX_PACKET_LENGTH),
                                                        mutex_flag_for_unsettled_trans_(false) //[521]
    {
      allocator_.set_mod_id(ObModIds::OB_UPS_TRANS_EXECUTOR_TASK);
      memset(ups_result_memory_, 0, OB_MAX_PACKET_LENGTH);

      memset(packet_handler_, 0, sizeof(packet_handler_));
      memset(trans_handler_, 0, sizeof(trans_handler_));
      memset(commit_handler_, 0, sizeof(commit_handler_));
      for (int64_t i = 0; i < OB_PACKET_NUM; i++)
      {
        packet_handler_[i] = phandle_non_impl;
        trans_handler_[i] = thandle_non_impl;
        commit_handler_[i] = chandle_non_impl;
      }

      packet_handler_[OB_FREEZE_MEM_TABLE] = phandle_freeze_memtable;
      packet_handler_[OB_UPS_MINOR_FREEZE_MEMTABLE] = phandle_freeze_memtable;
      packet_handler_[OB_UPS_MINOR_LOAD_BYPASS] = phandle_freeze_memtable;
      packet_handler_[OB_UPS_MAJOR_LOAD_BYPASS] = phandle_freeze_memtable;
      packet_handler_[OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE] = phandle_freeze_memtable;
      //add peiouya [MultiUPS] [UPS_SYNC_Frozen] 20150521:b
      packet_handler_[OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE_ACCORD_HB] = phandle_freeze_memtable;
      //add 20150521:e
      packet_handler_[OB_UPS_ASYNC_AUTO_FREEZE_MEMTABLE] = phandle_freeze_memtable;
      packet_handler_[OB_UPS_CLEAR_ACTIVE_MEMTABLE] = phandle_clear_active_memtable;
      packet_handler_[OB_UPS_ASYNC_CHECK_CUR_VERSION] = phandle_check_cur_version;
      //packet_handler_[OB_UPS_ASYNC_CHECK_SSTABLE_CHECKSUM] = phandle_check_sstable_checksum;

      //add zhaoqiong [Truncate Table]:20160127:b
      trans_handler_[OB_TRUNCATE_TABLE] = thandle_write_trans;
      //add:e
      trans_handler_[OB_COMMIT_END] = thandle_commit_end;
      trans_handler_[OB_NEW_SCAN_REQUEST] = thandle_scan_trans;
      trans_handler_[OB_NEW_GET_REQUEST] = thandle_get_trans;
      trans_handler_[OB_SCAN_REQUEST] = thandle_scan_trans;
      trans_handler_[OB_GET_REQUEST] = thandle_get_trans;
      trans_handler_[OB_MS_MUTATE] = thandle_write_trans;
      trans_handler_[OB_WRITE] = thandle_write_trans;
      trans_handler_[OB_PHY_PLAN_EXECUTE] = thandle_write_trans;
      trans_handler_[OB_START_TRANSACTION] = thandle_start_session;
      trans_handler_[OB_UPS_ASYNC_KILL_ZOMBIE] = thandle_kill_zombie;
      trans_handler_[OB_UPS_SHOW_SESSIONS] = thandle_show_sessions;
      trans_handler_[OB_UPS_KILL_SESSION] = thandle_kill_session;
      trans_handler_[OB_END_TRANSACTION] = thandle_end_session;
      //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
      trans_handler_[OB_WRITE_TRANS_STAT] = thandle_write_trans;
      trans_handler_[OB_END_DISTRIBUTED_TRANS] = thandle_end_trans;
      trans_handler_[OB_TRANS_PREPARE_ACK] = thandle_prepare_ack;
      trans_handler_[OB_WRITE_TRANS_STAT_ACK] = thandle_write_stat_ack;
      //for 2PC
      trans_handler_[OB_ROLLBACK_TRANSACTION] = thandle_rollback_trans;
      trans_handler_[OB_PREPARE_TRANSACTION] = thandle_end_session;
      trans_handler_[OB_COMMIT_TRANSACTION] = thandle_commit_trans;
      trans_handler_[OB_CHECK_SESSION_EXIST] = thandle_check_session_exist;
      //add 20150701:e
      //add by maosy [MultiUps 1.0] [batch_iud_snapshot] 20170622 b:
      trans_handler_[OB_GET_PUBLICED_TRANCEID] = thandle_get_publised_transid;
      // add by maosy e

      trans_handler_[OB_SWITCH_PARTITION_STAT] = thandle_update_partition_stat; //[449]

      commit_handler_[OB_MS_MUTATE] = chandle_write_commit;
      commit_handler_[OB_WRITE] = chandle_write_commit;
      commit_handler_[OB_PHY_PLAN_EXECUTE] = chandle_write_commit;
      commit_handler_[OB_END_TRANSACTION] = chandle_write_commit;
      //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
      commit_handler_[OB_PREPARE_TRANSACTION] = chandle_write_prepare;
      commit_handler_[OB_ROLLBACK_TRANSACTION] = chandle_write_rollback_trans;
      commit_handler_[OB_COMMIT_TRANSACTION] = chandle_write_commit_trans;
      commit_handler_[OB_WRITE_TRANS_STAT] = chandle_write_commit;
      //add 20150701:e
      commit_handler_[OB_SEND_LOG] = chandle_send_log;
      commit_handler_[OB_FAKE_WRITE_FOR_KEEP_ALIVE] = chandle_fake_write_for_keep_alive;
      commit_handler_[OB_SLAVE_REG] = chandle_slave_reg;
      //add zhaoqiong [fixed for Backup]:20150811:b
      commit_handler_[OB_BACKUP_REG] = chandle_backup_reg;
      //add:e

      commit_handler_[OB_UPS_ASYNC_CHECK_SSTABLE_CHECKSUM] = chandle_check_sstable_checksum;
      //add zhaoqiong [Truncate Table]:20160127:b
      commit_handler_[OB_TRUNCATE_TABLE] = chandle_write_commit;
      //add:e
      commit_handler_[OB_SWITCH_SCHEMA] = chandle_switch_schema;
      //add zhaoqiong [Schema Manager] 20150327:b
      commit_handler_[OB_SWITCH_SCHEMA_MUTATOR] = chandle_switch_schema_mutator;
      commit_handler_[OB_SWITCH_TMP_SCHEMA] = chandle_switch_tmp_schema;
      //add:e
      commit_handler_[OB_UPS_FORCE_FETCH_SCHEMA] = chandle_force_fetch_schema;
      commit_handler_[OB_UPS_SWITCH_COMMIT_LOG] = chandle_switch_commit_log;
      commit_handler_[OB_NOP_PKT] = chandle_nop;
    }

    TransExecutor::~TransExecutor()
    {
      destroy();
      session_mgr_.destroy();
      allocator_.destroy();
    }

    int TransExecutor::init(const int64_t trans_thread_num,
                            const int64_t trans_thread_start_cpu,
                            const int64_t trans_thread_end_cpu,
                            const int64_t commit_thread_cpu,
                            const int64_t commit_end_thread_num,
                            const int64_t distributed_trans_thread_num
                            )
    {
      int ret = OB_SUCCESS;
      bool queue_rebalance = true;
      bool dynamic_rebalance = true;
      
      //add wangdonghui [ups_replication] 20170323 :b
      message_residence_time_us_ = 2000;
      last_commit_log_time_us_ = 0;
      message_residence_max_us_ =6000;
      /*send_interval_us_ = 0;
      count_ = 0; average_batch_size_[0] = 0;
      best_batch_size_ = 0;
      last_gather_time_ = 0;
      last_refresh_time_ = 0;
      best_send_interval_us_ = 0;
      need_refresh_ = true;
      best_message_residence_time_ = 0;*/
      //add :e
      TransHandlePool::set_cpu_affinity(trans_thread_start_cpu, trans_thread_end_cpu);
      //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
      DistributedTransHandlePool::set_cpu_affinity(trans_thread_start_cpu, trans_thread_end_cpu);
      //add 20150701:e
      TransCommitThread::set_cpu_affinity(commit_thread_cpu);
      if (OB_SUCCESS != (ret = allocator_.init(ALLOCATOR_TOTAL_LIMIT, ALLOCATOR_HOLD_LIMIT, ALLOCATOR_PAGE_SIZE)))
      {
        YYSYS_LOG(WARN, "init allocator fail ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = session_mgr_.init(MAX_RO_NUM, MAX_RP_NUM, MAX_RW_NUM, &session_ctx_factory_)))
      {
        YYSYS_LOG(WARN, "init session mgr fail ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = flush_queue_.init(FLUSH_QUEUE_SIZE)))
      {
        YYSYS_LOG(ERROR, "flush_queue.init(%ld)=>%d", FLUSH_QUEUE_SIZE, ret);
      }
      else if (OB_SUCCESS != (ret = TransCommitThread::init(TASK_QUEUE_LIMIT, FINISH_THREAD_IDLE)))
      {
        YYSYS_LOG(WARN, "init TransCommitThread fail ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = TransHandlePool::init(trans_thread_num, TASK_QUEUE_LIMIT, queue_rebalance, dynamic_rebalance)))
      {
        YYSYS_LOG(WARN, "init TransHandlePool fail ret=%d", ret);
      }
      //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
      else if (OB_SUCCESS != (ret = DistributedTransHandlePool::init(distributed_trans_thread_num, TASK_QUEUE_LIMIT, queue_rebalance, dynamic_rebalance)))
      {
        YYSYS_LOG(WARN, "init DistributedTransHandlePool fail ret=%d", ret);
      }
      //add 20150701:e
      else if (OB_SUCCESS != (ret = CommitEndHandlePool::init(commit_end_thread_num, TASK_QUEUE_LIMIT, queue_rebalance, false)))
      {
        YYSYS_LOG(WARN, "init CommitEndHandlePool fail ret=%d", ret);
      }
      else
      {
        fifo_stream_.init("run/updateserver.fifo", 100L * 1024L * 1024L);
        nop_task_.pkt.set_packet_code(OB_NOP_PKT);
        partition_unchanged_version_.create(512L);//[449]
        YYSYS_LOG(INFO, "TransExecutor init succ");
      }
      return ret;
    }

    void TransExecutor::destroy()
    {
      StressRunnable::stop_stress();
      TransHandlePool::destroy();
      DistributedTransHandlePool::destroy();  //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701
      TransCommitThread::destroy();
      CommitEndHandlePool::destroy();
      fifo_stream_.destroy();
    }

    void TransExecutor::on_commit_push_fail(void* ptr)
    {
      YYSYS_LOG(ERROR, "commit push fail, will kill self, task=%p", ptr);
      kill(getpid(), SIGTERM);
    }

    void TransExecutor::handle_packet(ObPacket &pkt)
    {
      int ret = OB_SUCCESS;
      int pcode = pkt.get_packet_code();
      YYSYS_LOG(DEBUG, "start handle packet pcode=%d", pcode);
      if (0 > pcode
          || OB_PACKET_NUM <= pcode)
      {
        easy_request_t *req = pkt.get_request();
        YYSYS_LOG(ERROR, "invalid packet code=%d src=%s",
                  pcode, NULL == req ? NULL : get_peer_ip(req));
        ret = OB_UNKNOWN_PACKET;
      }
      else if (!handle_in_situ_(pcode))
      {
        int64_t task_size = sizeof(Task) + pkt.get_buffer()->get_capacity();
        Task *task = (Task*)allocator_.alloc(task_size);
        if (NULL == task)
        {
          ret = OB_MEM_OVERFLOW;
        }
        else
        {
          task->reset();
          task->pkt = pkt;
          task->src_addr = get_easy_addr(pkt.get_request());
          task->need_response = true; //[521]
          char *data_buffer = (char*)task + sizeof(Task);
          memcpy(data_buffer, pkt.get_buffer()->get_data(), pkt.get_buffer()->get_capacity());
          task->pkt.get_buffer()->set_data(data_buffer, pkt.get_buffer()->get_capacity());
          task->pkt.get_buffer()->get_position() = pkt.get_buffer()->get_position();
          YYSYS_LOG(DEBUG, "task_size=%ld data_size=%ld pos=%ld",
                    task_size, pkt.get_buffer()->get_capacity(), pkt.get_buffer()->get_position());
          (task->pkt).set_receive_ts(yysys::CTimeUtil::getTime());
          ret = push_task_(*task);
        }
      }
      else
      {
        //mod peiouya [MultiUPS] [UPS_SYNC_Frozen] 20150810:b
        if (OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE_ACCORD_HB == pcode
            && !UPS.check_if_need_freeze_for_hb_major ())
        {
          UPS.major_freeze_accord_hb_guard_unlock();
          YYSYS_LOG(DEBUG, "no need to do freeze accord hb, becase of having alredy freezed");
        }
        else
        {
        int64_t packet_timewait = (0 == pkt.get_source_timeout()) ?
                                  UPS.get_param().packet_max_wait_time :
                                  pkt.get_source_timeout();
        // 1.�ȴ��������е������ύ
        // 2.�ȴ�commitlog�������е���־ˢ������
        // �����������й��������ڶ���������˲�ͬ��active_memtable
        ret = session_mgr_.wait_write_session_end_and_lock(packet_timewait);
        if (OB_SUCCESS == ret)
        {

            //[588]
            if(SYS_TABLE_PAXOS_ID == UPS.get_param().paxos_id && OB_UPS_CLEAR_ACTIVE_MEMTABLE != pcode
                    && OB_UPS_ASYNC_CHECK_CUR_VERSION != pcode)
            {
                ret = UPS.set_timer_check_memtable_lock();
                if(OB_SUCCESS != ret)
                {
                    YYSYS_LOG(WARN, "fail to set timer to check memtable lock in paxos 0. ret=%d", ret);
                }
                UPS.get_table_mgr().get_table_mgr()->get_memtable_wr_lock();
            }

          ObSpinLockGuard guard(write_clog_mutex_);
          ThreadSpecificBuffer::Buffer* my_buffer = my_thread_buffer_.get_buffer();
          if (NULL == my_buffer)
          {
            YYSYS_LOG(WARN, "get thread specific buffer fail");
            ret = OB_MEM_OVERFLOW;
          }
          else
          {
            my_buffer->reset();
            ObDataBuffer thread_buff(my_buffer->current(), my_buffer->remain());
            packet_handler_[pcode](pkt, thread_buff);
          }

          //[588]
          if(SYS_TABLE_PAXOS_ID == UPS.get_param().paxos_id && OB_UPS_CLEAR_ACTIVE_MEMTABLE != pcode
                  && OB_UPS_ASYNC_CHECK_CUR_VERSION != pcode)
          {
              UPS.cancel_timer_check_memtable_lock();
          }

          session_mgr_.unlock_write_session();
        }
        else
        {
          YYSYS_LOG(WARN, "wait_write_session_end_and_lock(pkt=%d, timeout=%ld)=>%d", pcode, packet_timewait, ret);
        }
      }
        //mod 20150810:e
      }
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "handle_pkt fail, ret=%d, pkt=%d", ret, pkt.get_packet_code());
        UPS.response_result(ret, pkt);
      }
    }

    bool TransExecutor::handle_in_situ_(const int pcode)
    {
      bool bret = false;
      if (OB_FREEZE_MEM_TABLE == pcode
          || OB_UPS_MINOR_FREEZE_MEMTABLE == pcode
          || OB_UPS_MINOR_LOAD_BYPASS == pcode
          || OB_UPS_MAJOR_LOAD_BYPASS == pcode
          || OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE == pcode
          || OB_UPS_ASYNC_AUTO_FREEZE_MEMTABLE == pcode
          || OB_UPS_CLEAR_ACTIVE_MEMTABLE == pcode
          || OB_UPS_ASYNC_CHECK_CUR_VERSION == pcode
          //|| OB_UPS_ASYNC_CHECK_SSTABLE_CHECKSUM == pcode
          //add peiouya [MultiUPS] [UPS_SYNC_Frozen] 20150521:b
          || OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE_ACCORD_HB == pcode)
          //add 20150521:e
      {
        bret = true;
      }
      return bret;
    }

    int TransExecutor::push_task_(Task &task)
    {
      int ret = OB_SUCCESS;
      switch (task.pkt.get_packet_code())
      {
        case OB_NEW_SCAN_REQUEST:
        case OB_NEW_GET_REQUEST:
        case OB_SCAN_REQUEST:
        case OB_GET_REQUEST:
        case OB_MS_MUTATE:
        case OB_WRITE:
        case OB_TRUNCATE_TABLE: //add zhaoqiong [Truncate table] 20160127
        case OB_PHY_PLAN_EXECUTE:
        case OB_START_TRANSACTION:
        case OB_UPS_ASYNC_KILL_ZOMBIE:
        case OB_UPS_SHOW_SESSIONS:
        case OB_UPS_KILL_SESSION:
        case OB_END_TRANSACTION:
        case OB_WRITE_TRANS_STAT://add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701
        case OB_CHECK_SESSION_EXIST://add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150727
      case OB_GET_PUBLICED_TRANCEID://add by maosy [MultiUps 1.0] [batch_iud_snapshot] 20170622 
        case OB_SWITCH_PARTITION_STAT: //[449]
          ret = TransHandlePool::push(&task, task.pkt.get_req_sign(), task.pkt.get_packet_priority());
          break;
        case OB_SEND_LOG:
        case OB_FAKE_WRITE_FOR_KEEP_ALIVE:
        case OB_SLAVE_REG:
        //add zhaoqiong [fixed for Backup]:20150811:b
        case OB_BACKUP_REG:
        //add:e
        case OB_SWITCH_SCHEMA:
        //add zhaoqiong [Schema Manager] 20150327:b
        case OB_SWITCH_SCHEMA_MUTATOR:
        case OB_SWITCH_TMP_SCHEMA:
        //add:e
        case OB_UPS_FORCE_FETCH_SCHEMA:
        case OB_UPS_SWITCH_COMMIT_LOG:
        case OB_UPS_ASYNC_CHECK_SSTABLE_CHECKSUM:
          ret = TransCommitThread::push(&task);
          break;
        //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
        case OB_END_DISTRIBUTED_TRANS:
        case OB_TRANS_PREPARE_ACK:
        case OB_ROLLBACK_TRANSACTION:
        case OB_PREPARE_TRANSACTION:
        case OB_COMMIT_TRANSACTION:
        case OB_WRITE_TRANS_STAT_ACK:
          ret = DistributedTransHandlePool::push(&task);
          break;
        //add 20150701:e
        default:
          YYSYS_LOG(ERROR, "unknown packet code=%d src=%s",
                    task.pkt.get_packet_code(), inet_ntoa_r(task.src_addr));
          ret = OB_UNKNOWN_PACKET;
          break;
      }
      return ret;
    }

    bool TransExecutor::wait_for_commit_(const int pcode)
    {
      bool bret = true;
      if (OB_MS_MUTATE == pcode
          || OB_WRITE == pcode
          || OB_PHY_PLAN_EXECUTE == pcode
          || OB_END_TRANSACTION == pcode
          //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
          || OB_ROLLBACK_TRANSACTION == pcode
          || OB_COMMIT_TRANSACTION == pcode
          || OB_PREPARE_TRANSACTION == pcode
          || OB_WRITE_TRANS_STAT == pcode
          //add 20150701:e
          || OB_NOP_PKT == pcode)
      {
        bret = false;
      }
      return bret;
    }

    bool TransExecutor::is_only_master_can_handle(const int pcode)
    {
      return OB_FAKE_WRITE_FOR_KEEP_ALIVE == pcode
        || OB_SWITCH_SCHEMA == pcode
        //add zhaoqiong [Schema Manager] 20150327:b
        || OB_SWITCH_SCHEMA_MUTATOR == pcode
        || OB_SWITCH_TMP_SCHEMA == pcode
        //add:e
        || OB_UPS_FORCE_FETCH_SCHEMA == pcode
        || OB_UPS_SWITCH_COMMIT_LOG == pcode
        || OB_UPS_ASYNC_CHECK_SSTABLE_CHECKSUM == pcode;
    }

    int TransExecutor::fill_return_rows_(sql::ObPhyOperator &phy_op, ObNewScanner &scanner, sql::ObUpsResult &ups_result)
    {
      int ret = OB_SUCCESS;
      scanner.reuse();
      const ObRow *row = NULL;
      //add wangyao [sequence for optimize]:20171110
      if (!ups_result.get_k())
      {
          //add e
        while (OB_SUCCESS == (ret = phy_op.get_next_row(row)))
        {
          if (NULL == row)
          {
            YYSYS_LOG(WARN, "row null pointer, phy_op=%p type=%d", &phy_op, phy_op.get_type());
            ret = OB_ERR_UNEXPECTED;
            break;
          }
          if (OB_SUCCESS != (ret = scanner.add_row(*row)))
          {
            YYSYS_LOG(WARN, "add row to scanner fail, ret=%d %s", ret, to_cstring(*row));
            break;
          }
        }
        if (OB_ITER_END == ret)
        {
          if (OB_SUCCESS != (ret = ups_result.set_scanner(scanner)))
          {
            YYSYS_LOG(WARN, "set scanner to ups_result fail ret=%d", ret);
          }
        }
      }
      return ret;
    }

    void TransExecutor::reset_warning_strings_()
    {
      yysys::WarningBuffer *warning_buffer = yysys::get_tsi_warning_buffer();
      if (NULL != warning_buffer)
      {
        warning_buffer->reset();
      }
    }

    void TransExecutor::fill_warning_strings_(sql::ObUpsResult &ups_result)
    {
      yysys::WarningBuffer *warning_buffer = yysys::get_tsi_warning_buffer();
      if (NULL != warning_buffer)
      {
        for (uint32_t i = 0; i < warning_buffer->get_total_warning_count(); i++)
        {
          ups_result.add_warning_string(warning_buffer->get_warning(i));
          YYSYS_LOG(DEBUG, "fill warning string idx=%d [%s]", i, warning_buffer->get_warning(i));
        }
        warning_buffer->reset();
      }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    void TransExecutor::handle_trans(void *ptask, void *pdata)
    {
      ob_reset_err_msg();
      int ret = OB_SUCCESS;
      thread_errno() = OB_SUCCESS;
      bool release_task = true;
      Task *task = (Task*)ptask;
      TransParamData *param = (TransParamData*)pdata;
      int64_t packet_timewait = (NULL == task || 0 == task->pkt.get_source_timeout()) ?
                                UPS.get_param().packet_max_wait_time :
                                task->pkt.get_source_timeout();
      int64_t process_timeout = packet_timewait - QUERY_TIMEOUT_RESERVE;
      if (NULL == task)
      {
        YYSYS_LOG(WARN, "null pointer task=%p", task);
      }
      else if (NULL == param)
      {
        YYSYS_LOG(WARN, "null pointer param data pdata=%p src=%s",
                  pdata, inet_ntoa_r(task->src_addr));
        ret = OB_ERR_UNEXPECTED;
      }
      else if (0 > task->pkt.get_packet_code()
              || OB_PACKET_NUM <= task->pkt.get_packet_code())
      {
        YYSYS_LOG(ERROR, "unknown packet code=%d src=%s",
                  task->pkt.get_packet_code(), inet_ntoa_r(task->src_addr));
        ret = OB_UNKNOWN_PACKET;
      }
      else if (OB_COMMIT_END != task->pkt.get_packet_code()
              && (process_timeout < 0 || process_timeout < (yysys::CTimeUtil::getTime() - task->pkt.get_receive_ts())))
      {
        OB_STAT_INC(UPDATESERVER, UPS_STAT_PACKET_LONG_WAIT_COUNT, 1);
        ret = (OB_SUCCESS == task->last_process_retcode) ? OB_RESPONSE_TIME_OUT : task->last_process_retcode;
        YYSYS_LOG(WARN, "process timeout=%ld not enough cur_time=%ld receive_time=%ld packet_code=%d src=%s",
                  process_timeout, yysys::CTimeUtil::getTime(), task->pkt.get_receive_ts(), task->pkt.get_packet_code(), inet_ntoa_r(task->src_addr));
      }
      else
      {
        //����log�Դ���ǰ�����ֶΣ�trace id��chid���Ժ�����trace id��chidΪ׼
        PROFILE_LOG(DEBUG, TRACE_ID
                    SOURCE_CHANNEL_ID
                    PCODE
                    WAIT_TIME_US_IN_RW_QUEUE,
                    (task->pkt).get_trace_id(),
                    (task->pkt).get_channel_id(),
                    (task->pkt).get_packet_code(),
                    yysys::CTimeUtil::getTime() - (task->pkt).get_receive_ts());
        release_task = trans_handler_[task->pkt.get_packet_code()](*this, *task, *param);
      }
      if (NULL != task)
      {
        if ((OB_SUCCESS != ret && !IS_SQL_ERR(ret) && OB_BEGIN_TRANS_LOCKED != ret)
            || (OB_SUCCESS != thread_errno() && !IS_SQL_ERR(thread_errno()) && OB_BEGIN_TRANS_LOCKED != thread_errno()))
        {
          if (REACH_TIME_INTERVAL(1 * 1000 * 1000))
          {
            YYSYS_LOG(WARN, "process fail ret=%d pcode=%d src=%s",
                      (OB_SUCCESS != ret) ? ret : thread_errno(), task->pkt.get_packet_code(), inet_ntoa_r(task->src_addr));
          }
        }
        //mod hongchen [LOCK_WAIT_TIMEOUT_PARALLEL_BUGFIX] 20170824:b
        if (OB_SUCCESS != ret)
        {
          //UPS.response_result(ret, task->last_process_err_msg_ptr, task->pkt);
          if (__sync_bool_compare_and_swap(task->pkt.get_need_response(), true, false))
          {
            UPS.response_result(ret, task->last_process_err_msg_ptr, task->pkt);
          }
        }
        if (release_task)
        {
          while (!__sync_bool_compare_and_swap(task->pkt.get_mutex_flag(), false, true))
          {
            if (TC_REACH_TIME_INTERVAL(1 * 1000 * 1000))
            {
              YYSYS_LOG(WARN, "too slow for acquire mutex for task->pkt!");
              //[616]
              if(task->pkt.get_packet_code() == OB_COMMIT_END)
              {
                  break;
              }
            }
//add support for arm platform by wangd 202106:b
#if defined(__aarch64__)
            asm("yield");
#else
//add support for arm platform by wangd 202106:e
            asm("pause");
#endif //add support for arm platform by wangd 202106
          }
          allocator_.free(task);
          task = NULL;
        }
        //mod hongchen [LOCK_WAIT_TIMEOUT_PARALLEL_BUGFIX] 20170824:e
      }
    }

#define LOG_SESSION(header, session_ctx, task)                     \
  FILL_TRACE_BUF(session_ctx->get_tlog_buffer(), header " packet wait=%ld start_time=%ld timeout=%ld src=%s fd=%d %s ctx=%p", \
                 yysys::CTimeUtil::getTime() - task.pkt.get_receive_ts(), \
                 task.pkt.get_receive_ts(),                             \
                 task.pkt.get_source_timeout(),                         \
                 inet_ntoa_r(task.src_addr),                            \
                 get_fd(task.pkt.get_request()),                        \
                 to_cstring(task.sid),                                  \
                 session_ctx);

    //mod peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
    //int TransExecutor::get_session_type(const ObTransID& sid, SessionType& type, const bool check_session_expired)
    int TransExecutor::get_session_type(const ObTransID& sid, const ObTransID& coordinator_sid, SessionType& type, const bool check_session_expired)
    //mod 20150701:e
    {
      int ret = OB_SUCCESS;
      SessionGuard session_guard(session_mgr_, lock_mgr_, ret);
      //mod peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
      //BaseSessionCtx *session_ctx = NULL;
      RWSessionCtx *session_ctx = NULL;
      //mod:20150701:e
      if (OB_SUCCESS != (ret = session_guard.fetch_session(sid, session_ctx, check_session_expired)))
      {
        YYSYS_LOG(WARN, "fetch_session(%s)=>%d", to_cstring(sid), ret);
      }
      else
      {
        type = session_ctx->get_type();
        session_ctx->set_coordinator_info(coordinator_sid);  //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701
      }
      return ret;
    }

    bool TransExecutor::handle_write_trans_(Task &task, ObMutator &mutator, ObNewScanner &scanner)
    {
      int &ret = thread_errno();
      ret = OB_SUCCESS;
      ObTransReq req;
      SessionGuard session_guard(session_mgr_, lock_mgr_, ret);
      RWSessionCtx *session_ctx = NULL;
      ILockInfo *lock_info = NULL;
      int64_t pos = task.pkt.get_buffer()->get_position();
      int64_t packet_timewait = (0 == task.pkt.get_source_timeout()) ?
                                UPS.get_param().packet_max_wait_time :
                                task.pkt.get_source_timeout();
      int64_t process_timeout = packet_timewait - QUERY_TIMEOUT_RESERVE;
      req.start_time_ = task.pkt.get_receive_ts();
      req.timeout_ = process_timeout;
      req.idle_time_ = process_timeout;
      task.sid.reset();
      bool give_up_lock = true;
      //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150709:b
      ObTransID trans_id;
      bool commit = false;
      //add 20150709:e
      if (!UPS.is_master_lease_valid())
      {
        ret = OB_NOT_MASTER;
        YYSYS_LOG(WARN, "ups master_lease is invalid, pcode=%d", task.pkt.get_packet_code());
      }
      //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150709:b
      else if (OB_WRITE_TRANS_STAT == task.pkt.get_packet_code())
      {
        int64_t trans_memtable_version = -1;
        if (OB_SUCCESS != (ret = trans_id.deserialize(task.pkt.get_buffer()->get_data(), task.pkt.get_buffer()->get_capacity(), task.pkt.get_buffer()->get_position())))
        {
          YYSYS_LOG(ERROR, "deseralize trans id fail, ret=>%d", ret);
        }
        else if(OB_SUCCESS != (ret = serialization::decode_bool(task.pkt.get_buffer()->get_data(), task.pkt.get_buffer()->get_capacity(),
                                                                task.pkt.get_buffer()->get_position(), &commit)))
        {
          YYSYS_LOG(ERROR, "deseralize commit stat fail, ret=>%d", ret);
        }
        else if(OB_SUCCESS != (ret = serialization::decode_i64(task.pkt.get_buffer()->get_data(), task.pkt.get_buffer()->get_capacity(),
                                                                task.pkt.get_buffer()->get_position(), &trans_memtable_version)))
        {
          YYSYS_LOG(ERROR, "deseralize trans_memtable_version fail, ret=>%d", ret);
        }
        else
        {
          int64_t current_version = UPS.get_table_mgr().get_table_mgr()->get_active_version();
          if(0 != trans_memtable_version && current_version != trans_memtable_version)
          {
            YYSYS_LOG(WARN, "memtable version not match, will rollback, trans version:%ld, memtable version:%ld", trans_memtable_version, current_version);
            //ret = OB_MEM_VERSION_NOT_MATCH;
            commit = false;
          }
          if (OB_SUCCESS != (ret = ui_.ui_set_mutator(mutator, trans_id, commit)))
          {
            YYSYS_LOG(WARN, "set mutator info fail, trans_id:%s, commit:%d, ret=%d", to_cstring(trans_id), commit, ret);
          }
        }
      }
      //add 20150709:e
      else if (OB_SUCCESS != (ret = ui_.ui_deserialize_mutator(*task.pkt.get_buffer(), mutator)))
      {
        YYSYS_LOG(WARN, "deserialize mutator fail ret=%d", ret);
      }
      else
      {
        task.pkt.get_buffer()->get_position() = pos;
      }
      //del 20150701:e
      if (OB_SUCCESS != ret)
      {}
      //mod peiouya [MultiUPS] [UPS_SYNC_Frozen] 20150811:b
      //else if (OB_SUCCESS != (ret = session_guard.start_session(req, task.sid, session_ctx)))
      else if (OB_SUCCESS != (ret = session_guard.start_session(req, task.sid, session_ctx, static_cast<common::PacketCode>(task.pkt.get_packet_code()))))
      //mod 20150811:e
      {
        if (OB_BEGIN_TRANS_LOCKED == ret)
        {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = TransHandlePool::push(&task,
                  task.pkt.get_req_sign(),
                  task.pkt.get_packet_priority())))
          {
            ret = tmp_ret;
          }
        }
        else
        {
          YYSYS_LOG(ERROR, "start_session()=>%d", ret);
        }
      }
      else
      {
        //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
        if (OB_WRITE_TRANS_STAT == task.pkt.get_packet_code())
        {
          session_ctx->set_coordinator_info(trans_id);
          session_ctx->set_trans_stat(commit);
          //[588]
          UPS.get_table_mgr().get_table_mgr()->get_memtable_rd_lock();
        }
        //add 20150701:e
        LOG_SESSION("mutator start", session_ctx, task);
        session_ctx->set_conflict_session_id(task.last_conflict_session_id);
        session_ctx->set_start_handle_time(yysys::CTimeUtil::getTime());
        session_ctx->set_stmt_start_time(task.pkt.get_receive_ts());
        session_ctx->set_stmt_timeout(process_timeout);
        session_ctx->set_self_processor_index(TransHandlePool::get_thread_index() + TransHandlePool::get_thread_num());
        session_ctx->set_priority((PriorityPacketQueueThread::QueuePriority)task.pkt.get_packet_priority());
        if (ST_READ_WRITE != session_ctx->get_type())
        {
          ret = OB_TRANS_NOT_MATCH;
          YYSYS_LOG(ERROR, "session.type[%d] is not RW", session_ctx->get_type());
        }
        else if (NULL == (lock_info = session_ctx->get_lock_info()))
        {
          ret = OB_NOT_INIT;
          YYSYS_LOG(ERROR, "lock_info == NULL");
        }
        //mod peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
        //else if (OB_SUCCESS != (ret = UPS.get_table_mgr().apply(OB_WRITE != task.pkt.get_packet_code(), *session_ctx, *lock_info, mutator)))
        else if (OB_SUCCESS != (ret = UPS.get_table_mgr().apply((OB_WRITE != task.pkt.get_packet_code() && OB_WRITE_TRANS_STAT != task.pkt.get_packet_code()),
                                                                *session_ctx, *lock_info, mutator)))
        //mod 20150701:e
        {
          if ((OB_ERR_EXCLUSIVE_LOCK_CONFLICT == ret
                || OB_ERR_SHARED_LOCK_CONFLICT == ret)
              && !session_ctx->is_session_expired()
              && !session_ctx->is_stmt_expired())
          {
            int tmp_ret = OB_SUCCESS;
            task.last_conflict_session_id = session_ctx->get_conflict_session_id();
            if (OB_SUCCESS != (tmp_ret = TransHandlePool::push(&task,
                                                               0,
                    task.pkt.get_packet_priority())))
            {
              YYSYS_LOG(WARN, "push back task fail, ret=%d conflict_processor_index=%ld task=%p",
                        tmp_ret, session_ctx->get_conflict_processor_index(), &task);
            }
            else
            {
              give_up_lock = false;
            }
          }
          if (give_up_lock)
          {
            OB_STAT_INC(UPDATESERVER, UPS_STAT_APPLY_FAIL_COUNT, 1);
            YYSYS_LOG(WARN, "table mgr apply fail ret=%d", ret);
          }
        }
        else
        {
          scanner.reuse();
          if (OB_SUCCESS != (ret = session_ctx->get_ups_result().set_scanner(scanner)))
          {
            YYSYS_LOG(ERROR, "session_ctx.set_scanner()=>%d", ret);
          }
          else if (OB_SUCCESS != (ret = session_ctx->get_ups_mutator().get_mutator().pre_serialize()))
          {
            YYSYS_LOG(ERROR, "session_ctx.mutator.pre_serialize()=>%d", ret);
          }
          else
          {
            YYSYS_LOG(DEBUG, "precommit end timeu=%ld", yysys::CTimeUtil::getTime() - task.pkt.get_receive_ts());
            OB_STAT_INC(UPDATESERVER, get_stat_num(session_ctx->get_priority(), APPLY, COUNT), 1);
            OB_STAT_INC(UPDATESERVER, get_stat_num(session_ctx->get_priority(), APPLY, QTIME), session_ctx->get_start_handle_time() - session_ctx->get_stmt_start_time());
            OB_STAT_INC(UPDATESERVER, get_stat_num(session_ctx->get_priority(), APPLY, TIMEU), yysys::CTimeUtil::getTime() - session_ctx->get_start_handle_time());
            session_guard.revert();
            ret = TransCommitThread::push(&task);
            if (OB_SUCCESS != ret && task.sid.is_valid())
            {
              session_mgr_.end_session(task.sid.descriptor_, true);
            }
          }
        }
        //[588]
        if(OB_SUCCESS != ret && OB_WRITE_TRANS_STAT == task.pkt.get_packet_code())
        {
            UPS.get_table_mgr().get_table_mgr()->release_memtable_lock();
        }
      }
      if (OB_SUCCESS != ret && OB_BEGIN_TRANS_LOCKED != ret && give_up_lock)
      {
        UPS.response_result(ret, task.pkt);
      }
      return (OB_SUCCESS != ret && OB_BEGIN_TRANS_LOCKED != ret && give_up_lock);
    }
//add by maosy [MultiUps 1.0] [batch_iud_snapshot] 20170622 b:
    bool TransExecutor::handle_get_publised_transid_(Task &task, ObDataBuffer &buffer)
    {
        int &ret = thread_errno();
        bool need_free_task = true;
        ret = OB_SUCCESS ;
        int64_t trans_id = 0 ;
        if (!UPS.is_master_lease_valid())
        {
          ret = OB_NOT_MASTER;
          YYSYS_LOG(WARN, "ups master_lease is invalid, pcode=%d", task.pkt.get_packet_code());
        }
        else
        {
            trans_id = session_mgr_.get_published_trans_id();
            UPS.response_published_transid(ret,task.pkt,trans_id,buffer);
        }
        return need_free_task;
    }
	// add by maosy e
    bool TransExecutor::handle_start_session_(Task &task, ObDataBuffer &buffer)
    {
      int &ret = thread_errno();
      bool need_free_task = true;
      ret = OB_SUCCESS;
      ObTransReq req;
      int64_t pos = task.pkt.get_buffer()->get_position();
      SessionGuard session_guard(session_mgr_, lock_mgr_, ret);
      BaseSessionCtx* session_ctx = NULL;
      task.sid.reset();
      if (!UPS.is_master_lease_valid())
      {
        ret = OB_NOT_MASTER;
        YYSYS_LOG(WARN, "ups master_lease is invalid, pcode=%d", task.pkt.get_packet_code());
      }
      else if (OB_SUCCESS != (ret = req.deserialize(task.pkt.get_buffer()->get_data(),
                                                    task.pkt.get_buffer()->get_capacity(),
                                                    pos)))
      {
        YYSYS_LOG(WARN, "deserialize session_req fail ret=%d", ret);
      }
      else
      {
        req.start_time_ = task.pkt.get_receive_ts();
      }
      if (OB_SUCCESS != ret)
      {}
      //mod peiouya [MultiUPS] [UPS_SYNC_Frozen] 20150811:b
      //else if (OB_SUCCESS != (ret = session_guard.start_session(req, task.sid, session_ctx)))
      else if (OB_SUCCESS != (ret = session_guard.start_session(req, task.sid, session_ctx, static_cast<common::PacketCode>(task.pkt.get_packet_code()))))
      //mod 20150811:e
      {
        if (OB_BEGIN_TRANS_LOCKED == ret)
        {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = TransHandlePool::push(&task,
                  task.pkt.get_req_sign(),
                  task.pkt.get_packet_priority())))
          {
            ret = tmp_ret;
          }
          else
          {
            need_free_task = false;
          }
        }
        else
        {
          YYSYS_LOG(WARN, "begin session fail ret=%d", ret);
        }
      }
      else
      {
        LOG_SESSION("session start", session_ctx, task);
        PRINT_TRACE_BUF(session_ctx->get_tlog_buffer());
      }
      if (need_free_task)
      {
        UPS.response_trans_id(ret, task.pkt, task.sid, buffer);
      }
      return need_free_task;
    }

    bool TransExecutor::handle_end_session_(Task &task, ObDataBuffer &buffer)
    {
      int &ret = thread_errno();
      ret = OB_SUCCESS;
      bool need_free_task = true;
      ObEndTransReq req;
      SessionType type = ST_READ_ONLY;
      UNUSED(buffer);
      task.sid.reset();
      const bool check_session_expired = true;
      int64_t pos = task.pkt.get_buffer()->get_position();
      if (!UPS.is_master_lease_valid())
      {
        ret = OB_NOT_MASTER;
        YYSYS_LOG(WARN, "ups master_lease is invalid, pcode=%d", task.pkt.get_packet_code());
      }
      else if (OB_SUCCESS != (ret = req.deserialize(task.pkt.get_buffer()->get_data(),
                                                    task.pkt.get_buffer()->get_capacity(),
                                                    pos)))
      {
        YYSYS_LOG(WARN, "deserialize session_req fail ret=%d", ret);
      }
      else if (!req.trans_id_.is_valid())
      {
        ret = OB_TRANS_NOT_MATCH;
        YYSYS_LOG(ERROR, "sid[%s] is invalid", to_cstring(task.sid));
      }
      if (OB_SUCCESS != ret)
      {
        UPS.response_result(ret, task.pkt);
      }
      //mod peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
      //else if (OB_SUCCESS != (ret = get_session_type(req.trans_id_, type, check_session_expired)))
      else if (OB_SUCCESS != (ret = get_session_type(req.trans_id_, req.participant_trans_id_[0],type, check_session_expired)))
      //mod 20150701:e
      {
        UPS.response_result(req.rollback_? OB_SUCCESS: ret, task.pkt);
        YYSYS_LOG(WARN, "get_session_type(%s)=>%d", to_cstring(req.trans_id_), ret);
      }
      else
      {
        // sid ��ȷ
        if (ST_READ_WRITE == type && !req.rollback_)
        {
          task.sid = req.trans_id_;
          //add duyr [MultiUPS] [READ_ATOMIC] [write_part] 20151117:b
          if (OB_PREPARE_TRANSACTION == task.pkt.get_packet_code())
          {
            SessionGuard session_guard(session_mgr_, lock_mgr_, ret);
            RWSessionCtx* session_ctx = NULL;
            if (OB_SUCCESS != (ret = session_guard.fetch_session(req.trans_id_, session_ctx)))
            {
              YYSYS_LOG(ERROR, "fetch_session(sid=%s)=>%d", to_cstring(req.trans_id_), ret);
            }
            else if (NULL == session_ctx)
            {
              ret = OB_ERR_UNEXPECTED;
              YYSYS_LOG(ERROR,"session_ctx=NULL!ret=%d",ret);
            }
            else
            {
              //backfill the coordinator_sid as trans version
              ret = session_ctx->backfill_trans_version();
              if (OB_SUCCESS != ret)
              {
                YYSYS_LOG(WARN,"store distr_trans_version failed!ret=%d",ret);
              }
              YYSYS_LOG(DEBUG,"read_atomic::debug,finish backfill_trans_version on prepare!coor_trans_id=[%s]",
                        to_cstring(session_ctx->get_coordinator_info()));
              session_ctx->set_can_be_killed(false); //[521]
            }
          }

          if (OB_SUCCESS != ret)
          {
          }
          //add duyr 20151117:e
          //mod duyr [MultiUPS] [READ_ATOMIC] [write_part] 20151117:b
//          if (OB_SUCCESS != (ret = TransCommitThread::push(&task)))
          else if (OB_SUCCESS != (ret = TransCommitThread::push(&task)))
          //mod duyr 20151117:e
          {
            YYSYS_LOG(ERROR, "push task=%p to TransCommitThread fail, ret=%d %s", &task, ret, to_cstring(req.trans_id_));
            session_mgr_.end_session(req.trans_id_.descriptor_, true);
            UPS.response_result(OB_TRANS_ROLLBACKED, task.pkt);
          }
          else
          {
            need_free_task = false;
          }
        }
        else
        {
          ret = session_mgr_.end_session(req.trans_id_.descriptor_, req.rollback_);
          UPS.response_result(req.rollback_? OB_SUCCESS: ret, task.pkt);
        }
      }
      return need_free_task;
    }
    //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150716:b
    /*@berif ���׶��ύ*/
    bool TransExecutor::handle_end_trans_(Task &task, common::ObDataBuffer &buffer)
    {
      int &ret = thread_errno();
      ret = OB_SUCCESS;
      bool need_free_task = true;
      ObEndTransReq req;
      UNUSED(buffer);
      task.sid.reset();
      const bool check_session_expired = true;
      int end_session_ret = OB_SUCCESS;
      SessionGuard session_guard(session_mgr_, lock_mgr_, end_session_ret);
      RWSessionCtx* session_ctx = NULL;
      int64_t pos = task.pkt.get_buffer()->get_position();
      if (!UPS.is_master_lease_valid())
      {
        ret = OB_NOT_MASTER;
        YYSYS_LOG(WARN, "ups master_lease is invalid, pcode=%d", task.pkt.get_packet_code());
      }
      else if (OB_SUCCESS != (ret = req.deserialize(task.pkt.get_buffer()->get_data(),
                                                    task.pkt.get_buffer()->get_capacity(),
                                                    pos)))
      {
        YYSYS_LOG(WARN, "deserialize session_req fail ret=%d", ret);
      }
      else if (!req.trans_id_.is_valid())
      {
        ret = OB_TRANS_NOT_MATCH;
        YYSYS_LOG(ERROR, "sid[%s] is invalid", to_cstring(req.trans_id_));
      }
      else if (req.rollback_)
      {
         ret = OB_ERROR;
         YYSYS_LOG(ERROR, "should not be here");
      }
      if (OB_SUCCESS != ret)
      {
        UPS.response_result(ret, task.pkt);
      }
      else if (OB_SUCCESS != (ret = session_guard.fetch_session(req.trans_id_, session_ctx, check_session_expired)))
      {
        UPS.response_result(ret, task.pkt);
        YYSYS_LOG(WARN, "Session has been killed, error %d, \'%s\'", ret, to_cstring(req.trans_id_));
      }
      else if (OB_SUCCESS != (ret = session_ctx->set_participant_info(req)))
      {
        UPS.response_result(ret, task.pkt);
        YYSYS_LOG(WARN, "set participant info failed, trans rollback, ret=%d", ret);
      }
      else
      {
        need_free_task = false;
        session_ctx->set_task(&task);
        session_guard.revert();

        session_ctx->set_accept_prepare_ack(true);
        if (OB_SUCCESS != (ret = prepare_distributed_trans(req.participant_trans_id_,req.participant_num_)))
        {
          YYSYS_LOG(WARN, "prepare distributed trans failed, trans rollback, ret=%d", ret);
          //rollback
          bool commit = false;
          session_ctx->set_accept_prepare_ack(false);
          if(session_ctx->get_write_trans_stat_time() != 0 &&
             yysys::CTimeUtil::getTime() - session_ctx->get_write_trans_stat_time() >
             UPS.get_param().fetch_trans_stat_wait_time)
          {
            YYSYS_LOG(WARN, "maybe already write stat by timeout thread");
          }
          else if (OB_SUCCESS != (ret = write_trans_stat(*session_ctx, req.participant_trans_id_[0],commit)))
          {
            YYSYS_LOG(ERROR, "write trans rollback record failed, ret=%d", ret);
          }
        }
      }
      return need_free_task;
    }

    bool TransExecutor::handle_prepare_ack_(Task &task, common::ObDataBuffer &buffer)
    {
      int &ret = thread_errno();
      ret = OB_SUCCESS;
      bool need_free_task = true;
      int end_session_ret = OB_SUCCESS;
      SessionGuard session_guard(session_mgr_, lock_mgr_, end_session_ret);
      RWSessionCtx* session_ctx = NULL;

      ObTransID tid;//coordinator tid, used for fetch coordinator info
      ObServer participant;
      bool is_rollback = false;
      UNUSED(buffer);
      int64_t pos = task.pkt.get_buffer()->get_position();
      if (!UPS.is_master_lease_valid())
      {
        ret = OB_NOT_MASTER;
        YYSYS_LOG(WARN, "ups master_lease is invalid, pcode=%d", task.pkt.get_packet_code());
      }
      else if (OB_SUCCESS != (ret = tid.deserialize(task.pkt.get_buffer()->get_data(),
                                                    task.pkt.get_buffer()->get_capacity(),
                                                    pos)))
      {
        YYSYS_LOG(WARN, "deserialize coordinator trans id fail ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = participant.deserialize(task.pkt.get_buffer()->get_data(),
                                                    task.pkt.get_buffer()->get_capacity(),
                                                    pos)))
      {
        YYSYS_LOG(WARN, "deserialize participant fail ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::decode_bool(task.pkt.get_buffer()->get_data(),
                                                               task.pkt.get_buffer()->get_capacity(),
                                                               pos,
                                                               &is_rollback)))
      {
        YYSYS_LOG(WARN, "deserialize is_rollback fail ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = session_guard.fetch_session(tid, session_ctx)))
      {
        YYSYS_LOG(WARN, "Session has been killed, error %d, \'%s\'", ret, to_cstring(tid));
      }
      else if(session_ctx->get_accept_prepare_ack())
      {
        YYSYS_LOG(DEBUG,"prepare[%s] stat[%d],",to_cstring(participant),is_rollback);
        session_ctx->set_participant_ack(participant);
        if (is_rollback)
        {
          session_ctx->set_someone_prepare_failed();
        }

        if(session_ctx->all_participant_ack())
        {
          //bool commit = true;
          bool commit = !session_ctx->is_someone_prepare_failed();
          YYSYS_LOG(DEBUG,"receive all prepare, commit stat[%d]", commit);
          if(session_ctx->get_write_trans_stat_time() != 0 &&
             yysys::CTimeUtil::getTime() - session_ctx->get_write_trans_stat_time() >
             UPS.get_param().fetch_trans_stat_wait_time)
          {
            YYSYS_LOG(WARN, "maybe already write stat by timeout thread");
          }
          else if (OB_SUCCESS != (ret = session_mgr_.check()))
          {
              YYSYS_LOG(WARN, "stop commit,unsettled trans will finish it. trans is:%s", to_cstring(tid));
          }
          else if (OB_SUCCESS != (ret = write_trans_stat(*session_ctx, tid,commit)))
          {
            YYSYS_LOG(WARN, "write trans commit record failed, ret=%d", ret);
          }
        }
      }
      UPS.response_result(OB_SUCCESS, task.pkt);//bugfix peiouya
      return need_free_task;
    }

    bool TransExecutor::handle_write_stat_ack_(Task &task, common::ObDataBuffer &buffer)
    {
      int &ret = thread_errno();
      ret = OB_SUCCESS;
      bool need_free_task = true;
      int end_session_ret = OB_SUCCESS;
      SessionGuard session_guard(session_mgr_, lock_mgr_, end_session_ret);
      RWSessionCtx* session_ctx = NULL;

      ObTransID tid;//coordinator tid, used for fetch coordinator info
      bool commit_stat = false; //sys table record
      UNUSED(buffer);
      int64_t pos = task.pkt.get_buffer()->get_position();
      if (!UPS.is_master_lease_valid())
      {
        ret = OB_NOT_MASTER;
        YYSYS_LOG(WARN, "ups master_lease is invalid, pcode=%d", task.pkt.get_packet_code());
      }
      else if (OB_SUCCESS != (ret = tid.deserialize(task.pkt.get_buffer()->get_data(),
                                                    task.pkt.get_buffer()->get_capacity(),
                                                    pos)))
      {
        YYSYS_LOG(WARN, "deserialize coordinator trans id fail ret=%d", ret);
      }
      else if(OB_SUCCESS != (ret = serialization::decode_bool(task.pkt.get_buffer()->get_data(),
                                                              task.pkt.get_buffer()->get_capacity(),
                                                              pos, &commit_stat)))
      {
        YYSYS_LOG(WARN, "deserialize commit_stat fail ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = session_guard.fetch_session(tid, session_ctx)))
      {
        YYSYS_LOG(WARN, "Session has been killed, error %d, \'%s\'", ret, to_cstring(tid));
      }
      else if (OB_SUCCESS != (ret = session_mgr_.check()))
      {
          YYSYS_LOG(WARN, "stop commit,unsettled trans will finish it. trans id:%s", to_cstring(tid));
      }
      else if(commit_stat)
      {
        //write commit log ack, commit trans
        YYSYS_LOG(DEBUG, "receive write stat ok,trans_id:%s commit", to_cstring(tid));
        commit_distributed_trans(session_ctx->get_participant_info(),session_ctx->get_participant_num());
      }
      else
      {
        //write rollback log ack, rollback trans
        YYSYS_LOG(DEBUG, "receive write stat ok,trans_id:%s rollback", to_cstring(tid));
        rollback_distributed_trans(session_ctx->get_participant_info(),session_ctx->get_participant_num());
      }
      UPS.response_result(OB_SUCCESS, task.pkt);//bugfix peiouya
      return need_free_task;
    }

    /*@berif �����е�ups(Э����+������)����prepare����*/
    int TransExecutor::prepare_distributed_trans(const ObTransID* participant_tid, const int32_t participant_num)
    {
      int ret = OB_SUCCESS;
      if(NULL == participant_tid || participant_num <= 0)
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(WARN, "invalid argument, participant can not be null");
      }
      else
      {
        int64_t timeout = UPS.get_param().dis_trans_process_timeout;
        ObEndTransReq tmp_req;
        tmp_req.participant_num_ = 2; //mod chensy from 1 to 2 [304]
        tmp_req.participant_trans_id_[0] = participant_tid[0];

        for( int i = 0; i < participant_num; i++)
        {
            if(i != 0)
            {
                tmp_req.participant_trans_id_[0].dis_trans_role_ = PARTTICIPANT;
                tmp_req.participant_trans_id_[0].paxos_id_ = UPS.get_param().paxos_id;
            }
            else
            {
                tmp_req.participant_trans_id_[0].dis_trans_role_ = COORDINATOR;
                tmp_req.participant_trans_id_[0].paxos_id_ = UPS.get_param().paxos_id;
            }
          tmp_req.trans_id_ = participant_tid[i];
          if(OB_SUCCESS != (ret = UPS.get_rpc_stub().ups_prepare_trans(timeout,participant_tid[i].ups_, tmp_req)))
          {
            YYSYS_LOG(WARN, "send prepare to %s failed, trans will rollback ,ret=%d", to_cstring(participant_tid[i]), ret);
            break;
          }
          else
          {
            YYSYS_LOG(DEBUG, "send prepare to %s", to_cstring(participant_tid[i]));
          }
        }
      }
      return ret;
    }

    void TransExecutor::rollback_distributed_trans(const ObTransID* participant_tid, const int32_t participant_num)
    {
      if(NULL == participant_tid)
      {
        YYSYS_LOG(WARN, "invalid argument, participant can not be null");
      }
      else
      {
        int64_t timeout = UPS.get_param().dis_trans_process_timeout;
        //mod hongchen [OPTI_FOR_DIS_TRANS] 20170627:b
        /*
        for( int i = 0; i < participant_num;i++)
        {
          UPS.get_rpc_stub().ups_rollback_trans(timeout,participant_tid[i].ups_, participant_tid[i]);
          YYSYS_LOG(DEBUG, "coordinator[%s] send rollback to %s", to_cstring(participant_tid[0]), to_cstring(participant_tid[i]));
        }
        * */
        int ups_self_idx = -1;
        //first,send to other participants except cordinate
        for( int i = 0; i < participant_num;i++)
        {
          if (UPS.get_self() != participant_tid[i].ups_)
          {
            UPS.get_rpc_stub().ups_rollback_trans(timeout,participant_tid[i].ups_, participant_tid[i]);
            YYSYS_LOG(DEBUG, "coordinator[%s] send rollback to %s", to_cstring(participant_tid[0]), to_cstring(participant_tid[i]));
          }
          else
          {
            ups_self_idx = i;
          }
        }
        //last send to itself, means cordinate
        OB_ASSERT(0 <= ups_self_idx);
        UPS.get_rpc_stub().ups_rollback_trans(timeout,UPS.get_self(), participant_tid[ups_self_idx]);
        YYSYS_LOG(DEBUG, "send commit to %s", to_cstring(UPS.get_self()));
        //mod hongchen [OPTI_FOR_DIS_TRANS] 20170627:e
        YYSYS_LOG(WARN, "distributed transaction is rollbacked, coordinator[%s]", to_cstring(participant_tid[0]));
      }
    }

    void TransExecutor::commit_distributed_trans(const ObTransID* participant_tid, const int32_t participant_num)
    {
      if(NULL == participant_tid)
      {
        YYSYS_LOG(WARN, "invalid argument, participant can not be null");
      }
      else
      {
        int64_t timeout = UPS.get_param().dis_trans_process_timeout;
        //mod hongchen [OPTI_FOR_DIS_TRANS] 20170627:b
        /*
        for( int i = 0; i < participant_num;i++)
        {
          UPS.get_rpc_stub().ups_commit_trans(timeout,participant_tid[i].ups_, participant_tid[i]);
          YYSYS_LOG(DEBUG, "send commit to %s", to_cstring(participant_tid[i]));
        }
        * */
        int ups_self_idx = -1;
        //first,send to other participants except cordinate
        for( int i = 0; i < participant_num;i++)
        {
          if (UPS.get_self() != participant_tid[i].ups_)
          {
            UPS.get_rpc_stub().ups_commit_trans(timeout,participant_tid[i].ups_, participant_tid[i]);
            YYSYS_LOG(DEBUG, "send commit to %s", to_cstring(participant_tid[i]));
          }
          else
          {
            ups_self_idx = i;
          }
        }
        //last send to itself, means cordinate
        OB_ASSERT(0 <= ups_self_idx);
        UPS.get_rpc_stub().ups_commit_trans(timeout,UPS.get_self(), participant_tid[ups_self_idx]);
        YYSYS_LOG(DEBUG, "send commit to %s", to_cstring(UPS.get_self()));
        //mod hongchen [OPTI_FOR_DIS_TRANS] 20170627:e
      }
    }

    int TransExecutor::write_trans_stat(RWSessionCtx& ctx,const ObTransID& trans_id, bool commit)
    {
      int ret = OB_SUCCESS;

      //mod [521]
      int64_t last_write_time = ctx.get_write_trans_stat_time();
      if(last_write_time == ATOMIC_CAS(ctx.get_write_trans_stat_time_for_sync(), last_write_time, yysys::CTimeUtil::getTime()))
      {
          //ctx.set_write_trans_stat_time(yysys::CTimeUtil::getTime());
          ObServer sys_ups = UPS.get_sys_ups();

          YYSYS_LOG(DEBUG, "sys ups:%s, memtable version = %ld", to_cstring(sys_ups),ctx.get_mem_version());
          if(!sys_ups.is_valid())
          {
              ret = OB_ERROR;
              YYSYS_LOG(WARN, "get system ups fail");
          }
          else if(OB_SUCCESS != (ret = UPS.get_ups_rpc_stub().mutate_distribute_stat(sys_ups,trans_id,commit,ctx.get_mem_version())))
          {
              YYSYS_LOG(WARN, "apply mutator fail:ret[%d]", ret);
          }
      }
      else
      {
          ret = OB_ERROR;
          YYSYS_LOG(WARN, "already write trans stat");
      }

      return ret;
    }

    int TransExecutor::end_sub_trans(RWSessionCtx &ctx, const common::ObTransID& trans_id, const bool commit)
    {
      int ret = OB_SUCCESS;
      YYSYS_LOG(INFO, "enter end sub trans, commit:%d",commit);
      //mod[521]
      if(__sync_bool_compare_and_swap(ctx.get_writting_trans_stat_for_sync(), false, true))
      {
          //ctx.set_writting_trans_stat(true);
          int32_t packet_code = OB_ROLLBACK_TRANSACTION;
          if(commit)
          {
              packet_code = OB_COMMIT_TRANSACTION;
          }
          Task * task = (Task*)allocator_.alloc(sizeof(TransExecutor::Task));
          if (NULL == task)
          {
              ret = OB_MEM_OVERFLOW;
              YYSYS_LOG(ERROR, "no memory");
          }
          else
          {
              //[616]
              task->pkt.reset_lock_wait_para();

              task->pkt.set_packet_code(packet_code);
              task->sid = trans_id;
              task->need_response = false;
              if (OB_SUCCESS != TransCommitThread::push(task))
              {
                  ctx.set_writting_trans_stat(false);
                  allocator_.free(task);
                  YYSYS_LOG(ERROR, "end part trans error, commit:%d",commit);
              }
          }
      }
      else
      {
          ret = OB_ALREADY_WRITE_STAT_LOG;
          YYSYS_LOG(WARN, "already write stat log");
      }
      return ret;
    }

    int TransExecutor::handle_unsettled_participant_trans(RWSessionCtx &ctx, const common::ObTransID& trans_id)
    {
      int ret = OB_SUCCESS;
      bool commit = false;
      bool find_trans_stat = false;
      bool need_end_sub_trans = false;

      if(ctx.get_coordinator_info().descriptor_ != ObTransID::INVALID_SESSION_ID)
      {
          //fetch from sys table
          if(OB_SUCCESS != (ret = fetch_trans_stat(ctx.get_coordinator_info(),find_trans_stat,commit)))
          {
              //get failed, wait
              YYSYS_LOG(WARN, "fetch trans stat failed, coordinator:%s, ret: %d", to_cstring(ctx.get_coordinator_info()), ret);
          }
          else if(!find_trans_stat)
          {
              //sys table have no record, need check coordinator exist
              bool coordinator_exist = false;
              ObServer coordinator_master_ups;
              int64_t timeout_us = 3000000; //mark dduuhhtt
              bool tmplog_replay_finished = false;
              if(OB_SUCCESS != (ret = UPS.get_rpc_stub().find_session_exist(UPS.get_param().dis_trans_process_timeout,ctx.get_coordinator_info(),coordinator_exist)))
              {
                  if(OB_SUCCESS != (ret = UPS.get_ups_rpc_stub().get_master_ups_info_by_paxos_id(UPS.get_root_server(),coordinator_master_ups,ctx.get_coordinator_info().paxos_id_,timeout_us)))
                  {
                      YYSYS_LOG(INFO," fetch coordinator master ups failed, wait, coordinator_info [%s]", to_cstring(ctx.get_coordinator_info()));
                  }
                  else if(coordinator_master_ups == ctx.get_coordinator_info().ups_)
                  {
                      YYSYS_LOG(INFO,"coordinator ups exist, wait,coordinator_info [%s]", to_cstring(ctx.get_coordinator_info()));
                  }
                  else
                  {
                      if(OB_SUCCESS != (ret = UPS.get_rpc_stub().is_coordinator_master_ups_tmplog_replay_finished(UPS.get_param().dis_trans_process_timeout,coordinator_master_ups,tmplog_replay_finished)))
                      {
                          YYSYS_LOG(INFO,"fetch coordinator ups session failed,wait,coordinator_info [%s]", to_cstring(ctx.get_coordinator_info()));
                      }
                      else if(tmplog_replay_finished)
                      {
                          if(OB_SUCCESS != (ret = fetch_trans_stat(ctx.get_coordinator_info(),find_trans_stat,commit)))
                          {
                              YYSYS_LOG(WARN, "fetch trans stat failed, err:%d", ret);
                          }
                          else if(!find_trans_stat)
                          {
                              YYSYS_LOG(WARN,"coordinator prepare_log [%s] not exist, rollback itself",to_cstring(ctx.get_coordinator_info()));
                              need_end_sub_trans = true;
                          }
                          else
                          {
                              YYSYS_LOG(INFO,"double fetch from sys success, trans stat:%d",commit);
                              need_end_sub_trans = true;
                          }
                      }
                      else
                      {
                          YYSYS_LOG(INFO,"coordinator master ups session tmplog replay not finished,wait,coordinator_info [%s]", to_cstring(ctx.get_coordinator_info()));
                      }
                  }

              }
              else if(coordinator_exist)
              {
                  YYSYS_LOG(INFO,"coordinator[%s] exist,wait",to_cstring(ctx.get_coordinator_info()));
              }
              else
              {
                  if(OB_SUCCESS != (ret = fetch_trans_stat(ctx.get_coordinator_info(),find_trans_stat,commit)))
                  {
                      YYSYS_LOG(WARN,"fetch trans stat failed, err:%d", ret);
                  }
                  else if(!find_trans_stat &&ctx.get_coordinator_info().descriptor_ != ObTransID::INVALID_SESSION_ID)
                  {
                      commit = false;
                      YYSYS_LOG(WARN, "coordinator[%s] not exist, write rollback to system table", to_cstring(ctx.get_coordinator_info()));
                      if (OB_SUCCESS != (ret = write_trans_stat(ctx, ctx.get_coordinator_info(),commit)))
                      {
                          YYSYS_LOG(WARN, "write trans stat:%d failed, ret=%d", commit, ret);
                      }
                      else
                      {
                          need_end_sub_trans = true;
                      }
                  }
                  else
                  {
                      YYSYS_LOG(INFO, "doule fetch from sys success, trans stat :%d", commit);
                      need_end_sub_trans = true;
                  }
              }

          }
          else
          {
              //commit/rollback self according system table record
              YYSYS_LOG(INFO, "fetch trans stat :%d", commit);
              need_end_sub_trans = true;
          }
      }

      if(OB_SUCCESS == ret && need_end_sub_trans && !ctx.get_writting_trans_stat()
              && ctx.get_coordinator_info().descriptor_ != ObTransID::INVALID_SESSION_ID)
      {
        ret = end_sub_trans(ctx, trans_id, commit);
      }
      return ret;
    }

    bool TransExecutor::handle_rollback_trans_(Task &task, ObDataBuffer &buffer)
    {
      //participant func
      //1��check prepare stat
      //2��if prepare, write rollback log
      //3��rollback session
      int &ret = thread_errno();
      ret = OB_SUCCESS;
      bool need_free_task = true;
      ObTransID trans_id;
      UNUSED(buffer);
      task.sid.reset();
      int64_t pos = task.pkt.get_buffer()->get_position();
      RWSessionCtx *ctx = NULL;
      bool need_write_log = false;
      Task* ms_request = NULL;
      if (!UPS.is_master_lease_valid())
      {
        ret = OB_NOT_MASTER;
        YYSYS_LOG(WARN, "ups master_lease is invalid, pcode=%d", task.pkt.get_packet_code());
      }
      else if (OB_SUCCESS != (ret = trans_id.deserialize(task.pkt.get_buffer()->get_data(),
                                                    task.pkt.get_buffer()->get_capacity(),
                                                    pos)))
      {
        YYSYS_LOG(WARN, "deserialize session_req fail ret=%d", ret);
      }
      else
      {
        SessionGuard session_guard(session_mgr_, lock_mgr_, ret, false); //mod [521]

        if (OB_SUCCESS != (ret = session_guard.fetch_session(trans_id, ctx)))
        {
          YYSYS_LOG(WARN, "fetch_session(%s)=>%d", to_cstring(trans_id), ret);
        }
        else
        {
          if(ctx->is_prepare() && !ctx->get_writting_trans_stat())
          {
            need_write_log = true;
            if(__sync_bool_compare_and_swap(ctx->get_writting_trans_stat_for_sync(), false, true))
            {
                //ctx->set_writting_trans_stat(true); [dduuhhtt]
            }
            else
            {
                ret = OB_ALREADY_WRITE_STAT_LOG;
                YYSYS_LOG(WARN, "already write stat log");
            }

          }
          if(NULL != ctx->get_task())
          {
            //coordinator & participant, need return ms and free task
            ms_request = static_cast<Task*>(ctx->get_task());
          }
        }
      }

      if(OB_SUCCESS != ret)
      {
        UPS.response_result(ret, task.pkt);
      }
      else if(need_write_log)
      {
        //have prepare log, need write rollback log
        task.sid = trans_id;
        if (OB_SUCCESS != TransCommitThread::push(&task))
        {
          YYSYS_LOG(ERROR, "rollback part trans error");
          ctx->set_writting_trans_stat(false);
        }
        else
        {
          need_free_task = false;
        }
      }
      else
      {
        //have no prepare log, no need write rollback log
        //coordinator & participant, need return ms and free task
        if (NULL != ms_request)
        {
          int64_t now = yysys::CTimeUtil::getTime();
          int64_t timeout = ms_request->pkt.get_source_timeout();
          if (0 < timeout && now > timeout + ms_request->pkt.get_receive_ts())
          {
            YYSYS_LOG(WARN, "already timeout, no need return");
          }
          else
          {
            UPS.response_result(OB_TRANS_ROLLBACKED,ms_request->pkt);
          }
          allocator_.free(ms_request);
          ctx->set_task(NULL);
        }
        if(ctx->get_can_be_killed()) //[521]
        {
            ret = session_mgr_.end_session(trans_id.descriptor_);
            //UPS.response_result(OB_TRANS_ROLLBACKED, task.pkt);
        }
      }

      return need_free_task;
    }

    bool TransExecutor::handle_commit_trans_(Task &task, ObDataBuffer &buffer)
    {
      //participant func

      //1��check prepare stat
      //2��push task to commit queue
      // this func should process tail of handle_end_session function
      int &ret = thread_errno();
      ret = OB_SUCCESS;
      bool need_free_task = true;
      ObTransID trans_id;
      UNUSED(buffer);
      task.sid.reset();
      int64_t pos = task.pkt.get_buffer()->get_position();
      RWSessionCtx *ctx = NULL;
      if (!UPS.is_master_lease_valid())
      {
        ret = OB_NOT_MASTER;
        YYSYS_LOG(WARN, "ups master_lease is invalid, pcode=%d", task.pkt.get_packet_code());
      }
      else if (OB_SUCCESS != (ret = trans_id.deserialize(task.pkt.get_buffer()->get_data(),
                                                    task.pkt.get_buffer()->get_capacity(),
                                                    pos)))
      {
        YYSYS_LOG(WARN, "deserialize session_req fail ret=%d", ret);
      }
      else
      {
        SessionGuard session_guard(session_mgr_, lock_mgr_, ret, false);
        if (OB_SUCCESS != (ret = session_guard.fetch_session(trans_id, ctx)))
        {
          YYSYS_LOG(ERROR, "fetch_session(%s)=>%d", to_cstring(trans_id), ret);
        }
        else if(!__sync_bool_compare_and_swap(ctx->get_writting_trans_stat_for_sync(), false, true))
        {
            ctx->set_can_be_killed(false);
            ret = OB_ALREADY_WRITE_STAT_LOG;
            YYSYS_LOG(WARN, "already write stat log");
        }
        else if(!ctx->is_prepare())
        {
          ret = OB_ERROR;
          YYSYS_LOG(ERROR, "can not commit before prepare");
        }
      }

      if (OB_SUCCESS != ret)
      {
          if(NULL != ctx)
          {
              if(ctx->get_can_be_killed())
              {
                  bool rollback = true;
                  ret = session_mgr_.end_session(trans_id.descriptor_, rollback);
              }
          }
        UPS.response_result(ret, task.pkt);
      }
      else
      {
        task.sid = trans_id;
        if (OB_SUCCESS != (ret = TransCommitThread::push(&task)))
        {
          YYSYS_LOG(ERROR, "commit part trans error");
//          session_mgr_.end_session(trans_id.descriptor_, true);
//          UPS.response_result(OB_TRANS_ROLLBACKED, task.pkt);
          ctx->set_writting_trans_stat(false);
        }
        else
        {
          need_free_task = false;
        }
      }

      return need_free_task;
    }

    bool TransExecutor::handle_check_session_exist(Task &task, common::ObDataBuffer &buffer)
    {
      bool need_free_task = true;
      //mod dyr [MultiUPS] [DISTRIBUTED_TRANS] 20150831:b
      //int ret = OB_SUCCESS;
      int deserialize_ret = OB_SUCCESS;
      int &ret = thread_errno();
      ret = OB_SUCCESS;
      //mod 20150831:e
      int64_t pos = task.pkt.get_buffer()->get_position();
      ObTransID trans_id;
      bool exist = false;

      //mod dyr [MultiUPS] [DISTRIBUTED_TRANS] 20150831:b
//      if(OB_SUCCESS != (ret = trans_id.deserialize(task.pkt.get_buffer()->get_data(),
//                                                    task.pkt.get_buffer()->get_capacity(),
//                                                    pos)))
//      {
//        YYSYS_LOG(WARN, "deserialize trans_id error, ret=%d", ret);
//      }
      if(OB_SUCCESS != (deserialize_ret = trans_id.deserialize(task.pkt.get_buffer()->get_data(),
                                                    task.pkt.get_buffer()->get_capacity(),
                                                    pos)))
      {
        YYSYS_LOG(WARN, "deserialize trans_id error, ret=%d", deserialize_ret);
      }
      //mod 20150831:e
      else
      {
        exist = check_session_exist(trans_id);
      }

      common::ObResultCode result_msg;
      //mod dyr [MultiUPS] [DISTRIBUTED_TRANS] 20150831:b
//      result_msg.result_code_ = ret;
      result_msg.result_code_ = deserialize_ret;
      //mod 20150831:e

      ret = result_msg.serialize(buffer.get_data(), buffer.get_capacity(), buffer.get_position());
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "serialize result msg error, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::encode_bool(buffer.get_data(), buffer.get_capacity(), buffer.get_position(), exist)))
      {
        YYSYS_LOG(WARN, "serialize session_exist error, ret :%d", ret);
      }
      else if (OB_SUCCESS != (ret = UPS.send_response(task.pkt.get_packet_code(), task.pkt.get_api_version(), buffer, task.pkt.get_request(), task.pkt.get_channel_id())))
      {
        YYSYS_LOG(WARN, "ups send response fail, ret :%d", ret);
      }

      return need_free_task;
    }

    //add 20150701:e
    bool TransExecutor::handle_phyplan_trans_(Task &task,
                                             ObUpsPhyOperatorFactory &phy_operator_factory,
                                             sql::ObPhysicalPlan &phy_plan,
                                             ObNewScanner &new_scanner,
                                             ModuleArena &allocator,
                                             ObDataBuffer& buffer)
    {
      reset_warning_strings_();
      bool need_free_task = true;
      int &ret = thread_errno();
      ret = OB_SUCCESS;
      int end_session_ret = OB_SUCCESS;
      SessionGuard session_guard(session_mgr_, lock_mgr_, end_session_ret);
      RWSessionCtx* session_ctx = NULL;
      int64_t packet_timewait = (0 == task.pkt.get_source_timeout()) ?
                                UPS.get_param().packet_max_wait_time :
                                task.pkt.get_source_timeout();
      int64_t process_timeout = packet_timewait - QUERY_TIMEOUT_RESERVE;
      int64_t pos = task.pkt.get_buffer()->get_position();
      bool with_sid = false;
      //task.sid.reset();  //add hongchen [LOCK_WAIT_TIMEOUT] 20161209
      bool give_up_lock = true;
      if (!UPS.is_master_lease_valid())
      {
        ret = OB_NOT_MASTER;
        YYSYS_LOG(WARN, "ups master_lease is invalid, pcode=%d", task.pkt.get_packet_code());
      }
      else if (OB_SUCCESS != (ret = phy_plan.deserialize_header(task.pkt.get_buffer()->get_data(),
                                                                task.pkt.get_buffer()->get_capacity(),
                                                                pos)))
      {
        YYSYS_LOG(WARN, "phy_plan.deseiralize_header ret=%d", ret);
      }
      else if ((with_sid = phy_plan.get_trans_id().is_valid()))
      {
        const bool check_session_expired = true;
        if (OB_SUCCESS != (ret = session_guard.fetch_session(phy_plan.get_trans_id(), session_ctx, check_session_expired)))
        {
          YYSYS_LOG(WARN, "Session has been killed, error %d, \'%s\'", ret, to_cstring(phy_plan.get_trans_id()));
        }
        else if (session_ctx->get_stmt_start_time() > task.pkt.get_receive_ts())
        {
          YYSYS_LOG(ERROR, "maybe an expired request, will skip it, last_stmt_start_time=%ld receive_ts=%ld",
                    session_ctx->get_stmt_start_time(), task.pkt.get_receive_ts());
          ret = (OB_SUCCESS == task.last_process_retcode) ? OB_STMT_EXPIRED : task.last_process_retcode;
        }
        else
        {
          CLEAR_TRACE_BUF(session_ctx->get_tlog_buffer());
          session_ctx->reset_stmt();
          task.sid = phy_plan.get_trans_id();
        }
        // add by maosy [Delete_Update_Function_isolation_RC] 20161228
//        if(OB_SUCCESS==ret && phy_plan.get_trans_id ().read_times_ == EMPTY_ROW_CLEAR)
//        {
//            session_ctx->set_start_time_for_batch (0);
//            ret = OB_ERR_BATCH_EMPTY_ROW ;
//        }
        // add e
      }
      //add hongchen [LOCK_WAIT_TIMEOUT] 20161209:b
      else if (0 != task.pkt.get_last_conflict_lock_time())
      {
          const bool check_session_expired = true;
          if (OB_SUCCESS != (ret = session_guard.fetch_session(task.sid, session_ctx, check_session_expired)))
          {
            YYSYS_LOG(WARN, "Session has been killed, error %d, \'%s\'", ret, to_cstring(task.sid));
          }
          else if (session_ctx->get_stmt_start_time() > task.pkt.get_receive_ts())
          {
            YYSYS_LOG(ERROR, "maybe an expired request, will skip it, last_stmt_start_time=%ld receive_ts=%ld",
                      session_ctx->get_stmt_start_time(), task.pkt.get_receive_ts());
            ret = (OB_SUCCESS == task.last_process_retcode) ? OB_STMT_EXPIRED : task.last_process_retcode;
          }
          else
          {
            CLEAR_TRACE_BUF(session_ctx->get_tlog_buffer());
            session_ctx->reset_stmt();
            //add hongchen [LOCK_WAIT_TIMEOUT] 20161227:b
            //fix bug:save transid for case start_new_trans
            session_ctx->get_ups_result().set_trans_id(task.sid);
            //add hongchen [LOCK_WAIT_TIMEOUT] 20161227:e
          }
      }
      //add hongchen [LOCK_WAIT_TIMEOUT] 20161209:e
      else
      {
        task.sid.reset();  //add hongchen [LOCK_WAIT_TIMEOUT] 20161209
        phy_plan.get_trans_req().start_time_ = task.pkt.get_receive_ts();
              //del by maosy [MultiUPS 1.0][Delete_Update_Function]20170523
        // add by maosy [Delete_Update_Function_isolation_RC] 20161228
//        if(OB_SUCCESS==ret && phy_plan.get_trans_id ().read_times_ == EMPTY_ROW_CLEAR)
//        {
//            session_ctx->set_start_time_for_batch (0);
//            ret = OB_ERR_BATCH_EMPTY_ROW ;
//        }
        // add e
        if (OB_SUCCESS != (ret = session_guard.start_session(phy_plan.get_trans_req(), task.sid, session_ctx)))
        {
          if (OB_BEGIN_TRANS_LOCKED == ret)
          {
            int tmp_ret = OB_SUCCESS;
            task.last_process_retcode = ret;
            if (OB_SUCCESS != (tmp_ret = TransHandlePool::push(&task,
                    task.pkt.get_req_sign(),
                    task.pkt.get_packet_priority())))
            {
              ret = tmp_ret;
            }
            else
            {
              need_free_task = false;
            }
          }
          else
          {
            YYSYS_LOG(ERROR, "start_session()=>%d", ret);
          }
        }
        else
        {
          if (phy_plan.get_start_trans())
          {
            session_ctx->get_ups_result().set_trans_id(task.sid);
          }
        }
      }
      if (OB_SUCCESS != ret)
      {}
      else
      {
        int64_t cur_time = yysys::CTimeUtil::getTime();
        //add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20151207:b
        uint64_t active_mem_table_version = OB_INVALID_ID;
        //add 20151207:e
        BEGIN_SAMPLE_SLOW_QUERY(session_ctx);
        LOG_SESSION("start_trans", session_ctx, task);
        OB_STAT_INC(UPDATESERVER, UPS_STAT_TRANS_WTIME, cur_time - task.pkt.get_receive_ts());
        session_ctx->set_conflict_session_id(task.last_conflict_session_id);
        session_ctx->set_last_proc_time(cur_time);

        session_ctx->set_start_handle_time(yysys::CTimeUtil::getTime());
        session_ctx->set_stmt_start_time(task.pkt.get_receive_ts());
        session_ctx->set_stmt_timeout(process_timeout);
        session_ctx->set_self_processor_index(TransHandlePool::get_thread_index() + TransHandlePool::get_thread_num());
        session_ctx->set_priority((PriorityPacketQueueThread::QueuePriority)task.pkt.get_packet_priority());
        //add hongchen [MEMVERSION_CHECK_FIX] 20170727:b
        //firstly, set cur active memversion whenever, process all case including select_for_update case
        session_ctx->get_ups_result().set_memtable_version(UPS.get_table_mgr().get_cur_major_version());
        //add hongchen [MEMVERSION_CHECK_FIX] 20170727:e
        // add by maosy [MultiUps 1.0] [secondary index optimize]20170719 b:
        //todo �����ܻ������⣬�Ժ�Ҫע���Ƿ�clear�������ɾ���
        session_ctx->clear_tevalues();
        // add by maosy e
        // add by maosy for[delete and update do not read self-query ]
        session_ctx->mark_stmt();
        //        if(!session_ctx->get_has_mark_stmt ())
        //        {
        //            YYSYS_LOG(DEBUG,"stmt begin ");
        //        session_ctx->mark_stmt();
        //            session_ctx->set_has_mark_stmt (true);
        //        }
        YYSYS_LOG(DEBUG,"stmt times ");
        // add e
        //add hongchen [LOCK_WAIT_TIMEOUT] 20161209:b
        if (0 == task.pkt.get_last_conflict_lock_time())
        {
          task.pkt.set_lock_wait_timeout(UPS.get_param().lock_wait_time);
        }
        //add hongchen [LOCK_WAIT_TIMEOUT] 20161209:e
        bool* mutex_flag_for_pkt_ptr = session_ctx->get_mutex_flag_for_pkt_ptr();
        while(!__sync_bool_compare_and_swap(mutex_flag_for_pkt_ptr,false,true))
        {
          if (TC_REACH_TIME_INTERVAL(1 * 1000 * 1000))
          {
            YYSYS_LOG(WARN, "too slow for set pkt_ptr for lock_timout_logic!");
          }
//add support for arm platform by wangd 202106:b
#if defined(__aarch64__)
          asm("yield");
#else
//add support for arm platform by wangd 202106:e
          asm("pause");
#endif //add support for arm platform by wangd 202106
        }
        session_ctx->set_pkt_ptr(&(task.pkt));  //add hongchen [LOCK_WAIT_TIMEOUT] 20161209
        sql::ObPhyOperator *main_op = NULL;
        phy_operator_factory.set_session_ctx(session_ctx);
        phy_operator_factory.set_table_mgr(&UPS.get_table_mgr());
        phy_plan.clear();
        allocator.reuse();
        phy_plan.set_allocator(&allocator);
        phy_plan.set_operator_factory(&phy_operator_factory);

        int64_t pos = task.pkt.get_buffer()->get_position();
        if (OB_SUCCESS != (ret = phy_plan.deserialize(task.pkt.get_buffer()->get_data(),
                                                      task.pkt.get_buffer()->get_capacity(),
                                                      pos)))
        {
          YYSYS_LOG(WARN, "deserialize phy_plan fail ret=%d", ret);
        }
        else
        {
          FILL_TRACE_BUF(session_ctx->get_tlog_buffer(), "phyplan allocator used=%ld total=%ld",
                        allocator.used(), allocator.total());
        }
        if (OB_SUCCESS != ret)
        {}
        //add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20151207:b
        /**
         * check the paln version berfore the query open,in MultiUPS,the frozen version
         * of the paln(inner plan) come from MS must equal the frozen version of UPS,there
         * maybe a major frozen during the inner_plan was sended to UPS
         */
        else if (OB_SUCCESS != (ret = UPS.get_table_mgr().get_active_memtable_version(active_mem_table_version)))
        {
          YYSYS_LOG(WARN,"get active mem table version failed,ret=%d",ret);
        }
        else if (!UPS.is_ups_usable(active_mem_table_version))
        {
          YYSYS_LOG(INFO, "current UPS has been offline");
          ret = OB_CURRENT_PAXOS_GROUP_OFFLINE;
        }
        //else if (OB_SUCCESS != (ret = phy_plan.check_version_range_validity(active_mem_table_version, phy_plan.get_curr_frozen_version())))
        else if(UPS.get_param().check_mem_version_flag
                && OB_SUCCESS != (ret = phy_plan.check_version_range_validity(active_mem_table_version,
                                                                              phy_plan.get_curr_frozen_version()))
                && partition_change_between_versions(active_mem_table_version, phy_plan.get_curr_frozen_version())) //add [449]
        {
          YYSYS_LOG(WARN, "paln versionRange not serial,ret=%d",ret);
        }
        else if (NULL == (main_op = phy_plan.get_main_query()))
        {
          YYSYS_LOG(WARN, "main query null pointer");
          ret = OB_ERR_UNEXPECTED;
        }
        else if (OB_SUCCESS != (ret = main_op->open())
                || OB_SUCCESS != (ret = fill_return_rows_(*main_op, new_scanner, session_ctx->get_ups_result())))
        {
          session_ctx->rollback_stmt();
//          session_ctx->set_has_mark_stmt (false);
          if ((OB_ERR_EXCLUSIVE_LOCK_CONFLICT == ret
                || OB_ERR_SHARED_LOCK_CONFLICT == ret)
              && !session_ctx->is_session_expired()
              && !session_ctx->is_stmt_expired())
          {
            int tmp_ret = OB_SUCCESS;
            uint32_t session_descriptor = session_ctx->get_session_descriptor();
            int64_t conflict_processor_index = 0;
            //del hongchen [LOCK_WAIT_TIMEOUT] 20161209:b
            /*
            if (!with_sid)
            {
              end_session_ret = ret;
            }
            */
            //del hongchen [LOCK_WAIT_TIMEOUT] 20161209:e
            task.last_conflict_session_id = session_ctx->get_conflict_session_id();
            __sync_bool_compare_and_swap(mutex_flag_for_pkt_ptr,true,false);
            session_ctx = NULL;
            session_guard.revert();
            task.last_process_retcode = ret;
            task.set_last_err_msg(ob_get_err_msg().ptr());
            if (OB_SUCCESS != (tmp_ret = TransHandlePool::push(&task,
                    conflict_processor_index,
                    task.pkt.get_packet_priority())))
            {
              YYSYS_LOG(WARN, "push back task fail, ret=%d conflict_processor_index=%ld task=%p",
                        tmp_ret, conflict_processor_index, &task);
              session_mgr_.end_session(session_descriptor, true);
            }
            else
            {
              give_up_lock = false;
              need_free_task = false;
            }
          }
          if (OB_ERR_PRIMARY_KEY_DUPLICATE != ret
              && give_up_lock)
          {
            OB_STAT_INC(UPDATESERVER, UPS_STAT_APPLY_FAIL_COUNT, 1);
            //YYSYS_LOG(WARN, "main_op open fail ret=%d", ret);
          }
          main_op->close();
        }
        else
        {
          session_ctx->commit_stmt();
          main_op->close();
          fill_warning_strings_(session_ctx->get_ups_result());
          YYSYS_LOG(DEBUG, "precommit end timeu=%ld", yysys::CTimeUtil::getTime() - task.pkt.get_receive_ts());
        }
        //add hongchen [LOCK_WAIT_TIMEOUT_PARALLEL_BUGFIX] 20170824:b
        if (NULL != session_ctx)
        {
          task.pkt.set_last_conflict_lock_time(0);
          session_ctx->set_pkt_ptr(NULL);
          __sync_bool_compare_and_swap(mutex_flag_for_pkt_ptr,true,false);
        }
        //add hongchen [LOCK_WAIT_TIMEOUT_PARALLEL_BUGFIX] 20170824:e

        if (OB_SUCCESS != ret)
        {
          if (phy_plan.get_start_trans()
              && NULL != session_ctx
              && session_ctx->get_ups_result().get_trans_id().is_valid()
              && give_up_lock)
          {
            session_ctx->get_ups_result().set_error_code(ret);
            ret = OB_SUCCESS;
            session_ctx->get_ups_result().serialize(buffer.get_data(), buffer.get_capacity(), buffer.get_position());
            const char *error_string = ob_get_err_msg().ptr();
            //mod hongchen [LOCK_WAIT_TIMEOUT] 20161209:b
            {
              if (__sync_bool_compare_and_swap(task.pkt.get_need_response(), true, false))
              {
                 UPS.response_buffer(ret, task.pkt, buffer, error_string);
              }
            }
            //UPS.response_buffer(ret, task.pkt, buffer, error_string);
            //mod hongchen [LOCK_WAIT_TIMEOUT] 20161209:e
          }
        }
        else if (with_sid || phy_plan.get_start_trans())
        {
          //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150720:b
          if(NULL != session_ctx->get_uc_info().host)
          {
            SSTableID sst_id = session_ctx->get_uc_info().host->get_version();
            session_ctx->get_ups_result().set_memtable_version(sst_id.major_version);
            YYSYS_LOG(DEBUG, "set major version:%ld", sst_id.major_version);
          }
          else
          {
            YYSYS_LOG(DEBUG, "have no mutator info, maybe select for update, can not set major version");
          }
          //add 20150720:e
          session_ctx->get_ups_result().serialize(buffer.get_data(), buffer.get_capacity(), buffer.get_position());
          //mod hongchen [LOCK_WAIT_TIMEOUT] 20161209:b
          {
            if (__sync_bool_compare_and_swap(task.pkt.get_need_response(), true, false))
            {
              UPS.response_buffer(ret, task.pkt, buffer);
            }
          }
          //UPS.response_buffer(ret, task.pkt, buffer);
          //mod hongchen [LOCK_WAIT_TIMEOUT] 20161209:e
          OB_STAT_INC(UPDATESERVER, get_stat_num(session_ctx->get_priority(), APPLY, COUNT), 1);
          OB_STAT_INC(UPDATESERVER, get_stat_num(session_ctx->get_priority(), APPLY, QTIME), session_ctx->get_start_handle_time() - session_ctx->get_stmt_start_time());
          OB_STAT_INC(UPDATESERVER, get_stat_num(session_ctx->get_priority(), APPLY, TIMEU), yysys::CTimeUtil::getTime() - session_ctx->get_start_handle_time());
          END_SAMPLE_SLOW_QUERY(session_ctx, task.pkt);
        }
        else
        {
          if (OB_SUCCESS != (ret = session_ctx->get_ups_mutator().get_mutator().pre_serialize()))
          {
            YYSYS_LOG(ERROR, "session_ctx.mutator.pre_serialize()=>%d", ret);
          }
          else
          {
            FILL_TRACE_BUF(session_ctx->get_tlog_buffer(), "ret=%d affected_rows=%ld", ret, session_ctx->get_ups_result().get_affected_rows());
            PRINT_TRACE_BUF(session_ctx->get_tlog_buffer());
            OB_STAT_INC(UPDATESERVER, get_stat_num(session_ctx->get_priority(), APPLY, COUNT), 1);
            OB_STAT_INC(UPDATESERVER, get_stat_num(session_ctx->get_priority(), APPLY, QTIME), session_ctx->get_start_handle_time() - session_ctx->get_stmt_start_time());
            OB_STAT_INC(UPDATESERVER, get_stat_num(session_ctx->get_priority(), APPLY, TIMEU), yysys::CTimeUtil::getTime() - session_ctx->get_start_handle_time());
            END_SAMPLE_SLOW_QUERY(session_ctx, task.pkt);

            cur_time = yysys::CTimeUtil::getTime();
            OB_STAT_INC(UPDATESERVER, UPS_STAT_TRANS_HTIME, cur_time - session_ctx->get_last_proc_time());
            session_ctx->set_last_proc_time(cur_time);

            session_ctx = NULL;
            session_guard.revert();
            if (OB_SUCCESS != (ret = TransCommitThread::push(&task)))
            {
              YYSYS_LOG(WARN, "commit thread queue is full, ret=%d", ret);
            }
            else
            {
              need_free_task = false;
            }
            if (OB_SUCCESS != ret && task.sid.is_valid())
            {
              session_mgr_.end_session(task.sid.descriptor_, true);
            }
          }
        }
        if (NULL != session_ctx)
        {
          FILL_TRACE_BUF(session_ctx->get_tlog_buffer(), "ret=%d affected_rows=%ld", ret, session_ctx->get_ups_result().get_affected_rows());
          PRINT_TRACE_BUF(session_ctx->get_tlog_buffer());
        }
        if (OB_SUCCESS != ret
            && ((!with_sid && !phy_plan.get_start_trans()) || !IS_SQL_ERR(ret))
            && give_up_lock)
        {
          end_session_ret = ret;
          YYSYS_LOG(DEBUG, "need rollback session %s ret=%d", to_cstring(task.sid), ret);
        }
      }
      phy_plan.clear();
      if (OB_SUCCESS != ret && OB_BEGIN_TRANS_LOCKED != ret && give_up_lock)
      {
        //ret = (OB_ERR_SHARED_LOCK_CONFLICT == ret) ? OB_EAGAIN : ret;
        const char *error_string = ob_get_err_msg().ptr();
        //mod hongchen [LOCK_WAIT_TIMEOUT] 20161209:b
        {
          if (__sync_bool_compare_and_swap(task.pkt.get_need_response(), true, false))
          {
            UPS.response_result(ret, error_string, task.pkt);
          }
        }
        //UPS.response_result(ret, error_string, task.pkt);
        //mod hongchen [LOCK_WAIT_TIMEOUT] 20161209:e
      }
      return need_free_task;
    }

    void TransExecutor::handle_get_trans_(ObPacket &pkt,
                                          ObGetParam &get_param,
                                          ObScanner &scanner,
                                          ObCellNewScanner &new_scanner,
                                          ObDataBuffer &buffer)
    {
      int &ret = thread_errno();
      ret = OB_SUCCESS;
      uint32_t session_descriptor = UINT32_MAX;
      ROSessionCtx *session_ctx = NULL;
      int64_t packet_timewait = (0 == pkt.get_source_timeout()) ?
                                UPS.get_param().packet_max_wait_time :
                                pkt.get_source_timeout();
      int64_t process_timeout = packet_timewait - QUERY_TIMEOUT_RESERVE;
      int64_t pos = pkt.get_buffer()->get_position();
      if (OB_SUCCESS != (ret = get_param.deserialize(pkt.get_buffer()->get_data(),
                                                    pkt.get_buffer()->get_capacity(),
                                                    pos)))
      {
        YYSYS_LOG(WARN, "deserialize get_param fail ret=%d", ret);
      }
      else if (!UPS.can_serve_read_req(get_param.get_is_read_consistency(), get_param.get_version_range().get_query_version()))
      {
        if (REACH_TIME_INTERVAL(10 * 1000 * 1000))
        {
          //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//          YYSYS_LOG(WARN, "the scan request require consistency, ObiRole:%s RoleMgr:%s, query_version=%ld",
//                    UPS.get_obi_role().get_role_str(), UPS.get_role_mgr().get_role_str(), get_param.get_version_range().get_query_version());
          YYSYS_LOG(WARN, "the scan request require consistency, RoleMgr:%s, query_version=%ld",
                    UPS.get_role_mgr().get_role_str(), get_param.get_version_range().get_query_version());
          //mod 20150701:e
        }
        ret = OB_NOT_MASTER;
      }
      else if (OB_SUCCESS != (ret = session_mgr_.begin_session(ST_READ_ONLY, pkt.get_receive_ts(), process_timeout, process_timeout, session_descriptor)))
      {
        YYSYS_LOG(WARN, "begin session fail ret=%d", ret);
      }
      else if (NULL == (session_ctx = session_mgr_.fetch_ctx<ROSessionCtx>(session_descriptor)))
      {
        YYSYS_LOG(WARN, "fetch ctx fail session_descriptor=%u", session_descriptor);
        ret = OB_ERR_UNEXPECTED;
      }
      else if (session_ctx->is_session_expired())
      {
        session_mgr_.revert_ctx(session_descriptor);
        session_ctx = NULL;
        ret = OB_TRANS_ROLLBACKED;
      }
      else
      {
        BEGIN_SAMPLE_SLOW_QUERY(session_ctx);
        FILL_TRACE_BUF(session_ctx->get_tlog_buffer(), "start handle get, packet wait=%ld start_time=%ld timeout=%ld src=%s",
                      yysys::CTimeUtil::getTime() - pkt.get_receive_ts(),
                      pkt.get_receive_ts(),
                      pkt.get_source_timeout(),
                      NULL == pkt.get_request() ? NULL : get_peer_ip(pkt.get_request()));
        thread_read_prepare();
        session_ctx->set_start_handle_time(yysys::CTimeUtil::getTime());
        session_ctx->set_stmt_start_time(pkt.get_receive_ts());
        session_ctx->set_stmt_timeout(process_timeout);
        session_ctx->set_priority((PriorityPacketQueueThread::QueuePriority)pkt.get_packet_priority());
        //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
        if (get_param.get_data_mark_param().is_valid())
        {
            YYSYS_LOG(DEBUG,"mul_del::debug,UPS get real data mark param[%s]!",
                      to_cstring(get_param.get_data_mark_param()));
        }
        //add duyr 20160531:e
        //add by maosy [MultiUps 1.0] [batch_iud_snapshot] 20170622 b:
        int64_t batch_snapshot_time = get_param.get_published_transid(UPS.get_param().paxos_id);
        if(batch_snapshot_time != OB_INVALID_DATA)
        {
             YYSYS_LOG(DEBUG,"transid = %ld",batch_snapshot_time);
            session_ctx->set_trans_id(batch_snapshot_time);
        }
        // add by e
        if (OB_NEW_GET_REQUEST == pkt.get_packet_code())
        {
          //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151202:b
         if (get_param.get_read_atomic_param().is_valid())
          YYSYS_LOG(DEBUG,"read_atomic::debug,UPS real read atomic param from CS get param!param=[%s]",
                    to_cstring(get_param.get_read_atomic_param()));
          //add duyr 20151202:e
          new_scanner.reuse();
          //add by maosy [MultiUps 1.0][secondary index optimize]20170401 b:
          ObUpdateServerMain *ups_main = NULL;
          if (NULL == (ups_main = ObUpdateServerMain::get_instance()))
          {
            YYSYS_LOG(WARN, "get updateserver main fail");
          }
          else
          {
            UpsSchemaMgrGuard sm_guard;
            const CommonSchemaManager *sm = NULL;
            const CommonTableSchema *tm = NULL;
            if (NULL != (sm = ups_main->get_update_server().get_table_mgr().get_schema_mgr().get_schema_mgr(sm_guard))
                && NULL != (tm = sm->get_table_schema(get_param[0]->table_id_)))
            {

                if(tm->get_index_helper().tbl_tid != OB_INVALID_ID)
                {
                    new_scanner.set_rowkey_info(tm->get_rowkey_info());
                }
            }
            //[546]
            else
            {
                CommonSchemaManagerWrapper csm;
                const CommonSchemaManager *smm = NULL;
                int err = OB_SUCCESS;
                int64_t query_version = 0;
                if((query_version = get_param.get_version_range().get_query_version()) <= 0)
                {
                    YYSYS_LOG(WARN, "empty version range to scan, version_range=%s",
                              to_cstring(get_param.get_version_range()));
                }
                else
                {
                    if(OB_SUCCESS != (err = ups_main->get_update_server().get_table_mgr().get_schema(query_version, csm)))
                    {
                        YYSYS_LOG(WARN, "get CommonSchemaManagerWrapper error, err=%d", err);
                    }
                    else if(NULL != (smm = csm.get_impl()) && NULL != (tm = smm->get_table_schema(get_param[0]->table_id_)))
                    {
                        if(tm->get_index_helper().tbl_tid != OB_INVALID_ID)
                        {
                            new_scanner.set_rowkey_info(tm->get_rowkey_info());
                        }
                    }
                }
            }
          }
          // add by maosy e
          common::ObRowDesc row_desc;
          if(OB_SUCCESS != (ret = ObNewScannerHelper::get_row_desc(get_param, true, row_desc)))
          {
            YYSYS_LOG(WARN, "get row desc fail:ret[%d]", ret);
          }
          else
          {
            new_scanner.set_row_desc(row_desc);
            ret = UPS.get_table_mgr().new_get(*session_ctx, get_param, new_scanner, pkt.get_receive_ts(), process_timeout);
          }
          if (OB_SUCCESS == ret)
          {
            UPS.response_scanner(ret, pkt, new_scanner, buffer);
          }
        }
        else
        {
          scanner.reset();
          ret = UPS.get_table_mgr().get(*session_ctx, get_param, scanner, pkt.get_receive_ts(), process_timeout);
          if (OB_SUCCESS == ret)
          {
            UPS.response_scanner(ret, pkt, scanner, buffer);
          }
        }
        FILL_TRACE_BUF(session_ctx->get_tlog_buffer(), "get from table mgr ret=%d", ret);
        //OB_STAT_INC(UPDATESERVER, UPS_STAT_NL_GET_COUNT, 1);
        OB_STAT_INC(UPDATESERVER, get_stat_num(session_ctx->get_priority(), GET, COUNT), 1);
        OB_STAT_INC(UPDATESERVER, UPS_STAT_GET_QTIME, session_ctx->get_start_handle_time() - session_ctx->get_session_start_time());
        //OB_STAT_INC(UPDATESERVER, UPS_STAT_NL_GET_TIMEU, session_ctx->get_session_timeu());
        OB_STAT_INC(UPDATESERVER, get_stat_num(session_ctx->get_priority(), GET, TIMEU), session_ctx->get_session_timeu());
        END_SAMPLE_SLOW_QUERY(session_ctx, pkt);
        thread_read_complete();
        session_ctx->set_last_active_time(yysys::CTimeUtil::getTime());
        session_mgr_.revert_ctx(session_descriptor);
        session_mgr_.end_session(session_descriptor);
        log_get_qps_();
      }
      if (OB_SUCCESS != ret)
      {
        UPS.response_result(ret, pkt);
      }
    }

    //add zhujun [transaction read uncommit] 2016/3/22
    void TransExecutor::handle_get_trans_in_one(Task &task,
                                          ObGetParam &get_param,
                                          ObScanner &scanner,
                                          ObCellNewScanner &new_scanner,
                                          ObDataBuffer &buffer)
    {
      //YYSYS_LOG(INFO, "TransExecutor::handle_get_trans_in_one");
      //add zhujun
      common::ObPacket pkt = task.pkt;
      int end_session_ret = OB_SUCCESS;
      SessionGuard session_guard(session_mgr_, lock_mgr_, end_session_ret);
      const bool check_session_expired = true;
      //add:e
      int &ret = thread_errno();
      ret = OB_SUCCESS;
      //add by maosy [MultiUPS 1.0] [read uncommit]20170525 b:
      uint32_t session_descriptor = get_param.get_trans_id(UPS.get_param().paxos_id).descriptor_;
      // add by maosy  e
      //ROSessionCtx *session_ctx = NULL;
      RWSessionCtx* session_ctx = NULL;
      int64_t packet_timewait = (0 == pkt.get_source_timeout()) ?
                                UPS.get_param().packet_max_wait_time :
                                pkt.get_source_timeout();
      int64_t process_timeout = packet_timewait - QUERY_TIMEOUT_RESERVE;
      int64_t pos = pkt.get_buffer()->get_position();
      if (OB_SUCCESS != (ret = get_param.deserialize(pkt.get_buffer()->get_data(),
                                                    pkt.get_buffer()->get_capacity(),
                                                    pos)))
      {
        YYSYS_LOG(WARN, "deserialize get_param fail ret=%d", ret);
      }
      else if (!UPS.can_serve_read_req(get_param.get_is_read_consistency(), get_param.get_version_range().get_query_version()))
      {
        if (REACH_TIME_INTERVAL(10 * 1000 * 1000))
        {
//          /*YYSYS_LOG(WARN, "the scan request require consistency, ObiRole:%s RoleMgr:%s, query_version=%ld",
//                    UPS.get_obi_role().get_role*/_str(), UPS.get_role_mgr().get_role_str(), get_param.get_version_range().get_query_version()); // uncertainty ups ���Ѿ�ɾ��
        }
        ret = OB_NOT_MASTER;
      }
      //add by maosy [MultiUPS 1.0] [read uncommit]20170525 b:
      //else if (OB_SUCCESS != (ret = session_guard.fetch_session(get_param.get_trans_id(), session_ctx, check_session_expired)))
      else if (OB_SUCCESS != (ret = session_guard.fetch_session(get_param.get_trans_id(UPS.get_param().paxos_id), session_ctx, check_session_expired)))
      {
          YYSYS_LOG(WARN, "Session has been killed, error %d, \'%s\'", ret, to_cstring(get_param.get_trans_id(UPS.get_param().paxos_id)));
          // add by maosy
      }
      else if (session_ctx->is_session_expired())
      {
        session_mgr_.revert_ctx(session_descriptor);
        session_ctx = NULL;
        ret = OB_TRANS_ROLLBACKED;
      }
      else
      {
        int64_t cur_time = yysys::CTimeUtil::getTime();
        BEGIN_SAMPLE_SLOW_QUERY(session_ctx);
        FILL_TRACE_BUF(session_ctx->get_tlog_buffer(), "start handle get, packet wait=%ld start_time=%ld timeout=%ld src=%s",
                      yysys::CTimeUtil::getTime() - pkt.get_receive_ts(),
                      pkt.get_receive_ts(),
                      pkt.get_source_timeout(),
                      NULL == pkt.get_request() ? NULL : get_peer_ip(pkt.get_request()));
        thread_read_prepare();
        //add zhujun
        session_ctx->set_conflict_session_id(task.last_conflict_session_id);
        session_ctx->set_last_proc_time(cur_time);
        //add:e
        //add hbt:read_uncomomit
        //������ʼʱ�̵��ѷ����汾�ż�¼����
        int64_t trans_start_trans_id=session_ctx->get_trans_id();
        //��select����ȡ��ǰʱ�����ύ������
        session_ctx->set_trans_id(session_mgr_.get_published_trans_id());
        //add:e
        session_ctx->set_start_handle_time(yysys::CTimeUtil::getTime());
        //del by maosy
        // add by maosy [Delete_Update_Function_isolation_RC] 20161222
        //        if(get_param.get_trans_id(UPS.get_param().paxos_id).read_times_ == FIRST_SELECT_READ )
        //        {
        //            if(session_ctx->get_start_time_for_batch () ==0)
        //            {
        //                session_ctx->set_start_time_for_batch (session_ctx->get_trans_id ());
        //            }
        //            else
        //            {
        //                session_ctx->set_new_transid ();
        //            }
        //            YYSYS_LOG(DEBUG,"start time = %ld,session = %p",session_ctx->get_start_time_for_batch (),session_ctx);
        //        }
        // add e
        session_ctx->set_stmt_start_time(pkt.get_receive_ts());
        session_ctx->set_stmt_timeout(process_timeout);
        session_ctx->set_priority((PriorityPacketQueueThread::QueuePriority)pkt.get_packet_priority());
        //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
        if (get_param.get_data_mark_param().is_valid())
        {
            YYSYS_LOG(DEBUG,"mul_del::debug,UPS get real data mark param[%s]!is in one!",
                      to_cstring(get_param.get_data_mark_param()));
        }
        //add duyr 20160531:e
        //add by maosy [MultiUps 1.0] [batch_iud_snapshot] 20170622 b:
        int64_t batch_snapshot_time = get_param.get_published_transid(UPS.get_param().paxos_id);
        if(batch_snapshot_time != OB_INVALID_DATA)
        {
             YYSYS_LOG(DEBUG,"transid = %ld",batch_snapshot_time);
            session_ctx->set_trans_id(batch_snapshot_time);
        }
        // add by e
        if (OB_NEW_GET_REQUEST == pkt.get_packet_code())
        {
          new_scanner.reuse();
          //add by maosy [MultiUps 1.0][secondary index optimize]20170401 b:
          ObUpdateServerMain *ups_main = NULL;
          if (NULL == (ups_main = ObUpdateServerMain::get_instance()))
          {
            YYSYS_LOG(WARN, "get updateserver main fail");
          }
          else
          {
            UpsSchemaMgrGuard sm_guard;
            const CommonSchemaManager *sm = NULL;
            const CommonTableSchema *tm = NULL;
            if (NULL != (sm = ups_main->get_update_server().get_table_mgr().get_schema_mgr().get_schema_mgr(sm_guard))
                && NULL != (tm = sm->get_table_schema(get_param[0]->table_id_)))
            {

                if(tm->get_index_helper().tbl_tid != OB_INVALID_ID)
                {
                    new_scanner.set_rowkey_info(tm->get_rowkey_info());
                }
            }
            else
            {
                CommonSchemaManagerWrapper csm;
                const CommonSchemaManager *smm = NULL;
                int err = OB_SUCCESS;
                int64_t query_version = 0;
                if((query_version = get_param.get_version_range().get_query_version()) <= 0)
                {
                    YYSYS_LOG(WARN, "empty version range to scan, version_range=%s",
                              to_cstring(get_param.get_version_range()));
                }
                else
                {
                    if(OB_SUCCESS != (err = ups_main->get_update_server().get_table_mgr().get_schema(query_version, csm)))
                    {
                        YYSYS_LOG(WARN, "get CommonSchemaManagerWrapper error, err=%d", err);
                    }
                    else if(NULL != (smm = csm.get_impl()) && NULL != (tm = smm->get_table_schema(get_param[0]->table_id_)))
                    {
                        if(tm->get_index_helper().tbl_tid != OB_INVALID_ID)
                        {
                            new_scanner.set_rowkey_info(tm->get_rowkey_info());
                        }
                    }
                }
            }
          }
          // add e
          common::ObRowDesc row_desc;
          if(OB_SUCCESS != (ret = ObNewScannerHelper::get_row_desc(get_param, true, row_desc)))
          {
            YYSYS_LOG(WARN, "get row desc fail:ret[%d]", ret);
          }
          else
          {
            new_scanner.set_row_desc(row_desc);
            ret = UPS.get_table_mgr().new_get(*session_ctx, get_param, new_scanner, pkt.get_receive_ts(), process_timeout);
          }
          if (OB_SUCCESS == ret)
          {
            UPS.response_scanner(ret, pkt, new_scanner, buffer);
          }
        }
        else
        {
          scanner.reset();
          ret = UPS.get_table_mgr().get(*session_ctx, get_param, scanner, pkt.get_receive_ts(), process_timeout);
          if (OB_SUCCESS == ret)
          {
            UPS.response_scanner(ret, pkt, scanner, buffer);
          }
        }
        FILL_TRACE_BUF(session_ctx->get_tlog_buffer(), "get from table mgr ret=%d", ret);
        //OB_STAT_INC(UPDATESERVER, UPS_STAT_NL_GET_COUNT, 1);
        OB_STAT_INC(UPDATESERVER, get_stat_num(session_ctx->get_priority(), GET, COUNT), 1);
        OB_STAT_INC(UPDATESERVER, UPS_STAT_GET_QTIME, session_ctx->get_start_handle_time() - session_ctx->get_session_start_time());
        //OB_STAT_INC(UPDATESERVER, UPS_STAT_NL_GET_TIMEU, session_ctx->get_session_timeu());
        OB_STAT_INC(UPDATESERVER, get_stat_num(session_ctx->get_priority(), GET, TIMEU), session_ctx->get_session_timeu());
        END_SAMPLE_SLOW_QUERY(session_ctx, pkt);
        thread_read_complete();
        session_ctx->set_last_active_time(yysys::CTimeUtil::getTime());
        //add:hbt:read_uncommit
        //������ʼʱ�̵��ѷ����汾�ŷŻص�ctx��
        session_ctx->set_trans_id(trans_start_trans_id);
        session_guard.revert();
        //add:e
        //delete zhujun
        //session_mgr_.end_session(session_descriptor,false,true);
        log_get_qps_();
      }
      if (OB_SUCCESS != ret)
      {
        UPS.response_result(ret, pkt);
      }
    }



    void TransExecutor::handle_scan_trans_(ObPacket &pkt,
                                          ObScanParam &scan_param,
                                          ObScanner &scanner,
                                          ObCellNewScanner &new_scanner,
                                          ObDataBuffer &buffer)
    {
      int &ret = thread_errno();
      ret = OB_SUCCESS;
      uint32_t session_descriptor = UINT32_MAX;
      ROSessionCtx *session_ctx = NULL;
      int64_t packet_timewait = (0 == pkt.get_source_timeout()) ?
                                UPS.get_param().packet_max_wait_time :
                                pkt.get_source_timeout();
      int64_t process_timeout = packet_timewait - QUERY_TIMEOUT_RESERVE;
      int64_t pos = pkt.get_buffer()->get_position();
      if (OB_SUCCESS != (ret = scan_param.deserialize(pkt.get_buffer()->get_data(),
                                                      pkt.get_buffer()->get_capacity(),
                                                      pos)))
      {
        YYSYS_LOG(WARN, "deserialize get_param fail ret=%d", ret);
      }
      else if (!UPS.can_serve_read_req(scan_param.get_is_read_consistency(), scan_param.get_version_range().get_query_version()))
      {
        if (REACH_TIME_INTERVAL(10 * 1000 * 1000))
        {
          //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//          YYSYS_LOG(WARN, "the scan request require consistency, ObiRole:%s RoleMgr:%s, query_version=%ld",
//                    UPS.get_obi_role().get_role_str(), UPS.get_role_mgr().get_role_str(), scan_param.get_version_range().get_query_version());
          YYSYS_LOG(WARN, "the scan request require consistency, RoleMgr:%s, query_version=%ld",
                    UPS.get_role_mgr().get_role_str(), scan_param.get_version_range().get_query_version());
          //mod 20150701:e
        }
        ret = OB_NOT_MASTER;
      }
      else if (OB_SUCCESS != (ret = session_mgr_.begin_session(ST_READ_ONLY, pkt.get_receive_ts(), process_timeout, process_timeout, session_descriptor)))
      {
        YYSYS_LOG(WARN, "begin session fail ret=%d", ret);
      }
      else if (NULL == (session_ctx = session_mgr_.fetch_ctx<ROSessionCtx>(session_descriptor)))
      {
        YYSYS_LOG(WARN, "fetch ctx fail session_descriptor=%u", session_descriptor);
        ret = OB_ERR_UNEXPECTED;
      }
      else if (session_ctx->is_session_expired())
      {
        session_mgr_.revert_ctx(session_descriptor);
        session_ctx = NULL;
        ret = OB_TRANS_ROLLBACKED;
      }
      else
      {
        BEGIN_SAMPLE_SLOW_QUERY(session_ctx);
        FILL_TRACE_BUF(session_ctx->get_tlog_buffer(), "start handle scan, packet wait=%ld start_time=%ld timeout=%ld src=%s",
                      yysys::CTimeUtil::getTime() - pkt.get_receive_ts(),
                      pkt.get_receive_ts(),
                      pkt.get_source_timeout(),
                      NULL == pkt.get_request() ? NULL : get_peer_ip(pkt.get_request()));
        thread_read_prepare();
        session_ctx->set_start_handle_time(yysys::CTimeUtil::getTime());
        session_ctx->set_stmt_start_time(pkt.get_receive_ts());
        session_ctx->set_stmt_timeout(process_timeout);
        session_ctx->set_priority((PriorityPacketQueueThread::QueuePriority)pkt.get_packet_priority());
        //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
        if (scan_param.get_data_mark_param().is_valid())
        {
            YYSYS_LOG(DEBUG,"mul_del::debug,UPS scan real data mark param[%s]!",
                      to_cstring(scan_param.get_data_mark_param()));
        }
        //add duyr 20160531:e
        //add by maosy [MultiUps 1.0] [batch_iud_snapshot] 20170622 b:
        int64_t batch_snapshot_time = scan_param.get_published_transid(UPS.get_param().paxos_id);
        if(batch_snapshot_time != OB_INVALID_DATA)
        {
             YYSYS_LOG(DEBUG,"transid = %ld",batch_snapshot_time);
            session_ctx->set_trans_id(batch_snapshot_time);
        }
        // add by e
        // add by maosy [MultiUps 1.0] [secondary index optimize]20170401 b:
        bool is_index = false ;
        bool is_index_has_storing = false;
        ObUpdateServerMain *ups_main = NULL;
        ObRowkeyInfo rowkey_info;
        if (NULL == (ups_main = ObUpdateServerMain::get_instance()))
        {
          ret = OB_ERR_UNEXPECTED;
          YYSYS_LOG(WARN, "get updateserver main fail, ret=%d", ret);
        }
        else
        {
          UpsSchemaMgrGuard sm_guard;
          const CommonSchemaManager *sm = NULL;
          const CommonTableSchema   *tm = NULL;
          if (NULL != (sm = ups_main->get_update_server().get_table_mgr().get_schema_mgr().get_schema_mgr(sm_guard))
              && NULL != (tm = sm->get_table_schema(scan_param.get_table_id())))
          {
              if(tm->get_index_helper().tbl_tid != OB_INVALID_ID)
              {
                is_index = true;
                rowkey_info = tm->get_rowkey_info();
                is_index_has_storing = sm->is_index_has_storing(scan_param.get_table_id());
              }
          }
          else
          {
              //add [546]
              CommonSchemaManagerWrapper csm;
              const CommonSchemaManager *smm = NULL;
              int64_t query_version = 0;
              if((query_version = scan_param.get_version_range().get_query_version()) <= 0)
              {
                  YYSYS_LOG(WARN, "empty version range to scan, version_range=%s",
                            to_cstring(scan_param.get_version_range()));
                  ret = OB_ERROR;
              }
              else
              {
                  if(OB_SUCCESS != (ret = ups_main->get_update_server().get_table_mgr().get_schema(query_version, csm)))
                  {
                      YYSYS_LOG(WARN, "get CommonSchemaManagerWrapper error, ret=%d", ret);
                  }
                  else if(NULL != (smm = csm.get_impl()) && NULL != (tm = smm->get_table_schema(scan_param.get_table_id())))
                  {
                      if(tm->get_index_helper().tbl_tid != OB_INVALID_ID)
                      {
                          is_index = true;
                          rowkey_info = tm->get_rowkey_info();
                          is_index_has_storing = sm->is_index_has_storing(scan_param.get_table_id());
                      }
                  }
                  else
                  {
                      ret = OB_ERR_UNEXPECTED;
                  }
              }
              if(ret != OB_SUCCESS)
              {
                  ret = OB_ERR_UNEXPECTED;
                  YYSYS_LOG(WARN, "get table[%lu] schema error, ret=%d", scan_param.get_table_id(), ret);
              }

          }
        }
        // add by maosy e
        if (OB_SUCCESS != ret)
        {
          //nothing todo
        }
        else if (OB_NEW_SCAN_REQUEST == pkt.get_packet_code())
        {
          //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151202:b
          const ObReadAtomicParam& tmp_param = scan_param.get_read_atomic_param();
          if (tmp_param.is_valid())
             YYSYS_LOG(DEBUG,"read_atomic::debug,UPS real read atomic param from CS scan param!param=[%s]",
                    to_cstring(tmp_param));
          //add duyr 20151202:e
          new_scanner.reuse();
          //add by maosy[MultiUps 1.0] [secondary index optimize]20170401 b:
          if(is_index)
          {
            new_scanner.set_rowkey_info(rowkey_info);
          }
          // add e
          common::ObRowDesc row_desc;
          common::ObRowDesc final_row_desc;
          if(OB_SUCCESS != (ret = ObNewScannerHelper::get_row_desc(scan_param, final_row_desc)))
          {
            YYSYS_LOG(WARN, "get row desc fail:ret[%d]", ret);
          }
          else
          {
            //new_scanner.set_row_desc(row_desc);
            if (is_index)
            {
              bool need_final_row_desc = false;
              for(int64_t index = 0 ; index < rowkey_info.get_size();index++ )
              {
                uint64_t column_id = OB_INVALID_ID;
                rowkey_info.get_column_id(index,column_id);
                ret=scan_param.add_column_if_notexists(column_id);
                if (OB_SUCCESS == ret)
                {
                  need_final_row_desc = true;
                }
                else if (OB_ENTRY_EXIST == ret)
                {
                  ret = OB_SUCCESS;
                }
                else
                {
                  YYSYS_LOG(WARN, "fail to add column to scan_param, ret=%d", ret);
                  break;
                }
              }
              if (OB_SUCCESS != ret)
              {
                //nothing todo
              }
              else if (!need_final_row_desc)
              {
                new_scanner.set_row_desc(final_row_desc);
              }
              else if (OB_SUCCESS != (ret = ObNewScannerHelper::get_row_desc(scan_param, row_desc)))
              {
                YYSYS_LOG(WARN, "get row desc fail:ret[%d]", ret);
              }
              else
              {
                new_scanner.set_final_row_desc(&final_row_desc);
                new_scanner.set_row_desc(row_desc);
              }
            }
            else
            {
              new_scanner.set_row_desc(final_row_desc);
            }
            if (OB_SUCCESS == ret)
            {
              ret = UPS.get_table_mgr().new_scan(*session_ctx, scan_param, new_scanner, pkt.get_receive_ts(), process_timeout);
            }
          }
#if 0
          if (OB_SUCCESS == ret)
          {
            ObUpsRow tmp_ups_row;
            tmp_ups_row.set_row_desc(row_desc);
            if (OB_SUCCESS != (ret = ObNewScannerHelper::print_new_scanner(new_scanner, tmp_ups_row, true)))
            {
              YYSYS_LOG(WARN, "print new scanner fail:ret[%d]", ret);
            }
          }
#endif
          if (OB_SUCCESS == ret)
          {
            UPS.response_scanner(ret, pkt, new_scanner, buffer);
          }
        }
        else
        {
          scanner.reset();
          //add by maosy[MultiUps 1.0] [secondary index optimize]20170605 b:
          if(is_index)
          {
            //scanner.set_need_check();
            if(is_index_has_storing && (1 == scan_param.get_column_id_size()))
            {
              //û�������е����
              scanner.set_index_has_storing();
            }
            scanner.set_rowkey_info(rowkey_info);
            for(int64_t index = 0 ; index < rowkey_info.get_size();index++ )
            {
              uint64_t column_id = OB_INVALID_ID;
              rowkey_info.get_column_id(index,column_id);
              ret =  scan_param.add_column_if_notexists(column_id);
              if(ret == OB_ENTRY_EXIST)
              {
                ret = OB_SUCCESS ;
              }
              else if (OB_SUCCESS == ret)
              {
                scanner.set_add_rowkey(column_id);
              }
              else
              {
                YYSYS_LOG(WARN,"fail to add column to scan_param, ret=%d", ret);
                break;
              }
            }
          }
          // add e
          if (OB_SUCCESS == ret)
          {
            ret = UPS.get_table_mgr().scan(*session_ctx, scan_param, scanner, pkt.get_receive_ts(), process_timeout);
          }
          if (OB_SUCCESS == ret)
          {
            UPS.response_scanner(ret, pkt, scanner, buffer);
          }
        }
        FILL_TRACE_BUF(session_ctx->get_tlog_buffer(), "get from table mgr ret=%d", ret);
        //OB_STAT_INC(UPDATESERVER, UPS_STAT_NL_SCAN_COUNT, 1);
        OB_STAT_INC(UPDATESERVER, get_stat_num(session_ctx->get_priority(), SCAN, COUNT), 1);
        OB_STAT_INC(UPDATESERVER, UPS_STAT_SCAN_QTIME, session_ctx->get_start_handle_time() - session_ctx->get_session_start_time());
        //OB_STAT_INC(UPDATESERVER, UPS_STAT_NL_SCAN_TIMEU, session_ctx->get_session_timeu());
        OB_STAT_INC(UPDATESERVER, get_stat_num(session_ctx->get_priority(), SCAN, TIMEU), session_ctx->get_session_timeu());
        END_SAMPLE_SLOW_QUERY(session_ctx, pkt);
        thread_read_complete();
        session_ctx->set_last_active_time(yysys::CTimeUtil::getTime());
        session_mgr_.revert_ctx(session_descriptor);
        session_mgr_.end_session(session_descriptor);
        log_scan_qps_();
      }
      if (OB_SUCCESS != ret)
      {
        UPS.response_result(ret, pkt);
      }
    }

    //add zhujun [transaction read uncommit] 2016/3/29
    void TransExecutor::handle_scan_trans_in_one(Task &task,
                            common::ObScanParam &scan_param,
                            common::ObScanner &scanner,
                            common::ObCellNewScanner &new_scanner,
                            common::ObDataBuffer &buffer)
    {
        //YYSYS_LOG(INFO, "TransExecutor::handle_scan_trans_in_one transaction id = %s",to_cstring(scan_param.get_trans_id()));
        //add zhujun
        common::ObPacket pkt = task.pkt;
        int end_session_ret = OB_SUCCESS;
        SessionGuard session_guard(session_mgr_, lock_mgr_, end_session_ret);
        const bool check_session_expired = true;
        //add:e
        int &ret = thread_errno();
        ret = OB_SUCCESS;
        //add by maosy [MultiUPS 1.0] [read uncommit]20170525 b:
        uint32_t session_descriptor = scan_param.get_trans_id(UPS.get_param().paxos_id).descriptor_;
        // add by maosy e
        RWSessionCtx* session_ctx = NULL;
//        ROSessionCtx *session_ctx = NULL;
        int64_t packet_timewait = (0 == pkt.get_source_timeout()) ?
                                  UPS.get_param().packet_max_wait_time :
                                  pkt.get_source_timeout();
        int64_t process_timeout = packet_timewait - QUERY_TIMEOUT_RESERVE;
        int64_t pos = pkt.get_buffer()->get_position();
        if(OB_SUCCESS != ret )
        {}
        else if (OB_SUCCESS != (ret = scan_param.deserialize(pkt.get_buffer()->get_data(),
                                                        pkt.get_buffer()->get_capacity(),
                                                        pos)))
        {
          YYSYS_LOG(WARN, "deserialize get_param fail ret=%d", ret);
        }
        else if (!UPS.can_serve_read_req(scan_param.get_is_read_consistency(), scan_param.get_version_range().get_query_version()))
        {
          if (REACH_TIME_INTERVAL(10 * 1000 * 1000))
          {
//            YYSYS_LOG(WARN, "the scan request require consistency, ObiRole:%s RoleMgr:%s, query_version=%ld",
//                      UPS.get_obi_role().get_role_str(), UPS.get_role_mgr().get_role_str(), scan_param.get_version_range().get_query_version());   // uncertainty ups  ��ɾ��
          }
          ret = OB_NOT_MASTER;
        }
        /*
        else if (OB_SUCCESS != (ret = session_mgr_.begin_session(ST_READ_ONLY, pkt.get_receive_ts(), process_timeout, process_timeout, session_descriptor)))
        {
          YYSYS_LOG(WARN, "begin session fail ret=%d", ret);
        }
        else if (NULL == (session_ctx = session_mgr_.fetch_ctx<ROSessionCtx>(session_descriptor)))
        {
          YYSYS_LOG(WARN, "fetch ctx fail session_descriptor=%u", session_descriptor);
          ret = OB_ERR_UNEXPECTED;
        }*/
        //add by maosy [MultiUPS 1.0] [read uncommit]20170525 b:
        //else if (OB_SUCCESS != (ret = session_guard.fetch_session(scan_param.get_trans_id(), session_ctx, check_session_expired)))
        else if (OB_SUCCESS != (ret = session_guard.fetch_session(scan_param.get_trans_id(UPS.get_param().paxos_id), session_ctx, check_session_expired)))
        {
            YYSYS_LOG(WARN, "Session has been killed, error %d, \'%s\'", ret, to_cstring(scan_param.get_trans_id(UPS.get_param().paxos_id)));
            // add by maosy e
        }
        else if (session_ctx->is_session_expired())
        {
          session_mgr_.revert_ctx(session_descriptor);
          session_ctx = NULL;
          ret = OB_TRANS_ROLLBACKED;
        }
        else
        {
          int64_t cur_time = yysys::CTimeUtil::getTime();
          BEGIN_SAMPLE_SLOW_QUERY(session_ctx);
          FILL_TRACE_BUF(session_ctx->get_tlog_buffer(), "start handle scan, packet wait=%ld start_time=%ld timeout=%ld src=%s",
                        yysys::CTimeUtil::getTime() - pkt.get_receive_ts(),
                        pkt.get_receive_ts(),
                        pkt.get_source_timeout(),
                        NULL == pkt.get_request() ? NULL : get_peer_ip(pkt.get_request()));
          thread_read_prepare();
          //add zhujun
          session_ctx->set_conflict_session_id(task.last_conflict_session_id);
          session_ctx->set_last_proc_time(cur_time);
          //add:e
          //add hbt:read_uncomomit
          //������ʼʱ�̵��ѷ����汾�ż�¼����
          int64_t trans_start_trans_id=session_ctx->get_trans_id();
          //��select����ȡ��ǰʱ�����ύ������
          session_ctx->set_trans_id(session_mgr_.get_published_trans_id());
          //add:e
          session_ctx->set_start_handle_time(yysys::CTimeUtil::getTime());
          // del by maosy
          // add by maosy [Delete_Update_Function_isolation_RC] 20161222
//          if(scan_param.get_trans_id (UPS.get_param().paxos_id).read_times_ == FIRST_SELECT_READ )
//          {
//              if(session_ctx->get_start_time_for_batch () ==0)
//              {
//                  session_ctx->set_start_time_for_batch (session_ctx->get_trans_id ());
//              }
//              else
//              {
//                  session_ctx->set_new_transid ();
//              }
//              YYSYS_LOG(DEBUG,"start time = %ld,session = %p",session_ctx->get_start_time_for_batch (),session_ctx);
//          }
          // add e
          session_ctx->set_stmt_start_time(pkt.get_receive_ts());
          session_ctx->set_stmt_timeout(process_timeout);
          session_ctx->set_priority((PriorityPacketQueueThread::QueuePriority)pkt.get_packet_priority());
          //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
          if (scan_param.get_data_mark_param().is_valid())
          {
              YYSYS_LOG(DEBUG,"mul_del::debug,UPS scan real data mark param[%s]!is in one",
                        to_cstring(scan_param.get_data_mark_param()));
          }
          //add duyr 20160531:e
          //add by maosy [MultiUps 1.0] [batch_iud_snapshot] 20170622 b:
          int64_t batch_snapshot_time = scan_param.get_published_transid(UPS.get_param().paxos_id);
          if(batch_snapshot_time != OB_INVALID_DATA)
          {
               YYSYS_LOG(DEBUG,"transid = %ld",batch_snapshot_time);
              session_ctx->set_trans_id(batch_snapshot_time);
          }
          // add by e
          if (OB_NEW_SCAN_REQUEST == pkt.get_packet_code())
          {
            bool is_index = false;
            ObRowkeyInfo rowkey_info;
            new_scanner.reuse();

            //add by maosy [MultiUps 1.0][secondary index optimize]20170401 b:
            ObUpdateServerMain *ups_main = NULL;
            if (NULL == (ups_main = ObUpdateServerMain::get_instance()))
            {
              ret = OB_ERR_UNEXPECTED;
              YYSYS_LOG(WARN, "get updateserver main fail, ret=%d", ret);
            }
            else
            {
              UpsSchemaMgrGuard sm_guard;
              const CommonSchemaManager *sm = NULL;
              const CommonTableSchema   *tm = NULL;
              if (NULL != (sm = ups_main->get_update_server().get_table_mgr().get_schema_mgr().get_schema_mgr(sm_guard))
                  && NULL != (tm = sm->get_table_schema(scan_param.get_table_id())))
              {
                if(tm->get_index_helper().tbl_tid != OB_INVALID_ID)
                {
                  new_scanner.set_rowkey_info(tm->get_rowkey_info());
                  is_index = true;
                  rowkey_info = tm->get_rowkey_info();
                }
              }
              else
              {
                  //add [546]
                  CommonSchemaManagerWrapper csm;
                  const CommonSchemaManager *smm = NULL;
                  int64_t query_version = 0;
                  if((query_version = scan_param.get_version_range().get_query_version()) <= 0)
                  {
                      YYSYS_LOG(WARN, "empty version range to scan, version_range=%s",
                                to_cstring(scan_param.get_version_range()));
                      ret = OB_ERROR;
                  }
                  else
                  {
                      if(OB_SUCCESS != (ret = ups_main->get_update_server().get_table_mgr().get_schema(query_version, csm)))
                      {
                          YYSYS_LOG(WARN, "get CommonSchemaManagerWrapper error, ret=%d", ret);
                      }
                      else if(NULL != (smm = csm.get_impl()) && NULL != (tm = smm->get_table_schema(scan_param.get_table_id())))
                      {
                          if(tm->get_index_helper().tbl_tid != OB_INVALID_ID)
                          {
                              new_scanner.set_rowkey_info(tm->get_rowkey_info());
                              is_index = true;
                              rowkey_info = tm->get_rowkey_info();
                          }
                      }
                      else
                      {
                          ret = OB_ERR_UNEXPECTED;
                      }
                  }
                  if(ret != OB_SUCCESS)
                  {
                      ret = OB_ERR_UNEXPECTED;
                      YYSYS_LOG(WARN, "get table[%lu] schema error, ret=%d", scan_param.get_table_id(), ret);
                  }
              }
            }
            // add e
            common::ObRowDesc row_desc;
            common::ObRowDesc final_row_desc;
            if (OB_SUCCESS != ret)
            {
              //nothing todo
            }
            else if(OB_SUCCESS != (ret = ObNewScannerHelper::get_row_desc(scan_param, final_row_desc)))
            {
              YYSYS_LOG(WARN, "get row desc fail:ret[%d]", ret);
            }
            else
            {
              //new_scanner.set_row_desc(row_desc);
              if (is_index)
              {
                bool need_final_row_desc = false;
                for(int64_t index = 0 ; index < rowkey_info.get_size();index++ )
                {
                  uint64_t column_id = OB_INVALID_ID;
                  rowkey_info.get_column_id(index,column_id);
                  ret = scan_param.add_column_if_notexists(column_id);
                  if (OB_SUCCESS == ret)
                  {
                    need_final_row_desc = true;
                  }
                  else if (OB_ENTRY_EXIST == ret)
                  {
                    ret = OB_SUCCESS;
                  }
                  else
                  {
                    YYSYS_LOG(WARN, "fail to add column to scan_param, ret=%d", ret);
                    break;
                  }
                }
                if (OB_SUCCESS != ret)
                {
                  //nothing todo
                }
                else if (!need_final_row_desc)
                {
                  new_scanner.set_row_desc(final_row_desc);
                }
                else if (OB_SUCCESS != (ret = ObNewScannerHelper::get_row_desc(scan_param, row_desc)))
                {
                  YYSYS_LOG(WARN, "get row desc fail:ret[%d]", ret);
                }
                else
                {
                  new_scanner.set_final_row_desc(&final_row_desc);
                  new_scanner.set_row_desc(row_desc);
                }
              }
              else
              {
                new_scanner.set_row_desc(final_row_desc);
              }
              if (OB_SUCCESS == ret)
              {
                ret = UPS.get_table_mgr().new_scan(*session_ctx, scan_param, new_scanner, pkt.get_receive_ts(), process_timeout);
              }
            }
            if (OB_SUCCESS == ret)
            {
              UPS.response_scanner(ret, pkt, new_scanner, buffer);
            }
          }
          else
          {
            scanner.reset();
            ret = UPS.get_table_mgr().scan(*session_ctx, scan_param, scanner, pkt.get_receive_ts(), process_timeout);
            if (OB_SUCCESS == ret)
            {
              UPS.response_scanner(ret, pkt, scanner, buffer);
            }
          }
          FILL_TRACE_BUF(session_ctx->get_tlog_buffer(), "get from table mgr ret=%d", ret);
          //OB_STAT_INC(UPDATESERVER, UPS_STAT_NL_SCAN_COUNT, 1);
          OB_STAT_INC(UPDATESERVER, get_stat_num(session_ctx->get_priority(), SCAN, COUNT), 1);
          OB_STAT_INC(UPDATESERVER, UPS_STAT_SCAN_QTIME, session_ctx->get_start_handle_time() - session_ctx->get_session_start_time());
          //OB_STAT_INC(UPDATESERVER, UPS_STAT_NL_SCAN_TIMEU, session_ctx->get_session_timeu());
          OB_STAT_INC(UPDATESERVER, get_stat_num(session_ctx->get_priority(), SCAN, TIMEU), session_ctx->get_session_timeu());
          END_SAMPLE_SLOW_QUERY(session_ctx, pkt);
          thread_read_complete();
          session_ctx->set_last_active_time(yysys::CTimeUtil::getTime());
          //add:hbt:read_uncommit
          //������ʼʱ�̵��ѷ����汾�ŷŻص�ctx��
          session_ctx->set_trans_id(trans_start_trans_id);
          session_guard.revert();
          //add:e
          //delete zhujun
          //session_mgr_.end_session(session_descriptor);
          log_scan_qps_();
        }
        if (OB_SUCCESS != ret)
        {
          UPS.response_result(ret, pkt);
        }

    }

    void TransExecutor::handle_kill_zombie_()
    {
      const bool force = false;
      session_mgr_.kill_zombie_session(force);
      process_unsettled_trans();  //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701
    }

    void TransExecutor::handle_show_sessions_(ObPacket &pkt,
                                              ObNewScanner &scanner,
                                              ObDataBuffer &buffer)
    {
      scanner.reuse();
      session_mgr_.show_sessions(scanner);
      UPS.response_scanner(OB_SUCCESS, pkt, scanner, buffer);
    }

    void TransExecutor::handle_kill_session_(ObPacket &pkt)
    {
      int &ret = thread_errno();
      ret = OB_SUCCESS;
      uint32_t session_descriptor = INVALID_SESSION_DESCRIPTOR;
      int64_t pos = pkt.get_buffer()->get_position();
      if (OB_SUCCESS != (ret = serialization::decode_vi32(pkt.get_buffer()->get_data(),
                                                          pkt.get_buffer()->get_capacity(),
                                                          pos,
                                                          (int32_t*)&session_descriptor)))
      {
        YYSYS_LOG(WARN, "deserialize session descriptor fail ret=%d", ret);
      }
      else
      {
        ret = session_mgr_.kill_session(session_descriptor);
        YYSYS_LOG(INFO, "kill session ret=%d sd=%u", ret, session_descriptor);
      }
      UPS.response_result(ret, pkt);
    }

    void *TransExecutor::on_trans_begin()
    {
      TransParamData *ret = NULL;
      void *buffer = ob_malloc(sizeof(TransParamData), ObModIds::OB_UPS_PARAM_DATA);
      if (NULL != buffer)
      {
        ret = new(buffer) TransParamData();
        ret->buffer.set_data(ret->cbuffer, sizeof(ret->cbuffer));
      }
      return ret;
    }

    void TransExecutor::on_trans_end(void *ptr)
    {
      if (NULL != ptr)
      {
        ob_free(ptr);
        ptr = NULL;
      }
    }

    void TransExecutor::log_scan_qps_()
    {
      static volatile uint64_t counter = 0;
      static const int64_t mod = 100000;
      static int64_t last_report_ts = 0;
      if (1 == (ATOMIC_ADD(&counter, 1) % mod))
      {
        int64_t cur_ts = yysys::CTimeUtil::getTime();
        YYSYS_LOG(INFO, "SCAN total=%lu, SCAN_QPS=%ld", counter, 1000000 * mod/(cur_ts - last_report_ts));
        last_report_ts = cur_ts;
      }
    }

    void TransExecutor::log_get_qps_()
    {
      static volatile uint64_t counter = 0;
      static const int64_t mod = 100000;
      static int64_t last_report_ts = 0;
      if (1 == (ATOMIC_ADD(&counter, 1) % mod))
      {
        int64_t cur_ts = yysys::CTimeUtil::getTime();
        YYSYS_LOG(INFO, "GET total=%lu, GET_QPS=%ld", counter, 1000000 * mod/(cur_ts - last_report_ts));
        last_report_ts = cur_ts;
      }
    }

    //add [449]
    void TransExecutor::handle_update_partition_stat(ObPacket &pkt)
    {
        int &ret = thread_errno();
        ret = OB_SUCCESS;
        int64_t mem_version = OB_INVALID_VERSION;
        int64_t pos = pkt.get_buffer()->get_position();

        if(OB_SUCCESS != (ret = serialization::decode_vi64(pkt.get_buffer()->get_data(),
                                                           pkt.get_buffer()->get_capacity(),
                                                           pos, &mem_version)))
        {
            YYSYS_LOG(WARN, "deserialize mem version fail ret=%d", ret);
        }
        else if(hash::HASH_INSERT_SUCC == (ret = partition_unchanged_version_.set(mem_version, 1)))
        {
            YYSYS_LOG(INFO, "update partition unchanged version %ld success", mem_version);
            ret = OB_SUCCESS;
        }
        UPS.response_result(ret, pkt);

        //clear out-of-date version
        hash::ObHashMap<int64_t, int, hash::ReadWriteDefendMode>::iterator iter = partition_unchanged_version_.begin();
        ObVector<int64_t> expired_version;
        int64_t merged_version = UPS.table_mgr_.get_merged_version();

        for(; iter != partition_unchanged_version_.end(); iter++)
        {
            if(iter->first < merged_version)
            {
                expired_version.push_back(iter->first);
            }
        }

        for(int i = 0; i < expired_version.size(); i++)
        {
            partition_unchanged_version_.erase(expired_version[i]);
        }

    }

    bool TransExecutor::partition_change_between_versions(uint64_t active_mem_table_version, uint64_t curr_frozen_version)
    {
        bool ret = false;
        int temp = 0;
        for(uint64_t i = curr_frozen_version; i < active_mem_table_version - 1; i++)
        {
            if(common::hash::HASH_EXIST != partition_unchanged_version_.get(i,temp))
            {
                YYSYS_LOG(WARN, "version %lu may change partition rules", i);
                ret = true;
                break;
            }
        }
        return ret;
    }


    ////////////////////////////////////////////////////////////////////////////////////////////////////

    bool is_write_packet(ObPacket& pkt)
    {
      bool ret = false;
      switch(pkt.get_packet_code())
      {
        case OB_MS_MUTATE:
        case OB_WRITE:
        case OB_PHY_PLAN_EXECUTE:
        case OB_END_TRANSACTION:
        case OB_TRUNCATE_TABLE:
        //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
        case OB_COMMIT_TRANSACTION:
        case OB_WRITE_TRANS_STAT:
        //add 20150701:e
          ret = true;
          break;
        default:
          ret = false;
      }
      return ret;
    }

    //add shili, [MultiUPS] [LONG_TRANSACTION_LOG]  20170523:b
    bool is_prepare_packet(ObPacket& pkt)
    {
      bool ret = false;
      switch(pkt.get_packet_code())
      {
        case OB_PREPARE_TRANSACTION:
          ret = true;
          break;
        default:
          ret = false;
      }
      return ret;
    }
    //add e

    //add wangyao[389]:b
    bool is_dis_commit_packet(ObPacket &pkt)
    {
        bool ret = false;
        switch(pkt.get_packet_code())
        {
          case OB_COMMIT_TRANSACTION:
            ret = true;
            break;
          default:
            ret = false;
        }
        return ret;
    }
    //add:e

    int64_t TransExecutor::get_commit_queue_len()
    {
      return session_mgr_.get_trans_seq().get_seq() - TransCommitThread::task_queue_.get_seq() + 1;
    }

    int64_t TransExecutor::get_seq(void* ptr)
    {
      int64_t seq = 0;
      Task& task = *((Task*)ptr);
      int64_t trans_id = yysys::CTimeUtil::getTime();
      int ret = OB_SUCCESS;
      int err = OB_SUCCESS;
      seq = session_mgr_.get_trans_seq().next(trans_id);
      //add zhaoqiong[244]:b
      bool is_conflict = false;
      uint64_t table_id = OB_INVALID_ID;
      //add:e
      if (NULL == ptr)
      {
        YYSYS_LOG(ERROR, "commit queue, NULL ptr, will kill self");
        kill(getpid(), SIGTERM);
      }
      else if (is_write_packet(task.pkt))
      {
        {
          SessionGuard session_guard(session_mgr_, lock_mgr_, ret);
          RWSessionCtx* session_ctx = NULL;
          if (OB_SUCCESS != (ret = session_guard.fetch_session(task.sid, session_ctx)))
          {
            YYSYS_LOG(ERROR, "fetch_session(sid=%s)=>%d", to_cstring(task.sid), ret);
          }
          //add shili [LONG_TRANSACTION_LOG]  20160926:b
          //else if (!UPS.get_log_mgr().check_log_size(session_ctx->get_ups_mutator().get_serialize_size()))
          //{
          //  ret = OB_LOG_TOO_LARGE;
          //  YYSYS_LOG(ERROR, "mutator.size[%ld] too large",
          //            session_ctx->get_ups_mutator().get_serialize_size());
          //}
          else if (!UPS.get_log_mgr().check_mutator_size(session_ctx->get_ups_mutator().get_serialize_size(),
                                                         UPS.get_param().max_mutator_size))
          {
            ret = OB_LOG_TOO_LARGE;
            YYSYS_LOG(ERROR, "mutator.size[%ld] too large",
                      session_ctx->get_ups_mutator().get_serialize_size());
          }
          //add e
          else if (OB_SUCCESS == (ret = session_ctx->check_truncate_conflict(table_id,is_conflict)) && is_conflict)
          {
              ret = OB_TABLE_UPDATE_LOCKED;
              YYSYS_LOG(ERROR,"check_truncate_conflict[table_id=%ld], session rollback[ret=%d]",table_id,ret);
          }
          if(ret != OB_SUCCESS)
          {
              //do nothing
          }
          else if(session_ctx->is_frozen())
          {
            task.sid.reset();
            YYSYS_LOG(ERROR, "session stat is frozen, maybe request duplicate, sd=%u",
                      session_ctx->get_session_descriptor());
          }
          else
          {
              session_ctx->set_frozen();//[592]
            session_ctx->set_trans_id(trans_id);
            session_ctx->get_checksum();
          }
          if(is_dis_commit_packet(task.pkt))
          {
              err = ret;
              ret = OB_SUCCESS;
          }
        }
        if(is_dis_commit_packet(task.pkt))
        {
            ret = err;
        }
//        if (OB_SUCCESS == ret && task.sid.is_valid()
//            && OB_SUCCESS != (ret = session_mgr_.precommit(task.sid.descriptor_)))
//        {
//          YYSYS_LOG(ERROR, "precommit(%s)=>%d", to_cstring(task.sid), ret);
//        }
        if (OB_SUCCESS != ret)
        {
            if(is_dis_commit_packet(task.pkt))
            {
                common::ObTransID tmp_trans_id;
                task.sid.is_unset_trans_ = true;
                tmp_trans_id = task.sid;
                session_mgr_.add_unsettled_trans(tmp_trans_id);
            }
          task.sid.reset();
          if(OB_TABLE_UPDATE_LOCKED == ret)
          {
              task.last_process_retcode = OB_TABLE_UPDATE_LOCKED;
          }
        }
      }
      //add shili, [MultiUPS] [LONG_TRANSACTION_LOG]  20170523:b
      else if(is_prepare_packet(task.pkt))
      {
        {
          SessionGuard session_guard(session_mgr_, lock_mgr_, ret);
          RWSessionCtx *session_ctx = NULL;
          if(OB_SUCCESS != (ret = session_guard.fetch_session(task.sid, session_ctx)))
          {
            YYSYS_LOG(ERROR, "fetch_session(sid=%s)=>%d", to_cstring(task.sid), ret);
          }
          if(session_ctx->is_global_ctx())
          {
            YYSYS_LOG(DEBUG, "coordinator_tid_=%s,fetch_session(sid=%s),mutator_size:%ld",
                      to_cstring(session_ctx->get_coordinator_info()), to_cstring(task.sid),
                      session_ctx->get_ups_mutator().get_serialize_size());
            if(!UPS.get_log_mgr().check_log_size(session_ctx->get_ups_mutator().get_serialize_size()))
            {
              ret = OB_LOG_TOO_LARGE;
              YYSYS_LOG(ERROR, "distributed ctx not support big mutator,mutator.size[%ld] too large",
                        session_ctx->get_ups_mutator().get_serialize_size());
            }
            else if(session_ctx->is_frozen())
            {
              task.sid.reset();
              YYSYS_LOG(ERROR, "session stat is frozen, maybe request duplicate, sd=%u",
                        session_ctx->get_session_descriptor());
            }
            else //add wangyao [252]
            {
                session_ctx->set_frozen();//[521]
                session_ctx->set_trans_id(trans_id);
            }

            if (OB_SUCCESS != ret)
            {
              UPS.get_rpc_stub().ups_response_preapre_ack(UPS.get_param().dis_trans_process_timeout,
                                                          session_ctx->get_coordinator_info().ups_,
                                                          session_ctx->get_coordinator_info(),
                                                          UPS.get_self(),true);
              ret= OB_SUCCESS;
              task.sid.reset();
            }
          }
        }
      }
      //add e
      return seq;
    }

    void TransExecutor::handle_commit(void *ptask, void *pdata)
    {
      int ret = OB_SUCCESS;
      thread_errno() = OB_SUCCESS;
      bool release_task = true;
      Task *task = (Task*)ptask;
      CommitParamData *param = (CommitParamData*)pdata;
      if (NULL == task)
      {
        YYSYS_LOG(WARN, "null pointer task=%p", task);
      }
      else if (NULL == param)
      {
        YYSYS_LOG(WARN, "null pointer param data pdata=%p src=%s",
                  pdata, inet_ntoa_r(task->src_addr));
        ret = OB_ERR_UNEXPECTED;
      }
      else if (0 > task->pkt.get_packet_code()
              || OB_PACKET_NUM <= task->pkt.get_packet_code())
      {
        YYSYS_LOG(ERROR, "unknown packet code=%d src=%s",
                  task->pkt.get_packet_code(), inet_ntoa_r(task->src_addr));
        ret = OB_UNKNOWN_PACKET;
      }
      else if (OB_SUCCESS != (ret = handle_flushed_log_()))
      {
        YYSYS_LOG(ERROR, "handle_flushed_clog()=>%d", ret);
      }
      else
      {
        //����log�Դ���ǰ�����ֶΣ�trace id��chid���Ժ�����trace id��chidΪ׼
        PROFILE_LOG(DEBUG, TRACE_ID
                    SOURCE_CHANNEL_ID
                    PCODE
                    WAIT_TIME_US_IN_COMMIT_QUEUE,
                    (task->pkt).get_trace_id(),
                    (task->pkt).get_channel_id(),
                    (task->pkt).get_packet_code(),
                    yysys::CTimeUtil::getTime() - (task->pkt).get_receive_ts());
        if (wait_for_commit_(task->pkt.get_packet_code()))
        {
          {
            ObSpinLockGuard guard(write_clog_mutex_);
            commit_log_(); // ����־������ˢ�̣������ύend_session����Ӧ�ͻ��˵�����
          }
          if (is_only_master_can_handle(task->pkt.get_packet_code()))
          {
            //mod peiouya [MultiUPS] [UPS_SYNC_Frozen] 20150811:b
            //FakeWriteGuard guard(session_mgr_);
            FakeWriteGuard guard(session_mgr_, static_cast<common::PacketCode>(task->pkt.get_packet_code()));
            //mod 20150811:e
            if (!guard.is_granted())
            {
              ret = OB_NOT_MASTER;
              YYSYS_LOG(WARN, "only master can handle pkt_code=%d", task->pkt.get_packet_code());
            }
            else
            {
              ObSpinLockGuard guard(write_clog_mutex_);
              // �����֧���ܼ�FakeWriteGuard, ���򱸻�����־��û���ܴ�����
              release_task = commit_handler_[task->pkt.get_packet_code()](*this, *task, *param);
            }
          }
          else
          {
            ObSpinLockGuard guard(write_clog_mutex_);
            release_task = commit_handler_[task->pkt.get_packet_code()](*this, *task, *param);
          }
        }
        else
        {
          ObSpinLockGuard guard(write_clog_mutex_);
          release_task = commit_handler_[task->pkt.get_packet_code()](*this, *task, *param);
        }
      }
      if (NULL != task)
      {
        if (OB_SUCCESS != ret
            || OB_SUCCESS != thread_errno())
        {
          YYSYS_LOG(WARN, "process fail ret=%d pcode=%d src=%s",
                    (OB_SUCCESS != ret) ? ret : thread_errno(), task->pkt.get_packet_code(), inet_ntoa_r(task->src_addr));
        }
        if (OB_SUCCESS != ret)
        {
          UPS.response_result(ret, task->pkt);
        }
        if (release_task)
        {
          allocator_.free(task);
          task = NULL;
        }
      }
    }

    //mod peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150731:b
    //int TransExecutor::handle_write_commit_(Task &task)
    int TransExecutor::handle_write_commit_(Task &task, const bool prepare)
    //mod 20150731:e
    {
      int &ret = thread_errno();
      ret = OB_SUCCESS;
      if (!task.sid.is_valid())
      {
        if(is_prepare_packet(task.pkt))
        {
           YYSYS_LOG(ERROR,"prepare failed!");
        }
        else
        {
          ret = OB_TRANS_ROLLBACKED;
          if(OB_TABLE_UPDATE_LOCKED == task.last_process_retcode)
          {
              ret = OB_TABLE_UPDATE_LOCKED;
          }
          YYSYS_LOG(WARN, "session is rollbacked, maybe precommit failed");
        }
      }
      //[686]
      else if(ObUpsRoleMgr::MASTER != UPS.get_role_mgr().get_role())
      {
          ret = OB_ERROR;
          YYSYS_LOG(WARN, "master ups has changed");
      }
      else
      {
        SessionGuard session_guard(session_mgr_, lock_mgr_, ret);
        RWSessionCtx* session_ctx = NULL;
        if (OB_SUCCESS != (ret = session_guard.fetch_session(task.sid, session_ctx)))
        {
          YYSYS_LOG(ERROR, "fetch_session(sid=%s)=>%d", to_cstring(task.sid), ret);
        }
        //mod peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150731:b
        //else if (OB_SUCCESS != (ret = session_mgr_.update_commited_trans_id(session_ctx)))
        else if (!prepare && OB_SUCCESS != (ret = session_mgr_.update_commited_trans_id(session_ctx)))
        //mod 20150731:e
        {
          YYSYS_LOG(ERROR, "session_mgr.update_commited_trans_id(%s)=>%d", to_cstring(*session_ctx), ret);
        }
        else
        {
          if (0 != session_ctx->get_last_proc_time())
          {
            int64_t cur_time = yysys::CTimeUtil::getTime();
            OB_STAT_INC(UPDATESERVER, UPS_STAT_TRANS_CTIME, cur_time - session_ctx->get_last_proc_time());
            session_ctx->set_last_proc_time(cur_time);
          }

          batch_start_time() = (0 == batch_start_time()) ? yysys::CTimeUtil::getTime() : batch_start_time();
          int64_t cur_timestamp = session_ctx->get_trans_id();
          session_ctx->get_uc_info().uc_checksum = ob_crc64(session_ctx->get_uc_info().uc_checksum, &cur_timestamp, sizeof(cur_timestamp));
          if (cur_timestamp <= 0)
          {
            YYSYS_LOG(ERROR, "session_ctx.trans_id=%ld <= 0, will kill self", cur_timestamp);
            kill(getpid(), SIGTERM);
          }
          FILL_TRACE_BUF(session_ctx->get_tlog_buffer(), "trans_checksum=%lu trans_id=%ld",
                         session_ctx->get_uc_info().uc_checksum, cur_timestamp);
          //int ret = fill_log_(task, *session_ctx);
          //add shili [LONG_TRANSACTION_LOG]  20160926:b
          ret = fill_session_log_(task, *session_ctx,prepare);
          //add e
          if(ret != OB_SUCCESS)
          {
            YYSYS_LOG(WARN, "fill session log fail,ret=%d,tid=%s", ret, to_cstring(task.sid));
          }

        }
        if (OB_SUCCESS != ret)
        {
          YYSYS_LOG(ERROR, "unexpect error, ret=%d %s, will kill self", ret, to_cstring(task.sid));
          kill(getpid(), SIGTERM);
        }
      }

      //[588]
      if(OB_WRITE_TRANS_STAT == task.pkt.get_packet_code())
      {
          UPS.get_table_mgr().get_table_mgr()->release_memtable_lock();
      }

      //modify wangdonghui [Paxos ups_replication] 20170323 :b TO_DO dynamic adjus
      if( UPS.get_wait_sync_type() == ObUpsLogMgr::WAIT_FLUSH )
      {
          int64_t cur_time_us = yysys::CTimeUtil::getTime();
          if(OB_SUCCESS == ret
             && ((message_residence_time_us_ < message_residence_max_us_?
                 message_residence_time_us_: message_residence_max_us_) <= cur_time_us - last_commit_log_time_us_))

        {
            ret = commit_log_();
            last_commit_log_time_us_ = yysys::CTimeUtil::getTime();
        }
      }
      else
      {
        int64_t cur_time_us = yysys::CTimeUtil::getTime();
        if (OB_SUCCESS == ret
            && (0 == TransCommitThread::get_queued_num()//next is ready 6000���ң�700
                  || cur_time_us - last_commit_log_time_us_ >= message_residence_time_us_ * 5))
                  //|| MAX_BATCH_NUM <= uncommited_session_list_.size()))
        {
            ret = commit_log_();
            last_commit_log_time_us_ = yysys::CTimeUtil::getTime();
        }
      }
      //mod :e
      if (OB_SUCCESS != ret)
      {
        UPS.response_result(ret, task.pkt);
      }
      return ret;
    }

    //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
    int TransExecutor::handle_write_trans_end_(Task &task, const bool commit)
    {
      int &ret = thread_errno();
      ret = OB_SUCCESS;
      if (!task.sid.is_valid())
      {
        ret = OB_TRANS_ROLLBACKED;
        YYSYS_LOG(WARN, "session is rollbacked, maybe precommit faile");
      }
      //[686]
      else if(ObUpsRoleMgr::MASTER != UPS.get_role_mgr().get_role())
      {
          ret = OB_ERROR;
          YYSYS_LOG(WARN, "master ups has changed");
      }
      else
      {
        SessionGuard session_guard(session_mgr_, lock_mgr_, ret);
        RWSessionCtx* session_ctx = NULL;
        if (OB_SUCCESS != (ret = session_guard.fetch_session(task.sid, session_ctx)))
        {
          YYSYS_LOG(ERROR, "fetch_session(sid=%s)=>%d", to_cstring(task.sid), ret);
        }
        else if (commit && OB_SUCCESS != (ret = session_mgr_.update_commited_trans_id(session_ctx)))
        {
          YYSYS_LOG(ERROR, "session_mgr.update_commited_trans_id(%s)=>%d", to_cstring(*session_ctx), ret);
        }
        else
        {
          if (0 != session_ctx->get_last_proc_time())
          {
            int64_t cur_time = yysys::CTimeUtil::getTime();
            OB_STAT_INC(UPDATESERVER, UPS_STAT_TRANS_CTIME, cur_time - session_ctx->get_last_proc_time());
            session_ctx->set_last_proc_time(cur_time);
          }

          batch_start_time() = (0 == batch_start_time()) ? yysys::CTimeUtil::getTime() : batch_start_time();
          int ret = fill_trans_end_log_(task, *session_ctx, commit);
          if (OB_SUCCESS != ret)
          {
            if (OB_EAGAIN != ret)
            {
              YYSYS_LOG(WARN, "fill log fail ret=%d %s", ret, to_cstring(task.sid));
            }
            else if (OB_SUCCESS != (ret = commit_log_()))
            {
              YYSYS_LOG(WARN, "commit log fail ret=%d %s", ret, to_cstring(task.sid));
            }
            else if (OB_SUCCESS != (ret = fill_trans_end_log_(task, *session_ctx, commit)))
            {
              YYSYS_LOG(ERROR, "second fill log fail ret=%d %s serialize_size=%ld uncommited_number=%ld",
                        ret, to_cstring(task.sid),
                        session_ctx->get_ups_mutator().get_serialize_size(),
                        uncommited_session_list_.size());
            }
            else
            {
              YYSYS_LOG(INFO, "second fill log succ %s", to_cstring(task.sid));
            }
          }
        }
        if (OB_SUCCESS != ret)
        {
          YYSYS_LOG(ERROR, "unexpect error, ret=%d %s, will kill self", ret, to_cstring(task.sid));
          kill(getpid(), SIGTERM);
        }
      }
      if (OB_SUCCESS != ret)
      {
        //UPS.response_result(ret, task.pkt);
        YYSYS_LOG(WARN, "fail to write trans end log, commit:%d, ret %d", commit, ret);
      }
      else if (0 == TransCommitThread::get_queued_num()
              || MAX_BATCH_NUM <= uncommited_session_list_.size())
      {
        ret = commit_log_();
      }
      return ret;
    }

    //add 20150701:e
    void TransExecutor::on_commit_idle()
    {
      commit_log_();
      handle_flushed_log_();
      try_submit_auto_freeze_();
    }

    int TransExecutor::handle_response(ObAckQueue::WaitNode& node)
    {
      int ret = OB_SUCCESS;
      YYSYS_LOG(TRACE, "send_log response: %s", to_cstring(node));
      if (OB_SUCCESS != node.err_)
      {
        YYSYS_LOG(WARN, "send_log %s faile", to_cstring(node));
        //mod wangjiahao [Paxos ups_replication] 20150731 :b
        if (OB_LOG_NOT_SYNC != node.err_ && UPS.get_slave_mgr().need_to_del(node.server_, UPS.get_delete_ups_wait_time()))
        { //means that UPS offline or network err
          UPS.get_slave_mgr().delete_server(node.server_);
        }
        else
        {
          //do nothing
        }
        //mod :e
      }
      else
      {
        //add wangjiahao [Paxos ups_replication]  20150601:b
        UPS.get_slave_mgr().set_last_reply_log_seq(node.server_, node.end_seq_);
        //add:e
        UPS.get_log_mgr().get_clog_stat().add_net_us(node.start_seq_, node.end_seq_, node.get_delay());
        //add wangdonghui [ups_replication] 20170323 :b
        if(-1 == node.message_residence_time_us_)
        {}
        else
        {
            message_residence_time_us_ = (message_residence_time_us_ >> 1) + (node.message_residence_time_us_ >> 1);
            //YYSYS_LOG(INFO, "WDH_TEST::time: %ld, node: %ld", message_residence_time_us_, node.message_residence_time_us_);
        }
        //add :e
      }
      return ret;
    }

    int TransExecutor::on_ack(ObAckQueue::WaitNode& node)
    {
      int ret = OB_SUCCESS;
      YYSYS_LOG(TRACE, "on_ack: %s", to_cstring(node));
      if (OB_SUCCESS != (ret = TransCommitThread::push(&nop_task_)))
      {
        YYSYS_LOG(ERROR, "push nop_task to wakeup commit thread fail, %s, ret=%d", to_cstring(node), ret);
      }
      return ret;
    }
    //add wangjiahao [Paxos ups_replication]  20150601:b
    int TransExecutor::get_quorum_seq(int64_t &quorum_seq) const
    {
        int ret = OB_SUCCESS;
        if (UPS.get_role_mgr().get_role() == ObUpsRoleMgr::MASTER)
        {
            ret = UPS.get_slave_mgr().get_quorum_seq(quorum_seq);
        }
        else
        {
            ret = OB_NOT_MASTER;
        }
        return ret;
    }
    //add:e
    int TransExecutor::handle_flushed_log_()
    {
      int err = OB_SUCCESS;
      int64_t flushed_clog_id = UPS.get_log_mgr().get_flushed_clog_id();
      //add wangjiahao [Paxos ups_replication_tmplog] 20150629 :b
      if (!UPS.get_role_mgr().is_master())
      {
      }
      else if (OB_SUCCESS != UPS.get_log_mgr().write_cpoint(flushed_clog_id, true)) //function handle_flushed_log_() is a sigle thread method
      {
        YYSYS_LOG(WARN, "master write commit point fail");
      }
      //add :e
      int64_t flush_seq = 0;
      Task *task = NULL;
      bool need_free_task = false;
      while(true)
      {
        if (OB_SUCCESS != (err = flush_queue_.tail(flush_seq, (void*&)task))
            && OB_ENTRY_NOT_EXIST != err)
        {
          YYSYS_LOG(ERROR, "flush_queue_.tail()=>%d, will kill self", err);
          kill(getpid(), SIGTERM);
        }
        else if (OB_ENTRY_NOT_EXIST == err)
        {
          err = OB_SUCCESS;
          break;
        }
        else if (flush_seq > flushed_clog_id)
        {
          break;
        }
        //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
        else if (OB_ROLLBACK_TRANSACTION == task->pkt.get_packet_code())
        {
          session_mgr_.end_session(task->sid.descriptor_);
          need_free_task = true;
        }
        else if (OB_PREPARE_TRANSACTION == task->pkt.get_packet_code())
        {
          int ret_ok = OB_SUCCESS;
          SessionGuard session_guard(session_mgr_, lock_mgr_, ret_ok);
          RWSessionCtx *session_ctx = NULL;
          if (OB_SUCCESS != (err = session_guard.fetch_session(task->sid, session_ctx)))
          {
            YYSYS_LOG(ERROR, "unexpected fetch_session fail ret=%d %s, will kill self", err, to_cstring(task->sid));
            kill(getpid(), SIGTERM);
          }
          else
          {
            UPS.response_result(OB_SUCCESS, task->pkt);
            YYSYS_LOG(DEBUG, "prepare ack coordinator:%s", to_cstring(session_ctx->get_coordinator_info()));
            UPS.get_rpc_stub().ups_response_preapre_ack(UPS.get_param().dis_trans_process_timeout,
                                                        session_ctx->get_coordinator_info().ups_,
                                                        session_ctx->get_coordinator_info(),UPS.get_self());
            need_free_task = true;
          }
        }
        //add 20150701:e
        else
        {
          int ret_ok = OB_SUCCESS;
          SessionGuard session_guard(session_mgr_, lock_mgr_, ret_ok);
          RWSessionCtx *session_ctx = NULL;
          if (OB_SUCCESS != (err = session_guard.fetch_session(task->sid, session_ctx)))
          {
            YYSYS_LOG(ERROR, "unexpected fetch_session fail ret=%d %s, will kill self", err, to_cstring(task->sid));
            kill(getpid(), SIGTERM);
          }
          else
          {
            //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
            if (OB_WRITE_TRANS_STAT == task->pkt.get_packet_code())
            {
              //send_write_trans_stat_ack
              UPS.get_rpc_stub().ups_write_trans_stat_ack(UPS.get_param().dis_trans_process_timeout,
                                                          session_ctx->get_coordinator_info().ups_,
                                                          session_ctx->get_coordinator_info(),
                                                          session_ctx->get_trans_stat());
            }
            //add 20150701:e
            task->pkt.set_packet_code(OB_COMMIT_END);
            session_guard.revert();
            session_mgr_.end_session(session_ctx->get_session_descriptor(), false, true, BaseSessionCtx::ES_PUBLISH);
            if (OB_SUCCESS != (err = CommitEndHandlePool::push(task)))
            {
              YYSYS_LOG(ERROR, "push(task=%p)=>%d, will kill self", task, err);
              kill(getpid(), SIGTERM);
            }
            else if (OB_SUCCESS != (err = flush_queue_.pop()))
            {
              YYSYS_LOG(ERROR, "flush_queue.consume_tail()=>%d", err);
            }
          }
        }
        //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150716:b
        if(need_free_task)
        {
          allocator_.free(task);
          task = NULL;
          if (OB_SUCCESS != (err = flush_queue_.pop()))
          {
            YYSYS_LOG(ERROR, "flush_queue.consume_tail()=>%d", err);
          }
          need_free_task = false;
        }
        //add 20150716:e
      }
      return err;
    }


    //add shili [LONG_TRANSACTION_LOG]  20160926:b
    int TransExecutor::fill_session_log_(Task &task, RWSessionCtx &session_ctx,const bool prepare)
    {
      int ret= OB_SUCCESS;
      int64_t mutator_size = session_ctx.get_ups_mutator().get_serialize_size();
      if(UPS.get_log_mgr().is_normal_mutator_size(mutator_size))//small  transaction log
      {
        YYSYS_LOG(DEBUG,"mutator size:%ld",mutator_size);
        if(OB_SUCCESS!=(ret = fill_normal_log_(task, session_ctx,prepare)))
        {
          YYSYS_LOG(WARN, "fill small tran log fail,ret=%d", ret);
        }
      }
      else//long  transaction log
      {
        YYSYS_LOG(INFO,"big log mutator size,%ld",mutator_size);
        if(OB_SUCCESS!=(ret = fill_big_log_(task, session_ctx, prepare)))
        {
          YYSYS_LOG(WARN, "fill long tran log fail,ret=%d", ret);
        }
      }
      return ret;
    }

    /*@berif  д ��־С��2M����� log �� log buffer*/
    int TransExecutor::fill_normal_log_(Task &task, RWSessionCtx &session_ctx,const bool prepare) // uncertainty
    {
      int ret= OB_SUCCESS;
      ret = fill_log_(task, session_ctx,prepare);// uncertainty
      if (OB_SUCCESS != ret)
      {
        if (OB_EAGAIN != ret)
        {
          YYSYS_LOG(WARN, "fill log fail ret=%d %s", ret, to_cstring(task.sid));
        }
        else if (OB_SUCCESS != (ret = commit_log_()))
        {
          YYSYS_LOG(WARN, "commit log fail ret=%d %s", ret, to_cstring(task.sid));
        }
        else if (OB_SUCCESS != (ret = fill_log_(task, session_ctx,prepare)))// uncertainty
        {
          YYSYS_LOG(ERROR, "second fill log fail ret=%d %s serialize_size=%ld uncommited_number=%ld",
                    ret, to_cstring(task.sid),
                    session_ctx.get_ups_mutator().get_serialize_size(),
                    uncommited_session_list_.size());
        }
        else
        {
          YYSYS_LOG(INFO, "second fill log succ %s", to_cstring(task.sid));
        }
      }
      return ret;
    }

    /*@berif д�� long trans ȫ��log*/
    int TransExecutor::fill_big_log_(Task &task, RWSessionCtx &session_ctx, const bool prepare)
    {
      int ret = OB_SUCCESS;
      MemTable *mt = NULL;
      char *buf = NULL;
      int64_t mutator_size = 0;
      UPS.get_big_log_writer().reset();
      if(0 != session_ctx.get_ups_mutator().get_mutator().size() ||
         (OB_PHY_PLAN_EXECUTE != task.pkt.get_packet_code()
          && OB_END_TRANSACTION != task.pkt.get_packet_code()
          && OB_PREPARE_TRANSACTION != task.pkt.get_packet_code()))
      {
        if(NULL == (mt = session_ctx.get_uc_info().host))
        {
          ret = OB_ERR_UNEXPECTED;
        }
        else if (prepare)
        {
          ////prepare do not calculate checksum, calculate until receive commit
          //YYSYS_LOG(DEBUG, "prepare uncheck_sum:%lu", session_ctx.get_uc_info().uc_checksum);
          //session_ctx.get_ups_mutator().set_mutate_timestamp(session_ctx.get_trans_id());
          //
          mutator_size = session_ctx.get_ups_mutator().get_serialize_size();
          //if(NULL == (buf = (char *) ob_malloc(mutator_size, ObModIds::OB_UPS_BIG_LOG_DATA))) //��Ҫ�ֶ��ͷ��ڴ�
          //{
          //  ret = OB_MEM_OVERFLOW;
          //  YYSYS_LOG(ERROR, "ob_malloc alloc(%ld)=>%d", mutator_size, ret);
          //}
          //else if(OB_SUCCESS != (ret = session_ctx.get_ups_mutator().pre_serialize(buf, mutator_size)))
          //{
          //  YYSYS_LOG(WARN, "pre serialize  ups mutator fail,ret=%d", ret);
          //}
          //else
          //{
          //  UPS.get_big_log_writer().assign(buf, (int32_t) mutator_size, OB_UPS_MUTATOR_PREPARE);
          //  if(OB_SUCCESS != (ret = UPS.get_big_log_writer().write_big_log(task.sid, session_ctx)))
          //  {
          //    YYSYS_LOG(WARN, "write big log fail,ret=%d", ret);
          //  }
          //}
          //if(NULL != buf)
          //{
          //  ob_free(buf);//�ͷ�������ڴ�
          //  buf = NULL;
          //  UPS.get_big_log_writer().assign(NULL,0);//ptr_ָ��Ϊ��
          //}
          ret = OB_NOT_SUPPORTED;
          YYSYS_LOG(ERROR, "big log not supported distributed ctx,mutatorsize:%ld,ret=%d", mutator_size, ret);
        }
        else
        {
          int64_t uc_checksum = 0;
          uc_checksum = mt->calc_uncommited_checksum(session_ctx.get_uc_info().uc_checksum);
          int64_t old = uc_checksum;
          uc_checksum ^= session_ctx.get_checksum();
          session_ctx.get_ups_mutator().set_mutate_timestamp(session_ctx.get_trans_id());
          session_ctx.get_ups_mutator().set_memtable_checksum_before_mutate(mt->get_uncommited_checksum());
          session_ctx.get_ups_mutator().set_memtable_checksum_after_mutate(uc_checksum);
          uc_checksum = old;

          mutator_size = session_ctx.get_ups_mutator().get_serialize_size();
          if(NULL == (buf = (char *) ob_malloc(mutator_size, ObModIds::OB_UPS_BIG_LOG_DATA))) //��Ҫ�ֶ��ͷ��ڴ�
          {
            ret = OB_MEM_OVERFLOW;
            YYSYS_LOG(ERROR, "ob_malloc alloc(%ld)=>%d", mutator_size, ret);
          }
          else if(OB_SUCCESS != (ret = session_ctx.get_ups_mutator().pre_serialize(buf, mutator_size)))
          {
            YYSYS_LOG(WARN, "pre serialize  ups mutator fail,ret=%d", ret);
          }
          else
          {
            UPS.get_big_log_writer().assign(buf, (int32_t) mutator_size, OB_LOG_UPS_MUTATOR);
            if(OB_SUCCESS != (ret = UPS.get_big_log_writer().write_big_log(task.sid, session_ctx)))
            {
              YYSYS_LOG(WARN, "write big log fail,ret=%d", ret);
            }
            else
            {
                if(OB_SUCCESS != (ret = UPS.get_log_mgr().set_fill_log_max_log_id()))
                {
                    YYSYS_LOG(ERROR,"set_fill_log_max_log_id failed[ret=%d]",ret);
                }
                else
                {
                    session_ctx.get_uc_info().uc_checksum = uc_checksum;
                    mt->update_uncommited_checksum(session_ctx.get_uc_info().uc_checksum);
                }
            }
          }

          if(NULL != buf)
          {
            ob_free(buf);//�ͷ�������ڴ�
            buf = NULL;
            UPS.get_big_log_writer().assign(NULL,0);//ptr_ָ��Ϊ��
          }
        }//end else
      }
      else
      {
        session_ctx.get_uc_info().host = NULL;
      }
      if(OB_SUCCESS == ret)
      {
        ObLogCursor filled_cursor;
        if(OB_SUCCESS != (ret = UPS.get_log_mgr().get_filled_cursor(filled_cursor)))
        {
          YYSYS_LOG(ERROR, "get_fill_cursor()=>%d", ret);
        }
        else if(0 != flush_queue_.push(filled_cursor.log_id_, &task))
        {
          ret = (OB_SUCCESS == ret) ? OB_MEM_OVERFLOW : ret;
          YYSYS_LOG(ERROR, "unexpected push task to uncommited_session_list fail list_size=%ld, will kill self",
                    uncommited_session_list_.size());
          kill(getpid(), SIGTERM);
        }
        else
        {
          // ��֤��flush commit log�ɹ��󲻻ᱻkill��  todo shili �Ƿ���Ҫ��������
          session_ctx.set_frozen();
        }
      }
      FILL_TRACE_BUF(session_ctx.get_tlog_buffer(), "checksum=%lu affected_rows=%ld ret=%d",
                     (NULL == mt) ? 0 : mt->get_uncommited_checksum(), session_ctx.get_ups_result().get_affected_rows(),
                     ret);
      return ret;
    }
    //add e

    //mod peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150731:b
    //int TransExecutor::fill_log_(Task &task, RWSessionCtx &session_ctx)
    int TransExecutor::fill_log_(Task &task, RWSessionCtx &session_ctx, const bool prepare)
    //mod 20150731:e
    {
      int ret = OB_SUCCESS;
      MemTable *mt = NULL;
      if (0 != session_ctx.get_ups_mutator().get_mutator().size()
          || (OB_PHY_PLAN_EXECUTE != task.pkt.get_packet_code()
          //mod peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150731:b
          //    && OB_END_TRANSACTION != task.pkt.get_packet_code()))
              && OB_END_TRANSACTION != task.pkt.get_packet_code()
              && OB_PREPARE_TRANSACTION != task.pkt.get_packet_code()))
          //mod 20150731:e
      {
        if (NULL == (mt = session_ctx.get_uc_info().host))
        {
          ret = OB_ERR_UNEXPECTED;
        }
        //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150816:b
        else if (prepare)
        {
          //prepare do not calculate checksum, calculate until receive commit
          YYSYS_LOG(DEBUG, "prepare uncheck_sum:%lu", session_ctx.get_uc_info().uc_checksum);
          session_ctx.get_ups_mutator().set_mutate_timestamp(session_ctx.get_trans_id());
          ret = UPS.get_table_mgr().fill_prepare_log(session_ctx.get_ups_mutator(), session_ctx.get_coordinator_info(), session_ctx.get_tlog_buffer());
        }
        //add 20150816:e
        else
        {
          int64_t uc_checksum = 0;
          uc_checksum = mt->calc_uncommited_checksum(session_ctx.get_uc_info().uc_checksum);
          int64_t old = uc_checksum;
          uc_checksum ^= session_ctx.get_checksum();
          session_ctx.get_ups_mutator().set_mutate_timestamp(session_ctx.get_trans_id());
          session_ctx.get_ups_mutator().set_memtable_checksum_before_mutate(mt->get_uncommited_checksum());
          session_ctx.get_ups_mutator().set_memtable_checksum_after_mutate(uc_checksum);
          uc_checksum = old;

          ret = UPS.get_table_mgr().fill_commit_log(session_ctx.get_ups_mutator(), session_ctx.get_tlog_buffer());
          if (OB_SUCCESS == ret)
          {
              if(OB_SUCCESS != (ret = UPS.get_log_mgr().set_fill_log_max_log_id()))
              {
                 YYSYS_LOG(ERROR,"set_fill_log_max_log_id failed[ret=%d]",ret);
              }
              else
              {
                  session_ctx.get_uc_info().uc_checksum = uc_checksum;
                  mt->update_uncommited_checksum(session_ctx.get_uc_info().uc_checksum);
              }
          }
        }
      }
      else
      {
        session_ctx.get_uc_info().host = NULL;
      }
      if (OB_SUCCESS == ret)
      {
        ObLogCursor filled_cursor;
        if (OB_SUCCESS != (ret = UPS.get_log_mgr().get_filled_cursor(filled_cursor)))
        {
          YYSYS_LOG(ERROR, "get_fill_cursor()=>%d", ret);
        }
        else if (0 != flush_queue_.push(filled_cursor.log_id_, &task))
        {
          ret = (OB_SUCCESS == ret) ? OB_MEM_OVERFLOW : ret;
          YYSYS_LOG(ERROR, "unexpected push task to uncommited_session_list fail list_size=%ld, will kill self", uncommited_session_list_.size());
          kill(getpid(), SIGTERM);
        }
        else
        {
          // ��֤��flush commit log�ɹ��󲻻ᱻkill��
          //mod peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150731:b
          //session_ctx.set_frozen();
          if(prepare)
          {
            session_ctx.set_prepare();
          }
          else
          {
            session_ctx.set_frozen();
          }
          //mod 20150731:e
        }
      }
      FILL_TRACE_BUF(session_ctx.get_tlog_buffer(), "checksum=%lu affected_rows=%ld ret=%d",
                    (NULL == mt) ? 0 : mt->get_uncommited_checksum(),
                    session_ctx.get_ups_result().get_affected_rows(), ret);
      return ret;
    }
    //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
    int TransExecutor::fill_trans_end_log_(Task &task, RWSessionCtx &session_ctx, const bool commit)
    {
      int ret = OB_SUCCESS;
      MemTable *mt = NULL;
      PartCommitInfo commit_info;
      int response_ret = OB_SUCCESS;
      int64_t uc_checksum = 0;
      if (NULL == (mt = session_ctx.get_uc_info().host))
      {
        YYSYS_LOG(WARN, "uncommit info host is null");
      }
      else
      {
        if(!commit)
        {
          response_ret = OB_TRANS_ROLLBACKED;
        }
        else
        {
          uc_checksum = mt->calc_uncommited_checksum(session_ctx.get_uc_info().uc_checksum);
          int64_t old = uc_checksum;
          uc_checksum ^= session_ctx.get_checksum();
          commit_info.mutate_timestamp_ = session_ctx.get_trans_id();
          commit_info.memtable_checksum_before_mutate_ = mt->get_uncommited_checksum();
          commit_info.memtable_checksum_after_mutate_ = uc_checksum;
          uc_checksum = old;
        }

        commit_info.coordinator_ = session_ctx.get_coordinator_info();
        ret = UPS.get_table_mgr().fill_stat_log(commit_info, commit);

        if (OB_SUCCESS == ret && commit)
        {
            if(OB_SUCCESS != (ret = UPS.get_log_mgr().set_fill_log_max_log_id()))
            {
                YYSYS_LOG(ERROR,"set_fill_log_max_log_id failed[ret=%d]",ret);
            }
            else
            {
                session_ctx.get_uc_info().uc_checksum = uc_checksum;
                mt->update_uncommited_checksum(session_ctx.get_uc_info().uc_checksum);
            }
        }
      }

      if (OB_SUCCESS == ret)
      {
        ObLogCursor filled_cursor;
        if (OB_SUCCESS != (ret = UPS.get_log_mgr().get_filled_cursor(filled_cursor)))
        {
          YYSYS_LOG(ERROR, "get_fill_cursor()=>%d", ret);
        }
        else if (0 != flush_queue_.push(filled_cursor.log_id_, &task))
        {
          ret = (OB_SUCCESS == ret) ? OB_MEM_OVERFLOW : ret;
          YYSYS_LOG(ERROR, "unexpected push task to uncommited_session_list fail list_size=%ld, will kill self", uncommited_session_list_.size());
          kill(getpid(), SIGTERM);
        }
        else
        {
          // ��֤��flush commit log�ɹ��󲻻ᱻkill��
          session_ctx.set_frozen();

          if (NULL != session_ctx.get_task())
          {
            //coordinator
            Task* task = static_cast<Task*>(session_ctx.get_task());
            if (NULL != task)
            {
              int64_t now = yysys::CTimeUtil::getTime();
              int64_t timeout = task->pkt.get_source_timeout();
              if (0 < timeout && now > timeout + task->pkt.get_receive_ts())
              {
                YYSYS_LOG(WARN, "already timeout, no need return");
              }
              else
              {
                UPS.response_result(response_ret,task->pkt);
              }
              allocator_.free(task);
              session_ctx.set_task(NULL);
            }
          }
        }
      }
      return ret;
    }

    int TransExecutor::fetch_trans_stat(const common::ObTransID &trans_id, bool& find_trans_stat, bool& commit)
    {
      int ret = OB_SUCCESS;
      char buf[OB_MAX_SQL_LENGTH] = "";
      char ip_buf[OB_MAX_SERVER_ADDR_SIZE] = "";
      //mod peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20160304:b
      //const char * sql_temp = "select /*+read_consistency(STRONG) */ commit_stat from  __ups_session_info where "
      //    "start_time = %ld and svr_ip = \'%s\' and svr_port = %d;";
      const char * sql_temp = "select /*+read_consistency(STRONG) */ commit_stat from  __ts_session_info where "
          "start_time = %ld and descriptor = %ld and svr_ip = \'%s\' and svr_port = %d;";
      //mod linhx [desi] 20211208:b
      //before debug: commit_stat from  __ups_session_info where
      //mod linhx [desi] 20211208:e
      //add peiouya 20160304:e
      ObString sql;
      find_trans_stat = false;
      if(false == trans_id.ups_.ip_to_string(ip_buf,sizeof(ip_buf)))
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(WARN, "convert server ip to string failed:ret[%d]", ret);
      }
      else
      {
        //mod peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20160304:b
        //snprintf(buf, sizeof (buf), sql_temp, trans_id.start_time_us_, ip_buf, trans_id.ups_.get_port());
        snprintf(buf, sizeof (buf), sql_temp, trans_id.start_time_us_,static_cast<int64_t>(trans_id.descriptor_), ip_buf, trans_id.ups_.get_port());
        //add peiouya 20160304:e
        sql.assign_ptr(buf, static_cast<ObString::obstr_size_t>(strlen(buf)));
      }

      static const int MY_VERSION = 1;
      ThreadSpecificBuffer::Buffer* my_buffer = UPS.get_rpc_buffer();
      ObDataBuffer data_buff(my_buffer->current(), my_buffer->remain());
      ObServer ms;
      UPS.get_ms(ms);
      if (OB_SUCCESS != (ret = sql.serialize(data_buff.get_data(), data_buff.get_capacity(),
              data_buff.get_position())))
      {
        YYSYS_LOG(WARN, "serialize sql fail, ret: [%d], sql: [%.*s]",
            ret, sql.length(), sql.ptr());
      }
      else if (OB_SUCCESS != (ret = UPS.get_client_manager().send_request(ms, OB_SQL_EXECUTE,
              MY_VERSION, UPS.get_param().fetch_schema_timeout, data_buff)))
      {
        YYSYS_LOG(WARN, "send sql request to [%s] fail, ret: [%d], sql: [%.*s]",
            to_cstring(ms), ret, sql.length(), sql.ptr());
      }
      else
      {
        //YYSYS_LOG(DEBUG, "send request success, trans_id:%s, sql:%s", to_cstring(trans_id),buf);
        int64_t pos = 0;
        sql::ObSQLResultSet rs;
        ret = rs.deserialize(data_buff.get_data(), data_buff.get_position(), pos);
        if (OB_SUCCESS != ret)
        {
          YYSYS_LOG(ERROR, "deserialize result_code failed: pos[%ld], ret[%d]", pos, ret);
        }
        else if (rs.get_errno() != OB_SUCCESS)
        {
//          ret = rs.get_errno();
//          YYSYS_LOG(WARN, "execute sql at[%s] fail, ret: [%d], sql: [%.*s]",
//              to_cstring(ms), ret, sql.length(), sql.ptr());
            //mod wangyao [288]
            ret = OB_FETCH_DIS_TRANS_STAT_ERR;
            YYSYS_LOG(WARN, "execute sql at[%s] fail, rs.get_errno: [%d], sql: [%.*s]",
                     to_cstring(ms), rs.get_errno(), sql.length(), sql.ptr());
        }
        else
        {
          ObNewScanner& scanner = rs.get_new_scanner();
          if (1 == scanner.get_row_num())
          {
            if (OB_SUCCESS != (ret = fill_trans_stat(commit, scanner)))
            {
              YYSYS_LOG(WARN, "failted to parse and update proxy list, ret=%d", ret);
            }
            else
            {
              find_trans_stat = true;
              YYSYS_LOG(INFO, "find trans stat success, commit:%d", commit);
            }
          }
          else
          {
            YYSYS_LOG(WARN, "scanner.get_row_num():%ld", scanner.get_row_num());
          }

        }
      }
      return ret;
    }

    int TransExecutor::fill_trans_stat(bool& commit, ObNewScanner& scanner)
    {
      int ret = OB_SUCCESS;
      ObRow row;
      const ObObj *cell = NULL;
      uint64_t tid = 0;
      uint64_t cid = 0;
      int64_t cell_idx = 0;

      if(OB_SUCCESS != (ret = scanner.get_next_row(row)))
      {
        YYSYS_LOG(WARN, "failed to get next row, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = row.raw_get_cell(cell_idx, cell, tid, cid)))
      {
        YYSYS_LOG(WARN, "failed to get cell, ret=%d", ret);
      }
      //mod dyr [MultiUPS] [DISTRIBUTED_TRANS] 20150831:b
//      else if (OB_SUCCESS != (ret = cell->get_bool(commit)))
//      {
//        YYSYS_LOG(WARN, "failed to get commit stat, ret=%d", ret);
//      }
      if (OB_SUCCESS == ret)
      {
        if (NULL == cell)
        {
          YYSYS_LOG(WARN,"get trans commit stat failed,cell is null!");
          ret = OB_ENTRY_NOT_EXIST;
        }
        else if (OB_SUCCESS != (ret = cell->get_bool(commit)))
        {
          YYSYS_LOG(WARN, "failed to get commit stat, ret=%d", ret);
        }
      }
      //mod 20150831:e
      return ret;
    }

    void TransExecutor::process_unsettled_trans()
    {
      YYSYS_LOG(DEBUG, "enter process_unsettled_trans");
      //[521]
      if(__sync_bool_compare_and_swap(&mutex_flag_for_unsettled_trans_, false, true))
      {
          common::SkipList<common::ObTransID>& unsettled_trans = session_mgr_.get_unsettled_trans();
          if(unsettled_trans.count() > 0)
          {
              int ret = OB_SUCCESS;
              YYSYS_LOG(INFO, "unsettle trans num > 0,enter process_every_node");
              ret = unsettled_trans.process_every_node(handle_unsettled_trans);
              if (OB_SUCCESS != ret)
              {
                  YYSYS_LOG(WARN, "fail to process_every_node, ret =%d", ret);
              }
          }
          __sync_bool_compare_and_swap(&mutex_flag_for_unsettled_trans_, true, false);
      }
    }

    bool TransExecutor::check_session_exist(const ObTransID& trans_id)
    {
      bool exist = true;
      BaseSessionCtx *session = session_mgr_.fetch_ctx(trans_id.descriptor_);
      if(NULL == session
         || session->get_session_descriptor() != trans_id.descriptor_
         || session->get_session_start_time() != trans_id.start_time_us_
         || !(UPS.get_self() == trans_id.ups_))
      {
        exist = false;
      }
      if(session != NULL)
      {
          session_mgr_.revert_ctx(trans_id.descriptor_);
      }
      return exist;
    }

    //add 20150701:e
    int TransExecutor::handle_commit_end_(Task &task, ObDataBuffer &buffer)
    {
      int ret = OB_SUCCESS;
      {
        int ret_ok = OB_SUCCESS;
        SessionGuard session_guard(session_mgr_, lock_mgr_, ret_ok);
        RWSessionCtx *session_ctx = NULL;
        if (OB_SUCCESS != (ret = session_guard.fetch_session(task.sid, session_ctx)))
        {
          YYSYS_LOG(ERROR, "unexpected fetch_session fail ret=%d %s, will kill self", ret, to_cstring(task.sid));
          kill(getpid(), SIGTERM);
        }
        else
        {
          session_ctx->get_ups_result().serialize(buffer.get_data(),
                                                  buffer.get_capacity(),
                                                  buffer.get_position());
        }
      }
      bool rollback = false;
      if (OB_SUCCESS != ret)
      {}
      else if (OB_SUCCESS != (ret = session_mgr_.end_session(task.sid.descriptor_, rollback)))
      {
        YYSYS_LOG(ERROR, "unexpected end_session fail ret=%d %s, will kill self", ret, to_cstring(task.sid));
        kill(getpid(), SIGTERM);
      }
      if(task.need_response)
      {
          if (OB_SUCCESS == ret)
          {
              UPS.response_buffer(ret, task.pkt, buffer); // phyplan�Ļ����ܶ��߳�ִ��
          }
          else
          {
              UPS.response_result(ret, task.pkt);
          }
      }
      return ret;
    }

    int TransExecutor::commit_log_()
    {
      int ret = OB_SUCCESS;
      int64_t end_log_id = 0;

      if (0 < flush_queue_.size())
      {
        CLEAR_TRACE_BUF(TraceLog::get_logbuffer());
        ret = UPS.get_log_mgr().async_flush_log(end_log_id, TraceLog::get_logbuffer());
        /*
        if (OB_SUCCESS != ret)
        {
          YYSYS_LOG(ERROR, "flush commit log fail ret=%d uncommited_number=%ld, will kill self", ret, uncommited_session_list_.size());
          kill(getpid(), SIGTERM);
        }
        YYSYS_TRACE_LOG("[GROUP_COMMIT] %s", TraceLog::get_logbuffer().buffer);
        bool rollback = (OB_SUCCESS != ret);
        int64_t i = 0;
        ObList<Task*>::iterator iter;
        for (iter = uncommited_session_list_.begin(); iter != uncommited_session_list_.end(); iter++, i++)
        {
          Task *task = *iter;
          task->pkt.set_packet_code(OB_COMMIT_END);
          if (OB_SUCCESS != (ret = CommitEndHandlePool::push(task)))
          {
            YYSYS_LOG(ERROR, "push(task=%p)=>%d, will kill self", task, ret);
            kill(getpid(), SIGTERM);
          }
          continue;
          if (NULL == task)
          {
            YYSYS_LOG(ERROR, "unexpected task null pointer batch=%ld, will kill self", uncommited_session_list_.size());
            kill(getpid(), SIGTERM);
          }
          else
          {
            {
              int ret_ok = OB_SUCCESS;
              SessionGuard session_guard(session_mgr_, lock_mgr_, ret_ok);
              RWSessionCtx *session_ctx = NULL;
              if (OB_SUCCESS != (ret = session_guard.fetch_session(task->sid, session_ctx)))
              {
                YYSYS_LOG(ERROR, "unexpected fetch_session fail ret=%d %s, will kill self", ret, to_cstring(task->sid));
                kill(getpid(), SIGTERM);
              }
              else
              {
                FILL_TRACE_BUF(session_ctx->get_tlog_buffer(), "%sbatch=%ld:%ld", TraceLog::get_logbuffer().buffer, i, uncommited_session_list_.size());
                ups_result_buffer_.set_data(ups_result_memory_, OB_MAX_PACKET_LENGTH);
                session_ctx->get_ups_result().serialize(ups_result_buffer_.get_data(),
                                                        ups_result_buffer_.get_capacity(),
                                                        ups_result_buffer_.get_position());
              }
            }
            if (OB_SUCCESS != (ret = session_mgr_.end_session(task->sid.descriptor_, rollback)))
            {
              YYSYS_LOG(ERROR, "unexpected end_session fail ret=%d %s, will kill self", ret, to_cstring(task->sid));
              kill(getpid(), SIGTERM);
            }
            ret = rollback ? OB_TRANS_ROLLBACKED : ret;
            if (OB_PHY_PLAN_EXECUTE == task->pkt.get_packet_code()
                && OB_SUCCESS == ret)
            {
              UPS.response_buffer(ret, task->pkt, ups_result_buffer_);
            }
            else
            {
              UPS.response_result(ret, task->pkt);
            }
            allocator_.free(task);
            task = NULL;
          }
        }
        uncommited_session_list_.clear(); */
        OB_STAT_INC(UPDATESERVER, UPS_STAT_BATCH_COUNT, 1);
        OB_STAT_INC(UPDATESERVER, UPS_STAT_BATCH_TIMEU, yysys::CTimeUtil::getTime() - batch_start_time());
        batch_start_time() = 0;
      }
      try_submit_auto_freeze_();
      return ret;
    }

    void TransExecutor::try_submit_auto_freeze_()
    {
      int err = OB_SUCCESS;
      static int64_t last_try_freeze_time = 0;
      if (TRY_FREEZE_INTERVAL < (yysys::CTimeUtil::getTime() - last_try_freeze_time)
          && UPS.get_table_mgr().need_auto_freeze())
      {
        int64_t cur_ts = yysys::CTimeUtil::getTime();
        //mod peiouya [MultiUPS] [UPS_Manage_Function] 20150615:b
        //if (OB_SUCCESS != (err = UPS.submit_auto_freeze()))
        //{
        //  YYSYS_LOG(WARN, "submit_auto_freeze()=>%d", err);
        //}
        //else
        //{
        //  YYSYS_LOG(INFO, "submit async auto freeze task, last_ts=%ld, cur_ts=%ld", last_try_freeze_time, cur_ts);
        //  last_try_freeze_time = cur_ts;
        //}
        if (UPS.check_if_permit_freeze())
        {
          if (OB_SUCCESS != (err = UPS.submit_auto_freeze()))
          {
            YYSYS_LOG(WARN, "submit_auto_freeze()=>%d", err);
          }
          else
          {
            YYSYS_LOG(INFO, "submit async auto freeze task, last_ts=%ld, cur_ts=%ld", last_try_freeze_time, cur_ts);
            last_try_freeze_time = cur_ts;
          }
        }
        //skip slave
        else if (UPS.get_role_mgr().is_master())
        {
          YYSYS_LOG(INFO,"wait for sync freeze");
          int64_t default_time_out = 3 * 1000 * 1000; //3s
          if (OB_SUCCESS == session_mgr_.wait_write_session_end_and_lock(default_time_out))
          {
            session_mgr_.unlock_write_session();
          }
          session_mgr_.disable_start_write_session();
          session_mgr_.lock_write_session_for_sync_freeze ();  //add peiouya [MultiUPS] [UPS_SYNC_Frozen] 20150811
        }
        //mod 20150615:e
      }
    }

    void *TransExecutor::on_commit_begin()
    {
      CommitParamData *ret = NULL;
      void *buffer = ob_malloc(sizeof(CommitParamData), ObModIds::OB_UPS_PARAM_DATA);
      if (NULL != buffer)
      {
        ret = new(buffer) CommitParamData();
        ret->buffer.set_data(ret->cbuffer, sizeof(ret->cbuffer));
      }
      return ret;
    }

    void TransExecutor::on_commit_end(void *ptr)
    {
      if (NULL != ptr)
      {
        ob_free(ptr);
        ptr = NULL;
      }
    }

    void TransExecutor::log_trans_info() const
    {
      YYSYS_LOG(INFO, "==========log trans executor start==========");
      YYSYS_LOG(INFO, "allocator info hold=%ld allocated=%ld", allocator_.hold(), allocator_.allocated());
      YYSYS_LOG(INFO, "session_mgr info flying session num ro=%ld rp=%ld rw=%ld",
                session_mgr_.get_flying_rosession_num(),
                session_mgr_.get_flying_rpsession_num(),
                session_mgr_.get_flying_rwsession_num());
      YYSYS_LOG(INFO, "queued_num trans_thread=%ld commit_thread=%ld",
                TransHandlePool::get_queued_num(),
                TransCommitThread::get_queued_num());
      YYSYS_LOG(INFO, "==========log trans executor end==========");
    }

    int &TransExecutor::thread_errno()
    {
      static __thread int thread_errno = OB_SUCCESS;
      return thread_errno;
    }

    int64_t &TransExecutor::batch_start_time()
    {
      static __thread int64_t batch_start_time = 0;
      return batch_start_time;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    void TransExecutor::phandle_non_impl(ObPacket &pkt, ObDataBuffer &buffer)
    {
      UNUSED(buffer);
      YYSYS_LOG(ERROR, "packet code=%d no implement phandler", pkt.get_packet_code());
      UPS.response_result(OB_NOT_IMPLEMENT, pkt);
    }

    void TransExecutor::phandle_freeze_memtable(ObPacket &pkt, ObDataBuffer &buffer)
    {
      UPS.ups_freeze_memtable(pkt.get_api_version(),
                              &pkt,
                              buffer,
                              pkt.get_packet_code());
    }

    void TransExecutor::phandle_clear_active_memtable(ObPacket &pkt, ObDataBuffer &buffer)
    {
      UNUSED(buffer);
      UPS.ups_clear_active_memtable(pkt.get_api_version(),
                                    pkt.get_request(),
                                    pkt.get_channel_id());
    }

    void TransExecutor::phandle_check_cur_version(ObPacket &pkt, ObDataBuffer &buffer)
    {
      UNUSED(pkt);
      UNUSED(buffer);
      UPS.ups_check_cur_version();
    }

//    void TransExecutor::phandle_check_sstable_checksum(ObPacket &pkt, ObDataBuffer &buffer)
//    {
//      UNUSED(pkt);
//      UNUSED(buffer);
//      UPS.ups_commit_check_sstable_checksum(*pkt.get_buffer());
//    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    bool TransExecutor::thandle_non_impl(TransExecutor &host, Task &task, TransParamData &pdata)
    {
      UNUSED(host);
      UNUSED(pdata);
      YYSYS_LOG(ERROR, "packet code=%d no implement thandler", task.pkt.get_packet_code());
      UPS.response_result(OB_NOT_IMPLEMENT, task.pkt);
      return true;
    }

    bool TransExecutor::thandle_commit_end(TransExecutor &host, Task &task, TransParamData &pdata)
    {
      pdata.buffer.get_position() = 0;
      host.handle_commit_end_(task, pdata.buffer);
      return true;
    }

    bool TransExecutor::thandle_scan_trans(TransExecutor &host, Task &task, TransParamData &pdata)
    {
      if (common::PACKET_RECORDER_FLAG)
      {
        host.fifo_stream_.push(&task.pkt);
      }
      pdata.buffer.get_position() = 0;

      //modify zhujun [transaction read uncommit]2016/3/29
      int64_t pos = task.pkt.get_buffer()->get_position();
      if(OB_SUCCESS != pdata.scan_param.deserialize(task.pkt.get_buffer()->get_data(),
                                                          task.pkt.get_buffer()->get_capacity(),
                                                          pos))
      {
          YYSYS_LOG(WARN, "deserialize scan_param fail");
      }
      //add by maosy [MultiUPS 1.0] [read uncommit]20170525 b:
      //else if (pdata.scan_param.get_trans_id().is_valid() &&
      else if (pdata.scan_param.get_trans_id(UPS.get_param().paxos_id).is_valid() &&
               NULL != host.session_mgr_.fetch_ctx<ROSessionCtx>(pdata.scan_param.get_trans_id(UPS.get_param().paxos_id).descriptor_))
      {
          host.session_mgr_.revert_ctx(pdata.scan_param.get_trans_id(UPS.get_param().paxos_id).descriptor_);
          // add by maosy e
          host.handle_scan_trans_in_one(task,pdata.scan_param,pdata.scanner,pdata.new_scanner,pdata.buffer);
      }
      else
      {
          host.handle_scan_trans_(task.pkt, pdata.scan_param, pdata.scanner, pdata.new_scanner, pdata.buffer);
      }
      //YYSYS_LOG(INFO, "get_param.get_trans_id():%s",to_cstring(pdata.scan_param.get_trans_id()));
      return true;
    }

    bool TransExecutor::thandle_get_trans(TransExecutor &host, Task &task, TransParamData &pdata)
    {
      if (common::PACKET_RECORDER_FLAG)
      {
        host.fifo_stream_.push(&task.pkt);
      }
      pdata.buffer.get_position() = 0;
      // modify zhujun [transaction read uncommit] 2016/3/22
      int64_t pos = task.pkt.get_buffer()->get_position();
      if(OB_SUCCESS != pdata.get_param.deserialize(task.pkt.get_buffer()->get_data(),
                                                          task.pkt.get_buffer()->get_capacity(),
                                                          pos))
      {
          YYSYS_LOG(WARN, "deserialize get_param fail");
      }
      //�ж�������Ƿ���Ч���Լ���Ӧ����ŵ�SESSION�Ƿ�ر�
      //add by maosy [MultiUPS 1.0] [read uncommit]20170525 b:
      //else if (pdata.get_param.get_trans_id().is_valid() &&
      else if(pdata.get_param.get_trans_id(UPS.get_param().paxos_id).is_valid() &&
              NULL != host.session_mgr_.fetch_ctx<ROSessionCtx>(pdata.get_param.get_trans_id(UPS.get_param().paxos_id).descriptor_))//�ж��Ƿ��Ѿ��ύ��,����sessioδ���ر�
      {
          host.session_mgr_.revert_ctx(pdata.get_param.get_trans_id(UPS.get_param().paxos_id).descriptor_);
          // add by maosy e
          host.handle_get_trans_in_one(task,pdata.get_param, pdata.scanner, pdata.new_scanner, pdata.buffer);

          //host.handle_get_trans_in_two(task,pdata.get_param,pdata.scanner,pdata.new_scanner,pdata.buffer);
      }
      else
      {
          host.handle_get_trans_(task.pkt, pdata.get_param, pdata.scanner, pdata.new_scanner, pdata.buffer);
      }
      //modify:e
      return true;
    }

    bool TransExecutor::thandle_write_trans(TransExecutor &host, Task &task, TransParamData &pdata)
    {
      bool ret = true;
      pdata.buffer.get_position() = 0;
      if (OB_PHY_PLAN_EXECUTE == task.pkt.get_packet_code())
      {
        ret = host.handle_phyplan_trans_(task, pdata.phy_operator_factory, pdata.phy_plan, pdata.new_scanner, pdata.allocator, pdata.buffer);
      }
      else
      {
        ret = host.handle_write_trans_(task, pdata.mutator, pdata.new_scanner);
      }
      return ret;
    }

    bool TransExecutor::thandle_start_session(TransExecutor &host, Task &task, TransParamData &pdata)
    {
      bool ret = true;
      pdata.buffer.get_position() = 0;
      ret = host.handle_start_session_(task, pdata.buffer);
      return ret;
    }

    bool TransExecutor::thandle_kill_zombie(TransExecutor &host, Task &task, TransParamData &pdata)
    {
      UNUSED(task);
      UNUSED(pdata);
      host.handle_kill_zombie_();
      return true;
    }

    bool TransExecutor::thandle_show_sessions(TransExecutor &host, Task &task, TransParamData &pdata)
    {
      pdata.buffer.get_position() = 0;
      host.handle_show_sessions_(task.pkt, pdata.new_scanner, pdata.buffer);
      return true;
    }

    bool TransExecutor::thandle_kill_session(TransExecutor &host, Task &task, TransParamData &pdata)
    {
      UNUSED(pdata);
      host.handle_kill_session_(task.pkt);
      return true;
    }

    bool TransExecutor::thandle_end_session(TransExecutor &host, Task &task, TransParamData &pdata)
    {
      pdata.buffer.get_position() = 0;
      return host.handle_end_session_(task, pdata.buffer);
    }
    ////////////////////////////////////////////////////////////////////////////////////////////////////

    bool TransExecutor::chandle_non_impl(TransExecutor &host, Task &task, CommitParamData &pdata)
    {
      UNUSED(host);
      UNUSED(pdata);
      YYSYS_LOG(ERROR, "packet code=%d no implement chandler", task.pkt.get_packet_code());
      UPS.response_result(OB_NOT_IMPLEMENT, task.pkt);
      return true;
    }

    bool TransExecutor::chandle_write_commit(TransExecutor &host, Task &task, CommitParamData &pdata)
    {
      UNUSED(pdata);
      int ret = host.handle_write_commit_(task);
      return (OB_SUCCESS != ret);
    }
    //add by maosy [MultiUps 1.0] [batch_iud_snapshot] 20170622 b:
    bool TransExecutor::thandle_get_publised_transid(TransExecutor &host, Task &task, TransParamData &pdata)
    {
        pdata.buffer.get_position() = 0;
        return host.handle_get_publised_transid_(task, pdata.buffer);
    }
    // add by e
    //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
    /*Э�������׶��ύ*/
    bool TransExecutor::thandle_end_trans(TransExecutor &host, Task &task, TransParamData &pdata)
    {
      pdata.buffer.get_position() = 0;
      return host.handle_end_trans_(task, pdata.buffer);
    }

    /*@berif �����ظ�ack*/
    bool TransExecutor::thandle_prepare_ack(TransExecutor &host, Task &task, TransParamData &pdata)
    {
      pdata.buffer.get_position() = 0;
      return host.handle_prepare_ack_(task, pdata.buffer);
    }

    bool TransExecutor::thandle_rollback_trans(TransExecutor &host, Task &task, TransParamData &pdata)
    {
      bool ret = true;
      pdata.buffer.get_position() = 0;
      ret = host.handle_rollback_trans_(task, pdata.buffer);
      return ret;
    }

    bool TransExecutor::thandle_commit_trans(TransExecutor &host, Task &task, TransParamData &pdata)
    {
      bool ret = true;
      pdata.buffer.get_position() = 0;
      ret = host.handle_commit_trans_(task, pdata.buffer);
      return ret;
    }

    bool TransExecutor::thandle_write_stat_ack(TransExecutor &host, Task &task, TransParamData &pdata)
    {
      pdata.buffer.get_position() = 0;
      return host.handle_write_stat_ack_(task, pdata.buffer);
    }

    bool TransExecutor::thandle_check_session_exist(TransExecutor &host, Task &task, TransParamData &pdata)
    {
      pdata.buffer.get_position() = 0;
      return host.handle_check_session_exist(task, pdata.buffer);
    }


    bool TransExecutor::thandle_update_partition_stat(TransExecutor &host, Task &task, TransParamData &pdata)
    {
        UNUSED(pdata);
        host.handle_update_partition_stat(task.pkt);
        return true;
    }

    /*@berif �����ֲ�ʽ�����prepare ����*/
    bool TransExecutor::chandle_write_prepare(TransExecutor &host, Task &task, CommitParamData &pdata)
    {
      UNUSED(pdata);
      //mod peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150731:b
      //int ret = host.handle_write_prepare_(task);
      bool prepare = true;
      int ret = host.handle_write_commit_(task, prepare);
      //mod 20150731:e
      return (OB_SUCCESS != ret);
    }

    bool TransExecutor::chandle_write_commit_trans(TransExecutor &host, Task &task, CommitParamData &pdata)
    {
      UNUSED(pdata);
      //mod peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150731:b
      //int ret = host.handle_write_commit_trans_(task);
      bool commit = true;
      int ret = host.handle_write_trans_end_(task,commit);
      //mod 20150731:e
      return (OB_SUCCESS != ret);
    }

    bool TransExecutor::chandle_write_rollback_trans(TransExecutor &host, Task &task, CommitParamData &pdata)
    {
      UNUSED(pdata);
      //mod peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150731:b
      //int ret = host.handle_write_rollback_trans_(task);
      bool commit = false;
      int ret = host.handle_write_trans_end_(task,commit);
      //mod 20150731:e
      return (OB_SUCCESS != ret);
    }
    //add 20150701:e

    bool TransExecutor::chandle_send_log(TransExecutor &host, Task &task, CommitParamData &pdata)
    {
      UNUSED(host);
      int ret = false;
      pdata.buffer.get_position() = 0;
      if(OB_SUCCESS != UPS.ups_slave_write_log(task.pkt.get_api_version(),
                              *(task.pkt.get_buffer()),
                              task.pkt.get_request(),
                              task.pkt.get_channel_id(),
                              //add chujiajia [Paxos ups_replication] 20160107:b
                              task.pkt.get_cmt_log_seq(),
                              //add:e
                              pdata.buffer))
      {
        ret = true;
      }
      //add wangdonghui [ups_log_replication_optimize] 20161009 :b
      else
      {
        task.pkt.set_receive_ts(yysys::CTimeUtil::getTime());
        //mod [mem leak]
        //UPS.ups_push_wait_flush(&task);
        int err = OB_SUCCESS;
        if(OB_SUCCESS != (err = UPS.ups_push_wait_flush(&task)))
        {
            YYSYS_LOG(WARN, "push wait_flush thread queue failed:ret[%d]", err);
            UPS.get_trans_executor().get_allocator()->free(&task);
        }
      }
      //add :e
      return ret;
    }

    bool TransExecutor::chandle_fake_write_for_keep_alive(TransExecutor &host, Task &task, CommitParamData &pdata)
    {
      UNUSED(host);
      UNUSED(task);
      UNUSED(pdata);
      UPS.ups_handle_fake_write_for_keep_alive();
      return true;
    }

    bool TransExecutor::chandle_slave_reg(TransExecutor &host, Task &task, CommitParamData &pdata)
    {
      UNUSED(host);
      pdata.buffer.get_position() = 0;
      UPS.ups_slave_register(task.pkt.get_api_version(),
                            *(task.pkt.get_buffer()),
                            task.pkt.get_request(),
                            task.pkt.get_channel_id(),
                            pdata.buffer);
      return true;
    }

   //add zhaoqiong [fixed for Backup]:20150811:b
    bool TransExecutor::chandle_backup_reg(TransExecutor &host, Task &task, CommitParamData &pdata)
    {
      UNUSED(host);
      pdata.buffer.get_position() = 0;
      UPS.ups_backup_register(task.pkt.get_api_version(),
                            *(task.pkt.get_buffer()),
                            task.pkt.get_request(),
                            task.pkt.get_channel_id(),
                            pdata.buffer);
      return true;
    }
	//add:e

    bool TransExecutor::chandle_check_sstable_checksum(TransExecutor &host, Task &task, CommitParamData &pdata)
    {
        UNUSED(host);
        UNUSED(pdata);
        UPS.ups_commit_check_sstable_checksum(*(task.pkt.get_buffer()));
        return true;
    }

    bool TransExecutor::chandle_switch_schema(TransExecutor &host, Task &task, CommitParamData &pdata)
    {
      UNUSED(host);
      UNUSED(pdata);
      UPS.ups_switch_schema(task.pkt.get_api_version(),
                            &(task.pkt),
                            *(task.pkt.get_buffer()));
      return true;
    }

    //add zhaoqiong [Schema Manager] 20150327:b
    bool TransExecutor::chandle_switch_schema_mutator(TransExecutor &host, Task &task, CommitParamData &pdata)
    {
      UNUSED(host);
      UNUSED(pdata);
      UPS.ups_switch_schema_mutator(task.pkt.get_api_version(),
                            &(task.pkt),
                            *(task.pkt.get_buffer()));
      return true;
    }
    bool TransExecutor::chandle_switch_tmp_schema(TransExecutor &host, Task &task, CommitParamData &pdata)
    {
      UNUSED(host);
      UNUSED(pdata);
      UNUSED(task);
      UPS.ups_switch_tmp_schema();
      return true;
    }

    //add:e

    bool TransExecutor::chandle_force_fetch_schema(TransExecutor &host, Task &task, CommitParamData &pdata)
    {
      UNUSED(host);
      UNUSED(pdata);
      UPS.ups_force_fetch_schema(task.pkt.get_api_version(),
                                 task.pkt.get_request(),
                                 task.pkt.get_channel_id());
      return true;
    }

    bool TransExecutor::chandle_switch_commit_log(TransExecutor &host, Task &task, CommitParamData &pdata)
    {
      UNUSED(host);
      pdata.buffer.get_position() = 0;
      UPS.ups_switch_commit_log(task.pkt.get_api_version(),
                                task.pkt.get_request(),
                                task.pkt.get_channel_id(),
                                pdata.buffer);
      return true;
    }

    bool TransExecutor::chandle_nop(TransExecutor &host, Task &task, CommitParamData &pdata)
    {
      UNUSED(host);
      UNUSED(task);
      UNUSED(pdata);
      //YYSYS_LOG(INFO, "handle nop");
      return false;
    }
    //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
    int handle_unsettled_trans(const void *arg)
    {
      int err = OB_SUCCESS;
      int ret = OB_SUCCESS;
      TransExecutor& executor = UPS.get_trans_executor();
      //const common::ObTransID* trans_id = (const common::ObTransID*)(arg);
      common::ObTransID* trans_id = const_cast<common::ObTransID*>((const common::ObTransID*)(arg));
      SessionGuard session_guard(executor.get_session_mgr(),executor.get_lock_mgr(),ret);
      RWSessionCtx *ctx = NULL;
      if (!trans_id->is_valid())
      {
        err = OB_ENTRY_NOT_EXIST;
        YYSYS_LOG(WARN, "invalid session(%s)", to_cstring(*trans_id));
      }
      else if (OB_SUCCESS != (err = session_guard.fetch_session(*trans_id, ctx)))
      {
        session_guard.revert();
        YYSYS_LOG(WARN, "fetch_session(%s)=>%d, maybe already end", to_cstring(*trans_id), err);
        err = OB_ENTRY_NOT_EXIST;
      }
      else if(trans_id->is_unset_trans_)
      {
          ctx->set_writting_trans_stat(false);
          trans_id->is_unset_trans_ = false;
      }
      if(OB_SUCCESS != err)
      {}
      else
      {
          ctx->set_can_be_killed(false);//[521]
          if (ctx->get_task() != NULL)
          {
              //coordinator
              bool commit = false;
              if(ctx->all_participant_ack())
              {
                  YYSYS_LOG(DEBUG, "enter coodinator, and receive all ack. commit");
                  commit = true;
              }
              else
              {
                  YYSYS_LOG(WARN, "trans is timeout, coodinator[%s], will rollback", to_cstring(*trans_id));
              }

              ctx->set_accept_prepare_ack(false);
              session_guard.revert();
              if(ctx->get_write_trans_stat_time() != 0 &&
                      yysys::CTimeUtil::getTime() - ctx->get_write_trans_stat_time() >
                      UPS.get_param().fetch_trans_stat_wait_time)
              {
                  YYSYS_LOG(WARN, "already write stat, need wait for write timeout");
                  //add wangyao [284]
                  if(!ctx->get_writting_trans_stat())
                  {
                      executor.end_sub_trans(*ctx, *trans_id, commit);
                  }
              }
              else if (OB_SUCCESS != (err = executor.write_trans_stat(*ctx, *trans_id,commit)))
              {
                  YYSYS_LOG(ERROR, "write trans stat:%d failed, ret=%d", commit, err);
              }
          }
          else
          {
              //participant
              session_guard.revert();
              if (ctx->get_writting_trans_stat())
              {
                  YYSYS_LOG(WARN, "already writteing trans stat");
              }
              else if(OB_SUCCESS != (err = executor.handle_unsettled_participant_trans(*ctx,*trans_id)))
              {
                  YYSYS_LOG(WARN, "handle unsetled participant trans faild, err:%d", err);
              }
          }
      }
      return err;
    }

    int handle_unsettled_trans_for_replay(const void *arg)
    {
      int err = OB_SUCCESS;
      int ret = OB_SUCCESS;
      TransExecutor& executor = ObUpdateServerMain::get_instance()->get_update_server().get_trans_executor();
      const common::ObTransID* trans_id = (const common::ObTransID*)(arg);
      SessionGuard session_guard(executor.get_session_mgr(),executor.get_lock_mgr(),ret);
      RWSessionCtx *ctx = NULL;
      bool commit = false;
      bool find_trans_stat = false;
      if (!trans_id->is_valid())
      {
        err = OB_ENTRY_NOT_EXIST;
        YYSYS_LOG(WARN, "invalid session(%s)", to_cstring(*trans_id));
      }
      else if (OB_SUCCESS != (err = session_guard.fetch_session(*trans_id, ctx)))
      {
        session_guard.revert();
        YYSYS_LOG(WARN, "fetch_session(%s)=>%d, maybe already end", to_cstring(*trans_id), err);
        err = OB_ENTRY_NOT_EXIST;
      }
      //TODO
      //else if(ctx->get_coordinator_info().ups_ == ObUpdateServerMain::get_instance()->get_update_server().get_self())
      else if((ctx->get_coordinator_info().dis_trans_role_ == COORDINATOR) && (UPS.get_role_mgr().get_role() == ObUpsRoleMgr::MASTER))
      {
        //coordinator
        session_guard.revert();
        //if fetch from sys table fail, wait next process
        //else if get nothing from sys table, rollback
        //else commit/rollback self
        if(OB_SUCCESS != (err = executor.fetch_trans_stat(ctx->get_coordinator_info(),find_trans_stat,commit)))
        {
          //OB_ERR_EXCLUSIVE_LOCK_CONFLICT, wait
          YYSYS_LOG(WARN, "fetch trans stat failed, find: %d, commit: %d", find_trans_stat, commit);
        }
        //else if(!find_trans_stat)
        else if(!find_trans_stat && ctx->get_coordinator_info().descriptor_ != ObTransID::INVALID_SESSION_ID)
        {
          //get nothing from sys table, rollback
          YYSYS_LOG(WARN, "not find trans stat, write rollback to sys table, trans_id=%s", to_cstring(ctx->get_coordinator_info()));
          commit = false;
          if (OB_SUCCESS != (err = executor.write_trans_stat(*ctx, ctx->get_coordinator_info(),commit)))
          {
            YYSYS_LOG(ERROR, "write trans stat:%d failed, ret=%d", commit, err);
          }
        }
        else if(!ctx->get_writting_trans_stat() && ctx->get_coordinator_info().descriptor_ != ObTransID::INVALID_SESSION_ID)
        {
          executor.end_sub_trans(*ctx, *trans_id, commit);
        }
      }
      else
      {
        //participant
        session_guard.revert();
        if (ctx->get_writting_trans_stat())
        {
          YYSYS_LOG(WARN, "already writteing trans stat");
        }
        else if(OB_SUCCESS != (err = executor.handle_unsettled_participant_trans(*ctx,*trans_id)))
        {
          YYSYS_LOG(WARN, "handle unsetled participant trans faild, err:%d", err);
        }
      }
      return err;
    }
    //add 20150701:e

  //add chujiajia [Paxos ups_replication] 20160113:b
    void TransExecutor::disable_fake_write_granted()
    {
      FakeWriteGuard guard(session_mgr_);
      guard.set_fake_write_granted(false);
    }
    void TransExecutor::enable_fake_write_granted()
    {
      FakeWriteGuard guard(session_mgr_);
      guard.set_fake_write_granted(true);
    }
    //add:e
  }
}
