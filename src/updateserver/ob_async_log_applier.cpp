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
 *   yuanqi <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 */
#include "ob_async_log_applier.h"
#include "ob_ups_log_mgr.h"
#include "ob_lock_mgr.h"
#include "ob_trans_executor.h"
#include "ob_session_guard.h"
#include "ob_ups_utils.h"

namespace oceanbase
{
  namespace updateserver
  {
    int64_t ReplayTaskProfile::to_string(char* buf, int64_t len) const
    {
      return snprintf(buf, len, "log: %ld[%s], submit: %ld, aqueue: %ld, apply: %ld, cqueue: %ld, commit: %ld, flush: %ld",
                      host_.log_id_, ::to_cstring(host_.trans_id_),
                      submit_time_us_, start_apply_time_us_ - submit_time_us_, end_apply_time_us_ - start_apply_time_us_,
                      start_commit_time_us_ - end_apply_time_us_, end_commit_time_us_ - start_commit_time_us_,
                      end_flush_time_us_ - start_flush_time_us_);
    }

    ObLogTask::ObLogTask(): log_id_(0), barrier_log_id_(0),
                            log_entry_(), log_data_(NULL), batch_buf_(NULL), batch_buf_len_(0),
                            trans_id_(), mutation_ts_(0),
                            checksum_before_mutate_(0), checksum_after_mutate_(0),
                            replay_type_(RT_LOCAL), profile_(*this)
    {
    }

    ObLogTask::~ObLogTask()
    {}

    void ObLogTask::reset()
    {
      log_id_ = 0;
      barrier_log_id_ = 0;
      new(&log_entry_)ObLogEntry;
      log_data_ = NULL;
      batch_buf_ = NULL;
      batch_buf_len_ = 0;
      trans_id_.reset();
      mutation_ts_ = 0;
      checksum_before_mutate_ = 0;
      checksum_after_mutate_ = 0;
      replay_type_ = RT_LOCAL;
      new(&profile_)ReplayTaskProfile(*this);
      //add shili [LONG_TRANSACTION_LOG]  20160926:b
      is_big_log_completed_ = false;
      //add e
    }

    ObAsyncLogApplier::ObAsyncLogApplier(): inited_(false),
                                            retry_wait_time_us_(REPLAY_RETRY_WAIT_TIME_US),
                                            trans_executor_(NULL), session_mgr_(NULL),
                                            lock_mgr_(NULL), table_mgr_(NULL), log_mgr_(NULL)
    {
    }

    ObAsyncLogApplier::~ObAsyncLogApplier()
    //mod peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150818:b
    //{}
    {task_map_.destroy();} 
    //mod 20150818:e

    int ObAsyncLogApplier::init(TransExecutor* trans_executor, SessionMgr* session_mgr,
                                LockMgr* lock_mgr, ObUpsTableMgr* table_mgr, ObUpsLogMgr* log_mgr)
    {
      int err = OB_SUCCESS;
      if (NULL == trans_executor || NULL == session_mgr || NULL == lock_mgr || NULL == table_mgr || NULL == log_mgr)
      {
        err = OB_INVALID_ARGUMENT;
      }
      else if (NULL != trans_executor_ || NULL != session_mgr_ || NULL != lock_mgr_ || NULL != table_mgr_ || NULL != log_mgr_)
      {
        err = OB_INIT_TWICE;
      }
      //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150818:b
      else if (OB_SUCCESS != (err = task_map_.create(TASK_NUM)))
      {
        YYSYS_LOG(ERROR, "create hashmap fail, err = %d", err);
      }
      //add 20150818:e
      else
      {
        trans_executor_ = trans_executor;
        session_mgr_ = session_mgr;
        lock_mgr_ = lock_mgr;
        table_mgr_ = table_mgr;
        log_mgr_ = log_mgr;
        inited_ = true;
      }
      return err;
    }

    bool ObAsyncLogApplier::is_inited() const
    {
      return inited_;
    }

    int ObAsyncLogApplier::on_submit(ObLogTask& task)
    {
      int err = OB_SUCCESS;
      task.profile_.on_submit();
      return err;
    }

    int ObAsyncLogApplier::start_transaction(ObLogTask& task)
    {
      int err = OB_SUCCESS;
      UNUSED(task);
      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      else
      {}
      return err;
    }


    //add shili [LONG_TRANSACTION_LOG]  20160926:b
    /* @berif  ��buf �����л���mutator,����mutator Ӧ�õ� memtable ��
     * @param    buf   [in]  mutator �����л�֮ǰ���ֽ���
     * @param    data_size  [in] mutator �����л�֮ǰ���ֽ�������
     * @param    task  [out] ���������ύʱ ����
     */
    int ObAsyncLogApplier::handle_normal_mutator(const char *buf, const int32_t data_size, ObLogTask &task)
    {
      int err = OB_SUCCESS;
      int64_t pos = 0;
      RPSessionCtx *session_ctx = NULL;
      SessionGuard session_guard(*session_mgr_, *lock_mgr_, err);
      ObTransReq req;
      req.type_ = REPLAY_TRANS;
      req.isolation_ = NO_LOCK;
      req.start_time_ = yysys::CTimeUtil::getTime();
      req.timeout_ = INT64_MAX;
      req.idle_time_ = INT64_MAX;
      task.trans_id_.reset();

      if (OB_SUCCESS != (err = session_guard.start_session(req, task.trans_id_, session_ctx)))
      {
        if (OB_BEGIN_TRANS_LOCKED == err)
        {
          err = OB_EAGAIN;
          YYSYS_LOG(TRACE, "begin session fail: TRANS_LOCKED, log_id=%ld", task.log_id_);
        }
        else
        {
          YYSYS_LOG(WARN, "begin session fail ret=%d, log_id=%ld", err, task.log_id_);
        }
      }
      else if (OB_SUCCESS != (err = session_ctx->get_ups_mutator().deserialize(buf,(int64_t)data_size, pos)))
      {
        YYSYS_LOG(ERROR, "ups_mutator.deserialize(buf[%p:%d])", buf, data_size);
      }
      else
      {
        const bool using_id = true;
        ObUpsMutator& mutator = session_ctx->get_ups_mutator();
        task.mutation_ts_ = mutator.get_mutate_timestamp();
        task.checksum_before_mutate_ = mutator.get_memtable_checksum_before_mutate();
        task.checksum_after_mutate_ = mutator.get_memtable_checksum_after_mutate();
        session_ctx->set_replay_local_log(RT_LOCAL == task.replay_type_);
        session_ctx->set_trans_id(mutator.get_mutate_timestamp());
        tc_is_replaying_log() = true;
        if (OB_SUCCESS != (err = table_mgr_->apply(using_id, *session_ctx,
                                                   *session_ctx->get_lock_info(),
                                                   mutator.get_mutator())))
        {
          YYSYS_LOG(DEBUG, "apply err=%d trans_id=%s, checksum=%lu",
                    err, to_cstring(task.trans_id_), session_ctx->get_uc_info().uc_checksum);
        }
        else
        {
          int64_t mutate_ts = mutator.get_mutate_timestamp();
          session_ctx->get_uc_info().uc_checksum = ob_crc64(session_ctx->get_uc_info().uc_checksum,
                                                            &mutate_ts, sizeof(mutate_ts));
        }
        tc_is_replaying_log() = false;
      }
      return err;
    }
    //add e
    //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
    //int ObAsyncLogApplier::handle_normal_mutator(ObLogTask& task)
    int ObAsyncLogApplier::handle_normal_mutator(ObLogTask& task, int64_t update_pos, ObTransID* trans_id)
    //mod 20150701:e
    {
      int err = OB_SUCCESS;
      const char* log_data = task.log_data_;
      const int64_t data_len = task.log_entry_.get_log_data_len();
      //mod peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
      //int64_t pos = 0;
      int64_t pos = update_pos;
      RPSessionCtx *session_ctx = NULL;
      SessionGuard session_guard(*session_mgr_, *lock_mgr_, err);
      ObTransReq req;
      req.type_ = REPLAY_TRANS;
      req.isolation_ = NO_LOCK;
      req.start_time_ = yysys::CTimeUtil::getTime(); 
      req.timeout_ = INT64_MAX;
      req.idle_time_ = INT64_MAX;
      task.trans_id_.reset();

      if (OB_SUCCESS != (err = session_guard.start_session(req, task.trans_id_, session_ctx)))
      {
        if (OB_BEGIN_TRANS_LOCKED == err)
        {
          err = OB_EAGAIN;
          YYSYS_LOG(TRACE, "begin session fail: TRANS_LOCKED, log_id=%ld", task.log_id_);
        }
        else
        {
          YYSYS_LOG(WARN, "begin session fail ret=%d, log_id=%ld", err, task.log_id_);
        }
      }
      else if (OB_SUCCESS != (err = session_ctx->get_ups_mutator().deserialize(log_data, data_len, pos)))
      {
        YYSYS_LOG(ERROR, "ups_mutator.deserialize(buf[%p:%ld])", log_data, data_len);
      }
      else
      {
        const bool using_id = true;
        ObUpsMutator& mutator = session_ctx->get_ups_mutator();
        task.mutation_ts_ = mutator.get_mutate_timestamp();
        task.checksum_before_mutate_ = mutator.get_memtable_checksum_before_mutate();
        task.checksum_after_mutate_ = mutator.get_memtable_checksum_after_mutate();
        session_ctx->set_replay_local_log(RT_LOCAL == task.replay_type_);
        session_ctx->set_trans_id(mutator.get_mutate_timestamp());
        tc_is_replaying_log() = true;
        if (OB_SUCCESS != (err = table_mgr_->apply(using_id, *session_ctx,
                                                   *session_ctx->get_lock_info(),
                                                   mutator.get_mutator())))
        {
          YYSYS_LOG(DEBUG, "apply err=%d trans_id=%s, checksum=%lu",
                    err, to_cstring(task.trans_id_), session_ctx->get_uc_info().uc_checksum);
          //add duyr [MultiUPS] [READ_ATOMIC] [write_part] 20151117:b
          session_ctx->rollback_prepare_trans_ver();
          //add duyr 20151117:e
        }
        else
        {
          //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
          if(NULL != trans_id)
          {
            //prepare trans, need set coordinator
            session_ctx->set_coordinator_info(*trans_id);
            //add duyr [MultiUPS] [READ_ATOMIC] [write_part] 20151117:b
            err = session_ctx->backfill_trans_version();
            if (OB_SUCCESS != err)
            {
              YYSYS_LOG(WARN,"back fill distr trans version on replay log fail!ret=%d",err);
            }
            if (NULL != trans_id)
            {
              YYSYS_LOG(DEBUG,"read_atomic::debug,finished log replay prepare's mutator!"
                        "need_gen_mutator=%d,"
                        "cur_session_des=%d,"
                        "cur_sid=[%s],"
                        "coor_sid=[%s]",
                        session_ctx->get_need_gen_mutator(),
                        session_ctx->get_session_descriptor(),
                        to_cstring(task.trans_id_),
                        to_cstring(*trans_id));
            }
            //add duyr 20151117:e
          }
          //add 20150701:e
          int64_t mutate_ts = mutator.get_mutate_timestamp();
          session_ctx->get_uc_info().uc_checksum = ob_crc64(session_ctx->get_uc_info().uc_checksum,
                                                            &mutate_ts, sizeof(mutate_ts));
        }
        tc_is_replaying_log() = false;
      }
      return err;
    }
    //mod peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150818:b
    int ObAsyncLogApplier::handle_part_trans_commit(ObLogTask& task)
    {
      int err = OB_SUCCESS;
      const char* log_data = task.log_data_;
      const int64_t data_len = task.log_entry_.get_log_data_len();
      int64_t pos = 0;

      PartCommitInfo commit_info;
      common::ObTransID trans_id;
      if (OB_SUCCESS != (err = commit_info.deserialize(log_data, data_len, pos)))
      {
        YYSYS_LOG(ERROR, "commit_info.deserialize(buf[%p:%ld])", log_data, data_len);
      }
      else
      {
        //add peiouya [REPLAY_CHECKSUM_ERROR] 20150312:b
        RPSessionCtx *session_ctx = NULL;
        SessionGuard session_guard(*session_mgr_, *lock_mgr_, err);
        //add 20160312:e
        task.mutation_ts_ = commit_info.mutate_timestamp_;
        task.checksum_before_mutate_ = commit_info.memtable_checksum_before_mutate_;
        task.checksum_after_mutate_ = commit_info.memtable_checksum_after_mutate_;
//        if (hash::HASH_EXIST != (err = task_map_.get(commit_info.coordinator_, trans_id)))
//        {
//          //maybe select for update, have no prepare log
//          YYSYS_LOG(WARN, "get prepare task fail, ret = %d", err);
//          err = OB_SUCCESS;
//        }
//        else


        //[629]
        int64_t now_time = yysys::CTimeUtil::getTime();
        while(hash::HASH_EXIST != (err = task_map_.get(commit_info.coordinator_, trans_id)))
        {
            if(yysys::CTimeUtil::getTime() - now_time > 6*1000*1000)
            {
                YYSYS_LOG(WARN, "get prepare task fail. need wait.now time:%ld dis trans id:%s",
                          now_time, to_cstring(commit_info.coordinator_));
            }
            //add support for arm platform by qiuhm 202109:b
#if defined(__aarch64__)
            asm("yield");
#else
//add support for arm platform by qiuhm 202109:e
            asm("pause");
#endif //add support for arm platform by qiuhm 202109
        }

        if (ObTransID::INVALID_SESSION_ID == trans_id.descriptor_)
        {
          YYSYS_LOG(ERROR, "get prepare task trans id ERROR, replay_trans_id = %d", trans_id.descriptor_);
          err = OB_ERROR;
        }
        //add peiouya [REPLAY_CHECKSUM_ERROR] 20150312:b
        else if (OB_SUCCESS != (err = session_guard.fetch_session(trans_id, session_ctx)))
        {
          YYSYS_LOG(WARN, "fetch_session(%s)=>%d", to_cstring(trans_id), err);
        }
        //add 20160312:e
        else
        {
          err = OB_SUCCESS;
          session_ctx->set_trans_id(commit_info.mutate_timestamp_);  //add peiouya [REPLAY_CHECKSUM_ERROR] 20150312
          task.trans_id_ = trans_id;
          task_map_.erase(commit_info.coordinator_);
        }
      }
      return err;
    }

    int ObAsyncLogApplier::handle_part_trans_rollback(ObLogTask& task)
    {
      int err = OB_SUCCESS;
      const char* log_data = task.log_data_;
      const int64_t data_len = task.log_entry_.get_log_data_len();
      int64_t pos = 0;

      PartCommitInfo commit_info;
      common::ObTransID trans_id;

      if (OB_SUCCESS != (err = commit_info.deserialize(log_data, data_len, pos)))
      {
        YYSYS_LOG(ERROR, "commit_info.deserialize(buf[%p:%ld])", log_data, data_len);
      }
      else
      {
          int64_t now_time = yysys::CTimeUtil::getTime();
          while(hash::HASH_EXIST != (err = task_map_.get(commit_info.coordinator_, trans_id)))
          {
              if(yysys::CTimeUtil::getTime() - now_time > 6*1000*1000)
              {
                  YYSYS_LOG(WARN, "get prepare task fail. need wait.now time:%ld dis trans id:%s",
                            now_time, to_cstring(commit_info.coordinator_));
              }
              //add support for arm platform by qiuhm 202109:b
#if defined(__aarch64__)
            asm("yield");
#else
//add support for arm platform by qiuhm 202109:e
            asm("pause");
#endif //add support for arm platform by qiuhm 202109
          }
          //       else  if (hash::HASH_EXIST != (err = task_map_.get(commit_info.coordinator_, trans_id)))
          //      {
          //        //maybe select for update, have no prepare log
          //        YYSYS_LOG(WARN, "get prepare task fail, ret = %d", err);
          //        err = OB_SUCCESS;
          //      }
          if (ObTransID::INVALID_SESSION_ID == trans_id.descriptor_)
          {
              YYSYS_LOG(ERROR, "get prepare task trans id ERROR, replay_trans_id = %d", trans_id.descriptor_);
              err = OB_ERROR;
          }
          else if (OB_SUCCESS != (err = session_mgr_->end_session(trans_id.descriptor_)))
          {
              YYSYS_LOG(ERROR, "end_session(trans_id :%d)=>%d", trans_id.descriptor_, err);
          }
          else
          {
              task_map_.erase(commit_info.coordinator_);
          }
      }
      return err;
    }

    int ObAsyncLogApplier::handle_unsettled_trans()
    {
      int ret = OB_SUCCESS;
      common::hash::ObHashMap<common::ObTransID,common::ObTransID>::const_iterator iter = task_map_.begin();
      while(task_map_.size() > 0)
      {
        YYSYS_LOG(INFO, "replay finished, has unsettlted trans, count:%ld",task_map_.size());
        for(iter = task_map_.begin(); iter != task_map_.end(); )
        {
          ret = handle_unsettled_trans_for_replay(&(iter->second));
          if(OB_ENTRY_NOT_EXIST == ret)
          {
            YYSYS_LOG(WARN,"erase node %s", to_cstring(iter->first));
            task_map_.erase((iter++)->first);
          }
          else
          {
            ++iter;
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

      YYSYS_LOG(INFO, "replay finished, has no unsettlted trans");
      return ret;
    }
    //add 20150818:e

    bool ObAsyncLogApplier::is_memory_warning()
    {
      bool bret = false;
      TableMemInfo mi;
      table_mgr_->get_memtable_memory_info(mi);
      int64_t memory_available = mi.memtable_limit - mi.memtable_total + (MemTank::REPLAY_MEMTABLE_RESERVE_SIZE_GB << 30);
      if (get_table_available_warn_size() > memory_available)
      {
        ups_available_memory_warn_callback(memory_available);
        if (memory_available <= 0)
        {
          bret = true;
        }
      }
      return bret;
    }

    int ObAsyncLogApplier::apply(ObLogTask& task)
    {
      int err = OB_SUCCESS;
      ObUpsMutator *mutator = GET_TSI_MULT(ObUpsMutator, 1);
      CommonSchemaManagerWrapper *schema = GET_TSI_MULT(CommonSchemaManagerWrapper, 1);
      const char* log_data = task.log_data_;
      const int64_t data_len = task.log_entry_.get_log_data_len();
      LogCommand cmd = (LogCommand)task.log_entry_.cmd_;
      int64_t pos = 0;
      int64_t file_id = 0;
      task.profile_.start_apply();
      task.mutation_ts_ = yysys::CTimeUtil::getTime();
      ObBigLogWriter * big_log_writer = log_mgr_->get_big_log_writer();//add shili [LONG_TRANSACTION_LOG]  20160926
      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      else if (NULL == mutator || NULL == schema)
      {
        err = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (is_memory_warning())
      {
        if (TC_REACH_TIME_INTERVAL(MEM_OVERFLOW_REPORT_INTERVAL))
        {
          YYSYS_LOG(WARN, "no memory, sleep %ldus wait for dumping sstable", retry_wait_time_us_);
        }
        err = OB_EAGAIN;
        usleep(static_cast<useconds_t>(retry_wait_time_us_));
      }
      else
      {
        //add zhaoqiong [Schema Manager] 20150327:b
        //schema from TSI, the attributes are not the default values
        //should reset first
        schema->reset();
        ObSchemaMutator schema_mutator;
        //add:e
        common::ObTransID coordinator_tid;//add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701

        switch(cmd)
        {
          //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
          case OB_UPS_MUTATOR_PREPARE:
            //normal branch: slave ups replay master log
            if (OB_SUCCESS != (err = coordinator_tid.deserialize_for_prepare(log_data, data_len, pos)))
            {
              YYSYS_LOG(ERROR, "coordinator deserialize error[ret=%d log_data=%p data_len=%ld]", err, log_data, data_len);
            }
            else
            {
              YYSYS_LOG(DEBUG,"PREPARE LOG, coordinator_tid:%s", to_cstring(coordinator_tid));
              if (OB_SUCCESS != (err = handle_normal_mutator(task, pos, &coordinator_tid)))
              {
                YYSYS_LOG(WARN, "fail to handle normal mutator. err=%d", err);
              }
              //mod peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150818:b
              else if(hash::HASH_INSERT_SUCC != (err = task_map_.set(coordinator_tid, task.trans_id_)))
              {
                YYSYS_LOG(ERROR, "save prepare task fail, ret = %d", err);
                err = OB_ERROR;
              }
              else
              {
                err = OB_SUCCESS;
                YYSYS_LOG(DEBUG, "trans_id: %s", to_cstring(coordinator_tid));
              }
              //add 20150818:e
            }
            break;
          //add 20150701:e
          case OB_LOG_UPS_MUTATOR:
            if (OB_SUCCESS != (err = mutator->deserialize_header(log_data, data_len, pos)))
            {
              YYSYS_LOG(ERROR, "UpsMutator deserialize error[ret=%d log_data=%p data_len=%ld]", err, log_data, data_len);
            }
            else if (mutator->is_normal_mutator() || mutator->is_truncate_mutator())
            {
              if (OB_SUCCESS != (err = handle_normal_mutator(task)))
              {
                YYSYS_LOG(WARN, "fail to handle normal mutator. err=%d", err);
              }
            }
            else
            {
              pos = 0;
              if (OB_SUCCESS != (err = mutator->deserialize(log_data, data_len, pos)))
              {
                YYSYS_LOG(ERROR, "mutator->deserialize(buf=%p[%ld])", log_data, data_len);
              }
              else if (OB_SUCCESS != (err = table_mgr_->replay_mgt_mutator(*mutator, task.replay_type_)))
              {
                YYSYS_LOG(ERROR, "replay_mgt_mutator(replay_type=%d)=>%d", task.replay_type_, err);
              }
            }
            if (OB_SUCCESS != err)
            {
              YYSYS_LOG(WARN, "table_mgr->replay(log_cmd=%d, data=%p[%ld])=>%d", cmd, log_data, data_len, err);
            }
            break;
          case OB_UPS_SWITCH_SCHEMA:
            if (OB_SUCCESS != (err = schema->deserialize(log_data, data_len, pos)))
            {
              YYSYS_LOG(ERROR, "ObSchemaManagerWrapper deserialize error[ret=%d log_data=%p data_len=%ld]",
                        err, log_data, data_len);
            }
            else if (OB_SUCCESS != (err = table_mgr_->set_schemas(*schema)))
            {
              YYSYS_LOG(ERROR, "UpsTableMgr set_schemas error, ret=%d schema_version=%ld", err, schema->get_version());
            }
            else
            {
              YYSYS_LOG(INFO, "switch schema succ");
            }
            break;
          //add zhaoqiong [Schema Manager] 20150327:b
          case OB_UPS_WRITE_SCHEMA_NEXT:
            if (OB_SUCCESS != (err = table_mgr_->replay_mgt_next(log_data, data_len, pos, task.replay_type_)))
            {
              YYSYS_LOG(ERROR, "UpsTableMgr deserialize_schema_next error");
            }
            else
            {
             YYSYS_LOG(INFO, "deserialize_schema_next succ");
            }
            break;
          case OB_UPS_SWITCH_SCHEMA_NEXT:
            if (OB_SUCCESS != (err = table_mgr_->set_schema_next(log_data, data_len, pos)))
            {
              YYSYS_LOG(ERROR, "UpsTableMgr deserialize_schema_next error");
            }
            else
            {
              YYSYS_LOG(INFO, "deserialize_schema_next succ");
            }
            break;
          case OB_UPS_SWITCH_SCHEMA_MUTATOR:
            if (OB_SUCCESS != (err = schema_mutator.deserialize(log_data, data_len, pos)))
            {
             YYSYS_LOG(ERROR, "ObSchemaMutator deserialize error[ret=%d log_data=%p data_len=%ld]",
                      err, log_data, data_len);
            }
            else if (OB_SUCCESS != (err = table_mgr_->get_schema_mgr().apply_schema_mutator(schema_mutator)))
            {
              YYSYS_LOG(ERROR, "UpsTableMgr apply_schema_mutator error, ret=%d schema_mutator version=%ld", err, schema_mutator.get_end_version());
            }
            else
            {
              YYSYS_LOG(INFO, "apply schema mutator succ");
            }
            break;
          //add:e
          // add shili [LONG_TRANSACTION_LOG]  20160926:b
          case OB_UPS_BIG_LOG_DATA:
            YYSYS_LOG(DEBUG, "OB_UPS_BIG_LOG_DATA");
            if(big_log_writer == NULL)
            {
              err = OB_ERR_NULL_POINTER;
              YYSYS_LOG(ERROR, "big log writer is NULL,err=%d", err);
            }
            else
            {
              big_log_writer->set_log_applier(this);
              if(OB_SUCCESS != (err = big_log_writer->handle_big_log(task)))
              {
                YYSYS_LOG(WARN, "big log writer package big log fail,,err=%d", err);
              }
            }
            break;
          //add e
          case OB_LOG_SWITCH_LOG:
            if (OB_SUCCESS != (err = serialization::decode_i64(log_data, data_len, pos, (int64_t*)&file_id)))
            {
              YYSYS_LOG(ERROR, "decode_i64 log_id error, err=%d", err);
            }
            else
            {
              pos = data_len;
              YYSYS_LOG(INFO, "replay log: SWITCH_LOG, file_id=%ld", file_id);
            }
            break;
          //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
          case OB_PART_TRANS_COMMIT:
            if(OB_SUCCESS != (err = handle_part_trans_commit(task)))
            {
             YYSYS_LOG(WARN, "fail to handle part trans commit err=%d", err);
            }
            break;
          case OB_PART_TRANS_ROLLBACK:
            if(OB_SUCCESS != (err = handle_part_trans_rollback(task)))
            {
              YYSYS_LOG(WARN, "fail to handle part trans rollback err=%d", err);
            }
            pos = data_len;
            break;
          //add 20150701:e
          case OB_LOG_NOP:
            pos = data_len;
            break;
          default:
            err = OB_ERROR;
            break;
        }
        if (OB_MEM_OVERFLOW == err
            || OB_ALLOCATE_MEMORY_FAILED == err)
        {
          err = OB_EAGAIN;
          //YYSYS_LOG(WARN, "memory overflow, need retry");
        }
        if (OB_SUCCESS != err && OB_EAGAIN != err)
        {
          YYSYS_LOG(ERROR, "replay_log(cmd=%d, log_data=%p[%ld], %s)=>%d", 
                    cmd, log_data, data_len, to_cstring(task), err);
        }
      }
      task.profile_.end_apply();
      return err;
    }

    int ObAsyncLogApplier::add_memtable_uncommited_checksum_(const uint32_t session_descriptor, uint64_t *ret_checksum)
    {
      int ret = OB_SUCCESS;
      RPSessionCtx *session = session_mgr_->fetch_ctx<RPSessionCtx>(session_descriptor);
      if (NULL == session)
      {
        YYSYS_LOG(WARN, "fetch session ctx fail sd=%u",  session_descriptor);
        ret = OB_ERROR;
      }
      else
      {
        ret = table_mgr_->add_memtable_uncommited_checksum(&(session->get_uc_info().uc_checksum));
        if (OB_SUCCESS != ret)
        {
          YYSYS_LOG(WARN, "add_memtable_uncommited_checksum fail ret=%d", ret);
        }
        else
        {
          if (NULL != ret_checksum)
          {
            *ret_checksum = session->get_uc_info().uc_checksum;
            (*ret_checksum) ^= session->get_checksum();
          }
        }
        session_mgr_->revert_ctx(session_descriptor);
      }
      return ret;
    }

    int ObAsyncLogApplier::end_transaction(ObLogTask& task)
    {
      int err = OB_SUCCESS;
      uint64_t checksum2check = 0;
      bool rollback = false;
      task.profile_.start_commit();
      common::LogCommand cmd = (common::LogCommand) task.log_entry_.cmd_;
      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      else if (OB_SUCCESS != (err = log_mgr_->add_log_replay_event(task.log_id_, task.mutation_ts_, task.replay_type_, cmd)))
      {
        YYSYS_LOG(ERROR, "delay_stat_.add_log_replay_event(seq=%ld, ts=%ld)=>%d",
                  task.log_id_, task.mutation_ts_, err);
      }
      else if (!task.trans_id_.is_valid())
      {
        YYSYS_LOG(TRACE, "task.trans_id is invalid, log_id=%ld,cmd=%d", task.log_id_, task.log_entry_.cmd_);
      }
      else if (OB_SUCCESS != (err = add_memtable_uncommited_checksum_(task.trans_id_.descriptor_, &checksum2check)))
      {
        YYSYS_LOG(ERROR, "calc_transaction_checksum fail err=%d sd=%u", err, task.trans_id_.descriptor_);
      }
      else if (OB_SUCCESS != (err = table_mgr_->check_checksum(checksum2check,
                                                               task.checksum_before_mutate_,
                                                               task.checksum_after_mutate_)))
      {
        YYSYS_LOG(ERROR, "table_mgr->check_checksum(%s)=>%d", to_cstring(task), err);
        YYSYS_LOG(ERROR, "checksum error, cursor state: has_data_max_log_id=%ld,"
                  " commit_log_id=%ld, commit_point_on_disk=%ld, flushed_cursor=%ld, tmp_cursor=%ld",
                  log_mgr_->get_fill_log_max_log_id(),
                  log_mgr_->get_flushed_clog_id(),
                  log_mgr_->get_last_wrote_commit_point(),
                  log_mgr_->get_flushed_cursor_log_id(),
                  log_mgr_->get_tmp_cursor_log_id());
      }
      else if (OB_SUCCESS != (err = session_mgr_->end_session(task.trans_id_.descriptor_, rollback)))
      {
        YYSYS_LOG(ERROR, "end_session(%s)=>%d", to_cstring(task.trans_id_), err);
      }
      else
      {
        session_mgr_->get_trans_seq().update(task.mutation_ts_);
      }
      if (OB_SUCCESS != err)
      {
        YYSYS_LOG(WARN, "err=%d %s", err, to_cstring(task));
      }
      //YYSYS_LOG(INFO, "end_trans[trans_id=%ld, checksum=%ld]", task.trans_id_, task.checksum_after_mutate_);
      task.profile_.end_commit();
      return err;
    }

    int ObAsyncLogApplier::flush(ObLogTask& task)
    {
      int err = OB_SUCCESS;
      task.profile_.start_flush();
      if (!is_inited())
      {
        err = OB_NOT_INIT;
      }
      else
      {
        err = log_mgr_->write_log_as_slave(task.batch_buf_, task.batch_buf_len_);
      }
      task.profile_.end_flush();
      return err;
    }

    int ObAsyncLogApplier::on_destroy(ObLogTask& task)
    {
      int err = OB_SUCCESS;
      if (task.profile_.enable_)
      {
        char buf[ReplayTaskProfile::MAX_PROFILE_INFO_BUF_LEN];
        task.profile_.to_string(buf, sizeof(buf));
        YYSYS_LOG(INFO, "%s", buf);
      }
      return err;
    }
  }; // end namespace updateserver
}; // end namespace oceanbase
