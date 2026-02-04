////===================================================================
 //
 // ob_lock_mgr.cpp updateserver / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
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

#include "common/ob_common_stat.h"
#include "ob_lock_mgr.h"
#include "ob_sessionctx_factory.h"

namespace oceanbase
{
  namespace updateserver
  {
    int IRowUnlocker::cb_func(const bool rollback, void *data, BaseSessionCtx &session)
    {
      UNUSED(rollback);
      // ��Ҫ Ҫ�ڽ�����ʱ��te_value��cur_uc_info��գ����ⱻ��һ��session�ظ�ʹ�����������˽�е�cur_uc_info
      // ���������߳��ύ�󣬽��������ύ�̷߳���
      // ���������߳̽��������ύ�߳��ύ�ͷ���
      TEValue *te_value = (TEValue*)data;
      if (NULL != te_value)
      {
        te_value->cur_uc_info = NULL;
      }
      return unlock(te_value, session);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    RowExclusiveUnlocker::RowExclusiveUnlocker()
    {
    }

    RowExclusiveUnlocker::~RowExclusiveUnlocker()
    {
    }

    int RowExclusiveUnlocker::unlock(TEValue *value, BaseSessionCtx &session)
    {
      int ret = OB_SUCCESS;
      if (NULL == value)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = value->row_lock.exclusive_unlock(session.get_session_descriptor())))
      {
        YYSYS_LOG(ERROR, "exclusive unlock row fail sd=%u %s value=%p", session.get_session_descriptor(), value->log_str(), value);
      }
      else
      {
        YYSYS_LOG(DEBUG, "exclusive unlock row succ sd=%u %s value=%p", session.get_session_descriptor(), value->log_str(), value);
      }
      return ret;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    RPLockInfo::RPLockInfo(RPSessionCtx &session_ctx) : ILockInfo(READ_COMMITED),
                                                        session_ctx_(session_ctx),
                                                        row_exclusive_unlocker_(),
                                                        callback_mgr_()
    {
    }

    RPLockInfo::~RPLockInfo()
    {
    }

    int RPLockInfo::on_trans_begin()
    {
      return OB_SUCCESS;
    }

    int RPLockInfo::on_read_begin(const TEKey &key, TEValue &value)
    {
      UNUSED(key);
      UNUSED(value);
      return OB_SUCCESS;
    }

    //add dyr [NPU-2015009-cursor] [NPU-OB-009] 20150319:b
   /*for cursor fetch unlock*/
    int RPLockInfo::force_unlock_row(const TEKey &key, TEValue &value)
    {
        int ret = OB_SUCCESS;
        uint32_t sd = session_ctx_.get_session_descriptor();
        if (value.row_lock.is_exclusive_locked_by(sd))
        {
            if (value.is_able_force_unlock())
            {
            if (OB_SUCCESS != (ret = row_exclusive_unlocker_.unlock(&value, session_ctx_)))
            {
                YYSYS_LOG(USER_ERROR, "Force unlock row failed \'%s\' for key \'PRIMARY\'", to_cstring(key.row_key));
                YYSYS_LOG(ERROR, "Force unlock row failed \'%s\' for key \'PRIMARY\' table_id=%lu request=%u owner=%u",
                          to_cstring(key.row_key), key.table_id, sd, (uint32_t)(value.row_lock.uid_ & QLock::UID_MASK));
            }
            else
            {
              YYSYS_LOG(DEBUG, "Force unlock row succ sd=%u %s %s value=%p",
                              session_ctx_.get_session_descriptor(), key.log_str(), value.log_str(), &value);
                }
            }
        }
        else
        {
            YYSYS_LOG(DEBUG, "don't need force unlock row succ sd=%u %s %s value=%p",
                            session_ctx_.get_session_descriptor(), key.log_str(), value.log_str(), &value);
        }
        return ret;
    }
    //add dyr 20150319:e

    int RPLockInfo::on_write_begin(const TEKey &key, TEValue &value)
    {
      int ret = OB_SUCCESS;
      uint32_t sd = session_ctx_.get_session_descriptor();
      if (!value.row_lock.is_exclusive_locked_by(sd))
      {
        int64_t session_end_time = session_ctx_.get_session_start_time() + session_ctx_.get_session_timeout();
        session_end_time = (0 <= session_end_time) ? session_end_time : INT64_MAX;
        int64_t stmt_end_time = session_ctx_.get_stmt_start_time() + session_ctx_.get_stmt_timeout();
        stmt_end_time = (0 <= stmt_end_time) ? stmt_end_time : INT64_MAX;
        int64_t end_time = std::min(session_end_time, stmt_end_time);
        if (OB_SUCCESS == (ret = value.row_lock.exclusive_lock(sd, end_time, session_ctx_.is_alive())))
        {
          if (OB_SUCCESS != (ret = callback_mgr_.add_callback_info(session_ctx_, &row_exclusive_unlocker_, &value)))
          {
            row_exclusive_unlocker_.unlock(&value, session_ctx_);
          }
          else
          {
            YYSYS_LOG(DEBUG, "exclusive lock row succ sd=%u %s %s value=%p",
                            session_ctx_.get_session_descriptor(), key.log_str(), value.log_str(), &value);
          }
        }
        else
        {
          ret = OB_ERR_EXCLUSIVE_LOCK_CONFLICT;
        }
      }
      //add dyr [NPU-2015009-cursor] [NPU-OB-009] 20150409:b
      if (OB_SUCCESS == ret)
      {//cur session has got the exclusive_lock
          if (OB_SUCCESS != (ret = value.set_unable_force_unlock()))
          {
              YYSYS_LOG(ERROR,"set row value lock flag error!");
          }
      }
      //add dyr 20150409:e
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(USER_ERROR, "Exclusive lock conflict \'%s\' for key \'PRIMARY\'", to_cstring(key.row_key));
        YYSYS_LOG(INFO, "Exclusive lock conflict \'%s\' for key \'PRIMARY\' table_id=%lu request=%u owner=%u",
                  to_cstring(key.row_key), key.table_id, sd, (uint32_t)(value.row_lock.uid_ & QLock::UID_MASK));
      }
      return ret;
    }

    void RPLockInfo::on_trans_end()
    {
      // do nothing
    }

    void RPLockInfo::on_precommit_end()
    {
      bool rollback = false; // do not care
      callback_mgr_.callback(rollback, session_ctx_);
    }

    int RPLockInfo::cb_func(const bool rollback, void *data, BaseSessionCtx &session)
    {
      UNUSED(data);
      return callback_mgr_.callback(rollback, session);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    RCLockInfo::RCLockInfo(RWSessionCtx &session_ctx) : ILockInfo(READ_COMMITED),
                                                        session_ctx_(session_ctx),
                                                        row_exclusive_unlocker_(),
                                                        callback_mgr_()
    {
    }

    RCLockInfo::~RCLockInfo()
    {
    }

    int RCLockInfo::on_trans_begin()
    {
      return OB_SUCCESS;
    }

    int RCLockInfo::on_read_begin(const TEKey &key, TEValue &value)
    {
      UNUSED(key);
      UNUSED(value);
      return OB_SUCCESS;
    }

    //add dyr [NPU-2015009-cursor] [NPU-OB-009] 20150319:b
    int RCLockInfo::force_unlock_row(const TEKey &key, TEValue &value)
    {
        int ret = OB_SUCCESS;
        uint32_t sd = session_ctx_.get_session_descriptor();
        if (value.row_lock.is_exclusive_locked_by(sd))
        {
            if (value.is_able_force_unlock())
            {
            if (OB_SUCCESS != (ret = row_exclusive_unlocker_.unlock(&value, session_ctx_)))
            {
                YYSYS_LOG(USER_ERROR, "Force unlock row failed \'%s\' for key \'PRIMARY\'", to_cstring(key.row_key));
                YYSYS_LOG(ERROR, "Force unlock row failed \'%s\' for key \'PRIMARY\' table_id=%lu request=%u owner=%u",
                          to_cstring(key.row_key), key.table_id, sd, (uint32_t)(value.row_lock.uid_ & QLock::UID_MASK));
            }
            else
            {
              YYSYS_LOG(DEBUG, "Force unlock row succ sd=%u %s %s value=%p",
                              session_ctx_.get_session_descriptor(), key.log_str(), value.log_str(), &value);
                }
            }
        }
        else
        {
            YYSYS_LOG(DEBUG, "don't need force unlock row succ sd=%u %s %s value=%p",
                            session_ctx_.get_session_descriptor(), key.log_str(), value.log_str(), &value);
        }
        return ret;
    }
    //add dyr 20150319:e

    int RCLockInfo::on_write_begin(const TEKey &key, TEValue &value)
    {
      int ret = OB_SUCCESS;
      uint32_t sd = session_ctx_.get_session_descriptor();
      if (!value.row_lock.is_exclusive_locked_by(sd))
      {
        int64_t lock_start_time = yysys::CTimeUtil::getTime();
        int64_t lock_end_time = 0;
        if (OB_SUCCESS == (ret = value.row_lock.try_exclusive_lock(sd)))
        {
          session_ctx_.set_conflict_session_id(0);
          lock_end_time = yysys::CTimeUtil::getTime();
          if (OB_SUCCESS != (ret = callback_mgr_.add_callback_info(session_ctx_, &row_exclusive_unlocker_, &value)))
          {
            row_exclusive_unlocker_.unlock(&value, session_ctx_);
          }
          else
          {
            YYSYS_LOG(DEBUG, "exclusive lock row succ sd=%u %s %s value=%p",
                            session_ctx_.get_session_descriptor(), key.log_str(), value.log_str(), &value);
          }
          //add hongchen [LOCK_WAIT_TIMEOUT] 20161209:b
          if (NULL != session_ctx_.get_pkt_ptr())
          {
            session_ctx_.get_pkt_ptr()->set_last_conflict_lock_time(0);
          }
          //add hongchen [LOCK_WAIT_TIMEOUT] 20161209:e
          OB_STAT_INC(UPDATESERVER, UPS_STAT_LOCK_SUCC_COUNT, 1);
        }
        else
        {
          lock_end_time = yysys::CTimeUtil::getTime();
          //add hongchen [LOCK_WAIT_TIMEOUT] 20161209:b
          if (NULL != session_ctx_.get_pkt_ptr() && 0 == session_ctx_.get_pkt_ptr()->get_last_conflict_lock_time())
          {
            session_ctx_.get_pkt_ptr()->set_last_conflict_lock_time(lock_end_time);
          }
          //add hongchen [LOCK_WAIT_TIMEOUT] 20161209:e
          session_ctx_.set_conflict_processor_index(session_ctx_.get_host().get_processor_index(value.row_lock.get_uid()));
          ret = OB_ERR_EXCLUSIVE_LOCK_CONFLICT;
          OB_STAT_INC(UPDATESERVER, UPS_STAT_LOCK_FAIL_COUNT, 1);
        }
        OB_STAT_INC(UPDATESERVER, UPS_STAT_LOCK_WAIT_TIME, lock_end_time - lock_start_time);
      }
      //add dyr [NPU-2015009-cursor] [NPU-OB-009] 20150319:b
      if (OB_SUCCESS == ret)
      {//cur session has got the exclusive_lock
          if (OB_SUCCESS != (ret = value.set_unable_force_unlock()))
          {
              YYSYS_LOG(ERROR,"set row value lock flag error!");
          }
      }
      //add dyr 20150319:e
      if (OB_SUCCESS != ret)
      {
        ob_set_err_msg("Exclusive lock conflict \'%s\' for key \'PRIMARY\'",
                  to_cstring(key.row_key));
        if (session_ctx_.set_conflict_session_id((uint32_t)(value.row_lock.uid_ & QLock::UID_MASK)))
        {
          YYSYS_LOG(INFO, "Exclusive lock conflict \'%s\' for key \'PRIMARY\' table_id=%lu request=%u owner=%u",
                    to_cstring(key.row_key), key.table_id, sd, (uint32_t)(value.row_lock.uid_ & QLock::UID_MASK));
        }
      }
      return ret;
    }

    void RCLockInfo::on_trans_end()
    {
      // do nothing
    }

    void RCLockInfo::on_precommit_end()
    {
      // do nothing
    }

    int RCLockInfo::cb_func(const bool rollback, void *data, BaseSessionCtx &session)
    {
      UNUSED(data);
      return callback_mgr_.callback(rollback, session);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    LockMgr::LockMgr()
    {
    }

    LockMgr::~LockMgr()
    {
    }

    ILockInfo *LockMgr::assign(const IsolationLevel level, BaseSessionCtx &session_ctx)
    {
      int tmp_ret = OB_SUCCESS;
      void *buffer = NULL;
      ILockInfo *ret = NULL;
      RPSessionCtx* rpsession_ctx = NULL;
      RWSessionCtx* rwsession_ctx = NULL;
      switch (level)
      {
      case NO_LOCK:
        if (NULL == (rpsession_ctx = dynamic_cast<RPSessionCtx*>(&session_ctx)))
        {
          YYSYS_LOG(ERROR, "can not cast session_ctx to RPSessionCtx, ctx=%s", to_cstring(*rpsession_ctx));
        }
        else if (NULL == (buffer = rpsession_ctx->alloc(sizeof(RPLockInfo))))
        {
          YYSYS_LOG(ERROR, "alloc rplock info fail, ctx=%s", to_cstring(*rpsession_ctx));
        }
        else if (OB_SUCCESS != (tmp_ret = rpsession_ctx->add_callback_info(*rpsession_ctx, ret = new(buffer) RPLockInfo(*rpsession_ctx), NULL)))
        {
          YYSYS_LOG(ERROR, "add_callback_info()=>%d, ctx=%s", tmp_ret, to_cstring(*rpsession_ctx));
          ret = NULL;
        }
        break;
      case READ_COMMITED:
        if (NULL == (rwsession_ctx = dynamic_cast<RWSessionCtx*>(&session_ctx)))
        {
          YYSYS_LOG(ERROR, "can not cast session_ctx to RWSessionCtx, ctx=%s", to_cstring(*rwsession_ctx));
        }
        else if (NULL == (buffer = rwsession_ctx->alloc(sizeof(RCLockInfo))))
        {
          YYSYS_LOG(ERROR, "alloc rclock info fail, ctx=%s", to_cstring(*rwsession_ctx));
        }
        else if (OB_SUCCESS != (tmp_ret = rwsession_ctx->add_callback_info(*rwsession_ctx, ret = new(buffer) RCLockInfo(*rwsession_ctx), NULL)))
        {
          YYSYS_LOG(ERROR, "add_callback_info()=>%d, ctx=%s", tmp_ret, to_cstring(*rwsession_ctx));
          ret = NULL;
        }
        break;
      default:
        YYSYS_LOG(WARN, "isolation level=%d not support", level);
        break;
      }
      return ret;
    }
  }
}

