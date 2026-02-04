////===================================================================
 //
 // ob_session_mgr.cpp updateserver / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2012-08-20 by Yubai (yubai.lk@taobao.com) 
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

#include <algorithm>
#include "ob_session_mgr.h"
#include "ob_update_server_main.h"
//add hongchen [LOCK_WAIT_TIMEOUT] 20161209:b
#include "yysys.h"
//add hongchen [LOCK_WAIT_TIMEOUT] 20161209:e

//add hongchen [LOCK_WAIT_TIMEOUT] 20161209:b
#define UPS ObUpdateServerMain::get_instance()->get_update_server()//add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701
//add hongchen [LOCK_WAIT_TIMEOUT] 20161209:e
namespace oceanbase
{
  using namespace common;
  namespace updateserver
  {
    CallbackMgr::CallbackMgr() : callback_list_(NULL),
                                 prepare_list_(NULL)
    {
    }

    CallbackMgr::~CallbackMgr()
    {
    }

    void CallbackMgr::reset()
    {
      callback_list_ = NULL;
      prepare_list_ = NULL;
    }

    int CallbackMgr::callback(const bool rollback, BaseSessionCtx &session)
    {
      int ret = OB_SUCCESS;
      CallbackInfo *iter = callback_list_;
      while (NULL != iter)
      {
        if (NULL != iter->callback)
        {
          int tmp_ret = iter->callback->cb_func(rollback, iter->data, session);
          if (OB_SUCCESS != tmp_ret)
          {
            YYSYS_LOG(WARN, "invoke callback fail ret=%d cb=%p data=%p", tmp_ret, iter->callback, iter->data);
            ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
          }
        }
        iter = iter->next;
      }
      callback_list_ = NULL;
      return ret;
    }

    void CallbackMgr::commit_prepare_list()
    {
      CallbackInfo *iter = prepare_list_;
      while (NULL != iter)
      {
        CallbackInfo *next = iter->next;
        iter->next = callback_list_;
        callback_list_ = iter;
        iter = next;
      }
      prepare_list_ = NULL;
    }

    void CallbackMgr::rollback_prepare_list(BaseSessionCtx &session)
    {
      YYSYS_TRACE_LOG("rollback head=%p", prepare_list_);
      CallbackInfo *iter = prepare_list_;
      while (NULL != iter)
      {
        if (NULL != iter->callback)
        {
          bool rollback = true;
          int tmp_ret = iter->callback->cb_func(rollback, iter->data, session);
          if (OB_SUCCESS != tmp_ret)
          {
            YYSYS_LOG(WARN, "invoke callback fail ret=%d cb=%p data=%p", tmp_ret, iter->callback, iter->data);
          }
        }
        iter = iter->next;
      }
      prepare_list_ = NULL;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    MinTransIDGetter::MinTransIDGetter(SessionMgr &sm) : sm_(sm),
                                                         min_trans_id_(INT64_MAX)
    {
    }

    MinTransIDGetter::~MinTransIDGetter()
    {
    }

    void MinTransIDGetter::operator()(const uint32_t sd)
    {
      BaseSessionCtx *ctx = sm_.fetch_ctx(sd);
      if (NULL != ctx)
      {
        if (ctx->get_type() == ST_READ_ONLY
            && !ctx->is_frozen()
            && min_trans_id_ > ctx->get_trans_id())
        {
          min_trans_id_ = ctx->get_trans_id();
        }
        sm_.revert_ctx(sd);
      }
    }

    int64_t MinTransIDGetter::get_min_trans_id()
    {
      return min_trans_id_;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    ZombieKiller::ZombieKiller(SessionMgr &sm,
                               const bool force) : sm_(sm),
                                                   force_(force)
    {
    }

    ZombieKiller::~ZombieKiller()
    {
    }

    void ZombieKiller::operator()(const uint32_t sd)
    {
      BaseSessionCtx *ctx = sm_.fetch_ctx(sd);
      if (NULL != ctx)
      {
        if (__sync_bool_compare_and_swap(ctx->get_mutex_flag_for_pkt_ptr(), false, true))
        {
          int64_t session_end_time = ctx->get_session_start_time() + ctx->get_session_timeout();
          session_end_time = (0 <= session_end_time) ? session_end_time : INT64_MAX;
          int64_t idle_end_time = ctx->get_last_active_time() + ctx->get_session_idle_time();
          idle_end_time = (0 <= idle_end_time) ? idle_end_time: INT64_MAX;
          int64_t end_time = std::min(session_end_time, idle_end_time);
          bool is_zombie = false;
          bool lock_wait_timweout = false;
          bool is_session_killed  = false;
          bool is_need_response   = false;
          bool is_need_unlock     = false;
          common::ObPacket * pkt = ctx->get_pkt_ptr();
          if (NULL != pkt
               && __sync_bool_compare_and_swap(pkt->get_mutex_flag(), false, true))
          {
            is_need_unlock = true;
            if (0 != pkt->get_last_conflict_lock_time()
                && yysys::CTimeUtil::getTime() - pkt->get_last_conflict_lock_time() > pkt->get_lock_wait_timeout())
            {
              lock_wait_timweout = true;
              if (!(ctx->is_session_expired() || ctx->is_stmt_expired())
                  && __sync_bool_compare_and_swap(pkt->get_need_response(), true, false))
              {
                is_need_response = true;
              }
            }
          }
          if (yysys::CTimeUtil::getTime() > end_time
              || force_
              || lock_wait_timweout)
          {
              if(ctx->get_can_be_killed() || ObUpsRoleMgr::MASTER != UPS.get_role_mgr().get_role()) //[521] mod
              {
                ctx->kill();
              }
            //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
            if(ctx->is_prepare())
            {
              //distribute transaction, need special process
              common::ObTransID trans_id;
              trans_id.descriptor_ = sd;
              trans_id.start_time_us_ = ctx->get_session_start_time();
              trans_id.ups_ = ObUpdateServerMain::get_instance()->get_update_server().get_self();
              sm_.add_unsettled_trans(trans_id);
            }
            //add 20150701:e
          }
          is_zombie = ctx->is_killed();
          sm_.revert_ctx(sd);
          if (is_zombie)
          {
            bool rollback = true;
            bool force = false;
            if (OB_SUCCESS == sm_.end_session(sd, rollback, force))
            {
              YYSYS_LOG(INFO, "session killed succ, sd=%u", sd);
              is_session_killed = true;
              if (lock_wait_timweout)
              {
                YYSYS_LOG(INFO, "session killed succ because of lock timeout, sd=%u, last_lock_conflict_time=%ld, lock_wait_time=%ld.",
                                sd, pkt->get_last_conflict_lock_time(),
                                pkt->get_lock_wait_timeout());
                if (is_need_response)
                {
                  UPS.response_result(OB_TRANS_ROLLBACKED, *pkt);
                }
              }
            }
            else
            {
              YYSYS_LOG(INFO, "session killed fail, maybe using, sd=%u", sd);
            }
          }
          if (is_need_unlock)
          {
            __sync_bool_compare_and_swap(pkt->get_mutex_flag(), true, false);
          }
          if (!is_session_killed)
          {
            __sync_bool_compare_and_swap(ctx->get_mutex_flag_for_pkt_ptr(), true, false);
          }
        }
        else
        {
          sm_.revert_ctx(sd);
        }
      }
    }

    ShowSessions::ShowSessions(SessionMgr &sm, ObNewScanner &scanner) : sm_(sm),
                                                                        scanner_(scanner),
                                                                        row_desc_(),
                                                                        row_()
    {
      row_desc_.add_column_desc(SESSION_TABLE_ID, SESSION_COL_ID_SD);
      row_desc_.add_column_desc(SESSION_TABLE_ID, SESSION_COL_ID_TYPE);
      row_desc_.add_column_desc(SESSION_TABLE_ID, SESSION_COL_ID_TID);
      row_desc_.add_column_desc(SESSION_TABLE_ID, SESSION_COL_ID_STIME);
      row_desc_.add_column_desc(SESSION_TABLE_ID, SESSION_COL_ID_SSTIME);
      row_desc_.add_column_desc(SESSION_TABLE_ID, SESSION_COL_ID_TIMEO);
      row_desc_.add_column_desc(SESSION_TABLE_ID, SESSION_COL_ID_STIMEO);
      row_desc_.add_column_desc(SESSION_TABLE_ID, SESSION_COL_ID_ITIME);
      row_desc_.add_column_desc(SESSION_TABLE_ID, SESSION_COL_ID_ATIME);
      //add zhaoqiong [ups show session lock conflict info] 20161215:b
      row_desc_.add_column_desc(SESSION_TABLE_ID, SESSION_COL_ID_CONFLICTID);
      row_desc_.add_column_desc(SESSION_TABLE_ID, SESSION_COL_ID_LWTIME);
      //add:e
      row_desc_.set_rowkey_cell_count(1);
      row_.set_row_desc(row_desc_);
    }

    ShowSessions::~ShowSessions()
    {
    }

    void ShowSessions::operator()(const uint32_t sd)
    {
      BaseSessionCtx *ctx = sm_.fetch_ctx(sd);
      RWSessionCtx *rwctx = NULL; //add zhaoqiong [ups show session lock conflict info] 20161215
      if (NULL != ctx)
      {
        ObObj cell;
        cell.set_int(ctx->get_session_descriptor());
        row_.set_cell(SESSION_TABLE_ID, SESSION_COL_ID_SD, cell);
        cell.set_int(ctx->get_type());
        row_.set_cell(SESSION_TABLE_ID, SESSION_COL_ID_TYPE, cell);
        cell.set_int(ctx->get_trans_id());
        row_.set_cell(SESSION_TABLE_ID, SESSION_COL_ID_TID, cell);
        cell.set_int(ctx->get_session_start_time());
        row_.set_cell(SESSION_TABLE_ID, SESSION_COL_ID_STIME, cell);
        cell.set_int(ctx->get_stmt_start_time());
        row_.set_cell(SESSION_TABLE_ID, SESSION_COL_ID_SSTIME, cell);
        cell.set_int(ctx->get_session_timeout());
        row_.set_cell(SESSION_TABLE_ID, SESSION_COL_ID_TIMEO, cell);
        cell.set_int(ctx->get_stmt_timeout());
        row_.set_cell(SESSION_TABLE_ID, SESSION_COL_ID_STIMEO, cell);
        cell.set_int(ctx->get_session_idle_time());
        row_.set_cell(SESSION_TABLE_ID, SESSION_COL_ID_ITIME, cell);
        cell.set_int(ctx->get_last_active_time());
        row_.set_cell(SESSION_TABLE_ID, SESSION_COL_ID_ATIME, cell);
        //add zhaoqiong [ups show session lock conflict info] 20161215:b
        rwctx = static_cast<RWSessionCtx *>(ctx);
        if(rwctx != NULL)
        {
            cell.set_int(rwctx->get_conflict_session_id());
        }
        else
        {
            cell.set_int(0);
        }
        row_.set_cell(SESSION_TABLE_ID, SESSION_COL_ID_CONFLICTID, cell);
        if(rwctx != NULL && rwctx->get_conflict_session_id() !=0)
        {
            cell.set_int(ctx->get_lock_wait_time());
        }
        else
        {
            cell.set_int(0);
        }
        row_.set_cell(SESSION_TABLE_ID, SESSION_COL_ID_LWTIME, cell);
        //add:e
        YYSYS_LOG(INFO, "[session] %s", to_cstring(row_));
        scanner_.add_row(row_);
        sm_.revert_ctx(sd);
      }
    }

    ObNewScanner &ShowSessions::get_scanner()
    {
      return scanner_;
    }

    const ObRowDesc &ShowSessions::get_row_desc() const
    {
      return row_desc_;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    SessionMgr::SessionMgr() : inited_(false),
                               trans_seq_(),
                               published_trans_id_(0),
                               commited_trans_id_(0),
                               min_flying_trans_id_(0),
                               calc_timestamp_(0),
                               ctx_map_(),
                               session_lock_(),
                               is_session_lock_for_sync_frozen_(false),  //add peiouya [MultiUPS] [UPS_SYNC_Frozen] 20150810
                               allow_start_write_session_(false),
                               ks_stat_(KS_NORMAL)
    {
      published_trans_id_ = yysys::CTimeUtil::getTime();
      commited_trans_id_ = published_trans_id_;
    }

    SessionMgr::~SessionMgr()
    {
      if (inited_)
      {
        destroy();
      }
    }

    int SessionMgr::init(const uint32_t max_ro_num,
                        const uint32_t max_rp_num,
                        const uint32_t max_rw_num,
                        ISessionCtxFactory *factory)
    {
      int ret = OB_SUCCESS;
      if (inited_)
      {
        ret = OB_INIT_TWICE;
      }
      else if (0 >= max_ro_num
              || 0 >= max_rp_num
              || 0 >= max_rw_num
              || NULL == (factory_ = factory))
      {
        YYSYS_LOG(WARN, "invalid param max_ro_num=%u max_rp_num=%u max_rw_num=%u factory=%p",
                  max_ro_num, max_rp_num, max_rw_num, factory);
        ret = OB_INVALID_ARGUMENT;
      }
      //else if (1 != start())
      //{
      //  YYSYS_LOG(WARN, "start thread to flush_min_flying_trans_id fail");
      //  ret = OB_ERR_UNEXPECTED;
      //}
      else if (OB_SUCCESS != (ret = ctx_map_.init(max_ro_num + max_rp_num + max_rw_num)))
      {
        YYSYS_LOG(WARN, "init ctx_map fail, ret=%d num=%u", ret, max_ro_num + max_rp_num + max_rw_num);
      }
      else
      {
        const int64_t MAX_CTX_NUM[SESSION_TYPE_NUM] = {max_ro_num, max_rp_num, max_rw_num};
        BaseSessionCtx *ctx = NULL;
        for (int i = 0; i < SESSION_TYPE_NUM; i++)
        {
          if (OB_SUCCESS != (ret = ctx_list_[i].init(MAX_CTX_NUM[i])))
          {
            YYSYS_LOG(WARN, "init ctx_list fail, ret=%d type=%d max_ctx_num=%ld", ret, i, MAX_CTX_NUM[i]);
            break;
          }
          for (int64_t j = 0; OB_SUCCESS == ret && j < MAX_CTX_NUM[i]; j++)
          {
            if (NULL == (ctx = factory_->alloc((SessionType)i, *this)))
            {
              YYSYS_LOG(WARN, "alloc ctx fail, type=%d", i);
              ret = OB_MEM_OVERFLOW;
              break;
            }
            if (OB_SUCCESS != (ret = ctx_list_[i].push(ctx)))
            {
              YYSYS_LOG(WARN, "push ctx to list fail, ret=%d ctx=%p", ret, ctx);
              break;
            }
          }
        }
      }
      //conflict_level A
      if (OB_SUCCESS == ret && OB_SUCCESS == (ret = unsettled_transaction_.init()))
      {
        inited_ = true;
      }
      else
      {
        destroy();
      }
      return ret;
    }

    void SessionMgr::destroy()
    {
      stop();
      wait();
      if (0 != ctx_map_.size())
      {
        YYSYS_LOG(ERROR, "some transaction have not end, counter=%d", ctx_map_.size());
      }

      ctx_map_.destroy();

      for (int i = 0; i < SESSION_TYPE_NUM; i++)
      {
        BaseSessionCtx *ctx = NULL;
        while (OB_SUCCESS == ctx_list_[i].pop(ctx))
        {
          if (NULL != ctx
              && NULL != factory_)
          {
            factory_->free(ctx);
          }
        }
        ctx_list_[i].destroy();
      }

      calc_timestamp_ = 0;
      min_flying_trans_id_ = -1;
      published_trans_id_ = -1;
      commited_trans_id_ = -1;
      factory_ = NULL;
      inited_ = false;
    }

    void SessionMgr::run(yysys::CThread* thread, void* arg)
    {
      UNUSED(thread);
      UNUSED(arg);
      static const int32_t MAX_SLEEP_TIME = 16L * 1024L;
      static const int32_t MIN_SLEEP_TIME = 1L;
      int32_t next_sleep_time = MAX_SLEEP_TIME; 
      int64_t last_min_flying_trans_id = 0;
      while (!_stop)
      {
        flush_min_flying_trans_id();
        const int64_t cur_min_flying_trans_id = get_min_flying_trans_id();
        const int32_t prev_sleep_time = next_sleep_time;
        if (last_min_flying_trans_id != cur_min_flying_trans_id)
        {
          next_sleep_time = next_sleep_time / 2;
          next_sleep_time = (MIN_SLEEP_TIME > next_sleep_time) ? MIN_SLEEP_TIME : next_sleep_time;
        }
        else
        {
          next_sleep_time = next_sleep_time * 2;
          next_sleep_time = (MAX_SLEEP_TIME < next_sleep_time) ? MAX_SLEEP_TIME : next_sleep_time;
        }
        if (prev_sleep_time != next_sleep_time
            && REACH_TIME_INTERVAL(1000000))
        {
          YYSYS_LOG(INFO, "flush_min_flying_trans_id interval switch from %d to %d",
                    prev_sleep_time, next_sleep_time);
        }
        last_min_flying_trans_id = cur_min_flying_trans_id;
        if (0 < next_sleep_time)
        {
          usleep(next_sleep_time);
        }
      }
    }

    //mod peiouya [MultiUPS] [UPS_SYNC_Frozen] 20150811:b
    //int SessionMgr::begin_session(const SessionType type, const int64_t start_time, const int64_t timeout, const int64_t idle_time,  uint32_t &session_descriptor)
    int SessionMgr::begin_session(const SessionType type, const int64_t start_time, const int64_t timeout, const int64_t idle_time,  uint32_t &session_descriptor, const common::PacketCode pcode)
    {
      int ret = OB_SUCCESS;
      SessionLockGuard guard(type, session_lock_);
      BaseSessionCtx *ctx = NULL;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      //else if (ST_READ_WRITE == type && !allow_start_write_session_)
      else if (ST_READ_WRITE == type
               && !allow_start_write_session_
               && !UPS.can_process_with_disable_sws (pcode))
      {
        ret = OB_EAGAIN;
        YYSYS_LOG(ERROR, "not allowed to start write_session");
      }
      else if (!guard.is_lock_succ() && !UPS.can_process_with_disable_sws (pcode))
      {
        ret = OB_BEGIN_TRANS_LOCKED;
      }
      else if (NULL == (ctx = alloc_ctx_(type)))
      {
        YYSYS_LOG(WARN, "alloc ctx fail, type=%d", type);
        ret = OB_MEM_OVERFLOW;
      }
      else
      {
          if(OB_WRITE_TRANS_STAT == pcode)
          {
              ctx->set_can_be_killed(false);
          }
        while (true)
        {
          uint32_t sd = 0;
          const int64_t begin_trans_id = published_trans_id_;
          ctx->set_trans_id(begin_trans_id);
          ctx->set_session_start_time(start_time);
          ctx->set_stmt_start_time(start_time);
          ctx->set_session_timeout(timeout);
          ctx->set_session_idle_time(idle_time);
          if (OB_SUCCESS != (ret = ctx_map_.assign(ctx, sd)))
          {
            YYSYS_LOG(WARN, "assign from ctx_map fail, ret=%d ctx=%p type=%d", ret, ctx, type);
            free_ctx_(ctx);
            break;
          }
          if (begin_trans_id != published_trans_id_)
          {
            ctx_map_.erase(sd);
          }
          else
          {
            ctx->set_session_descriptor(sd);
            session_descriptor = sd;
            FILL_TRACE_BUF(ctx->get_tlog_buffer(), "type=%d sd=%u ctx=%p trans_id=%ld", type, sd, ctx, begin_trans_id);
            break;
          }
        }
      }
      return ret;
    }

    int SessionMgr::precommit(const uint32_t session_descriptor)
    {
      return end_session(session_descriptor, false, true, BaseSessionCtx::ES_CALLBACK);
    }

    int SessionMgr::update_commited_trans_id(BaseSessionCtx* ctx)
    {
      int ret = OB_SUCCESS;
      if (NULL == ctx)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (ctx->need_to_do(BaseSessionCtx::ES_UPDATE_COMMITED_TRANS_ID))
      {
        if (ctx->get_trans_id() > 0)
        {
          commited_trans_id_ = ctx->get_trans_id();
        }
        ctx->mark_done(BaseSessionCtx::ES_UPDATE_COMMITED_TRANS_ID);
      }
      return ret;
    }

    int SessionMgr::end_session(const uint32_t session_descriptor, const bool rollback, const bool force, const uint64_t es_flag)
    {
      int ret = OB_SUCCESS;
      BaseSessionCtx *ctx = NULL;
      FetchMod fetch_mod = force ? FM_MUTEX_BLOCK : FM_MUTEX_NONBLOCK;
      if (!inited_)
      {
        ret = OB_NOT_INIT;
      }
      else if (NULL == (ctx = ctx_map_.fetch(session_descriptor, fetch_mod)))
      {
        YYSYS_LOG(WARN, "fetch ctx fail, sd=%u rollback=%s force=%s",
                  session_descriptor, STR_BOOL(rollback), STR_BOOL(force));
        ret = OB_ERR_UNEXPECTED;
      }
      else if (!force
              && !ctx->is_killed())
      {
        YYSYS_LOG(INFO, "end session not force, session is still alive, will not end it, sd=%u", session_descriptor);
        ctx_map_.revert(session_descriptor);
        ret = OB_UPS_TRANS_RUNNING;
      }
      else
      {
          //add peiouya[SECONDARY_INDEX_OPTI_CHECKSUM_WITH_TRANS_ROLLBACK_BUG_FIX]:b
          if(rollback)
          {
              if(ST_READ_WRITE == ctx->get_type() ||
                 ST_REPLAY == ctx->get_type()) //[710]
              {
                  RWSessionCtx *session_ctx = dynamic_cast<RWSessionCtx *>(ctx);
                  UPS.get_table_mgr().rollback(*session_ctx);
                  session_ctx->get_tekeys()->clear();
              }
          }
          //add:e
        if (ctx->need_to_do((BaseSessionCtx::ES_STAT)(es_flag & BaseSessionCtx::ES_CALLBACK)))
        {
          ctx->end(rollback);
          ctx->mark_done(BaseSessionCtx::ES_CALLBACK);
        }
        FILL_TRACE_BUF(ctx->get_tlog_buffer(), "rollback=%s force=%s es_flag=%lx",
                       STR_BOOL(rollback), STR_BOOL(force), es_flag);
        if ((ST_READ_WRITE == ctx->get_type() || ST_REPLAY == ctx->get_type()) && !rollback)
        {
          if (ctx->need_to_do((BaseSessionCtx::ES_STAT)(es_flag & BaseSessionCtx::ES_UPDATE_COMMITED_TRANS_ID)))
          {
            if (ctx->get_trans_id() > 0) // ctx->get_trans_id() == 0 说明是INTERNAL_WRITE, 在把sstable load到inmemtable时用到
            {
              commited_trans_id_ = ctx->get_trans_id();
            }
            ctx->mark_done(BaseSessionCtx::ES_UPDATE_COMMITED_TRANS_ID);
          }
          if (ctx->need_to_do((BaseSessionCtx::ES_STAT)(es_flag & BaseSessionCtx::ES_PUBLISH)))
          {
            if (ctx->get_trans_id() > 0) // ctx->get_trans_id() == 0 说明是INTERNAL_WRITE, 在把sstable load到inmemtable时用到
            {
              published_trans_id_ = ctx->get_trans_id();
            }
            ctx->publish();
            ctx->mark_done(BaseSessionCtx::ES_PUBLISH);
            if (0 != ctx->get_last_proc_time())
            {
              int64_t cur_time = yysys::CTimeUtil::getTime();
              OB_STAT_INC(UPDATESERVER, UPS_STAT_TRANS_FTIME, cur_time - ctx->get_last_proc_time());
              ctx->set_last_proc_time(cur_time);
            }
          }
        }
        if (es_flag & BaseSessionCtx::ES_FREE)
        {
          if ((ST_READ_WRITE == ctx->get_type()
                || ST_REPLAY == ctx->get_type())
              && 0 != ctx->get_session_start_time())
          {
            //OB_STAT_INC(UPDATESERVER, UPS_STAT_NL_APPLY_COUNT, 1);
            OB_STAT_INC(UPDATESERVER, get_stat_num(ctx->get_priority(), TRANS, COUNT), 1);
            //OB_STAT_INC(UPDATESERVER, UPS_STAT_NL_APPLY_TIMEU, ctx->get_session_timeu());
            OB_STAT_INC(UPDATESERVER, get_stat_num(ctx->get_priority(), TRANS, TIMEU), ctx->get_session_timeu());
          }
          if (0 != ctx->get_last_proc_time())
          {
            int64_t cur_time = yysys::CTimeUtil::getTime();
            OB_STAT_INC(UPDATESERVER, UPS_STAT_TRANS_RTIME, cur_time - ctx->get_last_proc_time());
            ctx->set_last_proc_time(cur_time);
          }

          ctx->on_free();
          FILL_TRACE_BUF(ctx->get_tlog_buffer(), "type=%d sd=%u ctx=%p trans_id=%ld trans_timeu=%ld",
                        ctx->get_type(), session_descriptor, ctx, ctx->get_trans_id(),
                        yysys::CTimeUtil::getTime() - ctx->get_session_start_time());
          PRINT_TRACE_BUF(ctx->get_tlog_buffer());
          bool erase = true;
          ctx_map_.revert(session_descriptor, erase);
          ctx->reset();
          free_ctx_(ctx);
        }
        else
        {
          ctx->set_frozen();
          ctx_map_.revert(session_descriptor);
        }
      }
      return ret;
    }

    BaseSessionCtx *SessionMgr::fetch_ctx(const uint32_t session_descriptor)
    {
      return ctx_map_.fetch(session_descriptor);
    }

    void SessionMgr::revert_ctx(const uint32_t session_descriptor)
    {
      ctx_map_.revert(session_descriptor);
    }

    int64_t SessionMgr::get_processor_index(const uint32_t session_descriptor)
    {
      int64_t ret = OB_SUCCESS;
      BaseSessionCtx *ctx = fetch_ctx(session_descriptor);
      if (NULL != ctx)
      {
        ret = ctx->get_self_processor_index();
        revert_ctx(session_descriptor);
      }
      return ret;
    }

    int64_t SessionMgr::get_min_flying_trans_id()
    {
      //int64_t old_timestamp = calc_timestamp_;
      //if (old_timestamp + CALC_INTERVAL < yysys::CTimeUtil::getTime())
      //{
      //  int64_t cur_timestamp = yysys::CTimeUtil::getTime();
      //  if (old_timestamp == ATOMIC_CAS(&calc_timestamp_, old_timestamp, cur_timestamp))
      //  {
      //    MinTransIDGetter cb(*this);
      //    ctx_map_.traverse(cb);
      //    min_flying_trans_id_ = std::min(cb.get_min_trans_id(), (int64_t)published_trans_id_);
      //  }
      //}
      return min_flying_trans_id_;
    }

    void SessionMgr::flush_min_flying_trans_id()
    {
      MinTransIDGetter cb(*this);
      ctx_map_.traverse(cb);
      min_flying_trans_id_ = std::min(cb.get_min_trans_id(), (int64_t)commited_trans_id_);
      calc_timestamp_ = yysys::CTimeUtil::getTime();
    }

    int SessionMgr::wait_write_session_end_and_lock(const int64_t timeout_us)
    {
      int ret = OB_SUCCESS;
      int64_t now_time = yysys::CTimeUtil::getTime();
      int64_t abs_timeout_us = now_time + timeout_us;
      int64_t start_time = now_time;
      int64_t FORCE_KILL_WAITTIME = UPS.get_param().force_kill_session_wait_time;
      session_lock_.wrlock();
      while (true)
      {
        if (0 == ctx_list_[ST_READ_WRITE].get_free())
        {
          break;
        }
        now_time = yysys::CTimeUtil::getTime();
        if (now_time > abs_timeout_us)
        {
          ret = OB_PROCESS_TIMEOUT;
          break;
        }
        if (now_time >= (start_time + FORCE_KILL_WAITTIME))
        {
          start_time = now_time;
          YYSYS_LOG(INFO, "wait time over %ld, will force kill session", FORCE_KILL_WAITTIME);
          const bool force = true;

          switch_ks_state(KS_PROCESS);
          YYSYS_LOG(INFO, "ready to kill session by force and add distribute trans into unsettled trans");

          kill_zombie_session(force);
          UPS.get_trans_executor().process_unsettled_trans();//add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701

          switch_ks_state(KS_NORMAL);
        }
//add support for arm platform by wangd 202106:b
#if defined(__aarch64__)
            asm("yield");
#else
//add support for arm platform by wangd 202106:e
            asm("pause");
#endif //add support for arm platform by wangd 202106
      }
      if (OB_SUCCESS != ret)
      {
        session_lock_.unlock();
      }
      return ret;
    }

    void SessionMgr::unlock_write_session()
    {
      session_lock_.unlock();
    }
    //add peiouya [MultiUPS] [UPS_SYNC_Frozen] 20150810:b
    //this function only use for UPS_SYNC_Frozen. other functions don't use
    void SessionMgr::lock_write_session_for_sync_freeze()
    {
      session_lock_.wrlock ();
      is_session_lock_for_sync_frozen_ = true;
    }
    void SessionMgr::unlock_write_session_for_sync_freeze()
    {
      if (is_session_lock_for_sync_frozen_)
      {
        is_session_lock_for_sync_frozen_ = false;
        session_lock_.unlock();
      }
    }
    //add 20150811:e

    void SessionMgr::kill_zombie_session(const bool force)
    {
      //YYSYS_LOG(INFO, "start kill_zombie_session force=%s", STR_BOOL(force));
      ZombieKiller cb(*this, force);
      ctx_map_.traverse(cb);
    }

    int SessionMgr::kill_session(const uint32_t session_descriptor)
    {
      int ret = OB_SUCCESS;
      BaseSessionCtx *session = fetch_ctx(session_descriptor);
      if (NULL == session)
      {
        YYSYS_LOG(WARN, "invalid session descriptor=%u", session_descriptor);
      }
      else
      {
        session->kill();
        revert_ctx(session_descriptor);
      }
      return ret;
    }

    void SessionMgr::show_sessions(ObNewScanner &scanner)
    {
      ShowSessions cb(*this, scanner);
      ctx_map_.traverse(cb);
    }

    int64_t SessionMgr::get_commited_trans_id() const
    {
      return commited_trans_id_;
    }

    int64_t SessionMgr::get_published_trans_id() const
    {
      return published_trans_id_;
    }
    //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
    int SessionMgr::add_unsettled_trans(common::ObTransID& trans_id)
    {
      int ret = OB_SUCCESS;
      OnceGuard once_guard(unsettled_trans_lock_);
      if(!once_guard.try_lock())
      {
        ret = OB_ERROR;
        YYSYS_LOG(ERROR, "can not process by more than one thread");
      }
      else if (OB_NOT_INIT == (ret = unsettled_transaction_.search(trans_id)))
      {
        YYSYS_LOG(ERROR, "skip list not init");
      }
      else if (OB_ENTRY_NOT_EXIST == ret)
      {
        ret = unsettled_transaction_.insert(trans_id);
        if(OB_SUCCESS != ret)
        {
          YYSYS_LOG(ERROR, "memory overflow, ret=%d",ret);
        }
        else
        {
          YYSYS_LOG(INFO, "skiplist is %s",to_cstring(unsettled_transaction_));
        }
      }
      return ret;
    }

    common::SkipList<common::ObTransID>& SessionMgr::get_unsettled_trans()
    {
      return unsettled_transaction_;
    }
    //add 20150701:e
    BaseSessionCtx *SessionMgr::alloc_ctx_(const SessionType type)
    {
      BaseSessionCtx *ret = NULL;
      if (SESSION_TYPE_START >= type
          || SESSION_TYPE_NUM <= type)
      {
        YYSYS_LOG(WARN, "invalid session_type=%d", type);
      }
      else if (OB_SUCCESS != ctx_list_[type].pop(ret)
              || NULL == ret)
      {
        YYSYS_LOG(WARN, "alloc ctx fail, type=%d free=%ld", type, ctx_list_[type].get_total());
      }
      else
      {
        YYSYS_LOG(DEBUG, "alloc ctx succ type=%d ctx=%p", type, ret);
      }
      return ret;
    }

    void SessionMgr::free_ctx_(BaseSessionCtx *ctx)
    {
      if (NULL == ctx)
      {
        YYSYS_LOG(WARN, "ctx null pointer");
      }
      else
      {
        SessionType type = ctx->get_type();
        uint32_t sd = ctx->get_session_descriptor();
        if (SESSION_TYPE_START >= type
            || SESSION_TYPE_NUM <= type)
        {
          YYSYS_LOG(WARN, "invalid session_type=%d ctx=%p", type, ctx);
        }
        else if (OB_SUCCESS != ctx_list_[type].push(ctx))
        {
          YYSYS_LOG(WARN, "free ctx fail, ctx=%p type=%d free=%ld", ctx, type, ctx_list_[type].get_total());
        }
        else
        {
          YYSYS_LOG(DEBUG, "free ctx succ type=%d sd=%u ctx=%p", type, sd, ctx);
        }
      }
    }

    //[521]
    void SessionMgr::switch_ks_state(kill_session_state stat)
    {
        switch (stat){
        case KS_NORMAL:
            if(KS_PROCESS != ATOMIC_CAS(&ks_stat_, KS_PROCESS, KS_NORMAL))
            {
                //nothing to do
                YYSYS_LOG(INFO, "ks_stat_ wrong");
            }
            else
            {
                YYSYS_LOG(INFO, "process unsettled trans over");
            }
            break;
        case KS_PROCESS:
            if(KS_NORMAL != ATOMIC_CAS(&ks_stat_, KS_NORMAL, KS_PROCESS))
            {
                //nothing to do
                YYSYS_LOG(INFO, "ks_stat_ wrong");
            }
            else
            {
                YYSYS_LOG(INFO, "start process unsettled trans");
            }
            break;
        default:
            break;
        }
    }

    int SessionMgr::check()
    {
        int ret = OB_SUCCESS;
        if(KS_NORMAL == ks_stat_)
        {
            //nothing to do
        }
        else if(KS_PROCESS == ks_stat_)
        {
            YYSYS_LOG(INFO, "unsettled trans will process this trans.");
            ret = OB_ERROR;
        }
        return ret;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    void KillZombieDuty::runTimerTask()
    {
      ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
      if (NULL == ups_main)
      {
        YYSYS_LOG(WARN, "get ups_main fail");
      }
      else
      {
        ObUpdateServer &ups = ups_main->get_update_server();
        ups.submit_kill_zombie();
      }
    }
  }
}

