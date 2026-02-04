////===================================================================
 //
 // ob_sessionctx_factory.h updateserver / Oceanbase
 //
 // Copyright (C) 2010, 2013 Taobao.com, Inc.
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

#ifndef  OCEANBASE_UPDATESERVER_SESSIONCTX_FACTORY_H_
#define  OCEANBASE_UPDATESERVER_SESSIONCTX_FACTORY_H_
#include "common/ob_define.h"
#include "sql/ob_ups_result.h"
#include "ob_session_mgr.h"
#include "ob_ups_mutator.h"
#include "ob_table_engine.h"
#include "ob_lock_mgr.h"
#include "common/ob_fifo_allocator.h"

namespace oceanbase
{
  namespace updateserver
  {
    typedef BaseSessionCtx ROSessionCtx;


    struct TEValueUCInfo
    {
      static ObCellInfoNode* const INVALID_CELL_INFO_NODE;
      TEValue *value;
      uint32_t session_descriptor;
      int16_t uc_cell_info_cnt;
      int16_t uc_cell_info_size;
      ObCellInfoNode *uc_list_head;
      ObCellInfoNode *uc_list_tail;
      ObCellInfoNode *uc_list_tail_before_stmt;
      TEValueUCInfo()
      {
        reset();
      };
      void reset()
      {
        value = NULL;
        session_descriptor = INVALID_SESSION_DESCRIPTOR;
        uc_cell_info_cnt = 0;
        uc_cell_info_size = 0;
        uc_list_head = NULL;
        uc_list_tail = NULL;
        uc_list_tail_before_stmt = INVALID_CELL_INFO_NODE;
      };
    };

    struct TransUCInfo
    {
      int64_t uc_row_counter;
      uint64_t uc_checksum;
      MemTable *host;
      TransUCInfo()
      {
        reset();
      };
      void reset()
      {
        uc_row_counter = 0;
        uc_checksum = 0;
        host = NULL;
      };
    };

    class PageArenaWrapper : public ObIAllocator
    {
      public:
        PageArenaWrapper(common::ModuleArena &arena) : arena_(arena)
        {};
        ~PageArenaWrapper()
        {};
      public:
        void *alloc(const int64_t sz)
        {return arena_.alloc(sz);};
        void free(void *ptr)
        {arena_.free((char*)ptr);};
        void set_mod_id(int32_t mod_id)
        {UNUSED(mod_id);};
      private:
        common::ModuleArena &arena_;
    };

    class TEValueChecksumCallback : public ISessionCallback
    {
      public:
        TEValueChecksumCallback() : checksum_(0)
        {};
        void reset()
        {checksum_ = 0;};
        int cb_func(const bool rollback, void *data, BaseSessionCtx &session)
        {
          UNUSED(rollback);
          UNUSED(session);
          if (!rollback)
          {
            TEValue *te_value = (TEValue*)data;
            if (NULL != te_value->list_tail)
            {
              checksum_ ^= te_value->list_tail->modify_time;
            }
          }
          return OB_SUCCESS;
        };
        int64_t get_checksum() const
        {return checksum_;};
      private:
        int64_t checksum_;
    };

    class SessionMgr;
    class RWSessionCtx : public BaseSessionCtx, public CallbackMgr
    {
      const static int64_t INIT_TABLE_MAP_SIZE = 10; //add zhaoqiong [244]
      typedef int64_t v4si __attribute__ ((vector_size (32)));
      static v4si v4si_zero;
      enum Stat
      {
        // 状态转移:
        // ST_ALIVE --> ST_FROZEN
        // ST_FROZEN --> ST_FROZEN
        // ST_KILLING --> ST_FROZEN
        // ST_ALIVE --> ST_KILLING
        ST_ALIVE = 0,
        //mod peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
        /*
        ST_FROZEN = 1,
        ST_KILLING = 2,
        */
        ST_PREPARE = 1,
        ST_FROZEN = 2,
        ST_KILLING = 3,
        //mod 20150701:e
      };
      static const int64_t ALLOCATOR_PAGE_SIZE = 16L * 1024L;
      public:
        RWSessionCtx(const SessionType type, SessionMgr &host, common::FIFOAllocator &fifo_allocator, const bool need_gen_mutator=true);
        virtual ~RWSessionCtx();
      public:
        void end(const bool need_rollback);
        void publish();
        void on_free();
        void *alloc(const int64_t size);
        void reset();
        virtual void kill();
        int add_stmt_callback(ISessionCallback *callback, void *data)
        {
          return stmt_callback_list_.add_callback_info(*this, callback, data);
        }
        int add_publish_callback(ISessionCallback *callback, void *data);
        int add_free_callback(ISessionCallback *callback, void *data);
        int prepare_checksum_callback(void *data)
        {
          return checksum_callback_list_.prepare_callback_info(*this, &checksum_callback_, data);
        };
        void commit_prepare_checksum()
        {
          checksum_callback_list_.commit_prepare_list();
        };
        void rollback_prepare_checksum()
        {
          checksum_callback_list_.rollback_prepare_list(*this);
        };
        int64_t get_checksum()
        {
          checksum_callback_list_.callback(false, *this);
          return checksum_callback_.get_checksum();
        };
        //add duyr [MultiUPS] [READ_ATOMIC] [write_part] 20151117:b
        int prepare_trans_ver_callback(void *data)
        {
          return trans_version_callback_list_.prepare_callback_info(*this,
                                                                    &trans_version_callback_,
                                                                    data);
        }
        void commit_prepare_trans_ver()
        {
          trans_version_callback_list_.commit_prepare_list();
        }
        void rollback_prepare_trans_ver()
        {
          trans_version_callback_list_.rollback_prepare_list(*this);
        }


        int backfill_trans_version(const bool rollback = false)
        {
          commit_prepare_trans_ver();
          return trans_version_callback_list_.callback(rollback,*this);
        }
        //add duyr 20151117:e
        void mark_stmt()
        {
          get_ups_mutator().get_mutator().mark();
          mark_stmt_total_row_counter_ = get_ups_result().get_affected_rows();
          mark_stmt_new_row_counter_ = get_uc_info().uc_row_counter;
          mark_stmt_checksum_ = get_uc_info().uc_checksum;
          mark_dml_count_ = dml_count_;
          stmt_callback_list_.reset();
        };
        void rollback_stmt()
        {
          get_ups_mutator().get_mutator().rollback();
          get_ups_result().set_affected_rows(mark_stmt_total_row_counter_);
          get_uc_info().uc_row_counter = mark_stmt_new_row_counter_;
          get_uc_info().uc_checksum = mark_stmt_checksum_;
          stmt_callback_list_.callback(true, *this);
          rollback_prepare_list(*this);
          rollback_prepare_checksum();
          //add duyr [MultiUPS] [READ_ATOMIC] [write_part] 20151117:b
          rollback_prepare_trans_ver();
          //add duyr 20151117:e
          dml_count_ = mark_dml_count_;
        };
        void commit_stmt()
        {
          stmt_callback_list_.callback(false, *this);
          //add shili [MultiUPS] [READ_ATOMIC] [write_part] 20151117:b
          commit_prepare_trans_ver();
          //add shili 20151117:e
          commit_prepare_list();
          commit_prepare_checksum();
        };
        void inc_dml_count(const ObDmlType dml_type)
        {
          if (OB_DML_UNKNOW < dml_type
              && OB_DML_NUM > dml_type)
          {
            int64_t *d = (int64_t*)&dml_count_;
            d[dml_type - 1] += 1;
          }
          else
          {
            YYSYS_LOG(ERROR, "invalid dml_type=%d", dml_type);
          }
        };
        //add shili, [MultiUPS] [LONG_TRANSACTION_LOG]  20170523:b
        bool is_global_ctx()
        {
          return coordinator_tid_.is_valid();
        }
        bool is_someone_prepare_failed()
        {return is_someone_prepare_failed_;}
        void set_someone_prepare_failed()
        {is_someone_prepare_failed_ = true;}
        //add e

        int is_truncate_conflict(uint64_t table_id, bool &is_truncate);
        int check_truncate_conflict(uint64_t &table_id, bool &is_truncate);

      public:
        const bool get_need_gen_mutator() const
        { return need_gen_mutator_; }
        ObUpsMutator &get_ups_mutator();
        TransUCInfo &get_uc_info();
        TEValueUCInfo *alloc_tevalue_uci();
        int init_lock_info(LockMgr& lock_mgr, const IsolationLevel isolation);
        ILockInfo *get_lock_info();
        int64_t get_min_flying_trans_id();
        void flush_min_flying_trans_id();
        sql::ObUpsResult &get_ups_result();
        const bool volatile &is_alive() const;
        bool is_killed() const;
        void set_frozen();
        bool is_frozen() const;
        void reset_stmt();
        uint32_t get_conflict_session_id() const
        { return last_conflict_session_id_; }
        bool set_conflict_session_id(uint32_t session_id)
        { 
          bool is_conflict_sid_changed = (last_conflict_session_id_ != session_id);
          last_conflict_session_id_ = session_id;
          return is_conflict_sid_changed;
        }

        int add_table_item(uint64_t table_id);//[244]

        //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
        void set_prepare();
        bool is_prepare() const;
        const common::ObTransID& get_coordinator_info()
        {return coordinator_tid_;}
        void set_coordinator_info(const common::ObTransID& trans_id)
        {coordinator_tid_ = trans_id;}
        /**
         * @brief set accept prepare ack, used for guard accept prepare ack
         * @param accept
         */
        void set_accept_prepare_ack(bool accept)
        {
            accept_prepare_ack_ = accept;
            YYSYS_LOG(DEBUG, "set accept %d", accept);
        }
        bool get_accept_prepare_ack()
        {return accept_prepare_ack_;}
        /**
         * @brief set writting trans, if set this mark , do not write end trans log any more
         * @param writting
         */
        void set_writting_trans_stat(bool writting)
        {
            writting_trans_stat_ = writting;
                     YYSYS_LOG(DEBUG, "set writting_trans_stat %d", writting);
        }
        bool get_writting_trans_stat()
        {return writting_trans_stat_;}

        bool *get_writting_trans_stat_for_sync()
        { return &writting_trans_stat_; }

        void set_write_trans_stat_time(int64_t timestamp)
        {
            last_write_trans_stat_time_ = timestamp;
                     YYSYS_LOG(DEBUG, "set writting_trans_stat %ld", timestamp);
        }
        int64_t get_write_trans_stat_time()
        {return last_write_trans_stat_time_;}

        int64_t *get_write_trans_stat_time_for_sync()
        { return &last_write_trans_stat_time_; }

        int32_t get_participant_num()
        {return participant_num_;}
        int set_participant_info(const common::ObEndTransReq &req);
        void set_participant_ack(const common::ObServer &participant);
        /**
         * @brief check received all participant prepare ack
         * @return true if received all else false
         */
        bool all_participant_ack();
        const common::ObTransID* get_participant_info()
        {return participant_trans_id_;}
        /**
         * @brief save ms end trans task
         */
        void set_task(void * task)
        {src_task_ = task;}
        void* get_task()
        {return src_task_;}
        /**
         * @brief used for sys UPS record write commit or rollback into sys table
         * @param commit
         */
        void set_trans_stat(const bool commit)
        {commit_stat_ = commit;}
        bool get_trans_stat()
        {return commit_stat_;}
        int64_t get_mem_version()
        {return mem_version_;}
        //add 20150701:e
    //add by maosy [MultiUps 1.0][secondary index optimize]20170401 b:
        int push_tevalues(TEValue* te_value)
        {
            int ret = tevalues_.push_back(te_value);
            return ret ;
        }
        TEValue* get_tevalues(int64_t index)
        {
            return tevalues_.at(index);
        }
        int64_t get_tevalues_count()
        {
            return tevalues_.count();
        }
        void clear_tevalues()
        {
            tevalues_.clear();
            tevalues_index_replace_.clear();
        }
        void set_tevalues_index(ObSEArray<int64_t,100>& tevalues_index)
        {
            tevalues_index_replace_ =tevalues_index ;
        }
        int64_t get_tevalues_index(int64_t index)
        {
            return tevalues_index_replace_.at(index);
        }
        //add by maosy e
        int push_tekey(TEKey &tekey)
        {
            int ret = tekeys_.push_back(tekey);
            return ret;
        }
        int64_t get_tekeys_count()
        {
            return tekeys_.count();
        }
        ObSEArray<TEKey, 100> *get_tekeys()
        {
            return &tekeys_;
        }

      private:
        common::ObSEArray<uint64_t, INIT_TABLE_MAP_SIZE> table_array_; //add [224]
        common::ModulePageAllocator mod_;
        common::ModuleArena page_arena_;
        common::ModuleArena stmt_page_arena_;
        PageArenaWrapper stmt_page_arena_wrapper_;
        volatile Stat stat_;
        volatile bool alive_flag_;
        uint32_t last_conflict_session_id_;
        bool commit_done_;
        const bool need_gen_mutator_;
        ObUpsMutator ups_mutator_;
        sql::ObUpsResult ups_result_;
        TransUCInfo uc_info_;
        ILockInfo *lock_info_;
        CallbackMgr stmt_callback_list_;
        CallbackMgr publish_callback_list_;
        CallbackMgr free_callback_list_;
        TEValueChecksumCallback checksum_callback_;
        CallbackMgr checksum_callback_list_;
        //add duyr [MultiUPS] [READ_ATOMIC] [write_part] 20151117:b
        TEValueTransverCallback trans_version_callback_;
        CallbackMgr trans_version_callback_list_;
        //add duyr 20151117:e
        int64_t mark_stmt_total_row_counter_;
        int64_t mark_stmt_new_row_counter_;
        uint64_t mark_stmt_checksum_;
        v4si mark_dml_count_;
        v4si dml_count_;
        //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
        //participant
        bool writting_trans_stat_;//limit write trans log, just allow one time
        bool accept_prepare_ack_;//used for coordinator guard accept prepare ack
        common::ObTransID coordinator_tid_;
        //coordinator
        //record participants info
        int32_t participant_num_;
        int64_t mem_version_;
        common::ObTransID participant_trans_id_[common::MAX_UPS_COUNT_ONE_CLUSTER];
        bool is_someone_prepare_failed_;
        //record received participant ack or not
        bool participant_ack_[common::MAX_UPS_COUNT_ONE_CLUSTER];
        bool is_rollback_dis_;
        int64_t last_write_trans_stat_time_;
        void* src_task_;
        bool commit_stat_;
        //add 20150701:e
    //add by maosy [MultiUps 1.0][secondary index optimize]20170401 b:
        ObSEArray<TEValue*,100> tevalues_;
        ObSEArray<int64_t,100> tevalues_index_replace_;
		// add by maosy e
        ObSEArray<TEKey, 100> tekeys_;
    };

    class RPSessionCtx: public RWSessionCtx
    {
      public:
        RPSessionCtx(const SessionType type, SessionMgr &host, common::FIFOAllocator &fifo_allocator): RWSessionCtx(type, host, fifo_allocator, false), is_replay_local_log_(false)
        {}
        ~RPSessionCtx()
        {}
        bool is_replay_local_log() const
        { return is_replay_local_log_; }
        void set_replay_local_log(bool is_replay_local_log)
        { is_replay_local_log_ = is_replay_local_log; }
        void kill()
        { YYSYS_LOG(WARN, "replay_session can not be killed: %s", to_cstring(*this)); }
      private:
        bool is_replay_local_log_;
    };

    class SessionCtxFactory : public ISessionCtxFactory
    {
      //mod zhaoqiong [fifo allocator bug] 20161118:b
      //static const int64_t ALLOCATOR_TOTAL_LIMIT = 5L * 1024L * 1024L * 1024L; 
      //static const int64_t ALLOCATOR_HOLD_LIMIT = static_cast<int64_t>(1.5 * 1024L * 1024L * 1024L); //1.5G //ALLOCATOR_TOTAL_LIMIT / 2;
      static const int64_t ALLOCATOR_TOTAL_LIMIT = 40L * 1024L * 1024L * 1024L;
      static const int64_t ALLOCATOR_HOLD_LIMIT = ALLOCATOR_TOTAL_LIMIT / 2; //ALLOCATOR_TOTAL_LIMIT / 2;
      //mod:e
      static const int64_t ALLOCATOR_PAGE_SIZE = 16L * 1024L * 1024L;
      public:
        SessionCtxFactory();
        ~SessionCtxFactory();
      public:
        BaseSessionCtx *alloc(const SessionType type, SessionMgr &host);
        void free(BaseSessionCtx *ptr);
      private:
        common::ModulePageAllocator mod_;
        common::ModuleArena allocator_;
        common::FIFOAllocator ctx_allocator_;
    };
  }
}

#endif //OCEANBASE_UPDATESERVER_SESSIONCTX_FACTORY_H_
