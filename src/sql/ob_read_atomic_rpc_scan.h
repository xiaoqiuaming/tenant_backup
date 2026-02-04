//add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151229:b
#ifndef OB_READ_ATOMIC_RPC_SCAN_H
#define OB_READ_ATOMIC_RPC_SCAN_H

#include "sql/ob_rpc_scan.h"

namespace oceanbase
{
  namespace sql
  {

    /**
      *  when use the hint grammar(READ_ATOMIC_WEAK/READ_ATOMIC_STRONG),
      *it will use the read atomic arithmetic to read data!
      *  if don't use the hint grammar,
      *it just reuse the ObRpcScan to read data!
      *  it's use method is the same as ObRpcScan:
      *1:call set_table;
      *2:call init;
      *3:call open;
      *4:call get_next_row;
      *5:call close;
      */
    class ObReadAtomicRpcScan : public ObRpcScan
    {
    private:
      //num of each row's previous commit trans versions!
      //the max num is common::ObReadAtomicParam::MAX_TRANS_VERSET_SIZE
      static const int64_t PREV_COMMIT_TRANSVER_COUNT = 5;
      static const int64_t HASH_BUCKET_NUM = 1024;
      //c1 in (1,2,3),so the in expr len is 3!
      static const int64_t MAX_IN_EXPR_LENGTH = 200;
      //c1 in (1,2,3),if the c1 is rowkey,
      //the rowkey cell size =sizeof(1)+sizeof(2)+sizeof(3)
      static const int64_t MAX_ROWKEY_CELL_SIZE = 1.5 * 1024 * 1024;
      //1.5M
      static const uint64_t SYS_TABLE_TRANSVER_STAT_TID = OB_UPS_SESSION_INFO_TID;

      enum ReadRoundState
      {
        NONE_READ_ROUND = 0, FIRST_READ_ROUND, SECOND_READ_ROUND
      };

      enum FetchTransverStatMeth
      {
        NO_FETCH_TRANSVER = 0, FETCH_FROM_CACHE, FETCH_FROM_SYS_TABLE
      };

      struct DataMarkState
      {
        static const int64_t MAX_MARK_STAT_BUF_SIZE = 5 * 1024;
        int64_t first_read_round_row_num_;
        int64_t second_read_round_row_num_;

        DataMarkState() : first_read_round_row_num_(0), second_read_round_row_num_(0)
        {
        }

        inline bool is_valid() const
        {
          return (first_read_round_row_num_ > 0 || second_read_round_row_num_ > 0);
        }

        const char *to_cstring() const
        {
          static const int64_t BUFFER_NUM = 3;
          static __thread char buff[BUFFER_NUM][MAX_MARK_STAT_BUF_SIZE];
          static __thread int64_t i = 0;
          i++;
          memset(buff[i % BUFFER_NUM], 0, MAX_MARK_STAT_BUF_SIZE);
          to_string(buff[i % BUFFER_NUM], MAX_MARK_STAT_BUF_SIZE);
          return buff[i % BUFFER_NUM];
        }

        int64_t to_string(char *buf, int64_t buf_len) const
        {
          int64_t pos = 0;
          databuff_printf(buf, buf_len, pos, "DataMarkState(");
          databuff_printf(buf, buf_len, pos, "first_read_round_row_num_=%ld,", first_read_round_row_num_);
          databuff_printf(buf, buf_len, pos, "second_read_round_row_num_=%ld,", second_read_round_row_num_);
          databuff_printf(buf, buf_len, pos, ")");
          return pos;
        }
      };

      typedef common::hash::ObHashMap<common::ObTransVersion, common::ObTransVersion::TransVerState, common::hash::NoPthreadDefendMode> TransVersionMap;
      typedef common::hash::ObHashMap<int64_t, common::ObReadAtomicDataMark, common::hash::NoPthreadDefendMode> PaxosDataMarkMap;

      typedef common::hash::ObHashMap<common::ObReadAtomicDataMark, DataMarkState, common::hash::NoPthreadDefendMode> DataMarkStatMap;
    public:
      ObReadAtomicRpcScan();

      virtual ~ObReadAtomicRpcScan();

      virtual void reset();

      virtual void reuse();

      int init(ObSqlContext *context, const common::ObRpcScanHint *hint = NULL);

      virtual int open();

      virtual int close();

      virtual ObPhyOperatorType get_type() const
      {
        return PHY_READ_ATOMIC_RPC_SCAN;
      }

      virtual int get_next_row(const common::ObRow *&row);

      virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;

      int add_output_column(const ObSqlExpression &expr,bool change_tid_for_storing = false); // mod  

      void set_rowkey_cell_count(const int64_t rowkey_cell_count);

      //启用read atomic算法时，暂时不支持group by 和聚集函数下压
      int add_filter(ObSqlExpression *expr);

      int add_group_column(const uint64_t tid, const uint64_t cid);

      int add_aggr_column(const ObSqlExpression &expr);


      int64_t to_string(char *buf, const int64_t buf_len) const;

      DECLARE_PHY_OPERATOR_ASSIGN;

    private:
      //disallow copy
      ObReadAtomicRpcScan(const ObReadAtomicRpcScan &other);

      ObReadAtomicRpcScan &operator=(const ObReadAtomicRpcScan &other);

    private:
      int init_read_atomic_param_(const common::ObReadAtomicLevel level, const ReadRoundState read_round,
                                  common::ObReadAtomicParam *des_param);

      int init_second_read_(ObSqlContext *context);

      int init_fetch_transver_state_(ObSqlContext *context, const FetchTransverStatMeth fetch_meth);

      int init_fetch_transver_state_rpc_(ObSqlContext *context);

      int store_prepare_output_column_exprs_(const ObSqlExpression &expr);

      int store_prepare_output_cid_(const uint64_t tid, const uint64_t cid);

      int internal_add_output_column_(const ReadRoundState read_round);

      int add_rowkey_output_column_(const ReadRoundState read_round);

      int add_extend_output_column_(const ReadRoundState read_round);

      int add_prepare_output_column_(const ReadRoundState read_round);

      int add_special_output_column_(const uint64_t tid, const uint64_t cid, const ReadRoundState read_round);

      int add_second_read_rpc_filter_(bool &need_add_filter_again, const bool is_first_time);

      int cons_new_in_expr_with_first_read_row_(ObSqlExpression &new_in_expr_filter, bool &need_add_filter_again,
                                                const bool is_first_time);

      int cons_final_row_desc_();

      bool is_prepare_data_column_(const uint64_t tid, const uint64_t cid);

      bool is_output_column_(const uint64_t tid, const uint64_t cid);

      int internal_get_next_row_(const common::ObRow *&row);

      int execute_first_round_read_();

      int fetch_transver_final_stat_(const FetchTransverStatMeth fetch_meth);

      int fetch_transver_final_stat_from_cache_();

      int fetch_transver_final_stat_from_systable_();

      int fill_exact_data_mark_set_(common::ObReadAtomicParam &read_atomic_param);

      int fill_exact_commit_transver_set_(common::ObReadAtomicParam &read_atomic_param);

      int add_fetch_transver_stat_rpc_filter_(TransVersionMap::const_iterator &iter,
                                              TransVersionMap::const_iterator end, const bool is_first_time);

      int finish_final_row_(const bool is_first_time);

      int execute_second_round_read_(const bool is_first_time, const common::ObRow *&second_read_row);

      int conver_to_final_row_(const common::ObRow &src_row, const common::ObReadAtomicParam *param,
                               bool &is_final_row_exist);

      int filter_final_row_(bool &is_final_row_output);

      int set_transver_state_(const common::ObTransVersion &trans_ver,
                              const common::ObTransVersion::TransVerState ver_state);

      bool is_committed_transver_(const common::ObTransVersion &trans_ver);

      int set_data_mark_state_(const common::ObReadAtomicDataMark &data_mark, const ReadRoundState read_round);

      int set_paxos_data_mark_and_check_safe(const common::ObReadAtomicDataMark &data_mark, const ReadRoundState read_round);


      inline common::ObReadAtomicParam *get_first_read_atomic_param_()
      {
        common::ObReadAtomicParam *ret = NULL;
        ret = ObRpcScan::get_read_atomic_param();
        return ret;
      }

      inline const common::ObReadAtomicParam *get_first_read_atomic_param_() const
      {
        const common::ObReadAtomicParam *ret = NULL;
        ret = ObRpcScan::get_read_atomic_param();
        return ret;
      }

      inline common::ObReadAtomicParam *get_second_read_atomic_param_()
      {
        common::ObReadAtomicParam *ret = NULL;
        ret = second_read_rpc_scan_.get_read_atomic_param();
        return ret;
      }

      inline const common::ObReadAtomicParam *get_second_read_atomic_param_() const
      {
        const common::ObReadAtomicParam *ret = NULL;
        ret = second_read_rpc_scan_.get_read_atomic_param();
        return ret;
      }

      int finish_in_expr_left_param_(const ObRowDesc &row_desc, const common::ObRowkeyInfo &rowkey_info,
                                     const uint64_t table_id, ObSqlExpression &in_expr);

      int add_in_expr_mid_param_(const common::ObRow &row, const ObRowDesc &row_desc,
                                 const common::ObRowkeyInfo &rowkey_info, const uint64_t table_id,
                                 ObSqlExpression &in_expr, int64_t &rowkey_cell_size);

      int finish_in_expr_end_param_(const int64_t row_num, ObSqlExpression &in_expr);

      bool need_second_read_() const;

      bool need_fetch_transver_state(FetchTransverStatMeth &fetch_meth) const;

      bool is_first_read_in_time_limit() const;

      bool need_fetch_sys_table() const;

      bool is_consistent_data_checked_with_data_mark();

      int clear_with_read_atomic_();

      //for debug!!!
    private:
      void dump_transver_state_(const int32_t log_level = YYSYS_LOG_LEVEL_DEBUG);

      void dump_data_mark_state_(const int32_t log_level = YYSYS_LOG_LEVEL_DEBUG);

    private:
      bool is_inited_;
      bool is_first_read_rpc_opened_;      /*在第一轮读rpc_scan open成功之后，设置为true,表示一轮读完成*/
      bool is_with_read_atomic_;

      //following vals is useful only when is_withread_atomic_ == true
      bool is_output_cols_added_;
      bool is_first_read_finish_;        /*一轮读是否完毕*/
      bool is_second_read_filter_added_;
      bool is_second_read_rpc_opend_;
      bool second_read_need_filter_again_;
      uint64_t min_used_column_id_;
      uint64_t max_used_column_id_;
      uint64_t total_base_column_num_;
      common::ObRpcScanHint first_read_hint_;
      common::ObRpcScanHint second_read_hint_;
      /*等于first_read_hint_ */

      common::ObRow final_row_;
      common::ObRowDesc final_row_desc_; /**/

      //store all output cid added by user!
      common::ObRowDesc orig_output_column_ids_;
      //store all exprs from add_output_column fun
      common::ObSEArray<ObSqlExpression, OB_PREALLOCATED_NUM, common::ModulePageAllocator, ObArrayExpressionCallBack<ObSqlExpression> > orig_output_column_exprs_;

      /**
        *1:not allow to modify or destry these filters,
        *these filters will be destried by ObRpcScan,they are readonly here!!!!
        *2:can't copy thest filters then use them!
        *because these filters may changed by other ObPhyOperator!
        *(such as filters will be change by ObMultiBind when the filter is sub select!)
        *if we copy then use,the filters maybe wrong!
        */
      common::ObSEArray<const ObSqlExpression *, OB_PREALLOCATED_NUM, common::ModulePageAllocator> orig_filters_;


      //used to store all prepare cid!
      common::ObSEArray<ObSqlExpression, OB_PREALLOCATED_NUM, common::ModulePageAllocator, ObArrayExpressionCallBack<ObSqlExpression> > prepare_output_column_exprs_;
      common::ObRowDesc prepare_output_cids_;

      common::ObRowStore first_read_row_cache_;
      /*存放一轮读 所有的数据*/
      common::ObRow first_read_cache_row_;
      //store the state of each transversion
      bool is_transver_stat_map_created_;
      TransVersionMap transver_state_map_;
      /*保存了 trans_version 对应的状态（PREPARE_STATE or COMMIT_STATE）*/
      PaxosDataMarkMap paxos_data_mark_map_;

      //ObReadAtomicRpcScan is the first_read_rpc_scan
      //used to second round read data!
      ObRpcScan second_read_rpc_scan_;

      //store the data mark from first round read and second read!
      DataMarkStatMap data_mark_state_map_;
      /*paxos_data_mark_map*/

      //used to read sys table __ups_session_info!
      ObRpcScan fetch_transver_stat_rpc_;
      common::ObRowkeyInfo sys_table_rowkey_info_;
      common::ObRowDesc sys_table_row_desc_;

      int64_t first_rpc_scan_start_time_;

    };
  }//end namespace sql
}//end namespace oceanbase

#endif // OB_READ_ATOMIC_RPC_SCAN_H
//add duyr 20151229:e
