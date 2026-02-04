/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_executor.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_UPS_EXECUTOR_H
#define _OB_UPS_EXECUTOR_H 1
#include "ob_no_children_phy_operator.h"
#include "mergeserver/ob_ms_rpc_proxy.h"
#include "ob_physical_plan.h"
#include "ob_result_set.h"
#include "ob_sql_session_info.h"
#include "common/ob_calc_info.h"
#include "mergeserver/ob_ms_partition_manager.h"
#include "sql/ob_basic_stmt.h"
//add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160321:b
#include "ob_ups_modify.h"
#include "ob_table_rpc_scan.h"
//add 20160321:e

namespace oceanbase
{
  namespace sql
  {
    //add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160425:b
    struct TransReturnValueInfo
    {
        int64_t paxos_id_;
        int ret_;
        TransReturnValueInfo():paxos_id_(OB_INVALID_PAXOS_ID),ret_(OB_SUCCESS)
        {}
    };
    //add 20160425:e
    class ObUpsExecutor: public ObNoChildrenPhyOperator
    {
      public:
        ObUpsExecutor();
        virtual ~ObUpsExecutor();
        virtual void reset();
        virtual void reuse();
        void set_rpc_stub(mergeserver::ObMergerRpcProxy* rpc)
        {rpc_ = rpc;}
        void set_inner_plan(ObPhysicalPlan *plan)
        {inner_plan_ = plan;}
        ObPhysicalPlan *get_inner_plan()
        { return inner_plan_; }
        //add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20151123:b
        enum InnerPlanType
        {
          PRE_INNER_PLAN = 0,//for pre execution plan
          NORMAL_INNER_PLAN, //for normal execution plan
          FULL_ROW_INNER_PLAN//for full row execution plan
        };
        /**
         * @brief set sql_context when transformer,in open(),get partition monitor and
         *        partition manager info
         * @param sql_context [IN]
         */
        void set_sql_context(ObSqlContext *sql_context)
        {sql_context_ = sql_context;}
        //add 20151123:e

        /// execute the insert statement
        virtual int open();
        virtual int close()
        {return common::OB_SUCCESS;};
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;

        int get_next_row_for_update_rowkey(const common::ObRow *&row,const ObRowDesc &fake_row_desc);

        virtual int get_next_row(const common::ObRow *&row);
        //add wangy b [sequence]
        int get_next_row_for_sequence(const common::ObRow *&row);
        int get_next_row_for_prevval(const common::ObRow *&row);
        //add e
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const
        {
            UNUSED(row_desc);
            return common::OB_NOT_SUPPORTED;
        }
        virtual enum ObPhyOperatorType get_type() const
        {return PHY_UPS_EXECUTOR;};
        void get_output_infor(int64_t& batch_num,int64_t& inserted_row_num);//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20150423
        //add gaojt [Delete_Update_Function] [JHOBv0.1] 20151008:b
        //void set_is_delete_update(bool is_delete_update){is_delete_update_ = is_delete_update;};
        void set_sub_query_num(int64_t sub_query_num)
        {sub_query_num_ = sub_query_num;};
        //add gaojt 20151008:e
        DECLARE_PHY_OPERATOR_ASSIGN;
      private:
        // types and constants
      private:
        // disallow copy
        ObUpsExecutor(const ObUpsExecutor &other);
        ObUpsExecutor& operator=(const ObUpsExecutor &other);
        // function members
        int make_fake_desc(const int64_t column_num);
        int set_trans_params(ObSQLSessionInfo *session, common::ObTransReq &req);
        //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
        /**
         * @brief execute sub trans on one ups; move sending physical plan to ups function
         *        in original open() to this indepandent func, only change
         *        the rule to judge whether need start new trans
         * @param paxos_id current values' paxos id
         * @param split_values mark original values splited into multi values
         * @param outer_result_set
         * @param session
         * @return
         */
        int execute_sub_trans(
            //add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160425:b
            ObPhysicalPlan *& final_inner_plan,
            TransReturnValueInfo& trans_return_value_info_,
            //add 20160425:e
            const int32_t paxos_id,
            const bool split_values,
            ObResultSet*& outer_result_set,
            ObSQLSessionInfo*& session);
        /**
         * @brief rollback distributed trans when current stmt execute failed
         */
        void rollback_distributed_trans();
        /**
         * @brief check need start new sub trans
         * @param [in]paxos_id
         * @param [in]split_values
         * @return need_start_new_trans
         */
        bool need_start_new_trans(const int32_t paxos_id, const bool split_values);
        /**
         * @brief end trans used for one stmt split into multi stmts,
         *        need use this func end
         * @return
         */
        int commit_trans();
        //add 20150701:e
        //add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20151123:b
      public:
        void set_pre_execution_plan(ObPhysicalPlan *pre_execution_plan)
        {pre_execution_plan_ = pre_execution_plan;}
        ObPhysicalPlan *get_pre_execution_plan(void)
        {return pre_execution_plan_;}
        void set_full_row_execution_plan(ObPhysicalPlan *full_row_exe_plan)
        {full_row_execution_plan_ = full_row_exe_plan;}
        ObPhysicalPlan *get_full_row_execution_plan(void)
        {return full_row_execution_plan_;}
      private:

        int choose_execution_plan(ObPhysicalPlan *& final_execution_plan);
        int execute_physical_plan(ObPhysicalPlan *& final_execution_plan,
                                  ObResultSet *outer_result_set,
                                  ObSQLSessionInfo *session);

        int clear_sub_trans_return_info();
        bool is_all_pre_sub_trans_return_success();
        bool skip_execute_cur_sub_trans(int64_t paxos_id);
        bool can_execute_full_row_execution_plan();

        /**
         * only record the sub trans return info when the subtrans return OB_SUCCESS or OB_INCOMPLETE_ROW
         */
        int record_sub_trans_return_info(TransReturnValueInfo& trans_return_value_info, int ret);

        /**
         * @brief judge if the table is a rule changed table
         */
        int get_is_rule_change_table(uint64_t table_id,
                                     ObExprValues& expr_valus_op,
                                     ObBasicStmt::StmtType stmt_type,
                                     bool& is_execute_pre_plan);

        /**
         * @brief get partition type of current table(the table which will be replaced or inserted of updated or deleted)
         * @param has_no_partition [OUT] true if current table has no partition
         * @return OB_SUCCESS if success or other error code
         */
        int get_partition_type(ObPhysicalPlan *& final_inner_plan, bool &has_no_partition);

        /**
         * @brief init the partition manager for getting paxos_id
         */
        int check_partition_manager();

        /**
         * @brief current table has no rule,execute like sigle UPS
         * @return OB_SUCCESS if success or other error code
         * @param outer_result_set [IN]
         * @param session [IN]
         */
        int execute_trans_without_partition(ObPhysicalPlan *& final_inner_plan, ObResultSet *outer_result_set, ObSQLSessionInfo *session);

        /**
         * @brief current table has partition,execute sub_trans to different UPS by paxos_id
         * @param sub_ob_expr_values_row_stores [IN] rows come from ObExprValues
         * @param sub_ob_values_row_stores [IN] rows come from ObValues, if REPLACE stmt,no ObValues rows here
         * @param affect_nums_total [OUT] all rows effected in different UPS
         * @param warnning_nums_total [OUT] all warnings
         * @param outer_result_set [IN]
         * @param session [IN]
         */
        int execute_trans_with_partition(ObPhysicalPlan *& final_inner_plan,
                                         const ObArray<ObSubRowStore>& sub_ob_expr_values_row_stores,
                                         const ObArray<ObSubRowStore>& sub_ob_values_row_stores,
                                         ObResultSet *outer_result_set,
                                         ObSQLSessionInfo *session
                                         );
         // add by maosy [MultiUps 1.0] [batch_udi] 20170417 b
        /**
         * @brief batch current table has partition,execute sub_trans to different UPS by paxos_id
         * @param affect_nums_total [OUT] all rows effected in different UPS
         * @param warnning_nums_total [OUT] all warnings
         * @param outer_result_set [IN]
         * @param session [IN]
         */
        int execute_batch_trans_with_partition(ObPhysicalPlan *& final_inner_plan,
                                               ObResultSet *outer_result_set,
                                               ObSQLSessionInfo *session
                                               );
		// add by maosy 20170417 e


        /**
         * @brief bind static values for current sub_trans into ObValues op
         * @param sub_row_store [IN] row_set of current sub_trans
         * @param static_values [IN] the op ObValues
         * @return OB_SUCCESS if success or other error code
         */
        int bind_static_values(const ObSubRowStore &sub_row_store, ObValues *& static_values);

        /**
         * @brief bind input values for current sub_trans into ObExprValues op
         * @param sub_row_store [IN] row_set of current sub_trans
         * @param input_values [IN] the op ObExprValues
         * @return OB_SUCCESS if success or other error code
         */
        int bind_expr_values(const ObSubRowStore &sub_row_store, ObExprValues *& input_values);

        /**
         * @brief resolve(split) the inc data which come from the client into sub_set,
         *        the rows with the same rowkey will be split into the same sub_set with
         *        the same paxos id.
         * @param sub_ob_expr_values_row_stores [OUT] in with empty,out with the sub_set rows
         * @param paxos_id_array [INI,OUT] in with empty,out with the paxos ids,which has the same size as "sub_ob_expr_values_row_stores"
         * @param mem_allocator [IN] deep copy row obj for string
         * @return OB_SUCCESS if success or other error code
         */
        int resolve_input_values(ObPhysicalPlan *& final_inner_plan,
                                 ObArray<ObSubRowStore>& sub_ob_expr_values_row_stores,
                                 ObArray<int64_t>& paxos_id_array,
                                 common::ModuleArena &mem_allocator);

        /**
         * @brief resolve(split) the static data which come from query(CS RPC) into sub_set,
         *        the rows with the same paxos_is will be split into the same sub_set,
         *        the rows in ObValues has the same order as the rows in the ObExprValues.
         * @param sub_ob_values_row_stores [OUT] in with empty,out with the sub_set rows
         * @param paxos_id_array [IN] assist to spilt the ObValues into sub_set by paxos_id
         * @param mem_allocator [IN] deep copy row obj for string
         * @return OB_SUCCESS if success or other error code
         */
        int resolve_static_values(ObPhysicalPlan *& final_inner_plan,
                                  ObArray<ObSubRowStore>& sub_ob_values_row_stores,
                                  const ObArray<int64_t>& paxos_id_array,
                                  common::ModuleArena &mem_allocator);

        /**
         * @brief assign paxos id to each row of input array
         * @param expr_values_stores [IN][OUT] out with paxos id for each row
         */
        int assign_paxos_id(ObPhysicalPlan *& final_inner_plan, ObArray<ObRow>& expr_values_stores);
        int assign_paxos_id(ObArray<ObRow>& expr_values_stores, const ObArray<int64_t>& paxos_ids);

        /**
         * @brief split the whole rows into sub set by paxos id,each sub set has the same paxos id,
         *        which will be send to the same UPS
         * @param values_stores [IN] the rows will be spilted
         * @param sub_ob_expr_values_row_stores [OUT] in with empyt,out with all sub_set rows
         * @param paxos_id_array [OUT] out with the paxos ids for each sub_set rows
         */
        int distribute_values_row_store(const ObArray<ObRow>& values_stores,
                                        ObArray<ObSubRowStore>& sub_ob_expr_values_row_stores,
                                        ObArray<int64_t>& paxos_id_array);

        /**
         * @brief get the index of current paxos id in the sub_row_store_array
         * @param sub_row_store_array [IN]
         * @param paxos_id [IN] the paxos_id to find
         * @param index [OUT] negative for not found,positive number if found
         * @return OB_SUCCESS if success or other error code
         */
        int find_paxos_id(const ObArray<ObSubRowStore> &sub_row_store_array,const int64_t paxos_id,int64_t &index);

        int check_paxos_ids(const ObArray<int64_t>& input_values_paxos_ids,
                            const ObArray<int64_t>& static_values_paxos_ids);
        //add 20151123:e
        int null_check_error();
      private:
        // data members
        mergeserver::ObMergerRpcProxy* rpc_;
        ObPhysicalPlan *inner_plan_;
        ObUpsResult local_result_;
        common::ObRow curr_row_;
        common::ObRowDesc row_desc_;
    //add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20151123:b
        ObSqlContext * sql_context_;
        ObPhysicalPlan *pre_execution_plan_;
        ObPhysicalPlan *full_row_execution_plan_;
        bool executing_full_row_plan_;//ture:in full row execution plan,false:pre or raw plan
        int64_t sub_trans_num_;
        InnerPlanType inner_plan_type_;
        int64_t total_affected_num_;
        int64_t total_warning_count_;
        ObArray<TransReturnValueInfo> sub_trans_return_value_info_;//for pre plan and full plan
        //add 20151123:e
        int64_t insert_select_batch_num_;//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20150423
        int64_t inserted_row_num_;//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20150424
        //add gaojt [Delete_Update_Function] [JHOBv0.1] 20151010:b
        int64_t sub_query_num_;
        //bool is_delete_update_;
        bool is_row_num_null_;
        //add gaojt 20151010:e
        bool is_multi_batch_;
        bool using_hot_update_;  //add hongchen [HOT_UPDATE_SKEW] 20170724
        bool enable_dis_trans_;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_UPS_EXECUTOR_H */
