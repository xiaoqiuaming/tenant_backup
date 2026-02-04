/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_transformer.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#ifndef _OB_TRANSFORMER_H
#define _OB_TRANSFORMER_H

#include "ob_phy_operator.h"
#include "ob_logical_plan.h"
#include "ob_multi_phy_plan.h"
#include "ob_multi_logic_plan.h"
#include "ob_sql_context.h"
#include "ob_insert_stmt.h"
#include "ob_show_stmt.h"
#include "ob_values.h"
#include "ob_expr_values.h"
#include "ob_table_rpc_scan.h"
#include "common/ob_list.h"
#include "common/ob_row_desc_ext.h"
#include "common/ob_se_array.h"
#include "ob_join.h"
#include "common/ob_sql_ups_rpc_proxy.h"
//add wenghaixing for fix insert bug decimal key 2014/10/11
#include "ob_postfix_expression.h"
//add e
//add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160118:b
#include "ob_update_stmt.h"
//add 20160118:e
//add liumengzhan_delete_index
#include "ob_delete_index.h"
#include "ob_index_trigger_upd.h"
//add:e
//add fanqiushi_index
#include "ob_sql_expression.h"
#include "common/page_arena.h"
#include "common/ob_array.h"
//add:e
//add lijianqiang [sequence] 20150717:b
#include "ob_sequence.h"
//add 20150717:e
#include "ob_fill_values.h"//add gaojt [Delete_Update_Function] [JHOBv0.1] 20150907
#include "common/ob_row_desc.h"
#include "ob_index_trigger_rep.h"//add by maosy [MultiUps 1.0] [hot update and secondary index] 20170415 e
#include "ob_update_rowkey.h"
#include "ob_optimizer_relation.h"
#include "ob_create_view_stmt.h"
#include "ob_create_view.h"


namespace oceanbase
{
  namespace common {
      template <>
      struct ob_vector_traits< ObBitSet<> >
      {
          typedef ObBitSet<>* pointee_type;
          typedef ObBitSet<> value_type;
          typedef const ObBitSet<> const_value_type;
          typedef value_type* iterator;
          typedef const value_type* const_iterator;
          typedef int32_t difference_type;
      };
  }
  namespace sql
  {
    class ObWhenFilter;
    class ObMergeJoin;
    class ObBloomFilterJoin;
    class ObHashJoinSingle;
//add wanglei:b
class ObSemiJoin;
//add:e
    class ObIndexTableInfo;
    class ObOptimizerRelation;
    class ObStatSelCalculator;
    class ObSelInfo;
    class ObSort;
    class ObTransformer
    {
      public:
      //mod peiouya [NotNULL_check] [JHOBv0.1] 20131222:b
      /*expr: it is convenient to use if context is added to transformer construct*/
      //ObTransformer(ObSqlContext &context);
        ObTransformer(ObSqlContext &context, ObResultSet & result);
      //mod 20131222:e
        virtual ~ObTransformer();
        //add fanqiushi_index
        typedef common::ObArray<ObSqlExpression,ModulePageAllocator, ObArrayExpressionCallBack<ObSqlExpression> >  Expr_Array;
        //add:e
    //add wanglei
    typedef common::ObArray<uint64_t>  Join_column_Array;
    //add:e
        int generate_physical_plans(
            ObMultiLogicPlan &logical_plans,
            ObMultiPhyPlan &physical_plans,
            ErrStat& err_stat);

        int gen_physical_select(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            const uint64_t& query_id,
            int32_t* index,
            bool optimizer_open = false
                );

        int gen_physical_select_for_update_rowkey(ObLogicalPlan *logical_plan,
                                                 ObPhysicalPlan *physical_plan,
                                                 ObUpdateRowkey *&op_update_rowkey,
                                                 ErrStat &err_stat,
                                                 const uint64_t &query_id,
                                                 int32_t *index);
        //add fanqiushi_index
        bool decide_is_use_storing_or_not_V2(Expr_Array  *filter_array,
                                             Expr_Array *project_array,
                                             uint64_t &index_table_id,
                                             uint64_t main_tid,
                                             Join_column_Array *join_column,//add wanglei for semi join
                                             ObStmt *stmt,//add wanglei for semi join
                                             //add liumz, [optimize group_order by index]20170419:b
                                             ObLogicalPlan *logical_plan,
                                             Expr_Array *order_array,
                                             Expr_Array *group_array
                                             //add:e
                                             );
        //add wenghaixing [secondary index for paper]20150505
        bool if_rowkey_in_expr(Expr_Array  *filter_array,uint64_t main_tid);
        //add e
        bool is_wherecondition_have_main_cid_V2(Expr_Array *filter_array,uint64_t main_cid);
        bool is_expr_can_use_storing_V2(ObSqlExpression c_filter,uint64_t mian_tid,uint64_t &index_tid,Expr_Array * filter_array,Expr_Array *project_array);
        bool is_index_table_has_all_cid_V2(uint64_t index_tid,Expr_Array *filter_array,Expr_Array *project_array);
        int64_t is_cid_in_index_table(uint64_t cid,uint64_t tid);
         //add:e
        //add liumz, [optimize group_order by index]20170419:b
        int optimize_order_by_index(ObArray<uint64_t> &idx_tids, uint64_t main_tid, uint64_t &index_table_id, bool &hit_index_ret, ObStmt *stmt, ObLogicalPlan *logical_plan);
        int optimize_group_by_index(ObArray<uint64_t> &idx_tids, uint64_t main_tid, uint64_t &index_table_id, bool &hit_index_ret, ObStmt *stmt, ObLogicalPlan *logical_plan);
        //add:e
        //add wanglei [second index fix] 20160425:b
         bool is_expr_has_more_than_two_columns(ObSqlExpression * expr);
         int64_t get_type_num(int64_t idx,int64_t type,ObSEArray<ObObj, 64> &expr_);
        //add wanglei [second index fix] 20160425:e
        //add wanglei:b
        //bool is_joincondition_have_main_cid(Join_Expr_Array *join_filter_array,uint64_t main_cid);
        bool is_this_expr_can_use_index_for_join(uint64_t cid,
                                                 uint64_t &index_tid,
                                                 uint64_t main_tid,
                                                 const ObSchemaManagerV2 *sm_v2);
        bool is_expr_can_use_storing_for_join(uint64_t cid,
                                              uint64_t mian_tid,
                                              uint64_t &index_tid,
                                              Expr_Array * filter_array,
                                              Expr_Array *project_array);
        //add:e
        ObSqlContext* get_sql_context();

        bool is_enable_index_for_one_table(ObLogicalPlan *logical_plan,
                                           ObPhysicalPlan *physical_plan,
                                           ObStmt *stmt,
                                           ObOptimizerRelation *rel_opt,
                                           uint64_t table_id,
                                           ObArray<ObIndexTableInfo> &can_used_index_table_array);
        bool decide_is_use_storing_or_not_V3(Expr_Array *filter_array,
                                             Expr_Array *project_array,
                                             ObArray<ObIndexTableInfo> &can_used_index_table_array,
                                             uint64_t main_tid,
                                             Join_column_Array *join_column,
                                             ObStmt *stmt,
                                             ObLogicalPlan *logical_plan,
                                             Expr_Array *order_array,
                                             Expr_Array *group_array);
        bool is_expr_can_use_storing_V3(ObSqlExpression c_filter,
                                        uint64_t mian_tid,
                                        ObArray<uint64_t> &index_tid_array,
                                        Expr_Array *filter_array,
                                        Expr_Array *project_array);
        bool is_this_expr_can_use_index_for_joinV2(uint64_t cid,
                                                   ObArray<uint64_t> &can_used_index_table_array,
                                                   uint64_t main_tid,
                                                   const ObSchemaManagerV2 *sm_v2);
        bool is_expr_can_use_storing_for_joinV2(uint64_t cid,
                                                uint64_t mian_tid,
                                                ObArray<uint64_t> &can_used_index_table_array,
                                                Expr_Array *filter_array,Expr_Array *project_array);
        bool is_can_use_hint_for_storing_V3(Expr_Array *filter_array,
                                            Expr_Array *project_array,
                                            uint64_t index_table_id,
                                            Join_column_Array *join_column,
                                            ObStmt *stmt,
                                            ObArray<ObIndexTableInfo> &can_used_index_table_array);
        bool is_can_use_hint_index_V3(Expr_Array *filter_ayyay,
                                      uint64_t index_table_id,
                                      Join_column_Array *join_column,
                                      ObStmt *stmt,
                                      ObArray<ObIndexTableInfo> &can_used_index_table_array);

        int add_semi_join_expr_V2(ObLogicalPlan *logical_plan,
                                  ObPhysicalPlan *physical_plan,
                                  ObSemiJoin &join_op,
                                  ObSort &l_sort,
                                  ObSort &r_sort,
                                  ObSqlRawExpr &expr,
                                  const bool is_table_expr_same_order,
                                  oceanbase::common::ObList<ObSqlRawExpr *> &remainder_cnd_list,
                                  bool &is_add_other_join_cond,
                                  ObJoin::JoinType join_type,
                                  ObSelectStmt *select_stmt,
                                  int id,
                                  ObJoinTypeArray &hint_temp);
        ObStatExtractor * get_stat_extractor(){return &stat_extractor_;}
        int optimize_order_by_index_V2(ObArray<ObIndexTableInfo> &index_table_info_array,
                                       uint64_t main_tid,
                                       ObStmt *stmt,
                                       ObLogicalPlan *logical_plan);
        int optimize_group_by_index_V2(ObArray<ObIndexTableInfo> &index_table_info_array,
                                       uint64_t main_tid,
                                       ObStmt *stmt,
                                       ObLogicalPlan *logical_plan);

      private:
        DISALLOW_COPY_AND_ASSIGN(ObTransformer);
        void *trans_malloc(const size_t nbyte);
        void trans_free(void* p);

        int generate_physical_plan(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan*& physical_plan,
            ErrStat& err_stat,
            const uint64_t& query_id = common::OB_INVALID_ID,
            int32_t* index = NULL,
            bool optimizer_open = true
                );

        int add_cur_time_plan(
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            //mod liuzy [datetime func] 20150909:b
            /*Exp: modify "ObCurTimeType& " to "ObArray<ObCurTimeType>& "*/
//            const ObCurTimeType& type);
            const ObArray<ObCurTimeType>& type);
            //mod 20150909:b
/*
        int gen_physical_replace(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            const uint64_t& query_id,
            int32_t* index);*/
        // add by liyongfeng:20140105 [secondary index for replace]
        int gen_physical_replace_new(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            const uint64_t& query_id,
            int32_t* index);
        int gen_expr_array(ObUpdateStmt *update_stmt, ObIndexTriggerUpd *index_trigger_upd, const ObRowDesc &row_desc,
                           ObLogicalPlan *logical_plan,ObPhysicalPlan* inner_plan,ErrStat& err_stat);
        // add:e
        //add by maosy [MultiUps 1.0] [sequence and secondary index ] 20170615 b
        int gen_expr_array(ObIndexTriggerUpd *index_trigger_upd,ObProject *project,ErrStat& err_stat);
        // add by maosy e
        //add liumz, [optimize replace index]20161110:b
        bool is_need_static_data_for_index(uint64_t main_tid);
        //add:e
        int gen_physical_insert_new(
          ObLogicalPlan *logical_plan,
          ObPhysicalPlan *physical_plan,
          ErrStat& err_stat,
          const uint64_t& query_id,
          int32_t* index);
        int gen_physical_delete(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            const uint64_t& query_id,
            int32_t* index);
        int gen_physical_update(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            const uint64_t& query_id,
            int32_t* index);
        int gen_physical_explain(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            const uint64_t& query_id,
            int32_t* index);
        int gen_physical_create_table(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            const uint64_t& query_id,
            int32_t* index);
        //add liu jun.[MultiUPS] [sql_api] 20150320:b
        int gen_physical_part_func_stmt(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            const uint64_t& query_id,
            int32_t* index);
        //add 20150320:e
        //add by wenghaixing[secondary index] 20141024
        int gen_physical_create_index(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            const uint64_t& query_id,
            int32_t* index);
        //add e
        //add wenghaixing[secondary index upd.3]20141128
        int cons_whole_row_desc(uint64_t table_id,ObRowDesc& desc,ObRowDescExt& desc_ext);
        int cons_whole_row_desc(ObUpdateStmt* upd_stmt, uint64_t table_id, ObRowDesc& desc, ObRowDescExt& desc_ext);
        int cons_del_upd_row_desc(ObStmt* stmt, uint64_t table_id, ObRowDesc& desc, ObRowDescExt& desc_ext);
        int column_in_update_stmt(ObUpdateStmt* upd_stmt, uint64_t table_id, uint64_t cid, bool &in_stmt);
        //add e
        int gen_physical_drop_table(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            const uint64_t& query_id,
            int32_t* index);

        int gen_physical_truncate_table(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            const uint64_t& query_id,
            int32_t* index); //add zhaoqiong [Truncate Table]:20160318

        int gen_physical_alter_group(
                ObLogicalPlan *logical_plan,
                ObPhysicalPlan *physical_plan,
                ErrStat &err_stat,
                const uint64_t &query_id,
                int32_t *index);

        //add wenghaixing[secondary index drop index]20141223
        int gen_physical_drop_index(
                ObLogicalPlan *logical_plan,
                ObPhysicalPlan *physical_plan,
                ErrStat& err_stat,
                const uint64_t& query_id,
                int32_t* index);
        //add e
        int gen_physical_alter_table(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            const uint64_t& query_id,
            int32_t* index);
        int gen_physical_show(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            const uint64_t& query_id,
            int32_t* index);
        int gen_phy_tables(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            ObSelectStmt *select_stmt,
            bool& group_agg_pushed_down,
            bool& limit_pushed_down,
            oceanbase::common::ObList<ObPhyOperator*>& phy_table_list,
            oceanbase::common::ObList<ObBitSet<> >& bitset_list,
            oceanbase::common::ObList<ObSqlRawExpr*>& remainder_cnd_list,
            oceanbase::common::ObList<ObSqlRawExpr*>& none_columnlize_alias
       //add liuzy [sequence select] 20150525:b
            ,ObPhyOperator *sequence_op = NULL,
            //add 20150525:e
            bool optimizer_open = false
            );
        int gen_physical_kill_stmt(
          ObLogicalPlan *logical_plan,
          ObPhysicalPlan* physical_plan,
          ErrStat& err_stat,
          const uint64_t& query_id,
          int32_t* index);
        //add fanqiushi_index
        int gen_phy_table_for_storing(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            ObStmt *stmt,
            uint64_t table_id,
            ObPhyOperator*& table_op,
            bool* group_agg_pushed_down = NULL,
            bool* limit_pushed_down = NULL,
            bool is_use_storing_column = false,
            uint64_t index_tid=OB_INVALID_ID,
            Expr_Array *filter_array=NULL,
              Expr_Array *project_array=NULL );
        int gen_phy_table_without_storing(ObLogicalPlan *logical_plan,
                ObPhysicalPlan *physical_plan,
                ErrStat& err_stat,
                ObStmt *stmt,
                uint64_t table_id,
                ObPhyOperator*& table_op,
                bool* group_agg_pushed_down = NULL,
                bool* limit_pushed_down = NULL,
                uint64_t index_tid_without_storing=OB_INVALID_ID,
                Expr_Array * filter_array = NULL,
                Expr_Array * project_array = NULL,
                Join_column_Array *join_column = NULL //add by wanglei [semi join second index] 20151231
                );

        bool is_can_use_hint_for_storing_V2(
                Expr_Array *filter_array,
                Expr_Array *project_array,
                uint64_t index_table_id,
                Join_column_Array *join_column,//add wanglei for semi join
                ObStmt *stmt
                );//add wanglei for semi join
        bool is_can_use_hint_index_V2(
                Expr_Array *filter_ayyay,
                uint64_t index_table_id,
                Join_column_Array *join_column,//add wanglei for semi join
                ObStmt *stmt//add wanglei for semi join
                );
        //add:e
        int gen_phy_table(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            ObStmt *stmt,
            uint64_t table_id,
            ObPhyOperator*& table_op,
            bool* group_agg_pushed_down = NULL,
            bool* limit_pushed_down = NULL
      //add liuzy [sequence select] 20150525:b
            ,ObPhyOperator *sequence_op = NULL
            //add 20150525:e
            ,bool outer_join_scope = false,//add liumz, [outer_join_on_where]20150927
            bool optimizer_open = false
      );
        //add fanqiushi_index
        bool handle_index_for_one_table(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            ObStmt *stmt,
            uint64_t table_id,
            ObPhyOperator*& table_op,
            ObPhyOperator *sequence_op,//add liumz, [index_sequence_filter]20170704
            bool* group_agg_pushed_down = NULL,
            bool* limit_pushed_down = NULL,
            bool outer_join_scope = false,//add liumz, [outer_join_on_where]20150927
            bool optimizer_open = false
            );
        //add:e
    int add_bloomfilter_join_expr(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ObBloomFilterJoin& join_op,
            ObSort& l_sort,
            ObSort& r_sort,
            ObSqlRawExpr& expr,
            const bool is_table_expr_same_order);

        bool check_hash_join_with_same_column_type(
                ObLogicalPlan *logical_plan,
                ObSelectStmt *select_stmt,
                ObBinaryRefRawExpr * ref1,
                ObBinaryRefRawExpr * ref2
                );
        int add_hash_join_single_expr(
                ObLogicalPlan *logical_plan,
                ObPhysicalPlan *physical_plan,
                ObHashJoinSingle& join_op,
                ObSqlRawExpr& expr,
                const bool is_table_expr_same_order);
        int add_merge_join_expr(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ObMergeJoin& join_op,
            ObSort& l_sort,
            ObSort& r_sort,
            ObSqlRawExpr& expr,
            const bool is_table_expr_same_order);
    /*add by wanglei [semi join] 20151106*/
        int add_semi_join_expr(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ObSemiJoin &join_op,
            ObSort& l_sort,
            ObSort& r_sort,
            ObSqlRawExpr& expr,
            const bool is_table_expr_same_order,
            oceanbase::common::ObList<ObSqlRawExpr *> &remainder_cnd_list,
            bool &is_add_other_join_cond,
            ObJoin::JoinType join_type, ObSelectStmt *select_stmt, int id,ObJoinTypeArray& hint_temp);
    //add e
        int gen_phy_joins(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            ObSelectStmt *select_stmt,
            ObJoin::JoinType join_type,
            oceanbase::common::ObList<ObPhyOperator*>& phy_table_list,
            oceanbase::common::ObList<ObBitSet<> >& bitset_list,
            oceanbase::common::ObList<ObSqlRawExpr*>& remainder_cnd_list,
            oceanbase::common::ObList<ObSqlRawExpr*>& none_columnlize_alias,
            bool implied_join = false,
            bool optimizer_open = false,
            bool effective_from_opt = false,
            JoinedTable::JoinOperator opt_join_operator = JoinedTable::MERGE_JOIN,
            bool effective_opt = false,
            bool outer_join_scope = false//add liumz, [outer_join_on_where]20150927
            );
        int gen_phy_group_by(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            ObSelectStmt *select_stmt,
            ObPhyOperator *in_op,
            ObPhyOperator *&out_op);
        //add dolphin [ROW_NUMBER-PARTITION_BY]@20150827:b
        int gen_phy_partition_by(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat &err_stat,
            ObSelectStmt *select_stmt,
            ObPhyOperator *in_op,
            ObPhyOperator *&out_op);
        //add:e
        int gen_phy_scalar_aggregate(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            ObSelectStmt *select_stmt,
            ObPhyOperator *in_op,
            ObPhyOperator *&out_op);
        //add liumz, [ROW_NUMBER]20150824
        int gen_phy_scalar_analytic(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            ObSelectStmt *select_stmt,
            ObPhyOperator *in_op,
            ObPhyOperator *&out_op);
        //add:e
        int gen_phy_distinct(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            ObSelectStmt *select_stmt,
            ObPhyOperator *in_op,
            ObPhyOperator *&out_op);
        int gen_phy_order_by(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            ObSelectStmt *select_stmt,
            ObPhyOperator *in_op,
            ObPhyOperator *&out_op,
            bool use_generated_id = false);
        int gen_phy_limit(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            ObSelectStmt *select_stmt,
            ObPhyOperator *in_op,
            ObPhyOperator *&out_op);
        int gen_phy_values(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            const ObInsertStmt *insert_stmt,
            const ObRowDesc& row_desc,
            const ObRowDescExt& row_desc_ext,
            const ObSEArray<int64_t, 64> *row_desc_map,
            ObExprValues& value_op
            );
      // add by liyongfeng [secondary index for replace]
        int gen_phy_static_data_scan_for_replace(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            const ObInsertStmt *insert_stmt,
            const ObRowDesc& row_desc,
            const common::ObSEArray<int64_t, 64> &row_desc_map,
            const uint64_t table_id,
            const ObRowkeyInfo &rowkey_info,
            ObTableRpcScan &table_scan);
        // add:e
        int gen_phy_static_data_scan(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            //mod lijianqiang [seqeunce] 20150515 :b
        //    const ObInsertStmt * insert_stmt,
            ObStmt *stmt,
            //mod 20150515:e
            const ObRowDesc& row_desc,
            const common::ObSEArray<int64_t, 64> &row_desc_map,
            const uint64_t table_id,
            const ObRowkeyInfo &rowkey_info,
            ObTableRpcScan &table_scan
            );
        int gen_phy_show_tables(
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            ObShowStmt *show_stmt,
            ObPhyOperator *&out_op);
// add by zhangcd [multi_database.show_tables] 20150617:b
        int gen_phy_show_system_tables(
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            ObShowStmt *show_stmt,
            ObPhyOperator *&out_op);
// add by zhangcd [multi_database.show_tables] 20150617:e
// add by zhangcd [multi_database.show_databases] 20150617:b
        int gen_phy_show_databases(
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            ObShowStmt *show_stmt,
            ObPhyOperator *&out_op);
// add by zhangcd [multi_database.show_databases] 20150617:e
// add by zhangcd [multi_database.show_databases] 20150617:b
        int gen_phy_show_current_database(
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            ObShowStmt *show_stmt,
            ObPhyOperator *&out_op);
// add by zhangcd [multi_database.show_databases] 20150617:e

        // add by zhangcd [multi_database.bugfix] 20150805:e
        int get_all_databases(ErrStat& err_stat, ObStrings &databases);
        // add:e

        //add liumengzhan_show_index
        int gen_phy_show_index(
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            ObShowStmt *show_stmt,
            ObPhyOperator *&out_op);
        //add:e
        int gen_phy_show_all_index(
                ObPhysicalPlan *physical_plan,
                ErrStat& err_stat,
                ObShowStmt *show_stmt,
                ObPhyOperator *&out_op);

        int gen_phy_show_columns(
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            ObShowStmt *show_stmt,
            ObPhyOperator *&out_op);
        int gen_phy_show_variables(
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            ObShowStmt *show_stmt,
            ObPhyOperator *&out_op);
        int gen_phy_show_warnings(
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            ObShowStmt *show_stmt,
            ObPhyOperator *&out_op);
        int gen_phy_show_grants(
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            ObShowStmt *show_stmt,
            ObPhyOperator *&out_op);
        int gen_phy_show_table_status(
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            ObShowStmt *show_stmt,
            ObPhyOperator *&out_op);
        int gen_phy_show_processlist(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            ObShowStmt *show_stmt,
            ObPhyOperator *&out_op);
        template <class T>
        int get_stmt(
            ObLogicalPlan *logical_plan,
            ErrStat& err_stat,
            const uint64_t& query_id,
            T *& stmt);
        template <class T>
        int add_phy_query(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            const uint64_t& query_id,
            T * stmt,
            ObPhyOperator *phy_op,
            int32_t* index);
        int gen_physical_priv_stmt(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            const uint64_t& query_id,
            int32_t* index);
        int gen_physical_prepare(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            const uint64_t& query_id,
            int32_t* index);
        int gen_physical_variable_set(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            const uint64_t& query_id,
            int32_t* index);
        int gen_physical_execute(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            const uint64_t& query_id,
            int32_t* index);
        int gen_physical_deallocate(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            const uint64_t& query_id,
            int32_t* index);
        int gen_phy_table_for_update(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan*& physical_plan,
            ErrStat& err_stat,
            ObStmt *stmt,
            uint64_t table_id,
            const ObRowkeyInfo &rowkey_info,
            const ObRowDesc &row_desc,
            const ObRowDescExt &row_desc_ext,
            ObPhyOperator*& table_op, ObPhyOperator *sequence_op = NULL//add liuzy [sequence select for update] 20150918
            );
        //add by wenghaixing [secondary index upd.3] 20141128
        int gen_phy_table_for_update_v2(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan*& physical_plan,
            ErrStat& err_stat,
            ObStmt *stmt,
            uint64_t table_id,
            const ObRowkeyInfo &rowkey_info,
            const ObRowDesc &row_desc,
            const ObRowDescExt &row_desc_ext,
            ObPhyOperator*& table_op
            //add lijianqiang [sequence update] 20160316:b
            ,ObPhyOperator* sequence_op  = NULL );
            //add 20160316:e

        //add e
        //add wenghaixing [secondary index replace bug_fix]20150517
        int cons_row_desc_with_index(const uint64_t table_id,
            const ObStmt *stmt,
            ObRowDescExt &row_desc_ext,
            ObRowDesc &row_desc,
            const ObRowkeyInfo *&rowkey_info,
            ErrStat& err_stat);
        int is_column_in_stmt(const uint64_t table_id, const uint64_t column_id, const ObStmt *stmt,bool &in_stmt);
        int gen_phy_values_idx(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            const ObInsertStmt *insert_stmt,
            ObRowDesc& row_desc,
            ObRowDescExt& row_desc_ext,
            const ObRowDesc& row_desc_idx,
            const ObRowDescExt& row_desc_ext_idx,
            const ObSEArray<int64_t, 64> *row_desc_map,
            ObExprValues& value_op);
        int row_desc_intersect(ObRowDesc &row_desc,
                               ObRowDescExt &row_desc_ext,
                               ObRowDesc row_desc_idx,
                               ObRowDescExt row_desc_ext_idx);
        //add e
        int gen_physical_update_new(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan*& physical_plan,
            ErrStat& err_stat,
            const uint64_t& query_id,
            int32_t* index);
        /**
         * @brief cons_row_desc 构造行描述
         * @param table_id [in]表id
         * @param stmt [in]对应的statement
         * @param row_desc_ext [out]
         * @param row_desc [out]
         * @param rowkey_info [out]
         * @param row_desc_map [out]
         * @param err_stat [out]
         * @param is_update [in]
         * @return success or error code
         */
        int cons_row_desc(const uint64_t table_id,
            const ObStmt *stmt,
            ObRowDescExt &row_desc_ext,
            ObRowDesc &row_desc,
            const ObRowkeyInfo *&rowkey_info,
            common::ObSEArray<int64_t, 64> &row_desc_map,
            ErrStat& err_stat);
    //add liumengzhan_delete_index
    int cons_index_row_desc(uint64_t table_id,
            ObRowDesc &desc,
            ObRowDescExt &desc_ext);
    //add:e
        int gen_physical_delete_new(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan* physical_plan,
            ErrStat& err_stat,
            const uint64_t& query_id,
            int32_t* index
            , ObUpdateRowkey *op_update_rowkey = NULL
                );
        int gen_physical_start_trans(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan* physical_plan,
            ErrStat& err_stat,
            const uint64_t& query_id,
            int32_t* index);
        int gen_physical_end_trans(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan* physical_plan,
            ErrStat& err_stat,
            const uint64_t& query_id,
            int32_t* index);
        int wrap_ups_executor(ObPhysicalPlan *physical_plan,
            const uint64_t query_id,
            ObPhysicalPlan*& new_plan,
            int32_t* index,
            ErrStat& err_stat, bool need_loop = false);
        int gen_phy_select_for_update(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            const uint64_t& query_id,
            int32_t* index);
        int try_push_down_group_agg(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            const ObSelectStmt *select_stmt,
            bool& group_agg_pushed_down,
            ObPhyOperator *& scan_op);
        //add liumz, [optimize group_order by index]20170419:b
        int try_push_down_group_agg_for_storing(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            const ObSelectStmt *select_stmt,
            bool& group_agg_pushed_down,
            ObPhyOperator *& scan_op,
            const uint64_t index_tid,
            bool is_ailias_table);
        //add:e
        int try_push_down_limit(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            const ObSelectStmt *select_stmt,
            bool& limit_pushed_down,
            ObPhyOperator *scan_op);
        int gen_phy_show_parameters(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            ObShowStmt *show_stmt,
            ObPhyOperator *&out_op);
        //add liu jun.[MultiUPS] [sql_api] 20150325:b
        //mod wuna [MultiUPS] [sql_api] 20160223:b
        //int gen_phy_show_functions(
          //  ObLogicalPlan *logical_plan,
          //  ObPhysicalPlan *physical_plan,
          //  ErrStat& err_stat,
          //  ObShowStmt *show_stmt,
          //  ObPhyOperator *&out_op)
        int gen_phy_show_partition_functions_rules_groups(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            ObShowStmt *show_stmt,
            ObPhyOperator *&out_op,
            const uint64_t table_id,
            const char* const table_name);
        //mod 20160223:e
        int gen_phy_show_current_paxos_id(
                ObPhysicalPlan *physical_plan,
                ErrStat &err_stat,
                ObShowStmt *show_stmt,
                ObPhyOperator *&out_op);
        int get_table_rules(
                const ObTableSchema& table_schema,
                ObString& prefix_name,
                ObString &rule_name,
                ObString &par_list,
                ErrStat &err_stat);
        int get_partition_rules(
                ObString &rule_name,
                int64_t &rule_par_num,
                ObString &rule_par_list,
                ObString& rule_body,
                ErrStat &err_stat);
        int add_table_partition(
                const common::ObTableSchema& table_schema,
                char* buf,
                const int64_t& buf_len,
                int64_t& pos,
                ErrStat &err_stat);
        //add 20150325:e
        int gen_phy_show_create_table(
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            ObShowStmt *show_stmt,
            ObPhyOperator *&out_op);
        int gen_phy_show_create_view(
            ObPhysicalPlan *physical_plan,
            ErrStat &err_stat,
            ObShowStmt *show_stmt,
            ObPhyOperator *&out_op);

        int get_all_index(uint64_t table_id, uint64_t idx_id[], int32_t &index_num);
        int gen_phy_show_create_index(
                ObPhysicalPlan *physical_plan,
                ErrStat &err_stat,
                ObShowStmt *show_stmt,
                ObPhyOperator *&out_op);
        int cons_index_definition(
                const common::ObTableSchema &table_schema,
                char *buf,
                const int64_t &buf_len,
                int64_t &pos,
                ErrStat &err_stat);
        int cons_table_definition(
            const common::ObTableSchema& table_schema,
            char* buf,
            const int64_t& buf_len,
            int64_t& pos,
            ErrStat& err_stat);
        int gen_physical_alter_system(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            const uint64_t& query_id,
            int32_t* index);
        int gen_phy_when(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat& err_stat,
            const uint64_t& query_id,
            ObPhyOperator& child_op,
            ObWhenFilter *& when_filter);
        int merge_tables_version(ObPhysicalPlan & outer_plan, ObPhysicalPlan & inner_plan);

        bool parse_join_info(const ObString &join_info, TableSchema &table_schema);
        bool check_join_column(const int32_t column_index,
              const char* column_name, const char* join_column_name,
              TableSchema& schema, const ObTableSchema& join_table_schema);

        int allocate_column_id(TableSchema & table_schema);
        int gen_physical_change_obi_stmt(
          ObLogicalPlan *logical_plan,
          ObPhysicalPlan* physical_plan,
          ErrStat& err_stat,
          const uint64_t& query_id,
          int32_t* index);
        //add wenghaixing[decimal] for fix delete bug 2014/10/10
        int ob_write_obj_for_delete(ModuleArena &allocator, const ObObj &src, ObObj &dst,ObObj type);
        //add e
        //add dolphin [database manager]@20150617
        int check_dbname_for_table(ErrStat& err_stat,const ObString& dbname);
    //add lijianqiang [sequence] 20150717:b
        int wrap_sequence(ObLogicalPlan *&logical_plan,
                          ObPhysicalPlan *&physical_plan,
                          ErrStat &err_stat,
                          const ObSEArray<int64_t, 64> &row_desc_map,
                          ObSequence *sequence_op,
                          ObStmt *stmt,
                          ObPhysicalPlan *inner_plan = NULL);
        //add 20150717:e
    //add gaojt [Delete_Update_Function] [JHOBv0.1] 20150817:b
        int gen_phy_table_for_update_new(ObLogicalPlan *logical_plan,
                ObPhysicalPlan*& inner_plan,
                ObPhysicalPlan *&physical_plan,
                ErrStat& err_stat,
                ObStmt *stmt,
                uint64_t table_id,
                const ObRowkeyInfo &rowkey_info,
                const ObRowDesc &row_desc,
                const ObRowDescExt &row_desc_ext,
                ObPhyOperator*& table_op, bool is_delete_update
                //add lijianqiang [sequence] 20150909:b
                , ObPhyOperator* sequence_op = NULL
                //add 20150909:e
                , bool is_column_hint_index = false
                 );
        int is_multi_delete_update(ObLogicalPlan *logical_plan,
                            ObPhysicalPlan*& physical_plan,
                            const ObRowkeyInfo &rowkey_info, ObStmt *stmt, ErrStat &err_stat,
                            bool& is_multi_delete_update);
        int get_fillvalues_operator(ObPhyOperator *main_query, ObPhyOperator *&fill_values);
        int constuct_top_operator_of_select(bool is_non_where_condition,
                                            ParseNode* ud_where_parse_tree,
                                            ObPhysicalPlan*& physical_plan,
                                            std::string column_names,
                                            std::string table_name, int32_t &select_opeartor_id,
                                            ErrStat& err_stat);
                                            // add by maosy [MultiUps 1.0] [batch_udi] 20170420 b
        int get_table_rpc_scan(ObPhyOperator * phy_operator , ObTableRpcScan *&table_rpc_scan,uint64_t table_id);
        // add by maosy 20170420 e
        int get_parse_result(const ObString &select_stmt, ParseResult &select_parse_result);
        int gene_parse_result(const common::ObString& query_string, ParseResult& syntax_tree);
        int get_column_name_table_name(std::string& column_names,
                                           std::string& table_name,
                                           TableItem* table_item, const ObRowDesc &row_desc
                                       // add e
                                       );
        //add gaojt 20150817:e
        //add lijianqiang [set_row_key_ignore] 20151019:b
        bool can_ignore_current_key(ObLogicalPlan *logical_plan,
                                    ObPhysicalPlan*& physical_plan,
                                    int64_t column_idx,
                                    uint64_t table_id,
                                    uint64_t column_id,
                                    ObUpdateStmt *update_stmt);
        //add 20151019:e
        //add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160408:b
        int wrap_new_physical_plan(ObPhysicalPlan *& new_plan, ErrStat& err_stat);

        int cons_full_row_desc(
            const uint64_t table_id,
            const ObRowDesc& raw_row_desc,
            const ObRowDescExt& raw_row_desc_ext,
            const ObSEArray<int64_t, 64> &raw_row_desc_map,
            ObRowDesc& cur_row_desc,
            ObRowDescExt& cur_row_desc_ext,
            ObSEArray<int64_t, 64> &cur_row_desc_map,
            ErrStat& err_stat);

        int gen_phy_table_for_pre_execution_plan(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan*& physical_plan,
            ErrStat& err_stat,
            ObStmt *stmt,
            uint64_t table_id,
            const ObRowkeyInfo &rowkey_info,
            const ObRowDesc &row_desc,
            const ObRowDescExt &row_desc_ext,
            ObPhyOperator*& table_op);

        int add_condition_to_filter(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ObStmt *stmt,
            const ObRowkeyInfo &rowkey_info,
             ObFilter * filter_op,
            ModuleArena& rowkey_alloc,
            ObObj * rowkey_objs,
            ObPostfixExpression::ObPostExprNodeType * type_objs,
            bool& has_other_cond,
            ErrStat& err_stat);

        int add_out_put_column_for_inc_scan(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan*& physical_plan,
            ErrStat& err_stat,
            const ObRowDesc& row_desc,
            const ObRowkeyInfo &rowkey_info,
            ObObj *rowkey_objs,
            ObPostfixExpression::ObPostExprNodeType * type_objs,
            ObExprValues * get_param_values);
//add by maosy [MultiUps 1.0] [hot update and secondary index] 20170415 b
        int gen_physical_index_trigger_for_update(
                ObPhysicalPlan*& physical_plan,
                ErrStat& err_stat,
                ObIndexTriggerUpd *&index_trigger_upd,
                ObProject *&project_op ,
                uint64_t table_id,
                ObRowDesc& row_desc,
                ObRowDescExt& row_desc_ext,
                const ObRowkeyInfo *rowkey_info,
                ObUpdateStmt * update_stmt,
                IndexList &out
                );
        int gen_physical_index_trigger_for_replace(
                ObLogicalPlan *logical_plan,
                ObPhysicalPlan*& physical_plan,
                ErrStat& err_stat,
                ObIndexTriggerRep *&index_trigger_rep,
                ObRowDesc& row_desc,
                ObRowDescExt& row_desc_ext,
                ObInsertStmt *insert_stmt,
                const ObSEArray<int64_t, 64>& row_desc_map,
                 ObRowDesc row_desc_for_static_data,
                 ObRowDescExt row_desc_ext_for_static_data,
                ObRowDesc& main_desc,
                const ObRowkeyInfo *rowkey_info
                );
//add by maosy 20170415 e


        //update pre plan
        int gen_physical_update_new_pre_execution_plan(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ObUpdateStmt * update_stmt,
            uint64_t table_id,
            const ObRowDesc& raw_row_desc,
            const ObRowDescExt& raw_row_desc_ext,
            const ObSEArray<int64_t, 64>& raw_row_desc_map,
            const ObRowkeyInfo *rowkey_info,
            ErrStat& err_stat,
            const uint64_t& query_id
//add by maosy [MultiUps 1.0] [hot update and secondary index] 20170415 b
            ,IndexList &out);
            // add by maosy e

        //update full row plan
        int gen_physical_update_new_full_row_execution_plan(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ObUpdateStmt * update_stmt,
            uint64_t table_id,
            const ObRowDesc& raw_row_desc,
            const ObRowDescExt& raw_row_desc_ext,
            const ObSEArray<int64_t, 64>& raw_row_desc_map,
            const ObRowkeyInfo *rowkey_info,
            ErrStat& err_stat,
            const uint64_t& query_id
//add by maosy [MultiUps 1.0] [hot update and secondary index] 20170415 b
                ,IndexList &out);
                // add by maosy e

        //replace pre plan
        int gen_physical_replace_pre_execution_plan(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ObInsertStmt *insert_stmt,
            uint64_t table_id,
            const ObRowDesc& raw_row_desc,
            const ObRowDescExt& raw_row_desc_ext,
            const ObSEArray<int64_t, 64>& row_desc_map,
            ErrStat&err_stat,
            const uint64_t& query_id
//add by maosy [MultiUps 1.0] [hot update and secondary index] 20170415 b todo
                ,bool is_column_hint_index);
                // add by maosy e

        //replace full row plan
        int gen_physical_replace_full_row_execution_plan(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ObInsertStmt *insert_stmt,
            uint64_t table_id,
            const ObRowDesc& row_desc,
            const ObRowDescExt& row_desc_ext,
            const ObSEArray<int64_t, 64>& row_desc_map,
            const ObRowkeyInfo *rowkey_info,
            ErrStat&err_stat,
            const uint64_t& query_id
//add by maosy [MultiUps 1.0] [hot update and secondary index] 20170415 b todo
                ,bool is_column_hint_index);
                // add by maosy e

        int add_set_clause_for_update_to_project(ObLogicalPlan *& logical_plan,
            ObPhysicalPlan *& inner_plan,
            ObProject *& project_op,
            const ObRowDesc& row_desc,
            const ObRowkeyInfo *rowkey_info,
            ObUpdateStmt * update_stmt,
            uint64_t table_id,
            ErrStat& err_stat
            );

        int fill_unchanged_column_info_for_update(
            ObPhysicalPlan *physical_plan,
            ObProject * project_op,
            const ObRowDesc& cur_row_desc,
            const ObRowDescExt& cur_row_desc_ext,
            const int64_t& raw_row_column_num,
            ErrStat& err_stat);

        int add_unchanged_column_to_expr_values(
            ObExprValues& expr_values_op,
            const ObRowDesc& new_row_desc,
            const int64_t& raw_column_num,
            ErrStat& err_stat);

        int add_out_put_column_for_table_rpc_scan(
            ObTableRpcScan& table_rpc_scan,
            const ObRowDesc& cur_row_desc,
            const int64_t& raw_column_num,
            ObDmlType dml_type,
            ErrStat& err_stat);
        //add 20160408:e
 //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
        int get_table_max_used_cid(ObSqlContext *context,
                                   const uint64_t table_id,
                                   uint64_t &max_used_cid);
        //add duyr 20160531:e

        //add dragon [Bugfix 1224] 2016-8-29 15:55:38
        /**
         * @brief get_filter_array: 获取查询中where条件中表达式
         * @param table_id [in] 与哪个table相关
         * @param filter_array [out] 输出的结果
         * @param fp_array [out] 用于释放函数中申请的filter
         * @return 错误码
         */
        int get_filter_array(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan* physical_plan,
            uint64_t table_id,
            ObSelectStmt *select_stmt,
            Expr_Array &filter_array,
            common::ObArray<ObSqlExpression *> &fp_array);
        /**
         * @brief get_project_array: 获取查询中select后面的输出列信息
         * @param logical_plan
         * @param physical_plan
         * @param table_id [in] 与哪个table相关
         * @param select_stmt
         * @param project_array [out] 输出的结果
         * @param alias_exprs [out] 要把相应的alias改成原样
         * @return 错误码
         */
        int get_project_array(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan* physical_plan,
            uint64_t table_id,
            ObSelectStmt *select_stmt,
            Expr_Array &project_array,
            ObArray<uint64_t> &alias_exprs);
        //add 2016-8-29 15:55:44e
        int semi_join_add_filter(const uint64_t right_main_cid, const uint64_t &right_table_id,
                                 const uint64_t index_table_id, ObSort &sort,
                                 ObSqlExpression *&expr_temp, const bool is_left, bool &add_suc);
        int get_project_and_filter_array(ObLogicalPlan *logical_plan, ObPhysicalPlan *physical_plan,
                                         const uint64_t table_id, ObSelectStmt *select_stmt,
                                         Expr_Array &project_array, Expr_Array &filter_array);
        int set_update_rowkey(
                ObPhysicalPlan *physical_plan,
                const uint64_t query_id,
                ObUpdateRowkey *&op_update_rowkey,
                int32_t *index,
                ErrStat &err_stat);
        int gen_phy_update_rowkey_for_select_rows(ObLogicalPlan *logical_plan,
                                                  ObPhysicalPlan *physical_plan,
                                                  ObUpdateRowkey *&op_update_rowkey,
                                                  ErrStat &err_stat,
                                                  const uint64_t &query_id,
                                                  int32_t *index);

        int wrap_update_rowkey_ups_executor(
                ObPhysicalPlan *physical_plan,
                const uint64_t query_id,
                ObPhysicalPlan *&new_plan,
                ObUpdateRowkey *&op_update_rowkey,
                UpsExecutorType &ups_executor_type,
                ErrStat &err_stat
                );
        int cons_row_desc_for_select_all(const uint64_t table_id,
                const ObStmt *stmt,
                ObRowDescExt &row_desc_ext,
                ObRowDesc &row_desc,
                const ObRowkeyInfo *&rowkey_info,
                ErrStat &err_stat);
        int gen_phy_table_for_update_for_select(
                ObLogicalPlan *logical_plan,
                ObPhysicalPlan *&physical_plan,
                ErrStat &err_stat,
                ObStmt *stmt,
                uint64_t table_id,
                const ObRowkeyInfo &rowkey_info,
                const ObRowDesc &row_desc,
                const ObRowDescExt &row_desc_ext,
                ObPhyOperator *&table_op, ObPhyOperator *sequence_op=NULL
                );
        int gen_phy_update_rowkey_insert_new_rows(ObLogicalPlan *logical_plan,
                                                  ObPhysicalPlan *physical_plan,
                                                  ObUpdateRowkey *&op_update_rowkey,
                                                  ErrStat &err_stat,
                                                  const uint64_t &query_id,
                                                  int32_t *index);
        int is_hit_update_rowkey(const uint64_t table_id, const ObRowkeyInfo *&rowkey_info, ObUpdateStmt *update_stmt,bool &is_update_rowkey);

        int gen_phy_update_rowkey_for_select_more_rows(ObLogicalPlan *logical_plan,
                                                       ObPhysicalPlan *physical_plan,
                                                       ObUpdateRowkey *&op_update_rowkey,
                                                       ErrStat &err_stat,
                                                       const uint64_t &query_id,
                                                       int32_t *index);
        int gen_physical_gather_statistics(
                ObLogicalPlan *logical_plan,
                ObPhysicalPlan *physical_plan,
                ErrStat &err_stat,
                const uint64_t& query_id,
                int32_t *index);

        int check_partition_value_type(
                ObLogicalPlan *logical_plan,
                ObSelectStmt *select_stmt,
                ObString table_name,
                ObTableRuleNode table_node,
                ObArray<ObObj> &partition_value,
                ObString &partition_key);
        int gen_phy_select_partition_calc_func(
                ObLogicalPlan *logical_plan,
                ObPhysicalPlan *physical_plan,
                ErrStat &err_stat,
                const uint64_t &query_id,
                int32_t *index);

        int gen_physical_create_view(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat &err_stat,
            const uint64_t &query_id,
            int32_t *index);

        int gen_physical_drop_view(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat &err_stat,
            const uint64_t &query_id,
            int32_t *index);

      private:
    //add peiouya [NotNULL_check] [JHOBv0.1] 20131222:b
    enum operateType
    {
      OP_INSERT,
      OP_REPLACE,
      };
        int column_null_check(
      ObLogicalPlan *logical_plan,
      ObInsertStmt *insert_stmt,
      ErrStat& err_stat,
      enum operateType op_type);
    //add 20131222:e
      private:
        common::ObIAllocator *mem_pool_;
        ObSqlContext *sql_context_;
        bool group_agg_push_down_param_;
    //add peiouya  [NotNULL_check] [JHOBv0.1] 20131222:b
    ObResultSet *result_;
    //add 20131222:e
    bool is_multi_batch_;//add gaojt [Delete_Update_Function] [JHOBv0.1] 20160302
    //int64_t questionmark_num_in_update_assign_list_;//add gaojt [Delete_Update_Function] [JHOBv0.1] 20160519
    ObStatExtractor stat_extractor_;
    bool is_first_on_expr_with_index_;
    };

    inline ObSqlContext* ObTransformer::get_sql_context()
    {
      return sql_context_;
    }
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_TRANSFORMER_H */
