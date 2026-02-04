/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_executor.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_ups_executor.h"
#include "common/utility.h"
#include "common/ob_trace_log.h"
#include "common/ob_common_stat.h"
#include "ob_bind_values.h"//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20150424
#include "ob_fill_values.h"//add gaojt [Delete_Update_Function] [JHOBv0.1] 20151011
//add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160321:b
#include "ob_raw_expr.h"
//add 20160321:e

#include "ob_multiple_merge.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObUpsExecutor::ObUpsExecutor()
  : rpc_(NULL), inner_plan_(NULL)
  //add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160408:b
  ,sql_context_(NULL),pre_execution_plan_(NULL)
  ,full_row_execution_plan_(NULL)
  ,executing_full_row_plan_(false)
  ,sub_trans_num_(0)
  ,inner_plan_type_(NORMAL_INNER_PLAN)
  ,total_affected_num_(0)
  ,total_warning_count_(0)
  //add 20160408:e

  ,insert_select_batch_num_(0)//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20150423
  ,sub_query_num_(0),is_row_num_null_(false)//add gaojt [Delete_Update_Function] [JHOBv0.1] 20151008
  ,is_multi_batch_(false)
  ,using_hot_update_(false) //add hongchen [HOT_UPDATE_SKEW] 20170724
  ,enable_dis_trans_(true)
{
}

ObUpsExecutor::~ObUpsExecutor()
{
  reset();
}

void ObUpsExecutor::reset()
{
  rpc_ = NULL;
  if(NULL != inner_plan_)
  {
    if(inner_plan_->is_cons_from_assign())
    {
      inner_plan_->clear();
      ObPhysicalPlan::free(inner_plan_);
    }
    else
    {
      inner_plan_->~ObPhysicalPlan();
    }
    inner_plan_ = NULL;
  }
  local_result_.clear();
  //curr_row_.reset(false, ObRow::DEFAULT_NULL);
  row_desc_.reset();
  //add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20151123:b
  sql_context_ = NULL;
  sub_trans_num_ = 0;
  executing_full_row_plan_ = false;
  total_affected_num_ = 0;
  total_warning_count_ = 0;
  inner_plan_type_ = NORMAL_INNER_PLAN;
  sub_trans_return_value_info_.clear();
  if (NULL != pre_execution_plan_ || NULL != full_row_execution_plan_)
  {
    if (NULL != pre_execution_plan_)
    {
      pre_execution_plan_->~ObPhysicalPlan();
      pre_execution_plan_ = NULL;
    }
    if (NULL != full_row_execution_plan_)
    {
      full_row_execution_plan_->~ObPhysicalPlan();
      full_row_execution_plan_ = NULL;
    }
  }
  //add 20151123:e
  using_hot_update_ = false;  //add hongchen [HOT_UPDATE_SKEW] 20170724
  enable_dis_trans_ = true;
}

void ObUpsExecutor::reuse()
{
  reset();
}

int ObUpsExecutor::open()
{
  int ret = OB_SUCCESS;
  // quiz: why there are two diffrent result sets?
  ObResultSet *outer_result_set = NULL;
  ObResultSet *my_result_set = NULL;
  ObSQLSessionInfo *session = NULL;
  //add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160413:b
  ObPhysicalPlan * final_inner_plan = NULL;
  //add 20160413:e
  if(rpc_ == NULL
      || inner_plan_ == NULL)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(WARN, "ObUpsExecutor is not initiated well, ret=%d", ret);
  }
  else
  {
    OB_ASSERT(my_phy_plan_);
    my_result_set = my_phy_plan_->get_result_set();
    outer_result_set = my_result_set->get_session()->get_current_result_set();
    my_result_set->set_session(outer_result_set->get_session());    // be careful!
    session = my_phy_plan_->get_result_set()->get_session();
    inner_plan_->set_result_set(my_result_set);
    inner_plan_->set_curr_frozen_version(my_phy_plan_->get_curr_frozen_version());
    local_result_.clear();
    // When read_only is enabled, the server permits no updates except for system tables.
    if(session->is_read_only() && my_phy_plan_->is_user_table_operation())
    {
      YYSYS_LOG(USER_ERROR, "The server is read only and no update is permitted. Ask your DBA for help.");
      ret = OB_ERR_READ_ONLY;
    }
  }
  
  //add hongchen [HOT_UPDATE_SKEW] 20170724:b
  if (NULL != sql_context_ && NULL != sql_context_->merge_service_)
  {
    using_hot_update_ = sql_context_->merge_service_->get_config().using_hot_update;
    enable_dis_trans_ = sql_context_->merge_service_->get_config().enable_dist_trans;
  }
  //add hongchen [HOT_UPDATE_SKEW] 20170724:e


  //add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160321:b
  /**
   * REPLACE stmt and UPDATE stmt, we replace or update with full row info for
   * hot table and rule changed table,we generate the physical oprators or branches before
   * opening the plan and may assemble the plan dynamicly when opening the plan.
   */
  if (OB_SUCCESS == ret)
  {
    //may update or replace stmt
    if (NULL != pre_execution_plan_)
    {
      pre_execution_plan_->set_result_set(my_result_set);
      pre_execution_plan_->set_curr_frozen_version(my_phy_plan_->get_curr_frozen_version());
    }
    if (NULL != full_row_execution_plan_)
    {
      full_row_execution_plan_->set_result_set(my_result_set);
      full_row_execution_plan_->set_curr_frozen_version(my_phy_plan_->get_curr_frozen_version());
    }

    if (OB_SUCCESS != (ret = check_partition_manager()))
    {
      YYSYS_LOG(WARN,"check partition manager failed,ret=%d",ret);
    }
    else if (OB_SUCCESS != (ret = choose_execution_plan(final_inner_plan)))
    {
      YYSYS_LOG(WARN, "choose execution plan failed,ret=%d",ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = execute_physical_plan(final_inner_plan, outer_result_set, session)))
    {
      if(OB_ERR_VARCHAR_TOO_LONG == ret)
      {
          ObString err_mes = ob_get_err_msg();
          char err_str[1024] = {0};
          err_mes.to_string(err_str, 1024);
          char table_id_str[10] = {0};
          char col_id_str[10] = {0};
          char col_len_str[10] = {0};
          int tem_ret = 0;
          tem_ret = sscanf(err_str, "%[^.] %*[.] %[^.] %*[.] %[^.]", table_id_str, col_id_str, col_len_str);
          if (3!=tem_ret)
          {
              YYSYS_LOG(USER_ERROR, "parsing error");
              ret = OB_ERR_UNEXPECTED;
          }
          else
          {
              int table_id = atoi(table_id_str);
              int col_id = atoi(col_id_str);
              int col_len = atoi(col_len_str);
              const ObTableSchema *table_schema = NULL;
              const ObColumnSchemaV2 *column_schema = NULL;
              if( NULL == (table_schema = sql_context_->schema_manager_->get_table_schema(table_id)))
              {
                  YYSYS_LOG(USER_ERROR, "table_schema is null");
                  ret = OB_ERR_UNEXPECTED;
              }
              else if(NULL != (column_schema = sql_context_->schema_manager_->get_column_schema(table_id, col_id)))
              {
                  YYSYS_LOG(USER_ERROR, "Varchar is too long, tname[%s], cname[%s], clength[%d]", table_schema->get_table_name(),column_schema->get_name(),col_len);
              }
              else
              {
                  YYSYS_LOG(USER_ERROR, "col_schema is null");
                  ret = OB_ERR_UNEXPECTED;
              }
          }
      }
      else if(OB_DECIMAL_UNLEGAL_ERROR == ret)
      {
          ObString err_mes = ob_get_err_msg();
          char err_str[1024] = {0};
          err_mes.to_string(err_str, 1024);
          char table_id_str[10] = {0};
          char col_id_str[10] = {0};
          char schema_p[10] = {0};
          char schema_s[10] = {0};
          char cell_p[10] = {0};
          char cell_s[10] = {0};
          char row_num_s[10] = {0};
          int tem_ret = 0;
          const ObTableSchema *table_schema = NULL;
          const ObColumnSchemaV2 *column_schema = NULL;
          tem_ret = sscanf(err_str, "%[^.] %*[.] %[^.] %*[.] %[^.]%*[.]%[^.]%*[.]%[^.]%*[.]%[^.]%*[.]%[^.]", table_id_str,col_id_str, schema_p,schema_s, cell_p,cell_s,row_num_s);
          if (NULL == (table_schema = sql_context_->schema_manager_->get_table_schema(atoi(table_id_str))))
          {
              YYSYS_LOG(ERROR, "table_schema is null");
              ret = OB_ERR_UNEXPECTED;
          }
          else if(NULL == (column_schema = sql_context_->schema_manager_->get_column_schema(atoi(table_id_str),atoi(col_id_str))))
          {
              YYSYS_LOG(ERROR, "col_schema is null");
              ret = OB_ERR_UNEXPECTED;
          }
          if (7 == tem_ret)
          {
              YYSYS_LOG(USER_ERROR, "Overflow occurred during convert decimal(%d,%d) to decimal(%d,%d) for table[%s] column[%s] at row[%d]", atoi(cell_p), atoi(cell_s), atoi(schema_p), atoi(schema_s), table_schema->get_table_name(), column_schema->get_name(), atoi(row_num_s));
          }
          else if(6 == tem_ret)
          {
              YYSYS_LOG(USER_ERROR, "Overflow occurred during convert decimal(%d,%d) to decimal(%d,%d) for table[%s] column[%s]", atoi(cell_p), atoi(cell_s), atoi(schema_p), atoi(schema_s), table_schema->get_table_name(), column_schema->get_name());
          }
          else
          {
              YYSYS_LOG(ERROR, "parsing error");
              ret = OB_ERR_UNEXPECTED;
          }
      }

      YYSYS_LOG(WARN, "execute physical plan failed,ret=%d",ret);
    }
    /**
     * when the pre_execution_plan_ execute failed, we need execute full_row_execution_plan_ if possible.
     */
    if (OB_SUCCESS != ret)
    {
    }
    else if (can_execute_full_row_execution_plan())
    {
      YYSYS_LOG(DEBUG,"execute the full row execution plan");
      executing_full_row_plan_ = true;//very important
      inner_plan_type_ = ObUpsExecutor::FULL_ROW_INNER_PLAN;
      final_inner_plan = full_row_execution_plan_;
      if (NULL != final_inner_plan &&
          OB_SUCCESS != (ret = execute_physical_plan(final_inner_plan,
                                                     outer_result_set,
                                                     session)))
      {
        YYSYS_LOG(WARN, "execute full row exexcution plan failed,ret=%d",ret);
      }
    }
    if (OB_SUCCESS == ret)
    {
      outer_result_set->set_affected_rows(total_affected_num_);
      outer_result_set->set_warning_count(total_warning_count_);
    }
  }
  //add 20160321:e
  return ret;
}


//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20150423:b
void ObUpsExecutor::get_output_infor(int64_t& batch_num,int64_t& inserted_row_num)
{
    batch_num = insert_select_batch_num_;
    inserted_row_num = inserted_row_num_;
}
//add gaojt 20150423:e


//add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20151123:b
int ObUpsExecutor::execute_physical_plan(ObPhysicalPlan *& final_inner_plan,
                                         ObResultSet *outer_result_set,
                                         ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;
  ObBindValues* bind_values = NULL;//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20150424
  ObFillValues* fill_values = NULL;//add gaojt [Delete_Update_Function] [JHOBv0.1] 20160325
  if (NULL == final_inner_plan ||
      NULL == outer_result_set ||
      NULL == session)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR, "not init,ret=%d",ret);
  }
  else
  {
    if(OB_LIKELY(OB_SUCCESS == ret))
    {
      // 1. fetch static data or covert the chidren info
      ObPhyOperator *main_query = final_inner_plan->get_main_query();
      //add hongchen [PREPARE_REPLACE_BUG] 20170623:b
      ObExprValues* expr_values_operator = dynamic_cast<ObExprValues *>(final_inner_plan->get_phy_op_by_id_from_operator_store(final_inner_plan->get_expr_values_op_id()));
      expr_values_operator->reset_evaled_mark();
      //add hongchen [PREPARE_REPLACE_BUG] 20170623:e
      for(int32_t i = 0; i < final_inner_plan->get_query_size(); ++i)
      {
        ObPhyOperator *aux_query = final_inner_plan->get_phy_query(i);
        if(aux_query != main_query)
        {
          YYSYS_LOG(DEBUG, "execute sub query %d", i);
          if(OB_SUCCESS != (ret = aux_query->open()))
          {
            YYSYS_LOG(WARN, "failed to execute sub-query, err=%d i=%d", ret, i);
            if (OB_ERR_SUB_QUERY_NULL_COLUMN == ret)
            {
                ret = null_check_error();
            }
            break;
          }
          if(NULL != (bind_values = dynamic_cast<ObBindValues*>(aux_query)))
          {
            is_multi_batch_ = true;
          }
          else if(NULL != (fill_values = dynamic_cast<ObFillValues*>(aux_query)))
          {
            is_multi_batch_ = true;
          }
        }
      } // end for
      if(NULL != bind_values && 0 == bind_values->get_inserted_row_num())
      {
          is_row_num_null_ = true;
      }
      if (NULL != fill_values)
          {
              if( fill_values->is_ud_non_row())
              {
                  is_row_num_null_ = true;
              }
          }
    }

    // 2. send to ups(s)
    /**
     * reconfiguration of the MultiUPS PHYSICAL PLAN UPSEXECUTOR.
     * if has no partiton,current inc data only be sended to one UPS,distribute the trans like sigle UPS,
     * if has partition,current inc data may be sended to different UPS,construct before sending and
     * distribute the trans in sub_trans.
     * major difference from previous:
     * 1.get the physical op by id avoid ambiguity if there are more ops with the same type
     * 2.wrap some detials
     */
    if (OB_LIKELY(OB_SUCCESS == ret) && !is_row_num_null_)
    {
      bool is_table_level = false;
      if (OB_SUCCESS != (ret = get_partition_type(final_inner_plan, is_table_level)))
      {
        YYSYS_LOG(WARN,"get partintion type failed,ret=%d",ret);
      }
      else if (OB_SUCCESS != (ret = clear_sub_trans_return_info()))
      {
        YYSYS_LOG(WARN, "clear sub trans return info failed,ret=%d",ret);
      }
      else if (is_table_level)
      {
        YYSYS_LOG(DEBUG, "TABLE_LEVEL");
        if (OB_SUCCESS != (ret = execute_trans_without_partition(final_inner_plan, outer_result_set, session)))
        {
          YYSYS_LOG(WARN,"execute trans without partition failed,ret=%d",ret);
        }
      }
	  // add by maosy [MultiUps 1.0] [batch_udi] 20170417 b
      else if(is_multi_batch_)
      {
          YYSYS_LOG(DEBUG, "MULTI BATCH");
          if(OB_SUCCESS !=(ret = execute_batch_trans_with_partition(final_inner_plan,outer_result_set,session)))
          {
              YYSYS_LOG(WARN,"failed to executor batch physical with partition ,ret = %d",ret );
          }
      }
	  // add by maosy 20170417 e
      else//has partition
      {
        YYSYS_LOG(DEBUG, "ROW_LEVEL");
        ObArray<ObSubRowStore> sub_input_values_row_stores;
        ObArray<ObSubRowStore> sub_static_values_row_stores;
        common::ModuleArena &mem_allocator = final_inner_plan->get_row_memery_allocator();
        mem_allocator.reuse();
        ObArray<int64_t> input_values_paxos_id_array;

        if (OB_SUCCESS != (ret = resolve_input_values(final_inner_plan, sub_input_values_row_stores,
                                                      input_values_paxos_id_array,
                                                      mem_allocator)))
        {
          YYSYS_LOG(WARN,"resolve the ObExprValues(inc data) failed,ret=%d",ret);
        }
        else if (OB_SUCCESS != (ret = resolve_static_values(final_inner_plan,
                                                            sub_static_values_row_stores,
                                                            input_values_paxos_id_array,
                                                            mem_allocator)))
        {
          YYSYS_LOG(WARN,"resolve the ObValues(static data) failed,ret=%d",ret);
        }
        if (OB_SUCCESS == ret)
        {
          if (OB_SUCCESS != (ret = execute_trans_with_partition(final_inner_plan,
                                                                sub_input_values_row_stores,
                                                                sub_static_values_row_stores,
                                                                outer_result_set,
                                                                session
                                                                )))
          {
            YYSYS_LOG(WARN,"bind sub values and execute sub trans failed,ret=%d",ret);
          }
        }
      }//end else
    }

    if(OB_SUCCESS == ret)
    {
        if(NULL != bind_values)
        {
            inserted_row_num_ = bind_values->get_inserted_row_num();
            insert_select_batch_num_++;
        }
    }
    // 3 close sub queries anyway
    if(OB_LIKELY(NULL != final_inner_plan))
    {
      ObPhyOperator *main_query = final_inner_plan->get_main_query();
      for(int32_t i = 0; i < final_inner_plan->get_query_size(); ++i)
      {
        ObPhyOperator *aux_query = final_inner_plan->get_phy_query(i);
        if(aux_query != main_query)
        {
          YYSYS_LOG(DEBUG, "close sub query %d", i);
          aux_query->close();
        }
      }
    }
  }
  return ret;
}

int ObUpsExecutor::choose_execution_plan(ObPhysicalPlan *& final_execution_plan)
{
  /**
   * if has pre_execution_plan_, the stmt may UPDATE or REPLACE, we do judgement first
   * choose which plan will be executed firstly:
   *   1.hot table or rule change table,for update or replace stmt,we execute "pre_execution_plan_"
   *     first,if succeed, return to client. if pre_execution_plan_" can't cons complete  row info
   *     in ups,we execute  "full_row_execution_plan_" which will do update with complete row info.
   *   2.normal stmt(insert,delete,update,replace) with raw inner plan for normal table.
   */
  int ret = OB_SUCCESS;
  bool is_hot_table = false;/// @todo handle the hot table in the future
  final_execution_plan = inner_plan_;
  inner_plan_type_ = ObUpsExecutor::NORMAL_INNER_PLAN;
  ObExprValues * expr_values_op = NULL;
  /*if (NULL != full_row_execution_plan_)
  {
    executing_full_row_plan_ = true;//very important
    final_execution_plan = full_row_execution_plan_;
    inner_plan_type_ = ObUpsExecutor::FULL_ROW_INNER_PLAN ;
    YYSYS_LOG(ERROR,"execute full plan!");
  }
  else*/ //if (NULL != pre_execution_plan_)  //del hongchen [HOT_UPDATE_SKEW] 20170910
  //mod hongchen [HOT_UPDATE_SKEW] 20170910:b
  if (!is_hot_table && !using_hot_update_)
  {
    //nothing todo
  }
  //if (NULL != pre_execution_plan_)
  else if (NULL != pre_execution_plan_)
  //mod hongchen [HOT_UPDATE_SKEW] 20170910:e
  {
    int32_t column_size = 0;
    const ObRowDesc *row_desc;
    const ObColumnSchemaV2* column = NULL;
    bool is_rule_changed = true;
    bool is_new_created_table = false;
    uint64_t table_id = pre_execution_plan_->get_table_id();
    ObBasicStmt::StmtType stmt_type = pre_execution_plan_->get_stmt_type();
    if (OB_INVALID_ID == table_id)
    {
      ret = OB_ERR_UNEXPECTED;
      YYSYS_LOG(ERROR, "table id is not init,ret=%d",ret);
    }
    else if (ObBasicStmt::T_NONE == stmt_type)
    {
      ret = OB_NOT_INIT;
      YYSYS_LOG(ERROR, "stmt type is not init,ret=%d",ret);
    }
    else if (IS_SYS_TABLE_ID(table_id))
    {
      YYSYS_LOG(DEBUG, "sys table,do nothing");
    }
    /// @todo new created table just use the raw inner_plan,do not need pre or full plan
    else if (is_new_created_table)
    {
      YYSYS_LOG(DEBUG, "new created table,do nothing");
    }
    else if (NULL == (column = sql_context_->schema_manager_->get_table_schema(table_id, column_size)))
    {
      ret = OB_SCHEMA_ERROR;
      YYSYS_LOG(ERROR, "get columns schema failed,table_id=%ld,ret=%d",table_id, ret);
    }
    //careful,we get the inner_plan to judge the rows is complete row or not
    /// @note U can delete the dynamic_cast<ObExprValues *>(...),and just use thesuperclass
    /// "ObPhyOperator" to point the ObExprValues. U can have a better understand with below form.
    else if (NULL == (expr_values_op = dynamic_cast<ObExprValues *>(inner_plan_->get_phy_op_by_id_from_operator_store(inner_plan_->get_expr_values_op_id()))))
    {
      ret = OB_ERR_UNEXPECTED;
      YYSYS_LOG(ERROR, "expr values op is null,ret=%d",ret);
    }
    else if (OB_SUCCESS != (ret = expr_values_op->get_row_desc(row_desc)))
    {
      YYSYS_LOG(WARN, "get row desc failed,ret=%d",ret);
    }
    //full update or full replace
    else if (row_desc->get_column_num() == column_size)
    {
      YYSYS_LOG(DEBUG, "replace  or update with full row,do nothing");
    }
    //convert the ObSqlExpression to ObRow
    else if (OB_SUCCESS != (ret = expr_values_op->open()))
    {
      YYSYS_LOG(WARN, "open expr values op failed,ret=%d",ret);
    }
    /**
     * when the rule changed,we execute the pre_execution_plan_ first,
     * if the pre_execution_plan can't cos complete row info in ups,we execute
     * the full_row_execution_plan secondly.
     */
    else if (OB_SUCCESS != (ret = get_is_rule_change_table(table_id,
                                                           *expr_values_op,
                                                           stmt_type,
                                                           is_rule_changed)))
    {
      YYSYS_LOG(WARN, "get is execute pre plan failed,ret=%d",ret);
    }
    //very important,need resuse the ObRowStore
    else if (OB_SUCCESS != (ret = expr_values_op->close()))
    {
      YYSYS_LOG(WARN, "close expr values op failed,ret=%d",ret);
    }

    if (OB_SUCCESS == ret)
    {
      //mod hongchen [HOT_UPDATE_SKEW] 20170910:b
      ////mod hongchen [HOT_UPDATE_SKEW] 20170724:b
      ////if (is_rule_changed || is_hot_table)//hot table will be handled in the future.
      //if(is_hot_table || using_hot_update_)
      //{
      //  final_execution_plan = pre_execution_plan_;
      //  inner_plan_type_ = ObUpsExecutor::PRE_INNER_PLAN;
      //  YYSYS_LOG(DEBUG, "execute the pre execution plan");
      //}
      ///*
      ////add by maosy [MultiUps 1.0][#167]20170503 b:
      //else if(full_row_execution_plan_ !=NULL)
      //{
      //    executing_full_row_plan_ = true;//very important
      //    final_execution_plan = full_row_execution_plan_;
      //    inner_plan_type_ = ObUpsExecutor::FULL_ROW_INNER_PLAN ;
      //    YYSYS_LOG(DEBUG, "execute the full execution plan");
      //}
      ////add by maosy 20170503 e
      //*/
      ////mod hongchen [HOT_UPDATE_SKEW] 20170724:e
      final_execution_plan = pre_execution_plan_;
      inner_plan_type_ = ObUpsExecutor::PRE_INNER_PLAN;
      YYSYS_LOG(DEBUG, "execute the pre execution plan");
      //mod hongchen [HOT_UPDATE_SKEW] 20170910:e
    }
  }
  return ret;
}

int ObUpsExecutor::get_is_rule_change_table(uint64_t table_id,
                                            ObExprValues& expr_valus_op,
                                            ObBasicStmt::StmtType stmt_type,
                                            bool& is_execute_pre_plan)
{
  int ret = OB_SUCCESS;
  const ObRow *row = NULL;
  const ObRowkey *rowkey = NULL;
  is_execute_pre_plan = false;
  ObCalcInfo calc_info;
  calc_info.set_table_id(table_id);
  calc_info.set_schema_manager(sql_context_->schema_manager_);
  calc_info.set_stmt_type(stmt_type);

  while (OB_SUCCESS == ret)
  {
    ret = expr_valus_op.get_next_row(row);
    if (OB_ITER_END == ret)
    {
      ret = OB_SUCCESS;
      break;
    }
    else if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "failed to get next row, ret=%d", ret);
      break;
    }
    else if (OB_SUCCESS != (ret = row->get_rowkey(rowkey)))
    {
      YYSYS_LOG(WARN, "failed to get rowkey, ret=%d", ret);
      break;
    }
    else
    {
      calc_info.set_row_key(rowkey);
      bool is_get_pre_paxos_id = true;
      int32_t pre_paxos_id = OB_INVALID_PAXOS_ID;
      int32_t cur_paxos_id = OB_INVALID_PAXOS_ID;
      /**
       * @todo if the table is create in current version, can execute pre_execution_plan_
       * directly,we need judge this by compare the schema version with the active version
       */
      if (OB_SUCCESS != (ret = sql_context_->partition_mgr_->get_paxos_id(table_id, calc_info, cur_paxos_id)))
      {
        YYSYS_LOG(WARN, "get cur paxos id failed, ret=%d",ret);
      }
      else if (OB_SUCCESS != (ret = sql_context_->partition_mgr_->get_paxos_id(table_id, calc_info, pre_paxos_id, &is_get_pre_paxos_id)))
      {
        YYSYS_LOG(WARN, "get pre paxos id failed, ret=%d",ret);
      }
      else if (cur_paxos_id != pre_paxos_id)//rules are changed
      {
        is_execute_pre_plan = true;
        break;
      }
      else
      {
        is_execute_pre_plan = false;
        YYSYS_LOG(DEBUG, "do not need execute pre plan");
      }
    }
  } // end while
  return ret;
}

int ObUpsExecutor::get_partition_type(ObPhysicalPlan *& final_inner_plan, bool &has_no_partition)
{
  int ret = OB_SUCCESS;
  if (NULL == final_inner_plan)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR, "final inner plan is not init,ret=%d",ret);
  }
  else
  {
    uint64_t table_id = final_inner_plan->get_table_id();
    if (OB_INVALID_ID == table_id)
    {
      ret = OB_ERROR;
      YYSYS_LOG(WARN, "table id is invalid,please check!,ret=%d",ret);
    }
    else if (OB_SUCCESS != (ret = sql_context_->partition_mgr_->get_table_part_type(table_id, has_no_partition)))
    {
      YYSYS_LOG(WARN, "get table type fail,ret=%d",ret);
    }
  }
  return ret;
}

int ObUpsExecutor::check_partition_manager()
{
  int ret = OB_SUCCESS;
  if (NULL ==  sql_context_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR, "sql context is not init,ret=%d",ret);
  }
  else if (NULL == sql_context_->partition_mgr_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR, "partition manager is not init,ret=%d",ret);
  }
  return ret;
}

int ObUpsExecutor::clear_sub_trans_return_info()
{
  int ret = OB_SUCCESS;
  /**
   * when the pre execution plan, if can exectute the full row execution plan,
   * the sub_trans_return_value_info_ can't be clear,we need the info to skip
   * sub trans that return OB_SUCCESS, only the sub trans that return OB_INCOMPLETE_ROW
   * will be executed in the full execution plan.
   */
  if (!executing_full_row_plan_)
  {
    sub_trans_return_value_info_.clear();
    sub_trans_num_ = 0;
    total_affected_num_ = 0;
    total_warning_count_ = 0;
  }
  return ret;
}

int ObUpsExecutor::execute_trans_without_partition(ObPhysicalPlan *& final_inner_plan, ObResultSet *outer_result_set, ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;
  YYSYS_LOG(DEBUG, "execute sub trans without partition");
  ObCalcInfo calc_info;
  int32_t paxos_id = 0;
  uint64_t table_id = OB_INVALID_ID;
  if (NULL == final_inner_plan)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR, "final inner plan is not init,ret=%d",ret);
  }
  else if (OB_INVALID_ID == (table_id= final_inner_plan->get_table_id()))
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR, "table id is not init,ret=%d",ret);
  }
  else if (ObBasicStmt::T_NONE ==  final_inner_plan->get_stmt_type())
  {
    ret = OB_ERROR;
    YYSYS_LOG(ERROR, "stmt type not init,ret=%d",ret);
  }
  else
  {
    calc_info.set_stmt_type(final_inner_plan->get_stmt_type());
    if (OB_SUCCESS != (ret = sql_context_->partition_mgr_->get_paxos_id(table_id, calc_info, paxos_id)))
    {
      YYSYS_LOG(WARN, "failed to get paxos id, err=%d", ret);
    }
    else
    {
      bool split_values = false;
      sub_trans_num_ = 1;
      TransReturnValueInfo trans_return_value_info;
      trans_return_value_info.paxos_id_ = paxos_id;
      if (skip_execute_cur_sub_trans(paxos_id))
      {
        YYSYS_LOG(INFO, "pre execution plan for cur sub trans return succeed,skip cur sub trans");
      }
      else if (OB_SUCCESS != (ret = execute_sub_trans(final_inner_plan, trans_return_value_info, paxos_id,
                                                      split_values, outer_result_set, session)))
      {
        if (OB_INCOMPLETE_ROW == ret)
        {
          ret = OB_SUCCESS;
        }
        else
        {
          YYSYS_LOG(WARN, "failed to execute sub trans, ret=%d, paxos_id=%d", ret, paxos_id);
        }
      }
    }
  }
  return ret;
}

int ObUpsExecutor::execute_trans_with_partition(ObPhysicalPlan *& final_inner_plan,
                                                const ObArray<ObSubRowStore>& sub_ob_expr_values_row_stores,
                                                const ObArray<ObSubRowStore>& sub_ob_values_row_stores,
                                                ObResultSet *outer_result_set,
                                                ObSQLSessionInfo *session
                                                )
{
  int ret = OB_SUCCESS;
  YYSYS_LOG(DEBUG,"execute sub trans with partition");
  bool split_values = sub_ob_expr_values_row_stores.count() > 1;
  ObValues *static_values = NULL;
  ObExprValues * input_values = NULL;
  ObExprValues *input_index_values = NULL;
  sub_trans_num_ = sub_ob_expr_values_row_stores.count();
  static_values = dynamic_cast<ObValues *>(final_inner_plan->get_phy_op_by_id_from_operator_store(final_inner_plan->get_values_op_id()));
  input_values = dynamic_cast<ObExprValues *>(final_inner_plan->get_phy_op_by_id_from_operator_store(final_inner_plan->get_expr_values_op_id()));
  if (final_inner_plan->get_index_expr_values_op_id()!=OB_INVALID_ID)
  {
      input_index_values = dynamic_cast<ObExprValues *>(final_inner_plan->get_phy_op_by_id_from_operator_store(final_inner_plan->get_index_expr_values_op_id()));
  }
  for (int64_t i = 0; OB_SUCCESS == ret && i < sub_ob_expr_values_row_stores.count(); i++)
  {
    const ObSubRowStore &sub_expr_values_row_store = sub_ob_expr_values_row_stores.at(i);    //取得一个sub_row_store
    int64_t paxos_id = sub_expr_values_row_store.paxos_id_;
    if (OB_SUCCESS != (ret = bind_expr_values(sub_expr_values_row_store, input_values)))         //将sub_row_store 绑定到plan中ObExprValues操作符中
    {
      YYSYS_LOG(WARN, "bind row store to plan fail,ret=%d", ret);
    }
    else if (NULL != static_values)//has static data
    {
      int64_t sub_values_index = -1;
      //judge if sub_ob_values_row_stores has the same row
      if (OB_SUCCESS !=(ret = find_paxos_id(sub_ob_values_row_stores, paxos_id, sub_values_index)))
      {
        YYSYS_LOG(WARN, "failed to get paxos id, err=%d", ret);
      }
      else if (sub_values_index < 0)//not find(second check)
      {
        YYSYS_LOG(WARN, "don't find paxos_id ,paxos_id:%ld", paxos_id);
      }
      else if (OB_SUCCESS != (ret = bind_static_values(sub_ob_values_row_stores.at(sub_values_index), static_values)))
      {
        YYSYS_LOG(WARN, "bind_ob_values_sub_row_store fail,ret=%d", ret);
      }
    }
    else if(NULL != input_index_values && OB_SUCCESS != (ret = bind_expr_values(sub_expr_values_row_store, input_index_values)))
    {
        YYSYS_LOG(WARN, "bind row store to plan fail,ret=%d", ret);
    }
    if (OB_LIKELY(OB_SUCCESS == ret))
    {
      //send
      YYSYS_LOG(DEBUG,"execute sub trans[%ld],paxos_id:%ld",i,paxos_id);
      TransReturnValueInfo trans_return_value_info;
      trans_return_value_info.paxos_id_ = paxos_id;
      if (skip_execute_cur_sub_trans(paxos_id))
      {
        YYSYS_LOG(INFO, "pre execution plan for cur sub trans return succeed,skip cur sub trans");
      }
      else if (OB_SUCCESS != (ret = execute_sub_trans(final_inner_plan, trans_return_value_info, (int32_t)paxos_id,
                                                 split_values, outer_result_set, session)))
      {
        if (!IS_SQL_ERR(ret))
        {
          YYSYS_LOG(WARN, "failed to execute sub trans, ret=%d, paxos_id=%ld", ret, paxos_id);
        }
        if (OB_INCOMPLETE_ROW != ret)
        {
          //we need all sub trans return info when execute the pre plan
          break;
        }
        else
        {
          ret = OB_SUCCESS;
          continue;
        }
      }
      else
      {
        //del lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160425:b
        //affect_nums_total += local_result_.get_affected_rows();
        //warnning_nums_total += local_result_.get_warning_count();
        //del 20160425:e
      }
    }
  }//end for

  if (OB_SUCCESS == ret )
  {
    //del lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160425:b
    /**
     * some sub trans of pre plan are succeed, affect some rows, the full plan
     * only excute the sub trans that return OB_INCOMPLETE_ROW affect no rows.
     */
    //outer_result_set->set_affected_rows(affect_nums_total);
    //outer_result_set->set_warning_count(warnning_nums_total);
    //del 20160425:e
    //mod lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160425:b
    //if (split_values && session->get_autocommit() && !session->get_trans_start())
    if (((ObUpsExecutor::PRE_INNER_PLAN == inner_plan_type_ && is_all_pre_sub_trans_return_success())
         || ObUpsExecutor::PRE_INNER_PLAN != inner_plan_type_)
        &&(split_values && session->get_autocommit() && !session->get_trans_start()))
    //mod 20160425:e
    {
      if (OB_SUCCESS != (ret = commit_trans()))
      {
        YYSYS_LOG(DEBUG, "fail to end tmp trans");
      }
    }
  }
  else
  {
    //not success, distributed trans, single sql statement
    //single sql statement execute failed, rollback distributed trans
    if (split_values && session->get_autocommit() && !session->get_trans_start())
    {
      YYSYS_LOG(WARN, "failed to execute statement, err=%d, trans rollback", ret);
      rollback_distributed_trans();
    }
  }
  return ret;
}

int ObUpsExecutor::bind_expr_values(const ObSubRowStore &sub_row_store, ObExprValues *& input_values)
{
  int ret = OB_SUCCESS;
  YYSYS_LOG(DEBUG,"bind expr values");
  if (NULL != input_values)
  {
    input_values->row_store_reuse();//ready for this time
    int64_t row_num = sub_row_store.row_store_.count();
    YYSYS_LOG(DEBUG,"expr values row num:%ld",row_num);
    if (0 == row_num)
    {
      ret = OB_EMPTY_ARRAY;
      YYSYS_LOG(ERROR, "empty sub row store,ret=%d",ret);
    }
    else
    {
      for (int64_t i = 0; OB_SUCCESS == ret && i < row_num; i++)
      {
        ObRow &row = const_cast<ObRow&>(sub_row_store.row_store_.at(i));
        YYSYS_LOG(DEBUG,"expr value add row :%s",to_cstring(row));
        if (OB_SUCCESS != (ret = input_values->add_values(row)))
        {
          YYSYS_LOG(WARN, "bind ObExprValues to plan fail,ret=%d",ret);
        }
      }
    }
  }
  else
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"there is no ObExprValues,ret=%d",ret);
  }
  return ret;
}

int ObUpsExecutor::bind_static_values(const ObSubRowStore &sub_row_store, ObValues *& static_values)
{
  int ret = OB_SUCCESS;
   YYSYS_LOG(DEBUG,"bind static values");
  if (NULL != static_values)
  {
    static_values->row_store_reuse();
    int64_t row_num = sub_row_store.row_store_.count();
    if (0 == row_num)
    {
      ret = OB_EMPTY_ARRAY;
      YYSYS_LOG(ERROR, "empty sub row store,ret=%d",ret);
    }
    else
    {
      for (int64_t i = 0; OB_SUCCESS == ret && i < row_num; i++)
      {
        const ObRow &row = sub_row_store.row_store_.at(i);
        if(OB_SUCCESS != (ret = static_values->add_values(row)))
        {
          YYSYS_LOG(WARN, "bind ObValues to plan fail,ret=%d",ret);
        }
      }
    }
  }
  else
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR,"there is no ObValues,ret=%d",ret);
  }
  return ret;
}

int ObUpsExecutor::resolve_input_values(ObPhysicalPlan *& final_inner_plan,
                                        ObArray<ObSubRowStore>& sub_ob_expr_values_row_stores,
                                        ObArray<int64_t>& paxos_id_array,
                                        common::ModuleArena &mem_allocator)
{
  int ret = OB_SUCCESS;
  YYSYS_LOG(DEBUG,"resolve input values");
  ObExprValues *expr_values_operator = NULL;
  if (NULL == final_inner_plan)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR, "final inner plan not init, ret=%d",ret);
  }
  else
  {
    ObArray<ObRow> &expr_values_stores = final_inner_plan->get_expr_value_row_store();   //存储ob_expr_values 所有行数组
    expr_values_stores.clear();  //prepare bug,需要每次清空

    if (NULL == (expr_values_operator = dynamic_cast<ObExprValues *>(final_inner_plan->get_phy_op_by_id_from_operator_store(final_inner_plan->get_expr_values_op_id()))))
    {
      ret = OB_ERR_NOT_EXIST_OPERATOR;
      YYSYS_LOG(ERROR, "get ObExprValues op fail");
    }
    //1.eval the expresions
    else if (OB_SUCCESS != (ret = expr_values_operator->eval(expr_values_stores, mem_allocator)))//need deep copy for obj with string value
    {
      YYSYS_LOG(WARN, "eval expr_values fail,ret=%d",ret);
    }
    //2.calc paxos_id for each row
    if(OB_SUCCESS != ret )
    {}
    else if (OB_SUCCESS != (ret = assign_paxos_id(final_inner_plan, expr_values_stores)))
    {
      YYSYS_LOG(WARN, "assign paxos id failed,ret=%d",ret);
    }
    //3.distribute to sub row store according the paxos id
    else if (OB_SUCCESS != (ret = distribute_values_row_store(expr_values_stores,
                                                              sub_ob_expr_values_row_stores,
                                                              paxos_id_array)))
    {
      YYSYS_LOG(WARN, "assign paxos id failed,ret=%d",ret);
    }
  }
  return ret;
}

int ObUpsExecutor::assign_paxos_id(ObPhysicalPlan *& final_inner_plan, ObArray<ObRow>& values_stores)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = check_partition_manager()))
  {
    YYSYS_LOG(WARN,"check partition manager failed, ret=%d",ret);
  }
  else if (NULL == final_inner_plan)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR, "finla inner plan not init,ret=%d",ret);
  }

  if (OB_SUCCESS == ret)
  {
    const ObRowkey *rowkey = NULL;
    int32_t  paxos_id = -1;
    uint64_t table_id = OB_INVALID_ID;
    ObBasicStmt::StmtType stmt_type = ObBasicStmt::T_NONE;
    if (OB_INVALID_ID == (table_id = final_inner_plan->get_table_id()))
    {
      ret = OB_ERR_UNEXPECTED;
      YYSYS_LOG(ERROR, "table id is not init,ret=%d",ret);
    }
    else if (ObBasicStmt::T_NONE == (stmt_type = final_inner_plan->get_stmt_type()))
    {
      ret = OB_ERR_UNEXPECTED;
      YYSYS_LOG(ERROR, "stmt type is not inti,ret=%d",ret);
    }
    else
    {
      common::ObCalcInfo calc_info;
      calc_info.set_table_id(table_id);
      calc_info.set_stmt_type(stmt_type);
      calc_info.set_schema_manager(sql_context_->schema_manager_);
      for (int64_t i = 0; OB_SUCCESS == ret && i < values_stores.count(); i++)
      {
        values_stores.at(i).get_rowkey(rowkey);
        calc_info.set_row_key(rowkey);//set rowkey
        if (OB_SUCCESS != (ret = sql_context_->partition_mgr_->get_paxos_id(calc_info, paxos_id)))
        {
          YYSYS_LOG(WARN, "get paxos id failed,ret=%d", ret);
        }
        else
        {
          values_stores.at(i).set_paxos_id(paxos_id);
          YYSYS_LOG(DEBUG,"current row paxos_id is[%d]",paxos_id);
        }
      }//end for
    }
  }
  return ret;
}

int ObUpsExecutor::assign_paxos_id(ObArray<ObRow>& values_stores, const ObArray<int64_t>& paxos_ids)
{
  int ret = OB_SUCCESS;
  int64_t row_count = values_stores.count();
  if (paxos_ids.count() != row_count)
  {
    ret = OB_ERROR;
    YYSYS_LOG(ERROR, "row count=%ld doesn't equal the paxos ids count=%ld,ret=%d",row_count, paxos_ids.count(), ret);
  }
  else
  {
    for (int64_t i = 0; OB_SUCCESS == ret && i < row_count; i++)
    {
        values_stores.at(i).set_paxos_id(paxos_ids.at(i));
        YYSYS_LOG(DEBUG,"current row paxos_id is[%ld]",paxos_ids.at(i));
    }//end for
  }
  return ret;
}

int ObUpsExecutor::distribute_values_row_store(const ObArray<ObRow>& values_stores,
                                               ObArray<ObSubRowStore>& sub_values_row_stores,
                                               ObArray<int64_t>& paxos_id_array)
{
  int ret = OB_SUCCESS;
  int64_t row_num = values_stores.count();
  for (int64_t i = 0; OB_SUCCESS == ret && i < row_num; i++) //for each row
  {
    const ObRow &row = values_stores.at(i);
    int64_t paxos_id = row.get_paxos_id();
    int64_t index = -1;
    if (paxos_id < 0)
    {
      ret = OB_ERR_PAXOS_ID;
      YYSYS_LOG(ERROR, "error paxos id,ret=%d",OB_ERR_PAXOS_ID);
    }
    else
    {
      if (OB_SUCCESS != (ret = paxos_id_array.push_back(paxos_id)))
      {
        YYSYS_LOG(WARN, "push paxos id failed, ret=%d",ret);
      }
      else if (OB_SUCCESS != (ret = find_paxos_id(sub_values_row_stores, paxos_id, index)))
      {
        YYSYS_LOG(WARN, "find paxos_id in sub_row_store_array fail,ret=%d", ret);
      }
      else if (index >= 0) //find,add to the sub_set with same paxos_id
      {
        YYSYS_LOG(DEBUG,"current values sub set row is[%s],set num is[%ld]",to_cstring(row),index);
        if (OB_SUCCESS != (ret = sub_values_row_stores.at(index).add_row(row)))
        {
          YYSYS_LOG(WARN, "sub values row store add row failed, ret=%d",ret);
        }
      }
      else //index< 0 new one
      {
        //create new ObSubRowStore and push back
        ObSubRowStore sub_row_store;
        sub_row_store.paxos_id_ = paxos_id;
        sub_row_store.add_row(row);
        YYSYS_LOG(DEBUG,"current values sub set row is[%s]",to_cstring(row));
        if (OB_SUCCESS != (ret = sub_values_row_stores.push_back(sub_row_store)))
        {
          YYSYS_LOG(WARN, "push sub row store failed,ret=%d",ret);
        }
      }
    }
  }//end for
  return ret;
}



int  ObUpsExecutor::find_paxos_id(const ObArray<ObSubRowStore> &sub_row_store_array,const int64_t paxos_id,int64_t &index)
{
  int ret = OB_SUCCESS;
  bool had_found = false;
  int64_t sub_row_store_size = sub_row_store_array.count();
  index = -1;
  if (paxos_id < 0)
  {
    ret = OB_ERR_PAXOS_ID;
    YYSYS_LOG(ERROR, "paxos_id is illegal paxos_id,ret=%d",ret);
  }
  if (OB_SUCCESS == ret)
  {
    for (int64_t i = 0;i < sub_row_store_size; i++)
    {
      const ObSubRowStore &row_store = sub_row_store_array.at(i);
      if (paxos_id == row_store.paxos_id_)
      {
        had_found = true;
        index = i;
        break;
      }
    }
    if (false == had_found && sub_row_store_size > 0)
    {
      index = -1;
    }
  }
  return ret;
}

int ObUpsExecutor::resolve_static_values(ObPhysicalPlan *& final_inner_plan,
                                         ObArray<ObSubRowStore>& sub_ob_values_row_stores,
                                         const ObArray<int64_t>& paxos_id_array,
                                         common::ModuleArena &mem_allocator)
{
  /**
   * for static data, the CS may return empty row with ActionFlag::OP_ROW_DOES_NOT_EXIST,
   * and the row may not have rowkey info,but the rows has the same order with input values
   */
  int ret = OB_SUCCESS;
  YYSYS_LOG(DEBUG,"resolve static values");
  ObValues *values_operator = NULL;
  ObArray<int64_t> static_data_paxos_id_array;
  values_operator = dynamic_cast<ObValues *>(final_inner_plan->get_phy_op_by_id_from_operator_store(final_inner_plan->get_values_op_id()));
  if (NULL != values_operator)//update insert delete here
  {
    ObArray<ObRow> &values_row_store = final_inner_plan->get_values_row_store();
    values_row_store.clear();
    //1.get row store array
    if (OB_SUCCESS != (ret = values_operator->get_row_store_array(values_row_store, mem_allocator)))
    {
      YYSYS_LOG(WARN,"get row store of ObValues failed,ret=%d",ret);
    }
    //2.calc paxos id for each row with the paxos id has been calced
    else if (OB_SUCCESS != (ret = assign_paxos_id(values_row_store, paxos_id_array)))
    {
      YYSYS_LOG(WARN, "assign paxos id failed,ret=%d",ret);
    }
    //3.distribute to sub row store according the paxos id
    else if (OB_SUCCESS != (ret = distribute_values_row_store(values_row_store,
                                                              sub_ob_values_row_stores,
                                                              static_data_paxos_id_array)))
    {
      YYSYS_LOG(WARN,"distribute Obvalues rows failed,ret=%d",ret);
    }
    else if (OB_SUCCESS != (ret = check_paxos_ids(paxos_id_array,
                                                  static_data_paxos_id_array)))
    {
      YYSYS_LOG(WARN, "check paxos ids failed, ret=%d",ret);
    }
  }
  else
  {
    YYSYS_LOG(DEBUG, "has no ObValues");
  }
  return ret;
}

int ObUpsExecutor::check_paxos_ids(const ObArray<int64_t>& input_values_paxos_ids,
                                   const ObArray<int64_t>& static_values_paxos_ids)
{
  int ret = OB_SUCCESS;
  int64_t input_size = input_values_paxos_ids.count();
  int64_t static_size = static_values_paxos_ids.count();
  //must has the same row num
  if (input_size != static_size)
  {
    ret = OB_ERROR;
    YYSYS_LOG(ERROR, "the input row size is not equal the static values size, input_size=%ld, static_size=%ld,ret=%d",input_size, static_size, ret);
  }
  else
  {
    //each row must has the same paxos id
    for (int64_t i = 0; OB_SUCCESS == ret && i < input_size; i++)
    {
      if (input_values_paxos_ids.at(i) != static_values_paxos_ids.at(i))
      {
        ret = OB_ERROR;
        YYSYS_LOG(ERROR, 
		"the input row paxos id is not equal the static values paxos id, input paxos_id=%ld, static_paxos_id=%ld,ret=%d",
		input_values_paxos_ids.at(i), 
		static_values_paxos_ids.at(i), 
		ret);
        break;
      }
    }
  }
  return ret;
}


bool ObUpsExecutor::can_execute_full_row_execution_plan()
{
  bool can_execute = false;
  //the full execution plan only can be executed once
  if (!executing_full_row_plan_ &&
      sub_trans_return_value_info_.count() == sub_trans_num_)
  {
    for (int64_t i = 0; i < sub_trans_return_value_info_.count(); i++)
    {
      if (OB_INCOMPLETE_ROW == sub_trans_return_value_info_.at(i).ret_)
      {
        can_execute = true;
        break;
      }
    }
  }
  return can_execute;
}


bool ObUpsExecutor::is_all_pre_sub_trans_return_success()
{
  bool all_success = true;
  for (int64_t i = 0; i < sub_trans_return_value_info_.count(); i++)
  {

      if (OB_SUCCESS != sub_trans_return_value_info_.at(i).ret_)
      {
        all_success = false;
        break;
      }
  }
  return all_success;
}

bool ObUpsExecutor::skip_execute_cur_sub_trans(int64_t paxos_id)
{
  bool skip = false;
  /**
   * only execute the full row execution plan will skip the sub trans that
   * succeed in pre plan
   */
  if (executing_full_row_plan_)
  {
    for (int64_t i = 0; i < sub_trans_return_value_info_.count(); i++)
    {
      if (sub_trans_return_value_info_.at(i).paxos_id_ == paxos_id)
      {
        if (OB_SUCCESS == sub_trans_return_value_info_.at(i).ret_)
        {
          skip = true;
          break;
        }
      }
    }
  }
  return skip;
}

int ObUpsExecutor::record_sub_trans_return_info(TransReturnValueInfo& trans_return_value_info, int ret)
{
  int err = OB_SUCCESS;
  trans_return_value_info.ret_= ret;
  if (OB_INVALID_PAXOS_ID == trans_return_value_info.paxos_id_)
  {
    err = OB_NOT_INIT;
    YYSYS_LOG(ERROR, "the paxos id is not init,ret=%d",err);
  }
  else if (!executing_full_row_plan_)
  {
    if (OB_SUCCESS != (ret = sub_trans_return_value_info_.push_back(trans_return_value_info)))
    {
      YYSYS_LOG(WARN, "add sub trans return info faild,ret=%d",ret);
    }
  }
  return err;
}
//add 20151123:e

//add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
void ObUpsExecutor::rollback_distributed_trans()
{
  int err = OB_SUCCESS;
  ObSQLSessionInfo *session = my_phy_plan_->get_result_set()->get_session();
  if(session->get_participant_num() >= 1)
  {
    //distribute transaction branch, rollback all participant session
    ObEndTransReq req;
    session->get_trans_info(req);
    req.rollback_ = true;
    if(OB_SUCCESS != (err = rpc_->ups_end_trans(req)))
    {
      YYSYS_LOG(WARN, "failed to end ups transaction, err=%d trans=%s",
                err, to_cstring(req));
    }
  }
  session->reset_trans_info();
}

bool ObUpsExecutor::need_start_new_trans(const int32_t paxos_id, const bool split_values)
{
  bool start_new_trans = false;
  ObSQLSessionInfo *session = my_phy_plan_->get_result_set()->get_session();
  if(0 == session->get_participant_num())
  {
    start_new_trans = (!session->get_autocommit() || session->get_trans_start() || split_values);
  }
  else if(session->get_participant_num() > 0)
  {
    ObTransID part_trans_id;
    if(OB_ENTRY_NOT_EXIST == session->get_participant_trans_info(paxos_id, part_trans_id))
    {
      start_new_trans = true;
    }
    session->set_trans_id(part_trans_id);
    YYSYS_LOG(DEBUG, "paxos id:%d, need start new trans:%d, trans_id:%s",
              paxos_id, start_new_trans, to_cstring(part_trans_id));
  }
  return start_new_trans;
}

int ObUpsExecutor::commit_trans()
{
  int ret = OB_SUCCESS;
  //send end trans to ups
  ObEndTransReq req;
  ObSQLSessionInfo *session = my_phy_plan_->get_result_set()->get_session();
  //OB_ASSERT(session);
  session->get_trans_info(req);
  if(!req.trans_id_.is_valid())
  {
    YYSYS_LOG(WARN, "not in transaction");
  }
  else if(OB_SUCCESS != (ret = rpc_->ups_end_trans(req)))
  {
    YYSYS_LOG(WARN, "failed to end ups transaction, err=%d trans=%s",
              ret, to_cstring(req));
    if(OB_TRANS_ROLLBACKED == ret)
    {
      YYSYS_LOG(USER_ERROR, "transaction is rolled back");
    }
  }
  session->reset_trans_info();
  if(0 < session->get_curr_trans_start_time())
  {
    session->set_curr_trans_start_time(0);
  }
  return ret;
}

int ObUpsExecutor::execute_sub_trans(
    //add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160425:b
    ObPhysicalPlan *& final_inner_plan,
    TransReturnValueInfo& trans_return_value_info,
    //add 20160425:e
    const int32_t paxos_id,
    const bool split_values,
    ObResultSet *&outer_result_set,
    ObSQLSessionInfo *&session)
{
  int ret = OB_SUCCESS;
  int err = OB_SUCCESS;
  int tmp_err = OB_SUCCESS;
  ObServer update_server;
  local_result_.clear();
  bool start_new_trans = need_start_new_trans(paxos_id, split_values);
 //add by maosy [MultiUps 1.0] [batch_iud_snapshot] 20170622 b:
  if(session->get_master_ups_transid(paxos_id) != OB_INVALID_DATA)
  {
      ObPhyOperator* fuse_op = final_inner_plan->get_phy_op_by_id_from_operator_store(final_inner_plan->get_multiple_merge_op_id());
      if(NULL != (dynamic_cast<ObMultipleMerge*>(fuse_op)))
      {
          dynamic_cast<ObMultipleMerge*>(fuse_op)->set_start_time(session->get_master_ups_transid(paxos_id));
      }
  }
  // add by e
  if(OB_SUCCESS != (ret = rpc_->get_master_update_server(paxos_id, update_server)))
  {
    YYSYS_LOG(WARN, "failed to get participant, paxos id:%d, ret=%d", paxos_id, ret);
  }
  else if(!start_new_trans && session->get_participant_num() > 0
          && session->get_current_trans_id().ups_ != update_server)
  {
    YYSYS_LOG(WARN, "paxos[%d] master ups changed, old master:%s, current master:%s , trans rollback",
              paxos_id, to_cstring(session->get_current_trans_id().ups_), to_cstring(update_server));
    ret = OB_TRANS_ROLLBACKED;
    rollback_distributed_trans();
  }
  else if(!is_row_num_null_)
  {
    /// @note lijianqiang::add new param final_inner_plan repalce the "inner_plan_"
    final_inner_plan->set_start_trans(start_new_trans);
    if(start_new_trans
        && (OB_SUCCESS != (ret = set_trans_params(session, final_inner_plan->get_trans_req()))))
    {
      YYSYS_LOG(WARN, "failed to set params for transaction request, err=%d", ret);
    }
    else if(outer_result_set->is_with_rows()
            && OB_SUCCESS != (ret = make_fake_desc(outer_result_set->get_field_columns().count())))
    {
      YYSYS_LOG(WARN, "failed to get row descriptor, err=%d", ret);
    }
    int64_t remain_us = 0;
    if(OB_LIKELY(OB_SUCCESS == ret))
    {
      int64_t begin_time_us = yysys::CTimeUtil::getTime();
      if(my_phy_plan_->is_timeout(&remain_us))
      {
        ret = OB_PROCESS_TIMEOUT;
        YYSYS_LOG(WARN, "ups execute timeout. remain_us[%ld]", remain_us);
      }
      else if(OB_UNLIKELY(NULL != my_phy_plan_ && my_phy_plan_->is_terminate(ret)))
      {
        YYSYS_LOG(WARN, "execution was terminated ret is %d", ret);
      }
      else if(OB_SUCCESS != (ret = rpc_->ups_plan_execute(remain_us, *final_inner_plan, update_server, local_result_)))
      {
        int64_t elapsed_us = yysys::CTimeUtil::getTime() - begin_time_us;
        OB_STAT_INC(MERGESERVER, SQL_UPS_EXECUTE_COUNT);
        OB_STAT_INC(MERGESERVER, SQL_UPS_EXECUTE_TIME, elapsed_us);
        //del lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160422:b
        //YYSYS_LOG(WARN, "failed to execute plan on updateserver, err=%d", ret);
        //del 20160422:e
        //add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20151201:b
        /**
         * before sending the inner_plan, the major_forzen_version is V, active_mem_version is V+1,
         * during the inner_plan is sending to UPS, major_froze was taken place, which lead to the
         * major_frozen_version is V+1, the active_mem_version is V+2,but the versionRange of inner_plan
         * is still [v+1,max), in MultiUPS, the partiton rules will be legal if the versionRange
         * is serial,current active_mem_version is v+2 not equal v+1, this case is not permitted,
         * or the data will be lost which with version v+1. so in UPS when the inner_plan trans is opened,
         * the versionRange will be checked firstly.
         */
        //if (OB_INVALID_START_VERSION == ret)
        if (OB_CHECK_VERSION_RETRY == ret)
        {
          //YYSYS_LOG(INFO, "rules changed,try to redo inner plan,ret=%d",OB_INVALID_START_VERSION);
            YYSYS_LOG(INFO, "rules changed,try to redo inner plan,ret=%d",OB_CHECK_VERSION_RETRY);
        }
        else if (OB_INCOMPLETE_ROW == ret)
        {
          /**
           * only pre_execution_plan_ can return this err code,
           */
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = record_sub_trans_return_info(trans_return_value_info, OB_INCOMPLETE_ROW)))
          {
            tmp_ret = OB_ERROR;
            YYSYS_LOG(ERROR, "record sub trans return info failed,ret=%d",tmp_ret);
          }
        }
        else
        {
          YYSYS_LOG(WARN, "failed to execute plan on updateserver, err=%d", ret);
        }
        //add 20151201:e

        //mod lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160425:b
        //if(OB_TRANS_ROLLBACKED == ret || split_values)
        if(OB_TRANS_ROLLBACKED == ret || (split_values && OB_INCOMPLETE_ROW != ret))
        //mod 20160425:e
        {
          // when updateserver returning TRANS_ROLLBACKED, it cannot get local_result_ to fill error message
          YYSYS_LOG(USER_ERROR, "transaction is rolled back");
          rollback_distributed_trans();
        }
      }
      else
      {
        int64_t elapsed_us = yysys::CTimeUtil::getTime() - begin_time_us;
        OB_STAT_INC(MERGESERVER, SQL_UPS_EXECUTE_COUNT);
        OB_STAT_INC(MERGESERVER, SQL_UPS_EXECUTE_TIME, elapsed_us);
        ret = local_result_.get_error_code();
        if(start_new_trans && local_result_.get_trans_id().is_valid())
        {
          FILL_TRACE_LOG("ups_err=%d ret_trans_id=%s", ret, to_cstring(local_result_.get_trans_id()));
          OB_STAT_INC ( OBMYSQL, SQL_MULTI_STMT_TRANS_COUNT );
          OB_STAT_INC ( OBMYSQL, SQL_MULTI_STMT_TRANS_STMT_COUNT );
          session->set_trans_id(local_result_.get_trans_id());
          if(OB_SUCCESS != (err = session->check_mem_version(local_result_.get_memtable_version())))
          {
            YYSYS_LOG(WARN, "mem version not match, trans will rollback");
          }
          else if(OB_SUCCESS != (err = session->add_participant_info(paxos_id, local_result_.get_trans_id())))
          {
            YYSYS_LOG(WARN, "add participant info fail, trans will rollback");
          }
          else
          {
            YYSYS_LOG(DEBUG, "add new trans_id:%s, paxos id:%d success. enable_dis_trans_=%d",
                      to_cstring(local_result_.get_trans_id()), paxos_id, enable_dis_trans_);
            if (!enable_dis_trans_)
            {
                if (session->get_participant_num()>1)
                {
                    tmp_err = OB_DISABLE_DIS_TRANS;
                    YYSYS_LOG(USER_ERROR, "enable_dis_trans is false, now not allow use dis trans");
                    rollback_distributed_trans();
                }
            }
            if (OB_SUCCESS!= tmp_err)
            {}
            //if(!session->get_trans_start() && 1 == session->get_participant_num())
            else if(!session->get_trans_start() && 1 == session->get_participant_num())
            {
              int64_t now = yysys::CTimeUtil::getTime();
              session->set_curr_trans_start_time(now);
              YYSYS_LOG(DEBUG, "new trans start, start=%ld", now);
            }
          }
        }

        if (OB_SUCCESS != tmp_err)
        {}
        else if(OB_CHECK_VERSION_RETRY == ret)
        {
            YYSYS_LOG(INFO, "rules changed,try to redo inner plan,ret=%d",OB_CHECK_VERSION_RETRY);
        }
        //add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160425:b
        /*if (OB_INVALID_START_VERSION == ret)
        {
          YYSYS_LOG(INFO, "rules changed,try to redo inner plan,ret=%d",OB_INVALID_START_VERSION);
        }
        */
        else if (OB_INCOMPLETE_ROW == ret || OB_SUCCESS == ret)
        {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = record_sub_trans_return_info(trans_return_value_info, ret)))
          {
            tmp_ret = OB_ERROR;
            YYSYS_LOG(ERROR, "record sub trans return info failed,ret=%d",tmp_ret);
          }
        }
        //add 20160425:e

        if (OB_SUCCESS != tmp_err)
        {}
        //add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160425:b
        else if (OB_INCOMPLETE_ROW != ret || OB_SUCCESS != err)
        {
        //add 20160425:e
          if(OB_SUCCESS != ret || OB_SUCCESS != err)
          {
            YYSYS_LOG(WARN, "ups execute plan failed, ret=%d, trans_id=%s",
                      ret, to_cstring(local_result_.get_trans_id()));
            if(OB_TRANS_ROLLBACKED == ret || OB_MEM_VERSION_NOT_MATCH == err
               //mod lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160425:b
               //|| OB_ARRAY_OUT_OF_RANGE == err || split_values)
               || OB_ARRAY_OUT_OF_RANGE == err || (split_values && OB_INCOMPLETE_ROW != ret))
              //mod 20160425:e
            {
              YYSYS_LOG(WARN, "transaction is rolled back, ret:%d, err:%d", ret, err);
              rollback_distributed_trans();
            }
          }
          else
          {
            YYSYS_LOG(DEBUG, "affected_rows=%ld warning_count=%ld",
                      local_result_.get_affected_rows(), local_result_.get_warning_count());
            outer_result_set->set_affected_rows(local_result_.get_affected_rows());
            outer_result_set->set_warning_count(local_result_.get_warning_count());
            //add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160425:b
            /**
             * we add to member to record the total affected num and warning count
             * the outer_result_set will be setted finally
             */
            total_affected_num_ += local_result_.get_affected_rows();
            total_warning_count_ += local_result_.get_warning_count();
            //add 20160425:e
            if(0 < local_result_.get_warning_count())
            {
              local_result_.reset_iter_warning();
              const char *warn_msg = NULL;
              while(NULL != (warn_msg = local_result_.get_next_warning()))
              {
                YYSYS_LOG(WARN, "updateserver warning: %s", warn_msg);
              }
            }
          }
        //add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160425:b
        }
        //add 20160425:e
      }
    }
  }/*
  // add by maosy [Delete_Update_Function_isolation_RC] 20161228
  else if(OB_SUCCESS==ret && session->get_read_times () == EMPTY_ROW_CLEAR)
  {
    int64_t remain_us = 0;
      if (my_phy_plan_->is_timeout(&remain_us))
      {
        ret = OB_PROCESS_TIMEOUT;
        YYSYS_LOG(WARN, "ups execute timeout. remain_us[%ld]", remain_us);
      }
      else if (OB_UNLIKELY(NULL != my_phy_plan_ && my_phy_plan_->is_terminate(ret)))
      {
          YYSYS_LOG(WARN, "execution was terminated ret is %d", ret);
      }
      else if (OB_ERR_BATCH_EMPTY_ROW == (ret = rpc_->ups_plan_execute(remain_us, *inner_plan_, local_result_)))
      {
          ret = OB_SUCCESS;
      }
      else
      {
         YYSYS_LOG(WARN,"empty packet to clear start time for batch is failed ,ret = %d",ret);
      }
  }
  // add e*/
  if (OB_SUCCESS != tmp_err)
  {
      ret = tmp_err;
  }
  else if(OB_SUCCESS == ret && OB_SUCCESS != err)
  {
    ret = err;
  }
  return ret;
}

//add 20150701:e

int ObUpsExecutor::set_trans_params(ObSQLSessionInfo *session, common::ObTransReq &req)
{
  int ret = OB_SUCCESS;
  // get isolation level etc. from session
  ObObj val;
  ObString isolation_str;
  int64_t tx_timeout_val = 0;
  int64_t tx_idle_timeout = 0;
  if(OB_SUCCESS != (ret = session->get_sys_variable_value(ObString::make_string("tx_isolation"), val)))
  {
    YYSYS_LOG(WARN, "failed to get tx_isolation value, err=%d", ret);
  }
  else if(OB_SUCCESS != (ret = val.get_varchar(isolation_str)))
  {
    YYSYS_LOG(WARN, "wrong obj type, err=%d", ret);
    ret = OB_ERR_UNEXPECTED;
  }
  else if(OB_SUCCESS != (ret = req.set_isolation_by_name(isolation_str)))
  {
    YYSYS_LOG(WARN, "failed to set isolation level, err=%d", ret);
    ret = OB_ERR_UNEXPECTED;
  }
  else if(OB_SUCCESS != (ret = session->get_sys_variable_value(ObString::make_string("yb_tx_timeout"), val)))
  {
    YYSYS_LOG(WARN, "failed to get tx_timeout value, err=%d", ret);
  }
  else if(OB_SUCCESS != (ret = val.get_int(tx_timeout_val)))
  {
    YYSYS_LOG(WARN, "wrong obj type, err=%d", ret);
    ret = OB_ERR_UNEXPECTED;
  }
  else if(OB_SUCCESS != (ret = session->get_sys_variable_value(ObString::make_string("yb_tx_idle_timeout"), val)))
  {
    YYSYS_LOG(WARN, "failed to get tx_idle_timeout value, err=%d", ret);
  }
  else if(OB_SUCCESS != (ret = val.get_int(tx_idle_timeout)))
  {
    YYSYS_LOG(WARN, "wrong obj type, err=%d", ret);
    ret = OB_ERR_UNEXPECTED;
  }
  else
  {
    req.timeout_ = tx_timeout_val;
    req.idle_time_ = tx_idle_timeout;
  }
  return ret;
}

int ObUpsExecutor::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  OB_ASSERT(my_phy_plan_);
  // for session stored physical plan, my_phy_plan_->get_result_set() is the result_set who stored the plan,
  // since the two commands are in the same session, so we can get the real result_set from session.
  // for global stored physical plan, new coppied plan has itsown my_phy_plan_, so both my_phy_plan_->get_result_set()
  // and my_phy_plan_->get_result_set()->get_session()->get_current_result_set() are correct
  ObResultSet *my_result_set = my_phy_plan_->get_result_set()->get_session()->get_current_result_set();
  if(OB_UNLIKELY(!my_result_set->is_with_rows()))
  {
    ret = OB_NOT_SUPPORTED;
  }
  else if(OB_UNLIKELY(curr_row_.get_row_desc() == NULL))
  {
    curr_row_.set_row_desc(row_desc_);
  }
  if(ret == OB_SUCCESS
      && (ret = local_result_.get_scanner().get_next_row(curr_row_)) == OB_SUCCESS)
  {
    row = &curr_row_;
  }
  return ret;
}
//add wangyao  [sequence for get prevval values]:20171206
int ObUpsExecutor::get_next_row_for_prevval(const common::ObRow *&row)
{
    int ret=OB_SUCCESS;
    ObRowDesc row_desc;
    row_desc.add_column_desc(OB_TEMP_SEQUENCE_TABLE_TID,OB_APP_MIN_COLUMN_ID);
    row_desc.add_column_desc(OB_TEMP_SEQUENCE_TABLE_TID,OB_APP_MIN_COLUMN_ID+1);
    row_desc.set_rowkey_cell_count(1);
    curr_row_.set_row_desc(row_desc);
    //ret = local_result_.get_scanner().get_next_row(curr_row_);
    if (OB_SUCCESS !=(ret = local_result_.get_scanner().get_next_row(curr_row_)))
    {
      ret =OB_ERROR;
    }
    else
    {
       row = &curr_row_;
    }
    return ret;
}
//add e
//add wangyao  [sequence for optimize]:20171005
int ObUpsExecutor::get_next_row_for_sequence(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  ObRowDesc row_desc;
  for(int i = OB_APP_MIN_COLUMN_ID;i < END_ID;i++)
  {
      if(i==CONST_START_WITH_ID)
      {
          continue;
      }
      else
      {
          row_desc.add_column_desc(OB_ALL_SEQUENCE_TID,i);
      }
  }
  row_desc.set_rowkey_cell_count(1);
  curr_row_.set_row_desc(row_desc);
  //ret = local_result_.get_scanner().get_next_row(curr_row_);
  if (OB_SUCCESS !=(ret = local_result_.get_scanner().get_next_row(curr_row_)))
  {
    ret =OB_ERROR;
  }
  else
  {
     row = &curr_row_;
  }
  return ret;
}
//add e
int ObUpsExecutor::make_fake_desc(const int64_t column_num)
{
  int ret = OB_SUCCESS;
  row_desc_.reset();
  for(int64_t i = 0; ret == OB_SUCCESS && i < column_num; i++)
  {
    if((ret = row_desc_.add_column_desc(OB_INVALID_ID, OB_APP_MIN_COLUMN_ID + i)) != OB_SUCCESS)
    {
      YYSYS_LOG(WARN, "Generate row descriptor of ObUpsExecutor failed, err=%d", ret);
      break;
    }
  }
  return ret;
}
// add by maosy [MultiUps 1.0] [batch_udi] 20170417 b
int ObUpsExecutor::execute_batch_trans_with_partition(ObPhysicalPlan *&final_inner_plan, ObResultSet *outer_result_set, ObSQLSessionInfo *session)
{
    int ret = OB_SUCCESS ;
    ObValues *static_values = NULL;
    ObExprValues * input_values = NULL;
    static_values = dynamic_cast<ObValues *>(final_inner_plan->get_phy_op_by_id_from_operator_store(final_inner_plan->get_values_op_id()));
    input_values = dynamic_cast<ObExprValues *>(final_inner_plan->get_phy_op_by_id_from_operator_store(final_inner_plan->get_expr_values_op_id()));
    sub_trans_num_ = static_values->get_sub_trans_num();
    YYSYS_LOG(DEBUG, "SUB TREAN NUM = %ld",sub_trans_num_);
    bool split_values = sub_trans_num_ >1 ? true:false;
    for(int64_t i = 0 ; i <sub_trans_num_ && OB_SUCCESS ==ret ;i++)
    {
        int64_t paxos_id = OB_INVALID_PAXOS_ID ;
        if(static_values->get_paxos_id_of_rowstore(i)!=
                ( paxos_id = input_values->get_paxos_id_of_rowstore(i)))
        {
            YYSYS_LOG(ERROR,"failed to compare paxos id,values=%ld,expr = %ld",
                      static_values->get_paxos_id_of_rowstore(i),input_values->get_paxos_id_of_rowstore(i));
            ret = OB_ERR_UNEXPECTED;
            break;
        }
        if (OB_LIKELY(OB_SUCCESS == ret))
        {
          //send
          YYSYS_LOG(DEBUG,"execute sub trans[%ld],paxos_id:%ld",i,paxos_id);
          TransReturnValueInfo trans_return_value_info;
          trans_return_value_info.paxos_id_ = paxos_id;
          if (OB_SUCCESS != (ret = execute_sub_trans(final_inner_plan, trans_return_value_info, (int32_t)paxos_id,
                                                     split_values, outer_result_set, session)))
          {
            if (!IS_SQL_ERR(ret))
            {
              YYSYS_LOG(WARN, "failed to execute sub trans, ret=%d, paxos_id=%ld", ret, paxos_id);
            }
            if (OB_INCOMPLETE_ROW != ret)
            {
              //we need all sub trans return info when execute the pre plan
              break;
            }
            else
            {
              ret = OB_SUCCESS;
              continue;
            }
          }

        }
    }// end for

    if (OB_SUCCESS == ret )
    {
      if (((ObUpsExecutor::PRE_INNER_PLAN == inner_plan_type_ && is_all_pre_sub_trans_return_success())
           || ObUpsExecutor::PRE_INNER_PLAN != inner_plan_type_)
          &&(split_values && session->get_autocommit() && !session->get_trans_start()))
      {
        if (OB_SUCCESS != (ret = commit_trans()))
        {
          YYSYS_LOG(DEBUG, "fail to end tmp trans");
        }
      }
    }
    else
    {
      if (split_values && session->get_autocommit() && !session->get_trans_start())
      {
        YYSYS_LOG(WARN, "failed to execute statement, err=%d, trans rollback", ret);
        rollback_distributed_trans();
      }
    }
    return ret ;
}
// add by maosy 20170417 e

int ObUpsExecutor::get_next_row_for_update_rowkey(const ObRow *&row, const ObRowDesc &fake_row_desc)
{
    int ret = OB_SUCCESS;
    curr_row_.set_row_desc(fake_row_desc);
    if (ret == OB_SUCCESS
            && (ret = local_result_.get_scanner().get_next_row(curr_row_)) == OB_SUCCESS)
    {
        row = &curr_row_;
    }
    return ret;
}

namespace oceanbase
{
  namespace sql
  {
    REGISTER_PHY_OPERATOR(ObUpsExecutor, PHY_UPS_EXECUTOR);
  }
}

int64_t ObUpsExecutor::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "UpsExecutor(ups_plan=");
  if(NULL != inner_plan_)
  {
    pos += inner_plan_->to_string(buf + pos, buf_len - pos);
  }
  databuff_printf(buf, buf_len, pos, ")\n");
  //add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160425:b
  databuff_printf(buf, buf_len, pos, "***pre_plan***\n");
  databuff_printf(buf, buf_len, pos, "UpsExecutor(pre_plan=");
  if(NULL != pre_execution_plan_)
  {
    pos += pre_execution_plan_->to_string(buf + pos, buf_len - pos);
  }
  databuff_printf(buf, buf_len, pos, ")\n");
  databuff_printf(buf, buf_len, pos, "***full_row_plan***\n");
  databuff_printf(buf, buf_len, pos, "UpsExecutor(full_row_plan=");
  if(NULL != full_row_execution_plan_)
  {
    pos += full_row_execution_plan_->to_string(buf + pos, buf_len - pos);
  }
  databuff_printf(buf, buf_len, pos, ")\n");
  //add 20160425:e
  return pos;
}

PHY_OPERATOR_ASSIGN(ObUpsExecutor)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObUpsExecutor);
  reset();
  if(!my_phy_plan_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(WARN, "ObPhysicalPlan/allocator is not set, ret=%d", ret);
  }
  else if((inner_plan_ = ObPhysicalPlan::alloc()) == NULL)
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    YYSYS_LOG(WARN, "Can not generate new inner physical plan, ret=%d", ret);
  }
  else
  {
    // inner_plan_->set_allocator(NULL); // no longer need
    inner_plan_->set_result_set(my_phy_plan_->get_result_set());
    if((ret = inner_plan_->assign(*o_ptr->inner_plan_)) != OB_SUCCESS)
    {
      YYSYS_LOG(WARN, "Assign inner physical plan, ret=%d", ret);
    }
  }
  return ret;
}

int ObUpsExecutor::null_check_error()
{
    int ret = OB_ERROR;
    ObPhyOperator *op = NULL;
    ObUpsModifyWithDmlType *dml_op = NULL;
    ObDmlType op_dml_type = OB_DML_UNKNOW;
    op = inner_plan_->get_main_query();
    dml_op = dynamic_cast<ObUpsModifyWithDmlType *>(op);
    op_dml_type = dml_op->get_dml_type();
    if(OB_DML_INSERT == op_dml_type)
    {
        ret = OB_ERR_INSERT_NULL_COLUMN;
    }
    else if(OB_DML_REPLACE == op_dml_type)
    {
        ret = OB_ERR_REPLACE_NULL_COLUMN;
    }
    return ret;
}
