#include "ob_update_rowkey.h"
#include "common/ob_errno.h"
#define CREATE_PHY_OPERRATOR_NEW(op, type_name, physical_plan, err)   \
  ({err = OB_SUCCESS;                                               \
  op = OB_NEW(type_name, ObModIds::OB_UPDATE_ROWKEY);       \
  if(op == NULL) \
{ \
  err = OB_ERR_PARSER_MALLOC_FAILED; \
  YYSYS_LOG(WARN, "Can not malloc space for %s", #type_name);  \
  } \
  else \
{ \
  op->set_phy_plan(physical_plan);  \
  ob_inc_phy_operator_stat(op->get_type()); \
  }\
  op;})

using namespace oceanbase::sql;
using namespace oceanbase::common;

ObUpdateRowkey::ObUpdateRowkey()
  : ob_ups_executor_select_(NULL), ob_ups_executor_delete_(NULL), ob_ups_executor_insert_(NULL), ob_select_more_rows_rpc_(NULL), rowkey_info_(NULL),
    ob_expr_values_(NULL), ob_table_rpc_scan_(NULL)
{
  is_multi_update_ = false;
}

ObUpdateRowkey::~ObUpdateRowkey()
{
  reset();
}

void ObUpdateRowkey::reset()
{
  if(ob_table_rpc_scan_ != NULL)
  {
    ob_table_rpc_scan_->reset();
    ob_table_rpc_scan_=NULL;
  }
  if(ob_expr_values_ != NULL)
  {
    ob_expr_values_->reset();
    ob_expr_values_=NULL;
  }
  ob_sql_expressions_.clear();
  ob_value_types_.clear();
  row_store.clear();
  if(NULL != ob_ups_executor_select_)
  {
    ob_ups_executor_select_->reset();
    ob_ups_executor_select_ = NULL;
  }
  if(NULL != ob_ups_executor_delete_)
  {
    ob_ups_executor_delete_->reset();
    ob_ups_executor_delete_ = NULL;
  }
  if(NULL != ob_ups_executor_insert_)
  {
    ob_ups_executor_insert_->reset();
    ob_ups_executor_insert_ = NULL;
  }
  old_rows_.clear();
  rowkey_info_ = NULL;
  is_multi_update_ = false;
  if(NULL != ob_select_more_rows_rpc_)
  {
    ob_select_more_rows_rpc_->reset();
    ob_select_more_rows_rpc_ = NULL;
  }
}

void ObUpdateRowkey::reuse()
{
  reset();
}

int ObUpdateRowkey::open()
{
  int ret = OB_SUCCESS;
  bool is_start_transaction = false;
  ObFillValues *fill_values = NULL;
  ObBindValues *bind_values = NULL;
  int64_t affect_rows = 0;
  bool is_empty = false;
  get_fill_bind_values(fill_values, bind_values);

  if (sql_context_.session_info_->get_trans_id().is_valid()
      || sql_context_.session_info_->get_trans_start())
  {
    is_start_transaction = false;
  }
  else if( OB_SUCCESS != (ret = start_transaction()))
  {
    YYSYS_LOG(WARN, "fail to start transaction");
  }
  else
  {
    is_start_transaction = true;
  }

  if(sql_context_.session_info_->get_trans_id().is_valid()
     || sql_context_.session_info_->get_trans_start())
  {
    ret = set_stmt_start_time();
  }

  if(OB_SUCCESS != ret)
  {}
  else if(0 != get_child_num())
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(WARN, "the operator ObUpdateRowkey is warn, ret=%d",ret);
  }
  else
  {
    //1.select old row
    const common::ObRow *cur_row = NULL;
    if (is_multi_update_)
    {
      if (OB_SUCCESS != (ret = select_more_rows_from_table(is_empty)))
      {
        YYSYS_LOG(WARN, "select old row for update faild");
      }
    }
    else if (OB_SUCCESS != (ret = ob_ups_executor_select_->open()))
    {
      YYSYS_LOG(WARN, "select old row for update faild");
    }
    else
    {
      int64_t remain_us = 0;
      if (OB_SUCCESS == ret
          && !this->my_phy_plan_->is_terminate(ret) && !this->my_phy_plan_->is_timeout(&remain_us))
      {
        ret = ob_ups_executor_select_->get_next_row_for_update_rowkey(cur_row, row_desc_);
        if (ret == OB_ITER_END)
        {
          ret = OB_SUCCESS;
          is_empty = true;
        }
        else if(OB_SUCCESS != (ret = cur_row->get_is_row_empty(is_empty)))
        {
          YYSYS_LOG(WARN, "faild to check row empty, ret=[%d]",ret);
        }
        else if(is_empty)
        {
          ret = OB_SUCCESS;
          YYSYS_LOG(WARN, "row is empty, ret=[%d]",ret);
        }
        else
        {
          const ObRowStore::StoredRow *tmp_row = NULL;
          row_store.add_row(*cur_row, tmp_row);
          old_rows_.push_back(tmp_row);
        }
      }
    }
    if(old_rows_.count() > 5375) //MAX_SUB_GET_REQUEST_NUM * (DEFAULT_MAX_GET_ROWS_PER_SUBREQ + 1)
    {
      ret = OB_ERROR_OUT_OF_RANGE;
      YYSYS_LOG(WARN, "update rows number=[%ld] > limit rows number=[5375], ret = [%d]", old_rows_.count(), OB_ERROR_OUT_OF_RANGE);
    }

    while(ret == OB_SUCCESS && !is_empty && !is_batch_over(fill_values, bind_values))
    {
      //2.delete_old_row
      if(NULL == ob_ups_executor_delete_)
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(WARN, "Ups Executor for delete must not be NULL");
      }
      else if (OB_SUCCESS != (ret = ob_ups_executor_delete_->open()))
      {
        YYSYS_LOG(WARN, "open delete old row for update faild");
      }
      else
      {
        get_affect_rows(affect_rows, fill_values, bind_values);
      }
      if(NULL == fill_values && NULL == bind_values)
      {
        break;
      }
    }
    if (is_multi_update_ && ret == OB_SUCCESS && affect_rows != old_rows_.count())
    {
      ret = OB_NOT_EQUAL_ROWS;
      YYSYS_LOG(WARN, "delete row nums[%ld] NOT equal select row nums[%ld]",affect_rows, old_rows_.count());
    }

    sql_context_.session_info_->reset_master_ups_transids();
    sql_context_.session_info_->set_read_times(NO_BATCH_UD);

    if (ret == OB_SUCCESS && !is_empty)
    {
      if (OB_SUCCESS != (ret = cons_insert_rpc_scan_and_expr_values(old_rows_, row_desc_)))
      {
        YYSYS_LOG(WARN, "cons_table_rpc_scan faild");
      }
      //4.insert_new_row
      if (ret == OB_SUCCESS && OB_SUCCESS != (ret = ob_ups_executor_insert_->open()))
      {
        YYSYS_LOG(WARN, "open insert new row for update faild");
      }
    }
  }
  set_affect_row(old_rows_.count());
  if (OB_SUCCESS != ret)
  {
    YYSYS_LOG(WARN,"update rowkey is error, need rollback");
    if(NULL != fill_values && !fill_values->is_already_clear())
    {
      fill_values->clear_prepare_select_result();
    }
    if(OB_SUCCESS != rollback())
    {
      YYSYS_LOG(WARN, "failed to rollback,ret=[%d]",ret);
    }
    else
    {
      if(OB_ERR_VARCHAR_TOO_LONG != ret && OB_DECIMAL_UNLEGAL_ERROR != ret)
      {
        YYSYS_LOG(USER_ERROR, "%s, error code = %d", ob_strerror(ret), ret);
      }
      if (OB_CHECK_VERSION_RETRY != ret)
      {
        ret = OB_TRANS_ROLLBACKED;
      }
      YYSYS_LOG(INFO, "transaction is rolled back");
    }
  }
  else if (is_start_transaction && sql_context_.session_info_->get_autocommit())
  {
    if (OB_SUCCESS != (ret = commit()))
    {
      YYSYS_LOG(WARN, "fail to commit,update rowkey, ret=[%d]",ret);
    }
  }
  return ret;
}

int ObUpdateRowkey::set_row_desc(const common::ObRowDesc &row_desc)
{
  row_desc_ = row_desc;
  return OB_SUCCESS;
}

int ObUpdateRowkey::set_row_desc_ext(const common::ObRowDescExt &row_desc_ext)
{
  row_desc_ext_ = row_desc_ext;
  return OB_SUCCESS;
}

int ObUpdateRowkey::add_sql_expression(ObSqlExpression sql_expression)
{
  return ob_sql_expressions_.push_back(sql_expression);
}

int ObUpdateRowkey::add_value_type(const ObObjType obj_type)
{
  return ob_value_types_.push_back(obj_type);
}

int ObUpdateRowkey::set_op(ObTableRpcScan *table_rpc_scan, ObExprValues *expr_values)
{
  int ret = OB_SUCCESS;
  if (table_rpc_scan == NULL || expr_values == NULL)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(ERROR, "invalid operator");
  }
  else
  {
    ob_table_rpc_scan_ = table_rpc_scan;
    ob_expr_values_ = expr_values;
  }
  return ret;
}

void ObUpdateRowkey::set_affect_row(int64_t affected_row)
{
  OB_ASSERT(my_phy_plan_);
  ObResultSet *outer_result_set = NULL;
  ObResultSet *my_result_set = NULL;
  my_result_set = my_phy_plan_->get_result_set();
  outer_result_set = my_result_set->get_session()->get_current_result_set();
  my_result_set->set_session(outer_result_set->get_session());
  outer_result_set->set_affected_rows(affected_row);
}

void ObUpdateRowkey::get_fill_bind_values(ObFillValues *&fill_values, ObBindValues *&bind_values)
{
  if (ob_ups_executor_delete_ != NULL)
  {
    ObPhysicalPlan *inner_plan = ob_ups_executor_delete_->get_inner_plan();
    int64_t ups_num = inner_plan->get_operator_size();
    for (int64_t j=0;j<ups_num;j++)
    {
      ObPhyOperator *phy_opt = inner_plan->get_phy_operator(j);
      if (NULL != (dynamic_cast<ObFillValues *>(phy_opt)))
      {
        fill_values = dynamic_cast<ObFillValues *>(phy_opt);
        break;
      }
      else if (NULL != (dynamic_cast<ObBindValues *>(phy_opt)))
      {
        bind_values = dynamic_cast<ObBindValues *>(phy_opt);
        break;
      }
    }
  }
}

void ObUpdateRowkey::get_affect_rows(int64_t &affected_row, ObFillValues *fill_values, ObBindValues *bind_values)
{
  if (NULL != fill_values)
  {
    affected_row += fill_values->get_affect_row();
  }
  else if (NULL != bind_values)
  {
    affected_row += bind_values->get_inserted_row_num();
  }
}

int ObUpdateRowkey::set_stmt_start_time()
{
  int ret = OB_SUCCESS;
  bool is_table_level = false;
  uint64_t table_id = get_phy_plan()->get_table_id();
  if(OB_INVALID_ID == table_id)
  {
    YYSYS_LOG(WARN, "table id is invalid, please check!");
  }
  else if(OB_SUCCESS != (sql_context_.partition_mgr_->get_table_part_type(table_id, is_table_level)))
  {
    YYSYS_LOG(WARN, "failed to get partition type, table_id=%lu", table_id);
  }
  YYSYS_LOG(DEBUG, "get partition type, table_id=%lu, ret=%d, is_table_level=%d", table_id, ret, is_table_level);
  if(is_table_level)
  {
    ObCalcInfo calc_info;
    int32_t paxos_id = 0;
    ObServer ups_server;
    int64_t published_transid;
    calc_info.set_stmt_type(ObBasicStmt::T_UPDATE);
    if (OB_SUCCESS != (ret = sql_context_.partition_mgr_->get_paxos_id(table_id, calc_info, paxos_id)))
    {
      YYSYS_LOG(WARN, "failed to get paxos id, err=%d", ret);
    }
    else if(OB_SUCCESS != (ret = sql_context_.merger_rpc_proxy_->get_master_update_server(paxos_id, ups_server)))
    {
      YYSYS_LOG(WARN, "failed to get master ups, ret=%d", ret);
    }
    else if(OB_SUCCESS != (ret = sql_context_.merger_rpc_proxy_->get_stmt_start_time(ups_server, published_transid)))
    {
      YYSYS_LOG(WARN, "failed to get stmt start time, ret=%d",ret);
    }
    else
    {
      YYSYS_LOG(DEBUG, "PAXOS ID=%d, time=%ld", paxos_id, published_transid);
      sql_context_.session_info_->set_master_ups_transid(paxos_id, published_transid);
    }
    if(ret == OB_SUCCESS)
    {
      return ret;
    }
    else
    {
      YYSYS_LOG(WARN, "get partition type, table_id=%lu, is_table_level=%d, paxos_id=%d, ret=%d", table_id, is_table_level, paxos_id, ret);
      ret = OB_SUCCESS;
    }
    YYSYS_LOG(DEBUG, "get partition type, table_id=%lu, is_table_level=%d, paxos_id=%d", table_id, is_table_level, paxos_id);

  }
  int32_t count = sql_context_.merger_rpc_proxy_->get_master_update_server_count();
  for (int32_t index = 0; index < count; index++)
  {
    int64_t paxos_id = 0;
    ObServer ups_server;
    int64_t published_transid;
    if (OB_SUCCESS != (ret = sql_context_.merger_rpc_proxy_->get_master_ups(index, paxos_id, ups_server)))
    {
      YYSYS_LOG(WARN, "failed to get mast ups, ret=%d", ret);
    }
    else if(OB_SUCCESS != (ret = sql_context_.merger_rpc_proxy_->get_stmt_start_time(ups_server,published_transid)))
    {
      YYSYS_LOG(WARN, "failed to get start time, ret=%d",ret);
    }
    else
    {
      sql_context_.session_info_->set_master_ups_transid(paxos_id, published_transid);
    }
  }
  return ret;
}

int ObUpdateRowkey::start_transaction()
{
  int ret = OB_SUCCESS;
  ObString start_thx = ObString::make_string("START TRANSACTION");
  ret = execute_stmt_no_return_rows(start_thx);
  return ret;
}

int ObUpdateRowkey::commit()
{
  int ret = OB_SUCCESS;
  ObString start_thx = ObString::make_string("COMMIT");
  ret = execute_stmt_no_return_rows(start_thx);
  return ret;
}

int ObUpdateRowkey::rollback()
{
  int ret = OB_SUCCESS;
  ObString start_thx = ObString::make_string("ROLLBACK");
  ret = execute_stmt_no_return_rows(start_thx);
  return ret;
}

int ObUpdateRowkey::execute_stmt_no_return_rows(const ObString &stmt)
{
  int ret = OB_SUCCESS;
  ObMySQLResultSet tmp_result;
  if (OB_SUCCESS != (ret = tmp_result.init()))
  {
    YYSYS_LOG(WARN, "init result set failed, ret=%d",ret);
  }
  else if(OB_SUCCESS != (ret = ObSql::direct_execute(stmt, tmp_result, sql_context_)))
  {
    YYSYS_LOG(WARN, "direct_execute failed, sql=%.*s, ret=%d", stmt.length(), stmt.ptr(), ret);
  }
  else if(OB_SUCCESS != (ret = tmp_result.open()))
  {
    YYSYS_LOG(WARN, "open result set failed, sql=%.*s, ret=%d", stmt.length(), stmt.ptr(), ret);
  }
  else
  {
    int err = tmp_result.close();
    if (OB_SUCCESS != err)
    {
      YYSYS_LOG(WARN, "failed to close result set,err=%d", err);
    }
    tmp_result.reset();
  }
  return ret;
}

int ObUpdateRowkey::select_more_rows_from_table(bool &empty)
{
  int ret = OB_SUCCESS;
  if (ob_select_more_rows_rpc_ != NULL)
  {
    if (OB_SUCCESS != (ret = (ob_select_more_rows_rpc_->open())))
    {
      YYSYS_LOG(WARN, "open result set failed,  ret=%d", ret);
    }
    else
    {
      const ObRow *row = NULL;
      int64_t remain_us = 0;
      while (OB_SUCCESS == ret
             && !this->my_phy_plan_->is_terminate(ret) && !this->my_phy_plan_->is_timeout(&remain_us))
      {
        ret = ob_select_more_rows_rpc_->get_next_row(row);
        if (OB_ITER_END == ret)
        {
          ret = OB_SUCCESS;
          break;
        }
        else if (OB_SUCCESS != ret)
        {
          YYSYS_LOG(WARN, "get next row from ObResultSet failed,ret=%d",ret);
        }
        else
        {
          const ObRowStore::StoredRow *tmp_row;
          row_store.add_row(*row, tmp_row);
          old_rows_.push_back(tmp_row);
        }
      }
      if (old_rows_.count() == 0)
        empty = true;
      if (OB_SUCCESS != (ret = (ob_select_more_rows_rpc_->close())))
      {
        YYSYS_LOG(WARN, "close result set failed, ret=%d", ret);
      }
    }
  }
  return ret;
}

int ObUpdateRowkey::cons_insert_rpc_scan_and_expr_values(ObArray<const ObRowStore::StoredRow *> old_rows, ObRowDesc row_desc)
{
  int ret = OB_SUCCESS;
  ObRow old_row;
  old_row.set_row_desc(row_desc_);
  uint64_t table_id = OB_INVALID_ID;
  ob_sql_read_strategy_.clear_simple_cond_filter_list();
  ob_expr_values_->set_row_desc(row_desc, row_desc_ext_);
  OB_ASSERT(ob_sql_expressions_.count() == ob_value_types_.count());
  ModuleArena buf(OB_COMMON_MEM_BLOCK_SIZE, ModulePageAllocator(ObModIds::OB_UPDATE_ROWKEY));

  ObSqlExpression *rows_filter = ObSqlExpression::alloc();
  if (NULL == rows_filter)
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    YYSYS_LOG(WARN, "no memory");
  }
  ObSqlExpression column_ref;

  ExprItem expr_item;
  expr_item.type_ = T_REF_COLUMN;
  int64_t rowkey_column_num = rowkey_info_->get_size();
  for(int i = 0; OB_SUCCESS == ret && i < row_desc.get_column_num(); ++i)
  {
    if (OB_UNLIKELY(OB_SUCCESS != (ret = row_desc.get_tid_cid(i, table_id, expr_item.value_.cell_.cid))))
    {
      YYSYS_LOG(WARN, "failed to get_tid_cid, err=%d",ret);
      break;
    }
    else if(rowkey_info_->is_rowkey_column(expr_item.value_.cell_.cid))
    {
      expr_item.value_.cell_.tid = table_id;
      column_ref.reset();
      column_ref.set_tid_cid(table_id, expr_item.value_.cell_.cid);
      if (OB_SUCCESS != (ret = rows_filter->add_expr_item(expr_item)))
      {
        YYSYS_LOG(WARN, "failed to add expr item, err=%d",ret);
        break;
      }
      else if (OB_SUCCESS != (ret = column_ref.add_expr_item(expr_item)))
      {
        YYSYS_LOG(WARN, "failed to add expr_item, err=%d",ret);
        break;
      }
      else if (OB_SUCCESS != (ret = column_ref.add_expr_item_end()))
      {
        YYSYS_LOG(WARN, "failed to add expr item, err=%d",ret);
        break;
      }
      else if (OB_SUCCESS != (ret = ob_table_rpc_scan_->add_output_column(column_ref)))
      {
        YYSYS_LOG(WARN, "failed to add output column, err=%d",ret);
        break;
      }
    }
  }//end for
  if(OB_LIKELY(OB_SUCCESS == ret))
  {
    column_ref.reset();
    column_ref.set_tid_cid(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);
    if (OB_SUCCESS != (ret = ObSqlExpressionUtil::make_column_expr(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, column_ref)))
    {
      YYSYS_LOG(WARN, "fail to make column expr:ret[%d]",ret);
    }
    else if (OB_SUCCESS!= (ret = ob_table_rpc_scan_->add_output_column(column_ref)))
    {
      YYSYS_LOG(WARN, "failed to add output column, err=%d",ret);
    }
  }
  if (OB_LIKELY(OB_SUCCESS == ret))
  {
    expr_item.type_ = T_OP_ROW;
    expr_item.value_.int_ = rowkey_column_num;
    if (OB_SUCCESS != (ret = rows_filter->add_expr_item(expr_item)))
    {
      YYSYS_LOG(WARN, "failed to add expr item,err=%d",ret);
    }
  }
  if (OB_LIKELY(OB_SUCCESS == ret))
  {
    expr_item.type_ = T_OP_LEFT_PARAM_END;
    expr_item.value_.int_ = 2;
    if (OB_SUCCESS != (ret = rows_filter->add_expr_item(expr_item)))
    {
      YYSYS_LOG(WARN, "failed to add expr item, err=%d",ret);
    }
  }

  for(int64_t row_idx = 0; ret == OB_SUCCESS && row_idx < old_rows.count(); row_idx++)
  {
    if(OB_SUCCESS != (ret = ObRowUtil::convert(old_rows.at(row_idx)->get_compact_row(), old_row)))
    {
    }
    ObRow new_row = old_row;
    ObObj tmp_value;
    ObObj cast_value;
    uint64_t column_id = OB_INVALID_ID;
    char cast_buf[OB_MAX_ROW_LENGTH];
    ObString cast_buffer;

    for(int64_t column_idx = 0; ret == OB_SUCCESS && column_idx < row_desc.get_column_num(); ++column_idx)
    {
      if (OB_UNLIKELY(OB_SUCCESS != (ret = row_desc.get_tid_cid(column_idx, table_id, column_id))))
      {
        YYSYS_LOG(WARN, "failed to get_tid_cid, err=%d",ret);
        break;
      }
      else
      {
        for (int64_t idx = 0; ret == OB_SUCCESS && idx < ob_sql_expressions_.count(); idx++)
        {
          const ObObj *value = NULL;
          ObSqlExpression update_filter = ob_sql_expressions_.at(idx);
          const ObObjType type = ob_value_types_.at(idx);
          if (update_filter.get_column_id() == column_id)
          {
            if(OB_SUCCESS != (ret = update_filter.calc(old_row, value)))
            {
              YYSYS_LOG(WARN, "failed to calculate, err=%d",ret);
              break;
            }
            else
            {
              cast_buffer.assign_ptr(cast_buf, OB_MAX_ROW_LENGTH);
              cast_value = *value;
              if (OB_SUCCESS != (ret = obj_cast(cast_value, type, cast_buffer)))
              {
                YYSYS_LOG(WARN, "failed to cast obj, value=%s, type=%d, err=%d", to_cstring(*value), type, ret);
                break;
              }
            }
            if(OB_SUCCESS!= (ret = ob_write_obj_v2(buf,cast_value,tmp_value)))
            {
              YYSYS_LOG(WARN, "str buf write obj fail:ret[%d]",ret);
            }
            else if(OB_SUCCESS != (ret = new_row.set_cell(table_id, column_id, tmp_value)))
            {
              YYSYS_LOG(WARN, "failed to set cell, err=%d",ret);
              break;
            }
          }
        }
      }
    }
    ObConstRawExpr col_val;
    for(int64_t rowkey_idx = 0; OB_SUCCESS == ret && rowkey_idx < row_desc.get_column_num(); rowkey_idx++)
    {
      const ObObj *cell = NULL;
      uint64_t rowkey_cid = OB_INVALID_ID;
      if (OB_SUCCESS != (ret = row_desc.get_tid_cid(rowkey_idx, table_id, rowkey_cid)))
      {
        break;
      }
      else if (rowkey_info_->is_rowkey_column(rowkey_cid))
      {
        if (OB_SUCCESS != (ret = new_row.get_cell(table_id, rowkey_cid, cell)))
        {
          YYSYS_LOG(WARN, "get_cell fail, err=%d",ret);
          break;
        }
        else if(OB_SUCCESS != (ret = col_val.set_value_and_type(*cell)))
        {
          YYSYS_LOG(WARN, "fail to set column value,err=%d",ret);
          break;
        }
        else if(OB_SUCCESS != (ret = col_val.fill_sql_expression(*rows_filter)))
        {
          YYSYS_LOG(WARN, "Failed to add expr item, err=%d",ret);
          break;
        }
        else
        {
          ObObjType cond_val_type;
          uint32_t cond_val_precision;
          uint32_t cond_val_scale;
          if (OB_SUCCESS != sql_context_.schema_manager_->get_cond_val_info(table_id, rowkey_cid, cond_val_type,cond_val_precision, cond_val_scale))
          {
            YYSYS_LOG(WARN, "Failed to get cond val info");
          }
          else if(ObDecimalType == cond_val_type)
          {
            ObPostfixExpression &ops = rows_filter->get_decoded_expression_v2();
            ObObj &obj = ops.get_expr();
            if (ObDecimalType == obj.get_type())
            {
              ops.fix_varchar_and_decimal(cond_val_precision, cond_val_scale);
            }
            else if(ObVarcharType == obj.get_type())
            {
              ops.fix_varchar_and_decimal(cond_val_precision, cond_val_scale);
            }
          }
        }
      }
    }
    if (OB_LIKELY(ret == OB_SUCCESS))
    {
      if (rowkey_column_num > 0)
      {
        expr_item.type_ = T_OP_ROW;
        expr_item.value_.int_ = rowkey_column_num;
        if (OB_SUCCESS != (ret = rows_filter->add_expr_item(expr_item)))
        {
          YYSYS_LOG(WARN, "Failed to add expr item, err=%d",ret);
        }
      }
    }
    for (int64_t i=0; ret == OB_SUCCESS && i < row_desc.get_column_num();i++)
    {
      uint64_t tid = OB_INVALID_ID;
      uint64_t cid = OB_INVALID_ID;
      const ObObj *cell = NULL;
      ObConstRawExpr col_expr;
      if (OB_SUCCESS != (ret = new_row.raw_get_cell(i, cell, tid, cid)))
      {
        YYSYS_LOG(WARN, "get_cell fail, err=%d",ret);
        break;
      }
      else if (OB_SUCCESS !=(ret = col_expr.set_value_and_type(*cell)))
      {
        YYSYS_LOG(WARN, "fail to set column expr,err=%d",ret);
        break;
      }
      else
      {
        const ObColumnSchemaV2 *column_schema = NULL;
        if (NULL == sql_context_.schema_manager_)
        {
          ret = OB_ERR_UNEXPECTED;
          YYSYS_LOG(WARN, "schema manager is null,%d",ret);
          break;
        }
        else if(NULL == (column_schema = sql_context_.schema_manager_->get_column_schema(tid,cid)))
        {
          ret = OB_ERR_UNEXPECTED;
          YYSYS_LOG(WARN, "column schema is null,%d",ret);
          break;
        }
        else if (OB_SUCCESS == ret && ObVarcharType == column_schema->get_type() && ObNullType != cell->get_type())
        {
          if(cell->get_val_len() > column_schema->get_size())
          {
            const ObRowDesc *row_desc = NULL;
            row_desc = new_row.get_row_desc();
            uint64_t tid = OB_INVALID_ID;
            uint64_t cid = OB_INVALID_ID;
            row_desc->get_tid_cid(i, tid, cid);
            YYSYS_LOG(INFO, "Varchar is too long, tid[%ld], cid[%ld], length in schema[%ld], length in varchar[%d]",
                      tid, cid, column_schema->get_size(), cell->get_val_len());
            ret = OB_ERR_VARCHAR_TOO_LONG;
            const ObTableSchema *table_schema = NULL;;
            if(NULL == (table_schema = (sql_context_.schema_manager_->get_table_schema(tid))))
            {
              YYSYS_LOG(USER_ERROR, "table_schema is null");
              ret = OB_ERR_UNEXPECTED;
            }
            else
            {
              YYSYS_LOG(USER_ERROR, "Varchar is too long, tname[%s], cname[%s], clength[%d]", table_schema->get_table_name(), column_schema->get_name(), cell->get_val_len());
            }
            break;
          }
        }
        ObSqlRawExpr col_raw_expr(
              common::OB_INVALID_ID,
              tid,
              cid,
              &col_expr);
        ObSqlExpression output_expr;
        if((ret = col_raw_expr.fill_sql_expression(output_expr)) != OB_SUCCESS)
        {
          YYSYS_LOG(WARN, "Add table output columns failed");
          break;
        }
        else if(OB_SUCCESS != (ret = ob_expr_values_->add_value(output_expr)))
        {
          YYSYS_LOG(WARN, "Failed to push_back expr, err=%d", ret);
          break;
        }
      }
    }
  }
  //------------------
  if (OB_LIKELY(OB_SUCCESS == ret))
  {
    expr_item.type_ = T_OP_ROW;
    expr_item.value_.int_ = old_rows.count();
    ExprItem expr_in;
    expr_in.type_ = T_OP_IN;
    expr_in.value_.int_ = 2;
    if (OB_SUCCESS != (ret = rows_filter->add_expr_item(expr_item)))
    {
      YYSYS_LOG(WARN, "Failed to add expr item, err=%d",ret);
    }
    else if(OB_SUCCESS != (ret = rows_filter->add_expr_item(expr_in)))
    {
      YYSYS_LOG(WARN, "Failed to add expr item, err=%d",ret);
    }
    else if(OB_SUCCESS != (ret = rows_filter->add_expr_item_end()))
    {
      YYSYS_LOG(WARN, "Failed to add expr item end, err=%d",ret);
    }
    else if (OB_SUCCESS != (ret = ob_sql_read_strategy_.add_filter(*rows_filter)))
    {
      YYSYS_LOG(WARN, "Failed to add filter, err=%d",ret);
    }
    else if(OB_SUCCESS != (ret = ob_table_rpc_scan_->add_filter(rows_filter)))
    {
      YYSYS_LOG(WARN, "Failed to add filter, err=%d",ret);
    }
  }
  return ret;
}

bool ObUpdateRowkey::is_batch_over(ObFillValues *&fill_values, ObBindValues *&bind_values)
{
  bool is_over = false;
  if(NULL != fill_values)
  {
    is_over = fill_values->is_multi_batch_over();
  }
  else if (NULL != bind_values)
  {
    is_over = bind_values->is_batch_over();
  }
  return is_over;
}

namespace oceanbase
{
  namespace sql
  {
    REGISTER_PHY_OPERATOR(ObUpdateRowkey, PHY_UPDATE_ROWKEY);
  }
}

int64_t ObUpdateRowkey::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ObUpdateRowkey(phy_plan=\n");
  if (NULL != ob_ups_executor_select_)
  {
    databuff_printf(buf, buf_len, pos, "select row plan(===\n");
    pos += ob_ups_executor_select_->to_string(buf + pos, buf_len - pos);
    databuff_printf(buf, buf_len, pos, "end select===)\n");
  }
  if (NULL != ob_select_more_rows_rpc_)
  {
    databuff_printf(buf, buf_len, pos, "select row plan(===\n");
    pos += ob_select_more_rows_rpc_->to_string(buf + pos, buf_len - pos);
    databuff_printf(buf, buf_len, pos, "end select===)\n");
  }
  if (NULL != ob_ups_executor_delete_)
  {
    databuff_printf(buf, buf_len, pos, "delete plan(===\n");
    pos += ob_ups_executor_delete_->to_string(buf + pos, buf_len - pos);
    databuff_printf(buf, buf_len, pos, "end delete===)\n");
  }
  if (NULL != ob_ups_executor_insert_)
  {
    databuff_printf(buf, buf_len, pos, "insert plan(===\n");
    pos += ob_ups_executor_insert_->to_string(buf + pos, buf_len - pos);
    databuff_printf(buf, buf_len, pos, "end insert===)\n");
  }
  return pos;
}

PHY_OPERATOR_ASSIGN(ObUpdateRowkey)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObUpdateRowkey);
  reset();
  if (!my_phy_plan_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(WARN, "ObPhysicalPlan/allocator is not set, ret=%d",ret);
  }
  else
  {
    if (o_ptr->ob_ups_executor_select_ && !ob_ups_executor_select_)
    {
      CREATE_PHY_OPERRATOR_NEW(ob_ups_executor_select_, ObUpsExecutor, my_phy_plan_, ret);
      if (OB_SUCCESS == ret)
      {
        ob_ups_executor_select_->set_phy_plan(my_phy_plan_);
      }
      if(OB_SUCCESS == ret
         && (ret = ob_ups_executor_select_->assign(o_ptr->ob_ups_executor_select_)) != OB_SUCCESS)
      {
        YYSYS_LOG(WARN, "Assign ob_ups_executor_select_ failed. ret=%d",ret);
      }
    }
    if (o_ptr->ob_table_rpc_scan_ && !ob_table_rpc_scan_)
    {
      CREATE_PHY_OPERRATOR_NEW(ob_table_rpc_scan_, ObTableRpcScan, my_phy_plan_, ret);
      if (OB_SUCCESS == ret)
      {
        ob_table_rpc_scan_->set_phy_plan(my_phy_plan_);
      }
      if(OB_SUCCESS == ret
         && (ret = ob_table_rpc_scan_->assign(o_ptr->ob_table_rpc_scan_)) != OB_SUCCESS)
      {
        YYSYS_LOG(WARN, "Assign ob_table_rpc_scan_ failed. ret=%d",ret);
      }
    }
    if (o_ptr->ob_ups_executor_delete_ && !ob_ups_executor_delete_)
    {
      CREATE_PHY_OPERRATOR_NEW(ob_ups_executor_delete_, ObUpsExecutor, my_phy_plan_, ret);
      if (OB_SUCCESS == ret)
      {
        ob_ups_executor_delete_->set_phy_plan(my_phy_plan_);
      }
      if(OB_SUCCESS == ret
         && (ret = ob_ups_executor_delete_->assign(o_ptr->ob_ups_executor_delete_)) != OB_SUCCESS)
      {
        YYSYS_LOG(WARN, "Assign ob_ups_executor_delete_ failed. ret=%d",ret);
      }
    }
    if (o_ptr->ob_ups_executor_insert_ && !ob_ups_executor_insert_)
    {
      CREATE_PHY_OPERRATOR_NEW(ob_ups_executor_insert_, ObUpsExecutor, my_phy_plan_, ret);
      if (OB_SUCCESS == ret)
      {
        ob_ups_executor_insert_->set_phy_plan(my_phy_plan_);
      }
      if(OB_SUCCESS == ret
         && (ret = ob_ups_executor_insert_->assign(o_ptr->ob_ups_executor_insert_)) != OB_SUCCESS)
      {
        YYSYS_LOG(WARN, "Assign ob_ups_executor_insert_ failed. ret=%d",ret);
      }
    }
    for(int64_t i=0; i < ob_sql_expressions_.count(); ++i)
    {
      if (OB_SUCCESS != (ret = ob_sql_expressions_.push_back(o_ptr->ob_sql_expressions_.at(i))))
      {
        YYSYS_LOG(WARN, "assign ob_sql_expressions_ failed!");
      }
      else if (OB_SUCCESS != (ret = ob_value_types_.push_back(o_ptr->ob_value_types_.at(i))))
      {
        YYSYS_LOG(WARN, "assign ob_value_types_ failed!");
      }
    }

    is_multi_update_ = o_ptr->is_multi_update_;
    row_desc_ = o_ptr->row_desc_;
    row_desc_ext_ = o_ptr->row_desc_ext_;
    rowkey_info_ = o_ptr->rowkey_info_;
    ob_expr_values_ = o_ptr->ob_expr_values_;
    ob_table_rpc_scan_ = o_ptr->ob_table_rpc_scan_;
    ob_sql_read_strategy_ = o_ptr->ob_sql_read_strategy_;
  }
  return ret;
}
