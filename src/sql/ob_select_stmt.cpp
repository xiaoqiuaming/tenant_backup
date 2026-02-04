#include "ob_select_stmt.h"
#include "parse_malloc.h"
#include "ob_logical_plan.h"
#include "sql_parser.tab.h"
#include "ob_raw_expr.h"
#include "ob_schema_checker.h"
#include "common/utility.h"
#include "ob_optimizer_relation.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

ObSelectStmt::ObSelectStmt(ObStringBuf* name_pool)
: ObStmt(name_pool, ObStmt::T_SELECT)
{
  //if (m_columnMap.create(MAX_MAP_BUCKET_NUM) == -1)
  //  throw new ParseException(name_pool_, "Create m_columnMap error!");
  left_query_id_ = OB_INVALID_ID;
  right_query_id_ = OB_INVALID_ID;
  limit_count_id_  = OB_INVALID_ID;
  limit_offset_id_ = OB_INVALID_ID;
  for_update_ = false;
  is_max_min_eliminate_ = false;
  gen_joined_tid_ = UINT64_MAX - 2;
  //add tianz [EXPORT_TOOL] 20141120:b
  has_range_ = false;
  start_is_min_ = false;
  end_is_max_ = false;
  //add 20141120:e
  //add liuzy [sequence select]20150703:b
  select_clause_has_sequence_ = false;
  column_has_sequence_count_ = 0;
  //add 20150703:e
  //delete by xionghui [subquery_final] 20160216 :b
  /*
  //add zhujun [fix equal-subquery bug] 20151013:b
  is_equal_subquery_= false;
  //add 20151013:e
  //add xionghui [fix like-subquery bug] 20151015:b
  is_like_subquery_ = false;
  //add 20151015:e
  */
  //delete e:
  is_partition_calc_func_ = false;
  partition_calc_table_id_ = OB_INVALID_ID;
  partition_cal_version_flag_ = OB_INVALID_ID;
  if(!table_id_statInfo_map_.created())
  {
      table_id_statInfo_map_.create(STAT_INFO_TABLE_MAP_SIZE);
  }
  select_stmt_rel_info_ = NULL;

}

ObSelectStmt::~ObSelectStmt()
{
  // m_columnMap.destroy();
  for (int32_t i = 0; i < joined_tables_.size(); i++)
  {
    //ob_free(reinterpret_cast<char *>(joined_tables_[i]));
    joined_tables_[i]->~JoinedTable();
    parse_free(joined_tables_[i]);
  }

  if(select_stmt_rel_info_ != NULL)
  {
      select_stmt_rel_info_->~ObOptimizerRelation();
      select_stmt_rel_info_ = NULL;
  }
  select_items_.clear();
  select_items_.clear();
  joined_tables_.clear();
  group_expr_ids_.clear();
  group_expr_indexed_flags_.clear();//add liumz, [optimize group_order by index]20170419
  order_items_.clear();
  order_items_indexed_flags_.clear();//add liumz, [optimize group_order by index]20170419
  //add liumz, [ROW_NUMBER]20150824
  partition_expr_ids_.clear();
  order_items_for_rownum_.clear();
  //add:e
  //add tianz [EXPORT_TOOL] 20141120:b
  for (int64_t i = 0; i < range_vectors_.count(); i++)
  {
    ObArray<uint64_t>& value_row = range_vectors_.at(i);
    value_row.clear();
  }
  //add 20141120

  from_item_method_list_.destroy();
  from_item_appear_order_list_.destroy();
  oceanbase::common::ObList<ObOptimizerRelation*>::iterator unaryRef_expr_rel_info_it = unaryRef_expr_rel_info_list_.begin();
  for(; unaryRef_expr_rel_info_it != unaryRef_expr_rel_info_list_.end(); unaryRef_expr_rel_info_it++)
  {
      (*unaryRef_expr_rel_info_it)->~ObOptimizerRelation();
  }

  unaryRef_expr_rel_info_list_.destroy();
  oceanbase::common::ObList<ObOptimizerRelation*>::iterator joined_from_item_rel_info_it = joined_from_item_rel_info_list_.begin();
  for(; joined_from_item_rel_info_it != joined_from_item_rel_info_list_.end(); joined_from_item_rel_info_it++)
  {
      (*joined_from_item_rel_info_it)->~ObOptimizerRelation();
  }

  joined_from_item_rel_info_list_.destroy();

  oceanbase::common::ObList<ObOptimizerRelation*>::iterator sub_rel_opt_it = subquery_rel_opt_list_.begin();
  for(; sub_rel_opt_it != subquery_rel_opt_list_.end(); sub_rel_opt_it++)
  {
      (*sub_rel_opt_it)->~ObOptimizerRelation();
  }
  subquery_rel_opt_list_.destroy();

  oceanbase::common::ObList<ObOptimizerRelation*>::iterator rel_opt_it = rel_opt_list.begin();
  for(; rel_opt_it != rel_opt_list.end(); rel_opt_it++)
  {
      (*rel_opt_it)->~ObOptimizerRelation();
  }
  rel_opt_list.destroy();

  common::hash::ObHashMap<uint64_t,ObBaseRelStatInfo*, common::hash::NoPthreadDefendMode>::const_iterator table_iter =
                                                                      table_id_statInfo_map_.begin();
  ObBaseRelStatInfo* rel_stat_info = NULL;
  ObColumnStatInfo* col_stat_info = NULL;
  for(; table_iter != table_id_statInfo_map_.end();table_iter++)
  {
      rel_stat_info = (ObBaseRelStatInfo*)(table_iter->second);
      if(rel_stat_info != NULL)
      {
          common::hash::ObHashMap<uint64_t,ObColumnStatInfo*, common::hash::NoPthreadDefendMode>::const_iterator column_iter =
                                                                              rel_stat_info->column_id_value_map_.begin();
          for(;column_iter != rel_stat_info->column_id_value_map_.end();column_iter++)
          {
              col_stat_info = (ObColumnStatInfo*)(column_iter->second);
              if(col_stat_info != NULL)
              {
                  col_stat_info->value_frequency_map_.destroy();
                  col_stat_info = NULL;
              }
          }
          rel_stat_info->column_id_value_map_.destroy();
          rel_stat_info = NULL;
      }
  }

  table_id_statInfo_map_.destroy();
  for(int32_t i = 0; i < joined_tables_info_.size(); i++)
  {
      joined_tables_info_[i]->~JoinedTableInfo();
      parse_free(joined_tables_info_[i]);
  }
  joined_tables_info_.clear();

}

int ObSelectStmt::check_alias_name(
    ResultPlan& result_plan,
    const ObString& alias_name) const
{
  int& ret = result_plan.err_stat_.err_code_ = OB_SUCCESS;
  ObLogicalPlan *logical_plan = static_cast<ObLogicalPlan*>(result_plan.plan_tree_);
  ObSchemaChecker *schema_checker = static_cast<ObSchemaChecker*>(result_plan.schema_checker_);
  if (schema_checker == NULL)
  {
    ret = OB_ERR_SCHEMA_UNSET;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Schema(s) are not set");
  }

  for (int32_t i = 0; ret == OB_SUCCESS && i < table_items_.size(); i++)
  {
    /* check if it is column of base-table */
    TableItem& item = table_items_[i];
    if (item.type_ == TableItem::BASE_TABLE
      || item.type_ == TableItem::ALIAS_TABLE)
    {
      if (schema_checker->column_exists(item.table_name_, alias_name))
      {
        ret = OB_ERR_COLUMN_DUPLICATE;
        snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
            "alias name %.*s is ambiguous", alias_name.length(), alias_name.ptr());
        break;
      }
    }
    else if (item.type_ == TableItem::GENERATED_TABLE)
    {
      /* check if it is column of generated-table */
      ObSelectStmt* sub_query = static_cast<ObSelectStmt*>(logical_plan->get_query(item.ref_id_));
      for (int32_t j = 0; ret == OB_SUCCESS && j < sub_query->get_select_item_size(); j++)
      {
        const SelectItem& select_item = sub_query->get_select_item(j);
        if (select_item.alias_name_ == alias_name)
        {
          ret = OB_ERR_COLUMN_DUPLICATE;
          snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
              "alias name %.*s is ambiguous", alias_name.length(), alias_name.ptr());
          break;
        }
      }
    }
  }

  /* check if it is alias name of self-select */
  for (int32_t i = 0; ret == OB_SUCCESS && i < select_items_.size(); i++)
  {
    const SelectItem& select_item = get_select_item(i);
    if (select_item.alias_name_ == alias_name)
    {
      ret = OB_ERR_COLUMN_DUPLICATE;
      snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
          "alias name %.*s is ambiguous", alias_name.length(), alias_name.ptr());
      break;
    }
  }

  return ret;
}

int ObSelectStmt::add_select_item(
    uint64_t eid,
    bool is_real_alias,
    const ObString& alias_name,
    const ObString& expr_name,
    const ObObjType& type)
{
  int ret = OB_SUCCESS;
  if (eid != OB_INVALID_ID)
  {
    SelectItem item;
    item.expr_id_ = eid;
    item.is_real_alias_ = is_real_alias;
    ret = ob_write_string(*name_pool_, alias_name, item.alias_name_);
    if (ret == OB_SUCCESS)
      ret = ob_write_string(*name_pool_, expr_name, item.expr_name_);
    if (ret == OB_SUCCESS)
    {
      item.type_ = type;
      ret = select_items_.push_back(item);
      ret = output_columns_dsttype_.push_back(type);  //add peiouya [IN_TYPEBUG_FIX] 20151225
    }
  }
  else
  {
    ret = OB_ERR_ILLEGAL_ID;
  }
  return ret;
}

int ObSelectStmt::add_raw_column(
    const uint64_t &expr_id,
    const common::ObString &column_name)
{
  int ret = OB_SUCCESS;
  std::pair<ObString, uint64_t> raw_column_type;
  if (OB_SUCCESS != (ret = ob_write_string(*name_pool_, column_name, raw_column_type.first)))
  {
    YYSYS_LOG(WARN, "write string failed, ret=%d", ret);
  }
  else
  {
    raw_column_type.second = expr_id;
    ret = raw_column_names_.push_back(raw_column_type);
  }
  return ret;
}

const common::ObString *ObSelectStmt::get_raw_column(const uint64_t expr_id) const
{
  for (int32_t i = 0; i < raw_column_names_.count(); i++)
  {
    const std::pair<ObString, uint64_t> &column = raw_column_names_.at(i);
    if(expr_id == column.second)
    {
      return (&column.first);
    }
  }
  return NULL;
}
const SelectItem *ObSelectStmt::get_select_item(uint64_t expr_id) const
{
  const SelectItem *select_item = NULL;
  for(int32_t i = 0; i < select_items_.size(); i++)
  {
    const SelectItem &item = select_items_[i];
    if (expr_id == item.expr_id_)
    {
      select_item = &item;
      break;
    }
  }
  return select_item;
}
// return the first expr with name alias_name
uint64_t ObSelectStmt::get_alias_expr_id(oceanbase::common::ObString& alias_name)
{
  uint64_t expr_id = OB_INVALID_ID;
  for (int32_t i = 0; i < select_items_.size(); i++)
  {
    SelectItem& item = select_items_[i];
    if (alias_name == item.alias_name_)
    {
      expr_id = item.expr_id_;
      break;
    }
  }
  return expr_id;
}

int ObSelectStmt::remove_joined_table(uint64_t table_id)
{
    int ret = OB_SUCCESS;
    int32_t num = get_joined_table_size();
    int32_t i = 0;
    for(; i<num;i++)
    {
        if(joined_tables_[i]->joined_table_id_ == table_id)
        {
            joined_tables_[i]->~JoinedTable();
            ret = joined_tables_.remove(i);
            break;
        }
    }
    if(i == num)
    {
        YYSYS_LOG(WARN,"remove joined table error,table_id = %ld", table_id);
        ret = OB_ERROR;
    }
    return ret;
}

int64_t JoinedTable::get_expr_ids_index(int64_t index)
{
    int64_t total=0;
    for(int64_t i =0;i<index;++i)
    {
        total += expr_nums_per_join_.at(i);
    }
    return total;
}

JoinedTable* ObSelectStmt::get_joined_table(uint64_t table_id)
{
  JoinedTable *joined_table = NULL;
  int32_t num = get_joined_table_size();
  for (int32_t i = 0; i < num; i++)
  {
    if (joined_tables_[i]->joined_table_id_ == table_id)
    {
      joined_table = joined_tables_[i];
      break;
    }
  }
  return joined_table;
}

int ObSelectStmt::check_having_ident(
  ResultPlan& result_plan,
  ObString& column_name,
  TableItem* table_item,
  ObRawExpr*& ret_expr) const
{
  ObSqlRawExpr  *sql_expr;
  ObRawExpr     *expr;
  ret_expr = NULL;
  int& ret = result_plan.err_stat_.err_code_ = OB_SUCCESS;
  ObLogicalPlan* logical_plan = static_cast<ObLogicalPlan*>(result_plan.plan_tree_);
  if (logical_plan == NULL)
  {
    ret = OB_ERR_LOGICAL_PLAN_FAILD;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
              "Wrong invocation of YaoStmt::add_table_item, logical_plan must exist!!!");
  }

  ObSchemaChecker* schema_checker = NULL;
  if (ret == OB_SUCCESS)
  {
    schema_checker = static_cast<ObSchemaChecker*>(result_plan.schema_checker_);
    if (schema_checker == NULL)
    {
      ret = OB_ERR_SCHEMA_UNSET;
      snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
              "Schema(s) are not set");
    }
  }

  for (int32_t i = 0; ret == OB_SUCCESS && i < select_items_.size(); i++)
  {
    const SelectItem& select_item = get_select_item(i);
    const ObString *expr_name = get_raw_column(select_item.expr_id_);
    char column[OB_MAX_COLUMN_NAME_LENGTH] = {0};
    int64_t column_idx = 0;
    bool use_raw_column = false;
    if(expr_name != NULL)
    {
      const int64_t len = OB_MAX_COMPLETE_TABLE_NAME_LENGTH + OB_MAX_COLUMN_NAME_LENGTH + 9;
      char complete_name[len] = {0};
      snprintf(complete_name, len, "%.*s", expr_name->length(), expr_name->ptr());
      int64_t pos = 0;
      for(int64_t i = expr_name->length() - 1; i >= 0; i--)
      {
        if(complete_name[i] == '.')
        {
          pos = i + 1;
          break;
        }
      }
      for(int64_t i = pos; i < expr_name->length() && column_idx < OB_MAX_COLUMN_NAME_LENGTH;)
      {
        if (complete_name[i] == '\"' || complete_name[i] == '\'')
        {
          i++;
        }
        else
        {
          column[column_idx++] = complete_name[i];
          i++;
        }
      }
      if(column_idx < OB_MAX_COLUMN_NAME_LENGTH)
      {
        column[column_idx] = '\0';
        use_raw_column = true;
        YYSYS_LOG(INFO, "column=[%s]", column);
      }
    }
    // for single column expression, we already set it as alias name
    if (column_name == select_item.alias_name_ || (use_raw_column && column_name.compare(column) == 0))
    {
      sql_expr = logical_plan->get_expr(select_item.expr_id_);
      expr = sql_expr->get_expr();
      if (table_item)
      {
        if (expr->get_expr_type() == T_REF_COLUMN)
        {
          ObBinaryRefRawExpr* col_expr = dynamic_cast<ObBinaryRefRawExpr *>(expr);
          if (col_expr && col_expr->get_first_ref_id() == table_item->table_id_)
          {
            ColumnItem* column_item = get_column_item_by_id(col_expr->get_first_ref_id(), col_expr->get_second_ref_id());
            if (column_item && column_item->column_name_ == column_name)
            {
              ObBinaryRefRawExpr *b_expr = (ObBinaryRefRawExpr*)parse_malloc(sizeof(ObBinaryRefRawExpr), name_pool_);
              b_expr = new(b_expr) ObBinaryRefRawExpr();
              b_expr->set_expr_type(T_REF_COLUMN);
              b_expr->set_first_ref_id(col_expr->get_first_ref_id());
              b_expr->set_second_ref_id(col_expr->get_second_ref_id());
              ret_expr = b_expr;
              break;
            }
          }
        }
      }
      else
      {
        if (ret_expr)
        {
          ret = OB_ERR_COLUMN_AMBIGOUS;
          snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
              "column %.*s of having clause is ambiguous", column_name.length(), column_name.ptr());
          parse_free(ret_expr);
          ret_expr = NULL;
          break;
        }
        // for having clause: having cc > 0
        // type 1: select t1.cc
        if (expr->get_expr_type() == T_REF_COLUMN && !select_item.is_real_alias_)
        {
          ObBinaryRefRawExpr *col_expr = dynamic_cast<ObBinaryRefRawExpr *>(expr);
          ObBinaryRefRawExpr *b_expr = (ObBinaryRefRawExpr*)parse_malloc(sizeof(ObBinaryRefRawExpr), name_pool_);
          b_expr = new(b_expr) ObBinaryRefRawExpr();
          b_expr->set_expr_type(T_REF_COLUMN);
          b_expr->set_first_ref_id(col_expr->get_first_ref_id());
          b_expr->set_second_ref_id(col_expr->get_second_ref_id());
          ret_expr = b_expr;
        }
        // type 2: select t1.cc as cc
        // type 3: select t1.c1 as cc
        // type 4: select t1.c1 + t2.c1 as cc
        else
        {
          ObBinaryRefRawExpr *b_expr = (ObBinaryRefRawExpr*)parse_malloc(sizeof(ObBinaryRefRawExpr), name_pool_);
          b_expr = new(b_expr) ObBinaryRefRawExpr();
          b_expr->set_expr_type(T_REF_COLUMN);
          b_expr->set_first_ref_id(OB_INVALID_ID);
          b_expr->set_second_ref_id(sql_expr->get_column_id());
          ret_expr = b_expr;
        }
      }
    }
  }

  // No non-duplicated ident found
  if (ret == OB_SUCCESS && ret_expr == NULL)
  {
    for (int32_t i = 0; ret == OB_SUCCESS && i < group_expr_ids_.size(); i++)
    {
      sql_expr = logical_plan->get_expr(group_expr_ids_[i]);
      expr = sql_expr->get_expr();
      //ObRawExpr* expr = logical_plan->get_expr(group_expr_ids_[i])->get_expr();
      if (expr->get_expr_type() != T_REF_COLUMN)
        continue;

      ObBinaryRefRawExpr* col_expr = dynamic_cast<ObBinaryRefRawExpr *>(expr);
      // Only need to check original columns, alias columns are already checked before
      if (table_item == NULL || table_item->table_id_ == col_expr->get_first_ref_id())
      {
        ColumnItem* column_item = get_column_item_by_id(
                                      col_expr->get_first_ref_id(),
                                      col_expr->get_second_ref_id());
        if (column_item && column_name == column_item->column_name_)
        {
          ObBinaryRefRawExpr *b_expr = (ObBinaryRefRawExpr*)parse_malloc(sizeof(ObBinaryRefRawExpr), name_pool_);
          b_expr = new(b_expr) ObBinaryRefRawExpr();
          b_expr->set_expr_type(T_REF_COLUMN);
          b_expr->set_first_ref_id(column_item->table_id_);
          b_expr->set_second_ref_id(column_item->column_id_);
          ret_expr = b_expr;
          break;
        }
      }
    }
  }

  if (ret == OB_SUCCESS && ret_expr == NULL)
  {
    ret = OB_ERR_COLUMN_UNKNOWN;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Unknown %.*s in having clause", column_name.length(), column_name.ptr());
  }
  return ret;
}

int ObSelectStmt::copy_select_items(ObSelectStmt* select_stmt)
{
  int ret = OB_SUCCESS;
  int32_t num = select_stmt->get_select_item_size();
  SelectItem new_select_item;
  for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
  {
    const SelectItem& select_item = select_stmt->get_select_item(i);
    new_select_item.expr_id_ = select_item.expr_id_;
    new_select_item.type_ = select_item.type_;
    ret = ob_write_string(*name_pool_, select_item.alias_name_, new_select_item.alias_name_);
    if (ret == OB_SUCCESS)
      ret = ob_write_string(*name_pool_, select_item.expr_name_, new_select_item.expr_name_);
    if (ret == OB_SUCCESS)
      ret = select_items_.push_back(new_select_item);
  }
  return ret;
}

//add peiouya [IN_TYPEBUG_FIX] 20151225:b
int ObSelectStmt::add_dsttype_for_output_columns(common::ObArray<common::ObObjType>& columns_dsttype)
{
   output_columns_dsttype_ = columns_dsttype;
   return OB_SUCCESS;
}
//add 20151225:e
//add qianzm [set_operation] 20160115:b
int ObSelectStmt::add_result_type_array_for_setop(common::ObArray<common::ObObjType>& result_columns_type)
{
   result_type_array_for_setop_ = result_columns_type;
   return OB_SUCCESS;
}
//add 20160115:e
//add qianzm [set_operation] 20151222 :b
int ObSelectStmt::copy_select_items_v2(ObSelectStmt* select_stmt, ObLogicalPlan* logical_plan)
{
  int ret = OB_SUCCESS;
  int32_t num = select_stmt->get_select_item_size();
  SelectItem new_select_item;
  ObSqlRawExpr* sql_expr = NULL;
  for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
  {
    const SelectItem& select_item = select_stmt->get_select_item(i);
    new_select_item.expr_id_ = select_item.expr_id_;
    sql_expr = logical_plan->get_expr(select_item.expr_id_);
    new_select_item.type_ = sql_expr->get_result_type();
    ret = ob_write_string(*name_pool_, select_item.alias_name_, new_select_item.alias_name_);
    if (ret == OB_SUCCESS)
      ret = ob_write_string(*name_pool_, select_item.expr_name_, new_select_item.expr_name_);
    if (ret == OB_SUCCESS)
      ret = select_items_.push_back(new_select_item);
  }
  return ret;
}
//add e
int JoinedTable::add_join_exprs(ObVector<uint64_t>& exprs)
{
  int ret = OB_SUCCESS;
  int64_t expr_size = exprs.size();
  if (expr_size <= 0)
  {
    ret = OB_INVALID_ARGUMENT;
  }
  else if ((ret = expr_nums_per_join_.push_back(exprs.size())) != OB_SUCCESS)
  {
    YYSYS_LOG(WARN,"add exprs size failed,ret=%d",ret);
  }
  else
  {
    for(int32_t i=0; (i < expr_size) && (OB_SUCCESS == ret); ++i)
    {
      ret = add_expr_id(exprs.at(i));
    }
  }
  return ret;
}

void ObSelectStmt::print_json(char *buf, const int64_t buf_len, int64_t &pos, int32_t level)
{
    print_indentation_json(buf, buf_len, pos, level);
    databuff_printf(buf, buf_len, pos, "{\n");

    print_indentation_json(buf, buf_len, pos, level +1);
    databuff_printf(buf, buf_len, pos, "\"QueryId\":%ld,\n", get_query_id());
    ObStmt::print_json(buf, buf_len, pos, level +1);
    print_indentation_json(buf, buf_len, pos, level);
    databuff_printf(buf, buf_len, pos, "}");
}

void ObSelectStmt::print(FILE* fp, int32_t level, int32_t index)
{
  int32_t i;

  print_indentation(fp, level);
  fprintf(fp, "ObSelectStmt %d Begin\n", index);
  ObStmt::print(fp, level);

  if (set_op_ == NONE)
  {
    print_indentation(fp, level);
    if (is_distinct_)
      fprintf(fp, "SELECT ::= DISTINCT ");
    else
      fprintf(fp, "SELECT ::= ");
    for (i = 0; i < select_items_.size(); i++)
    {
      if (i > 0)
        fprintf(fp, ", ");
      SelectItem& item = select_items_[i];
      if (item.alias_name_.length() > 0)
        fprintf(fp, "<%lu, %.*s>", item.expr_id_,
          item.alias_name_.length(), item.alias_name_.ptr());
      else
        fprintf(fp, "<%ld>", item.expr_id_);
    }
    fprintf(fp, "\n");

    print_indentation(fp, level);
    fprintf(fp, "FROM ::= ");
    for (i = 0; i < from_items_.size(); i++)
    {
      if (i > 0)
        fprintf(fp, ", ");
      FromItem& item = from_items_[i];
      if (item.is_joined_)
      {
        JoinedTable* joined_table = get_joined_table(item.table_id_);
        for (int32_t j = 1; j < joined_table->table_ids_.count(); j++)
        {
          if (j == 1)
            fprintf(fp, "<%lu> ", joined_table->table_ids_.at(j - 1));

          switch (joined_table->join_types_.at(j - 1))
          {
            case JoinedTable::T_FULL:
              fprintf(fp, "FULL JOIN ");
              break;
            case JoinedTable::T_LEFT:
              fprintf(fp, "LEFT JOIN ");
              break;
            case JoinedTable::T_RIGHT:
              fprintf(fp, "RIGHT JOIN ");
              break;
            case JoinedTable::T_INNER:
              fprintf(fp, "INNER JOIN ");
              break;
            default:
              break;
          }
          fprintf(fp, "<%lu> ", joined_table->table_ids_.at(j));
          fprintf(fp, "ON <%lu>", joined_table->expr_ids_.at(j - 1));
        }
      }
      else
      {
        fprintf(fp, "<%lu>", item.table_id_);
      }
    }
    fprintf(fp, "\n");

    if (group_expr_ids_.size() > 0)
    {
      print_indentation(fp, level);
      fprintf(fp, "GROUP BY ::= ");
      for (i = 0; i < group_expr_ids_.size(); i++)
      {
        if (i > 0)
          fprintf(fp, ", ");
        fprintf(fp, "<%lu>", group_expr_ids_[i]);
      }
      fprintf(fp, "\n");
    }

    if (having_expr_ids_.size() > 0)
    {
      print_indentation(fp, level);
      fprintf(fp, "HAVING ::= ");
      for (i = 0; i < having_expr_ids_.size(); i++)
      {
        if (i > 0)
          fprintf(fp, ", ");
        fprintf(fp, "<%lu>", having_expr_ids_[i]);
      }
      fprintf(fp, "\n");
    }

  }
  else
  {
    print_indentation(fp, level);
    fprintf(fp, "LEFTQUERY ::= <%lu>\n", left_query_id_);
    print_indentation(fp, level);

    switch(set_op_)
    {
      case UNION:
        fprintf(fp, "<UNION ");
        break;
      case INTERSECT:
        fprintf(fp, "<INTERSECT ");
        break;
      case EXCEPT:
        fprintf(fp, "<EXCEPT ");
        break;
      default:
        break;
    }

    if (is_set_distinct_)
      fprintf(fp, "DISTINCT>\n");
    else
      fprintf(fp, "ALL>\n");

    print_indentation(fp, level);
    fprintf(fp, "RIGHTQUERY ::= <%lu>\n", right_query_id_);
  }

  for (i = 0; i < order_items_.size(); i++)
  {
    if (i == 0)
    {
      print_indentation(fp, level);
      fprintf(fp, "ORDER BY ::= ");
    }
    else
      fprintf(fp, ", ");
    OrderItem& item = order_items_[i];
    fprintf(fp, "<%lu, %s>", item.expr_id_,
      item.order_type_ == OrderItem::ASC ? "ASC" : "DESC");
    if (i == order_items_.size() - 1)
      fprintf(fp, "\n");
  }

  if (has_limit())
  {
    print_indentation(fp, level);
    fprintf(fp, "LIMIT ::= <");
    if (limit_count_id_ == OB_INVALID_ID)
      fprintf(fp, "NULL, ");
    else
      fprintf(fp, "%lu, ", limit_count_id_);
    if (limit_offset_id_ == OB_INVALID_ID)
      fprintf(fp, "NULL>\n");
    else
      fprintf(fp, "%lu>\n", limit_offset_id_);
  }

  print_indentation(fp, level);
  fprintf(fp, "ObSelectStmt %d End\n", index);
}
