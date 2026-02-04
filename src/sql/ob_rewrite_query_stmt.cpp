#include "sql/ob_rewrite_query_stmt.h"
#include "sql/ob_sql_expression.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObRewriteQueryStmt::ObRewriteQueryStmt()
  :logical_plan_(NULL), schema_checker_(NULL), column_list_(NULL)
{
}

ObRewriteQueryStmt::~ObRewriteQueryStmt()
{
}

int ObRewriteQueryStmt::init(ObLogicalPlan *logical_plan, ObSchemaChecker *schema_checker, ObArray<ObString> *column_list)
{
  int ret = OB_SUCCESS;
  if (logical_plan == NULL || schema_checker == NULL || column_list == NULL)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(WARN, "init failed, arguments is null.");
  }
  else
  {
    logical_plan_ = logical_plan;
    schema_checker_ = schema_checker;
    column_list_ = column_list;
  }
  return ret;
}

int ObRewriteQueryStmt::rewrite_select_stmt(ObSelectStmt *select_stmt, char *sql_text, bool main_stmt)
{
  int ret = OB_SUCCESS;
  ObSelectStmt::SetOperator set_op = select_stmt->get_set_op();
  if (set_op != ObSelectStmt::NONE)
  {
    int64_t pos = 0;
    uint64_t left_query_id = select_stmt->get_left_query_id();
    uint64_t right_query_id = select_stmt->get_right_query_id();
    ObSelectStmt* left_stmt = dynamic_cast<ObSelectStmt*>(logical_plan_->get_query(left_query_id));
    ObSelectStmt* right_stmt = dynamic_cast<ObSelectStmt*>(logical_plan_->get_query(right_query_id));
    char left_stmt_text[OB_MAX_SQL_LENGTH] = {0};
    char right_stmt_text[OB_MAX_SQL_LENGTH] = {0};
    ret = rewrite_select_stmt(left_stmt, left_stmt_text, main_stmt);
    if (ret == OB_SUCCESS)
    {
      ret = rewrite_select_stmt(right_stmt, right_stmt_text, false);
    }
    if (ret == OB_SUCCESS)
    {
      databuff_printf(sql_text, OB_MAX_SQL_LENGTH, pos, "%s", left_stmt_text);
      switch (set_op)
      {
        case ObSelectStmt::UNION:
        {
          databuff_printf(sql_text, OB_MAX_SQL_LENGTH, pos, " union ");
          break;
        }
        case ObSelectStmt::INTERSECT:
        {
          databuff_printf(sql_text, OB_MAX_SQL_LENGTH, pos, " intersect ");
          break;
        }
        case ObSelectStmt::EXCEPT:
        {
          databuff_printf(sql_text, OB_MAX_SQL_LENGTH, pos, " except ");
          break;
        }
        default:
          break;
      }
      databuff_printf(sql_text, OB_MAX_SQL_LENGTH, pos, "%s", right_stmt_text);
      sql_text[pos] = '\0';
    }
  }
  else
  {
    char select_clause[OB_MAX_SQL_LENGTH] = {0};
    char from_clause[OB_MAX_SQL_LENGTH] = {0};
    char where_clause[OB_MAX_SQL_LENGTH] = {0};
    char group_clause[OB_MAX_SQL_LENGTH] = {0};
    char having_clause[OB_MAX_SQL_LENGTH] = {0};
    char order_clause[OB_MAX_SQL_LENGTH] = {0};
    char limit_clause[OB_MAX_SQL_LENGTH] = {0};
    select_clause[0] = '\0';
    from_clause[0] = '\0';
    where_clause[0] = '\0';
    group_clause[0] = '\0';
    having_clause[0] = '\0';
    order_clause[0] = '\0';
    limit_clause[0] = '\0';

    if (OB_SUCCESS != (ret = rewrite_select_clause(select_stmt, select_clause, main_stmt)))
    {
      YYSYS_LOG(WARN, "rewrite select clause failed.");
    }
    else if (OB_SUCCESS != (ret = rewrite_from_clause(select_stmt, from_clause)))
    {
      YYSYS_LOG(WARN, "rewrite from clause failed.");
    }
    else if (OB_SUCCESS != (ret = rewrite_where_clause(select_stmt, where_clause)))
    {
      YYSYS_LOG(WARN, "rewrite where clause failed.");
    }
    else if (OB_SUCCESS != (ret = rewrite_group_clause(select_stmt, group_clause)))
    {
      YYSYS_LOG(WARN, "rewrite group clause failed.");
    }
    else if (OB_SUCCESS != (ret = rewrite_having_clause(select_stmt, having_clause)))
    {
      YYSYS_LOG(WARN, "rewrite having clause failed.");
    }
    else if (OB_SUCCESS != (ret = rewrite_order_clause(select_stmt, order_clause)))
    {
      YYSYS_LOG(WARN, "rewrite order clause failed.");
    }
    else if (OB_SUCCESS != (ret = rewrite_limit_clause(select_stmt, limit_clause)))
    {
      YYSYS_LOG(WARN, "rewrite limit clause failed.");
    }
    else
    {
      int64_t pos = 0;
      size_t select_clause_len = strlen(select_clause);
      size_t from_clause_len = strlen(from_clause);
      size_t where_clause_len = strlen(where_clause);
      size_t group_clause_len = strlen(group_clause);
      size_t having_clause_len = strlen(having_clause);
      size_t order_clause_len = strlen(order_clause);
      size_t limit_clause_len = strlen(limit_clause);
      size_t len = select_clause_len + from_clause_len + where_clause_len + group_clause_len
                   + having_clause_len + order_clause_len + limit_clause_len;
      if (static_cast<int64_t>(len) >= OB_MAX_SQL_LENGTH)
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(WARN, "the sql length exceeds the requirement.");
      }
      else
      {
        databuff_printf(sql_text, OB_MAX_SQL_LENGTH, pos, "%s", select_clause);
        if (from_clause_len > 0 && pos < OB_MAX_SQL_LENGTH)
        {
          databuff_printf(sql_text, OB_MAX_SQL_LENGTH, pos, "%s", from_clause);
        }
        if (where_clause_len > 0 && pos < OB_MAX_SQL_LENGTH)
        {
          databuff_printf(sql_text, OB_MAX_SQL_LENGTH, pos, "%s", where_clause);
        }
        if (group_clause_len > 0 && pos < OB_MAX_SQL_LENGTH)
        {
          databuff_printf(sql_text, OB_MAX_SQL_LENGTH, pos, "%s", group_clause);
        }
        if (having_clause_len > 0 && pos < OB_MAX_SQL_LENGTH)
        {
          databuff_printf(sql_text, OB_MAX_SQL_LENGTH, pos, "%s", having_clause);
        }
        if (order_clause_len > 0 && pos < OB_MAX_SQL_LENGTH)
        {
          databuff_printf(sql_text, OB_MAX_SQL_LENGTH, pos, "%s", order_clause);
        }
        if (limit_clause_len > 0 && pos < OB_MAX_SQL_LENGTH)
        {
          databuff_printf(sql_text, OB_MAX_SQL_LENGTH, pos, "%s", limit_clause);
        }
        sql_text[pos] = '\0';
      }
      YYSYS_LOG(DEBUG, "view_definition = [%s]", sql_text);
    }
  }
  return ret;
}

int ObRewriteQueryStmt::rewrite_select_clause(ObSelectStmt *select_stmt, char *select_clause, bool main_stmt)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (select_stmt->is_distinct())
  {
    databuff_printf(select_clause, OB_MAX_SQL_LENGTH, pos, "select distinct ");
  }
  else
  {
    databuff_printf(select_clause, OB_MAX_SQL_LENGTH, pos, "select ");
  }
  int32_t size = select_stmt->get_select_item_size();
  for (int32_t idx=0 ; ret == OB_SUCCESS && idx<size ; idx++)
  {
    const SelectItem& item = select_stmt->get_select_item(idx);
    uint64_t expr_id = item.expr_id_;
    //��һ������� select * from (select sum(c1) from t1);
    //select "sum(c1)" as "c1" from (select sum("db"."v1"."c1") as "sum(c1)" from "db"."v1");
    //1.�����ǰselect_item��ӦexprΪnull����Ҫ��select_item��expr_nameд��
    //2.�Ӳ�ѯ����Ҫ��sum(c1)��дΪ sum("db"."t1"."c1") as "sum(c1)"
    bool column_name_is_null = false;
    ObRawExpr *expr = logical_plan_->get_expr(expr_id)->get_expr();
    ret = rewrite_independ_expr(select_stmt, expr, select_clause, pos ,&column_name_is_null);
    if (column_name_is_null)
    {
      databuff_printf(select_clause, OB_MAX_SQL_LENGTH, pos, "\"%.*s\"",
                      item.expr_name_.length(), item.expr_name_.ptr());
    }
    if (main_stmt == true)
    {
      const ObString &column_name = column_list_->at(idx);
      databuff_printf(select_clause, OB_MAX_SQL_LENGTH, pos, " as \"%.*s\"",
                      column_name.length(), column_name.ptr());
    }
    else
    {
      if (item.is_real_alias_)//has alias name
      {
        databuff_printf(select_clause, OB_MAX_SQL_LENGTH, pos, " as \"%.*s\"",
                        item.alias_name_.length(), item.alias_name_.ptr());
      }
      else
      {
        databuff_printf(select_clause, OB_MAX_SQL_LENGTH, pos, " as \"%.*s\"",
                        item.expr_name_.length(), item.expr_name_.ptr());
      }
    }
    if(idx < size -1)
    {
      databuff_printf(select_clause, OB_MAX_SQL_LENGTH, pos, ", ");
    }
  }
  select_clause[pos] = '\0';
  return ret;
}

int ObRewriteQueryStmt::rewrite_table_item(ObSelectStmt *select_stmt, const uint64_t table_id, char *from_clause, int64_t &pos)
{
  int ret = OB_SUCCESS;
  const TableItem* table_item = select_stmt->get_table_item_by_id(table_id);
  switch (table_item->type_)
  {
    case TableItem::BASE_TABLE:
    case TableItem::ALIAS_TABLE:
    {
      const ObTableSchema *table_schema = schema_checker_->get_table_schema(table_item->ref_id_);
      if (table_schema == NULL)
      {
        ret = OB_ENTRY_NOT_EXIST;
        YYSYS_LOG(WARN, "get table schema failed");
      }
      else
      {
        bool is_sys_table = false;
        const char *name = table_schema->get_table_name();
        char db_name[OB_MAX_DATBASE_NAME_LENGTH] = {0};
        char table_name[OB_MAX_TABLE_NAME_LENGTH] = {0};
        split(name, db_name, table_name, is_sys_table);
        if (table_item->type_ == TableItem::BASE_TABLE)
        {
          if (is_sys_table)
          {
            databuff_printf(from_clause, OB_MAX_SQL_LENGTH, pos, "\"%s\"", db_name);
          }
          else
          {
            databuff_printf(from_clause, OB_MAX_SQL_LENGTH, pos, "\"%s\".\"%s\"", db_name, table_name);
          }
        }
        else
        {
          databuff_printf(from_clause, OB_MAX_SQL_LENGTH, pos, "\"%s\".\"%s\" as \"%.*s\"", db_name, table_name, table_item->alias_name_.length(), table_item->alias_name_.ptr());//?????);
        }
      }
      break;
    }
    case TableItem::GENERATED_TABLE:
    {
      ObSelectStmt *sub_select_stmt = dynamic_cast<ObSelectStmt *> (logical_plan_->get_query(table_item->ref_id_));
      char sub_stmt[OB_MAX_SQL_LENGTH] = {0};
      ret = rewrite_select_stmt(sub_select_stmt, sub_stmt);
      if (ret == OB_SUCCESS)
      {
        databuff_printf(from_clause, OB_MAX_SQL_LENGTH, pos, "(%s)", sub_stmt);

        if (table_item->is_real_alias_)
        {
          databuff_printf(from_clause, OB_MAX_SQL_LENGTH, pos, " as \"%.*s\"",
                          table_item->table_name_.length(), table_item->table_name_.ptr());
        }
      }
      break;
    }
    default:
      ret = OB_ERR_UNEXPECTED;
      YYSYS_LOG(WARN, "won't be here.");
      break;
  }
  return ret;
}

int ObRewriteQueryStmt::rewrite_joined_table(ObSelectStmt *select_stmt, const JoinedTableInfo* joined_table, char *from_clause, int64_t &pos)
{
  int ret = OB_SUCCESS;
  databuff_printf(from_clause, OB_MAX_SQL_LENGTH, pos, "(");
  if (joined_table->child_[0] != NULL)
  {
    ret = rewrite_joined_table(select_stmt, joined_table->child_[0], from_clause, pos);
  }
  else
  {
    ret = rewrite_table_item(select_stmt, joined_table->tid_[0], from_clause, pos);
  }
  if (ret == OB_SUCCESS)
  {
    switch(joined_table->type_)
    {
      case JoinedTable::T_FULL:
        databuff_printf(from_clause, OB_MAX_SQL_LENGTH, pos, " full join ");
        break;
      case JoinedTable::T_LEFT:
        databuff_printf(from_clause, OB_MAX_SQL_LENGTH, pos, " left join ");
        break;
      case JoinedTable::T_RIGHT:
        databuff_printf(from_clause, OB_MAX_SQL_LENGTH, pos, " right join ");
        break;
      case JoinedTable::T_INNER:
        databuff_printf(from_clause, OB_MAX_SQL_LENGTH, pos, " inner join ");
        break;
      default:
        break;
    }
  }
  if (ret == OB_SUCCESS && joined_table->child_[1] != NULL)
  {
    ret = rewrite_joined_table(select_stmt, joined_table->child_[1], from_clause, pos);
  }
  else
  {
    ret = rewrite_table_item(select_stmt, joined_table->tid_[1], from_clause, pos);
  }
  if (ret == OB_SUCCESS && joined_table->expr_ids_.count() > 0)
  {
    int64_t size = joined_table->expr_ids_.count();
    databuff_printf(from_clause, OB_MAX_SQL_LENGTH, pos, " on ");
    for (int64_t i=0 ; ret == OB_SUCCESS && i<size ;i++)
    {
      const uint64_t expr_id = joined_table->expr_ids_.at(i);
      ObRawExpr *expr = logical_plan_->get_expr(expr_id)->get_expr();
      ret = rewrite_independ_expr(select_stmt, expr, from_clause, pos);
      if ( i < size -1)
      {
        databuff_printf(from_clause, OB_MAX_SQL_LENGTH, pos, " and ");
      }
    }
  }
  databuff_printf(from_clause, OB_MAX_SQL_LENGTH, pos, ")");
  return ret;
}

int ObRewriteQueryStmt::rewrite_from_clause(ObSelectStmt *select_stmt, char *from_clause)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int32_t size = select_stmt->get_from_item_size();
  if (size > 0)
  {
    databuff_printf(from_clause, OB_MAX_SQL_LENGTH, pos, " from ");
    for (int32_t idx=0 ; ret == OB_SUCCESS && idx<size ; idx++)
    {
      const FromItem& from_item = select_stmt->get_from_item(idx);
      if (from_item.is_joined_)
      {
        //joined table�� table_id����ʱ���ɵ�
        const JoinedTableInfo* joined_table = NULL;
        int32_t joined_table_info_size = select_stmt->get_joined_table_info_size();
        for (int32_t i=0 ; i<joined_table_info_size ; i++)
        {
          const JoinedTableInfo* table = select_stmt->get_joined_table_info(i);
          if (table->joined_table_id_ == from_item.table_id_)
          {
            joined_table = table;
            break;
          }
        }
        if (joined_table == NULL)
        {
          ret = OB_ENTRY_NOT_EXIST;
          YYSYS_LOG(WARN, "get joined table info failed");
        }
        else if (OB_SUCCESS != (ret = rewrite_joined_table(select_stmt, joined_table, from_clause, pos)))
        {
          YYSYS_LOG(WARN, "rewrite joined table info failed");
        }
      }
      else if (OB_SUCCESS != (ret = rewrite_table_item(select_stmt, from_item.table_id_, from_clause, pos)))
      {
        YYSYS_LOG(WARN, "get table item failed");
      }
      if (idx < size - 1)
      {
        databuff_printf(from_clause, OB_MAX_SQL_LENGTH, pos, ", ");
      }
    }
  }
  else
  {
    databuff_printf(from_clause, OB_MAX_SQL_LENGTH, pos, " from dual");
  }
  from_clause[pos] = '\0';
  return ret;
}

int ObRewriteQueryStmt::rewrite_where_clause(ObSelectStmt *select_stmt, char *where_clause)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObVector<uint64_t> &where_exprs = select_stmt->get_where_exprs();
  int32_t size = where_exprs.size();
  if (size > 0)
  {
    databuff_printf(where_clause, OB_MAX_SQL_LENGTH, pos, " where ");
    for (int32_t idx=0 ; ret == OB_SUCCESS && idx<size ; idx++)
    {
      uint64_t expr_id = where_exprs.at(idx);
      ObRawExpr *expr=logical_plan_->get_expr(expr_id)->get_expr();
      databuff_printf(where_clause, OB_MAX_SQL_LENGTH, pos, "(");
      ret = rewrite_independ_expr(select_stmt, expr, where_clause, pos);
      databuff_printf(where_clause, OB_MAX_SQL_LENGTH, pos, ")");
      if (idx < size -1)
      {
        databuff_printf(where_clause, OB_MAX_SQL_LENGTH, pos, " and ");
      }
    }
    where_clause[pos] = '\0';
  }
  return ret;
}

int ObRewriteQueryStmt::rewrite_group_clause(ObSelectStmt *select_stmt, char *group_clause)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int32_t size = select_stmt->get_group_expr_size();
  if (size > 0)
  {
    databuff_printf(group_clause, OB_MAX_SQL_LENGTH, pos, " group by ");
    for (int32_t idx=0 ; ret == OB_SUCCESS && idx<size ; idx++)
    {
      uint64_t expr_id = select_stmt->get_group_expr_id(idx);
      ObRawExpr *expr = logical_plan_->get_expr(expr_id)->get_expr();
      ret = rewrite_independ_expr(select_stmt, expr, group_clause, pos);
      if (idx < size -1)
      {
        databuff_printf(group_clause, OB_MAX_SQL_LENGTH, pos, ", ");
      }
    }
    group_clause[pos] = '\0';
  }
  return ret;
}

int ObRewriteQueryStmt::rewrite_having_clause(ObSelectStmt *select_stmt, char *having_clause)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int32_t size = select_stmt->get_having_expr_size();
  if (size > 0)
  {
    databuff_printf(having_clause, OB_MAX_SQL_LENGTH, pos, " having ");
    for (int32_t idx=0 ; ret == OB_SUCCESS && idx<size ; idx++)
    {
      uint64_t expr_id = select_stmt->get_having_expr_id(idx);
      ObRawExpr *expr = logical_plan_->get_expr(expr_id)->get_expr();
      ret = rewrite_independ_expr(select_stmt, expr, having_clause, pos);
      if (idx < size -1)
      {
        databuff_printf(having_clause, OB_MAX_SQL_LENGTH, pos, " and ");
      }
    }
    having_clause[pos] = '\0';
  }
  return ret;
}

int ObRewriteQueryStmt::rewrite_order_clause(ObSelectStmt *select_stmt,char *order_clause)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int32_t size = select_stmt->get_order_item_size();
  if (size > 0)
  {
    databuff_printf(order_clause, OB_MAX_SQL_LENGTH, pos, " order by ");
    for (int32_t idx=0 ; ret == OB_SUCCESS && idx<size ; idx++)
    {
      const OrderItem &item = select_stmt->get_order_item(idx);
      ObRawExpr *expr = logical_plan_->get_expr(item.expr_id_)->get_expr();
      if (expr->get_expr_type() == T_INT)
      {
        //T_INT :select 5 ,c1 from t1 order by 1;//1
        databuff_printf(order_clause, OB_MAX_SQL_LENGTH, pos, "''");
      }
      else
      {
        ret = rewrite_independ_expr(select_stmt, expr, order_clause, pos);
      }
      if (idx < size - 1)
      {
        databuff_printf(order_clause, OB_MAX_SQL_LENGTH, pos, ", ");
      }
      else
      {
        databuff_printf(order_clause, OB_MAX_SQL_LENGTH, pos, " %s",
                        (item.order_type_ == OrderItem::ASC ? "asc" : "desc"));
      }
    }
    order_clause[pos] = '\0';
  }
  return ret;
}

int ObRewriteQueryStmt::rewrite_limit_clause(ObSelectStmt *select_stmt, char *limit_clause)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (select_stmt->has_limit())
  {
    databuff_printf(limit_clause, OB_MAX_SQL_LENGTH, pos, " limit ");
    uint64_t expr_id = select_stmt->get_limit_expr_id();
    ObRawExpr *expr = logical_plan_->get_expr(expr_id)->get_expr();
    ret = rewrite_independ_expr(select_stmt, expr, limit_clause, pos);
    if (ret == OB_SUCCESS)
    {
      uint64_t expr_id = select_stmt->get_offset_expr_id();
      if (expr_id != OB_INVALID_ID)
      {
        databuff_printf(limit_clause, OB_MAX_SQL_LENGTH, pos, " offset ");
        ObRawExpr *expr = logical_plan_->get_expr(expr_id)->get_expr();
        ret = rewrite_independ_expr(select_stmt, expr, limit_clause, pos);
      }
    }
    limit_clause[pos] = '\0';
  }
  return ret;
}

int ObRewriteQueryStmt::rewrite_independ_expr(ObSelectStmt *select_stmt,ObRawExpr *expr,
                                              char *text, int64_t &pos, bool *column_is_null)
{
  int ret = OB_SUCCESS;
  const ObItemType &expr_type = expr->get_expr_type();
  switch (expr_type)
  {
    case T_BINARY:
    case T_STRING:
    case T_SYSTEM_VARIABLE:
    case T_TEMP_VARIABLE:
    {
      ObObj value;
      ObString val;
      ObConstRawExpr *c_expr = dynamic_cast<ObConstRawExpr *> (expr);
      value = c_expr->get_value();
      ret = value.get_varchar(val);
      if (ret == OB_SUCCESS)
      {//select 'string',//where c1 = 'len'..
        databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "'%.*s'", val.length(), val.ptr());
      }
      break;
    }
    case T_FLOAT:
    {
      ObObj value;
      ObConstRawExpr *c_expr = dynamic_cast<ObConstRawExpr *> (expr);
      value = c_expr->get_value();
      float val = 0;
      ret = value.get_float(val);
      if(ret == OB_SUCCESS)
      {
        databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "%f", val);
      }
      break;
    }
    case T_DOUBLE:
    {
      ObObj value;
      ObConstRawExpr *c_expr = dynamic_cast<ObConstRawExpr *> (expr);
      value = c_expr->get_value();
      double val = 0;
      ret = value.get_double(val);
      if(ret == OB_SUCCESS)
      {
        databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "%lf", val);
      }
      break;
    }
    case T_DECIMAL:
    {
      ObObj value;
      ObConstRawExpr *c_expr = dynamic_cast<ObConstRawExpr *> (expr);
      value = c_expr->get_value();
      ObDecimal val;
      ret = value.get_decimal(val);
      if(ret == OB_SUCCESS)
      {
        databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "%s", to_cstring(val));
      }
      break;
    }
    case T_INT:
    {
      ObObj value;
      ObConstRawExpr *c_expr = dynamic_cast<ObConstRawExpr *> (expr);
      value = c_expr->get_value();
      int64_t val = 0;
      ret = value.get_int(val);
      if(ret == OB_SUCCESS)
      {
        databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "%ld", val);
      }
      break;
    }
    case T_BOOL:
    {
      ObObj value;
      ObConstRawExpr *c_expr = dynamic_cast<ObConstRawExpr *> (expr);
      value = c_expr->get_value();
      bool val = false;
      ret = value.get_bool(val);
      if(ret == OB_SUCCESS)
      {
        databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "%s", (val ? "true" : "false"));
      }
      break;
    }
    case T_DATE:
    {
      ObObj value;
      ObConstRawExpr *c_expr = dynamic_cast<ObConstRawExpr *> (expr);
      value = c_expr->get_value();
      ObPreciseDateTime val = 0;
      ret = value.get_precise_datetime(val);
      if(ret == OB_SUCCESS)
      {
        databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "%ld", val);
      }
      break;
    }
    case T_DATE_NEW:
    {
      ObObj value;
      ObConstRawExpr *c_expr = dynamic_cast<ObConstRawExpr *> (expr);
      value = c_expr->get_value();
      ObDate val = 0;
      ret = value.get_date(val);
      if(ret == OB_SUCCESS)
      {
        databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "%ld", val);
      }
      break;
    }
    case T_TIME:
    {
      ObObj value;
      ObConstRawExpr *c_expr = dynamic_cast<ObConstRawExpr *> (expr);
      value = c_expr->get_value();
      ObTime val = 0;
      ret = value.get_time(val);
      if(ret == OB_SUCCESS)
      {
        databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "%ld", val);
      }
      break;
    }
    case T_NULL:
    {
      databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "null");
      break;
    }
    case T_QUESTIONMARK:
    {
      databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "?");
      break;
    }
    case T_CUR_TIME:
    {
      databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "now()");
      break;
    }
    case T_CUR_TIME_HMS:
    {
      databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "current_time()");
      break;
    }
    case T_CUR_DATE:
    {
      databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "current_date()");
      break;
    }
    case T_REF_COLUMN:
    {
      ObBinaryRefRawExpr *b_expr = dynamic_cast<ObBinaryRefRawExpr *> (expr);
      //for aggr function
      if (b_expr->is_contain_aggr())
      {
        ObRawExpr *sub_expr = b_expr->get_aggr_expr();
        ret = rewrite_independ_expr(select_stmt, sub_expr, text, pos);
        break;
      }

      //for column item
      uint64_t tid = b_expr->get_first_ref_id();
      uint64_t cid = b_expr->get_second_ref_id();
      const ColumnItem *column_item = select_stmt->get_column_item_by_id(tid, cid);
      if (column_item == NULL)
      {
        //
        //
        //
        //���� tid & cid,����select_item����ÿһ��item��table_id��column_id�ж�
        //��ȡ���Ӧ��expr
        uint64_t expr_id = OB_INVALID_ID;
        int32_t select_item_size = select_stmt->get_select_item_size();
        for (int32_t i=0 ; i<select_item_size ; i++)
        {
          const SelectItem &select_item = select_stmt->get_select_item(i);
          ObSqlRawExpr *sql_expr = logical_plan_->get_expr(select_item.expr_id_);
          if (sql_expr->get_column_id() == cid && sql_expr->get_table_id() == tid)
          {
            expr_id = select_item.expr_id_;
            break;
          }
        }
        if (expr_id == OB_INVALID_ID)
        {
          ret = OB_ERR_COLUMN_UNKNOWN;
          YYSYS_LOG(WARN, "find column item failed, tid=[%lu], cid=[%lu]", tid, cid);
        }
        else
        {
          ObRawExpr *sub_expr = logical_plan_->get_expr(expr_id)->get_expr();
          ret = rewrite_independ_expr(select_stmt, sub_expr, text, pos);
        }
      }
      else
      {
        const ObTableSchema *table_schema = NULL;
        const TableItem *table_item = select_stmt->get_table_item_by_id(column_item->table_id_);
        switch (table_item->type_)
        {
          case TableItem::BASE_TABLE:
          {
            table_schema = schema_checker_->get_table_schema(table_item->table_id_);
            if (table_schema == NULL)
            {
              ret = OB_INVALID_ARGUMENT;
              YYSYS_LOG(WARN, "get table schema failed, tid=[%lu]", tid);
              break;
            }
            bool is_sys_table = false;
            const char *name =table_schema->get_table_name();
            char db_name[OB_MAX_DATBASE_NAME_LENGTH] = {0};
            char table_name[OB_MAX_TABLE_NAME_LENGTH] = {0};
            split(name, db_name, table_name, is_sys_table);
            YYSYS_LOG(DEBUG, "db_name=[%s], table_name=[%s]", db_name, table_name);
            if (is_sys_table)
            {
              databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "\"%s\".\"%.*s\"", db_name, column_item->column_name_.length(), column_item->column_name_.ptr());
            }
            else
            {
              databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "\"%s\".\"%s\".\"%.*s\"", db_name, table_name, column_item->column_name_.length(), column_item->column_name_.ptr());
            }

            break;
          }
          case TableItem::ALIAS_TABLE:
          {
            databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "\"%.*s\".\"%.*s\"", table_item->alias_name_.length(), table_item->alias_name_.ptr(), column_item->column_name_.length(), column_item->column_name_.ptr());
            break;
          }
          case TableItem::GENERATED_TABLE:
          {
            //�Ӳ�ѯ��һ���б���
            if (table_item->is_real_alias_)
            {
              databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "\"%.*s\".", table_item->table_name_.length(), table_item->table_name_.ptr());
            }
            if (column_item->column_name_.length() > 0)
            {
              databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "\"%.*s\"", column_item->column_name_.length(), column_item->column_name_.ptr());
            }
            else if (NULL != column_is_null)
            {
              *column_is_null = true;
            }
            break;
          }
          default:
          {
            ret=OB_ERR_UNEXPECTED;
            YYSYS_LOG(WARN,"unexpected error");
            break;
          }
        }
      }
      break;
    }
    case T_OP_EXISTS:
    case T_OP_POS:
    case T_OP_NEG:
    case T_OP_NOT:
    {
      switch(expr_type)
      {
        case T_OP_EXISTS:
        {
          databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"exists");
          break;
        }
        case T_OP_POS:
        {
          databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"+");
          break;
        }
        case T_OP_NEG:
        {
          databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"-");
          break;
        }
        case T_OP_NOT:
        {
          databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"not ");
          break;
        }
        default:
          break;
      }
      ObUnaryOpRawExpr *u_expr = dynamic_cast<ObUnaryOpRawExpr *> (expr);
      ObRawExpr * op_expr = u_expr->get_op_expr();
      ret = rewrite_independ_expr(select_stmt,op_expr,text,pos);
      break;
    }
    case T_OP_ADD:
    case T_OP_MINUS:
    case T_OP_MUL:
    case T_OP_DIV:
    case T_OP_REM:
    case T_OP_POW:
    case T_OP_MOD:
    case T_OP_LE:
    case T_OP_LT:
    case T_OP_EQ:
    case T_OP_GE:
    case T_OP_GT:
    case T_OP_NE:
    case T_OP_LIKE:
    case T_OP_NOT_LIKE:
    case T_OP_AND:
    case T_OP_OR:
    case T_OP_IS:
    case T_OP_IS_NOT:
    case T_OP_CNN:
    {
      ObBinaryOpRawExpr *b_expr = dynamic_cast<ObBinaryOpRawExpr *> (expr);
      ObRawExpr *first_expr=b_expr->get_first_op_expr();
      ObRawExpr *second_expr=b_expr->get_second_op_expr();
      if(expr_type == T_OP_CNN)
      {
        databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"concat(");
        ret = rewrite_independ_expr(select_stmt,first_expr,text,pos);
        if(ret == OB_SUCCESS)
        {
          databuff_printf(text,OB_MAX_SQL_LENGTH,pos,",");
          ret = rewrite_independ_expr(select_stmt,second_expr,text,pos);
        }
        if(ret == OB_SUCCESS)
        {
          databuff_printf(text,OB_MAX_SQL_LENGTH,pos,")");
        }
        break;
      }
      databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"(");
      ret = rewrite_independ_expr(select_stmt,first_expr,text,pos);
      if(ret == OB_SUCCESS)
      {
        databuff_printf(text,OB_MAX_SQL_LENGTH,pos," ");
        switch(expr_type)
        {
          case T_OP_ADD:
          {
            databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"+");
            break;
          }
          case T_OP_MINUS:
          {
            databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"-");
            break;
          }
          case T_OP_MUL:
          {
            databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"*");
            break;
          }
          case T_OP_DIV:
          {
            databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"/");
            break;
          }
          case T_OP_REM:
          {
            databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"%%");
            break;
          }
          case T_OP_POW:
          {
            databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"^");
            break;
          }
          case T_OP_MOD:
          {
            databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"mod");
            break;
          }
          case T_OP_LE://<=
          {
            databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"<=");
            break;
          }
          case T_OP_LT:
          {
            databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"<");
            break;
          }
          case T_OP_EQ:
          {
            databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"=");
            break;
          }
          case T_OP_GE:
          {
            databuff_printf(text,OB_MAX_SQL_LENGTH,pos,">=");
            break;
          }
          case T_OP_GT:
          {
            databuff_printf(text,OB_MAX_SQL_LENGTH,pos,">");
            break;
          }
          case T_OP_NE:
          {
            databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"!=");
            break;
          }
          case T_OP_LIKE:
          {
            databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"like");
            break;
          }
          case T_OP_NOT_LIKE:
          {
            databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"not like");
            break;
          }
          case T_OP_AND:
          {
            databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"and");
            break;
          }
          case T_OP_OR:
          {
            databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"or");
            break;
          }
          case T_OP_IS:
          {
            databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"is");
            break;
          }
          case T_OP_IS_NOT:
          {
            databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"is not");
            break;
          }
          default:
            break;
        }
        databuff_printf(text,OB_MAX_SQL_LENGTH,pos," ");
        ret = rewrite_independ_expr(select_stmt,second_expr,text,pos);
        if(ret != OB_SUCCESS)
        {
          YYSYS_LOG(WARN,"rewrite operation expr failed, ret=%d",ret);
          break;
        }
        databuff_printf(text,OB_MAX_SQL_LENGTH,pos,")");
      }
      break;
    }
    case T_OP_BTW:
    case T_OP_NOT_BTW:
    {
      //expr between expr and expr
      ObTripleOpRawExpr *t_expr = dynamic_cast<ObTripleOpRawExpr *> (expr);
      ObRawExpr *first_expr = t_expr->get_first_op_expr();
      ObRawExpr *second_expr = t_expr->get_second_op_expr();
      ObRawExpr *third_expr = t_expr->get_third_op_expr();
      ret = rewrite_independ_expr(select_stmt,first_expr,text,pos);
      if(ret == OB_SUCCESS)
      {
        if(expr_type == T_OP_BTW)
        {
          databuff_printf(text,OB_MAX_SQL_LENGTH,pos," between ");
        }
        else
        {
          databuff_printf(text,OB_MAX_SQL_LENGTH,pos," not between ");
        }
        ret = rewrite_independ_expr(select_stmt,second_expr,text,pos);
        if(ret == OB_SUCCESS)
        {
          databuff_printf(text,OB_MAX_SQL_LENGTH,pos," and ");
          ret = rewrite_independ_expr(select_stmt,third_expr,text,pos);
        }
      }
      break;
    }
    case T_OP_IN:
    case T_OP_NOT_IN:
    {
      //expr in expr
      ObBinaryOpRawExpr *in_expr=dynamic_cast<ObBinaryOpRawExpr *>(expr);
      ObRawExpr *first_expr = in_expr->get_first_op_expr();
      ObRawExpr *second_expr = in_expr->get_second_op_expr();
      ret = rewrite_independ_expr(select_stmt,first_expr,text,pos);
      if(ret == OB_SUCCESS)
      {
        if(expr_type == T_OP_IN)
        {
          databuff_printf(text,OB_MAX_SQL_LENGTH,pos," in (");
        }
        else
        {
          databuff_printf(text,OB_MAX_SQL_LENGTH,pos," not in (");
        }
        ret = rewrite_independ_expr(select_stmt,second_expr,text,pos);
        if(ret != OB_SUCCESS)
        {
          YYSYS_LOG(WARN,"rewrite in expr failed, ret =%d",ret);
          break;
        }
        databuff_printf(text,OB_MAX_SQL_LENGTH,pos,")");
      }
      break;
    }
    case T_OP_CASE:
    case T_OP_ARG_CASE:
    {
      //casec1
      //when c1 >= 1 then 'a'
      //when c1 >= 9 thwn 'b'
      //else 'c' end
      databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"case ");
      ObCaseOpRawExpr *case_expr = dynamic_cast<ObCaseOpRawExpr *> (expr);
      if(expr_type == T_OP_ARG_CASE)
      {
        ObRawExpr *arg_expr = case_expr->get_arg_op_expr();
        ret = rewrite_independ_expr(select_stmt,arg_expr,text,pos);
      }
      if(ret != OB_SUCCESS)
      {
        YYSYS_LOG(WARN,"rewrite case expr failed,ret =%d",ret);
        break;
      }

      int32_t when_size = case_expr->get_when_expr_size();
      for (int32_t i=0;ret==OB_SUCCESS && i<when_size;i++)
      {
        ObRawExpr *when_expr = case_expr->get_when_op_expr(i);
        ObRawExpr *then_expr = case_expr->get_then_op_expr(i);
        databuff_printf(text,OB_MAX_SQL_LENGTH,pos," when ");
        ret = rewrite_independ_expr(select_stmt,when_expr,text,pos);
        databuff_printf(text,OB_MAX_SQL_LENGTH,pos," then ");
        if(ret == OB_SUCCESS)
        {
          ret = rewrite_independ_expr(select_stmt,then_expr,text,pos);
        }
      }

      ObRawExpr *default_expr = case_expr->get_default_op_expr();
      if(default_expr != NULL)
      {
        databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"else ");
        ret = rewrite_independ_expr(select_stmt,default_expr,text,pos);
      }
      if(ret == OB_SUCCESS)
      {
        databuff_printf(text,OB_MAX_SQL_LENGTH,pos," end");
      }
      break;
    }
    case T_OP_ROW:
    {//expr_list c1 in (1*3,2,3,5,6)
      ObMultiOpRawExpr *multi_expr = dynamic_cast<ObMultiOpRawExpr *> (expr);
      int32_t expr_size = multi_expr->get_expr_size();
      for (int32_t i=0;ret==OB_SUCCESS && i<expr_size;i++)
      {
        ObRawExpr *op_expr = multi_expr->get_op_expr(i);
        ret = rewrite_independ_expr(select_stmt,op_expr,text,pos);
        if(ret == OB_SUCCESS)
        {
          if(i < expr_size -1)
          {
            databuff_printf(text,OB_MAX_SQL_LENGTH,pos,", ");
          }
        }
      }
      break;
    }
    case T_REF_QUERY:
    {
      ObUnaryRefRawExpr *sub_query_expr = dynamic_cast<ObUnaryRefRawExpr *>(expr);
      uint64_t ref_id = sub_query_expr->get_ref_id();
      ObBasicStmt *stmt = logical_plan_->get_query(ref_id);
      if(stmt->get_stmt_type()==ObBasicStmt::T_SELECT)
      {
        ObSelectStmt *sub_select_stmt = dynamic_cast<ObSelectStmt *>(stmt);
        char sub_stmt[OB_MAX_SQL_LENGTH] = {0};
        ret = rewrite_select_stmt(sub_select_stmt,sub_stmt);
        if(ret == OB_SUCCESS)
        {
          databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"(%s)",sub_stmt);
        }
      }
      //insert update delete
      else
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(INFO,"unexpected error");
      }
      break;
    }
    case T_FUN_COUNT:
    case T_FUN_MAX:
    case T_FUN_MIN:
    case T_FUN_SUM:
    case T_FUN_AVG:
    case T_FUN_LISTAGG:
    case T_FUN_ROW_NUMBER:
    {
      ObAggFunRawExpr *agg_expr = dynamic_cast<ObAggFunRawExpr *>(expr);
      if(expr_type == T_FUN_ROW_NUMBER)
      {
        databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"row_number() over ");
        int32_t partition_expr_size = select_stmt->get_partition_expr_size();
        int32_t order_by_size = select_stmt->get_order_item_for_rownum_size();
        if(partition_expr_size > 0 || order_by_size > 0)
        {
          databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"(");
        }
        if(partition_expr_size > 0)
        {
          databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"partition by ");
        }
        for (int32_t i=0;ret==OB_SUCCESS && i<partition_expr_size;i++)
        {
          uint64_t expr_id = select_stmt->get_partition_expr_id(i);
          ObRawExpr *op_expr = logical_plan_->get_expr(expr_id)->get_expr();
          ret = rewrite_independ_expr(select_stmt,op_expr,text,pos);
          if(ret == OB_SUCCESS)
          {
            if(i < partition_expr_size - 1)
            {
              databuff_printf(text,OB_MAX_SQL_LENGTH,pos,", ");
            }
          }
        }
        databuff_printf(text,OB_MAX_SQL_LENGTH,pos," ");
        if(order_by_size > 0)
        {
          databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"order by ");
        }
        for(int32_t i=0;ret==OB_SUCCESS && i<order_by_size;i++)
        {
          const OrderItem &order_item = select_stmt->get_order_item_for_rownum(i);
          uint64_t expr_id = order_item.expr_id_;
          ObRawExpr *op_expr = logical_plan_->get_expr(expr_id)->get_expr();
          ret = rewrite_independ_expr(select_stmt,op_expr,text,pos);
          if(ret == OB_SUCCESS)
          {
            databuff_printf(text,OB_MAX_SQL_LENGTH,pos," %s",
                            (order_item.order_type_ == OrderItem::ASC ? "asc" : "desc"));
            if(i < order_by_size - 1)
            {
              databuff_printf(text,OB_MAX_SQL_LENGTH,pos,", ");
            }
          }
        }
        if(order_by_size > 0)
        {
          databuff_printf(text,OB_MAX_SQL_LENGTH,pos," ");
        }
        if(partition_expr_size >0 || order_by_size >0 )
        {
          databuff_printf(text,OB_MAX_SQL_LENGTH,pos,")");
        }
      }
      else if (expr_type == T_FUN_LISTAGG)
      {
        databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"listagg (");
        ObRawExpr *param_expr = agg_expr->get_param_expr();
        ret = rewrite_independ_expr(select_stmt,param_expr,text,pos);
        if(ret == OB_SUCCESS)
        {
          ObRawExpr *listagg_param_delimeter = agg_expr->get_listagg_param_delimeter();//T_STRING
          if(listagg_param_delimeter != NULL)
          {
            databuff_printf(text,OB_MAX_SQL_LENGTH,pos,",");
            ret = rewrite_independ_expr(select_stmt,listagg_param_delimeter,text,pos);
            databuff_printf(text,OB_MAX_SQL_LENGTH,pos,")");
          }
          else
          {
            databuff_printf(text,OB_MAX_SQL_LENGTH,pos,")");
          }
        }

        int32_t order_by_size = select_stmt->get_order_item_for_listagg_size();
        if(order_by_size >0)
        {
          databuff_printf(text,OB_MAX_SQL_LENGTH,pos," within group (order by ");
          for (int32_t i=0;ret==OB_SUCCESS && i<order_by_size;i++)
          {
            const OrderItem &order_item = select_stmt->get_order_item_for_listagg(i);
            uint64_t expr_id = order_item.expr_id_;
            ObRawExpr *op_expr = logical_plan_->get_expr(expr_id)->get_expr();
            ret = rewrite_independ_expr(select_stmt,op_expr,text,pos);
            if(ret == OB_SUCCESS)
            {
              databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"%s",
                              (order_item.order_type_==OrderItem::ASC ? "asc" : "desc"));
              if(i<order_by_size-1)
              {
                databuff_printf(text,OB_MAX_SQL_LENGTH,pos,", ");
              }
            }
          }
          databuff_printf(text,OB_MAX_SQL_LENGTH,pos,")");
        }
      }
      else
      {
        switch(expr_type)
        {
          case T_FUN_COUNT:
            databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"count(");
            break;
          case T_FUN_SUM:
            databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"sum(");
            break;
          case T_FUN_AVG:
            databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"avg(");
            break;
          case T_FUN_MAX:
            databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"max(");
            break;
          case T_FUN_MIN:
            databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"min(");
            break;
          default:
            break;
        }
        ObRawExpr *param_expr = agg_expr->get_param_expr();
        if(param_expr !=NULL)
        {
          ret = rewrite_independ_expr(select_stmt,param_expr,text,pos);
        }
        else if(param_expr == NULL && expr_type == T_FUN_COUNT)//count(*)
        {
          databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"*");
        }
        databuff_printf(text,OB_MAX_SQL_LENGTH,pos,")");
      }
      break;
    }
    case T_FUN_SYS:
    {
      ObSysFunRawExpr *func_expr = dynamic_cast<ObSysFunRawExpr *> (expr);
      const common::ObString &func_name = func_expr->get_func_name();
      int32_t sys_func_id = -1;
      for (int32_t id=SYS_FUNC_LENGTH;id<SYS_FUNC_NUM;id++)
      {
        const char *name = oceanbase::sql::ObPostfixExpression::get_sys_func_name(static_cast<ObSqlSysFunc>(id));
        if(static_cast<int32_t>(strlen(name))==func_name.length()
           && strncasecmp(name,func_name.ptr(),func_name.length())==0)
        {
          sys_func_id = id;
          break;
        }
      }
      if(sys_func_id == -1)
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(WARN,"find sys func failed");
        break;
      }

      if(SYS_FUNC_TRIM==sys_func_id)
      {
        databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"trim(");
        //TRIM([[{BOTH | LEADING | TRAILING}] [remstr] FROM] str)
        //child_[0]:{BOTH | LEADING | TRAILING}
        ObRawExpr *param_expr = func_expr->get_param_expr(0);
        OB_ASSERT(param_expr);
        ObObj value;
        ObConstRawExpr *c_expr = dynamic_cast<ObConstRawExpr *> (param_expr);
        value = c_expr->get_value();
        int64_t val = 0 ;
        ret = value.get_int(val);
        if(ret == OB_SUCCESS)
        {
          switch(val)
          {
            case 0:
              databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"both ");
              break;
            case 1:
              databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"leading ");
              break;
            case 2:
              databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"trailing ");
              break;
            default:
              break;
          }
        }
        //child_[1]:[remstr]
        //child_[2]:str
        int32_t param_size = func_expr->get_param_size();
        for(int32_t i=1;ret==OB_SUCCESS && i<param_size;i++)
        {
          ObRawExpr *param_expr = func_expr->get_param_expr(i);
          if(param_expr !=NULL)
          {
            ret = rewrite_independ_expr(select_stmt,param_expr,text,pos);
          }
          if(ret == OB_SUCCESS && i<param_size -1)
          {
            databuff_printf(text,OB_MAX_SQL_LENGTH,pos," from ");
          }
        }
      }
      else if ( SYS_FUNC_CAST == sys_func_id)
      {
        databuff_printf(text,OB_MAX_SQL_LENGTH,pos,"cast(");
        int32_t param_size = func_expr->get_param_size();
        //cast (expr as data_type)
        ObRawExpr *param_expr = func_expr->get_param_expr(0);
        OB_ASSERT(param_expr);
        //expr
        ret = rewrite_independ_expr(select_stmt, param_expr,text,pos);
        if(ret != OB_SUCCESS)
        {
          YYSYS_LOG(WARN,"rewrite cast expr failed,ret=%d",ret);
          break;
        }
        //data_type
        databuff_printf(text,OB_MAX_SQL_LENGTH,pos," as ");
        ObRawExpr *type_expr = func_expr->get_param_expr(1);
        OB_ASSERT(type_expr);
        ObObj value;
        ObConstRawExpr *c_expr = dynamic_cast<ObConstRawExpr *> (type_expr);
        value = c_expr->get_value();
        int64_t type = 0;
        ret = value.get_int(type);
        if(ret == OB_SUCCESS)
        {
          switch(type)
          {
            case T_TYPE_INTEGER:
              databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "integer");
              break;
            case T_TYPE_BIG_INTEGER:
              databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "bigint");
              break;
            case T_TYPE_DECIMAL:
            {
              databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "decimal(");
              if (func_expr->get_param_size() == 4)
              {
                ret = rewrite_independ_expr(select_stmt, func_expr->get_param_expr(2), text, pos);
                if (ret == OB_SUCCESS)
                {
                  databuff_printf(text, OB_MAX_SQL_LENGTH, pos, ", ");
                  ret = rewrite_independ_expr(select_stmt, func_expr->get_param_expr(3), text, pos);
                }
                databuff_printf(text, OB_MAX_SQL_LENGTH, pos, ")");
              }
              else
              {
                databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "38, 0)");
              }
              break;
            }
            case T_TYPE_BOOLEAN:
              databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "boolean");
              break;
            case T_TYPE_FLOAT:
            {
              //expr as float ( num )
              databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "float");
              ObRawExpr *second_param = func_expr->get_param_expr(2);
              if (param_size == 3 && second_param != NULL)//num �п���Ϊnull
              {
                databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "(");
                ret = rewrite_independ_expr(select_stmt, second_param, text, pos);
                databuff_printf(text, OB_MAX_SQL_LENGTH, pos, ")");
              }
              break;
            }
            case T_TYPE_DOUBLE:
              databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "double");
              break;
            case T_TYPE_TIMESTAMP:
            case T_TYPE_CHARACTER:
            case T_TYPE_VARCHAR:
            case T_TYPE_TIME:
            {
              switch (type)
              {
                case T_TYPE_TIMESTAMP:
                  databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "timestamp");
                  break;
                case T_TYPE_CHARACTER:
                  databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "character");
                  break;
                case T_TYPE_VARCHAR:
                  databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "varchar");
                  if (0 < func_expr->get_length() && func_expr->get_length() < OB_MAX_VARCHAR_LENGTH)
                  {
                    databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "(%u)", func_expr->get_length());
                  }
                  else
                  {
                    ret = OB_INVALID_ARGUMENT;
                    YYSYS_LOG(WARN, "invalid varchar length[%u]", func_expr->get_length());
                  }
                  break;
                case T_TYPE_TIME:
                  databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "time");
                  break;
                default:
                  break;
              }
              break;
            }
            case T_TYPE_CREATETIME:
              databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "createtime");
              break;
            case T_TYPE_MODIFYTIME:
              databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "modifytime");
              break;
            case T_TYPE_DATE:
              databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "date");
              break;
            default:
              ret = OB_ERR_UNEXPECTED;
              YYSYS_LOG(WARN, "unexpected error");
              break;
          }
        }
      }
      else if (SYS_FUNC_DATE_ADD_YEAR <= sys_func_id
               && sys_func_id <= SYS_FUNC_DATE_SUB_MICROSECOND)
      {
        //date_add(expr, INTERVAL expr type)
        const char *sys_func_name = oceanbase::sql::ObPostfixExpression::get_sys_func_name(static_cast<ObSqlSysFunc>(sys_func_id));
        if (NULL == strstr(sys_func_name, "add"))
        {
          databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "date_sub(");
        }
        else
        {
          databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "date_add(");
        }
        const ObItemType& op_type = func_expr->get_date_op_type();
        switch (op_type)
        {
          //[735]
          case T_CUR_TIME:
          case T_CUR_DATE:
          case T_CUR_TIME_UPS:
          case T_CUR_TIME_OP:
          case T_CUR_TIME_HMS:
          {
            ObRawExpr *param_expr = func_expr->get_param_expr(0);
            if(param_expr != NULL)
            {
              ret = rewrite_independ_expr(select_stmt, param_expr, text, pos);
            }
            break;
          }
          case T_NULL:
          {
            databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "null");
            break;
          }
          case T_DATE:
          {
            databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "timestamp \'%.*s\'", func_expr->get_datetime().length(), func_expr->get_datetime().ptr());
            break;
          }
          case T_TIME:
          {
            databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "time \'%.*s\'", func_expr->get_datetime().length(), func_expr->get_datetime().ptr());
            break;
          }
          case T_DATE_NEW:
          {
            databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "date \'%.*s\'", func_expr->get_datetime().length(), func_expr->get_datetime().ptr());
            break;
          }
          case T_IDENT:
          case T_OP_NAME_FIELD:
          {
            ObRawExpr *param_expr = func_expr->get_param_expr(0);
            if (NULL != param_expr)
            {
              ret = rewrite_independ_expr(select_stmt, param_expr, text, pos);
            }
            break;
          }
          default:
            //[735]
            if(!func_expr->param_is_sys_func())
            {
              ObRawExpr *param_expr = func_expr->get_param_expr(0);
              if (NULL != param_expr)
              {
                ret = rewrite_independ_expr(select_stmt, param_expr, text, pos);
              }
              break;
            }
            ret = OB_ERR_UNEXPECTED;
            YYSYS_LOG(WARN, "unexpected error");
            break;
        }
        if (ret == OB_SUCCESS)
        {
          databuff_printf(text, OB_MAX_SQL_LENGTH, pos, ", interval ");
          //T_OP_MUL
          ObBinaryOpRawExpr *b_expr = dynamic_cast<ObBinaryOpRawExpr *>(func_expr->get_param_expr(1));
          ObRawExpr *first_expr = b_expr->get_first_op_expr();
          ret = rewrite_independ_expr(select_stmt, first_expr, text, pos);
          if (ret != OB_SUCCESS)
          {
            break;
          }
          databuff_printf(text, OB_MAX_SQL_LENGTH, pos, " ");
          switch (sys_func_id)
          {
            case SYS_FUNC_DATE_ADD_YEAR:
            case SYS_FUNC_DATE_SUB_YEAR:
              databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "years");
              break;
            case SYS_FUNC_DATE_ADD_MONTH:
            case SYS_FUNC_DATE_SUB_MONTH:
              databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "months");
              break;
            case SYS_FUNC_DATE_ADD_DAY:
            case SYS_FUNC_DATE_SUB_DAY:
              databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "days");
              break;
            case SYS_FUNC_DATE_ADD_HOUR:
            case SYS_FUNC_DATE_SUB_HOUR:
              databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "hours");
              break;
            case SYS_FUNC_DATE_ADD_MINUTE:
            case SYS_FUNC_DATE_SUB_MINUTE:
              databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "minutes");
              break;
            case SYS_FUNC_DATE_ADD_SECOND:
            case SYS_FUNC_DATE_SUB_SECOND:
              databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "seconds");
              break;
            case SYS_FUNC_DATE_ADD_MICROSECOND:
            case SYS_FUNC_DATE_SUB_MICROSECOND:
              databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "microseconds");
              break;
            default:
              ret = OB_ERR_UNEXPECTED;
              YYSYS_LOG(WARN, "unexpected error");
              break;
          }
        }
      }
      else
      {
        databuff_printf(text, OB_MAX_SQL_LENGTH, pos, "%.*s(", func_name.length(), func_name.ptr());
        int32_t param_size = func_expr->get_param_size();
        for (int32_t i=0 ; ret == OB_SUCCESS && i<param_size ; i++)
        {
          ObRawExpr *param_expr = func_expr->get_param_expr(i);
          if (param_expr != NULL)
          {
            ret = rewrite_independ_expr(select_stmt, param_expr, text, pos);
          }
          if (ret == OB_SUCCESS && i < param_size -1)
          {
            databuff_printf(text, OB_MAX_SQL_LENGTH, pos, ", ");
          }
        }
      }
      databuff_printf(text, OB_MAX_SQL_LENGTH, pos, ")");
      break;
    }
    default:
      ret = OB_INVALID_ARGUMENT;
      YYSYS_LOG(WARN, "won't be here, expr_type = [%d]", expr_type);
      break;
  }

  return ret;
}

void ObRewriteQueryStmt::split(const char *complete_name, char *db_name, char *table_name, bool &is_sys_table)
{
  size_t len = strlen(complete_name);
  bool is_table_name = false;
  size_t table_idx = 0, db_idx = 0;
  for (size_t i=0 ; i<len ; i++)
  {
    if (complete_name[i] == '.')
    {
      is_table_name = true;
      continue;
    }
    if (is_table_name)
    {
      table_name[table_idx ++] = complete_name[i];
    }
    else
    {
      db_name[db_idx ++] = complete_name[i];
    }
  }
  db_name[db_idx] = '\0';
  table_name[table_idx] = '\0';
  if (!is_table_name)
  {
    is_sys_table = true;
  }
}
