#include <map>
#include "sql/ob_optimizer_logical.h"
#include "sql/ob_select_stmt.h"
#include "sql/ob_explain_stmt.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_vector.h"
#include "parse_malloc.h"
#include "sql/ob_optimizer_relation.h"

namespace oceanbase
{
  namespace  sql
  {
    bool ObOptimizerLogical::is_simple_subquery(ObSelectStmt *select_stmt)
    {
      bool ret = false;
      if(!(select_stmt->get_stmt_type() == ObSelectStmt::T_SELECT))
      {
      }
      else if(select_stmt->is_for_update())
      {
      }
      else if(select_stmt->get_set_op() != ObSelectStmt::NONE)
      {
      }
      else if(select_stmt->has_limit() ||
              select_stmt->is_distinct() ||
              select_stmt->is_show_stmt() ||
              select_stmt->has_limit() ||
              select_stmt->get_group_expr_size()>0 ||
              select_stmt->get_agg_fun_size() >0 ||
              select_stmt->get_having_expr_size() >0 ||
              select_stmt->get_order_item_size() >0 ||
              select_stmt->get_when_expr_size() >0 ||
              select_stmt->get_has_range() ||
              select_stmt->has_sequence()
              )
      {
      }
      else if(select_stmt->get_anal_fun_size() >0
              || select_stmt->get_partition_expr_size()>0
              || select_stmt->get_order_item_for_rownum_size()>0)
      {
      }
      else if(select_stmt->get_query_hint().is_has_hint_ == true)
      {
      }
      else
      {
        ret = true;
      }
      return ret;
    }

    int ObOptimizerLogical::pull_up_subqueries(ObLogicalPlan *&logical_plan, ObResultSet &result, ResultPlan &result_plan)
    {
      int ret = OB_SUCCESS;
      uint64_t query_id = common::OB_INVALID_ID;
      ObSelectStmt *main_stmt = NULL;
      std::map<uint64_t,uint64_t>father_alias_table;
      if(logical_plan)
      {
        int32_t stmt_size = logical_plan->get_stmts_count();
        if(stmt_size >1)
        {
          if(query_id == OB_INVALID_ID)
          {
            ObBasicStmt *basic_stmt = logical_plan->get_main_stmt();
            if(basic_stmt->get_stmt_type() == ObBasicStmt::T_EXPLAIN)
            {
              ObExplainStmt *explain_stmt = dynamic_cast<ObExplainStmt*>(basic_stmt);
              main_stmt = dynamic_cast<ObSelectStmt*>(
                            logical_plan->get_query(explain_stmt->get_explain_query_id()));
            }
            else
            {
              main_stmt = dynamic_cast<ObSelectStmt*>(basic_stmt);
            }
          }
          if(main_stmt == NULL)
          {
            ret = OB_ERR_ILLEGAL_ID;
            YYSYS_LOG(INFO, "Stmt is not select stmt, so it will not enter the logical optimizer");
          }
          else if(OB_LIKELY(ret == OB_SUCCESS))
          {
            if(!main_stmt->is_for_update()
               && main_stmt->get_stmt_type() == ObSelectStmt::T_SELECT
               && main_stmt->get_query_hint().is_has_hint_ == false)
            {
              ret = pull_up_subquery_union(logical_plan,main_stmt,result_plan,father_alias_table);
            }
          }
        }
      }
      UNUSED(result);
      return ret;
    }

    int ObOptimizerLogical::pull_up_subquery_union(ObLogicalPlan *&logical_plan, ObSelectStmt *&main_stmt,
                                                   ResultPlan &result_plan, std::map<uint64_t, uint64_t> &father_alias_table)
    {
      int ret = OB_SUCCESS;
      if(main_stmt->get_set_op() != ObSelectStmt::NONE)
      {
        uint64_t left_query_id = main_stmt->get_left_query_id();
        uint64_t right_query_id = main_stmt->get_right_query_id();
        ObSelectStmt *left_stmt = dynamic_cast<ObSelectStmt*>(logical_plan->get_query(left_query_id));
        ObSelectStmt *right_stmt = dynamic_cast<ObSelectStmt*>(logical_plan->get_query(right_query_id));
        ret = split_mainquery_from_items(logical_plan,left_stmt,result_plan);
        int32_t left_from_item_size = left_stmt->get_from_item_size();
        for(int32_t i=0;i<left_from_item_size;++i)
        {
          FromItem& from_item = const_cast<FromItem&>(left_stmt->get_from_item(i));
          pull_up_subquery_recursion(logical_plan,left_stmt,from_item,result_plan,father_alias_table,i);
        }
        ret = split_mainquery_from_items(logical_plan,right_stmt,result_plan);
        int32_t right_from_item_size = right_stmt->get_from_item_size();
        for(int32_t i=0;i<right_from_item_size;++i)
        {
          FromItem& from_item = const_cast<FromItem&>(right_stmt->get_from_item(i));
          pull_up_subquery_recursion(logical_plan,right_stmt,from_item,result_plan,father_alias_table,i);
        }
        common::ObVector<SelectItem>& main_select_items= main_stmt->get_select_items();
        common::ObVector<SelectItem>& left_select_items= left_stmt->get_select_items();
        for(int32_t i=0;i<main_select_items.size();++i)
        {
          main_select_items.at(i) = left_select_items.at(i);
        }
      }
      else
      {
        ret = split_mainquery_from_items(logical_plan,main_stmt,result_plan);
        int32_t from_item_size = main_stmt->get_from_item_size();
        for(int32_t i=0;i<from_item_size;++i)
        {
          FromItem& from_item = const_cast<FromItem&>(main_stmt->get_from_item(i));
          pull_up_subquery_recursion(logical_plan,main_stmt,from_item,result_plan,father_alias_table,i);
        }
      }
      ret = set_main_query_alias_table(logical_plan,main_stmt,result_plan,father_alias_table);
      return ret;
    }

    int ObOptimizerLogical::split_mainquery_from_items(ObLogicalPlan *&logical_plan,
                                                       ObSelectStmt *&main_stmt, ResultPlan &result_plan)
    {
      int ret = OB_SUCCESS;
      bool flag = false;
      common::ObVector<FromItem>& from_items = main_stmt->get_from_items();
      common::ObVector<uint64_t>& where_exprs = main_stmt->get_where_exprs();
      for(int32_t i=0;i<main_stmt->get_from_item_size();++i)
      {
        flag = false;
        FromItem& from_item = const_cast<FromItem&>(main_stmt->get_from_item(i));
        if(from_item.is_joined_)
        {
          uint64_t joined_table_id = from_item.table_id_;
          JoinedTable* joined_table = main_stmt->get_joined_table(joined_table_id);
          int64_t joined_type_size = joined_table->get_join_types().count();
          for(int64_t j=0;j< joined_type_size;++j)
          {
            if(joined_table->get_join_types().at(j) != JoinedTable::T_INNER)
            {
              flag = true;
              break;
            }
          }
          if(flag == true)
          {
            continue;
          }
          else
          {
            common::ObArray<uint64_t>& joined_table_ids = joined_table->get_table_ids();
            common::ObArray<uint64_t>& joined_expr_ids = joined_table->get_expr_ids();
            common::ObArray<int64_t>& joined_expr_nums = joined_table->get_expr_nums_per_join();
            for(int64_t j= joined_table_ids.count() -1;j>=0;--j)
            {
              FromItem new_from_item;
              new_from_item.table_id_ = joined_table_ids.at(j);
              new_from_item.is_joined_ = false;
              new_from_item.from_item_rel_opt_ = NULL;
              from_items.insert(from_items.begin()+i,new_from_item);
              if(j>0)
              {
                int64_t joined_expr_ids_index = joined_table->get_expr_ids_index(j-1);
                for(int64_t k=0;k<joined_expr_nums.at(j-1);++k)
                {
                  uint64_t expr_id = joined_expr_ids.at(joined_expr_ids_index +k);
                  where_exprs.push_back(expr_id);
                }
              }
            }
            int64_t count = joined_table_ids.count();
            ret = main_stmt->remove_joined_table(joined_table_id);
            if(ret != OB_SUCCESS)
            {
              YYSYS_LOG(WARN,"DHC LXB main_stmt->remove_joined_table ret=%d",ret);
            }
            ret = main_stmt->remove_from_item(i+static_cast<int32_t>(count));
            if(from_item.from_item_rel_opt_ != NULL)
            {
              from_item.from_item_rel_opt_->~ObOptimizerRelation();
            }
            if(ret != OB_SUCCESS)
            {
              YYSYS_LOG(WARN,"DHC LXB main_stmt->remove_from_item ret=%d",ret);
            }
          }
        }
      }
      UNUSED(logical_plan);
      UNUSED(result_plan);
      return ret;
    }

    int ObOptimizerLogical::set_main_query_alias_table(ObLogicalPlan *&logical_plan,
                                                       ObSelectStmt *&main_stmt, ResultPlan &result_plan,
                                                       std::map<uint64_t, uint64_t> &father_alias_table)
    {
      int ret = OB_SUCCESS;
      std::map<uint64_t,uint64_t> column_id_hashmap;
      std::map<uint64_t,uint64_t> table_id_hashmap;
      std::map<uint64_t,uint64_t>::iterator father_alias_it;
      for(father_alias_it = father_alias_table.begin();father_alias_it != father_alias_table.end(); ++father_alias_it)
      {
        uint64_t ref_id = father_alias_it->first;
        uint64_t gen_aid = logical_plan->generate_table_id();
        father_alias_table[ref_id] = gen_aid;
        bool flag = false;
        common::ObRowDesc* table_hash = main_stmt->get_table_hash();
        table_hash->add_column_desc(gen_aid,OB_INVALID_ID);
        int32_t from_item_size = main_stmt->get_from_item_size();
        for(int32_t i=0;i<from_item_size;++i)
        {
          FromItem& from_item = const_cast<FromItem&>(main_stmt->get_from_item(i));
          if(from_item.is_joined_)
          {
            uint64_t joined_table_id = from_item.table_id_;
            JoinedTable* joined_table = main_stmt->get_joined_table(joined_table_id);
            common::ObArray<uint64_t>& joined_table_ids = joined_table->get_table_ids();
            common::ObArray<uint64_t>& joined_expr_ids = joined_table->get_expr_ids();
            for(int64_t j=0; j< joined_table_ids.count(); ++j)
            {
              if(joined_table_ids.at(j) == ref_id)
              {
                joined_table_ids.at(j) = gen_aid;
                flag = true;
              }
            }
            if(flag == true)
            {
              for(int64_t j=0; j< joined_expr_ids.count(); ++j)
              {
                ObSqlRawExpr* sql_raw_expr = const_cast<ObSqlRawExpr*>(logical_plan->get_expr(joined_expr_ids.at(j)));
                sql_raw_expr->optimize_sql_expression(main_stmt,
                                                      table_id_hashmap,column_id_hashmap,ref_id,gen_aid, father_alias_table,3);
              }
            }
          }
          else

          {
            if(from_item.table_id_ == ref_id)
            {
              from_item.table_id_ = gen_aid;
              flag = true;
            }
          }
        }

        if(flag == false)
        {
          continue;
        }
        uint64_t gen_idx = logical_plan->generate_alias_table_id();
        ObString new_alias_name;
        ob_write_string(*logical_plan->get_name_pool(),
                        ObString::link_string("alias",gen_idx),
                        new_alias_name);
        int32_t table_size = main_stmt->get_table_size();
        for(int32_t i=0;i< table_size;++i)
        {
          TableItem& table_item = main_stmt->get_table_item(i);
          if(table_item.table_id_ == ref_id)
          {
            table_item.table_id_ = gen_aid;
            table_item.alias_name_ = new_alias_name;
            table_item.type_ = TableItem::ALIAS_TABLE;
          }
        }
        int32_t column_size = main_stmt->get_column_size();
        for(int32_t i=0;i< column_size;++i)
        {
          ColumnItem* column_item = const_cast<ColumnItem*>(main_stmt->get_column_item(i));
          if(column_item->table_id_ == ref_id)
          {
            column_item->table_id_ = gen_aid;
            main_stmt->get_table_item_by_id(gen_aid)->has_scan_columns_ = true;
          }
        }

        int32_t select_size = main_stmt->get_select_item_size();
        for(int32_t i=0;i<select_size;++i)
        {
          SelectItem& select_item = const_cast<SelectItem&>(main_stmt->get_select_item(i));
          uint64_t expr_id = select_item.expr_id_;
          ObSqlRawExpr* sql_raw_expr = const_cast<ObSqlRawExpr*>(logical_plan->get_expr(expr_id));
          sql_raw_expr->optimize_sql_expression(main_stmt,
                                                table_id_hashmap,column_id_hashmap,ref_id,gen_aid,father_alias_table,3);
        }
        int32_t condition_size = main_stmt->get_condition_size();
        for(int32_t i=0;i< condition_size;++i)
        {
          uint64_t expr_id = main_stmt->get_condition_id(i);
          ObSqlRawExpr* sql_raw_expr = const_cast<ObSqlRawExpr*>(logical_plan->get_expr(expr_id));
          sql_raw_expr->optimize_sql_expression(main_stmt,
                                                table_id_hashmap,column_id_hashmap,ref_id,gen_aid,father_alias_table,3);
        }

      }
      UNUSED(result_plan);
      return ret;
    }

    int ObOptimizerLogical::pull_up_subquery_recursion(ObLogicalPlan *&logical_plan,
                                                       ObSelectStmt *&main_stmt, FromItem &from_item, ResultPlan &result_plan,
                                                       std::map<uint64_t, uint64_t> &father_alias_table, int32_t from_item_idx)
    {
      int ret = OB_SUCCESS;
      uint64_t table_id = from_item.table_id_;
      bool is_joined = from_item.is_joined_;
      JoinedTable* joined_table = NULL;
      uint64_t join_type = JoinedTable::T_INNER;
      if(is_joined)
      {
        joined_table = main_stmt->get_joined_table(table_id);
        for(int64_t i=0;i<joined_table->table_ids_.count();++i)
        {
          if(i>0 && joined_table->get_join_types().at(i-1) != JoinedTable::T_INNER)
          {
            join_type = joined_table->get_join_types().at(i-1);
            break;
          }
          else if(i==0 && joined_table->get_join_types().at(0) != JoinedTable::T_INNER)
          {
            join_type = joined_table->get_join_types().at(0);
            break;
          }
        }
        for(int64_t i=0;i<joined_table->table_ids_.count();++i)
        {
          uint64_t joined_table_id = joined_table->table_ids_.at(i);
          TableItem* table_item = main_stmt->get_table_item_by_id(joined_table_id);
          if(table_item->type_ == TableItem::GENERATED_TABLE)
          {
            uint64_t ref_id = table_item->ref_id_;
            ObSelectStmt* subquery_stmt = dynamic_cast<ObSelectStmt*>(logical_plan->get_query(ref_id));
            if(subquery_stmt == NULL)
            {
              ret = OB_ERR_ILLEGAL_ID;
              YYSYS_LOG(ERROR, "Get Stmt error");
            }
            else if(OB_LIKELY(ret == OB_SUCCESS))
            {
              ret = pull_up_simple_subquery(logical_plan,main_stmt,from_item,
                                            table_item, joined_table,join_type,result_plan,father_alias_table,from_item_idx);
            }
            else
            {

            }
          }
        }
      }
      else
      {
        TableItem* table_item = main_stmt->get_table_item_by_id(table_id);
        if(table_item->type_ == TableItem::GENERATED_TABLE)
        {
          uint64_t ref_id = table_item->ref_id_;
          ObSelectStmt* subquery_stmt = dynamic_cast<ObSelectStmt*>(logical_plan->get_query(ref_id));
          if(subquery_stmt == NULL)
          {
            ret = OB_ERR_ILLEGAL_ID;
            YYSYS_LOG(ERROR, "Get Stmt error");
          }
          else if(OB_LIKELY(ret == OB_SUCCESS))
          {
            ret = pull_up_simple_subquery(logical_plan,main_stmt,from_item,table_item,
                                          joined_table,join_type,result_plan,father_alias_table,from_item_idx);
          }
          else
          {

          }
        }
      }
      return ret;
    }

    int ObOptimizerLogical::if_can_pull_up(
        ObLogicalPlan *&logical_plan, ObSelectStmt *subquery_stmt,
        JoinedTable *&joined_table, uint64_t join_type, bool is_joined,
        uint64_t table_id, ObSelectStmt *&main_stmt)
    {
      int ret = OB_SUCCESS;
      int32_t sub_from_size = subquery_stmt->get_from_item_size();
      for(int32_t i=0;i<sub_from_size;++i)
      {
        FromItem& sub_from_item = const_cast<FromItem&>(subquery_stmt->get_from_item(i));
        bool sub_is_joined = sub_from_item.is_joined_;
        if(is_joined)
        {
          if(sub_is_joined)
          {
            JoinedTable* sub_joined_table = subquery_stmt->get_joined_table(sub_from_item.table_id_);
            if(join_type != JoinedTable::T_INNER)
            {
              ret = OB_SQL_CAN_NOT_PULL_UP;
            }
            else
            {
              for(int64_t j=0;j<sub_joined_table->get_join_types().count();++j)
              {
                if(sub_joined_table->get_join_types().at(j) != JoinedTable::T_INNER)
                {
                  ret =OB_SQL_CAN_NOT_PULL_UP;
                  break;
                }
              }
            }
          }
          else
          {
            if(join_type != JoinedTable::T_INNER)
            {
              ret = OB_SQL_CAN_NOT_PULL_UP;
            }
          }
        }
        else
        {
          if(sub_is_joined)
          {
            JoinedTable* sub_joined_table = subquery_stmt->get_joined_table(sub_from_item.table_id_);
            for(int64_t j=0;j<sub_joined_table->get_join_types().count();++j)
            {
              if(sub_joined_table->get_join_types().at(j) != JoinedTable::T_INNER)
              {
                ret =OB_SQL_CAN_NOT_PULL_UP;
                break;
              }
            }
          }
        }

        if(ret != OB_SUCCESS)
        {
          YYSYS_LOG(DEBUG, "SQL can not be pulled up because of out join");
          break;
        }
      }

      if(ret == OB_SUCCESS)
      {
        int32_t sub_select_item_size = subquery_stmt->get_select_item_size();
        for(int32_t i=0;i<sub_select_item_size;++i)
        {
          SelectItem& sub_select_item = const_cast<SelectItem&>(subquery_stmt->get_select_item(i));
          ObSqlRawExpr* sql_raw_expr = logical_plan->get_expr(sub_select_item.expr_id_);
          ObRawExpr* raw_expr = sql_raw_expr->get_expr();
          ObItemType item_type = raw_expr->get_expr_type();
          if(item_type != T_REF_COLUMN)
          {
            ret = OB_SQL_CAN_NOT_PULL_UP;
            YYSYS_LOG(DEBUG, "SQL can not be pulled up because of select item");
            break;
          }
        }
        if(ret == OB_SUCCESS)
        {
          int32_t select_item_size = main_stmt->get_select_item_size();
          for(int32_t i=0;i<select_item_size;++i)
          {
            SelectItem& select_item = const_cast<SelectItem&>(main_stmt->get_select_item(i));
            ObSqlRawExpr* sql_raw_expr = logical_plan->get_expr(select_item.expr_id_);
            ObRawExpr* raw_expr = sql_raw_expr->get_expr();
            ObItemType item_type = raw_expr->get_expr_type();
            if(item_type != T_REF_COLUMN)
            {
              ret = OB_SQL_CAN_NOT_PULL_UP;
              YYSYS_LOG(DEBUG, "SQL can not be pulled up because of select item");
              break;
            }
          }
        }
      }
      if(ret == OB_SUCCESS)
      {
        int32_t condition_size = subquery_stmt->get_condition_size();
        for(int32_t i=0;i<condition_size;++i)
        {
          uint64_t condition_id = subquery_stmt->get_condition_id(i);
          ObSqlRawExpr* sql_raw_expr = logical_plan->get_expr(condition_id);
          ObRawExpr* raw_expr = sql_raw_expr->get_expr();
          ObItemType item_type = raw_expr->get_expr_type();
          if((item_type >= T_OP_NEG && item_type <= T_OP_NOT_IN)
             || item_type == T_OP_CNN)
          {
            if((item_type>= T_OP_EQ && item_type<= T_OP_NE)
               || item_type == T_OP_IN
               || item_type == T_OP_NOT_IN)
            {
              ObBinaryOpRawExpr* binary_op_raw_expr = dynamic_cast<ObBinaryOpRawExpr*>(raw_expr);
              if(binary_op_raw_expr == NULL)
              {
                ret = OB_SQL_CAN_NOT_PULL_UP;
                YYSYS_LOG(DEBUG, "SQL can not be pulled up because of condition");
                break;
              }
              ObRawExpr* left_expr = binary_op_raw_expr->get_first_op_expr();
              ObRawExpr* right_expr = binary_op_raw_expr->get_second_op_expr();
              if(left_expr == NULL || right_expr == NULL)
              {
                ret = OB_SQL_CAN_NOT_PULL_UP;
                YYSYS_LOG(DEBUG, "SQL can not be pulled up because of condition");
                break;
              }
              else if(left_expr->get_expr_type() != T_REF_COLUMN
                      || right_expr->get_expr_type() == T_REF_QUERY)
              {
                ret = OB_SQL_CAN_NOT_PULL_UP;
                YYSYS_LOG(DEBUG, "SQL can not be pulled up because of condition");
                break;
              }
            }
          }
          else
          {
            ret = OB_SQL_CAN_NOT_PULL_UP;
            YYSYS_LOG(DEBUG, "SQL can not be pulled up because of condition");
            break;
          }
        }
      }

      if(ret == OB_SUCCESS)
      {
        int32_t condition_size = main_stmt->get_condition_size();
        for(int32_t i=0;i<condition_size;++i)
        {
          uint64_t condition_id = main_stmt->get_condition_id(i);
          ObSqlRawExpr* sql_raw_expr = logical_plan->get_expr(condition_id);
          ObRawExpr* raw_expr = sql_raw_expr->get_expr();
          ObItemType item_type = raw_expr->get_expr_type();
          if((item_type >= T_OP_NEG && item_type <= T_OP_NOT_IN)
             || item_type == T_OP_CNN)
          {
            if((item_type>= T_OP_EQ && item_type<= T_OP_NE)
               || item_type == T_OP_IN
               || item_type == T_OP_NOT_IN)
            {
              ObBinaryOpRawExpr* binary_op_raw_expr = dynamic_cast<ObBinaryOpRawExpr*>(raw_expr);
              if(binary_op_raw_expr == NULL)
              {
                ret = OB_SQL_CAN_NOT_PULL_UP;
                YYSYS_LOG(DEBUG, "SQL can not be pulled up because of condition");
                break;
              }
              ObRawExpr* left_expr = binary_op_raw_expr->get_first_op_expr();
              ObRawExpr* right_expr = binary_op_raw_expr->get_second_op_expr();
              if(left_expr == NULL || right_expr == NULL)
              {
                ret = OB_SQL_CAN_NOT_PULL_UP;
                YYSYS_LOG(DEBUG, "SQL can not be pulled up because of condition");
                break;
              }
              else if(left_expr->get_expr_type() != T_REF_COLUMN
                      || right_expr->get_expr_type() == T_REF_QUERY)
              {
                ret = OB_SQL_CAN_NOT_PULL_UP;
                YYSYS_LOG(DEBUG, "SQL can not be pulled up because of condition");
                break;
              }
            }
          }
          else
          {
            ret = OB_SQL_CAN_NOT_PULL_UP;
            YYSYS_LOG(DEBUG, "SQL can not be pulled up because of condition");
            break;
          }
        }
      }
      UNUSED(joined_table);
      UNUSED(table_id);
      return ret;
    }

    int ObOptimizerLogical::combine_subquery_from_items(ObLogicalPlan *&logical_plan,
                                                        ObSelectStmt* subquery_stmt, ResultPlan &result_plan, bool is_joined)
    {
      int ret = OB_SUCCESS;
      int32_t sub_from_size = subquery_stmt->get_from_item_size();
      if(is_joined && sub_from_size >1)
      {
        common::ObVector<uint64_t> join_exprs;
        for(int32_t i=0;i<sub_from_size;++i)
        {
          FromItem& sub_from_item = const_cast<FromItem&>(subquery_stmt->get_from_item(i));
          bool sub_is_joined = sub_from_item.is_joined_;
          if(sub_is_joined)
          {
            JoinedTable* sub_joined_table = subquery_stmt->get_joined_table(sub_from_item.table_id_);
            common::ObArray<uint64_t>& sub_joined_table_ids = sub_joined_table->get_table_ids();
            for(int64_t j=0;j< sub_joined_table_ids.count();++j)
            {
              common::ObBitSet<> table_bitset;
              int32_t bit_index = subquery_stmt->get_table_bit_index(sub_joined_table_ids.at(j));
              table_bitset.add_member(bit_index);
              for(int32_t k=0;k<subquery_stmt->get_condition_size();++k)
              {
                uint64_t sub_where_exprs_id = subquery_stmt->get_condition_id(k);
                ObSqlRawExpr* sql_raw_expr = logical_plan->get_expr(sub_where_exprs_id);
                common::ObBitSet<>& bit_set = sql_raw_expr->get_tables_set();
                if(table_bitset.is_superset(bit_set))
                {
                  sql_raw_expr->set_applied(true);
                }
              }
            }
          }
          else
          {
            common::ObBitSet<> table_bitset;
            int32_t bit_index = subquery_stmt->get_table_bit_index(sub_from_item.table_id_);
            table_bitset.add_member(bit_index);
            for(int32_t j=0;j< subquery_stmt->get_condition_size();++j)
            {
              uint64_t sub_where_exprs_id = subquery_stmt->get_condition_id(j);
              ObSqlRawExpr* sql_raw_expr = logical_plan->get_expr(sub_where_exprs_id);
              common::ObBitSet<>& bit_set = sql_raw_expr->get_tables_set();
              if(table_bitset.is_superset(bit_set))
              {
                sql_raw_expr->set_applied(true);
              }
            }
          }
        }
        common::ObVector<uint64_t>& sub_where_exprs = subquery_stmt->get_where_exprs();
        int32_t sub_condition_size = subquery_stmt->get_condition_size();
        for(int32_t i=sub_condition_size-1;i>=0;--i)
        {
          uint64_t sub_where_exprs_id = subquery_stmt->get_condition_id(i);
          ObSqlRawExpr* sql_raw_expr = logical_plan->get_expr(sub_where_exprs_id);
          if(!(sql_raw_expr->is_apply()))
          {
            join_exprs.push_back(sub_where_exprs_id);
            sub_where_exprs.remove(i);
          }
        }
        JoinedTable* new_joined_table = (JoinedTable*)parse_malloc(sizeof(JoinedTable),result_plan.name_pool_);
        new_joined_table = new(new_joined_table) JoinedTable;
        FromItem & sub_from_item = const_cast<FromItem&>(subquery_stmt->get_from_item(0));
        bool sub_is_joined = sub_from_item.is_joined_;
        if(sub_is_joined)
        {
          JoinedTable* sub_joined_table = subquery_stmt->get_joined_table(sub_from_item.table_id_);
          common::ObArray<uint64_t>& sub_joined_table_ids = sub_joined_table->get_table_ids();
          common::ObArray<uint64_t>& sub_expr_ids = sub_joined_table->get_expr_ids();
          common::ObArray<int64_t>& expr_nums_per_join = sub_joined_table->get_expr_nums_per_join();
          int64_t expr_nums_per_join_before =0;
          for(int64_t i=0;i<sub_joined_table_ids.count();++i)
          {
            new_joined_table->add_table_id(sub_joined_table_ids.at(i));
            if(i>0)
            {
              uint64_t sub_join_type = sub_joined_table->get_join_types().at(i-1);
              switch (sub_join_type)
              {
                case JoinedTable::T_FULL:
                  new_joined_table->add_join_type(JoinedTable::T_FULL);
                  break;
                case JoinedTable::T_INNER:
                  new_joined_table->add_join_type(JoinedTable::T_INNER);
                  break;
                case JoinedTable::T_LEFT:
                  new_joined_table->add_join_type(JoinedTable::T_LEFT);
                  break;
                case JoinedTable::T_RIGHT:
                  new_joined_table->add_join_type(JoinedTable::T_RIGHT);
                  break;
                default:
                  break;
              }
              ObVector<uint64_t>exprs;
              int64_t per_join_nums = expr_nums_per_join.at(i-1);
              OB_ASSERT(per_join_nums >=1);
              while (per_join_nums > 0)
              {
                exprs.push_back(sub_expr_ids.at(expr_nums_per_join_before));
                sub_expr_ids.at(expr_nums_per_join_before)=0;
                expr_nums_per_join_before +=1;
                per_join_nums -=1;
              }
              new_joined_table->add_join_exprs(exprs);
            }
          }
        }
        else
        {
          new_joined_table->add_table_id(sub_from_item.table_id_);
        }
        for(int64_t i=0;i<new_joined_table->get_table_ids().count();++i)
        {
          uint64_t new_table_id = new_joined_table->get_table_ids().at(i);
          common::ObBitSet<> table_bitset;
          int32_t bit_index = subquery_stmt->get_table_bit_index(new_table_id);
          table_bitset.add_member(bit_index);
          int32_t join_exprs_size = join_exprs.size();
          for(int32_t j=join_exprs_size-1;j>=0;--j)
          {
            ObSqlRawExpr* sql_raw_expr = logical_plan->get_expr(join_exprs.at(j));
            common::ObBitSet<>& bit_set = sql_raw_expr->get_tables_set();
            if(table_bitset.is_subset(bit_set))
            {
              ObRawExpr* raw_expr = sql_raw_expr->get_expr();
              ObBinaryOpRawExpr* binary_op_raw_expr = dynamic_cast<ObBinaryOpRawExpr*>(raw_expr);
              ObBinaryRefRawExpr* left_expr = dynamic_cast<ObBinaryRefRawExpr*>(binary_op_raw_expr->get_first_op_expr());
              ObBinaryRefRawExpr* right_expr = dynamic_cast<ObBinaryRefRawExpr*>(binary_op_raw_expr->get_second_op_expr());
              uint64_t left_first_ref_id = left_expr->get_first_ref_id();
              uint64_t right_first_ref_id = right_expr->get_first_ref_id();
              if(left_first_ref_id == new_table_id)
              {
                new_joined_table->add_table_id(right_first_ref_id);
              }
              else if(right_first_ref_id == new_table_id)
              {
                new_joined_table->add_table_id(left_first_ref_id);
              }
              new_joined_table->add_join_type(JoinedTable::T_INNER);
              ObVector<uint64_t> exprs;
              exprs.push_back(join_exprs.at(j));
              join_exprs.remove(j);
              for(int32_t k=0;k<join_exprs.size();++k)
              {
                ObSqlRawExpr* sql_raw_expr_same = logical_plan->get_expr(join_exprs.at(k));
                common::ObBitSet<>& bit_set_same = sql_raw_expr_same->get_tables_set();
                if(bit_set == bit_set_same)
                {
                  exprs.push_back(join_exprs.at(k));
                  join_exprs.remove(k);
                  j--;
                }
              }
              new_joined_table->add_join_exprs(exprs);
            }
          }

          for(int32_t j=0;j< sub_from_size;++j)
          {
            FromItem& sub_from_item = const_cast<FromItem&>(subquery_stmt->get_from_item(j));
            bool sub_is_joined = sub_from_item.is_joined_;
            if(sub_is_joined)
            {
              JoinedTable* sub_joined_table = subquery_stmt->get_joined_table(sub_from_item.table_id_);
              common::ObArray<uint64_t>& sub_expr_ids = sub_joined_table->get_expr_ids();
              common::ObArray<int64_t>& expr_nums_per_join = sub_joined_table->get_expr_nums_per_join();
              int64_t expr_nums_per_join_before = 0;
              for(int64_t k=0;k<expr_nums_per_join.count();++k)
              {
                ObVector<uint64_t> exprs;
                int64_t per_join_nums = expr_nums_per_join.at(k);
                OB_ASSERT(per_join_nums >=1);
                expr_nums_per_join_before = sub_joined_table->get_expr_ids_index(k);
                if(sub_expr_ids.at(expr_nums_per_join_before) >0)
                {
                  ObSqlRawExpr* sql_raw_expr = logical_plan->get_expr(sub_expr_ids.at(expr_nums_per_join_before));
                  common::ObBitSet<>& bit_set= sql_raw_expr->get_tables_set();
                  if(table_bitset.is_subset(bit_set))
                  {
                    ObRawExpr* raw_expr = sql_raw_expr->get_expr();
                    ObBinaryOpRawExpr* binary_op_raw_expr = dynamic_cast<ObBinaryOpRawExpr*>(raw_expr);
                    ObBinaryRefRawExpr* left_expr = dynamic_cast<ObBinaryRefRawExpr*>(binary_op_raw_expr->get_first_op_expr());
                    ObBinaryRefRawExpr* right_expr = dynamic_cast<ObBinaryRefRawExpr*>(binary_op_raw_expr->get_second_op_expr());
                    uint64_t left_first_ref_id = left_expr->get_first_ref_id();
                    uint64_t right_first_ref_id = right_expr->get_first_ref_id();
                    if(left_first_ref_id == new_table_id)
                    {
                      new_joined_table->add_table_id(right_first_ref_id);
                    }
                    else if(right_first_ref_id == new_table_id)
                    {
                      new_joined_table->add_table_id(left_first_ref_id);
                    }

                    uint64_t sub_join_type = sub_joined_table->get_join_types().at(k);
                    switch (sub_join_type)
                    {
                      case JoinedTable::T_FULL:
                        new_joined_table->add_join_type(JoinedTable::T_FULL);
                        break;
                      case JoinedTable::T_INNER:
                        new_joined_table->add_join_type(JoinedTable::T_INNER);
                        break;
                      case JoinedTable::T_LEFT:
                        new_joined_table->add_join_type(JoinedTable::T_LEFT);
                        break;
                      case JoinedTable::T_RIGHT:
                        new_joined_table->add_join_type(JoinedTable::T_RIGHT);
                        break;
                      default:
                        break;
                    }
                    while(per_join_nums>0)
                    {
                      exprs.push_back(sub_expr_ids.at(expr_nums_per_join_before));
                      sub_expr_ids.at(expr_nums_per_join_before) = 0;
                      expr_nums_per_join_before +=1;
                      per_join_nums -=1;
                    }
                    new_joined_table->add_join_exprs(exprs);
                  }
                }
              }
            }
          }
        }
        uint64_t new_joined_tid = subquery_stmt->generate_joined_tid();
        new_joined_table->set_joined_tid(new_joined_tid);
        common::ObVector<FromItem>& sub_from_items = subquery_stmt->get_from_items();
        sub_from_items.clear();
        subquery_stmt->add_from_item(new_joined_tid,true);
        subquery_stmt->add_joined_table(new_joined_table);

      }
      return ret;
    }
    int ObOptimizerLogical::adjust_subquery_join_item(
        ObLogicalPlan *&logical_plan, JoinedTable *&joined_table,
        JoinedTable *&sub_joined_table,int64_t sub_joined_position,
        std::map<uint64_t,uint64_t> alias_table_hashmap,
        int64_t join_table_idx, ObSelectStmt *&main_stmt)
    {
      int ret = OB_SUCCESS;
      common::ObArray<uint64_t>& sub_joined_table_ids = sub_joined_table->get_table_ids();
      common::ObArray<uint64_t>& sub_joined_expr_ids = sub_joined_table->get_expr_ids();
      common::ObArray<uint64_t>& sub_joined_types = sub_joined_table->get_join_types();
      common::ObArray<int64_t>& sub_expr_nums_per_join = sub_joined_table->get_expr_nums_per_join();

      common::ObArray<uint64_t>& table_ids = joined_table->get_table_ids();
      common::ObArray<uint64_t>& expr_ids = joined_table->get_expr_ids();
      common::ObArray<uint64_t>& joined_types = joined_table->get_join_types();
      common::ObArray<int64_t>& expr_nums_per_join = joined_table->get_expr_nums_per_join();
      int64_t sub_position = sub_joined_position;
      int64_t position = join_table_idx+1;
      int64_t next_joined_table_position = sub_joined_position;
      while(sub_position>0)
      {
        if(next_joined_table_position ==0)
        {
          break;
        }
        int64_t total = table_ids.count();
        int64_t expr_total = expr_ids.count();
        int64_t type_total = joined_types.count();
        uint64_t next_expr_id =0;
        uint64_t next_joined_table_id =0;
        uint64_t next_joined_type = 100;
        ObVector<uint64_t> next_exprs;
        int64_t sub_expr_nums =0;
        if(next_joined_table_position <=1)
        {
          sub_expr_nums = sub_expr_nums_per_join.at(0);
          for(int64_t j=0;j<sub_expr_nums;++j)
          {
            next_exprs.push_back(sub_joined_expr_ids.at(j));
          }
        }
        else
        {
          sub_expr_nums = sub_expr_nums_per_join.at(next_joined_table_position -1);
          int64_t expr_ids_index = sub_joined_table->get_expr_ids_index(next_joined_table_position-1);
          for(int64_t j=0;j<sub_expr_nums;++j)
          {
            next_exprs.push_back(sub_joined_expr_ids.at(expr_ids_index+j));
          }
        }
        if(next_joined_table_position <=1)
        {
          next_joined_type = sub_joined_types.at(0);
        }
        else
        {
          next_joined_type = sub_joined_types.at(next_joined_table_position-1);
        }
        std::map<uint64_t,uint64_t> column_id_hashmap;
        std::map<uint64_t,uint64_t> table_id_hashmap;
        uint64_t cur_join_table_id = sub_joined_table_ids.at(next_joined_table_position);
        for(int64_t j=0;j< sub_expr_nums;++j)
        {
          next_expr_id = next_exprs.at(static_cast<int32_t>(j));
          ObSqlRawExpr* sql_raw_expr = const_cast<ObSqlRawExpr*>(logical_plan->get_expr(next_expr_id));
          sql_raw_expr->optimize_sql_expression(main_stmt,
                                                table_id_hashmap,column_id_hashmap,cur_join_table_id,
                                                next_joined_table_id,alias_table_hashmap,4);
          if(next_joined_table_id !=0)
          {
            sub_joined_table_ids.at(next_joined_table_position) = 0;
            for(int64_t k=0;k< sub_joined_position;++k)
            {
              if(sub_joined_table_ids.at(k) == next_joined_table_id)
              {
                next_joined_table_position =k;
                break;
              }
            }
            break;
          }
        }
        if(next_joined_table_id ==0)
        {
          next_joined_table_position =0;
          next_joined_table_id = sub_joined_table_ids.at(next_joined_table_position);
        }
        if(position == total)
        {
          table_ids.push_back(alias_table_hashmap[next_joined_table_id]);
          joined_table->add_join_exprs(next_exprs);
          joined_types.push_back(next_joined_type);
        }
        else
        {
          for(int j=0;j<sub_expr_nums;++j)
          {
            expr_ids.push_back(-1);
          }
          int64_t temp_expr_total = expr_ids.count();
          while(position <total)
          {
            int64_t expr_nums=0;
            if(total == table_ids.count())
            {
              table_ids.push_back(table_ids.at(total-1));
              joined_types.push_back(joined_types.at(type_total-1));
              expr_nums_per_join.push_back(expr_nums_per_join.at(type_total-1));
              expr_nums = expr_nums_per_join.at(type_total-1);
              for(int64_t j=0;j<expr_nums;++j)
              {
                expr_ids.at(temp_expr_total-1) = expr_ids.at(expr_total - expr_nums+j);
                temp_expr_total --;
              }
            }
            else
            {
              table_ids.at(total) = table_ids.at(total-1);
              joined_types.at(type_total) = joined_types.at(type_total -1);
              expr_nums_per_join.at(type_total) =expr_nums_per_join.at(type_total-1);
              expr_nums = expr_nums_per_join.at(type_total-1);
              for(int64_t j=0;j<expr_nums;++j)
              {
                expr_ids.at(temp_expr_total-1) = expr_ids.at(expr_total - expr_nums+j);
                temp_expr_total --;
              }
            }
            --total;
            expr_total -= expr_nums;
            --type_total;
          }
          table_ids.at(position) = alias_table_hashmap[next_joined_table_id];
          joined_types.at(position-1) = next_joined_type;
          expr_nums_per_join.at(position-1)= sub_expr_nums;
          for(int64_t j=0;j<sub_expr_nums;++j)
          {
            expr_ids.at(temp_expr_total-1) = next_exprs.at(static_cast<int32_t>(j));
            temp_expr_total --;
          }
        }
        ++position;
        --sub_position;
      }
      sub_position =0;
      while(sub_position <sub_joined_table_ids.count()-1)
      {
        int64_t total = table_ids.count();
        int64_t expr_total = expr_ids.count();
        int64_t type_total = joined_types.count();
        if(sub_joined_table_ids.at(sub_position+1) >0)
        {
          int64_t sub_expr_nums = 0;
          ObVector<uint64_t> next_exprs;
          sub_expr_nums = sub_expr_nums_per_join.at(sub_position);
          int64_t sub_expr_ids_index = sub_joined_table->get_expr_ids_index(sub_position);
          for(int64_t j=0;j<sub_expr_nums;++j)
          {
            next_exprs.push_back(sub_joined_expr_ids.at(sub_expr_ids_index+j));
          }
          if(position == total)
          {
            table_ids.push_back(alias_table_hashmap[sub_joined_table_ids.at(sub_position+1)]);
            joined_types.push_back(sub_joined_types.at(sub_position));
            joined_table->add_join_exprs(next_exprs);
          }
          else
          {
            for(int j=0;j<sub_expr_nums;++j)
            {
              expr_ids.push_back(-1);
            }
            int64_t temp_expr_total = expr_ids.count();
            while(position <total)
            {
              int64_t expr_nums=0;
              if(total == table_ids.count())
              {
                table_ids.push_back(table_ids.at(total-1));
                joined_types.push_back(joined_types.at(type_total-1));
                expr_nums_per_join.push_back(expr_nums_per_join.at(type_total-1));
                expr_nums = expr_nums_per_join.at(type_total-1);
                for(int64_t j=0;j<expr_nums;++j)
                {
                  expr_ids.at(temp_expr_total-1) = expr_ids.at(expr_total - expr_nums+j);
                  temp_expr_total --;
                }
              }
              else
              {
                table_ids.at(total) = table_ids.at(total-1);
                joined_types.at(type_total) = joined_types.at(type_total -1);
                expr_nums_per_join.at(type_total) =expr_nums_per_join.at(type_total-1);
                expr_nums = expr_nums_per_join.at(type_total-1);
                for(int64_t j=0;j<expr_nums;++j)
                {
                  expr_ids.at(temp_expr_total-1) = expr_ids.at(expr_total - expr_nums+j);
                  temp_expr_total --;
                }
              }
              --total;
              --type_total;
              expr_total -= expr_nums;
            }
            table_ids.at(position) = alias_table_hashmap[sub_joined_table_ids.at(sub_position+1)];
            joined_types.at(position-1) = sub_joined_types.at(sub_position);
            expr_nums_per_join.at(position-1)= sub_expr_nums;
            for(int64_t j=0;j<sub_expr_nums;++j)
            {
              expr_ids.at(temp_expr_total-1) = next_exprs.at(static_cast<int32_t>(j));
              temp_expr_total --;
            }
          }
          ++position;
        }
        ++sub_position;
      }
      return ret;
    }

    int ObOptimizerLogical::pull_up_from_items(ObLogicalPlan *&logical_plan,
                                               ObSelectStmt *&main_stmt, ObSelectStmt *subquery_stmt, FromItem &from_item,
                                               std::map<uint64_t, uint64_t> &alias_table_hashmap, std::map<uint64_t, uint64_t> &table_id_hashmap,
                                               std::map<uint64_t, uint64_t> &column_id_hashmap, uint64_t table_id,
                                               std::map<uint64_t, uint64_t> &father_alias_table, int32_t from_item_idx)
    {
      int ret = OB_SUCCESS;
      bool is_joined = from_item.is_joined_;
      common::ObVector<FromItem>& from_items = main_stmt->get_from_items();
      int32_t sub_from_size = subquery_stmt->get_from_item_size();
      for(int32_t i= sub_from_size-1;i>=0; --i)
      {
        FromItem& sub_from_item = const_cast<FromItem&>(subquery_stmt->get_from_item(i));
        bool sub_is_joined = sub_from_item.is_joined_;
        if(is_joined)
        {
          if(sub_is_joined)
          {
            JoinedTable* sub_joined_table = subquery_stmt->get_joined_table(sub_from_item.table_id_);
            common::ObArray<uint64_t>& sub_joined_table_ids = sub_joined_table->get_table_ids();
            common::ObRowDesc* table_hash = main_stmt->get_table_hash();
            for(int64_t j=0;j<sub_joined_table_ids.count();++j)
            {
              int64_t idx = table_hash->get_idx(sub_joined_table_ids.at(j),OB_INVALID_ID);
              if(idx>0)
              {
                uint64_t alias_table_id = logical_plan->generate_table_id();
                alias_table_hashmap[sub_joined_table_ids.at(j)] = alias_table_id;
                father_alias_table.insert(std::pair<uint64_t,uint64_t>(sub_joined_table_ids.at(j),sub_joined_table_ids.at(j)));

              }
              else
              {
                alias_table_hashmap[sub_joined_table_ids.at(j)] = sub_joined_table_ids.at(j);
              }
              table_hash->add_column_desc(alias_table_hashmap[sub_joined_table_ids.at(j)],OB_INVALID_ID);
            }
          }
          else
          {
            common::ObRowDesc* table_hash = main_stmt->get_table_hash();
            int64_t idx = table_hash->get_idx(sub_from_item.table_id_,OB_INVALID_ID);
            if(idx>0)
            {
              uint64_t alias_table_id = logical_plan->generate_table_id();
              alias_table_hashmap[sub_from_item.table_id_] = alias_table_id;
              father_alias_table.insert(std::pair<uint64_t,uint64_t>(sub_from_item.table_id_,sub_from_item.table_id_));

            }
            else
            {
              alias_table_hashmap[sub_from_item.table_id_] = sub_from_item.table_id_;
            }
            table_hash->add_column_desc(alias_table_hashmap[sub_from_item.table_id_],OB_INVALID_ID);
          }
        }
        else
        {
          if(sub_is_joined)
          {
            JoinedTable* sub_joined_table = subquery_stmt->get_joined_table(sub_from_item.table_id_);
            common::ObArray<uint64_t>& sub_joined_table_ids = sub_joined_table->get_table_ids();
            common::ObArray<uint64_t>& sub_joined_expr_ids = sub_joined_table->get_expr_ids();
            common::ObRowDesc* table_hash = main_stmt->get_table_hash();
            for(int64_t j=0;j<sub_joined_table_ids.count();++j)
            {
              int64_t idx = table_hash->get_idx(sub_joined_table_ids.at(j),OB_INVALID_ID);
              if(idx>0)
              {
                uint64_t alias_table_id = logical_plan->generate_table_id();
                alias_table_hashmap[sub_joined_table_ids.at(j)] = alias_table_id;
                father_alias_table.insert(std::pair<uint64_t,uint64_t>(sub_joined_table_ids.at(j),sub_joined_table_ids.at(j)));

              }
              else
              {
                alias_table_hashmap[sub_joined_table_ids.at(j)] = sub_joined_table_ids.at(j);
              }
              table_hash->add_column_desc(alias_table_hashmap[sub_joined_table_ids.at(j)],OB_INVALID_ID);
              sub_joined_table_ids.at(j) = alias_table_hashmap[sub_joined_table_ids.at(j)];
            }
            for(int64_t k=0;k<sub_joined_expr_ids.count();++k)
            {
              uint64_t expr_id = sub_joined_expr_ids.at(k);
              ObSqlRawExpr* sql_raw_expr = const_cast<ObSqlRawExpr*>(logical_plan->get_expr(expr_id));
              uint64_t real_table_id =0;
              sql_raw_expr->optimize_sql_expression(main_stmt,
                                                    table_id_hashmap,column_id_hashmap,table_id,real_table_id,alias_table_hashmap,2);
            }
            uint64_t joined_tid = main_stmt->generate_joined_tid();
            if(i == sub_from_size -1)
            {
              from_item.table_id_ = joined_tid;
              from_item.is_joined_ = sub_from_item.is_joined_;
            }
            else
            {
              FromItem new_from_item;
              new_from_item.table_id_ = joined_tid;
              new_from_item.is_joined_ = sub_from_item.is_joined_;
              new_from_item.from_item_rel_opt_ = NULL;
              from_items.insert(from_items.begin()+from_item_idx,new_from_item);
            }
            sub_joined_table->set_joined_tid(joined_tid);
            main_stmt->add_joined_table(sub_joined_table);
          }
          else
          {
            common::ObRowDesc* table_hash = main_stmt->get_table_hash();
            int64_t idx = table_hash->get_idx(sub_from_item.table_id_,OB_INVALID_ID);
            if(idx>0)
            {
              uint64_t alias_table_id = logical_plan->generate_table_id();
              alias_table_hashmap[sub_from_item.table_id_] = alias_table_id;
              father_alias_table.insert(std::pair<uint64_t,uint64_t>(sub_from_item.table_id_,sub_from_item.table_id_));

            }
            else
            {
              alias_table_hashmap[sub_from_item.table_id_] = sub_from_item.table_id_;
            }
            table_hash->add_column_desc(alias_table_hashmap[sub_from_item.table_id_],OB_INVALID_ID);
            if(i ==sub_from_size-1)
            {
              from_item.table_id_ = alias_table_hashmap[sub_from_item.table_id_];
              from_item.is_joined_ = sub_from_item.is_joined_;
            }
            else
            {
              FromItem new_from_item;
              new_from_item.table_id_ = alias_table_hashmap[sub_from_item.table_id_];
              new_from_item.is_joined_ = sub_from_item.is_joined_;
              new_from_item.from_item_rel_opt_ = NULL;
              from_items.insert(from_items.begin()+from_item_idx,new_from_item);
            }
          }
        }
      }
      return ret;
    }

    int ObOptimizerLogical::pull_up_table_items(ObLogicalPlan *&logical_plan,
                                                ObSelectStmt *&main_stmt, ObSelectStmt *subquery_stmt,
                                                std::map<uint64_t, uint64_t> &alias_table_hashmap, uint64_t table_id)
    {
      int ret = OB_SUCCESS;
      common::ObVector<TableItem>& table_items = main_stmt->get_table_items();
      int32_t table_item_size = main_stmt->get_table_size();
      for(int32_t i=0;i<table_item_size;++i)
      {
        if(main_stmt->get_table_item(i).table_id_ == table_id)
        {
          table_items.remove(i);
          break;
        }
      }
      int32_t sub_table_item_size = subquery_stmt->get_table_size();
      for(int32_t i=0;i<sub_table_item_size;++i)
      {
        TableItem& sub_table_item = subquery_stmt->get_table_item(i);
        uint64_t sub_table_id = sub_table_item.table_id_;
        if(alias_table_hashmap[sub_table_id] == sub_table_id)
        {
          table_items.push_back(sub_table_item);
        }
        else
        {
          ObString new_alias_name;
          ob_write_string(*logical_plan->get_name_pool(),
                          ObString::link_string("alias",logical_plan->generate_alias_table_id()),
                          new_alias_name);
          sub_table_item.table_id_ = alias_table_hashmap[sub_table_id];
          sub_table_item.alias_name_ = new_alias_name;
          sub_table_item.type_ = TableItem::ALIAS_TABLE;
          table_items.push_back(sub_table_item);
        }
      }
      return ret;
    }

    int ObOptimizerLogical::pull_up_column_items(ObLogicalPlan *&logical_plan,
                                                 ObSelectStmt *&main_stmt, ObSelectStmt *subquery_stmt,
                                                 std::map<uint64_t, uint64_t> &alias_table_hashmap,
                                                 std::map<uint64_t, uint64_t> &table_id_hashmap,
                                                 std::map<uint64_t, uint64_t> &column_id_hashmap, uint64_t table_id)
    {
      int ret = OB_SUCCESS;
      int32_t sub_select_size = subquery_stmt->get_select_item_size();
      int* select_item_mark = new int[sub_select_size];
      for(int32_t j=0;j<sub_select_size;++j)
      {
        select_item_mark[j] =0;
      }
      int32_t sub_column_size = subquery_stmt->get_column_size();
      common::ObVector<ColumnItem>& main_column_items = main_stmt->get_column_items();
      int32_t column_size = main_stmt->get_column_size();
      for(int32_t i= column_size-1;i>=0;--i)
      {
        ColumnItem* column_item = const_cast<ColumnItem*>(main_stmt->get_column_item(i));
        common::ObString column_name = column_item->column_name_;
        if(column_item->table_id_ == table_id)
        {
          for(int32_t j=0;j<sub_select_size;++j)
          {
            SelectItem& sub_select_item = const_cast<SelectItem&>(subquery_stmt->get_select_item(j));
            uint64_t sub_expr_id = sub_select_item.expr_id_;
            common::ObString sub_alias_name = sub_select_item.alias_name_;
            ObSqlRawExpr* sql_raw_expr = logical_plan->get_expr(sub_expr_id);
            ObRawExpr* raw_expr = sql_raw_expr->get_expr();
            ObItemType item_type = raw_expr->get_expr_type();
            if(item_type == T_REF_COLUMN)
            {
              ObBinaryRefRawExpr* binary_ref_raw_expr = dynamic_cast<ObBinaryRefRawExpr*>(raw_expr);
              uint64_t first_ref_id = binary_ref_raw_expr->get_first_ref_id();
              uint64_t second_ref_id = binary_ref_raw_expr->get_second_ref_id();
              if(select_item_mark[j]==0 && column_name.compare(sub_alias_name) ==0)
              {
                select_item_mark[j] =1;
                column_id_hashmap[column_item->column_id_] = second_ref_id;
                table_id_hashmap[column_item->column_id_] = alias_table_hashmap[first_ref_id];
                break;
              }
            }
          }
          main_column_items.remove(i);
        }
      }
      for(int32_t i=0;i<sub_column_size;++i)
      {
        ColumnItem* sub_column_item = const_cast<ColumnItem*>(subquery_stmt->get_column_item(i));
        sub_column_item->query_id_ = main_stmt->get_query_id();
        sub_column_item->table_id_ = alias_table_hashmap[sub_column_item->table_id_];
        main_column_items.push_back(*sub_column_item);
        main_stmt->get_table_item_by_id(sub_column_item->table_id_)->has_scan_columns_ = true;
      }
      return ret;
    }

    int ObOptimizerLogical::pull_up_where_items(ObLogicalPlan *&logical_plan,
                                                ObSelectStmt *&main_stmt, ObSelectStmt *subquery_stmt,
                                                std::map<uint64_t, uint64_t> &alias_table_hashmap,
                                                std::map<uint64_t, uint64_t> &table_id_hashmap,
                                                std::map<uint64_t, uint64_t> &column_id_hashmap, uint64_t table_id)
    {
      int ret = OB_SUCCESS;
      int32_t sub_condition_size = subquery_stmt->get_condition_size();
      common::ObVector<uint64_t>& main_where_exprs = main_stmt->get_where_exprs();
      for(int32_t j=0;j<sub_condition_size;++j)
      {
        uint64_t sub_where_exprs_id =subquery_stmt->get_condition_id(j);
        main_where_exprs.push_back(sub_where_exprs_id);
        ObSqlRawExpr* sql_raw_expr = logical_plan->get_expr(sub_where_exprs_id);
        uint64_t real_table_id =0;
        sql_raw_expr->optimize_sql_expression(main_stmt,
                                              table_id_hashmap,column_id_hashmap,table_id,real_table_id,alias_table_hashmap,2);
      }
      int32_t condition_size = main_stmt->get_condition_size();
      for(int32_t i=0;i<condition_size;++i)
      {
        uint64_t real_table_id =0;
        uint64_t expr_id = main_stmt->get_condition_id(i);
        ObSqlRawExpr* sql_raw_expr = const_cast<ObSqlRawExpr*>(logical_plan->get_expr(expr_id));
        sql_raw_expr->optimize_sql_expression(main_stmt,
                                              table_id_hashmap,column_id_hashmap,table_id,real_table_id,alias_table_hashmap,1);
      }
      return ret;
    }
    int ObOptimizerLogical::pull_up_select_items(ObLogicalPlan *&logical_plan,
                                                 ObSelectStmt *&main_stmt, ObSelectStmt *subquery_stmt,
                                                 std::map<uint64_t, uint64_t> &alias_table_hashmap,
                                                 std::map<uint64_t, uint64_t> &table_id_hashmap,
                                                 std::map<uint64_t, uint64_t> &column_id_hashmap, uint64_t table_id)
    {
      int ret = OB_SUCCESS;
      int32_t sub_select_size = subquery_stmt->get_select_item_size();
      for(int32_t i=0;i<sub_select_size;++i)
      {
        SelectItem& sub_select_item = const_cast<SelectItem&>(subquery_stmt->get_select_item(i));
        uint64_t sub_expr_id = sub_select_item.expr_id_;
        ObSqlRawExpr* sub_sql_raw_expr = logical_plan->get_expr(sub_expr_id);
        logical_plan->delete_expr_by_id(sub_expr_id);
        sub_sql_raw_expr->~ObSqlRawExpr();
      }
      int32_t select_size = main_stmt->get_select_item_size();
      for(int32_t j=0;j<select_size;++j)
      {
        SelectItem& select_item = const_cast<SelectItem&>(main_stmt->get_select_item(j));
        uint64_t expr_id = select_item.expr_id_;
        ObSqlRawExpr* sql_raw_expr = logical_plan->get_expr(expr_id);
        uint64_t real_table_id=0;
        sql_raw_expr->optimize_sql_expression(main_stmt,
                                              table_id_hashmap,column_id_hashmap,table_id,real_table_id,alias_table_hashmap,1);
      }
      return ret;
    }

    int ObOptimizerLogical::pull_up_from_join_items(ObLogicalPlan *&logical_plan,
                                                    ObSelectStmt *&main_stmt, ObSelectStmt *subquery_stmt,
                                                    std::map<uint64_t, uint64_t> &alias_table_hashmap,
                                                    std::map<uint64_t, uint64_t> &table_id_hashmap,
                                                    std::map<uint64_t, uint64_t> &column_id_hashmap, uint64_t table_id, JoinedTable *&joined_table)
    {
      int ret = OB_SUCCESS;
      common::ObArray<uint64_t>& table_ids = joined_table->get_table_ids();
      for(int64_t i=0;i<table_ids.count();++i)
      {
        if(table_ids.at(i) == table_id)
        {
          uint64_t expr_id = 0;
          uint64_t real_table_id =0;
          int64_t cur_expr_nums_idx =0;
          int64_t cur_expr_nums =0;
          if(i==0)
          {
            cur_expr_nums_idx =0;
            cur_expr_nums = joined_table->get_expr_nums_per_join().at(0);
          }
          else
          {
            cur_expr_nums_idx = joined_table->get_expr_ids_index(i-1);
            cur_expr_nums = joined_table->get_expr_nums_per_join().at(i-1);
          }
          for(int64_t j=0;j<cur_expr_nums;++j)
          {
            expr_id = joined_table->get_expr_ids().at(cur_expr_nums_idx+j);
            ObSqlRawExpr* sql_raw_expr = const_cast<ObSqlRawExpr*>(logical_plan->get_expr(expr_id));
            sql_raw_expr->optimize_sql_expression(main_stmt,
                                                  table_id_hashmap,column_id_hashmap,table_id,real_table_id,alias_table_hashmap,1);
          }
          table_ids.at(i) = real_table_id;
          int32_t sub_from_size = subquery_stmt->get_from_item_size();
          for(int32_t j=0;j<sub_from_size;++j)
          {
            FromItem& sub_from_item = const_cast<FromItem&>(subquery_stmt->get_from_item(j));
            bool sub_is_joined = sub_from_item.is_joined_;
            if(sub_is_joined)
            {
              JoinedTable* sub_joined_table = subquery_stmt->get_joined_table(sub_from_item.table_id_);
              common::ObArray<uint64_t>& sub_joined_table_ids = sub_joined_table->get_table_ids();
              common::ObArray<uint64_t>& sub_joined_expr_ids = sub_joined_table->get_expr_ids();
              int64_t sub_joined_position = -1;
              for(int64_t k=sub_joined_table_ids.count()-1;k>=0;--k)
              {
                if(real_table_id == alias_table_hashmap[sub_joined_table_ids.at(k)])
                {
                  sub_joined_position = k;
                  break;
                }
              }
              if(sub_joined_position >=0)
              {
                adjust_subquery_join_item(logical_plan, joined_table,
                                          sub_joined_table, sub_joined_position,
                                          alias_table_hashmap, i, main_stmt);
              }
              for(int64_t k=0;k<sub_joined_expr_ids.count();++k)
              {
                uint64_t expr_id = sub_joined_expr_ids.at(k);
                ObSqlRawExpr* sql_raw_expr = const_cast<ObSqlRawExpr*>(logical_plan->get_expr(expr_id));
                sql_raw_expr->optimize_sql_expression(main_stmt,
                                                      table_id_hashmap,column_id_hashmap,table_id,real_table_id,alias_table_hashmap,2);
              }
            }
            else
            {

            }
          }
          break;
        }
      }
      common::ObArray<uint64_t>& expr_ids = joined_table->get_expr_ids();
      for(int64_t i=0;i<expr_ids.count();++i)
      {
        uint64_t real_table_id =0;
        uint64_t expr_id = expr_ids.at(i);
        ObSqlRawExpr* sql_raw_expr = const_cast<ObSqlRawExpr*>(logical_plan->get_expr(expr_id));
        sql_raw_expr->optimize_sql_expression(main_stmt,
                                              table_id_hashmap,column_id_hashmap,table_id,real_table_id,alias_table_hashmap,1);
      }
      return ret;
    }

    int ObOptimizerLogical::pull_up_simple_subquery(ObLogicalPlan *&logical_plan,
                                                    ObSelectStmt *&main_stmt, FromItem &from_item, TableItem *&table_item,
                                                    JoinedTable *&joined_table, uint64_t join_type, ResultPlan &result_plan,
                                                    std::map<uint64_t, uint64_t> &father_alias_table, int32_t from_item_idx)
    {
      int ret = OB_SUCCESS;
      uint64_t table_id = table_item->table_id_;
      bool is_joined = from_item.is_joined_;
      uint64_t ref_id = table_item->ref_id_;
      ObSelectStmt* subquery_stmt = dynamic_cast<ObSelectStmt*>(logical_plan->get_query(ref_id));
      ret = pull_up_subquery_union(logical_plan,subquery_stmt,result_plan,father_alias_table);
      std::map<uint64_t,uint64_t> column_id_hashmap;
      std::map<uint64_t,uint64_t> table_id_hashmap;
      std::map<uint64_t,uint64_t> alias_table_hashmap;
      if(ret == OB_SUCCESS
         && is_simple_subquery(subquery_stmt)
         && is_simple_subquery(main_stmt))
      {

      }
      else
      {
        ret = OB_SQL_CAN_NOT_PULL_UP;
      }
      if(ret == OB_SUCCESS)
      {
        ret = if_can_pull_up(
                logical_plan,subquery_stmt,joined_table,
                join_type,is_joined,table_id,main_stmt);
      }
      if(ret == OB_SUCCESS)
      {

      }
      if(ret == OB_SUCCESS)
      {
        ret = pull_up_from_items(
                logical_plan,main_stmt,subquery_stmt,from_item,
                alias_table_hashmap,table_id_hashmap,column_id_hashmap,
                table_id,father_alias_table,from_item_idx);
      }
      if(ret == OB_SUCCESS)
      {
        ret = pull_up_table_items(logical_plan,main_stmt,subquery_stmt,alias_table_hashmap,table_id);
        if(ret == OB_SUCCESS)
        {
          ret = pull_up_column_items(logical_plan,main_stmt,subquery_stmt,alias_table_hashmap,table_id_hashmap,column_id_hashmap,table_id);
        }
        if(ret == OB_SUCCESS)
        {
          ret = pull_up_select_items(logical_plan,main_stmt,subquery_stmt,alias_table_hashmap,table_id_hashmap,column_id_hashmap,table_id);
        }
        if(ret == OB_SUCCESS)
        {
          ret = pull_up_where_items(logical_plan,main_stmt,subquery_stmt,alias_table_hashmap,table_id_hashmap,column_id_hashmap,table_id);
        }
        if(ret == OB_SUCCESS)
        {
          if(is_joined)
          {
            ret = pull_up_from_join_items(logical_plan,main_stmt,subquery_stmt,alias_table_hashmap,table_id_hashmap,column_id_hashmap,table_id,joined_table);
          }
        }
        if(ret == OB_SUCCESS)
        {
          logical_plan->delete_stmt_by_query_id(ref_id);
          subquery_stmt->~ObSelectStmt();
        }
      }
      if(ret == OB_SUCCESS)
      {

      }
      else
      {
        if(ret == OB_SQL_CAN_NOT_PULL_UP)
        {

        }
        else
        {
          YYSYS_LOG(ERROR,"pull up sub query fail");
        }
      }
      return ret;
    }

    int ObOptimizerLogical::pull_up_simple_subquery_alt(ObLogicalPlan *&logical_plan, ObSelectStmt *&main_stmt, FromItem &from_item, TableItem *&table_item, JoinedTable *&joined_table, uint64_t join_type, ResultPlan &result_plan, std::map<uint64_t, uint64_t> &father_alias_table, int32_t from_item_idx)
    {
      FILE *pfile = fopen("/home/optimizer1/ywc/pull_up_simple_subquery.log","w+");
      UNUSED(from_item_idx);
      int ret = OB_SUCCESS;
      uint64_t table_id  = table_item->table_id_;
      bool is_joined = from_item.is_joined_;
      uint64_t ref_id = table_item->ref_id_;
      ObSelectStmt* subquery_stmt = dynamic_cast<ObSelectStmt*>(logical_plan->get_query(ref_id));
      ret = pull_up_subquery_union(logical_plan,subquery_stmt,result_plan,father_alias_table);
      if(ret == OB_SUCCESS
         && is_simple_subquery(subquery_stmt)
         && is_simple_subquery(main_stmt))
      {

      }
      else
      {
        ret = OB_SQL_CAN_NOT_PULL_UP;
      }
      if(ret == OB_SUCCESS)
      {
        ret = if_can_pull_up(
                logical_plan,subquery_stmt,joined_table,
                join_type,is_joined,table_id,main_stmt);
      }
      if(ret == OB_SUCCESS)
      {
        ObVector<TableItem>& sub_table_items = subquery_stmt->get_table_items();
        for(int i=0;i<sub_table_items.size();i++)
        {
          TableItem& sub_table_item = sub_table_items.at(i);
          TableItem old_sub_table_item = sub_table_item;
          if(sub_table_item.type_ == TableItem::BASE_TABLE)
          {
            uint64_t gen_alise_table_id = logical_plan->generate_table_id();
            uint64_t gen_alise_idx = logical_plan->generate_alias_table_id();
            ObString gen_alias_name;
            ob_write_string(*logical_plan->get_name_pool(),
                            ObString::link_string("alias",gen_alise_idx),
                            gen_alias_name);
            sub_table_item.table_id_ = gen_alise_table_id;
            sub_table_item.alias_name_ = gen_alias_name;
            sub_table_item.type_ = TableItem::ALIAS_TABLE;

            ObVector<ColumnItem>& sub_column_items = subquery_stmt->get_column_items();
            for(int i=0;i<sub_column_items.size();i++)
            {
              ColumnItem& sub_column_item = sub_column_items.at(i);
              if(sub_column_item.table_id_ == old_sub_table_item.table_id_)
                sub_column_item.table_id_ = gen_alise_table_id;
            }

            ObVector<FromItem>& sub_from_items = subquery_stmt->get_from_items();
            for(int i=0;i<sub_from_items.size();i++)
            {
              FromItem& sub_from_item = sub_from_items.at(i);
              if(sub_from_item.is_joined_)
              {
                uint64_t joined_table_id = sub_from_item.table_id_;
                JoinedTable* joined_table = subquery_stmt->get_joined_table(joined_table_id);
                for(int32_t j=0;j<joined_table->table_ids_.count();j++)
                {
                  if(joined_table->table_ids_.at(j) == old_sub_table_item.table_id_)
                    joined_table->table_ids_.at(j) = gen_alise_table_id;
                }
              }
              else if(sub_from_item.table_id_ == old_sub_table_item.table_id_)
                sub_from_item.table_id_ = gen_alise_table_id;
            }
            ObVector<uint64_t>& sub_where_exprs = subquery_stmt->get_where_exprs();
            ObVector<ObSqlRawExpr*>& expr_list = logical_plan->get_expr_list();
            for(int i=0;i<sub_where_exprs.size();i++)
            {
              uint64_t expr_id = sub_where_exprs.at(i);
              ObSqlRawExpr* expr_item = expr_list.at(int32_t(expr_id-1));
              expr_item->alter_sql_expression(old_sub_table_item.table_id_,gen_alise_table_id,subquery_stmt);
            }


          }
        }
      }
      if(ret == OB_SUCCESS)
      {
        FILE * file = fopen("/home/optimizer1/ywc/LogicalPlan.log","a+");
        fprintf(file,"\n-------------------------2----------------------\n");
        logical_plan->print(file,0);
        fclose(file);
        TableItem old_main_table_item = *table_item;
        ObVector<TableItem>& main_table_items = main_stmt->get_table_items();
        for(int i=0;i<main_table_items.size();i++)
        {
          TableItem& main_table_item = main_table_items.at(i);
          if(main_table_item.ref_id_ == subquery_stmt->get_query_id())
          {
            main_table_items.remove(main_table_items.begin()+i);
            ObVector<TableItem>& sub_table_items = subquery_stmt->get_table_items();
            for(int j=0;j<sub_table_items.size();j++)
            {
              TableItem& sub_table_item = sub_table_items.at(j);
              main_table_items.insert(main_table_items.begin()+i+j,sub_table_item);
            }
          }
        }
        ObVector<ColumnItem>& main_column_items = main_stmt->get_column_items();
        ObVector<ColumnItem>& sub_column_items = subquery_stmt->get_column_items();
        for(int i=0;i<main_column_items.size();i++)
        {
          ColumnItem main_column_item = main_column_items.at(i);
          if(main_column_item.table_id_ == old_main_table_item.table_id_)
          {
            uint64_t column_idx = main_column_item.column_id_-16;
            main_column_items.remove(main_column_items.begin()+i);
            ColumnItem& column_item = sub_column_items.at(int32_t(column_idx));
            ob_write_string(*logical_plan->get_name_pool(),
                            main_column_item.column_name_,column_item.column_name_);
          }
        }
        for(int j=0;j<sub_column_items.size();j++)
        {
          main_column_items.insert(main_column_items.begin()+j,sub_column_items.at(j));
        }

        ObVector<uint64_t>& main_where_exprs = main_stmt->get_where_exprs();
        ObVector<uint64_t>& sub_where_exprs = subquery_stmt->get_where_exprs();
        for(int i=0;i<sub_where_exprs.size();i++)
        {
          main_where_exprs.push_back(sub_where_exprs[i]);
        }

        ObVector<SelectItem>& sub_select_items = subquery_stmt->get_select_items();
        for(int j=0;j<sub_select_items.size();j++)
        {
          SelectItem& sub_select_item = sub_select_items.at(j);
          uint64_t sub_expr_id = sub_select_item.expr_id_;
          ObSqlRawExpr* sub_sql_raw_expr = logical_plan->get_expr(sub_expr_id);
          logical_plan->delete_expr_by_id(sub_expr_id);
          sub_sql_raw_expr->~ObSqlRawExpr();
        }

        if(old_main_table_item.type_ == TableItem::GENERATED_TABLE)
        {
          ObVector<ObSqlRawExpr*>& expr_list = logical_plan->get_expr_list();
          ObVector<TableItem>& sub_table_items = subquery_stmt->get_table_items();
          for(int i=0;i<sub_table_items.size();i++)
          {
            for(int j=0;j<expr_list.size();j++)
            {
              ObSqlRawExpr* expr_item = expr_list.at(j);
              expr_item->alter_sql_expression(old_main_table_item.table_id_,0,subquery_stmt);
            }
          }
        }

        ObVector<FromItem>& main_from_items = main_stmt->get_from_items();
        ObVector<FromItem>& sub_from_items = subquery_stmt->get_from_items();
        for(int i=0;i<main_from_items.size();i++)
        {
          FromItem& main_from_item = main_from_items.at(i);
          if(main_from_item.is_joined_)
          {
            JoinedTable* joined_table = main_stmt->get_joined_table(main_from_item.table_id_);
            for(int32_t j=0;j<joined_table->table_ids_.count();j++)
            {
              fprintf(pfile, "\njoined_table->table_ids_=%ld", joined_table->table_ids_.at(j));
              if(joined_table->table_ids_.at(j) == old_main_table_item.table_id_)
              {
                FromItem& sub_from_item = sub_from_items.at(0);
                joined_table->table_ids_.at(j) = sub_from_item.table_id_;
              }
            }
          }
          else if(main_from_item.table_id_ == old_main_table_item.table_id_)
          {
            main_from_items.remove(main_from_items.begin()+i);
            for(int j=0;j<sub_from_items.size();j++)
            {
              FromItem& sub_from_item = sub_from_items.at(j);
              FromItem new_from_item;
              fprintf(pfile, "\n sub_from_item.table_id_=%ld, sub_from_item.is_joined_=%d\n", sub_from_item.table_id_, sub_from_item.is_joined_);
              new_from_item.table_id_ = sub_from_item.table_id_;
              new_from_item.is_joined_ = sub_from_item.is_joined_;
              new_from_item.from_item_rel_opt_ = NULL;
              main_from_items.insert(main_from_items.begin()+i+j,new_from_item);
            }
          }
        }
        logical_plan->delete_stmt_by_query_id(ref_id);
        subquery_stmt->~ObSelectStmt();

      }
      fclose(pfile);
      return ret;
    }
    int ObOptimizerLogical::rule_max_min_eliminate(
        ObLogicalPlan *&logical_plan, ResultPlan *result_plan, const ObSchemaManagerV2 *schema)
    {
      int ret = OB_SUCCESS;
      if(logical_plan)
      {
        ObSelectStmt *main_stmt = NULL;
        ObBasicStmt* basic_stmt = logical_plan->get_main_stmt();
        if(basic_stmt->get_stmt_type() == ObBasicStmt::T_EXPLAIN)
        {
          ObExplainStmt *explain_stmt = dynamic_cast<ObExplainStmt*>(basic_stmt);
          main_stmt = dynamic_cast<ObSelectStmt*>(
                        logical_plan->get_query(explain_stmt->get_explain_query_id()));
        }
        else
        {
          main_stmt = dynamic_cast<ObSelectStmt*>(basic_stmt);
        }
        if(main_stmt == NULL)
        {
          ret = OB_ERR_ILLEGAL_ID;
          YYSYS_LOG(ERROR,"Stmt is not select stmt, so it will not enter the eliminate");
        }
        else if(OB_LIKELY(ret == OB_SUCCESS))
        {
          if(!main_stmt->is_for_update()
             && main_stmt->get_stmt_type() == ObSelectStmt::T_SELECT
             && !main_stmt->get_query_hint().is_has_hint_
             && !main_stmt->get_query_hint().not_use_index_
             && !main_stmt->get_query_hint().has_index_hint())
          {
            ret = eliminate_max_min_union(logical_plan,main_stmt,result_plan,schema);
          }
        }
      }
      return ret;
    }

    int ObOptimizerLogical::eliminate_max_min_union(ObLogicalPlan *&logical_plan, ObSelectStmt *&main_stmt, ResultPlan *result_plan, const ObSchemaManagerV2 *schema)
    {
      int ret = OB_SUCCESS;
      if(main_stmt->get_set_op()!= ObSelectStmt::NONE)
      {
        uint64_t left_query_id = main_stmt->get_left_query_id();
        uint64_t right_query_id = main_stmt->get_right_query_id();
        ObSelectStmt *left_stmt = dynamic_cast<ObSelectStmt*>(logical_plan->get_query(left_query_id));
        ObSelectStmt *right_stmt = dynamic_cast<ObSelectStmt*>(logical_plan->get_query(right_query_id));
        if(left_stmt->get_set_op() == ObSelectStmt::NONE)
        {
          ret = eliminate_max_min_recursion(logical_plan,left_stmt,result_plan,schema,false);
        }
        else
        {
          ret = eliminate_max_min_union(logical_plan,left_stmt,result_plan,schema);
        }

        if(right_stmt->get_set_op() == ObSelectStmt::NONE)
        {
          ret = eliminate_max_min_recursion(logical_plan,right_stmt,result_plan,schema,false);
        }
        else
        {
          ret = eliminate_max_min_union(logical_plan,right_stmt,result_plan,schema);
        }
      }
      else
      {
        ret = eliminate_max_min_recursion(logical_plan,main_stmt,result_plan,schema,false);
      }
      return ret;
    }

    int ObOptimizerLogical::eliminate_max_min_recursion(ObLogicalPlan *&logical_plan, ObSelectStmt *&main_stmt, ResultPlan *result_plan, const ObSchemaManagerV2 *schema, bool is_gen)
    {
      int ret = OB_SUCCESS;
      int32_t size = main_stmt->get_from_item_size();
      if(size ==1 && main_stmt->get_group_expr_size()==0)
      {
        ObVector<FromItem>& main_fromItems = main_stmt->get_from_items();
        FromItem& fromItem = main_fromItems.at(0);
        uint64_t table_id = fromItem.table_id_;
        if(!fromItem.is_joined_)
        {
          TableItem* tableItem = main_stmt->get_table_item_by_id(table_id);
          switch (tableItem->type_)
          {
            case TableItem::BASE_TABLE:
            case TableItem::ALIAS_TABLE:
            {
              int32_t num = main_stmt->get_agg_fun_size();
              ObSqlRawExpr* agg_expr = NULL;
              bool all_min_flag = (num==0?false:true);
              for(int32_t i=0;i<num;i++)
              {
                agg_expr = logical_plan->get_expr(main_stmt->get_agg_expr_id(i));
                if(agg_expr->get_expr()->get_expr_type() != T_FUN_MIN)
                {
                  all_min_flag = false;
                  break;
                }
              }
              if(!is_gen)
              {
                if(all_min_flag)
                {
                  if(num ==1)
                  {
                    ret = eliminate_single_max_min(logical_plan,main_stmt,result_plan,schema,false);
                  }
                  else if(num>1)
                  {
                    ret = eliminate_multi_max_min(logical_plan,main_stmt,result_plan,schema,false);
                  }
                }
              }
              else if(num ==0 && is_gen)
              {
                ret = eliminate_single_max_min(logical_plan,main_stmt,result_plan,schema,true);
              }
              else if(num ==1 && is_gen)
              {
                ret = eliminate_single_max_min(logical_plan,main_stmt,result_plan,schema,false);
              }
              else if(num>1 && is_gen)
              {
                ret = eliminate_multi_max_min(logical_plan,main_stmt,result_plan,schema,false);
              }


              break;
            }
            case TableItem::GENERATED_TABLE:
            {
              if(main_stmt->get_set_op() != ObSelectStmt::NONE)
              {
                ret = eliminate_max_min_union(logical_plan,main_stmt,result_plan,schema);
              }
              else
              {
                int32_t num = main_stmt->get_agg_fun_size();
                ObSqlRawExpr *agg_expr = NULL;
                if(num ==1 || is_gen)
                {
                  bool all_min_flag = true;
                  for(int32_t i=0;i<num;i++)
                  {
                    agg_expr = logical_plan->get_expr(main_stmt->get_agg_expr_id(i));
                    if(!agg_expr->get_expr()->is_aggr_fun() || agg_expr->get_expr()->get_expr_type() != T_FUN_MIN)
                    {
                      all_min_flag = false;
                      break;
                    }
                  }
                  bool is_all_agg_or_col = true;
                  if(is_gen)
                  {
                    int32_t num = main_stmt->get_select_item_size();
                    for(int32_t i=0;i<num;i++)
                    {
                      ObRawExpr *raw_expr = logical_plan->get_expr(main_stmt->get_select_item(i).expr_id_)->get_expr();
                      if(raw_expr->get_expr_type() != T_FUN_MIN && raw_expr->get_expr_type()!= T_REF_COLUMN)
                      {
                        is_all_agg_or_col = false;
                        break;
                      }
                    }
                  }
                  if(all_min_flag && is_all_agg_or_col)
                  {
                    uint64_t ref_id = tableItem->ref_id_;
                    ObSelectStmt* subquery_stmt = dynamic_cast<ObSelectStmt*>(logical_plan->get_query(ref_id));
                    if(subquery_stmt == NULL)
                    {
                      ret = OB_ERR_ILLEGAL_ID;
                      YYSYS_LOG(ERROR,"get stmt error");
                    }
                    else
                    {
                      ret = eliminate_max_min_recursion(logical_plan,subquery_stmt,result_plan,schema,true);
                    }
                  }
                }
                else if(num==0)
                {
                  uint64_t ref_id = tableItem->ref_id_;
                  ObSelectStmt* subquery_stmt = dynamic_cast<ObSelectStmt*>(logical_plan->get_query(ref_id));
                  if(subquery_stmt == NULL)
                  {
                    ret = OB_ERR_ILLEGAL_ID;
                    YYSYS_LOG(ERROR,"get stmt error");
                  }
                  else
                  {
                    ret = eliminate_max_min_recursion(logical_plan,subquery_stmt,result_plan,schema,false);
                  }
                }
                else if(num>1)
                {
                  uint64_t ref_id = tableItem->ref_id_;
                  ObSelectStmt* subquery_stmt = dynamic_cast<ObSelectStmt*>(logical_plan->get_query(ref_id));
                  if(subquery_stmt == NULL)
                  {
                    ret = OB_ERR_ILLEGAL_ID;
                    YYSYS_LOG(ERROR,"get stmt error");
                  }
                  else
                  {
                    ret = eliminate_max_min_recursion(logical_plan,subquery_stmt,result_plan,schema,false);
                  }
                }
              }
              break;
            }
            default:
              break;
          }
        }
        else if(fromItem.is_joined_ && !is_gen)
        {
          JoinedTable* joined_table = main_stmt->get_joined_table(table_id);
          for(int64_t i=0;i<joined_table->table_ids_.count();i++)
          {
            uint64_t joined_table_id = joined_table->table_ids_.at(i);
            TableItem* tableItem = main_stmt->get_table_item_by_id(joined_table_id);
            if(tableItem->type_ == TableItem::GENERATED_TABLE)
            {
              uint64_t ref_id = tableItem->ref_id_;
              ObSelectStmt* subquery_stmt = dynamic_cast<ObSelectStmt*>(logical_plan->get_query(ref_id));
              ret = eliminate_max_min_recursion(logical_plan,subquery_stmt,result_plan,schema,false);
            }
          }
        }
      }
      else if(size >1)
      {
        for(int32_t i=0;i<size;i++)
        {
          FromItem& from_item = const_cast<FromItem&>(main_stmt->get_from_item(i));
          if(!from_item.is_joined_)
          {
            TableItem* table_item = main_stmt->get_table_item_by_id(from_item.table_id_);
            if(table_item->type_ == TableItem::GENERATED_TABLE)
            {
              uint64_t ref_id = table_item->ref_id_;
              ObSelectStmt* select_stmt = dynamic_cast<ObSelectStmt*>(logical_plan->get_query(ref_id));
              ret = eliminate_max_min_recursion(logical_plan,select_stmt,result_plan,schema,false);
            }
          }
        }
      }
      return ret;
    }

    int ObOptimizerLogical::eliminate_single_max_min(ObLogicalPlan *&logical_plan, ObSelectStmt *&main_stmt, ResultPlan *result_plan, const ObSchemaManagerV2 *schema, bool is_gen)
    {
      int ret = OB_SUCCESS;
      if(if_can_eliminate(main_stmt) && is_all_agg_cols_rowkey_or_index(logical_plan,main_stmt,schema) &&
         is_select_items_qualified(logical_plan, main_stmt))
      {
        if(!is_gen)
        {
          OB_ASSERT(main_stmt->get_agg_fun_size() ==1);
          ObAggFunRawExpr *aggExpr = dynamic_cast<ObAggFunRawExpr*>(logical_plan->get_expr(main_stmt->get_agg_expr_id(0))->get_expr());
          ObRawExpr* param_expr = aggExpr->get_param_expr();
          ObBinaryRefRawExpr *col_expr = dynamic_cast<ObBinaryRefRawExpr*>(param_expr);
          uint64_t table_id = col_expr->get_first_ref_id();
          uint64_t column_id = col_expr->get_second_ref_id();

          main_stmt->set_is_max_min_eliminate(true);
          main_stmt->set_eliminate_index_col_id(column_id);
          ObSelectStmt *new_stmt = (ObSelectStmt*)parse_malloc(sizeof(ObSelectStmt),result_plan->name_pool_);
          if(new_stmt!= NULL)
          {
            uint64_t main_stmt_query_id = main_stmt->get_query_id();
            int32_t main_stmt_index = logical_plan->get_stmt_index_by_query_id(main_stmt_query_id);
            ObStringBuf *name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
            new_stmt = new(new_stmt) ObSelectStmt(name_pool);
            uint64_t query_id = logical_plan->generate_query_id();
            main_stmt->set_query_id(query_id);
            new_stmt->set_query_id(main_stmt_query_id);
            new_stmt->set_stmt_type(ObBasicStmt::T_SELECT);
            new_stmt->assign_set_op(ObSelectStmt::NONE);
            new_stmt->assign_all();

            ObString db_name;
            ObString table_name = main_stmt->get_table_item_by_id(table_id)->table_name_;
            ObString alias_name;
            uint64_t new_table_id = OB_INVALID_ID;
            ret = logical_plan->get_stmts().insert(logical_plan->get_stmts().begin() + main_stmt_index,new_stmt);
            if(ret != OB_SUCCESS)
            {
              snprintf(result_plan->err_stat_.err_msg_,MAX_ERROR_MSG,
                       "Can not add YaoSelectStmt to logical plan");
            }
            else if(OB_SUCCESS != (ret = new_stmt->add_table_item(
                                     *result_plan,
                                     db_name,
                                     table_name,
                                     alias_name,
                                     new_table_id,
                                     TableItem::GENERATED_TABLE,
                                     query_id,
                                     true
                                     )))
            {
              YYSYS_LOG(ERROR,"add table item to new stmt failed [%d]",ret);
            }
            else if(OB_SUCCESS != (ret = new_stmt->add_from_item(new_table_id)))
            {
              YYSYS_LOG(ERROR,"add table item to new stmt failed [%d]",ret);
            }
            else
            {
              ColumnItem* main_columnItem = main_stmt->get_column_item_by_id(table_id,column_id);
              ObSqlRawExpr * sql_expr = NULL;
              uint64_t expr_id = OB_INVALID_ID;
              if(!is_select_item_contain_col(logical_plan,main_stmt,column_id))
              {
                if(OB_SUCCESS != (ret = create_column_expr(
                                    logical_plan,
                                    result_plan,
                                    main_stmt,
                                    sql_expr,
                                    table_id,
                                    column_id,
                                    col_expr->get_result_type())))
                {
                  YYSYS_LOG(ERROR,"create column expr failed ret %d",ret);
                }
                else
                {
                  expr_id = sql_expr->get_expr_id();
                  ret = main_stmt->add_select_item(
                          expr_id,
                          false,
                          main_columnItem->column_name_,
                          main_columnItem->column_name_,
                          sql_expr->get_result_type());
                  if(ret != OB_SUCCESS)
                  {
                    YYSYS_LOG(ERROR,"add select item to main stmt failed [%d]",ret);
                  }
                }
              }
              if(ret == OB_SUCCESS)
              {
                int32_t size = main_stmt->get_select_item_size();
                for(int32_t i=0;ret ==OB_SUCCESS && i<size;i++)
                {
                  const SelectItem& sel_item = main_stmt->get_select_item(i);
                  ObSqlRawExpr* select_sql_expr = logical_plan->get_expr(sel_item.expr_id_);
                  ObBinaryRefRawExpr *b_expr = dynamic_cast<ObBinaryRefRawExpr*>(select_sql_expr->get_expr());
                  if(b_expr != NULL && main_stmt->get_agg_fun_size() >0 &&
                     b_expr->get_second_ref_id() == logical_plan->get_expr(main_stmt->get_agg_expr_id(0))->get_column_id())
                  {
                    if(OB_SUCCESS != (ret = new_stmt->add_agg_func(main_stmt->get_agg_expr_id(0))))
                    {
                      YYSYS_LOG(ERROR,"add agg func id to new stmt failed %d",ret);
                    }
                    else if(OB_SUCCESS !=(ret = main_stmt->remove_agg_func_id(0)))
                    {
                      YYSYS_LOG(ERROR,"remove agg func id from main stmt failed [%d]", ret);
                    }
                    else if(OB_SUCCESS != (ret = new_stmt->add_select_item(sel_item.expr_id_,sel_item.is_real_alias_,sel_item.alias_name_,sel_item.expr_name_,sel_item.type_)))
                    {
                      YYSYS_LOG(ERROR,"add select item to new stmt failed %d",ret);
                    }
                    else if(OB_SUCCESS != (ret = main_stmt->get_select_items().remove(i)))
                    {
                      YYSYS_LOG(ERROR,"remove agg select item from main stmt failed %d",ret);
                    }
                    else
                    {
                      i--;
                      size--;
                    }
                  }
                  else if(sel_item.expr_id_ != expr_id && b_expr != NULL && b_expr->get_expr_type() == T_REF_COLUMN)
                  {
                    ColumnItem *col_item = main_stmt->get_column_item_by_id(table_id,b_expr->get_second_ref_id());
                    ObSqlRawExpr* sql_expr = NULL;
                    if(OB_SUCCESS != (ret = create_column_expr(
                                        logical_plan,
                                        result_plan,
                                        new_stmt,
                                        sql_expr,
                                        new_table_id,col_item->column_id_,
                                        b_expr->get_result_type())))
                    {
                      YYSYS_LOG(ERROR,"create column expr failed ret  %d",ret);
                    }
                    else if(sql_expr == NULL || OB_SUCCESS != (ret = new_stmt->add_select_item(
                                                                 sql_expr->get_expr_id(),
                                                                 sel_item.is_real_alias_,
                                                                 sel_item.alias_name_,
                                                                 sel_item.expr_name_,
                                                                 sql_expr->get_result_type())))
                    {
                      YYSYS_LOG(ERROR,"add select item failed %d",ret);
                    }
                  }
                }
                if(ret == OB_SUCCESS)
                {
                  size = main_stmt->get_select_item_size();
                  ObVector<uint64_t> col_ids;
                  for(int32_t i= size-1;i>=0;i--)
                  {
                    SelectItem& sel_item = const_cast<SelectItem&>(main_stmt->get_select_item(i));
                    ObSqlRawExpr* select_sql_expr = logical_plan->get_expr(sel_item.expr_id_);
                    ObBinaryRefRawExpr *b_expr = dynamic_cast<ObBinaryRefRawExpr*>(select_sql_expr->get_expr());
                    if(b_expr!= NULL)
                    {
                      uint64_t col_id = b_expr->get_second_ref_id();
                      if(sel_item.is_real_alias_)
                      {
                        ColumnItem *col_item = main_stmt->get_column_item_by_id(b_expr->get_first_ref_id(),col_id);
                        sel_item.is_real_alias_ = false;
                        sel_item.alias_name_ = col_item->column_name_;
                        sel_item.expr_name_ = col_item->column_name_;
                      }
                      bool is_dup = false;
                      for(int32_t j=0;j<col_ids.size();j++)
                      {
                        if(col_ids.at(j) == col_id)
                        {
                          is_dup = true;
                          break;
                        }
                      }
                      if(!is_dup)
                      {
                        col_ids.push_back(col_id);
                      }
                      else
                      {
                        logical_plan->delete_expr_by_id(sel_item.expr_id_);
                        ret = main_stmt->get_select_items().remove(i);
                        if(ret != OB_SUCCESS)
                        {
                          YYSYS_LOG(ERROR, "remove select item error [%d]", ret);
                          break;
                        }
                      }
                    }
                  }
                }
                if(ret == OB_SUCCESS)
                {
                  size = new_stmt->get_select_item_size();
                  for(int32_t i=0;i<size;i++)
                  {
                    const SelectItem& sel_item = new_stmt->get_select_item(i);
                    ObSqlRawExpr* select_sql_expr = logical_plan->get_expr(sel_item.expr_id_);
                    ObBinaryRefRawExpr *b_expr = dynamic_cast<ObBinaryRefRawExpr*>(select_sql_expr->get_expr());
                    bool is_min_sel = false;
                    if(b_expr != NULL && b_expr->get_second_ref_id() == logical_plan->get_expr(new_stmt->get_agg_expr_id(0))->get_column_id())
                    {
                      is_min_sel = true;
                    }
                    if(b_expr != NULL)
                    {
                      uint64_t col_id = is_min_sel ? column_id : b_expr->get_second_ref_id();
                      ColumnItem *col_item = main_stmt->get_column_item_by_id(table_id,col_id);
                      ColumnItem *column_item = new_stmt->get_column_item(NULL,col_item->column_name_);
                      if(column_item == NULL)
                      {
                        ColumnItem *new_col_item = NULL;
                        ret = new_stmt->add_column_item(*result_plan,col_item->column_name_,NULL,&new_col_item,true);
                        if(ret != OB_SUCCESS)
                        {
                          YYSYS_LOG(ERROR, "add column item failed [%d]", ret);
                          break;
                        }
                        else
                        {
                          column_item = new_col_item;
                        }
                      }
                      if(is_min_sel)
                      {
                        col_expr->set_first_ref_id(new_table_id);
                        col_expr->set_second_ref_id(column_item->column_id_);
                      }
                      else
                      {
                        b_expr->set_second_ref_id(column_item->column_id_);
                      }
                    }
                  }
                  if(ret == OB_SUCCESS)
                  {
                    ret = add_limit_one_expr(logical_plan,main_stmt,result_plan);
                  }
                }
              }
            }
          }
          else
          {
            ret = OB_ERR_PARSER_MALLOC_FAILED;
            YYSYS_LOG(WARN,"out of memory");
            snprintf(result_plan->err_stat_.err_msg_,MAX_ERROR_MSG,
                     "Can not malloc YaoSelectStmt");
          }
        }
        else
        {
          int32_t index = logical_plan->get_stmt_index_by_query_id(main_stmt->get_query_id());
          int32_t index_copy = index;
          ObSelectStmt *sel_stmt = NULL;
          while(index >=1)
          {
            index--;
            sel_stmt = dynamic_cast<ObSelectStmt*>(logical_plan->get_stmts().at(index));
            if(sel_stmt->get_agg_fun_size() >0)
            {
              break;
            }
          }
          int32_t size = sel_stmt->get_agg_fun_size();
          OB_ASSERT(size == 1);
          uint64_t expr_id = sel_stmt->get_agg_expr_id(0);
          ObAggFunRawExpr *agg_expr = dynamic_cast<ObAggFunRawExpr*>(logical_plan->get_expr(expr_id)->get_expr());
          ObBinaryRefRawExpr *col_expr = dynamic_cast<ObBinaryRefRawExpr*>(agg_expr->get_param_expr());
          uint64_t table_id = col_expr->get_first_ref_id();
          uint64_t column_id = col_expr->get_second_ref_id();
          ColumnItem *col_item = sel_stmt->get_column_item_by_id(table_id,column_id);
          ObSelectStmt *next_stmt = NULL;
          ObBinaryRefRawExpr *next_col_expr = NULL;
          while(index< index_copy)
          {
            index++;
            next_stmt = dynamic_cast<ObSelectStmt*>(logical_plan->get_stmt(index));
            int32_t sel_item_size = next_stmt->get_select_item_size();

            for(int32_t i=0;i< sel_item_size;i++)
            {
              const SelectItem& sel_item = next_stmt->get_select_item(i);
              if(col_item->column_name_ == sel_item.alias_name_)
              {
                next_col_expr = dynamic_cast<ObBinaryRefRawExpr*>(logical_plan->get_expr(sel_item.expr_id_)->get_expr());
                col_item = next_stmt->get_column_item_by_id(next_col_expr->get_first_ref_id(),next_col_expr->get_second_ref_id());
                break;
              }
            }
          }
          if(next_col_expr == NULL)
          {
            YYSYS_LOG(ERROR,"do not find col in main stmt");
          }
          else
          {
            const ObTableSchema *table_schema = NULL;
            if(NULL ==(table_schema = schema->get_table_schema(next_col_expr->get_first_ref_id())))
            {
              ret = OB_ERR_ILLEGAL_ID;
              YYSYS_LOG(WARN, "Fail to get table schema for table[%ld]", table_id);
            }
            else
            {
              if(!table_schema->get_rowkey_info().is_rowkey_column(next_col_expr->get_second_ref_id()))
              {
                uint64_t tmp_index_tid[OB_MAX_INDEX_NUMS];
                for(int32_t m=0;m< OB_MAX_INDEX_NUMS;m++)
                {
                  tmp_index_tid[m] = OB_INVALID_ID;
                }
                if(schema->is_cid_in_index(next_col_expr->get_second_ref_id(),next_col_expr->get_first_ref_id(),tmp_index_tid))
                {
                  main_stmt->set_is_max_min_eliminate(true);
                  main_stmt->set_eliminate_index_col_id(next_col_expr->get_second_ref_id());
                  ret = add_limit_one_expr(logical_plan,main_stmt,result_plan);
                }
              }
              else
              {
                main_stmt->set_is_max_min_eliminate(true);
                main_stmt->set_eliminate_index_col_id(next_col_expr->get_second_ref_id());
                ret = add_limit_one_expr(logical_plan,main_stmt,result_plan);
              }
            }
          }

        }
      }
      return ret;
    }

    int ObOptimizerLogical::eliminate_multi_max_min(ObLogicalPlan *&logical_plan, ObSelectStmt *&main_stmt, ResultPlan *result_plan, const ObSchemaManagerV2 *schema, bool is_gen)
    {
      UNUSED(is_gen);
      int ret = OB_SUCCESS;
      if(if_can_eliminate(main_stmt) && is_all_agg_cols_rowkey_or_index(logical_plan,main_stmt,schema) &&
         is_select_items_qualified(logical_plan,main_stmt))
      {
        int32_t main_stmt_index = logical_plan->get_stmt_index_by_query_id(main_stmt->get_query_id());
        ObSelectStmt *new_stmt = (ObSelectStmt*) parse_malloc(sizeof(ObSelectStmt),result_plan->name_pool_);
        if(new_stmt == NULL)
        {
          ret = OB_ERR_PARSER_MALLOC_FAILED;
          YYSYS_LOG(WARN, "out of memory");
          snprintf(result_plan->err_stat_.err_msg_,MAX_ERROR_MSG,
                   "Can not malloc YaoSelectStmt");
        }
        else
        {
          ObStringBuf *name_pool = static_cast<ObStringBuf *>(result_plan->name_pool_);
          new_stmt = new(new_stmt) ObSelectStmt(name_pool);
          uint64_t new_query_id = logical_plan->generate_query_id();
          new_stmt->set_query_id(main_stmt->get_query_id());
          main_stmt->set_query_id(new_query_id);
          new_stmt->set_stmt_type(ObBasicStmt::T_SELECT);
          new_stmt->assign_set_op(ObSelectStmt::NONE);
          new_stmt->assign_all();

          if(OB_SUCCESS != (ret =logical_plan->get_stmts().insert(logical_plan->get_stmts().begin() + main_stmt_index,new_stmt)))
          {
            YYSYS_LOG(ERROR,"insert stmt error %d",ret);
          }
          else if(OB_SUCCESS != (ret = logical_plan->get_stmts().remove(main_stmt_index +1)))
          {
            YYSYS_LOG(ERROR,"remove stmt error %d",ret);
          }
          else if(OB_SUCCESS != (ret = split_multi_mins(logical_plan,result_plan,main_stmt,main_stmt_index)))
          {
            YYSYS_LOG(ERROR,"split multi min aggfuncs error %d",ret);
          }
          else
          {
            uint64_t joined_table_id = new_stmt->generate_joined_tid();
            JoinedTable *joined_table = (JoinedTable*) parse_malloc(sizeof(JoinedTable),result_plan->name_pool_);
            if(joined_table == NULL)
            {
              ret = OB_ERR_PARSER_MALLOC_FAILED;
              YYSYS_LOG(WARN, "out of memory");
              snprintf(result_plan->err_stat_.err_msg_,MAX_ERROR_MSG,
                       "Can not malloc space for JoinedTable");
            }
            else
            {
              joined_table = new(joined_table) JoinedTable;
              joined_table->set_joined_tid(joined_table_id);

              uint64_t agg_table_id = OB_INVALID_ID;
              ObString db_name;
              ObString table_name = main_stmt->get_table_item(0).table_name_;
              ObString alias_name;
              uint64_t gen_idx;
              for(int32_t i=0;ret == OB_SUCCESS && i<main_stmt->get_agg_fun_size();i++)
              {
                gen_idx = logical_plan->generate_alias_table_id();
                ob_write_string(*logical_plan->get_name_pool(),
                                ObString::link_string("alias", gen_idx),
                                alias_name);
                if(OB_SUCCESS != (ret = new_stmt->add_table_item(
                                    *result_plan,
                                    db_name,
                                    table_name,
                                    alias_name,
                                    agg_table_id,
                                    TableItem::GENERATED_TABLE,
                                    logical_plan->get_stmt(main_stmt_index +1 + 2*i)->get_query_id(),
                                    true)))
                {
                  YYSYS_LOG(ERROR,"add table item error %d",ret);
                  break;
                }
                else if(OB_SUCCESS != (ret = joined_table->add_table_id(agg_table_id)))
                {
                  YYSYS_LOG(ERROR, "add table id to joined_table error [%d]", ret);

                }
                if(i !=0)
                {
                  ObVector<uint64_t> join_exprs;
                  if(OB_SUCCESS != (ret = joined_table->add_join_type(JoinedTable::T_INNER)))
                  {
                    YYSYS_LOG(ERROR,"add join type to joined_table error %d",ret);
                  }
                  else if(OB_SUCCESS != (ret = add_on_expr(logical_plan,result_plan,join_exprs)))
                  {
                    YYSYS_LOG(ERROR,"add on 1 = 1 expr to inner join error %d",ret);
                  }
                  else if(OB_SUCCESS != (ret = joined_table->add_join_exprs(join_exprs)))
                  {
                    YYSYS_LOG(ERROR,"add join_exprs to joined_table error %d",ret);
                  }
                }
              }
              if(ret != OB_SUCCESS)
              {
                YYSYS_LOG(ERROR,"add join table to joined_table error %d",ret);
              }
              else if(OB_SUCCESS !=(ret = new_stmt->add_joined_table(joined_table)))
              {
                snprintf(result_plan->err_stat_.err_msg_,MAX_ERROR_MSG,
                         "Can not add JoinedTable");
              }
              else if(OB_SUCCESS != (ret = new_stmt->add_from_item(joined_table_id,true)))
              {
                YYSYS_LOG(ERROR, "add from item to new stmt error [%d]", ret);
              }
              else if(OB_SUCCESS != (ret = add_select_star_columns(logical_plan,new_stmt,result_plan)))
              {
                YYSYS_LOG(ERROR,"add column items to new stmt error %d",ret);
              }
            }
          }
        }
      }
      return ret;
    }

    int ObOptimizerLogical::split_multi_mins(
        ObLogicalPlan *&logical_plan,
        ResultPlan *result_plan,
        ObSelectStmt *&main_stmt,
        int32_t main_stmt_index)
    {
      int ret = OB_SUCCESS;
      bool first_stmt = true;
      int32_t select_item_index = 0;
      ObStringBuf *name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
      for(int32_t i=0;ret== OB_SUCCESS && i <main_stmt->get_agg_fun_size();i++)
      {
        ObAggFunRawExpr *agg_expr = dynamic_cast<ObAggFunRawExpr*>(logical_plan->get_expr(main_stmt->get_agg_expr_id(i))->get_expr());
        ObSelectStmt *agg_main_stmt = (ObSelectStmt*)parse_malloc(sizeof(ObSelectStmt),result_plan->name_pool_);
        ObSelectStmt *agg_sub_stmt = (ObSelectStmt*)parse_malloc(sizeof(ObSelectStmt),result_plan->name_pool_);
        if(agg_main_stmt == NULL || agg_sub_stmt == NULL)
        {
          ret = OB_ERR_PARSER_MALLOC_FAILED;
          YYSYS_LOG(WARN, "out of memory");
          snprintf(result_plan->err_stat_.err_msg_,MAX_ERROR_MSG,
                   "Can not malloc YaoSelectStmt");
        }
        if(ret == OB_SUCCESS)
        {
          uint64_t agg_main_stmt_query_id = OB_INVALID_ID;
          agg_main_stmt = new(agg_main_stmt) ObSelectStmt(name_pool);
          if(first_stmt)
          {
            agg_main_stmt->set_query_id(main_stmt->get_query_id());
            first_stmt = false;
          }
          else
          {
            agg_main_stmt_query_id = logical_plan->generate_query_id();
            agg_main_stmt->set_query_id(agg_main_stmt_query_id);
          }
          agg_main_stmt->set_stmt_type(ObBasicStmt::T_SELECT);
          agg_main_stmt->assign_set_op(ObSelectStmt::NONE);
          agg_main_stmt->assign_all();
          logical_plan->get_stmts().insert(logical_plan->get_stmts().begin() + main_stmt_index +1 +2*i,agg_main_stmt);

          agg_sub_stmt = new(agg_sub_stmt) ObSelectStmt(name_pool);
          uint64_t agg_sub_stmt_query_id = logical_plan->generate_query_id();
          agg_sub_stmt->set_query_id(agg_sub_stmt_query_id);
          agg_sub_stmt->set_stmt_type(ObBasicStmt::T_SELECT);
          agg_sub_stmt->assign_set_op(ObSelectStmt::NONE);
          agg_sub_stmt->assign_all();

          logical_plan->get_stmts().insert(logical_plan->get_stmts().begin() + main_stmt_index + 2 + 2 * i, agg_sub_stmt);

          ObBinaryRefRawExpr* param_expr = dynamic_cast<ObBinaryRefRawExpr*>(agg_expr->get_param_expr());
          uint64_t table_id = param_expr->get_first_ref_id();
          uint64_t column_id = param_expr->get_second_ref_id();
          ColumnItem *main_columnItem = main_stmt->get_column_item_by_id(table_id,column_id);
          agg_sub_stmt->set_is_max_min_eliminate(true);
          agg_sub_stmt->set_eliminate_index_col_id(column_id);

          if(!is_select_item_contain_col(logical_plan,main_stmt,column_id))
          {
            ObSqlRawExpr *sql_expr = NULL;
            ret = create_column_expr(
                    logical_plan,
                    result_plan,
                    main_stmt,
                    sql_expr,
                    table_id,
                    column_id,
                    param_expr->get_result_type());
            if(ret != OB_SUCCESS)
            {
              YYSYS_LOG(ERROR,"create_column_expr failed ret %d",ret);
              break;
            }
            else
            {
              uint64_t expr_id = sql_expr->get_expr_id();
              const ObObjType type = sql_expr->get_result_type();
              ret = agg_sub_stmt->add_select_item(expr_id,false,main_columnItem->column_name_,main_columnItem->column_name_,type);
              if(ret != OB_SUCCESS)
              {
                YYSYS_LOG(ERROR,"add select item to agg sub stmt error");
                break;
              }
            }
          }
          else
          {
            YYSYS_LOG(INFO,"have select item contains agg column id %ld",column_id);
          }
          if(ret == OB_SUCCESS)
          {
            int32_t size = main_stmt->get_select_item_size();

            for(int32_t j=0;ret==OB_SUCCESS && j<size;j++)
            {
              const SelectItem& sel_item = main_stmt->get_select_item(j);
              ObSqlRawExpr *sql_expr = logical_plan->get_expr(sel_item.expr_id_);
              ObBinaryRefRawExpr *b_expr = dynamic_cast<ObBinaryRefRawExpr *>(sql_expr->get_expr());
              uint64_t table_id = b_expr->get_first_ref_id();
              uint64_t col_id = b_expr->get_second_ref_id();
              if(b_expr!= NULL && !is_agg_select_item(logical_plan,main_stmt,col_id)
                 && !is_select_item_contain_col(logical_plan,agg_sub_stmt,col_id))
              {
                ObSqlRawExpr *sql_expr = NULL;
                if(OB_SUCCESS != (ret = create_column_expr(
                                    logical_plan,
                                    result_plan,
                                    main_stmt,
                                    sql_expr,
                                    b_expr->get_first_ref_id(),
                                    b_expr->get_second_ref_id(),
                                    b_expr->get_result_type())))
                {
                  YYSYS_LOG(ERROR,"create column expr failed ret %d",ret);
                  break;
                }
                else
                {
                  ColumnItem *col_item = main_stmt->get_column_item_by_id(table_id,col_id);
                  if(col_item == NULL || OB_SUCCESS != (ret = agg_sub_stmt->add_select_item(
                                                          sql_expr->get_expr_id(),
                                                          false,
                                                          col_item->column_name_,
                                                          col_item->column_name_,
                                                          sql_expr->get_result_type())))
                  {
                    YYSYS_LOG(ERROR,"add select item failed ret %d",ret);
                    break;
                  }
                }
              }
            }
          }

          if(ret == OB_SUCCESS)
          {
            int32_t table_size = main_stmt->get_table_items().size();
            for(int32_t j=0;j<table_size;j++)
            {
              agg_sub_stmt->get_table_items().push_back(main_stmt->get_table_items().at(j));
              if((ret = agg_sub_stmt->get_table_hash()->add_column_desc(main_stmt->get_table_item(j).table_id_,OB_INVALID_ID)) != OB_SUCCESS)
                snprintf(result_plan->err_stat_.err_msg_,MAX_ERROR_MSG,
                         "Can not add table_id to hash table");
            }
            int32_t fromItem_size = main_stmt->get_from_item_size();
            for(int32_t j=0;j<fromItem_size;j++)
            {
              agg_sub_stmt->get_from_items().push_back(main_stmt->get_from_items().at(j));
            }

            int32_t column_size = main_stmt->get_column_size();
            for(int32_t j=0;j<column_size;j++)
            {
              agg_sub_stmt->get_column_items().push_back(main_stmt->get_column_items().at(j));
            }

            int32_t cond_num = main_stmt->get_condition_size();
            for(int32_t j=0;j<cond_num;j++)
            {
              agg_sub_stmt->get_where_exprs().push_back(main_stmt->get_condition_id(j));
            }
            ret = add_limit_one_expr(logical_plan,agg_sub_stmt,result_plan);
          }

          if(ret == OB_SUCCESS)
          {
            ObString db_name;
            ObString table_name = main_stmt->get_table_item(0).table_name_;
            ObString alias_name;
            uint64_t new_table_id = OB_INVALID_ID;
            if(OB_SUCCESS != (ret = agg_main_stmt->add_table_item(
                                *result_plan,
                                db_name,
                                table_name,
                                alias_name,
                                new_table_id,
                                TableItem::GENERATED_TABLE,
                                agg_sub_stmt_query_id,
                                true
                                )))
            {
              YYSYS_LOG(ERROR,"add table item to agg main stmt error %d",ret);
            }
            else if(OB_SUCCESS != (ret = agg_main_stmt->add_from_item(new_table_id)))
            {
              YYSYS_LOG(ERROR,"add from item to agg main stmt error ret %d",ret);
            }
            else
            {
              int32_t size = main_stmt->get_select_item_size();
              bool is_first_agg = false;
              for(int32_t j=select_item_index;ret == OB_SUCCESS && j<size;j++)
              {
                const SelectItem& sel_item = main_stmt->get_select_item(j);
                ObSqlRawExpr *sql_expr = logical_plan->get_expr(sel_item.expr_id_);
                ObBinaryRefRawExpr *b_expr = dynamic_cast<ObBinaryRefRawExpr *>(sql_expr->get_expr());
                if(b_expr != NULL && is_agg_select_item(logical_plan,main_stmt,b_expr->get_second_ref_id()))
                {
                  if(!is_first_agg)
                  {
                    is_first_agg = true;
                  }
                  else
                  {
                    break;
                  }
                  select_item_index++;
                  ObSqlRawExpr *agg_select_expr = NULL;
                  ret = create_aggfunc_select_item(logical_plan,result_plan,agg_main_stmt,agg_select_expr,main_columnItem->column_name_,agg_expr->is_param_distinct());
                  if(ret != OB_SUCCESS || agg_select_expr == NULL)
                  {
                    YYSYS_LOG(ERROR,"create aggfunc select item error");
                  }
                  else if(OB_SUCCESS != (ret = agg_main_stmt->add_select_item(
                                           agg_select_expr->get_expr_id(),
                                           sel_item.is_real_alias_,
                                           sel_item.alias_name_,
                                           sel_item.expr_name_,
                                           agg_select_expr->get_result_type())))
                  {
                    snprintf(result_plan->err_stat_.err_msg_,MAX_ERROR_MSG,
                             "Add select item error");
                  }
                }
                else if(b_expr != NULL)
                {
                  select_item_index++;
                  ColumnItem *col_item = main_stmt->get_column_item_by_id(b_expr->get_first_ref_id(),b_expr->get_second_ref_id());
                  ColumnItem *column_item = agg_main_stmt->get_column_item(NULL,col_item->column_name_);
                  if(column_item == NULL)
                  {
                    ColumnItem *new_col_item = NULL;
                    ret = agg_main_stmt->add_column_item(
                            *result_plan,
                            col_item->column_name_,
                            NULL,
                            &new_col_item,
                            true);
                    if(ret != OB_SUCCESS)
                    {
                      YYSYS_LOG(ERROR,"add column item failed %d",ret);
                      break;
                    }
                    else
                    {
                      column_item = new_col_item;
                    }
                  }
                  ObSqlRawExpr *sql_expr = NULL;
                  ret = create_column_expr(
                          logical_plan,
                          result_plan,
                          agg_main_stmt,
                          sql_expr,
                          new_table_id,
                          column_item->column_id_,
                          b_expr->get_result_type());
                  if(ret != OB_SUCCESS)
                  {
                    YYSYS_LOG(ERROR,"create_column_expr failed ret [%d]", ret);
                    break;
                  }
                  else
                  {
                    uint64_t expr_id = sql_expr->get_expr_id();
                    ret = agg_main_stmt->add_select_item(
                            expr_id,
                            sel_item.is_real_alias_,
                            sel_item.alias_name_,
                            sel_item.expr_name_,
                            sql_expr->get_result_type());

                    if(ret != OB_SUCCESS)
                    {
                      YYSYS_LOG(ERROR,"add select item failed %d",ret);
                      break;
                    }
                  }
                }
              }
            }

          }
        }
      }
      return ret;
    }

    int ObOptimizerLogical::create_aggfunc_select_item(
        ObLogicalPlan *&logical_plan,
        ResultPlan *result_plan,
        ObSelectStmt *&agg_main_stmt,
        ObSqlRawExpr *&agg_select_sql_expr,
        const ObString &column_name,
        bool is_param_distinct)
    {
      int ret = OB_SUCCESS;
      uint64_t select_sql_expr_id = OB_INVALID_ID;
      ObSqlRawExpr *select_sql_expr = (ObSqlRawExpr *) parse_malloc(sizeof(ObSqlRawExpr),result_plan->name_pool_);
      if(select_sql_expr != NULL)
      {
        select_sql_expr = new(select_sql_expr) ObSqlRawExpr();
        ret = logical_plan->add_expr(select_sql_expr);
        select_sql_expr_id = logical_plan->generate_expr_id();
        select_sql_expr->set_expr_id(select_sql_expr_id);
      }
      uint64_t agg_sql_expr_id = OB_INVALID_ID;
      ObSqlRawExpr *agg_sql_expr = (ObSqlRawExpr *) parse_malloc(sizeof(ObSqlRawExpr),result_plan->name_pool_);
      if(agg_sql_expr != NULL)
      {
        agg_sql_expr = new(agg_sql_expr) ObSqlRawExpr();
        ret = logical_plan->add_expr(agg_sql_expr);
        agg_sql_expr_id= logical_plan->generate_expr_id();
        agg_sql_expr->set_expr_id(agg_sql_expr_id);
        agg_main_stmt->add_agg_func(agg_sql_expr_id);
        agg_sql_expr->set_table_id(OB_INVALID_ID);
        agg_sql_expr->set_column_id(logical_plan->generate_column_id());

        ObAggFunRawExpr *agg_raw_expr = (ObAggFunRawExpr *)parse_malloc(sizeof(ObAggFunRawExpr),result_plan->name_pool_);
        if(agg_raw_expr != NULL)
        {
          agg_raw_expr = new(agg_raw_expr) ObAggFunRawExpr();
          ColumnItem *column_item = agg_main_stmt->get_column_item(NULL,column_name);
          if(column_item == NULL)
          {
            ret = agg_main_stmt->add_column_item(
                    *result_plan,
                    column_name,
                    NULL,
                    &column_item,
                    true);
          }

          if(ret == OB_SUCCESS && column_item != NULL)
          {
            ObBinaryRefRawExpr *b_expr = (ObBinaryRefRawExpr *) parse_malloc(sizeof(ObBinaryRefRawExpr),result_plan->name_pool_);
            if(b_expr != NULL)
            {
              b_expr = new(b_expr) ObBinaryRefRawExpr();
            }
            b_expr->set_expr_type(T_REF_COLUMN);
            b_expr->set_result_type(column_item->data_type_);
            b_expr->set_first_ref_id(column_item->table_id_);
            b_expr->set_second_ref_id(column_item->column_id_);

            agg_sql_expr->get_tables_set().add_member(agg_main_stmt->get_table_bit_index(column_item->table_id_));
            if(agg_main_stmt->get_table_bit_index(column_item->table_id_) <0)
            {
              YYSYS_LOG(ERROR,"negative bitmap values,table_id =%ld",column_item->table_id_);
            }
            agg_raw_expr->set_param_expr(b_expr);
            agg_raw_expr->set_expr_type(T_FUN_MIN);
            agg_raw_expr->set_result_type(b_expr->get_result_type());
            if(is_param_distinct)
            {
              agg_raw_expr->set_param_distinct();
            }
            agg_sql_expr->set_expr(agg_raw_expr);
            agg_sql_expr->set_contain_aggr(true);
            agg_sql_expr->get_tables_set().add_member(0);

            ObBinaryRefRawExpr *select_col_expr = (ObBinaryRefRawExpr *)parse_malloc(sizeof(ObBinaryRefRawExpr),result_plan->name_pool_);
            if(select_col_expr != NULL)
            {
              select_col_expr = new(select_col_expr) ObBinaryRefRawExpr();
              select_col_expr->set_expr_type(T_REF_COLUMN);
              select_col_expr->set_result_type(agg_sql_expr->get_result_type());
              select_col_expr->set_first_ref_id(agg_sql_expr->get_table_id());
              select_col_expr->set_second_ref_id(agg_sql_expr->get_column_id());
              select_sql_expr->get_tables_set().add_member(0);
              select_sql_expr->set_contain_aggr(true);

              select_sql_expr->set_table_id(OB_INVALID_ID);
              select_sql_expr->set_column_id(logical_plan->generate_column_id());
              select_sql_expr->set_expr(select_col_expr);

              agg_select_sql_expr = select_sql_expr;
            }
          }
        }
      }
      return ret;
    }

    bool ObOptimizerLogical::if_can_eliminate(ObSelectStmt *&main_stmt)
    {
      bool if_can_eliminate = false;
      if(main_stmt->get_stmt_type() != ObSelectStmt::T_SELECT)
      {

      }
      else if(main_stmt->is_for_update())
      {

      }
      else if(main_stmt->get_set_op() != ObSelectStmt::NONE)
      {

      }
      else if(main_stmt->has_limit() ||
              main_stmt->is_distinct() ||
              main_stmt->is_show_stmt() ||
              main_stmt->has_limit() ||
              main_stmt->get_group_expr_size() >0 ||
              main_stmt->get_having_expr_size() >0 ||
              main_stmt->get_order_item_size() >0 ||
              main_stmt->get_when_expr_size() >0 ||
              main_stmt->get_has_range() ||
              main_stmt->has_sequence()
              )
      {

      }
      else if(main_stmt->get_anal_fun_size()>0 ||
              main_stmt->get_partition_expr_size()>0 ||
              main_stmt->get_order_item_for_rownum_size()>0)
      {

      }
      else if(main_stmt->get_query_hint().is_has_hint_)
      {

      }
      else
      {
        if_can_eliminate = true;
      }
      return if_can_eliminate;
    }

    bool ObOptimizerLogical::is_select_items_qualified(
        ObLogicalPlan *&logical_plan,
        ObSelectStmt *&main_stmt)
    {
      bool is_select_items_qulified = true;
      int32_t size = main_stmt->get_select_item_size();
      for(int32_t i=0;i<size;i++)
      {
        const SelectItem& sel_item = main_stmt->get_select_item(i);
        ObSqlRawExpr *sql_expr = logical_plan->get_expr(sel_item.expr_id_);
        ObRawExpr *raw_expr = sql_expr->get_expr();
        ObAggFunRawExpr *agg_expr = dynamic_cast<ObAggFunRawExpr*>(raw_expr);
        ObBinaryRefRawExpr *b_expr = dynamic_cast<ObBinaryRefRawExpr *>(raw_expr);
        if(agg_expr == NULL && b_expr == NULL)
        {
          is_select_items_qulified = false;
          break;
        }
      }
      return is_select_items_qulified;
    }

    bool ObOptimizerLogical::is_agg_select_item(
        ObLogicalPlan *&logical_plan,
        ObSelectStmt *&main_stmt,
        uint64_t column_id)
    {
      bool is_agg_select_item = false;
      int32_t size = main_stmt->get_agg_fun_size();
      for(int32_t i=0;i<size;i++)
      {
        ObSqlRawExpr *sql_expr = logical_plan->get_expr(main_stmt->get_agg_expr_id(i));
        if(sql_expr != NULL)
        {
          if(sql_expr->get_column_id() ==column_id)
          {
            is_agg_select_item = true;
            break;
          }
        }
      }
      return is_agg_select_item;
    }

    bool ObOptimizerLogical::is_select_item_contain_col(
        ObLogicalPlan *&logical_plan,
        ObSelectStmt *&main_stmt,
        uint64_t column_id)
    {
      bool is_contain_col = false;
      int32_t size = main_stmt->get_select_item_size();
      for(int32_t i=0;i<size;i++)
      {
        const SelectItem& sel_item = main_stmt->get_select_item(i);
        ObSqlRawExpr *sql_expr = logical_plan->get_expr(sel_item.expr_id_);
        ObRawExpr *raw_expr = sql_expr->get_expr();
        switch (raw_expr->get_expr_type())
        {
          case T_REF_COLUMN:
          {
            ObBinaryRefRawExpr *b_expr = dynamic_cast<ObBinaryRefRawExpr*>(raw_expr);
            if(b_expr->get_second_ref_id() == column_id)
            {
              is_contain_col = true;
            }
            break;
          }
          default:
            break;
        }
        if(is_contain_col)
        {
          break;
        }
      }
      return is_contain_col;
    }

    bool ObOptimizerLogical::is_all_agg_cols_rowkey_or_index(
        ObLogicalPlan *&logical_plan,
        ObSelectStmt *&main_stmt,
        const ObSchemaManagerV2 *schema)
    {
      int32_t size = main_stmt->get_agg_fun_size();
      bool all_rowkey_or_index = true;
      for(int32_t i=0;i<size;i++)
      {
        ObAggFunRawExpr *aggExpr = dynamic_cast<ObAggFunRawExpr*>(logical_plan->get_expr(main_stmt->get_agg_expr_id(i))->get_expr());
        ObRawExpr *param_expr = aggExpr->get_param_expr();
        if(param_expr != NULL && param_expr->get_expr_type() == T_REF_COLUMN)
        {
          ObBinaryRefRawExpr *col_expr = dynamic_cast<ObBinaryRefRawExpr*>(param_expr);
          uint64_t table_id = col_expr->get_first_ref_id();
          uint64_t column_id = col_expr->get_second_ref_id();
          const ObTableSchema *table_schema =NULL;
          if(NULL == (table_schema = schema->get_table_schema(table_id)))
          {
            YYSYS_LOG(WARN,"Fail to get table schema for table %ld",table_id);
          }
          else if(!table_schema->get_rowkey_info().is_rowkey_column(column_id))
          {
            uint64_t tmp_index_tid[OB_MAX_INDEX_NUMS];
            for(int32_t m=0;m<OB_MAX_INDEX_NUMS;m++)
            {
              tmp_index_tid[m] = OB_INVALID_ID;
            }
            if(!schema->is_cid_in_index(column_id,table_id,tmp_index_tid))
            {
              all_rowkey_or_index = false;
              break;
            }
          }
        }
      }
      return all_rowkey_or_index;
    }

    int ObOptimizerLogical::add_select_star_columns(
        ObLogicalPlan *&logical_plan,
        ObSelectStmt *&new_stmt,
        ResultPlan *result_plan)
    {
      int ret = OB_SUCCESS;
      int32_t num = new_stmt->get_table_size();
      ColumnItem *column_item = NULL;
      for(int32_t i=0;ret == OB_SUCCESS && i<num;i++)
      {
        TableItem &table_item = new_stmt->get_table_item(i);
        if(table_item.type_ == TableItem::GENERATED_TABLE)
        {
          ObSelectStmt *sub_select = static_cast<ObSelectStmt*>(logical_plan->get_query(table_item.ref_id_));
          if(sub_select == NULL)
          {
            ret = OB_ERR_ILLEGAL_ID;
            snprintf(result_plan->err_stat_.err_msg_,MAX_ERROR_MSG,
                     "Can not get sub-query whose id = %lu",table_item.ref_id_);
          }
          else
          {
            int32_t num = sub_select->get_select_item_size();
            for(int32_t j=0;ret== OB_SUCCESS && j<num;j++)
            {
              const SelectItem &select_item = sub_select->get_select_item(j);
              column_item = new_stmt->get_column_item_by_id(table_item.table_id_,OB_APP_MIN_COLUMN_ID +j);
              if(column_item == NULL)
              {
                ColumnItem new_column_item;
                new_column_item.column_id_ = OB_APP_MIN_COLUMN_ID +j;
                if((ret = ob_write_string(*new_stmt->get_name_pool(),
                                          select_item.alias_name_,
                                          new_column_item.column_name_))!= OB_SUCCESS)
                {
                  ret = OB_ERR_PARSER_MALLOC_FAILED;
                  YYSYS_LOG(WARN, "out of memory");
                  snprintf(result_plan->err_stat_.err_msg_,MAX_ERROR_MSG,
                           "Can not malloc space for column name");
                  break;
                }

                new_column_item.table_id_ = table_item.table_id_;
                new_column_item.query_id_ = 0;
                new_column_item.is_name_unique_ = false;
                new_column_item.is_group_based_ = false;
                new_column_item.data_type_ = select_item.type_;
                ret = new_stmt->add_column_item(new_column_item);
                if(ret != OB_SUCCESS)
                {
                  snprintf(result_plan->err_stat_.err_msg_,MAX_ERROR_MSG,
                           "Add column error");
                  break;
                }
                column_item = &new_column_item;
              }
              if(ret == OB_SUCCESS)
              {
                ObSqlRawExpr *sql_expr = NULL;
                if(OB_SUCCESS!= (ret = create_column_expr(logical_plan,
                                                          result_plan,
                                                          new_stmt,
                                                          sql_expr,
                                                          column_item->table_id_,
                                                          column_item->column_id_,
                                                          column_item->data_type_
                                                          )))
                {
                  YYSYS_LOG(ERROR,"create column expr error %d",ret);
                  break;
                }
                else if(OB_SUCCESS != (ret = new_stmt->add_select_item(
                                         sql_expr->get_expr_id(),
                                         false,
                                         select_item.alias_name_,
                                         select_item.expr_name_,
                                         select_item.type_)))
                {
                  snprintf(result_plan->err_stat_.err_msg_,MAX_ERROR_MSG,
                           "Can not add select item");
                  break;
                }
              }
            }
          }
        }
      }
      return ret;
    }

    int ObOptimizerLogical::add_limit_one_expr(ObLogicalPlan *&logical_plan, ObSelectStmt *&main_stmt, ResultPlan *result_plan)
    {
      int ret = OB_SUCCESS;
      uint64_t limit_count_expr_id = OB_INVALID_ID;
      uint64_t limit_offset_expr_id = OB_INVALID_ID;
      ObSqlRawExpr *limit_sql_expr = (ObSqlRawExpr*)parse_malloc(sizeof(ObSqlRawExpr),result_plan->name_pool_);
      if(limit_sql_expr == NULL)
      {
        ret = OB_ERR_PARSER_MALLOC_FAILED;
        YYSYS_LOG(WARN, "out of memory");
        snprintf(result_plan->err_stat_.err_msg_,MAX_ERROR_MSG,
                 "Can not malloc space for YaoSqlRawExpr");
      }
      if(ret == OB_SUCCESS)
      {
        limit_sql_expr = new(limit_sql_expr) ObSqlRawExpr();
        ret = logical_plan->add_expr(limit_sql_expr);
        if(ret != OB_SUCCESS)
        {
          snprintf(result_plan->err_stat_.err_msg_,MAX_ERROR_MSG,
                   "Add YaoSqlRawExpr error");
        }
        if(ret == OB_SUCCESS)
        {
          limit_count_expr_id = logical_plan->generate_expr_id();
          limit_sql_expr->set_expr_id(limit_count_expr_id);

          ObObj val;
          val.set_int(1);
          ObConstRawExpr *c_expr = (ObConstRawExpr*) parse_malloc(sizeof(ObConstRawExpr),result_plan->name_pool_);
          if(c_expr != NULL)
          {
            c_expr = new(c_expr) ObConstRawExpr();
            c_expr->set_expr_type(T_INT);
            c_expr->set_result_type(ObIntType);
            c_expr->set_value(val);

            limit_sql_expr->set_table_id(OB_INVALID_ID);
            limit_sql_expr->set_column_id(logical_plan->generate_column_id());
            limit_sql_expr->set_expr(c_expr);

            main_stmt->set_limit_offset(limit_count_expr_id,limit_offset_expr_id);
          }
        }
      }
      return ret;
    }

    int ObOptimizerLogical::create_column_expr(
        ObLogicalPlan *&logical_plan,
        ResultPlan *result_plan,
        ObSelectStmt *&select_stmt,
        ObSqlRawExpr *&ret_sql_expr,
        uint64_t table_id,
        uint64_t column_id,
        ObObjType type)
    {
      int ret = OB_SUCCESS;
      ObSqlRawExpr* sql_expr = (ObSqlRawExpr*)parse_malloc(sizeof(ObSqlRawExpr),result_plan->name_pool_);
      uint64_t expr_id = OB_INVALID_ID;
      if(sql_expr == NULL)
      {
        ret = OB_ERR_PARSER_MALLOC_FAILED;
        YYSYS_LOG(WARN, "out of memory");
        snprintf(result_plan->err_stat_.err_msg_,MAX_ERROR_MSG,
                 "Can not malloc space for YaoSqlRawExpr");
      }
      else
      {
        sql_expr = new(sql_expr) ObSqlRawExpr();
        ret = logical_plan->add_expr(sql_expr);
        if(ret != OB_SUCCESS)
          snprintf(result_plan->err_stat_.err_msg_,MAX_ERROR_MSG,
                   "Add YaoSqlRawExpr error");

        if(ret == OB_SUCCESS)
        {
          expr_id = logical_plan->generate_expr_id();
          sql_expr->set_expr_id(expr_id);
          ObBinaryRefRawExpr *b_expr = (ObBinaryRefRawExpr*) parse_malloc(sizeof(ObBinaryRefRawExpr),result_plan->name_pool_);
          if(b_expr == NULL)
          {
            ret = OB_ERR_PARSER_MALLOC_FAILED;
            YYSYS_LOG(WARN, "out of memory");
            snprintf(result_plan->err_stat_.err_msg_,MAX_ERROR_MSG,
                     "Can not malloc space for YaoRawExpr");
          }
          else
          {
            b_expr = new(b_expr) ObBinaryRefRawExpr();
            b_expr->set_expr_type(T_REF_COLUMN);
            b_expr->set_result_type(type);
            b_expr->set_first_ref_id(table_id);
            b_expr->set_second_ref_id(column_id);
            sql_expr->get_tables_set().add_member(select_stmt->get_table_bit_index(table_id));
            sql_expr->set_table_id(OB_INVALID_ID);
            sql_expr->set_column_id(logical_plan->generate_column_id());
            sql_expr->set_expr(b_expr);

            ret_sql_expr = sql_expr;
          }
        }
      }
      return ret;
    }

    int ObOptimizerLogical::add_on_expr(
        ObLogicalPlan *&logical_plan,
        ResultPlan *result_plan,
        ObVector<uint64_t> &join_exprs)
    {
      int ret = OB_SUCCESS;
      uint64_t on_expr_id = OB_INVALID_ID;
      ObSqlRawExpr* on_expr = (ObSqlRawExpr*) parse_malloc(sizeof(ObSqlRawExpr),result_plan->name_pool_);
      if(on_expr != NULL)
      {
        on_expr = new(on_expr) ObSqlRawExpr();
        ret = logical_plan->add_expr(on_expr);
        if(ret == OB_SUCCESS)
        {
          on_expr_id = logical_plan->generate_expr_id();
          on_expr->set_expr_id(on_expr_id);

          ObObj val;
          val.set_int(1);
          ObConstRawExpr *sub_expr1 = (ObConstRawExpr*) parse_malloc(sizeof(ObConstRawExpr),result_plan->name_pool_);
          if(sub_expr1 != NULL)
          {
            sub_expr1 = new(sub_expr1) ObConstRawExpr();
            sub_expr1->set_expr_type(T_INT);
            sub_expr1->set_result_type(ObIntType);
            sub_expr1->set_value(val);
          }

          ObObj val2;
          val2.set_int(1);
          ObConstRawExpr *sub_expr2 = (ObConstRawExpr*) parse_malloc(sizeof(ObConstRawExpr),result_plan->name_pool_);
          if(sub_expr2 != NULL)
          {
            sub_expr2 = new(sub_expr2) ObConstRawExpr();
            sub_expr2->set_expr_type(T_INT);
            sub_expr2->set_result_type(ObIntType);
            sub_expr2->set_value(val2);
          }

          ObBinaryOpRawExpr *b_expr = (ObBinaryOpRawExpr*) parse_malloc(sizeof(ObBinaryOpRawExpr),result_plan->name_pool_);
          if(b_expr != NULL)
          {
            b_expr = new(b_expr) ObBinaryOpRawExpr();
            b_expr->set_expr_type(T_OP_EQ);
            b_expr->set_result_type(ObBoolType);
            b_expr->set_op_exprs(sub_expr1,sub_expr2);
            on_expr->set_expr(b_expr);
          }
          join_exprs.push_back(on_expr_id);
        }
      }
      return ret;
    }
  }
}



