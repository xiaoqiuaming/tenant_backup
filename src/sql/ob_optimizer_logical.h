#ifndef OB_OPTIMIZER_PLAN_H
#define OB_OPTIMIZER_PLAN_H

#include "sql/ob_logical_plan.h"

namespace oceanbase
{
  namespace sql
  {
    class ObOptimizerLogical
    {
      public:
        ObOptimizerLogical(){}
        ~ObOptimizerLogical(){}

        static int rule_max_min_eliminate(ObLogicalPlan *&logical_plan,ResultPlan *result_plan,const ObSchemaManagerV2 *schema);

        static int eliminate_max_min_union(ObLogicalPlan *&logical_plan,ObSelectStmt *&main_stmt,ResultPlan *result_plan,const ObSchemaManagerV2 *schema);

        static int eliminate_max_min_recursion(ObLogicalPlan *&logical_plan,ObSelectStmt *&main_stmt,ResultPlan *result_plan,const ObSchemaManagerV2 *schema, bool is_gen);
        static int eliminate_single_max_min(ObLogicalPlan *&logical_plan,ObSelectStmt *&main_stmt,ResultPlan *result_plan,const ObSchemaManagerV2 *schema, bool is_gen);
        static int eliminate_multi_max_min(ObLogicalPlan *&logical_plan,ObSelectStmt *&main_stmt,ResultPlan *result_plan,const ObSchemaManagerV2 *schema, bool is_gen);

        static int add_limit_one_expr(ObLogicalPlan *&logical_plan,ObSelectStmt *&main_stmt,ResultPlan *result_plan);

        static int split_multi_mins(
            ObLogicalPlan *&logical_plan,
            ResultPlan *result_plan,
            ObSelectStmt *&main_stmt,
            int32_t main_stmt_index);

        static int create_aggfunc_select_item(
            ObLogicalPlan *&logical_plan,
            ResultPlan *result_plan,
            ObSelectStmt *&agg_main_stmt,
            ObSqlRawExpr *&agg_select_sql_expr,
            const ObString &column_name,
            bool is_param_distinct);

        static bool is_all_agg_cols_rowkey_or_index(
            ObLogicalPlan *&logical_plan,
            ObSelectStmt *&main_stmt,
            const ObSchemaManagerV2 *schema);

        static bool is_select_item_contain_col(
            ObLogicalPlan *&logical_plan,
            ObSelectStmt *&main_stmt,
            uint64_t column_id);

        static bool is_agg_select_item(
            ObLogicalPlan *&logical_plan,
            ObSelectStmt *&main_stmt,
            uint64_t column_id);

        static bool if_can_eliminate(
            ObSelectStmt *&main_stmt);
        static bool is_select_items_qualified(
            ObLogicalPlan *&logical_plan,
            ObSelectStmt *&main_stmt);

        static int create_column_expr(
            ObLogicalPlan *&logical_plan,
            ResultPlan *result_plan,
            ObSelectStmt *&select_stmt,
            ObSqlRawExpr *&ret_sql_expr,
            uint64_t table_id,
            uint64_t column_id,
            ObObjType type);

        static int add_on_expr(
            ObLogicalPlan *&logical_plan,
            ResultPlan *result_plan,
            ObVector<uint64_t> &join_exprs);

        static int add_select_star_columns(
            ObLogicalPlan *&logical_plan,
            ObSelectStmt *&new_stmt,
            ResultPlan *result_plan
            );

        static bool is_simple_subquery(ObSelectStmt *select_stmt);

        static int pull_up_subqueries(ObLogicalPlan *&logical_plan,ObResultSet &result,ResultPlan &result_plan);

        static int pull_up_subquery_union(ObLogicalPlan *&logical_plan,ObSelectStmt *&main_stmt,
                                          ResultPlan &result_plan, std::map<uint64_t,uint64_t>&father_alias_table);

        static int pull_up_subquery_recursion(
            ObLogicalPlan *&logical_plan,ObSelectStmt *&main_stmt, FromItem& from_item,
            ResultPlan &result_plan, std::map<uint64_t,uint64_t>&father_alias_table,
            int32_t from_item_idx);

        static int if_can_pull_up(
            ObLogicalPlan *&logical_plan,ObSelectStmt *subquery_stmt,
            JoinedTable *&joined_table, uint64_t join_type, bool is_joined,
            uint64_t table_id, ObSelectStmt *&main_stmt);

        static int combine_subquery_from_items(ObLogicalPlan *&logical_plan,ObSelectStmt *subquery_stmt,ResultPlan &result_plan,bool is_joined);
        static int adjust_subquery_join_item(
            ObLogicalPlan *&logical_plan, JoinedTable *&joined_table,
            JoinedTable *&sub_joined_table, int64_t sub_joined_position,
            std::map<uint64_t,uint64_t>alias_table_hashmap,
            int64_t join_table_idx,ObSelectStmt *&main_stmt);

        static int pull_up_from_items(
            ObLogicalPlan *&logical_plan,
            ObSelectStmt *&main_stmt,
            ObSelectStmt *subquery_stmt,
            FromItem &from_item,
            std::map<uint64_t,uint64_t>&alias_table_hashmap,
            std::map<uint64_t,uint64_t>&table_id_hashmap,
            std::map<uint64_t,uint64_t>&column_id_hashmap,uint64_t table_id,
            std::map<uint64_t,uint64_t>&father_alias_table,
            int32_t from_item_idx);

        static int pull_up_table_items(ObLogicalPlan *&logical_plan,
                                       ObSelectStmt *&main_stmt,ObSelectStmt *subquery_stmt,
                                       std::map<uint64_t,uint64_t>&alias_table_hashmap,uint64_t table_id);
        static int pull_up_column_items(ObLogicalPlan *&logical_plan,
                                        ObSelectStmt *&main_stmt,ObSelectStmt *subquery_stmt,
                                        std::map<uint64_t,uint64_t>&alias_table_hashmap,
                                        std::map<uint64_t,uint64_t>&table_id_hashmap,
                                        std::map<uint64_t,uint64_t>&column_id_hashmap,uint64_t table_id);

        static int pull_up_select_items(ObLogicalPlan *&logical_plan,
                                        ObSelectStmt *&main_stmt,ObSelectStmt *subquery_stmt,
                                        std::map<uint64_t,uint64_t>&alias_table_hashmap,
                                        std::map<uint64_t,uint64_t>&table_id_hashmap,
                                        std::map<uint64_t,uint64_t>&column_id_hashmap, uint64_t table_id);

        static int pull_up_where_items(ObLogicalPlan *&logical_plan,
                                       ObSelectStmt *&main_stmt,ObSelectStmt *subquery_stmt,
                                       std::map<uint64_t,uint64_t>&alias_table_hashmap,
                                       std::map<uint64_t,uint64_t>&table_id_hashmap,
                                       std::map<uint64_t,uint64_t>&column_id_hashmap,uint64_t table_id);

        static int pull_up_from_join_items(ObLogicalPlan *&logical_plan,
                                           ObSelectStmt *&main_stmt,ObSelectStmt *subquery_stmt,
                                           std::map<uint64_t,uint64_t>&alias_table_hashmap,
                                           std::map<uint64_t,uint64_t>&table_id_hashmap,
                                           std::map<uint64_t,uint64_t>&column_id_hashmap,uint64_t table_id,
                                           JoinedTable *&joined_table);

        static int pull_up_simple_subquery(ObLogicalPlan *&logical_plan,
                                           ObSelectStmt *&main_stmt,FromItem &from_item,TableItem *&table_item,
                                           JoinedTable *&joined_table,uint64_t join_type,ResultPlan &result_plan,
                                           std::map<uint64_t,uint64_t>& father_alias_table,int32_t from_item_idx);
        static int pull_up_simple_subquery_alt(ObLogicalPlan *&logical_plan,
                                               ObSelectStmt *&main_stmt,FromItem &from_item,TableItem *&table_item,
                                               JoinedTable *&joined_table,uint64_t join_type,ResultPlan &result_plan,
                                               std::map<uint64_t,uint64_t>& father_alias_table,int32_t from_item_idx);

        static int set_main_query_alias_table(ObLogicalPlan *&logical_plan,
                                              ObSelectStmt *&main_stmt,ResultPlan &result_plan,
                                              std::map<uint64_t,uint64_t>& father_alias_table);

        static int split_mainquery_from_items(ObLogicalPlan *&logical_plan,
                                              ObSelectStmt *&main_stmt,ResultPlan &result_plan);

      private:
    };

  }
}
#endif
