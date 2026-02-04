//add lvjc [create view]
#ifndef OCEANBASE_SQL_OB_REWRITE_QUERY_H_
#define OCEANBASE_SQL_OB_REWRITE_QUERY_H_

#include "sql/ob_basic_stmt.h"
#include "sql/ob_select_stmt.h"
#include "sql/ob_logical_plan.h"
#include "sql/ob_schema_checker.h"
#include "sql/ob_column_def.h"
#include "common/ob_array.h"
#include "common/ob_string_buf.h"
#include "common/ob_string.h"

namespace oceanbase
{
    namespace sql
    {
        class ObRewriteQueryStmt
        {
            public:
                ObRewriteQueryStmt();
                virtual ~ObRewriteQueryStmt();

                int init(ObLogicalPlan *logical_plan, ObSchemaChecker *schema_checker, ObArray<ObString> *column_list);

                int rewrite_select_clause(ObSelectStmt *select_stmt, char *select_clause, bool main_stmt = false);

                int rewrite_from_clause(ObSelectStmt *select_stmt, char *from_clause);

                int rewrite_group_clause(ObSelectStmt *select_stmt, char *group_clause);

                int rewrite_having_clause(ObSelectStmt *select_stmt, char *having_clause);

                int rewrite_order_clause(ObSelectStmt *select_stmt, char *order_clause);

                int rewrite_limit_clause(ObSelectStmt *select_stmt, char *limit_clause);

                int rewrite_where_clause(ObSelectStmt *select_stmt, char *where_clause);

                int rewrite_select_stmt(ObSelectStmt *select_stmt, char *sql_text, bool main_stmt = false);

            private:
                //根据expr的类型，获取expr对应的字符串
                int rewrite_independ_expr(ObSelectStmt *select_stmt, ObRawExpr *expr, char *text, int64_t &pos, bool *column_is_null = NULL);

                int rewrite_table_item(ObSelectStmt *select_stmt, const uint64_t table_id, char *from_clause, int64_t &pos);

                int rewrite_joined_table(ObSelectStmt *select_stmt, const JoinedTableInfo* joined_table, char *from_clause, int64_t &pos);

                void split(const char *complete_name, char *db_name, char *table_name, bool &is_sys_table);

            private:
                ObLogicalPlan *logical_plan_;
                ObSchemaChecker *schema_checker_;
                common::ObArray<ObString> *column_list_;
        };
    }
}

#endif
//add lvjc e
