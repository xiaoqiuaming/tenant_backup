#ifndef OB_UPDATE_ROWKEY_H
#define OB_UPDATE_ROWKEY_H
#include "ob_no_children_phy_operator.h"
#include "mergeserver/ob_ms_rpc_proxy.h"
#include "ob_physical_plan.h"
#include "ob_result_set.h"
#include "ob_sql_session_info.h"
#include "ob_ups_executor.h"
#include "obmysql/ob_mysql_result_set.h"
#include "common/ob_row_store.h"
#include "common/ob_row_util.h"
#include "ob_fill_values.h"
#include "ob_bind_values.h"
#include "ob_table_rpc_scan.h"

namespace oceanbase
{
    namespace sql
    {
        class ObUpdateRowkey : public ObNoChildrenPhyOperator
        {
            public:
                ObUpdateRowkey();

                virtual ~ObUpdateRowkey();

                virtual void reset();

                virtual void reuse();

                virtual int open();

                virtual int close()
                { return common::OB_SUCCESS; }

                virtual int64_t to_string(char *buf, const int64_t buf_len) const;

                virtual int get_next_row(const common::ObRow *&row)
                {
                    UNUSED(row);
                    return common::OB_NOT_SUPPORTED;
                }
                virtual int get_row_desc(const common::ObRowDesc *&row_desc) const
                {
                    UNUSED(row_desc);
                    return common::OB_NOT_SUPPORTED;
                }

                int set_row_desc(const common::ObRowDesc &row_desc);

                int set_row_desc_ext(const common::ObRowDescExt &row_desc_ext);

                virtual enum ObPhyOperatorType get_type() const
                { return PHY_UPDATE_ROWKEY; }
                void set_sql_context(ObSqlContext sql_context)
                { sql_context_ = sql_context; }
                void set_select_plan(ObUpsExecutor *ups_executor)
                { ob_ups_executor_select_ = ups_executor; }
                void set_delete_plan(ObUpsExecutor *ups_executor)
                { ob_ups_executor_delete_ = ups_executor; }
                void set_insert_plan(ObUpsExecutor *ups_executor)
                { ob_ups_executor_insert_ = ups_executor; }
                void set_select_more_rows(ObPhyOperator *table_rpc_scan)
                { ob_select_more_rows_rpc_ = table_rpc_scan; }
                void set_rowkey_info(const ObRowkeyInfo *rowkey_info)
                { rowkey_info_ = rowkey_info; }
                void set_is_multi_update(bool is_multi_update)
                { is_multi_update_ = is_multi_update; }
                void set_sql_read_atratege(ObSqlReadStrategy &sql_read_strategy)
                { ob_sql_read_strategy_ = sql_read_strategy; }

                int add_sql_expression(ObSqlExpression sql_expression);

                int add_value_type(const ObObjType obj_type);

                int set_op(ObTableRpcScan *table_rpc_scan, ObExprValues *expr_values);

                int cons_insert_rpc_scan_and_expr_values(ObArray<const ObRowStore::StoredRow *> old_rows, ObRowDesc row_desc);

                int set_stmt_start_time();
                int execute_stmt_no_return_rows(const ObString &stmt);

                int start_transaction();

                int commit();

                int rollback();

                int select_more_rows_from_table(bool &empty);

                void set_affect_row(int64_t affected_row);

                void get_fill_bind_values(ObFillValues *&fill_values, ObBindValues *&bind_values);

                void get_affect_rows(int64_t &affected_row_, ObFillValues *fill_values, ObBindValues *bind_values);
                bool is_batch_over(ObFillValues *&fill_values, ObBindValues *&bind_values);

                DECLARE_PHY_OPERATOR_ASSIGN;
            private:
                ObRowDesc row_desc_;
                ObRowDescExt row_desc_ext_;
                ObArray<const ObRowStore::StoredRow *> old_rows_;
                ObUpsExecutor *ob_ups_executor_select_;
                ObUpsExecutor *ob_ups_executor_delete_;
                ObUpsExecutor *ob_ups_executor_insert_;
                ObPhyOperator *ob_select_more_rows_rpc_;
                const common::ObRowkeyInfo *rowkey_info_;
                ObExprValues *ob_expr_values_;
                ObSqlReadStrategy ob_sql_read_strategy_;
                ObTableRpcScan *ob_table_rpc_scan_;
                common::ObArray<ObSqlExpression> ob_sql_expressions_;
                common::ObArray<ObObjType> ob_value_types_;
                ObSqlContext sql_context_;
                ObRowStore row_store;
                bool is_multi_update_;
        };
    }// end namespace sql
}//end namespace oceanbase

#endif // OB_UPDATE_ROWKEY_H
