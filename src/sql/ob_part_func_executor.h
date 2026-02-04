#ifndef OB_PART_FUNC_EXECUTOR_H
#define OB_PART_FUNC_EXECUTOR_H

#include "sql/ob_no_children_phy_operator.h"
#include "sql/ob_sql_context.h"
#include "sql/ob_create_part_func_stmt.h"
#include "sql/ob_drop_part_func_stmt.h"
#include "mergeserver/ob_ms_partition_manager.h"
namespace oceanbase
{
  namespace sql
  {
    class ObPartFuncExecutor : public ObNoChildrenPhyOperator
    {
    public:
      ObPartFuncExecutor();
      virtual ~ObPartFuncExecutor();
      virtual int open();
      virtual int close();
      virtual void reset();
      virtual void reuse();
      virtual ObPhyOperatorType get_type() const
      { return PHY_PART_EXECUTOR; }
      virtual int64_t to_string(char* buf, const int64_t buf_len) const;
      /// @note always return OB_ITER_END
      virtual int get_next_row(const common::ObRow *&row);
      /// @note always return OB_NOT_SUPPORTED
      virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
      virtual void set_stmt(const ObBasicStmt *stmt);
      const ObBasicStmt* get_stmt() const;
      int do_create_part_func(const ObBasicStmt *stmt);
      int do_drop_part_func(const ObBasicStmt *stmt);
      int get_param_list(char* params) const;
      int execute_stmt_no_return_rows(const ObString &stmt);
      int execute_delete_func(const ObString &stmt,ObString &func_name);
      int start_transaction();
      int commit();
      int rollback();

      int get_next_row(ObSQLResultSet &result_set, ObRow &row);//add wuna [MultiUps][sql_api] 20160301
      int check_if_part_func_using(ObString& func_name,bool& is_using);//add wuna [MultiUps][sql_api] 20160301
      void set_context(ObSqlContext* sql_context);
      const ObSqlContext* get_sql_context() const;
    private:
      const ObBasicStmt *stmt_;
      ObSqlContext *context_;
      ObResultSet *result_set_out_;
    };

    inline void ObPartFuncExecutor::set_stmt(const ObBasicStmt *stmt)
    {
      stmt_ = stmt;
    }

    inline const ObBasicStmt* ObPartFuncExecutor::get_stmt() const
    {
      return stmt_;
    }
    //add wuna [MultiUps][sql_api] 20160301:b
    inline int ObPartFuncExecutor::get_next_row(ObSQLResultSet &result_set, ObRow &row)
    {
      int ret = OB_SUCCESS;
      ret = result_set.get_new_scanner().get_next_row(row);
      return ret;
    }
    //add 20160301;e

    inline int ObPartFuncExecutor::get_next_row(const common::ObRow *&row)
    {
      row = NULL;
      return common::OB_ITER_END;
    }

    inline int ObPartFuncExecutor::get_row_desc(const common::ObRowDesc *&row_desc) const
    {
      row_desc = NULL;
      return common::OB_NOT_SUPPORTED;
    }

    inline const ObSqlContext* ObPartFuncExecutor::get_sql_context() const
    {
      return context_;
    }
  }
}


#endif // OB_CREATE_HASH_FUNC_H
