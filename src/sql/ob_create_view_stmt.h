//add lvjc [create view]
#ifndef OCEANBASE_SQL_OB_CREATE_VIEW_STMT_H_
#define OCEANBASE_SQL_OB_CREATE_VIEW_STMT_H_

#include "common/ob_array.h"
#include "common/ob_string_buf.h"
#include "common/ob_string.h"
#include "sql/parse_node.h"
#include "sql/ob_basic_stmt.h"
#include "sql/ob_select_stmt.h"
#include "sql/ob_rewrite_query_stmt.h"

namespace oceanbase
{
  namespace sql
  {
    enum ObViewCheckOption
    {
      OB_WITH_NONE_CHECK_OPTION = 0,
      OB_WITH_CASCADED_CHECK_OPTION = 1,
      OB_WITH_LOCAL_CHECK_OPTION = 2,
    };
    
    class ObCreateViewStmt : public ObBasicStmt
    {
    public:
      explicit ObCreateViewStmt(common::ObStringBuf* name_pool);

      virtual ~ObCreateViewStmt();

      bool get_do_replace() const;

      const common::ObString &get_view_name() const;

      const common::ObString &get_db_name() const;

      const ObViewCheckOption get_with_check_option() const;

      common::ObArray<ObString> *get_column_list();

      int64_t get_column_size();

      const char *get_sql_text() const;

      void set_or_replace(bool or_replace);

      void set_with_check_option(ObViewCheckOption with_check_option);

      int set_view_name(ResultPlan &result_plan, const common::ObString &view_name, const bool is_alter);

      int set_db_name(ResultPlan &result_plan, const common::ObString &dbname);

      int add_column_name(ResultPlan &result_plan, const ObString &column_name);

      void set_sql_text(const char *text);

      ObRewriteQueryStmt *get_rewrite_query();

      virtual void print(FILE *fp, int32_t level, int32_t index = 0);

      void set_select_query_id(const uint64_t query_id);

      uint64_t get_select_query_id() const;

    protected:
      common::ObStringBuf *name_pool_;
    
    private:
      bool or_replace_;
      ObViewCheckOption with_check_option_;
      common::ObString view_name_;
      common::ObString dbname_;
      char text_[OB_MAX_SQL_LENGTH];
      ObRewriteQueryStmt rewrite_query_stmt_;
      common::ObArray<ObString> column_names_;
      uint64_t select_query_id_;
    };

    inline void ObCreateViewStmt::set_or_replace(bool or_replace)
    {
      or_replace_ = or_replace;
    }

    inline void ObCreateViewStmt::set_with_check_option(ObViewCheckOption with_check_option)
    {
      with_check_option_ = with_check_option;
    }

    inline const common::ObString &ObCreateViewStmt::get_view_name() const
    {
      return view_name_;
    }

    inline const common::ObString &ObCreateViewStmt::get_db_name() const
    {
      return dbname_;
    }

    inline bool ObCreateViewStmt::get_do_replace() const
    {
      return or_replace_;
    }

    inline const ObViewCheckOption ObCreateViewStmt::get_with_check_option() const
    {
      return with_check_option_;
    }

    inline const char* ObCreateViewStmt::get_sql_text() const
    {
      return text_;
    }

    inline ObRewriteQueryStmt *ObCreateViewStmt::get_rewrite_query()
    {
      return &rewrite_query_stmt_;
    }

    inline common::ObArray<ObString> *ObCreateViewStmt::get_column_list()
    {
      return &column_names_;
    }

    inline int64_t ObCreateViewStmt::get_column_size()
    {
        return column_names_.count();
    }

    inline uint64_t ObCreateViewStmt::get_select_query_id() const
    {
      return select_query_id_;
    }

    inline void ObCreateViewStmt::set_select_query_id(const uint64_t query_id)
    {
      select_query_id_ = query_id;
    }
  }
}

#endif //OCEANBASE_SQL_OB_CREATE_TABLE_STMT_H_
//add lvjc e
