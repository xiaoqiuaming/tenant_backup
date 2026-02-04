
#ifndef OB_GATHER_STATISTICS_STMT_H
#define OB_GATHER_STATISTICS_STMT_H

#include "common/ob_array.h"
#include "common/ob_string.h"
#include "common/ob_strings.h"
#include "common/ob_string_buf.h"
#include "common/ob_rowkey.h"
#include "common/ob_schema.h"
#include "sql/ob_basic_stmt.h"
#include "parse_node.h"
#include "ob_schema_checker.h"

namespace oceanbase{
  namespace sql{
    class ObGatherStatisticsStmt : public ObBasicStmt
    {
    public:
      explicit ObGatherStatisticsStmt(common::ObStringBuf* name_pool);
      virtual ~ObGatherStatisticsStmt();

      int add_statistics_columns(ResultPlan& result_plan, const common::ObString tname, const common::ObString column_name);

      int64_t get_statistics_colums_count() const;

      int set_statistics_table(ResultPlan& result_plan, const common::ObString tname);

      uint64_t get_statistics_table_id() const;

      uint64_t get_column_id(int64_t idx) const;

      const common::ObString& get_table_name() const;

      void set_if_exists(bool if_exists);

      bool get_if_exists() const;

      int set_row_key_info(ResultPlan& result_plan, const common::ObString tname);

      ObRowkeyInfo &get_row_key_info();

      void print(FILE* fp, int32_t level, int32_t index = 0);

    protected:
      common::ObStringBuf*        name_pool_;

    private:
      common::ObArray<uint64_t>   statistics_column_ids_;
      uint64_t                    statistics_table_id_;
      bool                        if_exists_;
      common::ObRowkeyInfo        row_key_info_;
      common::ObString            table_name_;
      char tname_[OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH];
    };

    inline const common::ObString& ObGatherStatisticsStmt::get_table_name() const
    {
      return table_name_;
    }

    inline uint64_t ObGatherStatisticsStmt::get_column_id(int64_t idx) const
    {
      if (idx < 0 || idx >= statistics_column_ids_.count())
      {
        YYSYS_LOG(WARN, "Invalid index!");
        return OB_INVALID_ID;
      }
      else
      {
        return statistics_column_ids_.at(idx);
      }
    }

    inline void ObGatherStatisticsStmt::set_if_exists(bool if_exists)
    {
      if_exists_ = if_exists;
    }

    inline bool ObGatherStatisticsStmt::get_if_exists() const
    {
      return if_exists_;
    }

    inline int64_t ObGatherStatisticsStmt::get_statistics_colums_count() const
    {
      return statistics_column_ids_.count();
    }

    inline uint64_t ObGatherStatisticsStmt::get_statistics_table_id() const
    {
      return statistics_table_id_;
    }

    inline int ObGatherStatisticsStmt::set_row_key_info(ResultPlan& result_plan, const ObString tname)
    {
      int ret = OB_SUCCESS;
      uint64_t table_id = common::OB_INVALID_ID;
      ObSchemaChecker* schema_checker = NULL;
      schema_checker = static_cast<ObSchemaChecker*>(result_plan.schema_checker_);
      if (NULL == schema_checker)
      {
        ret = common::OB_ERR_SCHEMA_UNSET;
        snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
            "Schema(s) are not set");
      }
      if (OB_SUCCESS == ret)
      {
        if (common::OB_INVALID_ID == (table_id = schema_checker->get_table_id(tname)))
        {
          ret = common::OB_ERR_TABLE_UNKNOWN;
          snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
              "the table to gather statistics '%.*s' is not exist", tname.length(), tname.ptr());
        }
        else
        {
          row_key_info_ = schema_checker->get_table_schema(table_id)->get_rowkey_info();
        }
      }
      return ret;
    }

    inline int ObGatherStatisticsStmt::set_statistics_table(ResultPlan& result_plan, const ObString tname)
    {
      int ret = OB_SUCCESS;
      uint64_t table_id = common::OB_INVALID_ID;
      ObSchemaChecker* schema_checker = NULL;
      schema_checker = static_cast<ObSchemaChecker*>(result_plan.schema_checker_);
      if (NULL == schema_checker)
      {
        ret = common::OB_ERR_SCHEMA_UNSET;
        snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
            "Schema(s) are not set");
      }
      if (OB_SUCCESS == ret)
      {
        if (common::OB_INVALID_ID == (table_id = schema_checker->get_table_id(tname)))
        {
          ret = common::OB_ERR_TABLE_UNKNOWN;
          snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
              "the table to gather statistics '%.*s' is not exist", tname.length(), tname.ptr());
        }
      }
      if (OB_SUCCESS == ret)
      {
        statistics_table_id_ = table_id;
        table_name_.assign_buffer(tname_, 512);
        table_name_.write(tname.ptr(), tname.length());
      }
      return ret;
    }
  }
}

#endif //

