
#include "ob_gather_statistics_stmt.h"
#include "ob_schema_checker.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

ObGatherStatisticsStmt::ObGatherStatisticsStmt(common::ObStringBuf* name_pool)
  : ObBasicStmt(ObBasicStmt::T_GATHER_STATISTICS)
{
  name_pool_ = name_pool;
}

ObGatherStatisticsStmt::~ObGatherStatisticsStmt()
{
}

int ObGatherStatisticsStmt::add_statistics_columns(ResultPlan& result_plan, const common::ObString tname, const common::ObString column_name)
{
  int ret = OB_SUCCESS;
  ObSchemaChecker* schema_checker = NULL;
  schema_checker = static_cast<ObSchemaChecker*>(result_plan.schema_checker_);
  if (schema_checker == NULL)
  {
    ret = common::OB_ERR_SCHEMA_UNSET;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "Schema(s) are not set");
  }

  if (OB_SUCCESS == ret)
  {
    uint64_t col_id = schema_checker->get_column_id(tname,column_name);
    if (OB_INVALID_ID == col_id)
    {
      ret = OB_ERR_COLUMN_NOT_FOUND;
      snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
                "Statistics Columns %.*s are not found", column_name.length(), column_name.ptr());
    }
    if (OB_SUCCESS == ret)
    {
      for (int64_t j = 0; j < statistics_column_ids_.count(); j++)
      {
        if (statistics_column_ids_.at(j) == col_id)
        {
          ret = OB_ERR_COLUMN_DUPLICATE;
          snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
                              "No unique colums_: '%.*s'", column_name.length(), column_name.ptr());
          break;
        }
      }
    }
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(ERROR, "gather statistics stmt error ret=[%d]", ret);
    }
    else
    {
      statistics_column_ids_.push_back(col_id);
    }
  }
  return ret;
}


ObRowkeyInfo &ObGatherStatisticsStmt::get_row_key_info()
{
  return row_key_info_;
}

void ObGatherStatisticsStmt::print(FILE* fp, int32_t level, int32_t index)
{
  UNUSED(fp);
  UNUSED(level);
  UNUSED(index);
}

