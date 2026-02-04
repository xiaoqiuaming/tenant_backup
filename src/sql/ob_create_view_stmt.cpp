//add lvjc [create view]
#include "ob_create_view_stmt.h"
#include "ob_schema_checker.h"
#include "ob_select_stmt.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

ObCreateViewStmt::ObCreateViewStmt(ObStringBuf* name_pool)
  : ObBasicStmt(ObBasicStmt::T_CREATE_VIEW)
{
  name_pool_ = name_pool;
  or_replace_ = false;
  with_check_option_ = OB_WITH_NONE_CHECK_OPTION;
  memset(text_, 0, OB_MAX_SQL_LENGTH);
}

ObCreateViewStmt::~ObCreateViewStmt()
{
}

int ObCreateViewStmt::set_view_name(ResultPlan &result_plan, const ObString& view_name, const bool is_alter)
{
  int& ret = result_plan.err_stat_.err_code_ = OB_SUCCESS;
  char buff[OB_MAX_COMPLETE_TABLE_NAME_LENGTH] = {0};
  ObString db_view_name;
  const ObTableSchema *table_schema = NULL;
  ObSchemaChecker* schema_checker = NULL;
  if (NULL == (schema_checker = static_cast<ObSchemaChecker*>(result_plan.schema_checker_)))
  {
    ret = OB_ERR_SCHEMA_UNSET;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
             "schema(s) are not set");
  }
  else
  {
    db_view_name.assign_buffer(buff, static_cast<ObString::obstr_size_t>(OB_MAX_COMPLETE_TABLE_NAME_LENGTH));
    db_view_name.concat(dbname_, view_name);
    table_schema = schema_checker->get_table_schema(db_view_name);
  }

  if (is_alter && table_schema == NULL)
  {
    ret = OB_ENTRY_NOT_EXIST;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
             "VIEW '%.*s' does not exist", db_view_name.length(), db_view_name.ptr());
  }
  else if (!or_replace_ && table_schema != NULL)
  {
    ret = OB_ERR_ALREADY_EXISTS;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
             "table '%.*s' already exists", db_view_name.length(), db_view_name.ptr());
  }
  else if (or_replace_ && table_schema != NULL)
  {
    //如果表已经存在，需要判断已经存在的表类型是否为view,如果为BASE_TABLE报错
    if (table_schema->get_type() != ObTableSchema::VIEW)
    {
      ret = OB_ERR_ALREADY_EXISTS;
      snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
               "table '%.*s' already exists", db_view_name.length(), db_view_name.ptr());
    }
  }
  if (ret == OB_SUCCESS
          && OB_SUCCESS != (ret = ob_write_string(*name_pool_, view_name, view_name_)))
  {
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
             "can not malloc space for view name '%.*s'", view_name.length(), view_name.ptr());
  }
  return ret;
}

int ObCreateViewStmt::set_db_name(ResultPlan &result_plan, const ObString& dbname)
{
  int& ret = result_plan.err_stat_.err_code_ = OB_SUCCESS;//物理计划解析时还需对db_name进行检测
  if (OB_SUCCESS != (ret = ob_write_string(*name_pool_, dbname, dbname_)))
  {
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
             "can not malloc space for database name '%.*s'", dbname.length(), dbname.ptr());
  }
  return ret;
}

int ObCreateViewStmt::add_column_name(ResultPlan &result_plan, const ObString &column_name)
{
  int& ret = result_plan.err_stat_.err_code_ = OB_SUCCESS;
  for (int64_t i = 0; i<column_names_.count() && ret==OB_SUCCESS; i++)
  {
    if (column_names_.at(i).compare(column_name) == 0)
    {
      ret = OB_ERR_COLUMN_DUPLICATE;
      snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
               "duplicate column name '%.*s'", column_name.length(), column_name.ptr());
      break;
    }
  }
  if (ret == OB_SUCCESS)
  {
    ObString col;
    if (OB_SUCCESS != (ret = ob_write_string(*name_pool_, column_name, col)))
    {
      snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
                "can not malloc space for column name '%.*s'", column_name.length(), column_name.ptr());
    }
    else if (OB_SUCCESS != (ret = column_names_.push_back(col)))
    {
      snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
                "add column name failed");
    }
  }
  return ret;
}

void ObCreateViewStmt::set_sql_text(const char *text)
{
  if (text != NULL && *text != '\0')
  {
    int64_t pos = 0;
    //select_stmt
    databuff_printf(text_, OB_MAX_SQL_LENGTH, pos, "%s", text);
    if (pos < OB_MAX_SQL_LENGTH)
    {
      text_[pos] = '\0';
    }
    YYSYS_LOG(INFO, "view_definition = [%s]", text_);
  }
}

void ObCreateViewStmt::print(FILE *fp, int32_t level, int32_t index)
{
  print_indentation(fp, level);
  fprintf(fp, "ObCreateViewStmt %d Begin\n", index);
  if (or_replace_)
  {
    print_indentation(fp, level + 1);
    fprintf(fp, "or_replace_ = TRUE\n");
  }
  print_indentation(fp, level + 1);
  fprintf(fp, "database name ::= %.*s\n", dbname_.length(), dbname_.ptr());
  print_indentation(fp, level + 1);
  fprintf(fp, "view name ::= %.*s\n", view_name_.length(), view_name_.ptr());
  fprintf(fp, "COLUMN DEFINITION(s) ::=\n");
  for (int32_t i = 0; i < column_names_.count(); i++)
  {
    ObString &col = column_names_.at(i);
    print_indentation(fp, level + 2);
    fprintf(fp, "Column(%d) ::= %.*s\n", i + 1, col.length(), col.ptr());
  }
  fprintf(fp, "WITH CHECK OPTION ::=\n");
  print_indentation(fp, level + 1);
  switch (with_check_option_)
  {
    case (OB_WITH_NONE_CHECK_OPTION):
        fprintf(fp, "WITH NONE CHECK OPTION\n");
        break;
    case (OB_WITH_LOCAL_CHECK_OPTION):
        fprintf(fp, "WITH LOCAL CHECK OPTION\n");
        break;
    case (OB_WITH_CASCADED_CHECK_OPTION):
        fprintf(fp, "WITH CASCADED CHECK OPTION\n");
        break;
    default:
        break;//can not be here
  }
  print_indentation(fp, level);
  fprintf(fp, "ObCreateViewStmt %d End\n", index);
}
//add lvjc [creata view] e
