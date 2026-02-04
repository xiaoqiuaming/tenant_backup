#include "ob_part_func_executor.h"
#include "ob_sql.h"

using namespace oceanbase;
using namespace oceanbase::sql;

ObPartFuncExecutor::ObPartFuncExecutor():context_(NULL), result_set_out_(NULL)
{
}

ObPartFuncExecutor::~ObPartFuncExecutor()
{
    if(NULL != stmt_)
    {
        stmt_->~ObBasicStmt();
    }
}

int ObPartFuncExecutor::open()
{
  int ret = OB_SUCCESS;
  switch(stmt_->get_stmt_type())
  {
    case ObBasicStmt::T_CREATE_PART_FUNC:
      ret = do_create_part_func(stmt_);
      break;
    case ObBasicStmt::T_DROP_PART_FUNC:
      ret = do_drop_part_func(stmt_);
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      YYSYS_LOG(ERROR, "not a privilege-related sql, ret=%d", ret);
      break;
  };
  return ret;
}

int ObPartFuncExecutor::close()
{
  int ret = OB_SUCCESS;
  return ret;
}

void ObPartFuncExecutor::reset()
{
}

void ObPartFuncExecutor::reuse()
{
}

int64_t ObPartFuncExecutor::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  UNUSED(buf);
  UNUSED(buf_len);
  return pos;
}

int ObPartFuncExecutor::do_create_part_func(const ObBasicStmt *stmt)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const ObCreatePartFuncStmt *create_part_func_stmt = dynamic_cast<const ObCreatePartFuncStmt*>(stmt);
  char param_list_char[OB_MAX_VARCHAR_LENGTH];
  const ObArray<ObString>& all_params = create_part_func_stmt->get_all_parameters();
  int32_t params_num = static_cast<int32_t>(all_params.count());
  const int32_t partition_type = create_part_func_stmt->get_partition_type();//add wuna [MultiUps][sql_api]20151217
  int32_t list_pos = 0;

  if(OB_SUCCESS == ret)
  {
    for(int32_t i = 0;i < params_num - 1;i++)
    {
      const ObString& param = all_params.at(i);
      int32_t length = param.length();
      if(list_pos + length + 1< OB_MAX_VARCHAR_LENGTH)
      {
        memcpy(param_list_char + list_pos,param.ptr(), length);
        param_list_char[list_pos + length] = ',';
        list_pos = list_pos + length + 1;
      }
      else
      {
        ret = OB_ERR_PARAMETER_LIST;
        YYSYS_LOG(WARN,"Partition parameter list is too long");
        break;
      }
    }
    if(OB_SUCCESS == ret)
    {
      int32_t length = all_params.at(params_num - 1).length();
      if(list_pos + length + 1< OB_MAX_VARCHAR_LENGTH)
      {
        memcpy(param_list_char + list_pos,all_params.at(params_num - 1).ptr(), length);
        param_list_char[list_pos + length] = '\0';
      }
      else
      {
        ret = OB_ERR_PARAMETER_LIST;
        YYSYS_LOG(WARN,"Partition parameter list is too long");
      }
    }
  }
  if (OB_SUCCESS != ret)
  {
  }
  else if (OB_UNLIKELY(NULL == create_part_func_stmt))
  {
    ret = OB_ERR_UNEXPECTED;
    YYSYS_LOG(ERROR, "dynamic cast create partition function stmt failed, ret=%d", ret);
  }
  //check the expression rules
  else if (OB_SUCCESS != (ret = const_cast<ObCreatePartFuncStmt*>(create_part_func_stmt)->check_expression()))
  {
    YYSYS_LOG(WARN,"check partition function failed,ret=%d",ret);
  }
  //insert into __all_partition_rules values()
  else if (OB_SUCCESS == ret)
  {
    const ObString func_name = create_part_func_stmt->get_function_name();
    int32_t param_num = create_part_func_stmt->get_parameters_num();
//    ObString param_list;//del wuna [MultiUps][sql_api] 20151217
    const ObString func_content = create_part_func_stmt->get_func_context();

    ObString processed_func_content;
    char content_buf[func_content.length()*2];
    if(PRERANGE == partition_type)
    {
        int j=0;
        const char* ptr = func_content.ptr();
        for(int i=0;i<func_content.length();i++)
        {
            if(ptr[i] == '\'')
            {
                content_buf[j++] = '\\';
                content_buf[j++] = ptr[i];
            }
            else if(ptr[i] == '\\')
            {
                content_buf[j++] = '\\';
                content_buf[j++] = ptr[i];
            }
            else
            {
                content_buf[j++] = ptr[i];
            }
        }
        processed_func_content.assign_ptr(content_buf,j);
    }
    else
    {
        processed_func_content = create_part_func_stmt->get_func_context();
    }

      pos = 0;
      char insert_func_buff[65535];
      // add lqc [create rule out trans]
      ObEndTransReq req;
      context_->session_info_->get_trans_info(req);
      context_->session_info_->reset_trans_info();
      bool is_autocommit = context_->session_info_->get_autocommit();
      context_->session_info_->set_autocommit(true);
      // add e
    ObString insert_func;
    insert_func.assign_buffer(insert_func_buff, 65535);
    //mod by wuna [MultiUps][sql_api]20151217:b
//    databuff_printf(insert_func_buff, 65535, pos, "INSERT INTO __all_partition_rules (rule_name,
//                    rule_par_num,rule_par_list,rule_body) VALUES('%.*s',%d, '%s', '%.*s')",
//                    func_name.length(),func_name.ptr(),param_num,param_list_char,func_content.length(),func_content.ptr());
    databuff_printf(insert_func_buff, 65535, pos, "INSERT INTO __all_partition_rules (rule_name, \
                   rule_par_num,rule_par_list,rule_body,type) VALUES('%.*s',%d, '%s', '%.*s',%d)",
                   func_name.length(),func_name.ptr(),param_num,param_list_char,
                   processed_func_content.length(),processed_func_content.ptr(),partition_type);
    //mod 20151217:e
    if (pos >= 65535)
    {
      // overflow
      ret = OB_BUF_NOT_ENOUGH;
      YYSYS_LOG(WARN, "partition rules buffer overflow,ret=%d", ret);
    }
    else
    {
      insert_func.assign_ptr(insert_func_buff, static_cast<ObString::obstr_size_t>(pos));
      ret = execute_stmt_no_return_rows(insert_func);
      if (OB_SUCCESS != ret)
      {
        ret = OB_ERR_FUNCTION_EXIST;
        YYSYS_LOG(USER_ERROR, " partition function '%.*s' already exists", func_name.length(), func_name.ptr());
      }
    }
      // add lqc [create rule out trans]
      context_->session_info_->set_trans_info(req);
      context_->session_info_->set_autocommit(is_autocommit);
      //add e
  }
  return ret;
}

int ObPartFuncExecutor::do_drop_part_func(const ObBasicStmt *stmt)
{
  int ret = OB_SUCCESS;
  bool is_start_trans = true;
  const ObDropPartFuncStmt* drp_func_stmt = dynamic_cast<const ObDropPartFuncStmt*>(stmt);
  if(OB_UNLIKELY(NULL == drp_func_stmt))
  {
    ret = OB_ERR_UNEXPECTED;
    YYSYS_LOG(ERROR,"dynamic cast stmt to drop partition function stmt,ret:%d", ret);
  }
  else
  {
    if(OB_ERR_TRANS_ALREADY_STARTED == (ret = start_transaction()))
    {
      ret = OB_SUCCESS;
      is_start_trans = false;
      YYSYS_LOG(WARN, "transaction alreay started,ret:%d", ret);
    }
    else if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "start transaction failed,ret:%d", ret);
    }

    if(OB_SUCCESS == ret)
    {
      int64_t i = 0;
      const ObStrings* funcs = drp_func_stmt->get_funcs();
      ObString func_name;
      int64_t count = funcs->count();
      for(i = 0;OB_SUCCESS == ret && i < count;i++)
      {
        int64_t pos = 0;
        char buf[512];
        ObString delete_func;
        bool is_using=false;//add wuna [MultiUps][sql_api] 20160301
        if(OB_SUCCESS != (ret = funcs->get_string(i,func_name)))
        {
          YYSYS_LOG(ERROR, "get function name from drop part stmt failed, ret=%d", ret);
          break;
        }
        //add wuna [MultiUps][sql_api] 20160301:b
        else if(OB_SUCCESS != (ret = check_if_part_func_using(func_name,is_using)))
        {
          YYSYS_LOG(WARN,"check if part func using failed,ret=%d",ret);
          break;
        }
        else if(true == is_using)
        {
          ret=OB_ERROR;
          YYSYS_LOG(USER_ERROR,"Partition Funtion %.*s is Using,Cann't Drop Now.",
                    func_name.length(),func_name.ptr());
          break;
        }
        else if((func_name.compare("boc_account_mixs") ==0) || func_name.compare("boc_card_mixs") == 0)
        {
            ret = OB_ERROR;
            YYSYS_LOG(USER_ERROR,"Partition Funtion %.*s is can't be droped.",
                      func_name.length(),func_name.ptr());
            break;
        }
        //add 20160301:e
        else
        {
          databuff_printf(buf, 512, pos, "delete from __all_partition_rules where rule_name = '%.*s'", func_name.length(), func_name.ptr());
          if (pos >= 511)
          {
            // overflow
            ret = OB_BUF_NOT_ENOUGH;
            YYSYS_LOG(WARN, "privilege buffer overflow,ret=%d", ret);
            break;
          }
          else
          {
            delete_func.assign_ptr(buf, static_cast<ObString::obstr_size_t>(pos));
            if(OB_SUCCESS != (ret = execute_delete_func(delete_func,func_name)))
            {
              YYSYS_LOG(ERROR,"execute delete function failed,ret=%d",ret);
              break;
            }
            //add wuna [MultiUps][sql_api] 20160704:b
            else if(NULL == context_->partition_mgr_)
            {
              ret = OB_ERROR;
              YYSYS_LOG(ERROR,"partition_mgr_ is null,ret=%d",ret);
              break;
            }
            else if(OB_SUCCESS != (ret = context_->partition_mgr_->erase_invalid_part_rule_map(func_name)))
            {
              YYSYS_LOG(ERROR,"erase_invalid_part_rule_map failed,ret=%d",ret);
              break;
            }
            //add 20160704:e
          }
        }
      }

      if(is_start_trans)
      {
        if (OB_SUCCESS == ret)
        {
          ret = commit();
        }
        else
        {
          // 如果rollback也失败，依然设置之前失败的物理执行计划到对外的结果集中,rollback 失败，ups会清除
          // rollback 失败，不会覆盖以前的返回值ret,也不覆盖以前的message
          int err = rollback();
          if (OB_SUCCESS != err)
          {
            YYSYS_LOG(WARN, "rollback failed,ret=%d", err);
          }
        }
      }
    }
  }
  context_->session_info_->set_current_result_set(result_set_out_);
  context_->session_info_->get_current_result_set()->set_errcode(ret);
  return ret;
}
//add wuna [MultiUps][sql_api] 20160301:b
int ObPartFuncExecutor::check_if_part_func_using(ObString& func_name,bool& is_using)
{
  int ret=OB_SUCCESS;
  char sql_char[1024];
  ObString select_tables;
  int64_t pos = 0;
  ObSQLResultSet result_set;
  ObRow row;
  databuff_printf(sql_char, 1024, pos, "select /*+read_consistency(STRONG) */ table_name from __all_table_rules where rule_name= '%.*s'",
                  func_name.length(),func_name.ptr());
  if(pos >= 1024)
  {
    ret = OB_BUF_NOT_ENOUGH;
    YYSYS_LOG(WARN, "buffer not enough, ret=%d", ret);
  }
  else
  {
    select_tables.assign_ptr(sql_char, static_cast<ObString::obstr_size_t>(pos));
    YYSYS_LOG(INFO, "select_tables=%.*s", select_tables.length(), select_tables.ptr());
  }
  if(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = context_->partition_mgr_->execute_sql(select_tables, result_set)))
    {
      YYSYS_LOG(WARN, "failed to execute sql, ret=%d", ret);
    }
    else
    {
      ret = get_next_row(result_set, row);
      if(OB_ITER_END == ret)
      {
        is_using=false;
        ret = OB_SUCCESS;
      }
      else if(OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "get next row from ObResultSet failed,ret=%d", ret);
      }
      else
      {
        is_using=true;
      }
    }
  }
  return ret;
}
//add 20160301:e
/*int ObPartFuncExecutor::get_param_list(char* param_list) const
{
  int ret = OB_SUCCESS;
  ObCreatePartFuncStmt* create_stmt = dynamic_cast<ObCreatePartFuncStmt *>(stmt_);
  const ObArray<ObString> all_params = create_stmt->get_all_parameters();
  int num = static_cast<int>(all_params.count());
  int location = 0;
  for(int i = 0;i < num - 1;i++)
  {
    const ObString one_param = all_params.at(i);
    for()
    {

    }
  }
  return ret;
}*/

void ObPartFuncExecutor::set_context(ObSqlContext *sql_context)
{
  context_ = sql_context;
  result_set_out_ = context_->session_info_->get_current_result_set();
}

int ObPartFuncExecutor::execute_stmt_no_return_rows(const ObString &stmt)
{
  int ret = OB_SUCCESS;
  ObResultSet tmp_result;
  context_->session_info_->set_current_result_set(&tmp_result);
  if (OB_SUCCESS != (ret = tmp_result.init()))
  {
    YYSYS_LOG(WARN, "init result set failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = ObSql::direct_execute(stmt, tmp_result, *context_)))
  {
    result_set_out_->set_message(tmp_result.get_message());
    YYSYS_LOG(WARN, "direct_execute failed, sql=%.*s ret=%d", stmt.length(), stmt.ptr(), ret);
  }
  else if (OB_SUCCESS != (ret = tmp_result.open()))
  {
    YYSYS_LOG(WARN, "open result set failed, sql=%.*s ret=%d", stmt.length(), stmt.ptr(), ret);
  }
  else
  {
    OB_ASSERT(tmp_result.is_with_rows() == false);
    int err = tmp_result.close();
    if (OB_SUCCESS != err)
    {
      YYSYS_LOG(WARN, "failed to close result set,err=%d", err);
    }
    tmp_result.reset();
  }
  context_->session_info_->set_current_result_set(result_set_out_);
  return ret;
}

int ObPartFuncExecutor::execute_delete_func(const ObString &delete_func,ObString &func_name)
{
  int ret = OB_SUCCESS;
  ObResultSet tmp_result;
  context_->session_info_->set_current_result_set(&tmp_result);
  if (OB_SUCCESS != (ret = tmp_result.init()))
  {
    YYSYS_LOG(WARN, "init result set failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = ObSql::direct_execute(delete_func, tmp_result, *context_)))
  {
    result_set_out_->set_message(tmp_result.get_message());
    YYSYS_LOG(WARN, "direct_execute failed, sql=%.*s ret=%d", delete_func.length(), delete_func.ptr(), ret);
  }
  else if (OB_SUCCESS != (ret = tmp_result.open()))
  {
    YYSYS_LOG(WARN, "open result set failed, sql=%.*s ret=%d", delete_func.length(), delete_func.ptr(), ret);
  }
  else
  {
    OB_ASSERT(tmp_result.is_with_rows() == false);
    int64_t affected_rows = tmp_result.get_affected_rows();
    if (affected_rows == 0)
    {
      YYSYS_LOG(WARN, "delete function not exist, sql=%.*s", delete_func.length(), delete_func.ptr());
      ret = OB_ERR_FUNCTION_NOT_EXISTS;
      YYSYS_LOG(USER_ERROR, "partition function '%.*s' doesn't exist", func_name.length(), func_name.ptr());
    }
    int err = tmp_result.close();
    if (OB_SUCCESS != err)
    {
      YYSYS_LOG(WARN, "failed to close result set,err=%d", err);
    }
    tmp_result.reset();
  }
  context_->session_info_->set_current_result_set(result_set_out_);
  return ret;
}

int ObPartFuncExecutor::start_transaction()
{
  int ret = OB_SUCCESS;
  ObString start_trans = ObString::make_string("start transaction");
  ret = execute_stmt_no_return_rows(start_trans);
  return ret;
}

int ObPartFuncExecutor::commit()
{
  int ret = OB_SUCCESS;
  ObString commit = ObString::make_string("commit");
  ret = execute_stmt_no_return_rows(commit);
  return ret;
}

int ObPartFuncExecutor::rollback()
{
  int ret = OB_SUCCESS;
  ObString roolback = ObString::make_string("rollback");
  ret = execute_stmt_no_return_rows(roolback);
  return ret;
}
