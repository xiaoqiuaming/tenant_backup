/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_alter_table.cpp
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#include "common/ob_privilege.h"
#include "ob_alter_table.h"
#include "common/utility.h"
#include "mergeserver/ob_rs_rpc_proxy.h"
#include "sql/ob_result_set.h"
#include "sql/ob_sql.h"
//add liu jun. [MultiUPS] [part_cache] 20150626:b
#include "mergeserver/ob_ms_partition_manager.h"
//20150626:e
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObAlterTable::ObAlterTable()
{
}

ObAlterTable::~ObAlterTable()
{
}

void ObAlterTable::reset()
{
  local_context_.rs_rpc_proxy_ = NULL;
  alter_schema_.table_id_ = OB_INVALID_ID;
}

void ObAlterTable::reuse()
{
  local_context_.rs_rpc_proxy_ = NULL;
  alter_schema_.table_id_ = OB_INVALID_ID;
}


int ObAlterTable::open()
{
  int ret = OB_SUCCESS;
  if (local_context_.rs_rpc_proxy_ == NULL
    || strlen(alter_schema_.table_name_) <= 0
    || alter_schema_.table_id_ == OB_INVALID_ID
   /*mod by wuna [MultiUps] [sql_api] 20160109:b*/
//    || (alter_schema_.columns_.count() <= 0 //add liuj [Alter_Rename] [JHOBv0.1] 20150104
//	&& alter_schema_.has_table_rename_ == false)) //add liuj [Alter_Rename] [JHOBv0.1] 20150104
    || (alter_schema_.columns_.count() <= 0
        && alter_schema_.has_table_rename_ == false && alter_schema_.is_rule_modify_ == false
        && alter_schema_.is_alter_expire_info_ == false && alter_schema_.is_drop_expire_info_ == false
        && alter_schema_.is_load_type_modify_ == false))
   /*mod 20160109:e*/
  {
    YYSYS_LOG(INFO,
              "local_context_.rs_rpc_proxy_ =%p strlen(alter_schema_.table_name_) =%ld alter_schema_.table_id_=%ld alter_schema_columns_count =%ld has_table_rename_ = %d",
              local_context_.rs_rpc_proxy_,
              strlen(alter_schema_.table_name_),
              alter_schema_.table_id_,
              alter_schema_.columns_.count(),
              alter_schema_.has_table_rename_);
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR, "ObAlterTable not init, ret=%d", ret);
  }
  else if (local_context_.session_info_->get_trans_start() || local_context_.session_info_->get_trans_id().is_valid() )
  {
    YYSYS_LOG(WARN, "alter table is not allowed in transaction, err=%d", ret);
    ret = OB_ERR_TRANS_ALREADY_STARTED;
  }
  else if (OB_SUCCESS != (ret = local_context_.rs_rpc_proxy_->alter_table(alter_schema_)))
  {
    YYSYS_LOG(WARN, "failed to create table, err=%d", ret);
  }
    //add wuna [MultiUps][sql_api] 20151217:b
  // else if(alter_schema_.is_rule_modify_ == true)
  // {
  //   ret = execute_alter_rule_stmt();
  //   if(OB_SUCCESS != ret)
  //   {
  //     YYSYS_LOG(WARN,"execute alter rule stmt failed.ret=%d.",ret);
  //   }
  // }
  //add 20151217      
  
  /* del liumz, [multi_database.priv_management]20150709
   * this block is useless             //uncertainty   chunk中代码这一部分全部注释 ups 中没有全注释，但没有涉 及multiups 功能
  else
  {
    bool need_grant = false;
    for (int64_t i = 0; i < alter_schema_.columns_.count(); i++)
    {
      if (alter_schema_.columns_.at(i).type_ == AlterTableSchema::ADD_COLUMN)
      {
        need_grant = true;
        break;
      }
    }
    if (need_grant)
    {
      // 还没有赋予权限即成功,如果创建成功了，但是赋予权限失败，由DBA介入
      // same strategy of create table
      ObString user_name = my_phy_plan_->get_result_set()->get_session()->get_user_name();
      ObString table_name;
      table_name.assign_ptr(alter_schema_.table_name_, (ObString::obstr_size_t)strlen(alter_schema_.table_name_));
      local_context_.disable_privilege_check_ = true;
      int retry_times = 10; // default retry tiems
      int i = 0;
      for (; i < retry_times; ++i)
      {
        const ObSchemaManagerV2 *schema_mgr = local_context_.merger_schema_mgr_->get_user_schema(0);
        if (NULL == schema_mgr)
        {
          YYSYS_LOG(WARN, "%s 's schema is not available,retry", alter_schema_.table_name_);
          usleep(10 * 1000); // 10ms
        }
        else
        {
          //add wenghaixing [database manage.bug_dolphin_fix]20150706
          const int len = OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH +1;
          char db_table_name[len] = {0};
          int64_t pos = 0;
          databuff_printf(db_table_name, len, pos, "%s.%s",alter_schema_.dbname_, alter_schema_.table_name_);
          if (pos >= len)
          {
            ret = OB_ERR_TABLE_NAME_LENGTH;
            YYSYS_LOG(ERROR, "db.table name too long, db[%s], table[%s]", alter_schema_.dbname_, alter_schema_.table_name_);
            break;
          }
          //add e
          //modify wenghaixing [database manage.bug_dolphin_fix]20150706
          //if (schema_mgr->get_table_schema(alter_schema_.table_name_) != NULL)
          if (schema_mgr->get_table_schema(db_table_name) != NULL)
          //modify e
          {
            // new table 's schema still not available
            YYSYS_LOG(INFO, "get alter table %s 's schema success", alter_schema_.table_name_);
            local_context_.schema_manager_ = schema_mgr;
            break;
          }
          else
          {
            YYSYS_LOG(WARN, "%s 's schema is not available,retry", alter_schema_.table_name_);
            if (local_context_.merger_schema_mgr_->release_schema(schema_mgr) != OB_SUCCESS)
            {
              YYSYS_LOG(WARN, "release schema failed");
            }
          }
        }
      }

      //mod liumz, [database manage.dolphin_bug_fix]20150707:b
      //if (OB_UNLIKELY(i >= retry_times))
      if (OB_SUCCESS != ret || OB_UNLIKELY(i >= retry_times))
      //mod:e
      {
        ret = OB_ERR_GRANT_PRIVILEGES_TO_CREATE_TABLE;
        //报警，让DBA使用admin账户来处理，给新建的表手工加权限
        //mod liumz, [database manage.dolphin_bug_fix]20150707:b
//        YYSYS_LOG(ERROR, "User: %.*s alter table %s success, "
//                         "but grant all privileges, grant option on %s to '%.*s' failed, ret=%d",
//                         user_name.length(), user_name.ptr(),
//                         alter_schema_.table_name_, alter_schema_.table_name_,
//                         user_name.length(), user_name.ptr(), ret);
        YYSYS_LOG(ERROR, "User: %.*s alter table %s success, "
                         "but grant all privileges, grant option on %s.%s to '%.*s' failed, ret=%d",
                         user_name.length(), user_name.ptr(),
                         alter_schema_.table_name_, alter_schema_.dbname_, alter_schema_.table_name_,
                         user_name.length(), user_name.ptr(), ret);
        //mod:e
      }
      else
      {
        int err = OB_SUCCESS;
        char grant_buff[256];
        int64_t pos = 0;
        ObString grant_stmt;
        ObResultSet local_result;
        //mod liumz, [database manage.dolphin_bug_fix]20150707:b
//        databuff_printf(grant_buff, 256, pos, "GRANT ALL PRIVILEGES, GRANT OPTION ON %.*s to '%.*s'",
//                        table_name.length(), table_name.ptr(), user_name.length(), user_name.ptr());
        databuff_printf(grant_buff, 256, pos, "GRANT ALL PRIVILEGES, GRANT OPTION ON %s.%s to '%.*s'",
                        alter_schema_.dbname_, alter_schema_.table_name_, user_name.length(), user_name.ptr());
        //mod:e
        grant_stmt.assign_ptr(grant_buff, (ObString::obstr_size_t)pos);
        if (pos >= 255)
        {
          //overflow
          err = OB_BUF_NOT_ENOUGH;
          YYSYS_LOG(WARN, "privilege buffer overflow, ret=%d", err);
        }
        else if (OB_SUCCESS != (err = local_result.init()))
        {
          YYSYS_LOG(WARN, "init result set failed,ret=%d", err);
        }
        else if (OB_SUCCESS != (err = ObSql::direct_execute(grant_stmt, local_result, local_context_)))
        {
          YYSYS_LOG(WARN, "grant privilege to created table failed, sql=%.*s, ret=%d",
              grant_stmt.length(), grant_stmt.ptr(), err);
        }
        else if (OB_SUCCESS != (err = local_result.open()))
        {
          YYSYS_LOG(WARN, "open result set failed,ret=%d", err);
        }
        else if (OB_SUCCESS != (err = local_result.close()))
        {
          YYSYS_LOG(WARN, "close result set failed,ret=%d", err);
        }
        else
        {
          local_result.reset();
        }
      }
      local_context_.disable_privilege_check_ = false;
      if (local_context_.schema_manager_ != NULL)
      {
        int err = local_context_.merger_schema_mgr_->release_schema(local_context_.schema_manager_);
        if (OB_SUCCESS != err)
        {
          YYSYS_LOG(WARN, "release schema failed,ret=%d", err);
        }
        else
        {
          local_context_.schema_manager_ = NULL;
        }
      }
	  }
  }*/
  return ret;
}

int ObAlterTable::close()
{
  return OB_SUCCESS;
}
//add wuna [MultiUps][sql_api] 20160111:b
int ObAlterTable::execute_alter_rule_stmt()
{
  int ret = OB_SUCCESS;
  int64_t frozen_version = my_phy_plan_->get_curr_frozen_version();
  int64_t pos = 0;
  char insert_func_buff[65535];
  ObString insert_func;
  ObResultSet local_result;
  if(OB_WITHOUT_PARTITION == alter_schema_.partition_type_ ||OB_DIRECT_PARTITION == alter_schema_.partition_type_)
  {
    // if(0 == strlen(alter_schema_.partition_func_name_))
    // {
    //   strcpy(alter_schema_.partition_func_name_,"NULL");
    // }
    // if(0 == strlen(alter_schema_.param_list_))
    // {
    //   strcpy(alter_schema_.param_list_,"NULL");
    // }
    if(0 != strlen(alter_schema_.partition_func_name_) && 0 != strlen(alter_schema_.param_list_))
    {
      databuff_printf(insert_func_buff, 65535, pos, "REPLACE INTO __all_table_rules (table_id, \
                    start_version,table_name,partition_type,prefix_name,rule_name,par_list) VALUES(%ld,%ld,'%s',%d,'%s','%s','%s')",
                    alter_schema_.table_id_,frozen_version+2,alter_schema_.table_name_,alter_schema_.partition_type_,
                    alter_schema_.group_name_prefix_,alter_schema_.partition_func_name_,alter_schema_.param_list_
                    );
    }
    else if(0 == strlen(alter_schema_.partition_func_name_) && 0 == strlen(alter_schema_.param_list_))
    {
      databuff_printf(insert_func_buff, 65535, pos, "REPLACE INTO __all_table_rules (table_id, \
                    start_version,table_name,partition_type,prefix_name,rule_name,par_list) VALUES(%ld,%ld,'%s',%d,'%s', NULL, NULL)",
                    alter_schema_.table_id_,frozen_version+2,alter_schema_.table_name_,alter_schema_.partition_type_,
                    alter_schema_.group_name_prefix_
              );
    }
    else
    {
      ret = OB_ERROR;
      YYSYS_LOG(WARN, "partition_func_name_ and param_list_ should be both NULL or not NULL ,ret=%d", ret);
    }
    

    
  }
  if (pos >= 65535 || pos <= 0)
  {
    // overflow or not init
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "partition rules buffer overflow or not init,ret=%d", ret);
  }
  else
  {
    insert_func.assign_ptr(insert_func_buff, static_cast<ObString::obstr_size_t>(pos));
    int retry_times = 10; // default retry tiems
    int retry_num = 0;
    ret = fill_sql_context(retry_num,retry_times);
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN,"generate sql context failed.");
    }
    else
    {
      if (OB_SUCCESS != (ret = local_result.init()))
      {
        YYSYS_LOG(WARN, "init result set failed,ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = ObSql::direct_execute(insert_func, local_result, local_context_)))
      {
        YYSYS_LOG(WARN, "insert table rules failed, sql=%.*s, ret=%d",
            insert_func.length(), insert_func.ptr(), ret);
      }
      else if (OB_SUCCESS != (ret = local_result.open()))
      {
        YYSYS_LOG(WARN, "open result set failed,ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = local_result.close()))
      {
        YYSYS_LOG(WARN, "close result set failed,ret=%d", ret);
      }
      else
      {
        local_result.reset();
      }
    }
    int err = release_sql_context();
    if(OB_SUCCESS != err)
    {
      YYSYS_LOG(WARN, "release sql context failed, ret=%d", err);
    }
  }
  return ret;
}
int ObAlterTable::fill_sql_context(int& retry_num,int retry_times)
{
  int ret = OB_SUCCESS;
  ObMsPartitionManager *temp = NULL;
  if(NULL == (temp = dynamic_cast<ObMsPartitionManager *>
              (local_context_.part_monitor_->get_partition_manager())))
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "dynamic cast to ObMsPartitionManager failed");
  }
  else
  {
    local_context_.partition_mgr_ = temp;
    for (; retry_num < retry_times; ++retry_num)
    {
      const ObSchemaManagerV2 *schema_mgr = local_context_.merger_schema_mgr_->get_user_schema(0);
      if (NULL == schema_mgr)
      {
        YYSYS_LOG(WARN, "%s 's schema is not available,retry", alter_schema_.table_name_);
        usleep(10 * 1000); // 10ms
      }
      else
      {
        //mod lqc [MultiUps 1.0] [#37] 20170505 b
       // if (NULL != schema_mgr->get_table_schema(alter_schema_.table_name_))
        if (NULL != schema_mgr->get_table_schema(alter_schema_.table_id_))
        {//mod e
          // new table 's schema still not available
          YYSYS_LOG(INFO, "get alter table %s 's schema success", alter_schema_.table_name_);
          local_context_.schema_manager_ = schema_mgr;
          break;
        }
        else
        {
          YYSYS_LOG(WARN, "%s 's schema is not available,retry", alter_schema_.table_name_);
          if ((ret=local_context_.merger_schema_mgr_->release_schema(schema_mgr)) != OB_SUCCESS)
          {
            YYSYS_LOG(WARN, "release schema failed");
          }
        }
      }
    }
  }
  return ret;
}
int ObAlterTable::release_sql_context()
{
  int err = OB_SUCCESS;
  if (NULL != local_context_.schema_manager_ )
  {
    int err1 = local_context_.merger_schema_mgr_->release_schema(local_context_.schema_manager_);
    if (OB_SUCCESS != err1)
    {
      err = err1;
      YYSYS_LOG(WARN, "release schema failed,ret=%d", err1);
    }
    local_context_.schema_manager_ = NULL;
  }
  if(NULL != local_context_.partition_mgr_ )
  {
    int err2 = local_context_.part_monitor_->release_manager(local_context_.partition_mgr_);
    if(OB_SUCCESS != err2)
    {
      err = (OB_SUCCESS == err) ? err2 : err;
      YYSYS_LOG(WARN, "release partition manager failed, ret=%d", err2);
    }
    local_context_.partition_mgr_ = NULL;
  }
  return err;
}
//add 20160111:e
namespace oceanbase
{
  namespace sql
  {
    REGISTER_PHY_OPERATOR(ObAlterTable, PHY_ALTER_TABLE);
  }
}

int64_t ObAlterTable::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "AlterTable(table_name=%s, ", alter_schema_.table_name_);
  databuff_printf(buf, buf_len, pos, "table_id=%lu, ", alter_schema_.table_id_);
  databuff_printf(buf, buf_len, pos, "columns=[");
  for (int64_t i = 0; i < alter_schema_.columns_.count(); ++i)
  {
    const AlterTableSchema::AlterColumnSchema& alt_col = alter_schema_.columns_.at(i);
    databuff_printf(buf, buf_len, pos, "(column_name=%s, ", alt_col.column_.column_name_);
    databuff_printf(buf, buf_len, pos, "column_id_=%lu, ", alt_col.column_.column_id_);
    switch (alt_col.type_)
    {
      case AlterTableSchema::ADD_COLUMN:
        databuff_printf(buf, buf_len, pos, "action_type=ADD_COLUMN, ");
        databuff_printf(buf, buf_len, pos, "column_group_id=%lu, ", alt_col.column_.column_group_id_);
        databuff_printf(buf, buf_len, pos, "rowkey_id=%ld, ", alt_col.column_.rowkey_id_);
        databuff_printf(buf, buf_len, pos, "join_table_id=%lu, ", alt_col.column_.join_table_id_);
        databuff_printf(buf, buf_len, pos, "join_column_id=%lu, ", alt_col.column_.join_column_id_);
        databuff_printf(buf, buf_len, pos, "data_type=%d, ", alt_col.column_.data_type_);
        databuff_printf(buf, buf_len, pos, "data_length_=%ld, ", alt_col.column_.data_length_);
        databuff_printf(buf, buf_len, pos, "data_precision=%ld, ", alt_col.column_.data_precision_);
        databuff_printf(buf, buf_len, pos, "nullable=%s, ", alt_col.column_.nullable_ ? "TRUE" : "FALSE");
        databuff_printf(buf, buf_len, pos, "length_in_rowkey=%ld, ", alt_col.column_.length_in_rowkey_);
        databuff_printf(buf, buf_len, pos, "gm_create=%ld, ", alt_col.column_.gm_create_);
        databuff_printf(buf, buf_len, pos, "gm_modify=%ld)", alt_col.column_.gm_modify_);
        break;
      case AlterTableSchema::DEL_COLUMN:
        databuff_printf(buf, buf_len, pos, "action_type=DEL_COLUMN, ");
        break;
      case AlterTableSchema::MOD_VARCHAR_LENGTH:
        databuff_printf(buf, buf_len, pos, "action_type=MOD_VARCHAR_LENGTH, ");
        break;
      case AlterTableSchema::MOD_DECIMAL_PRECISION:
        databuff_printf(buf, buf_len, pos, "action_type=MOD_DECIMAL_PRECISION, ");
        break;
      case AlterTableSchema::MOD_COLUMN:
	  //add fyd [NotNULL_check] [JHOBv0.1] 20140108:b
      case AlterTableSchema::MOD_COLUMN_NULL:
	  //add 20140108:e
        databuff_printf(buf, buf_len, pos, "action_type=MOD_COLUMN, ");
        databuff_printf(buf, buf_len, pos, "nullable=%s, ", alt_col.column_.nullable_ ? "TRUE" : "FALSE");
        break;
      default:
        break;
    }
    if (i != alter_schema_.columns_.count())
      databuff_printf(buf, buf_len, pos, ", ");
  } // end for
  databuff_printf(buf, buf_len, pos, "])\n");
  return pos;
}
