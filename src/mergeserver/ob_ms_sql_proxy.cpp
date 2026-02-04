/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <2013-04-17 15:02:26 fufeng.syd>
 * Version: $Id$
 * Filename: ob_ms_sql_proxy.cpp
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@alipay.com>
 *
 */


#include "yysys.h"
#include "ob_ms_sql_proxy.h"
#include "sql/ob_sql.h"
#include "sql/ob_sql_context.h"
#include "mergeserver/ob_merge_server_service.h"
#include "common/ob_schema_manager.h"
//add liu jun. [MultiUPS] [part_cache] 20150629:b
#include "ob_ms_partition_manager.h"
//20150629:e

#include "ob_merge_server_main.h"//add liuzy [MultiUPS] [add_paxos_interface] 20160323:b

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::common::serialization;
using namespace oceanbase::mergeserver;

ObMsSQLProxy::ObMsSQLProxy()
  :ms_service_(NULL),
  rpc_proxy_(NULL),
  root_rpc_(NULL),
  async_rpc_(NULL),
  schema_mgr_(NULL),
  part_monitor_(NULL),//add liu jun. [MultiUPS] [part_cache] 20150608
  cache_proxy_(NULL)
{
}

int ObMsSQLProxy::execute(const ObString &sqlstr, ObSQLResultSet &rs,
                          sql::ObSqlContext &context, int64_t schema_version)
{
  int ret = OB_SUCCESS;
  sql::ObResultSet &result = rs.get_result_set();
  UNUSED(schema_version);
  if (OB_SUCCESS !=
      (ret = ObSql::direct_execute(sqlstr, result, context)))
  {
    YYSYS_LOG(WARN, "fail to execute ret=%d query [%.*s]",
        ret, sqlstr.length(), sqlstr.ptr());
  }
  if (NULL != context.schema_manager_)
  {
    schema_mgr_->release_schema(context.schema_manager_);
    context.schema_manager_ = NULL;
  }

  rs.set_sqlstr(sqlstr);
  rs.set_errno(ret);
  return ret;
}

//mod liu jun. [MultiUPS] [part_cache] 20150608:b
void ObMsSQLProxy::set_env(mergeserver::ObMergerRpcProxy  *rpc_proxy,
                           mergeserver::ObMergerRootRpcProxy *root_rpc,
                           mergeserver::ObMergerAsyncRpcStub   *async_rpc,
                           common::ObMergerSchemaManager *schema_mgr,
                           common::ObPartitionMonitor *part_monitor,//add liu jun.
                           common::ObTabletLocationCacheProxy *cache_proxy,
                           const mergeserver::ObMergeServerService *ms_service)
{
  rpc_proxy_ = rpc_proxy;
  root_rpc_ = root_rpc;
  async_rpc_ = async_rpc;
  schema_mgr_ = schema_mgr;
  part_monitor_ = part_monitor;
  cache_proxy_ = cache_proxy;
  ms_service_ = ms_service;
}
//mod:e

/// @brief fix this function, thereby executing stmt of alter system config in inner sql form
/// @author liuzy
int ObMsSQLProxy::init_sql_env(ObSqlContext &context, int64_t &schema_version,
                               ObSQLResultSet &rs, ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  ObString name = ObString::make_string(OB_READ_CONSISTENCY);
  ObObj type;
  type.set_type(ObIntType);
  ObObj value;
  value.set_int(WEAK);
  ObResultSet &result = rs.get_result_set();
  schema_version = schema_mgr_->get_latest_version();
  context.schema_manager_ = schema_mgr_->get_user_schema(schema_version); // reference count
  //add liuzy [MultiUPS] [add_paxos_interface] 20160323:b
  //const obmysql::ObMySQLServer &sql_server = ObMergeServerMain::get_instance()->get_mysql_server();
  //obmysql::ObMySQLServer  *sql_server_p = const_cast<obmysql::ObMySQLServer*>(&sql_server);
  //add 20160323:e
  //add liu jun. [MultiUPS] [part_cache] 20150626:b
  if(NULL == (context.partition_mgr_ = dynamic_cast<ObMsPartitionManager *>(part_monitor_->get_partition_manager())))
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "get partition manager failed");
  }
  else if (OB_SUCCESS != (ret = result.init()))
  //20150626:e
  {
    YYSYS_LOG(ERROR, "init result set error, ret = [%d]", ret);
  }
  else if (NULL == context.schema_manager_)
  {
    YYSYS_LOG(INFO, "table schema not ready, schema_version=%ld", schema_version);
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != (ret = session.init(block_allocator_)))
  {
    YYSYS_LOG(WARN, "failed to init context, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = session.load_system_variable(name, type, value)))
  {
    YYSYS_LOG(ERROR, "load system variable %.*s failed, ret=%d", name.length(), name.ptr(), ret);
  }
  else if (NULL == context.schema_manager_)
  {
    YYSYS_LOG(WARN, "fail to get user schema. schema_version=%ld", schema_version);
    ret = OB_ERROR;
  }
  else
  {
    session.set_version_provider(ms_service_);
    session.set_config_provider(&ms_service_->get_config());
    context.session_info_ = &session;
    //del liuzy [MultiUPS] [add_paxos_interface] 20160323:b
//    context.session_info_->set_current_result_set(&result);
    //del 20160323:e

    //add liuzy [MultiUPS] [add_paxos_interface] 20160321:b
    context.merger_schema_mgr_ = schema_mgr_;
    context.part_monitor_ = part_monitor_;
    //add 20160321:e
    context.cache_proxy_ = cache_proxy_;    // thread safe singleton
    context.async_rpc_ = async_rpc_;        // thread safe singleton
    context.merger_rpc_proxy_ = rpc_proxy_; // thread safe singleton
    context.rs_rpc_proxy_ = root_rpc_;      // thread safe singleton
    context.merge_service_ = ms_service_;
    context.disable_privilege_check_ = true;
    // reuse memory pool for parser
    context.session_info_->get_parser_mem_pool().reuse();
    //del liuzy [MultiUPS] [add_paxos_interface] 20160323:b
//    context.session_info_->get_transformer_mem_pool().start_batch_alloc();
    //del 20160323:e
  }
  //add liuzy [MultiUPS] [add_paxos_interface] 20160323:b
  /* del hongchen no need 20170810
  if (OB_SUCCESS == ret && OB_SUCCESS != (ret = sql_server_p->load_system_params(session)))
  {
    YYSYS_LOG(ERROR, "load all system variable failed, ret=%d", ret);
  }
  else
  */
  {
   session.set_version_provider(ms_service_);
   session.set_config_provider(&ms_service_->get_config());
   context.session_info_ = &session;
   context.session_info_->set_current_result_set(&result);  
   context.session_info_->get_transformer_mem_pool().start_batch_alloc();
  }
  //add 20160323:e
  //add liu jun. [MultiUPS] [part_cache] 20150814:b
  if(OB_SUCCESS != ret && NULL != context.partition_mgr_)
  {
    int err = OB_SUCCESS;
    if(OB_SUCCESS != (err = part_monitor_->release_manager(context.partition_mgr_)))
    {
      YYSYS_LOG(ERROR, "can not find partition manager");
    }
  }
  //add:20150814:e
  return ret;
}

int ObMsSQLProxy::cleanup_sql_env(ObSqlContext &context, ObSQLResultSet &rs)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = rs.close()))
  {
    YYSYS_LOG(WARN, "close result set error, ret: [%d]", ret);
  }
  if (OB_SUCCESS != (ret = rs.reset()))
  {
    YYSYS_LOG(WARN, "reuse result set error, ret: [%d]", ret);
  }
  OB_ASSERT(context.session_info_);
  context.session_info_->get_parser_mem_pool().reuse();
  context.session_info_->get_transformer_mem_pool().end_batch_alloc(true);
  //add liu jun. [MultiUPS] [part_cache] 20150626:b
  if(NULL != context.partition_mgr_)
  {
    int err = OB_SUCCESS;
    if(OB_SUCCESS != (err = part_monitor_->release_manager(context.partition_mgr_)))
    {
      YYSYS_LOG(WARN, "can not find the partition manger");
    }
  }
  //20150626:e
  return ret;
}
