/**
 * (C) 2010-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * ob_rpc_proxy.h for rpc among chunk server, update server and
 * root server.
 *
 * Authors:
 *   xielun <xielun.szd@taobao.com>
 *   huating <huating.zmq@taobao.com>
 *
 */
#include "ob_rpc_proxy.h"
#include "common/ob_rpc_stub.h"

#include "common/utility.h"
#include "common/ob_schema.h"
#include "common/ob_scanner.h"
#include "common/ob_read_common_data.h"
#include "common/ob_trace_log.h"
#include "common/ob_crc64.h"
#include "common/ob_schema_manager.h"
#include "common/ob_statistics.h"
#include "common/ob_common_stat.h"
#include "ob_chunk_server_stat.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase::common;

    //mod shili [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20150630:b
    //    ObMergerRpcProxy::ObMergerRpcProxy():
    //      ups_list_lock_(yysys::WRITE_PRIORITY),
    //      mm_ups_list_lock_(yysys::WRITE_PRIORITY),
    //      master_master_ups_()
    ObMergerRpcProxy::ObMergerRpcProxy():
      ups_list_lock_(yysys::WRITE_PRIORITY),
      mm_ups_list_lock_(yysys::WRITE_PRIORITY),
      master_master_ups_(),
      master_ups_list_lock_(yysys::WRITE_PRIORITY)
      //mod 20150630:e
    {
      init_ = false;
      rpc_stub_ = NULL;
      rpc_retry_times_ = 0;
      rpc_timeout_ = 0;
      min_fetch_interval_ = 10 * 1000 * 1000L;
      fetch_ups_timestamp_ = 0;
      cur_finger_print_ = 0;
      //add dyr [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20150825:b
      cur_master_ups_list_finger_print_ = 0;
      //add dyr 20150825:e
      fail_count_threshold_ = 100;
      black_list_timeout_ = 60 * 1000 * 1000L;
      fetch_schema_timestamp_ = 0;
      schema_manager_ = NULL;

      fetch_mm_ups_timestamp_ = 0;
    }

    ObMergerRpcProxy::ObMergerRpcProxy(
        const int64_t retry_times, const int64_t timeout,
        const ObServer & root_server)
    {
      init_ = false;
      rpc_retry_times_ = retry_times;
      rpc_timeout_ = timeout;
      root_server_ = root_server;
      min_fetch_interval_ = 10 * 1000 * 1000L;
      fetch_ups_timestamp_ = 0;
      rpc_stub_ = NULL;
      cur_finger_print_ = 0;
      //add dyr [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20150825:b
      cur_master_ups_list_finger_print_ = 0;
      //add dyr 20150825:e
      fail_count_threshold_ = 100;
      black_list_timeout_ = 60 * 1000 * 1000L;
      fetch_schema_timestamp_ = 0;
      schema_manager_ = NULL;
    }

    ObMergerRpcProxy::~ObMergerRpcProxy()
    {
    }

    bool ObMergerRpcProxy::check_inner_stat(void) const
    {
      return(init_ && (NULL != rpc_stub_) && (NULL != schema_manager_));
    }

    int ObMergerRpcProxy::init(
        common::ObGeneralRpcStub * rpc_stub, ObSqlRpcStub * sql_rpc_stub, ObMergerSchemaManager * schema
        //add lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
        , const int32_t cluster_id
        //add:e
        )
    {
      int ret = OB_SUCCESS;
      if ((NULL == rpc_stub) || (NULL == schema) || (NULL == sql_rpc_stub))
      {
        YYSYS_LOG(WARN, "check schema or tablet cache failed:"
            "rpc[%p], schema[%p], sql_rpc_stub[%p]", rpc_stub, schema, sql_rpc_stub);
        ret = OB_INPUT_PARAM_ERROR;
      }
      else if (true == init_)
      {
        YYSYS_LOG(WARN, "check already inited");
        ret = OB_INNER_STAT_ERROR;
      }
      else
      {
        rpc_stub_ = rpc_stub;
        sql_rpc_stub_ = sql_rpc_stub;
        schema_manager_ = schema;
        //add lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
        cluster_id_ = cluster_id;
        //add:e
        init_ = true;
      }
      return ret;
    }



    //master_ups_list [out]�� master ups �б�
    //master_ups_count [out]�� master  ups ������
    //@berif�����ڻ�ȡmaster ups �б�
    //TODO(by duyanrong)   ���Ӷ�ʱ���񣬻�ȡ master_update_server_list_
    int ObMergerRpcProxy::get_master_ups_list(common::ObUpsList  &master_ups_list, int64_t &master_ups_count)
    {
      int ret = OB_SUCCESS;
      int32_t ups_count = master_update_server_list_.ups_count_;
      if(ups_count == 0)
      {
        ret = fetch_master_update_server_list(ups_count);
        if(OB_SUCCESS != ret)
        {
          YYSYS_LOG(WARN, "get  master  ups fail");
        }
        else if(ups_count == 0)
        {
        ret = OB_ENTRY_NOT_EXIST;
          YYSYS_LOG(WARN, "can find  master ups,ups  count:%d", ups_count);
        }
        else
        {
          memcpy(&master_ups_list, &master_update_server_list_, sizeof(master_ups_list));
          master_ups_count = ups_count;
        }
      }
      else
      {
        memcpy(&master_ups_list, &master_update_server_list_, sizeof(master_ups_list));
        master_ups_count = ups_count;
      }
      return ret;
    }
    //add 20150630:e

    //add shili [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20150630:b
    //count   [out] �� master_ups ������
    //@berif ��rs ����master update server list
    int ObMergerRpcProxy::fetch_master_update_server_list(int32_t &count)
    {
      int ret = OB_SUCCESS;
      if(!check_inner_stat())
      {
        YYSYS_LOG(ERROR, "%s", "check inner stat failed");
        ret = OB_ERROR;
      }
      else
      {
        ObUpsList list;
        if(OB_SUCCESS != (ret = rpc_stub_->fetch_master_server_list(rpc_timeout_, root_server_, list)))
        {
          YYSYS_LOG(WARN, "fetch master server list from root server %s failed:ret[%d]",
                    to_cstring(root_server_), ret);
        }
        else
        {
           count = list.ups_count_;
          //mod dyr [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20150825:b
//          if(0 != list.ups_count_)
//          {
//            yysys::CWLockGuard lock(master_ups_list_lock_);
//            memcpy(&master_update_server_list_, &list, sizeof(master_update_server_list_));
//          }
//          else
//          {
//            master_update_server_list_.ups_count_ =0;
//            YYSYS_LOG(DEBUG, "master ups list not changed count[%d]", list.ups_count_);
//          }

           modify_ups_list(list);
           uint64_t master_list_finger_print = ob_crc64(&list, sizeof(list));
           if (list.ups_count_ != 0 && master_list_finger_print != cur_master_ups_list_finger_print_)
           {
             YYSYS_LOG(INFO, "master ups list changed succ:cur_finger_print[%lu], new_finger_print[%lu], ups_count[%d]",
                       cur_master_ups_list_finger_print_, master_list_finger_print, count);
             list.print();
             yysys::CWLockGuard lock(master_ups_list_lock_);
             cur_master_ups_list_finger_print_ = master_list_finger_print;
             memcpy(&master_update_server_list_, &list, sizeof(master_update_server_list_));
           }
           else
           {
             YYSYS_LOG(DEBUG, "master ups list not changed count[%d]", list.ups_count_);
           }
          //mod dyr 20150825:e

        }
      }
      return ret;
    }
    //add 20150630:e


    int ObMergerRpcProxy::fetch_update_server_list(int32_t &count)
    {
      int ret = OB_SUCCESS;
      if (!check_inner_stat())
      {
        YYSYS_LOG(ERROR, "%s", "check inner stat failed");
        ret = OB_ERROR;
      }
      else
      {
        ObUpsList list;
        if (OB_SUCCESS != (ret = rpc_stub_->fetch_server_list(rpc_timeout_, root_server_, list)))
        {
          YYSYS_LOG(WARN, "fetch server list from root server %s failed:ret[%d]",
              to_cstring(root_server_), ret);
        }
        else
        {
          count = list.ups_count_;         
          // if has error modify the list
          modify_ups_list(list);
          // check finger print changed
          uint64_t finger_print = ob_crc64(&list, sizeof(list));
          if (list.ups_count_ != 0 && finger_print != cur_finger_print_)
          {
            YYSYS_LOG(INFO, "ups list changed succ:cur[%lu], new[%lu], ups_count[%d]",
                cur_finger_print_, finger_print, count);
            list.print();
            update_ups_info(list);
            yysys::CWLockGuard lock(ups_list_lock_);
            cur_finger_print_ = finger_print;
            memcpy(&update_server_list_, &list, sizeof(update_server_list_));
            // init update server blacklist fail_count threshold, timeout
            ret = black_list_.init(static_cast<int32_t>(fail_count_threshold_), black_list_timeout_,
              MERGE_SERVER, update_server_list_);
            if (ret != OB_SUCCESS)
            {
              // if failed use old blacklist info
              YYSYS_LOG(ERROR, "init black list failed use old blacklist info:ret[%d]", ret);
            }
            else
            {
              ret = ups_black_list_for_merge_.init(static_cast<int32_t>(fail_count_threshold_), black_list_timeout_,
                CHUNK_SERVER, update_server_list_);
              if (ret != OB_SUCCESS)
              {
                // if failed use old blacklist info
                YYSYS_LOG(ERROR, "init black list failed use old blacklist info:ret[%d]", ret);
              }
            }
          }
          else
          {
            YYSYS_LOG(DEBUG, "ups list not changed:finger[%lu], count[%d]", finger_print, list.ups_count_);
          }
        }
      }
      return ret;
    }

    //[492]
    int ObMergerRpcProxy::force_fetch_sys_table_update_server_list(ObUpsList &list, int32_t &count)
    {
        int ret = OB_SUCCESS;
        if(!check_inner_stat())
        {
            YYSYS_LOG(ERROR, "%s", "check inner stat failed");
            ret = OB_ERROR;
        }
        else
        {
            if(OB_SUCCESS != (ret = rpc_stub_->force_fetch_sys_table_update_server_list(rpc_timeout_, root_server_, list)))
            {
                YYSYS_LOG(WARN, "fetch master server list from root server %s failed:ret[%d]",
                          to_cstring(root_server_), ret);
            }
            else
            {
                count = list.ups_count_;
            }
        }
        return ret;
    }

    void ObMergerRpcProxy::update_ups_info(const ObUpsList & list)
    {
      for (int64_t i = 0; i < list.ups_count_; ++i)
      {
        if (UPS_MASTER == list.ups_array_[i].stat_)
        {
          update_server_ = list.ups_array_[i].addr_;
          inconsistency_update_server_.set_ipv4_addr(
          update_server_.get_ipv4(), list.ups_array_[i].inner_port_);
          break;
        }
      }      
    }

    int ObMergerRpcProxy::release_schema(const ObSchemaManagerV2 * manager)
    {
      int ret = OB_SUCCESS;
      if (!check_inner_stat() || (NULL == manager))
      {
        YYSYS_LOG(WARN, "check inner stat failed");
        ret = OB_ERROR;
      }
      else
      {
        ret = schema_manager_->release_schema(manager);
        if (ret != OB_SUCCESS)
        {
          YYSYS_LOG(WARN, "release scheam failed:schema[%p], timestamp[%ld]",
              manager, manager->get_version());
        }
      }
      return ret;
    }

    int ObMergerRpcProxy::get_schema(const uint64_t table_id,
        const int64_t timestamp, const ObSchemaManagerV2 ** manager)
    {
      int ret = OB_SUCCESS;
      if (!check_inner_stat() || (NULL == manager))
      {
        YYSYS_LOG(WARN, "check inner stat failed");
        ret = OB_ERROR;
      }
      else if (table_id <= 0 || table_id == OB_INVALID_ID)
      {
        YYSYS_LOG(WARN, "get schema error table id =%lu", table_id);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        switch (timestamp)
        {
        // local newest version
        case LOCAL_NEWEST:
          {
            *manager = schema_manager_->get_schema(table_id);
            if (NULL == *manager)
            {
              ret = get_new_schema(timestamp, manager);
              YYSYS_LOG(INFO, "force get user schema, ts=%ld err=%d manager=%p", timestamp, ret, *manager);
            }
            break;
          }
        // get server new version with old timestamp
        default:
          {
            ret = get_new_schema(timestamp, manager);
          }
        }
        // check shema data
        if ((ret != OB_SUCCESS) || (NULL == *manager))
        {
          YYSYS_LOG(DEBUG, "check get schema failed:schema[%p], version[%ld], ret[%d]",
              *manager, timestamp, ret);
        }
      }
      return ret;
    }

    int ObMergerRpcProxy::get_new_schema(const int64_t timestamp,
                                         const ObSchemaManagerV2 ** manager)
    {
      int ret = OB_SUCCESS;
      // check update timestamp LEAST_FETCH_SCHMEA_INTERVAL
      if (yysys::CTimeUtil::getTime() - fetch_schema_timestamp_ < LEAST_FETCH_SCHEMA_INTERVAL)
      {
        YYSYS_LOG(WARN, "check last fetch schema timestamp is too nearby:version[%ld]", timestamp);
        ret = OB_OP_NOT_ALLOW;
      }
      else
      {
        int64_t new_version = 0;
        ret = rpc_stub_->fetch_schema_version(rpc_timeout_, root_server_, new_version);
        if (ret != OB_SUCCESS)
        {
          YYSYS_LOG(WARN, "fetch schema version from %s failed:ret[%d]", to_cstring(root_server_), ret);
        }
        else if (new_version <= timestamp)
        {
          YYSYS_LOG(DEBUG, "check local version not older than root version:local[%ld], root[%ld]",
            timestamp, new_version);
          ret = OB_NO_NEW_SCHEMA;
        }
        else
        {
          ret = fetch_new_schema(new_version, manager);
          if (ret != OB_SUCCESS)
          {
            YYSYS_LOG(WARN, "fetch new schema failed:local[%ld], root[%ld], ret[%d]",
              timestamp, new_version, ret);
          }
        }
      }
      return ret;
    }

    int ObMergerRpcProxy::fetch_new_schema(const int64_t timestamp,
                                           const ObSchemaManagerV2 ** manager)
    {
      int ret = OB_SUCCESS;
      if (NULL == manager)
      {
        YYSYS_LOG(WARN, "check shema manager param failed:manager[%p]", manager);
        ret = OB_INPUT_PARAM_ERROR;
      }
      else
      {
        yysys::CThreadGuard lock(&schema_lock_);
        if (schema_manager_->get_latest_version() >= timestamp)
        {
          *manager = schema_manager_->get_user_schema(0);
          if (NULL == *manager)
          {
            YYSYS_LOG(WARN, "get latest but local schema failed:schema[%p], latest[%ld]",
              *manager, schema_manager_->get_latest_version());
            ret = OB_INNER_STAT_ERROR;
          }
          else
          {
            YYSYS_LOG(DEBUG, "get new schema is fetched by other thread:schema[%p], latest[%ld]",
              *manager, (*manager)->get_version());
          }
        }
        else
        {
          char * temp = (char *)ob_malloc(sizeof(ObSchemaManagerV2),ObModIds::OB_MS_RPC);
          if (NULL == temp)
          {
            YYSYS_LOG(ERROR, "check ob malloc failed");
            ret = OB_ALLOCATE_MEMORY_FAILED;
          }
          else
          {
            ObSchemaManagerV2 * schema = new(temp) ObSchemaManagerV2;
            //add zhaoqiong [Schema Manager] 20150327:b
            const ObSchemaManagerV2 * schema_tmp = NULL;
            bool need_fetch_schema = true;
            //add:e
            if (NULL == schema)
            {
              YYSYS_LOG(ERROR, "check replacement new schema failed:schema[%p]", schema);
              ret = OB_INNER_STAT_ERROR;
            }
            //add zhaoqiong [Schema Manager] 20150327:b
            else if (NULL != (schema_tmp = schema_manager_->get_user_schema(0)))
            {
              schema->copy_without_sort(*schema_tmp);
              ObSchemaMutator schema_mutator;
              if (OB_SUCCESS != (ret = schema_manager_->release_schema(schema_tmp)))
              {
                YYSYS_LOG(WARN, "can not release schema version [%ld], ret[%d]",schema_manager_->get_latest_version(), ret);
              }
              else if (OB_SUCCESS != (ret = schema_service_->fetch_schema_mutator(schema_manager_->get_latest_version(), timestamp, schema_mutator)))
              {
                YYSYS_LOG(ERROR, "fetch schema mutator error, ret = %d", ret);
              }
              else if (!schema_mutator.get_refresh_schema())
              {
                need_fetch_schema = false;
                if (OB_SUCCESS != (ret = schema->apply_schema_mutator(schema_mutator)))
                {
                  YYSYS_LOG(WARN, "apply_schema_mutator fail(mutator version[%ld->%ld])",
                            schema_mutator.get_start_version(), schema_mutator.get_end_version());
                }
              }
            }
            //add:e
            //mod zhaoqiong [Schema Manager] 20150327:b
//            else
//            {
//              ret = rpc_stub_->fetch_schema(rpc_timeout_, root_server_, 0, false, *schema);
//              if (ret != OB_SUCCESS)
//              {
//                YYSYS_LOG(WARN, "rpc fetch schema failed:version[%ld], ret[%d]", timestamp, ret);
//              }
//              else
//              {
//                fetch_schema_timestamp_ = yysys::CTimeUtil::getTime();
//                ret = schema_manager_->add_schema(*schema, manager);
//                // maybe failed because of timer thread fetch and add it already
//                if (OB_SUCCESS != ret)
//                {
//                  YYSYS_LOG(WARN, "add new schema failed:version[%ld], ret[%d]", schema->get_version(), ret);
//                  ret = OB_SUCCESS;
//                  *manager = schema_manager_->get_user_schema(0);
//                  if (NULL == *manager)
//                  {
//                    YYSYS_LOG(WARN, "get latest schema failed:schema[%p], latest[%ld]",
//                        *manager, schema_manager_->get_latest_version());
//                    ret = OB_INNER_STAT_ERROR;
//                  }
//                }
//                else
//                {
//                  YYSYS_LOG(DEBUG, "fetch and add new schema succ:version[%ld]", schema->get_version());
//                }
//              }
//            }
            if (OB_SUCCESS == ret && need_fetch_schema && schema_manager_->get_latest_version() == -1)
            {
              YYSYS_LOG(INFO, "fetch full schema from rs");
              schema->reset();
              ret = rpc_stub_->fetch_schema(rpc_timeout_, root_server_, 0, false, *schema);
            }
            else if (OB_SUCCESS == ret && need_fetch_schema && timestamp > 0)
            {
              YYSYS_LOG(INFO, "fetch full schema from system table");//add zhaoqiong [Schema Manager] 20150327
              schema->reset();
              if (OB_SUCCESS !=  (ret = schema_service_->get_schema(false, *schema)))
              {
                YYSYS_LOG(WARN, "get schema from system table failed:ret[%d]", ret);
              }
              else
              {
                schema->set_version(timestamp);
              }
            }

            if (ret != OB_SUCCESS)
            {
              YYSYS_LOG(WARN, "rpc fetch schema failed:version[%ld], ret[%d]", timestamp, ret);
            }
            else
            {
              YYSYS_LOG(INFO, "fetch new schema succ, timestamp:%ld", schema->get_version());
              fetch_schema_timestamp_ = yysys::CTimeUtil::getTime();
              ret = schema_manager_->add_schema(*schema, manager);
              // maybe failed because of timer thread fetch and add it already
              if (OB_SUCCESS != ret)
              {
                YYSYS_LOG(WARN, "add new schema failed:version[%ld], ret[%d]", schema->get_version(), ret);
                ret = OB_SUCCESS;
                *manager = schema_manager_->get_user_schema(0);
                if (NULL == *manager)
                {
                  YYSYS_LOG(WARN, "get latest schema failed:schema[%p], latest[%ld]",
                      *manager, schema_manager_->get_latest_version());
                  ret = OB_INNER_STAT_ERROR;
                }
              }
              else
              {
                YYSYS_LOG(DEBUG, "fetch and add new schema succ:version[%ld]", schema->get_version());
              }
            }
            //mod:e
            schema->~ObSchemaManagerV2();
            ob_free(temp);
            temp = NULL;
          }
        }
      }
      return ret;
    }

    int ObMergerRpcProxy::fetch_schema_version(int64_t & timestamp)
    {
      int ret = OB_SUCCESS;
      if (!check_inner_stat())
      {
        YYSYS_LOG(WARN, "check inner stat failed");
        ret = OB_INNER_STAT_ERROR;
      }
      else
      {
        ret = rpc_stub_->fetch_schema_version(rpc_timeout_, root_server_, timestamp);
        if (ret != OB_SUCCESS)
        {
          YYSYS_LOG(WARN, "fetch schema version failed:ret[%d]", ret);
        }
        else
        {
          YYSYS_LOG(DEBUG, "fetch schema version succ:version[%ld]", timestamp);
        }
      }
      return ret;
    }

    int ObMergerRpcProxy::get_frozen_time(
        const int64_t frozen_version, int64_t& forzen_time)
    {
      int ret = OB_SUCCESS;
      if (!check_inner_stat())
      {
        YYSYS_LOG(WARN, "check inner stat failed");
        ret = OB_INNER_STAT_ERROR;
      }
      else
      {
        ObServer update_server;
        ret = get_update_server(false, update_server);
        if (ret != OB_SUCCESS)
        {
          YYSYS_LOG(WARN, "get master update server failed:ret[%d]", ret);
        }
        else
        {
          ret = rpc_stub_->fetch_frozen_time(rpc_timeout_, update_server,
              frozen_version, forzen_time);
          if (ret != OB_SUCCESS)
          {
            YYSYS_LOG(WARN, "fetch frozen time failed:ret[%d]", ret);
          }
          else
          {
            YYSYS_LOG(DEBUG, "fetch frozen time succ:frozen version[%ld],"
                "frozen time[%ld]",
                frozen_version, forzen_time);
          }
        }
      }
      return ret;
    }

    //add zhaoqiong [Truncate Table]:20160318:b
    int ObMergerRpcProxy::check_incremental_data_range(
        int64_t table_id, ObVersionRange &version, ObVersionRange &new_range, bool is_strong_consistency)
    {
      int ret = OB_SUCCESS;

      if (!check_inner_stat())
      {
        YYSYS_LOG(WARN, "check inner stat failed");
        ret = OB_INNER_STAT_ERROR;
      }
      else
      {
        ObServer update_server;
        ret = get_update_server(SYS_TABLE_PAXOS_ID, is_strong_consistency, update_server);
        if (ret != OB_SUCCESS)
        {
          YYSYS_LOG(WARN, "get update server failed:ret[%d]", ret);
        }
        else
        {
          char range_buf[OB_RANGE_STR_BUFSIZ];
          version.to_string(range_buf, sizeof(range_buf));
          YYSYS_LOG(DEBUG, "check incremental_data range start:old_version[%s]",
              range_buf);
          ret = rpc_stub_->check_incremental_data_range(rpc_timeout_, update_server,
              table_id, version, new_range);
          if (ret != OB_SUCCESS)
          {
            YYSYS_LOG(WARN, "check incremental_data range failed:ret[%d]", ret);
          }
          else
          {
            memset(range_buf, 0, sizeof(range_buf));
            new_range.to_string(range_buf, sizeof(range_buf));
            YYSYS_LOG(DEBUG, "check incremental_data succ:new_version[%s]",
                range_buf);
          }
        }
      }
      return ret;
    }
    //add:e

    int ObMergerRpcProxy::get_frozen_schema(
      const int64_t frozen_version, ObSchemaManagerV2& schema)
    {
      int ret = OB_SUCCESS;

      if (!check_inner_stat())
      {
        YYSYS_LOG(WARN, "check inner stat failed");
        ret = OB_INNER_STAT_ERROR;
      }
      else
      {
        ObServer update_server;
        ret = get_update_server(false, update_server);
        if (ret != OB_SUCCESS)
        {
          YYSYS_LOG(WARN, "get master update server failed:ret[%d]", ret);
        }
        else
        {
          ret = rpc_stub_->fetch_schema(rpc_timeout_, update_server,
              frozen_version, false, schema);
          if (ret != OB_SUCCESS)
          {
            YYSYS_LOG(WARN, "fetch frozen schema failed:ret[%d]", ret);
          }
          else
          {
            YYSYS_LOG(DEBUG, "fetch frozen schema succ:frozen version[%ld]",
                frozen_version);
            //add zhaoqiong [Schema Manager] 20150327:b
            YYSYS_LOG(TRACE, "==========print schema version[%ld] start==========",schema.get_version());
            schema.print_debug_info();
            YYSYS_LOG(TRACE, "==========print schema version[%ld] end==========",schema.get_version());
            //add:e
          }
        }
      }

      return ret;
    }

    int ObMergerRpcProxy::get_update_server(const bool renew,
                                            ObServer & server,
                                            bool need_master)
    {
      int ret = OB_SUCCESS;
      bool is_master_addr_invalid = false;
      {
        yysys::CRLockGuard lock(ups_list_lock_);
        is_master_addr_invalid = (0 == update_server_.get_ipv4());
      }
      if (true == renew || is_master_addr_invalid)
      {
        int64_t timestamp = yysys::CTimeUtil::getTime();
        if (timestamp - fetch_ups_timestamp_ > min_fetch_interval_ || is_master_addr_invalid)
        {
          int32_t server_count = 0;
          yysys::CThreadGuard lock(&update_lock_);
          if (timestamp - fetch_ups_timestamp_ > min_fetch_interval_ || is_master_addr_invalid)
          {
            YYSYS_LOG(DEBUG, "renew fetch update server list");
            fetch_ups_timestamp_ = yysys::CTimeUtil::getTime();
            // renew the udpate server list
            ret = fetch_update_server_list(server_count);
            if (ret != OB_SUCCESS)
            {
              YYSYS_LOG(WARN, "fetch update server list failed:ret[%d]", ret);
            }
            else if (server_count == 0)
            {
              YYSYS_LOG(DEBUG, "fetch update server list empty retry fetch vip update server");
              // using old protocol get update server ip
              ret = rpc_stub_->fetch_update_server(rpc_timeout_, root_server_, server);
              if (ret != OB_SUCCESS)
              {
                YYSYS_LOG(WARN, "find update server vip failed:ret[%d]", ret);
              }
              else
              {
                update_server_ = server;
              }

              if (OB_SUCCESS == ret)
              {
                // using old protocol get update server ip for daily merge
                ret = rpc_stub_->fetch_update_server(rpc_timeout_, root_server_,
                    server, true);
                if (ret != OB_SUCCESS)
                {
                  YYSYS_LOG(WARN, "find update server vip for daily merge failed:ret[%d]", ret);
                }
                else
                {
                  inconsistency_update_server_ = server;
                }
              }
            }
          }
          else
          {
            YYSYS_LOG(DEBUG, "fetch update server list by other thread");
          }
        }
      }
      // renew master update server addr
      yysys::CThreadGuard lock(&update_lock_);
      YYSYS_LOG(DEBUG, "update_server_[%s],inconsistency_update_server_[%s]", update_server_.to_cstring(), inconsistency_update_server_.to_cstring());
      server = need_master ? update_server_ : inconsistency_update_server_;
      return ret;
    }

    int ObMergerRpcProxy::ups_mutate(const ObMutator &mutate_param, const bool has_data, ObScanner &scanner)
    {
        YYSYS_LOG(INFO, "huangcc_test::ups_mutate");
        int ret = OB_SUCCESS;
        if(!check_inner_stat())
        {
            YYSYS_LOG(ERROR, "%s", "check inner stat failed");
            ret = OB_INNER_STAT_ERROR;
        }
        else
        {
            ret = OB_PROCESS_TIMEOUT;
            ObServer update_server;
            int64_t end_time = rpc_timeout_ + yysys::CTimeUtil::getTime();
            for(int64_t i = 0; yysys::CTimeUtil::getTime() < end_time; ++i)
            {
                ret = get_update_server((OB_NOT_MASTER == ret || OB_RESPONSE_TIME_OUT == ret), update_server, true);
                if(ret != OB_SUCCESS)
                {
                    YYSYS_LOG(WARN, "get master update server failed:ret[%d]", ret);
                    break;
                }
                ret = rpc_stub_->mutate(rpc_timeout_, update_server, mutate_param, has_data, scanner);
                if(false == check_need_retry_ups(ret))
                {
                    break;
                }
                else
                {
                    YYSYS_LOG(WARN, "mutate fail. retry. ret=%d, i=%ld, rpc_timeout_=%ld.", ret, i, rpc_timeout_);
                    usleep(static_cast<useconds_t>(RETRY_INTERVAL_TIME + (i + 1)));
                }
            }
        }
        return ret;
    }


    int ObMergerRpcProxy::get_master_master_update_server(const bool renew, ObServer & master_master_ups)
    {
      int ret = OB_SUCCESS;
      bool is_master_addr_invalid = false;
      {
        yysys::CRLockGuard lock(mm_ups_list_lock_);
        is_master_addr_invalid = (0 == master_master_ups_.get_ipv4());
      }
      if (true == renew || is_master_addr_invalid)
      {
        int64_t timestamp = yysys::CTimeUtil::getTime();
        if (timestamp - fetch_mm_ups_timestamp_ > min_fetch_interval_ || is_master_addr_invalid)
        {
          yysys::CThreadGuard lock(&mm_update_lock_);
          timestamp = yysys::CTimeUtil::getTime();
          if (timestamp - fetch_mm_ups_timestamp_ > min_fetch_interval_ || is_master_addr_invalid)
          {
            YYSYS_LOG(DEBUG, "renew fetch update server list");
            fetch_mm_ups_timestamp_ = timestamp;
            // renew the udpate server list
            if (OB_SUCCESS != (ret = get_inst_master_ups(root_server_, master_master_ups)))
            {
              YYSYS_LOG(WARN, "fail to get master master ups. ret=%d", ret);
            }
            else
            {
              yysys::CWLockGuard lock(mm_ups_list_lock_);
              master_master_ups_ = master_master_ups;
              YYSYS_LOG(DEBUG, "master_master_ups_=%s", to_cstring(master_master_ups_));
            }
          }
          else
          {
            YYSYS_LOG(DEBUG, "fetch update server list by other thread");
          }
        }
      }
      // renew master update server addr
      yysys::CWLockGuard lock (mm_ups_list_lock_);
      master_master_ups = master_master_ups_;
      return ret;
    }

    int ObMergerRpcProxy::get_inst_master_ups(const common::ObServer &root_server, common::ObServer &ups_master)
    {
      int err = OB_SUCCESS;
      common::ObServer master_inst_rs;

      // query who is master instance rootserver according to OBI_ROLE
      //del pangtianze [Paxos] 20170627:b  only one master rs in big cluster
//      if (OB_SUCCESS == err)
//      {
//        if (OB_SUCCESS != (err = get_master_obi_rs(root_server, master_inst_rs)))
//        {
//          YYSYS_LOG(WARN, "fail to get master obi rootserver addr. err=%d", err);
//        }
//      }
      //del:e
      //add pangtianze [Paxos] 20170627:b
      master_inst_rs = root_server;
      //add:e
      // ask the master instance rs for master master ups addr
      if (OB_SUCCESS == err)
      {
        if (OB_SUCCESS != (err = rpc_stub_->get_master_ups_info(rpc_timeout_, master_inst_rs, ups_master)))
        {
          YYSYS_LOG(WARN, "fail to get master obi ups addr. master_inst_rs=%s, err=%d", master_inst_rs.to_cstring(), err);
        }
      }
      return err;
    }

    int ObMergerRpcProxy::get_master_obi_rs(const common::ObServer &rootserver,
        common::ObServer &master_obi_rs)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = rpc_stub_->get_master_obi_rs(rpc_timeout_, rootserver, master_obi_rs)))
      {
        YYSYS_LOG(WARN, "get master ob rootservre fail, ret: [%d]", ret);
      }
      return ret;
    }



    bool ObMergerRpcProxy::check_range_param(const ObNewRange & range_param)
    {
      bool bret = true;
      if (((!range_param.start_key_.is_min_row()) && (0 == range_param.start_key_.length()))
          || (!range_param.end_key_.is_max_row() && (0 == range_param.end_key_.length())))
      {
        YYSYS_LOG(WARN, "check range param failed");
        bret = false;
      }
      return bret;
    }

    bool ObMergerRpcProxy::check_scan_param(const ObScanParam & scan_param)
    {
      bool bret = true;
      const ObNewRange * range = scan_param.get_range();
      // the min/max value length is zero
      if (NULL == range)// || (0 == range->start_key_.length()))
      {
        YYSYS_LOG(WARN, "check scan range failed");
        bret = false;
      }
      else
      {
        bret = check_range_param(*range);
      }
      return bret;
    }

    // must be in one chunk server
    int ObMergerRpcProxy::cs_get(
        const ObGetParam & get_param,
        ObScanner & scanner,  ObIterator *it_out[],int64_t& it_size)
    {
      UNUSED(get_param);
      UNUSED(scanner);
      UNUSED(it_out);
      UNUSED(it_size);
      YYSYS_LOG(WARN, "not implement");
      return OB_ERROR;
    }

    // only one communication with chunk server
    int ObMergerRpcProxy::cs_scan(
        const ObScanParam & scan_param,
        ObScanner & scanner, ObIterator *it_out[],int64_t& it_size)
    {
      UNUSED(scan_param);
      UNUSED(scanner);
      UNUSED(it_out);
      UNUSED(it_size);
      YYSYS_LOG(WARN, "not implement");
      return OB_ERROR;
    }

    void ObMergerRpcProxy::modify_ups_list(ObUpsList & list)
    {

      if (0 == list.ups_count_)
      {
        // add vip update server to list
        YYSYS_LOG(DEBUG, "check ups count is zero:count[%d]", list.ups_count_);
        if (update_server_.get_port() == 0 || update_server_.get_ipv4() == 0)
        {
          YYSYS_LOG(INFO, "can't get ups list from rootserver, and prev ups is None");
        }
        else
        {
          YYSYS_LOG(INFO, "can't get ups list from rootserver, using prev ups: [%s]",
                    to_cstring(update_server_));
          ObUpsInfo info;
          info.addr_ = update_server_;
          // set inner port to update server port
          info.inner_port_ = inconsistency_update_server_.get_port();
          info.ms_read_percentage_ = 100;
          info.cs_read_percentage_ = 100;
          list.ups_count_ = 1;
          list.ups_array_[0] = info;
          list.sum_ms_percentage_ = 100;
          list.sum_cs_percentage_ = 100;
        }
      }
      else
      {
        YYSYS_LOG(DEBUG, "reset the percentage for all servers");
        if (list.get_sum_percentage(MERGE_SERVER) <= 0)
        {
          for (int32_t i = 0; i < list.ups_count_; ++i)
          {
            // reset all ms to equal
            list.ups_array_[i].ms_read_percentage_ = 1;
          }
          // reset all ms sum percentage to count
          list.sum_ms_percentage_ = list.ups_count_;
        }
        if (list.get_sum_percentage(CHUNK_SERVER) <= 0)
        {
          for (int32_t i = 0; i < list.ups_count_; ++i)
          {
            // reset all cs to equal
            list.ups_array_[i].cs_read_percentage_ = 1;
          }
          // reset all cs sum percentage to count
          list.sum_cs_percentage_ = list.ups_count_;
        }


        //[402]:b
        bool online_paxos_group_array[MAX_UPS_COUNT_ONE_CLUSTER] = {false};
        memset(online_paxos_group_array, 0, sizeof(bool)*MAX_UPS_COUNT_ONE_CLUSTER);

        for(int32_t i = 0; i < list.ups_count_; ++i)
        {
            if(0 <= list.ups_array_[i].paxos_id_ && list.ups_array_[i].paxos_id_ < MAX_UPS_COUNT_ONE_CLUSTER
                    && online_paxos_group_array[list.ups_array_[i].paxos_id_] != true)
            {
                online_paxos_group_array[list.ups_array_[i].paxos_id_] = true;
            }
        }

        for(int32_t pid = 0; pid < MAX_UPS_COUNT_ONE_CLUSTER; pid++)
        {
            if(online_paxos_group_array[pid] == true)
            {
                if(0 >= list.get_sum_percentage(pid, OB_ALL_CLUSTER_FLAG, MERGE_SERVER))
                {
                    list.reset_percentage(pid, MERGE_SERVER);
                }
                if(0 >= list.get_sum_percentage(pid, OB_ALL_CLUSTER_FLAG, CHUNK_SERVER))
                {
                    list.reset_percentage(pid, CHUNK_SERVER);
                }
            }
        }
        //[402]:e
      }
    }



    //add shili [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20150627:b
    //is_strong_consistency [in] true: get data from master_ups  false:get data from slave ups
    //update_server [out]  ����paxos_id  ��õ� upsdate server
    //@berif ����paxos_id ��ȡ ups ��ַ��is_strong_consistencyΪtrue����ʾ��ups��ַ  false:��ʾslave ups��ַ
    int ObMergerRpcProxy::get_update_server(const int64_t paxos_id,
                                            const bool is_strong_consistency,
                                            ObServer &update_server,
                                            const common::ObServerType server_type
                                           )
    {
      int     ret           = OB_SUCCESS;
      int32_t server_count  = 0;
      //master
      if(true == is_strong_consistency)
      {
        if (OB_SUCCESS == ret)
        {
          yysys::CRLockGuard lock(master_ups_list_lock_);
          server_count = master_update_server_list_.ups_count_;
        }
        if(OB_SUCCESS == ret && 0 == server_count)
        {
          YYSYS_LOG(DEBUG, "no master_ups right now local, updating...");
          if(OB_SUCCESS != (ret = fetch_master_update_server_list(server_count)))
          {
            YYSYS_LOG(WARN, "get master update server list failed:ret[%d]", ret);
          }
          else if(0 == server_count)
          {
            ret = OB_UPS_NOT_EXIST;
            YYSYS_LOG(WARN, "no master update server available right now, ret: [%d]", ret);
          }
          else
          {
            YYSYS_LOG(DEBUG, "master update local ups list"
                      " info successfully! Got [%d] ups.", server_count);
          }
        }
        if(OB_SUCCESS == ret)
        {
          if(OB_SUCCESS != (ret= select_update_server(paxos_id,master_update_server_list_,
                                                      update_server,true, server_type)))
          {
            ret = OB_ENTRY_NOT_EXIST;
            YYSYS_LOG(WARN,"can't find update_server,paxos_id[%ld],"
                           "is_strong_consistency[%d],server_count[%d],ret[%d]",
                           paxos_id,is_strong_consistency,master_update_server_list_.ups_count_,ret);
          }
        }
      }
      //slave
      else
      {
        if (OB_SUCCESS == ret)
        {
          yysys::CRLockGuard lock(ups_list_lock_);
          server_count = update_server_list_.ups_count_;
        }

        if(OB_SUCCESS == ret && 0 == server_count)
        {
          YYSYS_LOG(DEBUG, "no ups right now local, updating...");
          if(OB_SUCCESS != (ret = fetch_update_server_list(server_count)))
          {
            YYSYS_LOG(WARN, "fetch update server list fail, ret: [%d]", ret);
          }
          else if(0 == server_count)
          {
            ret = OB_UPS_NOT_EXIST;
            YYSYS_LOG(WARN, "no update server available right now, ret: [%d]", ret);
          }
          else
          {
            YYSYS_LOG(DEBUG, "update local ups list"
                      " info successfully! Got [%d] ups.", server_count);
          }
        }

        if(OB_SUCCESS == ret)
        {
          if(OB_SUCCESS != (ret= select_update_server(paxos_id, update_server_list_,
                                                      update_server, false, server_type)))
          {
            YYSYS_LOG(WARN,"can't find update_server,paxos_id[%ld],"
                           "is_strong_consistency[%d],server_count[%d],ret[%d]",
                           paxos_id,is_strong_consistency,update_server_list_.ups_count_,ret);
          }
        }
      }

      //[492]
      if(ret != OB_SUCCESS && (paxos_id == SYS_TABLE_PAXOS_ID))
      {
          int err = OB_SUCCESS;
          ObUpsList sys_ups_list;
          int32_t sys_ups_count = 0;
          YYSYS_LOG(WARN, "fetch sys update server list fail, maybe system is changing master rs, "
                    "try to force get sys table ups list. ret[%d]", ret);
          if(OB_SUCCESS != (err = force_fetch_sys_table_update_server_list(sys_ups_list, sys_ups_count)))
          {
              YYSYS_LOG(WARN, "fetch sys update server list fail, ret: [%d]", err);
          }
          else if(0 == sys_ups_count)
          {
              err = OB_UPS_NOT_EXIST;
              YYSYS_LOG(WARN,"no update server available right now, ret: [%d]", err);
          }
          else
          {
              YYSYS_LOG(INFO, "force get sys table ups list"
                        " info successfully! Got [%d] ups.", sys_ups_count);
          }

          if(OB_SUCCESS == err)
          {
              if(true == is_strong_consistency)
              {
                  if(OB_SUCCESS != (err = select_update_server(paxos_id, sys_ups_list,
                                                               update_server, true, server_type)))
                  {
                      YYSYS_LOG(WARN, "can't find update_server,paxos_id[%ld],"
                                "is_strong_consistency[%d],server_count[%d],ret[%d]",
                                paxos_id, is_strong_consistency, update_server_list_.ups_count_, err);
                  }
              }
              else
              {
                  if(OB_SUCCESS != (err = select_update_server(paxos_id, sys_ups_list,
                                                               update_server, false, server_type)))
                  {
                      YYSYS_LOG(WARN, "can't find update_server,paxos_id[%ld],"
                                "is_strong_consistency[%d],server_count[%d],ret[%d]",
                                paxos_id, is_strong_consistency, update_server_list_.ups_count_, err);
                  }
              }
          }
          ret=err;
      }

      return ret;
    }
    //add 20150627:e

    // add lqc [multiups merge]20170808 b
    // be careful, the ObServerType default as  CHUNK_SERVER
    int ObMergerRpcProxy::select_update_server(const int64_t paxos_id,
                                               const ObUpsList &list,
                                               ObServer &update_server,
                                               const bool is_strong_consistency,
                                               const ObServerType server_type
                                              )
    {
      int ret = OB_SUCCESS;
      //reset
      update_server.reset ();
      {
          yysys::CRLockGuard lock(master_ups_list_lock_);
          if( 0 == list.ups_count_)
          {
              ret = OB_INVALID_ARGUMENT;
              YYSYS_LOG(WARN, "ups_array should not be NULL , ret=%d", ret);
          }
          else
          {
              if(is_strong_consistency)
              {
                  //get the master ups which is belong to paxos
                  for(int32_t k = 0; k < list.ups_count_; k++)
                  {
                      //[402]
                      //if(paxos_id == list.ups_array_[k].paxos_id_)
                      if(paxos_id == list.ups_array_[k].paxos_id_ && UPS_MASTER == list.ups_array_[k].stat_)
                      {
                          update_server = list.ups_array_[k].get_server (server_type);
                          YYSYS_LOG(DEBUG,"get the master update server [%s]",update_server.to_cstring ());
                          break;
                      }
                  }
              }
              else
              {
                  //[402]
                  //step 1: find ups in ups_node, specially in same cluster with CS/MS
                  list.get_ups(paxos_id, cluster_id_, update_server, server_type);

                  if(!update_server.is_valid())
                  {
                      //step 2: if step1 not found, find ups in master_ups's ups_node(cid_2, pid)

                      int64_t cluster_id = list.get_master_ups_cluster_id(paxos_id);
                      if(cluster_id != cluster_id_ && cluster_id != OB_INVALID_INDEX)
                      {
                          list.get_ups(paxos_id, cluster_id, update_server, server_type);
                      }
                  }

                  if(!update_server.is_valid())
                  {
                      //step3: if step2 failed, then find a random ups with cur percent > 0 in this group
                      list.get_ups(paxos_id, OB_ALL_CLUSTER_FLAG, update_server,server_type);
                  }
              }

          }
      }

      if (OB_SUCCESS == ret && !update_server.is_valid())
      {
        ret = OB_UPS_NOT_EXIST;
        //YYSYS_LOG(WARN, "get update_server[%s] failed,will get the master, ret=%d",
        //                update_server.to_cstring(),ret);
        YYSYS_LOG(WARN, "get update_server of paxos group[%ld] failed, ret=%d", paxos_id, ret);
      }
      return ret;
    }
    //add e

    //add liuzy [Multiups] [UPS_List_Info] 20160427:b
    int ObMergerRpcProxy::get_paxos_group_offline_info(ObServer &server, ObVersionRange range,
                                                       const ObClientManager *& client_mgr,
                                                       common::ObArray<int64_t> &paxos_id_array)
    {
      int ret = OB_SUCCESS;
      int64_t last_idx = usable_paxos_group_info_array_.count() - 1;

      //add hongchen [VERSION_MISMATCH_BUGFIX] 20170720:b
      //save major version for convient
      int64_t range_start_major_version = range.start_version_.major_;
      int64_t range_end_major_version = range.end_version_.major_;
      //add hongchen [VERSION_MISMATCH_BUGFIX] 20170720:e

      //mod [VERSION_MISMATCH_BUGFIX] 20170720:b
      //if (0 > last_idx || usable_paxos_group_info_array_.at(last_idx).version_ < range.end_version_)
      if (0 > last_idx || usable_paxos_group_info_array_.at(last_idx).version_ < range_end_major_version)
      {
        ObVersionRange tmp_range;
        if (0 > last_idx)
        {
          //tmp_range.start_version_ = range.start_version_;
          tmp_range.start_version_ = range_start_major_version;
        }
        else
        {
          tmp_range.start_version_ = usable_paxos_group_info_array_.at(last_idx).version_ + 1;
        }
        //tmp_range.end_version_ = range.end_version_;
        tmp_range.end_version_ = range_end_major_version;
        if (OB_SUCCESS != (ret = refresh_usable_paxos_group_info_array(server, client_mgr, tmp_range)))
        {
          YYSYS_LOG(ERROR, "refresh usable paxos group info array failed, ret=%d", ret);
        }
        YYSYS_LOG(INFO, "range version is:[%s], paxos idx array count:[%ld]",
                  to_cstring(range), usable_paxos_group_info_array_.count());
      }
      if (OB_SUCCESS == ret)
      {
        uint32_t bit_word = OB_BITSET_MAX_WORD;
        int64_t use_paxos_num = 0;
        common::ObBitSet<MAX_UPS_COUNT_ONE_CLUSTER> tmp_bitset;
        for (int64_t idx = 0; idx < usable_paxos_group_info_array_.count(); ++idx)
        {
          //if (usable_paxos_group_info_array_.at(idx).version_ < range.start_version_)
          if (usable_paxos_group_info_array_.at(idx).version_ < range_start_major_version)
          {
            continue;
          }
          //else if (usable_paxos_group_info_array_.at(idx).version_ > range.end_version_)
          else if (usable_paxos_group_info_array_.at(idx).version_ > range_end_major_version)
          {
            break;
          }
          else
          {
            bit_word &= usable_paxos_group_info_array_.at(idx).offline_bitset_.get_bitset_word(OB_BITSET_INDEX);
            if (usable_paxos_group_info_array_.at(idx).use_paxos_num_ > use_paxos_num)
            {
              use_paxos_num = usable_paxos_group_info_array_.at(idx).use_paxos_num_;
            }
          }
        }
        //mod [VERSION_MISMATCH_BUGFIX] 20170720:e
        tmp_bitset.set_bitset_word(OB_BITSET_INDEX, bit_word);
        for (int32_t paxos_idx = 0; paxos_idx < use_paxos_num; ++paxos_idx)
        {
          if (!tmp_bitset.has_member(paxos_idx))
          {
            paxos_id_array.push_back((int64_t)paxos_idx);
          }
        }
      }
      return ret;
    }
    int ObMergerRpcProxy::refresh_usable_paxos_group_info_array(ObServer ms_server, const ObClientManager *& client_mgr,
                                                                ObVersionRange range)
    {
      int ret = OB_SUCCESS;
      int64_t pos = 0;
      int64_t session_id = 0;
      common::ObString sql_str;
      const int64_t timeout = 1000000;
      const int64_t DEFAULT_VERSION = 1;
      char buf[OB_MAX_PACKET_LENGTH];
      ObDataBuffer msgbuf(buf, OB_MAX_PACKET_LENGTH);
      char sql_char[OB_MAX_SQL_LENGTH];
      memset(sql_char, 0, OB_MAX_SQL_LENGTH);
      databuff_printf(sql_char, OB_MAX_SQL_LENGTH, pos,
        "select version, use_num, stat_info from %s where version >= %ld and version <= %ld and name=\'%s\'",
        OB_ALL_CLUSTER_STAT_INFO_TABLE_NAME, range.start_version_.version_, range.end_version_.version_, PAXOS_STAT);
      if (pos >= OB_MAX_SQL_LENGTH)
      {
        ret = OB_BUF_NOT_ENOUGH;
        YYSYS_LOG(WARN, "buffer not enough, ret=%d", ret);
      }
      else
      {
        ObSQLResultSet res;
        sql_str.assign(sql_char, static_cast<ObString::obstr_size_t>(pos));
        YYSYS_LOG(INFO, "sql_str:[%.*s]", sql_str.length(), sql_str.ptr());
        if (OB_SUCCESS != (ret = sql_str.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          YYSYS_LOG(ERROR, "failed to serialize sql string, ret = [%d]", ret);
        }
        else if (OB_SUCCESS != (ret = client_mgr->send_request(ms_server, OB_SQL_EXECUTE, DEFAULT_VERSION,
                                                               timeout, msgbuf, session_id)))
        {
          YYSYS_LOG(ERROR, "failed to send request, ret = [%d]", ret);
        }
        else
        {
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (ret = res.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            YYSYS_LOG(ERROR, "deserialize result set failed, ret=%d", ret);
          }
          else
          {
            ObRow row;
            int64_t version;
            int64_t use_num;
            int64_t stat_info;
            const common::ObObj *column = NULL;
            YYSYS_LOG(INFO, "get new scanner:[%s]", res.get_new_scanner().is_empty() ? "is empty" : "not empty");
            while (OB_SUCCESS == ret && OB_SUCCESS == (ret = res.get_new_scanner().get_next_row(row)))
            {
              if (OB_SUCCESS != (ret = row.raw_get_cell(0, column)))
              {
                YYSYS_LOG(WARN, "get version cell failed, ret=%d", ret);
              }
              else if (OB_SUCCESS != (ret = column->get_int(version)))
              {
                YYSYS_LOG(WARN, "get version value failed, ret=%d", ret);
              }
              else if (OB_SUCCESS != (ret = row.raw_get_cell(1, column)))
              {
                YYSYS_LOG(WARN, "get use_num cell failed, ret=%d", ret);
              }
              else if (OB_SUCCESS != (ret = column->get_int(use_num)))
              {
                YYSYS_LOG(WARN, "get use_num value failed, ret=%d", ret);
              }
              else if (OB_SUCCESS != (ret = row.raw_get_cell(2, column)))
              {
                YYSYS_LOG(WARN, "get stat_info cell failed, ret=%d", ret);
              }
              else if (OB_SUCCESS != (ret = column->get_int(stat_info)))
              {
                YYSYS_LOG(WARN, "get stat_info value failed, ret=%d", ret);
              }
              else
              {
                UsablePaoxsInfo info;
                info.version_ = version;
                info.use_paxos_num_ = use_num;
                info.offline_bitset_.set_bitset_word(OB_BITSET_INDEX, (uint32_t)stat_info);
                usable_paxos_group_info_array_.push_back(info);
              }
            }
            if (OB_ITER_END == ret)
            {
              ret = OB_SUCCESS;
            }
            else if (OB_SUCCESS != ret)
            {
              YYSYS_LOG(ERROR, "refresh usable paxos info failed, ret=%d", ret);
            }
          }
        }
      }
      return ret;
    }
    //20160427:e


    template<class T, class RpcT>
    int ObMergerRpcProxy::master_ups_get(RpcT *rpc_stub, const ObGetParam & get_param, T & scanner,
        const int64_t time_out)
    {
      int ret = OB_ERROR;
      ObServer update_server;
      int64_t end_time = time_out + yysys::CTimeUtil::getTime();
      for (int64_t i = 0; yysys::CTimeUtil::getTime() < end_time; ++i)
      {
        bool need_renew = ((OB_NOT_MASTER == ret) || (OB_RESPONSE_TIME_OUT == ret));
        ret = get_master_master_update_server(need_renew, update_server);
        if (ret != OB_SUCCESS)
        {
          YYSYS_LOG(WARN, "get master update server failed:ret[%d]", ret);
          break;
        }
        ret = rpc_stub->get((time_out > 0) ? time_out : rpc_timeout_, update_server, get_param, scanner);
        if (false == check_need_retry_ups(ret))
        {
          break;
        }
        else
        {
          YYSYS_LOG(WARN, "ups get fail. retry. ret=%d, i=%ld, rpc_timeout_=%ld.", ret, i, rpc_timeout_);
          usleep(static_cast<useconds_t>(RETRY_INTERVAL_TIME * (i + 1)));
        }
      }
      if (OB_INVALID_START_VERSION == ret)
      {
        OB_STAT_INC(CHUNKSERVER, FAIL_CS_VERSION_COUNT);
      }
      return ret;
    }

    template<class T, class RpcT>
    int ObMergerRpcProxy::slave_ups_get(RpcT *rpc_stub, const ObGetParam & get_param,
      T & scanner, const ObServerType server_type, const int64_t time_out)
    {
      int ret = OB_SUCCESS;
      int32_t retry_times = 0;
      int32_t cur_index = -1;
      int32_t max_count = 0;
      int32_t server_count = update_server_list_.ups_count_;

      if (0 == server_count)
      {
        YYSYS_LOG(INFO, "no ups right now local, updating...");
        if (OB_SUCCESS != (ret = fetch_update_server_list(server_count)))
        {
          YYSYS_LOG(WARN, "fetch update server list fail, ret: [%d]", ret);
        }
        else if (0 == server_count)
        {
          ret = OB_UPS_NOT_EXIST;
          YYSYS_LOG(WARN, "no update server available right now, ret: [%d]", ret);
        }
        else
        {
          YYSYS_LOG(INFO, "update local ups list"
                    " info successfully! Got [%d] ups.", server_count);
        }
      }
      if (OB_SUCCESS == ret)
      {
        yysys::CRLockGuard lock(ups_list_lock_);
        max_count = server_count;
        cur_index = ObReadUpsBalance::select_server(update_server_list_, server_type);
        if (cur_index < 0)
        {
          YYSYS_LOG(WARN, "select server failed:count[%d], index[%d]", server_count, cur_index);
          ret = OB_ENTRY_NOT_EXIST;
        }
        else
        {
          ObUpsBlackList& black_list =
            (MERGE_SERVER == server_type) ? black_list_ : ups_black_list_for_merge_;
          // bring back to alive no need write lock
          if (black_list.get_valid_count() <= 0)
          {
            YYSYS_LOG(WARN, "check all the update server not invalid:count[%d]",
                      black_list.get_valid_count());
            black_list.reset();
          }
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = OB_ENTRY_NOT_EXIST;
        ObServer update_server;
        for (int32_t i = cur_index; retry_times < max_count; ++i, ++retry_times)
        {
          //LOCK BLOCK
          {
            yysys::CRLockGuard lock(ups_list_lock_);
            int32_t server_count = update_server_list_.ups_count_;
            if (false == check_server(i % server_count, server_type))
            {
              YYSYS_LOG(WARN, "check update server failed:index[%d]", i%server_count);
              continue;
            }
            update_server = update_server_list_.ups_array_[i%server_count].get_server(server_type);
          }
          YYSYS_LOG(DEBUG, "select slave update server for get:index[%d], ip[%d], port[%d]",
              i, update_server.get_ipv4(), update_server.get_port());
          ret = rpc_stub->get((time_out > 0) ? time_out : rpc_timeout_, update_server, get_param, scanner);
          if (OB_INVALID_START_VERSION == ret)
          {
            OB_STAT_INC(CHUNKSERVER, FAIL_CS_VERSION_COUNT);
            YYSYS_LOG(WARN, "check chunk server data version failed:ret[%d]", ret);
          }
          else if (ret != OB_SUCCESS)
          {
            // inc update server fail counter for blacklist
            //LOCK BLOCK
            {
              yysys::CRLockGuard lock(ups_list_lock_);
              int32_t server_count = update_server_list_.ups_count_;
              ObUpsBlackList& black_list =
                (MERGE_SERVER == server_type) ? black_list_ : ups_black_list_for_merge_;
              black_list.fail(i%server_count, update_server);
              YYSYS_LOG(WARN, "get from update server failed:ip[%d], port[%d], ret[%d]",
                  update_server.get_ipv4(), update_server.get_port(), ret);
            }
          }
          else
          {
            break;
          }
        }
      }
      return ret;
    }

    //
    int ObMergerRpcProxy::ups_get(const ObGetParam & get_param,
      ObScanner & scanner, const ObServerType server_type, const int64_t time_out)
    {
      return ups_get_(rpc_stub_, get_param, scanner, server_type, time_out);
    }

    template<class T, class RpcT>
    int ObMergerRpcProxy::ups_get_(RpcT *rpc_stub, const ObGetParam & get_param,
      T & scanner, const ObServerType server_type, const int64_t time_out)
    {
      int ret = OB_SUCCESS;
      int64_t start_time = yysys::CTimeUtil::getTime();
      if (!check_inner_stat())
      {
        YYSYS_LOG(ERROR, "%s", "check inner stat failed");
        ret = OB_INNER_STAT_ERROR;
      }
      else if (NULL == get_param[0])
      {
        YYSYS_LOG(ERROR, "check first cell failed:cell[%p]", get_param[0]);
        ret = OB_INPUT_PARAM_ERROR;
      }
      else if (true == get_param.get_is_read_consistency())
      {
        ret = master_ups_get(rpc_stub, get_param, scanner, time_out);
        if (ret != OB_SUCCESS)
        {
          YYSYS_LOG(ERROR, "get from master ups failed:ret[%d]", ret);
        }
      }
      else
      {
        ret = slave_ups_get(rpc_stub, get_param, scanner, server_type, time_out);
        if (ret != OB_SUCCESS)
        {
          YYSYS_LOG(ERROR, "get from slave ups failed:ret[%d]", ret);
        }
      }

      if ((OB_SUCCESS == ret) && (get_param.get_cell_size() > 0) && scanner.is_empty())
      {
        YYSYS_LOG(WARN, "update server error, response request with zero cell");
        ret = OB_ERR_UNEXPECTED;
      }
      OB_STAT_INC(CHUNKSERVER, INC_QUERY_GET_COUNT);
      OB_STAT_INC(CHUNKSERVER, INC_QUERY_GET_TIME, yysys::CTimeUtil::getTime() - start_time);

      return ret;
    }

    template<class T, class RpcT>
    int ObMergerRpcProxy::master_ups_scan(RpcT *rpc_stub, const ObScanParam & scan_param, T & scanner,
        const int64_t time_out)
    {
      int ret = OB_ERROR;
      ObServer update_server;
      int64_t end_time = rpc_timeout_ + yysys::CTimeUtil::getTime();
      for (int64_t i = 0; yysys::CTimeUtil::getTime() < end_time; ++i)
      {
        bool need_renew = ((OB_NOT_MASTER == ret) || (OB_RESPONSE_TIME_OUT == ret));
        ret = get_master_master_update_server(need_renew, update_server);
        if (ret != OB_SUCCESS)
        {
          YYSYS_LOG(WARN, "get master update server failed:ret[%d]", ret);
          break;
        }
        ret = rpc_stub->scan((time_out > 0) ? time_out : rpc_timeout_, update_server, scan_param, scanner);
        if (false == check_need_retry_ups(ret))
        {
          break;
        }
        else
        {
          YYSYS_LOG(WARN, "ups scan fail. retry. ret=%d, i=%ld, rpc_timeout_=%ld ups[%s]", ret, i, rpc_timeout_, to_cstring(update_server));
          usleep(static_cast<useconds_t>(RETRY_INTERVAL_TIME * (i + 1)));
        }
      }
      if (OB_INVALID_START_VERSION == ret)
      {
        OB_STAT_INC(CHUNKSERVER, FAIL_CS_VERSION_COUNT);
      }
      return ret;
    }
    //add zhaoqiong [Schema Manager] 20150327:b
    int ObMergerRpcProxy::set_schema_service(common::ObSchemaServiceImpl* schema_service)
    {
      int ret = OB_SUCCESS;
      if (NULL == schema_service)
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(WARN, "schema service should not be null");
      }
      else
      {
        schema_service_ = schema_service;
      }
      return ret;
    }
    //add:e

    int ObMergerRpcProxy::set_rpc_param(const int64_t retry_times, const int64_t timeout)
    {
      int ret = OB_SUCCESS;
      if ((retry_times < 0) || (timeout <= 0))
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(WARN, "check rpc timeout param failed:retry_times[%ld], timeout[%ld]",
          retry_times, timeout);
      }
      else
      {
        rpc_retry_times_ = retry_times;
        rpc_timeout_ = timeout;
      }
      return ret;
    }

    int ObMergerRpcProxy::set_blacklist_param(
        const int64_t timeout, const int64_t fail_count)
    {
      int ret = OB_SUCCESS;
      if ((timeout <= 0) || (fail_count <= 0))
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(WARN, "check blacklist param failed:timeout[%ld], threshold[%ld]",
          timeout, fail_count);
      }
      else
      {
        fail_count_threshold_ = fail_count;
        black_list_timeout_ = timeout;
      }
      return ret;
    }

    bool ObMergerRpcProxy::check_server(const int32_t index, const ObServerType server_type)
    {
      bool ret = true;
      ObUpsBlackList& black_list =
        (MERGE_SERVER == server_type) ? black_list_ : ups_black_list_for_merge_;

      // check the read percentage and not in black list, if in list timeout make it to be alive
      if ((false == black_list.check(index))
        || (update_server_list_.ups_array_[index].get_read_percentage(server_type) <= 0))
      {
        ret = false;
      }

      return ret;
    }

    template<class T, class RpcT>
    int ObMergerRpcProxy::slave_ups_scan(RpcT *rpc_stub, const ObScanParam & scan_param,
      T & scanner, const ObServerType server_type, const int64_t time_out)
    {
      int ret = OB_SUCCESS;
      int32_t cur_index = -1;
      int32_t max_count = 0;
      int32_t retry_times = 0;
      int32_t server_count = update_server_list_.ups_count_;

      if (0 == server_count)
      {
        YYSYS_LOG(INFO, "no ups right now local, updating...");
        if (OB_SUCCESS != (ret = fetch_update_server_list(server_count)))
        {
          YYSYS_LOG(WARN, "fetch update server list fail, ret: [%d]", ret);
        }
        else if (0 == server_count)
        {
          ret = OB_UPS_NOT_EXIST;
          YYSYS_LOG(WARN, "no update server available right now, ret: [%d]", ret);
        }
        else
        {
          YYSYS_LOG(INFO, "update local ups list"
                    " info successfully! Got [%d] ups.", server_count);
        }
      }
      if (OB_SUCCESS == ret)
      {
        yysys::CRLockGuard lock(ups_list_lock_);
        max_count = server_count;
        cur_index = ObReadUpsBalance::select_server(update_server_list_, server_type);
        if (cur_index < 0)
        {
          YYSYS_LOG(WARN, "select server failed:count[%d], index[%d]", server_count, cur_index);
          ret = OB_ENTRY_NOT_EXIST;
        }
        else
        {
          ObUpsBlackList& black_list =
            (MERGE_SERVER == server_type) ? black_list_ : ups_black_list_for_merge_;
          // bring back to alive no need write lock
          if (black_list.get_valid_count() <= 0)
          {
            YYSYS_LOG(WARN, "check all the update server not invalid:count[%d]",
                      black_list.get_valid_count());
            black_list.reset();
          }
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = OB_ENTRY_NOT_EXIST;
        ObServer update_server;
        for (int32_t i = cur_index; retry_times < max_count; ++i, ++retry_times)
        {
          //LOCK BLOCK
          {
            yysys::CRLockGuard lock(ups_list_lock_);
            int32_t server_count = update_server_list_.ups_count_;
            if (false == check_server(i % server_count, server_type))
            {
              YYSYS_LOG(WARN, "check update server failed:index[%d]", i%server_count);
              continue;
            }
            update_server = update_server_list_.ups_array_[i%server_count].get_server(server_type);
          }
          YYSYS_LOG(DEBUG, "select slave update server for scan:index[%d], ip[%d], port[%d]",
              i, update_server.get_ipv4(), update_server.get_port());
          ret = rpc_stub->scan((time_out > 0) ? time_out : rpc_timeout_, update_server, scan_param, scanner);
          if (OB_INVALID_START_VERSION == ret)
          {
            OB_STAT_INC(CHUNKSERVER, FAIL_CS_VERSION_COUNT);
            YYSYS_LOG(WARN, "check chunk server data version failed:ret[%d]", ret);
          }
          else if (ret != OB_SUCCESS)
          {
            // inc update server fail counter for blacklist
            //LOCK BLOCK
            {
              yysys::CRLockGuard lock(ups_list_lock_);
              int32_t server_count = update_server_list_.ups_count_;
              ObUpsBlackList& black_list =
                (MERGE_SERVER == server_type) ? black_list_ : ups_black_list_for_merge_;
              black_list.fail(i%server_count, update_server);
              YYSYS_LOG(WARN, "get from update server failed:ups_ip[%s], port[%d], ret[%d]",
                  to_cstring(update_server), update_server.get_port(), ret);
            }
          }
          else
          {
            YYSYS_LOG(DEBUG, "%s", "ups get data succ");
            break;
          }
        }
      }
      return ret;
    }

    //add shili [MultiUPS] [MERGE] 20150701:b
    int ObMergerRpcProxy::ups_scan(const ObScanParam &scan_param, const int64_t paxos_id,
                                   ObScanner &scanner, const ObServerType server_type, const int64_t time_out)
    {
      return ups_scan_by_paxos_id(rpc_stub_, paxos_id, scan_param, scanner, server_type, time_out);
    }
    //add  20150701:e
    //add wenghaixing [secondary index static_index_build.cs_scan]20150324
    int ObMergerRpcProxy::cs_cs_scan(const ObScanParam &scan_param, ObServer chunkserver, ObScanner &scanner, const ObServerType server_type, const int64_t time_out)
    {
      return cs_scan_(rpc_stub_, scan_param, chunkserver, scanner, server_type, time_out);
    }
    //add shili [MultiUPS] [MERGE] 20150701:b
    template<class T, class RpcT>
    int ObMergerRpcProxy::ups_scan_by_paxos_id(RpcT *rpc_stub,
        const int64_t paxos_id,
        const common::ObScanParam &scan_param,
        T &scanner,
        const common::ObServerType server_type/* = common::MERGE_SERVER*/,
        const int64_t time_out /*= 0*/)
    {
      int ret = OB_SUCCESS;
      int64_t start_time = yysys::CTimeUtil::getTime();
      if(!check_inner_stat())
      {
        ret = OB_INNER_STAT_ERROR;
        YYSYS_LOG(ERROR, "check inner stat failed,ret=%d",ret);
      }
      else if(!check_scan_param(scan_param))
      {
        ret = OB_INPUT_PARAM_ERROR;
        YYSYS_LOG(ERROR, "check scan param failed,ret=%d",ret);
      }
      else
      {
        ret = ups_scan_data(rpc_stub, paxos_id,scan_param.get_is_read_consistency(),
                            scan_param, scanner, time_out, server_type);
        if(ret != OB_SUCCESS)
        {
          YYSYS_LOG(ERROR, "scan from master ups failed:ret[%d]", ret);
        }
      }
      OB_STAT_INC(CHUNKSERVER, INC_QUERY_SCAN_COUNT);
      OB_STAT_INC(CHUNKSERVER, INC_QUERY_SCAN_TIME, yysys::CTimeUtil::getTime() - start_time);
      return ret;
    }
    //add 20150701:e
	//add shili [MultiUPS] [MERGE] 20150701:b
    template<class T, class RpcT>
    int ObMergerRpcProxy::ups_scan_data(RpcT *rpc_stub,
                                        const int64_t paxos_id,
                                        const bool is_strong_consistency,
                                        const common::ObScanParam &scan_param,
                                        T &scanner,
                                        const int64_t time_out/* = 0*/,
                                        const common::ObServerType server_type/* = common::MERGE_SERVER*/
                                       )
    {
      int ret = OB_SUCCESS;
      ObServer update_server;
      int64_t end_time = rpc_timeout_ + yysys::CTimeUtil::getTime();
      for(int64_t i = 0; yysys::CTimeUtil::getTime() < end_time; ++i)
      {
        ret = get_update_server(paxos_id, is_strong_consistency, update_server, server_type);
        if(ret != OB_SUCCESS)
        {
          YYSYS_LOG(WARN, "get master update server failed:ret[%d]", ret);
          break;
        }
        YYSYS_LOG(DEBUG, "select update server for scan data:paxos_id[%ld], ups[%s]",
                         paxos_id, update_server.to_cstring());
        ret = rpc_stub->scan((time_out > 0) ? time_out : rpc_timeout_, update_server, scan_param, scanner);
        if(false == check_need_retry_ups(ret))
        {
          break;
        }
        else
        {
          YYSYS_LOG(WARN, "ups scan fail. retry. ret=%d, i=%ld, rpc_timeout_=%ld ups[%s]", ret, i, rpc_timeout_, to_cstring(update_server));
          usleep(static_cast<useconds_t>(RETRY_INTERVAL_TIME * (i + 1)));
        }
      }
      if(OB_INVALID_START_VERSION == ret)
      {
        OB_STAT_INC(CHUNKSERVER, FAIL_CS_VERSION_COUNT);
      }
      return ret;
    }
    //add 20150701:e
    template<class T, class RpcT>
    int ObMergerRpcProxy::cs_scan_(RpcT *rpc_stub, const ObScanParam & scan_param, const ObServer chunkserver,
      T & scanner, const ObServerType server_type, const int64_t time_out )
    {
      int ret = OB_SUCCESS;
      UNUSED(server_type);
      int64_t start_time = yysys::CTimeUtil::getTime();
      if (!check_inner_stat())
      {
        YYSYS_LOG(ERROR, "%s", "check inner stat failed");
        ret = OB_INNER_STAT_ERROR;
      }
      else if (!check_scan_param(scan_param))
      {
        YYSYS_LOG(ERROR, "%s", "check scan param failed");
        ret = OB_INPUT_PARAM_ERROR;
      }
      else
      {
        ret = cs_cs_scan_(rpc_stub, scan_param, chunkserver, scanner, time_out);
        if (ret != OB_SUCCESS)
        {
          YYSYS_LOG(ERROR, "scan from chunkserver failed:ret[%d]", ret);
        }
      }
      OB_STAT_INC(CHUNKSERVER, INC_QUERY_SCAN_COUNT);
      OB_STAT_INC(CHUNKSERVER, INC_QUERY_SCAN_TIME, yysys::CTimeUtil::getTime() - start_time);

      return ret;
    }
	

    template<class T, class RpcT>
    int ObMergerRpcProxy::cs_cs_scan_(RpcT *rpc_stub, const ObScanParam & scan_param, const ObServer chunkserver, T & scanner,
        const int64_t time_out)
    {
      int ret = OB_SUCCESS;
      int64_t end_time = rpc_timeout_ + yysys::CTimeUtil::getTime();
      for (int64_t i = 0; yysys::CTimeUtil::getTime() < end_time; ++i)
      {
        if (ret != OB_SUCCESS)
        {
          YYSYS_LOG(WARN, "get master update server failed:ret[%d]", ret);
          break;
        }
        ret = rpc_stub->scan((time_out > 0) ? time_out : rpc_timeout_, chunkserver, scan_param, scanner);
        //add wenghaixing [secondary index static_index_build.cs_scan]20150330
        if(scan_param.if_need_fake())
        {
          //YYSYS_LOG(INFO, "test::whx scan_param----fake_range[%s]", to_cstring(*scan_param.get_fake_range()));
        }
        //add e
        if (false == check_need_retry_cs(ret))
        {
          break;
        }
        else
        {
          YYSYS_LOG(WARN, "cs to cs scan fail. retry. ret=%d, i=%ld, rpc_timeout_=%ld ups[%s]", ret, i, rpc_timeout_, to_cstring(chunkserver));
          usleep(static_cast<useconds_t>(RETRY_INTERVAL_TIME * (i + 1)));
        }
      }
      if (OB_INVALID_START_VERSION == ret)
      {
        OB_STAT_INC(CHUNKSERVER, FAIL_CS_VERSION_COUNT);
      }
      return ret;
    }
    //add e
    template<class T, class RpcT>
    int ObMergerRpcProxy::ups_scan_(RpcT *rpc_stub, const ObScanParam & scan_param,
      T & scanner, const ObServerType server_type, const int64_t time_out )
    {
      int ret = OB_SUCCESS;
      int64_t start_time = yysys::CTimeUtil::getTime();
      if (!check_inner_stat())
      {
        YYSYS_LOG(ERROR, "%s", "check inner stat failed");
        ret = OB_INNER_STAT_ERROR;
      }
      else if (!check_scan_param(scan_param))
      {
        YYSYS_LOG(ERROR, "%s", "check scan param failed");
        ret = OB_INPUT_PARAM_ERROR;
      }
      else if (true == scan_param.get_is_read_consistency())
      {
        ret = master_ups_scan(rpc_stub, scan_param, scanner, time_out);
        if (ret != OB_SUCCESS)
        {
          YYSYS_LOG(ERROR, "scan from master ups failed:ret[%d]", ret);
        }
      }
      else
      {
        ret = slave_ups_scan(rpc_stub, scan_param, scanner, server_type, time_out);
        if (ret != OB_SUCCESS)
        {
          YYSYS_LOG(ERROR, "scan from slave ups failed:ret[%d]", ret);
        }
      }
      OB_STAT_INC(CHUNKSERVER, INC_QUERY_SCAN_COUNT);
      OB_STAT_INC(CHUNKSERVER, INC_QUERY_SCAN_TIME, yysys::CTimeUtil::getTime() - start_time);

      return ret;
    }

    int ObMergerRpcProxy::sql_ups_get(const common::ObGetParam & get_param,
                    common::ObNewScanner & scanner,
                    const int64_t timeout /* = 0 */)
    {
      return ups_get_(sql_rpc_stub_, get_param, scanner, common::MERGE_SERVER, timeout);
    }

    //add lijianqiang [MultiUPS] [SELECT_MERGE] 20160104:b
    int ObMergerRpcProxy::sql_ups_get(const ObGetParam & get_param,
                                      ObNewScanner & new_scanner,
                                      const int64_t time_out,
                                      const int64_t paxos_id)
    {
      int ret = OB_SUCCESS;
      ObServer update_server;
      int64_t start_time = yysys::CTimeUtil::getTime();
      bool get_is_read_consistency = false;
      if(!check_inner_stat())
      {
        YYSYS_LOG(ERROR, "%s", "check inner stat failed");
        ret = OB_INNER_STAT_ERROR;
      }
      else if(NULL == get_param[0])
      {
        YYSYS_LOG(ERROR, "check first cell failed:cell[%p]", get_param[0]);
        ret = OB_INPUT_PARAM_ERROR;
      }
      else
      {
        get_is_read_consistency = get_param.get_is_read_consistency();
        if(OB_SUCCESS!= (ret = get_update_server(paxos_id, get_is_read_consistency, update_server)))
        {
          YYSYS_LOG(WARN, "get %s update server failed:ret[%d]", get_is_read_consistency? "master":"slaver", ret);
        }
        YYSYS_LOG(DEBUG, " select update server for get:paxos_id[%ld], ups[%s]",
                         paxos_id, update_server.to_cstring());
        if (OB_SUCCESS == ret)
        {
          int64_t end_time = time_out + yysys::CTimeUtil::getTime();
          for(int64_t i = 0; yysys::CTimeUtil::getTime() < end_time; ++i)
          {
            ret = sql_rpc_stub_->get((time_out > 0) ? time_out : rpc_timeout_, update_server, get_param, new_scanner);
            if(false == check_need_retry_ups(ret))
            {
              break;
            }
            else
            {
              YYSYS_LOG(WARN, "ups get fail. retry. ret=%d, i=%ld, rpc_timeout_=%ld.", ret, i, rpc_timeout_);
              usleep(static_cast<useconds_t>(RETRY_INTERVAL_TIME * (i + 1)));
            }
          }
          if(OB_INVALID_START_VERSION == ret)
          {
            OB_STAT_INC(CHUNKSERVER, FAIL_CS_VERSION_COUNT);
          }
        }
      }
      OB_STAT_INC(CHUNKSERVER, INC_QUERY_SCAN_COUNT);
      OB_STAT_INC(CHUNKSERVER, INC_QUERY_GET_TIME, yysys::CTimeUtil::getTime() - start_time);
      return ret;
    }


    int ObMergerRpcProxy::sql_ups_scan(const ObScanParam & scan_param,
                             ObNewScanner & new_scanner,
                             const int64_t time_out,
                             const int64_t paxos_id)
    {
      int ret = OB_SUCCESS;
      ObServer update_server;
      bool get_is_read_consistency = false;
      int64_t start_time = yysys::CTimeUtil::getTime();
      if(!check_inner_stat())
      {
        YYSYS_LOG(ERROR, "%s", "check inner stat failed");
        ret = OB_INNER_STAT_ERROR;
      }
      else if(!check_scan_param(scan_param))
      {
        YYSYS_LOG(ERROR, "%s", "check scan param failed");
        ret = OB_INPUT_PARAM_ERROR;
      }
      else
      {
        get_is_read_consistency = scan_param.get_is_read_consistency();
        if(OB_SUCCESS!= (ret = get_update_server(paxos_id, get_is_read_consistency, update_server)))
        {
          YYSYS_LOG(WARN, "get %s update server failed:ret[%d]", get_is_read_consistency? "master":"slaver", ret);
        }
        YYSYS_LOG(DEBUG, "pang: select update server for scan:paxos_id[%ld], ups[%s]",
                         paxos_id, update_server.to_cstring());
        if (OB_SUCCESS == ret)
        {
          int64_t end_time = rpc_timeout_ + yysys::CTimeUtil::getTime();
          for(int64_t i = 0; yysys::CTimeUtil::getTime() < end_time; ++i)
          {
            ret = sql_rpc_stub_->scan((time_out > 0) ? time_out : rpc_timeout_, update_server, scan_param, new_scanner);
            if(false == check_need_retry_ups(ret))
            {
              break;
            }
            else
            {
              YYSYS_LOG(WARN, "ups scan fail. retry. ret=%d, i=%ld, rpc_timeout_=%ld ups[%s]", ret, i, rpc_timeout_, to_cstring(update_server));
              usleep(static_cast<useconds_t>(RETRY_INTERVAL_TIME * (i + 1)));
            }
          }
          if(OB_INVALID_START_VERSION == ret)
          {
            OB_STAT_INC(CHUNKSERVER, FAIL_CS_VERSION_COUNT);
          }
        }
      }
      OB_STAT_INC(CHUNKSERVER, INC_QUERY_SCAN_COUNT);
      OB_STAT_INC(CHUNKSERVER, INC_QUERY_SCAN_TIME, yysys::CTimeUtil::getTime() - start_time);
      return ret;
    }
    //add 20160104:e



    int ObMergerRpcProxy::sql_ups_scan(const common::ObScanParam & scan_param,
                     common::ObNewScanner & scanner,
                     const int64_t timeout /* = 0 */)
    {
      return ups_scan_(sql_rpc_stub_, scan_param, scanner, common::MERGE_SERVER, timeout);
    }

  } // end namespace chunkserver
} // end namespace oceanbase
