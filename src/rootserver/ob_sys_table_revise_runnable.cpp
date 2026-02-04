/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sys_table_revise_runnable.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_sys_table_revise_runnable.h"
#include <sys/types.h>

using namespace oceanbase::rootserver;
using namespace oceanbase::common;

ObSysTableReviseRunnable::ObSysTableReviseRunnable(ObUpsManager& ups_manager, ObChunkServerManager &server_manager,
                                                   ObRootServer2 *root_server, ObRootWorker *root_work, const int64_t &sys_table_revise_interval)
    :ups_manager_(ups_manager), server_manager_(server_manager), root_server_(root_server), root_work_(root_work), check_time_interval_(sys_table_revise_interval)
{
}

ObSysTableReviseRunnable::~ObSysTableReviseRunnable()
{
}

void ObSysTableReviseRunnable::run(yysys::CThread* thread, void* arg)
{
    UNUSED(thread);
    UNUSED(arg);
    YYSYS_LOG(INFO, "[NOTICE] system table revise thread start, tid=%ld", syscall(__NR_gettid));
    if (NULL == root_server_)
    {
        YYSYS_LOG(ERROR, "root_server_ is null");
    }
    SET_THD_NAME_ONCE("rs-systab");
    while (!_stop)
    {
        if (root_server_->is_master())
        {
            bool is_permit_delete = true;
            int ret = OB_SUCCESS;
            //1 sync __all_server
            //1.1 rs force replace
            ObSEArray<ObRootAsyncTaskQueue::ObSeqTask, OB_MAX_RS_COUNT> rs_task_arr;
            root_server_->get_task_array_for_inner_table_revise(rs_task_arr);
            for (int64_t idx = 0; idx < rs_task_arr.count(); ++idx)
            {
               ret = modify_all_server_table(rs_task_arr.at(idx));
               if (OB_SUCCESS != ret)
               {
                  is_permit_delete = false;
               }
            }

            //1.2 ups force replace
            ObSEArray<ObRootAsyncTaskQueue::ObSeqTask, OB_MAX_UPS_COUNT> ups_task_arr;
            ups_manager_.get_task_array_for_inner_table_revise(ups_task_arr);
            for (int64_t idx = 0; idx < ups_task_arr.count(); ++idx)
            {
               ret = modify_all_server_table(ups_task_arr.at(idx));
               if (OB_SUCCESS != ret)
               {
                  is_permit_delete = false;
               }
            }

            //1.3 ms/cs fore replace
            ObSEArray<ObRootAsyncTaskQueue::ObSeqTask, ObChunkServerManager::MAX_SERVER_COUNT>  task_arr;
            server_manager_.get_task_array_for_inner_table_revise(task_arr);
            for (int64_t idx = 0; idx < task_arr.count(); ++idx)
            {
               ret = modify_all_server_table(task_arr.at(idx));
               if (OB_SUCCESS != ret)
               {
                  is_permit_delete = false;
               }
            }
            //1.last
            if (is_permit_delete)
            {
              ObRootAsyncTaskQueue::ObSeqTask task;
              task.type_ = SERVER_OFFLINE;
              //modify_all_server_table(task);
            }
            else
            {
              YYSYS_LOG(WARN, "this loop abandon delete");
            }

            //2 sync __all_cluster
            for (int32_t cluster_id = 0; cluster_id <= OB_MAX_CLUSTER_ID; ++cluster_id)
            {
                YYSYS_LOG(DEBUG, "check and revise cluster[%d]", cluster_id);
                check_cluster_alive(cluster_id);
            }
        }
        YYSYS_LOG(INFO, "system table revise running, rs_role[%d]", root_server_->get_rs_role());
        const int32_t sys_table_revise_interval = (int32_t)(check_time_interval_/1000000);
        //usleep(static_cast<useconds_t>(check_time_interval_));
        sleep(sys_table_revise_interval);
    }
    YYSYS_LOG(INFO, "[NOTICE] system table revise thread exit");
}

int ObSysTableReviseRunnable::modify_all_server_table(const ObRootAsyncTaskQueue::ObSeqTask & task)
{
    int ret = OB_SUCCESS;
    // write server info to internal table
    char buf[OB_MAX_SQL_LENGTH] = "";
    int32_t server_port = task.server_.get_port();
    char ip_buf[OB_MAX_SERVER_ADDR_SIZE] = "";
    if (false == task.server_.ip_to_string(ip_buf, sizeof(ip_buf)))
    {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(WARN, "convert server ip to string failed:ret[%d]", ret);
    }
    else
    {
        switch (task.type_)
        {
        case SERVER_ONLINE:
        {
            const char * sql_temp = "REPLACE INTO __all_server(cluster_id, paxos_id, svr_type,"
                    " svr_ip, svr_port, inner_port, svr_role)"
                    " VALUES(%d,%ld,\'%s\',\'%s\',%u,%u,%d);";
            snprintf(buf, sizeof (buf), sql_temp, task.server_.cluster_id_, task.paxos_id_, print_role(task.role_),
                     ip_buf, server_port, task.inner_port_, task.server_status_);
            break;
        }
        case SERVER_OFFLINE:
        {
            int64_t cur_time = yysys::CTimeUtil::getTime();
            cur_time -= DELETE_SERVER_TIMES_AGO;// 900s
            const char * sql_temp = "DELETE /*+UD_MULTI_BATCH*/ FROM __all_server where gm_modify < cast(%ld as modifytime);";
            snprintf(buf, sizeof (buf), sql_temp, cur_time);
            break;
        }
        default:
        {
            ret = OB_INVALID_ARGUMENT;
            YYSYS_LOG(WARN, "check input param failed:task_type[%d]", task.type_);
        }
        }
    }
    if (OB_SUCCESS == ret)
    {
        ObString sql;
        sql.assign_ptr(buf, static_cast<ObString::obstr_size_t>(strlen(buf)));
        ret = root_work_->get_sql_proxy().query(true, RETRY_TIMES, TIMEOUT, sql);
        if (OB_SUCCESS != ret)
        {
            YYSYS_LOG(WARN, "revise __all_server inner table failed: sql[%s]", buf);
        }
    }
    return ret;
}

int ObSysTableReviseRunnable::check_cluster_alive(const int32_t cluster_id)
{
  int ret = OB_SUCCESS;
  yysys::CWLockGuard guard(*root_server_->get_sys_table_revise_lock());
  {
    bool is_alive = root_server_->is_cluster_alive(cluster_id);
    if(!is_alive)
    {
      if(OB_SUCCESS != (ret = process_offline_cluster(cluster_id)))
      {
        YYSYS_LOG(WARN,"process offline cluster faild! ret %d", ret);
      }
    }
    //add bingo [Paxos datasource __all_cluster] 20170407:b
    else if((!root_server_->get_server_manager().is_cluster_alive_with_ms(cluster_id)) ||
            (!root_server_->get_server_manager().is_cluster_alive_with_cs(cluster_id)) ) //all ms offline or all cs offline
    {
      //add bingo [Paxos __all_cluster] 20170713:b
      if(OB_SUCCESS != (ret = process_cluster_while_all_mscs_offline(cluster_id)))
      {
        YYSYS_LOG(WARN,"process cluster while all mscs offline faild! ret %d", ret);
      }
      //add:e
    }
    //add:e
  }
  return ret;
}

//add bingo [Paxos __all_cluster] 20170713:b
int ObSysTableReviseRunnable::process_offline_cluster(int32_t cluster_id)
{
  int ret = OB_SUCCESS;
  int32_t master_cluster_id = root_server_->get_master_cluster_id();
  if(master_cluster_id < 0)
  {
    ret = OB_CLUSTER_ID_ERROR;
    YYSYS_LOG(WARN,"invalid master cluster id %d", master_cluster_id);
  }
  if(cluster_id < 0)
  {
    ret = OB_CLUSTER_ID_ERROR;
    YYSYS_LOG(WARN,"invalid cluster id %d", cluster_id);
  }

  if(OB_SUCCESS == ret)
  {
    int32_t cluster_flows[OB_CLUSTER_ARRAY_LEN];
    memset(cluster_flows, 0, OB_CLUSTER_ARRAY_LEN * sizeof(int32_t));
    //add bingo [Paxos datasource __all_cluster] 20170714:b
    int64_t cluster_ports[OB_CLUSTER_ARRAY_LEN];
    memset(cluster_ports, 0, OB_CLUSTER_ARRAY_LEN * sizeof(int64_t));
    //add:e

    ObServer ms;
    bool query_master = false;
    if (OB_SUCCESS != (ret = root_work_->get_sql_proxy().get_ms_provider().get_ms(ms,query_master)))
    {
      YYSYS_LOG(WARN, "get mergeserver address failed, ret %d", ret);
    }
    else if (OB_SUCCESS != (ret = root_work_->get_sql_proxy().get_rpc_stub().fetch_cluster_flow_list(ms, cluster_flows, root_server_->get_config().network_timeout)))
    {
      YYSYS_LOG(WARN, "fetch master cluster flow list failed, ret %d, ms %s", ret, to_cstring(ms));
    }
    //add bingo [Paxos datasource __all_cluster] 20170714:b
    else if (OB_SUCCESS != (ret = root_work_->get_sql_proxy().get_rpc_stub().fetch_cluster_port_list(ms, cluster_ports, root_server_->get_config().network_timeout)))
    {
      YYSYS_LOG(WARN, "fetch cluster port list failed, ret %d, ms %s", ret, to_cstring(ms));
    }
    //add:e
    else if(0 != cluster_flows[cluster_id] || master_cluster_id == cluster_id)
    {
      int32_t new_flow = cluster_flows[cluster_id];
      int32_t m_cid = master_cluster_id;
      if(master_cluster_id == cluster_id)
      {
        //add bingo [Paxos datasource __all_cluster] 20170714:b
        if(cluster_ports[root_server_->get_self().cluster_id_] == 0)
        {//add:e
          for(int32_t cid = 0; cid < OB_CLUSTER_ARRAY_LEN; cid++)
          {
            //add bingo [Paxos datasource __all_cluster] 20170714:b
            if(cid != cluster_id && cluster_ports[cid] != 0)
            {//add:e
              m_cid = cid;
              break;
            }
          }
        }

        if(m_cid == master_cluster_id)
        {
          m_cid = root_server_->get_self().cluster_id_;
        }
        root_server_->set_master_cluster_id(m_cid);
        new_flow += cluster_flows[m_cid];
      }
      else
      {
        new_flow += cluster_flows[master_cluster_id];
      }

      char buf_tmp[OB_MAX_SQL_LENGTH] = "";
      const char * sql_temp = "REPLACE INTO %s"
        "(cluster_id, cluster_role, cluster_flow_percent)"
        "VALUES(%d, %d, %d);";
      snprintf(buf_tmp, sizeof (buf_tmp), sql_temp, OB_ALL_CLUSTER, m_cid, 1, new_flow);
      ObString sql_tmp;
      sql_tmp.assign_ptr(buf_tmp, static_cast<ObString::obstr_size_t>(strlen(buf_tmp)));
      ret = root_work_->get_sql_proxy().query(true, RETRY_TIMES, TIMEOUT, sql_tmp);
      if (OB_SUCCESS == ret)
      {
        YYSYS_LOG(INFO, "update all_cluster sql success! ret: [%d], cluster_id=%d", ret, root_server_->get_master_cluster_id());
      }
      else
      {
        YYSYS_LOG(WARN, "update all_cluster sql failed! ret: [%d], cluster_id=%d", ret, root_server_->get_master_cluster_id());
      }
    }
  }

  return ret;
}

int ObSysTableReviseRunnable::process_cluster_while_all_mscs_offline(int32_t cluster_id)
{
  int ret = OB_SUCCESS;
  int32_t master_cluster_id = root_server_->get_master_cluster_id();
  if(master_cluster_id < 0)
  {
    ret = OB_CLUSTER_ID_ERROR;
    YYSYS_LOG(WARN,"invalid master cluster id %d", master_cluster_id);
  }
  if(cluster_id < 0)
  {
    ret = OB_CLUSTER_ID_ERROR;
    YYSYS_LOG(WARN,"invalid cluster id %d", cluster_id);
  }

  if(OB_SUCCESS == ret)
  {
    //get cluster flow
    int32_t cluster_flows[OB_CLUSTER_ARRAY_LEN];
    memset(cluster_flows, 0, OB_CLUSTER_ARRAY_LEN * sizeof(int32_t));
    //add bingo [Paxos datasource __all_cluster] 20170714:b
    int64_t cluster_ports[OB_CLUSTER_ARRAY_LEN];
    memset(cluster_ports, 0, OB_CLUSTER_ARRAY_LEN * sizeof(int64_t));
    //add:e

    ObServer ms;
    bool query_master = false;
    if (OB_SUCCESS != (ret = root_work_->get_sql_proxy().get_ms_provider().get_ms(ms,query_master)))
    {
      YYSYS_LOG(WARN, "get mergeserver address failed, ret %d", ret);
    }
    else if (OB_SUCCESS != (ret = root_work_->get_sql_proxy().get_rpc_stub().fetch_cluster_flow_list(ms, cluster_flows, root_server_->get_config().network_timeout)))
    {
      YYSYS_LOG(WARN, "fetch master cluster flow list failed, ret %d, ms %s", ret, to_cstring(ms));
    }
    //add bingo [Paxos datasource __all_cluster] 20170714:b
    else if (OB_SUCCESS != (ret = root_work_->get_sql_proxy().get_rpc_stub().fetch_cluster_port_list(ms, cluster_ports, root_server_->get_config().network_timeout)))
    {
      YYSYS_LOG(WARN, "fetch cluster port list failed, ret %d, ms %s", ret, to_cstring(ms));
    }
    //add:e
    else if(0 != cluster_flows[cluster_id] || master_cluster_id == cluster_id)
    {
      //get cluster with ms and cs
      bool is_cluster_alive_with_ms_and_cs[OB_CLUSTER_ARRAY_LEN];
      root_server_->get_alive_cluster_with_ms_and_cs(is_cluster_alive_with_ms_and_cs);

      //check whether needs to redistribute cluster flow
      int32_t max_flow_cluster_id = OB_ALL_CLUSTER_FLAG;
      int32_t max_id = 0;
      for(int32_t cid = 0; cid < OB_CLUSTER_ARRAY_LEN; cid++)
      {
        if(cid != cluster_id)
        {
          //add bingo [Paxos datasource __all_cluster] 20170714:b
          if(cluster_ports[cid] != 0)
          {//add:e
            //mod pangtianze [MultiUPS] [merge with paxos] 20170714:b
            //max_flow_cluster_id = cluster_flows[max_flow_cluster_id]  > cluster_flows[cid] ? max_flow_cluster_id : cid;
            max_id = cluster_flows[max_id]  > cluster_flows[cid] ? max_flow_cluster_id : cid;
            max_flow_cluster_id = max_id;
            //mod:e
          }
        }
      }
      int32_t target_cluster_id = OB_ALL_CLUSTER_FLAG;
      int32_t target_cluster_flow = 100;
      if(max_flow_cluster_id == OB_ALL_CLUSTER_FLAG || !is_cluster_alive_with_ms_and_cs[max_flow_cluster_id])
      {
        for(int32_t cid = 0; cid < OB_CLUSTER_ARRAY_LEN; cid++)
        {
          if(cid != cluster_id)
          {
            //add bingo [Paxos datasource __all_cluster] 20170714:b
            if(cluster_ports[cid] != 0  && is_cluster_alive_with_ms_and_cs[cid])
            {//add:e
              target_cluster_id = cid;
              break;
            }
          }
        }
        if(target_cluster_id == OB_ALL_CLUSTER_FLAG)
        {
          YYSYS_LOG(WARN,"No valid cluster to switch master cluster");
          ret = OB_SUCCESS;
        }
      }
      else
      {
        target_cluster_id = max_flow_cluster_id;
        target_cluster_flow = cluster_flows[max_flow_cluster_id] + cluster_flows[cluster_id];
      }

      //redistribute cluster flow
      if( OB_ALL_CLUSTER_FLAG != target_cluster_id )
      {
        int32_t c_role = 2;
        if(master_cluster_id == cluster_id)
        {
          root_server_->set_master_cluster_id(target_cluster_id);
          c_role = 1;
        }
        else if(master_cluster_id == target_cluster_id)
        {
          c_role = 1;
        }
        //add cluster flow to target cluster
        char buf_tmp[OB_MAX_SQL_LENGTH] = "";
        const char * sql_temp = "REPLACE INTO %s"
          "(cluster_id, cluster_role, cluster_flow_percent)"
          "VALUES(%d, %d, %d);";
        snprintf(buf_tmp, sizeof (buf_tmp), sql_temp, OB_ALL_CLUSTER, target_cluster_id, c_role, target_cluster_flow);
        ObString sql_tmp;
        sql_tmp.assign_ptr(buf_tmp, static_cast<ObString::obstr_size_t>(strlen(buf_tmp)));
        ret = root_work_->get_sql_proxy().query(true, RETRY_TIMES, TIMEOUT, sql_tmp);
        if (OB_SUCCESS == ret)
        {
          YYSYS_LOG(INFO, "add cluster flow to target cluster success! ret:[%d], cluster_id=%d, cluster_flow_percent=%d", ret, target_cluster_id, target_cluster_flow);
        }
        else
        {
          YYSYS_LOG(WARN, "add cluster flow to target cluster failed! ret:[%d], cluster_id=%d, cluster_flow_percent=%d", ret, target_cluster_id, target_cluster_flow);
        }

        //zero the offline(ms/cs) cluster flow
        char buf_tmp1[OB_MAX_SQL_LENGTH] = "";
        const char * sql_temp1 = "REPLACE INTO %s"
          "(cluster_id, cluster_role, cluster_flow_percent)"
          "VALUES(%d, %d, %d);";
        snprintf(buf_tmp1, sizeof (buf_tmp1), sql_temp1, OB_ALL_CLUSTER, cluster_id, 2, 0);
        ObString sql_tmp1;
        sql_tmp1.assign_ptr(buf_tmp1, static_cast<ObString::obstr_size_t>(strlen(buf_tmp1)));
        ret = root_work_->get_sql_proxy().query(true, RETRY_TIMES, TIMEOUT, sql_tmp1);
        if (OB_SUCCESS == ret)
        {
          YYSYS_LOG(INFO, "zero cluster flow of cluster[%d] success! ret:[%d]", cluster_id, ret);
        }
        else
        {
          YYSYS_LOG(WARN, "zero cluster flow of cluster[%d] failed! ret:[%d]", cluster_id, ret);
        }
      }
    }
  }
  return ret;
}
//add:e


