/*
 * Copyright (C) 2007-2012 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Description here
 *
 * Version: $Id$
 *
 * Authors:
 *   zhidong <xielun.szd@taobao.com>
 *     - some work details here
 */

#include "common/ob_define.h"
#include "common/ob_server.h"
#include "ob_root_server2.h"
#include "ob_root_worker.h"
#include "ob_heartbeat_checker.h"

using namespace oceanbase::common;
using namespace oceanbase::rootserver;


ObHeartbeatChecker::ObHeartbeatChecker(ObRootServer2 * root_server):root_server_(root_server)
{
  OB_ASSERT(root_server != NULL);
}

ObHeartbeatChecker::~ObHeartbeatChecker()
{
}

void ObHeartbeatChecker::run(yysys::CThread * thread, void * arg)
{
  UNUSED(thread);
  UNUSED(arg);
  ObServer tmp_server;
  //add huangjianwei [Paxos rs_switch] 20160720:b
  //ObString svr_type;
  //add:e
  bool master = true;
  int64_t now = 0;
  int64_t preview_rotate_time = 0;
  YYSYS_LOG(INFO, "[NOTICE] heart beat checker thread start");
  SET_THD_NAME_ONCE("rs-mscs-heart");
  while (!_stop)
  {
    now = yysys::CTimeUtil::getTime();
    if ((now > preview_rotate_time + 10 *1000 * 1000) &&
        ((now/(1000 * 1000)  - timezone) % (24 * 3600)  == 0))
    {
      preview_rotate_time = now;
      YYSYS_LOG(INFO, "rotateLog");
      YYSYS_LOGGER.rotateLog(NULL, NULL);
    }
    //mod pangtianze [Paxos rs_election] 20161101:b
    //if (root_server_->is_master())
    if (ObRoleMgr::MASTER == root_server_->get_rs_role())
    //mod:e
    {       
      now = yysys::CTimeUtil::getTime();
      ObChunkServerManager::iterator it = root_server_->server_manager_.begin();
      for (; it != root_server_->server_manager_.end(); ++it)
      {
        if (it->status_ != ObServerStatus::STATUS_DEAD)
        {
          if (it->is_alive(now, root_server_->config_.cs_lease_duration_time))
          {
            if (now - it->last_hb_time_ >= (root_server_->config_.cs_lease_duration_time / 2
                  + it->hb_retry_times_ * HB_RETRY_FACTOR))
            {
              it->hb_retry_times_ ++;
              tmp_server = it->server_;
              tmp_server.set_port(it->port_cs_);
              //mod liu jun. [MultiUPS] [PARTITION_LOCK_FUCTION] 20150604:b
			   
			   if (root_server_->worker_->get_rpc_stub()     // uncertainty  ????withindex 
             /*del liumz, [MultiUPS] [merge index code]20170309
              //modify wenghaixing [secondary index static_index_build.heartbeat]20150528
              .heartbeat_to_cs(tmp_server,
                    root_server_->config_.cs_lease_duration_time,
                    root_server_->get_frozen_version_for_cs_heartbeat(),
                    root_server_->get_schema_version(),
                    root_server_->get_partition_lock_flag(),
                    root_server_->get_config_version()
					//add pangtianze [Paxos rs_election] 20150708:b
					,servers, server_count
					//add:e
          ) != OB_SUCCESS) */
                  .heartbeat_to_cs_with_index(tmp_server,

                                      root_server_->config_.cs_lease_duration_time,
                                      root_server_->get_frozen_version_for_cs_heartbeat(),
                                      root_server_->get_schema_version(),
                    root_server_->get_config_version(),
                    //add liumz, [MultiUPS] [merge index code]20170309
                    root_server_->get_partition_lock_flag(),
                    //add:e
									  root_server_->get_index_beat(),
                                      root_server_->get_build_index_version(),
                                      root_server_->is_last_finished()) != OB_SUCCESS)
                                        //modify e
          //uncertainty ???ups??????
              //mod:e
              {
                YYSYS_LOG(WARN, "heartbeat to cs fail, cs:[%s]", to_cstring(tmp_server));
                //do nothing
              }
              //add pangtianze [Paxos rs_election] 20170321:b
              //del pangtianze [Paxos] 20170321
//              else
//              {
//                  ObServer rs_servers[OB_MAX_RS_COUNT];
//                  int32_t rs_server_count = 0;
//                  root_server_->get_rs_node_mgr()->get_all_alive_servers(rs_servers, rs_server_count);
//                  int ret2 = root_server_->worker_->get_rpc_stub()
//                          .send_rs_list_to_server(tmp_server, rs_servers, rs_server_count);
//                  if (OB_SUCCESS != ret2)
//                  {
//                      YYSYS_LOG(WARN, "refresh rs list to chunk server [%s] failed, err=%d", tmp_server.to_cstring(), ret2);
//                  }
//              }
              //add:e
            }
          }
          else
          {
            ObServer cs = it->server_;
            cs.set_port(it->port_cs_);
            YYSYS_LOG(ERROR,"chunkserver[%s] is down, now:%ld, lease_duration:%s",
                to_cstring(cs), now, root_server_->config_.cs_lease_duration_time.str());
            //add huangjianwei [Paxos rs_switch] 20160720:b
            /*svr_type = ObString::make_string("chunkserver");
            char cs_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
            //del pangtianze [Paxos bugfix] 20170421:b
            if (cs.ip_to_string(cs_ip,sizeof(cs_ip)) != true)
            {
              YYSYS_LOG(ERROR,"chunkserver ip convert failed");
            }

            else if(OB_SUCCESS != root_server_->schema_service_->delete_server_status(svr_type,ObString::make_string(cs_ip),it->port_cs_))
            {
              YYSYS_LOG(ERROR,"delete chunkserver[%s] status from __all_server_status failed",to_cstring(cs));
            }
            else*/
            //del:e
            {
              //add:e
              root_server_->server_manager_.set_server_down(it);
			
            // mod zhaoqiong [MultiUPS] [MS_CS_Manage_Function] 20150528:b
            //root_server_->log_worker_->server_is_down(it->server_, now);
            root_server_->log_worker_->server_is_down(it->server_, now, it->cluster_id_);
            //mod:e 
              // scoped lock
              {
                //this for only one thread modify root_table
                yysys::CThreadGuard mutex_guard(&(root_server_->root_table_build_mutex_));
                yysys::CWLockGuard guard(root_server_->root_table_rwlock_);
                if (root_server_->root_table_ != NULL)
                {
                  root_server_->root_table_->server_off_line(static_cast<int32_t>
                                                             (it - root_server_->server_manager_.begin()), now);
                  // some cs is down, signal the balance worker
                  root_server_->balancer_thread_->wakeup();
                }
                else
                {
                  YYSYS_LOG(ERROR, "root_table_for_query_ = NULL, server_index=%ld",
                            it - root_server_->server_manager_.begin());
                }
              }
            }
            if (master)
            {
              root_server_->commit_task(SERVER_OFFLINE, OB_CHUNKSERVER, it->server_, 0, "hb server version null");
            }
          }
        }

        if (it->ms_status_ != ObServerStatus::STATUS_DEAD && it->port_ms_ != 0)
        {
          if (it->is_ms_alive(now, root_server_->config_.cs_lease_duration_time) )
          {
              if (now - it->last_hb_time_ms_ >
                  (root_server_->config_.cs_lease_duration_time / 2))
              {
                //hb to ms
                tmp_server = it->server_;
                tmp_server.set_port(it->port_ms_);
                if (OB_SUCCESS != root_server_->worker_->get_rpc_stub()
                    .heartbeat_to_ms(tmp_server,
                      root_server_->config_.cs_lease_duration_time,
                      root_server_->last_frozen_mem_version_,
                      root_server_->get_schema_version(),
                      root_server_->get_partition_lock_flag(),
                      root_server_->get_obi_role(),
                      root_server_->get_privilege_version(),
                      root_server_->get_config_version()))
                {
                  YYSYS_LOG(WARN, "heartbeat to ms fail, ms:[%s]", to_cstring(tmp_server));
                }
                //add pangtianze [Paxos rs_election] 20170321:b
                //del pangtianze [Paxos] 20170321
//                else
//                {
//                    ObServer rs_servers[OB_MAX_RS_COUNT];
//                    int32_t rs_server_count = 0;
//                    root_server_->get_rs_node_mgr()->get_all_alive_servers(rs_servers, rs_server_count);
//                    int ret2 = root_server_->worker_->get_rpc_stub()
//                            .send_rs_list_to_server(tmp_server, rs_servers, rs_server_count);
//                    if (OB_SUCCESS != ret2)
//                    {
//                        YYSYS_LOG(WARN, "refresh rs list to merge server [%s] failed, err=%d", tmp_server.to_cstring(), ret2);
//                    }
//                }
                //add:e
              }
          }
          else
          {
            tmp_server = it->server_;
            tmp_server.set_port(it->port_ms_);
            //add pangtianze [Paxos] 20170420:b
            YYSYS_LOG(ERROR,"mergeserver[%s] is down, is_listen_ms:%s, now:%ld, lease_duration:%s",
                to_cstring(tmp_server), it->is_lms() ? "true" : "false", now, root_server_->config_.cs_lease_duration_time.str());
            //add:e
            //add huangjianwei [Paxos rs_switch] 20160720:b
//            char ms_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
//            if (tmp_server.ip_to_string(ms_ip,sizeof(ms_ip)) != true)
//            {
//              YYSYS_LOG(ERROR,"mergeserver ip convert failed");
//            }
            //add:e
            root_server_->server_manager_.set_server_down_ms(it);
		
            // mod zhaoqiong [MultiUPS] [MS_CS_Manage_Function] 20150528:b
            //root_server_->server_manager_.set_server_down_ms(it);
            root_server_->log_worker_->server_is_down_ms(it->server_, it->port_ms_sql_, now, it->cluster_id_);
            //mod:e
            if (master)
            {
              // no sql port for chunkserver manager
              if (!it->lms_)
              {
                if (master)
                {
                  //add huangjianwei [Paxos rs_switch] 20160720:b
                    //del pangtianze [Paxos bugfix] 20170421:b
                    /*
                  svr_type = ObString::make_string("mergeserver");
                  if(OB_SUCCESS != root_server_->schema_service_->delete_server_status(svr_type,ObString::make_string(ms_ip),it->port_ms_))
                  {
                    YYSYS_LOG(ERROR,"delete mergeserver[%s] status from __all_server_status failed",to_cstring(tmp_server));
                  }
                  else
                  {
                    root_server_->server_manager_.set_server_down_ms(it);
                  }
                  */
                  //del
                  //add:e
                  root_server_->commit_task(SERVER_OFFLINE, OB_MERGESERVER, tmp_server,
                    it->port_ms_sql_, "hb server version null");
                }
              }
              //add lbzhong [Paxos Cluster.Balance] 201607014:b
              else
              {
                if (master)
                {
                  //add huangjianwei [Paxos rs_switch] 20160720:b
                    //del pangtianze [Paxos bugfix] 20170421:b
                    /*
                  svr_type = ObString::make_string("listen_ms");
                  if(OB_SUCCESS != root_server_->schema_service_->delete_server_status(svr_type,ObString::make_string(ms_ip),it->port_ms_))
                  {
                    YYSYS_LOG(ERROR,"delete listen_mergeserver[%s] status from __all_server_status failed",to_cstring(tmp_server));
                  }
                  else
                  {
                    root_server_->server_manager_.set_server_down_ms(it);
                  }*/
                  //del
                  //add:e
                  root_server_->commit_task(LMS_OFFLINE, OB_MERGESERVER, tmp_server,
                    it->port_ms_sql_, "hb server version null");
                }
              }
              //add:e
            }
          }
        }
      } //end for
    } //end if master
    //async heart beat
    usleep(CHECK_INTERVAL_US);
  }
  YYSYS_LOG(INFO, "[NOTICE] heart beat checker thread exit");
}
