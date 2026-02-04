/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <2013-04-23 17:05:22 fufeng.syd>
 * Version: $Id$
 * Filename: ob_root_reload_config.cpp
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@alipay.com>
 *
 */

#include "ob_root_reload_config.h"
#include "ob_root_server2.h"

using namespace oceanbase;
using namespace oceanbase::rootserver;
using namespace oceanbase::common;

ObRootReloadConfig::ObRootReloadConfig(const ObRootServerConfig &config)
  : config_(config), root_server_(NULL)
{

}

ObRootReloadConfig::~ObRootReloadConfig()
{

}

int ObRootReloadConfig::operator ()()
{
  int ret = OB_SUCCESS;

  if (NULL == root_server_)
  {
    YYSYS_LOG(WARN, "NULL root server.");
    ret = OB_NOT_INIT;
  }
  else
  {
    const ObRootServerConfig& config = root_server_->get_config();
    //config.print();

    if (OB_SUCCESS == ret && NULL != root_server_->ups_manager_)
    {
      root_server_->get_rs_node_mgr()->set_lease_params((int64_t)config.lease_interval_time,
                                                          (int64_t)config.leader_lease_interval_time,
                                                          (int64_t)config.no_hb_response_duration_time );
      //mod pangtianze [Paxos Cluster.Flow.UPS] 20170817:b
      if (!root_server_->ups_manager_->get_is_flow_control_by_ip())
      {
          root_server_->ups_manager_->set_ups_config((int32_t)config.read_master_master_ups_percent,
                                                     (int32_t)config.merge_master_master_ups_percent, //[628]
                                                     (int32_t)config.is_strong_consistency_read,
                                                     rootserver::UPS_MS_FLOW);
      }
      if (!root_server_->ups_manager_->get_is_cs_flow_init())
      {
        //root_server_->ups_manager_->set_is_cs_flow_init(true);
        root_server_->ups_manager_->set_ups_config((int32_t)config.read_master_master_ups_percent,
                                                   (int32_t)config.merge_master_master_ups_percent, //[628]
                                                   (int32_t)config.is_strong_consistency_read,
                                                   rootserver::UPS_CS_FLOW);
      }
      //mod pangtianze [Paxos Cluster.Flow.UPS] 20170817:e
      //add peiouya [MultiUPS] [UPS_Manage_Function] 20150520:b
      root_server_->ups_manager_->set_cluster_and_paxos_config_and_paxos_flag ((int64_t)config.use_cluster_num,
                                                                               (int64_t)config.use_paxos_num,
                                                                               (bool)config.is_use_paxos);
      //add 20150520:e
      root_server_->load_cluster_replicas_num();
    }

    YYSYS_LOG(INFO, "after reload config, ret=%d", ret);
  }
  return ret;
}

void ObRootReloadConfig::set_root_server(ObRootServer2 &root_server)
{
  root_server_ = &root_server;
}
