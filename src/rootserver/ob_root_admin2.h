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
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *     - some work details here
 */
#ifndef _OB_ROOT_ADMIN2_H
#define _OB_ROOT_ADMIN2_H 1

#include "common/ob_base_client.h"
#include "common/ob_obi_role.h"
#include "rootserver/ob_root_stat_key.h"
#include "common/ob_define.h"
#include <yysys.h>
#include <map>
#include <string>

namespace oceanbase
{
  namespace rootserver
  {
    static const int MAX_CONFIG_STR_LENGTH = 1024;
    using oceanbase::common::ObBaseClient;

    struct Arguments;
    typedef int (*CmdHandler)(ObBaseClient &client, Arguments &args);

    // declare all the handlers here
    int do_get_obi_role(ObBaseClient &client, Arguments &args);
    int do_set_obi_role(ObBaseClient &client, Arguments &args);
    int do_rs_admin(ObBaseClient &client, Arguments &args);
    int do_change_log_level(ObBaseClient &client, Arguments &args);
    int do_rs_stat(ObBaseClient &client, Arguments &args);
    int do_cs_create_table(ObBaseClient &client, Arguments &args);
    int do_set_ups_config(ObBaseClient &client, Arguments &args);
    int do_set_master_ups_config(ObBaseClient &client, Arguments &args);
    int do_change_ups_master(ObBaseClient &client, Arguments &args);
    int do_import_tablets(ObBaseClient &client, Arguments &args);
    int do_get_row_checksum(ObBaseClient &client, Arguments &args);
    int do_print_schema(ObBaseClient &client, Arguments &args);
    int do_print_root_table(ObBaseClient &client, Arguments &args);
    int do_shutdown_servers(ObBaseClient &client, Arguments &args);
    int do_restart_servers(ObBaseClient &client, Arguments &args);
    int do_dump_cs_tablet_info(ObBaseClient &client, Arguments &args);
    //add pangtianze [Paxos rs_election] 20150731:b
    int do_set_rs_leader(ObBaseClient &client, Arguments &args);
    //add:e
    int do_check_tablet_version(ObBaseClient &client, Arguments &args);
    int do_split_tablet(ObBaseClient &client, Arguments &args);
    int do_check_roottable(ObBaseClient &client, Arguments &args);
    int do_set_config(ObBaseClient &client, Arguments &args);
    int do_get_config(ObBaseClient &client, Arguments &args);
    int do_change_table_id(ObBaseClient &client, Arguments &args);
    int read_root_table_point(ObBaseClient &client, Arguments &args);
    int do_import(ObBaseClient &client, Arguments &args);
    int do_kill_import(ObBaseClient &client, Arguments &args);
    int do_create_table_for_emergency(ObBaseClient &client, Arguments &args);
    int do_drop_table_for_emergency(ObBaseClient &client, Arguments &args);
    int do_force_cs_report(ObBaseClient &client, Arguments &args);
    //add peiouya [MultiUPS] [PARTITION_LOCK_FUCTION] 20150609:b
    int do_major_freeze(ObBaseClient &client, Arguments &args);
    //add 20150609:e
    int do_minor_freeze(ObBaseClient &client, Arguments &args);

    //add chujiajia [Paxos rs_election] 20151102:b
    int do_change_rs_paxos_number(ObBaseClient &client, Arguments &args);
    //add:e
    //add chujiajia [Paxos ups_election] 20151222:b
    int do_change_ups_quorum_scale(ObBaseClient &client, Arguments &args);
    //add:e
    //add lbzhong [Paxos Cluster.Balance] 201607020:b
    int do_dump_balancer_info(ObBaseClient &client, Arguments &args);
    int do_get_replica_num(ObBaseClient &client, Arguments &args);
    //add:e
    //add bingo [Paxos table replica] 20170620:b
    int do_get_table_replica_num(ObBaseClient &client, Arguments &args);
    //add:e
    //add bingo [Paxos Cluster.Balance] 20161020:b
    int do_set_master_cluster_id(ObBaseClient &client, Arguments &args);
    //add:e
    //add bingo [Paxos rs management] 20170301:b
    int do_get_rs_leader(ObBaseClient &client, Arguments &args);
    //add:e
    int do_get_rs_stat(ObBaseClient &client, Arguments &args);

    //add liuzy [MultiUPS] [add_paxos_interface] 20160112:b
    int do_add_paxos_group(ObBaseClient &client, Arguments &args);
    //add 20160112:e

    int do_del_paxos_group(ObBaseClient &client, Arguments &args);

    //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160222:b
    int do_take_paxos_group_offline(ObBaseClient &client, Arguments &args);
    //add 20160222:e
    //add liuzy [MultiUPS] [add_cluster_interface] 20160311:b
    int do_add_cluster(ObBaseClient &client, Arguments &args);
    //add 20160311:e

    int do_del_cluster(ObBaseClient &client, Arguments &args);

    //add liuzy [MultiUPS] [take_cluster_offline_interface] 20160324:b
    int do_take_cluster_offline(ObBaseClient &client, Arguments &args);
    //add 20160324:e
    //add liuzy [MultiUPS] [take_online_interface] 20160418:b
    int do_take_paxos_group_online(ObBaseClient &client, Arguments &args);
    int do_take_cluster_online(ObBaseClient &client, Arguments &args);
    //add 20160418:e
    /*mod:e*/
     //add bingo [Paxos sstable info to rs log] 20170614:b
    int do_dump_sstable_info(ObBaseClient &client, Arguments &args);
    //add:e
    
    int do_dump_unusual_tablets(ObBaseClient &client, Arguments &args);
    int do_execute_range_collection(ObBaseClient &client, Arguments &args);
    int do_get_statistics_task(ObBaseClient &client, Arguments &args);

    struct Command
    {
      const char* cmdstr;
      int pcode;
      CmdHandler handler;
    };

    struct Arguments
    {
      Command command;
      const char* rs_host;
      int rs_port;
      int64_t request_timeout_us;
      oceanbase::common::ObiRole obi_role;
      int log_level;
      int stat_key;
      int32_t obi_read_percentage;
      const char* ups_ip;           // ups or rs ip
      int ups_port;                 // ups or rs port
      const char* cs_ip;
      int cs_port;
      int32_t ms_read_percentage;
      int32_t cs_read_percentage;
      int32_t master_master_ups_read_percentage;
      //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
      //int32_t slave_master_ups_read_percentage;
      //del:e
      //add bingo[Paxps Cluster.Flow.UPS] 20170116:b
      int32_t is_strong_consistent;
      //add:e
      int32_t force_change_ups_master;
      int32_t force_set_rs_leader;
      int32_t force_set_quorum_scale;
      uint64_t table_id;
      //add chujiajia [Paxos rs_election] 20151102:b
      int32_t rs_paxos_number;
      //add:e
      //add chujiajia [Paxos rs_election] 20151225:b
      int32_t ups_quorum_scale;
      //add:e
      //add lbzhong [Paxos Cluster.Balance] 201607021:b
      int32_t replica_num;
      //int32_t cluster_id;
      //add:e
      //add bingo [Paxos Cluster.Balance] 20161020:b
      int32_t new_master_cluster_id;
      //add:e
      int64_t collection_start_value;
      int64_t collection_end_value;
      char *database_name;
      int64_t table_version;
      const char* server_list;
      int32_t flag_cancel;
      int64_t tablet_version;
      char config_str[MAX_CONFIG_STR_LENGTH];
      const char* location;
      /*mod liuqc [add multi_ups to trunk] 2016/9/28 16:40 :b*/
      //add liuzy [MultiUPS] [add_paxos_interface] 20160112:b
      int64_t new_paxos_group_num;
      //add 20160112:e
      //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160222:b
      int64_t paxos_group_id;
      //add 20160222:e
      //add liuzy [MultiUPS] [add_cluster_interface] 20160311:b
      int64_t new_cluster_num;
      //add 20160311:e
      //add liuzy [MultiUPS] [take_cluster_offline_interface] 20160324:b
      int64_t cluster_id;
      //add 20160324:e
      /*mod:e*/
      int argc;
      char **argv;
      Arguments()
      {
        command.cmdstr = NULL;
        command.pcode = -1;
        command.handler = NULL;
        rs_host = DEFAULT_RS_HOST;
        rs_port = DEFAULT_RS_PORT;
        request_timeout_us = DEFAULT_REQUEST_TIMEOUT_US;
        log_level = INVALID_LOG_LEVEL;
        stat_key = OB_RS_STAT_COMMON;
        obi_read_percentage = -1;
        ups_ip = NULL;
        ups_port = 0;
        cs_ip = NULL;
        cs_port = 0;
        table_version = 0;
        cs_read_percentage = -1;
        ms_read_percentage = -1;
        master_master_ups_read_percentage = -1;
        //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
        //slave_master_ups_read_percentage = -1;
        //del:e
        //add bingo[Paxps Cluster.Flow.UPS] 20170116:b
        is_strong_consistent = -1; //[634]
        //add:e
        force_change_ups_master = 0;
        force_set_rs_leader = 0;
        force_set_quorum_scale = 0;
        table_id = common::OB_INVALID_ID;
        //add chujiajia [Paxos rs_election] 20151102:b
        rs_paxos_number = -1;
        ups_quorum_scale = -1;
        //add:e
        //add lbzhong [Paxos Cluster.Balance] 201607021:b
        replica_num = -1;
        cluster_id = common::OB_ALL_CLUSTER_FLAG;
        //add:e
        //add bingo [Paxos Cluster.Balance] 20161020:b
        new_master_cluster_id = common::OB_ALL_CLUSTER_FLAG;
        //add:e
        server_list = NULL;
        //add liuzy [MultiUPS] [add_paxos_interface] 20160112:b
        new_paxos_group_num = 0;
        //add 20160112:e
        //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160222:b
        paxos_group_id = -1;
        //add 20160222:e
        //add liuzy [MultiUPS] [add_cluster_interface] 20160311:b
        new_cluster_num = 0;
        //add 20160311:e
        //add liuzy [MultiUPS] [take_cluster_offline_interface] 20160324:b
        cluster_id = -1;
        //add 20160324:e
        flag_cancel = 0;
        config_str[0] = '\0';
        location = NULL;
        argc = 0;
        argv = NULL;

        tablet_version = 0;
      }
      void print();
      public:
      static const int INVALID_LOG_LEVEL = -1;
      private:
      static const char* DEFAULT_RS_HOST;
      static const int DEFAULT_RS_PORT = 2500;
      static const int64_t DEFAULT_REQUEST_TIMEOUT_US = 20000000; // 20s
    };

    void usage();
    void version();

    void build_cmd_with_suboption_map(std::map<const char *, std::string> &cmd_map);

    void print_stat_error_prompt(Arguments &args);

    void print_error_prompt(const char *cmd, const char *host, int port);

    int parse_cmd_line(int argc, char* argv[], Arguments &args);
    //add liuzy [MultiUPS] [take_online_interface] 20160418:b
    void print_err_message(int ret, bool is_cluster, Arguments &args);
    //add 20160418:e

  } // end namespace rootserver
} // end namespace oceanbase

#endif /* _OB_ROOT_ADMIN2_H */

