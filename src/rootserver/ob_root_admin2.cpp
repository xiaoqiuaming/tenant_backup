/*
 * Copyright (C) 2007-2013 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * rootserver admin tool
 *
 * Version: $Id$
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *     - some work details here
 */

#include "rootserver/ob_root_admin2.h"

#include <unistd.h>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <string>
#include "common/ob_define.h"
#include "common/ob_version.h"
#include "common/ob_result.h"
#include "common/serialization.h"
#include "common/ob_server.h"
#include "common/ob_schema.h"
#include "common/ob_scanner.h"
#include "common/ob_ups_info.h"
#include "common/utility.h"
#include "common/ob_get_param.h"
#include "common/ob_schema.h"
#include "common/ob_scanner.h"
#include "common/file_directory_utils.h"
#include "rootserver/ob_root_admin_cmd.h"
#include "rootserver/ob_root_stat_key.h"
#include "ob_daily_merge_checker.h"
#include "ob_root_table2.h"
#include "ob_tablet_info_manager.h"
using namespace oceanbase::common;
using namespace oceanbase::rootserver;

namespace oceanbase
{
  namespace rootserver
  {
    void usage()
    {
      printf("Usage: rs_admin -r <rootserver_ip> -p <rootserver_port> <command> -o <suboptions>\n");
      printf("\n\t-r <rootserver_ip>\tthe default is `127.0.0.1'\n");
      printf("\t-p <rootserver_port>\tthe default is 2500\n");
      printf("\t-t <request_timeout_us>\tthe default is 10000000(10S)\n");
      printf("\n\t<command>:\n");
      printf("\tstat -o ");
      const char** str = &OB_RS_STAT_KEYSTR[0];
      for (; NULL != *str; ++str)
      {
        printf("%s|", *str);
      }
      printf("\n");
      printf("\tboot_strap\n");
      printf("\tboot_recover\n");
      printf("\tdo_check_point\n");
      printf("\treload_config\n");
      printf("\tlog_move_to_debug\n");
      printf("\tlog_move_to_error\n");
      printf("\tdump_root_table\n");
      printf("\tclean_root_table\n");
      printf("\tcheck_root_table -o cs_ip=<cs_ip>,cs_port=<cs_port>\n");
      printf("\tget_row_checksum -o tablet_version=<tablet_version>,table_id=<table_id>\n");
      printf("\tcheck_tablet_version -o tablet_version=<tablet_version>\n");
      //printf("\tdump_unusual_tablets\n");
      printf("\tdump_unusual_tablets -o tablet_version=<tablet_version>\n");

      printf("\tdump_server_info\n");
      printf("\tdump_migrate_info\n");
      printf("\tdump_cs_tablet_info -o cs_ip=<cs_ip>,cs_port=<cs_port>\n");
      printf("\tchange_log_level -o ERROR|WARN|INFO|DEBUG\n");
      printf("\tclean_error_msg\n");
      printf("\tcs_create_table -o table_id=<table_id> -o table_version=<frozen_verson>\n");
      printf("\tget_obi_role\n");
      printf("\tset_obi_role -o OBI_SLAVE|OBI_MASTER\n");
      printf("\tget_config\n");
      printf("\tset_config -o config_name=config_value[,config_name2=config_value2[,...]]\n");
      printf("\tget_obi_config\n");
      printf("\tset_obi_config -o read_percentage=<read_percentage>\n");
      printf("\tset_obi_config -o rs_ip=<rs_ip>,rs_port=<rs_port>,read_percentage=<read_percentage>\n");
      printf("\tset_ups_config -o ups_ip=<ups_ip>,ups_port=<ups_port>,ms_read_percentage=<percentage>,cs_read_percentage=<percentage>\n");
      //add bingo[Paxps Cluster.Flow.UPS] 20170116:b
      //mod lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
      //printf("\tset_master_ups_config -o master_master_ups_read_percentage=<percentage>,slave_master_ups_read_percentage=<percentage>\n");
      printf("\tset_master_ups_config -o master_master_ups_read_percentage=<percentage>,is_strong_consistent=<1 or 0>\n");
      //mod:e
      //add:e
      //mod pangtianze [Paxos] not give choice to force change master for user 20170703:b
      //printf("\tchange_ups_master -o ups_ip=<ups_ip>,ups_port=<ups_port>[,force]\n");
      printf("\tchange_ups_master -o ups_ip=<ups_ip>,ups_port=<ups_port>\n");
      //mod:e
      printf("\timport_tablets -o table_id=<table_id>\n");
      //printf("\tprint_root_table -o table_id=<table_id>\n");
      printf("\tprint_schema -o location=<location>\n");
      printf("\tread_root_table_point -o location=<location>\n");
      printf("\trefresh_schema\n");
      printf("\tcheck_schema\n");
      printf("\tswitch_schema\n");
      printf("\tchange_table_id -o table_id=<table_id>\n");
      printf("\tforce_create_table -o table_id=<table_id>\n");
      printf("\tforce_drop_table -o table_id=<table_id>\n");
      printf("\tenable_balance|disable_balance\n");
      printf("\tenable_rereplication|disable_rereplication\n");
      printf("\tshutdown -o server_list=<ip1+ip2+...+ipn>[,cancel]\n");
      printf("\trestart_cs -o server_list=<ip1+ip2+...+ipn>[,cancel]\n");
      printf("\tsplit_tablet -o table_id=<table_id> -o table_version=<table_version>\n");
      printf("\timport table_name table_id uri\n");
      printf("\tkill_import table_name table_id\n");
      printf("\tforce_cs_report\n");
      //add peiouya [MultiUPS] [PARTITION_LOCK_FUCTION] 20150609:b
      printf("\tmajor_freeze\n");
      //add 20150609:e
      printf("\tminor_freeze\n");

      //add liuzy [MultiUPS] [add_paxos_interface] 20160112:b
      printf("\tadd_paxos_group -o new_paxos_group_num=<new_paxos_group_num>\n");
      //add 20160112:e
      printf("\tdel_paxos_group -o paxos_group_id=<max_paxos_group_id>\n");
      //add liuzy [MultiUPS] [take_online_interface] 20160418:b
      printf("\ttake_paxos_group_online -o paxos_group_id=<paxos_group_id>\n");
      //add 20160418:e

      //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160222:b
      printf("\ttake_paxos_group_offline -o paxos_group_id=<paxos_group_id>\n");
      //add 20160222:e
      //add liuzy [MultiUPS] [add_cluster_interface] 20160311:b
      printf("\tadd_cluster -o new_cluster_num=<new_cluster_num>\n");
      //add 20160311:e
      printf("\tdel_cluster -o cluster_id=<max_cluster_id>\n");

      //add liuzy [MultiUPS] [take_online_interface] 20160418:b
      printf("\ttake_cluster_online -o cluster_id=<cluster_id>\n");
      //add 20160418:e
      //add liuzy [MultiUPS] [take_cluster_offline_interface] 20160324:b
      printf("\ttake_cluster_offline -o cluster_id=<cluster_id>\n");
      //add 20160324:e
      //printf("\n\t-- For Paxos --\n");
      //add pangtianze [Paxos rs_election] 20150731:b
      printf("\tset_rs_leader -o rs_ip=<rs_ip>,rs_port=<rs_port>[,force]\n");
      //add:e
      //add chujiajia [Paxos ups_election] 20151222:b
      printf("\tchange_rs_paxos_num -o paxos_num=<rs_paxos_num>\n");
      //printf("\tchange_ups_quorum_scale -o quorum_scale=<ups_quorum_scale>\n");
      printf("\tchange_ups_quorum_scale -o paxos_group_id=<paxos_group_id>,quorum_scale=<ups_quorum_scale>[,force]\n");

      //add:e
      //add lbzhong [Paxos Cluster.Balance] 201607020:b
      printf("\tdump_balancer_info -o table_id=<table_id>\n");
      printf("\tget_replica_num\n");
      //add bingo [Paxos table replica] 20170620:b
      printf("\tget_table_replica_num  -o table_id=<table_id>\n");
      //add:e
      //add bingo [Paxos Cluster.Balance] 20161020:b
      printf("\tset_master_cluster_id -o new_master_cluster_id=<cluser_id>\n");
      printf("\tget_rs_leader\n");
      //add:e
      printf("\tget_rs_stat\n");
      printf("\n\t-- For Statistics --\n");
      printf("\tget_statistics_task -o db_name=<database_name>\n");
      printf("\tgather_statistics\n");
      printf("\texecute_range_collection -o range_start=<range_start>,range_end=<range_end>\n");
      printf("\tstop_gather_statistics\n");
      printf("\n\t-- End Statistics --\n");
      //add pangtianze [Paxos rs_election] 20170228:b
      printf("\trefresh_rs_list\n");
      //add:e
      //add bingo [Paxos set_boot_ok] 20170315:b
      printf("\tset_boot_ok\n");
      printf("\tdump_sstable_info\n");
      //add:e
      // printf("\t-- End Paxos --\n");
      printf("\n\t-h\tprint this help message\n");
      printf("\t-V\tprint the version\n");
      printf("\nSee `rs_admin.log' for the detailed execution log.\n");
    }

    void version()
    {
      printf("rs_admin (%s %s)\n", PACKAGE_STRING, RELEASEID);
      printf("GIT_VERSION: %s\n", git_version());
      printf("BUILD_TIME: %s %s\n\n", build_date(), build_time());
      printf("Copyright (c) 2007-2011 Taobao Inc.\n");
    }

    const char* Arguments::DEFAULT_RS_HOST = "127.0.0.1";

    // define all commands string and its pcode here
    Command COMMANDS[] = {
      {
        "get_obi_role", OB_GET_OBI_ROLE, do_get_obi_role
      },
      {
        "set_obi_role", OB_SET_OBI_ROLE, do_set_obi_role
      },
      {
        "force_create_table", OB_FORCE_CREATE_TABLE_FOR_EMERGENCY, do_create_table_for_emergency
      },
      {
        "force_drop_table", OB_FORCE_DROP_TABLE_FOR_EMERGENCY, do_drop_table_for_emergency
      },
      {
        "do_check_point", OB_RS_ADMIN_CHECKPOINT, do_rs_admin
      },
      {
        "reload_config", OB_RS_ADMIN_RELOAD_CONFIG, do_rs_admin
      },
      {
        "log_move_to_debug", OB_RS_ADMIN_INC_LOG_LEVEL, do_rs_admin
      },
      {
        "log_move_to_error", OB_RS_ADMIN_DEC_LOG_LEVEL, do_rs_admin
      },
      {
        "dump_root_table", OB_RS_ADMIN_DUMP_ROOT_TABLE, do_rs_admin
      },
      {
        "clean_root_table", OB_RS_ADMIN_CLEAN_ROOT_TABLE, do_rs_admin
      },
      {
        "dump_server_info", OB_RS_ADMIN_DUMP_SERVER_INFO, do_rs_admin
      },
      {
        "dump_migrate_info", OB_RS_ADMIN_DUMP_MIGRATE_INFO, do_rs_admin
      } ,
      {
        "dump_unusual_tablets", OB_RS_DUMP_UNUSUAL_TABLETS, do_dump_unusual_tablets
      },
      {
        "check_schema", OB_RS_ADMIN_CHECK_SCHEMA, do_rs_admin
      },
      {
        "refresh_schema", OB_RS_ADMIN_REFRESH_SCHEMA, do_rs_admin
      },
      {
        "switch_schema", OB_RS_ADMIN_SWITCH_SCHEMA, do_rs_admin
      },
      {
        "clean_error_msg", OB_RS_ADMIN_CLEAN_ERROR_MSG, do_rs_admin
      },
      {
        "change_log_level", OB_CHANGE_LOG_LEVEL, do_change_log_level
      },
      {
        "stat", OB_RS_STAT, do_rs_stat
      },
      {
        "set_ups_config", OB_SET_UPS_CONFIG, do_set_ups_config
      },
      {
        "cs_create_table", OB_CS_CREATE_TABLE, do_cs_create_table
      },
      {
        "set_master_ups_config", OB_SET_MASTER_UPS_CONFIG, do_set_master_ups_config
      },
      {
        "change_ups_master", OB_CHANGE_UPS_MASTER, do_change_ups_master
      },
      {
        "change_table_id", OB_CHANGE_TABLE_ID, do_change_table_id
      } ,
      {
        "import_tablets", OB_CS_IMPORT_TABLETS, do_import_tablets
      },
      {
        "get_row_checksum", OB_GET_ROW_CHECKSUM, do_get_row_checksum
      },
      {
        "print_schema", OB_FETCH_SCHEMA, do_print_schema
      },
      {
        "print_root_table", OB_GET_REQUEST, do_print_root_table
      },
      {
        "enable_balance", OB_RS_ADMIN_ENABLE_BALANCE, do_rs_admin
      },
      {
        "disable_balance", OB_RS_ADMIN_DISABLE_BALANCE, do_rs_admin
      },
      {
        "enable_rereplication", OB_RS_ADMIN_ENABLE_REREPLICATION, do_rs_admin
      },
      {
        "disable_rereplication", OB_RS_ADMIN_DISABLE_REREPLICATION, do_rs_admin
      },
      {
        "enable_load_data", OB_RS_ADMIN_ENABLE_LOAD_DATA, do_rs_admin
      },
      {
        "shutdown", OB_RS_SHUTDOWN_SERVERS, do_shutdown_servers
      },
      {
        "boot_strap", OB_RS_ADMIN_BOOT_STRAP, do_rs_admin
      }
      //add liuxiao [secondary index] 20150320
      //bootstrapç”¨äºŽå»ºåˆ—æ ¡éªŒå’Œå†…éƒ¨è¡¨
      ,
      {
        "boot_strap_for_create_all_cchecksum_info", OB_RS_ADMIN_CREATE_ALL_CCHECKSUM_INFO, do_rs_admin
      },
      //add e
      {
        "boot_recover", OB_RS_ADMIN_BOOT_RECOVER, do_rs_admin
      },
      {
        "init_cluster", OB_RS_ADMIN_INIT_CLUSTER, do_rs_admin
      },
      {
        "restart_cs", OB_RS_RESTART_SERVERS, do_restart_servers
      },
      {
        "dump_cs_tablet_info" , OB_RS_DUMP_CS_TABLET_INFO, do_dump_cs_tablet_info
      },
      //add pangtianze [Paxos rs_election] 20170228:b
      {
        "refresh_rs_list", OB_RS_ADMIN_REFRESH_RS_LIST, do_rs_admin
      },
      //add:e
      //add pangtianze [Paxos rs_election] 20150813:b
      {
        "set_rs_leader" , OB_RS_ADMIN_SET_LEADER, do_set_rs_leader
      },
      //add:e
      //add chujiajia [Paxos rs_election] 20151102:b
      {
        "change_rs_paxos_num" , OB_CHANGE_RS_PAXOS_NUMBER, do_change_rs_paxos_number
      },
      //add:e
      //add chujiajia [Paxos rs_election] 20151225:b
      {
        "change_ups_quorum_scale" , OB_CHANGE_UPS_QUORUM_SCALE, do_change_ups_quorum_scale
      },
      //add:e
      {
        "check_tablet_version", OB_RS_CHECK_TABLET_MERGED, do_check_tablet_version
      },
      {
        "split_tablet", OB_RS_SPLIT_TABLET, do_split_tablet
      },
      {
        "check_root_table", OB_RS_CHECK_ROOTTABLE, do_check_roottable
      },
      {
        "set_config", OB_SET_CONFIG, do_set_config
      },
      {
        "get_config", OB_GET_CONFIG, do_get_config
      },
      {
        "import", OB_RS_ADMIN_START_IMPORT, do_import
      },
      {
        "kill_import", OB_RS_ADMIN_START_KILL_IMPORT, do_kill_import
      },
      {
        "read_root_table_point", OB_RS_ADMIN_READ_ROOT_TABLE, read_root_table_point
      },
      {
        "force_cs_report", OB_RS_FORCE_CS_REPORT, do_force_cs_report
      }

      //add peiouya [MultiUPS] [PARTITION_LOCK_FUCTION] 20150609:b
      ,
      {
        "major_freeze", OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE, do_major_freeze
      }
      //add 20150609:e
      ,
      {
        "minor_freeze", OB_UPS_MINOR_FREEZE_MEMTABLE, do_minor_freeze
      }
      //add liuzy [MultiUPS] [add_paxos_interface] 20160112:b
      ,
      {
        "add_paxos_group", OB_RS_ADD_PAXOS_GROUP, do_add_paxos_group
      }
      //add 20160112:e
      ,
      {
        "del_paxos_group", OB_RS_DEL_PAXOS_GROUP, do_del_paxos_group
      }

      //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160222:b
      ,
      {
        "take_paxos_group_offline", OB_RS_TAKE_PAXOS_GROUP_OFFLINE, do_take_paxos_group_offline
      }
      //add 20160222:e
      //add liuzy [MultiUPS] [add_cluster_interface] 20160311:b
      ,
      {
        "add_cluster", OB_RS_ADD_CLUSTER, do_add_cluster
      }
      //add 20160311:e
      ,
      {
        "del_cluster", OB_RS_DEL_CLUSTER, do_del_cluster
      }
      //add liuzy [MultiUPS] [take_cluster_offline_interface] 20160324:b
      ,
      {
        "take_cluster_offline", OB_RS_TAKE_CLUSTER_OFFLINE, do_take_cluster_offline
      }
      //add 20160324:e
      //add liuzy [MultiUPS] [take_online_interface] 20160418:b
      ,
      {
        "take_paxos_group_online", OB_RS_TAKE_PAXOS_GROUP_ONLINE, do_take_paxos_group_online
      },
      {
        "take_cluster_online", OB_RS_TAKE_CLUSTER_ONLINE, do_take_cluster_online
      }
      //add 20160418:e
      ,
      //add lbzhong [Paxos Cluster.Balance] 201607020:b
      {
        "dump_balancer_info", OB_RS_DUMP_BALANCER_INFO, do_dump_balancer_info
      },
      {
        "get_replica_num", OB_GET_REPLICA_NUM, do_get_replica_num
      },
      //add:e
      //add bingo [Paxos Cluster.Balance] 20161020:b
      {
        "set_master_cluster_id", OB_SET_MASTER_CLUSTER_ID, do_set_master_cluster_id
      },
      //add:e
      //add bingo [Paxos rs management] 20170301:b
      {
        "get_rs_leader", OB_GET_RS_LEADER, do_get_rs_leader
      },
      {
        "get_rs_stat", OB_GET_RS_STAT, do_get_rs_stat
      },
      //add:e
      //add bingo [Paxos set_boot_ok] 20170315:b
      {
        "set_boot_ok", OB_RS_ADMIN_SET_BOOT_OK, do_rs_admin
      },
      
      //add:e
      //add bingo [Paxos sstable info to rs log] 20170614:b
      {
        "dump_sstable_info", OB_DUMP_SSTABLE_INFO, do_dump_sstable_info
      },
      //add:e
      //add bingo [Paxos table replica] 20170620:b
      {
        "get_table_replica_num", OB_GET_TABLE_REPLICA_NUM, do_get_table_replica_num
      },
      //add:e
      {
        "gather_statistics", OB_RS_ADMIN_GATHER_STATISTICS, do_rs_admin
      },
      {
        "stop_gather_statistics", OB_RS_ADMIN_STOP_GATHER_STATISTICS, do_rs_admin
      },
      {
        "execute_range_collection", OB_EXECUTE_RANGE_COLLECTION, do_execute_range_collection
      },
      {
        "get_statistics_task", OB_GET_STATISTICS_TASK, do_get_statistics_task
      }
    };

    enum
    {
      OPT_OBI_ROLE_MASTER = 0,
      OPT_OBI_ROLE_SLAVE = 1,
      OPT_LOG_LEVEL_ERROR = 2,
      OPT_LOG_LEVEL_WARN = 3,
      OPT_LOG_LEVEL_INFO = 4,
      OPT_LOG_LEVEL_DEBUG = 5,
      OPT_READ_PERCENTAGE = 6,
      OPT_UPS_IP = 7,
      OPT_UPS_PORT = 8,
      OPT_MS_READ_PERCENTAGE = 9,
      OPT_CS_READ_PERCENTAGE = 10,
      OPT_RS_IP = 11,
      OPT_RS_PORT = 12,
      OPT_TABLE_ID = 13,
      OPT_FORCE = 14,
      OPT_SERVER_LIST = 15,
      OPT_CANCEL = 16,
      OPT_CS_IP = 17,
      OPT_CS_PORT = 18,
      OPT_TABLET_VERSION = 19,
      OPT_TABLE_VERSION = 20,
      OPT_READ_MASTER_MASTER_UPS_PERCENTAGE = 21,
      //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
      //OPT_READ_SLAVE_MASTER_UPS_PERCENTAGE = 22,
      //del:e
      //add bingo[Paxps Cluster.Flow.UPS] 20170116:b
      OPT_IS_STRONG_CONSISTENT = 22,
      //add:e
      OPT_SCHEMA_LOCATION = 23,
      //add chujiajia [Paxos rs_election] 20151102:b
      OPT_CHANGE_RS_PAXOS_NUMBER = 24,
      //add:e
      //add chujiajia [Paxos rs_election] 20151225:b
      OPT_CHANGE_UPS_QUORUM_SCALE = 25,
      //add:e
      //add lbzhong [Paxos Cluster.Balance] 201607022:b
      OPT_CLUSTER_ID = 26,
      OPT_REPLICA_NUM = 27,
      //add:e
      //add bingo [Paxos Cluster.Balance] 20161020:b
      OPT_MASTER_CLUSTER_ID = 28,
      //add:e
      //add liuzy [MultiUPS] [add_paxos_interface] 20160111:b
      OPT_ADD_PAXOS_GROUP = 29,
      //add 20160111:e
      //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160222:b
      OPT_TAKE_PAXOS_GROUP_OFFLINE = 30,
      //add 20160222:e
      //add liuzy [MultiUPS] [add_cluster_interface] 20160311:b
      OPT_ADD_CLUSTER = 31,
      //add 20160311:e
      //add liuzy [MultiUPS] [take_cluster_offline_interface] 20160324:b
      OPT_TAKE_CLUSTER_OFFLINE = 32,
      //add 20160324:e
      //add liuzy [MultiUPS] [take_online_interface] 20160418:b
      OPT_TAKE_PAXOS_GROUP_ONLINE = 33,
      OPT_TAKE_CLUSTER_ONLINE = 34,
      //add 20160418:e
      OPT_PAXOS_GROUP_ID = 35,
      OPT_COLLECTION_START = 36,
      OPT_COLLECTION_END = 37,
      OPT_DATABASE_NAME = 38,
      THE_END
    };

    // 需要与上面的enum一一对应
    const char* SUB_OPTIONS[] =
    {
      "OBI_MASTER",
      "OBI_SLAVE",
      "ERROR",
      "WARN",
      "INFO",
      "DEBUG",
      "read_percentage",            // 6
      "ups_ip",                     // 7
      "ups_port",                   // 8
      "ms_read_percentage",         // 9
      "cs_read_percentage",         // 10
      "rs_ip",                      // 11
      "rs_port",                    // 12
      "table_id",                   // 13
      "force",                      // 14
      "server_list",                // 15
      "cancel",                     // 16
      "cs_ip",                      //17
      "cs_port",                    //18
      "tablet_version",             //19
      "table_version",
      "master_master_ups_read_percentage", //21
      //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
      //"slave_master_ups_read_percentage",
      //del:e
      //add bingo[Paxps Cluster.Flow.UPS] 20170116:b
      "is_strong_consistent",       //22
      //add:e
      "location",                   //23
      //add chujiajia [Paxos rs_election] 20151102:b
      "paxos_num",      //24
      //add:e
      //add chujiajia [Paxos rs_election] 20151102:b
      "quorum_scale",           //25
      //add:e
      //add lbzhong [Paxos Cluster.Balance] 201607022:b
      "cluster_id",                 //26
      "replica_num",                 //27
      //add:e
      //add bingo [Paxos Cluster.Balance] 20161020:b
      "new_master_cluster_id",        //28
      //add:e
      //add liuzy [MultiUPS] [add_paxos_interface] 20160112:b
      "new_paxos_group_num",           //29
      //add 20160112:e
      //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160222:b
      "paxos_group_id",          //30
      //add 20160222:e
      //add liuzy [MultiUPS] [add_cluster_interface] 20160311:b
      "new_cluster_num",         //31
      //add 20160311:e
      //add liuzy [MultiUPS] [take_cluster_offline_interface] 20160324:b
      "cluster_id",              //32
      //add 20160324:e
      //add liuzy [MultiUPS] [take_online_interface] 20160418:b
      "paxos_group_id",          //33
      "cluster_id",              //34
      //add 20160418:e
      "paxos_group_id", //35
      "range_start", //36
      "range_end", //37
      "db_name", //38
      NULL
    };

    void build_cmd_with_suboption_map(std::map<const char *, std::string> &cmd_map)
    {
      cmd_map.insert(std::map<const char *, std::string>::value_type(COMMANDS[18].cmdstr,
                                                                     "stat -o reserve|common|start_time|prog_version|pid|local_time|mem|rs_status|frozen_version|schema_version|log_id|log_file_id|table_num|tablet_num|replicas_num|cs_num|ms_num|cs|ms|ups|rs_slave|ops_get|ops_scan|rs_slave_num|frozen_time|client_conf|sstable_dist|fail_get_count|fail_scan_count|get_obi_role_count|migrate_count|copy_count|merge|unusual_tablets_num|shutdown_cs|all_server|table_count|tablet_count|row_count|data_size|rs_paxos_num|ups_quorum_scale|ups_leader|master_cluster_id|paxos_group_offline_info|cluster_offline_info|all_server_in_clusters|ups_manager"));
      cmd_map.insert(std::map<const char *, std::string>::value_type(COMMANDS[45].cmdstr,
                                                                     "check_root_table -o cs_ip=<cs_ip>,cs_port=<cs_port>"));
      cmd_map.insert(std::map<const char *, std::string>::value_type(COMMANDS[25].cmdstr,
                                                                     "get_row_checksum -o tablet_version=<tablet_version>,table_id=<table_id>"));
      cmd_map.insert(std::map<const char *, std::string>::value_type(COMMANDS[43].cmdstr,
                                                                     "check_tablet_version -o tablet_version=<tablet_version>"));
      cmd_map.insert(std::map<const char *, std::string>::value_type(COMMANDS[12].cmdstr,
                                                                     "dump_unusual_tablets -o tablet_version=<tablet_version>"));
      cmd_map.insert(std::map<const char *, std::string>::value_type(COMMANDS[38].cmdstr,
                                                                     "dump_cs_tablet_info -o cs_ip=<cs_ip>,cs_port=<cs_port>"));
      cmd_map.insert(std::map<const char *, std::string>::value_type(COMMANDS[17].cmdstr,
                                                                     "change_log_level -o ERROR|WARN|INFO|DEBUG"));
      cmd_map.insert(std::map<const char *, std::string>::value_type(COMMANDS[20].cmdstr,
                                                                     "cs_create_table -o table_id=<table_id> -o table_version=<frozen_version>"));
      cmd_map.insert(std::map<const char *, std::string>::value_type(COMMANDS[1].cmdstr,
                                                                     "set_obi_role -o OBI_SLAVE|OBI_MASTER"));
      cmd_map.insert(std::map<const char *, std::string>::value_type(COMMANDS[46].cmdstr,
                                                                     "set_config -o config_name=config_value[,config_name2=config_value2[,...]]"));
      cmd_map.insert(std::map<const char *, std::string>::value_type(COMMANDS[19].cmdstr,
                                                                     "set_ups_config -o ups_ip=<ups_ip>,ups_port=<ups_port>,ms_read_percentage=<percentage>,cs_read_percentage=<percentage>"));
      cmd_map.insert(std::map<const char *, std::string>::value_type(COMMANDS[21].cmdstr,
                                                                     "set_master_ups_config -o master_master_ups_read_percentage=<percentage>,is_strong_consistent=<1 or 0>"));
      cmd_map.insert(std::map<const char *, std::string>::value_type(COMMANDS[22].cmdstr,
                                                                     "change_ups_master -o ups_ip=<ups_ip>,ups_port=<ups_port>"));
      cmd_map.insert(std::map<const char *, std::string>::value_type(COMMANDS[24].cmdstr,
                                                                     "import_tablets -o table_id=<table_id>"));
      cmd_map.insert(std::map<const char *, std::string>::value_type(COMMANDS[26].cmdstr,
                                                                     "print_schema -o location=<location>"));
      cmd_map.insert(std::map<const char *, std::string>::value_type(COMMANDS[50].cmdstr,
                                                                     "read_root_table_point -o location=<location>"));
      cmd_map.insert(std::map<const char *, std::string>::value_type(COMMANDS[23].cmdstr,
                                                                     "change_table_id -o table_id=<table_id>"));
      cmd_map.insert(std::map<const char *, std::string>::value_type(COMMANDS[2].cmdstr,
                                                                     "force_create_table -o table_id=<table_id>"));
      cmd_map.insert(std::map<const char *, std::string>::value_type(COMMANDS[3].cmdstr,
                                                                     "force_drop_table -o table_id=<table_id>"));
      cmd_map.insert(std::map<const char *, std::string>::value_type(COMMANDS[32].cmdstr,
                                                                     "shutdown -o server_list=<ip1+ip2+...+ipn>[,cancel]"));
      cmd_map.insert(std::map<const char *, std::string>::value_type(COMMANDS[37].cmdstr,
                                                                     "restart_cs -o server_list=<ip1+ip2+...+ipn>[,cancel]"));
      cmd_map.insert(std::map<const char *, std::string>::value_type(COMMANDS[44].cmdstr,
                                                                     "split_tablet -o table_id=<table_id> -o table_version=<table_version>"));
      cmd_map.insert(std::map<const char *, std::string>::value_type(COMMANDS[54].cmdstr,
                                                                     "add_paxos_group -o new_paxos_group_num=<new_paxos_group_num>"));
      cmd_map.insert(std::map<const char *, std::string>::value_type(COMMANDS[58].cmdstr,
                                                                     "take_paxos_group_online -o paxos_group_id=<paxos_group_id>"));
      cmd_map.insert(std::map<const char *, std::string>::value_type(COMMANDS[55].cmdstr,
                                                                     "take_paxos_group_offline -o paxos_group_id=<paxos_group_id>"));
      cmd_map.insert(std::map<const char *, std::string>::value_type(COMMANDS[56].cmdstr,
                                                                     "add_cluster -o new_cluster_num=<new_cluster_num>"));
      cmd_map.insert(std::map<const char *, std::string>::value_type(COMMANDS[59].cmdstr,
                                                                     "take_cluster_online -o cluster_id=<cluster_id>"));
      cmd_map.insert(std::map<const char *, std::string>::value_type(COMMANDS[57].cmdstr,
                                                                     "take_cluster_offline -o cluster_id=<cluster_id>"));
      cmd_map.insert(std::map<const char *, std::string>::value_type(COMMANDS[40].cmdstr,
                                                                     "set_rs_leader -o rs_ip=<rs_ip>,rs_port=<rs_port>[,force]"));
      cmd_map.insert(std::map<const char *, std::string>::value_type(COMMANDS[41].cmdstr,
                                                                     "change_rs_paxos_num -o paxos_num=<rs_paxos_num>"));
      cmd_map.insert(std::map<const char *, std::string>::value_type(COMMANDS[42].cmdstr,
                                                                     "change_ups_quorum_scale -o paxos_group_id=<paxos_group_id>,quorum_scale=<ups_quorum_scale>[,force]"));
      cmd_map.insert(std::map<const char *, std::string>::value_type(COMMANDS[60].cmdstr,
                                                                     "dump_balancer_info -o table_id=<table_id>"));
      cmd_map.insert(std::map<const char *, std::string>::value_type(COMMANDS[67].cmdstr,
                                                                     "get_table_replica_num -o table_id=<table_id>"));
      cmd_map.insert(std::map<const char *, std::string>::value_type(COMMANDS[62].cmdstr,
                                                                     "set_master_cluster_id -o new_master_cluster_id=<cluster_id>"));
    }

    void print_stat_error_prompt(Arguments &args)
    {
      std::map<int64_t, const char *> distance_map;
      int64_t distance = 0;

      for (int i = 0; OB_RS_STAT_KEYSTR[i] != NULL; i++)
      {
        distance = get_edit_distance(args.config_str, OB_RS_STAT_KEYSTR[i]);
        distance_map.insert(std::map<int64_t, const char *>::value_type(distance, OB_RS_STAT_KEYSTR[i]));
      }

      int count = 0;
      printf("\nthe most probably commands you may want to input are:\n");
      for (std::map<int64_t, const char *>::iterator it = distance_map.begin() ;
           it != distance_map.end() && count < 3 ; it ++)
      {
        printf("bin/rs_admin -r %s -p %d stat -o %s\n", args.rs_host, args.rs_port, it->second);
        count ++;
      }
      printf("\n");
    }

    void print_error_prompt(const char *cmd, const char *host, int port)
    {
      std::map<int64_t, const char *> distance_map;
      int64_t distance = 0;

      for (uint32_t i = 0; i < ARRAYSIZEOF(COMMANDS); ++i)
      {
        distance = get_edit_distance(cmd, COMMANDS[i].cmdstr);
        distance_map.insert(std::map<int64_t, const char *>::value_type(distance, COMMANDS[i].cmdstr));
      }

      std::map<const char *, std::string> cmd_with_suboption_map;
      build_cmd_with_suboption_map(cmd_with_suboption_map);

      int count = 0;
      printf("\nthe most probably commands you may want to input are:\n");
      for (std::map<int64_t, const char *>::iterator it = distance_map.begin() ;
           it != distance_map.end() && count < 3 ; it ++)
      {
        std::map<const char *, std::string>::iterator sub_it = cmd_with_suboption_map.find(it->second);
        if (sub_it != cmd_with_suboption_map.end())
        {
          printf("bin/rs_admin -r %s -p %d %s\n", host, port, (sub_it->second).c_str());
        }
        else
        {
          printf("bin/rs_admin -r %s -p %d %s\n", host, port, it->second);
        }
        count ++;
      }
      printf("\n");
    }

    int parse_cmd_line(int argc, char* argv[], Arguments &args)
    {
      int ret = OB_SUCCESS;
      // merge SUB_OPTIONS and OB_RS_STAT_KEYSTR
      int key_num = 0;
      const char** str = &OB_RS_STAT_KEYSTR[0];
      for (; NULL != *str; ++str)
      {
        key_num++;
      }
      int local_num = static_cast<int>(ARRAYSIZEOF(SUB_OPTIONS));
      const char** all_sub_options = new(std::nothrow) const char*[key_num+local_num];
      if (NULL == all_sub_options)
      {
        printf("no memory\n");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        //memset(all_sub_options, 0, sizeof(all_sub_options));
        for (int i = 0; i < local_num; ++i)
        {
          all_sub_options[i] = SUB_OPTIONS[i];
        }
        for (int i = 0; i < key_num; ++i)
        {
          all_sub_options[i+local_num-1] = OB_RS_STAT_KEYSTR[i];
        }
        all_sub_options[local_num+key_num-1] = NULL;

        char *subopts = NULL;
        char *value = NULL;
        int ch = -1;
        int suboptidx = 0;
        while(-1 != (ch = getopt(argc, argv, "hVr:p:o:t:")))
        {
          switch(ch)
          {
            case '?':
              usage();
              exit(-1);
              break;
            case 'h':
              usage();
              exit(0);
              break;
            case 'V':
              version();
              exit(0);
              break;
            case 'r':
              args.rs_host = optarg;
              break;
            case 'p':
              args.rs_port = atoi(optarg);
              break;
            case 't':
              args.request_timeout_us = atoi(optarg);
              break;
            case 'o':
              subopts = optarg;
              snprintf(args.config_str, MAX_CONFIG_STR_LENGTH, "%s", subopts);
              while('\0' != *subopts)
              {
                switch(suboptidx = getsubopt(&subopts, (char* const*)all_sub_options, &value))
                {
                  case -1:
                    args.stat_key = -1;
                    break;
                  case OPT_OBI_ROLE_MASTER:
                    args.obi_role.set_role(ObiRole::MASTER);
                    break;
                  case OPT_OBI_ROLE_SLAVE:
                    args.obi_role.set_role(ObiRole::SLAVE);
                    break;
                  case OPT_LOG_LEVEL_ERROR:
                    args.log_level = YYSYS_LOG_LEVEL_ERROR;
                    break;
                  case OPT_LOG_LEVEL_WARN:
                    args.log_level = YYSYS_LOG_LEVEL_WARN;
                    break;
                  case OPT_LOG_LEVEL_INFO:
                    args.log_level = YYSYS_LOG_LEVEL_INFO;
                    break;
                  case OPT_LOG_LEVEL_DEBUG:
                    args.log_level = YYSYS_LOG_LEVEL_DEBUG;
                    break;
                  case OPT_READ_PERCENTAGE:
                    if (NULL == value)
                    {
                      printf("read_percentage needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.obi_read_percentage = atoi(value);
                    }
                    break;
                  case OPT_UPS_IP:
                    if (NULL == value)
                    {
                      printf("option `ups' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.ups_ip = value;
                    }
                    break;
                  case OPT_SCHEMA_LOCATION:
                    if (NULL == value)
                    {
                      printf("option location needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.location = value;
                      YYSYS_LOG(INFO, "location name is %s", args.location);
                    }
                    break;
                  case OPT_UPS_PORT:
                    if (NULL == value)
                    {
                      printf("option `port' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.ups_port = atoi(value);
                    }
                    break;
                  case OPT_CS_IP:
                    if (NULL == value)
                    {
                      printf("option 'cs_ip' needs an argment value \n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.cs_ip = value;
                    }
                    break;
                  case OPT_CS_PORT:
                    if (NULL == value)
                    {
                      printf("option `port' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.cs_port = atoi(value);
                    }
                    break;
                  case OPT_TABLET_VERSION:
                    if (NULL == value)
                    {
                      printf("option 'tablet_version' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.tablet_version = atoi(value);
                    }
                    break;

                    //add chujiajia [Paxos rs_election] 20151102:b
                  case OPT_CHANGE_RS_PAXOS_NUMBER:
                    if (NULL == value)
                    {
                      printf("option 'paxos_num' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.rs_paxos_number = atoi(value);
                    }
                    break;
                    //add:e

                    //add chujiajia [Paxos rs_election] 20151225:b
                  case OPT_CHANGE_UPS_QUORUM_SCALE:
                    if (NULL == value)
                    {
                      printf("option 'quorum_scale' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.ups_quorum_scale = atoi(value);
                    }
                    break;
                    //add:e
                  case OPT_CLUSTER_ID:
                    if (NULL == value)
                    {
                      printf("option 'cluster_id' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.cluster_id = atoi(value);
                    }
                    break;
                  case OPT_PAXOS_GROUP_ID:
                    if (NULL == value)
                    {
                      printf("option 'paxos_group_id' needs an argument value\n");
                      return OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.paxos_group_id = atoi(value);
                    }
                    break;
                  case OPT_REPLICA_NUM:
                    if (NULL == value)
                    {
                      printf("option 'replica_num' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.replica_num = atoi(value);
                    }
                    break;
                    //add:e
                    //add bingo [Paxos Cluster.Balance] 20161020:b
                  case OPT_MASTER_CLUSTER_ID:
                    if(NULL == value)
                    {
                      printf("option 'new_master_cluster_id' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.new_master_cluster_id = atoi(value);
                    }
                    break;
                    //add:e

                  case OPT_MS_READ_PERCENTAGE:
                    if (NULL == value)
                    {
                      printf("option `ms_read_percentage' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.ms_read_percentage = atoi(value);
                    }
                    break;
                  case OPT_CS_READ_PERCENTAGE:
                    if (NULL == value)
                    {
                      printf("option `cs_read_percentage' needs an argument value\n");
                      ret = OB_ERROR;
                    }
                    else
                    {
                      args.cs_read_percentage = atoi(value);
                    }
                    break;
                  case OPT_READ_MASTER_MASTER_UPS_PERCENTAGE:
                    if (NULL == value)
                    {
                      printf("option `master_master_ups_read_percentage' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      char *end = NULL;
                      int32_t data = static_cast<int32_t>(strtol(value, &end, 10));
                      if (end != value + strlen(value))
                      {
                        printf("option `master_master_ups_read_percentage' needs an valid integer value, value=%s\n", value);
                        ret = OB_INVALID_ARGUMENT;
                      }
                      else
                      {
                        args.master_master_ups_read_percentage = data;
                      }
                    }
                    break;
                    //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
                    /*
                  case OPT_READ_SLAVE_MASTER_UPS_PERCENTAGE:
                    if (NULL == value)
                    {
                      printf("option `slave_master_ups_read_percentage' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      char *end = NULL;
                      int32_t data = static_cast<int32_t>(strtol(value, &end, 10));
                      if (end !=  value + strlen(value))
                      {
                        printf("option `slave_master_ups_read_percentage' needs an valid integer value, value=%s\n", value);
                        ret = OB_INVALID_ARGUMENT;
                      }
                      else
                      {
                        args.slave_master_ups_read_percentage = data;
                      }
                    }
                    break;
                    */
                    //del:e
                    //add bingo[Paxps Cluster.Flow.UPS] 20170116:b
                  case OPT_IS_STRONG_CONSISTENT:
                    if(NULL == value)
                    {
                      printf("option 'is_strong_consistent' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.is_strong_consistent = atoi(value);
                    }
                    break;
                    //add:e
                  case OPT_FORCE:
                    //mod pangtianze [Paxos] not give choice to force change master for user 20170703:b
                    //args.force_change_ups_master = 1;
                    args.force_change_ups_master = 0;
                    //mod:e
                    args.force_set_rs_leader = 1;
                    args.force_set_quorum_scale = 1;
                    break;
                  case OPT_RS_IP:
                    if (NULL == value)
                    {
                      printf("option `rs_ip' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.ups_ip = value;
                    }
                    break;
                  case OPT_RS_PORT:
                    if (NULL == value)
                    {
                      printf("option `rs_port' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.ups_port = atoi(value);
                    }
                    break;
                  case OPT_TABLE_VERSION:
                    if (NULL == value)
                    {
                      printf("option `table_id' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.table_version = atoi(value);
                    }
                    break;
                  case OPT_TABLE_ID:
                    if (NULL == value)
                    {
                      printf("option `table_id' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.table_id = atoi(value);
                    }
                    break;
                  case OPT_SERVER_LIST:
                    if (NULL == value)
                    {
                      printf("option `server_list' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.server_list = value;
                    }
                    break;
                  case OPT_CANCEL:
                    args.flag_cancel = 1;
                    break;

                    //add liuzy [MultiUPS] [add_paxos_interface] 20160112:b
                  case OPT_ADD_PAXOS_GROUP:
                    if (NULL == value)
                    {
                      printf("option 'new_paxos_group_num' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.new_paxos_group_num = atoi(value);
                    }
                    break;
                    //add 20160112:e
                    //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160222:b
                  case OPT_TAKE_PAXOS_GROUP_OFFLINE:
                    if (NULL == value)
                    {
                      printf("option 'paxos_group_id' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.paxos_group_id = atoi(value);
                    }
                    break;
                    //add 20160222:e
                    //add liuzy [MultiUPS] [add_cluster_interface] 20160311:b
                  case OPT_ADD_CLUSTER:
                    if (NULL == value)
                    {
                      printf("option 'new_cluster_num' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.new_cluster_num = atoi(value);
                    }
                    break;
                    //add 20160311:e
                    //add liuzy [MultiUPS] [take_cluster_offline_interface] 20160324:b
                  case OPT_TAKE_CLUSTER_OFFLINE:
                    if (NULL == value)
                    {
                      printf("option 'cluster_id' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.cluster_id = atoi(value);
                    }
                    break;
                    //add 20160324:e
                    //add liuzy [MultiUPS] [take_online_interface] 20160418:b
                  case OPT_TAKE_PAXOS_GROUP_ONLINE:
                    if (NULL == value)
                    {
                      printf("option 'paxos_group_id' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.paxos_group_id = atoi(value);
                    }
                    break;
                  case OPT_TAKE_CLUSTER_ONLINE:
                    if (NULL == value)
                    {
                      printf("option 'cluster_id' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.cluster_id = atoi(value);
                    }
                    break;
                    //add 20160418:e
                  case OPT_COLLECTION_START:
                    if (NULL == value)
                    {
                      printf("Option 'range_start' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else if (atoi(value) < 0)
                    {
                      printf("Option 'range_start' must be superior to 0 \n");
                    }
                    else
                    {
                      args.collection_start_value = atoi(value);
                      YYSYS_LOG(INFO, "TEST::WEIIXING start[%ld]", args.collection_start_value);
                    }
                    break;
                  case OPT_COLLECTION_END:
                    if (NULL == value)
                    {
                      printf("Option 'range_end' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else if (atoi(value) < 0)
                    {
                      printf("Option 'range_end' must be superior to 0 \n");
                    }
                    else if(atoi(value) < args.collection_start_value)
                    {
                      printf("Option 'range_end' must be superior to 'range_start' \n");
                    }
                    else
                    {
                      args.collection_end_value = atoi(value);
                      YYSYS_LOG(INFO, "TEST::WEIIXING end[%ld]", args.collection_end_value);
                    }
                    break;
                  case OPT_DATABASE_NAME:
                    if (NULL == value)
                    {
                      printf("option 'db_name' needs an argument value\n");
                      ret = OB_INVALID_ARGUMENT;
                    }
                    else
                    {
                      args.database_name = value;
                    }
                    break;
                  default:
                    // stat keys
                    args.stat_key = suboptidx - local_num + 1;
                    break;
                }
              }
            default:
              break;
          }
        }
        if (OB_SUCCESS != ret)
        {
          usage();
        }
        else if (optind >= argc)
        {
          printf("no command specified\n");
          usage();
          ret = OB_INVALID_ARGUMENT;
        }
        else
        {
          args.argc = argc - optind;
          args.argv = &argv[optind];
          char* cmd = argv[optind];
          for (uint32_t i = 0; i < ARRAYSIZEOF(COMMANDS); ++i)
          {
            if (0 == strcmp(cmd, COMMANDS[i].cmdstr))
            {
              args.command.cmdstr = cmd;
              args.command.pcode = COMMANDS[i].pcode;
              args.command.handler = COMMANDS[i].handler;
              break;
            }
          }
          if (-1 == args.command.pcode)
          {
            printf("unknown command=%s\n", cmd);
            ret = OB_INVALID_ARGUMENT;
            print_error_prompt(cmd, args.rs_host, args.rs_port);
          }
          else
          {
            if (OB_RS_ADMIN_GATHER_STATISTICS == args.command.pcode && args.request_timeout_us < 900000000)
            {
              args.request_timeout_us = 900000000;
            }
            args.print();
          }
        }
        if (NULL != all_sub_options)
        {
          delete [] all_sub_options;
          all_sub_options = NULL;
        }
      }
      return ret;
    }

    void Arguments::print()
    {
      YYSYS_LOG(INFO, "server_ip=%s port=%d", rs_host, rs_port);
      YYSYS_LOG(INFO, "cmd=%s pcode=%d", command.cmdstr, command.pcode);
      YYSYS_LOG(INFO, "obi_role=%d", obi_role.get_role());
      YYSYS_LOG(INFO, "log_level=%d", log_level);
      YYSYS_LOG(INFO, "stat_key=%d", stat_key);
      YYSYS_LOG(INFO, "obi_read_percentage=%d", obi_read_percentage);
      YYSYS_LOG(INFO, "ups=%s port=%d", ups_ip, ups_port);
      YYSYS_LOG(INFO, "cs=%s port=%d",cs_ip, cs_port);
      //add chujiajia [Paxos rs_election] 20151102:b
      YYSYS_LOG(INFO, "paxos_num=%d", rs_paxos_number);
      YYSYS_LOG(INFO, "quorum_scale=%d", ups_quorum_scale);
      //add:e
      //add lbzhong [Paxos Cluster.Balance] 201607022:b
      YYSYS_LOG(INFO, "cluster_id=%ld,replica_num=%d", cluster_id, replica_num);
      //add:e
      //add bingo [Paxos Cluster.Balance] 20161020:b
      YYSYS_LOG(INFO, "new_master_cluster_id=%d", new_master_cluster_id);
      //add:e
      YYSYS_LOG(INFO, "ms_read_percentage=%d", ms_read_percentage);
      YYSYS_LOG(INFO, "cs_read_percentage=%d", cs_read_percentage);
      YYSYS_LOG(INFO, "force_change_ups_master=%d", force_change_ups_master);
      YYSYS_LOG(INFO, "force_set_rs_leader=%d", force_set_rs_leader);
      YYSYS_LOG(INFO, "force_set_quorum_scale=%d", force_set_quorum_scale);
      YYSYS_LOG(INFO, "table_id=%lu", table_id);
      YYSYS_LOG(INFO, "server_list=%s", server_list);
      YYSYS_LOG(INFO, "tablet_version=%ld", tablet_version);

      //add liuzy [MultiUPS] [add_paxos_interface] 20160112:b
      YYSYS_LOG(INFO, "new_paxos_group_num=%ld", new_paxos_group_num);
      //add 20160112:e
      //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160222:b
      YYSYS_LOG(INFO, "paxos_group_id=%ld", paxos_group_id);
      //add 20160222:e
      //add liuzy [MultiUPS] [add_cluster_interface] 20160311:b
      YYSYS_LOG(INFO, "new_cluster_num=%ld", new_cluster_num);
      //add 20160311:e
      //add liuzy [MultiUPS] [take_cluster_offline_interface] 20160324:b
      YYSYS_LOG(INFO, "cluster_id=%ld", cluster_id);
      //add 20160324:e

    }

    //add liuzy [MultiUPS] [add_paxos_interface] 20160112:b
    int do_add_paxos_group(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      const int buff_size = static_cast<int> (sizeof(ObPacket) + 8);
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);
      if (0 >= args.new_paxos_group_num)
      {
        ret = OB_INVALID_ARGUMENT;
        printf("ERROR_MESSAGE: new_paxos_group_num[%ld] must be positive\n", args.new_paxos_group_num);
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(),
                                                               msgbuf.get_position(), args.new_paxos_group_num)))
      {
        printf("failed to serialize new_paxos_group_num, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = client.send_recv(OB_RS_ADD_PAXOS_GROUP, MY_VERSION,
                                                     args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(),
                                                         msgbuf.get_position())))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          print_err_message(ret, false, args);
        }
        else
        {
          printf("add new paxos group is succeeded.\n");
        }
      }
      if (OB_SUCCESS != ret)
      {
        printf("add new paxos group failed. err=%d.\n", ret);
      }
      return ret;
    }
    //add 20160112:e

    int do_del_paxos_group(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      const int buff_size = static_cast<int32_t> (sizeof(ObPacket) + 8);
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);
      if (0 >= args.paxos_group_id)
      {
        ret = OB_INVALID_ARGUMENT;
        printf("ERROR_MESSAGE: paxos_group_id[%ld] must be positive\n", args.paxos_group_id);
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(),
                                                               msgbuf.get_position(), args.paxos_group_id)))
      {
        printf("failed to serialize paxos_group_id, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = client.send_recv(OB_RS_DEL_PAXOS_GROUP, MY_VERSION,
                                                     args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(),
                                                         msgbuf.get_position())))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          print_err_message(ret, false, args);
        }
        else
        {
          printf("del paxos group[%ld] is succeeded.\n", args.paxos_group_id);
        }
      }
      if (OB_SUCCESS != ret)
      {
        printf("del paxos group[%ld] failed. err=%d.\n", args.paxos_group_id, ret);
      }
      return ret;
    }


    //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160222:b
    int do_take_paxos_group_offline(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      const int buff_size = static_cast<int> (sizeof(ObPacket) + 8);
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);
      if (0 >= args.paxos_group_id)
      {
        ret = OB_INVALID_ARGUMENT;
        printf("ERROR_MESSAGE: paxos_group_id[%ld] must be positive number\n", args.paxos_group_id);
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(),
                                                               msgbuf.get_position(), args.paxos_group_id)))
      {
        printf("failed to serialize paxos_id, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = client.send_recv(OB_RS_TAKE_PAXOS_GROUP_OFFLINE, MY_VERSION,
                                                     args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(),
                                                         msgbuf.get_position())))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          print_err_message(ret, false, args);
        }
        else
        {
          printf("take paxos group[%ld] offline succeeded\n", args.paxos_group_id);
        }
      }
      if (OB_SUCCESS != ret)
      {
        printf("take paxos group[%ld] offline failed, err=%d\n", args.paxos_group_id, ret);
      }
      return ret;
    }
    //add 20160222:e
    //add liuzy [MultiUPS] [add_cluster_interface] 20160311:b
    int do_add_cluster(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      const int buff_size = static_cast<int> (sizeof(ObPacket) + 8);
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);
      if (0 >= args.new_cluster_num)
      {
        ret = OB_INVALID_ARGUMENT;
        printf("ERROR_MESSAGE: new_cluster_num[%ld] must be positive number\n", args.new_cluster_num);
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(),
                                                               msgbuf.get_position(), args.new_cluster_num)))
      {
        printf("failed to serialize new_cluster_num, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = client.send_recv(OB_RS_ADD_CLUSTER, MY_VERSION,
                                                     args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(),
                                                         msgbuf.get_position())))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          print_err_message(ret, true, args);
        }
        else
        {
          printf("add new cluster succeeded\n");
        }
      }
      if (OB_SUCCESS != ret)
      {
        printf("add new clsuter failed, err=%d\n", ret);
      }
      return ret;
    }
    //add 20160311:e

    int do_del_cluster(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      const int buff_size = static_cast<int32_t> (sizeof(ObPacket) + 8);
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);
      if (0 >= args.cluster_id)
      {
        ret = OB_INVALID_ARGUMENT;
        printf("ERROR_MESSAGE: cluster_id[%ld] must be positive number\n", args.cluster_id);
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(),
                                                               msgbuf.get_position(), args.cluster_id)))
      {
        printf("failed to serialize cluster_id, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = client.send_recv(OB_RS_DEL_CLUSTER, MY_VERSION,
                                                     args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(),
                                                         msgbuf.get_position())))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          print_err_message(ret, true, args);
        }
        else
        {
          printf("del cluster[%ld] succeeded\n", args.cluster_id);
        }
      }
      if (OB_SUCCESS != ret)
      {
        printf("del clsuter[%ld] failed, err=%d\n", args.cluster_id, ret);
      }
      return ret;
    }

    //add liuzy [MultiUPS] [take_cluster_offline_interface] 20160324:b
    int do_take_cluster_offline(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      const int buff_size = static_cast<int> (sizeof(ObPacket) + 8);
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);
      if (0 >= args.cluster_id)
      {
        ret = OB_INVALID_ARGUMENT;
        printf("ERROR_MESSAGE: cluster_id[%ld] must be positive number\n", args.cluster_id);
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(),
                                                               msgbuf.get_position(), args.cluster_id)))
      {
        printf("failed to serialize cluster_id, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = client.send_recv(OB_RS_TAKE_CLUSTER_OFFLINE, MY_VERSION,
                                                     args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(),
                                                         msgbuf.get_position())))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          print_err_message(ret, true, args);
        }
        else
        {
          printf("take cluster[%ld] offline succeeded\n", args.cluster_id);
        }
      }
      if (OB_SUCCESS != ret)
      {
        printf("take cluster[%ld] offline failed, err=%d\n", args.cluster_id, ret);
      }
      return ret;
    }
    //add 20160324:e
    //add liuzy [MultiUPS] [take_online_interface] 20160418:b
    /*Exp: take paxos group online*/
    int do_take_paxos_group_online(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      const int buff_size = static_cast<int> (sizeof(ObPacket) + 8);
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);
      if (0 >= args.paxos_group_id)
      {
        ret = OB_INVALID_ARGUMENT;
        printf("ERROR_MESSAGE: paxos_group_id[%ld] must be positive number\n", args.paxos_group_id);
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(),
                                                               msgbuf.get_position(), args.paxos_group_id)))
      {
        printf("failed to serialize paxos_group_id, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = client.send_recv(OB_RS_TAKE_PAXOS_GROUP_ONLINE, MY_VERSION,
                                                     args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(),
                                                         msgbuf.get_position())))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          print_err_message(ret, false, args);
        }
        else
        {
          printf("take paxos group[%ld] online succeeded\n", args.paxos_group_id);
        }
      }
      if (OB_SUCCESS != ret)
      {
        printf("take paxos group[%ld] online failed, err=%d\n", args.paxos_group_id, ret);
      }
      return ret;
    }
    /*Exp: take cluster online*/
    int do_take_cluster_online(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      const int buff_size = static_cast<int> (sizeof(ObPacket) + 8);
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);
      if (0 >= args.cluster_id)
      {
        ret = OB_INVALID_ARGUMENT;
        printf("invalid param, cluster_id[%ld] must be positive number, err=%d\n", args.cluster_id, ret);
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(),
                                                               msgbuf.get_position(), args.cluster_id)))
      {
        printf("failed to serialize cluster_id, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = client.send_recv(OB_RS_TAKE_CLUSTER_ONLINE, MY_VERSION,
                                                     args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(),
                                                         msgbuf.get_position())))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          print_err_message(ret, true, args);
        }
        else
        {
          printf("take cluster[%ld] online succeeded\n", args.cluster_id);
        }
      }
      if (OB_SUCCESS != ret)
      {
        printf("take cluster[%ld] online failed, err=%d\n", args.cluster_id, ret);
      }
      return ret;
    }
    void print_err_message(int ret, bool is_cluster, Arguments &args)
    {
      printf("ERROR_MESSAGE: ");
      switch (ret)
      {
        case OB_NEW_PAXOS_GROUP_NUM_OUT_OF_RANGE:
          printf("new paxos group num[%ld] is out of the range\n", args.new_paxos_group_num);
          break;
        case OB_NEW_CLUSTER_NUM_OUT_OF_RANGE:
          printf("new cluster num[%ld] is out of the range\n", args.new_cluster_num);
          break;
        case OB_CURRENT_SYSTEM_MERGE_DOING:
          printf("system is merging, please wait merging done\n");
          break;
        case OB_UPS_MANAGER_NODE_INEXISTENT:
          if (is_cluster)
            printf("cluster[%ld] is not existent\n", args.cluster_id);
          else
            printf("paxos group[%ld] is not existent\n", args.paxos_group_id);
          break;
        case OB_CURRENT_PAXOS_GROUP_OFFLINE:
          printf("paxos group[%ld] has been taken offline, need not take it offline again\n", args.paxos_group_id);
          break;
        case OB_CURRENT_CLUSTER_OFFLINE:
          printf("cluster[%ld] has been taken offline, need not take it offline again\n", args.cluster_id);
          break;
        case OB_CURRENT_PAXOS_GROUP_ONLINE:
          printf("paxos group[%ld] has been taken online, need not take it online again and can't do delete\n", args.paxos_group_id);
          break;

        case OB_NOT_THE_LAST_IDX:
          printf("Paxos group id or cluster id is not the largest alive idx.\n");
          break;
        case OB_PAXOS_GROUP_NOT_OFFLINE_CURRENT_VERSION:
          printf("paxos group[%ld] has been taken offline, then need to finish merge\n", args.paxos_group_id);
          break;
        case OB_CLUSTER_NOT_OFFLINE_CURRENT_VERSION:
          printf("cluster[%ld] has been taken offline, then need to finish merge\n", args.cluster_id);
          break;
        case OB_CURRENT_CLUSTER_ONLINE:
          //printf("cluster[%ld] has been taken online, need not take it online again\n", args.cluster_id);
          printf("cluster[%ld] has been taken online, need not take it online again and can't do delete\n", args.cluster_id);
          break;
        case OB_UPS_REMAIN_COUNT_INVALID:
          printf("cluster[%ld] offline will lead to that remain ups do not elect new master\n", args.cluster_id);
          break;
        case OB_RS_REMAIN_COUNT_INVALID:
          printf("cluster[%ld] offline will lead to that remain rs do not elect new master\n", args.cluster_id);
          break;
        case OB_CURRENT_CLUSTER_HAS_MASTER_RS:
          printf("cluster[%ld] has master rs, and it can not be taken offline\n", args.cluster_id);
          break;
        case OB_CURRENT_PAXOS_ALETR_GROUP:
          printf("paxos group[%ld] has been alter group, need not take it offline\n", args.paxos_group_id);
          break;
        default:
          printf("unknow error code:[%d]\n", ret);
          break;
      }
    }
    //add 20160418:e
    int do_get_obi_role(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("get_obi_role...\n");
      static const int32_t MY_VERSION = 1;
      const int buff_size = static_cast<int> (sizeof(ObPacket) + 32);
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);

      if (OB_SUCCESS != (ret = client.send_recv(OB_GET_OBI_ROLE, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        ObiRole role;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to get obi role, err=%d\n", result_code.result_code_);
        }
        else if (OB_SUCCESS != (ret = role.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialized role, err=%d\n", ret);
        }
        else
        {
          printf("obi_role=%s\n", role.get_role_str());
        }
      }
      return ret;
    }

    int do_set_obi_role(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("set_obi_role...role=%d\n", args.obi_role.get_role());
      static const int32_t MY_VERSION = 1;
      const int buff_size = static_cast<int> (sizeof(ObPacket) + 32);
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);
      if (OB_SUCCESS != (ret = args.obi_role.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
      {
        printf("failed to serialize role, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = client.send_recv(OB_SET_OBI_ROLE, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to set obi role, err=%d\n", result_code.result_code_);
        }
        else
        {
          printf("Okay\n");
        }
      }
      return ret;
    }

    int do_rs_admin(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("do_rs_admin, cmd=%d...\n", args.command.pcode);
      static const int32_t MY_VERSION = 1;
      const int buff_size = static_cast<int> (sizeof(ObPacket) + 32);
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);
      // admin消息的内容为一个int32_t类型，定义在ob_root_admin_cmd.h中
      if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.command.pcode)))
      {
        printf("failed to serialize, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = client.send_recv(OB_RS_ADMIN, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d, timeout=%ld\n", ret, args.request_timeout_us);
      }
      else
      {
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to do_rs_admin, err=%d\n", result_code.result_code_);
        }
        else
        {
          printf("Okay\n");
        }
      }
      return ret;
    }

    int do_change_log_level(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("do_change_log_level, level=%d...\n", args.log_level);
      static const int32_t MY_VERSION = 1;
      const int buff_size = static_cast<int> (sizeof(ObPacket) + 32);
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);
      if (Arguments::INVALID_LOG_LEVEL == args.log_level)
      {
        printf("invalid log level, level=%d\n", args.log_level);
      }
      // change_log_level消息的内容为一个int32_t类型log_level
      else if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.log_level)))
      {
        printf("failed to serialize, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = client.send_recv(OB_CHANGE_LOG_LEVEL, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to change_log_level, err=%d\n", result_code.result_code_);
        }
        else
        {
          printf("Okay\n");
        }
      }
      return ret;
    }

    int do_rs_stat(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("do_rs_stat, key=%d...\n", args.stat_key);
      static const int32_t MY_VERSION = 1;
      char *buff = new(std::nothrow) char[OB_MAX_PACKET_LENGTH];
      ObDataBuffer msgbuf(buff, OB_MAX_PACKET_LENGTH);
      // rs_stat消息的内容为一个int32_t类型，定义在ob_root_stat_key.h中
      if (NULL == buff)
      {
        printf("no memory\n");
      }
      else if (0 > args.stat_key)
      {
        printf("invalid stat_key=%d\n", args.stat_key);
        print_stat_error_prompt(args);
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.stat_key)))
      {
        printf("failed to serialize, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = client.send_recv(OB_RS_STAT, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to do_rs_admin, err=%d\n", result_code.result_code_);
        }
        else
        {
          printf("%s\n", msgbuf.get_data()+msgbuf.get_position());
        }
      }
      if (NULL != buff)
      {
        delete [] buff;
        buff = NULL;
      }
      return ret;
    }
    int do_get_master_ups_config(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("get_master_ups_config...\n");
      static const int32_t MY_VERSION = 1;
      const int buff_size = static_cast<int> (sizeof(ObPacket) + 32);
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);

      if (OB_SUCCESS != (ret = client.send_recv(OB_GET_MASTER_UPS_CONFIG, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        int32_t master_master_ups_read_percent = 0;
        //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
        //int32_t slave_master_ups_read_percent = 0;
        //del:e
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to get obi role, err=%d\n", result_code.result_code_);
        }
        else if (OB_SUCCESS != (ret = serialization::decode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), &master_master_ups_read_percent)))
        {
          printf("failed to deserialize master_master_ups_read_percent");
        }
        //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
        /*
        else if (OB_SUCCESS != (ret = serialization::decode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), &slave_master_ups_read_percent)))
        {
          printf("failed to deserialize slave_master_ups_read_percent");
        }
        */
        //del:e
        else
        {
          //mod lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
          /*
          printf("master_master_ups_read_percentage=%d, slave_master_ups_read_percent=%d\n",
              master_master_ups_read_percent, slave_master_ups_read_percent);
          */
          printf("master_master_ups_read_percentage=%d\n", master_master_ups_read_percent);
          //mode:e
        }
      }
      return ret;
    }

    int do_set_master_ups_config(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;

      //[634]
      if ((0 > args.master_master_ups_read_percentage)
               || (100 < args.master_master_ups_read_percentage)
          //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
          /*
                  || ((-1 != args.slave_master_ups_read_percentage)
                    && (0 > args.slave_master_ups_read_percentage
                      || 100 < args.slave_master_ups_read_percentage))
                  */
          //del:e
          //add bingo[Paxps Cluster.Flow.UPS] 20170116:b
          || (0 != args.is_strong_consistent
              && 1 != args.is_strong_consistent)
          //add:e
          )
      {
        //add bingo[Paxos Cluster.Flow.UPS] 20170116:b
        //mod lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
        /*
        printf("invalid param, master_master_ups_read_percentage=%d, slave_master_ups_read_percentage=%d\n",
            args.master_master_ups_read_percentage, args.slave_master_ups_read_percentage);
            */
        printf("invalid param, master_master_ups_read_percentage=%d, is_strong_consistent=%d\n", args.master_master_ups_read_percentage, args.is_strong_consistent);
        //mod:e
        //add:e
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      //add bingo[Paxos Cluster.Flow.UPS] 20170221:b
      else if(0 == args.master_master_ups_read_percentage && 1 == args.is_strong_consistent)
      {
        printf("invalid param, master_master_ups_read_percentage=%d, is_strong_consistent=%d\n", args.master_master_ups_read_percentage, args.is_strong_consistent);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      //add:e
      else
      {
        //add bingo[Paxps Cluster.Flow.UPS] 20170116:b
        //mod lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
        /*
        printf("set_master_ups_config, master_master_ups_read_percentage=%d,read_salve_master_ups_percentage=%d...\n",
            args.master_master_ups_read_percentage, args.slave_master_ups_read_percentage);
        */
        printf("set_master_ups_config, master_master_ups_read_percentage=%d, is_strong_consistent=%d...\n", args.master_master_ups_read_percentage, args.is_strong_consistent);
        //mod:e
        //add:e

        static const int32_t MY_VERSION = 1;
        const int buff_size = OB_MAX_PACKET_LENGTH;
        char buff[buff_size];
        ObDataBuffer msgbuf(buff, buff_size);
        if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.master_master_ups_read_percentage)))
        {
          printf("failed to serialize read_percentage, err=%d\n", ret);
        }
        //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
        /*
        else if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.slave_master_ups_read_percentage)))
        {
          printf("failed to serialize read_percentage, err=%d\n", ret);
        }
        */
        //del:e
        //add bingo[Paxps Cluster.Flow.UPS] 20170116:b
        else if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.is_strong_consistent)))
        {
          printf("failed to serialize read consistency, err=%d\n", ret);
        }
        //add:e
        else if (OB_SUCCESS != (ret = client.send_recv(OB_SET_MASTER_UPS_CONFIG, MY_VERSION, args.request_timeout_us, msgbuf)))
        {
          printf("failed to send request, err=%d\n", ret);
        }
        else
        {
          ObResultCode result_code;
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            printf("failed to deserialize response, err=%d\n", ret);
          }
          else if (OB_SUCCESS != (ret = result_code.result_code_))
          {
            printf("failed to set master ups config, err=%d\n", result_code.result_code_);
          }
          else
          {
            printf("Okay\n");
          }
        }
      }
      return ret;
    }

    int do_cs_create_table(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      common::ObServer cs_addr;
      if ((1 > args.table_id) || (OB_INVALID_ID == args.table_id))
      {
        printf("invalid param, table_id=%lu\n", args.table_id);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (!cs_addr.set_ipv4_addr(args.rs_host, args.rs_port))
      {
        printf("invalid param, cs_host=%s cs_port=%d\n", args.rs_host, args.rs_port);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        printf("force cs create empty table cs_ip=%s cs_port=%d version=%ld table_id=%lu...\n",
               args.rs_host, args.rs_port, args.table_version, args.table_id);
        static const int32_t MY_VERSION = 1;
        const int buff_size = static_cast<int> (sizeof(ObPacket) + 128);
        char buff[buff_size];
        ObDataBuffer msgbuf(buff, buff_size);
        ObNewRange range;
        range.table_id_ = args.table_id;
        range.border_flag_.unset_inclusive_start();
        range.border_flag_.set_inclusive_end();
        range.set_whole_range();
        int64_t timeout_us = args.request_timeout_us;
        if (OB_SUCCESS != (ret = range.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          YYSYS_LOG(ERROR, "failed to serialize range, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(),
                                                                         msgbuf.get_position(), args.table_version)))
        {
          YYSYS_LOG(ERROR, "failed to serialize key_src, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = client.send_recv(OB_CS_CREATE_TABLE, MY_VERSION, timeout_us, msgbuf)))
        {
          YYSYS_LOG(WARN, "failed to send request, err=%d", ret);
        }
        else
        {
          ObResultCode result;
          int64_t pos = 0;
          if (OB_SUCCESS != (ret = result.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
          {
            YYSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
          }
          else if (OB_SUCCESS != result.result_code_)
          {
            ret = result.result_code_;
            YYSYS_LOG(ERROR, "failed to create tablet:rang[%s], ret[%d]", to_cstring(range), ret);
          }
        }
        printf("[%s] create empty table:cs[%s], table[%lu], version[%ld], ret[%d]\n", (OB_SUCCESS == ret) ? "SUCC" : "FAIL",
               cs_addr.to_cstring(), range.table_id_, args.table_version, ret);
      }
      return ret;
    }

    int do_set_ups_config(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      common::ObServer ups_addr;
      if (0 > args.ms_read_percentage || 100 < args.ms_read_percentage)
      {
        printf("invalid param, ms_read_percentage=%d\n", args.ms_read_percentage);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 > args.cs_read_percentage || 100 < args.cs_read_percentage)
      {
        printf("invalid param, cs_read_percentage=%d\n", args.cs_read_percentage);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == args.ups_ip)
      {
        printf("invalid param, ups_ip is NULL\n");
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 >= args.ups_port)
      {
        printf("invalid param, ups_port=%d\n", args.ups_port);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (!ups_addr.set_ipv4_addr(args.ups_ip, args.ups_port))
      {
        printf("invalid param, ups_host=%s ups_port=%d\n", args.ups_ip, args.ups_port);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        printf("set_ups_config, ups_ip=%s ups_port=%d ms_read_percentage=%d cs_read_percentage=%d...\n",
               args.ups_ip, args.ups_port, args.ms_read_percentage, args.cs_read_percentage);

        static const int32_t MY_VERSION = 1;
        const int buff_size = OB_MAX_PACKET_LENGTH;
        char buff[buff_size];
        ObDataBuffer msgbuf(buff, buff_size);

        if (OB_SUCCESS != (ret = ups_addr.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to serialize ups_addr, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.ms_read_percentage)))
        {
          printf("failed to serialize read_percentage, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.cs_read_percentage)))
        {
          printf("failed to serialize read_percentage, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = client.send_recv(OB_SET_UPS_CONFIG, MY_VERSION, args.request_timeout_us, msgbuf)))
        {
          printf("failed to send request, err=%d\n", ret);
        }
        else
        {
          ObResultCode result_code;
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            printf("failed to deserialize response, err=%d\n", ret);
          }
          else if (OB_SUCCESS != (ret = result_code.result_code_))
          {
            printf("failed to set obi config, err=%d\n", result_code.result_code_);
          }
          else
          {
            printf("Okay\n");
          }
        }
      }
      return ret;
    }

    int do_check_roottable(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      const int buff_size = OB_MAX_PACKET_LENGTH;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);

      common::ObServer cs_addr;
      printf("check root_table integrity...");
      if ((NULL == args.cs_ip) && (0 >= args.cs_port))
      {
      }
      else
      {
        if ((NULL == args.cs_ip) || (0 >= args.cs_port))
        {
          printf("invalid param, cs_host=%s cs_port=%d\n", args.cs_ip, args.cs_port);
          usage();
          ret = OB_INVALID_ARGUMENT;
        }
        else if (!cs_addr.set_ipv4_addr(args.cs_ip, args.cs_port))
        {
          printf("invalid param, cs_host=%s cs_port=%d\n", args.cs_ip, args.cs_port);
          usage();
          ret = OB_INVALID_ARGUMENT;
        }
      }
      printf("expect chunkserver addr:cs_addr=%s...\n", to_cstring(cs_addr));
      if (OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = cs_addr.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to serialize cs_addr, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = client.send_recv(OB_RS_CHECK_ROOTTABLE, MY_VERSION, args.request_timeout_us, msgbuf)))
        {
          printf("failed to send request, err = %d", ret);
        }
        else
        {
          ObResultCode result_code;
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            printf("failed to deserialize response code, err=%d", ret);
          }
          else if (OB_SUCCESS != (ret = result_code.result_code_))
          {
            printf("failed to check root_table integrity. err=%d", ret);
          }
          else
          {
            printf("Okay\n");
            bool is_integrity = false;
            if (OB_SUCCESS != (ret = serialization::decode_bool(
                                 msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), &is_integrity)))
            {
              printf("failed to decode check result, err=%d", ret);
            }
            else
            {
              printf("check result : roottable is %s integrity, expect cs=%s", is_integrity ? "" : "not", to_cstring(cs_addr));
            }
          }
        }
      }
      return ret;
    }

    int do_dump_cs_tablet_info(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      const int buff_size = OB_MAX_PACKET_LENGTH;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);

      common::ObServer cs_addr;
      if (NULL == args.cs_ip)
      {
        printf("invalid param, cs_ip is NULL\n");
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 >= args.cs_port)
      {
        printf("invalid param, cs_port is %d\n", args.cs_port);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (!cs_addr.set_ipv4_addr(args.cs_ip, args.cs_port))
      {
        printf("invalid param, cs_host=%s cs_port=%d\n", args.cs_ip, args.cs_port);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        printf("dump chunkserver tablet info, cs_ip=%s, cs_port=%d...\n", args.cs_ip, args.cs_port);
        if (OB_SUCCESS != (ret = cs_addr.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to serialize cs_addr, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = client.send_recv(OB_RS_DUMP_CS_TABLET_INFO, MY_VERSION, args.request_timeout_us, msgbuf)))
        {
          printf("failed to send request, err = %d", ret);
        }
        else
        {
          ObResultCode result_code;
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            printf("failed to deserialize response code, err=%d", ret);
          }
          else if (OB_SUCCESS != (ret = result_code.result_code_))
          {
            printf("failed to dump cs tablet info. err=%d", ret);
          }
          else
          {
            printf("Okay\n");
            int64_t tablet_num = 0;
            if (OB_SUCCESS != (ret = serialization::decode_vi64(
                                 msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), &tablet_num)))
            {
              printf("failed to decode tablet_num, err=%d", ret);
            }
            else
            {
              printf("tablet_num=%ld\n", tablet_num);
            }
          }
        }
      }
      return ret;
    }
    //add pangtianze [Paxos rs_election] 20150731:b
    int do_set_rs_leader(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      const int buff_size = OB_MAX_PACKET_LENGTH;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);
      common::ObServer rs_addr;
      if (NULL == args.ups_ip)
      {
        printf("invalid param, rs_ip is NULL\n");
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 >= args.ups_port)
      {
        printf("invalid param, rs_port is %d\n", args.cs_port);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (!rs_addr.set_ipv4_addr(args.ups_ip, args.ups_port))
      {
        printf("invalid param, rs_host=%s rs_port=%d\n", args.ups_ip, args.ups_port);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 != args.force_set_rs_leader && 1 != args.force_set_rs_leader)
      {
        printf("invalid param, force=%d\n", args.force_set_rs_leader);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        //printf("set rs leader, rs_ip=%s, rs_port=%d...\n", args.ups_ip, args.ups_port);
        printf("set rs leader, rs_ip=%s rs_port=%d force=%d...\n", args.ups_ip, args.ups_port, args.force_set_rs_leader);
        if (!rs_addr.is_valid())
        {
          printf("target rs_addr is invalid, rs_addr=%s\n", rs_addr.to_cstring());
          ret = OB_INVALID_ARGUMENT;
        }
        else if (OB_SUCCESS != (ret = rs_addr.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to serialize rs_addr, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.force_set_rs_leader)))
        {
          printf("failed to serialize int32, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = client.send_recv(OB_RS_ADMIN_SET_LEADER, MY_VERSION, args.request_timeout_us, msgbuf)))
        {
          printf("failed to send request, err = %d", ret);
        }
        else
        {
          ObResultCode result_code;
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            printf("failed to deserialize response code, err=%d", ret);
          }
          //mod chujiajia [Paxos rs_election] 20151105:b
          //else if (OB_SUCCESS != (ret = result_code.result_code_))
          //{
          //  printf("Error occur, please check target rootserver and try again. err=%d", ret);
          //}
          else if (OB_SUCCESS != (ret = result_code.result_code_))
          {
            switch (result_code.result_code_)
            {
              case OB_IS_ALREADY_THE_MASTER:
                printf("the new leader is same to the old leader, leader don't change! err=%d.\n", ret);
                break;
              case OB_NOT_MASTER:
                printf("the command is not send to the leader, please check command and try again. err=%d.\n", ret);
                break;
              case OB_RS_NOT_EXIST:
                printf("rootserver [%s] is not exist. err=%d.\n", rs_addr.to_cstring(), ret);
                break;
              default:
                printf("the command didn't execute succ, please check command and try again. err=%d.\n", ret);
                break;
            }
          }
          //add:e
          else
          {
            printf("Okay, begin to set new leader, please check the result after a few seconds.\n");
          }
        }
      }
      return ret;
    }
    //add:e
    int do_change_ups_master(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      const int buff_size = OB_MAX_PACKET_LENGTH;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);

      common::ObServer ups_addr;
      if (NULL == args.ups_ip)
      {
        printf("invalid param, ups_ip is NULL\n");
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 >= args.ups_port)
      {
        printf("invalid param, ups_port=%d\n", args.ups_port);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (!ups_addr.set_ipv4_addr(args.ups_ip, args.ups_port))
      {
        printf("invalid param, ups_host=%s ups_port=%d\n", args.ups_ip, args.ups_port);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 != args.force_change_ups_master && 1 != args.force_change_ups_master)
      {
        printf("invalid param, force=%d\n", args.force_change_ups_master);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        printf("change_ups_master, ups_ip=%s ups_port=%d force=%d...\n", args.ups_ip, args.ups_port, args.force_change_ups_master);
        if (!ups_addr.is_valid())
        {
          printf("target ups_addr is invalid, ups_addr=%s\n", ups_addr.to_cstring());
          ret = OB_INVALID_ARGUMENT;
        }
        else if (OB_SUCCESS != (ret = ups_addr.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to serialize ups_addr, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.force_change_ups_master)))
        {
          printf("failed to serialize int32, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = client.send_recv(OB_CHANGE_UPS_MASTER, MY_VERSION, args.request_timeout_us, msgbuf)))
        {
          printf("failed to send request, err=%d\n", ret);
        }
        else
        {
          ObResultCode result_code;
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            printf("failed to deserialize response, err=%d\n", ret);
          }
          else if (OB_SUCCESS != (ret = result_code.result_code_))
          {
            if (OB_UPS_TRANS_RUNNING == ret)
            {
              printf("failed to change ups master because ups is busy, err=%d\n", result_code.result_code_);
            }
            else
            {
              printf("failed to change ups master, err=%d\n", result_code.result_code_);
            }
          }
          else
          {
            printf("Okay\n");
          }
        }
      }
      return ret;
    }

    int do_get_row_checksum(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      const int buff_size = static_cast<int> (sizeof(ObPacket) + 32);
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);
      ObRowChecksum row_checksum;
      if (args.tablet_version <= 0)
      {
        printf("invalid tablet_version\n");
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.tablet_version)))
      {
        printf("failed to serialize, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.table_id)))
      {
        printf("failed to serialize, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = client.send_recv(OB_GET_ROW_CHECKSUM, MY_VERSION, args.request_timeout_us*20, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        int64_t pos = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to get table row_checksum, err=%d\n", result_code.result_code_);
        }
        else if (OB_SUCCESS != (ret = row_checksum.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else
        {
          printf("Okay\n%s\n", to_cstring(row_checksum));
        }
      }
      return ret;
    }

    int do_import_tablets(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("import_tablets...table_id=%lu\n", args.table_id);
      static const int32_t MY_VERSION = 1;
      const int buff_size = static_cast<int> (sizeof(ObPacket) + 32);
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);
      int64_t import_version = 0;
      if (OB_INVALID_ID == args.table_id)
      {
        printf("invalid table_id\n");
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.table_id)))
      {
        printf("failed to serialize, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), import_version)))
      {
        printf("failed to serialize, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = client.send_recv(OB_CS_IMPORT_TABLETS, MY_VERSION, args.request_timeout_us*20, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        int64_t pos = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to import tablets, err=%d\n", result_code.result_code_);
        }
        else
        {
          printf("Okay\n");
        }
      }
      return ret;
    }

    int do_print_schema(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("do_print_schema...\n");
      static const int32_t MY_VERSION = 1;
      char *buff = new(std::nothrow) char[OB_MAX_PACKET_LENGTH];
      ObDataBuffer msgbuf(buff, OB_MAX_PACKET_LENGTH);
      int64_t schema_version = 0;
      if (NULL == buff)
      {
        printf("no memory\n");
      }
      else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), schema_version)))
      {
        YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = client.send_recv(args.command.pcode, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        //common::ObSchemaManagerV2 schema_manager;
        int64_t pos = 0;
        //add liumz, bugfix: [alloc memory for ObSchemaManagerV2 in heap, not on stack]20160712:b
        common::ObSchemaManagerV2 *schema_manager = OB_NEW(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER);
        if (NULL == schema_manager)
        {
          YYSYS_LOG(WARN, "fail to new schema_manager.");
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        //add:e
        else if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to fetch schema, err=%d\n", result_code.result_code_);
        }
        else if (OB_SUCCESS != (ret = schema_manager->deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
        {
          printf("failed to deserialize schema manager, err=%d\n", ret);
        }

        //add zhaoqiong [Schema Manager] 20150327:b
        while (OB_SUCCESS == ret && !schema_manager->is_completion())
        {
          int64_t new_table_begin = -1;
          int64_t new_column_begin = -1;
          schema_manager->get_next_start_pos(new_table_begin,new_column_begin);
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), schema_manager->get_version())))
          {
            YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
          }
          else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), new_table_begin)))
          {
            YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
          }
          else if (OB_SUCCESS != (ret = common::serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), new_column_begin)))
          {
            YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
          }
          else if (OB_SUCCESS != (ret = client.send_recv(OB_FETCH_SCHEMA_NEXT, MY_VERSION, args.request_timeout_us, msgbuf)))
          {
            printf("failed to send request, err=%d\n", ret);
          }
          else
          {
            pos = 0;
            if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
            {
              printf("failed to deserialize response, err=%d\n", ret);
            }
            else if (OB_SUCCESS != (ret = result_code.result_code_))
            {
              printf("failed to fetch schema, err=%d\n", result_code.result_code_);
            }
            else if (OB_SUCCESS != (ret = schema_manager->deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
            {
              printf("failed to deserialize schema manager, err=%d\n", ret);
            }
          }
        }
        //add:e
        //mod zhaoqiong [Schema Manager] 20150327:b
        //else
        if (OB_SUCCESS == ret)
          //mod:e
        {
          //schema_manager.print(stdout);
          if (common::FileDirectoryUtils::exists(args.location))
          {
            printf("file already exist. fail to write. file_name=%s\n", args.location);
          }
          else
          {
            printf("write schema to file. file_name=%s\n", args.location);
            ret = schema_manager->write_to_file(args.location);
            if (OB_SUCCESS != ret)
            {
              YYSYS_LOG(WARN, "fail to write schema to file. file_name=%s\n", args.location);
            }
          }
        }
        //add liumz, bugfix: [alloc memory for ObSchemaManagerV2 in heap, not on stack]20160712:b
        if (schema_manager != NULL)
        {
          OB_DELETE(ObSchemaManagerV2, ObModIds::OB_RS_SCHEMA_MANAGER, schema_manager);
        }
        //add:e
      }
      if (NULL != buff)
      {
        delete [] buff;
        buff = NULL;
      }
      return ret;
    }

    int do_print_root_table(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("do_print_root_table, table_id=%lu...\n", args.table_id);
      static const int32_t MY_VERSION = 1;
      char *buff = new(std::nothrow) char[OB_MAX_PACKET_LENGTH];
      ObDataBuffer msgbuf(buff, OB_MAX_PACKET_LENGTH);
      ObObj objs[1];
      ObCellInfo cell;
      cell.table_id_ = args.table_id;
      cell.column_id_ = 0;
      objs[0].set_int(0);
      cell.row_key_.assign(objs, 1);
      ObGetParam get_param;
      if (OB_INVALID_ID == args.table_id)
      {
        printf("invalid table_id\n");
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = get_param.add_cell(cell)))
      {
        printf("failed to add cell to get_param, err=%d\n", ret);
      }
      else if (NULL == buff)
      {
        printf("no memory\n");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (OB_SUCCESS != (ret = get_param.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
      {
        printf("failed to serialize get_param, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = client.send_recv(args.command.pcode, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        common::ObScanner scanner;
        int64_t pos = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to get root table, err=%d\n", result_code.result_code_);
        }
        else if (OB_SUCCESS != (ret = scanner.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
        {
          printf("failed to deserialize scanner, err=%d\n", ret);
        }
        else
        {
          print_root_table(stdout, scanner);
        }
      }
      if (NULL != buff)
      {
        delete [] buff;
        buff = NULL;
      }
      return ret;
    }

    static int serialize_servers(const char* server_list, ObDataBuffer &msgbuf)
    {
      int ret = OB_SUCCESS;
      char* servers = NULL;
      char* servers2 = NULL;
      if (NULL == server_list)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == (servers = strdup(server_list))
               || NULL == (servers2 = strdup(server_list)))
      {
        YYSYS_LOG(ERROR, "no memory");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        char *saveptr = NULL;
        const char* delim = "+";
        // count
        int32_t count = 0;
        char *server = strtok_r(servers, delim, &saveptr);
        while(NULL != server)
        {
          count++;
          server = strtok_r(NULL, delim, &saveptr);
        }
        if (0 >= count)
        {
          ret = OB_INVALID_ARGUMENT;
          YYSYS_LOG(ERROR, "no valid server, servers=%s", server_list);
        }
        else if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), count)))
        {
          YYSYS_LOG(ERROR, "serialize error");
        }
        else
        {
          // serialize
          ObServer addr;
          const int ignored_port = 1;
          server = strtok_r(servers2, delim, &saveptr);
          while(NULL != server)
          {
            if (!addr.set_ipv4_addr(server, ignored_port))
            {
              ret = OB_INVALID_ARGUMENT;
              YYSYS_LOG(ERROR, "invalid addr, server=%s", server);
              break;
            }
            else if (OB_SUCCESS != (ret = addr.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
            {
              YYSYS_LOG(ERROR, "serialize error");
              break;
            }
            else
            {
              YYSYS_LOG(INFO, "server=%s", server);
            }
            server = strtok_r(NULL, delim, &saveptr);
          }
        }
      }
      if (NULL != servers)
      {
        free(servers);
        servers = NULL;
      }
      if (NULL != servers2)
      {
        free(servers2);
        servers2 = NULL;
      }
      return ret;
    }

    int do_restart_servers(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("do_restart_servers, server_list=%s cancel=%d...\n",
             args.server_list, args.flag_cancel);
      static const int32_t MY_VERSION = 1;
      char *buff = new(std::nothrow) char[OB_MAX_PACKET_LENGTH];
      ObDataBuffer msgbuf(buff, OB_MAX_PACKET_LENGTH);

      if (NULL == args.server_list)
      {
        printf("invalid NULL server_list\n");
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == buff)
      {
        printf("no memory\n");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }

      if(OB_SUCCESS == ret)
      {
        if(0 == strcmp(args.server_list, "all"))
        {
          ret = serialization::encode_bool(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), true);
        }
        else
        {
          ret = serialization::encode_bool(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), false);
          if(OB_SUCCESS == ret)
          {
            if(OB_SUCCESS != (ret = serialize_servers(args.server_list, msgbuf)))
            {
              printf("failed to serialize get_param, err=%d\n", ret);
            }
          }
        }
      }

      if(OB_SUCCESS == ret)
      {
        if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.flag_cancel)))
        {
          YYSYS_LOG(ERROR, "serialize error");
        }
        else if (OB_SUCCESS != (ret = client.send_recv(args.command.pcode, MY_VERSION, args.request_timeout_us,
                                                       msgbuf)))
        {
          printf("failed to send request, err=%d\n", ret);
        }
        else
        {
          ObResultCode result_code;
          int64_t pos = 0;
          if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
          {
            printf("failed to deserialize response, err=%d\n", ret);
          }
          else if (OB_SUCCESS != (ret = result_code.result_code_))
          {
            printf("failed to get restart servers, err=%d\n", result_code.result_code_);
          }
          else
          {
            printf("Okay\n");
          }
        }
      }
      if (NULL != buff)
      {
        delete [] buff;
        buff = NULL;
      }
      return ret;
    }

    int do_shutdown_servers(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("do_shutdown_servers, server_list=%s cancel=%d...\n",
             args.server_list, args.flag_cancel);
      static const int32_t MY_VERSION = 1;
      char *buff = new(std::nothrow) char[OB_MAX_PACKET_LENGTH];
      ObDataBuffer msgbuf(buff, OB_MAX_PACKET_LENGTH);

      if (NULL == args.server_list)
      {
        printf("invalid NULL server_list\n");
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (NULL == buff)
      {
        printf("no memory\n");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else if (OB_SUCCESS != (ret = serialize_servers(args.server_list, msgbuf)))
      {
        printf("failed to serialize get_param, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(),
                                                               msgbuf.get_position(), args.flag_cancel)))
      {
        YYSYS_LOG(ERROR, "serialize error");
      }
      else if (OB_SUCCESS != (ret = client.send_recv(args.command.pcode, MY_VERSION, args.request_timeout_us,
                                                     msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        int64_t pos = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_position(), pos)))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to get shut down servers, err=%d\n", result_code.result_code_);
        }
        else
        {
          printf("Okay\n");
        }
      }
      if (NULL != buff)
      {
        delete [] buff;
        buff = NULL;
      }
      return ret;
    }

    int do_check_tablet_version(ObBaseClient &client, Arguments &args)
    {
      int err = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      const int buff_size = OB_MAX_PACKET_LENGTH;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);

      if (0 >= args.tablet_version)
      {
        printf("invalid param. tablet_version is %ld", args.tablet_version);
        usage();
        err = OB_INVALID_ARGUMENT;
      }
      else
      {
        if (OB_SUCCESS != (err = serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(),
                                                            msgbuf.get_position(), args.tablet_version)))
        {
          printf("fail to encode tablet_version to buf. err=%d", err);
        }
        else if (OB_SUCCESS != (err = client.send_recv(OB_RS_CHECK_TABLET_MERGED, MY_VERSION, args.request_timeout_us, msgbuf)))
        {
          printf("failed to send request. err=%d", err);
        }
        else
        {
          ObResultCode result_code;
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (err = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            printf("failed to deserialize response code, err=%d", err);
          }
          else if (OB_SUCCESS != (err = result_code.result_code_))
          {
            printf("failed to check tablet version. err=%d", err);
          }
          else
          {
            int64_t is_merged = 0;
            if (OB_SUCCESS != (err = serialization::decode_vi64(msgbuf.get_data(), msgbuf.get_capacity(),
                                                                msgbuf.get_position(), &is_merged)))
            {
              printf("failed to deserialize is_merged, err=%d", err);
            }
            else
            {
              if (0 == is_merged)
              {
                printf("OKay, check_version=%ld, already have some tablet not reach this version\n",args.tablet_version);
              }
              else
              {
                printf("OKay, check_version=%ld, all tablet reach this version\n", args.tablet_version);
              }
            }
          }
        }
      }
      return err;
    }

    int do_split_tablet(ObBaseClient &client, Arguments &args)
    {
      int err = OB_SUCCESS;
      printf("for split tablet...\n");
      static const int32_t MY_VERSION = 1;
      const int buff_size = OB_MAX_PACKET_LENGTH;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);

      if (OB_SUCCESS != (err = client.send_recv(OB_RS_SPLIT_TABLET, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request. err=%d", err);
      }
      else
      {
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (err = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response code, err=%d", err);
        }
        else if (OB_SUCCESS != (err = result_code.result_code_))
        {
          printf("failed to split tablet. err=%d", err);
        }
        else
        {
          printf("OKay");
        }
      }
      return err;
    }

    int do_set_config(ObBaseClient &client, Arguments &args)
    {
      int err = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      char buf[OB_MAX_PACKET_LENGTH];
      ObString str = ObString::make_string(args.config_str);
      int64_t pos = 0;
      if (strlen(args.config_str) <= 0)
      {
        printf("ERROR: config string not specified.\n");
        printf("\tset_config -o config_name=config_value[,config_name2=config_value2[,...]]\n");
        err = OB_ERROR;
      }
      else if (OB_SUCCESS != (err = str.serialize(buf, sizeof (buf), pos)))
      {
        printf("Serialize config string failed! ret: [%d]", err);
      }
      else
      {
        printf("config_str: %s\n", args.config_str);

        ObDataBuffer msgbuf(buf, sizeof (buf));
        msgbuf.get_position() = pos;
        if (OB_SUCCESS != (err = client.send_recv(OB_SET_CONFIG, MY_VERSION, args.request_timeout_us, msgbuf)))
        {
          printf("failed to send request. err=%d\n", err);
        }
        else
        {
          ObResultCode result_code;
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (err = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            printf("failed to deserialize response code, err=%d\n", err);
          }
          else if (OB_SUCCESS != (err = result_code.result_code_))
          {
            printf("failed to set config. err=%d\n", err);
          }
          else
          {
            printf("OKay\n");
          }
        }
      }
      return err;
    }

    int do_get_config(ObBaseClient &client, Arguments &args)
    {
      int err = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      char buf[OB_MAX_PACKET_LENGTH];
      ObDataBuffer msgbuf(buf, sizeof (buf));
      msgbuf.get_position() = 0;
      if (OB_SUCCESS != (err = client.send_recv(OB_GET_CONFIG, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request. err=%d\n", err);
      }
      else
      {
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (err = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response code, err=%d\n", err);
        }
        else if (OB_SUCCESS != (err = result_code.result_code_))
        {
          printf("failed to set config. err=%d\n", err);
        }
        else
        {
          static const int HEADER_LENGTH = static_cast<int> (sizeof(uint32_t)) +  static_cast<int> (sizeof (uint64_t));
          uint64_t length = *reinterpret_cast<uint64_t *>(msgbuf.get_data() + msgbuf.get_position() + sizeof (uint32_t));
          msgbuf.get_position() += HEADER_LENGTH;
          printf("%.*s", static_cast<int>(length), msgbuf.get_data() + msgbuf.get_position());
        }
      }
      return err;
    }
    int do_change_table_id(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      const int buff_size = OB_MAX_PACKET_LENGTH;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);

      if (0 >= args.table_id)
      {
        printf("invalid param, table_id=%ld\n", args.table_id);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        printf("change_table_id, table_id=%ld...\n", args.table_id);
        if (OB_SUCCESS != (ret = serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.table_id)))
        {
          printf("failed to serialize table id, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = client.send_recv(OB_CHANGE_TABLE_ID, MY_VERSION, args.request_timeout_us, msgbuf)))
        {
          printf("failed to send request, err=%d\n", ret);
        }
        else
        {
          ObResultCode result_code;
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            printf("failed to deserialize response, err=%d\n", ret);
          }
          else if (OB_SUCCESS != (ret = result_code.result_code_))
          {
            printf("failed to change table id, err=%d\n", result_code.result_code_);
          }
          else
          {
            printf("Okay\n");
          }
        }
      }
      return ret;
    }

    int do_import(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      const int buff_size = OB_MAX_PACKET_LENGTH;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);
      ObString table_name;
      uint64_t table_id;
      ObString uri;
      int64_t timeout = 120*1000L*1000L;//120s

      if (args.request_timeout_us > timeout)
      {
        timeout = args.request_timeout_us;
      }

      if (args.argc != 4)
      {
        printf("wrong cmd format, please use import <tablename> <table_id> <uri>\n");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 == (table_id = strtoull(args.argv[2], 0, 10)))
      {
        printf("table id must not 0");
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        table_name.assign_ptr(args.argv[1], static_cast<ObString::obstr_size_t>(strlen(args.argv[1])));
        uri.assign_ptr(args.argv[3], static_cast<ObString::obstr_size_t>(strlen(args.argv[3])));
        printf("import table_name=%.*s table_id=%lu uri=%.*s\n", table_name.length(), table_name.ptr(), table_id, uri.length(), uri.ptr());
        if (OB_SUCCESS != (ret = table_name.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to serialize table name, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = serialization::encode_i64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), table_id)))
        {
          printf("failed to serialize table id, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = uri.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to serialize table name, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = client.send_recv(OB_RS_ADMIN_START_IMPORT, MY_VERSION, timeout, msgbuf)))
        {
          printf("failed to send request, err=%d\n", ret);
        }
        else
        {
          ObResultCode result_code;
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            printf("failed to deserialize response, err=%d\n", ret);
          }
          else if (OB_SUCCESS != (ret = result_code.result_code_))
          {
            printf("failed to import table, err=%d\n", result_code.result_code_);
          }
          else
          {
            printf("Okay\n");
          }
        }
      }
      return ret;
    }

    int do_kill_import(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      const int buff_size = OB_MAX_PACKET_LENGTH;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);
      ObString table_name;
      uint64_t table_id;
      int64_t timeout = 120*1000L*1000L;//120s

      if (args.request_timeout_us > timeout)
      {
        timeout = args.request_timeout_us;
      }


      if (args.argc != 3)
      {
        printf("wrong cmd format, please use import <tablename> <table_id>\n");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 == (table_id = strtoull(args.argv[2], 0, 10)))
      {
        printf("table id must not 0");
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        table_name.assign_ptr(args.argv[1], static_cast<ObString::obstr_size_t>(strlen(args.argv[1])));
        printf("kill import table_name=%.*s table_id=%lu\n", table_name.length(), table_name.ptr(), table_id);
        if (OB_SUCCESS != (ret = table_name.serialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to serialize table name, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = serialization::encode_i64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), table_id)))
        {
          printf("failed to serialize table id, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = client.send_recv(OB_RS_ADMIN_START_KILL_IMPORT, MY_VERSION, timeout, msgbuf)))
        {
          printf("failed to send request, err=%d\n", ret);
        }
        else
        {
          ObResultCode result_code;
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            printf("failed to deserialize response, err=%d\n", ret);
          }
          else if (OB_SUCCESS != (ret = result_code.result_code_))
          {
            printf("failed to kill import table, err=%d\n", result_code.result_code_);
          }
          else
          {
            printf("Okay\n");
          }
        }
      }
      return ret;
    }
    int do_create_table_for_emergency(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("force to create table for emergency, table_id=%ld\n", args.table_id);
      static const int32_t MY_VERSION = 1;
      const int buff_size = static_cast<int> (sizeof(ObPacket) + 32);
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);
      if (OB_SUCCESS != (ret = serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.table_id)))
      {
        printf("failed to serialize role, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = client.send_recv(OB_FORCE_CREATE_TABLE_FOR_EMERGENCY, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to create table for emergency, err=%d\n", result_code.result_code_);
        }
        else
        {
          printf("Okay\n");
        }
      }
      return ret;
    }
    int do_drop_table_for_emergency(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("force to drop table for emergency, table_id=%ld\n", args.table_id);
      static const int32_t MY_VERSION = 1;
      const int buff_size = static_cast<int> (sizeof(ObPacket) + 32);
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);
      if (OB_SUCCESS != (ret = serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.table_id)))
      {
        printf("failed to serialize role, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = client.send_recv(OB_FORCE_DROP_TABLE_FOR_EMERGENCY, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to drop table for emergency, err=%d\n", result_code.result_code_);
        }
        else
        {
          printf("Okay\n");
        }
      }
      return ret;
    }

    int read_root_table_point(ObBaseClient &client, Arguments &args)
    {
      UNUSED(client);
      int ret = OB_SUCCESS;
      ObTabletInfoManager *tablet_manager = OB_NEW(ObTabletInfoManager, ObModIds::OB_RS_TABLET_MANAGER);
      ObRootTable2 *root_table  = OB_NEW(ObRootTable2, ObModIds::OB_RS_ROOT_TABLE, tablet_manager);
      if (tablet_manager != NULL && root_table != NULL)
      {
        printf("read_from_file.");
        if (OB_SUCCESS == root_table->read_from_file(args.location))
        {
          root_table->dump();
        }
      }
      else
      {
        YYSYS_LOG(WARN, "obmalloc root table or tablet manager failed");
      }
      if (root_table != NULL)
      {
        OB_DELETE(ObRootTable2, ObModIds::OB_RS_ROOT_TABLE, root_table);
      }
      if (tablet_manager != NULL)
      {
        OB_DELETE(ObTabletInfoManager, ObModIds::OB_RS_TABLET_MANAGER, tablet_manager);
      }
      return ret;
    }
    int do_force_cs_report(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("force cs report...\n");
      static const int32_t MY_VERSION = 1;
      const int buff_size = static_cast<int> (sizeof(ObPacket) + 32);
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);

      if (OB_SUCCESS != (ret = client.send_recv(OB_RS_FORCE_CS_REPORT, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        ObiRole role;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to request cs report, err=%d\n", result_code.result_code_);
        }
      }
      return ret;
    }

    //add peiouya [MultiUPS] [PARTITION_LOCK_FUCTION] 20150609:b
    int do_major_freeze(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("major_freeze...\n");
      static const int32_t MY_VERSION = 1;
      const int buff_size = static_cast<int> (sizeof(ObPacket) + 32);
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);

      if (OB_SUCCESS != (ret = client.send_recv(OB_UPS_ASYNC_MAJOR_FREEZE_MEMTABLE, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          //mod peiouya [MultiUPS] [PARTITION_LOCK_FUCTION] 20150811:b
          if (OB_OP_NOT_ALLOW == ret)
          {
            printf("not allowed to do freeze, maybe system is freezing or merging. err=%d\n", result_code.result_code_);
          }
          else
          {
            printf("failed to request major_freeze, err=%d\n", result_code.result_code_);
          }
          //mod 20150811:e
        }
      }
      //add 20150609:e

      return ret;
    }

    int do_minor_freeze(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      printf("minor_freeze...\n");
      static const int32_t MY_VERSION = 1;
      const int buff_size = static_cast<int> (sizeof(ObPacket) + 32);
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);

      if (OB_SUCCESS != (ret = client.send_recv(OB_UPS_MINOR_FREEZE_MEMTABLE, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          if (OB_OP_NOT_ALLOW == ret)
          {
            printf("not allowed to do freeze, maybe system is freezing or merging. err=%d\n", result_code.result_code_);
          }
          else
          {
            printf("failed to request minor_freeze, err=%d\n", result_code.result_code_);
          }
        }
      }
      return ret;
    }

    //add chujiajia [Paxos rs_election] 20151102:b
    int do_change_rs_paxos_number(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      const int buff_size = OB_MAX_PACKET_LENGTH;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);
      if (0 >= args.rs_paxos_number)
      {
        printf("invalid param, rs_paxos_num=%d\n", args.rs_paxos_number);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        printf("change_rs_paxos_num, rs_paxos_num=%d...\n", args.rs_paxos_number);
        if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.rs_paxos_number)))
        {
          printf("failed to serialize rs_paxos_num, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = client.send_recv(OB_CHANGE_RS_PAXOS_NUMBER, MY_VERSION, args.request_timeout_us, msgbuf)))
        {
          printf("failed to send request, err=%d\n", ret);
        }
        else
        {
          ObResultCode result_code;
          //add pangtianze [Paxos rs_election] 20161010:b
          int32_t current_max_deploy_rs_count = 0;
          int32_t old_max_deploy_rs_count = 0;
          int32_t alive_rs_count = 0;
          //add:e
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            printf("failed to deserialize response, err=%d\n", ret);
          }
          //add pangtianze [Paxos rs_election] 20161010:b
          else if (OB_SUCCESS != (ret = serialization::decode_vi32(msgbuf.get_data(), msgbuf.get_capacity(),
                                                                   msgbuf.get_position(), &current_max_deploy_rs_count)))
          {
            YYSYS_LOG(WARN,"failed to deserialize current_rs_paxos_num, ret=%d", ret);
          }
          else if (OB_SUCCESS != (ret = serialization::decode_vi32(msgbuf.get_data(), msgbuf.get_capacity(),
                                                                   msgbuf.get_position(), &old_max_deploy_rs_count)))
          {
            YYSYS_LOG(WARN,"failed to deserialize old_rs_paxos_num, ret=%d", ret);
          }
          else if (OB_SUCCESS != (ret = serialization::decode_vi32(msgbuf.get_data(), msgbuf.get_capacity(),
                                                                   msgbuf.get_position(), &alive_rs_count)))
          {
            YYSYS_LOG(WARN,"failed to deserialize alive_rs_count, ret=%d", ret);
          }
          else if (OB_SUCCESS != (ret = result_code.result_code_))
          {
            switch (result_code.result_code_)
            {
              case OB_INVALID_PAXOS_NUM:
                //add bingo [Paxos rs management] 201170217:b
                if(args.rs_paxos_number == 2 && alive_rs_count == 1)
                {
                  printf("receive new paxos number:%d, but the second rs should be online first, alive_rs_count still is %d, err=%d.\n",
                         args.rs_paxos_number,alive_rs_count,ret);
                }
                //add:e
                else if(args.rs_paxos_number <= (alive_rs_count / 2) ||
                        args.rs_paxos_number >= (alive_rs_count * 2) ||
                        args.rs_paxos_number > OB_MAX_RS_COUNT)
                {
                  printf("receive invalid new paxos number:%d, it should be in (%d,%d), err=%d.\n",
                         args.rs_paxos_number, alive_rs_count/2, alive_rs_count*2, ret);
                }
                else if (args.rs_paxos_number != alive_rs_count && 0 != alive_rs_count && 1 != args.rs_paxos_number)
                {
                  printf("new paxos number must be equal to alive rootserver count, but new_paxos_num=%d, rs_count=%d, err=%d.\n",
                         args.rs_paxos_number, alive_rs_count, ret);
                }
                break;
              case OB_RS_NOT_EXIST:
                printf("some slave rootserver maybe not get new paxos number, please check and do again!, err=%d\n", ret);
                break;
              default:
                printf("error occurred. err=%d\n", ret);
                break;
            }
          }
          else if (1 == args.rs_paxos_number)
          {
            //mod bingo [Paxos rs management] 20170217:b
            printf("change paxos number [%d->1], slave rootserver should be offine soon, please check and kill them if still online\n", old_max_deploy_rs_count);
            //mod:e
          }
          else if (1 == old_max_deploy_rs_count)
          {
            //mod bingo [Paxos rs management] 20170217:b
            printf("change paxos number [1->%d], please check alive servers and the service\n", args.rs_paxos_number);
            //mod:e
          }
          //add:e
          else
          {
            printf("change rootserver paxos number [%d->%d] over, Please do 'rs_paxos_num' to see if the new paxos number is OK.\n"
                   , old_max_deploy_rs_count, current_max_deploy_rs_count);
          }
        }
      }
      return ret;
    }
    //add:e
    //add chujiajia [Paxos rs_election] 20151225:b
    int do_change_ups_quorum_scale(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      const int buff_size = OB_MAX_PACKET_LENGTH;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);
      int64_t paxos_group_id = args.paxos_group_id;
      if (paxos_group_id < 0)
      {
        printf("invalid param, paxos_group_id=%ld\n", paxos_group_id);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      if (0 >= args.ups_quorum_scale)
      {
        printf("invalid param, ups_quorum_scale=%d\n", args.ups_quorum_scale);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else if (0 != args.force_set_quorum_scale && 1 != args.force_set_quorum_scale)
      {
        printf("invalid param, force=%d\n", args.force_set_quorum_scale);
        usage();
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        bool force = false;
        if (1 == args.force_set_quorum_scale)
        {
          force = true;
        }
        //printf("change_ups_quorum_scale, new_quorum_num=%d...\n", args.ups_quorum_scale);
        printf("changing ups_quorum_scale of paxos group[%ld], new_quorum_num=[%d]...\n", args.paxos_group_id, args.ups_quorum_scale);
        if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.ups_quorum_scale)))
        {
          printf("failed to serialize quorum scale, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.paxos_group_id)))
        {
          printf("failed to serialize paxos group id, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = serialization::encode_bool(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), force)))
        {
          printf("failed to serialize force flag, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = client.send_recv(OB_CHANGE_UPS_QUORUM_SCALE, MY_VERSION, args.request_timeout_us, msgbuf)))
        {
          printf("failed to send request, err=%d\n", ret);
        }
        else
        {
          ObResultCode result_code;
          //add pangtianze [Paxos rs_election] 20161010:b
          int32_t current_ups_quorum_scale = 0;
          int32_t old_ups_quorum_scale = 0;
          int32_t alive_ups_count = 0;
          //add:e
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            printf("failed to deserialize response, err=%d\n", ret);
          }
          //add pangtianze [Paxos rs_election] 20161010:b
          else if (OB_SUCCESS != (ret = serialization::decode_vi32(msgbuf.get_data(), msgbuf.get_capacity(),
                                                                   msgbuf.get_position(), &current_ups_quorum_scale)))
          {
            YYSYS_LOG(WARN,"failed to deserialize current_max_deploy_ups_count, ret=%d", ret);
          }
          else if (OB_SUCCESS != (ret = serialization::decode_vi32(msgbuf.get_data(), msgbuf.get_capacity(),
                                                                   msgbuf.get_position(), &old_ups_quorum_scale)))
          {
            YYSYS_LOG(WARN,"failed to deserialize old_max_deploy_ups_count, ret=%d", ret);
          }
          else if (OB_SUCCESS != (ret = serialization::decode_vi32(msgbuf.get_data(), msgbuf.get_capacity(),
                                                                   msgbuf.get_position(), &alive_ups_count)))
          {
            YYSYS_LOG(WARN,"failed to deserialize alive_ups_count, ret=%d", ret);
          }
          else if (OB_SUCCESS != (ret = result_code.result_code_))
          {
            // switch (result_code.result_code_)
            // {
            //   //mod add bingo [Paxos ups management] 20170217:b
            //   //case OB_INVALID_PAXOS_NUM:
            //   case OB_INVALID_QUORUM_SCALE:
            //     if(args.ups_quorum_scale == 2 && alive_ups_count == 1){
            //       printf("receive new quorum scale:%d, but the second ups should be online first, alive_ups_count still is %d, err=%d.\n",
            //            args.ups_quorum_scale,alive_ups_count,ret);
            //     }
            //     //mod add:e
            //     else if(args.ups_quorum_scale <= (alive_ups_count / 2) ||
            //        args.ups_quorum_scale >= (alive_ups_count * 2) ||
            //        args.ups_quorum_scale > OB_MAX_UPS_COUNT)
            //     {
            //       printf("receive invalid new quorum scale:%d, it should be in (%d,%d), err=%d.\n",
            //              args.ups_quorum_scale, alive_ups_count/2, alive_ups_count*2, ret);
            //     }
            //     else if (args.ups_quorum_scale != alive_ups_count && 0 != alive_ups_count)
            //     {
            //       printf("new quorum scale must be equal to alive updateserver count, but new_quorum_scale=%d, ups_count=%d, err=%d.\n",
            //              args.ups_quorum_scale, alive_ups_count, ret);
            //     }
            //     break;
            //   case OB_RS_NOT_EXIST:
            //     printf("some slave rootserver maybe not get new quorum scale, please check and do again!, err=%d\n", ret);
            //     break;
            //   default:
            //     printf("error occurred. err=%d\n", ret);
            //     break;
            // }
            printf("%.*s \n", result_code.message_.length(), result_code.message_.ptr());
          }
          //add:e
          //add bingo [Paxos ups management] 20170217:b
          // else if (1 == args.ups_quorum_scale)
          // {
          //   printf("change quorum scale [%d->1], slave updateserver should be set offine by user, please check and kill them\n", old_ups_quorum_scale);
          // }
          // else if (1 == old_ups_quorum_scale)
          // {
          //    printf("change quorum scale [1->%d], please check alive servers and the service\n", args.ups_quorum_scale);
          // }
          //add:e
          else
          {
            // printf("change updateserver quorum scale [%d->%d] over, Please do 'ups_quorum_scale' to see if the new quorum scale is OK.\n"
            //        ,old_ups_quorum_scale, current_ups_quorum_scale);
            printf("change ups_quorum_scale of paxos group [%ld], from [%d] to [%d], %s force, \n"
                   "\t[new_ups_quorum|alive_ups_count] = [%d|%d], err=%d \n",
                   args.paxos_group_id, old_ups_quorum_scale, args.ups_quorum_scale, force? "with" : "without",
                   current_ups_quorum_scale, alive_ups_count, ret);
            printf("Please use rs_admin again and do ' stat -o ups_quorum_scale' to see whether the new quorum scale has been CHANGED. \n");
          }
        }
      }
      return ret;
    }
    //add:e

    //add lbzhong [Paxos Cluster.Balance] 201607020:b
    int do_dump_balancer_info(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      const int buff_size = OB_MAX_PACKET_LENGTH;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);

      printf("dump balancer info, table_id=%ld...\n", args.table_id);
      if (OB_SUCCESS != (ret = serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.table_id)))
      {
        printf("failed to serialize table_id, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = client.send_recv(OB_RS_DUMP_BALANCER_INFO, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err = %d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response code, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to dump balancer info. err=%d\n", ret);
        }
        else
        {
          printf("Okay\n");
        }
      }
      return ret;
    }

    int do_get_replica_num(ObBaseClient &client, Arguments &args)
    {
      int err = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      char buf[OB_MAX_PACKET_LENGTH];
      ObDataBuffer msgbuf(buf, sizeof (buf));
      msgbuf.get_position() = 0;
      if (OB_SUCCESS != (err = client.send_recv(OB_GET_REPLICA_NUM, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request. err=%d\n", err);
      }
      else
      {
        int32_t replicas_nums[OB_CLUSTER_ARRAY_LEN];
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (err = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response code, err=%d\n", err);
        }
        else if (OB_SUCCESS != (err = result_code.result_code_))
        {
          printf("failed to get replica num. err=%d\n", err);
        }
        else
        {
          for(int32_t cluster_id = 0; cluster_id < OB_CLUSTER_ARRAY_LEN; cluster_id++)
          {
            if (OB_SUCCESS != (err = serialization::decode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), &replicas_nums[cluster_id])))
            {
              printf("failed to deserialize replica num\n");
              break;
            }
          }
        }
        if(OB_SUCCESS == err)
        {
          for(int32_t cluster_id = 0; cluster_id < OB_CLUSTER_ARRAY_LEN; cluster_id++)
          {
            if(replicas_nums[cluster_id] >= 0)
            {
              printf("cluster[%d]=%d\n", cluster_id, replicas_nums[cluster_id]);
            }
          }
        }
      }
      return err;
    }
    //add:e

    //add bingo [Paxos Cluster.Balance] 20161020:b
    int do_set_master_cluster_id(ObBaseClient &client, Arguments &args)
    {
      int err = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      char buf[OB_MAX_PACKET_LENGTH];
      if (args.new_master_cluster_id < 0 || args.new_master_cluster_id > OB_MAX_CLUSTER_ID)
      {
        printf("ERROR: new_master_cluster_id=%d, (0 <= cluster_id <= OB_MAX_CLUSTER_ID).\n", args.new_master_cluster_id);
        err = OB_ERROR;
      }
      else
      {
        printf("set new master_cluster_id: new_master_cluster_id=%d\n", args.new_master_cluster_id);

        ObDataBuffer msgbuf(buf, sizeof (buf));
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (err = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.new_master_cluster_id)))
        {
          printf("failed to serialize new master_cluster_id, err=%d\n", err);
        }
        else if (OB_SUCCESS != (err = client.send_recv(OB_SET_MASTER_CLUSTER_ID, MY_VERSION, args.request_timeout_us, msgbuf)))
        {
          printf("failed to send request. err=%d\n", err);
        }
        else
        {
          ObResultCode result_code;
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (err = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            printf("failed to deserialize response code, err=%d\n", err);
          }
          else if(OB_NOT_MASTER == (err = result_code.result_code_))
          {
            printf("the command is sent to the slave rootserver, please check command and try again. err=%d.\n", err);
          }
          else if (OB_CLUSTER_ID_ERROR == (err = result_code.result_code_))
          {
            printf("failed to set master_cluser_id, invalid cluster_id. err=%d\n", err);
          }
          else if (OB_SUCCESS != (err = result_code.result_code_))
          {
            printf("failed to set master_cluser_id. err=%d\n", err);
          }
          else
          {
            printf("OKay\n");
          }
        }
      }
      return err;
    }
    //add:e

    //add bingo [Paxos rs_election] 20170301:b
    int do_get_rs_leader(ObBaseClient &client, Arguments &args)
    {
      int err = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      char buf[OB_MAX_PACKET_LENGTH];
      ObDataBuffer msgbuf(buf, sizeof (buf));
      msgbuf.get_position() = 0;
      if (OB_SUCCESS != (err = client.send_recv(OB_GET_RS_LEADER, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request. err=%d\n", err);
      }
      else
      {
        ObResultCode result_code;
        common::ObServer leader;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (err = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response code, err=%d\n", err);
        }
        else if(OB_SUCCESS != (err = leader.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize leader, err=%d\n", err);
        }
        else if (OB_SUCCESS != (err = result_code.result_code_))
        {
          if(OB_RS_DOING_ELECTION == err)
          {
            printf("rs is doing election, please wait and do again later\n");
          }
          else
          {
            printf("failed to get rs leader. err=%d\n", err);
          }
        }
        else
        {
          if (!leader.is_valid())
          {
            err = OB_ERROR;
            printf("rs leader is invalid, err=%d\n", err);
          }
          else
          {
            msgbuf.get_position() = 0;
            if (OB_SUCCESS != (err = client.get_client_mgr().send_request(leader, OB_CHECK_IS_MASTER_RS, MY_VERSION, args.request_timeout_us, msgbuf)))
            {
              printf("failed to send request. err=%d\n", err);
            }
            else
            {
              ObResultCode result_code;
              msgbuf.get_position() = 0;
              bool is_master_rs = false;
              if (OB_SUCCESS != (err = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
              {
                printf("failed to deserialize response code, err=%d\n", err);
              }
              else if (OB_SUCCESS != (err = serialization::decode_bool(msgbuf.get_data(), msgbuf.get_capacity(),
                                                                                     msgbuf.get_position(), &is_master_rs)))
              {
                printf("deserialize is_master_rs failed, err=%d\n", err);
              }
              else if (is_master_rs)
              {
                printf("rs election done, leader rootserver[%s]\n", leader.to_cstring());
              }
              else
              {
                err = OB_ERROR;
                printf("failed to get rs leader, rs[%s] is not leader\n", leader.to_cstring());
              }
            }
          }
          //printf("rs election done, leader rootserver[%s]\n", leader.to_cstring());
        }
      }
      return err;
    }
    //add:e

    int do_get_rs_stat(ObBaseClient &client, Arguments &args)
    {
      int err = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      char buf[OB_MAX_PACKET_LENGTH];
      ObDataBuffer msgbuf_all_rs(buf, sizeof (buf));
      ObResultCode result_code;
      bool is_get_all_rs = true;
      msgbuf_all_rs.get_position() = 0;

      if (OB_SUCCESS != (err = serialization::encode_bool(msgbuf_all_rs.get_data(), msgbuf_all_rs.get_capacity(), msgbuf_all_rs.get_position(), is_get_all_rs)))
      {
        printf("failed to serialize is_get_all_rs flag, err=%d\n", err);
      }
      else if (OB_SUCCESS != (err = client.send_recv(OB_GET_RS_STAT, MY_VERSION, args.request_timeout_us, msgbuf_all_rs)))
      {
        printf("failed to send request. err=%d\n", err);
      }
      else
      {
        msgbuf_all_rs.get_position() = 0;
        if (OB_SUCCESS != (err = result_code.deserialize(msgbuf_all_rs.get_data(), msgbuf_all_rs.get_capacity(), msgbuf_all_rs.get_position())))
        {
          printf("failed to deserialize response code, err=%d\n", err);
        }
        else if (OB_SUCCESS != (err = result_code.result_code_))
        {
          printf("failed to do_rs_admin. err=%d\n", result_code.result_code_);
        }
        else
        {
          int32_t server_count = 0;
          common::ObServer server;
          char buf2[OB_MAX_PACKET_LENGTH];
          ObDataBuffer msgbuf_stat(buf2, sizeof (buf2));
          is_get_all_rs = false;

          if (OB_SUCCESS != (err = serialization::decode_vi32(msgbuf_all_rs.get_data(), msgbuf_all_rs.get_capacity(), msgbuf_all_rs.get_position(), &server_count)))
          {
            YYSYS_LOG(WARN, "failed to deserialize server_count. err=%d", err);
          }
          else
          {
            for (int32_t i = 0; i < server_count; i++)
            {
              memset(buf2, 0, OB_MAX_PACKET_LENGTH);
              msgbuf_stat.get_position() = 0;
              if (OB_SUCCESS != (err = server.deserialize(msgbuf_all_rs.get_data(), msgbuf_all_rs.get_capacity(), msgbuf_all_rs.get_position())))
              {
                YYSYS_LOG(WARN, "failed to deserialize rootserver. err=%d", err);
                break;
              }
              else
              {
                client.set_server(server);
                if (OB_SUCCESS != (err = serialization::encode_bool(msgbuf_stat.get_data(), msgbuf_stat.get_capacity(), msgbuf_stat.get_position(), is_get_all_rs)))
                {
                  printf("failed to serialize is_get_all_rs flag, err=%d\n", err);
                }
                else if (OB_SUCCESS != (err = client.send_recv(OB_GET_RS_STAT, MY_VERSION, args.request_timeout_us, msgbuf_stat)))
                {
                  printf("%s: failed to send request. err=%d\n", to_cstring(server), err);
                }
                else
                {
                  msgbuf_stat.get_position() = 0;
                  if (OB_SUCCESS != (err = result_code.deserialize(msgbuf_stat.get_data(), msgbuf_stat.get_capacity(), msgbuf_stat.get_position())))
                  {
                    printf("failed to deserialize response code, err=%d\n", err);
                  }
                  else if (OB_SUCCESS != (err = result_code.result_code_))
                  {
                    printf("failed to do_rs_admin, err=%d\n", result_code.result_code_);
                  }
                  else
                  {
                    printf("%s: %s\n", to_cstring(server), msgbuf_stat.get_data() + msgbuf_stat.get_position());
                  }
                }
              }
            }
          }
        }
      }
      return err;
    }

    //add bingo [Paxos sstable info to rs log] 20170614:b
    int do_dump_sstable_info(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      const int buff_size = OB_MAX_PACKET_LENGTH;
      char buff[buff_size];
      ObDataBuffer msgbuf(buff, buff_size);

      printf("dump sstable info...\n");
      if (OB_SUCCESS != (ret = client.send_recv(OB_DUMP_SSTABLE_INFO, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err = %d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response code, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to dump sstable info. err=%d\n", ret);
        }
        else
        {
          printf("Okay\n");
        }
      }
      return ret;
    }
    //add:e

    //add bingo [Paxos table replica] 20170620:b
    int do_get_table_replica_num(ObBaseClient &client, Arguments &args)
    {
      int err = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      char buf[OB_MAX_PACKET_LENGTH];
      ObDataBuffer msgbuf(buf, sizeof (buf));

      printf("get table replica num, table_id=%ld...\n", args.table_id);

      if (OB_SUCCESS != (err = serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.table_id)))
      {
        printf("failed to serialize table_id, err=%d\n", err);
      }
      else if (OB_SUCCESS != (err = client.send_recv(OB_GET_TABLE_REPLICA_NUM, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request. err=%d\n", err);
      }
      else
      {
        int32_t replicas_nums[OB_CLUSTER_ARRAY_LEN];
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (err = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response code, err=%d\n", err);
        }
        else if (OB_SUCCESS != (err = result_code.result_code_))
        {
          printf("failed to get replica num. err=%d\n", err);
        }
        else
        {
          for(int32_t cluster_id = 0; cluster_id < OB_CLUSTER_ARRAY_LEN; cluster_id++)// mod lqc [multiusp] 20170702
          {
            if (OB_SUCCESS != (err = serialization::decode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), &replicas_nums[cluster_id])))
            {
              printf("failed to deserialize replica num\n");
              break;
            }
          }
        }
        if(OB_SUCCESS == err)
        {
          for(int32_t cluster_id = 0; cluster_id < OB_CLUSTER_ARRAY_LEN; cluster_id++) // mod lqc [multiusp] 20170702
          {
            if(replicas_nums[cluster_id] >= 0)
            {
              printf("cluster[%d]=%d\n", cluster_id, replicas_nums[cluster_id]);
            }
          }
        }
      }
      return err;
    }
    //add:e


    int do_dump_unusual_tablets(ObBaseClient &client, Arguments &args)
    {
      int ret = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      char buf[OB_MAX_PACKET_LENGTH];
      ObDataBuffer msgbuf(buf, sizeof (buf));

      printf("dump unusual tablets, tablet_version=%ld...\n", args.tablet_version);

      if (args.tablet_version < 0)
      {
        printf("invalid tablet_version\n");
        ret = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.tablet_version)))
      {
        printf("failed to serialize tablet_version, err=%d\n", ret);
      }
      else if (OB_SUCCESS != (ret = client.send_recv(OB_RS_DUMP_UNUSUAL_TABLETS, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request, err=%d\n", ret);
      }
      else
      {
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response code, err=%d\n", ret);
        }
        else if (OB_SUCCESS != (ret = result_code.result_code_))
        {
          printf("failed to dump unusual tablets. err=%d\n", ret);
        }
        else
        {
          printf("Okay!\n");
        }
      }
      return ret;
    }

    int do_execute_range_collection(ObBaseClient &client, Arguments &args)
    {
      int err = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      char buf[OB_MAX_PACKET_LENGTH];
      ObDataBuffer msgbuf(buf, sizeof (buf));

      printf("execute range collection, range[%ld, %ld]...\n", args.collection_start_value, args.collection_end_value);

      if (OB_SUCCESS != (err = serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.collection_start_value)))
      {
        printf("failed to serialize collection_start_value, err=%d\n", err);
      }
      else if (OB_SUCCESS != (err = serialization::encode_vi64(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), args.collection_end_value)))
      {
        printf("failed to serialize collection_end_value, err=%d\n", err);
      }
      else if (OB_SUCCESS != (err = client.send_recv(OB_EXECUTE_RANGE_COLLECTION, MY_VERSION, args.request_timeout_us, msgbuf)))
      {
        printf("failed to send request. err=%d\n", err);
      }
      else
      {
        ObResultCode result_code;
        msgbuf.get_position() = 0;
        if (OB_SUCCESS != (err = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
        {
          printf("failed to deserialize response code, err=%d\n", err);
        }
        else if (OB_SUCCESS != (err = result_code.result_code_))
        {
          printf("failed to execute range collection\n");
        }
        else
        {
          printf("execute range collection successfully!\n");
        }
      }
      return err;
    }

    int do_get_statistics_task(ObBaseClient &client, Arguments &args)
    {
      int err = OB_SUCCESS;
      static const int32_t MY_VERSION = 1;
      char buf[OB_MAX_PACKET_LENGTH];

      ObString str;
      if (args.database_name == NULL)
      {
        str = ObString::make_string("NULL");
      }
      else
      {
        str = ObString::make_string(args.database_name);
      }

      int64_t pos = 0;
      if (OB_SUCCESS != (err = str.serialize(buf, sizeof(buf), pos)))
      {
        printf("failed to serialize database name, ret=%d\n", err);
      }
      else
      {
        printf("database_name: %s\n", args.database_name);
        ObDataBuffer msgbuf(buf, sizeof (buf));
        msgbuf.get_position() = pos;

        if (OB_SUCCESS != (err = client.send_recv(OB_GET_STATISTICS_TASK, MY_VERSION, args.request_timeout_us, msgbuf)))
        {
          printf("failed to send request. err=%d\n", err);
        }
        else
        {
          ObResultCode result_code;
          msgbuf.get_position() = 0;
          if (OB_SUCCESS != (err = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
          {
            printf("failed to deserialize response code, err=%d\n", err);
          }
          else if (OB_SUCCESS != (err = result_code.result_code_))
          {
            printf("failed to get statistics task. err=%d\n", err);
          }
          else
          {
            printf("OKay!\n");
          }
        }
      }
      return err;
    }
  } // end namespace rootserver
}
