/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Time-stamp: <2013-05-15 09:16:27 fufeng.syd>
 * Version: $Id$
 * Filename: ob_root_server_config.h
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@taobao.com>
 */

#ifndef _OB_ROOT_SERVER_CONFIG_H_
#define _OB_ROOT_SERVER_CONFIG_H_

#include "common/ob_server_config.h"
#include "common/ob_config.h"

using namespace oceanbase;
using namespace oceanbase::common;

namespace oceanbase
{
  namespace rootserver
  {
    class ObRootServerConfig
        : public common::ObServerConfig
    {
      public:
        int get_root_server(ObServer &server) const;
        int get_master_root_server(ObServer &server) const;

      protected:
        common::ObRole get_server_type() const
        { return OB_ROOTSERVER; }

      public:
        DEF_TIME(cs_lease_duration_time, "10s", "(default:10s range: ) cs lease duration time");
        DEF_TIME(migrate_wait_time, "60s", "(default:60s range: ) waiting time for balance thread to start work");
        DEF_TIME(safe_lost_one_time, "3600s", "(default:3600s range: ) safe duration while lost one copy");
        DEF_TIME(safe_wait_init_time, "60s", "(default:60s range: ) time interval for build  root table");
        DEF_BOOL(create_table_in_init, "False", "(default:False range: ) create tablet switch while init");
        DEF_BOOL(ddl_system_table_switch, "False", "(default:False range: ) true: allow to create or drop system tables. false: not allow to create or drop system tables");
        DEF_BOOL(monitor_row_checksum, "True", "(default:True range: ) compare row checksum between master cluster and slave master");
        DEF_TIME(monitor_row_checksum_interval, "1800s", "(default:1800s range: ) compare row checksum between master cluster and slave master");
        DEF_TIME(monitor_row_checksum_timeout, "3s", "(default:3s range: ) get master checksum timeout");
        DEF_TIME(monitor_drop_table_interval, "600s", "[1s,]", "(default:600s range:[1s,]) delete droped table in daily merge check interval");
        //add liumz, [secondary index static_index_build] 20150323:b
        DEF_TIME(monitor_create_index_timeout, "3600s", "[1s,]", "(default:3600s range:[1s,]) create single static index timeout");
        //add:e

        DEF_TIME(check_create_index_done_interval, "10s", "[1s,]", "(default:10s range:[1s,]) check create local or global index done sleep interval");


        DEF_BOOL(monitor_column_checksum, "True", "(default:True range: ) wether compare column checksum or not");
        DEF_INT(tablet_replicas_num, "3", "[1,3]", "(default:3 range:[1,3]) tablet replicas num");
        DEF_INT(io_thread_count, "4", "[1,100]", "(default:4 range:[1,100]) io thread count");
        DEF_INT(read_thread_count, "20", "[10,100]", "(default:20 range:[10,100]) read thread count");
        DEF_INT(read_queue_size, "10000", "[10,100000]", "(default:10000 range:[10,100000]) read queue size");
        DEF_INT(write_queue_size, "10000", "[10,100000]", "(default:10000 range:[10,100000]) write queue size");
        DEF_INT(log_queue_size, "10000", "[10,100000]", "(default:10000 range:[10,100000]) log queue size");
        //add pangtianze [Paxos rs_election] 20150619:b
        DEF_INT(election_queue_size, "10000", "[10,100000]", "(default:10000 range:[10,100000]) election queue size");
        //add:e
        //add pangtianze [Paxos rs_election] 20150721:b
        DEF_INT(rs_paxos_number, "0", "[0,]", "(default:0 range:[0,]) rootserver paxos number, used in rs election");
        //DEF_INT(ups_quorum_scale,   "0", "[0,]", "until log replicas are more than this number, master commit log");
        DEF_INT(ups_quorum_scale,   "0", "[0,]", "(default:0 range:[0,]) the default ups quorum scale of new paxos group when add new ones");
        //add:e
        DEF_STR(paxos_ups_quorum_scales, "0,0,0,0,0,0,0,0,0", "(default:0,0,0,0,0,0,0,0,0 range: ) ups quorum scale in each paxos group");

        //add pangtianze [Paxos rs_election] 20161020:b
        DEF_BOOL(use_paxos, "True", "(default:True range: ) rootserver use paxos");
        //add:e
        DEF_TIME(network_timeout, "3s", "(default:3s range: ) network timeout for rpc process");
        DEF_TIME(inner_table_network_timeout, "50s", "(default:50s range: ) network timeout for interal table access process");
        DEF_BOOL(lease_on, "True", "(default:True range: ) lease switch between master and slave rootserver");
        //mod pangtianze [Paxos rs_election] 20160918:b
        //DEF_TIME(lease_interval_time, "15s", "lease interval time");
        DEF_TIME(lease_interval_time, "5s", "(default:5s range: ) lease interval time");
        //mod:e
        DEF_TIME(lease_reserved_time, "10s", "(default:10s range: ) lease reserved time");
        //mod:e
        //add pangtianze [Paxos rs_election] 20160918:b
        DEF_TIME(leader_lease_interval_time, "1s", "(default:1s range: ) lease interval time");
        //no_hb_response_duration_time + leader_lease_interval_time < lease_interval_time
        DEF_TIME(no_hb_response_duration_time, "3s", "(default:3s range: ) allowed duration time without recevie heartbeart responses");
        //add:e
        //add huangjianwei [Paxos rs_switch] 20160727:b
        //mod pangtianze [Paxos] 20170628, change 10s -> 30s
        DEF_TIME(server_status_refresh_time,"30s","(default:30s range: ) server status refresh time");
        //add:e
        DEF_TIME(slave_register_timeout, "3s", "(default:3s range: ) slave register rpc timeout");
        DEF_TIME(log_sync_timeout, "3s", "(default:3s range: ) log sync rpc timeout");
        DEF_TIME(vip_check_period, "500ms", "(default:500ms range: ) vip check period");
        DEF_TIME(log_replay_wait_time, "100ms", "(default:100ms range: ) log replay wait time");
        DEF_CAP(log_sync_limit, "40MB", "(default:40MB range: ) log sync limit");
        DEF_TIME(max_merge_duration_time, "2h", "(default:2h range: ) max merge duration time");
        DEF_TIME(cs_probation_period, "5s", "(default:5s range: ) duration before cs can adopt migrate");

        DEF_TIME(ups_lease_time, "9s", "(default:9s range: ) ups lease time");
        DEF_TIME(ups_lease_reserved_time, "8500ms", "(default:8500ms range: ) ups lease reserved time");
        DEF_TIME(ups_renew_reserved_time, "7770ms", "(default:7770ms range: ) ups renew reserved time");
        ///need greater than ups lease
        //mod pangtianze [Paxos] 20170427:b
        //DEF_TIME(ups_waiting_register_time, "25s", "rs select master ups, should wait the time after first ups regist");
        DEF_TIME(ups_waiting_register_time, "15s", "(default:15s range: ) rs select master ups, should wait the time after first ups regist");
        //mod:e
        DEF_TIME(expected_request_process_time, "10ms", "(default:10ms range: ) expected request process time, check before pushing in queue");
        // balance related flags
        DEF_TIME(balance_worker_idle_time, "30s", "[1s,]", "(default:30s range:[1s,]) balance worker idle wait time");
        DEF_TIME(import_rpc_timeout, "60s", "[30s,]", "(default:60s range:[30s,]) import rpc timeout, used in fetch range list and import");
        DEF_INT(balance_tolerance_count, "10", "[1,1000]", "(default:10 range:[1,1000]) tolerance count for balance");
        DEF_TIME(balance_timeout_delta, "10s", "(default:10s range: ) balance timeout delta");
        DEF_TIME(balance_max_timeout, "5m", "(default:5m range: ) max timeout for one group of balance task");
        DEF_INT(balance_max_concurrent_migrate_num, "10", "[1,1024]", "max concurrent migrate num for balance");
        //DEF_INT(balance_max_migrate_out_per_cs, "2", "[1,10]", "max migrate out per cs");
        //DEF_INT(balance_max_migrate_in_per_cs, "2", "[1,10]", "max migrate in per cs");
        DEF_INT(balance_max_migrate_out_per_cs, "2", "[1,]", "(default:2 range:[1,]) max migrate out per cs");
        DEF_INT(balance_max_migrate_in_per_cs, "2", "[1,]", "(default:2 range:[1,]) max migrate in per cs");
        
        
        DEF_BOOL(enable_balance, "True", "(default:True range: ) balance switch");
        DEF_BOOL(enable_rereplication, "True", "(default:True range: ) rereplication switch");

        DEF_BOOL(enable_load_data, "False", "(default:False range: ) load data switch");
        DEF_INT(load_data_replicas_num, "2", "[1,3]", "(default:2 range:[1,3]) load data replicas num when loading is over");
        DEF_TIME(load_data_check_status_interval, "30s", "(default:30s range: ) load data switch");
        DEF_INT(data_source_max_fail_count, "10", "(default:10 range: ) max data source fail count");
        DEF_TIME(load_data_max_timeout_per_range, "3m", "(default:3m range: ) max timeout for cs fetch the data per range");

        DEF_TIME(tablet_migrate_disabling_period, "60s", "(default:60s range: ) cs can participate in balance after regist");
        DEF_BOOL(enable_new_root_table, "False", "(default:False range: ) new root table switch");
        DEF_INT(obconnector_port, "5433", "(default:5433 range: ) obconnector port");
        DEF_TIME(delete_table_time, "10m", "(default:10m range: ) time for cs to delete one table while bypass");
        DEF_TIME(load_sstable_time, "20m", "(default:20m range: ) time for cs to load one table while bypass");
        DEF_TIME(report_tablet_time, "5m", "(default:5m range: ) time for cs to report tablets while bypass");
        DEF_TIME(build_root_table_time, "5m", "(default:5m range: ) time for cs to build new root_table while bypass");

        DEF_BOOL(enable_cache_schema, "True", "(default:True range: ) cache schema switch");
        DEF_BOOL(is_import, "False", "(default:False range: ) is import application");
        DEF_BOOL(is_ups_flow_control, "False", "(default:False range: ) ups flow control switch");
        DEF_INT(read_master_master_ups_percent, "20", "[0,100]", "(default:20 range:[0,100]) master master ups read percent");

        DEF_INT(merge_master_master_ups_percent, "20", "[0,100]", "(default:20 range:[0,100]) master master ups merge percent");


        //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
        /* no slave_master_ups anymore */
        //DEF_INT(read_slave_master_ups_percent, "50", "[0,100]", "slave master ups read percent");
        //del:e
        //add pangtianze [Paxos Cluster.Flow.UPS] 20170119:b
        DEF_INT(is_strong_consistency_read, "0", "[0,1]", "(default:0 range:[0,1]) if master ups has all read flow");
        //add:e
        DEF_CAP(max_commit_log_size, "64MB", "(default:64MB range: ) max commit log size");
        DEF_INT(commit_log_sync_type, "1", "(default:1 range: ) commit log sync type");

        DEF_STR(rs_data_dir, "data/as", "(default:data/as range: ) admin server data directory");
        DEF_STR(commit_log_dir, "data/as_commitlog", "(default:data/as_commitlog range: ) root server commit log directory");
        DEF_STR(first_meta_filename, "first_tablet_meta", "(default:first_tablet_meta range: ) first meta file name");
        DEF_STR(schema_filename, "etc/schema.ini", "(default:etc/schema.ini range: ) schame file name");

        DEF_IP(master_root_server_ip, "0.0.0.0", "(default: range: ) master OceanBase instance vip address");
        DEF_INT(master_root_server_port, "0", "(default:0 range: ) master OceanBase instance listen port");

        //add peiouya [MultiUPS] [UPS_Manage_Function] 20150427:b
        DEF_BOOL(is_use_paxos, "False", "(default:False range: ) paxos switch");
        DEF_INT(use_cluster_num, "1", "[1,6]", "(default:1 range:[1,6]) one paxos group min work num");
        //using_paxos_id_num:N means using paxos id must be 0,1,2,3,N-1
        DEF_INT(use_paxos_num, "1", "[1,9]","(default:1 range:[1,9]) save all use paxos_id"); /* calc later */
        DEF_TIME(major_freeze_wait_time, "3m", "[6s,]", "(default:3m range:[6s,]) time for ms and cs to fetch partition rules");
        //add 20150427:e
        DEF_TIME(delete_table_rules_wait_time ,"300m", "(default:300m range: ) time for delete table rules");//add wuna [MultiUPS] [sql_api] 20160421
        //add zhaoqiong[roottable tablet management]20160223:b
        DEF_BOOL(enable_rereplication_in_table_level, "False", "(default:False range: ) table_level rereplication switch");
        //add:e
        //add pangtianze [Paxos Cluster.Balance] 20170620:b
        //mod pangtianze [Paxos Cluster.Balance] 20170810:b 0;0;0;0;0;0 -> 2;1;1;1;1;0
        DEF_STR(cluster_replica_num, "2;1;1;1;1;0", "(default:2;1;1;1;1;0 range: ) tablet replica number in each cluster");
        //add:e
        //add pangtianze [Paxos inner table revise] 20170826
        DEF_TIME(sys_table_revise_interval, "60s", "[1s,]", "(default:60s range:[1s,]) check and revise system table interval");
        //add:e
        //add pangtianze [Paxos rs_election_priority] 20170911:b
        DEF_INT(rs_election_priority, "1", "[1,5]", "(default:1 range:[1,5]) rootserver election priority[1-5], 1 is highest and 5 is lowest");
        //add:e
        DEF_TIME(clean_statistic_info_time, "180s", "time for clean __all_statistic_info table");

    };
  }
}
#endif /* _OB_ROOT_SERVER_CONFIG_H_ */
