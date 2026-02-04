/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Filename: ob_merge_server_config.h
 *
 * Authors:
 *   Yudi Shi <fufeng.syd@taobao.com>
 *
 */

#ifndef _OB_MERGE_SERVER_CONFIG_H_
#define _OB_MERGE_SERVER_CONFIG_H_

#include <stdint.h>
#include "yysys.h"
#include "common/ob_config.h"
#include "common/ob_object.h"
#include "common/ob_server_config.h"
#include "common/ob_system_config.h"
#include "sql/ob_sql_config_provider.h"
using namespace oceanbase;
using namespace oceanbase::common;

namespace oceanbase
{
  namespace mergeserver
  {
    union UValue
    {
        int32_t i32;
        int64_t i64;
    };

    class ObMergeServerConfig
      : public common::ObServerConfig, public sql::ObSQLConfigProvider
    {
      protected:
        common::ObRole get_server_type() const
        { return OB_MERGESERVER; }

      public:
        bool is_read_only() const
        {return read_only;};
        bool is_regrant_priv() const
        {return regrant_priv;};
        DEF_INT(root_server_port, "0", "(1024,65535)", "(default:0 range:(1024,65535)) root server listen port");
        DEF_TIME(task_left_time, "10ms", "(default:10ms range: ) task left time for drop ahead");
        DEF_INT(task_queue_size, "10000", "[1,]", "(default:10000 range:[1,]) task queue size");
        DEF_INT(io_thread_count, "12", "[1,]", "(default:12 range:[1,]) io thread count for libeasy");
        //DEF_INT(task_thread_count, "4", "task thread number");
        DEF_INT(task_thread_count, "8", "(default:8 range: ) task thread number");
        DEF_INT(log_interval_count, "100", "(default:100 range: ) legacy param, used for OB0.3 Only");
        //mod zhaoqiong [Schema Manager] 20150420:b 
        //DEF_TIME(network_timeout, "2s", "timeout when communication with other server");
        DEF_TIME(network_timeout, "3s", "(default:3s range: ) timeout when communication with other server");
        //mod:e
        DEF_TIME(frozen_version_timeout, "600s", "(default:600s range: ) ups frozen version cache tiemout");
        DEF_TIME(monitor_interval, "600s", "(default:600s range: ) execute monitor task once every monitor_interval");
        DEF_TIME(lease_check_interval, "6s", "(default:6s range: ) lease check interval");
        //add liu jun. [MultiUPS] [part_cache] 20150617:b
        //add 20150617:e
        //add wuna [MultiUPS] [part_cache] 20160118:b
        DEF_TIME(partition_manager_release_interval, "300s", "[0,3600]", "(default:300s range:[0,3600]) release partition manager interval");
        //add 20160118:e
        //add liuzy [MultiUPS] [BugFix] [FetchUpsList] 20160803:b
        DEF_TIME(fetch_ups_interval, "2s", "(default:2s range: ) fetch ups list interval");
        //add 20160803:e
        DEF_CAP(location_cache_size, "32MB", "(default:32MB range: ) location cache size");
        DEF_TIME(location_cache_timeout, "600s", "(default:600s range: ) location cache timeout");
        DEF_TIME(vtable_location_cache_timeout, "8s", "(default:8s range: ) virtual table location cache timeout");
        DEF_CAP(intermediate_buffer_size, "8MB", "(default:8MB range: ) intermediate buffer size to store one packet, 4 times network packet size (2M)");
        DEF_INT(memory_size_limit_percentage, "40", "(0,100]", "(default:40 range:(0,100]) max percentage of totoal physical memory ms can use");
        DEF_INT(max_parellel_count, "16", "[1,]", "(default:16 range:[1,]) max parellel sub request to chunkservers for one request");
        DEF_INT(max_get_rows_per_subreq, "20", "[0,]", "(default:20 range:[0,]) row count to split to cs when using multi-get, 0 means no split");
        DEF_TIME(max_req_process_time, "15s", "(default:15s range: ) max process time for each request");
        DEF_BOOL(use_new_balance_method, "True", "(default:True range: ) use new balance method");
        DEF_INT(reserve_get_param_count, "3", "[1,]", "(default:3 range:[1,]) legacy param, used for OB0.3 Only");
        DEF_INT(timeout_percent, "70", "[10,80]", "(default:70 range:[10,80]) max cs timeout to ms timeout, used by ms retry");
        DEF_BOOL(allow_return_uncomplete_result, "False", "(default:False range: ) allow return uncomplete result");
        //add bizb [slow_query]
        DEF_TIME(slow_query_threshold, "0ms", "(default:0ms range: ) query time beyond this value will be treat as slow query. When the value is 0ms, it means that this function is turned off");
        //end
        //DEF_TIME(slow_query_threshold, "100ms", "(default:100ms range: ) query time beyond this value will be treat as slow query");
        DEF_BOOL(lms, "False", "(default:False range: ) is listener mergeserver");
        DEF_CAP(frozen_data_cache_size, "256MB", "(default:256MB range: ) frozen data cache size");
        DEF_CAP(bloom_filter_cache_size, "256MB", "(default:256MB range: ) bloom filter cache size");
        DEF_TIME(change_obi_timeout, "30s", "(default:30s range: ) change obi role timeout");//30s
        DEF_INT(check_ups_log_interval, "1", "(default:1 range: ) change obi role, check ups log interval");
        //param for obmysql
        DEF_INT(obmysql_port, "3100", "(1024,65536)", "(default:3100 range:(1024,65536)) obmysql listen port");
        DEF_INT(obmysql_io_thread_count, "8", "[1,]", "(default:8 range:[1,]) obmysql io thread count for libeasy");
        DEF_INT(obmysql_work_thread_count, "120", "[1,]", "(default:120 range:[1,]) obmysql io thread count for doing sql task");
        DEF_INT(obmysql_task_queue_size, "10000", "[1,]", "(default:10000 range:[1,]) obmysql task queue size");

        // query cache
        DEF_STR(query_cache_type, "OFF", "(default:OFF range: ) query cache switch: OFF, ON");
        DEF_INT(query_cache_size, "0", "[0,100]", "(default:0 range:[0,100]) query cache size, percentage of total physical memory");
        DEF_CAP(query_cache_max_result, "4KB", "(default:4KB range: ) only result set smaller than this will be cached; need reboot");
        DEF_INT(query_cache_max_param_num, "512", "[1,]", "(default:512 range:[1,]) only result set with number of prepared statements smaller than this will be cached; need reboot");
        DEF_TIME(query_cache_consistent_expiration_time, "10ms", "(default:10ms range: ) query cache expiration for consistent query");
        DEF_TIME(query_cache_expiration_time, "10s", "(default:10s range: ) query cache expiration for inconsistent query");

        DEF_STR(recorder_fifo_path, "run/mergeserver.fifo", "(default:run/mergeserver.fifo range: ) named FIFO to read mergeserver\\'s SQL packets");
        DEF_BOOL(read_only, "false", "(default:false range: ) This variable is off by default. When it is enabled, the server permits no updates except for system tables.");
        //add liumz, [multi_database.priv_manage]20150708:b
        DEF_BOOL(regrant_priv, "true", "(default:true range: ) whether regrant priv to db_level_priv\\'s owners after create table");
        //add:e
        DEF_INT(io_thread_start_cpu, "-1", "(default:-1 range: ) start number of cpu, set affinity by io thread");
        DEF_INT(io_thread_end_cpu,   "-1", "(default:-1 range: ) end number of cpu, set affinity by io thread");
        DEF_INT(obmysql_io_thread_start_cpu, "-1", "(default:-1 range: ) start number of cpu, set affinity by io thread");
        DEF_INT(obmysql_io_thread_end_cpu,   "-1", "(default:-1 range: ) end number of cpu, set affinity by io thread");
        //add hongchen [HOT_UPDATE_SKEW] 20170724:b
        DEF_BOOL(using_hot_update, "false", "(default:false range: ) whether using short-path update logic.");
        //add hongchen [HOT_UPDATE_SKEW] 20170724:e
        //add hongchen [EXPORT_TOOL_OPTI] 20170821:b
        DEF_INT(export_tool_parallel, "50",  "[1, 500]","(default:50 range:[1, 500]) parallel for export tool");
        //add hongchen [EXPORT_TOOL_OPTI] 20170821:e
        //add pangtianze [Paxos strong_consistency_read] 20170825:b
        DEF_BOOL(using_strong_consistency, "False", "(default:False range: ) use strong consistency read");
        //add:e

        DEF_BOOL(force_print_sql, "False", "(default:False range: ) force print sql info no matter slow query or not");
        DEF_BOOL(enable_dist_trans, "true", "(default:true range: ) distributed transaction switch");
        DEF_CAP(statistic_info_cache_size, "512MB", "(default:512MB range: ) statistic info cache size");
        DEF_TIME(statistic_info_cache_timeout, "36000s", "(default:36000s range: ) statistic info cache timeout");
        DEF_BOOL(clean_statistic_info_cache, "False", "(default:False range: ) clean statistic info cache");
        DEF_INT(retry_sql_wait_time, "40", "[20, 500]", "(default:40 range:[20, 500]ms) retry sql transaction wait time");
    };
  } /* mergeserver */
} /* oceanbase */

#endif /* _OB_MERGE_SERVER_CONFIG_H_ */
