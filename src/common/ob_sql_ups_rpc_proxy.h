/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sql_ups_rpc_proxy.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#ifndef _OB_SQL_UPS_RPC_PROXY_H
#define _OB_SQL_UPS_RPC_PROXY_H 1

#include "ob_server.h"
#include "ob_new_scanner.h"
#include "ob_get_param.h"
#include "ob_scan_param.h"
#include "sql/ob_basic_stmt.h"
#include "common/ob_ups_info.h"// //add lijianqiang [MultiUPS] [SELECT_MERGE] 20160107
#include "common/ob_client_manager.h" //add liuzy [MultiUPS] [UPS_List_Info] 20160428

namespace oceanbase
{
  namespace common
  {
           
    class ObPartitionMonitor;//[conflict_level A]
    class ObSqlUpsRpcProxy
    {
      public:
        virtual ~ObSqlUpsRpcProxy()
        {}

        virtual int sql_ups_get(const ObGetParam & get_param, ObNewScanner & new_scanner, const int64_t timeout) = 0;
        virtual int sql_ups_scan(const ObScanParam & scan_param, ObNewScanner & new_scanner, const int64_t timeout) = 0;
        virtual int check_incremental_data_range( int64_t table_id, ObVersionRange &version, ObVersionRange &new_range, bool is_strong_consistency = false)= 0;//add zhaoqiong [Truncate Table]:20160318
         //add lijianqiang [MultiUPS] [SELECT_MERGE] 20160104:b
        virtual int sql_ups_get(const ObGetParam & get_param, ObNewScanner & new_scanner, const int64_t timeout, const int64_t paxos_id) = 0;
        virtual int sql_ups_scan(const ObScanParam & scan_param, ObNewScanner & new_scanner, const int64_t timeout, const int64_t paxos_id) = 0;
        virtual int get_master_ups_list(common::ObUpsList  &master_ups_list, int64_t &master_ups_count) = 0;
        virtual ObPartitionMonitor *get_cs_partition_monitor() = 0;
        //add 20160104:e
        //add liuzy [Multiups] [UPS_List_Info] 20160427:b
        virtual int get_paxos_group_offline_info(ObServer &server, ObVersionRange range,
                                                 const ObClientManager *& client_mgr,
                                                 ObArray<int64_t> &paxos_id_array) = 0;
        //20160427:e
    };
  }
}

#endif /* _OB_SQL_UPS_RPC_PROXY_H */

