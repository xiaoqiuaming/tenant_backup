/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_rs_ms_message.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_RS_MS_MESSAGE_H
#define _OB_RS_MS_MESSAGE_H 1

#include "ob_server.h"
namespace oceanbase
{
  namespace common
  {
    enum ObUpsStat
    {
      UPS_SLAVE = 0,       ///< slave
      UPS_MASTER = 1,      ///< the master
    };

    enum ObServerType
    {
      CHUNK_SERVER = 100,
      MERGE_SERVER = 200,
    };

    struct ObUpsInfo
    {
        common::ObServer addr_;
        int32_t inner_port_;
        ObUpsStat stat_;          // do not use this in 0.2.1
        int8_t ms_read_percentage_;
        int8_t cs_read_percentage_;
        //mod peiouya [MultiUPS] [UPS_Manage_Function] 20150422:b
        //int16_t reserve2_;
        //int32_t reserve3_;
        //add peiouya [MultiUPS] [UPS_Manage_Function] 20150616:b
        //for ms/cs  finger_print , if not align, calc finger_print error!
        int16_t mem_align_;
        //add 20150616:e
        int64_t cluster_id_;
        int64_t paxos_id_;
        //mod 20150422:e
        ObUpsInfo();
        inline int8_t get_read_percentage(const ObServerType type = MERGE_SERVER) const;
        inline common::ObServer get_server(const ObServerType type = MERGE_SERVER) const;
        int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
        int deserialize(const char* buf, const int64_t buf_len, int64_t& pos);
    };

    inline ObUpsInfo::ObUpsInfo()
      :inner_port_(0), stat_(UPS_SLAVE),
        ms_read_percentage_(0), cs_read_percentage_(0),
        mem_align_(0),  //add peiouya [MultiUPS] [UPS_Manage_Function] 20150616
        //mod peiouya [MultiUPS] [UPS_Manage_Function] 20150422:b
        //reserve2_(0), reserve3_(0)
        cluster_id_(-1), paxos_id_(-1)
      //mod 20150422:e
    {
    }

    inline int8_t ObUpsInfo::get_read_percentage(const ObServerType type/*= MERGE_SERVER*/) const
    {
      int8_t percentage = 0;
      if (CHUNK_SERVER == type)
      {
        percentage = cs_read_percentage_;
      }
      else if (MERGE_SERVER == type)
      {
        percentage = ms_read_percentage_;
      }
      return percentage;
    }

    inline common::ObServer ObUpsInfo::get_server(const  ObServerType type/*= MERGE_SERVER*/) const
    {
      common::ObServer server = addr_;
      if (CHUNK_SERVER == type)
      {
        server.set_port(inner_port_);
      }
      return server;
    }

    struct ObUpsList
    {
        //mod peiouya [MultiUPS] [UPS_Manage_Function] 20150423:b
        //static const int32_t MAX_UPS_COUNT = 8;
        //mod peiouya [MultiUPS] [UPS_Manage_Function] 20150616:b
        //static const int32_t MAX_UPS_COUNT = 512;
        static const int32_t MAX_UPS_COUNT = MAX_UPS_COUNT_ONE_CLUSTER * OB_MAX_UPS_COUNT;
        //mod 20150616:e
        //mod 20150423:e
        ObUpsInfo ups_array_[MAX_UPS_COUNT];
        int32_t ups_count_;
        int32_t reserve_;
        int32_t sum_ms_percentage_;
        int32_t sum_cs_percentage_;
        static const int32_t MAX_UPS_NODE_COUNT = OB_MAX_CLUSTER_COUNT * MAX_UPS_COUNT_ONE_CLUSTER;
        int32_t sum_ms_percentage_array[MAX_UPS_NODE_COUNT];
        int32_t sum_cs_percentage_array[MAX_UPS_NODE_COUNT];

        ObUpsList();
        ObUpsList& operator= (const ObUpsList &other);
        // the sum is calculated when deserialize not calculate every time
        // must deserializ at first when using this interface
        // if type not in (ObServerType) return 0
        inline int32_t get_sum_percentage(const ObServerType type = MERGE_SERVER) const;
        int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
        int deserialize(const char* buf, const int64_t buf_len, int64_t& pos);
        void print() const;
        void print(char* buf, const int64_t buf_len, int64_t &pos) const;

        void set_sum_percentage_of_array();
        int32_t get_sum_percentage(int64_t paxos_id, int64_t cluster_id, ObServerType type) const;
        void get_ups(int64_t paxos_id, int64_t cluster_id, ObServer &update_server, ObServerType server_type) const;
        int64_t get_master_ups_cluster_id(int64_t paxos_id) const;
        void reset_percentage(int64_t paxos_id, ObServerType server_type);
    };

    inline ObUpsList::ObUpsList()
      :ups_count_(0), reserve_(0), sum_ms_percentage_(0), sum_cs_percentage_(0)
    {
      //add hongchen [FINGERPRINT_FIX] 20170717:b
      // for ups fingerprint
      memset(this,0,sizeof(ObUpsList));
      //add hongchen [FINGERPRINT_FIX] 20170717:b
    }

    inline int32_t ObUpsList::get_sum_percentage(const ObServerType type/*= MERGE_SERVER*/) const
    {
      int32_t sum = 0;
      if (CHUNK_SERVER == type)
      {
        sum = sum_cs_percentage_;
      }
      else if (MERGE_SERVER == type)
      {
        sum = sum_ms_percentage_;
      }
      return sum;
    }
  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_RS_MS_MESSAGE_H */

