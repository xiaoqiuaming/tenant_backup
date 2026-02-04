/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_rs_ms_message.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_ups_info.h"
#include "utility.h"
#include <yysys.h>
using namespace oceanbase::common;

int ObUpsInfo::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = addr_.serialize(buf, buf_len, pos)))
  {
    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, pos, inner_port_)))
  {
    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, pos, (int32_t)stat_)))
  {
    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_i8(buf, buf_len, pos, ms_read_percentage_)))
  {
    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_i8(buf, buf_len, pos, cs_read_percentage_)))
  {
    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  //mod peiouya [MultiUPS] [UPS_Manage_Function] 20150422:b
  // else if (OB_SUCCESS != (ret = serialization::encode_i16(buf, buf_len, pos, reserve2_)))
  // {
  // YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  // }
  // else if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, pos, reserve3_)))
  // {
  // YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  // }
  else if (OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, pos, cluster_id_)))
  {
    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, pos, paxos_id_)))
  {
    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  //mod 20150422:e
  return ret;
}

int ObUpsInfo::deserialize(const char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_INVALID_ARGUMENT;
  int32_t stat;
  if (OB_SUCCESS != (ret = addr_.deserialize(buf, buf_len, pos)))
  {
    YYSYS_LOG(ERROR, "deserialize error");
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, buf_len, pos, &inner_port_)))
  {
    YYSYS_LOG(ERROR, "deserialize error");
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, buf_len, pos, &stat)))
  {
    YYSYS_LOG(ERROR, "deserialize error");
  }
  else if (OB_SUCCESS != (ret = serialization::decode_i8(buf, buf_len, pos, &ms_read_percentage_)))
  {
    YYSYS_LOG(ERROR, "deserialize error");
  }
  else if (OB_SUCCESS != (ret = serialization::decode_i8(buf, buf_len, pos, &cs_read_percentage_)))
  {
    YYSYS_LOG(ERROR, "deserialize error");
  }
  //mod peiouya [MultiUPS] [UPS_Manage_Function] 20150422:b
  // else if (OB_SUCCESS != (ret = serialization::decode_i16(buf, buf_len, pos, &reserve2_)))
  // {
  // YYSYS_LOG(ERROR, "deserialize error");
  // }
  // else if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, buf_len, pos, &reserve3_)))
  // {
  // YYSYS_LOG(ERROR, "deserialize error");
  // }
  else if (OB_SUCCESS != (ret = serialization::decode_i64(buf, buf_len, pos, &cluster_id_)))
  {
    YYSYS_LOG(ERROR, "deserialize error");
  }
  else if (OB_SUCCESS != (ret = serialization::decode_i64(buf, buf_len, pos, &paxos_id_)))
  {
    YYSYS_LOG(ERROR, "deserialize error");
  }
  //mod 20150422:e
  else if (UPS_SLAVE != stat && UPS_MASTER != stat)
  {
    YYSYS_LOG(ERROR, "invalid ups stat, stat=%d", stat);
  }
  else
  {
    stat_ = (ObUpsStat)stat;
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObUpsList::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, pos, ups_count_)))
  {
    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else
  {
    for (int32_t i = 0; i < ups_count_; ++i)
    {
      if (OB_SUCCESS != (ret = ups_array_[i].serialize(buf, buf_len, pos)))
      {
        YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
        break;
      }
    }
  }
  return ret;
}

int ObUpsList::deserialize(const char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, buf_len, pos, &ups_count_)))
  {
    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    // reset
    sum_cs_percentage_ = sum_ms_percentage_ = 0;
    for (int32_t i = 0; i < ups_count_ && i < MAX_UPS_COUNT; ++i)
    {
      if (OB_SUCCESS != (ret = ups_array_[i].deserialize(buf, buf_len, pos))
          || !(0 <= ups_array_[i].paxos_id_ && ups_array_[i].paxos_id_ < MAX_UPS_COUNT_ONE_CLUSTER)
          || !(0 <= ups_array_[i].cluster_id_ && ups_array_[i].cluster_id_ < OB_MAX_CLUSTER_COUNT))
      {
        YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
        ret = OB_INVALID_ARGUMENT;
        break;
      }
      else
      {
        sum_ms_percentage_ += ups_array_[i].ms_read_percentage_;
        sum_cs_percentage_ += ups_array_[i].cs_read_percentage_;
      }
    }
  }
  set_sum_percentage_of_array();//add for [402-bugfix ups select]
  return ret;
}

//add for [402-bugfix ups select]-b
void ObUpsList::set_sum_percentage_of_array()
{
  int pid, cid;
  for (int32_t i = 0; i < ups_count_ && i < MAX_UPS_COUNT; ++i)
  {
    pid = (int)ups_array_[i].paxos_id_;
    cid = (int)ups_array_[i].cluster_id_;
    sum_ms_percentage_array[cid * MAX_UPS_COUNT_ONE_CLUSTER + pid] += ups_array_[i].ms_read_percentage_;
    sum_cs_percentage_array[cid * MAX_UPS_COUNT_ONE_CLUSTER + pid] += ups_array_[i].cs_read_percentage_;
  }
}

void ObUpsList::reset_percentage(int64_t paxos_id, ObServerType type)
{
  if (CHUNK_SERVER == type)
  {
    for (int32_t i = 0; i < ups_count_; i++)
    {
      if (ups_array_[i].paxos_id_ == paxos_id)
      {
        ups_array_[i].cs_read_percentage_ = 1;
        sum_cs_percentage_array[ups_array_[i].cluster_id_ * MAX_UPS_COUNT_ONE_CLUSTER + ups_array_[i].paxos_id_] += 1;
      }
    }
  }
  else if (MERGE_SERVER == type)
  {
    for (int32_t i = 0; i < ups_count_; i++)
    {
      if (ups_array_[i].paxos_id_ == paxos_id)
      {
        ups_array_[i].ms_read_percentage_ = 1;
        sum_ms_percentage_array[ups_array_[i].cluster_id_ * MAX_UPS_COUNT_ONE_CLUSTER + ups_array_[i].paxos_id_] += 1;
      }
    }
  }
}

int32_t ObUpsList::get_sum_percentage(int64_t paxos_id, int64_t cluster_id, const ObServerType type) const
{
  int32_t sum = 0;

  if (paxos_id < 0 || paxos_id >= MAX_UPS_COUNT_ONE_CLUSTER)
  {
    YYSYS_LOG(WARN, "paxos_id is out of range, which shoud in [0, %ld]", MAX_UPS_COUNT_ONE_CLUSTER - 1);
  }
  else if (cluster_id < -1 || cluster_id >= OB_MAX_CLUSTER_COUNT)
  {
    YYSYS_LOG(WARN, "cluster_id is out of range, which shoud in [0, %ld]", OB_MAX_CLUSTER_COUNT - 1);
  }
  else if (cluster_id == OB_ALL_CLUSTER_FLAG)
  {
    for (int cid = 0; cid < OB_MAX_CLUSTER_COUNT; cid++)
    {
      if (CHUNK_SERVER == type)
      {
        sum += sum_cs_percentage_array[cid * MAX_UPS_COUNT_ONE_CLUSTER + paxos_id];
      }
      else if (MERGE_SERVER == type)
      {
        sum += sum_ms_percentage_array[cid * MAX_UPS_COUNT_ONE_CLUSTER + paxos_id];
      }
    }
  }
  else
  {
    if (CHUNK_SERVER == type)
    {
      sum = sum_cs_percentage_array[cluster_id * MAX_UPS_COUNT_ONE_CLUSTER + paxos_id];
    }
    else if (MERGE_SERVER == type)
    {
      sum = sum_ms_percentage_array[cluster_id * MAX_UPS_COUNT_ONE_CLUSTER + paxos_id];
    }
  }
  return sum;
}

void ObUpsList::get_ups(int64_t paxos_id, int64_t cluster_id, ObServer &update_server, ObServerType server_type) const
{
  int32_t cur_percent = 0;
  int32_t total_percent = 0;
  int32_t sum_percent = 0;
  int32_t random_percent = 0;

  if (0 > paxos_id || paxos_id >= MAX_UPS_COUNT_ONE_CLUSTER)
  {
    YYSYS_LOG(WARN, "paxos_id is out of range, which shoud in [0, %ld]", MAX_UPS_COUNT_ONE_CLUSTER - 1);
  }
  else if (-1 > cluster_id || cluster_id >= OB_MAX_CLUSTER_COUNT)
  {
    YYSYS_LOG(WARN, "cluster_id is out of range, which shoud in [0, %ld]", OB_MAX_CLUSTER_COUNT - 1);
  }
  else if (0 < (sum_percent = get_sum_percentage(paxos_id, cluster_id, server_type)))
  {
    random_percent = static_cast<int32_t>(random() % sum_percent);
    for (int32_t k = 0; k < ups_count_; k++)
    {
      if (paxos_id == ups_array_[k].paxos_id_
          && (OB_ALL_CLUSTER_FLAG == cluster_id ? true : cluster_id == ups_array_[k].cluster_id_))
      {
        cur_percent = ups_array_[k].get_read_percentage(server_type);
        total_percent += cur_percent;
        if ((random_percent < total_percent) && (cur_percent > 0))
        {
          update_server = ups_array_[k].get_server(server_type);
          YYSYS_LOG(DEBUG, "got the update server [%s],sum_percent[%d],random_percent[%d],"
                    "cur_percent[%d], total_percent[%d], paxos_id[%ld], read_type[%s]",
                    update_server.to_cstring(), sum_percent, random_percent, cur_percent, total_percent,
                    paxos_id, server_type == CHUNK_SERVER ? "CHUNK_SERVER" : "MERGE_SERVER");
          break;
        }
      }
    }
  }
}

int64_t ObUpsList::get_master_ups_cluster_id(int64_t paxos_id) const
{
  int64_t cluster_id = OB_INVALID_INDEX;
  for (int32_t k = 0; k < ups_count_; k++)
  {
    if (paxos_id == ups_array_[k].paxos_id_ && ups_array_[k].stat_ == UPS_MASTER)
    {
      cluster_id = ups_array_[k].cluster_id_;
    }
  }
  return cluster_id;
}
//add for [402-bugfix ups select]-e

void ObUpsList::print() const
{
  for (int32_t i = 0; i < ups_count_; ++i)
  {
    //mod peiouya [MultiUPS] [UPS_Manage_Function] 20150422:b
    // YYSYS_LOG(INFO, "ups_list, idx=%d addr=%s inner_port=%d ms_read_percentage=%hhd cs_read_percentage=%hhd",
    // i, to_cstring(ups_array_[i].addr_),
    // ups_array_[i].inner_port_,
    // ups_array_[i].ms_read_percentage_,
    // ups_array_[i].cs_read_percentage_);
    YYSYS_LOG(INFO, "ups_list, idx=%d addr=%s inner_port=%d ms_read_percentage=%hhd cs_read_percentage=%hhd cluster_id_=%ld paxos_id_=%ld",
              i, to_cstring(ups_array_[i].addr_),
              ups_array_[i].inner_port_,
              ups_array_[i].ms_read_percentage_,
              ups_array_[i].cs_read_percentage_,
              ups_array_[i].cluster_id_,
              ups_array_[i].paxos_id_);
    //mod 20150422:e
  }
}

void ObUpsList::print(char* buf, const int64_t buf_len, int64_t &pos) const
{
  for (int32_t i = 0; i < ups_count_; ++i)
  {
    //mod peiouya [MultiUPS] [UPS_Manage_Function] 20150422:b
    // databuff_printf(buf, buf_len, pos, "%s(%d %hhd %hhd) ",
    // to_cstring(ups_array_[i].addr_),
    // ups_array_[i].inner_port_,
    // ups_array_[i].ms_read_percentage_,
    // ups_array_[i].cs_read_percentage_);
    databuff_printf(buf, buf_len, pos, "%s(%d %hhd %hhd %ld %ld) ",
                    to_cstring(ups_array_[i].addr_),
                    ups_array_[i].inner_port_,
                    ups_array_[i].ms_read_percentage_,
                    ups_array_[i].cs_read_percentage_,
                    ups_array_[i].cluster_id_,
                    ups_array_[i].paxos_id_);
    //mod 20150422:e
  }
}
