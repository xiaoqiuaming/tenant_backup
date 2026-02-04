/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_rs_ups_message.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_rs_ups_message.h"
#include <yysys.h>

using namespace oceanbase::common;

int ObMsgUpsHeartbeat::serialize(char* buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = ups_master_.serialize(buf, buf_len, pos)))
  {
    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, self_lease_)))
  {
    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  //add chujiajia [Paxos rs_election] 20151229:b
  else if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, pos, ups_quorum_)))
  {
      YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  //add:e
  //del peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//  else if (OB_SUCCESS != (ret = obi_role_.serialize(buf, buf_len, pos)))
//  {
//    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
//  }
  //del 20150701:e
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, schema_version_)))
  {
    YYSYS_LOG(ERROR, "serailize schema_version fail, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, config_version_)))
  {
    YYSYS_LOG(ERROR, "serailize config_version fail, err=%d", ret);
  }
  //add peiouya [MultiUPS] [UPS_Manage_Function] 20150521:b
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, last_frozen_version_)))
  {
    YYSYS_LOG(ERROR, "serailize last_frozen_version_ fail, err=%d", ret);
  }
  //add 20150521:e
  //add peiouya [MultiUPS] [UPS_Manage_Function] 20150615:b
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, to_be_frozen_version_)))
  {
    YYSYS_LOG(ERROR, "serailize to_be_frozen_version_ fail, err=%d", ret);
  }
  //add 20150605:e
  //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160318:b
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, offline_version_)))
  {
    YYSYS_LOG(ERROR, "serailize offline_version_ fail, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_bool(buf, buf_len, pos, minor_freeze_flag_)))
  {
    YYSYS_LOG(ERROR, "serailize minor_freeze_flag_ fail, err=%d", ret);
  }
  //add 20160318:e
  return ret;
}

int ObMsgUpsHeartbeat::deserialize(const char* buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = ups_master_.deserialize(buf, data_len, pos)))
  {
    YYSYS_LOG(ERROR, "deserialize error");
    ret = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &self_lease_)))
  {
    YYSYS_LOG(ERROR, "deserialize error");
    ret = OB_INVALID_ARGUMENT;
  }
  //add chujiajia [Paxos rs_election] 20151229:b
  else if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, data_len, pos, &ups_quorum_)))
  {
    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  //add:e
  //del peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//  else if (OB_SUCCESS != (ret = obi_role_.deserialize(buf, data_len, pos)))
//  {
//    YYSYS_LOG(ERROR, "failed to deserialize, err=%d", ret);
//  }
  //del 20150701:e
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &schema_version_)))
  {
    YYSYS_LOG(ERROR, "deserialize schema_version fail, ret: [%d]", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &config_version_)))
  {
    YYSYS_LOG(ERROR, "deserialize config_version fail, ret: [%d]", ret);
  }
  //add peiouya [MultiUPS] [UPS_Manage_Function] 20150521:b
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &last_frozen_version_)))
  {
    YYSYS_LOG(ERROR, "deserialize last_frozen_version_ fail, ret: [%d]", ret);
  }
  //add 20150521:e
  //add peiouya [MultiUPS] [UPS_Manage_Function] 20150615:b
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &to_be_frozen_version_)))
  {
    YYSYS_LOG(ERROR, "deserialize to_be_frozen_version_ fail, ret: [%d]", ret);
  }
  //add 20150605:e
  //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160318:b
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &offline_version_)))
  {
    YYSYS_LOG(ERROR, "deserailize offline_version_ fail, err=%d", ret);
  }
  //add 20160318:e
  else if (OB_SUCCESS != (ret = serialization::decode_bool(buf, data_len, pos, &minor_freeze_flag_)))
  {
    YYSYS_LOG(ERROR, "deserailize minor_freeze_flag_ fail, err=%d", ret);
  }
  return ret;
}

//add for [minor freeze for online upgrade]-b
int ObMsgUpsHeartbeat::deserialize_v4(const char* buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = ups_master_.deserialize(buf, data_len, pos)))
  {
    YYSYS_LOG(ERROR, "deserialize error");
    ret = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &self_lease_)))
  {
    YYSYS_LOG(ERROR, "deserialize error");
    ret = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, data_len, pos, &ups_quorum_)))
  {
    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &schema_version_)))
  {
    YYSYS_LOG(ERROR, "deserialize schema_version fail, ret: [%d]", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &config_version_)))
  {
    YYSYS_LOG(ERROR, "deserialize config_version fail, ret: [%d]", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &last_frozen_version_)))
  {
    YYSYS_LOG(ERROR, "deserialize last_frozen_version_ fail, ret: [%d]", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &to_be_frozen_version_)))
  {
    YYSYS_LOG(ERROR, "deserialize to_be_frozen_version_ fail, ret: [%d]", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &offline_version_)))
  {
    YYSYS_LOG(ERROR, "deserailize offline_version_ fail, err=%d", ret);
  }
  return ret;
}
//add for [minor freeze for online upgrade]-e

int ObMsgUpsHeartbeatResp::serialize(char* buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = addr_.serialize(buf, buf_len, pos)))
  {
    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len,
                                                           pos, (int32_t)status_)))
  {
    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  //del peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//  else if (OB_SUCCESS != (ret = obi_role_.serialize(buf, buf_len, pos)))
//  {
//    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
//  }
  //del 20150701:e
  //add chujiajia [Paxos rs_election] 20151229:b
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, quorum_scale_)))
  {
    YYSYS_LOG(ERROR, "failed to serialize quorum_scale_, err=%d", ret);
  }
  //add:e
  //add peiouya [MultiUPS] [UPS_Manage_Function] 20150521:b
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, last_frozen_version_)))
  {
    YYSYS_LOG(ERROR, "failed to serialize last_frozen_version_, err=%d", ret);
  }
  //add 20150521:e
  //add peiouya [MultiUPS] [UPS_Manage_Function] 20150527:b
  else if (OB_SUCCESS != (ret = serialization::encode_bool(buf, buf_len, pos, is_active_)))
  {
    YYSYS_LOG(ERROR, "failed to serialize is_active_, err=%d", ret);
  }
  //add 20150527:e
  //add peiouya [MultiUPS] [PARTITION_LOCK_FUCTION] 20150530:b
  else if (OB_SUCCESS != (ret = serialization::encode_bool(buf, buf_len, pos, partition_lock_flag_)))
  {
    YYSYS_LOG(ERROR, "failed to serialize partition_lock_flag_, err=%d", ret);
  }
  //add 20150530:e
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, minor_freeze_stat_)))
  {
    YYSYS_LOG(ERROR, "failed to serialize minor_freeze_stat_, err=%d", ret);
  }
  return ret;
}

int ObMsgUpsHeartbeatResp::deserialize(const char* buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int32_t status = -1;
  if (OB_SUCCESS != (ret = addr_.deserialize(buf, data_len, pos)))
  {
    YYSYS_LOG(ERROR, "deserialize error");
    ret = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, data_len, pos, &status)))
  {
    YYSYS_LOG(ERROR, "deserialize error");
    ret = OB_INVALID_ARGUMENT;
  }
  //del peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//  else if (OB_SUCCESS != (ret = obi_role_.deserialize(buf, data_len, pos)))
//  {
//    YYSYS_LOG(ERROR, "failed to deserialize, err=%d", ret);
//  }
  //del 20150701:e
  //add chujiajia [Paxos rs_election] 20151229:b
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &quorum_scale_)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize quorum_scale_, err=%d", ret);
  }
  //add:e
  //add peiouya [MultiUPS] [UPS_Manage_Function] 20150521:b
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &last_frozen_version_)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize last_frozen_version_, err=%d", ret);
  }
  //add 20150521:e
  //add peiouya [MultiUPS] [UPS_Manage_Function] 20150527:b
  else if (OB_SUCCESS != (ret = serialization::decode_bool(buf, data_len, pos, &is_active_)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize is_active_, err=%d", ret);
  }
  //add 20150527:e
  //add peiouya [MultiUPS] [PARTITION_LOCK_FUCTION] 20150530:b
  else if (OB_SUCCESS != (ret = serialization::decode_bool(buf, data_len, pos, &partition_lock_flag_)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize partition_lock_flag_, err=%d", ret);
  }
  //add 20150530:e
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &minor_freeze_stat_)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize minor_freeze_stat_, err=%d", ret);
  }
  else
  {
    if (SYNC == status)
    {
      status_ = SYNC;
    }
    else if (NOTSYNC == status)
    {
      status_ = NOTSYNC;
    }
    else
    {
      YYSYS_LOG(ERROR, "unknown status=%d", status);
      ret = OB_INVALID_ARGUMENT;
    }
  }
  return ret;
}

//add for [minor freeze for online upgrade]-b
int ObMsgUpsHeartbeatResp::serialize_v2(char* buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = addr_.serialize(buf, buf_len, pos)))
  {
    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len,
                                                           pos, (int32_t)status_)))
  {
    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, quorum_scale_)))
  {
    YYSYS_LOG(ERROR, "failed to serialize quorum_scale_, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, last_frozen_version_)))
  {
    YYSYS_LOG(ERROR, "failed to serialize last_frozen_version_, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_bool(buf, buf_len, pos, is_active_)))
  {
    YYSYS_LOG(ERROR, "failed to serialize is_active_, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_bool(buf, buf_len, pos, partition_lock_flag_)))
  {
    YYSYS_LOG(ERROR, "failed to serialize partition_lock_flag_, err=%d", ret);
  }
  return ret;
}

int ObMsgUpsHeartbeatResp::deserialize_v2(const char* buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int32_t status = -1;
  if (OB_SUCCESS != (ret = addr_.deserialize(buf, data_len, pos)))
  {
    YYSYS_LOG(ERROR, "deserialize error");
    ret = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, data_len, pos, &status)))
  {
    YYSYS_LOG(ERROR, "deserialize error");
    ret = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &quorum_scale_)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize quorum_scale_, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &last_frozen_version_)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize last_frozen_version_, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_bool(buf, data_len, pos, &is_active_)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize is_active_, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_bool(buf, data_len, pos, &partition_lock_flag_)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize partition_lock_flag_, err=%d", ret);
  }
  else
  {
    if (SYNC == status)
    {
      status_ = SYNC;
    }
    else if (NOTSYNC == status)
    {
      status_ = NOTSYNC;
    }
    else
    {
      YYSYS_LOG(ERROR, "unknown status=%d", status);
      ret = OB_INVALID_ARGUMENT;
    }
  }
  return ret;
}
//add for [minor freeze for online upgrade]-e

int ObMsgUpsRegister::serialize(char* buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = addr_.serialize(buf, buf_len, pos)))
  {
    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len,
                                                           pos, inner_port_)))
  {
    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len,
                                                           pos, log_seq_num_)))
  {
    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len,
                                                           pos, lease_)))
  {
    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  //add peiouya [MultiUPS] [UPS_Manage_Function] 20150521:b
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len,
                                                           pos, last_frozen_version_)))
  {
    YYSYS_LOG(ERROR, "failed to serialize last_frozen_version_, err=%d", ret);
  }
  //add 20150521:e
  //add shili [MUTIUPS] [START UPS]  20150427:b
  else if(OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len,
                                                          pos, paxos_id_)))
  {
    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  //add 20150427:e
  else if (OB_SUCCESS != (ret = serialization::encode_vstr(buf, buf_len,
                                                           pos, server_version_)))
  {
    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  return ret;
}

int ObMsgUpsRegister::deserialize(const char* buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t server_version_length = 0;
  const char * server_version_temp = NULL;
  if (OB_SUCCESS != (ret = addr_.deserialize(buf, data_len, pos)))
  {
    YYSYS_LOG(ERROR, "deserialize error");
    ret = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, data_len, pos, &inner_port_)))
  {
    YYSYS_LOG(ERROR, "deserialize error");
    ret = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &log_seq_num_)))
  {
    YYSYS_LOG(ERROR, "deserialize error");
    ret = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &lease_)))
  {
    YYSYS_LOG(ERROR, "deserialize error");
    ret = OB_INVALID_ARGUMENT;
  }
  //add peiouya [MultiUPS] [UPS_Manage_Function] 20150521:b
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &last_frozen_version_)))
  {
    YYSYS_LOG(ERROR, "deserialize error");
    ret = OB_INVALID_ARGUMENT;
  }
  //add 20150521:e
  //add shili [MUTIUPS] [START UPS]  20150427:b
  else if(OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &paxos_id_)))
  {
    YYSYS_LOG(ERROR, "deserialize error");
    ret = OB_INVALID_ARGUMENT;;
  }
  //add 20150427:e
  else if (NULL == (server_version_temp = serialization::decode_vstr(buf, data_len, pos, &server_version_length)))
  {
    YYSYS_LOG(ERROR, "deserialize error");
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    memcpy(server_version_, server_version_temp, server_version_length);
  }
  return ret;
}

int ObMsgUpsSlaveFailure::serialize(char* buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = my_addr_.serialize(buf, buf_len, pos)))
  {
    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = slave_addr_.serialize(buf, buf_len, pos)))
  {
    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  return ret;
}

int ObMsgUpsSlaveFailure::deserialize(const char* buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = my_addr_.deserialize(buf, data_len, pos)))
  {
    YYSYS_LOG(ERROR, "deserialize error");
    ret = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (ret = slave_addr_.deserialize(buf, data_len, pos)))
  {
    YYSYS_LOG(ERROR, "deserialize error");
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

int ObMsgRevokeLease::serialize(char* buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, lease_)))
  {
    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = ups_master_.serialize(buf, buf_len, pos)))
  {
    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  return ret;
}

int ObMsgRevokeLease::deserialize(const char* buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &lease_)))
  {
    YYSYS_LOG(ERROR, "deserialize error");
    ret = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (ret = ups_master_.deserialize(buf, data_len, pos)))
  {
    YYSYS_LOG(ERROR, "deserialize error");
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

