/*
* Version: $ObPaxos V0.1$
*
* Authors:
*  pangtianze <pangtianze@ecnu.cn>
*
* Date:
*  20150609
*
*  - message struct from rs to rs
*
*/
#include "ob_rs_rs_message.h"
#include <yysys.h>

using namespace oceanbase::common;

int ObMsgRsLeaderBroadcast::serialize(char* buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = addr_.serialize(buf, buf_len, pos)))
  {
    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, lease_)))
  {
    YYSYS_LOG(ERROR, "failed to lease, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, term_)))
  {
    YYSYS_LOG(ERROR, "failed to serailize rs term, err=%d", ret);
  }
  return ret;
}
int ObMsgRsLeaderBroadcast::deserialize(const char* buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = addr_.deserialize(buf, data_len, pos)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &lease_)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize lease, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &term_)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize rs term, err=%d", ret);
  }
  return ret;
}

int ObMsgRsLeaderBroadcastResp::serialize(char* buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = addr_.serialize(buf, buf_len, pos)))
  {
    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, term_)))
  {
    YYSYS_LOG(ERROR, "failed to serailize rs term, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_bool(buf, buf_len, pos, is_granted_)))
  {
    YYSYS_LOG(ERROR, "failed to serailize is_granted, err=%d", ret);
  }
  return ret;
}
int ObMsgRsLeaderBroadcastResp::deserialize(const char* buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = addr_.deserialize(buf, data_len, pos)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &term_)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize rs term, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_bool(buf, data_len, pos, &is_granted_)))
  {
    YYSYS_LOG(ERROR, "failed to serailize is_granted, err=%d", ret);
  }
  return ret;
}

int ObMsgRsRequestVote::serialize(char* buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = addr_.serialize(buf, buf_len, pos)))
  {
    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, lease_)))
  {
    YYSYS_LOG(ERROR, "failed to lease, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, term_)))
  {
    YYSYS_LOG(ERROR, "failed to serailize rs term, err=%d", ret);
  }
  return ret;
}
int ObMsgRsRequestVote::deserialize(const char* buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = addr_.deserialize(buf, data_len, pos)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &lease_)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize lease, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &term_)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize rs term, err=%d", ret);
  }
  return ret;
}

int ObMsgRsRequestVoteResp::serialize(char* buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = addr_.serialize(buf, buf_len, pos)))
  {
    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, term_)))
  {
    YYSYS_LOG(ERROR, "failed to serailize rs term, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_bool(buf, buf_len, pos, is_granted_)))
  {
    YYSYS_LOG(ERROR, "failed to serailize is_granted, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_bool(buf, buf_len, pos, need_regist_)))
  {
    YYSYS_LOG(ERROR, "failed to serailize need_regist, err=%d", ret);
  }
  return ret;
}
int ObMsgRsRequestVoteResp::deserialize(const char* buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = addr_.deserialize(buf, data_len, pos)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &term_)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize rs term, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_bool(buf, data_len, pos, &is_granted_)))
  {
    YYSYS_LOG(ERROR, "failed to serailize is_granted, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_bool(buf, data_len, pos, &need_regist_)))
  {
    YYSYS_LOG(ERROR, "failed to serailize need_regist, err=%d", ret);
  }
  return ret;
}

int ObMsgRsHeartbeat::serialize(char* buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = addr_.serialize(buf, buf_len, pos)))
  {
    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, lease_)))
  {
    YYSYS_LOG(ERROR, "failed to lease, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, current_term_)))
  {
    YYSYS_LOG(ERROR, "failed to serailize rs term, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, config_version_)))
  {
    YYSYS_LOG(ERROR, "failed to serailize config version, err=%d", ret);
  }
    else if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, pos, paxos_num_)))
  {
    YYSYS_LOG(ERROR, "failed to serailize paxos num, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, pos, quorum_scale_)))
  {
    YYSYS_LOG(ERROR, "failed to serailize quorum scale, err=%d", ret);
  }
  //add for [paoxs ups quorum manager]-b
  else if (OB_SUCCESS != (ret = serialization::encode_vstr(buf, buf_len, pos, paxos_ups_quorum_scales_, OB_MAX_PAXOS_UPS_QUORUM_SCALES_LENGTH)))
  {
    YYSYS_LOG(ERROR, "failed to serailize paxos ups quorum scales[%s], err=%d", (char *)paxos_ups_quorum_scales_, ret);
  }
   //add for [paoxs ups quorum manager]-e
   //add for [paoxs]-b
  else if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, pos, master_cluster_id_)))
  {
    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  //add for [paoxs]-e
  //add wangdonghui [paxos daily merge] 20170510 :b
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, last_frozen_time_)))
  {
    YYSYS_LOG(ERROR, "failed to serailize last frozen time, err=%d", ret);
  }
  //add :e
  //add liuzy [MultiUPS] [take_cluster_offline_interface] 20160606:b
  else if (OB_SUCCESS != (ret = serialization::encode_bool(buf, buf_len, pos, has_offline_)))
  {
    YYSYS_LOG(ERROR, "failed to serailize cluster id, err=%d", ret);
  }
  //add 20160606:e
  return ret;
}
int ObMsgRsHeartbeat::deserialize(const char* buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t strActualLen = -1;

  if (OB_SUCCESS != (ret = addr_.deserialize(buf, data_len, pos)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &lease_)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize lease, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &current_term_)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize rs term, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &config_version_)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize config version, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, data_len, pos, &paxos_num_)))
  {
    YYSYS_LOG(ERROR, "failed to serailize paxos num, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, data_len, pos, &quorum_scale_)))
  {
    YYSYS_LOG(ERROR, "failed to serailize quorum scale, err=%d", ret);
  }
  //add for [paoxs ups quorum manager]-b
  else if (NULL == serialization::decode_vstr(buf, data_len, pos, paxos_ups_quorum_scales_, OB_MAX_PAXOS_UPS_QUORUM_SCALES_LENGTH, &strActualLen))
  {
    YYSYS_LOG(ERROR, "failed to serailize quorum scale, err=%d", ret);
  }
   //add for [paoxs ups quorum manager]-e
   //add for [paoxs]-b
  else if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, data_len, pos, &master_cluster_id_)))
  {
    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  //add for [paoxs]-e
  //add wangdonghui [paxos daily merge] 20170510 :b
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &last_frozen_time_)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize last frozen time, err=%d", ret);
  }
  //add :e
  //add liuzy [MultiUPS] [take_cluster_offline_interface] 20160606:b
  else if (OB_SUCCESS != (ret = serialization::decode_bool(buf, data_len, pos, &has_offline_)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize cluster id, err=%d", ret);
  }
  //add 20160606:e
  return ret;
}
//add for [paxos for online upgrade]-b
int ObMsgRsHeartbeat::deserialize_v1(const char* buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  if (OB_SUCCESS != (ret = addr_.deserialize(buf, data_len, pos)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &lease_)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize lease, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &current_term_)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize rs term, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &config_version_)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize config version, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, data_len, pos, &paxos_num_)))
  {
    YYSYS_LOG(ERROR, "failed to serailize paxos num, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &last_frozen_time_)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize last frozen time, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_bool(buf, data_len, pos, &has_offline_)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize cluster id, err=%d", ret);
  }
  return ret;
}

int ObMsgRsHeartbeat::deserialize_v2(const char* buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  if (OB_SUCCESS != (ret = addr_.deserialize(buf, data_len, pos)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &lease_)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize lease, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &current_term_)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize rs term, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &config_version_)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize config version, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, data_len, pos, &paxos_num_)))
  {
    YYSYS_LOG(ERROR, "failed to serailize paxos num, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, data_len, pos, &quorum_scale_)))
  {
    YYSYS_LOG(ERROR, "failed to serailize quorum scale, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, data_len, pos, &master_cluster_id_)))
  {
    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &last_frozen_time_)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize last frozen time, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_bool(buf, data_len, pos, &has_offline_)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize cluster id, err=%d", ret);
  }
  return ret;
}
//add for [paxos for online upgrade]-e

int ObMsgRsHeartbeatResp::serialize(char* buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = addr_.serialize(buf, buf_len, pos)))
  {
    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, current_term_)))
  {
    YYSYS_LOG(ERROR, "failed to serailize rs term, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, pos, paxos_num_)))
  {
    YYSYS_LOG(ERROR, "failed to serailize paxos num, err=%d", ret);
  }
  //add pangtianze [Paxos rs_election] 20161010:b
  else if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, pos, quorum_scale_)))
  {
    YYSYS_LOG(ERROR, "failed to serailize quorum scale, err=%d", ret);
  }
  //add:e
  //add for [paxos ups quorum manager]-b
  else if (OB_SUCCESS != (ret = serialization::encode_vstr(buf, buf_len, pos, paxos_ups_quorum_scales_, OB_MAX_PAXOS_UPS_QUORUM_SCALES_LENGTH)))
  {
    YYSYS_LOG(ERROR, "failed to serailize paxos ups quorum scales[%s], err=%d", paxos_ups_quorum_scales_, ret);
  }
  //add for [paoxs ups quorum manager]-e
  return ret;
}

//add for [paoxs ups quorum manager]-b
int ObMsgRsHeartbeatResp::serialize_v2(char* buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = addr_.serialize(buf, buf_len, pos)))
  {
    YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, current_term_)))
  {
    YYSYS_LOG(ERROR, "failed to serailize rs term, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, pos, paxos_num_)))
  {
    YYSYS_LOG(ERROR, "failed to serailize paxos num, err=%d", ret);
  }
  //add pangtianze [Paxos rs_election] 20161010:b
  else if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, pos, quorum_scale_)))
  {
    YYSYS_LOG(ERROR, "failed to serailize quorum scale, err=%d", ret);
  }
  //add:e
  return ret;
}//add for [paoxs ups quorum manager]-e

int ObMsgRsHeartbeatResp::deserialize(const char* buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t strActualLen = -1;
  if (OB_SUCCESS != (ret = addr_.deserialize(buf, data_len, pos)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &current_term_)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize rs term, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, data_len, pos, &paxos_num_)))
  {
    YYSYS_LOG(ERROR, "failed to serailize paxos num, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, data_len, pos, &quorum_scale_)))
  {
    YYSYS_LOG(ERROR, "failed to serailize quorum scale, err=%d", ret);
  }
  //add for [paoxs ups quorum manager]-b
  else if (NULL == serialization::decode_vstr(buf, data_len, pos, paxos_ups_quorum_scales_, OB_MAX_PAXOS_UPS_QUORUM_SCALES_LENGTH, &strActualLen))
  {
    YYSYS_LOG(ERROR, "failed to deserialize quorum scale, err=%d", ret);
  }
   //add for [paoxs ups quorum manager]-e
  return ret;
}

//add chujiajia [Paxos rs_election] 20151030:b
int ObMsgRsChangePaxosNum::serialize(char* buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = addr_.serialize(buf, buf_len, pos)))
  {
    YYSYS_LOG(ERROR, "failed to serialize addr_, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, pos, new_paxos_num_)))
  {
    YYSYS_LOG(ERROR, "failed to serailize rs new_paxos_num_, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, term_)))
  {
    YYSYS_LOG(ERROR, "failed to serailize rs term, err=%d", ret);
  }
  return ret;
}

int ObMsgRsChangePaxosNum::deserialize(const char* buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = addr_.deserialize(buf, data_len, pos)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize addr_, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, data_len, pos, &new_paxos_num_)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize rs new_paxos_num_, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &term_)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize rs term, err=%d", ret);
  }
  return ret;
}

int ObMsgRsNewQuorumScale::serialize(char* buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = addr_.serialize(buf, buf_len, pos)))
  {
    YYSYS_LOG(ERROR, "failed to serialize addr_, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, pos, new_ups_quorum_scale_)))
  {
    YYSYS_LOG(ERROR, "failed to serailize rs new_paxos_num_, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, paxos_group_id_)))
  {
    YYSYS_LOG(ERROR, "failed to serailize rs paxos_group_id_, err=%d", ret);
  }
  //add for [paxos ups quorum manager]-b
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, term_)))
  {
    YYSYS_LOG(ERROR, "failed to serailize rs term, err=%d", ret);
  }
  //add for [paxos ups quorum manager]-e
  return ret;
}

int ObMsgRsNewQuorumScale::deserialize(const char* buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = addr_.deserialize(buf, data_len, pos)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize addr_, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, data_len, pos, &new_ups_quorum_scale_)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize rs new_paxos_num_, err=%d", ret);
  }
  //add for [paxos ups quorum manager]-b
  else if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, data_len, pos, &paxos_group_id_)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize rs paxos_group_id_, err=%d", ret);
  }
  //add for [paxos ups quorum manager]-e
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &term_)))
  {
    YYSYS_LOG(ERROR, "failed to deserialize rs term, err=%d", ret);
  }
  return ret;
}

