/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_rs_ups_message.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_RS_UPS_MESSAGE_H
#define _OB_RS_UPS_MESSAGE_H 1
#include "ob_server.h"
#include "data_buffer.h"
#include "ob_result.h"
#include "ob_obi_role.h"
namespace oceanbase
{
  namespace common
  {
    struct ObMsgUpsHeartbeat
    {
      static const int MY_VERSION = 5;
      ObServer ups_master_;
      int64_t self_lease_;
      //add lbzhong [Paxos ups_replication] 20151030:b
      int32_t ups_quorum_;
      //add:e
      //del peiouya [MultiUPS] [DELETE_OBI] 20150701:b
      //ObiRole obi_role_;
      //del 20150701:e
      int64_t schema_version_;
      int64_t config_version_;
      //add peiouya [MultiUPS] [UPS_Manage_Function] 20150521:b
      int64_t last_frozen_version_;
      //add 20150521:e
      //add peiouya [MultiUPS] [UPS_Manage_Function] 20150615:b
      int64_t to_be_frozen_version_;
      //add 20150605:e
      //add peiouya [MultiUPS] [PARTITION_LOCK_FUCTION] 20150830:b
      bool partition_unlock_flag_;
      //add 20150830:e
      //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160318:b
      int64_t offline_version_;
      //add 20160318:e
      bool minor_freeze_flag_;
      int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
      int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
      int deserialize_v4(const char* buf, const int64_t data_len, int64_t& pos);
    };

    struct ObMsgUpsHeartbeatResp // aka UPS renew message
    {
      static const int MY_VERSION = 3;
      ObServer addr_;
      enum UpsSyncStatus
      {
        SYNC = 0,
        NOTSYNC = 1
      } status_;
      //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
      //ObiRole obi_role_;
      //del 20150701:e
      //add chujiajia [Paxos rs_election] 20151229:b
      int64_t quorum_scale_;
      //add:e
      //add peiouya [MultiUPS] [UPS_Manage_Function] 20150521:b
      int64_t last_frozen_version_;
      //add 20150521:e
      bool is_active_;
      //add peiouya [MultiUPS] [PARTITION_LOCK_FUCTION] 20150830:b
      //for lock partition signal
      bool partition_lock_flag_;
      //add 20150830:e
      int64_t minor_freeze_stat_;
      int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
      int deserialize(const char* buf, const int64_t data_len, int64_t& pos);

      int serialize_v2(char* buf, const int64_t buf_len, int64_t& pos) const;
      int deserialize_v2(const char* buf, const int64_t data_len, int64_t& pos);
    };

    struct ObMsgUpsRegister
    {
      static const int MY_VERSION = 1;
      ObServer addr_;
      int32_t inner_port_;
      int64_t log_seq_num_;
      int64_t lease_;
      //add peiouya [MultiUPS] [UPS_Manage_Function] 20150521:b
      int64_t last_frozen_version_;
      //add 20150521:e
      //mod shili [MUTIUPS] [START¡¡UPS]  20150427:b
      int64_t paxos_id_;
      //mod  20150427:e
      char server_version_[OB_SERVER_VERSION_LENGTH];
      int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
      int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
    };

    struct ObMsgUpsSlaveFailure
    {
      static const int MY_VERSION = 1;
      ObServer my_addr_;
      ObServer slave_addr_;
      int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
      int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
    };

    struct ObMsgRevokeLease
    {
      static const int MY_VERSION = 1;
      int64_t lease_;
      ObServer ups_master_;
      int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
      int deserialize(const char* buf, const int64_t data_len, int64_t& pos);
    };
  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_RS_UPS_MESSAGE_H */

