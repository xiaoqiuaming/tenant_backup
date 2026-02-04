/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yuanqi <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 */
#ifndef __OB_COMMON_OB_TRANSACTION_H__
#define __OB_COMMON_OB_TRANSACTION_H__
#include "ob_define.h"
#include "ob_server.h"
#include "ob_string.h"
namespace oceanbase
{
  namespace common
  {
    enum IsolationLevel
    {
      NO_LOCK = 1,
      READ_COMMITED = 2,
      REPEATABLE_READ = 3,
      SERIALIZABLE = 4,
    };

    enum TransType
    {
      READ_ONLY_TRANS = 0,
      READ_WRITE_TRANS = 1,
      INTERNAL_WRITE_TRANS = 2, // 不加锁,避免触发trigger
      REPLAY_TRANS = 3, // 不加锁，
    };
    // add by maosy [Delete_Update_Function_isolation_RC] 20161228
    enum BatchReadTimes//标记批量读取时机的标记量
    {
      NO_BATCH_UD = 0,
      FIRST_SELECT_READ = 1 ,
      CLEAR_TIMESTAMP = 2 ,
    };
    // add e

    enum DisTransRole
    {
      INVALID_VALUE = 0 ,
      COORDINATOR = 1 ,
      PARTTICIPANT = 2 ,  //dduuhhtt
    };

    struct ObTransReq
    {
        ObTransReq(): type_(READ_WRITE_TRANS), isolation_(READ_COMMITED),
          start_time_(0),
          timeout_(OB_DEFAULT_SESSION_TIMEOUT),
          idle_time_(timeout_)
        {}
        ~ObTransReq()
        {}
        int32_t type_;
        int32_t isolation_;
        int64_t start_time_;
        int64_t timeout_;
        int64_t idle_time_;

        int64_t to_string(char* buf, int64_t len) const;
        int set_isolation_by_name(const ObString &isolation);
        NEED_SERIALIZE_AND_DESERIALIZE;
    };

    struct ObTransID
    {
        const static uint32_t INVALID_SESSION_ID = 0;
        ObTransID(): descriptor_(INVALID_SESSION_ID), ups_(), start_time_us_(0)
        ,read_times_(NO_BATCH_UD) /*add by maosy [Delete_Update_Function_for_snpshot] 20161210*/
        ,dis_trans_role_(INVALID_VALUE)
        ,paxos_id_(-1)
        ,is_unset_trans_(false)
        {}
        ~ObTransID()
        {}
        void reset();
        bool is_valid()const;
        int64_t to_string(char* buf, int64_t len) const;
        //add shili [LONG_TRANSACTION_LOG]  20160926:b
        bool equal(const ObTransID other) const;
        int serialize_4_biglog(char *buf, const int64_t buf_len, int64_t &pos) const;
        int deserialize_4_biglog(const char *buf, const int64_t data_len, int64_t &pos);
        int64_t get_serialize_size_4_biglog(void) const;
        //add e

        //[304]
        int serialize_for_prepare(char *buf, const int64_t buf_len, int64_t &pos) const;
        int deserialize_for_prepare(const char *buf, const int64_t data_len, int64_t &pos);
        int64_t get_serialize_size_for_prepare(void) const;


        //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
        //used for hashmap and skiplist
        bool operator < (const ObTransID &r_value) const;
        bool operator == (const ObTransID &r_value) const;
        inline int64_t hash() const
        {
          return start_time_us_;
        }
        //add 20150701:e
        NEED_SERIALIZE_AND_DESERIALIZE;
        uint32_t descriptor_;
        common::ObServer ups_;
        int64_t start_time_us_;
        // mod by maosy [Delete_Update_Function_isolation_RC] 20161218 b:
        int64_t read_times_;
        //add by maosy  20161210

        int64_t dis_trans_role_;
        int64_t paxos_id_;
        bool is_unset_trans_;

    };
    //add by maosy [MultiUPS 1.0] [read uncommit]20170525 b:
    struct ObPartitionTransID
    {
        ObPartitionTransID():transid_()
        {
          paxos_id_ = OB_INVALID_PAXOS_ID;
          published_transid_ = OB_INVALID_DATA;
        }
        ~ObPartitionTransID()
        {}
        void reset()
        {
          transid_.reset();
          paxos_id_ = OB_INVALID_PAXOS_ID;
          published_transid_ = OB_INVALID_DATA;
        }
        NEED_SERIALIZE_AND_DESERIALIZE;
        int64_t to_string(char* buf, int64_t len) const;
        ObTransID transid_;
        int64_t paxos_id_;
        int64_t published_transid_;
    };
    // add by maosy e

    struct ObEndTransReq
    {
        //mod peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
        //ObEndTransReq(): trans_id_(), rollback_(false) {}
        ObEndTransReq(): trans_id_(), rollback_(false),participant_num_(0),memtable_version_(0)
        {}
        //mod 20150701:e
        ~ObEndTransReq()
        {}
        int64_t to_string(char* buf, int64_t len) const;
        NEED_SERIALIZE_AND_DESERIALIZE;
        ObTransID trans_id_;
        bool   rollback_;
        //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
        int32_t participant_num_;
        int64_t memtable_version_;
        //if contain coordinator trans id, default save it in participant_trans_id_[0]
        ObTransID participant_trans_id_[common::MAX_UPS_COUNT_ONE_CLUSTER];
        //add 20150701:e
    };
  }; // end namespace common
}; // end namespace oceanbase

#endif /* __OB_COMMON_OB_TRANSACTION_H__ */
