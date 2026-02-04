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
#include "ob_transaction.h"
#include "utility.h"

namespace oceanbase
{
  namespace common
  {
    // Transaction Isolcation Levels: READ-UNCOMMITTED READ-COMMITTED REPEATABLE-READ SERIALIZABLE
    int ObTransReq::set_isolation_by_name(const ObString &isolation)
    {
      int ret = OB_SUCCESS;
      if (OB_LIKELY(isolation == ObString::make_string("READ-COMMITTED")))
      {
        isolation_ = READ_COMMITED;
      }
      //mod by duyide[isolation] 20210902:b
      /*
      else if (isolation == ObString::make_string("READ-UNCOMMITTED"))
      {
        isolation_ = NO_LOCK;
      }
      else if (isolation == ObString::make_string("REPEATABLE-READ"))
      {
        isolation_ = REPEATABLE_READ;
      }
      else if (isolation == ObString::make_string("SERIALIZABLE"))
      {
        isolation_ = SERIALIZABLE;
      }
      */
      //add:e
      else
      {
        YYSYS_LOG(USER_ERROR, "Unknown isolation level `%.*s'", isolation.length(), isolation.ptr());
        ret = OB_INVALID_ARGUMENT;
      }
      return ret;
    }

    int64_t ObTransReq::to_string(char* buf, int64_t len) const
    {
      int64_t pos = 0;
      databuff_printf(buf, len, pos,
                      "TransReq(type=%d,isolation=%x,start_time=%ld,timeout=%ld,idletime=%ld)",
                      type_, isolation_, start_time_, timeout_, idle_time_);
      return pos;
    }

    int ObTransReq::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (OB_SUCCESS != (err = serialization::encode_i32(buf, buf_len, new_pos, type_)))
      {
        YYSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::encode_i32(buf, buf_len, new_pos, isolation_)))
      {
        YYSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::encode_i64(buf, buf_len, new_pos, start_time_)))
      {
        YYSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::encode_i64(buf, buf_len, new_pos, timeout_)))
      {
        YYSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::encode_i64(buf, buf_len, new_pos, idle_time_)))
      {
        YYSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else
      {
        pos = new_pos;
      }
      return err;
    }

    int ObTransReq::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (OB_SUCCESS != (err = serialization::decode_i32(buf, data_len, new_pos, &type_)))
      {
        YYSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::decode_i32(buf, data_len, new_pos, &isolation_)))
      {
        YYSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::decode_i64(buf, data_len, new_pos, &start_time_)))
      {
        YYSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::decode_i64(buf, data_len, new_pos, &timeout_)))
      {
        YYSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::decode_i64(buf, data_len, new_pos, &idle_time_)))
      {
        YYSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else
      {
        pos = new_pos;
      }
      return err;
    }

    int64_t ObTransReq::get_serialize_size(void) const
    {
      return serialization::encoded_length_i32(type_)
          + serialization::encoded_length_i32(isolation_)
          + serialization::encoded_length_i64(start_time_)
          + serialization::encoded_length_i64(timeout_)
          + serialization::encoded_length_i64(idle_time_);
    }

    void ObTransID::reset()
    {
      descriptor_ = INVALID_SESSION_ID;
      ups_.reset();
      start_time_us_ = 0;
      // mod by maosy [Delete_Update_Function_isolation_RC] 20161218 b:
      read_times_ = NO_BATCH_UD;
      // add by maosy 20161210
      dis_trans_role_ = INVALID_VALUE;
      paxos_id_ = -1;
    }

    bool ObTransID::is_valid() const
    {
      return INVALID_SESSION_ID != descriptor_;
    }


    //add shili [LONG_TRANSACTION_LOG]  20160926:b
    bool ObTransID::equal(const ObTransID other) const
    {
      bool res = true;
      if (descriptor_ != other.descriptor_)
      {
        res = false;
      }
      else if (!ups_.is_same_ip(other.ups_))
      {
        res = false;
      }
      else if(start_time_us_ != other.start_time_us_)
      {
        res = false;
      }
      //add by maosy [Delete_Update_Function_for_snpshot] 20161210
      else if(read_times_ != other.read_times_)
      {
        res =false;
      }
      // add by maosy 20161210
      else if(is_unset_trans_ != other.is_unset_trans_)
      {
        res = false;
      }
      return res;
    }
    //add e

    /*@berif ֻ���л�descriptor_  start_time_us_  ups_��������Ա��������л�
    * @note  ��Ҫ�޸� �ú�����Ϊ�˱��ֶ���־������
    */
    int ObTransID::serialize_4_biglog(char *buf, const int64_t buf_len, int64_t &pos) const
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (OB_SUCCESS != (err = serialization::encode_i32(buf, buf_len, new_pos, descriptor_)))
      {
        YYSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::encode_i64(buf, buf_len, new_pos, start_time_us_)))
      {
        YYSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else if (OB_SUCCESS != (err = ups_.serialize(buf, buf_len, new_pos)))
      {
        YYSYS_LOG(ERROR, "ups.serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else
      {
        pos = new_pos;
      }
      return err;
    }

    /*@berif ֻ�����л�descriptor_  start_time_us_  ups_��������Ա�������л�
    * @note  ��Ҫ�޸� �ú�����Ϊ�˱��ֶ���־������
    */
    int ObTransID::deserialize_4_biglog(const char *buf, const int64_t data_len, int64_t &pos)
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (OB_SUCCESS != (err = serialization::decode_i32(buf, data_len, new_pos, (int32_t*)&descriptor_)))
      {
        YYSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::decode_i64(buf, data_len, new_pos, &start_time_us_)))
      {
        YYSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if (OB_SUCCESS != (err = ups_.deserialize(buf, data_len, new_pos)))
      {
        YYSYS_LOG(ERROR, "ups.deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else
      {
        pos = new_pos;
      }
      return err;
    }

    /*@berif ֻ����descriptor_  start_time_us_  ups_��������Ա�������С
    * @note  ��Ҫ�޸� �ú�����Ϊ�˱��ֶ���־������
    */
    int64_t ObTransID::get_serialize_size_4_biglog(void) const
    {
      return serialization::encoded_length_i32(descriptor_)
          + serialization::encoded_length_i64(start_time_us_)
          + ups_.get_serialize_size();
    }
    //add e

    int64_t ObTransID::to_string(char* buf, int64_t len) const
    {
      int64_t pos = 0;
      databuff_printf(buf, len, pos, "TransID(sd=%u,ups=%s,start=%ld, paxos_id=%ld, role=%ld)",
                      descriptor_, ups_.to_cstring(), start_time_us_, paxos_id_, dis_trans_role_);
      return pos;
    }
    //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
    bool ObTransID::operator < (const ObTransID &r_value) const
    {
      return (start_time_us_ < r_value.start_time_us_);
    }

    bool ObTransID::operator == (const ObTransID &r_value) const
    {
      return ((start_time_us_ == r_value.start_time_us_) &&
              (ups_ == r_value.ups_) &&
              (descriptor_ == r_value.descriptor_));
    }
    //add 20150701:e

    int ObTransID::serialize_for_prepare(char *buf, const int64_t buf_len, int64_t &pos) const
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (OB_SUCCESS != (err = serialization::encode_i32(buf, buf_len, new_pos, descriptor_)))
      {
        YYSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::encode_i64(buf, buf_len, new_pos, start_time_us_)))
      {
        YYSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else if (OB_SUCCESS != (err = ups_.serialize(buf, buf_len, new_pos)))
      {
        YYSYS_LOG(ERROR, "ups.serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else if (OB_SUCCESS !=(err = serialization::encode_i64 (buf, buf_len, new_pos,read_times_)))
      {
        YYSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else if (OB_SUCCESS !=(err = serialization::encode_i64 (buf, buf_len, new_pos,dis_trans_role_)))
      {
        YYSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else if (OB_SUCCESS !=(err = serialization::encode_i64 (buf, buf_len, new_pos,paxos_id_)))
      {
        YYSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else
      {
        pos = new_pos;
      }
      return err;
    }

    int ObTransID::deserialize_for_prepare(const char *buf, const int64_t data_len, int64_t &pos)
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (OB_SUCCESS != (err = serialization::decode_i32(buf, data_len, new_pos, (int32_t*)&descriptor_)))
      {
        YYSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::decode_i64(buf, data_len, new_pos, &start_time_us_)))
      {
        YYSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if (OB_SUCCESS != (err = ups_.deserialize(buf, data_len, new_pos)))
      {
        YYSYS_LOG(ERROR, "ups.deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if(OB_SUCCESS != (err = serialization::decode_i64 (buf, data_len, new_pos,&read_times_)))
      {
        YYSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if(OB_SUCCESS != (err = serialization::decode_i64 (buf, data_len, new_pos,&dis_trans_role_)))
      {
        YYSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if(OB_SUCCESS != (err = serialization::decode_i64 (buf, data_len, new_pos,&paxos_id_)))
      {
        YYSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else
      {
        pos = new_pos;
      }
      return err;
    }

    int64_t ObTransID::get_serialize_size_for_prepare(void) const
    {
      return serialization::encoded_length_i32(descriptor_)
          + serialization::encoded_length_i64(start_time_us_)
          + ups_.get_serialize_size()
          + serialization::encoded_length_i64(read_times_)
          + serialization::encoded_length_i64(dis_trans_role_)
          + serialization::encoded_length_i64(paxos_id_);
    }

    int ObTransID::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (OB_SUCCESS != (err = serialization::encode_i32(buf, buf_len, new_pos, descriptor_)))
      {
        YYSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::encode_i64(buf, buf_len, new_pos, start_time_us_)))
      {
        YYSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else if (OB_SUCCESS != (err = ups_.serialize(buf, buf_len, new_pos)))
      {
        YYSYS_LOG(ERROR, "ups.serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      //add by maosy [Delete_Update_Function_for_snpshot] 20161210
      else if (OB_SUCCESS !=(err = serialization::encode_i64 (buf, buf_len, new_pos,read_times_)))
      {
        YYSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      //add by maosy 20161210
      else
      {
        pos = new_pos;
      }
      return err;
    }

    int ObTransID::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (OB_SUCCESS != (err = serialization::decode_i32(buf, data_len, new_pos, (int32_t*)&descriptor_)))
      {
        YYSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::decode_i64(buf, data_len, new_pos, &start_time_us_)))
      {
        YYSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if (OB_SUCCESS != (err = ups_.deserialize(buf, data_len, new_pos)))
      {
        YYSYS_LOG(ERROR, "ups.deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      // mod by maosy [Delete_Update_Function_isolation_RC] 20161218 b:
      else if(OB_SUCCESS != (err = serialization::decode_i64 (buf, data_len, new_pos,&read_times_)))
      {
        YYSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      //add by maosy
      else
      {
        pos = new_pos;
      }
      return err;
    }

    int64_t ObTransID::get_serialize_size(void) const
    {
      return serialization::encoded_length_i32(descriptor_)
          + serialization::encoded_length_i64(start_time_us_)
          + ups_.get_serialize_size()
          //add by maosy [Delete_Update_Function_for_snpshot] 20161210
          +serialization::encoded_length_i64(read_times_);
      // mod e
    }

    //add by maosy [MultiUPS 1.0] [read uncommit]20170525 b:
    int64_t ObPartitionTransID::get_serialize_size()const
    {
      return transid_.get_serialize_size()+
          serialization::encoded_length_i64(paxos_id_)
          +serialization::encoded_length_i64(published_transid_) ;
    }
    int ObPartitionTransID::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (OB_SUCCESS != (err = transid_.serialize(buf,buf_len,new_pos)))
      {
        YYSYS_LOG(ERROR, "failde to serialize transid (buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::encode_i64(buf, buf_len, new_pos, paxos_id_)))
      {
        YYSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::encode_i64(buf, buf_len, new_pos, published_transid_)))
      {
        YYSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else
      {
        pos = new_pos;
      }
      return err ;
    }

    int ObPartitionTransID::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (OB_SUCCESS != (err = transid_.deserialize(buf,data_len,new_pos)))
      {
        YYSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::decode_i64(buf, data_len, new_pos, &paxos_id_)))
      {
        YYSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::decode_i64(buf, data_len, new_pos, &published_transid_)))
      {
        YYSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else
      {
        pos = new_pos;
      }
      return err ;
    }

    int64_t ObPartitionTransID::to_string(char *buf, int64_t len) const
    {
      int64_t pos = 0 ;
      databuff_printf(buf,len,pos,"transid= %s,paxos_id = %ld,published_transid = %ld",
                      to_cstring(transid_),paxos_id_,published_transid_);
      return pos;
    }
    // add by maosy e


    int64_t ObEndTransReq::to_string(char* buf, int64_t len) const
    {
      int64_t pos = 0;
      databuff_printf(buf, len, pos, "EndTransReq(%s,rollback=%s)",
                      to_cstring(trans_id_), STR_BOOL(rollback_));
      return pos;
    }

    int ObEndTransReq::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (OB_SUCCESS != (err = trans_id_.serialize(buf, buf_len, new_pos)))
      {
        YYSYS_LOG(ERROR, "ups.deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::encode_bool(buf, buf_len, new_pos, rollback_)))
      {
        YYSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
      }
      //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
      else if (OB_SUCCESS != (err = serialization::encode_vi32(buf, buf_len, new_pos, participant_num_)))
      {
        YYSYS_LOG(ERROR, "serialize participant_num_ failed, err:%d", err);
      }
      else if (OB_SUCCESS != (err = serialization::encode_vi64(buf, buf_len, new_pos, memtable_version_)))
      {
        YYSYS_LOG(ERROR, "serialize memtable_version_ failed, err:%d", err);
      }
      //add 20150701:e
      else if(participant_num_ > 1)
      {
        for (int i = 0; OB_SUCCESS == err && i< participant_num_; i++)
        {
          if (OB_SUCCESS != (err = participant_trans_id_[i].serialize_for_prepare(buf, buf_len, new_pos)))
          {
            YYSYS_LOG(ERROR, "serialize_for_prepare participant_trans_id_[%d] failed, err:%d", i, err);
          }
        }
      }
      else
        //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
      {
        for (int i = 0; OB_SUCCESS == err && i< participant_num_; i++)
        {
          if (OB_SUCCESS != (err = participant_trans_id_[i].serialize(buf, buf_len, new_pos)))
          {
            YYSYS_LOG(ERROR, "serialize participant_trans_id_[%d] failed, err:%d", i, err);
          }
        }
      }
      if (OB_SUCCESS == err)
        //add 20150701:e
      {
        pos = new_pos;
      }
      return err;
    }

    int ObEndTransReq::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
    {
      int err = OB_SUCCESS;
      int64_t new_pos = pos;
      if (OB_SUCCESS != (err = trans_id_.deserialize(buf, data_len, new_pos)))
      {
        YYSYS_LOG(ERROR, "ups.deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::decode_bool(buf, data_len, new_pos, &rollback_)))
      {
        YYSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
      else if (OB_SUCCESS != (err = serialization::decode_vi32(buf, data_len, new_pos, &participant_num_)))
      {
        YYSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      else if (OB_SUCCESS != (err = serialization::decode_vi64(buf, data_len, new_pos, &memtable_version_)))
      {
        YYSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
      }
      //add 20150701:e
      else if(participant_num_ > 1)
      {
        for (int i = 0; OB_SUCCESS == err && i< participant_num_; i++)
        {
          if (OB_SUCCESS != (err = participant_trans_id_[i].deserialize_for_prepare(buf, data_len, new_pos)))
          {
            YYSYS_LOG(ERROR, "serialize participant_trans_id_[%d] failed, err:%d", i, err);
          }
        }
      }
      else
        //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
      {
        for (int i = 0; OB_SUCCESS == err && i< participant_num_; i++)
        {
          if (OB_SUCCESS != (err = participant_trans_id_[i].deserialize(buf, data_len, new_pos)))
          {
            YYSYS_LOG(ERROR, "trans_id.deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
          }
        }
      }
      if (OB_SUCCESS == err)
        //add 20150701:e
      {
        pos = new_pos;
      }
      return err;
    }

    int64_t ObEndTransReq::get_serialize_size(void) const
    {
      //mod peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
      //return trans_id_.get_serialize_size() + serialization::encoded_length_bool(rollback_);
      int64_t ret = trans_id_.get_serialize_size() + serialization::encoded_length_bool(rollback_)
                    + serialization::encoded_length_vi32(participant_num_) + serialization::encoded_length_vi64(memtable_version_);
      //[304]
      if(participant_num_ > 1)
      {
        for (int i = 0; i < participant_num_; i++)
        {
          ret = ret + participant_trans_id_[i].get_serialize_size_for_prepare();
        }
      }
      else
      {
        for (int i = 0; i < participant_num_; i++)
        {
          ret = ret + participant_trans_id_[i].get_serialize_size();
        }

      }
      return ret;
      //mod 20150701:e
    }
  }; // end namespace common
}; // end namespace oceanbase
