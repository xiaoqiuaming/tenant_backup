#include "ob_object.h"
#include "ob_action_flag.h"
#include "ob_common_param.h"
#include "ob_schema.h"
#include "ob_rowkey_helper.h"
//add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
#include "ob_ups_row.h"
#include "ob_row_fuse.h"
//add duyr 20160531:e

namespace oceanbase
{
  namespace common
  {
    bool ObCellInfo::operator == (const ObCellInfo & other) const
    {
      return ((table_name_ == other.table_name_) && (table_id_ == other.table_id_)
              && (row_key_ == other.row_key_) && (column_name_ == other.column_name_)
              && (column_id_ == other.column_id_) && (value_ == other.value_));
    }

    //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151130:b
    ///****************************ObTransVersion************************************///
    ObTransVersion::ObTransVersion():
      ObTransID(),
      trans_type_(INVALID_TRANS_TYPE)
    {
    }
    void ObTransVersion::reset()
    {
      ObTransID::reset();
      trans_type_ = INVALID_TRANS_TYPE;
    }
    bool ObTransVersion::is_valid()const
    {
      bool bret = false;
      bret = ObTransID::is_valid();
      if (bret)
      {
        bret = (INVALID_TRANS_TYPE != trans_type_);
        if (!bret)
        {
          YYSYS_LOG(WARN,"invalid trans type!trans_ver=[%s]",
                    to_cstring(*this));
        }
      }
      return bret;
    }
    bool ObTransVersion::is_single_point_trans()const
    {
      return (SINGLE_POINT_TRANS == trans_type_);
    }
    bool ObTransVersion::is_distributed_trans()const
    {
      return (DISTRIBUTED_TRANS == trans_type_);
    }
    int ObTransVersion::set_trans_dist_type(const TransDistType &type)
    {
      int ret = OB_SUCCESS;
      if (INVALID_TRANS_TYPE == type)
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(ERROR,"invalid trans type!ret=%d,type=%d",ret,type);
      }
      else
      {
        trans_type_ = type;
      }
      return ret;
    }
    int64_t ObTransVersion::to_string(char *buf, int64_t len) const
    {
      int64_t pos = 0;
      databuff_printf(buf, len, pos, "TransVersion(sd=%u,ups=%s,start=%ld,trans_type=%d)",
                      descriptor_, ups_.to_cstring(), start_time_us_,trans_type_);
      return pos;
    }
    ObTransVersion& ObTransVersion::operator=(const ObTransVersion &other)
    {
      if (this == &other)
        return *this;
      this->reset();
      start_time_us_ = other.start_time_us_;
      ups_           = other.ups_;
      descriptor_    = other.descriptor_;
      trans_type_    = other.trans_type_;
      return *this;
    }
    bool ObTransVersion::operator == (const ObTransVersion &r_value) const
    {
      return ((start_time_us_ == r_value.start_time_us_) &&
              (ups_ == r_value.ups_) &&
              (descriptor_ == r_value.descriptor_) &&
              (trans_type_ == r_value.trans_type_));
    }
    bool ObTransVersion::operator != (const ObTransVersion &r_value) const
    {
      return (!(*this == r_value));
    }
    void ObTransVersion::assign(const ObTransID &trans_id)
    {
      this->reset();
      start_time_us_ = trans_id.start_time_us_;
      ups_ = trans_id.ups_;
      descriptor_ = trans_id.descriptor_;
    }
    int64_t ObTransVersion::hash() const
    {
      int64_t ret = start_time_us_;
      common::hash::hash_func<int64_t> hash_int64;
      common::hash::hash_func<uint32_t> hash_uint32;
      common::hash::hash_func<int32_t> hash_int32;
      int64_t ip_hash_val = 0;
      char local_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
      ObString tmp_string;
      if (ups_.ip_to_string(local_ip, sizeof(local_ip)) == true)
      {
        tmp_string.assign(local_ip,static_cast<int32_t>(strlen(local_ip)));
        ip_hash_val = tmp_string.hash();
      }
      ret = hash_int64(start_time_us_)
            + ip_hash_val
            + hash_uint32(descriptor_)
            + hash_int32(trans_type_);
      return ret;
    }
    DEFINE_SERIALIZE(ObTransVersion)
    {
      int     ret     = OB_SUCCESS;
      int64_t tmp_pos = pos;
      if (NULL == buf || buf_len <= 0 || pos > buf_len)
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(ERROR, "invalid param, buf=%p, buf_len=%ld, pos=%ld,ret=%d",
                  buf, buf_len, pos, ret);
      }
      if (OB_SUCCESS == ret)
      {
        ret = ObTransID::serialize(buf,buf_len,tmp_pos);
        if (OB_SUCCESS != ret)
        {
          YYSYS_LOG(WARN,"fail to serialize trans id part!ret=%d,buf_len=%ld,tmp_pos=%ld",
                    ret,buf_len,tmp_pos);
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf,buf_len,tmp_pos,static_cast<int64_t>(trans_type_));
      }
      if (OB_SUCCESS == ret)
      {
        pos = tmp_pos;
      }
      else
      {
        YYSYS_LOG(WARN,"fail to serialize trans ver!ret=%d,ver=[%s]",
                  ret,to_cstring(*this));
      }
      return ret;
    }
    DEFINE_DESERIALIZE(ObTransVersion)
    {
      int     ret     = OB_SUCCESS;
      int64_t tmp_pos = pos;
      if (NULL == buf || data_len <= 0 || pos > data_len)
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(ERROR, "invalid param, buf=%p, data_len=%ld, pos=%ld,ret=%d",
                  buf, data_len, pos,ret);
      }
      if (OB_SUCCESS == ret)
      {
        this->reset();
      }
      if (OB_SUCCESS == ret)
      {
        ret = ObTransID::deserialize(buf,data_len,tmp_pos);
        if (OB_SUCCESS != ret)
        {
          YYSYS_LOG(WARN,"fail to deserialize trans id part!ret=%d,data_len=%ld,pos=%ld",
                    ret,data_len,tmp_pos);
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&trans_type_));
      }
      if (OB_SUCCESS == ret)
      {
        pos = tmp_pos;
      }
      else
      {
        YYSYS_LOG(WARN,"fail to deserialize trans ver!ret=%d,ver=[%s]",
                  ret,to_cstring(*this));
      }
      return ret;
    }
    DEFINE_GET_SERIALIZE_SIZE(ObTransVersion)
    {
      int64_t total_size = 0;
      total_size += ObTransID::get_serialize_size();
      total_size += serialization::encoded_length_i64(static_cast<int64_t>(trans_type_));
      return total_size;
    }
    ///****************************ObReadAtomicDataMark************************************///
    ObReadAtomicDataMark::ObReadAtomicDataMark():
      minor_version_end_(OB_INVALID_VERSION),
      minor_version_start_(OB_INVALID_VERSION),
      major_version_(OB_INVALID_VERSION),
      ups_paxos_id_(OB_INVALID_PAXOS_ID),
      data_store_type_(INVALID_STORE_TYPE)
    {
    }

    ObReadAtomicDataMark::ObReadAtomicDataMark(const ObReadAtomicDataMark& other)
    {
      reset();
      *this = other;
    }
    ObReadAtomicDataMark& ObReadAtomicDataMark::operator=(const ObReadAtomicDataMark& other)
    {
      if (this == &other)
        return *this;
      reset();
      minor_version_end_   = other.minor_version_end_;
      minor_version_start_ = other.minor_version_start_;
      major_version_       = other.major_version_;
      ups_paxos_id_        = other.ups_paxos_id_;
      data_store_type_     = other.data_store_type_;
      return *this;
    }

    int ObReadAtomicDataMark::compare(const ObReadAtomicDataMark &other) const
    {
      int ret = 0;
      if (INVALID_STORE_TYPE == data_store_type_
          && INVALID_STORE_TYPE != other.data_store_type_)
      {
        ret = -1;
      }
      else if (INVALID_STORE_TYPE != data_store_type_
               && INVALID_STORE_TYPE == other.data_store_type_)
      {
        ret = 1;
      }
      else if (major_version_ < other.major_version_)
      {
        ret = -1;
      }
      else if (major_version_ > other.major_version_)
      {
        ret = 1;
      }
      else if (CS_SSTABLE_DATA == data_store_type_
               || CS_SSTABLE_DATA == other.data_store_type_)
      {
        if (CS_SSTABLE_DATA != data_store_type_)
        {
          ret = -1;
        }
        else if (CS_SSTABLE_DATA != other.data_store_type_)
        {
          ret = 1;
        }
        else
        {
          ret = 0;
        }
      }
      else if (minor_version_start_ < other.minor_version_start_)
      {
        ret = -1;
      }
      else if (minor_version_start_ > other.minor_version_start_)
      {
        ret = 1;
      }
      else if (minor_version_end_ < other.minor_version_end_)
      {
        ret = -1;
      }
      else if (minor_version_end_ > other.minor_version_end_)
      {
        ret = 1;
      }
      else if (UPS_MEMTABLE_DATA == data_store_type_
               && UPS_SSTABLE_DATA == other.data_store_type_)
      {
        ret = -1;
      }
      else if (UPS_SSTABLE_DATA == data_store_type_
               && UPS_MEMTABLE_DATA == other.data_store_type_)
      {
        ret = 1;
      }
      else
      {
        ret = 0;
      }
      return ret;
    }
    bool ObReadAtomicDataMark::operator ==(const ObReadAtomicDataMark &other) const
    {
      int ret = this->compare(other);
      return ret == 0;
    }
    bool ObReadAtomicDataMark::operator !=(const ObReadAtomicDataMark &other) const
    {
      int ret = this->compare(other);
      return ret != 0;
    }
    bool ObReadAtomicDataMark::operator<(const ObReadAtomicDataMark &other) const
    {
      int ret = this->compare(other);
      return ret < 0;
    }
    bool ObReadAtomicDataMark::operator<=(const ObReadAtomicDataMark &other) const
    {
      int ret = this->compare(other);
      return ret <= 0;
    }
    bool ObReadAtomicDataMark::operator>(const ObReadAtomicDataMark &other) const
    {
      int ret = this->compare(other);
      return ret > 0;
    }
    bool ObReadAtomicDataMark::operator>=(const ObReadAtomicDataMark &other) const
    {
      int ret = this->compare(other);
      return ret >= 0;
    }

    void ObReadAtomicDataMark::reset()
    {
      minor_version_end_   = OB_INVALID_VERSION;
      minor_version_start_ = OB_INVALID_VERSION;
      major_version_       = OB_INVALID_VERSION;
      ups_paxos_id_        = OB_INVALID_PAXOS_ID;
      data_store_type_     = INVALID_STORE_TYPE;
    }

    bool ObReadAtomicDataMark::is_valid() const
    {
      bool bret = false;
      if (CS_SSTABLE_DATA == data_store_type_
          && OB_INVALID_VERSION != major_version_
          && 0 < major_version_)
      {
        bret = true;
      }
      else if ((UPS_SSTABLE_DATA == data_store_type_
                || UPS_MEMTABLE_DATA == data_store_type_)
               && OB_INVALID_VERSION != major_version_
               && 0 < major_version_
               && OB_INVALID_VERSION != minor_version_end_
               && 0 < minor_version_end_
               && OB_INVALID_VERSION != minor_version_start_
               && 0 < minor_version_start_
               && OB_INVALID_PAXOS_ID != ups_paxos_id_
               && 0 <= ups_paxos_id_)
      {
        bret = true;
      }
      else if (INVALID_STORE_TYPE != data_store_type_
               || OB_INVALID_PAXOS_ID != ups_paxos_id_
               || OB_INVALID_VERSION != minor_version_end_
               || OB_INVALID_VERSION != minor_version_start_
               || OB_INVALID_VERSION != major_version_)
      {//means cur mark is invalid!
        YYSYS_LOG(WARN,"cur data mark is valid!data_store_type=%d,"
                  "ups_paxos_id=%ld,minor_version_end=%ld,"
                  "minor_version_start=%ld,major_version=%ld",
                  data_store_type_,ups_paxos_id_,minor_version_end_,
                  minor_version_start_,major_version_);
      }
      return bret;
    }


    bool ObReadAtomicDataMark::is_paxos_id_useful()const
    {
      bool bret = false;
      if (UPS_MEMTABLE_DATA == data_store_type_
          || UPS_SSTABLE_DATA == data_store_type_)
      {
        bret = true;
      }
      return bret;
    }

    const char* ObReadAtomicDataMark::to_cstring() const
    {
      static const int64_t BUFFER_NUM = 3;
      static __thread char buff[BUFFER_NUM][MAX_MARK_BUF_SIZE];
      static __thread int64_t i = 0;
      i++;
      memset(buff[i % BUFFER_NUM], 0, MAX_MARK_BUF_SIZE);
      to_string(buff[i % BUFFER_NUM], MAX_MARK_BUF_SIZE);
      return buff[ i % BUFFER_NUM];
    }

    int64_t ObReadAtomicDataMark::to_string(char* buf, int64_t buf_len) const
    {
      int64_t pos = 0;
      databuff_printf(buf, buf_len, pos, "ObReadAtomicDataMark(");
      databuff_printf(buf, buf_len, pos, "minor_version_end=%ld,",
                      minor_version_end_);
      databuff_printf(buf, buf_len, pos, "minor_version_start=%ld,",
                      minor_version_start_);
      databuff_printf(buf, buf_len, pos, "major_version=%ld,",
                      major_version_);
      databuff_printf(buf, buf_len, pos, "ups_paxos_id_=%ld,",
                      ups_paxos_id_);
      databuff_printf(buf, buf_len, pos, "data_store_type_=%d,",
                      data_store_type_);
      databuff_printf(buf, buf_len, pos, ")");
      return pos;
    }
    DEFINE_SERIALIZE(ObReadAtomicDataMark)
    {
      int     ret     = OB_SUCCESS;
      int64_t tmp_pos = pos;
      if (NULL == buf || buf_len <= 0 || pos > buf_len)
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(ERROR, "invalid param, buf=%p, buf_len=%ld, pos=%ld,ret=%d",
                  buf, buf_len, pos, ret);
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf,buf_len,tmp_pos,minor_version_start_);
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf,buf_len,tmp_pos,minor_version_end_);
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf,buf_len,tmp_pos,major_version_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf,buf_len,tmp_pos,ups_paxos_id_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf,buf_len,tmp_pos,static_cast<int64_t>(data_store_type_));
      }
      if (OB_SUCCESS == ret)
      {
        pos = tmp_pos;
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObReadAtomicDataMark)
    {
      int     ret     = OB_SUCCESS;
      int64_t tmp_pos = pos;
      if (NULL == buf || data_len <= 0 || pos > data_len)
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(ERROR, "invalid param, buf=%p, data_len=%ld, pos=%ld,ret=%d",
                  buf, data_len, pos,ret);
      }

      //better reset
      if (OB_SUCCESS == ret)
      {
        reset();
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, tmp_pos, &minor_version_start_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, tmp_pos, &minor_version_end_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, tmp_pos, &major_version_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, tmp_pos, &ups_paxos_id_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&data_store_type_));
      }
      if (OB_SUCCESS == ret)
      {
        pos = tmp_pos;
      }

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObReadAtomicDataMark)
    {
      int64_t total_size = 0;
      total_size += serialization::encoded_length_i64(minor_version_start_);
      total_size += serialization::encoded_length_i64(minor_version_end_);
      total_size += serialization::encoded_length_i64(major_version_);
      total_size += serialization::encoded_length_i64(ups_paxos_id_);
      total_size += serialization::encoded_length_i64(static_cast<int64_t>(data_store_type_));
      return total_size;
    }

    ///****************************ObReadAtomicParam*****************************///
    ObReadAtomicParam::ObReadAtomicParam(const ObReadAtomicParam &other)
    {
      reset();
      *this = other;
    }

    ObReadAtomicParam& ObReadAtomicParam::operator=(const ObReadAtomicParam &other)
    {
      int tmp_ret = OB_SUCCESS;
      if (this == &other)
        return *this;

      reset();
      need_last_commit_trans_ver_   = other.need_last_commit_trans_ver_;
      need_last_prepare_trans_ver_  = other.need_last_prepare_trans_ver_;
      need_commit_data_             = other.need_commit_data_;
      need_prepare_data_            = other.need_prepare_data_;
      need_trans_meta_data_         = other.need_trans_meta_data_;
      need_data_mark_               = other.need_data_mark_;
      need_exact_transver_data_     = other.need_exact_transver_data_;
      need_exact_data_mark_data_    = other.need_exact_data_mark_data_;
      need_row_key_                 = other.need_row_key_;
      need_prevcommit_trans_verset_ = other.need_prevcommit_trans_verset_;
      last_commit_trans_ver_cid_    = other.last_commit_trans_ver_cid_;
      last_prepare_trans_ver_cid_   = other.last_prepare_trans_ver_cid_;
      read_atomic_meta_cid_         = other.read_atomic_meta_cid_;
      commit_data_extendtype_cid_   = other.commit_data_extendtype_cid_;
      prepare_data_extendtype_cid_  = other.prepare_data_extendtype_cid_;
      major_ver_cid_                = other.major_ver_cid_;
      minor_ver_start_cid_          = other.minor_ver_start_cid_;
      minor_ver_end_cid_            = other.minor_ver_end_cid_;
      ups_paxos_id_cid_             = other.ups_paxos_id_cid_;
      data_store_type_cid_          = other.data_store_type_cid_;
      prevcommit_trans_verset_cid_off_ = other.prevcommit_trans_verset_cid_off_;
      prepare_data_cid_off_         = other.prepare_data_cid_off_;
      table_id_                     = other.table_id_;
      min_used_column_cid_          = other.min_used_column_cid_;
      rowkey_cell_count_            = other.rowkey_cell_count_;
      max_prevcommit_trans_verset_count_ = other.max_prevcommit_trans_verset_count_;
      table_base_column_num_        = other.table_base_column_num_;
      table_composite_column_num_   = other.table_composite_column_num_;
      is_data_mark_array_sorted_    = other.is_data_mark_array_sorted_;
      for (int64_t i=0;OB_SUCCESS == tmp_ret && i < other.exact_data_mark_array_.count();i++)
      {
        const ObReadAtomicDataMark &data_mark = other.exact_data_mark_array_.at(i);
        if (OB_SUCCESS != (tmp_ret = exact_data_mark_array_.push_back(data_mark)))
        {
          YYSYS_LOG(WARN,"fail to push back data mark[%s],ret=%d",
                    to_cstring(data_mark),tmp_ret);
        }
      }
      if (OB_SUCCESS == tmp_ret
          &&other.is_trans_verset_map_create_
          && other.exact_transver_num_ > 0)
      {
        ExactTransversetMap::const_iterator iter = other.exact_trans_verset_map_.begin();
        ExactTransversetMap::const_iterator end  = other.exact_trans_verset_map_.end();
        for (;OB_SUCCESS == tmp_ret && iter != end;iter++)
        {
          if (OB_SUCCESS != (tmp_ret = add_exact_transver(iter->first)))
          {
            YYSYS_LOG(WARN,"fail to copy the versinon[%s]!ret=%d",to_cstring(iter->first),tmp_ret);
          }
        }
      }
      return *this;
    }

    void ObReadAtomicParam::reset()
    {
      need_last_commit_trans_ver_  = false;
      need_last_prepare_trans_ver_ = false;
      need_trans_meta_data_   = false;
      need_commit_data_       = false;
      need_prepare_data_      = false;
      need_data_mark_         = false;
      need_exact_transver_data_ = false;
      need_exact_data_mark_data_= false;
      need_prevcommit_trans_verset_ = false;
      need_row_key_           = false;
      last_commit_trans_ver_cid_   = 0;
      last_prepare_trans_ver_cid_  = 0;
      read_atomic_meta_cid_   = 0;
      commit_data_extendtype_cid_  = 0;
      prepare_data_extendtype_cid_ = 0;
      major_ver_cid_          = 0;
      minor_ver_start_cid_    = 0;
      minor_ver_end_cid_      = 0;
      ups_paxos_id_cid_       = 0;
      data_store_type_cid_    = 0;
      prevcommit_trans_verset_cid_off_ = 0;
      prepare_data_cid_off_   = 0;
      table_id_               = OB_INVALID_ID;
      min_used_column_cid_    = 0;
      rowkey_cell_count_      = 0;
      max_prevcommit_trans_verset_count_= 0;
      table_base_column_num_      = 0;
      table_composite_column_num_ = 0;
      exact_transver_num_ = 0;
      reset_exact_transver_map();
      reset_exact_datamark_array();
    }

    void ObReadAtomicParam::reset_exact_transver_map()
    {
      is_trans_verset_map_create_ = false;
      exact_trans_verset_map_.destroy();
    }

    void ObReadAtomicParam::reset_exact_datamark_array()
    {
      is_data_mark_array_sorted_ = false;
      exact_data_mark_array_.clear();
    }

    int ObReadAtomicParam::add_exact_transver(const ObTransVersion &ver)
    {
      int ret = OB_SUCCESS;
      ObTransVersion::TransVerState old_state = ObTransVersion::INVALID_STATE;
      int hash_get_ret = OB_SUCCESS;
      int hash_ret = OB_SUCCESS;

      if (!is_trans_verset_map_create_)
      {
        exact_trans_verset_map_.destroy();
        if (OB_SUCCESS != (ret = exact_trans_verset_map_.create(HASH_BUCKET_NUM)))
        {
          YYSYS_LOG(WARN,"fail to create hashmap!ret=%d",ret);
        }
        else
        {
          is_trans_verset_map_create_ = true;
        }
      }

      if (OB_SUCCESS != ret)
      {
      }
      else if (hash::HASH_NOT_EXIST == (hash_get_ret = exact_trans_verset_map_.get(ver,old_state)))
      {
        if (hash::HASH_INSERT_SUCC != (hash_ret = exact_trans_verset_map_.set(ver,ObTransVersion::COMMIT_STATE)))
        {
          ret = OB_ERR_UNEXPECTED;
          YYSYS_LOG(WARN,"fail to insert transver[%s] into hashmap!hash_ret=%d,ret=%d",
                    to_cstring(ver),hash_ret,ret);
        }
        else
        {
          exact_transver_num_++;
        }
      }
      else if (hash::HASH_EXIST != hash_get_ret)
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(WARN,"fail to get transver[%s] from hashmap!hash_ret=%d,old_state=%d,ret=%d",
                  to_cstring(ver),hash_get_ret,old_state,ret);
      }
      return ret;
    }
    int ObReadAtomicParam::add_exact_data_mark(const ObReadAtomicDataMark &data_mark)
    {
      int ret = OB_SUCCESS;
      if (!data_mark.is_valid())
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(ERROR,"invalid data mark[%s],ret=%d",
                  to_cstring(data_mark),ret);
      }
      else if (OB_SUCCESS != (ret = exact_data_mark_array_.push_back(data_mark)))
      {
        YYSYS_LOG(WARN,"fail to store data mark[%s],ret=%d",
                  to_cstring(data_mark),ret);
      }
      else if (is_data_mark_array_sorted_)
      {
        //        if (exact_data_mark_array_.count() > 1)
        //        {
        //          YYSYS_LOG(WARN,"cur data mark has been sorted,better don't add new data mark anymore!!");
        //        }
        if(OB_SUCCESS != (ret = sort_exact_data_mark_array()))
        {
          YYSYS_LOG(WARN,"fail to sort data mark array!ret=%d",ret);
        }
      }

      return ret;
    }

    int ObReadAtomicParam::sort_exact_data_mark_array()
    {
      int ret = OB_SUCCESS;
      if (exact_data_mark_array_.count() < 0)
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(ERROR,"invalid data mark count[%ld],ret=%d",
                  exact_data_mark_array_.count(),ret);
      }
      else if (exact_data_mark_array_.count() == 0)
      {
        //there is no data mark
        //        is_data_mark_array_sorted_  = true;
      }
      else
      {
        ObReadAtomicDataMark *first = &(exact_data_mark_array_.at(0));
        ObReadAtomicDataMark *end   = first + exact_data_mark_array_.count();
        std::sort(first,end);
        is_data_mark_array_sorted_  = true;
      }



      return ret;
    }

    bool ObReadAtomicParam::is_inited()const
    {
      bool bret = false;
      if (need_last_commit_trans_ver_
          || need_last_prepare_trans_ver_
          || need_prevcommit_trans_verset_
          || need_commit_data_
          || need_prepare_data_
          || need_trans_meta_data_
          || need_data_mark_
          || need_exact_transver_data_
          || need_exact_data_mark_data_
          || need_row_key_)
      {
        bret = true;
      }
      return bret;
    }


    bool ObReadAtomicParam::is_need_extend_data()const
    {
      bool bret = false;
      if (need_last_commit_trans_ver_
          || need_last_prepare_trans_ver_
          || need_prevcommit_trans_verset_
          || need_trans_meta_data_
          || need_data_mark_
          || need_row_key_)
      {
        bret = true;
      }
      return bret;
    }

    bool ObReadAtomicParam::is_need_real_data()const
    {
      bool bret = false;
      if (need_commit_data_
          || need_exact_transver_data_
          || need_exact_data_mark_data_
          || need_prepare_data_)
      {
        bret = true;
      }
      return bret;
    }

    bool ObReadAtomicParam::is_valid()const
    {
      bool bret = false;
      if (!is_inited())
      {
      }
      else if (OB_INVALID_ID == table_id_)
      {
        YYSYS_LOG(WARN,"invalid tid!");
      }
      else if (need_commit_data_ && need_exact_transver_data_)
      {
        YYSYS_LOG(WARN,"need_commit_data must exclusion with need_exact_transver_data_!");
      }
      //      else if (need_commit_data_ && need_exact_data_mark_data_)
      //      {
      //        YYSYS_LOG(WARN,"need_commit_data must exclusion with need_exact_data_mark_data_!");
      //      }
      else if (need_last_commit_trans_ver_
               && (last_commit_trans_ver_cid_ <= 0
                   || OB_INVALID_ID == last_commit_trans_ver_cid_))
      {
        YYSYS_LOG(WARN,"invalid last_commit_trans_ver_cid_=%ld",last_commit_trans_ver_cid_);
      }
      else if (need_last_prepare_trans_ver_
               && (last_prepare_trans_ver_cid_ <= 0
                   || OB_INVALID_ID == last_prepare_trans_ver_cid_ ))
      {
        YYSYS_LOG(WARN,"invalid last_prepare_trans_ver_cid_=%ld",last_prepare_trans_ver_cid_);
      }
      else if (need_trans_meta_data_
               && (read_atomic_meta_cid_ <= 0
                   || OB_INVALID_ID == read_atomic_meta_cid_))
      {
        YYSYS_LOG(WARN,"invalid read_atomic_meta_cid_=%ld",read_atomic_meta_cid_);
      }
      else if (need_prevcommit_trans_verset_
               && (max_prevcommit_trans_verset_count_ <= 0
                   || max_prevcommit_trans_verset_count_ >= static_cast<uint64_t>(ObReadAtomicHelper::MAX_TRANS_VERSET_SIZE)
                   || prevcommit_trans_verset_cid_off_ <= 0
                   || OB_INVALID_ID == prevcommit_trans_verset_cid_off_))
      {
        YYSYS_LOG(WARN,"invalid argument!"
                  "max_prevcommit_trans_verset_count_=%ld,"
                  "max_trans_verset_size = %ld,"
                  "prevcommit_trans_verset_cid_off_ = %ld",
                  max_prevcommit_trans_verset_count_,
                  ObReadAtomicHelper::MAX_TRANS_VERSET_SIZE,
                  prevcommit_trans_verset_cid_off_);
      }
      else if (need_prevcommit_trans_verset_
               && (prevcommit_trans_verset_cid_off_ < last_commit_trans_ver_cid_
                   || prevcommit_trans_verset_cid_off_ < last_prepare_trans_ver_cid_
                   || prevcommit_trans_verset_cid_off_ < read_atomic_meta_cid_
                   || prevcommit_trans_verset_cid_off_ < commit_data_extendtype_cid_
                   || prevcommit_trans_verset_cid_off_ < prepare_data_extendtype_cid_
                   || prevcommit_trans_verset_cid_off_ < major_ver_cid_
                   || prevcommit_trans_verset_cid_off_ < minor_ver_start_cid_
                   || prevcommit_trans_verset_cid_off_ < minor_ver_end_cid_
                   || prevcommit_trans_verset_cid_off_ < ups_paxos_id_cid_
                   || prevcommit_trans_verset_cid_off_ < data_store_type_cid_))
      {
        YYSYS_LOG(WARN,"invalid prevcommit_trans_verset_cid_off_!"
                  "prevcommit_trans_verset_cid_off must bigger than other extend data cid!"
                  "param=[%s]",to_cstring(*this));
      }
      else if (need_row_key_
               && rowkey_cell_count_ <= 0)
      {
        YYSYS_LOG(WARN,"invalid argument!"
                  "need_row_key=%d,rowkey_cell_count_=%ld",
                  need_row_key_,
                  rowkey_cell_count_);
      }
      else if ((need_commit_data_
                || need_exact_transver_data_
                || need_exact_data_mark_data_)
               && (commit_data_extendtype_cid_ <= 0
                   || OB_INVALID_ID == commit_data_extendtype_cid_))
      {
        YYSYS_LOG(WARN,"invalid commit data extend type cid!"
                  "cid=%ld,need_commit_data_=%d,"
                  "need_exact_transver_data_=%d,"
                  "need_exact_data_mark_data_=%d",
                  commit_data_extendtype_cid_,
                  need_commit_data_,need_exact_transver_data_,
                  need_exact_data_mark_data_);
      }
      else if (need_data_mark_
               && (ups_paxos_id_cid_ <= 0
                   || OB_INVALID_ID == ups_paxos_id_cid_
                   || major_ver_cid_ <= 0
                   || OB_INVALID_ID == major_ver_cid_
                   || minor_ver_start_cid_ <= 0
                   || OB_INVALID_ID == minor_ver_start_cid_
                   || minor_ver_end_cid_ <= 0
                   || OB_INVALID_ID == minor_ver_end_cid_
                   || data_store_type_cid_ <= 0
                   || OB_INVALID_ID == data_store_type_cid_))
      {
        YYSYS_LOG(WARN,"invalid data mark cid!ups_paxos_id_cid_=%ld,"
                  "major_ver_cid_=%ld,minor_ver_start_cid_=%ld"
                  "minor_ver_end_cid_=%ld,data_store_type_cid_=%ld",
                  ups_paxos_id_cid_,major_ver_cid_,
                  minor_ver_start_cid_,minor_ver_end_cid_,
                  data_store_type_cid_);
      }
      else if (need_prepare_data_
               && (prepare_data_extendtype_cid_ <= 0
                   || OB_INVALID_ID == prepare_data_extendtype_cid_
                   || prepare_data_cid_off_ <= 0
                   || OB_INVALID_ID == prepare_data_cid_off_
                   || min_used_column_cid_ <= 0
                   || OB_INVALID_ID == min_used_column_cid_
                   || table_base_column_num_ <= 0))
      {
        YYSYS_LOG(WARN,"invalid prepare_cid! prepare_data_extendtype_cid_=%ld, prepare_data_cid_off_=%ld,"
                  "min_used_column_cid_=%ld,table_base_column_num_=%ld",
                  prepare_data_extendtype_cid_,prepare_data_cid_off_,
                  min_used_column_cid_,table_base_column_num_);
      }
      else if (need_prepare_data_
               && (prepare_data_cid_off_ < prevcommit_trans_verset_cid_off_
                   || prepare_data_cid_off_ < (prevcommit_trans_verset_cid_off_
                                               + max_prevcommit_trans_verset_count_)
                   || prepare_data_cid_off_ < min_used_column_cid_
                   || prepare_data_cid_off_ < min_used_column_cid_ + table_base_column_num_
                   || prepare_data_cid_off_ < last_commit_trans_ver_cid_
                   || prepare_data_cid_off_ < last_prepare_trans_ver_cid_
                   || prepare_data_cid_off_ < read_atomic_meta_cid_
                   || prepare_data_cid_off_ < commit_data_extendtype_cid_
                   || prepare_data_cid_off_ < prepare_data_extendtype_cid_
                   || prepare_data_cid_off_ < major_ver_cid_
                   || prepare_data_cid_off_ < minor_ver_start_cid_
                   || prepare_data_cid_off_ < minor_ver_end_cid_
                   || prepare_data_cid_off_ < ups_paxos_id_cid_
                   || prepare_data_cid_off_ < data_store_type_cid_))
      {
        YYSYS_LOG(WARN,"invalid prepare_data_cid_off_!"
                  "prepare_data_cid_off_ must bigger than other data cid!"
                  "param=[%s]",to_cstring(*this));
      }
      else
      {
        bret = true;
      }
      return bret;
    }



    bool ObReadAtomicParam::commit_ext_type_must_valid()const
    {
      bool bret = false;
      if (need_commit_data_
          || need_exact_transver_data_
          || need_exact_data_mark_data_)
      {
        bret = true;
      }
      return bret;
    }

    bool ObReadAtomicParam::prepare_ext_type_must_valid()const
    {
      bool bret = false;
      if (need_prepare_data_)
      {
        bret = true;
      }
      return bret;
    }

    int64_t ObReadAtomicParam::to_string(char* buf, int64_t buf_len) const
    {
      int64_t pos = 0;
      databuff_printf(buf, buf_len, pos, "ReadAtomicParam(");
      databuff_printf(buf, buf_len, pos, "need_last_commit_trans_ver_=%d,",
                      need_last_commit_trans_ver_);
      databuff_printf(buf, buf_len, pos, "need_last_prepare_trans_ver_=%d,",
                      need_last_prepare_trans_ver_);
      databuff_printf(buf, buf_len, pos, "need_prevcommit_trans_verset_=%d,",
                      need_prevcommit_trans_verset_);
      databuff_printf(buf, buf_len, pos, "need_commit_data_=%d,",
                      need_commit_data_);
      databuff_printf(buf, buf_len, pos, "need_prepare_data_=%d,",
                      need_prepare_data_);
      databuff_printf(buf, buf_len, pos, "need_trans_meta_data_=%d,",
                      need_trans_meta_data_);
      databuff_printf(buf, buf_len, pos, "need_data_mark_=%d,",
                      need_data_mark_);
      databuff_printf(buf, buf_len, pos, "need_exact_transver_data_=%d,",
                      need_exact_transver_data_);
      databuff_printf(buf, buf_len, pos, "need_exact_data_mark_data_=%d,",
                      need_exact_data_mark_data_);
      databuff_printf(buf, buf_len, pos, "need_row_key_=%d,",
                      need_row_key_);
      databuff_printf(buf, buf_len, pos, "last_commit_trans_ver_cid_=%ld,",
                      last_commit_trans_ver_cid_);
      databuff_printf(buf, buf_len, pos, "last_prepare_trans_ver_cid_=%ld,",
                      last_prepare_trans_ver_cid_);
      databuff_printf(buf, buf_len, pos, "read_atomic_meta_cid_=%ld,",
                      read_atomic_meta_cid_);
      databuff_printf(buf, buf_len, pos, "commit_data_extendtype_cid_=%ld,",
                      commit_data_extendtype_cid_);
      databuff_printf(buf, buf_len, pos, "prepare_data_extendtype_cid_=%ld,",
                      prepare_data_extendtype_cid_);
      databuff_printf(buf, buf_len, pos, "major_ver_cid_=%ld,",
                      major_ver_cid_);
      databuff_printf(buf, buf_len, pos, "minor_ver_start_cid_=%ld,",
                      minor_ver_start_cid_);
      databuff_printf(buf, buf_len, pos, "minor_ver_end_cid_=%ld,",
                      minor_ver_end_cid_);
      databuff_printf(buf, buf_len, pos, "ups_paxos_id_cid_=%ld,",
                      ups_paxos_id_cid_);
      databuff_printf(buf, buf_len, pos, "data_store_type_cid_=%ld,",
                      data_store_type_cid_);
      databuff_printf(buf, buf_len, pos, "prevcommit_trans_verset_cid_off_=%ld,",
                      prevcommit_trans_verset_cid_off_);
      databuff_printf(buf, buf_len, pos, "prepare_data_cid_off_=%ld,",
                      prepare_data_cid_off_);
      databuff_printf(buf, buf_len, pos, "table_id=%ld,",
                      table_id_);
      databuff_printf(buf, buf_len, pos, "min_used_column_cid_=%ld,",
                      min_used_column_cid_);
      databuff_printf(buf, buf_len, pos, "rowkey_cell_count_=%ld,",
                      rowkey_cell_count_);
      databuff_printf(buf, buf_len, pos, "max_prevcommit_trans_verset_count_=%ld,",
                      max_prevcommit_trans_verset_count_);
      databuff_printf(buf, buf_len, pos, "table_base_column_num=%ld,",
                      table_base_column_num_);
      databuff_printf(buf, buf_len, pos, "table_composite_column_num_=%ld,",
                      table_composite_column_num_);
      databuff_printf(buf, buf_len, pos, "is_data_mark_array_sorted_=%d,",
                      is_data_mark_array_sorted_);
      databuff_printf(buf, buf_len, pos, "\nexact_data_mark_[");
      for (int64_t i=0;i<exact_data_mark_array_.count();i++)
      {
        databuff_printf(buf, buf_len, pos, "<%ldth_data_mark,%s>",
                        i,exact_data_mark_array_.at(i).to_cstring());
      }
      databuff_printf(buf, buf_len, pos, "]");
      databuff_printf(buf, buf_len, pos, "\nexact_trans_verset_[");
      if (is_trans_verset_map_create_ && exact_transver_num_ > 0)
      {
        ExactTransversetMap::const_iterator iter = exact_trans_verset_map_.begin();
        ExactTransversetMap::const_iterator end  = exact_trans_verset_map_.end();
        for (int64_t i=0;iter != end;iter++,i++)
        {
          databuff_printf(buf, buf_len, pos, "<%ldth_ver,%s>",
                          i,to_cstring(iter->first));
        }
      }
      databuff_printf(buf, buf_len, pos, "]");
      databuff_printf(buf, buf_len, pos, ")");
      return pos;
    }

    DEFINE_SERIALIZE(ObReadAtomicParam)
    {
      int     ret     = OB_SUCCESS;
      int64_t tmp_pos = pos;
      if (NULL == buf || buf_len <= 0 || pos > buf_len)
      {
        YYSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld",
                  buf, buf_len, pos);
        ret = OB_INVALID_ARGUMENT;
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_bool(buf, buf_len, tmp_pos, need_last_commit_trans_ver_);
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_bool(buf, buf_len, tmp_pos, need_last_prepare_trans_ver_);
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_bool(buf, buf_len, tmp_pos, need_prevcommit_trans_verset_);
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_bool(buf, buf_len, tmp_pos, need_commit_data_);
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_bool(buf, buf_len, tmp_pos, need_prepare_data_);
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_bool(buf, buf_len, tmp_pos, need_trans_meta_data_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_bool(buf, buf_len, tmp_pos, need_data_mark_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_bool(buf, buf_len, tmp_pos, need_exact_transver_data_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_bool(buf, buf_len, tmp_pos, need_exact_data_mark_data_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_bool(buf, buf_len, tmp_pos, need_row_key_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf,buf_len,tmp_pos,last_commit_trans_ver_cid_);
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf,buf_len,tmp_pos,last_prepare_trans_ver_cid_);
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf,buf_len,tmp_pos,read_atomic_meta_cid_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf,buf_len,tmp_pos,commit_data_extendtype_cid_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf,buf_len,tmp_pos,prepare_data_extendtype_cid_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf,buf_len,tmp_pos,major_ver_cid_);
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf,buf_len,tmp_pos,minor_ver_start_cid_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf,buf_len,tmp_pos,minor_ver_end_cid_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf,buf_len,tmp_pos,ups_paxos_id_cid_);
      }
      
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf,buf_len,tmp_pos,data_store_type_cid_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf,buf_len,tmp_pos,prevcommit_trans_verset_cid_off_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf,buf_len,tmp_pos,prepare_data_cid_off_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf,buf_len,tmp_pos,table_id_);
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf,buf_len,tmp_pos,min_used_column_cid_);
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf,buf_len,tmp_pos,rowkey_cell_count_);
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf,buf_len,tmp_pos,max_prevcommit_trans_verset_count_);
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf,buf_len,tmp_pos,table_base_column_num_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf,buf_len,tmp_pos,table_composite_column_num_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_bool(buf, buf_len, tmp_pos, is_data_mark_array_sorted_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf,buf_len,tmp_pos,exact_data_mark_array_.count());
      }
      if (OB_SUCCESS == ret)
      {
        for (int64_t i=0;OB_SUCCESS == ret && i < exact_data_mark_array_.count();i++)
        {
          if (OB_SUCCESS != (ret = exact_data_mark_array_.at(i).serialize(buf,buf_len,tmp_pos)))
          {
            YYSYS_LOG(WARN,"fail to serialize data mark[%s],ret=%d",
                      to_cstring(exact_data_mark_array_.at(i)),ret);
          }
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf,buf_len,tmp_pos,exact_transver_num_);
      }
      if (OB_SUCCESS == ret && exact_transver_num_ > 0)
      {
        if (!is_trans_verset_map_create_)
        {
          ret = OB_NOT_INIT;
          YYSYS_LOG(ERROR,"invalid trans version set map!ret=%d",ret);
        }
        else
        {
          int64_t num_count = 0;
          ExactTransversetMap::const_iterator iter = exact_trans_verset_map_.begin();
          ExactTransversetMap::const_iterator end  = exact_trans_verset_map_.end();
          for (;OB_SUCCESS == ret && iter != end;iter++,num_count++)
          {
            ret = iter->first.serialize(buf,buf_len,tmp_pos);
          }

          if (OB_SUCCESS == ret && num_count != exact_transver_num_)
          {
            ret = OB_ERR_UNEXPECTED;
            YYSYS_LOG(ERROR,"real serialize transver num[%ld] don't queal exact_transver_num_[%ld],ret=%d",
                      num_count,exact_transver_num_,ret);
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        pos = tmp_pos;
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObReadAtomicParam)
    {
      int     ret     = OB_SUCCESS;
      int64_t tmp_pos = pos;
      int64_t transver_num  = 0;
      int64_t data_mark_num = 0;
      if (NULL == buf || data_len <= 0 || pos > data_len)
      {
        YYSYS_LOG(WARN, "invalid param, buf=%p, data_len=%ld, pos=%ld",
                  buf, data_len, pos);
        ret = OB_INVALID_ARGUMENT;
      }

      //better reset
      if (OB_SUCCESS == ret)
      {
        reset();
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_bool(buf, data_len, tmp_pos, &need_last_commit_trans_ver_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_bool(buf, data_len, tmp_pos, &need_last_prepare_trans_ver_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_bool(buf, data_len, tmp_pos, &need_prevcommit_trans_verset_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_bool(buf, data_len, tmp_pos, &need_commit_data_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_bool(buf, data_len, tmp_pos, &need_prepare_data_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_bool(buf, data_len, tmp_pos, &need_trans_meta_data_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_bool(buf, data_len, tmp_pos, &need_data_mark_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_bool(buf, data_len, tmp_pos, &need_exact_transver_data_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_bool(buf, data_len, tmp_pos, &need_exact_data_mark_data_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_bool(buf, data_len, tmp_pos, &need_row_key_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&last_commit_trans_ver_cid_));
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&last_prepare_trans_ver_cid_));
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&read_atomic_meta_cid_));
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&commit_data_extendtype_cid_));
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&prepare_data_extendtype_cid_));
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&major_ver_cid_));
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&minor_ver_start_cid_));
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&minor_ver_end_cid_));
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&ups_paxos_id_cid_));
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&data_store_type_cid_));
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&prevcommit_trans_verset_cid_off_));
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&prepare_data_cid_off_));
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&table_id_));
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&min_used_column_cid_));
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&rowkey_cell_count_));
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&max_prevcommit_trans_verset_count_));
      }

      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&table_base_column_num_));
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&table_composite_column_num_));
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_bool(buf, data_len, tmp_pos, &is_data_mark_array_sorted_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, tmp_pos, &data_mark_num);
      }
      if (OB_SUCCESS == ret)
      {
        for (int64_t i=0;OB_SUCCESS == ret && i < data_mark_num;i++)
        {
          ObReadAtomicDataMark tmp_data_mark;
          if (OB_SUCCESS != (ret = tmp_data_mark.deserialize(buf,data_len,tmp_pos)))
          {
            YYSYS_LOG(WARN,"fail to deserialize data mark[%s],buf=%p,data_len=%ld,tmp_pos=%ld,ret=%d",
                      to_cstring(tmp_data_mark),buf,data_len,tmp_pos,ret);
          }
          else if (OB_SUCCESS != (ret = exact_data_mark_array_.push_back(tmp_data_mark)))
          {
            YYSYS_LOG(WARN,"fail to store deserialize data mark[%s],ret=%d",
                      to_cstring(tmp_data_mark),ret);
          }
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, tmp_pos, &transver_num);
      }
      if (OB_SUCCESS == ret)
      {
        for(int64_t i=0;OB_SUCCESS == ret && i < transver_num;i++)
        {
          ObTransVersion tmp_ver;
          if (OB_SUCCESS != (ret = tmp_ver.deserialize(buf,data_len,tmp_pos)))
          {
            YYSYS_LOG(WARN,"fail to deserialize the %ld the version[%s]!len=%ld,pos=%ld,ret=%d",
                      i,to_cstring(tmp_ver),data_len,tmp_pos,ret);
          }
          else if (OB_SUCCESS != (ret = add_exact_transver(tmp_ver)))
          {
            YYSYS_LOG(WARN,"fail to store the %ld the version[%s]!ret=%d",
                      i,to_cstring(tmp_ver),ret);
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        pos = tmp_pos;
      }

      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObReadAtomicParam)
    {
      int64_t total_size = 0;

      total_size += serialization::encoded_length_bool(need_last_commit_trans_ver_);
      total_size += serialization::encoded_length_bool(need_last_prepare_trans_ver_);
      total_size += serialization::encoded_length_bool(need_prevcommit_trans_verset_);
      total_size += serialization::encoded_length_bool(need_commit_data_);
      total_size += serialization::encoded_length_bool(need_prepare_data_);
      total_size += serialization::encoded_length_bool(need_trans_meta_data_);
      total_size += serialization::encoded_length_bool(need_data_mark_);
      total_size += serialization::encoded_length_bool(need_exact_transver_data_);
      total_size += serialization::encoded_length_bool(need_exact_data_mark_data_);
      total_size += serialization::encoded_length_bool(need_row_key_);
      total_size += serialization::encoded_length_i64(last_commit_trans_ver_cid_);
      total_size += serialization::encoded_length_i64(last_prepare_trans_ver_cid_);
      total_size += serialization::encoded_length_i64(read_atomic_meta_cid_);
      total_size += serialization::encoded_length_i64(commit_data_extendtype_cid_);
      total_size += serialization::encoded_length_i64(prepare_data_extendtype_cid_);
      total_size += serialization::encoded_length_i64(major_ver_cid_);
      total_size += serialization::encoded_length_i64(minor_ver_start_cid_);
      total_size += serialization::encoded_length_i64(minor_ver_end_cid_);
      total_size += serialization::encoded_length_i64(ups_paxos_id_cid_);
      total_size += serialization::encoded_length_i64(data_store_type_cid_);
      total_size += serialization::encoded_length_i64(prevcommit_trans_verset_cid_off_);
      total_size += serialization::encoded_length_i64(prepare_data_cid_off_);
      total_size += serialization::encoded_length_i64(table_id_);
      total_size += serialization::encoded_length_i64(min_used_column_cid_);
      total_size += serialization::encoded_length_i64(rowkey_cell_count_);
      total_size += serialization::encoded_length_i64(max_prevcommit_trans_verset_count_);
      total_size += serialization::encoded_length_i64(table_base_column_num_);
      total_size += serialization::encoded_length_i64(table_composite_column_num_);
      total_size += serialization::encoded_length_bool(is_data_mark_array_sorted_);
      total_size += serialization::encoded_length_i64(exact_data_mark_array_.count());
      for (int64_t i=0;i<exact_data_mark_array_.count();i++)
      {
        total_size += exact_data_mark_array_.at(i).get_serialize_size();
      }
      total_size += serialization::encoded_length_i64(exact_transver_num_);
      if (is_trans_verset_map_create_ && exact_transver_num_ > 0)
      {
        ExactTransversetMap::const_iterator iter = exact_trans_verset_map_.begin();
        ExactTransversetMap::const_iterator end  = exact_trans_verset_map_.end();
        for (;iter!=end;iter++)
        {
          total_size += iter->first.get_serialize_size();
        }
      }
      return total_size;
    }
    ///////////////////////////ObReadAtomicHelper/////////////////////////////
    int ObReadAtomicHelper::transver_copy_to_buf(const ObTransVersion &src_transver,
                                                 char *des_buf,
                                                 const int64_t des_buf_len)
    {
      int ret = OB_SUCCESS;
      ObTransVersion *des_transver = NULL;
      if (NULL == des_buf || des_buf_len < TRANS_VERSION_BUF_SIZE)
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(ERROR,"invalid des_buf!des_buf=%p,buf_len=%ld,min_transver_buf_size=%ld,ret=%d",
                  des_buf,des_buf_len,TRANS_VERSION_BUF_SIZE,ret);
      }
      else if (OB_SUCCESS != (ret = transver_cast_from_buf(des_buf,des_buf_len,des_transver)))
      {
        YYSYS_LOG(WARN,"fail to cast buf to transver!ret=%d",ret);
      }
      else
      {//deep copy
        des_transver->reset();
        (*des_transver) = src_transver;
      }
      return ret;
    }
    int ObReadAtomicHelper::transver_cast_to_buf(const ObTransVersion *src_transver,
                                                 char *&des_buf,
                                                 int64_t &des_buf_len)
    {
      int ret     = OB_SUCCESS;
      des_buf     = NULL;
      des_buf_len = 0;
      if (NULL == src_transver)
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(ERROR,"src transver ptr is NULL!ret=%d",ret);
      }
      else
      {
        des_buf = (char *)(const_cast<ObTransVersion *>(src_transver));
        des_buf_len = TRANS_VERSION_BUF_SIZE;
        if (NULL == des_buf)
        {
          ret = OB_ERR_UNEXPECTED;
          YYSYS_LOG(ERROR,"fail to cast transver to buf!ret=%d",ret);
        }
      }
      return ret;
    }
    int ObReadAtomicHelper::transver_copy_from_buf(const char *src_buf,
                                                   const int64_t src_buf_len,
                                                   ObTransVersion &des_transver)
    {
      int ret = OB_SUCCESS;
      ObTransVersion *src_transver = NULL;
      if (NULL == src_buf
          || src_buf_len < TRANS_VERSION_BUF_SIZE)
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(WARN,"invalid source src_buf!"
                  "src_buf=%p,buf_len=%ld,TRANS_VERSION_BUF_SIZE=%ld,ret=%d",
                  src_buf,src_buf_len,TRANS_VERSION_BUF_SIZE,ret);
      }
      else if (OB_SUCCESS != (ret = transver_cast_from_buf(src_buf,src_buf_len,src_transver)))
      {
        YYSYS_LOG(WARN,"fail to cast buf to transver!ret=%d",ret);
      }
      else
      {
        des_transver.reset();
        des_transver = *src_transver;
      }
      return ret;
    }
    int ObReadAtomicHelper::transver_cast_from_buf(const char *src_buf,
                                                   const int64_t src_buf_len,
                                                   ObTransVersion *&des_transver)
    {
      int ret      = OB_SUCCESS;
      des_transver = NULL;
      if (NULL == src_buf || src_buf_len < TRANS_VERSION_BUF_SIZE)
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(ERROR,"invalid source src_buf!"
                  "src_buf=%p,buf_len=%ld,TRANS_VERSION_BUF_SIZE=%ld,ret=%d",
                  src_buf,src_buf_len,TRANS_VERSION_BUF_SIZE,ret);
      }
      else if (NULL == (des_transver = const_cast<ObTransVersion *>((const ObTransVersion *)(src_buf))))
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(ERROR,"fail to cast const buf to transver!ret=%d",ret);
      }
      return ret;
    }
    int ObReadAtomicHelper::init_buf_with_transver(char *des_buf, const int64_t des_buf_len)
    {
      int ret = OB_SUCCESS;
      ObTransVersion *des_transver = NULL;
      if (OB_SUCCESS != (ret = transver_cast_from_buf(des_buf,des_buf_len,des_transver)))
      {
        YYSYS_LOG(WARN,"fail to cast buf to transver!ret=%d",ret);
      }
      else
      {
        des_transver->reset();
      }
      return ret;
    }
    bool ObReadAtomicHelper::is_trans_ver_obj(const common::ObObj &val,
                                              const bool check_valid)
    {
      bool bret = false;
      ObTransVersion *ver_ptr = NULL;
      const char *buf_ptr     = (char *)(val.get_data_ptr());
      const int64_t buf_len   = val.get_data_length();
      if (ObVarcharType == val.get_type()
          && OB_SUCCESS == (transver_cast_from_buf(buf_ptr,buf_len,ver_ptr))
          && NULL != ver_ptr)
      {
        bret = true;
        if (check_valid && !ver_ptr->is_valid())
        {
          bret = false;
        }
      }
      return bret;
    }
    int ObReadAtomicHelper::get_trans_ver(const common::ObObj &val,
                                          const ObTransVersion **des_ver,
                                          const bool must_valid)
    {
      int ret = OB_SUCCESS;
      ObTransVersion *ver_ptr = NULL;
      const char *buf_ptr     = (char *)(val.get_data_ptr());
      const int64_t buf_len   = val.get_data_length();
      if (NULL != des_ver)
        *des_ver = NULL;
      if (!is_trans_ver_obj(val))
      {
        if (ObNullType == val.get_type()
            || (ObExtendType == val.get_type()
                && ObActionFlag::OP_NOP == val.get_ext()))
        {//means has no trans version!
          if (must_valid)
          {
            ret = OB_ERROR;
            YYSYS_LOG(WARN,"the trans version is empty!must_valid=%d,ret=%d",
                      must_valid,ret);
          }
        }
        else
        {
          ret = OB_NOT_SUPPORTED;
          YYSYS_LOG(ERROR,"unknow trans version obj!val=[%s],ret=%d",
                    to_cstring(val),ret);
        }
      }
      else if (OB_SUCCESS != (ret = transver_cast_from_buf(buf_ptr,buf_len,ver_ptr)))
      {
        YYSYS_LOG(WARN,"fail to get transversion!val=[%s],ret=%d",to_cstring(val),ret);
      }
      else if (must_valid && (NULL == ver_ptr || !ver_ptr->is_valid()))
      {
        ret = OB_ERROR;
        YYSYS_LOG(WARN,"cur trans version is invalid!ver_ptr=%p,ret=%d",ver_ptr,ret);
      }
      else if (NULL != des_ver)
      {
        *des_ver = ver_ptr;
      }
      return ret;
    }
    int ObReadAtomicHelper::get_data_mark(const common::ObObj &major_ver_val,
                                          const common::ObObj &minor_ver_start_val,
                                          const common::ObObj &minor_ver_end_val,
                                          const common::ObObj &paxos_id_val,
                                          const common::ObObj &data_store_type_val,
                                          ObReadAtomicDataMark &des_mark)
    {
      int ret = OB_SUCCESS;
      int64_t data_store_type = ObReadAtomicDataMark::INVALID_STORE_TYPE;
      des_mark.reset();
      if (ObIntType == paxos_id_val.get_type()
          && ObIntType == major_ver_val.get_type()
          && ObIntType == minor_ver_start_val.get_type()
          && ObIntType == minor_ver_end_val.get_type()
          && ObIntType == data_store_type_val.get_type())
      {
        if (OB_SUCCESS != (ret = paxos_id_val.get_int(des_mark.ups_paxos_id_)))
        {
          YYSYS_LOG(WARN,"fail to get paxos id from obj!paxos_id=%ld,ret=%d",
                    des_mark.ups_paxos_id_,ret);
        }
        else if (OB_SUCCESS != (ret = major_ver_val.get_int(des_mark.major_version_)))
        {
          YYSYS_LOG(WARN,"fail to get major ver from obj!major_ver=%ld,ret=%d",
                    des_mark.major_version_,ret);
        }
        else if (OB_SUCCESS != (ret = minor_ver_start_val.get_int(des_mark.minor_version_start_)))
        {
          YYSYS_LOG(WARN,"fail to get minor ver start from obj!minor_ver_start=%ld,ret=%d",
                    des_mark.minor_version_start_,ret);
        }
        else if (OB_SUCCESS != (ret = minor_ver_end_val.get_int(des_mark.minor_version_end_)))
        {
          YYSYS_LOG(WARN,"fail to get minor ver end from obj!minor_ver_end=%ld,ret=%d",
                    des_mark.minor_version_end_,ret);
        }
        else if (OB_SUCCESS != (ret = data_store_type_val.get_int(data_store_type)))
        {
          YYSYS_LOG(WARN,"fail to get data store type from obj!type=%ld,ret=%d",
                    data_store_type,ret);
        }
        else
        {
          des_mark.data_store_type_ = static_cast<ObReadAtomicDataMark::DataStoreType>(data_store_type);
        }
      }
      else if (ObIntType != paxos_id_val.get_type()
               && ObNullType != paxos_id_val.get_type()
               && (ObExtendType != paxos_id_val.get_type()
                   || ObActionFlag::OP_NOP != paxos_id_val.get_ext()))
      {
        ret = OB_NOT_SUPPORTED;
        YYSYS_LOG(ERROR,"unknow paxos_id obj!val=[%s],ret=%d",
                  to_cstring(paxos_id_val),ret);
      }
      else if (ObIntType != major_ver_val.get_type()
               && ObNullType != major_ver_val.get_type()
               && (ObExtendType != major_ver_val.get_type()
                   || ObActionFlag::OP_NOP != major_ver_val.get_ext()))
      {
        ret = OB_NOT_SUPPORTED;
        YYSYS_LOG(ERROR,"unknow major ver obj!val=[%s],ret=%d",
                  to_cstring(major_ver_val),ret);
      }
      else if (ObIntType != minor_ver_start_val.get_type()
               && ObNullType != minor_ver_start_val.get_type()
               && (ObExtendType != minor_ver_start_val.get_type()
                   || ObActionFlag::OP_NOP != minor_ver_start_val.get_ext()))
      {
        ret = OB_NOT_SUPPORTED;
        YYSYS_LOG(ERROR,"unknow minor ver start obj!val=[%s],ret=%d",
                  to_cstring(minor_ver_start_val),ret);
      }
      else if (ObIntType != minor_ver_end_val.get_type()
               && ObNullType != minor_ver_end_val.get_type()
               && (ObExtendType != minor_ver_end_val.get_type()
                   || ObActionFlag::OP_NOP != minor_ver_end_val.get_ext()))
      {
        ret = OB_NOT_SUPPORTED;
        YYSYS_LOG(ERROR,"unknow minor ver end obj!val=[%s],ret=%d",
                  to_cstring(minor_ver_end_val),ret);
      }
      else if (ObIntType != data_store_type_val.get_type()
               && ObNullType != data_store_type_val.get_type()
               && (ObExtendType != data_store_type_val.get_type()
                   || ObActionFlag::OP_NOP != data_store_type_val.get_ext()))
      {
        ret = OB_NOT_SUPPORTED;
        YYSYS_LOG(ERROR,"unknow data store type obj!val=[%s],ret=%d",
                  to_cstring(data_store_type_val),ret);
      }
      return ret;
    }
    int ObReadAtomicHelper::gen_prevcommit_trans_verset_cids(const ObReadAtomicParam& param,
                                                             uint64_t *cid_buf,
                                                             const int64_t buf_size)
    {//FIXME:READ_ATOMIC should use buf_size output the real vercount!
      int ret = OB_SUCCESS;
      if (NULL == cid_buf
          || buf_size <= 0
          || !param.need_prevcommit_trans_verset_
          || !param.is_valid())
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(ERROR,"invalid argument!cid_buf=%p,buf_size=%ld,param=[%s],ret=%d",
                  cid_buf,buf_size,to_cstring(param),ret);
      }
      else
      {
        int64_t off_cid = param.prevcommit_trans_verset_cid_off_;
        int64_t max_verset_count = param.max_prevcommit_trans_verset_count_;
        for (int64_t i=0;i<buf_size && i<max_verset_count;i++)
        {
          cid_buf[i] = off_cid + i;
        }
      }
      return ret;
    }
    int ObReadAtomicHelper::convert_commit_cid_to_prepare_cid(const ObReadAtomicParam &param,
                                                              const uint64_t commit_cid,
                                                              uint64_t &prepare_cid)
    {
      int ret = OB_SUCCESS;
      if (OB_INVALID_ID == commit_cid
          || !param.need_prepare_data_
          || OB_ACTION_FLAG_COLUMN_ID == commit_cid
          || param.min_used_column_cid_ > commit_cid
          || OB_MAX_TMP_COLUMN_ID < commit_cid
          || is_read_atomic_special_cid(param,commit_cid)
          || !param.is_valid())
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(WARN,"invalid commit cid!commit_cid=%ld,param=[%s],ret=%d",
                  commit_cid,to_cstring(param),ret);
      }
      else if (commit_cid >= (param.min_used_column_cid_
                              + param.table_base_column_num_))
      {//it's maybe composite column!
        prepare_cid = param.prepare_data_cid_off_
                      + param.table_base_column_num_
                      + OB_MAX_TMP_COLUMN_ID - commit_cid;
      }
      else
      {//it's base column!
        prepare_cid = param.prepare_data_cid_off_
                      + commit_cid - param.min_used_column_cid_;
      }
      if (OB_SUCCESS == ret && !is_prepare_cid(param,prepare_cid))
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(WARN,"fail to convert commit cid[%ld] to prepare cid[%ld],param=[%s],ret=%d",
                  commit_cid,prepare_cid,to_cstring(param),ret);
      }
      if (OB_SUCCESS != ret)
      {
        prepare_cid = OB_INVALID_ID;
      }
      return ret;
    }
    int ObReadAtomicHelper::convert_prepare_cid_to_commit_cid(const ObReadAtomicParam &param,
                                                              const uint64_t prepare_cid,
                                                              uint64_t &commit_cid)
    {
      int ret = OB_SUCCESS;
      if (OB_INVALID_ID == prepare_cid
          || !param.need_prepare_data_
          || OB_ACTION_FLAG_COLUMN_ID == prepare_cid
          || !is_prepare_cid(param,prepare_cid)
          || !param.is_valid())
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(WARN,"invalid prepare data cid!prepare_cid=%ld,param=[%s],ret=%d",
                  prepare_cid,to_cstring(param),ret);
      }
      else if (prepare_cid >= (param.prepare_data_cid_off_
                               + param.table_base_column_num_))
      {//it's prepare_cid of composite column
        commit_cid   = param.prepare_data_cid_off_
                       + param.table_base_column_num_
                       + OB_MAX_TMP_COLUMN_ID - prepare_cid;
      }
      else
      {//it's prepare_cid of commit_cid
        commit_cid = prepare_cid + param.min_used_column_cid_
                     - param.prepare_data_cid_off_;
      }
      if (OB_SUCCESS == ret  && is_read_atomic_special_cid(param,commit_cid))
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(WARN,"fail to convert prepare cid[%ld] to commit cid[%ld],param=[%s],ret=%d",
                  prepare_cid,commit_cid,to_cstring(param),ret);
      }
      if (OB_SUCCESS != ret)
      {
        commit_cid = OB_INVALID_ID;
      }
      return ret;
    }
    bool ObReadAtomicHelper::is_read_atomic_special_cid(const ObReadAtomicParam &param,
                                                        const uint64_t column_id)
    {
      bool bret = false;
      if (OB_ACTION_FLAG_COLUMN_ID == column_id
          || OB_INVALID_ID == column_id
          || !param.is_valid())
      {
        bret = false;
      }
      else if ((param.need_last_commit_trans_ver_ && param.last_commit_trans_ver_cid_ == column_id)
               || (param.need_last_prepare_trans_ver_ && param.last_prepare_trans_ver_cid_ == column_id)
               || (param.need_trans_meta_data_ && param.read_atomic_meta_cid_ == column_id)
               || ((param.need_commit_data_
                    || param.need_exact_data_mark_data_
                    || param.need_exact_transver_data_) && param.commit_data_extendtype_cid_ == column_id)
               || (param.need_prepare_data_ && param.prepare_data_extendtype_cid_ == column_id)
               || (param.need_data_mark_ && param.major_ver_cid_ == column_id)
               || (param.need_data_mark_ && param.minor_ver_start_cid_ == column_id)
               || (param.need_data_mark_ && param.minor_ver_end_cid_ == column_id)
               || (param.need_data_mark_ && param.data_store_type_cid_ == column_id)
               || (param.need_data_mark_ && param.ups_paxos_id_cid_ == column_id)
               || (param.need_prevcommit_trans_verset_
                   && param.prevcommit_trans_verset_cid_off_ <= column_id
                   && column_id < (param.prevcommit_trans_verset_cid_off_
                                   + param.max_prevcommit_trans_verset_count_))
               || (param.need_prepare_data_
                   && param.prepare_data_cid_off_ <= column_id
                   && column_id < (param.prepare_data_cid_off_
                                   + param.table_base_column_num_
                                   + param.table_composite_column_num_)))
      {
        bret = true;
      }
      return bret;
    }
    bool ObReadAtomicHelper::is_prepare_cid(const ObReadAtomicParam &param, const uint64_t column_id)
    {
      bool bret = false;
      uint64_t prepare_cid_off = param.prepare_data_cid_off_;
      if (OB_ACTION_FLAG_COLUMN_ID == column_id
          || OB_INVALID_ID == column_id
          || !param.need_prepare_data_
          || !param.is_valid())
      {
        bret = false;
      }
      else if (prepare_cid_off <= column_id
               && column_id < (prepare_cid_off
                               + param.table_base_column_num_
                               + param.table_composite_column_num_))
      {
        bret = true;
      }
      return bret;
    }
    bool ObReadAtomicHelper::is_trans_ver_cid(const ObReadAtomicParam &param,
                                              const uint64_t column_id)
    {
      bool bret = false;
      if (OB_ACTION_FLAG_COLUMN_ID == column_id
          || OB_INVALID_ID == column_id
          || !param.is_valid())
      {
        bret = false;
      }
      else if ((param.need_last_commit_trans_ver_ && param.last_commit_trans_ver_cid_ == column_id)
               || (param.need_last_prepare_trans_ver_  && param.last_prepare_trans_ver_cid_ == column_id)
               || (param.need_prevcommit_trans_verset_
                   &&param.prevcommit_trans_verset_cid_off_ <= column_id
                   &&(param.prevcommit_trans_verset_cid_off_ + param.max_prevcommit_trans_verset_count_) > column_id ))
      {
        bret = true;
      }
      return bret;
    }
    bool ObReadAtomicHelper::is_extend_type_obj(const common::ObObj &val)
    {
      bool bret = false;
      int64_t tmp_ext_type = 0;
      if (ObIntType == val.get_type()
          && OB_SUCCESS == val.get_int(tmp_ext_type)
          && tmp_ext_type > 0)
      {
        bret = true;
      }
      return bret;
    }
    int ObReadAtomicHelper::get_extend_type(const common::ObObj &val,
                                            int64_t &ext_type)
    {
      int ret = OB_SUCCESS;
      if (!is_extend_type_obj(val))
      {
        if (ObNullType == val.get_type()
            || (ObExtendType == val.get_type()
                && ObActionFlag::OP_NOP == val.get_ext()))
        {//means ups has no commit or prepare data!!!
          ext_type = ObActionFlag::OP_NOP;
        }
        else
        {
          ret = OB_NOT_SUPPORTED;
          YYSYS_LOG(WARN,"unknow extend type obj!val=[%s],ret=%d",
                    to_cstring(val),ret);
        }
      }
      else if (OB_SUCCESS != (ret = val.get_int(ext_type)))
      {
        YYSYS_LOG(WARN,"fail to get extend type!val=[%s],ext_type=%ld,ret=%d",
                  to_cstring(val),ext_type,ret);
      }
      else if (ObActionFlag::OP_NOP != ext_type
               && ObActionFlag::OP_VALID != ext_type
               && ObActionFlag::OP_DEL_ROW != ext_type
               && ObActionFlag::OP_ROW_DOES_NOT_EXIST != ext_type)
      {
        ret = OB_NOT_SUPPORTED;
        YYSYS_LOG(WARN,"unknow extend type!val=[%s],ext_type=%ld,ret=%d",
                  to_cstring(val),ext_type,ret);
      }
      return ret;
    }
    bool ObReadAtomicHelper::is_on_exact_trans_verset(const ObReadAtomicParam &param,
                                                      const ObTransVersion *ver)
    {
      bool bret = false;
      int hash_ret = OB_SUCCESS;
      ObTransVersion::TransVerState val = ObTransVersion::INVALID_STATE;
      if (!param.need_exact_transver_data_
          || !param.is_trans_verset_map_create_
          || NULL == ver)
      {
        YYSYS_LOG(WARN,"invalid read atomic param!ver=%p,param=[%s]",
                  ver,to_cstring(param));
      }
      else
      {
        hash_ret = param.exact_trans_verset_map_.get(*ver,val);
        if (hash::HASH_EXIST == hash_ret)
        {
          bret = true;
        }
        else if (hash::HASH_NOT_EXIST != hash_ret)
        {
          YYSYS_LOG(WARN,"fail to search hashmap!hash_ret=%d,hash_key=[%s],hash_val=%d,param=[%s]",
                    hash_ret,to_cstring(*ver),val,to_cstring(param));
        }
      }
      return bret;
    }
    int ObReadAtomicHelper::get_strategy_with_data_mark(const ObReadAtomicParam &param,
                                                        const ObReadAtomicDataMark &data_mark,
                                                        ObReadAtomicParam::ReadAtomicReadStrategy &strategy)
    {
      int ret = OB_SUCCESS;
      int64_t count = param.exact_data_mark_array_.count();
      strategy = ObReadAtomicParam::READ_WITH_READ_ATOMIC_PARAM;
      if (!data_mark.is_valid()
          || !param.need_exact_data_mark_data_
          || !param.is_valid())
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(ERROR,"invalid data mark[%s],param=[%s],ret=%d",
                  to_cstring(data_mark),to_cstring(param),ret);
      }
      else
      {
        for (int64_t i=0;OB_SUCCESS == ret && i < count;i++)
        {
          const ObReadAtomicDataMark &exact_data_mark = param.exact_data_mark_array_.at(i);
          if (data_mark.is_paxos_id_useful()
              && exact_data_mark.is_paxos_id_useful()
              && exact_data_mark.ups_paxos_id_ != data_mark.ups_paxos_id_)
          {
            continue;
          }
          else
          {
            if (data_mark < exact_data_mark)
            {
              strategy = ObReadAtomicParam::READ_WITH_COMMIT_DATA;
              YYSYS_LOG(DEBUG,"read_atomic::debug,data_mark[%s] lt exact_mark[%s],strategy=%d",
                        to_cstring(data_mark),to_cstring(exact_data_mark),strategy);
              if (param.is_data_mark_array_sorted_)
              {//the remainder exact data mark is asc order
                break;
              }
            }
            else if (data_mark > exact_data_mark)
            {
              strategy = ObReadAtomicParam::READ_WITH_READ_ATOMIC_PARAM;
              YYSYS_LOG(DEBUG,"read_atomic::debug,data_mark[%s] gt exact_mark[%s],strategy=%d",
                        to_cstring(data_mark),to_cstring(exact_data_mark),strategy);
            }
            else
            {
              strategy = ObReadAtomicParam::READ_WITH_READ_ATOMIC_PARAM;
              YYSYS_LOG(DEBUG,"read_atomic::debug,data_mark[%s] eq exact_mark[%s],strategy=%d",
                        to_cstring(data_mark),to_cstring(exact_data_mark),strategy);
              break;
            }
          }
        }
      }
      if (OB_SUCCESS == ret && ObReadAtomicParam::INVALID_STRATEGY == strategy)
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(ERROR,"invalid strategy!ret=%d",ret);
      }
      YYSYS_LOG(DEBUG,"read_atomic::debug,finish get strategy!"
                "ret=%d,data_mark=[%s],strategy=%d",
                ret,to_cstring(data_mark),strategy);
      return ret;
    }

    int ObReadAtomicHelper::fuse_row(const ObUpsRow *commit_prepare_ups_row,
                                     const ObRow *sstable_row,
                                     const ObReadAtomicParam &param,
                                     ObRow *result)
    {
      int ret = OB_SUCCESS;

      //READ_ATOMIC read_atomic::debug
      YYSYS_LOG(DEBUG,"read_atomic::debug,begin split!param=[%s]",to_cstring(param));
      if (NULL != commit_prepare_ups_row)
        YYSYS_LOG(DEBUG,"read_atomic::debug,orig_commit_prepare_row=[%s]",
                  to_cstring(*commit_prepare_ups_row));
      if (NULL != sstable_row)
        YYSYS_LOG(DEBUG,"read_atomic::debug,orig_sstable_row=[%s]",
                  to_cstring(*sstable_row));

      const ObObj *commit_cell  = NULL;
      uint64_t commit_cid = OB_INVALID_ID;
      uint64_t tid = OB_INVALID_ID;
      uint64_t cid = OB_INVALID_ID;

      const ObObj *sst_action_flag_cell = NULL;

      bool need_reset_commit_ext_type = false;
      bool need_expand_sst_row  = false;
      //FIXME:READ_ATOMIC expand_sst_row is using shallow copy?
      ObRow expand_sst_row;
      int64_t expand_sst_column_num  = 0;
      const ObRowDesc *expand_sst_row_desc = NULL;
      int64_t commit_ext_type = OB_INVALID_DATA;
      int64_t prepare_ext_type = OB_INVALID_DATA;
      uint64_t table_id = param.table_id_;

      if (NULL == commit_prepare_ups_row
          || NULL == sstable_row
          || NULL == sstable_row->get_row_desc()
          || NULL == result
          || !param.is_valid()
          || OB_INVALID_ID == table_id)
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(ERROR,"invalid argument!commit_prepare_ups_row=%p,sstable_row=%p,result=%p,param=[%s],tid=%ld,ret=%d",
                  commit_prepare_ups_row,sstable_row,result,to_cstring(param),table_id,ret);
      }
      else if (OB_SUCCESS != (ret = get_extend_vals_(commit_prepare_ups_row,
                                                     param,NULL,NULL,NULL,NULL,
                                                     &commit_ext_type,
                                                     &prepare_ext_type)))
      {
        YYSYS_LOG(WARN,"fail to get extend type!"
                  "commit_ext_type=%ld,prepare_ext_type=%ld,table_id=%ld,ret=%d",
                  commit_ext_type,prepare_ext_type,table_id,ret);
      }
      else
      {//init
        expand_sst_row_desc = sstable_row->get_row_desc();
        need_expand_sst_row = param.need_prepare_data_;
        need_reset_commit_ext_type = (ObActionFlag::OP_ROW_DOES_NOT_EXIST == commit_ext_type);

        if (need_expand_sst_row)
        {
          if(ObActionFlag::OP_NOP == prepare_ext_type
             || ObActionFlag::OP_ROW_DOES_NOT_EXIST == prepare_ext_type
             || ObActionFlag::OP_DEL_ROW == prepare_ext_type)
          {//no prepare ups data,no need to fuse the prepare data with sstable data
            need_expand_sst_row = false;
          }
        }

        if (OB_SUCCESS == ret && (need_expand_sst_row || need_reset_commit_ext_type))
        {
          bool is_sstable_row_empty = false;
          is_sstable_row_empty = commit_prepare_ups_row->get_is_delete_row();
          if (!is_sstable_row_empty
              && OB_INVALID_INDEX != expand_sst_row_desc->get_idx(OB_INVALID_ID,
                                                                  OB_ACTION_FLAG_COLUMN_ID))
          {
            if (OB_SUCCESS != (ret = sstable_row->get_cell(OB_INVALID_ID,
                                                           OB_ACTION_FLAG_COLUMN_ID,
                                                           sst_action_flag_cell)))
            {
              YYSYS_LOG(WARN, "fail to get sstable action flag column:ret[%d]", ret);
            }
            else
            {
              is_sstable_row_empty = (ObActionFlag::OP_ROW_DOES_NOT_EXIST == sst_action_flag_cell->get_ext());
            }
          }
          need_expand_sst_row = need_expand_sst_row ? (!is_sstable_row_empty):need_expand_sst_row;
          need_reset_commit_ext_type = need_reset_commit_ext_type ? (!is_sstable_row_empty):need_reset_commit_ext_type;
          YYSYS_LOG(DEBUG,"read_atomic::debug,is_sstable_row_empty=%d",is_sstable_row_empty);
        }

        if (OB_SUCCESS == ret && need_expand_sst_row)
        {
          expand_sst_column_num = expand_sst_row_desc->get_column_num();
          expand_sst_row.set_row_desc(*expand_sst_row_desc);
          expand_sst_row.assign(*sstable_row);
        }
      }

      //expand sstable row!
      if (OB_SUCCESS == ret && need_expand_sst_row)
      {
        for (int64_t i=0;OB_SUCCESS == ret && i <expand_sst_column_num;i++)
        {
          commit_cell  = NULL;
          commit_cid   = OB_INVALID_ID;
          tid = OB_INVALID_ID;
          cid = OB_INVALID_ID;
          if (OB_SUCCESS != (ret = expand_sst_row_desc->get_tid_cid(i,tid,cid)))
          {
            YYSYS_LOG(WARN,"fail to get tid[%ld] cid[%ld]!idx=%ld,ret=%d",
                      tid,cid,i,ret);
          }
          else if (is_prepare_cid(param,cid))
          {
            if (OB_SUCCESS != (ret = convert_prepare_cid_to_commit_cid(param,cid,commit_cid)))
            {
              YYSYS_LOG(WARN,"fail to convert prepare_cid[%ld] to commit_cid[%ld]!ret=%d",
                        cid,commit_cid,ret);
            }
            else if (OB_INVALID_INDEX == expand_sst_row_desc->get_idx(tid,commit_cid))
            {
              //no commit sst cid,do nothing!
            }
            else if (OB_SUCCESS != (ret = expand_sst_row.get_cell(tid,commit_cid,commit_cell)))
            {
              YYSYS_LOG(WARN,"fail to get commit sst cell!tid=%ld,prepare_cid=%ld,commit_cid=%ld,cell=%p,ret=%d",
                        tid,cid,commit_cid,commit_cell,ret);
            }
            else if (OB_SUCCESS != (ret = expand_sst_row.set_cell(tid,cid,*commit_cell)))
            {
              YYSYS_LOG(WARN,"fail to set prepare sst cell!tid=%ld,prepare_cid=%ld,commit_cid=%ld,commit_cell=[%s],ret=%d",
                        tid,cid,commit_cid,to_cstring(*commit_cell),ret);
            }
          }
        }
      }

      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN,"will not fuse any row!ret=%d",ret);
      }
      else if (!need_expand_sst_row
               && OB_SUCCESS != (ret = ObRowFuse::fuse_row(commit_prepare_ups_row,
                                                           sstable_row,
                                                           result)))
      {
        YYSYS_LOG(WARN,"fail to fuse commit_prepare_row and sstable row!ups_row=[%s],sst_row=[%s],ret=%d",
                  to_cstring(*commit_prepare_ups_row),
                  to_cstring(*sstable_row),ret);
      }
      else if (need_expand_sst_row
               && OB_SUCCESS != (ret = ObRowFuse::fuse_row(commit_prepare_ups_row,
                                                           &expand_sst_row,
                                                           result)))
      {
        YYSYS_LOG(WARN,"fail to fuse commit_prepare_row and expand_sstable row!ups_row=[%s],expand_sst_row=[%s],ret=%d",
                  to_cstring(*commit_prepare_ups_row),
                  to_cstring(expand_sst_row),ret);
      }

      if (OB_SUCCESS == ret
          && need_reset_commit_ext_type)
      {
        ObObj nop_commit_ext_type;
        nop_commit_ext_type.set_int(ObActionFlag::OP_NOP);
        if (OB_SUCCESS != (ret = result->set_cell(table_id,
                                                  param.commit_data_extendtype_cid_,
                                                  nop_commit_ext_type)))
        {
          YYSYS_LOG(WARN,"fail to reset commit ext type!ret=%d,reslut_row=[%s]",
                    ret,to_cstring(*result));
        }
      }


      //READ_ATOMIC read_atomic::debug
      YYSYS_LOG(DEBUG,"read_atomic::debug,finish fuse row!"
                "need_reset_commit_ext_type=%d,need_expand_sst_row=%d,"
                "expand_sst_row=[%s],ret=%d",
                need_reset_commit_ext_type,need_expand_sst_row,
                to_cstring(expand_sst_row),ret);
      if (NULL != result)
      {
        YYSYS_LOG(DEBUG,"read_atomic::debug,result_row=[%s],ret=%d",
                  to_cstring(*result),ret);
      }

      return ret;
    }

    /*  @berif  ��ȡ һ�������� ������ Ԫ���ݣ�last_commit_transver last_prepare_transver  verset_array�� ��data_mark����
    * @param commit_prepare_row[in] ��õ�һ�е�����
    * @param param [in]
    * @param  last_commit_transver[out]  commit���������µ�Ԫ�صİ汾
    * @param  last_prepare_transver[out]  prepare���������µ�Ԫ�صİ汾
    * @param  verset_array [out] precommit ���ϣ�last_commit ǰ��N��Ԫ�صļ��ϣ�
    * @param   data_mark[out]
    * */
    int ObReadAtomicHelper::get_extend_vals_(const common::ObRow *commit_prepare_row,
                                             const common::ObReadAtomicParam &param,
                                             const common::ObTransVersion **last_commit_transver,
                                             const common::ObTransVersion **last_prepare_transver,
                                             const common::ObTransVersion **verset_array,
                                             int64_t *total_ver_count,
                                             int64_t *commit_ext_type,
                                             int64_t *prepare_ext_type,
                                             ObReadAtomicDataMark *data_mark)
    {
      int ret = OB_SUCCESS;
      int64_t max_array_size = 0;
      const ObObj *cell  = NULL;
      int64_t  cell_idx  = OB_INVALID_INDEX;
      uint64_t tid       = OB_INVALID_ID;
      uint64_t cid       = OB_INVALID_ID;

      uint64_t   table_id = param.table_id_;
      const ObRowDesc* row_desc = NULL;
      if (!param.is_valid()
          || OB_INVALID_ID == table_id
          || NULL == commit_prepare_row
          || NULL == commit_prepare_row->get_row_desc())
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(WARN,"invalid argument!read atomic param=[%s],src_row=%p,table_id=%ld,ret=%d",
                  to_cstring(param),commit_prepare_row,table_id,ret);
      }
      else
      {
        row_desc = commit_prepare_row->get_row_desc();
        // add by maosy [FIX_SUPPORT_READ_ATOMIC] 20170628:b
        {
          //todo,��ʱ����ԭ��
          int64_t num = row_desc->get_column_num();
          for(int64_t index = 0 ; index <num ;index++)
          {
            if(OB_SUCCESS !=(ret = row_desc->get_tid_cid(index,tid,cid)))
            {}
            else if(cid < OB_ACTION_FLAG_COLUMN_ID && cid >param.min_used_column_cid_ )
            {
              table_id = tid;
              YYSYS_LOG(INFO,"TABLE ID = %lu",table_id);
              break;
            }
          }
        }
        // add by maosy e

        //1==>get last commit transver
        if (OB_SUCCESS == ret
            && NULL != last_commit_transver
            && param.need_last_commit_trans_ver_)
        {
          cell     = NULL;
          cell_idx = OB_INVALID_INDEX;
          tid      = OB_INVALID_ID;
          cid      = OB_INVALID_ID;
          if (OB_INVALID_INDEX == (cell_idx = row_desc->get_idx(table_id,param.last_commit_trans_ver_cid_)))
          {
            *last_commit_transver = NULL;
            //            YYSYS_LOG(WARN,"didn't get last commit trans version from src_row=[%s]",
            //                      to_cstring(commit_prepare_row));
          }
          else if (OB_SUCCESS != (ret = commit_prepare_row->raw_get_cell(cell_idx,cell)))
          {
            YYSYS_LOG(WARN,"fail to get last commit trans version obj!table_id=%ld,ret=%d",
                      table_id,ret);
          }
          else if (OB_SUCCESS != (ret = get_trans_ver(*cell,last_commit_transver)))
          {
            YYSYS_LOG(WARN,"fail to get last commit transvesion!table_id=%ld,cell=[%s],ret=%d",
                      table_id,to_cstring(*cell),ret);
          }
        }
        else if (NULL != last_commit_transver)
        {
          *last_commit_transver = NULL;
        }

        //2==>get last prepare transver
        if (OB_SUCCESS == ret
            && NULL != last_prepare_transver
            && param.need_last_prepare_trans_ver_)
        {
          cell     = NULL;
          cell_idx = OB_INVALID_INDEX;
          tid      = OB_INVALID_ID;
          cid      = OB_INVALID_ID;
          if (OB_INVALID_INDEX == (cell_idx = row_desc->get_idx(table_id,param.last_prepare_trans_ver_cid_)))
          {
            *last_prepare_transver = NULL;
            //            YYSYS_LOG(WARN,"didn't get last prepare trans version from src_row=[%s]",
            //                      to_cstring(commit_prepare_row));
          }
          else if (OB_SUCCESS != (ret = commit_prepare_row->raw_get_cell(cell_idx,cell)))
          {
            YYSYS_LOG(WARN,"fail to get last prepare trans version obj!table_id=%ld,ret=%d",
                      table_id,ret);
          }
          else if (OB_SUCCESS != (ret = get_trans_ver(*cell,last_prepare_transver)))
          {
            YYSYS_LOG(WARN,"fail to get last prepare trans vesion!table_id=%ld,cell=[%s],cell_type=%d,ret=%d",
                      table_id,to_cstring(*cell),cell->get_type(),ret);
          }
        }
        else if (NULL != last_prepare_transver)
        {
          *last_prepare_transver = NULL;
        }

        //3==>get prevcommit trans version
        if (OB_SUCCESS == ret
            && NULL != verset_array
            && NULL != total_ver_count
            && *total_ver_count > 0
            && param.need_prevcommit_trans_verset_)
        {
          tid      = OB_INVALID_ID;
          cid      = OB_INVALID_ID;
          cell     = NULL;
          cell_idx = OB_INVALID_INDEX;
          max_array_size   = *total_ver_count;
          *total_ver_count = 0;
          int64_t max_ver_count = param.max_prevcommit_trans_verset_count_;
          uint64_t trans_verset_cids[MAX_TRANS_VERSET_SIZE];/*precommit���� Ԫ�ض�Ӧ��column_id ����*/
          int64_t buf_size = MAX_TRANS_VERSET_SIZE;
          if (OB_SUCCESS != (ret = gen_prevcommit_trans_verset_cids(param,
                                                                    trans_verset_cids,
                                                                    buf_size)))
          {
            YYSYS_LOG(WARN,"fail to get prevcommit trans ver cids!table_id=%ld,ret=%d",
                      table_id,ret);
          }
          else
          {
            for (int64_t i=0;
                 OB_SUCCESS == ret
                 && i <max_ver_count
                 && i<buf_size
                 && (*total_ver_count) < max_array_size;
                 i++)
            {
              if (OB_INVALID_INDEX ==(cell_idx = row_desc->get_idx(table_id,trans_verset_cids[i])))
              {
                //means cur trans ver cid isn't contained on this row!
                continue;
              }
              else if (OB_SUCCESS != (ret = commit_prepare_row->raw_get_cell(cell_idx,cell)))
              {
                YYSYS_LOG(WARN,"fail to get prevcommit trans version obj!table_id=%ld,ret=%d",
                          table_id,ret);
              }
              else if (is_trans_ver_obj(*cell,true) && OB_SUCCESS == (ret = get_trans_ver(*cell,
                                                                                          &(verset_array[(*total_ver_count)]),
                                                                                          true)))
              {//all prevcommit trans version must be valid
                (*total_ver_count)++;
              }
              else if (OB_SUCCESS != ret)
              {
                YYSYS_LOG(WARN,"fail to get the %ldth prevcommit transversion!cell=[%s],idx=%ld,ret=%d",
                          (*total_ver_count),to_cstring(*cell),i,ret);
              }
            }
          }
        }
        else if (NULL != total_ver_count)
        {
          *total_ver_count = 0;
        }

        //4==>get commit data extend type
        if (OB_SUCCESS == ret
            && (param.need_commit_data_
                || param.need_exact_transver_data_
                || param.need_exact_data_mark_data_)
            && NULL != commit_ext_type)
        {
          tid  = OB_INVALID_ID;
          cid  = OB_INVALID_ID;
          cell = NULL;
          cell_idx         = OB_INVALID_INDEX;
          *commit_ext_type = OB_INVALID_DATA;
          if (OB_INVALID_INDEX ==(cell_idx = row_desc->get_idx(table_id,param.commit_data_extendtype_cid_)))
          {
            //means commit list has no extendtype obj!
            //            *commit_ext_type      = OB_INVALID_DATA;
          }
          else if (OB_SUCCESS != (ret = commit_prepare_row->raw_get_cell(cell_idx,cell)))
          {
            YYSYS_LOG(WARN,"fail to commit data extendtype obj!table_id=%ld,ret=%d",
                      table_id,ret);
          }
          else if (OB_SUCCESS != (ret = get_extend_type(*cell,*commit_ext_type)))
          {
            YYSYS_LOG(WARN,"invalid commit extendtype obj!obj=[%s],ext_type=%ld,table_id=%ld,ret=%d",
                      to_cstring(*cell),*commit_ext_type,table_id,ret);
          }
        }
        else if (NULL != commit_ext_type)
        {
          *commit_ext_type = OB_INVALID_DATA;
        }

        //5==>get prepare data extend type
        if (OB_SUCCESS == ret
            && param.need_prepare_data_
            && NULL != prepare_ext_type)
        {
          tid  = OB_INVALID_ID;
          cid  = OB_INVALID_ID;
          cell = NULL;
          cell_idx          = OB_INVALID_INDEX;
          *prepare_ext_type = OB_INVALID_DATA;
          if (OB_INVALID_INDEX ==(cell_idx = row_desc->get_idx(table_id,param.prepare_data_extendtype_cid_)))
          {
            //means prepare list has no extendtype obj!
            //            *prepare_ext_type = OB_INVALID_DATA;
          }
          else if (OB_SUCCESS != (ret = commit_prepare_row->raw_get_cell(cell_idx,cell)))
          {
            YYSYS_LOG(WARN,"fail to prepare data extendtype obj!table_id=%ld,ret=%d",
                      table_id,ret);
          }
          else if (OB_SUCCESS != (ret = get_extend_type(*cell,*prepare_ext_type)))
          {
            YYSYS_LOG(WARN,"invalid prepare extendtype obj!obj=[%s],ext_type=%ld,table_id=%ld,ret=%d",
                      to_cstring(*cell),*prepare_ext_type,table_id,ret);
          }
        }
        else if (NULL != prepare_ext_type)
        {
          *prepare_ext_type = OB_INVALID_DATA;
        }

        //prepare_ext_type and commit_ext_type will return ObActionFlag::OP_NOP
        //or ObActionFlag::OP_VALID or ObActionFlag::OP_DEL_ROW
        //or OB_INVALID_DATA
        if (OB_SUCCESS == ret && NULL != commit_ext_type)
        {
          if (param.commit_ext_type_must_valid() && OB_INVALID_DATA == *commit_ext_type)
          {
            ret = OB_ERR_UNEXPECTED;
            YYSYS_LOG(ERROR,"get invalid commit extend type!ret=%d",ret);
          }
          else if (ObActionFlag::OP_NOP        != *commit_ext_type
                   && ObActionFlag::OP_ROW_DOES_NOT_EXIST != *commit_ext_type
                   && ObActionFlag::OP_VALID   != *commit_ext_type
                   && ObActionFlag::OP_DEL_ROW != *commit_ext_type
                   && OB_INVALID_DATA != *commit_ext_type)
          {
            ret = OB_NOT_SUPPORTED;
            YYSYS_LOG(ERROR,"unknow commit extend type[%ld],ret=%d",
                      *commit_ext_type,ret);
          }
        }

        if (OB_SUCCESS == ret && NULL != prepare_ext_type)
        {
          if (param.prepare_ext_type_must_valid() && OB_INVALID_DATA == *prepare_ext_type)
          {
            ret = OB_ERR_UNEXPECTED;
            YYSYS_LOG(ERROR,"get invalid prepare extend type!ret=%d",ret);
          }
          else if (ObActionFlag::OP_NOP        != *prepare_ext_type
                   && ObActionFlag::OP_ROW_DOES_NOT_EXIST != *prepare_ext_type
                   && ObActionFlag::OP_VALID   != *prepare_ext_type
                   && ObActionFlag::OP_DEL_ROW != *prepare_ext_type
                   && OB_INVALID_DATA != *prepare_ext_type)
          {
            ret = OB_NOT_SUPPORTED;
            YYSYS_LOG(ERROR,"unknow prepare extend type[%ld],ret=%d",
                      *prepare_ext_type,ret);
          }
        }

        //6==>get incre data mark
        if (OB_SUCCESS == ret
            && param.need_data_mark_
            && NULL != data_mark)
        {
          const ObObj *paxos_id_cell = NULL;
          const ObObj *major_ver_cell = NULL;
          const ObObj *minor_ver_start_cell = NULL;
          const ObObj *minor_ver_end_cell   = NULL;
          const ObObj *data_store_type_cell = NULL;
          int64_t  paxos_id_idx   = OB_INVALID_INDEX;
          int64_t  major_ver_idx  = OB_INVALID_INDEX;
          int64_t  minor_start_ver_idx = OB_INVALID_INDEX;
          int64_t  minor_end_ver_idx   = OB_INVALID_INDEX;
          int64_t  data_store_type_idx = OB_INVALID_INDEX;
          tid      = OB_INVALID_ID;
          cid      = OB_INVALID_ID;
          data_mark->reset();
          if (OB_INVALID_INDEX == (paxos_id_idx = row_desc->get_idx(table_id,param.ups_paxos_id_cid_))
              || OB_INVALID_INDEX == (major_ver_idx = row_desc->get_idx(table_id,param.major_ver_cid_))
              || OB_INVALID_INDEX == (minor_start_ver_idx = row_desc->get_idx(table_id,param.minor_ver_start_cid_))
              || OB_INVALID_INDEX == (minor_end_ver_idx   = row_desc->get_idx(table_id,param.minor_ver_end_cid_))
              || OB_INVALID_INDEX == (data_store_type_idx = row_desc->get_idx(table_id,param.data_store_type_cid_)))
          {
            ret = OB_ERR_UNEXPECTED;
            YYSYS_LOG(ERROR,"invalid data mark!cur_row=[%s],"
                      "paxos_id_cid_idx=%ld,major_ver_idx=%ld,"
                      "minor_start_ver_idx=%ld,minor_end_ver_idx=%ld,"
                      "data_store_type_idx=%ld,ret=%d",
                      to_cstring(*commit_prepare_row),
                      paxos_id_idx,major_ver_idx,minor_start_ver_idx,minor_end_ver_idx,
                      data_store_type_idx,ret);
          }
          else if (OB_SUCCESS != (ret = commit_prepare_row->raw_get_cell(paxos_id_idx,
                                                                         paxos_id_cell)))
          {
            YYSYS_LOG(WARN,"fail to get ups paxos id obj!table_id=%ld,ret=%d",
                      table_id,ret);
          }
          else if (OB_SUCCESS != (ret = commit_prepare_row->raw_get_cell(major_ver_idx,
                                                                         major_ver_cell)))
          {
            YYSYS_LOG(WARN,"fail to get major_ver obj!table_id=%ld,ret=%d",
                      table_id,ret);
          }
          else if (OB_SUCCESS != (ret = commit_prepare_row->raw_get_cell(minor_start_ver_idx,
                                                                         minor_ver_start_cell)))
          {
            YYSYS_LOG(WARN,"fail to get minor ver start obj!table_id=%ld,ret=%d",
                      table_id,ret);
          }
          else if (OB_SUCCESS != (ret = commit_prepare_row->raw_get_cell(minor_end_ver_idx,
                                                                         minor_ver_end_cell)))
          {
            YYSYS_LOG(WARN,"fail to get minor ver end obj!table_id=%ld,ret=%d",
                      table_id,ret);
          }
          else if (OB_SUCCESS != (ret = commit_prepare_row->raw_get_cell(data_store_type_idx,
                                                                         data_store_type_cell)))
          {
            YYSYS_LOG(WARN,"fail to get data store type obj!table_id=%ld,ret=%d",
                      table_id,ret);
          }
          else if (OB_SUCCESS != (ret = get_data_mark(*major_ver_cell,
                                                      *minor_ver_start_cell,
                                                      *minor_ver_end_cell,
                                                      *paxos_id_cell,
                                                      *data_store_type_cell,
                                                      *data_mark)))
          {
            YYSYS_LOG(WARN,"fail to data_mark!table_id=%ld,data_mark=[%s],ret=%d",
                      table_id,to_cstring(*data_mark),ret);
          }
        }
        else if (NULL != data_mark)
        {
          data_mark->reset();
        }
      }
      return ret;
    }
    int ObReadAtomicHelper::split_commit_preapre_row_(const ObRow *commit_prepare_row,
                                                      const ObReadAtomicParam &param,
                                                      ObRow *commit_row,
                                                      bool *is_commit_row_exist,
                                                      ObRow *prepare_row,
                                                      bool *is_prepare_row_exist)
    {
      int ret = OB_SUCCESS;
      uint64_t table_id   = param.table_id_;
      int64_t src_column_num  = 0;
      int64_t commit_ext_type      = OB_INVALID_DATA;
      int64_t prepare_ext_type     = OB_INVALID_DATA;
      const ObRowDesc *commit_row_desc  = NULL;
      const ObRowDesc *prepare_row_desc = NULL;
      const ObObj *src_row_action_flag     = NULL;
      ObObj *commit_row_action_flag  = NULL;
      ObObj *prepare_row_action_flag = NULL;
      bool  is_need_prepare_row      = false;
      bool  is_need_commit_row       = false;

      const ObObj *cell   = NULL;
      uint64_t tid        = OB_INVALID_ID;
      uint64_t cid        = OB_INVALID_ID;
      uint64_t new_cid    = OB_INVALID_ID;

      if (NULL != commit_prepare_row)
      {
        YYSYS_LOG(DEBUG,"read_atomic::debug,begin split read atomic row!src_row=[%s],param=[%s]",
                  to_cstring(*commit_prepare_row),to_cstring(param));
      }

      if (!param.is_valid()
          || OB_INVALID_ID == table_id
          || NULL == commit_prepare_row)
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(ERROR,"invalid argument!tid=%ld,src_row=%p,param=[%s],ret=%d",
                  table_id,commit_prepare_row,to_cstring(param),ret);
      }
      else if (0 >= (src_column_num = commit_prepare_row->get_column_num()))
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(ERROR,"invalid argument!tid=%ld,src_row=[%s],src_column_num=%ld,ret=%d",
                  table_id,to_cstring(*commit_prepare_row),src_column_num,ret);
      }
      else if (NULL != commit_row && NULL == (commit_row_desc = commit_row->get_row_desc()))
      {
        ret = OB_NOT_INIT;
        YYSYS_LOG(ERROR,"must set commit row desc!table_id=%ld,ret=%d",table_id,ret);
      }
      else if (NULL != commit_prepare_row->get_row_desc()
               && OB_INVALID_INDEX != commit_prepare_row->get_row_desc()->get_idx(OB_INVALID_ID,
                                                                                  OB_ACTION_FLAG_COLUMN_ID)
               && OB_SUCCESS != (ret = commit_prepare_row->get_cell(OB_INVALID_ID,
                                                                    OB_ACTION_FLAG_COLUMN_ID,
                                                                    src_row_action_flag)))
      {
        YYSYS_LOG(WARN,"fail to get src row action flag!ret=%d",ret);
      }
      else if (NULL != commit_row_desc
               && OB_INVALID_INDEX != commit_row_desc->get_idx(OB_INVALID_ID,
                                                               OB_ACTION_FLAG_COLUMN_ID)
               && OB_SUCCESS != (ret = commit_row->get_cell(OB_INVALID_ID,
                                                            OB_ACTION_FLAG_COLUMN_ID,
                                                            commit_row_action_flag)))
      {
        YYSYS_LOG(WARN,"fail to get commit row action flag!ret=%d",ret);
      }
      else if (NULL != prepare_row && NULL == (prepare_row_desc = prepare_row->get_row_desc()))
      {
        ret = OB_NOT_INIT;
        YYSYS_LOG(ERROR,"must set prepare row desc!table_id=%ld,ret=%d",table_id,ret);
      }
      else if (NULL != prepare_row_desc
               && OB_INVALID_INDEX != prepare_row_desc->get_idx(OB_INVALID_ID,
                                                                OB_ACTION_FLAG_COLUMN_ID)
               && OB_SUCCESS != (ret = prepare_row->get_cell(OB_INVALID_ID,
                                                             OB_ACTION_FLAG_COLUMN_ID,
                                                             prepare_row_action_flag)))
      {
        YYSYS_LOG(WARN,"fail to get prepare row action flag!ret=%d",ret);
      }
      else
      {//init
        if (NULL != commit_row
            &&(param.need_commit_data_
               || param.need_exact_transver_data_
               || param.need_exact_data_mark_data_))
        {
          is_need_commit_row = true;
        }

        if (NULL != prepare_row
            && param.need_prepare_data_)
        {
          is_need_prepare_row = true;
        }
      }

      //1:check action flag obj!
      if (OB_SUCCESS == ret
          && (is_need_commit_row || is_need_prepare_row))
      {

        if (OB_SUCCESS != (ret = get_extend_vals_(commit_prepare_row,
                                                  param,NULL,NULL,NULL,NULL,
                                                  &commit_ext_type,
                                                  &prepare_ext_type)))
        {
          YYSYS_LOG(WARN,"fail to get commit and prepare data extend type obj!table_id=%ld,ret=%d",
                    table_id,ret);
        }
        else
        {
          //try decide if need commit data anymore!
          if (is_need_commit_row)
          {
            if (ObActionFlag::OP_DEL_ROW == commit_ext_type
                || ObActionFlag::OP_ROW_DOES_NOT_EXIST == commit_ext_type)
            {//means cur row is ups data and commit part has no data
              is_need_commit_row = false;
            }
            else if (NULL != src_row_action_flag
                     && ObExtendType == src_row_action_flag->get_type()
                     && ObActionFlag::OP_ROW_DOES_NOT_EXIST == src_row_action_flag->get_ext())
            {//means we can't decide it by commit extend type!
              //must look up action flag!
              is_need_commit_row = false;
            }
          }

          //decide if need prepare data anymore!
          if (is_need_prepare_row && (ObActionFlag::OP_DEL_ROW == prepare_ext_type
                                      || ObActionFlag::OP_NOP == prepare_ext_type
                                      || ObActionFlag::OP_ROW_DOES_NOT_EXIST == prepare_ext_type))
          {//no matter repare row is ups row or cs or ms row!
            //
            is_need_prepare_row = false;
          }
        }
      }

      //2:get commit row and prepare row!
      if (OB_SUCCESS == ret
          && (is_need_commit_row || is_need_prepare_row ))
      {
        for (int64_t col_idx = 0; OB_SUCCESS == ret && col_idx < src_column_num; col_idx++)
        {
          cell  = NULL;
          tid   = OB_INVALID_ID;
          cid   = OB_INVALID_ID;
          new_cid     = OB_INVALID_ID;
          if (OB_SUCCESS != (ret = commit_prepare_row->raw_get_cell(col_idx, cell, tid, cid)))
          {
            YYSYS_LOG(WARN, "fail to get cell:ret[%d],idx=%ld,tid=%lu,cid=%lu",
                      ret,col_idx,tid,cid);
          }

          //fill commit row
          if (OB_SUCCESS == ret && is_need_commit_row
              && OB_INVALID_INDEX != commit_row_desc->get_idx(tid,cid)
              && OB_SUCCESS != (ret = commit_row->set_cell(tid,cid,*cell)))
          {
            YYSYS_LOG(WARN,"fail to set commit cell!tid=%ld,cid=%ld,src_col_idx=%ld,cell=[%s],ret=%d",
                      tid,cid,col_idx,to_cstring(*cell),ret);
          }

          if (OB_SUCCESS == ret && is_need_prepare_row)
          {
            if (is_prepare_cid(param,cid))
            {//use the real prepare cell to cover write the commit cell
              if (OB_SUCCESS != (ret = convert_prepare_cid_to_commit_cid(param,cid,new_cid)))
              {
                YYSYS_LOG(WARN,"fail to convert prepare_cid[%ld] to commit_cid[%ld]!table_id=%ld,ret=%d",
                          cid,new_cid,table_id,ret);
              }
              else if (OB_INVALID_INDEX == prepare_row_desc->get_idx(tid,new_cid))
              {
                //do nothing,use the old commit cell as the final prepare cell!
              }
              else if (OB_SUCCESS != (ret = prepare_row->set_cell(tid,new_cid,*cell)))
              {
                YYSYS_LOG(WARN,"fail to set prepare cell!tid=%ld,cid=%ld,new_cid=%ld,"
                          "src_col_idx=%ld,cell=[%s],ret=%d",
                          tid,cid,new_cid,col_idx,to_cstring(*cell),ret);
              }
            }
            else if (OB_INVALID_INDEX != prepare_row_desc->get_idx(tid,cid)
                     && OB_SUCCESS != (ret = prepare_row->set_cell(tid,cid,*cell)))
            {//use commit cell to write the prepare cell

              YYSYS_LOG(WARN,"fail to set prepare cell!tid=%ld,cid=%ld,"
                        "src_col_idx=%ld,cell=[%s],ret=%d",
                        tid,cid,col_idx,to_cstring(*cell),ret);
            }

          }
        }//end of for
      }//end of 2

      //finish row
      if (OB_SUCCESS == ret)
      {
        if (is_need_commit_row)
        {
          if (NULL != is_commit_row_exist)
            *is_commit_row_exist = true;

          if (NULL != commit_row_action_flag
              && NULL != is_commit_row_exist
              && ObExtendType == commit_row_action_flag->get_type()
              && ObActionFlag::OP_ROW_DOES_NOT_EXIST == commit_row_action_flag->get_ext())
          {
            //            commit_row_action_flag->set_ext(ObActionFlag::OP_VALID);
            *is_commit_row_exist = false;
          }

        }
        else
        {
          if (NULL != commit_row)
            commit_row->reset(false, ObRow::DEFAULT_NULL);
          if (NULL != is_commit_row_exist)
            *is_commit_row_exist = false;
          if (NULL != commit_row_action_flag)
            commit_row_action_flag->set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
        }
      }

      if (OB_SUCCESS == ret)
      {
        if (is_need_prepare_row)
        {
          if (NULL != prepare_row_action_flag
              && ObExtendType == prepare_row_action_flag->get_type()
              && ObActionFlag::OP_ROW_DOES_NOT_EXIST == prepare_row_action_flag->get_ext())
          {
            prepare_row_action_flag->set_ext(ObActionFlag::OP_VALID);
          }

          if (NULL != is_prepare_row_exist)
            *is_prepare_row_exist = true;
        }
        else
        {
          if (NULL != prepare_row)
            prepare_row->reset(false, ObRow::DEFAULT_NULL);
          if (NULL != is_prepare_row_exist)
            *is_prepare_row_exist = false;
          if (NULL != prepare_row_action_flag)
            prepare_row_action_flag->set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
        }
      }

      if (NULL != commit_row)
      {
        YYSYS_LOG(DEBUG,"read_atomic::debug,final splited commit row=[%s],is_need_commit_row=%d,commit_ext_type=%ld,ret=%d",
                  to_cstring(*commit_row),is_need_commit_row,commit_ext_type,ret);
      }

      if (NULL != prepare_row)
      {
        YYSYS_LOG(DEBUG,"read_atomic::debug,final splited prepare row=[%s],is_need_prepare_row=%d,prepare_ext_type=%ld,ret=%d",
                  to_cstring(*prepare_row),is_need_prepare_row,prepare_ext_type,ret);
      }

      return ret;
    }

    int ObReadAtomicHelper::add_data_mark_into_row(const ObReadAtomicParam &param,
                                                   const ObReadAtomicDataMark &data_mark,
                                                   ObRow &des_row)
    {
      int ret = OB_SUCCESS;
      uint64_t table_id = param.table_id_;
      const ObRowDesc* row_desc = NULL;
      common::ObObj minor_ver_start_cell;
      common::ObObj minor_ver_end_cell;
      common::ObObj major_ver_cell;
      common::ObObj ups_paxos_id_cell;
      common::ObObj data_store_type_cell;

      YYSYS_LOG(DEBUG,"read_atomic::debug,begin add data mark into row!"
                "orig_row=[%s],data_mark[%s]",
                to_cstring(des_row),to_cstring(data_mark));

      if (!param.is_valid()
          || !data_mark.is_valid()
          || OB_INVALID_ID == table_id
          || NULL == (row_desc = des_row.get_row_desc()))
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(ERROR,"invalid argument!param[%s],data_mark=[%s],row_desc=%p,ret=%d",
                  to_cstring(param),to_cstring(data_mark),row_desc,ret);
      }
      else if (!param.need_data_mark_)
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(ERROR,"didn't need data mark!ret=%d",ret);
      }
      else
      {
        minor_ver_start_cell.set_int(data_mark.minor_version_start_);
        minor_ver_end_cell.set_int(data_mark.minor_version_end_);
        major_ver_cell.set_int(data_mark.major_version_);
        ups_paxos_id_cell.set_int(data_mark.ups_paxos_id_);
        data_store_type_cell.set_int(data_mark.data_store_type_);
        if (OB_SUCCESS != (ret = des_row.set_cell(table_id,
                                                  param.minor_ver_start_cid_,
                                                  minor_ver_start_cell)))
        {
          YYSYS_LOG(WARN,"fail to add minor ver start cell!ret=%d",ret);
        }
        else if (OB_SUCCESS != (ret = des_row.set_cell(table_id,
                                                       param.minor_ver_end_cid_,
                                                       minor_ver_end_cell)))
        {
          YYSYS_LOG(WARN,"fail to add minor ver end cell!ret=%d",ret);
        }
        else if (OB_SUCCESS != (ret = des_row.set_cell(table_id,
                                                       param.major_ver_cid_,
                                                       major_ver_cell)))
        {
          YYSYS_LOG(WARN,"fail to add major ver cell!ret=%d",ret);
        }
        else if (OB_SUCCESS != (ret = des_row.set_cell(table_id,
                                                       param.ups_paxos_id_cid_,
                                                       ups_paxos_id_cell)))
        {
          YYSYS_LOG(WARN,"fail to add ups paxos id cell!ret=%d",ret);
        }
        else if (OB_SUCCESS != (ret = des_row.set_cell(table_id,
                                                       param.data_store_type_cid_,
                                                       data_store_type_cell)))
        {
          YYSYS_LOG(WARN,"fail to add data store type cell!ret=%d",ret);
        }
      }

      YYSYS_LOG(DEBUG,"read_atomic::debug,finish add data mark into row!"
                "final_row=[%s],ret=%d",to_cstring(des_row),ret);

      return ret;
    }
    //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
    ObDataMarkParam::ObDataMarkParam(const ObDataMarkParam &other)
    {
      reset();
      need_modify_time_   = other.need_modify_time_;
      need_major_version_ = other.need_major_version_;
      need_minor_version_ = other.need_minor_version_;
      need_data_store_type_ = other.need_data_store_type_;
      modify_time_cid_    = other.modify_time_cid_;
      major_version_cid_  = other.major_version_cid_;
      minor_ver_start_cid_= other.minor_ver_start_cid_;
      minor_ver_end_cid_  = other.minor_ver_end_cid_;
      data_store_type_cid_= other.data_store_type_cid_;
      table_id_           = other.table_id_;
    }

    ObDataMarkParam& ObDataMarkParam::operator =(const ObDataMarkParam &other)
    {
      if (this != &other)
      {
        reset();
        need_modify_time_   = other.need_modify_time_;
        need_major_version_ = other.need_major_version_;
        need_minor_version_ = other.need_minor_version_;
        need_data_store_type_ = other.need_data_store_type_;
        modify_time_cid_    = other.modify_time_cid_;
        major_version_cid_  = other.major_version_cid_;
        minor_ver_start_cid_= other.minor_ver_start_cid_;
        minor_ver_end_cid_  = other.minor_ver_end_cid_;
        data_store_type_cid_= other.data_store_type_cid_;
        table_id_           = other.table_id_;
      }
      return *this;
    }

    void ObDataMarkParam::reset()
    {
      need_modify_time_   = false;
      need_major_version_ = false;
      need_minor_version_ = false;
      need_data_store_type_ = false;
      modify_time_cid_    = 0;
      major_version_cid_  = 0;
      minor_ver_start_cid_= 0;
      minor_ver_end_cid_  = 0;
      data_store_type_cid_= 0;
      table_id_           = OB_INVALID_ID;
    }

    bool ObDataMarkParam::is_inited()const
    {
      bool bret = false;
      if (need_modify_time_
          || need_major_version_
          || need_minor_version_
          || need_data_store_type_)
      {
        bret = true;
      }
      return bret;
    }

    bool ObDataMarkParam::is_valid()const
    {
      bool bret = false;
      if (!is_inited())
      {
      }
      else if (OB_INVALID_ID == table_id_
               || table_id_ <= 0)
      {
        YYSYS_LOG(WARN,"invalid table id=%ld",table_id_);
      }
      else if (need_modify_time_
               && (modify_time_cid_ <= 0
                   || OB_INVALID_ID == modify_time_cid_))
      {
        YYSYS_LOG(WARN,"invalid modify time cid[%ld]!",modify_time_cid_);
      }
      else if (need_major_version_
               && (major_version_cid_ <= 0
                   || OB_INVALID_ID == major_version_cid_))
      {
        YYSYS_LOG(WARN,"invalid major version cid[%ld]!",major_version_cid_);
      }
      else if (need_minor_version_
               && (minor_ver_start_cid_ <= 0
                   || minor_ver_end_cid_ <= 0
                   || OB_INVALID_ID == minor_ver_start_cid_
                   || OB_INVALID_ID == minor_ver_end_cid_))
      {
        YYSYS_LOG(WARN,"invalid minor_ver start cid[%ld] or end cid[%ld]!",
                  minor_ver_start_cid_,minor_ver_end_cid_);
      }
      else if (need_data_store_type_
               && (data_store_type_cid_ <= 0
                   || OB_INVALID_ID == data_store_type_cid_))
      {
        YYSYS_LOG(WARN,"invalid data store type cid[%ld]!",data_store_type_cid_);
      }
      else
      {
        bret = true;
      }
      return bret;
    }

    bool ObDataMarkParam::is_data_mark_cid(const uint64_t cid) const
    {
      bool bret = false;
      if ((need_modify_time_ && cid == modify_time_cid_)
          || (need_major_version_ && cid == major_version_cid_)
          || (need_minor_version_ && cid == minor_ver_start_cid_)
          || (need_minor_version_ && cid == minor_ver_end_cid_)
          || (need_data_store_type_ && cid == data_store_type_cid_))
      {
        bret = true;
      }
      return bret;
    }

    int64_t ObDataMarkParam::to_string(char *buf, int64_t buf_len) const
    {
      int64_t pos = 0;
      databuff_printf(buf, buf_len, pos, "ObDataMarkParam(");
      databuff_printf(buf, buf_len, pos, "table_id=%ld,",
                      table_id_);
      databuff_printf(buf, buf_len, pos, "need_modify_time_=%d,",
                      need_modify_time_);
      databuff_printf(buf, buf_len, pos, "need_major_version_=%d,",
                      need_major_version_);
      databuff_printf(buf, buf_len, pos, "need_prevcommit_trans_verset_=%d,",
                      need_minor_version_);
      databuff_printf(buf, buf_len, pos, "need_data_store_type_=%d,",
                      need_data_store_type_);
      databuff_printf(buf, buf_len, pos, "modify_time_cid_=%ld,",
                      modify_time_cid_);
      databuff_printf(buf, buf_len, pos, "major_version_cid_=%ld,",
                      major_version_cid_);
      databuff_printf(buf, buf_len, pos, "minor_ver_start_cid_=%ld,",
                      minor_ver_start_cid_);
      databuff_printf(buf, buf_len, pos, "minor_ver_end_cid_=%ld,",
                      minor_ver_end_cid_);
      databuff_printf(buf, buf_len, pos, "data_store_type_cid_=%ld,",
                      data_store_type_cid_);
      databuff_printf(buf, buf_len, pos, ")");
      return pos;
    }



    DEFINE_SERIALIZE(ObDataMarkParam)
    {
      int     ret     = OB_SUCCESS;
      int64_t tmp_pos = pos;
      if (NULL == buf || buf_len <= 0 || pos > buf_len)
      {
        YYSYS_LOG(WARN, "invalid param, buf=%p, buf_len=%ld, pos=%ld",
                  buf, buf_len, pos);
        ret = OB_INVALID_ARGUMENT;
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf,buf_len,tmp_pos,table_id_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_bool(buf, buf_len, tmp_pos, need_modify_time_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_bool(buf, buf_len, tmp_pos, need_major_version_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_bool(buf, buf_len, tmp_pos, need_minor_version_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_bool(buf, buf_len, tmp_pos, need_data_store_type_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf,buf_len,tmp_pos,modify_time_cid_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf,buf_len,tmp_pos,major_version_cid_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf,buf_len,tmp_pos,minor_ver_start_cid_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf,buf_len,tmp_pos,minor_ver_end_cid_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_i64(buf,buf_len,tmp_pos,data_store_type_cid_);
      }
      if (OB_SUCCESS == ret)
      {
        pos = tmp_pos;
      }

      return ret;
    }

    DEFINE_DESERIALIZE(ObDataMarkParam)
    {
      int     ret     = OB_SUCCESS;
      int64_t tmp_pos = pos;
      if (NULL == buf || data_len <= 0 || pos > data_len)
      {
        YYSYS_LOG(WARN, "invalid param, buf=%p, data_len=%ld, pos=%ld",
                  buf, data_len, pos);
        ret = OB_INVALID_ARGUMENT;
      }

      //better reset
      if (OB_SUCCESS == ret)
      {
        reset();
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&table_id_));
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_bool(buf, data_len, tmp_pos, &need_modify_time_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_bool(buf, data_len, tmp_pos, &need_major_version_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_bool(buf, data_len, tmp_pos, &need_minor_version_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_bool(buf, data_len, tmp_pos, &need_data_store_type_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&modify_time_cid_));
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&major_version_cid_));
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&minor_ver_start_cid_));
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&minor_ver_end_cid_));
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_i64(buf, data_len, tmp_pos, reinterpret_cast<int64_t*>(&data_store_type_cid_));
      }
      if (OB_SUCCESS == ret)
      {
        pos = tmp_pos;
      }
      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObDataMarkParam)
    {
      int64_t total_size = 0;
      total_size += serialization::encoded_length_i64(table_id_);
      total_size += serialization::encoded_length_bool(need_modify_time_);
      total_size += serialization::encoded_length_bool(need_major_version_);
      total_size += serialization::encoded_length_bool(need_minor_version_);
      total_size += serialization::encoded_length_bool(need_data_store_type_);
      total_size += serialization::encoded_length_i64(modify_time_cid_);
      total_size += serialization::encoded_length_i64(major_version_cid_);
      total_size += serialization::encoded_length_i64(minor_ver_start_cid_);
      total_size += serialization::encoded_length_i64(minor_ver_end_cid_);
      total_size += serialization::encoded_length_i64(data_store_type_cid_);
      return total_size;
    }

    int ObDataMarkHelper::get_data_mark(const ObRow *row, const ObDataMarkParam &param, ObDataMark &out_data_mark)
    {
      int ret = OB_SUCCESS;
      uint64_t table_id  = param.table_id_;
      const ObObj *cell  = NULL;
      int64_t  cell_idx  = OB_INVALID_INDEX;
      uint64_t tid       = OB_INVALID_ID;
      uint64_t cid       = OB_INVALID_ID;
      const ObRowDesc* row_desc = NULL;
      (void)tid; // add for [build warning]
      (void)cid; // add for [build warning]
      out_data_mark.reset();
      if (NULL == row
          || !param.is_valid()
          || NULL == (row_desc = row->get_row_desc()))
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(ERROR,"invalid argument!row=%p,row_desc=%p,param=[%s],ret=%d",
                  row,row_desc,to_cstring(param),ret);
      }
      else
      {
        if (OB_SUCCESS == ret
            && param.need_modify_time_)
        {
          cell     = NULL;
          cell_idx = OB_INVALID_INDEX;
          tid      = OB_INVALID_ID;
          cid      = OB_INVALID_ID;
          if (OB_INVALID_INDEX == (cell_idx = row_desc->get_idx(param.table_id_,
                                                                param.modify_time_cid_)))
          {
            ret = OB_ERR_UNEXPECTED;
            YYSYS_LOG(ERROR,"row didn't cont mtime cid!row=[%s],ret=%d",
                      to_cstring(*row),ret);
          }
          else if (OB_SUCCESS != (ret = row->raw_get_cell(cell_idx,cell)))
          {
            YYSYS_LOG(WARN,"fail to get mtime obj!table_id=%ld,ret=%d",
                      table_id,ret);
          }
          else if (ObNullType == cell->get_type()
                   || (ObExtendType == cell->get_type()
                       && ObActionFlag::OP_NOP == cell->get_ext()))
          {
            YYSYS_LOG(DEBUG,"mul_del::debug,cur row has no mtime!cell=[%s],row=[%s]",
                      to_cstring(*cell),to_cstring(*row));
          }
          else if (ObIntType != cell->get_type())
          {
            ret =OB_ERR_UNEXPECTED;
            YYSYS_LOG(ERROR,"invalid mtime cell[%s],ret=%d",to_cstring(*cell),ret);
          }
          else if (OB_SUCCESS != (ret = cell->get_int(out_data_mark.modify_time_)))
          {
            YYSYS_LOG(WARN,"fail to get mtime from cell[%s],ret=%d",to_cstring(*cell),ret);
          }
        }

        if (OB_SUCCESS == ret
            && param.need_major_version_)
        {
          cell     = NULL;
          cell_idx = OB_INVALID_INDEX;
          tid      = OB_INVALID_ID;
          cid      = OB_INVALID_ID;
          if (OB_INVALID_INDEX == (cell_idx = row_desc->get_idx(param.table_id_,
                                                                param.major_version_cid_)))
          {
            ret = OB_ERR_UNEXPECTED;
            YYSYS_LOG(ERROR,"row didn't cont major ver cid!row=[%s],ret=%d",
                      to_cstring(*row),ret);
          }
          else if (OB_SUCCESS != (ret = row->raw_get_cell(cell_idx,cell)))
          {
            YYSYS_LOG(WARN,"fail to get major ver obj!table_id=%ld,ret=%d",
                      table_id,ret);
          }
          else if (ObNullType == cell->get_type()
                   || (ObExtendType == cell->get_type()
                       && ObActionFlag::OP_NOP == cell->get_ext()))
          {
            YYSYS_LOG(INFO,"mul_del::debug,cur row has no major ver!cell=[%s],row=[%s]",
                      to_cstring(*cell),to_cstring(*row));
          }
          else if (ObIntType != cell->get_type())
          {
            ret =OB_ERR_UNEXPECTED;
            YYSYS_LOG(ERROR,"invalid major ver cell[%s],ret=%d",to_cstring(*cell),ret);
          }
          else if (OB_SUCCESS != (ret = cell->get_int(out_data_mark.major_version_)))
          {
            YYSYS_LOG(WARN,"fail to get major ver from cell[%s],ret=%d",to_cstring(*cell),ret);
          }
        }

        if (OB_SUCCESS == ret
            && param.need_minor_version_)
        {
          cell     = NULL;
          cell_idx = OB_INVALID_INDEX;
          tid      = OB_INVALID_ID;
          cid      = OB_INVALID_ID;
          if (OB_INVALID_INDEX == (cell_idx = row_desc->get_idx(param.table_id_,
                                                                param.minor_ver_start_cid_)))
          {
            ret = OB_ERR_UNEXPECTED;
            YYSYS_LOG(ERROR,"row didn't cont minor ver start cid!row=[%s],ret=%d",
                      to_cstring(*row),ret);
          }
          else if (OB_SUCCESS != (ret = row->raw_get_cell(cell_idx,cell)))
          {
            YYSYS_LOG(WARN,"fail to get minor ver start obj!table_id=%ld,ret=%d",
                      table_id,ret);
          }
          else if (ObNullType == cell->get_type()
                   || (ObExtendType == cell->get_type()
                       && ObActionFlag::OP_NOP == cell->get_ext()))
          {
            YYSYS_LOG(INFO,"mul_del::debug,cur row has no minor ver start!cell=[%s],row=[%s]",
                      to_cstring(*cell),to_cstring(*row));
          }
          else if (ObIntType != cell->get_type())
          {
            ret =OB_ERR_UNEXPECTED;
            YYSYS_LOG(ERROR,"invalid minor ver start cell[%s],ret=%d",to_cstring(*cell),ret);
          }
          else if (OB_SUCCESS != (ret = cell->get_int(out_data_mark.minor_ver_start_)))
          {
            YYSYS_LOG(WARN,"fail to get minor ver start from cell[%s],ret=%d",to_cstring(*cell),ret);
          }
        }

        if (OB_SUCCESS == ret
            && param.need_minor_version_)
        {
          cell     = NULL;
          cell_idx = OB_INVALID_INDEX;
          tid      = OB_INVALID_ID;
          cid      = OB_INVALID_ID;
          if (OB_INVALID_INDEX == (cell_idx = row_desc->get_idx(param.table_id_,
                                                                param.minor_ver_end_cid_)))
          {
            ret = OB_ERR_UNEXPECTED;
            YYSYS_LOG(ERROR,"row didn't cont minor ver end cid!row=[%s],ret=%d",
                      to_cstring(*row),ret);
          }
          else if (OB_SUCCESS != (ret = row->raw_get_cell(cell_idx,cell)))
          {
            YYSYS_LOG(WARN,"fail to get minor ver end obj!table_id=%ld,ret=%d",
                      table_id,ret);
          }
          else if (ObNullType == cell->get_type()
                   || (ObExtendType == cell->get_type()
                       && ObActionFlag::OP_NOP == cell->get_ext()))
          {
            YYSYS_LOG(INFO,"mul_del::debug,cur row has no minor ver end!cell=[%s],row=[%s]",
                      to_cstring(*cell),to_cstring(*row));
          }
          else if (ObIntType != cell->get_type())
          {
            ret =OB_ERR_UNEXPECTED;
            YYSYS_LOG(ERROR,"invalid minor ver end cell[%s],ret=%d",to_cstring(*cell),ret);
          }
          else if (OB_SUCCESS != (ret = cell->get_int(out_data_mark.minor_ver_end_)))
          {
            YYSYS_LOG(WARN,"fail to get minor ver end  from cell[%s],ret=%d",to_cstring(*cell),ret);
          }
        }

        if (OB_SUCCESS == ret
            && param.need_data_store_type_)
        {
          int64_t tmp_type = INVALID_STORE_TYPE;
          cell     = NULL;
          cell_idx = OB_INVALID_INDEX;
          tid      = OB_INVALID_ID;
          cid      = OB_INVALID_ID;
          if (OB_INVALID_INDEX == (cell_idx = row_desc->get_idx(param.table_id_,
                                                                param.data_store_type_cid_)))
          {
            ret = OB_ERR_UNEXPECTED;
            YYSYS_LOG(ERROR,"row didn't cont data store type cid!row=[%s],ret=%d",
                      to_cstring(*row),ret);
          }
          else if (OB_SUCCESS != (ret = row->raw_get_cell(cell_idx,cell)))
          {
            YYSYS_LOG(WARN,"fail to get data store type obj!table_id=%ld,ret=%d",
                      table_id,ret);
          }
          else if (ObNullType == cell->get_type()
                   || (ObExtendType == cell->get_type()
                       && ObActionFlag::OP_NOP == cell->get_ext()))
          {
            YYSYS_LOG(INFO,"mul_del::debug,cur row has no data store type!cell=[%s],row=[%s]",
                      to_cstring(*cell),to_cstring(*row));
          }
          else if (ObIntType != cell->get_type())
          {
            ret =OB_ERR_UNEXPECTED;
            YYSYS_LOG(ERROR,"invalid data store type cell[%s],ret=%d",to_cstring(*cell),ret);
          }
          else if (OB_SUCCESS != (ret = cell->get_int(tmp_type)))
          {
            YYSYS_LOG(WARN,"fail to get data store type from cell[%s],ret=%d",to_cstring(*cell),ret);
          }
          else
          {
            out_data_mark.data_store_type_ = static_cast<ObDataMarkDataStoreType>(tmp_type);
          }
        }
      }

      if (NULL != row)
      {
        YYSYS_LOG(DEBUG,"mul_del::debug,finish get data mark!orig_row=[%s],out_data_mark=[%s],orig_param=[%s],ret=%d",
                  to_cstring(*row),to_cstring(out_data_mark),to_cstring(param),ret);
      }

      return ret;
    }

    int ObDataMarkHelper::convert_data_mark_row_to_normal_row(const ObDataMarkParam &param,
                                                              const ObRow *src_row,
                                                              ObRow *des_row)
    {
      int ret = OB_SUCCESS;
      const ObRowDesc *src_row_desc = NULL;
      ObRowDesc *des_row_desc = NULL;
      int64_t src_row_col_num = 0;
      const ObObj *cell   = NULL;
      uint64_t tid = OB_INVALID_ID;
      uint64_t cid = OB_INVALID_ID;
      if (NULL == src_row
          || NULL == (src_row_desc = src_row->get_row_desc())
          || NULL == des_row
          || NULL == (des_row_desc = const_cast<ObRowDesc *>(des_row->get_row_desc()))
          || !param.is_valid())
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(ERROR,"invalid argum!src_row=%p,src_row_desc=%p,des_row=%p,des_row_desc=%p,"
                  "param_is_valid=%d,param=[%s],ret=%d",src_row,src_row_desc,
                  des_row,des_row_desc,param.is_valid(),to_cstring(param),ret);
      }
      else
      {
        des_row->clear();
        des_row_desc->reset();
        src_row_col_num = src_row_desc->get_column_num();
        YYSYS_LOG(DEBUG,"mul_del::debug,begin convert!orig_row=[%s]",to_cstring(*src_row));
      }

      //1:cons row desc
      if (OB_SUCCESS == ret)
      {
        for (int64_t idx=0;OB_SUCCESS == ret && idx<src_row_col_num;idx++)
        {
          tid = OB_INVALID_ID;
          cid = OB_INVALID_ID;
          if (OB_SUCCESS != (ret = src_row_desc->get_tid_cid(idx,tid,cid)))
          {
            YYSYS_LOG(WARN,"fail to get %ldth tid[%ld] cid[%ld]!ret=%d",
                      idx,tid,cid,ret);
          }
          else if (!param.is_data_mark_cid(cid)
                   && OB_SUCCESS != (ret = des_row_desc->add_column_desc(tid,cid)))
          {
            YYSYS_LOG(WARN,"fail to add %ldth tid[%ld] cid[%ld]!ret=%d",
                      idx,tid,cid,ret);
          }
        }

        if (OB_SUCCESS == ret)
        {
          des_row->set_row_desc(*des_row_desc);
        }
      }

      //2:set row cell
      if (OB_SUCCESS == ret)
      {
        for (int64_t col_idx=0;OB_SUCCESS == ret && col_idx < src_row_col_num;col_idx++)
        {
          cell = NULL;
          tid  = OB_INVALID_ID;
          cid  = OB_INVALID_ID;
          if (OB_SUCCESS != (ret = src_row->raw_get_cell(col_idx,cell,tid,cid)))
          {
            YYSYS_LOG(WARN,"fail to get %ldth cell[%p] tid[%ld] cid[%ld],ret=%d",
                      col_idx,cell,tid,cid,ret);
          }
          else if (!param.is_data_mark_cid(cid)
                   && OB_SUCCESS != (ret = des_row->set_cell(tid,cid,*cell)))
          {
            YYSYS_LOG(WARN,"fail to set %ldth cell[%s] tid[%ld] cid[%ld],ret=%d",
                      col_idx,to_cstring(*cell),tid,cid,ret);
          }
        }
      }

      YYSYS_LOG(DEBUG,"mul_del::debug,finish convert!final_row=%p,ret=%d",des_row,ret);
      if (NULL != des_row)
        YYSYS_LOG(DEBUG,"mul_del::debug,final convert row=[%s]",to_cstring(*des_row));
      return ret;
    }

    //add duyr 20160531:e

    ObReadParam::ObReadParam()
    {
      reset();
    }

    void ObReadParam::reset()
    {
      is_read_master_ = 1;
      is_result_cached_ = 0;
      version_range_.start_version_ = 0;
      version_range_.end_version_ = 0;
      //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151202:b
      read_atomic_param_.reset();    //uncertainty �������л�
      //add duyr 20151202:e
      //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
      data_mark_param_.reset();
      //add duyr 20160531:e
      //add by maosy [MultiUPS 1.0] [read uncommit]20170531 b:
      trans_id_.clear();
      // add by maosy e

    }

    ObReadParam::~ObReadParam()
    {
    }

    void ObReadParam::set_is_read_consistency(const bool consistency)
    {
      is_read_master_ = consistency;
    }

    bool ObReadParam::get_is_read_consistency()const
    {
      return (is_read_master_ > 0);
    }
    const ObReadAtomicParam& ObReadParam::get_read_atomic_param() const
    {
      return read_atomic_param_;
    }

    void ObReadParam::set_read_atomic_param(const ObReadAtomicParam &param)
    {
      read_atomic_param_ = param;
    }
    //add duyr 20151202:e

    void ObReadParam::set_is_result_cached(const bool cached)
    {
      is_result_cached_ = cached;
    }

    bool ObReadParam::get_is_result_cached()const
    {
      return (is_result_cached_ > 0);
    }

    void ObReadParam::set_version_range(const ObVersionRange & range)
    {
      version_range_ = range;
    }

    ObVersionRange ObReadParam::get_version_range(void) const
    {
      return version_range_;
    }

    int ObReadParam::serialize_reserve_param(char * buf, const int64_t buf_len, int64_t & pos) const
    {
      ObObj obj;
      // serialize RESERVER PARAM FIELD
      obj.set_ext(ObActionFlag::RESERVE_PARAM_FIELD);
      int ret = obj.serialize(buf, buf_len, pos);
      if (ret == OB_SUCCESS)
      {
        obj.set_int(get_is_read_consistency());
        ret = obj.serialize(buf, buf_len, pos);
      }
      return ret;
    }

    int ObReadParam::deserialize_reserve_param(const char * buf, const int64_t data_len, int64_t & pos)
    {
      ObObj obj;
      int64_t int_value = 0;
      int ret = obj.deserialize(buf, data_len, pos);
      if (OB_SUCCESS == ret)
      {
        ret = obj.get_int(int_value);
        if (OB_SUCCESS == ret)
        {
          //is read master
          set_is_read_consistency(int_value);
        }
      }
      return ret;
    }

    int64_t ObReadParam::get_reserve_param_serialize_size(void) const
    {
      ObObj obj;
      // reserve for read master
      obj.set_ext(ObActionFlag::RESERVE_PARAM_FIELD);
      int64_t total_size = obj.get_serialize_size();
      obj.set_int(get_is_read_consistency());
      total_size += obj.get_serialize_size();
      return total_size;
    }

    DEFINE_SERIALIZE(ObReadParam)
    {
      ObObj obj;
      // is cache
      obj.set_int(ObReadParam::get_is_result_cached());
      int ret = obj.serialize(buf, buf_len, pos);
      // scan version range
      if (ret == OB_SUCCESS)
      {
        ObVersionRange version_range = ObReadParam::get_version_range();;
        obj.set_int(version_range.border_flag_.get_data());
        ret = obj.serialize(buf, buf_len, pos);
        if (ret == OB_SUCCESS)
        {
          obj.set_int(version_range.start_version_);
          ret = obj.serialize(buf, buf_len, pos);
        }

        if (ret == OB_SUCCESS)
        {
          obj.set_int(version_range.end_version_);
          ret = obj.serialize(buf, buf_len, pos);
        }
      }
      // uncertainty  ���л�����
      //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151202:b
      if (OB_SUCCESS == ret)
      {
        ret = read_atomic_param_.serialize(buf,buf_len,pos);
      }
      //add duyr 20151202:e
      //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
      if (OB_SUCCESS == ret)
      {
        ret = data_mark_param_.serialize(buf,buf_len,pos);
      }
      //add duyr  20160531:e
      //add by maosy [MultiUPS 1.0] [read uncommit]20170525 b:
      //trans_id
      if (OB_SUCCESS == ret)
      {
        int64_t count = trans_id_.count() ;
        ret = serialization::encode_i64(buf,buf_len,pos,count);
        YYSYS_LOG(DEBUG,"ObGetParam::serialize transaction id:%s",to_cstring(trans_id_));
        for(int64_t i =0 ; i < count &&OB_SUCCESS==ret ; i++)
        {
          ret = trans_id_.at(i).serialize(buf,buf_len,pos);
        }
      }
      //add by maosy e
      return ret;
    }

    DEFINE_DESERIALIZE(ObReadParam)
    {
      ObObj obj;
      int ret = OB_SUCCESS;
      int64_t int_value = 0;
      ret = obj.deserialize(buf, data_len, pos);
      if (OB_SUCCESS == ret)
      {
        ret = obj.get_int(int_value);
        if (OB_SUCCESS == ret)
        {
          //is cached
          set_is_result_cached(int_value);
        }
      }

      // version range
      if (OB_SUCCESS == ret)
      {
        // border flag
        ret = obj.deserialize(buf, data_len, pos);
        if (OB_SUCCESS == ret)
        {
          ret = obj.get_int(int_value);
          if (OB_SUCCESS == ret)
          {
            version_range_.border_flag_.set_data(static_cast<int8_t>(int_value));
          }
        }
      }

      // start version
      if (OB_SUCCESS == ret)
      {
        ret = obj.deserialize(buf, data_len, pos);
        if (OB_SUCCESS == ret)
        {
          ret = obj.get_int(int_value);
          if (OB_SUCCESS == ret)
          {
            version_range_.start_version_ = int_value;
          }
        }
      }

      // end version
      if (OB_SUCCESS == ret)
      {
        ret = obj.deserialize(buf, data_len, pos);
        if (OB_SUCCESS == ret)
        {
          ret = obj.get_int(int_value);
          if (OB_SUCCESS == ret)
          {
            version_range_.end_version_ = int_value;
          }
        }
      }
      //uncertainty   �Ϸ����л������ڴ˷����л�����
      //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151202:b
      if (OB_SUCCESS == ret)
      {
        ret = read_atomic_param_.deserialize(buf,data_len,pos);
      }
      //add duyr 20151202:e

      //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
      if (OB_SUCCESS == ret)
      {
        ret = data_mark_param_.deserialize(buf,data_len,pos);
      }
      //add duyr 20160531:e
      //add by maosy [MultiUPS 1.0] [read uncommit]20170525 b:
      // trans_id
      if (OB_SUCCESS == ret)
      {
        int64_t count = 0 ;
        ret = serialization::decode_i64(buf,data_len,pos,&count);
        for(int64_t i =0 ; i < count && OB_SUCCESS ==ret ; i++)
        {
          ObPartitionTransID par_transid;
          ret = par_transid.deserialize(buf,data_len,pos);
          if(OB_SUCCESS !=ret )
          {
            YYSYS_LOG(WARN,"failed to serialize transid ,ret = %d",ret);
          }
          else
          {
            ret = trans_id_.push_back(par_transid);
          }
        }
        YYSYS_LOG(DEBUG,"ObGetParam::serialize transaction id:%s",to_cstring(trans_id_));
        if (OB_SUCCESS == ret)
        {
          //add huangcc [fix transaction read uncommit bug]2016/11/21
          //                        if (trans_id_.is_valid())
          if (trans_id_.count()>0)
          {
            is_read_master_ = true;
          }
          //add end
        }
      }
      //add by maosy e
      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObReadParam)
    {
      ObObj obj;
      // is cache
      obj.set_int(get_is_result_cached());
      int64_t total_size = obj.get_serialize_size();

      // scan version range
      obj.set_int(version_range_.border_flag_.get_data());
      total_size += obj.get_serialize_size();
      obj.set_int(version_range_.start_version_);
      total_size += obj.get_serialize_size();
      obj.set_int(version_range_.end_version_);
      total_size += obj.get_serialize_size();
      //uncertainty ������Ҫѡ��
      //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151202:b
      total_size += read_atomic_param_.get_serialize_size();
      //add duyr 20151202:e
      //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
      total_size += data_mark_param_.get_serialize_size();
      //add duyr 20160531:e
      //add by maosy [MultiUPS 1.0] [read uncommit]20170525 b:
      {
        int64_t count = trans_id_.count();
        total_size =+ serialization::encoded_length_i64(count);
        for(int64_t i =0 ; i < count ;i++)
        {
          total_size += trans_id_.at(i).get_serialize_size();
        }
      }
      //add by maosy e
      return total_size;
    }


    //-------------------------------------------------------------------------------
    int set_ext_obj_value(char * buf, const int64_t buf_len, int64_t & pos, const int64_t value)
    {
      int ret = OB_SUCCESS;
      ObObj obj;
      obj.set_ext(value);
      ret = obj.serialize(buf, buf_len, pos);
      return ret;
    }

    int get_ext_obj_value(char * buf, const int64_t buf_len, int64_t & pos, int64_t& value)
    {
      int ret = OB_SUCCESS;
      ObObj obj;
      if ( OB_SUCCESS == (ret = obj.deserialize(buf, buf_len, pos))
           && ObExtendType == obj.get_type())
      {
        ret = obj.get_ext(value);
      }
      return ret;
    }

    int set_int_obj_value(char * buf, const int64_t buf_len, int64_t & pos, const int64_t value)
    {
      int ret = OB_SUCCESS;
      ObObj obj;
      obj.set_int(value);
      ret = obj.serialize(buf, buf_len, pos);
      return ret;
    }

    int get_int_obj_value(const char* buf, const int64_t buf_len, int64_t & pos, int64_t & int_value)
    {
      int ret = OB_SUCCESS;
      ObObj obj;
      if ( OB_SUCCESS == (ret = obj.deserialize(buf, buf_len, pos))
           && ObIntType == obj.get_type())
      {
        ret = obj.get_int(int_value);
      }
      return ret;
    }

    int set_str_obj_value(char * buf, const int64_t buf_len, int64_t & pos, const ObString &value)
    {
      int ret = OB_SUCCESS;
      ObObj obj;
      obj.set_varchar(value);
      ret = obj.serialize(buf, buf_len, pos);
      return ret;
    }

    int get_str_obj_value(const char* buf, const int64_t buf_len, int64_t & pos, ObString & str_value)
    {
      int ret = OB_SUCCESS;
      ObObj obj;
      if ( OB_SUCCESS == (ret = obj.deserialize(buf, buf_len, pos))
           && ObVarcharType == obj.get_type())
      {
        ret = obj.get_varchar(str_value);
      }
      return ret;
    }


    int set_rowkey_obj_array(char* buf, const int64_t buf_len, int64_t & pos, const ObObj* array, const int64_t size)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS == (ret = set_int_obj_value(buf, buf_len, pos, size)))
      {
        for (int64_t i = 0; i < size && OB_SUCCESS == ret; ++i)
        {
          ret = array[i].serialize(buf, buf_len, pos);
        }
      }

      return ret;
    }

    int get_rowkey_obj_array(const char* buf, const int64_t buf_len, int64_t & pos, ObObj* array, int64_t& size)
    {
      int ret = OB_SUCCESS;
      size = 0;
      if (OB_SUCCESS == (ret = get_int_obj_value(buf, buf_len, pos, size)))
      {
        for (int64_t i = 0; i < size && OB_SUCCESS == ret; ++i)
        {
          ret = array[i].deserialize(buf, buf_len, pos);
        }
      }

      return ret;
    }

    int64_t get_rowkey_obj_array_size(const ObObj* array, const int64_t size)
    {
      int64_t total_size = 0;
      ObObj obj;
      obj.set_int(size);
      total_size += obj.get_serialize_size();

      for (int64_t i = 0; i < size; ++i)
      {
        total_size += array[i].get_serialize_size();
      }
      return total_size;
    }

    int get_rowkey_compatible(const char* buf, const int64_t buf_len, int64_t & pos,
                              const ObRowkeyInfo& info, ObObj* array, int64_t& size, bool& is_binary_rowkey)
    {
      int ret = OB_SUCCESS;
      ObObj obj;
      int64_t obj_count = 0;
      ObString str_value;

      is_binary_rowkey = false;
      if ( OB_SUCCESS == (ret = obj.deserialize(buf, buf_len, pos)) )
      {
        if (ObIntType == obj.get_type() && (OB_SUCCESS == (ret = obj.get_int(obj_count))))
        {
          // new rowkey format.
          for (int64_t i = 0; i < obj_count && OB_SUCCESS == ret; ++i)
          {
            if (i >= size)
            {
              ret = OB_SIZE_OVERFLOW;
            }
            else
            {
              ret = array[i].deserialize(buf, buf_len, pos);
            }
          }

          if (OB_SUCCESS == ret) size = obj_count;
        }
        else if (ObVarcharType == obj.get_type() && OB_SUCCESS == (ret = obj.get_varchar(str_value)))
        {
          is_binary_rowkey = true;
          // old fashion , binary rowkey stream
          if (size < info.get_size())
          {
            YYSYS_LOG(WARN, "input size=%ld not enough, need rowkey obj size=%ld", size, info.get_size());
            ret = OB_SIZE_OVERFLOW;
          }
          else if (str_value.length() == 0)
          {
            // allow empty binary rowkey , incase min, max range.
            size = 0;
          }
          else if (str_value.length() < info.get_binary_rowkey_length())
          {
            YYSYS_LOG(WARN, "binary rowkey length=%d < need rowkey length=%ld",
                      str_value.length(), info.get_binary_rowkey_length());
            ret = OB_SIZE_OVERFLOW;
          }
          else
          {
            size = info.get_size();
            ret = ObRowkeyHelper::binary_rowkey_to_obj_array(info, str_value, array, size);
          }
        }
      }

      return ret;
    }

    int get_rowkey_info_from_sm(const ObSchemaManagerV2* schema_mgr,
                                const uint64_t table_id, const ObString& table_name, ObRowkeyInfo& rowkey_info)
    {
      int ret = OB_SUCCESS;
      const ObTableSchema* tbl = NULL;
      if (NULL == schema_mgr)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if (table_id > 0 && table_id != OB_INVALID_ID)
      {
        tbl = schema_mgr->get_table_schema(table_id);
      }
      else if (NULL != table_name.ptr())
      {
        tbl = schema_mgr->get_table_schema(table_name);
      }

      if (NULL == tbl)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        rowkey_info = tbl->get_rowkey_info();
      }
      return ret;
    }

  } /* common */
} /* oceanbase */
