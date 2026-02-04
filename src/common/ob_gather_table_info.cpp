
#include "ob_gather_table_info.h"
#include <algorithm>
#include "yysys.h"
#include "utility.h"
#include "ob_tablet_info.h"

namespace oceanbase
{
  namespace common
  {
    void ObGatherTableInfo::dump()
    {
      YYSYS_LOG(INFO, "======dump gather table info======");
      YYSYS_LOG(INFO, "gather table info dump: table id = %lu", table_id_);

      for(int i = 0; i < columns_list_.get_array_index(); i++)
      {
        YYSYS_LOG(INFO, "gather table info dump: column id = %lu", columns_[i]);
      }
      YYSYS_LOG(INFO, "gather table info dump: replication_idx = %lu", replication_idx_);
      YYSYS_LOG(INFO, "============ end ===========");
    }

    DEFINE_SERIALIZE(ObGatherTableInfo)
    {
      int ret = OB_ERROR;
      int64_t size = columns_list_.get_array_index();
      if(OB_SUCCESS == (ret = serialization::encode_vi64(buf, buf_len, pos, table_id_)))
      {
        ret = serialization::encode_vi64(buf, buf_len, pos, size);
      }
      if(OB_SUCCESS == ret)
      {
        for(int64_t i = 0; i < size; i++)
        {
          ret = serialization::encode_vi64(buf, buf_len, pos, columns_[i]);
          if(OB_SUCCESS != ret)
            break;
        }
      }
      return ret;
    }

    DEFINE_DESERIALIZE(ObGatherTableInfo)
    {
      int ret = OB_ERROR;
      int64_t size = 0;
      if(OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, reinterpret_cast<int64_t *>(&table_id_))))
      {

      }
      else if(OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &size)))
      {

      }
      if(OB_SUCCESS == ret && size > 0)
      {
        for(int64_t i = 0; i < size; i++)
        {
          uint64_t tmp_col_id;
          if(OB_SUCCESS == (ret = serialization::decode_vi64(buf, data_len, pos, reinterpret_cast<int64_t *>(&tmp_col_id))))
          {
            columns_list_.push_back(tmp_col_id);
          }
          else
          {
            break;
          }
        }
      }
      return ret;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObGatherTableInfo)
    {
      int64_t total_size = 0;
      int64_t size = columns_list_.get_array_index();
      total_size += serialization::encoded_length_i64(table_id_);
      if(0 < size)
      {
        for(int64_t i = 0; i <= size; i++)
        {
          total_size += serialization::encoded_length_i64(columns_[i]);
        }
      }
      return total_size;
    }
  }
}
