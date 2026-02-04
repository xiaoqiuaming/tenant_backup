

#include "ob_gather_statistics.h"
#include "common/ob_privilege.h"
#include "common/utility.h"
#include "mergeserver/ob_rs_rpc_proxy.h"
#include "sql/ob_result_set.h"
#include "sql/ob_sql.h"
#include "common/ob_privilege_type.h"
#include "common/location/ob_tablet_location_range_iterator.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::rootserver;

class ObChunkServerManager;

ObGatherStatistics::ObGatherStatistics()
  : if_not_exists_(false)
{
  context_ = NULL;
}

ObGatherStatistics::~ObGatherStatistics()
{
}

void ObGatherStatistics::reset()
{
  if_not_exists_ = false;
  local_context_.rs_rpc_proxy_ = NULL;
  buf_pool_.reset();
}

void ObGatherStatistics::reuse()
{
  if_not_exists_ = false;
  local_context_.rs_rpc_proxy_ = NULL;
}

int ObGatherStatistics::close()
{
  int ret = OB_SUCCESS;
  return ret;
}

int64_t ObGatherStatistics::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "Gather Statistics(if_exists=%c tables=[", if_not_exists_ ?'Y':'N');
  databuff_printf(buf, buf_len, pos, "])\n");
  return pos;
}

void ObGatherStatistics::set_context(ObSqlContext *context)
{
  context_ = context;
}

int ObGatherStatistics::open()
{
  int ret = OB_SUCCESS;
  ObTabletLocationRangeIterator range_iter;
  ObNewRange range;
  range.table_id_ = gather_info_.table_id_;
  range.start_key_.set_min_row();
  range.end_key_.set_max_row();
  range.set_rowkey_info(&search_key_);
  int64_t pos = 0;  
  ObResultSet result;
  YYSYS_LOG(DEBUG, "Row_key info is %s", to_cstring(search_key_));
  if (gather_info_.columns_list_.get_array_index() <= 0)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR, "not init, rpc_=%p", local_context_.rs_rpc_proxy_);
  }
  else if (local_context_.session_info_->get_autocommit() == false || local_context_.session_info_->get_trans_id().is_valid())
  {
    YYSYS_LOG(WARN, "gather statistics is not allowed in transaction, err=%d", ret);
    ret = OB_ERR_TRANS_ALREADY_STARTED;
  }
  else if (OB_SUCCESS != (ret = range_iter.initialize(local_context_.cache_proxy_, &range, ScanFlag::FORWARD, &get_buf_pool())))
  {
    YYSYS_LOG(WARN,"fail to initialize range iterator [ret:%d]", ret);
  }

  if (OB_SUCCESS == ret)
  {
    for (int64_t i = 0; i < gather_info_.columns_list_.get_array_index() ; i++)
    {
      pos = 0;
      char insert_gather_buff[1024];
      ObString insert_gather;
      bool load = false;
      int64_t count = 0;
      databuff_printf(insert_gather_buff, 1024, pos, "INSERT INTO __all_udi_monitor_list VALUES(%ld,%ld,%d,%ld)", gather_info_.table_id_, gather_info_.columns_[i], load, count);
      if (pos >= 1023)
      {
        ret = OB_BUF_NOT_ENOUGH;
        YYSYS_LOG(WARN, "gather buff overflow, ret = %d", ret);
      }
      else
      {
        insert_gather.assign_ptr(insert_gather_buff, static_cast<ObString::obstr_size_t>(pos));
        if (OB_SUCCESS != (ret = result.init()))
        {
          YYSYS_LOG(WARN, "init result set failed,%d", ret);
        }
        else if (OB_SUCCESS != (ret = ObSql::direct_execute(insert_gather, result, *context_)))
        {
          YYSYS_LOG(WARN, "direct execute sql =%.*s failed, ret=%d", insert_gather.length(), insert_gather.ptr(), ret);
        }
        else if (OB_SUCCESS != (ret = result.open()))
        {
          YYSYS_LOG(WARN, "open result failed, ret=%d", ret);
        }
        else
        {
          OB_ASSERT(result.is_with_rows() == false);
          YYSYS_LOG(INFO, "execute %.*s success", insert_gather.length(), insert_gather.ptr());
          int tmp_err = result.close();
          if (OB_SUCCESS != tmp_err)
          {
          YYSYS_LOG(WARN, "failed to close result set, ret=%d", tmp_err);
          }
          result.reset();
        }
      }
    }
  }
  return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObGatherStatistics, PHY_GATHER_STATISTICS);
  }
}
