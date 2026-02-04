
#include "ob_ups_scan.h"
#include "common/utility.h"
#include "common/ob_profile_log.h"

using namespace oceanbase;
using namespace sql;

void ObUpsScan::reset()
{
  cur_scan_param_.reset();
  row_desc_.reset();
}

void ObUpsScan::reuse()
{
  cur_scan_param_.reset();
  row_desc_.reset();
}

int ObUpsScan::open()
{
  int ret = OB_SUCCESS;
  row_counter_ = 0;
  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "check inner stat fail");
  }
  else
  {
    cur_scan_param_.set_is_read_consistency(is_read_consistency_);

    if(OB_SUCCESS != (ret = fetch_next(true)))
    {
      YYSYS_LOG(WARN, "fetch row fail:ret[%d]", ret);
    }
    else
    {
      cur_ups_row_.set_row_desc(row_desc_);
    }
  }
  return ret;
}

int ObUpsScan::get_next_scan_param(const ObRowkey &last_rowkey, ObScanParam &scan_param)
{
  int ret = OB_SUCCESS;

  ObNewRange next_range = *(scan_param.get_range());

  next_range.start_key_ = last_rowkey;
  next_range.border_flag_.unset_inclusive_start();

  if(OB_SUCCESS != (ret = scan_param.set_range(next_range)))
  {
    YYSYS_LOG(WARN, "scan param set range fail:ret[%d]", ret);
  }

  return ret;
}

int ObUpsScan::add_column(const uint64_t &column_id)
{
  int ret = OB_SUCCESS;
  if(OB_SUCCESS != (ret = cur_scan_param_.add_column(column_id)))
  {
    YYSYS_LOG(WARN, "add column id fail:ret[%d] column_id[%lu]", ret, column_id);
  }
  else if(OB_INVALID_ID == range_.table_id_)
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "set range first");
  }
  else if(OB_SUCCESS != (ret = row_desc_.add_column_desc(range_.table_id_, column_id)))
  {
    YYSYS_LOG(WARN, "add column desc fail:ret[%d]", ret);
  }
  return ret;
}

int ObUpsScan::close()
{
  int ret = OB_SUCCESS;
  YYSYS_LOG(DEBUG, "ups scan row count=%ld", row_counter_);
  return ret;
}

int ObUpsScan::get_next_row(const common::ObRowkey *&rowkey, const ObRow *&row)
{
  int ret = OB_SUCCESS;
  bool is_fullfilled = false;
  int64_t fullfilled_item_num = 0;

  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "check inner stat fail");
  }

  while(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = cur_new_scanner_.get_is_req_fullfilled(is_fullfilled, fullfilled_item_num)))
    {
      YYSYS_LOG(WARN, "get is req fullfilled fail:ret[%d]", ret);
    }
    else
    {
      ret = cur_new_scanner_.get_next_row(rowkey, cur_ups_row_);
      if(OB_ITER_END == ret )
      {
        YYSYS_LOG(DEBUG, "ups scanner is_fullfilled[%s]", is_fullfilled ? "TRUE" : "FALSE");
        if(is_fullfilled)
        {
          break;
        }
        else
        {
          if(OB_SUCCESS != (ret = fetch_next(false)))
          {
            YYSYS_LOG(WARN, "fetch row fail:ret[%d]", ret);
          }
        }
      }
      else if(OB_SUCCESS == ret)
      {
        ++row_counter_;
        break;
      }
      else
      {
        YYSYS_LOG(WARN, "get next row fail:ret[%d]", ret);
      }
    }
  }

  if(OB_SUCCESS == ret)
  {
    row = &cur_ups_row_;
    YYSYS_LOG(DEBUG, "ups scan row[%s]", to_cstring(cur_ups_row_));
  }
  return ret;
}

int ObUpsScan::fetch_next(bool first_scan)
{
  int ret = OB_SUCCESS;
  ObRowkey last_rowkey;
  INIT_PROFILE_LOG_TIMER();

  if(!first_scan)
  {
    if(OB_SUCCESS != (ret = cur_new_scanner_.get_last_row_key(last_rowkey)))
    {
      YYSYS_LOG(ERROR, "new scanner get rowkey fail:ret[%d]", ret);
    }
    else if(OB_SUCCESS != (ret = get_next_scan_param(last_rowkey, cur_scan_param_)))
    {
      YYSYS_LOG(WARN, "get scan param fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    int64_t remain_us = 0;
    if (is_timeout(&remain_us))
    {
      YYSYS_LOG(WARN, "process ups scan timeout, remain_us[%ld]", remain_us);
      ret = OB_PROCESS_TIMEOUT;
    }
    else
    {
      YYSYS_LOG(DEBUG, "remain ups scan time [%ld]us", remain_us);
      //mod lijianqiang [MultiUPS] [SELECT_MERGE] 20160105:b
      //if(OB_SUCCESS != (ret = rpc_proxy_->sql_ups_scan(cur_scan_param_, cur_new_scanner_, remain_us)))
      if(OB_SUCCESS != (ret = rpc_proxy_->sql_ups_scan(cur_scan_param_, cur_new_scanner_, remain_us, paxos_id_)))
      //mod 20160105:e
      {
        YYSYS_LOG(WARN, "scan ups fail:ret[%d]", ret);
      }
      //add lijianqiang [MultiUPS] [SELECT_MERGE] 20160107:b
      else
      {
        has_row_version_ = cur_new_scanner_.get_has_row_version();
        cur_row_version_index_ = 0;  //remember reset the row_version_index for each rpc
      }
      //add 20160107:e
    }
  }
  PROFILE_LOG_TIME(DEBUG, "ObUpsScan::fetch_next first_scan[%d] , range=%s",
      first_scan, to_cstring(*cur_scan_param_.get_range()));

  return ret;
}

int ObUpsScan::set_range(const ObNewRange &range)
{
  int ret = OB_SUCCESS;
  ObString table_name; //设置一个空的table name
  range_ = range;
  if(OB_SUCCESS != (ret = cur_scan_param_.set(range_.table_id_, table_name, range_)))
  {
    YYSYS_LOG(WARN, "scan_param set range fail:ret[%d]", ret);
  }
  return ret;
}

void ObUpsScan::set_version_range(const ObVersionRange &version_range)
{
  cur_scan_param_.set_version_range(version_range);
}

//add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
void ObUpsScan::set_data_mark_param(const ObDataMarkParam &param)
{
  cur_scan_param_.set_data_mark_param(param);
}
//add duyr 20160531:e
//add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151223:b
void ObUpsScan::set_read_atomic_param(const ObReadAtomicParam &read_atomic_param)
{
  cur_scan_param_.set_read_atomic_param(read_atomic_param);
}
//add duyr 20151223:e
//add lijianqiang [MultiUPS] [SELECT_MERGE] 20160107:b
int ObUpsScan::next_row()
{
  int ret = OB_SUCCESS;
  const ObRow * row = NULL;
  const ObRowkey * cur_rowkey = NULL;
  ret = this->get_next_row(cur_rowkey, row);
  YYSYS_LOG(DEBUG,"ret=[%d],row num is[%ld]",ret, cur_new_scanner_.get_row_num());
  if (OB_SUCCESS == ret)//get cur row version when get next row success
  {
    cur_rowkey_ = const_cast<ObRowkey *>(cur_rowkey);
    if (has_row_version_)
    {
      cur_row_version_ = cur_new_scanner_.get_cur_row_version(cur_row_version_index_);
      //add shili, [MultiUPS] [COLUMN_VERSION_MERGE]  20170423:b
      cur_row_version_v2_ = cur_new_scanner_.get_cur_row_version_v2(cur_row_version_index_);
      //add e
      cur_row_version_index_ ++;
    }
//    YYSYS_LOG(DEBUG,"cur row is[%s],version is[%ld]",to_cstring(*row),cur_row_version_);
  }
  return ret;
}

int ObUpsScan::get_row(const ObRowkey *&rowkey, const ObRow *&row, int64_t& row_version)
{
  int ret = OB_SUCCESS;
  if (NULL == cur_rowkey_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(WARN, "call next row befor getting cur row, ret=%d",ret);
  }
  else
  {
    row = &cur_ups_row_;
    rowkey = cur_rowkey_;
    row_version = cur_row_version_;
    YYSYS_LOG(DEBUG, "cur scan upsrow is[%s],cur rowkey is[%s],cur version is[%ld]", to_cstring(cur_ups_row_), to_cstring(*cur_rowkey_), cur_row_version_);
  }
  return ret;
}

int ObUpsScan:: get_row(const ObRowkey *&rowkey, const ObRow *&row)
{
  UNUSED(rowkey);
  UNUSED(row);
  YYSYS_LOG(ERROR, "not implement");
  return OB_NOT_IMPLEMENT;
}
//add 20160107:e

//add shili, [MultiUPS] [COLUMN_VERSION_MERGE]  20170423:b
int ObUpsScan::get_row(const ObRowkey *&rowkey, const ObRow *&row, int64_t& row_version,const ObRowVersion *&version)
{
  int ret = OB_SUCCESS;
  if (NULL == cur_rowkey_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(WARN, "call next row befor getting cur row, ret=%d",ret);
  }
  else
  {
    row = &cur_ups_row_;
    rowkey = cur_rowkey_;
    row_version = cur_row_version_;
    version = &cur_row_version_v2_;
    YYSYS_LOG(DEBUG, "scan cur row is :%s,rowkey is %s,cur colmn versons:%s",
              to_cstring(*row),to_cstring(*rowkey),to_cstring(*version));
    YYSYS_LOG(DEBUG, "cur scan upsrow is[%s],cur rowkey is[%s],cur version is[%ld]", to_cstring(cur_ups_row_), to_cstring(*cur_rowkey_), cur_row_version_);
  }
  return ret;
}
//add e


int ObUpsScan::set_child(int32_t child_idx, ObPhyOperator &child_operator)
{
  int ret = OB_NOT_IMPLEMENT;
  UNUSED(child_idx);
  UNUSED(child_operator);
  YYSYS_LOG(WARN, "not implement");
  return ret;
}

namespace oceanbase
{
  namespace sql
  {
    REGISTER_PHY_OPERATOR(ObUpsScan, PHY_UPS_SCAN);
  }
}

int64_t ObUpsScan::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "UpsScan()\n");
  //add lijianqiang [MultiUPS] [SELECT_MERGE] 20160503:b
  databuff_printf(buf, buf_len, pos, "paxos id=%ld\n",paxos_id_);
  pos += cur_scan_param_.to_string(pos+buf, buf_len - pos);
  //add 20160503:e
  return pos;
}

ObUpsScan::ObUpsScan()
  :rpc_proxy_(NULL),
   ts_timeout_us_(0),
   row_counter_(0),
   is_read_consistency_(true)
{
  //add lijianqiang [MultiUPS] [SELECT_MERGE] 20160104:b
  paxos_id_ = OB_INVALID_PAXOS_ID;
  cur_rowkey_ = NULL;
  has_row_version_ = false;
  cur_row_version_ = OB_INVALID_VERSION;
  cur_row_version_index_ = 0;
  //add 20160104:e
}

ObUpsScan::~ObUpsScan()
{
}

bool ObUpsScan::check_inner_stat()
{
  return NULL != rpc_proxy_ && ts_timeout_us_ > 0;
}

int ObUpsScan::set_ups_rpc_proxy(ObSqlUpsRpcProxy *rpc_proxy)
{
  int ret = OB_SUCCESS;
  if(NULL == rpc_proxy)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(WARN, "rpc_proxy is null");
  }
  else
  {
    rpc_proxy_ = rpc_proxy;
  }
  return ret;
}

int ObUpsScan::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  row_desc = &row_desc_;
  return ret;
}

bool ObUpsScan::is_result_empty() const
{
  int err = OB_SUCCESS;
  bool is_fullfilled = false;
  int64_t fullfilled_row_num = 0;
  if (OB_SUCCESS != (err = cur_new_scanner_.get_is_req_fullfilled(is_fullfilled, fullfilled_row_num) ))
  {
    YYSYS_LOG(WARN, "fail to get is fullfilled_item_num:err[%d]", err);
  }
  return (is_fullfilled && (fullfilled_row_num == 0));
}
