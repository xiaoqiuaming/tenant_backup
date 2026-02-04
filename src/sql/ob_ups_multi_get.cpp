/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_multi_get.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "ob_ups_multi_get.h"
#include "common/utility.h"
#include "common/ob_trace_log.h"

using namespace oceanbase;
using namespace sql;

//add shili [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20150806:b
  #define ALLOC_NEW(allocator,type_name,ptr)  \
  ({ \
  ptr=NULL;\
  ptr = (type_name*)allocator.alloc(sizeof(type_name));  \
  if(NULL==ptr)                             \
  {                                         \
    YYSYS_LOG(WARN, "alloc mem fail NULL"); \
    ret = OB_ALLOCATE_MEMORY_FAILED;        \
  }                                         \
  else                                      \
  {                                         \
    ptr = new(ptr) type_name();  \
    if(!ptr)  \
    { \
      ret = OB_ERROR; \
      YYSYS_LOG(WARN, "new error"); \
    } \
  }\
  ret;})
//add 20150806::e
ObUpsMultiGet::ObUpsMultiGet()
  //:get_param_(NULL),//del lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160104
: rpc_proxy_(NULL),
  got_row_count_(0),
  row_desc_(NULL),
  ts_timeout_us_(0)
{
  //add lijianqiang [MultiUPS] [SELECT_MERGE] 20160104:b
  paxos_id_ = OB_INVALID_PAXOS_ID;
  cur_rowkey_ = NULL;
  cur_row_version_ = OB_INVALID_VERSION;
  has_row_version_ = false;
  cur_row_version_index_ = 0;
  get_param_.reset(true);//need deep copy
  //add 20160104:e
}

ObUpsMultiGet::~ObUpsMultiGet()
{
}

void ObUpsMultiGet::reset()
{
  //mod lijianqiang [MultiUPS] [SELECT_MERGE] 20160105:b
  //get_param_ = NULL;
  get_param_.reset(true);
  paxos_id_ = OB_INVALID_PAXOS_ID;
  cur_rowkey_ = NULL;
  has_row_version_ = false;
  cur_row_version_ = OB_INVALID_VERSION;
  cur_row_version_index_ = 0;
  //mod 20160105:e
  cur_new_scanner_.clear();
  got_row_count_ = 0;
  row_desc_ = NULL;
}

void ObUpsMultiGet::reuse()
{
  //mod lijianqiang [MultiUPS] [SELECT_MERGE] 20160105:b
  //get_param_ = NULL;
  get_param_.reset(true);
  paxos_id_ = OB_INVALID_PAXOS_ID;
  cur_rowkey_ = NULL;
  has_row_version_ = false;
  cur_row_version_ = OB_INVALID_VERSION;
  cur_row_version_index_ = 0;
  //mod 20160105:e
  cur_new_scanner_.reuse();
  got_row_count_ = 0;
  row_desc_ = NULL;
}

bool ObUpsMultiGet::check_inner_stat()
{
  bool ret = false;
  //mod lijianqiang [MultiUPS] [SELECT_MERGE] 20160105:b
  //ret = NULL != get_param_ && NULL != rpc_proxy_ && NULL != row_desc_ && ts_timeout_us_ > 0;
  ret = NULL != rpc_proxy_ && NULL != row_desc_ && ts_timeout_us_ > 0;
  //mod 20160105:e
  if(!ret)
  {
    //YYSYS_LOG(WARN, "get_param_[%p], rpc_proxy_[%p], row_desc_[%p], ts_timeout_us_[%ld]",
     // get_param_, rpc_proxy_, row_desc_, ts_timeout_us_);
	 YYSYS_LOG(WARN, "rpc_proxy_[%p], row_desc_[%p], ts_timeout_us_[%ld]",
      rpc_proxy_, row_desc_, ts_timeout_us_);
  }
  return ret;
}

int ObUpsMultiGet::set_child(int32_t child_idx, ObPhyOperator &child_operator)
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
    REGISTER_PHY_OPERATOR(ObUpsMultiGet, PHY_UPS_MULTI_GET);
  }
}

int64_t ObUpsMultiGet::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "UpsMultiGet()\n");
  //add lijianqiang [MultiUPS] [SELECT_MERGE] 20160503:b
  databuff_printf(buf, buf_len, pos, "paxos id=%ld\n",paxos_id_);
  pos += get_param_.to_string(pos+buf, buf_len - pos);
  //add 20160503:e
  return pos;
}

int ObUpsMultiGet::open()
{
  int ret = OB_SUCCESS;
  bool is_fullfilled = false;
  int64_t fullfilled_row_num = 0;

  got_row_count_ = 0;

  FILL_TRACE_LOG("begin open ups multi get.");

  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "check inner stat fail");
  }
  else if(OB_SUCCESS != (ret = next_get_param()))
  {
    YYSYS_LOG(WARN, "construct next get param fail:ret[%d]", ret);
  }

  if (OB_SUCCESS == ret)
  {
    int64_t remain_us = 0;
    if (is_timeout(&remain_us))
    {
      YYSYS_LOG(WARN, "process ups scan timeout, remain_us[%ld]", remain_us);
      ret = OB_PROCESS_TIMEOUT;
    }
    //mod lijianqiang [MultiUPS] [SELECT_MERGE] 20160105:b
    //else if(OB_SUCCESS != (ret = rpc_proxy_->sql_ups_get(cur_get_param_, cur_new_scanner_, remain_us)))
    else if(OB_SUCCESS != (ret = rpc_proxy_->sql_ups_get(cur_get_param_, cur_new_scanner_, remain_us, paxos_id_)))
    //mod 20160105:e
    {
      YYSYS_LOG(WARN, "ups get rpc fail:ret[%d]", ret);
    }
    else if(OB_SUCCESS != (ret = cur_new_scanner_.get_is_req_fullfilled(is_fullfilled, fullfilled_row_num)))
    {
      YYSYS_LOG(WARN, "get is req fillfulled fail:ret[%d]", ret);
    }
    else
    {
      if(fullfilled_row_num > 0)
      {
        got_row_count_ += fullfilled_row_num;
      }
      else if(!is_fullfilled)
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(WARN, "get no item:ret[%d]", ret);
      }
      //add lijianqiang [MultiUPS] [SELECT_MERGE] 20160107:b
      cur_row_version_index_ = 0;  //remember reset the row_version_index for each rpc
      has_row_version_ = cur_new_scanner_.get_has_row_version();
      //add 20160107:e
    }
  }
  return ret;
}

int ObUpsMultiGet::close()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObUpsMultiGet::get_next_row(const ObRowkey *&rowkey, const ObRow *&row)
{
  int ret = OB_SUCCESS;
  bool is_fullfilled = false;
  int64_t fullfilled_row_num = 0;

  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "check inner stat fail");
  }

  while(OB_SUCCESS == ret)
  {
    ret = cur_new_scanner_.get_next_row(rowkey, cur_ups_row_);
    if(OB_ITER_END == ret)
    {
      if(OB_SUCCESS != (ret = cur_new_scanner_.get_is_req_fullfilled(is_fullfilled, fullfilled_row_num)))
      {
        YYSYS_LOG(WARN, "get is fullfilled fail:ret[%d]", ret);
      }
      else if(is_fullfilled)
      {
        ret = OB_ITER_END;
      }
      else
      {
        if(OB_SUCCESS != (ret = next_get_param()))
        {
          YYSYS_LOG(WARN, "construct next get param fail:ret[%d]", ret);
        }
        //mod lijianqiang [MultiUPS] [SELECT_MERGE] 20160105:b
        //else if(OB_SUCCESS != (ret = rpc_proxy_->sql_ups_get(cur_get_param_, cur_new_scanner_, ts_timeout_us_)))
        else if(OB_SUCCESS != (ret = rpc_proxy_->sql_ups_get(cur_get_param_, cur_new_scanner_, ts_timeout_us_, paxos_id_)))
        //mod 20160105:e
        {
          YYSYS_LOG(WARN, "ups get rpc fail:ret[%d]", ret);
        }
        else if(OB_SUCCESS != (ret = cur_new_scanner_.get_is_req_fullfilled(is_fullfilled, fullfilled_row_num)))
        {
          YYSYS_LOG(WARN, "get is req fillfulled fail:ret[%d]", ret);
        }
        else
        {
          if(fullfilled_row_num > 0)
          {
            got_row_count_ += fullfilled_row_num;
          }
          else if(!is_fullfilled)
          {
            ret = OB_ERR_UNEXPECTED;
            YYSYS_LOG(WARN, "get no item:ret[%d]", ret);
          }
          //add lijianqiang [MultiUPS] [SELECT_MERGE] 20160107:b
          cur_row_version_index_ = 0;  //remember reset the row_version_index for each rpc
          has_row_version_ = cur_new_scanner_.get_has_row_version();
          //add 20160107:e
        }
      }
    }
    else if(OB_SUCCESS == ret)
    {
      break;
    }
    else
    {
      YYSYS_LOG(WARN, "get next row fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    row = &cur_ups_row_;
    YYSYS_LOG(DEBUG, "cur_row is nop row=%s,is delete=%s, row=%s",cur_ups_row_.get_is_all_nop()?"true":"false",
              cur_ups_row_.get_is_delete_row()?"true":"false",to_cstring(*row));
  }
  return ret;
}


/*
 * 如果一次rpc调用不能返回所以结果数据，通过next_get_param构造下次请求的
 * get_param
 * 这个函数隐含一个假设，ObNewScanner返回的数据必然在get_param的前面，且
 * 个数一样，这个在服务器端保证
 */
int ObUpsMultiGet::next_get_param()
{
  int ret = OB_SUCCESS;
  cur_get_param_.reset();
  /**
   * @note lijianqiang
   * change the get_param_ from point to entity, use "." instend of "->" for get_param_
   */
  //cur_get_param_.set_version_range(get_param_->get_version_range()); //uncertainty ups 中修改了类型
  //cur_get_param_.set_is_read_consistency(get_param_->get_is_read_consistency());
  cur_get_param_.set_version_range(get_param_.get_version_range());
  cur_get_param_.set_is_read_consistency(get_param_.get_is_read_consistency());
  //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151202:b
  cur_get_param_.set_read_atomic_param(get_param_.get_read_atomic_param());
  //add duyr 20151202:e
  // add by maosy [MultiUps 1.0] [batch_udi] 20170420 b
  cur_get_param_.set_data_mark_param(get_param_.get_data_mark_param());
  // add by maosy 20170420 e
  //add by maosy [MultiUPS 1.0] [read uncommit]20170531 b:
  cur_get_param_.set_trans_id(get_param_.get_trans_id());
  // add by e
  ObGetParam::ObRowIndex row_idx;
  if(got_row_count_ >= get_param_.get_row_size())//uncertainty ups 中修改了类型
 // if(got_row_count_ >= get_param_->get_row_size())
  {
    ret = OB_SIZE_OVERFLOW;
    YYSYS_LOG(WARN, "get row count size overflow:[%ld]", got_row_count_);
  }
  else
  {
   // row_idx = get_param_->get_row_index()[got_row_count_]; //uncertainty ups 中修改了类型
     row_idx = get_param_.get_row_index()[got_row_count_];
  }

  if(OB_SUCCESS == ret)
  {
   for(int64_t i=row_idx.offset_;OB_SUCCESS == ret && i<get_param_.get_cell_size();i++) 
   // for(int64_t i=row_idx.offset_;OB_SUCCESS == ret && i<get_param_->get_cell_size();i++) //uncertainty ups 中修改了类型
    {
     // if(OB_SUCCESS != (ret = cur_get_param_.add_cell((*(*get_param_)[i])))) //uncertainty ups 中修改了类型
	  if(OB_SUCCESS != (ret = cur_get_param_.add_cell((*get_param_[i]))))

      {
        YYSYS_LOG(WARN, "get param add cell fail:ret[%d]", ret);
      }
    }
  }
  return ret;
}
//add lijianqiang [MultiUPS] [SELECT_MERGE] 20160104:b
int ObUpsMultiGet::add_cell(const ObCellInfo& cell_info)
{
  int ret = OB_SUCCESS;
  ret = get_param_.add_cell(cell_info);
  return ret;
}
int ObUpsMultiGet::next_row()
{
  int ret = OB_SUCCESS;
  const ObRow * row = NULL;
  const ObRowkey * cur_rowkey = NULL;
  ret = this->get_next_row(cur_rowkey, row);
  YYSYS_LOG(DEBUG,"test::ret=[%d],row num is[%ld]",ret, cur_new_scanner_.get_row_num());
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
  }
  return ret;
}
int ObUpsMultiGet::get_row(const ObRowkey *&rowkey, const ObRow *&row, int64_t& row_version)
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
    YYSYS_LOG(DEBUG, "cur get upsrow is[%s],cur rowkey is[%s],cur version is[%ld]", to_cstring(cur_ups_row_), to_cstring(*cur_rowkey_),cur_row_version_);
  }
  return ret;
}
int ObUpsMultiGet:: get_row(const ObRowkey *&rowkey, const ObRow *&row)//not used now
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
  }
  return ret;
}

//add 20160104:e


//add shili, [MultiUPS] [COLUMN_VERSION_MERGE]  20170423:b
int ObUpsMultiGet::get_row(const ObRowkey *&rowkey, const ObRow *&row, int64_t& row_version,const ObRowVersion *&version)
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
    YYSYS_LOG(DEBUG, "get cur row is :%s,rowkey is %s,cur colmn versons:%s",
              to_cstring(*row),to_cstring(*rowkey),to_cstring(*version));
    YYSYS_LOG(DEBUG, "cur get upsrow is[%s],cur rowkey is[%s],cur version is[%ld]", to_cstring(cur_ups_row_), to_cstring(*cur_rowkey_),cur_row_version_);
  }
  return ret;
}
//add e


void ObUpsMultiGet::set_row_desc(const ObRowDesc &row_desc)
{
  row_desc_ = &row_desc;
  cur_ups_row_.set_row_desc(*row_desc_);
}

int ObUpsMultiGet::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  row_desc = row_desc_;
  return ret;
}
