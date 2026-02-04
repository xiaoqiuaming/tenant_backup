/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_get_fuse.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#include "ob_tablet_get_fuse.h"
#include "common/ob_row_fuse.h"
//add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151223:b
#include "common/ob_common_param.h"
//add duyr 20151223:e

using namespace oceanbase;
using namespace common;
using namespace sql;

ObTabletGetFuse::ObTabletGetFuse()
  :sstable_get_(NULL),
  incremental_get_(NULL),
  data_version_(0),
  last_rowkey_(NULL)
  //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151223:b
  ,read_atomic_param_(),
  //add duyr 20151223:e
  get_param_(NULL),
  rowkey_index_(0)
{
}

void ObTabletGetFuse::reset()
{
  sstable_get_ = NULL;
  incremental_get_ = NULL;
  data_version_ = 0;
  last_rowkey_ = NULL;
  //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151223:b
  read_atomic_param_.reset();
  //add duyr 20151223:e
  get_param_ = NULL;
  rowkey_index_ = 0;
}

void ObTabletGetFuse::reuse()
{
  sstable_get_ = NULL;
  incremental_get_ = NULL;
  data_version_ = 0;
  last_rowkey_ = NULL;
  //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151223:b
  read_atomic_param_.reset();
  //add duyr 20151223:e
  get_param_ = NULL;
  rowkey_index_ = 0;
}

int ObTabletGetFuse::set_sstable_get(ObRowkeyPhyOperator *sstable_get)
{
  int ret = OB_SUCCESS;
  if (NULL == sstable_get)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(WARN, "sstable_get is null");
  }
  else
  {
    sstable_get_ = sstable_get;
  }
  return ret;
}

int ObTabletGetFuse::set_incremental_get(ObRowkeyPhyOperator *incremental_get)
{
  int ret = OB_SUCCESS;
  if (NULL == incremental_get)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(WARN, "incremental_get is null");
  }
  else
  {
    incremental_get_ = incremental_get;
  }
  return ret;
}

int ObTabletGetFuse::open()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = sstable_get_->open()))
  {
    YYSYS_LOG(WARN, "fail to open sstable get:ret[%d]", ret);
  }
  else if (OB_SUCCESS != (ret = incremental_get_->open()))
  {
    YYSYS_LOG(WARN, "fail to open incremental get:ret[%d]", ret);
  }
  FILL_TRACE_LOG("open get fuse op done ret =%d", ret);
  return ret;
}

/*@beirf sstable and ups_get both get one row(if there is nop row or delete row,it also return a nop or deleted row)
         and merge*/
int ObTabletGetFuse::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  const ObRow *sstable_row = NULL;
  const ObRowkey *sstable_rowkey = NULL;
  const ObRow *tmp_row = NULL;
  const ObUpsRow *incremental_row = NULL;
  const ObRowkey *incremental_rowkey = NULL;
  ret = sstable_get_->get_next_row(sstable_rowkey, sstable_row);
  //add duyr [MultiUPS] [READ_ATOMIC] [read_atomic::debug] 20151223:b
  if (NULL != sstable_row)
  {
    YYSYS_LOG(DEBUG,"read_atomic::debug,orig_get_sst_row=[%s],ret=%d",
              to_cstring(*sstable_row),ret);
  }
  //add duyr 20151223:e
  if (OB_SUCCESS != ret && OB_ITER_END != ret)
  {
    YYSYS_LOG(WARN, "fail to get sstable next row:ret[%d]", ret);
  }

  if (OB_SUCCESS == ret)
  {
    ObCellInfo *cell = NULL;
    ObGetParam::ObRowIndex row_index;
    ObRowkey get_param_rowkey;
    const ObRowkey *get_param_current_key = NULL;
    if(get_param_ != NULL && rowkey_index_ < get_param_->get_cell_size())
    {
      row_index = get_param_->get_row_index()[rowkey_index_];
      cell = (*get_param_)[row_index.offset_];
      YYSYS_LOG(DEBUG, "multi_get test: rowkey_index_=%ld, get_param_->get_cell_size()=%ld,row_index.offset=%d,row_index.size=%d",
                rowkey_index_, get_param_->get_cell_size(), row_index.offset_, row_index.size_);
      if (NULL == cell)
      {
        YYSYS_LOG(ERROR, "current cell in get param is NULL");
        ret = OB_ERROR;
      }
      else
      {
        get_param_rowkey = (cell->row_key_);
        get_param_current_key = &get_param_rowkey;
        YYSYS_LOG(DEBUG, "multi_get test: get_param_current_key=%s", to_cstring(*get_param_current_key));
      }
    }
    if (ret == OB_SUCCESS && get_param_current_key != NULL)
    {
      YYSYS_LOG(DEBUG, "get_param_ current rowkey=[%s]", to_cstring(*get_param_current_key));
      if(OB_SUCCESS != (ret = incremental_get_->set_sstable_rowkey(get_param_current_key)))
      {
        YYSYS_LOG(ERROR, "fail to set get_param current rowkey!");
      }
    }
    if(ret != OB_SUCCESS)
    {}
    else if (OB_SUCCESS != (ret = incremental_get_->get_next_row(incremental_rowkey, tmp_row)))
    {
      YYSYS_LOG(WARN, "fail to get increment next row:ret[%d]", ret);
      if (OB_ITER_END == ret)
      {
        ret = OB_ERROR;
      }
    }
     //add duyr [MultiUPS] [READ_ATOMIC] [read_atomic::debug] 20151223:b
    if (NULL != tmp_row)
    {
      YYSYS_LOG(DEBUG,"read_atomic::debug,orig_get_inc_row=[%s],ret=%d",
                to_cstring(*tmp_row),ret);
    }
    //add duyr 20151223:e
  }

  //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
//  if (NULL != sstable_row)
//      YYSYS_LOG(DEBUG,"mul_del::debug,orig_sstable_row=[%s],ret=%d",
//                to_cstring(*sstable_row),ret);
//  if (NULL != tmp_row)
//      YYSYS_LOG(DEBUG,"mul_del::debug,orig_inc_row=[%s],ret=%d",
//                to_cstring(*tmp_row),ret);
  //add duyr 20160531:e

  if (OB_SUCCESS == ret)
  {
    incremental_row = dynamic_cast<const ObUpsRow *>(tmp_row);
    if (NULL == incremental_row)
    {
      ret = OB_ERROR;
      YYSYS_LOG(WARN, "should be ups row");
    }
  }

  if (OB_SUCCESS == ret)
  {
    YYSYS_LOG(DEBUG, "tablet get fuse incr[%s] sstable row[%s]", to_cstring(*incremental_row), to_cstring(*sstable_row));
    //mod duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151223:b
//    if (OB_SUCCESS != (ret = ObRowFuse::fuse_row(incremental_row, sstable_row, &curr_row_)))
//    {
//      YYSYS_LOG(WARN, "fail to fuse row:ret[%d]", ret);
//    }
    if (read_atomic_param_.is_valid()
        && OB_SUCCESS != (ret = ObReadAtomicHelper::fuse_row(incremental_row,
                                                             sstable_row,
                                                             read_atomic_param_,
                                                             &curr_row_)))
    {
      YYSYS_LOG(WARN,"fail to fuse read atomic row!ret=%d",ret);
    }
    else if (!read_atomic_param_.is_valid()
             && OB_SUCCESS != (ret = ObRowFuse::fuse_row(incremental_row, sstable_row, &curr_row_)))
    {
      YYSYS_LOG(WARN, "fail to fuse row:ret[%d]", ret);
    }
    else
    {
      row = &curr_row_;
      last_rowkey_ = sstable_rowkey;
      rowkey_index_++;
    }
  }
  return ret;
}

int ObTabletGetFuse::close()
{
  int ret = OB_SUCCESS;
  int err = OB_SUCCESS;
  if (OB_SUCCESS != (err = sstable_get_->close()))
  {
    ret = err;
    YYSYS_LOG(WARN, "fail to close sstable get:err[%d]", err);
  }
  if (OB_SUCCESS != (err = incremental_get_->close()))
  {
    ret = err;
    YYSYS_LOG(WARN, "fail to close incremental get:err[%d]", err);
  }
  return ret;
}

namespace oceanbase
{
  namespace sql
  {
    REGISTER_PHY_OPERATOR(ObTabletGetFuse, PHY_TABLET_GET_FUSE);
  }
}

int64_t ObTabletGetFuse::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ObTabletGetFuse");
  return pos;
}

int ObTabletGetFuse::set_child(int32_t child_idx, ObPhyOperator &child_operator)
{
  int ret = OB_NOT_IMPLEMENT;
  UNUSED(child_idx);
  UNUSED(child_operator);
  return ret;
}

int ObTabletGetFuse::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  return sstable_get_->get_row_desc(row_desc);
}

int ObTabletGetFuse::get_last_rowkey(const common::ObRowkey *&rowkey)
{
  rowkey = last_rowkey_;
  return OB_SUCCESS;
}
