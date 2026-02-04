/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_raw_row.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_raw_row.h"
using namespace oceanbase::common;
ObRawRow::ObRawRow()
  : cells_((ObObj*)cells_buffer_), cells_count_(0), reserved1_(0), reserved2_(0)
{
  memset(cells_buffer_, 0, sizeof(cells_buffer_));
}

ObRawRow::~ObRawRow()
{
}

void ObRawRow::assign(const ObRawRow &other)
{
  if (this != &other)
  {
    for (int16_t i = 0; i < other.cells_count_; ++i)
    {
      this->cells_[i] = other.cells_[i];
    }
    this->cells_count_ = other.cells_count_;
  }
}

int ObRawRow::add_cell(const common::ObObj &cell)
{
  int ret = OB_SUCCESS;
  if (cells_count_ >= MAX_COLUMNS_COUNT)
  {
    YYSYS_LOG(WARN, "array overflow, cells_count=%hd", cells_count_);
    ret = OB_SIZE_OVERFLOW;
  }
  else
  {
    cells_[cells_count_++] = cell;
  }
  return ret;
}
//add shili [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20150705:b
//@berif  ��ȡObRawRow �������Ŀռ�
int64_t ObRawRow::get_deep_copy_size() const
{
  int64_t obj_arr_len = cells_count_ * sizeof(ObObj);
  int64_t total_len = obj_arr_len;
  for (int64_t i = 0; i < cells_count_ ; ++i)  //����ObVarcharType ObDecimalType���Ϳռ�
  {
//    if (cells_[i].get_type() == ObVarcharType||cells_[i].get_type() == ObDecimalType)
    if (cells_[i].get_type() == ObVarcharType)
    {
      total_len += cells_[i].get_val_len();
    }
    else if (cells_[i].get_type() == ObDecimalType)
    {
      total_len += cells_[i].get_nwords() * sizeof(uint64_t);
    }
  }
  return total_len;
}
//add 20150705:e

//add shili [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20150705:b
void ObRawRow::dump(const int32_t log_level /*= YYSYS_LOG_LEVEL_DEBUG*/) const
{
  for(int16_t i=0;i<cells_count_;i++)
  {
    cells_[i].dump(log_level);
  }
}
//add 20150705:e

