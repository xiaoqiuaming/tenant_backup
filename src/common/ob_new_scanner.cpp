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
 *   yubai <yubai.lk@taobao.com>
 *     - some work details if you want
 *   yanran <yanran.hfs@taobao.com> 2010-10-27
 *     - new serialization format(ObObj composed, extented version)
 *   xiaochu.yh <xiaochu.yh@taobao.com> 2012-6-14
 *     - derived from ob_scanner.cpp and make it row interface only
 */

#include "ob_new_scanner.h"
#include "ob_new_scanner_helper.h"

#include <new>

#include "yylog.h"
#include "ob_malloc.h"
#include "ob_define.h"
#include "serialization.h"
#include "ob_action_flag.h"
#include "utility.h"
#include "ob_row.h"
#include "ob_schema.h"

using namespace oceanbase::common;

///////////////////////////////////////////////////////////////////////////////////////////////////

ObNewScanner::ObNewScanner() :
  row_store_(ObModIds::OB_NEW_SCANNER), cur_size_counter_(0), mem_size_limit_(DEFAULT_MAX_SERIALIZE_SIZE),
  mod_(ObModIds::OB_NEW_SCANNER), rowkey_allocator_(ModuleArena::DEFAULT_PAGE_SIZE, mod_),
  default_row_desc_(NULL)
{
  data_version_ = 0;
  has_range_ = false;
  is_request_fullfilled_ = false;
  fullfilled_row_num_ = 0;
  cur_row_num_ = 0;
  //add lijianqiang [MultiUPS] [SELECT_MERGE] 20151228:b
  has_row_version_ = false;
  scanner_type_ = common::OB_NEW_SCANNER_TYPE;
  //add 20151228:e

  //add shili, [MultiUPS] [COLUMN_VERSION_MERGE]  20170423:b
  //row_version_list_.clear();
  row_begin_idx_.clear();
  column_id_list_.clear();
  column_version_list_.clear();
  cur_row_version_ptr_ = NULL;
  //add e
  //add by maosy [MultiUPS 1.0][secondary index optimize]20170401 b:
  rowkey_info_.~ObRowkeyInfo();
  // add by maosy
}

ObNewScanner::~ObNewScanner()
{
  // memory auto released in row_store_;
  clear();
}

void ObNewScanner::reuse()
{
  row_store_.reuse();
  cur_size_counter_ = 0;

  range_.reset();
  rowkey_allocator_.reuse();
  data_version_ = 0;
  has_range_ = false;
  is_request_fullfilled_ = false;
  fullfilled_row_num_ = 0;
  last_row_key_.assign(NULL, 0);

  cur_row_num_ = 0;
  default_row_desc_ = NULL;
  //add lijianqiang [MultiUPS] [SELECT_MERGE] 20151228:b
  has_row_version_ = false;
  row_version_.clear();//the scanner will be resused for each new_scan or new_get,rememeber clear
  //add 20151228:e

  //add shili, [MultiUPS] [COLUMN_VERSION_MERGE]  20170423:b
  //row_version_list_.clear();
  row_begin_idx_.clear();
  column_id_list_.clear();
  column_version_list_.clear();
  cur_row_version_ptr_ = NULL;
  //add e
  rowkey_info_.~ObRowkeyInfo();	  //add by maosy  [MultiUPS 1.0][secondary index optimize]20170401 b:

}

void ObNewScanner::clear()
{
  row_store_.clear();
  cur_size_counter_ = 0;

  range_.reset();
  rowkey_allocator_.free();
  data_version_ = 0;
  has_range_ = false;
  is_request_fullfilled_ = false;
  fullfilled_row_num_ = 0;
  last_row_key_.assign(NULL, 0);

  cur_row_num_ = 0;
  default_row_desc_ = NULL;
  //add lijianqiang [MultiUPS] [SELECT_MERGE] 20151228:b
  has_row_version_ = false;
  row_version_.clear();
  //add 20151228:e
  //add shili, [MultiUPS] [COLUMN_VERSION_MERGE]  20170423:b
  //row_version_list_.clear();
  row_begin_idx_.clear();
  column_id_list_.clear();
  column_version_list_.clear();
  cur_row_version_ptr_ = NULL;
  //add e
  rowkey_info_.~ObRowkeyInfo(); //add by maosy  [MultiUPS 1.0][secondary index optimize]20170401 b:
}

int64_t ObNewScanner::set_mem_size_limit(const int64_t limit)
{
  if (0 > limit
      || DEFAULT_MAX_SERIALIZE_SIZE < limit)
  {
    YYSYS_LOG(WARN, "invlaid limit_size=%ld cur_limit_size=%ld",
              limit, mem_size_limit_);
  }
  else if (0 == limit)
  {
    // 0 means using the default value
  }
  else
  {
    mem_size_limit_ = limit;
  }
  return mem_size_limit_;
}


int ObNewScanner::add_row(const ObRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = row_store_.add_row(row, cur_size_counter_)))
  {
    YYSYS_LOG(WARN, "fail to add_row to row store. ret=%d", ret);
  }
  if (cur_size_counter_ > mem_size_limit_)
  {
    //YYSYS_LOG(WARN, "scanner memory exceeds the limit."
    //     "cur_size_counter_=%ld, mem_size_limit_=%ld", cur_size_counter_, mem_size_limit_);
    // rollback last added row
    if (OB_SUCCESS != row_store_.rollback_last_row()) // ret code ignored
    {
      YYSYS_LOG(WARN, "fail to rollback last row");
    }
    ret = OB_SIZE_OVERFLOW;
  }
  else
  {
    cur_row_num_++;
  }
  return ret;
}


int ObNewScanner::serialize_meta_param(char * buf, const int64_t buf_len, int64_t & pos) const
{
  ObObj obj;
  obj.set_ext(ObActionFlag::META_PARAM_FIELD);
  int ret = obj.serialize(buf, buf_len, pos);
  if (ret != OB_SUCCESS)
  {
    YYSYS_LOG(WARN, "serialize meta param error, ret=%d buf=%p data_len=%ld pos=%ld",
              ret, buf, buf_len, pos);
  }
  else
  {
    // row count
    obj.set_int(cur_row_num_);
    ret = obj.serialize(buf, buf_len, pos);
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld pos=%ld",
                ret, buf, buf_len, pos);
    }
  }

  // last rowkey
  if (OB_SUCCESS == ret)
  {
    ret = set_rowkey_obj_array(buf, buf_len, pos,
                               last_row_key_.get_obj_ptr(), last_row_key_.get_obj_cnt());
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "last_row_key_ serialize error, ret=%d buf=%p data_len=%ld pos=%ld",
                ret, buf, buf_len, pos);
    }
  }
  return ret;
}

// WARN: can not add cell after deserialize scanner if do that rollback will be failed for get last row key
int ObNewScanner::deserialize_meta_param(const char* buf, const int64_t data_len, int64_t& pos, ObObj &last_obj)
{
  int64_t rowkey_col_num = OB_MAX_ROWKEY_COLUMN_NUMBER;
  ObObj* rowkey = reinterpret_cast<ObObj*>(rowkey_allocator_.alloc(sizeof(ObObj) * rowkey_col_num));
  int ret = OB_SUCCESS;

  if (NULL == rowkey)
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    YYSYS_LOG(WARN, "fail to alloc memory for rowkey. ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = deserialize_int_(buf, data_len, pos, cur_row_num_, last_obj)))
  {
    YYSYS_LOG(WARN, "deserialize cur_row_num_ error, ret=%d buf=%p data_len=%ld pos=%ld",
              ret, buf, data_len, pos);
  }
  // last row key
  else if (OB_SUCCESS != (ret = get_rowkey_obj_array(buf, data_len, pos, rowkey, rowkey_col_num)))
  {
    YYSYS_LOG(WARN, "deserialize last rowkey error, ret=%d buf=%p data_len=%ld pos=%ld",
              ret, buf, data_len, pos);
  }
  else
  {
    last_row_key_.assign(rowkey ,rowkey_col_num);
    if (OB_SUCCESS != (ret = last_row_key_.deep_copy(last_row_key_, rowkey_allocator_)))
    {
      YYSYS_LOG(WARN, "fail to set deserialize last row key. ret=%d last_row_key=%s", ret, to_cstring(last_row_key_));
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = last_obj.deserialize(buf, data_len, pos);
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "ObObj deserialize error, ret=%d", ret);
    }
  }

  return ret;
}


//add lijianqiang [MultiUPS] [SELECT_MERGE] 20151228:b
int ObNewScanner::serialize_row_version(char * buf, const int64_t buf_len, int64_t & pos) const
{
  int ret = OB_SUCCESS;
  if (0 == cur_row_num_)
  {
    YYSYS_LOG(DEBUG, "no row return,do not serialize row version");
  }
  else
  {
    ObObj obj;
    obj.set_ext(ObActionFlag::ROW_VERSION_FIELD);
    int ret = obj.serialize(buf, buf_len, pos);
    if (ret != OB_SUCCESS)
    {
      YYSYS_LOG(WARN, "serialize version field error, ret=%d buf=%p data_len=%ld pos=%ld",
                ret, buf, buf_len, pos);
    }
    else
    {
      int32_t row_version_size = row_version_.size();
      YYSYS_LOG(DEBUG,"test::row version size is=%d, row num is=%ld",row_version_size, cur_row_num_);
      if (row_version_size != cur_row_num_)//check first,each row has a version
      {
        ret = OB_SERIALIZE_ERROR;
        YYSYS_LOG(ERROR,"version size is not equal row num,row version size=%d, cur row num=%ld,ret=%d",row_version_size, cur_row_num_, ret);
      }
      if (OB_SUCCESS == ret)
      {
        for (int32_t i = 0; i < row_version_size; i++)
        {
          obj.set_int(row_version_.at(i));
          ret = obj.serialize(buf, buf_len, pos);
          if (OB_SUCCESS != ret)
          {
            YYSYS_LOG(WARN, " ObObj serialize error, ret=%d buf=%p data_len=%ld pos=%ld",
                      ret, buf, buf_len, pos);
            break;
          }
        }
      }

      //add shili, [MultiUPS] [COLUMN_VERSION_MERGE]  20170423:b
      //      if(OB_SUCCESS==ret)
      //      {
      //        if(cur_row_num_!=row_version_list_.count())
      //        {
      //          ret = OB_SERIALIZE_ERROR;
      //          YYSYS_LOG(ERROR,"version size is not equal row num,row version size=%ld, cur row num=%ld,ret=%d",
      //                    row_version_list_.count(), cur_row_num_, ret);
      //        }
      //        else
      //        {
      //          for(int64_t i=0;i<row_version_list_.count();i++)
      //          {
      //            const ObRowVersion &version = row_version_list_.at(i);
      //            if (ret == OB_SUCCESS && (OB_SUCCESS != (ret = version.serialize(buf, buf_len, pos))))
      //            {
      //              YYSYS_LOG(WARN, "serialize fail. ret=%d", ret);
      //              break;
      //            }
      //          }
      //        }
      //      }

      if(OB_SUCCESS==ret)
      {
        if(cur_row_num_!=row_begin_idx_.size())
        {
          ret = OB_SERIALIZE_ERROR;
          YYSYS_LOG(ERROR,"version size is not equal row num,row version size=%d, cur row num=%ld,ret=%d",
                    row_begin_idx_.size(), cur_row_num_, ret);
        }
        else
        {
          for(int32_t i=0;i<row_begin_idx_.size();i++)
          {
            obj.set_int(row_begin_idx_.at(i));
            ret = obj.serialize(buf, buf_len, pos);
            if (OB_SUCCESS != ret)
            {
              YYSYS_LOG(WARN, " ObObj serialize error, ret=%d buf=%p data_len=%ld pos=%ld",
                        ret, buf, buf_len, pos);
              break;
            }
          }
          obj.set_int(static_cast<int64_t>(column_id_list_.size()));
          ret = obj.serialize(buf, buf_len, pos);
          for(int32_t i=0;i<column_id_list_.size();i++)
          {
            obj.set_int(column_id_list_.at(i));
            ret = obj.serialize(buf, buf_len, pos);
            if (OB_SUCCESS != ret)
            {
              YYSYS_LOG(WARN, " ObObj serialize error, ret=%d buf=%p data_len=%ld pos=%ld",
                        ret, buf, buf_len, pos);
              break;
            }
          }
          for(int32_t i=0;i<column_version_list_.size();i++)
          {
            obj.set_int(column_version_list_.at(i));
            ret = obj.serialize(buf, buf_len, pos);
            if (OB_SUCCESS != ret)
            {
              YYSYS_LOG(WARN, " ObObj serialize error, ret=%d buf=%p data_len=%ld pos=%ld",
                        ret, buf, buf_len, pos);
              break;
            }
          }
        }
      }
      //add e
    }
  }
  return ret;
}

int ObNewScanner::deserialize_row_version(const char* buf, const int64_t data_len, int64_t& pos, ObObj &last_obj)
{
  int ret = OB_SUCCESS;
  int64_t cur_version;
  row_version_.clear();
  if (0 == cur_row_num_)
  {
    has_row_version_ = false;
    //    YYSYS_LOG(INFO, "no row return,may be scan");
  }
  else
  {
    has_row_version_ = true;
    for (int32_t i=0; i<cur_row_num_; i++)
    {
      if (OB_SUCCESS != (ret = deserialize_int_(buf, data_len, pos, cur_version, last_obj)))
      {
        YYSYS_LOG(WARN, "deserialize cur_version error, ret=%d buf=%p data_len=%ld pos=%ld",
                  ret, buf, data_len, pos);
        break;
      }
      else if (OB_SUCCESS != (ret = row_version_.push_back(cur_version)))
      {
        YYSYS_LOG(WARN,"add deserialize row version failed,cur version=%ld,ret=%d",cur_version ,ret);
        break;
      }
    }
    //add shili, [MultiUPS] [COLUMN_VERSION_MERGE]  20170423:b
    if(OB_SUCCESS==ret)
    {
      //      ObRowVersion version;
      //      for (int32_t i=0; i<cur_row_num_; i++)
      //      {
      //        if(OB_SUCCESS!=(ret=row_version_list_.push_back(version)))
      //        {
      //        }
      //        else if (OB_SUCCESS != (ret = row_version_list_.at(row_version_list_.count() - 1).deserialize(buf, data_len, pos)))
      //        {
      //          YYSYS_LOG(WARN, "fail to deserialize row version. ret=%d", ret);
      //          break;
      //        }
      //      }

      for (int32_t i=0; OB_SUCCESS==ret && i<cur_row_num_; i++)
      {
        if (OB_SUCCESS != (ret = deserialize_int_(buf, data_len, pos, cur_version, last_obj)))
        {
          YYSYS_LOG(WARN, "deserialize cur_version error, ret=%d buf=%p data_len=%ld pos=%ld",
                    ret, buf, data_len, pos);
          break;
        }
        else if (OB_SUCCESS != (ret = row_begin_idx_.push_back(cur_version)))
        {
          YYSYS_LOG(WARN,"add deserialize row version failed,cur version=%ld,ret=%d",cur_version ,ret);
          break;
        }
      }
      int64_t column_num = 0;
      ret = deserialize_int_(buf, data_len, pos, column_num, last_obj);
      for (int32_t i=0; OB_SUCCESS==ret && i<column_num; i++)
      {
        if (OB_SUCCESS != (ret = deserialize_int_(buf, data_len, pos, cur_version, last_obj)))
        {
          YYSYS_LOG(WARN, "deserialize cur_version error, ret=%d buf=%p data_len=%ld pos=%ld",
                    ret, buf, data_len, pos);
          break;
        }
        else if (OB_SUCCESS != (ret = column_id_list_.push_back(cur_version)))
        {
          YYSYS_LOG(WARN,"add deserialize row version failed,cur version=%ld,ret=%d",cur_version ,ret);
          break;
        }
      }
      for (int32_t i=0; OB_SUCCESS==ret && i<column_num; i++)
      {
        if (OB_SUCCESS != (ret = deserialize_int_(buf, data_len, pos, cur_version, last_obj)))
        {
          YYSYS_LOG(WARN, "deserialize cur_version error, ret=%d buf=%p data_len=%ld pos=%ld",
                    ret, buf, data_len, pos);
          break;
        }
        else if (OB_SUCCESS != (ret = column_version_list_.push_back(cur_version)))
        {
          YYSYS_LOG(WARN,"add deserialize row version failed,cur version=%ld,ret=%d",cur_version ,ret);
          break;
        }
      }
    }
    //add e
  }
  //get next ext action flag
  if (OB_SUCCESS == ret)
  {
    ret = last_obj.deserialize(buf, data_len, pos);
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "ObObj deserialize error, ret=%d", ret);
    }
  }
  return ret;
}

int64_t ObNewScanner::get_row_version_serialize_size(void) const
{
  int64_t ret = 0;
  ObObj obj;
  int32_t row_version_size = row_version_.size();
  if (row_version_size != cur_row_num_)
  {
    YYSYS_LOG(WARN,"row version size is not equal row num ,please check,row version size is=%d, row num is=%ld",row_version_size, cur_row_num_);
  }
  else
  {
    if (0 != row_version_size)
    {
      obj.set_ext(ObActionFlag::ROW_VERSION_FIELD);
      ret += obj.get_serialize_size();
    }
    for (int32_t i=0; i<row_version_size; i++)
    {
      obj.set_int(row_version_.at(i));
      ret += obj.get_serialize_size();
    }
  }

  //add shili, [MultiUPS] [COLUMN_VERSION_MERGE]  20170423:b
  if(OB_SUCCESS==ret)
  {
    //    if(row_version_list_.count()!=cur_row_num_)
    //    {
    //      YYSYS_LOG(WARN,"row version size is not equal row num ,please check,row version size is=%ld, row num is=%ld",
    //                row_version_list_.count(), cur_row_num_);
    //    }
    //    else
    //    {
    //      for(int64_t i =0;i<row_version_list_.count();i++)
    //      {
    //        ret += row_version_list_.at(i).get_serialize_size();
    //      }
    //    }
    if (row_begin_idx_.size() != cur_row_num_)
    {
      YYSYS_LOG(WARN,"row version size is not equal row num ,please check,row version size is=%d, row num is=%ld",
                row_begin_idx_.size(), cur_row_num_);
    }
    else
    {
      ret += row_begin_idx_.size() * obj.get_serialize_size();
      ret += 2 * column_id_list_.size() * obj.get_serialize_size();
      ret +=  obj.get_serialize_size();
    }
  }
  //add e
  return ret;
}
//add 20151228:e


//int ObNewScanner::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
DEFINE_SERIALIZE(ObNewScanner)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  int64_t next_pos = pos;

  if (OB_SUCCESS == ret)
  {
    ///obj.reset();
    obj.set_ext(ObActionFlag::BASIC_PARAM_FIELD);
    ret = obj.serialize(buf, buf_len, next_pos);
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                ret, buf, buf_len, next_pos);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ///obj.reset();
    obj.set_int(is_request_fullfilled_ ? 1 : 0);
    ret = obj.serialize(buf, buf_len, next_pos);
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                ret, buf, buf_len, next_pos);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ///obj.reset();
    obj.set_int(fullfilled_row_num_);
    ret = obj.serialize(buf, buf_len, next_pos);
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                ret, buf, buf_len, next_pos);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ///obj.reset();
    obj.set_int(data_version_);
    ret = obj.serialize(buf, buf_len, next_pos);
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                ret, buf, buf_len, next_pos);
    }
  }

  if (has_range_)
  {
    if (OB_SUCCESS == ret)
    {
      obj.set_ext(ObActionFlag::TABLET_RANGE_FIELD);
      ret = obj.serialize(buf, buf_len, next_pos);
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                  ret, buf, buf_len, next_pos);
      }
    }
    // range - border flag
    if (OB_SUCCESS == ret)
    {
      obj.set_int(range_.border_flag_.get_data());
      ret = obj.serialize(buf, buf_len, next_pos);
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                  ret, buf, buf_len, next_pos);
      }
    }
    // range - start key
    if (OB_SUCCESS == ret)
    {
      ret = range_.start_key_.serialize(buf, buf_len,  next_pos);
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "ObRowkey serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                  ret, buf, buf_len, next_pos);
      }
    }
    // range - end key
    if (OB_SUCCESS == ret)
    {
      ret = range_.end_key_.serialize(buf, buf_len,  next_pos);
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                  ret, buf, buf_len, next_pos);
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = serialize_meta_param(buf, buf_len, next_pos);
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "fail to serialize meta param. ret=%d", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ///obj.reset();
    obj.set_ext(ObActionFlag::TABLE_PARAM_FIELD);
    ret = obj.serialize(buf, buf_len, next_pos);
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                ret, buf, buf_len, next_pos);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = row_store_.serialize(buf, buf_len, next_pos);
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "ObRowStore serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                ret, buf, buf_len, next_pos);
    }
  }
  //add lijianqiang [MultiUPS] [SELECT_MERGE] 20151228:b
  if (has_row_version_)
  {
    if (OB_SUCCESS == ret)
    {
      ret = serialize_row_version(buf, buf_len, next_pos);
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "row_version serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                  ret, buf, buf_len, next_pos);
      }
    }
  }
  //add 20151228:e

  if (OB_SUCCESS == ret)
  {
    ///obj.reset();
    //YYSYS_LOG(DEBUG, "set  END_PARAM_FIELD to scanner");
    obj.set_ext(ObActionFlag::END_PARAM_FIELD);
    ret = obj.serialize(buf, buf_len, next_pos);
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "ObObj serialize error, ret=%d buf=%p data_len=%ld next_pos=%ld",
                ret, buf, buf_len, next_pos);
    }
  }

  if (OB_SUCCESS == ret)
  {
    pos = next_pos;
  }
  return ret;
}


int64_t ObNewScanner::get_meta_param_serialize_size() const
{
  int64_t ret = 0;
  ObObj obj;
  obj.set_ext(ObActionFlag::META_PARAM_FIELD);
  ret += obj.get_serialize_size();
  obj.set_int(cur_row_num_);
  ret += obj.get_serialize_size();
  ret += get_rowkey_obj_array_size(last_row_key_.ptr(), last_row_key_.get_obj_cnt());
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObNewScanner)
//int64_t ObNewScanner::get_serialize_size(void) const
{
  int64_t ret = 0;
  ObObj obj;
  obj.set_ext(ObActionFlag::BASIC_PARAM_FIELD);
  ret += obj.get_serialize_size();
  obj.set_int(is_request_fullfilled_ ? 1 : 0);
  ret += obj.get_serialize_size();
  obj.set_int(fullfilled_row_num_);
  ret += obj.get_serialize_size();
  obj.set_int(data_version_);
  ret += obj.get_serialize_size();
  if (has_range_)
  {
    obj.set_ext(ObActionFlag::TABLET_RANGE_FIELD);
    ret += obj.get_serialize_size();
    obj.set_int(range_.border_flag_.get_data());
    ret += obj.get_serialize_size();
    ret += range_.start_key_.get_serialize_size();
    ret += range_.end_key_.get_serialize_size();
  }
  ret += get_meta_param_serialize_size();
  obj.set_ext(ObActionFlag::TABLE_PARAM_FIELD);
  ret += obj.get_serialize_size();
  ret += row_store_.get_serialize_size();
  //add lijianqiang [MultiUPS] [SELECT_MERGE] 20151228:b
  if (has_row_version_)
  {
    ret += get_row_version_serialize_size();
  }
  //add 20151228:e
  obj.set_ext(ObActionFlag::END_PARAM_FIELD);
  ret += obj.get_serialize_size();
  return ret;
}

//DEFINE_DESERIALIZE(ObNewScanner)
int ObNewScanner::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;

  if (NULL == buf || 0 >= data_len - pos)
  {
    YYSYS_LOG(WARN, "invalid param buf=%p data_len=%ld pos=%ld", buf, data_len, pos);
    ret = OB_ERROR;
  }
  else
  {
    ObObj param_id;
    ObObjType obj_type;
    int64_t param_id_type;
    bool is_end = false;
    int64_t new_pos = pos;

    clear();

    ret = param_id.deserialize(buf, data_len, new_pos);
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "ObObj deserialize error, ret=%d", ret);
    }

    while (OB_SUCCESS == ret && new_pos <= data_len && !is_end)
    {
      obj_type = param_id.get_type();
      // read until reaching a ObExtendType ObObj
      while (OB_SUCCESS == ret && ObExtendType != obj_type)
      {
        ret = param_id.deserialize(buf, data_len, new_pos);
        if (OB_SUCCESS != ret)
        {
          YYSYS_LOG(WARN, "ObObj deserialize error, ret=%d", ret);
        }
        else
        {
          obj_type = param_id.get_type();
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = param_id.get_ext(param_id_type);
        if (OB_SUCCESS != ret)
        {
          YYSYS_LOG(WARN, "ObObj type is not ObExtendType, invalid status, ret=%d", ret);
        }
        else
        {
          switch (param_id_type)
          {
            case ObActionFlag::BASIC_PARAM_FIELD:
              ret = deserialize_basic_(buf, data_len, new_pos, param_id);
              if (OB_SUCCESS != ret)
              {
                YYSYS_LOG(WARN, "deserialize_basic_ error, ret=%d buf=%p data_len=%ld new_pos=%ld",
                          ret, buf, data_len, new_pos);
              }
              break;
            case ObActionFlag::META_PARAM_FIELD:
              ret = deserialize_meta_param(buf, data_len, new_pos, param_id);
              if (OB_SUCCESS != ret)
              {
                YYSYS_LOG(WARN, "deserialize_meta_data error, ret=%d buf=%p data_len=%ld new_pos=%ld",
                          ret, buf, data_len, new_pos);
              }
              break;
            case ObActionFlag::TABLE_PARAM_FIELD:
              ret = deserialize_table_(buf, data_len, new_pos, param_id);
              if (OB_SUCCESS != ret)
              {
                YYSYS_LOG(WARN, "deserialize_table_ error, ret=%d buf=%p data_len=%ld new_pos=%ld",
                          ret, buf, data_len, new_pos);
              }
              break;
              //add lijianqiang [MultiUPS] [SELECT_MERGE] 20151228:b
            case ObActionFlag::ROW_VERSION_FIELD:
              ret = deserialize_row_version(buf, data_len, new_pos, param_id);
              if (OB_SUCCESS != ret)
              {
                YYSYS_LOG(WARN, "deserialize row version error, ret=%d buf=%p data_len=%ld new_pos=%ld",
                          ret, buf, data_len, new_pos);
              }
              break;
              //add 20151228:e
            case ObActionFlag::END_PARAM_FIELD:
              is_end = true;
              break;
            default:
              ret = param_id.deserialize(buf, data_len, new_pos);
              if (OB_SUCCESS != ret)
              {
                YYSYS_LOG(WARN, "ObObj deserialize error, ret=%d", ret);
              }
              break;
          }
        }
      }
    }

    if (OB_SUCCESS == ret)
    {
      pos = new_pos;
    }
  }
  return ret;
}


int ObNewScanner::deserialize_basic_(const char* buf, const int64_t data_len, int64_t& pos, ObObj &last_obj)
{
  int ret = OB_SUCCESS;

  int64_t value = 0;

  // deserialize isfullfilled
  ret = deserialize_int_(buf, data_len, pos, value, last_obj);
  if (OB_SUCCESS != ret)
  {
    YYSYS_LOG(WARN, "deserialize_int_ error, ret=%d buf=%p data_len=%ld pos=%ld",
              ret, buf, data_len, pos);
  }
  else
  {
    is_request_fullfilled_ = (value == 0 ? false : true);
  }
  // deserialize expect-to-be-filled row number
  if (OB_SUCCESS == ret)
  {
    ret = deserialize_int_(buf, data_len, pos, value, last_obj);
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "deserialize_int_ error, ret=%d buf=%p data_len=%ld pos=%ld",
                ret, buf, data_len, pos);
    }
    else
    {
      fullfilled_row_num_ = value;
    }
  }
  // deserialize data_version
  if (OB_SUCCESS == ret)
  {
    ret = deserialize_int_(buf, data_len, pos, value, last_obj);
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "deserialize_int_ error, ret=%d buf=%p data_len=%ld pos=%ld",
                ret, buf, data_len, pos);
    }
    else
    {
      data_version_ = value;
    }
  }
  // deserialize range
  if (OB_SUCCESS == ret)
  {
    ret = last_obj.deserialize(buf, data_len, pos);
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "ObObj deserialize error, ret=%d buf=%p data_len=%ld pos=%ld",
                ret, buf, data_len, pos);
    }
    else
    {
      if (ObExtendType != last_obj.get_type())
      {
        // maybe new added field
      }
      else
      {
        int64_t type = last_obj.get_ext();
        ObNewRange range;
        ObObj start_rowkey_buf[OB_MAX_ROWKEY_COLUMN_NUMBER];
        ObObj end_rowkey_buf[OB_MAX_ROWKEY_COLUMN_NUMBER];
        range.start_key_.assign(start_rowkey_buf, OB_MAX_ROWKEY_COLUMN_NUMBER);
        range.end_key_.assign(end_rowkey_buf, OB_MAX_ROWKEY_COLUMN_NUMBER);
        if (ObActionFlag::TABLET_RANGE_FIELD == type)
        {
          int64_t border_flag = 0;
          if (OB_SUCCESS != (ret = deserialize_int_(buf, data_len, pos, border_flag, last_obj)))
          {
            YYSYS_LOG(WARN, "deserialize_int_ error, ret=%d buf=%p data_len=%ld pos=%ld",
                      ret, buf, data_len, pos);
          }
          else if (OB_SUCCESS != (ret = range.start_key_.deserialize(buf, data_len, pos)))
          {
            YYSYS_LOG(WARN, "deserialize range start key error, ret=%d buf=%p data_len=%ld pos=%ld",
                      ret, buf, data_len, pos);
          }
          else if (OB_SUCCESS != (ret = range.end_key_.deserialize(buf, data_len, pos)))
          {
            YYSYS_LOG(WARN, "deserialize range end key error, ret=%d buf=%p data_len=%ld pos=%ld",
                      ret, buf, data_len, pos);
          }
          else
          {
            range.table_id_ = OB_INVALID_ID; // ignore
            range.border_flag_.set_data(static_cast<int8_t>(border_flag));
            if (OB_SUCCESS != (ret = deep_copy_range(rowkey_allocator_, range, range_)))
            {
              YYSYS_LOG(WARN, "fail to deep copy range %s", to_cstring(range_));
            }
            has_range_ = true;
          }
        }
        else
        {
          // maybe another param field
        }
      }
    }
  }

  // after reading all neccessary fields,
  // filter unknown ObObj
  if (OB_SUCCESS == ret && ObExtendType != last_obj.get_type())
  {
    ret = last_obj.deserialize(buf, data_len, pos);
    while (OB_SUCCESS == ret && ObExtendType != last_obj.get_type())
    {
      ret = last_obj.deserialize(buf, data_len, pos);
    }
  }

  return ret;
}

int ObNewScanner::deserialize_table_(const char* buf, const int64_t data_len, int64_t& pos, ObObj &last_obj)
{
  int ret = OB_SUCCESS;
  ret = row_store_.deserialize(buf, data_len, pos);
  if (OB_SUCCESS != ret)
  {
    YYSYS_LOG(WARN, "deserialize_table_ error, ret=%d buf=%p data_len=%ld new_pos=%ld",
              ret, buf, data_len, pos);
  }
  if (OB_SUCCESS == ret)
  {
    ret = last_obj.deserialize(buf, data_len, pos);
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "ObObj deserialize error, ret=%d", ret);
    }
  }
  return ret;
}

int ObNewScanner::deserialize_int_(const char* buf, const int64_t data_len, int64_t& pos,
                                   int64_t &value, ObObj &last_obj)
{
  int ret = OB_SUCCESS;

  ret = last_obj.deserialize(buf, data_len, pos);
  if (OB_SUCCESS != ret)
  {
    YYSYS_LOG(WARN, "ObObj deserialize error, ret=%d buf=%p data_len=%ld pos=%ld",
              ret, buf, data_len, pos);
  }
  else
  {
    if (ObIntType != last_obj.get_type())
    {
      YYSYS_LOG(WARN, "ObObj type is not Int, Type=%d", last_obj.get_type());
    }
    else
    {
      ret = last_obj.get_int(value);
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "ObObj get_int error, ret=%d", ret);
      }
    }
  }

  return ret;
}

int ObNewScanner::deserialize_varchar_(const char* buf, const int64_t data_len, int64_t& pos,
                                       ObString &value, ObObj &last_obj)
{
  int ret = OB_SUCCESS;

  ret = last_obj.deserialize(buf, data_len, pos);
  if (OB_SUCCESS != ret)
  {
    YYSYS_LOG(WARN, "ObObj deserialize error, ret=%d buf=%p data_len=%ld pos=%ld",
              ret, buf, data_len, pos);
  }
  else
  {
    if (ObVarcharType != last_obj.get_type())
    {
      YYSYS_LOG(WARN, "ObObj type is not Varchar, Type=%d", last_obj.get_type());
    }
    else
    {
      ret = last_obj.get_varchar(value);
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "ObObj get_varchar error, ret=%d", ret);
      }
    }
  }

  return ret;
}

int ObNewScanner::deserialize_int_or_varchar_(const char* buf, const int64_t data_len, int64_t& pos,
                                              int64_t& int_value, ObString &varchar_value, ObObj &last_obj)
{
  int ret = OB_SUCCESS;

  ret = last_obj.deserialize(buf, data_len, pos);
  if (OB_SUCCESS != ret)
  {
    YYSYS_LOG(WARN, "ObObj deserialize error, ret=%d buf=%p data_len=%ld pos=%ld",
              ret, buf, data_len, pos);
  }
  else
  {
    if (ObIntType == last_obj.get_type())
    {
      ret = last_obj.get_int(int_value);
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "ObObj get_int error, ret=%d", ret);
      }
    }
    else if (ObVarcharType == last_obj.get_type())
    {
      ret = last_obj.get_varchar(varchar_value);
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "ObObj get_varchar error, ret=%d", ret);
      }
    }
    else
    {
      YYSYS_LOG(WARN, "ObObj type is not int or varchar, Type=%d", last_obj.get_type());
      ret = OB_ERROR;
    }
  }

  return ret;
}

//add shili, [MultiUPS] [COLUMN_VERSION_MERGE]  20170423:b
int ObNewScanner::add_row_version(const ObRowVersion & version)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS!=(ret=row_begin_idx_.push_back(column_id_list_.size())))
  {
    YYSYS_LOG(WARN, "row version list push back fail,ret=%d",ret);
  }

  for (int64_t idx = 0;OB_SUCCESS == ret && idx < version.column_num_; ++idx)
  {
    if (OB_SUCCESS!=(ret=column_id_list_.push_back(version.column_versions_[idx].column_id_)))
    {
      YYSYS_LOG(WARN, "row version list push back fail,ret=%d",ret);
    }
    else if (OB_SUCCESS!=(ret=column_version_list_.push_back(version.column_versions_[idx].version_)))
    {
      YYSYS_LOG(WARN, "row version list push back fail,ret=%d",ret);
    }
  }
  return ret;
}

void ObNewScanner::set_row_version_ptr(ObRowVersion * version)
{
  cur_row_version_ptr_ = version;
}
//add e


int ObNewScanner::add_row(const ObRowkey &rowkey, const ObRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = row_store_.add_row(rowkey, row, cur_size_counter_)))
  {
    YYSYS_LOG(WARN, "fail to add_row to row store. ret=%d", ret);
  }
  //add lijianqiang [MultiUPS] [SELECT_MERGE] 20151228:b
  int64_t version_size = 0;
  if (has_row_version_)
  {
    /**
     * @note Be careful! the cur_size_counter_ is assigned in the row_store_.add_row(...)
     * for each row
     */
    YYSYS_LOG(DEBUG,"has row version");
    //version_size = (cur_row_num_ + 1) * (int64_t)sizeof(common::ObObj);
    version_size = (cur_row_num_ + 2) * serialization::encoded_length_i64(0);
    //add shili, [MultiUPS] [COLUMN_VERSION_MERGE]  20170423:b
    //    for(int64_t i=0;i<row_version_list_.count();i++)
    //    {
    //      version_size+=row_version_list_.at(i).get_serialize_size();
    //    }
    //    version_size+= cur_row_version_ptr_->get_serialize_size();
    version_size += (cur_row_num_ + 1) * serialization::encoded_length_i64(0) ;
    version_size += 2 * (column_id_list_.size() + cur_row_version_ptr_->column_num_) * serialization::encoded_length_i64(0);
    //add e
  }
  //add 20151228:e
  if (cur_size_counter_ + version_size > mem_size_limit_)
  {
    //YYSYS_LOG(WARN, "scanner memory exceeds the limit."
    //     "cur_size_counter_=%ld, mem_size_limit_=%ld", cur_size_counter_, mem_size_limit_);
    // rollback last added row
    if (OB_SUCCESS != row_store_.rollback_last_row()) // ret code ignored
    {
      YYSYS_LOG(WARN, "fail to rollback last row");
    }
    ret = OB_SIZE_OVERFLOW;
  }
  else
  {
    //add lijianqiang [MultiUPS] [SELECT_MERGE] 20151228:b
    if (has_row_version_)
    {
      row_version_.push_back(data_version_);
      //add shili, [MultiUPS] [COLUMN_VERSION_MERGE]  20170423:b
      if(OB_SUCCESS!=(ret=add_row_version(*cur_row_version_ptr_)))
      {
        YYSYS_LOG(WARN, "add_row_version fail,ret=%d",ret);
      }
      YYSYS_LOG(DEBUG, "current row version=%s,cur row num=%ld,row_version size is=%d",
                to_cstring(*cur_row_version_ptr_),cur_row_num_ +1, row_version_.size());
      //add e
    }
    //add 20151228:e
    cur_row_num_++;
  }
  return ret;
}

int ObNewScanner::get_next_row(const ObRowkey *&rowkey, ObRow &row)
{
  int ret = OB_SUCCESS;
  ret = row_store_.get_next_row(rowkey, row);
  return ret;
}

int ObNewScanner::get_next_row(ObRow &row)
{
  int ret = OB_SUCCESS;
  if (NULL == row.get_row_desc() && NULL != default_row_desc_)
  {
    row.set_row_desc(*default_row_desc_);
  }
  ret = row_store_.get_next_row(row);
  return ret;
}

bool ObNewScanner::is_empty() const
{
  return row_store_.is_empty();
}

int ObNewScanner::set_last_row_key(ObRowkey &row_key)
{
  return row_key.deep_copy(last_row_key_, rowkey_allocator_);
}

int ObNewScanner::get_last_row_key(ObRowkey &row_key) const
{
  int ret = OB_SUCCESS;

  if (last_row_key_.length() == 0)
  {
    ret = OB_ENTRY_NOT_EXIST;
  }
  else
  {
    row_key = last_row_key_;
  }

  return ret;
}

int ObNewScanner::set_is_req_fullfilled(const bool &is_fullfilled, const int64_t fullfilled_row_num)
{
  int err = OB_SUCCESS;
  if (0 > fullfilled_row_num)
  {
    YYSYS_LOG(WARN,"param error [fullfilled_row_num:%ld]", fullfilled_row_num);
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err)
  {
    fullfilled_row_num_ = fullfilled_row_num;
    is_request_fullfilled_ = is_fullfilled;
  }
  return err;
}

int ObNewScanner::get_is_req_fullfilled(bool &is_fullfilled, int64_t &fullfilled_row_num) const
{
  int err = OB_SUCCESS;
  is_fullfilled = is_request_fullfilled_;
  fullfilled_row_num = fullfilled_row_num_;
  return err;
}

int ObNewScanner::set_range(const ObNewRange &range)
{
  int ret = OB_SUCCESS;

  if (has_range_)
  {
    YYSYS_LOG(WARN, "range has been setted before");
    ret = OB_ERROR;
  }
  else
  {
    ret = deep_copy_range(rowkey_allocator_, range, range_);
    has_range_ = true;
  }

  return ret;
}

int ObNewScanner::set_range_shallow_copy(const ObNewRange &range)
{
  int ret = OB_SUCCESS;

  if (has_range_)
  {
    YYSYS_LOG(WARN, "range has been setted before");
    ret = OB_ERROR;
  }
  else
  {
    range_ = range;
    has_range_ = true;
  }
  return ret;
}

int ObNewScanner::get_range(ObNewRange &range) const
{
  int ret = OB_SUCCESS;

  if (!has_range_)
  {
    YYSYS_LOG(WARN, "range has not been setted");
    ret = OB_ENTRY_NOT_EXIST;
  }
  else
  {
    range = range_;
  }

  return ret;
}

int64_t ObNewScanner::to_string(char* buffer, const int64_t length) const
{
  int64_t pos = 0;
  databuff_printf(buffer, length, pos, "ObNewScanner(range:");
  if (has_range_)
  {
    pos = range_.to_string(buffer, length);
  }
  else
  {
    databuff_printf(buffer, length, pos, "no range");
  }
  databuff_printf(buffer, length, pos, ",fullfill=%d,fullfill_row_num=%ld,cur_row_num=%ld,cur_size_count=%ld,data_version=%ld",
                  is_request_fullfilled_, fullfilled_row_num_, cur_row_num_, cur_size_counter_, data_version_);
  return pos;
}

void ObNewScanner::dump(void) const
{
  YYSYS_LOG(INFO, "[dump] scanner range info:");
  if (has_range_)
  {
    range_.hex_dump();
  }
  else
  {
    YYSYS_LOG(INFO, "    no scanner range found!");
  }

  YYSYS_LOG(INFO, "[dump] is_request_fullfilled_=%d", is_request_fullfilled_);
  YYSYS_LOG(INFO, "[dump] fullfilled_row_num_=%ld", fullfilled_row_num_);
  YYSYS_LOG(INFO, "[dump] cur_row_num_=%ld", cur_row_num_);
  YYSYS_LOG(INFO, "[dump] cur_size_counter_=%ld", cur_size_counter_);
  YYSYS_LOG(INFO, "[dump] data_version_=%ld", data_version_);
}

void ObNewScanner::dump_all(int log_level) const
{
  int err = OB_SUCCESS;
  /// dump all result that will be send to ob client
  if (OB_SUCCESS == err && log_level >= YYSYS_LOG_LEVEL_DEBUG)
  {
    this->dump();
    // TODO: write new Scanner iterator
#if 0
    ObCellInfo *cell = NULL;
    bool row_change = false;
    ObNewScannerIterator iter = this->begin();
    while((iter != this->end()) &&
          (OB_SUCCESS == (err = iter.get_cell(&cell, &row_change))))
    {
      if (NULL == cell)
      {
        err = OB_ERROR;
        break;
      }
      if (row_change)
      {
        YYSYS_LOG(DEBUG, "dump cellinfo rowkey with hex format, length(%d)", cell->row_key_.length());
        hex_dump(cell->row_key_.ptr(), cell->row_key_.length(), true, YYSYS_LOG_LEVEL_DEBUG);
      }
      cell->value_.dump(YYSYS_LOG_LEVEL_DEBUG);
      ++iter;
    }
#endif

    if (err != OB_SUCCESS)
    {
      YYSYS_LOG(WARN, "fail to get scanner cell as debug output. ");
    }
  }
}

int ObCellNewScanner::set_is_req_fullfilled(const bool &is_fullfilled, const int64_t fullfilled_row_num)
{
  if (!is_fullfilled)
  {
    is_size_overflow_ = true;
  }
  return ObNewScanner::set_is_req_fullfilled(is_fullfilled, fullfilled_row_num);
}

int ObCellNewScanner::add_cell(const ObCellInfo &cell_info,
                               const bool is_compare_rowkey, const bool is_rowkey_change)
{
  int ret = OB_SUCCESS;
  bool row_changed = false;
  if (last_table_id_ == OB_INVALID_ID)
  {
    // first cell;
    row_changed = true;
  }
  else if (last_table_id_ != cell_info.table_id_)
  {
    YYSYS_LOG(WARN, "cannot add another table :%lu, current table:%ld",
              cell_info.table_id_, last_table_id_);
    ret = OB_INVALID_ARGUMENT;
  }
  else if (!is_compare_rowkey && is_rowkey_change)
  {
    row_changed = true;
  }
  else if (cur_rowkey_ != cell_info.row_key_)
  {
    row_changed = true;
  }

  if (OB_SUCCESS == ret)
  {
    if (row_changed && last_table_id_ != OB_INVALID_ID)
    {
      // okay, got a row in compactor;
      ret = add_current_row();
      if(OB_SIZE_OVERFLOW == ret)
      {
        is_size_overflow_ = true;
      }
      else if(OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "add current row fail:ret[%d]", ret);
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    if(!ups_row_valid_)
    {
      ups_row_.reuse();
      str_buf_.reset();
      cur_row_version_.reset(); //add shili, [MultiUPS] [COLUMN_VERSION_MERGE]  20170423:b
    }
    // add current cell to compactor;
    if (OB_SUCCESS != (ret = ObNewScannerHelper::add_ups_cell(ups_row_, cell_info, &str_buf_)))
    {
      YYSYS_LOG(WARN, "cannot add current cell to compactor, "
                "cur_size_counter_=%ld, last_table_id_=%ld, cur table=%ld, rowkey=%s",
                cur_size_counter_, last_table_id_, cell_info.table_id_, to_cstring(cell_info.row_key_));
    }
    //add shili, [MultiUPS] [COLUMN_VERSION_MERGE]  20170423:b
    else if(OB_SUCCESS!=(ret=ObNewScannerHelper::add_column_version(cur_row_version_,cell_info,data_version_)))
    {
      YYSYS_LOG(WARN, "fail to add column version,ret=%d",ret);
    }
    //add e
    else
    {
      ups_row_valid_ = true;
    }
  }

  if(OB_SUCCESS == ret)
  {
    if (row_changed)
    {
      last_table_id_ = cell_info.table_id_;
      cell_info.row_key_.deep_copy(cur_rowkey_, rowkey_allocator_);
    }
  }
  return ret;
}

int ObCellNewScanner::finish()
{
  int ret = OB_SUCCESS;
  if(ups_row_valid_ && !is_size_overflow_)
  {
    ret = add_current_row();
    if(OB_SIZE_OVERFLOW == ret)
    {
      is_size_overflow_ = true;
      ret = OB_SUCCESS;
    }
    else if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "add current row fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = set_is_req_fullfilled(!is_size_overflow_, get_row_num())))
    {
      YYSYS_LOG(WARN, "set is req fullfilled fail:ret[%d]", ret);
    }
  }
  return ret;
}

void ObCellNewScanner::set_row_desc(const ObRowDesc &row_desc)
{
  row_desc_ = &row_desc;
  ups_row_.set_row_desc(*row_desc_);
}


int ObCellNewScanner::add_current_row()
{
  int ret = OB_SUCCESS;
  if(!ups_row_valid_)
  {
    YYSYS_LOG(WARN, "ups row is invalid");
    ret = OB_ERR_UNEXPECTED;
  }
  else if (NULL == row_desc_)
  {
    YYSYS_LOG(WARN, "set row desc first.");
    ret = OB_INVALID_ARGUMENT;
  }
  else if (ups_row_.get_column_num() <= 0)
  {
    YYSYS_LOG(WARN, "current row has no cell");
  }
  //add by maosy  [MultiUPS 1.0][secondary index optimize]20170401 b:
  YYSYS_LOG(DEBUG,"TEST= %s,is del = %d,is nop = %d",to_cstring(ups_row_), ups_row_.get_is_delete_row(),ups_row_.get_is_all_nop());
  if(OB_SUCCESS ==ret /*&& !ups_row_.get_is_delete_row()*/)
  {
    YYSYS_LOG(DEBUG,"ROW key = %s,row_key = %s",to_cstring(rowkey_info_),to_cstring(cur_rowkey_));
    YYSYS_LOG(DEBUG,"TEST= %s,is del = %d,is nop = %d",to_cstring(ups_row_), ups_row_.get_is_delete_row(),ups_row_.get_is_all_nop());
    YYSYS_LOG(DEBUG,"rowkey_info_.get_size() %ld", rowkey_info_.get_size());
    for(int64_t i = 0 ; i < rowkey_info_.get_size();i++)
    {
      ObObj * cell =NULL;
      uint64_t cid = OB_INVALID_ID ;
      rowkey_info_.get_column_id(i,cid);
      YYSYS_LOG(DEBUG,"tid %lu, cid %lu", last_table_id_, cid);
      if(OB_SUCCESS !=(ret = ups_row_.get_cell(last_table_id_,cid,cell)))
      {
        //todo ,ԭ�������������п����ò�����
        YYSYS_LOG(WARN,"failed to get cell ,tid = %lu,cid = %lu,row = %s", last_table_id_,cid,to_cstring(ups_row_));
        ret = OB_SUCCESS ;
        continue ;
      }
      //mod hongchen [SKEW_INDEX_COLUMN_BUGFIX] 20170801:b
      //to shield ERROR log
      //else if(cell->compare((cur_rowkey_.ptr()[i])) !=0)
      else if(!(cell->can_compare(cur_rowkey_.ptr()[i]))
              || (cell->compare((cur_rowkey_.ptr()[i])) !=0))
      {
        //mod hongchen [SKEW_INDEX_COLUMN_BUGFIX] 20170801:e
        YYSYS_LOG(DEBUG,"ROW key = %s",to_cstring(cur_rowkey_));
        ups_row_.set_is_delete_row(true);
        int64_t data_version ;
        int64_t index ;
        if(OB_SUCCESS != (ret = ups_row_.reset()))
        {
          YYSYS_LOG(WARN,"failed to reset ups_row");
        }
        else if(OB_SUCCESS !=(ret = cur_row_version_.get_column_version(cid,data_version,index)))
        {
          YYSYS_LOG(WARN,"failed to get_column vesion");
        }
        else
        {
          //add hongchen [SKEW_INDEX_COLUMN_BUGFIX] 20170801:b
          YYSYS_LOG(DEBUG,"date version = %ld",data_version);
          if (OB_INVALID_VERSION == data_version && cur_row_version_.is_deleted_row())
          {
            data_version = cur_row_version_.get_delete_version();
          }
          //add hongchen [SKEW_INDEX_COLUMN_BUGFIX] 20170801:e
          YYSYS_LOG(DEBUG,"date version = %ld",data_version);
          cur_row_version_.reset();
          cur_row_version_.add_column_version(OB_DELETED_FLAG_COLUMN_ID,data_version);
        }
        break;
      }
    }
    YYSYS_LOG(DEBUG,"TEST= %s",to_cstring(ups_row_));
  }
  // add by maosy e

  //modify for output
  ObUpsRow final_ups_row;
  if (OB_SUCCESS == ret && NULL != final_row_desc_)
  {
    //modify ups_row_
    final_ups_row.set_row_desc(*final_row_desc_);
    for (int64_t index = 0; OB_SUCCESS == ret && index < final_row_desc_->get_column_num(); ++index)
    {
      uint64_t table_id  = OB_INVALID_ID;
      uint64_t column_id = OB_INVALID_ID;
      common::ObObj* cell = NULL;
      if (OB_SUCCESS != (ret = final_row_desc_->get_tid_cid(index, table_id, column_id)))
      {
        YYSYS_LOG(WARN, "fail to get tid cid from final_row_desc_, ret=%d", ret);
        break;
      }
      else if (OB_SUCCESS != (ret = ups_row_.get_cell(table_id, column_id, cell)))
      {
        YYSYS_LOG(WARN, "fail to get cell from ups_row_, ret=%d", ret);
        break;
      }
      else if (OB_SUCCESS != (ret = final_ups_row.set_cell(table_id, column_id, *cell)))
      {
        YYSYS_LOG(WARN, "fail to set cell for final_ups_row, ret=%d", ret);
        break;
      }
    }
    if (OB_SUCCESS == ret)
    {
      final_ups_row.set_is_all_nop(ups_row_.get_is_all_nop());
      final_ups_row.set_is_delete_row(ups_row_.get_is_delete_row());
    }
    //modify cur_row_version_
    if (OB_SUCCESS == ret)
    {
      if (!ups_row_.get_is_delete_row())
      {
        ObRowVersion final_row_version;
        final_row_version.column_num_ = final_row_desc_->get_column_num();
        for(int64_t index = 0; OB_SUCCESS == ret && index < final_row_desc_->get_column_num(); ++index)
        {
          uint64_t table_id  = OB_INVALID_ID;
          uint64_t column_id = OB_INVALID_ID;
          int64_t  column_version = OB_INVALID_VERSION;
          int64_t  c_index = OB_INVALID_INDEX;
          if (OB_SUCCESS != (ret = final_row_desc_->get_tid_cid(index, table_id, column_id)))
          {
            YYSYS_LOG(WARN, "fail to get tid cid from final_row_desc_, ret=%d", ret);
            break;
          }
          else if (OB_SUCCESS != (ret = cur_row_version_.get_column_version(column_id, column_version, c_index)))
          {
            YYSYS_LOG(WARN, "fail to get version from cur_row_version_, ret=%d", ret);
            break;
          }
          else if (OB_SUCCESS != (ret = final_row_version.update_column_version(column_id, column_version)))
          {
            YYSYS_LOG(WARN, "fail to get version from cur_row_version_, ret=%d", ret);
            break;
          }
        }
        cur_row_version_ = final_row_version;
      }
    }
  }
  //add shili, [MultiUPS] [COLUMN_VERSION_MERGE]  20170423:b
  if(OB_SUCCESS == ret)
  {
    set_row_version_ptr(&cur_row_version_);
  }
  //add e

  if(OB_SUCCESS == ret)
  {
    if (NULL != final_row_desc_)
    {
      ret = add_row(cur_rowkey_, final_ups_row);
    }
    else
    {
      ret = add_row(cur_rowkey_, ups_row_);
    }
    if (OB_SUCCESS != ret && OB_SIZE_OVERFLOW != ret)
    {
      YYSYS_LOG(WARN, "add row to row_store error, ret=%d, last_row_key_=%s, cell num=%ld",
                ret, to_cstring(last_row_key_), ups_row_.get_column_num());
    }
  }
  if(OB_SUCCESS == ret)
  {
    last_row_key_ = cur_rowkey_;
    ups_row_valid_ = false;
  }
  return ret;
}

void ObCellNewScanner::clear()
{
  ObNewScanner::clear();
  row_desc_ = NULL;
  final_row_desc_ = NULL;
  last_table_id_ = OB_INVALID_ID;
  ups_row_valid_ = false;
  is_size_overflow_ = false;
}

void ObCellNewScanner::reuse()
{
  ObNewScanner::reuse();
  str_buf_.reset();
  row_desc_ = NULL;
  final_row_desc_ = NULL;
  last_table_id_ = OB_INVALID_ID;
  ups_row_valid_ = false;
  is_size_overflow_ = false;
}

//add shili, [MultiUPS] [COLUMN_VERSION_MERGE]  20170423
ObColumnVersion::ObColumnVersion()
{
  column_id_= OB_INVALID_ID;
  version_ = OB_INVALID_INDEX;
}

ObColumnVersion::ObColumnVersion(int64_t column_id,int64_t version):column_id_(column_id),version_(version)
{
}

int64_t ObColumnVersion::to_string(char* buffer, int64_t length) const
{
  int64_t pos = 0;
  databuff_printf(buffer, length, pos, "(%ld,%ld) ", column_id_,version_);
  return pos;
}

ObRowVersion::ObRowVersion()
{
  column_num_ = 0;
}

ObRowVersion& ObRowVersion::operator=(const ObRowVersion &other)
{
  reset();
  column_num_ = other.column_num_;
  for (int64_t i = 0; i <= column_num_; ++i)
  {
    column_versions_[i] = other.column_versions_[i];
  }
  return *this;
}

void ObRowVersion::reset()
{
  column_num_ = 0;
}

int64_t ObRowVersion::to_string(char *buffer, int64_t length) const
{
  int64_t pos = 0;
  for(int64_t i = 0; i < column_num_; ++i)
  {
    databuff_printf(buffer, length, pos, "<%ld:%s> ", i, to_cstring(column_versions_[i]));
  }
  return pos;
}



bool ObRowVersion::is_deleted_row() const
{
  return  (uint64_t)column_num_ == 1 &&  (uint64_t)column_versions_[0].column_id_ == OB_DELETED_FLAG_COLUMN_ID;
}

int64_t ObRowVersion::get_delete_version() const
{
  OB_ASSERT(is_deleted_row());
  return  column_versions_[0].version_;
}

//add for [212]-b
bool ObRowVersion::is_nop_version() const
{
  bool is_nop = true;
  for (int i = 1; i < column_num_; i++)
  {
    if ((uint64_t) column_versions_[i].column_id_ != OB_NOP_FLAG_COLUMN_ID)
    {
      is_nop = false;
    }
  }
  return is_nop;
}
//add for [212]-e

/*@berif ��ȡ column_id ��column_version�����������û��column_id ��version ΪOB_INVALID_VERSION indexΪOB_INVALID_INDEX*/
int  ObRowVersion::get_column_version(const int64_t column_id, int64_t &version, int64_t &index) const
{
  int ret = OB_SUCCESS;
  version = OB_INVALID_VERSION;
  index = OB_INVALID_INDEX;
  for(int64_t i = 0; i < column_num_; i++)
  {
    const ObColumnVersion &column_version = column_versions_[i];
    if(column_version.column_id_ == column_id)
    {
      index = i;
      version = column_version.version_;
      break;
    }
  }
  return ret;
}

/*@berif �����µ�column_version */
int  ObRowVersion::add_column_version(const int64_t column_id, const int64_t version)
{
  int ret = OB_SUCCESS;
  column_versions_[column_num_].column_id_ = column_id;
  column_versions_[column_num_].version_ = version;
  ++column_num_;
  return ret;
}

/*@berif ����column��version,����������������µ�column_version*/
int  ObRowVersion::update_column_version(const int64_t column_id, const int64_t version,
                                         const int64_t index/*=OB_INVALID_INDEX*/)
{
  int ret = OB_SUCCESS;
  int64_t local_index = index;
  if(OB_INVALID_INDEX == local_index) //��Ҫ������index
  {
    for(int64_t i = 0; i < column_num_; i++)
    {
      ObColumnVersion &column_version = column_versions_[i];
      if(column_version.column_id_ == column_id)
      {
        local_index = i;
        break;
      }
    }
  }

  if(OB_SUCCESS == ret)
  {
    if(OB_INVALID_INDEX == local_index) //��û�и�column,����version���ӽ�ȥ
    {
      if(OB_SUCCESS != (ret = add_column_version(column_id, version)))
      {
        YYSYS_LOG(WARN, "add column version fail,ret = %d", ret);
      }
    }
    else
    {
      ObColumnVersion &column_version = column_versions_[local_index];
      if(version <= column_version.version_)
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(WARN, "new version is not greater than local verison,ret = %d", ret);
      }
      else
      {
        column_version.version_ = version;
      }
    }
  }
  return ret;
}

int ObRowVersion::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  ObObj count_obj;
  count_obj.set_int(column_num_);
  if(OB_SUCCESS != (ret = count_obj.serialize(buf, buf_len, pos)))
  {
    YYSYS_LOG(WARN, "fail to serialize expr count. ret=%d", ret);
  }
  else
  {
    for(int64_t i = 0; i < column_num_; ++i)
    {
      const ObColumnVersion &version = column_versions_[i];
      if(ret == OB_SUCCESS && (OB_SUCCESS != (ret = version.serialize(buf, buf_len, pos))))
      {
        YYSYS_LOG(WARN, "serialize fail. ret=%d", ret);
        break;
      }
    } // end for
  }
  return ret;
}

int ObRowVersion::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  ObColumnVersion version;
  int64_t count = 0;
  reset();

  if(OB_SUCCESS != (ret = obj.deserialize(buf, data_len, pos)))
  {
    YYSYS_LOG(WARN, "fail to deserialize obj. ret=%d", ret);
  }
  else if(OB_SUCCESS != (ret = obj.get_int(count)))
  {
    YYSYS_LOG(WARN, "fail to get row version. ret=%d", ret);
  }
  else
  {
    column_num_ = count;
  }

  for(int64_t i = 0; OB_SUCCESS == ret && i < column_num_; i++)
  {
    if(OB_SUCCESS != (ret = version.deserialize(buf, data_len, pos)))
    {
      YYSYS_LOG(DEBUG, "fail to add expr to project ret=%d. buf=%p, data_len=%ld, pos=%ld", ret, buf, data_len, pos);
    }
    else
    {
      column_versions_[i] = version;
    }
  }
  return ret;
}

int64_t ObRowVersion::get_serialize_size(void) const
{
  int64_t size = 0;
  ObObj obj;
  obj.set_int(column_num_);
  size += obj.get_serialize_size();

  for(int64_t i = 0; i < column_num_; ++i)
  {
    const ObColumnVersion &version = column_versions_[i];
    size += version.get_serialize_size();
  }
  return size;
}

int ObColumnVersion::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int err = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_SUCCESS != (err = serialization::encode_i64(buf, buf_len, new_pos, column_id_)))
  {
    YYSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
  }
  else if (OB_SUCCESS != (err = serialization::encode_i64(buf, buf_len, new_pos, version_)))
  {
    YYSYS_LOG(ERROR, "serialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, buf_len, err);
  }
  else
  {
    pos = new_pos;
  }
  return err;
}

int ObColumnVersion::deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int err = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_SUCCESS != (err = serialization::decode_i64(buf, data_len, new_pos, &column_id_)))
  {
    YYSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
  }
  else if (OB_SUCCESS != (err = serialization::decode_i64(buf, data_len, new_pos, &version_)))
  {
    YYSYS_LOG(ERROR, "deserialize(buf=%p[%ld-%ld])=>%d", buf, new_pos, data_len, err);
  }
  else
  {
    pos = new_pos;
  }
  return err;
}

int64_t ObColumnVersion::get_serialize_size(void) const
{
  return serialization::encoded_length_i64(column_id_)
      + serialization::encoded_length_i64(version_);
}
//add e
