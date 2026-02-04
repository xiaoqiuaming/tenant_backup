/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_expr_values.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_expr_values.h"
#include "ob_duplicate_indicator.h"
#include "common/utility.h"
#include "common/ob_obj_cast.h"
#include "common/hash/ob_hashmap.h"
//add dragon [varchar limit] 2016-8-12 12:09:42
#include "common/ob_schema.h"
#include "mergeserver/ob_merge_server_main.h"
#include "mergeserver/ob_merge_server.h"
//add e
using namespace oceanbase::sql;
using namespace oceanbase::common;
//add dragon [varchar limit] 2016-8-15 09:00:18
using namespace oceanbase::mergeserver;
//add e

ObExprValues::ObExprValues()
  :values_(OB_TC_MALLOC_BLOCK_SIZE, ModulePageAllocator(ObModIds::OB_SQL_ARRAY)),
   from_deserialize_(false),
   check_rowkey_duplicat_(false),
   check_rowkey_duplicat_rep_(false),//add hushuang[Secondary Index]for replace bug:20161103
   do_eval_when_serialize_(false),
   //add wenghaixing[decimal] for fix delete bug 2014/10/10
   is_del_update(false)
   //add e
   , row_num_(0), from_sub_query_(false) //add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140715
   , from_ud_(false) //add gaojt [Delete_Update_Function] [JHOBv0.1] 20151219
   //add dragon [varchar limit] 2016-8-15 12:50:22
   , manager_(NULL)
   //add e
   ,had_evaled_(false)  //add shili [MERGE_BUG_FIX]  20170210
 ,cur_paxosi_id_  (OB_INVALID_PAXOS_ID)// add by maosy [MultiUps 1.0] [batch_udi] 20170417
{
}

ObExprValues::~ObExprValues()
{
}

//add dragon [varchar limit] 2016-8-12 10:53:57
int ObExprValues::init ()
{
  int ret = OB_SUCCESS;
  manager_ = NULL;
  //1.get mergeserver
  ObMergeServerMain *ins = ObMergeServerMain::get_instance ();

  if(NULL == ins)
  {
    ret = OB_ERROR;
    YYSYS_LOG(ERROR, "get ms instance failed");
  }
  else
  {
    //2.get merge schema manager
    manager_ = ins->get_merge_server ().get_schema_mgr();
    if(NULL == manager_)
    {
      ret = OB_ERR_NULL_POINTER;
      YYSYS_LOG(ERROR, "get manager failed");
    }
  }
  return ret;
}
//add e

//add wenghaixing[decimal] for fix delete bug 2014/10/10
void ObExprValues::set_del_upd()
{
    is_del_update=true;
}
//add e

void ObExprValues::reset()
{
  row_desc_.reset();
  row_desc_ext_.reset();
  values_.clear();
  row_store_.clear();
  //row_.reset(false, ObRow::DEFAULT_NULL);
  from_deserialize_ = false;
  check_rowkey_duplicat_ = false;
  check_rowkey_duplicat_rep_ = false;//add hushuang[Secondary Index]for replace bug:20161103
  do_eval_when_serialize_ = false;
   //add wenghaixing[decimal] for fix delete bug 2014/10/10
   is_del_update=false;
   //add e
  //add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140715:b
  result_row_store_.clear();
  row_num_ = 0;
  from_sub_query_ = false;
  //add 20140715:e
  //add gaojt [Delete_Update_Function] [JHOBv0.1] 20151219:b
  from_ud_ = false;
  //add gaojt 20151219:e
  had_evaled_ = false; //add shili [MERGE_BUG_FIX]  20170210
  // add by maosy [MultiUps 1.0] [batch_udi] 20170417 b
  for (int i = 0 ; i < MAX_UPS_COUNT_ONE_CLUSTER ;i++)
  {
      if(bit_paxos_.has_member(i))
      {
          batch_row_stores_[i].clear();
      }
  }
  bit_paxos_.clear();
  cur_paxosi_id_ = OB_INVALID_PAXOS_ID;
  paxos_ids_.clear();
  // add by maosy 20170417 e
}
//add shili [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20150707:b
//@berif 重置 row_store_中的值
void ObExprValues::row_store_reuse()
{
  row_store_.reuse();
}
//add 20150707:e

void ObExprValues::reuse()
{
  row_desc_.reset();
  row_desc_ext_.reset();
  values_.clear();
  row_store_.clear();
  //row_.reset(false, ObRow::DEFAULT_NULL);
  from_deserialize_ = false;
  check_rowkey_duplicat_ = false;
  check_rowkey_duplicat_rep_ = false;//add hushuang[Secondary Index]for replace bug:20161103
  do_eval_when_serialize_ = false;
    //add wenghaixing[decimal] for fix delete bug 2014/10/10
   is_del_update=false;
    //add e
  //add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140715:b
  result_row_store_.clear();
  row_num_ = 0;
  from_sub_query_ = false;
  //add 20140715:e
  //add gaojt [Delete_Update_Function] [JHOBv0.1] 20151219:b
  from_ud_ = false;
  //add gaojt 20151219:e
  had_evaled_ = false;//add shili [MERGE_BUG_FIX]  20170210:b
  // add by maosy [MultiUps 1.0] [batch_udi] 20170417 b
  for (int i = 0 ; i < MAX_UPS_COUNT_ONE_CLUSTER ;i++)
  {
      if(bit_paxos_.has_member(i))
      {
          batch_row_stores_[i].clear();
      }
  }
  bit_paxos_.clear();
  cur_paxosi_id_ = OB_INVALID_PAXOS_ID;
  paxos_ids_.clear();
  // add by maosy 20170417 e
}

int ObExprValues::set_row_desc(const common::ObRowDesc &row_desc, const common::ObRowDescExt &row_desc_ext)
{
  row_desc_ = row_desc;
  row_desc_ext_ = row_desc_ext;
  return OB_SUCCESS;
}

int ObExprValues::add_value(const ObSqlExpression &v)
{
  int ret = OB_SUCCESS;
  if ((ret = values_.push_back(v)) == OB_SUCCESS)
  {
    values_.at(values_.count() - 1).set_owner_op(this);
  }
  return ret;
}

int ObExprValues::open()
{
  int ret = OB_SUCCESS;
  if (from_deserialize_)
  {
    if(!had_evaled_)//add shili [MERGE_BUG_FIX]  20170210:b
    {
      row_store_.reset_iterator();
    }
    row_.set_row_desc(row_desc_);
    // pass
  }
  else if (0 >= row_desc_.get_column_num()
      || 0 >= row_desc_ext_.get_column_num())
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(WARN, "row_desc not init");
  }
  //add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140715:b
  /*Exp:sub_query branch,different with normal values*/
  else if(from_sub_query_)
  {
    row_.set_row_desc(row_desc_);
  }
  //add 20140715:e
  //add gaojt [Delete_Update_Function] [JHOBv0.1] 20151219:b
  else if(from_ud_)
  {
    row_.set_row_desc(row_desc_);
  }
  //add gaojt 20151219:e
  else if (0 >= values_.count())
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(WARN, "values not init");
  }
  else
  {
    row_.set_row_desc(row_desc_);
    if(!had_evaled_) //add shili [MERGE_BUG_FIX]  20170210
    {
      row_store_.reuse();
      if (OB_SUCCESS != (ret = eval()))
      {
        YYSYS_LOG(WARN, "failed to eval exprs, err=%d", ret);
      }
    }
  }
  return ret;
}

int ObExprValues::close()
{
  if (from_deserialize_)
  {
    row_store_.reset_iterator();
  }
  else
  {
    row_store_.reuse();
  }
  //add by maosy [MultiUPS 1.0] [iud]20170525 b:
  if(cur_paxosi_id_ != OB_INVALID_PAXOS_ID)
  {
      if (from_deserialize_)
      {
          batch_row_stores_[cur_paxosi_id_].reset_iterator();
      }
      else
      {
          batch_row_stores_[cur_paxosi_id_].reuse();
      }
  }
  // add by maosy e
  return OB_SUCCESS;
}

//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140911:b
/*Exp: reset the environment*/
int ObExprValues::reset_stuff_for_insert()
{
  int ret = OB_SUCCESS;
  result_row_store_.clear();
  row_num_ = 0;
  // add by maosy [MultiUps 1.0] [batch_udi] 20170417
  for (int i = 0 ; i < MAX_UPS_COUNT_ONE_CLUSTER ;i++)
  {
      if(bit_paxos_.has_member(i))
      {
          batch_row_stores_[i].clear();
      }
  }
  bit_paxos_.clear();
  paxos_ids_.clear();
  had_evaled_ =false;
  // add by maosy e
  return ret;
}
//add gaojt 20151204:e
int ObExprValues::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  //add by maosy [MultiUPS 1.0] [batch_iud]20170525 b:
  if(cur_paxosi_id_ == OB_INVALID_PAXOS_ID)//no partition
  {
  // add by maosy e 
      if (OB_SUCCESS != (ret = row_store_.get_next_row(row_)))
      {
          if (OB_ITER_END != ret)
          {
              YYSYS_LOG(WARN, "failed to get next row from row store, err=%d", ret);
          }
      }
      //add by maosy [MultiUPS 1.0] [batch_iud]20170525 b:
  }
  else // if partition ,find row in batch row store
  {
      if (OB_SUCCESS != (ret = batch_row_stores_[cur_paxosi_id_].get_next_row(row_)))
      {
        if (OB_ITER_END != ret)
        {
          YYSYS_LOG(WARN, "failed to get next row from row store, err=%d", ret);
        }
      }
  }
  if(OB_SUCCESS !=ret)
  {}
  // add by maosy e
  else
  {
    row = &row_;
  }
  return ret;
}

int ObExprValues::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  row_desc = &row_desc_;
  return OB_SUCCESS;
}

//add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160321:b
int ObExprValues::get_row_desc_ext(const common::ObRowDescExt *&row_desc_ext) const
{
  row_desc_ext = &row_desc_ext_;
  return OB_SUCCESS;
}

void ObExprValues::clear_values(void)
{
  values_.clear();
}

void ObExprValues::reset_row_store_iterator(void)
{
  row_store_.reset_iterator();
}
//add 20160321:e
namespace oceanbase
{
  namespace sql
  {
    REGISTER_PHY_OPERATOR(ObExprValues, PHY_EXPR_VALUES);
  }
}

int64_t ObExprValues::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ExprValues(values_num=%ld, values=",
                  values_.count());
  pos += values_.to_string(buf+pos, buf_len-pos);
  databuff_printf(buf, buf_len, pos, ", row_desc=");
  pos += row_desc_.to_string(buf+pos, buf_len-pos);
  databuff_printf(buf, buf_len, pos, ")\n");
  return pos;
}


//add shili [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20150706:b
void ObExprValues::dump()
{
  YYSYS_LOG(DEBUG, "exprvalues print start");
  YYSYS_LOG(DEBUG, "row_store :%s",to_cstring(row_store_));
  YYSYS_LOG(DEBUG, "row_desc :%s",to_cstring(row_desc_));
  YYSYS_LOG(DEBUG, "exprvalues print end");
}


//@berif 向row_store_ 添加一行数据
int ObExprValues::add_values(ObRow & val_row)
{
  int ret=OB_SUCCESS;
  const ObRowStore::StoredRow *stored_row = NULL;
  val_row.set_row_desc(row_desc_);
  if(OB_LIKELY(OB_SUCCESS ==ret))
  {
    if (OB_SUCCESS != (ret = row_store_.add_row(val_row, stored_row)))
    {
      YYSYS_LOG(WARN, "failed to add row into store, err=%d", ret);
    }
  }
  return ret;
}
//add 20150706:e


//add shili [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20150705:b
//@berif 将row 深拷贝并添加到row_store_array
//row [in] 将拷贝的 row
//row_store_array [in out] 被添加的数组
//row_mem_allocator [in]  用于分配内存
int ObExprValues::add_row_deep_copy(const ObRow &row,ObArray<ObRow> &row_store_array,common::ModuleArena & row_mem_allocator)
{
  int ret = OB_SUCCESS;
  char *ind_buf = NULL;
  ind_buf = row_mem_allocator.alloc(sizeof(ObRow));
  if(NULL == ind_buf)
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    YYSYS_LOG(WARN, "No memory");
  }
  else
  {
    ObRow *row_ptr = new(ind_buf) ObRow();
    if(!row_ptr)
    {
      ret = OB_ERROR;
      YYSYS_LOG(WARN, "new ObRow fail");
    }
    else if(OB_SUCCESS != (ret = row.deep_copy(*row_ptr, row_mem_allocator)))
    {
      YYSYS_LOG(WARN, "deep copy fail,ret=%d",ret);
    }
    else
    {
      ret = row_store_array.push_back(*row_ptr);  //将重新分配内存的row加入到 row_store_array中
      if(OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "push back row into row_store fail,ret=%d", ret);
      }
    }
  }
  return ret;
}
//add 20150705:e
//add shili [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20150705:b
//@berif  将values_中的值进行计算，并形成行，深拷贝添加到row_store_array中
int ObExprValues::eval(ObArray<ObRow> &row_store_array,common::ModuleArena & row_mem_allocator)
{
  //YYSYS_LOG(DEBUG,"eval1()");
  int ret = OB_SUCCESS;
  /*
  OB_ASSERT(0 < values_.count());
  OB_ASSERT(0 < row_desc_.get_column_num());
  OB_ASSERT(0 == (values_.count() % row_desc_.get_column_num()));
  ModuleArena buf(OB_COMMON_MEM_BLOCK_SIZE, ModulePageAllocator(ObModIds::OB_SQL_TRANSFORMER));
  char* varchar_buff = NULL;
  if (NULL == (varchar_buff = buf.alloc(OB_MAX_VARCHAR_LENGTH)))
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    YYSYS_LOG(WARN, "No memory");
  }
  else
  {
    int64_t col_num = row_desc_.get_column_num();
    int64_t row_num = values_.count() / col_num;
    if (row_num >= 1)
    {
      if (row_desc_.get_rowkey_cell_count() <= 0)
      {
        ret = OB_ERROR;
        YYSYS_LOG(WARN, "RowKey is empty, ret=%d", ret);
      }
    }
    for (int64_t i = 0; OB_SUCCESS == ret && i < values_.count(); i+=col_num) // for each row
    {
      ObRow val_row;
      val_row.set_row_desc(row_desc_);
      ObString varchar;
      ObObj casted_cell;
      for (int64_t j = 0; OB_SUCCESS == ret && j < col_num; ++j)
      {
        varchar.assign_ptr(varchar_buff, OB_MAX_VARCHAR_LENGTH);
        casted_cell.set_varchar(varchar); // reuse the varchar buffer
        const ObObj *single_value = NULL;
        uint64_t table_id = OB_INVALID_ID;
        uint64_t column_id = OB_INVALID_ID;
        ObObj tmp_value;
        ObObj data_type;
        ObSqlExpression &val_expr = values_.at(i+j);
        if ((ret = val_expr.calc(val_row, single_value)) != OB_SUCCESS) // the expr should be a const expr here
        {
          YYSYS_LOG(WARN, "Calculate value result failed, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = row_desc_ext_.get_by_idx(j, table_id, column_id, data_type)))
        {
          ret = OB_ERR_UNEXPECTED;
          YYSYS_LOG(WARN, "Failed to get column, err=%d", ret);
        }
        else
        {
          if(is_del_update)
          {
            if (OB_SUCCESS != obj_cast(*single_value, data_type, casted_cell, single_value))
            {
              YYSYS_LOG(DEBUG, "failed to cast obj, err=%d", ret);
            }
          }
          else
          {
            if (OB_SUCCESS != (ret = obj_cast(*single_value, data_type, casted_cell, single_value)))
            {
              YYSYS_LOG(WARN, "failed to cast obj, err=%d", ret);
            }
          }

        }
        if(OB_SUCCESS!=ret)
        {
        }
        else if (OB_SUCCESS != (ret = ob_write_obj_v2(buf, *single_value, tmp_value)))
        {
          YYSYS_LOG(WARN, "str buf write obj fail:ret[%d]", ret);
        }
        else if ((ret = val_row.set_cell(table_id, column_id, tmp_value)) != OB_SUCCESS)
        {
          YYSYS_LOG(WARN, "Add value to ObRow failed");
        }
        else
        {
          YYSYS_LOG(DEBUG, "i=%ld j=%ld cell=%s", i, j, to_cstring(tmp_value));
        }
      } // end for column
      if (OB_LIKELY(OB_SUCCESS == ret))
      {
        if (OB_SUCCESS != (ret = add_row_deep_copy(val_row,row_store_array,row_mem_allocator)))
        {
          YYSYS_LOG(WARN, "failed to add row into store, err=%d", ret);
        }
      }
    }// end for row
  }
  if(OB_SUCCESS==ret)
  {
    had_evaled_ = true;
  }*/
//add by maosy  [MultiUPS 1.0][secondary index optimize]20170624 b:
//物理计划分发之前已经open过了，
  ObRow val_row;
  val_row.set_row_desc(row_desc_);
  while(OB_SUCCESS == (ret = row_store_.get_next_row(val_row)))
  {
    if (OB_SUCCESS != (ret = add_row_deep_copy(val_row,row_store_array,row_mem_allocator)))
    {
      YYSYS_LOG(WARN, "failed to add row into store, err=%d", ret);
    }
  }
  if( OB_ITER_END == ret )
    ret = OB_SUCCESS ;

  //add shili [MERGE_BUG_FIX]  20170210:b
  if(OB_SUCCESS==ret)
  {
    had_evaled_ = true;
  }
  //add e
//add by maosy e
  return ret;
}
//add 20150705:e
/*
// add by maosy
int ObExprValues::eval_for_ud(ObArray<ObRow> &row_store_array, ModuleArena &row_mem_allocator)
{
  //YYSYS_LOG(DEBUG,"eval_for_ud()");
  int ret = OB_SUCCESS;
  ObRow val_row;
  val_row.set_row_desc(row_desc_);
  while(OB_SUCCESS == (ret = row_store_.get_next_row(val_row)))
  {
    if (OB_SUCCESS != (ret = add_row_deep_copy(val_row,row_store_array,row_mem_allocator)))
    {
      YYSYS_LOG(WARN, "failed to add row into store, err=%d", ret);
    }
  }
  if( OB_ITER_END == ret )
    ret = OB_SUCCESS ;

  //add shili [MERGE_BUG_FIX]  20170210:b
  if(OB_SUCCESS==ret)
  {
    had_evaled_ = true;
  }
  //add e
  return ret ;
}

// add e
*/
int ObExprValues::eval()
{
  //YYSYS_LOG(DEBUG,"eval2()");
  int ret = OB_SUCCESS;
  OB_ASSERT(0 < values_.count());
  OB_ASSERT(0 < row_desc_.get_column_num());
  OB_ASSERT(0 == (values_.count() % row_desc_.get_column_num()));
  ModuleArena buf(OB_COMMON_MEM_BLOCK_SIZE, ModulePageAllocator(ObModIds::OB_SQL_TRANSFORMER));
  char* varchar_buff = NULL;
  if (NULL == (varchar_buff = buf.alloc(OB_MAX_VARCHAR_LENGTH)))
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    YYSYS_LOG(WARN, "No memory");
  }
  else
  {
    const ObRowStore::StoredRow *stored_row = NULL;
    ObDuplicateIndicator *indicator = NULL;
    ObDuplicateIndicator *indicator_rep = NULL;//add hushuang[Secondary Index]for replace bug:20161103
    int64_t col_num = row_desc_.get_column_num();
    int64_t row_num = values_.count() / col_num;
    // RowKey duplication checking doesn't need while 1 row only
    if (check_rowkey_duplicat_ && row_num > 1)
    {
      void *ind_buf = NULL;
      if (row_desc_.get_rowkey_cell_count() <= 0)
      {
        ret = OB_ERROR;
        YYSYS_LOG(WARN, "RowKey is empty, ret=%d", ret);
      }
      else if ((ind_buf = buf.alloc(sizeof(ObDuplicateIndicator))) == NULL)
      {
        ret = OB_ERROR;
        YYSYS_LOG(WARN, "Malloc ObDuplicateIndicator failed, ret=%d", ret);
      }
      else
      {
        indicator = new (ind_buf) ObDuplicateIndicator();
        if ((ret = indicator->init(row_num)) != OB_SUCCESS)
        {
          YYSYS_LOG(WARN, "Init ObDuplicateIndicator failed, ret=%d", ret);
        }
      }
    }
    //add by hushuang [Secondary Index] for replace bug:20161103
    if (check_rowkey_duplicat_rep_ && row_num > 1)
    {
      void *ind_buf = NULL;
      if (row_desc_.get_rowkey_cell_count() <= 0)
      {
        ret = OB_ERROR;
        YYSYS_LOG(WARN, "RowKey is empty, ret=%d", ret);
      }
      else if ((ind_buf = buf.alloc(sizeof(ObDuplicateIndicator))) == NULL)
      {
        ret = OB_ERROR;
        YYSYS_LOG(WARN, "Malloc ObDuplicateIndicator_Rep failed, ret=%d", ret);
      }
      else
      {
        indicator_rep = new (ind_buf) ObDuplicateIndicator();
        if ((ret = indicator_rep->init(row_num)) != OB_SUCCESS)
        {
          YYSYS_LOG(WARN, "Init ObDuplicateIndicator_Rep failed, ret=%d", ret);
        }
      }

      for (int64_t i = values_.count() - col_num ;check_rowkey_duplicat_rep_ && OB_SUCCESS == ret && i >= 0; i-=col_num)//for each row
      {
        ObRow val_row;
        val_row.set_row_desc(row_desc_);
        ObString varchar;
        ObObj casted_cell;
        for (int64_t j = 0; OB_SUCCESS == ret && j < col_num; ++j)
        {
          varchar.assign_ptr(varchar_buff, OB_MAX_VARCHAR_LENGTH);
          casted_cell.set_varchar(varchar); // reuse the varchar buffer
          const ObObj *single_value = NULL;
          uint64_t table_id = OB_INVALID_ID;
          uint64_t column_id = OB_INVALID_ID;
          ObObj tmp_value;
          ObObj data_type;
          ObSqlExpression &val_expr = values_.at(i+j);
          if ((ret = val_expr.calc(val_row, single_value)) != OB_SUCCESS) // the expr should be a const expr here
          {
            YYSYS_LOG(WARN, "Calculate value result failed, err=%d", ret);
          }
          else if (OB_SUCCESS != (ret = row_desc_ext_.get_by_idx(j, table_id, column_id, data_type)))
          {
            ret = OB_ERR_UNEXPECTED;
            YYSYS_LOG(WARN, "Failed to get column, err=%d", ret);
          }
          else
          {
            if(is_del_update)
            {
              if (OB_SUCCESS != obj_cast(*single_value, data_type, casted_cell, single_value))
              {
                YYSYS_LOG(DEBUG, "failed to cast obj, err=%d", ret);
              }
            }
            else
            {
              if (OB_SUCCESS != (ret = obj_cast(*single_value, data_type, casted_cell, single_value)))
              {
                YYSYS_LOG(WARN, "failed to cast obj, err=%d", ret);
              }
            }
          }
          if(OB_SUCCESS!=ret)
          {
            if (OB_DECIMAL_UNLEGAL_ERROR == ret)
            {
              int64_t row_num = 0;
              row_num = i / col_num + 1;
              if (OB_SUCCESS != parse_decimal_illeagal_info(*single_value, table_id, column_id, row_num))
              {
                YYSYS_LOG(WARN, "failed to generate error info");
              }
            }
          }
          else if (OB_SUCCESS != (ret = ob_write_obj_v2(buf, *single_value, tmp_value)))
          {
            YYSYS_LOG(WARN, "str buf write obj fail:ret[%d]", ret);
          }
          else if ((ret = val_row.set_cell(table_id, column_id, tmp_value)) != OB_SUCCESS)
          {
            YYSYS_LOG(WARN, "Add value to ObRow failed");
          }
          else
          {
           //YYSYS_LOG(DEBUG, "i=%ld j=%ld cell=%s", i, j, to_cstring(tmp_value));
          }
        } // end for
        if (OB_LIKELY(OB_SUCCESS == ret))
        {
          if (indicator_rep)
          {
            const ObRowkey *rowkey = NULL;
            bool is_dup = false;
            if ((ret = val_row.get_rowkey(rowkey)) != OB_SUCCESS)
            {
              YYSYS_LOG(WARN, "Get RowKey failed, err=%d", ret);
            }
            else if ((ret = indicator_rep->have_seen(*rowkey, is_dup)) != OB_SUCCESS)
            {
              YYSYS_LOG(WARN, "Check duplication failed, err=%d", ret);
            }
            else if(is_dup)
            {
              //YYSYS_LOG(ERROR, "is_dup");
              ret = OB_SUCCESS;
              continue;
            }
          }
        }
        if (OB_LIKELY(OB_SUCCESS == ret))
        {
          if (OB_SUCCESS != (ret = row_store_.add_row(val_row, stored_row)))
          {
            YYSYS_LOG(WARN, "failed to add row into store, err=%d", ret);
          }
        }
      }//end for
      if (indicator_rep)
      {
        indicator_rep->~ObDuplicateIndicator();
      }
    }
    //add:e 20161103
    //add hushuang [Secondary Index] for replace bug:20161104
    else
   {//add:e
    for (int64_t i = 0; OB_SUCCESS == ret && i < values_.count(); i+=col_num) // for each row
    {
      ObRow val_row;
      val_row.set_row_desc(row_desc_);
      ObString varchar;
      ObObj casted_cell;
      for (int64_t j = 0; OB_SUCCESS == ret && j < col_num; ++j)
      {
        varchar.assign_ptr(varchar_buff, OB_MAX_VARCHAR_LENGTH);
        casted_cell.set_varchar(varchar); // reuse the varchar buffer
        const ObObj *single_value = NULL;
        uint64_t table_id = OB_INVALID_ID;
        uint64_t column_id = OB_INVALID_ID;
        ObObj tmp_value;
        ObObj data_type;
        ObSqlExpression &val_expr = values_.at(i+j);
        if ((ret = val_expr.calc(val_row, single_value)) != OB_SUCCESS) // the expr should be a const expr here
        {
          YYSYS_LOG(WARN, "Calculate value result failed, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = row_desc_ext_.get_by_idx(j, table_id, column_id, data_type)))
        {
          ret = OB_ERR_UNEXPECTED;
          YYSYS_LOG(WARN, "Failed to get column, err=%d", ret);
        }
        /*
        else if (0 < row_desc_.get_rowkey_cell_count()
                 && j < row_desc_.get_rowkey_cell_count()
                 && single_value->is_null())
        {
          YYSYS_LOG(USER_ERROR, "primary key can not be null");
          ret = OB_ERR_INSERT_NULL_ROWKEY;
        }
        */
         //modify wenghaixing[decimal] for fix delete bug 2014/10/10
        else
        {  
        if(is_del_update)
            {
                if (OB_SUCCESS != obj_cast(*single_value, data_type, casted_cell, single_value))
                {
                    YYSYS_LOG(DEBUG, "failed to cast obj, err=%d", ret);
                }
            }
            else
            {
                if (OB_SUCCESS != (ret = obj_cast(*single_value, data_type, casted_cell, single_value)))
                {
                        YYSYS_LOG(WARN, "failed to cast obj, err=%d", ret);
                }
            }

        }
        //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
        //else if (OB_SUCCESS != (ret = ob_write_obj(buf, *single_value, tmp_value))) old code
                //old code
        /*
           else if (OB_SUCCESS != (ret = obj_cast(*single_value, data_type, casted_cell, single_value)))
        {
          YYSYS_LOG(WARN, "failed to cast obj, err=%d", ret);
        }
        */
        //modify e
        if(OB_SUCCESS!=ret)
        {
          if (OB_DECIMAL_UNLEGAL_ERROR == ret)
          {
            row_num = i / col_num + 1;
            if (OB_SUCCESS != parse_decimal_illeagal_info(*single_value, table_id, column_id, row_num))
            {
              YYSYS_LOG(WARN, "failed to generate error info");
            }
          }
        }
        else if (OB_SUCCESS != (ret = ob_write_obj_v2(buf, *single_value, tmp_value)))
            //modify:e
        {
          YYSYS_LOG(WARN, "str buf write obj fail:ret[%d]", ret);
        }
        else if ((ret = val_row.set_cell(table_id, column_id, tmp_value)) != OB_SUCCESS)
        {
          YYSYS_LOG(WARN, "Add value to ObRow failed");
        }
        else
        {
          //YYSYS_LOG(DEBUG, "i=%ld j=%ld cell=%s", i, j, to_cstring(tmp_value));
        }
      } // end for
      if (OB_LIKELY(OB_SUCCESS == ret))
      {
        if (OB_SUCCESS != (ret = row_store_.add_row(val_row, stored_row)))
        {
          YYSYS_LOG(WARN, "failed to add row into store, err=%d", ret);
        }
        else if (indicator)
        {
          const ObRowkey *rowkey = NULL;
          bool is_dup = false;
          if ((ret = val_row.get_rowkey(rowkey)) != OB_SUCCESS)
          {
            YYSYS_LOG(WARN, "Get RowKey failed, err=%d", ret);
          }
          else if ((ret = indicator->have_seen(*rowkey, is_dup)) != OB_SUCCESS)
          {
            YYSYS_LOG(WARN, "Check duplication failed, err=%d", ret);
          }
          else if (is_dup)
          {
            ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
            YYSYS_LOG(USER_ERROR, "Duplicate entry \'%s\' for key \'PRIMARY\'", to_cstring(*rowkey));
          }
          YYSYS_LOG(DEBUG, "check rowkey isdup is %c rowkey=%s", is_dup?'Y':'N', to_cstring(*rowkey));
        }
      }
    }   // end for
  }
    if (indicator)
    {
      indicator->~ObDuplicateIndicator();
    }
  }
  //add shili [MERGE_BUG_FIX]  20170210:b
  if(OB_SUCCESS==ret)
  {
    had_evaled_ = true;
  }
  //add e
  return ret;
}

PHY_OPERATOR_ASSIGN(ObExprValues)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObExprValues);
  reset();
  row_desc_ = o_ptr->row_desc_;
  row_desc_ext_ = o_ptr->row_desc_ext_;

  values_.reserve(o_ptr->values_.count());
  for (int64_t i = 0; i < o_ptr->values_.count(); i++)
  {
    if ((ret = this->values_.push_back(o_ptr->values_.at(i))) == OB_SUCCESS)
    {
      this->values_.at(i).set_owner_op(this);
    }
    else
    {
      break;
    }
  }
  do_eval_when_serialize_ = o_ptr->do_eval_when_serialize_;
  check_rowkey_duplicat_ = o_ptr->check_rowkey_duplicat_;
  // Does not need to assign row_store_, because this function is used by MS only before opening
  return ret;
}

DEFINE_SERIALIZE(ObExprValues)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  if (do_eval_when_serialize_)
  {
    if (OB_SUCCESS != (ret = (const_cast<ObExprValues*>(this))->open()))
    {
      YYSYS_LOG(WARN, "failed to open expr_values, err=%d", ret);
    }
  }

  if (OB_LIKELY(OB_SUCCESS == ret))
  {
    if (OB_SUCCESS != (ret = row_desc_.serialize(buf, buf_len, tmp_pos)))
    {
      YYSYS_LOG(WARN, "serialize row_desc fail ret=%d buf=%p buf_len=%ld pos=%ld", ret, buf, buf_len, tmp_pos);
    }
    //add by maosy [MultiUPS 1.0] [batch_iud]20170525 b:
    else if(cur_paxosi_id_!= OB_INVALID_PAXOS_ID)
    {
        if (OB_SUCCESS != (ret = batch_row_stores_[cur_paxosi_id_].serialize(buf, buf_len, tmp_pos)))
        {
            YYSYS_LOG(WARN, "serialize row_store fail ret=%d buf=%p buf_len=%ld pos=%ld", ret, buf, buf_len, tmp_pos);
        }
    }
    // add  by  maosy e
    else if (OB_SUCCESS != (ret = row_store_.serialize(buf, buf_len, tmp_pos)))
    {
      YYSYS_LOG(WARN, "serialize row_store fail ret=%d buf=%p buf_len=%ld pos=%ld", ret, buf, buf_len, tmp_pos);
    }
    if(OB_SUCCESS !=ret)
    {}
    else
    {
      pos = tmp_pos;
    }
  }
  if (do_eval_when_serialize_)
  {
    (const_cast<ObExprValues*>(this))->close();
  }
  return ret;
}

DEFINE_DESERIALIZE(ObExprValues)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  if (OB_SUCCESS != (ret = row_desc_.deserialize(buf, data_len, tmp_pos)))
  {
    YYSYS_LOG(WARN, "serialize row_desc fail ret=%d buf=%p data_len=%ld pos=%ld", ret, buf, data_len, tmp_pos);
  }
  else if (OB_SUCCESS != (ret = row_store_.deserialize(buf, data_len, tmp_pos)))
  {
    YYSYS_LOG(WARN, "serialize row_store fail ret=%d buf=%p data_len=%ld pos=%ld", ret, buf, data_len, tmp_pos);
  }
  else
  {
    from_deserialize_ = true;
    pos = tmp_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObExprValues)
{
    // add by maosy [MultiUps 1.0] [batch_udi] 20170525 b
//  return (row_desc_.get_serialize_size() + row_store_.get_serialize_size());
    int64_t total_size = row_desc_.get_serialize_size();
    if(cur_paxosi_id_ != OB_INVALID_PAXOS_ID)
    {
        total_size += batch_row_stores_[cur_paxosi_id_].get_serialize_size();
    }
    else
    {
        total_size += row_store_.get_serialize_size();
    }
    return total_size ;
    // add by maosy 20170525 e
}
// add by maosy [MultiUps 1.0] [batch_udi] 20170417 b
//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140715:b
int ObExprValues::add_values_for_batch(const ObRow &value)
{
    int ret = OB_SUCCESS;
    const ObRowStore::StoredRow *stored_row = NULL;
    if(OB_SUCCESS !=(ret = result_row_store_.add_row(value, stored_row)))
    {
        YYSYS_LOG(WARN,"failed to add row to row_store,row = %s,ret = %d",to_cstring(value),ret );
    }
    return ret ;
}
/*Exp:add sub_query's result to values*/
//int ObExprValues::add_values_for_batch(const common::ObRow &value)
int ObExprValues::add_values( common::ObRow &value, bool is_table_level)
{
    const ObRowStore::StoredRow *stored_row = NULL;
    int ret = OB_SUCCESS;
    YYSYS_LOG(DEBUG,"PAOXS_ID =%ld,is_table=%d",value.get_paxos_id(),is_table_level);
    if(is_table_level)
    {
        if(OB_SUCCESS !=(ret = row_store_.add_row(value, stored_row)))
        {
            YYSYS_LOG(WARN,"failed to add row to row_store,row = %s,ret = %d",to_cstring(value),ret );
        }
    }
    else
    {
        if(OB_SUCCESS !=(ret = batch_row_stores_[value.get_paxos_id()].add_row(value,stored_row)))
        {
            YYSYS_LOG(WARN,"failed to add row to row_store,row = %s,ret = %d",to_cstring(value),ret );
        }
        else
        {
            int32_t paxos_id = (int32_t)(value.get_paxos_id());
            if(!bit_paxos_.has_member(paxos_id))
            {
                bit_paxos_.add_member(paxos_id);
            }
        }
    }
    return ret ;
}
/*
int ObExprValues::create_row_store_for_batch()
{
    int ret = OB_SUCCESS;
    if(cur_paxosi_id_ ==OB_INVALID_PAXOS_ID)
    {
        return ret ;
    }
    else
    {
        for(int64_t i = 0 ; i < batch_row_stores_.count() && OB_SUCCESS ==ret ;i++)
        {
            if(batch_row_stores_.at(i).paxos_id_ == cur_paxosi_id_)
            {
                YYSYS_LOG(DEBUG,"ROW STORE = %s",to_cstring(batch_row_stores_.at(i).row_store_));
                int64_t index =0 ;
                int64_t count = batch_row_stores_.at(i).row_store_.count();
                const ObRowStore::StoredRow *stored_row = NULL;
                row_store_.reuse();
                while (index < count  && ret == OB_SUCCESS)
                {
                    YYSYS_LOG(DEBUG,"ROW = %s",to_cstring(batch_row_stores_.at(i).row_store_.at(index)));
                    if(OB_SUCCESS !=ret )
                    {}
                    else if(OB_SUCCESS !=
                    (ret = row_store_.add_row(batch_row_stores_.at(i).row_store_.at(index),stored_row) ))
                    {
                        YYSYS_LOG(WARN,"failed to add row ,ret = %d",ret);
                    }
                    else
                    {
                        index ++ ;
                    }
                }
                break;
            }
        }
        cur_paxosi_id_ = OB_INVALID_PAXOS_ID ;
    }
    return ret ;
}*/
// add by maosy 20170417 b
//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20141024:b
/*Exp: check whether sub_query's result is duplicate,
* and move the result to row_store_*/
int ObExprValues::store_input_values()
{
  int ret = OB_SUCCESS;
  OB_ASSERT(0 < row_desc_.get_column_num());
  ModuleArena buf(OB_COMMON_MEM_BLOCK_SIZE, ModulePageAllocator(ObModIds::OB_SQL_TRANSFORMER));
  char* varchar_buff = NULL;
  if (NULL == (varchar_buff = buf.alloc(OB_MAX_VARCHAR_LENGTH)))
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    YYSYS_LOG(WARN, "No memory");
  }
  else
  {
    const ObRowStore::StoredRow *stored_row = NULL;
    ObDuplicateIndicator *indicator = NULL;
    if(from_sub_query_ && row_num_ >1 )
    {
        void *ind_buf = NULL;
        if (row_desc_.get_rowkey_cell_count() <= 0)
        {
          ret = OB_ERROR;
          YYSYS_LOG(WARN, "RowKey is empty, ret=%d", ret);
        }
        else if (NULL == (ind_buf = buf.alloc(sizeof(ObDuplicateIndicator))))
        {
          ret = OB_ERROR;
          YYSYS_LOG(WARN, "Malloc ObDuplicateIndicator failed, ret=%d", ret);
        }
        else
        {
          indicator = new (ind_buf) ObDuplicateIndicator();
          if ((ret = indicator->init(row_num_)) != OB_SUCCESS)
          {
            YYSYS_LOG(WARN, "Init ObDuplicateIndicator failed, ret=%d", ret);
          }
        }
    }
    int64_t col_num = row_desc_.get_column_num();
    ObRow temp_row;
    temp_row.set_row_desc(row_desc_);
    // add by maosy [MultiUps 1.0] [batch_udi] 20170421 b
    bool is_need_partition= false;
    int64_t size =0 ;
    if(paxos_ids_.count()>0)
    {
        is_need_partition = true;
    }
    int row_num = 1;
    // add by maosy 20170421 e
    while(OB_SUCCESS == ret)
    {
      ret = result_row_store_.get_next_row(temp_row);
      if(OB_ITER_END == ret)
      {
        ret = OB_SUCCESS;
        break;
      }
      else if(OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "fail to get next row from sub query");
      }
      else
      {
        ObRow val_row;
        val_row.set_row_desc(row_desc_);
        ObString varchar;
        ObObj casted_cell;
        for (int64_t j = 0; OB_SUCCESS == ret && j < col_num; ++j)
        {
          varchar.assign_ptr(varchar_buff, OB_MAX_VARCHAR_LENGTH);
          casted_cell.set_varchar(varchar); // reuse the varchar buffer
          const ObObj *single_value = NULL;
          uint64_t table_id = OB_INVALID_ID;
          uint64_t column_id = OB_INVALID_ID;
          ObObj tmp_value;
          ObObj data_type;
          if (OB_SUCCESS != (ret = row_desc_ext_.get_by_idx(j, table_id, column_id, data_type)))
          {
            ret = OB_ERR_UNEXPECTED;
            YYSYS_LOG(WARN, "Failed to get column, err=%d", ret);
          }
          else if(OB_SUCCESS != (ret = temp_row.get_cell(table_id, column_id, single_value)))
          {
            ret = OB_ERR_UNEXPECTED;
            YYSYS_LOG(WARN, "Failed to get column value, err=%d", ret);
          }
          else if(OB_SUCCESS != (ret = column_check(table_id, column_id, single_value)))
          {
            ret = OB_ERR_SUB_QUERY_NULL_COLUMN;
            YYSYS_LOG(WARN, "Failed to pass column check, err=%d", ret);
          }
          else if (OB_SUCCESS != (ret = obj_cast(*single_value, data_type, casted_cell, single_value)))
          {
            YYSYS_LOG(WARN, "failed to cast obj, err=%d", ret);
            if (OB_DECIMAL_UNLEGAL_ERROR == ret)
            {
              if (OB_SUCCESS != parse_decimal_illeagal_info(*single_value, table_id, column_id, row_num))
              {
                YYSYS_LOG(WARN, "failed to generate error info");
              }
            }
          }
          else if (OB_SUCCESS != (ret = ob_write_obj_v2(buf, *single_value, tmp_value)))
          {
            YYSYS_LOG(WARN, "str buf write obj fail:ret[%d]", ret);
          }
          else if (OB_SUCCESS != (ret = val_row.set_cell(table_id, column_id, tmp_value)))
          {
            YYSYS_LOG(WARN, "Add value to ObRow failed");
          }
        }
        if(ret == OB_SUCCESS)
        {
            if(is_need_partition)
            {
                // add by maosy [MultiUps 1.0] [batch_udi] 20170421 b
                val_row.set_paxos_id(paxos_ids_.at(size));
                size ++;
                // add by maosy  20170421 e
                if (OB_SUCCESS != (ret = add_values(val_row,false)))
                {
                    YYSYS_LOG(WARN, "failed to add row into store, err=%d", ret);
                }
            }
            else
            {
                if(OB_SUCCESS != (ret = row_store_.add_row(val_row,stored_row)))
                {
                    YYSYS_LOG(WARN,"failed to add row = %s  row store ,ret = %d",to_cstring(val_row),ret);
                }
            }
            if(OB_SUCCESS !=ret )
            {}
          else if (indicator)
          {
            const ObRowkey *rowkey = NULL;
            bool is_dup = false;
            if ((ret = val_row.get_rowkey(rowkey)) != OB_SUCCESS)
            {
              YYSYS_LOG(WARN, "Get RowKey failed, err=%d", ret);
            }
            else if ((ret = indicator->have_seen(*rowkey, is_dup)) != OB_SUCCESS)
            {
              YYSYS_LOG(WARN, "Check duplication failed, err=%d", ret);
            }
            else if (is_dup)
            {
              ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
              YYSYS_LOG(USER_ERROR, "Duplicate entry \'%s\' for key \'PRIMARY\'", to_cstring(*rowkey));
            }
            YYSYS_LOG(DEBUG, "check rowkey isdup is %c rowkey=%s", is_dup?'Y':'N', to_cstring(*rowkey));
          }
       }

        //add dragon [varchar limit] 2016-8-12 11:10:19
        //do varchar length check
        if(OB_SUCCESS == ret)
        {
          if(OB_SUCCESS != (ret = do_varchar_len_check(val_row)))
          {
            YYSYS_LOG(WARN, "varchar check not pass..");
            break;
          }
        }
        //add e
      }
      row_num++;
    }//end while
    // add by maosy [MultiUps 1.0] [batch_udi] 20170421 b
    if( OB_SUCCESS ==ret && is_need_partition && size != paxos_ids_.count())
    {
        ret  =  OB_ERR_UNEXPECTED ;
        YYSYS_LOG(ERROR ,"failed to check ,row size = %ld,count = %ld",size,paxos_ids_.count());
    }
    // add by maosy 20170421 e
  }
  return ret;
}
//add 20141024:e

//add dragon [varchar limit] 2016-8-12 11:14:27
int ObExprValues::check_self()
{
  int ret = OB_SUCCESS;
  if(NULL == manager_ && OB_SUCCESS != init())
  {
    YYSYS_LOG(WARN, "check self failed, ret = %d", ret);
  }
  return ret;
}

int ObExprValues::alloc_schema_mgr(const ObSchemaManagerV2 *&schema_mgr)
{
  int ret = OB_SUCCESS;
  schema_mgr = NULL;
  int64_t sv = manager_->get_latest_version (); //获取最新的schema版本号
  if(NULL == (schema_mgr = manager_->get_user_schema (sv)))
  {
    ret = OB_ERR_NULL_POINTER;
    YYSYS_LOG(WARN, "get user schema failed, schema version is %ld", sv);
  }
  return ret;
}

int ObExprValues::release_schema (const ObSchemaManagerV2 *schema_mgr)
{
  int ret = OB_SUCCESS;
  if(NULL != schema_mgr)
  {
    ret = manager_->release_schema (schema_mgr);
  }
  return ret;
}

int ObExprValues::do_varchar_len_check(ObRow &row)
{
  int ret = OB_SUCCESS;
  int err = OB_SUCCESS; //用于判断schema_mgr是否释放成功
  if(OB_SUCCESS != (ret = check_self()))
  {
    YYSYS_LOG(WARN, "do self check failed, ret[%d]", ret);
  }
  else
  {
    const ObObj *cell = NULL;
    //申请schema_mgr_
    const ObSchemaManagerV2 *schema_mgr = NULL;
    if(OB_SUCCESS != (ret = alloc_schema_mgr(schema_mgr)))
    {
      YYSYS_LOG(WARN, "alloc ob schema managerV2 failed, ret[%d]", ret);
    }
    else
    {
      YYSYS_LOG(DEBUG, "alloc ob schema_mgr[%p] succ", schema_mgr);
    }
    for(int64_t cell_idx = 0; OB_SUCCESS == ret && cell_idx < row.get_column_num (); cell_idx++)
    {
      if(OB_SUCCESS != (ret = row.raw_get_cell (cell_idx, cell)))
      {
        YYSYS_LOG(WARN, "get cell[%ld] failed", cell_idx);
        break;
      }
      //如果获取到的cell的类型是varchar，则进行长度判断
      else if(ObVarcharType == cell->get_type ())
      {
        const ObRowDesc *row_desc = row.get_row_desc ();//获取行描述
        uint64_t tid = OB_INVALID_ID;
        uint64_t cid = OB_INVALID_ID;
        ret = row_desc->get_tid_cid(cell_idx, tid, cid); //获取表id和列id
        //获取column schema
        const ObColumnSchemaV2 *col_sche = schema_mgr->get_column_schema (tid, cid);
        if(NULL != col_sche)
        {
          int64_t length_sche = col_sche->get_size (); //字段属性的长度
          int64_t length_var = cell->get_data_length (); //变量的长度
          if(length_var > length_sche) //compare
          {
            YYSYS_LOG(WARN, "Varchar is too long, tid[%ld], cid[%ld], length in schema[%ld], "
                      "length in vachar[%ld]", tid, cid, length_sche, length_var);
            ret = OB_ERR_VARCHAR_TOO_LONG;
            YYSYS_LOG(USER_ERROR, "%ld.%ld.%ld", tid, cid, length_var);
          }
        }
        else
        {
          YYSYS_LOG(ERROR, "fetch column schema failed!");
          ret = OB_ERR_NULL_POINTER;
        }
      }
    }
    if(OB_SUCCESS != (err = release_schema (schema_mgr)))
    {
      YYSYS_LOG(WARN, "release schema failed! errno[%d]", err);
      if(OB_SUCCESS == ret)
        ret = err;
    }
  }
  return ret;
}
//add e

int ObExprValues::parse_decimal_illeagal_info(const ObObj obj, const uint64_t table_id, const uint64_t column_id, const int64_t row_num)
{
  int ret = OB_SUCCESS;
  int err = OB_SUCCESS; //用于判断schema_mgr是否释放成功
  if(OB_SUCCESS != (ret = check_self()))
  {
    YYSYS_LOG(WARN, "do self check failed, ret[%d]", ret);
  }
  else
  {
    //申请schema_mgr_
    const ObSchemaManagerV2 *schema_mgr = NULL;
    if(OB_SUCCESS != (ret = alloc_schema_mgr(schema_mgr)))
    {
      YYSYS_LOG(WARN, "alloc ob schema managerV2 failed, ret[%d]", ret);
    }
    else
    {
      YYSYS_LOG(DEBUG, "alloc ob schema_mgr[%p] succ", schema_mgr);
    }

    const ObTableSchema* table_schema =NULL;
    const ObColumnSchemaV2  *column_schema = NULL;
    if(NULL ==(table_schema = schema_mgr->get_table_schema(table_id)))
    {
      YYSYS_LOG(ERROR,"table_schema is null");
      ret = OB_ERR_UNEXPECTED;
    }
    else if(NULL != (column_schema = schema_mgr->get_column_schema(table_id, column_id)))
    {
      YYSYS_LOG(USER_ERROR, "%ld.%ld.%d.%d.%d.%d.%ld", table_id, column_id, column_schema->get_precision(),column_schema->get_scale(), obj.get_precision(), obj.get_scale(), row_num);
    }
    else
    {
      YYSYS_LOG(ERROR,"col_schema is null");
      ret = OB_ERR_UNEXPECTED;      
    }
    if(OB_SUCCESS != (err = release_schema(schema_mgr)))
    {
      YYSYS_LOG(WARN, "release schema failed! errno[%d]", err);
      if(OB_SUCCESS == ret)
        ret = err;
    }
  }
  return ret;
}

bool ObExprValues::column_check(uint64_t table_id, uint64_t column_id, const ObObj *single_value)
{
  int ret = OB_SUCCESS;
  int err = OB_SUCCESS; //用于判断schema_mgr是否释放成功
  const ObSchemaManagerV2 *schema_mgr = NULL;
  const ObColumnSchemaV2  *column_schema = NULL;

  if(OB_SUCCESS != (ret = check_self()))
  {
    YYSYS_LOG(WARN, "do self check failed, ret[%d]", ret);
  }
  else if(OB_SUCCESS != (ret = alloc_schema_mgr(schema_mgr)))
  {
    ret = OB_ERR_UNEXPECTED;
    YYSYS_LOG(WARN, "Failed to get table schema, err=%d", ret);
  }
  else if(NULL == (column_schema = schema_mgr->get_column_schema(table_id, column_id)))
  {
    ret = OB_ERR_UNEXPECTED;
    YYSYS_LOG(WARN,"Failed to get column schema, err=%d", ret);
  }
  else
  {
    if (!column_schema->is_nullable())
    {
      if (single_value->is_null())
      {
        ret = OB_ERR_SUB_QUERY_NULL_COLUMN;
        YYSYS_LOG(ERROR,"column:'%s' can not be null,err=%d", column_schema->get_name(), ret);
      }
    }
    if(OB_SUCCESS != (err = release_schema(schema_mgr)))
    {
      YYSYS_LOG(WARN, "release schema failed! errno[%d]", err);
      if(OB_SUCCESS == ret)
        ret = err;
    }
  }
  return ret;
}
