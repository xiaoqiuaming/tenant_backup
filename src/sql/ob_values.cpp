/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_values.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_values.h"
#include "common/utility.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;
//add wenghaixing[decimal] for fix delete bug 2014/10/15
ObValuesKeyInfo::ObValuesKeyInfo()
{
    tid_=OB_INVALID_ID;
    cid_=OB_INVALID_ID;
    type_=ObNullType;
    precision_=38;
    scale_=0;
}
ObValuesKeyInfo::~ObValuesKeyInfo()
{

}
//add e
ObValues::ObValues()
//add wenghaixing[decimal] for fix delete bug 2014/10/15
    :obj_array_(common::OB_MAX_ROWKEY_COLUMN_NUMBER*sizeof(ObValuesKeyInfo), ModulePageAllocator(ObModIds::OB_SQL_EXPR))
//add e
{
    //add wenghaixing[decimal] for fix delete bug 2014/10/10
    is_need_fix_obvalues=false;
    //add e
    // add by maosy [MultiUps 1.0] [batch_udi] 20170417
    cur_paxosi_id_ = OB_INVALID_PAXOS_ID;
    // add by maosy  20170417
}

ObValues::~ObValues()
{
}

//add wenghaixing[decimal] for fix delete bug 2014/10/10
void ObValues::set_fix_obvalues()
{
    is_need_fix_obvalues=true;
}
//add e

void ObValues::reset()
{
  row_desc_.reset();
  //curr_row_.reset(false, ObRow::DEFAULT_NULL);
  row_store_.clear();
  ObSingleChildPhyOperator::reset();
  //add wenghaixing[decimal] for fix delete bug 2014/10/10
  is_need_fix_obvalues=false;
  obj_array_.clear();
  //add e
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

void ObValues::reuse()
{
  row_desc_.reset();
  //curr_row_.reset(false, ObRow::DEFAULT_NULL);
  row_store_.clear();
  ObSingleChildPhyOperator::reset();
  //add wenghaixing[decimal] for fix delete bug 2014/10/10
  is_need_fix_obvalues=false;
  obj_array_.clear();
  //add e
  	// add by maosy [MultiUps 1.0] [batch_udi] 20170417 b
  for (int i = 0 ; i < MAX_UPS_COUNT_ONE_CLUSTER ;i++)
  {
      if(bit_paxos_.has_member(i))
      {
          batch_row_stores_[i].clear();
      }
  }
//  batch_row_stores_.clear();
    bit_paxos_.clear();
  cur_paxosi_id_ = OB_INVALID_PAXOS_ID;
    paxos_ids_.clear();
  	// add by maosy 20170417 e
}

int ObValues::set_row_desc(const common::ObRowDesc &row_desc)
{
  YYSYS_LOG(DEBUG, "DEBUG ObValues set row desc %s", to_cstring(row_desc));
  row_desc_ = row_desc;
  return OB_SUCCESS;
}

int ObValues::add_values(const common::ObRow &value)
{
  const ObRowStore::StoredRow *stored_row = NULL;
  return row_store_.add_row(value, stored_row);
}
//add gaojt [Delete_Update_Function] [JHOBv0.1] 20161108:b

void ObValues::set_rowkey_num(int64_t rowkey_num)
{
    rowkey_num_ = rowkey_num;
}

void ObValues::set_table_id(uint64_t table_id)
{
    table_id_ = table_id;
}

int ObValues::set_column_ids(uint64_t column_id)
{
    int ret = OB_SUCCESS;

    if (OB_SUCCESS != (ret = column_ids_.push_back(column_id)))
    {
        YYSYS_LOG(WARN,"fail to push back");
    }

    return ret;
}

// mod by maosy [MultiUps 1.0] [batch_udi] 20170417 b
void ObValues::add_current_row_desc()
{
    curr_row_.set_row_desc(row_desc_);
}
int ObValues::add_values_for_ud(common::ObRow* value,bool is_table_level)
{
    const ObRowStore::StoredRow *stored_row = NULL;
    int ret = OB_SUCCESS;
    YYSYS_LOG(DEBUG,"PAOXS_ID =%ld,is_table=%d",value->get_paxos_id(),is_table_level);
    if(OB_SUCCESS !=ret )
    {}
    else if(is_table_level)
    {
        if (OB_SUCCESS != (ret = row_store_.add_row(*value, stored_row)))
        {
            YYSYS_LOG(WARN, "fail to add row:ret[%d]", ret);
        }
    }
    else
    {
        int32_t paxos_id = (int32_t)(value->get_paxos_id());
        batch_row_stores_[value->get_paxos_id()].add_row(*value,stored_row);
        if(!bit_paxos_.has_member(paxos_id))
        {
            bit_paxos_.add_member(paxos_id);
        }
    }
    return ret;
}

//add gaojt 20161108:e

// add by maosy [MultiUps 1.0] [batch_udi] 20170417 b
/*
int ObValues::create_row_store_for_batch()
{
    int ret = OB_SUCCESS;
    if(cur_paxosi_id_ ==OB_INVALID_PAXOS_ID)
    {
        return ret ;
    }
    else
    {
        row_store_.reuse();
        for(int64_t i = 0 ; i < batch_row_stores_.count();i++)
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
                    YYSYS_LOG(INFO,"ROW = %s",to_cstring(batch_row_stores_.at(i).row_store_.at(index)));
                    if (OB_SUCCESS != (ret =
                                       row_store_.add_row(batch_row_stores_.at(i).row_store_.at(index),stored_row)))
                    {
                        YYSYS_LOG(WARN, "fail to add row:ret[%d]", ret);
                    }
                    index ++ ;
                }
                break;
            }
        }
    }
    return ret ;
}*/
	// add by maosy 20170417 e
int ObValues::open()
{
  int ret = OB_SUCCESS;
  curr_row_.set_row_desc(row_desc_);
  if (NULL != child_op_)
  {
    if (OB_SUCCESS != (ret = load_data()))
    {
      YYSYS_LOG(WARN, "failed to load data from child op, err=%d", ret);
    }
  }
  return ret;
}

int ObValues::close()
{
    row_store_.clear();
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
    // add by maosy 20170417 b
    return OB_SUCCESS;
}

int ObValues::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = row_store_.get_next_row(curr_row_)))
  {
    if (OB_ITER_END != ret)
    {
      YYSYS_LOG(WARN, "failed to get next row from row store, err=%d", ret);
    }
  }
  else
  {
    row = &curr_row_;
  }
  return ret;
}

int ObValues::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  row_desc = &row_desc_;
  return OB_SUCCESS;
}

namespace oceanbase
{
  namespace sql
  {
    REGISTER_PHY_OPERATOR(ObValues, PHY_VALUES);
  }
}

int64_t ObValues::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "Values(row_store=%s)\n", to_cstring(row_store_));
  if (NULL != child_op_)
  {
    pos += child_op_->to_string(buf+pos, buf_len-pos);
  }
  return pos;
}

PHY_OPERATOR_ASSIGN(ObValues)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObValues);
  reset();
  ObRowStore *store_ptr = const_cast<ObRowStore*>(&o_ptr->row_store_);
  if ((ret = row_desc_.assign(o_ptr->row_desc_)) == OB_SUCCESS)
  {
    ObRow row;
    int64_t cur_size_counter;
    store_ptr->reset_iterator();
    while ((ret = store_ptr->get_next_row(row)) == OB_SUCCESS)
    {
      if ((ret = row_store_.add_row(row, cur_size_counter)) != OB_SUCCESS)
      {
        break;
      }
    }
    if (ret == OB_ITER_END)
    {
      ret = OB_SUCCESS;
    }
    store_ptr->reset_iterator();
  }
  return ret;
}

DEFINE_SERIALIZE(ObValues)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  if (OB_SUCCESS != (ret = row_desc_.serialize(buf, buf_len, tmp_pos)))
  {
    YYSYS_LOG(WARN, "serialize row_desc fail ret=%d buf=%p buf_len=%ld pos=%ld", ret, buf, buf_len, tmp_pos);
  }
  else if (OB_SUCCESS != (ret = row_store_.serialize(buf, buf_len, tmp_pos)))
  {
    YYSYS_LOG(WARN, "serialize row_store fail ret=%d buf=%p buf_len=%ld pos=%ld", ret, buf, buf_len, tmp_pos);
  }
  else
  {
    pos = tmp_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(ObValues)
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
    pos = tmp_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObValues)
{
  return (row_desc_.get_serialize_size() + row_store_.get_serialize_size());
}

//add shili [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20150717:b
//@berif  ��ObValues�е� ������ ���,�����ӵ�row_store_array
//row_store_array  [out] ���ڴ洢���е���
//row_mem_allocator  [in] ���ڷ������ʹ�õ��ڴ�
int ObValues::get_row_store_array(ObArray<common::ObRow> &row_store_array,common::ModuleArena & row_mem_allocator)
{
  int ret = OB_SUCCESS;
  const ObRow * row =NULL;
  while(OB_SUCCESS==ret)
  {
    ret=get_next_row(row);  //���һ��
    if(OB_ITER_END == ret)
    {
      ret=OB_SUCCESS;
      break;
    }
    else
    {
      ret = add_row_deep_copy(*(const_cast<ObRow*>(row)),row_store_array,row_mem_allocator);//��row  ������row_store_array��
      if(OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "add_row_deep_copy fail,ret=%d",ret);
        break;
      }
    }
  }
  return ret;
}
//add 20150717:e
//add shili [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20150705:b
//����ObValues�е� row_store_��Ա
void ObValues::row_store_reuse()//���� row_store_�е�ֵ
{
  row_store_.reuse();
}

int ObValues::add_row_deep_copy(ObRow &row,ObArray<ObRow> &row_store_array,common::ModuleArena & row_mem_allocator)
{
  int ret = OB_SUCCESS;
  char *ind_buf = NULL;
  ind_buf=row_mem_allocator.alloc(sizeof(ObRow));
  if(NULL == ind_buf)
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    YYSYS_LOG(WARN, "No memory");
  }
  else
  {
    ObRow * row_ptr=new(ind_buf) ObRow();
    ret=row.deep_copy(*row_ptr,row_mem_allocator);  //��ȿ����� *row_ptr��
    if(OB_SUCCESS!=ret)
    {
      YYSYS_LOG(WARN, "deep copy error");
    }
    else
    {
      ret=row_store_array.push_back(*row_ptr);  //�����·����ڴ��row���뵽 row_store_array��
      if(OB_SUCCESS!=ret)
      {
        YYSYS_LOG(WARN, "push back row into row_store fail,ret=%d",ret);
      }
    }
  }
  return ret;
}
//add 20150705:e
int ObValues::load_data()
{
  int ret = OB_SUCCESS;
  int err = OB_SUCCESS;
  const ObRow *row = NULL;
  const ObRowDesc *row_desc = NULL;
  const ObRowStore::StoredRow *stored_row = NULL;

  if (OB_SUCCESS != (ret = child_op_->open()))
  {
    YYSYS_LOG(WARN, "fail to open rpc scan:ret[%d]", ret);
  }

  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = child_op_->get_row_desc(row_desc)))
    {
      YYSYS_LOG(WARN, "fail to get row_desc:ret[%d]", ret);
    }
    else
    {
      row_desc_ = *row_desc;
    }
  }
  // add by maosy [MultiUps 1.0] [batch_udi] 20170421 b
  bool is_need_partition= false;
  int size =0 ;
  if(paxos_ids_.count()>0)
  {
      is_need_partition = true;
  }
  // add by maosy 20170421 e
  while (OB_SUCCESS == ret)
  {
    ret = child_op_->get_next_row(row);
    //conflict_level A 
    if(NULL!=row)
    {
      row->dump();
    }
    else
    {
      YYSYS_LOG(WARN, "row is null,ret=%d", ret);
    }

    if (OB_ITER_END == ret)
    {
      ret = OB_SUCCESS;
      break;
    }
    else if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "fail to get next row from rpc scan");
    }
    else
    {
	  // add by maosy [MultiUps 1.0] [batch_udi] 20170421 b
        if(is_need_partition)
        {
            const_cast<ObRow*>(row)->set_paxos_id(paxos_ids_.at(size));
            size++;
        }
		  // add by maosy 20170421 e
      //modify fanqiushi DECIMAL OceanBase_BankCommV0.2 2014_6_5:b for delete bug
      /*
      YYSYS_LOG(DEBUG, "load data from child, row=%s", to_cstring(*row));
      if (OB_SUCCESS != (ret = row_store_.add_row(*row, stored_row)))
      {
        YYSYS_LOG(WARN, "fail to add row:ret[%d]", ret);
      }
      */
      YYSYS_LOG(DEBUG, "load data from child, row=%s", to_cstring(*row));
      if(is_need_fix_obvalues)
      {
        int ret_v2=OB_SUCCESS;
        ModuleArena row_alloc(OB_MAX_ROW_LENGTH, ModulePageAllocator(ObModIds::OB_SQL_RPC_SCAN));
        ObRow row_v2=*row;
        //const ObRowkey* rowkey = NULL;
       // if(OB_SUCCESS!=(ret_v2=row->get_rowkey(rowkey)))
        //{
          //YYSYS_LOG(ERROR,"test in get_rowkey");
        //}
       // else
       // {
          uint64_t table_id = OB_INVALID_ID;
          uint64_t column_id = OB_INVALID_ID;
          const ObObj *ori_cell = NULL;
          for(int64_t j=0;j<row_v2.get_column_num();j++)
          {
              if(OB_SUCCESS!=(ret_v2=row_v2.raw_get_cell(j, ori_cell, table_id, column_id)))
              {}
              else if(ori_cell!=NULL)
              {
                  //for(int32_t i=0;i<rowkey->get_obj_cnt();i++)
                  if(is_rowkey_column(table_id, column_id))
                  {
                      //ObObj cell = rowkey->get_obj_ptr()[i];
                      ObObjType schema_type;
                      uint32_t schema_p;
                      uint32_t schema_s;
                      if(OB_SUCCESS!=(ret_v2=get_rowkey_schema(table_id,column_id,schema_type,schema_p,schema_s)))
                      {}
                      else
                      {
                        ObObj result_cell;
                        ObObj tmp_cell;
                        if(ObDecimalType==schema_type&&ori_cell->get_type()==ObDecimalType)
                        {
                           //ObString os;
                           //ori_cell->get_decimal(os);
                           uint64_t *t1 = NULL;
                           if (OB_SUCCESS != (ret = ori_cell->get_ttint(t1)))
                           {
                               YYSYS_LOG(ERROR, "get decimal pointer from ori_cell error,ret = %d",ret);
                           }
                           else
                           {
                               tmp_cell.set_decimal(t1,schema_p,schema_s,ori_cell->get_vscale(), ori_cell->get_nwords());
                               if(OB_SUCCESS!=(ret_v2=ob_write_obj_v2(row_alloc,tmp_cell,result_cell)))
                               {}
                               else if(OB_SUCCESS!=(ret_v2=row_v2.set_cell(table_id,column_id,result_cell)))
                               {}
                           }


                        }
                      }
                  }
              }
          }
          //}
		    // add by maosy [MultiUps 1.0] [batch_udi] 20170421 b
          if(is_need_partition)
          {
              if(OB_SUCCESS==ret_v2)
              {
                  if (OB_SUCCESS != (ret = add_values_for_ud(&row_v2,false)))
                  {
                      YYSYS_LOG(WARN, "fail to add row:ret[%d]", ret);
                  }
              }
              else if (OB_SUCCESS != (ret = add_values_for_ud(const_cast<ObRow*>(row),false)))
              {
                  YYSYS_LOG(WARN, "fail to add row:ret[%d]", ret);
              }
          }
          else
          {
		    // add by maosy 20170421 e
              if(OB_SUCCESS==ret_v2)
              {
                  if (OB_SUCCESS != (ret = row_store_.add_row(row_v2, stored_row)))
                  {
                      YYSYS_LOG(WARN, "fail to add row:ret[%d]", ret);
                  }
              }
              else if (OB_SUCCESS != (ret = row_store_.add_row(*row, stored_row)))
              {
                  YYSYS_LOG(WARN, "fail to add row:ret[%d]", ret);
              }
          }
          row_alloc.free();
      }
      else
      {
	    // add by maosy [MultiUps 1.0] [batch_udi] 20170421 b
          if(is_need_partition)
          {
              if (OB_SUCCESS != (ret = add_values_for_ud(const_cast<ObRow*>(row),false)))
              {
                  YYSYS_LOG(WARN, "fail to add row:ret[%d]", ret);
              }
          }
		    // add by maosy 20170421 e
          else if (OB_SUCCESS != (ret = row_store_.add_row(*row, stored_row)))
          {
              YYSYS_LOG(WARN, "fail to add row:ret[%d]", ret);
          }
      }
        //modify e
    }
  }
    // add by maosy [MultiUps 1.0] [batch_udi] 20170421 b
  if(size != paxos_ids_.count())
  {
      ret = OB_ERR_UNEXPECTED ;
      YYSYS_LOG(WARN,"failed to check paxos id ,size = %d,count = %ld",size,paxos_ids_.count());
  }
    // add by maosy 20170421 e
  if (OB_SUCCESS != (err = child_op_->close()))
  {
    YYSYS_LOG(WARN, "fail to close rpc scan:err[%d]", err);
    if (OB_SUCCESS == ret)
    {
      ret = err;
    }
  }
  return ret;
}
//add wenghaixing[decimal] for fix delete bug 2014/10/15
void ObValues::add_rowkey_array(uint64_t tid,uint64_t cid,common::ObObjType type,uint32_t p,uint32_t s)
{

       ObValuesKeyInfo oki;
       oki.set_key_info(tid,cid,p,s,type);
       obj_array_.push_back(oki);
}

bool ObValues::is_rowkey_column(uint64_t tid,uint64_t cid)
{
    ObValuesKeyInfo oki;
    bool ret=false;
    for(int i=0;i<obj_array_.count();i++)
    {
        obj_array_.at(i,oki);
        if(oki.is_rowkey(tid,cid))
        {
                ret=true;
                break;
        }
    }
    return ret;

}

int ObValues::get_rowkey_schema(uint64_t tid,uint64_t cid,common::ObObjType& type,uint32_t& p,uint32_t& s)
{
    int ret=OB_ERROR;
    ObValuesKeyInfo oki;
    for(int i=0;i<obj_array_.count();i++)
    {
        obj_array_.at(i,oki);
        if(oki.is_rowkey(tid,cid))
        {
            oki.get_type(type);
            oki.get_key_info(p,s);
            ret=OB_SUCCESS;
            break;
        }

    }
    if(OB_SUCCESS!=ret)
    {
        YYSYS_LOG(WARN,"can not find rowkey info in obvalues!tid=%ld,cid=%ld",tid,cid);
    }
    return ret;
}
//add e
