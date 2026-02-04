#include "ob_index_trigger_upd.h"
#include "common/ob_obj_cast.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

namespace oceanbase
{
  namespace sql
  {
    REGISTER_PHY_OPERATOR(ObIndexTriggerUpd, PHY_INDEX_TRIGGER_UPD);
  }
}

ObIndexTriggerUpd::ObIndexTriggerUpd()
{
  //add wenghaixing [secondary index upd.bugfix]20150127
  has_other_cond_=false;
  //arena_.reuse();
  //add e
}

ObIndexTriggerUpd::~ObIndexTriggerUpd()
{
  arena_.free();
}

int ObIndexTriggerUpd::open()
{

  int ret = OB_SUCCESS;
  //add wenghaixing [secondary index upd.bugfix]20150127
  row_.set_row_desc(data_row_desc_);
  arena_.reuse();
  //add e
  if (OB_SUCCESS != (ret = ObSingleChildPhyOperator::open()))
  {
    if (!IS_SQL_ERR(ret))
    {
      YYSYS_LOG(WARN, "failed to open child_op, err=%d", ret);
    }
  }
    /*
    else if(OB_SUCCESS!=(ret=get_next_data_row()))
    {
        YYSYS_LOG(ERROR,"failed to get data table row!ret[%d]",ret);
    }
    else
    {
        YYSYS_LOG(ERROR,"test::whx,success open()");
    }
    */


  return ret;
}

//add wenghaixing [secondary index upd.4] 20141129
int ObIndexTriggerUpd::close()
{
  int ret = OB_SUCCESS;
  if (NULL == child_op_)
  {
    ret = OB_NOT_INIT;
  }
  else
  {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = child_op_->close()))
    {
      YYSYS_LOG(WARN, "close child_op fail ret=%d", tmp_ret);
      //add liuxiao [secondary index static_index_build.bug_fix.merge_error]20150604
      ret = tmp_ret;
      //add e
    }
    else
    {
      arena_.reuse();
    }
  }
  return ret;
}

int ObIndexTriggerUpd::handle_trigger(const ObSchemaManagerV2 *schema_mgr, updateserver::ObIUpsTableMgr *host,
                                      updateserver::RWSessionCtx &session_ctx,  ObRowStore *store)
{
  int ret = OB_SUCCESS;
  if(schema_mgr == NULL)
  {
    YYSYS_LOG(ERROR,"schema manager is NULL");
    ret = OB_SCHEMA_ERROR;
  }
  else
  {

    for(int64_t i = 0;i<update_index_num_;i++)
    {
      if(OB_SUCCESS != (ret = handle_trigger_one_table(i,schema_mgr,host,session_ctx, store)) && ret != OB_DECIMAL_UNLEGAL_ERROR)
      {
        int err = OB_SUCCESS;
        const ObTableSchema* schema = NULL;
        uint64_t tid = OB_INVALID_ID;
        uint64_t cid = OB_INVALID_ID;
        if(OB_SUCCESS != (err = (index_row_desc_del_[i].get_tid_cid(0,tid,cid))))
        {
          YYSYS_LOG(WARN,"failed to get table id from index_row_desc_del_,err[%d]",err);
          break;
        }
        else
        {
          schema = schema_mgr->get_table_schema(tid);
        }
        if(NULL != schema || OB_INVALID_ID == tid)
        {
          YYSYS_LOG(ERROR,"handle one table error,err=%d",ret);
          ret = OB_ERROR;
          break;
        }
        else
        {
          YYSYS_LOG(WARN,"this index is not in schema,maybe was droped!tid[%ld]",tid);
          ret = OB_SUCCESS;
          continue;
        }
      }
    }
  }
  return ret;
}

int ObIndexTriggerUpd::handle_trigger_one_table(int64_t index_num,const ObSchemaManagerV2 *schema_mgr,
                                                updateserver::ObIUpsTableMgr *host, updateserver::RWSessionCtx &session_ctx,  ObRowStore* store)
{
  int ret = OB_SUCCESS;
  //YYSYS_LOG(ERROR,"test::whx get_next_data_row!");
  common::ObRow input_row;
  //common::ObRowDesc *desc=NULL;
  const ObRowStore::StoredRow *stored_row = NULL;
  ObRowStore store_del;
  ObRowStore store_upd;
  common::ObRow row_to_store;
  int64_t i = index_num;
  char* varchar_buff = NULL;
  input_row.set_row_desc(data_row_desc_);
  sql::ObProject *tmp_op=NULL;

  if (NULL == store)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR, "child_op_ is NULL");
  }
  else if(NULL == (varchar_buff = arena_.alloc(OB_MAX_VARCHAR_LENGTH)))
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    YYSYS_LOG(WARN, "No memory");
  }
  else if(NULL == (tmp_op = static_cast<sql::ObProject*>(child_op_)))
  {
    ret = OB_ERROR;
  }
  //add e
  else
  {
    uint64_t data_tid = OB_INVALID_ID;
    uint64_t tid = OB_INVALID_ID;
    uint64_t cid = OB_INVALID_ID;
    const ObObj *obj=NULL;
    ObObj tmp_value;
  ObObj obj_dml, obj_virtual;
    const ObRowDesc *row_desc=NULL;
    if(OB_SUCCESS != (ret = get_trigger_row_desc(row_desc)))
    {
      YYSYS_LOG(WARN,"get row desc failed,ret[%d]",ret);
    }
    if(OB_SUCCESS == ret)
    {
//      data_tid=row_desc->get_tid_cid(0,data_tid,cid);
      tmp_op->reset_row_num_idx();
      while(OB_SUCCESS == ret)
      {
        ret = store->get_next_row(input_row);
        if(OB_SUCCESS != ret)
        {
          break;
        }
        //fill sequence val
        if(OB_SUCCESS == ret && OB_SUCCESS != (ret = tmp_op->fill_sequence_info(set_index_columns_)))
        {
          YYSYS_LOG(WARN, "fill sql expr array failed, ret = %d",ret);
        }
        //first we construct delete row
        ObString varchar;
        ObObj casted_cell;
        ObObj data_type;
        for(int64_t j = 0;j<row_desc->get_column_num()&&OB_SUCCESS == ret;j++)
        {
          if(OB_SUCCESS != (ret = row_desc->get_tid_cid(j,data_tid,cid)))
          {
            YYSYS_LOG(ERROR,"failed in get tid_cid from row desc,ret[%d]",ret);
            break;
          }
          else if(OB_ACTION_FLAG_COLUMN_ID == cid)
          {
              //add by maosy [MultiUps 1.0] [hot update and secondary index] 20170415 b
              //之前这一块的处理，主键更新添加了接口row_desc->exist_column_id来处理，认为不太好，还是把action_flag取出来了
              if(OB_SUCCESS != (ret = row_desc->get_tid_cid(0,data_tid,cid)))
              {
                  YYSYS_LOG(ERROR,"failed in get tid_cid from row desc,ret[%d]",ret);
                  break;
              }
              // add by maosy e
          }
          else if(OB_INDEX_VIRTUAL_COLUMN_ID == cid)
          {
          }
          else if(OB_SUCCESS != (ret = input_row.get_cell(data_tid,cid,obj)))
          {
            YYSYS_LOG(ERROR,"failed in get cell from input_row,ret[%d],tid[%ld],cid[%ld]",ret,data_tid,cid);
            break;
          }
          else
          {
            if(NULL == obj)
            {
              YYSYS_LOG(ERROR,"obj's pointer can not be NULL!");
              ret = OB_INVALID_ARGUMENT;
              break;
            }
            else if(OB_SUCCESS != (ret = row_.set_cell(data_tid,cid,*obj)))
            {
              YYSYS_LOG(ERROR,"set cell for update failed,ret=%d",ret);
              break;
            }
          }
        }
        row_to_store.set_row_desc(index_row_desc_del_[i]);
        for(int col_idx = 0;col_idx<index_row_desc_del_[i].get_column_num()&&OB_SUCCESS == ret;col_idx++)
        {
          if(OB_SUCCESS != (ret = index_row_desc_del_[i].get_tid_cid(col_idx,tid,cid)))
          {
            YYSYS_LOG(ERROR,"failed in get tid_cid from row desc,ret[%d]",ret);
            break;
          }
          else if(OB_ACTION_FLAG_COLUMN_ID == cid)
          {
            obj_dml.set_int(ObActionFlag::OP_DEL_ROW);
            if(OB_SUCCESS != (ret = row_to_store.set_cell(tid,cid,obj_dml)))
            {
              YYSYS_LOG(ERROR,"set cell for update failed,ret=%d",ret);
              break;
            }
          }
          else if(OB_SUCCESS != (ret = row_.get_cell(data_tid,cid,obj)))
          {
            YYSYS_LOG(ERROR,"failed in get cell from input_row,ret[%d],tid[%ld],cid[%ld]",ret,data_tid,cid);
            break;
          }
          else
          {
            if(NULL == obj)
            {
              YYSYS_LOG(ERROR,"obj's pointer can not be NULL!");
              ret = OB_INVALID_ARGUMENT;
              break;
            }
            else if(OB_SUCCESS != (ret = row_to_store.set_cell(tid,cid,*obj)))
            {
              YYSYS_LOG(ERROR,"set cell for update failed,ret=%d",ret);
              break;
            }
          }
        }
        if(OB_SUCCESS == ret)
        {
          if(OB_SUCCESS != (ret = (store_del.add_row(row_to_store,stored_row))))
          {
            YYSYS_LOG(ERROR, "failed to do row_store.add_row faile,ret=%d,row=%s",ret,to_cstring(row_to_store));
          }
          else
          {
            row_to_store.clear();
          }
        }
        //add by maosy e
        //YYSYS_LOG(ERROR,"test::whx row_to_store_del=%s",to_cstring(store_del));
        //second we construct update row
        row_to_store.set_row_desc(index_row_desc_upd_[i]);
        for(int col_idx = 0;col_idx<index_row_desc_upd_[i].get_column_num()&&OB_SUCCESS == ret;col_idx++)
        {
          if(OB_SUCCESS != (ret = index_row_desc_upd_[i].get_tid_cid(col_idx,tid,cid)))
          {
            YYSYS_LOG(ERROR,"failed in get tid_cid from row desc,ret[%d]",ret);
            break;
          }
          else if(OB_ACTION_FLAG_COLUMN_ID == cid)
          {
          }
          else if(OB_INDEX_VIRTUAL_COLUMN_ID == cid)
          {
            obj_virtual.set_null();
            obj = &obj_virtual;
          }
          else if(OB_SUCCESS != (ret = row_.get_cell(data_tid,cid,obj)))
          {
            YYSYS_LOG(ERROR,"failed in get cell from input_row,ret[%d]",ret);
            break;
          }

          if(OB_SUCCESS == ret)
          {
              for(int expr_num = 0;expr_num<set_index_columns_.count()&&OB_SUCCESS == ret;expr_num++)
              {
                  varchar.assign_ptr(varchar_buff, OB_MAX_VARCHAR_LENGTH);
                  casted_cell.set_varchar(varchar);
                  if(OB_SUCCESS != (ret = cast_obj_.at(expr_num, data_type)))
                  {
                      YYSYS_LOG(WARN, "get cast obj failed!ret [%d]",ret);
                      break;
                  }
                  if(cid == set_index_columns_.at(expr_num).get_column_id())
                  {
                      ObSqlExpression &expr = set_index_columns_.at(expr_num);
                      if (OB_SUCCESS != (ret = expr.calc(row_, obj)))
                      {
                          YYSYS_LOG(WARN, "failed to calculate, err=%d", ret);
                          break;
                      }
                      else if(OB_SUCCESS != (obj_cast(*obj, data_type, casted_cell, obj)))
                      {
                          break;
                      }
                      else if(OB_SUCCESS != (ret = ob_write_obj_v2(arena_, *obj, tmp_value)))
                      {
                          YYSYS_LOG(WARN, "str buf write obj fail:ret[%d]", ret);
                          break;
                      }
                      else
                      {
                          //YYSYS_LOG(ERROR, "test::whx obj p = %d,s = %d,type = %d", data_type.get_precision(), data_type.get_scale(),data_type.get_type());
                          obj = &tmp_value;
                      }
                  }
              }
              
            if(OB_SUCCESS == ret)
            {
              if(NULL == obj)
              {
                YYSYS_LOG(ERROR,"obj's pointer can not be NULL!");
                ret = OB_INVALID_ARGUMENT;
                break;
              }
              else if(OB_SUCCESS != (ret = row_to_store.set_cell(tid,cid,*obj)))
              {
                YYSYS_LOG(ERROR,"set cell for update failed,ret=%d",ret);
                break;
              }
            }
          }
        }
        if(OB_SUCCESS == ret)
        {
          if(OB_SUCCESS != (ret = (store_upd.add_row(row_to_store,stored_row))))
          {
            YYSYS_LOG(ERROR, "failed to do row_store.add_row faile,ret=%d,row=%s",ret,to_cstring(row_to_store));
          }
          else
          {
            row_to_store.clear();
          }
          //YYSYS_LOG(ERROR,"test::whx row_to_store_upd=%s",to_cstring(store_upd));
        }

      }//end while

      if(ret == OB_ITER_END)
         ret = OB_SUCCESS;
      tmp_op->reset_row_num_idx();
    }
  }
  //modify liuxiao [secondary index static_index_build.bug_fix.merge_error]20150604
  //reset_iterator();
  if(NULL != child_op_)
  {
    reset_iterator();
  }
  //modify e
  if(ret == OB_SUCCESS)
  {
    ObIndexCellIterAdaptor icia;
    ObDmlType dml_type = OB_DML_INDEX_INSERT;
    icia.set_row_iter(&store_del,index_row_desc_del_[i].get_rowkey_cell_count(),schema_mgr,index_row_desc_del_[i]);
    ret = host->apply(session_ctx,icia,dml_type,INDEX_TABLE);
    // YYSYS_LOG(ERROR, "test::fanqs  host->apply ret=%d",ret);

  }
  store_del.clear();
  if(ret == OB_SUCCESS)
  {
      ObIndexCellIterAdaptor icia;
	  //add by maosy [MultiUps 1.0]  [secondary index optimize]20170401 b:
	  //ObDmlType dml_type = OB_DML_INSERT;
    //icia.set_row_iter(&store_upd,index_row_desc_upd_[i].get_rowkey_cell_count(),schema_mgr,index_row_desc_upd_[i]);
    //ret = host->apply(session_ctx,icia,dml_type);
      ObDmlType dml_type = OB_DML_INDEX_INSERT;
      icia.set_row_iter(&store_upd,index_row_desc_upd_[i].get_rowkey_cell_count(),schema_mgr,index_row_desc_upd_[i]);
      ret = host->apply(session_ctx,icia,dml_type,INDEX_TABLE);
	  //add by maosy e

     //YYSYS_LOG(ERROR, "test::fanqs  host->apply ret=%d",ret);

  }
  //add liuxiao [secondary index bug fix] 20150908
  if(OB_SUCCESS == ret && NULL != store)
  {
    store->reset_iterator();
  }
  //add e;
  store_upd.clear();
  return ret;
}
//add e
void ObIndexTriggerUpd::reset()
{
  index_num_ = 0;
  update_index_num_ = 0;
  set_index_columns_.clear();
  cast_obj_.clear();
  data_row_desc_.reset();
  for(int64_t i = 0;i<OB_MAX_INDEX_NUMS;i++)
  {
    index_row_desc_del_[i].reset();
    index_row_desc_upd_[i].reset();
  }
  arena_.reuse();
  ObSingleChildPhyOperator::reset();

}

void ObIndexTriggerUpd::reuse()
{
  index_num_=0;
  update_index_num_ = 0;
  set_index_columns_.clear();
  cast_obj_.clear();
  data_row_desc_.reset();
  for(int64_t i = 0;i<OB_MAX_INDEX_NUMS;i++)
  {
    index_row_desc_del_[i].reset();
    index_row_desc_upd_[i].reset();
  }    //ObSingleChildPhyOperator::reuse();
  arena_.reuse();

}


int ObIndexTriggerUpd::add_set_column(const ObSqlExpression &expr)
{
  int ret = OB_SUCCESS;
  if(OB_SUCCESS == (ret = set_index_columns_.push_back(expr)))
  {
    set_index_columns_.at(set_index_columns_.count() - 1).set_owner_op(this);
  }
  return ret;
}

int ObIndexTriggerUpd::add_cast_obj(const ObObj &obj)
{
  int ret = OB_SUCCESS;
  if(OB_SUCCESS == (ret = cast_obj_.push_back(obj)))
  {
    //cast_obj_.at(cast_obj_.count() - 1).set_owner_op(this);
  }
  return ret;
}
//add wenghaixing [secondary index upd.bugfix]20150127
void ObIndexTriggerUpd::set_data_row_desc(common::ObRowDesc &desc , bool is_multi)
{
    data_row_desc_.reset();
  data_row_desc_ = desc;
//add by maosy  [MultiUPS 1.0][secondary index optimize]20170511 b:
  int ret = OB_SUCCESS;
  if(is_multi)
  {
  }
  else if(OB_SUCCESS !=(ret =  data_row_desc_.add_column_desc(OB_INVALID_ID,OB_ACTION_FLAG_COLUMN_ID) ))
  {
      YYSYS_LOG(WARN,"failed to add column desc ,ret = %d,row desc = %s" ,ret ,to_cstring(data_row_desc_));
  }
  // add by maosy 
}
void ObIndexTriggerUpd::set_cond_bool(bool val)
{
  has_other_cond_ = val;
}
void ObIndexTriggerUpd::get_cond_bool(bool &val)
{
  val = has_other_cond_;
}
void ObIndexTriggerUpd::reset_iterator()
{
  if(!has_other_cond_)
  {
    ObMultipleGetMerge* omg=static_cast<ObMultipleGetMerge*>(child_op_->get_child(0));
    omg->reset_iterator();
  }
  else
  {
    ObMultipleGetMerge* omg=static_cast<ObMultipleGetMerge*>(child_op_->get_child(0)->get_child(0));
    omg->reset_iterator();
  }
}

//add e

//add lijianqiang [sequence update] 20150916:b
void ObIndexTriggerUpd::clear_clumns()
{
  set_index_columns_.clear();
  YYSYS_LOG(DEBUG,"clear the index array!");
}
//add 20150926:e
int ObIndexTriggerUpd::add_row_desc_del(int64_t idx,common::ObRowDesc desc)
{
  int ret = OB_SUCCESS;
  if(idx >= OB_MAX_INDEX_NUMS||idx<0)
  {
    YYSYS_LOG(ERROR,"add row desc_del ,idx is invalid! idx=%ld",idx);
    ret = OB_ERROR;
  }
  else
    index_row_desc_del_[idx]=desc;
  return ret;
}
int ObIndexTriggerUpd::add_row_desc_upd(int64_t idx,common::ObRowDesc desc)
{
  int ret = OB_SUCCESS;
  if(idx >= OB_MAX_INDEX_NUMS||idx<0)
  {
    YYSYS_LOG(ERROR,"add row desc_del ,idx is invalid! idx=%ld",idx);
    ret=OB_ERROR;
  }
  else
    index_row_desc_upd_[idx]=desc;
  return ret;
}

int ObIndexTriggerUpd::get_row_desc_del(int64_t idx, common::ObRowDesc &desc)
{
  int ret = OB_SUCCESS;
  if(idx >= OB_MAX_INDEX_NUMS)
  {
    ret=OB_ERROR;
  }
  else
    desc=index_row_desc_del_[idx];
  return ret;
}

void ObIndexTriggerUpd::set_index_num(int64_t num)
{
  index_num_=num;
}

void ObIndexTriggerUpd::set_update_index_num(int64_t num)
{
  update_index_num_ = num;
}

int ObIndexTriggerUpd::get_row_desc_upd(int64_t idx, common::ObRowDesc &desc)
{
  int ret=OB_SUCCESS;
  if(idx>OB_MAX_INDEX_NUMS)
  {
    ret=OB_ERROR;
  }
  else
    desc=index_row_desc_upd_[idx];
  return ret;
}
int ObIndexTriggerUpd:: get_next_data_row(const ObRow *&input_row)
{
  int ret = OB_SUCCESS;
  if(!has_other_cond_)
  {
    ret = child_op_->get_child(0)->get_next_row(input_row);
  }
  else if(has_other_cond_)
  {
    ret = child_op_->get_child(0)->get_child(0)->get_next_row(input_row);
  }
  if(OB_SUCCESS != ret && OB_ITER_END != ret)
  {
    YYSYS_LOG(ERROR,"can not get next_row ,err = %d",ret);
  }
  return ret;
}

int ObIndexTriggerUpd::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  if (NULL == child_op_)
  {
    ret = OB_NOT_INIT;
  }
  else
  {
    ObProject* op=static_cast<ObProject*>(child_op_);
    if(OB_SUCCESS != (ret = op->get_next_row(row)))
    {
      if(ret != OB_ITER_END && !IS_SQL_ERR(ret))//add liumz, [ups -5049 log too much]20161217
         YYSYS_LOG(WARN, "child_op get_next_row fail ret=%d", ret);
          //else ret=OB_SUCCESS;
    }
  }
  return ret;
}

int ObIndexTriggerUpd::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
    //row_desc = &data_row_desc_;
  if(OB_UNLIKELY(NULL == child_op_))
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(WARN, "child op pointer is NULL");
  }
  else
  {
    ret = child_op_->get_row_desc(row_desc);
  }
  return ret;
}

int ObIndexTriggerUpd::get_trigger_row_desc(const ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  row_desc = &data_row_desc_;
  return ret;
}

DEFINE_SERIALIZE(ObIndexTriggerUpd)
{
  int ret = OB_SUCCESS;
  if(OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, pos,index_num_)))
  {
    YYSYS_LOG(WARN,"failed to encode index_num_,ret[%d]",ret);
  }
  else if(OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, pos,update_index_num_)))
  {
    YYSYS_LOG(WARN,"failed to encode update index_num_,ret[%d]",ret);
  }
  else if(OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, pos,(uint64_t)data_max_cid_)))
  {
    YYSYS_LOG(WARN,"failed to encode data_max_cid_,ret[%d]",ret);
  }
  //for fix other cond bug  wenghaixing 20150127
  else if(OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, pos, (int64_t)has_other_cond_)))
  {
    YYSYS_LOG(WARN,"failed to encode has_other_cond_");
  }
  //add e
  else
  {
    for(int64_t i=0;i<update_index_num_;i++)
    {
      if (ret == OB_SUCCESS && (OB_SUCCESS != (ret = index_row_desc_del_[i].serialize(buf, buf_len, pos))))
      {
        YYSYS_LOG(WARN, "serialize fail. ret=%d", ret);
        break;
      }
      if (ret == OB_SUCCESS && (OB_SUCCESS != (ret = index_row_desc_upd_[i].serialize(buf, buf_len, pos))))
      {
        YYSYS_LOG(WARN, "serialize fail. ret=%d", ret);
        break;
      }
    }
    if (ret == OB_SUCCESS && (OB_SUCCESS != (ret = data_row_desc_.serialize(buf, buf_len, pos))))
    {
      YYSYS_LOG(WARN, "serialize fail. ret=%d", ret);
    }
  }
  if(OB_SUCCESS == ret)
  {
    int64_t len = 0;
    len = set_index_columns_.count();
    if(OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, pos,len)))
    {
      YYSYS_LOG(WARN,"failed to encode ,ret[%d]",ret);
    }
    else
    {
      for(int64_t j=0;j<len;j++)
      {
        const ObSqlExpression &expr = set_index_columns_.at(j);
        if (ret == OB_SUCCESS && (OB_SUCCESS != (ret = expr.serialize(buf, buf_len, pos))))
        {
          YYSYS_LOG(WARN, "serialize fail. ret=%d", ret);
          break;
        }
      }
    }
  }

  if(OB_SUCCESS == ret)
  {
    int64_t len = 0;
    len = cast_obj_.count();
    if(OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, pos,len)))
    {
      YYSYS_LOG(WARN,"failed to encode ,ret[%d]",ret);
    }
    else
    {
      for(int64_t j = 0; j < len; j++)
      {
        const ObObj &obj = cast_obj_.at(j);
        if(OB_SUCCESS != (ret = obj.serialize(buf, buf_len, pos)))
        {
          YYSYS_LOG(WARN, "serialize fail. ret=%d", ret);
          break;
        }
      }
    }
  }

  return ret;
}

DEFINE_DESERIALIZE(ObIndexTriggerUpd)
{
  int ret=OB_SUCCESS;
  //add for fix other cond bug  wenghaixing 20150127
  int64_t cond_flag = 0;
  int64_t max_cid = OB_INVALID_ID;
  //add e
  if(OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, pos, &index_num_)))
  {
    YYSYS_LOG(WARN,"failed to decode index_num_,ret[%d]",ret);
  }
  else if(OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, pos, &update_index_num_)))
  {
    YYSYS_LOG(WARN,"failed to decode update index_num_,ret[%d]",ret);
  }
  else if(OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, pos, &max_cid)))
  {
    YYSYS_LOG(WARN,"failed to decode max_cid,ret[%d]",ret);
  }
  //add for fix other cond bug  wenghaixing 20150127
  else if(OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, pos, &cond_flag)))
  {
    YYSYS_LOG(WARN,"failed to decode cond_flag");
  }
  //add e
  else
  {
    for(int64_t i=0;i<update_index_num_;i++)
    {
      if (OB_SUCCESS != (ret = index_row_desc_del_[i].deserialize(buf, data_len, pos)))
      {
        YYSYS_LOG(WARN, "fail to deserialize expression. ret=%d", ret);
        break;
      }
      else if (OB_SUCCESS != (ret = index_row_desc_upd_[i].deserialize(buf, data_len, pos)))
      {
        YYSYS_LOG(WARN, "fail to deserialize expression. ret=%d", ret);
        break;
      }
    }
    if (OB_SUCCESS == ret&&OB_SUCCESS != (ret = data_row_desc_.deserialize(buf, data_len, pos)))
    {
      YYSYS_LOG(WARN, "fail to deserialize expression. ret=%d", ret);
    }
  }
  if(OB_SUCCESS == ret)
  {
    int64_t len=0;
    ObSqlExpression expr;
    if(OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, pos, &len)))
    {
      YYSYS_LOG(WARN,"failed to decode index_num_,ret[%d]",ret);
    }
    else
    {
      for(int64_t j=0;j<len;j++)
      {
        if (OB_SUCCESS != (ret = add_set_column(expr)))
        {
          YYSYS_LOG(DEBUG, "fail to add expr to project ret=%d. buf=%p, data_len=%ld, pos=%ld", ret, buf, data_len, pos);
          break;
        }
        if (OB_SUCCESS != (ret = set_index_columns_.at(j).deserialize(buf, data_len, pos)))
        {
          YYSYS_LOG(WARN, "fail to deserialize expression. ret=%d", ret);
          break;
        }
      }
    }
  }
  if(OB_SUCCESS == ret)
  {
    int64_t len = 0;
    ObObj obj;
    if(OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, pos, &len)))
    {
      YYSYS_LOG(WARN,"failed to decode index_num_,ret[%d]",ret);
    }
    else
    {
      for(int64_t j = 0; j < len; j++)
      {
        if (OB_SUCCESS != (ret = add_cast_obj(obj)))
        {
          YYSYS_LOG(WARN, "failed to add cast obj to project ret = %d",ret);
          break;
        }
        if(OB_SUCCESS != (ret = cast_obj_.at(cast_obj_.count()-1).deserialize(buf, data_len, pos)))
        {
          YYSYS_LOG(WARN, "failed to deserialize expression. ret = %d",ret);
          break;
        }
      }
    }
  }
  //add for fix other cond bug  wenghaixing 20150127
  if(OB_SUCCESS == ret)
  {
    has_other_cond_ = cond_flag == 0?false:true;
    data_max_cid_ = (uint64_t)max_cid;
  }
  //add e

  return ret;
}


DEFINE_GET_SERIALIZE_SIZE(ObIndexTriggerUpd)
{
 /* int64_t size = 0;
  ObObj obj;
  obj.set_int(columns_.count());
  size += obj.get_serialize_size();
  for (int64_t i = 0; i < columns_.count(); ++i)
  {
    const ObSqlExpression &expr = columns_.at(i);
    size += expr.get_serialize_size();
  }
  */

  int64_t size = 0;
  size+=serialization::encoded_length_i64(index_num_);
  size+=serialization::encoded_length_i64(update_index_num_);
  size+=serialization::encoded_length_i64(data_max_cid_);
  for(int64_t i=0;i<index_num_;i++)
  {
    size += index_row_desc_del_[i].get_serialize_size();
    size += index_row_desc_upd_[i].get_serialize_size();
  }
  size += data_row_desc_.get_serialize_size();
  size += static_cast<int64_t>(sizeof(int64_t));
  for(int64_t j=0;j<set_index_columns_.count();j++)
  {
    const ObSqlExpression &expr = set_index_columns_.at(j);
    size += expr.get_serialize_size();
  }
  size += static_cast<int64_t>(sizeof(int64_t));
  for(int64_t j = 0; j < cast_obj_.count(); j++)
  {
    const ObObj &obj = cast_obj_.at(j);
    size += obj.get_serialize_size();
  }
  //add for fix other cond bug  wenghaixing 20150127
  size += static_cast<int64_t>(sizeof(int64_t));
  //add e

  return size;
}


ObPhyOperatorType ObIndexTriggerUpd::get_type() const
{
  return PHY_INDEX_TRIGGER_UPD;
}

int64_t ObIndexTriggerUpd::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ObIndexTriggerUpd(index_num=[%ld],columns:",update_index_num_);
  databuff_printf(buf, buf_len, pos,"Row Desc:");
  for(int64_t i=0;i<update_index_num_;i++)
  {
    int64_t pos2 = index_row_desc_del_[i].to_string(buf+pos, buf_len-pos);
    pos += pos2;
    if (i != update_index_num_ -1)
    {
      databuff_printf(buf, buf_len, pos, ",");
    }
    pos2 = index_row_desc_upd_[i].to_string(buf+pos, buf_len-pos);
    pos += pos2;
    if (i != update_index_num_ -1)
    {
      databuff_printf(buf, buf_len, pos, ",");
    }
  }
  int64_t pos2 = data_row_desc_.to_string(buf+pos, buf_len-pos);
  pos += pos2;
  databuff_printf(buf, buf_len, pos, "\n");
  databuff_printf(buf, buf_len, pos, "set_comluns EXPR:");
  for(int64_t j=0;j<set_index_columns_.count();j++)
  {
    int64_t pos2 = set_index_columns_.at(j).to_string(buf+pos, buf_len-pos);
    pos += pos2;
    if (j != set_index_columns_.count() -1)
    {
      databuff_printf(buf, buf_len, pos, ",");
    }
  }
  if (NULL != child_op_)
  {
    int64_t pos2 = child_op_->to_string(buf+pos, buf_len-pos);
    pos += pos2;
  }
  return pos;
}

PHY_OPERATOR_ASSIGN(ObIndexTriggerUpd)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObIndexTriggerUpd);
  reset();
  for(int64_t i=0;i<o_ptr->index_num_;i++)
  {
    index_row_desc_del_[i]=o_ptr->index_row_desc_del_[i];
    index_row_desc_upd_[i]=o_ptr->index_row_desc_upd_[i];
  }
  data_row_desc_=o_ptr->data_row_desc_;
  for(int64_t j=0;j<o_ptr->set_index_columns_.count();j++)
  {
    if(ret!=OB_SUCCESS)break;
    if ((ret = set_index_columns_.push_back(o_ptr->set_index_columns_.at(j))) == OB_SUCCESS)
    {
      set_index_columns_.at(j).set_owner_op(this);
    }
    else
    {
      break;
    }
  }
  index_num_ = o_ptr->index_num_;
  update_index_num_ = o_ptr->update_index_num_;
  return ret;
}
