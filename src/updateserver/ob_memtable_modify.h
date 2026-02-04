/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_modify.h
 *
 * Authors:
 *   Li Kai <yubai.lk@alipay.com>
 *
 */
#ifndef  OCEANBASE_UPDATESERVER_MEMTABLE_MODIFY_H_
#define  OCEANBASE_UPDATESERVER_MEMTABLE_MODIFY_H_

#include "sql/ob_ups_modify.h"
#include "common/ob_iterator.h"
#include "common/ob_iterator_adaptor.h"
#include "ob_sessionctx_factory.h"
#include "ob_ups_table_mgr.h"
#include "ob_ups_utils.h"
//add fanqiushi_index
#include "sql/ob_index_trigger.h"
//add:e
//add wenghaixing [secondary index upd.4]20141129
#include "sql/ob_index_trigger_upd.h"
//add e
// add by liyongfeng:20150105 [secondary index replace]
#include "sql/ob_index_trigger_rep.h"
// add:e
//add liumengzhan_delete_index
#include "sql/ob_delete_index.h"
//add:e
//add wangyao b [sequence]
#include "sql/ob_sequence_define.h"
#include "sql/ob_sequence.h"
//add e
//[426]
#include "sql/ob_when_filter.h"

namespace oceanbase
{
  namespace updateserver
  {
    template <class T>
    class MemTableModifyTmpl : public T, public RowkeyInfoCache
    {
      public:
        MemTableModifyTmpl(RWSessionCtx &session, ObIUpsTableMgr &host);
        ~MemTableModifyTmpl();
      public:
        int open();
        int close();
        int get_next_row(const common::ObRow *&row);
        int get_row_desc(const common::ObRowDesc *&row_desc) const;
        int get_affected_rows(int64_t& row_count);
        int64_t to_string(char* buf, const int64_t buf_len) const;
        //add wenghaixing [secondary index check_index_num]20150807
        int64_t get_index_num();
        //add e
        //add wenghaixing [secondary index lock_conflict]20150807
        bool ob_operator_has_trigger();
        //add e
        //add wangyao b [sequence]
        bool get_is_sequence();
        bool get_update_prevval();
        //add e
      private:
        RWSessionCtx &session_;
        ObIUpsTableMgr &host_;
    };

    template <class T>
    MemTableModifyTmpl<T>::MemTableModifyTmpl(RWSessionCtx &session, ObIUpsTableMgr &host): session_(session),
                                                                                 host_(host)
    {
    }

    template <class T>
    MemTableModifyTmpl<T>::~MemTableModifyTmpl()
    {
    }

    template <class T>
    int MemTableModifyTmpl<T>::open()
    {
      int ret = OB_SUCCESS;
      const ObRowDesc *row_desc = NULL;
      uint64_t table_id = OB_INVALID_ID;
      uint64_t column_id = OB_INVALID_ID;
      const ObRowkeyInfo *rki = NULL;
      if (NULL == T::child_op_)
      {
        ret = OB_NOT_INIT;
      }
      else if (OB_SUCCESS != (ret = T::child_op_->open()))
      {
        if (!IS_SQL_ERR(ret))
        {
          YYSYS_LOG(WARN, "child operator open fail ret=%d", ret);
        }
      }
      else if (OB_SUCCESS != (ret = T::child_op_->get_row_desc(row_desc))
              || NULL == row_desc)
      {
        if (OB_ITER_END != ret)
        {
          YYSYS_LOG(WARN, "get_row_desc from child_op=%p type=%d fail ret=%d", T::child_op_, T::child_op_->get_type(), ret);
          ret = (OB_SUCCESS != ret) ? OB_ERROR : ret;
        }
        else
        {
          ret = OB_SUCCESS;
        }
      }
      else if (OB_SUCCESS != (ret = row_desc->get_tid_cid(0, table_id, column_id))
              || OB_INVALID_ID == table_id)
      {
        YYSYS_LOG(WARN, "get_tid_cid from row_desc fail, child_op=%p type=%d %s ret=%d",
                  T::child_op_, T::child_op_->get_type(), to_cstring(*row_desc), ret);
        ret = (OB_SUCCESS != ret) ? OB_ERROR : ret;
      }
      else if (NULL == (rki = get_rowkey_info(host_, table_id)))
      {
        YYSYS_LOG(WARN, "get_rowkey_info fail table_id=%lu", table_id);
        ret = OB_SCHEMA_ERROR;
      }
      else
      {
        UpsSchemaMgrGuard sm_guard;
        const CommonSchemaManager *sm = NULL;
        if (NULL == (sm = host_.get_schema_mgr().get_schema_mgr(sm_guard)))
        {
          YYSYS_LOG(WARN, "get_schema_mgr fail");
          ret = OB_SCHEMA_ERROR;
        }
        else
        {   //modify wenghaixing [secondary index] 20150811
            int64_t index_num = get_index_num();
            if(!ob_operator_has_trigger())
            {
              if(OB_SUCCESS == ret)
              {
                  //add wangyao [sequence for optimize]:20171025
                  if (get_is_sequence())
                  {
                       ObRowStore store;
                       const common::ObRow *row = NULL;
                       const ObRowDesc *desc = NULL;
                       const ObRowStore::StoredRow *stored_row = NULL;
                       SequenceInfo s_info;
                       ObSequence tmp_sequence;
                       bool not_use_prevval=false;
                       //add wangyao [sequence for get prevval value]:20171206
                       if(get_update_prevval())
                       {
                           not_use_prevval = true;
                       }
                       //add e
                       if (OB_SUCCESS != (ret = T::child_op_ -> get_next_row(row)))
                       {
                         //YYSYS_LOG(WARN, "failed to get_next_row,ret=%d",ret);//del qiuhm[filter log]
                       }
                       else if (OB_SUCCESS != (ret = tmp_sequence.parse_row(row, s_info,not_use_prevval)))
                       {
                         YYSYS_LOG(ERROR,"parse row to sequence info faild,ret=%d",ret);
                       }
                       else if (OB_SUCCESS != (ret = tmp_sequence.calc_nextval_for_sequence(s_info)))
                       {
                         YYSYS_LOG(ERROR, "calc nextval for sequence failed! ret=%d",ret);
                       }
                       else
                       {
                           int64_t can_use_prevval_value=s_info.can_use_prevval_;
                           common::ObRow* tmp_row = const_cast<common::ObRow *>(row);
                           ObObj next_use_obj;
                           s_info.can_use_prevval_=1;
                           next_use_obj.set_int(s_info.can_use_prevval_);
                           if(OB_SUCCESS != (ret = tmp_row->raw_set_cell((CURRENT_VALUE_ID - OB_APP_MIN_COLUMN_ID), s_info.current_value_)))
                           {
                            YYSYS_LOG(ERROR, "fail to add current value to row, ret=%d", ret);
                           }
                           else if(OB_SUCCESS != (ret = tmp_row->raw_set_cell((CAN_USE_PREVVAL_ID - OB_APP_MIN_COLUMN_ID -1), next_use_obj)))
                           {
                             YYSYS_LOG(ERROR, "fail to add nextval value to row, ret=%d", ret);
                           }
                           else
                           {
                               if(0 == s_info.valid_)
                               {
                                   ObObj valid_use_obj;
                                   valid_use_obj.set_int(s_info.valid_);
                                   if(OB_SUCCESS != (ret = tmp_row->raw_set_cell((VALID_ID - OB_APP_MIN_COLUMN_ID),valid_use_obj)))
                                   {
                                    YYSYS_LOG(ERROR, "fail to add valid value to row, ret=%d", ret);
                                   }
                               }
                               else
                               {
                                   YYSYS_LOG(DEBUG,"the sequence is valid");
                               }
                           }
                           if (OB_SUCCESS == ret)
                           {
                               //add wangyao [sequence for get prevval value]:20171206
                               common::ObRow other_row;
                               ObRowDesc row_desc;
                               row_desc.add_column_desc(OB_TEMP_SEQUENCE_TABLE_TID,OB_APP_MIN_COLUMN_ID);
                               row_desc.add_column_desc(OB_TEMP_SEQUENCE_TABLE_TID,OB_APP_MIN_COLUMN_ID+1);
                               row_desc.set_rowkey_cell_count(1);
                               other_row.set_row_desc(row_desc);
                               ObObj t_row_key;
                               t_row_key.set_type(ObInt32Type);
                               t_row_key.set_int32(1);
                               other_row.raw_set_cell(0,t_row_key);
                               other_row.raw_set_cell(1,s_info.prevval_);
                               //add e
                               YYSYS_LOG(DEBUG,"tmp_row is %s",to_cstring(*tmp_row));
                               if(OB_SUCCESS != (ret=store.add_row(*tmp_row, stored_row)))
                               {
                                  YYSYS_LOG(WARN, "add tmp_row to row store failed,ret = %d", ret);
                               }
                               else if (OB_SUCCESS != (ret = T::child_op_ -> get_row_desc(desc)))
                               {
                                 YYSYS_LOG(WARN, "get_row_desc from child_op=%p type=%d fail ret=%d", T::child_op_, T::child_op_->get_type(), ret);
                               }
                               else
                               {
                                 ObIndexCellIterAdaptor cia;
                                 cia.set_row_iter(&store, rki->get_size(), sm, *desc);
                                 ret = host_.apply(session_, cia, T::get_dml_type());
                                 session_.inc_dml_count(T::get_dml_type());
                               }
                               if (OB_SUCCESS == ret)
                               {
                                   ObNewScanner scanner;
                                   if(0 == can_use_prevval_value)
                                   {
                                       ObObj obj;
                                       s_info.can_use_prevval_=0;
                                       obj.set_int(s_info.can_use_prevval_);
                                       tmp_row->raw_set_cell((CAN_USE_PREVVAL_ID - OB_APP_MIN_COLUMN_ID -1), obj);
                                   }
                                   if(OB_SUCCESS !=(ret = scanner.add_row(*tmp_row)))
                                   {
                                       YYSYS_LOG(WARN,"add tmp_row to scanner fail, ret=%d %s", ret, to_cstring(*tmp_row));
                                   }
                                   //add wangyao [sequence for get prevval value]:20171206
                                   else
                                   {
                                       if(not_use_prevval)
                                       {
                                         if(OB_SUCCESS !=(ret = scanner.add_row(other_row)))
                                         {
                                           YYSYS_LOG(WARN,"add other_row to scanner fail, ret=%d %s", ret, to_cstring(other_row));
                                         }
                                       }
                                   //add e
                                       if(OB_SUCCESS == ret && OB_SUCCESS !=(ret = session_.get_ups_result().set_scanner(scanner)))
                                       {
                                           YYSYS_LOG(WARN, "set scanner to ups_result fail ret=%d", ret);
                                       }
                                       else
                                       {
                                           session_.get_ups_result().set_k();
                                       }
                                    }
                                 }
                           }
                       }
                   }
                   else
                   {
                          // add e
                      ObCellIterAdaptor cia;
                      //modify wenghaixing [secondary index static_index_build.consistency]20150424
                      //cia.set_row_iter(T::child_op_, rki->get_size(), sm);
                      cia.set_row_iter(T::child_op_, rki->get_size(), sm, index_num);
                      //modify e
                      ret = host_.apply(session_, cia, T::get_dml_type());
                      //YYSYS_LOG(ERROR, "child_op_'s index num[%ld],index_num[%ld]",T::child_op_->get_index_num(),index_num);
                      session_.inc_dml_count(T::get_dml_type());
                  }
              }
            }
            else
            {
                ObRowStore store, store_ex;
                const common::ObRow *row = NULL;
                const ObRowDesc *desc = NULL;
                const ObRowStore::StoredRow *stored_row = NULL;
                if(sql::PHY_INDEX_TRIGGER_UPD == T::child_op_->get_type())
                {
                  sql::ObProject *tmp_op=NULL;
                  tmp_op = static_cast<sql::ObProject*>(T::child_op_->get_child(0));
                  tmp_op -> set_row_store(&store_ex, &store);
                }
                else if(sql::PHY_DELETE_INDEX== T::child_op_->get_type())
                {
                  sql::ObProject *tmp_op=NULL;

                  //[426] mod
                  if(sql::PHY_WHEN_FILTER == T::child_op_->get_child(0)->get_type())
                  {
                      sql::ObWhenFilter *when_op = NULL;
                      when_op = static_cast<sql::ObWhenFilter *>(T::child_op_->get_child(0));
                      tmp_op = static_cast<sql::ObProject*>(when_op->get_child(0));
                  }
                  else
                  {
                      tmp_op = static_cast<sql::ObProject*>(T::child_op_->get_child(0));
                  }

                  tmp_op -> set_row_store(NULL, &store_ex);
                }
                if(OB_SUCCESS != (ret = T::child_op_ -> get_row_desc(desc)))
                {
                  YYSYS_LOG(WARN, "get row desc failed, ret = %d", ret);
                }

                //add hongchen [SKEW_INDEX_COLUMN_BUGFIX] 20170801:b
                ObRowDesc last_row_desc;
                ObRowDesc extra_column_desc;
                uint64_t  table_id  = OB_INVALID_ID;
                uint64_t  column_id = OB_INVALID_ID;
                const ObTableSchema *index_table_schema = NULL;
                uint64_t index_tid_array[OB_MAX_ROWKEY_COLUMN_NUMBER];
                uint64_t array_count=0;
                if (OB_DML_INSERT != T::get_dml_type())
                {
                  //nothing todo
                }
                else if (OB_SUCCESS != (ret = desc->get_tid_cid(0,table_id, column_id)))
                {
                  YYSYS_LOG(WARN,"fail to get tid from desc, ret=%d", ret);
                }
                else if (OB_SUCCESS != ( ret = sm->get_all_modifiable_index_tid(table_id, index_tid_array, array_count)))
                {
                  YYSYS_LOG(WARN,"fail to get index tid list for table %lu, ret = %d", table_id, ret);
                }
                else
                {
                  last_row_desc = *(const_cast<ObRowDesc*>(desc));
                  for (int64_t i = 0;OB_SUCCESS == ret && i < static_cast<int64_t>(array_count);i++)
                  {
                    if (NULL == (index_table_schema = sm->get_table_schema(index_tid_array[i])))
                    {
                      ret = OB_SCHEMA_ERROR;
                      YYSYS_LOG(WARN, "fail to acquire index table[%lu] schema, ret=%d", index_tid_array[i], ret);
                    }
                    else
                    {
                      const ObRowkeyInfo& rowkey_info = index_table_schema->get_rowkey_info();
                      for (int64_t idx = 0; OB_SUCCESS == ret && idx < rowkey_info.get_size(); idx++)
                      {
                        column_id = OB_INVALID_ID;
                        if (OB_SUCCESS != (ret = rowkey_info.get_column_id(idx, column_id)))
                        {
                          YYSYS_LOG(WARN,"fail to get column id from rowkey into, ret=%d", ret);
                        }
                        else if (OB_INVALID_INDEX != last_row_desc.get_idx(table_id, column_id))
                        {
                          //NOTHING TODO
                        }
                        else if (OB_SUCCESS != (ret = last_row_desc.add_column_desc(table_id, column_id)))
                        {
                          YYSYS_LOG(WARN,"fail to add column[%lu] to last_row_desc, ret=%d", column_id, ret);
                        }
                        else if (OB_SUCCESS != (ret = extra_column_desc.add_column_desc(table_id, column_id)))
                        {
                          YYSYS_LOG(WARN,"fail to add column[%lu] to extra_column_desc, ret=%d", column_id, ret);
                        }
                      }
                    }
                  }
                }

                YYSYS_LOG(DEBUG,"last row desc %s", to_cstring(last_row_desc));
                YYSYS_LOG(DEBUG," ROW DESC %s",to_cstring(*desc));
                YYSYS_LOG(DEBUG,"EXTRA C DESC %s", to_cstring(extra_column_desc));
                common::ObRow full_row;
                if (OB_SUCCESS == ret && 0 < extra_column_desc.get_column_num())
                {
                  full_row.set_row_desc(last_row_desc);
                  full_row.reset(false,ObRow::DEFAULT_NULL);
                  desc = &last_row_desc;
                }
                //add hongchen [SKEW_INDEX_COLUMN_BUGFIX] 20170801:b

                  while(OB_SUCCESS == ret && OB_SUCCESS == (ret = T::child_op_ -> get_next_row(row)))
                  {
                    if(NULL != row && sql::PHY_INDEX_TRIGGER_UPD != T::child_op_ -> get_type())
                    {
                      //add hongchen [SKEW_INDEX_COLUMN_BUGFIX] 20170801:b
                      if (0 < extra_column_desc.get_column_num())
                      {
                        table_id = OB_INVALID_ID;
                        column_id = OB_INVALID_ID;
                        const common::ObObj *cell = NULL;
                        for (int64_t idx = 0; OB_SUCCESS == ret && idx < row->get_column_num(); idx++)
                        {
                          if (OB_SUCCESS != (ret = row->raw_get_cell(idx, cell, table_id, column_id)))
                          {
                            YYSYS_LOG(WARN, "fail to get cell from row, ret=%d", ret);
                          }
                          else if (OB_SUCCESS != (ret = full_row.set_cell(table_id, column_id, *cell)))
                          {
                            YYSYS_LOG(WARN, "fail to add cell<%lu,%lu> to full row, ret = %d", table_id, column_id, ret);
                          }
                        }
                        row = &full_row;
                      }
                      //add hongchen [SKEW_INDEX_COLUMN_BUGFIX] 20170801:e
                      //mod hongchen [SKEW_INDEX_COLUMN_BUGFIX] 20170801:b
                      //if(OB_SUCCESS != (ret = store.add_row(*row, stored_row)))
                      if(OB_SUCCESS == ret && OB_SUCCESS != (ret = store.add_row(*row, stored_row)))
                      //mod hongchen [SKEW_INDEX_COLUMN_BUGFIX] 20170801:e
                      {
                        YYSYS_LOG(WARN, "add row to row store failed,ret = %d", ret);
                        break;
                      }
                    }
                  }
                  if(OB_ITER_END == ret)
                  {
                    ret = OB_SUCCESS;
                  }



                //modify data table first
                if(NULL != desc && OB_SUCCESS == ret)
                {
                  //store.reset_iterator();
                    ObIndexCellIterAdaptor icia;
                    icia.set_row_iter(&store, rki->get_size(), sm, *desc,index_num);
                      //add by maosy [MultiUps 1.0][secondary index optimize]20170401 b:
                      //ret = host_.apply(session_, icia, T::get_dml_type());
                      ret = host_.apply(session_,icia,T::get_dml_type(),PRIMARY_TABLE);
                      // add by maosy
                  }
                //then modify index table
                if(OB_SUCCESS == ret)
                {
                  store.reset_iterator();
                  //add liumengzhan_delete_index
                  if(sql::PHY_DELETE_INDEX == T::child_op_->get_type())
                  {
                      ObIUpsTableMgr* host = &host_;
                      sql::ObDeleteIndex *delete_index_op = NULL;
                      delete_index_op = dynamic_cast<sql::ObDeleteIndex*>(T::child_op_);
                      //if(OB_SUCCESS != (ret = delete_index_op->handle_trigger(sm, host, session_, T::get_dml_type(),&store_ex)))
                      if(OB_SUCCESS != (ret = delete_index_op->handle_trigger(sm, host, session_, OB_DML_INDEX_INSERT,&store_ex)))
                      {
                          YYSYS_LOG(ERROR, "delete index table fail,ret = %d",ret);
                      }
                  }
                  // add by maosy
                  //modify by fanqiushi_index
                  if(T::child_op_->get_type()==sql::PHY_INDEX_TRIGGER)
                  {
                    store.reset_iterator();  //add hongchen [REPLACE_INDEX_OPT] 20170804
                    ObIUpsTableMgr* host=&host_;
                    sql::ObIndexTrigger *tmp_index_tri=NULL;
                    tmp_index_tri=static_cast<sql::ObIndexTrigger*>(T::child_op_);
                    //mod hongchen [SKEW_INDEX_COLUMN_BUGFIX] 20170801:b
                    ////add by maosy [MultiUps 1.0][secondary index optimize]20170401 b:
                    ////if(OB_SUCCESS!=(ret=tmp_index_tri->handle_trigger(row_desc,sm,rki->get_size(),host,session_, T::get_dml_type(),&store)))
                    //if(OB_SUCCESS!=(ret=tmp_index_tri->handle_trigger(row_desc,sm,rki->get_size(),host,session_, OB_DML_INDEX_INSERT,&store)))
                    //    //add by maosy e
                    if(OB_SUCCESS!=(ret=tmp_index_tri->handle_trigger(desc,sm,rki->get_size(),host,session_, OB_DML_INDEX_INSERT,&store)))
                    //mod hongchen [SKEW_INDEX_COLUMN_BUGFIX] 20170801:e
                    {
                      YYSYS_LOG(ERROR, "modify index table fail,ret[%d]",ret);
                    }
                  }
                  //add wenghaixing [secondary index upd.4] 20141129
                  if(T::child_op_->get_type()==sql::PHY_INDEX_TRIGGER_UPD)
                  {
                    ObIUpsTableMgr* host=&host_;
                    sql::ObIndexTriggerUpd *tmp_index_tri=NULL;
                    tmp_index_tri=static_cast<sql::ObIndexTriggerUpd*>(T::child_op_);
                    if(OB_SUCCESS!=(ret=tmp_index_tri->handle_trigger(sm,host,session_,&store_ex)))
                    {
                      YYSYS_LOG(ERROR, "modify index table fail,ret[%d]",ret);
                    }
                  }
                  //add e
                  // add by liyongfeng:20150105 [secondary index replace]
                  if (sql::PHY_INDEX_TRIGGER_REP == T::child_op_->get_type())
                  {
                    store.reset_iterator();  //add hongchen [REPLACE_INDEX_OPT] 20170804
                    ObIUpsTableMgr* host = &host_;
                    sql::ObIndexTriggerRep *index_replace_op = dynamic_cast<sql::ObIndexTriggerRep*>(T::child_op_);
                    if (OB_SUCCESS != (ret = index_replace_op->handle_trigger(sm, host, session_,&store)))
                    {
                      YYSYS_LOG(ERROR, "fail to replace table index, err=%d", ret);
                    }
                  }
                }
                //modify e
            }
        }
      }
	  //add by maosy[MultiUps 1.0] [secondary index optimize]20170401 b:
      if(OB_SUCCESS ==ret && session_.get_ups_result().get_affected_rows()!=0) //mod wangyao [302 bugfix]
      {
          if(OB_SUCCESS !=(ret = session_.get_ups_mutator().get_mutator().add_stat_barrier()))
          {
              YYSYS_LOG(WARN,"add query barrier to mutator failed , ret = %d",ret );
          }
      }
	  // add by maosy e

      // add by maosy [MultiUps 1.0] [secondary index optimize]20170608 b:
      session_.clear_tevalues();
      // add by maosy e
      if (OB_SUCCESS != ret)
      {
        if (NULL != T::child_op_)
        {
          T::child_op_->close();
        }
      }
      return ret;
    }

    template <class T>
    int MemTableModifyTmpl<T>::close()
    {
      int ret = OB_SUCCESS;
      if (NULL == T::child_op_)
      {
        ret = OB_NOT_INIT;
      }
      else
      {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = T::child_op_->close()))
        {
          YYSYS_LOG(WARN, "child operator close fail ret=%d", tmp_ret);
        }
      }
      return ret;
    }

    template <class T>
    int MemTableModifyTmpl<T>::get_affected_rows(int64_t& row_count)
    {
      int ret = OB_SUCCESS;
      row_count = session_.get_ups_result().get_affected_rows();
      return ret;
    }

    template <class T>
    int MemTableModifyTmpl<T>::get_next_row(const common::ObRow *&row)
    {
      UNUSED(row);
      return OB_ITER_END;
    }

    template <class T>
    int MemTableModifyTmpl<T>::get_row_desc(const common::ObRowDesc *&row_desc) const
    {
      UNUSED(row_desc);
      return OB_ITER_END;
    }

    template <class T>
    int64_t MemTableModifyTmpl<T>::to_string(char* buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      databuff_printf(buf, buf_len, pos, "MemTableModify(op_type=%d dml_type=%s session=%p[%d:%ld])\n",
                      T::get_type(),
                      str_dml_type(T::get_dml_type()),
                      &session_,
                      session_.get_session_descriptor(),
                      session_.get_session_start_time());
      if (NULL != T::child_op_)
      {
        pos += T::child_op_->to_string(buf+pos, buf_len-pos);
      }
      return pos;
    }

    //add wenghaixing [secondary index check_index_num]20150807
    template <class T>
    int64_t MemTableModifyTmpl<T>::get_index_num()
    {
      int64_t index_num = 0;
      if(sql::PHY_DELETE_INDEX == T::child_op_->get_type())
      {
        sql::ObDeleteIndex *delete_index_op = NULL;
        delete_index_op = dynamic_cast<sql::ObDeleteIndex*>(T::child_op_);
        index_num = delete_index_op->get_index_num();
      }
      else if(T::child_op_->get_type()==sql::PHY_INDEX_TRIGGER)
      {
        sql::ObIndexTrigger *tmp_index_tri=NULL;
        tmp_index_tri = dynamic_cast<sql::ObIndexTrigger*>(T::child_op_);
        index_num = tmp_index_tri->get_index_num();
      }
      else if(T::child_op_->get_type()==sql::PHY_INDEX_TRIGGER_UPD)
      {
        sql::ObIndexTriggerUpd *tmp_index_tri=NULL;
        tmp_index_tri=static_cast<sql::ObIndexTriggerUpd*>(T::child_op_);
        index_num = tmp_index_tri->get_index_num();
      }
      else if (sql::PHY_INDEX_TRIGGER_REP == T::child_op_->get_type())
      {
        sql::ObIndexTriggerRep *index_replace_op = dynamic_cast<sql::ObIndexTriggerRep*>(T::child_op_);
        index_num = index_replace_op->get_index_num();
      }
      else if(sql::PHY_PROJECT == T::child_op_->get_type())
      {
        sql::ObProject *update_op = dynamic_cast<sql::ObProject*>(T::child_op_);
        index_num = update_op->get_index_num();
      }
      return index_num;
    }
    template <class T>
    bool MemTableModifyTmpl<T>::ob_operator_has_trigger()
    {return T::child_op_->get_type() >= sql::PHY_INDEX_TRIGGER;}
    //add e

    //add wangyao [sequence for optimize]:20171120
    template <class T>
    bool MemTableModifyTmpl<T>::get_is_sequence(){return T::get_is_sequence();}
    template <class T>
    bool MemTableModifyTmpl<T>::get_update_prevval(){return T::get_update_prevval();}
    //add e

    typedef MemTableModifyTmpl<sql::ObUpsModify> MemTableModify;
    typedef MemTableModifyTmpl<sql::ObUpsModifyWithDmlType> MemTableModifyWithDmlType;
  } // end namespace updateserver
} // end namespace oceanbase

#endif /* OCEANBASE_UPDATESERVER_MEMTABLE_MODIFY_H_ */

