////===================================================================
//
// ob_memtable.cpp updateserver / Oceanbase
//
// Copyright (C) 2010 Taobao.com, Inc.
//
// Created on 2010-09-09 by Yubai (yubai.lk@taobao.com)
//
// -------------------------------------------------------------------
//
// Description
//
//
// -------------------------------------------------------------------
//
// Change Log
//
////====================================================================

#include "common/ob_trace_log.h"
#include "ob_memtable.h"
#include "ob_ups_utils.h"
#include "ob_update_server_main.h"

namespace oceanbase
{
  namespace updateserver
  {
    using namespace common;
    using namespace hash;

    MemTable::MemTable() : inited_(false), mem_tank_(), table_engine_(mem_tank_), table_bf_(), version_(0), ref_cnt_(0),
                           checksum_before_mutate_(0), checksum_after_mutate_(0), checksum_(0), uncommited_checksum_(0),
                           last_trans_id_(0), trans_mgr_(), row_counter_(0), tevalue_cb_(), trans_cb_()
    {
      MIN_OBJ.set_min_value();
      MAX_OBJ.set_max_value();
    }

    MemTable::~MemTable()
    {
      if(inited_)
      {
        destroy();
      }
    }

    int MemTable::init(const int64_t hash_size)
    {
      int ret = OB_SUCCESS;
      if(inited_)
      {
        YYSYS_LOG(WARN, "have already inited");
        ret = OB_ERROR;
      }
      else if(OB_SUCCESS != (ret = table_engine_.init(hash_size)))
      {
        YYSYS_LOG(WARN, "init table engine fail ret=%d", ret);
      }
      else if(OB_SUCCESS != (ret = table_bf_.init(BLOOM_FILTER_NHASH, BLOOM_FILTER_NBYTE)))
      {
        table_engine_.destroy();
        YYSYS_LOG(WARN, "init table bloomfilter fail ret=%d", ret);
      }
      else if(OB_SUCCESS != (ret = trans_mgr_.init(MAX_TRANS_NUM)))
      {
        table_engine_.destroy();
        table_bf_.destroy();
        YYSYS_LOG(WARN, "init trans mgr fail ret=%d", ret);
      }
      else
      {
        inited_ = true;
      }
      return ret;
    }

    int MemTable::destroy()
    {
      int ret = OB_SUCCESS;
      if(!inited_)
      {
        YYSYS_LOG(WARN, "have not inited");
      }
      else
      {
        row_counter_ = 0;
        trans_mgr_.destroy();
        checksum_after_mutate_ = 0;
        checksum_before_mutate_ = 0;
        checksum_ = 0;
        uncommited_checksum_ = 0;
        table_bf_.destroy();
        table_engine_.destroy();
        mem_tank_.clear();
        inited_ = false;
      }
      return ret;
    }

    int MemTable::clear()
    {
      int ret = OB_SUCCESS;
      if(!inited_)
      {
        YYSYS_LOG(WARN, "have not inited");
      }
      else
      {
        row_counter_ = 0;
        ret = table_engine_.clear();
      }
      return ret;
    }

    int MemTable::rollback(void *data)
    {
      int ret = OB_SUCCESS;
      RollbackInfo *rollback_info = (RollbackInfo *) data;
      if(NULL == rollback_info || NULL == rollback_info->dest)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        YYSYS_LOG(INFO, "rollback %s te_value=%p from [%s] to [%s]", rollback_info->key.log_str(), rollback_info->dest,
                  rollback_info->dest->log_str(), rollback_info->src.log_str());
        *(rollback_info->dest) = rollback_info->src;
      }
      return ret;
    }

    int MemTable::commit(void *data)
    {
      int ret = OB_SUCCESS;
      CommitInfo *commit_info = (CommitInfo *) data;
      if(NULL == commit_info)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        atomic_add((uint64_t *) &row_counter_, commit_info->row_counter);
      }
      return ret;
    }

    int MemTable::copy_cells_(TEValueUCInfo &uci, ObUpsCompactCellWriter &ccw, const bool need_mark)
    {
      int ret = OB_SUCCESS;
      ObCellInfoNode *node = (ObCellInfoNode *) mem_tank_.node_alloc(
        static_cast<int32_t>(sizeof(ObCellInfoNode) + ccw.size()));
      if(NULL == node)
      {
        ret = OB_MEM_OVERFLOW;
      }
        //add duyr [MultiUPS] [READ_ATOMIC] [write_part] 20151221:b
      else if(OB_SUCCESS != (ret = common::ObReadAtomicHelper::init_buf_with_transver(node->trans_version_,
                                                                                      static_cast<int64_t>(sizeof(node->trans_version_)))))
      {
        YYSYS_LOG(WARN, "fail to init transver_buf!ret=%d", ret);
      }
        //add duyr 20151221:e
      else
      {
        // add by maosy for[delete and update do not read self-query ]
        //          node->modify_time = ccw.get_ctime_recorder().get_ctime();
        if(need_mark)
        {
          if(uci.uc_list_tail == NULL ||
             (uci.uc_list_tail != NULL && uci.uc_list_tail->modify_time != OB_INVALID_VERSION))
          {
            node->modify_time = OB_INVALID_VERSION;// add by maosy
          }
          else
          {
            ret = OB_ERR_REPEAT_UPDATE;
            YYSYS_LOG(ERROR, "twice update");
          }
          //              YYSYS_LOG(INFO,"MAOSY");
        }
        else
        {
          node->modify_time = ccw.get_ctime_recorder().get_ctime();
        }
        node->next = NULL;
        memcpy(node->buf, ccw.get_buf(), ccw.size());
        if(NULL == uci.uc_list_tail)
        {
          uci.uc_list_head = node;
        }
        else
        {
          uci.uc_list_tail->next = node;
        }
        uci.uc_list_tail = node;
      }
      return ret;
    }

    int MemTable::copy_cells_(TransNode &tn, TEValue &value, ObUpsCompactCellWriter &ccw)
    {
      int ret = OB_SUCCESS;
      ObCellInfoNode *node = (ObCellInfoNode *) mem_tank_.node_alloc(
        static_cast<int32_t>(sizeof(ObCellInfoNode) + ccw.size()));
      if(NULL == node)
      {
        ret = OB_MEM_OVERFLOW;
      }
      else
      {
        memcpy(node->buf, ccw.get_buf(), ccw.size());
        node->next = NULL;
        node->modify_time = tn.get_trans_id();
        if(NULL == value.list_tail)
        {
          value.list_head = node;
        }
        else
        {
          value.list_tail->next = node;
        }
        value.list_tail = node;
      }
      return ret;
    }

    int MemTable::build_mtime_cell_(const int64_t mtime, const uint64_t table_id, ObUpsCompactCellWriter &ccw)
    {
      ObObj obj;
      obj.set_modifytime(mtime);
      uint64_t column_id = OB_UPS_MODIFY_TIME_COLUMN_ID(table_id);
      return ccw.append(column_id, obj);
    }

    int MemTable::update_value_(RWSessionCtx &session, const uint64_t table_id, const ObCellInfo &cell_info,
                                TEValueUCInfo &uci, ObUpsCompactCellWriter &ccw, ObBatchChecksum &bc)
    {
      int ret = OB_SUCCESS;
      if(!is_nop_(cell_info.value_))
      {
        ccw.get_ctime_recorder().add(ccw.is_row_deleted(), cell_info.value_);
        if(ccw.is_row_deleted())
        {
          if(0 != session.get_trans_id())
          {
            ret = build_mtime_cell_(session.get_trans_id(), table_id, ccw);
            uci.uc_cell_info_cnt++;
          }
          ccw.set_row_deleted(false);
        }
        if(OB_SUCCESS == ret)
        {
          if(is_delete_row_(cell_info.value_))
          {
            ret = ccw.row_delete();
            ccw.set_row_deleted(true);
          }
            //add zhaoqiong [Truncate Table]:20160318:b
          else if(is_trun_tab_(cell_info.value_))
          {
            ret = ccw.tab_truncate();
            ccw.set_tab_truncated(true);
          }
            //add:e
          else
          {
            ret = ccw.append(cell_info.column_id_, cell_info.value_);
            uci.uc_cell_info_size = static_cast<int16_t>(uci.uc_cell_info_size +
                                                         get_varchar_length_kb_(cell_info.value_));
          }
          uci.uc_cell_info_cnt++;
          bc.fill(&(cell_info.column_id_), sizeof(cell_info.column_id_));
          cell_info.value_.checksum(bc);
          YYSYS_LOG(DEBUG, "[CHECKSUM][OBJ] %ld %s", bc.calc(), print_obj(cell_info.value_));
        }
      }
      return ret;
    }

    int MemTable::update_value_(const TransNode &tn, const uint64_t table_id, ObCellInfo &cell_info, TEValue &value,
                                ObUpsCompactCellWriter &ccw, ObBatchChecksum &bc)
    {
      int ret = OB_SUCCESS;
      if(!is_nop_(cell_info.value_))
      {
        if(ccw.is_row_deleted())
        {
          ret = build_mtime_cell_(tn.get_trans_id(), table_id, ccw);
          value.cell_info_cnt++;
          ccw.set_row_deleted(false);
        }
        if(OB_SUCCESS == ret)
        {
          if(is_delete_row_(cell_info.value_))
          {
            ret = ccw.row_delete();
            ccw.set_row_deleted(true);
          }
          else
          {
            ret = ccw.append(cell_info.column_id_, cell_info.value_);
            value.cell_info_size = static_cast<int16_t>(value.cell_info_size +
                                                        get_varchar_length_kb_(cell_info.value_));
          }
          value.cell_info_cnt++;
          bc.fill(&(cell_info.column_id_), sizeof(cell_info.column_id_));
          cell_info.value_.checksum(bc);
        }
      }
      return ret;
    }

    int MemTable::get_cur_value_(RWSessionCtx &session, ILockInfo &lock_info,TEKey &cur_key,TEValue *&cur_value,
                                     TEValueUCInfo *&cur_uci, bool is_row_changed,int64_t &total_row_counter,
                                                                  int64_t &new_row_counter, ObBatchChecksum &bc)
    {
      int ret = OB_SUCCESS;
      if(is_row_changed || NULL == cur_value)
      {
        while(true)
        {
          if(NULL != (cur_value = table_engine_.get(cur_key)))
          {
            if(OB_SUCCESS != (ret = lock_info.on_write_begin(cur_key, *cur_value)))
            {
              //YYSYS_LOG(WARN, "lock info on write begin fail, ret=%d %s", ret, cur_key.log_str());
            }
            else
            {
              if(NULL == cur_value->cur_uc_info && cur_value->is_empty())
              {
                new_row_counter++;
              }
            }
            break;
          }
          else if(NULL == (cur_value = (TEValue *) mem_tank_.tevalue_alloc(sizeof(TEValue))))
          {
            ret = OB_MEM_OVERFLOW;
            break;
          }
          else
          {
            cur_value->reset();
            TEKey tmp_row_key = cur_key;
            if(OB_SUCCESS != (ret = lock_info.on_write_begin(cur_key, *cur_value)))
            {
              YYSYS_LOG(WARN, "lock info on write begin fail, ret=%d %s", ret, cur_key.log_str());
              break;
            }
            else if(OB_SUCCESS != (ret = mem_tank_.write_string(cur_key.row_key, &(tmp_row_key.row_key))))
            {
              YYSYS_LOG(WARN, "copy rowkey fail, ret=%d %s", ret, cur_key.log_str());
              break;
            }
              //mod zhaoqiong [Truncate Table]:20160318:b
              //else if (OB_SUCCESS != (ret = table_bf_.insert(tmp_row_key.table_id, tmp_row_key.row_key)))
            else if(tmp_row_key.row_key.length() > 0 &&
                    OB_SUCCESS != (ret = table_bf_.insert(tmp_row_key.table_id, tmp_row_key.row_key)))
              //mod:e
            {
              YYSYS_LOG(WARN, "insert cur_key to bloomfilter fail ret=%d %s", ret, cur_key.log_str());
              break;
            }
            else if(OB_SUCCESS != (ret = table_engine_.set(tmp_row_key, cur_value)))
            {
              if(OB_ENTRY_EXIST == ret)
              {
                YYSYS_LOG(DEBUG, "put to table_engine entry exist %s", cur_key.log_str());
                continue;
              }
              else
              {
                YYSYS_LOG(WARN, "put to table_engine fail ret=%d %s", ret, cur_key.log_str());
                break;
              }
            }
            else
            {
              new_row_counter++;
              break;
            }
          }
        }
        if(OB_SUCCESS == ret)
        {
          total_row_counter++;
          cur_key.checksum(bc);
          YYSYS_LOG(DEBUG, "[CHECKSUM][RK] %ld %s", bc.calc(), cur_key.log_str());
        }
      }
      if(OB_SUCCESS == ret && NULL != cur_value)
      {
        if(NULL == (cur_uci = cur_value->cur_uc_info) || (is_row_changed && !session.get_need_gen_mutator()))
        {
          cur_value->cur_uc_info = NULL;
          if(NULL == (cur_uci = session.alloc_tevalue_uci()))
          {
            YYSYS_LOG(WARN, "alloc tevalue callback info from session ctx fail");
            ret = OB_MEM_OVERFLOW;
          }
          else if(OB_SUCCESS != (ret = session.prepare_callback_info(session, &tevalue_cb_, cur_uci)))
          {
            YYSYS_LOG(WARN, "add tevalue callback info fail, ret=%d", ret);
          }
            //add duyr [MultiUPS] [READ_ATOMIC] [write_part] 20151117:b
          else if(OB_SUCCESS != (ret = session.prepare_trans_ver_callback(cur_uci)))
          {
            YYSYS_LOG(WARN, "add distributed trans version callback info fail, ret=%d", ret);
          }
            //add duyr 20151117:e
          else
          {
            cur_uci->value = cur_value;
            cur_uci->session_descriptor = session.get_session_descriptor();
            cur_value->cur_uc_info = cur_uci;
            YYSYS_LOG(DEBUG, "alloc uc_info value=%p uc_info=%p", cur_uci->value, cur_value->cur_uc_info);
          }
        }
        else
        {
          YYSYS_LOG(DEBUG, "fetch uc_info value=%p uc_info=%p", cur_uci->value, cur_value->cur_uc_info);
        }
        if(is_row_changed && ST_READ_WRITE == session.get_type())
        {
          if(NULL == cur_value || NULL == cur_value->cur_uc_info)
          {
          }
          else if(cur_value->cur_uc_info->uc_list_tail_before_stmt != TEValueUCInfo::INVALID_CELL_INFO_NODE)
          {
          }
          else
          {
            YYSYS_LOG(DEBUG, "mark list_tail: %p", cur_value->cur_uc_info->uc_list_tail);
            cur_value->cur_uc_info->uc_list_tail_before_stmt = cur_value->cur_uc_info->uc_list_tail;
            if(OB_SUCCESS != (ret = session.add_stmt_callback(&tevalue_stmt_cb_, cur_value->cur_uc_info)))
            {
              YYSYS_LOG(ERROR, "add tevalue_stmt callback info fail, ret=%d", ret);
            }
          }
        }
      }
      return ret;
    }

    int MemTable::get_cur_value_(TransNode &tn, TEKey &cur_key, const TEKey &prev_key, TEValue *prev_value,
                                 TEValue *&cur_value, bool is_row_changed, int64_t &total_row_counter,
                                 int64_t &new_row_counter, ObBatchChecksum &bc)
    {
      int ret = OB_SUCCESS;
      if(!is_row_changed || cur_key == prev_key)
      {
        cur_key = prev_key;
        cur_value = prev_value;
      }
      else
      {
        bool is_new_row = false;
        (void) is_new_row;
        total_row_counter++;
        if(NULL == (cur_value = table_engine_.get(cur_key)))
        {
          new_row_counter++;
          if(NULL == (cur_value = (TEValue *) mem_tank_.tevalue_alloc(sizeof(TEValue))))
          {
            ret = OB_MEM_OVERFLOW;
          }
          else
          {
            cur_value->reset();
            ObRowkey tmp_row_key;
            if(OB_SUCCESS == (ret = mem_tank_.write_string(cur_key.row_key, &tmp_row_key)))
            {
              cur_key.row_key = tmp_row_key;
              if(OB_SUCCESS != (ret = table_bf_.insert(cur_key.table_id, cur_key.row_key)))
              {
                YYSYS_LOG(WARN, "insert cur_key to bloomfilter fail ret=%d %s", ret, cur_key.log_str());
              }
              else if(OB_SUCCESS != (ret = table_engine_.set(cur_key, cur_value)))
              {
                YYSYS_LOG(WARN, "put to table_engine fail ret=%d %s", ret, cur_key.log_str());
              }
              else
              {
              }
            }
          }
        }
        if(NULL != cur_value && NULL == cur_value->list_head)
        {
          is_new_row = true;
        }
        if(OB_SUCCESS == ret)
        {
          RollbackInfo *rollback_info = (RollbackInfo *) tn.stack_alloc(sizeof(RollbackInfo));
          if(NULL == rollback_info)
          {
            ret = OB_MEM_OVERFLOW;
          }
          else
          {
            rollback_info->key = cur_key;
            rollback_info->dest = cur_value;
            rollback_info->src = *cur_value;
            if(OB_SUCCESS != (ret = tn.add_rollback_data(this, rollback_info)))
            {
              YYSYS_LOG(WARN, "add rollback data fail ret=%d", ret);
            }
          }
        }
        cur_key.checksum(bc);
      }
      return ret;
    }

    int MemTable::rewrite_tevalue(RWSessionCtx &session)
    {
        int ret = OB_SUCCESS;
        for(int64_t idx = 0; OB_SUCCESS == ret && idx < session.get_tekeys_count(); ++idx)
        {
            ret = table_engine_.update_tevalue_keybtree_and_keyhash(session.get_tekeys()->at(idx),
                                                                    &EMPTY_TEVALUE);
            if(OB_SUCCESS != ret)
            {
                YYSYS_LOG(ERROR,"failed to rewrite index tevalue, ret=%d", ret);
            }
        }
        return ret;
    }

//add by maosy  [MultiUps 1.0] [secondary index optimize]20170401 b:
int MemTable::ob_sem_handler_(RWSessionCtx &session,
                                  const ObCellInfo &cell_info,
                                  TEKey &cur_key,
                                  TEValue *&cur_value,
                                  int64_t &total_row_counter,
                                  int64_t &new_row_counter,
                                  ObBatchChecksum &bc,
                                  const ObDmlType dml_type)
    {
        YYSYS_LOG(DEBUG,"cur key = %s",to_cstring(cur_key));
        UNUSED(session);
//        UNUSED(cell_info);
        OB_ASSERT(cur_value);
        int ret = OB_SUCCESS;
        //TEKey tmp_te_key = cur_key;
        ObRowkey tmp_row_key;
        TEValue *tmp_tevalues ;
        if(OB_DML_INDEX_INSERT == dml_type)
        {
            total_row_counter++;
        }
        else
        {
            ret = OB_ERR_UNEXPECTED;
        }
        if(OB_SUCCESS!= ret )
        {}
        else if (NULL != (tmp_tevalues = (table_engine_.get(cur_key))))
        {
            if(cur_value ==tmp_tevalues)
            {
                YYSYS_LOG(DEBUG,"no need to set key and query ");
            }
            else if(&EMPTY_TEVALUE == tmp_tevalues)
            {
                ObRowkey tmpp_row_key;
                mem_tank_.write_string(cur_key.row_key, &tmpp_row_key);
                cur_key.row_key = tmpp_row_key;
                ret = table_engine_.update_tevalue_keybtree_and_keyhash(cur_key, cur_value);
                if(OB_SUCCESS != ret)
                {
                    YYSYS_LOG(WARN,"failed to update tevalue for cur_key[%s]", to_cstring(cur_key));
                }
                else if(OB_SUCCESS != (ret = session.push_tekey(cur_key)))
                {
                    YYSYS_LOG(WARN,"failed to save cur_key for update its tevalue, ret=%d", ret);
                }
            }
            else
            {
                ret = OB_MEMTABLE_UNEXPECT_ERROR;
                YYSYS_LOG(ERROR,"memtable has error ,cur key = %s,get values = %p,set values = %p",to_cstring(cur_key),tmp_tevalues,cur_value);
            }
        }
        else if (OB_SUCCESS == (ret = mem_tank_.write_string(cur_key.row_key, &tmp_row_key)))
        {
          cur_key.row_key = tmp_row_key;
          if (OB_SUCCESS != (ret = table_bf_.insert(cur_key.table_id, cur_key.row_key)))
          {
            YYSYS_LOG(WARN, "insert cur_key to bloomfilter fail ret=%d %s", ret, cur_key.log_str());
          }
          else if (OB_SUCCESS != (ret = table_engine_.set(cur_key, cur_value)))
          {
              if (OB_ENTRY_EXIST == ret)
              {
                  YYSYS_LOG(DEBUG, "put to table_engine entry exist %s", cur_key.log_str());
                  ret = OB_SUCCESS ;
              }
              else
              {
                  YYSYS_LOG(WARN, "put to table_engine fail ret=%d %s", ret, cur_key.log_str());
              }
          }
          else
          {
              if(OB_SUCCESS != (ret = session.push_tekey(cur_key)))
              {
                  YYSYS_LOG(WARN,"failed to save cur_key, ret=%d", ret);
              }
              new_row_counter++;
          }
        }

        if(OB_SUCCESS ==ret )
        {
            cur_key.checksum(bc);
            uint64_t cid = OB_INVALID_ID ;
            bc.fill(&(cid), sizeof(cid));
            cell_info.value_.checksum(bc);
            YYSYS_LOG(DEBUG,"obj = %s",to_cstring(cell_info.value_));
////            ObObj obj;
////            obj.checksum(bc);
        }
        return ret;
    }
//add by moasy e

    int MemTable::ob_sem_handler_(RWSessionCtx &session, ILockInfo &lock_info, const ObCellInfo &cell_info,
                                  TEKey &cur_key, //del const  //uncertainty updaterowky ����ʱȥ����const����
                                  TEValue *&cur_value, const bool is_row_changed, const bool is_row_finished,
                                  ObUpsCompactCellWriter &ccw, int64_t &total_row_counter, int64_t &new_row_counter,
                                  ObBatchChecksum &bc

    )
    {
      int ret = OB_SUCCESS;
      //add zhaoqiong [Truncate Table]:20160318:b
      /* check the table_value info*/
      TEKey cur_table_key;
      cur_table_key.table_id = cur_key.table_id;
      TEValue *cur_table_value;
      cur_table_value = table_engine_.get(cur_table_key);
      int64_t last_truncate_time = 0;
      if(cur_table_value != NULL && cur_table_value->cell_info_cnt != 0)
      {
        //ret = OB_TABLE_UPDATE_LOCKED;
        //YYSYS_LOG(ERROR, "table_id %ld has been locked, wait for mem_freeze", cur_key.table_id);
          last_truncate_time = cur_table_value->list_tail->modify_time;
          YYSYS_LOG(DEBUG,"table_id %ld has been recently truncated at %ld, cell_info_cnt=%d", cur_key.table_id, last_truncate_time, cur_table_value->cell_info_cnt);
      }
      //add:e

      TEValueUCInfo *cur_uci = NULL;
      if(ret == OB_SUCCESS && /*add zhaoqiong [Truncate Table]:20160318:b*/
         OB_SUCCESS ==
         (ret = get_cur_value_(session, lock_info, cur_key, cur_value, cur_uci, is_row_changed, total_row_counter,
                               new_row_counter, bc)))
      {
        if(NULL == cur_value || NULL == cur_uci)
        {
          ret = OB_MEM_OVERFLOW;
        }
        else
        {
          ret = update_value_(session, cur_key.table_id, cell_info, *cur_uci, ccw, bc);
          if(OB_SUCCESS == ret)
          {
            //if ((is_row_changed || is_row_finished)
            //    && (MAX_ROW_SIZE / 2) < cur_value->cell_info_size)
            //{
            //  // ��������ֵ��һ��ʱ ��ǿ�����¼���min flying trans id
            //  session.flush_min_flying_trans_id();
            //}
            if((is_row_changed || is_row_finished)
               &&(get_max_row_cell_num() < cur_value->cell_info_cnt
                  || MAX_ROW_SIZE < cur_value->cell_info_size
                  ||(last_truncate_time != 0 && (cur_value->is_empty() || (last_truncate_time > cur_value->list_head->modify_time)))
                  )
                    )
            {
              merge_(session, cur_key, *cur_value, cur_table_value);
              YYSYS_LOG(DEBUG," cur_value.modify_time=%ld, cur_value.cell_info_cnt=%d, last_truncate_timestamp=%ld",
                        cur_value->is_empty()? 0 : cur_value->list_head->modify_time, cur_value->cell_info_cnt, last_truncate_time);
            }
            if(is_row_finished)
            {
              OB_STAT_INC(UPDATESERVER, UPS_STAT_APPLY_ROW_COUNT);
              OB_STAT_INC(UPDATESERVER, UPS_STAT_APPLY_ROW_UNMERGED_CELL_COUNT, cur_value->cell_info_cnt);
              ccw.row_finish();
//              if(session.get_start_time_for_batch() != 0)
//              {
//                ret = copy_cells_(*cur_uci, ccw, true);
//              }
//              else
//              {
                ret = copy_cells_(*cur_uci, ccw);
//              }
              ccw.reset();
            }
            if(OB_SUCCESS == ret && session.get_need_gen_mutator() && is_row_too_long_(session, cur_key, *cur_value))
            {
              YYSYS_LOG(WARN, "row size overflow [%s] [%s]", cur_key.log_str(), cur_value->log_str());
              ret = OB_SIZE_OVERFLOW;
            }
            if(is_row_finished)
            {
              session.prepare_checksum_callback(cur_value);
              lock_info.on_precommit_end();
            }
          }
          else
          {
            YYSYS_LOG(WARN, "update value fail ret=%d %s", ret, print_cellinfo(&cell_info));
          }
        }
      }
      return ret;
    }

    int MemTable::ob_sem_handler_(TransNode &tn, ObCellInfo &cell_info, TEKey &cur_key, TEValue *&cur_value,
                                  const TEKey &prev_key, TEValue *prev_value, bool is_row_changed, bool is_row_finished,
                                  ObUpsCompactCellWriter &ccw, int64_t &total_row_counter, int64_t &new_row_counter,
                                  ObBatchChecksum &bc)
    {
      int ret = OB_SUCCESS;
      if(OB_SUCCESS ==
         (ret = get_cur_value_(tn, cur_key, prev_key, prev_value, cur_value, is_row_changed, total_row_counter,
                               new_row_counter, bc)))
      {
        if(NULL == cur_value)
        {
          ret = OB_MEM_OVERFLOW;
        }
        else
        {
          ret = update_value_(tn, cur_key.table_id, cell_info, *cur_value, ccw, bc);
          if(OB_SUCCESS == ret && is_row_finished)
          {
            ccw.row_finish();
            ret = copy_cells_(tn, *cur_value, ccw);
            ccw.reset();
            if(get_max_row_cell_num() < cur_value->cell_info_cnt || MAX_ROW_SIZE < cur_value->cell_info_size)
            {
              merge_(tn, cur_key, *cur_value);
            }
          }
          if(MAX_ROW_SIZE < cur_value->cell_info_size)
          {
            YYSYS_LOG(WARN, "row size overflow [%s] [%s]", cur_key.log_str(), cur_value->log_str());
            ret = OB_SIZE_OVERFLOW;
          }
        }
      }
      return ret;
    }

    bool MemTable::is_row_too_long_(const RWSessionCtx &session, const TEKey &te_key, const TEValue &te_value)
    {
      bool bret = true;
      if(MAX_ROW_SIZE > (te_value.cell_info_size + te_value.cur_uc_info->uc_cell_info_size))
      {
        bret = false;
      }
      else
      {
        YYSYS_LOG(INFO, "maybe row is too long calculate the compacted size of whole row %s %s", te_key.log_str(),
                  te_value.log_str());
        ObRowCompaction *rc_iter = GET_TSI_MULT(ObRowCompaction, TSI_UPS_ROW_COMPACTION_1);
        if(NULL == rc_iter)
        {
          YYSYS_LOG(WARN, "get tsi ObRowCompaction fail");
        }
        else
        {
          MemTableGetIter get_iter;
          get_iter.set_(te_key, &te_value, NULL, true, &session);
          rc_iter->set_iterator(&get_iter);
          rc_iter->set_frozen_schema(NULL); //[403 bugfix]
          int64_t size = 0;
          int tmp_ret = OB_SUCCESS;
          while(OB_SUCCESS == (tmp_ret = rc_iter->next_cell()))
          {
            ObCellInfo *ci = NULL;
            if(OB_SUCCESS != (tmp_ret = rc_iter->get_cell(&ci)) || NULL == ci)
            {
              tmp_ret = (OB_SUCCESS == tmp_ret) ? OB_ERROR : tmp_ret;
              break;
            }
            size += get_varchar_length_kb_(ci->value_);
          }
          if(OB_ITER_END != tmp_ret)
          {
            YYSYS_LOG(WARN, "iter ret=%d unexpected %s %s", tmp_ret, te_key.log_str(), te_value.log_str());
          }
          else
          {
            if(MAX_ROW_SIZE > size)
            {
              YYSYS_LOG(INFO, "compacted size=%ld not too long", size);
              bret = false;
            }
            else
            {
              YYSYS_LOG(WARN, "compacted size=%ld too long", size);
            }
          }
        }
      }
      return bret;
    }

    int MemTable::merge_(RWSessionCtx &session,
                         const TEKey &te_key,
                         TEValue &te_value,
                         TEValue *table_value)
    {
      int ret = OB_SUCCESS;
      int64_t timeu = yysys::CTimeUtil::getTime();

      TEValue new_value;
      new_value.reset();
      new_value.index_stat = te_value.index_stat;
      new_value.cur_uc_info = te_value.cur_uc_info;
      new_value.row_lock = te_value.row_lock;
      new_value.undo_list = te_value.undo_list;

      int64_t last_truncate_timestamp = 0;
      if(table_value != NULL && table_value->cell_info_cnt > 0)
      {
          last_truncate_timestamp = table_value->list_tail->modify_time;
      }

      MemTableGetIter get_iter;
      BaseSessionCtx merge_session(session.get_type(), session.get_host());
      //merge_session.set_trans_id(session.get_min_flying_trans_id());
      merge_session.set_trans_id(session.get_trans_id());
      //get_iter.set_(te_key, &te_value, NULL, false, &merge_session);
      get_iter.set_(te_key, &te_value, NULL, false, &merge_session,NULL, NULL, table_value);
      ObRowCompaction *rc_iter = GET_TSI_MULT(ObRowCompaction, TSI_UPS_ROW_COMPACTION_1);
      FixedSizeBuffer<OB_MAX_PACKET_LENGTH> *tbuf = GET_TSI_MULT(FixedSizeBuffer<OB_MAX_PACKET_LENGTH>,
                                                                 TSI_UPS_FIXED_SIZE_BUFFER_2);
      if(NULL == rc_iter)
      {
        YYSYS_LOG(WARN, "get tsi ObRowCompaction fail");
        ret = OB_ERROR;
      }
      else if(NULL == tbuf)
      {
        YYSYS_LOG(WARN, "get tsi FixedSizeBuffer fail");
        ret = OB_ERROR;
      }
      else
      {
        rc_iter->set_iterator(&get_iter);
        rc_iter->set_frozen_schema(NULL);//[403 bugfix]
      }
      ObUpsCompactCellWriter ccw;
      if(OB_SUCCESS == ret)
      {
        ccw.init(tbuf->get_buffer(), tbuf->get_size(), &mem_tank_);
      }
      int64_t mtime = 0;
      while(OB_SUCCESS == ret && OB_SUCCESS == (ret = rc_iter->next_cell()))
      {
        ObCellInfo *ci = NULL;
        ret = rc_iter->get_cell(&ci);
        if(OB_SUCCESS != ret)
        {
          break;
        }
        if(NULL == ci)
        {
          ret = OB_ERROR;
          break;
        }
        if(is_row_not_exist_(ci->value_))
        {
          ret = OB_EAGAIN;
          break;
        }
        if(ObModifyTimeType == ci->value_.get_type())
        {
          int64_t tmp = 0;
          ci->value_.get_modifytime(tmp);
          if(tmp > mtime)
          {
            mtime = tmp;
          }
        }

        if(is_delete_row_(ci->value_))
        {
          ret = ccw.row_delete();
        }
        else
        {
          ret = ccw.append(ci->column_id_, ci->value_);
        }
        if(OB_SUCCESS != ret)
        {
          break;
        }
        YYSYS_LOG(DEBUG, "merge new obj [%s]", print_obj(ci->value_));
        new_value.cell_info_cnt++;
        new_value.cell_info_size = static_cast<int16_t>(new_value.cell_info_size + get_varchar_length_kb_(ci->value_));
      }

      if(0 == mtime && 0 < ccw.size())
      {
        ObCellInfoNode *end = const_cast<ObCellInfoNode *>(get_iter.get_cur_node_iter_());
        ObCellInfoNode *iter = te_value.list_head;
        while(NULL != iter)
        {
          if(end == iter->next)
          {
            mtime = iter->modify_time;
            break;
          }
          iter = iter->next;
        }
      }

      //add duyr [MultiUPS] [READ_ATOMIC] [write_part] 20151221:b
      const char *trans_ver_buf = NULL;
      int64_t buf_len = 0;
      if(0 < ccw.size())
      {
        ObCellInfoNode *end = const_cast<ObCellInfoNode *>(get_iter.get_cur_node_iter_());
        ObCellInfoNode *iter = te_value.list_head;
        while(NULL != iter)
        {
          if(end == iter->next)
          {
            trans_ver_buf = iter->trans_version_;
            buf_len = static_cast<int64_t>(sizeof(iter->trans_version_));
            break;
          }
          iter = iter->next;
        }
      }
      //add duyr 20151221:e

      if(OB_ITER_END == ret)
      {
        ret = OB_SUCCESS;
        if(mtime <= last_truncate_timestamp)
        {
            mtime = last_truncate_timestamp;
        }
        if(0 < ccw.size())
        {
          ccw.row_finish();
          ObCellInfoNode *node = (ObCellInfoNode *) mem_tank_.node_alloc(
            static_cast<int32_t>(sizeof(ObCellInfoNode) + ccw.size()));
          if(NULL == node)
          {
            ret = OB_MEM_OVERFLOW;
          }
            //add duyr [MultiUPS] [READ_ATOMIC] [write_part] 20151221:b
          else if(OB_SUCCESS != (ret = common::ObReadAtomicHelper::init_buf_with_transver(node->trans_version_,
                                                                                          static_cast<int64_t>(sizeof(node->trans_version_)))))
          {
            YYSYS_LOG(WARN, "fail to init transver_buf!ret=%d", ret);
          }
            //add duyr 20151221:e
          else
          {
            //add duyr [MultiUPS] [READ_ATOMIC] [write_part] 20151221:b
            if(NULL != trans_ver_buf)
            {
              memcpy(node->trans_version_, trans_ver_buf, buf_len);
            }
            //add duyr 20151221:e
            memcpy(node->buf, ccw.get_buf(), ccw.size());
            node->next = NULL;
            node->modify_time = mtime;
            new_value.list_head = node;
            new_value.list_tail = node;
          }
        }
      }

      if(OB_SUCCESS == ret && NULL != new_value.list_head && NULL != new_value.list_tail)
      {
        ObCellInfoNode *node = const_cast<ObCellInfoNode *>(get_iter.get_cur_node_iter_());
        if(NULL != node)
        {
          new_value.list_tail->next = node;
          if(NULL != te_value.cur_uc_info && node == te_value.cur_uc_info->uc_list_head)
          {
            // �Ѿ�������δ�ύ���� ���list_tail�����µ�list_head
            new_value.list_tail = new_value.list_head;
          }
          else
          {
            new_value.list_tail = te_value.list_tail;
          }
          while(NULL != node && node != new_value.list_tail)
          {
            std::pair<int64_t, int64_t> sc = node->get_size_and_cnt();
            new_value.cell_info_cnt = static_cast<int16_t>(new_value.cell_info_cnt + sc.first);
            new_value.cell_info_size = static_cast<int16_t>(new_value.cell_info_size + sc.second);
            node = node->next;
          }
        }
        timeu = yysys::CTimeUtil::getTime() - timeu;
        YYSYS_LOG(DEBUG, "merge te_value succ, key-value: [%s] [%s] ==> [%s] value=%p timeu=%ld", te_key.log_str(),
                  te_value.log_str(), new_value.log_str(), &te_value, timeu);
        YYSYS_LOG(DEBUG, "merge te_value succ, list: [%s] ==> [%s] value=%p timeu=%ld", te_value.log_list(),
                  new_value.log_list(), &te_value, timeu);
        if(NULL != new_value.list_head)
        {
          ObUndoNode *undo_node = (ObUndoNode *) mem_tank_.undo_node_alloc(sizeof(ObUndoNode));
          if(NULL == undo_node)
          {
            YYSYS_LOG(WARN, "alloc undo node fail");
          }
          else
          {
            undo_node->head = te_value.list_head;
            undo_node->next = new_value.undo_list;
            new_value.undo_list = undo_node;
            // change te_value to new_value
            te_value = new_value;
            OB_STAT_INC(UPDATESERVER, UPS_STAT_MERGE_COUNT, 1);
            OB_STAT_INC(UPDATESERVER, UPS_STAT_MERGE_TIMEU, timeu);
          }
        }
      }
      return ret;
    }

    int MemTable::merge_(const TransNode &tn, const TEKey &te_key, TEValue &te_value)
    {
      int ret = OB_SUCCESS;
      int64_t timeu = yysys::CTimeUtil::getTime();

      TEValue new_value;
      new_value.reset();

      MemTableGetIter get_iter;
      TransNodeWrapper4Merge trans_node_wrapper(tn);
      get_iter.set_(te_key, &te_value, NULL, &trans_node_wrapper);
      ObRowCompaction *rc_iter = GET_TSI_MULT(ObRowCompaction, TSI_UPS_ROW_COMPACTION_1);
      FixedSizeBuffer<OB_MAX_PACKET_LENGTH> *tbuf = GET_TSI_MULT(FixedSizeBuffer<OB_MAX_PACKET_LENGTH>,
                                                                 TSI_UPS_FIXED_SIZE_BUFFER_2);
      if(NULL == rc_iter)
      {
        YYSYS_LOG(WARN, "get tsi ObRowCompaction fail");
        ret = OB_ERROR;
      }
      else if(NULL == tbuf)
      {
        YYSYS_LOG(WARN, "get tsi FixedSizeBuffer fail");
        ret = OB_ERROR;
      }
      else
      {
        rc_iter->set_iterator(&get_iter);
        rc_iter->set_frozen_schema(NULL);
      }
      ObUpsCompactCellWriter ccw;
      if(OB_SUCCESS == ret)
      {
        ccw.init(tbuf->get_buffer(), tbuf->get_size(), &mem_tank_);
      }
      int64_t mtime = 0;
      while(OB_SUCCESS == ret && OB_SUCCESS == (ret = rc_iter->next_cell()))
      {
        ObCellInfo *ci = NULL;
        ret = rc_iter->get_cell(&ci);
        if(OB_SUCCESS != ret)
        {
          break;
        }
        if(NULL == ci)
        {
          ret = OB_ERROR;
          break;
        }
        if(is_row_not_exist_(ci->value_))
        {
          ret = OB_EAGAIN;
          break;
        }
        if(0 == mtime && ObModifyTimeType == ci->value_.get_type())
        {
          ci->value_.get_modifytime(mtime);
        }

        if(is_delete_row_(ci->value_))
        {
          ret = ccw.row_delete();
        }
        else
        {
          ret = ccw.append(ci->column_id_, ci->value_);
        }
        if(OB_SUCCESS != ret)
        {
          break;
        }
        YYSYS_LOG(DEBUG, "merge new obj [%s]", print_obj(ci->value_));
        new_value.cell_info_cnt++;
        new_value.cell_info_size = static_cast<int16_t>(new_value.cell_info_size + get_varchar_length_kb_(ci->value_));
      }

      if(OB_ITER_END == ret)
      {
        ret = OB_SUCCESS;
        if(0 < ccw.size())
        {
          ccw.row_finish();
          ObCellInfoNode *node = (ObCellInfoNode *) mem_tank_.node_alloc(
            static_cast<int32_t>(sizeof(ObCellInfoNode) + ccw.size()));
          if(NULL == node)
          {
            ret = OB_MEM_OVERFLOW;
          }
          else
          {
            memcpy(node->buf, ccw.get_buf(), ccw.size());
            node->next = NULL;
            node->modify_time = mtime;
            new_value.list_head = node;
            new_value.list_tail = node;
          }
        }
      }

      if(OB_SUCCESS == ret)
      {
        ObCellInfoNode *node = const_cast<ObCellInfoNode *>(get_iter.get_cur_node_iter_());
        if(NULL != node)
        {
          if(NULL == new_value.list_tail)
          {
            new_value.list_head = node;
          }
          else
          {
            new_value.list_tail->next = node;
          }
          new_value.list_tail = te_value.list_tail;
          while(NULL != node)
          {
            std::pair<int64_t, int64_t> sc = node->get_size_and_cnt();
            new_value.cell_info_cnt = static_cast<int16_t>(new_value.cell_info_cnt + sc.first);
            new_value.cell_info_size = static_cast<int16_t>(new_value.cell_info_size + sc.second);
            node = node->next;
          }
        }
        timeu = yysys::CTimeUtil::getTime() - timeu;
        YYSYS_LOG(DEBUG, "merge te_value succ [%s] [%s] ==> [%s] timeu=%ld", te_key.log_str(), te_value.log_str(),
                  new_value.log_str(), timeu);
        if(NULL != new_value.list_head)
        {
          // change te_value to new_value
          te_value = new_value;
        }
      }
      return ret;
    }

    int MemTable::set(const MemTableTransDescriptor td, ObUpsMutator &mutator, const bool check_checksum,
                      ObUpsTableMgr *ups_table_mgr, ObScanner *scanner)
    {
      int ret = OB_SUCCESS;
      TransNode *tn = NULL;
      FixedSizeBuffer<OB_MAX_PACKET_LENGTH> *tbuf = GET_TSI_MULT(FixedSizeBuffer<OB_MAX_PACKET_LENGTH>,
                                                                 TSI_UPS_FIXED_SIZE_BUFFER_1);
      ObBatchChecksum bc;
      bc.set_base(0);
      if(!inited_)
      {
        YYSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }
      else if(NULL == tbuf)
      {
        YYSYS_LOG(WARN, "get tsi FixedSizeBuffer fail");
        ret = OB_MEM_OVERFLOW;
      }
      else if(NULL == (tn = trans_mgr_.get_trans_node(td)))
      {
        YYSYS_LOG(WARN, "get trans node fail td=%lu", td);
        ret = OB_ERROR;
      }
      else if(NULL != scanner && NULL == ups_table_mgr)
      {
        YYSYS_LOG(WARN, "to return scanner ups_table_mgr must not null");
        ret = OB_INVALID_ARGUMENT;
      }
      else if(check_checksum && checksum_before_mutate_ != mutator.get_memtable_checksum_before_mutate())
      {
        handle_checksum_error(mutator);
        ret = OB_ERROR;
      }
      else
      {
        int64_t total_row_counter = 0;
        int64_t new_row_counter = 0;
        int64_t cell_counter = 0;
        TEKey cur_key;
        TEValue *cur_value = NULL;
        TEKey prev_key;
        TEValue *prev_value = NULL;
        CellInfoProcessor ci_proc;
        ObMutatorCellInfo *mutator_cell_info = NULL;
        ObCellInfo *cell_info = NULL;
        ObUpsCompactCellWriter ccw;
        ccw.init(tbuf->get_buffer(), tbuf->get_size(), &mem_tank_);
        while(OB_SUCCESS == ret && OB_SUCCESS == (ret = mutator.get_mutator().next_cell()))
        {
          bool is_row_changed = false;
          bool is_row_finished = false;
          if(OB_SUCCESS ==
             (ret = mutator.get_mutator().get_cell(&mutator_cell_info, &is_row_changed, &is_row_finished)) &&
             NULL != mutator_cell_info)
          {
            cell_info = &(mutator_cell_info->cell_info);
            if(!ci_proc.analyse_syntax(*mutator_cell_info))
            {
              ret = OB_ERROR;
            }
            else if(ci_proc.need_skip())
            {
              continue;
            }
            else
            {
              YYSYS_LOG(DEBUG, "trans set cell_info %s irc=%s irf=%s trans_id=%ld", print_cellinfo(cell_info),
                        STR_BOOL(is_row_changed), STR_BOOL(is_row_finished), tn->get_trans_id());
              cur_key.table_id = cell_info->table_id_;
              cur_key.row_key = cell_info->row_key_;
              if(!ci_proc.is_db_sem())
              {
                ret = ob_sem_handler_(*tn, *cell_info, cur_key, cur_value, prev_key, prev_value, is_row_changed,
                                      is_row_finished, ccw, total_row_counter, new_row_counter, bc);
              }
              else
              {
                ret = OB_NOT_SUPPORTED;
                YYSYS_LOG(WARN, "can not handle db sem now %s", print_cellinfo(cell_info));
              }
              if(OB_SUCCESS == ret)
              {
                if(ci_proc.need_return())
                {
                  if(NULL == scanner)
                  {
                    YYSYS_LOG(WARN, "need return apply result but scanner null pointer");
                  }
                  else
                  {
                    ret = OB_NOT_SUPPORTED;
                    YYSYS_LOG(ERROR, "not support return result");
                  }
                }
                prev_key = cur_key;
                prev_value = cur_value;
              }
              cell_counter++;
            }
            // �������mutatorȫת��Ϊupdate����
            mutator_cell_info->op_type.set_ext(common::ObActionFlag::OP_UPDATE);
          }
          else
          {
            YYSYS_LOG(WARN, "mutator get cell fail ret=%d", ret);
          }
        }
        ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
        if(OB_SUCCESS == ret)
        {
          CommitInfo *commit_info = (CommitInfo *) tn->stack_alloc(sizeof(CommitInfo));
          if(NULL == commit_info)
          {
            ret = OB_MEM_OVERFLOW;
          }
          else
          {
            commit_info->row_counter = new_row_counter;
            ret = tn->add_commit_data(this, commit_info);
          }
        }
        mutator.reset_iter();
        int64_t trans_id = tn->get_trans_id();
        bc.fill(&trans_id, sizeof(trans_id));
        FILL_TRACE_LOG("total_row_num=%ld new_row_num=%ld cell_num=%ld trans_id=%ld ret=%d", total_row_counter,
                       new_row_counter, cell_counter, trans_id, ret);
      }
      int64_t cur_mutator_checksum = bc.calc();
      checksum_after_mutate_ = ob_crc64(checksum_after_mutate_, &cur_mutator_checksum, sizeof(cur_mutator_checksum));
      if(OB_ERROR == ret && mem_tank_.mem_over_limit())
      {
        ret = OB_MEM_OVERFLOW;
      }
      if(OB_SUCCESS == ret)
      {
        if(check_checksum && checksum_after_mutate_ != mutator.get_memtable_checksum_after_mutate())
        {
          handle_checksum_error(mutator);
          ret = OB_ERROR;
        }
        else
        {
          mutator.set_memtable_checksum_before_mutate(checksum_before_mutate_);
          mutator.set_memtable_checksum_after_mutate(checksum_after_mutate_);
          mutator.set_mutate_timestamp(tn->get_trans_id());
        }
      }
      return ret;
    }

    int MemTable::check_checksum(const uint64_t checksum2check, const uint64_t checksum_before_mutate,
                                 const uint64_t checksum_after_mutate)
    {
      int err = OB_SUCCESS;
      if(checksum_ != checksum_before_mutate || checksum2check != checksum_after_mutate)
      {
        err = OB_CHECKSUM_ERROR;
        YYSYS_LOG(ERROR, "checksum_error(mutator[%lu->%lu], memtable[%lu->%lu], last_trans_id=%ld",
                  checksum_before_mutate, checksum_after_mutate, checksum_, checksum2check, last_trans_id_);
      }
      return err;
    }

    int MemTable::set(RWSessionCtx &session_ctx,
                      ObIterator &iter,
                      const ObDmlType dml_type
                      //add by maosy[MultiUps 1.0] [secondary index optimize]20170401 b:
                      ,const ObApplyState is_index)
                      // add by maosy e

    {
      int ret = OB_SUCCESS;
      ILockInfo *lock_info = session_ctx.get_lock_info();
      FixedSizeBuffer<OB_MAX_PACKET_LENGTH> *tbuf = GET_TSI_MULT(FixedSizeBuffer<OB_MAX_PACKET_LENGTH>,
                                                                 TSI_UPS_FIXED_SIZE_BUFFER_1);
      ObBatchChecksum bc;
      bc.set_base(session_ctx.get_uc_info().uc_checksum);
      if(!inited_)
      {
        YYSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }
      else if(NULL == lock_info)
      {
        YYSYS_LOG(ERROR, "lock info null pointer");
        ret = OB_ERR_UNEXPECTED;
      }
      else if(NULL == tbuf)
      {
        YYSYS_LOG(WARN, "get tsi FixedSizeBuffer fail");
        ret = OB_MEM_OVERFLOW;
      }
      else
      {
          bool is_truncate_flag = false;
        int64_t total_row_counter = 0;
        int64_t new_row_counter = 0;
        int64_t cell_counter = 0;
        TEKey cur_key;
        TEValue *cur_value = NULL;
        CellInfoProcessor ci_proc;
        ObCellInfo *cell_info = NULL;
        ObUpsCompactCellWriter ccw;
        ccw.init(tbuf->get_buffer(), tbuf->get_size(), &mem_tank_);
        //add by maosy[MultiUps 1.0][secondary index optimize]20170401 b:
        int64_t index =0;//TEKEY��ӦTEVALUES��index
        int64_t replace_index = 0;//���е��ڼ���replace��
        // add by maosy
        while(OB_SUCCESS == ret && OB_SUCCESS == (ret = iter.next_cell()))
        {
            bool is_row_changed = false;
            bool is_row_finished = false;
            if(OB_SUCCESS != (ret = iter.get_cell(&cell_info, &is_row_changed)) ||
                    OB_SUCCESS != (ret = iter.is_row_finished(&is_row_finished)) || NULL == cell_info)
            {
                ret = (OB_SUCCESS == ret) ? OB_ERROR : ret;
                YYSYS_LOG(WARN, "iterator get cell fail ret=%d", ret);
            }
            else
            {
                YYSYS_LOG(DEBUG, "trans set cell_info %s irc=%s irf=%s", print_cellinfo(cell_info),
                          STR_BOOL(is_row_changed), STR_BOOL(is_row_finished));
                cur_key.table_id = cell_info->table_id_;
                cur_key.row_key = cell_info->row_key_;
                if(cell_info->row_key_.length() == 0)
                {
                    is_truncate_flag = true;
                }
                if(0 == cell_counter)
                {
                    FILL_TRACE_BUF(session_ctx.get_tlog_buffer(), "first row %s", cur_key.log_str());
                }
                if(!ci_proc.is_db_sem())
                {
                    //add by maosy[MultiUps 1.0] [secondary index optimize]20170401 b:
                    if(is_index ==INDEX_TABLE ||is_index == INDEX_REPLACE_DEL )
                    {
                        if(is_row_finished)
                        {
                            ObObj value;
                            if(is_index == INDEX_REPLACE_DEL)
                            {
                                index = session_ctx.get_tevalues_index(replace_index);
                                value.set_int(index);
                            }
                            cell_info->value_ = value ;
                            cur_value = session_ctx.get_tevalues(index);

                            YYSYS_LOG(DEBUG,"INDEX=%lu,session = %p,tekey = %s,cur_value=%p",
                                      index,&session_ctx,to_cstring(cur_key),cur_value);
                            ret= ob_sem_handler_(session_ctx, *cell_info,
                                                 cur_key,cur_value,total_row_counter,
                                                 new_row_counter, bc, dml_type
                                                 );
                        }
                    }
                    else
                    {
                        //add by maosy e
                        ret = ob_sem_handler_(session_ctx, *lock_info, *cell_info, cur_key, cur_value, is_row_changed,
                                              is_row_finished, ccw, total_row_counter, new_row_counter, bc);
                        // //add by maosy [MultiUps 1.0][secondary index optimize]20170401 b:
                        if(is_index==PRIMARY_TABLE && is_row_finished && OB_SUCCESS == ret) //mod [324 bugfix]
                        {
                            YYSYS_LOG(DEBUG,"put the tevalues= %p,sess=%p,tekey = %s",
                                      cur_value,&session_ctx,to_cstring(cur_key));
                            if(OB_SUCCESS !=(ret = session_ctx.push_tevalues(cur_value)))
                            {
                                YYSYS_LOG(WARN,"failed to push ,= %d",ret );
                            }
                            else
                            {
                                YYSYS_LOG(DEBUG,"tevalues = %p,index = %ld",
                                          session_ctx.get_tevalues(session_ctx.get_tevalues_count()-1), session_ctx.get_tevalues_count()-1);
                            }
                        }
                        // add by maosy e
                    }
                }
                else
                {
                    ret = OB_NOT_SUPPORTED;
                    YYSYS_LOG(WARN, "can not handle db sem now %s", print_cellinfo(cell_info));
                }
                cell_counter++;
                if(OB_SUCCESS == ret)
                {
                    // �������mutatorȫת��Ϊupdate����
                    if(!session_ctx.get_need_gen_mutator())
                    {
                    }
                    else if(is_row_changed &&
                            OB_SUCCESS != (ret = session_ctx.get_ups_mutator().get_mutator().set_dml_type(dml_type)))
                    {
                        YYSYS_LOG(WARN, "set dml type to mutator fail, ret=%d", ret);
                    }
                    //[244 bug fix]
                    else if (is_row_changed && (OB_SUCCESS != (ret = session_ctx.add_table_item(cur_key.table_id))))
                    {
                        YYSYS_LOG(WARN,"failed to add table_id=%ld into session_ctx.table_map", cur_key.table_id);
                    }
                    //add by maosy [MultiUps 1.0][secondary index optimize]20170401 b:
                    if(is_index < INDEX_TABLE)
                    {
                        // add by maosy e
                        if (OB_SUCCESS == ret &&
                                OB_SUCCESS != (ret = session_ctx.get_ups_mutator().get_mutator().update(cell_info->table_id_,
                                                                                                        cell_info->row_key_,
                                                                                                        cell_info->column_id_,
                                                                                                        cell_info->value_)))
                        {
                            YYSYS_LOG(WARN, "add cell info to mutator fail, ret=%d", ret);
                        }
                        else if(is_row_finished &&
                                OB_SUCCESS != (ret = session_ctx.get_ups_mutator().get_mutator().add_row_barrier()))
                        {
                            YYSYS_LOG(WARN, "add rowkey barrier to mutator fail, ret=%d", ret);
                        }
                    }
                    //add by maosy [MultiUps 1.0][secondary index optimize]20170401 b:
                    if(is_row_finished && is_index > PRIMARY_TABLE)
                    {

                        uint64_t cid = OB_INVALID_ID ;
                        const ObObj obj;
                        if(is_index == INDEX_REPLACE_DEL)
                        {
                            const_cast<ObObj *>(&obj)->set_int(index);
                            YYSYS_LOG(DEBUG,"index = %ld",index);
                            replace_index++;
                        }
                        index++;
                        if (OB_SUCCESS == ret &&
                                OB_SUCCESS != (ret = session_ctx.get_ups_mutator().get_mutator().update(cell_info->table_id_,
                                                                                                        cell_info->row_key_,
                                                                                                        cid,
                                                                                                        obj)))
                        {
                            YYSYS_LOG(WARN, "add cell info to mutator fail, ret=%d", ret);
                        }
                        else if (is_row_finished
                                 && OB_SUCCESS != (ret = session_ctx.get_ups_mutator().get_mutator().add_row_barrier()))
                        {
                            YYSYS_LOG(WARN, "add rowkey barrier to mutator fail, ret=%d", ret);
                        }
                    }
                    //add by maosy e
                }
            }
        }// end while
        ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;

        FILL_TRACE_BUF(session_ctx.get_tlog_buffer(),
                       "dml_type=%s total_row_num=%ld new_row_num=%ld cell_num=%ld ret=%d", str_dml_type(dml_type),
                       total_row_counter, new_row_counter, cell_counter, ret);
        if(is_truncate_flag)
        {
            ret = session_ctx.get_ups_mutator().set_trun_flag();
        }
        if(OB_SUCCESS == ret)
        {
          session_ctx.get_ups_result().set_affected_rows(total_row_counter);
          session_ctx.get_uc_info().uc_row_counter += new_row_counter;
          session_ctx.get_uc_info().uc_checksum = bc.calc();
        }
      }
      return ret;
    }

    int MemTable::set(RWSessionCtx &session_ctx, ILockInfo &lock_info, ObMutator &mutator)
    {
      int ret = OB_SUCCESS;
      FixedSizeBuffer<OB_MAX_PACKET_LENGTH> *tbuf = GET_TSI_MULT(FixedSizeBuffer<OB_MAX_PACKET_LENGTH>,
                                                                 TSI_UPS_FIXED_SIZE_BUFFER_1);
      ObBatchChecksum bc;
      bc.set_base(session_ctx.get_uc_info().uc_checksum);
      session_ctx.mark_stmt();
      if(!inited_)
      {
        YYSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }
      else if(NULL == tbuf)
      {
        YYSYS_LOG(WARN, "get tsi FixedSizeBuffer fail");
        ret = OB_MEM_OVERFLOW;
      }
      else
      {
          bool is_truncate_flag = false;
        int64_t total_row_counter = 0;
        int64_t new_row_counter = 0;
        int64_t cell_counter = 0;
        TEKey cur_key;
        TEValue *cur_value = NULL;
        CellInfoProcessor ci_proc;
        ObMutatorCellInfo *mutator_cell_info = NULL;
        ObCellInfo *cell_info = NULL;
        ObUpsCompactCellWriter ccw;
        ccw.init(tbuf->get_buffer(), tbuf->get_size(), &mem_tank_);
        //add by maosy [secondary index optimize]20170401 b:
        int64_t index = 0 ;
        bool is_replace_start = false;//���replace��start�Ŀ�ʼ
        // add by maosy e
        while(OB_SUCCESS == ret && OB_SUCCESS == (ret = mutator.next_cell()))
        {
            //add by maosy[MultiUps 1.0] [secondary index optimize]20170401 b:
            //first step clear last statement tevales;
            if(mutator.is_statement_end())
            {
                YYSYS_LOG(DEBUG,"the end of query ");
                session_ctx.clear_tevalues();
                index =0 ;
                if(!session_ctx.get_need_gen_mutator())
                {
                }
                else if(OB_SUCCESS != (ret = session_ctx.get_ups_mutator().get_mutator().add_stat_barrier()))
                {
                    YYSYS_LOG(WARN, "add query barrier to mutator fail, ret=%d", ret);
                }
            }
            // add by maosy e
          bool is_row_changed = false;
          bool is_row_finished = false;
          if(OB_SUCCESS != (ret = mutator.get_cell(&mutator_cell_info, &is_row_changed, &is_row_finished)) ||
             NULL == mutator_cell_info)
          {
            ret = (OB_SUCCESS == ret) ? OB_ERROR : ret;
            YYSYS_LOG(WARN, "mutator get cell fail ret=%d", ret);
          }
          else
          {
            cell_info = &(mutator_cell_info->cell_info);
            //add by maosy [MultiUps 1.0][secondary index optimize]20170401 b:
            ObDmlType dml_type = OB_DML_UNKNOW;
            mutator.get_curr_node_dmltype(dml_type);
            // add by maosy e
            if(!ci_proc.analyse_syntax(*mutator_cell_info))
            {
              ret = OB_ERROR;
            }
            else if(ci_proc.need_skip())
            {
              continue;
            }
            else
            {
              YYSYS_LOG(DEBUG, "trans set value=%p cell_info %s irc=%s irf=%s", cur_value, print_cellinfo(cell_info),
                        STR_BOOL(is_row_changed), STR_BOOL(is_row_finished));
              cur_key.table_id = cell_info->table_id_;
              cur_key.row_key = cell_info->row_key_;
              if(cell_info->row_key_.length() == 0)
              {
                  is_truncate_flag = true;
              }
              if(0 == cell_counter)
              {
                FILL_TRACE_BUF(session_ctx.get_tlog_buffer(), "first row %s", cur_key.log_str());
              }
              if(!ci_proc.is_db_sem())
              {
                  //add by maosy[MultiUps 1.0] [secondary index optimize]20170401 b:
                  if(OB_DML_INDEX_INSERT == dml_type)
                  {
                      if(is_row_finished)
                      {
                          if(index == session_ctx.get_tevalues_count())
                          {
                              //YYSYS_LOG(INFO,"the first index over");
                              index =0;
                          }
                          if(cell_info->value_.get_type()==ObIntType )// �����replace�ĵ�һ�׶ˣ�
                          {
                              cell_info->value_.get_int(index);
                              is_replace_start =true;
                          }
                          else if(is_replace_start)
                              //��һ������int��type�������replace�ĵ�һ�׶εĽ���
                          {
                              index =0;
                              is_replace_start =false ;
                          }

                          cur_value = session_ctx.get_tevalues(index);
                          YYSYS_LOG(DEBUG,"put the tevalues= %p,sess=%p,tekey = %s",
                                    cur_value,&session_ctx,to_cstring(cur_key));
                          index++;
                          ret= ob_sem_handler_(session_ctx,*cell_info,
                                               cur_key,cur_value,total_row_counter,
                                               new_row_counter, bc, dml_type
                                               );
                      }
                  }
                  else
                  {
                      //add by maosy e
                      ret = ob_sem_handler_(session_ctx, lock_info, *cell_info, cur_key, cur_value, is_row_changed,
                                            is_row_finished, ccw, total_row_counter, new_row_counter, bc);
                      //add by maosy [MultiUps 1.0][secondary index optimize]20170401 b:
                      //mod [324 bugfix]
                      if(OB_SUCCESS == ret && is_row_finished )
                      {
                          YYSYS_LOG(DEBUG,"put the tevalues= %p,sess=%p,tekey = %s",
                                    cur_value,&session_ctx,to_cstring(cur_key));
                          //session_ctx.push_tevalues(cur_value);
                          if(OB_SUCCESS != (ret = session_ctx.push_tevalues(cur_value)))
                          {
                              YYSYS_LOG(WARN,"failed to push ,ret = %d", ret);
                          }
                      }
                      // add by maosy e
                  }
              }
              else
              {
                ret = OB_NOT_SUPPORTED;
                YYSYS_LOG(WARN, "can not handle db sem now %s", print_cellinfo(cell_info));
              }
              cell_counter++;
            }
            if(OB_SUCCESS == ret)
            {
              // �������mutatorȫת��Ϊupdate����
              mutator_cell_info->op_type.set_ext(common::ObActionFlag::OP_UPDATE);
              if(!session_ctx.get_need_gen_mutator())
              {
              }
              else if(OB_SUCCESS != (ret = session_ctx.get_ups_mutator().get_mutator().add_cell(*mutator_cell_info)))
              {
                YYSYS_LOG(WARN, "add cell info to mutator fail, ret=%d", ret);
              }
              else if(is_row_finished &&
                      OB_SUCCESS != (ret = session_ctx.get_ups_mutator().get_mutator().add_row_barrier()))
              {
                YYSYS_LOG(WARN, "add rowkey barrier to mutator fail, ret=%d", ret);
              }
            }
          }
        }
        ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
        mutator.reset_iter();
        FILL_TRACE_BUF(session_ctx.get_tlog_buffer(), "total_row_num=%ld new_row_num=%ld cell_num=%ld ret=%d",
                       total_row_counter, new_row_counter, cell_counter, ret);
        if(is_truncate_flag && !session_ctx.get_ups_mutator().is_truncate_mutator())
        {
            ret = session_ctx.get_ups_mutator().set_trun_flag();
        }
        if(OB_SUCCESS == ret)
        {
          session_ctx.get_uc_info().uc_row_counter += new_row_counter;
          session_ctx.get_uc_info().uc_checksum = bc.calc();
          session_ctx.commit_stmt();
        }
        else
        {
          session_ctx.rollback_stmt();
        }
      }
      return ret;
    }

    void MemTable::handle_checksum_error(ObUpsMutator &mutator)
    {
      YYSYS_LOG(ERROR, "checksum wrong table_checksum_before=%ld mutator_checksum_before=%ld "
        "table_checksum_after=%ld mutator_checksum_after=%ld", checksum_before_mutate_,
                mutator.get_memtable_checksum_before_mutate(), checksum_after_mutate_,
                mutator.get_memtable_checksum_after_mutate());
      ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
      if(NULL != ups_main)
      {
        ups_main->get_update_server().get_role_mgr().set_state(ObUpsRoleMgr::STOP);
      }
      ObMutatorCellInfo *mutator_cell_info = NULL;
      while(OB_SUCCESS == mutator.next_cell())
      {
        mutator.get_cell(&mutator_cell_info);
        if(NULL != mutator_cell_info)
        {
          YYSYS_LOG(INFO, "%s %s", print_obj(mutator_cell_info->op_type),
                    print_cellinfo(&(mutator_cell_info->cell_info)));
        }
      }
    }

    //[244 bugfix]
    int MemTable::query_cur_row_without_lock(const TEKey &key, TEValue *&value)
    {
        int ret = OB_SUCCESS;
        value = NULL;
        if(OB_SUCCESS == ret && NULL == value)
        {
            if(using_memtable_bloomfilter())
            {
                if(table_bf_.may_contain(key.table_id, key.row_key))
                {
                    value = table_engine_.get(key);
                }
            }
            else
            {
                value = table_engine_.get(key);
            }
        }
        return ret;
    }

    int MemTable::ensure_cur_row(const TEKey &key, TEValue *&value)
    {
      int ret = OB_SUCCESS;
      TEValue *new_value = NULL;
      value = NULL;
      while(OB_SUCCESS == ret && NULL == value)
      {
        if(using_memtable_bloomfilter())
        {
          if(table_bf_.may_contain(key.table_id, key.row_key))
          {
            value = table_engine_.get(key);
          }
        }
        else
        {
          value = table_engine_.get(key);
        }
        if(NULL != value)
        {
          break;
        }
        if(NULL == (new_value = (TEValue *) mem_tank_.tevalue_alloc(sizeof(TEValue))))
        {
          ret = OB_MEM_OVERFLOW;
        }
        else
        {
          new_value->reset();
          TEKey tmp_row_key = key;
          if(OB_SUCCESS != (ret = mem_tank_.write_string(key.row_key, &(tmp_row_key.row_key))))
          {
            YYSYS_LOG(WARN, "copy rowkey fail, ret=%d %s", ret, key.log_str());
          }
            //mod zhaoqiong [Truncate Table]:20160318:b
            //else if (OB_SUCCESS != (ret = table_bf_.insert(tmp_row_key.table_id, tmp_row_key.row_key)))
          else if(tmp_row_key.row_key.length() > 0 &&
                  OB_SUCCESS != (ret = table_bf_.insert(tmp_row_key.table_id, tmp_row_key.row_key)))
            //mod:e
          {
            YYSYS_LOG(WARN, "insert cur_key to bloomfilter fail ret=%d %s", ret, key.log_str());
          }
          else if(OB_SUCCESS != (ret = table_engine_.set(tmp_row_key, new_value)) && OB_ENTRY_EXIST != ret)
          {
            YYSYS_LOG(WARN, "put to table_engine fail ret=%d %s", ret, key.log_str());
          }
          else if(OB_ENTRY_EXIST == ret)
          {
            ret = OB_SUCCESS;
            YYSYS_LOG(INFO, "put to table_engine entry exist %s", key.log_str());
          }
          else
          {
            value = new_value;
          }
        }
      }
      return ret;
    }

    int MemTable::rdlock_row(ILockInfo *lock_info, const TEKey &key, TEValue *&value, const sql::ObLockFlag lock_flag)
    {
      int ret = OB_SUCCESS;
      if(NULL == lock_info)
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(ERROR, "lock_info == NULL");
      }
          else if((0 == (lock_flag & sql::LF_WRITE)) && (0 == (lock_flag & sql::UNLF_WRITE)) &&
              (NO_LOCK == lock_info->get_isolation_level() || READ_COMMITED == lock_info->get_isolation_level()))
      {
        value = NULL;
      }
      else if(OB_SUCCESS != (ret = ensure_cur_row(key, value)))
      {
        YYSYS_LOG(WARN, "ensure cur row fail, ret=%d, %s", ret, key.log_str());
      }
      else if(NULL == value)
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(ERROR, "ensure_cur_row() return NULL value");
      }
      else if(0 != (lock_flag & sql::LF_WRITE))
      {
        if(OB_SUCCESS != (ret = lock_info->on_write_begin(key, *value)))
        {
          //YYSYS_LOG(WARN, "lock info on write begin fail, ret=%d %s", ret, key.log_str());
          ret = OB_ERR_SHARED_LOCK_CONFLICT;
        }
          //add dyr [NPU-2015009-cursor] [NPU-OB-009] 20150319:b
          //�ɹ���û�����֮����ʾ���õ�ǰ�п��Ա�ǿ�ƽ���
          //��������������ύ������ٴε���on_write_begin,��ʱ������ʾ���ÿ�ǿ�ƽ�����
          //����ʹ��Ĭ�ϵĲ��ɽ���
        else if(OB_SUCCESS != (ret = value->set_able_force_unlock()))
        {
          YYSYS_LOG(ERROR, "try set lock flag can be force unlock error!");
        }
        //add dyr 20150319:e
      }
        //add dyr [NPU-2015009-cursor] [NPU-OB-009] 20150319:b
      else if(0 != (lock_flag & sql::UNLF_WRITE))
      {
        if(OB_SUCCESS != (ret = lock_info->force_unlock_row(key, *value)))
        {
          YYSYS_LOG(WARN, "force unlock row fail, ret=%d %s", ret, key.log_str());
        }
      }
        //add dyr 20150319:e
      else
      {
        if(OB_SUCCESS != (ret = lock_info->on_read_begin(key, *value)))
        {
          YYSYS_LOG(WARN, "lock info on read begin fail, ret=%d %s", ret, key.log_str());
        }
      }
      return ret;
    }

    int MemTable::get(const BaseSessionCtx &session_ctx, const uint64_t table_id, const ObRowkey &row_key,
                      MemTableIterator &iterator, ColumnFilter *column_filter/* = NULL*/,
                      const sql::ObLockFlag lock_flag /*=0*/
      //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151207:b
      , const common::ObReadAtomicDataMark *data_mark/* = NULL*/
      //add duyr 20151207:e
    )
    {
      int ret = OB_SUCCESS;
      TEKey key(table_id, row_key);
      TEValue *value = NULL;
      TEValue *cur_table_value = NULL;  //add zhaoqiong [Truncate Table]:20160318
      ILockInfo *lock_info = NULL;
      if(!inited_)
      {
        YYSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }

      //add zhaoqiong [Truncate Table]:20160318:b
      if(ret == OB_SUCCESS)
      {
        /*check */
        /* check the table_value info*/
        TEKey cur_table_key;
        cur_table_key.table_id = table_id;
        cur_table_value = table_engine_.get(cur_table_key);
        int64_t last_truncate_time = 0;
        if(cur_table_value != NULL && cur_table_value->cell_info_cnt != 0)
        {
          //ret = OB_TABLE_UPDATE_LOCKED;
          //YYSYS_LOG(ERROR, "table_id %ld has been locked, wait for mem_freeze", key.table_id);
            last_truncate_time = cur_table_value->list_tail->modify_time;
            YYSYS_LOG(DEBUG,"table_id %ld has been recently truncated at %ld", key.table_id, cur_table_value->list_tail->modify_time);
        }
        (void)last_truncate_time;
      }

      if(ret == OB_SUCCESS)
      {
        //add:e
        if(ST_READ_WRITE == session_ctx.get_type())
        {
          if(NULL == (lock_info = ((RWSessionCtx &) session_ctx).get_lock_info()))
          {
            ret = OB_ERR_UNEXPECTED;
            YYSYS_LOG(ERROR, "session_ctx is RW_SESSION but lock_info==NULL");
          }
          else if(OB_SUCCESS != (ret = rdlock_row(lock_info, key, value, lock_flag)))
          {
            if(!IS_SQL_ERR(ret))
            {
              YYSYS_LOG(WARN, "rdlock_row()=>%d", ret);
            }
          }
        }
        if(OB_SUCCESS == ret && NULL == value)
        {
          if(using_memtable_bloomfilter() && !table_bf_.may_contain(table_id, row_key))
          {
          } // ����value=NULL, iterator����ʱ���Դ���
          else
          {
            value = table_engine_.get(key);
          }
        }
      }
      if(OB_SUCCESS == ret)
      {
        iterator.get_get_iter_().set_(key, value, column_filter, true, &session_ctx
          //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151207:b
          , data_mark
          //add duyr 20151207:e
          , NULL, cur_table_value
        );
      }
      //add zhaoqiong [Truncate Table]:20160318
      else if(OB_TABLE_UPDATE_LOCKED == ret)
      {
        iterator.get_get_iter_().set_(key, value, column_filter, true, &session_ctx
          //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151207:b
          , data_mark
          //add duyr 20151207:e
          , NULL, cur_table_value
        );
        ret = OB_SUCCESS;
      }
      //add:e
      return ret;
    }

    int MemTable::get(const MemTableTransDescriptor td, const uint64_t table_id, const ObRowkey &row_key,
                      MemTableIterator &iterator, ColumnFilter *column_filter/* = NULL*/)
    {
      int ret = OB_SUCCESS;
      TransNode *tn = NULL;
      TEKey key;
      key.table_id = table_id;
      key.row_key = row_key;
      /*
      key.rowkey_prefix = 0;
      memcpy(&(key.rowkey_prefix), key.row_key.ptr(),
            std::min(key.row_key.length(), (int32_t)sizeof(key.rowkey_prefix)));
            */
      TEValue *value = NULL;
      if(!inited_)
      {
        YYSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }
      else if(NULL == (tn = trans_mgr_.get_trans_node(td)))
      {
        YYSYS_LOG(WARN, "get trans node fail td=%lu", td);
        ret = OB_ERROR;
      }
      else if(using_memtable_bloomfilter() && !table_bf_.may_contain(table_id, row_key))
      {
        iterator.get_get_iter_().set_(key, NULL, column_filter, tn);
      }
      else
      {
        value = table_engine_.get(key);
        iterator.get_get_iter_().set_(key, value, column_filter, tn);
      }
      return ret;
    }

    int MemTable::scan(const BaseSessionCtx &session_ctx, const ObNewRange &range, const bool reverse,
                       MemTableIterator &iter, ColumnFilter *column_filter/* = NULL*/
      //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151207:b
      , const ObReadAtomicDataMark *data_mark/* = NULL*/
      //add duyr 20151207:e
      ,int64_t query_version/* =0*/
    )
    {
      int ret = OB_SUCCESS;
      int err = OB_SUCCESS; //add zhaoqiong [Truncate Table]:20160318
      TEKey start_key;
      TEKey end_key;
      int start_exclude = get_start_exclude(range);
      int end_exclude = get_end_exclude(range);
      int min_key = get_min_key(range);
      int max_key = get_max_key(range);

      start_key.table_id = get_table_id(range);
      start_key.row_key = get_start_key(range);
      end_key.table_id = get_table_id(range);
      end_key.row_key = get_end_key(range);

      if(0 != min_key)
      {
        start_exclude = 0;
        min_key = 0;
        start_key.row_key.set_min_row();
      }

      if(0 != max_key)
      {
        end_exclude = 0;
        max_key = 0;
        end_key.row_key.set_max_row();
      }

      if(!inited_)
      {
        YYSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }

      //add zhaoqiong [Truncate Table]:20160318
      /*check */
      /* check the table_value info*/
      TEKey cur_table_key;
      TEValue *cur_table_value;
      cur_table_key.table_id = start_key.table_id;
      cur_table_value = table_engine_.get(cur_table_key);
      if(cur_table_value != NULL && cur_table_value->cell_info_cnt != 0)
      {
        err = OB_TABLE_UPDATE_LOCKED;
        YYSYS_LOG(DEBUG, "table_id %ld has been locked, wait for mem_freeze", start_key.table_id);
      }
      //add:e

      if(OB_SUCCESS != table_engine_.scan(start_key, min_key, start_exclude, end_key, max_key, end_exclude, reverse,
                                          iter.get_scan_iter_().get_te_iter_()))
      {
        ret = OB_ERROR;
      }
      else if(err == OB_SUCCESS)
      {
        iter.get_scan_iter_().set_(get_table_id(range), column_filter, true, &session_ctx, NULL,
          //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151207:b
                                   data_mark
          //add duyr 20151207:e
                                   ,query_version
        );
      }
      else if(err == OB_TABLE_UPDATE_LOCKED)
      {
        iter.get_scan_iter_().set_(get_table_id(range), column_filter, true, &session_ctx, cur_table_value,
          //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151207:b
                                   data_mark
          //add duyr 20151207:e
                                   ,query_version
        );
      }
      return ret;
    }

    int MemTable::scan(const MemTableTransDescriptor td, const ObNewRange &range, const bool reverse,
                       MemTableIterator &iter, ColumnFilter *column_filter/* = NULL*/)
    {
      int ret = OB_SUCCESS;

      TransNode *tn = NULL;
      TEKey start_key;
      TEKey end_key;
      int start_exclude = get_start_exclude(range);
      int end_exclude = get_end_exclude(range);
      int min_key = get_min_key(range);
      int max_key = get_max_key(range);

      start_key.table_id = get_table_id(range);
      start_key.row_key = get_start_key(range);;
      end_key.table_id = get_table_id(range);
      end_key.row_key = get_end_key(range);

      if(0 != min_key)
      {
        start_exclude = 0;
        min_key = 0;
        start_key.row_key.assign(&MIN_OBJ, 1);
      }

      if(0 != max_key)
      {
        end_exclude = 0;
        max_key = 0;
        end_key.row_key.assign(&MAX_OBJ, 1);
      }

      if(!inited_)
      {
        YYSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }
      else if(NULL == (tn = trans_mgr_.get_trans_node(td)))
      {
        YYSYS_LOG(WARN, "get trans node fail td=%lu", td);
        ret = OB_ERROR;
      }
      else if(OB_SUCCESS !=
              table_engine_.scan(start_key, min_key, start_exclude, end_key, max_key, end_exclude, reverse,
                                 iter.get_scan_iter_().get_te_iter_()))
      {
        ret = OB_ERROR;
      }
      else
      {
        iter.get_scan_iter_().set_(get_table_id(range), column_filter, tn);
      }
      return ret;
    }

    int MemTable::start_transaction(const TETransType trans_type, MemTableTransDescriptor &td, const int64_t trans_id)
    {
      int ret = OB_SUCCESS;
      if(!inited_)
      {
        YYSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }
      else if(OB_SUCCESS != (ret = trans_mgr_.start_transaction(trans_type, td, trans_id)))
      {
        YYSYS_LOG(WARN, "trans mgr start transaction fail ret=%d", ret);
      }
      else
      {
        // do nothing
      }
      return ret;
    }

    int MemTable::end_transaction(const MemTableTransDescriptor td, bool rollback)
    {
      int ret = OB_SUCCESS;
      if(!inited_)
      {
        YYSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }
      else if(OB_SUCCESS != (ret = trans_mgr_.end_transaction(td, rollback)))
      {
        YYSYS_LOG(WARN, "trans mgr end transaction fail ret=%d", ret);
      }
      else
      {
        // do nothing
      }
      return ret;
    }

    int MemTable::start_mutation(const MemTableTransDescriptor td)
    {
      int ret = OB_SUCCESS;
      TransNode *tn = NULL;
      if(!inited_)
      {
        YYSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }
      else if(NULL == (tn = trans_mgr_.get_trans_node(td)))
      {
        YYSYS_LOG(WARN, "get trans node fail td=%lu", td);
        ret = OB_ERROR;
      }
      else if(OB_SUCCESS != (ret = tn->start_mutation()))
      {
        YYSYS_LOG(WARN, "start mutation fail ret=%d", ret);
      }
      else
      {
        checksum_after_mutate_ = checksum_before_mutate_;
      }
      return ret;
    }

    int MemTable::end_mutation(const MemTableTransDescriptor td, bool rollback)
    {
      int ret = OB_SUCCESS;
      TransNode *tn = NULL;
      if(!inited_)
      {
        YYSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }
      else if(NULL == (tn = trans_mgr_.get_trans_node(td)))
      {
        YYSYS_LOG(WARN, "get trans node fail td=%lu", td);
        ret = OB_ERROR;
      }
      else if(OB_SUCCESS != (ret = tn->end_mutation(rollback)))
      {
        YYSYS_LOG(WARN, "end mutation fail ret=%d rollback=%d", ret, rollback);
      }
      else
      {
        if(!rollback)
        {
          checksum_before_mutate_ = checksum_after_mutate_;
        }
        else
        {
          checksum_after_mutate_ = checksum_before_mutate_;
        }
      }
      return ret;
    }

    //add zhaoqiong [Truncate Table]:20160318:b
    int MemTable::get_table_truncate_stat(uint64_t table_id, bool &is_truncated)
    {
      int ret = OB_SUCCESS;
      is_truncated = false;
      if(row_counter_ != 0)
      {
        ret = table_engine_.get_table_truncate_stat(table_id, is_truncated);
      }
      //else memtable is empty
      return ret;
    }

    //add:e
    int MemTable::get_bloomfilter(TableBloomFilter &table_bf) const
    {
      return table_bf.deep_copy(table_bf_);
    }

    int MemTable::scan_all(TableEngineIterator &iter)
    {
      int ret = OB_SUCCESS;
      TEKey empty_key;
      int min_key = 1;
      int max_key = 1;
      int start_exclude = 0;
      int end_exclude = 0;
      bool reverse = false;
      if(!inited_)
      {
        YYSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }
      else if(OB_SUCCESS !=
              table_engine_.scan(empty_key, min_key, start_exclude, empty_key, max_key, end_exclude, reverse, iter))
      {
        YYSYS_LOG(WARN, "table engine scan fail ret=%d", ret);
      }
      else
      {
        YYSYS_LOG(INFO, "scan all start succ");
      }
      return ret;
    }


    //add zhaoqiong [Truncate Table]:20160318
    int MemTable::scan_all_table(TableEngineIterator &iter)
    {
      int ret = OB_SUCCESS;
      TEKey empty_key;
      int min_key = 1;
      int max_key = 1;
      int start_exclude = 0;
      int end_exclude = 0;
      bool reverse = false;
      if(!inited_)
      {
        YYSYS_LOG(WARN, "have not inited");
        ret = OB_ERROR;
      }
      else if(OB_SUCCESS !=
              table_engine_.scan_table(empty_key, min_key, start_exclude, empty_key, max_key, end_exclude, reverse,
                                       iter))
      {
        YYSYS_LOG(WARN, "table engine scan fail ret=%d", ret);
      }
      else
      {
        YYSYS_LOG(INFO, "scan all table start succ");
      }
      return ret;
    }
    //add:e

    //[306 bugfix]
    int MemTable::scan_one_table(TableEngineIterator &iter, int64_t table_id)
    {
        int ret = OB_SUCCESS;
        TEKey key;
        key.table_id = table_id;

        int min_key = 0;
        int max_key = 0;
        int start_exclude = 0;
        int end_exclude = 0;
        bool reverse = false;

        if(!inited_)
        {
            YYSYS_LOG(WARN,"have not inited");
            ret = OB_ERROR;
        }
        else if(OB_SUCCESS != (ret = table_engine_.scan_table(key, min_key, start_exclude,
                                                              key, max_key, end_exclude,
                                                              reverse, iter)))
        {
            YYSYS_LOG(WARN,"table engine scan fail ret=%d", ret);
        }
        else
        {

        }
        return ret;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    MemTableGetIter::MemTableGetIter() : te_key_(), te_value_(NULL), column_filter_(NULL),
      //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
                                         data_mark_param_(),
      //add duyr 20160531:e
                                         return_rowkey_column_(true), session_ctx_(NULL), is_iter_end_(true),
                                         node_iter_(NULL), cell_iter_(), iter_counter_(0), ci_(), nop_v_node_(),
                                         rne_v_node_(), mctime_v_node_()
      //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151207:b
      , node_list_array_(common::OB_COMMON_MEM_BLOCK_SIZE, ModulePageAllocator(ObModIds::OB_READ_ATOMIC_ITER)),
                                         output_commit_list_transverset_(common::OB_COMMON_MEM_BLOCK_SIZE,
                                                                         ModulePageAllocator(
                                                                           ObModIds::OB_READ_ATOMIC_ITER)),
                                         output_undo_list_transverset_(common::OB_COMMON_MEM_BLOCK_SIZE,
                                                                       ModulePageAllocator(
                                                                         ObModIds::OB_READ_ATOMIC_ITER))
    //add duyr 20151207:e
    {
      //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151207:b
      reset_read_atomic();
      //add duyr 20151207:e
      ObUpsCompactCellWriter ccw;

      ccw.init(nop_v_node_.buf, sizeof(nop_v_buf_));
      ccw.row_nop();
      ccw.row_finish();
      nop_v_node_.next = NULL;

      ccw.init(rne_v_node_.buf, sizeof(rne_v_buf_));
      ccw.row_not_exist();
      ccw.row_finish();
      rne_v_node_.next = NULL;

      mctime_v_node_.next = NULL;
    }

    MemTableGetIter::~MemTableGetIter()
    {
    }

    void MemTableGetIter::reset()
    {
      te_value_ = NULL;
      column_filter_ = NULL;
      //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
      data_mark_param_.reset();
      //add duyr 20160531:e
      //return_rowkey_column_ = true;
      //trans_node_ = NULL;
      session_ctx_ = NULL;
      is_iter_end_ = true;
      node_iter_ = NULL;
      cell_iter_.reset();
      cell_iter_.reset_all();
      iter_counter_ = 0;
      //ci_.reset();
      //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151207:b
      reset_read_atomic();
      //add duyr 20151207:e
      nop_v_node_.next = NULL;
      rne_v_node_.next = NULL;
      mctime_v_node_.next = NULL;
    }

    //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151207:b
    void MemTableGetIter::reset_read_atomic()
    {
      cur_iter_read_state_ = READ_NONE;
      read_atomic_param_.reset();
      read_atomic_param_is_valid_ = false;
      data_mark_.reset();
      atomic_read_strategy_ = ObReadAtomicParam::INVALID_STRATEGY;

      next_head_node_idx_ = 0;
      is_node_iter_end_ = true;
      is_extend_iter_end_ = true;
      cur_cellinfo_is_rne_ = false;
      is_row_empty_ = false;
      need_return_rowkey_indep_ = false;
      rowkey_cell_idx_ = 0;
      cur_commit_list_transver_idx_ = 0;
      cur_undo_list_transver_idx_ = 0;
      output_last_commit_trans_ver_ = NULL;
      output_last_prepare_trans_ver_ = NULL;
      output_commit_ext_type_ = OB_INVALID_DATA;
      output_prepare_ext_type_ = OB_INVALID_DATA;
      output_paxos_id_ = OB_INVALID_PAXOS_ID;
      output_major_ver_ = OB_INVALID_VERSION;
      output_minor_ver_start_ = OB_INVALID_VERSION;
      output_minor_ver_end_ = OB_INVALID_VERSION;
      output_data_store_type_ = ObReadAtomicDataMark::INVALID_STORE_TYPE;
      node_list_array_.clear();
      output_commit_list_transverset_.clear();
      output_undo_list_transverset_.clear();
      memset(prevcommit_trans_verset_cids_, 0, sizeof(prevcommit_trans_verset_cids_));
    }
    //add duyr 20151207:e



    //add shili [MultiUPS] [READ_ATOMIC] [read_part] 20160607:b
    /*@berif ���õ�һ�����������ĵ�һ������node,����������״̬*/
    int  MemTableGetIter::get_first_iter_node(const ObCellInfoNode *&node_iter)
    {
      int ret = OB_SUCCESS;
      NodeList *cur_node_list = NULL;
      common::ObTransVersion *trans_ver = NULL;
      node_iter = get_read_atomic_next_list_head_();
      if(NULL == node_iter)
      {
        YYSYS_LOG(DEBUG, "====================first node is null========================");
      }
      else
      {
        cur_node_list = get_read_atomic_cur_node_list();
        if(NULL == cur_node_list)
        {
          ret = OB_ERR_UNEXPECTED;
          YYSYS_LOG(WARN, "cur_node_list  is empty,ret=%d", ret);
        }
        else if(NULL == cur_node_list->list_tail_trans_ver_)
        {
          YYSYS_LOG(DEBUG, "NULL == cur_node_list->list_tail_trans_ver_");
        }
        else
        {
          if(OB_SUCCESS != (ret = common::ObReadAtomicHelper::transver_cast_from_buf(node_iter->trans_version_,
                                                                                     static_cast<int64_t>(sizeof(node_iter->trans_version_)),
                                                                                     trans_ver)))
          {
            YYSYS_LOG(WARN, "fail to get cur node iter's transver!ret=%d", ret);
          }
          else if(NULL == trans_ver)
          {
            ret = OB_ERR_UNEXPECTED;
            YYSYS_LOG(WARN, "cur node iter trans ver is NULL!ret=%d", ret);
          }
        }

        if(OB_SUCCESS == ret)
        {
          if(cur_node_list == NULL)
          {
            ret = OB_ERR_UNEXPECTED;
          }
          else if(cur_node_list->list_tail_trans_ver_ == NULL)
          {
          }
            /*���һ��ʼ�ڵ�trans_version �� ���� tail_trans_version ��ͬ*/
          else if((*cur_node_list->list_tail_trans_ver_) == (*trans_ver))
          {
            cur_node_list->list_iter_state_ = ITER_TAIL_NODE;
          }
          else  /*�����ͬ˵��״̬ Ϊnormal, tailnode ���ǵ�һ���ڵ�*/
          {
            cur_node_list->list_iter_state_ = ITER_NORMAL_NODE;
          }
          if(cur_node_list != NULL)
          {
            YYSYS_LOG(DEBUG, "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
            YYSYS_LOG(DEBUG,
                      "first _node_trans_ver:%s,   first_node_list_trans_ver:%s,cur_node_list->list_iter_state_:%d",
                      trans_ver == NULL ? "null" : to_cstring(*trans_ver),
                      cur_node_list->list_tail_trans_ver_ == NULL ? "nil" : to_cstring(
                        *(cur_node_list->list_tail_trans_ver_)), cur_node_list->list_iter_state_);
            YYSYS_LOG(DEBUG, "\n+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");

          }
        }
      }
      return ret;
    }
    //add e



    void MemTableGetIter::set_(const TEKey &te_key, const TEValue *te_value, const ColumnFilter *column_filter,
                               const bool return_rowkey_column, const BaseSessionCtx *session_ctx
      //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151207:b
      , const ObReadAtomicDataMark *data_mark/* = NULL*/, const CommonSchemaManager *sm, const TEValue *table_value
      //add duyr 20151207:e
                               ,int64_t query_version
    )
    {
        YYSYS_LOG(DEBUG,"TEKEY = %s,te value= %p",to_cstring(te_key),te_value);
      te_key_ = te_key;
      te_value_ = te_value;
      table_value_ = table_value;//[306 bugfix]
      column_filter_ = column_filter;
      return_rowkey_column_ = return_rowkey_column;
      session_ctx_ = session_ctx;
      is_iter_end_ = false;
      node_iter_ = NULL;
      cell_iter_.reset();
      cell_iter_.reset_all();
      iter_counter_ = 0;
      ci_.table_id_ = te_key_.table_id;
      ci_.row_key_ = te_key_.row_key;
      ci_.column_id_ = OB_INVALID_ID;
      ci_.value_.reset();

      //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151207:b

      reset_read_atomic();
      is_node_iter_end_ = false;
      is_row_empty_ = (NULL == te_value_ || te_value_->is_empty(false));
      if(NULL != column_filter_ && column_filter_->get_read_atomic_param().is_valid())
      {
        YYSYS_LOG(DEBUG, "\n\n\n======================start ==============================");
        YYSYS_LOG(DEBUG, "=====================start ==============================");
        YYSYS_LOG(DEBUG, "======================start ==============================\n\n\n");
        YYSYS_LOG(DEBUG, "=======================SESSION:%s======================",
                  session_ctx == NULL ? "NULL" : to_cstring(*session_ctx));
        is_row_empty_ = (NULL == te_value_ || te_value_->is_empty(true));
        if(!is_row_empty_)
        {
          read_atomic_param_ = column_filter_->get_read_atomic_param();
          read_atomic_param_is_valid_ = read_atomic_param_.is_valid();
          if(NULL != data_mark)
          {
            data_mark_ = *data_mark;
          }
          is_extend_iter_end_ = false;
          //it's important!!!!!
          int tmp_ret = OB_SUCCESS;
          if(!read_atomic_param_is_valid_)
          {//FIXME:READ_ATOMIC
            //should't be here!!!!!means the operator= of the read atomic param
            //is wrong!!!!!
            tmp_ret = OB_ERR_UNEXPECTED;
            YYSYS_LOG(ERROR, "read atomic param can't be invalid!ret=%d", tmp_ret);
          }
          else if(OB_SUCCESS != (tmp_ret = prepare_read_atomic_output_data()))
          {
            YYSYS_LOG(WARN, "fail to prepare read atomic output data!tmp_ret=%d,row_key=[%s],table_id=%ld", tmp_ret,
                      to_cstring(ci_.row_key_), ci_.table_id_);
          }
        }
        //tmp::test
        YYSYS_LOG(DEBUG, "read_atomic::debug,tid=%ld,row_key=[%s],"
          "final read atomic param=[%s],"
          "input_data_mark=%p,data_mark=[%s],"
          "te_val=%p,is_row_empty_=%d", ci_.table_id_, to_cstring(ci_.row_key_), to_cstring(read_atomic_param_),
                  data_mark, to_cstring(data_mark_), te_value_, is_row_empty_);
      }
      //add duyr 20151207:e

      //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
      data_mark_param_.reset();
      if(NULL != column_filter_ && column_filter_->get_data_mark_param().is_valid())
      {
        data_mark_param_ = column_filter_->get_data_mark_param();
        YYSYS_LOG(DEBUG, "mul_del::debug,final mem iter data mark param=[%s]!", to_cstring(data_mark_param_));
      }
      //add duyr 20160531:e

      //mod duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151208:b
      //      ObCellInfoNode *list_head = get_list_head_();
      const ObCellInfoNode *list_head = NULL;
      if(read_atomic_param_is_valid_)
      {
        int ret = OB_SUCCESS;
        if(OB_SUCCESS != (ret = get_first_iter_node(list_head)))
        {
          YYSYS_LOG(WARN, "get first iter node error,ret=%d", ret);
        }
      }
      else
      {
        list_head = get_list_head_();
      }
      //mod duyr 20151208:e

      if(NULL == list_head)
      {
        //cell_iter_.set_rne();
          if(table_value_ == NULL || table_value_->is_empty())
          {
              cell_iter_.set_rne();
          }
          else
          {
              cell_iter_.set_rdel();
          }
        //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151208:b
        cur_cellinfo_is_rne_ = true;
        //add duyr 20151208:e
      }
      else
      {
        if(!read_uncommited_data_() && NULL != session_ctx && list_head->modify_time > session_ctx->get_trans_id())
        {
          //cell_iter_.set_rne();
            if(table_value_ == NULL || table_value_->is_empty())
            {
                cell_iter_.set_rne();
            }
            else
            {
                //[526]
                if(list_head->modify_time < table_value_->list_tail->modify_time)
                {
                    cell_iter_.set_rdel();
                }
                else
                {
                    cell_iter_.set_rne();
                }

            }
          //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151208:b
          cur_cellinfo_is_rne_ = true;
          //add duyr 20151208:e
        }
        else
        {
          node_iter_ = list_head;

          //tmp::debug
          if(ci_.table_id_ > 3000)
          {
            YYSYS_LOG(DEBUG, "read_atomic::debug,the first node_iter_ptr=%p", node_iter_);
          }

          if(NULL != column_filter_ && !column_filter_->is_all_column())
          {
            cell_iter_.set_nop();
          }

          if(return_rowkey_column_)
          {
            cell_iter_.set_rowkey_column(te_key_.table_id, te_key_.row_key, sm, query_version);
          }

          uint64_t ctime_column_id = OB_UPS_CREATE_TIME_COLUMN_ID(te_key_.table_id);
          cell_iter_.set_ctime_column_id(ctime_column_id);


          //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
          if(data_mark_param_.is_valid())
          {
            if(data_mark_param_.need_modify_time_)
            {
              cell_iter_.set_data_mark_mtime(data_mark_param_.modify_time_cid_, node_iter_->modify_time);
              YYSYS_LOG(DEBUG, "mul_del::debug,orig_modify_time=%ld", node_iter_->modify_time);
            }
            if(data_mark_param_.need_major_version_)
            {
              cell_iter_.set_data_mark_major_ver(data_mark_param_.major_version_cid_, OB_INVALID_DATA);
            }
            if(data_mark_param_.need_minor_version_)
            {
              cell_iter_.set_data_mark_minor_ver_start(data_mark_param_.minor_ver_start_cid_, OB_INVALID_DATA);
            }
            if(data_mark_param_.need_minor_version_)
            {
              cell_iter_.set_data_mark_minor_ver_end(data_mark_param_.minor_ver_end_cid_, OB_INVALID_DATA);
            }
            if(data_mark_param_.need_data_store_type_)
            {
              cell_iter_.set_data_mark_data_store_type(data_mark_param_.data_store_type_cid_, OB_INVALID_DATA);
            }
          }
          //add duyr 20160531:e
          uint64_t mtime_column_id = OB_UPS_MODIFY_TIME_COLUMN_ID(te_key_.table_id);
          cell_iter_.set_mtime(mtime_column_id, node_iter_->modify_time);

          if(table_value_ != NULL && !table_value_->is_empty())
          {
              if(list_head->modify_time < table_value_->list_tail->modify_time)
              {
                  cell_iter_.set_rdel();
                  YYSYS_LOG(DEBUG,"memtablegetiter set_rdel()");
                  YYSYS_LOG(DEBUG,
                            "list_head->modify_time=%ld,list_tail->modify_time=%ld, table_value_->list_tail->modify_time=%ld",
                            list_head->modify_time,
                            te_value->list_tail->modify_time,
                            table_value_->list_tail->modify_time);
              }
              else
              {
                  YYSYS_LOG(DEBUG,
                            "list_head->modify_time=%ld,list_tail->modify_time=%ld, table_value_->list_tail->modify_time=%ld",
                            list_head->modify_time,
                            te_value->list_tail->modify_time,
                            table_value_->list_tail->modify_time);
              }
          }

          cell_iter_.set_cell_info_node(node_iter_);
        }
      }
      cell_iter_.set_head();
    }

    void MemTableGetIter::set_(const TEKey &te_key,
                                   const TEValue *te_value,
                                                           const ColumnFilter *column_filter,
                               const ITransNode *trans_node)
    {
      te_key_ = te_key;
      te_value_ = te_value;
      column_filter_ = column_filter;
      UNUSED(trans_node);//trans_node_ = trans_node;
      is_iter_end_ = false;
      node_iter_ = NULL;
      cell_iter_.reset();
      cell_iter_.reset_all();
      iter_counter_ = 0;
      ci_.table_id_ = te_key_.table_id;
      ci_.row_key_ = te_key_.row_key;
      ci_.column_id_ = OB_INVALID_ID;
      ci_.value_.reset();

      if(NULL == te_value_ || NULL == te_value_->list_head)
      {
        cell_iter_.set_rne();
      }
      else
      {
        ObObj mtime_obj;
        mtime_obj.set_modifytime(te_value_->list_head->modify_time);
        if(trans_end_(mtime_obj))
        {
          cell_iter_.set_rne();
        }
        else
        {
          node_iter_ = te_value_->list_head;

          if(NULL != column_filter_ && !column_filter_->is_all_column())
          {
            cell_iter_.set_nop();
          }

          cell_iter_.set_rowkey_column(te_key_.table_id, te_key_.row_key);

          uint64_t ctime_column_id = OB_UPS_CREATE_TIME_COLUMN_ID(te_key_.table_id);
          cell_iter_.set_ctime_column_id(ctime_column_id);

          uint64_t mtime_column_id = OB_UPS_MODIFY_TIME_COLUMN_ID(te_key_.table_id);
          cell_iter_.set_mtime(mtime_column_id, node_iter_->modify_time);

          cell_iter_.set_cell_info_node(node_iter_);

          __builtin_prefetch(node_iter_);
        }
      }
      cell_iter_.set_head();
    }

    const ObCellInfoNode *MemTableGetIter::get_cur_node_iter_() const
    {
      return node_iter_;
    }

    bool MemTableGetIter::trans_end_(const ObObj &value)
    {
      bool bret = false;
      if(ObModifyTimeType == value.get_type() && !read_uncommited_data_())
      {
        int64_t v = 0;
        value.get_modifytime(v);
        if(NULL != session_ctx_ && v > session_ctx_->get_trans_id())
        {
          YYSYS_LOG(DEBUG, "trans_end v=%ld trans_id=%ld", v, session_ctx_->get_trans_id());
          bret = true;
        }
      }
      return bret;
    }

    //mod duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151209:b
    //    int MemTableGetIter::next_cell()
    /*���ڵ��� ��ʵ������*/
    int MemTableGetIter::node_iter_next_cell_()
    //mod duyr 20151209:e
    {
      int ret = OB_SUCCESS;
      //mod duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151209:b
      //      if (is_iter_end_)
      if(is_node_iter_end_)
        //mod duyr 20151209:e
      {
        ret = OB_ITER_END;
      }

      while(OB_SUCCESS == ret)
      {
        // һ��node�¿��ܻ��ж��obj �ȵ���node
        ret = cell_iter_.next_cell();
        if(OB_ITER_END == ret)
        {
          if(ci_.table_id_ > 3000)
          {
            YYSYS_LOG(DEBUG, "***********One  cellinfo  node is end ,need NEW  one**************");
          }

          //mod duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151209:b
          //          if (NULL == node_iter_
          //              || NULL == (node_iter_ = node_iter_->next))
          if(read_atomic_param_is_valid_ && OB_SUCCESS != (ret = get_read_atomic_next_node_iter_()))
          {
            if(OB_ITER_END == ret)
            {
              YYSYS_LOG(DEBUG, "**************** all  data lists  is over  *************");
            }
            break;
          }
          else if(!read_atomic_param_is_valid_ && (NULL == node_iter_ || NULL == (node_iter_ = node_iter_->next)))
            //mod duyr 20151209:e
          {
            //ret = OB_ITER_END;
            //break;
              //mod [306 bugfix]
              if(!cell_iter_.get_rdel_cell())
              {
                  ret = OB_ITER_END;
                  break;
              }
              else
              {
                  ret = OB_SUCCESS;
              }
          }
          // add by maosy for[delete and update do not read self-query ]
          else if(node_iter_->modify_time == OB_INVALID_VERSION)
          {
              YYSYS_LOG(DEBUG, "TEST ");
              ret = OB_ITER_END;
              break;
          }
          // add e
          else
          {
            __builtin_prefetch(node_iter_);

            //tmp::debug
            if(ci_.table_id_ > 3000)
            {
              YYSYS_LOG(DEBUG, "read_atomic::debug,the cur node_iter_ptr=%p", node_iter_);
            }

            cell_iter_.reset();

            if(NULL != column_filter_ && !column_filter_->is_all_column())
            {
              cell_iter_.set_nop();
            }

            if(return_rowkey_column_)
            {
              cell_iter_.set_rowkey_column(te_key_.table_id, te_key_.row_key);
            }

            uint64_t ctime_column_id = OB_UPS_CREATE_TIME_COLUMN_ID(te_key_.table_id);
            cell_iter_.set_ctime_column_id(ctime_column_id);

            //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b
            if(data_mark_param_.is_valid())
            {
              if(data_mark_param_.need_modify_time_)
              {
                cell_iter_.set_data_mark_mtime(data_mark_param_.modify_time_cid_, node_iter_->modify_time);
                YYSYS_LOG(DEBUG, "mul_del::debug,orig_modify_time=%ld", node_iter_->modify_time);
              }
              if(data_mark_param_.need_major_version_)
              {
                cell_iter_.set_data_mark_major_ver(data_mark_param_.major_version_cid_, OB_INVALID_DATA);
              }
              if(data_mark_param_.need_minor_version_)
              {
                cell_iter_.set_data_mark_minor_ver_start(data_mark_param_.minor_ver_start_cid_, OB_INVALID_DATA);
              }
              if(data_mark_param_.need_minor_version_)
              {
                cell_iter_.set_data_mark_minor_ver_end(data_mark_param_.minor_ver_end_cid_, OB_INVALID_DATA);
              }
              if(data_mark_param_.need_data_store_type_)
              {
                cell_iter_.set_data_mark_data_store_type(data_mark_param_.data_store_type_cid_, OB_INVALID_DATA);
              }
            }
            //add duyr 20160531:e
            uint64_t mtime_column_id = OB_UPS_MODIFY_TIME_COLUMN_ID(te_key_.table_id);
            cell_iter_.set_mtime(mtime_column_id, node_iter_->modify_time);


            cell_iter_.set_cell_info_node(node_iter_);

            cell_iter_.set_head();
            ret = OB_SUCCESS;
            continue;
          }
        }
        if(OB_SUCCESS != ret)
        {
          break;
        }

        ret = cell_iter_.get_cell(ci_.column_id_, ci_.value_);
        if(OB_SUCCESS != ret)
        {
          break;
        }
        //add hongchen[SECONDARY_INDEX_OPTI_BUGFIX] 20170809:b
        if (cell_iter_.is_cur_cell_from_te_kay())
        {
          ci_.is_from_te_key_ = true;
        }
        else
        {
          ci_.is_from_te_key_ = false;
        }
        //add hongchen[SECONDARY_INDEX_OPTI_BUGFIX] 20170809:e
        YYSYS_LOG(DEBUG, "NEXT_CELL: tid=%ld value=%p node=%p %s",
                  NULL == session_ctx_ ? 0 : session_ctx_->get_trans_id(), te_value_, node_iter_, print_cellinfo(&ci_));
        if(trans_end_(ci_.value_))
        {
          //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151209:b
          YYSYS_LOG(DEBUG, "=====================trans end =====================");
          if(read_atomic_param_is_valid_)
          {
            YYSYS_LOG(DEBUG, "trans end!"
              "ret=%d,cid=%ld,val=[%s]", ret, ci_.column_id_, to_cstring(ci_.value_));
            NodeList *cur_node_list = get_read_atomic_cur_node_list();
            if(cur_node_list == NULL)
            {
              ret = OB_ERR_UNEXPECTED;
              YYSYS_LOG(ERROR, "cur node list can't be NULL!ret=%d", ret);
            }
            else
            {
              cur_node_list->list_iter_state_ = ITER_END;
              if(READING_COMMIT_DATA == cur_iter_read_state_ && NULL != cur_node_list->list_head_ &&
                 NULL != node_iter_ && OB_SUCCESS !=
                                       (ret = update_prepare_list_and_transver(cur_node_list->list_head_, node_iter_,
                                                                               NULL, NULL, true)))
              {
                YYSYS_LOG(WARN, "fail to update prepare list and preapre transver!ret=%d", ret);
              }
            }

            if(OB_SUCCESS != ret)
            {
              break;
            }
            else if(has_remainder_real_data_need_output())
            {
              cell_iter_.reset();
              YYSYS_LOG(DEBUG, "read_atomic::debug,has real data output!continue!"
                "ret=%d,next_head_node_idx_=%ld,total_iter_list_count_=%ld", ret, next_head_node_idx_,
                        node_list_array_.count());
              continue;
            }
          }
          //add duyr 20151209:e
          ret = OB_ITER_END;
          break;
        }

        if(NULL != column_filter_ && !column_filter_->column_exist(ci_.column_id_))
        {
          continue;
        }
        else
        {
          if(ObModifyTimeType == ci_.value_.get_type())
          {
            int64_t v = 0;
            ci_.value_.get_modifytime(v);
            if(INT64_MAX == v && read_uncommited_data_())
            {
              ci_.value_.set_modifytime(session_ctx_->get_trans_id());
            }
          }
          else if(ObCreateTimeType == ci_.value_.get_type())
          {
            int64_t v = 0;
            ci_.value_.get_createtime(v);
            if(INT64_MAX == v && read_uncommited_data_())
            {
              ci_.value_.set_createtime(session_ctx_->get_trans_id());
            }
          }

          //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151207:b
          if(OB_SUCCESS == ret && read_atomic_param_is_valid_)
          {
            if(read_atomic_param_.need_prepare_data_ && (READING_PREPARE_DATA == cur_iter_read_state_ ||
                                                         READING_PREPARE_DATA_COMMIT_PART ==
                                                         cur_iter_read_state_))// ������prepare���ݣ���Ҫת��cid
            {
              YYSYS_LOG(DEBUG, "orig_prepare_data!tid=%ld,cid=%ld,val=[%s]", ci_.table_id_, ci_.column_id_,
                        to_cstring(ci_.value_));

              if(ObExtendType == ci_.value_.get_type())
              {//don't output any real prepare actionflag column!!!
                //use the prepare_data_extendtype_cid_ to output this actionflag column!
                if(ObActionFlag::OP_DEL_ROW == ci_.value_.get_ext())
                {
                  output_prepare_ext_type_ = ObActionFlag::OP_DEL_ROW;
                }
                continue;
              }
              else if(OB_SUCCESS !=
                      (ret = common::ObReadAtomicHelper::convert_commit_cid_to_prepare_cid(read_atomic_param_,
                                                                                           ci_.column_id_,
                                                                                           ci_.column_id_)))
              {
                YYSYS_LOG(WARN, "fail to construct prepare data column id!ret=%d", ret);
                break;
              }
              else if(NULL != column_filter_ && !column_filter_->column_exist(ci_.column_id_))
              {//don't need cur column!
                continue;
              }
              else
              {
                output_prepare_ext_type_ = ObActionFlag::OP_VALID;
              }

              YYSYS_LOG(DEBUG, "final_prepare_data!tid=%ld,"
                "cid=%ld,val=[%s],prepare_ext_type=%ld,ret=%d", ci_.table_id_, ci_.column_id_, to_cstring(ci_.value_),
                        output_prepare_ext_type_, ret);
            }
            else if((read_atomic_param_.need_commit_data_ && READING_COMMIT_DATA == cur_iter_read_state_) ||
                    (read_atomic_param_.need_exact_data_mark_data_ &&
                     READING_EXACT_DATA_MARK_COMMIT_DATA == cur_iter_read_state_) ||
                    (read_atomic_param_.need_exact_transver_data_ &&
                     (READING_EXACT_VERSET_DATA_COMMIT_PART == cur_iter_read_state_ ||
                      READING_EXACT_VERSET_DATA == cur_iter_read_state_)))
            {
              //it's safety to output committed actionflag column!
              if(ObExtendType == ci_.value_.get_type())
              {
                if(ObActionFlag::OP_DEL_ROW == ci_.value_.get_ext())
                {
                  output_commit_ext_type_ = ObActionFlag::OP_DEL_ROW;
                }
              }
              else
              {
                output_commit_ext_type_ = ObActionFlag::OP_VALID;
              }
            }
          }
          //add duyr 20151210:e

          //del duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151209:b
          //iter_counter_++;
          //del duyr 20151209:e
          break;
        }
      }//end while

      if(OB_SUCCESS == ret)
      {
        //mod duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151209:b
        //        is_iter_end_ = false;
        is_node_iter_end_ = false;
        //mod duyr 20151209:e
      }
      else
      {
        //mod duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151209:b
        //        is_iter_end_ = true;
        is_node_iter_end_ = true;
        //mod duyr 20151209:e
        cell_iter_.reset_all();//[306 bugfix]
      }

      if(read_atomic_param_is_valid_)
      {
        YYSYS_LOG(DEBUG, "next data cell"
          "ret=%d,val=[%s],cid=[%ld],is_node_iter_end_=%d,"
          "iter_counter_=%ld", ret, to_cstring(ci_.value_), ci_.column_id_, is_node_iter_end_, iter_counter_);
      }


      //tmp::debug
      if(ci_.table_id_ > 3000)
      {
        YYSYS_LOG(DEBUG, "read_atomic::debug,finish get next cell!the cur node_iter_ptr=%p", node_iter_);
      }

      return ret;
    }

    int MemTableGetIter::get_cell(ObCellInfo **cell_info)
    {
      return get_cell(cell_info, NULL);
    }

    int MemTableGetIter::get_cell(ObCellInfo **cell_info, bool *is_row_changed)
    {
      int ret = OB_SUCCESS;
      if(is_iter_end_ || 0 >= iter_counter_)
      {
        ret = OB_ITER_END;
      }
      else if(NULL == cell_info)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        *cell_info = &ci_;
        if(NULL != is_row_changed)
        {
          *is_row_changed = (1 == iter_counter_);
        }
      }

      //READ_ATOMIC read_atomic::debug
      if(read_atomic_param_is_valid_)
      {
        YYSYS_LOG(DEBUG, "\n\n================================next data cell!=========================\n"
          "ret=%d,cell_tid=%ld,cell_rowkey=[%s],cell_cid=%ld,val=[%s],"
          "iter_counter_=%ld", ret, ci_.table_id_, to_cstring(ci_.row_key_), ci_.column_id_, to_cstring(ci_.value_),
                  iter_counter_);
        if(ci_.value_.get_type() == ObExtendType && ci_.value_.get_ext() == ObActionFlag::OP_DEL_ROW)
        {
          YYSYS_LOG(INFO, "============this row is delete ==========");
        }
      }
      //add duyr [Delete_Update_Function_isolation] [JHOBv0.1] 20160531:b

      if(data_mark_param_.is_valid())
      {
        YYSYS_LOG(DEBUG, "mul_del::debug,final cell_id=%ld,cell_val=[%s],tid=[%ld],key=[%s],ret=%d", ci_.column_id_,
                  to_cstring(ci_.value_), ci_.table_id_, to_cstring(ci_.row_key_), ret);
      }
      //add duyr 20160531:e
      return ret;
    }


    //add peiouya [secondary index opti bug when rollback:b
    __thread uint64_t index_table_id = OB_INVALID_ID;
    __thread bool is_specical_index = false;

    //[403 bugfix]
    int MemTableGetIter::is_index_table(const uint64_t table_id, bool &is_index, bool &is_speci_index, const CommonSchemaManager *frozen_schema)
    {
        int ret = OB_SUCCESS;
        is_index = false;
        is_speci_index = false;
        if(index_table_id == table_id)
        {
            is_index = true;
            is_speci_index = is_specical_index;
        }
        else
        {
            ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
            UpsSchemaMgrGuard sm_guard;
            const CommonSchemaManager *sm = NULL;
            const CommonTableSchema *tm = NULL;

            if(NULL != frozen_schema)
            {
                sm = frozen_schema;
                tm = frozen_schema->get_table_schema(table_id);
            }
            else if(NULL != (sm = ups_main->get_update_server().get_table_mgr().get_schema_mgr().get_schema_mgr(sm_guard)))
            {
                tm = sm->get_table_schema(table_id);
            }
            else
            {
                ret = OB_ERR_UNEXPECTED;
                YYSYS_LOG(WARN,"get table[%lu] schema error, ret=%d", table_id, ret);
            }

            if(OB_SUCCESS == ret)
            {
                if(NULL == tm)
                {
                    ret = OB_ERR_UNEXPECTED;
                    YYSYS_LOG(WARN,"get table[%lu] schema error, ret=%d", table_id, ret);
                }
                else if(tm->get_index_helper().tbl_tid != OB_INVALID_ID)
                {
                    const CommonTableSchema *tm_dt = NULL;
                    if(NULL == (tm_dt = sm->get_table_schema(tm->get_index_helper().tbl_tid)))
                    {
                        ret = OB_ERR_UNEXPECTED;
                        YYSYS_LOG(WARN,"get table[%lu] schema error, ret=%d", tm->get_index_helper().tbl_tid, ret);
                    }
                    else if(tm->get_rowkey_info().get_size() != tm_dt->get_rowkey_info().get_size())
                    {
                        is_index = true;
                        is_speci_index = false;
                        index_table_id = table_id;
                        is_specical_index = false;
                    }
                    else
                    {
                        is_index = true;
                        is_speci_index = true;
                        index_table_id =table_id;
                        is_specical_index = true;
                    }
                }
            }
        }
        return ret;
    }

    //add:e

    bool MemTableGetIter::read_uncommited_data_(
      //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151210:b
      const bool need_locked
      //add duyr 20151210:e
    )
    {
      bool bret = false;
      if(NULL != session_ctx_ && NULL != te_value_ &&
         te_value_->row_lock.is_exclusive_locked_by(session_ctx_->get_session_descriptor()))
      {
        bret = true;
      }

      //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151210:b
      //FIXME:should use modify time to filter prepare data commit part
      //and exact verset data commit part?
      //we use modify time to filter just prepare data commit part!
      //need test whether it will lose data or not!!!!
      if(!bret && !need_locked && read_atomic_param_is_valid_ && (READING_PREPARE_DATA == cur_iter_read_state_
                                                                  //              || READING_PREPARE_DATA_COMMIT_PART == cur_iter_read_state_
                                                                  || READING_EXACT_VERSET_DATA_COMMIT_PART ==
                                                                     cur_iter_read_state_ ||
                                                                  READING_EXACT_VERSET_DATA == cur_iter_read_state_))
      {//FIXME:it's not safe to read uncommited data without exclusive lock!
        bret = true;
      }
      //add duyr 20151210:e
      return bret;
    }

    ObCellInfoNode *MemTableGetIter::get_list_head_()
    {
      ObCellInfoNode *ret = NULL;
      if(NULL != te_value_)
      {
        if(NULL != session_ctx_ && ST_READ_ONLY == session_ctx_->get_type() && NULL != te_value_->list_head &&
           session_ctx_->get_trans_id() < te_value_->list_head->modify_time)
        {
          // need to lookup undo list
          ObUndoNode *iter = te_value_->undo_list;
          while(NULL != iter)
          {
            YYSYS_LOG(DEBUG, "read_atomic::debug,looking undo list!"
              "ctx_trans_id=%ld,undo_list_head_mod_time=%ld,ret=%p", session_ctx_->get_trans_id(),
                      iter->head->modify_time, ret);
            if(NULL != iter->head && session_ctx_->get_trans_id() >= iter->head->modify_time)
            {
              ret = iter->head;
              break;
            }
            else
            {
              iter = iter->next;
            }
          }

          YYSYS_LOG(DEBUG, "read_atomic::debug,finish lookup undo list!"
            "ctx_trans_id=%ld,list_head=%ld,ret=%p", session_ctx_->get_trans_id(), te_value_->list_head->modify_time,
                    ret);
        }

        if(NULL == ret)
        {
          ret = te_value_->list_head;
          if(NULL != te_value_->cur_uc_info && read_uncommited_data_(
            //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151210:b
            true
            //add duyr 20151210:e
          ))
          {
            if(NULL == ret)
            {
              if(te_value_->cur_uc_info->uc_list_head->modify_time == OB_INVALID_VERSION)
              {
                YYSYS_LOG(INFO, "TEST");
              }
              else
              {
                ret = te_value_->cur_uc_info->uc_list_head;
              }
            }
            else if(NULL != te_value_->list_tail)
            {
              // ��δ�ύ���ݽӵ�����β�� ��������������� �����޸�list_tail ��˶������ύû��Ӱ��
              te_value_->list_tail->next = te_value_->cur_uc_info->uc_list_head;
            }
          }
        }
      }

      YYSYS_LOG(DEBUG, "read_atomic::debug,finish get commit list head!"
        "session_ctx_=%p,ret=%p", session_ctx_, ret);
      if(NULL != session_ctx_)
        YYSYS_LOG(DEBUG, "read_atomic::debug,ctx_trans_id=%ld", session_ctx_->get_trans_id());

      return ret;
    }

    //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151208:b
    /*���ڵ��� Ԫ���� ����ʵ������*/
    int MemTableGetIter::next_cell()
    {
      int ret = OB_SUCCESS;
      if(is_iter_end_ || (is_node_iter_end_ && is_extend_iter_end_))
      {
        ret = OB_ITER_END;
      }

      if(OB_SUCCESS == ret)
      {
        //don't change the order,must first iter node,then iter the extend data!
        //first get real data
        if(!is_node_iter_end_)
        {
          ret = node_iter_next_cell_();//���ݵ���
        }

        //second get extend data
        if(OB_SUCCESS != ret && OB_ITER_END != ret && OB_UPS_TABLE_NOT_EXIST != ret)
        {
          YYSYS_LOG(WARN, "fail to get next node cell!ret=%d", ret);
        }
        else if(read_atomic_param_is_valid_)
        {
          bool has_call_next_extend_cell = false;
          if(OB_SUCCESS == ret && cur_cellinfo_is_rne_)
          {//means there is no real data output!
            //but if has extend output data,
            //we will not output cur cellinfo witch is ObActionFlag::OP_ROW_DOES_NOT_EXIST!!
            if(OB_ITER_END == (ret = extend_iter_next_cell_()))//Ԫ���ݵ���
            {//no extend output data!!!!
              //just output cur cellinfo
              ret = OB_SUCCESS;
            }
            else if(OB_SUCCESS == ret)
            {//has extend output data!!!
              //cur cellinfo has been modified!!!!
              cur_cellinfo_is_rne_ = false;
            }
            has_call_next_extend_cell = true;
            YYSYS_LOG(DEBUG, "read_atomic::debug,won't output the extend obj!");
          }

          if(!has_call_next_extend_cell && (OB_ITER_END == ret || is_node_iter_end_))
          {
            //            if (!is_extend_iter_end_)
            //            {
            ret = extend_iter_next_cell_();
            if(OB_SUCCESS != ret && OB_ITER_END != ret)
            {
              YYSYS_LOG(WARN, "fail to get next extend cell!ret=%d", ret);
            }
            //            }
          }
        }
      }

      if(OB_SUCCESS == ret)
      {
        is_iter_end_ = false;
        iter_counter_++;
      }
      else
      {
        is_iter_end_ = true;
      }

      if(read_atomic_param_is_valid_)
        YYSYS_LOG(DEBUG, "read_atomic::debug,end of next cell!tid=%ld,ret=%d,"
          "is_iter_end_=%d,is_node_iter_end_=%d,"
          "is_extend_iter_end_=%d,"
          "need_return_rowkey_indep_=%d,"
          "cell_tid=%ld,cell_rowkey=[%s],cell_cid=%ld,cell_val=[%s]", te_key_.table_id, ret, is_iter_end_,
                  is_node_iter_end_, is_extend_iter_end_, need_return_rowkey_indep_, ci_.table_id_,
                  to_cstring(ci_.row_key_), ci_.column_id_, to_cstring(ci_.value_));

      return ret;
    }

    /*@beif ��memtable ֮ǰ ȷ����ȡ���� �� ��������������*/
    int MemTableGetIter::prepare_read_atomic_output_data()
    {
      int ret = OB_SUCCESS;
      const ObCellInfoNode *commit_list_head = NULL;
      const ObCellInfoNode *commit_list_end = NULL;
      const ObCellInfoNode *uc_list_head = NULL;
      const ObUndoNode *commit_undo_iter = NULL;
      //      bool ignor_high_priority_param = false;
      bool will_output_exact_data = false;
      bool will_output_extend_data = false;
      bool will_output_commit_data = false;
      bool will_output_prepare_data = false;
      if(!read_atomic_param_is_valid_)
      {
        ret = OB_NOT_INIT;
        YYSYS_LOG(ERROR, "must has valid read atomic param!ret=%d,param=[%s]", ret, to_cstring(read_atomic_param_));
      }
      else if(is_row_empty_)
      {
        ret = OB_ITER_END;
        YYSYS_LOG(DEBUG, "read_atomic::debug,cur row is empty!won't output any data!"
          "ret=%d,row_key=[%s]", ret, to_cstring(te_key_.row_key));
      }

      if(OB_SUCCESS == ret && read_atomic_param_is_valid_)
      {
        //get commit list
        get_commit_list_(commit_list_head, commit_list_end, uc_list_head, commit_undo_iter);

        //get the read strategy!  ���ֶ��㷨  ����data_mark ȷ����ȡ�Ĳ���
        if(OB_SUCCESS == ret && read_atomic_param_.need_exact_data_mark_data_)
        {
          if(OB_SUCCESS !=
             (ret = common::ObReadAtomicHelper::get_strategy_with_data_mark(read_atomic_param_, data_mark_,
                                                                            atomic_read_strategy_)))
          {
            YYSYS_LOG(WARN, "fail to get read atomic strategy!"
              "ret=%d,atomic_read_strategy_=%d", ret, atomic_read_strategy_);
          }
          else if(ObReadAtomicParam::INVALID_STRATEGY == atomic_read_strategy_)
          {
            ret = OB_ERR_UNEXPECTED;
            YYSYS_LOG(ERROR, "invalid strategy!ret=%d,atomic_read_strategy_=%d", ret, atomic_read_strategy_);
          }
          else if(ObReadAtomicParam::NO_READ == atomic_read_strategy_)
          {//won't read any data!!!!it's ok!!!!
            ret = OB_ITER_END;
            YYSYS_LOG(DEBUG, "read_atomic::debug,strategy is no read,"
              "memtable iter will not read any data!"
              "data_mark=[%s],ret=%d", to_cstring(data_mark_), ret);
          }
          else if(ObReadAtomicParam::READ_WITH_COMMIT_DATA != atomic_read_strategy_ &&
                  ObReadAtomicParam::READ_WITH_READ_ATOMIC_PARAM != atomic_read_strategy_)
          {
            ret = OB_NOT_SUPPORTED;
            YYSYS_LOG(ERROR, "unknow atomic read strategy[%d],ret=%d", atomic_read_strategy_, ret);
          }

          YYSYS_LOG(DEBUG, "read_atomic::debug,finish get stragety!ret=%d,stragety=%d", ret, atomic_read_strategy_);
        }

        //find exact data mark or exact transver data!
        if(OB_SUCCESS == ret)
        {
          if(read_atomic_param_.need_exact_data_mark_data_ &&
             ObReadAtomicParam::READ_WITH_COMMIT_DATA == atomic_read_strategy_)
          {
            if(NULL != commit_list_head &&
               OB_SUCCESS != (ret = add_iter_list_(commit_list_head, NULL, READING_EXACT_DATA_MARK_COMMIT_DATA)))
            {
              YYSYS_LOG(WARN, "fail to store commit list!ret=%d", ret);
            }
            else if(NULL != commit_list_head)
            {
              will_output_exact_data = true;
            }
          }
          else if(read_atomic_param_.need_exact_transver_data_)
          {
            const ObCellInfoNode *exact_list_head = NULL;
            const ObCellInfoNode *exact_list_commit_part_head = NULL;
            const common::ObTransVersion *exact_list_tail_transver = NULL;
            const ObUndoNode *exact_undo_iter = NULL;

            if(!read_atomic_param_.is_trans_verset_map_create_)
            {
              ret = OB_NOT_INIT;
              YYSYS_LOG(ERROR, "trans verset map is not created!ret=%d", ret);
            }
            else if(read_atomic_param_.exact_trans_verset_map_.size() <= 0)
            {
              //no exact transver!
            }
            else if(OB_SUCCESS !=
                    (ret = get_exact_trans_verset_list_(exact_list_head, exact_list_tail_transver, exact_undo_iter,
                                                        exact_list_commit_part_head)))
            {
              YYSYS_LOG(WARN, "fail to get exact trans verset list!ret=%d", ret);
            }
            else if(NULL != exact_list_head)
            {
              if(NULL == exact_list_commit_part_head || NULL == exact_list_tail_transver ||
                 !exact_list_tail_transver->is_valid())
              {
                ret = OB_ERR_UNEXPECTED;
                YYSYS_LOG(ERROR, "get invalid exact list head!ret=%d,exact_list_commit_part_head=%p,"
                  "exact_list_tail_transver=%p", ret, exact_list_commit_part_head, exact_list_tail_transver);
              }
                /*�����uncommited�����У������� commited ������*/
              else if(exact_list_head != exact_list_commit_part_head && OB_SUCCESS != (ret = add_iter_list_(
                exact_list_commit_part_head, exact_list_tail_transver, READING_EXACT_VERSET_DATA_COMMIT_PART)))
              {
                YYSYS_LOG(WARN, "fail to add commit part of the exact verset list!ret=%d", ret);
              }
                /*����exact_list_head ���� */
              else if(OB_SUCCESS !=
                      (ret = add_iter_list_(exact_list_head, exact_list_tail_transver, READING_EXACT_VERSET_DATA)))
              {
                YYSYS_LOG(WARN, "fail to add exact list!ret=%d", ret);
              }
              else
              {
                will_output_exact_data = true;
              }
            }
            if(NULL != exact_list_tail_transver)
            {
              YYSYS_LOG(DEBUG, "shili::debug  exact_list_tail_transver:%s", to_cstring(*exact_list_tail_transver));
            }
          }
        }


        //if will output commit list,
        //must first add commit list,
        //then and prepare list!!!!
        if(OB_SUCCESS == ret && !will_output_commit_data && read_atomic_param_.need_commit_data_ &&
           NULL != commit_list_head)
        {
          //commit list don't need list tail trans version!
          //it will be ended by modify time!(such as trans_end() fuc!)
          if(OB_SUCCESS != (ret = add_iter_list_(commit_list_head, NULL, READING_COMMIT_DATA)))
          {
            YYSYS_LOG(WARN, "fail to add commit list!ret=%d", ret);
          }
          else
          {
            will_output_commit_data = true;
          }
        }

        //get prepare list and last prepare trans version!
        //just get a initial value here,
        //prepare list and prepare last transver may modified
        //when finish output commit data!!!!
        if(OB_SUCCESS == ret && OB_SUCCESS !=
                                (ret = update_prepare_list_and_transver(commit_list_head, commit_list_end, uc_list_head,
                                                                        commit_undo_iter, false,
                                                                        &will_output_prepare_data)))
        {
          YYSYS_LOG(WARN, "fail to get prepare list!ret=%d", ret);
        }
        else if(OB_SUCCESS == ret && !will_output_extend_data && read_atomic_param_.need_last_prepare_trans_ver_ &&
                NULL != output_last_prepare_trans_ver_)
        {
          will_output_extend_data = true;
        }

        //find last commit trans version
        //it's just a initial value
        //it will be modified again when output commit data!!!!!
        if(OB_SUCCESS == ret)
        {
          if(read_atomic_param_.need_last_commit_trans_ver_)
          {
            const ObCellInfoNode *iter = commit_list_head;
            bool is_trans_end = trans_end_(iter);
            while(OB_SUCCESS == ret && NULL != iter && !is_trans_end)
            {
              if((is_trans_end = trans_end_(iter->next)) || NULL == iter->next || commit_list_end == iter->next)
              {
                common::ObTransVersion *trans_ver = NULL;
                if(OB_SUCCESS != (ret = common::ObReadAtomicHelper::transver_cast_from_buf(iter->trans_version_,
                                                                                           sizeof(iter->trans_version_),
                                                                                           trans_ver)))
                {
                  YYSYS_LOG(WARN, "fail to cast buf to transver!ret=%d", ret);
                }
                else if(NULL == trans_ver)
                {
                  ret = OB_ERR_UNEXPECTED;
                  YYSYS_LOG(ERROR, "last commit trans ver is NULL!ret=%d", ret);
                }
                else
                {
                  output_last_commit_trans_ver_ = trans_ver;
                  will_output_extend_data = true;

                  YYSYS_LOG(DEBUG,
                            "read_atomic::debug,succ get last commit trans ver!last_commit_ver=%p,trans_ver_buf=%p",
                            output_last_commit_trans_ver_, iter->trans_version_);
                  if(NULL != trans_ver)
                    YYSYS_LOG(DEBUG, "read_atomic::debug,trans_ver=[%s]", to_cstring(*trans_ver));
                }
                break;
              }
              iter = iter->next;
            }
          }
        }

        //find prevcommit trans version set
        //it's just a initial value
        //it will be modified when output commit data!!!!!
        if(OB_SUCCESS == ret)
        {
          if(read_atomic_param_.need_prevcommit_trans_verset_)
          {
            if(OB_SUCCESS != (ret = common::ObReadAtomicHelper::gen_prevcommit_trans_verset_cids(read_atomic_param_,
                                                                                                 prevcommit_trans_verset_cids_,
                                                                                                 sizeof(prevcommit_trans_verset_cids_))))
            {
              YYSYS_LOG(WARN, "fail to get trans verset cids!tmp_ret=%d", ret);
            }
              //if will read commit data
              //don't need get prev commit transver from commit list here!
              //commit list's transver will be filled when output commit data!!!!
            else if(OB_SUCCESS != (ret = store_prev_commit_verset_(commit_list_head, commit_list_end, commit_undo_iter,
                                                                   will_output_commit_data)))
            {
              YYSYS_LOG(WARN, "fail to store prev commit verset!ret=%d", ret);
            }
            else if(!will_output_extend_data)
            {
              int64_t total_prevcommit_transver_num =
                output_commit_list_transverset_.count() + output_undo_list_transverset_.count();
              if(total_prevcommit_transver_num > 0 || will_output_commit_data)
              {
                will_output_extend_data = true;
              }
            }
          }
        }

        //decide if need output rowkey independently
        if(OB_SUCCESS == ret)
        {
          if(read_atomic_param_.need_row_key_)
          {
            return_rowkey_column_ = true;
            if(!will_output_exact_data && !will_output_commit_data)
            {//means cur row has no commit data output,
              //have to output rowkey indep!!!!
              need_return_rowkey_indep_ = true;
              will_output_extend_data = true;
            }
          }
        }


        if(OB_SUCCESS == ret && read_atomic_param_.need_data_mark_ &&
           (will_output_exact_data || will_output_commit_data || will_output_prepare_data || will_output_extend_data))
        {
          if(!data_mark_.is_valid())
          {
            ret = OB_ERROR;
            YYSYS_LOG(ERROR, "invalid data mark!ret=%d,data_mark=[%s]", ret, to_cstring(data_mark_));
          }
          else
          {
            output_paxos_id_ = data_mark_.ups_paxos_id_;
            output_major_ver_ = data_mark_.major_version_;
            output_minor_ver_start_ = data_mark_.minor_version_start_;
            output_minor_ver_end_ = data_mark_.minor_version_end_;
            output_data_store_type_ = data_mark_.data_store_type_;
            will_output_extend_data = true;
          }
        }

        if(OB_SUCCESS == ret)
        {
          if(will_output_exact_data || will_output_commit_data || will_output_prepare_data || will_output_extend_data)
          {
            if(OB_INVALID_DATA == output_commit_ext_type_)
            {
              output_commit_ext_type_ = ObActionFlag::OP_ROW_DOES_NOT_EXIST;
            }

            if(OB_INVALID_DATA == output_prepare_ext_type_)
            {
              output_prepare_ext_type_ = ObActionFlag::OP_ROW_DOES_NOT_EXIST;
            }
          }
        }
      }//end if (OB_SUCCESS == tmp_ret && read_atomic_param_is_valid_)

      if(OB_SUCCESS != ret)
      {//make sure won't output any real data or any extend data!!!
        is_extend_iter_end_ = true;
        next_head_node_idx_ = 0;
        node_list_array_.clear();
        if(OB_ITER_END == ret)
        {
          ret = OB_SUCCESS;
        }

        YYSYS_LOG(DEBUG, "read_atomic::debug,mem iter won't output any data!"
          "ret=%d,row_key=[%s]", ret, to_cstring(te_key_.row_key));
      }
      YYSYS_LOG(DEBUG, "read_atomic::debug,finish prepare read atomic!"
        "ret=%d,rowk_key=[%s],atomic_param_is_valid=%d,"
        "need_return_rowkey_indep_=%d,will_output_exact_data=%d,"
        "will_output_commit_data=%d,will_output_extend_data=%d,"
        "will_output_prepare_data=%d,total_node_list_count=%ld", ret, to_cstring(te_key_.row_key),
                read_atomic_param_is_valid_, need_return_rowkey_indep_, will_output_exact_data, will_output_commit_data,
                will_output_extend_data, will_output_prepare_data, node_list_array_.count());
      return ret;
    }

    /*����TEvalue�и�������*/
    void MemTableGetIter::get_commit_list_(const ObCellInfoNode *&commit_list_head,
                                           const ObCellInfoNode *&commit_list_end, const ObCellInfoNode *&uc_list_head,
                                           const ObUndoNode *&commit_undo_iter)
    {
      bool will_read_uncommit_data = false;
      commit_list_head = NULL;
      commit_list_end = NULL;
      uc_list_head = NULL;
      commit_undo_iter = NULL;
      TEValueUCInfo *cur_uc_info = NULL;

      if(NULL != te_value_)
      {
        if(NULL != session_ctx_ && ST_READ_ONLY == session_ctx_->get_type() && NULL != te_value_->list_head &&
           session_ctx_->get_trans_id() < te_value_->list_head->modify_time)
        {
          // need to lookup undo list
          ObUndoNode *iter = te_value_->undo_list;
          while(NULL != iter)
          {
            YYSYS_LOG(DEBUG, "read_atomic::debug,looking read atomic undo list!"
              "ctx_trans_id=%ld,undo_list_head_mod_time=%ld,commit_list_head=%p", session_ctx_->get_trans_id(),
                      iter->head->modify_time, commit_list_head);
            if(NULL != iter->head && session_ctx_->get_trans_id() >= iter->head->modify_time)
            {
              if(NULL != (cur_uc_info = te_value_->cur_uc_info))
              {
                uc_list_head = cur_uc_info->uc_list_head;
              }
              commit_list_head = iter->head;
              commit_undo_iter = iter;
              break;
            }
            iter = iter->next;
          }

          YYSYS_LOG(DEBUG, "read_atomic::debug,finish read atomic lookup undo list!"
            "ctx_trans_id=%ld,list_head=%ld,commit_list_head=%p", session_ctx_->get_trans_id(),
                    te_value_->list_head->modify_time, commit_list_head);

        }

        if(NULL == commit_list_head)
        {
          if(NULL != (cur_uc_info = te_value_->cur_uc_info))
          {
            uc_list_head = cur_uc_info->uc_list_head;
          }
          commit_list_head = te_value_->list_head;
          if(NULL != te_value_->cur_uc_info && read_uncommited_data_(true))
          {
            will_read_uncommit_data = true;
            if(NULL == commit_list_head)
            {//it's ok to use te_value_->cur_uc_info,
              //because te_values_ is locked,no one will change the te_value_->cur_uc_info ptr!
              if(NULL != te_value_->cur_uc_info)
              {
                uc_list_head = te_value_->cur_uc_info->uc_list_head;
              }
              commit_list_head = te_value_->cur_uc_info->uc_list_head;
            }
            else if(NULL != te_value_->list_tail)
            {
              // ��δ�ύ���ݽӵ�����β�� ��������������� �����޸�list_tail ��˶������ύû��Ӱ��
              te_value_->list_tail->next = te_value_->cur_uc_info->uc_list_head;
            }
          }
        }

        if(NULL != commit_list_head)
        {
          const ObCellInfoNode *iter = commit_list_head;
          while(NULL != iter)
          {
            if(!will_read_uncommit_data && NULL != session_ctx_ && iter->modify_time > session_ctx_->get_trans_id())
            {
              commit_list_end = iter;
              break;
            }
            iter = iter->next;
          }
        }
      }

      YYSYS_LOG(DEBUG, "read_atomic::debug,finish get read atomic commit list head!"
        "session_ctx_=%p,te_value_=%p,commit_list_head=%p", session_ctx_, te_value_, commit_list_head);
      if(NULL != session_ctx_)
        YYSYS_LOG(DEBUG, "read_atomic::debug,ctx_trans_id=%ld,ctx_type=%d", session_ctx_->get_trans_id(),
                  session_ctx_->get_type());
      if(NULL != te_value_ && NULL != te_value_->list_head)
        YYSYS_LOG(DEBUG, "read_atomic::debug,list_head_modtime=%ld,undo_list=%p", te_value_->list_head->modify_time,
                  te_value_->undo_list);

    }




    /*@berif ȷ��TeValue�з������������ �����µ�trans_version*/
    /*@param exact_list_head [out]   ���շ������������ ���ڵ� ������head
    * @param exact_list_tail_trans_ver [out]  TEValue�� version���ڵ���version_set �����µ�trans_version
    * @param  exact_undo_iter[out]   ���trans_version  �� undo_list�У���Ϊ undolist������ͷ
    * @param  exact_list_commit_part_head [out]  trans_version �ڵ�commit_list(commit_list���� undo_list����һ��������)
    */
    int MemTableGetIter::get_exact_trans_verset_list_(const ObCellInfoNode *&exact_list_head,
                                                      const common::ObTransVersion *&exact_list_tail_trans_ver,
                                                      const ObUndoNode *&exact_undo_iter,
                                                      const ObCellInfoNode *&exact_list_commit_part_head)
    {
      int ret = OB_SUCCESS;
      TEValueUCInfo *cur_uc_info = NULL;
      const ObCellInfoNode *iter = NULL;
      exact_list_head = NULL;
      exact_list_tail_trans_ver = NULL;
      exact_undo_iter = NULL;
      exact_list_commit_part_head = NULL;
      if(!read_atomic_param_is_valid_ || !read_atomic_param_.need_exact_transver_data_ ||
         !read_atomic_param_.is_trans_verset_map_create_)
      {
        ret = OB_NOT_INIT;
        YYSYS_LOG(ERROR, "must has valid param!ret=%d,param=[%s]", ret, to_cstring(read_atomic_param_));
      }
      else if(read_atomic_param_.exact_trans_verset_map_.size() > 0)
      {
        //lookup newest ver in ucommit list
        if(OB_SUCCESS == ret && NULL != te_value_ && NULL != (cur_uc_info = te_value_->cur_uc_info))
        {
          exact_list_head = NULL;
          exact_list_tail_trans_ver = NULL;
          exact_undo_iter = NULL;
          exact_list_commit_part_head = NULL;
          const ObCellInfoNode *uc_list_head = cur_uc_info->uc_list_head;
          const ObCellInfoNode *list_head = te_value_->list_head;
          iter = uc_list_head;
          /*��ӡtrans_version */
          if(read_atomic_param_.need_exact_transver_data_)
          {
            YYSYS_LOG(DEBUG,
                      "shili::debug++++++++++++++++++++++++++trans_version_printing++++++++++++++++++++++++++++\n");
            ObReadAtomicParam::ExactTransversetMap::const_iterator begin = read_atomic_param_.exact_trans_verset_map_.begin();
            ObReadAtomicParam::ExactTransversetMap::const_iterator end = read_atomic_param_.exact_trans_verset_map_.end();
            ObReadAtomicParam::ExactTransversetMap::const_iterator iter = begin;
            while(OB_SUCCESS == ret && iter != end)
            {
              YYSYS_LOG(DEBUG, "shili::debug,trans_version:%s,trans_version_state:%d", to_cstring(iter->first),
                        iter->second);
              iter++;
            }
            YYSYS_LOG(DEBUG,
                      "shili::debug++++++++++++++++++++++++++trans_version_printing  end++++++++++++++++++++++++++++");
          }
          /*������uncommit ������Ѱ��*/
          while(OB_SUCCESS == ret && NULL != iter)
          {
            common::ObTransVersion *des_ver = NULL;
            if(OB_SUCCESS != (ret = common::ObReadAtomicHelper::transver_cast_from_buf(iter->trans_version_,
                                                                                       static_cast<int64_t>(sizeof(iter->trans_version_)),
                                                                                       des_ver)))
            {
              YYSYS_LOG(WARN, "fail to cast buf to transver!ret=%d", ret);
            }
            else if(NULL != des_ver &&
                    common::ObReadAtomicHelper::is_on_exact_trans_verset(read_atomic_param_, des_ver))
            {//no matter if des_ver is valid or not!!!!!
              exact_list_head = uc_list_head;
              exact_list_commit_part_head = list_head;
              exact_list_tail_trans_ver = des_ver;
              if(NULL == exact_list_commit_part_head)
              {
                exact_list_commit_part_head = exact_list_head;
              }
              YYSYS_LOG(DEBUG, "read_atomic::debug,find one exact_ver_node_iter_ver=[%s] on uc list!"
                "on prepare list", to_cstring(*des_ver));
            }
            iter = iter->next;
          }
        }

        /*uncommited ����û���ҵ�����commit��������*/
        //lookup newest ver in commit list
        if(OB_SUCCESS == ret && NULL == exact_list_head && NULL != te_value_)
        {
          exact_list_head = NULL;
          exact_list_tail_trans_ver = NULL;
          exact_undo_iter = NULL;
          exact_list_commit_part_head = NULL;
          const ObCellInfoNode *list_head = te_value_->list_head;
          iter = list_head;
          while(OB_SUCCESS == ret && NULL != iter)
          {
            common::ObTransVersion *des_ver = NULL;
            if(OB_SUCCESS != (ret = common::ObReadAtomicHelper::transver_cast_from_buf(iter->trans_version_,
                                                                                       static_cast<int64_t>(sizeof(iter->trans_version_)),
                                                                                       des_ver)))
            {
              YYSYS_LOG(WARN, "fail to cast buf to trans ver!ret=%d", ret);
            }
            else if(NULL != des_ver &&
                    common::ObReadAtomicHelper::is_on_exact_trans_verset(read_atomic_param_, des_ver))
            {//no matter if des_ver is valid or not!!!!!
              exact_list_head = list_head;
              exact_list_commit_part_head = exact_list_head;
              exact_list_tail_trans_ver = des_ver;
              YYSYS_LOG(DEBUG, "read_atomic::debug,find one exact_ver_node_iter_ver=[%s] on commit list",
                        to_cstring(*des_ver));
            }
            iter = iter->next;
          }
        }

        //lookup undo list  commit������û���ҵ�����undo list����
        if(OB_SUCCESS == ret && NULL == exact_list_head && NULL != te_value_)
        {
          exact_list_head = NULL;
          exact_list_tail_trans_ver = NULL;
          exact_undo_iter = NULL;
          exact_list_commit_part_head = NULL;
          const ObUndoNode *undo_iter = te_value_->undo_list;

          while(OB_SUCCESS == ret && NULL == exact_list_head && NULL != undo_iter)
          {
            iter = undo_iter->head;
            while(OB_SUCCESS == ret && NULL != iter)
            {
              common::ObTransVersion *des_ver = NULL;
              if(OB_SUCCESS != (ret = common::ObReadAtomicHelper::transver_cast_from_buf(iter->trans_version_,
                                                                                         static_cast<int64_t>(sizeof(iter->trans_version_)),
                                                                                         des_ver)))
              {
                YYSYS_LOG(WARN, "fail to cast buf to trans ver!ret=%d", ret);
              }
              else if(NULL != des_ver &&
                      common::ObReadAtomicHelper::is_on_exact_trans_verset(read_atomic_param_, des_ver))
              {//no matter if des_ver is valid or not!!!!!
                exact_list_head = undo_iter->head;
                exact_list_commit_part_head = exact_list_head;
                exact_list_tail_trans_ver = des_ver;
                exact_undo_iter = undo_iter;
                YYSYS_LOG(DEBUG, "read_atomic::debug,find one exact_ver_node_iter_ver=[%s] on undo list",
                          to_cstring(*des_ver));
              }
              iter = iter->next;
            }
            undo_iter = undo_iter->next;
          }
        }

        if(OB_SUCCESS == ret && NULL != exact_list_head)
        {
          //we don't check if exact_list_tail_trans_ver here,
          //because if it's not NULL,means it comes from exact trans version map!!!!
          if(NULL == exact_list_tail_trans_ver || NULL == exact_list_commit_part_head)
          {
            ret = OB_ERR_UNEXPECTED;
            YYSYS_LOG(ERROR, "ptr can't be NULL!ret=%d,exact_list_tail_trans_ver=%p,exact_list_commit_part_head=%p",
                      ret, exact_list_tail_trans_ver, exact_list_commit_part_head);
          }
          else
          {
            YYSYS_LOG(DEBUG, "read_atomic::debug,final exact_list_tail_trans_ver=[%s],is_valid=%d",
                      to_cstring(*exact_list_tail_trans_ver), exact_list_tail_trans_ver->is_valid());
          }
        }
      }

      if(NULL == exact_list_head || NULL == exact_list_commit_part_head || NULL == exact_list_tail_trans_ver)
      {
        exact_list_head = NULL;
        exact_list_tail_trans_ver = NULL;
        exact_undo_iter = NULL;
        exact_list_commit_part_head = NULL;
      }
      YYSYS_LOG(DEBUG, "read_atomic::debug,finish get exact list!"
        "exact_list_head=%p,exact_list_tail_trans_ver=%p,"
        "exact_undo_iter=%p,exact_list_commit_part_head=%p,ret=%d", exact_list_head, exact_list_tail_trans_ver,
                exact_undo_iter, exact_list_commit_part_head, ret);

      return ret;
    }

    int MemTableGetIter::get_prepare_list_(const ObCellInfoNode *&prepare_list_head,
                                           const common::ObTransVersion *&prepare_list_tail_trans_ver,
                                           const ObUndoNode *&prepare_undo_iter,
                                           const ObCellInfoNode *&prepare_list_commit_part_head,
                                           const ObCellInfoNode *commit_list_head,
                                           const ObCellInfoNode *commit_list_end, const ObCellInfoNode *uc_list_head,
                                           const ObUndoNode *commit_undo_iter)
    {
      int ret = OB_SUCCESS;
      TEValueUCInfo *cur_uc_info = NULL;
      const ObCellInfoNode *trans_ver_node = NULL;
      common::ObTransVersion *trans_ver = NULL;
      prepare_list_head = NULL;
      prepare_list_tail_trans_ver = NULL;
      prepare_undo_iter = NULL;
      prepare_list_commit_part_head = NULL;


      //first find the prepare_list_head
      if(NULL != commit_list_head && NULL != commit_list_end)
      {//means prepare list on the same list
        prepare_list_head = commit_list_head;
        prepare_undo_iter = commit_undo_iter;
        trans_ver_node = commit_list_end;
        prepare_list_commit_part_head = prepare_list_head;
        YYSYS_LOG(DEBUG, "read_atomic::debug,means prepare list on the same list");
      }
      else if(NULL != commit_undo_iter)
      {//means commit list comes from undo list!
        if(commit_undo_iter == te_value_->undo_list)
        {
          prepare_list_head = te_value_->list_head;
          prepare_undo_iter = NULL;
          YYSYS_LOG(DEBUG, "read_atomic::debug,prepare list is the newst list head!");
        }
        else
        {
          const ObUndoNode *undo_iter = te_value_->undo_list;
          while(NULL != undo_iter)
          {
            if(commit_undo_iter == undo_iter->next)
            {
              prepare_list_head = undo_iter->head;
              prepare_undo_iter = undo_iter;
              YYSYS_LOG(DEBUG, "read_atomic::debug,prepare list is the undo list!");
              break;
            }
            undo_iter = undo_iter->next;
          }
        }
        trans_ver_node = prepare_list_head;
        prepare_list_commit_part_head = prepare_list_head;
      }
      else if(NULL != te_value_ && NULL != (cur_uc_info = te_value_->cur_uc_info))
      {
        const ObCellInfoNode *cur_uc_list_head = cur_uc_info->uc_list_head;
        if(uc_list_head == cur_uc_list_head)
        {//means the uc_list not change!
          prepare_list_head = uc_list_head;
          trans_ver_node = prepare_list_head;
          prepare_undo_iter = NULL;
          prepare_list_commit_part_head = commit_list_head;
          if(NULL == prepare_list_commit_part_head)
          {
            prepare_list_commit_part_head = prepare_list_head;
          }
          YYSYS_LOG(DEBUG, "read_atomic::debug,prepare list is the uc list head!");
        }
        else if(NULL != uc_list_head)
        {
          const ObCellInfoNode *iter = commit_list_head;
          while(NULL != iter)
          {
            if(iter == uc_list_head)
            {
              prepare_list_head = commit_list_head;
              trans_ver_node = uc_list_head;
              prepare_undo_iter = commit_undo_iter;
              prepare_list_commit_part_head = prepare_list_head;
              YYSYS_LOG(DEBUG, "read_atomic::debug,prepare list is the uc list on the commit list!");
              break;
            }
            iter = iter->next;
          }
        }
        else
        {
          YYSYS_LOG(DEBUG, "read_atomic::debug,didn't get prepare list!");
        }
      }
      else
      {
        prepare_list_head = NULL;
        trans_ver_node = NULL;
        prepare_list_tail_trans_ver = NULL;
        prepare_undo_iter = NULL;
        prepare_list_commit_part_head = NULL;
      }

      //second find the prepare_trans_ver
      if(OB_SUCCESS == ret && NULL != trans_ver_node)
      {
        if(OB_SUCCESS != (ret = common::ObReadAtomicHelper::transver_cast_from_buf(trans_ver_node->trans_version_,
                                                                                   static_cast<int64_t>(sizeof(trans_ver_node->trans_version_)),
                                                                                   trans_ver)))
        {
          YYSYS_LOG(WARN, "fail to cast buf to trans ver!ret=%d", ret);
        }
        else if(NULL == trans_ver)
        {
          ret = OB_ERR_UNEXPECTED;
          YYSYS_LOG(ERROR, "trans ver can't be NULL!ret=%d", ret);
        }
        else
        {
          prepare_list_tail_trans_ver = trans_ver;
        }
      }

      if(OB_SUCCESS == ret && NULL != prepare_list_head)
      {
        if(NULL == prepare_list_tail_trans_ver || NULL == prepare_list_commit_part_head)
        {
          ret = OB_ERR_UNEXPECTED;
          YYSYS_LOG(ERROR, "should not be NULL!ret=%d,prepare_list_tail_trans_ver=%p,prepare_list_commit_part_head=%p",
                    ret, prepare_list_tail_trans_ver, prepare_list_commit_part_head);
        }
        else
        {
          YYSYS_LOG(DEBUG, "read_atomic::debug,final prepare_list_tail_trans_ver=[%s],"
            "is_valid=%d,is_dist_trans=%d", to_cstring(*prepare_list_tail_trans_ver),
                    prepare_list_tail_trans_ver->is_valid(), prepare_list_tail_trans_ver->is_distributed_trans());
        }
      }

      if(NULL == prepare_list_head || NULL == prepare_list_tail_trans_ver || NULL == prepare_list_commit_part_head ||
         !prepare_list_tail_trans_ver->is_valid() ||
         !prepare_list_tail_trans_ver->is_distributed_trans())//don't output invalid transver or single point transver prepare list!
      {
        prepare_list_head = NULL;
        prepare_list_tail_trans_ver = NULL;
        prepare_undo_iter = NULL;
        prepare_list_commit_part_head = NULL;
      }

      YYSYS_LOG(DEBUG, "read_atomic::debug,finish get prepare list!ret=%d,"
        "prepare_list_head=%p,prepare_list_tail_trans_ver=%p,"
        "prepare_undo_iter=%p,prepare_list_commit_part_head=%p", ret, prepare_list_head, prepare_list_tail_trans_ver,
                prepare_undo_iter, prepare_list_commit_part_head);
      return ret;
    }


    /*@berif ���ֶ��㷨�ĵ�һ�ֶȵ�ʱ�� �������µļ��� version  ��output_commit_list_transverset_ ��output_undo_list_transverset_
    * @param  commit_list_head[in]  TeValue ���ύ���ж�ͷ
    * @param  commit_list_end[in]  TeValue ���ύ���ж�β
    * @param  commit_undo_iter[in]  UNDO ����*/
    int MemTableGetIter::store_prev_commit_verset_(const ObCellInfoNode *commit_list_head,
                                                   const ObCellInfoNode *commit_list_end,
                                                   const ObUndoNode *commit_undo_iter,
                                                   const bool will_output_commit_data)
    {
      int ret = OB_SUCCESS;
      common::ObTransVersion *pre_ver = NULL;
      common::ObTransVersion *cur_ver = NULL;
      int64_t cur_transver_num = 0;
      int64_t max_transver_num = read_atomic_param_.max_prevcommit_trans_verset_count_;
      if(!read_atomic_param_is_valid_)
      {
        ret = OB_NOT_INIT;
        YYSYS_LOG(ERROR, "param must be valid!ret=%d,param=[%s]", ret, to_cstring(read_atomic_param_));
      }
      else
      {
        const ObUndoNode *undo_iter = NULL;
        const ObCellInfoNode *iter = NULL;
        output_commit_list_transverset_.clear();
        output_undo_list_transverset_.clear();

        //lookup commit ver in commit list
        //if will output commit data,
        //the prev commit trans ver will stored when output commit data!!!!
        if(!will_output_commit_data)
        {
          iter = commit_list_head;
          while(OB_SUCCESS == ret
                //&& cur_transver_num < max_transver_num
                && NULL != iter && commit_list_end != iter && !trans_end_(iter))
          {
            cur_ver = NULL;
            if(OB_SUCCESS != (ret = common::ObReadAtomicHelper::transver_cast_from_buf(iter->trans_version_,
                                                                                       static_cast<int64_t>(sizeof(iter->trans_version_)),
                                                                                       cur_ver)))
            {
              YYSYS_LOG(WARN, "fail to cast buf to transver!ret=%d", ret);
            }
            else if(NULL != cur_ver && cur_ver->is_valid() && cur_ver->is_distributed_trans())
            {
              if(NULL != pre_ver && *cur_ver == *pre_ver)
              {
                //just continue
                YYSYS_LOG(DEBUG, "read_atomic::debug,cur_ver has been stord!ver=[%s]", to_cstring(*cur_ver));
              }
              else if(OB_SUCCESS != (ret = store_trans_ver(cur_ver, output_commit_list_transverset_, true)))
              {
                YYSYS_LOG(WARN, "fail to store commit list transver!ret=%d", ret);
              }
              else
              {
                cur_transver_num++;
                pre_ver = cur_ver;
              }
            }
            iter = iter->next;
          }
        }

        //lookup undo list if not filled
        if(OB_SUCCESS == ret && cur_transver_num < max_transver_num)
        {
          if(NULL == commit_undo_iter && NULL != te_value_)
          {//means commit list come from last commit list,need lookup undo list
            undo_iter = te_value_->undo_list;
          }
          else if(NULL != commit_undo_iter)
          {
            undo_iter = commit_undo_iter->next;
          }

          while(OB_SUCCESS == ret && NULL != undo_iter && cur_transver_num < max_transver_num)
          {
            iter = undo_iter->head;
            while(OB_SUCCESS == ret && NULL != iter && !trans_end_(iter)
              //&& cur_transver_num < max_transver_num
              )
            {
              cur_ver = NULL;
              if(OB_SUCCESS != (ret = common::ObReadAtomicHelper::transver_cast_from_buf(iter->trans_version_,
                                                                                         static_cast<int64_t>(sizeof(iter->trans_version_)),
                                                                                         cur_ver)))
              {
                YYSYS_LOG(WARN, "fail to cast buf to transver!ret=%d", ret);
              }
              else if(NULL != cur_ver && cur_ver->is_valid() && cur_ver->is_distributed_trans())
              {
                if(NULL != pre_ver && *cur_ver == *pre_ver)
                {
                  //just continue
                  YYSYS_LOG(DEBUG, "read_atomic::debug,cur_ver has been stord!ver=[%s]", to_cstring(*cur_ver));
                }
                else if(OB_SUCCESS != (ret = store_trans_ver(cur_ver, output_undo_list_transverset_, true)))
                {
                  YYSYS_LOG(WARN, "fail to store undo list transver!ret=%d", ret);
                }
                else
                {
                  cur_transver_num++;
                  pre_ver = cur_ver;
                }
              }
              iter = iter->next;
            }
            undo_iter = undo_iter->next;
          }
        }
      }

      YYSYS_LOG(DEBUG, "read_atomic::debug,finish store prev commit transver!"
        "ret=%d,commit_list_tranver_num=%ld,undo_list_transver_num=%ld,"
        "will_output_commit_data=%d", ret, output_commit_list_transverset_.count(),
                output_undo_list_transverset_.count(), will_output_commit_data);

      return ret;
    }


    int MemTableGetIter::store_trans_ver(const common::ObTransVersion *trans_ver,
                                         ObArray<const common::ObTransVersion *> &array, const bool allow_repeat)
    {
      int ret = OB_SUCCESS;
      int64_t total_num = array.count();
      bool is_repeat = false;
      if(NULL == trans_ver || !trans_ver->is_valid())
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(ERROR, "invalid argument!transver=[%p],ret=%d", trans_ver, ret);
      }
      else
      {
        if(!allow_repeat)
        {
          for(int64_t i = 0; OB_SUCCESS == ret && i < total_num; i++)
          {
            if(*trans_ver == *(array.at(i)))
            {
              is_repeat = true;
              YYSYS_LOG(INFO, "read_atomic::debug,cur transver is repeat!ver=[%s]", to_cstring(*trans_ver));
              break;
            }
          }
        }

        if(!is_repeat && OB_SUCCESS != (ret = array.push_back(trans_ver)))
        {
          YYSYS_LOG(WARN, "fail to store transver[%s],ret=%d", to_cstring(*trans_ver), ret);
        }
      }
      return ret;
    }

    /*@berif  ���� list_head  �����������飨node_list_array_����*/
    int MemTableGetIter::add_iter_list_(const ObCellInfoNode *list_head,
                                        const common::ObTransVersion *list_tail_transver, const ReadDataState state)
    {
      int ret = OB_SUCCESS;
      if(NULL == list_head)
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(ERROR, "list head can't be NULL!ret=%d", ret);
      }
      else
      {
        NodeList node_list;
        node_list.list_head_ = list_head;
        node_list.list_tail_trans_ver_ = list_tail_transver;
        node_list.read_data_state_ = state;
        node_list.list_iter_state_ = ITER_NOT_BEGIN;

        if(OB_SUCCESS != (ret = node_list_array_.push_back(node_list)))
        {
          YYSYS_LOG(WARN, "fail to store node list!"
            "ret=%d,list_head=%p,list_tail_transver=%p,state=%d", ret, node_list.list_head_,
                    node_list.list_tail_trans_ver_, node_list.read_data_state_);
        }
      }

      if(read_atomic_param_is_valid_)
        YYSYS_LOG(DEBUG, "read_atomic::debug,finish add iter list!"
          "tid=%ld,next_head_node_idx_=%ld,"
          "total_iter_list_count_=%ld,"
          "list_head=%p,list_tail_transver=%p,state=%d,ret=%d", te_key_.table_id, next_head_node_idx_,
                  node_list_array_.count(), list_head, list_tail_transver, state, ret);
      return ret;
    }

    int MemTableGetIter::delete_iter_list(const ReadDataState state)
    {
      int ret = OB_SUCCESS;
      for(int64_t i = 0; OB_SUCCESS == ret && i < node_list_array_.count(); i++)
      {
        NodeList &old_node_list = node_list_array_.at(i);
        if(state == old_node_list.read_data_state_)
        {
          if(ITER_NOT_BEGIN < old_node_list.list_iter_state_)
          {
            ret = OB_ERROR;
            YYSYS_LOG(ERROR, "old node has been itered end!can't delete it!"
              "ret=%d,old_list_hand=%p,old_list_tail_trans_ver=%p,"
              "old_list_read_state=%d,old_list_iter_state=%d", ret, old_node_list.list_head_,
                      old_node_list.list_tail_trans_ver_, old_node_list.read_data_state_,
                      old_node_list.list_iter_state_);
            break;
          }
          old_node_list.list_iter_state_ = ITER_INVALID_STATE;
        }
      }
      return ret;
    }

    int MemTableGetIter::update_prepare_list_and_transver(const ObCellInfoNode *commit_list_head,
                                                          const ObCellInfoNode *commit_list_end,
                                                          const ObCellInfoNode *uc_list_head,
                                                          const ObUndoNode *commit_undo_iter,
                                                          const bool over_write_prepare_list,
                                                          bool *is_finish_add_prepare_list)
    {
      int ret = OB_SUCCESS;
      const ObCellInfoNode *prepare_list_head = NULL;
      const common::ObTransVersion *prepare_list_tail_trans_ver = NULL;
      const ObUndoNode *prepare_undo_iter = NULL;
      const ObCellInfoNode *prepare_list_commit_part_head = NULL;
      if(NULL != is_finish_add_prepare_list)
      {
        *is_finish_add_prepare_list = false;
      }
      if(!read_atomic_param_is_valid_)
      {
        ret = OB_NOT_INIT;
        YYSYS_LOG(ERROR, "must has valid param!param=[%s],ret=%d", to_cstring(read_atomic_param_), ret);
      }
      else if(!read_atomic_param_.need_prepare_data_ && !read_atomic_param_.need_last_prepare_trans_ver_)
      {
        //do nothing
      }
      else
      {
        ret = get_prepare_list_(prepare_list_head, prepare_list_tail_trans_ver, prepare_undo_iter,
                                prepare_list_commit_part_head, commit_list_head, commit_list_end, uc_list_head,
                                commit_undo_iter);
        if(OB_SUCCESS != ret)
        {
          YYSYS_LOG(WARN, "fail to get prepare list!ret=%d", ret);
        }
        else if(NULL != prepare_list_head)
        {
          if(NULL == prepare_list_commit_part_head || NULL == prepare_list_tail_trans_ver ||
             !prepare_list_tail_trans_ver->is_valid())
          {
            ret = OB_ERR_UNEXPECTED;
            YYSYS_LOG(ERROR, "get invalid prepare list!ret=%d,prepare_list_commit_part_head=%p,"
              "prepare_list_tail_trans_ver=%p", ret, prepare_list_commit_part_head, prepare_list_tail_trans_ver);
          }
          else if(read_atomic_param_.need_prepare_data_ && over_write_prepare_list &&
                  OB_SUCCESS != (ret = delete_iter_list(READING_PREPARE_DATA_COMMIT_PART)))
          {
            YYSYS_LOG(WARN, "fail to delete the prepare commi part data!ret=%d", ret);
          }
          else if(read_atomic_param_.need_prepare_data_ && over_write_prepare_list &&
                  OB_SUCCESS != (ret = delete_iter_list(READING_PREPARE_DATA)))
          {
            YYSYS_LOG(WARN, "fail to delete the prepare data!ret=%d", ret);
          }
          else if(read_atomic_param_.need_prepare_data_ && prepare_list_commit_part_head != prepare_list_head &&
                  OB_SUCCESS != (ret = add_iter_list_(prepare_list_commit_part_head, prepare_list_tail_trans_ver,
                                                      READING_PREPARE_DATA_COMMIT_PART)))
          {
            YYSYS_LOG(WARN, "fail to add commit part list of prepare list!ret=%d", ret);
          }
          else if(read_atomic_param_.need_prepare_data_ && OB_SUCCESS != (ret = add_iter_list_(prepare_list_head,
                                                                                               prepare_list_tail_trans_ver,
                                                                                               READING_PREPARE_DATA)))
          {
            YYSYS_LOG(WARN, "fail to add prepare list!ret=%d", ret);
          }
          else if(read_atomic_param_.need_last_prepare_trans_ver_)
          {
            output_last_prepare_trans_ver_ = prepare_list_tail_trans_ver;
            YYSYS_LOG(DEBUG, "read_atomic::debug,succ get last prepare trans ver!last_prepare_trans_ver=%p",
                      output_last_prepare_trans_ver_);
            if(NULL != output_last_prepare_trans_ver_)
              YYSYS_LOG(DEBUG, "read_atomic::debug,trans_ver=[%s]", to_cstring(*output_last_prepare_trans_ver_));
          }

          if(OB_SUCCESS == ret && NULL != is_finish_add_prepare_list && NULL != prepare_list_head &&
             read_atomic_param_.need_prepare_data_)
          {
            *is_finish_add_prepare_list = true;
          }
        }
      }
      return ret;
    }

    int MemTableGetIter::update_last_commit_and_prevcommit_transver(const ObCellInfoNode *commit_list_node)
    {
      int ret = OB_SUCCESS;
      common::ObTransVersion *trans_ver = NULL;
      if(!read_atomic_param_is_valid_)
      {
        ret = OB_NOT_INIT;
        YYSYS_LOG(ERROR, "must has valid param!ret=%d,param=[%s]", ret, to_cstring(read_atomic_param_));
      }
      else if(NULL == commit_list_node)
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(ERROR, "commit_list_node is NULL!ret=%d", ret);
      }
      else if(read_atomic_param_.need_last_commit_trans_ver_ || read_atomic_param_.need_prevcommit_trans_verset_)
      {
        if(OB_SUCCESS != (ret = common::ObReadAtomicHelper::transver_cast_from_buf(commit_list_node->trans_version_,
                                                                                   sizeof(commit_list_node->trans_version_),
                                                                                   trans_ver)))
        {
          YYSYS_LOG(WARN, "fail to get cur node iter transver!ret=%d", ret);
        }
        else if(NULL == trans_ver)
        {
          ret = OB_ERR_UNEXPECTED;
          YYSYS_LOG(ERROR, "commit trans ver is NULL!ret=%d", ret);
        }
        else if(!trans_ver->is_valid())
        {
          ret = OB_ERR_UNEXPECTED;
          YYSYS_LOG(ERROR, "commit trans ver must valid!ret=%d,ver=[%s]", ret, to_cstring(*trans_ver));
        }
        else if(read_atomic_param_.need_prevcommit_trans_verset_ && trans_ver->is_distributed_trans() &&
                OB_SUCCESS != (ret = store_trans_ver(trans_ver, output_commit_list_transverset_, true)))
        {
          YYSYS_LOG(WARN, "fail to store prevcommit trans ver!ret=%d", ret);
        }
        else if(read_atomic_param_.need_last_commit_trans_ver_)
        {
          output_last_commit_trans_ver_ = trans_ver;
        }
      }
      return ret;
    }

    /*nodeʱ���  ���� ��ǰ����� ��������*/
    bool MemTableGetIter::trans_end_(const ObCellInfoNode *node_iter)
    {
      bool bret = false;
      if(NULL != node_iter && !read_uncommited_data_())
      {
        if(NULL != session_ctx_ && node_iter->modify_time > session_ctx_->get_trans_id())
        {
          YYSYS_LOG(DEBUG, "trans_end v=%ld trans_id=%ld", node_iter->modify_time, session_ctx_->get_trans_id());
          bret = true;
        }
      }
      return bret;
    }

    bool MemTableGetIter::has_remainder_real_data_need_output() const
    {
      bool bret = false;
      int64_t start = (next_head_node_idx_ >= 0) ? next_head_node_idx_ : 0;
      int64_t total_iter_list_count = node_list_array_.count();
      for(int64_t i = start; i < total_iter_list_count; i++)
      {
        const NodeList &node_list = node_list_array_.at(i);
        if(NULL != node_list.list_head_ && ITER_NOT_BEGIN == node_list.list_iter_state_)
        {
          bret = true;
          break;
        }
      }
      return bret;
    }


    /*
    * @berif  ��ȡ��һ�������� ����ͷ
    * @return  ����ͷ
    * */
    const ObCellInfoNode *MemTableGetIter::get_read_atomic_next_list_head_()
    {
      const ObCellInfoNode *ret = NULL;
      int64_t total_iter_list_count = node_list_array_.count();
      while(next_head_node_idx_ < total_iter_list_count)
      {
        NodeList &node_list = node_list_array_.at(next_head_node_idx_);
        if(NULL != node_list.list_head_ && ITER_NOT_BEGIN == node_list.list_iter_state_)
        {
          ret = node_list.list_head_;
          cur_iter_read_state_ = node_list.read_data_state_;
          node_list.list_iter_state_ = ITER_NORMAL_NODE;
          next_head_node_idx_++;
          break;
        }
        next_head_node_idx_++;
      }

      if(NULL == ret)
      {
        cur_iter_read_state_ = READ_NONE;
      }

      if(read_atomic_param_is_valid_)
        YYSYS_LOG(DEBUG, "read_atomic::debug,finish get next list head!"
          "tid=%ld,next_head_node_idx_=%ld,total_iter_list_count_=%ld,list_head=%p,cur_iter_read_state_=%d",
                  te_key_.table_id, next_head_node_idx_, node_list_array_.count(), ret, cur_iter_read_state_);
      return ret;
    }

    /*��ȡ��ǰ����������*/
    MemTableGetIter::NodeList *MemTableGetIter::get_read_atomic_cur_node_list()
    {
      NodeList *ret = NULL;
      if(node_list_array_.count() <= 0 || next_head_node_idx_ <= 0)
      {
        ret = NULL;
      }
      else if((next_head_node_idx_ - 1) < node_list_array_.count())
      {
        ret = &(node_list_array_.at(next_head_node_idx_ - 1));
      }
      return ret;
    }

    /*@berif ������ ������ prepare �� commit ��undo_list �� ��ȡһ��node*/
    int MemTableGetIter::get_read_atomic_next_node_iter_()
    {
      int ret = OB_SUCCESS;

      YYSYS_LOG(DEBUG, "read_atomic::debug,begin get next node!old_node_iter=%p", node_iter_);

      NodeList *cur_node_list = get_read_atomic_cur_node_list();/*��õ�ǰ����������*/
      if(!read_atomic_param_is_valid_)
      {
        ret = OB_NOT_INIT;
        YYSYS_LOG(ERROR, "param should be valid!param=[%s],ret=%d", to_cstring(read_atomic_param_), ret);
      }
      else if(NULL == node_iter_)/*ȫ�������Ѿ�������ϣ�����״̬��ΪITER_END �� ����OB_ITER_END*/
      {
        if(NULL != cur_node_list)
        {
          cur_node_list->list_iter_state_ = ITER_END;
        }
        ret = OB_ITER_END;
      }
      else if(NULL == cur_node_list)
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(ERROR, "cur node list can't be NULL!node_iter_=%p,"
          "next_head_node_idx_=%ld,total_list_num=%ld,ret=%d", node_iter_, next_head_node_idx_,
                  node_list_array_.count(), ret);
      }
      else if(ITER_END == cur_node_list->list_iter_state_)/*����������ϣ�������һ����������������ͷ*/
      {
        node_iter_ = get_read_atomic_next_list_head_();
        YYSYS_LOG(DEBUG, "======================= one list is end================");
      }
      else if(READING_COMMIT_DATA == cur_iter_read_state_ &&
              (read_atomic_param_.need_last_commit_trans_ver_ || read_atomic_param_.need_prevcommit_trans_verset_) &&
              OB_SUCCESS != (ret = update_last_commit_and_prevcommit_transver(node_iter_)))
      {
        YYSYS_LOG(WARN, "fail to update commit transver!ret=%d", ret);
      }
      else if(NULL == (node_iter_ = node_iter_->next))/*�ƶ�ָ�뵽��һ���ڵ�,�����һ���ڵ���NULL ��Ҫ�����һ������*/
      {
        node_iter_ = get_read_atomic_next_list_head_();
        YYSYS_LOG(DEBUG, "===================== one list is end =====================");
        YYSYS_LOG(DEBUG, "\n\n========================================================\n\n");

      }

      while(NULL != node_iter_ && OB_SUCCESS == ret)
      {
        cur_node_list = get_read_atomic_cur_node_list();
        if(NULL == cur_node_list)
        {
          ret = OB_ERR_UNEXPECTED;
          YYSYS_LOG(ERROR, "node list can't be NULL!ret=%d", ret);
        }
        else if(NULL == cur_node_list->list_tail_trans_ver_)
        {
          YYSYS_LOG(DEBUG, "NULL == cur_node_list->list_tail_trans_ver_");
        }
        else
        {//means must use tail trans ver to filter node iter!!!!
          common::ObTransVersion *trans_ver = NULL;
          if(OB_SUCCESS != (ret = common::ObReadAtomicHelper::transver_cast_from_buf(node_iter_->trans_version_,
                                                                                     static_cast<int64_t>(sizeof(node_iter_->trans_version_)),
                                                                                     trans_ver)))
          {
            YYSYS_LOG(WARN, "fail to get cur node iter's transver!ret=%d", ret);
          }
          else if(NULL == trans_ver)
          {
            ret = OB_ERR_UNEXPECTED;
            YYSYS_LOG(ERROR, "cur node iter trans ver is NULL!ret=%d", ret);
          }
            /*trans_ver һ��     ITER_TAIL_NODE��ʾ  �����Ǹ����������һ��Ԫ�ء�*/
          else if(ITER_NORMAL_NODE == cur_node_list->list_iter_state_ &&
                  *trans_ver == *(cur_node_list->list_tail_trans_ver_))
          {//means begin iter the tail node
            cur_node_list->list_iter_state_ = ITER_TAIL_NODE;
            YYSYS_LOG(DEBUG,
                      "================node_trans_ver:%s,   cur_node_list_trans_ver:%s   first cellinfo same ====================",
                      to_cstring(*trans_ver), to_cstring(*(cur_node_list->list_tail_trans_ver_)));
          }
            /*trans_ver �� ����tail_trans_ver��һ������ʾ�������Ѿ�����*/
          else if(ITER_TAIL_NODE == cur_node_list->list_iter_state_ &&
                  *trans_ver != *(cur_node_list->list_tail_trans_ver_))
          {//means finish iter the tail node
            cur_node_list->list_iter_state_ = ITER_END;
            node_iter_ = get_read_atomic_next_list_head_();
            YYSYS_LOG(DEBUG,
                      "===============node_trans_ver:%s,   cur_node_list_trans_ver:%s,is different, one list is end ==============",
                      to_cstring(*trans_ver), to_cstring(*(cur_node_list->list_tail_trans_ver_)));
            continue;
          }
          else if(ITER_TAIL_NODE == cur_node_list->list_iter_state_)
          {
            YYSYS_LOG(DEBUG,
                      "================node_trans_ver:%s,   cur_node_list_trans_ver:%s  is *** NOT **** first  cellinfo same============",
                      to_cstring(*trans_ver), to_cstring(*(cur_node_list->list_tail_trans_ver_)));
          }

          {
            YYSYS_LOG(DEBUG, "\n\n\n+++++++++++++++++++++++++++++++++++++++++++++++\n");
            YYSYS_LOG(DEBUG, "cur_node_trans_ver:%s,   cur_node_list_trans_ver:%s,cur_node_list->list_iter_state_:%d",
                      to_cstring(*trans_ver), to_cstring(*(cur_node_list->list_tail_trans_ver_)),
                      cur_node_list->list_iter_state_);
            YYSYS_LOG(DEBUG, "\n+++++++++++++++++++++++++++++++++++++++++++++++++++\n\n\n");
          }
        }
        break;
      }

      if(OB_SUCCESS == ret && NULL == node_iter_)
      {
        ret = OB_ITER_END;
      }

      YYSYS_LOG(DEBUG, "read_atomic::debug,finish get next_node_iter=%p,next_head_node_idx_=%ld,"
        "cur_node_list_iter_state=%d,ret=%d", node_iter_, next_head_node_idx_,
                ((NULL == cur_node_list) ? -100 : cur_node_list->list_iter_state_), ret);

      return ret;
    }

    int MemTableGetIter::next_rowkey_cell_(bool &is_last_rowkey_cell)
    {
      int ret = OB_SUCCESS;
      is_last_rowkey_cell = false;
      const ObRowkeyInfo *rki = cell_iter_.get_rowkey_info(te_key_.table_id);
      if(NULL == rki || NULL == te_key_.row_key.ptr())
      {
        ret = OB_SCHEMA_ERROR;
        YYSYS_LOG(WARN, "get rowkey info fail!ret=%d table_id=%lu rowkey_info=%p rowkey=%p %s", ret, te_key_.table_id,
                  rki, te_key_.row_key.ptr(), to_cstring(te_key_.row_key));
      }
      else if(rowkey_cell_idx_ >= te_key_.row_key.length())
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(WARN, "rowkey has iter end!rowkey_cell_idx_=%ld,rowkey_length=%ld,ret=%d", rowkey_cell_idx_,
                  te_key_.row_key.length(), ret);
      }
      else if(OB_SUCCESS != (ret = rki->get_column_id(rowkey_cell_idx_, ci_.column_id_)))
      {
        YYSYS_LOG(WARN, "fail to get rowkey cid!rowkey_cell_idx_=%ld,cid=%ld,ret=%d", rowkey_cell_idx_, ci_.column_id_,
                  ret);
      }
      else
      {
        ci_.value_ = te_key_.row_key.ptr()[rowkey_cell_idx_];

        if(rowkey_cell_idx_ == (te_key_.row_key.length() - 1))
        {
          is_last_rowkey_cell = true;
        }
        //iter_counter_++;
        rowkey_cell_idx_++;
      }

      YYSYS_LOG(DEBUG, "read_atomic::debug,get one rowkey cell!"
        "ret=%d,val=[%s],cid=[%ld],is_last_rowkey_cell=%d,"
        "iter_counter_=%ld,rowkey_cell_idx_=%ld", ret, to_cstring(ci_.value_), ci_.column_id_, is_last_rowkey_cell,
                iter_counter_, rowkey_cell_idx_);

      return ret;
    }

    /*���Ԫ����*/
    int MemTableGetIter::extend_iter_next_cell_()
    {//it's ugly!need optimize!!!!!
      int ret = OB_SUCCESS;
      bool is_last_rowkey_cell = false;
      bool has_get_cell = false;
      if(is_extend_iter_end_ || !read_atomic_param_is_valid_)
      {
        ret = OB_ITER_END;
      }

      //don't modify the output order!!!!!!

      //output rowkey cell
      if(OB_SUCCESS == ret && !has_get_cell && read_atomic_param_.need_row_key_ && need_return_rowkey_indep_)
      {
        ret = next_rowkey_cell_(is_last_rowkey_cell);
        if(OB_SUCCESS != ret)
        {
          YYSYS_LOG(WARN, "fail to get next rowkey cell!ret=%d", ret);
        }
        else if(is_last_rowkey_cell)
        {//finish return all rowkey!
          need_return_rowkey_indep_ = false;
        }
        if(OB_SUCCESS == ret)
        {
          has_get_cell = true;
        }
      }

      //output the commit extend type
      if(OB_SUCCESS == ret && !has_get_cell &&
         (read_atomic_param_.need_commit_data_ || read_atomic_param_.need_exact_data_mark_data_ ||
          read_atomic_param_.need_exact_transver_data_) && output_commit_ext_type_ != OB_INVALID_DATA)
      {
        if(ObActionFlag::OP_ROW_DOES_NOT_EXIST == output_commit_ext_type_ &&
           read_atomic_param_.need_last_commit_trans_ver_ && NULL != output_last_commit_trans_ver_ &&
           output_last_commit_trans_ver_->is_valid())
        {
          ret = OB_ERR_UNEXPECTED;
          YYSYS_LOG(ERROR, "shouldn't get valid commit transver,but didn't get prepare data!"
            "ret=%d,commit_transver=[%s]", ret, to_cstring(*output_last_commit_trans_ver_));
        }
        else
        {
          ci_.column_id_ = read_atomic_param_.commit_data_extendtype_cid_;
          ci_.value_.set_int(output_commit_ext_type_);
          output_commit_ext_type_ = OB_INVALID_DATA;
          has_get_cell = true;
          cur_iter_read_state_ = (READING_COMMIT_EXT_TYPE != cur_iter_read_state_) ? READING_COMMIT_EXT_TYPE
                                                                                   : cur_iter_read_state_;
        }
      }

      //output the prepare extend type
      if(OB_SUCCESS == ret && !has_get_cell && read_atomic_param_.need_prepare_data_ &&
         output_prepare_ext_type_ != OB_INVALID_DATA)
      {
        if(ObActionFlag::OP_ROW_DOES_NOT_EXIST == output_prepare_ext_type_ &&
           read_atomic_param_.need_last_prepare_trans_ver_ && NULL != output_last_prepare_trans_ver_ &&
           output_last_prepare_trans_ver_->is_valid())
        {
          ret = OB_ERR_UNEXPECTED;
          YYSYS_LOG(ERROR, "shouldn't get valid preapre transver,but didn't get prepare data!"
            "ret=%d,prepare_transver=[%s]", ret, to_cstring(*output_last_prepare_trans_ver_));
        }
        else
        {

          ci_.column_id_ = read_atomic_param_.prepare_data_extendtype_cid_;
          ci_.value_.set_int(output_prepare_ext_type_);
          output_prepare_ext_type_ = OB_INVALID_DATA;
          has_get_cell = true;
          cur_iter_read_state_ = (READING_PREPARE_EXT_TYPE != cur_iter_read_state_) ? READING_PREPARE_EXT_TYPE
                                                                                    : cur_iter_read_state_;
        }
      }

      //output last commit trans version
      if(OB_SUCCESS == ret && !has_get_cell && read_atomic_param_.need_last_commit_trans_ver_ &&
         NULL != output_last_commit_trans_ver_)
      {
        ObString tmp_val;
        char *des_buf = NULL;
        int64_t buf_len = 0;
        if(OB_SUCCESS !=
           (ret = common::ObReadAtomicHelper::transver_cast_to_buf(output_last_commit_trans_ver_, des_buf, buf_len)))
        {
          YYSYS_LOG(WARN, "fail to cast ObTransVersion ptr to char ptr!ret=%d", ret);
        }
        else
        {
          YYSYS_LOG(DEBUG,
                    "read_atomic::debug,output last commit trans ver!trans_ver=[%s],cid=%ld,last_commit_trans_ver_=%p,des_buf=%p",
                    to_cstring(*output_last_commit_trans_ver_), ci_.column_id_, output_last_commit_trans_ver_, des_buf);
          tmp_val.assign(des_buf, static_cast<ObString::obstr_size_t>(buf_len));
          ci_.column_id_ = read_atomic_param_.last_commit_trans_ver_cid_;
          ci_.value_.set_varchar(tmp_val);
          has_get_cell = true;
          output_last_commit_trans_ver_ = NULL;
          cur_iter_read_state_ = (READING_LAST_COMMIT_TRANS_VER != cur_iter_read_state_) ? READING_LAST_COMMIT_TRANS_VER
                                                                                         : cur_iter_read_state_;
        }

      }

      //output last prepare trans version
      if(OB_SUCCESS == ret && !has_get_cell && read_atomic_param_.need_last_prepare_trans_ver_ &&
         NULL != output_last_prepare_trans_ver_)
      {
        ObString tmp_val;
        char *des_buf = NULL;
        int64_t buf_len = 0;
        if(OB_SUCCESS !=
           (ret = common::ObReadAtomicHelper::transver_cast_to_buf(output_last_prepare_trans_ver_, des_buf, buf_len)))
        {
          YYSYS_LOG(WARN, "fail to cast ObTransVersion ptr to char ptr!ret=%d", ret);
        }
        else
        {
          YYSYS_LOG(DEBUG,
                    "read_atomic::debug,output last prepare trans ver!trans_ver=[%s],cid=%ld,last_prepare_trans_ver_=%p,des_buf=%p",
                    to_cstring(*output_last_prepare_trans_ver_), ci_.column_id_, output_last_prepare_trans_ver_,
                    des_buf);
          tmp_val.assign(des_buf, static_cast<ObString::obstr_size_t>(buf_len));
          ci_.column_id_ = read_atomic_param_.last_prepare_trans_ver_cid_;
          ci_.value_.set_varchar(tmp_val);
          has_get_cell = true;
          output_last_prepare_trans_ver_ = NULL;
          cur_iter_read_state_ = (READING_LAST_PREPARE_TRANS_VER != cur_iter_read_state_)
                                 ? READING_LAST_PREPARE_TRANS_VER : cur_iter_read_state_;
        }
      }

      //output prevcommit trans version
      if(OB_SUCCESS == ret && !has_get_cell && read_atomic_param_.need_prevcommit_trans_verset_)
      {
        int64_t commit_list_transver_num = output_commit_list_transverset_.count();
        int64_t undo_list_transver_num = output_undo_list_transverset_.count();
        int64_t cur_verset_idx = cur_commit_list_transver_idx_ + cur_undo_list_transver_idx_;
        int64_t total_verset_count = commit_list_transver_num + undo_list_transver_num;
        int64_t max_verset_num = static_cast<int64_t>(read_atomic_param_.max_prevcommit_trans_verset_count_);
        while(OB_SUCCESS == ret && !has_get_cell && cur_verset_idx < total_verset_count &&
              cur_verset_idx < max_verset_num)
        {
          ObString tmp_val;
          const common::ObTransVersion *trans_ver = NULL;
          char *des_buf = NULL;
          int64_t buf_len = 0;
          uint64_t column_id = prevcommit_trans_verset_cids_[cur_verset_idx];

          //first output trans ver comes from commit list
          if(commit_list_transver_num > 0 && cur_commit_list_transver_idx_ < commit_list_transver_num)
          {//output begin from the end of the array!!!!!!!
            trans_ver = output_commit_list_transverset_.at(
              commit_list_transver_num - 1 - cur_commit_list_transver_idx_);
            cur_commit_list_transver_idx_++;
          }
          else if(undo_list_transver_num > 0 && cur_undo_list_transver_idx_ < undo_list_transver_num)
          {
            trans_ver = output_undo_list_transverset_.at(undo_list_transver_num - 1 - cur_undo_list_transver_idx_);
            cur_undo_list_transver_idx_++;
          }
          cur_verset_idx = cur_commit_list_transver_idx_ + cur_undo_list_transver_idx_;


          YYSYS_LOG(DEBUG,
                    "read_atomic::debug,prepare output %ldth prevcommit trans ver!trans_ver=%p,cid=%ld,total_verset_count_=%ld",
                    cur_verset_idx, trans_ver, column_id, total_verset_count);
          if(NULL != trans_ver)
            YYSYS_LOG(DEBUG, "read_atomic::debug,trans_ver=[%s]", to_cstring(*trans_ver));

          if(NULL == trans_ver)
          {
            continue;
          }
          else if(OB_SUCCESS != (ret = common::ObReadAtomicHelper::transver_cast_to_buf(trans_ver, des_buf, buf_len)))
          {
            YYSYS_LOG(WARN, "fail to cast ObTransVersion ptr to char ptr!ret=%d", ret);
            break;
          }
          else
          {
            tmp_val.assign(des_buf, static_cast<ObString::obstr_size_t>(buf_len));
            ci_.column_id_ = column_id;
            ci_.value_.set_varchar(tmp_val);
            has_get_cell = true;
            cur_iter_read_state_ = (READING_COMMIT_VERSET != cur_iter_read_state_) ? READING_COMMIT_VERSET
                                                                                   : cur_iter_read_state_;
            YYSYS_LOG(DEBUG, "read_atomic::debug,succ output %ldth prevcommit trans ver!"
              "trans_ver=[%s],cid=%ld,trans_ver=%p,des_buf=%p", cur_verset_idx, to_cstring(*trans_ver), ci_.column_id_,
                      trans_ver, des_buf);
            break;
          }
        }
      }

      //output data mark
      if(OB_SUCCESS == ret && !has_get_cell && read_atomic_param_.need_data_mark_)
      {
        if(OB_INVALID_VERSION != output_major_ver_)
        {
          output_major_ver_ = static_cast<int64_t>(data_mark_.major_version_);
          ci_.column_id_ = read_atomic_param_.major_ver_cid_;
          ci_.value_.set_int(output_major_ver_);
          has_get_cell = true;
          output_major_ver_ = OB_INVALID_VERSION;
        }
        else if(OB_INVALID_VERSION != output_minor_ver_start_)
        {
          output_minor_ver_start_ = static_cast<int64_t>(data_mark_.minor_version_start_);
          ci_.column_id_ = read_atomic_param_.minor_ver_start_cid_;
          ci_.value_.set_int(output_minor_ver_start_);
          has_get_cell = true;
          output_minor_ver_start_ = OB_INVALID_VERSION;
        }
        else if(OB_INVALID_VERSION != output_minor_ver_end_)
        {
          output_minor_ver_end_ = static_cast<int64_t>(data_mark_.minor_version_end_);
          ci_.column_id_ = read_atomic_param_.minor_ver_end_cid_;
          ci_.value_.set_int(output_minor_ver_end_);
          has_get_cell = true;
          output_minor_ver_end_ = OB_INVALID_VERSION;
        }
        else if(OB_INVALID_PAXOS_ID != output_paxos_id_)
        {
          output_paxos_id_ = static_cast<int64_t>(data_mark_.ups_paxos_id_);
          ci_.column_id_ = read_atomic_param_.ups_paxos_id_cid_;
          ci_.value_.set_int(output_paxos_id_);
          has_get_cell = true;
          output_paxos_id_ = OB_INVALID_PAXOS_ID;
        }
        else if(ObReadAtomicDataMark::INVALID_STORE_TYPE != output_data_store_type_)
        {
          output_data_store_type_ = static_cast<int64_t>(data_mark_.data_store_type_);
          ci_.column_id_ = read_atomic_param_.data_store_type_cid_;
          ci_.value_.set_int(output_data_store_type_);
          has_get_cell = true;
          output_data_store_type_ = ObReadAtomicDataMark::INVALID_STORE_TYPE;
        }

        if(OB_SUCCESS == ret && has_get_cell)
        {
          cur_iter_read_state_ = (READING_DATA_MARK != cur_iter_read_state_) ? READING_DATA_MARK : cur_iter_read_state_;
        }
      }

      //output meta data
      if(OB_SUCCESS == ret && !has_get_cell && read_atomic_param_.need_trans_meta_data_)
      {
        //TODO:READ_ATOMIC don't suport yet!
        //        cur_iter_read_state_ = (READING_META_DATA != cur_iter_read_state_) ?
        //                                READING_META_DATA : cur_iter_read_state_;
      }

      if(OB_SUCCESS == ret && !has_get_cell)
      {
        ret = OB_ITER_END;
      }

      if(OB_SUCCESS != ret)
      {
        is_extend_iter_end_ = true;
      }
      else
      {
        is_extend_iter_end_ = false;
      }
      return ret;
    }


    //add duyr 20151208:e

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    MemTableScanIter::MemTableScanIter()
      : te_iter_(), table_id_(OB_INVALID_ID), column_filter_(NULL), return_rowkey_column_(true), session_ctx_(NULL),
        is_iter_end_(true), table_value_(NULL), /*add zhaoqiong [Truncate Table]:20160318*/
        get_iter_()
      //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20160121:b
      , data_mark_()
    //add duyr 20160121:e
      ,query_version_(0)
    {
    }

    MemTableScanIter::~MemTableScanIter()
    {
    }

    void MemTableScanIter::reset()
    {
      te_iter_.reset();
      table_id_ = OB_INVALID_ID;
      column_filter_ = NULL;
      return_rowkey_column_ = true;
      session_ctx_ = NULL;
      is_iter_end_ = true;
      get_iter_.reset();
      table_value_ = NULL; /*add zhaoqiong [Truncate Table]:20160318*/
      //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20160121:b
      data_mark_.reset();
      //add duyr 20160121:e
      query_version_ = 0;
    }

    void MemTableScanIter::set_(const uint64_t table_id, ColumnFilter *column_filter, const bool return_rowkey_column,
                                const BaseSessionCtx *session_ctx,
                                TEValue *value/*add zhaoqiong [Truncate Table]:20160318*/
      //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151207:b
      , const ObReadAtomicDataMark *data_mark/* = NULL*/
      //add duyr 20151207:e
      ,int64_t query_version /*0*/
    )
    {
      table_id_ = table_id;
      column_filter_ = column_filter;
      return_rowkey_column_ = return_rowkey_column;
      session_ctx_ = session_ctx;
      is_iter_end_ = false;
      table_value_ = value; /*add zhaoqiong [Truncate Table]:20160318*/
      query_version_ = query_version;
      //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20160121:b
      data_mark_.reset();
      te_iter_.set_is_read_atomic(false);
      if(NULL != column_filter && column_filter->get_read_atomic_param().is_valid())
      {
        te_iter_.set_is_read_atomic(true);
        if(NULL != data_mark)
        {
          data_mark_ = *data_mark;
          YYSYS_LOG(DEBUG, "read_atomic::debug,set mem scan data mark[%s]", to_cstring(*data_mark));
        }
      }
      //add duyr 20160121:e
    }

    void MemTableScanIter::set_(const uint64_t table_id, ColumnFilter *column_filter, const TransNode *trans_node)
    {
      table_id_ = table_id;
      column_filter_ = column_filter;
      UNUSED(trans_node);//trans_node_ = trans_node;
      is_iter_end_ = false;
    }

    TableEngineIterator &MemTableScanIter::get_te_iter_()
    {
      return te_iter_;
    }

    bool MemTableScanIter::is_row_not_exist_(MemTableGetIter &get_iter)
    {
      bool bret = false;
      ObCellInfo *ci = NULL;
      if(OB_SUCCESS == get_iter.get_cell(&ci) && NULL != ci && ObExtendType == ci->value_.get_type() &&
         ObActionFlag::OP_ROW_DOES_NOT_EXIST == ci->value_.get_ext())
      {
        bret = true;
      }
      return bret;
    }

    int MemTableScanIter::next_cell()
    {
      int ret = OB_SUCCESS;
      if(is_iter_end_)
      {
        ret = OB_ITER_END;
      }
      else
      {
        ret = get_iter_.next_cell();
        if(OB_ITER_END == ret)
        {
          while(true)
          {
            ret = te_iter_.next();
            if(OB_SUCCESS != ret || te_iter_.get_key().table_id != table_id_)
            {
              ret = OB_ITER_END;
              is_iter_end_ = true;
              break;
            }
            //get_iter_.reset();
            //mod zhaoqiong [Truncate Table]:20160318:b
            get_iter_.set_(te_iter_.get_key(), te_iter_.get_value(), column_filter_, return_rowkey_column_, session_ctx_, NULL, NULL, table_value_, query_version_);
            //get_iter_.set_(te_iter_.get_key(), te_iter_.get_value(), column_filter_, return_rowkey_column_, session_ctx_);
//            if(table_value_ == NULL)
//            {
//              get_iter_.set_(te_iter_.get_key(), te_iter_.get_value(), column_filter_, return_rowkey_column_,
//                             session_ctx_
//                //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20160121:b
//                , &data_mark_
//                //add duyr 20160121:e
//              );
//            }
//            else
//            {
//              get_iter_.set_(te_iter_.get_key(), table_value_, column_filter_, return_rowkey_column_, session_ctx_);
//            }
            //mod:e
            ret = get_iter_.next_cell();
            if(OB_ITER_END == ret)
            {
              continue;
            }
            if(OB_SUCCESS != ret || !is_row_not_exist_(get_iter_))
            {
              break;
            }
          }
        }
      }
      if(OB_SUCCESS == ret)
      {
        is_iter_end_ = false;
      }
      return ret;
    }

    int MemTableScanIter::get_cell(ObCellInfo **cell_info)
    {
      return get_cell(cell_info, NULL);
    }

    int MemTableScanIter::get_cell(ObCellInfo **cell_info, bool *is_row_changed)
    {
      int ret = OB_SUCCESS;
      if(is_iter_end_)
      {
        ret = OB_ITER_END;
      }
      else
      {
        ret = get_iter_.get_cell(cell_info, is_row_changed);
      }
      return ret;
    }

    //mod peiouya [secondary index opti bug when rollback][403 bugfix]
    int MemTableScanIter::is_index_table(const uint64_t table_id, bool &is_index, bool &is_speci_index, const CommonSchemaManager *frozen_schema)
    {
        int ret = OB_SUCCESS;
        is_index = false;
        is_speci_index = false;
        if(index_table_id == table_id)
        {
            is_index = true;
            is_speci_index = is_specical_index;
        }
        else
        {
            ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
            UpsSchemaMgrGuard sm_guard;
            const CommonSchemaManager *sm = NULL;
            const CommonTableSchema *tm = NULL;

            if(NULL != frozen_schema)
            {
                sm = frozen_schema;
                tm = frozen_schema->get_table_schema(table_id);
            }
            else if(NULL != (sm = ups_main->get_update_server().get_table_mgr().get_schema_mgr().get_schema_mgr(sm_guard)))
            {
                tm = sm->get_table_schema(table_id);
            }
            else
            {
                ret = OB_ERR_UNEXPECTED;
                YYSYS_LOG(WARN,"get table[%lu] schema error, ret=%d", table_id, ret);
            }

            if(OB_SUCCESS == ret)
            {
                if(NULL == tm)
                {
                    ret = OB_ERR_UNEXPECTED;
                    YYSYS_LOG(WARN,"get table[%lu] schema error, ret=%d", table_id, ret);
                }
                else if(tm->get_index_helper().tbl_tid != OB_INVALID_ID)
                {
                    const CommonTableSchema *tm_dt = NULL;
                    if(NULL == (tm_dt = sm->get_table_schema(tm->get_index_helper().tbl_tid)))
                    {
                        ret = OB_ERR_UNEXPECTED;
                        YYSYS_LOG(WARN,"get table[%lu] schema error, ret=%d", tm->get_index_helper().tbl_tid, ret);
                    }
                    else if(tm->get_rowkey_info().get_size() != tm_dt->get_rowkey_info().get_size())
                    {
                        is_index = true;
                        is_speci_index = false;
                        index_table_id = table_id;
                        is_specical_index = false;
                    }
                    else
                    {
                        is_index = true;
                        is_speci_index = true;
                        index_table_id =table_id;
                        is_specical_index = true;
                    }
                }
            }
        }
        return ret;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    MemTableIterator::MemTableIterator() : scan_iter_(), get_iter_(), iter_(NULL)
    {
    }

    MemTableIterator::~MemTableIterator()
    {
    }

    void MemTableIterator::reset()
    {
      if(&scan_iter_ == iter_)
      {
        scan_iter_.reset();
      }
      if(&get_iter_ == iter_)
      {
        get_iter_.reset();
      }
      iter_ = NULL;
    }

    MemTableScanIter &MemTableIterator::get_scan_iter_()
    {
      iter_ = &scan_iter_;
      return scan_iter_;
    }

    MemTableGetIter &MemTableIterator::get_get_iter_()
    {
      iter_ = &get_iter_;
      return get_iter_;
    }

    int MemTableIterator::next_cell()
    {
      int ret = OB_SUCCESS;
      if(NULL == iter_)
      {
        ret = OB_ERROR;
      }
      else
      {
        ret = iter_->next_cell();
      }
      return ret;
    }

    int MemTableIterator::get_cell(ObCellInfo **cell_info)
    {
      return get_cell(cell_info, NULL);
    }

    int MemTableIterator::get_cell(ObCellInfo **cell_info, bool *is_row_changed)
    {
      int ret = OB_SUCCESS;
      if(NULL == iter_)
      {
        ret = OB_ERROR;
      }
      else
      {
        ret = iter_->get_cell(cell_info, is_row_changed);
      }
      return ret;
    }

    int MemTableIterator::is_index_table(const uint64_t table_id, bool &is_index, bool &is_speci_index, const CommonSchemaManager *frozen_schema)
    {
        int ret = OB_SUCCESS;
        if(NULL == iter_)
        {
            ret = OB_ERROR;
        }
        else
        {
            ret = iter_->is_index_table(table_id, is_index, is_speci_index, frozen_schema);
        }
        return ret;
    }
  }
}


