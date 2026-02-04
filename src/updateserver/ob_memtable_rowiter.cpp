////===================================================================
 //
 // ob_memtable_rowiter.cpp updateserver / Oceanbase
 //
 // Copyright (C) 2010 Taobao.com, Inc.
 //
 // Created on 2011-03-24 by Yubai (yubai.lk@taobao.com) 
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

#include "common/ob_define.h"
#include "common/ob_atomic.h"
#include "common/ob_tsi_factory.h"
#include "sstable/ob_sstable_trailer.h"
#include "sstable/ob_sstable_block_builder.h"
#include "ob_memtable_rowiter.h"
#include "ob_update_server_main.h"

namespace oceanbase
{
  namespace updateserver
  {
    using namespace common;
    using namespace hash;

    MemTableRowIterator::MemTableRowIterator() : memtable_(NULL),
                                                 memtable_iter_(),
                                                 memtable_table_iter_(), /*add zhaoqiong [Truncate Table]:20160318*/
                                                 get_iter_(),
                                                 rc_iter_()
    {
      reset_();
    }

    MemTableRowIterator::~MemTableRowIterator()
    {
      destroy();
    }

    int MemTableRowIterator::init(MemTable *memtable, const int store_type)
    {
      int ret = OB_SUCCESS;
      if (NULL != memtable_)
      {
        YYSYS_LOG(WARN, "have already inited");
        ret = OB_INIT_TWICE;
      }
      else if (NULL == memtable)
      {
        YYSYS_LOG(WARN, "invalid param memtable=%p", memtable_);
        ret = OB_INVALID_ARGUMENT;
      }
      else if (!get_schema_handle_())
      {
        YYSYS_LOG(WARN, "get schema handle fail");
        ret = OB_ERROR;
      }
      else if (OB_SUCCESS != (ret = memtable->scan_all(memtable_iter_)))
      {
        YYSYS_LOG(WARN, "scan all start fail ret=%d memtable=%p", ret, memtable);
        revert_schema_handle_();
      }
      //add zhaoqiong [Truncate Table]:20160318:b
      else if (OB_SUCCESS != (ret = memtable->scan_all_table(memtable_table_iter_)))
      {
        YYSYS_LOG(WARN, "scan all table start fail ret=%d memtable=%p", ret, memtable);
        revert_schema_handle_();
      }
      //add:e
      else
      {
        store_type_ = store_type;
        memtable_ = memtable;
      }
      return ret;
    }

    void MemTableRowIterator::destroy()
    {
      if (NULL != memtable_)
      {
        memtable_iter_.reset();
        memtable_table_iter_.reset(); /*add zhaoqiong [Truncate Table]:20160318*/
        revert_schema_handle_();
        memtable_ = NULL;
      }
      reset_();
    }

    void MemTableRowIterator::reset_()
    {
      schema_handle_ = UpsSchemaMgr::INVALID_SCHEMA_HANDLE;
      store_type_ = 0;
    }

    void MemTableRowIterator::revert_schema_handle_()
    {
      ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
      if (NULL != ups_main)
      {
        UpsSchemaMgr &schema_mgr = ups_main->get_update_server().get_table_mgr().get_schema_mgr();
        schema_mgr.revert_schema_handle(schema_handle_);
      }
    }

    bool MemTableRowIterator::get_schema_handle_()
    {
      bool bret = false;
      ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
      if (NULL != ups_main)
      {
        UpsSchemaMgr &schema_mgr = ups_main->get_update_server().get_table_mgr().get_schema_mgr();
        bret = (OB_SUCCESS == schema_mgr.get_schema_handle(schema_handle_));
      }
      return bret;
    }

    int MemTableRowIterator::reset_iter()
    {
      int ret = OB_SUCCESS;
      if (NULL == memtable_)
      {
        YYSYS_LOG(WARN, "have not inited this=%p", this);
        ret = OB_NOT_INIT;
      }
      else
      {
        YYSYS_LOG(INFO, "reset row_iter this=%p", this);
        memtable_iter_.reset();
        ret = memtable_->scan_all(memtable_iter_);
        //add zhaoqiong [Truncate Table]:20160318:b
        memtable_table_iter_.reset();
        ret = memtable_->scan_all_table(memtable_table_iter_);
        //add:e
      }
      return ret;
    }

    int MemTableRowIterator::set_table_iter_(int64_t table_id)
    {
        int ret = OB_SUCCESS;
        if(NULL == memtable_)
        {
            YYSYS_LOG(WARN,"have not inited this=%p",this);
            ret = OB_NOT_INIT;
        }
        else
        {
            memtable_table_iter_.reset();
            ret = memtable_->scan_one_table(memtable_table_iter_, table_id);
        }
        return ret;
    }

    int MemTableRowIterator::next_row()
    {
      int ret = OB_SUCCESS;
      if (NULL == memtable_)
      {
        YYSYS_LOG(WARN, "have not inited this=%p", this);
        ret = OB_NOT_INIT;
      }
      else
      {
        ret = memtable_iter_.next();
      }
      return ret;
    }

    //add zhaoqiong [Truncate Table]:20160318:b
    int MemTableRowIterator::next_table()
    {
      int ret = OB_SUCCESS;
      if (NULL == memtable_)
      {
        YYSYS_LOG(WARN, "have not inited this=%p", this);
        ret = OB_NOT_INIT;
      }
      else
      {
        ret = memtable_table_iter_.next();
      }
      return ret;
    }
    //add:e

    int MemTableRowIterator::get_row(sstable::ObSSTableRow &sstable_row, const CommonSchemaManager *sm)
    {
      int ret = OB_SUCCESS;
      const TEKey key = memtable_iter_.get_key();
      const TEValue *pvalue = memtable_iter_.get_value();
      if (NULL == memtable_)
      {
        YYSYS_LOG(WARN, "have not inited this=%p", this);
        ret = OB_NOT_INIT;
      }
      else if (NULL == pvalue)
      {
        YYSYS_LOG(WARN, "value null pointer");
        ret = OB_ERROR;
      }
      //add [306 bugfix]
      else if(ret != set_table_iter_(key.table_id))
      {
          YYSYS_LOG(WARN,"set_table_iter_ failed");
          ret = OB_ERROR;
      }
      else
      {
        TEValue *table_value = NULL;
        if(memtable_table_iter_.next() != OB_ITER_END && (key.table_id == memtable_table_iter_.get_key().table_id))
        {
            table_value = memtable_table_iter_.get_value();
        }
        else
        {

        }
        sstable_row.clear();
        if (OB_SUCCESS != (ret = sstable_row.set_internal_rowkey(key.table_id, key.row_key)))
        {
          YYSYS_LOG(WARN, "set internal rowkey to sstable_row fail key=[%s]", key.log_str());
        }
        //mod wangyao [258 bugfix]
        else
        {
            // TODO sstable是否支持读取rowkey列
            get_iter_.set_(key, pvalue, NULL, true, NULL, NULL, sm, table_value);
            rc_iter_.set_iterator(&get_iter_);
            rc_iter_.set_frozen_schema(sm);//add wangyao [403 bugfix]

            //add zhaoqiong [bugfix::trim rowkey in varchar type in case of memtable checksum mismatch] 20160612:b
            const common::ObRowkeyInfo * info = get_rowkey_info(key.table_id, sm);
            common::ObRowkeyColumn column;
            int64_t index = 0;
            //add:e
            bool row_delete = false; // add for truncate
            //add by maosy[MultiUps 1.0] [secondary index optimize]20170401 b:
            bool is_change_rowkey =false;
            //add by maosy
            while (OB_SUCCESS == (ret = rc_iter_.next_cell()))
            {
                ObCellInfo *ci = NULL;
                if (OB_SUCCESS != (ret = rc_iter_.get_cell(&ci))
                        || NULL == ci)
                {
                    YYSYS_LOG(WARN, "get cell from get_iter/rc_iter fail ret=%d", ret);
                    ret = (OB_SUCCESS == ret) ? OB_ERROR : ret;
                    break;
                }
                // sstable不接受column_id为OB_INVALID_ID所以转成了0
                // 迭代出来后需要再修改成OB_INVALID_ID
                uint64_t column_id = ci->column_id_;
                ObObj obj = ci->value_;  //add zhaoqiong [bugfix::trim rowkey in varchar type in case of memtable checksum mismatch] 20160612
                if (OB_INVALID_ID == column_id)
                {
                    column_id = OB_FULL_ROW_COLUMN_ID;
                }

                if((column_id == OB_FULL_ROW_COLUMN_ID) && (obj.get_type() == ObExtendType) && (obj.get_ext() == ObActionFlag::OP_DEL_ROW))
                {
                    row_delete = true;
                }
                else
                {
                    row_delete = false;
                }

                //add zhaoqiong [bugfix::trim rowkey in varchar type in case of memtable checksum mismatch] 20160612
                if (OB_SUCCESS == info->get_index(column_id,index,column))
                {
                    if (OB_SUCCESS != sstable_row.shallow_add_rowkey_obj(index, column_id))
                    {
                        YYSYS_LOG(WARN, "add obj to sstable_row fail ret=%d", ret);
                    }
                    //add by maosy [MultiUps 1.0][secondary index optimize]20170401 b:
                    else if(key.row_key.get_obj_ptr()[index] != obj)
                    {
                        is_change_rowkey = true;
                        YYSYS_LOG(DEBUG,"row key = %s,obj =%s,index = %ld",
                                  to_cstring(key.row_key),to_cstring(obj),index);
                        break;
                    }
                    // add by maosy
                }
                //add:e

                //mod zhaoqiong [bugfix::trim rowkey in varchar type in case of memtable checksum mismatch] 20160612
                //          if (OB_SUCCESS != (ret = sstable_row.shallow_add_obj(ci->value_, column_id)))
                //          {
                //            YYSYS_LOG(WARN, "add obj to sstable_row fail ret=%d [%s]", ret, print_cellinfo(ci));
                //            break;
                //          }
                else if (OB_SUCCESS != (ret = sstable_row.shallow_add_obj(obj, column_id)))
                {
                    YYSYS_LOG(WARN, "add obj to sstable_row fail ret=%d [%s]", ret, print_obj(obj));
                    break;
                }
                //mod:e
            }
            ret = (OB_ITER_END == ret) ? OB_SUCCESS : ret;
            if((ret == OB_SUCCESS) && (table_value != NULL && !table_value->is_empty()))
            {
                if(row_delete)
                {
                    ret = OB_TABLE_UPDATE_LOCKED;
                }
            }
            //add by maosy[MultiUps 1.0] [secondary index optimize]20170401 b:
            if(OB_SUCCESS == ret && is_change_rowkey)
            {
                sstable_row.clear();
                if (OB_SUCCESS != (ret = sstable_row.set_internal_rowkey(key.table_id, key.row_key)))
                {
                    YYSYS_LOG(WARN, "set internal rowkey to sstable_row fail key=[%s]", key.log_str());
                }
                else
                {
                    YYSYS_LOG(DEBUG,"test");
                    ObObj obj;
                    obj.set_type(ObExtendType);
                    obj.set_ext(ObActionFlag::OP_DEL_ROW);
                    if (OB_SUCCESS != (ret = sstable_row.shallow_add_obj(obj,OB_FULL_ROW_COLUMN_ID)))
                    {
                        YYSYS_LOG(WARN, "add obj to sstable_row fail ret=%d [%s]", ret, print_obj(obj));
                    }
                }
            }
		// add by maosy 
        }
      }
      return ret;
    }

    bool MemTableRowIterator::get_compressor_name(ObString &compressor_str)
    {
      bool bret = false;
      ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
      if (NULL != ups_main)
      {
        const char *compressor_name = ups_main->get_update_server().get_param().sstable_compressor_name;
        if (NULL == compressor_name
            || 0 == strlen(compressor_name))
        {
          compressor_name = DEFAULT_COMPRESSOR_NAME;
        }
        compressor_str.assign_ptr(const_cast<char*>(compressor_name), static_cast<int32_t>(strlen(compressor_name)));
        YYSYS_LOG(INFO, "get compressor_name=%s from config", compressor_name);
        bret = true;
      }
      return bret;
    }

    bool MemTableRowIterator::get_sstable_schema(sstable::ObSSTableSchema &sstable_schema, const CommonSchemaManager *sm)
    {
      int bret = false;
      int ret = OB_SUCCESS; /*add zhaoqiong [Truncate Table]:20160318*/
      if (NULL != memtable_)
      {
        ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
        if (NULL != ups_main)
        {
          UpsSchemaMgr &schema_mgr = ups_main->get_update_server().get_table_mgr().get_schema_mgr();
          //mod zhaoqiong [Truncate Table]:20160318:b
//          if (OB_SUCCESS == schema_mgr.build_sstable_schema(schema_handle_, sstable_schema))
//          {
//            bret = true;
//          }
          if (OB_SUCCESS != (ret = schema_mgr.build_sstable_schema(schema_handle_, sstable_schema, sm)))
          {
            YYSYS_LOG(WARN, "build sstable schema failed, err[%d]", ret);
          }
          else if (OB_SUCCESS != (ret = fill_truncate_info_(sstable_schema)))
          {
            YYSYS_LOG(WARN, "fill truncate info into sstable schema failed, err[%d]", ret);
          }
          else
          {
            bret = true;
          }
          //mod:e
        }
      }
      return bret;
    }

    const ObRowkeyInfo *MemTableRowIterator::get_rowkey_info(const uint64_t table_id, const CommonSchemaManager *sm) const
    {
      return RowkeyInfoCache::get_rowkey_info(table_id, sm);
    }

    bool MemTableRowIterator::get_store_type(int &store_type)
    {
      bool bret = false;
      if (NULL != memtable_)
      {
        store_type = store_type_;
        bret = true;
      }
      return bret;
    }

    bool MemTableRowIterator::get_block_size(int64_t &block_size)
    {
      bool bret = false;
      ObUpdateServerMain *ups_main = ObUpdateServerMain::get_instance();
      if (NULL != ups_main)
      {
        block_size = ups_main->get_update_server().get_param().sstable_block_size;
        YYSYS_LOG(INFO, "get block_size=%ld from config", block_size);
        bret = true;
      }
      return bret;
    }

    //add zhaoqiong [Truncate Table]:20160318:b
    int MemTableRowIterator::fill_truncate_info_(sstable::ObSSTableSchema &sstable_schema)
    {
      int ret = OB_SUCCESS;
      sstable::ObSSTableSchemaTableDef tab_def;
      TEKey key;
      TEValue *pvalue = NULL;
      while (OB_SUCCESS == ret && OB_SUCCESS == (ret = next_table()))
      {
        key = memtable_table_iter_.get_key();
        pvalue = memtable_table_iter_.get_value();
        get_iter_.set_(key, pvalue, NULL, false, NULL);
        if (OB_SUCCESS == (ret = get_iter_.next_cell()))
        {
          ObCellInfo *ci = NULL;
          if (OB_SUCCESS != (ret = get_iter_.get_cell(&ci))
              || NULL == ci)
          {
            YYSYS_LOG(WARN, "get cell from get_iter/rc_iter fail ret=%d", ret);
            ret = (OB_SUCCESS == ret) ? OB_ERROR : ret;
            break;
          }
          tab_def.table_id_ = static_cast<uint32_t>(key.table_id);
          if (OB_SUCCESS != (ret = sstable_schema.add_table_def(tab_def)))
          {
            YYSYS_LOG(WARN, "add table def failed, table_id=%d", tab_def.table_id_);
          }
        }
        else
        {
          YYSYS_LOG(WARN, "get cell from get_iter/rc_iter ret=%d", ret);
        }
      }
      if (ret != OB_SUCCESS && ret != OB_ITER_END)
      {
        YYSYS_LOG(ERROR, "fill truncate info failed, ret=[%d]", ret);
      }
      else
      {
        YYSYS_LOG(INFO, "fill truncate info succ");
        ret = OB_SUCCESS;
      }
      return ret;
    }
    //add:e
  }
}

