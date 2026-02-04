////===================================================================
//
// ob_iterator_adaptor.cpp common / Oceanbase
//
// Copyright (C) 2010, 2013 Taobao.com, Inc.
//
// Created on 2012-11-21 by Yubai (yubai.lk@taobao.com)
//
// -------------------------------------------------------------------
//
// Description
//
// -------------------------------------------------------------------
//
// Change Log
//
////====================================================================
#include "ob_iterator_adaptor.h"
#include "ob_new_scanner_helper.h"
#include "ob_schema.h"
#include "ob_obj_cast.h"
#include "utility.h"


namespace oceanbase
{
  namespace common
  {
    REGISTER_CREATOR(oceanbase::sql::ObPhyOperatorGFactory, oceanbase::sql::ObPhyOperator, ObRowIterAdaptor, oceanbase::sql::PHY_ROW_ITER_ADAPTOR);
  }
}

namespace oceanbase
{
  namespace common
  {
    ObObjCastHelper::ObObjCastHelper() : need_cast_(false),
      col_num_(0)
    {
    }

    ObObjCastHelper::~ObObjCastHelper()
    {
    }

    void ObObjCastHelper::set_need_cast(const bool need_cast)
    {
      need_cast_ = need_cast;
    }

    int64_t ObObjCastHelper::get_col_len(int64_t idx)
    {
      return col_lens_[idx];
    }

    uint32_t ObObjCastHelper::get_col_type(int64_t idx)
    {
      return col_types_[idx].type_;
    }

    int ObObjCastHelper::reset(const ObRowDesc &row_desc, const ObSchemaManagerV2 &schema_mgr, int64_t index_num)
    {
        UNUSED(index_num);//[662]
      int ret = OB_SUCCESS;
      need_cast_ = true;
      col_num_ = 0;
      //add wenghaixing [secondary index static_index_build.consistency]20150424
      //uint64_t data_tid = OB_INVALID_ID;
      //add e
      if (OB_ROW_MAX_COLUMNS_COUNT < row_desc.get_column_num())
      {
        YYSYS_LOG(WARN, "row_desc too long %ld", row_desc.get_column_num());
        ret = OB_SIZE_OVERFLOW;
      }
      else
      {
        int64_t i = 0;
        uint64_t tid = OB_INVALID_ID;
        uint64_t cid = OB_INVALID_ID;
        const ObColumnSchemaV2 *col_schema = NULL;
        uint8_t type = (uint8_t)ObMinType;
        int64_t col_len = 0;

        while (i < row_desc.get_column_num())
        {
          if (OB_SUCCESS != (ret = row_desc.get_tid_cid(i, tid, cid)))
          {
            YYSYS_LOG(WARN, "get_tid_cid from row_desc fail ret=%d idx=%ld", ret, i);
            break;
          }
          if (OB_ACTION_FLAG_COLUMN_ID == cid)
          {
            type = (uint8_t)ObMinType;
          }
          else if (NULL == (col_schema = schema_mgr.get_column_schema(tid, cid)))
          {
            if (cid < (uint64_t)OB_APP_MIN_COLUMN_ID)
            {
              type = (uint8_t)ObMinType;
            }
            else
            {
              YYSYS_LOG(WARN, "get_column_schema fail tid=%lu cid=%lu", tid, cid);
              ret = OB_ENTRY_NOT_EXIST;
              break;
            }
          }
          else
          {
            type = (uint8_t)col_schema->get_type();
            if (OB_INVALID_ID != schema_mgr.get_table_schema(tid)->get_index_helper().tbl_tid
                && ObVarcharType == type && 0 == col_schema->get_size())
            {
              col_len = OB_MAX_VARCHAR_LENGTH;
            }
            else
            {
              col_len = col_schema->get_size();
            }
          }
          col_lens_[i] = col_len;

          col_types_[i++].type_ = type;
          //add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
          /*
           *�ú�����Ҫ�����ǰ�col_schema�����ֵ�浽col_types_�������档����col_schema�Ǵ�schema_mgr�����õ��û�
           *����ı���schema��col_types_������ں���������ת����ʱ���õ���
           *
           *OBԭ�ȵ�ʵ��ֻ�ǰѸ��е����ʹ浽col_types_�������档�����ӵĴ���ԭ�������жϸ����Ƿ�Ϊdecimal���͡�����ǵĻ���
           *�����е�precision��scaleҲ�浽col_types_��������
           */
          int64_t middle_i=i-1;
          if(type==(uint8_t)ObDecimalType)
          {
            col_types_[middle_i].dec_precision_ = static_cast<uint8_t>(col_schema->get_precision()) & META_PREC_MASK;
            col_types_[middle_i].dec_scale_ = static_cast<uint8_t>(col_schema->get_scale()) & META_SCALE_MASK;
            //YYSYS_LOG(ERROR, "test::whx dec_precision = [%d], dec_scale_ = [%d]", col_types_[middle_i].dec_precision_ ,col_types_[middle_i].dec_scale_);
          }
          //add:e
          YYSYS_LOG(DEBUG, "set col type idx=%ld tid=%lu cid=%lu type=%hhu", i - 1, tid, cid, col_types_[i - 1].type_);
        }
        if (OB_SUCCESS == ret)
        {
          col_num_ = i;
        }
        //add wenghaixing [secondary index static_index_build.consistency]20150424
        //data_tid = tid;
        //add e
      }
      //add wenghaixing [secondary index static_index_build.consistency]20150424
      //Ϊ�˷�ֹ���ݲ�һ�£���ups����һ�����
      /*  //del [622]
      if(OB_SUCCESS == ret)
      {
        int64_t count = 0;
        if(schema_mgr.is_have_modifiable_index(data_tid) )
        {
          if(OB_SUCCESS == (ret = schema_mgr.get_all_modifiable_index_num(data_tid, count)))
          {
            if(count != index_num)
            {
              YYSYS_LOG(ERROR, "this table has [%ld] modifiable index ,but operator is [%ld]", count, index_num);
              ret = OB_ERROR;
            }
          }

        }

      }*/
      //add e
      return ret;
    }

    int ObObjCastHelper::cast_cell(const int64_t idx, ObObj &cell) const
    {
      int ret = OB_SUCCESS;
      if (!need_cast_)
      {
        // do nothing
      }
      else if (col_num_ <= idx
               || 0 > idx)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if ((uint8_t)ObMinType == col_types_[idx].type_)
      {
        // need not cast
      }
      else
      {
        ObString cast_buffer = get_tsi_buffer_();
        ret = obj_cast(cell, (ObObjType)col_types_[idx].type_, cast_buffer);
      }
      return ret;
    }

    //add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
    /*
     *�ӿں������������col_types_�������precision��scale��idx�������±꣬col_types_�����������Ƕ���е�schema.
     *��2���ӿ�ֻ��ObCellAdaptor::next_cell()������á�
     */
    uint32_t ObObjCastHelper::get_precision(const int64_t idx)
    {
      return col_types_[idx].dec_precision_;
    }
    uint32_t ObObjCastHelper::get_scale(const int64_t idx)
    {
      return col_types_[idx].dec_scale_;
    }
    //add:e

    int ObObjCastHelper::cast_rowkey_cell(const int64_t idx, ObObj &cell) const
    {
      int ret = OB_SUCCESS;
      if (!need_cast_)
      {
        // do nothing
      }
      else if (col_num_ <= idx
               || 0 > idx)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else if ((uint8_t)ObMinType == col_types_[idx].type_)
      {
        // need not cast
      }
      else
      {
        ObString cast_buffer = get_tsi_buffer_(idx);
        ret = obj_cast(cell, (ObObjType)col_types_[idx].type_, cast_buffer);
      }
      return ret;
    }

    ObString ObObjCastHelper::get_tsi_buffer_()
    {
      ObString ret;
      static __thread char BUFFER[OB_MAX_VARCHAR_LENGTH];
      ret.assign_ptr(BUFFER, OB_MAX_VARCHAR_LENGTH);
      return ret;
    }

    ObString ObObjCastHelper::get_tsi_buffer_(const int64_t idx)
    {
      ObString ret;
      static __thread char BUFFER[OB_MAX_ROWKEY_COLUMN_NUMBER][OB_MAX_VARCHAR_LENGTH];
      if (OB_MAX_ROWKEY_COLUMN_NUMBER > idx)
      {
        ret.assign_ptr(BUFFER[idx], OB_MAX_VARCHAR_LENGTH);
      }
      return ret;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    int ObRowkeyCastHelper::cast_rowkey(const ObRowkeyInfo &rki, ObRowkey &rowkey)
    {
      int ret = OB_SUCCESS;
      if (NULL == rowkey.ptr()
          || rki.get_size() != rowkey.get_obj_cnt())
      {
        YYSYS_LOG(WARN, "invalid param rowkey_ptr=%p rki_size=%ld rowkey_obj_cnt=%ld",
                  rowkey.ptr(), rki.get_size(), rowkey.get_obj_cnt());
        ret = OB_INVALID_ARGUMENT;
      }
      YYSYS_LOG(DEBUG, "before cast rowkey %s", to_cstring(rowkey));
      for (int64_t i = 0; OB_SUCCESS == ret && i < rowkey.get_obj_cnt(); i++)
      {
        ObString cast_buffer = get_tsi_buffer_(i);
        const ObRowkeyColumn *rc = rki.get_column(i);
        if (NULL == rc)
        {
          YYSYS_LOG(WARN, "get rowkey column schema fail idx=%ld", i);
          ret = OB_ERR_UNEXPECTED;
        }
        else if (OB_SUCCESS != (ret = obj_cast(const_cast<ObObj&>(rowkey.ptr()[i]), rc->type_, cast_buffer)))
        {
          YYSYS_LOG(WARN, "cast rowkey cell fail idx=%ld %s", i, to_cstring(rowkey.ptr()[i]));
          break;
        }
        else
        {
          YYSYS_LOG(DEBUG, "cast rowkey cell succ idx=%ld %s", i, to_cstring(rowkey.ptr()[i]));
        }
      }
      return ret;
    }

    ObString ObRowkeyCastHelper::get_tsi_buffer_(const int64_t idx)
    {
      ObString ret;
      static __thread char BUFFER[OB_MAX_ROWKEY_COLUMN_NUMBER][OB_MAX_VARCHAR_LENGTH];
      if (OB_MAX_ROWKEY_COLUMN_NUMBER > idx)
      {
        ret.assign_ptr(BUFFER[idx], OB_MAX_VARCHAR_LENGTH);
      }
      return ret;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    ObCellAdaptor::ObCellAdaptor() : row_(NULL),
      rk_size_(0),
      cur_idx_(0),
      is_iter_end_(false),
      cell_(),
      need_nop_cell_(false),
      och_()
    {
    }

    ObCellAdaptor::~ObCellAdaptor()
    {
        str_buf_.clear();
    }

    int ObCellAdaptor::next_cell()
    {
      int ret = OB_SUCCESS;
      const ObObj *value = NULL;
      if (NULL == row_)
      {
        ret = OB_NOT_INIT;
      }
      else if (is_iter_end_)
      {
        ret = OB_ITER_END;
      }
      else if (cur_idx_ >= row_->get_column_num())
      {
        if (!need_nop_cell_)
        {
          ret = OB_ITER_END;
        }
        else
        {
          need_nop_cell_ = false;
          cell_.column_id_ = OB_INVALID_ID;
          cell_.value_.set_ext(ObActionFlag::OP_NOP);
        }
      }
      else if (OB_SUCCESS != (ret = row_->raw_get_cell(cur_idx_, value, cell_.table_id_, cell_.column_id_))
               || NULL == value)
      {
        ret = (OB_SUCCESS == ret) ? OB_ERROR : ret;
      }
      else
      {
        YYSYS_LOG(DEBUG, "CELL_ADAPTOR idx=%ld %s", cur_idx_, print_cellinfo(&cell_));
        cell_.value_ = *value;
        if (OB_ACTION_FLAG_COLUMN_ID == cell_.column_id_)
        {
          cell_.value_.set_ext(ObActionFlag::OP_DEL_ROW);
        }
        else if (OB_SUCCESS != (ret = och_.cast_cell(cur_idx_, cell_.value_)))
        {
          YYSYS_LOG(WARN, "obj_cast fail ret=%d cur_idx=%ld %s", ret, cur_idx_, to_cstring(cell_.value_));
        }
        if (ObExtendType == cell_.value_.get_type())
        {
          cell_.column_id_ = OB_INVALID_ID;
        }
        //add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
        /*
         *cell_����������ת����Ľ����������е�������decimal���Ͱ�och_�����ĸ��е�precision��scaleҲд��cell_����
         *
         *modify_value�Ƕ�ת�����ֵ��һ�ν�ȡ��
         */
        //mod liuzy [TRANS_DECIMAL_DATE/TIME_BUG_FIX] 20151229:b
        //        if (ObDecimalType == cell_.value_.get_type())
        if (OB_SUCCESS == ret && ObVarcharType == cell_.value_.get_type())
        {
          int64_t col_len;
          col_len = och_.get_col_len(cur_idx_);
          if (cell_.value_.get_val_len() > col_len)
          {
            YYSYS_LOG(INFO, "varchar is too long,length in schema[%ld], length in varchar[%d]", col_len, cell_.value_.get_val_len());
            ret = OB_ERR_VARCHAR_TOO_LONG;
            YYSYS_LOG(USER_ERROR, "%ld.%ld.%d", cell_.table_id_, cell_.column_id_, cell_.value_.get_val_len());
          }
        }
        if (OB_SUCCESS == ret && ObDecimalType == cell_.value_.get_type())
          //mod 20151229:e
        {
          uint32_t p=och_.get_precision(cur_idx_);
          uint32_t s=och_.get_scale(cur_idx_);
          uint64_t *t1 = NULL;
          if(OB_SUCCESS!=(ret=cell_.value_.get_ttint(t1)))
          {
            YYSYS_LOG(ERROR, "failed to do get_decimal() in ObCellAdaptor::next_cell");
          }
          else
          {
            uint32_t tmp_vs = cell_.value_.get_vscale();
            ObDecimal tmp_dec;
            cell_.value_.get_decimal(tmp_dec);
            TTInt *word = tmp_dec.get_words();
            TTInt whole;
            TTInt BASE(10),pp(tmp_vs);
            BASE.Pow(pp);
            whole = word[0] / BASE;
            if (word[0] .IsSign())
            {
              whole.ChangeSign();
            }
            std::string str_for_int;
            whole.ToString(str_for_int);
            int len_int = (int)str_for_int.length();
            if ((int) (p - s) < len_int)
            {
              ret=OB_DECIMAL_UNLEGAL_ERROR;
              YYSYS_LOG(ERROR, "OB_DECIMAL_UNLEGAL_ERROR,p=%d,s=%d,od.get_precision()=%d,od.get_vscale()=%d,value=%s",
                        p, s, cell_.value_.get_precision(), cell_.value_.get_vscale(), to_cstring(cell_.value_));
              YYSYS_LOG(USER_ERROR, "%ld.%ld.%d.%d.%d.%d", cell_.table_id_, cell_.column_id_, p, s, cell_.value_.get_precision(), cell_.value_.get_vscale());
            }
            else
            {
              //cell_.value_.set_precision(p);
              //cell_.value_.set_scale(s);
                if(OB_SUCCESS != (ret = tmp_dec.modify_value(p, s)))
                {

                }
                else
                {
                    ObObj obj2;
                    uint32_t len = 0;
                    obj2.set_decimal(tmp_dec);
                    if(tmp_dec.get_words()->table[1] == 0)
                    {
                        len = 1;
                    }
                    else
                    {
                        len = 2;
                    }
                    obj2.set_nwords(len);
                    str_buf_.write_obj(obj2, &cell_.value_);
                }
            }
          }
        }
        //add:e
        if (OB_SUCCESS == ret)
        {
          cur_idx_ += 1;
        }
      }
      is_iter_end_ = (OB_SUCCESS != ret);
      return ret;
    }

    int ObCellAdaptor::get_cell(ObCellInfo** cell)
    {
      return get_cell(cell, NULL);
    }

    int ObCellAdaptor::get_cell(ObCellInfo** cell, bool* is_row_changed)
    {
      int ret = OB_SUCCESS;
      if (NULL == row_)
      {
        ret = OB_NOT_INIT;
      }
      else if (is_iter_end_)
      {
        ret = OB_ITER_END;
      }
      else if (NULL == cell)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        *cell = &cell_;
        if (NULL != is_row_changed)
        {
          *is_row_changed = (cur_idx_ <= (rk_size_ + 1));
        }
      }
      return ret;
    }

    int ObCellAdaptor::is_row_finished(bool* is_row_finished)
    {
      int ret = OB_SUCCESS;
      if (NULL == row_)
      {
        ret = OB_NOT_INIT;
      }
      else if (is_iter_end_)
      {
        ret = OB_ITER_END;
      }
      else
      {
        if (NULL != is_row_finished)
        {
          if (cur_idx_ == row_->get_column_num()
              && !need_nop_cell_)
          {
            *is_row_finished = true;
          }
          else
          {
            *is_row_finished = false;
          }
        }
      }
      return ret;
    }

    void ObCellAdaptor::set_row(const ObRow *row, const int64_t rk_size)
    {
      row_ = NULL;
      rk_size_ = 0;
      cur_idx_ = 0;
      is_iter_end_ = false;
      cell_.reset();
      need_nop_cell_ = false;
      if (NULL != row
          && 0 < rk_size)
      {
        const ObObj *rk_values = NULL;
        uint64_t table_id = OB_INVALID_ID;
        uint64_t column_id = OB_INVALID_ID;
        if (rk_size <= row->get_column_num()
            && OB_SUCCESS == row->raw_get_cell(0, rk_values, table_id, column_id)
            && NULL != rk_values
            && OB_SUCCESS == cast_rowkey_(*row, rk_size))
        {
          YYSYS_LOG(DEBUG, "set_row succ rk_size=%ld col_num=%ld", rk_size, row->get_column_num());
          row_ = row;
          rk_size_ = rk_size;
          cur_idx_ = rk_size;
          cell_.row_key_.assign(const_cast<ObObj*>(rk_values), rk_size);
          cell_.table_id_ = table_id;
          cell_.column_id_ = column_id;

          need_nop_cell_ = (rk_size == row->get_column_num());
        }
        else
        {
          YYSYS_LOG(WARN, "set_row fail rk_size=%ld col_num=%ld", rk_size, row->get_column_num());
        }
      }
    }

    int ObCellAdaptor::cast_rowkey_(const ObRow &row, const int64_t rk_size)
    {
      int ret = OB_SUCCESS;
      for (int64_t i = 0; i < rk_size; i++)
      {
        const ObObj *value = NULL;
        uint64_t table_id = OB_INVALID_ID;
        uint64_t column_id = OB_INVALID_ID;
        if (OB_SUCCESS != (ret = row.raw_get_cell(i, value, table_id, column_id))
            || NULL == value)
        {
          YYSYS_LOG(WARN, "get_cell from row fail ret=%d idx=%ld", ret, i);
          ret = (OB_SUCCESS == ret) ? OB_ERROR : ret;
          break;
        }
        ObObj &casted_value = const_cast<ObObj&>(*value);
        if (OB_SUCCESS != (ret = och_.cast_rowkey_cell(i, casted_value)))
        {
          YYSYS_LOG(WARN, "obj_cast fail ret=%d idx=%ld %s", ret, i, to_cstring(casted_value));
          break;
        }
      }
      return ret;
    }

    void ObCellAdaptor::reset()
    {
      row_ = NULL;
      rk_size_ = 0;
      cur_idx_ = 0;
      is_iter_end_ = false;
      cell_.reset();
      need_nop_cell_ = false;
      str_buf_.reset();
    }

    ObObjCastHelper &ObCellAdaptor::get_och()
    {
      return och_;
    }
    //add fanqiushi_index
    ////////////////////////////////////////////////////////////////////////////////////////////////////
    ObIndexCellIterAdaptor::ObIndexCellIterAdaptor() : row_iter_(NULL),
      row_desc_(),
      index_row_tmp_(),
      rk_size_(0),
      single_row_iter_(),
      is_iter_end_(false),
      set_row_iter_ret_(OB_SUCCESS)
    {
    }

    ObIndexCellIterAdaptor::~ObIndexCellIterAdaptor()
    {
    }
    void ObIndexCellIterAdaptor::set_row_iter(ObRowStore *row_iter, const int64_t rk_size, const ObSchemaManagerV2 *schema_mgr,ObRowDesc row_desc,int64_t index_num)
    {
      row_iter_ = NULL;
      rk_size_ = 0;
      single_row_iter_.reset();
      row_desc_.reset();
      //index_row_tmp_.reset();
      is_iter_end_ = false;
      if (NULL != row_iter
          && 0 < rk_size )
      {
        int tmp_ret = OB_SUCCESS;
        if (NULL != schema_mgr)
        {
          tmp_ret = single_row_iter_.get_och().reset(row_desc, *schema_mgr, index_num);
        }
        else
        {
          single_row_iter_.get_och().set_need_cast(false);
        }
        if (OB_SUCCESS == tmp_ret)
        {
          row_desc_=row_desc;
          //ObRow row_tmp2 ;
          index_row_tmp_.set_row_desc(row_desc_);
          tmp_ret = row_iter->get_next_row(index_row_tmp_);
          YYSYS_LOG(DEBUG, "INDEX_ITER_ADAPTOR ret=%d row_tmp2= %s",
                    tmp_ret, to_cstring(index_row_tmp_));
          if (OB_SUCCESS == tmp_ret || OB_ITER_END == tmp_ret)
          {
            row_iter_ = row_iter;
            rk_size_ = rk_size;
            if (OB_ITER_END == tmp_ret)
            {
              is_iter_end_ = true;
            }
            else
            {
              single_row_iter_.set_row(&index_row_tmp_, rk_size);
            }
          }
        }
        set_row_iter_ret_ = tmp_ret;
      }
    }

    void ObIndexCellIterAdaptor::reset()
    {
      row_iter_ = NULL;
      row_desc_.reset();
      //index_row_tmp_.reset();
      rk_size_ = 0;
      single_row_iter_.reset();
      is_iter_end_ = false;
    }
    int ObIndexCellIterAdaptor::is_row_finished(bool* is_row_finished)
    {
      int ret = OB_SUCCESS;
      if (NULL == row_iter_)
      {
        ret = OB_NOT_INIT;
      }
      else if (is_iter_end_)
      {
        ret = OB_ITER_END;
      }
      else
      {
        ret = single_row_iter_.is_row_finished(is_row_finished);
      }
      return ret;
    }

    int ObIndexCellIterAdaptor::next_cell()
    {
      int ret = OB_SUCCESS;
      //YYSYS_LOG(ERROR,"test::fanqs,,enter ObIndexCellIterAdaptor::next_cell,set_row_iter_ret_=%d,is_iter_end_=%d",set_row_iter_ret_,is_iter_end_);
      if (OB_SUCCESS != set_row_iter_ret_)
      {
        ret = set_row_iter_ret_;
      }
      else if (NULL == row_iter_)
      {
        //YYSYS_LOG(ERROR,"test::fanqs,,row_iter_=null");
        ret = OB_NOT_INIT;
      }
      else if (is_iter_end_)
      {
        ret = OB_ITER_END;
      }
      else
      {

        ret = single_row_iter_.next_cell();
        //YYSYS_LOG(ERROR,"test::fanqs,,enter single_row_iter_.next_cell(),ret=%d",ret);
        if (OB_ITER_END == ret)
        {
          // const ObRow *row = NULL;
          //ObRow row_tmp2 ;
          index_row_tmp_.set_row_desc(row_desc_);
          if (OB_SUCCESS != (ret = row_iter_->get_next_row(index_row_tmp_)))
          {
            ret = (OB_SUCCESS == ret) ? OB_ERROR : ret;
          }
          else
          {
            single_row_iter_.set_row(&index_row_tmp_, rk_size_);
            ret = single_row_iter_.next_cell();
          }
        }
      }
      is_iter_end_ = (OB_SUCCESS != ret);
      return ret;
    }

    int ObIndexCellIterAdaptor::get_cell(ObCellInfo** cell)
    {
      return get_cell(cell, NULL);
    }
    int ObIndexCellIterAdaptor::get_cell(ObCellInfo** cell, bool* is_row_changed)
    {
      int ret = OB_SUCCESS;
      if (NULL == row_iter_)
      {
        ret = OB_NOT_INIT;
      }
      else if (is_iter_end_)
      {
        ret = OB_ITER_END;
      }
      else if (NULL == cell)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        ret = single_row_iter_.get_cell(cell, is_row_changed);
      }
      return ret;
    }
    //add:e
    ////////////////////////////////////////////////////////////////////////////////////////////////////

    ObCellIterAdaptor::ObCellIterAdaptor() : row_iter_(NULL),
      rk_size_(0),
      single_row_iter_(),
      is_iter_end_(false),
      set_row_iter_ret_(OB_SUCCESS)
    {
    }

    ObCellIterAdaptor::~ObCellIterAdaptor()
    {
    }

    int ObCellIterAdaptor::next_cell()
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != set_row_iter_ret_)
      {
        ret = set_row_iter_ret_;
      }
      else if (NULL == row_iter_)
      {
        ret = OB_NOT_INIT;
      }
      else if (is_iter_end_)
      {
        ret = OB_ITER_END;
      }
      else
      {
        ret = single_row_iter_.next_cell();
        if (OB_ITER_END == ret)
        {
          const ObRow *row = NULL;
          if (OB_SUCCESS != (ret = row_iter_->get_next_row(row))
              || NULL == row)
          {
            ret = (OB_SUCCESS == ret) ? OB_ERROR : ret;
          }
          else
          {
            single_row_iter_.set_row(row, rk_size_);
            ret = single_row_iter_.next_cell();
          }
        }
      }
      is_iter_end_ = (OB_SUCCESS != ret);
      return ret;
    }

    int ObCellIterAdaptor::get_cell(ObCellInfo** cell)
    {
      return get_cell(cell, NULL);
    }

    int ObCellIterAdaptor::get_cell(ObCellInfo** cell, bool* is_row_changed)
    {
      int ret = OB_SUCCESS;
      if (NULL == row_iter_)
      {
        ret = OB_NOT_INIT;
      }
      else if (is_iter_end_)
      {
        ret = OB_ITER_END;
      }
      else if (NULL == cell)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        ret = single_row_iter_.get_cell(cell, is_row_changed);
      }
      return ret;
    }

    int ObCellIterAdaptor::is_row_finished(bool* is_row_finished)
    {
      int ret = OB_SUCCESS;
      if (NULL == row_iter_)
      {
        ret = OB_NOT_INIT;
      }
      else if (is_iter_end_)
      {
        ret = OB_ITER_END;
      }
      else
      {
        ret = single_row_iter_.is_row_finished(is_row_finished);
      }
      return ret;
    }

    //modify wenghaixing [secondary index static_index_build.consistency]20150424
    //void ObCellIterAdaptor::set_row_iter(sql::ObPhyOperator *row_iter, const int64_t rk_size, const ObSchemaManagerV2 *schema_mgr)
    void ObCellIterAdaptor::set_row_iter(sql::ObPhyOperator *row_iter, const int64_t rk_size, const ObSchemaManagerV2 *schema_mgr, int64_t index_num)
    //e
    {
      row_iter_ = NULL;
      rk_size_ = 0;
      single_row_iter_.reset();
      is_iter_end_ = false;
      if (NULL != row_iter
          && 0 < rk_size)
      {
        int tmp_ret = OB_SUCCESS;
        const ObRowDesc *row_desc = NULL;

        if (NULL != schema_mgr)
        {
          //          const ObRowDesc *row_desc = NULL;
          if (OB_SUCCESS != (tmp_ret = row_iter->get_row_desc(row_desc))
              || NULL == row_desc)
          {
            YYSYS_LOG(WARN, "get_row_desc fail ret=%d phy_op=%p phy_type=%d", tmp_ret, row_iter, row_iter->get_type());
            tmp_ret = (OB_SUCCESS == tmp_ret) ? OB_ERROR : tmp_ret;
          }
          else
          {
            //modify wenghaixing [secondary index static_index_build.consistency]20150424
            //tmp_ret = single_row_iter_.get_och().reset(*row_desc, *schema_mgr);
            tmp_ret = single_row_iter_.get_och().reset(*row_desc, *schema_mgr, index_num);
            //modify e
          }
        }
        else
        {
          single_row_iter_.get_och().set_need_cast(false);
        }
        const ObRow *row = NULL;
        if (OB_SUCCESS == tmp_ret)
        {
          tmp_ret = row_iter->get_next_row(row);
          YYSYS_LOG(DEBUG, "ITER_ADAPTOR ret=%d op=%p type=%d %s",
                    tmp_ret, row_iter, row_iter->get_type(), (NULL == row) ? "nil" : to_cstring(*row));
          if ((OB_SUCCESS == tmp_ret && NULL != row)
              || OB_ITER_END == tmp_ret)
          {
            row_iter_ = row_iter;
            rk_size_ = rk_size;
            if (OB_ITER_END == tmp_ret)
            {
              is_iter_end_ = true;
            }
            else
            {
              single_row_iter_.set_row(row, rk_size);
            }
          }
        }
        set_row_iter_ret_ = tmp_ret;
      }
    }

    void ObCellIterAdaptor::reset()
    {
      row_iter_ = NULL;
      rk_size_ = 0;
      single_row_iter_.reset();
      is_iter_end_ = false;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////

    ObRowIterAdaptor::ObRowIterAdaptor() : mod_(ObModIds::OB_ROW_ITER_ADAPTOR),
      allocator_(ALLOCATOR_PAGE_SIZE, mod_),
      cell_iter_(NULL),
      cur_row_(),
      is_ups_row_(true)
    {
    }

    ObRowIterAdaptor::ObRowIterAdaptor(bool is_ups_row) : mod_(ObModIds::OB_ROW_ITER_ADAPTOR),
      allocator_(ALLOCATOR_PAGE_SIZE, mod_),
      cell_iter_(NULL),
      cur_row_(),
      is_ups_row_(is_ups_row)
    {
    }

    ObRowIterAdaptor::~ObRowIterAdaptor()
    {
    }

    int ObRowIterAdaptor::set_child(int32_t child_idx, ObPhyOperator &child_operator)
    {
      UNUSED(child_idx);
      UNUSED(child_operator);
      return OB_NOT_SUPPORTED;
    }

    int ObRowIterAdaptor::open()
    {
      int ret = OB_SUCCESS;
      // do nothing
      return ret;
    }

    int ObRowIterAdaptor::close()
    {
      int ret = OB_SUCCESS;
      // do nothing
      return ret;
    }

    int ObRowIterAdaptor::get_next_row(const ObRow *&row)
    {
      const ObRowkey *rowkey = NULL;
      return get_next_row(rowkey, row);
    }

    int ObRowIterAdaptor::get_next_row(const ObRowkey *&rowkey, const ObRow *&row)
    {
      int ret = OB_SUCCESS;
      if (NULL == cell_iter_)
      {
        ret = OB_NOT_INIT;
      }
      else
      {
        int64_t cell_counter = 0;
        cur_row_.reset(false, is_ups_row_ ? ObRow::DEFAULT_NOP : ObRow::DEFAULT_NULL);
        allocator_.reuse();
        bool is_row_finished = false;
        while (OB_SUCCESS == (ret = cell_iter_->next_cell()))
        {
          ObCellInfo *ci = NULL;
          if (OB_SUCCESS != (ret = cell_iter_->get_cell(&ci))
              || NULL == ci
              || OB_SUCCESS != (ret = cell_iter_->is_row_finished(&is_row_finished)))
          {
            ret = (OB_SUCCESS == ret) ? OB_ERROR : ret;
            break;
          }
          if (deep_copy_
              && OB_SUCCESS != (ret = ob_write_obj(allocator_, ci->value_, ci->value_)))
          {
            YYSYS_LOG(WARN, "deep copy cell fail ret=%d allocator_total=%ld allocator_used=%ld",
                      ret, allocator_.total(), allocator_.used());
            break;
          }
          if (OB_SUCCESS != (ret = ObNewScannerHelper::add_cell(cur_row_, *ci, is_ups_row_)))
          {
            YYSYS_LOG(WARN, "add cell to cur_row fail ret=%d cell=[%s] row=[%s]",
                      ret, print_cellinfo(ci), to_cstring(cur_row_));
            break;
          }
          cell_counter++;
          if (is_row_finished)
          {
            rowkey = &(ci->row_key_);
            break;
          }
        }
        if (!is_row_finished
            && 0 != cell_counter)
        {
          YYSYS_LOG(ERROR, "unexpected row iter end, but irf is false, ret=%d cell_counter=%ld %s", ret, cell_counter, to_cstring(cur_row_));
          ret = OB_ERR_UNEXPECTED;
        }
        ret = (0 == cell_counter) ? OB_ITER_END : ret;
        rowkey = (OB_SUCCESS == ret) ? rowkey : NULL;
        row = (OB_SUCCESS == ret) ? &cur_row_ : NULL;
      }
      return ret;
    }

    int ObRowIterAdaptor::get_row_desc(const ObRowDesc *&row_desc) const
    {
      int ret = OB_SUCCESS;
      if (NULL == (row_desc = cur_row_.get_row_desc()))
      {
        ret = OB_NOT_INIT;
      }
      return ret;
    }

    int64_t ObRowIterAdaptor::to_string(char* buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      if (NULL != buf
          && 0 < buf_len)
      {
        int64_t plen = snprintf(buf, buf_len, "iter=%p allocator_total=%ld allocator_used=%ld deep_copy=%s",
                                cell_iter_, allocator_.total(), allocator_.used(), STR_BOOL(deep_copy_));
        pos = std::min(plen, buf_len);
        pos += cur_row_.to_string(buf + pos, buf_len - pos);
      }
      return pos;
    }

    void ObRowIterAdaptor::set_cell_iter(ObIterator *cell_iter, const ObRowDesc &row_desc, const bool deep_copy)
    {
      cell_iter_ = cell_iter;
      cur_row_.set_row_desc(row_desc);
      deep_copy_ = deep_copy;
      allocator_.reuse();
    }

    void ObRowIterAdaptor::reset()
    {
      cell_iter_ = NULL;
      cur_row_.reset(false, is_ups_row_ ? ObRow::DEFAULT_NOP : ObRow::DEFAULT_NULL);
      deep_copy_ = false;
      allocator_.free();
    }

    void ObRowIterAdaptor::reuse()
    {
      cell_iter_ = NULL;
      cur_row_.reset(false, is_ups_row_ ? ObRow::DEFAULT_NOP : ObRow::DEFAULT_NULL);
      deep_copy_ = false;
      allocator_.reuse();
    }


  }
}
