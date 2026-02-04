/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_row.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_ROW_H
#define _OB_ROW_H 1
#include "common/ob_array.h"
#include "ob_raw_row.h"
#include "ob_row_desc.h"
#include "ob_rowkey.h"

namespace oceanbase
{
  namespace common
  {
    /// ��
    class ObRow
    {
      public:
        enum DefaultValue
        {
          DEFAULT_NULL,
          DEFAULT_NOP
        };

      public:
        ObRow();
        virtual ~ObRow();

        void clear()
        {
          raw_row_.clear();
          row_desc_ = NULL;
        }
		    //add shili [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20150705:b
        int64_t get_deep_copy_size() const;

        //notice: this copy to  rhs(des)  rhs ΪĿ��
        template <typename Allocator>
        int deep_copy(ObRow &rhs, Allocator &allocator) const  //��ȿ���
        {
          int ret = OB_SUCCESS;
          if(OB_SUCCESS != (ret = rowkey_.deep_copy(rhs.rowkey_, allocator)))
          {
            YYSYS_LOG(WARN, "fail to deep copy rowkey_,ret=%d",ret);
          }
          else
          {
            if(OB_SUCCESS != (ret = raw_row_.deep_copy(rhs.raw_row_, allocator)))
            {
              YYSYS_LOG(WARN, "fail to deep copy rowkey_,ret=%d",ret);
            }
          }
          if(OB_SUCCESS == ret)
          {
            rhs.paxos_id_ = paxos_id_;
            rhs.row_desc_ = this->row_desc_;
          }
          return ret;
        }
        //add 20150705:e

        inline void set_paxos_id(int64_t paxos_id) //set paxos id
        {
          paxos_id_=paxos_id;
        }

        inline int64_t get_paxos_id() const
        {
          return paxos_id_;
        }
        //add 20150705:e

        /// ��ֵ��ǳ�������ر�ģ�����varchar���Ͳ�����������
        void assign(const ObRow &other);
        ObRow(const ObRow &other);
        ObRow &operator= (const ObRow &other);
        /**
         * ����������
         * �������������ڹ���һ�����ж���ʱ��ʼ����ObRow�Ĺ���Ҫʹ�õ�ObRowDesc�Ĺ��ܣ����ObRow������Թ���һ��ObRowDesc����
         * @param row_desc
         */
        virtual void set_row_desc(const ObRowDesc &row_desc);
        /**
         * ��ȡ������
         */
        const ObRowDesc* get_row_desc() const;
        /**
         * ���ݱ�ID����ID���cell
         *
         * @param table_id
         * @param column_id
         * @param cell [out]
         *
         * @return
         */
        int get_cell(const uint64_t table_id, const uint64_t column_id, const common::ObObj *&cell) const;
        int get_cell(const uint64_t table_id, const uint64_t column_id, common::ObObj *&cell);
        /**
         * ����ָ���е�ֵ
         */
        int set_cell(const uint64_t table_id, const uint64_t column_id, const common::ObObj &cell);
        /**
         * ��ɱ��е���Ԫ����
         */

        int64_t get_column_num() const;


        /**
         * ��õ�cell_idx��cell
         */
        int raw_get_cell(const int64_t cell_idx, const common::ObObj *&cell,
            uint64_t &table_id, uint64_t &column_id) const;
        inline int raw_get_cell(const int64_t cell_idx, const common::ObObj* &cell) const;
        inline int raw_get_cell_for_update(const int64_t cell_idx, common::ObObj *&cell);

        /// ���õ�cell_idx��cell
        int raw_set_cell(const int64_t cell_idx, const common::ObObj &cell);

        int get_rowkey(const ObRowkey *&rowkey) const;

        /**
         * �Ƚ�����row�Ĵ�С
         * @param row
         * @return
         * (1) if (this == row) return 0;
         * (2) if (this >  row) return POSITIVE_VALUE;
         * (3) if (this <  row) return NEGATIVE_VALUE;
         */
        int compare(const ObRow &row) const;

        /* dump row data */
        void dump() const;

        /* if skip_rowkey == true DO NOT reset rowkey */
        virtual int reset(bool skip_rowkey, enum DefaultValue default_value);

        /*
         * ͨ��action_flag_column�ж���һ���Ƿ�Ϊ��
         * ע�⣺û��action_flag_column���ж��᷵��false
         */
        int get_is_row_empty(bool &is_row_empty) const;
        //add shili [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20150710:b
        int is_empty_row(bool &is_nop_row) const;
        int is_delete_row(bool &is_delete_row) const;
        int set_action_flag_column(const int64_t action_flag);
        //add 20150710:e
        // add by maosy [Delete_Update_Function_isolation_RC] 20161228
        /*�õ���һ�е�action_flag�����û��action_flag��һ���򷵻ش��������򷵻�ob_success��action_flag*/
        int get_action_flag(int64_t &action_flag) const;
        // add e
        int64_t to_string(char* buf, const int64_t buf_len) const;
        friend class ObCompactCellWriter;
        friend class ObRowFuse;

      private:
        const ObObj* get_obj_array(int64_t& array_size) const;

      protected:
        ObRowkey rowkey_;
        ObRawRow raw_row_;
        const ObRowDesc *row_desc_;
        int64_t paxos_id_;   //add shili [MultiUPS] [PHYSICAL_PLAN_TRANSFORM]

    };

    inline const ObRowDesc* ObRow::get_row_desc() const
    {
      return row_desc_;
    }

    inline int64_t ObRow::get_column_num() const
    {
      int64_t ret = -1;
      if (NULL == row_desc_)
      {
        YYSYS_LOG(ERROR, "row_desc_ is NULL");
      }
      else
      {
        ret = row_desc_->get_column_num();
      }
      return ret;
    }

    inline void ObRow::set_row_desc(const ObRowDesc &row_desc)
    {
      if (row_desc.get_rowkey_cell_count() > 0)
      {
        rowkey_.assign(raw_row_.cells_, row_desc.get_rowkey_cell_count());
      }
      row_desc_ = &row_desc;
    }

    inline int ObRow::raw_get_cell_for_update(const int64_t cell_idx, common::ObObj *&cell)
    {
      int ret = OB_SUCCESS;
      if (OB_UNLIKELY(cell_idx >= get_column_num()))
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(WARN, "invalid cell_idx=%ld cells_count=%ld", cell_idx, get_column_num());
      }
      else
      {
        ret = raw_row_.get_cell(cell_idx, cell);
      }
      return ret;
    }

    inline int ObRow::raw_get_cell(const int64_t cell_idx, const common::ObObj *&cell) const
    {
      int ret = OB_SUCCESS;
      if (OB_UNLIKELY(cell_idx >= get_column_num()))
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(WARN, "invalid cell_idx=%ld cells_count=%ld", cell_idx, get_column_num());
      }
      else
      {
        ret = raw_row_.get_cell(cell_idx, cell);
      }
      return ret;
    }

    inline const ObObj* ObRow::get_obj_array(int64_t& array_size) const
    {
      return raw_row_.get_obj_array(array_size);
    }


    //add shili [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20150714:b
    //@berif ���ڴ洢paxos_id ��ͬ�� ������
    struct ObSubRowStore
    {
      ObSubRowStore():paxos_id_(-1)
      {}
      ~ObSubRowStore()
      {
        row_store_.~ObArray();
      }
      ObArray<ObRow> row_store_;
      void reset()
      {
        row_store_.clear();
      }

      int64_t paxos_id_;
      inline int add_row(const ObRow &row)
      {
        int ret=OB_SUCCESS;
        ret=row_store_.push_back(row);
        return  ret;
      }
    };
    //add 20150714:e


  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_ROW_H */
