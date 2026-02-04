/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_raw_row.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_RAW_ROW_H
#define _OB_RAW_ROW_H 1
#include "common/ob_define.h"
#include "common/ob_object.h"


namespace oceanbase
{
  namespace common
  {
    class ObRow;

    class ObRawRow
    {
      friend class ObRow;

      public:
        ObRawRow();
        ~ObRawRow();

        void assign(const ObRawRow &other);
        void clear();
        int add_cell(const common::ObObj &cell);

        int64_t get_cells_count() const;
        int get_cell(const int64_t i, const common::ObObj *&cell) const;
        int get_cell(const int64_t i, common::ObObj *&cell);
        int set_cell(const int64_t i, const common::ObObj &cell);


         //add shili [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20150705:b
        int64_t get_deep_copy_size() const;
        //@berif deep this(src)  to rhs(des)
        template <typename Allocator>
          int deep_copy(ObRawRow& rhs, Allocator& allocator) const
        {
          int ret = OB_SUCCESS;
          if(cells_count_>0&&NULL !=cells_)
          {
            int64_t obj_arr_len = cells_count_ * sizeof(ObObj);
            int64_t total_len = get_deep_copy_size();
            char* ptr = NULL;
            ObObj* obj_ptr = NULL;
            char* varchar_ptr = NULL;
            ObString varchar_val;
            uint64_t *decimal_val = NULL;
            int32_t varchar_len = 0;
            if (NULL == (ptr = (char*)allocator.alloc(total_len)))
            {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              YYSYS_LOG(WARN, "allocate mem for obj array fail:total_len[%ld]", total_len);
            }
            else
            {
              obj_ptr = new(ptr) ObObj[cells_count_];
              varchar_ptr = ptr + obj_arr_len;
            }
            for (int64_t i = 0; i < cells_count_ && OB_SUCCESS == ret; ++i)
            {
              obj_ptr[i] = cells_[i];  // copy object
              if (cells_[i].get_type() == ObVarcharType)
              {
                cells_[i].get_varchar(varchar_val);
                varchar_len = cells_[i].get_val_len();

                memcpy(varchar_ptr, varchar_val.ptr(), varchar_len);
                varchar_val.assign_ptr(varchar_ptr, varchar_len);

                obj_ptr[i].set_varchar(varchar_val);
                varchar_ptr += varchar_len;
              }
              else if(cells_[i].get_type() == ObDecimalType)
              {
                if (OB_SUCCESS != (ret = cells_[i].get_ttint(decimal_val)))
                {
                  YYSYS_LOG(ERROR, "get decimal pointer from obj_ptr_ error,ret = %d", ret);
                }
                else
                {
                  uint64_t *tmp = reinterpret_cast<uint64_t *>(varchar_ptr);
                  uint64_t decimal_len = 8 * cells_[i].get_nwords();
                  memcpy(tmp, decimal_val, decimal_len);
                  obj_ptr[i].set_decimal(tmp,cells_[i].get_precision(),cells_[i].get_scale(),
                                         cells_[i].get_vscale(), cells_[i].get_nwords());
                  varchar_ptr += decimal_len;
                }
              }
            }
            if(OB_SUCCESS == ret)
            {
              rhs.cells_=obj_ptr;
              rhs.cells_count_=cells_count_;
              rhs.reserved1_= reserved1_;
              rhs.reserved2_= reserved2_;
            }
          }
          else
          {
            rhs.cells_ = NULL;
            rhs.cells_count_ = 0;
          }
          return ret;
        }
        void dump(const int32_t log_level = YYSYS_LOG_LEVEL_DEBUG) const;
        //add 20150705:e
       

      private:
        // types and constants
        static const int64_t MAX_COLUMNS_COUNT = common::OB_ROW_MAX_COLUMNS_COUNT;
      private:
        const ObObj* get_obj_array(int64_t& array_size) const;
        // disallow copy
        ObRawRow(const ObRawRow &other);
        ObRawRow& operator=(const ObRawRow &other);
        // function members
      private:
        // data members
        char cells_buffer_[MAX_COLUMNS_COUNT * sizeof(ObObj)];
        common::ObObj *cells_;
        int16_t cells_count_;
        int16_t reserved1_;
        int32_t reserved2_;
    };

    inline int ObRawRow::get_cell(const int64_t i, const common::ObObj *&cell) const
    {
      int ret = common::OB_SUCCESS;
      if (0 > i || i >= MAX_COLUMNS_COUNT)
      {
        YYSYS_LOG(WARN, "invalid index, count=%hd i=%ld", cells_count_, i);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        cell = &cells_[i];
      }
      return ret;
    }

    inline int ObRawRow::get_cell(const int64_t i, common::ObObj *&cell)
    {
      int ret = common::OB_SUCCESS;
      if (0 > i || i >= MAX_COLUMNS_COUNT)
      {
        YYSYS_LOG(WARN, "invalid index, count=%hd i=%ld", cells_count_, i);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        cell = &cells_[i];
      }
      return ret;
    }

    inline int ObRawRow::set_cell(const int64_t i, const common::ObObj &cell)
    {
      int ret = common::OB_SUCCESS;
      if (0 > i || i >= MAX_COLUMNS_COUNT)
      {
        YYSYS_LOG(WARN, "invalid index, count=%ld i=%ld", MAX_COLUMNS_COUNT, i);
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        cells_[i] = cell;
        if (i >= cells_count_)
        {
          cells_count_ = static_cast<int16_t>(1+i);
        }
      }
      return ret;
    }

    inline int64_t ObRawRow::get_cells_count() const
    {
      return cells_count_;
    }

    inline void ObRawRow::clear()
    {
      cells_count_ = 0;
    }

    inline const ObObj* ObRawRow::get_obj_array(int64_t& array_size) const
    {
      array_size = cells_count_;
      return cells_;
    }
  } // end namespace common
} // end namespace oceanbase

#endif /* _OB_RAW_ROW_H */

