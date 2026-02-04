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
 */

#ifndef OCEANBASE_COMMON_OB_NEW_SCANNER_H_
#define OCEANBASE_COMMON_OB_NEW_SCANNER_H_

#include "ob_define.h"
#include "ob_read_common_data.h"
#include "ob_row_iterator.h"
#include "ob_rowkey.h"
#include "ob_range2.h"
#include "page_arena.h"
#include "ob_string_buf.h"
#include "ob_object.h"
#include "ob_row.h"
#include "ob_row_store.h"
#include "ob_se_array.h"
#include "ob_schema.h"
namespace oceanbase
{
  namespace common
  {
    //add shili, [MultiUPS] [COLUMN_VERSION_MERGE]  20170423
    struct ObColumnVersion
    {
        ObColumnVersion();
        ObColumnVersion(int64_t column_id,int64_t version);
        int64_t to_string(char* buffer, int64_t length) const;

        int64_t  column_id_;
        int64_t  version_;
        NEED_SERIALIZE_AND_DESERIALIZE;
    };

    /*�а汾���� ��������е��а汾����*/
    struct ObRowVersion
    {
        ObRowVersion();
        ObRowVersion& operator=(const ObRowVersion &other);

        void reset();
        bool is_deleted_row() const;
        int64_t get_delete_version() const;
        int  get_column_version(const int64_t column_id,int64_t& version,int64_t& index) const;
        int  add_column_version(const int64_t column_id,const int64_t version);
        int  update_column_version(const int64_t column_id,const int64_t version,const int64_t index=OB_INVALID_INDEX);
        int64_t to_string(char* buffer, int64_t length) const;

        bool is_nop_version() const; //add for [212]
        NEED_SERIALIZE_AND_DESERIALIZE;

        int64_t column_num_;
        ObColumnVersion column_versions_[common::OB_MAX_COLUMN_NUMBER];
    };
    //add e

    class ObNewScanner : public ObRowIterator
    {
      public:
        static const int64_t DEFAULT_MAX_SERIALIZE_SIZE = OB_MAX_PACKET_LENGTH - OB_MAX_ROW_KEY_LENGTH * 2 - 1024;
      public:
        ObNewScanner();
        virtual ~ObNewScanner();

        virtual void clear();
        virtual void reuse();

        int64_t set_mem_size_limit(const int64_t limit);

        /// @brief add a row in the ObNewScanner
        /// @retval OB_SUCCESS successful
        /// @retval OB_SIZE_OVERFLOW memory limit is exceeded if add this row
        /// @retval otherwise error
        int add_row(const ObRow &row);

        int add_row(const ObRowkey &rowkey, const ObRow &row);

        //add shili, [MultiUPS] [COLUMN_VERSION_MERGE]  20170423:b
        int add_row_version(const ObRowVersion & version);
        void set_row_version_ptr(ObRowVersion * version);
        //add e

        /// @retval OB_ITER_END iterate end
        /// @retval OB_SUCCESS go to the next cell succ
        int get_next_row(ObRow &row);

        int get_next_row(const ObRowkey *&rowkey, ObRow &row);

        /* @brief set default row desc which will auto filt into row
         * when invoke get_next_row if row desc of the given parameter
         * row is NULL. */
        void set_default_row_desc(const ObRowDesc *row_desc)
        { default_row_desc_ = row_desc; }

        bool is_empty() const;
        /// after deserialization, get_last_row_key can retreive the last row key of this scanner
        /// @param [out] row_key the last row key
        int get_last_row_key(ObRowkey &row_key) const;
        int set_last_row_key(ObRowkey &row_key);
        int serialize_meta_param(char * buf, const int64_t buf_len, int64_t & pos) const;
        int deserialize_meta_param(const char* buf, const int64_t data_len, int64_t& pos, ObObj &last_obj);
        //add lijianqiang [MultiUPS] [SELECT_MERGE] 20151228:b
        int serialize_row_version(char * buf, const int64_t buf_len, int64_t & pos) const;
        int deserialize_row_version(const char* buf, const int64_t data_len, int64_t& pos, ObObj &last_obj);
        int64_t get_row_version_serialize_size(void) const;
        //add 20151228:e
        NEED_SERIALIZE_AND_DESERIALIZE;

      public:
        /// indicating the request was fullfiled, must be setted by cs and ups
        /// @param [in] is_fullfilled  ���������㵼��scanner��ֻ�������ֽ����ʱ������Ϊfalse
        /// @param [in] fullfilled_row_num -# when getting, this means processed position in GetParam
        ///                                 -# when scanning, this means row number fullfilled
        int set_is_req_fullfilled(const bool &is_fullfilled, const int64_t fullfilled_row_num);
        int get_is_req_fullfilled(bool &is_fullfilled, int64_t &fullfilled_row_num) const;

        /// when response scan request, range must be setted
        /// the range is the seviced range of this tablet or (min, max) in updateserver
        /// @param [in] range
        int set_range(const ObNewRange &range);
        /// same as above but do not deep copy keys of %range, just set the reference.
        /// caller ensures keys of %range remains reachable until ObNewScanner serialized.
        int set_range_shallow_copy(const ObNewRange &range);
        int get_range(ObNewRange &range) const;

        inline void set_data_version(const int64_t version)
        {
          data_version_ = version;
        }

        inline int64_t get_data_version() const
        {
          return data_version_;
        }

        //add lijianqiang [MultiUPS] [SELECT_MERGE] 20151228:b
        inline void set_has_row_version(bool has_row_version)
        {
          //          YYSYS_LOG(ERROR,"test::ljq::set has row version");
          has_row_version_ = has_row_version;
        }
        inline bool get_has_row_version(void)
        {
          return has_row_version_;
        }
        inline ObScannerType& get_my_scanner_type(void)
        {
          return scanner_type_;
        }

        inline int64_t get_cur_row_version(int64_t index)
        {
          OB_ASSERT(index  >= 0 && index < row_version_.size());
          return row_version_.at(int32_t(index));
        }
        //add 20151228:e

        //add shili, [MultiUPS] [COLUMN_VERSION_MERGE]  20170423:b
        inline const ObRowVersion  get_cur_row_version_v2(int64_t index)
        {
          int32_t idx = static_cast<int32_t>(index);
          OB_ASSERT(idx  >= 0 && idx < row_begin_idx_.size());
          ObRowVersion row_versoin;
          int64_t begin = row_begin_idx_.at(idx);
          int32_t end = column_id_list_.size();
          if (idx < row_begin_idx_.size() - 1)
          {
            end = static_cast<int32_t>(row_begin_idx_.at(idx + 1));
            //row_versoin.column_num_ = row_begin_idx_.at(index + 1) - begin;
          }
          row_versoin.column_num_ = end - begin;
          int64_t idxx = 0;
          for (; begin < end; ++begin)
          {
            row_versoin.column_versions_[idxx].column_id_ = column_id_list_.at(static_cast<int32_t>(begin));
            row_versoin.column_versions_[idxx].version_   = column_version_list_.at(static_cast<int32_t>(begin));
            ++idxx;
          }
          return row_versoin;
        }
        //add e

        /* ��ȡ����ռ�õĿռ��ܴ�С��������δʹ�õĻ�����) */
        inline int64_t get_used_mem_size() const
        {
          return row_store_.get_used_mem_size();
        }
        /** ��ȡʵ�����ݵĴ�С
        */
        inline int64_t get_size() const
        {
          return cur_size_counter_;
        }

        inline int64_t get_row_num() const
        {
          return cur_row_num_;
        }

        int64_t to_string(char* buffer, const int64_t length) const;
        void dump(void) const;
        void dump_all(int log_level) const;
        //add by maosy  [MultiUps 1.0] [secondary index optimize]20170401 b:
        void set_rowkey_info( ObRowkeyInfo rowkey)
        {
          rowkey_info_= rowkey ;
        }
        // add by maosy e
        //add shili [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20150710:b
        void assign(const ObNewScanner & other);
        inline bool has_range()
        { return has_range_; }
        //add 20150710:e
      private:
        int deserialize_basic_(const char* buf, const int64_t data_len, int64_t& pos, ObObj &last_obj);

        int deserialize_table_(const char* buf, const int64_t data_len, int64_t& pos, ObObj &last_obj);

        static int deserialize_int_(const char* buf, const int64_t data_len, int64_t& pos,
                                    int64_t &value, ObObj &last_obj);

        static int deserialize_varchar_(const char* buf, const int64_t data_len, int64_t& pos,
                                        ObString &value, ObObj &last_obj);

        static int deserialize_int_or_varchar_(const char* buf, const int64_t data_len, int64_t& pos,
                                               int64_t& int_value, ObString &varchar_value, ObObj &last_obj);

        int64_t get_meta_param_serialize_size() const;

      protected:
        ObRowStore row_store_;
        int64_t cur_size_counter_;
        int64_t mem_size_limit_;
        int64_t data_version_;
        bool has_range_;
        bool is_request_fullfilled_;
        int64_t  fullfilled_row_num_;
        int64_t cur_row_num_;
        ObNewRange range_;
        ObRowkey last_row_key_;
        ModulePageAllocator mod_;
        mutable ModuleArena rowkey_allocator_;
        const ObRowDesc* default_row_desc_;
        ObRowkeyInfo rowkey_info_;      //add by maosy  [MultiUps 1.0] [secondary index optimize]20170401 b:

        //        const ObRowDesc *  row_desc_;  //add shili [MultiUPS] [PHYSICAL_PLAN_TRANSFORM]
        //add lijianqiang [MultiUPS] [SELECT_MERGE] 20151228:b
        bool has_row_version_;//to indicate if need to serialize the version in row level
        ObVector<int64_t> row_version_;//the final version for each row
        common::ObScannerType scanner_type_;
        //add 20151228:e

        //add shili, [MultiUPS] [COLUMN_VERSION_MERGE]  20170423:b
        //ObArray<ObRowVersion>  row_version_list_;  //scanner �������еİ汾
        ObVector<int64_t>  row_begin_idx_;        //ÿһ��
        ObVector<int64_t>  column_id_list_;       //scanner �������еİ汾
        ObVector<int64_t>  column_version_list_;  //��column_id_list_ һһ��Ӧ
        ObRowVersion      * cur_row_version_ptr_;
        //add e
    };



    class ObCellNewScanner : public ObNewScanner
    {
      public:
        ObCellNewScanner()
          : row_desc_(NULL),
            final_row_desc_(NULL),
            last_table_id_(OB_INVALID_ID),
            ups_row_valid_(false),
            is_size_overflow_(false)
        {
        }
        ~ObCellNewScanner()
        {}
      public:
        // compatible with ObScanner's interface;
        int add_cell(const ObCellInfo &cell_info,
                     const bool is_compare_rowkey = true, const bool is_rowkey_change = false);
        int finish();
        void clear();
        void reuse();
        void set_row_desc(const ObRowDesc & row_desc);
        int set_is_req_fullfilled(const bool &is_fullfilled, const int64_t fullfilled_row_num);
        
        //do not need rollback
        inline int rollback()
        {
          return OB_SUCCESS;
        }
        void set_final_row_desc(ObRowDesc* row_desc)
        {
          final_row_desc_ = row_desc;
        }

      private:
        int add_current_row();

      private:
        const ObRowDesc* row_desc_;
        ObRowDesc* final_row_desc_; //add hongchen
        uint64_t last_table_id_;
        ObUpsRow ups_row_;
        ObStringBuf str_buf_;
        ObRowkey cur_rowkey_;
        bool ups_row_valid_;
        bool is_size_overflow_;

        //add shili, [MultiUPS] [COLUMN_VERSION_MERGE]  20170423
        ObRowVersion   cur_row_version_;    //��ǰ �����ݵİ汾
        //add e
    };

  } /* common */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_COMMON_OB_NEW_SCANNER_H_ */
