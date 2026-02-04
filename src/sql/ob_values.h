/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_values.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_VALUES_H
#define _OB_VALUES_H 1

#include "sql/ob_single_child_phy_operator.h"
#include "common/ob_row_store.h"
//add wenghaixing[decimal] for fix delete bug 2014/10/15
#include "common/ob_se_array.h"
#include "common/ob_define.h"
//add e
namespace oceanbase
{
  namespace sql
  {
    //add wenghaixing[decimal] for fix delete bug 2014/10/15
    class ObValuesKeyInfo
    {

    public:

        ObValuesKeyInfo();
        ~ObValuesKeyInfo();
        void set_key_info(uint64_t tid,uint64_t cid,uint32_t p,uint32_t s,common::ObObjType type);
        void get_type(common::ObObjType& type);
        void get_key_info(uint32_t& p,uint32_t& s);

        bool is_rowkey(uint64_t tid,uint64_t cid);
    private:
        uint32_t precision_;
        uint32_t scale_;
        uint64_t cid_;
        uint64_t tid_;
        common::ObObjType type_;
    };
    inline void ObValuesKeyInfo::set_key_info(uint64_t tid,uint64_t cid,uint32_t p,uint32_t s,common::ObObjType type)
    {
        tid_=tid;
        cid_=cid;
        type_=type;
        precision_=p;
        scale_=s;
    }
    inline void ObValuesKeyInfo::get_type(common::ObObjType& type)
    {
        type=type_;
    }
    inline void ObValuesKeyInfo::get_key_info(uint32_t& p,uint32_t& s)
    {
        p=precision_;
        s=scale_;
    }
    inline bool ObValuesKeyInfo::is_rowkey(uint64_t tid,uint64_t cid)
    {
        bool ret =false;
        if(tid==tid_&&cid==cid_)ret=true;
        return ret;
    }
    //add e
    class ObValues: public ObSingleChildPhyOperator
    {
      public:
        ObValues();
        virtual ~ObValues();
        virtual void reset();
        virtual void reuse();
        int set_row_desc(const common::ObRowDesc &row_desc);
        int add_values(const common::ObRow &value);
        const common::ObRowStore &get_row_store() /*{return row_store_;};*/
		//add by maosy [MultiUPS 1.0] [batch_iud]20170525 b:
        {
            if(cur_paxosi_id_ != common::OB_INVALID_PAXOS_ID )
            {
                return batch_row_stores_[cur_paxosi_id_];
            }
            return row_store_;
        };
		// add by maosy e
        virtual int open();
        virtual int close();
        virtual int get_next_row(const common::ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        enum ObPhyOperatorType get_type() const
        {return PHY_VALUES;}
        DECLARE_PHY_OPERATOR_ASSIGN;
        NEED_SERIALIZE_AND_DESERIALIZE;
        //add wenghaixing[decimal] for fix delete bug 2014/10/10
        void set_fix_obvalues();
        void add_rowkey_array(uint64_t tid,uint64_t cid,common::ObObjType type,uint32_t p,uint32_t s);
        bool is_rowkey_column(uint64_t tid,uint64_t cid);
        int get_rowkey_schema(uint64_t tid,uint64_t cid,common::ObObjType& type,uint32_t& p,uint32_t& s);
        typedef common::ObSEArray<ObValuesKeyInfo, common::OB_MAX_ROWKEY_COLUMN_NUMBER> RowkeyInfo;
        //add e
        //add shili [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20150717:b
        int get_row_store_array(common::ObArray<common::ObRow> &row_store_array,common::ModuleArena & row_mem_allocator);
        int add_row_deep_copy(common::ObRow &row,common::ObArray<common::ObRow> &row_store_array,common::ModuleArena & row_mem_allocator);
        void row_store_reuse();
        //add 20150717:e
		//add gaojt [Delete_Update_Function] [JHOBv0.1] 20161108:b
        int  add_values_for_ud(common::ObRow *value, bool is_table_level=true);
        void set_rowkey_num(int64_t rowkey_num);
        void set_table_id(uint64_t table_id);
        int  set_column_ids(uint64_t column_id);
        //add gaojt 20161108:e

        // add by maosy [MultiUps 1.0] [batch_udi] 20170417 b
        void add_current_row_desc();
        int64_t get_sub_trans_num ()
        {
            int64_t count = 0 ;
            for(int i = 0 ; i < common::MAX_UPS_COUNT_ONE_CLUSTER ;i++)
            {
                if(bit_paxos_.has_member(i))
                {
                    count++;
                }
            }
            return count;
        }
        int64_t get_paxos_id_of_rowstore(int64_t index)
        {
            int cur_index = 0;
             for(int i = 0 ; i < common::MAX_UPS_COUNT_ONE_CLUSTER ;i++)
             {
                 if(bit_paxos_.has_member(i))
                 {
                     if(cur_index ==index)
                     {
                         cur_paxosi_id_ = i;
                         break;
                     }
                     cur_index ++;
                 }
             }
            return cur_paxosi_id_;
        }
        int set_paxos_id (int64_t paxos_id)
        {
            return paxos_ids_.push_back(paxos_id);
        }
        // add by maosy  20170417 e
        // add e
      private:
        // types and constants
        int load_data();
      private:
        // disallow copy
        ObValues(const ObValues &other);
        ObValues& operator=(const ObValues &other);
        // function members
      private:
        // data members
        common::ObRowDesc row_desc_;
        common::ObRow curr_row_;
        common::ObRowStore row_store_;
        //add wenghaixing[decimal] for fix delete bug 2014/10/10
        bool is_need_fix_obvalues;
        RowkeyInfo obj_array_;
        //e
        //add gaojt [Delete_Update_Function] [JHOBv0.1] 20161108:b
        int64_t  rowkey_num_;
        uint64_t table_id_;
        common::ObArray<uint64_t> column_ids_;
        //add gaojt 20161108:e
        // add by maosy [MultiUps 1.0] [batch_udi] 20170417 b
        common::ObRowStore batch_row_stores_[common::MAX_UPS_COUNT_ONE_CLUSTER];
        common::ObBitSet<common::MAX_UPS_COUNT_ONE_CLUSTER> bit_paxos_;
        int64_t cur_paxosi_id_ ;
        common::ObArray<int64_t> paxos_ids_;
        common::ModuleArena row_mem_allocator_;
		// add by maosy 20170417 e
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_VALUES_H */
