/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_expr_values.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_EXPR_VALUES_H
#define _OB_EXPR_VALUES_H 1
#include "sql/ob_no_children_phy_operator.h"
#include "common/ob_row_store.h"
#include "sql/ob_sql_expression.h"
#include "common/ob_array.h"
#include "common/ob_row_desc_ext.h"
//add dragon [varchar limit] 2016-8-12 14:42:44
#include "common/ob_schema_manager.h"
//add e
namespace oceanbase
{
  namespace sql
  {
    class ObExprValues: public ObNoChildrenPhyOperator
    {
      public:
        ObExprValues();
        virtual ~ObExprValues();

        int set_row_desc(const common::ObRowDesc &row_desc, const common::ObRowDescExt &row_desc_ext);
        int add_value(const ObSqlExpression &v);

        void reserve_values(int64_t num)
        {values_.reserve(num);}
        void set_check_rowkey_duplicate(bool flag)
        { check_rowkey_duplicat_ = flag; }
        //add by hushuang [Secondary Index]for replace bug 20161103
        void set_check_rowkey_duplicate_rep(bool flag)
        { check_rowkey_duplicat_rep_ = flag; }
        //add:e
        void set_do_eval_when_serialize(bool v)
        { do_eval_when_serialize_ = v;}
        //add fanqiushi_index
        void reset_iterator()
        {row_store_.reset_iterator();}
        //add:e
        ObExpressionArray &get_values()
        {return values_;}
        virtual int open();
        virtual int close();
        virtual void reset();
        virtual void reuse();
        virtual int get_next_row(const common::ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        enum ObPhyOperatorType get_type() const
        {return PHY_EXPR_VALUES;}
        DECLARE_PHY_OPERATOR_ASSIGN;
        NEED_SERIALIZE_AND_DESERIALIZE;
        //add wenghaixing[decimal] for fix delete bug 2014/10/10
        void set_del_upd();
        //add e

        //add dragon [varchar limit] 2016-8-12 11:13:40
        /**
         * @brief init 对内部变量进行一些初始化，目前只初始化内部指针manager_
         * @return 初始化成功返回成功；否则返回错误码
         */
        int init();
        /**
         * @brief check_self 对内部指针进行检查，该函数可扩展
         * @return 指针都被赋予相应初值返回成功；否则返回相应错误码
         */
        int check_self();
        /**
         * @brief alloc_schema_mgr 从ms schema manager处申请一份最新的schema manager
         * @param schema_mgr [out]
         * @return 申请成功返回成功；否则返回错误码
         */
        int alloc_schema_mgr(const ObSchemaManagerV2 *&schema_mgr);
        /**
         * @brief release_schema 释放申请的schema，引用计数减一
         * @param schema_mgr [in]
         * @return 成功返回OB_SUCCESS,否则返回错误码
         */
        int release_schema(const ObSchemaManagerV2 *schema_mgr);
        /**
         * @brief do_varchar_len_check 检查row中每个varvhar字段是否满足schema的要求
         * @param row [in]
         * @return 都满足返回成功；否则返回错误码
         */
        int do_varchar_len_check(ObRow &row);
        //add e

        //add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140715:b
        /*Exp:add sub_query's result to values*/
        // add by maosy [MultiUps 1.0] [batch_udi] 20170417 b
        int add_values_for_batch(const common::ObRow &value);
        int add_values(ObRow &value, bool is_table_level);
        // add by maosy e

        /*Exp:set sub_query's result row num*/
        void set_row_num(int64_t row_num)
        {row_num_ = row_num;}

        /*Exp: check whether sub_query's result is duplicate*/
        int check_duplicate();
        int store_input_values();//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20141024  /*Exp: reset the environment*/
        /*Exp: set the mark of sub_query*/
        void set_from_sub_query()
        {from_sub_query_ = true;}
        //add 20140715:e
        //add gaojt [Delete_Update_Function] [JHOBv0.1] 20151219:b
        int reset_stuff_for_insert();
        void set_from_ud(bool from_ud)
        {from_ud_ = from_ud;}
        //add gaojt 20151219:e
        //add shili [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20150705:b
        void row_store_reuse();//重置 row_store_中的值
        int eval(ObArray<ObRow> &row_store_array,common::ModuleArena & row_mem_allocator);
        int add_row_deep_copy(const ObRow &row,ObArray<ObRow> &row_store_array,common::ModuleArena & row_mem_allocator);
        int add_values(ObRow &val_row);
        void dump();
        //add 20150705:e
        //add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160321:b
        int get_row_desc_ext(const common::ObRowDescExt *&row_desc_ext) const;
        void clear_values(void);
        void reset_row_store_iterator();
        //add 20160321:e
        /*
        int eval_for_ud(ObArray<ObRow> &row_store_array,common::ModuleArena & row_mem_allocator);//批量的数据已经放在了row_stroe_
        */
        // add by maosy [MultiUps 1.0] [batch_udi] 20170417 b
        int64_t get_paxos_id_of_rowstore(int64_t index)
        {
            int cur_index = 0;
            for(int32_t i = 0 ; i < MAX_UPS_COUNT_ONE_CLUSTER ;i++)
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
        //add hongchen [PREPARE_REPLACE_BUG] 20170623:b
        void reset_evaled_mark()
        {had_evaled_ =false;}
        //add hongchen [PREPARE_REPLACE_BUG] 20170623:e
        // add by maosy  20170417 b

        int parse_decimal_illeagal_info(const ObObj obj, const uint64_t table_id, const uint64_t column_id, const int64_t row_num = 1);

        bool column_check(uint64_t table_id, uint64_t column_id, const ObObj *single_value);

      private:
        // types and constants
      private:
        // disallow copy
        ObExprValues(const ObExprValues &other);
        ObExprValues& operator=(const ObExprValues &other);
        // function members
        int eval();
      private:
        // data members
        ObExpressionArray values_;
        common::ObRowStore row_store_; //
        common::ObRowDesc row_desc_;
        common::ObRowDescExt row_desc_ext_;
        common::ObRow row_;
        bool from_deserialize_;
        bool check_rowkey_duplicat_;
        bool check_rowkey_duplicat_rep_;//add hushuang[Secondary Index] for replace bug 20161103
        bool do_eval_when_serialize_;

        //add wenghaixing DECIMAL OceanBase_BankCommV0.2 2014_6_5:b
        bool is_del_update;
        //add e
        //add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140715:b
        common::ObRowStore result_row_store_;//for sub query
        int64_t row_num_;
        bool from_sub_query_;
        //add 20140715:e
        //add gaojt [Delete_Update_Function] [JHOBv0.1] 20160226:b
        bool from_ud_;
        //add gaojt 20160226:e

        //add dragon [varchar limit] 2016-8-12 10:46:57
        //用于获取schema信息
        common::ObMergerSchemaManager* manager_; //别忘记释放访问的schema！！！
        //add e

      //add shili [MERGE_BUG_FIX]  20170210:b
      bool had_evaled_;
      //add e
      // add by maosy [MultiUps 1.0] [batch_udi] 20170417 b
      common::ObRowStore batch_row_stores_[MAX_UPS_COUNT_ONE_CLUSTER] ;
      common::ObBitSet<MAX_UPS_COUNT_ONE_CLUSTER> bit_paxos_;
      int64_t cur_paxosi_id_;
      common::ObArray<int64_t> paxos_ids_;
      // add by maosy 20170417 b

    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_EXPR_VALUES_H */
