/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_fill_values.h
 *
 * Authors:
 *   gaojt
 * function:
 *   Delete_Update_Function
 *
 */
#ifndef _OB_FILL_VALUES_H
#define _OB_FILL_VALUES_H 1
#include "ob_multi_children_phy_operator.h"
#include "ob_sql_expression.h"
#include "common/ob_schema.h"
#include "ob_table_rpc_scan.h"
#include "ob_expr_values.h"
#include "common/ob_row.h"
#include "common/ob_row_desc.h"
#include "obmysql/ob_mysql_server.h"
#include "ob_sequence_update.h"
namespace oceanbase
{
  namespace sql
  {
    class ObFillValues: public ObMultiChildrenPhyOperator
    {
      public:
        ObFillValues();
        virtual ~ObFillValues();

        int set_row_desc(const common::ObRowDesc &row_desc);
        virtual int open();
        virtual int close();
        virtual void reset();
        virtual void reuse();
        virtual int get_next_row(const common::ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        int fill_values(ObValues*& tmp_value,
                        ObProject *& select_result,
                        const ObRowkeyInfo &rowkey_info,
                        ObExprValues*& get_param_values,
                        const ObRowDesc &row_desc,
                        bool& is_close_sub_query);
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;

        void reset_for_prepare_multi();
        void set_rowkey_info(common::ObRowkeyInfo rowkey_info)
        {rowkey_info_ = rowkey_info;};
        void set_is_column_hint_index(bool is_column_hint_index)
        {is_column_hint_index_ = is_column_hint_index;};
        void set_sql_context(ObSqlContext sql_context)
        {sql_context_ = sql_context;};
        bool is_ud_non_row()
        {return is_row_num_null_;};
        enum ObPhyOperatorType get_type() const
        {return PHY_FILL_VALUES;}

        void set_sequence_update(ObPhyOperator *sequence_op)
        {sequence_update_ = sequence_op;}

        int64_t get_affect_row()
        {    return affect_row_;};
        bool is_multi_batch_over()
        { return is_multi_batch_over_;};
        void set_max_row_value_size(int64_t max_row_value_size)
        {
            max_packet_size_ = max_row_value_size;
        }
        //add:evoid reset_for_prepare_multi();
        int clear_prepare_select_result();
        bool is_already_clear()
        {return is_already_clear_;};
        int add_row_to_obvalues(ObValues *&tmp_value, common::ObRow* row);
        void add_modify_cid(uint64_t column_id )
        {
            modify_time_cid_ = column_id ;
        }
        DECLARE_PHY_OPERATOR_ASSIGN;
    private :
		// add by maosy [MultiUps 1.0] [batch_udi] 20170417 b
        int get_partiton_type(uint64_t table_id,bool &is_table_level);
        int get_paxos_id (ObRow *row);
		// add by maosy 20170417 e
      private:
        // disallow copy
        ObFillValues(const ObFillValues &other);
        ObFillValues& operator=(const ObFillValues &other);
      private:
        static const int64_t MAX_UDPATE_DELETE_VALUE_SIZE = static_cast<int64_t>(0.8*2*1024L*1024L);
      private:
        int64_t max_row_num_ ;
        int64_t max_packet_size_;
        common::ObRowkeyInfo rowkey_info_;
        common::ObRowDesc row_desc_;
        bool is_row_num_null_;
        bool is_column_hint_index_;
        ObPhyOperator * sequence_update_;//add lijianqiang [sequence update] 20160319
        bool is_reset_;/*Exp: whether reset the environment*/
        bool is_close_sub_query_;
        ObSqlContext sql_context_;
        int64_t affect_row_;
        bool is_multi_batch_over_;
        bool is_already_clear_;
        bool is_table_level_;	// add by maosy [MultiUps 1.0] [batch_udi] 20170417
        uint64_t modify_time_cid_;
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_FILL_VALUES_H */
