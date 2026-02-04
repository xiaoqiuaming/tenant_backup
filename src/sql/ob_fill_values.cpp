/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   gaojt
 * function:
 * Delete_Update_Function
 *
 */
#include "ob_fill_values.h"
#include "common/utility.h"
#include "common/ob_obj_cast.h"
#include "common/hash/ob_hashmap.h"
#include "ob_iud_loop_control.h"
#include "ob_raw_expr.h"
#include "ob_values.h"


using namespace oceanbase::sql;
using namespace oceanbase::common;
ObFillValues::ObFillValues()
    :
      max_row_num_(256),
      max_packet_size_(MAX_UDPATE_DELETE_VALUE_SIZE),
      is_row_num_null_(false),
      is_column_hint_index_(false),
      //add lijianqiang [sequence update] 20160319:b
      sequence_update_(NULL),
      //add 20160319:e
      is_reset_(false),
      is_close_sub_query_(true), affect_row_(0),is_multi_batch_over_(false),is_already_clear_(false),is_table_level_(false)	// add by maosy [MultiUps 1.0] [batch_udi] 20170417 b
    ,modify_time_cid_(OB_INVALID_ID)
{

}

ObFillValues::~ObFillValues()
{

}
void ObFillValues::reset_for_prepare_multi()
{
    is_reset_ = false;
    is_multi_batch_over_ = false;
    is_row_num_null_ = false;
    is_close_sub_query_ = true;
    affect_row_ = 0;
}
void ObFillValues::reset()
{
    is_row_num_null_ = false;
    is_column_hint_index_ = false;
    row_desc_.reset();
    is_reset_ = false;
    is_close_sub_query_ = true;
    affect_row_ = 0;
    is_multi_batch_over_=false;
    max_row_num_=256;
    max_packet_size_ = MAX_UDPATE_DELETE_VALUE_SIZE;
    is_already_clear_ = false;
    is_table_level_ =false;	// add by maosy [MultiUps 1.0] [batch_udi] 20170417 b
    modify_time_cid_ = OB_INVALID_ID ;
}

void ObFillValues::reuse()
{
    is_row_num_null_ = false;
    is_column_hint_index_ = false;
    row_desc_.reset();
    is_reset_ = false;
    is_close_sub_query_ = true;
    affect_row_=0;
    is_multi_batch_over_=false;
    max_row_num_=256;
    max_packet_size_ = MAX_UDPATE_DELETE_VALUE_SIZE;
    is_already_clear_ = false;
    is_table_level_ =false; // add by maosy [MultiUps 1.0] [batch_udi] 20170417 b
    modify_time_cid_ = OB_INVALID_ID ;
}

int ObFillValues::set_row_desc(const common::ObRowDesc &row_desc)
{
    row_desc_ = row_desc;
    return OB_SUCCESS;
}
/*
 * first,check the valid of children operator
 * then,call the fill_values function, filling the ObExprValues
 *      and the child of ObValues operator with values fetched
 *      from the first child of ObFillValues
 * last,if there is none row to delete or update,don't open the
 *      second and third child
 */
int ObFillValues::open()
{
    int ret = OB_SUCCESS;
    affect_row_=0;
    ObValues* tmp_value = dynamic_cast<ObValues*>(get_child(0));
    ObExprValues* expr_values = dynamic_cast<ObExprValues*>(get_child(1));
    ObProject * select_result = dynamic_cast<ObProject*>(get_child(2));
    if(NULL == tmp_value || NULL == expr_values || NULL ==select_result)
    {
        ret = OB_ERROR;
        YYSYS_LOG(WARN,"tmp_value[%p] or expr_values[%p] or select [%p]",
                  tmp_value,expr_values,select_result);
    }
    else if(is_reset_)
    {
        expr_values->reset_stuff_for_insert();
    }
    else
    {//一条语句只会走一次
        tmp_value->add_current_row_desc();
        sql_context_.session_info_->set_read_times (FIRST_SELECT_READ);
        uint64_t table_id = get_phy_plan()->get_table_id();	// add by maosy [MultiUps 1.0] [batch_udi] 20170417 
        if (OB_SUCCESS != (ret = select_result->open()))
        {
            YYSYS_LOG(ERROR,"fail to open the constucted select-result-set,ret = %d",ret);
        }
        // add by maosy [MultiUps 1.0] [batch_udi] 20170417 b
        else if(OB_SUCCESS !=(ret = get_partiton_type(table_id,is_table_level_)))
        {
            YYSYS_LOG(WARN,"failed to get partition type ,ret = %d,table_id =%lu",ret ,table_id);
        }
        // add by maosy 20170417 b
        else
        {
            is_close_sub_query_ = false;
            is_reset_ = true;
            int64_t cell_num  =0;
            cell_num = row_desc_.get_column_num();
            cell_num = cell_num >=4 ? cell_num:4;
            max_row_num_ = 20164/(cell_num+2);
            YYSYS_LOG(DEBUG,"MAX CELL ROW SIZE=%ld",max_row_num_);
            ObUpsModifyWithDmlType *ups_modify = NULL;
            ObPhysicalPlan * phy_plan = get_phy_plan ();
            int64_t phy_count = phy_plan->get_operator_size ();
            for(int64_t i =0  ; i <phy_count ;i++)
            {
                if(NULL != (ups_modify = dynamic_cast<ObUpsModifyWithDmlType *>(phy_plan->get_phy_operator (i))))
                {
                    max_packet_size_ -= ups_modify->get_child (0)->get_serialize_size ();
                    break;
                }
            }
            YYSYS_LOG(DEBUG,"a packet size value size = %ld",max_packet_size_);
            // add e
            YYSYS_LOG(DEBUG,"mul_ud:debug- row_desc=%s",to_cstring(row_desc_));
        }
    }
    if(OB_SUCCESS == ret)
    {
        if(OB_SUCCESS != (ret = expr_values->reset_stuff_for_insert()))
        {
            YYSYS_LOG(WARN,"fail to reset_stuff in expr_values");
        }
        else if(OB_SUCCESS != (ret = expr_values->open()))
        {
            YYSYS_LOG(WARN,"fail to open ob_expr_values");
        }
        else if(OB_SUCCESS != (ret = fill_values(tmp_value,
                                                 select_result,
                                                 rowkey_info_,
                                                 expr_values,
                                                 row_desc_,
                                                 is_close_sub_query_))
                )
        {
            YYSYS_LOG(WARN,"fail to fill expr values,expr_values=%p\
                      ", expr_values);
        }
    }
//    if(OB_SUCCESS == ret)
//    {
//        if(!is_row_num_null_ &&OB_SUCCESS != (ret = expr_values->store_input_values()))
//        {
//            YYSYS_LOG(ERROR,"fail to do the function: store_input_values");
//        }
//    }
    return ret;
}
/*
 * if there is none row to delete or udpate, then just close the first child which
 *    is only opened
 * else close all the children of ObFillValues
 *
 */
int ObFillValues::close()
{
    int ret = OB_SUCCESS;
   int32_t child_num = ObMultiChildrenPhyOperator::get_child_num()-1 ; //构造的select是最后一个孩子，todo
    for (int32_t index = 0; OB_SUCCESS == ret && index < child_num; index++)
    {
        if( 1 == index && is_row_num_null_) // If the update/delete rows is 0, then don't close the ObExprValues
        {
            continue;
        }
        else if (OB_SUCCESS != (ret = ObMultiChildrenPhyOperator::get_child(index)->close()))
        {
            YYSYS_LOG(WARN,"fail to close the [%d]th child of ObFillValues",index);
        }
    }
    if(is_close_sub_query_)
    {
        if (OB_SUCCESS != (ret = ObMultiChildrenPhyOperator::get_child(2)->close()))
        {
            YYSYS_LOG(WARN,"fail to close the 2th child of ObFillValues");
        }
        else
        {
            is_already_clear_ = true;
        }
    }
    return ret;
}

int ObFillValues::get_next_row(const common::ObRow *&row)
{
    int ret = OB_SUCCESS;
    UNUSED(row);
    return ret;
}

int ObFillValues::get_row_desc(const common::ObRowDesc *&row_desc) const
{
    row_desc = &row_desc_;
    return OB_SUCCESS;
}

int ObFillValues::fill_values(ObValues *&tmp_value,
                              ObProject *&select_result,
                              const ObRowkeyInfo &rowkey_info,
                              ObExprValues*& get_param_values,
                              const ObRowDesc &row_desc,
                              bool &is_close_sub_query)
{
    int ret = OB_SUCCESS;
    int max_index = MAX_UPS_COUNT_ONE_CLUSTER ;
    int64_t row_num[MAX_UPS_COUNT_ONE_CLUSTER+1]={0};//最后一位存放最大的row_num，最大行超出范围后，这一批次结束
    int64_t total_row_size[MAX_UPS_COUNT_ONE_CLUSTER+1]={0};
    int total_row_num = 0;
    ObSequenceUpdate * sequence_update_op = NULL;
    sequence_update_op = dynamic_cast<ObSequenceUpdate *>(sequence_update_);
    const ObRow* row = NULL;
    FILL_TRACE_LOG("calcuter start");
    while(OB_SUCCESS == ret)
    {
        if(total_row_size[max_index] < max_packet_size_ && row_num[max_index]< max_row_num_)
        {
            ret = select_result->get_next_row(row);
        }
        else
        {
            YYSYS_LOG(DEBUG,"one batch over ,row_size = %ld,row_num = %ld",total_row_size[max_index],row_num[max_index]);
            break;
        }
        if(OB_ITER_END == ret)
        {
            sql_context_.session_info_->set_read_times (CLEAR_TIMESTAMP);
            is_close_sub_query = true;
            is_multi_batch_over_ = true;
            YYSYS_LOG(DEBUG,"EDN");
            if(0 == row_num[max_index])
            {
                is_row_num_null_ = true;
            }
            ret = OB_SUCCESS;
            break;
        }
        else if(OB_SUCCESS != ret)
        {
            YYSYS_LOG(WARN,"fail to get next row,ret = %d",ret);
        }
        else if (NULL == row)
        {
            YYSYS_LOG(WARN, "fail to get row from select statement,err = %d", ret);
            ret = OB_ERROR;
            break;
        }
        else
        {
            int64_t tmp_total_size = 0;
            total_row_num++;
            ObRow val_row;//for ob_values, temp value
            val_row.set_row_desc(row_desc_);
            int64_t  num = row->get_column_num ();
            for(int64_t i = 0 ;OB_SUCCESS==ret && i <num ;i++)
            {
                const ObObj *cell = NULL;
                uint64_t tid = OB_INVALID_ID;
                uint64_t column_id = OB_INVALID_ID;
                if ( OB_SUCCESS != (ret =row_desc.get_tid_cid (i,tid,column_id)))
                {
                    YYSYS_LOG(WARN,"fail to get the %ldth cell",i);
                }
                else if ( OB_SUCCESS != (ret = const_cast<ObRow *>(row)->raw_get_cell(i,cell)))
                {
                    YYSYS_LOG(WARN,"fail to get the %ldth cell",i);
                }
                else if(NULL == cell)
                {
                    YYSYS_LOG(WARN,"get null cell");
                    ret = OB_ERROR;
                    break;
                }
                else if((ret = val_row.set_cell(tid,column_id,*cell)) != OB_SUCCESS)
                {
                    YYSYS_LOG(WARN, "Add value to ObRow failed");
                }
                else
                {
                    tmp_total_size+=cell->get_serialize_size();
                }
            }
            YYSYS_LOG(DEBUG,"select-row=%s",to_cstring(row_desc));
            if(OB_SUCCESS != ret)
            {}
            // add by maosy [MultiUps 1.0] [batch_udi] 20170417 b
            else if( !is_table_level_ && OB_SUCCESS !=(ret = get_paxos_id(&val_row)))
            {
                YYSYS_LOG(WARN,"failed to aggin paxos id for ,row = %s,ret = %d",to_cstring(val_row),ret);
            }
            else if (OB_SUCCESS != (ret = tmp_value->add_values_for_ud(&val_row,is_table_level_)))
                // add by maosy  20170417 b
            {
                YYSYS_LOG(WARN,"fail to add row to obvalues,ret[%d]",ret);
            }
            else
            {
                ObRow val_row_expr;//for expr_values, temp value
                const ObRowDesc *row_desc_expr ;
                if(OB_SUCCESS !=(ret = get_param_values->get_row_desc(row_desc_expr)))
                {
                    YYSYS_LOG(WARN,"FAILE TO GET ");
                }
                else
                {
                    val_row_expr.set_row_desc(*row_desc_expr);
                }
                int64_t cell_num = row->get_column_num ();
                for(int64_t index = 0; ret == OB_SUCCESS && index < cell_num; index++)
                {
                    const ObObj *cell = NULL;
                    uint64_t tid = OB_INVALID_ID;
                    uint64_t column_id = OB_INVALID_ID;
                    if ( OB_SUCCESS != (ret =row_desc.get_tid_cid (index,tid,column_id)))
                    {
                        YYSYS_LOG(WARN,"fail to get the %ldth cell",index);
                    }
                    else
                    {
                        if(!rowkey_info.is_rowkey_column (column_id))
                        {
                            if(is_column_hint_index_ && column_id != modify_time_cid_)
                            {
                                ObObj           null_obj;
                                if((ret = val_row_expr.set_cell(tid, column_id, null_obj)) != OB_SUCCESS)
                                {
                                    YYSYS_LOG(WARN, "Add value to ObRow failed");
                                }
                                else
                                {
                                    tmp_total_size+=null_obj.get_serialize_size();
                                }
                            }
                            continue;
                        }
                        else if ( OB_SUCCESS != (ret = const_cast<ObRow *>(row)->raw_get_cell(index,cell)))
                        {
                            YYSYS_LOG(WARN,"fail to get the %ldth cell",index);
                        }
                        else if(NULL == cell)
                        {
                            YYSYS_LOG(WARN,"get null cell");
                            ret = OB_ERROR;
                            break;
                        }
                        else if((ret = val_row_expr.set_cell(tid,column_id,*cell)) != OB_SUCCESS)
                        {
                            YYSYS_LOG(WARN, "Add value to ObRow failed");
                        }
                        else
                        {
                            tmp_total_size+=cell->get_serialize_size();
                        }
                    }
                }
                // add by maosy [MultiUps 1.0] [batch_udi] 20170417 b
                val_row_expr.set_paxos_id(val_row.get_paxos_id());
                // add by maosy 20170417 b
                if(OB_SUCCESS == ret)
                {
                    if (OB_SUCCESS != (ret = get_param_values->add_values(val_row_expr,is_table_level_)))
                    {
                        YYSYS_LOG(WARN,"Failed to add cell into get param, err=%d", ret);
                        break;
                    }
                    else
                    {
                        if(is_table_level_)
                        {
                            total_row_size[max_index] += tmp_total_size ;
                            row_num[max_index]++;
                        }
                        else
                        {
                            int32_t paxos_id = (int32_t)(val_row.get_paxos_id());
                            total_row_size[paxos_id]+=tmp_total_size;
                            total_row_size[max_index] =
                                    total_row_size[max_index] > total_row_size[paxos_id] ? total_row_size[max_index]:total_row_size[paxos_id];
                            row_num[paxos_id]++;
                            row_num[max_index] =
                                    row_num[max_index] > row_num[paxos_id] ? row_num[max_index]:row_num[paxos_id];

                        }
                    }
                }
                // add by maosy 20170417 e
            }//end for add to expr values
        }// end for push a row
    }//end while
    if (OB_SUCCESS == ret && NULL != sequence_update_op)
    {
        if(OB_SUCCESS != (ret = sequence_update_op->handle_the_set_clause_of_seuqence(total_row_num)))
        {
            YYSYS_LOG(WARN, "fail to handle_the_set_clause_of_seuqence, ret=%d", ret);
        }
    }
    FILL_TRACE_LOG("calcuter end");
    affect_row_+=total_row_num;
    return ret;
}

int ObFillValues::add_row_to_obvalues(ObValues*& tmp_value,ObRow *row)
{
    int ret = OB_SUCCESS;

    OB_ASSERT(tmp_value && row);
    if (OB_SUCCESS != (ret = tmp_value->add_values_for_ud(row)))
    {
        YYSYS_LOG(WARN,"fail to add value for ud,ret[%d]",ret);
    }

    return ret;
}

namespace oceanbase
{
namespace sql
{
REGISTER_PHY_OPERATOR(ObFillValues, PHY_FILL_VALUES);
}
}

int64_t ObFillValues::to_string(char* buf, const int64_t buf_len) const
{
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "ObFillValues()\n");

    for (int32_t i = 0; i < ObMultiChildrenPhyOperator::get_child_num(); ++i)
    {
        databuff_printf(buf, buf_len, pos, "====child_%d====\n", i);
        pos += get_child(i)->to_string(buf + pos, buf_len - pos);
        if (i != ObMultiChildrenPhyOperator::get_child_num() - 1)
        {
            databuff_printf(buf, buf_len, pos, ",");
        }
    }

    return pos;
}

int ObFillValues::clear_prepare_select_result()
{
    int ret = OB_SUCCESS;
    if (OB_SUCCESS != (ret = ObMultiChildrenPhyOperator::get_child(2)->close()))
    {
        YYSYS_LOG(WARN,"fail to close the 2th child of ObFillValues");
    }
    return ret;
}

PHY_OPERATOR_ASSIGN(ObFillValues)
{
    int ret = OB_SUCCESS;
    UNUSED(other);
    reset();
    return ret;
}
	// add by maosy [MultiUps 1.0] [batch_udi] 20170417 b
int ObFillValues::get_partiton_type(uint64_t table_id, bool &is_table_level)
{
    int ret = OB_SUCCESS ;
    if (OB_INVALID_ID == table_id)
    {
      ret = OB_ERROR;
      YYSYS_LOG(WARN, "table id is invalid,please check!,ret=%d",ret);
    }
    else if(OB_SUCCESS !=(ret = sql_context_.partition_mgr_->get_table_part_type(table_id,is_table_level)))
    {
        YYSYS_LOG(WARN,"failed to get partition type ,table_id = %lu,ret = %d",table_id,ret);
    }
    return ret ;
}

int ObFillValues::get_paxos_id(ObRow *row)
{
    int ret = OB_SUCCESS ;
    const ObRowkey *rowkey = NULL;
    int32_t  paxos_id = -1;
    uint64_t table_id = get_phy_plan()->get_table_id();
    ObBasicStmt::StmtType stmt_type = ObBasicStmt::T_NONE;
    if (OB_INVALID_ID == table_id )
    {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(ERROR, "table id is not init,ret=%d",ret);
    }
    else if (ObBasicStmt::T_NONE == (stmt_type =get_phy_plan()->get_stmt_type()))
    {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(ERROR, "stmt type is not inti,ret=%d",ret);
    }
    else if (OB_SUCCESS !=(ret = row->get_rowkey(rowkey)))
    {
        YYSYS_LOG(WARN,"failed to get rowkey,row = %s,ret = %d",to_cstring(*row),ret);
    }
    else
    {
        common::ObCalcInfo calc_info;
        calc_info.set_table_id(table_id);
        calc_info.set_stmt_type(stmt_type);
        calc_info.set_schema_manager(sql_context_.schema_manager_);
        calc_info.set_row_key(rowkey);//set rowkey
        if (OB_SUCCESS != (ret = sql_context_.partition_mgr_->get_paxos_id(calc_info, paxos_id)))
        {
            YYSYS_LOG(WARN, "get paxos id failed,ret=%d", ret);
        }
        else if(paxos_id == OB_INVALID_PAXOS_ID)
        {
            ret = OB_ERR_PAXOS_ID ;
            YYSYS_LOG(WARN,"failed to get paxos id ,ret = %d,row = %s",ret , to_cstring(*row));
        }
        else
        {
            row->set_paxos_id(paxos_id);
            YYSYS_LOG(DEBUG,"current row paxos_id is[%d]",paxos_id);
        }
    }
    return ret ;
}
	// add by maosy 20170417 e
