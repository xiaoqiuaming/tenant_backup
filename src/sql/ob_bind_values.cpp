/**
 * ob_bind_values.cpp
 *
 * Authors:
 *   gaojt
 * function:Insert_Subquery_Function
 *
 * used for insert ... select
 */

#include "ob_bind_values.h"
#include "ob_postfix_expression.h"
#include "ob_expr_values.h"
#include "ob_result_set.h"
#include "obmysql/ob_mysql_server.h"
#include "mergeserver/ob_ms_sql_scan_request.h"
#include "ob_iud_loop_control.h"
#include "ob_raw_expr.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

ObBindValues::ObBindValues()
    : is_reset_(0),row_num_(0),is_close_sub_query_(true),is_batch_over_(false),max_rowsize_capacity_(256),max_insert_value_size_(MAX_INSERT_VALUE_SIZE)
{
}

ObBindValues::~ObBindValues()
{
}

void ObBindValues::reset()
{
    is_reset_ = 0;
    row_num_ = 0;
    is_close_sub_query_ = true;
    is_batch_over_=false;
    column_items_.clear();
    max_rowsize_capacity_=256;
}

void ObBindValues::reuse()
{
    is_reset_ = 0;
    row_num_ = 0;
    is_close_sub_query_ = true;
    is_batch_over_=false;
    column_items_.clear();
    max_rowsize_capacity_=256;
   max_insert_value_size_ = MAX_INSERT_VALUE_SIZE;
}


//never used
int ObBindValues::get_next_row(const ObRow *&row)
{
  UNUSED(row);
  int ret = OB_SUCCESS;
  return ret;
}

//never used
int ObBindValues::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  UNUSED(row_desc);
  int ret = OB_SUCCESS;
  return ret;
}

/*Exp:for the explain stm*/
int64_t ObBindValues::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ObBindValues()\n");

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
//add 20140715:e

/*Exp:close the sub-operator of ObBindValues*/
int ObBindValues::close()
{
    int ret = OB_SUCCESS;
    if(is_close_sub_query_)
    {
        if( 0 == row_num_)
        {
            for (int32_t i = 0; ret == OB_SUCCESS && i < ObMultiChildrenPhyOperator::get_child_num(); i++)
            {
              if( 2 == i )
              {
                  continue;
              }
              else if ((ret = get_child(i)->close()) != OB_SUCCESS)
              {
                YYSYS_LOG(WARN, "failed to close %dth child_op, err=%d", i, ret);
              }
            }
        }
        else if( OB_SUCCESS != (ObMultiChildrenPhyOperator::close()))
        {
            YYSYS_LOG(WARN, "failed to close child_op in ob_bind_values, ret=%d",ret);
        }
    }
    else
    {
        for (int32_t i = 1; ret == OB_SUCCESS && i < ObMultiChildrenPhyOperator::get_child_num(); i++)
        {
          if ((ret = get_child(i)->close()) != OB_SUCCESS)
          {
            YYSYS_LOG(WARN, "failed to close %dth child_op, err=%d", i, ret);
          }
        }
    }
    return ret;
}
/*Exp:open and exec the sub-operator of ObBindValues*/
int ObBindValues::open()
{
  int ret = OB_SUCCESS;
  row_num_=0;
  if (ObMultiChildrenPhyOperator::get_child_num() != 4)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR, "the operator ObBindValues is not init");
  }
  else
  {
    if(OB_SUCCESS != (ret = generate_one_batch()))
    {
      YYSYS_LOG(WARN, "failed to generate one batch, err=%d", ret);
    }
    else if(OB_SUCCESS != (ret = dynamic_cast<ObExprValues*>(get_child(1))->store_input_values()))
    {
      YYSYS_LOG(WARN, "failed to open children 2, err=%d", ret);
    }
    else if(0 != row_num_ && OB_SUCCESS != (ret = get_child(2)->open()))
    {
      YYSYS_LOG(WARN, "failed to open children 3, err=%d", ret);
    }
  }
  return ret;
}

/*Exp:execute the once loop and reset the environment at the begin of every loop*/
int ObBindValues::generate_one_batch()
{
    int ret = OB_SUCCESS;
    int64_t row_num = 0;
    //get the child operator of ObBindValues
    ObPhyOperator *sub_query = get_child(0);
    ObExprValues *expr_values = dynamic_cast<ObExprValues*>(get_child(1));
    ObValues *tmp_table = dynamic_cast<ObValues*>(get_child(2));
    ObTableRpcScan *table_scan = dynamic_cast<ObTableRpcScan*>(get_child(3));
    if(NULL == sub_query || NULL == expr_values || NULL == tmp_table || NULL == table_scan)
    {
        ret = ERROR;
        YYSYS_LOG(WARN,"NULL ERROR.sub_query=%p,expr_values=%p,tmp_table=%p,table_scan=%p",
                  sub_query,expr_values,tmp_table,table_scan);
    }
    else if(1==is_reset_)
    {
        table_scan->reset_stuff();
        expr_values->reset_stuff_for_insert();
    }
    else
    {
        if(OB_SUCCESS != (ret = sub_query->open()))
        {
            YYSYS_LOG(WARN, "failed to open sub_query, err=%d", ret);
        }
        // add by maosy [MultiUps 1.0] [batch_udi] 20170421 b
        else if(OB_SUCCESS !=(ret = get_partiton_type(table_id_,is_table_level_)))
        {
            YYSYS_LOG(WARN,"failed to get partition type ,ret = %d",ret );
        }
        // add by maosy 20170421 e
        else
        {
            is_close_sub_query_ = false;
            is_reset_ = 1;
            int64_t cell_num  =0;
            cell_num = column_items_.size();
            cell_num = cell_num >=4 ?cell_num:4;
            max_rowsize_capacity_ = 20164/(cell_num+1);
            YYSYS_LOG(DEBUG,"MAX CELL ROW SIZE=%ld,value size = %ld",max_rowsize_capacity_,max_insert_value_size_);
        }
    }
    if(OB_SUCCESS == ret)
    {
        if(OB_SUCCESS != (ret = expr_values->open()))
        {
            YYSYS_LOG(WARN, "failed to open expr_values, err=%d", ret);
        }
        else if(OB_SUCCESS != (ret = generate_in_operator(sub_query,
                                                          table_scan,
                                                          expr_values,
                                                          tmp_table,
                                                          row_num,
                                                          is_close_sub_query_)))
        {
            YYSYS_LOG(WARN, "failed to complete in operator, err=%d", ret);
        }
        row_num_ += row_num;
    }
    return ret;
}

//construct left part of in opertator
int ObBindValues::construct_in_left_part(ObSqlExpression *&rows_filter,
                                         ExprItem& expr_item,
                                         ObTableRpcScan* table_scan)
{
    int ret = OB_SUCCESS;
    ObSqlExpression column_ref;
    expr_item.type_ = T_REF_COLUMN;
    expr_item.value_.cell_.tid = table_id_;
    int64_t rowkey_column_num = rowkey_info_.get_size();
    for (int32_t i = 0; OB_SUCCESS == ret && i < column_items_.size(); ++i)
    {
        if (rowkey_info_.is_rowkey_column((column_items_.at(i)).column_id_))
        {
            expr_item.value_.cell_.cid = (column_items_.at(i)).column_id_;
            column_ref.reset();
            column_ref.set_tid_cid(table_id_, expr_item.value_.cell_.cid);
            if (OB_SUCCESS != (ret = rows_filter->add_expr_item(expr_item)))
            {
                YYSYS_LOG(WARN, "failed to add expr item, err=%d", ret);
                break;
            }
            else if (OB_SUCCESS != (ret = column_ref.add_expr_item(expr_item)))
            {
                YYSYS_LOG(WARN, "failed to add expr_item, err=%d", ret);
                break;
            }
            else if (OB_SUCCESS != (ret = column_ref.add_expr_item_end()))
            {
                YYSYS_LOG(WARN, "failed to add expr item, err=%d", ret);
                break;
            }
            else if (OB_SUCCESS != (ret = table_scan->add_output_column(column_ref)))
            {
                YYSYS_LOG(WARN, "failed to add output column, err=%d", ret);
                break;
            }
        }
    } // end for
    // add action flag column
    if (OB_LIKELY(OB_SUCCESS == ret))
    {
        column_ref.reset();
        column_ref.set_tid_cid(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);
        if (OB_SUCCESS != (ret = ObSqlExpressionUtil::make_column_expr(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, column_ref)))
        {
            YYSYS_LOG(WARN, "fail to make column expr:ret[%d]", ret);
        }
        else if (OB_SUCCESS != (ret = table_scan->add_output_column(column_ref)))
        {
            YYSYS_LOG(WARN, "failed to add output column, err=%d", ret);
        }
    }
    if (OB_LIKELY(OB_SUCCESS == ret))
    {
        expr_item.type_ = T_OP_ROW;
        expr_item.value_.int_ = rowkey_column_num;
        if (OB_SUCCESS != (ret = rows_filter->add_expr_item(expr_item)))
        {
            YYSYS_LOG(WARN,"Failed to add expr item, err=%d", ret);
        }
    }
    if (OB_LIKELY(OB_SUCCESS == ret))
    {
        expr_item.type_ = T_OP_LEFT_PARAM_END;
        // a in (a,b,c) => 1 Dim;  (a,b) in ((a,b),(c,d)) =>2 Dim; ((a,b),(c,d)) in (...) =>3 Dim
        expr_item.value_.int_ = 2;
        if (OB_SUCCESS != (ret = rows_filter->add_expr_item(expr_item)))
        {
            YYSYS_LOG(WARN, "failed to add expr item, err=%d", ret);
        }
    }
    return ret;
}
//construct right part of in operator
int ObBindValues::construct_in_right_part(const ObRow *row,
                            ObSqlExpression *&rows_filter,
                            ExprItem &expr_item,
                            ObExprValues* expr_values,
                            ObValues* tmp_table,
                            int64_t& total_row_size)
{
    int ret = OB_SUCCESS;
    int64_t rowkey_column_num = rowkey_info_.get_size();
    ObRow val_row;//for expr_values, temp value
    val_row.set_row_desc(row_desc_);
    YYSYS_LOG(DEBUG,"mul-iud:debug-row_desc=%s", to_cstring(row_desc_));
    //add gaojt [Insert_Subquery_Function_multi_key] 20160619:b
    int32_t column_num = column_items_.size();
    //add gaojt 20160619:e
    //try to fill value and expr_value content
    for(int32_t j = 0; ret == OB_SUCCESS && j < column_num; j++)
    {
        const ObObj *cell = NULL;
        const_cast<ObRow *>(row)->raw_get_cell(static_cast<int64_t>(j), cell);
        if(NULL == cell)
        {
            YYSYS_LOG(WARN,"get null cell");
            ret = OB_ERROR;
            break;
        }
        else if((ret = val_row.set_cell((column_items_.at(j)).table_id_, (column_items_.at(j)).column_id_, *cell)) != OB_SUCCESS)
        {
            YYSYS_LOG(WARN, "Add value to ObRow failed");
        }
        else if(rowkey_info_.is_rowkey_column((column_items_.at(j)).column_id_))
        {
            rows_filter->add_expr_in_obj(*(const_cast<ObObj*>(cell)));
            //add wenghaixing[decimal] for fix insert bug 2014/10/11
            ObObjType schema_type;
            uint32_t schema_p;
            uint32_t schema_s;
            if(OB_SUCCESS!=(ret=tmp_table->get_rowkey_schema((column_items_.at(j)).table_id_,(column_items_.at(j)).column_id_,schema_type,schema_p,schema_s)))
            {
                YYSYS_LOG(WARN,"get rowkey decimal schemal failed!ret=%d",ret);
                break;
            }
            else if(ObDecimalType == schema_type)
            {
                ObPostfixExpression& ops_1=rows_filter->get_decoded_expression_v2();
                ObObj& obj_1=ops_1.get_expr();
                if(ObDecimalType==obj_1.get_type())
                {
                    ops_1.fix_varchar_and_decimal(schema_p,schema_s);
                }
                else if(ObVarcharType==obj_1.get_type())
                {
                    ops_1.fix_varchar_and_decimal(schema_p,schema_s);
                }
            }
            //add:e
            //add by maosy [Insert_Subquery_Function] 
            total_row_size +=cell->get_serialize_size();
            // add e
        }
        total_row_size +=cell->get_serialize_size();
    }
    // add by maosy [MultiUps 1.0] [batch_udi] 20170421 b
    if(OB_SUCCESS ==ret && !is_table_level_)
    {
        if( OB_SUCCESS !=(ret = get_paxos_id(&val_row)))
        {
            YYSYS_LOG(WARN,"failed to set paxos id ,ret = %d",ret );
        }
        else
        {
            int64_t paxos_id = val_row.get_paxos_id();
            expr_values->set_paxos_id(paxos_id);
            tmp_table->set_paxos_id(paxos_id);
            const_cast<ObRow *>(row)->set_paxos_id(paxos_id);
        }
    }
    // add by maosy 20170421 e
    if(OB_SUCCESS==ret && OB_SUCCESS != (ret=expr_values->add_values_for_batch(val_row)))
    {
        YYSYS_LOG(WARN, "Failed to add values to ObExprValues, err=%d", ret);
    }

    if(OB_SUCCESS == ret)
    {
        if (rowkey_column_num > 0)
        {
            expr_item.type_ = T_OP_ROW;
            expr_item.value_.int_ = rowkey_column_num;
            if (OB_SUCCESS != (ret = rows_filter->add_expr_item(expr_item)))
            {
                YYSYS_LOG(WARN, "Failed to add expr item, err=%d", ret);
            }
        }
    }
    return ret;
}
//construct end part of in operator
int ObBindValues::construct_in_end_part(ObTableRpcScan *&table_scan,
                          ObSqlExpression *&rows_filter,
                          int64_t row_num,
                          ExprItem &expr_item)
{
    int ret = OB_SUCCESS;
    expr_item.type_ = T_OP_ROW;
    expr_item.value_.int_ = row_num;
    ExprItem expr_in;
    expr_in.type_ = T_OP_IN;
    expr_in.value_.int_ = 2;
    if (OB_SUCCESS != (ret = rows_filter->add_expr_item(expr_item)))
    {
        YYSYS_LOG(WARN,"Failed to add expr item, err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = rows_filter->add_expr_item(expr_in)))
    {
        YYSYS_LOG(WARN,"Failed to add expr item, err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = rows_filter->add_expr_item_end()))
    {
        YYSYS_LOG(WARN,"Failed to add expr item end, err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = table_scan->add_filter(rows_filter)))
    {
        YYSYS_LOG(WARN,"Failed to add filter, err=%d", ret);
    }
    return ret;
}

int ObBindValues::generate_in_operator(ObPhyOperator *sub_query,
                                       ObTableRpcScan* table_scan,
                                       ObExprValues *expr_values,
                                       ObValues* tmp_table,
                                       int64_t &row_num,
                                       bool &is_close_sub_query)
{
    int ret = OB_SUCCESS;
    ExprItem expr_item;
    ObSqlExpression *rows_filter = ObSqlExpression::alloc();
    if (NULL == rows_filter)
    {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        YYSYS_LOG(WARN, "no memory");
    }
    else
    {
        int max_index = MAX_UPS_COUNT_ONE_CLUSTER ;
        int64_t partition_row_num[MAX_UPS_COUNT_ONE_CLUSTER+1]={0};//×îºóÒ»Î»´æ·Å×î´óµÄrow_num£¬×î´óÐÐ³¬³ö·¶Î§ºó£¬ÕâÒ»Åú´Î½áÊø
        int64_t total_row_size[MAX_UPS_COUNT_ONE_CLUSTER+1]={0};
        const ObRow* row = NULL;
        if(OB_SUCCESS != (ret = construct_in_left_part(rows_filter,expr_item,table_scan)))
        {
            YYSYS_LOG(ERROR,"fail to construct in left part,ret=%d",ret);
        }
        while(OB_SUCCESS == ret)
        {
            int64_t total_rowkey_size =0;
            if((total_row_size[max_index] < max_insert_value_size_)&&(partition_row_num[max_index]<max_rowsize_capacity_))
            {
                ret = sub_query->get_next_row(row);
            }
            if(OB_ITER_END == ret)
            {
                is_close_sub_query = true;
                is_batch_over_=true;
                ret = OB_SUCCESS;
                break;
            }
            else if((OB_SUCCESS == ret) &&
                    (NULL == row ||  total_row_size[max_index] >= max_insert_value_size_ || partition_row_num[max_index]>=max_rowsize_capacity_))
            {
                if (NULL == row)
                {
                    YYSYS_LOG(WARN, "fail to get row from select statement,err = %d", ret);
                    ret = OB_ERROR;
                }
                break;
            }
            else if(OB_SUCCESS != ret)
            {
                YYSYS_LOG(WARN,"fail to get next row,ret = %d",ret);
            }
            else
            {
                row_num++;
                if(OB_SUCCESS != (ret = construct_in_right_part(row,
                                                                rows_filter,
                                                                expr_item,
                                                                expr_values,
                                                                tmp_table,
                                                                total_rowkey_size
                                                                )))
                {
                    YYSYS_LOG(ERROR,"fail to construct in right part,row=%p,rows_filter=%p,ret=%d",row,rows_filter,ret);
                }
                //add by maosy [MultiUPS 1.0] [read uncommit]20170525 b:
                else
                {
                    if(is_table_level_)
                    {
                        total_row_size[max_index] += total_rowkey_size ;
                        partition_row_num[max_index]++;
                    }
                    else
                    {
                        int32_t paxos_id = (int32_t)(row->get_paxos_id());
                        total_row_size[paxos_id]+=total_rowkey_size;
                        total_row_size[max_index] =
                                total_row_size[max_index] > total_row_size[paxos_id] ? total_row_size[max_index]:total_row_size[paxos_id];
                        partition_row_num[paxos_id]++;
                        partition_row_num[max_index]=
                                partition_row_num[max_index]>partition_row_num[paxos_id] ? partition_row_num[max_index]:partition_row_num[paxos_id];
                    }
                }
				// add by maosy e
            }
        }
    }
        if(OB_SUCCESS == ret)
        {
            if(OB_SUCCESS != (ret = construct_in_end_part(table_scan,
                                                          rows_filter,
                                                          row_num,
                                                          expr_item)))
            {
                YYSYS_LOG(ERROR,"fail to construct in end part,rows_filter=%p,row_num=%ld,ret=%d",rows_filter,row_num,ret);
            }
            expr_values->set_row_num (row_num);
        }
        if (OB_SUCCESS != ret && NULL != rows_filter)
        {
            ObSqlExpression::free(rows_filter);
        }
        return ret;
}
int ObBindValues::set_row_desc_map(ObSEArray<int64_t, 64> &row_desc_map)
{
  row_desc_map_ = row_desc_map;
  return OB_SUCCESS;
}
int ObBindValues::set_row_desc(const common::ObRowDesc &row_desc)
{
  row_desc_ = row_desc;
  return OB_SUCCESS;
}

int ObBindValues::set_rowkey_info(const common::ObRowkeyInfo &rowkey_info)
{
  rowkey_info_ = rowkey_info;
  return OB_SUCCESS;
}
void ObBindValues::set_table_id(uint64_t table_id)
{
  table_id_ = table_id;
}
uint64_t ObBindValues::get_table_id_()
{
  return table_id_;
}
//add gaojt [Insert_Subquery_Function_multi_key] 20160619:b
int ObBindValues::add_column_item(const ColumnItem& column_item)
{
    int ret = OB_SUCCESS;
    ret = column_items_.push_back(column_item);
    return ret;
}
//add gaojt 20160619:e

// add by maosy [MultiUps 1.0] [batch_udi] 20170417 b
int ObBindValues::get_partiton_type(uint64_t table_id, bool &is_table_level)
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

int ObBindValues::get_paxos_id(ObRow *row)
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
PHY_OPERATOR_ASSIGN(ObBindValues)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObBindValues);
  reset();
  row_desc_ = o_ptr->row_desc_;
  rowkey_info_ = o_ptr->rowkey_info_;
  row_desc_map_ = o_ptr->row_desc_map_;
  is_reset_ = o_ptr->is_reset_;
  table_id_ = o_ptr->table_id_;
  return ret;
}
namespace oceanbase
{
namespace sql
{
REGISTER_PHY_OPERATOR(ObBindValues,PHY_BIND_VALUES);
}
}
//add 20141027:e
