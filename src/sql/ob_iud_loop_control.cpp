//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20150507:b
/**
 * ob_bind_values.cpp
 *
 * Authors:
 *   gaojt
 *function:Insert_Subquery_Function
 * used for insert ... select
 */

#include "ob_iud_loop_control.h"
#include "ob_bind_values.h"
#include "ob_ups_executor.h"
#include "ob_physical_plan.h"
#include "ob_sql_session_info.h"
//add maosy [Delete_Update_Function] 20170106:b
#include "common/ob_errno.h"
//add:e
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObIudLoopControl::ObIudLoopControl():
  batch_num_(0),affected_row_(0),need_start_trans_(false)
{
}

ObIudLoopControl::~ObIudLoopControl()
{
}

void ObIudLoopControl::reset()
{
  affected_row_=0;
  need_start_trans_ =false;
return ObSingleChildPhyOperator::reset();
}
//add by gaojt [Delete_Update_Function-Transaction] 201601206:b
int ObIudLoopControl::commit()
{
  YYSYS_LOG(DEBUG,"enter into commit()");
  int ret = OB_SUCCESS;
  ObString start_thx = ObString::make_string("COMMIT");
  ret = execute_stmt_no_return_rows(start_thx);
  YYSYS_LOG(DEBUG,"leave commit()");
  return ret;
}

int ObIudLoopControl::start_transaction()
{
  YYSYS_LOG(DEBUG,"enter into start_transaction()");
  int ret = OB_SUCCESS;
  ObString start_thx = ObString::make_string("START TRANSACTION");
  ret = execute_stmt_no_return_rows(start_thx);
  YYSYS_LOG(DEBUG,"leave start_transaction()");
  return ret;
}

int ObIudLoopControl::execute_stmt_no_return_rows(const ObString &stmt)
{
  YYSYS_LOG(DEBUG,"enter into execute_stmt_no_return_rows()");
  int ret = OB_SUCCESS;
  ObMySQLResultSet tmp_result;
  if (OB_SUCCESS != (ret = tmp_result.init()))
  {
    YYSYS_LOG(WARN, "init result set failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = ObSql::direct_execute(stmt, tmp_result, sql_context_)))
  {
    YYSYS_LOG(WARN, "direct_execute failed, sql=%.*s ret=%d", stmt.length(), stmt.ptr(), ret);
  }
  else if (OB_SUCCESS != (ret = tmp_result.open()))
  {
    YYSYS_LOG(WARN, "open result set failed, sql=%.*s ret=%d", stmt.length(), stmt.ptr(), ret);
  }
  else
  {
    int err = tmp_result.close();
    if (OB_SUCCESS != err)
    {
      YYSYS_LOG(WARN, "failed to close result set,err=%d", err);
    }
    tmp_result.reset();
  }
  YYSYS_LOG(DEBUG,"leave execute_stmt_no_return_rows()");
  return ret;
}

int ObIudLoopControl::rollback()
{
    YYSYS_LOG(DEBUG,"enter into rollback()");
    int ret = OB_SUCCESS;
    ObString start_thx = ObString::make_string("ROLLBACK");
    ret = execute_stmt_no_return_rows(start_thx);
    YYSYS_LOG(DEBUG,"leave rollback()");
    return ret;
}
//add by gaojt 201601206:e
void ObIudLoopControl::reuse()
{
    reset();
}
int ObIudLoopControl::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  UNUSED(row_desc);
  int ret = OB_SUCCESS;
  return ret;
}

//never used
int ObIudLoopControl::get_next_row(const ObRow *&row)
{
  UNUSED(row);
  int ret = OB_SUCCESS;
  return ret;
}
/*Exp:for the explain stm*/
int64_t ObIudLoopControl::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ObIudLoopControl(\n");
  databuff_printf(buf, buf_len, pos, ")\n");
  if (NULL != child_op_)
  {
    pos += child_op_->to_string(buf+pos, buf_len-pos);
  }
  return pos;
}

/*Exp:close the sub-operator of ObIudLoopControl*/
int ObIudLoopControl::close()
{
  affected_row_=0;
  return ObSingleChildPhyOperator::close();
}
//add by maosy [MultiUps 1.0] [batch_iud_snapshot] 20170622 b:
int ObIudLoopControl::set_stmt_start_time()
{
    int ret = OB_SUCCESS;
    bool is_table_level = false;
    uint64_t table_id = get_phy_plan()->get_table_id();
    if (OB_INVALID_ID == table_id)
    {
      YYSYS_LOG(WARN, "table id is invalid, please check!");
    }
    else if (OB_SUCCESS != (sql_context_.partition_mgr_->get_table_part_type(table_id, is_table_level)))
    {
      YYSYS_LOG(WARN, "failed to get partition type, table_id = %lu", table_id);
    }
    YYSYS_LOG(DEBUG, "get partition type, table_id = %lu, ret = %d, is_table_level=%d", table_id, ret, is_table_level);
    if (is_table_level)
    {
      ObCalcInfo calc_info;
      int32_t paxos_id = 0;
      ObServer ups_server;
      int64_t published_transid;
      calc_info.set_stmt_type(ObBasicStmt::T_UPDATE);
      if (OB_SUCCESS != (ret = sql_context_.partition_mgr_->get_paxos_id(table_id, calc_info, paxos_id)))
      {
        YYSYS_LOG(WARN, "failed to get paxos id, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = sql_context_.merger_rpc_proxy_->get_master_update_server(paxos_id, ups_server)))
      {
        YYSYS_LOG(WARN, "failed to get master ups, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = sql_context_.merger_rpc_proxy_->get_stmt_start_time(ups_server, published_transid)))
      {
        YYSYS_LOG(WARN, "failed to get start time, ret=%d", ret);
      }
      else
      {
        YYSYS_LOG(DEBUG, "paxos_id=%d, time=%ld", paxos_id, published_transid);
        sql_context_.session_info_->set_master_ups_transid(paxos_id, published_transid);
        YYSYS_LOG(DEBUG, "get partition type, table_id=%lu, is_table_level=%d, paxos_id=%d, ret=%d",
                  table_id, is_table_level, paxos_id, ret);
      }
      if (ret == OB_SUCCESS)
      {
        return ret;
      }
      else
      {
        YYSYS_LOG(WARN, "get partition type, table_id=%lu, is_table_level=%d, paxos_id=%d, ret=%d",
                  table_id, is_table_level, paxos_id, ret);
        ret = OB_SUCCESS;
      }
      YYSYS_LOG(DEBUG, "is_table_level=%d, paxos_id=%d, time=%ld", is_table_level, paxos_id, published_transid);
    }
    int32_t count = sql_context_.merger_rpc_proxy_->get_master_update_server_count();
    for(int32_t index = 0 ; index < count ; index++)
    {
        int64_t paxos_id= 0;
        ObServer ups_server ;
        int64_t published_transid;
        if(OB_SUCCESS !=(ret = sql_context_.merger_rpc_proxy_->get_master_ups(index,paxos_id,ups_server)))
        {
            YYSYS_LOG(WARN,"failed to get mast ups,ret =%d",ret );
        }
        else if(OB_SUCCESS !=(ret = sql_context_.merger_rpc_proxy_->get_stmt_start_time(ups_server,published_transid)))
        {
            YYSYS_LOG(WARN,"failed to get start time ,ret =%d",ret );
        }
        else
        {
            YYSYS_LOG(DEBUG,"PAXOS ID = %ld,time =%ld",paxos_id,published_transid);
            sql_context_.session_info_->set_master_ups_transid(paxos_id,published_transid);
        }
    }
    return ret ;
}
//add by maosy e

int ObIudLoopControl::open()
{
    int ret = OB_SUCCESS;
    bool is_ud_original = false;//�������������������ǵ���������Ϊtrue
    ObFillValues* fill_values = NULL;
    ObBindValues* bind_values = NULL;
    get_fill_bind_values(fill_values,bind_values);
    bool is_start_transaction = false;//�����������Ƿ������ֶ�����
    if(NULL == fill_values && NULL == bind_values)
    {
        is_ud_original = true;
    }
    if (!is_ud_original && (need_start_trans_  || !sql_context_.session_info_->get_autocommit ()))
        //������䲻��������ֻ����������ʱ��ŻῪ������
    {
        YYSYS_LOG(DEBUG,"need start = %d,is_autocommit = %d,trans_id is valid= %d",need_start_trans_,
                sql_context_.session_info_->get_autocommit (),
                  sql_context_.session_info_->get_trans_id().is_valid());
        if (sql_context_.session_info_->get_trans_id().is_valid()
                || sql_context_.session_info_->get_trans_start())
        {
           is_start_transaction = false;//�Ѿ��������񣬲���Ҫ���ֶ�����
        }
        else if (OB_SUCCESS != (ret = start_transaction()))
        {
            YYSYS_LOG(WARN,"fail to start transaction");
        }
        else
        {
            is_start_transaction = true;//�����ֶ�����
        }
    }
    //add by maosy [MultiUps 1.0] [batch_iud_snapshot] 20170622 b:
    if(!is_ud_original && (sql_context_.session_info_->get_trans_id().is_valid()
            || sql_context_.session_info_->get_trans_start()))
    {
        ret = set_stmt_start_time();//todo
    }
    // add by e
    if(OB_SUCCESS != ret )
    {}
    else if(1 != get_child_num())
    {
        ret = OB_NOT_INIT;
        YYSYS_LOG(ERROR, "the operator ObIudLoopControl is not init");
    }
    else
    {
        while(OB_SUCCESS == ret && !is_batch_over(fill_values,bind_values))
        {
            if (NULL == get_child(0))
            {
                ret = OB_ERR_UNEXPECTED;
                YYSYS_LOG(ERROR, "Ups Executor must not be NULL");
            }
            else
            {
                ret = get_child(0)->open();
                if (OB_SUCCESS != ret)
                {
                    YYSYS_LOG(WARN, "failed to open Ups Executor , ret=%d", ret);
                }
                else
                {
                    get_affect_rows(is_ud_original,fill_values,bind_values);
                    batch_num_++;
                }
            }
            if (is_ud_original)
            {
                break;
            }
            else
            {
                YYSYS_LOG(DEBUG,"has manipulation %ld rows , %ld batch is over ",affected_row_,batch_num_);
            }
        }
        if(OB_ITER_END == ret)
        {
            ret = OB_SUCCESS;
        }
    }
    sql_context_.session_info_->reset_master_ups_transids();//add by maosy [MultiUps 1.0] [batch_iud_snapshot] 20170622 
    if (!is_ud_original)
    {
        set_affect_row();
        sql_context_.session_info_->set_read_times (NO_BATCH_UD);
        //���������������ִ��ʧ���ˣ� ����ʲô��������Ҫ�ع�����
        if(OB_SUCCESS == ret)
        {}
        else
        {
            YYSYS_LOG(WARN, "This is the %ld batch data,has handled %ld rows, and the, transID[%d].",
                      batch_num_,affected_row_, sql_context_.session_info_->get_trans_id().descriptor_);
            if (NULL != fill_values && !fill_values->is_already_clear())
            {
                fill_values->clear_prepare_select_result();
            }
            if( OB_SUCCESS != (rollback()))
            {
                YYSYS_LOG(WARN,"fail to rollback");
            }
            else
            {
                if (OB_ERR_VARCHAR_TOO_LONG != ret && OB_DECIMAL_UNLEGAL_ERROR != ret)
                {
                    YYSYS_LOG(USER_ERROR,"%s, error code = %d", ob_strerror(ret), ret);
                }

                if (ret == OB_CHECK_VERSION_RETRY)
                {
                    YYSYS_LOG(WARN,"multi row transaction should retry");
                }
                else{
                    ret = OB_TRANS_ROLLBACKED;
                }
                YYSYS_LOG(INFO,"transaction is rolled back");
            }
        }
    }
    // �������ʱ�ҿ������������Զ��ύ������������Ҫ��ִ����ɺ��ύ����
    if (is_start_transaction && sql_context_.session_info_->get_autocommit ())
    {
        if ( OB_SUCCESS == ret)
        {
            if (OB_SUCCESS != (ret = commit()))
            {
                YYSYS_LOG(WARN,"fail to commit");
            }
        }
    }

    return ret;
}

void ObIudLoopControl::get_affect_rows(bool& is_ud_original,
                                       ObFillValues* fill_values,
                                       ObBindValues* bind_values)
{
    if (NULL != fill_values)
    {
        affected_row_+=fill_values->get_affect_row();
    }
    else if (NULL != bind_values)
    {
        affected_row_+=bind_values->get_inserted_row_num();
    }
    else
    {
        is_ud_original = true;
    }
}

void ObIudLoopControl::set_affect_row()
{
    OB_ASSERT(my_phy_plan_);
    ObResultSet* outer_result_set = NULL;
    ObResultSet* my_result_set = NULL;
    my_result_set = my_phy_plan_->get_result_set();
    outer_result_set = my_result_set->get_session()->get_current_result_set();
    my_result_set->set_session(outer_result_set->get_session()); // be careful!
    YYSYS_LOG(DEBUG,"set affected row =%ld",affected_row_);
    outer_result_set->set_affected_rows(affected_row_);
}
bool ObIudLoopControl::is_batch_over(ObFillValues*& fill_values,ObBindValues*& bind_values)
{
    bool is_over = false;
    if(NULL != fill_values)
    {
        is_over = fill_values->is_multi_batch_over();
    }
    else if (NULL != bind_values)
    {
        is_over = bind_values->is_batch_over();
    }
    return is_over;
}

void ObIudLoopControl::get_fill_bind_values(ObFillValues *&fill_values, ObBindValues *&bind_values)
{
    //modify wangjg [Delete_Update_Function bugfix] 20170804:b
    /*OB_ASSERT(my_phy_plan_);
    int64_t num = my_phy_plan_->get_operator_size();
    for(int64_t i = 0 ; i <num ; i++)
    {
        ObUpsExecutor* ups_executor= dynamic_cast<ObUpsExecutor*>(my_phy_plan_->get_phy_operator(i));
        if(ups_executor!=NULL)
        {
            ObPhysicalPlan * inner_plan = ups_executor->get_inner_plan();
            int64_t ups_num = inner_plan->get_operator_size();
            for(int64_t j = 0 ; j <ups_num ; j++)
            {
                ObPhyOperator* phy_opt = inner_plan->get_phy_operator(j);
                if(NULL !=(dynamic_cast<ObFillValues*>(phy_opt)))
                {
                    fill_values= dynamic_cast<ObFillValues*>(phy_opt);
                    break;
                }
                else if(NULL !=(dynamic_cast<ObBindValues*>(phy_opt)))
                {
                    bind_values= dynamic_cast<ObBindValues*>(phy_opt);
                    break;
                }

            }//end for
            break;
        }//end if
    }//end for
    */
    ObPhyOperator * child_op = get_child(0);
    ObUpsExecutor * ups_executor = NULL;
    if(child_op != NULL)
    {
      ups_executor = dynamic_cast<ObUpsExecutor*>(child_op);
    }
    if(ups_executor != NULL)
    {
        ObPhysicalPlan * inner_plan = ups_executor->get_inner_plan();
        int64_t ups_num = inner_plan->get_operator_size();
        for(int64_t j = 0; j < ups_num; j++)
        {
            ObPhyOperator * phy_opt = inner_plan->get_phy_operator(j);
            if(NULL != (dynamic_cast<ObFillValues*>(phy_opt)))
            {
                fill_values = dynamic_cast<ObFillValues*>(phy_opt);
                break;
            }
            else if(NULL != (dynamic_cast<ObBindValues*>(phy_opt)))
            {
                bind_values = dynamic_cast<ObBindValues*>(phy_opt);
                break;
            }
        }//end for
    }//end if
    //modify wangjg :e
}

PHY_OPERATOR_ASSIGN(ObIudLoopControl)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObIudLoopControl);
  reset();
  batch_num_ = o_ptr->batch_num_;
  affected_row_ = o_ptr->affected_row_;
  return ret;
}
namespace oceanbase
{
namespace sql
{
REGISTER_PHY_OPERATOR(ObIudLoopControl,PHY_IUD_LOOP_CONTROL);
}
}
//add gaojt 20150507:e
