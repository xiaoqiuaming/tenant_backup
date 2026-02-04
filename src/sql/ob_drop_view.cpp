/**
 * add lujc[drop view]
 */

#include "sql/ob_drop_view.h"
#include "common/utility.h"
#include "mergeserver/ob_rs_rpc_proxy.h"
#include "sql/ob_sql.h"
#include"common/ob_inner_table_operator.h"
#include"common/ob_trigger_msg.h"

using namespace  oceanbase::sql ;
using namespace  oceanbase::common ;

ObDropView::ObDropView()
    :if_exists_(false),rpc_(NULL)
    , result_set_out_(NULL)
{
}

ObDropView::~ObDropView()
{
}

void ObDropView::reset()
{
    if_exists_=false;
    rpc_ = NULL;
    local_context_.rs_rpc_proxy_ = NULL;
    result_set_out_ = NULL;
}

void ObDropView::reuse()
{
    if_exists_=false;
    rpc_ = NULL;
    local_context_.rs_rpc_proxy_ = NULL;
    result_set_out_ = NULL;
}



void ObDropView::set_sql_context(const ObSqlContext &context)
{
    local_context_ = context;
    local_context_.schema_manager_ = NULL;
    result_set_out_ = local_context_.session_info_->get_current_result_set();
}

void ObDropView::set_if_exists(bool if_exists)
{
    if_exists_=if_exists;
}

int ObDropView::add_view_name(const ObString &tname)
{
    return views_.add_string(tname);
}

int ObDropView::add_table_id(const uint64_t &tid)
{
    return table_ids_.push_back(tid);
}

int ObDropView::execute_stmt_no_return_rows(const ObString &stmt)
{
    int ret = OB_SUCCESS;
    ObResultSet tmp_result;
    local_context_.session_info_->set_current_result_set(&tmp_result);
    if(OB_SUCCESS!=(ret=tmp_result.init()))
    {
        YYSYS_LOG(WARN,"init result set failed, ret=%d",ret);
    }
    else if(OB_SUCCESS !=(ret=ObSql::direct_execute(stmt,tmp_result,local_context_)))
    {
        if(result_set_out_ != NULL) result_set_out_->set_message(tmp_result.get_message());
        YYSYS_LOG(WARN,"direct_execute failed, sql=%.*s ret=%d",stmt.length(),stmt.ptr(),ret);
    }
    else if(OB_SUCCESS !=(ret =tmp_result.open()))
    {
        YYSYS_LOG(WARN,"open result set failed, sql=%.*s ret=%d",stmt.length(),stmt.ptr(),ret);
    }
    else
    {
        OB_ASSERT(tmp_result.is_with_rows() == false);
        int err =tmp_result.close();
        if(OB_SUCCESS != err)
        {
            YYSYS_LOG(WARN,"failed to close result set,err=%d",err);

        }
        tmp_result.reset();
    }
    local_context_.session_info_->set_current_result_set(result_set_out_);
    return ret;
}

int ObDropView::commit()
{
    int ret = OB_SUCCESS;
    ObString commit = ObString::make_string("commit");
    ret = execute_stmt_no_return_rows(commit);
    if((OB_SUCCESS == ret)&&(OB_SUCCESS !=(ret = insert_trigger())))
    {
        YYSYS_LOG(ERROR,"insert trigger  failed,ret=%d",ret);
    }
    if(OB_SUCCESS == ret && local_context_.session_info_->get_autocommit() == false)
    {
        ret = execute_stmt_no_return_rows(commit);
    }
    return ret;

}


int ObDropView::insert_trigger()
{
    int ret = OB_SUCCESS;
    int64_t timestamp = yysys::CTimeUtil::getTime();
    ObString sql;
    ObServer server;
    server.set_ipv4_addr(yysys::CNetUtil::getLocalAddr(NULL),0);
    char buf[OB_MAX_SQL_LENGTH]="";
    sql.assign(buf,sizeof(buf));
    ret = ObInnerTableOperator::update_all_trigger_event(sql,timestamp,server,UPDATE_PRIVILEGE_TIMESTAMP_TRIGGER,0);
    if(ret !=OB_SUCCESS)
    {
        YYSYS_LOG(ERROR,"get update all trigger event sql failed:ret[%d]",ret);
    }
    else
    {
        ret=execute_stmt_no_return_rows(sql);
        if(ret != OB_SUCCESS)
        {
            YYSYS_LOG(WARN,"execute_stmt_no_return_rows failed:sql[%.*s], ret[%d]",sql.length(),sql.ptr(),ret);
        }
    }
    return ret;
}

int ObDropView::open()
{
    int ret = OB_SUCCESS;
    if( NULL ==rpc_ ||0>=table_ids_.count())
    {
        ret=OB_NOT_INIT;
        YYSYS_LOG(ERROR,"not init");
    }
    else if(local_context_.session_info_->get_trans_start() || local_context_.session_info_->get_trans_id().is_valid())
    {
        YYSYS_LOG(WARN,"drop table is not allowed in transaction, err=%d",ret);
        ret = OB_ERR_TRANS_ALREADY_STARTED;
    }
    else if(OB_SUCCESS !=(ret =rpc_->drop_view(if_exists_, views_)))
    {
        YYSYS_LOG(WARN,"failed to create table, err=%d",ret);
    }
    else
    {
        YYSYS_LOG(INFO,"drop view succ, views=[%s] if_exists=%c",
                  to_cstring(views_),if_exists_?'Y' :'N');
        clean_owner_privilege();
    }
    return ret;
}

void ObDropView::clean_owner_privilege()
{
    int ret=OB_SUCCESS;
    ObString delete_stmt;
    char delete_buff[512];
    int64_t pos=0;
    uint64_t tid=OB_INVALID_ID;
    ObArray<ObPrivilege::UserIdDatabaseId> udi_array;
    local_context_.schema_manager_ = local_context_.merger_schema_mgr_->get_user_schema(0);
    if(NULL ==local_context_.schema_manager_)
    {
        ret =OB_SCHEMA_ERROR;
        YYSYS_LOG(WARN,"get schema mgr failed, ret=%d",ret);
    }

    for(int64_t i = 0;i<table_ids_.count() && OB_SUCCESS ==ret;i++)
    {
        tid=table_ids_.at(i);
        if(OB_SUCCESS !=(ret =(*(local_context_.pp_privilege_))->get_user_db_id(tid,udi_array)))
        {
            YYSYS_LOG(WARN,"get user_db_id by table id failed, ret=%d",ret);
        }
        else
        {
            for(int64_t j=0;j<udi_array.count();j++)
            {
                ObPrivilege::UserIdDatabaseId &udi=udi_array.at(j);
                pos = 0;
                databuff_printf(delete_buff,512,pos,"delete from __all_table_privilege where user_id=%lu and db_id=%lu and table_id=%lu",
                                udi.userid_,udi.dbid_,tid);
                if( pos>= 511)
                {
                    ret = OB_BUF_NOT_ENOUGH;
                    YYSYS_LOG(WARN,"buffer overflow ret=%d",ret);
                }
                else
                {
                    delete_stmt.assign_ptr(delete_buff,static_cast<ObString::obstr_size_t>(pos));
                    local_context_.disable_privilege_check_=true;
                    ObResultSet local_result;
                    if(OB_SUCCESS !=(ret =local_result.init()))
                    {
                        YYSYS_LOG(WARN,"init result set failed,ret=%d",ret);
                    }
                    else if(OB_SUCCESS !=(ret=ObSql::direct_execute(delete_stmt,local_result,local_context_)))
                    {
                        YYSYS_LOG(WARN,"clean privilege from dropped table failed, sql=%.*s, ret=%d",delete_stmt.length(),delete_stmt.ptr(),ret);
                    }
                    else if(OB_SUCCESS !=(ret =local_result.open()))
                    {
                        YYSYS_LOG(WARN,"open result set failed,ret=%d",ret);
                    }
                    else
                    {
                        OB_ASSERT(local_result.is_with_rows() == false);
                        int64_t affected_rows = local_result.get_affected_rows();
                        if(affected_rows ==0)
                        {
                            YYSYS_LOG(DEBUG,"test ::lmz,0 rows affected,sql = %.*s",delete_stmt.length(),delete_stmt.ptr());
                        }
                        int err =local_result.close();
                        if(OB_SUCCESS != err)
                        {
                            YYSYS_LOG(WARN,"close result set failed,ret=%d",ret);
                        }
                            local_result.reset();
                        }
                    }
            }

        }
    }

    if(local_context_.session_info_->get_autocommit()==false)
    {
        ret=commit();
    }

    if(local_context_.schema_manager_!=NULL)
    {
        int err=local_context_.merger_schema_mgr_->release_schema(local_context_.schema_manager_);
        if(OB_SUCCESS!=err)
        {
            YYSYS_LOG(WARN,"release schema failed,ret=%d",err);
        }
        else
        {
            local_context_.schema_manager_=NULL;
        }
    }
}

int ObDropView::close()
{
    int ret=OB_SUCCESS;
    return ret;
}


namespace oceanbase
{
    namespace sql
    {
         REGISTER_PHY_OPERATOR(ObDropView,PHY_DROP_VIEW);
    }

}

int64_t ObDropView::to_string(char *buf, const int64_t buf_len) const
{
    int64_t pos=0;
    databuff_printf(buf,buf_len,pos,"DropView(if_exists=%c views=[",if_exists_ ? 'Y' :'N');
    pos+=views_.to_string(buf+pos,buf_len -pos);
    databuff_printf(buf,buf_len,pos,"])\n");
    return pos;
}


