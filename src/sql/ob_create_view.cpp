//add lvjc [create view]
#include <yylog.h>
#include "ob_create_view.h"
#include "common/ob_privilege.h"
#include "common/utility.h"
#include "mergeserver/ob_rs_rpc_proxy.h"
#include "sql/ob_result_set.h"
#include "sql/ob_sql.h"
//add liu jun [Multiups] [part_cache]
#include "mergeserver/ob_ms_partition_manager.h"
//add liu jun e
#include "common/ob_privilege_type.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

ObCreateView::ObCreateView()
{
    do_replace_ = false;
}

ObCreateView::~ObCreateView()
{
}

void ObCreateView::reset()
{
    do_replace_ = false;
    local_context_.rs_rpc_proxy_ = NULL;
}

void ObCreateView::reuse()
{
    do_replace_ = false;
    local_context_.rs_rpc_proxy_ = NULL;
}

void ObCreateView::set_sql_context(const ObSqlContext  &context)
{
    local_context_ = context;
    local_context_.schema_manager_ = NULL;
}

int ObCreateView::open()
{
    int ret = OB_SUCCESS;

    if (NULL == local_context_.rs_rpc_proxy_ || 0 >= strlen(table_schema_.table_name_))
    {
        ret = OB_NOT_INIT;
        YYSYS_LOG(ERROR, "not init, rpc_=%p", local_context_.rs_rpc_proxy_);
    }
    //zhangxu [forbid_execute_DDL_in_transaction]
    else if (local_context_.session_info_->get_trans_start() || local_context_.session_info_->get_trans_id().is_valid())
    {
        YYSYS_LOG(WARN, "create view is not allowed in transaction, err=%d", ret);
        ret = OB_ERR_TRANS_ALREADY_STARTED;
    }
    else if (OB_SUCCESS != (ret = local_context_.rs_rpc_proxy_->create_view(do_replace_, table_schema_)))
    {
        YYSYS_LOG(WARN, "failed to create view, err=%d", ret);
    }
    else
    {
        //还没有赋予权限即成功，如果创建成功了，但是赋予权限失败，由DBA介入
        YYSYS_LOG(INFO, "create view succ, view_name=%s or_replace=%c",
                  table_schema_.table_name_, do_replace_ ? 'Y' : 'N');
        grant_owner_privilege();
    }

    return ret;
}

//add liumz, [multi_database.create_table]
void construct_grant_table_privilege_stmt(char *buf, int buf_size, int64_t &pos, const ObString &user_name, const ObString db_name, const ObString table_name, const ObBitSet<> &privileges)
{
    databuff_printf(buf, buf_size, pos, "GRANT ");
    if (privileges.has_member(OB_PRIV_ALL))
    {
        databuff_printf(buf, buf_size, pos, "ALL PRIVILEGES,");
        if (privileges.has_member(OB_PRIV_GRANT_OPTION))
        {
            databuff_printf(buf, buf_size, pos, "GRANT OPTION,");
        }
    }
    else
    {
        if (privileges.has_member(OB_PRIV_ALTER))
        {
            databuff_printf(buf, buf_size, pos, "ALTER,");
        }
        if (privileges.has_member(OB_PRIV_CREATE))
        {
            databuff_printf(buf, buf_size, pos, "CREATE,");
        }
        if (privileges.has_member(OB_PRIV_CREATE_USER))
        {
            databuff_printf(buf, buf_size, pos, "CREATE USER,");
        }
        if (privileges.has_member(OB_PRIV_DELETE))
        {
            databuff_printf(buf, buf_size, pos, "DELETE,");
        }
        if (privileges.has_member(OB_PRIV_DROP))
        {
            databuff_printf(buf, buf_size, pos, "DROP,");
        }
        if (privileges.has_member(OB_PRIV_GRANT_OPTION))
        {
            databuff_printf(buf, buf_size, pos, "GRANT OPTION,");
        }
        if (privileges.has_member(OB_PRIV_INSERT))
        {
            databuff_printf(buf, buf_size, pos, "INSERT,");
        }
        if (privileges.has_member(OB_PRIV_UPDATE))
        {
            databuff_printf(buf, buf_size, pos, "UPDATE,");
        }
        if (privileges.has_member(OB_PRIV_SELECT))
        {
            databuff_printf(buf, buf_size, pos, "SELECT,");
        }
        if (privileges.has_member(OB_PRIV_REPLACE))
        {
            databuff_printf(buf, buf_size, pos, "REPLACE,");
        }
    }
    pos = pos - 1;
    databuff_printf(buf, buf_size, pos, " ON \"%.*s\".\"%.*s\" TO '%.*s'", db_name.length(), db_name.ptr(),
                    table_name.length(), table_name.ptr(), user_name.length(), user_name.ptr());
}

void ObCreateView::grant_owner_privilege()
{
    int ret = OB_SUCCESS;
    ObString user_name = my_phy_plan_->get_result_set()->get_session()->get_user_name();
    char grant_buff[512];
    ObString grant_stmt;
    int64_t pos = 0;
    ObString table_name;
    //add liu jun [multiups] [part_cache] ?????
    ObMsPartitionManager *temp = NULL;
    //add e
    //add liumz, [multi_database.create_table]
    bool is_regrant_priv = my_phy_plan_->get_result_set()->get_session()->is_regrant_priv();
    ObString db_name;
    db_name.assign_ptr(table_schema_.dbname_, static_cast<ObString::obstr_size_t>(strlen(table_schema_.dbname_)));
    //add e
    table_name.assign_ptr(table_schema_.table_name_, static_cast<ObString::obstr_size_t>(strlen(table_schema_.table_name_)));
    //modeify ddlphin [database manager]
    databuff_printf(grant_buff, 512, pos, "GRANT ALL PRIVILEGES, GRANT OPTION ON \"%s\".\"%.*s\" to '%.*s'",
                    table_schema_.dbname_, table_name.length(), table_name.ptr(),
                    user_name.length(), user_name.ptr());
    //modify e
    if (pos >= 511)
    {
        //overflow
        ret = OB_BUF_NOT_ENOUGH;
        YYSYS_LOG(WARN, "privilege buffer overflow ret=%d", ret);
    }
    else
    {
        grant_stmt.assign_ptr(grant_buff, static_cast<ObString::obstr_size_t>(pos));

        //重新获取schema
        //如果获取10次都获取不到最新的含有      的schema，则由DBA来进行处理
        static const int retry_times = 10;
        const ObSchemaManagerV2 *curr_schema_mgr = NULL;
        int i = 1;
        //add liu jun [MultiUPS] [part_cache] ?????
        if (NULL == (temp = dynamic_cast<ObMsPartitionManager *>
        (local_context_.part_monitor_->get_partition_manager())))
        {
            ret = OB_ERROR;
            YYSYS_LOG(WARN, "get ObMsPartitionManager failed");
        }
        else
        {
            local_context_.partition_mgr_ = temp;
            //add e
            for (; i <= retry_times; ++i)
            {
                curr_schema_mgr = local_context_.merger_schema_mgr_->get_user_schema(0);
                if (NULL == curr_schema_mgr)
                {
                    YYSYS_LOG(WARN, "%s 's schema is not available,retry", table_schema_.table_name_);
                    usleep(10 * 1000);// 10 ms
                }
                else
                {
                    //new table 's schema still not available

                    //add dolphin [database manger]
                    char buf[OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1];//add liumz [name_length]
                    size_t l = strlen(table_schema_.dbname_);
                    if (l > 0)
                    {
                        memcpy(buf, table_schema_.dbname_, l);
                        buf[l] = '.';
                        memcpy(buf + l + 1, table_schema_.table_name_, strlen(table_schema_.table_name_));
                        buf[l + 1 + strlen(table_schema_.table_name_)] = 0;
                    }
                    else
                    {
                        memcpy(buf, table_schema_.table_name_, strlen(table_schema_.table_name_));
                        buf[strlen(table_schema_.table_name_)] = 0;
                    }
                    //add e
                    if (NULL == curr_schema_mgr->get_table_schema(buf))
                    {
                        YYSYS_LOG(WARN, "%s 's schema is not available,retry", table_schema_.table_name_);
                        int err = local_context_.merger_schema_mgr_->release_schema(curr_schema_mgr);
                        if (OB_SUCCESS != err)
                        {
                            YYSYS_LOG(WARN, "release schema failed,ret=%d", err);
                        }
                    }
                    else
                    {
                        YYSYS_LOG(INFO, "get created table %s's schema success", table_schema_.table_name_);
                        local_context_.schema_manager_ = curr_schema_mgr;
                        break;
                    }
                }
            }  //end for
            if (i == retry_times + 1)
            {
                ret = OB_ERR_GRANT_PRIVILEGES_TO_CREATE_TABLE;
                //报警，让DBA使用admin账户来处理
                YYSYS_LOG(ERROR, "User: %.*s create table %s success, but grant all privileges, grant option on %s.%s to '%.*s' failed, ret=%d",
                          user_name.length(), user_name.ptr(), table_schema_.table_name_, table_schema_.dbname_, table_schema_.table_name_, user_name.length(), user_name.ptr(), ret);
                //mod e
            }
            if (OB_SUCCESS == ret)
            {
                local_context_.disable_privilege_check_ = true;
                ObResultSet local_result;
                if (OB_SUCCESS != (ret = local_result.init()))
                {
                    YYSYS_LOG(WARN, "init result set failed,ret=%d", ret);
                }
                else if (OB_SUCCESS != (ret = ObSql::direct_execute(grant_stmt, local_result, local_context_)))
                {
                    YYSYS_LOG(WARN, "grant privilege to created table failed, sql=%.*s, ret=%d", grant_stmt.length(), grant_stmt.ptr(), ret);
                }
                else if (OB_SUCCESS != (ret = local_result.open()))
                {
                    YYSYS_LOG(WARN, "open result set failed,ret=%d", ret);
                }
                else
                {
                    OB_ASSERT(local_result.is_with_rows() == false);
                    int err = local_result.close();
                    if (OB_SUCCESS != err)
                    {
                        YYSYS_LOG(WARN,"close result set failed,ret=%d",ret);
                    }
                    local_result.reset();
                }
            }

            if(OB_SUCCESS == ret && is_regrant_priv)
            {
                ObPrivilege::UserDbPriMap *user_db_priv = const_cast<ObPrivilege *>(*(local_context_.pp_privilege_))->get_user_database_privilege_map();
                ObPrivilege::UserDbPriMap::const_iterator it = user_db_priv->begin();
                for(; it != user_db_priv->end() && OB_LIKELY(OB_SUCCESS == ret); it++)
                {
                    if(OB_INVALID_ID == it->first.dbid_ || OB_DSADMIN_UID == it->first.userid_)
                        continue;
                    pos = 0;
                    ObString username;
                    ObString dbname;
                    ObBitSet<> db_priv;

                    if(OB_SUCCESS != (ret = (*(local_context_.pp_privilege_))->get_db_name(it->first.dbid_,dbname)))
                    {
                        YYSYS_LOG(WARN, "get db name failed, db_id[%lu], ret=%d", it->first.dbid_, ret);
                    }
                    else if(dbname == db_name)
                    {
                        if (OB_SUCCESS != (ret = (*(local_context_.pp_privilege_))->get_user_name(it->first.userid_, username)))
                        {
                            YYSYS_LOG(WARN, "get user name failed, user_id[%lu], ret=%d", it->first.userid_, ret);
                        }
                        else if (username != user_name)
                        {
                            db_priv = it->second.privileges_;
                            if(!db_priv.is_empty())
                            {
                                construct_grant_table_privilege_stmt(grant_buff, 512, pos, username, db_name, table_name, db_priv);
                                if(pos >= 511)
                                {
                                    ret = OB_BUF_NOT_ENOUGH;
                                    YYSYS_LOG(WARN, "privilege buffer overflow ret=%d", ret);
                                }
                                else
                                {
                                    grant_stmt.assign_ptr(grant_buff, static_cast<ObString::obstr_size_t>(pos));
                                    local_context_.disable_privilege_check_ = true;
                                    ObResultSet local_result;
                                    if(OB_SUCCESS != (ret = local_result.init()))
                                    {
                                        YYSYS_LOG(WARN, "init result set failed,ret=%d", ret);
                                    }
                                    else if(OB_SUCCESS != (ret = ObSql::direct_execute(grant_stmt, local_result, local_context_)))
                                    {
                                        YYSYS_LOG(WARN, "grant privilege to created table failed, sql=%.*s, ret=%d", grant_stmt.length(), grant_stmt.ptr(), ret);
                                    }
                                    else if (OB_SUCCESS != (ret = local_result.open()))
                                    {
                                        YYSYS_LOG(WARN, "open result set failed,ret=%d", ret);
                                    }
                                    else
                                    {
                                        OB_ASSERT(local_result.is_with_rows() == false);
                                        int err = local_result.close();
                                        if(OB_SUCCESS != err)
                                        {
                                            YYSYS_LOG(WARN, "close result set failed,ret=%d", ret);
                                        }
                                        local_result.reset();
                                    }
                                }
                            }
                        }
                    }
                }
                if(OB_SUCCESS != ret)
                {
                    YYSYS_LOG(ERROR, "User: %.*s create table %s success, but regrant table privs to db owners failed, ret=%d",
                              user_name.length(), user_name.ptr(), table_schema_.table_name_, ret);
                }
            }
            if(local_context_.schema_manager_ != NULL)
            {
                int err = local_context_.merger_schema_mgr_->release_schema(local_context_.schema_manager_);
                if(OB_SUCCESS != err)
                {
                    YYSYS_LOG(WARN, "release schema failed,ret=%d", err);
                }
                else
                {
                    local_context_.schema_manager_ = NULL;
                }
            }
            if(temp != NULL)
            {
                int err = local_context_.part_monitor_->release_manager(temp);
                if(OB_SUCCESS != err)
                {
                    YYSYS_LOG(WARN, "release partition manager failed, ret=%d", err);
                }
                local_context_.partition_mgr_ = NULL;
            }
        }
    }
}

int ObCreateView::close()
{
    int err = OB_SUCCESS;
    return err;
}

namespace oceanbase
{
    namespace sql
    {
        REGISTER_PHY_OPERATOR(ObCreateView, PHY_CREATE_VIEW);
    }
}

int64_t ObCreateView::to_string(char *buf, const int64_t buf_len) const
{
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "CreateView(do_replace=%s, ", do_replace_ ? "TRUE" : "FALSE");
    databuff_printf(buf, buf_len, pos, "view_name=%s, ", table_schema_.table_name_);
    databuff_printf(buf, buf_len, pos, "table_id=%lu, ", table_schema_.table_id_);
    databuff_printf(buf, buf_len, pos, "table_type=VIEW");
    return pos;
}
//add lvjc[create view] e
