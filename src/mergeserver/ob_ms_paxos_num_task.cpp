#include "ob_ms_paxos_num_task.h"
#include "ob_merge_server_service.h"

using namespace oceanbase::mergeserver;

const ObString& ObMergerPaxosNumTask::query = ObString::make_string("select /*+read_consistency(STRONG)*/ value from __all_sys_config_stat where name = 'use_paxos_num'");

ObMergerPaxosNumTask::ObMergerPaxosNumTask()
{
}

ObMergerPaxosNumTask::~ObMergerPaxosNumTask()
{
}

int ObMergerPaxosNumTask::init(ObMergeServerService *service, ObMsSQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  if(NULL != service && NULL != sql_proxy)
  {
    service_ = service;
    sql_proxy_ = sql_proxy;
  }
  else
  {
    ret = OB_ERROR;
  }
  return ret;
}

void ObMergerPaxosNumTask::runTimerTask(void)
{
  int ret = OB_SUCCESS;
  ObSQLResultSet sql_rs;
  ObResultSet &result = sql_rs.get_result_set();
  ObSQLSessionInfo session;
  ObSqlContext context;
  int64_t schema_version = 0;
  if(OB_SUCCESS != (ret = sql_proxy_->init_sql_env(context, schema_version, sql_rs, session)))
  {
    YYSYS_LOG(WARN, "init sql env error, err=%d", ret);
  }
  else
  {
    if(OB_SUCCESS != (ret = sql_proxy_->execute(query, sql_rs, context, schema_version)))
    {
      YYSYS_LOG(WARN, "execute paxos num sql failed,sql=%.*s, ret=%d", query.length(), query.ptr(), ret);
    }
    else if(OB_SUCCESS != (ret = result.open()))
    {
      YYSYS_LOG(ERROR, "open result failed, ret=%d", ret);
    }
    else
    {
      const ObRow *row = NULL;
      while (OB_SUCCESS == ret)
      {
        ret = result.get_next_row(row);
        if(OB_ITER_END == ret)
        {
          break;
        }
        else if(OB_SUCCESS != ret)
        {
          YYSYS_LOG(ERROR, "failed to get next row, ret=%d", ret);
        }
        else
        {
          const ObObj *pcell = NULL;
          uint64_t table_id = OB_INVALID_ID;
          uint64_t column_id = OB_INVALID_ID;
          ObString value;
          ret = row->raw_get_cell(0, pcell, table_id, column_id);
          if(OB_SUCCESS == ret)
          {
            if(pcell->get_type() == ObVarcharType)
            {
              if(OB_SUCCESS != (ret = pcell->get_varchar(value)))
              {
                YYSYS_LOG(WARN, "failed to get varchar from ObObj, ret=%d", ret);
              }
              else
              {
                char **endptr = NULL;
                int64_t num = strtol(value.ptr(), endptr, 10);
                if(num > 0 && num < MAX_UPS_COUNT_ONE_CLUSTER)
                {
                  service_->set_use_paxos_num(num);
                }
                else
                {
                  YYSYS_LOG(WARN, "use paxos num in the bad range,use_paxos_num=%ld", num);
                }
              }
            }
            else
            {
              YYSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
            }
          }
        }
      }
    }
    sql_proxy_->cleanup_sql_env(context, sql_rs);
  }
}
