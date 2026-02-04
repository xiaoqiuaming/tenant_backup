/* add wuna [MultiUPS] [sql_api] 20160421*/
#include "ob_root_delete_table_rules_task.h"
#include "rootserver/ob_root_worker.h"

namespace oceanbase
{
 namespace rootserver
 {
  int ObRootTableRulesDeleteTask::init(ObRootWorker *worker)
  {
    int ret = OB_SUCCESS;
    if (NULL == worker)
    {
      YYSYS_LOG(WARN, "invalid argument. worker=NULL.");
      ret = OB_INVALID_ARGUMENT;
    }
    else
    {
      worker_ = worker;
    }
    return ret;
  }
  //mod yuanzp 20170922:b
  void ObRootTableRulesDeleteTask::runTimerTask()
  {
    ObRootServer2& rs = worker_->get_root_server();
    if (rs.is_master ())
    {
      int ret = OB_SUCCESS;
      ObRootMsProvider& ms_provider = rs.get_ms_provider();
      int64_t last_frozen_mem_version = rs.get_last_frozen_version();
      const static  int64_t MIN_SAVE_VERSION_INTERVAL = 10;
      const static  int64_t MAX_CHECK_VERSION_NUMS = 3;
      bool is_merged = false;
      int64_t check_version_num = 0;
      while(true)
      {
        YYSYS_LOG(DEBUG,"is_merged[%d],last_frozen_mem_version:[%ld],check_version_num:[%ld]",is_merged,last_frozen_mem_version,check_version_num);
        if (is_merged
            || (last_frozen_mem_version <= OB_UPS_START_MAJOR_VERSION + MIN_SAVE_VERSION_INTERVAL)
            || check_version_num > MAX_CHECK_VERSION_NUMS)
        {
          break;
        }
        rs.check_tablet_version(--last_frozen_mem_version, 0, is_merged);
        ++check_version_num;
      }

      if (is_merged)
      {
        ObServer ms_server;
        ms_provider.get_ms(ms_server, true);
        uint64_t start_table_id = OB_INVALID_ID;
        uint64_t end_table_id = OB_INVALID_ID;
        uint64_t max_table_id = 0;
        if (OB_SUCCESS != (ret = rs.get_schema_service()->get_max_used_table_id(max_table_id) ))
        {
          YYSYS_LOG(WARN, "fail to get max used table id. ret=%d", ret);
        }
        else
        {
            for(start_table_id = 3001; start_table_id <= max_table_id; start_table_id = end_table_id )
            {
                end_table_id = start_table_id + TABLE_INTERVAL;
                char buf[OB_MAX_SQL_LENGTH];
                memset(buf, 0, OB_MAX_SQL_LENGTH);
                int64_t pos = 0;
                ObString select_sql;
                ObRow row;
                ObSQLResultSet res;

                databuff_printf(buf, OB_MAX_SQL_LENGTH, pos,
                                "select table_id,start_version from __all_table_rules where table_id >= %ld and table_id < %ld \
                                and start_version < %ld order by table_id,start_version desc;",
                                start_table_id,end_table_id,last_frozen_mem_version - MIN_SAVE_VERSION_INTERVAL);

                if(pos >= OB_MAX_SQL_LENGTH)
                {
                  ret = OB_BUF_NOT_ENOUGH;
                  YYSYS_LOG(WARN, "buffer not enough, ret=%d", ret);
                  break;
                }
                else
                {
                  select_sql.assign(buf,static_cast<ObString::obstr_size_t>(pos));
                  YYSYS_LOG(INFO, "select_str=%.*s", select_sql.length(), select_sql.ptr());
                  if (OB_SUCCESS != (ret = worker_->get_rpc_stub().execute_sql(ms_server, select_sql, res)))
                  {
                    YYSYS_LOG(WARN, "select table rules info failed, ret=%d", ret);
                  }
                  else
                  {
                    int64_t pre_table_id = OB_INVALID_ID;
                    int64_t table_id = OB_INVALID_ID;
                    int64_t pre_start_version = OB_INVALID_INDEX;
                    int64_t start_version = OB_INVALID_INDEX;
                    const common::ObObj *column = NULL;

                    while(true)
                    {
                      ret = res.get_new_scanner().get_next_row(row);
                      if(OB_ITER_END == ret)
                      {
                        ret = OB_SUCCESS;
                        break;
                      }
                      else if(OB_SUCCESS != ret)
                      {
                        YYSYS_LOG(WARN, "get next row from ObResultSet failed,ret=%d", ret);
                        break;
                      }
                      else
                      {
                        if (OB_SUCCESS != (ret = row.raw_get_cell(0, column)))
                        {
                          YYSYS_LOG(WARN, "get table_id cell failed, ret=%d", ret);
                          break;
                        }
                        else if (OB_SUCCESS != (ret = column->get_int(table_id)))
                        {
                          YYSYS_LOG(WARN, "get table_id value failed, ret=%d", ret);
                          break;
                        }
                        else if (OB_SUCCESS != (ret = row.raw_get_cell(1, column)))
                        {
                          YYSYS_LOG(WARN, "get start_version cell failed, ret=%d", ret);
                          break;
                        }
                        else if (OB_SUCCESS != (ret = column->get_int(start_version)))
                        {
                          YYSYS_LOG(WARN, "get start_version value failed, ret=%d", ret);
                          break;
                        }
                        else
                        {
                          if(pre_table_id != table_id)
                          {
                            pre_table_id = table_id;
                          }
                          else //pre_table_id == table_id
                          {
                            if(pre_start_version < last_frozen_mem_version - MIN_SAVE_VERSION_INTERVAL)
                            {
                              if(OB_SUCCESS != (ret=execute_delete_table_rules(ms_server,table_id,start_version)))
                              {
                                YYSYS_LOG(WARN, "execute_delete_table_rules failed, ret=%d", ret);
                              }
                            }
                          }
                        }
                        pre_start_version = start_version;
                      }
                    }
                    
                  }
                }
            }

        }

      }
    }
  }
  //mod 20170922:e
  int ObRootTableRulesDeleteTask::execute_delete_table_rules(const common::ObServer& ms_server,int64_t table_id,int64_t start_version)
  {
    int ret=OB_SUCCESS;
    char buf[OB_MAX_SQL_LENGTH];
    memset(buf, 0, OB_MAX_SQL_LENGTH);
    int64_t pos = 0;
    ObString delete_sql;
    databuff_printf(buf, OB_MAX_SQL_LENGTH, pos,
                    "delete from __all_table_rules where table_id=%ld and start_version=%ld;",table_id,start_version);

    if(pos >= OB_MAX_SQL_LENGTH)
    {
      ret = OB_BUF_NOT_ENOUGH;
      YYSYS_LOG(WARN, "buffer not enough, ret=%d", ret);
    }
    else
    {
      delete_sql.assign(buf,static_cast<ObString::obstr_size_t>(pos));
      YYSYS_LOG(INFO, "delete_str=%.*s", delete_sql.length(), delete_sql.ptr());
      if (OB_SUCCESS != (ret = worker_->get_rpc_stub().execute_sql(ms_server, delete_sql, TIMEOUT)))
      {
        YYSYS_LOG(WARN, "delete table rules info failed, ret=%d,table_id=%ld,start_version=%ld", ret,table_id,start_version);
      }
    }
    return ret;
  }


 }
}
