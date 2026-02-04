/*
*   Version: 0.1
*
*   Authors:
*      liu jun
*        - some work details if you want
*/

#include "ob_ms_partition_manager.h"
#include "ob_merge_server_service.h"

ObMsPartitionManager::ObMsPartitionManager(ObMergeServerService *service, const ObClientManager *config_mgr) :
  service_(service),
  client_mgr_(config_mgr)
{
  set_server_type(ObPartitionManager::MergeServer);
}

ObMsPartitionManager::ObMsPartitionManager(ObMergeServerService *service, const ObClientManager *config_mgr, int64_t frozen_version_before_promote) :
  service_(service),
  client_mgr_(config_mgr),
  frozen_version_before_promote_(frozen_version_before_promote)
{
    set_server_type(ObPartitionManager::MergeServer);
}

ObMsPartitionManager::~ObMsPartitionManager()
{
  destroy();
}

void ObMsPartitionManager::destroy()
{
  service_ = NULL;
  client_mgr_ = NULL;
  
}

int ObMsPartitionManager::get_table_part_type(const uint64_t table_id, bool &is_table_level)
{
  int ret = OB_SUCCESS;
  //add lqc  [MultiUPS] [index_partition] 20170318:b
  const ObSchemaManagerV2 *schema = NULL;
  if(OB_SUCCESS == ret)
  {
    if(NULL == (schema = service_->get_schema_mgr()->get_schema(table_id)))
    {
      YYSYS_LOG(WARN, "get schema fialed, ret=%d", ret);
    }
  }
  //add e
  int64_t version = get_frozen_version() + 1;
  if(OB_SUCCESS != (ret = ObPartitionManager::get_table_part_type(table_id, version,schema, is_table_level)))
  {
    YYSYS_LOG(WARN, "get table partition type faild ret=%d",ret);
  }
  else
  {
    //NOTHING TODO
  }
  //add lqc  [MultiUPS] [index_partition] 20170318:b
  if(NULL != schema)
  {
    if(OB_SUCCESS != (service_->get_schema_mgr()->release_schema(schema)))
    {
      YYSYS_LOG(ERROR, "failed to release schema ret[%d]", ret);
    }
    schema = NULL;
  }
  //add e
  return ret;
}
//add shili [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20150806:b
int ObMsPartitionManager::get_paxos_id(const ObCalcInfo &calc_stru, int32_t &paxos_id)
{
  int ret=OB_SUCCESS;
  ret = get_paxos_id(calc_stru.get_table_id(), calc_stru, paxos_id);
  if(OB_SUCCESS != ret && OB_GROUP_NOT_EXIST != ret)
  {
    YYSYS_LOG(WARN, "failed to get paxos id, err=%d", ret);
  }
  else if(OB_GROUP_NOT_EXIST == ret)
  {
    paxos_id = 0;
    ret = OB_SUCCESS;
  }
  return ret;
}
//add 20150806::e

int ObMsPartitionManager::get_paxos_id(uint64_t table_id, const ObCalcInfo &calc_stru, int32_t &paxos_id, bool * is_get_pre_paxos_id/*NULL*/)
{
  int ret = OB_SUCCESS;
  //add lqc  [MultiUPS] [index_partition] 20170318:b
  const ObSchemaManagerV2 *schema = NULL;
  if(OB_SUCCESS == ret)
  {
    if(NULL ==(schema = service_->get_schema_mgr()->get_schema(table_id)))
    {
      YYSYS_LOG(WARN, "get schema failed, ");
    }
  }
  //add e
  //mod lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160118:b
  /**
   * In physical transformer, get paxos id twice to judge the rule for current table has been changed or not.
   */
  //int64_t version = get_frozen_version() + 1;
  int64_t version = OB_INVALID_VERSION;
  version = get_frozen_version() + 1;
  if (NULL != is_get_pre_paxos_id)
  {
    if (*is_get_pre_paxos_id)
    {
      version -= 1;
    }
  }
  if (1 == version)//the start version is 2, if 1 == version, no rules
  {
    paxos_id = OB_INVALID_PAXOS_ID;
  }
  else
  {
    ret = ObPartitionManager::get_paxos_id(table_id, calc_stru, version,schema, paxos_id);
    if(OB_SUCCESS != ret)
    {
      /**
       * for MS, get paxos id will occur when do update/insert/replace/delete,some version may get none
       * from rule cache,there are some internal errors is normaly.
       */
      if (IS_PARTITION_INTERNAL_ERR(ret))
      {
        YYSYS_LOG(DEBUG, "partition internal err,ret=%d",ret);
        paxos_id = OB_INVALID_PAXOS_ID;
        ret = OB_SUCCESS;
      }
      else
      {
        YYSYS_LOG(WARN, "get paxos id failed, ret=%d", ret);
      }
    }
  //mod 20160118:e
  }
  //add lqc  [MultiUPS] [index_partition] 20170318:b
  if(NULL != schema)
  {
    if(OB_SUCCESS != (service_->get_schema_mgr()->release_schema(schema)))
    {
      YYSYS_LOG(ERROR, "failed to release schema ");
    }
    schema = NULL;
  }
  //add e
  return ret;
}

int ObMsPartitionManager::execute_sql(const ObString &query_sql, ObSQLResultSet &rs)
{
  int ret = OB_SUCCESS;
  const ObServer &ms_server = service_->get_server();
  if(0 == ms_server.get_port())
  {
    YYSYS_LOG(WARN, "self can't work");
    ret = OB_NOT_INIT;
  }
  else
  {
    char buff[OB_MAX_PACKET_LENGTH];
    ObDataBuffer msgbuf(buff, OB_MAX_PACKET_LENGTH);
    int64_t session_id = 0;
    if (OB_SUCCESS != (ret = query_sql.serialize(msgbuf.get_data(),
                                              msgbuf.get_capacity(),
                                              msgbuf.get_position())))
    {
      YYSYS_LOG(ERROR, "failed to serialize, err = [%d]", ret);
    }
    else if(OB_SUCCESS !=
            (ret = client_mgr_->send_request(ms_server,OB_SQL_EXECUTE,
                                             DEFAULT_VERSION, TIMEOUT,
                                             msgbuf, session_id)))
    {
      YYSYS_LOG(ERROR, "failed to send request, err = [%d]", ret);
    }
    else
    {
      bool fullfilled = true;
      bool empty = true;
      do
      {
        msgbuf.get_position() = 0;
        if(OB_SUCCESS !=
           (ret = rs.deserialize(msgbuf.get_data(), msgbuf.get_capacity(),
                                 msgbuf.get_position())))
        {
          YYSYS_LOG(ERROR,
                    "fail to deserialize result buffer, ret = [%d]\n", ret);
        }
        else if (OB_SUCCESS != rs.get_errno())
        {
          YYSYS_LOG(WARN, "fail to exeucte sql: [%s], errno: [%d]",
                    to_cstring(rs.get_sql_str()), rs.get_errno());
          ret = rs.get_errno();
          break;
        }
        else
        {
          empty = rs.get_new_scanner().is_empty();
          rs.get_fullfilled(fullfilled);
          if (fullfilled || empty)
          {
            break;
          }
          else
          {
            msgbuf.get_position() = 0;
            if (OB_SUCCESS != (ret = client_mgr_->get_next(
                                 ms_server, session_id, TIMEOUT,
                                 msgbuf, msgbuf)))
            {
              YYSYS_LOG(ERROR, "failted to send get_next, ret = [%d]", ret);
              break;
            }
          }
        }
      } while (OB_SUCCESS == ret);
    }
  }
  return ret;
}

/*
int64_t ObMsPartitionManager::get_frozen_version() const
{
  return service_->get_frozen_version();
}
*/

int64_t ObMsPartitionManager::get_frozen_version(bool for_update_all_rules_use) const
{
    return for_update_all_rules_use ? frozen_version_before_promote_ +1 : service_->fetch_frozen_version();
}

//add liuzy [MultiUPS] [take_paxos_offline_interface] 20160228:b
/// @brief ms will send request to renew current paxos offline bitset from rs, when version of bitset unequal to
/// current frozen version of ms. Because base class can not get rs ip, this request is executed by ms.
int ObMsPartitionManager::renew_paxos_usable_view() const
{
  int ret = OB_SUCCESS;
  char buff[OB_MAX_PACKET_LENGTH];
  ObDataBuffer msgbuf(buff, OB_MAX_PACKET_LENGTH);
  const ObServer &root_server = service_->get_root_server();
  if (0 == root_server.get_ipv4())
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(WARN, "get root server failed, err = [%d]", ret);
  }
  else if (OB_SUCCESS != (ret = client_mgr_->send_request(root_server, OB_RS_GET_CURRENT_PAXOS_USABLE_VIEW,
                                                          DEFAULT_VERSION, TIMEOUT, msgbuf)))
  {
    YYSYS_LOG(ERROR, "failed to send request, err = [%d]", ret);
  }
  else
  {
    msgbuf.get_position() = 0;
    int64_t view_version = 0;
    ObBitSet<MAX_UPS_COUNT_ONE_CLUSTER> new_set;
    if (OB_SUCCESS != (ret = serialization::decode_vi64(msgbuf.get_data(), msgbuf.get_capacity(),
                                                            msgbuf.get_position(), &view_version)))
    {
      YYSYS_LOG(WARN, "failed to deserialize current version, err = [%d]", ret);
    }
    else if (OB_SUCCESS != (ret = new_set.deserialize(msgbuf.get_data(), msgbuf.get_capacity(),
                                                          msgbuf.get_position())))
    {
      YYSYS_LOG(WARN, "failed to deserialize paxos usable bit set, err = [%d]", ret);
    }
    else
    {
      paxos_usable_view_.set_view(view_version, new_set);
    }
  }
  return ret;
}
//add 20160228:e
