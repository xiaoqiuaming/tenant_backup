/*
*   Version: 0.1
*
*   Authors:
*      liu jun
*        - some work details if you want
*/

#include "ob_cs_partition_manager.h"
#include "ob_chunk_service.h"
#include "common/ob_config_manager.h"

using namespace oceanbase::chunkserver;

ObCsPartitionManager::ObCsPartitionManager(ObChunkService *service, ObConfigManager *config_mgr)
  :service_(service),
   config_mgr_(config_mgr)
{
  set_server_type(ObPartitionManager::ChunkServer);
}

ObCsPartitionManager::ObCsPartitionManager(ObChunkService *service, ObConfigManager *config_mgr, int64_t frozen_version_sync_from_rs_before_promote)
    :service_(service),
      config_mgr_(config_mgr),
      frozen_version_sync_from_rs_before_promote_(frozen_version_sync_from_rs_before_promote)
{
  set_server_type(ObPartitionManager::ChunkServer);
}

ObCsPartitionManager::~ObCsPartitionManager()
{
  destroy();
}

void ObCsPartitionManager::destroy()
{
  service_ = NULL;
  config_mgr_ = NULL;
}

//add lijianqiang [MultiUPS] [MERGE] 20160329:b
int ObCsPartitionManager::get_table_part_type_for_daily_merge(const uint64_t table_id, bool &is_table_level)
{
  int ret = OB_SUCCESS;
  //add lqc  [MultiUPS] [index_partition] 20170318:b
  const ObSchemaManagerV2 *schema = NULL;
  if (OB_SUCCESS == ret)
  {
    if(NULL == (schema = service_->get_schema_manag()->get_schema(table_id)))
    {
      YYSYS_LOG(WARN, "get schema failed     ");
    }
  }
  //add e
  /**
   * DAILY MERGE:
   *   there is maybe more than one major_freeze happened during the daily merge,
   *   the valid version for current merge is CS serving_data_version + 1, not
   *   frozen_data_version + 1.only get one version(serving_data_version + 1) data from UPS.
   */
  int64_t data_version = get_serving_data_version() > 0 ? get_serving_data_version() + 1 : 2;
  if(OB_SUCCESS !=(ret = ObPartitionManager::get_table_part_type(table_id, data_version,schema, is_table_level))
          && OB_TABLE_NODE_NOT_EXIST != ret) //[547]
  {
    YYSYS_LOG(WARN,"get table partition failed");
  }
  else if(OB_TABLE_NODE_NOT_EXIST == ret)//[547]
  {
      is_table_level = false;
      ret = OB_SUCCESS;
      YYSYS_LOG(INFO, "cur table has no table rule node, use all paxos group");
  }


  //add lqc  [MultiUPS] [index_partition] 20170318:b
  if(NULL != schema)
  {
    if(OB_SUCCESS != (service_->get_schema_manag()->release_schema(schema)))
    {
      YYSYS_LOG(ERROR, "failed to release schema ");
    }
    schema = NULL;
  }
  //add e
  return ret;
}

//int ObCsPartitionManager::get_table_part_type_for_select(const uint64_t table_id, bool &is_table_level)
int ObCsPartitionManager::get_table_part_type_for_select(const uint64_t table_id, bool &is_table_level, int64_t version)
{
  int ret = OB_SUCCESS;
  //add lqc  [MultiUPS] [index_partition] 20170318:b
  const ObSchemaManagerV2 *schema = NULL;
  if(OB_SUCCESS == ret)
  {
     if(NULL == (schema = service_->get_schema_manag()->get_schema(table_id)))
    {
      YYSYS_LOG(WARN, "get schema failed     ");
    }
  }
  //add e
  /**
   * SELECT STMT:
   *   when getting data from UPS,need to judge the table partition type in table_level or row_level,
   *   maybe there are more than one version if major frozen happened during select.Also, if there are
   *   multiple frozen versions,when U create table,the serving data version may has no table_node for
   *   the table U just create,so, be careful!
   */
  int64_t start_version = get_serving_data_version() > 0 ? get_serving_data_version() + 1 : 2;
  int64_t end_version = get_frozen_version() + 2;

  //[495]
  if(version != OB_INVALID_VERSION && version < start_version)
  {
      start_version = version;
  }

  YYSYS_LOG(DEBUG, "current serving data version is=%ld",get_serving_data_version());
  for (int64_t i=start_version; OB_SUCCESS == ret && i<end_version; i++)
  {
    ret = ObPartitionManager::get_table_part_type(table_id, i,schema ,is_table_level);//mod lqc  [MultiUPS] [index_partition] 20170318:b

    if (OB_TABLE_NODE_NOT_EXIST == ret)
    {
      YYSYS_LOG(INFO, "table node not exist,table id=%ld,fetch version=%ld,ret=%d",table_id, i, ret);
      ret = OB_SUCCESS;
      continue;
    }
    else if (OB_SUCCESS == ret)
    {
      /// @note maybe all version with table level
      if (false == is_table_level)
      {
        YYSYS_LOG(DEBUG, "not table level, the version is=%ld",i);
        break;
      }
    }
    else if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "failed to get table partition type, fetch version=%ld, ret=%d", i, ret);
    }
  }
  //add lqc  [MultiUPS] [index_partition] 20170318:b
  if(NULL != schema)
  {
    if(OB_SUCCESS != (service_->get_schema_manag()->release_schema(schema)))
    {
      YYSYS_LOG(ERROR, "failed to release schema ");
    }
    schema = NULL;
  }
  //add e
  return ret;
}

int ObCsPartitionManager::get_paxos_id_without_partition_for_daily_merge(const uint64_t& table_id, VersionPaxosId& paxos_version)
{
  int ret = OB_SUCCESS;
  int32_t paxos_id = OB_INVALID_PAXOS_ID;
  ObCalcInfo calc_struc;
  calc_struc.set_table_id(table_id);
  //add lqc  [MultiUPS] [index_partition] 20170318:b
  const ObSchemaManagerV2 *schema=NULL;
  if(OB_SUCCESS == ret)
  {
    if(NULL == (schema = service_->get_schema_manag()->get_schema(table_id)))
    {
      YYSYS_LOG(WARN, "get schema failed     ");
    }
  }
  //add e
  int64_t real_version = get_serving_data_version() > 0 ? get_serving_data_version() + 1 : 2;
  ret = ObPartitionManager::get_paxos_id(table_id, calc_struc, real_version,schema, paxos_id);
  if (OB_SUCCESS != ret && OB_GROUP_NOT_EXIST != ret)
  {
    YYSYS_LOG(WARN, "get paxos id failed, version is=%ld,ret=%d", real_version, ret);
  }
  else if (OB_GROUP_NOT_EXIST == ret)
  {
    //cur table may no data,so no group node,run with empty result.
    paxos_version.paxos_id_ = 0;
    paxos_version.version_ = real_version;
    YYSYS_LOG(INFO, "cur table has no data without group node,table id[%ld]",table_id);
    ret = OB_SUCCESS;
  }
  else
  {
    paxos_version.paxos_id_ = paxos_id;
    paxos_version.version_ = real_version;
  }
   //add lqc  [MultiUPS] [index_partition] 20170318:b
  if(NULL != schema)
  {
    if(OB_SUCCESS != (service_->get_schema_manag()->release_schema(schema)))
    {
      YYSYS_LOG(ERROR, "failed to release schema ");
    }
    schema = NULL;
  }
  //add e
  return ret;
}

//int ObCsPartitionManager::get_paxos_id_without_partition_for_select(const uint64_t& table_id, ObArray<VersionPaxosId>& paxos_id_array)
int ObCsPartitionManager::get_paxos_id_without_partition_for_select(const uint64_t &table_id, ObArray<VersionPaxosId> &paxos_id_array, int64_t version)
{
  int ret = OB_SUCCESS;
  ObCalcInfo calc_struc;
  calc_struc.set_table_id(table_id);
  int64_t start_version = get_serving_data_version() > 0 ? get_serving_data_version() + 1 : 2;
  int64_t end_version = get_frozen_version() + 2;

  //[495]
  if(version != OB_INVALID_VERSION && version < start_version)
  {
      start_version = version;
  }

  int32_t paxos_id = OB_INVALID_PAXOS_ID;
  VersionPaxosId version_paxos_id; 
  //add lqc  [MultiUPS] [index_partition] 20170318:b
  const ObSchemaManagerV2 *schema = NULL;
  if(OB_SUCCESS == ret)
  {
     if(NULL == (schema = service_->get_schema_manag()->get_schema(table_id)))
    {
      YYSYS_LOG(WARN, "get schema failed     ");
    }
  }
  //add e
  /**
   * @note this interface is only used for all version of current table is partitioned in table level
   */
  for (int64_t i=start_version; OB_SUCCESS == ret && i<end_version; i++)
  {
     ret = ObPartitionManager::get_paxos_id(table_id, calc_struc, i,schema, paxos_id);
      if (IS_PARTITION_INTERNAL_ERR(ret))
    {
      YYSYS_LOG(INFO, "get invalid paxos id, version=%ld, ret=%d", i, ret);
      ret = OB_SUCCESS;
      continue;
    }
    if (OB_SUCCESS == ret)
    {
      version_paxos_id.paxos_id_ = paxos_id;
      version_paxos_id.version_ = i;
      if (OB_SUCCESS != (ret = paxos_id_array.push_back(version_paxos_id)))
      {
        YYSYS_LOG(ERROR, "add paxos_id to array failed,ret=%d",ret);
      }
    }
  }
  //add lqc  [MultiUPS] [index_partition] 20170318:b
  if(NULL != schema)
  {
    if(OB_SUCCESS != (service_->get_schema_manag()->release_schema(schema)))
    {
      YYSYS_LOG(ERROR, "failed to release schema ");
    }
    schema = NULL;
  }
  //add e
  return ret;
}

//add 20160329:e
//int ObCsPartitionManager::get_table_part_type(const uint64_t table_id, bool &is_table_level, bool is_for_daily_merge/*false*/)
//{
//  int ret = OB_SUCCESS;
//  //mod lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160303:b
//  //int64_t end_version = get_frozen_version();
//  /**
//   * In CS, the partition type is been used in two ways:
//   *   1.SELECT STMT,get data from UPS,need to judge the table partition type in table_level
//   * or row_level,maybe there are more than one version if major forozen happened during select.
//   *   2.DAILY MERGE,only get one version(serving_data_version + 1) data from UPS.
//   */
//  int64_t start_version = get_serving_data_version() + 1;
//  int64_t end_version = OB_INVALID_VERSION;
//  if (is_for_daily_merge)
//  {
//    end_version = start_version + 1;
//  }
//  else//for select
//  {
//    end_version = get_frozen_version() + 2;
//  }
//  for (int64_t i=start_version; OB_SUCCESS == ret && i<end_version; i++)
//  {
//    if (OB_SUCCESS != (ret = ObPartitionManager::get_table_part_type(table_id, i, is_table_level)))
//    {
//      YYSYS_LOG(WARN, "failed to get table partition type, fetch version=%ld, ret=%d", end_version + 1, ret);
//    }
//    if (OB_SUCCESS == ret)
//    {
//      if (false == is_table_level)
//      {
//        YYSYS_LOG(INFO, "not table level, the version is=%ld",i);
//        break;
//      }
//    }
//  }
//  return ret;
//}
//add 20150706:e


//add lijianqiang [MultiUPS] [SELECT_MERGE] 20160104:b
//int ObCsPartitionManager::get_paxos_id_without_partition(const uint64_t& table_id, VersionPaxosId& paxos_version, int64_t version/*OB_INVALID_VERSION*/)
//{
//  int ret = OB_SUCCESS;
//  int32_t paxos_id = OB_INVALID_PAXOS_ID;
//  int64_t real_version = OB_INVALID_VERSION;
//  ObCalcInfo calc_struc;
//  calc_struc.set_table_id(table_id);
//  /**
//   * IN CS, this interface used in two ways:
//   *   1.SELECT STMT, there is maybe major_freeze happened during select, so the version is necessary.
//   * the param "version" can't be with default value OB_INVALID_VERSION;
//   *   2.DAILY MERGE, there is maybe more than one major_freeze happened during the daily merge,
//   * the valid version for current merge is CS serving_data_version + 1, not frozen_data_version + 1.
//   *
//   */
//  if (OB_INVALID_VERSION != version)//for select
//  {
//    real_version = version;
//  }
//  else//version = OB_INVALID_VERSION for daily merge
//  {
//    real_version = get_serving_data_version() + 1;
//  }
//  if(OB_SUCCESS != (ret = ObPartitionManager::get_paxos_id(table_id, calc_struc, real_version, paxos_id)))
//  {
//    YYSYS_LOG(WARN, "get paxos id failed, ret=%d", ret);
//  }
//  else
//  {
//    paxos_version.paxos_id_ = paxos_id;
//    paxos_version.version_ = real_version;
//  }
//  return ret;
//}
////add 20160104:e
//add shili [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20150806:b
//int ObCsPartitionManager::get_paxos_version_array(const ObCalcInfo &calc_struc, ObArray<VersionPaxosId> &paxos_version_array)
int ObCsPartitionManager::get_paxos_version_array(const ObCalcInfo &calc_struc, ObArray<VersionPaxosId> &paxos_version_array, int64_t version)
{
  int ret = OB_SUCCESS;
  ret = get_paxos_id(calc_struc.get_table_id(), calc_struc, paxos_version_array, version); //获得paxos_id 数组
  //del lijianqiang [MultiUPS] [SELECT_MERGE] 20160316:b
//  if(OB_SUCCESS != ret && OB_GROUP_NOT_EXIST != ret)
//  {
//    YYSYS_LOG(WARN, "get paxos_id  fail,ret=%d",ret);
//  }
  //del 20160316:e
  if(0 == paxos_version_array.count() && OB_SUCCESS == ret)
  {
    YYSYS_LOG(WARN, "paxos_version_array empty");
    ret=OB_EMPTY_ARRAY;
  }
  //mod peiouya [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20150823:b
  //bug fix
  //else if(OB_GROUP_NOT_EXIST == ret ||OB_TABLE_NODE_NOT_EXIST)
  //mod lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160701:b
  //else if(OB_GROUP_NOT_EXIST == ret
  //        ||OB_TABLE_NODE_NOT_EXIST == ret)
  else if (IS_PARTITION_INTERNAL_ERR(ret))
  //mod 20160701:e
  //mod 20150823:e
  {
    //add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160701:b
    if (0 != paxos_version_array.count())
    {
      /**
       * note@lijianqiang
       * if paxos_version_array count is not eq zero,it may include some valid elements and
       * some invalid elemnets,so careful when U using the elements,remember to filter the
       * invalid elements(OB_INVALID_PAXOS_ID)
       */
//      for (int64_t i=0;i<paxos_version_array.count();i++)
//      {
//        YYSYS_LOG(ERROR, "the table id=%ld,paxos id=%d, version=%ld,ret=%d",calc_struc.get_table_id(),paxos_version_array.at(i).paxos_id_,paxos_version_array.at(i).version_,ret);
//      }
    }
    else
    {
    //add 20160701:e
      chunkserver::ObCsPartitionManager::VersionPaxosId version_paxos_id;
      version_paxos_id.paxos_id_  = 0;
      version_paxos_id.version_   = 0;
      paxos_version_array.push_back(version_paxos_id);
      YYSYS_LOG(DEBUG, "find none paxos id,ret=%d",ret);
    }
    ret = OB_SUCCESS;//internal error, return OB_SUCCESS
  }
  //add lijianqiang [MultiUPS] [SELECT_MERGE] 20160316:b
  else if(OB_SUCCESS != ret)
  {
    YYSYS_LOG(WARN, "get paxos_id failed,ret=%d",ret);
  }
  //add 20160316:e
  return ret;
}
//add 20150806::e


//int ObCsPartitionManager::get_paxos_id(const uint64_t &table_id, const ObCalcInfo &calc_struc, ObArray<VersionPaxosId> &res)
int ObCsPartitionManager::get_paxos_id(const uint64_t &table_id, const ObCalcInfo &calc_struc, ObArray<VersionPaxosId> &res, int64_t version)
{
  int ret = OB_ERROR;
  int err = OB_SUCCESS;
  //add lqc  [MultiUPS] [index_partition] 20170318:b
  const ObSchemaManagerV2 *schema=NULL;
  if(OB_SUCCESS == err)
  {
    if(NULL == (schema = service_->get_schema_manag()->get_schema(table_id)))
    {
      YYSYS_LOG(WARN, "get schema failed, ");
    }
  }
  //add e
  //mod lijianqiang [MultiUPS] [SELECT_MERGE] 20160303:b
  //int64_t start_version = get_serving_data_version()>2 ? get_serving_data_version() : 2;
  /**
   * the valid version range is [serving_data_version + 1, frozen_data_version + 1],
   * it's no use if the start_version == serving_data_version, just consume of time when do SELECT.
   * For CS, the min valid serving data version is 2.
   */
  int64_t start_version = get_serving_data_version()>0 ? get_serving_data_version() + 1 : 2;
  //mod 20160303:e
  int64_t end_version = get_frozen_version();

  //[495]
  if(version != OB_INVALID_VERSION && version < start_version)
  {
      start_version = version;
  }

  VersionPaxosId temp;
  for(int64_t i = start_version;i < end_version + 2;i++)
  {
    YYSYS_LOG(DEBUG, "current searching version is[%ld]",i);
    err = ObPartitionManager::get_paxos_id(table_id, calc_struc, i,schema, temp.paxos_id_);
    if(OB_SUCCESS == err)
    {
      temp.version_ = i;
      res.push_back(temp);
      ret = OB_SUCCESS;
    }
    else if(OB_GROUP_NOT_EXIST == err)
    {
      YYSYS_LOG(DEBUG, "cur version is=%ld,ret=%d",i, err);
      ret = err;
      break;
    }
    //add lijianqiang [MultiUPS] [SELECT_MERGE] 20160316:b
    else if(OB_ERR_NO_PARTITION == err)
    {
      ret = err;
      break;
    }
    //add 20160316:e
    else if(OB_TABLE_NODE_NOT_EXIST == err)
    {
      if(OB_SUCCESS == ret)
      {
        ret = OB_ERROR;
        break;
      }
      else
      {
        /**
         * @note lijianqiang
         * table node not exist, maybe no table rule in current version, the paxos id is
         * invalid, be careful ,need to do judgement before using when use the paxos_id_
         */
        temp.version_ = i;
        res.push_back(temp);
        continue;
      }
    }
    else
    {
      YYSYS_LOG(WARN, "failed to get paxos id, ret=%d", err);
      break;
    }
  }
  //add lqc  [MultiUPS] [index_partition] 20170318:b
  if(NULL != schema)
  {
    if(OB_SUCCESS != (service_->get_schema_manag()->release_schema(schema)))
    {
      YYSYS_LOG(ERROR, "failed to release schema ");
    }
    schema = NULL;
  }
  //add e
  return ret;
}

int ObCsPartitionManager::execute_sql(const ObString &query_sql, sql::ObSQLResultSet &rs)
{
  int ret = OB_SUCCESS;
  ObServer ms_server;
  //[582]
  //config_mgr_->get_ms(ms_server);
  ObScanParam param;
  service_->get_local_cs_ms_list().get_ms(param, 0, ms_server);

  if(0 == ms_server.get_port())
  {
    YYSYS_LOG(WARN, "No mergeserver right now.");
    ret = OB_MS_NOT_EXIST;
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
            (ret = config_mgr_->get_client_mgr()->send_request(ms_server,OB_SQL_EXECUTE,
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
          break;
        }
        else if (OB_SUCCESS != (ret = rs.get_errno()))
        {
          YYSYS_LOG(WARN, "fail to exeucte sql: [%s], errno: [%d]",
                    to_cstring(rs.get_sql_str()), ret);
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
            if (OB_SUCCESS != (ret = config_mgr_->get_client_mgr()->get_next(
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

int64_t ObCsPartitionManager::get_serving_data_version() const
{
  return service_->get_serving_data_version();
}

//int64_t ObCsPartitionManager::get_frozen_version() const
//{
//  int64_t frozen_version = service_->get_frozen_version();
//  return frozen_version!=0 ? frozen_version:1;
//}

int64_t ObCsPartitionManager::get_frozen_version(bool for_update_all_rules_use) const
{
  int64_t frozen_version = service_->get_frozen_version();
  if(for_update_all_rules_use)
  {
      return frozen_version != 0 ? frozen_version_sync_from_rs_before_promote_ + 1 : 1;
  }
  return frozen_version!=0 ? frozen_version:1;
}


//add liuzy [MultiUPS] [take_paxos_offline_interface] 20160301:b
int ObCsPartitionManager::renew_paxos_usable_view() const
{
  return OB_SUCCESS;
}
//add 20160301:e
