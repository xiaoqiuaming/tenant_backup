/*
*   Version: 0.1
*
*   Authors:
*      liu jun
*        - some work details if you want
*/

#ifndef OB_CS_PARTITION_MANAGER_H
#define OB_CS_PARTITION_MANAGER_H

#include "sql/ob_sql_result_set.h"
#include "common/ob_partition_manager.h"

using namespace oceanbase;

namespace oceanbase
{
  namespace common
  {
    class ObConfigManager;
  }
  namespace chunkserver
  {
    class ObChunkService;
    class ObCsPartitionManager : public ObPartitionManager
    {
    public:
      typedef struct VersionPaxosId
      {
        VersionPaxosId():version_(OB_INVALID_VERSION), paxos_id_(OB_INVALID_PAXOS_ID)
        {}
        int64_t version_;
        int32_t paxos_id_;
      }VersionPaxosId;
    public:
      ObCsPartitionManager(ObChunkService *service, ObConfigManager *config_mgr);

      ObCsPartitionManager(ObChunkService *service, ObConfigManager *config_mgr, int64_t frozen_version_sync_from_rs_before_promote);

      virtual ~ObCsPartitionManager();
      void destroy();
      //add lijianqiang [MultiUPS] [MERGE] 20160329:b
      int get_table_part_type_for_daily_merge(const uint64_t table_id, bool &is_table_level);

      //int get_table_part_type_for_select(const uint64_t table_id, bool &is_table_level);
      int get_table_part_type_for_select(const uint64_t table_id, bool &is_table_level, int64_t version = OB_INVALID_VERSION);

      int get_paxos_id_without_partition_for_daily_merge(const uint64_t& table_id, VersionPaxosId& paxos_version);

      //int get_paxos_id_without_partition_for_select(const uint64_t& table_id, ObArray<VersionPaxosId>& paxos_id_array);
      int get_paxos_id_without_partition_for_select(const uint64_t& table_id, ObArray<VersionPaxosId>& paxos_id_array, int64_t version = OB_INVALID_VERSION);

      //add 20160329:e
      //del lijianqiang [MultiUPS] [SELECT_MERGE] 20160314:b
      //int get_table_part_type(const uint64_t table_id, bool &is_table_level);
//      int get_table_part_type(const uint64_t table_id, bool &is_table_level, bool is_for_daily_merge = false);
      //del 20160314:e
      //int get_paxos_id(const uint64_t &table_id, const ObCalcInfo &calc_struc, ObArray<VersionPaxosId> &res);
      int get_paxos_id(const uint64_t &table_id, const ObCalcInfo &calc_struc, ObArray<VersionPaxosId> &res, int64_t version = OB_INVALID_VERSION);

      //add shili [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20150806:b
      //int get_paxos_version_array(const ObCalcInfo &calc_struc, ObArray<VersionPaxosId> &paxos_version_array);
      int get_paxos_version_array(const ObCalcInfo &calc_struc, ObArray<VersionPaxosId> &paxos_version_array, int64_t version = OB_INVALID_VERSION);
      //del lijianqiang [MultiUPS] [sql_api] 20160315:b
      //int get_paxos_id(const ObArray<VersionPaxosId> &version_paxosid_array,int64_t &paxos_id);
      //del 20160315:e
      //add 20150806::e
      //add lijianqiang [MultiUPS] [SELECT_MERGE] 20160104:b
      //int get_paxos_id_without_partition(const uint64_t& table_id, VersionPaxosId& paxos_version, int64_t version = OB_INVALID_VERSION);
      //add 20160104:e
      int execute_sql(const ObString &query_sql, sql::ObSQLResultSet &rs);

      //int64_t get_frozen_version() const;
      int64_t get_frozen_version(bool for_update_all_rules_use = false) const;
    private:
      int64_t get_serving_data_version() const;
      //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160301:b
      virtual int renew_paxos_usable_view() const;
      //add e

    private:
      static const int64_t DEFAULT_VERSION = 1;
      static const int64_t TIMEOUT = 1000000;

    private:
      ObChunkService *service_;
      ObConfigManager *config_mgr_;
      int64_t frozen_version_sync_from_rs_before_promote_;
    };
  }
}
#endif // OB_CS_PARTITION_MANAGER_H
