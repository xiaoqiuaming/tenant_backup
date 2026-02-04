/*
*   Version: 0.1
*
*   Authors:
*      liu jun
*        - some work details if you want
*/

#ifndef OB_MS_PARTITION_MANAGER_H
#define OB_MS_PARTITION_MANAGER_H

#include "common/ob_partition_manager.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

namespace oceanbase
{
  namespace mergeserver
  {
    class ObMergeServerService;

    class ObMsPartitionManager : public ObPartitionManager
    {
      public:
        ObMsPartitionManager(ObMergeServerService *service, const ObClientManager *config_mgr);

        ObMsPartitionManager(ObMergeServerService *service, const ObClientManager *config_mgr, int64_t frozen_version_before_promote);

        virtual ~ObMsPartitionManager();
        void destroy();
        int get_table_part_type(const uint64_t table_id, bool &is_table_level);
        int get_paxos_id(uint64_t table_id, const ObCalcInfo &calc_stru, int32_t &paxos_id, bool * is_get_pre_paxos_id = NULL);
        //add shili [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20150806:b
        int get_paxos_id(const ObCalcInfo &calc_stru, int32_t &paxos_id);
        //add 20150806::e
        virtual int execute_sql(const ObString &query_sql, ObSQLResultSet &rs);
      private:
        //virtual int64_t get_frozen_version() const;
        virtual int64_t get_frozen_version(bool for_update_all_rules_use = false) const;
        static const int64_t DEFAULT_VERSION = 1;
        static const int64_t TIMEOUT = 1000000;
        //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160228:b
        virtual int renew_paxos_usable_view() const;
        //add 20160228:e

      private:
        ObMergeServerService *service_;
        const ObClientManager *client_mgr_;
        int64_t frozen_version_before_promote_;
    };
  }
}
#endif // OB_MS_PARTITION_MANAGER_H
