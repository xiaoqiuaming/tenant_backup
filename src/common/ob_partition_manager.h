/*
*   Version: 0.1
*
*   Authors:
*      liu jun
*        - some work details if you want
*/

#ifndef OB_PARTITION_MANAGER_H
#define OB_PARTITION_MANAGER_H

#include <yysys.h>
#include <pthread.h>
#include "ob_fifo_allocator.h"
#include "sql/ob_sql_context.h"
#include "hash/ob_hashmap.h"
#include "ob_part_rule_node.h"
#include "ob_table_rule_node.h"
#include "ob_group_node.h"
#include "ob_calc_info.h"
#include "sql/ob_sql_result_set.h"

#define OB_PART_NEW(T, ...)                     \
  ({                                            \
  T* ret = NULL;                                \
  ret = new(std::nothrow) T(__VA_ARGS__);              \
  ret;                                          \
  })

#define OB_PART_DELETE(ptr)                       \
  ({                                            \
  if(NULL != ptr)                     \
{                                   \
  delete(ptr);                           \
  ptr=NULL;                          \
  }                                \
  })

using namespace oceanbase::sql;

namespace oceanbase
{
  namespace common
  {
    typedef struct ObPartitionInfo
    {
        int32_t paxos_id_;
        const char *group_name_;
        int32_t length_;
    }ObPartitionInfo;

    //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160228:b
    /*Exp: this view consist of two parts, bitset that be maintained by rs and version of bitset*/
    struct PaxosUsableView
    {
      public:
        PaxosUsableView(): set_version_(OB_DEFAULT_OFFLINE_VERSION)
        {}
        inline bool is_paxos_usable(const int32_t paxos_id)
        {
          return !paxos_offline_set_.has_member(paxos_id);
        }
        inline uint64_t get_version()
        {
          return set_version_;
        }
        inline void set_view(const uint64_t new_version, const common::ObBitSet<MAX_UPS_COUNT_ONE_CLUSTER> &new_set)
        {
          set_version_ = new_version;
          paxos_offline_set_ = new_set;
        }
      private:
        uint64_t set_version_;
        common::ObBitSet<MAX_UPS_COUNT_ONE_CLUSTER> paxos_offline_set_;
    };
    //add 20160228:e

    //������ص�range��ΧĬ�Ͼ�Ϊ����ҿ��ġ�

    class ObPartitionManager
    {
      public:
        static const int64_t MAX_CACHED_VERSION_COUNT = 3;
        static const int64_t TOTAL_LIMIT = 1024 * 1024 * 1024;//1GB
        static const int64_t HOLD_LIMIT = TOTAL_LIMIT/2;
        static const int64_t PAGE_SIZE = 8 * 1024 * 1024;
        enum ServerType
        {
          NotInit,
          MergeServer,
          ChunkServer
        };

      public:
        ObPartitionManager();
        virtual ~ObPartitionManager();
        virtual int execute_sql(const ObString &str, ObSQLResultSet &rs) = 0;
        //version type is true means get active version,otherwise get frozen version.
        int get_table_part_type(
            const uint64_t& table_id,
            const int64_t version,
            const ObSchemaManagerV2* schema,//add lqc  [MultiUPS] [index_partition] 20170318:b
            bool &is_table_level);
        int get_paxos_id(
            const uint64_t& table_id,
            const ObCalcInfo &calc_struct,
            int64_t version,
            const ObSchemaManagerV2* schema,//add lqc  [MultiUPS] [index_partition] 20170318:b
            int32_t& paxos_id);

        //only ms use
        int get_paxos_info(//no use!
                           const ObCalcInfo &calc_struct,
                           int64_t version,
                           ObPartitionInfo &part_info);
        int update_all_rules();
        void reuse();
        int get_server_type();
        int erase_invalid_part_rule_map(ObString& rule_name);//add by wuna [MultiUps] [sql_api] 20160704
        //add lijianqiang [MultiUPS] [SELECT_MERGE] 20160503:b
        static void convert_version_range(const ObVersionRange& old_version_range, ObVersionRange& new_version_range,
                                          int64_t servering_data_version, int64_t frozen_data_version);
        //add 20160503:e

        //add for [465-partition calculate group function]-b
        int get_table_node_with_version(
            uint64_t table_id,
            int64_t version_flag,
            int64_t cur_version,
            bool &has_next_version,
            bool &has_last_version,
            ObTableRuleNode &table_node,
            int64_t &table_rule_start_version,
            int64_t &table_rule_end_version,
            ObTableRuleNode &last_table_node);
        int get_group_node_id_with_version(
            int64_t version_flag,
            bool &has_next_version,
            bool &has_last_version,
            const ObString group_name,
            int64_t version,
            int32_t &paxos_id,
            int64_t &group_start_version,
            int64_t &group_end_version,
            ObTableRuleNode table_node,
            bool &is_need_last_table_rule);
        int get_group_and_paxos_id(
            ObTableRuleNode table_node,
            int64_t version_flag,
            int64_t cur_version,
            bool &has_next_version,
            bool &has_last_version,
            ObArray<ObObj> partition_value,
            int64_t &group_start_version,
            int64_t &group_end_version,
            ObString &group_name,
            int32_t &paxos_id,
            bool &is_need_last_table_rule);
        int get_part_rule_node_and_fill_value(
            ObTableRuleNode table_node,
            ObPartRuleNode &part_node,
            ObArray<ObObj> partition_value,
            ObArray<ObString> &col_list,
            ObCellArray &cell_array);
        //add for [465-partition calculate group function]-e

        //According to the table_id, return the right table rules node.
        int get_table_rule_node(
            const uint64_t& table_id,
            int64_t version,
            ObTableRuleNode * const table_node);

      private:
        //According to the rule_name, return partition rule node.
        int get_part_rule_node(const ObString &rule_name,
                               ObPartRuleNode *&part_node);
        //According to the group_name, return group node.
        int get_group_node_id(
            const ObString &group_name,
            const ObCalcInfo &calc_struct,
            int64_t version,
            int32_t &paxos_id);
        //mod by wuna [MultiUps] [sql_api] 20151217:b
        //int generate_part_rule_node(const ObString &rule_name, ObPartRuleNode *&part_node);
        int generate_part_rule_node(ObTableRuleNode *&table_node,ObPartRuleNode *&part_node,
                                    const ObCalcInfo& calc_info,ObArray<ObString>& col_list,ObCellArray& cell_array, const bool is_partition_cal = false);
        //mod 20151217:e
        //find the node we required.
        bool find_table_rule_node(
            const uint64_t &table_id,
            int64_t version,
            ObTableRuleNode * const table_node);
        int find_part_rule_node(
            const ObString &rule_name,
            bool &is_exist,
            ObPartRuleNode *&part_node);
        bool find_group_node_id(
            const ObString &group_name,
            int64_t version,
            int32_t &paxos_id);
        bool find_table_version_node(
            const ObArray<ObTableRuleNode *> &nodes_array,
            int64_t version,
            ObTableRuleNode * const table_node);
        bool find_group_version_node_id(
            const ObArray<ObGroupNode *> &nodes_array,
            int32_t &paxos_id,
            int64_t version,
            int64_t group_start_version = OB_INVALID_VERSION);
        //get the required node from ups&cs with sql.
        //VersionRange:left is close,right is open.
        int fetch_table_rule_nodes(
            const uint64_t &table_id,
            const ObVersionRange &version_range,
            ObArray<ObTableRuleNode* > &nodes_array,
            const bool is_partition_cal = false);
        int fetch_part_rule_node(
            const ObString &rule_name,
            ObPartRuleNode *&part_node);
        int fetch_group_nodes(
            const ObString &group_name,
            const ObVersionRange &version_range,
            ObArray<ObGroupNode *> &nodes_array,
            const bool is_partition_cal =false);
        //fetch all records from one table
        int get_all_table_rules();
        int get_all_part_rules();
        int get_all_group_rules();
        //add the node,and return the right version node.
        int add_table_rule_nodes(const ObArray<ObTableRuleNode *> &nodes_array);
        int add_part_rule_node(ObPartRuleNode *&part_node);
        int add_group_nodes(const ObArray<ObGroupNode *> &nodes_array);
        //nodes save many ObTableRuleNode without version_range, this function fill the version range.
        //    int fill_table_with_version_range(ObArray<ObTableRuleNode *> &nodes, const bool add = false) const;
        int fill_table_with_version_range(ObArray<ObTableRuleNode *> &nodes, const bool for_update_all_rules_use = false, const bool add = false) const;
        //    int fill_group_with_version_range(ObArray<ObGroupNode *> &nodes, const bool add = false) const;
        int fill_group_with_version_range(ObArray<ObGroupNode *> &nodes, const bool for_update_all_rules_use = false, const bool add = false) const;
        int try_add_group_nodes(
            const ObString &group_name,
            const ObCalcInfo &calc_struct,
            bool &exist_flag,
            int64_t version,
            int32_t &paxos_id);
        //del peiouya 20170508:b
        //ms/cs not permitted to create new group
        //int create_group_node(
        //    const ObString &group_name,
        //    ObGroupNode *&group_node);
        //int allocate_paxos_id(int32_t &paxos_id) const;
        //del peiouya 20170508:E
        //
        int64_t find_table_replace_pos(const ObArray<ObTableRuleNode*> &nodes_array) const;
        int64_t find_group_replace_pos(const ObArray<ObGroupNode*> &nodes_array) const;
        int get_next_row(ObSQLResultSet &result_set, ObRow &row) const;
        //    virtual int64_t get_frozen_version() const = 0;
        virtual int64_t get_frozen_version(bool for_update_all_rules_use = false) const = 0;
        //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160228:b
        virtual int renew_paxos_usable_view() const = 0;
        bool is_paxos_id_usable(const int32_t &paxos_id) const;
        //add 20160228:e
        //mod wuna [MultiUps][sql_api] 20151217:b
        //      int calculate(
        //          const ObCalcInfo &calc_info,
        //          const ObString &table_list,
        //          ObPartRuleNode *part_node,
        //          ObObj &result) const;
        int calculate(
            ObCellArray& cell_array,
            ObArray<ObString>& col_list,
            ObPartRuleNode *part_node,
            ObObj &result);
        //mod 20151217:e
        int fill_with_value(
            const ObCalcInfo &calc_info,
            const ObArray<ObString> &list,
            ObCellArray &cell_array) const;
        //del wuna [MultiUps][sql_api] 20160114:b
        //      int build_param_to_idx_map(
        //          const ObArray<ObString> &list,
        //          const ObPartRuleNode *node,
        //          hash::ObHashMap<ObString,int64_t,hash::NoPthreadDefendMode> &param_to_idx) const;
        //del 20160114:e
        int generate_complete_name(
            const ObString &prefix_name,
            const ObObj &value,
            ObString &group_name);
        //add lqc [MultiUps 1.0][part column is null] 20170525 b
        int get_rang_list_group_name(ObTableRuleNode * const table_node,ObString &group_name);
        //add e
      protected:
        void set_server_type(ServerType server_type);

      private:
        DISALLOW_COPY_AND_ASSIGN(ObPartitionManager);
        //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160228:b
      protected:
        mutable PaxosUsableView paxos_usable_view_;
        //add 20160228:e
      private:
        ServerType server_type_;
        mutable yysys::CThreadMutex part_lock_;
        mutable yysys::CThreadMutex table_lock_;
        mutable yysys::CThreadMutex group_lock_;
        //mod wuna [MultiUps][sql_api] 20151217:b
        //common::FIFOAllocator allocator_;
        common::PageArena<char> allocator_;
        //mod 20151217:e
        hash::ObHashMap<ObString, ObPartRuleNode*, hash::ReadWriteDefendMode> all_part_rules_;
        hash::ObHashMap<uint64_t, ObArray<ObTableRuleNode*>, hash::ReadWriteDefendMode> all_table_rules_;
        hash::ObHashMap<ObString, ObArray<ObGroupNode*>, hash::ReadWriteDefendMode> all_all_groups_;
        static const int64_t INTERVAL_TIME = 10;
        //mutable int64_t    alloted_; //del  lqc [MultiUps 1.0] [#13] 20170405 b
    };
  }
}
#endif // OB_PARTITION_MANAGER_H
