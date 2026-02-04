/*
*   Version: 0.1
*
*   Authors:
*      liu jun
*        - some work details if you want
*/

#include "ob_partition_manager.h"
//add hongchen [PERFORMANCE_OPTI] 20170821:b
#include "common/ob_cached_allocator.h"
//add hongchen [PERFORMANCE_OPTI] 20170821:e

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

namespace oceanbase
{
  namespace common
  {
    //add hongchen [PERFORMANCE_OPTI] 20170821:b
    static ObCachedAllocator<ObTableRuleNode> TABLE_RULE_NODE_ALLOC;
    static ObCachedAllocator<ObPartRuleNode>  PART_RULE_NODE_ALLOC;
    //add hongchen [PERFORMANCE_OPTI] 20170821:e
    ObPartitionManager::ObPartitionManager():server_type_(NotInit) /*alloted_(0)*///del  lqc [MultiUps 1.0] [#13] 20170405 b:e
    {
      int ret = OB_SUCCESS;
      if(OB_SUCCESS != (ret = all_part_rules_.create(hash::cal_next_prime(512))))
      {
        YYSYS_LOG(ERROR,"create all_part_rules_ failed, ret=%d", ret);
      }
      else if(OB_SUCCESS != (ret = all_table_rules_.create(hash::cal_next_prime(512))))
      {
        YYSYS_LOG(ERROR,"create all_table_rules_ failed, ret=%d",ret);
        all_part_rules_.destroy();
      }
      else if(OB_SUCCESS != (ret = all_all_groups_.create(hash::cal_next_prime(512))))
      {
        YYSYS_LOG(ERROR,"create all_all_groups_ failed, ret=%d", ret);
        all_part_rules_.destroy();
        all_table_rules_.destroy();
      }
      //del wuna [MultiUps][sql_api] 20151217:b
      //else if(OB_SUCCESS != (ret = allocator_.init(TOTAL_LIMIT, HOLD_LIMIT, PAGE_SIZE)))
      //{
      //  YYSYS_LOG(ERROR, "init allocator failed, ret=%d", ret);
      //  all_part_rules_.destroy();
      //  all_table_rules_.destroy();
      //  all_all_groups_.destroy();
      //}
      //add lijianqiang 20150827:b
      //else
      //{
      //  allocator_.set_mod_id(ObModIds::OB_PARTITION_MANAGER);
      //}
      //add 20150827:e
      //del 20151217:e
    }

    ObPartitionManager::~ObPartitionManager()
    {
      all_part_rules_.destroy();
      all_table_rules_.destroy();
      all_all_groups_.destroy();
    }

    int ObPartitionManager::get_table_part_type(
        const uint64_t &table_id,
        const int64_t version,
        const ObSchemaManagerV2* schema,//add lqc  [MultiUPS] [index_partition] 20170318:b
        bool &is_table_level)
    {
      int ret = OB_SUCCESS;
      //mod hongchen [PERFORMANCE_OPTI] 20170821:b
      //ObTableRuleNode *table_node = OB_NEW(ObTableRuleNode, ObModIds::OB_PARTITION_MANAGER);
      ObTableRuleNode *table_node = TABLE_RULE_NODE_ALLOC.alloc();
      //mod hongchen [PERFORMANCE_OPTI] 20170821:e
      //add lqc  [MultiUPS] [index_partition] 20170318:b
      uint64_t refrence_table_id =OB_INVALID_ID;
      if(NULL != schema)
      {
        refrence_table_id = schema->get_data_table_td(table_id);
      }
      //add e
      if(IS_SYS_TABLE_ID(table_id))
      {
        is_table_level = true;
      }
      //add lqc  [MultiUPS] [index_partition] 20170318:b
      else if(OB_INVALID_ID != refrence_table_id)
      {
        if(OB_SUCCESS !=(ret =get_table_part_type(refrence_table_id,version,schema,is_table_level)))
        {
          YYSYS_LOG(WARN, "get table type failed,ret=%d",ret);
        }
      }
      //add e
      else if(OB_SUCCESS != (ret = get_table_rule_node(table_id, version, table_node)))
      {
      }
      else if (OB_WITHOUT_PARTITION == table_node->get_partition_type())
      {
        is_table_level = true;
      }
      else if(OB_DIRECT_PARTITION == table_node->get_partition_type())
      {
        is_table_level = false;
      }
      else if(OB_REFERENCE_PARTITION == table_node->get_partition_type())
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(WARN, "reference partition not supported");
      }
      else
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(WARN, "wrong partition type");
      }
      //mod hongchen [PERFORMANCE_OPTI] 20170821:b
      TABLE_RULE_NODE_ALLOC.free(table_node);
      //OB_DELETE(ObTableRuleNode, ObModIds::OB_PARTITION_MANAGER, table_node);
      //mod hongchen [PERFORMANCE_OPTI] 20170821:e
      return ret;
    }

    int ObPartitionManager::get_paxos_id(
        const uint64_t& table_id,
        const ObCalcInfo &calc_struct,
        int64_t version,
        const ObSchemaManagerV2* schema,//add lqc  [MultiUPS] [index_partition] 20170318:b
        int32_t &paxos_id)
    {
      int ret = OB_SUCCESS;
      //mod hongchen [PERFORMANCE_OPTI] 20170821:b
      //ObTableRuleNode *table_node = OB_NEW(ObTableRuleNode, ObModIds::OB_PARTITION_MANAGER);
      //ObPartRuleNode *part_node = OB_NEW(ObPartRuleNode, ObModIds::OB_PARTITION_MANAGER);
      ObTableRuleNode *table_node = TABLE_RULE_NODE_ALLOC.alloc();
      ObPartRuleNode *part_node   = PART_RULE_NODE_ALLOC.alloc();
      //mod hongchen [PERFORMANCE_OPTI] 20170821:e
      paxos_id = OB_INVALID_PAXOS_ID;
      //add lqc  [MultiUPS] [index_partition] 20170318:b
      uint64_t refrence_table_id = OB_INVALID_ID;
      if(NULL != schema)
      {
        refrence_table_id = schema->get_data_table_td(table_id);
      }
      //add e
      if(IS_SYS_TABLE_ID(table_id))
      {
        paxos_id = 0;
      }//add lqc  [MultiUPS] [index_partition] 20170318:b
      else if(OB_INVALID_ID != refrence_table_id)
      {
        if(OB_SUCCESS !=( ret = get_paxos_id(refrence_table_id,calc_struct,version,schema,paxos_id)))
        {
          YYSYS_LOG(WARN, "get paxos id failed,ret=%d",ret);
        }
      }
      //add e
      else if(OB_SUCCESS != (ret = get_table_rule_node(table_id, version, table_node)))
      {
        YYSYS_LOG(WARN, "get none table rule node,ret=%d",ret);
      }
      else if (OB_WITHOUT_PARTITION == table_node->get_partition_type())
      {
        const ObString &group_name = table_node->get_prefix_name();
        ret = get_group_node_id(group_name, calc_struct, version, paxos_id);
      }
      else if(OB_DIRECT_PARTITION == table_node->get_partition_type())
      {
        //mod wuna[MultiUps][sql_api]20151217:b
        //const ObString &rule_name = table_node->get_rule_name();
        //const ObString &col_list = table_node->get_para_list();
        //if(OB_SUCCESS != (ret = generate_part_rule_node(rule_name, part_node)))
        ObObj result;
        ObArray<ObString> col_list;
        ObCellArray cell_array;
        if(OB_SUCCESS != (ret = generate_part_rule_node(table_node, part_node,calc_struct,col_list,cell_array)))
          //mod 20151217:e
        {
          YYSYS_LOG(WARN, "failed to get part rule node, ret=%d", ret);
        }
        //mod lqc [MultiUps 1.0][part column is null] 20170518 b
        //mod wuna[MultiUps][sql_api]20151217:b
        //else if(OB_SUCCESS != (ret = calculate(calc_struct, table_list, part_node, result)))
        //        else if(OB_SUCCESS != (ret = calculate(cell_array, col_list, part_node, result)))
        //        //mod 20151217:e
        //        {
        //          YYSYS_LOG(WARN, "calculate failed, ret=%d", ret);
        //        }

        else
        {
            //[610]
            if(cell_array.get_cell_size() < col_list.count())
            {
                YYSYS_LOG(INFO,"partition key is NULL");
                ret = OB_ERR_PARTITION_COLUMN_IS_NULL;
            }
          if(OB_SUCCESS == ret)
          {
            for(int32_t i= 0;i < col_list.count();i++ )
            {
              if(cell_array[i].value_.is_null())
              {
                ret = OB_ERR_PARTITION_COLUMN_IS_NULL;
                break;
              }
            }
          }
          if(OB_ERR_PARTITION_COLUMN_IS_NULL == ret)
          {
            //part column is null ,set the group name that is the first in the groups
            if(part_node->get_type() == ObCalcExpression::RANGE ||
               part_node->get_type() == ObCalcExpression::LIST)
            {
              ObString group_name;
              char tmp[OB_MAX_TABLE_NAME_LENGTH] = {0};
              group_name.assign_buffer(tmp, static_cast<int32_t>(OB_MAX_TABLE_NAME_LENGTH));
              if(OB_SUCCESS != get_rang_list_group_name(table_node,group_name))
              {
                YYSYS_LOG(WARN, "get group name  failed, ret=%d", ret);
              }
              else
              {
                result.set_varchar(group_name);
                ret = OB_SUCCESS;
              }
            }
            else//hash or enum
            {
              result.set_int(0);
              ret = OB_SUCCESS;
            }
          }
          else
          {
            if(OB_SUCCESS != (ret = calculate(cell_array, col_list, part_node, result)))
            {
              YYSYS_LOG(WARN, "calculate failed, ret=%d", ret);
            }
          } // mode e

          if(OB_SUCCESS == ret)
          {
            //add wuna[MultiUps][sql_api]20151217:b
            ObString group_name;
            if(part_node->get_type() == ObCalcExpression::RANGE ||
               part_node->get_type() == ObCalcExpression::LIST)
            {
              ret = result.get_varchar(group_name);
              if(OB_SUCCESS != ret)
              {
                YYSYS_LOG(WARN,"get group name failed.ret=%d",ret);
              }
            }
            else//hash,enum
            {
              //add 20151217:e
              char temp[OB_MAX_TABLE_NAME_LENGTH] = {0};
              const ObString &prefix_group = table_node->get_prefix_name();
              //ObString group_name;//del wuna[MultiUps][sql_api]20151217
              group_name.assign_buffer(temp, static_cast<int32_t>(OB_MAX_TABLE_NAME_LENGTH));
              if(OB_SUCCESS != (ret = generate_complete_name(prefix_group, result, group_name)))
              {
                YYSYS_LOG(WARN, "failed to generate complete name");
              }
              YYSYS_LOG(DEBUG, "result is::");
              result.dump();
            }
            if(OB_SUCCESS == ret)
            {
              YYSYS_LOG(DEBUG, "group name is[%.*s]",group_name.length(),group_name.ptr());
              if(OB_SUCCESS != (ret = get_group_node_id(group_name, calc_struct, version, paxos_id)))
              {
                if (!IS_PARTITION_INTERNAL_ERR(ret))//hanlde the ret code lijianqiang 20160704
                {
                  YYSYS_LOG(ERROR, "get group node id failed.ret=%d",ret);
                }
              }
            }
          }
        }
      }
      else if(OB_REFERENCE_PARTITION == table_node->get_partition_type())
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(WARN, "reference partition not supported");
      }
      else
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(WARN, "wrong partition type");
      }
      //mod hongchen [PERFORMANCE_OPTI] 20170821:b
      TABLE_RULE_NODE_ALLOC.free(table_node);
      PART_RULE_NODE_ALLOC.free(part_node);
      //yysys::CThreadGuard guard(&part_lock_);
      //OB_DELETE(ObTableRuleNode, ObModIds::OB_PARTITION_MANAGER, table_node);
      //OB_DELETE(ObPartRuleNode, ObModIds::OB_PARTITION_MANAGER, part_node);
      //mod hongchen [PERFORMANCE_OPTI] 20170821:e
      return ret;
    }

    int ObPartitionManager::get_paxos_info(
        const ObCalcInfo &calc_struct,
        int64_t version,
        ObPartitionInfo &part_info)
    {
      int ret = OB_SUCCESS;
      //mod hongchen [PERFORMANCE_OPTI] 20170821:b
      //ObTableRuleNode *table_node = OB_NEW(ObTableRuleNode, ObModIds::OB_PARTITION_MANAGER);
      ObTableRuleNode *table_node = TABLE_RULE_NODE_ALLOC.alloc();
      //mod hongchen [PERFORMANCE_OPTI] 20170821:e
      ObPartRuleNode *part_node = NULL;
      int32_t paxos_id = -1;
      uint64_t table_id = calc_struct.get_table_id();
      if(IS_SYS_TABLE_ID(table_id))
      {
        paxos_id = 0;
      }
      else if(OB_SUCCESS != (ret = get_table_rule_node(table_id, version, table_node)))
      {
        YYSYS_LOG(WARN, "get table rule node failed");
      }
      else if (OB_WITHOUT_PARTITION == table_node->get_partition_type())
      {
        const ObString &group_name = table_node->get_prefix_name();
        ret = get_group_node_id(group_name, calc_struct, version, paxos_id);
        part_info.paxos_id_ = paxos_id;
        part_info.group_name_ = group_name.ptr();
        part_info.length_ = group_name.length();
      }
      else if(OB_DIRECT_PARTITION == table_node->get_partition_type())
      {
        //        const ObString &rule_name = table_node->get_rule_name();//del wuna[MultiUps][sql_api]20151217
        ObObj result;
        //        const ObString &col_list = table_node->get_para_list();//del wuna[MultiUps][sql_api]20151217
        //mod wuna[MultiUps][sql_api]20151217:b
        //if(OB_SUCCESS != (ret = generate_part_rule_node(rule_name, part_node)))
        ObArray<ObString> col_list;
        ObCellArray cell_array;
        if(OB_SUCCESS != (ret = generate_part_rule_node(table_node, part_node,calc_struct,col_list,cell_array)))
          //mod 20151217:e
        {
          YYSYS_LOG(WARN, "failed to get part rule node, ret=%d", ret);
        }
        //mod wuna[MultiUps][sql_api]20151217:b
        //else if(OB_SUCCESS != (ret = calculate(calc_struct, table_list, part_node, result)))
        else if(OB_SUCCESS != (ret = calculate(cell_array, col_list, part_node, result)))
          //mod 20151216:e
        {
          YYSYS_LOG(WARN, "calculate failed, ret=%d", ret);
        }
        else
        {
          char temp[OB_MAX_TABLE_NAME_LENGTH] = {0};
          const ObString &prefix_group = table_node->get_prefix_name();
          ObString tmp_str;
          ObString group_name;

          tmp_str.assign_buffer(temp, static_cast<int32_t>(OB_MAX_TABLE_NAME_LENGTH));
          generate_complete_name(prefix_group, result, tmp_str);
          if(OB_SUCCESS != (ret = ob_write_string(allocator_, tmp_str, group_name)))
          {
            YYSYS_LOG(WARN, "failed to copy string %.*s, ret=%d", group_name.length(), group_name.ptr(), ret);
          }
          else if(OB_SUCCESS != (ret = get_group_node_id(tmp_str, calc_struct, version, paxos_id)))
          {
            if (!IS_PARTITION_INTERNAL_ERR(ret))
            {
              YYSYS_LOG(WARN, "get group node's paxos_id failed, ret=%d", ret);
            }
          }
          else
          {
            part_info.paxos_id_ = paxos_id;
            part_info.group_name_ = group_name.ptr();
            part_info.length_ = group_name.length();
          }
        }
      }
      else
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(WARN, "wrong partition type");
      }
      //mod hongchen [PERFORMANCE_OPTI] 20170821:b
      TABLE_RULE_NODE_ALLOC.free(table_node);
      //OB_DELETE(ObTableRuleNode, ObModIds::OB_PARTITION_MANAGER, table_node);
      //mod hongchen [PERFORMANCE_OPTI] 20170821:e
      return ret;
    }

    int ObPartitionManager::update_all_rules()
    {
      int ret = OB_SUCCESS;
      if(OB_SUCCESS != (ret = get_all_table_rules()))
      {
        YYSYS_LOG(WARN, "get the complete all_table_rules failed, ret=%d", ret);
      }
      else if(OB_SUCCESS != (ret = get_all_part_rules()))
      {
        YYSYS_LOG(WARN, "get the complete all_part_rules failed, ret=%d", ret);
      }
      else if(OB_SUCCESS != (ret = get_all_group_rules()))
      {
        YYSYS_LOG(WARN, "get the complete all_all_group failed, ret=%d", ret);
      }
      //add for [cs partition cache optimization debug]-b
      hash::ObHashMap<uint64_t, ObArray<ObTableRuleNode *>, hash::ReadWriteDefendMode>::const_iterator iter3 = all_table_rules_.begin();

      for (; iter3 != all_table_rules_.end(); iter3++)
      {
        uint64_t table_id = iter3->first;
        ObArray<ObTableRuleNode *> table_nodes = iter3->second;
        YYSYS_LOG(DEBUG, "update_all_rules:table_id=[%ld]", table_id);
        YYSYS_LOG(DEBUG, "table_nodes arrary start:---------------------");
        for (int i = 0; i < table_nodes.count(); i++)
        {
          ObTableRuleNode *table_node = table_nodes.at(i);
          //can add log for debug
          YYSYS_LOG(DEBUG, "update_all_rules:table_id=[%ld]", table_node->get_table_id());
          YYSYS_LOG(DEBUG, "update_all_rules:partition_type=[%d]", table_node->get_partition_type());
          YYSYS_LOG(DEBUG, "update_all_rules:prefix_name_=[%.*s]", table_node->get_prefix_name().length(), table_node->get_prefix_name().ptr());
          YYSYS_LOG(DEBUG, "update_all_rules:rule_name_=[%.*s]", table_node->get_rule_name().length(), table_node->get_rule_name().ptr());
          YYSYS_LOG(DEBUG, "update_all_rules:para_list_=[%.*s]", table_node->get_para_list().length(), table_node->get_para_list().ptr());
          YYSYS_LOG(DEBUG, "update_all_rules:version_range=[%ld, %ld)", table_node->get_start_version(), table_node->get_end_version());
        }
        YYSYS_LOG(DEBUG, "table_nodes arrary end.-------------------");
      }
      YYSYS_LOG(DEBUG, "update_all_rules:all_table_rules:end.#########");

      hash::ObHashMap<ObString, ObPartRuleNode *, hash::ReadWriteDefendMode>::const_iterator iter5 = all_part_rules_.begin();
      YYSYS_LOG(DEBUG, "update_all_rules:all_part_rules start:#########");
      for (; OB_SUCCESS == ret && iter5 != all_part_rules_.end(); iter5++)
      {
        ObPartRuleNode *rule_node = iter5->second;
        YYSYS_LOG(DEBUG, "update_all_rules:rule_name=[%.*s]", rule_node->get_rule_name().length(), rule_node->get_rule_name().ptr());
        YYSYS_LOG(DEBUG, "update_all_rules:rule_para_num=[%d]", rule_node->get_rule_para_num());
        YYSYS_LOG(DEBUG, "update_all_rules:rule_para_list=[%.*s]", rule_node->get_rule_para_list().length(), rule_node->get_rule_para_list().ptr());
        YYSYS_LOG(DEBUG, "update_all_rules:rule_body=[%.*s]", rule_node->get_rule_body().length(), rule_node->get_rule_body().ptr());
      }
      YYSYS_LOG(DEBUG, "update_all_rules:all_part_rules end:#########");

      hash::ObHashMap<ObString, ObArray<ObGroupNode *>, hash::ReadWriteDefendMode>::const_iterator iter4 = all_all_groups_.begin();
      YYSYS_LOG(DEBUG, "update_all_rules:all_all_group start:#########");
      for (; iter4 != all_all_groups_.end(); iter4++)
      {
        ObString group_name = iter4->first;
        ObArray<ObGroupNode *> group_nodes = iter4->second;
        YYSYS_LOG(DEBUG, "update_all_rules:group_name=[%.*s]", group_name.length(), group_name.ptr());
        for (int i = 0; i < group_nodes.count(); i++)
        {
          ObGroupNode *group_node = group_nodes.at(i);
          YYSYS_LOG(DEBUG, "update_all_rules:group_name=[%.*s]", group_node->get_group_name().length(), group_node->get_group_name().ptr());
          YYSYS_LOG(DEBUG, "update_all_rules:paxos_id_=[%d]", group_node->get_paxos_id());
        }
      }
      //add for [cs partition cache optimization debug]-e
      YYSYS_LOG(DEBUG, "update_all_rules:all_all_groups_:end.##########");
      return ret;
    }

    void ObPartitionManager::reuse()
    {
      all_part_rules_.clear();
      all_table_rules_.clear();
      all_all_groups_.clear();
    }

    int ObPartitionManager::get_server_type()
    {
      return static_cast<int>(server_type_);
    }
    //add by wuna [MultiUps] [sql_api] 20160704:b
    int ObPartitionManager::erase_invalid_part_rule_map(ObString& rule_name)
    {
      int ret = OB_SUCCESS;
      yysys::CThreadGuard guard(&part_lock_);
      if(-1 == all_part_rules_.erase(rule_name))
      {
        ret = OB_ERROR;
        YYSYS_LOG(WARN, "erase part rules failed");
      }
      return ret;
    }
    //add wuna 20160704:e

    //add lijianqiang [MultiUPS] [SELECT_MERGE] 20160503:b
    void ObPartitionManager::convert_version_range(const ObVersionRange& old_version_range, ObVersionRange& new_version_range,
                                                   int64_t servering_data_version, int64_t frozen_data_version)

    {
      /**
       * convert the version range from radom to include start and include end
       * e.g. [2-0-0, MAX) --> [2-0-0, frozen_data_version+1-0-0]
       *      (MIN, 3-0-0] --> [servering_data_version+1-0-0, 3-0-0]
       *      (MIN, MAX) --> [servering_data_version+1-0-0, frozen_data_version+1-0-0]
       *      (2-0-0, 4-0-0) --> [3-0-0, 3-0-0]
       */
      //assign
      new_version_range = old_version_range;
      //do change
      if (!old_version_range.border_flag_.inclusive_start() &&
          !old_version_range.border_flag_.is_min_value())
      {
        new_version_range.start_version_.version_ += 1;
      }
      if (!old_version_range.border_flag_.inclusive_end() &&
          !old_version_range.border_flag_.is_max_value())
      {
        new_version_range.end_version_.version_ -= 1;
      }
      if (old_version_range.border_flag_.is_min_value())
      {
        new_version_range.start_version_.version_ = servering_data_version + 1;
        new_version_range.border_flag_.unset_min_value();
      }
      if (old_version_range.border_flag_.is_max_value())
      {
        new_version_range.end_version_.version_  = frozen_data_version + 1;
        new_version_range.border_flag_.unset_max_value();
      }
      //start version can't less than 0
      if (new_version_range.start_version_.version_ < 0)
      {
        new_version_range.start_version_.version_ = 0;
      }
      //end version GE start version
      if (new_version_range.start_version_.version_ > new_version_range.end_version_.version_)
      {
        new_version_range.end_version_.version_ = new_version_range.start_version_.version_;
      }
      new_version_range.border_flag_.set_inclusive_start();
      new_version_range.border_flag_.set_inclusive_end();

      YYSYS_LOG(DEBUG, "the old version range=%s,the new version range=%s",to_cstring(old_version_range), to_cstring(new_version_range));
    }

    //add 20160503:e

    int ObPartitionManager::get_table_rule_node(
        const uint64_t &table_id,
        int64_t version,
        ObTableRuleNode * const table_node)
    {
      int ret = OB_SUCCESS;
      bool is_exist = find_table_rule_node(table_id, version, table_node);
      if(false == is_exist)
      {
        ObVersionRange version_range;
        ObArray<ObTableRuleNode*> nodes_array;
        version_range.start_version_ = version;
        version_range.end_version_ = version + 1;
        if(OB_SUCCESS != (ret = fetch_table_rule_nodes(table_id, version_range, nodes_array)))
        {
          YYSYS_LOG(WARN, "fetch table rule nodes failed");
        }
        /*mod by wuna[MultiUps] [sql_api] 20160112:b*/
        //else if(OB_SUCCESS != (ret = add_table_rule_nodes(nodes_array)))
        //{
        //  YYSYS_LOG(WARN, "add table rule nodes failed");
        //}
        //else if(false == (is_exist = find_table_rule_node(table_id, version, table_node)))
        else if(false == (is_exist = find_table_version_node(nodes_array, version, table_node)))
        {
          ret = OB_TABLE_NODE_NOT_EXIST;
          YYSYS_LOG(INFO,"find none table version node,ret=%d",ret);
        }
        else if(OB_SUCCESS != (ret = add_table_rule_nodes(nodes_array)))
        {
          YYSYS_LOG(WARN, "add table rule nodes failed");
        }
        /*mod 20160112:e*/
      }
      return ret;
    }

    int ObPartitionManager::get_part_rule_node(
        const ObString &rule_name,
        ObPartRuleNode *&part_node)
    {
      int ret = OB_SUCCESS;
      bool is_exist = false;
      if(OB_SUCCESS != (ret = find_part_rule_node(rule_name, is_exist, part_node)))
      {
        YYSYS_LOG(WARN, "find partition rule node failed");
      }
      else if(false == is_exist)
      {
        if(OB_SUCCESS != (ret = fetch_part_rule_node(rule_name, part_node)))
        {
          YYSYS_LOG(WARN, "fetch partition rule node failed, rule_name=%.*s", rule_name.length(), rule_name.ptr());
        }
        else if(OB_SUCCESS != (ret = add_part_rule_node(part_node)))
        {
          YYSYS_LOG(WARN, "add partition rule node failed");
        }
        else if(OB_SUCCESS != (ret = find_part_rule_node(rule_name, is_exist, part_node)))
        {
          YYSYS_LOG(WARN, "find partition rule node failed");
        }
        else if(false == is_exist || NULL == part_node)
        {
          //couldn't be here.
          ret = OB_ERROR;
          YYSYS_LOG(WARN, "can not find partition rule node after fetch, rule_name=%.*s", rule_name.length(), rule_name.ptr());
        }
      }
      return ret;
    }

    int ObPartitionManager::get_group_node_id(
        const ObString &group_name,
        const ObCalcInfo &calc_struct,
        int64_t version,
        int32_t &paxos_id)
    {
      int ret = OB_SUCCESS;
      paxos_id = OB_INVALID_PAXOS_ID;
      bool is_exist = find_group_node_id(group_name, version, paxos_id);
      YYSYS_LOG(DEBUG, "is_exist=%s,version is=%ld,paxos_id=%d, group name[%.*s],rowKeyis[%s]",
                is_exist==true ? "is_exist":"not exist",
                version,
                paxos_id,
                group_name.length(),group_name.ptr(),
                to_cstring(calc_struct.get_rowkey()));
      if(false == is_exist)
      {
        ret = try_add_group_nodes(group_name, calc_struct, is_exist, version, paxos_id);
        if(OB_SUCCESS == ret || OB_GROUP_NOT_EXIST == ret)
        {
          YYSYS_LOG(DEBUG, "ret=%d,version is=%ld,paxos_id=%d, calc rowkey is[%s]",ret, version,paxos_id, to_cstring(calc_struct.get_rowkey()));
        }
        else
        {
          YYSYS_LOG(WARN, "try add group nodes failed");
        }
      }
      return ret;
    }
    //mod wuna [MultiUps][sql_api] 20151217:b
    //  int ObPartitionManager::generate_part_rule_node(const ObString &rule_name, ObPartRuleNode *&part_node)
    int ObPartitionManager::generate_part_rule_node(
        ObTableRuleNode *&table_node,
        ObPartRuleNode *&part_node,
        const ObCalcInfo& calc_info,
        ObArray<ObString>& col_list,
        ObCellArray& cell_array,
        const bool is_partition_cal)
    //mod 20151217:e
    {
      int ret = OB_SUCCESS;
      ObArray<ObString> param_list;//add wuna [MultiUps][sql_api] 20151217
      const ObString &rule_name = table_node->get_rule_name();//add wuna [MultiUps][sql_api] 20151217
      const ObString &colmun_str = table_node->get_para_list();//add wuna [MultiUps][sql_api] 20151217
      ObPartRuleNode *tem_part_node = NULL;//add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160412:b
      if(OB_SUCCESS != (ret = get_part_rule_node(rule_name, tem_part_node)))
      {
        YYSYS_LOG(WARN, "get partition rule node failed, ret=%d", ret);
      }
      //add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160412:b
      else if (NULL == part_node)
      {
        ret = OB_ERROR;
        YYSYS_LOG(ERROR, "the part node is NULL,ret=%d",ret);
      }
      //add 20160412:e
      //add wuna [MultiUps][sql_api] 20160701:b
      else if(OB_SUCCESS != (ret = part_node->separate(tem_part_node->get_rule_para_list(), PARTITION_LIST_SEPARATOR, param_list)))
      {
        YYSYS_LOG(WARN, "split parameters failed");
      }
      else if(OB_SUCCESS != (ret = part_node->separate(colmun_str, PARTITION_LIST_SEPARATOR, col_list)))
      {
        YYSYS_LOG(WARN, "split table parameters failed, table_list=%.*s, ret=%d", colmun_str.length(), colmun_str.ptr(), ret);
      }
      else if(col_list.count() != param_list.count())
      {
        ObPartRuleNode *new_part_node = NULL;
        ObArray<ObString> new_param_list;
        if(OB_SUCCESS != (ret = fetch_part_rule_node(rule_name, new_part_node)))
        {
          YYSYS_LOG(WARN, "fetch partition rule node failed, rule_name=%.*s", rule_name.length(), rule_name.ptr());
        }
        else if(OB_SUCCESS != (ret = add_part_rule_node(new_part_node)))
        {
          YYSYS_LOG(WARN, "add partition rule node failed");
        }
        else if (NULL == new_part_node)
        {
          ret = OB_ERROR;
          YYSYS_LOG(ERROR, "the part node is NULL,ret=%d",ret);
        }
        else if(OB_SUCCESS != (ret = new_part_node->separate(new_part_node->get_rule_para_list(), PARTITION_LIST_SEPARATOR, new_param_list)))
        {
          YYSYS_LOG(WARN, "split parameters failed");
        }
        else if(col_list.count() != new_param_list.count())
        {
          ret = OB_ERROR;
          YYSYS_LOG(WARN, "column list num is not equal to partition param num,\
                    col_list_num=%ld,param_list_num=%ld", col_list.count(), param_list.count());
        }
        else if (OB_SUCCESS != (ret = part_node->assign(*new_part_node)))
        {
          YYSYS_LOG(WARN, "assign new part node failed,ret=%d",ret);
        }
      }
      //add 20160701:e
      else
      {
        //add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160412:b
        if (OB_SUCCESS != (ret = part_node->assign(*tem_part_node)))
        {
          YYSYS_LOG(WARN, "assign part node failed,ret=%d",ret);
        }
        //add 20160412:e
      }
      //mod wuna [MultiUps][sql_api] 20151217:b
      if(OB_SUCCESS == ret && !is_partition_cal)
      {
        if(OB_SUCCESS != (ret = fill_with_value(calc_info, col_list, cell_array)))
        { //mod lqc [MultiUps 1.0] 20170518 b
          if(OB_ERR_PARTITION_COLUMN_IS_NULL == ret)
          {
            ret = OB_SUCCESS;
            YYSYS_LOG(DEBUG, "partition column is null, ret=%d", ret);
          }
          else
          {
            YYSYS_LOG(WARN, "fill value failed, ret=%d", ret);
          } // mod e
        }
        //else if(OB_SUCCESS != (ret = part_node->parser(allocator_)))
        else if(OB_SUCCESS != (ret = part_node->parser(table_node,cell_array)))
          //mod 20151217:e
        {
          YYSYS_LOG(WARN, "resolve partition rule node failed, rule_name=%.*s, ret=%d", rule_name.length(), rule_name.ptr(), ret);
        }
      }
      return ret;
    }

    int ObPartitionManager::get_next_row(ObSQLResultSet &result_set, ObRow &row) const
    {
      int ret = OB_SUCCESS;
      ret = result_set.get_new_scanner().get_next_row(row);
      return ret;
    }

    void ObPartitionManager::set_server_type(ServerType server_type)
    {
      server_type_ = server_type;
    }

    bool ObPartitionManager::find_table_rule_node(
        const uint64_t &table_id,
        int64_t version,
        ObTableRuleNode * const table_node)
    {
      bool is_exist = false;
      ObArray<ObTableRuleNode *> nodes_array;

      if(hash::HASH_EXIST == all_table_rules_.get(table_id, nodes_array))
      {
        is_exist = find_table_version_node(nodes_array, version, table_node);
      }
      return is_exist;
    }

    int ObPartitionManager::find_part_rule_node(
        const ObString &rule_name,
        bool &is_exist,
        ObPartRuleNode *&part_node)
    {
      int ret = OB_SUCCESS;
      is_exist = false;
      if(0 == rule_name.length())
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(WARN, "invalid argument, rule_name=%.*s, part_node=%p", rule_name.length(), rule_name.ptr(), part_node);
      }
      else if(hash::HASH_EXIST == all_part_rules_.get(rule_name, part_node))
      {
        is_exist = true;
      }
      return ret;
    }

    bool ObPartitionManager::find_table_version_node(
        const ObArray<ObTableRuleNode*> &nodes_array,
        int64_t version,
        ObTableRuleNode * const table_node)
    {
      bool ret = false;
      int tmp_ret = OB_SUCCESS;
      int64_t num = nodes_array.count();
      (void)tmp_ret;//modify for [build warning]
      /*mod by wuna [MultiUps] [sql_api] 20160112:b*/
      //      if(num <= 0 || num > 3)
      if(num <= 0)
        /*mod 20160112:e*/
      {
        ret = false;
        //mod lijianqiang [MultiUPS] [sql_api] 20160303:b
        /**
         * get no row from __all_table_rules is normal,no WARN
         */
        //YYSYS_LOG(WARN, "table rule node array 's number is wrong, num=%ld", num);
        YYSYS_LOG(INFO, "table rule node array 's number is zero, num=%ld", num);
        //mod 20160303:e
      }
      else if(NULL == table_node)
      {
        tmp_ret = OB_ALLOCATE_MEMORY_FAILED;
        YYSYS_LOG(ERROR, "no memory");
      }
      else
      {
        ObVersionRange version_range;
        version_range.start_version_ = version;
        version_range.end_version_ = version + 1;
        yysys::CThreadGuard guard(&table_lock_);
        for(int64_t i = 0;i < num;i++)
        {
          YYSYS_LOG(DEBUG, "current node info::%s",to_cstring(*nodes_array.at(i)));//add lijianqiang
          if(nodes_array.at(i)->is_valid_range(version_range))
          {
            ret = true;
            *table_node = *(nodes_array.at(i));
            break;
          }
        }
      }
      return ret;
    }

    bool ObPartitionManager::find_group_version_node_id(
        const ObArray<ObGroupNode *> &nodes_array,
        int32_t &paxos_id,
        int64_t version,
        int64_t group_start_version)
    {
      bool ret = false;
      int64_t count = nodes_array.count();
      ObGroupNode* group_node = NULL;
      ObVersionRange version_range;
      version_range.start_version_ = version;
      version_range.end_version_ = version + 1;
      (void)group_start_version; //modify for [build warning]
      /*mod by wuna [MultiUps] [sql_api] 20160112:b*/
      //      int64_t num = nodes_array.count();
      //      if(num <= 0 || num > 3)
      if(count <= 0)
        /*mod 20160112:e*/
      {
        ret = false;
      }
      else
      {
        yysys::CThreadGuard guard(&group_lock_);
        for(int64_t i = 0;i < count;i++)
        {
          group_node = nodes_array.at(i);
          if(group_node->is_valid_range(version_range))
          {
            //mod liuzy [MultiUPS] [take_paxos_offline_interface] 20160306:b
            //            ret = true;
            //            paxos_id = group_node->get_paxos_id();
            paxos_id = group_node->get_paxos_id();
            group_start_version = group_node->get_start_version();
            if (is_paxos_id_usable(paxos_id))
            {
              ret = true;
            }
            //mod 20160306:e
            break;
          }
        }
      }
      return ret;
    }

    bool ObPartitionManager::find_group_node_id(
        const ObString &group_name,
        int64_t version,
        int32_t &paxos_id)
    {
      bool is_exist = false;
      paxos_id = OB_INVALID_PAXOS_ID;
      ObArray<ObGroupNode *> group_nodes;

      if(hash::HASH_EXIST == all_all_groups_.get(group_name, group_nodes))
      {
        is_exist = find_group_version_node_id(group_nodes, paxos_id, version);
        YYSYS_LOG(DEBUG, "is_exist=%s,version is=%ld,paxos_id=%d, group name[%.*s] nodes size=%ld",
                  is_exist==true ? "is_exist":"not exist",
                  version,
                  paxos_id,
                  group_name.length(),group_name.ptr(),
                  group_nodes.count());
      }
      return is_exist;
    }

    //
    int ObPartitionManager::fetch_table_rule_nodes(
        const uint64_t &table_id,
        const ObVersionRange &version_range,
        ObArray<ObTableRuleNode *> &nodes_array,
        const bool is_partition_cal)
    {
      int ret = OB_SUCCESS;
      char select_tables[1024];
      ObString select_table_nodes;
      ObSQLResultSet result;
      ObArray<ObTableRuleNode *> temp_nodes;
      int64_t pos = 0;
      ObRow row;

      databuff_printf(select_tables,
                      1024,
                      pos,
                      "select /*+read_consistency(STRONG) */ table_id, start_version, partition_type, prefix_name, rule_name, par_list from __all_table_rules where table_id = %lu and start_version < %ld order by start_version asc",
                      table_id,
                      version_range.end_version_.version_);
      if(pos >= 1024)
      {
        ret = OB_BUF_NOT_ENOUGH;
        YYSYS_LOG(WARN, "table rule nodes buffer not enough, ret=%d", ret);
      }
      else
      {
        select_table_nodes.assign_ptr(select_tables, static_cast<ObString::obstr_size_t>(pos));
        YYSYS_LOG(INFO, "select_tables=%.*s", select_table_nodes.length(), select_table_nodes.ptr());
      }
      if(OB_SUCCESS == ret)
      {
        if(OB_SUCCESS != (ret = execute_sql(select_table_nodes, result)))
        {
          YYSYS_LOG(WARN, "execute sql failed, ret=%d", ret);
        }
        else
        {
          ObTableRuleNode *table_node = NULL;
          while(true)
          {
            ret = get_next_row(result, row);
            if(OB_ITER_END == ret)
            {
              ret = OB_SUCCESS;
              break;
            }
            else if(OB_SUCCESS != ret)
            {
              YYSYS_LOG(WARN, "get next row from ObResultSet failed,ret=%d", ret);
              break;//bugfix:add by wuna 20151126
            }
            else
            {
              if(NULL == (table_node = OB_PART_NEW(ObTableRuleNode)))
              {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                YYSYS_LOG(ERROR, "fail to new ObTableRuleNode");
                break;
              }
              else
              {
                table_node->reset();
                if(OB_SUCCESS != (ret = table_node->row_to_entity(row, allocator_)))
                {
                  YYSYS_LOG(WARN, "transform table rule row to entity failed,ret=%d",ret);
                  OB_PART_DELETE(table_node);
                  break;
                }
              }
            }
            if(OB_SUCCESS == ret)
            {
              YYSYS_LOG(INFO, "get one table rule row,row=%s", to_cstring(row));
              temp_nodes.push_back(table_node);
            }
          }
        }
      }

      if (OB_SUCCESS != ret)
      {}
      else if(OB_SUCCESS != (ret = fill_table_with_version_range(temp_nodes)))
      {
        YYSYS_LOG(WARN, "fill the version range failed");
      }
      else
      {
        int64_t count = temp_nodes.count();
        for(int64_t i = 0;i < count;i++)
        {
          ObTableRuleNode *table_node = temp_nodes.at(i);
          if(table_node->is_valid_range(version_range) || is_partition_cal)
          {
            nodes_array.push_back(table_node);
          }
        }
      }
      return ret;
    }

    int ObPartitionManager::fetch_part_rule_node(
        const ObString &rule_name,
        ObPartRuleNode *&part_node)
    {
      int ret = OB_SUCCESS;
      char query[1024];
      ObString query_sql;
      ObSQLResultSet sql_rs;
      int64_t pos = 0;
      ObRow row;

      databuff_printf(query, 1024, pos, "select /*+read_consistency(strong)*/ rule_name, rule_par_num, rule_par_list, rule_body, type from __all_partition_rules where rule_name = '%.*s'",
                      rule_name.length(), rule_name.ptr());
      if(pos >= 1024)
      {
        ret = OB_BUF_NOT_ENOUGH;
        YYSYS_LOG(WARN, "partition node buffer not enough, ret=%d", ret);
      }
      else
      {
        query_sql.assign_ptr(query, static_cast<ObString::obstr_size_t>(pos));
        YYSYS_LOG(INFO, "partition query_sql=%.*s", query_sql.length(), query_sql.ptr());
      }
      if(OB_SUCCESS == ret)
      {
        if(OB_SUCCESS != (ret = execute_sql(query_sql, sql_rs)))
        {
          YYSYS_LOG(WARN, "failed to execute sql, ret=%d", ret);
        }
        else
        {

          while(true)
          {
            ret = get_next_row(sql_rs, row);
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
              if(NULL == (part_node = OB_PART_NEW(ObPartRuleNode)))
              {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                YYSYS_LOG(WARN, "fail to new ObPartRuleNode");
              }
              else
              {
                if(OB_SUCCESS != (ret = part_node->row_to_entity(row, allocator_)))
                {
                  YYSYS_LOG(WARN, "transform part row to entity failed,ret=%d",ret);
                  OB_PART_DELETE(part_node);
                }
              }
            }
          }
        }
      }
      return ret;
    }

    int ObPartitionManager::fetch_group_nodes(
        const ObString &group_name,
        const ObVersionRange &version_range,
        ObArray<ObGroupNode *> &nodes_array,
        const bool is_partition_cal)
    {
      int ret = OB_SUCCESS;
      char select_groups[1024];
      ObString select_group_str;
      ObSQLResultSet result;
      ObGroupNode *group_node = NULL;
      ObArray<ObGroupNode *> temp_nodes;
      int64_t pos = 0;
      ObRow row;

      databuff_printf(select_groups, 1024, pos, "select /*+read_consistency(strong) */ group_name, start_version, paxos_id from __all_all_group where group_name = '%.*s' and start_version < %d" ,
                      group_name.length(), group_name.ptr(), static_cast<int32_t>(version_range.end_version_.version_));
      if(pos >= 1024)
      {
        ret = OB_BUF_NOT_ENOUGH;
        YYSYS_LOG(WARN, "group nodes buffer not enough, ret=%d", ret);
      }
      else
      {
        select_group_str.assign_ptr(select_groups, static_cast<ObString::obstr_size_t>(pos));
        YYSYS_LOG(INFO, "select_groups=%.*s", select_group_str.length(), select_group_str.ptr());
      }
      if(OB_SUCCESS == ret)
      {
        if(OB_SUCCESS != (ret = execute_sql(select_group_str, result)))
        {
          YYSYS_LOG(WARN, "execute sql failed, ret=%d", ret);
        }
        else
        {
          while(true)
          {
            ret = get_next_row(result, row);
            if(OB_ITER_END == ret)
            {
              ret = OB_SUCCESS;
              break;
            }
            else if(OB_SUCCESS != ret)
            {
              YYSYS_LOG(WARN, "get next row from ObResultSet failed,ret=%d", ret);
            }
            else
            {
              if(NULL == (group_node = OB_PART_NEW(ObGroupNode)))
              {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                YYSYS_LOG(WARN, "fail to new ObGroupNode");
              }
              if(OB_SUCCESS == ret)
              {
                if(OB_SUCCESS != (ret = group_node->row_to_entity(row, allocator_)))
                {
                  OB_PART_DELETE(group_node);
                  YYSYS_LOG(WARN, "transform group row to entity failed,ret=%d", ret);
                  break;
                }
              }
            }
            if(OB_SUCCESS == ret)
            {
              temp_nodes.push_back(group_node);
            }
          }
        }
      }
      if(OB_SUCCESS == ret)
      {
        if(OB_SUCCESS != (ret = fill_group_with_version_range(temp_nodes)))
        {
          YYSYS_LOG(WARN, "fill the version range failed");
        }
        else
        {
          int64_t count = temp_nodes.count();
          for(int64_t i = 0;i < count;i++)
          {
            ObGroupNode *group_node = temp_nodes.at(i);
            if(group_node->is_valid_range(version_range) || is_partition_cal)
            {
              nodes_array.push_back(group_node);
            }
          }
        }
      }
      return ret;
    }

    int ObPartitionManager::get_all_table_rules()
    {
      int ret = OB_SUCCESS;
      int hash_ret = OB_SUCCESS;
      char sql_char[1024];
      ObString select_tables;
      int64_t pos = 0;
      ObSQLResultSet result_set;
      ObArray<ObTableRuleNode*> temp_nodes;
      ObRow row;
      /*mod by wuna [MultiUps] [sql_api] 20160112:b*/
      //      databuff_printf(sql_char, 1024, pos, "select /*+read_consistency(STRONG) */ table_id, start_version, partition_type, prefix_name, rule_name, par_list, ref_info from __all_table_rules ");
      databuff_printf(sql_char, 1024, pos, "select /*+read_consistency(STRONG) */ table_id, start_version, \
                      partition_type, prefix_name, rule_name, par_list, ref_info from __all_table_rules order by table_id, start_version asc ");
      /*mod 20160112:e*/
      if(pos >= 1024)
      {
        ret = OB_BUF_NOT_ENOUGH;
        YYSYS_LOG(WARN, "table rule nodes buffer not enough, ret=%d", ret);
      }
      else
      {
        select_tables.assign_ptr(sql_char, static_cast<ObString::obstr_size_t>(pos));
        YYSYS_LOG(INFO, "select_tables=%.*s", select_tables.length(), select_tables.ptr());
      }
      if(OB_SUCCESS == ret)
      {
        if(OB_SUCCESS != (ret = execute_sql(select_tables, result_set)))
        {
          YYSYS_LOG(WARN, "failed to execute sql, ret=%d", ret);
        }
        else
        {
          uint64_t pre_table_id = INT64_MAX;
          uint64_t table_id = INT64_MAX;

          while(true)
          {
            ret = get_next_row(result_set, row);
            if(OB_ITER_END == ret)
            {
              if(0 == temp_nodes.count())
              {
                ret = OB_SUCCESS;
              }
              else if(OB_SUCCESS != (ret = fill_table_with_version_range(temp_nodes, true, true)))
              {
                YYSYS_LOG(WARN, "failed to fill table version range, ret=%d", ret);
              }
              else if(hash::HASH_INSERT_SUCC != (hash_ret = all_table_rules_.set(table_id, temp_nodes)))
              {
                ret = OB_ERROR;
                YYSYS_LOG(WARN, "failed to set table node of all table rules");
              }
              else
              {
                ret = OB_SUCCESS;
              }
              break;
            }
            else if(OB_SUCCESS != ret)
            {
              YYSYS_LOG(WARN, "get next row from ObResultSet failed,ret=%d", ret);
              break;
            }
            else
            {
              ObTableRuleNode *table_node = OB_PART_NEW(ObTableRuleNode);
              if(NULL == table_node)
              {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                YYSYS_LOG(ERROR, "no memory");
                break;
              }
              else if(OB_SUCCESS != (ret = table_node->row_to_entity(row, allocator_)))
              {
                OB_PART_DELETE(table_node);
                YYSYS_LOG(WARN, "failed to transform row to table entity");
                break;
              }

              if(OB_SUCCESS == ret)
              {
                table_id = table_node->get_table_id();
                //if they have the same table_id,push back into array,otherwise add to all_table_rules.
                if(table_id != pre_table_id && pre_table_id != INT64_MAX)
                {
                  if(OB_SUCCESS != (ret = fill_table_with_version_range(temp_nodes, true)))
                  {
                    YYSYS_LOG(WARN, "failed to fill table version range, ret=%d", ret);
                  }
                  //there maybe a invalid node for the first row here! add lijianqiang
                  else if(hash::HASH_INSERT_SUCC != (hash_ret = all_table_rules_.set(pre_table_id, temp_nodes)) )
                  {
                    ret = OB_ERROR;
                    YYSYS_LOG(WARN, "failed to set table node of all table rules");
                  }
                  temp_nodes.clear();
                  temp_nodes.push_back(table_node);
                }
                else
                {
                  temp_nodes.push_back(table_node);
                }
              }
            }
            pre_table_id = table_id;
          }
        }
      }
      return ret;
    }

    int ObPartitionManager::get_all_part_rules()
    {
      int ret = OB_SUCCESS;
      char query[1024];
      ObString query_sql;
      int64_t pos = 0;
      ObSQLResultSet sql_rs;
      ObRow row;

      databuff_printf(query, 1024, pos, "select /*+read_consistency(STRONG) */ rule_name, rule_par_num, rule_par_list, rule_body, type from __all_partition_rules");
      if(pos >= 1024)
      {
        ret = OB_BUF_NOT_ENOUGH;
        YYSYS_LOG(WARN, "part rule nodes buffer not enough, ret=%d", ret);
      }
      else
      {
        query_sql.assign_ptr(query, static_cast<ObString::obstr_size_t>(pos));
        YYSYS_LOG(INFO, "query_sql=%.*s", query_sql.length(), query_sql.ptr());
      }
      if(OB_SUCCESS == ret)
      {
        if(OB_SUCCESS != (ret = execute_sql(query_sql, sql_rs)))
        {
          YYSYS_LOG(WARN, "failed to execute sql, ret=%d", ret);
        }
        else
        {
          while(true)
          {
            ret = get_next_row(sql_rs, row);
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
              ObPartRuleNode *part_node = OB_PART_NEW(ObPartRuleNode);
              if(NULL == part_node)
              {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                YYSYS_LOG(ERROR, "no memory");
              }
              else if(OB_SUCCESS != (ret = part_node->row_to_entity(row, allocator_)))
              {
                OB_PART_DELETE(part_node);
                YYSYS_LOG(WARN, "failed to transform row to part entity");
                break;
              }
              else
              {
                int hash_ret = OB_SUCCESS;
                const ObString &rule_name = part_node->get_rule_name();
                if(hash::HASH_INSERT_SUCC != (hash_ret = all_part_rules_.set(rule_name, part_node)))
                {
                  ret = OB_ERROR;
                  YYSYS_LOG(WARN, "failed to set part rules of all part rules");
                }
              }
            }
          }
        }
      }
      return ret;
    }

    int ObPartitionManager::get_all_group_rules()
    {
      int ret = OB_SUCCESS;
      int hash_ret = OB_SUCCESS;
      char sql_char[1024];
      ObString select_tables;
      int64_t pos = 0;
      ObSQLResultSet result_set;
      ObArray<ObGroupNode*> temp_nodes;
      ObRow row;

      databuff_printf(sql_char, 1024, pos, "select /*+read_consistency(STRONG) */ group_name, start_version, paxos_id from __all_all_group order by group_name, start_version asc");
      if(pos >= 1024)
      {
        ret = OB_BUF_NOT_ENOUGH;
        YYSYS_LOG(WARN, "group nodes buffer not enough, ret=%d", ret);
      }
      else
      {
        select_tables.assign_ptr(sql_char, static_cast<ObString::obstr_size_t>(pos));
        YYSYS_LOG(INFO, "select_tables=%.*s", select_tables.length(), select_tables.ptr());
      }
      if(OB_SUCCESS == ret)
      {
        if(OB_SUCCESS != (ret = execute_sql(select_tables, result_set)))
        {
          YYSYS_LOG(WARN, "failed to execute sql, ret=%d", ret);
        }
        else
        {
          ObString pre_group_name;
          ObString group_name;

          while(true)
          {
            ret = get_next_row(result_set, row);
            if(OB_ITER_END == ret)
            {
              if(0 == temp_nodes.count())
              {
                ret = OB_SUCCESS;
              }
              //add lijianqiang 20150825:b
              /*Exp:need to fill version range here*/
              else if(OB_SUCCESS != (ret = fill_group_with_version_range(temp_nodes, true)))
              {
                YYSYS_LOG(WARN, "failed to fill group version range");
              }
              //add 20150825:e
              else if(hash::HASH_INSERT_SUCC != (hash_ret = all_all_groups_.set(group_name, temp_nodes)))
              {
                ret = OB_ERROR;
                YYSYS_LOG(WARN, "failed to set group nodes");
              }
              else
              {
                ret = OB_SUCCESS;
              }
              break;
            }
            else if(OB_SUCCESS != ret)
            {
              YYSYS_LOG(WARN, "get next row from ObResultSet failed,ret=%d", ret);
              break;
            }
            else//normal
            {
              ObGroupNode *group_node = OB_PART_NEW(ObGroupNode);
              if(NULL == group_node)
              {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                YYSYS_LOG(ERROR, "no memory");
                break;
              }
              else if(OB_SUCCESS != (ret = group_node->row_to_entity(row, allocator_)))
              {
                OB_PART_DELETE(group_node);
                YYSYS_LOG(WARN, "failed to transform row to group entity, ret=%d", ret);
                break;
              }
              if(OB_SUCCESS == ret)
              {
                //modify for [495-after alter group update wrong]-b
                //                if(pre_group_name != group_name)
                group_name = group_node->get_group_name();
                if(pre_group_name.ptr() != NULL && pre_group_name != group_name)
                  //modify for [495-after alter group update wrong]-e
                {
                  if(OB_SUCCESS != (ret = fill_group_with_version_range(temp_nodes, true, true)))
                  {
                    YYSYS_LOG(WARN, "failed to fill group version range");
                  }
                  else if(hash::HASH_INSERT_SUCC != (hash_ret = all_all_groups_.set(pre_group_name, temp_nodes)))
                  {
                    ret = OB_ERROR;
                    YYSYS_LOG(WARN, "failed to set group node of all all groups, group_name=%.*s, ret=%d", group_name.length(), group_name.ptr(), ret);
                  }
                  temp_nodes.clear();
                  temp_nodes.push_back(group_node);
                }
                else
                {
                  temp_nodes.push_back(group_node);
                }
              }
            }
            pre_group_name = group_name;
          }
        }
      }
      return ret;
    }

    int ObPartitionManager::add_table_rule_nodes(const ObArray<ObTableRuleNode *> &nodes_array)
    {
      int ret = OB_SUCCESS;
      ObTableRuleNode *table_node = NULL;
      if(nodes_array.count() == 0)
      {
      }
      else
      {
        ObArray<ObTableRuleNode*> temp_nodes;
        table_node = nodes_array.at(0);
        int64_t index = OB_INVALID_INDEX; //add lijianqiang [MultiUPS] [SELECT_MERGE] 20160412
        uint64_t table_id = table_node->get_table_id();
        yysys::CThreadGuard guard(&table_lock_);
        for(int64_t i = 0;i < nodes_array.count();i++)
        {
          table_node = nodes_array.at(i);
          bool exist = false;
          if(hash::HASH_NOT_EXIST == all_table_rules_.get(table_id, temp_nodes))
          {
            int hash_ret = OB_SUCCESS;
            if(hash::HASH_INSERT_SUCC != (hash_ret = all_table_rules_.set(table_id, nodes_array)))
            {
              ret = OB_ERROR;
              YYSYS_LOG(WARN, "failed to set table node of all table rules, table_id=%ld, ret=%d", table_id, ret);
            }
            break;
          }
          else if(OB_SUCCESS != (ret = table_node->is_in(temp_nodes, exist, index)))
          {
            YYSYS_LOG(WARN, "check the node whether exist failed,ret=%d", ret);
            break;
          }
          else if(true == exist)
          {
            /// @note lijianqiang:careful! not deep copy,just change the versionRange
            (*temp_nodes.at(index)) = (*table_node);//add lijianqiang [MultiUPS] [SELECT_MERGE] 20160412:
            OB_PART_DELETE (table_node);
          }
          else if(false == exist)
          {
            if(temp_nodes.count() >= MAX_CACHED_VERSION_COUNT)
            {
              int64_t pos = find_table_replace_pos(temp_nodes);
              //            if(pos > 0)
              if(pos >= 0) //modify for [440]
              {
                (*temp_nodes.at(pos)) = (*table_node);
                //add lijianqiang 20150827:b
                /* Exp:affter assigning the table_node to the temp_nodes[pos],
                 * the memory of table_node point to is unused any more,
                 * we need to free the memory the table_node point
                 */
                OB_PART_DELETE (table_node);
                //table_node = NULL;
                //add 20150827:e
              }
              else
              {
                ret = OB_ERR_NO_POSITION;
                YYSYS_LOG(WARN, "can't find the position to replace a table rule node,ret=%d", ret);
                break;
              }
            }
            else
            {
              temp_nodes.push_back(table_node);
              //add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160122:b
              if (hash::HASH_OVERWRITE_SUCC != (ret = all_table_rules_.set(table_id, temp_nodes,1)))
              {
                ret = OB_ERR_UNEXPECTED;
                YYSYS_LOG(WARN, "over write all table rules failes, ret = %d",ret);
                break;
              }
              else
              {
                ret = OB_SUCCESS;
              }
              //add 20160122:e

            }
          }
        }
      }
      return ret;
    }

    int ObPartitionManager::add_part_rule_node(ObPartRuleNode *&part_node)
    {
      int ret = OB_SUCCESS;
      ObPartRuleNode *temp = NULL;
      if(NULL == part_node)
      {
        ret = OB_ERROR;
        YYSYS_LOG(WARN, "part_node is NULL");
      }
      else
      {
        yysys::CThreadGuard guard(&part_lock_);//the hash map is ReadWriteDefendMode,the lock is needed? add lijianqiang
        if(OB_SUCCESS == ret)
        {
          const ObString &rule_name = part_node->get_rule_name();
          if(hash::HASH_EXIST == all_part_rules_.get(rule_name, temp))
          {
            if(OB_SUCCESS != (part_node->equals(*temp)))
            {
              if(hash::HASH_OVERWRITE_SUCC != all_part_rules_.set(rule_name, part_node,1))
              {
                ret = OB_ERROR;
                YYSYS_LOG(WARN, "failed to overwrite part rules of same name, rule_name=%.*s, ret=%d", rule_name.length(), rule_name.ptr(), ret);
              }
              YYSYS_LOG(WARN, "part_node not equals which is exist");
            }
          }
          else
          {
            int hash_ret = OB_SUCCESS;
            if(hash::HASH_INSERT_SUCC != (hash_ret = all_part_rules_.set(rule_name, part_node)))
            {
              ret = OB_ERROR;
              YYSYS_LOG(WARN, "failed to set part rules of all part rules, rule_name=%.*s, ret=%d", rule_name.length(), rule_name.ptr(), ret);
            }
          }
        }
      }
      return ret;
    }

    int ObPartitionManager::add_group_nodes(const ObArray<ObGroupNode *> &nodes_array)
    {
      int ret = OB_SUCCESS;
      int64_t count = nodes_array.count();
      if(count == 0)
      {
        //do nothing
      }
      else
      {
        ObArray<ObGroupNode *> temp_nodes;
        int64_t index = -1;
        ObGroupNode *group_node = nodes_array.at(0);
        const ObString &group_name = group_node->get_group_name();
        bool exist = false;
        yysys::CThreadGuard guard(&group_lock_);
        for(int64_t i = 0;i < nodes_array.count();i++)
        {
          group_node = nodes_array.at(i);
          if(hash::HASH_NOT_EXIST == all_all_groups_.get(group_name, temp_nodes))
          {
            int hash_ret = OB_SUCCESS;
            if(hash::HASH_INSERT_SUCC != (hash_ret = all_all_groups_.set(group_name, nodes_array)))
            {
              ret = OB_ERROR;
              YYSYS_LOG(WARN, "failed to set group node of all all groups, group_name=%.*s, ret=%d", group_name.length(), group_name.ptr(), ret);
            }
            break;
          }
          else if(OB_SUCCESS != (ret = group_node->is_in(temp_nodes, exist, index)))
          {
            YYSYS_LOG(WARN, "check the node whether exist failed,ret=%d", ret);
          }
          else if(true == exist)
          {
            *temp_nodes.at(index) = *group_node;
            OB_PART_DELETE (group_node);
          }
          else if(false == exist)
          {
            if(MAX_CACHED_VERSION_COUNT <= temp_nodes.count())
            {
              int64_t pos = find_group_replace_pos(temp_nodes);
              if(pos >= 0)
              {
                *temp_nodes.at(pos) = *group_node;
                //add lijianqiang 20150827:b
                /* Exp:affter assigning the group_node to the temp_nodes[pos],
                 * the memory of group_node point to is unused any more,
                 * we need to free the memory the group_node point
                 */
                OB_PART_DELETE (group_node);
                //group_node = NULL;
                //add 20150827:e
              }
              else
              {
                ret = OB_ERR_NO_POSITION;
                YYSYS_LOG(WARN, "can't find the position to replace a group node,ret=%d", ret);
                break;
              }
            }
            else
            {
              temp_nodes.push_back(group_node);
              //add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160122:b
              if (hash::HASH_OVERWRITE_SUCC != (ret = all_all_groups_.set(group_name, temp_nodes,1)))
              {
                ret = OB_ERR_UNEXPECTED;
                YYSYS_LOG(WARN, "over write all all groups failes, ret = %d",ret);
                break;
              }
              else
              {
                ret = OB_SUCCESS;
              }
              //add 20160122:e

            }
          }
        }
      }
      return ret;
    }
    static int64_t determine_last_node_end_version(int64_t node_start_version, int64_t current_frozen_version)
    {
      return node_start_version > current_frozen_version ? (node_start_version + 1) :
                                                           (current_frozen_version + 2);
    }

    //  int ObPartitionManager::fill_table_with_version_range(ObArray<ObTableRuleNode *> &nodes, const bool add /*=false*/) const
    int ObPartitionManager::fill_table_with_version_range(ObArray<ObTableRuleNode *> &nodes, const bool for_update_all_rules_use, const bool add /*=false*/) const
    {
      int ret = OB_SUCCESS;
      UNUSED(add);
      int64_t count = nodes.count();
      ObTableRuleNode *table_node = NULL;
      ObTableRuleNode *next_node = NULL;
      if(0 == count)
      {
      }
      else
      {
        for(int64_t i = 0;i < count - 1;i++)
        {
          table_node = nodes.at(i);
          next_node = nodes.at(i+1);
          table_node->set_end_version(next_node->get_start_version());
        }
        table_node = nodes.at(count - 1);
        //        table_node->set_end_version(determine_last_node_end_version(table_node->get_start_version(), get_frozen_version()));
        table_node->set_end_version(determine_last_node_end_version(table_node->get_start_version(), get_frozen_version(for_update_all_rules_use)));
      }
      return ret;
    }

    //  int ObPartitionManager::fill_group_with_version_range(ObArray<ObGroupNode *> &nodes, const bool add /*=false*/) const
    int ObPartitionManager::fill_group_with_version_range(ObArray<ObGroupNode *> &nodes, const bool for_update_all_rules_use, const bool add /*=false*/) const
    {
      int ret = OB_SUCCESS;
      UNUSED(add);
      int64_t count = nodes.count();
      ObGroupNode *group_node = NULL;
      ObGroupNode *next_node = NULL;
      if(0 == count)
      {
        //do nothing.
      }
      else
      {
        for(int64_t i = 0;i < count - 1;i++)
        {
          group_node = nodes.at(i);
          next_node = nodes.at(i+1);
          group_node->set_end_version(next_node->get_start_version());
        }
        group_node = nodes.at(count-1);

        //      group_node->set_end_version(determine_last_node_end_version(group_node->get_start_version(), get_frozen_version()));
        group_node->set_end_version(determine_last_node_end_version(group_node->get_start_version(), get_frozen_version(for_update_all_rules_use)));
      }
      return ret;
    }

    int ObPartitionManager::try_add_group_nodes(
        const ObString &group_name,
        const ObCalcInfo &calc_struct,
        bool &is_exist,
        int64_t version,
        int32_t &paxos_id)
    {
      UNUSED(calc_struct);
      int ret = OB_SUCCESS;
      is_exist = false;
      paxos_id = OB_INVALID_PAXOS_ID;
      ObArray<ObGroupNode *> nodes_array;
      ObVersionRange version_range;
      version_range.start_version_ = version;
      version_range.end_version_ = version + 1;
      if(OB_SUCCESS != (ret = fetch_group_nodes(group_name, version_range, nodes_array)))
      {
        YYSYS_LOG(WARN, "fecth group nodes failed");
      }
      //mod wuna [MultiUps][sql_api] 20160126:b
      //else if(OB_SUCCESS != (ret = add_group_nodes(nodes_array)))
      //{
      //  YYSYS_LOG(WARN, "add group nodes failed,ret=%d", ret);
      //}
      //else if(true == (is_exist = find_group_version_node_id(nodes_array, paxos_id, version)))
      //{
      //do nothing.
      //}
      else if(true == (is_exist = find_group_version_node_id(nodes_array, paxos_id, version)))
      {
        if(OB_SUCCESS != (ret = add_group_nodes(nodes_array)))
        {
          YYSYS_LOG(WARN, "add group nodes failed,ret=%d", ret);
        }
      }
      //mod 20160126:e
      //mod lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160701:b
      //else if(server_type_ == MergeServer)
      /**
       * The new group node will be builded only during current active version,
       * if U want the last version group node and get none from the sys table,
       * it's no need to generate a new node,just return OB_GROUP_NOT_EXIST.
       */
      //del peiouya 20170508:b
      //      ms/cs not permitted to create new group
      //      else if((server_type_ == MergeServer) && (get_frozen_version() + 1 <= version))
      //      //mod 20160701:e
      //      {
      //        ObBasicStmt::StmtType stmt_type = calc_struct.get_stmt_type();
      //        if(ObBasicStmt::T_INSERT == stmt_type || ObBasicStmt::T_REPLACE == stmt_type
      //           || ObBasicStmt::T_DELETE == stmt_type || ObBasicStmt::T_UPDATE == stmt_type)
      //        {
      //          ObGroupNode *group_node = NULL;
      //          ObArray<ObGroupNode *> created_group;
      //          if(OB_SUCCESS != (ret = create_group_node(group_name, group_node)))
      //          {
      //            YYSYS_LOG(WARN,"create group node failed");
      //          }
      //          else if(OB_SUCCESS != (ret = created_group.push_back(group_node)))
      //          {
      //            YYSYS_LOG(WARN, "push back group node failed");
      //          }
      //          //mod wuna [MultiUps][sql_api] 20160126:b
      //          //else if(OB_SUCCESS != (ret = add_group_nodes(created_group)))
      //          //{
      //          //  YYSYS_LOG(WARN, "add group nodes failed");
      //          //}
      //          else if(false == find_group_version_node_id(created_group, paxos_id, version))
      //          {
      //            // couldn't be here
      //            ret = OB_ERROR;
      //            YYSYS_LOG(WARN, "cann't find version after insert a group");
      //          }
      //          else if(OB_SUCCESS != (ret = add_group_nodes(created_group)))
      //          {
      //            YYSYS_LOG(WARN, "add group nodes failed");
      //          }
      //          //mod 20160126:e
      //        }
      //        else
      //        {
      //          ret = OB_GROUP_NOT_EXIST;
      //        }
      //      }
      //del peiouya 20170508:e
      else
      {
        ret = OB_GROUP_NOT_EXIST;
      }
      return ret;
    }

    //del peiouya 20170508:b
    //ms/cs not permitted to create new group
    /*
    int ObPartitionManager::create_group_node(
        const ObString &group_name,
        ObGroupNode *&group_node)
    {
      int ret = OB_SUCCESS;
      int32_t paxos_id = -1;
      char insert_group[1024];
      ObString insert_group_str;
      ObString tmp_name;
      ObSQLResultSet result;
      int64_t pos = 0;
      group_node = NULL;
      int64_t frozen_version = get_frozen_version();

      if(OB_SUCCESS != (ret = allocate_paxos_id(paxos_id)))
      {
        YYSYS_LOG(WARN, "allocate paxos id failed");
      }
      else
      {
        databuff_printf(insert_group, 1024, pos, "insert into __all_all_group(group_name, start_version, paxos_id) VALUES ('%.*s', %ld, %d)", group_name.length(),
                        group_name.ptr(), frozen_version + 1, paxos_id);
        if(pos >= 1024)
        {
          ret = OB_BUF_NOT_ENOUGH;
          YYSYS_LOG(WARN, "create group buffer not enough, ret=%d", ret);
        }
        else
        {
          insert_group_str.assign_ptr(insert_group, static_cast<ObString::obstr_size_t>(pos));
          YYSYS_LOG(INFO, "insert_groups=%.*s", insert_group_str.length(), insert_group_str.ptr());
          ret = execute_sql(insert_group_str, result);
          if(OB_SUCCESS == ret || OB_ERR_PRIMARY_KEY_DUPLICATE == ret)
          {
            ret = OB_SUCCESS;
          }
          else if(OB_ERR_EXCLUSIVE_LOCK_CONFLICT == ret)
          {
            usleep(static_cast<useconds_t>(INTERVAL_TIME));
            ret = execute_sql(insert_group_str, result);
            if(OB_SUCCESS == ret || OB_ERR_PRIMARY_KEY_DUPLICATE == ret)
            {
              ret = OB_SUCCESS;
            }
            else
            {
              YYSYS_LOG(WARN, "failed to insert group, ret=%d", ret);
            }
          }
          else
          {
            YYSYS_LOG(WARN, "execute sql failed, sql=%.*s ret=%d", insert_group_str.length(), insert_group_str.ptr(), ret);
          }
        }
        if(OB_SUCCESS == ret)
        {
          if(NULL == (group_node = OB_PART_NEW(ObGroupNode)))
          {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            YYSYS_LOG(ERROR, "no memory");
          }
          else if(OB_SUCCESS != (ret = ob_write_string(allocator_, group_name, tmp_name)))
          {
            OB_PART_DELETE(group_node);
            YYSYS_LOG(WARN, "failed to copy string %.*s, ret=%d", group_name.length(), group_name.ptr(), ret);
          }
          else
          {
            group_node->set_group_name(tmp_name);
            group_node->set_start_version(frozen_version + 1);//set the active memory table version.
            group_node->set_end_version(frozen_version + 2);
            group_node->set_paxos_id(paxos_id);
          }
        }
      }
      return ret;
    }
    int ObPartitionManager::allocate_paxos_id(int32_t &paxos_id) const
    {
      int ret = OB_SUCCESS;
      srand(static_cast<int32_t>(yysys::CTimeUtil::getTime()));
      int64_t use_paxos_num = get_use_paxos_num();
      if(use_paxos_num > 0 && use_paxos_num <= MAX_UPS_COUNT_ONE_CLUSTER)
      {
        //mod lqc [MultiUps 1.0] [#13] 20170405 b
        //paxos_id = static_cast<int32_t>(random() % use_paxos_num);
        //paxos_id = static_cast<int32_t>( alloted_ % MAX_UPS_COUNT_ONE_CLUSTER);
        //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160228:b
//        int is_usable = false;
//        do
//        {
//          is_usable = is_paxos_id_usable(paxos_id);
//          if (!is_usable)
//          {
//            //paxos_id = static_cast<int32_t>((paxos_id + 1) % use_paxos_num);
//          }
//        }
//        while (!is_usable);

        int loop = MAX_UPS_COUNT_ONE_CLUSTER;
        do
        {
          alloted_ %= MAX_UPS_COUNT_ONE_CLUSTER;
          if (!is_paxos_id_usable(paxos_id))
          {
            alloted_ ++ ;
          }
          else
          {
            paxos_id = static_cast<int32_t> (alloted_);
            alloted_++;
            break;
          }
        } while (loop-- > 0);
        //add 20160228:e
        //mod e
      }
      else
      {
        ret = OB_ERROR;
        YYSYS_LOG(WARN, "paxos num in the bad range, use_paxos_num=%ld", use_paxos_num);
      }
      return ret;
    }
*/
    //del peiouya 20170508:e
    int64_t ObPartitionManager::find_table_replace_pos(const ObArray<ObTableRuleNode *> &nodes_array) const
    {
      int64_t ret = -1;
      int64_t min_version = INT64_MAX;
      int64_t count = nodes_array.count();
      const ObTableRuleNode *table_node = NULL;
      for(int64_t i = 0;i < count;i++)
      {
        table_node = nodes_array.at(i);
        if(table_node->get_start_version() <= min_version)
        {
          min_version = table_node->get_start_version();
          ret = i;
        }
      }
      return ret;
    }

    int64_t ObPartitionManager::find_group_replace_pos(const ObArray<ObGroupNode *> &nodes_array) const
    {
      int64_t ret = -1;
      int64_t min_version = INT64_MAX;
      int64_t count = nodes_array.count();
      const ObGroupNode *group_node = NULL;
      for(int64_t i = 0;i < count;i++)
      {
        group_node = nodes_array.at(i);
        if(group_node->get_end_version() <= min_version)
        {
          min_version = group_node->get_end_version();
          ret = i;
        }
      }
      return ret;
    }

    //add liuzy [MultiUPS] [add_cluster_interface] 20160317:b
    bool ObPartitionManager::is_paxos_id_usable(const int32_t &paxos_id) const
    {
      bool ret = false;
      int tmp_ret = OB_SUCCESS;
      uint64_t cur_version = get_frozen_version() + 1;
      if (paxos_usable_view_.get_version() != cur_version)
      {
        if (OB_SUCCESS != (tmp_ret = renew_paxos_usable_view()))
        {
          YYSYS_LOG(WARN, "renew paxos usable view failed, err=%d", tmp_ret);
        }
      }
      if (OB_SUCCESS == tmp_ret)
      {
        ret = paxos_usable_view_.is_paxos_usable(paxos_id);
      }
      return ret;
    }
    //add 20160317:e

    //mod wuna [MultiUps][sql_api] 20151217:b
    //    int ObPartitionManager::calculate(
    //        const ObCalcInfo &calc_info,
    //        const ObString &colmun_str,
    //        ObPartRuleNode *part_node,
    //        ObObj &result) const
    int ObPartitionManager::calculate(
        ObCellArray& cell_array,
        ObArray<ObString>& col_list,
        ObPartRuleNode *part_node,
        ObObj &result)
    //mod 20151217:e
    {
      int ret = OB_SUCCESS;
      ObCalcExpression *calc_expr = NULL;
      ObArray<ObString> param_list;
      //del wuna [MultiUps][sql_api] 20151217:b
      //      hash::ObHashMap<ObString,int64_t,hash::NoPthreadDefendMode> param_to_idx;
      //      ObCellArray cell_array;//copy rowkey's ObObj Array to CellArray
      //      ObArray<ObString> col_list;
      //del 20151217:e
      //del wuna [MultiUps][sql_api] 20160114:b
      //      if(OB_SUCCESS != (ret = param_to_idx.create(hash::cal_next_prime(512))))
      //      {
      //        YYSYS_LOG(WARN, "initialize hash map failed, ret=%d", ret);
      //      }
      //del 20160114:e
      if(OB_UNLIKELY(NULL == part_node))
      {
        ret = OB_ERROR;
        YYSYS_LOG(WARN, "partition node is NULL");
      }
      //del wuna [MultiUps][sql_api] 20151217:b
      //      else if(OB_SUCCESS != (ret = part_node->separate(colmun_str, PARTITION_LIST_SEPARATOR, col_list)))
      //      {
      //        YYSYS_LOG(WARN, "split table parameters failed, table_list=%.*s, ret=%d", colmun_str.length(), colmun_str.ptr(), ret);
      //      }
      //      else if(OB_SUCCESS != (ret = fill_with_value(calc_info, col_list, cell_array)))
      //      {
      //        YYSYS_LOG(WARN, "fill value failed, ret=%d", ret);
      //      }
      //      else if(OB_SUCCESS != (ret = build_param_to_idx_map(col_list, part_node, param_to_idx)))
      //      {
      //        YYSYS_LOG(WARN, "build parameter to rowkey order failed, ret=%d", ret);
      //      }
      //del 20151217:e
      else if(OB_SUCCESS != (ret = part_node->separate(part_node->get_rule_para_list(), PARTITION_LIST_SEPARATOR, param_list)))
      {
        YYSYS_LOG(WARN, "split parameters failed");
      }
      else if(col_list.count() != param_list.count())
      {
        ret = OB_ERROR;
        YYSYS_LOG(WARN, "column list num is not equal to partition param num,\
                  col_list_num=%ld,param_list_num=%ld", col_list.count(), param_list.count());
      }
      else if(NULL == (calc_expr = part_node->get_calc_expr()))
      {
        ret = OB_ERROR;
        YYSYS_LOG(WARN, "calc expression is NULL");
      }
      //mod wuna [MultiUps][sql_api] 20151217:b
      //     else if(OB_SUCCESS != (ret = calc_expr->calc(cell_array, param_to_idx, result)))
      else if(OB_SUCCESS != (ret = calc_expr->calc(cell_array, part_node->get_type(), param_list, result)))
        //mod 20151217:e
      {
        YYSYS_LOG(WARN, "calculate failed, ret=%d", ret);
      }
      //      param_to_idx.destroy();//del wuna [MultiUps][sql_api] 20160114
      return ret;
    }

    int ObPartitionManager::fill_with_value(
        const ObCalcInfo &cal_info,
        const ObArray<ObString> &list,
        ObCellArray &cell_array) const
    {
      int ret = OB_SUCCESS;
      const ObObj *obj_array = NULL;
      int64_t count = 0;
      int64_t order = -1;

      cal_info.get_rowkey_obj_array(obj_array, count);
      ObCellInfo cell_info;
      ObInnerCellInfo *inner_info = NULL;
      count = list.count();

      for(int64_t i = 0;i < count;i++)
      {
        const ObString &col_name = list.at(i);
        if(OB_SUCCESS != (ret = cal_info.get_rowkey_order(col_name, order)))
        {
          YYSYS_LOG(WARN, "get rowkey order failed, col_name=%.*s, ret=%d", col_name.length(), col_name.ptr(), ret);
          break;
        }
        else
        {
          cell_info.value_ = obj_array[order];
          //mod lqc [MultiUps 1.0] 20170518 b
          if(obj_array[order].is_null())
          {
            ret = OB_ERR_PARTITION_COLUMN_IS_NULL;
            break;
          }//mod e
          else if(OB_SUCCESS != (ret = cell_array.append(cell_info, inner_info)))
          {
            YYSYS_LOG(WARN, "failed to append cell info, ret=%d", ret);
          }
        }
      }
      return ret;
    }
    //del wuna [MultiUps][sql_api] 20160114:b
    //    int ObPartitionManager::build_param_to_idx_map(
    //        const ObArray<ObString> &list,
    //        const ObPartRuleNode *node,
    //        hash::ObHashMap<ObString, int64_t,hash::NoPthreadDefendMode> &param_to_idx) const
    //    {
    //      int ret = OB_SUCCESS;
    //      int64_t count = list.count();
    //      if(node == NULL)
    //      {
    //        ret = OB_ERROR;
    //        YYSYS_LOG(WARN, "Partition Node is NULL");
    //      }
    //      else if(count != static_cast<int64_t>(node->get_rule_para_num()))
    //      {
    //        ret = OB_INNER_STAT_ERROR;
    //        YYSYS_LOG(WARN, "table parameters' count not equal with number");
    //      }
    //      else
    //      {
    //        param_to_idx.clear();
    //        //mod wuna [MultiUps][sql_api] 20151217:b
    //        //const ObArray<ObString> &array = node->get_parameters();
    //        ObArray<ObString> array /*= node->get_parameters()*/;
    //        if(OB_SUCCESS != (ret = node->separate(node->get_rule_para_list(), PARTITION_LIST_SEPARATOR, array)))
    //        //mod 20151217:e
    //        {
    //          YYSYS_LOG(WARN, "split parameters failed");
    //        }
    //        int hash_ret = OB_SUCCESS;
    //        for(int64_t i=0;i < count && OB_SUCCESS == ret;i++)
    //        {
    //          const ObString &param = array.at(i);
    //          if(hash::HASH_INSERT_SUCC != (hash_ret = param_to_idx.set(param, i)))
    //          {
    //            ret = OB_ERROR;
    //            YYSYS_LOG(WARN, "failed to set parameters to index");
    //            break;
    //          }
    //        }
    //      }
    //      return ret;
    //    }
    //del 20160114:e
    int ObPartitionManager::generate_complete_name(
        const ObString &prefix_name,
        const ObObj &value,
        ObString &group_name)
    {
      int ret = OB_SUCCESS;
      int64_t pos = 0;
      char value_ptr[OB_MAX_COLUMN_NAME_LENGTH] = {0};
      pos = value.value_to_string(value_ptr, OB_MAX_COLUMN_NAME_LENGTH);
      if(prefix_name.length() > OB_MAX_PREFIX_GROUP_NAME_LENGTH)
      {
        ret = OB_SIZE_OVERFLOW;
        YYSYS_LOG(WARN, "prefix group name is too long, prefix_name=%.*s", prefix_name.length(), prefix_name.ptr());
      }
      else if(pos + static_cast<int64_t>(strlen(GROUP_SEPARATOR)) > OB_MAX_POST_GROUP_NAME_LENGTH)
      {
        ret = OB_SIZE_OVERFLOW;
        YYSYS_LOG(WARN, "postfix group name is too long, postfix_name=%s", value_ptr);
      }
      else
      {
        group_name.write(prefix_name.ptr(), prefix_name.length());
        group_name.write(GROUP_SEPARATOR, static_cast<int32_t>(strlen(GROUP_SEPARATOR)));
        group_name.write(value_ptr, static_cast<int32_t>(pos));
      }
      return ret;
    }
    //add lqc [MultiUps 1.0][part column is null] 20170525 b
    int ObPartitionManager::get_rang_list_group_name(ObTableRuleNode * const table_node,ObString &group_name)
    {
      int ret = OB_SUCCESS;
      if (NULL == table_node)
      {
        ret = OB_ERROR;
        YYSYS_LOG(WARN,"get table rule node failed");
      }
      else
      {
        char tmp_group[OB_MAX_VARCHAR_LENGTH] = {0};
        const ObString &tmp = table_node->get_prefix_name();
        snprintf(tmp_group,tmp.length()+1,"%.*s",tmp.length()+1,tmp.ptr());
        char *sub_str= strtok(tmp_group,",");
        if( NULL != sub_str)
        {
          group_name.write(sub_str,static_cast<int32_t>(strlen(sub_str)));
          YYSYS_LOG(DEBUG,"get the group_name is %.*s:",group_name.length(),group_name.ptr());
        }
      }
      return ret;
    }
    //add e

    //add for [465-partition calculate group function]-b
    int ObPartitionManager::get_table_node_with_version(
        uint64_t table_id,
        int64_t version_flag,
        int64_t cur_version,
        bool &has_next_version,
        bool &has_last_version,
        ObTableRuleNode &table_node,
        int64_t &table_rule_start_version,
        int64_t &table_rule_end_version,
        ObTableRuleNode &last_table_node)
    {
      int ret = OB_SUCCESS;
      ObVersionRange version_range;
      ObArray<ObTableRuleNode *> nodes_array;
      int64_t select_version = cur_version + 1;
      version_range.start_version_ = select_version;
      version_range.end_version_ = select_version + 1;

      if (version_flag == 2)
      {
        if (OB_SUCCESS != (ret = get_table_rule_node(table_id, cur_version, &table_node)))
        {
          YYSYS_LOG(WARN, "get none table rule node,ret=%d", ret);
        }
      }
      else
      {
        if (OB_SUCCESS != (ret = fetch_table_rule_nodes(table_id, version_range, nodes_array, true)))
        {
          YYSYS_LOG(WARN, "fetch table rule nodes failed");
        }
        else if (version_flag == 0 || version_flag == 1)
        {
          if (true == (find_table_version_node(nodes_array, select_version, &table_node)))
          {
            has_next_version = true;
            if(version_flag == 1)
            {
              table_rule_start_version = table_node.get_start_version();
              table_rule_end_version = INT64_MAX;
            }
          }
          else
          {
            has_next_version = false;
          }

          if(version_flag == 0 || (version_flag == 1 && has_next_version == false))
          {
            if (true == (find_table_version_node(nodes_array, cur_version, &table_node)))
            {
              table_rule_start_version = table_node.get_start_version();
              table_rule_end_version = has_next_version ? select_version : INT64_MAX;
            }
            else
            {
              ret = OB_ENTRY_NOT_EXIST;
              YYSYS_LOG(WARN, "cur_version of table rule node is not exists");
            }
          }
        }
        else
        {
          ObTableRuleNode *temp_node = NULL;
          int64_t i = nodes_array.count() - 1;
          while (ret == OB_SUCCESS && i >= 0)
          {
            if (NULL == (temp_node = nodes_array.at(i)))
            {
              ret = OB_ERROR;
              YYSYS_LOG(WARN, "table node is NULL");
            }
            else if (temp_node->get_start_version() == cur_version + 1)
            {
              i = i - 1;
            }
            else
            {
              table_node = *(nodes_array.at(i));
              table_rule_start_version = table_node.get_start_version();
              table_rule_end_version = table_node.get_end_version();
              i = i - 1;
              break;
            }
          }
          if (ret == OB_SUCCESS && i >= 0 && nodes_array.at(i)->get_end_version() == nodes_array.at(i+1)->get_start_version())
          {
            YYSYS_LOG(DEBUG, "set last_table_node");
            last_table_node = *(nodes_array.at(i));
            has_last_version = true;
          }
          else if (ret == OB_SUCCESS && i < 0)
          {
            has_last_version = false;
          }
          else
          {
            ret = OB_ERROR;
            YYSYS_LOG(WARN, "table node not exist");
          }
        }
      }
      return ret;
    }

    int ObPartitionManager::get_group_node_id_with_version(
        int64_t version_flag,
        bool &has_next_version,
        bool &has_last_version,
        const ObString group_name,
        int64_t cur_version,
        int32_t &paxos_id,
        int64_t &group_start_version,
        int64_t &group_end_version,
        ObTableRuleNode table_node,
        bool &is_need_last_table_rule)
    {
      int ret = OB_SUCCESS;
      int64_t select_version = OB_INVALID_VERSION;
      ObArray<ObGroupNode *> group_nodes;
      select_version = cur_version + 1;
      ObVersionRange version_range;
      version_range.start_version_ = select_version;
      version_range.end_version_ = select_version + 1;

      if (version_flag == 2)
      {
        ObCalcInfo calc_struct;
        if (OB_SUCCESS != (ret = get_group_node_id(group_name, calc_struct, cur_version, paxos_id)))
        {
          if (!IS_PARTITION_INTERNAL_ERR(ret))
          {
            YYSYS_LOG(ERROR, "get group node id failed.ret=%d", ret);
          }
        }
      }
      else
      {
        if (OB_SUCCESS != (ret = fetch_group_nodes(group_name, version_range, group_nodes, true)))
        {
          YYSYS_LOG(WARN, "fetch group nodes failed");
        }
        else
        {
          ObGroupNode *temp_node = NULL;
          int64_t i;
          for (i = group_nodes.count() - 1; ret == OB_SUCCESS && i >= 0; i--)
          {
            if (NULL == (temp_node = group_nodes.at(i)))
            {
              ret = OB_ERROR;
              YYSYS_LOG(WARN, "group node is NULL");
            }
            else if (temp_node->get_start_version() == cur_version + 1)
            {
              has_next_version = true;
              if (version_flag == 1)
              {
                paxos_id = temp_node->get_paxos_id();
                group_start_version = temp_node->get_start_version();
                group_end_version = INT64_MAX;
                break;
              }
              else
              {
                group_end_version = temp_node->get_start_version();
              }
            }
            else if (temp_node->get_start_version() <= cur_version && temp_node->get_end_version() > cur_version)
            {
              if (version_flag == 0 || \
                  (version_flag == 1 && has_next_version == true) || \
                  (version_flag == -1 && is_need_last_table_rule && temp_node->get_start_version() < table_node.get_end_version()))
              {
                paxos_id = temp_node->get_paxos_id();
                group_start_version = temp_node->get_start_version();
                if (group_end_version == -1)
                {
                  group_end_version = INT64_MAX;
                }
                break;
              }
              else if (version_flag == -1)
              {
                group_end_version = temp_node->get_start_version();
              }
            }
            else
            {
              if (version_flag == -1)
              {
                if (!is_need_last_table_rule && (temp_node->get_start_version() >= table_node.get_end_version() || temp_node->get_end_version() <= table_node.get_start_version()))
                {
                  is_need_last_table_rule = true;
                  break;
                }
                else if (is_need_last_table_rule && temp_node->get_start_version() > table_node.get_end_version())
                {
                  group_end_version = temp_node->get_start_version();
                  continue;
                }
                else
                {
                  has_last_version = true;
                  paxos_id = temp_node->get_paxos_id();
                  group_start_version = temp_node->get_start_version();
                  break;
                }
              }
            }
          }
          if (ret == OB_SUCCESS && i >= 0)
          {
            //you can add log for debug
          }
          else if (ret == OB_SUCCESS && i < 0)
          {
            if(version_flag == -1)
            {
              is_need_last_table_rule = true;
            }
          }
          else
          {
            YYSYS_LOG(WARN, "failed to get group node");
          }
        }
      }
      return ret;
    }

    int ObPartitionManager::get_group_and_paxos_id(
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
        bool &is_need_last_table_rule)
    {
      int ret = OB_SUCCESS;
      ObPartRuleNode part_node;
      ObCellArray cell_array;
      ObObj result;
      ObArray<ObString> col_list;
      if (OB_SUCCESS != (ret = get_part_rule_node_and_fill_value(table_node, part_node, partition_value, col_list, cell_array)))
      {
        YYSYS_LOG(WARN, "failed to get_part_rule_node_and_fill_value");
      }
      else
      {
          //[610]
          if(cell_array.get_cell_size() < partition_value.count())
          {
              ret = OB_ERR_PARTITION_COLUMN_IS_NULL;
          }

        for (int32_t i = 0; i < partition_value.count()&& OB_SUCCESS == ret; i++)
        {
          if (cell_array[i].value_.is_null())
          {
            ret = OB_ERR_PARTITION_COLUMN_IS_NULL;
            break;
          }
        }
        if (OB_ERR_PARTITION_COLUMN_IS_NULL == ret)
        {
          if (part_node.get_type() == ObCalcExpression::RANGE ||
              part_node.get_type() == ObCalcExpression::LIST)
          {
            if(OB_SUCCESS != get_rang_list_group_name(&table_node, group_name))
            {
              YYSYS_LOG(WARN, "get group name  failed, ret=%d", ret);
            }
            else
            {
              result.set_varchar(group_name);
              ret = OB_SUCCESS;
            }
          }
          else
          {
            result.set_int(0);
            ret = OB_SUCCESS;
          }
        }
        else
        {
          if (OB_SUCCESS != (ret = calculate(cell_array, col_list, &part_node, result)))
          {
            YYSYS_LOG(WARN, "calculate failed, ret=%d", ret);
          }
        }
        if (OB_SUCCESS == ret)
        {
          if (part_node.get_type() == ObCalcExpression::RANGE ||
              part_node.get_type() == ObCalcExpression::LIST)
          {
            ret = result.get_varchar(group_name);
            if (OB_SUCCESS != ret)
            {
              YYSYS_LOG(WARN, "get group name failed.ret=%d", ret);
            }
          }
          else
          {
            const ObString &prefix_group = table_node.get_prefix_name();
            if (OB_SUCCESS != (ret = generate_complete_name(prefix_group, result, group_name)))
            {
              YYSYS_LOG(WARN, "failed to generate complete name");
            }
            YYSYS_LOG(DEBUG, "result is::");
            result.dump();
          }
          if (OB_SUCCESS == ret)
          {
            if(version_flag == 2)
            {
              ObCalcInfo calc_struct;
              if (OB_SUCCESS != (ret = get_group_node_id(group_name, calc_struct, cur_version, paxos_id)))
              {
                if (!IS_PARTITION_INTERNAL_ERR(ret))
                {
                  YYSYS_LOG(ERROR, "get group node id failed.ret=%d", ret);
                }
              }
            }
            else if (OB_SUCCESS != (ret = get_group_node_id_with_version(version_flag, has_next_version, has_last_version, group_name, cur_version, \
                                                                         paxos_id, group_start_version, group_end_version, table_node, is_need_last_table_rule)))
            {
              YYSYS_LOG(WARN,"failed to get group name and paxos id");
            }
            else
            {
              YYSYS_LOG(DEBUG, "group_name:%s, paxos_id:%d, has_next_version:%d, has_last_version:%d", to_cstring(group_name), paxos_id, has_next_version, has_last_version);
            }
          }
        }
      }
      return ret;
    }

    int ObPartitionManager::get_part_rule_node_and_fill_value(
        ObTableRuleNode table_node,
        ObPartRuleNode &part_node,
        ObArray<ObObj> partition_value,
        ObArray<ObString> &col_list,
        ObCellArray &cell_array)
    {
      int ret = OB_SUCCESS;
      ObCalcInfo calc_struct;
      ObTableRuleNode *tmp_table_node = &table_node;
      ObPartRuleNode *tmp_part_node = &part_node;
      if (OB_SUCCESS != (ret = generate_part_rule_node(tmp_table_node, tmp_part_node, calc_struct, col_list, cell_array, true)))
      {
        YYSYS_LOG(WARN, "failed to get part rule node, ret=%d", ret);
      }
      else
      {
        int64_t partition_expr_count = partition_value.count();
        ObCellInfo cell_info;
        ObInnerCellInfo *inner_info = NULL;
        if (col_list.count() != partition_expr_count)
        {
          ret = OB_ERROR;
          YYSYS_LOG(WARN, "partition_key num is not equal to function param num,\
                    partition_key_num=%ld, param_list_num=%ld", partition_expr_count, col_list.count());
        }
        else
        {
          for (int64_t i = 0; ret == OB_SUCCESS && i < partition_expr_count; i++)
          {
            cell_info.value_ = partition_value.at(i);
            if (cell_info.value_.is_null())
            {
              ret = OB_ERR_PARTITION_COLUMN_IS_NULL;
              break;
            }
            else if (OB_SUCCESS != (ret = cell_array.append(cell_info, inner_info)))
            {
              YYSYS_LOG(WARN, "failed to append cell info, ret=%d", ret);
            }
          }
          if (ret != OB_SUCCESS)
          {
            if (OB_ERR_PARTITION_COLUMN_IS_NULL == ret)
            {
              ret = OB_SUCCESS;
              YYSYS_LOG(DEBUG, "partition column is null, ret=%d", ret);
            }
            else
            {
              YYSYS_LOG(WARN, "fill value failed, ret=%d", ret);
            }
          }
          else if (OB_SUCCESS != (ret = part_node.parser(tmp_table_node, cell_array)))
          {
            YYSYS_LOG(WARN, "resolve partition rule node failed, rule_name=%.*s, ret=%d", table_node.get_rule_name().length(), table_node.get_rule_name().ptr(), ret);
          }
        }
      }
      return ret;
    }
  }
}
