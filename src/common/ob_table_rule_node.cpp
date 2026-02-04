/*
*   Version: 0.1
*
*   Authors:
*      liu jun
*        - some work details if you want
*/

#include "ob_table_rule_node.h"

using namespace oceanbase::common;

ObTableRuleNode::ObTableRuleNode()
  :table_id_(OB_INVALID_ID),partition_type_(OB_TABLE_PARTITON_MAX_TYPE)
{
  version_range_.start_version_ = 0;
  version_range_.end_version_ = 0;
}

void ObTableRuleNode::reset()
{
  table_id_ = OB_INVALID_ID;
  prefix_name_.reset();
  rule_name_.reset();
  para_list_.reset();
  version_range_.start_version_ = 0;
  version_range_.end_version_ = 0;
}

void ObTableRuleNode::reuse()
{
  reset();
}
//mod wuna [MultiUps][sql_api] 20151217:b
//int ObTableRuleNode::row_to_entity(const ObRow &row, FIFOAllocator &allocator)
int ObTableRuleNode::row_to_entity(const ObRow &row, PageArena<char> &allocator)
//mod 20151217:e
{
  int ret = OB_SUCCESS;
  const ObObj *pcell = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  if(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = row.raw_get_cell(0, pcell, table_id, column_id)))
    {
      YYSYS_LOG(WARN, "raw_get_cell(table_id) from ObObj, ret=%d", ret);
    }
    else
    {
      if(pcell->get_type() == ObIntType)
      {
        int64_t id = 0;
        if(OB_SUCCESS != (ret = pcell->get_int(id)))
        {
          YYSYS_LOG(WARN, "failed to get_int(table_id) from ObObj, ret=%d", ret);
        }
        else
        {
          if(id <= 0)
          {
            ret = OB_ERR_UNEXPECTED;
            YYSYS_LOG(WARN, "table rule node 's table_id out of range, table_id=%ld", id);
          }
          else
          {
            table_id_ = static_cast<uint64_t>(id);
          }
        }
      }
      else
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(WARN, "got type of %d cell from row, expected type=%d", pcell->get_type(), ObIntType);
      }
    }
  }
  if(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = row.raw_get_cell(1, pcell, table_id, column_id)))
    {
      YYSYS_LOG(WARN, "raw_get_cell(start_version) from ObObj, ret=%d", ret);
    }
    else
    {
      if(pcell->get_type() == ObIntType)
      {
        int64_t start_version = 0;
        if(OB_SUCCESS != (ret = pcell->get_int(start_version)))
        {
          YYSYS_LOG(WARN, "failed to get_int(start_version) from ObObj, ret=%d", ret);
        }
        else
        {
          if(2 <= start_version)
          {
            version_range_.start_version_ = start_version;
          }
          else
          {
            ret = OB_ERR_UNEXPECTED;
            YYSYS_LOG(WARN, "table rule node 's start_version out of range, start_version=%ld", start_version);
          }
        }
      }
      else
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(WARN, "got type of %d cell from row, expected type=%d", pcell->get_type(), ObIntType);
      }
    }
  }
  if(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = row.raw_get_cell(2, pcell, table_id, column_id)))
    {
      YYSYS_LOG(WARN, "raw_get_cell(partition_type) from ObObj, ret=%d", ret);
    }
    else
    {
      if(pcell->get_type() == ObIntType)
      {
        int64_t part_type = 0;
        if(OB_SUCCESS != (ret = pcell->get_int(part_type)))
        {
          YYSYS_LOG(WARN, "failed to get_int(partition_type) from ObObj, ret=%d", ret);
        }
        else
        {
          if(part_type >= OB_WITHOUT_PARTITION && part_type < OB_TABLE_PARTITON_MAX_TYPE)
          {
            partition_type_ = static_cast<ObTablePartitionType>(part_type);
          }
          else
          {
            ret = OB_ERR_UNEXPECTED;
            YYSYS_LOG(WARN, "table rule node 's partition_type out of range");
          }
        }
      }
      else
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(WARN, "got type of %d cell from row, expected type=%d", pcell->get_type(), ObIntType);
      }
    }
  }
  if(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = row.raw_get_cell(3, pcell, table_id, column_id)))
    {
      YYSYS_LOG(WARN, "raw_get_cell(prefix_name) from ObObj, ret=%d", ret);
    }
    else
    {
      if(pcell->get_type() == ObVarcharType)
      {
        ObString prefix_name;
        if(OB_SUCCESS != (ret = pcell->get_varchar(prefix_name)))
        {
          YYSYS_LOG(WARN, "failed to get varchar from ObObj, ret=%d", ret);
        }
        else if(OB_SUCCESS != (ret = ob_write_string(allocator, prefix_name, prefix_name_)))
        {
          YYSYS_LOG(WARN, "failed to copy string %.*s, ret=%d", prefix_name.length(), prefix_name.ptr(), ret);
        }
      }
      else
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(WARN, "got type of %d cell from row, expected type=%d", pcell->get_type(), ObVarcharType);
      }
    }
  }
  if(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = row.raw_get_cell(4, pcell, table_id, column_id)))
    {
      YYSYS_LOG(WARN, "raw_get_cell(rule_name) from ObObj, ret=%d", ret);
    }
    else
    {
      if(pcell->get_type() == ObVarcharType)
      {
        ObString rule_name;
        if(OB_SUCCESS != (ret = pcell->get_varchar(rule_name)))
        {
          YYSYS_LOG(WARN, "failed to get varchar from ObObj, ret=%d", ret);
        }
        else if(OB_SUCCESS != (ret = ob_write_string(allocator, rule_name, rule_name_)))
        {
          YYSYS_LOG(WARN, "failed to copy string %.*s, ret=%d", rule_name.length(), rule_name.ptr(), ret);
        }
      }
      else if(pcell->get_type() == ObNullType)
      {
      }
      else
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(WARN, "got type of %d cell from row, expected type=%d", pcell->get_type(), ObVarcharType);
      }
    }
  }
  if(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = row.raw_get_cell(5, pcell, table_id, column_id)))
    {
      YYSYS_LOG(WARN, "raw_get_cell(par_list) from ObObj, ret=%d", ret);
    }
    else
    {
      if(pcell->get_type() == ObVarcharType)
      {
        ObString par_list;
        if(OB_SUCCESS != (ret = pcell->get_varchar(par_list)))
        {
          YYSYS_LOG(WARN, "failed to get varchar from ObObj, ret=%d", ret);
        }
        else if(OB_SUCCESS != (ret = ob_write_string(allocator, par_list, para_list_)))
        {
          YYSYS_LOG(WARN, "failed to copy string %.*s, ret=%d", par_list.length(), par_list.ptr(), ret);
        }
      }
      else if(pcell->get_type() == ObNullType)
      {
      }
      else
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(WARN, "got type of %d cell from row, expected type=%d", pcell->get_type(), ObVarcharType);
      }
    }
  }
  return ret;
}

int ObTableRuleNode::is_in(const ObArray<ObTableRuleNode *> &nodes_array, bool &exist, int64_t& index) const
{
  int ret = OB_SUCCESS;
  exist = false;
  index = -1;//add lijianqiang [MultiUPS] [SELECT_MERGE] 20160412
  int64_t count = nodes_array.count();
  const ObTableRuleNode *table_node = NULL;
  for(int64_t i = 0;i < count;i++)
  {
    table_node = nodes_array.at(i);
    if(table_id_ == table_node->get_table_id()
         && version_range_.start_version_ == table_node->get_start_version())
    {
      exist = true;
      index = i;//add lijianqiang [MultiUPS] [SELECT_MERGE] 20160412
      if(prefix_name_ == table_node->get_prefix_name()
        && rule_name_ == table_node->get_rule_name()
        && para_list_ == table_node->get_para_list()
        && partition_type_ == table_node->get_partition_type())
      {
      }
      else
      {
        ret = OB_INNER_STAT_ERROR;
      }
      break;
    }
  }
  return 0 == count ? OB_INNER_STAT_ERROR:ret;
}

bool ObTableRuleNode::is_valid_range(const ObVersionRange &version_range) const
{
  bool flag =false;
  int64_t entity_left = version_range_.start_version_.version_;
  int64_t entity_right = version_range_.end_version_.version_;
  int64_t range_left = version_range.start_version_.version_;
  int64_t range_right = version_range.end_version_.version_;
  if((entity_left <= range_left && range_left < entity_right)
     ||(range_left <= entity_left && entity_left < range_right))
  {
    flag = true;
  }
//  YYSYS_LOG(ERROR, "entity's start_version:%ld", entity_left);
//  YYSYS_LOG(ERROR, "entity's end_version:%ld", entity_right);
//  YYSYS_LOG(ERROR, "parameter start_version:%ld", range_left);
//  YYSYS_LOG(ERROR, "parameter end_version:%ld", range_right);
//  YYSYS_LOG(ERROR, "flag=%d", flag);
  YYSYS_LOG(DEBUG, "entity's start_version:%ld", entity_left);
  YYSYS_LOG(DEBUG, "entity's end_version:%ld", entity_right);
  YYSYS_LOG(DEBUG, "parameter start_version:%ld", range_left);
  YYSYS_LOG(DEBUG, "parameter end_version:%ld", range_right);
  YYSYS_LOG(DEBUG, "flag=%d", flag);
  return flag;
}

int64_t ObTableRuleNode::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ObTableRuleNode[");
  databuff_printf(buf, buf_len, pos,"table id=%ld ", table_id_);
  databuff_printf(buf, buf_len, pos,"partition_type_=%d ", partition_type_);
  databuff_printf(buf, buf_len, pos,"prefix_name_=%.*s ", prefix_name_.length(), prefix_name_.ptr());
  databuff_printf(buf, buf_len, pos,"rule_name_=%.*s ", rule_name_.length(), rule_name_.ptr());
  databuff_printf(buf, buf_len, pos,"para_list_=%.*s ", para_list_.length(), para_list_.ptr());
  pos += version_range_.to_string(buf+pos, buf_len-pos);
  databuff_printf(buf, buf_len, pos, "]");
  return pos;
}
