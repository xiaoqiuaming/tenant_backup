/*
*   Version: 0.1
*
*   Authors:
*      liu jun
*        - some work details if you want
*/

#include "ob_group_node.h"

using namespace oceanbase::common;

ObGroupNode::ObGroupNode()
{
  reset();
}

void ObGroupNode::reset()
{
  group_name_.reset();
  paxos_id_ = -1;
  version_range_.start_version_.version_ = 0;
  version_range_.end_version_.version_ = 0;
}

void ObGroupNode::reuse()
{
  reset();
}
//mod wuna [MultiUps][sql_api] 20151217:b
//int ObGroupNode::row_to_entity(const ObRow &row, FIFOAllocator &allocator)
int ObGroupNode::row_to_entity(const ObRow &row, PageArena<char> &allocator)
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
      YYSYS_LOG(WARN, "raw_get_cell(group_name) from ObObj, ret=%d", ret);
    }
    else
    {
      if(ObVarcharType == pcell->get_type())
      {
        ObString group_name;
        if(OB_SUCCESS != (ret = pcell->get_varchar(group_name)))
        {
          YYSYS_LOG(WARN, "failed to get_varchar(group_name) from ObObj, ret=%d", ret);
        }
        else if(OB_SUCCESS != (ret = ob_write_string(allocator, group_name, group_name_)))
        {
          YYSYS_LOG(WARN, "failed to copy string %.*s, ret=%d", group_name.length(), group_name.ptr(), ret);
        }
      }
      else
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObVarcharType);
      }
    }
  }
  if(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = row.raw_get_cell(1, pcell, table_id, column_id)))
    {
      YYSYS_LOG(WARN, "raw_get_cell(group_name) from ObObj, ret=%d", ret);
    }
    else
    {
      if(ObIntType == pcell->get_type())
      {
        int64_t start_version = 0;
        if(OB_SUCCESS != (ret = pcell->get_int(start_version)))
        {
          YYSYS_LOG(WARN, "failed to get_int(start_version) from ObObj, ret=%d", ret);
        }
        else
        {
          if(start_version > 0)
          {
            version_range_.start_version_ = start_version;
          }
          else
          {
            ret = OB_ERR_UNEXPECTED;
            YYSYS_LOG(WARN, "group node 's start_version out of range");
          }
        }
      }
      else
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
      }
    }
  }
  if(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = row.raw_get_cell(2, pcell, table_id, column_id)))
    {

    }
    else
    {
      if(ObIntType == pcell->get_type())
      {
        int64_t paxos_id = -1;
        if(OB_SUCCESS != (ret = pcell->get_int(paxos_id)))
        {
          ret = OB_ERR_UNEXPECTED;
          YYSYS_LOG(WARN, "failed to get_int(paxos_id) from ObObj, ret=%d", ret);
        }
        else
        {
          if(paxos_id >= 0)
          {
            paxos_id_ = static_cast<int32_t>(paxos_id);
          }
          else
          {
            ret = OB_ERR_UNEXPECTED;
            YYSYS_LOG(WARN, "group node 's paxos_id out of range");
          }
        }
      }
      else
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
      }
    }
  }
  return ret;
}

int ObGroupNode::is_in(const ObArray<ObGroupNode *> &nodes_array, bool &exist, int64_t& index) const
{
  //  YYSYS_LOG(ERROR, "TEST:ObGroupEntity::is_in");
  int ret = OB_SUCCESS;
  exist = false;
  index = -1;
  const int64_t count = nodes_array.count();
  const ObGroupNode *group_node = NULL;
  for(int64_t i = 0;i < count;i++)
  {
    group_node = nodes_array.at(i);
    if(NULL == group_node)
    {
    }
    else
    {
      if(group_name_ == group_node->get_group_name()
         && version_range_.start_version_ == group_node->get_start_version())
      {
        exist = true;
        index = i;
        if(paxos_id_ != group_node->get_paxos_id())
        {
          ret = OB_INNER_STAT_ERROR;
        }
      }
    }
  }
  return ret;
}

int64_t ObGroupNode::to_string(char* buf, const int64_t buf_len) const//add lijianqiang
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ObGroupNode[");
  databuff_printf(buf, buf_len, pos, "group_name_=%.*s ", group_name_.length(), group_name_.ptr());
  databuff_printf(buf, buf_len, pos, "paxos_id_=%d ", paxos_id_);
  databuff_printf(buf, buf_len, pos, "version_range=%s ",to_cstring(version_range_));
  //  pos += version_range_.to_string(buf+pos, buf_len-pos);
  databuff_printf(buf, buf_len, pos, "]");
  return pos;
}

