/*
*   Version: 0.1
*
*   Authors:
*      liu jun
*        - some work details if you want
*/

#ifndef OB_GROUP_NODE_H
#define OB_GROUP_NODE_H

#include "ob_string.h"
#include "ob_range.h"
#include "ob_row.h"
#include "ob_fifo_allocator.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace common
  {
    class ObGroupNode
    {
      public:
        ObGroupNode();
        void reset();
        void reuse();
        int64_t get_start_version() const;
        int64_t get_end_version() const;
        int32_t get_paxos_id() const;
        void set_group_name(ObString &group_name);
        void set_start_version(int64_t start_version);
        void set_end_version(int64_t end_version);
        void set_paxos_id(int32_t paxos_id);
        const ObString& get_group_name() const;
        //mod wuna [MultiUps][sql_api] 20151217:b
        //int row_to_entity(const ObRow &row, FIFOAllocator &allocator);
        int row_to_entity(const ObRow &row, PageArena<char> &allocator);
        //mod 20151217:e
        int is_in(const ObArray<ObGroupNode *> &nodes_array, bool &exist , int64_t& index) const;
        bool is_in_range(int64_t version) const;
        bool is_valid_range(const ObVersionRange &version_range) const;
        int64_t to_string(char* buf, const int64_t buf_len) const;//add lijianqiang
      private:
        ObString group_name_;
        int32_t paxos_id_;
        ObVersionRange version_range_;
    };

    inline int64_t ObGroupNode::get_start_version() const
    {
      return version_range_.start_version_;
    }

    inline int64_t ObGroupNode::get_end_version() const
    {
      return version_range_.end_version_;
    }

    inline int32_t ObGroupNode::get_paxos_id() const
    {
      return paxos_id_;
    }

    inline void ObGroupNode::set_group_name(ObString &group_name)
    {
      group_name_ = group_name;
    }

    inline void ObGroupNode::set_start_version(int64_t start_version)
    {
      version_range_.start_version_.version_ = start_version;
    }

    inline void ObGroupNode::set_end_version(int64_t end_version)
    {
      version_range_.end_version_.version_ = end_version;
    }

    inline void ObGroupNode::set_paxos_id(int32_t paxos_id)
    {
      paxos_id_ = paxos_id;
    }

    inline const ObString &ObGroupNode::get_group_name() const
    {
      return group_name_;
    }

    inline bool ObGroupNode::is_in_range(int64_t version) const
    {
      return (version >= version_range_.start_version_ && version < version_range_.end_version_) ? true : false;
    }

    inline bool ObGroupNode::is_valid_range(const ObVersionRange &version_range) const
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
      YYSYS_LOG(DEBUG, "entity's start_version:%ld", entity_left);
      YYSYS_LOG(DEBUG, "entity's end_version:%ld", entity_right);
      YYSYS_LOG(DEBUG, "parameter start_version:%ld", range_left);
      YYSYS_LOG(DEBUG, "parameter end_version:%ld", range_right);
      YYSYS_LOG(DEBUG, "flag=%d", flag);
      return flag;
    }
  }
}
#endif // OB_GROUP_NODE_H
