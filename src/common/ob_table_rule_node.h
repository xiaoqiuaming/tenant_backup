/*
*   Version: 0.1
*
*   Authors:
*      liu jun
*        - some work details if you want
*/

#ifndef OB_TABLE_RULE_NODE_H
#define OB_TABLE_RULE_NODE_H

#include "ob_string.h"
#include "ob_range.h"
#include "ob_row.h"
#include "ob_fifo_allocator.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace common
  {
    class ObTableRuleNode
    {
      public:
        ObTableRuleNode();
        void reset();
        void reuse();
        //mod wuna [MultiUps][sql_api] 20151217:b
        //int row_to_entity(const ObRow &row, FIFOAllocator &allocater);
        int row_to_entity(const ObRow &row, PageArena<char> &allocater);
        //mod 20151217
        ObTablePartitionType get_partition_type() const;
        int64_t get_start_version() const;
        int64_t get_end_version() const;
        uint64_t get_table_id() const;
        const ObString &get_prefix_name() const;
        const ObString &get_rule_name() const;
        const ObString &get_para_list() const;
        void set_end_version(int64_t end_version);
        //check is it exist in the nodes_array
        int is_in(const ObArray<ObTableRuleNode *> &nodes_array, bool &exist, int64_t& index) const;
        bool is_valid_range(const ObVersionRange &version_range) const;
        ObVersionRange& get_version_range();
        int64_t to_string(char* buf, const int64_t buf_len) const;//add lijianqiang
      private:
        uint64_t table_id_;
        ObTablePartitionType partition_type_;
        ObString prefix_name_;
        ObString rule_name_;
        ObString para_list_;
        ObVersionRange version_range_;
    };

    inline ObTablePartitionType ObTableRuleNode::get_partition_type() const
    {
      return partition_type_;
    }

    inline int64_t ObTableRuleNode::get_start_version() const
    {
      return version_range_.start_version_;
    }

    inline int64_t ObTableRuleNode::get_end_version() const
    {
      return version_range_.end_version_;
    }

    inline uint64_t ObTableRuleNode::get_table_id() const
    {
      return table_id_;
    }

    inline const ObString& ObTableRuleNode::get_prefix_name() const
    {
      return prefix_name_;
    }

    inline const ObString& ObTableRuleNode::get_rule_name() const
    {
      return rule_name_;
    }

    inline const ObString& ObTableRuleNode::get_para_list() const
    {
      return para_list_;
    }

    inline void ObTableRuleNode::set_end_version(int64_t end_version)
    {
      version_range_.end_version_ = end_version;
    }

    inline ObVersionRange &ObTableRuleNode::get_version_range()
    {
      return version_range_;
    }
  }
}
#endif // OB_TABLE_RULE_NODE_H
