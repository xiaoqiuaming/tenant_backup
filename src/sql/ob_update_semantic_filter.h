/**
 * OB_UPDATE_SEMANTIC_FILTER_H defined for update stmt pre_execution_plan in ups.
 *
 *
 * Version: $Id
 *
 * Authors:
 *   lijianqiang <lijianqiang@mail.nwpu.edu.cn>
 *     --some work detials if U want
 */

#ifndef OB_UPDATE_SEMANTIC_FILTER_H
#define OB_UPDATE_SEMANTIC_FILTER_H

#include "ob_multi_children_phy_operator.h"
namespace oceanbase
{
  namespace sql
  {
    /**
     * in MultiUPS,for update stmt, if the rules changed or the table is a hot table,
     * also the updated row is not complete row, we firstly execute the plan named
     * "pre_execution_plan" in UPS, which will cons complete row info at first,if can't
     * cons complete row info for current repalced rows,return OB_INCOMPLETE_ROW to MS.
     * This class is a filter which judge if can update values for the pre_excution plan.
     */
    class ObUpdateSemanticFilter:public ObMultiChildrenPhyOperator
    {
      public:
        ObUpdateSemanticFilter();
        virtual ~ObUpdateSemanticFilter();
        virtual void reset();
        virtual void reuse();
        virtual int open();
        virtual int close();
        virtual int get_next_row(const common::ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        int set_child(int32_t child_idx, ObPhyOperator &child_operator);
        virtual int32_t get_child_num() const;
        int64_t to_string(char* buf, const int64_t buf_len) const;
        enum ObPhyOperatorType get_type() const
        {return PHY_UPDATE_SEMANTIC_FILTER;}
        NEED_SERIALIZE_AND_DESERIALIZE;
        DECLARE_PHY_OPERATOR_ASSIGN;
      private:
        // disallow copy
        ObUpdateSemanticFilter(const ObUpdateSemanticFilter &other);
        ObUpdateSemanticFilter& operator=(const ObUpdateSemanticFilter &other);
      private:
        bool could_update_; //for ups side
        int32_t child_num_;
    };
  }
}

#endif // OB_UPDATE_SEMANTIC_FILTER_H
