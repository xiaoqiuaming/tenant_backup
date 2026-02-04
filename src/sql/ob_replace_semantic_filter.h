/**
 * OB_REPLACE_SEMANTIC_FILTER_H defined for replace stmt pre_execution_plan in ups.
 *
 *
 * Version: $Id
 *
 * Authors:
 *   lijianqiang <lijianqiang@mail.nwpu.edu.cn>
 *     --some work detials if U want
 */

#ifndef OB_REPLACE_SEMANTIC_FILTER_H
#define OB_REPLACE_SEMANTIC_FILTER_H

#include "ob_single_child_phy_operator.h"
#include "ob_expr_values.h"

namespace oceanbase
{
  namespace sql
  {
    /**
     * in MultiUPS,for replace stmt, if the rules changed or the table is a hot table,
     * also the replaced row is not complete row, we firstly execute the plan named
     * "pre_execution_plan" in UPS, which will cons complete row info at first,if can't
     * cons complete row info for current repalced rows,return OB_INCOMPLETE_ROW to MS.
     * This class is a filter which judge if can insert values for the pre_excution plan.
     */
    class ObReplaceSemanticFilter:public ObSingleChildPhyOperator
    {
      public:
        ObReplaceSemanticFilter();
        virtual ~ObReplaceSemanticFilter();
        virtual void reset();
        virtual void reuse();
        int open();
        int close();
        int get_next_row(const common::ObRow *&row);
        int get_row_desc(const common::ObRowDesc *&row_desc) const;
        int64_t to_string(char* buf, const int64_t buf_len) const;
        enum ObPhyOperatorType get_type() const
        {return PHY_REPLACE_SEMANTIC_FILTER;}
        void set_raw_row_desc(const ObRowDesc& row_desc)
        {raw_row_desc_ = row_desc;}
        int cut_null_cell(const common::ObRow *& row);
        void set_input_values(uint64_t subquery)
        {input_values_subquery_ = subquery;}
        NEED_SERIALIZE_AND_DESERIALIZE;
        DECLARE_PHY_OPERATOR_ASSIGN;
      private:
        // disallow copy
        ObReplaceSemanticFilter(const ObReplaceSemanticFilter &other);
        ObReplaceSemanticFilter& operator=(const ObReplaceSemanticFilter &other);
      private:
        uint64_t input_values_subquery_; // for MS side
        ObExprValues insert_values_; // need serialized for UPS side
        bool could_replace_; //for ups side
        ObRowDesc raw_row_desc_;//need serialize and deserialize
        ObRow cur_row_;//for ups side
    };

  }
}

#endif // OB_REPLACE_SEMANTIC_FILTER_H
