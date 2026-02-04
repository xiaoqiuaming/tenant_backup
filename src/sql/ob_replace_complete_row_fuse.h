/**
 * OB_REPLACE_COMPLETE_ROW_FUSE_H defined for replace stmt rows fuse in ups.
 *
 *
 * Version: $Id
 *
 * Authors:
 *   lijianqiang <lijianqiang@mail.nwpu.edu.cn>
 *     --some work detials if U want
 */

#ifndef OB_REPLACE_COMPLETE_ROW_FUSE_H
#define OB_REPLACE_COMPLETE_ROW_FUSE_H

#include "ob_single_child_phy_operator.h"
#include "ob_expr_values.h"
namespace oceanbase
{
  namespace sql
  {
    /**
     * in MultiUPS,for replace stmt, if the rules changed or the table is a hot table,
     * also the replaced row is not complete row, we need to send the full row to UPS
     * which with NULL cells without modification.this class is used for fuse the input
     * data(INPUT from client) and the exist data(static data(CS get) + inc_data(UPS get))
     */
    class ObReplaceCompleteRowFuse:public ObSingleChildPhyOperator
    {
      public:
        ObReplaceCompleteRowFuse();
        virtual ~ObReplaceCompleteRowFuse();
        virtual void reset();
        virtual void reuse();
        int open();
        int close();
        int get_next_row(const common::ObRow *&row);
        int get_row_desc(const common::ObRowDesc *&row_desc) const;
        int64_t to_string(char* buf, const int64_t buf_len) const;
        enum ObPhyOperatorType get_type() const
        {return PHY_REPLACE_COMPLETE_ROW_FUSE;}
        void set_input_values(uint64_t subquery)
        {input_values_subquery_ = subquery;}
        void set_ignore_cell_index(int64_t ignore_cell_index)
        {ignore_cell_index_ = ignore_cell_index;}
        int fuse_replace_complete_row(const ObRow *incr_row, const ObRow *input_row, ObRow &result);
        int convert_to_output_row(ObRow& intput_row, ObRow& output_row);
//        void set_phy_plan(ObPhysicalPlan *the_plan);


        NEED_SERIALIZE_AND_DESERIALIZE;
        DECLARE_PHY_OPERATOR_ASSIGN;
      private:
        // disallow copy
        ObReplaceCompleteRowFuse(const ObReplaceCompleteRowFuse &other);
        ObReplaceCompleteRowFuse& operator=(const ObReplaceCompleteRowFuse &other);
      private:
        ObRow cur_fuse_row_;// for UPS side,has action falg
        ObRow cur_output_row_;//for UPS side,for output,has no acton flag
        uint64_t input_values_subquery_; // for MS side
        int64_t ignore_cell_index_;//need serialized,in fact,is the num of columns which is being repalced
        ObExprValues insert_values_; // need serialized for UPS side
    };
//    inline void ObReplaceCompleteRowFuse::set_phy_plan(ObPhysicalPlan *the_plan)
//    {
//      ObPhyOperator::set_phy_plan(the_plan);
//      insert_values_.set_phy_plan(the_plan);
//    }
  }
}
#endif // OB_REPLACE_COMPLETE_ROW_FUSE_H
