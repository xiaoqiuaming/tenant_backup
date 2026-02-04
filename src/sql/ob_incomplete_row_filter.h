/**
 * OB_INCOMPLETE_ROW_FILTER_H defined for replace stmt and update stmt,
 * when execute the pre_execution physical plan in UPS, this operator will
 * check the row is complete or not.
 *
 * Version: $Id
 *
 * Authors:
 *   lijianqiang <lijianqiang@mail.nwpu.edu.cn>
 *     --some work detials if U want
 */
#ifndef OB_INCOMPLETE_ROW_FILTER_H
#define OB_INCOMPLETE_ROW_FILTER_H

#include "ob_single_child_phy_operator.h"
#include "updateserver/ob_ups_table_mgr.h"

namespace oceanbase
{
  namespace sql
  {
    class ObIncompleteRowFilter: public ObSingleChildPhyOperator
    {
      public:
        ObIncompleteRowFilter();
        virtual ~ObIncompleteRowFilter();
        virtual void reset();
        virtual void reuse();
        int get_next_row(const common::ObRow *&row)
        {
          UNUSED(row);
          return OB_NOT_SUPPORTED;
        }
        int get_row_desc(const common::ObRowDesc *&row_desc) const
        {
          UNUSED(row_desc);
          return OB_NOT_SUPPORTED;
        }
        int64_t to_string(char* buf, const int64_t buf_len) const;
        enum ObPhyOperatorType get_type() const
        {return PHY_INCOMPLETE_ROW_FILTER;}
        NEED_SERIALIZE_AND_DESERIALIZE;
        DECLARE_PHY_OPERATOR_ASSIGN;
      private:
        // disallow copy
        ObIncompleteRowFilter(const ObIncompleteRowFilter &other);
        ObIncompleteRowFilter& operator=(const ObIncompleteRowFilter &other);
    };
  }
}



#endif // OB_INCOMPLETE_ROW_FILTER_H
