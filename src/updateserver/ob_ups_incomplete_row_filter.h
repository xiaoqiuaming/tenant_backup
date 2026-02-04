/**
 * OB_UPS_INCOMPLETE_ROW_FILTER_CPP defined for replace stmt and update stmt,
 * when execute the pre_execution physical plan in UPS, this operator will
 * check the row is complete or not.
 *
 * Version: $Id
 *
 * Authors:
 *   lijianqiang <lijianqiang@mail.nwpu.edu.cn>
 *     --some work detials if U want
 */

#ifndef OB_UPS_INCOMPLETE_ROW_FILTER_H
#define OB_UPS_INCOMPLETE_ROW_FILTER_H

#include "sql/ob_incomplete_row_filter.h"

namespace oceanbase
{
  namespace updateserver
  {
    /**
     * This class is used for REPLACE STMT AND UPDATE STMT, we build three plans in MS,
     * this class(operator) is for pre_execution_physical_plan, we execute the plan without
     * static data firstly if the UPS has the complete row info for current rowkeys(datas).
     * if the rows U update (replace) is complete, just execute the plan like replace stmt
     * in UPS. Or return to MS and let the MS get static with plan(complete_row_execution_plan)
     * do complete row update(replace). all these cases happen when current table is hot table or
     * the table rule is changed.
     */
    class ObUpsIncompleteRowFilter : public sql::ObIncompleteRowFilter
    {
      public:
        ObUpsIncompleteRowFilter();
        virtual ~ObUpsIncompleteRowFilter();
        virtual void reset();
        virtual void reuse();
        int open();
        int close();
        int get_next_row(const common::ObRow *&row);
        int get_row_desc(const common::ObRowDesc *&row_desc) const;
        int64_t to_string(char* buf, const int64_t buf_len) const;
        void set_table_mgr(updateserver::ObIUpsTableMgr* table_mgr)
        {table_mgr_ = table_mgr;}
        int check_schema_validity();
      private:
        // disallow copy
        ObUpsIncompleteRowFilter(const ObUpsIncompleteRowFilter &other);
        ObUpsIncompleteRowFilter& operator=(const ObUpsIncompleteRowFilter &other);
      private:
        ObIUpsTableMgr* table_mgr_;//for ups side to check schema
    };
  }
}

#endif // OB_UPS_INCOMPLETE_ROW_FILTER_H
