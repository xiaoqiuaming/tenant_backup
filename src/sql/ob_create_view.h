//add lvjc [create view]

#ifndef OCEANBASE_SQL_OB_CREATE_VIEW_H_
#define OCEANBASE_SQL_OB_CREATE_VIEW_H_

#include "sql/ob_create_view_stmt.h"
#include "sql/ob_no_children_phy_operator.h"
#include "sql/ob_sql_context.h"
#include "common/ob_schema_service.h"
#include "common/ob_partition_monitor.h"

namespace oceanbase
{
    namespace mergeserver
    {
        class ObMergerRootRpcProxy;
    } // end namespace mergeserver

    namespace sql
    {
        class ObCreateView : public ObNoChildrenPhyOperator
        {
            public:
              ObCreateView();

              virtual ~ObCreateView();

              common::TableSchema &get_table_schema();

              void set_sql_context(const ObSqlContext &context);

              void set_do_replace(bool do_replace);

              void set_with_check_option(ObViewCheckOption with_check_option);

              virtual int open();

              virtual int close();

              virtual void reset();

              virtual void reuse();

              virtual ObPhyOperatorType get_type() const
              { return PHY_CREATE_VIEW; }

              virtual int64_t to_string(char *buf, const int64_t buf_len) const;

              virtual int get_next_row(const common::ObRow *&row);

              virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;

            private:
              void grant_owner_privilege();

              ObCreateView(const ObCreateView &other);
              ObCreateView &operator = (const ObCreateView &other);

            private:
              //data members
              bool do_replace_;
              ObViewCheckOption with_check_option_;
              common::TableSchema table_schema_;
              ObSqlContext local_context_;
        };

        inline common::TableSchema &ObCreateView::get_table_schema()
        {
            return table_schema_;
        }

        inline void ObCreateView::set_do_replace(bool do_replace)
        {
            do_replace_ = do_replace;
        }

        inline int ObCreateView::get_next_row(const common::ObRow *&row)
        {
            row = NULL;
            return common::OB_ITER_END;
        }

        inline int ObCreateView::get_row_desc(const common::ObRowDesc *&row_desc) const
        {
            row_desc = NULL;
            return common::OB_NOT_SUPPORTED;
        }

        inline void ObCreateView::set_with_check_option(ObViewCheckOption with_check_option)
        {
            with_check_option_ = with_check_option;
        }
    } //end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_CREATE_VIEW_H_ */
//add lvjc e
