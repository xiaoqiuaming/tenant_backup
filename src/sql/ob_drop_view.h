//add lvjc [drop view]


#ifndef _OB_DROP_VIEW_H
#define _OB_DROP_VIEW_H

#include "sql/ob_no_children_phy_operator.h"
#include "common/ob_strings.h"
#include "sql/ob_sql_context.h"

namespace oceanbase
{
   namespace mergeserver
   {
   class ObMergerRootRpcProxy;
   }

   namespace  sql
   {
    class ObDropView : public ObNoChildrenPhyOperator
    {
    public:
        ObDropView();

        virtual ~ObDropView();

        virtual void reset();
        virtual void reuse();

        void set_rpc_stub(mergeserver::ObMergerRootRpcProxy *rpc);

        void set_sql_context(const ObSqlContext &context);

        void set_if_exists(bool if_exists);

        int add_view_name(const common::ObString &tname);

        int add_table_id(const uint64_t &tid);

        int execute_stmt_no_return_rows(const ObString &stmt);

        int commit();

        int insert_trigger();

        virtual int open();

        virtual int close();

        virtual ObPhyOperatorType get_type() const
        { return PHY_DROP_VIEW; }

        virtual int64_t to_string(char *buf, const int64_t buf_len) const;

        virtual int get_next_row(const common::ObRow *&row);

        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;

   private:

        void clean_owner_privilege();

        ObDropView(const ObDropView &other);

        ObDropView &operator =(const ObDropView &other);


  private:

        bool if_exists_;
        common::ObStrings views_;
        mergeserver::ObMergerRootRpcProxy *rpc_;
        common::ObArray<uint64_t>table_ids_;
        ObSqlContext local_context_;
        ObResultSet *result_set_out_;
    };

    inline int ObDropView::get_next_row(const common::ObRow *&row)
    {
        row = NULL;
        return common::OB_ITER_END;
    }

    inline int ObDropView::get_row_desc(const common::ObRowDesc *&row_desc) const
    {
        row_desc = NULL;
        return common::OB_NOT_SUPPORTED;
    }

    inline void ObDropView::set_rpc_stub(mergeserver::ObMergerRootRpcProxy *rpc)
    {
        rpc_=rpc;
    }

  }
}

#endif

