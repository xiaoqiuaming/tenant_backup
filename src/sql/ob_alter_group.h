
#ifndef _OB_ALTER_GROUP_H
#define _OB_ALTER_GROUP_H 1

#include "sql/ob_no_children_phy_operator.h"
#include "common/ob_strings.h"
#include "sql/ob_sql_context.h"

namespace oceanbase
{
  namespace mergeserver
  {
    class ObMergerRootRpcProxy;
  } // end namespace mergeserver

  namespace sql
  {
    class ObAlterGroup: public ObNoChildrenPhyOperator
    {
      public:
        ObAlterGroup();

        virtual ~ObAlterGroup();

        virtual void reset();

        virtual void reuse();

        // init
        void set_rpc_stub(mergeserver::ObMergerRootRpcProxy* rpc);

        void set_sql_context(const ObSqlContext& context);

        void set_group_name(const common::ObString& name);

        void set_paxos_idx(const int64_t &idx);

        void set_affect_row(int64_t num);

        virtual int open();

        virtual int close();
        
        virtual ObPhyOperatorType get_type() const
        { return PHY_ALTER_GROUP; }

        /// @note always return OB_ITER_END
        virtual int get_next_row(const common::ObRow *&row);
        
        /// @note always return OB_NOT_SUPPORTED
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;

        int64_t to_string(char* buf, const int64_t buf_len) const;
        
      private:
        // disallow copy
      private:
        ObAlterGroup(const ObAlterGroup &other);

        ObAlterGroup& operator=(const ObAlterGroup &other);

      private:
        // data members
        common::ObString  group_name_;
        int64_t paxos_idx_;
        ObSqlContext local_context_;
    };
    
    inline int ObAlterGroup::get_next_row(const common::ObRow *&row)
    {
      row = NULL;
      return common::OB_ITER_END;
    }

    inline int ObAlterGroup::get_row_desc(const common::ObRowDesc *&row_desc) const
    {
      row_desc = NULL;
      return common::OB_NOT_SUPPORTED;
    }
  } // end namespace sql
} // end namespace oceanbase

#endif 

