/**
  *    add lvjc [drop view] 20210701 b
  */
#ifndef OCEANBASE_SQL_OB_DROP_VIEW_STMT_H_
#define OCEANBASE_SQL_OB_DROP_VIEW_STMT_H_

#include "common/ob_string.h"
#include "common/ob_string_buf.h"
#include "sql/ob_basic_stmt.h"
#include "sql/parse_node.h"

namespace oceanbase
{
    namespace sql
    {
        class ObDropViewStmt : public ObBasicStmt
        {
        public:
            explicit ObDropViewStmt(common::ObStringBuf *name_pool);
            virtual ~ObDropViewStmt();

            void set_if_exists(bool if_not_exists);

            bool get_if_exists() const;

            int add_view_name_id(ResultPlan &result_plan, const common::ObString &table_name);

            int64_t get_view_size() const;

            const common::ObString &get_view_name(int64_t index) const;

            int64_t get_view_id_size() const;

            uint64_t get_view_id(int64_t index) const;

            void print(FILE *fp, int32_t level, int32_t index = 0);

        protected:
            common::ObStringBuf *name_pool_;

        private:
            bool if_exists_;
            common::ObArray<common::ObString> views_;
            common::ObArray<uint64_t> view_ids_;

        };

        inline int64_t ObDropViewStmt::get_view_id_size() const
        {
            return view_ids_.count();
        }

        inline uint64_t ObDropViewStmt::get_view_id(int64_t index) const
        {
            OB_ASSERT(0 <= index && index < view_ids_.count());
            return view_ids_.at(index);
        }

        inline void ObDropViewStmt::set_if_exists(bool if_exists)
        {
            if_exists_ = if_exists;
        }

        inline bool ObDropViewStmt::get_if_exists() const
        {
            return if_exists_;
        }

        inline int64_t ObDropViewStmt::get_view_size() const
        {
            return views_.count();
        }

        inline const common::ObString &ObDropViewStmt::get_view_name(int64_t index) const
        {
            OB_ASSERT(0 <= index && index < views_.count());
            return views_.at(index);
        }
    }
}

#endif //OCEANBASE_SQL_OB_DROP_VIEW_STMT_H_
