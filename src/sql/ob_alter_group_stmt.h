
#ifndef OCEANBASE_SQL_OB_ALTER_GROUP_STMT_H_
#define OCEANBASE_SQL_OB_ALTER_GROUP_STMT_H_

#include "ob_basic_stmt.h"
#include "common/ob_strings.h"

namespace oceanbase
{
  namespace sql
  {
    class ObAlterGroupStmt : public ObBasicStmt
    {
    public:

      explicit ObAlterGroupStmt(common::ObStringBuf* name_pool);

      virtual ~ObAlterGroupStmt();

      virtual void print(FILE* fp, int32_t level, int32_t index = 0);

      int set_group_info(const common::ObString &user, const int64_t idx);

      const common::ObStrings *get_group() const;

      int64_t get_paxos_idx() const;

    private:

    public:
      ObAlterGroupStmt(const ObAlterGroupStmt &other);

      ObAlterGroupStmt& operator=(const ObAlterGroupStmt &other);

    private:
      common::ObStringBuf*  name_pool_;
      common::ObStrings  group_name_;
      int64_t paxos_idx_;
    };
  }
}

#endif //OCEANBASE_SQL_OB_ALTER_TABLE_STMT_H_


