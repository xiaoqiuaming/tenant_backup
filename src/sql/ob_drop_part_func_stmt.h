#ifndef OB_DROP_PART_FUNC_STMT_H
#define OB_DROP_PART_FUNC_STMT_H 1

#include "ob_basic_stmt.h"
#include "common/ob_string.h"
#include "common/ob_strings.h"

namespace oceanbase
{
  namespace sql
  {
    class ObDropPartFuncStmt:public ObBasicStmt
    {
    public:
      explicit ObDropPartFuncStmt(common::ObStringBuf* name_pool);
      virtual ~ObDropPartFuncStmt();
      virtual void print(FILE* fp, int32_t level, int32_t index);
      int add_func(const common::ObString &func_name);
      const common::ObStrings* get_funcs() const;

      ObDropPartFuncStmt(const ObDropPartFuncStmt& other);
      ObDropPartFuncStmt& operator=(const ObDropPartFuncStmt& other);
    private:
      common::ObStrings funcs_name_;
      common::ObStringBuf* name_pool_;
    };
  }
}
#endif // OB_DROP_PART_FUNC_STMT_H
