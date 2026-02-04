#include "ob_drop_part_func_stmt.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

ObDropPartFuncStmt::ObDropPartFuncStmt(ObStringBuf* name_pool):ObBasicStmt(ObBasicStmt::T_DROP_PART_FUNC)
{
  name_pool_ = name_pool;
}

ObDropPartFuncStmt::~ObDropPartFuncStmt()
{
}

ObDropPartFuncStmt::ObDropPartFuncStmt(const ObDropPartFuncStmt& other)
  :ObBasicStmt(other.get_stmt_type(), other.get_query_id())
{
  *this = other;
}

ObDropPartFuncStmt& ObDropPartFuncStmt::operator=(const ObDropPartFuncStmt& other)
{
  int ret = OB_SUCCESS;
  if(this == &other)
  {
  }
  else
  {
    const ObStrings *funcs = other.get_funcs();
    int64_t i = 0;
    int64_t count = funcs->count();
    ObString func;
    for(i = 0;i < count;i++)
    {
      if(OB_SUCCESS != (ret = funcs->get_string(i,func)))
      {
        break;
      }
      else if(OB_SUCCESS != (ret = this->add_func(func)))
      {
        break;
      }
    }
  }
  return *this;
}

const ObStrings* ObDropPartFuncStmt::get_funcs() const
{
  return &funcs_name_;
}

int ObDropPartFuncStmt::add_func(const ObString &func_name)
{
  int ret = OB_SUCCESS;
  if(OB_SUCCESS != (ret = funcs_name_.add_string(func_name)))
  {
    YYSYS_LOG(WARN, "failed to add user to DropUserStmt, err=%d", ret);
  }
  return ret;
}
void ObDropPartFuncStmt::print(FILE *fp, int32_t level, int32_t index)
{
  UNUSED(fp);
  UNUSED(level);
  UNUSED(index);
}
