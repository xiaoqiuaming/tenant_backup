
#include "ob_alter_group_stmt.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

ObAlterGroupStmt::ObAlterGroupStmt(const ObAlterGroupStmt &other)
  : ObBasicStmt(other.get_stmt_type(), other.get_query_id())
{
  *this = other;
}

ObAlterGroupStmt::ObAlterGroupStmt(ObStringBuf* name_pool)
  : ObBasicStmt(ObBasicStmt::T_ALTER_GROUP)
{
  name_pool_ = name_pool;
  paxos_idx_ = -1;
}

ObAlterGroupStmt::~ObAlterGroupStmt()
{
}

ObAlterGroupStmt& ObAlterGroupStmt::operator=(const ObAlterGroupStmt &other)
{
  if (this == &other)
  {
  }
  else
  {
    int ret = OB_SUCCESS;
    const common::ObStrings* group = other.get_group();
    int i = 0;
    for (i = 0;i < group->count();i++)
    {
      ObString group_name;
      if (OB_SUCCESS != (ret = group->get_string(i, group_name)))
      {
        YYSYS_LOG(WARN, "get group name failed,ret=%d", ret);
        break;
      }
      else if (OB_SUCCESS != (ret = this->set_group_info(group_name, other.get_paxos_idx())))
      {
        YYSYS_LOG(WARN, "add group info failed, ret=%d", ret);
        break;
      }
    }
  }
  return *this;
}

void ObAlterGroupStmt::print(FILE* fp, int32_t level, int32_t index)
{
  print_indentation(fp, level);
  fprintf(fp, "<ObAlterGroupStmt id=%d>\n", index);
  common::ObString group_name;
  if (common::OB_SUCCESS != group_name_.get_string(0, group_name))
  {
    YYSYS_LOG(WARN, "failed to get group_name");
  }
  else
  {
    print_indentation(fp, level + 1);
    fprintf(fp, "group_info := (%.*s, %ld)",
          group_name.length(), group_name.ptr(), paxos_idx_);
  }
  fprintf(fp, "\n");
  print_indentation(fp, level);
  fprintf(fp, "</ObAlterGroupStmt>\n");
}

int64_t ObAlterGroupStmt::get_paxos_idx() const
{
  return paxos_idx_;
}

const ObStrings *ObAlterGroupStmt::get_group() const
{
  return &group_name_;
}

int ObAlterGroupStmt::set_group_info(const common::ObString& group_name, const int64_t idx)
{
  int ret = OB_SUCCESS;
  if (0 != group_name_.count())
  {
    ret = OB_ERR_UNEXPECTED;
    YYSYS_LOG(ERROR, "should be set only once");
  }
  else if (OB_SUCCESS != (ret = group_name_.add_string(group_name)))
  {
    YYSYS_LOG(WARN, "failed to add string, err=%d", ret);
  }
  else
  {
    paxos_idx_ = idx;
  }
  return ret;
}
