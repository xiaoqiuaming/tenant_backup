#include "sql/ob_alter_group.h"
#include "common/utility.h"
#include "mergeserver/ob_rs_rpc_proxy.h"
#include "sql/ob_sql.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

ObAlterGroup::ObAlterGroup()
  : paxos_idx_(-1)
{
}

ObAlterGroup::~ObAlterGroup()
{
}

void ObAlterGroup::reset()
{
  paxos_idx_ = -1;
  local_context_.rs_rpc_proxy_ = NULL;
}

void ObAlterGroup::reuse()
{
  paxos_idx_ = -1;
  local_context_.rs_rpc_proxy_ = NULL;
}

void ObAlterGroup::set_sql_context(const ObSqlContext &context)
{
  local_context_ = context;
  local_context_.schema_manager_ = NULL;
}

void ObAlterGroup::set_group_name(const ObString& group_name)
{
  group_name_ = group_name;
}

void ObAlterGroup::set_paxos_idx(const int64_t &idx)
{
  paxos_idx_ = idx;
}

void ObAlterGroup::set_affect_row(int64_t affected_row)
{
  OB_ASSERT(my_phy_plan_);
  ObResultSet* outer_result_set = NULL;
  ObResultSet* my_result_set = NULL;
  my_result_set = my_phy_plan_->get_result_set();
  outer_result_set = my_result_set->get_session()->get_current_result_set();
  my_result_set->set_session(outer_result_set->get_session()); // be careful!
  YYSYS_LOG(DEBUG,"set affected row =%ld",affected_row);
  outer_result_set->set_affected_rows(affected_row);
}

int ObAlterGroup::open()
{
  int ret = OB_SUCCESS;
  if (NULL == local_context_.rs_rpc_proxy_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR, "ObAlterGroup not init, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = local_context_.rs_rpc_proxy_->alter_group(group_name_, paxos_idx_)))
  {
    YYSYS_LOG(WARN, "failed to alter group, err=%d", ret);
  }
  else
  {
    set_affect_row(1);
    YYSYS_LOG(INFO, "alter group succ,group name(%.*s) paxos_idx_=%ld",
              group_name_.length(), group_name_.ptr(), paxos_idx_);
  }
  return ret;
}

int ObAlterGroup::close()
{
  int ret = OB_SUCCESS;
  return ret;
}

int64_t ObAlterGroup::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "alter group paxos_idx=%ld,group name(%.*s)",
                                      paxos_idx_, group_name_.length(), group_name_.ptr());
  return pos;
}

namespace oceanbase
{
  namespace sql
  {
    REGISTER_PHY_OPERATOR(ObAlterGroup, PHY_ALTER_GROUP);
  }
}
