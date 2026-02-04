/*
* Version: $ObPaxos V0.1$
*
* Authors:
*  pangtianze <pangtianze@ecnu.cn>
*
* Date:
*  20150611
*
*  - heartbeat from rs to rs
*
*/
#include "ob_rs_heartbeat_runnable.h"
#include "common/ob_define.h"
#include "yysys.h"

using namespace rootserver;
using namespace common;
ObRsHeartbeatRunnable::ObRsHeartbeatRunnable(ObRootElectionNodeMgr &rs_node_mgr)
  :rs_node_mgr_(rs_node_mgr)
{
}
ObRsHeartbeatRunnable::~ObRsHeartbeatRunnable()
{
}
void ObRsHeartbeatRunnable::run(yysys::CThread* thread, void* arg)
{
  UNUSED(thread);
  UNUSED(arg);
  YYSYS_LOG(INFO, "[NOTICE] rs heartbeat thread start, tid=%ld", syscall(__NR_gettid));
  SET_THD_NAME_ONCE("rs-rs-heart");
  while (!_stop)
  {
    if (ObRootElectionNode::OB_LEADER == rs_node_mgr_.get_my_role())
    {
      rs_node_mgr_.grant_lease();
    }
    usleep(CHECK_INTERVAL_US);
  }
  YYSYS_LOG(INFO, "[NOTICE] rs heartbeat thread exit");
}


