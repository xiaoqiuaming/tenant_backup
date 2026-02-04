/*
* Version: $ObPaxos V0.1$
*
* Authors:
*  pangtianze <pangtianze@ecnu.cn>
*
* Date:
*  20150609
*
*  - check lease to decide whether or not that rs needs raise new election
*
*/
#include "ob_root_election_checker.h"
#include "ob_root_election_node_mgr.h"
#include "common/ob_define.h"
#include "yysys.h"

using namespace rootserver;
using namespace common;
ObRsCheckRunnable::ObRsCheckRunnable(ObRootElectionNodeMgr& rs_election_mgr)
  :rs_node_mgr_(rs_election_mgr)
{
}
ObRsCheckRunnable::~ObRsCheckRunnable()
{
}
void ObRsCheckRunnable::run(yysys::CThread* thread, void* arg)
{
  UNUSED(thread);
  UNUSED(arg);
  YYSYS_LOG(INFO, "[NOTICE] rs check thread start, waiting [10s]");
  const int wait_second = 10;
  for (int64_t i = 0; i < wait_second && !_stop; i++)
  {
    sleep(1);
  }
  YYSYS_LOG(INFO, "[NOTICE] rs check working");
  SET_THD_NAME_ONCE("rs-rs-chk");
  while (!_stop)
  {
    //mod pangtianze [Paxos rs_election] 20170628:b
    int64_t begt = yysys::CTimeUtil::getTime();
    rs_node_mgr_.check_lease();
    int64_t endt = yysys::CTimeUtil::getTime();
    if (endt - begt > 500 * 1000) //
    {
        YYSYS_LOG(WARN, " real check lease interval[%ld]", endt - begt);
    }
    usleep(CHECK_INTERVAL_US);
  }
  YYSYS_LOG(INFO, "[NOTICE] rs check thread exit");
}
