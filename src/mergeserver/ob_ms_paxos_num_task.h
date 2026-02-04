#ifndef OB_MERGER_PAXOS_NUM_TASK_H
#define OB_MERGER_PAXOS_NUM_TASK_H

#include "common/ob_timer.h"
#include "ob_ms_sql_proxy.h"

namespace oceanbase{
  namespace mergeserver{
    class ObMergeServerService;
    class ObMergerPaxosNumTask : public common::ObTimerTask
    {
    public:
      ObMergerPaxosNumTask();
      ~ObMergerPaxosNumTask();

      int init(ObMergeServerService *service, ObMsSQLProxy *sql_proxy);
      void runTimerTask(void);
    private:
      ObMergeServerService *service_;
      ObMsSQLProxy *sql_proxy_;
      static const ObString &query;
    };
  }
}
#endif // OB_MERGER_PAXOS_NUM_TASK_H
