#ifndef OB_ROOT_SCALE_TASK_H
#define OB_ROOT_SCALE_TASK_H

#include "common/ob_timer.h"

namespace oceanbase
{
  namespace rootserver
  {
    class ObRootWorker;
    class ObRootPaxosExpansionDuty : public common::ObTimerTask
    {
    public:
      ObRootPaxosExpansionDuty() :worker_(NULL), timer_(NULL)
      {}
      virtual ~ObRootPaxosExpansionDuty()
      {}
      int init(ObRootWorker *worker, common::ObTimer *timer);
      virtual void runTimerTask(void);
    private:
      ObRootWorker *worker_;
      common::ObTimer *timer_;
   };

    class ObRootWorker;
    class ObRootClusterExpansionDuty : public common::ObTimerTask
    {
    public:
      ObRootClusterExpansionDuty() :worker_(NULL), timer_(NULL)
      {}
      virtual ~ObRootClusterExpansionDuty()
      {}
      int init(ObRootWorker *worker, common::ObTimer *timer);
      virtual void runTimerTask(void);
    private:
      ObRootWorker *worker_;
      common::ObTimer *timer_;
   };

    class ObRootWorker;
    class ObRootInitClusterStat : public common::ObTimerTask
    {
    public:
      ObRootInitClusterStat() : worker_(NULL), timer_(NULL)
      {}
      virtual ~ObRootInitClusterStat()
      {}
      int init(ObRootWorker *worker, common::ObTimer *timer);
      virtual void runTimerTask(void);
    private:
      ObRootWorker *worker_;
      common::ObTimer *timer_;
    };
  }
}


#endif // OB_ROOT_PAXOS_OFFLINE_TASK_H
