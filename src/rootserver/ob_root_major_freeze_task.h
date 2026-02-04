#ifndef OB_ROOT_MAJOR_FREEZE_TASK_H
#define OB_ROOT_MAJOR_FREEZE_TASK_H

#include "common/ob_timer.h"

namespace oceanbase
{
  namespace rootserver
  {
    class ObRootWorker;
    class ObRootMajorFreezeDuty: public common::ObTimerTask
    {
      public:
        ObRootMajorFreezeDuty():to_be_frozen_version_(common::OB_INVALID_VERSION),worker_(NULL)
      {}
        virtual ~ObRootMajorFreezeDuty()
        {}
        int init(ObRootWorker *worker, const int64_t to_be_frozen_version);
        virtual void runTimerTask(void);
      private:
        int64_t to_be_frozen_version_;
        ObRootWorker* worker_;
    };
    //add hongchen [CLUSTER_STAT_BUGFIX] 20170821:b
    class ObRootClusterStatDuty: public common::ObTimerTask
    {
      public:
        ObRootClusterStatDuty():to_be_frozen_version_(common::OB_INVALID_VERSION),worker_(NULL),timer_(NULL)
      {}
        virtual ~ObRootClusterStatDuty()
        {}
        int init(ObRootWorker *worker, common::ObTimer *timer, const int64_t to_be_frozen_version);
        virtual void runTimerTask(void);
      private:
        int64_t to_be_frozen_version_;
        ObRootWorker* worker_;
        common::ObTimer*      timer_;
    };
    //add hongchen [CLUSTER_STAT_BUGFIX] 20170821:e
    class ObRootPartitionDuty: public common::ObTimerTask
    {
      public:
        ObRootPartitionDuty():last_mem_frozen_version_(common::OB_INVALID_VERSION),worker_(NULL)
        {}
        virtual ~ObRootPartitionDuty()
        {}
        int init(ObRootWorker *worker, const int64_t last_mem_frozen_version, const bool partition_changed_flag);
        virtual void runTimerTask(void);
      private:
        int64_t last_mem_frozen_version_;
        ObRootWorker* worker_;
        bool partition_changed_flag_;
    };
  }
}

#endif // OB_ROOT_MAJOR_FREEZE_TASK_H
