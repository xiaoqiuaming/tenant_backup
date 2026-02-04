#ifndef OB_MS_PARTITION_TASK_H
#define OB_MS_PARTITION_TASK_H

#include "common/ob_partition_monitor.h"
#include "common/ob_config_manager.h"

using namespace oceanbase::mergeserver;

namespace oceanbase
{
  namespace mergeserver
  {
    class ObMsPartitionTask : public ObTimerTask
    {
      public:
        static const int RETRY_TIMES = 3;
        static const int64_t RETRY_INTERVAL_TIME = 1000 * 1000;
        ObMsPartitionTask();
        //void init(ObMergeServerService *service, ObConfigManager *config_mgr, ObPartitionMonitor *part_monitor);
        void init(ObMergeServerService *service, ObConfigManager *config_mgr, ObPartitionMonitor *part_monitor, bool frozen_version_has_changed);
        void runTimerTask();
      private:
        ObPartitionMonitor *part_monitor_;
        ObMergeServerService *service_;
        ObConfigManager *config_mgr_;
        bool frozen_version_has_changed_;
    };

    /*
    inline void ObMsPartitionTask::init(ObMergeServerService *service, ObConfigManager *config_mgr, ObPartitionMonitor *part_monitor)
    {
      service_ = service;
      config_mgr_ = config_mgr;
      part_monitor_ = part_monitor;
    }
    */
    inline void ObMsPartitionTask::init(ObMergeServerService *service, ObConfigManager *config_mgr, ObPartitionMonitor *part_monitor, bool frozen_version_has_changed)
    {
      service_ = service;
      config_mgr_ = config_mgr;
      part_monitor_ = part_monitor;
      frozen_version_has_changed_ = frozen_version_has_changed;
    }
  }
}
#endif // OB_MS_PARTITION_TASK_H
