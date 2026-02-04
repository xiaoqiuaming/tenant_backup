#ifndef OB_CS_PARTITION_TASK_H
#define OB_CS_PARTITION_TASK_H

#include "common/ob_partition_monitor.h"
#include "common/ob_config_manager.h"
#include "ob_cs_partition_manager.h"

using namespace oceanbase::chunkserver;
namespace oceanbase
{
  namespace chunkserver
  {
    class ObChunkService;
    class ObCsPartitionTask : public ObTimerTask
    {
    public:
      ObCsPartitionTask();
      void init(ObChunkService *service, ObConfigManager* config_mgr, ObPartitionMonitor *part_monitor, bool frozen_version_sync_from_rs_has_changed);
      void runTimerTask();
    private:
      ObPartitionMonitor *part_monitor_;
      ObChunkService *service_;
      ObConfigManager *config_mgr_;

      bool frozen_version_sync_from_rs_has_changed_;

    };

    inline void ObCsPartitionTask::init(ObChunkService *service, ObConfigManager *config_mgr, ObPartitionMonitor *part_monitor, bool frozen_version_sync_from_rs_has_changed)
    {
      service_ = service;
      config_mgr_ = config_mgr;
      part_monitor_ = part_monitor;
      frozen_version_sync_from_rs_has_changed_ = frozen_version_sync_from_rs_has_changed;
    }
  }
}
#endif // OB_CS_PARTITION_TASK_H
