/*add by wuna [MultiUps] [part_cache] 20160118*/

#ifndef OB_CS_PARTITION_RELEASE_TASK_H
#define OB_CS_PARTITION_RELEASE_TASK_H

#include "common/ob_partition_monitor.h"
#include "common/ob_config_manager.h"

using namespace oceanbase::chunkserver;

namespace oceanbase
{
  namespace chunkserver
  {
    class ObChunkService;
    class ObCsPartitionReleaseTask : public ObTimerTask
    {
    public:
      ObCsPartitionReleaseTask();
      int init(ObChunkService *service,ObPartitionMonitor *part_monitor);
      void runTimerTask();
    private:
      ObChunkService *service_;
      ObPartitionMonitor *part_monitor_;
    };

    inline int ObCsPartitionReleaseTask::init(ObChunkService *service,ObPartitionMonitor *part_monitor)
    {
      int ret = OB_SUCCESS;
      if(NULL != service && NULL != part_monitor)
      {
        service_ = service;
        part_monitor_ = part_monitor;
      }
      else
      {
        ret = OB_ERROR;
      }
      return ret;
    }
  }
}

#endif // OB_CS_PARTITION_RELEASE_TASK_H
