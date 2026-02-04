/*add by wuna [MultiUps] [part_cache] 20160118*/
#include "ob_cs_partition_manager.h"
#include "ob_cs_partition_release_task.h"

ObCsPartitionReleaseTask::ObCsPartitionReleaseTask():service_(NULL),part_monitor_(NULL)
{
}

void ObCsPartitionReleaseTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObArray<ObPartitionMonitor::ManagerItem*> part_mgrs = part_monitor_->get_partition_manager_array();
  if(0 == part_mgrs.count())
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "unexpected error.");
  }
  else
  {
    part_monitor_->release_partition_manager_task();
  }
  YYSYS_LOG(DEBUG, "CS RELEASE TASK RUNNED.part_mgrs.count is %ld",part_mgrs.count());
  (void)ret;
}

