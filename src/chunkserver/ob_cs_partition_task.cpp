#include "ob_cs_partition_task.h"
#include "ob_chunk_service.h"

ObCsPartitionTask::ObCsPartitionTask():part_monitor_(NULL),
    service_(NULL), config_mgr_(NULL), frozen_version_sync_from_rs_has_changed_(false)
{
}

void ObCsPartitionTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObCsPartitionManager *part_mgr = new(std::nothrow)ObCsPartitionManager(service_, config_mgr_, frozen_version_sync_from_rs_has_changed_ ? service_->get_frozen_version() - 1 : service_->get_frozen_version());
  if(NULL == part_mgr)
  {
    ret = OB_ERROR;
    YYSYS_LOG(ERROR, "no memory");
  }
  else
  {
    for(int i = 0;i < ObPartitionMonitor::RETRY_TIMES;i++)
    {
      part_mgr->reuse();
      if(OB_SUCCESS != (ret = part_mgr->update_all_rules()))
      {
        YYSYS_LOG(WARN, "update all rules failed, retry_times=%d", i);
      }
      else
      {
        YYSYS_LOG(INFO, "update all rules succ");
        break;
      }
      usleep(ObPartitionMonitor::RETRY_INTERVAL_TIME);
    }

    //no matter whether ret is success, must add new partition manager
    ret = OB_SUCCESS;
    if(OB_SUCCESS != (ret = part_monitor_->add_partition_manager(part_mgr)))
    {
      YYSYS_LOG(ERROR, "add cs partition manager failed, ret=%d", ret);
    }
    else
    {
      YYSYS_LOG(INFO, "add cs partition manager succ");
    }
  }
}
