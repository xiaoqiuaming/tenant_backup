#include "ob_ms_partition_task.h"
#include "ob_ms_partition_manager.h"
#include "ob_merge_server_service.h"

ObMsPartitionTask::ObMsPartitionTask():part_monitor_(NULL),
  service_(NULL), config_mgr_(NULL), frozen_version_has_changed_(false)
{
}

void ObMsPartitionTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  //ObMsPartitionManager *part_mgr = new(std::nothrow) ObMsPartitionManager(service_, config_mgr_->get_client_mgr());
  ObMsPartitionManager *part_mgr = new(std::nothrow) ObMsPartitionManager(service_, config_mgr_->get_client_mgr(), frozen_version_has_changed_ ? service_->fetch_frozen_version() - 1 : service_->fetch_frozen_version());
  if(NULL == part_mgr)
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "no memory");
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
        YYSYS_LOG(INFO, "update all rules scc");
        break;
      }
      usleep(ObPartitionMonitor::RETRY_INTERVAL_TIME);
    }
    ret = OB_SUCCESS;
    if(OB_SUCCESS != (ret = part_monitor_->add_partition_manager(part_mgr)))
    {
      YYSYS_LOG(WARN, "add ms partition manager failed, ret=%d", ret);
    }
    else
    {
      YYSYS_LOG(INFO, "ms add new partition manager succ");
    }
  }
}
