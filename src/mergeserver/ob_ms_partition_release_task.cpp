/*add by wuna [MultiUps] [part_cache] 20160118*/

#include "ob_ms_partition_release_task.h"
#include "ob_ms_partition_manager.h"
#include "ob_ms_rpc_proxy.h" //add liuzy [MultiUPS] [BugFix] [FetchUpsList] 20160803
ObMsPartitionReleaseTask::ObMsPartitionReleaseTask():part_monitor_(NULL)
{
}

void ObMsPartitionReleaseTask::runTimerTask()
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
  YYSYS_LOG(DEBUG, "MS RELEASE TASK RUNNED.part_mgrs.count is %ld",part_mgrs.count());
  (void)ret;
}

//add liuzy [MultiUPS] [BugFix] [FetchUpsList] 20160803:b
ObMsFetchUpsTask::ObMsFetchUpsTask(): rpc_(NULL)
{}

void ObMsFetchUpsTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  int32_t count = 0;
  if (OB_SUCCESS != (ret = rpc_->fetch_master_update_server_list(count)))
  {
    YYSYS_LOG(WARN, "fetch master ups list failed, ret=%d", ret);
  }
  else
  {
    YYSYS_LOG(DEBUG, "fetch master ups list succeed, ups count:[%d]", count);
  }
}
//add 20160803:e
