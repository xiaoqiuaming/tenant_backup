/*add by wuna [MultiUps] [part_cache] 20160118*/
#ifndef OB_MS_PARTITION_RELEASE_TASK_H
#define OB_MS_PARTITION_RELEASE_TASK_H

#include "common/ob_partition_monitor.h"
#include "common/ob_config_manager.h"

using namespace oceanbase::mergeserver;

namespace oceanbase
{
  namespace mergeserver
  {
    class ObMsPartitionReleaseTask : public ObTimerTask
    {
      public:
        ObMsPartitionReleaseTask();
        int init(ObMergeServerService *service,ObPartitionMonitor *part_monitor);
        void runTimerTask();
      private:
        ObMergeServerService *service_;
        ObPartitionMonitor *part_monitor_;
    };

    inline int ObMsPartitionReleaseTask::init(ObMergeServerService *service,ObPartitionMonitor *part_monitor)
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
    //add liuzy [MultiUPS] [BugFix] [FetchUpsList] 20160803:b
    class ObMergerRpcProxy;
    class ObMsFetchUpsTask : public common::ObTimerTask
    {
      public:
        ObMsFetchUpsTask ();
      public:
        int init(ObMergerRpcProxy *rpc);
        virtual void runTimerTask();
      private:
        ObMergerRpcProxy *rpc_;
    };
    inline int ObMsFetchUpsTask::init(ObMergerRpcProxy *rpc)
    {
      int ret = OB_SUCCESS;
      if (NULL == rpc)
      {
        ret = OB_NOT_INIT;
        YYSYS_LOG(WARN, "rpc proxy is not init, ret=%d", ret);
      }
      else
      {
        rpc_ = rpc;
      }
      return ret;
    }
    //add 20160803:e
  }
}
#endif // OB_MS_PARTITION_RELEASE_TASK_H
