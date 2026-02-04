#include "ob_root_scale_task.h"
#include "rootserver/ob_root_worker.h"

namespace oceanbase
{
  namespace rootserver
  {
    int ObRootPaxosExpansionDuty::init(ObRootWorker *worker, common::ObTimer *timer)
    {
      int ret = OB_SUCCESS;
      if (NULL == worker)
      {
        YYSYS_LOG(WARN, "invalid argument. worker=NULL");
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        worker_ = worker;
        timer_ = timer;
      }
      return ret;
    }

    void ObRootPaxosExpansionDuty::runTimerTask(void)
    {
      if (!worker_->get_root_server().is_merge_done())
      {
        timer_->schedule(*this, 1 * 1000 * 1000, false);
      }
      else
      {
        worker_->get_root_server().set_offline_paxos_stat();
      }
    }

    int ObRootClusterExpansionDuty::init(ObRootWorker *worker, common::ObTimer *timer)
    {
      int ret = OB_SUCCESS;
      if (NULL == worker)
      {
        YYSYS_LOG(WARN, "invalid argument. worker=NULL");
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        worker_ = worker;
        timer_ = timer;
      }
      return ret;
    }

    void ObRootClusterExpansionDuty::runTimerTask(void)
    {
      if (!worker_->get_root_server().is_merge_done())
      {
        timer_->schedule(*this, 1 * 1000 * 1000, false);
      }
      else
      {
        worker_->get_root_server().set_offline_cluster_stat();
      }
    }

    int ObRootInitClusterStat::init(ObRootWorker *worker, common::ObTimer *timer)
    {
      int ret = OB_SUCCESS;
      if (NULL == worker)
      {
        YYSYS_LOG(WARN, "invalid argument. worker=NULL");
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        worker_ = worker;
        timer_ = timer;
      }
      return ret;
    }
    void ObRootInitClusterStat::runTimerTask(void)
    {
       int ret = OB_SUCCESS;
      if (ObRoleMgr::MASTER == worker_->get_role_manager()->get_role())
      {
        if (!worker_->get_root_server().is_initialized())
        {
          timer_->schedule(*this, 1 * 1000 * 1000, false);
        }
        else if (OB_SUCCESS != (ret = worker_->get_root_server().init_cluster_stat_info()))
        {
          if (ret == OB_RS_INVALID_CLUSTER_OR_PAXOS_NUM)
          {}
          else
          {
            timer_->schedule(*this, 30 * 1000, false);
          }
        }
        else if (OB_SUCCESS != worker_->get_root_server().init_cluster_stat_info())
        {
           YYSYS_LOG(WARN, "push cluster stat info to other rs failed!");
        }
      }
    }
  }
}


