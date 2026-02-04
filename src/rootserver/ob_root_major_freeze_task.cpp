#include "rootserver/ob_root_major_freeze_task.h"
#include "rootserver/ob_root_worker.h"
#include "rootserver/ob_root_server2.h"
namespace oceanbase
{
  namespace rootserver
  {
    int ObRootMajorFreezeDuty::init(ObRootWorker *worker, const int64_t to_be_frozen_version)
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
        to_be_frozen_version_ = to_be_frozen_version;
      }
      return ret;
    }

    void ObRootMajorFreezeDuty::runTimerTask(void)
    {
      if (to_be_frozen_version_ > worker_->get_root_server().get_new_frozen_version())
      {
        //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160318:b
        worker_->get_root_server().renew_cur_version_offline_set();
        worker_->get_root_server().refresh_all_cluster_stat_info(to_be_frozen_version_ + 1);
        //add 20160318:e
        worker_->get_root_server().set_new_frozen_version(to_be_frozen_version_, true);
      }
    }
    //add hongchen [CLUSTER_STAT_BUGFIX] 20170821:b
    int ObRootClusterStatDuty::init(ObRootWorker *worker, common::ObTimer * timer,const int64_t to_be_frozen_version)
    {
      int ret = OB_SUCCESS;
      if (NULL == worker || NULL == timer)
      {
        YYSYS_LOG(WARN, "invalid argument. worker=NULL or timer=NULL");
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        worker_ = worker;
        timer_  = timer;
        to_be_frozen_version_ = to_be_frozen_version;
      }
      return ret;
    }

    void ObRootClusterStatDuty::runTimerTask(void)
    {
      //if (to_be_frozen_version_ > worker_->get_root_server().get_new_frozen_version())
      if (to_be_frozen_version_ > worker_->get_root_server().get_new_frozen_version() || !worker_->get_root_server().get_cluster_sys_stat())
      {
        worker_->get_root_server().renew_cur_version_offline_set();
        worker_->get_root_server().refresh_all_cluster_stat_info(to_be_frozen_version_ + 1);
        if (OB_SUCCESS != (timer_->schedule(*this, 1 * 1000 * 1000, false)))
        {
          YYSYS_LOG(WARN,"may be fail to push cluster stat duty");
        }
      }
      //[492]
      else if(to_be_frozen_version_ > worker_->get_root_server().get_last_frozen_version())
      {
          if(OB_SUCCESS != (timer_->schedule(*this, 1 * 1000 * 1000, false)))
          {
              YYSYS_LOG(WARN, "may be fail to push cluster stat duty");
          }
      }
      else if(to_be_frozen_version_ == worker_->get_root_server().get_last_frozen_version())
      {
          if(OB_SUCCESS != (worker_->get_root_server().refresh_sys_cur_data_version_info()))
          {
              if(OB_SUCCESS != (timer_->schedule(*this, 1 * 1000 * 1000, false)))
              {
                  YYSYS_LOG(WARN, "may be fail to push cluster stat duty");
              }
          }
      }
    }
    //add hongchen [CLUSTER_STAT_BUGFIX] 20170821:e
    int ObRootPartitionDuty::init(ObRootWorker *worker, const int64_t last_mem_frozen_version, const bool partition_changed_flag)
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
        last_mem_frozen_version_  = last_mem_frozen_version;
        partition_changed_flag_ = partition_changed_flag;
      }
      return ret;
    }

    void ObRootPartitionDuty::runTimerTask(void)
    {
      if (!partition_changed_flag_)
      {
        worker_->get_root_server().notify_partition_unchanged_version(last_mem_frozen_version_);
      }
      else
      {
        YYSYS_LOG(DEBUG, "partition rule changed in version %ld", last_mem_frozen_version_);
      }
      
    }
  }
}

