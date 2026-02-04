/*
*   Version: 0.1
*
*   Authors:
*      liu jun
*        - some work details if you want
*/

#include "ob_partition_monitor.h"

namespace oceanbase
{
  namespace common
  {
    ObPartitionMonitor::ObPartitionMonitor():latest_pos_(65535)
    {
    }
    ObPartitionMonitor::~ObPartitionMonitor()
    {
      //mod wuna [MultiUps] [sql_qpi] 20160118:b
      //for(int i = 0;i < MAX_VERSION_COUNT;i++)
      //{
      //  if(part_mgrs_[i].part_mgr_ != NULL)
      //  {
      //    delete part_mgrs_[i].part_mgr_;
      //    part_mgrs_[i].part_mgr_ = NULL;
      //  }
      //}
      yysys::CThreadGuard guard(&lock_);
      for(int i = 0;i < part_mgrs_.count();i++)
      {
        if(NULL != part_mgrs_.at(i))
        {
          if (NULL != part_mgrs_.at(i)->part_mgr_)
          {
            delete part_mgrs_.at(i)->part_mgr_;
            part_mgrs_.at(i)->part_mgr_ = NULL;
            //part_mgrs_.at(i)->~ManagerItem();
          }
          delete part_mgrs_.at(i);
          part_mgrs_.at(i) = NULL;
        }
      }
      part_mgrs_.clear();
      //mod 20160118:e
    }
    //add wuna [MultiUps] [sql_qpi] 20160310:b
    void ObPartitionMonitor::release_partition_manager_task()
    {
      yysys::CThreadGuard guard(&lock_);
      for(int64_t i = 0;i < part_mgrs_.count()-1;i++)
      {
        if(NULL != part_mgrs_.at(i))
        {
          if(NULL != part_mgrs_.at(i)->part_mgr_ && 0 == part_mgrs_.at(i)->ref_count_)
          {
           // part_mgrs.at(i)->~ManagerItem();
            delete part_mgrs_.at(i)->part_mgr_;
            part_mgrs_.at(i)->part_mgr_ = NULL;
            delete part_mgrs_.at(i);
            part_mgrs_.at(i) = NULL;
            part_mgrs_.remove(i);
            latest_pos_ = part_mgrs_.count()-1;
          }
        }
      }

    }
    //add 20160310:e

    int ObPartitionMonitor::add_partition_manager(ObPartitionManager *part_mgr)
    {
      int ret = OB_SUCCESS;
      if(NULL == part_mgr)
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(WARN, "check input param failed");
      }
      else
      {
        //mod wuna [MultiUps] [sql_qpi] 20160118:b
        //yysys::CThreadGuard guard(&lock_);
        //need to optimize
        //int32_t new_pos = 1- latest_pos_;
        //if(part_mgrs_[new_pos].ref_count_ == 0)
        //{
        //  latest_pos_ = new_pos;
        //  if(latest_pos_ >= 0 && latest_pos_ < MAX_VERSION_COUNT)
        //  {
        //    part_mgrs_[new_pos].part_mgr_ = part_mgr;
        // }
        //  else
        //  {
        //    ret = OB_ERROR;
        //    YYSYS_LOG(WARN, "latest position is illegal, latest_pos=%d", latest_pos_);
        //  }
        //}
        //else
        //{
        //  ret = OB_ERROR;
        //  YYSYS_LOG(ERROR, "partition manager has not release completely, ref_count=%ld", part_mgrs_[new_pos].ref_count_);
        //}
        ManagerItem* manager_item = NULL;
        manager_item = new(std::nothrow) ManagerItem();
        if (NULL == manager_item)
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
          manager_item->part_mgr_ = part_mgr;
          ret = part_mgrs_.push_back(manager_item);
          if(OB_SUCCESS == ret)
          {
            yysys::CThreadGuard guard(&lock_);
            latest_pos_ = part_mgrs_.count()-1;
          }
        }
        //mod 20160118:e
      }
      return ret;
    }

    ObArray<ObPartitionMonitor::ManagerItem*> ObPartitionMonitor::get_partition_manager_array()
    {
      return part_mgrs_;
    }

    ObPartitionManager *ObPartitionMonitor::get_partition_manager()
    {
      ObPartitionManager *part_mgr = NULL;
      yysys::CThreadGuard guard(&lock_);
      if(!check_inner_stat())
      {
        YYSYS_LOG(ERROR, "check inner stat failed");
      }
      else
      {
        //mod wuna [MultiUps] [sql_qpi] 20160118:b
        //part_mgrs_[latest_pos_].ref_count_++;
        //part_mgr = part_mgrs_[latest_pos_].part_mgr_;
//        YYSYS_LOG(ERROR, "TEST:reference_count=%ld", part_mgrs_[latest_pos_].ref_count_);
//        YYSYS_LOG(ERROR, "TEST:duplicate, reference_count=%ld", part_mgrs_[1-latest_pos_].ref_count_);
        part_mgrs_.at(latest_pos_)->ref_count_++;
        part_mgr = part_mgrs_.at(latest_pos_)->part_mgr_;
        //mod 20160118:e
      }
      return part_mgr;
    }

    int ObPartitionMonitor::release_manager(ObPartitionManager *part_mgr)
    {
      int ret = OB_ERROR;
      yysys::CThreadGuard guard(&lock_);
      //find the part_mgr.if exists, ret=success,otherwise return err.
      //mod wuna [MultiUps] [sql_qpi] 20160118:b
      //for(int32_t i = 0;i < MAX_VERSION_COUNT;i++)
      //{
      //  if(part_mgrs_[i].part_mgr_ == part_mgr)
      //  {
      //    part_mgrs_[i].ref_count_--;
      //    ret = OB_SUCCESS;
      //      YYSYS_LOG(ERROR, "TEST:release reference_count=%ld", part_mgrs_[i].ref_count_);
      //    break;
      //  }
      //}
      for(int32_t i = 0;i < part_mgrs_.count();i++)
      {
        if(part_mgrs_.at(i)->part_mgr_ == part_mgr)
        {
          part_mgrs_.at(i)->ref_count_--;
          ret = OB_SUCCESS;
          break;
        }
      }
      //mod 20160118:e
      //del wuna [MultiUps] [sql_qpi] 20160118:b
      //deconstruct the unuseful partition manager
      //for(int i = 0;i < MAX_VERSION_COUNT;i++)
      //{
      //  if(i != latest_pos_ && 0 == part_mgrs_[i].ref_count_ && NULL != part_mgrs_[i].part_mgr_)
      //  {
      //    delete part_mgrs_[i].part_mgr_;
      //    part_mgrs_[i].part_mgr_ = NULL;
      //  }
      //}
      //del 20160118:e
      return ret;
    }
  }
}
