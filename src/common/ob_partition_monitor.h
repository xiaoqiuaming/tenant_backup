/*
*   Version: 0.1
*
*   Authors:
*      liu jun
*        - some work details if you want
*/

#ifndef OB_PARTITION_MONITOR_H
#define OB_PARTITION_MONITOR_H
#include "ob_partition_manager.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace common
  {
    class ObPartitionMonitor
    {
      public:
        typedef struct ManagerItem
        {
            int64_t ref_count_;
            ObPartitionManager *part_mgr_;
            ManagerItem():ref_count_(0),part_mgr_(NULL)
            {}
        }ManagerItem;
        static const int RETRY_TIMES = 3;
        static const int64_t RETRY_INTERVAL_TIME = 1000 * 1000;
        //static const int32_t MAX_VERSION_COUNT = 2;//del wuna [MultiUps] [sql_api] 20160118
        ObPartitionMonitor();
        virtual ~ObPartitionMonitor();
        void release_partition_manager_task();//add wuna [MultiUps] [sql_qpi] 20160310
        int add_partition_manager(ObPartitionManager *part_mgr);
        ObArray<ManagerItem*> get_partition_manager_array();
        ObPartitionManager *get_partition_manager();
        int release_manager(ObPartitionManager *part_mgr);
      private:
        bool check_inner_stat()
        {
          //mod wuna [MultiUps] [sql_api] 20160118:b
          //return NULL != part_mgrs_[latest_pos_].part_mgr_;
          return (part_mgrs_.count()>0 && NULL != part_mgrs_.at(latest_pos_)->part_mgr_);
          //mod 20160118:e
        }
      private:

        //mod wuna [MultiUps] [sql_api] 20160118:b
        //ManagerItem part_mgrs_[MAX_VERSION_COUNT];
        ObArray<ManagerItem*> part_mgrs_;
        //mod 20160118:e
        int64_t latest_pos_;
        mutable yysys::CThreadMutex lock_;
    };
  }
}
#endif // OB_PARTITION_MONITOR_H
