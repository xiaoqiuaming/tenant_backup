#ifndef OB_CHUNK_STATISTICS_COLLECTOR_H
#define OB_CHUNK_STATISTICS_COLLECTOR_H


#include <yysys.h>
#include <Mutex.h>
#include <Monitor.h>


#include "common/ob_define.h"
#include "common/ob_schema.h"
#include "common/ob_vector.h"
#include "common/thread_buffer.h"
#include "common/ob_rowkey.h"
#include "common/ob_range2.h"
#include "common/ob_gather_table_info.h"
#include "common/ob_tablet_info.h"
#include "common/ob_array.h"
#include "common/location/ob_tablet_location_list.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace common;
    class ObTabletManager;
    class ObChunkServer;
    class ObTablet;
    class ObStatisticsBuilder;

    const static int64_t OB_MAX_COLLECT_COLUMN_NUM = 1 << 16;//65536

    class ObStatisticsCollector : public yysys::CDefaultRunnable
    {
      public:
        ObStatisticsCollector();
        ~ObStatisticsCollector(){}

        int init(ObTabletManager *manager);
        int schedule();
        void destroy();
        int set_config_param();
        int create_work_thread(const int64_t max_work_thread);
        bool can_launch_next_round();

        template <typename Allocator>
        int parse_location_from_scanner(ObScanner &scanner, ObRowkey& row_key, uint64_t table_id, uint64_t replication_idx,
                                        hash::ObHashMap<ObNewRange, ObTabletLocationList> &range_hash, Allocator& temp_allocator);
        int check_self();
        bool can_work_start() const;
        inline void set_collection_tag(bool tag)
        {
            is_start_gather_ = tag;
        }
        inline void set_merging_tag(bool tag)
        {
            is_start_merging_ = tag;
            YYSYS_LOG(DEBUG,"Start to set the tag! %d",is_start_merging_);
        }
        inline bool get_collection_tag()
        {
            return is_start_gather_;
        }

        inline void set_finished_tasks_num(int64_t num)
        {
            finished_tasks_num_ = num;
        }
        inline int64_t get_finished_tasks_num()
        {
            return finished_tasks_num_;
        }
        inline void set_total_tasks_num(int64_t num)
        {
            total_tasks_num_ = num;
        }
        inline int64_t get_total_tasks_num()
        {
            return total_tasks_num_;
        }


        inline common::ObArrayHelper<common::ObGatherTableInfo>* get_gather_list()
        {
            return &gather_list_;
        }

        inline int64_t get_collection_task_size()
        {
            return gather_list_.get_array_index();
        }
        inline int add_collection_task(ObGatherTableInfo gather_info)
        {
            int ret = OB_SUCCESS;
            if(OB_INVALID_ID != gather_info.table_id_)
            {
                if(!(gather_list_.push_back(gather_info)))
                {
                    ret = OB_ERROR;
                    YYSYS_LOG(WARN, "failed add collection task into list!");
                }
                gather_info.dump();
            }
            else
            {
                ret = OB_ERROR;
                YYSYS_LOG(WARN, "gather table_id is invalid! table_id is %ld", gather_info.table_id_);
            }
            return ret;
        }
        inline ObTabletManager* get_tablet_manager()
        {
            return tablet_manager_;
        }
        inline int64_t get_active_threads_num()
        {
            return active_thread_num_;
        }
     private:
        const static int64_t MAX_WORK_THREAD = 32;
     private:
        int create_all_gather_workers();
        int create_statistics_builders(ObStatisticsBuilder** builder, const int64_t size);
        int destroy_statistics_builders(ObStatisticsBuilder** builder, const int64_t size);
        virtual void run(yysys::CThread* thread, void *arg);
        void construct_statistics(const int64_t thread_no);
        int get_collection_task(ObGatherTableInfo* &gather_info, int &err);
        bool is_finish_all_taskes();
        int get_statistics_builder(const int64_t thread_no, ObStatisticsBuilder* &builder);
        int fetch_tablet_by_tid(ObVector<ObTablet *> &tablet_list, uint64_t table_id, uint64_t replication_idx);
        int is_tablet_need_gather_statistics(ObTablet* tablet, ObTabletLocationList &list, bool &is_primary_replication,
                                             hash::ObHashMap<ObNewRange, ObTabletLocationList> &range_hash);
        void construct_tablet_item(const uint64_t table_id,
                                   const common::ObRowkey &start_key, const common::ObRowkey &end_key, common::ObNewRange &range,
                                   ObTabletLocationList &list);
     private:
        volatile bool inited_;
        volatile bool is_start_gather_;
        volatile bool is_start_merging_;
        int64_t thread_num_;
        ObTabletManager *tablet_manager_;
        volatile int64_t active_thread_num_;
        int64_t min_work_thread_num_;
        pthread_cond_t cond_;
        pthread_mutex_t mutex_;
        pthread_mutex_t phase_mutex_;
        pthread_mutex_t tablet_range_mutex_;
        volatile int64_t local_work_start_time_;
        volatile int64_t local_work_complete_time_;
        ObStatisticsBuilder *builder_[MAX_WORK_THREAD];
        common::ObGatherTableInfo gather_info_[OB_MAX_COLLECT_COLUMN_NUM];
        common::ObArrayHelper<common::ObGatherTableInfo> gather_list_;

        int64_t finished_tasks_num_;
        int64_t total_tasks_num_;

    };

  }
}
#endif // OB_CHUNK_STATISTICS_COLLECTOR_H
