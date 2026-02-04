
#ifndef OB_STATISTICS_BUILDER_H
#define OB_STATISTICS_BUILDER_H


#include "common/ob_define.h"
#include "sstable/ob_sstable_writer.h"
#include "sstable/ob_sstable_reader.h"
#include "ob_get_scan_proxy.h"
#include "ob_get_cell_stream_wrapper.h"
#include "ob_cs_query_agent.h"
#include "ob_chunk_index_worker.h"
#include "common/ob_schema_manager.h"
#include "sql/ob_tablet_scan.h"
#include "ob_agent_scan.h"
#include "common/ob_tablet_info.h"
#include "ob_chunk_statistics_collector.h"
#include "ob_tablet_manager.h"
#include "sql/ob_sort.h"

namespace oceanbase
{
  namespace common
  {
    struct ObGatherTableInfo;
  }
  namespace chunkserver
  {
    class ObStatisticsCollector;
    class ObStatisticsBuilder
    {
      public:
        static const int64_t DEFAULT_ESTIMATE_ROW_COUNT = 256*1024LL;
        static const int64_t DEFAULT_MEMORY_LIMIT = 256*1024*1024LL;
      public:
        ObStatisticsBuilder(ObStatisticsCollector* worker, ObTabletManager* tablet_manager);
        ~ObStatisticsBuilder(){}


        int init();
        int start(ObTablet* tablet, ObGatherTableInfo gather_info);

        int gather_partition_statistics(ObNewRange range, uint64_t column_id, int32_t disk);
        int gather_partition_statistics_v2(ObTablet* tablet, uint64_t column_id, int32_t disk);

        void insert_top_value_v2(const common::ObTopValue &value);
        void insert_top_value(const common::ObTopValue &value);

        int update_tablet_statistics_meta(ObTablet* tablet, uint64_t column_id);
        int update_empty_tablet_statistics_meta(uint64_t table_id, uint64_t column_id);

      private:
        sql::ObSort sort_;
        ObStatisticsCollector* worker_;
        ObTabletManager* tablet_manager_;
        common::ModulePageAllocator mod_;
        common::ModuleArena allocator_;
        common::ObTopValue top_value_[10];
        common::ObArrayHelper<common::ObTopValue> top_value_list_;
        common::ObObj min_value_;
        common::ObObj max_value_;
        int64_t offset_;
        int64_t row_count_;
        int64_t different_num_;
        int64_t size_;
        int column_type_;

        ObMergerRpcProxy* rpc_proxy_;

    };
  }
}

#endif // OB_STATISTICS_BUILDER_H
