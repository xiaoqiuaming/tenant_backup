#ifndef OB_STATISTIC_INFO_CACHE_H_
#define OB_STATISTIC_INFO_CACHE_H_

#include "common/ob_gather_table_info.h"
#include "common/hash/ob_hashutils.h"
#include "ob_ms_sql_proxy.h"
#include "ob_analysis_statistic_info.h"

#include "common/ob_array_helper.h"
#include "common/page_arena.h"
#include "common/ob_object.h"

namespace oceanbase
{
  namespace mergeserver
  {
    class ObAnalysisStatisticInfo;
    struct StatisticColumnKey
    {
        uint64_t table_id_;
        uint64_t column_id_;

        int64_t hash() const
        {
          common::hash::hash_func<uint64_t> hash_uint64;
          return hash_uint64(table_id_) + hash_uint64(column_id_);
        }

        bool operator==(const StatisticColumnKey& key) const
        {
          bool ret = false;
          if (table_id_ == key.table_id_ && column_id_ == key.column_id_)
          {
            ret = true;
          }
          return ret;
        }
    };
    struct StatisticColumnValue
    {
        common::ObTopValue top_value_[10];
        int64_t top_value_num_;
        common::ObObj min_;
        common::ObObj max_;
        int64_t row_count_;
        int64_t different_num_;
        int64_t size_;
        int64_t timestamp_;
        common::ModuleArena allocator_;

        StatisticColumnValue()
        {
          row_count_ = 0;
          different_num_ = 0;
          size_ = 0;
          timestamp_ = 0;
        }
        ~StatisticColumnValue()
        {
        }
        StatisticColumnValue(const StatisticColumnValue &other)
        {
          for(int64_t i=0;i<other.top_value_num_;i++)
          {
            top_value_[i].deep_copy(allocator_, other.top_value_[i]);
          }
          top_value_num_ = other.top_value_num_;
          ob_write_obj_v2(allocator_, other.min_, min_);
          ob_write_obj_v2(allocator_, other.max_, max_);
          row_count_ = other.row_count_;
          different_num_ = other.different_num_;
          size_ = other.size_;
          timestamp_ = other.timestamp_;
        }
        StatisticColumnValue& operator=(const StatisticColumnValue &other)
        {
          for(int64_t i=0;i<other.top_value_num_;i++)
          {
            top_value_[i].deep_copy(allocator_, other.top_value_[i]);
          }
          top_value_num_ = other.top_value_num_;
          ob_write_obj_v2(allocator_, other.min_, min_);
          ob_write_obj_v2(allocator_, other.max_, max_);
          row_count_ = other.row_count_;
          different_num_ = other.different_num_;
          size_ = other.size_;
          timestamp_ = other.timestamp_;
          return *this;
        }
    };
    struct StatisticTableValue
    {
        int64_t row_count_;
        int64_t size_;
        int64_t mean_row_size_;
        uint64_t statistic_columns_[OB_MAX_COLUMN_NUMBER];
        int64_t statistic_columns_num_;
        int64_t timestamp_;
        StatisticTableValue()
        {
          row_count_ = 0;
          size_ = 0;
          mean_row_size_ = 0;
          statistic_columns_num_ = 0;
          timestamp_ = 0;
        }
        ~StatisticTableValue()
        {

        }
        StatisticTableValue(const StatisticTableValue &other)
        {
          row_count_ = other.row_count_;
          size_ = other.size_;
          mean_row_size_ = other.mean_row_size_;
          for(int64_t i=0;i<other.statistic_columns_num_;i++)
          {
            statistic_columns_[i] = other.statistic_columns_[i];
          }
          statistic_columns_num_ = other.statistic_columns_num_;
          timestamp_ = other.timestamp_;
        }
        StatisticTableValue& operator=(const StatisticTableValue &other)
        {
          row_count_ = other.row_count_;
          size_ = other.size_;
          mean_row_size_ = other.mean_row_size_;
          for(int64_t i=0;i<other.statistic_columns_num_;i++)
          {
            statistic_columns_[i] = other.statistic_columns_[i];
          }
          statistic_columns_num_ = other.statistic_columns_num_;
          timestamp_ = other.timestamp_;
          return *this;
        }
    };
    class ObStatisticInfoCache
    {
      public:
        static const int32_t DEFAULT_VERSION = 1;

        static const int64_t KVCACHE_BLOCK_SIZE =
            sizeof(StatisticColumnKey) + sizeof(StatisticColumnValue) +
            sizeof(common::CacheHandle) +4 * 1024L;
        static const int64_t KVCACHE_ITEM_SIZE = KVCACHE_BLOCK_SIZE;
        typedef common::KeyValueCache<StatisticColumnKey,StatisticColumnValue, KVCACHE_ITEM_SIZE, KVCACHE_BLOCK_SIZE,
        common::KVStoreCacheComponent::MultiObjFreeList> KVCloumnCache;

        typedef common::KeyValueCache<uint64_t,StatisticTableValue, KVCACHE_ITEM_SIZE, KVCACHE_BLOCK_SIZE,
        common::KVStoreCacheComponent::MultiObjFreeList> KVTableCache;
        typedef common::CacheHandle Handle;
      public:
        ObStatisticInfoCache();
        ~ObStatisticInfoCache();

        int init(ObMsSQLProxy* sql_proxy, const int64_t max_cache_size, const int64_t timeout);
        int clear();
        int destroy();

        int put_analysised_column_sinfo_into_cache(uint64_t tid,uint64_t cid);
        int put_analysised_table_sinfo_into_cache(uint64_t tid);
        int add_column_statistic_info_into_cache(uint64_t tid,uint64_t cid, StatisticColumnValue &scv);
        int get_column_statistic_info_from_cache(uint64_t tid,uint64_t cid, StatisticColumnValue &scv);
        int add_table_statistic_info_into_cache(uint64_t tid, StatisticTableValue &stv);
        int get_table_statistic_info_from_cache(uint64_t tid, StatisticTableValue &stv);

        void enlarge_total_size(int64_t total_size)
        {
          kvtablecache_.enlarge_total_size(total_size);
          kvcolumncache_.enlarge_total_size(total_size);
        }

        int renew_column_statistic_asy(uint64_t tid,uint64_t cid);
        int renew_table_statistic_asy(uint64_t tid);

        inline void set_cache_timeout(const int64_t timeout)
        {
          cache_timeout_ = timeout;
        }
        inline int64_t get_cache_timeout() const
        {
          return cache_timeout_;
        }
      private:
        bool inited_;
        KVCloumnCache kvcolumncache_;
        KVTableCache kvtablecache_;
        ObMsSQLProxy* sql_proxy_;

        int64_t cache_timeout_;
    };
  }

  namespace common
  {
    namespace KVStoreCacheComponent
    {
      struct StatisticColumnKeyDeepCopyTag {};

      template <>
      struct traits<mergeserver::StatisticColumnKey *>
      {
          typedef StatisticColumnKeyDeepCopyTag Tag;
      };

      inline mergeserver::StatisticColumnKey *do_copy(const mergeserver::StatisticColumnKey &other,
                                                      char *buffer, StatisticColumnKeyDeepCopyTag)
      {
        mergeserver::StatisticColumnKey *ret = (mergeserver::StatisticColumnKey*)buffer;
        if (NULL!= ret)
        {
          ret->table_id_ = other.table_id_;
          ret->column_id_ = other.column_id_;
        }
        return ret;
      }

      inline int32_t do_size(mergeserver::StatisticColumnKey* const &data,
                             StatisticColumnKeyDeepCopyTag)
      {
        return static_cast<int32_t>(sizeof(data) + sizeof(mergeserver::StatisticColumnKey));
      }

      inline void do_destroy(mergeserver::StatisticColumnKey **data,
                             StatisticColumnKeyDeepCopyTag)
      {
        using namespace mergeserver;
        (*data)->~StatisticColumnKey();
      }

      struct StatisticColumnValueDeepCopyTag {};

      template <>
      struct traits<mergeserver::StatisticColumnValue *>
      {
          typedef StatisticColumnValueDeepCopyTag Tag;
      };

      inline mergeserver::StatisticColumnValue **do_copy(mergeserver::StatisticColumnValue * const &other,
                                                         char *buffer, StatisticColumnValueDeepCopyTag)
      {
        mergeserver::StatisticColumnValue **ret = (mergeserver::StatisticColumnValue**)buffer;
        if (NULL!=ret)
        {
          *ret = new(buffer + sizeof(mergeserver::StatisticColumnValue*)) mergeserver::StatisticColumnValue(*other);
        }
        return ret;
      }

      inline int32_t do_size(mergeserver::StatisticColumnValue* const &data,
                             StatisticColumnValueDeepCopyTag)
      {
        return static_cast<int32_t>(sizeof(data) + sizeof(mergeserver::StatisticColumnValue));
      }

      inline void do_destroy(mergeserver::StatisticColumnValue **data,
                             StatisticColumnValueDeepCopyTag)
      {
        using namespace mergeserver;
        (*data)->~StatisticColumnValue();
      }

      struct StatisticTableValueDeepCopyTag {};

      template <>
      struct traits<mergeserver::StatisticTableValue *>
      {
          typedef StatisticTableValueDeepCopyTag Tag;
      };

      inline mergeserver::StatisticTableValue **do_copy(mergeserver::StatisticTableValue * const &other,
                                                        char *buffer, StatisticTableValueDeepCopyTag)
      {
        mergeserver::StatisticTableValue **ret = (mergeserver::StatisticTableValue**)buffer;
        if (NULL!=ret)
        {
          *ret = new(buffer + sizeof(mergeserver::StatisticTableValue*)) mergeserver::StatisticTableValue(*other);
        }
        return ret;
      }

      inline int32_t do_size(mergeserver::StatisticTableValue* const &data,
                             StatisticTableValueDeepCopyTag)
      {
        return static_cast<int32_t>(sizeof(data) + sizeof(mergeserver::StatisticTableValue));
      }

      inline void do_destroy(mergeserver::StatisticTableValue **data,
                             StatisticTableValueDeepCopyTag)
      {
        using namespace mergeserver;
        (*data)->~StatisticTableValue();
      }
    }
  }
}

#endif // OB_STATISTIC_INFO_CACHE_H_
