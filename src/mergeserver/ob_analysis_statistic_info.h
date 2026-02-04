/*
 * add by zhangebo[statistics info]20200630
 *
 */
#ifndef OB_ANALYSIS_STATISTIC_INFO_H_
#define OB_ANALYSIS_STATISTIC_INFO_H_

#include <stdint.h>
#include "common/ob_gather_table_info.h"
#include "common/hash/ob_hashutils.h"
#include "ob_ms_sql_proxy.h"
#include "common/ob_string.h"

using namespace oceanbase::common;
namespace oceanbase
{
  namespace mergeserver
  {
    struct StatisticTopValue
    {
        std::string top_value_;
        int64_t num_;
        StatisticTopValue()
        {
          num_ = 0;
        }
        ~StatisticTopValue()
        {
        }
    };
    struct Compare
    {
        bool operator () (StatisticTopValue stv1,StatisticTopValue stv2)
        {
          return stv1.num_ > stv2.num_;
        }
    };

    class ObAnalysisStatisticInfo
    {

      public:
        ObAnalysisStatisticInfo();
        ~ObAnalysisStatisticInfo();

        common::ObTopValue* get_top_value();
        int get_top_value_num();
        int64_t get_row_count();
        int64_t get_different_num();
        int64_t get_size();
        ObObj get_min();
        ObObj get_max();
        uint64_t* get_statistic_columns();
        int64_t get_statistic_columns_num();
        //add huangcc 20170629:b
        bool is_no_gather_statinfo()
        {
          return no_gather_statinfo_;
        }
        //add:e

        int init();
        void reset();
        int analysis_column_statistic_info(uint64_t tid,uint64_t cid,ObMsSQLProxy* sql_proxy);
        int analysis_table_statistic_info(uint64_t tid,ObMsSQLProxy* sql_proxy);
        void splite_string(std::string &info,std::vector<std::string> &vec);
        int merge_duplicates_and_sort();
        int trans_string_to_obj();
        int compute_min_max();

      private:
        common::ObTopValue top_value_[10];//存储整个表的高频值
        common::ObArrayHelper<common::ObTopValue> top_value_helper_;
        int top_value_num_;

        common::ObArray<StatisticTopValue> statistic_top_value_;//存储多个tablet所有的不同的高频值
        int64_t offset_;
        int32_t data_type_;
        int64_t row_count_;
        int64_t different_num_;
        int64_t size_;
        std::vector<std::string> top_value_vec_;//存储info分割后的多个字符串
        std::vector<std::string> min_max_vec_;//存储min_max分割后的多个字符串
        ObObj min_;
        ObObj max_;
        ObDecimal min_dec;
        ObDecimal max_dec;
        //add huangcc 20170629:b
        bool is_null_table_;
        bool no_gather_statinfo_;
        //add:e

        uint64_t statistic_columns_[OB_MAX_COLUMN_NUMBER];//存储多个tablet所有的不同的高频值
        common::ObArrayHelper<uint64_t> statistic_columns_helper_;
        int64_t statistic_columns_num_;
    };
    inline common::ObTopValue* ObAnalysisStatisticInfo::get_top_value()
    {
      return top_value_;
    }
    inline int ObAnalysisStatisticInfo::get_top_value_num()
    {
      return top_value_num_;
    }
    inline int64_t ObAnalysisStatisticInfo::get_row_count()
    {
      return row_count_;
    }
    inline int64_t ObAnalysisStatisticInfo::get_different_num()
    {
      return different_num_;
    }
    inline int64_t ObAnalysisStatisticInfo::get_size()
    {
      return size_;
    }
    inline ObObj ObAnalysisStatisticInfo::get_min()
    {
      return min_;
    }
    inline ObObj ObAnalysisStatisticInfo::get_max()
    {
      return max_;
    }
    inline uint64_t* ObAnalysisStatisticInfo::get_statistic_columns()
    {
      return statistic_columns_;
    }
    inline int64_t ObAnalysisStatisticInfo::get_statistic_columns_num()
    {
      return statistic_columns_num_;
    }
  }

}


#endif // OB_ANALYSIS_STATISTIC_INFO_H
