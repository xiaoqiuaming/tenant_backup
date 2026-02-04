#ifndef OCEANBASE_SQL_OB_OPTIMIZER_RELATION_H_
#define OCEANBASE_SQL_OB_OPTIMIZER_RELATION_H_

#include "common/ob_define.h"
#include "common/ob_list.h"
#include "ob_raw_expr.h"
#include "common/hash/ob_hashmap.h"
#include "common/utility.h"
#include "common/ob_vector.h"
#include "common/ob_array.h"
#include <stdint.h>
#include "mergeserver/ob_merge_server_service.h"
#include "mergeserver/ob_statistic_info_cache.h"

namespace oceanbase
{
  namespace sql
  {
    typedef struct ObSelInfo
    {
        uint64_t table_id_;
        uint64_t columun_id_;
        double selectivity_;
        bool enable;
        bool enable_expr_subquery_optimization;

        ObSelInfo()
        {
          table_id_ = OB_INVALID_ID;
          columun_id_ = OB_INVALID_ID;
          selectivity_ = 1.0;
          enable = true;
          enable_expr_subquery_optimization = false;
        }
    }ObSelInfo;

    typedef struct ObIndexTableInfo
    {
        uint64_t index_table_id_;
        uint64_t index_column_id_;
        bool is_back_;
        double cost_;
        bool group_by_applyed_;
        bool order_by_applyed_;

        ObIndexTableInfo():
          index_table_id_(OB_INVALID_ID),
          index_column_id_(OB_INVALID_ID),
          is_back_(true),
          cost_(0),
          group_by_applyed_(false),
          order_by_applyed_(false)
        {}
    }ObIndexTableInfo;

    typedef struct ObSeqScanInfo
    {
        bool group_by_applyed_;
        bool order_by_applyed_;
        double cost_;

        ObSeqScanInfo():
          group_by_applyed_(false),
          order_by_applyed_(false),
          cost_(0)
        {}

    }ObSeqScanInfo;

  }

  namespace common
  {
    template <>
    struct ob_vector_traits<oceanbase::sql::ObSelInfo>
    {
        typedef oceanbase::sql::ObSelInfo* pointee_type;
        typedef oceanbase::sql::ObSelInfo value_type;
        typedef const oceanbase::sql::ObSelInfo const_value_type;
        typedef value_type* iterator;
        typedef const value_type* const_iterator;
        typedef int32_t difference_type;
    };
    template<>
    struct ob_vector_traits<oceanbase::sql::ObIndexTableInfo>
    {
        typedef oceanbase::sql::ObIndexTableInfo* pointee_type;
        typedef oceanbase::sql::ObIndexTableInfo value_type;
        typedef const oceanbase::sql::ObIndexTableInfo const_value_type;
        typedef value_type* iterator;
        typedef const value_type* const_iterator;
        typedef int32_t difference_type;
    };

  }
  namespace sql
  {
    typedef double Selectivity;
    typedef double Cost;
#define DEFAULT_EQ_SEL 0.005
#define DEFAULT_INEQ_SEL 0.3333333333333333
#define DEFAULT_RANGE_INEQ_SEL 0.005
#define DEFAULT_MATCH_SEL 0.005
#define DEFAULT_NUM_DISTINCT 200
#define DEFAULT_UNK_SEL 0.005
#define DEFAULT_NOT_UNK_SEL (1.0 - DEFAULT_UNK_SEL)

#define DEFAULT_SEQ_PAGE_COST 1.0
#define DEFAULT_RANDOM_PAGE_COST 4.0
#define DEFAULT_CPU_TUPLE_COST 0.01
#define DEFAULT_CPU_INDEX_TUPLE_COST 0.005
#define DEFAULT_CPU_OPERATOR_COST 0.0025
#define DEFAULT_GROUP_BY_OPERATOR_COST 0.0025
#define DEFAULT_ORDER_BY_OPERATOR_COST 0.0025

#define DEFAULT_TABLE_TUPLES 20000
#define DEFAULT_START_COST 10.0
#define DEFAULT_CPU_DISK_PROPORTION 0.03

#define LOG2(x) (log(x) / 0.693147180559945)
#define CLAMP_PROBABILITY(p) \
  do { \
  if(p<0.0) \
  p=0.0; \
  else if(p>1.0) \
  p = 1.0; \
  }while(0)

    static const uint64_t OPT_TABLE_MAP_SIZE = 100;
    static const uint64_t STAT_INFO_TABLE_MAP_SIZE =100;
    static const uint64_t STAT_INFO_COL_MAP_SIZE = 200;
    static const uint64_t STAT_INFO_FREQ_MAP_SIZE = 10;
    static const double WIDTH_ARRY[] = {1.0,4.0,4.0,
                                        8.0,4.0,8.0,
                                        4.0,4.0,4.0,
                                        4.0,4.0,1.0,
                                        4.0,4.0,4.0,
                                        4.0,4.0
                                       };
    typedef struct ObColumnStatInfo
    {
        uint64_t column_id_;
        double avg_width_;
        bool unique_rowkey_column_;
        double distinct_num_;
        oceanbase::common::ObExprObj min_value_;
        oceanbase::common::ObExprObj max_value_;
        double avg_frequency_;
        ObObj obj[10];
        common::hash::ObHashMap<oceanbase::common::ObObj,double,common::hash::NoPthreadDefendMode> value_frequency_map_;
        ObColumnStatInfo()
        {
          if(!value_frequency_map_.created())
          {
            value_frequency_map_.create(STAT_INFO_FREQ_MAP_SIZE);
          }
        }
        ~ObColumnStatInfo()
        {
          if(value_frequency_map_.created())
          {
            value_frequency_map_.destroy();
          }
        }
    }ObColumnStatInfo;

    typedef struct ObBaseRelStatInfo
    {
        uint64_t table_id_;
        bool enable_statinfo;
        double tuples_;
        double size_;
        double column_num_;
        double avg_width_;
        uint64_t statistic_columns_[OB_MAX_COLUMN_NUMBER];
        int64_t statistic_columns_num_;
        bool empty_table_;
        common::hash::ObHashMap<uint64_t,ObColumnStatInfo*,common::hash::NoPthreadDefendMode> column_id_value_map_;
        ObBaseRelStatInfo()
        {
          enable_statinfo = true;
          if(!column_id_value_map_.created())
          {
            column_id_value_map_.create(STAT_INFO_COL_MAP_SIZE);
          }
        }
        ~ObBaseRelStatInfo()
        {
          if(column_id_value_map_.created())
          {
            for(common::hash::ObHashMap<uint64_t,ObColumnStatInfo*,common::hash::NoPthreadDefendMode>::iterator iter = column_id_value_map_.begin();iter!= column_id_value_map_.end();iter++)
            {
              if(iter->second)
              {
                ObColumnStatInfo* col_stat_info = (ObColumnStatInfo*)(iter->second);
                col_stat_info->~ObColumnStatInfo();
                iter->second = NULL;
              }
            }
            column_id_value_map_.destroy();
          }
        }
    }ObBaseRelStatInfo;

    class ObOptimizerRelation;
    class ObStatSelCalculator
    {
      public:
        static bool is_unique_rowkey_column(ObOptimizerRelation *rel_opt,
                                            const uint64_t table_id,
                                            const uint64_t column_id);
        static double get_equal_selectivity(ObOptimizerRelation *rel_opt,
                                            ObSelInfo &sel_info,
                                            const oceanbase::common::ObObj &value);
        static double get_equal_subquery_selectivity(ObOptimizerRelation *rel_opt,
                                                     ObSelInfo &sel_info);

        static double get_lt_selectivity(ObOptimizerRelation *rel_opt,
                                         ObSelInfo &sel_info,
                                         const oceanbase::common::ObObj &value);

        static double get_le_selectivity(ObOptimizerRelation *rel_opt,
                                         ObSelInfo &sel_info,
                                         const oceanbase::common::ObObj &value);
        static double get_btw_selectivity(ObOptimizerRelation *rel_opt,
                                          ObSelInfo &sel_info,
                                          const oceanbase::common::ObObj &value1,
                                          const oceanbase::common::ObObj &value2);
        static double get_like_selectivity(ObOptimizerRelation *rel_opt,
                                           ObSelInfo &sel_info,
                                           const oceanbase::common::ObObj &value);
        static double get_in_selectivity(ObOptimizerRelation *rel_opt,
                                         ObSelInfo &sel_info,
                                         const common::ObArray<ObObj> &value_array);
    };
    class ObJoinStatCalculator
    {
      public:
        static double calc_joinrel_size_estimate(
            ObLogicalPlan *logical_plan,
            ObSelectStmt *select_stmt,
            const uint64_t join_type,
            const double outer_rows,
            const double inner_rows,
            oceanbase::common::ObList<ObSqlRawExpr*>& restrictlist);
        static Selectivity clauselist_selectivity(
            ObLogicalPlan *logical_plan,
            ObSelectStmt *select_stmt,
            const uint64_t join_type,
            oceanbase::common::ObList<ObSqlRawExpr*>& restrictlist);
        static double clamp_row_est(double nrows);
        static double rint(double x);
    };

    class ObStatCostCalculator
    {
      private:
        static int get_sort_cost(double tuples, Cost &cost);
        static int get_group_by_cost(ObSelectStmt *select_stmt,
                                     ObOptimizerRelation *rel_opt,
                                     ObIndexTableInfo *index_table_info,
                                     Cost &cost);

        static int get_order_by_cost(ObSelectStmt *select_stmt,
                                     ObOptimizerRelation *rel_opt,
                                     ObIndexTableInfo *index_table_info,
                                     Cost &cost);

      public:
        static int get_cost_seq_scan(ObSelectStmt *select_stmt,
                                     ObOptimizerRelation *rel_opt,
                                     Cost &cost);
        static int get_cost_index_scan(ObSelectStmt *select_stmt,
                                       ObOptimizerRelation *rel_opt,
                                       ObIndexTableInfo &index_table_info,
                                       const double sel);

    };
    class ObStatExtractor
    {
      public:
        int fill_col_statinfo_map(common::hash::ObHashMap<uint64_t,ObBaseRelStatInfo*,common::hash::NoPthreadDefendMode>* table_id_statInfo_map,
                                  ObOptimizerRelation *rel_opt,
                                  uint64_t column_id,
                                  bool unique_column_rowkey = false);
        int fill_table_statinfo_map(common::hash::ObHashMap<uint64_t,ObBaseRelStatInfo*,common::hash::NoPthreadDefendMode>* table_id_statInfo_map,
                                    ObOptimizerRelation *rel_opt);

        inline mergeserver::ObStatisticInfoCache * get_statistic_info_cache()
        {
          return statistic_info_cache_;
        }

        inline void set_statistic_info_cache(mergeserver::ObStatisticInfoCache * cache)
        {
          statistic_info_cache_ = cache;
        }
      private:
        mergeserver::ObStatisticInfoCache *statistic_info_cache_;
    };

    typedef struct ObOptimizerFromItemHelper
    {
        double nrows;
        ObOptimizerRelation* left_base_rel_opt;
        ObOptimizerRelation* right_base_rel_opt;
        ObOptimizerRelation* left_rel_opt;
        ObOptimizerRelation* right_rel_opt;
        ObSqlRawExpr *cnd_id_expr;
        double left_join_rows;
        double right_join_rows;
        double diff_num_left;
        double diff_num_right;
        int cnd_it_id;

        double data_skew_left;
        double data_skew_right;
        double high_freq_dup_left;
        double high_freq_dup_right;

        ObOptimizerFromItemHelper()
        {
          nrows =0;
          left_join_rows =0;
          right_join_rows =0;
          diff_num_left =0;
          diff_num_right =0;
          cnd_it_id = -1;
          left_base_rel_opt = NULL;
          right_base_rel_opt = NULL;
          left_rel_opt = NULL;
          right_rel_opt = NULL;
          data_skew_left = 1.0;
          data_skew_right = 1.0;
          high_freq_dup_left = 1.0;
          high_freq_dup_right = 1.0;
        }
        bool operator < (const ObOptimizerFromItemHelper &r) const
        {
          bool ret = false;
          double left_min_diff_num = fmin(diff_num_left,diff_num_right);
          double right_min_diff_num = fmin(r.diff_num_left,r.diff_num_right);
          if(left_min_diff_num <10
             && right_min_diff_num >100
             && nrows <5*r.nrows)
          {
            ret = true;
          }
          else if(left_min_diff_num >100
                  && right_min_diff_num<10
                  && r.nrows <5*nrows)
          {
            ret = false;
          }
          else
          {
            ret = nrows < r.nrows;
          }
          return ret;
        }
    }ObOptimizerFromItemHelper;

    typedef struct ObFromItemJoinMethodHelper
    {
        bool exchange_order;
        ObSqlRawExpr *where_sql_expr;
        int join_method;
        bool previous_join;
        ObFromItemJoinMethodHelper()
        {
          exchange_order = false;
          where_sql_expr = NULL;
          join_method = JoinedTable::MERGE_JOIN;
          previous_join = false;
        }
    }ObFromItemJoinMethodHelper;

    class ObOPtimizerLoger
    {
      public:
        static const bool log_switch_ =false;
        static const FILE* getFile()
        {
          FILE *file = fopen("query_optimizer.log","a");
          if(NULL == file)
          {
            YYSYS_LOG(ERROR,"dhc check fopen output file failed");
          }
          return file;
        }
        static const void closeFile(const FILE* file)
        {
          FILE *fp = const_cast<FILE*>(file);
          if(NULL == fp)
          {
            YYSYS_LOG(ERROR,"dhc fclose failed");
          }
          else
          {
            fclose(fp);
          }
        }
        static const void resetFile()
        {
          FILE *file = fopen("query_optimizer.log","w");
          if(NULL == file)
          {
            YYSYS_LOG(ERROR,"dhc check fopen output file failed");
          }
          else
          {
            fclose(file);
          }
        }

        static void print(const char * log_string)
        {
          if(!log_switch_)
          {}
          else
          {
            FILE *fp = NULL;
            fp = fopen("query_optimizer.log","a");
            if(fp == NULL)
            {
              YYSYS_LOG(WARN, "Can't open 'query_optimizer.log' file!");
            }
            else if(log_string !=NULL)
            {
              fprintf(fp,"%s\n",log_string);
            }
            if(fp != NULL)
            {
              fclose(fp);
            }
          }
        }

    };
    class ObOptimizerRelation
    {
      public:
        enum RelOptKind
        {
          RELOPT_BASEREL,
          RELOPT_JOINREL,
          RELOPT_SUBQUERY,
          RELOPT_SELECT_FOR_UPDATE,
          RELOPT_MAIN_QUERY,
          RELOPT_INIT
        };

        ObOptimizerRelation()
          :rel_opt_kind_(RELOPT_BASEREL)
        {
          schema_managerv2_ = NULL;
          stat_extractor_ = NULL;
          table_id_statInfo_map_ = NULL;
          allocator_ = NULL;
          rows_ = 1.0;
          tuples_ = 1.0;
          join_rows_ = 1.0;
          table_id_ = OB_INVALID_ID;
          table_ref_id_ = OB_INVALID_ID;
          group_by_num_ = 0;
          order_by_num_ = 0;
        }
        explicit ObOptimizerRelation(const RelOptKind rel_opt_kind)
          :rel_opt_kind_(rel_opt_kind)
        {
          schema_managerv2_ = NULL;
          stat_extractor_ = NULL;
          table_id_statInfo_map_ = NULL;
          allocator_ = NULL;
          group_by_num_ = 0;
          order_by_num_ = 0;
        }
        explicit ObOptimizerRelation(const RelOptKind rel_opt_kind,uint64_t table_id)
          :rel_opt_kind_(rel_opt_kind),table_id_(table_id)
        {
          schema_managerv2_ = NULL;
          stat_extractor_ = NULL;
          table_id_statInfo_map_ = NULL;
          allocator_ = NULL;
          group_by_num_ = 0;
          order_by_num_ = 0;
        }
        virtual ~ObOptimizerRelation()
        {
          clear();
        }
        void clear()
        {
          index_table_array_.clear();
          sel_info_array_.clear();
          base_cnd_list_.clear();
          needed_columns_.clear();
          join_cnd_list_.clear();
        }
        void set_rel_opt_kind(const RelOptKind rel_opt_kind);
        void set_table_id(const uint64_t table_id);
        inline void set_table_ref_id(const uint64_t table_ref_id){table_ref_id_ = table_ref_id;}
        void set_expr_id(const uint64_t expr_id);
        RelOptKind get_rel_opt_kind() const;
        uint64_t get_table_id() const;
        inline uint64_t get_table_ref_id() const {return table_ref_id_;}
        uint64_t get_expr_id() const;
        inline double get_rows() const {return rows_;}
        inline void set_rows(double rows) {rows_ = rows >1.0?rows:1.0;}
        inline double get_tuples() const {return tuples_;}
        inline void set_tuples(double tuples) { tuples_ = tuples?tuples:1.0;}
        inline double get_join_rows() const {return join_rows_;}
        inline void set_join_rows(double join_rows) {join_rows_ = join_rows >1.0?join_rows:1.0;}
        inline double get_width() const {return width_;}
        inline void set_width(double width) {width_ = width;}

        inline int32_t get_group_by_num() {return group_by_num_;}
        inline void set_group_by_num(int32_t group_by_num) {group_by_num_ = group_by_num;}
        inline int32_t get_order_by_num() {return order_by_num_;}
        inline void set_order_by_num(int32_t order_by_num) {order_by_num_ = order_by_num;}

        inline Cost get_seq_scan_cost() const {return seq_scan_info_.cost_;}
        inline void set_seq_scan_cost(Cost seq_scan_cost) {seq_scan_info_.cost_ = seq_scan_cost;}

        inline ObSeqScanInfo get_seq_scan_info() {return seq_scan_info_;}
        inline void set_seq_scan_info(ObSeqScanInfo &seq_scan_info) {seq_scan_info_ = seq_scan_info;}

        inline common::hash::ObHashMap<uint64_t,ObBaseRelStatInfo*,common::hash::NoPthreadDefendMode>* get_table_id_statInfo_map()
        {
          return table_id_statInfo_map_;
        }
        inline void set_table_id_statInfo_map(common::hash::ObHashMap<uint64_t,ObBaseRelStatInfo*,common::hash::NoPthreadDefendMode>*map)
        {
          table_id_statInfo_map_ = map;
        }
        inline ObStatExtractor* get_stat_extractor()
        {
          return stat_extractor_;
        }
        inline void set_stat_extractor(ObStatExtractor* stat_extractor)
        {
          stat_extractor_ = stat_extractor;
        }
        inline void set_name_pool(common::ObStringBuf* allocator)
        {
          allocator_ = allocator;
        }
        inline common::ObStringBuf* get_name_pool()
        {
          return allocator_;
        }

        inline common::ObVector<ObSqlRawExpr*> &get_base_cnd_list();
        inline common::ObVector<ObSqlRawExpr*> &get_join_cnd_list();
        inline common::ObVector<uint64_t> &get_needed_columns();

        inline const common::ObSchemaManagerV2 *get_schema_managerv2()
        {
          return schema_managerv2_;
        }
        inline void set_schema_managerv2(const common::ObSchemaManagerV2 *sm)
        {
          schema_managerv2_ = sm;
        }
        int get_column_stat_info(uint64_t table_id,uint64_t column_id,ObColumnStatInfo* &col_stat_info);
        int get_base_rel_stat_info(uint64_t table_id,ObBaseRelStatInfo* &rel_stat_info);
        inline common::ObVector<ObIndexTableInfo> &get_index_table_array();
        inline common::ObVector<ObSelInfo> &get_sel_info_array();
        void reset_semi_join_right_index_table_cost(uint64_t column_id,double sel =0.0);
        inline int copy_obj(ObObj &src,ObObj &dest);
        void print_rel_opt_info();
        void print_rel_opt_info(const FILE *file);
        void print_rel_opt_info_V2();

      protected:
        void print_indentation(FILE *fp,int32_t level)const;

      private:
        RelOptKind rel_opt_kind_;
        uint64_t table_id_;
        uint64_t table_ref_id_;
        double rows_;
        double tuples_;
        double join_rows_;
        double width_;
        int32_t group_by_num_;
        int32_t order_by_num_;
        ObSeqScanInfo seq_scan_info_;
        common::ObVector<ObIndexTableInfo> index_table_array_;
        common::ObVector<ObSelInfo> sel_info_array_;

        common::ObVector<ObSqlRawExpr*> base_cnd_list_;
        common::ObVector<ObSqlRawExpr*> join_cnd_list_;
        common::ObVector<uint64_t> needed_columns_;
        const common::ObSchemaManagerV2 * schema_managerv2_;

        common::hash::ObHashMap<uint64_t,ObBaseRelStatInfo*,common::hash::NoPthreadDefendMode>* table_id_statInfo_map_;
        ObStatExtractor * stat_extractor_;
        common::ObStringBuf* allocator_;

    };
    inline void ObOptimizerRelation::set_rel_opt_kind(RelOptKind rel_opt_kind)
    {
      rel_opt_kind_ = rel_opt_kind;
    }
    inline ObOptimizerRelation::RelOptKind ObOptimizerRelation::get_rel_opt_kind() const
    {
      return rel_opt_kind_;
    }

    inline uint64_t ObOptimizerRelation::get_table_id() const
    {
      return table_id_;
    }
    inline void ObOptimizerRelation::set_table_id(const uint64_t table_id)
    {
      table_id_ = table_id;
    }
    inline void ObOptimizerRelation::print_indentation(FILE* fp,int32_t level) const
    {
      for(int i=0;i<level;++i)
        fprintf(fp,"    ");
    }
    inline common::ObVector<ObSqlRawExpr*> & ObOptimizerRelation::get_base_cnd_list()
    {
      return base_cnd_list_;
    }
    inline common::ObVector<ObSqlRawExpr*> & ObOptimizerRelation::get_join_cnd_list()
    {
      return join_cnd_list_;
    }
    inline common::ObVector<uint64_t> & ObOptimizerRelation::get_needed_columns()
    {
      return needed_columns_;
    }
    inline common::ObVector<ObIndexTableInfo> & ObOptimizerRelation::get_index_table_array()
    {
      return index_table_array_;
    }

    inline common::ObVector<ObSelInfo> & ObOptimizerRelation::get_sel_info_array()
    {
      return sel_info_array_;
    }

    inline int ObOptimizerRelation::copy_obj(ObObj &src,ObObj &dest)
    {
      int ret = OB_SUCCESS;
      if(allocator_ == NULL)
      {
        YYSYS_LOG(WARN, "allocator_ is null! using shallow copy.");
        src.obj_copy(dest);
      }
      else if(OB_SUCCESS != (ret = ob_write_obj_v2(*allocator_,src,dest)))
      {
        YYSYS_LOG(WARN,"QX write object fail,ret =%d",ret);
      }
      return ret;
    }
  }
}
#endif
