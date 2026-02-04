#ifndef OB_PHYSICAL_OPTIMIZER_H
#define OB_PHYSICAL_OPTIMIZER_H
#include "ob_transformer.h"
#include "ob_optimizer_relation.h"
#include "common/utility.h"
#include "ob_item_type.h"

namespace oceanbase
{

namespace sql
{

    using namespace oceanbase::common;
    static const uint64_t MAX_HASH_JOIN_ROWS = 128*1024*1024;
    static const double HASH_BUCKET_DEPTH_THRESHOLD = 10000.0;
    static const double MIN_HASH_JOIN_DIFF_NUM_THRESHOLD = 50.0;
    static const double SEMI_DIFF_NUM_THRESHOLD = 100.0;
    static const double SEMI_ROW_NUM_THRESHOLD = 200.0;

    static const double DATA_SKEW_THRESHOLD = 0.5;
    static const double DEFAULT_SKEW_THRESHOLD = 1.0;
    static const double DATA_SKEW_SERIOUS_THRESHOLD = 10;
    static const double HIGH_FREQ_NUM = 10;
    static const double DEFAULT_DIFF_VALUE_DUP = 1;
    static const double MIN_EASTIMATED_ROW = 1;
    static const double MIN_FAULT_TOLERANCE = 0.0001;
    typedef struct ObDiffValueInfo{
        double row_num;
        double total_diff_num;
        double high_freq_diff_num;
        double high_freq_dup_num;
        double low_freq_diff_num;
        double low_freq_dup_num;
        double data_skew;
        ObDiffValueInfo()
        {
            row_num = MIN_EASTIMATED_ROW;
            total_diff_num = MIN_EASTIMATED_ROW;
            high_freq_diff_num = MIN_EASTIMATED_ROW;
            high_freq_dup_num = DEFAULT_DIFF_VALUE_DUP;
            low_freq_diff_num = 0;
            low_freq_dup_num = 0;
            data_skew = DEFAULT_SKEW_THRESHOLD;
        }
        void set_default_eastimation(double nrows)
        {
            row_num = nrows;
            double diff_num = row_num / DEFAULT_DIFF_VALUE_DUP;
            if(row_num < MIN_EASTIMATED_ROW)
            {
                row_num = MIN_EASTIMATED_ROW;
                total_diff_num = MIN_EASTIMATED_ROW / DEFAULT_DIFF_VALUE_DUP;
                high_freq_diff_num = total_diff_num;
                high_freq_dup_num = DEFAULT_DIFF_VALUE_DUP;
            }
            else if((nrows / DEFAULT_DIFF_VALUE_DUP) <= HIGH_FREQ_NUM)
            {
                total_diff_num =diff_num;
                high_freq_diff_num = total_diff_num;
                high_freq_dup_num = DEFAULT_DIFF_VALUE_DUP;
            }
            else
            {
                total_diff_num = diff_num;
                high_freq_diff_num = HIGH_FREQ_NUM;
                high_freq_dup_num = DEFAULT_DIFF_VALUE_DUP;
                low_freq_diff_num = diff_num - HIGH_FREQ_NUM;
                low_freq_dup_num = DEFAULT_DIFF_VALUE_DUP;
            }
        }

        void to_string()
        {

        }

    }ObDiffValueInfo;
    typedef struct ObOptJoinParam{
        ObLogicalPlan *logical_plan;
        ObPhysicalPlan *physical_plan;
        ObSelectStmt *select_stmt;
        ObOptimizerRelation *left_rel_info;
        ObOptimizerRelation *right_rel_info;

        ObOptimizerRelation *left_base_rel_opt;
        ObOptimizerRelation *right_base_rel_opt;

        JoinedTable *joined_table;
        int32_t joined_table_id;
        bool right_base_rel_is_subquery;
        ObArray<int> join_method_array;
        ObBitSet<> left_table_bitset;
        ObOptJoinParam(){
            logical_plan = NULL;
            physical_plan = NULL;
            select_stmt = NULL;
            left_rel_info = NULL;
            right_rel_info= NULL;
            left_base_rel_opt= NULL;
            right_base_rel_opt= NULL;
            joined_table= NULL;
            joined_table_id=-1;
            right_base_rel_is_subquery= false;

        }
    }ObOptJoinParam;

    class ObPhysicalOptimizer
    {
    public:
        ObPhysicalOptimizer(ObTransformer *transformer);
        ~ObPhysicalOptimizer(){}
        template<class T>
        int get_stmt(
                ObLogicalPlan *logical_plan,
                ErrStat& err_stat,
                const uint64_t& query_id,
                T *& stmt);

        int transform_expr_const_object_type(ObLogicalPlan *logical_plan,
                                             ObSelectStmt *select_stmt,
                                             ObOptimizerRelation *rel_opt);
        bool is_having_equal_expr_in_join_column(ObOptimizerRelation *rel_opt,
                                                 const uint64_t table_id,
                                                 const uint64_t column_id);
        int gen_rel_opts(ObLogicalPlan *logical_plan,
                         ObPhysicalPlan *physical_plan,
                         ErrStat& err_stat,
                         const uint64_t& query_id);
        int init_rel_opt(ObLogicalPlan *logical_plan,
                         ObSelectStmt *select_stmt,
                         ObOptimizerRelation *rel_opt);
        int gen_rel_tuples(ObOptimizerRelation *rel_opt);
        int gen_bool_expr_divided_by_column_id(ObLogicalPlan *logical_plan,
                                               ObPhysicalPlan *physical_plan,
                                               ErrStat& err_stat,
                                               ObOptimizerRelation *rel_opt,
                                               const uint64_t column_id,
                                               ObRawExpr *expr,
                                               double &sel1,
                                               double &sel2,
                                               bool &enable_expr);
        int gen_clause_divided_by_column_id(ObLogicalPlan *logical_plan,
                                            ObPhysicalPlan *physical_plan,
                                            ErrStat &err_stat,
                                            ObOptimizerRelation *rel_opt,
                                            const uint64_t column_id,
                                            ObRawExpr *expr,
                                            double &sel1,
                                            double &sel2,
                                            bool & enable_expr,
                                            int idx = OB_INVALID_INDEX);

        int gen_clauselist_divided_by_column_id(ObLogicalPlan *logical_plan,
                                                ObPhysicalPlan *physical_plan,
                                                ErrStat &err_stat,
                                                ObOptimizerRelation *rel_opt,
                                                const uint64_t table_id,
                                                const uint64_t column_id,
                                                double &sel1,
                                                double &sel2);

        int gen_joined_column_diff_number(ObLogicalPlan *logical_plan,ObPhysicalPlan *physical_plan,ErrStat& err_stat,ObSelectStmt *select_stmt,
                                           ObOptimizerRelation *rel_opt,
                                          const uint64_t table_id,
                                          const uint64_t column_id,
                                          ObDiffValueInfo &diff_value_info);
        int gen_rel_size_estimitates(ObLogicalPlan *logical_plan,
                                     ObPhysicalPlan *physical_plan,
                                     ErrStat& err_stat,
                                      ObOptimizerRelation *rel_opt);
        int gen_expr_sub_query_optimization(ObLogicalPlan *logical_plan,
                                            ObPhysicalPlan *physical_plan,
                                            ErrStat& err_stat,
                                            ObRawExpr *expr);
        int gen_clauselist_selectivity(ObLogicalPlan *logical_plan,
                                       ObPhysicalPlan *physical_plan,
                                       ErrStat& err_stat,
                                        ObOptimizerRelation *rel_opt,
                                       double &sel);
        int gen_clause_selectivity(ObLogicalPlan *logical_plan,
                                   ObPhysicalPlan *physical_plan,
                                   ErrStat &err_stat,
                                   ObOptimizerRelation *rel_opt,
                                   ObSelInfo &sel_info,
                                   ObRawExpr *expr);
        int gen_const_selectivity(ObLogicalPlan *logical_plan,
                                  ObPhysicalPlan *physical_plan,
                                  ErrStat &err_stat,
                                  ObOptimizerRelation *rel_opt,
                                  ObSelInfo &sel_info,
                                  ObRawExpr *expr);
        int gen_equal_filter_selectivity(ObLogicalPlan *logical_plan,
                                         ObPhysicalPlan *physical_plan,
                                         ErrStat &err_stat,
                                         ObOptimizerRelation *rel_opt,
                                         ObSelInfo &sel_info,
                                         ObRawExpr *expr);
        int gen_bool_filter_selectivity(ObLogicalPlan *logical_plan,
                                        ObPhysicalPlan *physical_plan,
                                        ErrStat &err_stat,
                                        ObOptimizerRelation *rel_opt,
                                        ObSelInfo &sel_info,
                                        ObRawExpr *expr);
        int gen_btw_selectivity(ObLogicalPlan *logical_plan,
                                ObPhysicalPlan *physical_plan,
                                ErrStat &err_stat,
                                ObOptimizerRelation *rel_opt,
                                ObSelInfo &sel_info,
                                ObRawExpr *expr);
        int gen_range_filter_selectivity(ObLogicalPlan *logical_plan,
                                         ObPhysicalPlan *physical_plan,
                                         ErrStat &err_stat,
                                         ObOptimizerRelation *rel_opt,
                                         ObSelInfo &sel_info,
                                         ObRawExpr *expr);
        int gen_like_selectivity(ObLogicalPlan *logical_plan,
                                 ObPhysicalPlan *physical_plan,
                                 ErrStat &err_stat,
                                 ObOptimizerRelation *rel_opt,
                                 ObSelInfo &sel_info,
                                 ObRawExpr *expr);
        int gen_in_selectivity(ObLogicalPlan *logical_plan,
                               ObPhysicalPlan *physical_plan,
                               ErrStat &err_stat,
                               ObOptimizerRelation *rel_opt,
                               ObSelInfo &sel_info,
                               ObRawExpr *expr);
        int gen_rel_scan_costs(ObLogicalPlan *logical_plan,
                               ObPhysicalPlan *physical_plan,
                               ErrStat& err_stat,
                               ObSelectStmt *select_stmt,
                                ObOptimizerRelation *rel_opt);
        int gen_cost_seq_scan(ObLogicalPlan *logical_plan,
                              ObPhysicalPlan *physical_plan,
                              ErrStat& err_stat,
                              ObSelectStmt *select_stmt,
                               ObOptimizerRelation *rel_opt);

        int gen_cost_index_scan(ObLogicalPlan *logical_plan,
                                ObPhysicalPlan *physical_plan,
                                ErrStat& err_stat,
                                ObSelectStmt *select_stmt,
                                 ObOptimizerRelation *rel_opt);

        int gen_cost_subquery_scan(ObOptimizerRelation *rel_opt);
        int gen_join_method(
                            ObLogicalPlan *logical_plan,
                            ObPhysicalPlan *physical_plan,
                            ErrStat& err_stat,
                            const uint64_t& query_id,
                            ObOptimizerRelation *&sub_query_relation
                            );
        int Opt_Calc_JoinedTables_Cost(
                                       ObLogicalPlan *logical_plan,
                                       ObPhysicalPlan *physical_plan,
                                       ErrStat& err_stat,
                                       ObSelectStmt *select_stmt,
                                       JoinedTable *joined_table,
                                       oceanbase::common::ObList<ObSqlRawExpr*>& remainder_cnd_list,
                                       oceanbase::common::ObList<ObBitSet<> >& bitset_list,
                                        ObOptimizerRelation *left_rel_info,
                                       int32_t& joined_table_id);
        int Opt_Calc_FromItem_Cost(
                                    ObLogicalPlan *logical_plan,
                                       ObPhysicalPlan *physical_plan,
                                       ErrStat& err_stat,
                                       ObSelectStmt *select_stmt,
                                       oceanbase::common::ObList<ObSqlRawExpr*>& remainder_cnd_list,
                                        ObOptimizerRelation *&sub_query_relation,
                                       oceanbase::common::ObList<ObBitSet<> >& bitset_list);
        int find_column_type(
                             ObLogicalPlan *logical_plan,
                             ObSelectStmt *select_stmt,
                             uint64_t table_id,
                             uint64_t column_id,
                             ObObjType& column_type);
        int find_rel_info(
                          ObLogicalPlan *logical_plan,
                          ObPhysicalPlan *physical_plan,
                          ErrStat& err_stat,
                          ObSelectStmt *select_stmt,
                          uint64_t table_id,
                          ObOptimizerRelation *& tmp_relation,
                          bool & is_sub_query_table);
        void opt_get_join_table_raws(
                                    ObOptimizerRelation *rel_info,
                                     ObBinaryRefRawExpr *refexpr,
                                     ErrStat& err_stat,
                                     ObDiffValueInfo &diff_value_info,
                                     ObOptimizerRelation *&base_rel_opt,
                                     bool &is_base_table);
        int Opt_JoinedTables_GetEqExpr(
                                       ObRawExpr *equal_expr,
                                       ObOptimizerRelation *left_rel_info,
                                       ErrStat& err_stat,
                                       double &nrows,
                                       double &min_row);
        int Opt_JoinedTables_GetOrExpr(
                                       ObRawExpr *op_expr,
                                       ObOptimizerRelation *left_rel_info,
                                       ErrStat& err_stat,
                                       double &nrows,
                                       double &min_row);
    private:
        ObTransformer * transformer_;
        ObOptJoinParam join_ctx_;
        ObStatExtractor stat_extractor_;
    };
}
}
#endif //OB_PHYSICAL_OPTIMIZER_H
