#include "ob_physical_optimizer.h"
#include "mergeserver/ob_merge_server_main.h"
#include "parse_malloc.h"
#include "ob_schema_checker.h"

namespace oceanbase
{
namespace sql
{
    using namespace oceanbase::common;
    using namespace oceanbase::mergeserver;
    ObPhysicalOptimizer::ObPhysicalOptimizer(ObTransformer *transformer)
    {
        transformer_ = transformer;
        stat_extractor_.set_statistic_info_cache(ObMergeServerMain::get_instance()->get_merge_server().get_statistic_info_cache());
    }
    template <class T>
    int ObPhysicalOptimizer::get_stmt(
                                      ObLogicalPlan *logical_plan,
                                      ErrStat& err_stat,
                                      const uint64_t& query_id,
                                      T *& stmt)
    {
        int& ret = err_stat.err_code_ = OB_SUCCESS;
        if(query_id == OB_INVALID_ID)
        {
            stmt = dynamic_cast<T*> (logical_plan->get_main_stmt());
        }
        else
        {
           stmt = dynamic_cast<T*> (logical_plan->get_query(query_id));
        }
        if(stmt == NULL)
        {
            err_stat.err_code_ = OB_ERR_PARSER_SYNTAX;
        }
        return ret;
    }

    int ObPhysicalOptimizer::gen_rel_opts(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat &err_stat,
            const uint64_t &query_id)
    {
        int ret = OB_SUCCESS;
        ObSelectStmt *select_stmt = NULL;
        if((ret = get_stmt(logical_plan,err_stat,query_id,select_stmt))!= OB_SUCCESS)
        {
            YYSYS_LOG(ERROR,"QX get select_stmt fail, query_id = %ld. ret=%d", query_id, ret);
        }
        oceanbase::common::ObList<ObOptimizerRelation*> * rel_opt_list = NULL;
        rel_opt_list = select_stmt->get_rel_opt_list();
        common::hash::ObHashMap<uint64_t,ObBaseRelStatInfo*,common::hash::NoPthreadDefendMode> *map = NULL;
        map = select_stmt->get_table_id_statInfo_map();
        if(map != NULL && transformer_->get_sql_context()->merge_service_ != NULL)
        {

        }
        else
        {
            ret = OB_ERROR;
            YYSYS_LOG(ERROR,"QX map or sql_context_->merge_service_ is not initialize.");
        }

        if (rel_opt_list == NULL)
        {
            ret = OB_ERROR;
            YYSYS_LOG(ERROR,"QX rel_opt_list is NULL.");
        }

        int32_t num_item = select_stmt->get_from_item_size();
        TableItem* table_item = NULL;
        ObOptimizerRelation * rel_opt = NULL;
        for(int32_t i=0; ret == OB_SUCCESS && i<num_item;i++)
        {
            const FromItem& from_item = select_stmt->get_from_item(i);
            if(ret == OB_SUCCESS && from_item.is_joined_ == false)
            {
                if(from_item.table_id_ == OB_INVALID_ID
                        || (table_item = select_stmt->get_table_item_by_id(from_item.table_id_)) == NULL)
                {
                    ret = OB_ERR_ILLEGAL_ID;
                    YYSYS_LOG(ERROR, "QX wrong table id [%ld].", from_item.table_id_);
                    break;
                }
                else if(table_item->type_ != TableItem::BASE_TABLE &&
                        table_item->type_ != TableItem::ALIAS_TABLE)
                {
                    continue;
                }
                else if(table_item->ref_id_ <= common::OB_APP_MIN_TABLE_ID + 2000)
                {
                    ret = OB_QUERY_OPT_HAVE_SYSTEM_TABLE;
                    continue;
                }
                else
                {
                    void * buf = logical_plan->get_name_pool()->alloc(sizeof(ObOptimizerRelation));
                    if(buf == NULL)
                    {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        YYSYS_LOG(ERROR, "QX fail to new ObOptimizerRelation");
                        break;
                    }
                    else
                    {
                        rel_opt = new(buf) ObOptimizerRelation();
                    }
                }
                rel_opt->set_table_id(table_item->table_id_);
                rel_opt->set_table_ref_id(table_item->ref_id_);
                rel_opt->set_rel_opt_kind(ObOptimizerRelation::RELOPT_BASEREL);
                rel_opt->set_table_id_statInfo_map(map);
                if(OB_SUCCESS != (ret = init_rel_opt(logical_plan,select_stmt,rel_opt)))
                {

                }
                else if(OB_SUCCESS != (ret = gen_rel_size_estimitates(logical_plan,physical_plan,err_stat,rel_opt)))
                {

                }
                else if(OB_SUCCESS != (ret = gen_rel_scan_costs(logical_plan,physical_plan,err_stat,select_stmt,rel_opt)))
                {
                    YYSYS_LOG(DEBUG,"QX generate rel_opt scan cost fail. ret =%d",ret);
                }
                if(OB_SUCCESS == ret && OB_SUCCESS !=(ret = rel_opt_list->push_back(rel_opt)))
                {
                    YYSYS_LOG(ERROR, "QX push back rel_opt fail.");
                }
                else if(OB_SUCCESS == ret )
                {
                    rel_opt->print_rel_opt_info();
                }
            }
            else
            {
                JoinedTable *joined_table = select_stmt->get_joined_table(from_item.table_id_);
                if(joined_table == NULL)
                {
                    ret = OB_ERR_ILLEGAL_ID;
                    YYSYS_LOG(ERROR, "QX wrong joined table id '%ld'", from_item.table_id_);
                    break;
                }
                OB_ASSERT (joined_table->table_ids_.count() >=2);
                OB_ASSERT (joined_table->table_ids_.count() -1 == joined_table->join_types_.count());
                OB_ASSERT (joined_table->join_types_.count() == joined_table->expr_nums_per_join_.count());
                ObSqlRawExpr *join_expr = NULL;
                int64_t join_expr_position = 0;
                int64_t join_expr_num = 0;
                int64_t joined_table_count = joined_table->table_ids_.count();
                for(int64_t j=0; ret== OB_SUCCESS && j< joined_table_count;j++)
                {
                    uint64_t table_id = OB_INVALID_ID, real_table_id = OB_INVALID_ID;
                    table_id = joined_table->table_ids_.at(j);
                    if(table_id == OB_INVALID_ID ||(table_item = select_stmt->get_table_item_by_id(table_id)) == NULL)
                    {
                        ret = OB_ERR_ILLEGAL_ID;
                        YYSYS_LOG(ERROR, "QX wrong table id [%ld].",table_id);
                        break;
                    }
                    bool sub_query_flag = false;
                    switch (table_item->type_)
                    {
                    case TableItem::BASE_TABLE:
                    case TableItem::ALIAS_TABLE:
                    {
                        real_table_id = table_item->ref_id_;
                        break;
                    }
                    case TableItem::GENERATED_TABLE:
                    {
                        sub_query_flag = true;
                        break;
                    }
                    default:
                        OB_ASSERT(0);
                        break;
                    }
                    if(sub_query_flag)
                    {
                        continue;
                    }
                    else if(table_item->ref_id_ <= common::OB_APP_MIN_TABLE_ID +2000)
                    {
                        ret = OB_QUERY_OPT_HAVE_SYSTEM_TABLE;
                        continue;
                    }
                    void *buf = logical_plan->get_name_pool()->alloc(sizeof(ObOptimizerRelation));
                    if(buf == NULL)
                    {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        YYSYS_LOG(WARN, "QX fail to new ObOptimizerRelation");
                        break;
                    }
                    else
                    {
                        rel_opt = new (buf)ObOptimizerRelation();
                    }
                    if(rel_opt == NULL)
                    {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        YYSYS_LOG(WARN, "QX fail to new ObOptimizerRelation memory");
                        break;
                    }
                    rel_opt->set_rel_opt_kind(ObOptimizerRelation::RELOPT_BASEREL);
                    rel_opt->set_table_id(table_item->table_id_);
                    rel_opt->set_table_ref_id(real_table_id);
                    rel_opt->set_table_id_statInfo_map(map);
                    if(OB_SUCCESS != (ret = init_rel_opt(logical_plan,select_stmt,rel_opt)))
                    {
                        break;
                    }
                    if(j == 0)
                    {
                        join_expr_num = joined_table->expr_nums_per_join_.at(0);
                    }
                    else
                    {
                        join_expr_num = joined_table->expr_nums_per_join_.at(j-1);
                    }
                    if(j == (joined_table_count -1))
                    {
                    }
                    else if((j==0 && joined_table->join_types_.at(j) == JoinedTable::T_INNER) ||
                            (j>0 && joined_table->join_types_.at(j-1) == JoinedTable::T_INNER))
                    {
                        for(int64_t join_index = 0;join_index <join_expr_num;++join_index)
                        {
                            join_expr = logical_plan->get_expr(joined_table->expr_ids_.at(join_expr_position + join_index));
                            common::ObBitSet<> expr_bit_set;
                            expr_bit_set.add_member(select_stmt->get_table_bit_index(rel_opt->get_table_id()));
                            if(join_expr == NULL)
                            {
                                ret = OB_ERR_ILLEGAL_INDEX;
                                YYSYS_LOG(ERROR, "QX add outer join condition faild");
                                break;
                            }
                            else if(expr_bit_set.is_superset(join_expr->get_tables_set()) && join_expr->can_push_down_with_outerjoin())
                            {
                                ret = rel_opt->get_base_cnd_list().push_back(join_expr);
                            }
                        }
                    }
                    if(OB_SUCCESS == ret)
                    {
                        if(j !=0)
                        {
                            join_expr_position += join_expr_num;
                        }
                    }
                    else
                    {
                        break;
                    }
                    if(OB_SUCCESS != (ret = gen_rel_size_estimitates(logical_plan,physical_plan,err_stat,rel_opt)))
                    {
                    }
                    else if(OB_SUCCESS != (ret = gen_rel_scan_costs(logical_plan,physical_plan,err_stat,select_stmt,rel_opt)))
                    {
                    }
                    if(OB_SUCCESS == ret && OB_SUCCESS != (ret = rel_opt_list->push_back(rel_opt)))
                    {
                        YYSYS_LOG(ERROR, "QX push back rel_opt fail.");
                    }
                    else if(OB_SUCCESS == ret)
                    {
                        rel_opt->print_rel_opt_info();
                    }

                }
                if(OB_SUCCESS != ret)
                    return ret;
                bool left_join_to_inner_join = true;
                if(left_join_to_inner_join)

                {
                    int64_t joined_table_num = joined_table->table_ids_.count();
                    for(int64_t ii=0; ii < joined_table_num-1;ii++)
                    {

                    }
                    bool left_to_inners[joined_table_num];
                    for(int ii=0;ii<joined_table_num;ii++)
                    {
                        left_to_inners[ii] = false;
                    }
                    ObSqlRawExpr *join_expr = NULL;
                    int64_t join_expr_position = 0;
                    int64_t join_expr_num = 0;
                    join_expr_position = joined_table->expr_ids_.count();
                    for(int64_t j= joined_table_num-1;ret ==OB_SUCCESS && j>0;j--)
                    {
                        common::ObList<ObOptimizerRelation*>::const_iterator iter = rel_opt_list->begin();
                        rel_opt = NULL;
                        for(;iter!= rel_opt_list->end();iter++)
                        {
                            if((*iter)->get_table_id() == joined_table->table_ids_.at(j))
                            {
                                rel_opt = (*iter);
                                break;
                            }
                        }
                        if(rel_opt == NULL)
                        {
                        }
                        else if(rel_opt->get_base_cnd_list().size()>0 &&
                                (joined_table->join_types_.at(j-1) == JoinedTable::T_LEFT ||
                                 joined_table->join_types_.at(j-1) == JoinedTable::T_INNER))
                        {
                            left_to_inners[j] = true;
                        }
                        join_expr_num = joined_table->expr_nums_per_join_.at(j-1);
                        join_expr_position -= join_expr_num;
                        if(left_to_inners[j])
                        {
                            for(int64_t join_index =0;join_index <join_expr_num;join_index++)
                            {
                                join_expr = logical_plan->get_expr(joined_table->expr_ids_.at(join_expr_position + join_index));
                                if(join_expr == NULL)
                                {
                                    ret = OB_ERR_ILLEGAL_INDEX;
                                    YYSYS_LOG(ERROR, "QX add outer join condition faild");
                                    break;
                                }
                                else
                                {
                                    ObRawExpr *expr = join_expr->get_expr();
                                    ObBinaryRefRawExpr *binaryRef_expr1 = NULL;
                                    ObBinaryRefRawExpr *binaryRef_expr2 = NULL;
                                    uint64_t left_table_id = OB_INVALID_ID;
                                    if(expr == NULL)
                                    {
                                        ret = OB_ERROR;
                                        YYSYS_LOG(ERROR, "QX expr is null.");
                                    }
                                    else if(expr->is_join_cond())
                                    {
                                        ObBinaryOpRawExpr *binaryop_expr = dynamic_cast<ObBinaryOpRawExpr*>(expr);
                                        if(binaryop_expr == NULL)
                                        {
                                            ret = OB_ERROR;
                                            YYSYS_LOG(ERROR, "QX binaryop_expr is null.");
                                        }
                                        else
                                        {
                                            binaryRef_expr1 =
                                                    dynamic_cast<ObBinaryRefRawExpr*>(binaryop_expr->get_first_op_expr());
                                            binaryRef_expr2 =
                                                    dynamic_cast<ObBinaryRefRawExpr*>(binaryop_expr->get_second_op_expr());
                                        }
                                        if(binaryRef_expr1 == NULL || binaryRef_expr2 == NULL)
                                        {
                                            YYSYS_LOG(WARN, "QX binaryRef_expr1 or binaryRef_expr2 is null.");
                                        }
                                        else if(binaryRef_expr1->get_first_ref_id() == joined_table->table_ids_.at(j))
                                        {
                                            left_table_id = binaryRef_expr2->get_first_ref_id();
                                        }
                                        else if(binaryRef_expr2->get_first_ref_id() == joined_table->table_ids_.at(j))
                                        {
                                            left_table_id = binaryRef_expr1->get_first_ref_id();
                                        }
                                        if(left_table_id == OB_INVALID_ID)
                                        {
                                        }
                                        else
                                        {
                                            for(int64_t idx = j-1;idx>0;idx--)
                                            {
                                                if(joined_table->table_ids_.at(idx) == left_table_id)
                                                {
                                                    if(joined_table->join_types_.at(idx-1) == JoinedTable::T_LEFT)
                                                    {
                                                        left_to_inners[idx] = true;
                                                        if(idx ==1)
                                                        {
                                                            left_to_inners[0] = true;
                                                        }
                                                    }
                                                    break;
                                                }
                                            }
                                        }
                                    }

                                }
                            }
                        }
                    }
                    join_expr_position =0;
                    join_expr_num =0;
                    for(int64_t ii=0;ii<joined_table_num-1 &&ret == OB_SUCCESS;ii++)
                    {
                        join_expr_num = joined_table->expr_nums_per_join_.at(ii);
                        if(left_to_inners[ii+1])
                        {
                            joined_table->join_types_.at(ii) = JoinedTable::T_INNER;
                            for(int64_t join_index =0;join_index < join_expr_num && rel_opt!= NULL;++join_index)
                            {
                                join_expr = logical_plan->get_expr(joined_table->expr_ids_.at(join_expr_position + join_index));
                                common::ObBitSet<> expr_bit_set;
                                if(join_expr ==NULL)
                                {
                                    ret = OB_ERR_ILLEGAL_INDEX;
                                    YYSYS_LOG(ERROR, "QX add outer join condition faild");
                                    break;
                                }
                                else if(!join_expr->get_expr()->is_join_cond_opt() && join_expr->can_push_down_with_outerjoin())
                                {
                                    common::ObList<ObOptimizerRelation*>::const_iterator iter = rel_opt_list->begin();
                                    for(;iter != rel_opt_list->end();iter++)
                                    {
                                        rel_opt = (ObOptimizerRelation*)(*iter);
                                        expr_bit_set.clear();
                                        expr_bit_set.add_member(select_stmt->get_table_bit_index(rel_opt->get_table_id()));
                                        if(expr_bit_set.is_superset(join_expr->get_tables_set()))
                                        {
                                            ret = rel_opt->get_base_cnd_list().push_back(join_expr);
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        join_expr_position += join_expr_num;
                    }
                    bool adjust_order = true;
                    if(adjust_order && joined_table->join_types_.at(0) == JoinedTable::T_INNER)
                    {
                        double first_rel_opt_diff =0;
                        double second_rel_opt_diff =0;
                        double second_data_skew = 1.0;
                        int find_num = 0;
                        uint64_t left_column_id = OB_INVALID_ID,right_column_id = OB_INVALID_ID;
                        ObBinaryOpRawExpr *binaryop_expr = dynamic_cast<ObBinaryOpRawExpr*>(logical_plan->get_expr(joined_table->expr_ids_.at(0))->get_expr());
                        ObBinaryRefRawExpr *binaryRef_expr1 = NULL,*binaryRef_expr2 = NULL;
                        if(binaryop_expr ==NULL)
                        {
                        }
                        else
                        {
                            binaryRef_expr1 = dynamic_cast<ObBinaryRefRawExpr*>(binaryop_expr->get_first_op_expr());
                            binaryRef_expr2 = dynamic_cast<ObBinaryRefRawExpr*>(binaryop_expr->get_second_op_expr());
                        }
                        if(binaryRef_expr1 == NULL || binaryRef_expr2 == NULL)
                        {
                        }
                        else if(binaryRef_expr1->get_first_ref_id() == joined_table->table_ids_.at(0))
                        {
                            left_column_id = binaryRef_expr1->get_second_ref_id();
                            right_column_id = binaryRef_expr2->get_second_ref_id();
                        }
                        else if(binaryRef_expr2->get_first_ref_id() == joined_table->table_ids_.at(0))
                        {                            
                            right_column_id = binaryRef_expr1->get_second_ref_id();
                            left_column_id = binaryRef_expr2->get_second_ref_id();
                        }
                        if(left_column_id == OB_INVALID_ID || right_column_id == OB_INVALID_ID)
                        {
                        }
                        else
                        {
                            common::ObList<ObOptimizerRelation*>::const_iterator iter = rel_opt_list->begin();
                            for(;iter !=rel_opt_list->end();iter++)
                            {
                                rel_opt = (ObOptimizerRelation*)(*iter);
                                if(rel_opt->get_table_id() == joined_table->table_ids_.at(0))
                                {
                                    ObDiffValueInfo diff_value_info;
                                    gen_joined_column_diff_number(logical_plan,physical_plan,err_stat,select_stmt,rel_opt,rel_opt->get_table_id(),left_column_id,diff_value_info);
                                    first_rel_opt_diff = diff_value_info.total_diff_num;
                                    find_num ++;
                                }
                                else if(rel_opt->get_table_id() == joined_table->table_ids_.at(1))
                                {
                                    ObDiffValueInfo diff_value_info;
                                    gen_joined_column_diff_number(logical_plan,physical_plan,err_stat,select_stmt,rel_opt,rel_opt->get_table_id(),right_column_id,diff_value_info);
                                    second_rel_opt_diff = diff_value_info.total_diff_num;
                                    second_data_skew = diff_value_info.data_skew;
                                    find_num ++;
                                }
                                if(find_num >=2)
                                {
                                    break;
                                }
                            }
                        }
                        if(find_num >=2 && first_rel_opt_diff > second_rel_opt_diff
                               && (second_data_skew <DATA_SKEW_THRESHOLD
                                 && second_rel_opt_diff < SEMI_DIFF_NUM_THRESHOLD)
                                )
                        {
                            bool flag = true;
                            if(joined_table->expr_nums_per_join_.at(0)>=1)
                            {
                                for(int64_t join_index = 0;join_index <joined_table->expr_nums_per_join_.at(0);++join_index)
                                {
                                    ObSqlRawExpr *join_expr = logical_plan->get_expr(joined_table->expr_ids_.at(join_index));
                                    if(join_expr == NULL)
                                    {
                                        ret = OB_ERR_ILLEGAL_INDEX;
                                        YYSYS_LOG(ERROR,"QX add outer join condition faild");
                                        flag = false;
                                        break;
                                    }
                                    else if(!join_expr->get_expr()->is_join_cond_opt())
                                    {
                                        flag = false;
                                        break;
                                    }
                                }
                            }
                            if(flag)
                            {
                                uint64_t tmp_table_id = joined_table->table_ids_.at(0);
                                joined_table->table_ids_.at(0) = joined_table->table_ids_.at(1);
                                joined_table->table_ids_.at(1) = tmp_table_id;
                            }
                        }
                        bool enable_explicit_to_implicit_inner_join = true;
                        if (enable_explicit_to_implicit_inner_join)
                        {
                            int64_t joined_table_idx =0;
                            join_expr_position =0;
                            for(;joined_table_idx<joined_table_num-1 && ret== OB_SUCCESS ;joined_table_idx++)
                            {
                                if(joined_table->join_types_.at(joined_table_idx) != JoinedTable::T_INNER)
                                {
                                    break;
                                }
                                else
                                {
                                    join_expr_num = joined_table->expr_nums_per_join_.at(joined_table_idx);
                                    bool has_join_cond = false;
                                    for(int64_t join_index =0;join_index < join_expr_num;++join_index)
                                    {
                                        join_expr = logical_plan->get_expr(joined_table->expr_ids_.at(join_expr_position + join_index));
                                        if(join_expr == NULL)
                                        {
                                            ret = OB_ERR_ILLEGAL_INDEX;
                                            YYSYS_LOG(ERROR,"QX add outer join condition faild");
                                            break;
                                        }
                                        else if(join_expr->get_expr()->is_join_cond_opt())
                                        {
                                            has_join_cond = true;
                                        }
                                    }
                                    if(OB_SUCCESS == ret && has_join_cond)
                                    {
                                        join_expr_position += join_expr_num;
                                    }
                                    else
                                    {
                                        break;
                                    }
                                }
                            }
                            if(joined_table_idx == joined_table_num-1)
                            {
                                join_expr_position =0;
                                join_expr_num =0;
                                for(int32_t jj=1; ret == OB_SUCCESS && jj<joined_table->table_ids_.count(); jj++)
                                {
                                    join_expr_num = joined_table->expr_nums_per_join_.at(jj-1);

                                        for(int64_t join_index =0; ret == OB_SUCCESS && join_index < join_expr_num; ++join_index)
                                        {
                                            join_expr = logical_plan->get_expr(joined_table->expr_ids_.at(join_expr_position + join_index));
                                            if(join_expr ==NULL)
                                            {
                                                ret = OB_ERR_ILLEGAL_INDEX;
                                                YYSYS_LOG(ERROR, "Add outer join condition faild");
                                                break;
                                            }
                                            else
                                            {
                                                ret = select_stmt->get_where_exprs().push_back(join_expr->get_expr_id());
                                            }
                                        }
                                        if(OB_SUCCESS == ret)
                                        {
                                            ret = select_stmt->add_from_item(joined_table->table_ids_.at(jj));
                                            join_expr_position += join_expr_num;
                                        }
                                        else
                                        {
                                            break;
                                        }
                                }
                                if(OB_SUCCESS == ret)
                                {
                                    uint64_t from_item_table_id = joined_table->table_ids_.at(0);
                                    if((ret = select_stmt->remove_joined_table(from_item.table_id_)) != OB_SUCCESS)
                                    {
                                        YYSYS_LOG(WARN, "DHC ERROR remove_joined_table");
                                    }
                                    FromItem& from_item_update = select_stmt->get_from_item_for_update(i);
                                    from_item_update.table_id_ = from_item_table_id;
                                    from_item_update.is_joined_ = false;
                                }

                            }
                        }

                    }
                }
            }
        }
        return ret;
    }

    int ObPhysicalOptimizer::transform_expr_const_object_type(
            ObLogicalPlan *logical_plan,
            ObSelectStmt *select_stmt,
            ObOptimizerRelation *rel_opt)
    {
        int ret = OB_SUCCESS;
        uint64_t table_id,column_id;
        UNUSED(logical_plan);
        UNUSED(select_stmt);
        common::ObVector<ObSqlRawExpr*>::iterator iter = rel_opt->get_base_cnd_list().begin();
        for(; iter!= rel_opt->get_base_cnd_list().end();iter++)
        {
            ObRawExpr *raw_expr = (*iter)->get_expr();
            ObConstRawExpr *const_expr = NULL;
            ObBinaryRefRawExpr *binaryRef_expr = NULL;

            if(raw_expr == NULL)
            {
                ret = OB_ERROR;
                YYSYS_LOG(ERROR,"raw_expr is null.");
            }
            else if(raw_expr->get_expr_type() >= T_OP_EQ
                    && raw_expr->get_expr_type() <= T_OP_NE)
            {
                ObBinaryOpRawExpr *binary_expr = dynamic_cast<ObBinaryOpRawExpr *>(raw_expr);
                if(binary_expr == NULL)
                {

                }
                else if(binary_expr->get_first_op_expr()->is_const()
                        &&binary_expr->get_second_op_expr()->is_column())
                {
                    const_expr = dynamic_cast<ObConstRawExpr*>(binary_expr->get_first_op_expr());
                    binaryRef_expr = dynamic_cast<ObBinaryRefRawExpr*>(binary_expr->get_second_op_expr());
                }
                else if(binary_expr->get_second_op_expr()->is_const()
                        &&binary_expr->get_first_op_expr()->is_column())
                {
                    binaryRef_expr = dynamic_cast<ObBinaryRefRawExpr*>(binary_expr->get_first_op_expr());
                    const_expr = dynamic_cast<ObConstRawExpr*>(binary_expr->get_second_op_expr());
                }
                if(binaryRef_expr != NULL && const_expr != NULL)
                {
                    table_id = binaryRef_expr->get_first_ref_id();
                    column_id = binaryRef_expr->get_second_ref_id();
                    ObObj &const_value = const_cast<ObObj &>(const_expr->get_value());
                    const common::ObSchemaManagerV2 *schema_managerv2 = rel_opt->get_schema_managerv2();
                    const common::ObColumnSchemaV2 *column_schema = NULL;
                    if(rel_opt->get_table_id() != table_id)
                    {
                    }
                    else if(schema_managerv2 == NULL)
                    {
                        YYSYS_LOG(ERROR,"QX schema_managerv2 is null");
                    }
                    else
                    {
                        column_schema = schema_managerv2->get_column_schema(rel_opt->get_table_ref_id(),column_id);
                    }
                    if(column_schema == NULL)
                    {
                    }
                    else
                    {
                        if(column_schema->get_type() == const_value.get_type())
                        {
                        }
                        else if(column_schema->get_type() == ObDateType
                                && const_value.get_type() == ObVarcharType)
                        {
                            ObString tmp;
                            ret = obj_cast(const_value,ObDateType,tmp);
                            if(ret == OB_SUCCESS)
                            {
                                const_expr->set_expr_type(T_DATE_NEW);
                                const_expr->set_result_type(ObDateType);
                            }
                            else
                            {
                                ret = OB_SUCCESS;
                            }
                        }
                    }
                }
            }
        }
        return ret;
    }

    int ObPhysicalOptimizer::init_rel_opt(
            ObLogicalPlan *logical_plan,
            ObSelectStmt *select_stmt,
            ObOptimizerRelation *rel_opt)
    {
        int ret = OB_SUCCESS;
        int32_t num = 0;
        uint64_t table_id = rel_opt->get_table_id();
        const common::ObSchemaManagerV2 *schema_managerv2 = NULL;
        schema_managerv2 = transformer_->get_sql_context()->schema_manager_;
        rel_opt->set_schema_managerv2(schema_managerv2);
        rel_opt->set_stat_extractor(&stat_extractor_);
        rel_opt->set_name_pool(logical_plan->get_name_pool());
        rel_opt->set_group_by_num(select_stmt->get_group_expr_size());
        rel_opt->set_order_by_num(select_stmt->get_order_item_size());
        ret = gen_rel_tuples(rel_opt);
        if(ObOPtimizerLoger::log_switch_)
        {
            char tmp[256] = {0};
            snprintf(tmp,256,"QOQX table id= %ld tuples = %.6lf",table_id,rel_opt->get_tuples());
            ObOPtimizerLoger::print(tmp);
        }
        if(select_stmt != NULL)
        {
            num = select_stmt->get_condition_size();
        }
        else
        {
            ret = OB_ERROR;
            YYSYS_LOG(ERROR,"QX select_stmt is null!");
        }

        common::ObBitSet<> expr_bit_set;
        for(int32_t i =0;ret == OB_SUCCESS && i<num;i++)
        {
            uint64_t expr_id = select_stmt->get_condition_id(i);
            ObSqlRawExpr *where_expr = logical_plan->get_expr(expr_id);
            expr_bit_set.clear();
            expr_bit_set.add_member(select_stmt->get_table_bit_index(table_id));
            if(!where_expr || where_expr->is_apply() == true)
            {

            }
            else if(where_expr->get_tables_set().has_member(select_stmt->get_table_bit_index(table_id)))
            {
                if(where_expr->get_expr()->is_join_cond_opt())
                {
                    ret = rel_opt->get_join_cnd_list().push_back(where_expr);
                }
                else if(where_expr->can_push_down_with_outerjoin() &&
                        expr_bit_set == where_expr->get_tables_set())
                {
                    ret = rel_opt->get_base_cnd_list().push_back(where_expr);
                }
                else
                {
                }
            }
            else if(where_expr->get_expr()->is_const())
            {
                ret = rel_opt->get_base_cnd_list().push_back(where_expr);
            }
        }
        bool optimize_expr_type = true;
        if (ret == OB_SUCCESS && optimize_expr_type)
        {
            ret = transform_expr_const_object_type(logical_plan,select_stmt,rel_opt);
        }
        return ret;
    }
    int ObPhysicalOptimizer::gen_rel_tuples(
            ObOptimizerRelation *rel_opt)
    {
        int ret = OB_SUCCESS;
        bool enable_statinfo = false;
        uint64_t table_id = rel_opt->get_table_id();
        ObStatExtractor * stat_extractor = NULL;
        sql::ObBaseRelStatInfo * rel_stat_info = NULL;
        common::hash::ObHashMap<uint64_t,ObBaseRelStatInfo*,common::hash::NoPthreadDefendMode> *table_id_statInfo_map = NULL;
        table_id_statInfo_map = rel_opt->get_table_id_statInfo_map();
        stat_extractor = rel_opt->get_stat_extractor();
        if(stat_extractor ==NULL)
        {
            ret = OB_ERROR;
        }
        else if(table_id_statInfo_map == NULL)
        {
            ret = OB_ERROR;
        }
        else if(OB_SUCCESS != (ret = stat_extractor->fill_table_statinfo_map(table_id_statInfo_map,rel_opt)))
        {

        }
        else if(common::hash::HASH_EXIST != table_id_statInfo_map->get(table_id,rel_stat_info) ||
                rel_stat_info == NULL ||
                !rel_stat_info->enable_statinfo)
        {
            ret = OB_ERROR;
        }
        else
        {
            enable_statinfo = true;
            rel_opt->set_tuples(rel_stat_info->tuples_);
        }

        if(ret != OB_SUCCESS && !enable_statinfo)
        {
            ret = OB_SUCCESS;
            if(rel_stat_info == NULL)
            {
                void *buf = NULL;
                if(rel_opt->get_name_pool() == NULL)
                {
                    ret = OB_ERROR;
                    YYSYS_LOG(ERROR,"QX rel_opt name_pool is null.");
                }
                else if((buf = rel_opt->get_name_pool()->alloc(sizeof(sql::ObBaseRelStatInfo))) == NULL)
                {
                    ret = OB_ALLOCATE_MEMORY_FAILED;
                    YYSYS_LOG(ERROR,"QX fail to alloc ObBaseRelStatInfo space.");
                }
                else
                {
                    rel_stat_info = new(buf) sql::ObBaseRelStatInfo();
                }
            }
            if(OB_SUCCESS == ret && rel_stat_info!= NULL)
            {
                rel_stat_info->enable_statinfo = false;
                rel_stat_info->table_id_ = rel_opt->get_table_id();
                rel_stat_info->tuples_ = DEFAULT_TABLE_TUPLES;
                rel_opt->set_tuples(rel_stat_info->tuples_);
                if(common::hash::HASH_OVERWRITE_SUCC == (ret = table_id_statInfo_map->set(table_id,rel_stat_info,1)) ||
                        common::hash::HASH_INSERT_SUCC == ret)
                {
                    ret = OB_SUCCESS;
                }
                else
                {

                }
            }
        }
        return ret;
    }
    int is_relation_expr(
            ObOptimizerRelation *rel_opt,
            const uint64_t column_id,
            ObRawExpr *expr,
            bool &relation_expr)
    {
        UNUSED(rel_opt);
        int ret = OB_SUCCESS;
        ObBinaryRefRawExpr *binaryref_expr = NULL;
        switch (expr->get_expr_type())
        {
        case T_OP_EQ:
        case T_OP_NE:
        case T_OP_IS:
        case T_OP_IS_NOT:
        case T_OP_LE:
        case T_OP_LT:
        case T_OP_GE:
        case T_OP_GT:
        case T_OP_LIKE:
        case T_OP_NOT_LIKE:
        {
            ObBinaryOpRawExpr *binary_expr = dynamic_cast<ObBinaryOpRawExpr*>(expr);
            if(binary_expr != NULL)
            {
                if(binary_expr->get_first_op_expr()->is_const())
                {
                    binaryref_expr = dynamic_cast<ObBinaryRefRawExpr*>(binary_expr->get_second_op_expr());
                }
                else
                {
                    binaryref_expr = dynamic_cast<ObBinaryRefRawExpr*>(binary_expr->get_first_op_expr());
                }
                if(binaryref_expr == NULL)
                {

                }
                else if(binaryref_expr->get_second_ref_id() == column_id)
                {
                    relation_expr = true;
                }
                else
                {
                    relation_expr = false;
                }
            }
            break;
        }
        case T_OP_BTW:
        case T_OP_NOT_BTW:
        {
            ObTripleOpRawExpr * tripleop_expr = dynamic_cast<ObTripleOpRawExpr*>(expr);
            if(tripleop_expr == NULL)
            {
            }
            else if ((binaryref_expr = dynamic_cast<ObBinaryRefRawExpr*>(tripleop_expr->get_first_op_expr())))
            {
                if(binaryref_expr == NULL)
                {
                    YYSYS_LOG(ERROR,"QX binaryref_expr is null!");
                }
                else if(binaryref_expr->get_second_ref_id() == column_id)
                {
                    relation_expr = true;
                }
                else
                {
                    relation_expr = false;
                }
            }
            else
            {
                YYSYS_LOG(WARN,"QX some exprs == NULL");
            }
            break;
        }
        case T_OP_IN:
        case T_OP_NOT_IN:
        {
            ObBinaryOpRawExpr *in_expr = NULL;
            ObBinaryRefRawExpr *binaryref_expr = NULL;
            in_expr = dynamic_cast<ObBinaryOpRawExpr*>(expr);
            if(in_expr == NULL)
            {
            }
            else if(in_expr->get_first_op_expr()->get_expr_type() == T_REF_COLUMN &&
                    in_expr->get_second_op_expr()->get_expr_type() == T_OP_ROW)
            {
                binaryref_expr = dynamic_cast<ObBinaryRefRawExpr*>(in_expr->get_first_op_expr());
                if(binaryref_expr == NULL)
                {
                }
                else if(binaryref_expr ->get_second_ref_id() == column_id)
                {
                    relation_expr = true;
                }
                else
                {
                    relation_expr = false;
                }
            }
            else
            {
                relation_expr = false;
            }
            break;
        }
        default:
            break;
        }
        return ret;
    }

    int ObPhysicalOptimizer::gen_bool_expr_divided_by_column_id(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat &err_stat,
            ObOptimizerRelation *rel_opt,
            const uint64_t column_id,
            ObRawExpr *expr,
            double &sel1,
            double &sel2,
            bool &enable_expr)
    {
        int ret = OB_SUCCESS;
        if(expr == NULL)
        {
        }
        else
        {
            switch (expr->get_expr_type())
            {
            case T_OP_AND:
            {
                ObBinaryOpRawExpr * binary_expr = dynamic_cast<ObBinaryOpRawExpr*>(expr);
                double l_sel1 = sel1;
                double r_sel1 = sel1;
                double l_sel2 = sel2;
                double r_sel2 = sel2;
                ret = gen_clause_divided_by_column_id(logical_plan,physical_plan,err_stat,rel_opt,column_id,binary_expr->get_first_op_expr(),l_sel1,l_sel2,enable_expr);
                if(ret != OB_SUCCESS)
                {
                    break;
                }
                else if(!enable_expr)
                {
                    sel1 = 1.0;
                    sel2 = 1.0;
                    break;
                }
                ret = gen_clause_divided_by_column_id(logical_plan,physical_plan,err_stat,rel_opt,column_id,binary_expr->get_second_op_expr(),r_sel1,r_sel2,enable_expr);
                if(ret != OB_SUCCESS)
                {
                    break;
                }
                else if(!enable_expr)
                {
                    sel1 = 1.0;
                    sel2 = 1.0;
                    break;
                }
                if(l_sel1 == sel1 && r_sel1 == sel1)
                {
                }
                else if(l_sel1 !=sel1 && r_sel1 !=sel1)
                {
                    sel1 = l_sel1 *r_sel1 /sel1;
                }
                else if(l_sel1 !=sel1)
                {
                    sel1 = l_sel1;
                }
                else if(r_sel1 !=sel1)
                {
                    sel1 = r_sel1;
                }

                if(l_sel2 == sel2 && r_sel2 == sel2)
                {
                }
                else if(l_sel2 !=sel2 && r_sel2 !=sel2)
                {
                    sel2= l_sel2 *r_sel2 /sel2;
                }
                else if(l_sel2 !=sel2)
                {
                    sel2 = l_sel2;
                }
                else if(r_sel2 !=sel2)
                {
                    sel2 = r_sel2;
                }
                break;
            }
            case T_OP_OR:
            {
                ObBinaryOpRawExpr *binary_expr = dynamic_cast<ObBinaryOpRawExpr*>(expr);
                double l_sel1 = sel1;
                double r_sel1 = sel1;
                double l_sel2 = sel2;
                double r_sel2 = sel2;
                ret = gen_clause_divided_by_column_id(logical_plan,physical_plan,err_stat,rel_opt,column_id,binary_expr->get_first_op_expr(),l_sel1,l_sel2,enable_expr);
                if(ret != OB_SUCCESS)
                {
                    break;
                }
                else if(!enable_expr)
                {
                    sel1 = 1.0;
                    sel2 = 1.0;
                    break;
                }
                ret = gen_clause_divided_by_column_id(logical_plan,physical_plan,err_stat,rel_opt,column_id,binary_expr->get_second_op_expr(),r_sel1,r_sel2,enable_expr);
                if(ret != OB_SUCCESS)
                {
                    break;
                }
                else if(!enable_expr)
                {
                    sel1 = 1.0;
                    sel2 = 1.0;
                    break;
                }

                if(l_sel1 == sel1 && r_sel1 == sel1)
                {
                }
                else if(l_sel1 !=sel1 && r_sel1 !=sel1)
                {
                    sel1 = l_sel1 + r_sel1 - (l_sel1 * r_sel1)/sel1;
                }
                else if(l_sel1 !=sel1)
                {
                    sel1 = l_sel1;
                }
                else if(r_sel1 !=sel1)
                {
                    sel1 = r_sel1;
                }

                if(l_sel2 == sel2 && r_sel2 == sel2)
                {
                }
                else if(l_sel2 !=sel2 && r_sel2 !=sel2)
                {
                    sel2 = l_sel2 + r_sel2 - (l_sel2 * r_sel2)/sel2;
                }
                else if(l_sel2 !=sel2)
                {
                    sel2 = l_sel2;
                }
                else if(r_sel2 !=sel2)
                {
                    sel2 = r_sel2;
                }
                break;
            }
            case T_OP_NOT:
            {
                ObUnaryOpRawExpr *unary_expr = dynamic_cast<ObUnaryOpRawExpr*>(expr);
                if(unary_expr != NULL)
                {
                    double t_sel1 = sel1,t_sel2 = sel2;
                    ret = gen_clause_divided_by_column_id(logical_plan,physical_plan,err_stat,rel_opt,column_id,unary_expr->get_op_expr(),t_sel1,t_sel2,enable_expr);
                    if(sel1 != t_sel1)
                    {
                        sel1 = 1.0 - t_sel1;
                    }
                    if(sel2 != t_sel2)
                    {
                        sel2 = 1.0 - t_sel2;
                    }
                    if(!enable_expr)
                    {
                        sel1 = 1.0;
                        sel2 = 1.0;
                    }

                }
                break;
            }
            default:
                break;
            }
        }
        return ret;
    }

    int ObPhysicalOptimizer::gen_clause_divided_by_column_id(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat &err_stat,
            ObOptimizerRelation *rel_opt,
            const uint64_t column_id,
            ObRawExpr *expr,
            double &sel1,
            double &sel2,
            bool & enable_expr,
            int idx)
    {
        int ret = OB_SUCCESS;
        bool relation_expr = false;
        ObSelInfo sel_info;
        sel_info.enable = true;
        sel_info.enable_expr_subquery_optimization = false;
        int32_t sel_info_count = rel_opt->get_sel_info_array().size();
        if(expr == NULL)
        {
            ret= OB_ERROR;
            YYSYS_LOG(ERROR,"QX expr is null !");
        }
        else
        {
        }

        if(OB_SUCCESS != ret)
        {
            sel_info.selectivity_ = 1.0;
        }
        else if(expr->is_const())
        {
            if(idx != OB_INVALID_INDEX && sel_info_count >idx)
            {
                sel_info = rel_opt->get_sel_info_array().at(idx);
            }
            else
            {
                ret = gen_const_selectivity(logical_plan,physical_plan,err_stat,rel_opt,sel_info,expr);
            }
            relation_expr = true;
        }
        else if(expr->get_expr_type() == T_OP_EQ || expr->get_expr_type() == T_OP_IS)
        {
            if(OB_SUCCESS != (ret = is_relation_expr(rel_opt,column_id,expr,relation_expr)))
            {

            }
            else if(idx != OB_INVALID_INDEX && sel_info_count >idx)
            {
                sel_info = rel_opt->get_sel_info_array().at(idx);
            }
            else if(OB_SUCCESS != (ret = gen_equal_filter_selectivity(logical_plan,physical_plan,err_stat,rel_opt,sel_info,expr)))
            {
            }
        }
        else if(expr->get_expr_type() == T_OP_NE || expr->get_expr_type() == T_OP_IS_NOT)
        {
            if(OB_SUCCESS != (ret = is_relation_expr(rel_opt,column_id,expr,relation_expr)))
            {

            }
            else if(idx != OB_INVALID_INDEX && sel_info_count >idx)
            {
                sel_info = rel_opt->get_sel_info_array().at(idx);
            }
            else if(OB_SUCCESS != (ret = gen_equal_filter_selectivity(logical_plan,physical_plan,err_stat,rel_opt,sel_info,expr)))
            {
            }
            else
            {
                sel_info.selectivity_ = 1.0 - sel_info.selectivity_;
            }
        }
        else if(expr->is_range_filter())
        {
            if(OB_SUCCESS != (ret = is_relation_expr(rel_opt,column_id,expr,relation_expr)))
            {

            }
            else if(idx != OB_INVALID_INDEX && sel_info_count >idx)
            {
                sel_info = rel_opt->get_sel_info_array().at(idx);
            }
            else if(OB_SUCCESS != (ret = gen_range_filter_selectivity(logical_plan,physical_plan,err_stat,rel_opt,sel_info,expr)))
            {
            }
        }
        else if(expr->get_expr_type() == T_OP_AND ||
                expr->get_expr_type() == T_OP_OR ||
                expr->get_expr_type() == T_OP_NOT)
        {
            if(OB_SUCCESS != (ret = gen_bool_expr_divided_by_column_id(logical_plan,physical_plan,err_stat,rel_opt,column_id,expr,sel1,sel2,enable_expr)))
            {
            }
            else
            {
                sel_info.selectivity_ = 1.0;
                sel_info.enable = enable_expr;
            }
        }
        else if(expr->get_expr_type() == T_OP_LIKE)
        {
            if(OB_SUCCESS != (ret = is_relation_expr(rel_opt,column_id,expr,relation_expr)))
            {

            }
            else if(idx != OB_INVALID_INDEX && sel_info_count >idx)
            {
                sel_info = rel_opt->get_sel_info_array().at(idx);
            }
            else if(OB_SUCCESS != (ret = gen_like_selectivity(logical_plan,physical_plan,err_stat,rel_opt,sel_info,expr)))
            {
            }
        }
        else if(expr->get_expr_type() == T_OP_NOT_LIKE)
        {
            if(OB_SUCCESS != (ret = is_relation_expr(rel_opt,column_id,expr,relation_expr)))
            {

            }
            else if(idx != OB_INVALID_INDEX && sel_info_count >idx)
            {
                sel_info = rel_opt->get_sel_info_array().at(idx);
            }
            else if(OB_SUCCESS != (ret = gen_like_selectivity(logical_plan,physical_plan,err_stat,rel_opt,sel_info,expr)))
            {
            }
            else
            {
                sel_info.selectivity_ = 1.0 - sel_info.selectivity_;
            }
        }
        else if(expr->get_expr_type() == T_OP_IN)
        {
            if(OB_SUCCESS != (ret = is_relation_expr(rel_opt,column_id,expr,relation_expr)))
            {

            }
            else if(idx != OB_INVALID_INDEX && sel_info_count >idx)
            {
                sel_info = rel_opt->get_sel_info_array().at(idx);
            }
            else if(OB_SUCCESS != (ret = gen_in_selectivity(logical_plan,physical_plan,err_stat,rel_opt,sel_info,expr)))
            {
            }
        }
        else if(expr->get_expr_type() == T_OP_NOT_IN)
        {
            if(OB_SUCCESS != (ret = is_relation_expr(rel_opt,column_id,expr,relation_expr)))
            {

            }
            else if(idx != OB_INVALID_INDEX && sel_info_count >idx)
            {
                sel_info = rel_opt->get_sel_info_array().at(idx);
            }
            else if(OB_SUCCESS != (ret = gen_in_selectivity(logical_plan,physical_plan,err_stat,rel_opt,sel_info,expr)))
            {
            }
            else
            {
                sel_info.selectivity_ = 1.0 - sel_info.selectivity_;
            }
        }
        else
        {
            sel_info.selectivity_ = 1.0;
        }
        if(ret == OB_SUCCESS && relation_expr)
        {
            sel1 *= sel_info.selectivity_;
        }
        else
        {
            sel2 *= sel_info.selectivity_;
        }
        enable_expr = sel_info.enable;
        return ret;
    }

    int ObPhysicalOptimizer::gen_clauselist_divided_by_column_id(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat &err_stat,
            ObOptimizerRelation *rel_opt ,
            const uint64_t table_id,
            const uint64_t column_id,
            double &sel1,
            double &sel2)
    {
        UNUSED(table_id);
        int ret = OB_SUCCESS;
        bool enable_expr = true;
        oceanbase::common::ObVector<ObSqlRawExpr*>::iterator cnd_it;
        int idx = 0;
        for(cnd_it = rel_opt->get_base_cnd_list().begin(); ret == OB_SUCCESS && cnd_it !=rel_opt->get_base_cnd_list().end();idx++,cnd_it++)
        {
            ret = gen_clause_divided_by_column_id(logical_plan,physical_plan,err_stat,rel_opt,column_id,(*cnd_it)->get_expr(),sel1,sel2,enable_expr,idx);
        }
        return ret;
    }

    bool ObPhysicalOptimizer::is_having_equal_expr_in_join_column(ObOptimizerRelation *rel_opt,
                                                                  const uint64_t table_id,
                                                                  const uint64_t column_id)
    {
        bool ret = false;
        common::ObVector<ObSqlRawExpr*>::iterator iter = rel_opt->get_base_cnd_list().begin();
        for(;iter !=rel_opt->get_base_cnd_list().end();iter++)
        {
            ObRawExpr *raw_expr = (*iter)->get_expr();
            if(raw_expr->get_expr_type() == T_OP_EQ
                    || raw_expr->get_expr_type() == T_OP_IS)
            {
                ObBinaryOpRawExpr *binary_expr = dynamic_cast<ObBinaryOpRawExpr *>(raw_expr);
                ObConstRawExpr *const_expr = NULL;
                ObBinaryRefRawExpr *binaryRef_expr = NULL;
                if(binary_expr == NULL)
                {
                }
                else if(binary_expr->get_first_op_expr()->is_const()
                        && binary_expr->get_second_op_expr()->is_column())
                {
                    const_expr = dynamic_cast<ObConstRawExpr*>(binary_expr->get_first_op_expr());
                    binaryRef_expr = dynamic_cast<ObBinaryRefRawExpr*>(binary_expr->get_second_op_expr());
                }
                else if(binary_expr->get_second_op_expr()->is_const()
                        && binary_expr->get_first_op_expr()->is_column())
                {                  
                    binaryRef_expr = dynamic_cast<ObBinaryRefRawExpr*>(binary_expr->get_first_op_expr());
                    const_expr = dynamic_cast<ObConstRawExpr*>(binary_expr->get_second_op_expr());
                }
                if(binaryRef_expr == NULL || const_expr == NULL)
                {

                }
                else if(binaryRef_expr->get_first_ref_id() == table_id
                        &&binaryRef_expr ->get_second_ref_id() == column_id)
                {
                    ret = true;
                    break;
                }

            }
        }
        return ret;
    }

    int ObPhysicalOptimizer::gen_joined_column_diff_number(ObLogicalPlan *logical_plan,
                                                           ObPhysicalPlan *physical_plan,
                                                           ErrStat &err_stat,
                                                           ObSelectStmt *select_stmt,
                                                           ObOptimizerRelation *rel_opt,
                                                           const uint64_t table_id,
                                                           const uint64_t column_id,
                                                           ObDiffValueInfo &diff_value_info)
    {
        UNUSED(select_stmt);
        int ret = OB_SUCCESS;
        Selectivity sel1 = 1.0;
        Selectivity sel2 = 1.0;
        if(rel_opt->get_rel_opt_kind() != ObOptimizerRelation::RELOPT_BASEREL)
        {
        }
        else
        {
            ret = gen_clauselist_divided_by_column_id(logical_plan,physical_plan,err_stat,rel_opt,table_id,column_id,sel1,sel2);
        }
        if(OB_SUCCESS != ret)
        {
            ret = OB_SUCCESS;
            diff_value_info.set_default_eastimation(rel_opt->get_join_rows());
        }
        else
        {
            ObColumnStatInfo *column_stat_info = NULL;
            if(rel_opt->get_rel_opt_kind() != ObOptimizerRelation::RELOPT_BASEREL)
            {
                YYSYS_LOG(WARN,"QX rel_opt kind is not RELOPT_BASEREL! kind is %d.",rel_opt->get_rel_opt_kind());
            }
            else
            {
                ret = rel_opt->get_column_stat_info(table_id,column_id,column_stat_info);
            }
            if(OB_SUCCESS != ret || column_stat_info == NULL)
            {
                diff_value_info.set_default_eastimation(rel_opt->get_join_rows());
                ret = OB_SUCCESS;
            }
            else if(sel1 <1 && is_having_equal_expr_in_join_column(rel_opt,table_id,column_id))
            {
                diff_value_info.row_num = 1;
                diff_value_info.total_diff_num = 1;
                diff_value_info.high_freq_diff_num = 1;
                diff_value_info.high_freq_dup_num = rel_opt->get_join_rows();
            }
            else
            {
                rel_opt->print_rel_opt_info();
                double high_diff_num = 0.0,low_diff_num =0.0;
                int high_num =0;
                double high_frequency_count =0.0;
                double p=0.0,q=0.0;
                common::hash::ObHashMap<oceanbase::common::ObObj,double,common::hash::NoPthreadDefendMode>::const_iterator iter
                        = column_stat_info->value_frequency_map_.begin();
                for(;iter!=column_stat_info->value_frequency_map_.end();iter++)
                {
                    high_num++;
                    high_frequency_count += iter->second;
                }
                if((1-sel1)< MIN_FAULT_TOLERANCE && (1-sel2) <MIN_FAULT_TOLERANCE)
                {
                    high_diff_num = high_num;
                    low_diff_num = column_stat_info->distinct_num_ - high_num;
                    q = column_stat_info->avg_frequency_ *rel_opt->get_tuples();
                }
                else
                {
                    iter = column_stat_info->value_frequency_map_.begin();
                    for(;iter!=column_stat_info->value_frequency_map_.end();iter++)
                    {
                        p = rel_opt->get_tuples() * (iter->second);
                        high_diff_num += (1- pow((1-sel1),p))
                                * (1 - pow((1-sel2),p) )
                                * (1 - pow((1 -(rel_opt->get_join_rows() / rel_opt->get_rows())),p))
                                * high_frequency_count;
                    }
                    if(column_stat_info->distinct_num_ - high_num >0
                            && column_stat_info->avg_frequency_ >0)
                    {
                        q= column_stat_info->avg_frequency_ * rel_opt->get_tuples();
                        low_diff_num = (column_stat_info->distinct_num_ -high_num)
                                *(1- pow((1-sel1),q))
                                * (1 - pow((1-sel2),q) )
                                * (1 - pow((1 -(rel_opt->get_join_rows() / rel_opt->get_rows())),q))
                                * (1-high_frequency_count);
                    }
                }
                if(high_frequency_count <1.0)
                {
                    diff_value_info.data_skew = high_frequency_count / (1.0 - high_frequency_count);
                }
                else
                {
                    diff_value_info.data_skew = 0.0;
                }
                if(high_num !=0)
                {
                    diff_value_info.high_freq_diff_num = high_diff_num;
                    diff_value_info.high_freq_dup_num = rel_opt->get_tuples() * high_frequency_count/high_num;
                }
                else
                {
                    diff_value_info.high_freq_diff_num = 0;
                    diff_value_info.high_freq_dup_num = 0;
                }
                diff_value_info.low_freq_diff_num = low_diff_num;
                diff_value_info.low_freq_dup_num = q;
                diff_value_info.total_diff_num = diff_value_info.high_freq_diff_num + diff_value_info.low_freq_diff_num;
                if(diff_value_info.data_skew >DATA_SKEW_SERIOUS_THRESHOLD)
                {
                    double re_diff_num = fmin(column_stat_info->distinct_num_,rel_opt->get_join_rows() / diff_value_info.low_freq_dup_num);
                    diff_value_info.high_freq_diff_num = clamp_row_est(diff_value_info.high_freq_diff_num);
                    if(re_diff_num > diff_value_info.high_freq_diff_num)
                    {
                        diff_value_info.low_freq_diff_num = re_diff_num - diff_value_info.high_freq_diff_num;
                        diff_value_info.high_freq_dup_num = (rel_opt->get_join_rows() -
                                                             diff_value_info.low_freq_diff_num * diff_value_info.low_freq_dup_num) /
                                 diff_value_info.high_freq_diff_num;
                    }
                    diff_value_info.total_diff_num = diff_value_info.high_freq_diff_num+diff_value_info.low_freq_diff_num;
                }
                if(diff_value_info.total_diff_num < MIN_EASTIMATED_ROW)
                {
                    diff_value_info.high_freq_diff_num = MIN_EASTIMATED_ROW;
                    diff_value_info.high_freq_dup_num = rel_opt->get_join_rows() / MIN_EASTIMATED_ROW;
                    diff_value_info.total_diff_num = MIN_EASTIMATED_ROW;
                }
                if(diff_value_info.total_diff_num > rel_opt->get_join_rows())
                {
                    diff_value_info.total_diff_num = rel_opt->get_join_rows();
                    diff_value_info.low_freq_diff_num = diff_value_info.total_diff_num - diff_value_info.high_freq_diff_num;
                    if(diff_value_info.low_freq_diff_num <0)
                    {
                        diff_value_info.high_freq_diff_num += diff_value_info.low_freq_diff_num;
                        diff_value_info.low_freq_diff_num = 0;
                    }
                }
                double rows = diff_value_info.high_freq_diff_num * diff_value_info.high_freq_dup_num +
                        diff_value_info.low_freq_diff_num * diff_value_info.low_freq_dup_num;
                if(rows > rel_opt->get_join_rows())
                {
                    diff_value_info.high_freq_dup_num *= rel_opt->get_join_rows() /rows;
                    diff_value_info.low_freq_dup_num *= rel_opt->get_join_rows() /rows;
                }
                else if(diff_value_info.low_freq_dup_num >0)
                {
                    diff_value_info.low_freq_diff_num += (rel_opt->get_join_rows() - rows) / diff_value_info.low_freq_dup_num;
                    diff_value_info.total_diff_num = diff_value_info.high_freq_diff_num + diff_value_info.low_freq_diff_num;
                }
                diff_value_info.row_num = diff_value_info.high_freq_diff_num * diff_value_info.high_freq_dup_num +
                        diff_value_info.low_freq_diff_num * diff_value_info.low_freq_dup_num;
            }
        }
        if(ObOPtimizerLoger::log_switch_)
        {
            char tmp[256] ={0};
            snprintf(tmp,256,"QOQX gen_joined_column_diff_number end relation diff_num = %.6lf",diff_value_info.total_diff_num);
            ObOPtimizerLoger::print(tmp);
        }
        return ret;
    }

    int ObPhysicalOptimizer::gen_rel_size_estimitates(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat &err_stat,
            ObOptimizerRelation *rel_opt)
    {
        int ret = OB_SUCCESS;
        double nrows;
        double sel =1.0;
        if(OB_SUCCESS == (ret = gen_clauselist_selectivity(logical_plan,physical_plan,err_stat,rel_opt,sel)))
        {
            nrows = rel_opt->get_tuples()* sel;
            rel_opt->set_rows(clamp_row_est(nrows));
            rel_opt->set_join_rows(rel_opt->get_rows());
        }
        return ret;
    }

    int ObPhysicalOptimizer::gen_expr_sub_query_optimization(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat &err_stat,
            ObRawExpr *expr)
    {
        int ret = OB_SUCCESS;
        uint64_t query_id = OB_INVALID_ID;
        ObUnaryRefRawExpr * unaryRef_expr = NULL;
        ObOptimizerRelation *sub_query_relation = NULL;
        void *buf = logical_plan->get_name_pool()->alloc(sizeof(ObOptimizerRelation));
        if(buf == NULL)
        {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            YYSYS_LOG(ERROR,"fail to new ObOptimizerRelation");
        }
        else
        {
            sub_query_relation = new (buf)ObOptimizerRelation();
            sub_query_relation->set_rel_opt_kind(ObOptimizerRelation::RELOPT_INIT);
        }
        if(sub_query_relation != NULL
                && expr != NULL
                && expr->get_expr_type() == T_REF_QUERY)
        {
            unaryRef_expr = (dynamic_cast<ObUnaryRefRawExpr *>(expr));
            if(unaryRef_expr != NULL)
            {
                query_id = unaryRef_expr->get_ref_id();
            }
        }
        ObSelectStmt *select_stmt = NULL;
        if((ret = get_stmt(logical_plan,err_stat,query_id,select_stmt)) != OB_SUCCESS)
        {
            YYSYS_LOG(INFO,"DHC query optimizater  can't find select_stmt");
        }
        else if (unaryRef_expr == NULL)
        {
        }
        else if(OB_SUCCESS != (ret = gen_join_method(logical_plan,physical_plan,err_stat,query_id,sub_query_relation)))
        {
            sub_query_relation->~ObOptimizerRelation();
        }
        else
        {
            oceanbase::common::ObList<ObOptimizerRelation*> * unaryRef_expr_rel_info_list = select_stmt->get_unaryRef_expr_rel_info_list();
            unaryRef_expr_rel_info_list->push_back(sub_query_relation);
        }
        return ret;
    }

    int ObPhysicalOptimizer::gen_clauselist_selectivity(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat &err_stat,
            ObOptimizerRelation *rel_opt,
            double &sel)
    {
        int ret = OB_SUCCESS;
        sel = 1.0;
        oceanbase::common::ObVector<ObSqlRawExpr*>::iterator cnd_it;
        int i=0;
        for(cnd_it = rel_opt->get_base_cnd_list().begin();cnd_it!=rel_opt->get_base_cnd_list().end();cnd_it++)
        {
            ObSelInfo sel_info;
            sel_info.enable_expr_subquery_optimization = true;
            ret = gen_clause_selectivity(logical_plan,physical_plan,err_stat,rel_opt,sel_info,(*cnd_it)->get_expr());
            if(ret != OB_SUCCESS)
            {
                break;
            }
            rel_opt->get_sel_info_array().push_back(sel_info);
            sel *= sel_info.selectivity_;
            if(ObOPtimizerLoger::log_switch_)
            {
                char tmp[256] ={0};
                snprintf(tmp,256,"QOQX gen_clauselist_selectivity i=%d relation selectivity = %.6lf",i++, sel);
                ObOPtimizerLoger::print(tmp);
            }
        }
        return ret;
    }
    int ObPhysicalOptimizer::gen_clause_selectivity(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat &err_stat,
            ObOptimizerRelation *rel_opt,
            ObSelInfo &sel_info,
            ObRawExpr *expr)
    {
        int ret = OB_SUCCESS;
        if(expr == NULL)
        {
            ret = ERROR;
            sel_info.selectivity_ =1.0;
            sel_info.enable = true;
        }
        if(ret != OB_SUCCESS)
        {
        }
        else if(expr->is_const())
        {
            ret = gen_const_selectivity(logical_plan,physical_plan,err_stat,rel_opt,sel_info,expr);
        }
        else if(expr->get_expr_type() == T_OP_EQ || expr->get_expr_type() == T_OP_IS)
        {
            ret = gen_equal_filter_selectivity(logical_plan,physical_plan,err_stat,rel_opt,sel_info,expr);
        }
        else if(expr->get_expr_type() == T_OP_NE || expr->get_expr_type() == T_OP_IS_NOT)
        {
            ret = gen_equal_filter_selectivity(logical_plan,physical_plan,err_stat,rel_opt,sel_info,expr);
            if(sel_info.enable)
            {
                sel_info.selectivity_ = 1.0 - sel_info.selectivity_;
            }
        }
        else if(expr->is_range_filter())
        {
            ret = gen_range_filter_selectivity(logical_plan,physical_plan,err_stat,rel_opt,sel_info,expr);
        }
        else if(expr->get_expr_type() == T_OP_AND ||
                expr->get_expr_type() == T_OP_OR ||
                expr->get_expr_type() == T_OP_NOT)
        {
            ret = gen_bool_filter_selectivity(logical_plan,physical_plan,err_stat,rel_opt,sel_info,expr);
        }
        else if(expr->get_expr_type() == T_OP_LIKE)
        {
            ret = gen_like_selectivity(logical_plan,physical_plan,err_stat,rel_opt,sel_info,expr);
        }
        else if(expr->get_expr_type() == T_OP_NOT_LIKE)
        {
            ret = gen_like_selectivity(logical_plan,physical_plan,err_stat,rel_opt,sel_info,expr);
            if(sel_info.enable)
            {
                sel_info.selectivity_ = 1.0 - sel_info.selectivity_;
            }
        }
        else if(expr->get_expr_type()== T_OP_IN)
        {
            ret = gen_in_selectivity(logical_plan,physical_plan,err_stat,rel_opt,sel_info,expr);
        }
        else if(expr->get_expr_type()== T_OP_NOT_IN)
        {
            ret = gen_in_selectivity(logical_plan,physical_plan,err_stat,rel_opt,sel_info,expr);
            if(sel_info.enable)
            {
                sel_info.selectivity_ = 1.0 - sel_info.selectivity_;
            }
        }
        else
        {
            sel_info.selectivity_ = 1.0;
            sel_info.enable= true;
        }
        return ret;
    }

    int ObPhysicalOptimizer::gen_const_selectivity(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat &err_stat,
            ObOptimizerRelation *rel_opt,
            ObSelInfo &sel_info,
            ObRawExpr *expr)
    {
        UNUSED(logical_plan);
        UNUSED(physical_plan);
        UNUSED(err_stat);
        UNUSED(rel_opt);
        int ret= OB_SUCCESS;
        ObConstRawExpr* const_expr = NULL;
        const oceanbase::common::ObObj *obj = NULL;
        const_expr = dynamic_cast<ObConstRawExpr*>(expr);
        if(const_expr!= NULL)
        {
            obj = &(const_expr->get_value());
        }
        else
        {
            YYSYS_LOG(WARN,"QX const_expr is NULL.");
        }
        double sel =0.0;
        if(obj && obj->is_true())
        {
            sel = 1.0;
        }

        sel_info.enable= true;
        sel_info.selectivity_ = sel;
        return ret;

    }
    int ObPhysicalOptimizer::gen_equal_filter_selectivity(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat &err_stat,
            ObOptimizerRelation *rel_opt,
            ObSelInfo &sel_info,
            ObRawExpr *expr)
    {
        int ret = OB_SUCCESS;
        double sel = DEFAULT_EQ_SEL;
        ObConstRawExpr *const_expr = NULL;
        ObBinaryRefRawExpr *binaryRef_expr = NULL;
        ObBinaryOpRawExpr *binary_expr = dynamic_cast<ObBinaryOpRawExpr*>(expr);
        bool eq_sub_query =false;
        if(binary_expr !=NULL)
        {
            if(binary_expr->get_first_op_expr()->is_const())
            {
                const_expr = dynamic_cast<ObConstRawExpr*>(binary_expr->get_first_op_expr());
                binaryRef_expr = dynamic_cast<ObBinaryRefRawExpr*>(binary_expr->get_second_op_expr());
            }
            else if(binary_expr->get_second_op_expr()->is_const())
            {
                const_expr = dynamic_cast<ObConstRawExpr*>(binary_expr->get_second_op_expr());
                binaryRef_expr = dynamic_cast<ObBinaryRefRawExpr*>(binary_expr->get_first_op_expr());
            }
            else if(binary_expr->get_first_op_expr()->is_column()
                    &&binary_expr->get_second_op_expr()->is_sub_query())
            {
                binaryRef_expr = dynamic_cast<ObBinaryRefRawExpr*>(binary_expr->get_first_op_expr());
                eq_sub_query = true;
            }
            if(eq_sub_query && binaryRef_expr != NULL)
            {
                sel_info.table_id_ = binaryRef_expr->get_first_ref_id();
                sel_info.columun_id_ = binaryRef_expr->get_second_ref_id();
                ObStatSelCalculator::get_equal_subquery_selectivity(rel_opt,sel_info);
                if(sel_info.enable_expr_subquery_optimization)
                {
                    ret = gen_expr_sub_query_optimization(logical_plan,physical_plan,err_stat,binary_expr->get_second_op_expr());
                    if(ret != OB_SUCCESS)
                    {
                    }
                }
            }
            else if(const_expr != NULL && binaryRef_expr != NULL)
            {
                sel_info.table_id_ = binaryRef_expr->get_first_ref_id();
                sel_info.columun_id_ = binaryRef_expr->get_second_ref_id();
                ObStatSelCalculator::get_equal_selectivity(rel_opt,sel_info,const_expr->get_value());

            }
            else
            {
                sel_info.selectivity_ = DEFAULT_EQ_SEL;
                sel_info.enable = true;
            }
        }
        else
        {
            sel_info.selectivity_ = DEFAULT_EQ_SEL;
            sel_info.enable = true;
        }
        sel =sel_info.selectivity_;
        return ret;
    }

    int ObPhysicalOptimizer::gen_bool_filter_selectivity(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat &err_stat,
            ObOptimizerRelation *rel_opt,
            ObSelInfo &sel_info,
            ObRawExpr *expr)
    {
        int ret = OB_SUCCESS;
        double sel = 0.5;
        double sel1=0,sel2=0;
        ObSelInfo sel_info1,sel_info2;
        sel_info1.enable_expr_subquery_optimization = true;
        sel_info2.enable_expr_subquery_optimization = true;
        if(expr ==NULL)
        {
            sel_info.enable = true;
            YYSYS_LOG(WARN,"QX expr is NULL.");
        }
        else
        {
            switch (expr->get_expr_type())
            {
            case T_OP_AND:
            {
                ObBinaryOpRawExpr *binary_expr = dynamic_cast<ObBinaryOpRawExpr*>(expr);
                if(binary_expr != NULL)
                {
                    sel_info1.table_id_ = sel_info.table_id_;
                    sel_info2.table_id_ = sel_info.table_id_;
                    sel_info1.columun_id_ = sel_info.columun_id_;
                    sel_info2.columun_id_ = sel_info.columun_id_;
                    if(OB_SUCCESS != (ret =gen_clause_selectivity(logical_plan,physical_plan,err_stat,rel_opt,sel_info1,binary_expr->get_first_op_expr())))
                    {
                        break;
                    }
                    else if(OB_SUCCESS != (ret =gen_clause_selectivity(logical_plan,physical_plan,err_stat,rel_opt,sel_info2,binary_expr->get_second_op_expr())))
                    {
                        break;
                    }
                    if(sel_info1.enable &&sel_info2.enable)
                    {
                        sel1 = sel_info1.selectivity_;
                        sel2 = sel_info2.selectivity_;
                        sel = sel1*sel2;
                        sel_info.enable = true;
                    }
                    else
                    {
                        sel =1.0;
                        sel_info.enable =false;
                    }
                }
                else
                {
                    sel = 1.0;
                    sel_info.enable = true;
                    YYSYS_LOG(WARN,"QX binary_expr is NULL.");
                }
                break;
            }
               case T_OP_OR:
            {
                ObBinaryOpRawExpr *binary_expr = dynamic_cast<ObBinaryOpRawExpr*>(expr);
                if(binary_expr != NULL)
                {
                    sel_info1.table_id_ = sel_info.table_id_;
                    sel_info2.table_id_ = sel_info.table_id_;
                    sel_info1.columun_id_ = sel_info.columun_id_;
                    sel_info2.columun_id_ = sel_info.columun_id_;
                    if(OB_SUCCESS != (ret =gen_clause_selectivity(logical_plan,physical_plan,err_stat,rel_opt,sel_info1,binary_expr->get_first_op_expr())))
                    {
                        break;
                    }
                    else if(OB_SUCCESS != (ret =gen_clause_selectivity(logical_plan,physical_plan,err_stat,rel_opt,sel_info2,binary_expr->get_second_op_expr())))
                    {
                        break;
                    }
                    if(sel_info1.enable &&sel_info2.enable)
                    {
                        sel1 = sel_info1.selectivity_;
                        sel2 = sel_info2.selectivity_;
                        sel = sel1+sel2-(sel1*sel2);
                        sel_info.enable = true;
                    }
                    else
                    {
                        sel =1.0;
                        sel_info.enable =false;
                    }
                }
                else
                {
                    sel = 1.0;
                    sel_info.enable = true;
                }
                break;
            }
            case T_OP_NOT:
            {
                ObUnaryOpRawExpr *unary_expr = dynamic_cast<ObUnaryOpRawExpr*>(expr);
                if(unary_expr != NULL)
                {
                    if(OB_SUCCESS != (ret =gen_clause_selectivity(logical_plan,physical_plan,err_stat,rel_opt,sel_info,unary_expr->get_op_expr())))
                    {
                        break;
                    }
                    else if(sel_info.enable)
                    {
                        sel = 1.0-sel_info.selectivity_;
                    }
                    else
                    {
                        sel = 1.0;
                    }
                }
                else
                {
                    sel = 1.0;
                    sel_info.enable = true;
                }
                break;
            }
            default:
                break;
            }
        }
        sel_info.selectivity_ = sel;
        return ret;
    }

    int ObPhysicalOptimizer::gen_btw_selectivity(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat &err_stat,
            ObOptimizerRelation *rel_opt,
            ObSelInfo &sel_info,
            ObRawExpr *expr)
    {
        UNUSED(logical_plan);
        UNUSED(physical_plan);
        UNUSED(err_stat);
        int ret = OB_SUCCESS;
        double sel = DEFAULT_RANGE_INEQ_SEL;
        ObConstRawExpr *const_expr1 = NULL;
        ObConstRawExpr *const_expr2 = NULL;
        ObBinaryRefRawExpr *binaryRef_expr = NULL;
        ObTripleOpRawExpr *tripleop_expr = dynamic_cast<ObTripleOpRawExpr*>(expr);
        if(tripleop_expr == NULL)
        {
            sel_info.selectivity_ =1.0;
            sel_info.enable = true;
            YYSYS_LOG(WARN,"QX tripleop_expr is null.");
        }
        else if((binaryRef_expr = dynamic_cast<ObBinaryRefRawExpr*>(tripleop_expr->get_first_op_expr())) &&
                 (const_expr1 = dynamic_cast<ObConstRawExpr*>(tripleop_expr->get_second_op_expr())) &&
                 (const_expr2 = dynamic_cast<ObConstRawExpr*>(tripleop_expr->get_third_op_expr())))
        {
            sel_info.table_id_ = binaryRef_expr->get_first_ref_id();
            sel_info.columun_id_ = binaryRef_expr->get_second_ref_id();
            ObStatSelCalculator::get_btw_selectivity(rel_opt,sel_info,
                                                     const_expr1->get_value(),const_expr2->get_value());
        }
        else
        {
            sel_info.selectivity_ =1.0;
            sel_info.enable = true;
            YYSYS_LOG(WARN, "QX some exprs is NULL");
        }
        sel = sel_info.selectivity_ ;
        return ret;
    }

    int ObPhysicalOptimizer::gen_range_filter_selectivity(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat &err_stat,
            ObOptimizerRelation *rel_opt,
            ObSelInfo &sel_info,
            ObRawExpr *expr)
    {
        int ret = OB_SUCCESS;
        double sel = DEFAULT_INEQ_SEL;
        bool reverse = false;
        ObConstRawExpr *const_expr = NULL;
        ObBinaryRefRawExpr *binaryRef_expr = NULL;
        ObBinaryOpRawExpr *binary_expr = NULL;
        if(expr == NULL)
        {
            sel_info.enable = true;
            YYSYS_LOG(WARN,"QX expr is NULL.");
        }
        else
        {
            ObItemType type = expr->get_expr_type();
            switch (type)
            {
            case T_OP_LE:
            case T_OP_LT:
            case T_OP_GE:
            case T_OP_GT:
            {
                if(NULL == (binary_expr = dynamic_cast<ObBinaryOpRawExpr*>(expr)))
                {
                }
                else if(binary_expr->get_first_op_expr()->get_expr_type() == T_REF_COLUMN &&
                        binary_expr->get_second_op_expr()->get_expr_type() == T_REF_COLUMN)
                {
                    //expr is join condition
                }
                else if(binary_expr->get_first_op_expr()->get_expr_type() == T_REF_COLUMN)
                {
                    const_expr = dynamic_cast<ObConstRawExpr*>(binary_expr->get_second_op_expr());
                    binaryRef_expr = dynamic_cast<ObBinaryRefRawExpr*>(binary_expr->get_first_op_expr());
                }
                else
                {
                    const_expr = dynamic_cast<ObConstRawExpr*>(binary_expr->get_first_op_expr());
                    binaryRef_expr = dynamic_cast<ObBinaryRefRawExpr*>(binary_expr->get_second_op_expr());
                    reverse = true;
                }
                if(const_expr == NULL || binaryRef_expr == NULL)
                {
                    sel_info.selectivity_ = DEFAULT_INEQ_SEL;
                    sel = sel_info.selectivity_ ;
                    sel_info.enable = true;
                    if(binary_expr != NULL && sel_info.enable_expr_subquery_optimization)
                    {
                        if(binary_expr->get_first_op_expr()->get_expr_type() == T_REF_QUERY)
                        {
                            ret = gen_expr_sub_query_optimization(logical_plan,physical_plan,err_stat,binary_expr->get_first_op_expr());
                            if(ret != OB_SUCCESS)
                            {
                                break;
                            }
                        }
                        if(binary_expr->get_second_op_expr()->get_expr_type() == T_REF_QUERY)
                        {
                            ret = gen_expr_sub_query_optimization(logical_plan,physical_plan,err_stat,binary_expr->get_second_op_expr());
                            if(ret != OB_SUCCESS)
                            {
                                break;
                            }
                        }
                    }
                    break;
                }
                else
                {
                    sel_info.table_id_ = binaryRef_expr->get_first_ref_id();
                    sel_info.columun_id_ = binaryRef_expr->get_second_ref_id();
                }
                if((type == T_OP_LE && !reverse) ||(type == T_OP_GE && reverse))
                {
                    ObStatSelCalculator::get_le_selectivity(rel_opt,sel_info,const_expr->get_value());
                }
                else if((type == T_OP_LT && !reverse) ||(type == T_OP_GT && reverse))
                {
                    ObStatSelCalculator::get_lt_selectivity(rel_opt,sel_info,const_expr->get_value());
                }
                else if((type == T_OP_GE && !reverse) ||(type == T_OP_LE && reverse))
                {
                    ObStatSelCalculator::get_lt_selectivity(rel_opt,sel_info,const_expr->get_value());
                    if(sel_info.enable)
                    {
                        sel_info.selectivity_ = 1.0-sel_info.selectivity_;
                    }
                }
                else
                {
                    ObStatSelCalculator::get_le_selectivity(rel_opt,sel_info,const_expr->get_value());
                    if(sel_info.enable)
                    {
                        sel_info.selectivity_ = 1.0-sel_info.selectivity_;
                    }
                }
                sel = sel_info.selectivity_;
                break;
            }

            case T_OP_BTW:
            {
                ret = gen_btw_selectivity(logical_plan,physical_plan,err_stat,rel_opt,sel_info,expr);
                if(ret != OB_SUCCESS)
                {
                    break;
                }
                sel = sel_info.selectivity_;
                break;
            }
            case T_OP_NOT_BTW:
            {
                ret = gen_btw_selectivity(logical_plan,physical_plan,err_stat,rel_opt,sel_info,expr);
                if(ret != OB_SUCCESS)
                {
                    break;
                }
                else if(sel_info.enable)
                {
                    sel_info.selectivity_ = 1.0-sel_info.selectivity_;
                }
                sel = sel_info.selectivity_;
                break;
            }

            default:
                break;
            }
        }
        return ret;
    }

    int ObPhysicalOptimizer::gen_like_selectivity(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat &err_stat,
            ObOptimizerRelation *rel_opt,
            ObSelInfo &sel_info,
            ObRawExpr *expr)
    {
        int ret = OB_SUCCESS;
        double sel = DEFAULT_MATCH_SEL;
        ObConstRawExpr *const_expr = NULL;
        ObBinaryRefRawExpr *binaryRef_expr = NULL;
        ObBinaryOpRawExpr *binary_expr = dynamic_cast<ObBinaryOpRawExpr*>(expr);
        if(binary_expr == NULL)
        {
            sel_info.selectivity_ = 1.0;
            sel_info.enable = true;
            YYSYS_LOG(WARN,"QX binary_expr is NULL.");
        }
        else
        {
            const_expr = dynamic_cast<ObConstRawExpr*>(binary_expr->get_second_op_expr());
            binaryRef_expr = dynamic_cast<ObBinaryRefRawExpr*>(binary_expr->get_first_op_expr());
        }
        if(const_expr == NULL || binaryRef_expr == NULL)
        {
            sel_info.selectivity_ = DEFAULT_MATCH_SEL;
                        sel_info.enable = true;
        if(binary_expr != NULL && sel_info.enable_expr_subquery_optimization)
        {
            if(binary_expr->get_second_op_expr()->get_expr_type() == T_REF_QUERY)
            {
                ret = gen_expr_sub_query_optimization(logical_plan,physical_plan,err_stat,binary_expr->get_second_op_expr());
                if(ret != OB_SUCCESS)
                {
                    //break;
                }
            }

        }
        }
        else
        {
            sel_info.table_id_ = binaryRef_expr->get_first_ref_id();
            sel_info.columun_id_ = binaryRef_expr->get_second_ref_id();
            ObStatSelCalculator::get_like_selectivity(rel_opt,sel_info,const_expr->get_value());
        }
        sel = sel_info.selectivity_;
        return ret;
    }

    int ObPhysicalOptimizer::gen_in_selectivity(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat &err_stat,
            ObOptimizerRelation *rel_opt,
            ObSelInfo &sel_info,
            ObRawExpr *expr)
    {
        int ret = OB_SUCCESS;
        double sel = DEFAULT_MATCH_SEL;
        ObBinaryOpRawExpr *in_expr = NULL;
        ObMultiOpRawExpr *row_expr ;
        ObBinaryRefRawExpr *binaryref_expr = NULL;
        in_expr = dynamic_cast<ObBinaryOpRawExpr*>(expr);
        if(in_expr == NULL)
        {
            sel_info.selectivity_ = 1.0;
            sel_info.enable = true;
        }
        else if(in_expr->get_first_op_expr()->get_expr_type() == T_REF_COLUMN &&
                in_expr->get_second_op_expr()->get_expr_type() == T_OP_ROW)
        {
            binaryref_expr = dynamic_cast<ObBinaryRefRawExpr*>(in_expr->get_first_op_expr());
            row_expr = dynamic_cast<ObMultiOpRawExpr*>(in_expr->get_second_op_expr());
            if(binaryref_expr == NULL || row_expr == NULL)
            {
                sel_info.selectivity_ = DEFAULT_MATCH_SEL;
                            sel_info.enable = true;
            }
            else
            {
                ObArray<common::ObObj> value_array;
                ObConstRawExpr *const_expr = NULL;
                int32_t i =0;
                for(;i<row_expr->get_expr_size();i++)
                {
                    const_expr = dynamic_cast<ObConstRawExpr*>(row_expr->get_op_expr(i));
                    if(const_expr == NULL)
                    {
                        YYSYS_LOG(WARN,"QX const_expr is NULL.");
                    }
                    else
                    {
                        value_array.push_back(const_expr->get_value());
                    }
                }
                if(row_expr->get_expr_size() == 0)
                {
                    sel_info.selectivity_ = 0.0;
                    sel_info.enable = true;
                }
                else
                {
                    sel_info.table_id_ = binaryref_expr->get_first_ref_id();
                    sel_info.columun_id_ = binaryref_expr->get_second_ref_id();
                    ObStatSelCalculator::get_in_selectivity(rel_opt,sel_info,
                                                            value_array);
                }
            }
        }
        else if(in_expr->get_first_op_expr()->get_expr_type() == T_REF_COLUMN &&
                in_expr->get_second_op_expr()->get_expr_type() == T_REF_QUERY)
        {
            sel_info.selectivity_ = DEFAULT_MATCH_SEL;
                        sel_info.enable = true;
                        if(sel_info.enable_expr_subquery_optimization)
                        {
                            ret = gen_expr_sub_query_optimization(logical_plan,physical_plan,err_stat,in_expr->get_second_op_expr());
                            if(ret != OB_SUCCESS)
                            {
                                //break;
                            }
                        }
        }
        else
        {
            sel_info.selectivity_ = DEFAULT_MATCH_SEL;
                        sel_info.enable = true;
        }
        sel = sel_info.selectivity_;
        return ret;
    }
    int ObPhysicalOptimizer::gen_rel_scan_costs(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat &err_stat,
            ObSelectStmt *select_stmt,
            ObOptimizerRelation *rel_opt)
    {
        int ret = OB_SUCCESS;
        if(rel_opt->get_rel_opt_kind() == ObOptimizerRelation::RELOPT_BASEREL)
        {
            if(OB_SUCCESS != (ret = gen_cost_seq_scan(logical_plan,physical_plan,err_stat,select_stmt,rel_opt)))
            {

            }
            else if(OB_SUCCESS != (ret = gen_cost_index_scan(logical_plan,physical_plan,err_stat,select_stmt,rel_opt)))
            {

            }
        }
        else
        {
            YYSYS_LOG(DEBUG,"QX WARN: gen index scan cost fail.");
        }
        return ret;
    }

    int ObPhysicalOptimizer::gen_cost_seq_scan(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat &err_stat,
            ObSelectStmt *select_stmt,
            ObOptimizerRelation *rel_opt)
    {
        UNUSED(logical_plan);
        UNUSED(physical_plan);
        UNUSED(err_stat);
        int ret =OB_SUCCESS;
        Cost total_cost = 0.0;
        UNUSED(total_cost);
        OB_ASSERT(rel_opt->get_rel_opt_kind() == ObOptimizerRelation::RELOPT_BASEREL);
        ret = ObStatCostCalculator::get_cost_seq_scan(select_stmt,rel_opt,total_cost);
        if(ret == OB_SUCCESS)
        {
            rel_opt->set_seq_scan_cost(total_cost);
        }
        else
        {

        }
        return ret;
    }

    int ObPhysicalOptimizer::gen_cost_index_scan(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat &err_stat,
            ObSelectStmt *select_stmt,
            ObOptimizerRelation *rel_opt)
    {
        int ret = OB_SUCCESS;
        double sel1 = 1.0;
        double sel2 = 1.0;
        OB_ASSERT(rel_opt->get_rel_opt_kind() == ObOptimizerRelation::RELOPT_BASEREL);
        bool enable_index = false;
        ObArray<ObIndexTableInfo> can_used_index_table_array;
        enable_index = transformer_->is_enable_index_for_one_table(logical_plan,physical_plan,select_stmt,rel_opt,
                                                                    rel_opt->get_table_id(),can_used_index_table_array);
        if(enable_index)
        {
            for(int i=0;i<can_used_index_table_array.count();i++)
            {
                sel1 = 1.0;
                sel2 = 1.0;
                gen_clauselist_divided_by_column_id(logical_plan,
                                                    physical_plan,
                                                    err_stat,
                                                    rel_opt,
                                                    rel_opt->get_table_id(),
                                                    can_used_index_table_array.at(i).index_column_id_,
                                                    sel1,
                                                    sel2);
                ret = ObStatCostCalculator::get_cost_index_scan(select_stmt,
                                                                rel_opt,
                                                                can_used_index_table_array.at(i),
                                                                sel1);
                if(ret == OB_SUCCESS)
                {
                    rel_opt->get_index_table_array().push_back(can_used_index_table_array.at(i));
                }
            }
        }
        ret = OB_SUCCESS;
        return ret;
    }

    int ObPhysicalOptimizer::gen_join_method(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat &err_stat,
            const uint64_t &query_id,
            ObOptimizerRelation *&sub_query_relation
            )
    {
        int &ret = err_stat.err_code_ = OB_SUCCESS;
        bool is_system_table = false;
        ObSelectStmt *select_stmt = NULL;
        ObOptJoinParam joinpar = join_ctx_;
        if((ret = get_stmt(logical_plan,err_stat,query_id,select_stmt))!= OB_SUCCESS)
        {
            YYSYS_LOG(INFO,"Query optimizater can't find select_stmt");
        }
        else if(select_stmt->is_for_update())
        {
            sub_query_relation->set_rel_opt_kind(ObOptimizerRelation::RELOPT_SELECT_FOR_UPDATE);
        }
        else
        {
            OB_ASSERT(select_stmt);
            ObSelectStmt::SetOperator set_type = select_stmt->get_set_op();
            if(set_type != ObSelectStmt::NONE)
            {
                ObOptimizerRelation *left_sub_query_relation = NULL;
                void *buf = logical_plan->get_name_pool()->alloc(sizeof(ObOptimizerRelation));
                if(buf ==NULL )
                {
                    ret = OB_ALLOCATE_MEMORY_FAILED;
                    YYSYS_LOG(ERROR, "fail to new ObOptimizerRelation");
                }
                else
                {
                    left_sub_query_relation = new (buf) ObOptimizerRelation(ObOptimizerRelation::RELOPT_INIT);
                }
                ObOptimizerRelation *right_sub_query_relation = NULL;
                buf = logical_plan->get_name_pool()->alloc(sizeof(ObOptimizerRelation));
                if(buf ==NULL )
                {
                    ret = OB_ALLOCATE_MEMORY_FAILED;
                    YYSYS_LOG(ERROR, "fail to new ObOptimizerRelation");
                }
                else
                {
                    right_sub_query_relation = new (buf) ObOptimizerRelation(ObOptimizerRelation::RELOPT_INIT);
                }

                if(ret == OB_SUCCESS)
                {
                    ObSelectStmt *union_left_select_stmt = NULL;
                    if((ret = get_stmt(logical_plan,err_stat,select_stmt->get_left_query_id(),union_left_select_stmt))!= OB_SUCCESS)
                    {
                        YYSYS_LOG(INFO,"DHC query optimizater can't find select_stmt");
                    }
                    else
                    {
                        ret = gen_join_method(logical_plan,physical_plan,err_stat,select_stmt->get_left_query_id(),left_sub_query_relation);
                    }
                    if(ret == OB_SUCCESS)
                    {
                        ObOptimizerRelation* tmp = union_left_select_stmt->get_select_stmt_rel_info();
                        if(tmp)
                        {
                            delete tmp;
                        }
                        union_left_select_stmt->set_select_stmt_rel_info(left_sub_query_relation);
                    }
                    else
                    {
                        left_sub_query_relation->~ObOptimizerRelation();
                        YYSYS_LOG(INFO,"DHC err gen join method/subquery planer");
                    }
                    if(ObOPtimizerLoger::log_switch_)
                    {
                        const FILE* file = ObOPtimizerLoger::getFile();
                        left_sub_query_relation->print_rel_opt_info(file);
                        ObOPtimizerLoger::closeFile(file);
                    }
                }
                if(ret == OB_SUCCESS)
                {
                    ObSelectStmt *union_right_select_stmt = NULL;
                    if((ret = get_stmt(logical_plan,err_stat,select_stmt->get_right_query_id(),union_right_select_stmt))!= OB_SUCCESS)
                    {
                        YYSYS_LOG(INFO,"DHC query optimizater can't find select_stmt");
                    }
                    else
                    {
                        ret = gen_join_method(logical_plan,physical_plan,err_stat,select_stmt->get_right_query_id(),right_sub_query_relation);
                    }
                    if(ret == OB_SUCCESS)
                    {
                        ObOptimizerRelation* tmp = union_right_select_stmt->get_select_stmt_rel_info();
                        if(tmp)
                        {
                            delete tmp;
                        }
                        union_right_select_stmt->set_select_stmt_rel_info(right_sub_query_relation);
                    }
                    else
                    {
                        right_sub_query_relation->~ObOptimizerRelation();
                        YYSYS_LOG(INFO,"DHC err gen join method/subquery planer");
                    }
                    if(ObOPtimizerLoger::log_switch_)
                    {
                        const FILE* file = ObOPtimizerLoger::getFile();
                        right_sub_query_relation->print_rel_opt_info(file);
                        ObOPtimizerLoger::closeFile(file);
                    }
                }
                oceanbase::common::ObList<ObOptimizerRelation*> * subquery_rel_opt_list = select_stmt->get_subquery_rel_opt_list();
                subquery_rel_opt_list->push_back(left_sub_query_relation);
                subquery_rel_opt_list->push_back(right_sub_query_relation);
                if(ret == OB_SUCCESS)
                {
                    switch (set_type)
                    {
                    case ObSelectStmt::UNION:
                    {
                        sub_query_relation->set_rows(left_sub_query_relation->get_rows()+right_sub_query_relation->get_rows());
                        sub_query_relation->set_join_rows(left_sub_query_relation->get_join_rows()+right_sub_query_relation->get_join_rows());
                        sub_query_relation->set_tuples(left_sub_query_relation->get_tuples()+right_sub_query_relation->get_tuples());
                        break;
                    }
                    case ObSelectStmt::INTERSECT:
                    {
                        sub_query_relation->set_rows(right_sub_query_relation->get_rows());
                        sub_query_relation->set_join_rows(right_sub_query_relation->get_join_rows());
                        sub_query_relation->set_tuples(right_sub_query_relation->get_tuples());
                        break;
                    }
                    case ObSelectStmt::EXCEPT:
                    {
                        sub_query_relation->set_rows(left_sub_query_relation->get_rows());
                        sub_query_relation->set_join_rows(left_sub_query_relation->get_join_rows());
                        sub_query_relation->set_tuples(left_sub_query_relation->get_tuples());
                        break;
                    }
                    default:
                        break;
                    }
                    if(select_stmt->is_set_distinct())
                    {

                    }
                }
            }
            else
            {
                int32_t num_table = select_stmt->get_from_item_size();
                if(num_table<=0)
                {

                }
                else
                {
                    ret = gen_rel_opts(logical_plan,physical_plan,err_stat,query_id);
                    num_table = select_stmt->get_from_item_size();
                    ObBitSet<> bit_set;
                    ObList<ObBitSet<> > where_bit_set;
                    for(int32_t i=0;ret== OB_SUCCESS &&i<num_table;i++)
                    {
                        FromItem& from_item = const_cast<FromItem&>(select_stmt->get_from_item(i));
                        if(from_item.is_joined_ == false)
                        {
                            if(from_item.table_id_ == OB_INVALID_ID || from_item.table_id_ <= common::OB_APP_MIN_TABLE_ID +2000)
                            {
                                is_system_table = true;
                                break;
                            }
                            bit_set.add_member(select_stmt->get_table_bit_index(from_item.table_id_));
                            bool is_sub_query_table =false;
                            ObOptimizerRelation* from_item_query_relation = NULL;
                            ret = find_rel_info(logical_plan,physical_plan,err_stat,select_stmt,from_item.table_id_,from_item_query_relation,is_sub_query_table);
                            if(ret == OB_SUCCESS)
                            {
                                from_item.from_item_rel_opt_ = from_item_query_relation;
                            }
                            else
                            {
                                YYSYS_LOG(INFO,"DHC find ret info error");
                                break;
                            }
                            if(ObOPtimizerLoger::log_switch_)
                            {
                                const FILE* file = ObOPtimizerLoger::getFile();
                                from_item_query_relation->print_rel_opt_info(file);
                                ObOPtimizerLoger::closeFile(file);
                            }
                        }
                        else
                        {
                            ObOptimizerRelation* joined_from_item_rel_info = NULL;
                            void *buf = logical_plan->get_name_pool()->alloc(sizeof(ObOptimizerRelation));
                            if(buf == NULL)
                            {
                                ret = OB_ALLOCATE_MEMORY_FAILED;
                            }
                            else
                            {
                                joined_from_item_rel_info = new(buf) ObOptimizerRelation(ObOptimizerRelation::RELOPT_JOINREL);
                                joined_from_item_rel_info->set_tuples(0.0);
                                joined_from_item_rel_info->set_rows(0.0);
                                joined_from_item_rel_info->set_join_rows(1.0);
                                joined_from_item_rel_info->set_table_id(i);
                                from_item.from_item_rel_opt_ = joined_from_item_rel_info;
                                oceanbase::common::ObList<ObOptimizerRelation*>* joined_from_item_rel_info_list = select_stmt->get_joined_from_item_rel_info_list();
                                joined_from_item_rel_info_list->push_back(joined_from_item_rel_info);
                            }
                            JoinedTable *joined_table = select_stmt->get_joined_table(from_item.table_id_);
                            if(joined_table == NULL)
                            {
                                ret = OB_ERR_ILLEGAL_ID;
                                break;
                            }
                            OB_ASSERT(joined_table->table_ids_.count() >=2);
                            OB_ASSERT(joined_table->table_ids_.count() - 1 == joined_table->join_types_.count());

                            if(is_system_table || joined_table->table_ids_.at(0) == OB_INVALID_ID|| joined_table->table_ids_.at(0) <3000)
                            {
                                is_system_table = true;
                                break;
                            }
                            bit_set.add_member(select_stmt->get_table_bit_index(joined_table->table_ids_.at(0)));
                            ObSqlRawExpr *join_expr = NULL;
                            int64_t join_expr_position = 0;
                            int64_t join_expr_num = 0;
                            for(int32_t j=1; ret == OB_SUCCESS && j< joined_table->table_ids_.count();j++)
                            {
                                ObList<ObBitSet<> > outer_join_bitsets;
                                if(OB_SUCCESS != (ret = outer_join_bitsets.push_back(bit_set)))
                                {
                                    YYSYS_LOG(WARN,"fail to push bitset to list. ret=%d", ret);
                                    break;
                                }
                                ObBitSet<> right_table_bitset;
                                ObList<ObSqlRawExpr*> outer_join_cnds;
                                if(is_system_table || joined_table->table_ids_.at(j) == OB_INVALID_ID || joined_table->table_ids_.at(j) <3000)
                                {
                                    is_system_table = true;
                                    break;
                                }
                                int32_t right_table_bit_index = select_stmt->get_table_bit_index(joined_table->table_ids_.at(j));
                                right_table_bitset.add_member(right_table_bit_index);
                                if(OB_SUCCESS != (ret = outer_join_bitsets.push_back(right_table_bitset)))
                                {
                                    YYSYS_LOG(WARN,"fail to push bitset to list. ret=%d", ret);
                                    break;
                                }
                                bit_set.add_member(right_table_bit_index);
                                join_expr_num = joined_table->expr_nums_per_join_.at(j-1);
                                for(int64_t join_index =0; join_index<join_expr_num;++join_index)
                                {
                                    join_expr = logical_plan->get_expr(joined_table->expr_ids_.at(join_expr_position + join_index));
                                    if(join_expr == NULL)
                                    {
                                        ret = OB_ERR_ILLEGAL_INDEX;
                                        break;
                                    }
                                    else if(OB_SUCCESS != (ret = outer_join_cnds.push_back(join_expr)))
                                    {
                                        YYSYS_LOG(WARN,"fail to push bitset to list. ret=%d", ret);
                                        break;
                                    }
                                }
                                if(OB_SUCCESS == ret)
                                {
                                    join_expr_position += join_expr_num;
                                }
                                else
                                {
                                    break;
                                }

                                ret = Opt_Calc_JoinedTables_Cost(logical_plan,physical_plan,err_stat,select_stmt,joined_table,outer_join_cnds,outer_join_bitsets,from_item.from_item_rel_opt_,j);
                                if(ret == OB_SUCCESS)
                                {
                                    from_item.from_item_rel_opt_->set_rel_opt_kind(ObOptimizerRelation::RELOPT_JOINREL);
                                    from_item.from_item_rel_opt_->print_rel_opt_info();
                                }
                                else
                                {
                                    YYSYS_LOG(WARN,"DHC Opt_Calc_JoinedTables_Cost err ret=%d", ret);
                                    break;
                                }
                                if(ObOPtimizerLoger::log_switch_)
                                {
                                    const FILE* file = ObOPtimizerLoger::getFile();
                                    from_item.from_item_rel_opt_->print_rel_opt_info(file);
                                    ObOPtimizerLoger::closeFile(file);
                                }
                            }
                        }

                        if(ret == OB_SUCCESS && (ret = where_bit_set.push_back(bit_set)) != OB_SUCCESS)
                        {
                            break;
                        }
                        bit_set.clear();
                    }
                    if(ret == OB_SUCCESS && num_table == 1 && !is_system_table)
                    {
                        FromItem& from_item = const_cast<FromItem&> (select_stmt->get_from_item(0));
                        if(from_item.from_item_rel_opt_)
                        {
                            sub_query_relation->set_tuples(from_item.from_item_rel_opt_->get_tuples());
                            sub_query_relation->set_rows(from_item.from_item_rel_opt_->get_rows());
                            sub_query_relation->set_join_rows(from_item.from_item_rel_opt_->get_join_rows());
                        }
                        else
                        {
                            YYSYS_LOG(INFO,"DHC from_item is null");
                        }
                    }
                    else if(OB_SUCCESS == ret && !is_system_table)
                    {
                        //int32_t num = (int32_t)where_bit_set.size();
                        ObList<ObSqlRawExpr*> remainder_where_cnd_list;
                        int32_t condition_num = select_stmt->get_condition_size();
                        for(int32_t i=0; ret== OB_SUCCESS && i<condition_num;i++)
                        {
                            uint64_t expr_id = select_stmt->get_condition_id(i);
                            ObSqlRawExpr* where_expr = logical_plan->get_expr(expr_id);
                            if(where_expr && where_expr->get_expr()->is_join_cond() == true
                                    && (ret = remainder_where_cnd_list.push_back(where_expr)) != OB_SUCCESS)
                            {
                                break;
                            }
                        }
                        if(ret == OB_SUCCESS && remainder_where_cnd_list.size()>0)
                        {
                            ret = Opt_Calc_FromItem_Cost(logical_plan,physical_plan,err_stat,select_stmt,remainder_where_cnd_list,sub_query_relation,where_bit_set);
                            if(ret == OB_SUCCESS)
                            {
                                if(ObOPtimizerLoger::log_switch_)
                                {
                                    const FILE* file = ObOPtimizerLoger::getFile();
                                    sub_query_relation->print_rel_opt_info(file);
                                    ObOPtimizerLoger::closeFile(file);
                                }
                                sub_query_relation->set_rel_opt_kind(ObOptimizerRelation::RELOPT_JOINREL);
                            }
                            else
                            {
                                YYSYS_LOG(INFO,"DHC Calc FromItem cost error!");
                            }
                        }
                    }
                }
            }
        }
        join_ctx_ = joinpar;
        return ret;
    }

    int ObPhysicalOptimizer::Opt_Calc_FromItem_Cost(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat &err_stat,
            ObSelectStmt *select_stmt,
            oceanbase::common::ObList<ObSqlRawExpr *> &remainder_cnd_list,
            ObOptimizerRelation *&sub_query_relation,
            oceanbase::common::ObList<ObBitSet<> > &bitset_list)
    {
        int &ret = err_stat.err_code_ = OB_SUCCESS;
        oceanbase::common::ObList<ObSqlRawExpr*>::iterator cnd_it;
        //oceanbase::common::ObList<ObOptimizerRelation*> *rel_opt_list = select_stmt->get_rel_opt_list();
        ObOptimizerRelation *left_rel_info = NULL;
        ObOptimizerRelation *right_rel_info = NULL;
        int64_t num = remainder_cnd_list.size();
        common::ObStringBuf *parser_mem_pool = logical_plan->get_name_pool();
        oceanbase::common::ObList<ObSqlRawExpr*> sql_raw_expr_list;
        int remainder_cnd_id_it =0;
        uint64_t remainder_cnd_expr_id[num];
        int cnd_it_id =0;
        for(cnd_it = remainder_cnd_list.begin();ret == OB_SUCCESS && cnd_it!= remainder_cnd_list.end();cnd_it++)
        {
            if((*cnd_it)->get_expr()->is_join_cond())
            {
                ObSqlRawExpr* sql_expr = (ObSqlRawExpr*)parse_malloc(sizeof(ObSqlRawExpr),(void*)parser_mem_pool);
                sql_expr = new(sql_expr) ObSqlRawExpr();
                *sql_expr = *(*cnd_it);
                sql_raw_expr_list.push_back(sql_expr);
                remainder_cnd_expr_id[remainder_cnd_id_it] = sql_expr->get_expr_id();
                remainder_cnd_id_it++;
            }
            cnd_it_id++;
        }
        num = bitset_list.size();
        int from_item_calc[num];
        memset(from_item_calc,0,sizeof(from_item_calc));
        num = sql_raw_expr_list.size();
        ObOptimizerFromItemHelper fromitemhelper_array_[num];
        int father[num];
        memset(father,0,sizeof(father));
        int times= 0;
        while(num--)
        {
            times++;
            int id_cnd =0;
            int cnd_it_id =0;
            bool previous_join = false;
            double tmp_nrows = 0;
            for(cnd_it = sql_raw_expr_list.begin(); ret == OB_SUCCESS && cnd_it!= sql_raw_expr_list.end();)
            {
                if(father[cnd_it_id] ==0)
                {
                    ObBinaryOpRawExpr *join_cnd = dynamic_cast<ObBinaryOpRawExpr*>((*cnd_it)->get_expr());
                    ObBinaryRefRawExpr *lexpr = dynamic_cast<ObBinaryRefRawExpr*>(join_cnd->get_first_op_expr());
                    ObBinaryRefRawExpr *rexpr = dynamic_cast<ObBinaryRefRawExpr*>(join_cnd->get_second_op_expr());
                    int32_t left_bit_idx = select_stmt->get_table_bit_index(lexpr->get_first_ref_id());
                    int32_t right_bit_idx = select_stmt->get_table_bit_index(rexpr->get_first_ref_id());
                    ObDiffValueInfo diff_value_info_left;
                    ObDiffValueInfo diff_value_info_right;
                    oceanbase::common::ObList<ObBitSet<> >::iterator bitset_it =bitset_list.begin();
                    int from_id =0;
                    int left_from_id =-1;
                    int right_from_id = -1;
                    double outer_rows = 0;
                    double inner_rows =0;
                    double nrows = 0;
                    ObOptimizerRelation *left_base_rel_opt = NULL;
                    ObOptimizerRelation *right_base_rel_opt = NULL;
                    left_rel_info = NULL;
                    right_rel_info = NULL;
                    while(ret == OB_SUCCESS
                          &&(!left_rel_info || !right_rel_info)
                          && bitset_it != bitset_list.end())
                    {
                        if(bitset_it->has_member(left_bit_idx))
                        {
                            left_from_id = from_id;
                            if(from_item_calc[left_from_id] ==0)
                            {
                                FromItem& from_item = const_cast<FromItem&>(select_stmt->get_from_item(from_id));
                                left_rel_info = from_item.from_item_rel_opt_;
                            }
                            else
                            {
                                left_rel_info = sub_query_relation;
                            }
                            if(!left_rel_info)
                            {
                                YYSYS_LOG(INFO,"DHC cant find left from item rel info");
                            }
                            OB_ASSERT(left_rel_info);
                        }
                        if(bitset_it->has_member(right_bit_idx))
                        {
                            right_from_id = from_id;
                            if(from_item_calc[right_from_id] ==0)
                            {
                                FromItem& from_item = const_cast<FromItem&>(select_stmt->get_from_item(from_id));
                                right_rel_info = from_item.from_item_rel_opt_;
                            }
                            else
                            {
                                right_rel_info = sub_query_relation;
                            }
                            if(!right_rel_info)
                            {
                                YYSYS_LOG(INFO,"DHC cant find right from item rel info");
                            }
                            OB_ASSERT(right_rel_info);
                        }
                        bitset_it++;
                        from_id++;
                    }
                    OB_ASSERT(left_from_id != -1);
                    OB_ASSERT(right_from_id != -1);

                    ObOptimizerFromItemHelper opt_from_item_helper;
                    {
                        if(left_from_id == right_from_id)
                        {
                            ret = OB_NOT_SUPPORTED;
                            break;
                        }
                        else
                            if((from_item_calc[left_from_id] ==1 && from_item_calc[right_from_id] ==1))
                            {
                                previous_join = true;
                            }
                        if(left_rel_info->get_rel_opt_kind() == ObOptimizerRelation::RELOPT_JOINREL
                               || left_rel_info->get_rel_opt_kind() == ObOptimizerRelation::RELOPT_INIT)
                        {
                            bool is_base_table = true;
                            ret = find_rel_info(logical_plan,physical_plan,err_stat,select_stmt,lexpr->get_first_ref_id(),left_base_rel_opt,is_base_table);
                        }
                        else
                        {
                            left_base_rel_opt = left_rel_info;
                        }
                        if(left_base_rel_opt == NULL)
                        {
                            YYSYS_LOG(INFO,"DHC can't find left_base_rel_opt");
                        }
                        if(left_rel_info == NULL)
                        {
                            YYSYS_LOG(INFO,"DHC can't find left_rel_info");
                        }
                        OB_ASSERT(left_base_rel_opt && left_rel_info);
                        left_base_rel_opt->print_rel_opt_info();
                        if(left_base_rel_opt->get_rel_opt_kind()== ObOptimizerRelation::RELOPT_BASEREL)
                        {
                            ret = gen_joined_column_diff_number(logical_plan,physical_plan,err_stat,select_stmt,
                                                                left_base_rel_opt,
                                                                left_base_rel_opt->get_table_id(),
                                                                lexpr->get_second_ref_id(),
                                                                diff_value_info_left);
                        }
                        else
                        {
                            diff_value_info_left.set_default_eastimation(left_base_rel_opt->get_rows());
                        }
                        ret = OB_SUCCESS;
                        if(right_rel_info->get_rel_opt_kind() == ObOptimizerRelation::RELOPT_JOINREL
                              ||  right_rel_info->get_rel_opt_kind() == ObOptimizerRelation::RELOPT_INIT)
                        {
                            bool is_base_table = true;
                            ret = find_rel_info(logical_plan,physical_plan,err_stat,select_stmt,rexpr->get_first_ref_id(),right_base_rel_opt,is_base_table);
                        }
                        else
                        {
                            right_base_rel_opt = right_rel_info;
                        }
                        if(right_base_rel_opt == NULL)
                        {
                            YYSYS_LOG(INFO,"DHC can't find right_base_rel_opt");
                        }
                        if(right_rel_info == NULL)
                        {
                            YYSYS_LOG(INFO,"DHC can't find right_rel_info");
                        }
                        OB_ASSERT(right_rel_info && right_base_rel_opt);
                        right_base_rel_opt->print_rel_opt_info();
                        if(right_base_rel_opt->get_rel_opt_kind()== ObOptimizerRelation::RELOPT_BASEREL)
                        {
                            ret = gen_joined_column_diff_number(logical_plan,physical_plan,err_stat,select_stmt,
                                                                right_base_rel_opt,
                                                                right_base_rel_opt->get_table_id(),
                                                                rexpr->get_second_ref_id(),
                                                                diff_value_info_right);
                        }
                        else
                        {
                            diff_value_info_right.set_default_eastimation(right_base_rel_opt->get_rows());
                        }
                        ret = OB_SUCCESS;

                        outer_rows = left_rel_info->get_rows();
                        inner_rows = right_rel_info->get_rows();
                        double left_ratio = outer_rows / left_base_rel_opt->get_join_rows();
                        double right_ratio = inner_rows / right_base_rel_opt->get_join_rows();
                        double nrows1 = (left_ratio * right_ratio) *(
                                    diff_value_info_left.high_freq_dup_num * diff_value_info_right.high_freq_dup_num *
                                    fmin(diff_value_info_left.high_freq_diff_num,diff_value_info_right.high_freq_diff_num)+
                                    diff_value_info_left.low_freq_dup_num * diff_value_info_right.low_freq_dup_num *
                                    fmin(diff_value_info_left.low_freq_diff_num,diff_value_info_right.low_freq_diff_num));
                        double nrows2 = (left_ratio * right_ratio) *(
                                    diff_value_info_left.high_freq_dup_num * diff_value_info_right.low_freq_dup_num *
                                    fmin(diff_value_info_left.high_freq_diff_num,diff_value_info_right.low_freq_diff_num) +
                                    diff_value_info_left.low_freq_dup_num * diff_value_info_right.high_freq_dup_num *
                                    fmin(diff_value_info_left.low_freq_diff_num,diff_value_info_right.high_freq_diff_num));
                        int flag =0;
                        if((diff_value_info_left.high_freq_diff_num==0 &&
                            diff_value_info_right.high_freq_diff_num ==0) ||
                                (diff_value_info_left.low_freq_diff_num ==0 &&
                                 diff_value_info_right.low_freq_diff_num ==0))
                        {
                            nrows = nrows1;
                            flag = 1;
                        }
                        else if((diff_value_info_left.high_freq_diff_num ==0 &&
                                 diff_value_info_right.low_freq_diff_num ==0) ||
                                (diff_value_info_left.low_freq_diff_num ==0 &&
                                 diff_value_info_right.high_freq_diff_num ==0))
                        {
                            nrows = nrows2;
                            flag =2;
                        }
                        else
                        {
                            nrows = (nrows1+nrows2)/2;
                        }

                        if(nrows < 1.0)
                        {
                            nrows1=1;
                        }
                        if(flag ==1)
                        {
                            opt_from_item_helper.left_join_rows = diff_value_info_left.high_freq_dup_num
                                    *fmin(diff_value_info_left.high_freq_diff_num,diff_value_info_right.high_freq_diff_num)
                                    +diff_value_info_left.low_freq_dup_num
                                    *fmin(diff_value_info_left.low_freq_diff_num,diff_value_info_right.low_freq_diff_num);
                            opt_from_item_helper.right_join_rows = diff_value_info_right.high_freq_dup_num
                                    *fmin(diff_value_info_left.high_freq_diff_num,diff_value_info_right.high_freq_diff_num)
                                    +diff_value_info_right.low_freq_dup_num
                                    *fmin(diff_value_info_left.low_freq_diff_num,diff_value_info_right.low_freq_diff_num);
                        }
                        else if(flag ==2)
                        {
                            opt_from_item_helper.left_join_rows = diff_value_info_left.high_freq_dup_num
                                    *fmin(diff_value_info_left.high_freq_diff_num,diff_value_info_right.low_freq_diff_num)
                                    +diff_value_info_left.low_freq_dup_num
                                    *fmin(diff_value_info_left.low_freq_diff_num,diff_value_info_right.high_freq_diff_num);
                            opt_from_item_helper.right_join_rows = diff_value_info_right.high_freq_dup_num
                                    *fmin(diff_value_info_left.low_freq_diff_num,diff_value_info_right.high_freq_diff_num)
                                    +diff_value_info_right.low_freq_dup_num
                                    *fmin(diff_value_info_left.high_freq_diff_num,diff_value_info_right.low_freq_diff_num);
                        }
                        else
                        {
                            double join_rows =0;
                            join_rows += diff_value_info_left.high_freq_dup_num
                                    *fmin(diff_value_info_left.high_freq_diff_num,diff_value_info_right.high_freq_diff_num)
                                    +diff_value_info_left.low_freq_dup_num
                                    *fmin(diff_value_info_left.low_freq_diff_num,diff_value_info_right.low_freq_diff_num);

                            join_rows += diff_value_info_left.high_freq_dup_num
                                    *fmin(diff_value_info_left.high_freq_diff_num,diff_value_info_right.low_freq_diff_num)
                                    +diff_value_info_left.low_freq_dup_num
                                    *fmin(diff_value_info_left.low_freq_diff_num,diff_value_info_right.high_freq_diff_num);

                            opt_from_item_helper.left_join_rows = join_rows/2;

                            join_rows = 0;

                            join_rows += diff_value_info_right.high_freq_dup_num
                                    *fmin(diff_value_info_left.high_freq_diff_num,diff_value_info_right.high_freq_diff_num)
                                    +diff_value_info_right.low_freq_dup_num
                                    *fmin(diff_value_info_left.low_freq_diff_num,diff_value_info_right.low_freq_diff_num);

                            join_rows += diff_value_info_right.high_freq_dup_num
                                    *fmin(diff_value_info_left.low_freq_diff_num,diff_value_info_right.high_freq_diff_num)
                                    +diff_value_info_right.low_freq_dup_num
                                    *fmin(diff_value_info_left.high_freq_diff_num,diff_value_info_right.low_freq_diff_num);

                            opt_from_item_helper.right_join_rows = join_rows/2;
                        }
                        opt_from_item_helper.data_skew_left = diff_value_info_left.data_skew;
                        opt_from_item_helper.high_freq_dup_left = diff_value_info_left.high_freq_dup_num;
                        opt_from_item_helper.data_skew_right = diff_value_info_right.data_skew;
                        opt_from_item_helper.high_freq_dup_right = diff_value_info_right.high_freq_dup_num;

                        opt_from_item_helper.diff_num_left = diff_value_info_left.total_diff_num;
                        opt_from_item_helper.diff_num_right = diff_value_info_right.total_diff_num;
                        opt_from_item_helper.left_base_rel_opt = left_base_rel_opt;
                        opt_from_item_helper.right_base_rel_opt = right_base_rel_opt;
                        opt_from_item_helper.left_rel_opt = left_rel_info;
                        opt_from_item_helper.right_rel_opt = right_rel_info;

                        opt_from_item_helper.left_join_rows = clamp_row_est(opt_from_item_helper.left_join_rows);
                        opt_from_item_helper.right_join_rows = clamp_row_est(opt_from_item_helper.right_join_rows);
                        opt_from_item_helper.left_join_rows = fmin(opt_from_item_helper.left_join_rows,left_base_rel_opt->get_rows());
                        opt_from_item_helper.right_join_rows = fmin(opt_from_item_helper.right_join_rows,right_base_rel_opt->get_rows());
                    }
                    if(previous_join)
                    {
                        tmp_nrows = nrows;
                        nrows = -1;
                    }
                    opt_from_item_helper.nrows = nrows;
                    opt_from_item_helper.cnd_it_id = cnd_it_id;
                    ObSqlRawExpr *sql_expr = (ObSqlRawExpr*)parse_malloc(sizeof(ObSqlRawExpr),(void*)parser_mem_pool);
                    sql_expr = new(sql_expr)ObSqlRawExpr();
                    *sql_expr = *(*cnd_it);
                    opt_from_item_helper.cnd_id_expr = sql_expr;
                    fromitemhelper_array_[id_cnd++] = opt_from_item_helper;
                }
                else
                {

                }
                cnd_it_id++;
                cnd_it++;
                if(previous_join)
                {
                    break;
                }
            }
            std::sort(fromitemhelper_array_,fromitemhelper_array_+id_cnd);
            if(previous_join)
            {
                fromitemhelper_array_[0].nrows = tmp_nrows;
            }
            if(id_cnd <=0)
            {
                break;
            }
            father[fromitemhelper_array_[0].cnd_it_id] =1;
            int for_id =0;
            for(cnd_it = remainder_cnd_list.begin(); ret == OB_SUCCESS && cnd_it != remainder_cnd_list.end();for_id++,cnd_it++)
            {
                if(for_id == times -1)
                {
                    *(*cnd_it) = *(fromitemhelper_array_[0].cnd_id_expr);
                    (*(*cnd_it)).set_expr_id(remainder_cnd_expr_id[for_id]);
                    ObBinaryOpRawExpr *join_cnd = dynamic_cast<ObBinaryOpRawExpr*>((*cnd_it)->get_expr());
                    ObBinaryRefRawExpr *lexpr = dynamic_cast<ObBinaryRefRawExpr*>(join_cnd->get_first_op_expr());
                    ObBinaryRefRawExpr *rexpr = dynamic_cast<ObBinaryRefRawExpr*>(join_cnd->get_second_op_expr());
                    int32_t left_bit_idx = select_stmt->get_table_bit_index(lexpr->get_first_ref_id());
                    int32_t right_bit_idx = select_stmt->get_table_bit_index(rexpr->get_first_ref_id());

                    int left_from_id =-1;
                    int right_from_id = -1;
                    int from_id =0;
                    oceanbase::common::ObList<ObBitSet<> >::iterator bitset_it =bitset_list.begin();

                    while(ret == OB_SUCCESS
                          && bitset_it != bitset_list.end())
                    {
                        if(bitset_it->has_member(left_bit_idx))
                        {
                            from_item_calc[from_id] =1;
                            left_from_id = from_id;
                        }
                        if(bitset_it->has_member(right_bit_idx))
                        {
                            from_item_calc[from_id] =1;
                            right_from_id = from_id;
                        }
                        bitset_it++;
                        from_id++;
                    }
                    OB_ASSERT(left_from_id != -1);
                    OB_ASSERT(right_from_id != -1);

                    ObFromItemJoinMethodHelper *from_item_join_method = NULL;
                    void *buf = logical_plan->get_name_pool()->alloc(sizeof(ObFromItemJoinMethodHelper));
                    if(buf == NULL)
                    {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        YYSYS_LOG(ERROR,"fail to new ObFromItemJoinMethodHelper");
                    }
                    else
                    {
                        from_item_join_method = new(buf) ObFromItemJoinMethodHelper();
                    }
                    from_item_join_method->where_sql_expr = *cnd_it;
                    if(fromitemhelper_array_[0].nrows >=0.0)
                    {
                        if(previous_join)
                        {
                            from_item_join_method->previous_join=true;
                            sub_query_relation->set_rows(fmin(sub_query_relation->get_rows(),fromitemhelper_array_[0].nrows));
                        }
                        else
                        {
                            sub_query_relation->set_rows(fromitemhelper_array_[0].nrows);
                        }
                        if(fromitemhelper_array_[0].diff_num_left > fromitemhelper_array_[0].diff_num_right)
                        {
                            from_item_join_method->exchange_order = true;
                        }
                        else
                        {
                            from_item_join_method->exchange_order = false;
                        }

                        if(previous_join)
                        {
                            fromitemhelper_array_[0].left_base_rel_opt->set_join_rows(
                                        fmin(fromitemhelper_array_[0].left_base_rel_opt->get_join_rows(),fromitemhelper_array_[0].left_join_rows));
                            fromitemhelper_array_[0].right_base_rel_opt->set_join_rows(
                                        fmin(fromitemhelper_array_[0].right_base_rel_opt->get_join_rows(),fromitemhelper_array_[0].right_join_rows));

                        }
                        else
                        {
                            fromitemhelper_array_[0].left_base_rel_opt->set_join_rows(fromitemhelper_array_[0].left_join_rows);
                            fromitemhelper_array_[0].right_base_rel_opt->set_join_rows(fromitemhelper_array_[0].right_join_rows);
                        }
                        if((*cnd_it)->get_expr()->is_join_cond())
                        {
                            ObObjType left_column_type = common::ObMinType;
                            ObObjType right_column_type = common::ObMaxType;
                            if((ret = find_column_type(logical_plan,select_stmt,lexpr->get_first_ref_id(),lexpr->get_second_ref_id(),left_column_type)) != OB_SUCCESS)
                            {
                                YYSYS_LOG(INFO,"Can't find Column type");
                            }
                            else if((ret = find_column_type(logical_plan,select_stmt,rexpr->get_first_ref_id(),rexpr->get_second_ref_id(),right_column_type)) != OB_SUCCESS)
                            {
                                YYSYS_LOG(INFO,"Can't find Column type");
                            }
                            else if(left_column_type == right_column_type && right_column_type == common::ObDecimalType
                                    && fromitemhelper_array_[0].diff_num_left >= MIN_HASH_JOIN_DIFF_NUM_THRESHOLD
                                    && fromitemhelper_array_[0].diff_num_right >= MIN_HASH_JOIN_DIFF_NUM_THRESHOLD
                                    && !(fromitemhelper_array_[0].data_skew_left >= DATA_SKEW_THRESHOLD
                                         && fromitemhelper_array_[0].data_skew_right >= DATA_SKEW_THRESHOLD
                                         && fromitemhelper_array_[0].high_freq_dup_left * fromitemhelper_array_[0].high_freq_dup_right
                                         >= HASH_BUCKET_DEPTH_THRESHOLD)
                                    && fromitemhelper_array_[0].diff_num_left <= MAX_HASH_JOIN_ROWS)
                            {
                                from_item_join_method->join_method = JoinedTable::HASH_JOIN;
                            }
                            else
                                if(fromitemhelper_array_[0].right_rel_opt->get_rel_opt_kind() == ObOptimizerRelation::RELOPT_BASEREL
                                        && ((fromitemhelper_array_[0].diff_num_left < SEMI_DIFF_NUM_THRESHOLD
                                             && fromitemhelper_array_[0].data_skew_left < DATA_SKEW_THRESHOLD)
                                            || (fromitemhelper_array_[0].data_skew_left >= DATA_SKEW_THRESHOLD
                                                && fromitemhelper_array_[0].left_rel_opt->get_rows() < SEMI_ROW_NUM_THRESHOLD))
                                        && fromitemhelper_array_[0].diff_num_left <=fromitemhelper_array_[0].diff_num_right)
                                {
                                    from_item_join_method->join_method = JoinedTable::SEMI_JOIN;
                                    from_item_join_method->exchange_order = false;
                                    ObOptimizerRelation *reset_base_rel_opt = NULL;
                                    bool reset_base_rel_is_subquery = false;
                                    ret = find_rel_info(logical_plan,physical_plan,err_stat,select_stmt,fromitemhelper_array_[0].right_base_rel_opt->get_table_id(),reset_base_rel_opt,reset_base_rel_is_subquery);
                                    if(ret == OB_SUCCESS && reset_base_rel_is_subquery == false)
                                    {
                                        reset_base_rel_opt->reset_semi_join_right_index_table_cost(rexpr->get_second_ref_id());
                                    }
                                    else
                                    {
                                        YYSYS_LOG(WARN,"DHC reset_base_rel_opt error");
                                    }
                                }
                                else if(fromitemhelper_array_[0].left_rel_opt->get_rel_opt_kind() == ObOptimizerRelation::RELOPT_BASEREL
                                        && ((fromitemhelper_array_[0].diff_num_right < SEMI_DIFF_NUM_THRESHOLD
                                             && fromitemhelper_array_[0].data_skew_right < DATA_SKEW_THRESHOLD)
                                            || (fromitemhelper_array_[0].data_skew_right >= DATA_SKEW_THRESHOLD
                                                && fromitemhelper_array_[0].right_rel_opt->get_rows() < SEMI_ROW_NUM_THRESHOLD))
                                        && fromitemhelper_array_[0].diff_num_left >=fromitemhelper_array_[0].diff_num_right)
                                {
                                    from_item_join_method->join_method = JoinedTable::SEMI_JOIN;
                                    from_item_join_method->exchange_order = true;
                                    ObOptimizerRelation *reset_base_rel_opt = NULL;
                                    bool reset_base_rel_is_subquery = false;
                                    ret = find_rel_info(logical_plan,physical_plan,err_stat,select_stmt,fromitemhelper_array_[0].left_base_rel_opt->get_table_id(),reset_base_rel_opt,reset_base_rel_is_subquery);
                                    if(ret == OB_SUCCESS && reset_base_rel_is_subquery == false)
                                    {
                                        reset_base_rel_opt->reset_semi_join_right_index_table_cost(lexpr->get_second_ref_id());
                                    }
                                    else
                                    {
                                        YYSYS_LOG(WARN,"DHC reset_base_rel_opt error");
                                    }
                                }
                                else if(left_column_type == right_column_type
                                        &&!(fromitemhelper_array_[0].data_skew_left >= DATA_SKEW_THRESHOLD
                                            && fromitemhelper_array_[0].data_skew_right >= DATA_SKEW_THRESHOLD
                                            && fromitemhelper_array_[0].high_freq_dup_left*fromitemhelper_array_[0].high_freq_dup_right
                                            >= HASH_BUCKET_DEPTH_THRESHOLD)
                                        && fromitemhelper_array_[0].diff_num_left <= MAX_HASH_JOIN_ROWS)
                                {
                                    from_item_join_method->join_method = JoinedTable::HASH_JOIN;
                                }
                                else
                                {
                                    from_item_join_method->join_method = JoinedTable::MERGE_JOIN;
                                }
                        }
                        else
                        {
                            from_item_join_method->join_method = JoinedTable::MERGE_JOIN;
                        }
                    }
                    else
                    {
                        from_item_join_method->join_method = JoinedTable::MERGE_JOIN;
                    }
                    oceanbase::common::ObList<ObFromItemJoinMethodHelper*> *fromitem_method_list = select_stmt->get_from_item_method_list();
                    fromitem_method_list->push_back(from_item_join_method);
                    oceanbase::common::ObList<int> * from_item_appear_order_list = select_stmt->get_from_item_appear_order_list();
                    if(from_item_join_method->exchange_order)
                    {
                        from_item_appear_order_list->push_back(right_from_id);
                        from_item_appear_order_list->push_back(left_from_id);
                    }
                    else
                    {
                        from_item_appear_order_list->push_back(left_from_id);
                        from_item_appear_order_list->push_back(right_from_id);
                    }
                    break;
                }
            }
            for(int jjj=0;jjj<id_cnd;jjj++){
                fromitemhelper_array_[jjj].cnd_id_expr->~ObSqlRawExpr();
                parse_free(fromitemhelper_array_[jjj].cnd_id_expr);
            }
        }
        sub_query_relation->set_tuples(sub_query_relation->get_rows());
        sub_query_relation->set_join_rows(sub_query_relation->get_rows());
        sub_query_relation->print_rel_opt_info();
        return ret;
    }

    void ObPhysicalOptimizer::opt_get_join_table_raws(
            ObOptimizerRelation *rel_info,
            ObBinaryRefRawExpr *refexpr,
            ErrStat &err_stat,
            ObDiffValueInfo &diff_value_info,
            ObOptimizerRelation *&base_rel_opt,
            bool &is_base_table)
    {
        int &ret = err_stat.err_code_ = OB_SUCCESS;
        is_base_table = false;
        if(rel_info->get_table_id() != refexpr->get_first_ref_id())
        {
            ret = find_rel_info(join_ctx_.logical_plan,join_ctx_.physical_plan,err_stat,join_ctx_.select_stmt,
                                refexpr->get_first_ref_id(),base_rel_opt,is_base_table);
        }
        else
        {
            base_rel_opt = rel_info;
        }
        if(base_rel_opt == NULL)
        {
            YYSYS_LOG(INFO,"DHC can't find right_base_rel_opt");
        }
        OB_ASSERT(base_rel_opt);
        if(base_rel_opt->get_rel_opt_kind() == ObOptimizerRelation::RELOPT_BASEREL)
        {
            ret = gen_joined_column_diff_number(join_ctx_.logical_plan,join_ctx_.physical_plan,err_stat,join_ctx_.select_stmt,
                                                base_rel_opt,
                                                base_rel_opt->get_table_id(),
                                                refexpr->get_second_ref_id(),
                                                diff_value_info);
        }
        else
        {
            diff_value_info.set_default_eastimation(base_rel_opt->get_rows());
        }
        return;
    }

    int ObPhysicalOptimizer::Opt_JoinedTables_GetEqExpr(
            ObRawExpr *equal_expr,
            ObOptimizerRelation *left_rel_info,
            ErrStat &err_stat,
            double &nrows,
            double &min_row)
    {
        int &ret = err_stat.err_code_ = OB_SUCCESS;
        ObBinaryOpRawExpr *join_cnd = dynamic_cast<ObBinaryOpRawExpr*>(equal_expr);
        if(join_cnd && join_cnd->get_expr_type() == T_OP_EQ)
        {
            ObBinaryRefRawExpr *lexpr = dynamic_cast<ObBinaryRefRawExpr*>(join_cnd->get_first_op_expr());
            ObBinaryRefRawExpr *rexpr = dynamic_cast<ObBinaryRefRawExpr*>(join_cnd->get_second_op_expr());
            ObDiffValueInfo diff_value_info_left;
            ObDiffValueInfo diff_value_info_right;
            ObArray<int> &join_method_array = join_ctx_.join_method_array;
            ObBitSet<> &left_table_bitset = join_ctx_.left_table_bitset;
            bool is_base_table = true;
            ObOptimizerRelation *left_base_rel_opt = NULL;
            ObOptimizerRelation *right_base_rel_opt = NULL;
            int32_t left_bit_idx = join_ctx_.select_stmt->get_table_bit_index(lexpr->get_first_ref_id());
            if(!left_table_bitset.has_member(left_bit_idx))
            {
                ObBinaryRefRawExpr *tmp_expr = lexpr;
                lexpr = rexpr;
                rexpr = tmp_expr;
            }
            opt_get_join_table_raws(left_rel_info,lexpr,err_stat,diff_value_info_left,
                                    left_base_rel_opt,is_base_table);
            opt_get_join_table_raws(join_ctx_.right_rel_info,rexpr,err_stat,diff_value_info_right,
                                    right_base_rel_opt,is_base_table);
            double outer_rows = 0;
            if(join_ctx_.joined_table_id ==1)
            {
                outer_rows = left_base_rel_opt->get_rows();
                left_rel_info->set_rows(outer_rows);
            }
            else
            {
                outer_rows = left_rel_info->get_rows();
            }
            double inner_rows = join_ctx_.right_rel_info->get_rows();
            double left_ratio = outer_rows/ left_base_rel_opt->get_join_rows();
            double right_ratio = inner_rows / right_base_rel_opt->get_join_rows();
#define DHLEFT (diff_value_info_left.high_freq_dup_num)
#define DHRIGHT (diff_value_info_right.high_freq_dup_num)
#define DLLEFT (diff_value_info_left.low_freq_dup_num)
#define DLRIGHT (diff_value_info_right.low_freq_dup_num)

#define CHH fmin(diff_value_info_left.high_freq_diff_num,diff_value_info_right.high_freq_diff_num)
#define CLL fmin(diff_value_info_left.low_freq_diff_num,diff_value_info_right.low_freq_diff_num)
#define CHL fmin(diff_value_info_left.high_freq_diff_num,diff_value_info_right.low_freq_diff_num)
#define CLH fmin(diff_value_info_left.low_freq_diff_num,diff_value_info_right.high_freq_diff_num)

            double nrows1 = (left_ratio * right_ratio) * (DHLEFT * DHRIGHT * CHH + DLLEFT * DLRIGHT * CLL);
            double nrows2 = (left_ratio * right_ratio) * (DHLEFT * DLRIGHT * CHL + DLLEFT * DHRIGHT * CLH);
            int flag =0;
            if((diff_value_info_left.high_freq_diff_num ==0 &&
                diff_value_info_right.high_freq_diff_num ==0) ||
                    (diff_value_info_left.low_freq_diff_num ==0 &&
                     diff_value_info_right.low_freq_diff_num ==0))
            {
                nrows = nrows1;
                flag =1;
            }
            else if((diff_value_info_left.high_freq_diff_num ==0 &&
                     diff_value_info_right.low_freq_diff_num ==0) ||
                    (diff_value_info_left.low_freq_diff_num ==0 &&
                     diff_value_info_right.high_freq_diff_num ==0))
            {
                nrows = nrows2;
                flag =2;
            }
            else
            {
                nrows = (nrows1 + nrows2)/2;
            }
            bool left_or_inner = false;
            switch (join_ctx_.joined_table->join_types_.at(join_ctx_.joined_table_id-1))
            {
            case JoinedTable::T_FULL:
            {
                if(flag ==1)
                {
                    nrows += fmax(0,outer_rows - left_ratio * (DHLEFT *CHH + DLLEFT *CLL));
                    nrows += fmax(0,inner_rows - right_ratio * (DHRIGHT * CHH + DLRIGHT *CLL));
                }
                else if(flag ==2)
                {
                    nrows += fmax(0,outer_rows - left_ratio *(DHLEFT * CHL + DLLEFT * CLH));
                    nrows += fmax(0,inner_rows - right_ratio * (DHRIGHT * CLH + DLRIGHT * CHL));
                }
                else
                {
                    double stay_rows = 0;
                    stay_rows += fmax(0,outer_rows - left_ratio * (DHLEFT *CHH + DLLEFT *CLL));
                    stay_rows += fmax(0,outer_rows - left_ratio *(DHLEFT * CHL + DLLEFT * CLH));
                    stay_rows += fmax(0,inner_rows - right_ratio * (DHRIGHT * CHH + DLRIGHT *CLL));
                    stay_rows += fmax(0,inner_rows - right_ratio * (DHRIGHT * CLH + DLRIGHT * CHL));
                    nrows +=stay_rows/2;
                }
                if(nrows < outer_rows)
                    nrows = outer_rows;
                if(nrows < inner_rows)
                    nrows = inner_rows;
                break;
            }
            case JoinedTable::T_LEFT:
            {
                left_or_inner = true;
                if(diff_value_info_left.total_diff_num > diff_value_info_right.total_diff_num)
                {
                    if(flag ==1)
                    {
                        nrows += fmax(0,outer_rows - left_ratio * (DHLEFT *CHH + DLLEFT *CLL));
                    }
                    else if(flag ==2)
                    {
                        nrows += fmax(0,outer_rows - left_ratio *(DHLEFT * CHL + DLLEFT * CLH));
                    }
                    else
                    {
                        double stay_rows =0;
                        stay_rows += fmax(0,outer_rows - left_ratio * (DHLEFT *CHH + DLLEFT *CLL));
                        stay_rows += fmax(0,outer_rows - left_ratio *(DHLEFT * CHL + DLLEFT * CLH));
                        nrows += stay_rows/2;
                    }
                }
                if(nrows < outer_rows)
                    nrows = outer_rows;
                break;
            }
            case JoinedTable::T_RIGHT:
            {
                if(diff_value_info_left.total_diff_num < diff_value_info_right.total_diff_num)
                {
                    if(flag ==1)
                    {
                        nrows += fmax(0,inner_rows - right_ratio * (DHRIGHT * CHH + DLRIGHT *CLL));
                    }
                    else if(flag ==2)
                    {
                        nrows += fmax(0,inner_rows - right_ratio * (DHRIGHT * CLH + DLRIGHT * CHL));
                    }
                    else
                    {
                        double stay_rows = 0;
                        stay_rows += fmax(0,inner_rows - right_ratio * (DHRIGHT * CHH + DLRIGHT *CLL));
                        stay_rows += fmax(0,inner_rows - right_ratio * (DHRIGHT * CLH + DLRIGHT * CHL));
                        nrows += stay_rows/2;
                    }
                }
                if(nrows < inner_rows)
                    nrows = inner_rows;
                break;
            }
            case JoinedTable::T_INNER:
                left_or_inner = true;
                break;
            default:
                break;
            }
            if(nrows < 1.0)
            {
                nrows = 1;
            }
            if(min_row > nrows)
            {
                bool is_base_table = true;
                ret = find_rel_info(join_ctx_.logical_plan,join_ctx_.physical_plan,err_stat,join_ctx_.select_stmt,
                                    lexpr->get_first_ref_id(),left_base_rel_opt,is_base_table);
                min_row = nrows;
                left_rel_info->set_rows(min_row);
                left_rel_info->set_join_rows(min_row);
                left_rel_info->set_tuples(min_row);
                switch (join_ctx_.joined_table->join_types_.at(join_ctx_.joined_table_id-1))
                {
                case JoinedTable::T_FULL:
                    break;
                case JoinedTable::T_LEFT:
                    left_or_inner = true;
                    right_base_rel_opt->set_join_rows(fmin(right_base_rel_opt->get_rows(),right_base_rel_opt->get_join_rows()));
                    break;
                case JoinedTable::T_RIGHT:
                    left_base_rel_opt->set_join_rows(fmin(left_base_rel_opt->get_rows(),left_base_rel_opt->get_join_rows()));
                    break;
                case JoinedTable::T_INNER:
                {
                    left_or_inner = true;
                    double left_join_rows = 0;
                    double right_join_rows = 0;
                    if(flag ==1)
                    {
                        left_join_rows = DHLEFT*CHH + DLLEFT*CLL;
                        right_join_rows = DHRIGHT*CHH + DLRIGHT*CLL;
                    }
                    else if(flag ==2)
                    {
                        left_join_rows = DHLEFT*CHL + DLLEFT*CLH;
                        right_join_rows = DHRIGHT*CLH + DLRIGHT*CHL;
                    }
                    else
                    {
                        double join_rows =0;
                        join_rows +=DHLEFT*CHH + DLLEFT*CLL;
                        join_rows +=DHLEFT*CHL + DLLEFT*CLH;
                        left_join_rows = join_rows /2;

                        join_rows =0;
                        join_rows +=DHRIGHT*CHH + DLRIGHT*CLL;
                        join_rows +=DHRIGHT*CLH + DLRIGHT*CHL;
                        right_join_rows = join_rows /2;
                    }
                    left_base_rel_opt->set_join_rows(fmin(left_base_rel_opt->get_join_rows(),left_join_rows));
                    right_base_rel_opt->set_join_rows(fmin(right_base_rel_opt->get_join_rows(),right_join_rows));
                    break;
                }
                default:
                    break;
                }
            }
            int join_method = JoinedTable::MERGE_JOIN;
            if(equal_expr->is_join_cond())
            {
                ObObjType left_column_type = common::ObMinType;
                ObObjType right_column_type = common::ObMaxType;
                if((ret = find_column_type(join_ctx_.logical_plan,join_ctx_.select_stmt,
                                           lexpr->get_first_ref_id(),lexpr->get_second_ref_id(),left_column_type)) != OB_SUCCESS)
                {
                    YYSYS_LOG(INFO,"Can't find Column type");
                }
                else if((ret = find_column_type(join_ctx_.logical_plan,join_ctx_.select_stmt,
                                                rexpr->get_first_ref_id(), rexpr->get_second_ref_id(),right_column_type)) != OB_SUCCESS)
                {
                    YYSYS_LOG(INFO,"Can't find Column type");
                }
                else if(left_column_type == right_column_type && right_column_type == common::ObDecimalType
                        && diff_value_info_left.total_diff_num >= MIN_HASH_JOIN_DIFF_NUM_THRESHOLD
                        && !(diff_value_info_left.data_skew >= DATA_SKEW_THRESHOLD
                             && diff_value_info_right.data_skew >= DATA_SKEW_THRESHOLD
                             && DHLEFT *DHRIGHT >= HASH_BUCKET_DEPTH_THRESHOLD)
                        && diff_value_info_left.total_diff_num <= MAX_HASH_JOIN_ROWS)
                {
                    join_method = JoinedTable::HASH_JOIN;
                }
                else if(left_or_inner && !join_ctx_.right_base_rel_is_subquery
                        && ((diff_value_info_left.data_skew < DATA_SKEW_THRESHOLD
                             && diff_value_info_left.total_diff_num < SEMI_DIFF_NUM_THRESHOLD)
                            || (diff_value_info_left.data_skew >= DATA_SKEW_THRESHOLD
                                && outer_rows < SEMI_ROW_NUM_THRESHOLD)))
                {
                    join_method = JoinedTable::SEMI_JOIN;
                    right_base_rel_opt->reset_semi_join_right_index_table_cost(rexpr->get_second_ref_id());
                }
                else if(left_column_type == right_column_type
                        && !(diff_value_info_left.data_skew >= DATA_SKEW_THRESHOLD
                             && diff_value_info_right.data_skew >= DATA_SKEW_THRESHOLD
                             && DHLEFT*DHRIGHT >= HASH_BUCKET_DEPTH_THRESHOLD)
                        && diff_value_info_left.total_diff_num < MAX_HASH_JOIN_ROWS)
                {
                    join_method = JoinedTable::HASH_JOIN;
                }
                else
                {
                    join_method = JoinedTable::MERGE_JOIN;
                }
            }
            else
            {

            }
            join_method_array.push_back(join_method);
        }else{
            YYSYS_LOG(WARN,"join on expr is not EQ");
            ret = OB_ERROR;
        }
        return ret;
    }

    int ObPhysicalOptimizer::Opt_JoinedTables_GetOrExpr(
            ObRawExpr *op_expr,
            ObOptimizerRelation *left_rel_info,
            ErrStat &err_stat,
            double &nrows,
            double &min_row)
    {
        int &ret = err_stat.err_code_ = OB_SUCCESS;
        if(op_expr->get_expr_type() == T_OP_OR){
            double nrows_left = nrows;
            double nrows_right = nrows;
            double min_row_left = min_row;
            double min_row_right = min_row;
            ObOptimizerRelation lc_rel_info;
            lc_rel_info.set_table_id(left_rel_info->get_table_id());
            lc_rel_info.set_rel_opt_kind(left_rel_info->get_rel_opt_kind());
            lc_rel_info.set_rows(left_rel_info->get_rows());
            lc_rel_info.set_join_rows(left_rel_info->get_join_rows());
            lc_rel_info.set_tuples(left_rel_info->get_tuples());
            ObBinaryOpRawExpr *join_cnd = dynamic_cast<ObBinaryOpRawExpr*>(op_expr);
            ObBinaryOpRawExpr *l_expr = dynamic_cast<ObBinaryOpRawExpr*>(join_cnd->get_first_op_expr());
            ObBinaryOpRawExpr *r_expr = dynamic_cast<ObBinaryOpRawExpr*>(join_cnd->get_second_op_expr());
            if(l_expr && l_expr->get_expr_type() == T_OP_OR){
                ret = Opt_JoinedTables_GetOrExpr(l_expr, &lc_rel_info,err_stat,nrows_left,min_row_left);
            }else if(l_expr){
                ret = Opt_JoinedTables_GetEqExpr(l_expr,&lc_rel_info,err_stat,nrows_left,min_row_left);
            }else{
                YYSYS_LOG(WARN,"join on left expr is NULL.");
                ret = OB_ERR_EXPR_UNKNOWN;
            }
            if(ret == OB_SUCCESS && r_expr && r_expr->get_expr_type() == T_OP_OR){
                ret = Opt_JoinedTables_GetOrExpr(r_expr, &lc_rel_info,err_stat,nrows_right,min_row_right);
            }else if(r_expr){
                ret = Opt_JoinedTables_GetEqExpr(r_expr,&lc_rel_info,err_stat,nrows_right,min_row_right);
            }else{
                YYSYS_LOG(WARN,"join on right expr is NULL.");
                ret = OB_ERR_EXPR_UNKNOWN;
            }

            if(ret == OB_SUCCESS){
                double min_row_new = min_row_left + min_row_right;
                double nrows_new = nrows_left + nrows_right;
                min_row = fmax(min_row,min_row_new);
                nrows = fmax(nrows,nrows_new);
                min_row = fmax(min_row,nrows);

                left_rel_info->set_rows(min_row);
                left_rel_info->set_join_rows(min_row);
                left_rel_info->set_tuples(min_row);
            }else{
                YYSYS_LOG(WARN,"join on or expr cost error:%d.",ret);
            }
        }
        return ret;
    }

    int ObPhysicalOptimizer::Opt_Calc_JoinedTables_Cost(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat &err_stat,
            ObSelectStmt *select_stmt,
            JoinedTable *joined_table,
            oceanbase::common::ObList<ObSqlRawExpr *> &remainder_cnd_list,
            oceanbase::common::ObList<ObBitSet<> > &bitset_list,
            ObOptimizerRelation *left_rel_info,
            int32_t &joined_table_id)
    {
        int  &ret = err_stat.err_code_ = OB_SUCCESS;
        ObBitSet<> &left_table_bitset = join_ctx_.left_table_bitset;
        ret = bitset_list.pop_front(left_table_bitset);
        ObBitSet<> right_table_bitset;
        ret = bitset_list.pop_front(right_table_bitset);
        OB_ASSERT(!left_table_bitset.is_empty());
        OB_ASSERT(!right_table_bitset.is_empty());
        if(left_rel_info == NULL)
        {
            YYSYS_LOG(WARN,"DHC TEST can not find left_rel_info");
        }
        ObOptimizerRelation *right_rel_info = NULL;
        oceanbase::common::ObList<ObOptimizerRelation*> * rel_opt_list = select_stmt->get_rel_opt_list();
        oceanbase::common::ObList<ObOptimizerRelation*>::iterator rel_opt_it = rel_opt_list->begin();
        while (ret == OB_SUCCESS
               && rel_opt_it != rel_opt_list->end())
        {
            if(right_table_bitset.has_member(select_stmt->get_table_bit_index(((*rel_opt_it)->get_table_id()))))
            {
                ret = find_rel_info(logical_plan,physical_plan,err_stat,select_stmt,
                                    (*rel_opt_it)->get_table_id(),right_rel_info,join_ctx_.right_base_rel_is_subquery);
                break;
            }
            rel_opt_it++;

        }
        if(right_rel_info == NULL)
        {
            ret = find_rel_info(logical_plan,physical_plan,err_stat,select_stmt,
                                joined_table->table_ids_.at(joined_table_id),
                                right_rel_info,join_ctx_.right_base_rel_is_subquery);
        }
        OB_ASSERT(left_rel_info && right_rel_info);
        join_ctx_.logical_plan = logical_plan;
        join_ctx_.physical_plan = physical_plan;
        join_ctx_.select_stmt = select_stmt;

        join_ctx_.joined_table = joined_table;
        join_ctx_.joined_table_id = joined_table_id;
        join_ctx_.left_rel_info = left_rel_info;
        join_ctx_.right_rel_info = right_rel_info;

        int join_method = JoinedTable::MERGE_JOIN;
        bool only_has_single_table_cond = true;
        ObArray<int> &join_method_array = join_ctx_.join_method_array;
        if(ret != OB_SUCCESS)
        {
            YYSYS_LOG(INFO,"DHC find_rel_info ret=%d",ret);
        }
        else
        {
            oceanbase::common::ObList<ObSqlRawExpr*>::iterator cnd_it;
            double min_row = DBL_MAX;
            double nrows = 0;
            for(cnd_it = remainder_cnd_list.begin(); ret == OB_SUCCESS && cnd_it!=remainder_cnd_list.end();)
            {
                join_method = JoinedTable::MERGE_JOIN;
                ObRawExpr *exp = (*cnd_it)->get_expr();
                if(exp ->is_join_cond_opt())
                {
                    only_has_single_table_cond = false;
                    if(exp->get_expr_type() == T_OP_OR){
                        ret = Opt_JoinedTables_GetOrExpr(exp,left_rel_info,err_stat,nrows,min_row);
                    }else if(exp->get_expr_type() == T_OP_EQ){
                        ret = Opt_JoinedTables_GetEqExpr(exp,left_rel_info,err_stat,nrows,min_row);
                    }
                }
                else
                {

                }
                cnd_it++;
            }
        }
        if(!only_has_single_table_cond)
        {
            if(join_method_array.count() >=1)
            {
                bool merge_join = false,semi_join = false,bloomfilter_join = false,hash_join = false;
                for(int64_t i=0;i<join_method_array.count();i++)
                {
                    switch (join_method_array.at(i))
                    {
                    case JoinedTable::SEMI_JOIN:
                        semi_join = true;
                        break;
                    case JoinedTable::BLOOMFILTER_JOIN:
                        bloomfilter_join = true;
                        break;
                    case JoinedTable::HASH_JOIN:
                        hash_join = true;
                        break;
                    case JoinedTable::MERGE_JOIN:
                        merge_join = true;
                        break;
                    default:
                        break;
                    }
                }
                if(semi_join)
                {
                    join_method = JoinedTable::SEMI_JOIN;
                }
                else if(!merge_join && hash_join)
                {
                    join_method = JoinedTable::HASH_JOIN;
                }
                else if(bloomfilter_join)
                {
                    join_method = JoinedTable::BLOOMFILTER_JOIN;
                }
            }
            joined_table->optimized_join_operator_.push_back(join_method);
        }
        else
        {
            joined_table->optimized_join_operator_.push_back(JoinedTable::MERGE_JOIN);
        }
        return ret;
    }

//del wanjun [530]:b
#if 0
    int ObPhysicalOptimizer::Opt_Calc_JoinedTables_Cost(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat &err_stat,
            ObSelectStmt *select_stmt,
            JoinedTable *joined_table,
            oceanbase::common::ObList<ObSqlRawExpr *> &remainder_cnd_list,
            oceanbase::common::ObList<ObBitSet<> > &bitset_list,
            ObOptimizerRelation *left_rel_info,
            int32_t &joined_table_id)
    {
        int &ret = err_stat.err_code_ = OB_SUCCESS;
        ObBitSet<> left_table_bitset;
        ret = bitset_list.pop_front(left_table_bitset);
        ObBitSet<> right_table_bitset;
        ret = bitset_list.pop_front(right_table_bitset);
        OB_ASSERT(!left_table_bitset.is_empty());
        OB_ASSERT(!right_table_bitset.is_empty());
        ObOptimizerRelation *right_rel_info = NULL;
        bool right_base_rel_is_subquery = false;
        oceanbase::common::ObList<ObOptimizerRelation*> * rel_opt_list = select_stmt->get_rel_opt_list();
        oceanbase::common::ObList<ObOptimizerRelation*>::iterator rel_opt_it = rel_opt_list->begin();
        while (ret == OB_SUCCESS
               && rel_opt_it != rel_opt_list->end()) {
            if(right_table_bitset.has_member(select_stmt->get_table_bit_index((*rel_opt_it)->get_table_id())))
            {
                ret = find_rel_info(logical_plan,physical_plan,err_stat,select_stmt,(*rel_opt_it)->get_table_id(),right_rel_info,join_ctx_.right_base_rel_is_subquery);
                break;
            }
            rel_opt_it++;

        }
        if(right_rel_info == NULL)
        {
            ret = find_rel_info(logical_plan,physical_plan,err_stat,select_stmt,joined_table->table_ids_.at(joined_table_id),right_rel_info,join_ctx_.right_base_rel_is_subquery);
        }
        OB_ASSERT(left_rel_info && right_rel_info);

        int join_method = JoinedTable::MERGE_JOIN;
        bool only_has_single_table_cond = true;
        ObArray<int> join_method_array;
        if(ret != OB_SUCCESS)
        {
            YYSYS_LOG(INFO,"DHC find_rel_info ret =%d",ret);
        }
        else
        {
            oceanbase::common::ObList<ObSqlRawExpr*>::iterator cnd_it;
            double min_row = DBL_MAX;
            double nrows = 0;
            for(cnd_it = remainder_cnd_list.begin(); ret == OB_SUCCESS && cnd_it!=remainder_cnd_list.end();)
            {
                join_method = JoinedTable::MERGE_JOIN;
                if((*cnd_it)->get_expr()->is_join_cond_opt())
                {
                    only_has_single_table_cond = false;
                    ObBinaryOpRawExpr* join_cnd = dynamic_cast<ObBinaryOpRawExpr*>((*cnd_it)->get_expr());
                    ObBinaryRefRawExpr* lexpr = dynamic_cast<ObBinaryRefRawExpr*>(join_cnd->get_first_op_expr());
                    ObBinaryRefRawExpr* rexpr = dynamic_cast<ObBinaryRefRawExpr*>(join_cnd->get_second_op_expr());
                    ObDiffValueInfo diff_value_info_left;
                    ObDiffValueInfo diff_value_info_right;
                    ObOptimizerRelation *left_base_rel_opt = NULL;
                    ObOptimizerRelation *right_base_rel_opt = NULL;
                    bool left_base_rel_is_subquery = false;
                    int32_t left_bit_idx = select_stmt->get_table_bit_index(lexpr->get_first_ref_id());
                    if(!left_table_bitset.has_member(left_bit_idx))
                    {
                        ObBinaryRefRawExpr *tmp_expr = lexpr;
                        lexpr = rexpr;
                        rexpr = tmp_expr;
                    }
                    if(left_rel_info->get_table_id() != lexpr->get_first_ref_id())
                    {
                        ret = find_rel_info(logical_plan,physical_plan,err_stat,select_stmt,lexpr->get_first_ref_id(),left_base_rel_opt,left_base_rel_is_subquery);
                        left_rel_info->print_rel_opt_info();
                    }
                    else
                    {
                        left_base_rel_opt = left_rel_info;
                    }
                    if(left_base_rel_opt == NULL)
                    {

                    }
                    OB_ASSERT(left_base_rel_opt);
                    if(left_base_rel_opt->get_rel_opt_kind() == ObOptimizerRelation::RELOPT_BASEREL)
                    {
                        ret = gen_joined_column_diff_number(logical_plan,physical_plan,err_stat,select_stmt,
                                                            left_base_rel_opt,
                                                            left_base_rel_opt->get_table_id(),
                                                            lexpr->get_second_ref_id(),
                                                            diff_value_info_left);
                    }
                    else
                    {
                        diff_value_info_left.set_default_eastimation(left_base_rel_opt->get_rows());
                    }
                    bool is_base_table = true;
                    if(right_rel_info->get_table_id() != rexpr->get_first_ref_id())
                    {
                        ret = find_rel_info(logical_plan,physical_plan,err_stat,select_stmt,rexpr->get_first_ref_id(),right_base_rel_opt,is_base_table);
                        //right_rel_info->print_rel_opt_info();
                    }
                    else
                    {
                        right_base_rel_opt = right_rel_info;
                    }
                    if(right_base_rel_opt == NULL)
                    {

                    }
                    OB_ASSERT(right_base_rel_opt);
                    if(right_base_rel_opt->get_rel_opt_kind() == ObOptimizerRelation::RELOPT_BASEREL)
                    {
                        ret = gen_joined_column_diff_number(logical_plan,physical_plan,err_stat,select_stmt,right_base_rel_opt,right_base_rel_opt->get_table_id(),rexpr->get_second_ref_id(),diff_value_info_right);
                    }
                    else
                    {
                        diff_value_info_right.set_default_eastimation(right_base_rel_opt->get_rows());
                    }
                    double outer_rows =0;
                    if(joined_table_id ==1)
                    {
                        outer_rows = left_base_rel_opt->get_rows();
                        left_rel_info->set_rows(outer_rows);
                    }
                    else
                    {
                        outer_rows = left_rel_info->get_rows();
                    }
                    double inner_rows = right_rel_info->get_rows();
                    double left_ratio = outer_rows / left_base_rel_opt->get_join_rows();
                    double right_ratio = inner_rows / right_base_rel_opt->get_join_rows();
                    double nrows1 = (left_ratio * right_ratio) *(
                                diff_value_info_left.high_freq_dup_num * diff_value_info_right.high_freq_dup_num *
                                fmin(diff_value_info_left.high_freq_diff_num,diff_value_info_right.high_freq_diff_num)+
                                diff_value_info_left.low_freq_dup_num * diff_value_info_right.low_freq_dup_num *
                                fmin(diff_value_info_left.low_freq_diff_num,diff_value_info_right.low_freq_diff_num));
                    double nrows2 = (left_ratio * right_ratio) *(
                                diff_value_info_left.high_freq_dup_num * diff_value_info_right.low_freq_dup_num *
                                fmin(diff_value_info_left.high_freq_diff_num,diff_value_info_right.low_freq_diff_num) +
                                diff_value_info_left.low_freq_dup_num * diff_value_info_right.high_freq_dup_num *
                                fmin(diff_value_info_left.low_freq_diff_num,diff_value_info_right.high_freq_diff_num));
                    int flag =0;
                    if((diff_value_info_left.high_freq_diff_num==0 &&
                        diff_value_info_right.high_freq_diff_num ==0) ||
                            (diff_value_info_left.low_freq_diff_num ==0 &&
                             diff_value_info_right.low_freq_diff_num ==0))
                    {
                        nrows = nrows1;
                        flag = 1;
                    }
                    else if((diff_value_info_left.high_freq_diff_num ==0 &&
                             diff_value_info_right.low_freq_diff_num ==0) ||
                            (diff_value_info_left.low_freq_diff_num ==0 &&
                             diff_value_info_right.high_freq_diff_num ==0))
                    {
                        nrows = nrows2;
                        flag =2;
                    }
                    else
                    {
                        nrows = (nrows1+nrows2)/2;
                    }

                    bool left_or_inner = false;
                    switch (join_ctx_.joined_table->join_types_.at(join_ctx_.joined_table_id-1)) {
                    case JoinedTable::T_FULL:
                    {
                        if(flag ==1)
                        {
                            nrows += fmax(0,outer_rows - left_ratio * (DHLEFT *CHH + DLLEFT *CLL));
                            nrows += fmax(0,inner_rows - right_ratio * (DHRIGHT * CHH + DLRIGHT *CLL));
                        }
                        else if(flag ==2)
                        {
                            nrows += fmax(0,outer_rows - left_ratio *(DHLEFT * CHL + DLLEFT * CLH));
                            nrows += fmax(0,inner_rows - right_ratio * (DHRIGHT * CLH + DLRIGHT * CHL));
                        }
                        else
                        {
                            double stay_rows = 0;
                            stay_rows += fmax(0,outer_rows - left_ratio * (DHLEFT *CHH + DLLEFT *CLL));
                            stay_rows += fmax(0,outer_rows - left_ratio *(DHLEFT * CHL + DLLEFT * CLH));
                            stay_rows += fmax(0,inner_rows - right_ratio * (DHRIGHT * CHH + DLRIGHT *CLL));
                            stay_rows += fmax(0,inner_rows - right_ratio * (DHRIGHT * CLH + DLRIGHT * CHL));
                            nrows +=stay_rows/2;
                        }
                        if(nrows < outer_rows)
                            nrows = outer_rows;
                        if(nrows < inner_rows)
                            nrows = inner_rows;
                        break;
                    }
                    case JoinedTable::T_LEFT:
                    {
                        left_or_inner = true;
                        if(diff_value_info_left.total_diff_num > diff_value_info_right.total_diff_num)
                        {
                            if(flag ==1)
                            {
                                nrows += fmax(0,outer_rows - left_ratio * (DHLEFT *CHH + DLLEFT *CLL));
                            }
                            else if(flag ==2)
                            {
                                nrows += fmax(0,outer_rows - left_ratio *(DHLEFT * CHL + DLLEFT * CLH));
                            }
                            else
                            {
                                double stay_rows =0;
                                stay_rows += fmax(0,outer_rows - left_ratio * (DHLEFT *CHH + DLLEFT *CLL));
                                stay_rows += fmax(0,outer_rows - left_ratio *(DHLEFT * CHL + DLLEFT * CLH));
                                nrows += stay_rows/2;
                            }
                        }
                        if(nrows < outer_rows)
                            nrows = outer_rows;
                        break;
                    }
                    case JoinedTable::T_RIGHT:
                    {
                        if(flag ==1)
                        {
                            nrows += fmax(0,inner_rows - right_ratio * (DHRIGHT * CHH + DLRIGHT *CLL));
                        }
                        else if(flag ==2)
                        {
                            nrows += fmax(0,inner_rows - right_ratio * (DHRIGHT * CLH + DLRIGHT * CHL));
                        }
                        else
                        {
                            double stay_rows = 0;
                            stay_rows += fmax(0,inner_rows - right_ratio * (DHRIGHT * CHH + DLRIGHT *CLL));
                            stay_rows += fmax(0,inner_rows - right_ratio * (DHRIGHT * CLH + DLRIGHT * CHL));
                            nrows += stay_rows/2;
                        }
                        if(nrows < inner_rows)
                            nrows = inner_rows;
                        break;
                    }
                    case JoinedTable::T_INNER:
                    {
                        left_or_inner = true;
                        break;
                    }
                    default:
                        break;
                    }
                    if(nrows < 1.0)
                    {
                        nrows = 1;
                    }
                    if(min_row > nrows)
                    {
                        bool is_base_table = true;
                        ret = find_rel_info(join_ctx_.logical_plan,join_ctx_.physical_plan,err_stat,join_ctx_.select_stmt,lexpr->get_first_ref_id(),left_base_rel_opt,is_base_table);
                        min_row = nrows;
                        left_rel_info->set_rows(min_row);
                        left_rel_info->set_join_rows(min_row);
                        left_rel_info->set_tuples(min_row);
                        switch (join_ctx_.joined_table->join_types_.at(join_ctx_.joined_table_id-1)) {
                        case JoinedTable::T_FULL:
                            break;
                        case JoinedTable::T_LEFT:
                            left_or_inner = true;
                            right_base_rel_opt->set_join_rows(fmin(right_base_rel_opt->get_rows(),right_base_rel_opt->get_join_rows()));
                            break;
                        case JoinedTable::T_RIGHT:
                            left_base_rel_opt->set_join_rows(fmin(left_base_rel_opt->get_rows(),left_base_rel_opt->get_join_rows()));
                            break;
                        case JoinedTable::T_INNER:
                        {
                            left_or_inner = true;
                            double left_join_rows = 0;
                            double right_join_rows = 0;
                            if(flag ==1)
                            {
                                left_join_rows = DHLEFT*CHH + DLLEFT*CLL;
                                right_join_rows = DHRIGHT*CHH + DLRIGHT*CLL;
                            }
                            else if(flag ==2)
                            {
                                left_join_rows = DHLEFT*CHL + DLLEFT*CLH;
                                right_join_rows = DHRIGHT*CLH + DLRIGHT*CHL;
                            }
                            else
                            {
                                double join_rows =0;
                                join_rows +=DHLEFT*CHH + DLLEFT*CLL;
                                join_rows +=DHLEFT*CHL + DLLEFT*CLH;
                                left_join_rows = join_rows /2;

                                join_rows =0;
                                join_rows +=DHRIGHT*CHH + DLRIGHT*CLL;
                                join_rows +=DHRIGHT*CLH + DLRIGHT*CHL;
                                right_join_rows = join_rows /2;
                            }
                            left_base_rel_opt->set_join_rows(fmin(left_base_rel_opt->get_join_rows(),left_join_rows));
                            right_base_rel_opt->set_join_rows(fmin(right_base_rel_opt->get_join_rows(),right_join_rows));
                            break;
                        }
                        default:
                            break;
                        }
                    }
                        if((*cnd_it)->get_expr()->is_join_cond())
                        {
                            ObObjType left_column_type = common::ObMinType;
                            ObObjType right_column_type = common::ObMaxType;
                            if((ret = find_column_type(join_ctx_.logical_plan,join_ctx_.select_stmt,lexpr->get_first_ref_id(),lexpr->get_second_ref_id(),left_column_type)) != OB_SUCCESS)
                            {
                                YYSYS_LOG(INFO,"CANT find column type");
                            }
                            else if((ret = find_column_type(join_ctx_.logical_plan,join_ctx_.select_stmt,rexpr->get_first_ref_id(),rexpr->get_second_ref_id(),right_column_type)) != OB_SUCCESS)
                            {
                                YYSYS_LOG(INFO,"CANT find column type");
                            }
                            else if(left_column_type == right_column_type && right_column_type == common::ObDecimalType
                                    && diff_value_info_left.total_diff_num >= MIN_HASH_JOIN_DIFF_NUM_THRESHOLD
                                    && !(diff_value_info_left.data_skew >= DATA_SKEW_THRESHOLD
                                         && diff_value_info_right.data_skew >= DATA_SKEW_THRESHOLD
                                         && DHLEFT *DHRIGHT >= HASH_BUCKET_DEPTH_THRESHOLD)
                                    && diff_value_info_left.total_diff_num <= MAX_HASH_JOIN_ROWS)
                            {
                                join_method = JoinedTable::HASH_JOIN;
                            }
                            else if(left_or_inner && !join_ctx_.right_base_rel_is_subquery
                                    && ((diff_value_info_left.data_skew < DATA_SKEW_THRESHOLD
                                         && diff_value_info_left.total_diff_num < SEMI_DIFF_NUM_THRESHOLD)
                                        || (diff_value_info_left.data_skew >= DATA_SKEW_THRESHOLD
                                            && outer_rows < SEMI_ROW_NUM_THRESHOLD)))
                            {
                                join_method = JoinedTable::SEMI_JOIN;
                                right_base_rel_opt->reset_semi_join_right_index_table_const(rexpr->get_second_ref_id());
                            }
                            else if(left_column_type == right_column_type
                                    && !(diff_value_info_left.data_skew >= DATA_SKEW_THRESHOLD
                                         && diff_value_info_right.data_skew >= DATA_SKEW_THRESHOLD
                                         && DHLEFT*DHRIGHT >= HASH_BUCKET_DEPTH_THRESHOLD)
                                    && diff_value_info_left.total_diff_num < MAX_HASH_JOIN_ROWS)
                            {
                                join_method = JoinedTable::HASH_JOIN;
                            }
                            else
                            {
                                join_method = JoinedTable::MERGE_JOIN;
                            }
                        }
                        else
                        {

                        }
                        join_method_array.push_back(join_method);
                }
                else
                {

                }
                cnd_it++;
            }
        }
        if(!only_has_single_table_cond)
        {
            if(join_method_array.count() >=1)
            {
                bool merge_join = false,semi_join = false,bloomfilter_join = false,hash_join = false;
                for(int64_t i=0;i<join_method_array.count();i++)
                {
                    switch (join_method_array.at(i)) {
                    case JoinedTable::SEMI_JOIN:
                        semi_join = true;
                        break;
                    case JoinedTable::BLOOMFILER_JOIN:
                        bloomfilter_join = true;
                        break;
                    case JoinedTable::HASH_JOIN:
                        hash_join = true;
                        break;
                    case JoinedTable::MERGE_JOIN:
                        merge_join = true;
                        break;
                    default:
                        break;
                    }
                }
                if(semi_join)
                {
                    join_method = JoinedTable::SEMI_JOIN;
                }
                else if(!merge_join && hash_join)
                {
                    join_method = JoinedTable::HASH_JOIN;
                }
                else if(bloomfilter_join)
                {
                    join_method = JoinedTable::BLOOMFILTER_JOIN;
                }
            }
            joined_table->optimized_join_operator_.push_back(join_method);
        }
        else
        {
            joined_table->optimized_join_operator_.push_back(JoinedTable::MERGE_JOIN);
        }
        return ret;
    }
#endif
//del wangjun [530] :e

    int ObPhysicalOptimizer::find_column_type(
            ObLogicalPlan *logical_plan,
            ObSelectStmt *select_stmt,
            uint64_t table_id,
            uint64_t column_id,
            ObObjType &column_type
            )
    {
        int ret = OB_SUCCESS;
        ObSchemaChecker schema_checker;
        schema_checker.set_schema(*transformer_->get_sql_context()->schema_manager_);
        TableItem* table_item = NULL;
        const ColumnItem* col_item = NULL;
        if(table_id == OB_INVALID_ID || column_id == OB_INVALID_ID || (col_item = select_stmt->get_column_item_by_id(table_id,column_id)) == NULL)
        {
            ret = OB_ERR_ILLEGAL_ID;
        }
        else if((table_item = select_stmt->get_table_item_by_id(table_id)) == NULL)
        {
            ret = OB_ERR_ILLEGAL_ID;
        }
        else
        {
            switch (table_item->type_)
            {
            case TableItem::BASE_TABLE:
            case TableItem::ALIAS_TABLE:
            {
                const ObColumnSchemaV2 * col_schema = schema_checker.get_column_schema(table_item->table_name_,col_item->column_name_);
                if(col_schema != NULL)
                {
                    column_type = col_schema->get_type();
                }
                break;
            }
            case TableItem::GENERATED_TABLE:
            {
                ObBasicStmt* stmt = logical_plan->get_query(table_item->ref_id_);
                if(stmt == NULL)
                {
                    ret = OB_ERR_ILLEGAL_ID;
                }
                ObSelectStmt* sub_select_stmt = static_cast<ObSelectStmt*>(stmt);
                int32_t num = sub_select_stmt->get_select_item_size();
                for(int32_t i=0;i<num;i++)
                {
                    const SelectItem& select_item = sub_select_stmt->get_select_item(i);
                    if(col_item->column_name_ == select_item.alias_name_)
                    {
                        column_type = select_item.type_;
                    }
                }
                break;
            }
            default:
                ret = OB_ERROR;
                break;
            }
        }
        return ret;
    }

    int ObPhysicalOptimizer::find_rel_info(
            ObLogicalPlan *logical_plan,
            ObPhysicalPlan *physical_plan,
            ErrStat &err_stat,
            ObSelectStmt *select_stmt,
            uint64_t table_id,
            ObOptimizerRelation *& tmp_relation,
            bool &is_sub_query_table
            )
    {
        int ret =OB_SUCCESS;
        TableItem* table_item = NULL;
        if(table_id == OB_INVALID_ID || (table_item = select_stmt->get_table_item_by_id(table_id)) == NULL)
        {
            ret = OB_ERR_ILLEGAL_ID;
        }
        if(ret == OB_SUCCESS)
        {
            switch (table_item->type_)
            {
            case TableItem::BASE_TABLE:
            case TableItem::ALIAS_TABLE:
            {
                oceanbase::common::ObList<ObOptimizerRelation*> *rel_opt_list = select_stmt->get_rel_opt_list();
                oceanbase::common::ObList<ObOptimizerRelation*>::iterator rel_opt_it = rel_opt_list->begin();
                is_sub_query_table = false;
                while(ret == OB_SUCCESS
                      && rel_opt_it != rel_opt_list->end())
                {
                    if(table_id == (*rel_opt_it)->get_table_id())
                    {
                        tmp_relation = *rel_opt_it;
                        break;
                    }
                    rel_opt_it++;
                }
                OB_ASSERT(tmp_relation);

                break;
            }
            case TableItem::GENERATED_TABLE:
            {
                is_sub_query_table = true;
                oceanbase::common::ObList<ObOptimizerRelation*> *subquery_rel_opt_list = select_stmt->get_subquery_rel_opt_list();
                oceanbase::common::ObList<ObOptimizerRelation*>::iterator subquery_rel_opt_it = subquery_rel_opt_list->begin();
                bool find_out_sub_rel = false;
                while(ret == OB_SUCCESS
                      && subquery_rel_opt_it != subquery_rel_opt_list->end())
                {
                    if(table_id == (*subquery_rel_opt_it)->get_table_id())
                    {
                        tmp_relation = *subquery_rel_opt_it;
                        find_out_sub_rel = true;
                        break;
                    }
                    subquery_rel_opt_it++;
                }
                if(!find_out_sub_rel)
                {
                    ObOptimizerRelation* new_tmp_relation = NULL;
                    void *buf = logical_plan->get_name_pool()->alloc(sizeof(ObOptimizerRelation));
                    if(buf ==NULL)
                    {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        YYSYS_LOG(ERROR,"fail to new ObOptimizerRelation");
                    }
                    else
                    {
                        new_tmp_relation = new(buf)ObOptimizerRelation(ObOptimizerRelation::RELOPT_INIT);
                    }
                    ret = gen_join_method(logical_plan,physical_plan,err_stat,table_item->ref_id_,new_tmp_relation);
                    if(ret == OB_SUCCESS)
                    {
                        ObOptimizerRelation* new_tmp_relation_V2 = NULL;
                        void *buf2 =logical_plan->get_name_pool()->alloc(sizeof(ObOptimizerRelation));
                        if(buf2 ==NULL)
                        {
                            ret = OB_ALLOCATE_MEMORY_FAILED;
                            YYSYS_LOG(ERROR,"fail to new ObOptimizerRelation");
                        }
                        else
                        {
                            new_tmp_relation_V2 = new(buf2)ObOptimizerRelation();
                        }
                        new_tmp_relation_V2->set_rel_opt_kind(ObOptimizerRelation::RELOPT_SUBQUERY);
                        new_tmp_relation_V2->set_tuples(new_tmp_relation->get_tuples());
                        new_tmp_relation_V2->set_rows(new_tmp_relation->get_rows());
                        new_tmp_relation_V2->set_join_rows(new_tmp_relation->get_join_rows());
                        new_tmp_relation_V2->set_table_id(table_item->table_id_);
                        subquery_rel_opt_list->push_back(new_tmp_relation_V2);
                        if(new_tmp_relation->get_rel_opt_kind() == ObOptimizerRelation::RELOPT_INIT)
                        {
                            new_tmp_relation->~ObOptimizerRelation();
                        }
                        tmp_relation = new_tmp_relation_V2;
                    }
                    else
                    {
                        new_tmp_relation->~ObOptimizerRelation();
                        tmp_relation = NULL;
                        YYSYS_LOG(INFO, "DHC err gen join method/subquery planer");
                    }
                }
                break;
            }
            default:
                OB_ASSERT(0);
                break;
            }
        }
        return ret;
    }

}
}
