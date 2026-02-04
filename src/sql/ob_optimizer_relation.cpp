#include "ob_optimizer_relation.h"
#include <math.h>
#include <float.h>

namespace oceanbase
{
  namespace sql
  {

    double ObJoinStatCalculator::clamp_row_est(double nrows)
    {
      if(nrows <=1.0)
        nrows = 1.0;
      else
        nrows = rint(nrows);
      return nrows;
    }

    double ObJoinStatCalculator::rint(double x)
    {
      return (x>=0.0) ?floor(x+0.5):ceil(x-0.5);
    }

    double ObJoinStatCalculator::calc_joinrel_size_estimate(ObLogicalPlan *logical_plan,
                                                            ObSelectStmt *select_stmt,
                                                            const uint64_t join_type,
                                                            const double outer_rows,
                                                            const double inner_rows,
                                                            oceanbase::common::ObList<ObSqlRawExpr*>& restrictlist)
    {
      UNUSED(select_stmt);
      UNUSED(logical_plan);
      UNUSED(restrictlist);
      Selectivity jselec =0.5;
      Selectivity pselec =0.5;
      double nrows;
      switch (join_type)
      {
        case JoinedTable::T_FULL:
          nrows = outer_rows * inner_rows *jselec;
          if(nrows < outer_rows)
            nrows = outer_rows;
          if(nrows < inner_rows)
            nrows = inner_rows;
          nrows *= pselec;
          break;
        case JoinedTable::T_RIGHT:
        case JoinedTable::T_LEFT:
          nrows = outer_rows * inner_rows *jselec;
          if(nrows < outer_rows)
            nrows = outer_rows;
          nrows *= pselec;
          break;
        case JoinedTable::T_INNER:
          nrows = outer_rows * inner_rows *jselec;
          break;
        default:
          nrows =0;
          break;
      }
      YYSYS_LOG(ERROR,"DHC end calc_joinrel_size estimate=%lf",nrows);
      return clamp_row_est(nrows);
    }

    Selectivity
    ObJoinStatCalculator::clauselist_selectivity(ObLogicalPlan *logical_plan,
                                                 ObSelectStmt *select_stmt,
                                                 const uint64_t join_type,
                                                 oceanbase::common::ObList<ObSqlRawExpr*>& restrictlist)
    {
      UNUSED(logical_plan);
      UNUSED(select_stmt);
      UNUSED(join_type);
      UNUSED(restrictlist);
      Selectivity s1=1.0;
      return s1;
    }
    int cast_obj_to_exprObj(char *buf,ObObj &obj,
                            const ObObj &value,
                            ObExprObj &e_value)

    {
      int ret = OB_SUCCESS;
      obj.set_type(value.get_type());
      buf[0] = '\0';
      if(value.get_type() == ObVarcharType)
      {
        ObString str,cstr;
        value.get_varchar(str);
        str.to_string(buf,str.length()+1);
        buf[str.length()] = '\0';
        cstr.assign(buf,str.length()+1);
        obj.set_varchar(cstr);
        e_value.assign(obj);
      }
      else
      {
        e_value.assign(value);
      }
      return ret;
    }

    bool ObStatSelCalculator::is_unique_rowkey_column(ObOptimizerRelation *rel_opt,
                                                      const uint64_t table_id,
                                                      const uint64_t column_id)
    {
      bool unique_rowkey_column = false;
      UNUSED(table_id);
      uint64_t table_ref_id = rel_opt->get_table_ref_id();
      uint64_t c_id=0;
      const common::ObSchemaManagerV2 *schema_managerv2 = rel_opt->get_schema_managerv2();
      const common::ObTableSchema *table_schema = NULL;
      if(rel_opt->get_table_id() != table_id)
      {

      }
      else if(schema_managerv2 == NULL)
      {
        YYSYS_LOG(ERROR, "shema_managerv2 is null.");
      }
      else
      {
        table_schema = schema_managerv2->get_table_schema(table_ref_id);
      }

      if(rel_opt->get_table_id() != table_id)
      {

      }
      else if(table_schema == NULL)
      {
        YYSYS_LOG(ERROR, "table schema is null.");}
      else
      {
        common::ObRowkeyInfo rowkey_info = table_schema->get_rowkey_info();
        if(rowkey_info.get_size()!=1)
        {
        }
        else if(OB_SUCCESS == rowkey_info.get_column_id(0,c_id) && c_id == column_id)
        {
          unique_rowkey_column = true;
        }
      }
      return unique_rowkey_column;
    }

    double ObStatSelCalculator::get_equal_selectivity(ObOptimizerRelation *rel_opt,
                                                      ObSelInfo &sel_info,
                                                      const oceanbase::common::ObObj &value)
    {
      double sel =0.0;
      uint64_t table_id = sel_info.table_id_;
      uint64_t column_id = sel_info.columun_id_;
      int ret = OB_SUCCESS;
      common::ObExprObj e_value;
      ObObj obj;
      char buf[OB_MAX_VARCHAR_LENGTH/32];
      cast_obj_to_exprObj(buf,obj,value,e_value);
      int cmp =0;
      bool flag = false;
      if(rel_opt->get_table_id()!=table_id)
      {
        sel=1.0;
        sel_info.selectivity_ = sel;
        sel_info.enable = false;
      }
      else
      {
        bool unique_column = is_unique_rowkey_column(rel_opt,table_id,column_id);
        if(unique_column)
        {
          sel = 1.0 / rel_opt->get_tuples();
        }
        else
        {
          ObBaseRelStatInfo *rel_stat_info = NULL;
          ObColumnStatInfo *col_stat_info = NULL;
          if(OB_SUCCESS == (ret = rel_opt->get_base_rel_stat_info(table_id,rel_stat_info))
             && rel_stat_info != NULL)
          {
            if(rel_stat_info->empty_table_)
            {
              sel=1.0;
            }
            else if(OB_SUCCESS == (ret = rel_opt->get_column_stat_info(table_id,column_id,col_stat_info))
                    && col_stat_info != NULL)
            {
              common::hash::ObHashMap<oceanbase::common::ObObj,double,common::hash::NoPthreadDefendMode>::const_iterator iter=
                  col_stat_info->value_frequency_map_.begin();
              common::ObExprObj v_map;
              for(;iter != col_stat_info->value_frequency_map_.end();iter++)
              {
                v_map.assign(iter->first);
                if(v_map.get_type() == ObNullType)
                {
                  if(e_value.get_type() == ObNullType)
                  {
                    sel = iter->second;
                    flag = true;
                    break;
                  }
                  continue;
                }
                ret = e_value.compare(v_map,cmp);
                if(ret == OB_RESULT_UNKNOWN)
                {
                  ret = OB_SUCCESS;
                }
                if(ret != OB_SUCCESS)
                {}
                else if(cmp ==0)
                {
                  sel = iter->second;
                  flag = true;
                  break;
                }
                else
                {

                }
              }
              if(!flag)
              {
                sel = col_stat_info->avg_frequency_;
              }
            }
            else
            {
              sel = DEFAULT_EQ_SEL;
            }
          }
          else
          {
            sel =DEFAULT_EQ_SEL;
          }
        }
        CLAMP_PROBABILITY(sel);
        sel_info.enable = true;
        sel_info.selectivity_ = sel;
      }
      return sel;
    }

    double ObStatSelCalculator::get_equal_subquery_selectivity(ObOptimizerRelation *rel_opt,
                                                               ObSelInfo &sel_info)
    {
      double sel=0.0;
      double high_frequency=0.0;
      uint64_t table_id = sel_info.table_id_;
      uint64_t column_id = sel_info.columun_id_;
      int ret = OB_SUCCESS;
      if(rel_opt->get_table_id()!=table_id)
      {
        sel=1.0;
        sel_info.selectivity_ = sel;
        sel_info.enable = false;
      }
      else
      {
        bool unique_column = is_unique_rowkey_column(rel_opt,table_id,column_id);
        if(unique_column)
        {
          sel = 1.0 / rel_opt->get_tuples();
        }
        else
        {
          ObColumnStatInfo *col_stat_info = NULL;
          if(OB_SUCCESS == (ret = rel_opt->get_column_stat_info(table_id,column_id,col_stat_info))
             && col_stat_info != NULL)
          {
            common::hash::ObHashMap<oceanbase::common::ObObj,double,common::hash::NoPthreadDefendMode>::const_iterator iter=
                col_stat_info->value_frequency_map_.begin();
            for(;iter != col_stat_info->value_frequency_map_.end();iter++)
            {
              high_frequency +=iter->second;
            }
            sel = 1.0/col_stat_info->distinct_num_
                  * high_frequency + (1.0 - high_frequency) * col_stat_info->avg_frequency_;
          }
          else
          {
            sel = DEFAULT_EQ_SEL;
          }
        }
        CLAMP_PROBABILITY(sel);
        sel_info.enable = true;
        sel_info.selectivity_ = sel;
      }
      return sel;
    }
    double ObStatSelCalculator::get_lt_selectivity(ObOptimizerRelation *rel_opt,
                                                   ObSelInfo &sel_info,
                                                   const oceanbase::common::ObObj &value)
    {
      double sel=0.0;
      double frequency1 =0.0;
      double frequency2 =0.0;
      double frequency3 =0.0;
      uint64_t table_id = sel_info.table_id_;
      uint64_t column_id = sel_info.columun_id_;
      int ret= OB_SUCCESS;
      common::ObExprObj e_value;
      ObObj obj;
      char buf[OB_MAX_VARCHAR_LENGTH/32];
      cast_obj_to_exprObj(buf,obj,value,e_value);
      int cmp =0;
      if(rel_opt->get_table_id()!=table_id)
      {
        sel=1.0;
        sel_info.selectivity_ = sel;
        sel_info.enable = false;
      }
      else
      {
        ObBaseRelStatInfo *rel_stat_info = NULL;
        ObColumnStatInfo *col_stat_info = NULL;
        if(OB_SUCCESS == (ret = rel_opt->get_base_rel_stat_info(table_id,rel_stat_info))
           && rel_stat_info != NULL)
        {
          if(rel_stat_info->empty_table_)
          {
            sel=1.0;
          }
          else if(OB_SUCCESS == (ret = rel_opt->get_column_stat_info(table_id,column_id,col_stat_info))
                  && col_stat_info != NULL)
          {
            common::hash::ObHashMap<oceanbase::common::ObObj,double,common::hash::NoPthreadDefendMode>::const_iterator iter = col_stat_info->value_frequency_map_.begin();
            common::ObExprObj v_map;
            for(;iter != col_stat_info->value_frequency_map_.end();iter++)
            {
              v_map.assign(iter->first);
              if(v_map.get_type() == ObNullType)
              {
                if(e_value.get_type() == ObNullType)
                {
                  frequency2 += iter->second;
                }
                else
                {
                  frequency3 += iter->second;
                }
                continue;
              }
              ret = e_value.compare(v_map,cmp);
              if (ret != OB_SUCCESS)
              {
                YYSYS_LOG(DEBUG, "WARN: compare fail =>%d", ret);
              }
              else if(cmp>0)
              {
                frequency1 += iter->second;
              }
              else if(cmp ==0)
              {
                frequency2 += iter->second;
              }
              else
              {
                frequency3 += iter->second;
              }
            }
            sel += frequency1;
            sel += (1.0-frequency1-frequency2-frequency3)*
                   common::calc_proportion(e_value,col_stat_info->max_value_,col_stat_info->min_value_);
          }
          else
          {
            sel = DEFAULT_INEQ_SEL;
          }
        }
        else
        {
          sel = DEFAULT_INEQ_SEL;
        }
        CLAMP_PROBABILITY(sel);
        sel_info.enable = true;
        sel_info.selectivity_ = sel;
      }
      return sel;
    }

    double ObStatSelCalculator::get_le_selectivity(ObOptimizerRelation *rel_opt,
                                                   ObSelInfo &sel_info,
                                                   const oceanbase::common::ObObj &value)
    {
      double sel=0.0;
      double frequency1 =0.0;
      double frequency2 =0.0;
      double frequency3 =0.0;
      uint64_t table_id = sel_info.table_id_;
      uint64_t column_id = sel_info.columun_id_;
      int ret= OB_SUCCESS;
      common::ObExprObj e_value;
      ObObj obj;
      char buf[OB_MAX_VARCHAR_LENGTH/32];
      cast_obj_to_exprObj(buf,obj,value,e_value);
      int cmp =0;
      if(rel_opt->get_table_id()!=table_id)
      {
        sel=1.0;
        sel_info.selectivity_ = sel;
        sel_info.enable = false;
      }
      else
      {
        ObBaseRelStatInfo *rel_stat_info = NULL;
        ObColumnStatInfo *col_stat_info = NULL;
        if(OB_SUCCESS == (ret = rel_opt->get_base_rel_stat_info(table_id,rel_stat_info))
           && rel_stat_info != NULL)
        {
          if(rel_stat_info->empty_table_)
          {
            sel=1.0;
          }
          else if(OB_SUCCESS == (ret = rel_opt->get_column_stat_info(table_id,column_id,col_stat_info))
                  && col_stat_info != NULL)
          {
            common::hash::ObHashMap<oceanbase::common::ObObj,double,common::hash::NoPthreadDefendMode>::const_iterator iter = col_stat_info->value_frequency_map_.begin();
            common::ObExprObj v_map;
            for(;iter != col_stat_info->value_frequency_map_.end();iter++)
            {
              v_map.assign(iter->first);
              if(v_map.get_type() == ObNullType)
              {
                if(e_value.get_type() == ObNullType)
                {
                  frequency2 += iter->second;
                }
                else
                {
                  frequency3 += iter->second;
                }
                continue;
              }
              ret = e_value.compare(v_map,cmp);
              if (ret != OB_SUCCESS)
              {
                YYSYS_LOG(DEBUG, "WARN: compare fail =>%d", ret);
              }
              else if(cmp>0)
              {
                frequency1 += iter->second;
              }
              else if(cmp ==0)
              {
                frequency2 += iter->second;
              }
              else
              {
                frequency3 += iter->second;
              }
            }
            sel += frequency1 + frequency2;
            sel += (1.0-frequency1-frequency2-frequency3)*
                   common::calc_proportion(e_value,col_stat_info->max_value_,col_stat_info->min_value_);
          }
          else
          {
            sel = DEFAULT_INEQ_SEL;
          }
        }
        else
        {
          sel = DEFAULT_INEQ_SEL;
        }
        CLAMP_PROBABILITY(sel);
        sel_info.enable = true;
        sel_info.selectivity_ = sel;
      }
      return sel;
    }

    double ObStatSelCalculator::get_btw_selectivity(ObOptimizerRelation *rel_opt,
                                                    ObSelInfo &sel_info,
                                                    const oceanbase::common::ObObj &value1,
                                                    const oceanbase::common::ObObj &value2)
    {
      double sel=0.0;
      double frequency1 =0.0;
      double frequency2 =0.0;
      double frequency3 =0.0;
      uint64_t table_id = sel_info.table_id_;
      uint64_t column_id = sel_info.columun_id_;
      int ret= OB_SUCCESS;
      common::ObExprObj e_value1;
      common::ObExprObj e_value2;
      ObObj obj1;
      char buf1[OB_MAX_VARCHAR_LENGTH/32];
      cast_obj_to_exprObj(buf1,obj1,value1,e_value1);
      ObObj obj2;
      char buf2[OB_MAX_VARCHAR_LENGTH/32];
      cast_obj_to_exprObj(buf2,obj2,value2,e_value2);
      int cmp =0;
      if(rel_opt->get_table_id()!=table_id)
      {
        sel=1.0;
        sel_info.selectivity_ = sel;
        sel_info.enable = false;
      }
      else if(value1>value2)
      {
        sel=0.0;
        sel_info.selectivity_ = sel;
        sel_info.enable = true;
      }
      else
      {
        ObBaseRelStatInfo *rel_stat_info = NULL;
        ObColumnStatInfo *col_stat_info = NULL;
        if(OB_SUCCESS == (ret = rel_opt->get_base_rel_stat_info(table_id,rel_stat_info))
           && rel_stat_info != NULL)
        {
          if(rel_stat_info->empty_table_)
          {
            sel=1.0;
          }
          else if(OB_SUCCESS != (ret = rel_opt->get_column_stat_info(table_id,column_id,col_stat_info))
                  || col_stat_info == NULL)
          {
            sel = DEFAULT_RANGE_INEQ_SEL;
          }
          else if(common::is_numerical_type(col_stat_info->min_value_) && common::is_numerical_type(col_stat_info->max_value_))
          {
            common::hash::ObHashMap<oceanbase::common::ObObj,double,common::hash::NoPthreadDefendMode>::const_iterator iter = col_stat_info->value_frequency_map_.begin();
            common::ObExprObj v_map;
            for(;iter != col_stat_info->value_frequency_map_.end();iter++)
            {
              print_obj(value1);
              print_obj(value2);
              print_obj(iter->first);
              v_map.assign(iter->first);
              if(v_map.get_type() == ObNullType)
              {
                if(e_value2.get_type() == ObNullType)
                {
                  frequency2 += iter->second;
                }
                else
                {
                  frequency3 += iter->second;
                }
                continue;
              }
              ret = e_value1.compare(v_map,cmp);
              if (ret != OB_SUCCESS)
              {
                YYSYS_LOG(DEBUG, "WARN: compare fail =>%d", ret);
              }
              else if(cmp>0)
              {
                frequency1 += iter->second;
              }
              else if(cmp ==0)
              {
                frequency2 += iter->second;
              }
              else
              {
                e_value2.compare(v_map,cmp);
                if(cmp>=0)
                {
                  frequency2 += iter->second;
                }
                else
                {
                  frequency3 += iter->second;
                }
              }
            }
            double max_proportion = common::calc_proportion(e_value2,col_stat_info->max_value_,col_stat_info->min_value_);
            double min_proportion = common::calc_proportion(e_value1,col_stat_info->max_value_,col_stat_info->min_value_);
            sel += frequency2;
            sel += (1.0-frequency1-frequency2-frequency3)*
                   (max_proportion - min_proportion);
          }
          else
          {
            sel = DEFAULT_RANGE_INEQ_SEL;
          }
        }
        else
        {
          sel = DEFAULT_RANGE_INEQ_SEL;
        }
        CLAMP_PROBABILITY(sel);
        sel_info.enable = true;
        sel_info.selectivity_ = sel;
      }
      return sel;
    }
#define FIXED_CHAR_SEL  0.20
#define CHAR_RANGE_SEL  0.25
#define ANY_CHAR_SEL    0.9
#define FULL_WILDCARD_SEL  5.0
#define PARTIAL_WILDCARD_SEL  2.0

    double ObStatSelCalculator::get_like_selectivity(ObOptimizerRelation *rel_opt,
                                                     ObSelInfo &sel_info,
                                                     const oceanbase::common::ObObj &value)
    {
      double sel =1.0;
      bool is_use_default_like_selectivity = true;
      uint64_t table_id = sel_info.table_id_;
      uint64_t column_id = sel_info.columun_id_;
      UNUSED(value);
      UNUSED(column_id);
      if(rel_opt->get_table_id()!=table_id)
      {
        sel=1.0;
        sel_info.selectivity_ = sel;
        sel_info.enable = false;
      }
      else if(is_use_default_like_selectivity)
      {
        sel = DEFAULT_MATCH_SEL;
        sel_info.enable = true;
        sel_info.selectivity_ = sel;
      }
      else if(value.get_type() == ObVarcharType)
      {
        int pos,pattlen;
        char patt[OB_MAX_VARCHAR_LENGTH/32] = {0};
        ObString str;
        value.get_varchar(str);
        str.to_string(patt,str.length()+1);
        patt[str.length()] = '\0';
        pattlen = str.length();
        for(pos =0;pos<pattlen;pos++)
        {
          if(patt[pos] =='%' ||
             patt[pos] == '_')
            break;
          if(patt[pos] == '\\')
          {
            pos++;
            if(pos>= pattlen)
              break;
          }
        }
        for(;pos<pattlen;pos++)
        {
          if(patt[pos] !='%' && patt[pos] !='_')
            break;
        }
        for(;pos<pattlen;pos++)
        {
          if(patt[pos] == '%')
            sel *= FULL_WILDCARD_SEL;
          else if(patt[pos] == '_')
            sel *= ANY_CHAR_SEL;
          else if(patt[pos] == '\\')
          {
            pos++;
            if(pos >=pattlen)
              break;
            sel *= FIXED_CHAR_SEL;
          }
          else
            sel *= FIXED_CHAR_SEL;
        }
      }
      else
      {
        sel = DEFAULT_MATCH_SEL;
        sel_info.enable = true;
        sel_info.selectivity_ = sel;
      }
      CLAMP_PROBABILITY(sel);
      sel_info.enable = true;
      sel_info.selectivity_ = sel;
      return sel;
    }

    double ObStatSelCalculator::get_in_selectivity(ObOptimizerRelation *rel_opt,
                                                   ObSelInfo &sel_info,
                                                   const common::ObArray<ObObj> &value_array)
    {
      double sel =0.0;
      uint64_t table_id = sel_info.table_id_;
      if(rel_opt->get_table_id()!=table_id)
      {
        sel=1.0;
        sel_info.selectivity_ = sel;
        sel_info.enable = false;
      }
      else
      {
        sel_info.enable = true;
        for(int64_t i=0;i<value_array.count();i++)
        {
          sel += get_equal_selectivity(rel_opt,sel_info,value_array.at(i));
        }
      }
      CLAMP_PROBABILITY(sel);
      sel_info.selectivity_ = sel;
      return sel;
    }

    int ObStatCostCalculator::get_sort_cost(double tuples,Cost &cost)
    {
      int ret = OB_SUCCESS;
      double comparison_cost = 0.0,run_cost =0.0;
      comparison_cost += 2.0 * DEFAULT_CPU_OPERATOR_COST;
      if(tuples <2)
      {
        tuples = 2;
      }
      cost += comparison_cost * tuples * LOG2(tuples);
      run_cost = tuples * DEFAULT_CPU_OPERATOR_COST;
      cost+= run_cost;
      return ret;
    }

    int ObStatCostCalculator::get_group_by_cost(ObSelectStmt *select_stmt,
                                                ObOptimizerRelation *rel_opt,
                                                ObIndexTableInfo *index_table_info,
                                                Cost &cost)
    {
      int ret = OB_SUCCESS;
      if(select_stmt == NULL || rel_opt == NULL)
      {
        ret = OB_ERROR;
      }
      else
      {
        if((index_table_info == NULL
            && rel_opt->get_group_by_num()!=0
            && !rel_opt->get_seq_scan_info().group_by_applyed_)
           ||(rel_opt->get_group_by_num()!=0
              && index_table_info != NULL
              && !(index_table_info->group_by_applyed_)))
        {
          get_sort_cost(rel_opt->get_rows(),cost);
          cost+= rel_opt->get_rows() * DEFAULT_CPU_OPERATOR_COST * rel_opt->get_group_by_num();
        }
        else
        {}
      }
      return ret;
    }

    int ObStatCostCalculator::get_order_by_cost(ObSelectStmt *select_stmt,
                                                ObOptimizerRelation *rel_opt,
                                                ObIndexTableInfo *index_table_info,
                                                Cost &cost)
    {
      int ret = OB_SUCCESS;
      if(select_stmt == NULL || rel_opt == NULL)
      {
        ret = OB_ERROR;
      }
      else
      {
        if((index_table_info == NULL
            && rel_opt->get_order_by_num()!=0
            && !rel_opt->get_seq_scan_info().order_by_applyed_)
           ||(rel_opt->get_order_by_num()!=0
              && index_table_info != NULL
              && !(index_table_info->order_by_applyed_)))
        {
          get_sort_cost(rel_opt->get_rows(),cost);
        }
        else
        {}
      }
      return ret;
    }

    int ObStatCostCalculator::get_cost_seq_scan(ObSelectStmt *select_stmt,
                                                ObOptimizerRelation *rel_opt,
                                                Cost &cost)
    {
      UNUSED(select_stmt);
      int ret = OB_SUCCESS;
      Cost total_cost = 0.0;
      Cost width =0;
      Cost cpu_run_cost=0.0;
      Cost disk_run_cost =0.0;
      Cost group_by_cost =0.0;
      Cost order_by_cost =0.0;
      ObBaseRelStatInfo *rel_stat_info = NULL;
      ObStatExtractor *stat_extractor = NULL;
      stat_extractor = rel_opt->get_stat_extractor();
      common::hash::ObHashMap<uint64_t,ObBaseRelStatInfo*,common::hash::NoPthreadDefendMode>* table_id_statInfo_map = NULL;
      table_id_statInfo_map = rel_opt->get_table_id_statInfo_map();
      if(table_id_statInfo_map == NULL || stat_extractor == NULL)
      {
        ret = OB_ERROR;
      }
      else
      {
        ret = table_id_statInfo_map->get(rel_opt->get_table_id(),rel_stat_info);
        if(common::hash::HASH_EXIST == ret && rel_stat_info !=NULL && rel_stat_info->enable_statinfo)
        {
          ret = OB_SUCCESS;
        }
        else
        {
          ret = OB_ERROR;
        }
      }
      if(OB_SUCCESS != ret)
      {
        if(rel_stat_info != NULL)
        {
          total_cost = 1.0e10;
          ret = OB_SUCCESS;
        }
        else
        {
          total_cost = 1.0e10;
        }
      }
      else
      {
        rel_opt->set_width(width);
        cpu_run_cost = rel_stat_info->tuples_ *
                       (DEFAULT_CPU_TUPLE_COST *10 + DEFAULT_CPU_OPERATOR_COST * static_cast<double>(rel_opt->get_base_cnd_list().size()));
        get_group_by_cost(select_stmt,rel_opt,NULL,group_by_cost);
        get_order_by_cost(select_stmt,rel_opt,NULL,order_by_cost);
        cpu_run_cost += group_by_cost + order_by_cost;
        cpu_run_cost *= DEFAULT_CPU_DISK_PROPORTION;
        disk_run_cost = rel_stat_info->size_ / (1<<13) *DEFAULT_SEQ_PAGE_COST;
        total_cost = cpu_run_cost + disk_run_cost;
      }
      total_cost += DEFAULT_START_COST;
      cost = total_cost;
      return ret;
    }

    int ObStatCostCalculator::get_cost_index_scan(ObSelectStmt *select_stmt,
                                                  ObOptimizerRelation *rel_opt,
                                                  ObIndexTableInfo &index_table_info,
                                                  const double sel)
    {
      int ret = OB_SUCCESS;
      Cost total_cost = 0.0;
      Cost cpu_run_cost=0.0;
      Cost disk_run_cost =0.0;
      Cost group_by_cost =0.0;
      Cost order_by_cost =0.0;
      uint64_t index_table_id = index_table_info.index_table_id_;
      bool is_back_table = index_table_info.is_back_;
      uint64_t table_id = rel_opt->get_table_id();
      uint64_t table_ref_id = rel_opt->get_table_ref_id();
      rel_opt->set_table_id(index_table_id);
      rel_opt->set_table_ref_id(index_table_id);
      ObBaseRelStatInfo *rel_stat_info = NULL;
      ObStatExtractor *stat_extractor = NULL;
      stat_extractor = rel_opt->get_stat_extractor();
      common::hash::ObHashMap<uint64_t,ObBaseRelStatInfo*,common::hash::NoPthreadDefendMode>* table_id_statInfo_map = NULL;
      table_id_statInfo_map = rel_opt->get_table_id_statInfo_map();
      if(table_id_statInfo_map == NULL || stat_extractor == NULL)
      {
        ret = OB_ERROR;
      }
      else
      {
        ret = table_id_statInfo_map->get(index_table_id, rel_stat_info);
        if(common::hash::HASH_EXIST == ret
           && rel_stat_info !=NULL
           && rel_stat_info->enable_statinfo)
        {
          ret = OB_SUCCESS;
        }
        else if(rel_stat_info != NULL
                && !rel_stat_info->enable_statinfo)
        {
          ret = OB_ERROR;
        }
        else if(OB_SUCCESS != (ret = stat_extractor->fill_table_statinfo_map(table_id_statInfo_map,rel_opt)))
        {}
        else if(common::hash::HASH_EXIST == (ret = table_id_statInfo_map->get(index_table_id,rel_stat_info)) &&
                rel_stat_info != NULL && rel_stat_info->enable_statinfo)
        {
          ret = OB_SUCCESS;
        }
        else
        {
          ret = OB_ERROR;
        }
      }

      rel_opt->set_table_id(table_id);
      rel_opt->set_table_ref_id(table_ref_id);
      if(OB_SUCCESS != ret)
      {
        if(sel<0.25)
        {
          total_cost = 5.0e2*sel;
        }
        else
        {
          total_cost = 1.0e10+DEFAULT_START_COST;
        }
        ret = OB_SUCCESS;
      }
      else
      {
        cpu_run_cost = rel_stat_info->tuples_ * (DEFAULT_CPU_INDEX_TUPLE_COST *10 + DEFAULT_CPU_OPERATOR_COST) *sel;
        get_group_by_cost(select_stmt,rel_opt,&index_table_info,group_by_cost);
        get_order_by_cost(select_stmt,rel_opt,&index_table_info,order_by_cost);
        cpu_run_cost += group_by_cost + order_by_cost;
        cpu_run_cost *= DEFAULT_CPU_DISK_PROPORTION;
        disk_run_cost = rel_stat_info->size_ / (1<<13) *DEFAULT_SEQ_PAGE_COST * sel;
        if(is_back_table)
        {
          double tmp_cnd_size = 0.0;
          tmp_cnd_size = static_cast<double>(rel_opt->get_base_cnd_list().size() -1);
          if(common::hash::HASH_EXIST != (ret = table_id_statInfo_map->get(rel_opt->get_table_id(),rel_stat_info)) ||
             rel_stat_info == NULL)
          {
            ret = OB_SUCCESS;
            cpu_run_cost += 1.0e1;
            disk_run_cost += 2.0e2;
          }
          else
          {
            ret = OB_SUCCESS;
            Cost tmp_cpu_cost = cpu_run_cost;
            cpu_run_cost = rel_stat_info->tuples_ * sel * (DEFAULT_CPU_TUPLE_COST * 10 + tmp_cnd_size * DEFAULT_CPU_OPERATOR_COST);
            cpu_run_cost *= DEFAULT_CPU_DISK_PROPORTION;
            cpu_run_cost += tmp_cpu_cost;
            disk_run_cost += rel_stat_info->size_ /(1<<13) *DEFAULT_RANDOM_PAGE_COST * sel;
          }
        }
        total_cost = cpu_run_cost + disk_run_cost;
      }
      total_cost += DEFAULT_START_COST;
      index_table_info.cost_ = total_cost;
      return ret;
    }

    int ObStatExtractor::fill_col_statinfo_map(common::hash::ObHashMap<uint64_t,ObBaseRelStatInfo*,common::hash::NoPthreadDefendMode>* table_id_statInfo_map,
                                               ObOptimizerRelation *rel_opt,
                                               uint64_t column_id,
                                               bool unique_column_rowkey)
    {
      int ret = OB_SUCCESS;
      uint64_t table_id = rel_opt->get_table_id();
      uint64_t table_ref_id = rel_opt->get_table_ref_id();
      oceanbase::mergeserver::StatisticColumnValue scv;
      sql::ObBaseRelStatInfo *rel_stat_info = NULL;
      ObColumnStatInfo *col_stat_info = NULL;

      common::ObObj obj_min;
      common::ObObj obj_max;
      bool empty_table = false;
      if(table_id_statInfo_map == NULL)
      {
        ret = OB_ERROR;
        YYSYS_LOG(WARN, "table_id_statInfo_map is NULL");
      }
      else if (statistic_info_cache_ == NULL)
      {
        ret = OB_ERROR;
        YYSYS_LOG(WARN, "statistic_info_cache is NULL");
      }
      else if(common::hash::HASH_EXIST != (ret = table_id_statInfo_map->get(table_id,rel_stat_info))
              || rel_stat_info == NULL
              || !rel_stat_info->enable_statinfo)
      {
        ret = OB_ERROR;
        YYSYS_LOG(WARN, "get rel_stat_info is NULL");
      }
      else
      {
        ret = OB_ERROR;
        for(int i=0;i<rel_stat_info->statistic_columns_num_;i++)
        {
          if(rel_stat_info->statistic_columns_[i] == column_id)
          {
            ret = OB_SUCCESS;
            break;
          }
        }
      }
      if(ret != OB_SUCCESS)
      {
        YYSYS_LOG(WARN, "no columun statistics information.");
      }
      else if(OB_SUCCESS != (ret = statistic_info_cache_->get_column_statistic_info_from_cache(table_ref_id,column_id,scv)))
      {
        ret = OB_ERROR;
        YYSYS_LOG(WARN, "get_column_statistic_info_from_cache is fail.");
      }
      else
      {
        if(common::hash::HASH_EXIST != rel_stat_info->column_id_value_map_.get(column_id,col_stat_info) ||
           col_stat_info == NULL)
        {
          void * buf = NULL;
          if(rel_opt->get_name_pool() == NULL)
          {
            YYSYS_LOG(ERROR, "rel_opt name_pool is NULL");
            ret = OB_ERROR;
          }
          else if((buf = rel_opt->get_name_pool()->alloc(sizeof(ObColumnStatInfo))) == NULL)
          {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            YYSYS_LOG(ERROR, "col_stat_info malloc memory fail!");
          }
          else
          {
            col_stat_info = new(buf) ObColumnStatInfo();
          }
        }
        if(col_stat_info == NULL)
        {}
        else if(OB_SUCCESS != (ret = rel_opt->copy_obj(scv.min_,obj_min)))
        {
          YYSYS_LOG(ERROR, "min value object copy fail!");
        }
        else if(OB_SUCCESS != (ret = rel_opt->copy_obj(scv.max_,obj_max)))
        {
          YYSYS_LOG(ERROR, "max value object copy fail!");
        }
        else
        {
          col_stat_info->column_id_ = column_id;
          if(scv.different_num_ <1)
          {
            empty_table = true;
          }
          col_stat_info->distinct_num_ = static_cast<double>(scv.different_num_);
          if(empty_table)
          {
            col_stat_info->distinct_num_ = 1.0;
          }
          if(!col_stat_info->value_frequency_map_.created())
          {
            col_stat_info->value_frequency_map_.create(STAT_INFO_FREQ_MAP_SIZE);
          }
          col_stat_info->min_value_.assign(obj_min);
          col_stat_info->max_value_.assign(obj_max);
          if(scv.row_count_ ==0 || rel_stat_info->column_num_ ==0)
          {
            col_stat_info->avg_width_ = WIDTH_ARRY[scv.top_value_[0].obj_.get_type()];
          }
          else
          {
            col_stat_info->avg_width_ = static_cast<double>(scv.size_)/static_cast<double>(scv.row_count_)/rel_stat_info->column_num_;
          }
          col_stat_info->unique_rowkey_column_ = unique_column_rowkey;
          if(ObOPtimizerLoger::log_switch_)
          {
            char tmp[256] = {0};
            snprintf(tmp,256,"QOQX fill_col_statinfo_map start table_ref_id=%ld",rel_opt->get_table_ref_id());
            ObOPtimizerLoger::print(tmp);
            snprintf(tmp,256,"QOQX table_id =%ld column_id_ = %ld",rel_opt->get_table_id(),col_stat_info->column_id_);
            ObOPtimizerLoger::print(tmp);
            snprintf(tmp,256,"QOQX min = %s",to_cstring(col_stat_info->min_value_));
            ObOPtimizerLoger::print(tmp);
            snprintf(tmp,256,"QOQX max = %s",to_cstring(col_stat_info->max_value_));
            ObOPtimizerLoger::print(tmp);
            snprintf(tmp,256,"QOQX unique_rowkey_column_ = %d",col_stat_info->unique_rowkey_column_);
            ObOPtimizerLoger::print(tmp);
          }
          double total_high_frequency = 0,freq=0;
          for(int i=0;ret == OB_SUCCESS && i< scv.top_value_num_;i++)
          {
            freq = static_cast<double>(scv.top_value_[i].num_) / static_cast<double>(scv.row_count_);
            if(OB_SUCCESS != (ret = rel_opt->copy_obj(scv.top_value_[i].obj_,col_stat_info->obj[i])))
            {
              YYSYS_LOG(WARN, "write object fail. ret=%d", ret);
            }
            else
            {
              ret = col_stat_info->value_frequency_map_.set(col_stat_info->obj[i],freq,1);
            }
            total_high_frequency += freq;

            if (ObOPtimizerLoger::log_switch_)
            {
              char tmp[256] = {0};
              snprintf(tmp,256, "value_frequency_map[%d] k: %s v: %.6lf size = %ld",i,to_cstring(col_stat_info->obj[i]),freq,col_stat_info->value_frequency_map_.size());
              ObOPtimizerLoger::print(tmp);
            }
            if(ret == common::hash::HASH_OVERWRITE_SUCC || ret == common::hash::HASH_INSERT_SUCC)
            {
              ret = OB_SUCCESS;
            }
            else
            {
              YYSYS_LOG(DEBUG, "value_frequency_map_.set value fail,No.%d,obj=%s",i,to_cstring(col_stat_info->obj[i]));
            }
          }
          if(scv.top_value_num_ >= scv.different_num_ ||
             (1.0- total_high_frequency) <=0)
          {
            col_stat_info->avg_frequency_ =0;
          }
          else
          {
            col_stat_info->avg_frequency_ = (1.0 - total_high_frequency) * 1.0/ static_cast<double>(scv.different_num_ - scv.top_value_num_);
          }
          if(ObOPtimizerLoger::log_switch_)
          {
            char tmp[256] = {0};
            snprintf(tmp,256,"QOQX avg_frequency_ = %.6lf",col_stat_info->avg_frequency_);
            ObOPtimizerLoger::print(tmp);
            snprintf(tmp,256,"QOQX fill_col_statinfo_map end <<<<<<");
            ObOPtimizerLoger::print(tmp);
          }
        }
        if(ret != OB_SUCCESS)
        {
          YYSYS_LOG(WARN, "value_frequency_map_ add fail!");
        }
        else if(rel_stat_info->column_id_value_map_.created())
        {}
        else if(OB_SUCCESS != (ret = rel_stat_info->column_id_value_map_.create(STAT_INFO_COL_MAP_SIZE)))
        {
          YYSYS_LOG(ERROR, "rel_stat_info->column_id_value_map create fail =>%d", ret);
        }
        if(OB_SUCCESS != ret)
        {}
        else if(common::hash::HASH_OVERWRITE_SUCC == (ret = rel_stat_info->column_id_value_map_.set(column_id,col_stat_info,1))
                || common::hash::HASH_INSERT_SUCC == ret)
        {
          ret= OB_SUCCESS;
        }
        else
        {
          YYSYS_LOG(ERROR, "column_id_value_map_ add fail! column_id_value_map_.size = %ld =>%d", rel_stat_info->column_id_value_map_.size(), ret);
        }
      }
      scv.allocator_.free();
      return ret;
    }

    int ObStatExtractor::fill_table_statinfo_map(common::hash::ObHashMap<uint64_t,ObBaseRelStatInfo*,common::hash::NoPthreadDefendMode>* table_id_statInfo_map,
                                                 ObOptimizerRelation *rel_opt)
    {
      int ret = OB_SUCCESS;
      uint64_t table_id = rel_opt->get_table_id();
      uint64_t table_ref_id = rel_opt->get_table_ref_id();
      sql::ObBaseRelStatInfo *rel_stat_info = NULL;
      oceanbase::mergeserver::StatisticTableValue stv;
      bool empty_table = false;
      if(!table_id_statInfo_map->created())
      {
        ret = table_id_statInfo_map->create(STAT_INFO_TABLE_MAP_SIZE);
      }
      if(OB_SUCCESS != ret)
      {
        YYSYS_LOG(ERROR, "table_id_statInfo_map create fail.=>%d", ret);
      }
      else if(common::hash::HASH_EXIST != table_id_statInfo_map->get(table_id,rel_stat_info) ||
              rel_stat_info == NULL)
      {
        void *buf= NULL;
        if(rel_opt->get_name_pool() == NULL)
        {
          YYSYS_LOG(ERROR, "rel_opt name_pool is null");
          ret = OB_ERROR;
        }
        else if((buf = rel_opt->get_name_pool()->alloc(sizeof(ObBaseRelStatInfo))) == NULL)
        {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          YYSYS_LOG(ERROR, "rel_stat_info malloc memory fail!");
        }
        else
        {
          rel_stat_info = new (buf) ObBaseRelStatInfo();
        }
      }
      if(rel_stat_info == NULL)
      {}
      else if(statistic_info_cache_ == NULL)
      {
        ret = OB_ERROR;
        YYSYS_LOG(DEBUG, "statistic_info_cache_ is NULL");
      }
      else if(OB_SUCCESS == (ret =statistic_info_cache_->get_table_statistic_info_from_cache(table_ref_id,stv) ))
      {
        rel_stat_info->table_id_ = table_id;
        if(stv.statistic_columns_num_<=0)
        {
          rel_stat_info->enable_statinfo = false;
        }
        else
          if(stv.row_count_ <1)
          {
            empty_table = true;
            rel_stat_info->tuples_ = 1.0;
          }
          else
          {
            empty_table = false;
            rel_stat_info->tuples_ = static_cast<double>(stv.row_count_);
          }
        rel_stat_info->avg_width_ = static_cast<double>(stv.mean_row_size_);
        rel_stat_info->statistic_columns_num_ = stv.statistic_columns_num_;
        rel_stat_info->size_ = static_cast<double>(stv.size_);
        rel_stat_info->empty_table_ = empty_table;
        if(ObOPtimizerLoger::log_switch_)
        {
          char tmp[256] = {0};
          snprintf(tmp,256,"QOQX fill_table_statinfo_map start >>>>>");
          ObOPtimizerLoger::print(tmp);
          snprintf(tmp,256,"QOQX table_id =%ld table_ref_id = %ld",table_id, table_ref_id);
          ObOPtimizerLoger::print(tmp);
          snprintf(tmp,256,"QOQX tuples_ = %.6lf", rel_stat_info->tuples_);
          ObOPtimizerLoger::print(tmp);
          snprintf(tmp,256,"QOQX statistic_columns_num_ = %ld", rel_stat_info->statistic_columns_num_);
          ObOPtimizerLoger::print(tmp);
          snprintf(tmp,256,"QOQX size_ = %.6lf", rel_stat_info->size_);
          ObOPtimizerLoger::print(tmp);
          snprintf(tmp,256,"QOQX empty_table_ = %d", rel_stat_info->empty_table_);
          ObOPtimizerLoger::print(tmp);
          snprintf(tmp,256,"QOQX fill_table_statinfo_map end <<<<<");
          ObOPtimizerLoger::print(tmp);
        }
        for(int i=0;i<stv.statistic_columns_num_;i++)
        {
          rel_stat_info->statistic_columns_[i] = stv.statistic_columns_[i];
        }
        if(!rel_stat_info->column_id_value_map_.created())
        {
          rel_stat_info->column_id_value_map_.create(STAT_INFO_COL_MAP_SIZE);
        }
      }
      if(ret != OB_SUCCESS)
      {
        YYSYS_LOG(WARN, "no statistics return from cache, table_id = %ld", table_id);
      }
      else if(common::hash::HASH_OVERWRITE_SUCC == (ret = table_id_statInfo_map->set(table_id,rel_stat_info,1)) ||
              common::hash::HASH_INSERT_SUCC == ret)
      {
        ret = OB_SUCCESS;
      }
      else
      {
        YYSYS_LOG(DEBUG, "table_id_statInfo_map_ add fail! table_id_statInfo_map->size() =%ld => %d", table_id_statInfo_map->size(), ret);
      }
      return ret;
    }
    int ObOptimizerRelation::get_column_stat_info(uint64_t table_id,
                                                  uint64_t column_id,
                                                  ObColumnStatInfo* &col_stat_info)
    {
      int ret = OB_SUCCESS;
      ObBaseRelStatInfo *rel_stat_info = NULL;
      bool unique_column = false;
      unique_column = ObStatSelCalculator::is_unique_rowkey_column(this,table_id,column_id);
      if(OB_SUCCESS != (ret = get_base_rel_stat_info(table_id,rel_stat_info)))
      {
        YYSYS_LOG(DEBUG, "get relation stat info fail ret=%d, table_id =%ld",ret, table_id);
      }
      else if(rel_stat_info->enable_statinfo
              &&common::hash::HASH_EXIST == (ret = rel_stat_info->column_id_value_map_.get(column_id,col_stat_info))
              && col_stat_info != NULL)
      {
        ret = OB_SUCCESS;
      }
      else if(rel_stat_info->enable_statinfo)
      {
        ret = stat_extractor_->fill_col_statinfo_map(table_id_statInfo_map_,this,column_id,unique_column);
        if(ret != OB_SUCCESS)
        {
          YYSYS_LOG(DEBUG, "can't get column stat info table_id =%ld column_id = %ld", table_id, column_id);
        }
        else if(common::hash::HASH_EXIST == (ret = rel_stat_info->column_id_value_map_.get(column_id,col_stat_info))
                && col_stat_info != NULL)
        {
          ret = OB_SUCCESS;
        }
        else
        {
          YYSYS_LOG(WARN, "can't get column stat info table_id =%ld column_id = %ld =>%d", table_id, column_id, ret);
        }
      }
      return ret;
    }

    int ObOptimizerRelation::get_base_rel_stat_info(uint64_t table_id,
                                                    ObBaseRelStatInfo* &rel_stat_info)
    {
      int ret = OB_SUCCESS;
      oceanbase::mergeserver::ObStatisticInfoCache *statistic_info_cache = NULL;
      if(schema_managerv2_ == NULL)
      {
        ret = OB_ERROR;
        YYSYS_LOG(DEBUG, "schema_managerv2_ is null");
      }
      else if(stat_extractor_ == NULL)
      {
        ret = OB_ERROR;
        YYSYS_LOG(DEBUG, "stat_extractor_ is null");
      }
      else
      {
        statistic_info_cache = stat_extractor_->get_statistic_info_cache();
      }
      if(table_id_statInfo_map_ == NULL)
      {
        ret = OB_ERROR;
        YYSYS_LOG(DEBUG, "table_id_statInfo_map_ is null");
      }
      else if(statistic_info_cache == NULL)
      {
        ret = OB_ERROR;
        YYSYS_LOG(DEBUG, "statistic_info_cache is null");
      }
      else if(common::hash::HASH_EXIST != (ret = table_id_statInfo_map_->get(table_id,rel_stat_info)) ||
              rel_stat_info == NULL ||
              !rel_stat_info->enable_statinfo)
      {
        ret = OB_ERROR;
        YYSYS_LOG(DEBUG, "get rel_stat_info fail, table_id = %ld", table_id);
      }
      else
      {
        ret = OB_SUCCESS;
      }
      return ret;
    }

    void ObOptimizerRelation::reset_semi_join_right_index_table_cost(uint64_t column_id,
                                                                     double sel)
    {
      int32_t cheapest_index_table_idx =0;
      double tmp_cheapest_cost = DBL_MAX;
      bool flag = false;
      if(index_table_array_.size() >0)
      {
        for(int32_t i=0;i<index_table_array_.size();i++)
        {
          if(index_table_array_.at(i).index_column_id_ == column_id && index_table_array_.at(i).cost_ <tmp_cheapest_cost)
          {
            tmp_cheapest_cost = index_table_array_.at(i).cost_;
            cheapest_index_table_idx = i;
            flag = true;
          }
        }
        if(flag)
        {
          for(int32_t i=0;i<index_table_array_.size();i++)
          {
            if(cheapest_index_table_idx == i)
            {
              index_table_array_.at(i).cost_ = tmp_cheapest_cost * sel;
            }
            else
            {
              index_table_array_.at(i).cost_ = DBL_MAX;
            }
          }
        }
        else
        {
          bool reset_history = false;
          for(int32_t i=0;i<index_table_array_.size();i++)
          {
            if(index_table_array_.at(i).cost_ == 0)
            {
              reset_history =true;
            }
          }
          if(!reset_history)
          {
            index_table_array_.clear();
          }
          YYSYS_LOG(DEBUG, "reset cost: table id = %ld ref_id id=%ld column_id=%ld not find right index table.", table_id_, table_ref_id_, column_id);
        }
      }
      else
      {
        YYSYS_LOG(DEBUG, "reset cost: table id = %ld ref_id id=%ld index_table_array_ count == 0", table_id_, table_ref_id_);
      }
    }

    void ObOptimizerRelation::print_rel_opt_info()
    {
      YYSYS_LOG(DEBUG, "ST ===================rel_opt_info=begin===================");
      YYSYS_LOG(DEBUG, "ST table_id = %ld", table_id_);
      YYSYS_LOG(DEBUG, "ST table_ref_id_ = %ld", table_ref_id_);
      YYSYS_LOG(DEBUG, "ST rel_opt_kind_ = %d", rel_opt_kind_);
      YYSYS_LOG(DEBUG, "ST selectivity_i = %.20lf", rows_ / tuples_);
      YYSYS_LOG(DEBUG, "ST rows_ = %.20lf", rows_);
      YYSYS_LOG(DEBUG, "ST tuples_ = %.20lf", tuples_);
      YYSYS_LOG(DEBUG, "ST join_rows_ = %.20lf", join_rows_);
      YYSYS_LOG(DEBUG, "ST selectivity_j = %.20lf", join_rows_ / tuples_);
      YYSYS_LOG(DEBUG, "ST group_by_num_ = %d", group_by_num_);
      YYSYS_LOG(DEBUG, "ST order_by_num_ = %d", order_by_num_);
      YYSYS_LOG(DEBUG, "ST seq_scan_cost_ = %.20lf", get_seq_scan_cost());
      for (int32_t i = 0; i < index_table_array_.size(); i++)
      {
        YYSYS_LOG(DEBUG, "ST No.%d,index_table_id=%ld,column_id=%ld,is_back=%d,cost=%.20lf,group_by_applyed_=%d,order_by_applyed_=%d",
                  i,index_table_array_.at(i).index_table_id_,
                  index_table_array_.at(i).index_column_id_,
                  index_table_array_.at(i).is_back_,
                  index_table_array_.at(i).cost_,
                  index_table_array_.at(i).group_by_applyed_,
                  index_table_array_.at(i).order_by_applyed_);
      }
      YYSYS_LOG(DEBUG, "ST base_cnd_list_.size = %d", base_cnd_list_.size());
      YYSYS_LOG(DEBUG, "ST join_cnd_list_.size = %d", join_cnd_list_.size());
      YYSYS_LOG(DEBUG, "ST ====================rel_opt_info=end====================");
    }

    void ObOptimizerRelation::print_rel_opt_info(const FILE *file)
    {
      if (ObOPtimizerLoger::log_switch_)
      {
        FILE* fp = const_cast<FILE*>(file);
        fprintf(fp, "===================rel_opt_info=begin===================\n");
        fprintf(fp, "\ttable_id = %ld\n", table_id_);
        fprintf(fp, "\ttable_ref_id_ = %ld\n", table_ref_id_);
        fprintf(fp, "\trel_opt_kind_ = %d\n", rel_opt_kind_);
        fprintf(fp, "\tselectivity_i = %.20lf\n", rows_ / tuples_);
        fprintf(fp, "\trows_ = %.20lf\n", rows_);
        fprintf(fp, "\ttuples_ = %.20lf\n", tuples_);
        fprintf(fp, "\tjoin_rows_ = %.20lf\n", join_rows_);
        fprintf(fp, "\tselectivity_j = %.20lf\n", join_rows_ / tuples_);
        fprintf(fp, "\tgroup_by_num_ = %d\n", group_by_num_);
        fprintf(fp, "\torder_by_num_ = %d\n", order_by_num_);
        fprintf(fp, "\tseq_scan_cost_ = %.20lf\n", get_seq_scan_cost());
        for (int32_t i = 0; i < index_table_array_.size(); i++)
        {
          fprintf(fp, "\tNo.%d,index_table_id=%ld,column_id=%ld,is_back=%d,cost=%.20lf,group_by_applyed_=%d,order_by_applyed_=%d\n",
                    i,index_table_array_.at(i).index_table_id_,
                    index_table_array_.at(i).index_column_id_,
                    index_table_array_.at(i).is_back_,
                    index_table_array_.at(i).cost_,
                    index_table_array_.at(i).group_by_applyed_,
                    index_table_array_.at(i).order_by_applyed_);
        }
        fprintf(fp, "\tbase_cnd_list_.size = %d\n", base_cnd_list_.size());
        fprintf(fp, "\tjoin_cnd_list_.size = %d\n", join_cnd_list_.size());
        fprintf(fp, "\tneeded_columns_ size = %d\n", needed_columns_.size());
        fprintf(fp, "====================rel_opt_info=end====================\n");
      }
    }

    void ObOptimizerRelation::print_rel_opt_info_V2()
    {
      if (ObOPtimizerLoger::log_switch_)
      {
        char tmp[256] = {0};
        snprintf(tmp,256,"ST ===================rel_opt_info=begin===================");
        ObOPtimizerLoger::print(tmp);
        snprintf(tmp,256,"ST table_id =%ld",table_id_);
        ObOPtimizerLoger::print(tmp);
        snprintf(tmp,256,"ST table_ref_id_ =%ld",table_ref_id_);
        ObOPtimizerLoger::print(tmp);
        snprintf(tmp,256,"ST rel_opt_kind_ =%d",rel_opt_kind_);
        ObOPtimizerLoger::print(tmp);
        snprintf(tmp,256, "ST selectivity_i = %.6lf", rows_ / tuples_);
        ObOPtimizerLoger::print(tmp);
        snprintf(tmp,256, "ST rows_ = %.6lf", rows_);
        ObOPtimizerLoger::print(tmp);
        snprintf(tmp,256, "ST tuples_ = %.6lf", tuples_);
        ObOPtimizerLoger::print(tmp);
        snprintf(tmp,256, "ST join_rows_ = %.6lf", join_rows_);
        ObOPtimizerLoger::print(tmp);
        snprintf(tmp,256, "ST selectivity_j = %.6lf", join_rows_ / tuples_);
        ObOPtimizerLoger::print(tmp);
        snprintf(tmp,256, "ST group_by_num_ = %d", group_by_num_);
        ObOPtimizerLoger::print(tmp);
        snprintf(tmp,256, "ST order_by_num_ = %d", order_by_num_);
        ObOPtimizerLoger::print(tmp);
        snprintf(tmp,256, "ST seq_scan_cost_ = %.6lf", get_seq_scan_cost());
        ObOPtimizerLoger::print(tmp);
        for (int32_t i = 0; i < index_table_array_.size(); i++)
        {
          snprintf(tmp,256, "ST No.%d,index_table_id=%ld,column_id=%ld,is_back=%d,cost=%.6lf,group_by_applyed_=%d,order_by_applyed_=%d",
                    i,index_table_array_.at(i).index_table_id_,
                    index_table_array_.at(i).index_column_id_,
                    index_table_array_.at(i).is_back_,
                    index_table_array_.at(i).cost_,
                    index_table_array_.at(i).group_by_applyed_,
                    index_table_array_.at(i).order_by_applyed_);
          ObOPtimizerLoger::print(tmp);
        }
        snprintf(tmp,256, "ST base_cnd_list_.size = %d", base_cnd_list_.size());
        ObOPtimizerLoger::print(tmp);
        snprintf(tmp,256, "ST join_cnd_list_.size = %d", join_cnd_list_.size());
        ObOPtimizerLoger::print(tmp);
        snprintf(tmp,256, "====================rel_opt_info=end====================");
        ObOPtimizerLoger::print(tmp);
      }
    }
  }
}
