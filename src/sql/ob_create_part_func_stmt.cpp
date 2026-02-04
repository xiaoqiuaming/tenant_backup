
#include "ob_create_part_func_stmt.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

ObCreatePartFuncStmt::ObCreatePartFuncStmt(ObStringBuf *name_pool)
  : ObBasicStmt(ObBasicStmt::T_CREATE_PART_FUNC),param_num_(0),type_(OB_FUNC_TYPE_NUM)//add wuna[MultiUps] [sql_api] 20151217
{
  name_pool_ = name_pool;
}

ObCreatePartFuncStmt::~ObCreatePartFuncStmt()
{
 /*add wuna [MultiUps] [sql_api] 20151214:b*/
  for (int64_t i=0;i<range_partition_nodes_.count();i++)
  {
    RangePartitionNode* node = range_partition_nodes_.at(i);
    node->partition_value_obj_.clear();
    if (NULL != range_partition_nodes_.at(i))
    {
      ob_free(range_partition_nodes_.at(i),ObModIds::OB_PARTITION_MANAGER);
    }
  }
  range_partition_nodes_.clear();
  parameters_.clear();
 /*add 20151214:e*/
}

ObCreatePartFuncStmt::ObCreatePartFuncStmt(const ObCreatePartFuncStmt &other): ObBasicStmt(other.get_stmt_type(), other.get_query_id())
{
  *this = other;
}

ObCreatePartFuncStmt& ObCreatePartFuncStmt::operator=(const ObCreatePartFuncStmt& other)
{
  int ret = OB_SUCCESS;
  if(this == &other)
  {
  }
  else
  {
    function_name_ = other.get_function_name();
    function_context_ = other.get_func_context();
    param_num_ = other.get_parameters_num();
    type_ = other.get_partition_type();//add wuna[MultiUps][sql_api] 20151217
    for(int i = 0;i < param_num_;i++)
    {
      ObString param;
      if(OB_SUCCESS != (ret = other.get_parameter(i,param)))
      {
        break;
      }
      else
      {
        if(OB_SUCCESS != (ret = this->add_param_name(param)))
        {
          break;
        }
      }
    }
    if(OB_SUCCESS == ret)
    {
      for(int i = 0; i < range_partition_nodes_.count();i++)
      {
        this->range_partition_nodes_.push_back(const_cast<RangePartitionNode*>(other.get_range_partition_node(i)));
      }
    }
  }
  return *this;
}
int ObCreatePartFuncStmt::add_param_name(const common::ObString& param)
{
  return parameters_.push_back(param);
}

int ObCreatePartFuncStmt::add_param_name(ResultPlan &result_plan, const common::ObString& param)
{
  int& ret = result_plan.err_stat_.err_code_ = OB_SUCCESS;
  common::ObString param_name;
  if(OB_SUCCESS != (ret = ob_write_string(*name_pool_, param, param_name)))
  {
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "allocate memory for parameter name failed.");
  }
  else if(is_duplicate_partition_param_name(param_name))
  {
    ret = OB_ERR_ALREADY_EXISTS;
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
             "duplicate param '%.*s'", param_name.length(), param_name.ptr());
  }
  else if(OB_SUCCESS != (ret = parameters_.push_back(param_name)))
  {
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "push back parameter name failed.");
  }
  return ret;
}

int ObCreatePartFuncStmt::set_funcion_name(ResultPlan &result_plan, const ObString &function_name)
{
  int& ret = result_plan.err_stat_.err_code_ = OB_SUCCESS;
  if (OB_SUCCESS != (ret = ob_write_string(*name_pool_, function_name, function_name_)))
  {
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "allocate memory for hash function name failed");
  }
  return ret;
}

int ObCreatePartFuncStmt::set_func_context(ResultPlan &result_plan, const ObString &func_context)
{
  int& ret = result_plan.err_stat_.err_code_ = OB_SUCCESS;

  if (OB_SUCCESS != (ret = ob_write_string(*name_pool_, func_context, function_context_)))
  {
    snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
        "allocate memory for function context name failed");
  }
  return ret;
}
void ObCreatePartFuncStmt::print(FILE* fp, int32_t level, int32_t index)
{
  print_indentation(fp, level);
  fprintf(fp, "<ObCreatePartFuncStmt id=%d>\n", index);
  print_indentation(fp, level + 1);
  fprintf(fp, "Function Name := (%.*s)", function_name_.length(), function_name_.ptr());
  print_indentation(fp, level + 1);
  fprintf(fp, "Function Context := (%.*s)", function_context_.length(), function_context_.ptr());
  print_indentation(fp, level + 1);
  fprintf(fp, "Parameter Number := (%d)", param_num_);
  print_indentation(fp, level);
  //add wuna [MultiUps][sql_api] 20151217:b
  fprintf(fp, "Partition Type := (%d)", type_);
  print_indentation(fp, level);
  //add 20151217:e
  fprintf(fp, "<ObCreatePartFuncStmt id=%d>\n", index);
}

int ObCreatePartFuncStmt::check_expression()
{
  int ret = OB_SUCCESS;
  /*add wuna [MultiUps] [sql_api] 20151228;b*/
  ObArray<ObString> group_name_list;
  ObCellArray cell_array;
  ObObj result;
  if(HASHFUNC == type_)//hash partition function
  {
    ObHashExpression hash_expr;
    ObCellInfo cell_info;
    ObInnerCellInfo *inner_info = NULL;
    for(int64_t i = 0;i < param_num_;i++)
    {
      cell_info.value_.set_int(0);
      if(OB_SUCCESS != (ret = cell_array.append(cell_info, inner_info)))
      {
        YYSYS_LOG(WARN, "failed to append cell info, ret=%d", ret);
        break;
      }
    }
    if(OB_SUCCESS != ret)
    {
    }
    else if(OB_SUCCESS != (ret = hash_expr.set_expression(function_context_,group_name_list,cell_array,true)))
    {
      YYSYS_LOG(USER_ERROR, "Hash function definition is not correct.");
    }
    else if(OB_SUCCESS != (ret = hash_expr.calc(cell_array,ObCalcExpression::HASH, parameters_, result)))
    {
      YYSYS_LOG(USER_ERROR, "Hash function definition is not correct.");
    }
  }
  else if(RANGEFUNC == type_)//range partition function
  {
    //do nothing,because range func has checked in build_plan.
  }
  else if(PRERANGE == type_)
  {
    ObRangeExpression range_expr;
    if(OB_SUCCESS != (ret = range_expr.set_expression(function_context_,group_name_list,cell_array,true)))
    {
      YYSYS_LOG(USER_ERROR, "Range funtion definition is not correct.");
    }
    else if(param_num_ != range_expr.get_column_num())
    {
      ret=OB_ERR_PARTITION_INCONSISTENCY_NUM;
      YYSYS_LOG(USER_ERROR,"Inconsistency num in usage of column lists for partition function.");
    }
    else if(OB_SUCCESS != (ret = range_expr.check_range_partition_value()))
    {
      YYSYS_LOG(USER_ERROR, "range value must be strictly increasing for each partition and the last value must be maxvalue.");
    }
  }
  else if(ENUMFUNC == type_ || LISTFUNC == type_)//enum and list partition function
  {
    ObEnumExpression enum_expr;
    if(OB_SUCCESS != (ret = enum_expr.set_expression(function_context_,group_name_list,cell_array,true)))
    {
      if(ret == OB_ERR_PARTITION_VALUE_DUPLICATE)
      {
        YYSYS_LOG(USER_ERROR, "Multiple definition of same constant in Enum or List partitioning.");
      }
      else
      {
        YYSYS_LOG(USER_ERROR, "Enum or List funtion definition is not correct.");
      }
    }
    else if(param_num_ != enum_expr.get_column_num())
    {
      ret=OB_ERR_PARTITION_INCONSISTENCY_NUM;
      YYSYS_LOG(USER_ERROR,"Inconsistency num in usage of column lists for partition function.");
    }
  }
  else if(HARDCODEFUNC == type_)
  {
    ObHardCodeExpression hardcode_expr;
    if(OB_SUCCESS != (ret = hardcode_expr.set_expression(function_name_,group_name_list,cell_array,true)))
    {
      if(ret == OB_ERR_PARTITION_VALUE_DUPLICATE)
      {
        YYSYS_LOG(USER_ERROR, "Multiple definition of same constant in hardcode partitioning.");
      }
      else
      {
        YYSYS_LOG(USER_ERROR, "hardcode funtion definition is not correct.");
      }
    }
    else if(param_num_ != 1)
    {
      ret=OB_ERR_PARTITION_INCONSISTENCY_NUM;
      YYSYS_LOG(USER_ERROR,"Inconsistency num in usage of column lists for partition function.");
    }
  }
  else
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN,"unexpected part func type,is %d",type_);
  }
  /*add 20151228:e*/
  return ret;
}
/*add wuna [MultiUps] [sql_api] 20151214:b*/
int ObCreatePartFuncStmt::check_range_partition_value(ResultPlan& result_plan)
{
  int& ret = result_plan.err_stat_.err_code_ = OB_SUCCESS;
  if (1 == range_partition_nodes_.count())
  {
    ObRowkey cur_value( &(range_partition_nodes_.at(0)->partition_value_obj_.at(0)), param_num_);
    if (!(cur_value.is_max_row()))
    {
      ret = OB_ERROR;
      YYSYS_LOG(ERROR, "the last value must be maxvalue.");
    }
  }
  for(int64_t i=1;i<range_partition_nodes_.count() && OB_SUCCESS==ret;i++)
  {
    RangePartitionNode*& pre_range_node=range_partition_nodes_.at(i-1);
    RangePartitionNode*& cur_range_node=range_partition_nodes_.at(i);
    OB_ASSERT(pre_range_node->partition_value_obj_.count() == cur_range_node->partition_value_obj_.count());
    ObRowkey pre_value(&(pre_range_node->partition_value_obj_.at(0)),param_num_);
    ObRowkey cur_value(&(cur_range_node->partition_value_obj_.at(0)),param_num_);
    int cmp=0;
    cmp = pre_value.compare(cur_value);
    if(cmp >= 0)
    {
      ret = OB_ERROR;
      YYSYS_LOG(ERROR,"VALUES LESS THAN value must be strictly increasing for each partition.");
      break;
    }
    else
    {
      if (i == range_partition_nodes_.count() - 1 && !(cur_value.is_max_row()))
      {
        ret = OB_ERROR;
        YYSYS_LOG(ERROR, "the last value must be maxvalue.");
      }
      continue;
    }
  }
  return ret;
}

bool ObCreatePartFuncStmt::is_duplicate_partition_param_name(const ObString& param_name)
{
  int ret = false;
  for(int64_t i = 0;i < parameters_.count();i++)
  {
    const ObString& part_param_name = parameters_.at(i);
    if(part_param_name == param_name)
    {
      ret = true;
      YYSYS_LOG(ERROR,"duplicate partition param '%.*s' ",
                param_name.length(), param_name.ptr());
      break;
    }
  }
  return ret;
}
/*add 20151214:e*/
