/*
*   Version: 0.1
*
*   Authors:
*      liu jun
*        - some work details if you want
*/

#include "ob_part_rule_node.h"

using namespace oceanbase::common;

ObPartRuleNode::ObPartRuleNode() : calc_expr_(NULL), type_(ObCalcExpression::INVALID_EXPRESSION_TYPE),
                                   rule_para_num_(-1), inited_(false)
{
  //del wuna [MultiUps][sql_api] 20151217:b
  //params_.clear();
  //del 20151217:e
}
ObPartRuleNode::~ObPartRuleNode()
{
  delete calc_expr_;
  calc_expr_=NULL;
}

//add hongchen [PERFORMANCE_OPTI] 20170821:b
void ObPartRuleNode::reset()
{
  delete calc_expr_;
  calc_expr_=NULL;
  type_ = ObCalcExpression::INVALID_EXPRESSION_TYPE;
  rule_para_num_ = -1;
  inited_ = false;
  rule_name_.reset();
  rule_para_list_.reset();
  rule_body_.reset();
}
//add hongchen [PERFORMANCE_OPTI] 20170821:e

//add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160412:b
int ObPartRuleNode::assign(const ObPartRuleNode& other)
{
  if (other.calc_expr_ != NULL)
  {
    this->calc_expr_ = other.calc_expr_;
  }
  else
  {
    calc_expr_ = NULL;
  }
  this->rule_body_.assign(const_cast<char*>(other.rule_body_.ptr()),other.rule_body_.length());
  this->rule_name_.assign(const_cast<char*>(other.rule_name_.ptr()),other.rule_name_.length());
  this->rule_para_num_ = other.rule_para_num_;
  this->rule_para_list_.assign(const_cast<char*>(other.rule_para_list_.ptr()),other.rule_para_list_.length());
  this->type_ = other.type_;
  this->inited_ = other.inited_;
  return OB_SUCCESS;
}
//add 20160412:e

//mod wuna [MultiUps][sql_api] 20151217:b

//int ObPartRuleNode::row_to_entity(const ObRow &row, FIFOAllocator &allocator)
int ObPartRuleNode::row_to_entity(const ObRow &row, PageArena<char> &allocator)
//mod 20151217:e
{
  int ret = OB_SUCCESS;
  const ObObj *pcell = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  if(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = row.raw_get_cell(0, pcell, table_id, column_id)))
    {
      YYSYS_LOG(WARN, "raw_get_cell(rule_name) from ObObj, ret=%d", ret);
    }
    else
    {
      if(ObVarcharType == pcell->get_type())
      {
        ObString rule_name;
        if(OB_SUCCESS != (ret = pcell->get_varchar(rule_name)))
        {
          YYSYS_LOG(WARN, "failed to get_varchar(rule_name) from ObObj, ret=%d", ret);
        }
        else if(OB_SUCCESS != (ret = ob_write_string(allocator, rule_name, rule_name_)))
        {
          YYSYS_LOG(WARN, "failed to copy string %.*s, ret=%d", rule_name.length(), rule_name.ptr(), ret);
        }
      }
      else
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObVarcharType);
      }
    }
  }
  if(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = row.raw_get_cell(1, pcell, table_id, column_id)))
    {
      YYSYS_LOG(WARN, "raw_get_cell(rule_par_num) from ObObj, ret=%d", ret);
    }
    else
    {
      if(ObIntType == pcell->get_type())
      {
        int64_t rule_par_num = 0;
        if(OB_SUCCESS != (ret = pcell->get_int(rule_par_num)))
        {
          YYSYS_LOG(WARN, "failed to get_int(rule_par_num) from ObObj, ret=%d", ret);
        }
        else
        {
          rule_para_num_ = static_cast<int32_t>(rule_par_num);
        }
      }
      else
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
      }
    }
  }
  if(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = row.raw_get_cell(2, pcell, table_id, column_id)))
    {
      YYSYS_LOG(WARN, "raw_get_cell(rule_par_list) from ObObj, ret=%d", ret);
    }
    else
    {
      if(ObVarcharType == pcell->get_type())
      {
        ObString rule_par_list;
        if(OB_SUCCESS != (ret = pcell->get_varchar(rule_par_list)))
        {
          YYSYS_LOG(WARN, "failed to get_varchar(rule_par_list) from ObObj, ret=%d", ret);
        }
        else if(OB_SUCCESS != (ret = ob_write_string(allocator, rule_par_list, rule_para_list_)))
        {
          YYSYS_LOG(WARN, "failed to copy string %.*s, ret=%d", rule_par_list.length(), rule_par_list.ptr(), ret);
        }
      }
      else
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObVarcharType);
      }
    }
  }
  if(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = row.raw_get_cell(3, pcell, table_id, column_id)))
    {
      YYSYS_LOG(WARN, "raw_get_cell(rule_body) from ObObj, ret=%d", ret);
    }
    else
    {
      if(ObVarcharType == pcell->get_type())
      {
        ObString rule_body;
        if(OB_SUCCESS != (ret = pcell->get_varchar(rule_body)))
        {
          YYSYS_LOG(WARN, "failed to get_varchar(rule_body) from ObObj, ret=%d", ret);
        }
        else if(OB_SUCCESS != (ret = ob_write_string(allocator, rule_body, rule_body_)))
        {
          YYSYS_LOG(WARN, "failed to copy string %.*s, ret=%d", rule_body.length(), rule_body.ptr(), ret);
        }
      }
      else
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObVarcharType);
      }
    }
  }
  if(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = row.raw_get_cell(4, pcell, table_id, column_id)))
    {
      YYSYS_LOG(WARN, "raw_get_cell(type) from ObObj, ret=%d", ret);
    }
    else
    {
      if(ObIntType == pcell->get_type())
      {
        int64_t type = 0;
        if(OB_SUCCESS != (ret = pcell->get_int(type)))
        {
          YYSYS_LOG(WARN, "failed to get_int(type) from ObObj, ret=%d", ret);
        }
        else
        {
          type_ = static_cast<ObCalcExpression::ExpressionType>(type);
        }
      }
      else
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(WARN, "got type of %d cell from join row, expected type=%d", pcell->get_type(), ObIntType);
      }
    }
  }
  return ret;
}

const ObString &ObPartRuleNode::get_rule_name() const
{
  return rule_name_;
}

int32_t ObPartRuleNode::get_rule_para_num() const
{
  return rule_para_num_;
}
const ObString &ObPartRuleNode::get_rule_para_list() const
{
  return rule_para_list_;
}
const ObString &ObPartRuleNode::get_rule_body() const
{
  return rule_body_;
}

int ObPartRuleNode::equals(const ObPartRuleNode &part_entity)
{
  int ret = OB_SUCCESS;
  if(rule_name_ == part_entity.get_rule_name()
     && rule_para_num_ == part_entity.get_rule_para_num()
     && rule_para_list_ == part_entity.get_rule_para_list()
     && rule_body_ == part_entity.get_rule_body())
  {
  }
  else
  {
    ret = OB_ERROR;
  }
  return ret;
}

int ObPartRuleNode::separate(const ObString &src, const char separator, ObArray<ObString> &result) const
{
  int ret = OB_SUCCESS;
  const char *ptr = src.ptr();
  const char *begin_index = ptr;
  const int32_t src_length = src.length();
  int32_t col_length = 0;
  if(NULL == ptr)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(WARN, "can't separate the empty string");
  }
  else
  {
    for(int32_t i = 0;i < src_length;i++)
    {
      if(separator == ptr[i])
      {
        ObString col_name(col_length, col_length, begin_index);
        result.push_back(col_name);
        begin_index = begin_index + col_length + 1;
        col_length = 0;
      }
      else if(src_length - 1 == i)
      {
        ObString col_name(col_length + 1, col_length + 1, begin_index);
        result.push_back(col_name);
      }
      else
      {
        col_length++;
      }
    }
  }
  return ret;
}
//mod wuna [MultiUps][sql_api] 20151217:b
//int ObPartRuleNode::parser(FIFOAllocator &allocator)
int ObPartRuleNode::parser(ObTableRuleNode *&table_node,ObCellArray& cell_array)
//mod 20151217:e
{
  int ret = OB_SUCCESS;
  //add wuna [MultiUps][sql_api] 20151217:b
  const ObString& prefix_name = table_node->get_prefix_name();
  ObArray<ObString> rule_param_list;
  ObArray<ObString> group_name_list;
  //add 20151217:e
  yysys::CThreadGuard guard(&init_mutex_);  // bug fix multithread op peiouya 20150826
  if(!inited_)
  {
    switch(type_)
    {
      case ObCalcExpression::HASH:
        calc_expr_ = OB_NEW_EXPRESSION(ObHashExpression);
        break;
      case ObCalcExpression::RANGE:
      case ObCalcExpression::CRANGE:
        calc_expr_ = OB_NEW_EXPRESSION(ObRangeExpression);
        break;
      case ObCalcExpression::ENUM:
      case ObCalcExpression::LIST://add wuna [MultiUps][sql_api] 20151217
        calc_expr_ = OB_NEW_EXPRESSION(ObEnumExpression);
        break;
        //add for [577-hardcode partition]-b
    case ObCalcExpression::HARDCODE:
        calc_expr_ = OB_NEW_EXPRESSION(ObHardCodeExpression);
        break;
        //add for [577-hardcode partition]-e
      default:
        ret = OB_ERROR;
        YYSYS_LOG(WARN, "unknow expression type");
        break;
    }
    if(OB_SUCCESS == ret)
    {
      if(NULL == calc_expr_)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        YYSYS_LOG(WARN, "no memory");
      }
    //mod wuna [MultiUps][sql_api] 20151217:b
      //else if(OB_SUCCESS != (ret = separate(rule_para_list_, PARTITION_LIST_SEPARATOR, params_)))
    else if(OB_SUCCESS != (ret = separate(rule_para_list_, PARTITION_LIST_SEPARATOR, rule_param_list)))
      {
        YYSYS_LOG(WARN, "split parameters failed");
      }
      //else if(params_.count() != static_cast<int64_t>(rule_para_num_))
    else if(rule_param_list.count() != static_cast<int64_t>(rule_para_num_))
    //mod 20151217:e
      {
        ret = OB_INNER_STAT_ERROR;
        YYSYS_LOG(WARN, "parameter number not matching parameter list");
      }
      //add wuna [MultiUps][sql_api] 20151217:b
      else if((ObCalcExpression::RANGE == type_ || ObCalcExpression::LIST == type_) &&
              OB_SUCCESS !=(ret = separate(prefix_name,PARTITION_LIST_SEPARATOR, group_name_list)))
      {
        YYSYS_LOG(WARN, "split range group name failed");
      }
      //add 20151217:e
      //mod wuna [MultiUps][sql_api] 20151217:b
      //else if(OB_SUCCESS != (ret = calc_expr_->set_expression(rule_body_)))
      //add for [577-hardcode partition]-b
      else if (ObCalcExpression::HARDCODE == type_)
      {
        if (OB_SUCCESS != (ret = calc_expr_->set_expression(rule_name_, group_name_list, cell_array)))
        {
          YYSYS_LOG(WARN, "resolve hardcode expression failed, ret=%d", ret);
        }
      }
      //add for [577-hardcode partition]-e
      else if(OB_SUCCESS != (ret = calc_expr_->set_expression(rule_body_,group_name_list,cell_array)))
      //mod 20151217:e
      {
        YYSYS_LOG(WARN, "resolve expression failed, ret=%d", ret);
      }
      else
      {
        inited_ = true;
      }
    }
  }
  if(OB_SUCCESS != ret && NULL != calc_expr_)
  {
    OB_DELETE_EXPRESSION(calc_expr_);
    calc_expr_ = NULL;
  }
  return ret;
}

