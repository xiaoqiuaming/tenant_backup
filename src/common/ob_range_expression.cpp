/*
*   Version: 0.1
*
*   Authors:
*      liu jun
*        - some work details if you want
*/

#include "ob_range_expression.h"
#include "common/ob_obj_cast.h" //add wuna [MultiUps][sql_api] 20151222

using namespace oceanbase::common;

//add wuna [MultiUps][sql_api] 20151217:b
int64_t RangeItem::to_string(char* buffer, int64_t length) const
{
  int64_t pos = 0;
  int64_t temp_pos=0;
  pos = key.to_string(buffer,length);
  temp_pos = group_name.to_string(buffer+pos,length-pos);
  pos = pos + temp_pos;
  return pos;
}
//add 20151217:e
//mod wuna [MultiUps][sql_api] 20151217:b
//ObRangeExpression::ObRangeExpression(FIFOAllocator &allocator) : ObCalcExpression(allocator)
ObRangeExpression::ObRangeExpression() : ObCalcExpression(),
  column_num_(0),max_define_len_(0)
//mod 20151217:e
{
}
//add wuna [MultiUps][sql_api] 20151217:b
ObRangeExpression::~ObRangeExpression()
{
  for (int64_t i=0; i<range_values_.count(); i++)
  {
    if (NULL != range_values_.at(i))
    {
      range_values_.at(i)->~RangeItem();
    }
  }
  range_values_.clear();
  values_type_.clear();
}
//add 20151217:e
//mod wuna [MultiUps][sql_api] 20151217:b
//int ObRangeExpression::set_expression(const ObString &expr)
int ObRangeExpression::set_expression(const ObString &expr,
                                      ObArray<ObString> &group_name_list,
                                      ObCellArray& cell_array,
                                      bool is_for_check_expr)
//mod 20151217:e
{
  //add wuna [MultiUps][sql_api] 20151217:b
  //UNUSED(is_for_check_expr);
  int ret = OB_SUCCESS;
  int pos = 0;  // char position in range_rule_body_str
  const char *range_str = expr.ptr();
  max_define_len_ = expr.length();
  int range_value_idx = 0;
  int32_t column_num = 0;
  ObObj *group_values = NULL;
  RangeItem *item = NULL;

  while(pos < max_define_len_ && OB_SUCCESS == ret)
  {
    column_num = column_num_ > 0 ? column_num_ : MAX_COLUMN_NUM;
    if (NULL == (group_values = (ObObj*)allocator_.alloc(sizeof(ObObj)*column_num))
        || NULL == (item = (RangeItem *)allocator_.alloc(sizeof(RangeItem))))
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    else
    {
      item = new(item) RangeItem();
      //TODO skip_blank(range_str, pos); // ����ǰ���ո�
      if ('(' != range_str[pos++])
      {
        ret = OB_ERROR;
        YYSYS_LOG(WARN, "range expression start error, expr:%c", range_str[pos-1]);
        break;
      }
      else if(OB_SUCCESS != (ret = parse_element_values(range_str, pos, group_values, cell_array, is_for_check_expr)))
      {
        YYSYS_LOG(WARN, "parse element values error");
        break;
      }
      else
      {
        if(')' != range_str[pos++])
        {
          ret = OB_ERROR;
          YYSYS_LOG(WARN, "parse group values end error.");
          break;
        }
        else if(pos < max_define_len_)
        {
          if(',' != range_str[pos++])
          {
            ret = OB_ERROR;
            YYSYS_LOG(WARN, "range expression end error, expr:%.*s", expr.length(), expr.ptr());
            break;
          }
        }
        if(OB_SUCCESS == ret)
        {
          item->key.assign(group_values, column_num_);
          item->value_idx = range_value_idx;
          if(0 != group_name_list.count())
          {
            if (OB_SUCCESS !=(ret = ob_write_string(allocator_, group_name_list.at(range_value_idx), item->group_name)))
            {
              YYSYS_LOG(WARN,"failed to write string.");
              break;
            }
          }
          //          else
          //          {
          //            range_values_.push_back(item);
          //            ++range_value_idx;
          //          }
          range_values_.push_back(item);
          ++range_value_idx;
        }
      }
    }
  }
  //add 20151217:e
  return ret;
}
//add wuna [MultiUps][sql_api] 20151217:b
int ObRangeExpression::parse_element_values(const char* str,
                                            int &pos,
                                            ObObj* value,
                                            ObCellArray& cell_array,
                                            bool is_for_check_expr)
{
  int err = OB_SUCCESS;
  //TODO skip_blank(p, pos); // ����ǰ���ո�
  int32_t current_column_num = 0;
  int32_t string_len = 0;
  char *string_ptr = NULL;

  while(pos < max_define_len_ && OB_SUCCESS == err)
  {
    string_len = get_str_len(str, pos);
    ObString string_value;
    const char* maxval = "maxvalue";
    if(0 == string_len)
    {
      err = OB_ERROR;
      YYSYS_LOG(ERROR, "parse range body expr error.");
      break;
    }
    else if (NULL == (string_ptr = (char *)allocator_.alloc(string_len)))
    {
      err = OB_ALLOCATE_MEMORY_FAILED;
      YYSYS_LOG(ERROR, "OB_ALLOCATE_MEMORY_FAILED");
      break;
    }
    else
    {
      memcpy(string_ptr, str+pos, string_len);
      string_value.assign_ptr(string_ptr, string_len);
    }
    if(!is_for_check_expr)
    {
      ObObj orig_cell;
      orig_cell.set_varchar(string_value);
      ObObj cast_obj;
      ObObj obj_type;
      ObObjType type = cell_array[current_column_num].value_.get_type();
      obj_type.set_type(cell_array[current_column_num].value_.get_type());
      const ObObj *res_obj = NULL;
      if(0 == string_value.compare(maxval))
      {
        value[current_column_num++].set_max_value();
      }
      else if(str_is_str(string_value) && type != ObVarcharType)
      {
        YYSYS_LOG(WARN, "the type in partition func is varchar, the value's type is %d", type);
        err = OB_NOT_SUPPORTED;
      }
      else
      {
        if (OB_SUCCESS != (err = obj_cast(orig_cell, obj_type, cast_obj, res_obj)))
        {
          YYSYS_LOG(WARN, "failed to cast to int, obj=%s", to_cstring(orig_cell));
          break;
        }
        else
        {
          OB_ASSERT(type == res_obj->get_type());
          int64_t int_val=0;
          // add by maosy [MultiUps 1.0] [#12]
          int32_t int32_val =0 ;
          //add e
          ObDateTime datetime_val=0;
          switch(res_obj->get_type())
          {
            case ObIntType:
              err = res_obj->get_int(int_val);
              value[current_column_num++].set_int(int_val);
              break;
              // add by maosy [MultiUps 1.0] [#12]
            case ObInt32Type:
              err = res_obj->get_int32(int32_val);
              value[current_column_num++].set_int32(int32_val);
              break;
              // add e
            case ObVarcharType:
              value[current_column_num++].set_varchar(string_value);
              break;
            case ObDateTimeType:
              err = res_obj->get_datetime(datetime_val);
              value[current_column_num++].set_datetime(datetime_val);
              break;
            default:
              err = OB_ERROR;
              YYSYS_LOG(ERROR,"unexpected type,type = %d",res_obj->get_type());
              break;
          }
        }
      }
    }
    else
    {
      YYSYS_LOG(INFO, "string_value = [%.*s]", string_value.length(), string_value.ptr());
      if(0 == string_value.compare(maxval))
      {
        value[current_column_num++].set_max_value();
      }
      else
      {
        if(str_is_str(string_value))
        {
          value[current_column_num++].set_varchar(string_value);
        }
        else if(str_is_int(string_value))
        {
          int64_t result;
          if(OB_SUCCESS == str_to_int(string_value.ptr(), string_value.length(),result))
          {
            value[current_column_num++].set_int(result);
          }
          else
          {
            err = OB_ERROR;
          }
        }
        else
        {
          err = OB_ERROR;
          YYSYS_LOG(ERROR, "data_type must be int,ret=%d", err);
        }
      }
      if(OB_SUCCESS == err && 0 == column_num_)
      {
        values_type_.push_back(value[current_column_num - 1].get_type());
      }
    }

    if(OB_SUCCESS == err)
    {
      pos += string_len;
      if(',' != str[pos])
      {
        break;
      }
      else
      {
        ++pos;
      }
    }
    else
    {
      break;
    }
  }

  if(OB_SUCCESS == err)
  {
    if(0 == column_num_)
    {
      column_num_ = current_column_num;
      //mod lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160701:b
      //YYSYS_LOG(INFO, "set column num :%d", current_column_num);
      YYSYS_LOG(DEBUG, "set column num :%d", current_column_num);
      //mod 20160701:e
    }
    else if(current_column_num != column_num_)
    {
      err = OB_ERROR;
      YYSYS_LOG(ERROR, "range body expr column num is not consistent.");
    }
    //[416]
    else if(is_for_check_expr && OB_SUCCESS != (err = check_range_values_type(value)))
    {
      YYSYS_LOG(ERROR, "the value's type is not consistent");
    }
  }

  return err;
}
int ObRangeExpression::get_str_len(const char* str, const int pos)
{
  int tempos = pos;
  while(tempos < max_define_len_ && ')' != str[tempos] && ',' != str[tempos])
  {
    ++tempos;
  }
  return tempos - pos;
}
//add 20151217:e
int ObRangeExpression::calc(
    const ObCellArray &cell_array,
    ExpressionType part_type,//add wuna [MultiUps][sql_api] 20151217
    ObArray<ObString>& rule_param_list,
    ObObj &result)
{
  //add wuna [MultiUps][sql_api] 20151217:b
  UNUSED(rule_param_list);
  int ret = OB_SUCCESS;
  OB_ASSERT(part_type == ObCalcExpression::RANGE ||
            part_type == ObCalcExpression::CRANGE);
  ObObj check_value[column_num_];
  for(int i = 0; i < column_num_; i++)
  {
    check_value[i] = cell_array[i].value_;
  }
  ObRowkey check_key(check_value,column_num_);

  int32_t num_elements = (int32_t)range_values_.count();
  int32_t array_index=0;
  for(array_index=0; array_index<num_elements; array_index++)
  {
    int cmp = 0;
    cmp = check_key.compare(range_values_.at(array_index)->key);
    if(cmp>=0)
    {
      continue;
    }
    else
    {
      if(part_type == ObCalcExpression::CRANGE)
      {
        result.set_int(range_values_.at(array_index)->value_idx);
      }
      else if(part_type == ObCalcExpression::RANGE)
      {
        result.set_varchar(range_values_.at(array_index)->group_name);
      }
        break;
    }
  }
  if(array_index == num_elements)
  {
    ret = OB_ERR_NO_PARTITION;
    YYSYS_LOG(USER_ERROR,"Table has no partition for value.");
  }
  //add 20151217:e
  return ret;
}

int ObRangeExpression::get_column_num()
{
  return column_num_;
}

int ObRangeExpression::str_to_int(const char *str, const int &len, int64_t &value)
{
  int ret = OB_SUCCESS;
  bool positive = true;
  int64_t flag = 1;
  value = 0;

  for(int i = 0; i < len; i++)
  {
    if(0 == i && '-' == str[i])
    {
      positive = false;
      flag = -1;
      continue;
    }
    else if(0 == i && '+' == str[i])
    {
      continue;
    }
    value = value * 10 + flag * static_cast<int64_t>(str[i] - '0');
    if((positive && (value > INT64_MAX || value < 0)) || (!positive && (value < INT64_MIN || value > 0)))//overflow
    {
      YYSYS_LOG(WARN, "value overflow, legal range[%ld, %ld]", INT64_MAX, INT64_MIN);
      value = 0;
      ret = OB_ERROR;
      break;
    }
  }
  return ret;

}

bool ObRangeExpression::str_is_int(ObString str)
{
  bool flag = true;
  const char *s = str.ptr();
  for(int32_t i = 0; i < str.length(); i++)
  {
    if(0 == i && ('-' == s[i] || '+' == s[i]))
    {
      continue;
    }
    int32_t temp = s[i] - '0';
    if(0 <= temp && temp <= 9)
    {
      continue;
    }
    else
    {
      flag = false;
      break;
    }
  }
  return flag;
}


//[416]
bool ObRangeExpression::str_is_str(ObString &str)
{
  bool flag = false;
  char *s = str.ptr();
  const int32_t str_len = str.length();

  if(str_len >= 2 && s[0] == '\'' && s[str_len - 1] == '\'')
  {
    flag = true;
    memmove(s, s + 1, str_len - 2);
    s[str_len - 2] = '\0';
    str.set_length(str_len - 2);
  }
  return flag;
}

int ObRangeExpression::check_range_values_type(ObObj *range_value)
{
  int err = OB_SUCCESS;
  int64_t type_size = values_type_.count();

  if(type_size != column_num_)
  {
    err = OB_ERR_UNEXPECTED;
    YYSYS_LOG(ERROR, "the value_type count is not equal to column_size");
  }
  else
  {
    for(int64_t i = 0; i < column_num_; i++)
    {
      if(!range_value[i].is_max_value() && ObExtendType != values_type_.at(i)
         && range_value[i].get_type() != values_type_.at(i))
      {
        err = OB_INVALID_ARGUMENT;
        YYSYS_LOG(WARN, "the %ld'th value's type is not equal in all range values, err=%d", i + 1, err);
        break;
      }
    }
  }
  return err;
}


int ObRangeExpression::check_range_partition_value()
{
  int ret = OB_SUCCESS;

  if(1 == range_values_.count() && !(range_values_.at(0)->key.is_max_row()))
  {
    ret = OB_ERROR;
    YYSYS_LOG(ERROR, "the last value must be maxvalue.");
  }

  for(int64_t i = 1; i < range_values_.count() && OB_SUCCESS == ret; i++)
  {
    int cmp = 0;
    cmp = range_values_.at(i - 1)->key.compare((range_values_.at(i)->key));
    if (cmp >= 0)
    {
      ret = OB_ERROR;
      YYSYS_LOG(ERROR, "range partition value must be strictly increasing for each partition.");
      break;
    }
    else
    {
      if(i == (range_values_.count()- 1) && !(range_values_.at(i)->key.is_max_row()))
      {
        ret = OB_ERROR;
        YYSYS_LOG(ERROR, "the last value must be maxvalue.");
      }
      continue;
    }
  }
  return ret;
}
