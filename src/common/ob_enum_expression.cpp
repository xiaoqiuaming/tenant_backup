/*
*   Version: 0.1
*
*   Authors:
*      tian zheng
*        - some work details if you want
*/

#include "ob_enum_expression.h"
#include "common/ob_obj_cast.h"//add wuna [MultiUps][sql_api] 20151222

using namespace oceanbase::common;

int64_t EnumItem::to_string(char* buffer, int64_t length) const
{
  int64_t pos = 0;
  int64_t temp_pos = 0;
  pos = key.to_string(buffer,length);
  databuff_printf(buffer, length, pos, "value_idx: %d ", value_idx);
  temp_pos = group_name.to_string(buffer+pos,length-pos);
  pos = pos + temp_pos;
  return pos;
}

ObEnumExpression::ObEnumExpression()
  :ObCalcExpression(),
    column_num_(0),
    max_define_len_(0)
{
  /*add wuna [MultiUps][sql_api] 20151228:b*/
  int ret = OB_SUCCESS;
  if(OB_SUCCESS != (ret = all_element_values_.create(hash::cal_next_prime(512))))
  {
    YYSYS_LOG(ERROR,"create all_part_rules_ failed, ret=%d", ret);
  }
  /*add 20151228:e*/

}

ObEnumExpression::~ObEnumExpression()
{
  for (int64_t i=0; i<element_values_.count(); i++)
  {
    if (NULL != element_values_.at(i))
    {
      element_values_.at(i)->~EnumItem();
    }
  }
  element_values_.clear();
  all_element_values_.destroy();
}

//eg:1
//ö�ٵ�ֵ��'[]'���֣�Ĭ�ϴ�0������
//func0(a) = [1,2,3],[4,5,6]
//func0(1) = 0;func0[5] = 1;
//eg:2
//func1(a,b,c) = [(1,2,3),(4,5,6)],[(7,8,9)]
//func1(1,2,3) = 0;func1(7,8,9) = 1;
//mod wuna [MultiUps][sql_api] 20151217:b
//int ObEnumExpression::set_expression(const ObString &expr)
int ObEnumExpression::set_expression(const ObString &expr,ObArray<ObString> &group_name_list,
                                     ObCellArray& cell_array,bool is_for_check_expr)
//mod 20151217:e
{
  int ret = OB_SUCCESS;
  int pos = 0;  // char position in enum_str
  const char *enum_str = expr.ptr();
  int enum_value_idx = 0;

  if(0 == (max_define_len_ = expr.length()))
  {
    ret=OB_ERROR;
    YYSYS_LOG(WARN, "Enum function body can't be empty.");
  }
  while(pos < max_define_len_ && OB_SUCCESS == ret)
  {
    //TODO skip_blank(enum_str, pos); // ����ǰ���ո�
    if ('[' != enum_str[pos++])
    {
      ret = OB_ERROR;
      YYSYS_LOG(WARN, "enum expression start error, expr:%.*s", expr.length(), expr.ptr());
    }
    //mod wuna [MultiUps][sql_api] 20151217:b
    //    else if(OB_SUCCESS != (ret = parse_group_values(enum_str, pos, enum_value_idx)))
    else if(OB_SUCCESS != (ret = parse_group_values(enum_str, pos, enum_value_idx,
                                                    group_name_list,cell_array,is_for_check_expr)))
      //mod 20151217:e
    {
      YYSYS_LOG(WARN, "parse element values error");
    }
    else if (']' != enum_str[pos++])
    {
      ret = OB_ERROR;
      YYSYS_LOG(WARN, "enum expression end error, expr:%.*s", expr.length(), expr.ptr());
    }
    else if(pos < max_define_len_)
    {
      enum_value_idx ++;
      if(',' != enum_str[pos++])
      {
        ret = OB_ERROR;
        YYSYS_LOG(WARN, "enum expression end error, expr:%.*s", expr.length(), expr.ptr());
      }
    }
  }
  if(OB_SUCCESS == ret)
  {
    sort_elements();
  }
  return ret;
}

int ObEnumExpression::calc(
    const ObCellArray &cell_array,
    ExpressionType part_type,//add wuna [MultiUps][sql_api] 20151217
    ObArray<ObString>& rule_param_list,
    ObObj &result)
{
  int ret = OB_SUCCESS;
  UNUSED(rule_param_list);
  OB_ASSERT(part_type == ObCalcExpression::ENUM ||
            part_type == ObCalcExpression::LIST);//add wuna [MultiUps][sql_api] 20151217
  ObObj ckeck_value[MAX_COLUMN_NUM];
  for(int i = 0; i < column_num_; i++)
  {
    ckeck_value[i] = cell_array[i].value_;
  }

  ObRowkey check_key(ckeck_value,column_num_);
  //mod wuna [MultiUps][sql_api] 20151217:b
  //return calc(check_key,result);
  ret = calc(check_key,part_type,result);
  if(OB_SUCCESS != ret)
  {
    YYSYS_LOG(WARN,"calc enum expression failed.ret = %d",ret);
  }
  return ret;
  //mod 20151217:e
}
//mod wuna [MultiUps][sql_api] 20151217:b	
//int ObEnumExpression::calc(const ObRowkey &check_key, ObObj &result)
int ObEnumExpression::calc(const ObRowkey &check_key,ExpressionType type,ObObj &result)
//mod 20151217:e
{
  int ret = OB_SUCCESS;

  int32_t num_elements = (int32_t)element_values_.count();
  int array_index = 0, cmp = 0;
  int min_array_index = 0;
  int max_array_index = num_elements -1;
  bool is_found=false;//add wuna [MultiUps][sql_api] 20151217
  while(max_array_index >= min_array_index)
  {
    array_index = (max_array_index + min_array_index) >> 1;
    cmp = check_key.compare(element_values_.at(array_index)->key);

    if (cmp > 0)
      min_array_index= array_index + 1;
    else if (cmp < 0)
    {
      if (!array_index)
      {
        ret = OB_ERROR;
        YYSYS_LOG(WARN, "can not find the enumeration value");
        break;
      }
      max_array_index= array_index - 1;
    }
    else
    {
      //mod wuna [MultiUps][sql_api] 20151217:b
      //result.set_int(element_values_.at(array_index)->value_idx);
      if(type == ObCalcExpression::ENUM)
      {
        result.set_int(element_values_.at(array_index)->value_idx);
      }
      else if(type == ObCalcExpression::LIST)
      {
        result.set_varchar(element_values_.at(array_index)->group_name);
      }
      is_found = true;
      //mod 20151217:e
      break;
    }
  }
  //add wuna [MultiUps][sql_api] 20151217:b
  if(false == is_found)
  {
    ret = OB_ERR_NO_PARTITION;
    YYSYS_LOG(USER_ERROR, "Table has no partition for given value.");
  }
  //add 20151217:e
  return ret;
}
//mod wuna [MultiUps][sql_api] 20160629:b
int ObEnumExpression::get_str_len(const char* str, const int pos)
{
  int tempos = pos;
  while(tempos < max_define_len_ && ',' != str[tempos])
  {
    if(')' == str[tempos] && (']' == str[tempos+1] || ',' == str[tempos+1]))
      break;

    ++tempos;
  }
  return tempos - pos;
}
//mod 20160629:e

//mod wuna [MultiUps][sql_api] 20151222:b
//int ObEnumExpression::parse_group_values(const char* str, int &pos, const int32_t value_idx)
int ObEnumExpression::parse_group_values(const char* str, int &pos, const int32_t value_idx,ObArray<ObString> &group_name_list,
                                         ObCellArray& cell_array,bool is_for_check_expr)
//mod 20151222:e
{

  int ret = OB_SUCCESS;

  int32_t column_num = 0;
  ObObj *group_values = NULL;
  EnumItem *item = NULL;

  while(OB_SUCCESS == ret && pos < max_define_len_)
  {
    column_num = column_num_ > 0 ? column_num_ : MAX_COLUMN_NUM;
    if (NULL == (group_values = (ObObj*)allocator_.alloc(sizeof(ObObj)*column_num))
        || NULL == (item = (EnumItem *)allocator_.alloc(sizeof(EnumItem))))
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    else
    {
      item = new(item) EnumItem();
      //TODO skip_blank(str, pos); // ����ǰ���ո�
      if ('(' != str[pos++])
      {
        ret = OB_ERROR;
        YYSYS_LOG(WARN, "enum expression start error, expr:%c", str[pos-1]);
      }
      //mod wuna [MultiUps][sql_api] 20151222:b
      //else if(OB_SUCCESS != (ret = parse_element_values(str, pos, group_values)))
      else if(OB_SUCCESS != (ret = parse_element_values(str, pos, group_values,cell_array,is_for_check_expr)))
        //mod 20151222:e
      {
        YYSYS_LOG(WARN, "parse element values error");
      }
      else
      {
        item->key.assign(group_values, column_num_);
        item->value_idx = value_idx;
        //add wuna [MultiUps][sql_api] 20151222:b
        if(is_for_check_expr)
        {
          int32_t temp=0;
          if(hash::HASH_EXIST == all_element_values_.get(item->key, temp))
          {
            ret = OB_ERR_PARTITION_VALUE_DUPLICATE;
            YYSYS_LOG(WARN,"Multiple definition of same constant in list partitioning.");
            break;
          }
          else
          {
            if(hash::HASH_INSERT_SUCC != all_element_values_.set(item->key, item->value_idx))
            {
              ret = OB_ERROR;
              YYSYS_LOG(WARN, "failed to set part rules of all_element_values_");
              break;
            }
          }
        }
        else if(0 != group_name_list.count())//list
        {
          if ((ret = ob_write_string(allocator_, group_name_list.at(value_idx), item->group_name)) != OB_SUCCESS)
          {
            YYSYS_LOG(WARN,"failed to write string.");
            break;
          }
        }
        else//enum
        {
          //do nothing
        }
        //add 20151222:e
        element_values_.push_back(item);

        if(')' != str[pos++])
        {
          YYSYS_LOG(WARN, "parse group values end error");
          ret = OB_ERROR;
        }
        else if(',' != str[pos])
        {
          break;
        }
        else
        {
          ++pos;
        }
      }
    }
  }

  return ret;
}
//mod wuna [MultiUps][sql_api] 20151222:b
//int ObEnumExpression::parse_element_values(const char* str, int &pos, ObObj* value)
int ObEnumExpression::parse_element_values(const char* str, int &pos, ObObj* value,
                                           ObCellArray& cell_array,bool is_for_check_expr)
//mod 20151222:e
{
  int err = OB_SUCCESS;
  //TODO skip_blank(p, pos); // ����ǰ���ո�

  int32_t current_column_num = 0;
  int32_t string_len = 0;
  char *string_ptr = NULL;
  while(pos < max_define_len_ && OB_SUCCESS == err)
  {
    ObString string_value;
    string_len = get_str_len(str, pos);
    if(MAX_COLUMN_NUM == current_column_num)
    {
      //      current_column_num++;
      err = OB_ERROR;
      YYSYS_LOG(WARN, "enum values num should less than 17.");
      break;
    }
    else if(0 == string_len)
    {
      err = OB_ERROR;
      YYSYS_LOG(ERROR, "parse enum body expr error.");
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
    //mod wuna [MultiUps] [sql_api] 20151222:b
    if(!is_for_check_expr)
    {
      ObObj orig_cell;
      orig_cell.set_varchar(string_value);
      ObObj cast_obj;
      ObObj obj_type;
      ObObjType type = cell_array[current_column_num].value_.get_type();
      obj_type.set_type(cell_array[current_column_num].value_.get_type());
      const ObObj *res_obj = NULL;
      if (OB_SUCCESS != (err = obj_cast(orig_cell, obj_type, cast_obj, res_obj)))
      {
        YYSYS_LOG(WARN, "failed to cast to int, obj=%s,ret=%d", to_cstring(orig_cell),err);
        break;
      }
      else
      {
        OB_ASSERT(type == res_obj->get_type());
        int64_t int_val=0;
        int32_t int32_val=0;
        ObDateTime datetime_val=0;
        switch(res_obj->get_type())
        {
          case ObIntType:
            err = res_obj->get_int(int_val);
            value[current_column_num++].set_int(int_val);
            break;
          case ObInt32Type:
            err = res_obj->get_int32(int32_val);
            value[current_column_num++].set_int32(int32_val);
            break;
          case ObVarcharType:
            value[current_column_num++].set_varchar(string_value);
            break;
          case ObDateTimeType:
            err = res_obj->get_datetime(datetime_val);
            value[current_column_num++].set_datetime(datetime_val);
            break;
          default:
            err = OB_ERROR;
            YYSYS_LOG(ERROR,"unexpected type.type=%s",ob_obj_type_str(res_obj->get_type()));
            break;
        }
      }
    }
    else
    {
      value[current_column_num++].set_varchar(string_value);
    }
    pos += string_len;
    if(',' != str[pos])
    {
      break;
    }
    else
    {
      ++pos;
    }
    //mod 20151222:e
  }

  if(OB_SUCCESS == err)
  {
    if(0 == column_num_)
    {
      column_num_ = current_column_num;
      YYSYS_LOG(DEBUG, "set column num :%d", current_column_num);
    }
    else if(current_column_num != column_num_)
    {
      err = OB_ERROR;
      YYSYS_LOG(ERROR, "enum body expr column num is not consistent.");
    }
  }

  return err;
}

void ObEnumExpression::sort_elements()
{
  YYSYS_LOG(DEBUG, "elements before sort:");
  print_element();
  if (0 < element_values_.count())
  {
    YYSYS_LOG(DEBUG, "sort elements, count=%ld,", element_values_.count());
    EnumItem** first_element = &element_values_.at(0);
    std::sort(first_element, first_element+element_values_.count(), EnumItemCompare());
  }
  YYSYS_LOG(DEBUG, "elements after sort:");
  print_element();
}

void ObEnumExpression::print_element()
{
  YYSYS_LOG(DEBUG, "elements BEGIN:");
  for(int i = 0; i < element_values_.count(); ++i)
  {
    YYSYS_LOG(DEBUG, "elements[%d]:key=%s, index=%d", i, to_cstring(element_values_.at(i)->key),
              element_values_.at(i)->value_idx);
  }
  YYSYS_LOG(DEBUG, "elements END");
}
//add wuna [MultiUps] [sql_api] 20151222:b
int32_t ObEnumExpression::get_column_num()
{
  return column_num_;
}
//add 20151222:e
