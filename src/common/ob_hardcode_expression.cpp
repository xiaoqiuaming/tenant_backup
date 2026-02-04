
#include "ob_hardcode_expression.h"
#include "common/ob_obj_cast.h"

using namespace oceanbase::common;
using namespace oceanbase::common::hash;

ObHardCodeExpression::ObHardCodeExpression()
  : ObCalcExpression(),
    column_num_(1),
    default_group_num_(0)
{
  hardcode_type_ = 0;
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = account_cell_map_.create(hash::cal_next_prime(512))))
  {
    YYSYS_LOG(ERROR, "create account_cell_map_ failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = card_cell_map_.create(hash::cal_next_prime(512))))
  {
    YYSYS_LOG(ERROR, "create card_cell_map_ failed, ret=%d", ret);
  }
  else if (HASH_INSERT_SUCC != card_cell_map_.set("017", 1))
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "hash map insert error:ret[%d]", ret);
  }
  else if (HASH_INSERT_SUCC != (card_cell_map_.set("617", 1)))
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "hash map insert error:ret[%d]", ret);
  }
  else if (HASH_INSERT_SUCC != (account_cell_map_.set("331", 1)))
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "hash map insert error:ret[%d]", ret);
  }
  else if (HASH_INSERT_SUCC != (card_cell_map_.set("131", 2)))
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "hash map insert error:ret[%d]", ret);
  }
  else if (HASH_INSERT_SUCC != (card_cell_map_.set("631", 2)))
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "hash map insert error:ret[%d]", ret);
  }
  else if (HASH_INSERT_SUCC != (account_cell_map_.set("443", 2)))
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "hash map insert error:ret[%d]", ret);
  }
  else if (HASH_INSERT_SUCC != (card_cell_map_.set("011", 3)))
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "hash map insert error:ret[%d]", ret);
  }
  else if (HASH_INSERT_SUCC != (card_cell_map_.set("511", 3)))
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "hash map insert error:ret[%d]", ret);
  }
  else if (HASH_INSERT_SUCC != (card_cell_map_.set("911", 3)))
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "hash map insert error:ret[%d]", ret);
  }
  else if (HASH_INSERT_SUCC != (account_cell_map_.set("310", 3)))
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "hash map insert error:ret[%d]", ret);
  }
  else if (HASH_INSERT_SUCC != (card_cell_map_.set("021", 4)))
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "hash map insert error:ret[%d]", ret);
  }
  else if (HASH_INSERT_SUCC != (card_cell_map_.set("521", 4)))
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "hash map insert error:ret[%d]", ret);
  }
  else if (HASH_INSERT_SUCC != (account_cell_map_.set("320", 4)))
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "hash map insert error:ret[%d]", ret);
  }
  else if (HASH_INSERT_SUCC != (card_cell_map_.set("061", 5)))
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "hash map insert error:ret[%d]", ret);
  }
  else if (HASH_INSERT_SUCC != (card_cell_map_.set("561", 5)))
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "hash map insert error:ret[%d]", ret);
  }
  else if (HASH_INSERT_SUCC != (account_cell_map_.set("421", 5)))
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "hash map insert error:ret[%d]", ret);
  }
  else if (HASH_INSERT_SUCC != (card_cell_map_.set("062", 6)))
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "hash map insert error:ret[%d]", ret);
  }
  else if (HASH_INSERT_SUCC != (card_cell_map_.set("562", 6)))
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "hash map insert error:ret[%d]", ret);
  }
  else if (HASH_INSERT_SUCC != (account_cell_map_.set("411", 6)))
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "hash map insert error:ret[%d]", ret);
  }
  else if (HASH_INSERT_SUCC != (card_cell_map_.set("071", 7)))
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "hash map insert error:ret[%d]", ret);
  }
  else if (HASH_INSERT_SUCC != (card_cell_map_.set("571", 7)))
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "hash map insert error:ret[%d]", ret);
  }
  else if (HASH_INSERT_SUCC != (account_cell_map_.set("441", 7)))
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "hash map insert error:ret[%d]", ret);
  }
  else if (HASH_INSERT_SUCC != (card_cell_map_.set("091", 8)))
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "hash map insert error:ret[%d]", ret);
  }
  else if (HASH_INSERT_SUCC != (card_cell_map_.set("591", 8)))
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "hash map insert error:ret[%d]", ret);
  }
  else if (HASH_INSERT_SUCC != (card_cell_map_.set("991", 8)))
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "hash map insert error:ret[%d]", ret);
  }
  else if (HASH_INSERT_SUCC != (account_cell_map_.set("110", 8)))
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "hash map insert error:ret[%d]", ret);
  }
}

ObHardCodeExpression::~ObHardCodeExpression()
{
  account_cell_map_.destroy();
  card_cell_map_.destroy();
}

int ObHardCodeExpression::set_expression(const ObString &expr, ObArray<ObString> &group_name_list,
                                         ObCellArray &cell_array, bool is_for_check_expr)
{
  UNUSED(group_name_list);
  UNUSED(cell_array);
  UNUSED(is_for_check_expr);

  int ret = OB_SUCCESS;
  if (0 == expr.length())
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "HardCode function name can't be empty.");
  }
  else if ((expr.compare(ObString::make_string("boc_account_mixs")) != 0)&&(expr.compare(ObString::make_string("boc_card_mixs")) != 0))
  {
    ret = OB_ERROR;
    YYSYS_LOG(ERROR, "HardCode function only supports boc_acount_mixs and boc_card_mixs,your function name is %.*s",expr.length(), expr.ptr());
  }
  else
  {
    YYSYS_LOG(INFO, "HardCode function is %.*s .", expr.length(), expr.ptr());
  }

  if (ret == OB_SUCCESS && expr.compare(ObString::make_string("boc_account_mixs")) == 0)
  {
    hardcode_type_ = 1;
  }
  else if (ret == OB_SUCCESS && expr.compare(ObString::make_string("boc_card_mixs")) == 0)
  {
    hardcode_type_ = 2;
  }
  return ret;
}

int ObHardCodeExpression::match(ObString orig, char *temp_result)
{
  int ret = OB_SUCCESS;
  int32_t orig_length = orig.length();
  char *temp_org;

  temp_org = orig.ptr();

  if (hardcode_type_ == 1)
  {
    switch(orig_length)
    {
      case 24:
      {
        temp_result[0] = temp_org[9];
        temp_result[1] = temp_org[10];
        temp_result[2] = temp_org[11];
      }
        break;
      default:
        ret = OB_NOT_SUPPORTED;
        YYSYS_LOG(WARN, "account format is not correct,it's length is %d,will use default group", orig.length());
        break;
    }
  }
  else if (hardcode_type_ == 2)
  {
    switch(orig_length)
    {
      case 17:
      {
        temp_result[0] = temp_org[7];
        temp_result[1] = temp_org[8];
        temp_result[2] = temp_org[9];
      }
        break;
      case 19:
      {
        temp_result[0] = temp_org[6];
        temp_result[1] = temp_org[7];
        temp_result[2] = temp_org[8];
      }
        break;
      default:
        ret = OB_NOT_SUPPORTED;
        YYSYS_LOG(WARN, "card format is not correct,it's length is %d,will use default group", orig.length());
        break;
    }
  }
  else
  {
    ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

int ObHardCodeExpression::calc(
    const ObCellArray &cell_array,
    ExpressionType part_type,
    ObArray<ObString> &rule_param_list,
    ObObj &result)
{
  YYSYS_LOG(INFO, "calc hardcode hardcode_type = %ld", hardcode_type_);
  int ret = OB_SUCCESS;
  char orig_char[OB_MAX_VARCHAR_LENGTH] = {0};
  ObString orig_str;
  orig_str.assign_ptr(orig_char, OB_MAX_VARCHAR_LENGTH);

  char result_char[4] = {0};
  ObString result_str;
  result_str.assign_ptr(result_char, 4);
  UNUSED(rule_param_list);
  OB_ASSERT(part_type == ObCalcExpression::HARDCODE);
  ObObj ckeck_value[MAX_COLUMN_NUM];

  ckeck_value[0] = cell_array[0].value_;
  if (OB_SUCCESS != (ret = cell_array[0].value_.get_varchar(orig_str)))
  {
    YYSYS_LOG(ERROR, "calc hardcode expression failed.ret = %d", ret);
    ret = OB_ERROR;
  }
  else
  {
    ret = match(orig_str, result_char);
    result_char[3] = '\0';
    YYSYS_LOG(INFO, "calc hardcode expression, orig_str=%.*s, the key_str=%s.ret = %d", orig_str.length(), orig_str.ptr(), result_char, ret);
  }

  if ( ret == OB_NOT_SUPPORTED)
  {
    result.set_int(default_group_num_);
    YYSYS_LOG(WARN, "The length is not correct , It's key is %s, will use default group", result_char);
    ret = OB_SUCCESS;
  }
  else if (ret == OB_SUCCESS)
  {
    int32_t temp = -1;
    if (hardcode_type_ == 1 && (account_cell_map_.get(result_char, temp) != HASH_EXIST))
    {
      YYSYS_LOG(WARN, "The key of account is not in enum, It's key is %s,will use default group", result_char);
      result.set_int(default_group_num_);
    }
    else if (hardcode_type_ == 2 && (card_cell_map_.get(result_char, temp) != HASH_EXIST))
    {
      YYSYS_LOG(WARN, "The key of card is not in enum, It's key is %s,will use default group", result_char);
      result.set_int(default_group_num_);
    }
    else
    {
      switch (temp)
      {
        case 1:
          result.set_int(1);
          break;
        case 2:
          result.set_int(2);
          break;
        case 3:
          result.set_int(3);
          break;
        case 4:
          result.set_int(4);
          break;
        case 5:
          result.set_int(5);
          break;
        case 6:
          result.set_int(6);
          break;
        case 7:
          result.set_int(7);
          break;
        case 8:
          result.set_int(8);
          break;
        default:
          YYSYS_LOG(INFO, "The key of account or card is not in enum, It's key is %s,will use default group", result_char);
          result.set_int(default_group_num_);
          break;

      }
    }
    ret = OB_SUCCESS;
  }
  else
  {
    YYSYS_LOG(WARN, "there is something wrong when calc, it's key is %s,will use default group", result_char);
    result.set_int(default_group_num_);
    ret = OB_SUCCESS;
  }

  if (OB_SUCCESS != ret)
  {
    YYSYS_LOG(WARN, "calc hardcode expression failed.ret = %d", ret);
  }
  return ret;
}

int32_t ObHardCodeExpression::get_column_num()
{
  return column_num_;
}
