/*
*   Version: 0.1
*
*   Authors:
*      liu jun
*        - some work details if you want
*/

#include "ob_hash_expression.h"
#include "ob_postfix_expression.h"

using namespace oceanbase::common;
using namespace oceanbase::common::ObExpression;

//mod wuna [MultiUps][sql_api] 20151217:b
//ObHashExpression::ObHashExpression(FIFOAllocator &allocator) : ObCalcExpression(allocator)
//modify for [540-hash partition cal]-b
//ObHashExpression::ObHashExpression() : ObCalcExpression(),hash_mod_expr_("%11")
ObHashExpression::ObHashExpression() : ObCalcExpression()
//mod 20151217:e
{
  hash_mod_expr_ = to_cstring(OB_MAX_MOD_NUM); //add for [540-hash partition cal]
  //modify for [540-hash partition cal]-e
}

ObHashExpression::~ObHashExpression()
{
}

static inline int reserved_func(const ObExprObj &obj1, const ObExprObj &obj2, ObExprObj &result)
{
  int err = OB_INVALID_ARGUMENT;
  UNUSED(obj1);
  UNUSED(obj2);
  UNUSED(result);
  return err;
}


/* compare function list:
 * >   gt_func
 * >=  ge_func
 * <=  le_func
 * <   lt_func
 * ==  eq_func
 * !=  neq_func
 */
static int gt_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
{
  int err = OB_SUCCESS;
  if (NULL == stack_i)
  {
    YYSYS_LOG(WARN, "stack pointer is NULL.");
    err = OB_INVALID_ARGUMENT;
  }
  else if (idx_i < 2)
  {
    YYSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
    err = OB_INVALID_ARGUMENT;
  }
  else
  {

    stack_i[idx_i-2].gt(stack_i[idx_i-1], result);
    if (result.is_null())
    {
      ObObjType this_type = stack_i[idx_i-2].get_type();
      ObObjType other_type = stack_i[idx_i-1].get_type();
      if (this_type == ObNullType || other_type == ObNullType)
      {
        int cmp = this_type - other_type;
        result.set_bool(cmp > 0);
      }
      else
      {
        YYSYS_LOG(WARN, "unexpected branch, this_type=%d other_type=%d", this_type, other_type);
        err = OB_ERR_UNEXPECTED;
      }
    }
    idx_i -= 2;
  }
  return err;
}

static int ge_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
{
  int err = OB_SUCCESS;
  if (NULL == stack_i)
  {
    YYSYS_LOG(WARN, "stack pointer is NULL.");
    err = OB_INVALID_ARGUMENT;
  }
  else if (idx_i < 2)
  {
    YYSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    stack_i[idx_i-2].ge(stack_i[idx_i-1], result);
    if (result.is_null())
    {
      ObObjType this_type = stack_i[idx_i-2].get_type();
      ObObjType other_type = stack_i[idx_i-1].get_type();
      if (this_type == ObNullType || other_type == ObNullType)
      {
        int cmp = this_type - other_type;
        result.set_bool(cmp >= 0);
      }
      else
      {
        YYSYS_LOG(WARN, "unexpected branch, this_type=%d other_type=%d", this_type, other_type);
        err = OB_ERR_UNEXPECTED;
      }
    }
    idx_i -= 2;
  }
  return err;
}

static int le_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
{
  int err = OB_SUCCESS;
  if (NULL == stack_i)
  {
    YYSYS_LOG(WARN, "stack pointer is NULL.");
    err = OB_INVALID_ARGUMENT;
  }
  else if (idx_i < 2)
  {
    YYSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    stack_i[idx_i-2].le(stack_i[idx_i-1], result);
    if (result.is_null())
    {
      ObObjType this_type = stack_i[idx_i-2].get_type();
      ObObjType other_type = stack_i[idx_i-1].get_type();
      if (this_type == ObNullType || other_type == ObNullType)
      {
        int cmp = this_type - other_type;
        result.set_bool(cmp <= 0);
      }
      else
      {
        YYSYS_LOG(WARN, "unexpected branch, this_type=%d other_type=%d", this_type, other_type);
        err = OB_ERR_UNEXPECTED;
      }
    }
    idx_i -= 2;
  }
  return err;
}

static int lt_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
{
  int err = OB_SUCCESS;
  if (NULL == stack_i)
  {
    YYSYS_LOG(WARN, "stack pointer is NULL.");
    err = OB_INVALID_ARGUMENT;
  }
  else if (idx_i < 2)
  {
    YYSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    stack_i[idx_i-2].lt(stack_i[idx_i-1], result);
    if (result.is_null())
    {
      ObObjType this_type = stack_i[idx_i-2].get_type();
      ObObjType other_type = stack_i[idx_i-1].get_type();
      if (this_type == ObNullType || other_type == ObNullType)
      {
        int cmp = this_type - other_type;
        result.set_bool(cmp < 0);
      }
      else
      {
        YYSYS_LOG(WARN, "unexpected branch, this_type=%d other_type=%d", this_type, other_type);
        err = OB_ERR_UNEXPECTED;
      }
    }
    idx_i -= 2;
  }
  return err;
}

static int eq_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
{
  int err = OB_SUCCESS;
  if (NULL == stack_i)
  {
    YYSYS_LOG(WARN, "stack pointer is NULL.");
    err = OB_INVALID_ARGUMENT;
  }
  else if (idx_i < 2)
  {
    YYSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    stack_i[idx_i-2].eq(stack_i[idx_i-1], result);
    if (result.is_null())
    {
      ObObjType this_type = stack_i[idx_i-2].get_type();
      ObObjType other_type = stack_i[idx_i-1].get_type();
      if (this_type == ObNullType || other_type == ObNullType)
      {
        int cmp = this_type - other_type;
        result.set_bool(cmp == 0);
      }
      else
      {
        YYSYS_LOG(WARN, "unexpected branch, this_type=%d other_type=%d", this_type, other_type);
        err = OB_ERR_UNEXPECTED;
      }
    }
    idx_i -= 2;
  }
  return err;
}

static int neq_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
{
  int err = OB_SUCCESS;
  if (NULL == stack_i)
  {
    YYSYS_LOG(WARN, "stack pointer is NULL.");
    err = OB_INVALID_ARGUMENT;
  }
  else if (idx_i < 2)
  {
    YYSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    stack_i[idx_i-2].ne(stack_i[idx_i-1], result);
    if (result.is_null())
    {
      ObObjType this_type = stack_i[idx_i-2].get_type();
      ObObjType other_type = stack_i[idx_i-1].get_type();
      if (this_type == ObNullType || other_type == ObNullType)
      {
        int cmp = this_type - other_type;
        result.set_bool(cmp != 0);
      }
      else
      {
        YYSYS_LOG(WARN, "unexpected branch, this_type=%d other_type=%d", this_type, other_type);
        err = OB_ERR_UNEXPECTED;
      }
    }
    idx_i -= 2;
  }
  return err;
}


static int is_not_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
{
  int err = OB_SUCCESS;
  if (NULL == stack_i)
  {
    YYSYS_LOG(WARN, "stack pointer is NULL.");
    err = OB_INVALID_ARGUMENT;
  }
  else if (idx_i < 2)
  {
    YYSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    stack_i[idx_i-2].is_not(stack_i[idx_i-1], result);
    idx_i -= 2;
  }
  return err;
}

static int is_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
{
  int err = OB_SUCCESS;
  if (NULL == stack_i)
  {
    YYSYS_LOG(WARN, "stack pointer is NULL.");
    err = OB_INVALID_ARGUMENT;
  }
  else if (idx_i < 2)
  {
    YYSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    stack_i[idx_i-2].is(stack_i[idx_i-1], result);
    idx_i -= 2;
  }
  return err;
}


static int add_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
{
  int err = OB_SUCCESS;
  if (NULL == stack_i)
  {
    YYSYS_LOG(WARN, "stack pointer is NULL.");
    err = OB_INVALID_ARGUMENT;
  }
  else if (idx_i < 2)
  {
    YYSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    stack_i[idx_i-2].add(stack_i[idx_i-1], result);
    idx_i -= 2;
  }
  return err;
}


static int sub_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
{
  int err = OB_SUCCESS;
  if (NULL == stack_i)
  {
    YYSYS_LOG(WARN, "stack pointer is NULL.");
    err = OB_INVALID_ARGUMENT;
  }
  else if (idx_i < 2)
  {
    YYSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    stack_i[idx_i-2].sub(stack_i[idx_i-1], result);
    idx_i -= 2;
  }
  return err;
}


static int mul_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
{
  int err = OB_SUCCESS;
  if (NULL == stack_i)
  {
    YYSYS_LOG(WARN, "stack pointer is NULL.");
    err = OB_INVALID_ARGUMENT;
  }
  else if (idx_i < 2)
  {
    YYSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    stack_i[idx_i-2].mul(stack_i[idx_i-1], result);
    idx_i -= 2;
  }
  return err;
}


static int div_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
{
  int err = OB_SUCCESS;
  if (NULL == stack_i)
  {
    YYSYS_LOG(WARN, "stack pointer is NULL.");
    err = OB_INVALID_ARGUMENT;
  }
  else if (idx_i < 2)
  {
    YYSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    stack_i[idx_i-2].div(stack_i[idx_i-1], result, true);
    idx_i -= 2;
  }
  return err;
}

static int mod_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
{
  int err = OB_SUCCESS;
  if (NULL == stack_i)
  {
    YYSYS_LOG(WARN, "stack pointer is NULL.");
    err = OB_INVALID_ARGUMENT;
  }
  else if (idx_i < 2)
  {
    YYSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    stack_i[idx_i-2].mod(stack_i[idx_i-1], result);
    idx_i -= 2;
  }
  return err;
}


static int and_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
{
  int err = OB_SUCCESS;
  if (NULL == stack_i)
  {
    YYSYS_LOG(WARN, "stack pointer is NULL.");
    err = OB_INVALID_ARGUMENT;
  }
  else if (idx_i < 2)
  {
    YYSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    stack_i[idx_i-2].land(stack_i[idx_i-1], result);
    idx_i -= 2;
  }
  return err;
}

static int or_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
{
  int err = OB_SUCCESS;
  if (NULL == stack_i)
  {
    YYSYS_LOG(WARN, "stack pointer is NULL.");
    err = OB_INVALID_ARGUMENT;
  }
  else if (idx_i < 2)
  {
    YYSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    stack_i[idx_i-2].lor(stack_i[idx_i-1], result);
    idx_i -= 2;
  }
  return err;
}

static int minus_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
{
  int err = OB_SUCCESS;

  if (NULL == stack_i)
  {
    YYSYS_LOG(WARN, "stack pointer is NULL.");
    err = OB_INVALID_ARGUMENT;
  }
  else if (idx_i < 1)
  {
    YYSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    stack_i[idx_i-1].negate(result);
    idx_i -= 1;
  }
  return err;
}


static int plus_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
{
  int err = OB_SUCCESS;

  if (NULL == stack_i)
  {
    YYSYS_LOG(WARN, "stack pointer is NULL.");
    err = OB_INVALID_ARGUMENT;
  }
  else if (idx_i < 1)
  {
    YYSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    // don't touch it whatever the type is
    result = stack_i[idx_i-1];
    idx_i -= 1;
  }
  return err;
}



static int not_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
{
  int err = OB_SUCCESS;

  if (NULL == stack_i)
  {
    YYSYS_LOG(WARN, "stack pointer is NULL.");
    err = OB_INVALID_ARGUMENT;
  }
  else if (idx_i < 1)
  {
    YYSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    stack_i[idx_i-1].lnot(result);
    idx_i -= 1;
  }
  return err;
}

static int like_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
{
  int err = OB_SUCCESS;

  if (NULL == stack_i)
  {
    YYSYS_LOG(WARN, "stack pointer is NULL.");
    err = OB_INVALID_ARGUMENT;
  }
  else if (idx_i < 2)
  {
    YYSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
    err = OB_INVALID_ARGUMENT;
  }
  else
  {
    err = stack_i[idx_i-2].old_like(stack_i[idx_i-1], result);
  }
  idx_i -= 2;
  return err;
}



/*     ��ʼ����ѧ����������ñ� */
op_call_func_t ObHashExpression::call_func[MAX_FUNC] = {
  /*   WARNING: �����˳�򲻿��Ե�����
   *   ��Ҫ��ob_postfix_expression.h�е�enum�����Ӧ
   */
  add_func,
  sub_func,
  mul_func,
  div_func,
  mod_func,
  and_func,
  or_func,
  not_func,
  lt_func,
  le_func,
  eq_func,
  neq_func,
  ge_func,
  gt_func,
  like_func,
  is_func,
  is_not_func,
  minus_func,
  plus_func
};
//mod wuna [MultiUps][sql_api] 20151217:b
//int ObHashExpression::set_expression(const ObString &expr)
int ObHashExpression::set_expression(const ObString &expr,ObArray<ObString> &group_name_list,
                                     ObCellArray& cell_array,bool is_for_check_expr)
{
  UNUSED(group_name_list);//add wuna [MultiUps][sql_api] 20151217
  UNUSED(cell_array);//add wuna [MultiUps][sql_api] 20151217
  UNUSED(is_for_check_expr);
  int ret = OB_SUCCESS;
  char origin_join_mod_expr[OB_MAX_VARCHAR_LENGTH];
  int32_t expr_len = static_cast<int32_t>(expr.length());
  int32_t hash_mod_expr_len = static_cast<int32_t>(strlen(hash_mod_expr_));
  memset(origin_join_mod_expr,0,OB_MAX_VARCHAR_LENGTH);
  memcpy(origin_join_mod_expr,"(",1);
  memcpy(origin_join_mod_expr+1,expr.ptr(),expr_len);
  memcpy(origin_join_mod_expr+expr_len+1,")",1);
  //modify for [540-hash partition cal]-b
  //  memcpy(origin_join_mod_expr+expr_len+2,hash_mod_expr_,hash_mod_expr_len);
  memcpy(origin_join_mod_expr+expr_len + 2, "%", 1);
  memcpy(origin_join_mod_expr+expr_len + 3, hash_mod_expr_, hash_mod_expr_len);
  //modify for [540-hash partition cal]-e

  ObString final_expr;
  final_expr.assign(origin_join_mod_expr,static_cast<int32_t>(strlen(origin_join_mod_expr)));
  //mod 20151217:e
  ObExpressionParser parser;
  ObArrayHelper<ObObj> expr_array;
  expr_array.init(OB_MAX_COMPOSITE_SYMBOL_COUNT, expr_);
  ret = parser.parse(final_expr, expr_array);

  if(OB_SUCCESS == ret)
  {
    postfix_size_ = static_cast<int32_t>(expr_array.get_array_index());
  }
  else
  {
    YYSYS_LOG(WARN, "parse infix expression to postfix expression error");
  }
  return ret;
}
//add wuna[MultiUps][sql_api] 20160114:b
int ObHashExpression::build_param_to_idx_map(
    const ObArray<ObString> &rule_param_list,
    hash::ObHashMap<ObString, int64_t,hash::NoPthreadDefendMode> &param_to_idx) const
{
  int ret = OB_SUCCESS;
  int64_t count = rule_param_list.count();
  param_to_idx.clear();
  int hash_ret = OB_SUCCESS;
  for(int64_t i=0;i < count && OB_SUCCESS == ret;i++)
  {
    const ObString &param = rule_param_list.at(i);
    if(hash::HASH_INSERT_SUCC != (hash_ret = param_to_idx.set(param, i)))
    {
      ret = OB_ERROR;
      YYSYS_LOG(WARN, "failed to set parameters to index");
      break;
    }
  }
  return ret;
}

int ObHashExpression::is_valid_hash_operator(int64_t value)
{
  int ret = OB_SUCCESS;
  switch(value)
  {
    case ADD_FUNC:
      break;
    case SUB_FUNC:
      break;
    case MUL_FUNC:
      break;
    case DIV_FUNC:
      break;
    case MOD_FUNC:
      break;
    case MINUS_FUNC:
      break;
    case PLUS_FUNC:
      break;
    default:
      ret = OB_ERROR;
      YYSYS_LOG(ERROR,"invalid hash operator,is %ld.",value);
      break;
  }
  return ret;
}

//add 20160114:e

int ObHashExpression::calc(
    const ObCellArray &cell_array,
    ExpressionType part_type,//add wuna [MultiUps][sql_api] 20151217
    ObArray<ObString>& rule_param_list,
    ObObj &result)
{
  OB_ASSERT(part_type == ObCalcExpression::HASH);//add wuna [MultiUps][sql_api] 20151217
  int ret = OB_SUCCESS;
  int64_t type = 0;
  int hash_ret = 0;
  int64_t val_index = 0;
  ObString key_col_name;

  int64_t value = 0;
  int idx = 0;
  int idx_i = 0;
  ObExprObj temp;
  //add wuna[MultiUps][sql_api] 20160114:b
  hash::ObHashMap<ObString,int64_t,hash::NoPthreadDefendMode> param_to_idx;
  if(OB_SUCCESS != (ret = param_to_idx.create(hash::cal_next_prime(512))))
  {
    YYSYS_LOG(WARN, "initialize hash map failed, ret=%d", ret);
  }
  else if(OB_SUCCESS != (ret = build_param_to_idx_map(rule_param_list, param_to_idx)))
  {
    YYSYS_LOG(WARN, "build parameter to rowkey order failed, ret=%d", ret);
  }
  if(OB_SUCCESS == ret)
  {
    //add 20160114:e
    do
    {
      // YYSYS_LOG(DEBUG, "idx=%d, idx_i=%d, type=%d\n", idx, idx_i, type);
      // �����������:��id�����֡����������������
      expr_[idx++].get_int(type);
      // expr_����END���ű�ʾ����
      if (END == type)
      {
        //modify for [540-hash partition cal]-b
        ObObj res;
        stack_i_[--idx_i].to(res);
        int64_t res_value = 0;
        res.get_int(res_value);

        result.set_int(labs(res_value));
        //modify for [540-hash partition cal]-b

        if (idx_i != 0)
        {
          YYSYS_LOG(WARN,"calculation stack must be empty. check the code for bugs. idx_i=%d", idx_i);
          ret = OB_ERROR;
        }
        break;
      }
      else if(type <= BEGIN_TYPE || type >= END_TYPE)
      {
        YYSYS_LOG(WARN,"unsupported operand type [type:%ld]", value);
        ret = OB_INVALID_ARGUMENT;
        break;
      }

      if (idx_i < 0 || idx_i >= OB_MAX_COMPOSITE_SYMBOL_COUNT || idx > postfix_size_)
      {
        YYSYS_LOG(WARN,"calculation stack overflow [stack.index:%d] "
                  "or run out of operand [operand.used:%d,operand.avaliable:%d]", idx_i, idx, postfix_size_);
        ret = OB_ERR_UNEXPECTED;
        break;
      }
      if (OB_SUCCESS == ret)
      {
        switch(type)
        {
          case COLUMN_IDX:
            if (OB_SUCCESS  != (ret = expr_[idx++].get_varchar(key_col_name)))
            {
              YYSYS_LOG(WARN,"get_varchar error [ret:%d]", ret);
            }
            else
            {
              hash_ret = param_to_idx.get(key_col_name, val_index);
              if (hash::HASH_EXIST != hash_ret)
              {
                YYSYS_LOG(WARN,"get key(%.*s) failed", key_col_name.length(), key_col_name.ptr());
                ret = OB_ERROR;
                break;
              }
              else
              {
                /* decode. quite simple, isn't it. */
                stack_i_[idx_i++].assign(cell_array[val_index].value_);
                YYSYS_LOG(DEBUG, "succ decode.  key(%.*s) -> %ld",
                          key_col_name.length(), key_col_name.ptr(), val_index);
              }
            }
            break;
          case CONST_OBJ:
            stack_i_[idx_i++].assign(expr_[idx++]);
            break;
          case OP:
            // ����OP�����ͣ��Ӷ�ջ�е���1�����������������м���
            if(OB_SUCCESS != (ret = expr_[idx++].get_int(value)))
            {
              YYSYS_LOG(WARN,"get_int error [ret:%d]", ret);
            }
            else if (value <= MIN_FUNC || value >= MAX_FUNC)
            {
              YYSYS_LOG(WARN,"unsupported operator type [type:%ld]", value);
              ret = OB_INVALID_ARGUMENT;
            }
            else if(OB_SUCCESS != (ret = is_valid_hash_operator(value)))
            {
              YYSYS_LOG(WARN,"unsupported operator type [type:%ld]", value);
              ret = OB_INVALID_ARGUMENT;
            }
            else
            {
              if (OB_SUCCESS != (ret = (this->call_func[value])(stack_i_, idx_i, temp)))
              {
                YYSYS_LOG(WARN,"call calculation function error [value:%ld, idx_i:%d, ret:%d]",value, idx_i, ret);
              }
              else
              {
                stack_i_[idx_i++] = temp;
              }
            }
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            YYSYS_LOG(WARN,"unexpected [type:%ld]", type);
            break;
        }
      }
      if (OB_SUCCESS != ret)
      {
        break;
      }
    }while(true);
  }
  param_to_idx.destroy();//add wuna[MultiUps][sql_api] 20160114
  return ret;
}
