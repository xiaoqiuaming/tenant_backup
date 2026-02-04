/*
 * (C) 2007-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_postfix_expression.cpp is for what ...
 *
 * Version: $id: ob_postfix_expression.cpp, v 0.1 7/29/2011 14:39 xiaochu Exp $
 *
 * Authors:
 *   xiaochu <xiaochu.yh@taobao.com>
 *     - some work details if you want
 *
 */


#include "ob_postfix_expression.h"
#include "ob_cell_array.h"
#include "ob_schema.h"
using namespace oceanbase::common::ObExpression;

namespace oceanbase
{
  namespace common
  {
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
        //mod peiouya [DATE_TIME] 20150929:b
        //stack_i[idx_i-2].gt(stack_i[idx_i-1], result);
        //if (result.is_null())
        err = stack_i[idx_i-2].gt(stack_i[idx_i-1], result);
        if (err == OB_SUCCESS && result.is_null())
          //mod 20150929:e
        {
          ObObjType this_type = stack_i[idx_i-2].get_type();
          ObObjType other_type = stack_i[idx_i-1].get_type();
          if (this_type == ObNullType || other_type == ObNullType)
          {
            //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
            //int cmp = this_type - other_type;
            int cmp = other_type - this_type;
            //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e
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
        //mod peiouya [DATE_TIME] 20150929:b
        //stack_i[idx_i-2].ge(stack_i[idx_i-1], result);
        //if (result.is_null())
        err = stack_i[idx_i-2].ge(stack_i[idx_i-1], result);
        if (err == OB_SUCCESS && result.is_null())
          //mod 20150929:e
        {
          ObObjType this_type = stack_i[idx_i-2].get_type();
          ObObjType other_type = stack_i[idx_i-1].get_type();
          if (this_type == ObNullType || other_type == ObNullType)
          {
            //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
            //int cmp = this_type - other_type;
            int cmp = other_type - this_type;
            //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e
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
        //mod peiouya [DATE_TIME] 20150929:b
        //stack_i[idx_i-2].le(stack_i[idx_i-1], result);
        //if (result.is_null())
        err = stack_i[idx_i-2].le(stack_i[idx_i-1], result);
        if (err == OB_SUCCESS && result.is_null())
          //mod 20150929:e
        {
          ObObjType this_type = stack_i[idx_i-2].get_type();
          ObObjType other_type = stack_i[idx_i-1].get_type();
          if (this_type == ObNullType || other_type == ObNullType)
          {
            //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
            //int cmp = this_type - other_type;
            int cmp = other_type - this_type;
            //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e
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
        //mod peiouya [DATE_TIME] 20150929:b  //[conflict_level B]
        // stack_i[idx_i-2].lt(stack_i[idx_i-1], result);
        // if (result.is_null())
        err = stack_i[idx_i-2].lt(stack_i[idx_i-1], result);
        if (err == OB_SUCCESS && result.is_null())
          //mod 20150929:e
        {
          ObObjType this_type = stack_i[idx_i-2].get_type();
          ObObjType other_type = stack_i[idx_i-1].get_type();
          if (this_type == ObNullType || other_type == ObNullType)
          {
            //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
            //int cmp = this_type - other_type;
            int cmp = other_type - this_type;
            //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e
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
        //mod peiouya [DATE_TIME] 20150929:b
        //stack_i[idx_i-2].eq(stack_i[idx_i-1], result);
        //if (result.is_null())
        err = stack_i[idx_i-2].eq(stack_i[idx_i-1], result);
        if (err == OB_SUCCESS && result.is_null())
          //mod 20150929:e
        {
          ObObjType this_type = stack_i[idx_i-2].get_type();
          ObObjType other_type = stack_i[idx_i-1].get_type();
          if (this_type == ObNullType || other_type == ObNullType)
          {
            //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
            //int cmp = this_type - other_type;
            int cmp = other_type - this_type;
            //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e
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
        //mod peiouya [DATE_TIME] 20150929:b
        // stack_i[idx_i-2].ne(stack_i[idx_i-1], result);
        // if (result.is_null())
        err = stack_i[idx_i-2].ne(stack_i[idx_i-1], result);
        if (err == OB_SUCCESS && result.is_null())
          //mod 20150929:e
        {
          ObObjType this_type = stack_i[idx_i-2].get_type();
          ObObjType other_type = stack_i[idx_i-1].get_type();
          if (this_type == ObNullType || other_type == ObNullType)
          {
            //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
            //int cmp = this_type - other_type;
            int cmp = other_type - this_type;
            //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e
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
        //mod peiouya [DATE_TIME] 20150906:b
        //stack_i[idx_i-2].add(stack_i[idx_i-1], result);
        err = stack_i[idx_i-2].add(stack_i[idx_i-1], result);
        //mod 20150906:e
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
        //mod peiouya [DATE_TIME] 20150906:b
        //stack_i[idx_i-2].sub(stack_i[idx_i-1], result);
        err = stack_i[idx_i-2].sub(stack_i[idx_i-1], result);
        //mod 20150906:e
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
        //mod peiouya [DATE_TIME] 20150906:b
        //stack_i[idx_i-2].mul(stack_i[idx_i-1], result);
        err = stack_i[idx_i-2].mul(stack_i[idx_i-1], result);
        //mod 20150906:e
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
        //mod peiouya [DATE_TIME] 20150906:b
        //stack_i[idx_i-2].div(stack_i[idx_i-1], result, true);
        err = stack_i[idx_i-2].div(stack_i[idx_i-1], result, true);
        //mod 20150906:e
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
        //mod peiouya [DATE_TIME] 20150906:b
        //stack_i[idx_i-2].mod(stack_i[idx_i-1], result);
        err = stack_i[idx_i-2].mod(stack_i[idx_i-1], result);
        //mod 20150906:e
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

    //add  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
    static int cast_thin_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result)
    {
      int err = OB_SUCCESS;
      int64_t dest_type = 0;
      if (OB_UNLIKELY(NULL == stack_i))
      {
        YYSYS_LOG(WARN, "stack_i=%p.", stack_i);
        err = OB_INVALID_ARGUMENT;
      }
      else if (OB_UNLIKELY(idx_i < 2))
      {
        YYSYS_LOG(WARN, "no enough operand in the stack. current size:%d", idx_i);
        err = OB_INVALID_ARGUMENT;
      }
      else if (OB_SUCCESS != (err = stack_i[idx_i-1].get_int(dest_type)))
      {
        YYSYS_LOG(WARN, "fail to get int value. actual type = %d. err=%d", stack_i[idx_i-1].get_type(), err);
      }
      else if (ObIntType  != dest_type)
      {
        err = OB_NOT_SUPPORTED;
        YYSYS_LOG(ERROR,"cast_thin_func only support cast to ObIntType. but cur cast to type:%ld",dest_type);
      }
      else // if (ObIntType  == dest_type)
      {
        ObStringBuf tmp_str_buf;  //only for input-para, actually not to use

        if(ObVarcharType == stack_i[idx_i-2].get_type ())
        {
          err = stack_i[idx_i-2].cast_to(ObPreciseDateTimeType, result, tmp_str_buf);
        }

        if(OB_SUCCESS == err && ObVarcharType == stack_i[idx_i-2].get_type ())
        {
          result.set_int (result.get_precise_datetime ());
        }
        else if (OB_SUCCESS != (err = stack_i[idx_i-2].cast_to(ObIntType, result, tmp_str_buf)))
        {
          YYSYS_LOG(DEBUG,"fail to cast type:%d--->ObIntType, but we ignore it, along with set result with null!",
                    stack_i[idx_i-2].get_type ());
          result.set_null ();
          err = OB_SUCCESS;
        }
        else
        {
          //nothing todo
        }
        idx_i -= 2;
      }

      return err;
    }
    //add  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e

    /*     ��ʼ����ѧ����������ñ� */
    op_call_func_t ObPostfixExpression::call_func[MAX_FUNC] = {
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
      plus_func,
      cast_thin_func  //add  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608
    };

    ObPostfixExpression& ObPostfixExpression::operator=(const ObPostfixExpression &other)
    {
      int i = 0;
      if (&other != this)
      {
        // BUG: not deep copy implementation. WATCH OUT!
        for (i = 0; i < other.postfix_size_; i++)
        {
          expr_[i] = other.expr_[i];
        }
        postfix_size_ = other.postfix_size_;
      }
      return *this;
    }

    int ObPostfixExpression::set_expression(const ObObj *expr, oceanbase::common::ObStringBuf  & data_buf)
    {
      int err = OB_SUCCESS;
      int i = 0;
      postfix_size_ = 0;  // reset postfix size
      int64_t type = 0;

      if (NULL != expr)
      {
        while ((i < OB_MAX_COMPOSITE_SYMBOL_COUNT) && (OB_SUCCESS == err))
        {
          expr_[i] = expr[i];
          postfix_size_++;
          if (ObIntType != expr[i].get_type())
          {
            YYSYS_LOG(WARN, "unexpected postfix expression header element type. ObIntType expected"
                      "[type:%d]", expr[i].get_type());
            break;
          }
          else if (OB_SUCCESS != (err = expr[i].get_int(type)))
          {
            YYSYS_LOG(WARN, "get int value failed:err[%d]", err);
            break;
          }
          else if (type == END)
          {
            i++;
            break;
          }
          else
          {
            /*FIXME: �����������û��END������־�����ܵ��·ô���� */
            if( expr[i + 1].need_deep_copy())
            {
              if(OB_SUCCESS != (err = data_buf.write_obj(expr[i+1], expr_ + i + 1)))
              {
                YYSYS_LOG(WARN,"fail to copy obj [err:%d]", err);
              }
            }
            else
            {
              expr_[i+1] = expr[i+1];
            }
            if(OB_SUCCESS == err)
            {
              postfix_size_++;
              i += 2;
            }
          }
        }
      }
      else
      {
        err = OB_INVALID_ARGUMENT;
        YYSYS_LOG(WARN, "postfix expression is null!");
      }

      if (OB_SUCCESS == err && OB_MAX_COMPOSITE_SYMBOL_COUNT <= i)
      {
        err = OB_INVALID_ARGUMENT;
        YYSYS_LOG(WARN, "END symbol not found postfix expression. [i=%d]", i);
      }
      return err;
    }

    int ObPostfixExpression::set_expression(const ObString &expr, const ObScanParam &scan_param)
    {
      int err = OB_SUCCESS;

      UNUSED(scan_param);
      UNUSED(expr);
#if 0
      ObArrayHelper<ObExprObj> expr_array;
      expr_array.init(OB_MAX_COMPOSITE_SYMBOL_COUNT, expr_);
      err = parser_.parse(expr, expr_array);
      YYSYS_LOG(DEBUG, "parse done. expr_array len= %d", expr_array.get_array_index());

      {
        int i = 0;
        for (i = 0; i < expr_array.get_array_index(); i++)
        {
          expr_array.at(i)->dump();
        }
      }

      if (OB_SUCCESS == err)
      {
        postfix_size_ = expr_array.get_array_index();
      }
      else
      {
        YYSYS_LOG(WARN, "parse infix expression to postfix expression error");
      }

      if (OB_SUCCESS == err)
      {
        // TODO: decode expr_array using scan_param
        // �����㷨���£�
        //  1. ����expr_array���飬����������ΪCOLUMN_IDX��Obj����
        //  2. ��obj�б�ʾ������ֵkey_col_name��������hashmap�в��Ҷ�Ӧindex
        //      scan_param.some_hash_map(key_col_name, val_index)
        //  3. ����ҵ�����obj���������Ϊval_index
        //     ����Ҳ������򱨴�����
        //
      }
#endif
      return err;
    }

    //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
    //int ObPostfixExpression::set_expression(const ObString &expr,
    //    const hash::ObHashMap<ObString,int64_t,hash::NoPthreadDefendMode> & cname_to_idx_map,
    //    ObExpressionParser & parser, common::ObResultCode *rc)
    int ObPostfixExpression::set_expression(const ObString&                                                         expr,
                                            const hash::ObHashMap<ObString,int64_t,hash::NoPthreadDefendMode> &     cname_to_idx_map,
                                            ObExpressionParser &                                                    parser,
                                            common::ObResultCode                                                    *rc,
                                            bool                                                                    is_expire_info)
    //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e
    {
      int err = OB_SUCCESS;
      int hash_ret = 0;
      int i = 0;
      int64_t type = 0;
      int64_t val_index = 0;
      ObString key_col_name;

      ObArrayHelper<ObObj> expr_array;
      expr_array.init(OB_MAX_COMPOSITE_SYMBOL_COUNT, expr_);
      //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
      //err = parser.parse(expr, expr_array);
      err = parser.parse(expr, expr_array, is_expire_info);
      //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e
      YYSYS_LOG(DEBUG, "parse done. expr_array len= %ld", expr_array.get_array_index());

      if(0)
      {
        for (i = 0; i < expr_array.get_array_index(); i++)
        {
          expr_array.at(i)->dump();
        }
      }

      if (OB_SUCCESS == err)
      {
        postfix_size_ = static_cast<int32_t>(expr_array.get_array_index());
      }
      else
      {
        YYSYS_LOG(WARN, "parse infix expression to postfix expression error");
      }

      if (OB_SUCCESS == err)
      {
        // TODO: decode expr_array using scan_param
        // �����㷨���£�
        //  1. ����expr_array���飬����������ΪCOLUMN_IDX��Obj����
        //  2. ��obj�б�ʾ������ֵkey_col_name��������hashmap�в��Ҷ�Ӧindex
        //      scan_param.some_hash_map(key_col_name, val_index)
        //  3. ����ҵ�����obj���������Ϊval_index
        //     ����Ҳ������򱨴�����

        i = 0;
        while(i < postfix_size_ - 1)
        {
          if (OB_SUCCESS != expr_array.at(i)->get_int(type))
          {
            YYSYS_LOG(WARN, "unexpected data type. int expected, but actual type is %d",
                      expr_array.at(i)->get_type());
            err = OB_ERR_UNEXPECTED;
            break;
          }
          else
          {
            if (ObExpression::COLUMN_IDX == type)
            {
              if (OB_SUCCESS != expr_array.at(i+1)->get_varchar(key_col_name))
              {
                YYSYS_LOG(WARN, "unexpected data type. varchar expected, but actual type is %d",
                          expr_array.at(i+1)->get_type());
                err = OB_ERR_UNEXPECTED;
                break;
              }
              else
              {
                hash_ret = cname_to_idx_map.get(key_col_name,val_index);
                if (hash::HASH_EXIST != hash_ret)
                {
                  YYSYS_LOG(WARN,"get key(%.*s) failed", key_col_name.length(), key_col_name.ptr());
                  if(NULL != rc)
                  {
                    snprintf(rc->message_.ptr(), rc->message_.length(), "column name not declared before using [column_name:%.*s]",
                             key_col_name.length(), key_col_name.ptr());
                  }
                  err = OB_ERROR;
                  break;
                }
                else
                {
                  /* decode. quite simple, isn't it. */
                  expr_array.at(i+1)->set_int(val_index);
                  YYSYS_LOG(DEBUG, "succ decode.  key(%.*s) -> %ld",
                            key_col_name.length(), key_col_name.ptr(), val_index);
                }
              }
            }/* only column name needs to decode. other type is ignored */
            i += 2; /// skip <type, data> (2 objects as an element)
          }
        }
      }
      return err;
    }

    int ObExpressionParser::parse(const ObString &infix_string, ObArrayHelper<ObObj> &postfix_objs, bool is_expire_info
                                  ,bool need_check, const ObSchemaManagerV2 *tmp_schema_mgr, const char *table_name)
    //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e
    {
      reset();
      is_expire_info_ = is_expire_info;  //add  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608

      int err = OB_SUCCESS;
      int pos = 0;  // char position in infix_str
      const char *infix_str = infix_string.ptr();
      infix_max_len_ = infix_string.length();
      result_ = &postfix_objs;
      ObString key_col_name;
      ObString name;
      const ObColumnSchemaV2 *ColumnSchema = NULL;
      //add 20140909:e

      // �ִ�
      while(pos < infix_max_len_ && symbols_.get_array_index() < symbols_.get_array_size() && OB_SUCCESS == err)
      {
        Symbol sym;
        if(OB_SUCCESS != (err = get_sym(infix_str, pos, sym)))
        {
          if (OB_ITER_END != err)
          {
            YYSYS_LOG(WARN, "get sym err");
          }
          else //if (err == OB_ITER_END)
          {
            err = OB_SUCCESS;
            break;
          }
        }
        else
        {
          if (symbols_.push_back(sym))
          {
            err = OB_SUCCESS;
            YYSYS_LOG(DEBUG, "sym %ld=[type:%d][%.*s]",
                      symbols_.get_array_index(),
                      sym.type,
                      sym.length,
                      sym.value);
          }
          else
          {
            YYSYS_LOG(WARN, "error adding symbol to symbol list");
            err = OB_ERROR;
          }
        }
      }
      //add for [507-expire info enhancement]-b
      //**************************************1.encode********************//
      // ����
      // (1) �ַ���ת����
      // (2) ������
      int i = 0;
      const Symbol *prev_symbol = NULL;
      for (i = 0; i < symbols_.get_array_index() && OB_SUCCESS == err; i++)
      {
        prev_symbol = (i == 0) ? NULL : symbols_.at(i - 1);
        if (OB_SUCCESS != (err = encode_sym(*symbols_.at(i), prev_symbol)))
        {
          YYSYS_LOG(WARN, "encode symbol failed. sym %d =[type:%d][%.*s]", i,
                    symbols_.at(i)->type,
                    symbols_.at(i)->length,
                    symbols_.at(i)->value);
          break;
        }
        else
        {
          YYSYS_LOG(DEBUG, "encode symbol OK. sym %d =[type:%d][%.*s]", i,
                    symbols_.at(i)->type,
                    symbols_.at(i)->length,
                    symbols_.at(i)->value);
        }
      }

      //****************2.expire_info�ؼ���ֻ֧��AND �� OR �� NULL********************//
      int size = 0;
      const Symbol *symbol = NULL;
      const Symbol *column_symbol = NULL;
      if (OB_SUCCESS == err && need_check)
      {
        for (size = 0; size < symbols_.get_array_index() && OB_SUCCESS == err; size++)
        {
          symbol = symbols_.at(size);
          switch(symbol->type)
          {
            case OPERATOR:
            case COLUMN_NAME:
            case DATETIME:
            case NUMBER:
            case HEX_STRING:
            case HEX_NUMBER:
            case STRING:
              err = OB_SUCCESS;
              break;
            case KEYWORD:
              if ((strlen("AND") == (size_t)symbol->length && 0 == strncasecmp("AND", symbol->value, symbol->length))
                  || (strlen("OR") == (size_t)symbol->length && 0 == strncasecmp("OR", symbol->value, symbol->length))
                  || (strlen("NULL") == (size_t)symbol->length && 0 == strncasecmp("NULL", symbol->value, symbol->length)))
              {
                err = OB_SUCCESS;
              }
              else
              {
                err = OB_ERROR;
                YYSYS_LOG(ERROR, "expire_info key word can only include [AND|OR|NULL]!, but sym %d =[type:%d][%.*s]", size,
                          symbol->type,
                          symbol->length,
                          symbol->value);
              }
              break;
            default:
              err = OB_INVALID_ARGUMENT;
              YYSYS_LOG(ERROR, "invalid expire_info, include invalid param! sym %d =[type:%d][%.*s]", size,
                        symbol->type,
                        symbol->length,
                        symbol->value);
              break;
          }
        }
      }
      //****************3.����ʽ���������Ƿ���ȷ********************//
      if (OB_SUCCESS == err && is_expire_info)
      {
        for (size = 0; size < symbols_.get_array_index() && OB_SUCCESS == err; size++)
        {
          symbol = symbols_.at(size);
          // ֧��ʱ���ʽ$SYS_DATE>c4+relative_time��ʽ | c4=#YYYY-MM-DD# | $SYS_DAY>c4+relative_time
          // ʱ��Ƚ��漰����֧��ObVarchar | ObModifyTimeType | ObCreateTimeType | ObPreciseDateTimeType
          // ����֧�֣�[<|<=|>|>=|=|!=]
          if (DATETIME == symbol->type && OB_SUCCESS == err)
          {
            if((size - 2) >= 0 && (symbols_.at(size - 1)->type == OPERATOR) && (symbols_.at(size - 2)->type == COLUMN_NAME))
            {
              if (OB_SUCCESS == (err = check_sys_day(symbols_.at(size - 1)->code)))
              {
                symbols_.at(size - 2)->need_complete_for_expire_info = true;
                symbols_.at(size)->need_complete_for_expire_info = true;
                if (need_check)
                {
                  column_symbol = symbols_.at(size - 2);
                  if (OB_SUCCESS != column_symbol->encode.get_varchar(key_col_name))
                  {
                    YYSYS_LOG(WARN, "unexpected data type. varchar expected, "
                              "but actual type is %d",
                              symbol->encode.get_type());
                    err = OB_ERR_UNEXPECTED;
                    break;
                  }
                  else
                  {
                    name.assign_ptr(const_cast<char *>(table_name), static_cast<int32_t>(strlen(table_name)));
                    ColumnSchema = tmp_schema_mgr->get_column_schema(name, key_col_name);
                    if (NULL == ColumnSchema)
                    {
                      YYSYS_LOG(WARN, "expire condition includes invalid column name, ""table_name=%s, column_name=%.*s, expire_condition=%.*s",
                                table_name, key_col_name.length(), key_col_name.ptr(),
                                infix_string.length(), infix_string.ptr());
                      err = OB_INVALID_ARGUMENT;
                      break;
                    }
                    else
                    {
                      if (OB_SUCCESS != (err = check_time_compare_columnType(ColumnSchema->get_type())))
                      {
                        YYSYS_LOG(WARN, "expire condition includes invalid column name, ""table_name=%s, column_name=%.*s, expire_condition=%.*s",
                                  table_name, key_col_name.length(), key_col_name.ptr(),
                                  infix_string.length(), infix_string.ptr());
                        break;
                      }
                    }
                  }
                }
              }
              else
              {
                err = OB_INVALID_ARGUMENT;
                YYSYS_LOG(ERROR, "expire_info about time compare only support operator [<|<=|>|>=|=|!=]");
                break;
              }
            }
            else if ((size + 2) < symbols_.get_array_index() && (symbols_.at(size + 1)->type == OPERATOR) && (symbols_.at(size + 2)->type == COLUMN_NAME))
            {
              if (OB_SUCCESS == (err = check_sys_day(symbols_.at(size + 1)->code)))
              {
                symbols_.at(size)->need_complete_for_expire_info = true;
                symbols_.at(size + 2)->need_complete_for_expire_info = true;
                if (need_check)
                {
                  column_symbol = symbols_.at(size + 2);
                  if (OB_SUCCESS != column_symbol->encode.get_varchar(key_col_name))
                  {
                    YYSYS_LOG(WARN, "unexpected data type. varchar expected, "
                              "but actual type is %d",
                              symbol->encode.get_type());
                    err = OB_ERR_UNEXPECTED;
                    break;
                  }
                  else
                  {
                    name.assign_ptr(const_cast<char *>(table_name),
                                    static_cast<int32_t>(strlen(table_name)));
                    ColumnSchema = tmp_schema_mgr->get_column_schema(name, key_col_name);
                    if (NULL == ColumnSchema)
                    {
                      YYSYS_LOG(WARN, "expire condition includes invalid column name, ""table_name=%s, column_name=%.*s, expire_condition=%.*s",
                                table_name, key_col_name.length(), key_col_name.ptr(),
                                infix_string.length(), infix_string.ptr());
                      err = OB_INVALID_ARGUMENT;
                      break;
                    }
                    else
                    {
                      if (OB_SUCCESS != (err = check_time_compare_columnType(ColumnSchema->get_type())))
                      {
                        YYSYS_LOG(WARN, "expire condition includes invalid column name, ""table_name=%s, column_name=%.*s, expire_condition=%.*s",
                                  table_name, key_col_name.length(), key_col_name.ptr(),
                                  infix_string.length(), infix_string.ptr());
                        break;
                      }
                    }
                  }
                }
              }
              else
              {
                err = OB_INVALID_ARGUMENT;
                YYSYS_LOG(ERROR, "expire_info about time compare only support operator [<|<=|>|>=|=|!=]!");
                break;
              }
            }
            else
            {
              err = OB_EXPIRE_CONDITION_ERROR;
              YYSYS_LOG(ERROR, "invalid expire_info, please check the correct expire_info format!");
              break;
            }
          }
          //***********************�ַ����Ƚ��������ж�*************************//
          // ֧��ʱ���ʽ c4 = \'test\'
          // ʱ��Ƚ��漰����֧��ObVarcharType
          // ����֧�֣�[= | != |> | >= | <| <=]
          if (STRING == symbol->type && OB_SUCCESS == err && need_check)
          {
            if((size - 2) >= 0 && (symbols_.at(size - 1)->type == OPERATOR) && (symbols_.at(size - 2)->type == COLUMN_NAME))
            {
              if (OB_SUCCESS == (err = check_string_compare_operator_type(symbols_.at(size - 1)->code)))
              {
                column_symbol = symbols_.at(size - 2);
                if (OB_SUCCESS != column_symbol->encode.get_varchar(key_col_name))
                {
                  YYSYS_LOG(WARN, "unexpected data type. varchar expected, "
                            "but actual type is %d",
                            symbol->encode.get_type());
                  err = OB_ERR_UNEXPECTED;
                  break;
                }
                else
                {
                  name.assign_ptr(const_cast<char *>(table_name),
                                  static_cast<int32_t>(strlen(table_name)));
                  ColumnSchema = tmp_schema_mgr->get_column_schema(name, key_col_name);
                  if (NULL == ColumnSchema)
                  {
                    YYSYS_LOG(WARN, "expire condition includes invalid column name, ""table_name=%s, column_name=%.*s, expire_condition=%.*s",
                              table_name, key_col_name.length(), key_col_name.ptr(),
                              infix_string.length(), infix_string.ptr());
                    err = OB_INVALID_ARGUMENT;
                    break;
                  }
                  else
                  {
                    if (OB_SUCCESS != (err = check_string_compare_columnType(ColumnSchema->get_type())))
                    {
                      YYSYS_LOG(WARN, "expire condition includes invalid column name, ""table_name=%s, column_name=%.*s, expire_condition=%.*s",
                                table_name, key_col_name.length(), key_col_name.ptr(),
                                infix_string.length(), infix_string.ptr());
                      break;
                    }
                  }
                }
                //����ַ������治���ٸ����㣺������ʽ��: c4 = \'test\'+4
                if (err == OB_SUCCESS && (size + 1) < symbols_.get_array_index())
                {
                  const Symbol *tmp_symbol = NULL;
                  tmp_symbol = symbols_.at(size + 1);
                  switch(tmp_symbol->type)
                  {
                    case OPERATOR:
                      if ((strlen("AND") == (size_t)symbols_.at(size + 1)->length && 0 == strncasecmp("AND", symbols_.at(size + 1)->value, symbols_.at(size + 1)->length))
                          || (strlen("OR") == (size_t)symbols_.at(size + 1)->length && 0 == strncasecmp("OR", symbols_.at(size + 1)->value, symbols_.at(size + 1)->length))
                          )
                      {
                        err = OB_SUCCESS;
                      }
                      else
                      {
                        err = OB_ERROR;
                      }
                      break;
                    default:
                      err = OB_ERROR;
                      YYSYS_LOG(ERROR, "invalid expire_info, please check the correct expire_info format!");
                      break;
                  }
                }
              }
              else
              {
                err = OB_INVALID_ARGUMENT;
                YYSYS_LOG(ERROR, "expire_info about string compare only support operator [<|<=|>|>=|=|!=]!");
              }
            }
            // ��ʽ: \'test\' = c4
            else if ((size + 2) < symbols_.get_array_index() && (symbols_.at(size + 1)->type == OPERATOR) && (symbols_.at(size + 2)->type == COLUMN_NAME))
            {
              if (OB_SUCCESS == (err = check_string_compare_operator_type(symbols_.at(size + 1)->code)))
              {
                column_symbol = symbols_.at(size + 2);
                if (OB_SUCCESS != column_symbol->encode.get_varchar(key_col_name))
                {
                  YYSYS_LOG(WARN, "unexpected data type. varchar expected, "
                            "but actual type is %d",
                            symbol->encode.get_type());
                  err = OB_ERR_UNEXPECTED;
                  break;
                }
                else
                {
                  name.assign_ptr(const_cast<char *>(table_name),
                                  static_cast<int32_t>(strlen(table_name)));
                  ColumnSchema = tmp_schema_mgr->get_column_schema(name, key_col_name);
                  if (NULL == ColumnSchema)
                  {
                    YYSYS_LOG(WARN, "expire condition includes invalid column name, ""table_name=%s, column_name=%.*s, expire_condition=%.*s",
                              table_name, key_col_name.length(), key_col_name.ptr(),
                              infix_string.length(), infix_string.ptr());
                    err = OB_INVALID_ARGUMENT;
                    break;
                  }
                  else
                  {
                    if (OB_SUCCESS != (err = check_string_compare_columnType(ColumnSchema->get_type())))
                    {
                      YYSYS_LOG(WARN, "expire condition includes invalid column name, ""table_name=%s, column_name=%.*s, expire_condition=%.*s",
                                table_name, key_col_name.length(), key_col_name.ptr(),
                                infix_string.length(), infix_string.ptr());
                      break;
                    }
                  }
                }
              }
              else
              {
                err = OB_INVALID_ARGUMENT;
                YYSYS_LOG(ERROR, "expire_info about string compare only support operator [<|<=|>|>=|=|!=]!");
              }
            }
            else
            {
              err = OB_EXPIRE_CONDITION_ERROR;
              YYSYS_LOG(ERROR, "invalid expire_info, please check the correct expire_info format!");
              break;
            }
          }
        }
      }
      if(OB_SUCCESS == err && OB_SUCCESS == (err = infix2postfix()))
      {
        //YYSYS_LOG(INFO, "successful in converting infix expression to postfix expression");
      }
      else
      {
        YYSYS_LOG(WARN, "fail to convert infix expression to postfix expression");
      }
      return err;
    }


    //��org_cell�е�ֵ���뵽expr������
    int ObPostfixExpression::calc(ObObj &composite_val,
                                  const ObCellArray & org_cells,
                                  const int64_t org_row_beg,
                                  const int64_t org_row_end
                                  )
    {
      int err = OB_SUCCESS;
      int64_t type = 0, value = 0;
      int idx = 0;
      ObExprObj result;

      UNUSED(org_row_end);

      int idx_i = 0;

      do
      {
        // YYSYS_LOG(DEBUG, "idx=%d, idx_i=%d, type=%d\n", idx, idx_i, type);
        // �����������:��id�����֡����������������
        expr_[idx++].get_int(type);

        // expr_����END���ű�ʾ����
        if (END == type)
        {
          stack_i[--idx_i].to(composite_val); // assign result
          if (idx_i != 0)
          {
            YYSYS_LOG(WARN,"calculation stack must be empty. check the code for bugs. idx_i=%d", idx_i);
            err = OB_ERROR;
          }
          break;
        }
        else if(type <= BEGIN_TYPE || type >= END_TYPE)
        {
          YYSYS_LOG(WARN,"unsupported operand type [type:%ld]", value);
          err = OB_INVALID_ARGUMENT;
          break;
        }

        if (idx_i < 0 || idx_i >= OB_MAX_COMPOSITE_SYMBOL_COUNT || idx > postfix_size_)
        {
          YYSYS_LOG(WARN,"calculation stack overflow [stack.index:%d] "
                    "or run out of operand [operand.used:%d,operand.avaliable:%d]", idx_i, idx, postfix_size_);
          err = OB_ERR_UNEXPECTED;
          break;
        }
        if (OB_SUCCESS == err)
        {
          switch(type)
          {
            case COLUMN_IDX:
              if (OB_SUCCESS  != (err = expr_[idx++].get_int(value)))
              {
                YYSYS_LOG(WARN,"get_int error [err:%d]", err);
              }
              else if (org_row_beg + value > org_row_end)
              {
                YYSYS_LOG(WARN,"invalid row offset [org_row_beg:%ld, offset:%ld, org_row_end:%ld]",org_row_beg, value, org_row_end);
                err = OB_INVALID_ARGUMENT;
              }
              else
              {
                stack_i[idx_i++].assign(org_cells[org_row_beg + value].value_);
              }
              break;
            case CONST_OBJ:
              stack_i[idx_i++].assign(expr_[idx++]);
              break;
            case OP:
              // ����OP�����ͣ��Ӷ�ջ�е���1�����������������м���
              if(OB_SUCCESS != (err = expr_[idx++].get_int(value)))
              {
                YYSYS_LOG(WARN,"get_int error [err:%d]", err);
              }
              else if (value <= MIN_FUNC || value >= MAX_FUNC)
              {
                YYSYS_LOG(WARN,"unsupported operator type [type:%ld]", value);
                err = OB_INVALID_ARGUMENT;
              }
              else
              {
                if (OB_SUCCESS != (err = (this->call_func[value])(stack_i, idx_i, result)))
                {
                  YYSYS_LOG(WARN,"call calculation function error [value:%ld, idx_i:%d, err:%d]",value, idx_i, err);
                }
                else
                {
                  result.set_len(1);
                  if (const_cast<ObDecimal &>(result.get_decimal()).get_words()[0] > UINT64_MAX)
                  {
                    result.set_len(2);
                  }
                  stack_i[idx_i++] = result;
                }
              }
              break;
            default:
              err = OB_ERR_UNEXPECTED;
              YYSYS_LOG(WARN,"unexpected [type:%ld]", type);
              break;
          }
        }
        if (OB_SUCCESS != err)
        {
          break;
        }
      }while(true);

      return err;
    }

    DEFINE_SERIALIZE(ObPostfixExpression)
    {
      int i;
      int err = OB_SUCCESS;

      for (i = 0; i < postfix_size_; i++)
      {
        err = expr_[i].serialize(buf, buf_len, pos);
        if (OB_SUCCESS != err)
        {
          YYSYS_LOG(WARN, "serialization error. [pos:%ld, err:%d]", pos, err);
          break;
        }
      }
      return err;
    }

    DEFINE_DESERIALIZE(ObPostfixExpression)
    {
      ObObj obj;
      postfix_size_ = 0;
      int err = OB_SUCCESS;
      int64_t type = 0;
      while (postfix_size_ < OB_MAX_COMPOSITE_SYMBOL_COUNT)
      {

        /* ȡtype value pair�е�type */
        err = obj.deserialize(buf, data_len, pos);
        if (OB_SUCCESS != err)
        {
          YYSYS_LOG(WARN, "deserialize object error");
          break;
        }
        expr_[postfix_size_++] = obj;

        /* ���type, ���type=END�����Ϊ���һ��Ԫ�أ�break */
        if (ObIntType != obj.get_type())
        {
          err = OB_OBJ_TYPE_ERROR;
          YYSYS_LOG(WARN, "unexpected postfix expression header element type. ObIntType expected"
                    "[type:%d]", obj.get_type());
          break;
        }
        else if (OB_SUCCESS != (err = obj.get_int(type)))
        {
          YYSYS_LOG(WARN, "get int value failed [err:%d]", err);
          break;
        }
        else if (type == END)
        {
          //          YYSYS_LOG(INFO, "postfix expression deserialized");
          break;
        }

        /* ȡtype value pair�е�value */
        if (OB_SUCCESS != (err = obj.deserialize(buf, data_len, pos)))
        {
          YYSYS_LOG(WARN, "deserialize object error");
          break;
        }
        expr_[postfix_size_++] = obj;
      }

      if (END != type)
      {
        err = OB_ERR_UNEXPECTED;
        YYSYS_LOG(ERROR, "fail to deserialize. postfix_size_=%d", postfix_size_);
      }

      if (OB_SUCCESS != err)
      {
        postfix_size_ = 0;  // error found. rollback!
      }

      return err;
    }

    DEFINE_GET_SERIALIZE_SIZE(ObPostfixExpression)
    {
      int i = 0;
      int64_t total_size = 0;

      while(i < postfix_size_)
      {
        total_size += expr_[i].get_serialize_size();
        i++;
      }

      return total_size;
    }


  } /* common */
} /* namespace */
