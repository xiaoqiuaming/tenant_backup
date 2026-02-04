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
 *     - ��׺����ʽ��ֵ�������ڸ����е���Ҫ֧�ָ�����ֵ�ĳ���
 *
 */



#ifndef OCEANBASE_COMMON_OB_POSTFIX_EXPRESSION_H_
#define OCEANBASE_COMMON_OB_POSTFIX_EXPRESSION_H_
#include "ob_string.h"
#include "ob_expression.h"
#include "ob_array_helper.h"
#include "ob_object.h"
#include "hash/ob_hashmap.h"
#include "ob_result.h"
#include "common/ob_string_buf.h"
#include "ob_expr_obj.h"
namespace oceanbase
{
  namespace common
  {

    /*
     *  ��׺����ʽ=>��׺����ʽ ������
     *  reference: http://totty.iteye.com/blog/123252
     */
    class ObScanParam;
    class ObCellArray;
    class ObExpressionParser
    {
        class ObExpressionGrammaChecker
        {
            /* TODO: ���ͼ�� */
        };

      public:
        ObExpressionParser()
        {
          symbols_.init(OB_MAX_COMPOSITE_SYMBOL_COUNT, sym_list_);
          stack_.init(OB_MAX_COMPOSITE_SYMBOL_COUNT, sym_stack);
        }

        ~ObExpressionParser()
        {}

        //add peiouya [Expire_condition_modify] 20140909:b
        /**expr:Ŀ���Ǽ��������ǰ���ν��ַ��Ƿ�Ϸ�
        */
        int check_sys_day(const int code)
        {
          int err = OB_SUCCESS;
          switch(code)
          {
            case peCodeLessThan:
            case peCodeLessOrEqual:
            case peCodeGreaterThan:
            case peCodeGreaterOrEqual:
            case peCodeEqual:
            case peCodeNotEqual:
              err = OB_SUCCESS;
              break;
            default:
              err = OB_ERROR;
              YYSYS_LOG(ERROR, "invalid expire_info-----$SYS_DAY, include invalid param!");
              break;
          }
          return err;
        }

        //add for [507-expire info enhancement]-b
        int check_string_compare_operator_type(const int code)
        {
          int err = OB_SUCCESS;
          switch(code)
          {
            case peCodeLessThan:
            case peCodeLessOrEqual:
            case peCodeGreaterThan:
            case peCodeGreaterOrEqual:
            case peCodeEqual:
            case peCodeNotEqual:
              err = OB_SUCCESS;
              break;
            default:
              err = OB_ERROR;
              YYSYS_LOG(ERROR, "invalid expire_info include invalid param!");
              break;
          }
          return err;
        }

        int check_string_compare_columnType(const int code)
        {
          int err = OB_SUCCESS;
          switch(code)
          {
            case ObVarcharType:
              err = OB_SUCCESS;
              break;
            default:
              err = OB_INVALID_ARGUMENT;
              YYSYS_LOG(ERROR, "invalid expire_info include invalid param!");
              break;
          }
          return err;
        }

        int check_time_compare_columnType(const int code)
        {
          int err = OB_SUCCESS;
          switch(code)
          {
            case ObVarcharType:
            case ObDateTimeType:
            case ObModifyTimeType:
            case ObCreateTimeType:
            case ObPreciseDateTimeType:
            case ObIntType:
            case ObInt32Type:
              err = OB_SUCCESS;
              break;
            default:
              err = OB_INVALID_ARGUMENT;
              YYSYS_LOG(ERROR, "invalid expire_info include invalid param!");
              break;
          }
          return err;
        }
        //add for [507-expire info enhancement]-e

        //add 20140909:b
        //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
        //int parse(const ObString &infix_string, ObArrayHelper<ObObj> &postfix_objs)
        //modify for [507-expire_info enhancement]-b
        //    int parse(const ObString &infix_string, ObArrayHelper<ObObj> &postfix_objs, bool is_expire_info = false)
        int parse(const ObString &infix_string, ObArrayHelper<ObObj> &postfix_objs, bool is_expire_info = false
                                                                                                          , bool need_check = false, const ObSchemaManagerV2 *tmp_schema_mgr = NULL, const char *table_name = NULL);

        //add wenghaixing [secondary index create fix]20141226
        int64_t get_sym_type(int64_t i)
        {
          Symbol* sym= symbols_.at(i);
          if(sym==NULL)
            return 0;
          else
            return sym->type;
        }

        //add e

      private:

        enum Type
        {
          OPERATOR = 1,
          STRING,
          HEX_STRING,
          DATETIME,
          COLUMN_NAME,
          NUMBER,
          KEYWORD,
          HEX_NUMBER
        };

        enum peCode
        {
          peCodeLeftBracket = 1,
          peCodeRightBracket = 2,
          peCodeMul = 3,
          peCodeDiv = 4,
          peCodeAdd = 5,
          peCodeSub = 6,
          peCodeLessThan = 7,
          peCodeLessOrEqual = 8,
          peCodeEqual = 9,
          peCodeNotEqual = 10,
          peCodeGreaterThan = 11,
          peCodeGreaterOrEqual = 12,
          peCodeIs = 13,
          peCodeLike = 14,
          peCodeAnd = 15,
          peCodeOr = 16,
          peCodeNot = 17,
          peCodeMod = 20,
          peCodeColumnName = 50,
          peCodeString = 51,
          peCodeInt = 52,
          peCodeFloat = 53,
          peCodeDateTime = 54,
          peCodeNull = 55,
          peCodeTrue = 56,
          peCodeFalse = 57,
          peCodeMinus = 58,
          peCodePlus = 59,
          INVALID
        };


        enum pePriority
        {
          pePrioLeftBracket = 100,  /* highest */
          pePrioRightBracket = 0,   /* lowest */
          pePrioMinus = 10,
          pePrioPlus = 10,
          pePrioNot = 10,
          pePrioMul = 9,
          pePrioDiv = 9,
          pePrioMod = 9,
          pePrioAdd = 8,
          pePrioSub = 8,
          pePrioLessThan = 7,
          pePrioLessOrEqual = 7,
          pePrioEqual = 7,
          pePrioNotEqual = 7,
          pePrioGreaterThan = 7,
          pePrioGreaterOrEqual = 7,
          pePrioIs = 7,
          pePrioLike = 7,
          pePrioAnd = 6,
          pePrioOr = 5
          /*,
            pePrioString = -1,
            pePrioInt = -1,
            pePrioFloat = -1,
            pePrioDateTime = -1,
            pePrioNull = -1,
            pePrioTrue = -1,
            pePrioFalse = -1
            */
        };


        struct Symbol
        {
            Symbol():length(0), type(0)
            //add for [507-expire_info enhancement]-b
            ,need_complete_for_expire_info(false)
            //add for [507-expire_info enhancement]-e
            {};
            int length;
            int type;
            bool need_complete_for_expire_info; //add for [507-expire_info enhancement]
            char value[16 * 1024];  // FIXME: should use a predefined Macro
            int push(char c)
            {
              int err = OB_SUCCESS;

              if (length < 16 * 1024)
              {
                //YYSYS_LOG(DEBUG, "push:[lenght:%d]", length);
                value[length] = c;
                length++;
              }
              else
              {
                err = OB_ERROR;
                YYSYS_LOG(WARN, "expression parsing error. Token is too long! Exceeds 16KB");
              }
              return err;
            }
            ObObj encode;
            int code; //����
            int prio; //���ȼ�

            int toInt()
            {
              int err = OB_SUCCESS;
              int64_t d = 0;
              if (length >= 128)
              {
                err = OB_ERROR;
              }
              else
              {
                value[length] = '\0';
                // FIXME: ������
                d = (int64_t)atol(value);
                code = peCodeInt;
                encode.set_int(d);
              }
              return err;
            }

            int hexToInt()
            {
              int err = OB_SUCCESS;
              int64_t d = 0;
              if (length >= 128)
              {
                err = OB_ERROR;
              }
              else
              {
                value[length] = '\0';
                // FIXME: ������
                d = (int64_t)strtol(value, NULL, 16);
                code = peCodeInt;
                encode.set_int(d);
                //YYSYS_LOG(INFO, "hex2int, d=%ld", d);
              }
              return err;
            }

            int toDouble()
            {
              int err = OB_SUCCESS;
              double d = 0.0;
              if (length >= 128)
              {
                err = OB_ERROR;
              }
              else
              {
                value[length] = '\0';
                // FIXME: ������
                d = atof(value);
                code = peCodeFloat;
                encode.set_double(d);
              }
              return err;
            }
        };


        inline void skip_blank(const char *p, int &pos)
        {
          while(pos < infix_max_len_ && p[pos] == ' ')
          {
            pos++;
          }
        }

        inline int get_column_name(const char *p, int &pos, Symbol &sym)
        {
          char c;
          int err = OB_SUCCESS;

          skip_blank(p, pos);

          if (p[pos] == '`')
          {
            pos++;
            while(p[pos] != '`')
            {
              if (pos >= infix_max_len_)
              {
                err = OB_SIZE_OVERFLOW;
                YYSYS_LOG(WARN, "unexpected end of input");
                break;
              }

              if(OB_SUCCESS != (err = sym.push(p[pos])))
              {
                break;
              }
              pos++;
            }
            pos++;  // skip last `
          }
          else
          {
            do
            {
              if (pos >= infix_max_len_)
              {
                break;
              }
              c = p[pos];

              if (c == '(' || c == ')' || c == '>' || c == '<' || c == '!' || c == '=' ||
                  c == '+' || c == '-' || c == '*' || c == '/' || c == '%' || c == ' ')
              {
                break;
              }
              if(OB_SUCCESS != (err = sym.push(c)))
              {
                break;
              }
              pos++;
            }while(true);
          }
          return err;
        }


        inline int get_number(const char*p, int &pos, Symbol &sym)
        {
          int err = OB_SUCCESS;
          char c, c2;
          bool first_time = true;
          skip_blank(p, pos);

          if(pos + 1 < infix_max_len_)
          {
            c = p[pos];
            c2 = p[pos+1];
            if (c == '0' && (c2=='x' || c2=='X'))
            {
              sym.type = HEX_NUMBER;
              pos+=2;
              while(pos < infix_max_len_)
              {
                c = p[pos];
                if((c >= '0' && c <= '9') ||
                   (c >= 'a' && c <= 'f') ||
                   (c >= 'A' && c <= 'F'))
                {
                  if (OB_SUCCESS != (err = sym.push(c)))
                  {
                    break;
                  }
                  pos++;
                  first_time = false;
                }
                else
                {
                  // no data followed 0x
                  if (true == first_time)
                  {
                    err = OB_ERROR;
                  }
                  break;
                }
              }
            }
          }

          if (OB_SUCCESS == err && NUMBER == sym.type)
          {
            while(pos < infix_max_len_)
            {
              c = p[pos];
              if(c == '.')
              {
                if(first_time)
                {
                  first_time = false;
                  if (OB_SUCCESS != (err = sym.push(c)))
                  {
                    break;
                  }
                  pos++;
                }
                else
                {
                  // 2 dots is not right ;)
                  err = OB_ERROR;
                  break;
                }
              }
              else if(c >= '0' && c <= '9')
              {
                if (OB_SUCCESS != (err = sym.push(c)))
                {
                  break;
                }
                pos++;
              }
              else
              {
                break;
              }
            }
          }
          return err;
        }

        inline int get_left_parenthese(const char*p, int &pos, Symbol &sym)
        {
          int err = OB_SUCCESS;
          char c;

          skip_blank(p, pos);

          c = p[pos];
          if (c == '(')
          {
            if (OB_SUCCESS == (err = sym.push(c)))
            {
              //YYSYS_LOG(DEBUG, "push c=%c", c);
              pos++;
            }
          }
          else
          {
            YYSYS_LOG(WARN, "unexpected char %c", c);
          }
          return err;
        }

        inline int get_right_parenthese(const char*p, int &pos, Symbol &sym)
        {
          int err = OB_SUCCESS;
          char c;

          skip_blank(p, pos);

          c = p[pos];
          if (c == ')')
          {
            if (OB_SUCCESS == (err = sym.push(c)))
            {
              //YYSYS_LOG(DEBUG, "push c=%c", c);
              pos++;
            }
          }
          else
          {
            YYSYS_LOG(WARN, "unexpected char %c", c);
          }
          return err;
        }



        inline int get_operator(const char*p, int &pos, Symbol &sym)
        {
          int err = OB_SUCCESS;
          char c = 0;

          skip_blank(p, pos);

          if (pos < infix_max_len_)
          {
            c = p[pos];
          }

          if ( c == '+' || c == '-' || c == '*' || c == '/' || c == '%' || c == '=')
          {
            //YYSYS_LOG(DEBUG, "push c=%c", c);
            if (OB_SUCCESS == (err = sym.push(c)))
            {
              pos++;
            }
          }
          else if(c == '>' || c == '<' || c == '!')
          {
            //YYSYS_LOG(DEBUG, "push c=%c", c);
            if (OB_SUCCESS == (err = sym.push(c)))
            {
              pos++;
              // >=, <=, !=
              if (pos < infix_max_len_)
              {
                c = p[pos];
                if (c == '=')
                {
                  //YYSYS_LOG(DEBUG, "push c=%c", c);
                  if (OB_SUCCESS == (err = sym.push(c)))
                  {
                    pos++;
                  }
                }
              }
            }
          }

          return err;
        }


        inline int get_binary(const char*p, int &pos, Symbol &sym)
        {
          int err = OB_SUCCESS;

          skip_blank(p, pos);

          if ((pos < infix_max_len_ - 3) && (p[pos] == 'b' || p[pos] == 'B' ) && p[pos+1] == '\'')
          {
            pos++;
            pos++;
            while(p[pos] != '\'')
            {
              if (pos >= infix_max_len_)
              {
                err = OB_SIZE_OVERFLOW;
                YYSYS_LOG(WARN, "unexpected end of input");
                break;
              }
              if (OB_SUCCESS != (err = sym.push(p[pos])))
              {
                break;
              }
              pos++;
            }
            pos++;  // skip last '
            /* FIXME: ��ʮ�������ַ���ת����binary ObString */
            // code here?
            //
          }
          else
          {
            YYSYS_LOG(WARN, "unexpected string format");
          }

          return err;
        }


        inline int get_string(const char*p, int &pos, Symbol &sym)
        {
          int err = OB_SUCCESS;

          skip_blank(p, pos);

          if (p[pos] == '\'')
          {
            pos++;
            while(p[pos] != '\'')
            {
              if (pos >= infix_max_len_)
              {
                err = OB_SIZE_OVERFLOW;
                YYSYS_LOG(WARN, "unexpected end of input");
                break;
              }
              // string��֧��ת���ַ�(\)
              if (pos < infix_max_len_ - 1 && p[pos] == '\\' && p[pos+1] == '\'')
              {
                pos++;  // skip back slash(\)
              }

              if (OB_SUCCESS != (err = sym.push(p[pos])))
              {
                break;
              }
              pos++;
            }
            pos++;  // skip last '
          }
          else
          {
            YYSYS_LOG(WARN, "unexpected string format");
          }

          return err;
        }

        inline int get_keyword(const char*p, int &pos, Symbol &sym)
        {
          int err = OB_SUCCESS;
          char c;

          skip_blank(p, pos);
          do
          {
            if (pos >= infix_max_len_)
            {
              break;
            }
            c = p[pos];
            //YYSYS_LOG(DEBUG, "c=%c", c);
            if (c == '(' || c == ')' || c == '>' || c == '<' || c == '!' || c == '=' ||
                c == '+' || c == '-' || c == '*' || c == '/' || c == '%' || c == ' ')
            {
              break;
            }
            if (OB_SUCCESS != (err = sym.push(c)))
            {
              break;
            }
            pos++;
          }while(true);

          return err;
        }


        inline int get_datetime(const char*p, int &pos, Symbol &sym, char sep)
        {
          int err = OB_SUCCESS;

          skip_blank(p, pos);

          if (p[pos] == sep)
          {
            pos++;
            while(p[pos] != sep)
            {
              if (pos >= infix_max_len_)
              {
                err = OB_SIZE_OVERFLOW;
                YYSYS_LOG(WARN, "unexpected end of input");
                break;
              }
              if (OB_SUCCESS != (err = sym.push(p[pos])))
              {
                break;
              }
              pos++;
            }
            pos++;  // skip last sep
          }
          else
          {
            err = OB_ERR_UNEXPECTED;
            YYSYS_LOG(WARN, "unexpected datetime format");
          }
          return err;
        }


        inline int get_datetime_v1(const char*p, int &pos, Symbol &sym)
        {
          return get_datetime(p, pos, sym, '#');
        }


        inline int get_datetime_v2(const char*p, int &pos, Symbol &sym)
        {
          return get_datetime(p, pos, sym, '\'');
        }

        inline int decode_datetime(const char *str, int length, int64_t &decoded_val)
        {
          int err = OB_SUCCESS;
          char tmp_buf[127+1];  // zero terminator
          char *tmp_ptr = NULL;
          const char *format = NULL;
          //char *str = "2010-01-02 02:32:2";
          struct tm tm;
          char *ret;

          /// @note: must set to zero. strptime won't clear this when format is "%Y-%m-%d".
          memset(&tm, 0x00, sizeof(struct tm));

          if (length > 127 || length <= 0 || NULL == str)
          {
            err = OB_ERROR;
            YYSYS_LOG(WARN, "invalid param. length=%d, str=%p", length, str);
          }
          else
          {
            strncpy(tmp_buf, str, length);
            tmp_buf[length] = '\0';

            if (NULL != strchr(tmp_buf, ' '))
            {
              format = "%Y-%m-%d %H:%M:%S";
            }
            else if(NULL != strchr(tmp_buf, '-'))
            {
              format = "%Y-%m-%d";
            }
            else
            {
              // timestamp���﷽ʽ
              // FIXME: ��֧�ָ�����ʾ��һ����˵�������󲻴��ڣ��ʲ�֧��
              format = NULL;
            }

            if (NULL != format)
            {
              ret = strptime(tmp_buf, format, &tm);
              if (ret != (tmp_buf + length))
              {
                decoded_val = -1;
                err = OB_ERROR;
                YYSYS_LOG(WARN, "decode datetime error. [input:%.*s][format=%s][ret=%p][tmp_buf=%p][length=%d]",
                          length, tmp_buf, format, ret, tmp_buf, length);
              }
              else
              {
                tm.tm_isdst = -1;
                if(-1 == (decoded_val = (int64_t)mktime(&tm)))
                {
                  err = OB_ERROR;
                  YYSYS_LOG(WARN, "convert time to timestamp format error");
                }
                else
                {
                  /// bugfix: set to precise date time
                  decoded_val *= 1000L * 1000; // convert sec to micro-sec
                }
              }
            }
            else  // NULL == format, direct timestamp format like #12312423# in micro-sec format
            {
              decoded_val = strtol(tmp_buf, &tmp_ptr, 10);
              if ('\0' != *tmp_ptr) // entire string is not valid
              {
                err = OB_ERROR;
                YYSYS_LOG(WARN, "convert timestamp error");
              }
            }
          }
          return err;
        }

        int get_sym(const char*p, int &pos, Symbol &sym)
        {
          int err = OB_SUCCESS;
          char c, c2;

          skip_blank(p, pos); // ����ǰ���ո�

          if (pos < infix_max_len_ - 1)
          {
            c = p[pos];
            c2 = p[pos+1];
          }
          else if (pos < infix_max_len_)
          {
            c = p[pos];
            c2 = 0;
          }
          else
          {
            err = OB_ITER_END;
          }


          if(OB_SUCCESS == err && c != ' ')
          {
            if(c == '>' || c == '<' || c == '!' || c == '=' ||
               c == '+' || c == '-' || c == '*' || c == '/' || c == '%')
            {
              //YYSYS_LOG(DEBUG, "get operator");
              sym.type = OPERATOR;
              err = get_operator(p, pos, sym);
            }
            else if(c == '(')
            {
              //YYSYS_LOG(DEBUG, "get left parenthese");
              sym.type = OPERATOR;
              err = get_left_parenthese(p, pos, sym);
            }
            else if(c == ')')
            {
              //YYSYS_LOG(DEBUG, "get left parenthese");
              sym.type = OPERATOR;
              err = get_right_parenthese(p, pos, sym);
            }
            else if(c == '\'')
            {
              //YYSYS_LOG(DEBUG, "get string");
              sym.type = STRING;
              err = get_string(p, pos, sym);
            }
            else if(c == '#')
            {
              //YYSYS_LOG(DEBUG, "get datetime");
              sym.type = DATETIME;
              err = get_datetime_v1(p, pos, sym);
            }
            else if(c == 'd'|| c == 'D') // d... possiblely a datetime type, try it!
            {
              bool is_datetime = true;
              if (pos < infix_max_len_ - 8)
              {
                if (0 == strncasecmp("datetime'", &p[pos], 9))
                {
                  // datetime'...
                  pos += 8;
                }
                else if (0 == strncasecmp("date'", &p[pos], 5))
                {
                  // date'...
                  pos += 4;
                }
                else if (c2 == '\'')
                {
                  // D'
                  pos += 1;
                }
                else
                {
                  is_datetime = false;
                }
              }
              else if (pos < infix_max_len_ - 4)
              {
                if (0 == strncasecmp("date'", &p[pos], 5))
                {
                  // date'...
                  pos += 4;
                }
                else if (c2 == '\'')
                {
                  // D'
                  pos += 1;
                }
                else
                {
                  is_datetime = false;
                }
              }
              else if (c2 == '\'')
              {
                // D'
                pos += 1;
              }
              else
              {
                is_datetime = false;
              }

              if (true == is_datetime)
              {
                //YYSYS_LOG(DEBUG, "get datetime");
                sym.type = DATETIME;
                err = get_datetime_v2(p, pos, sym);
              }
              else
              {
                //YYSYS_LOG(DEBUG, "get keyword");
                sym.type = KEYWORD;
                err = get_keyword(p, pos, sym);
              }
            }
            else if(c == '`')
            {
              //YYSYS_LOG(DEBUG, "get column name");
              sym.type = COLUMN_NAME;
              err = get_column_name(p, pos, sym);
            }
            else if(c == '.' || (c >= '0' && c <= '9'))
            {
              //YYSYS_LOG(DEBUG, "get number");
              sym.type = NUMBER;
              err = get_number(p, pos, sym);
            }
            else if((c == 'b' || c == 'B') && c2 == '\'')
            {
              //YYSYS_LOG(DEBUG, "get string");
              sym.type = HEX_STRING;
              err = get_binary(p, pos, sym);
            }
            else
            {
              //YYSYS_LOG(DEBUG, "get keyword");
              sym.type = KEYWORD;
              err = get_keyword(p, pos, sym);
            }
          }
          else
          {
            //YYSYS_LOG(DEBUG, "end of input!!!!!!!!!!!!!!!!!!!!!!!");
          }

          return err;
        }

        int hex_to_binary(Symbol &sym)
        {
          int err = OB_SUCCESS;
          int i = 0;
          int out = 0;
          int hi, lo;

          // ���ַ��� 0~f ת���������� 0~15
          for(i = 0; i < sym.length; i++)
          {
            if (sym.value[i] >= 'A' && sym.value[i] <= 'F')
            {
              sym.value[i] = static_cast<char>(sym.value[i] - 'A' + 10);
            }
            else if (sym.value[i] >= 'a' && sym.value[i] <= 'f')
            {
              sym.value[i] = static_cast<char>(sym.value[i] - 'a' + 10);
            }
            else if(sym.value[i] >= '0' && sym.value[i] <= '9')
            {
              sym.value[i] = static_cast<char>(sym.value[i] - '0');
            }
            else
            {
              YYSYS_LOG(WARN, "Invalid binary format. sym[%d] = 0x%X", i, sym.value[i]);
              err = OB_ERROR;
            }
          }

          if (OB_SUCCESS == err)
          {
            i = 0;
            if (sym.length % 2 != 0)
            {
              hi = 0;
              lo = sym.value[i++];
              sym.value[out++] = static_cast<char>(lo);
            }
            while(i < sym.length)
            {
              hi = sym.value[i++];
              lo = sym.value[i++];
              sym.value[out++] = static_cast<char>(lo + (hi << 4));
              //YYSYS_LOG(DEBUG, "c=%c", sym.value[out-1]);
            }
            sym.length = out;
          }

          return err;

        }

        bool symbol_is_operand(const Symbol &sym)
        {
          bool ret = false;
          switch(sym.code)
          {
            case peCodeColumnName:
            case peCodeString:
            case peCodeInt:
            case peCodeFloat:
            case peCodeDateTime:
            case peCodeNull:
            case peCodeTrue:
            case peCodeFalse:
              // �������Ƚ����⣬��Ҫע�� (1-3)-5,()��Ϊһ��������Կ����Ǹ�operand
            case peCodeRightBracket:
              ret = true;
              break;
            default:
              ret = false;
              break;
          }
          return ret;
        }

        //modify peiouya [Expire_condition_modify] 20140909:b
        int encode_sym(Symbol &sym, const Symbol *prev_symbol)
        //      int encode_sym(Symbol &sym, const Symbol *prev_symbol, const bool is_sys_day)
        //modify 20140909:e
        {
          int err = OB_SUCCESS;
          int64_t tmp = 0;
          ObString s;
          bool dig_found = false;
          int i = 0;
          //YYSYS_LOG(DEBUG, "sym type:%d,value:%.*s", sym.type, sym.length, sym.value);
          switch(sym.type)
          {
            case OPERATOR:
              if (0 == strncmp("+", sym.value, sym.length))
              {
                // ���+��ǰ�治�ǲ�����������һ������������ô���Զ϶������ţ������ǼӺ�
                if ((prev_symbol == NULL) || ((prev_symbol != NULL) && (true != symbol_is_operand(*prev_symbol))))
                {
                  sym.code = peCodePlus;
                  sym.prio = pePrioPlus;
                  sym.encode.set_int(ObExpression::PLUS_FUNC);
                }
                else
                {
                  sym.code = peCodeAdd;
                  sym.prio = pePrioAdd;
                  sym.encode.set_int(ObExpression::ADD_FUNC);
                }
              }
              else if(0 == strncmp("-", sym.value, sym.length))
              {
                // ���-��ǰ�治�ǲ�����������һ������������ô���Զ϶��Ǹ��ţ������Ǽ���
                if ((prev_symbol == NULL) || ((prev_symbol != NULL) && (true != symbol_is_operand(*prev_symbol))))
                {
                  sym.code = peCodeMinus;
                  sym.prio = pePrioMinus;
                  sym.encode.set_int(ObExpression::MINUS_FUNC);
                }
                else
                {
                  sym.code = peCodeSub;
                  sym.prio = pePrioSub;
                  sym.encode.set_int(ObExpression::SUB_FUNC);
                }
              }
              else if(0 == strncmp("*", sym.value, sym.length))
              {
                sym.code = peCodeMul;
                sym.prio = pePrioMul;
                sym.encode.set_int(ObExpression::MUL_FUNC);
              }
              else if(0 == strncmp("/", sym.value, sym.length))
              {
                sym.code = peCodeDiv;
                sym.prio = pePrioDiv;
                sym.encode.set_int(ObExpression::DIV_FUNC);
              }
              else if(0 == strncmp("%", sym.value, sym.length))
              {
                sym.code = peCodeMod;
                sym.prio = pePrioMod;
                sym.encode.set_int(ObExpression::MOD_FUNC);
              }
              else if(0 == strncmp("<", sym.value, sym.length))
              {
                sym.code = peCodeLessThan;
                sym.prio = pePrioLessThan;
                sym.encode.set_int(ObExpression::LT_FUNC);
              }
              else if(0 == strncmp("<=", sym.value, sym.length))
              {
                sym.code = peCodeLessOrEqual;
                sym.prio = pePrioLessOrEqual;
                sym.encode.set_int(ObExpression::LE_FUNC);
              }
              else if(0 == strncmp("=", sym.value, sym.length))
              {
                sym.code = peCodeEqual;
                sym.prio = pePrioEqual;
                sym.encode.set_int(ObExpression::EQ_FUNC);
              }
              else if(0 == strncmp("!=", sym.value, sym.length))
              {
                sym.code = peCodeNotEqual;
                sym.prio = pePrioNotEqual;
                sym.encode.set_int(ObExpression::NE_FUNC);
              }
              else if(0 == strncmp(">", sym.value, sym.length))
              {
                sym.code = peCodeGreaterThan;
                sym.prio = pePrioGreaterThan;
                sym.encode.set_int(ObExpression::GT_FUNC);
              }
              else if(0 == strncmp(">=", sym.value, sym.length))
              {
                sym.code = peCodeGreaterOrEqual;
                sym.prio = pePrioGreaterOrEqual;
                sym.encode.set_int(ObExpression::GE_FUNC);
              }
              else if(0 == strncmp("(", sym.value, sym.length))
              {
                sym.code = peCodeLeftBracket;
                sym.prio = pePrioLeftBracket;
                sym.encode.set_int(ObExpression::LEFT_PARENTHESE);
              }
              else if(0 == strncmp(")", sym.value, sym.length))
              {
                sym.code = peCodeRightBracket;
                sym.prio = pePrioRightBracket;
                sym.encode.set_int(ObExpression::RIGHT_PARENTHESE);
              }
              else
              {
                YYSYS_LOG(WARN, "unexpected symbol value. [type:%d][value:%.*s]",
                          sym.type,
                          sym.length,
                          sym.value);
                err = OB_ERR_UNEXPECTED;
              }
              break;
            case DATETIME:
              //TODO:���ַ���ʱ��ת����ʱ���
              sym.code = peCodeDateTime;
              if(OB_SUCCESS != (err = decode_datetime(sym.value, sym.length, tmp)))
              {
                YYSYS_LOG(WARN, "fail to decode datetime.");
              }
              else
              {
                sym.encode.set_precise_datetime(tmp);
              }
              break;
            case NUMBER:
              for(i = 0; i < sym.length; i++)
              {
                if (sym.value[i] == '.')
                {
                  if (dig_found == false)
                  {
                    dig_found = true;
                  }
                  else
                  {
                    YYSYS_LOG(WARN, "invalid number format! [type:%d][value:%.*s]",
                              sym.type,
                              sym.length,
                              sym.value);
                    err = OB_ERROR;
                    break;
                  }
                }
              }
              if ( OB_SUCCESS == err )
              {
                if (dig_found)
                {
                  //FIXME: ��ǰ�汾��֧��0x, 10.0f�ȱ�ʾ��
                  //       ��֧��ʮ���������͸�������˫���ȣ�
                  // TODO: ����ת��
                  sym.toDouble();
                  //double d = 10.0;
                  //sym.code = peCodeFloat;
                  //sym.encode.set_double(d);
                }
                else
                {
                  sym.toInt();
                  //int64_t i = 10;
                  //sym.code = peCodeInt;
                  //sym.encode.set_int(i);
                }
              }
              //add peiouya [Expire_condition_modify] 20140909:b
              //            if (is_sys_day)
              //            {
              //              int64_t value = 0;
              //              sym.encode.get_int(value);
              //              value *= 24 * 60 * 60 * 1000000L;
              //              sym.type = DATETIME;
              //              sym.code = peCodeDateTime;
              //              sym.encode.set_precise_datetime(value);
              //            }
              //add 20140909:e
              break;
            case HEX_NUMBER:
              sym.hexToInt();
              //int64_t i = 10;
              //sym.code = peCodeInt;
              //sym.encode.set_int(i);
              //add peiouya [Expire_condition_modify] 20140909:b
              //                if (is_sys_day)
              //                {
              //                  int64_t value = 0;
              //                  sym.encode.get_int(value);
              //                  value *= 24 * 60 * 60 * 1000000L;
              //                  sym.type = DATETIME;
              //                  sym.code = peCodeDateTime;
              //                  sym.encode.set_precise_datetime(value);
              //                }
              //add 20140909:e
              break;
            case COLUMN_NAME:
#if 1
              /// ��ʽ����
              sym.code = peCodeColumnName;
              s.assign(sym.value, sym.length);
              sym.encode.set_varchar(s);
#else
              /// רΪ���Զ�д
              sym.toInt();
              sym.code = peCodeColumnName;  // fixup code type
#endif
              break;
            case STRING:
              sym.code = peCodeString;
              s.assign(sym.value, sym.length);
              sym.encode.set_varchar(s);
              break;
            case HEX_STRING:
              sym.code = peCodeString;
              /*TODO: ��4141���ִ�ת����AA�����Ĵ���Ȼ���ObString
            */
              if(OB_SUCCESS == (err = hex_to_binary(sym)))
              {
                s.assign(sym.value, sym.length);
                sym.encode.set_varchar(s);
              }
              break;
            case KEYWORD:
              if (strlen("AND") == (size_t)sym.length && 0 == strncasecmp("AND", sym.value, sym.length))
              {
                sym.type = OPERATOR;
                sym.code = peCodeAnd;
                sym.prio = pePrioAnd;
                sym.encode.set_int(ObExpression::AND_FUNC);
              }
              else if (strlen("OR") == (size_t)sym.length && 0 == strncasecmp("OR", sym.value, sym.length))
              {
                sym.type = OPERATOR;
                sym.code = peCodeOr;
                sym.prio = pePrioOr;
                sym.encode.set_int(ObExpression::OR_FUNC);
              }
              else if (strlen("NOT") == (size_t)sym.length && 0 == strncasecmp("NOT", sym.value, sym.length))
              {
                sym.type = OPERATOR;
                sym.code = peCodeNot;
                sym.prio = pePrioNot;
                sym.encode.set_int(ObExpression::NOT_FUNC);
              }
              else if (strlen("IS") == (size_t)sym.length && 0 == strncasecmp("IS", sym.value, sym.length))
              {
                sym.type = OPERATOR;
                sym.code = peCodeIs;
                sym.prio = pePrioIs;
                sym.encode.set_int(ObExpression::IS_FUNC);
              }
              else if (strlen("LIKE") == (size_t)sym.length && 0 == strncasecmp("LIKE", sym.value, sym.length))
              {
                sym.type = OPERATOR;
                sym.code = peCodeLike;
                sym.prio = pePrioLike;
                sym.encode.set_int(ObExpression::LIKE_FUNC);
              }
              else if (strlen("NULL") == (size_t)sym.length && 0 == strncasecmp("NULL", sym.value, sym.length))
              {
                sym.type = NUMBER;
                sym.code = peCodeNull;
                sym.encode.set_null();
              }
              else
              {
                sym.type = COLUMN_NAME;
#if 1
                /// ��ʽ����
                sym.code = peCodeColumnName;
                s.assign(sym.value, sym.length);
                sym.encode.set_varchar(s);
#else
                /// רΪ���Զ�д
                sym.toInt();
                sym.code = peCodeColumnName;  // fixup code type
#endif
                //YYSYS_LOG(INFO, "column name with ` (e.g `my_col_name`) is recommended!");
                break;
              }
              break;
            default:
              YYSYS_LOG(WARN, "unexpected sym type. [type:%d][value:%.*s]",
                        sym.type,
                        sym.length,
                        sym.value);
              err = OB_ERR_UNEXPECTED;
              break;
          }
          YYSYS_LOG(DEBUG, "sym type:%d,value:%.*s", sym.type, sym.length, sym.value);
          //sym.encode.dump();
          return err;
        }

        int parse_expression()
        {
          int err = OB_SUCCESS;
          int i = 0;
          int code = 0;
          Symbol *sym = NULL;
          //Symbol *next_sym = NULL;
          Symbol zero_sym;

          for (i = 0; i < symbols_.get_array_index(); i++)
          {
            sym = symbols_.at(i);
            code = sym->code;
            switch(code)
            {
              case peCodeLeftBracket:
                if (!stack_.push_back(sym))
                {
                  err = OB_ERROR;
                }
#if 0
                // ����һ�����ţ�����ǡ�+/-���������һ��0��symbol�б���
                // �Ż�������0��symbol�б������ϻᱻoutput��
                //       ֱ��output���Ա���һ��symbol�б������shuffle
                if (i < symbols_.get_array_index() - 1 )
                {
                  next_sym = symbols_.at(i+1);
                  if (peCodeSub == next_sym->code || peCodeAdd == next_sym->code)
                  {
                    zero_sym.push('0');
                    zero_sym.toInt();
                    output(&zero_sym);
                  }
                }
#endif
                break;
              case peCodeRightBracket:
                err = do_oper2();
                break;
              case peCodeMul:
              case peCodeDiv:
              case peCodeMod:
                //break;
              case peCodeAdd:
              case peCodeSub:
                // break;
              case peCodeLessThan:
              case peCodeLessOrEqual:
              case peCodeEqual:
              case peCodeNotEqual:
              case peCodeGreaterThan:
              case peCodeGreaterOrEqual:
              case peCodeIs:
              case peCodeLike:
                // break;
              case peCodeAnd:
                // break;
              case peCodeOr:
              case peCodeNot:
              case peCodeMinus:
              case peCodePlus:
                err = do_oper1(sym);
                break;
              case peCodeString:
              case peCodeInt:
              case peCodeFloat:
              case peCodeDateTime:
              case peCodeNull:
              case peCodeTrue:
              case peCodeFalse:
                err = output(sym);
                break;
                //add peiouya [Expire_condition_modify] 20140913:b
              case INVALID:
                //NOTHING TODO, SKIP CURRENT invalid sym
                break;
                //add 20140913:e
              default:
                err = output(sym);
                /*
                 err = OB_ERR_UNEXPECTED;
                 YYSYS_LOG(WARN, "unexpected symbol found");
                 */
                break;
            }
            if (OB_SUCCESS != err)
            {
              YYSYS_LOG(WARN, "fail. err = %d", err);
              break;
            }
          }

          while((OB_SUCCESS == err) && (0 < stack_.get_array_index()))
          {
            //YYSYS_LOG(INFO, "pop");
            err = output(*stack_.pop());
            //YYSYS_LOG(INFO, "pop end");
          }

          if (OB_SUCCESS == err)
          {
            err = output_terminator();
          }

          if (OB_SUCCESS == err && 0 == stack_.get_array_index())
          {
            err = OB_ITER_END;
          }
          return err;
        }

        int do_oper1(Symbol *new_sym)
        {
          int err = OB_SUCCESS;
          Symbol *old_sym;
          int code = 0;
          int new_prio = 0;
          int old_prio = 0;

          if (NULL != new_sym)
          {
            while((OB_SUCCESS == err) && (0 < stack_.get_array_index()))
            {
              old_sym = *stack_.pop();
              if (NULL  == old_sym)
              {
                YYSYS_LOG(WARN, "unexpect null pointer");
                err = OB_ERROR;
                break;
              }
              code = old_sym->code;
              if (peCodeLeftBracket == code)
              {
                if (!stack_.push_back(old_sym))
                {
                  err = OB_ERROR;
                  YYSYS_LOG(WARN, "fail to push symbol into stack. stack is full!");
                }
                break;
              }
              else
              {
                new_prio = new_sym->prio;
                old_prio = old_sym->prio;
                if (new_prio > old_prio)
                {
                  if (!stack_.push_back(old_sym))
                  {
                    err = OB_ERROR;
                    YYSYS_LOG(WARN, "fail to push symbol into stack. stack is full!");
                  }
                  break;
                }
                // bugfix code: �����������ҽ�ϣ������ұ߲��������ȼ���
                else if (new_prio == old_prio && (new_sym->code == peCodeMinus || new_sym->code == peCodePlus))
                {
                  if (!stack_.push_back(old_sym))
                  {
                    err = OB_ERROR;
                    YYSYS_LOG(WARN, "fail to push symbol into stack. stack is full!");
                  }
                  break;
                }
                else
                {
                  err = output(old_sym);
                }
              }
            } /* while */

            if (OB_SUCCESS == err)
            {
              if(!stack_.push_back(new_sym))
              {
                err = OB_ERROR;
              }
            }
          }
          else
          {
            YYSYS_LOG(WARN, "symbol pointer is null");
            err = OB_ERROR;
          }
          return err;
        }



        int do_oper2()
        {
          int err = OB_SUCCESS;
          Symbol *sym;
          int code = 0;

          while(0 < stack_.get_array_index())
          {
            sym = *stack_.pop();
            if (NULL == sym)
            {
              YYSYS_LOG(WARN, "fail to pop symbol from stack. stack is empty!");
              err = OB_ERROR;
              break;
            }
            else
            {
              code = sym->code;
              if (peCodeLeftBracket == code)
              {
                break;
              }
              else
              {
                err = output(sym);
                if (OB_SUCCESS != err)
                {
                  YYSYS_LOG(WARN, "unexpected error");
                  break;
                }
              }
            }
          }
          return err;
        }

        //add  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
        int conplete_for_expire_info(Symbol *s)
        {
          OB_ASSERT(is_expire_info_);

          int err = OB_SUCCESS;
          ObObj tmp_obj[4];  //only for expire_info

          switch (s->code)
          {
            case peCodeColumnName:
            case peCodeString:
            case peCodeDateTime:
              tmp_obj[0].set_int(ObExpression::CONST_OBJ);
              tmp_obj[1].set_int(common::ObIntType);
              tmp_obj[2].set_int(ObExpression::OP);
              tmp_obj[3].set_int(ObExpression::CAST_THIN_FUNC);
              for(int64_t index = 0; index < 4; ++index)
              {
                if(!result_->push_back(tmp_obj[index]))
                {
                  err = OB_ERROR;
                  YYSYS_LOG(WARN, "push obj failed. array_index=%ld array_size=%ld",
                            result_->get_array_index(), result_->get_array_size());
                  break;
                }
              }
              break;
            default:
              break;
          }

          return err;
        }
        //add  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e

        int output(Symbol *s)
        {
          int err = OB_SUCCESS;
          ObObj type_obj;
          /*
        YYSYS_LOG(INFO, ">>>>>>>>output===>>>>: [code:%d, value:%.*s]",
            s->code,
            s->length,
            s->value);
        s->encode.dump();
        */
          if (NULL == result_)
          {
            err = OB_ERROR;
            YYSYS_LOG(WARN, "add symbol to result stack failed. result=%p", result_);
          }
          else
          {
            /* ��׺����ʽ����׺����ʽ��Э��ת�� */
            switch(s->code)
            {
              case peCodeMul:
              case peCodeDiv:
              case peCodeMod:
              case peCodeAdd:
              case peCodeSub:
              case peCodeLessThan:
              case peCodeLessOrEqual:
              case peCodeEqual:
              case peCodeNotEqual:
              case peCodeGreaterThan:
              case peCodeGreaterOrEqual:
              case peCodeIs:
              case peCodeLike:
              case peCodeAnd:
              case peCodeOr:
              case peCodeNot:
              case peCodeMinus:
              case peCodePlus:
                type_obj.set_int(ObExpression::OP);
                break;
              case peCodeColumnName:
                type_obj.set_int(ObExpression::COLUMN_IDX);
                break;
              case peCodeString:
              case peCodeInt:
              case peCodeFloat:
              case peCodeDateTime:
              case peCodeNull:
              case peCodeTrue:
              case peCodeFalse:
                type_obj.set_int(ObExpression::CONST_OBJ);
                break;
              default:
                YYSYS_LOG(WARN, "unexpected symbol. code=%d", s->code);
                err = OB_ERROR;
                break;
            }
            if ( (OB_SUCCESS != err) || (!result_->push_back(type_obj)) )
            {
              err = OB_ERROR;
              YYSYS_LOG(WARN, "push obj failed. array_index=%ld array_size=%ld",
                        result_->get_array_index(), result_->get_array_size());
            }
            else if (!result_->push_back(s->encode))
            {
              err = OB_ERROR;
              YYSYS_LOG(WARN, "push obj failed. array_index=%ld array_size=%ld",
                        result_->get_array_index(), result_->get_array_size());
            }
            //add  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
            //modify for [507-expire_info enhancement]-b
            //          else if (is_expire_info_)
            else if (is_expire_info_ && s->need_complete_for_expire_info)
              //modify for [507-expire_info enhancement]-e
            {
              err = conplete_for_expire_info(s);
            }
            //add  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:e
          }
          return err;
        }

        int output_terminator()
        {
          int err = OB_SUCCESS;
          ObObj type_obj;

          type_obj.set_int(ObExpression::END);  // terminator symbol
          if (!result_->push_back(type_obj))
          {
            err = OB_ERROR;
            YYSYS_LOG(WARN, "push obj failed. array_index=%ld array_size=%ld",
                      result_->get_array_index(), result_->get_array_size());
          }
          return err;
        }

        int infix2postfix()
        {
          int err = OB_SUCCESS;
          if (symbols_.get_array_index() > 0 && NULL != result_)
          {
            err = parse_expression();
          }
          else
          {
            err = OB_ERROR;
            YYSYS_LOG(WARN, "param error in parsing expression. size=%ld, result=%p",
                      symbols_.get_array_index(), result_);
          }

          if (OB_ITER_END != err)
          {
            err = OB_ERROR;
          }
          else
          {
            err = OB_SUCCESS;
          }

          return err;
        }


        /* ���ö��� */
        void reset()
        {
          this->symbols_.clear();
          this->stack_.clear();
          this->result_ = NULL;
          this->infix_max_len_ = 0;  // input expr length
          is_expire_info_ = false;  //add  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608
        }

      private:
        // internal stack for parsing expression
        Symbol *sym_stack[OB_MAX_COMPOSITE_SYMBOL_COUNT];
        ObArrayHelper<Symbol *> stack_;
        // internal buffer for parsing symbols
        Symbol sym_list_[OB_MAX_COMPOSITE_SYMBOL_COUNT];
        ObArrayHelper<Symbol> symbols_;
        // result pointer. point to result buffer
        ObArrayHelper<ObObj> *result_;
        // �������ַ����ĳ��ȣ����ڷ�ֹԽ����ʺ��ж���ֹ
        int infix_max_len_;
        bool is_expire_info_;  //add  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608
    };










    class ObScanParam;
    typedef int(*op_call_func_t)(ObExprObj *a, int &b, ObExprObj &c);
    class ObPostfixExpression
    {
      public:
        ObPostfixExpression()
        {};
        ~ObPostfixExpression()
        {};
        ObPostfixExpression& operator=(const ObPostfixExpression &other);

        /* @param expr:�Ѿ��������˵ĺ�׺����ʽ���� */
        int set_expression(const ObObj *expr, ObStringBuf  & data_buf);

        /* @absolete interface
         *
         * @param expr: ��׺����ʽ
         * @param scan_param: ����name to index��ӳ�䣬���ڽ���
         */
        int set_expression(const ObString &expr, const ObScanParam &scan_param);

        /* @param expr: ��׺����ʽ
         * @param cname_to_idx_map: ����name to index��ӳ�䣬���ڽ���
         * @param parser: �����������ڽ���expr
         */
        //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b
        //int set_expression(const ObString &expr,
        //    const hash::ObHashMap<ObString,int64_t,hash::NoPthreadDefendMode> & cname_to_idx_map,
        //    ObExpressionParser & parser, common::ObResultCode *rc = NULL);
        int set_expression(const ObString&                                                         expr,
                           const hash::ObHashMap<ObString,int64_t,hash::NoPthreadDefendMode> &     cname_to_idx_map,
                           ObExpressionParser &                                                    parser,
                           common::ObResultCode                                                    *rc  =  NULL,
                           bool                                                                    is_expire_info = false);
        //mod  peiouya [EXPIRE_INFO_NOT_ARITHMETIC_FIX] 20160608:b

        /* ��org_cell�е�ֵ���뵽expr������ */
        int calc(ObObj &composite_val,
                 const ObCellArray & org_cells,
                 const int64_t org_row_beg,
                 const int64_t org_row_end
                 );

        const ObObj * get_expression()const
        {
          return expr_;
        }
        /* ���л� �� �����л� */
        NEED_SERIALIZE_AND_DESERIALIZE;
      private:
      private:
        static op_call_func_t call_func[ObExpression::MAX_FUNC];
        ObObj expr_[OB_MAX_COMPOSITE_SYMBOL_COUNT];
        // ObExpressionParser parser_;
        // TODO: �޸ĳ�ָ�����ã��������ݿ���
        ObExprObj stack_i[OB_MAX_COMPOSITE_SYMBOL_COUNT];
        int postfix_size_;
    }; // class ObPostfixExpression

  } // namespace commom
}// namespace oceanbae

#endif
