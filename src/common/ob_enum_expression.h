/*
*   Version: 0.1
*
*   Authors:
*      tian zheng
*        - some work details if you want
*/

#ifndef OB_ENUM_EXPRESSION_H
#define OB_ENUM_EXPRESSION_H

#include "ob_calc_expression.h"
#include "ob_array.h"
#include "hash/ob_hashmap.h"//add wuna [MultiUps] [sql_api] 20151228

namespace oceanbase
{
  namespace common
  {

    typedef struct EnumItem
    {
     ObRowkey key; //in values element
     int32_t value_idx; // group index in enum_values_
     ObString group_name; // group name in enum_values_//add wuna [MultiUps] [sql_api] 20151228
     int64_t to_string(char* buffer, int64_t length) const;
     ~EnumItem()
     {}
    }EnumItem;

    struct EnumItemCompare
    {
      bool operator()(const EnumItem* litem, const EnumItem* ritem)
      {
        return litem->key < ritem->key;
      }
    };

    class ObEnumExpression : public ObCalcExpression
    {
    public:
     //mod wuna [MultiUps][sql_api] 20151217:b
     // ObEnumExpression(FIFOAllocator &allocator);
         ObEnumExpression();
      ~ObEnumExpression();
      
      //int set_expression(const ObString &expr);
      int set_expression(const ObString &expr,ObArray<ObString> &group_name_list,
                         ObCellArray& cell_array,bool is_for_check_expr=false);
      
      int calc(
          const ObCellArray &cell_array,
          ExpressionType part_type,//add wuna [MultiUps][sql_api] 20151217
          ObArray<ObString>& rule_param_list,
          ObObj &result);
      //int calc(const ObRowkey &check_key, ObObj &result);
      int calc(const ObRowkey &check_key, ExpressionType type, ObObj &result);

      void print_element();
      int32_t get_column_num();//add wuna [MultiUps][sql_api] 20151217
//      static const int32_t MAX_COLUMN_NUM = 3;
       //mod 20151217:e
    private:
      int get_str_len(const char* str, const int pos);
      //mod wuna [MultiUps][sql_api] 20151222:b
      //int parse_group_values(const char* str, int &pos, const int32_t value_idx);
      //int parse_element_values(const char* str, int &pos, ObObj* value);
      int parse_group_values(const char* str, int &pos, const int32_t value_idx,ObArray<ObString> &group_name_list,
                             ObCellArray& cell_array,bool is_for_check_expr);
      int parse_element_values(const char* str, int &pos, ObObj* value,ObCellArray& cell_array,bool is_for_check_expr);
      //mod 20151222:e
      void sort_elements();
    private:
      int32_t column_num_;
      ObArray<EnumItem*> element_values_; //in values, need sort after parse
      int32_t max_define_len_;
      hash::ObHashMap<ObRowkey, int32_t, hash::ReadWriteDefendMode> all_element_values_;//add wuna[MultiUps][sql_api]20151228
    };
  }
}

#endif // OB_ENUM_EXPRESSION_H
