/*
*   Version: 0.1
*
*   Authors:
*      liu jun
*        - some work details if you want
*/

#ifndef OB_RANGE_EXPRESSION_H
#define OB_RANGE_EXPRESSION_H

#include "ob_calc_expression.h"

namespace oceanbase
{
  namespace common
  {
    //add wuna [MultiUps][sql_api] 20151217:b
    typedef struct RangeItem
    {
        ObRowkey key; //in values element
        int32_t value_idx;
        ObString group_name; // group name in range_values_
        int64_t to_string(char* buffer, int64_t length) const;
        ~RangeItem()
        {}
    }RangeItem;
    //add 20151217:e
    class ObRangeExpression : public ObCalcExpression
    {
      public:
        //mod wuna [MultiUps][sql_api] 20151217:b
        //  ObRangeExpression(FIFOAllocator &allocator);
        ObRangeExpression();
        ~ObRangeExpression();

        //mod wuna [MultiUps][sql_api] 20151217:b
        //int set_expression(const ObString &expr);
        int set_expression(const ObString &expr,ObArray<ObString> &group_name_list,
                           ObCellArray& cell_array,bool is_for_check_expr=false);
        //mod 20151217:e
        int calc(
            const ObCellArray &cell_array,
            ExpressionType part_type,//add wuna [MultiUps][sql_api] 20151217
            ObArray<ObString>& rule_param_list,
            ObObj &result);
        //add wuna [MultiUps][sql_api] 20151217:b
        int parse_element_values(const char* str,
                                 int &pos,
                                 ObObj* value,
                                 ObCellArray& cell_array,
                                 bool is_for_check_expr = false);
        int get_str_len(const char* str, const int pos);

        int get_column_num();
        int check_range_partition_value();
        int str_to_int(const char *str, const int &len, int64_t &value);
        bool str_is_int(const ObString str);

        //[416]
        bool str_is_str(ObString& str);
        int check_range_values_type(ObObj *range_value);

      private:
        int32_t column_num_;
        int32_t max_define_len_;
        ObArray<RangeItem*> range_values_;
        ObArray<ObObjType> values_type_;//[416]
    };
  }
}


#endif // OB_RANGE_EXPRESSION_H
