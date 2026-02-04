#ifndef OB_HARDCODE_EXPRESSION_H
#define OB_HARDCODE_EXPRESSION_H

#include "ob_calc_expression.h"
#include "ob_array.h"
#include "hash/ob_hashmap.h"

namespace oceanbase
{
  namespace common
  {
    class ObHardCodeExpression : public ObCalcExpression
    {
      public:
        ObHardCodeExpression();
        ~ObHardCodeExpression();

        int set_expression(const ObString &expr, ObArray<ObString> &group_name_list,
                           ObCellArray &cell_array, bool is_for_check_expr = false);
        int calc(
            const ObCellArray &cell_array,
            ExpressionType part_type,
            ObArray<ObString> &rule_param_list,
            ObObj &result);

        int32_t get_column_num();

        int match(ObString orig, char *result_char);

      private:
        int32_t column_num_;
        int32_t default_group_num_;
        hash::ObHashMap<const char *, int32_t> account_cell_map_;
        hash::ObHashMap<const char *, int32_t> card_cell_map_;
        int64_t hardcode_type_;

    };
  }
}

#endif // OB_HARDCODE_EXPRESSION_H
