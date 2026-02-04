/*
*   Version: 0.1
*
*   Authors:
*      liu jun
*        - some work details if you want
*/

#ifndef OB_HASH_EXPRESSION_H
#define OB_HASH_EXPRESSION_H

#include "ob_calc_expression.h"
#include "ob_postfix_expression.h"

namespace oceanbase
{
  namespace common
  {
    class ObHashExpression : public ObCalcExpression
    {
      public:
        //mod wuna [MultiUps][sql_api] 20151217:b
        //ObHashExpression(FIFOAllocator &allocator);
        ObHashExpression();
        ~ObHashExpression();

        //int set_expression(const ObString &expr);
        int set_expression(const ObString &expr,ObArray<ObString> &group_name_list,
                           ObCellArray& cell_array,bool is_for_check_expr=false);
        //mod 20151217:e
        //add wuna [MultiUps][sql_api] 20160114:b
        int build_param_to_idx_map(
            const ObArray<ObString> &rule_param_list,
            hash::ObHashMap<ObString, int64_t,hash::NoPthreadDefendMode> &param_to_idx) const;
        int is_valid_hash_operator(int64_t value);
        //add 20160114:e
        int calc(
            const ObCellArray &cell_array,
            ExpressionType part_type,//add wuna [MultiUps][sql_api] 20151217
            ObArray<ObString>& rule_param_list,
            ObObj &result);
      private:
        static op_call_func_t call_func[ObExpression::MAX_FUNC];
        ObObj expr_[OB_MAX_COMPOSITE_SYMBOL_COUNT];
        ObExprObj stack_i_[OB_MAX_COMPOSITE_SYMBOL_COUNT];
        int postfix_size_;
        const char* hash_mod_expr_;
    };
  }
}
#endif // OB_HASH_EXPRESSION_H
