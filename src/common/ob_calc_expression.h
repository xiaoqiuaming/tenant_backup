/*
*   Version: 0.1
*
*   Authors:
*      liu jun
*        - some work details if you want
*/

#ifndef OB_CALC_EXPRESSION_H
#define OB_CALC_EXPRESSION_H

#include "ob_object.h"
#include "page_arena.h"
#include "ob_array.h"
#include "ob_cell_array.h"
#include "hash/ob_hashmap.h"
#include "ob_fifo_allocator.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace common
  {
    class ObCalcExpression
    {
    public:
      enum ExpressionType
      {
        INVALID_EXPRESSION_TYPE = -1,
        HASH = 0,
        RANGE,
        ENUM,
        LIST, //add by wuna [MultiUps] [sql_api] 20151217
        CRANGE, //add for [range function]
        HARDCODE //add for [577-hardcode partition]
      };

    public:
     /* mod by wuna [MultiUps] [sql_api] 20151228:b*/
     // ObCalcExpression(FIFOAllocator &allocator);
     ObCalcExpression();
      virtual ~ObCalcExpression();
      //virtual int set_expression(const ObString &expr) = 0;
      virtual int set_expression(const ObString &expr,ObArray<ObString> &group_name_list,ObCellArray& cell_array,
                                 bool is_for_check_expr=false) = 0;
      //virtual int calc(
        //  const ObCellArray &cell_array,
          //const hash::ObHashMap<ObString,int64_t,hash::NoPthreadDefendMode> &cname_to_idx_map,
          //ObObj &result) = 0;
       virtual int calc(
          const ObCellArray &cell_array,
          ExpressionType part_type,
          ObArray<ObString>& rule_param_list,
          ObObj &result) = 0;
      static const int32_t MAX_COLUMN_NUM = 16;
    protected:
      //FIFOAllocator &allocator_;
      PageArena<char> allocator_;
      /* mod 20151228:e*/
    };
  }
}
#endif // OB_CALC_EXPRESSION_H
