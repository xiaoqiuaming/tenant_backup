/*
*   Version: 0.1
*
*   Authors:
*      liu jun
*        - some work details if you want
*/

#include "ob_calc_expression.h"

using namespace oceanbase::common;
/*mod by wuna [MultiUps] [sql_api] 20151228:b*/
//ObCalcExpression::ObCalcExpression(FIFOAllocator &allocator) : allocator_(allocator)
ObCalcExpression::ObCalcExpression()
/*mod 20151228:e*/
{
}

ObCalcExpression::~ObCalcExpression()
{
  allocator_.free();
}
