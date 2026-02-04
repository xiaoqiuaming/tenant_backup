/**
 * OB_INCOMPLETE_ROW_FILTER_CPP defined for replace stmt and update stmt,
 * when execute the pre_execution physical plan in UPS, this operator will
 * check the row is complete or not.
 *
 * Version: $Id
 *
 * Authors:
 *   lijianqiang <lijianqiang@mail.nwpu.edu.cn>
 *     --some work detials if U want
 */


#include "ob_incomplete_row_filter.h"

using namespace oceanbase;
using namespace sql;

ObIncompleteRowFilter::ObIncompleteRowFilter()
{
}

ObIncompleteRowFilter:: ~ObIncompleteRowFilter()
{
}

void ObIncompleteRowFilter::reset()
{
  ObSingleChildPhyOperator::reset();
}

void ObIncompleteRowFilter::reuse()
{
  ObSingleChildPhyOperator::reuse();
}

int64_t ObIncompleteRowFilter::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ObIncompleteRowFilter(");
  databuff_printf(buf, buf_len, pos, ")\n");
  if (NULL != child_op_)
  {
    pos += child_op_->to_string(buf+pos, buf_len-pos);
  }
  return pos;
}

PHY_OPERATOR_ASSIGN(ObIncompleteRowFilter)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObIncompleteRowFilter);
  reset();
  UNUSED(o_ptr);
  return ret;
}

DEFINE_SERIALIZE(ObIncompleteRowFilter)
{
  int ret = OB_SUCCESS;
  UNUSED(buf);
  UNUSED(buf_len);
  UNUSED(pos);
  return ret;
}

DEFINE_DESERIALIZE(ObIncompleteRowFilter)
{
  int ret = OB_SUCCESS;
  UNUSED(buf);
  UNUSED(data_len);
  UNUSED(pos);
   return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObIncompleteRowFilter)
{
  return (0);
}

namespace oceanbase
{
  namespace sql
  {
    REGISTER_PHY_OPERATOR(ObIncompleteRowFilter, PHY_INCOMPLETE_ROW_FILTER);
  }
}




