/**
 * OB_REPLACE_SEMANTIC_FILTER_CPP defined for replace stmt pre_execution_plan in ups.
 *
 *
 * Version: $Id
 *
 * Authors:
 *   lijianqiang <lijianqiang@mail.nwpu.edu.cn>
 *     --some work detials if U want
 */

#include "ob_replace_semantic_filter.h"
#include "ob_physical_plan.h"

using namespace oceanbase;
using namespace sql;

ObReplaceSemanticFilter::ObReplaceSemanticFilter():
  input_values_subquery_(OB_INVALID_ID),
  could_replace_(false)
{
}
ObReplaceSemanticFilter:: ~ObReplaceSemanticFilter()
{
}

void ObReplaceSemanticFilter::reset()
{
  insert_values_.reset();
  raw_row_desc_.reset();//do not forget, the op is resued
  input_values_subquery_ = common::OB_INVALID_ID;
  could_replace_ = false;
  ObSingleChildPhyOperator::reset();
}

void ObReplaceSemanticFilter::reuse()
{
  insert_values_.reuse();
  raw_row_desc_.reset();
  input_values_subquery_ = common::OB_INVALID_ID;
  could_replace_ = false;
  ObSingleChildPhyOperator::reuse();
}

int ObReplaceSemanticFilter::open()
{
  int ret = OB_SUCCESS;
  could_replace_ = false;
  const ObRow *row = NULL;
  cur_row_.set_row_desc(raw_row_desc_);
  if (NULL == child_op_)
  {
    ret = OB_NOT_INIT;
  }
  //open the op ObIncompleteRowFilter
  else if (OB_SUCCESS != (ret = child_op_->open()))
  {
    YYSYS_LOG(WARN, "open child_op fail ret=%d %p", ret, child_op_);
  }
  else if (OB_SUCCESS != (ret = insert_values_.open()))
  {
    YYSYS_LOG(WARN, "open insert_values fail ret=%d", ret);
  }
  else
  {
    if (OB_SUCCESS != (ret = child_op_->get_next_row(row)))
    {
      if (OB_ITER_END != ret)
      {
        if (OB_INCOMPLETE_ROW == ret)
        {
          YYSYS_LOG(INFO, "incomplete row for pre plan,do nothing,try full row replace plan,ret=%d",ret);
        }
        else if (!IS_SQL_ERR(ret))
        {
          YYSYS_LOG(ERROR, "child_op get_next_row fail but not OB_ITER_END or OB_INCOMPLETE_ROW, ret=%d", ret);
        }
      }
      else
      {
        YYSYS_LOG(DEBUG, "iterate all full rows  from child_op=%p type=%d will replace rows", child_op_, child_op_->get_type());
        could_replace_ = true;
        ret = OB_SUCCESS;
      }
    }
    else
    {
      ret = OB_ERROR;
      YYSYS_LOG(ERROR, "shoudn't return one row for pre plan replace stmt,ret=%d",ret);
    }
  }
  if (OB_SUCCESS != ret)
  {
    insert_values_.close();
  }
  return ret;
}

int ObReplaceSemanticFilter::close()
{
  int ret = OB_SUCCESS;
  if (NULL == child_op_)
  {
    ret = OB_NOT_INIT;
  }
  else
  {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = insert_values_.close()))
    {
      YYSYS_LOG(WARN, "close insert_values fail ret=%d", tmp_ret);
    }
    if (OB_SUCCESS != (tmp_ret = child_op_->close()))
    {
      YYSYS_LOG(WARN, "close child_op fail ret=%d", tmp_ret);
    }
  }
  return ret;
}

int ObReplaceSemanticFilter::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  const common::ObRow *tem_row;
  if (!could_replace_)
  {
    ret = OB_ITER_END;
  }
  else
  {
    ret = insert_values_.get_next_row(tem_row);
  }

  if (OB_SUCCESS != ret && OB_ITER_END != ret)
  {
    YYSYS_LOG(WARN, "fail to get input next row:ret[%d]", ret);
  }
  if (OB_SUCCESS == ret)
  {
    //cut the null cell we added for get the full row info.
    if (OB_SUCCESS != (ret = cut_null_cell(tem_row)))
    {
       YYSYS_LOG(WARN, "fail to get cut null cell,ret[%d]", ret);
    }
    else
    {
      row = &cur_row_;
      YYSYS_LOG(DEBUG, "the replace row is=%s",to_cstring(cur_row_));
    }
  }
  return ret;
}

int ObReplaceSemanticFilter::cut_null_cell(const common::ObRow *& row)
{
  int ret = OB_SUCCESS;
  const ObObj * cell;
  if (NULL == row)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR, "the row is not init,ret=%d",ret);
  }
  else
  {
    uint64_t raw_column_num = raw_row_desc_.get_column_num();
    for (uint64_t i=0; OB_SUCCESS == ret && i<raw_column_num; i++)
    {
      if (OB_SUCCESS != (ret = row->raw_get_cell(i, cell)))
      {
        YYSYS_LOG(WARN, "raw get cell from input values failed,ret=%d",ret);
      }
      else if (OB_SUCCESS != (ret = cur_row_.raw_set_cell(i, *cell)))
      {
        YYSYS_LOG(WARN, "raw set cell to cur row failed, ret=%d",ret);
      }
    }
  }
  return ret;
}

int ObReplaceSemanticFilter::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  row_desc = &raw_row_desc_;
  return ret;
}

int64_t ObReplaceSemanticFilter::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ObReplaceSemanticFilter(input_values_subquery=%lu values=", input_values_subquery_);
  pos += insert_values_.to_string(buf+pos, buf_len-pos);
  databuff_printf(buf, buf_len, pos, ")\n");
  if (NULL != child_op_)
  {
    pos += child_op_->to_string(buf+pos, buf_len-pos);
  }
  return pos;
}

PHY_OPERATOR_ASSIGN(ObReplaceSemanticFilter)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObReplaceSemanticFilter);
  reset();
  input_values_subquery_ = o_ptr->input_values_subquery_;
  could_replace_ = o_ptr->could_replace_;
  return ret;
}

DEFINE_SERIALIZE(ObReplaceSemanticFilter)
{
  int ret = OB_SUCCESS;
  ObExprValues *input_values = NULL;
  if (NULL == (input_values = dynamic_cast<ObExprValues*>(my_phy_plan_->get_phy_query_by_id(input_values_subquery_))))
  {
    YYSYS_LOG(ERROR, "invalid expr_values, subquery_id=%lu", input_values_subquery_);
  }
  else
  {
    if (OB_SUCCESS != (ret = input_values->serialize(buf, buf_len, pos)))
    {
      YYSYS_LOG(WARN, "serialize input values failed, ret=%d",ret);
    }
    if (OB_SUCCESS != (ret = raw_row_desc_.serialize(buf, buf_len, pos)))
    {
      YYSYS_LOG(WARN, "serialize row_desc fail ret=%d buf=%p buf_len=%ld pos=%ld", ret, buf, buf_len, pos);
    }
  }
  return ret;
}

DEFINE_DESERIALIZE(ObReplaceSemanticFilter)
{
  int ret = OB_SUCCESS;
   if (OB_SUCCESS != (ret = insert_values_.deserialize(buf, data_len, pos)))
   {
     YYSYS_LOG(WARN, "deserialize insert_values_ failed, ret=%d",ret);
   }
   else if (OB_SUCCESS != (ret = raw_row_desc_.deserialize(buf, data_len, pos)))
   {
     YYSYS_LOG(WARN, "deserialize row_desc fail ret=%d buf=%p data_len=%ld pos=%ld", ret, buf, data_len, pos);
   }
   return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObReplaceSemanticFilter)
{
  return (insert_values_.get_serialize_size() + raw_row_desc_.get_serialize_size());
}

namespace oceanbase
{
  namespace sql
  {
    REGISTER_PHY_OPERATOR(ObReplaceSemanticFilter, PHY_REPLACE_SEMANTIC_FILTER);
  }
}

