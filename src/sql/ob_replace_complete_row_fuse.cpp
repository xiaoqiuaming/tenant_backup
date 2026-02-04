/**
 * OB_REPLACE_COMPLETE_ROW_FUSE_CPP defined for replace stmt rows fuse in ups.
 *
 *
 * Version: $Id
 *
 * Authors:
 *   lijianqiang <lijianqiang@mail.nwpu.edu.cn>
 *     --some work detials if U want
 */

#include "ob_replace_complete_row_fuse.h"
#include "ob_physical_plan.h"

using namespace oceanbase;
using namespace sql;
ObReplaceCompleteRowFuse::ObReplaceCompleteRowFuse()
{
  input_values_subquery_ = OB_INVALID_ID;
  ignore_cell_index_ = 0;
}
ObReplaceCompleteRowFuse:: ~ObReplaceCompleteRowFuse()
{
}

void ObReplaceCompleteRowFuse::reset()
{
  insert_values_.reset();
  ignore_cell_index_ = 0;
  input_values_subquery_ = common::OB_INVALID_ID;
  ObSingleChildPhyOperator::reset();
}

void ObReplaceCompleteRowFuse::reuse()
{
  insert_values_.reuse();
  ignore_cell_index_ = 0;
  input_values_subquery_ = common::OB_INVALID_ID;
  ObSingleChildPhyOperator::reuse();
}

int ObReplaceCompleteRowFuse::open()
{
  int ret = OB_SUCCESS;
  const ObRowDesc *row_desc = NULL;
  const ObRowDesc *row_desc_with_flag = NULL;
  if (NULL == child_op_)
  {
    ret = OB_NOT_INIT;
  }
  else if (OB_SUCCESS != (ret = child_op_->open()))
  {
    YYSYS_LOG(WARN, "open child_op fail ret=%d %p", ret, child_op_);
  }
  else if (OB_SUCCESS != (ret = insert_values_.open()))
  {
    YYSYS_LOG(WARN, "open insert_values fail ret=%d", ret);
  }

  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = insert_values_.get_row_desc(row_desc)))
    {
      YYSYS_LOG(WARN, "get row desc fail:ret[%d]", ret);
    }
    else if (OB_SUCCESS != (ret = child_op_->get_row_desc(row_desc_with_flag)))
    {
      YYSYS_LOG(WARN, "get row desc fail:ret[%d]", ret);
    }
    else
    {
      cur_output_row_.set_row_desc(*row_desc);//out put row without action flag
      cur_fuse_row_.set_row_desc(*row_desc_with_flag);//the fuse result row need action flag
    }
  }

  if (OB_SUCCESS != ret)
  {
    insert_values_.close();
  }
  return ret;
}

int ObReplaceCompleteRowFuse::close()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (NULL == child_op_)
  {
    ret = OB_NOT_INIT;
  }
  else
  {
    if (OB_SUCCESS != (tmp_ret = insert_values_.close()))
    {
      YYSYS_LOG(WARN, "close insert_values fail ret=%d", tmp_ret);
    }
    if (OB_SUCCESS != (tmp_ret = child_op_->close()))
    {
      YYSYS_LOG(WARN, "close child_op fail ret=%d", tmp_ret);
    }
  }
  ret = (tmp_ret != OB_SUCCESS) ? tmp_ret : ret;
  return ret;
}

int ObReplaceCompleteRowFuse::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  const ObRow *input_row = NULL;
  const ObRow *inc_row = NULL;
  cur_fuse_row_.reset(false, ObRow::DEFAULT_NULL);
  ret = insert_values_.get_next_row(input_row);
  if (OB_SUCCESS != ret && OB_ITER_END != ret)
  {
    YYSYS_LOG(WARN, "fail to get input next row:ret[%d]", ret);
  }
  if (OB_SUCCESS == ret)
  {
    YYSYS_LOG(DEBUG, "input row is[%s]",to_cstring(*input_row));
    if (NULL == child_op_)
    {
      ret = OB_NOT_INIT;
    }
    else if (OB_SUCCESS != (ret = child_op_->get_next_row(inc_row)))
    {
      YYSYS_LOG(WARN, "fail to get increment add static next row:ret[%d]", ret);
      if (OB_ITER_END == ret)
      {
        ret = OB_ERROR;
      }
    }
  }
  if (OB_SUCCESS == ret)//get one row, fuse using the new stategy
  {
    YYSYS_LOG(DEBUG, "inc row is[%s]",to_cstring(*inc_row));
    if (OB_SUCCESS != (ret = fuse_replace_complete_row(inc_row, input_row, cur_fuse_row_)))
    {
      YYSYS_LOG(WARN, "fuse replace complete row failed, ret=%d",ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = convert_to_output_row(cur_fuse_row_, cur_output_row_)))
    {
      YYSYS_LOG(WARN, "convert to output row faield,ret=%d",ret);
    }
    else
    {
      row = &cur_output_row_;
      YYSYS_LOG(DEBUG, "current row is[%s]",to_cstring(*row));
    }
  }
  return ret;
}

int ObReplaceCompleteRowFuse::fuse_replace_complete_row(const ObRow *incr_row, const ObRow *input_row, ObRow &result)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  const ObObj *input_cell = NULL;
  const ObObj *inc_action_flag_cell = NULL;
  ObObj *result_cell = NULL;
  ObObj *result_action_flag_cell = NULL;

  if(NULL == incr_row || NULL == input_row)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(WARN, "incr_row[%p], input_row[%p]", incr_row, input_row);
  }
  else if (NULL == incr_row->get_row_desc() || NULL == input_row->get_row_desc())
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(WARN, "row desc[%p], result row desc[%p]",
      incr_row->get_row_desc(), input_row->get_row_desc());
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_INVALID_INDEX != incr_row->get_row_desc()->get_idx(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID))
    {
      if (OB_SUCCESS != (ret = incr_row->get_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, inc_action_flag_cell)))
      {
        YYSYS_LOG(WARN, "fail to get result action flag column:ret[%d]", ret);
      }
      else if (OB_SUCCESS != (ret = result.get_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, result_action_flag_cell)))
      {
        YYSYS_LOG(WARN, "fail to get result action flag column:ret[%d]", ret);
      }
    }

    else
    {
      ret = OB_ERROR;
      YYSYS_LOG(WARN, "inc data must have action flag column,please check!");
    }
  }
  if (OB_SUCCESS == ret)
  {
    OB_ASSERT(incr_row->get_column_num() == input_row->get_column_num() + 1);
    result.assign(*incr_row);
    if (ObActionFlag::OP_DEL_ROW == inc_action_flag_cell->get_ext() ||
        ObActionFlag::OP_ROW_DOES_NOT_EXIST == inc_action_flag_cell->get_ext())
    {
      if (OB_SUCCESS != (ret = result.reset(true, ObRow::DEFAULT_NULL)))
      {
        YYSYS_LOG(WARN, "fail to reset result row:ret[%d]", ret);
      }
    }
  }
  if (OB_SUCCESS == ret && 0 == ignore_cell_index_)
  {
    ret = OB_ERROR;
    YYSYS_LOG(ERROR, "ignore cll index not init,ret=%d",ret);
  }

  for (int64_t i = 0; OB_SUCCESS == ret && i < input_row->get_column_num(); i++)
  {
    if (OB_SUCCESS != (ret = input_row->raw_get_cell(i, input_cell, table_id, column_id)))
    {
      YYSYS_LOG(WARN, "fail to get ups cell:ret[%d]", ret);
    }
    else if (OB_SUCCESS != (ret = result.get_cell(table_id, column_id, result_cell)))
    {
      YYSYS_LOG(WARN, "fail to get result cell:ret[%d]", ret);
    }
    else
    {
      if (i < ignore_cell_index_)
      {
        if (OB_SUCCESS != (ret = result_cell->apply(*input_cell)))
        {
          YYSYS_LOG(WARN, "fail to apply ups cell to result cell:ret[%d]", ret);
        }
        else
        {
          result_action_flag_cell->set_ext(ObActionFlag::OP_VALID);
        }
      }
      else
      {
        YYSYS_LOG(DEBUG, "null input cell, skip,do nothing,ignore_cell_index=[%ld]",ignore_cell_index_);
        break;
      }
    }
  }
  return ret;
}
int ObReplaceCompleteRowFuse::convert_to_output_row(ObRow& input_row, ObRow& output_row)
{
  int ret = OB_SUCCESS;
  const ObObj *cell = NULL;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t column_id = OB_INVALID_ID;
  int64_t column_num = OB_INVALID_INDEX;
  if (OB_SUCCESS != (ret = output_row.reset(false,ObRow::DEFAULT_NULL)))
  {
    YYSYS_LOG(WARN, "reset out put row failed, ret=%d",ret);
  }
  else if (-1 == (column_num = input_row.get_column_num()))
  {
    ret = OB_ERROR;
    YYSYS_LOG(ERROR, "get column num failed, ret=%d",ret);
  }
  else
  {
    for (int64_t i=0; OB_SUCCESS == ret && i<column_num; i++)
    {
      if (OB_SUCCESS != (ret = input_row.raw_get_cell(i, cell, table_id, column_id)))
      {
        YYSYS_LOG(WARN, "fail to get ups cell:ret[%d]", ret);
      }
      else
      {
        if (OB_ACTION_FLAG_COLUMN_ID != column_id)
        {
          if (OB_SUCCESS != (ret = output_row.set_cell(table_id, column_id, *cell)))
          {
            YYSYS_LOG(WARN, "raw set cell failed,ret=%d",ret);
          }
        }
      }
    }
  }
  if (OB_SUCCESS == ret)
  {
    YYSYS_LOG(DEBUG, "current out put row is[%s]",to_cstring(output_row));
  }
  return ret;
}

int ObReplaceCompleteRowFuse::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = insert_values_.get_row_desc(row_desc)))
  {
    YYSYS_LOG(WARN, "fail to get  row desc from child:ret[%d]", ret);
  }
  return ret;
}

int64_t ObReplaceCompleteRowFuse::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ObReplaceCompleteRowFuse(input_values_subquery=%lu values=", input_values_subquery_);
  pos += insert_values_.to_string(buf+pos, buf_len-pos);
  databuff_printf(buf, buf_len, pos, ")\n");
  if (NULL != child_op_)
  {
    pos += child_op_->to_string(buf+pos, buf_len-pos);
  }
  return pos;
}

PHY_OPERATOR_ASSIGN(ObReplaceCompleteRowFuse)
{
  int ret = OB_SUCCESS;
  cur_fuse_row_.reset(false, ObRow::DEFAULT_NULL);
  cur_output_row_.reset(false, ObRow::DEFAULT_NULL);
  CAST_TO_INHERITANCE(ObReplaceCompleteRowFuse);
  reset();
  input_values_subquery_ = o_ptr->input_values_subquery_;
  return ret;
}

DEFINE_SERIALIZE(ObReplaceCompleteRowFuse)
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
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, ignore_cell_index_)))
    {
      YYSYS_LOG(WARN, "fail to encode child_num_:ret[%d]", ret);
    }
  }
  return ret;
}

DEFINE_DESERIALIZE(ObReplaceCompleteRowFuse)
{
  int ret = OB_SUCCESS;
   if (OB_SUCCESS != (ret = insert_values_.deserialize(buf, data_len, pos)))
   {
     YYSYS_LOG(WARN, "deserialize insert_values_ failed, ret=%d",ret);
   }
   else if(OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &ignore_cell_index_)))
   {
     YYSYS_LOG(WARN, "deserialize ignore_cell_index_ failed,ret=%d",ret);
   }
   return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObReplaceCompleteRowFuse)
{
  return (insert_values_.get_serialize_size() + serialization::encoded_length_vi64(ignore_cell_index_));
}

namespace oceanbase
{
  namespace sql
  {
    REGISTER_PHY_OPERATOR(ObReplaceCompleteRowFuse, PHY_REPLACE_COMPLETE_ROW_FUSE);
  }
}


