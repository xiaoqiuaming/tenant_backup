/**
 * OB_UPDATE_SEMANTIC_FILTER_CPP defined for update stmt pre_execution_plan in ups.
 *
 *
 * Version: $Id
 *
 * Authors:
 *   lijianqiang <lijianqiang@mail.nwpu.edu.cn>
 *     --some work detials if U want
 */


#include "ob_update_semantic_filter.h"
#include "common/ob_row_fuse.h"

using namespace oceanbase;
using namespace sql;
using namespace common;

ObUpdateSemanticFilter::ObUpdateSemanticFilter():
  could_update_(false),
  child_num_(0)
{
}

ObUpdateSemanticFilter:: ~ObUpdateSemanticFilter()
{
}

void ObUpdateSemanticFilter::reset()
{
  could_update_ = false;
  child_num_ = 0;
  ObMultiChildrenPhyOperator::reset();
}

void ObUpdateSemanticFilter::reuse()
{
  could_update_ = false;
  child_num_ = 0;
  ObMultiChildrenPhyOperator::reuse();
}


int ObUpdateSemanticFilter::set_child(int32_t child_idx, ObPhyOperator &child_operator)
{

  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = ObMultiChildrenPhyOperator::set_child(child_idx, child_operator)))
  {
    YYSYS_LOG(WARN, "set child of update semantic filter failed, ret=%d",ret);
  }
  else if (child_num_ < child_idx + 1)
  {
    child_num_ = child_idx + 1;
  }
  return ret;
}

int32_t ObUpdateSemanticFilter::get_child_num() const
{
  return child_num_;
}

int ObUpdateSemanticFilter::open()
{
  int ret = OB_SUCCESS;
  if (NULL == get_child(0))
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR, "the first child of updateSmanticFilter is not init,ret=%d",ret);
  }
  //open the op ObIncompleteRowFilter
  else if (OB_SUCCESS != (ret = get_child(0)->open()))
  {
    YYSYS_LOG(WARN, "open child_op fail ret=%d %p", ret, get_child(0));
  }
  else
  {
    const ObRow *row = NULL;
    if (OB_SUCCESS != (ret = get_child(0)->get_next_row(row)))
    {
      if (OB_ITER_END != ret)
      {
        if (OB_INCOMPLETE_ROW == ret)
        {
          YYSYS_LOG(INFO, "incomplete row for pre plan,do nothing,try full row update plan,ret=%d",ret);
        }
        else if (!IS_SQL_ERR(ret))
        {
          YYSYS_LOG(ERROR, "child_op get_next_row fail but not OB_ITER_END or OB_INCOMPLETE_ROW, ret=%d", ret);
        }
      }
      else
      {
        YYSYS_LOG(DEBUG, "iterate all full rows  from child_op=%p type=%d will update rows", get_child(0), get_child(0)->get_type());
        could_update_ = true;
        ret = OB_SUCCESS;
      }
    }
    else
    {
      ret = OB_ERROR;
      YYSYS_LOG(ERROR, "shoudn't return one row for pre plan stmt,ret=%d",ret);
    }
  }

  //can update directly, open the lock inc scan
  if (OB_SUCCESS == ret && could_update_)
  {
    if (OB_SUCCESS != (ret = get_child(1)->open()))
    {
      YYSYS_LOG(WARN, "open child_op inc scan for lock failed ret=%d %p", ret, get_child(1));
    }
  }

  if (NULL != get_child(0))
  {
    get_child(0)->close();
  }
  return ret;
}

int ObUpdateSemanticFilter::close()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = ObMultiChildrenPhyOperator::close()))
  {
    YYSYS_LOG(WARN, "close child_op fail ret=%d", ret);
  }
  return ret;
}

int ObUpdateSemanticFilter::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  bool is_complete_row = false;
  bool is_empty_row = false;
  while (OB_SUCCESS == ret)
  {
    if (!could_update_)
    {
      ret = OB_ITER_END;
    }
    else
    {
      ret = get_child(1)->get_next_row(row);
    }
    if (OB_SUCCESS != ret && OB_ITER_END != ret)
    {
      YYSYS_LOG(WARN, "fail to get input next row:ret[%d]", ret);
    }
    if (OB_SUCCESS == ret)
    {
      YYSYS_LOG(DEBUG, "update semantic next row is[%s]",to_cstring(*row));
      //if come here,the rows must be complete,we need to judge if the row is empty or not!
      if (OB_SUCCESS != (ret = common::ObRowFuse::get_is_complete_row(row, is_complete_row, is_empty_row)))
      {
        YYSYS_LOG(WARN, "fail to get is empty,ret[%d]", ret);
      }
      else if (!is_empty_row)
      {
        //get one,return
        break;
      }
      else
      {
        //empty row,continue
        continue;
      }
    }
  }
  return ret;
}

int ObUpdateSemanticFilter::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (NULL != get_child(0))
  {
    ret = get_child(0)->get_row_desc(row_desc);
  }
  else
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR, "child_op_ is NULL");
  }
  return ret;
}

int64_t ObUpdateSemanticFilter::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ObUpdateSemanticFilter(child num =%d)\n",get_child_num());
  for (int32_t i = 0; i<get_child_num(); i++)
  {
    if (NULL != get_child(i))
    {
      databuff_printf(buf, buf_len, pos, "child:%d\n",i);
      pos += get_child(i)->to_string(buf+pos, buf_len-pos);
    }
  }
  return pos;
}

PHY_OPERATOR_ASSIGN(ObUpdateSemanticFilter)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObUpdateSemanticFilter);
  reset();
  could_update_ = o_ptr->could_update_;
  child_num_ = o_ptr->child_num_;
  children_ops_ = o_ptr->children_ops_;
  return ret;
}

DEFINE_SERIALIZE(ObUpdateSemanticFilter)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, pos, child_num_)))
  {
    YYSYS_LOG(WARN, "fail to encode child_num_:ret[%d]", ret);
  }
  return ret;
}

DEFINE_DESERIALIZE(ObUpdateSemanticFilter)
{
  int ret = OB_SUCCESS;

  if (OB_SUCCESS != (ret = serialization::decode_vi32(buf, data_len, pos, &child_num_)))
  {
    YYSYS_LOG(WARN, "fail to decode child_num_:ret[%d]", ret);
  }
   return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObUpdateSemanticFilter)
{
  return (serialization::encoded_length_vi32(child_num_));
}

namespace oceanbase
{
  namespace sql
  {
    REGISTER_PHY_OPERATOR(ObUpdateSemanticFilter, PHY_UPDATE_SEMANTIC_FILTER);
  }
}


