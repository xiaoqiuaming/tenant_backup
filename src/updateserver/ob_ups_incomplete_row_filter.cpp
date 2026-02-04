/**
 * OB_UPS_INCOMPLETE_ROW_FILTER_CPP defined for replace stmt and update stmt,
 * when execute the pre_execution physical plan in UPS, this operator will
 * check the row is complete or not.
 *
 * Version: $Id
 *
 * Authors:
 *   lijianqiang <lijianqiang@mail.nwpu.edu.cn>
 *     --some work detials if U want
 */


#include "ob_ups_incomplete_row_filter.h"
#include "common/ob_row_fuse.h"

using namespace oceanbase;
using namespace updateserver;


ObUpsIncompleteRowFilter::ObUpsIncompleteRowFilter():table_mgr_(NULL)
{
}

ObUpsIncompleteRowFilter:: ~ObUpsIncompleteRowFilter()
{
}

void ObUpsIncompleteRowFilter::reset()
{
  sql::ObIncompleteRowFilter::reset();
}

void ObUpsIncompleteRowFilter::reuse()
{
  sql::ObIncompleteRowFilter::reuse();
}

int ObUpsIncompleteRowFilter::open()
{
  int ret = OB_SUCCESS;
  if (NULL == child_op_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR, "child op is not init,ret=%d",ret);
  }
  else if (OB_SUCCESS != (ret = child_op_->open()))
  {
    YYSYS_LOG(WARN, "open child_op fail ret=%d child=%p", ret, child_op_);
  }
  else if (OB_SUCCESS != (ret = check_schema_validity()))
  {
    YYSYS_LOG(WARN, "check schema validity failed,can't cons complete rows,ret=%d",ret);
  }
  return ret;
}

int ObUpsIncompleteRowFilter::check_schema_validity()
{
  /**
   * check_schema_validity is necessary, we cos complete row in ups for pre execution
   * plan, if schema info changed(U alter the table schema info),the complete row info
   * can't be sure any more,the pre plan can't be execute any more,just return error
   * info to MS.
   */
  int ret = OB_SUCCESS;
  if (NULL == table_mgr_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR, "the table mgr is not init,ret=%d",ret);
  }
  else
  {
    int32_t column_size = 0;
    uint64_t tid = OB_INVALID_ID;
    uint64_t cid = OB_INVALID_ID;
    const ObColumnSchemaV2* column = NULL;
    const common::ObRowDesc *row_desc = NULL;
    UpsSchemaMgrGuard sm_guard;
    const CommonSchemaManager *sm = NULL;
    if (NULL == (sm = table_mgr_->get_schema_mgr().get_schema_mgr(sm_guard)))
    {
      YYSYS_LOG(WARN, "get_schema_mgr fail");
      ret = OB_SCHEMA_ERROR;
    }
    else if (OB_SUCCESS != (ret = this->get_row_desc(row_desc)))
    {
      YYSYS_LOG(WARN, "get row desc failed,ret=%d",ret);
    }
    else if (OB_SUCCESS != (ret = row_desc->get_tid_cid(0, tid, cid)))
    {
      YYSYS_LOG(WARN, "get_tid_cid from row_desc fail ret=%d",ret);
    }
    else if (NULL == (column = sm->get_table_schema(tid, column_size)))
    {
      ret = OB_SCHEMA_ERROR;
      YYSYS_LOG(ERROR, "get schema err,the table id=%ld,ret=%d", tid, ret);
    }
    //mod hongchen [HOT_UPDATE_BUG_FIX] 20170719:b
    /*
    else if (row_desc->get_column_num() != (int32_t)(column_size + 1))
    {
      ret = OB_SCHEMA_ERROR;
      YYSYS_LOG(ERROR, "the schema num[%ld] is not equal the column num[%d]",row_desc->get_column_num(), column_size);
    }
    */
    else
    {
      tid = column->get_table_id();
      for (int32_t index = 0; index < column_size; ++index)
      {
        cid = column->get_id();
        ++column;
        if (OB_APP_MIN_COLUMN_ID > cid)
        {
          continue;
        }
        else if (OB_INVALID_INDEX == row_desc->get_idx(tid,cid))
        {
          ret = OB_SCHEMA_ERROR;
          YYSYS_LOG(ERROR, "row desc should include column<%lu,%lu>, but not.",tid,cid);
          break;
        }
      }
    }
    //mod hongchen [HOT_UPDATE_BUG_FIX] 20170719:e
  }
  return ret;
}

int ObUpsIncompleteRowFilter::close()
{
  int ret = OB_SUCCESS;
  if (NULL == child_op_)
  {
    ret = OB_NOT_INIT;
  }
  else
  {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = child_op_->close()))
    {
      YYSYS_LOG(WARN, "close child_op fail ret=%d", tmp_ret);
    }
  }
  return ret;
}

int ObUpsIncompleteRowFilter::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  bool is_complete_row = false;
  bool is_empty_row = false;
  bool is_static_date = true;// add by maosy [MultiUps 1.0] [#137] 20170426 
  while (OB_SUCCESS == ret)
  {
    if (NULL == child_op_)
    {
      ret = OB_NOT_INIT;
    }
    else if (OB_SUCCESS != (ret = child_op_->get_next_row(row)))
    {
      if (OB_ITER_END == ret)
      {
// add by maosy [MultiUps 1.0] [#137] 20170426 b
          if(is_static_date)
          {
              ret = OB_INCOMPLETE_ROW ;
              YYSYS_LOG(INFO,"ups has no date ,all data is static ,should exe full row plan");
          }
// add by maosy  20170426 
        break;
      }
      else
      {
        YYSYS_LOG(WARN, "fail to get input next row:ret[%d]", ret);
      }
    }
    else if (OB_SUCCESS != (ret = common::ObRowFuse::get_is_complete_row(row, is_complete_row, is_empty_row)))
    {
      YYSYS_LOG(WARN, "get is complete row failed,ret=%d",ret);
    }
    if (OB_SUCCESS == ret)
    {
        is_static_date = false ;// add by maosy [MultiUps 1.0] [#137] 20170426 
      if (!is_complete_row)
      {
        ret = OB_INCOMPLETE_ROW;
        YYSYS_LOG(INFO, "in complete row for update or replace stmt,ret=%d",ret);
        break;
      }
      else
      {
        continue;
      }
    }
  }
  return ret;
}

int ObUpsIncompleteRowFilter::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (NULL != child_op_)
  {
    ret = child_op_->get_row_desc(row_desc);
  }
  else
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR, "child_op_ is NULL");
  }
  return ret;
}

int64_t ObUpsIncompleteRowFilter::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ObUpsIncompleteRowFilter(");
  databuff_printf(buf, buf_len, pos, ")\n");
  pos += sql::ObIncompleteRowFilter::to_string(buf + pos, buf_len - pos);
  return pos;
}






