/**
 * ob_row_merger.cpp defined to merge all rows with same rowkey(different row version)
 * or different rowkey, in brief, merge data in row level.
 *
 * Version: $Id
 *
 * Authors:
 *   lijianqiang <lijianqiang@mail.nwpu.edu.cn>
 *     --some work detials if U want
 */

#include "ob_row_merger.h"

using namespace oceanbase;
using namespace common;

ObRowMerger::ObRowMerger()
{
  reset();
}

ObRowMerger::~ObRowMerger()
{
  reset();
}

int ObRowMerger::next_row()
{
  return OB_NOT_IMPLEMENT;
}

int ObRowMerger::get_row(const ObRowkey *&rowkey, const ObRow *&row)
{
  UNUSED(rowkey);
  UNUSED(row);
  YYSYS_LOG(ERROR, "not implement");
  return OB_NOT_IMPLEMENT;
}

int ObRowMerger::get_row(const ObRowkey *&rowkey, const ObRow *&row, int64_t& row_version)
{
  UNUSED(rowkey);
  UNUSED(row);
  UNUSED(row_version);
  YYSYS_LOG(ERROR, "not implement");
  return OB_NOT_IMPLEMENT;
}

int ObRowMerger::get_row(const ObRowkey *&rowkey, const ObRow *&row, int64_t& row_version,const ObRowVersion *&version)
{
  UNUSED(rowkey);
  UNUSED(row);
  UNUSED(row_version);
  UNUSED(version);
  YYSYS_LOG(ERROR, "not implement");
  return OB_NOT_IMPLEMENT;
}

void ObRowMerger::reset()
{
  row_iter_num_ = 0;
  cur_rowkey_   = NULL;
  result_row_.clear();
  row_version_.reset();
  str_buf_.reset();
  cur_sstable_rowkey_ = NULL;
  sstable_rowkey_buf_.reset();
}

void ObRowMerger::set_merge_row_desc(const ObRowDesc &row_desc)
{
  result_row_.set_row_desc(row_desc);
}

int ObRowMerger::add_row_iterator(ObRowMergeIterator * row_iter)
{
  int ret = OB_SUCCESS;
  if (NULL == row_iter)
  {
    YYSYS_LOG(WARN, "invalid param, row_iter=%p", row_iter);
    ret = OB_INVALID_ARGUMENT;
  }
  else if (uint64_t(row_iter_num_) >= OB_UPS_MAX_MINOR_VERSION_NUM)
  {
    YYSYS_LOG(WARN, "exceeds limit, iter_num_=%ld", row_iter_num_);
    ret = OB_ERROR;
  }
  else
  {
    row_iter_status_[row_iter_num_] = BEFORE_NEXT;
    row_iters_[row_iter_num_] = row_iter;
    ++row_iter_num_;
    YYSYS_LOG(DEBUG, "row iter num is[%ld]",row_iter_num_);
  }
  return ret;
}

//[693]
int ObRowMerger::set_cur_sstable_rowkey(const ObRowkey *&rowkey)
{
    int ret = OB_SUCCESS;
    sstable_rowkey_buf_.reset();

    void *buff = NULL;
    if(OB_SUCCESS != ret || NULL == rowkey)
    {

    }
    else if(NULL == (buff = sstable_rowkey_buf_.alloc(sizeof(ObRowkey))))
    {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        YYSYS_LOG(WARN, "fail to alloc mem for rowkey, ret=%d", ret);
    }

    cur_sstable_rowkey_ = new(buff) ObRowkey();
    if(ret == OB_SUCCESS)
    {
        if(OB_SUCCESS != (ret = rowkey->deep_copy(*cur_sstable_rowkey_, sstable_rowkey_buf_)))
        {
            YYSYS_LOG(WARN, "fail to deep copy rowkey, ret=%d", ret);
        }
    }
    if(ret != OB_SUCCESS)
    {
        sstable_rowkey_buf_.reset();
        cur_sstable_rowkey_ = NULL;
    }
    return ret;
}

int ObRowMerger::get_next_row(const common::ObRowkey *&rowkey, const common::ObRow *&row)
{
  int     ret = OB_SUCCESS;
  int64_t row_iter_end_num = 0;
  int64_t cur_row_data_version = OB_INVALID_VERSION;
  
  const ObRow *cur_row = NULL;
  const ObRowkey *min_rowkey = NULL;
  const ObRowkey *cur_rowkey  = NULL;
  const ObRowVersion *row_version_v2 = NULL;
  const ObRowkey *match_rowkey = NULL;
  
  const ObRowVersion* cur_iter_arr_row_version[MAX_UPS_COUNT_ONE_CLUSTER];
  const ObRow*        cur_iter_arr_row[MAX_UPS_COUNT_ONE_CLUSTER];
  int64_t             cur_iter_arr[MAX_UPS_COUNT_ONE_CLUSTER];
  int64_t             cur_iter_arr_idx = 0;
  
  //firstly, acquire and save info
  for(int64_t i = 0; OB_SUCCESS == ret && i < row_iter_num_; i++)
  {
    ret = this->next_row_(i);
    if(OB_ITER_END == ret)
    {
      row_iter_end_num++;
      ret = OB_SUCCESS;
    }
    else if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "failed next row for %ldth child, ret=%d", i, ret);
      break;
    }
    else if(OB_SUCCESS != (ret = row_iters_[i]->get_row(cur_rowkey, cur_row, cur_row_data_version, row_version_v2)))
    {
      YYSYS_LOG(WARN, "get current row failed from row_iters_[%ld],ret=%d", i, ret);
      break; 
    }
    else
    {
        //[693]
        if(cur_sstable_rowkey_!=NULL)
        {
            int cmp = cur_sstable_rowkey_->compare(*cur_rowkey);
            if(cmp == 0)
            {
                if(match_rowkey == NULL)
                {
                    match_rowkey = cur_rowkey;
                    cur_iter_arr_idx = 0;
                    cur_iter_arr[cur_iter_arr_idx] = i;
                    cur_iter_arr_row[cur_iter_arr_idx] = cur_row;
                    cur_iter_arr_row_version[cur_iter_arr_idx] = row_version_v2;
                    ++cur_iter_arr_idx;
                }
                else if (NULL != match_rowkey)
                {
                    cur_iter_arr[cur_iter_arr_idx] = i;
                    cur_iter_arr_row[cur_iter_arr_idx] = cur_row;
                    cur_iter_arr_row_version[cur_iter_arr_idx] = row_version_v2;
                    ++cur_iter_arr_idx;
                }
            }
            else
            {

            }

            if(NULL != match_rowkey)
            {
                min_rowkey = match_rowkey;
            }
            else
            {
                cur_iter_arr_idx = 0;
            }
        }
        else
        {
            if (NULL == min_rowkey)
            {
                min_rowkey = cur_rowkey;
                cur_iter_arr_idx = 0;
                cur_iter_arr[cur_iter_arr_idx] = i;
                cur_iter_arr_row[cur_iter_arr_idx] = cur_row;
                cur_iter_arr_row_version[cur_iter_arr_idx] = row_version_v2;
                ++cur_iter_arr_idx;
            }
            else if(NULL != min_rowkey)
            {
                int cmp = 0;
                cmp = min_rowkey->compare(*cur_rowkey);
                if (0 == cmp)
                {
                    cur_iter_arr[cur_iter_arr_idx] = i;
                    cur_iter_arr_row[cur_iter_arr_idx] = cur_row;
                    cur_iter_arr_row_version[cur_iter_arr_idx] = row_version_v2;
                    ++cur_iter_arr_idx;
                }
                else if (0 < cmp)
                {
                    min_rowkey = cur_rowkey;
                    cur_iter_arr_idx = 0;
                    cur_iter_arr[cur_iter_arr_idx] = i;
                    cur_iter_arr_row[cur_iter_arr_idx] = cur_row;
                    cur_iter_arr_row_version[cur_iter_arr_idx] = row_version_v2;
                    ++cur_iter_arr_idx;
                }
                else
                {
                    //nothing todo;
                }
            }
        }
    }
  }
  
  //second cons row_desc for result_row_
  ObRowDesc gen_row_desc;
  gen_row_desc.reset();
  for(int64_t i = 0; OB_SUCCESS == ret && i < cur_iter_arr_idx; i++)
  {
    const ObRowDesc* cur_row_desc = cur_row->get_row_desc();
    uint64_t table_id = OB_INVALID_ID;
    uint64_t column_id = OB_INVALID_ID;
    for(int64_t j = 0; OB_SUCCESS == ret && j < cur_row_desc->get_column_num(); j++)
    {
      if (OB_SUCCESS != (ret = cur_row_desc->get_tid_cid(j, table_id, column_id)))
      {
        YYSYS_LOG(WARN,"fail to get column desc, ret=%d", ret);
      }
      else if (OB_INVALID_INDEX != gen_row_desc.get_idx(table_id, column_id))
      {
      }
      else if (OB_SUCCESS != (ret = gen_row_desc.add_column_desc(table_id, column_id)))
      {
        YYSYS_LOG(WARN, "failed to add column desc, err=%d", ret);
      }
    }
  }
  
  //third fuse
  result_row_.clear();
  result_row_.set_row_desc(gen_row_desc);
  result_row_.reuse();
  YYSYS_LOG(DEBUG,"gen_row_desc %s", to_cstring(gen_row_desc));
  row_version_.reset();
  for(int64_t i = 0; OB_SUCCESS == ret && i < cur_iter_arr_idx; i++)
  {
    if (OB_SUCCESS != (ret = apply_row(cur_iter_arr_row[i], *cur_iter_arr_row_version[i])))
    {
      YYSYS_LOG(WARN,"apply row failed, ret=%d, cur iter num is %ld", ret, i);
    }
  }
  
  //fouth skew, repalce delete_obj with nop_obj, set is_all_nop_
  if (OB_SUCCESS == ret)
  {
//  if (OB_SUCCESS != (ret = skew()))
    if (OB_SUCCESS != (ret = skew(min_rowkey)))
    {
      YYSYS_LOG(WARN, "skew failed, ret=%d", ret);
    }
  }
  
  //fifth deepcopy fuesed row and rowkey
  cur_rowkey_ = NULL;
  cur_sstable_rowkey_ = NULL; //[693]
  if (OB_SUCCESS == ret)
  {
    str_buf_.reset();
    
    const ObObj*  cell= NULL;
    ObObj   stored_value;
    //deepcopy fuesed row
    for(int64_t i = 0; OB_SUCCESS == ret && i < result_row_.get_column_num(); i++)
    {
      if (OB_SUCCESS != (ret = result_row_.raw_get_cell(i, cell)))
      {
        YYSYS_LOG(WARN,"fail to get cell from result_row_, ret=%d", ret);
      }
      else if(OB_SUCCESS != (ret = str_buf_.write_obj(*cell, &stored_value)))
      {
        YYSYS_LOG(WARN, "write obj fail,ret=%d", ret);
      }
      else if(OB_SUCCESS != (ret = result_row_.raw_set_cell(i, stored_value)))
      {
        YYSYS_LOG(WARN, "fail to set cell, ret=%d", ret);
      }
    } 
    
    //deepcopy rowkey
    void*     buff = NULL;
    if (OB_SUCCESS != ret || NULL == min_rowkey)
    {
      //nothing todo
    }
    else if (NULL == (buff = str_buf_.alloc(sizeof(ObRowkey))))
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;  //add hongchen [ERRRO_MISS_BUGFIX] 20170901
      YYSYS_LOG(WARN, "fail to alloc mem for rowkey, ret=%d", ret);
    }
    else
    {
      cur_rowkey_ = new(buff) ObRowkey();
      if (OB_SUCCESS != (ret = min_rowkey->deep_copy(*cur_rowkey_, str_buf_)))
      {
        YYSYS_LOG(WARN, "fail to deep copy rowkey, ret=%d", ret);
      }
    }
  }
  
  //sixth pop fuse row
  for(int64_t i = 0; OB_SUCCESS == ret && i < cur_iter_arr_idx; i++)
  {
    if(OB_SUCCESS != (ret = pop_current_row(cur_iter_arr[i])))
    {
      YYSYS_LOG(WARN,"fail to pop fuesed row from current iter %ld, ret=%ld",i,cur_iter_arr[i]);
    }
  }

  //check, output result_row and cur_rowkey
  if((OB_SUCCESS == ret) && (row_iter_end_num == row_iter_num_)) // all iter ends
  {
    ret = OB_ITER_END;
  }

  if((OB_SUCCESS == ret) || (OB_ITER_END == ret))
  {
    rowkey = cur_rowkey_;
    row = &result_row_;
    YYSYS_LOG(DEBUG, "get result row[%s],rowkey[%s],result version:[%s],", to_cstring(result_row_),NULL==rowkey?"null":to_cstring(*rowkey),
              to_cstring(row_version_));
  }
  return ret;
}

int ObRowMerger::next_row_(int64_t row_iter_idx)
{
  int ret = OB_SUCCESS;

  if (row_iter_idx < 0 || row_iter_idx >= row_iter_num_)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(WARN, "invalid param, row_iter_idx=%ld, row_iter_num_=%ld,ret=%d",
              row_iter_idx, row_iter_num_, ret);
  }
  else if (NULL != row_iters_[row_iter_idx]) // double check
  {
    if (ITER_END == row_iter_status_[row_iter_idx])
    {
      ret = OB_ITER_END;
    }
    else
    {
      if (BEFORE_NEXT == row_iter_status_[row_iter_idx])
      {
        ret = row_iters_[row_iter_idx]->next_row();
        if (OB_ITER_END == ret)
        {
          row_iter_status_[row_iter_idx] = ITER_END;
        }
        else if (OB_SUCCESS != ret)
        {
          YYSYS_LOG(WARN, "failed to call next_row of the %ld-th row iter, ret=%d",
                    row_iter_idx, ret);
        }
        else
        {
          row_iter_status_[row_iter_idx] = AFTER_NEXT;
        }
      }
    }
  }

  return ret;
}

int ObRowMerger::pop_current_row(int64_t const index)
{
  int ret = OB_SUCCESS;
  if (index < 0 || row_iter_num_ < index)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(WARN, "the index is invalid,ret=%d",ret);
  }
  else
  {
    row_iter_status_[index] = BEFORE_NEXT;
    ret = next_row_(index);
    if (OB_ITER_END == ret)
    {
      ret = OB_SUCCESS;
    }
    else if (OB_SUCCESS == !ret)
    {
      YYSYS_LOG(WARN, "next row %ldth child failed,ret=%d",index, ret);
    }
  }
  return ret;
}

/*@berif   merge the row&version  with  old row&old version*/
int ObRowMerger::do_apply_delete(const int64_t delete_version)
{
  int ret = OB_SUCCESS;
  
  const ObRowDesc* row_desc = result_row_.get_row_desc();
  if (NULL == row_desc)
  {
    ret = OB_ERR_UNEXPECTED;
    YYSYS_LOG(WARN,"fail to acquire row desc from result row, ret=%d",ret); 
  }
  else
  {
    ObObj delete_cell;
    delete_cell.set_ext(ObActionFlag::OP_COLUMN_DELETE);
    uint64_t table_id = OB_INVALID_ID;
    uint64_t column_id = OB_INVALID_ID;
    int64_t  column_version = OB_INVALID_VERSION;
    int64_t  column_index   = OB_INVALID_INDEX;
    for(int64_t idx = 0;OB_SUCCESS == ret && idx < row_desc->get_column_num(); idx++)
    {
      if (OB_SUCCESS != (ret = row_desc->get_tid_cid(idx, table_id, column_id)))
      {
        YYSYS_LOG(WARN,"fail to get column desc, ret=%d", ret);
      }
      else
      {
        column_version = OB_INVALID_VERSION;
        column_index   = OB_INVALID_INDEX;
        row_version_.get_column_version(column_id, column_version, column_index);
        if(column_version == delete_version)
        {
          ret = OB_ERR_UNEXPECTED;
          YYSYS_LOG(WARN,"the same version column ocucr in more iters,BUT should occurs in only one. ret=%d", ret);
        }
        else if(column_version > delete_version)
        {
          //nothing todo
        }
        else if(OB_SUCCESS != (ret = result_row_.raw_set_cell(idx, delete_cell)))
        {
          YYSYS_LOG(WARN,"result row  set cell fail,ret=%d",ret);
        }
        else if(OB_SUCCESS != (ret = row_version_.update_column_version(column_id, delete_version, column_index)))
        {
          YYSYS_LOG(WARN,"result row  update column version fail,ret=%d",ret);
        }
      }
    } 
  }
  
  if (OB_SUCCESS == ret)
  {
    result_row_.set_is_delete_row(true);
  }
  return ret;
}

//add for [212-bug fix]-b
int ObRowMerger::do_apply_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  const ObRowDesc *row_desc = result_row_.get_row_desc();
  const ObRowDesc *cur_row_desc = row->get_row_desc();
  if (NULL == row_desc)
  {
    ret = OB_ERR_UNEXPECTED;
    YYSYS_LOG(WARN, "fail to acquire row desc from result row, ret=%d", ret);
  }
  else if (NULL == cur_row_desc)
  {
    ret = OB_ERR_UNEXPECTED;
    YYSYS_LOG(WARN, "fail to acquire row desc from row, ret=%d", ret);
  }
  else
  {
    uint64_t table_id = OB_INVALID_ID;
    uint64_t column_id = OB_INVALID_ID;
    ObObj nop_cell;
    nop_cell.set_ext(ObActionFlag::OP_NOP);
    for (int64_t idx = 0; OB_SUCCESS == ret && idx < cur_row_desc->get_column_num(); idx++)
    {
      const common::ObObj *cell = NULL;
      if (OB_SUCCESS != (ret = cur_row_desc->get_tid_cid(idx, table_id, column_id)))
      {
        YYSYS_LOG(WARN, "fail to get column desc, ret=%d", ret);
      }
      else if (OB_INVALID_INDEX == row_desc->get_idx(table_id, column_id))
      {
        YYSYS_LOG(INFO, "OB_INVALID_INDEX");
      }
      else if (OB_SUCCESS != (ret = row->get_cell(table_id, column_id, cell)))
      {
        YYSYS_LOG(WARN, "fail to get cell, ret=%d", ret);
      }
      else if (ObExtendType == cell->get_type() && ObActionFlag::OP_NOP == cell->get_ext())
      {
        YYSYS_LOG(INFO, "this cell is nop");
        if (OB_SUCCESS != (ret = result_row_.raw_set_cell(idx, nop_cell)))
        {
          YYSYS_LOG(WARN, "result row set cell fail, ret=%d", ret);
        }
      }
    }
  }
  return ret;
}
//add for [212-bug fix]-e

int ObRowMerger::do_apply_row(const common::ObRow *&row, const ObRowVersion &row_version)
{
  int ret = OB_SUCCESS;
  
  const ObRowDesc* row_desc = result_row_.get_row_desc();
  const ObRowDesc* cur_row_desc = row->get_row_desc();
  if (NULL == row_desc)
  {
    ret = OB_ERR_UNEXPECTED;
    YYSYS_LOG(WARN,"fail to acquire row desc from result row, ret=%d",ret); 
  }
  else if (NULL == cur_row_desc)
  {
    ret = OB_ERR_UNEXPECTED;
    YYSYS_LOG(WARN,"fail to acquire row desc from row, ret=%d",ret); 
  }
  else
  {
    uint64_t table_id = OB_INVALID_ID;
    uint64_t column_id = OB_INVALID_ID;
    int64_t  column_version = OB_INVALID_VERSION;
    int64_t  column_index   = OB_INVALID_INDEX;
    for(int64_t idx = 0;OB_SUCCESS == ret && idx < cur_row_desc->get_column_num(); idx++)
    {
      table_id = OB_INVALID_ID;
      column_id = OB_INVALID_ID;
      column_version = OB_INVALID_VERSION;
      column_index = OB_INVALID_INDEX;
      const common::ObObj *cell = NULL;
      if (OB_SUCCESS != (ret = cur_row_desc->get_tid_cid(idx, table_id, column_id)))
      {
        YYSYS_LOG(WARN,"fail to get column desc, ret-%d", ret);
      }
      else if (OB_INVALID_INDEX == row_desc->get_idx(table_id, column_id))
      {

      }
      else if(OB_SUCCESS != (ret = row->get_cell(table_id,column_id, cell)))
      {
        YYSYS_LOG(WARN,"fail to get cell. ret=%d", ret);
      }
      else if (ObExtendType == cell->get_type() && ObActionFlag::OP_NOP == cell->get_ext())
      {
        continue;
      }
      else if (OB_SUCCESS != (ret = row_version.get_column_version(column_id, column_version, column_index)))
      {
        YYSYS_LOG(WARN,"fail to get column version, ret-%d", ret);       
      }
      else
      {
        column_index   = OB_INVALID_INDEX;
        
        int64_t old_column_version = OB_INVALID_VERSION;
        row_version_.get_column_version(column_id, old_column_version, column_index);
        const common::ObObj *src_cell = NULL;
        if(column_version < old_column_version)
        {
          //nothing todo
        }
        else if(OB_SUCCESS != (ret = result_row_.get_cell(table_id, column_id, src_cell)))
        {
          YYSYS_LOG(WARN,"fail to get cell. ret=%d",ret);
        }
        else if ((column_version == old_column_version) 
                  && (!(ObExtendType == src_cell->get_type() && ObActionFlag::OP_COLUMN_DELETE == src_cell->get_ext())))
        {
          ret = OB_ERR_UNEXPECTED;
          YYSYS_LOG(WARN,"the same version column ocucr in more iters,BUT should occurs in only one. ret=%d", ret);
        }
        else if(OB_SUCCESS != (ret = result_row_.set_cell(table_id, column_id, *cell)))
        {
          YYSYS_LOG(WARN,"result row  set cell fail,ret=%d",ret);
        }
        else if (column_version == old_column_version)
        {
          //nothing todo
        }
        else if(OB_SUCCESS != (ret = row_version_.update_column_version(column_id, column_version, column_index)))
        {
          YYSYS_LOG(WARN,"result row  update column version fail,ret=%d",ret);
        }
      }
    }
  }
  
  return ret;
}

int ObRowMerger::apply_row(const common::ObRow *&row,const ObRowVersion &row_version)
{
  int ret = OB_SUCCESS;
  
  const ObUpsRow* ups_row= dynamic_cast<const ObUpsRow*>(row);
  bool not_nop = true; //add for [215-bug fix]
  if(NULL==ups_row)
  {
    ret = OB_ERR_UNEXPECTED;
  }
  else if (row_version.is_deleted_row())
  {
    int64_t delete_version = row_version.get_delete_version();
    if (ups_row->get_is_delete_row())
    {
      if (OB_INVALID_VERSION == delete_version)
      {
        //nothing todo
      }
      else if (OB_SUCCESS != (ret = do_apply_delete(delete_version)))
      {
        YYSYS_LOG(WARN,"fail to apply delete, ret=%d", ret);
      }
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      YYSYS_LOG(WARN,"row_version tagged with delete, BUT correspond ups row is not deleted row, ret=%d",ret);
    }
  }
  else
  {
    if(ups_row->get_is_delete_row())
    {
      int64_t delete_version = row_version.column_versions_[0].version_;

      if (OB_INVALID_VERSION == delete_version)
      {
        //nothing todo
      }
      else if (OB_SUCCESS != (ret = do_apply_delete(delete_version)))
      {
        YYSYS_LOG(WARN,"fail to apply delete, ret=%d", ret);
      }
      else if (row_version.is_nop_version())
      {
        not_nop = false;
        if (OB_SUCCESS != (ret = do_apply_row(row)))
        {
          YYSYS_LOG(WARN, "fail to apply row, ret=%d", ret);
        }
      }
    }
    if (not_nop)
    {
      if (OB_SUCCESS != (ret = do_apply_row(row, row_version)))
      {
        YYSYS_LOG(WARN,"fail to apply row, ret=%d", ret);
      }
    }
  }

  return ret;
}

//int ObRowMerger::skew()
int ObRowMerger::skew(const ObRowkey *rowkey)
{
  int ret = OB_SUCCESS;
  
  int64_t column_count = result_row_.get_column_num();
  bool is_all_nop = true;
  const common::ObObj *cell = NULL;
  
  ObObj nop_cell;
  nop_cell.set_ext(ObActionFlag::OP_NOP);
  
  for(int64_t idx = 0;OB_SUCCESS ==  ret && idx < column_count; idx++)
  {
    if(OB_SUCCESS != (ret = result_row_.raw_get_cell(idx, cell)))
    {
      YYSYS_LOG(WARN,"fail to get cell. ret=%d", ret);
    }
    else if (ObExtendType == cell->get_type() && ObActionFlag::OP_COLUMN_DELETE == cell->get_ext())
    {
      if (OB_SUCCESS != (ret = result_row_.raw_set_cell(idx, nop_cell)))
      {
        YYSYS_LOG(WARN,"result row  set cell fail,ret=%d",ret);
      }
    }
    else if (ObExtendType != cell->get_type() || ObActionFlag::OP_NOP != cell->get_ext())
    {
      is_all_nop = false;
    }
    else if (ObExtendType == cell->get_type() && ObActionFlag::OP_NOP == cell->get_ext())
    {
      if (NULL != rowkey)
      {
        is_all_nop = false;
        YYSYS_LOG(DEBUG, "rowkey exist, set_is_all_nop false");
      }
    }
  }
  
  result_row_.set_is_all_nop(is_all_nop);

  return ret;
}



