
#include "ob_hash_join_single.h"
#include "ob_table_rpc_scan.h"
#include "ob_table_mem_scan.h"
#include "ob_postfix_expression.h"
#include "common/utility.h"
#include "common/ob_row_util.h"
#include "sql/ob_physical_plan.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

ObHashJoinSingle::ObHashJoinSingle()
  :get_next_row_func_(NULL),
    last_left_row_(NULL),
    last_right_row_(NULL),
    table_filter_expr_(NULL)
    ,hashmap_num_(0)
{
  arena_.set_mod_id(ObModIds::OB_MS_SUB_QUERY);
  left_hash_key_cache_valid_ = false;
  left_bucket_pos_for_left_outer_join_ = -1;
  for(int i=0;i<MAX_SUB_QUERY_NUM; i++)
  {
    is_subquery_result_contain_null[i] = false;
  }
  bucket_node_ = NULL;
  is_mached_ = false;
  left_row_count_ = 0;
}

ObHashJoinSingle::~ObHashJoinSingle()
{
  sub_result_.~ObArray();
  for(int i=0;i<MAX_SUB_QUERY_NUM;i++)
  {
    sub_query_map_[i].destroy();
  }
  arena_.free();
  row_store_.clear();
  for (HashTableRowMap::iterator iter = hash_table_.begin() ;iter != hash_table_.end() ;iter ++)
  {
    delete iter->second;
    iter->second = NULL;
  }
  hash_table_.destroy();
}

void ObHashJoinSingle::reset()
{
  get_next_row_func_ = NULL;
  last_left_row_ = NULL;
  last_right_row_ = NULL;
  row_desc_.reset();
  left_bucket_pos_for_left_outer_join_ = -1;
  left_hash_key_cache_valid_ = false;
  equal_join_conds_.clear();
  other_join_conds_.clear();
  left_op_ = NULL;
  right_op_ = NULL;
  for(int i=0;i<MAX_SUB_QUERY_NUM;i++)
  {
    sub_query_map_[i].clear();
  }
  hashmap_num_ = 0;

  for(int i=0;i<MAX_SUB_QUERY_NUM;i++)
  {
    sub_query_map_and_bloomfilter_column_type[i].clear();
    is_subquery_result_contain_null[i] = false;
  }

  bucket_node_ = NULL;
  is_mached_ = false;
  row_store_.clear();  

  for (HashTableRowMap::iterator iter = hash_table_.begin() ;iter != hash_table_.end() ;iter ++)
  {
    delete iter->second;
    iter->second = NULL;
  }
  hash_table_.clear();

  left_row_count_ = 0;
}

void ObHashJoinSingle::reuse()
{
  get_next_row_func_ = NULL;
  last_left_row_ = NULL;
  last_right_row_ = NULL;
  row_desc_.reset();
  left_bucket_pos_for_left_outer_join_ = -1;
  left_hash_key_cache_valid_ = false;
  equal_join_conds_.clear();
  other_join_conds_.clear();
  left_op_ = NULL;
  right_op_ = NULL;
  for(int i=0;i<MAX_SUB_QUERY_NUM;i++)
  {
    sub_query_map_[i].clear();
  }
  hashmap_num_ = 0;

  for(int i=0;i<MAX_SUB_QUERY_NUM;i++)
  {
    sub_query_map_and_bloomfilter_column_type[i].clear();
    is_subquery_result_contain_null[i] = false;
  }

  bucket_node_ = NULL;
  is_mached_ = false;
  row_store_.clear();  

  for (HashTableRowMap::iterator iter = hash_table_.begin() ;iter != hash_table_.end() ;iter ++)
  {
    delete iter->second;
    iter->second = NULL;
  }
  hash_table_.clear();

  left_row_count_ = 0;
}

int ObHashJoinSingle::set_join_type(const ObJoin::JoinType join_type)
{
  int ret = OB_SUCCESS;
  ObJoin::set_join_type(join_type);
  switch(join_type)
  {
  case INNER_JOIN:
    get_next_row_func_ = &ObHashJoinSingle::inner_hash_get_next_row;
    use_bloomfilter_ = true;
    break;
  case LEFT_OUTER_JOIN:
    get_next_row_func_ = &ObHashJoinSingle::left_hash_outer_get_next_row;
    use_bloomfilter_ = true;
    break;
  case RIGHT_OUTER_JOIN:
    get_next_row_func_ = &ObHashJoinSingle::right_hash_outer_get_next_row;
    use_bloomfilter_ = false;
    break;
  case FULL_OUTER_JOIN:
    get_next_row_func_ = &ObHashJoinSingle::full_hash_outer_get_next_row;
    use_bloomfilter_ = false;
    break;
  default:
    ret = OB_ERR_UNEXPECTED;
    break;
  }
  return ret;
}

int ObHashJoinSingle::get_next_row(const ObRow *&row)
{
  OB_ASSERT(get_next_row_func_);
  return (this->*(this->ObHashJoinSingle::get_next_row_func_))(row);
}

int ObHashJoinSingle::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 >= row_desc_.get_column_num()))
  {
    YYSYS_LOG(ERROR, "not init");
    ret = OB_NOT_INIT;
  }
  else
  {
    row_desc = &row_desc_;
  }
  return ret;
}

int64_t ObHashJoinSingle::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "HashJoinSingle ");
  pos += ObJoin::to_string(buf + pos, buf_len - pos);
  return pos;
}

int ObHashJoinSingle::close()
{
  int ret = OB_SUCCESS;
  last_left_row_ = NULL;
  last_right_row_ = NULL;
  row_desc_.reset();
  row_store_.clear();  
  for (HashTableRowMap::iterator iter = hash_table_.begin() ;iter != hash_table_.end() ;iter ++)
  {
    delete iter->second;
    iter->second = NULL;
  }
  hash_table_.destroy();
  ret = ObJoin::close();
  return ret;
}

int ObHashJoinSingle::open()
{
  int ret = OB_SUCCESS;
  const ObRowDesc *left_row_desc = NULL;
  const ObRowDesc *right_row_desc = NULL;
  int64_t equal_join_conds_count = equal_join_conds_.count();

  if (left_op_ == NULL || right_op_ == NULL)
  {    
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR, "child(ren) operator(s) is/are NULL");
  }
  else
      if (equal_join_conds_count <= 0)
  {
    YYSYS_LOG(WARN, "hash join can not work without equijoin conditions");
    ret = OB_NOT_SUPPORTED;
    return ret;
  }
  else if (OB_SUCCESS != (ret = left_op_->open()))
  {
          YYSYS_LOG(WARN, "failed to open child(ren) operator(s), err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = left_op_->get_row_desc(left_row_desc)))
  {
    YYSYS_LOG(WARN, "failed to get child row desc, err=%d", ret);
  }
  if (OB_SUCCESS == ret)
  {
    const ObRow *row = NULL;
    ret = left_op_->get_next_row(row);
    hash_table_.create(HASH_BUCKET_NUM);
    while (OB_SUCCESS == ret)
    {
      left_row_count_ ++;
      ObRow *curr_row = const_cast<ObRow *>(row);
      uint64_t hash_key = 0;
      for (int64_t i = 0 ; i < equal_join_conds_count ;++i)
      {
        const ObSqlExpression &expr = equal_join_conds_.at(i);
        ExprItem::SqlCellInfo c1;
        ExprItem::SqlCellInfo c2;
        if (expr.is_equijoin_cond(c1, c2))
        {
          ObObj *temp = NULL;
          if (OB_SUCCESS != (ret = curr_row->get_cell(c1.tid, c1.cid, temp)))
          {
            YYSYS_LOG(ERROR, "failed to get cell, err=%d tid=%lu cid=%lu", ret, c1.tid, c1.cid);
            break;
          }
          else
          {
            hash_key = temp->murmurhash64A(hash_key);
          }
        }
      }
      const ObRowStore::StoredRow *stored_row = NULL;
      row_store_.add_row(*row, stored_row);
      HashTableRowPair* pair = new HashTableRowPair(stored_row, 0);
      if (common::hash::HASH_INSERT_SUCC != hash_table_.set_multiple(hash_key, pair))
      {
        YYSYS_LOG(WARN, "fail to insert pair into hash map");
        ret = OB_ERROR;
        break;
      }
      ret = left_op_->get_next_row(row);
    }
    if (ret == OB_ITER_END)
      ret = OB_SUCCESS;
  }

  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = right_op_->open()))
    {
        YYSYS_LOG(WARN, "failed to open right_op_ operator(s), err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = right_op_->get_row_desc(right_row_desc)))
    {
      YYSYS_LOG(WARN, "failed to get right_op_ row desc, err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = cons_row_desc(*left_row_desc, *right_row_desc)))
    {
      YYSYS_LOG(WARN, "failed to cons row desc, err=%d", ret);
    }
    else
    {    
      OB_ASSERT(left_row_desc);
      OB_ASSERT(right_row_desc);
      curr_row_.set_row_desc(row_desc_);
      last_left_row_ = NULL;
      last_right_row_ = NULL;
      curr_cached_left_row_.set_row_desc(*left_row_desc);
      left_hash_key_cache_valid_ = false;
      left_bucket_pos_for_left_outer_join_ = -1;

      if(OB_SUCCESS != (ret = process_sub_query()))
      {
        YYSYS_LOG(ERROR, "mergejoin::process sub query error");
      }
    }
  }
  return ret;
}

int ObHashJoinSingle::process_sub_query()
{
  int ret = OB_SUCCESS;
  const common::ObRowDesc *row_desc;
  const ObRow *row = NULL;
  const ObRow *temp_row = NULL;//add xionghui [fix equal subqeury bug] 20150122
  int64_t cond_num = other_join_conds_.count();
  for(int i = 0;i< cond_num;i++)
  {
    ObSqlExpression &expr = other_join_conds_.at(i);
    int64_t sub_query_num = expr.get_sub_query_num();
    if(sub_query_num<=0)
    {
      continue;
    }
    else
    {
      char* varchar_buf[MAX_SUB_QUERY_NUM] = {NULL};
      for(int j = 0;j<sub_query_num;j++)
      {
        int32_t sub_query_index =expr.get_sub_query_idx(j);
        ObPhyOperator * sub_operator = NULL;
        if(NULL == (sub_operator = my_phy_plan_->get_phy_query(sub_query_index)))
        {
          ret = OB_INVALID_INDEX;
          YYSYS_LOG(ERROR,"get child of sub query operator faild"); 
        }
        else if(OB_SUCCESS != (ret = sub_operator->open()))
        {
         YYSYS_LOG(WARN, "fail to open sub select query:ret[%d]", ret);
        }
        else if (OB_SUCCESS != (ret = sub_operator->get_row_desc(row_desc)))
        {
         YYSYS_LOG(WARN, "fail to get row_desc:ret[%d]", ret);
        }
        else
        {
          bool direct_bind = false; 
          sub_result_.clear();
	
          int64_t rowkey_count = 0;
          bool special_sub_query = false;
          bool special_case = false;
          
          expr.get_sub_query_column_num_new(j+1,rowkey_count,special_sub_query,special_case);
          
          uint64_t table_id = 0;
          uint64_t column_id = 0;
		  
          if(special_sub_query)
          {
            if(special_case)
            {
               if(rowkey_count > 1)
               {
                 ret=OB_ERR_COLUMN_SIZE;
                 YYSYS_LOG(USER_ERROR, "sub-query more than one cloumn ret=%d", ret);
               }
               else
               {
                   ret = sub_operator->get_next_row(row);
                   if(ret==OB_SUCCESS)
                   {
                       ret=sub_operator->get_next_row(temp_row);
                       if(ret==OB_ITER_END)
                       {
                           ret=OB_SUCCESS;
                       }
                       else
                       {
                           sub_operator->close();
                           ret=OB_ERR_SUB_QUERY_RESULTSET_EXCESSIVE;
                           YYSYS_LOG(USER_ERROR, "sub-query more than one row ret=%d", ret);
                       }
                   }
                   if (ret!=OB_SUCCESS)
                   {
                     if(OB_ITER_END == ret)
                     {
                       ret = OB_SUCCESS;
                       expr.delete_sub_query_info(j+1);
                       ExprItem dem;
                       dem.type_ = T_NULL;
                       dem.data_type_ = ObNullType;
                       expr.add_expr_item(dem);
                       expr.complete_sub_query_info();
                     }
                   }
                   else
                   {
                      row_desc->get_tid_cid(0,table_id,column_id);
                      ObObj* cell;
                      const_cast<ObRow *>(row)->get_cell(table_id,column_id,cell);
                      expr.delete_sub_query_info(j+1);
                      expr.add_expr_in_obj(*cell);
                      expr.complete_sub_query_info();
                   }
               }
            }
            else
            {
		    bool sub_query_result = false;
		    if(OB_ITER_END == (ret = sub_operator->get_next_row(row)))
		    {
		      ret = OB_SUCCESS;
		    }
		    else if(OB_SUCCESS != ret)
		    {
		      YYSYS_LOG(WARN, "fail to get next row from rpc scan");
		    }
		    else
		    {  
		      row_desc->get_tid_cid(0,table_id,column_id);
		      ObObj *temp;
		      const_cast<ObRow *>(row)->get_cell(table_id,column_id,temp);
		      sub_query_result = temp->is_true();
		      if(OB_ITER_END != (ret = sub_operator->get_next_row(row)))
		      {
		        sub_operator->close();
		        YYSYS_LOG(WARN, "Subquery returns more than 1 row");
		        ret = OB_ERR_SUB_QUERY_RESULTSET_EXCESSIVE;
		      }
		      else
		      {
		        ret = OB_SUCCESS;
		      }
		    }
        
		    if(OB_SUCCESS == ret)
		    {
          
		      expr.delete_sub_query_info(j+1);

		      ObObj * result = (ObObj*)arena_.alloc(sizeof(ObObj));
		      result->set_bool(sub_query_result);
		      expr.add_expr_in_obj(*result);
		      expr.complete_sub_query_info();
		    }
          }
          }
		  else
		  {
		   
            while (OB_SUCCESS == ret && sub_result_.count()<=BIG_RESULTSET_THRESHOLD)
            {
              ret = sub_operator->get_next_row(row);
              if (OB_ITER_END == ret)
              {
                ret = OB_SUCCESS;
                direct_bind = true;
                break;
              }
              else if (OB_SUCCESS != ret)
              {
                YYSYS_LOG(WARN, "fail to get next row from rpc scan");
              }
              else
              {
                ObObj value[rowkey_count];
                for(int i =0;i<rowkey_count;i++)
                {
                  row_desc->get_tid_cid(i,table_id,column_id);
                  ObObj *temp;
                  const_cast<ObRow *>(row)->get_cell(table_id,column_id,temp);
                  value[i] = *temp;
                }
	  
                ObRowkey columns;
                columns.assign(value,rowkey_count);
                ObRowkey columns2;
                if(OB_SUCCESS != (ret = columns.deep_copy(columns2,arena_)))
                {
                  YYSYS_LOG(WARN, "fail to deep copy column");
                  break;
                }		  
                sub_result_.push_back(columns2);      
              }
            }
            
            
            if(direct_bind && sub_result_.count()>0)
            {    
              expr.delete_sub_query_info(j+1);
              
              ExprItem dem1;
              dem1.type_ = T_OP_ROW;
              dem1.data_type_ = ObMinType;
              dem1.value_.int_ = rowkey_count;

              ExprItem dem2;
              dem2.type_ = T_OP_ROW;
              dem2.data_type_ = ObMinType;
              dem2.value_.int_ = sub_result_.count();
  
              while (sub_result_.count()>0)
              {
                ObRowkey columns;
                sub_result_.pop_back(columns);
                ObObj * obj_ptr = const_cast <ObObj *>(columns.get_obj_ptr());
                for(int i = 0;i<rowkey_count;i++)
                {
                  expr.add_expr_in_obj(obj_ptr[i]);
                } 
                expr.add_expr_item(dem1);	   
              }
  
              expr.add_expr_item(dem2);
              expr.complete_sub_query_info();
            }
            else if(hashmap_num_ >= MAX_SUB_QUERY_NUM)
            {
              YYSYS_LOG(WARN, "too many sub_query");
              ret = OB_ERR_SUB_QUERY_OVERFLOW;
            }
            else
            {
              
              char *tmp_varchar_buff = NULL;
              ObObj casted_cells[rowkey_count];
              ObObj dst_value[rowkey_count];
              alloc_small_mem_return_ptr(tmp_varchar_buff, rowkey_count, ret);
              bind_memptr_for_casted_cells(casted_cells, rowkey_count, tmp_varchar_buff);
              if (OB_SUCCESS != ret)
              {
                break;
              }
              varchar_buf[hashmap_num_]  = tmp_varchar_buff;

              sub_query_map_[hashmap_num_].create(HASH_BUCKET_NUM);	  

              ret = sub_operator->get_output_columns_dsttype (sub_query_map_and_bloomfilter_column_type[hashmap_num_]);
              if (OB_SUCCESS != ret)
              {
                break;
              }

              is_subquery_result_contain_null[hashmap_num_] = false;  
              while(sub_result_.count()>0)
              {
                ObRowkey columns;
                sub_result_.pop_back(columns);	
                
                cast_and_set_hashmap(&(sub_query_map_and_bloomfilter_column_type[hashmap_num_]),
                                                                                   columns,
                                                                                   rowkey_count,
                                                                                   casted_cells,
                                                                                   dst_value,
                                                                                   sub_query_map_[hashmap_num_],
                                                                                   arena_,
                                                                                   is_subquery_result_contain_null[hashmap_num_],  //add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518
                                                                                   ret);
                bind_memptr_for_casted_cells(casted_cells, rowkey_count, tmp_varchar_buff);

              }

              while (OB_SUCCESS == ret)
              {
                ret = sub_operator->get_next_row(row);
                if (OB_ITER_END == ret)
                {
                  ret = OB_SUCCESS;
                    break;
                }
                else if (OB_SUCCESS != ret)
                {
                  YYSYS_LOG(WARN, "fail to get next row from sub query");
                }
                else
                {
                  YYSYS_LOG(DEBUG, "load data from sub query, row=%s", to_cstring(*row));
                  ObObj value[rowkey_count];
                  ObRow *curr_row = const_cast<ObRow *>(row);
                  for(int i =0;i<rowkey_count;i++)
                  {
                    row_desc->get_tid_cid(i,table_id,column_id);
                    ObObj *temp;
                    curr_row->get_cell(table_id,column_id,temp);
                    value[i] = *temp;
                  }
                  ObRowkey columns;
                  columns.assign(value,rowkey_count);
                  
                  cast_and_set_hashmap(&(sub_query_map_and_bloomfilter_column_type[hashmap_num_]),
                                                                                     columns,
                                                                                     rowkey_count,
                                                                                     casted_cells,
                                                                                     dst_value,
                                                                                     sub_query_map_[hashmap_num_],
                                                                                     arena_,
                                                                                     is_subquery_result_contain_null[hashmap_num_],  //add peiouya [IN_AND NOT_IN_WITH_NULL_BUG_FIX] 20160518
                                                                                     ret);
                  bind_memptr_for_casted_cells(casted_cells, rowkey_count, tmp_varchar_buff);

                }
              }      
              hashmap_num_ ++;
            }
          }  
        }
        sub_operator->close();
      }
      for(int idx = 0; idx < MAX_SUB_QUERY_NUM; idx++)
      {
        if (NULL != varchar_buf[idx])
        {
          ob_free (varchar_buf[idx]);
        }
      }
    }
    expr.update_sub_query_num();
  }
  return ret;
}

int ObHashJoinSingle::compare_equijoin_cond(const ObRow& r1, const ObRow& r2, int &cmp) const
{
  int ret = OB_SUCCESS;
  cmp = 0;
  const ObObj *res1 = NULL;
  const ObObj *res2 = NULL;
  ObExprObj obj1;
  ObExprObj obj2;
  for (int64_t i = 0; i < equal_join_conds_.count(); ++i)
  {
    const ObSqlExpression &expr = equal_join_conds_.at(i);
    ExprItem::SqlCellInfo c1;
    ExprItem::SqlCellInfo c2;
    if (expr.is_equijoin_cond(c1, c2))
    {
      if (OB_SUCCESS != (ret = r1.get_cell(c1.tid, c1.cid, res1)))
      {
        YYSYS_LOG(ERROR, "failed to get cell, err=%d tid=%lu cid=%lu", ret, c1.tid, c1.cid);
        break;
      }
      else if (OB_SUCCESS != (ret = r2.get_cell(c2.tid, c2.cid, res2)))
      {
        YYSYS_LOG(ERROR, "failed to get cell, err=%d tid=%lu cid=%lu", ret, c2.tid, c2.cid);
        break;
      }
      else
      {
        obj1.assign(*res1);
        obj2.assign(*res2);
        if (OB_SUCCESS != obj1.compare(obj2, cmp))
        {
          if (obj1.is_null())
          {
            cmp = -10;
          }
          else
          {
            cmp = 10;
          }
          break;
        }
        else if (0 != cmp)
        {
          break;
        }
      }
    }
    else
    {
      YYSYS_LOG(ERROR, "invalid equijoin condition");
      ret = OB_ERR_UNEXPECTED;
      break;
    }
  }
  return ret;
}

int ObHashJoinSingle::curr_row_is_qualified(bool &is_qualified)
{
  int ret = OB_SUCCESS;
  is_qualified = true;
  const ObObj *res = NULL;
  int hash_mape_index = 0;
  for (int64_t i = 0; i < other_join_conds_.count(); ++i)
  {
    ObSqlExpression &expr = other_join_conds_.at(i);
    
    bool use_hash_map = false;
    bool is_hashmap_contain_null = false;
    common::hash::ObHashMap<common::ObRowkey,common::ObRowkey,common::hash::NoPthreadDefendMode>* p = NULL;
    common::ObArray<ObObjType> * p_data_type_desc = NULL;
    if(hashmap_num_>0 && expr.get_sub_query_num()>0)
    {
      p =&(sub_query_map_[hash_mape_index]);
      is_hashmap_contain_null = is_subquery_result_contain_null[hash_mape_index]; 
      p_data_type_desc=  &(sub_query_map_and_bloomfilter_column_type[hash_mape_index]);
      use_hash_map = true;
      hash_mape_index = hash_mape_index + (int)expr.get_sub_query_num();
    }
    
    if (OB_SUCCESS != (ret = expr.calc(curr_row_, res, p, is_hashmap_contain_null,p_data_type_desc, use_hash_map)))
    {
      YYSYS_LOG(WARN, "failed to calc expr, err=%d", ret);
    }
    else if (!res->is_true())
    {
      is_qualified = false;
      break;
    }
  }
  return ret;
}


int ObHashJoinSingle::cons_row_desc(const ObRowDesc &rd1, const ObRowDesc &rd2)
{
  int ret = OB_SUCCESS;
  uint64_t tid = OB_INVALID_ID;
  uint64_t cid = OB_INVALID_ID;
  for (int64_t i = 0; i < rd1.get_column_num(); ++i)
  {
    if (OB_SUCCESS != (ret = rd1.get_tid_cid(i, tid, cid)))
    {
      YYSYS_LOG(ERROR, "unexpected branch");
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    else if (OB_SUCCESS != (ret = row_desc_.add_column_desc(tid, cid)))
    {
      YYSYS_LOG(WARN, "failed to add column desc, err=%d", ret);
      break;
    }
  }
  for (int64_t i = 0; OB_SUCCESS == ret && i < rd2.get_column_num(); ++i)
  {
    if (OB_SUCCESS != (ret = rd2.get_tid_cid(i, tid, cid)))
    {
      YYSYS_LOG(ERROR, "unexpected branch");
      ret = OB_ERR_UNEXPECTED;
    }
    else if (OB_SUCCESS != (ret = row_desc_.add_column_desc(tid, cid)))
    {
      YYSYS_LOG(WARN, "failed to add column desc, err=%d", ret);
    }
  }
  return ret;
}

int ObHashJoinSingle::join_rows(const ObRow& r1, const ObRow& r2)
{
  int ret = OB_SUCCESS;
  const ObObj *cell = NULL;
  uint64_t tid = OB_INVALID_ID;
  uint64_t cid = OB_INVALID_ID;
  int64_t i = 0;
  for (; i < r1.get_column_num(); ++i)
  {
    if (OB_SUCCESS != (ret = r1.raw_get_cell(i, cell, tid, cid)))
    {
      YYSYS_LOG(ERROR, "unexpected branch, err=%d", ret);
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    else if (OB_SUCCESS != (ret = curr_row_.raw_set_cell(i, *cell)))
    {
      YYSYS_LOG(WARN, "failed to set cell, err=%d i=%ld", ret, i);
      break;
    }
  } 
  for (int64_t j = 0; OB_SUCCESS == ret && j < r2.get_column_num(); ++j)
  {
    if (OB_SUCCESS != (ret = r2.raw_get_cell(j, cell, tid, cid)))
    {
      YYSYS_LOG(ERROR, "unexpected branch, err=%d", ret);
      ret = OB_ERR_UNEXPECTED;
    }
    else if (OB_SUCCESS != (ret = curr_row_.raw_set_cell(i+j, *cell)))
    {
      YYSYS_LOG(WARN, "failed to set cell, err=%d j=%ld", ret, j);
    }
  } 
  return ret;
}

int ObHashJoinSingle::left_join_rows(const ObRow& r1)
{
  int ret = OB_SUCCESS;
  const ObObj *cell = NULL;
  uint64_t tid = OB_INVALID_ID;
  uint64_t cid = OB_INVALID_ID;
  int64_t i = 0;
  for (; i < r1.get_column_num(); ++i)
  {
    if (OB_SUCCESS != (ret = r1.raw_get_cell(i, cell, tid, cid)))
    {
      YYSYS_LOG(ERROR, "unexpected branch, err=%d", ret);
      ret = OB_ERR_UNEXPECTED;
      break;
    }
    else if (OB_SUCCESS != (ret = curr_row_.raw_set_cell(i, *cell)))
    {
      YYSYS_LOG(WARN, "failed to set cell, err=%d i=%ld", ret, i);
      break;
    }
  }
  int64_t right_row_column_num = row_desc_.get_column_num() - r1.get_column_num();
  ObObj null_cell;
  null_cell.set_null();
  for (int64_t j = 0; OB_SUCCESS == ret && j < right_row_column_num; ++j)
  {
    if (OB_SUCCESS != (ret = curr_row_.raw_set_cell(i+j, null_cell)))
    {
      YYSYS_LOG(WARN, "failed to set cell, err=%d j=%ld", ret, j);
    }
  }
  return ret;
}

int ObHashJoinSingle::right_join_rows(const ObRow& r2)
{
  int ret = OB_SUCCESS;
  const ObObj *cell = NULL;
  uint64_t tid = OB_INVALID_ID;
  uint64_t cid = OB_INVALID_ID;
  int64_t left_row_column_num = row_desc_.get_column_num() - r2.get_column_num();
  ObObj null_cell;
  null_cell.set_null();
  for (int64_t i = 0; i < left_row_column_num; ++i)
  {
    if (OB_SUCCESS != (ret = curr_row_.raw_set_cell(i, null_cell)))
    {
      YYSYS_LOG(WARN, "failed to set cell, err=%d i=%ld", ret, i);
      break;
    }
  }
  
  for (int64_t j = 0; OB_SUCCESS == ret && j < r2.get_column_num(); ++j)
  {
    if (OB_SUCCESS != (ret = r2.raw_get_cell(j, cell, tid, cid)))
    {
      YYSYS_LOG(ERROR, "unexpected branch, err=%d", ret);
      ret = OB_ERR_UNEXPECTED;
    }
    else if (OB_SUCCESS != (ret = curr_row_.raw_set_cell(left_row_column_num + j, *cell)))
    {
      YYSYS_LOG(WARN, "failed to set cell, err=%d j=%ld", ret, j);
    }
  }

  return ret;
}


int ObHashJoinSingle::get_next_leftouterjoin_left_row(const common::ObRow*& row)
{
  int ret = OB_SUCCESS;
  int64_t hash_table_bucket_num = hash::cal_next_prime(HASH_BUCKET_NUM);
  HashTableRowPair *pair = NULL;
  for ( ; left_bucket_pos_for_left_outer_join_ < hash_table_bucket_num ; )
  {
    if (common::hash::HASH_EXIST != (ret = hash_table_.get_all(left_bucket_pos_for_left_outer_join_, bucket_node_, 0, pair)))
    {
      if (common::hash::HASH_NOT_EXIST != ret)
      {
        YYSYS_LOG(WARN, "failed to get next row from hash_table, err=%d", ret);
        ret = OB_ERROR;
      }
      else
      {
        left_bucket_pos_for_left_outer_join_ ++;
        bucket_node_ = NULL;
        ret = OB_ITER_END;
        continue;
      }
    }
    else
    {
      if (1 == pair->second)
      {
        continue;
      }
      if (OB_SUCCESS != (ret = ObRowUtil::convert(pair->first->get_compact_row(), curr_cached_left_row_)))
      {
        YYSYS_LOG(WARN, "fail to convert compact row to ObRow:ret[%d]",ret);
      }
      else
      {
        row = &curr_cached_left_row_;
      }
      break;
    }
  }
  if (left_bucket_pos_for_left_outer_join_ >= hash_table_bucket_num)
  {
    ret = OB_ITER_END;
  }
  return ret;
}
      
int ObHashJoinSingle::get_next_equijoin_left_row(const ObRow *&r1, const ObRow& r2, uint64_t& bucket_hash_key, HashTableRowPair*& pair)
{
  int ret = OB_SUCCESS;
  uint64_t hash_key = 0;
  if (!left_hash_key_cache_valid_)
  {
    for (int64_t i = 0; i < equal_join_conds_.count(); ++i)
    {
      const ObSqlExpression &expr = equal_join_conds_.at(i);
      ExprItem::SqlCellInfo c1;
      ExprItem::SqlCellInfo c2;
      const ObObj *temp = NULL;
      if (expr.is_equijoin_cond(c1, c2))
      {
        if (OB_SUCCESS != (ret = r2.get_cell(c2.tid, c2.cid, temp)))
        {
          YYSYS_LOG(ERROR, "failed to get cell, err=%d tid=%lu cid=%lu", ret, c2.tid, c2.cid);
          break;
        }
        else
        {
          hash_key = temp->murmurhash64A(hash_key);
        }
      }
    }
    last_left_hash_key_ = hash_key;
  }
  else
  {
    hash_key = last_left_hash_key_;
  }
  bucket_hash_key = hash_key;
  while (OB_SUCCESS == ret)
  {
    if (common::hash::HASH_EXIST != (ret = hash_table_.get_multiple(bucket_node_, hash_key, pair)))
    {
      if (common::hash::HASH_NOT_EXIST != ret)
      {
        YYSYS_LOG(WARN, "failed to get next row from hash_table, err=%d", ret);
        ret = OB_ERROR;
      }
      else
      {
        ret = OB_ITER_END;
      }
      left_hash_key_cache_valid_ = false;
      break;
    }
    else
    {
      if (OB_SUCCESS != (ret = ObRowUtil::convert(pair->first->get_compact_row(), curr_cached_left_row_)))
      {
        YYSYS_LOG(WARN, "fail to convert compact row to ObRow:ret[%d]",ret);
      }
      else
      {
        int cmp = 0;
        if (OB_SUCCESS != (ret = compare_equijoin_cond(curr_cached_left_row_, r2, cmp)))
        {
          YYSYS_LOG(WARN, "failed to compare, err=%d", ret);
          break;
        }
        if (0 == cmp)
        {
          r1 = &curr_cached_left_row_;
          break;
        }
      }
    } 
  }
  return ret;
}

int ObHashJoinSingle::inner_hash_get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;

  const ObRow *right_row = NULL;
  const ObRow *left_row = NULL;

  HashTableRowPair *pair = NULL;
  uint64_t bucket_hash_key = 0;

  while (OB_SUCCESS == ret)
  {
    if (left_hash_key_cache_valid_)
    {
      if (OB_SUCCESS != (ret = get_next_equijoin_left_row(left_row, *last_right_row_, bucket_hash_key, pair)))
      {
        if (OB_ITER_END == ret)
        {
          bucket_node_ = NULL;
          left_hash_key_cache_valid_ = false;
          ret = OB_SUCCESS;
        }
      }
      else
      {
        bool is_qualified = false;
        if (last_right_row_ == NULL)
        {
          YYSYS_LOG(WARN, "last_right_row_ is NULL");
        }
        else if (OB_SUCCESS != (ret = join_rows(*left_row, *last_right_row_)))
        {
          YYSYS_LOG(WARN, "failed to join rows, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = curr_row_is_qualified(is_qualified)))
        {
          YYSYS_LOG(WARN, "failed to test qualification, err=%d", ret);
        }
        else if (is_qualified)
        {
          row = &curr_row_;
          HashTableRowPair *new_pair = new HashTableRowPair(pair->first, 1);
          delete pair;
          pair = NULL;
          if (common::hash::HASH_MODIFY_SUCC != hash_table_.modify_multiple(bucket_node_, bucket_hash_key, new_pair))
          {
            YYSYS_LOG(WARN, "failed to join rows, err=%d", ret);
            ret = OB_ERROR;
          }
          break;
        }
      }
    }
    else
    {
      if (left_row_count_ == 0)
      {
        ret = OB_ITER_END;
      }
      else
      {
        ret = right_op_->get_next_row(right_row);
      }
      if (OB_SUCCESS != ret)
      {
        if (OB_ITER_END == ret)
        {
          YYSYS_LOG(DEBUG, "end of right child op");
          break;
        }
        else
        {
          YYSYS_LOG(WARN, "failed to get next row from right child, err=%d", ret);
          break;
        }
      }
      OB_ASSERT(right_row);
      if (OB_SUCCESS != (ret = get_next_equijoin_left_row(left_row, *right_row, bucket_hash_key, pair)))
      {
        if (OB_ITER_END == ret)
        {
          bucket_node_ = NULL;
          left_hash_key_cache_valid_ = false;
          ret = OB_SUCCESS;
        }
      }
      else
      {
        left_hash_key_cache_valid_ = true;
        last_right_row_ = right_row;

        bool is_qualified = false;
        if (OB_SUCCESS != (ret = join_rows(*left_row, *right_row)))
        {
          YYSYS_LOG(WARN, "failed to join rows, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = curr_row_is_qualified(is_qualified)))
        {
          YYSYS_LOG(WARN, "failed to test qualification, err=%d", ret);
        }
        else if (is_qualified)
        {
          row = &curr_row_;
          HashTableRowPair *new_pair = new HashTableRowPair(pair->first, 1);
          delete pair;
          pair = NULL;
          if (common::hash::HASH_MODIFY_SUCC != hash_table_.modify_multiple(bucket_node_, bucket_hash_key, new_pair))
          {
            YYSYS_LOG(WARN, "failed to join rows, err=%d", ret);
            ret = OB_ERROR;
          }
          break;
        }
      }
    }
  }
  return ret;
}

int ObHashJoinSingle::left_hash_outer_get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  const ObRow *left_row  = NULL;
  const ObRow *inner_hash_row = NULL;

  if (left_bucket_pos_for_left_outer_join_ == -1)
  {
    if (OB_SUCCESS != (ret = inner_hash_get_next_row(inner_hash_row)))
    {
      if (OB_ITER_END != ret)
      {
        YYSYS_LOG(WARN, "failed to get next row from hash_table, err=%d", ret);
      }
      else
      {
        left_bucket_pos_for_left_outer_join_ = 0;
      }
    }
    else
    {
      row = inner_hash_row;
    }
  }

  if (left_bucket_pos_for_left_outer_join_ != -1)
  {
    if (OB_SUCCESS != (ret = get_next_leftouterjoin_left_row(left_row)))
    {
      if (OB_ITER_END != ret)
      {
        YYSYS_LOG(WARN, "failed to get next row from hash table, err=%d", ret);
      }
    }
    else
    {
      if (OB_SUCCESS != (ret = left_join_rows(*left_row)))
      {
        YYSYS_LOG(WARN, "failed to join rows, err=%d", ret);
      }
      else
      {
        row = &curr_row_;
      }
    }
  }

  return ret;
}

int ObHashJoinSingle::right_hash_outer_get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;

  const ObRow *right_row = NULL;
  const ObRow *left_row = NULL;

  HashTableRowPair *pair = NULL;
  uint64_t bucket_hash_key = 0;

  while (OB_SUCCESS == ret)
  {
    if (left_hash_key_cache_valid_)
    {
      if (OB_SUCCESS != (ret = get_next_equijoin_left_row(left_row, *last_right_row_, bucket_hash_key, pair)))
      {
        if (OB_ITER_END == ret)
        {
          bucket_node_ = NULL;
          left_hash_key_cache_valid_ = false;

          if (is_mached_ || last_right_row_ == NULL)
          {
            YYSYS_LOG(WARN, "huangcc_test::last_right_row_ is matched or last_right_row_ is NULL");
            ret = OB_SUCCESS;
          }
          else if (OB_SUCCESS != (ret = right_join_rows(*last_right_row_)))
          {
            YYSYS_LOG(WARN, "failed to join rows, err=%d", ret);
          }
          else
          {
            row = &curr_row_;
            last_right_row_ = NULL;
            break;
          }
        }
      }
      else
      {
        bool is_qualified = false;
        if (OB_SUCCESS != (ret = join_rows(*left_row, *last_right_row_)))
        {
          YYSYS_LOG(WARN, "failed to join rows, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = curr_row_is_qualified(is_qualified)))
        {
          YYSYS_LOG(WARN, "failed to test qualification, err=%d", ret);
        }
        else if (is_qualified)
        {
          row = &curr_row_;
          is_mached_ = true;
          HashTableRowPair *new_pair = new HashTableRowPair(pair->first, 1);
          delete pair;
          pair = NULL;
          if (common::hash::HASH_MODIFY_SUCC != hash_table_.modify_multiple(bucket_node_, bucket_hash_key, new_pair))
          {
            YYSYS_LOG(WARN, "failed to join rows, err=%d", ret);
            ret = OB_ERROR;
          }
          break;
        }
      }
    }
    else
    {
      ret = right_op_->get_next_row(right_row);
      if (OB_SUCCESS != ret)
      {
        if (OB_ITER_END == ret)
        {
          YYSYS_LOG(DEBUG, "end of right child op");
          break;
        }
        else
        {
          YYSYS_LOG(WARN, "failed to get next row from right child, err=%d", ret);
          break;
        }
      }
      OB_ASSERT(right_row);
      is_mached_ = false;
      
      if (OB_SUCCESS != (ret = get_next_equijoin_left_row(left_row, *right_row, bucket_hash_key, pair)))
      {
        if (OB_ITER_END == ret)
        {
          bucket_node_ = NULL;
          left_hash_key_cache_valid_ = false;
          if (OB_SUCCESS != (ret = right_join_rows(*right_row)))
          {
            YYSYS_LOG(WARN, "failed to join rows, err=%d", ret);
          }
          else
          {
            row = &curr_row_;
            last_right_row_ = NULL;
            break;
          }
        }
      }
      else
      {
        left_hash_key_cache_valid_ = true;
        last_right_row_ = right_row;

        bool is_qualified = false;
        if (OB_SUCCESS != (ret = join_rows(*left_row, *right_row)))
        {
          YYSYS_LOG(WARN, "failed to join rows, err=%d", ret);
        }
        else if (OB_SUCCESS != (ret = curr_row_is_qualified(is_qualified)))
        {
          YYSYS_LOG(WARN, "failed to test qualification, err=%d", ret);
        }
        else if (is_qualified)
        {
          row = &curr_row_;
          is_mached_ = true;
          HashTableRowPair *new_pair = new HashTableRowPair(pair->first, 1);
          delete pair;
          pair = NULL;
          if (common::hash::HASH_MODIFY_SUCC != hash_table_.modify_multiple(bucket_node_, bucket_hash_key, new_pair))
          {
            YYSYS_LOG(WARN, "failed to join rows, err=%d", ret);
            ret = OB_ERROR;
          }
          break;
        }
      }
    }
  }
  return ret;
}

int ObHashJoinSingle::full_hash_outer_get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  const ObRow *left_row  = NULL;
  const ObRow *right_hash_row = NULL;

  if (left_bucket_pos_for_left_outer_join_ == -1)
  {
    if (OB_SUCCESS != (ret = right_hash_outer_get_next_row(right_hash_row)))
    {
      if (OB_ITER_END != ret)
      {
        YYSYS_LOG(WARN, "failed to get next row from hash_table, err=%d", ret);
      }
      else
      {
        left_bucket_pos_for_left_outer_join_ = 0;
      }
    }
    else
    {
      row = right_hash_row;
    }
  }

  if (left_bucket_pos_for_left_outer_join_ != -1)
  {
    if (OB_SUCCESS != (ret = get_next_leftouterjoin_left_row(left_row)))
    {
      if (OB_ITER_END != ret)
      {
        YYSYS_LOG(WARN, "failed to get next row from hash table, err=%d", ret);
      }
    }
    else
    {
      if (OB_SUCCESS != (ret = left_join_rows(*left_row)))
      {
        YYSYS_LOG(WARN, "failed to join rows, err=%d", ret);
      }
      else
      {
        row = &curr_row_;
      }
    }
  }

  return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObHashJoinSingle, PHY_HASH_JOIN_SINGLE);
  }
}

PHY_OPERATOR_ASSIGN(ObHashJoinSingle)
{
  int ret = OB_SUCCESS;
  UNUSED(other);
  reset();
  return ret;
}
