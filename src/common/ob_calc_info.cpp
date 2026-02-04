/*
*   Version: 0.1
*
*   Authors:
*      liu jun
*        - some work details if you want
*/

#include "ob_calc_info.h"

using namespace oceanbase::common;

ObCalcInfo::ObCalcInfo():table_id_(0)
{
}

//add shili [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20150705:b
//obj_ptr [out]row_key obobj 指针
//count  [out] row_key obobj的数量
void ObCalcInfo::get_rowkey_obj_array(const ObObj * &obj_ptr,int64_t &count) const
{
  obj_ptr=row_key_.get_obj_ptr();
  count = row_key_.get_obj_cnt();
}
//add 20150705:e

//add shili [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20150705:b
//obj_ptr [out]row_key obobj 指针
//count  [out] row_key obobj的数量
int ObCalcInfo::get_rowkey_order(const ObString &column_name, int64_t &column_index) const
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  const ObRowkeyInfo *rowkey_info = NULL;
  if(table_id_ > 0)
  {
    if(NULL == (table_schema = schema_manager_->get_table_schema(table_id_)))
    {
      ret = OB_ERR_ILLEGAL_ID;
      YYSYS_LOG(WARN, "fail to get table schema for table[%ld]", table_id_);
    }
    else
    {
      rowkey_info = &table_schema->get_rowkey_info();
      int64_t rowkey_size = rowkey_info->get_size();
      const ObColumnSchemaV2 *column_schema = NULL;
      uint64_t column_id = OB_INVALID;
      if(rowkey_size != row_key_.get_obj_cnt())
      {
        ret = OB_ERR_ROWKEY_SIZE;
        YYSYS_LOG(WARN, "different rowkey size");
      }
      else
      {
        bool had_find_column_index = false;
        for(int64_t index = 0; index < rowkey_size; index++)
        {
          rowkey_info->get_column_id(index, column_id);
          if(NULL == (column_schema = schema_manager_->get_column_schema(table_id_, column_id)))
          {
            ret = OB_ERR_COLUMN_NOT_FOUND;
            YYSYS_LOG(WARN, "Get column item failed,ret=%d",ret);
            break;
          }
          else
          {
            int32_t  compare_ret = 0;
            const char *name = column_schema->get_name();
            if(0 == (compare_ret = column_name.compare(name)))
            {
              had_find_column_index = true;
              column_index = index;
              break;
            }
          }
        }
        if(false == had_find_column_index)
        {
          ret = OB_ERR_COLUMN_NAME_NOT_EXIST;
          YYSYS_LOG(WARN, "don't find the column_name:%s", column_name.ptr());
        }
      }
    }
  }
  else
  {
    ret = OB_ERR_TABLE_ID;
    YYSYS_LOG(WARN, "error table_id:%ld", table_id_);
  }
  return ret;
}
//add 20150706:e



