/*
*   Version: 0.1
*
*   Authors:
*      liu jun
*        - some work details if you want
*/

#ifndef OB_CALC_INFO_H
#define OB_CALC_INFO_H

//this class is about the partition function's resolving and calculating.
#include "sql/ob_basic_stmt.h"
#include "ob_string.h"
#include "ob_rowkey.h"
#include "ob_schema.h"

using namespace oceanbase::sql;

namespace oceanbase
{
  namespace common
  {
    class ObCalcInfo
    {
    public:
      ObCalcInfo();
      inline void set_stmt_type(ObBasicStmt::StmtType stmt_type)
      {
        stmt_type_ = stmt_type;
      }

      inline ObBasicStmt::StmtType get_stmt_type() const
      {
        return stmt_type_;
      }

      inline int64_t get_obj_count() const
      {
        return row_key_.get_obj_cnt();
      }


      //add shili [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20150706:b
      int get_rowkey_order(const ObString &column_name, int64_t &column_index) const;
      void get_rowkey_obj_array(const ObObj * &obj_ptr,int64_t &count) const;

      inline void set_row_key(const ObRowkey *rowkey)
      {
        row_key_.assign(const_cast<ObObj*>(rowkey->get_obj_ptr()),rowkey->get_obj_cnt());
      }

      inline void set_schema_manager(const ObSchemaManagerV2 * schema_mgr)
      {
        schema_manager_ = schema_mgr;
      }
      inline void set_table_id(uint64_t table_id)
      {
        table_id_ = table_id;
      }
      inline uint64_t get_table_id() const
      {
        return table_id_;
      }
      //add 20150706:e
      //add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160408:b
      const ObRowkey& get_rowkey(void) const
      { return row_key_; }
      //add 20160408:e
    private:
      //add shili [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20150706:b
      uint64_t table_id_;
      ObRowkey row_key_;
      const oceanbase::common::ObSchemaManagerV2 *schema_manager_;
      //add 20150706:e
      ObBasicStmt::StmtType stmt_type_;
    };
  }
}

#endif // OB_CALC_INFO_H
