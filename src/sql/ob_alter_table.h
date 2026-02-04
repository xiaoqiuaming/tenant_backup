/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_alter_table.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#ifndef OCEANBASE_SQL_OB_ALTER_TABLE_H_
#define OCEANBASE_SQL_OB_ALTER_TABLE_H_

#include "sql/ob_no_children_phy_operator.h"
#include "common/ob_schema_service.h"
#include "sql/ob_sql_context.h"
//add liu jun. [MultiUPS] [part_cache] 20150626:b
#include "common/ob_partition_monitor.h"
//20150626:e

namespace oceanbase
{
  namespace mergeserver
  {
    class ObMergerRootRpcProxy;
  } // end namespace mergeserver

  namespace sql
  {
    class ObAlterTable: public ObNoChildrenPhyOperator
    {
      public:
        ObAlterTable();
        virtual ~ObAlterTable();
        virtual void reset();
        virtual void reuse();
        // init
        void set_sql_context(ObSqlContext& context);
        common::AlterTableSchema& get_alter_table_schema();

        /// execute the create table statement
        virtual int open();
        virtual int close();
        virtual ObPhyOperatorType get_type() const
        { return PHY_ALTER_TABLE; }
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        /// @note always return OB_ITER_END
        virtual int get_next_row(const common::ObRow *&row);
        /// @note always return OB_NOT_SUPPORTED
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        //add wuna [MultiUps][sql_api] 20160111:b
        int execute_alter_rule_stmt();
        int fill_sql_context(int& retry_num,int retry_times);
        int release_sql_context();
        //add 20160111:e
      private:
        // disallow copy
        ObAlterTable(const ObAlterTable &other);
        ObAlterTable& operator=(const ObAlterTable &other);
      private:
        // data members
        common::AlterTableSchema   alter_schema_;
        ObSqlContext local_context_;
    };

    inline common::AlterTableSchema& ObAlterTable::get_alter_table_schema()
    {
      return alter_schema_;
    }
    
    inline int ObAlterTable::get_next_row(const common::ObRow *&row)
    {
      row = NULL;
      return common::OB_ITER_END;
    }

    inline int ObAlterTable::get_row_desc(const common::ObRowDesc *&row_desc) const
    {
      row_desc = NULL;
      return common::OB_NOT_SUPPORTED;
    }

    inline void ObAlterTable::set_sql_context(ObSqlContext& context)
    {
      local_context_ = context;
      local_context_.schema_manager_ = NULL;
      //add liu jun. [MultiUPS] [part_cache] 20150626:b
      local_context_.partition_mgr_ = NULL;
      //20150626:e
    }
  } // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_ALTER_TABLE_H_ */

