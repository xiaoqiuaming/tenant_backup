/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_alter_table_stmt.h
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#ifndef OCEANBASE_SQL_OB_ALTER_TABLE_STMT_H_
#define OCEANBASE_SQL_OB_ALTER_TABLE_STMT_H_

#include "common/hash/ob_hashmap.h"
#include "common/ob_string_buf.h"
#include "ob_column_def.h"
#include "ob_basic_stmt.h"
#include "parse_node.h"
#include "common/ob_array.h"//add wuna [MultiUps] [sql_api] 20160108

namespace oceanbase
{
  namespace sql
  {
    class ObAlterTableStmt : public ObBasicStmt
    {
    public:
      explicit ObAlterTableStmt(common::ObStringBuf* name_pool);
      ObAlterTableStmt();
      virtual ~ObAlterTableStmt();

      int init();
      void set_name_pool(common::ObStringBuf* name_pool);
      void set_has_table_rename();//add liuj [Alter_Rename] [JHOBv0.1] 20150104
      int set_table_name(ResultPlan& result_plan, const common::ObString& table_name);
      int set_new_table_name(ResultPlan& result_plan, const common::ObString& table_name);
      int add_column(ResultPlan& result_plan, const ObColumnDef& column_def);
      int drop_column(ResultPlan& result_plan, const ObColumnDef& column_def);
      int rename_column(ResultPlan& result_plan, const ObColumnDef& column_def);
      int alter_column(ResultPlan& result_plan, const ObColumnDef& column_def);
      const bool has_table_rename() const;//add liuj [Alter_Rename] [JHOBv0.1] 20150104
      const uint64_t get_table_id() const;
      const common::ObString& get_db_name() const;//add dolphin [database manager]@20150609
      int set_db_name(ResultPlan& result_plan,const common::ObString& dbname);//add dolphin [database manager]@20150609
      const common::ObString& get_table_name() const;
      const common::ObString& get_new_table_name() const;
      common::hash::ObHashMap<common::ObString, ObColumnDef>::iterator column_begin();
      common::hash::ObHashMap<common::ObString, ObColumnDef>::iterator column_end();
      //add wuna [MultiUps] [sql_api] 20160108:b
      common::ObTablePartitionType get_partition_type() const;
      void set_partition_type(common::ObTablePartitionType type);
      const common::ObArray<common::ObString>& get_col_list() const;
      void set_col_list(const common::ObArray<common::ObString>& col_list);
      int64_t get_col_list_size() const;
      const common::ObString& get_func_name() const;
      int set_func_name(ResultPlan& result_plan, const common::ObString& func_name);
      const common::ObString& get_group_name_prefix() const;
      int set_group_name_prefix(ResultPlan& result_plan, const common::ObString& group_name_prefix);
      const bool is_rule_modify() const;
      void set_is_rule_modify();
      int add_col_list(ResultPlan& result_plan, const common::ObString& column_name);
      bool is_duplicate_column_name(const common::ObString& column_name);
      int check_column_name(ResultPlan& result_plan, const common::ObString& column_name);
      bool is_valid_data_type(common::ObObjType& data_type);
      //add 20160108:e
      virtual void print(FILE* fp, int32_t level, int32_t index = 0);

      const common::ObString& get_new_expire_info() const;
      
      int set_new_expire_info(const common::ObString& expire_info);

      int set_expire_info_null();

      void set_is_drop_expire_info();

      const bool is_drop_expire_info() const;

      void set_is_expire_info_modify();

      const bool is_expire_info_modify() const;

      void set_is_add_expire_info();

      const bool is_add_expire_info() const;

      const bool is_load_type_modify() const;

      void set_is_load_type_modify();

      void set_is_block_cache_use(bool use_block_cache);

      const bool get_is_block_cache_use() const;


    protected:
      common::ObStringBuf*        name_pool_;

    private:
      uint64_t                    table_id_;
      common::ObString            db_name_;//add dolphin [database manger]@20150609
      common::ObString            table_name_;
      common::ObString            new_table_name_;
      uint64_t                    max_column_id_;
      bool                        has_table_rename_;//add liuj [Alter_Rename] [JHOBv0.1] 20150104
      common::hash::ObHashMap<common::ObString, ObColumnDef> columns_map_;
      //add wuna [MultiUps] [sql_api] 20160108:b
      common::ObArray<common::ObString>   col_list_;
      common::ObTablePartitionType     part_type_;
      common::ObString            func_name_;
      common::ObString            group_name_prefix_;
      bool                        is_rule_modify_;
      //add 20160108:e
      common::ObString  new_expire_info_;
      bool is_drop_expire_info_;
      bool is_expire_info_modify_;
      bool is_add_expire_info_;
      bool is_load_type_modify_;
      bool is_use_block_cache_;
    };

    inline const common::ObString& ObAlterTableStmt::get_new_expire_info() const
    {
      return new_expire_info_;
    }

    inline void ObAlterTableStmt::set_is_drop_expire_info()
    {
      is_drop_expire_info_ = true;
    }

    inline const bool ObAlterTableStmt::is_drop_expire_info() const
    {
      return is_drop_expire_info_;
    }

    inline const bool ObAlterTableStmt::is_expire_info_modify() const
    {
      return is_expire_info_modify_;
    }

    inline void ObAlterTableStmt::set_is_expire_info_modify()
    {
      is_expire_info_modify_ = true;
    }

    inline void ObAlterTableStmt::set_is_add_expire_info()
    {
      is_add_expire_info_ = true;
    }

    inline const bool ObAlterTableStmt::is_add_expire_info() const
    {
      return is_add_expire_info_;
    }

    inline const bool ObAlterTableStmt::is_load_type_modify() const
    {
      return is_load_type_modify_;
    }

    inline void ObAlterTableStmt::set_is_load_type_modify()
    {
      is_load_type_modify_ = true;
    }

    inline void ObAlterTableStmt::set_is_block_cache_use(bool use_block_cache)
    {
      is_use_block_cache_ = use_block_cache;
    }

    inline const bool ObAlterTableStmt::get_is_block_cache_use() const
    {
      return is_use_block_cache_;
    }

    inline void ObAlterTableStmt::set_name_pool(common::ObStringBuf* name_pool)
    {
      name_pool_ = name_pool;
    }
    inline const uint64_t ObAlterTableStmt::get_table_id() const
    {
      return table_id_;
    }
    //add liuj [Alter_Rename] [JHOBv0.1] 20150104
    inline const bool ObAlterTableStmt::has_table_rename() const
    {
      return has_table_rename_;
    }
    inline void ObAlterTableStmt::set_has_table_rename()
    {
      has_table_rename_ = true;
    }
    //add e.
    //add dolphin [database manager]@20150609
    inline const common::ObString& ObAlterTableStmt::get_db_name() const
    {
      return db_name_;
    }


    //add:e
    inline const common::ObString& ObAlterTableStmt::get_table_name() const
    {
      return table_name_;
    }
    inline const common::ObString& ObAlterTableStmt::get_new_table_name() const
    {
      return new_table_name_;
    }
    inline common::hash::ObHashMap<common::ObString, ObColumnDef>::iterator ObAlterTableStmt::column_begin()
    {
      return columns_map_.begin();
    }
    inline common::hash::ObHashMap<common::ObString, ObColumnDef>::iterator ObAlterTableStmt::column_end()
    {
      return columns_map_.end();
    }
    //add wuna [MultiUps] [sql_api] 20160108:b
    inline const common::ObArray<common::ObString>& ObAlterTableStmt::get_col_list() const
    {
      return col_list_;
    }
    inline void ObAlterTableStmt::set_col_list(const common::ObArray<common::ObString>& col_list)
    {
      col_list_ = col_list;
    }
    inline int64_t ObAlterTableStmt::get_col_list_size() const
    {
      return col_list_.count();
    }
    inline void ObAlterTableStmt::set_partition_type(common::ObTablePartitionType type)
    {
      part_type_ = type;
    }
    inline common::ObTablePartitionType ObAlterTableStmt::get_partition_type() const
    {
      return part_type_;
    }
    inline const common::ObString& ObAlterTableStmt::get_func_name() const
    {
      return func_name_;
    }
    inline const common::ObString& ObAlterTableStmt::get_group_name_prefix() const
    {
      return group_name_prefix_;
    }
    inline const bool ObAlterTableStmt::is_rule_modify() const
    {
      return is_rule_modify_;
    }
    inline void ObAlterTableStmt::set_is_rule_modify()
    {
      is_rule_modify_ = true;
    }
    //add 20160108:e
  }
}

#endif //OCEANBASE_SQL_OB_ALTER_TABLE_STMT_H_


