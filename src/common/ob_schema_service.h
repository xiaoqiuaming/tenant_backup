/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_schema_service.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *   - ����schema������ݽṹ��������ɾ������ȡschema�����ṹ�ӿ�
 *
 */

#ifndef _OB_SCHEMA_SERVICE_H
#define _OB_SCHEMA_SERVICE_H

#include "common/ob_define.h"
#include "ob_object.h"
#include "ob_string.h"
#include "ob_array.h"
#include "ob_hint.h"
#include "ob_strings.h" //[view]

namespace oceanbase
{
  namespace common
  {
    typedef ObObjType ColumnType;
    /* 表单join关系描述，对应于__all_join_info内部�? */
    //add wenghaixing [secondary index] 20141029
    typedef struct IndexStruct
    {
        uint64_t tbl_tid;
        IndexStatus status;
      public:
        IndexStruct()
        {
          tbl_tid = OB_INVALID_ID;
          status = ERROR;
        }
        NEED_SERIALIZE_AND_DESERIALIZE;
    }
    IndexHelper;
    //add e
    //add wenghaixing [secondary index static_index_build.heartbeat]20150528
    typedef struct IndexBeat
    {
        uint64_t idx_tid;
        IndexStatus status;
        int64_t hist_width;
        int64_t start_time;
        IndexPhase index_phase_;//add liumz, [static_index range_intersect]20170915
        IndexBeat()
        {
          idx_tid = OB_INVALID_ID;
          status = ERROR;
          hist_width = 0;
          start_time = 0;
          index_phase_= INIT_PHASE;
        }
        void reset()
        {
          idx_tid = OB_INVALID_ID;
          status = ERROR;
          hist_width = 0;
          start_time = 0;
          index_phase_= INIT_PHASE;
        }
        NEED_SERIALIZE_AND_DESERIALIZE;
    }IndexBeat;
    //add e
    struct JoinInfo
    {
        char left_table_name_[OB_MAX_TABLE_NAME_LENGTH];
        uint64_t left_table_id_;
        char left_column_name_[OB_MAX_COLUMN_NAME_LENGTH];
        uint64_t left_column_id_;
        char right_table_name_[OB_MAX_TABLE_NAME_LENGTH];
        uint64_t right_table_id_;
        char right_column_name_[OB_MAX_COLUMN_NAME_LENGTH];
        uint64_t right_column_id_;

        int64_t to_string(char* buf, const int64_t buf_len) const
        {
          int64_t pos = 0;
          databuff_printf(buf, buf_len, pos, "left_table:tname[%s], tid[%lu], cname[%s], cid[%lu]",
                          left_table_name_, left_table_id_, left_column_name_, left_column_id_);
          databuff_printf(buf, buf_len, pos, "right_table:tname[%s], tid[%lu], cname[%s], cid[%lu]",
                          right_table_name_, right_table_id_, right_column_name_, right_column_id_);
          return pos;
        }

        NEED_SERIALIZE_AND_DESERIALIZE;
    };

    /* ����column��������Ӧ��__all_all_column�ڲ��� */
    struct ColumnSchema
    {
        char column_name_[OB_MAX_COLUMN_NAME_LENGTH];
        char new_column_name_[OB_MAX_COLUMN_NAME_LENGTH];//add liuj [Alter_Rename] [JHOBv0.1] 20150104
        uint64_t column_id_;
        uint64_t column_group_id_;
        int64_t rowkey_id_;
        uint64_t join_table_id_;
        uint64_t join_column_id_;
        ColumnType data_type_;
        int64_t data_length_;
        int64_t data_precision_;
        int64_t data_scale_;
        bool nullable_;
        char default_value_[OB_MAX_DEBUG_MSG_LEN];
        bool default_value_is_null;
        int64_t length_in_rowkey_; //�����rowkey�У����ʾ�ڶ�����rowkey����ռ�õ��ֽ�����
        int32_t order_in_rowkey_;
        ObCreateTime gm_create_;
        ObModifyTime gm_modify_;
        ColumnSchema():column_id_(OB_INVALID_ID), column_group_id_(OB_INVALID_ID), rowkey_id_(-1),
          join_table_id_(OB_INVALID_ID), join_column_id_(OB_INVALID_ID), data_type_(ObMinType),
          data_precision_(0), data_scale_(0), nullable_(true), length_in_rowkey_(0), order_in_rowkey_(0)
        {
          column_name_[0] = '\0';
          new_column_name_[0] = '\0';//add liuj [Alter_Rename] [JHOBv0.1] 20150104
          default_value_[0] = '\0';
          default_value_is_null = true;
        }
        NEED_SERIALIZE_AND_DESERIALIZE;
    };

    /* ����schema��������Ӧ��__all_all_table�ڲ��� */
    struct TableSchema
    {
        enum TableType
        {
          NORMAL = 1,
          INDEX,
          META,
          VIEW,
        };

        enum WithCheckOption
        {
            OB_WITH_NONE_CHECK_OPTION = 0,
            OB_WITH_CASCADED_CHECK_OPTION = 1,
            OB_WITH_LOCAL_CHECK_OPTION = 2,
        };

        enum LoadType
        {
          INVALID = 0,
          DISK = 1,
          MEMORY
        };

        enum TableDefType
        {
          INTERNAL = 1,
          USER_DEFINED
        };

        enum SSTableVersion
        {
          OLD_SSTABLE_VERSION = 1,
          DEFAULT_SSTABLE_VERSION = 2,
          NEW_SSTABLE_VERSION = 3,
        };

        char table_name_[OB_MAX_TABLE_NAME_LENGTH];
        char dbname_[OB_MAX_DATBASE_NAME_LENGTH]; //add dophin [database manager]@20150609
        //add liu jun. [MultiUPS] [sql_api] 20150423:b
        char partition_func_name_[OB_MAX_TABLE_NAME_LENGTH];
        //mod wuna [MultiUPS][sql_api] 20151207:b
        //char group_name_prefix_[OB_MAX_TABLE_NAME_LENGTH];
        //char param_list_[OB_MAX_TABLE_NAME_LENGTH];
        char group_name_prefix_[OB_MAX_VARCHAR_LENGTH];
        char param_list_[OB_MAX_VARCHAR_LENGTH];
        char partition_func_rule_body_[OB_MAX_VARCHAR_LENGTH];
        //mod 20151207:e
        //add 20150423:e
        char compress_func_name_[OB_MAX_TABLE_NAME_LENGTH];
        char expire_condition_[OB_MAX_EXPIRE_CONDITION_LENGTH];
        char comment_str_[OB_MAX_TABLE_COMMENT_LENGTH];
        uint64_t table_id_;
        TableType table_type_;
        LoadType load_type_;
        TableDefType table_def_type_;
        bool is_use_bloomfilter_;
        bool is_pure_update_table_;
        //add liu jun. [MultiUPS] [sql_api] 20150423:b
        bool is_rule_modify_;
        int32_t table_partition_type_;
        int64_t partition_key_col_num_;
        int64_t partition_func_type_;//add wuna[MultiUPS] [sql_api] 20151208
        int64_t partition_form_;//add wuna[MultiUPS] [sql_api] 20151208
        int64_t consistency_level_;
        int64_t rowkey_split_;
        int32_t rowkey_column_num_;
        int32_t replica_num_;
        int64_t max_used_column_id_;
        int64_t create_mem_version_;
        int64_t tablet_block_size_;
        int64_t tablet_max_size_;
        int64_t max_rowkey_length_;
        int64_t merge_write_sstable_version_;
        int64_t schema_version_;
        uint64_t create_time_column_id_;
        uint64_t modify_time_column_id_;
        ObArray<ColumnSchema> columns_; //
        ObArray<JoinInfo> join_info_;
        //add wenghaixing [secondary index] 20141029
        IndexHelper ih_;
        //uint64_t index_list_[OB_MAX_INDEX_NUMS];//add wenghaixing [secondary index] 20141104
        //add e

        //[view]
        WithCheckOption with_check_option_;
        bool is_updatable_;
        char text_[OB_MAX_SQL_LENGTH];

      public:
        TableSchema()
          :table_id_(OB_INVALID_ID),
            table_type_(NORMAL),
            load_type_(DISK),
            table_def_type_(USER_DEFINED),
            is_use_bloomfilter_(false),
            is_pure_update_table_(false),
            //add liu jun. [MultiUPS] [sql_api] 20150423:b
            is_rule_modify_ (false),
            table_partition_type_(OB_TABLE_PARTITON_MAX_TYPE),
            partition_key_col_num_(0),
            //add 20150423:e
            partition_func_type_(OB_FUNC_TYPE_NUM),//add wuna[MultiUPS] [sql_api] 20151208
            partition_form_(OB_UNKNOWN_FORM_PARTITION),//add wuna[MultiUPS] [sql_api] 20151208
            consistency_level_(NO_CONSISTENCY),
            rowkey_split_(0),
            rowkey_column_num_(0),
            //mod peiouya [MultiUPS] [Create_table] 20150514:b
            //replica_num_(OB_SAFE_COPY_COUNT),
            replica_num_(OB_DEFAULT_COPY_COUNT),
            max_used_column_id_(0),
            create_mem_version_(0),
            tablet_block_size_(OB_DEFAULT_SSTABLE_BLOCK_SIZE),
            tablet_max_size_(OB_DEFAULT_MAX_TABLET_SIZE),
            max_rowkey_length_(0),
            merge_write_sstable_version_(DEFAULT_SSTABLE_VERSION),
            schema_version_(0),
            create_time_column_id_(OB_CREATE_TIME_COLUMN_ID),
            modify_time_column_id_(OB_MODIFY_TIME_COLUMN_ID)
          //[view]
          ,with_check_option_(OB_WITH_NONE_CHECK_OPTION)
          ,is_updatable_(false)
        {
          table_name_[0] = '\0';
          dbname_[0] = '\0'; //add dophin [database manager]@20150609
          //add liu jun. [MultiUPS] [sql_api] 20141118:b
          //  partition_func_name_[0] = '\0';
          memset(partition_func_name_,0,OB_MAX_TABLE_NAME_LENGTH);
          //  group_name_prefix_[0] = '\0';
          memset(group_name_prefix_,0,OB_MAX_VARCHAR_LENGTH);
          //  param_list_[0] = '\0';
          memset(param_list_,0,OB_MAX_VARCHAR_LENGTH);
          //  partition_func_rule_body_[0] = '\0';
          memset(partition_func_rule_body_,0,OB_MAX_VARCHAR_LENGTH);
          //add 20141118:e
          compress_func_name_[0] = '\0';
          expire_condition_[0] = '\0';
          comment_str_[0] = '\0';
          //memset(text_, 0, OB_MAX_SQL_LENGTH); //[view]
          text_[0] = '\0';
        }
        //add wenghaixing[secondary index] 20141029
        inline void add_index_helper(const IndexHelper& ih)
        {
          ih_=ih;
        }
        //add e
        //add wenghaixing [secondary index]20141104_02
        inline IndexHelper get_index_helper()
        {
          return ih_;
        }
        //add e
        inline void init_as_inner_table()
        {
          // clear all the columns at first
          clear();
          this->table_id_ = OB_INVALID_ID;
          this->table_type_ = TableSchema::NORMAL;
          this->load_type_ = TableSchema::DISK;
          this->table_def_type_ = TableSchema::INTERNAL;
          //mod peiouya [MultiUPS] [Create_table] 20150514:b
          //add zhaoqiong[roottable tablet management]20150302:b
          //this->replica_num_ = OB_SAFE_COPY_COUNT;
          //this->replica_num_ = OB_MAX_COPY_COUNT;
          //add e
          this->replica_num_ = OB_DEFAULT_COPY_COUNT;
          //mod:e
          this->create_mem_version_ = 1;
          this->tablet_block_size_ = OB_DEFAULT_SSTABLE_BLOCK_SIZE;
          this->tablet_max_size_ = OB_DEFAULT_MAX_TABLET_SIZE;
          strcpy(this->compress_func_name_, OB_DEFAULT_COMPRESS_FUNC_NAME);
          this->is_use_bloomfilter_ = false;
          this->is_pure_update_table_ = false;
          this->consistency_level_ = NO_CONSISTENCY;
          this->rowkey_split_ = 0;
          this->merge_write_sstable_version_ = DEFAULT_SSTABLE_VERSION;
          this->create_time_column_id_ = OB_CREATE_TIME_COLUMN_ID;
          this->modify_time_column_id_ = OB_MODIFY_TIME_COLUMN_ID;

          //[view]
          this->with_check_option_ = OB_WITH_NONE_CHECK_OPTION;
          this->is_updatable_ = false;
        }

        inline int add_column(const ColumnSchema& column)
        {
          return columns_.push_back(column);
        }

        inline int add_join_info(const JoinInfo& join_info)
        {
          return join_info_.push_back(join_info);
        }

        inline int64_t get_column_count(void) const
        {
          return columns_.count();
        }
        inline ColumnSchema* get_column_schema(const int64_t index)
        {
          ColumnSchema * ret = NULL;
          if ((index >= 0) && (index < columns_.count()))
          {
            ret = &columns_.at(index);
          }
          return ret;
        }
        inline ColumnSchema* get_column_schema(const uint64_t column_id)
        {
          ColumnSchema *ret = NULL;
          for(int64_t i = 0; i < columns_.count(); i++)
          {
            if (columns_.at(i).column_id_ == column_id)
            {
              ret = &columns_.at(i);
              break;
            }
          }
          return ret;
        }
        inline ColumnSchema* get_column_schema(const char * column_name)
        {
          ColumnSchema *ret = NULL;
          for(int64_t i = 0; i < columns_.count(); i++)
          {
            if (strcmp(column_name, columns_.at(i).column_name_) == 0)
            {
              ret = &columns_.at(i);
              break;
            }
          }
          return ret;
        }
        inline const ColumnSchema* get_column_schema(const char * column_name) const
        {
          const ColumnSchema *ret = NULL;
          for(int64_t i = 0; i < columns_.count(); i++)
          {
            if (strcmp(column_name, columns_.at(i).column_name_) == 0)
            {
              ret = &columns_.at(i);
              break;
            }
          }
          return ret;
        }
        inline int get_column_rowkey_index(const uint64_t column_id, int64_t &rowkey_idx)
        {
          int ret = OB_SUCCESS;
          rowkey_idx = -1;
          for (int64_t i = 0; i < columns_.count(); i++)
          {
            if (columns_.at(i).column_id_ == column_id)
            {
              rowkey_idx = columns_.at(i).rowkey_id_;
            }
          }
          if (-1 == rowkey_idx)
          {
            YYSYS_LOG(WARN, "fail to get column. column_id=%lu", column_id);
            ret = OB_ENTRY_NOT_EXIST;
          }
          return ret;
        }
        //add liu jun.[MultiUPS] [sql_api] 20150424:b
        bool is_rowkey_column(const ObString& column_str) const
        {
          bool flag = false;
          char column_name[OB_MAX_TABLE_NAME_LENGTH];
          int32_t length = column_str.length();
          snprintf(column_name, length + 1, column_str.ptr());
          const ColumnSchema* column_schema = get_column_schema(column_name);
          if(NULL == column_schema)
          {
          }
          else if(column_schema->rowkey_id_ > 0)
          {
            flag = true;
          }
          return flag;
        }
        // add get DEL column index for alter table to do DEL Column operation dolphin-SchemaValidation@20150516:b
        /*
       *@param [in] column name
       * @param [out] index indicate the column order in the columns array
       * @exception -1 if not found
       * @version 1.0
       * */
        inline int64_t get_column_schema_index(const char * column_name)
        {
          int64_t ret = -1;
          for(int64_t i = 0; i < columns_.count(); ++i)
          {
            if (strcmp(column_name, columns_.at(i).column_name_) == 0)
            {
              ret = i;
              break;
            }
          }
          return ret;
        }// add:e
        void clear();
        bool is_valid() const;
        //add wenghaixing [secondary index] 20141103
        bool is_valid_index() const;
        //add e
        int to_string(char* buffer, const int64_t length) const;

        static bool is_system_table(const common::ObString &tname);
        //add wenghaixing [secondary index drop table_with_index]20150126
        static bool is_system_table_v2(const common::ObString &tname);
        //add e
        //add liu jun.[MultiUPS] [sql_api] 20150427:b
        static bool is_valid_table_for_update(const common::ObString &tname);
        //add 20150427:e
        NEED_SERIALIZE_AND_DESERIALIZE;
    };
    //add liu jun.[MultiUPS] [sql_api] 20150427:b
    inline bool TableSchema::is_valid_table_for_update(const common::ObString &tname)
    {
      bool flag = true;
      UNUSED(tname);
      return flag;
    }
    //add 20150427:e

    inline bool TableSchema::is_system_table(const common::ObString &tname)
    {
      bool ret = false;
      if (tname.length() >= 1)
      {
        const char *p = tname.ptr();
        //modify by wenghaixing [secondary index drop index]20141223

        //if ('_' == p[0]) //old code
        if('_'==p[0]&&'_'==p[1]&&!isdigit(p[2]))
          //modify e
        {
          ret = true;
        }
      }
      return ret;
    }

    //add wenghaixing [secondary index drop table_with_index]20150126
    inline bool TableSchema::is_system_table_v2(const common::ObString &tname)
    {
      bool ret = false;
      if (tname.length() >= 1)
      {
        const char *p = tname.ptr();
        if ('_' == p[0]) //old code
        {
          ret = true;
        }
      }
      return ret;
    }

    //add e

    // for alter table add or drop columns rpc construct
    struct AlterTableSchema
    {
      public:
        enum AlterType
        {
          ADD_COLUMN = 0,
          DEL_COLUMN = 1,
          MOD_COLUMN = 2,
          //add fyd [NotNULL_check] [JHOBv0.1] 20140108:b
          MOD_COLUMN_NULL=3,
          //add 20140108:e
          RENAME_COLUMN = 4,//add liuj [Alter_Rename] [JHOBv0.1] 20150104
          MOD_VARCHAR_LENGTH = 5,
          MOD_COLUMN_DEFAULT = 6,
          MOD_DECIMAL_PRECISION = 7,
        };
        struct AlterColumnSchema
        {
            AlterType type_;
            ColumnSchema column_;
        };
        // get table name
        const char * get_table_name(void) const
        {
          return table_name_;
        }
        //add liuj [Alter_Rename] [JHOBv0.1] 20150104
        const char * get_new_table_name(void) const
        {
          return new_table_name_;
        }
        //add e.
        const char *get_new_expire_info(void) const
        {
          return new_expire_info_;
        }
        // add new column
        int add_column(const AlterType type, const ColumnSchema& column)
        {
          AlterColumnSchema schema;
          schema.type_ = type;
          schema.column_ = column;
          return columns_.push_back(schema);
        }
        // clear all
        void clear(void)
        {
          table_id_ = OB_INVALID_ID;
          dbname_[0]=0;//add dolphin [database manager]@20150609
          has_table_rename_ = false;//add liuj [Alter_Rename] [JHOBv0.1] 20150104
          table_name_[0] = '\0';
          new_table_name_[0] = '\0';//add liuj [Alter_Rename] [JHOBv0.1] 20150104
          //add wuna [MultiUPS] [sql_api] 20150109:b
          memset(partition_func_name_,0,OB_MAX_TABLE_NAME_LENGTH);
          memset(group_name_prefix_,0,OB_MAX_VARCHAR_LENGTH);
          memset(param_list_,0,OB_MAX_VARCHAR_LENGTH);
          is_rule_modify_ = false;
          //add 20150109:e
          new_expire_info_[0] = '\0';
          is_drop_expire_info_ = false;
          is_alter_expire_info_ = false;
          is_load_type_modify_ = false;
          is_use_block_cache_ = false;
          columns_.clear();
        }
        int64_t get_column_count(void) const
        {
          return columns_.count();
        }
        NEED_SERIALIZE_AND_DESERIALIZE;
        uint64_t table_id_;
        char dbname_[OB_MAX_DATBASE_NAME_LENGTH];//add dolphin [database manager]@20150609
        bool has_table_rename_;//add liuj [Alter_Rename] [JHOBv0.1] 20150104
        char new_table_name_[OB_MAX_TABLE_NAME_LENGTH];//add liuj [Alter_Rename] [JHOBv0.1] 20150104
        char table_name_[OB_MAX_TABLE_NAME_LENGTH];
        ObArray<AlterColumnSchema> columns_;
        //add wuna [MultiUPS] [sql_api] 20150109:b
        bool is_rule_modify_;
        char partition_func_name_[OB_MAX_TABLE_NAME_LENGTH];
        char group_name_prefix_[OB_MAX_VARCHAR_LENGTH];
        char param_list_[OB_MAX_VARCHAR_LENGTH];
        int32_t partition_type_;
        int32_t partition_key_col_num_;
        //add 20150109:e
        bool is_drop_expire_info_;
        bool is_alter_expire_info_;
        char new_expire_info_[OB_MAX_EXPIRE_CONDITION_LENGTH];
        bool is_load_type_modify_;
        bool is_use_block_cache_;
    };

    // ob table schema service interface layer
    class ObScanHelper;
    //add zhaoqiong [Schema Manager] 20150327:b
    class ObSchemaMutator;
    class ObMutator;
    class ObSchemaManagerV2;
    //add:e
    class ObSchemaService
    {
      public:
        virtual ~ObSchemaService()
        {}
        virtual int init(ObScanHelper* client_proxy, bool only_core_tables) = 0;
        //add zhaoqiong [Schema Manager] 20150327:b
        virtual int64_t get_schema_version() const = 0;
        virtual int set_schema_version(int64_t timestamp) = 0;
        virtual int refresh_schema() = 0;
        //add:e
        virtual int get_table_schema(const ObString& table_name, TableSchema& table_schema,const ObString& dbname = ObString::make_string("")) = 0;
        //virtual int create_table(const TableSchema& table_schema) = 0; //uncertainty ups�����ӱ���
        virtual int create_table(const TableSchema& table_schema,
                                 int64_t frozen_version = OB_INVALID_FROZEN_VERSION,
                                 common::ObArray<int32_t> *paxos_id_array = NULL,
                                 common::ObArray<ObString> *group_name_list = NULL) = 0;

        //      virtual int drop_table(const ObString& table_name) = 0;
        virtual int drop_table(const ObString& table_name, uint64_t new_table_id = OB_INVALID_ID) = 0;
        //      virtual int alter_table(const AlterTableSchema& table_schema, const int64_t old_schema_version) = 0;
        virtual int alter_table(const AlterTableSchema& table_schema, const int64_t old_schema_version, ObSchemaManagerV2 *schema_manager) = 0;
        //add fyd [NotNULL_check] [JHOBv0.1] 20140108:b
        //      virtual int alter_table(const AlterTableSchema& table_schema, TableSchema& old_table_schema,int64_t frozen_version = OB_INVALID_FROZEN_VERSION,common::ObArray<int32_t> *paxos_id_array = NULL,common::ObArray<ObString> *group_name_list = NULL) = 0;
        virtual int alter_table(const AlterTableSchema& table_schema,
                                TableSchema& old_table_schema,
                                ObSchemaManagerV2 *schema_manager,
                                int64_t frozen_version = OB_INVALID_FROZEN_VERSION,
                                common::ObArray<int32_t> *paxos_id_array = NULL,
                                common::ObArray<ObString> *group_name_list = NULL) = 0;
        //add 20140108:e
        virtual int get_table_id(const ObString& table_name, uint64_t& table_id) = 0;
        virtual int get_table_name(uint64_t table_id, ObString& table_name) = 0;
        virtual int get_max_used_table_id(uint64_t &max_used_tid) = 0;
        //      virtual int modify_table_id(TableSchema& table_schema, const int64_t new_table_id) = 0;
        virtual int modify_table_id(TableSchema& table_schema, const int64_t new_table_id, const int64_t frozen_version) = 0;
        virtual int set_max_used_table_id(const uint64_t max_used_tid) = 0;
        //add zhaoqiong [Schema Manager] 20150327:b
        /**
         * @brief fetch schema_mutator from system table
         * @param start_version: local schema timestamp
         * @param end_version: new schema timestamp
         * @param schema_mutator [out]
         */
        virtual int fetch_schema_mutator(const int64_t start_version, const int64_t end_version, ObSchemaMutator& schema_mutator) = 0;
        virtual int get_schema(bool only_core_tables, ObSchemaManagerV2& out_schema) = 0;
        virtual int add_ddl_operation(ObMutator* mutator, const uint64_t& table_id, const DdlType ddl_type) = 0;
        //add:e
        //add wenghaixing [secondary index col checksum] 20141208
        //modify liuxiao [muti database] 20150702
        //virtual int modify_index_stat(ObString index_table_name,uint64_t index_table_id,int status)=0;
        virtual int modify_index_stat(ObString index_table_name,uint64_t index_table_id,ObString db_name,int status)=0;
        //modify e
        //add e
        //add liuxiao [secondary index col checksum] 20150316
        virtual int clean_checksum_info(const int64_t max_draution_of_version,const int64_t current_version) = 0;
        virtual int check_column_checksum(const int64_t orginal_table_id,const int64_t index_table_id,const int64_t cluster_id, const int64_t current_version, bool &is_right) = 0;
        //virtual int prepare_checksum_info_row(const uint64_t table_id ,const ObRowkey& rowkey,const ObObj &column_check_sum ,ObMutator *mutator);
        virtual int get_checksum_info(const ObNewRange new_range,const int64_t cluster_id,const int64_t required_version,ObString& cchecksum) = 0;
        //add e
        virtual int get_checksum_info(const ObNewRange new_range, const int64_t required_version, ObString &cchecksum) = 0;
        //add liumz, [secondary index static_index_build] 20150629:b
        virtual int get_cluster_count(int64_t &cc) = 0;
        virtual int get_index_stat(const uint64_t table_id, const int64_t cluster_count, IndexStatus &stat) = 0;
        virtual int fetch_index_stat(const uint64_t table_id, const int64_t cluster_id, int64_t &stat) = 0;
        //add:e
        //add lqc [MultiUps 1.0] [#13] 20170503 b
        virtual ObScanHelper* get_client_proxy()const = 0 ;
        //add e
        //add jinty [Paxos Cluster.Balance] 20160708:b
        virtual int get_all_server_status(char *buf,
                                          ObArray<ObString> &typeArray,
                                          ObArray<int32_t> &inner_port_Array,
                                          ObArray<ObServer> &servers_ip_with_port,
                                          ObArray<int32_t> &cluster_id_array,
                                          ObArray<int32_t> & svr_role_array) = 0;
        //add e
    };

  }
}


#endif /* _OB_SCHEMA_SERVICE_H */
