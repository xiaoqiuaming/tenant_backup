#include "ob_root_ddl_operator.h"
#include "ob_root_server2.h"
#include "common/ob_range.h"
#include "common/ob_schema_service.h"
#include "common/ob_schema_service_impl.h"

using namespace oceanbase::common;
using namespace oceanbase::rootserver;

ObRootDDLOperator::ObRootDDLOperator():schema_client_(NULL), root_server_(NULL),rotate_value_(0),client_proxy_(NULL)//mod lqc [MultiUps 1.0] [#13] 20170417
{
}

ObRootDDLOperator::~ObRootDDLOperator()
{
  //add lqc [MultiUps 1.0] [#13] 20170417 b
  Str_buf.clear();
  client_proxy_ = NULL;
  //add e
}

void ObRootDDLOperator::init(ObRootServer2 * server, ObSchemaService * impl)
{
  root_server_ = server;
  schema_client_ = impl;
  //add lqc [MultiUps 1.0] [#13] 20170417 b
  this->client_proxy_ = schema_client_->get_client_proxy();
  nb_accessor_.init(client_proxy_);
  nb_accessor_.set_is_read_consistency(true);
  //add e
}


//[view]
int ObRootDDLOperator::create_view(const TableSchema &table_schema)
{
    int ret = OB_SUCCESS;
    if (!check_inner_stat())
    {
      ret = OB_ERROR;
      YYSYS_LOG(WARN, "check inner stat failed");
    }
    else
    {
      yysys::CThreadGuard lock(&mutex_lock_);
      // step 1. allocate and update the table max used table id
      ret = allocate_table_id(const_cast<TableSchema &> (table_schema));
      if (ret != OB_SUCCESS)
      {
        YYSYS_LOG(WARN, "allocate table id failed:name[%s], ret[%d]",
                  table_schema.table_name_, ret);
      }
      else
      {
        // step 2. update max used table id
        ret = update_max_table_id(table_schema.table_id_);
        if (ret != OB_SUCCESS)
        {
          YYSYS_LOG(WARN, "update max used table id failed:table_name[%s], table_id[%lu], ret[%d]",
                    table_schema.table_name_, table_schema.table_id_, ret);
        }
      }
    }

    if (OB_SUCCESS == ret)
    {
      // step 4. insert table scheam to inner table
      if (OB_SUCCESS != (ret = insert_schema_table(table_schema)))
      {
        YYSYS_LOG(ERROR, "update schema table failed:table_name[%s], table_id[%lu]",
                  table_schema.table_name_, table_schema.table_id_);
      }
      else
      {
        YYSYS_LOG(INFO, "update inner table for schema succ:table_name[%s], table_id[%lu]",
                  table_schema.table_name_, table_schema.table_id_);
      }
    }
    return ret;
}

int ObRootDDLOperator::create_table(const TableSchema & table_schema)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "check inner stat failed");
  }
  else
  {
    yysys::CThreadGuard lock(&mutex_lock_);
    // step 1. allocate and update the table max used table id
    ret = allocate_table_id(const_cast<TableSchema &> (table_schema));
    if (ret != OB_SUCCESS)
    {
      YYSYS_LOG(WARN, "allocate table id failed:name[%s], ret[%d]",
                table_schema.table_name_, ret);
    }
    else
    {
      // step 2. update max used table id
      ret = update_max_table_id(table_schema.table_id_);
      if (ret != OB_SUCCESS)
      {
        YYSYS_LOG(WARN, "update max used table id failed:table_name[%s], table_id[%lu], ret[%d]",
                  table_schema.table_name_, table_schema.table_id_, ret);
      }
    }
  }
  //modify wenghaixing [secondary index] 20141105

  //if (OB_SUCCESS == ret)
  if (OB_SUCCESS == ret&&table_schema.ih_.tbl_tid==OB_INVALID_ID)
    //modify e
  {
    // step 3. select cs for create empty tablet
    ret = create_empty_tablet(table_schema);
    if (ret != OB_SUCCESS)
    {
      YYSYS_LOG(WARN, "create empty tablet failed:table_name[%s], table_id[%lu], ret[%d]",
                table_schema.table_name_, table_schema.table_id_, ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    // step 4. insert table scheam to inner table
    // if(true != insert_schema_table(table_schema)) mod liu jun.[MultiUps_Live] 20150325
    if (OB_SUCCESS != (ret = insert_schema_table(table_schema)))//add liu jun.[MultiUps_Live] 20150325
    {
      //ret = OB_ERROR;//mod liu jun.[MultiUps_Live] 20150325
      YYSYS_LOG(ERROR, "update schema table failed:table_name[%s], table_id[%lu]",
                table_schema.table_name_, table_schema.table_id_);
    }
    else
    {
      YYSYS_LOG(INFO, "update inner table for schema succ:table_name[%s], table_id[%lu]",
                table_schema.table_name_, table_schema.table_id_);
    }
  }
  return ret;
}

int ObRootDDLOperator::create_empty_tablet(const TableSchema & table_schema)
{
  ObArray<ObServer> cs_list;
  int ret = root_server_->create_empty_tablet(const_cast<TableSchema &>(table_schema), cs_list);
  if (ret != OB_SUCCESS)
  {
    YYSYS_LOG(WARN, "create table tablets failed:table_name[%s], table_id[%lu], ret[%d]",
              table_schema.table_name_, table_schema.table_id_, ret);
  }
  else
  {
    YYSYS_LOG(DEBUG, "create empty tablet succ:table_name[%s], table_id[%lu], cs_count[%ld]",
              table_schema.table_name_, table_schema.table_id_, cs_list.count());
  }
  return ret;
}

//mod liu jun.[MultiUPS] [sql_api] 20150325
//bool ObRootDDLOperator::insert_schema_table(const TableSchema & table_schema)
int ObRootDDLOperator::insert_schema_table(const TableSchema & table_schema) //[uncertainty ]
{
  //bool succ = false;mod liu jun .[MultiUPS] [sql_api] 20150423
  int64_t frozen_version = 0;
  int ret = OB_SUCCESS;
  //add lqc [MultiUps 1.0] [#13] 20170405 b
  ObArray<int32_t> paxos_id_array;
  ObArray<ObString>group_name_list;
  if(OB_SUCCESS != (ret  = root_server_->fetch_mem_version(frozen_version)))
  {
    YYSYS_LOG(WARN, "failed to fetch memory frozen version");
  }//add lqc [MultiUps 1.0] [#13] 20170405 b
  else if(OB_INVALID_ID != table_schema.ih_.tbl_tid)
  {
    if(OB_SUCCESS != (ret = schema_client_->create_table(table_schema,frozen_version)))
    {
      YYSYS_LOG(WARN, "insert new table schema failed:ret[%d]", ret);
    }
  }
  else if(OB_SUCCESS != (ret = generate_group_name(table_schema,paxos_id_array,group_name_list)))
  {
    YYSYS_LOG(WARN, "generate group name failed,ret= %d",ret);
  }
  //add e
  else if(OB_SUCCESS != (ret = schema_client_->create_table(table_schema,frozen_version,&paxos_id_array,&group_name_list)))
  {
    YYSYS_LOG(WARN, "insert new table schema failed:ret[%d]", ret);
  }
  // mod liu jun .[MultiUPS] [sql_api] 20150423:b
  //  else
  //  {
  //    succ = true;
  //  }
  //mod 20150325:e
  // double check for read the table info for comparing
  ObString table_name;
  size_t len = strlen(table_schema.table_name_);
  ObString old_name(0, (int32_t)len, table_schema.table_name_);
  
  //add dolphin [database manager]@20150615:b
  ObString dt;
  char buf[OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1] = {0};
  dt.assign_buffer(buf,OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1);
  dt.concat(table_schema.dbname_,table_schema.table_name_);
  //add:e
  
  if(OB_SUCCESS == ret)
  {
    ret = schema_client_->get_table_name(table_schema.table_id_, table_name);
    //} mod liu jun .[MultiUPS] [sql_api] 20150325
    if (ret != OB_SUCCESS)
    {
      // WARNING: IF go into this path, THEN need client check wether write succ or not
      YYSYS_LOG(ERROR, "get table name failed need manual check:table_name[%s], ret[%d]",
                table_schema.table_name_, ret);
    }
    else if (/** modify dolphin [database manager]@20150615 old_name */ dt != table_name) //[uncertainty ]
    {
      //succ = false;mod liu jun .[MultiUPS] [sql_api] 20150325    //[uncertainty ]
      ret = OB_ERR_DOUBLE_CHECK_NAME;
      YYSYS_LOG(ERROR, "check schema table name not equal:table_name[%s], schema_name[%.*s]",
                table_schema.table_name_, table_name.length(), table_name.ptr());
    }
    else
    {
      //add zhaoqiong [Schema Manager] 20150327:b
      //use __all_ddl_operation timestamp as rs schema timestamp  //[conflict leve B]
      YYSYS_LOG(INFO, "create table, get schema timestamp=%ld",schema_client_->get_schema_version());
      root_server_->set_schema_version(schema_client_->get_schema_version());
      //add:e
      // succ = true;
    }
  }//add liu jun .[MultiUPS] [sql_api] 20150423
  //mod liu jun .[MultiUPS] [sql_api] 20150423:b
  //  else
  //  {
  //    succ = true;
  //  }
  //mod 20150423:e
  // TODO check table schema content equal with old schema
  //return succ;mod liu jun.[MultiUPS] [sql_api] 20150423
  return ret;//add liu jun.[MultiUPS] [sql_api] 20150423
}

int ObRootDDLOperator::allocate_table_id(TableSchema & table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = 0;
  // get max table id from inner table
  if ((ret = schema_client_->get_max_used_table_id(table_id)) != OB_SUCCESS)
  {
    YYSYS_LOG(WARN, "get max table id failed:ret[%d]", ret);
  }
  else if (table_schema.table_id_ == OB_INVALID_ID)
  {
    if (table_id + 1 > OB_APP_MIN_TABLE_ID)
    {
      table_schema.table_id_ = table_id + 1;
      YYSYS_LOG(DEBUG, "modify new table schema succ:table_id[%lu]", table_schema.table_id_);
    }
    else
    {
      ret = OB_ERROR;
      YYSYS_LOG(USER_ERROR, "User table id should in (%lu, MAX), ret[%d]", OB_APP_MIN_TABLE_ID, ret);
    }
  }
  else
  {
    if (table_schema.table_id_ > table_id)
    {
      ret = OB_ERROR;
      YYSYS_LOG(USER_ERROR, "User table id must not bigger than yb_max_used_table_id[%lu], ret[%d]", table_id, ret);
    }
  }
  if (ret == OB_SUCCESS)
  {
    for (int64_t i = 0; i < table_schema.join_info_.count(); i ++)
    {
      table_schema.join_info_.at(i).left_table_id_ = table_schema.table_id_;
      YYSYS_LOG(DEBUG, "table schema join info[%s]", to_cstring(table_schema.join_info_.at(i)));
    }
  }
  return ret;
}
//add wenghaixing [secondary index col checksum modify stat]20141208
//modify liuxiao [muti database] 20150702
int ObRootDDLOperator::modify_index_stat(ObString index_table_name,uint64_t index_table_id,ObString db_name,int stat)
//modify:e
{
  //20150702 为兼容多库，modify index增加如参db_name，原表的db_name
  int ret = OB_SUCCESS;
  yysys::CThreadGuard lock(&mutex_lock_);
  if(OB_SUCCESS != (ret = schema_client_->modify_index_stat(index_table_name,index_table_id,db_name,stat)))
  {
    YYSYS_LOG(ERROR,"failed to us ddl operator modify index status,index_table_id[%.*s],ret=%d",index_table_name.length(),index_table_name.ptr(),ret);
  }
  else
  {
    //add liumz
    root_server_->set_schema_version(schema_client_->get_schema_version());
    //add:e
    YYSYS_LOG(INFO,"modify index stat succ, index name[%.*s], schema_version[%ld]",index_table_name.length(),index_table_name.ptr(), schema_client_->get_schema_version());
  }
  return ret;
}
//add e
int ObRootDDLOperator::update_max_table_id(const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  uint64_t max_id = 0;
  if ((ret = schema_client_->get_max_used_table_id(max_id)) != OB_SUCCESS)
  {
    YYSYS_LOG(WARN, "get max table id failed:ret[%d]", ret);
  }
  else if (table_id > max_id)
  {
    // update the max table id
    int ret = schema_client_->set_max_used_table_id(table_id);
    if (ret != OB_SUCCESS)
    {
      YYSYS_LOG(WARN, "update table max table id failed:max_id[%lu], ret[%d]",
                table_id + 1, ret);
    }
    else
    {
      uint64_t new_max_id = 0;
      ret = schema_client_->get_max_used_table_id(new_max_id);
      if (ret != OB_SUCCESS)
      {
        YYSYS_LOG(WARN, "double check new max table id failed:max_id[%lu], ret[%d]", table_id, ret);
      }
      else if (new_max_id != table_id)
      {
        ret = OB_INNER_STAT_ERROR;
        YYSYS_LOG(ERROR, "check update max table id check failed:max_id[%lu], read[%lu]",
                  table_id, new_max_id);
      }
      else
      {
        YYSYS_LOG(INFO, "check updated max table table id succ:max_id[%lu]", new_max_id);
      }
    }
  }
  return ret;
}

int ObRootDDLOperator::drop_table(const common::ObString & table_name)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = 0;
  if (!check_inner_stat())
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "check inner stat failed");
  }
  else
  {
    // WARNING:can not drop the inner table
    if (delete_schema_table(table_name, table_id) != true)
    {
      ret = OB_ERROR;
      YYSYS_LOG(WARN, "delete table from schema table failed:table_name[%.*s], ret[%d]",
                table_name.length(), table_name.ptr(), ret);
    }
    else
    {
      YYSYS_LOG(INFO, "delete table from inner table succ:table_name[%.*s], table_id[%lu]",
                table_name.length(), table_name.ptr(), table_id);
    }
  }
  // clear the root table for this table items
  if (OB_SUCCESS == ret)
  {
    ret = delete_root_table(table_id);
    if (ret != OB_SUCCESS)
    {
      YYSYS_LOG(WARN, "delete root table failed:table_name[%.*s], table_id[%lu], ret[%d]",
                table_name.length(), table_name.ptr(), table_id, ret);
    }
    else
    {
      YYSYS_LOG(INFO, "delete root table succ:table_name[%.*s], table_id[%lu]",
                table_name.length(), table_name.ptr(), table_id);
    }
  }
  return ret;
}

int ObRootDDLOperator::delete_root_table(const uint64_t table_id)
{
  ObArray<uint64_t> list;
  int ret = list.push_back(table_id);
  if (ret != OB_SUCCESS)
  {
    YYSYS_LOG(WARN, "add table id failed:table_id[%lu], ret[%d]", table_id, ret);
  }
  else
  {
    // delete from root table and write commit log
    ret = root_server_->delete_tables(false, list);
    if (ret != OB_SUCCESS)
    {
      YYSYS_LOG(WARN, "drop root table table failed:table_id[%lu], ret[%d]", table_id, ret);
    }
  }
  return ret;
}

bool ObRootDDLOperator::delete_schema_table(const common::ObString & table_name, uint64_t & table_id)
{
  bool succ = false;
  yysys::CThreadGuard lock(&mutex_lock_);
  int ret = schema_client_->get_table_id(table_name, table_id);
  if (ret != OB_SUCCESS)
  {
    YYSYS_LOG(WARN, "get table id failed stop drop:table_name[%.*s], ret[%d]",
              table_name.length(), table_name.ptr(), ret);
  }
  else
  {
    ret = schema_client_->drop_table(table_name);
    if (ret != OB_SUCCESS)
    {
      YYSYS_LOG(WARN, "delete schema table failed:table_name[%.*s], ret[%d]",
                table_name.length(), table_name.ptr(), ret);
    }
    else
    {
      succ = true;
    }
    // double check table schema content
    uint64_t temp_table_id = 0;
    ret = schema_client_->get_table_id(table_name, temp_table_id);
    if (ret != OB_SUCCESS)
    {
      if (OB_ENTRY_NOT_EXIST == ret)
      {
        succ = true;
      }
    }
    else
    {
      YYSYS_LOG(WARN, "check get table id succ after delete:table_name[%.*s], table_id[%lu]",
                table_name.length(), table_name.ptr(), temp_table_id);
      succ = false;
    }
  }
  //add zhaoqiong [Schema Manager] 20150327:b
  //use __all_ddl_operation timestamp as rs schema timestamp
  if (succ)
  {
    root_server_->set_schema_version(schema_client_->get_schema_version());
  }
  //add:e
  return succ;
}

int ObRootDDLOperator::alter_table(AlterTableSchema & table_schema)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "check inner stat failed");
  }
  else
  {
    TableSchema old_schema;
    int64_t len = strlen(table_schema.table_name_);
    ObString table_name(static_cast<int32_t>(len), static_cast<int32_t>(len), table_schema.table_name_);
    yysys::CThreadGuard lock(&mutex_lock_);
    // get table schema from inner table
    //modify dolphin [database manager]@20150616:b
    //ret = schema_client_->get_table_schema(table_name, old_schema);
    ret = schema_client_->get_table_schema(ObString::make_string(table_schema.table_name_), old_schema,ObString::make_string(table_schema.dbname_));
    if (ret != OB_SUCCESS)
    {
      YYSYS_LOG(WARN, "get table schema failed:table_name[%s], ret[%d]",
                table_schema.table_name_, ret);
    }
    else if (old_schema.table_id_ < OB_ALL_JOIN_INFO_TID)
    {
      YYSYS_LOG(WARN, "check table id failed:table_id[%lu]", old_schema.table_id_);
      ret = OB_INVALID_ARGUMENT;
    }
    else
    {
      // modify table schema info
      table_schema.table_id_ = old_schema.table_id_;
      ret = alter_table_schema(old_schema, table_schema);
      if (ret != OB_SUCCESS)
      {
        YYSYS_LOG(WARN, "alter table schema failed:table_name[%s], table_id[%lu], ret[%d]",
                  table_schema.table_name_, table_schema.table_id_, ret);
      }
    }
    //add dolphin [SchemaValidation]@20150515:b
    if(OB_SUCCESS == ret)
    {
      if(OB_SUCCESS != (ret = construct_check_table_schema(old_schema,table_schema)))
      {
        YYSYS_LOG(ERROR, "construct the table schema for validation failed,table_name[%s]", old_schema.table_name_);
      }
    }
    if(OB_SUCCESS == ret)
    {
      if (!old_schema.is_valid())
      {
        YYSYS_LOG(WARN, "table schmea is invalid:table_name[%s]", old_schema.table_name_);
        ret = OB_INVALID_ARGUMENT;
      }
    }
    //add e:
    // alter inner table
    if (OB_SUCCESS == ret)
    {
      //mod lqc [MultiUps 1.0] [#37] 20170405 b
      //mod fyd [NotNULL_check] [JHOBv0.1] 20140108:b
      /*expr:we pass the original schema to fetch the useful info in the next step*/
      //ret = schema_client_->alter_table(table_schema, old_schema.schema_version_);
      //ret = schema_client_->alter_table(table_schema, old_schema/*.schema_version_*/);
      //mod 20140108:e
      if(!table_schema.is_rule_modify_)
      {
        if (OB_SUCCESS != (ret = schema_client_->alter_table(table_schema, old_schema/*.schema_version_*/, root_server_->get_local_schema())))
        {
          YYSYS_LOG(WARN, "alter table failed:table_name[%s], table_id[%lu], ret[%d]",
                    table_schema.table_name_, table_schema.table_id_, ret);
        }
      }
      else
      {
        ObArray<int32_t> paxos_id_array;
        ObArray<ObString>group_name_list;
        int64_t frozen_version = 0;
        if(OB_SUCCESS != (ret  = root_server_->fetch_mem_version(frozen_version)))
        {
          YYSYS_LOG(WARN, "failed to fetch memory frozen version");
        }
        else if(OB_SUCCESS != (ret = alter_group(table_schema,paxos_id_array,group_name_list)))
        {
          YYSYS_LOG(WARN, "generate group name failed,ret= %d",ret);
        }
        else if (OB_SUCCESS != (ret = schema_client_->alter_table(table_schema, old_schema/*.schema_version_*/, root_server_->get_local_schema(), frozen_version,&paxos_id_array,&group_name_list)))
        {
          YYSYS_LOG(WARN, "alter table failed:table_name[%s], table_id[%lu], ret[%d]",
                    table_schema.table_name_, table_schema.table_id_, ret);
        }
      }
      // mod e
      if (ret != OB_SUCCESS)
      {
        YYSYS_LOG(WARN, "alter table failed:table_name[%s], table_id[%lu], ret[%d]",
                  table_schema.table_name_, table_schema.table_id_, ret);
      }
      else
      {
        //add zhaoqiong [Schema Manager] 20150327:b
        //use __all_ddl_operation timestamp as rs schema timestamp
        root_server_->set_schema_version(schema_client_->get_schema_version());
        //add:e
        YYSYS_LOG(INFO, "alter table modify inner table succ:table_name[%s], table_id[%lu],db_name[%s]",
                  table_schema.table_name_, table_schema.table_id_,table_schema.dbname_);
      }
    }
  }
  return ret;
}

int ObRootDDLOperator::alter_table_schema(const TableSchema & old_schema, AlterTableSchema & alter_schema)
{
  int ret = OB_SUCCESS;
  const char * column_name = NULL;
  int64_t column_count = alter_schema.get_column_count();
  AlterColumn column_schema;
  uint64_t max_column_id = old_schema.max_used_column_id_;
  // check old table schema max column id
  if (max_column_id < OB_APP_MIN_COLUMN_ID)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(WARN, "check old table schema max column id failed:max[%lu], min[%lu]",
              max_column_id, OB_APP_MIN_COLUMN_ID);
  }
  else
  {
    // check if not exist allocate the column id
    for (int64_t i = 0; (OB_SUCCESS == ret) && (i < column_count); ++i)
    {
      column_name = alter_schema.columns_.at(i).column_.column_name_;
      // check and modify column id
      ret = check_alter_column(old_schema, alter_schema.columns_.at(i));
      if (ret != OB_SUCCESS)
      {
        YYSYS_LOG(WARN, "check column is valid failed:table[%s], column[%s], ret[%d]",
                  alter_schema.table_name_, column_name, ret);
        break;
      }
      else
      {
        ret = set_column_info(old_schema, column_name, max_column_id, alter_schema.columns_.at(i));
        if (ret != OB_SUCCESS)
        {
          YYSYS_LOG(WARN, "set column info failed:table[%s], column[%s], ret[%d]",
                    alter_schema.table_name_, column_name, ret);
          break;
        }
      }
    }
  }
  return ret;
}

int ObRootDDLOperator::check_alter_column(const TableSchema & old_schema,
                                          AlterTableSchema::AlterColumnSchema & alter_column)
{
  int ret = OB_SUCCESS;
  ObObjType data_type = alter_column.column_.data_type_;
  // create or modify datetime column not allow
  if ((data_type == ObCreateTimeType) || (data_type == ObModifyTimeType))
  {
    ret = OB_OP_NOT_ALLOW;
    YYSYS_LOG(WARN, "column type is create or modify time not allowed:type[%d]", data_type);
  }
  // rowkey column not allow
  else if (alter_column.column_.rowkey_id_ > 0)
  {
    ret = OB_OP_NOT_ALLOW;
    YYSYS_LOG(WARN, "alter primary rowkey column not allowed:table[%s], column[%s], rowkey_id[%ld]",
              old_schema.table_name_, alter_column.column_.column_name_, alter_column.column_.rowkey_id_);
  }
  return ret;
}

//  add construct check table schema for schema vadidation dolphin-SchemaValidation@20150515:b
int ObRootDDLOperator::construct_check_table_schema(TableSchema & old_schema, const AlterTableSchema & alter_schema)
{
  int ret = OB_SUCCESS;
  if(alter_schema.has_table_rename_)
    return ret;
  int64_t column_count = alter_schema.get_column_count();
  for(int64_t i = 0;i<column_count;++i)
  {
    if(OB_SUCCESS != (ret =construct_alter_schema_column(old_schema,alter_schema.columns_.at(i))))
    {
      YYSYS_LOG(WARN, "set construct_alter_schema_column  failed:table[%s], column[%s], ret[%d]",
                alter_schema.table_name_, alter_schema.columns_.at(i).column_.column_name_, ret);
      break;
    }
  }

  return ret;
}
int ObRootDDLOperator::construct_alter_schema_column(TableSchema & old_schema,const AlterTableSchema::AlterColumnSchema & alterColumn)
{
  int ret = OB_SUCCESS;

  ColumnSchema * old_column = NULL;
  if(alterColumn.type_ == AlterTableSchema::RENAME_COLUMN)
    old_column = old_schema.get_column_schema(alterColumn.column_.column_id_);
  else
    old_column = old_schema.get_column_schema(alterColumn.column_.column_name_);
  if(NULL != old_column)
  {
    switch(alterColumn.type_)
    {
      case AlterTableSchema::ADD_COLUMN:
      {

        ret = OB_ENTRY_EXIST;
        YYSYS_LOG(ERROR, "column already exist can not ADD_COLUMN column[%s], ret[%d]",
                  alterColumn.column_.column_name_, ret);
        break;

      }
      case AlterTableSchema::DEL_COLUMN:
      {

        // the operation column mustnot row key because FUNCTION-check_alter_column has stoped it happen.
        int64_t index = old_schema.get_column_schema_index(old_column->column_name_);
        if(OB_SUCCESS != (ret = old_schema.columns_.remove(index)))
        {
          YYSYS_LOG(ERROR, "the operation DEL_COLUMN failed,"
                    "cant remove the index [%ld],column[%s], ret[%d]",index,
                    alterColumn.column_.column_name_, ret);
        }
        //add liumz, [bugfix_drop_column]20150721:b
        break;
        //add:e
      }
      case AlterTableSchema::MOD_COLUMN:
      {
        old_column->column_group_id_ = alterColumn.column_.column_group_id_;
        old_column->column_id_= alterColumn.column_.column_id_;
        old_column->data_length_ = alterColumn.column_.data_length_;
        old_column->rowkey_id_ = alterColumn.column_.rowkey_id_;
        old_column->data_length_ = alterColumn.column_.data_length_;
        old_column->data_precision_ = alterColumn.column_.data_precision_;
        old_column->data_scale_ = alterColumn.column_.data_scale_;
        old_column->data_type_ = alterColumn.column_.data_type_;
        old_column->length_in_rowkey_ = alterColumn.column_.length_in_rowkey_;
        old_column->order_in_rowkey_ = alterColumn.column_.order_in_rowkey_;
        break;
      }
      case AlterTableSchema::MOD_COLUMN_NULL:
      {
        old_column->nullable_ = alterColumn.column_.nullable_;
        break;
      }
      case AlterTableSchema::MOD_COLUMN_DEFAULT:
      {
        int buf_len = sizeof(old_column->default_value_);
        int len = static_cast<int>(strlen(alterColumn.column_.default_value_));
        if (len == 0 || alterColumn.column_.default_value_[0] == '\0')
        {
          old_column->default_value_[0] = '\0';
        }
        else
        {
          if (len > buf_len)
          {
            len = buf_len - 1;
          }
          memcpy(old_column->default_value_, alterColumn.column_.default_value_, len);
          old_column->default_value_[len] = '\0';
        }
        break;
      }
      case AlterTableSchema::RENAME_COLUMN:
      {
        if(strlen(alterColumn.column_.column_name_) > OB_MAX_COLUMN_NAME_LENGTH-1 )
        {
          ret = OB_SIZE_OVERFLOW;
          YYSYS_LOG(ERROR, "rename col name [%s] is too length", alterColumn.column_.column_name_);
        }
        else
        {
          memset(old_column->column_name_,0,common::OB_MAX_COLUMN_NAME_LENGTH);
          memcpy(old_column->column_name_,alterColumn.column_.column_name_,common::OB_MAX_COLUMN_NAME_LENGTH);
        }
        break;
      }
      case AlterTableSchema::MOD_VARCHAR_LENGTH:
      {
        old_column->data_length_ = alterColumn.column_.data_length_;
        break;
      }
      case AlterTableSchema::MOD_DECIMAL_PRECISION:
      {
        old_column->data_precision_ = alterColumn.column_.data_precision_;
        old_column->data_scale_ = alterColumn.column_.data_scale_;
        break;
      }
      default:
      {
        YYSYS_LOG(WARN, "unknow alter operation not supported right now");
        ret = OB_INVALID_ARGUMENT;
        break;
      }
    }
  }
  else
  {
    if (alterColumn.type_ != AlterTableSchema::ADD_COLUMN)
    {
      ret = OB_ENTRY_NOT_EXIST;
      YYSYS_LOG(WARN, "column does not exist can not alter:column[%s], ret[%d]",
                alterColumn.column_.column_name_, ret);
    }
    else
    {
      if(strlen(alterColumn.column_.column_name_) > OB_MAX_COLUMN_NAME_LENGTH-1 )
      {
        ret = OB_SIZE_OVERFLOW;
        YYSYS_LOG(ERROR, "add col name [%s] is too length", alterColumn.column_.column_name_);
      }
      else
      {
        if(OB_SUCCESS != (ret =old_schema.add_column(alterColumn.column_)))
        {
          YYSYS_LOG(ERROR, "the operation ADD_COLUMN failed column[%s], ret[%d]",
                    alterColumn.column_.column_name_, ret);
        }
      }

    }
  }

  return ret;

}//add:e
// set old column id or allocate new column id
int ObRootDDLOperator::set_column_info(const TableSchema & old_schema, const char * column_name,
                                       uint64_t & max_column_id, AlterTableSchema::AlterColumnSchema & alter_column)
{
  int ret = OB_SUCCESS;
  const ColumnSchema * old_column = old_schema.get_column_schema(column_name);
  // column already exist
  if (old_column != NULL)
  {
    switch (alter_column.type_)
    {
      case AlterTableSchema::ADD_COLUMN:
      {
        ret = OB_ENTRY_EXIST;
        YYSYS_LOG(WARN, "column already exist can not add again:column[%s], ret[%d]",
                  column_name, ret);
        break;
      }
      case AlterTableSchema::DEL_COLUMN:
      {
        // TODO fill other property and check not join column
        alter_column.column_.column_id_ = old_column->column_id_;
        break;
      }
        //add fyd [NotNULL_check] [JHOBv0.1] 20140108:b
      case AlterTableSchema::MOD_COLUMN_NULL:
      {
        break;
      }
        //add 20140108:e
      case AlterTableSchema::MOD_COLUMN_DEFAULT:
      {
        break;
      }
        //add liuj [Alter_Rename] [JHOBv0.1] 20150104
      case AlterTableSchema::RENAME_COLUMN:
      {
        alter_column.column_.column_id_ = old_column->column_id_;
        alter_column.column_.column_group_id_ = old_column->column_group_id_;
        alter_column.column_.rowkey_id_ = old_column->rowkey_id_;
        alter_column.column_.join_table_id_ = old_column->join_table_id_;
        alter_column.column_.join_column_id_ = old_column->join_column_id_;
        alter_column.column_.data_type_ = old_column->data_type_;
        alter_column.column_.data_length_ = old_column->data_length_;
        alter_column.column_.data_precision_ = old_column->data_precision_;
        alter_column.column_.data_scale_ = old_column->data_scale_;
        alter_column.column_.nullable_ = old_column->nullable_;
        alter_column.column_.length_in_rowkey_ = old_column->length_in_rowkey_;
        alter_column.column_.order_in_rowkey_ = old_column->order_in_rowkey_;
        break;
      }
        //add e.
      case AlterTableSchema::MOD_VARCHAR_LENGTH:
      {
        break;
      }
      case AlterTableSchema::MOD_DECIMAL_PRECISION:
      {
        break;
      }
      default:
      {
        YYSYS_LOG(WARN, "modify exist column not supported right now");
        ret = OB_INVALID_ARGUMENT;
        break;
      }
    }
  }
  // column not exist
  else
  {
    if (alter_column.type_ != AlterTableSchema::ADD_COLUMN)
    {
      ret = OB_ENTRY_NOT_EXIST;
      YYSYS_LOG(WARN, "column does not exist can not drop:column[%s], ret[%d]",
                column_name, ret);
    }
    else
    {
      // allocate new column id
      alter_column.column_.column_id_ = ++max_column_id;
    }
  }
  return ret;
}
//int ObRootDDLOperator::modify_table_id(common::TableSchema &table_schema, const int64_t new_table_id)
int ObRootDDLOperator::modify_table_id(common::TableSchema &table_schema, const int64_t new_table_id, const int64_t frozen_version)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "check inner stat failed");
  }
  if (OB_SUCCESS == ret)
  {
    yysys::CThreadGuard lock(&mutex_lock_);
    ObString table_name;
    int ret = schema_client_->get_table_name(new_table_id, table_name);
    int64_t old_table_id = table_schema.table_id_;
    if (ret != OB_ENTRY_NOT_EXIST)
    {
      YYSYS_LOG(WARN, "table id is already been used. table_id=%ld, table_name=%s",
                new_table_id, to_cstring(table_name));
      ret = OB_ERROR;
    }
    else
    {
      //ret = schema_client_->modify_table_id(table_schema, new_table_id);
      ret = schema_client_->modify_table_id(table_schema, new_table_id, frozen_version);
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "fail to modify table id. new_table_id=%ld", new_table_id);
      }
      else
      {
        char update_tables[OB_SQL_LENGTH];
        ObString update_table_str;
        ObServer ms_server;
        int64_t  pos = 0;
        static const int64_t timeout = 1000000;
        ret = root_server_->get_ms_provider().get_ms(ms_server, true);
        if (OB_SUCCESS != ret)
        {
          YYSYS_LOG(WARN, "get ms failed:ret[%d]", ret);
        }
        else
        {
          databuff_printf(update_tables,
                          OB_SQL_LENGTH,
                          pos,
                          "update __all_table_rules set table_id = %lu where table_id = %ld",
                          new_table_id, old_table_id);
          if (pos >= OB_SQL_LENGTH)
          {
            ret = OB_BUF_NOT_ENOUGH;
            YYSYS_LOG(WARN, "create table rule buffer not enough, ret=%d", ret);
          }
          else
          {
            update_table_str.assign_ptr(update_tables, static_cast<ObString::obstr_size_t>(pos));
            YYSYS_LOG(INFO, "update_tables=%.*s", update_table_str.length(), update_table_str.ptr());
            for (int32_t i = 0; i < 3; ++i)
            {
              if (OB_SUCCESS != (ret = root_server_->get_rpc_stub().execute_sql(ms_server, update_table_str, timeout)))
              {
                YYSYS_LOG(WARN, "refresh system offline stat info failed, ret=%d", ret);
              }
              else
              {
                break;
              }
            }
          }
        }
      }

      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "fail to modify table_id or update table rules, ret=%d", ret);
      }
      else
      {
        if (table_schema.get_index_helper().tbl_tid != OB_INVALID_ID)
        {
          char update_index_status[OB_SQL_LENGTH];
          ObString update_index_status_str;
          ObServer ms_server;
          int64_t  pos = 0;
          static const int64_t timeout = 1000 * 1000L;
          ret = root_server_->get_ms_provider().get_ms(ms_server, true);
          if (OB_SUCCESS != ret)
          {
            YYSYS_LOG(WARN, "get ms failed, ret=%d", ret);
          }
          else
          {
            databuff_printf(update_index_status, OB_SQL_LENGTH, pos,
                            "update __first_tablet_entry set index_status=1 where table_id = %ld",
                            new_table_id);
            if (pos >= OB_SQL_LENGTH)
            {
              ret = OB_BUF_NOT_ENOUGH;
              YYSYS_LOG(WARN, "create update index status buffer not enough, ret=%d", ret);
            }
            else
            {
              update_index_status_str.assign_ptr(update_index_status, static_cast<ObString::obstr_size_t>(pos));
              YYSYS_LOG(INFO, "update_index_status_sql=%.*s", update_index_status_str.length(), update_index_status_str.ptr());
              for (int32_t i = 0; i < 3; i++)
              {
                if (OB_SUCCESS != (ret = root_server_->get_rpc_stub().execute_sql(ms_server, update_index_status_str, timeout)))
                {
                  YYSYS_LOG(WARN, "update index status failed , ret=%d", ret);
                }
                else
                {
                  break;
                }
              }
            }
          }
        }
      }
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "fail to modify table_id or update table rules, ret=%d", ret);
      }
      else
      {
        if (OB_INVALID_ID != table_schema.get_index_helper().tbl_tid)
        {
          for (int i = 0; i < OB_MAX_CLUSTER_COUNT; i++)
          {
            if (root_server_->is_cluster_alive_with_cs(i))
            {
              int64_t cluster_id = i;
              char insert_index_status[OB_SQL_LENGTH];
              ObString insert_index_status_str;
              ObServer ms_server;
              int64_t  pos = 0;
              static const int64_t timeout = 1000 * 1000L;
              ret = root_server_->get_ms_provider().get_ms(ms_server, true);
              if (OB_SUCCESS != ret)
              {
                YYSYS_LOG(WARN, "get ms failed, ret=%d", ret);
              }
              else
              {
                databuff_printf(insert_index_status, OB_SQL_LENGTH, pos,
                                "insert into __index_process_info(index_id, cluster_id, status) values(%ld, %ld, 1)",
                                new_table_id, cluster_id);
                if (pos >= OB_SQL_LENGTH)
                {
                  ret = OB_BUF_NOT_ENOUGH;
                  YYSYS_LOG(WARN, "create insert index status buffer not enough, ret=%d", ret);
                }
                else
                {
                  insert_index_status_str.assign_ptr(insert_index_status, static_cast<ObString::obstr_size_t>(pos));
                  YYSYS_LOG(INFO, "insert_index_status_sql=[%.*s]", insert_index_status_str.length(), insert_index_status_str.ptr());
                  for (int32_t i = 0; i < 3; i++)
                  {
                    if (OB_SUCCESS != (ret = root_server_->get_rpc_stub().execute_sql(ms_server, insert_index_status_str, timeout)))
                    {
                      YYSYS_LOG(WARN, "insert index status to __index_process_info failed , ret=%d", ret);
                    }
                    else
                    {
                      break;
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}
//add lqc [MultiUps 1.0] [#13] 20170405 b
int ObRootDDLOperator::get_online_paxos_id(int32_t &paxos_id)
{
  int ret = OB_SUCCESS;
  srand(static_cast<int32_t>(yysys::CTimeUtil::getTime()));
  int64_t use_paxos_num = root_server_->get_ups_manager()->get_use_paxos_num();
  if(use_paxos_num > 0 && use_paxos_num <= MAX_UPS_COUNT_ONE_CLUSTER)
  {
    int loop = MAX_UPS_COUNT_ONE_CLUSTER;
    do
    {
      rotate_value_ %= use_paxos_num;
      paxos_id = static_cast<int32_t> (rotate_value_);
      if (!is_paxos_id_usable(paxos_id))
      {
        rotate_value_ ++ ;
        paxos_id = OB_INVALID_PAXOS_ID;
      }
      //[691]
      else if(root_server_->is_paxos_group_offline_next_version(paxos_id))
      {
          rotate_value_++;
          paxos_id = OB_INVALID_PAXOS_ID;
      }
      else
      {
        //paxos_id = static_cast<int32_t> (rotate_value_);
        rotate_value_++;
        break;
      }
    } while (loop-- > 0);
  }
  if(OB_INVALID_PAXOS_ID != paxos_id)
  {
    ret = OB_SUCCESS;
    YYSYS_LOG(DEBUG,"get paxos_id succ ,ret = %d",ret);
  }
  else
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(WARN,"get paxos_id failed ,ret = %d",ret);
  }
  return ret;
}

int ObRootDDLOperator::generate_group_name(const TableSchema &table_schema,ObArray<int32_t> &paxos_id_array,ObArray<ObString> &group_name_list)
{
  int ret = OB_SUCCESS;
  int32_t paxos_id = OB_INVALID_PAXOS_ID;
  char name_prefix[OB_MAX_TABLE_NAME_LENGTH] = {0};
  snprintf(name_prefix,OB_MAX_TABLE_NAME_LENGTH,"%s",table_schema.group_name_prefix_);
  if (NULL == &table_schema)
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN,"frozen_version is invalid");
  }
  else
  {
    ObTablePartitionType part_type = static_cast<ObTablePartitionType>(table_schema.table_partition_type_);
    if(OB_WITHOUT_PARTITION == part_type)
    {
      if(OB_SUCCESS != (ret = get_online_paxos_id(paxos_id)))
      {
        YYSYS_LOG(WARN,"get paxos_id failed ,ret = %d",ret);
      }
      else if(OB_SUCCESS == (ret = check_group_name(name_prefix)))
      {
        YYSYS_LOG(DEBUG,"group name is already exit ,ret = %d",ret);
      }
      else
      {
        if(OB_SUCCESS != (ret = add_group_and_paxos_id(name_prefix,paxos_id,paxos_id_array,group_name_list)))
        {
          YYSYS_LOG(WARN,"add group and paxos id failed,ret = %d",ret);
        }
      }
    }
    else if(OB_DIRECT_PARTITION == part_type)
    {// get paxos_id , group name  and push into array,
      if(OB_RANGE_PARTITION == table_schema.partition_form_ ||
         OB_RANGE_COLUMNS_PARTITION == table_schema.partition_form_ ||
         OB_LIST_PARTITION == table_schema.partition_form_ ||
         OB_LIST_COLUMNS_PARTITION == table_schema.partition_form_)
      {
        char *sub_str= strtok(name_prefix,",");
        while(NULL != sub_str)
        {
          if(OB_SUCCESS != (ret = get_online_paxos_id(paxos_id)))
          {
            YYSYS_LOG(WARN,"get paxos_id failed ,ret = %d",ret);
          }
          else if(OB_SUCCESS == (ret = check_group_name(sub_str)))
          {// check the group name
            YYSYS_LOG(DEBUG,"group name is already exit ,ret = %d",ret);
          }
          else
          {
            if(OB_SUCCESS != (ret = add_group_and_paxos_id(sub_str,paxos_id,paxos_id_array,group_name_list)))
            {
              YYSYS_LOG(WARN,"add group and paxos id failed,ret = %d",ret);
              break;
            }
          }
          sub_str = strtok(NULL,",");
        }
      }
      else // has and enum
      {
        int64_t type_64 = 0;
        char part_func_rule_body_[OB_MAX_VARCHAR_LENGTH];
        if(OB_SUCCESS != (ret = get_table_rule_func_info(table_schema.partition_func_name_,part_func_rule_body_,type_64)))
        {
          YYSYS_LOG(WARN,"get table rule func failed ,ret[%d]",ret);
        }
        else if(ENUMFUNC == type_64)
        {
          int64_t count = 0;
          if(OB_SUCCESS != (ret = get_enum_group_num(part_func_rule_body_,count)))
          {
            YYSYS_LOG(WARN,"get enum num failed ,ret = %d",ret);
          }
          else
          {
            for(int64_t i = 0; i < count; i ++)
            {
              char group_name[OB_MAX_TABLE_NAME_LENGTH +4] = {0};

              if(OB_SUCCESS != (ret = get_online_paxos_id(paxos_id)))
              {
                YYSYS_LOG(WARN,"get paxos_id failed ,ret = %d",ret);
              }
              else if(OB_SUCCESS != (ret = spell_complete_group_name(name_prefix,i,group_name)) )
              {
                YYSYS_LOG(WARN,"get  complete group name failed ,ret = %d",ret);
              }
              else if(OB_SUCCESS == (ret = check_group_name(group_name)))
              {
                YYSYS_LOG(DEBUG,"group name is already exit ,ret = %d",ret);
              }
              else
              {
                if(OB_SUCCESS != (ret = add_group_and_paxos_id(group_name,paxos_id,paxos_id_array,group_name_list)))
                {
                  YYSYS_LOG(WARN,"add group and paxos id failed,ret = %d",ret);
                  break;
                }
              }
            }
          }
        }
        else if(HASHFUNC == type_64)
        {
          for(int64_t count = 0; count < OB_MAX_MOD_NUM; count ++)
          {
            char group_name[OB_MAX_TABLE_NAME_LENGTH +4] = {0};

            if(OB_SUCCESS != (ret = get_online_paxos_id(paxos_id)))
            {
              YYSYS_LOG(WARN,"get paxos_id failed ,ret = %d",ret);
            }
            else if(OB_SUCCESS != (ret = spell_complete_group_name(name_prefix,count,group_name,true)) )
            {
              YYSYS_LOG(WARN,"get  complete group name failed ,ret = %d",ret);
            }
            else if(OB_SUCCESS == (ret = check_group_name(group_name)))
            {
              YYSYS_LOG(DEBUG,"group name is already exit ,ret = %d",ret);
            }
            else
            {
              if(OB_SUCCESS != (ret = add_group_and_paxos_id(group_name,paxos_id,paxos_id_array,group_name_list)))
              {
                YYSYS_LOG(WARN,"add group and paxos id failed,ret = %d",ret);
                break;
              }
            }
          }
        }
        else if(PRERANGE == type_64)
        {
          int64_t count = 0;
          if(OB_SUCCESS != (ret = get_range_group_num(part_func_rule_body_,count)))
          {
            YYSYS_LOG(WARN,"get range num failed ,ret = %d",ret);
          }
          else
          {
            for(int64_t i = 0; i < count; i ++)
            {
              char group_name[OB_MAX_TABLE_NAME_LENGTH +4] = {0};

              if(OB_SUCCESS != (ret = get_online_paxos_id(paxos_id)))
              {
                YYSYS_LOG(WARN,"get paxos_id failed ,ret = %d",ret);
              }
              else if(OB_SUCCESS != (ret = spell_complete_group_name(name_prefix,i,group_name)) )
              {
                YYSYS_LOG(WARN,"get  complete group name failed ,ret = %d",ret);
              }
              else if(OB_SUCCESS == (ret = check_group_name(group_name)))
              {
                YYSYS_LOG(DEBUG,"group name is already exit ,ret = %d",ret);
              }
              else
              {
                if(OB_SUCCESS != (ret = add_group_and_paxos_id(group_name,paxos_id,paxos_id_array,group_name_list)))
                {
                  YYSYS_LOG(WARN,"add group and paxos id failed,ret = %d",ret);
                  break;
                }
              }
            }
          }
        }
        else if(HARDCODEFUNC == type_64)
        {
          for(int64_t count = 0; count < OB_MAX_MOD_NUM; count ++)
          {
            char group_name[OB_MAX_TABLE_NAME_LENGTH +4] = {0};

            if(OB_SUCCESS != (ret = get_online_paxos_id(paxos_id)))
            {
              YYSYS_LOG(WARN,"get paxos_id failed ,ret = %d",ret);
            }
            else if(OB_SUCCESS != (ret = spell_complete_group_name(name_prefix, count, group_name, true)) )
            {
              YYSYS_LOG(WARN,"get  complete group name failed ,ret = %d",ret);
            }
            else if(OB_SUCCESS == (ret = check_group_name(group_name)))
            {
              YYSYS_LOG(DEBUG,"group name is already exit ,ret = %d",ret);
            }
            else
            {
              if(OB_SUCCESS != (ret = add_group_and_paxos_id(group_name,paxos_id,paxos_id_array,group_name_list)))
              {
                YYSYS_LOG(WARN,"add group and paxos id failed,ret = %d",ret);
                break;
              }
            }
          }
        }
        else
        {
          ret = OB_ERR_UNEXPECTED;
          YYSYS_LOG(WARN, "wrong partition type");
        }
      }
    }
    else if(OB_REFERENCE_PARTITION == part_type)
    {
      ret = OB_INVALID_ARGUMENT;
      YYSYS_LOG(WARN, "reference partition not supported");
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      YYSYS_LOG(WARN, "wrong partition type");
    }
  }
  return ret;
}
int ObRootDDLOperator::spell_complete_group_name(char *prefix_name,int64_t &count ,char *group_name,bool need_mod )
{
  int ret = OB_SUCCESS;
  int32_t pos = 0;
  if (NULL == prefix_name)
  {
    ret= OB_ERROR;
    YYSYS_LOG(WARN,"prefix_name is NULL ret =%d ",ret);
  }// the paxos_id only occupy one bity and the '#' occupy one bity
  else if (static_cast<int64_t>(strlen(prefix_name) + 2 )  > OB_MAX_PREFIX_GROUP_NAME_LENGTH)
  {
    ret= OB_ERROR;
    YYSYS_LOG(WARN,"BUF not enough, ret =%d ",ret);
  }
  else
  {
    if(!need_mod)
    {
      pos = static_cast<int32_t>(count);
    }
    else
    {
      pos = static_cast<int32_t>(count % OB_MAX_MOD_NUM);
    }
    snprintf(group_name,static_cast<int32_t>(strlen(prefix_name))+ 4,"%s%s%d",prefix_name,GROUP_SEPARATOR,pos);
  }
  return ret;
}

int ObRootDDLOperator::check_group_name(char *group_name)
{
  int ret = OB_SUCCESS;
  char select_groups[OB_SQL_LENGTH];
  ObString select_group_str;
  ObSQLResultSet result;
  ObServer ms_server;
  int64_t pos = 0;
  ObRow row;
  ret = root_server_->get_ms_provider().get_ms(ms_server,true);//get ms sever from rs
  if(OB_SUCCESS != ret)
  {
    YYSYS_LOG(WARN, "get ms failed:ret[%d]", ret);
  }
  else
  {
    if (NULL == group_name)
    {
      YYSYS_LOG(WARN, "get group name  failed:ret[%d]", ret);
    }
    else
    {
      databuff_printf(select_groups,OB_SQL_LENGTH, pos, "select /*+read_consistency(strong) */ group_name, start_version,paxos_id from __all_all_group where group_name = '%.*s'" ,
                      static_cast<int32_t>(strlen(group_name)),group_name);
      if(pos >= OB_SQL_LENGTH)
      {
        ret = OB_BUF_NOT_ENOUGH;
        YYSYS_LOG(WARN, "create group buffer not enough, ret=%d", ret);
      }
      else
      {
        select_group_str.assign_ptr(select_groups, static_cast<ObString::obstr_size_t>(pos));
        YYSYS_LOG(INFO, "select_groups=%.*s", select_group_str.length(), select_group_str.ptr());
        if(OB_SUCCESS == ret )
        {
          if (OB_SUCCESS != (ret = root_server_->get_rpc_stub().execute_sql(ms_server,select_group_str,result)))
          {
            YYSYS_LOG(WARN, "execute sql failed, ret=%d", ret);
          }
          else
          {
            while(true)
            {
              ret =get_next_row(result, row);
              if(OB_ITER_END == ret)
              {
                ret = OB_INVALID_ARGUMENT;
                YYSYS_LOG(DEBUG, "no data ret = %d", ret);
                break;
              }
              else if(OB_SUCCESS != ret)
              {
                YYSYS_LOG(WARN, "get next row from ObResultSet failed,ret=%d", ret);
              }
              else
              {
                YYSYS_LOG(DEBUG, "get next row from ObResultSet succ,ret=%d", ret);
                break;
              }
            }
          }
        }
      }
    }
  }
  return ret;
}
int ObRootDDLOperator::get_next_row(ObSQLResultSet &result_set, ObRow &row) const
{
  int ret = OB_SUCCESS;
  ret = result_set.get_new_scanner().get_next_row(row);
  return ret;
}
bool ObRootDDLOperator::is_paxos_id_usable(int32_t &paxos_id) const
{

  ObBitSet<MAX_UPS_COUNT_ONE_CLUSTER> new_set;
  root_server_->get_cur_paxos_offline_set(new_set);
  return !new_set.has_member(paxos_id);
}
int ObRootDDLOperator::add_group_and_paxos_id(char * group_name,int32_t &paxos_id,ObArray<int32_t> &paxos_id_array,ObArray<ObString> &group_name_list)
{
  int ret = OB_SUCCESS;
  ObString tem_string;
  ObString tmp_buf;
  tmp_buf.assign_ptr(group_name,static_cast<int32_t>(strlen(group_name)));
  if (OB_SUCCESS == Str_buf.write_string(tmp_buf, &tem_string))
  {
    group_name_list.push_back(tem_string);
    paxos_id_array.push_back(paxos_id);
  }
  else
  {
    ret = OB_ERROR;
  }
  return ret;
}

int ObRootDDLOperator:: alter_group(common::AlterTableSchema &table_schema,ObArray<int32_t> &paxos_id_array,ObArray<ObString> &group_name_list)
{
  int ret = OB_SUCCESS;
  int32_t paxos_id = OB_INVALID_PAXOS_ID;
  char name_prefix[OB_MAX_TABLE_NAME_LENGTH] = {0};
  snprintf(name_prefix,OB_MAX_TABLE_NAME_LENGTH,"%s",table_schema.group_name_prefix_);
  if (NULL == &table_schema)
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN,"frozen_version is invalid");
  }
  else
  {
    ObTablePartitionType part_type = static_cast<ObTablePartitionType>(table_schema.partition_type_);
    if(table_schema.is_rule_modify_ && OB_WITHOUT_PARTITION == part_type)
    {

      if(OB_SUCCESS != (ret = get_online_paxos_id(paxos_id)))
      {
        YYSYS_LOG(WARN,"get paxos_id failed ,ret = %d",ret);
      }
      else if(OB_SUCCESS == (ret = check_group_name(name_prefix)))
      {
        YYSYS_LOG(DEBUG,"group name is already exit ,ret = %d",ret);
      }
      else
      {
        if(OB_SUCCESS != (ret = add_group_and_paxos_id(name_prefix,paxos_id,paxos_id_array,group_name_list)))
        {
          YYSYS_LOG(WARN,"add group and paxos id failed,ret = %d",ret);
        }
      }
    }
    else if(true == table_schema.is_rule_modify_ && OB_DIRECT_PARTITION == part_type)
    {
      int64_t type_64 = 0;
      char part_func_rule_body_[OB_MAX_VARCHAR_LENGTH];
      if(OB_SUCCESS != (ret = get_table_rule_func_info(table_schema.partition_func_name_,part_func_rule_body_,type_64)))
      {
        YYSYS_LOG(WARN,"get table rule func failed ,ret[%d]",ret);
      }
      else if((true == table_schema.is_rule_modify_ && RANGEFUNC == type_64)|| (true == table_schema.is_rule_modify_ && LISTFUNC == type_64 ))
      {
        char *sub_str= strtok(name_prefix,",");
        while(NULL != sub_str)
        {
          if(OB_SUCCESS != (ret = get_online_paxos_id(paxos_id)))
          {
            YYSYS_LOG(WARN,"get paxos_id failed ,ret = %d",ret);
          }
          else if(OB_SUCCESS == (ret = check_group_name(sub_str)))
          {
            YYSYS_LOG(DEBUG,"group name is already exit ,ret = %d",ret);
          }
          else
          {
            if(OB_SUCCESS != (ret = add_group_and_paxos_id(sub_str,paxos_id,paxos_id_array,group_name_list)))
            {
              YYSYS_LOG(WARN,"add group and paxos id failed,ret = %d",ret);
              break;
            }
          }
          sub_str = strtok(NULL,",");
        }
      }
      else if (true == table_schema.is_rule_modify_ && PRERANGE == type_64)
      {
        int64_t count = 0;
        if(OB_SUCCESS != (ret = get_range_group_num(part_func_rule_body_,count)))
        {
          YYSYS_LOG(WARN,"get range num failed ,ret = %d",ret);
        }
        else
        {
          for(int64_t i = 0; i < count; i ++)
          {
            char group_name[OB_MAX_TABLE_NAME_LENGTH +4] = {0};

            if(OB_SUCCESS != (ret = get_online_paxos_id(paxos_id)))
            {
              YYSYS_LOG(WARN,"get paxos_id failed ,ret = %d",ret);
            }
            else if(OB_SUCCESS != (ret = spell_complete_group_name(name_prefix,i,group_name)) )
            {
              YYSYS_LOG(WARN,"get  complete group name failed ,ret = %d",ret);
            }
            else if(OB_SUCCESS == (ret = check_group_name(group_name)))
            {
              YYSYS_LOG(DEBUG,"group name is already exit ,ret = %d",ret);
            }
            else
            {
              if(OB_SUCCESS != (ret = add_group_and_paxos_id(group_name,paxos_id,paxos_id_array,group_name_list)))
              {
                YYSYS_LOG(WARN,"add group and paxos id failed,ret = %d",ret);
                break;
              }
            }
          }
        }
      }
      else if(true == table_schema.is_rule_modify_ && HASHFUNC == type_64)
      {
        for(int64_t count = 0; count < OB_MAX_MOD_NUM; count ++)
        {
          char group_name[OB_MAX_TABLE_NAME_LENGTH +4] = {0};
          if(OB_SUCCESS != (ret = get_online_paxos_id(paxos_id)))
          {
            YYSYS_LOG(WARN,"get paxos_id failed ,ret = %d",ret);
          }
          else if(OB_SUCCESS != (ret = spell_complete_group_name(name_prefix,count,group_name,true)) )
          {
            YYSYS_LOG(WARN,"get  complete group name failed ,ret = %d",ret);
          }
          else if(OB_SUCCESS == (ret = check_group_name(group_name)))
          {
            YYSYS_LOG(DEBUG,"group name is already exit ,ret = %d",ret);
          }
          else
          {
            if(OB_SUCCESS != (ret = add_group_and_paxos_id(group_name,paxos_id,paxos_id_array,group_name_list)))
            {
              YYSYS_LOG(WARN,"add group and paxos id failed,ret = %d",ret);
              break;
            }
          }
        }
      }
      else if (true == table_schema.is_rule_modify_ && HARDCODEFUNC == type_64)
      {
        for(int64_t count = 0; count < OB_MAX_MOD_NUM; count ++)
        {
          char group_name[OB_MAX_TABLE_NAME_LENGTH +4] = {0};

          if(OB_SUCCESS != (ret = get_online_paxos_id(paxos_id)))
          {
            YYSYS_LOG(WARN,"get paxos_id failed ,ret = %d",ret);
          }
          else if(OB_SUCCESS != (ret = spell_complete_group_name(name_prefix, count, group_name, true)) )
          {
            YYSYS_LOG(WARN,"get  complete group name failed ,ret = %d",ret);
          }
          else if(OB_SUCCESS == (ret = check_group_name(group_name)))
          {
            YYSYS_LOG(DEBUG,"group name is already exit ,ret = %d",ret);
          }
          else
          {
            if(OB_SUCCESS != (ret = add_group_and_paxos_id(group_name,paxos_id,paxos_id_array,group_name_list)))
            {
              YYSYS_LOG(WARN,"add group and paxos id failed,ret = %d",ret);
              break;
            }
          }
        }
      }
      else if(true == table_schema.is_rule_modify_ && ENUMFUNC == type_64)
      {
        YYSYS_LOG(USER_ERROR,"Can't alter table rule to enum function.");
        ret = OB_ERROR;
      }
    }
    else if(true == table_schema.is_rule_modify_ && OB_REFERENCE_PARTITION == part_type)
    {
      ret = OB_INVALID_ARGUMENT;
      YYSYS_LOG(WARN, "reference partition not supported");
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      YYSYS_LOG(WARN, "wrong partition type");
    }
  }
  return ret;
}
int ObRootDDLOperator::get_table_rule_func_info(const char * func_name,char * func_rule_body,int64_t &type_64)
{
  int ret = OB_SUCCESS;
  ObCellInfo *rule_body = NULL;
  ObCellInfo *func_type = NULL;
  nb_accessor::QueryRes *res = NULL;
  nb_accessor::TableRow *table_row = NULL ;
  ObObj table_name_obj;
  ObString table_name_str;
  ObString rule_body_str;
  ObRowkey rowkey;
  char name_char[OB_MAX_TABLE_NAME_LENGTH];
  if(NULL != func_name)
  {
    int func_name_len = static_cast<int>(strlen(func_name));
    strncpy(name_char, func_name, func_name_len);
    table_name_str.assign_ptr(name_char, static_cast<int32_t>(strlen(func_name)));
    table_name_obj.set_varchar(table_name_str);
    rowkey.assign(&table_name_obj,1);

    if (OB_SUCCESS  != (ret = nb_accessor_.get(res,OB_ALL_PARTITION_RULES_NAME,rowkey,nb_accessor::SC("rule_name")("rule_body")("type"))))
    {
      YYSYS_LOG(WARN,"get partition row failed ,ret[%d]",ret);
    }
    else if(NULL == (table_row = res->get_only_one_row()))
    {
      YYSYS_LOG(USER_ERROR,"partition function '%.*s' doesn't exist",table_name_str.length(),table_name_str.ptr());
      ret = OB_ERR_FUNCTION_NOT_EXISTS;
    }
    else
    {
      func_type = table_row->get_cell_info("type");
      func_type->value_.get_int(type_64);
      rule_body = table_row->get_cell_info("rule_body");
      rule_body->value_.get_varchar(rule_body_str);
      snprintf(func_rule_body,OB_MAX_VARCHAR_LENGTH,"%.*s",rule_body_str.length(),rule_body_str.ptr());
    }
    if(NULL != res)
    {
      nb_accessor_.release_query_res(res);
      res = NULL;
    }
  }
  else
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN,"get func_name failed ,ret[%d]",ret);
  }
  return ret;
}
int ObRootDDLOperator::get_enum_group_num(char *func_rule_body,int64_t &count)
{
  int ret = OB_SUCCESS;
  if (NULL == func_rule_body)
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN,"frozen_version is invalid");
  }
  else
  {
    char rule_body[OB_MAX_VARCHAR_LENGTH] = {0};
    snprintf(rule_body,OB_MAX_VARCHAR_LENGTH,"%s",func_rule_body);
    char *sub_str= strtok(rule_body,"]");
    while( NULL != sub_str)
    {
      sub_str = strtok(NULL,"]");
      count ++;
      YYSYS_LOG(DEBUG,"get partition row failed ,%s",sub_str);
    }
  }
  return ret;
}
//add e

int ObRootDDLOperator::get_range_group_num(char *func_rule_body, int64_t &count)
{
  int ret = OB_SUCCESS;
  if (NULL == func_rule_body)
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN,"frozen_version is invalid");
  }
  else
  {
    char rule_body[OB_MAX_VARCHAR_LENGTH] = {0};
    snprintf(rule_body,OB_MAX_VARCHAR_LENGTH,"%s",func_rule_body);
    char *sub_str= strtok(rule_body,")");
    while( NULL != sub_str)
    {
      sub_str = strtok(NULL,")");
      count ++;
      YYSYS_LOG(DEBUG,"get partition row failed ,%s",sub_str);
    }
  }
  return ret;
}
