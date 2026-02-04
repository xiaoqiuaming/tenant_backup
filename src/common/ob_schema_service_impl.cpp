#include "ob_schema_service_impl.h"
#include "ob_extra_tables_schema.h"
#include "ob_schema_service.h"
#include "utility.h"

//add liuxiao [secondary index col checksum] 20150401
#include "common/ob_column_checksum.h"
//add e

using namespace oceanbase;
using namespace common;
using namespace nb_accessor;

#define DEL_ROW(table_name, rowkey) \
  if (OB_SUCCESS == ret) \
{ \
  ret = mutator->del_row(table_name, rowkey); \
  if(OB_SUCCESS != ret) \
{ \
  YYSYS_LOG(WARN, "insert del to mutator fail:ret[%d]", ret); \
  } \
  }

#define ADD_VARCHAR(table_name, rowkey, column_name, value) \
  if (OB_SUCCESS == ret) \
{ \
  ObObj vchar_value; \
  vchar_value.set_varchar(OB_STR(value)); \
  ret = mutator->insert(table_name, rowkey, OB_STR(column_name), vchar_value); \
  if(OB_SUCCESS != ret) \
{ \
  YYSYS_LOG(WARN, "insert value to mutator fail:column_name[%s], ret[%d]", column_name, ret); \
  } \
  }

#define ADD_INT(table_name, rowkey, column_name, value) \
  if(OB_SUCCESS == ret) \
{ \
  ObObj int_value; \
  int_value.set_int(value); \
  ret = mutator->insert(table_name, rowkey, OB_STR(column_name), int_value); \
  if(OB_SUCCESS != ret) \
{ \
  YYSYS_LOG(WARN, "insert value to mutator fail:column_name[%s], ret[%d]", column_name, ret); \
  } \
  }
#define ADD_CREATE_TIME(table_name, rowkey, column_name, value) \
  if(OB_SUCCESS == ret) \
{ \
  ObObj time_value; \
  time_value.set_createtime(value); \
  ret = mutator->insert(table_name, rowkey, OB_STR(column_name), time_value); \
  if(OB_SUCCESS != ret) \
{ \
  YYSYS_LOG(WARN, "insert value to mutator fail:column_name[%s], ret[%d]", column_name, ret); \
  } \
  }
#define ADD_MODIFY_TIME(table_name, rowkey, column_name, value) \
  if(OB_SUCCESS == ret) \
{ \
  ObObj time_value; \
  time_value.set_modifytime(value); \
  ret = mutator->insert(table_name, rowkey, OB_STR(column_name), time_value); \
  if(OB_SUCCESS != ret) \
{ \
  YYSYS_LOG(WARN, "insert value to mutator fail:column_name[%s], ret[%d]", column_name, ret); \
  } \
  }
//add wenghaixing[secondary index]20141029


#define ADD_PRECISE_TIME(table_name,rowkey,column_name,value)\
  if(OB_SUCCESS == ret) \
{ \
  ObObj time_value; \
  time_value.set_precise_datetime(value); \
  ret = mutator->update(table_name, rowkey, OB_STR(column_name), time_value); \
  if(OB_SUCCESS != ret) \
{ \
  YYSYS_LOG(WARN, "update value to mutator fail:column_name[%s], ret[%d]", column_name, ret); \
  } \
  }
//add e
//add fyd [NotNULL_check] [JHOBv0.1] 20140108:b
///*expr:to update specified column of the row*/
#define UPDATE_VARCHAR(table_name, rowkey, column_name, value) \
  if (OB_SUCCESS == ret) \
{ \
  ObObj vchar_value; \
  vchar_value.set_varchar(OB_STR(value)); \
  ret = mutator->update(table_name, rowkey, OB_STR(column_name), vchar_value); \
  if(OB_SUCCESS != ret) \
{ \
  YYSYS_LOG(WARN, "update value to mutator fail:column_name[%s], ret[%d]", column_name, ret); \
  } \
  }

#define UPDATE_VARCHAR_TO_NULL(table_name, rowkey, column_name) \
  if (OB_SUCCESS == ret) \
{ \
  ObObj vchar_value; \
  vchar_value.set_null(); \
  ret = mutator->update(table_name, rowkey, OB_STR(column_name), vchar_value); \
  if(OB_SUCCESS != ret) \
{ \
  YYSYS_LOG(WARN, "update value to mutator fail:column_name[%s], ret[%d]", column_name, ret); \
  } \
  }

#define UPDATE_INT(table_name, rowkey, column_name, value) \
  if(OB_SUCCESS == ret) \
{ \
  ObObj int_value; \
  int_value.set_int(value); \
  ret = mutator->update(table_name, rowkey, OB_STR(column_name), int_value); \
  if(OB_SUCCESS != ret) \
{ \
  YYSYS_LOG(WARN, "update value to mutator fail:column_name[%s], ret[%d]", column_name, ret); \
  } \
  }
/*#define UPDATE_CREATE_TIME(table_name, rowkey, column_name, value) \
if(OB_SUCCESS == ret) \
{ \
  ObObj time_value; \
  time_value.set_createtime(value); \
  ret = mutator->update(table_name, rowkey, OB_STR(column_name), time_value); \
  if(OB_SUCCESS != ret) \
  { \
    YYSYS_LOG(WARN, "update value to mutator fail:column_name[%s], ret[%d]", column_name, ret); \
  } \
}
#define UPDATE_MODIFY_TIME(table_name, rowkey, column_name, value) \
if(OB_SUCCESS == ret) \
{ \
  ObObj time_value; \
  time_value.set_modifytime(value); \
  ret = mutator->update(table_name, rowkey, OB_STR(column_name), time_value); \
  if(OB_SUCCESS != ret) \
  { \
    YYSYS_LOG(WARN, "update value to mutator fail:column_name[%s], ret[%d]", column_name, ret); \
  } \
}*/
//add 20140108:e

//add wenghaixing [secondary index] 20141029
int ObSchemaServiceImpl::add_index_process(ObMutator *mutator, const TableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if(NULL == mutator)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(WARN,"mutator is null");
  }

  ObRowkey rowkey;
  ObObj value[1];
  IndexHelper ih;
  //for(int64_t i=0;i<table_schema.ih_array_.count();i++)
  //{
  int64_t timestamp = yysys::CTimeUtil::getTime();
  ih = table_schema.ih_;
  if(OB_SUCCESS != ret)
  {
    YYSYS_LOG(WARN, "get index_process from table_schema fail:ret[%d]", ret);
    //break;
  }
  //value[0].set_int(ih.tbl_tid);
  // value[1].set_int(table_schema.table_id_ );
  value[0].set_precise_datetime(timestamp);
  rowkey.assign(value, 1);
  ADD_INT(index_process_name, rowkey, "table_id", ih.tbl_tid);
  ADD_INT(index_process_name, rowkey, "index_id", table_schema.table_id_);
  //ADD_PRECISE_TIME(index_process_name, rowkey, "write_time", timestamp);
  char status1[10]="AVALIBALE";
  char status2[14]="NOT_AVALIBALE";
  ADD_VARCHAR(index_process_name, rowkey, "index_stat", ih.status==AVALIBALE?status1:status2);
  //}
  return ret;
}
//add e
int ObSchemaServiceImpl::add_join_info(ObMutator* mutator, const TableSchema& table_schema)
{
  int ret = OB_SUCCESS;

  if(NULL == mutator)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(WARN, "mutator is null");
  }

  JoinInfo join_info;
  ObRowkey rowkey;

  ObObj value[4];


  if(OB_SUCCESS == ret)
  {
    for(int32_t i=0;i<table_schema.join_info_.count();i++)
    {
      ret = table_schema.join_info_.at(i, join_info);
      if(OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "get joininfo from table_schema fail:ret[%d], i[%d]", ret, i);
        break;
      }

      value[0].set_int(join_info.left_table_id_);
      value[1].set_int(join_info.left_column_id_);
      value[2].set_int(join_info.right_table_id_);
      value[3].set_int(join_info.right_column_id_);
      rowkey.assign(value, 4);

      //������Ҫ
      //rowkey�в���Ҫд�룬��������UPS�˵��޸�����Ժ����ȥ��
      //to be delete start
      // ADD_INT(joininfo_table_name, rowkey, "left_table_id", join_info.left_table_id_);
      // ADD_INT(joininfo_table_name, rowkey, "left_column_id", join_info.left_column_id_);
      // ADD_INT(joininfo_table_name, rowkey, "right_table_id", join_info.right_table_id_);
      // ADD_INT(joininfo_table_name, rowkey, "right_column_id", join_info.right_column_id_);
      // to be delete end
      ADD_VARCHAR(joininfo_table_name, rowkey, "left_table_name", join_info.left_table_name_);
      ADD_VARCHAR(joininfo_table_name, rowkey, "left_column_name", join_info.left_column_name_);
      ADD_VARCHAR(joininfo_table_name, rowkey, "right_table_name", join_info.right_table_name_);
      ADD_VARCHAR(joininfo_table_name, rowkey, "right_column_name", join_info.right_column_name_);

      YYSYS_LOG(DEBUG, "insert mutate join info[%s]", to_cstring(join_info));
    }
  }

  return ret;
}

////add fyd [NotNULL_check] [JHOBv0.1] 20140113:b
///*expr:add single column to table*/
//int ObSchemaServiceImpl::add_single_column(ObMutator* mutator,  ColumnSchema* column,ObRowkey & rowkey)
//{
//	int ret = OB_SUCCESS;
//  	ADD_INT(column_table_name, rowkey, "column_id", column->column_id_);
//  	ADD_INT(column_table_name, rowkey, "column_group_id", column->column_group_id_);
// 	ADD_INT(column_table_name, rowkey, "rowkey_id", column->rowkey_id_);
//  	ADD_INT(column_table_name, rowkey, "join_table_id", column->join_table_id_);
//  	ADD_INT(column_table_name, rowkey, "join_column_id", column->join_column_id_);
//  	ADD_INT(column_table_name, rowkey, "data_type", column->data_type_);
//  	ADD_INT(column_table_name, rowkey, "data_length", column->data_length_);
//  	ADD_INT(column_table_name, rowkey, "data_precision", column->data_precision_);
//  	ADD_INT(column_table_name, rowkey, "data_scale", column->data_scale_);
//  	ADD_INT(column_table_name, rowkey, "nullable", column->nullable_);
//  	ADD_INT(column_table_name, rowkey, "length_in_rowkey", column->length_in_rowkey_);
//  	ADD_INT(column_table_name, rowkey, "order_in_rowkey", column->order_in_rowkey_);
//	return ret;
//}
////add 20140113:e
int ObSchemaServiceImpl::add_column(ObMutator* mutator, const TableSchema& table_schema)
{
  int ret = OB_SUCCESS;

  if(NULL == mutator)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(WARN, "mutator is null");
  }

  ColumnSchema column;
  ObRowkey rowkey;
  ObString column_name;

  ObObj value[2];
  value[0].set_int(table_schema.table_id_);


  if (OB_SUCCESS == ret)
  {
    for(int32_t i=0;i<table_schema.columns_.count() && OB_SUCCESS == ret;i++)
    {
      ret = table_schema.columns_.at(i, column);
      if(OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "get column from table_schema fail:ret[%d], i[%d]", ret, i);
      }

      if(OB_SUCCESS == ret)
      {
        column_name.assign_ptr(column.column_name_, static_cast<int32_t>(strlen(column.column_name_)));
        value[1].set_varchar(column_name);
        rowkey.assign(value, 2);
        ADD_INT(column_table_name, rowkey, "column_id", column.column_id_);
        ADD_INT(column_table_name, rowkey, "column_group_id", column.column_group_id_);
        ADD_INT(column_table_name, rowkey, "rowkey_id", column.rowkey_id_);
        ADD_INT(column_table_name, rowkey, "join_table_id", column.join_table_id_);
        ADD_INT(column_table_name, rowkey, "join_column_id", column.join_column_id_);
        ADD_INT(column_table_name, rowkey, "data_type", column.data_type_);
        ADD_INT(column_table_name, rowkey, "data_length", column.data_length_);
        ADD_INT(column_table_name, rowkey, "data_precision", column.data_precision_);
        ADD_INT(column_table_name, rowkey, "data_scale", column.data_scale_);
        ADD_INT(column_table_name, rowkey, "nullable", column.nullable_);
        if(!column.default_value_is_null)
        {
          ADD_VARCHAR(column_table_name, rowkey, "column_default", column.default_value_);
        }
        ADD_INT(column_table_name, rowkey, "length_in_rowkey", column.length_in_rowkey_);
        ADD_INT(column_table_name, rowkey, "order_in_rowkey", column.order_in_rowkey_);
      }
    }
  }

  return ret;
}

//add zhaoqiong [Schema Manager] 20150327:b
int ObSchemaServiceImpl::add_ddl_operation(ObMutator* mutator, const uint64_t& table_id, const DdlType ddl_type)
{
  int ret = OB_SUCCESS;

  if(NULL == mutator)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(WARN, "mutator is null");
  }
  else
  {
    ObObj schema_version;
    int64_t last_timestamp = schema_timestamp_;
    schema_timestamp_ = yysys::CTimeUtil::getTime();
    if (schema_timestamp_ <= last_timestamp)
    {
      YYSYS_LOG(WARN, "timestamp error,last_timestamp=%ld, current_timestamp=%ld",last_timestamp,schema_timestamp_);
      schema_timestamp_ = last_timestamp + 1;
    }
    schema_version.set_int(schema_timestamp_);
    //modify liuxiao [secondary index] 20150713
    //���Ӽ��table_id��Ϣ
    //YYSYS_LOG(INFO, "ddl operation , schema timestamp=%ld",schema_timestamp_);
    YYSYS_LOG(INFO, "ddl operation , schema timestamp=%ld table_id:%ld",schema_timestamp_,table_id);
    //modify e
    ObRowkey rowkey;
    rowkey.assign(&schema_version, 1);

    ADD_INT(ddl_operation_name, rowkey, "table_id", table_id);
    ADD_INT(ddl_operation_name, rowkey, "operation_type", ddl_type);
  }

  return ret;
}
//add:e

ObSchemaServiceImpl::ObSchemaServiceImpl()
  :client_proxy_(NULL), is_id_name_map_inited_(false), only_core_tables_(true) ,schema_timestamp_(0)//add zhaoqiong [Schema Manager] 20150327
{
}

ObSchemaServiceImpl::~ObSchemaServiceImpl()
{
  client_proxy_ = NULL;
  is_id_name_map_inited_ = false;
}

bool ObSchemaServiceImpl::check_inner_stat()
{
  bool ret = true;
  yysys::CThreadGuard guard(&mutex_);
  if(!is_id_name_map_inited_)
  {
    int err = init_id_name_map();
    if(OB_SUCCESS != err)
    {
      ret = false;
      YYSYS_LOG(WARN, "init id name map fail:ret[%d]", err);
    }
    else
    {
      is_id_name_map_inited_ = true;
    }
  }

  if(ret && NULL == client_proxy_)
  {
    YYSYS_LOG(ERROR, "client proxy is NULL");
    ret = false;
  }
  return ret;
}

//[416]
#define str_is_str(string)\
  ({\
  bool ret = true;\
  size_t str_len = strlen(string);\
  if(str_len < 2 || string[0] != '\''){ret = false;}\
  else if(string[str_len - 1] != '\''){ret = false;}\
  ret;\
  })

int ObSchemaServiceImpl::check_param_type(const char *func_rule_body, const TableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  char rule_body[OB_MAX_VARCHAR_LENGTH] = {0};
  char param_list[OB_MAX_VARCHAR_LENGTH] = {0};

  snprintf(rule_body, OB_MAX_VARCHAR_LENGTH, "%s", func_rule_body);
  snprintf(param_list, OB_MAX_VARCHAR_LENGTH, "%s", table_schema.param_list_);

  char* func_param_list = strtok(rule_body, ")") + 1;
  char* param_col_list = param_list;
  char* param[table_schema.partition_key_col_num_], *col[table_schema.partition_key_col_num_];

  int i = 0;
  while((param[i] = strsep(&func_param_list, ",")) != NULL)
  {
    i++;
    if(i >= table_schema.partition_key_col_num_)
    {
      break;
    }
  }
  if(i != table_schema.partition_key_col_num_)
  {
    ret = OB_ERR_UNEXPECTED;
    YYSYS_LOG(ERROR, "get partition func params wrong, ret=%d", ret);
  }

  i = 0;
  if(OB_SUCCESS == ret)
  {
    while((col[i] = strsep(&param_col_list, ",")) != NULL)
    {
      i++;
      if(i >= table_schema.partition_key_col_num_)
      {
        break;
      }
    }

    if(i != table_schema.partition_key_col_num_)
    {
      ret = OB_ERR_UNEXPECTED;
      YYSYS_LOG(ERROR, "get partition func params wrong, ret=%d", ret);
    }
  }
  for(i = 0; i < table_schema.partition_key_col_num_ && OB_SUCCESS == ret; ++i)
  {
    const ColumnSchema* col_i = table_schema.get_column_schema(col[i]);
    if(str_is_str(param[i]) && col_i->data_type_ != ObVarcharType)
    {
      ret = OB_INVALID_ARGUMENT;
      YYSYS_LOG(WARN, "the type of col[%s] is [%d], but the param type of partition func is [%d], not compatible"
                ,col_i->column_name_, col_i->data_type_, ObVarcharType);
      break;
    }
    else if(!str_is_str(param[i]) && (col_i->data_type_ != ObInt32Type && col_i->data_type_ != ObIntType))
    {
      ret = OB_INVALID_ARGUMENT;
      YYSYS_LOG(WARN, "the type of col[%s] is [%d], but the param type of partition func is int, not compatible"
                ,col_i->column_name_, col_i->data_type_);
      break;
    }
  }
  return ret;
}

//add liu jun.[MultiUPS] [sql_api] 20150324:b
//mod wuna [MultiUps][sql_api] 20151217:b
//int ObSchemaServiceImpl::check_function_schema(const TableSchema &table_schema)
template <class T>
int ObSchemaServiceImpl::check_function_schema(const T& table_schema, const TableSchema &old_table_schema) //[416]
//mod 20151217:e
{
  int ret = OB_SUCCESS;
  QueryRes* res = NULL;
  ObRowkey rowkey;
  TableRow *table_row = NULL;
  bool flag = false;
  ObObj table_name_obj;
  ObString table_name_str;

  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "check inner stat fail");
  }
  //if the table doesn't exists partition function,return success
  else if('\0' == table_schema.partition_func_name_[0])
  {
    flag =true;
  }
  else if(OB_SUCCESS == ret)
  {
    char name_char[OB_MAX_TABLE_NAME_LENGTH];
    int func_name_len = static_cast<int>(strlen(table_schema.partition_func_name_));
    strncpy(name_char, table_schema.partition_func_name_, func_name_len);
    table_name_str.assign_ptr(name_char,
                              static_cast<int32_t>(strlen(table_schema.partition_func_name_)));
    table_name_obj.set_varchar(table_name_str);
    rowkey.assign(&table_name_obj,1);
    ret = nb_accessor_.get(res,OB_ALL_PARTITION_RULES_NAME,rowkey,SC("rule_name")
                           ("rule_par_num")("type")("rule_body"));
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN,"get function schema failed:ret[%d]",ret);
    }
  }
  if(OB_SUCCESS == ret && false == flag)
  {
    //check whether function exists.
    table_row = res->get_only_one_row();
    if(NULL == table_row)
    {
      YYSYS_LOG(USER_ERROR,"partition function '%.*s' doesn't exist",table_name_str.length(),table_name_str.ptr());
      ret = OB_ERR_FUNCTION_NOT_EXISTS;
    }
    //check parameter's num.
    if(OB_SUCCESS == ret)
    {
      ObCellInfo *func_name = NULL;
      ObCellInfo *param_num = NULL;
      ObCellInfo *func_type = NULL;

      ObCellInfo *func_body = NULL;
      ObString body_str;

      ObString name_str;
      int64_t num_64 = 0;
      int64_t type_64 = 0;
      func_name = table_row->get_cell_info("rule_name");
      param_num = table_row->get_cell_info("rule_par_num");
      func_type = table_row->get_cell_info("type");
      func_body = table_row->get_cell_info("rule_body");
      if(NULL == func_name || func_name->value_.get_type() != ObVarcharType)
      {
        ret = OB_ERROR;
        YYSYS_LOG(WARN, "check function name failed");
      }
      else if(NULL == param_num || param_num->value_.get_type() != ObIntType)
      {
        ret = OB_ERROR;
        YYSYS_LOG(WARN, "check parameter num failed");
      }
      else if(NULL == func_type || func_type->value_.get_type() != ObIntType)
      {
        ret = OB_ERROR;
        YYSYS_LOG(WARN, "check function type failed");
      }
      else if(NULL == func_body || func_body->value_.get_type() != ObVarcharType)
      {
        ret = OB_ERROR;
        YYSYS_LOG(WARN, "check function body failed");
      }
      else
      {
        func_name->value_.get_varchar(name_str);
        param_num->value_.get_int(num_64);
        func_type->value_.get_int(type_64);
        func_body->value_.get_varchar(body_str);
        int32_t num_32 = static_cast<int32_t>(num_64);
        if(num_32 != table_schema.partition_key_col_num_)
        {
          YYSYS_LOG(USER_ERROR,"Wrong number of parameters");
          ret = OB_ERR_PARAM_SIZE;
        }
        else if(true == table_schema.is_rule_modify_ && ENUMFUNC == type_64)
        {
          YYSYS_LOG(USER_ERROR,"Can't alter table rule to enum function.");
          ret = OB_ERROR;
        }
        else if(PRERANGE == type_64 && OB_SUCCESS != (ret = check_param_type(body_str.ptr(), old_table_schema)))
        {
          YYSYS_LOG(USER_ERROR,"Wrong type of parameters");
        }
      }
    }
  }
  return ret;
}
//add 20150324:e

int ObSchemaServiceImpl::init(ObScanHelper* client_proxy, bool only_core_tables)
{
  int ret = OB_SUCCESS;
  yysys::CThreadGuard guard(&mutex_);
  if (NULL == client_proxy)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(WARN, "client proxy is null");
  }
  else if (true == only_core_tables)
  {
    // do not clear id_name_map
    if (!id_name_map_.created())
    {
      if (OB_SUCCESS != (ret = id_name_map_.create(1000)))
      {
        YYSYS_LOG(WARN, "create id_name_map_ fail:ret[%d]", ret);
      }
    }
  }
  else if (id_name_map_.created())
  {
    if (OB_SUCCESS != (ret = id_name_map_.clear()))
    {
      YYSYS_LOG(WARN, "fail to clear id name hash map. ret=%d", ret);
    }
    else
    {
      is_id_name_map_inited_ = false;
      string_buf_.reuse();
    }
  }
  else if (OB_SUCCESS != (ret = id_name_map_.create(1000)))
  {
    YYSYS_LOG(WARN, "create id_name_map_ fail:ret[%d]", ret);
  }


  if (OB_SUCCESS == ret)
  {
    this->client_proxy_ = client_proxy;
    this->only_core_tables_ = only_core_tables;
    ret = nb_accessor_.init(client_proxy_);
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "init nb accessor fail:ret[%d]", ret);
    }
    else
    {
      nb_accessor_.set_is_read_consistency(true);
    }
  }
  return ret;
}
//add wenghaixing [secondary index col checksum] 20141208
//modify liuxiao [muti database] 20150702
// add the param db_name
int ObSchemaServiceImpl::create_modify_status_mutator(ObString index_table_name,ObString db_name, int status, ObMutator *mutator)
//modify:e
{
  int ret = OB_SUCCESS;
  {
    //modify liuxiao [muti database] 20150702
    /*
    ObObj table_name_value;
    table_name_value.set_varchar(index_table_name);
    ObRowkey rowkey;
    rowkey.assign(&table_name_value,1);
    UPDATE_INT(first_tablet_entry_name,rowkey,"index_status",status);
    */
    ObRowkey rowkey;
    ObObj rowkey_list[2];
    rowkey_list[0].set_varchar(index_table_name);
    rowkey_list[1].set_varchar(db_name);
    rowkey.assign(rowkey_list,2);
    UPDATE_INT(first_tablet_entry_name,rowkey,"index_status",status);
    //modify e
  }
  return ret;
}
//add e
int ObSchemaServiceImpl::create_table_mutator(const TableSchema& table_schema,
                                              ObMutator* mutator,
                                              int64_t frozen_version /*= OB_INVALID_FROZEN_VERSION*/,
                                              ObArray<int32_t> *paxos_id_array /* NULL*/,
                                              ObArray<ObString> *group_name_list /* NULL*/)
{
  int ret = OB_SUCCESS;

  ObString table_name;
  table_name.assign_ptr(const_cast<char*>(table_schema.table_name_), static_cast<int32_t>(strlen(table_schema.table_name_)));

  //add zhaoqiong [database manager]@20150609
  ObString db_name;
  db_name.assign_ptr(const_cast<char*>(table_schema.dbname_), static_cast<int32_t>(strlen(table_schema.dbname_)));
  //add:e

  //add dolphin [database manager]@20150609
  ObObj obj[2];
  obj[0].set_varchar(table_name);
  obj[1].set_varchar(db_name);

  //add:e
  //delete dolphin [database manger]@20150609:b
  /**
  ObObj table_name_value;

  table_name_value.set_varchar(table_name);
  **/
  //delete:e
  ObRowkey rowkey;
  //modify dolphin [database manager]@20150609:b
  //rowkey.assign(&table_name_value, 1);
  rowkey.assign(obj,2);
  //modify:e

  //ADD_VARCHAR(first_tablet_entry_name, rowkey, "table_name", table_schema.table_name_);
  ADD_INT(first_tablet_entry_name, rowkey, "table_id", table_schema.table_id_);
  ADD_INT(first_tablet_entry_name, rowkey, "table_type", table_schema.table_type_);
  ADD_INT(first_tablet_entry_name, rowkey, "load_type", table_schema.load_type_);
  ADD_INT(first_tablet_entry_name, rowkey, "table_def_type", table_schema.table_def_type_);
  ADD_INT(first_tablet_entry_name, rowkey, "rowkey_column_num", table_schema.rowkey_column_num_);
  ADD_INT(first_tablet_entry_name, rowkey, "replica_num", table_schema.replica_num_);
  ADD_INT(first_tablet_entry_name, rowkey, "max_used_column_id", table_schema.max_used_column_id_);
  ADD_INT(first_tablet_entry_name, rowkey, "create_mem_version", table_schema.create_mem_version_);
  ADD_INT(first_tablet_entry_name, rowkey, "tablet_max_size", table_schema.tablet_max_size_);
  ADD_INT(first_tablet_entry_name, rowkey, "tablet_block_size", table_schema.tablet_block_size_);
  ADD_VARCHAR(first_tablet_entry_name, rowkey, "compress_func_name", table_schema.compress_func_name_);

  ADD_INT(first_tablet_entry_name, rowkey, "is_use_bloomfilter", table_schema.is_use_bloomfilter_);
  ADD_INT(first_tablet_entry_name, rowkey, "is_pure_update_table", table_schema.is_pure_update_table_);
  //ADD_INT(first_tablet_entry_name, rowkey, "consistency_level", table_schema.consistency_level_);
  ADD_INT(first_tablet_entry_name, rowkey, "is_read_static", table_schema.consistency_level_);
  ADD_INT(first_tablet_entry_name, rowkey, "rowkey_split", table_schema.rowkey_split_);
  ADD_INT(first_tablet_entry_name, rowkey, "max_rowkey_length", table_schema.max_rowkey_length_);
  ADD_INT(first_tablet_entry_name, rowkey, "merge_write_sstable_version", table_schema.merge_write_sstable_version_);
  ADD_INT(first_tablet_entry_name, rowkey, "schema_version", table_schema.schema_version_);
  ADD_VARCHAR(first_tablet_entry_name, rowkey, "expire_condition", table_schema.expire_condition_);
  //ADD_VARCHAR(first_tablet_entry_name, rowkey, "comment_str", table_schema.comment_str_);

  ADD_INT(first_tablet_entry_name, rowkey, "create_time_column_id", table_schema.create_time_column_id_);
  ADD_INT(first_tablet_entry_name, rowkey, "modify_time_column_id", table_schema.modify_time_column_id_);
  //add wenghaixing [secondary index] 20141105
  ADD_INT(first_tablet_entry_name, rowkey, "data_table_id", table_schema.ih_.tbl_tid);
  int64_t st = 2;//status
  //add wenghaixing [secondary index col checksum] 20141217
  st = table_schema.ih_.status;
  ADD_INT(first_tablet_entry_name, rowkey, "index_status", st);
  //add e
  if(OB_SUCCESS == ret && table_schema.table_type_ != TableSchema::VIEW)
  {
    ret = add_column(mutator, table_schema);
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "add column to mutator fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret && table_schema.table_type_ != TableSchema::VIEW)
  {
    ret = add_join_info(mutator, table_schema);
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "add join info to mutator fail:ret[%d]", ret);
    }
  }

  //add liu jun.[MultiUPS] [sql_api] 20150423:b
  if(OB_SUCCESS != ret)
  {
  }
  else if(OB_INVALID_FROZEN_VERSION == frozen_version)
  {
    YYSYS_LOG(WARN,"invalid frozen version=%ld",frozen_version);
  }
  //add lqc [multiups #1.0] 201707027 b
  else if(OB_INVALID_ID != table_schema.ih_.tbl_tid)
  {
    //NOTHING TO
  }//add e
  else
  {
    bool is_range_list_partition = false;
    int64_t table_id = static_cast<int64_t>(table_schema.table_id_);
    ObObj table_rules_obj[2];
    table_rules_obj[0].set_int(table_id);
    table_rules_obj[1].set_int(frozen_version + 1);
    ObRowkey table_rule_row_key;
    table_rule_row_key.assign(table_rules_obj, 2);
    //add wuna[MultiUPS] [sql_api] 20160413:b
    //to join table_id with "__list@" or "__range@" as rule_name.
    char partition_func_name[OB_MAX_VARCHAR_LENGTH]={0};
    int64_t hash_func_name_len = static_cast<int64_t>(strlen(table_schema.partition_func_name_));
    memset(partition_func_name,0,OB_MAX_VARCHAR_LENGTH);
    if(OB_RANGE_PARTITION == table_schema.partition_form_ ||
       OB_RANGE_COLUMNS_PARTITION == table_schema.partition_form_ ||
       OB_LIST_PARTITION == table_schema.partition_form_ ||
       OB_LIST_COLUMNS_PARTITION == table_schema.partition_form_)
    {
      is_range_list_partition = true;
    }
    if(is_range_list_partition)
    {
      char table_id_str[OB_MAX_VARCHAR_LENGTH]={0};
      int64_t table_id_str_len = static_cast<int64_t>(sprintf(table_id_str,"%lu",table_id));
      if(OB_MAX_VARCHAR_LENGTH <= table_id_str_len+hash_func_name_len)
      {
        ret = OB_ERROR;
        YYSYS_LOG(WARN,"rule name is too long.");
      }
      else
      {
        memcpy(partition_func_name,table_schema.partition_func_name_,hash_func_name_len);
        memcpy(partition_func_name+hash_func_name_len,table_id_str,table_id_str_len);
      }
    }
    else
    {
      memcpy(partition_func_name,table_schema.partition_func_name_,hash_func_name_len);
    }
    if(OB_SUCCESS == ret && true == is_range_list_partition)
    {
      ObObj partition_rules_obj[1];
      const ObString partition_func_name_str = ObString::make_string(partition_func_name);
      partition_rules_obj[0].set_varchar(partition_func_name_str);
      ObRowkey part_rule_row_key;
      part_rule_row_key.assign(partition_rules_obj, 1);
      ADD_INT(partition_rule_name, part_rule_row_key, "rule_par_num", table_schema.partition_key_col_num_);
      ADD_VARCHAR(partition_rule_name, part_rule_row_key, "rule_par_list", table_schema.param_list_);
      ADD_VARCHAR(partition_rule_name, part_rule_row_key, "rule_body", table_schema.partition_func_rule_body_);
      ADD_INT(partition_rule_name, part_rule_row_key, "type", table_schema.partition_func_type_);
    }
    //add 20160413:e
    if(OB_SUCCESS == ret)
    {
      ADD_VARCHAR(table_rule_name, table_rule_row_key, "table_name", table_schema.table_name_);
      ADD_INT(table_rule_name, table_rule_row_key, "partition_type", table_schema.table_partition_type_);
      ObTablePartitionType part_type = static_cast<ObTablePartitionType>(table_schema.table_partition_type_);
      if(OB_DIRECT_PARTITION == part_type)
      {
        ADD_VARCHAR(table_rule_name, table_rule_row_key, "prefix_name", table_schema.group_name_prefix_);
        ADD_VARCHAR(table_rule_name, table_rule_row_key, "rule_name", partition_func_name);
        ADD_VARCHAR(table_rule_name, table_rule_row_key, "par_list", table_schema.param_list_);
      }
      else if(OB_WITHOUT_PARTITION == part_type)
      {
        ADD_VARCHAR(table_rule_name, table_rule_row_key, "prefix_name", table_schema.group_name_prefix_);
      }
    }
    //add lqc [MultiUps 1.0] [#13] 20170405 b
    if(OB_SUCCESS == ret)
    {
      if(OB_SUCCESS != (ret = update_all_all_group_mutator(table_schema,mutator,frozen_version,paxos_id_array,group_name_list)))
      {
        YYSYS_LOG(WARN, "update __all_all_group mutator failed,ret = %d",ret);
      }

    }//add e
  }
  //add 20150423:e

  //[view]
  if(ret == OB_SUCCESS && table_schema.table_type_ == TableSchema::VIEW)
  {
      if(OB_SUCCESS != (ret = add_all_view(mutator, table_schema)))
      {
          YYSYS_LOG(WARN, "add __all_view mutator failed, ret = %d", ret);
      }
  }

  return ret;
}
//add fyd [NotNULL_check] [JHOBv0.1] 20140108:b
//int ObSchemaServiceImpl::alter_table_mutator(const AlterTableSchema& table_schema, ObMutator* mutator, TableSchema &old_table_schema)
int ObSchemaServiceImpl::alter_table_mutator(const AlterTableSchema &table_schema, ObMutator *mutator, TableSchema &old_table_schema, ObSchemaManagerV2 *schema_manager)
{
  int ret = OB_SUCCESS;
  ObObj value[2];
  value[0].set_int(table_schema.table_id_);
  ObRowkey rowkey;
  ObString column_name;
  uint64_t max_column_id = 0;
  AlterTableSchema::AlterColumnSchema alter_column;
  if(table_schema.has_table_rename_ == false && table_schema.is_alter_expire_info_ == false
     && table_schema.is_drop_expire_info_ == false
     && table_schema.is_load_type_modify_ == false)//add liuj [Alter_Rename] [JHOBv0.1] 20150104
  {
    for (int32_t i = 0; (OB_SUCCESS == ret) && (i < table_schema.get_column_count()); ++i)
    {
      ret = table_schema.columns_.at(i, alter_column);
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "get column from table_schema fail:ret[%d], i[%d]", ret, i);
      }
      else
      {
        column_name.assign_ptr(alter_column.column_.column_name_,
                               static_cast<int32_t>(strlen(alter_column.column_.column_name_)));
        value[1].set_varchar(column_name);
        rowkey.assign(value, 2);
        switch (alter_column.type_)
        {
          case AlterTableSchema::ADD_COLUMN:
          {
            if (alter_column.column_.column_id_ <= max_column_id)
            {
              YYSYS_LOG(WARN, "check column id failed:column_id[%lu], max[%ld]",
                        alter_column.column_.column_id_, max_column_id);
              ret = OB_INVALID_ARGUMENT;
            }
            else
            {
              max_column_id = alter_column.column_.column_id_;
            }
            // add column succ
            if (OB_SUCCESS == ret)
            {
              ret = update_column_mutator(mutator, rowkey, alter_column.column_);
            }
            break;
          }
          case AlterTableSchema::MOD_COLUMN_NULL:
          {
            ret = renew_null_column_mutator(mutator, rowkey, alter_column.column_);
            break;
          }
          case AlterTableSchema::MOD_COLUMN_DEFAULT:
          {
            ret = renew_default_column_mutator(mutator, rowkey, alter_column.column_);
            break;
          }

          case AlterTableSchema::DEL_COLUMN:
          {
            DEL_ROW(column_table_name, rowkey);
            break;
          }
            //add liuj [Alter_Rename] [JHOBv0.1] 20150104
          case AlterTableSchema::RENAME_COLUMN:
          {
            ret = rename_column_mutator(mutator, rowkey, alter_column.column_);
            break;
          }
            //add e.
          case AlterTableSchema::MOD_VARCHAR_LENGTH:
          {
            ret = mod_datatype_mutator(mutator, rowkey, alter_column.column_);
            IndexList index_tid;
            if(schema_manager == NULL ||
               OB_SUCCESS != (ret = schema_manager->column_hint_index(table_schema.table_id_, alter_column.column_.column_id_, index_tid)))
            {
              YYSYS_LOG(WARN, "failed to get column_hint_index[%d] ", ret);
              ret = OB_SUCCESS;
            }
            else if(OB_SUCCESS == ret && index_tid.get_count() >0)
            {
              //
              for(int64_t i = index_tid.get_count() - 1; i >= 0; i--)
              {
                uint64_t itid = OB_INVALID_ID;
                index_tid.get_idx_id(i, itid);
                if(itid != OB_INVALID_ID)
                {
                  //
                  value[0].set_int(itid);
                  rowkey.assign(value, 2);

                  if(OB_SUCCESS != (ret = mod_datatype_mutator(mutator, rowkey, alter_column.column_)))
                  {
                    YYSYS_LOG(WARN, "fail to modify index table, table_id [%ld]:ret[%d]", itid, ret);
                    break;
                  }
                  else if(OB_SUCCESS != (ret = add_ddl_operation(mutator, itid, ALTER_TABLE)))
                  {
                    YYSYS_LOG(WARN, "add ddl operation to mutator fail:ret[%d]", ret);
                    break;
                  }
                }
              }
              ret = OB_SUCCESS;
            }
            break;
          }
          case AlterTableSchema::MOD_DECIMAL_PRECISION:
          {
            ret = mod_precision_mutator(mutator, rowkey, alter_column.column_);
            IndexList index_tid;
            if(schema_manager == NULL ||
               OB_SUCCESS != (ret = schema_manager->column_hint_index(table_schema.table_id_, alter_column.column_.column_id_, index_tid)))
            {
              YYSYS_LOG(WARN, "failed to get column_hint_index[%d]", ret);
              ret = OB_SUCCESS;
            }
            else if(OB_SUCCESS == ret && index_tid.get_count() >0)
            {
              //
              for(int64_t i = index_tid.get_count() - 1; i >= 0; i--)
              {
                uint64_t itid = OB_INVALID_ID;
                index_tid.get_idx_id(i, itid);
                if(itid != OB_INVALID_ID)
                {
                  //
                  value[0].set_int(itid);
                  rowkey.assign(value, 2);

                  if(OB_SUCCESS != (ret = mod_precision_mutator(mutator, rowkey, alter_column.column_)))
                  {
                    YYSYS_LOG(WARN, "fail to modify index table, table_id [%ld]:ret[%d]", itid, ret);
                    break;
                  }
                  else if(OB_SUCCESS != (ret = add_ddl_operation(mutator, itid, ALTER_TABLE)))
                  {
                    YYSYS_LOG(WARN, "add ddl operation to mutator fail:ret[%d]", ret);
                    break;
                  }
                }
              }
              ret = OB_SUCCESS;
            }
            break;
          }


          default :
          {
            ret = OB_INVALID_ARGUMENT;
            break;
          }
        }
      }
    }
  }
  else if(true == table_schema.is_alter_expire_info_ || true == table_schema.is_drop_expire_info_)
  {
    ret = reset_expire_info(mutator, table_schema, old_table_schema);
    if(ret == OB_SUCCESS && schema_manager != NULL)
    {
      IndexList index_list;
      if(OB_SUCCESS != (ret = schema_manager->get_index_list(table_schema.table_id_, index_list)))
      {
        YYSYS_LOG(WARN, "get table index list failed, err=%d", ret);
      }
      else if(index_list.get_count() > 0 && ret == OB_SUCCESS)
      {
        for(int64_t i = index_list.get_count() - 1; i >= 0; i--)
        {
          uint64_t idx_tid = OB_INVALID_ID;
          index_list.get_idx_id(i, idx_tid);
          if(idx_tid != OB_INVALID_ID)
          {
            ObTableSchema *old_index_table_schema = schema_manager->get_table_schema(idx_tid);
            if(old_index_table_schema != NULL)
            {
              const char *table_name_char = old_index_table_schema->get_table_name();
              char t_name[OB_MAX_TABLE_NAME_LENGTH] = {0};
              strcpy(t_name, table_name_char + strlen(table_schema.dbname_) + 1);
              if('\0' != t_name[0])
              {
                ObString table_name;
                table_name.assign_ptr(t_name, static_cast<int32_t>(strlen(t_name)));
                ObString db_name;
                db_name.assign_ptr(const_cast<char *>(table_schema.dbname_), static_cast<int32_t>(strlen(table_schema.dbname_)));
                ObRowkey index_rowkey;
                ObObj ex_value[2];
                ex_value[0].set_varchar(table_name);
                ex_value[1].set_varchar(db_name);
                index_rowkey.assign(ex_value, 2);
                if(OB_SUCCESS != (ret = reset_index_expire_info_mutator(mutator, index_rowkey, table_schema)))
                {
                  YYSYS_LOG(WARN, "fail to modify index table, table_id[%ld]:ret[%d]", idx_tid, ret);
                  break;
                }
                else if(OB_SUCCESS != (ret = add_ddl_operation(mutator, idx_tid, ALTER_TABLE)))
                {
                  YYSYS_LOG(WARN, "add ddl operation to mutator fail:ret[%d]", ret);
                  break;
                }
              }
            }
            else
            {
              ret = OB_SCHEMA_ERROR;
              YYSYS_LOG(WARN, "get index table schema failed, err=%d", ret);
            }
          }
          else
          {
            //YYSYS_LOG(DEBUG);
            break;
          }
        }

      }
    }
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "alter index table expire info failed, err=%d", ret);
    }
  }
  else if(true == table_schema.is_load_type_modify_)
  {
    ret = mod_load_type_mutator(mutator, table_schema, old_table_schema);
  }
  //add liuj [Alter_Rename] [JHOBv0.1] 20150104
  else
  {
    ret = rename_table_mutator(mutator, table_schema, old_table_schema);
  }
  //add e.
  // reset table max used column id
  if ((OB_SUCCESS == ret) && (max_column_id != 0))
  {
    ret = reset_column_id_mutator(mutator, table_schema, max_column_id);
  }
  if ((OB_SUCCESS == ret) && (old_table_schema.schema_version_ >= 0))
  {
    ret = reset_schema_version_mutator(mutator, table_schema, old_table_schema.schema_version_);
  }
  return ret;
}
//add 20140108:e
int ObSchemaServiceImpl::alter_table_mutator(const AlterTableSchema& table_schema, ObMutator* mutator, const int64_t old_schema_version, ObSchemaManagerV2 *schema_manager)
{
  int ret = OB_SUCCESS;
  ObObj value[2];
  value[0].set_int(table_schema.table_id_);
  ObRowkey rowkey;
  ObString column_name;
  uint64_t max_column_id = 0;
  AlterTableSchema::AlterColumnSchema alter_column;
  for (int32_t i = 0; (OB_SUCCESS == ret) && (i < table_schema.get_column_count()); ++i)
  {
    ret = table_schema.columns_.at(i, alter_column);
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "get column from table_schema fail:ret[%d], i[%d]", ret, i);
    }
    else
    {
      column_name.assign_ptr(alter_column.column_.column_name_,
                             static_cast<int32_t>(strlen(alter_column.column_.column_name_)));
      value[1].set_varchar(column_name);
      rowkey.assign(value, 2);
      switch (alter_column.type_)
      {
        case AlterTableSchema::ADD_COLUMN:
        {
          if (alter_column.column_.column_id_ <= max_column_id)
          {
            YYSYS_LOG(WARN, "check column id failed:column_id[%lu], max[%ld]",
                      alter_column.column_.column_id_, max_column_id);
            ret = OB_INVALID_ARGUMENT;
          }
          else
          {
            max_column_id = alter_column.column_.column_id_;
          }
        }
        case AlterTableSchema::MOD_COLUMN:
        {
          // add column succ
          if (OB_SUCCESS == ret)
          {
            ret = update_column_mutator(mutator, rowkey, alter_column.column_);
          }
          break;
        }
        case AlterTableSchema::DEL_COLUMN:
        {
          DEL_ROW(column_table_name, rowkey);
          break;
        }

        case AlterTableSchema::MOD_VARCHAR_LENGTH:
        {
          ret = mod_datatype_mutator(mutator, rowkey, alter_column.column_);
          IndexList index_tid;
          if(schema_manager == NULL ||
             OB_SUCCESS != (ret = schema_manager->column_hint_index(table_schema.table_id_, alter_column.column_.column_id_, index_tid)))
          {
            YYSYS_LOG(WARN, "failed to get column_hint_index[%d]", ret);
            ret = OB_SUCCESS;
          }
          else if(OB_SUCCESS == ret && index_tid.get_count() >0)
          {
            //
            for(int64_t i = index_tid.get_count() - 1; i >= 0; i--)
            {
              uint64_t itid = OB_INVALID_ID;
              index_tid.get_idx_id(i, itid);
              if(itid != OB_INVALID_ID)
              {
                //
                value[0].set_int(itid);
                rowkey.assign(value, 2);

                if(OB_SUCCESS != (ret = mod_datatype_mutator(mutator, rowkey, alter_column.column_)))
                {
                  YYSYS_LOG(WARN, "fail to modify index table, table_id [%ld]:ret[%d]", itid, ret);
                  break;
                }
                else if(OB_SUCCESS != (ret = add_ddl_operation(mutator, itid, ALTER_TABLE)))
                {
                  YYSYS_LOG(WARN, "add ddl operation to mutator fail:ret[%d]", ret);
                  break;
                }
              }
            }
            ret = OB_SUCCESS;
          }
          break;
        }

        case AlterTableSchema::MOD_DECIMAL_PRECISION:
        {
          if(OB_SUCCESS == ret)
          {
            ret = mod_precision_mutator(mutator, rowkey, alter_column.column_);
          }
          IndexList index_tid;
          if(schema_manager == NULL ||
             OB_SUCCESS != (ret = schema_manager->column_hint_index(table_schema.table_id_, alter_column.column_.column_id_, index_tid)))
          {
            YYSYS_LOG(WARN, "failed to get column_hint_index[%d]", ret);
            ret = OB_SUCCESS;
          }
          else if(OB_SUCCESS == ret && index_tid.get_count() >0)
          {
            //
            for(int64_t i = index_tid.get_count() - 1; i >= 0; i--)
            {
              uint64_t itid = OB_INVALID_ID;
              index_tid.get_idx_id(i, itid);
              if(itid != OB_INVALID_ID)
              {
                //
                value[0].set_int(itid);
                rowkey.assign(value, 2);

                if(OB_SUCCESS != (ret = mod_precision_mutator(mutator, rowkey, alter_column.column_)))
                {
                  YYSYS_LOG(WARN, "fail to modify index table, table_id [%ld]:ret[%d]", itid, ret);
                  break;
                }
                else if(OB_SUCCESS != (ret = add_ddl_operation(mutator, itid, ALTER_TABLE)))
                {
                  YYSYS_LOG(WARN, "add ddl operation to mutator fail:ret[%d]", ret);
                  break;
                }
              }
            }
            ret = OB_SUCCESS;
          }
          break;
        }

        default :
        {
          ret = OB_INVALID_ARGUMENT;
          break;
        }
      }
    }
  }
  // reset table max used column id
  if ((OB_SUCCESS == ret) && (max_column_id != 0))
  {
    ret = reset_column_id_mutator(mutator, table_schema, max_column_id);
  }
  if ((OB_SUCCESS == ret) && (old_schema_version >= 0))
  {
    ret = reset_schema_version_mutator(mutator, table_schema, old_schema_version);
  }
  return ret;
}

int ObSchemaServiceImpl::update_column_mutator(ObMutator* mutator, ObRowkey & rowkey, const ColumnSchema & column)
{
  int ret = OB_SUCCESS;
  ADD_INT(column_table_name, rowkey, "column_id", column.column_id_);
  ADD_INT(column_table_name, rowkey, "column_group_id", column.column_group_id_);
  ADD_INT(column_table_name, rowkey, "rowkey_id", column.rowkey_id_);
  ADD_INT(column_table_name, rowkey, "join_table_id", column.join_table_id_);
  ADD_INT(column_table_name, rowkey, "join_column_id", column.join_column_id_);
  ADD_INT(column_table_name, rowkey, "data_type", column.data_type_);
  ADD_INT(column_table_name, rowkey, "data_length", column.data_length_);
  ADD_INT(column_table_name, rowkey, "data_precision", column.data_precision_);
  ADD_INT(column_table_name, rowkey, "data_scale", column.data_scale_);
  ADD_INT(column_table_name, rowkey, "nullable", column.nullable_);
  if(!column.default_value_is_null)
  {
    ADD_VARCHAR(column_table_name, rowkey, "column_default", column.default_value_);
  }
  ADD_INT(column_table_name, rowkey, "length_in_rowkey", column.length_in_rowkey_);
  ADD_INT(column_table_name, rowkey, "order_in_rowkey", column.order_in_rowkey_);
  return ret;
}

//add liuj [Alter_Rename] [JHOBv0.1] 20150104
int ObSchemaServiceImpl::rename_column_mutator(ObMutator* mutator, ObRowkey & rowkey,  ColumnSchema & column)
{
  int ret = OB_SUCCESS;
  int64_t table_id = -1;
  ObString column_name;
  ObObj value[2];
  ObRowkey new_rowkey;

  if(OB_SUCCESS == ret)
  {
    ret = (rowkey.get_obj_ptr()->get_int(table_id));
    value[0].set_int(table_id);
    column_name.assign_ptr(column.new_column_name_,
                           static_cast<int32_t>(strlen(column.new_column_name_)));
    value[1].set_varchar(column_name);
    new_rowkey.assign(value, 2);
  }
  if( OB_SUCCESS == ret)
  {
    DEL_ROW(column_table_name, rowkey);
  }
  if( OB_SUCCESS == ret)
  {
    ret = update_column_mutator(mutator, new_rowkey, column);
  }
  return ret;
}
//add e.

//add liuj [Alter_Rename] [JHOBv0.1] 20150104
int ObSchemaServiceImpl::rename_table_mutator(ObMutator* mutator, const
                                              AlterTableSchema & alter_schema,const TableSchema & old_schema)
{
  int ret = OB_SUCCESS;

  ObString old_table_name;
  old_table_name.assign_ptr(const_cast<char *>(alter_schema.table_name_),static_cast<int32_t>(strlen(alter_schema.table_name_)));
  ObString new_table_name;
  new_table_name.assign_ptr(const_cast<char *>(alter_schema.new_table_name_),static_cast<int32_t>(strlen(alter_schema.new_table_name_)));

  //add zhaoqiong [database manager]@20150611
  ObString db_name;
  db_name.assign_ptr(const_cast<char *>(alter_schema.dbname_),static_cast<int32_t>(strlen(alter_schema.dbname_)));
  //add:e

  //delete dolphin [database manager]@20150609:b
  /**
  ObObj old_table_name_value;
  old_table_name_value.set_varchar(old_table_name);

  ObObj new_table_name_value;
  new_table_name_value.set_varchar(new_table_name);
  **/
  //delete:e
  //add dolphin [database manager]@20150609
  ObObj old[2];
  old[0].set_varchar(old_table_name);
  old[1].set_varchar(db_name);
  //add:e
  ObRowkey old_rowkey;
  //modify dolphin [database manager]@20150609
  //old_rowkey.assign(old, 1);
  old_rowkey.assign(old,2);
  //modify:e
  ObRowkey new_rowkey;
  //add dolphin [database manager]@20150609
  ObObj ne[2];
  ne[0].set_varchar(new_table_name);
  ne[1].set_varchar(db_name);
  //add:e
  //modify dolphin [database manger]@20150609
  //new_rowkey.assign(&new_table_name_value, 1); //[conflict_level B]	�߼��޸�
  new_rowkey.assign(ne,2);
  //modify:e
  DEL_ROW(first_tablet_entry_name, old_rowkey);

  ADD_INT(first_tablet_entry_name, new_rowkey, "create_time_column_id", old_schema.create_time_column_id_);
  ADD_INT(first_tablet_entry_name, new_rowkey, "modify_time_column_id", old_schema.modify_time_column_id_);
  ADD_INT(first_tablet_entry_name, new_rowkey, "table_id", old_schema.table_id_);
  ADD_INT(first_tablet_entry_name, new_rowkey, "table_type", old_schema.table_type_);
  ADD_INT(first_tablet_entry_name, new_rowkey, "load_type", old_schema.load_type_);
  ADD_INT(first_tablet_entry_name, new_rowkey, "table_def_type", old_schema.table_def_type_);
  ADD_INT(first_tablet_entry_name, new_rowkey, "rowkey_column_num", old_schema.rowkey_column_num_);
  /*ADD_INT(first_tablet_entry_name, new_rowkey, "column_num", old_schema.column_num_);*/
  ADD_INT(first_tablet_entry_name, new_rowkey, "max_used_column_id", old_schema.max_used_column_id_);
  ADD_INT(first_tablet_entry_name, new_rowkey, "replica_num", old_schema.replica_num_);
  ADD_INT(first_tablet_entry_name, new_rowkey, "create_mem_version", old_schema.create_mem_version_);
  ADD_INT(first_tablet_entry_name, new_rowkey, "tablet_max_size", old_schema.tablet_max_size_);
  ADD_INT(first_tablet_entry_name, new_rowkey, "max_rowkey_length", old_schema.max_rowkey_length_);
  ADD_VARCHAR(first_tablet_entry_name, new_rowkey, "compress_func_name", old_schema.compress_func_name_);
  ADD_INT(first_tablet_entry_name, new_rowkey, "is_use_bloomfilter", old_schema.is_use_bloomfilter_);
  ADD_INT(first_tablet_entry_name, new_rowkey, "merge_write_sstable_version", old_schema.merge_write_sstable_version_);
  ADD_INT(first_tablet_entry_name, new_rowkey, "is_pure_update_table", old_schema.is_pure_update_table_);
  ADD_INT(first_tablet_entry_name, new_rowkey, "rowkey_split", old_schema.rowkey_split_);
  ADD_VARCHAR(first_tablet_entry_name, new_rowkey, "expire_condition", old_schema.expire_condition_);
  ADD_INT(first_tablet_entry_name, new_rowkey, "tablet_block_size", old_schema.tablet_block_size_);
  ADD_INT(first_tablet_entry_name, new_rowkey, "is_read_static", old_schema.consistency_level_);
  /*ADD_INT(first_tablet_entry_name, new_rowkey, "schema_version", old_schema.schema_version_);*/
  //add liuxiao [secondary index] 20150616
  //����first tablet entry����������
  ADD_INT(first_tablet_entry_name, new_rowkey, "data_table_id", old_schema.ih_.tbl_tid);
  ADD_INT(first_tablet_entry_name, new_rowkey, "index_status", old_schema.ih_.status);
  //add e


  //[469]
  if(OB_SUCCESS == ret)
  {
    QueryRes *res = NULL;
    ObNewRange range;
    int32_t rowkey_column = 2;
    ObObj start_rowkey[rowkey_column];
    ObObj end_rowkey[rowkey_column];
    start_rowkey[0].set_int(old_schema.table_id_);
    start_rowkey[1].set_min_value();
    end_rowkey[0].set_int(old_schema.table_id_);
    end_rowkey[1].set_max_value();
    range.start_key_.assign(start_rowkey, rowkey_column);
    range.end_key_.assign(end_rowkey, rowkey_column);

    if(OB_SUCCESS != (ret = nb_accessor_.scan(res, OB_ALL_TABLE_RULES_NAME, range, SC("start_version"))))
    {
      YYSYS_LOG(WARN, "fail to nb scan cchecksum info of orginal table %d", ret);
    }
    else
    {
      bool update_flag = false;
      while (OB_SUCCESS == ret)
      {
        ObCellInfo *cell_info = NULL;
        TableRow *table_row = NULL;
        int64_t start_version = 0;
        if(NULL == res)
        {
          ret = OB_ERR_UNEXPECTED;
          YYSYS_LOG(ERROR, "results is NULL");
        }
        else
        {
          ret = res->next_row();
        }
        if(OB_SUCCESS == ret)
        {
          if(OB_SUCCESS != (ret = res->get_row(&table_row)))
          {
            YYSYS_LOG(WARN, "get row fail:ret[%d]", ret);
          }
          else if(NULL == table_row)
          {
            YYSYS_LOG(WARN, "failed to get row from query results");
            ret = OB_ERROR;
          }
          else
          {
            cell_info = table_row->get_cell_info("start_version");
            if(NULL == cell_info)
            {
              YYSYS_LOG(WARN, "failed to get cell info");
              ret = OB_ERROR;
            }
            else
            {
              if(OB_SUCCESS != (ret = cell_info->value_.get_int(start_version)))
              {
                YYSYS_LOG(ERROR, "get start_version fail, ret = %d", ret);
              }
              else
              {
                ObRowkey rowkey;
                ObObj rowkey_list[2];
                rowkey_list[0].set_int(old_schema.table_id_);
                rowkey_list[1].set_int(start_version);
                rowkey.assign(rowkey_list, 2);
                //
                UPDATE_VARCHAR(table_rule_name, rowkey, "table_name", alter_schema.new_table_name_);
                if(OB_SUCCESS == ret)
                {
                  update_flag = true;
                }
              }
            }
          }
        }
      }
      if(OB_ITER_END == ret && update_flag == true)
      {
        ret = OB_SUCCESS;
      }
    }

  }

  return ret;
}
//add e.


int ObSchemaServiceImpl::reset_expire_info(ObMutator *mutator, const AlterTableSchema &alter_schema, const TableSchema &old_schema)
{
  int ret = OB_SUCCESS;
  ObString table_name;
  table_name.assign_ptr(const_cast<char *>(old_schema.table_name_), static_cast<int32_t>(strlen(old_schema.table_name_)));

  ObString db_name;
  db_name.assign_ptr(const_cast<char *>(old_schema.dbname_), static_cast<int32_t>(strlen(old_schema.dbname_)));

  ObRowkey rowkey;
  ObObj obj[2];
  obj[0].set_varchar(table_name);
  obj[1].set_varchar(db_name);
  rowkey.assign(obj, 2);

  if(ret == OB_SUCCESS)
  {
    UPDATE_VARCHAR(first_tablet_entry_name, rowkey, "expire_condition", alter_schema.new_expire_info_);
  }
  else
  {
    ret = OB_EXPIRE_CONDITION_ERROR;
    YYSYS_LOG(WARN, "Update expire_condition failed! ret=%d", ret);
  }
  return ret;
}

int ObSchemaServiceImpl::reset_index_expire_info_mutator(ObMutator *mutator, ObRowkey &rowkey, const AlterTableSchema &alter_schema)
{
  int ret = OB_SUCCESS;
  if(ret == OB_SUCCESS)
  {
    UPDATE_VARCHAR(first_tablet_entry_name, rowkey, "expire_condition", alter_schema.new_expire_info_);
  }
  return ret;
}


int ObSchemaServiceImpl::mod_datatype_mutator(ObMutator *mutator, ObRowkey &rowkey, ColumnSchema &column)
{
  int ret = OB_SUCCESS;
  if(OB_SUCCESS == ret)
  {
    UPDATE_INT(column_table_name, rowkey, "data_length", column.data_length_);
  }
  return ret;
}

int ObSchemaServiceImpl::mod_precision_mutator(ObMutator *mutator, ObRowkey &rowkey, ColumnSchema &column)
{
  int ret = OB_SUCCESS;
  UPDATE_INT(column_table_name, rowkey, "data_precision", column.data_precision_);
  UPDATE_INT(column_table_name, rowkey, "data_scale", column.data_scale_);
  return ret;
}

//add fyd [NotNULL_check] [JHOBv0.1] 20140108:b
/*expr:update value of table attribute nullable */
int ObSchemaServiceImpl::renew_null_column_mutator(ObMutator* mutator, ObRowkey & rowkey, const ColumnSchema & column)
{
  int ret = OB_SUCCESS;
  UPDATE_INT(column_table_name, rowkey, "nullable", column.nullable_);
  return ret;
}
//add 20140108:e


int ObSchemaServiceImpl::renew_default_column_mutator(
    ObMutator *mutator,
    ObRowkey &rowkey,
    const ColumnSchema &column)
{
  int ret = OB_SUCCESS;
  if(!column.default_value_is_null)
  {
    UPDATE_VARCHAR(column_table_name, rowkey, "column_default", column.default_value_);
  }
  else
  {
    UPDATE_VARCHAR_TO_NULL(column_table_name, rowkey, "column_default");
  }
  return ret;
}


int ObSchemaServiceImpl::mod_load_type_mutator(ObMutator *mutator, const AlterTableSchema &alter_schema, const TableSchema &old_schema)
{
  int ret = OB_SUCCESS;
  ObString table_name;
  table_name.assign_ptr(const_cast<char *>(old_schema.table_name_), static_cast<int32_t>(strlen(old_schema.table_name_)));

  ObString db_name;
  db_name.assign_ptr(const_cast<char *>(old_schema.dbname_), static_cast<int32_t>(strlen(old_schema.dbname_)));

  ObRowkey rowkey;
  ObObj obj[2];
  obj[0].set_varchar(table_name);
  obj[1].set_varchar(db_name);
  rowkey.assign(obj, 2);

  if(alter_schema.is_use_block_cache_)
  {
    UPDATE_INT(first_tablet_entry_name, rowkey, "load_type", TableSchema::MEMORY);
  }
  else
  {
    UPDATE_INT(first_tablet_entry_name, rowkey, "load_type", TableSchema::DISK);
  }
  return ret;
}

int ObSchemaServiceImpl::reset_column_id_mutator(ObMutator* mutator, const AlterTableSchema & schema, const uint64_t max_column_id)
{
  int ret = OB_SUCCESS;
  if ((mutator != NULL) && (max_column_id > OB_APP_MIN_COLUMN_ID))
  {
    ObString table_name;
    table_name.assign_ptr(const_cast<char*>(schema.table_name_), static_cast<int32_t>(strlen(schema.table_name_)));

    //add zhaoqiong [database manager]@20150611:b
    ObString db_name;
    db_name.assign_ptr(const_cast<char*>(schema.dbname_), static_cast<int32_t>(strlen(schema.dbname_)));
    //add:e
    //delete dolphin [database manager]@20150609:b
    /**
    ObObj table_name_value;     //[conflict_level B]	�߼��޸�
    table_name_value.set_varchar(table_name);
    **/
    //delete:e
    ObRowkey rowkey;
    //add dolphin [database manager]@20150609:b
    ObObj obj[2];
    obj[0].set_varchar(table_name);     //[conflict_level B]	�߼��޸�
    obj[1].set_varchar(db_name);
    rowkey.assign(obj,2);
    //add:e
    //rowkey.assign(&table_name_value, 1);//delete dolphin [database manager]@20150609
    ADD_INT(first_tablet_entry_name, rowkey, "max_used_column_id", max_column_id);
  }
  else
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(WARN, "check input param failed:table_name[%s], max_column_id[%lu]", schema.table_name_, max_column_id);
  }
  return ret;
}

int ObSchemaServiceImpl::reset_schema_version_mutator(ObMutator* mutator, const AlterTableSchema & schema, const int64_t old_schema_version)
{
  int ret = OB_SUCCESS;
  if ((mutator != NULL) && (old_schema_version >= 0))
  {
    ObString table_name;
    //add liuj [Alter_Rename] [JHOBv0.1] 20150104
    if(schema.has_table_rename_ == false)
    {
      table_name.assign_ptr(const_cast<char*>(schema.table_name_), static_cast<int32_t>(strlen(schema.table_name_)));
    }
    else if(schema.has_table_rename_ == true)
    {
      table_name.assign_ptr(const_cast<char*>(schema.new_table_name_), static_cast<int32_t>(strlen(schema.new_table_name_)));
    }
    //add e.

    //add zhaoqiong [database manager]@20150609
    ObString db_name;
    db_name.assign_ptr(const_cast<char*>(schema.dbname_), static_cast<int32_t>(strlen(schema.dbname_)));
    //add:e

    //delete dolphin [database manager]@20150609:b
    /**
    ObObj table_name_value;
    table_name_value.set_varchar(table_name);
    ObRowkey rowkey;
    rowkey.assign(&table_name_value, 1);
    */
    //delete:e
    int64_t new_schema_version = old_schema_version+1;
    //add dolphin [database manager]@20150609:b
    ObRowkey rowkey;
    ObObj obj[2];
    obj[0].set_varchar(table_name);
    obj[1].set_varchar(db_name);
    rowkey.assign(obj,2);
    //add:e
    ADD_INT(first_tablet_entry_name, rowkey, "schema_version", new_schema_version);
  }
  else
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(WARN, "check input param failed:table_name[%s], old_schema_version[%ld]", schema.table_name_, old_schema_version);
  }
  return ret;
}
//add wenghaixing [secondary index col checksum modify stat] 20141208
int ObSchemaServiceImpl::modify_index_stat(ObString index_table_name,uint64_t index_table_id,ObString db_name, int stat)
{
  int ret = OB_SUCCESS;
  //todo modify status of error index table
  yysys::CThreadGuard guard(&cc_mutex_);
  ObMutator* mutator = NULL;
  mutator = GET_TSI_MULT(ObMutator, TSI_COMMON_MUTATOR_1);
  if(NULL == mutator)
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    YYSYS_LOG(WARN, "get thread specific Mutator fail");
  }
  if(OB_SUCCESS == ret)
  {
    ret = mutator->reset();
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "reset ob mutator fail:ret[%d]", ret);
    }
  }
  //add liuxiao [secondary index static index] 20150614
  if (OB_SUCCESS == ret)
  {
    ret = add_ddl_operation(mutator, index_table_id, ALTER_TABLE);
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "add ddl operation to mutator fail:ret[%d]", ret);
    }
  }
  //add e
  if(OB_SUCCESS == ret)
  {
    ret = create_modify_status_mutator(index_table_name,db_name,stat, mutator);
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "create table mutator fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = client_proxy_->mutate(*mutator);
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "apply mutator fail:ret[%d]", ret);
    }
  }
  return ret;
}
//add e
int ObSchemaServiceImpl::create_table(const TableSchema& table_schema,
                                      int64_t frozen_version /* = OB_INVALID_FROZEN_VERSION*/,
                                      ObArray<int32_t> *paxos_id_array/*NULL*/,
                                      ObArray<ObString> *group_name_list /*NULL*/)
{
  int ret = OB_SUCCESS;

  if (!table_schema.is_valid())
  {
    YYSYS_LOG(WARN, "invalid table schema, tid=%lu", table_schema.table_id_);
    ret = OB_ERR_INVALID_SCHEMA;
  }
  else if(!check_inner_stat())
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "check inner stat fail");
  }
  //mod wuna[MultiUPS] [sql_api] 20160413:b
  // add liu jun. [MultiUPS] [sql_api] 20150324:b
  //else if(OB_INVALID_FROZEN_VERSION == frozen_version || OB_SUCCESS != (ret = check_function_schema(table_schema)))
  else if(OB_INVALID_FROZEN_VERSION == frozen_version)
  {
    YYSYS_LOG(WARN, "Invalid frozen version=%ld.",frozen_version);
  }
  else if(OB_SUCCESS != (ret = check_function_schema(table_schema, table_schema))) //[416]
  {
    if((OB_RANGE_PARTITION == table_schema.partition_form_ ||
        OB_RANGE_COLUMNS_PARTITION == table_schema.partition_form_ ||
        OB_LIST_PARTITION == table_schema.partition_form_ ||
        OB_LIST_COLUMNS_PARTITION == table_schema.partition_form_)
       && OB_ERR_FUNCTION_NOT_EXISTS == ret)
    {
      ret = OB_SUCCESS;
    }
    else
    {
      YYSYS_LOG(WARN, "use wrong function when create table");
    }
  }
  //  add 20150324:e
  //mod 20160413:e
  yysys::CThreadGuard guard(&mutex_);

  ObMutator* mutator = NULL;

  if(OB_SUCCESS == ret)
  {
    mutator = GET_TSI_MULT(ObMutator, TSI_COMMON_MUTATOR_1);
    if(NULL == mutator)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      YYSYS_LOG(WARN, "get thread specific Mutator fail");
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = mutator->reset();
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "reset ob mutator fail:ret[%d]", ret);
    }
  }
  //add zhaoqiong [Schema Manager] 20150327:b
  if(OB_SUCCESS == ret)
  {
    //append a CREATE_TABLE operation record to __all_ddl_operation
    ret = add_ddl_operation(mutator, table_schema.table_id_, CREATE_TABLE);
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "add ddl operation to mutator fail:ret[%d]", ret);
    }
  }
  //add:e

  if(OB_SUCCESS == ret)
  {
    //mod liu jun.[MultiUPS] [sql_api] 20150424
    //ret = create_table_mutator(table_schema, mutator)
    ret = create_table_mutator(table_schema, mutator, frozen_version,paxos_id_array,group_name_list);
    //mod:20150424:e
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "create table mutator fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = client_proxy_->mutate(*mutator);
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "apply mutator fail:ret[%d]", ret);
    }
  }

  ObString table_name;

  table_name.assign_ptr(const_cast<char*>(table_schema.table_name_), static_cast<int32_t>(strlen(table_schema.table_name_)));
  //add dolphin [database manager]@20150613:b
  ObString db_name;
  ObString dt;
  char buf[OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1] = {0};
  dt.assign_buffer(buf,OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1);
  db_name.assign_ptr(const_cast<char*>(table_schema.dbname_), static_cast<int32_t>(strlen(table_schema.dbname_)));
  dt.concat(db_name,table_name);
  //add:e
  ObString table_name_store;
  if(OB_SUCCESS == ret)
  {
    yysys::CThreadGuard buf_guard(&string_buf_write_mutex_);
    ret = string_buf_.write_string(/** modify dolphin [database manager]@20150613 table_name */dt, &table_name_store);
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "write string fail:ret[%d]", ret);
    }
  }

  int err = 0;
  if(OB_SUCCESS == ret)
  {
    err = id_name_map_.set(table_schema.table_id_, table_name_store);
    if(hash::HASH_INSERT_SUCC != err)
    {
      if(hash::HASH_EXIST == err)
      {
        YYSYS_LOG(ERROR, "bug table exist:table_id[%lu], table_name_store[%.*s]",
                  table_schema.table_id_, table_name_store.length(), table_name_store.ptr());
      }
      else
      {
        ret = OB_ERROR;
        YYSYS_LOG(WARN, "id name map set fail:err[%d], table_id[%lu], table_name_store[%.*s]", err,
                  table_schema.table_id_, table_name_store.length(), table_name_store.ptr());
      }
    }
  }
  return ret;
}

//add zhaoqiong [Schema Manager] 20150327:b
int ObSchemaServiceImpl::refresh_schema()
{
  int ret = OB_SUCCESS;

  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "check inner stat fail");
  }
  else
  {
    yysys::CThreadGuard guard(&mutex_);
    ObMutator* mutator = GET_TSI_MULT(ObMutator, TSI_COMMON_MUTATOR_1);
    if(NULL == mutator)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      YYSYS_LOG(WARN, "get thread specific Mutator fail");
    }
    else if(OB_SUCCESS != (ret = mutator->reset()))
    {
      YYSYS_LOG(WARN, "reset ob mutator fail:ret[%d]", ret);
    }
    else if(OB_SUCCESS != (ret = add_ddl_operation(mutator, 0, REFRESH_SCHEMA)))
    {
      YYSYS_LOG(WARN, "add ddl operation to mutator fail:ret[%d]", ret);
    }
    else if(OB_SUCCESS != (ret = client_proxy_->mutate(*mutator)))
    {
      YYSYS_LOG(WARN, "apply mutator fail:ret[%d]", ret);
    }
  }

  return ret;
}
//add:e

#define ASSIGN_INT_FROM_ROWKEY(column, rowkey_index, field, type) \
  if(OB_SUCCESS == ret) \
{ \
  ObCellInfo * ci = NULL; \
  int64_t int_value = 0; \
  ci = table_row->get_cell_info(column); \
  if (NULL != ci && NULL != ci->row_key_.ptr() \
  && rowkey_index < ci->row_key_.length() \
  && ci->row_key_.ptr()[rowkey_index].get_type() == ObIntType) \
{ \
  ci->row_key_.ptr()[rowkey_index].get_int(int_value); \
  field = static_cast<type>(int_value); \
  YYSYS_LOG(DEBUG, "get cell info:column[%s], value[%ld]", column, int_value); \
  } \
  else \
{ \
  ret = OB_ERROR; \
  YYSYS_LOG(WARN, "get column[%s] with error cell info %s ", \
  column, NULL == ci ? "nil": print_cellinfo(ci)); \
  } \
  }

#define ASSIGN_VARCHAR_FROM_ROWKEY(column, rowkey_index, field, max_length) \
  if(OB_SUCCESS == ret) \
{ \
  ObCellInfo * ci = NULL; \
  ObString str_value; \
  ci = table_row->get_cell_info(column); \
  if (NULL != ci && NULL != ci->row_key_.ptr() \
  && rowkey_index < ci->row_key_.length() \
  && ci->row_key_.ptr()[rowkey_index].get_type() == ObVarcharType) \
{ \
  ci->row_key_.ptr()[rowkey_index].get_varchar(str_value);  \
  if(str_value.length() >= max_length) \
{ \
  ret = OB_SIZE_OVERFLOW; \
  YYSYS_LOG(WARN, "field max length is not enough:max_length[%ld], str length[%d]", max_length, str_value.length()); \
  } \
  else \
{ \
  memcpy(field, str_value.ptr(), str_value.length()); \
  field[str_value.length()] = '\0'; \
  } \
  } \
  else \
{ \
  ret = OB_ERROR; \
  YYSYS_LOG(WARN, "get column[%s] with error cell info %s ", \
  column, NULL == ci ? "nil": print_cellinfo(ci)); \
  } \
  }

#define ASSIGN_VARCHAR(column, field, max_length) \
  if(OB_SUCCESS == ret) \
{ \
  ObCellInfo * cell_info = NULL; \
  ObString str_value; \
  cell_info = table_row->get_cell_info(column); \
  if(NULL != cell_info) \
{ \
  if (cell_info->value_.get_type() == ObNullType) \
{ \
  field[0] = '\0'; \
  } \
  else if (cell_info->value_.get_type() == ObVarcharType)\
{ \
  cell_info->value_.get_varchar(str_value); \
  if(str_value.length() >= max_length) \
{ \
  ret = OB_SIZE_OVERFLOW; \
  YYSYS_LOG(WARN, "field max length is not enough:max_length[%ld], str length[%d]", max_length, str_value.length()); \
  } \
  else \
{ \
  memcpy(field, str_value.ptr(), str_value.length()); \
  field[str_value.length()] = '\0'; \
  } \
  } \
  else \
{ \
  ret = OB_ERROR; \
  YYSYS_LOG(WARN, "get column[%s] with error type %s ", \
  column, print_cellinfo(cell_info)); \
  } \
  } \
  else \
{ \
  ret = OB_ERROR;\
  YYSYS_LOG(WARN, "get column[%s] with error null cell.", column); \
  } \
  }

#define ASSIGN_VARCHAR_IS_NULL(column, field, max_length, value_is_null) \
  if(OB_SUCCESS == ret) \
{ \
  ObCellInfo * cell_info = NULL; \
  ObString str_value; \
  cell_info = table_row->get_cell_info(column); \
  if(NULL != cell_info) \
{ \
  if (cell_info->value_.get_type() == ObNullType) \
{ \
  field[0] = '\0'; \
  value_is_null = true;\
  } \
  else if (cell_info->value_.get_type() == ObVarcharType)\
{ \
  cell_info->value_.get_varchar(str_value); \
  if(str_value.length() >= max_length) \
{ \
  ret = OB_SIZE_OVERFLOW; \
  YYSYS_LOG(WARN, "field max length is not enough:max_length[%ld], str length[%d]", max_length, str_value.length()); \
  } \
  else \
{ \
  memcpy(field, str_value.ptr(), str_value.length()); \
  field[str_value.length()] = '\0'; \
  value_is_null = false;\
  } \
  } \
  else \
{ \
  ret = OB_ERROR; \
  YYSYS_LOG(WARN, "get column[%s] with error type %s ", \
  column, print_cellinfo(cell_info)); \
  } \
  } \
  else \
{ \
  ret = OB_ERROR;\
  YYSYS_LOG(WARN, "get column[%s] with error null cell.", column); \
  } \
  }


#define ASSIGN_INT(column, field, type) \
  if(OB_SUCCESS == ret) \
{ \
  ObCellInfo * cell_info = NULL; \
  int64_t int_value = 0; \
  cell_info = table_row->get_cell_info(column); \
  if(NULL != cell_info && cell_info->value_.get_type() == ObIntType) \
{ \
  cell_info->value_.get_int(int_value); \
  field = static_cast<type>(int_value); \
  YYSYS_LOG(DEBUG, "get cell info:column[%s], value[%ld]", column, int_value); \
  } \
  else if (NULL != cell_info && cell_info->value_.get_type() == ObNullType) \
{ \
  field = static_cast<type>(0); \
  YYSYS_LOG(WARN, "get cell value null:column[%s]", column); \
  } \
  else \
{ \
  ret = OB_ERROR; \
  YYSYS_LOG(WARN, "get column[%s] with error cell info %s ", \
  column, NULL == cell_info ? "nil": print_cellinfo(cell_info)); \
  } \
  }

#define ASSIGN_CREATE_TIME(column, field, type) \
  if(OB_SUCCESS == ret) \
{ \
  ObCellInfo * cell_info = NULL; \
  ObCreateTime value = false; \
  cell_info = table_row->get_cell_info(column); \
  if(NULL != cell_info) \
{ \
  cell_info->value_.get_createtime(value); \
  field = static_cast<type>(value); \
  YYSYS_LOG(DEBUG, "get cell info:column[%s], value[%ld]", column, value); \
  } \
  else \
{ \
  ret = OB_ERROR; \
  YYSYS_LOG(WARN, "get cell info:column[%s]", column); \
  } \
  }
#define ASSIGN_MODIFY_TIME(column, field, type) \
  if(OB_SUCCESS == ret) \
{ \
  ObCellInfo * cell_info = NULL; \
  ObModifyTime value = false; \
  cell_info = table_row->get_cell_info(column); \
  if(NULL != cell_info) \
{ \
  cell_info->value_.get_modifytime(value); \
  field = static_cast<type>(value); \
  YYSYS_LOG(DEBUG, "get cell info:column[%s], value[%ld]", column, value); \
  } \
  else \
{ \
  ret = OB_ERROR; \
  YYSYS_LOG(WARN, "get cell info:column[%s]", column); \
  } \
  }



int ObSchemaServiceImpl::drop_table(const ObString &table_name, uint64_t new_table_id)
{
  int ret = OB_SUCCESS;

  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "check inner stat fail");
  }


  uint64_t table_id = 0;

  ret = get_table_id(table_name, table_id);
  if(OB_SUCCESS != ret)
  {
    YYSYS_LOG(WARN, "get table id fail:table_name[%.*s]", table_name.length(), table_name.ptr());
  }

  // called after get_table_id() to prevent dead lock
  yysys::CThreadGuard guard(&mutex_);

  ObRowkey rowkey;
  //delete dolphin [database manager]@20150617:b//[conflict_level B]	�߼��޸�
  /*ObObj table_name_obj;
  table_name_obj.set_varchar(table_name);
  rowkey.assign(&table_name_obj, 1);*/
  //delete:e
  //add dolphin [database manager]@20150616:b
  ObObj table_name_obj[2];
  char dn[OB_MAX_DATBASE_NAME_LENGTH];
  char tn[OB_MAX_TABLE_NAME_LENGTH];
  ObString dname;
  ObString tname;
  dname.assign_buffer(dn,OB_MAX_DATBASE_NAME_LENGTH);
  tname.assign_buffer(tn,OB_MAX_TABLE_NAME_LENGTH);
  table_name.split_two(dname,tname);
  table_name_obj[0].set_varchar(tname);
  table_name_obj[1].set_varchar(dname);
  rowkey.assign(table_name_obj,2);
  //add:e
  if(OB_SUCCESS == ret)
  {
    ret = nb_accessor_.delete_row(FIRST_TABLET_TABLE_NAME, rowkey);
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "delete rwo from first tablet table fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = nb_accessor_.delete_row(OB_ALL_COLUMN_TABLE_NAME, SC("table_id")("column_name"),
                                  ScanConds("table_id", EQ, table_id));
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "delete row from first tablet table fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = nb_accessor_.delete_row(OB_ALL_JOININFO_TABLE_NAME, SC("left_table_id")("left_column_id")("right_table_id")("right_column_id"),
                                  ScanConds("left_table_id", EQ, table_id));
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "delete row from first tablet table fail:ret[%d]", ret);
    }
  }
  if(OB_SUCCESS == ret)
  {
    ret = nb_accessor_.delete_row(OB_ALL_JOININFO_TABLE_NAME, SC("left_table_id")("left_column_id")("right_table_id")("right_column_id"),
                                  ScanConds("right_table_id", EQ, table_id));
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "delete row from first tablet table fail:ret[%d]", ret);
    }
  }

  //[view]
  if(OB_SUCCESS == ret)
  {
      ObRowkey all_view_row_key;
      ObObj obj;
      obj.set_int(table_id);
      all_view_row_key.assign(&obj, 1);
      ret = nb_accessor_.delete_row(OB_ALL_VIEW_TABLE_NAME, all_view_row_key);
      if(OB_SUCCESS != ret)
      {
          YYSYS_LOG(WARN, "delete row from __all_view fail:ret[%d]", ret);
      }
  }

  //fix bug
  //del privilege info
  if (OB_SUCCESS == ret)
  {
    //mod liumz, [bugfix_drop_table]20150721:b
    //ret = nb_accessor_.delete_row(OB_ALL_TABLE_PRIVILEGE_TABLE_NAME, SC("user_id")("table_id"), ScanConds("table_id", EQ, table_id));
    ret = nb_accessor_.delete_row(OB_ALL_TABLE_PRIVILEGE_TABLE_NAME, SC("user_id")("db_id")("table_id"), ScanConds("table_id", EQ, table_id));
    //mod:e
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "fail to delete privilege info for table=%lu, ret=%d", table_id, ret);
    }
  }
  //add liu jun.[MultiUPS] [sql_api] 20150424:b
  if(new_table_id != OB_INVALID_ID)
  {
    ret = nb_accessor_.delete_row(OB_ALL_TABLE_RULES_NAME, SC("table_id")("start_version"), ScanConds("table_id", EQ, new_table_id));
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "fail to delete all table rules for table=%lu, ret=%d", table_id, ret);
    }
  }
  else
  {
    if (OB_SUCCESS == ret)
    {
      ret = nb_accessor_.delete_row(OB_ALL_TABLE_RULES_NAME, SC("table_id")("start_version"), ScanConds("table_id", EQ, table_id));
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "fail to delete all table rules for table=%lu, ret=%d", table_id, ret);
      }
    }
  }
  //add 20150424:e
  //add wuna.[MultiUPS] [sql_api] 20160413:b
  //to delete partition function in __all_partition_rules when drop list or range table.
  if (OB_SUCCESS == ret)
  {
    ObRowkey part_rule_list_rowkey;
    ObRowkey part_rule_range_rowkey;
    ObObj list_rule_name_obj;
    ObObj range_rule_name_obj;
    char list_rule_name[OB_MAX_VARCHAR_LENGTH]={0};
    char range_rule_name[OB_MAX_VARCHAR_LENGTH]={0};
    char table_id_str[OB_MAX_VARCHAR_LENGTH]={0};
    sprintf(table_id_str,"%lu",table_id);
    strcpy(list_rule_name,list_pre_name_str);
    strcat(list_rule_name,table_id_str);
    strcpy(range_rule_name,range_pre_name_str);
    strcat(range_rule_name,table_id_str);
    ObString list_rule_name_str = ObString::make_string(list_rule_name);
    ObString range_rule_name_str = ObString::make_string(range_rule_name);
    list_rule_name_obj.set_varchar(list_rule_name_str);
    range_rule_name_obj.set_varchar(range_rule_name_str);
    part_rule_list_rowkey.assign(&list_rule_name_obj, 1);
    part_rule_range_rowkey.assign(&range_rule_name_obj, 1);
    ret = nb_accessor_.delete_row(OB_ALL_PARTITION_RULES_NAME,part_rule_list_rowkey);
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "fail to delete all_partition_rules for table=%lu, ret=%d", table_id, ret);
    }
    else
    {
      ret = nb_accessor_.delete_row(OB_ALL_PARTITION_RULES_NAME,part_rule_range_rowkey);
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "fail to delete all_partition_rules for table=%lu, ret=%d", table_id, ret);
      }
    }
  }
  //add 20160413:e

  //add zhaoqiong [Schema Manager] 20150327:b
  ObMutator* mutator = NULL;

  if( OB_SUCCESS == ret)
  {
    mutator = GET_TSI_MULT(ObMutator, TSI_COMMON_MUTATOR_1);
    if(NULL == mutator)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      YYSYS_LOG(WARN, "get thread specific Mutator fail");
    }
  }

  if( OB_SUCCESS == ret)
  {
    ret = mutator->reset();
    if( OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "reset ob mutator fail:ret[%d]", ret);
    }
  }

  if( OB_SUCCESS == ret)
  {
    //append a DROP_TABLE operation record to __all_ddl_operation
    ret = add_ddl_operation(mutator, table_id, DROP_TABLE);
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "add ddl operation to mutator fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    ret = client_proxy_->mutate(*mutator);
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "apply mutator fail:ret[%d]", ret);
    }
  }
  //add:e
  //fix bug
  //delete privilege info
  if (OB_SUCCESS == ret)
  {
    //mod liumz, [bugfix_drop_table]20150721:b
    //ret = nb_accessor_.delete_row(OB_ALL_TABLE_PRIVILEGE_TABLE_NAME, SC("user_id")("table_id"), ScanConds("table_id", EQ, table_id));
    ret = nb_accessor_.delete_row(OB_ALL_TABLE_PRIVILEGE_TABLE_NAME, SC("user_id")("db_id")("table_id"), ScanConds("table_id", EQ, table_id));
    //mod:e
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "fail to delete privilege info for table=%lu, ret=%d", table_id, ret);
    }
  }

  int err = 0;
  if(OB_SUCCESS == ret)
  {
    err = id_name_map_.erase(table_id);
    if(hash::HASH_EXIST != err)
    {
      ret = hash::HASH_NOT_EXIST == err ? OB_ENTRY_NOT_EXIST : OB_SUCCESS;
      YYSYS_LOG(WARN, "id name map erase fail:err[%d], table_id[%lu]", err, table_id);
    }
  }

  return ret;
}

int ObSchemaServiceImpl::init_id_name_map()
{
  int ret = OB_SUCCESS;
  ObTableIdNameIterator iterator;
  if(OB_SUCCESS == ret)
  {
    ret = iterator.init(client_proxy_, only_core_tables_);
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "init iterator fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = init_id_name_map(iterator)))
    {
      YYSYS_LOG(WARN, "failed init id_name_map, err=%d", ret);
    }
  }

  iterator.destroy();
  return ret;
}

int ObSchemaServiceImpl::init_id_name_map(ObTableIdNameIterator& iterator)
{
  int ret = OB_SUCCESS;
  ObTableIdName * table_id_name = NULL;
  ObString tmp_str;
  //add dolphin [database manager]@20150613
  ObString dt;
  char buf[OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1] = {0};
  dt.assign_buffer(buf,OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1);
  //add:e
  while(OB_SUCCESS == ret && OB_SUCCESS == (ret = iterator.next()))
  {
    ret = iterator.get(&table_id_name);
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "get table id name fail:ret[%d]", ret);
    }

    if(OB_SUCCESS == ret)
    {
      //add dolphin [database manager]@20150613
      dt.concat(table_id_name->dbname_,table_id_name->table_name_);
      yysys::CThreadGuard buf_guard(&string_buf_write_mutex_);
      //modify dolphin [databae manager]@20150613:b
      //ret = string_buf_.write_string(table_id_name->table_name_, &tmp_str);
      ret = string_buf_.write_string(dt,&tmp_str);
      //modify:e
      if(OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "write string to string buf fail:ret[%d]", ret);
      }
    }

    int err = 0;
    if(OB_SUCCESS == ret)
    {
      err = id_name_map_.set(table_id_name->table_id_, tmp_str);
      if(hash::HASH_INSERT_SUCC != err)
      {
        ret = hash::HASH_EXIST == err ? OB_ENTRY_EXIST : OB_ERROR;
        YYSYS_LOG(WARN, "id name map set fail:err[%d], table_id[%lu]", err, table_id_name->table_id_);
      }
      else
      {
        YYSYS_LOG(DEBUG, "add id_name_map, tname=%.*s tid=%lu",
                  tmp_str.length(), tmp_str.ptr(), table_id_name->table_id_);
      }
    }
  }

  if(OB_ITER_END == ret)
  {
    ret = OB_SUCCESS;
  }

  return ret;
}


int ObSchemaServiceImpl::get_table_name(uint64_t table_id, ObString& table_name)
{
  int ret = OB_SUCCESS;
  YYSYS_LOG(DEBUG, "table_id = %lu", table_id);
  if (OB_FIRST_TABLET_ENTRY_TID ==  table_id)
  {
    table_name = first_tablet_entry_name;
  }
  else if (OB_ALL_ALL_COLUMN_TID == table_id)
  {
    table_name = column_table_name;
  }
  else if (OB_ALL_JOIN_INFO_TID == table_id)
  {
    table_name = joininfo_table_name;
  }
  //add zhaoqiong [Schema Manager] 20150327:b
  else if (OB_DDL_OPERATION_TID == table_id)
  {
    table_name = ddl_operation_name;
  }
  //add:e
  /*delete wenghaixing [secondary index] 20141104
  *��??oD���??��?����?��?��??��
  else if(OB_INDEX_PROCESS_TID==table_id)
  {
    table_name=index_process_name;
  }
  add e*/

  //[view]
  else if (OB_ALL_VIEW_TID == table_id)
  {
      table_name = all_view_name;
  }

  else if (!check_inner_stat())
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "check inner stat fail");
  }
  else
  {
    int err = id_name_map_.get(table_id, table_name);
    if(hash::HASH_EXIST != err)
    {
      ret = hash::HASH_NOT_EXIST == err ? OB_ENTRY_NOT_EXIST : OB_ERROR;
      YYSYS_LOG(WARN, "id name map get fail:err[%d], table_id[%lu]", err, table_id);
    }
  }
  YYSYS_LOG(DEBUG, "get table_name=%.*s", table_name.length(), table_name.ptr());
  return ret;
}
//add zhaoqiong [Schema Manager] 20150327:b
int64_t ObSchemaServiceImpl::get_schema_version() const
{
  return schema_timestamp_;
}
ObScanHelper* ObSchemaServiceImpl::get_client_proxy()const
{
  return client_proxy_;
}

int ObSchemaServiceImpl::set_schema_version(int64_t timestamp)
{
  int ret = OB_SUCCESS;
  //refresh schema maybe retry, when retry, schema_timestamp_ equel timestamp
  if (schema_timestamp_ > timestamp)
  {
    ret = OB_INNER_STAT_ERROR;
    YYSYS_LOG(ERROR, "set timestamp error");
  }
  else
  {
    schema_timestamp_ = timestamp;
  }
  return ret;
}

//add:e

int ObSchemaServiceImpl::get_table_id(const ObString& table_name, uint64_t& table_id)
{
  int ret = OB_SUCCESS;
  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "check inner stat fail");
  }
  //add dolphin [database manager]@20150616:b
  char dn[OB_MAX_DATBASE_NAME_LENGTH];
  char tn[OB_MAX_TABLE_NAME_LENGTH];
  ObString dname;
  ObString tname;
  dname.assign_buffer(dn,OB_MAX_DATBASE_NAME_LENGTH);
  tname.assign_buffer(tn,OB_MAX_TABLE_NAME_LENGTH);
  table_name.split_two(dname,tname);
  ObObj table_name_obj[2];
  //add:e
  QueryRes* res = NULL;
  TableRow* table_row = NULL;

  ObRowkey rowkey;
  //delete dolphin [database manager]@20150617:b
  //ObObj table_name_obj;
  //table_name_obj.set_varchar(table_name);   v
  // rowkey.assign(&table_name_obj, 1);
  //delete:e
  table_name_obj[0].set_varchar(tname);
  table_name_obj[1].set_varchar(dname);
  rowkey.assign(table_name_obj,2);
  if(OB_SUCCESS == ret)
  {
    ret = nb_accessor_.get(res, FIRST_TABLET_TABLE_NAME, rowkey, SC("table_id"));

    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "get table schema fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    table_row = res->get_only_one_row();
    if(NULL != table_row)
    {
      ASSIGN_INT("table_id", table_id, uint64_t);
    }
    else
    {
      ret = OB_ENTRY_NOT_EXIST;
      YYSYS_LOG(DEBUG, "get table row fail:table_name[%.*s]", table_name.length(), table_name.ptr());
    }
  }

  nb_accessor_.release_query_res(res);
  res = NULL;

  return ret;
}

int ObSchemaServiceImpl::assemble_table(const TableRow* table_row, TableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  //add liu jun. fix rename bug. turn OB_MAX_COLUMN_NAME_LENGTH to OB_MAX_TABLE_NAME_LENGTH.20150519
  ASSIGN_VARCHAR("table_name", table_schema.table_name_, OB_MAX_TABLE_NAME_LENGTH);
  //add dolphin [database manager]@20150611
  ASSIGN_VARCHAR("db_name", table_schema.dbname_, OB_MAX_DATBASE_NAME_LENGTH);

  /* !! OBSOLETE CODE !! no need extract rowkey field when updateserver supports ROWKEY column query.
  if (table_schema.table_name_[0] == '\0' || OB_SUCCESS != ret)
  {
    ret = OB_SUCCESS;
    ASSIGN_VARCHAR_FROM_ROWKEY("table_name", 0, table_schema.table_name_, OB_MAX_COLUMN_NAME_LENGTH);
    YYSYS_LOG(WARN, "assemble_table table_name=%s", table_schema.table_name_);
  }
  */

  ASSIGN_INT("table_id", table_schema.table_id_, uint64_t);
  ASSIGN_INT("table_type", table_schema.table_type_, TableSchema::TableType);
  ASSIGN_INT("load_type", table_schema.load_type_, TableSchema::LoadType);
  ASSIGN_INT("table_def_type", table_schema.table_def_type_, TableSchema::TableDefType);
  ASSIGN_INT("rowkey_column_num", table_schema.rowkey_column_num_, int32_t);
  ASSIGN_INT("replica_num", table_schema.replica_num_, int32_t);
  ASSIGN_INT("max_used_column_id", table_schema.max_used_column_id_, int64_t);
  ASSIGN_INT("create_mem_version", table_schema.create_mem_version_, int64_t);
  ASSIGN_INT("tablet_max_size", table_schema.tablet_max_size_, int64_t);
  ASSIGN_INT("tablet_block_size", table_schema.tablet_block_size_, int64_t);
  if (OB_SUCCESS == ret && table_schema.tablet_block_size_ <= 0)
  {
    YYSYS_LOG(WARN, "set tablet sstable block size to default value:read[%ld]", table_schema.tablet_block_size_);
    table_schema.tablet_block_size_ = OB_DEFAULT_SSTABLE_BLOCK_SIZE;
  }
  ASSIGN_INT("max_rowkey_length", table_schema.max_rowkey_length_, int64_t);
  ASSIGN_INT("merge_write_sstable_version", table_schema.merge_write_sstable_version_, int64_t);
  ASSIGN_INT("schema_version", table_schema.schema_version_, int64_t);
  ASSIGN_VARCHAR("compress_func_name", table_schema.compress_func_name_, OB_MAX_COLUMN_NAME_LENGTH);
  ASSIGN_VARCHAR("expire_condition", table_schema.expire_condition_, OB_MAX_EXPIRE_CONDITION_LENGTH);
  //ASSIGN_VARCHAR("comment_str", table_schema.comment_str_, OB_MAX_TABLE_COMMENT_LENGTH); //[conflict_level B]	�߼��޸�
  ASSIGN_INT("is_use_bloomfilter", table_schema.is_use_bloomfilter_, int64_t);
  ASSIGN_INT("is_pure_update_table", table_schema.is_pure_update_table_, int64_t);
  ASSIGN_INT("is_read_static", table_schema.consistency_level_, int64_t);
  //ASSIGN_INT("consistency_level", table_schema.consistency_level_, int64_t);
  ASSIGN_INT("rowkey_split", table_schema.rowkey_split_, int64_t);
  ASSIGN_INT("create_time_column_id", table_schema.create_time_column_id_, uint64_t);
  ASSIGN_INT("modify_time_column_id", table_schema.modify_time_column_id_, uint64_t);
  //add wenghaixing [secondary index ]20141105
  ASSIGN_INT("data_table_id", table_schema.ih_.tbl_tid, int64_t);
  int64_t st = 2;//status
  ASSIGN_INT("index_status", st, int64_t);
  if(0 == st)
  {
    table_schema.ih_.status = NOT_AVALIBALE;
  }
  else if(1 == st)
  {
    table_schema.ih_.status = AVALIBALE;
  }
  //add wenghaixing [secondary index col checksum] 20141217
  else if(3 == st)
  {
    table_schema.ih_.status = WRITE_ONLY;
  }
  //add e
  else if(2 == st)
  {
    table_schema.ih_.status = ERROR;
  }
  else
  {
    table_schema.ih_.status = INDEX_INIT;
  }
  //add e
  YYSYS_LOG(DEBUG, "table schema version is %ld, maxcolid=%lu, compress_fuction=%s, expire_condition=%s",
            table_schema.schema_version_, table_schema.max_used_column_id_, table_schema.compress_func_name_, table_schema.expire_condition_);
  return ret;
}

int ObSchemaServiceImpl::assemble_column(const TableRow* table_row, ColumnSchema& column)
{
  int ret = OB_SUCCESS;

  ASSIGN_VARCHAR("column_name", column.column_name_, OB_MAX_COLUMN_NAME_LENGTH);
  /* !! OBSOLETE CODE !! no need extract rowkey field when updateserver supports ROWKEY column query.
  if (column.column_name_[0] == '\0' || OB_SUCCESS != ret)
  {
    ret = OB_SUCCESS;
    // __all_all_column rowkey (table_id,column_name);
    ASSIGN_VARCHAR_FROM_ROWKEY("column_name", 1, column.column_name_, OB_MAX_COLUMN_NAME_LENGTH);
    YYSYS_LOG(WARN, "assemble_column column_name_=%s", column.column_name_);
  }
  */

  ASSIGN_INT("column_id", column.column_id_, uint64_t);
  ASSIGN_INT("column_group_id", column.column_group_id_, uint64_t);
  ASSIGN_INT("rowkey_id", column.rowkey_id_, int64_t);
  ASSIGN_INT("join_table_id", column.join_table_id_, uint64_t);
  ASSIGN_INT("join_column_id", column.join_column_id_, uint64_t);
  ASSIGN_INT("data_type", column.data_type_, ColumnType);
  ASSIGN_INT("data_length", column.data_length_, int64_t);
  ASSIGN_INT("data_precision", column.data_precision_, int64_t);
  ASSIGN_INT("data_scale", column.data_scale_, int64_t);
  ASSIGN_INT("nullable", column.nullable_, int64_t);
  ASSIGN_VARCHAR_IS_NULL("column_default", column.default_value_, OB_MAX_DEBUG_MSG_LEN, column.default_value_is_null);
  ASSIGN_INT("length_in_rowkey", column.length_in_rowkey_, int64_t);
  ASSIGN_INT("order_in_rowkey", column.order_in_rowkey_, int32_t);
  ASSIGN_CREATE_TIME("gm_create", column.gm_create_, ObCreateTime);
  ASSIGN_MODIFY_TIME("gm_modify", column.gm_modify_, ObModifyTime);

  return ret;
}

int ObSchemaServiceImpl::assemble_join_info(const TableRow* table_row, JoinInfo& join_info)
{
  int ret = OB_SUCCESS;
  ASSIGN_VARCHAR("left_table_name", join_info.left_table_name_, OB_MAX_TABLE_NAME_LENGTH);
  ASSIGN_INT("left_table_id", join_info.left_table_id_, uint64_t);
  //ASSIGN_INT_FROM_ROWKEY("left_table_id", 0, join_info.left_table_id_, uint64_t);
  ASSIGN_VARCHAR("left_column_name", join_info.left_column_name_, OB_MAX_COLUMN_NAME_LENGTH);
  ASSIGN_INT("left_column_id", join_info.left_column_id_, uint64_t);
  //ASSIGN_INT_FROM_ROWKEY("left_column_id", 1, join_info.left_column_id_, uint64_t);
  ASSIGN_VARCHAR("right_table_name", join_info.right_table_name_, OB_MAX_TABLE_NAME_LENGTH);
  ASSIGN_INT("right_table_id", join_info.right_table_id_, uint64_t);
  //ASSIGN_INT_FROM_ROWKEY("right_table_id", 2, join_info.right_table_id_, uint64_t);
  ASSIGN_VARCHAR("right_column_name", join_info.right_column_name_, OB_MAX_COLUMN_NAME_LENGTH);
  ASSIGN_INT("right_column_id", join_info.right_column_id_, uint64_t);
  //ASSIGN_INT_FROM_ROWKEY("right_column_id", 3, join_info.right_column_id_, uint64_t);

  return ret;
}
//add wenghaixing [secondary index] 20141104
//?a???��?����?��??��
int ObSchemaServiceImpl::assemble_index_process(const TableRow *table_row, IndexHelper &ih,uint64_t& IndexList)
{
  int ret=OB_SUCCESS;
  char status[OB_MAX_COLUMN_NAME_LENGTH];
  memset(status,0,OB_MAX_COLUMN_NAME_LENGTH);
  ASSIGN_INT("index_id", IndexList, int64_t);
  ASSIGN_INT("table_id", ih.tbl_tid, int64_t);
  ASSIGN_VARCHAR("index_stat",status,OB_MAX_COLUMN_NAME_LENGTH);
  if(strcmp(status,"AVALIBALE"))
  {
    ih.status=AVALIBALE;
  }
  else if(strcmp(status,"NOT_AVALIBALE"))
  {
    ih.status=NOT_AVALIBALE;
  }
  else
  {
    ih.status=ERROR;
  }
  return ret;
}
//add e

//[view]
int ObSchemaServiceImpl::assemble_view(const TableRow *table_row, TableSchema &table_schema)
{
    int ret = OB_SUCCESS;
    ASSIGN_VARCHAR("view_definition",table_schema.text_,OB_MAX_SQL_LENGTH);
    ASSIGN_INT("check_option", table_schema.with_check_option_, TableSchema::WithCheckOption);
    ASSIGN_INT("is_updatable", table_schema.is_updatable_, bool);
    return ret;
}

int ObSchemaServiceImpl::get_table_schema(const ObString& table_name, TableSchema& table_schema,const ObString& dbname)
{
  int ret = OB_SUCCESS;
  table_schema.clear();
  if (table_name == first_tablet_entry_name)
  {
    ret = ObExtraTablesSchema::first_tablet_entry_schema(table_schema);
  }
  else if (table_name == column_table_name)
  {
    ret = ObExtraTablesSchema::all_all_column_schema(table_schema);
  }
  else if (table_name == joininfo_table_name)
  {
    ret = ObExtraTablesSchema::all_join_info_schema(table_schema);
  }
  //add zhaoqiong [Schema Manager] 20150327:b
  else if (table_name == ddl_operation_name)
  {
    ret = ObExtraTablesSchema::all_ddl_operation(table_schema);
  }
  //add:e
  //add wenghaixing 20141029
  else if(table_name==index_process_name)
  {
    ret = ObExtraTablesSchema::all_index_process_schema(table_schema);
  }
  //add e

  //[view]
  else if (table_name == all_view_name)
  {
      ret = ObExtraTablesSchema::all_view_schema(table_schema);
  }

  else
  {
    if(!check_inner_stat())
    {
      ret = OB_ERROR;
      YYSYS_LOG(WARN, "check inner stat fail");
    }
    else
    {
      ret = fetch_table_schema(table_name, table_schema,dbname);
    }
  }

  if(OB_SUCCESS != ret)
  {
    YYSYS_LOG(WARN, "get table schema fail:ret[%d]", ret);
  }

  return ret;
}

int ObSchemaServiceImpl::fetch_table_schema(const ObString& table_name, TableSchema& table_schema,const ObString& dbname)
{
  int ret = OB_SUCCESS;
  YYSYS_LOG(TRACE, "fetch_table_schema begin: table_name=%.*s,", table_name.length(), table_name.ptr());

  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "check inner stat fail");
  }

  QueryRes* res = NULL;

  ObRowkey rowkey;
  //delete dolphin [database manager]@20150610
  /**
  ObObj table_name_obj;  //[conflict_level B]	�߼��޸�
  table_name_obj.set_varchar(table_name);
  rowkey.assign(&table_name_obj, 1);
  */
  //delete:e
  ObObj obj[2];
  if(table_name.length() < 1)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(WARN,"fetch schema param[table_name] cannot be empty");

  }

  //[635]
  char bypass_dn[OB_MAX_DATBASE_NAME_LENGTH];
  char bypass_tn[OB_MAX_TABLE_NAME_LENGTH];
  ObString bypass_db_name, bypass_tbname;
  bypass_db_name.assign_buffer(bypass_dn, OB_MAX_DATBASE_NAME_LENGTH);
  bypass_tbname.assign_buffer(bypass_tn, OB_MAX_TABLE_NAME_LENGTH);

  if(table_name.find('.'))
  {

    table_name.split_two(bypass_db_name, bypass_tbname);
    obj[0].set_varchar(bypass_tbname);
    obj[1].set_varchar(bypass_db_name);
  }
  else
  {
    obj[0].set_varchar(table_name);
    obj[1].set_varchar(dbname);
  }
  rowkey.assign(obj,2);
  TableRow* table_row = NULL;
  if(OB_SUCCESS == ret)
  {
    //add dolphin [database manager]@20150611
    //add @param db_name
    ret = nb_accessor_.get(res, FIRST_TABLET_TABLE_NAME, rowkey, SC("table_name")("db_name")("table_id")
                           ("table_type")("load_type")("table_def_type")("rowkey_column_num")("replica_num")
                           ("max_used_column_id")("create_mem_version")("tablet_max_size")("tablet_block_size")
                           ("max_rowkey_length")("compress_func_name")("expire_condition")("is_use_bloomfilter")
                           ("is_read_static")("merge_write_sstable_version")("schema_version")("is_pure_update_table")("rowkey_split")
                           /*
                                                      * modify wenghaixing [secondary index]20141105
                                                      *("create_time_column_id")("modify_time_column_id")); old code    //[conflict_level B]	�߼��޸�
                                                      */
                           ("create_time_column_id")("modify_time_column_id")("data_table_id")("index_status"));//("comment_str"));
    //modify e
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "get table schema fail:ret[%d]", ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    table_row = res->get_only_one_row();
    if(NULL != table_row)
    {
      ret = assemble_table(table_row, table_schema);
      if(OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "assemble table fail:ret[%d]", ret);
      }
    }
    else
    {
      ret = OB_ERROR;
      YYSYS_LOG(WARN, "get table row fail:table_name[%.*s]", table_name.length(), table_name.ptr());
    }
  }
  nb_accessor_.release_query_res(res);
  res = NULL;

  //[view]
  if (ret == OB_SUCCESS && table_schema.table_type_ == TableSchema::VIEW)
  {
      ObRowkey all_view_rowkey;
      ObObj obj;
      obj.set_int(table_schema.table_id_);
      all_view_rowkey.assign(&obj, 1);
      TableRow *text_row = NULL;
      ret = nb_accessor_.get(res, OB_ALL_VIEW_TABLE_NAME, all_view_rowkey, SC("view_definition")("check_option")("is_updatable"));
      if(ret != OB_SUCCESS)
      {
          YYSYS_LOG(WARN, "get table schema fail:ret[%d]", ret);
      }
      else
      {
          if(NULL == (text_row = res->get_only_one_row()))
          {
              ret = OB_ERROR;
              YYSYS_LOG(WARN, "get table row fail:table_name[%.*s]", table_name.length(), table_name.ptr());
          }
          else if (OB_SUCCESS != (ret = assemble_view(text_row, table_schema)))
          {
              YYSYS_LOG(WARN, "assemble table fail:ret[%d]", ret);
          }
      }
      nb_accessor_.release_query_res(res);
      res = NULL;
  }

  ObNewRange range;
  int32_t rowkey_column = 2;
  ObObj start_rowkey[rowkey_column];
  ObObj end_rowkey[rowkey_column];
  start_rowkey[0].set_int(table_schema.table_id_);
  start_rowkey[1].set_min_value();
  end_rowkey[0].set_int(table_schema.table_id_);
  end_rowkey[1].set_max_value();
  if (OB_SUCCESS == ret)
  {
    range.start_key_.assign(start_rowkey, rowkey_column);
    range.end_key_.assign(end_rowkey, rowkey_column);
  }
  if(OB_SUCCESS == ret)
  {
    ret = nb_accessor_.scan(res, OB_ALL_COLUMN_TABLE_NAME, range,
                            SC("column_name")("column_id")("gm_create")("gm_modify")("column_group_id")("rowkey_id")
                            ("join_table_id")("join_column_id")("data_type")("data_length")("data_precision")
                            ("data_scale")("nullable")("column_default")("length_in_rowkey")("order_in_rowkey"));
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "scan column table fail:ret[%d]", ret);
    }
  }

  ColumnSchema column;

  if(OB_SUCCESS == ret)
  {
    int i = 0;
    while(OB_SUCCESS == res->next_row() && OB_SUCCESS == ret)
    {
      res->get_row(&table_row);
      if(NULL != table_row)
      {
        i ++;
        ret = assemble_column(table_row, column);
        if(OB_SUCCESS != ret)
        {
          YYSYS_LOG(WARN, "assemble column fail:ret[%d]", ret);
        }

        if(OB_SUCCESS == ret)
        {
          ret = table_schema.add_column(column);
          if(OB_SUCCESS != ret)
          {
            YYSYS_LOG(WARN, "add column to table schema fail:ret[%d]", ret);
          }
        }
      }
      else
      {
        ret = OB_ERROR;
        YYSYS_LOG(WARN, "get column fail");
      }
    }
  }

  nb_accessor_.release_query_res(res);
  res = NULL;

  ObNewRange join_range;
  int32_t rowkey_column_num = 4;
  ObObj start_obj[rowkey_column_num];
  ObObj end_obj[rowkey_column_num];
  start_obj[0].set_int(table_schema.table_id_);
  end_obj[0].set_int(table_schema.table_id_);
  for (int32_t i = 1; i < rowkey_column_num; i++)
  {
    start_obj[i].set_min_value();
    end_obj[i].set_max_value();
  }
  join_range.start_key_.assign(start_obj, rowkey_column_num);
  join_range.end_key_.assign(end_obj, rowkey_column_num);
  if(OB_SUCCESS == ret)
  {
    ret = nb_accessor_.scan(res, OB_ALL_JOININFO_TABLE_NAME, join_range,
                            SC("left_table_id")("left_column_id")("right_table_id")("right_column_id")
                            ("left_table_name")("left_column_name")("right_table_name")("right_column_name"));
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "scan join info table fail:ret[%d]", ret);
    }
  }

  JoinInfo join_info;

  if(OB_SUCCESS == ret)
  {
    while(OB_SUCCESS == res->next_row() && OB_SUCCESS == ret)
    {
      res->get_row(&table_row);
      if(NULL != table_row)
      {
        ret = assemble_join_info(table_row, join_info);
        if(OB_SUCCESS != ret)
        {
          YYSYS_LOG(WARN, "assemble join info fail:ret[%d]", ret);
        }

        if(OB_SUCCESS == ret)
        {
          ret = table_schema.add_join_info(join_info);
          if(OB_SUCCESS != ret)
          {
            YYSYS_LOG(WARN, "add join info to table schema fail:ret[%d]", ret);
          }
        }
      }
      else
      {
        ret = OB_ERROR;
        YYSYS_LOG(WARN, "get join info fail");
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (!table_schema.is_valid())
    {
      //mod zhaoqiong [Schema Manager] 20150327:b
      //ret = OB_ERR_UNEXPECTED;
      ret = OB_ERR_INVALID_SCHEMA;
      //mod:e
      YYSYS_LOG(ERROR, "check the table schema is invalid:table_name[%s]", table_schema.table_name_);
    }
  }

  nb_accessor_.release_query_res(res);
  res = NULL;

  return ret;
}

int ObSchemaServiceImpl::set_max_used_table_id(const uint64_t max_used_tid)
{
  int ret = OB_SUCCESS;
  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "check inner stat fail");
  }
  else
  {
    ObObj rowkey_objs[2];
    rowkey_objs[0].set_int(0); // cluster_id
    rowkey_objs[1].set_varchar(ObString::make_string("yb_max_used_table_id")); // name
    ObRowkey rowkey;
    rowkey.assign(rowkey_objs, 2);
    ObString value;
    char buf[64] = "";
    snprintf(buf, sizeof(buf), "%lu", max_used_tid);
    value.assign(buf, static_cast<int32_t>(strlen(buf)));
    KV new_value("value", value);
    /// TODO should using add 1 operator
    if (OB_SUCCESS != (ret = nb_accessor_.update(OB_ALL_SYS_STAT_TABLE_NAME, rowkey, new_value)))
    {
      YYSYS_LOG(WARN, "failed to update the row, err=%d", ret);
    }
  }
  return ret;
}

int ObSchemaServiceImpl::get_max_used_table_id(uint64_t &max_used_tid)
{
  int ret = OB_SUCCESS;
  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "check inner stat fail");
  }
  else
  {
    ObObj rowkey_objs[2];
    rowkey_objs[0].set_int(0); // cluster_id
    rowkey_objs[1].set_varchar(ObString::make_string("yb_max_used_table_id")); // name
    ObRowkey rowkey;
    rowkey.assign(rowkey_objs, 2);
    QueryRes* res = NULL;
    if (OB_SUCCESS != (ret = nb_accessor_.get(res, OB_ALL_SYS_STAT_TABLE_NAME, rowkey, SC("value"))))
    {
      YYSYS_LOG(WARN, "failed to access row, err=%d", ret);
    }
    else
    {
      TableRow* table_row = res->get_only_one_row();
      if (NULL == table_row)
      {
        YYSYS_LOG(WARN, "failed to get row from query results");
        ret = OB_ERR_UNEXPECTED;
      }
      else
      {
        char value_buf[TEMP_VALUE_BUFFER_LEN] = "";
        ASSIGN_VARCHAR("value", value_buf, TEMP_VALUE_BUFFER_LEN);
        max_used_tid = strtoul(value_buf, NULL, 10);
        YYSYS_LOG(TRACE, "get max used id succ:id[%lu]", max_used_tid);
      }
      //      nb_accessor_.release_query_res(res);
      //      res = NULL;
    }
    nb_accessor_.release_query_res(res);
    res = NULL;
  }
  return ret;
}

//add liumz, [secondary index static_index_build] 20150629:b
int ObSchemaServiceImpl::get_index_stat(const uint64_t table_id, const int64_t cluster_count, IndexStatus &stat)
{
  int ret = OB_SUCCESS;
  stat = INDEX_INIT;
  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "check inner stat fail");
  }
  else
  {
    QueryRes* res = NULL;
    ObNewRange range;
    int32_t rowkey_column = 2;
    ObObj start_rowkey[rowkey_column];
    ObObj end_rowkey[rowkey_column];
    start_rowkey[0].set_int(table_id);
    start_rowkey[1].set_min_value();
    end_rowkey[0].set_int(table_id);
    end_rowkey[1].set_max_value();
    if (OB_SUCCESS == ret)
    {
      range.start_key_.assign(start_rowkey, rowkey_column);
      range.end_key_.assign(end_rowkey, rowkey_column);
    }
    if(OB_SUCCESS == ret)
    {
      ret = nb_accessor_.scan(res, OB_INDEX_PROCESS_TABLE_NAME, range, SC("status"));
      if(OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "scan __index_process_info fail:ret[%d]", ret);
      }
    }

    if(OB_SUCCESS == ret)
    {
      int64_t count = 0;
      int32_t st = -1;
      IndexStatus tmp_stat = INDEX_INIT;
      TableRow* table_row = NULL;
      int err = OB_SUCCESS;
      while(OB_SUCCESS == (err = res->next_row()) && OB_SUCCESS == ret)
      {
        res->get_row(&table_row);
        if (NULL == table_row)
        {
          YYSYS_LOG(WARN, "failed to get row from query results");
          ret = OB_ERROR;
        }
        else
        {
          /*
           *  3 cases: NOT_AVALIBALE, AVALIBALE, ERROR
           *
           */
          count++;
          ASSIGN_INT("status", st, int32_t);
          switch (st)
          {
            case 0:
              tmp_stat = NOT_AVALIBALE;
              break;
            case 1:
              if (count == 1 || (count > 1 && st == tmp_stat))
                tmp_stat = AVALIBALE;
              break;
            case 2:
              tmp_stat = ERROR;
              break;
            default:
              break;
          }
          if (ERROR == tmp_stat)
          {
            break;
          }
        }
      }//end while

      if (OB_SUCCESS == ret)
      {
        if (ERROR == tmp_stat
            || (NOT_AVALIBALE == tmp_stat && count == cluster_count)
            || (AVALIBALE == tmp_stat && count == cluster_count))
        {
          stat = tmp_stat;
        }
        /*else
        {
          ret = OB_EAGAIN;//��¼��������cluster��
        }*/
      }
    }

    nb_accessor_.release_query_res(res);
    res = NULL;
  }
  return ret;
}
/*
//add liumz, pangtianze [second index for Paxos] 20170502:b
int ObSchemaServiceImpl::fetch_index_stat(const uint64_t table_id, const int64_t cluster_id, int64_t &stat)
{
  UNUSED(cluster_id);
  int ret = OB_SUCCESS;
  stat = -1;
  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "check inner stat fail");
  }
  else
  {
      QueryRes* res = NULL;
      ObNewRange range;
      int32_t rowkey_column = 2;
      ObObj start_rowkey[rowkey_column];
      ObObj end_rowkey[rowkey_column];
      start_rowkey[0].set_int(table_id);
      start_rowkey[1].set_min_value();
      end_rowkey[0].set_int(table_id);
      end_rowkey[1].set_max_value();
      if (OB_SUCCESS == ret)
      {
        range.start_key_.assign(start_rowkey, rowkey_column);
        range.end_key_.assign(end_rowkey, rowkey_column);
      }
    if (OB_SUCCESS != (ret = nb_accessor_.scan(res, OB_INDEX_PROCESS_TABLE_NAME, range, SC("status"))))
    {
      YYSYS_LOG(WARN, "scan __index_process_info fail, err=%d", ret);
    }
    else
    {
      TableRow* table_row = res->get_only_one_row();
      //int32_t st = -1;
      if (NULL == table_row)
      {
        YYSYS_LOG(INFO, "failed to get row from query results. ");
      }
      else
      {
        ASSIGN_INT("status", stat, int64_t);
      }
//      nb_accessor_.release_query_res(res);
//      res = NULL;
    }
    //add liumz, [bugfix: checksum memory overflow]20170106:b
    if(NULL != res)
    {
      nb_accessor_.release_query_res(res);
      res = NULL;
    }
    //add:e
  }
  return ret;
}
//add:e
*/

int ObSchemaServiceImpl::fetch_index_stat(const uint64_t table_id, const int64_t cluster_id, int64_t &stat)
{
  int ret = OB_SUCCESS;
  stat = -1;
  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "check inner stat fail");
  }
  else
  {
    ObObj rowkey_objs[2];
    rowkey_objs[0].set_int(table_id);
    rowkey_objs[1].set_int(cluster_id);
    ObRowkey rowkey;
    rowkey.assign(rowkey_objs, 2);
    QueryRes* res = NULL;
    if (OB_SUCCESS != (ret = nb_accessor_.get(res, OB_INDEX_PROCESS_TABLE_NAME, rowkey, SC("status"))))
    {
      YYSYS_LOG(WARN, "failed to access row, err=%d", ret);
    }
    else
    {
      TableRow* table_row = res->get_only_one_row();
      //int32_t st = -1;
      if (NULL == table_row)
      {
        YYSYS_LOG(INFO, "failed to get row from query results");
      }
      else
      {
        ASSIGN_INT("status", stat, int64_t);
      }
      //      nb_accessor_.release_query_res(res);
      //      res = NULL;
    }
    //add liumz, [bugfix: checksum memory overflow]20170106:b
    if(NULL != res)
    {
      nb_accessor_.release_query_res(res);
      res = NULL;
    }
    //add:e
  }
  return ret;
}

//add jinty [Paxos Cluster.Balance] 20160708:b
int ObSchemaServiceImpl::get_all_server_status(char *buf, ObArray<ObString> &typeArray,
                                               ObArray<int32_t> &inner_port_Array,
                                               ObArray<ObServer> &servers_ip_with_port,
                                               ObArray<int32_t> &cluster_id_array,ObArray<int32_t> & svr_role_array)
{
  int ret = OB_SUCCESS;
  YYSYS_LOG(TRACE, "get_all_server_status begin");

  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "check inner stat fail");
  }

  QueryRes* res = NULL;

  ObNewRange range;
  int32_t rowkey_column_num = 3;
  ObObj start_obj[rowkey_column_num];
  ObObj end_obj[rowkey_column_num];
  for (int32_t i = 0; i < rowkey_column_num; i++)
  {
    start_obj[i].set_min_value();
    end_obj[i].set_max_value();
  }
  range.start_key_.assign(start_obj, rowkey_column_num);
  range.end_key_.assign(end_obj, rowkey_column_num);
  if(OB_SUCCESS == ret)
  {
    ret = nb_accessor_.scan(res, OB_ALL_SERVER, range,
                            SC("svr_type")("svr_ip")("svr_port")("inner_port")("cluster_id")("svr_role"));
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "scan all_server table fail:ret[%d]", ret);
    }
  }

  char type[SERVER_TYPE_LENGTH]="";
  char ip[SERVER_IP_LENGTH]="";
  int64_t pos = 0;
  int64_t size = 0;
  if(OB_SUCCESS == ret)
  {
    while(OB_SUCCESS == res->next_row())
    {
      TableRow* table_row = NULL;
      if(OB_SUCCESS != (ret = res->get_row(&table_row)))
      {
        YYSYS_LOG(WARN, "get row fail:ret[%d]", ret);
      }
      if(NULL != table_row)
      {
        int32_t int_port = 0;
        int32_t int_cluster_id = -1;
        int32_t int_svr_role = 0;
        int32_t int_inner_port = 0;
        ASSIGN_VARCHAR("svr_type",type,SERVER_TYPE_LENGTH);
        ASSIGN_VARCHAR("svr_ip",ip,SERVER_IP_LENGTH);
        //mod pangtianze [Paxos rs_switch] 20170210:b
        //ASSIGN_VARCHAR("svr_info",info,OB_MAX_SERVER_INFO);
        ASSIGN_INT("inner_port",int_inner_port,int32_t);
        //mod:e
        ASSIGN_INT("svr_port",int_port,int32_t);
        ASSIGN_INT("cluster_id",int_cluster_id,int32_t);
        ASSIGN_INT("svr_role",int_svr_role,int32_t);

        if(OB_SUCCESS == ret)
        {
          size = snprintf(buf + pos, SERVER_TYPE_LENGTH, "%s", type);
          ObString str_type = ObString::make_string(buf + pos);
          pos += size;
          ObServer temp_server;
          if (OB_SUCCESS == str_type.compare("mergeserver"))
          {
            temp_server.set_ipv4_addr(ObServer::convert_ipv4_addr(ip),int_inner_port);
            int_inner_port = int_port;
          }
          else if (OB_SUCCESS == str_type.compare("chunkserver"))
          {
            temp_server.set_ipv4_addr(ObServer::convert_ipv4_addr(ip),int_port);
            int_inner_port = 0;
          }
          //add pangtianze [Paxos rs_switch] 20170627:b
          else
          {
            //ignore
            continue;
          }
          //add:e
          //pos += size;
          if(OB_SUCCESS != (ret = typeArray.push_back(str_type)))
          {
            YYSYS_LOG(WARN, "push str_type fail, ret = %d",ret);
          }
          else if(OB_SUCCESS != (ret = servers_ip_with_port.push_back(temp_server)))
          {
            YYSYS_LOG(WARN, "push server fail, ret = %d",ret);
          }
          else if(OB_SUCCESS != (ret = inner_port_Array.push_back(int_inner_port)))
          {
            YYSYS_LOG(WARN, "push int_inner_port fail, ret = %d",ret);
          }
          else if(OB_SUCCESS != (ret = cluster_id_array.push_back(int_cluster_id)))
          {
            YYSYS_LOG(WARN, "push int_cluster_id fail, ret = %d",ret);
          }
          else if(OB_SUCCESS != (ret = svr_role_array.push_back(int_svr_role)))
          {
            YYSYS_LOG(WARN, "push int_svr_role fail, ret = %d",ret);
          }

        }
        else
        {
          YYSYS_LOG(WARN, "read row from all_server_status fail, ret = %d",ret);
        }
      }
      else
      {
        ret = OB_ERROR;
        YYSYS_LOG(ERROR, "get server status table row fail");
      }
    }//end of while
    if(OB_ITER_END == ret)
    {
      ret = OB_SUCCESS;
    }
  }
  nb_accessor_.release_query_res(res);
  res = NULL;
  return ret;
}
//add e
int ObSchemaServiceImpl::get_cluster_count(int64_t &cc)
{
  int ret = OB_SUCCESS;
  //add pangtianze [Paxos Cluster.Balance] 20170307
  ///��Paxos�ܹ��£�ϵͳֻ��һ����RS������index_process_info�ڲ���Ҳֻ����һ����¼
  /// ��ˣ�����liumz�Ľ��飬�ú���ֱ�ӷ��� cc = 1 ����
  if (OB_SUCCESS == ret)
  {
    cc = 1;
    return ret;
  }
  //add:e
  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "check inner stat fail");
  }
  else
  {
    QueryRes* res = NULL;
    ObNewRange range;
    ObObj start_obj, end_obj;
    start_obj.set_min_value();
    end_obj.set_max_value();
    range.start_key_.assign(&start_obj, 1);
    range.end_key_.assign(&end_obj, 1);
    if(OB_SUCCESS == ret)
    {
      ret = nb_accessor_.scan(res, OB_ALL_CLUSTER, range, SC("cluster_id"));
      if(OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "scan __all_cluster fail:ret[%d]", ret);
      }
      else
      {
        int64_t cluster_count = 0;
        while(OB_SUCCESS == res->next_row())
        {
          cluster_count++;
        }
        cc = cluster_count;
      }
    }

    nb_accessor_.release_query_res(res);
    res = NULL;
  }
  return ret;
}
//add:e

//add fyd [NotNULL_check] [JHOBv0.1] 20140108:b
int ObSchemaServiceImpl::alter_table(const AlterTableSchema &schema,
                                     TableSchema &old_table_schema,
                                     ObSchemaManagerV2 *schema_manager,
                                     int64_t frozen_version,
                                     ObArray<int32_t> *paxos_id_array,
                                     ObArray<ObString> *group_name_list)
{
  int ret = OB_SUCCESS;
  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "check inner stat fail");
  }
  /*add wuna [MultiUps] [sql_api] 20160111:b*/
  else if(schema.is_rule_modify_)
  {
    //[416]
    TableSchema old_schema = old_table_schema;
    snprintf(old_schema.param_list_, OB_MAX_VARCHAR_LENGTH, schema.param_list_);
    old_schema.partition_key_col_num_ = schema.partition_key_col_num_;
    ret = check_function_schema(schema, old_schema);
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN,"check_function_schema failed,ret = %d",ret);
    }
    //add lqc [MultiUps 1.0] [#37] 201705.07 b
    else if(OB_SUCCESS != (ret = alter_all_all_group_mutator(schema,old_table_schema,frozen_version,paxos_id_array,group_name_list)))
    {
      YYSYS_LOG(WARN, "alter __all_all_group mutator failed,ret = %d",ret);
    }
    //add e
    //[600]
    else if(OB_SUCCESS != (ret = alter_all_table_rules_mutator(schema, old_table_schema, frozen_version)))
    {
      YYSYS_LOG(WARN, "alter __all_table_rules mutator failed,ret = %d",ret);
    }
  }
  else
  {
    yysys::CThreadGuard guard(&mutex_);
    ObMutator* mutator = NULL;
    if(OB_SUCCESS == ret)
    {
      mutator = GET_TSI_MULT(ObMutator, TSI_COMMON_MUTATOR_1);
      if(NULL == mutator)
      {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        YYSYS_LOG(WARN, "get thread specific Mutator fail");
      }
    }
    if (OB_SUCCESS == ret)
    {
      ret = mutator->reset();
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "reset ob mutator fail:ret[%d]", ret);
      }
    }
    //add zhaoqiong [Schema Manager] 20150327:b
    //append a ALTER_TABLE operation record to __all_ddl_operation
    if (OB_SUCCESS == ret)    //[conflict_level B]	�߼��޸�
    {
      ret = add_ddl_operation(mutator, schema.table_id_, ALTER_TABLE);
      if(OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "add ddl operation to mutator fail:ret[%d]", ret);
      }
    }
    //add:e
    if (OB_SUCCESS == ret)
    {
      ret = alter_table_mutator(schema, mutator, old_table_schema, schema_manager);
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "set alter table mutator fail:ret[%d]", ret);
      }
    }

    if (OB_SUCCESS == ret)
    {
      ret = client_proxy_->mutate(*mutator);
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "apply mutator fail:ret[%d]", ret);
      }
      else
      {
        YYSYS_LOG(INFO, "send alter table to ups succ.");

      }
    }
  }
  return ret;
}
//add 20140108:e
int ObSchemaServiceImpl::alter_table(const AlterTableSchema & schema, const int64_t old_schema_version, ObSchemaManagerV2 *schema_manager)
{
  int ret = OB_SUCCESS;
  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "check inner stat fail");
  }
  yysys::CThreadGuard guard(&mutex_);
  ObMutator* mutator = NULL;
  if(OB_SUCCESS == ret)
  {
    mutator = GET_TSI_MULT(ObMutator, TSI_COMMON_MUTATOR_1);
    if(NULL == mutator)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      YYSYS_LOG(WARN, "get thread specific Mutator fail");
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = mutator->reset();
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "reset ob mutator fail:ret[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    ret = alter_table_mutator(schema, mutator, old_schema_version, schema_manager);
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "set alter table mutator fail:ret[%d]", ret);
    }
  }
  //add zhaoqiong [Schema Manager] 20150327:b
  //append a ALTER_TABLE operation record to __all_ddl_operation
  if (OB_SUCCESS == ret)
  {
    ret = add_ddl_operation(mutator, schema.table_id_, ALTER_TABLE);
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "add ddl operation to mutator fail:ret[%d]", ret);
    }
  }
  //add:e

  if (OB_SUCCESS == ret)
  {
    ret = client_proxy_->mutate(*mutator);
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "apply mutator fail:ret[%d]", ret);
    }
    else
    {
      YYSYS_LOG(INFO, "send alter table to ups succ.");
    }
  }
  return ret;
}
int ObSchemaServiceImpl::generate_new_table_name(char* buf, const uint64_t length, const char* table_name, const uint64_t table_name_length)
{
  int ret = OB_SUCCESS;
  if (table_name_length + sizeof(TMP_PREFIX) >= length
      || NULL == buf
      || NULL == table_name)
  {
    YYSYS_LOG(WARN, "invalid buf_len. need size=%ld, exist_buf_size=%ld, buf=%p, table_name=%s",
              table_name_length + 1, length, buf, table_name);
    ret = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == ret)
  {
    if (0 >= snprintf(buf, length, "%s", TMP_PREFIX))
    {
      YYSYS_LOG(WARN, "fail to print prefix to buf. buf_length=%ld, prefix_lenght=%ld", length, strlen(TMP_PREFIX));
      ret = OB_ERROR;
    }
    else if (0 >= snprintf(buf + strlen(TMP_PREFIX), length - strlen(TMP_PREFIX), "%s", table_name))
    {
      ret = OB_ERROR;
      YYSYS_LOG(WARN, "fail to print table_name to buf. pos=%ld, length=%ld", strlen(TMP_PREFIX), table_name_length);
    }
    else
    {
      buf[strlen(TMP_PREFIX) + table_name_length + 1] = '\0';
      YYSYS_LOG(INFO, "new table name is %s, tmp_prefix_len=%ld, table_name_length=%ld", buf, strlen(TMP_PREFIX), table_name_length);
    }
  }
  return ret;
}
int ObSchemaServiceImpl::modify_table_id(TableSchema& table_schema, const int64_t new_table_id, const int64_t frozen_version)
{
  int ret = OB_SUCCESS;
  YYSYS_LOG(INFO, "modify table id. old_table_id=%ld, new_table_id=%ld", table_schema.table_id_, new_table_id);
  if(!check_inner_stat())
  {
    ret = OB_ERROR;
    YYSYS_LOG(WARN, "check inner stat fail");
  }
  int64_t old_table_id = table_schema.table_id_;
  char table_name_buf[OB_MAX_TABLE_NAME_LENGTH];
  char old_table_name_buf[OB_MAX_TABLE_NAME_LENGTH];
  ObString table_name;
  memcpy(old_table_name_buf, table_schema.table_name_, strlen(table_schema.table_name_) + 1);
  table_name.assign_ptr(old_table_name_buf, static_cast<int32_t>(strlen(table_schema.table_name_)));

  //add zhaoqiong [database manager]@20150611
  ObString db_name;
  db_name.assign_ptr(const_cast<char *>(table_schema.dbname_),static_cast<int32_t>(strlen(table_schema.dbname_)));
  //add:e

  if (OB_SUCCESS == ret)
  {
    ret = generate_new_table_name(table_name_buf, OB_MAX_TABLE_NAME_LENGTH, table_schema.table_name_, strlen(table_schema.table_name_));
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "fail to genearte new table_name. table_name=%s, length=%ld",
                table_schema.table_name_, strlen(table_schema.table_name_));
    }
  }
  // common::TableSchema new_table_schema = table_schema;
  if (OB_SUCCESS == ret)
  {
    memcpy(table_schema.table_name_, table_name_buf, strlen(table_name_buf) + 1);
    table_schema.table_name_[strlen(table_name_buf)] = '\0';
    table_schema.table_id_ = new_table_id;
    ret = create_table(table_schema, frozen_version);
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "fail to create table. table_name=%s", table_name_buf);
    }
    else
    {
      YYSYS_LOG(INFO, "create tmp table for bypass success. table_name=%s", table_schema.table_name_);
    }
  }
  ObMutator* mutator = NULL;
  if (OB_SUCCESS == ret)
  {
    mutator = GET_TSI_MULT(ObMutator, TSI_COMMON_MUTATOR_1);
    if(NULL == mutator)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      YYSYS_LOG(WARN, "get thread specific ObMutator fail");
    }

    if(OB_SUCCESS == ret)
    {
      ret = mutator->reset();
      if(OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "reset ob mutator fail:ret[%d]", ret);
      }
    }
    if (OB_SUCCESS == ret)
    {
      ObRowkey rowkey;
      //delete dolphin [database manager]@20150609:b
      /**
      ObObj rowkey_value;
      ObString new_table_name;
      new_table_name.assign_ptr(table_schema.table_name_,
          static_cast<int32_t>(strlen(table_schema.table_name_)));
      rowkey_value.set_varchar(new_table_name);
      rowkey.assign(&rowkey_value, 1);
      //delete:e
      */
      ObObj value;
      value.set_int(old_table_id);
      //add dolphin [database manager]@20150609:b
      ObString new_table_name;
      new_table_name.assign_ptr(table_schema.table_name_,
                                static_cast<int32_t>(strlen(table_schema.table_name_)));


      ObObj obj[2];
      obj[0].set_varchar(new_table_name);
      obj[1].set_varchar(db_name);
      rowkey.assign(obj,2);
      //add:e
      ret = mutator->update(first_tablet_entry_name, rowkey, table_name_str, value);
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "fail to add update cell to mutator. ret=%d, rowkey=%s", ret, to_cstring(new_table_name));
      }
      else
      {
        YYSYS_LOG(INFO, "change table id for bypass table. table_name=%s, table_id=%ld",
                  table_schema.table_name_, old_table_id);
      }
    }
    if (OB_SUCCESS == ret)
    {
      ObRowkey rowkey;
      //delete dolphin [database manager]@20150609:b
      /**
      ObObj rowkey_value;
      rowkey_value.set_varchar(table_name);
      rowkey.assign(&rowkey_value, 1);
      */
      //delete:e
      //add dolphin [database manager]@20150609:b
      ObObj obj[2];
      obj[0].set_varchar(table_name);
      obj[1].set_varchar(db_name);
      rowkey.assign(obj,2);
      //add:e
      ObObj value;
      value.set_int(new_table_id);
      ret = mutator->update(first_tablet_entry_name, rowkey, table_name_str, value);
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "fail to add update cell to mutator. ret=%d, rowkey=%s", ret, to_cstring(table_name));
      }
      else
      {
        YYSYS_LOG(INFO, "change table id for bypass table. table_name=%.*s, table_id=%ld",
                  table_name.length(), table_name.ptr(), new_table_id);
      }
    }
    if(OB_SUCCESS == ret)
    {
      ObRowkey rowkey;
      ObObj obj[2];
      obj[0].set_varchar(table_name);
      obj[1].set_varchar(db_name);
      rowkey.assign(obj,2);

      ObString expire_condition;
      char condition_expr[OB_MAX_EXPIRE_CONDITION_LENGTH] = "1=0";
      expire_condition.assign_ptr(condition_expr, static_cast<int32_t>(strlen(condition_expr)));

      ObObj value;
      value.set_varchar(expire_condition);
      ret = mutator->update(first_tablet_entry_name, rowkey, expire_info, value);
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "fail to add update cell to mutator. ret=%d, rowkey=%s", ret, to_cstring(table_name));
      }
      else
      {
        YYSYS_LOG(INFO, "change expire_info for bypass table. table_name=%.*s, table_id=%ld",
                  table_name.length(), table_name.ptr(), new_table_id);
      }
    }
  }

  uint64_t db_id = OB_INVALID_ID;
  if(OB_SUCCESS == ret)
  {
    QueryRes *res = NULL;
    TableRow *table_row = NULL;
    ObRowkey rowkey;
    ObObj obj[1];
    obj[0].set_varchar(db_name);
    rowkey.assign(obj,1);

    ret = nb_accessor_.get(res, OB_ALL_DATABASE_NAME, rowkey, SC("db_id"));
    if(ret != OB_SUCCESS)
    {
      YYSYS_LOG(WARN, "get db_id failed:ret=%d", ret);
    }
    else
    {
      table_row = res->get_only_one_row();
      if(NULL != table_row)
      {
        ASSIGN_INT("db_id", db_id, uint64_t);
      }
      else
      {
        ret = OB_ENTRY_NOT_EXIST;
        //
      }
    }
  }

  //add privilege
  QueryRes* res = NULL;
  TableRow* table_row = NULL;
  ObNewRange range;
  int32_t rowkey_column = 3;
  ObObj start_rowkey[rowkey_column];
  ObObj end_rowkey[rowkey_column];

  //start_rowkey[1].set_int(old_table_id);
  start_rowkey[0].set_min_value();
  start_rowkey[2].set_int(old_table_id);
  start_rowkey[1].set_int(db_id);
  //end_rowkey[1].set_int(old_table_id);
  end_rowkey[0].set_max_value();
  end_rowkey[2].set_int(old_table_id);
  end_rowkey[1].set_int(db_id);

  if (OB_SUCCESS == ret)
  {
    range.start_key_.assign(start_rowkey, rowkey_column);
    range.end_key_.assign(end_rowkey, rowkey_column);
  }
  if (OB_SUCCESS == ret)
  {
    ret = nb_accessor_.scan(res,
                            OB_ALL_TABLE_PRIVILEGE_TABLE_NAME,
                            range,
                            SC("user_id")("priv_all")("priv_alter")("priv_create")("priv_create_user")("priv_delete")("priv_drop")("priv_grant_option")("priv_insert")("priv_update")("priv_select")(
                              "priv_replace"));
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "fail to get privilege info for table, table_id=%ld", old_table_id);
    }
  }
  if (OB_SUCCESS == ret)
  {
    while(OB_SUCCESS == res->next_row() && OB_SUCCESS == ret)
    {
      res->get_row(&table_row);
      if (NULL != table_row)
      {
        ret = prepare_privilege_for_table(table_row, mutator, new_table_id, db_id);
        if (OB_SUCCESS != ret)
        {
          YYSYS_LOG(WARN, "fail to get privilege info for table %ld, ret=%d", new_table_id, ret);
        }
      }
    }
  }
  nb_accessor_.release_query_res(res);
  res = NULL;
  if (OB_SUCCESS == ret)
  {
    ret = client_proxy_->mutate(*mutator);
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "apply mutator to update server fail. ret=%d", ret);
      while (OB_SUCCESS == mutator->next_cell())
      {
        ObMutatorCellInfo *ci = NULL;
        bool is_row_changed = false;
        bool is_row_finished = false;
        mutator->get_cell(&ci, &is_row_changed, &is_row_finished);
        YYSYS_LOG(INFO, "%s\n", common::print_cellinfo(&ci->cell_info));
      }
    }
  }
  ObString drop_table_name;
  drop_table_name.assign_ptr(table_schema.table_name_, static_cast<int32_t>(strlen(table_schema.table_name_)));

  ObString full_table_name;
  char buf[OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1] = {0};
  full_table_name.assign_buffer(buf, OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1);
  full_table_name.concat(db_name, drop_table_name);

  if ( OB_SUCCESS != drop_table(full_table_name, new_table_id) )
  {
    YYSYS_LOG(WARN, "fail to drop table. table_name=%s, table_schema_tid=%lu",
              table_schema.table_name_, table_schema.table_id_);
  }
  else
  {
    YYSYS_LOG(INFO, "drop tmp table for bypass. table_name=%s", table_schema.table_name_);
  }
  return ret;
}

int ObSchemaServiceImpl::prepare_privilege_for_table(const TableRow* table_row, ObMutator *mutator, const int64_t table_id, const int64_t db_id)
{
  int ret = OB_SUCCESS;
  int64_t user_id = 0;
  ASSIGN_INT("user_id", user_id, uint64_t);
  int64_t priv_all = 0;
  ASSIGN_INT("priv_all", priv_all, uint64_t);
  int64_t priv_alter = 0;
  ASSIGN_INT("priv_alter", priv_alter, uint64_t);
  int64_t priv_create = 0;
  ASSIGN_INT("priv_create", priv_create, uint64_t);
  int64_t priv_create_user = 0;
  ASSIGN_INT("priv_create_user", priv_create_user, uint64_t);
  int64_t priv_delete = 0;
  ASSIGN_INT("priv_delete", priv_delete, uint64_t);
  int64_t priv_drop = 0;
  ASSIGN_INT("priv_drop", priv_drop, uint64_t);
  int64_t priv_grant_option = 0;
  ASSIGN_INT("priv_grant_option", priv_grant_option, uint64_t);
  int64_t priv_insert = 0;
  ASSIGN_INT("priv_insert", priv_insert, uint64_t);
  int64_t priv_update = 0;
  ASSIGN_INT("priv_update", priv_update, uint64_t);
  int64_t priv_select = 0;
  ASSIGN_INT("priv_select", priv_select, uint64_t);
  int64_t priv_replace = 0;
  ASSIGN_INT("priv_replace", priv_replace, uint64_t);
  YYSYS_LOG(INFO,
            "use id=%ld, priv_all=%ld, priv_alter=%ld, priv_create=%ld, priv_create_user=%ld, priv_delete=%ld, priv_drop=%ld, priv_grant_option=%ld, priv_insert=%ld, priv_update=%ld, priv_select=%ld, priv_replace=%ld,db_id=%ld",
            user_id,
            priv_all,
            priv_alter,
            priv_create,
            priv_create_user,
            priv_delete,
            priv_drop,
            priv_grant_option,
            priv_insert,
            priv_update,
            priv_select,
            priv_replace
            ,db_id
            );
  ObRowkey rowkey;
  ObObj value[3];
  value[1].set_int(db_id);
  value[0].set_int(user_id);
  value[2].set_int(table_id);
  rowkey.assign(value, 3);
  ADD_INT(privilege_table_name, rowkey, "priv_all", priv_all);
  ADD_INT(privilege_table_name, rowkey, "priv_alter", priv_alter);
  ADD_INT(privilege_table_name, rowkey, "priv_create", priv_create);
  ADD_INT(privilege_table_name, rowkey, "priv_create_user", priv_create_user);
  ADD_INT(privilege_table_name, rowkey, "priv_delete", priv_delete);
  ADD_INT(privilege_table_name, rowkey, "priv_drop", priv_drop);
  ADD_INT(privilege_table_name, rowkey, "priv_grant_option", priv_grant_option);
  ADD_INT(privilege_table_name, rowkey, "priv_insert", priv_insert);
  ADD_INT(privilege_table_name, rowkey, "priv_update", priv_update);
  ADD_INT(privilege_table_name, rowkey, "priv_select", priv_select);
  ADD_INT(privilege_table_name, rowkey, "priv_replace", priv_replace);
  return ret;
}

//add zhaoqiong [Schema Manager] 20150327:b
int ObSchemaServiceImpl::fetch_schema_mutator(const int64_t start_version, const int64_t end_version, ObSchemaMutator& schema_mutator)
{
  int ret = OB_SUCCESS;
  if (start_version >= end_version || start_version < CORE_SCHEMA_VERSION)
  {
    YYSYS_LOG(WARN, "invalid argument,start_version= %ld,end_version= %ld", start_version, end_version);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    schema_mutator.set_version_range(start_version,end_version);

    ObTableIdNameIterator tables_id_name;
    ObTableIdName* tid_name = NULL;
    ObArray<TableSchema>& table_schema_array = schema_mutator.get_add_table_schema();
    ObArray<int64_t> created_table_array;//include droped table and altered table
    ObArray<int64_t>& droped_table_array = schema_mutator.get_droped_tables();

    bool only_core_tables = false;
    bool need_refresh_schema = false;
    //query all operation from __all_ddl_operation in range(start_version,end_version]
    if (OB_SUCCESS != (ret = init(this->client_proxy_, only_core_tables)))
    {
      YYSYS_LOG(WARN, "failed to init schema service, err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = tables_id_name.init(client_proxy_, only_core_tables, start_version,end_version)))
    {
      YYSYS_LOG(WARN, "failed to init iterator, err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = tables_id_name.get_table_list(created_table_array, droped_table_array, need_refresh_schema)))
    {
      YYSYS_LOG(WARN, "failed to get table list, err=%d", ret);
    }
    else if (need_refresh_schema)
    {
      schema_mutator.set_refresh_schema(need_refresh_schema);
    }
    else if (created_table_array.count() <= 0 && droped_table_array.count() <= 0)
    {
      ret = OB_INNER_STAT_ERROR;
      YYSYS_LOG(ERROR, "schema mutator has no content, this should not happen");
    }
    else
    {
      //check whether table is mutator info, compare with created_table_array
      while (OB_SUCCESS == ret && OB_SUCCESS == (ret = tables_id_name.next()))
      {
        TableSchema table_schema;
        if (OB_SUCCESS != (ret = tables_id_name.get(&tid_name)))
        {
          YYSYS_LOG(WARN, "failed to get next name, err=%d", ret);
        }
        else
        {
          //TODO check tid_name.id in add_array
          bool is_mutator = false;
          for (int i = 0; i < created_table_array.count(); i++)
          {
            if (created_table_array.at(i) == (int64_t)(tid_name->table_id_))
            {
              is_mutator = true;
              break;
            }
          }
          if (!is_mutator)
          {
            continue;
          }

          //modify dolphin [database manager]@20150616:b
          //if (OB_SUCCESS != (ret = get_table_schema(/**modify dolphin [datbase manager]@20150613 tid_name->table_name_*/dt, table_schema)))
          if (OB_SUCCESS != (ret = get_table_schema( tid_name->table_name_, table_schema,tid_name->dbname_)))
            //modify:e
          {
            YYSYS_LOG(WARN, "failed to get table schema, err=%d, table_name=%.*s", ret,
                      tid_name->table_name_.length(), tid_name->table_name_.ptr());
            ret = OB_INNER_STAT_ERROR;
          }
          else if (OB_SUCCESS != (ret = table_schema_array.push_back(table_schema)))
          {
            YYSYS_LOG(ERROR, "memory overflow");
          }
          else
          {
            YYSYS_LOG(INFO, "get table schema add into shemaManager, tname=%.*s,replica_count=%d",
                      tid_name->table_name_.length(), tid_name->table_name_.ptr(),table_schema.replica_num_);
          }
        }
      } // end while
      if (OB_ITER_END == ret)
      {
        //ObSchemaMutator may be empty
        ret = OB_SUCCESS;
      }
      else if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN,  "failed to get schema mutator, ret=%d", ret);
      }
    }
  }
  return ret;
}

int ObSchemaServiceImpl::get_schema(bool only_core_tables, ObSchemaManagerV2& out_schema)
{
  int ret = OB_SUCCESS;
  ObTableIdNameIterator tables_id_name;
  ObTableIdName* tid_name = NULL;
  ObArray<TableSchema> table_schema_array;
  if (OB_SUCCESS != (ret = init(this->client_proxy_, only_core_tables)))
  {
    YYSYS_LOG(WARN, "failed to init schema service, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = tables_id_name.init(client_proxy_, only_core_tables)))
  {
    YYSYS_LOG(WARN, "failed to init iterator, err=%d", ret);
  }
  else
  {
    while (OB_SUCCESS == ret && OB_SUCCESS == (ret = tables_id_name.next()))
    {
      TableSchema table_schema;
      if (OB_SUCCESS != (ret = tables_id_name.get(&tid_name)))
      {
        YYSYS_LOG(WARN, "failed to get next name, err=%d", ret);
      }
      else
      {
        //modify dolphin [database manager]@20150616:b
        //if (OB_SUCCESS != (ret = get_table_schema(/**modify dolphin [datbase manager]@20150613 tid_name->table_name_*/dt, table_schema)))
        if (OB_SUCCESS != (ret = get_table_schema(tid_name->table_name_, table_schema,tid_name->dbname_)))
          //modify:e
        {
          YYSYS_LOG(WARN, "failed to get table schema, err=%d, table_name=%.*s", ret,
                    tid_name->table_name_.length(), tid_name->table_name_.ptr());
          ret = OB_INNER_STAT_ERROR;
        }
        else
        {
          // @todo DEBUG
          table_schema_array.push_back(table_schema);
          YYSYS_LOG(DEBUG, "get table schema add into shemaManager, tname=%.*s",
                    tid_name->table_name_.length(), tid_name->table_name_.ptr());
        }
      }
    } // end while
    if (OB_ITER_END == ret)
    {
      ret = OB_SUCCESS;
    }
    else if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN,  "failed to get all table schema, only_core_tables=%s, ret=%d",
                only_core_tables ? "true" : "false", ret);
    }

    if (OB_SUCCESS == ret)
    {
      if (OB_SUCCESS != (ret = out_schema.add_new_table_schema(table_schema_array)))
      {
        YYSYS_LOG(WARN, "failed to add table schema into the schema manager, err=%d", ret);
      }
    }

    if (OB_SUCCESS == ret && !only_core_tables)
    {
      //[602]
      if (OB_SUCCESS != (ret = out_schema.sort_table()))
      {
        YYSYS_LOG(WARN, "failed to sort tables in schema manager, err=%d", ret);
      }
      else if (OB_SUCCESS != (ret = out_schema.sort_column()))
      {
        YYSYS_LOG(WARN, "failed to sort columns in schema manager, err=%d", ret);
      }
      //add liumz, [bugfix:init index hash when get schema]20160126:b
      else if(OB_SUCCESS != (ret = out_schema.init_index_hash()))
      {
        YYSYS_LOG(WARN, "failed to init_index_hash");
      }
      //add e
    }
  }
  return ret;
}
//add:e

//add liuxiao [secondary index col checksum] 20150316
//??������?��?��DD��?��o����??��2?������?��?3y??o����y??��?��?��?��?��?��DD��?��o����y?Y
int ObSchemaServiceImpl::clean_checksum_info(const int64_t max_draution_of_version,const int64_t current_version)
{
  int ret = OB_SUCCESS;
  QueryRes* res = NULL;    //�����?�䨮?����?����?D??��?��?��??3����cchecksum��y?Y
  TableRow* table_row = NULL;  //??��?DD��?cchecksumD??��
  ObNewRange range;            //
  int64_t cell_index = 0;

  range.set_whole_range(); //range����???amin��?max,��?sc1y??

  YYSYS_LOG(INFO, "clean version less than %ld", current_version - max_draution_of_version);

  if(OB_SUCCESS != ret)
  {
    YYSYS_LOG(WARN, "fail to start clean column_checksum %d", ret);
  }
  //???��3?D����a��?3y��?column checksum��y?Y
  else if(OB_SUCCESS != (ret = nb_accessor_.scan(res, OB_ALL_CCHECKSUM_INFO_TABLE_NAME, range, SC("version"),ScanConds("version", LT, current_version-max_draution_of_version))))
  {
    YYSYS_LOG(WARN, "failed to scan data to delete from column_checksum %d" , ret);
  }
  else
  {
    int64_t count = 0;
    //��������?����D��????��3?��?��y?Y2�騰?��?��?��?��?��y?Y
    //mod liumz, [bugfix: cchecksum too large]20161108:b
    //while(OB_SUCCESS == res->next_row() && OB_SUCCESS == ret)
    while(true)
    {
      ret = res->next_row();
      if (OB_ITER_END == ret)
      {
        YYSYS_LOG(INFO, "delete %ld row from column cheksum", count);
        bool is_fullfilled = false;
        int64_t fullfilled_item_num = 0;
        ret = res->get_scanner()->get_is_req_fullfilled(is_fullfilled, fullfilled_item_num);
        if(OB_SUCCESS != ret)
        {
          YYSYS_LOG(WARN, "get is req fullfilled fail:ret[%d]", ret);
          break;
        }
        else if (is_fullfilled)
        {
          YYSYS_LOG(WARN, "get is req fullfilled [%d]", is_fullfilled);
          ret = OB_ITER_END;
          break;
        }
        else if (OB_SUCCESS != (ret = nb_accessor_.get_next_scanner(res)))
        {
          YYSYS_LOG(WARN, "fail to get next scanner:ret[%d]", ret);
          break;
        }
        else
        {
          ret = res->next_row();//iterator new scanner
        }
      }
      if(OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "failed to get next row of col checksum");
        break;
      }
      else if(OB_SUCCESS != (ret = res->get_row(&table_row)))
      {
        YYSYS_LOG(WARN, "failed to get next row of col checksum");
        break;
      }
      //res->get_row(&table_row);
      //mod:e
      else if(NULL != table_row)
      {
        if(OB_SUCCESS != (ret = nb_accessor_.delete_row(OB_ALL_CCHECKSUM_INFO_TABLE_NAME,table_row->get_cell_info(cell_index)->row_key_)))
        {
          YYSYS_LOG(WARN, "failed to delete one row from column cheksum %d", ret);
          break;
        }
        else
        {
          count++;
          YYSYS_LOG(DEBUG, "delete one row from column cheksum %s", to_cstring(table_row->get_cell_info(cell_index)->row_key_));
        }
      }
      else
      {
        ret = OB_ERROR;
        YYSYS_LOG(WARN, "delete column checksum row is NULL");
        break;
      }
    }
    if (OB_ITER_END == ret)
    {
      YYSYS_LOG(INFO, "delete %ld row from column cheksum", count);
      ret = OB_SUCCESS;
    }
  }
  //add liumz, [bugfix: checksum memory overflow]20170106:b
  if(NULL != res)
  {
    nb_accessor_.release_query_res(res);
    res = NULL;
  }
  //add:e
  return ret;
}

int ObSchemaServiceImpl::get_checksum_info(const ObNewRange new_range,const int64_t cluster_id,const int64_t required_version,ObString& cchecksum)
{
  //cso?2���?����o��?��?��??��?��?��???��?��?��?tablet column checksum
  //?a?���?��??a��?����?a?3??tablet��??a??��D??��?��y?Y???T����????D?��?column checksum��??��??
  int ret = OB_SUCCESS;
  QueryRes* res = NULL;
  TableRow* table_row = NULL;
  //����?��D����a2��?����?cchecksum��?rowkey
  ObRowkey rowkey;
  ObObj rowkey_list[4];
  char range_buf[OB_RANGE_STR_BUFSIZ];
  //  new_range.to_string(range_buf,sizeof(range_buf));
  //  int32_t len = static_cast<int32_t>(strlen(range_buf));
  int32_t len = 0;
  int64_t rangestr_len = 0;
  new_range.to_string_memcpy(range_buf, sizeof(range_buf), rangestr_len);
  len = static_cast<int32_t>(rangestr_len);

  ObString str_range(0,len,range_buf);

  //11?��???�¦�?range,����??����id,version,o��range
  rowkey_list[0].set_int(new_range.table_id_);
  rowkey_list[1].set_int(cluster_id);
  rowkey_list[2].set_int(required_version);
  rowkey_list[3].set_varchar(str_range);
  rowkey.assign(rowkey_list,4);

  char varchar_cchecksum[common::OB_MAX_COL_CHECKSUM_STR_LEN];
  if(OB_SUCCESS != ( ret = nb_accessor_.get(res,OB_ALL_CCHECKSUM_INFO_TABLE_NAME,rowkey,SC("column_checksum"))))
  {
    YYSYS_LOG(ERROR, "faild to get nb cchecksum ret = %d",ret);
  }
  else
  {
    table_row = res->get_only_one_row();
    if(NULL != table_row)
    {
      //??��?��?��?DD,2�騨?3?column checksum��?��y?Y
      ASSIGN_VARCHAR("column_checksum",varchar_cchecksum,common::OB_MAX_COL_CHECKSUM_STR_LEN);
      len = static_cast<int32_t>(strlen(varchar_cchecksum));
      //modify liuxiao [secondary index bug.fix] 20150625
      //cchecksum.assign(varchar_cchecksum,len);
      cchecksum.write(varchar_cchecksum,len);
      //modify e
    }
    else
    {
      ret = OB_ENTRY_NOT_EXIST;
      YYSYS_LOG(WARN, "get table row fail:cchecksum  %d",ret);
    }
  }
  memset(range_buf,0,OB_RANGE_STR_BUFSIZ*sizeof(char));
  //add liumz, [bugfix: checksum memory overflow]20170106:b
  if(NULL != res)
  {
    nb_accessor_.release_query_res(res);
    res = NULL;
  }
  //add:e
  return ret;
}

int ObSchemaServiceImpl::get_checksum_info(const ObNewRange new_range, const int64_t required_version, ObString &cchecksum)
{
  int ret = OB_SUCCESS;
  QueryRes *res = NULL;
  TableRow *table_row = NULL;

  const int64_t rowkey_column_num = 4;
  char range_buf[OB_RANGE_STR_BUFSIZ];
  int32_t len = 0;
  int64_t rangestr_len = 0;
  new_range.to_string_memcpy(range_buf, sizeof(range_buf), rangestr_len);
  len = static_cast<int32_t>(rangestr_len);

  ObString str_range(0,len,range_buf);
  ObRowkey rowkey;
  ObObj rowkey_list[rowkey_column_num];

  char varchar_cchecksum[common::OB_MAX_COL_CHECKSUM_STR_LEN];

  rowkey_list[0].set_int(new_range.table_id_);
  rowkey_list[2].set_int(required_version);
  rowkey_list[3].set_varchar(str_range);

  for(int64_t cluster_id = 0; cluster_id < OB_MAX_CLUSTER_COUNT; ++cluster_id)
  {
    rowkey_list[1].set_int(cluster_id);
    rowkey.assign(rowkey_list, rowkey_column_num);
    if(NULL != res)
    {
      nb_accessor_.release_query_res(res);
      res = NULL;
    }
    if(OB_SUCCESS != (ret = nb_accessor_.get(res, OB_ALL_CCHECKSUM_INFO_TABLE_NAME, rowkey, SC("column_checksum"))))
    {
      YYSYS_LOG(ERROR, "faild to get nb cchecksum ret = %d", ret);
    }
    else
    {
      table_row = res->get_only_one_row();
      if(NULL != table_row)
      {
        ASSIGN_VARCHAR("column_checksum", varchar_cchecksum, common::OB_MAX_COL_CHECKSUM_STR_LEN);
        len = static_cast<int32_t>(strlen(varchar_cchecksum));
        cchecksum.write(varchar_cchecksum, len);
        break;
      }
      else
      {
        ret = OB_ENTRY_NOT_EXIST;
        YYSYS_LOG(WARN, "get table row fail:cchecksum %d", ret);
      }
    }
  }
  memset(range_buf, 0, OB_RANGE_STR_BUFSIZ * sizeof(char));
  if(NULL != res)
  {
    nb_accessor_.release_query_res(res);
    res = NULL;
  }
  return ret;
}


int ObSchemaServiceImpl::prepare_checksum_info_row(const uint64_t table_id ,const ObRowkey& rowkey,const ObObj& column_check_sum ,ObMutator *mutator)
{
  //???��?��?����?��?��?��D?�䨺1��?
  int ret = OB_SUCCESS;
  const uint64_t column_id = 19;
  if(OB_SUCCESS != (ret = mutator->insert(table_id,rowkey,column_id,column_check_sum)))
  {
    YYSYS_LOG(WARN, "faild to add column check sum to mutator");
  }
  return ret;
}
int ObSchemaServiceImpl::check_column_checksum(const int64_t orginal_table_id,const int64_t index_table_id,const int64_t cluster_id, const int64_t current_version, bool &is_right)
{
  //����ԭ����������id����__all_cchecksum_info�ڲ����а�version��ȡ��column checksum���бȽ�
  is_right = true;
  int ret = OB_SUCCESS;
  QueryRes* res = NULL;
  TableRow* table_row = NULL;
  ObNewRange range;
  ObString tmp_string;
  int64_t cell_index = 0;   //��Ϊֻȡ��column checksum�е����ݣ�����Ϊ0
  int64_t rowkey_column_num = 4;
  ObObj start_rowkey[rowkey_column_num];
  ObObj end_rowkey[rowkey_column_num];
  col_checksum org_table_column_checksum;
  col_checksum index_table_column_checksum;
  col_checksum cc;

  int64_t src_cc_count = 0;
  int64_t index_cc_count = 0;

  const char *groupby_column = "range"; //[index]

  char tmp[OB_MAX_COL_CHECKSUM_STR_LEN];
  tmp_string.assign_ptr(tmp,OB_MAX_COL_CHECKSUM_STR_LEN);

  org_table_column_checksum.reset();
  index_table_column_checksum.reset();

  start_rowkey[0].set_int(orginal_table_id);
  end_rowkey[0].set_int(orginal_table_id);

  if(OB_ALL_CLUSTER_FLAG != cluster_id)
  {
      start_rowkey[1].set_int(cluster_id);
      end_rowkey[1].set_int(cluster_id);
  }
  else
  {
      start_rowkey[1].set_int(OB_START_CLUSTER_ID);
      end_rowkey[1].set_int(OB_MAX_CLUSTER_ID);
  }

  start_rowkey[2].set_int(current_version);
  end_rowkey[2].set_int(current_version);
  start_rowkey[3].set_min_value();
  end_rowkey[3].set_max_value();

  range.start_key_.assign(start_rowkey,rowkey_column_num);
  range.end_key_.assign(end_rowkey,rowkey_column_num);
  //    range.border_flag_.inclusive_end();
  //    range.border_flag_.inclusive_start();
  range.border_flag_.unset_inclusive_start();
  range.border_flag_.unset_inclusive_end();

//  if (OB_SUCCESS != ret)
//  {
//    YYSYS_LOG(WARN, "fail to start get cchecksum info from orginal table");
//  }
//  else if(OB_SUCCESS != (ret = nb_accessor_.scan(res, OB_ALL_CCHECKSUM_INFO_TABLE_NAME, range, SC("column_checksum"))))
//  {
//    YYSYS_LOG(WARN, "fail to nb scan cchecksum info of orginal table %d",ret);
//  }

  if(OB_ALL_CLUSTER_FLAG != cluster_id)
  {
      ret = nb_accessor_.scan(res, OB_ALL_CCHECKSUM_INFO_TABLE_NAME, range, SC("column_checksum"));
  }
  else
  {
      ret = nb_accessor_.scan(res, OB_ALL_CCHECKSUM_INFO_TABLE_NAME, range, SC("column_checksum"), ScanConds("version", EQ, current_version), groupby_column);
  }
  if(OB_SUCCESS != ret)
  {
      YYSYS_LOG(WARN,"fail to nb scan cchecksum info of orginal table:%ld cluster:%ld ret=%d", orginal_table_id, cluster_id, ret);
  }
  else
  {
    YYSYS_LOG(INFO, "start calculate cchecksum of orginal_table_id:%ld", orginal_table_id);
    //mod liumz, [bugfix: cchecksum too large]20161107:b
    //while(OB_ITER_END != (ret = res->next_row()))
    while(true)
    {
      ret = res->next_row();
      if (OB_ITER_END == ret)
      {
        bool is_fullfilled = false;
        int64_t fullfilled_item_num = 0;
        ret = res->get_scanner()->get_is_req_fullfilled(is_fullfilled, fullfilled_item_num);
        if(OB_SUCCESS != ret)
        {
          YYSYS_LOG(WARN, "get is req fullfilled fail:ret[%d]", ret);
          break;
        }
        else if (is_fullfilled)
        {
          ret = OB_ITER_END;
          break;
        }
        else if (OB_SUCCESS != (ret = nb_accessor_.get_next_scanner(res)))
        {
          YYSYS_LOG(WARN, "fail to get next scanner:ret[%d]", ret);
          break;
        }
        else
        {
          ret = res->next_row();//iterator new scanner
        }
      }
      //mod:e
      if(OB_SUCCESS != ret)
      {
        YYSYS_LOG(ERROR, "failed to get next row of col checksum");
        break;
      }
      else if(OB_SUCCESS != (ret = res->get_row(&table_row)))
      {
        YYSYS_LOG(ERROR, "failed to get next row of col checksum");
        break;
      }
      else if(NULL != table_row)
      {
        if(OB_SUCCESS != (ret = table_row->get_cell_info(cell_index)->value_.get_varchar(tmp_string)))
        {
          YYSYS_LOG(ERROR, "failed calculate cchecksum of orginal_table_id:%ld", orginal_table_id);
          break;
        }
        else
        {
          //?????-������?column checksum
          cc.deepcopy(tmp_string.ptr(),(int32_t)tmp_string.length());
          ret = org_table_column_checksum.sum(cc, false);
          cc.reset();
          tmp_string.reset();
          src_cc_count++;
        }
      }
      else
      {
        ret = OB_ERROR;
        YYSYS_LOG(ERROR, "failed calculate cchecksum of orginal_table_id:%ld",orginal_table_id);
        break;
      }
    }
  }

  //add liumz, [bugfix: checksum memory overflow]20170106:b
  if(NULL != res)
  {
    nb_accessor_.release_query_res(res);
    res = NULL;
  }
  //add:e

  if (OB_SUCCESS != ret && OB_ITER_END != ret)
  {
    YYSYS_LOG(WARN, "failed to get org table cchecksum tabe_id:%ld", orginal_table_id);
  }
  else
  {
    //��������range ����index����column checksum����
    start_rowkey[0].set_int(index_table_id);
    end_rowkey[0].set_int(index_table_id);

    if(OB_ALL_CLUSTER_FLAG != cluster_id)
    {
        start_rowkey[1].set_int(cluster_id);
        end_rowkey[1].set_int(cluster_id);
    }
    else
    {
        start_rowkey[1].set_int(OB_START_CLUSTER_ID);
        end_rowkey[1].set_int(OB_MAX_CLUSTER_ID);
    }

    start_rowkey[2].set_int(current_version);
    end_rowkey[2].set_int(current_version);
    start_rowkey[3].set_min_value();
    end_rowkey[3].set_max_value();

    range.start_key_.assign(start_rowkey,rowkey_column_num);
    range.end_key_.assign(end_rowkey,rowkey_column_num);
    range.border_flag_.unset_inclusive_start();
    range.border_flag_.unset_inclusive_end();
    //      nb_accessor_.release_query_res(res);
    //      res = NULL;
  }
  if (OB_SUCCESS != ret && OB_ITER_END != ret)
  {
    YYSYS_LOG(ERROR, "failed to start scan index checksum ret = %d", ret);
  }
  else
  {
      if(OB_ALL_CLUSTER_FLAG != cluster_id)
      {
          ret = nb_accessor_.scan(res, OB_ALL_CCHECKSUM_INFO_TABLE_NAME, range, SC("column_checksum"));
      }
      else
      {
          ret = nb_accessor_.scan(res, OB_ALL_CCHECKSUM_INFO_TABLE_NAME, range, SC("column_checksum"), ScanConds("version", EQ, current_version), groupby_column);
      }
  }
  if(OB_SUCCESS != ret)
  {
      YYSYS_LOG(WARN,"fail to scan info of index checksum table_id=%ld cluster=%ld ret=%d", index_table_id, cluster_id, ret);
  }
  else
  {
    YYSYS_LOG(INFO, "start calculate cchecksum of index_table_id:%ld",index_table_id);
    //mod liumz, [bugfix: cchecksum too large]20161107:b
    //while(OB_ITER_END != (ret = res->next_row()))
    while(true)
    {
      ret = res->next_row();
      if (OB_ITER_END == ret)
      {
        bool is_fullfilled = false;
        int64_t fullfilled_item_num = 0;
        ret = res->get_scanner()->get_is_req_fullfilled(is_fullfilled, fullfilled_item_num);
        if(OB_SUCCESS != ret)
        {
          YYSYS_LOG(WARN, "get is req fullfilled fail:ret[%d]", ret);
          break;
        }
        else if (is_fullfilled)
        {
          ret = OB_ITER_END;
          break;
        }
        else if (OB_SUCCESS != (ret = nb_accessor_.get_next_scanner(res)))
        {
          YYSYS_LOG(WARN, "fail to get next scanner:ret[%d]", ret);
          break;
        }
        else
        {
          ret = res->next_row();//iterator new scanner
        }
      }
      //mod:e
      if(OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "fail to start get cchecksum info from orginal table");
        break;
      }
      else if(OB_SUCCESS != (ret = res->get_row(&table_row)))
      {
        YYSYS_LOG(WARN, "fail to start get cchecksum info from orginal table");
        break;
      }
      else if(NULL != table_row)
      {
        if(OB_SUCCESS != (ret = table_row->get_cell_info(cell_index)->value_.get_varchar(tmp_string)))
        {
          YYSYS_LOG(ERROR, "failed calculate cchecksum of index_table_id:%ld", index_table_id);
          break;
        }
        else
        {
          //????index������?column checksum
          cc.deepcopy(tmp_string.ptr(),(int32_t)tmp_string.length());
          ret = index_table_column_checksum.sum(cc, false);
          cc.reset();
          tmp_string.reset();
          index_cc_count++;
        }
      }
      else
      {
        ret = OB_ERROR;
        YYSYS_LOG(WARN, "failed calculate cchecksum of index_table_id:%ld", index_table_id);
        break;
      }
    }
  }

  //add liumz, [bugfix: checksum memory overflow]20170106:b
  if(NULL != res)
  {
    nb_accessor_.release_query_res(res);
    res = NULL;
  }
  //add:e

  //����??cchecksum
  if (OB_SUCCESS != ret && OB_ITER_END != ret)
  {
    YYSYS_LOG(ERROR, "can not compare this col checksum");
  }
  else
  {
    if(org_table_column_checksum.compare(index_table_column_checksum))
    {
      is_right = true;
      ret = OB_SUCCESS;
      YYSYS_LOG(INFO, "this index table column checksum is ok table_id:%ld",index_table_id);
    }
    else
    {
      is_right = false;
      ret = OB_ERROR;
      YYSYS_LOG(WARN, "this index table column checksum is incorrect. src table_id[%ld], src table_cc_count[%ld], index table_id[%ld], index table_cc_count[%ld]",
                orginal_table_id, src_cc_count, index_table_id, index_cc_count);
    }
  }

  //return OB_SUCCESS;
  return ret;
}

//add e

//[577] hard-code
void ObSchemaServiceImpl::add_all_all_group_comment(const ObString &function_name, char *temp_comment, const int64_t pos)
{
  ObString temp_function_name1;
  char temp_char1[OB_MAX_TABLE_NAME_LENGTH] = {0};
  snprintf(temp_char1, OB_MAX_TABLE_NAME_LENGTH, "boc_card_mixs");
  temp_function_name1.assign_ptr(temp_char1, static_cast<int32_t>(OB_MAX_TABLE_NAME_LENGTH));

  ObString temp_function_name2;
  char temp_char2[OB_MAX_TABLE_NAME_LENGTH] = {0};
  snprintf(temp_char2, OB_MAX_TABLE_NAME_LENGTH, "boc_account_mixs");
  temp_function_name2.assign_ptr(temp_char2, static_cast<int32_t>(OB_MAX_TABLE_NAME_LENGTH));

  if(function_name.compare(temp_function_name1) == 0)
  {
    switch(pos)
    {
      case 0:
        snprintf(temp_comment,OB_MAX_TABLE_NAME_LENGTH, "default");
        break;
      case 1:
        snprintf(temp_comment,OB_MAX_TABLE_NAME_LENGTH, "CCENO = 017 || 617");
        break;
      case 2:
        snprintf(temp_comment,OB_MAX_TABLE_NAME_LENGTH, "CCENO = 131 || 631");
        break;
      case 3:
        snprintf(temp_comment,OB_MAX_TABLE_NAME_LENGTH, "CCENO = 011 || 511 || 911");
        break;
      case 4:
        snprintf(temp_comment,OB_MAX_TABLE_NAME_LENGTH, "CCENO = 021 || 521");
        break;
      case 5:
        snprintf(temp_comment,OB_MAX_TABLE_NAME_LENGTH, "CCENO = 061 || 561");
        break;
      case 6:
        snprintf(temp_comment,OB_MAX_TABLE_NAME_LENGTH, "CCENO = 062 || 562");
        break;
      case 7:
        snprintf(temp_comment,OB_MAX_TABLE_NAME_LENGTH, "CCENO = 071 || 571");
        break;
      case 8:
        snprintf(temp_comment,OB_MAX_TABLE_NAME_LENGTH, "CCENO = 091 || 591 || 991");
        break;
      default:
        snprintf(temp_comment,OB_MAX_TABLE_NAME_LENGTH, "NULL");
        break;

    }
  }
  else if(function_name.compare(temp_function_name2) == 0)
  {
    switch(pos)
    {
      case 0:
        snprintf(temp_comment,OB_MAX_TABLE_NAME_LENGTH, "default");
        break;
      case 1:
        snprintf(temp_comment,OB_MAX_TABLE_NAME_LENGTH, "RGNNO = 331");
        break;
      case 2:
        snprintf(temp_comment,OB_MAX_TABLE_NAME_LENGTH, "RGNNO = 443");
        break;
      case 3:
        snprintf(temp_comment,OB_MAX_TABLE_NAME_LENGTH, "RGNNO = 310");
        break;
      case 4:
        snprintf(temp_comment,OB_MAX_TABLE_NAME_LENGTH, "RGNNO = 320");
        break;
      case 5:
        snprintf(temp_comment,OB_MAX_TABLE_NAME_LENGTH, "RGNNO = 421");
        break;
      case 6:
        snprintf(temp_comment,OB_MAX_TABLE_NAME_LENGTH, "RGNNO = 411");
        break;
      case 7:
        snprintf(temp_comment,OB_MAX_TABLE_NAME_LENGTH, "RGNNO = 441");
        break;
      case 8:
        snprintf(temp_comment,OB_MAX_TABLE_NAME_LENGTH, "RGNNO = 110");
        break;
      default:
        snprintf(temp_comment,OB_MAX_TABLE_NAME_LENGTH, "NULL");
        break;

    }
  }
  else
  {
    snprintf(temp_comment,OB_MAX_TABLE_NAME_LENGTH, "NULL");
  }
}


//add lqc [MultiUps 1.0] [#13] 20170405 b
int ObSchemaServiceImpl::update_all_all_group_mutator(const TableSchema& table_schema,
                                                      ObMutator* mutator,
                                                      int64_t &frozen_version,
                                                      ObArray<int32_t> *paxos_id_array,
                                                      ObArray<ObString> *group_name_list)
{
  int ret = OB_SUCCESS;

  if(NULL == mutator)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(WARN, "mutator is null");
  }
  else
  {
    if(OB_INVALID_ID != table_schema.ih_.tbl_tid || !group_name_list || !paxos_id_array)
    {
      //TODO NOTHING
    }
    else
    {
      if (( 0 == group_name_list->count()) && (0 == paxos_id_array->count()))
      {
        //TODO NOTHING
      }
      else if(group_name_list->count() != paxos_id_array->count())
      {
        ret = OB_ERROR;
        YYSYS_LOG(WARN," something arror");
      }
      else
      {
        ObObj ob_table_group_obj[2];
        ObRowkey group_row_key;
        for(int64_t i = 0 ;i <  group_name_list->count();i++)
        {
          //[577] hard-code :b
          char temp_comment[OB_MAX_TABLE_NAME_LENGTH] = {0};
          ObString temp_function_name;
          char temp_char[OB_MAX_TABLE_NAME_LENGTH] = {0};
          snprintf(temp_char, OB_MAX_TABLE_NAME_LENGTH, "%s", table_schema.partition_func_name_);
          temp_function_name.assign_ptr(temp_char, static_cast<int32_t>(OB_MAX_TABLE_NAME_LENGTH));
          add_all_all_group_comment(temp_function_name, temp_comment, i);
          //[577] hard-code:e
          ob_table_group_obj[0].set_varchar(group_name_list->at(i));
          ob_table_group_obj[1].set_int(frozen_version + 1);
          group_row_key.assign(ob_table_group_obj,2);
          ADD_INT(all_all_group_name,group_row_key,"paxos_id",paxos_id_array->at(i));

          ADD_VARCHAR(all_all_group_name, group_row_key, "reserver_field1", temp_comment);//[577] hard-code

          YYSYS_LOG(DEBUG,"get group_name ,ret = %d %s",group_name_list->at(i).length(),group_name_list->at(i).ptr());
        }
      }
    }
  }
  return ret;
}
int ObSchemaServiceImpl::alter_all_all_group_mutator(const AlterTableSchema& schema,
                                                     TableSchema& old_table_schema,
                                                     int64_t frozen_version,
                                                     ObArray<int32_t> *paxos_id_array,
                                                     ObArray<ObString> *group_name_list)
{
  int ret = OB_SUCCESS;
  yysys::CThreadGuard guard(&mutex_);
  ObMutator* mutator = NULL;
  if(OB_SUCCESS == ret)
  {
    mutator = GET_TSI_MULT(ObMutator, TSI_COMMON_MUTATOR_1);
    if(NULL == mutator)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      YYSYS_LOG(WARN, "get thread specific Mutator fail");
    }
  }
  if (OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = mutator->reset() ))
    {
      YYSYS_LOG(WARN, "reset ob mutator fail:ret[%d]", ret);
    }
    if (OB_SUCCESS != (ret= add_ddl_operation(mutator, schema.table_id_, ALTER_TABLE)))
    {
      YYSYS_LOG(WARN, "add ddl operation to mutator fail:ret[%d]", ret);
    }
    else
    {
      if (( 0 == group_name_list->count()) && (0 == paxos_id_array->count()))
      {
        //TODO NOTHING
      }
      else if(group_name_list->count() != paxos_id_array->count())
      {
        ret = OB_ERROR;
        YYSYS_LOG(WARN," something arror");
      }
      else
      {

        ObObj ob_table_group_obj[2];
        ObRowkey group_row_key;
        for(int64_t i = 0 ;i <  group_name_list->count();i++)
        {
          //[577] hard-code :b
          char temp_comment[OB_MAX_TABLE_NAME_LENGTH] = {0};
          ObString temp_function_name;
          char temp_char[OB_MAX_TABLE_NAME_LENGTH] = {0};
          snprintf(temp_char, OB_MAX_TABLE_NAME_LENGTH, "%s", schema.partition_func_name_);
          temp_function_name.assign_ptr(temp_char, static_cast<int32_t>(OB_MAX_TABLE_NAME_LENGTH));
          add_all_all_group_comment(temp_function_name, temp_comment, i);
          //[577] hard-code:e


          ob_table_group_obj[0].set_varchar(group_name_list->at(i));
          ob_table_group_obj[1].set_int(frozen_version+ 1);
          group_row_key.assign(ob_table_group_obj,2);
          ADD_INT(all_all_group_name,group_row_key,"paxos_id",paxos_id_array->at(i));

          ADD_VARCHAR(all_all_group_name, group_row_key, "reserver_field1", temp_comment);//[577] hard-code

          YYSYS_LOG(DEBUG,"get group_name ,ret = %d %s",group_name_list->at(i).length(),group_name_list->at(i).ptr());
        }
      }
    }
  }
  if ((OB_SUCCESS == ret) && (old_table_schema.schema_version_ >= 0))
  {
    ret = reset_schema_version_mutator(mutator, schema, old_table_schema.schema_version_);
  }
  if(OB_SUCCESS == ret)
  {
    ret = client_proxy_->mutate(*mutator);
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "apply mutator fail:ret[%d]", ret);
    }
  }
  return ret;
}
//add e

//[600]
int ObSchemaServiceImpl::alter_all_table_rules_mutator(const AlterTableSchema &schema,
                                                       TableSchema &old_table_schema,
                                                       int64_t frozen_version)
{
  int ret = OB_SUCCESS;
  yysys::CThreadGuard guard(&mutex_);
  ObMutator *mutator = NULL;
  if(OB_SUCCESS == ret)
  {
    mutator = GET_TSI_MULT(ObMutator, TSI_COMMON_MUTATOR_1);
    if(NULL == mutator)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      YYSYS_LOG(WARN, "get thread specific Mutator fail");
    }
  }
  if(OB_SUCCESS == ret)
  {
    if(OB_SUCCESS != (ret = mutator->reset()))
    {
      YYSYS_LOG(WARN, "reset ob mutator fail:ret[%d]", ret);
    }
    else
    {
      ObObj ob_table_rule_obj[2];
      ObRowkey table_rule_row_key;
      ob_table_rule_obj[0].set_int(schema.table_id_);
      ob_table_rule_obj[1].set_int(frozen_version + 2);
      table_rule_row_key.assign(ob_table_rule_obj, 2);

      if(OB_WITHOUT_PARTITION == schema.partition_type_ ||
         OB_DIRECT_PARTITION == schema.partition_type_)
      {
        if(OB_SUCCESS == ret)
        {
          ADD_VARCHAR(table_rule_name, table_rule_row_key, "table_name", schema.table_name_);
          ADD_INT(table_rule_name, table_rule_row_key, "partition_type", schema.partition_type_);
          ObTablePartitionType part_type = static_cast<ObTablePartitionType>(schema.partition_type_);
          if(OB_DIRECT_PARTITION == part_type)
          {
            ADD_VARCHAR(table_rule_name, table_rule_row_key, "prefix_name", schema.group_name_prefix_);
            ADD_VARCHAR(table_rule_name, table_rule_row_key, "rule_name", schema.partition_func_name_);
            ADD_VARCHAR(table_rule_name, table_rule_row_key, "par_list", schema.param_list_);
          }
          else if(OB_WITHOUT_PARTITION == part_type)
          {
            ADD_VARCHAR(table_rule_name, table_rule_row_key, "prefix_name", schema.group_name_prefix_);
            UPDATE_VARCHAR_TO_NULL(table_rule_name, table_rule_row_key, "rule_name");
            UPDATE_VARCHAR_TO_NULL(table_rule_name, table_rule_row_key, "par_list");
          }
        }
      }
    }
  }
  if((OB_SUCCESS == ret) && (old_table_schema.schema_version_ >= 0))
  {
    ret = reset_schema_version_mutator(mutator, schema, old_table_schema.schema_version_);
  }
  if(OB_SUCCESS == ret)
  {
    ret = client_proxy_->mutate(*mutator);
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "apply mutator fail:ret[%d]", ret);
    }
  }
  return ret;
}


//[view]
int ObSchemaServiceImpl::add_all_view(ObMutator *mutator, const TableSchema &table_schema)
{
    UNUSED(mutator);
    int ret = OB_SUCCESS;
    ObObj obj;
    obj.set_int(table_schema.table_id_);
    ObRowkey rowkey;
    rowkey.assign(&obj, 1);
    ADD_INT(all_view_name, rowkey, "check_option", table_schema.with_check_option_);
    ADD_INT(all_view_name, rowkey, "is_updatable", 1);
    ADD_VARCHAR(all_view_name, rowkey, "view_definition", table_schema.text_);
    return ret;
}
