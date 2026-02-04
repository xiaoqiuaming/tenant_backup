#include "sql_parser.tab.h"
#include "build_plan.h"
#include "dml_build_plan.h"
#include "priv_build_plan.h"
#include "ob_raw_expr.h"
#include "common/ob_bit_set.h"
#include "ob_select_stmt.h"
#include "ob_multi_logic_plan.h"
#include "ob_insert_stmt.h"
#include "ob_delete_stmt.h"
#include "ob_update_stmt.h"
#include "ob_schema_checker.h"
#include "ob_explain_stmt.h"
#include "ob_create_table_stmt.h"
#include "ob_drop_table_stmt.h"
#include "ob_truncate_table_stmt.h" //add zhaoqiong [Truncate Table]:20160318
#include "ob_show_stmt.h"
#include "ob_create_user_stmt.h"
#include "ob_prepare_stmt.h"
#include "ob_variable_set_stmt.h"
#include "ob_execute_stmt.h"
#include "ob_deallocate_stmt.h"
#include "ob_start_trans_stmt.h"
#include "ob_end_trans_stmt.h"
#include "ob_column_def.h"
#include "ob_alter_table_stmt.h"
#include "ob_alter_sys_cnf_stmt.h"
#include "ob_kill_stmt.h"
#include "parse_malloc.h"
#include "common/ob_define.h"
#include "common/ob_array.h"
#include "common/ob_string_buf.h"
#include "common/utility.h"
#include "common/ob_schema_service.h"
#include "common/ob_obi_role.h"
#include "ob_change_obi_stmt.h"
#include <stdint.h>
//add liumz, [multi_database.sql]:20150613
#include "ob_sql_session_info.h"
#include "parse_node.h"


//add wenghaixing [secondary index] 20141024
#include "ob_create_index_stmt.h"
#include "ob_drop_index_stmt.h"
//add e
#include "ob_create_part_func_stmt.h" //add liu jun. [MultiUPS] [sql_api] 20150421
#include "ob_drop_part_func_stmt.h" //add liu jun. [MultiUPS] [sql_api] 20150421
#include "ob_postfix_expression.h"//add wuna [MultiUPS] [sql_api] 20160105

#include <limits.h>
#include "common/Ob_Decimal.h"
#include "common/ob_obj_cast.h"
#include "ob_alter_group_stmt.h"
#include "ob_gather_statistics_stmt.h"
#include "ob_create_view_stmt.h"
#include "ob_rewrite_query_stmt.h"
#include "ob_drop_view_stmt.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

int resolve_multi_stmt(ResultPlan* result_plan, ParseNode* node);
int resolve_explain_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id);
int resolve_const_value(
    ResultPlan * result_plan,
    ParseNode *def_node,
    ObObj& default_value);

int resolve_const_value_v2(
    ResultPlan * result_plan,
    ParseNode *def_node,
    ObColumnDef& col_def);

int resolve_column_definition(
    ResultPlan * result_plan,
    ObColumnDef& col_def,
    ParseNode* node,
    bool *is_primary_key = NULL);
int resolve_table_elements(
    ResultPlan * result_plan,
    ObCreateTableStmt& create_table_stmt,
    ParseNode* node);
int resolve_create_table_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id);
//add by wenghaixing[secondary index] 20141024
int resolve_create_index_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id);
//add e
int resolve_drop_table_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id);

bool is_show_table_view_type(
    ResultPlan *result_plan,
    ParseNode *show_table_node);

int resolve_show_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id);
//add wenghaixing [secondary index drop index]20141222
int resolve_drop_index_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id
    );
//add e
int resolve_prepare_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id);
int resolve_variable_set_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id);
int resolve_execute_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id);
int resolve_deallocate_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id);
int resolve_alter_sys_cnf_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id);
int resolve_kill_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id);

int resolve_alter_group_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id);

//add liu jun. [MultiUPS] [sql_api] 20150421
/*add wuna [MultiUPS] [sql_api] 20151130:b*/
template <class T>
int resolve_table_hash_list(
    ResultPlan * result_plan,
    T*& stmt,
    ParseNode* node);
template <class T>
int generate_part_func_stmt(
    ResultPlan* result_plan,
    uint64_t& query_id,
    ObLogicalPlan* logical_plan,
    T* &create_part_func_stmt);
int resolve_table_list_partition(
    ResultPlan * result_plan,
    ObCreateTableStmt*& create_table_stmt,
    ObCreatePartFuncStmt* &create_part_func_stmt,
    ParseNode* node);
int resolve_table_range_partition(
    ResultPlan * result_plan,
    ObCreateTableStmt*& create_table_stmt,
    ObCreatePartFuncStmt* &create_part_func_stmt,
    ParseNode* node);
int resolve_partition_parameters_list(
    ResultPlan * result_plan,
    ObCreatePartFuncStmt& create_part_func_stmt,
    ObCreateTableStmt& create_table_stmt,
    ParseNode* node);
bool is_duplicate_partition_name(
    ObArray<ObString>& partition_name_list,
    const char* partition_name);
int resolve_partition_prefix_name(
    ResultPlan* result_plan,
    ParseNode* node,
    char* const group_name,
    const ObString& prefix_name);
int resolve_partition_body_expr(
    ResultPlan* result_plan,
    ParseNode* value,
    char* val_str,
    int64_t& val_str_len,
    ObObj& dest_obj,
    int64_t& op_row_num
    );
int resolve_table_list_definition_list(
    ResultPlan* result_plan,
    ObCreatePartFuncStmt& create_part_func_stmt,
    ObCreateTableStmt& create_table_stmt,
    ParseNode* node);
int resolve_table_range_definition_list(
    ResultPlan* result_plan,
    ObCreatePartFuncStmt& create_part_func_stmt,
    ObCreateTableStmt& create_table_stmt,
    ParseNode* node);
/*add 20151130:e*/
int resolve_create_table_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id);
int resolve_parameters_list(
    ResultPlan * result_plan,
    ObCreatePartFuncStmt& create_part_func_stmt,
    ParseNode* node
    );
int resolve_create_part_func_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id);
int resolve_drop_part_func_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id);
//add 20150319:e
int resolve_change_obi(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id);

int resolve_gather_statistics_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id);

int resolve_create_view_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id);

int resolve_view_elements(
    ResultPlan* result_plan,
    ParseNode* node,
    ObCreateViewStmt *create_view_stmt);

int resolve_drop_view_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id);

int resolve_view_column_attribute(
    ResultPlan *result_plan,
    ObShowStmt *show_stmt,
    ObSelectStmt *select_stmt);

int resolve_view_elements(
    ResultPlan *result_plan,
    ObLogicalPlan *logical_plan,
    ObSchemaChecker *schema_checker,
    ObSelectStmt *select_stmt,
    ObString *expr_name,
    ObSqlRawExpr *sql_expr,
    ObVector<ColumnItem *> &column_items,
    ObVector<TableItem *> &table_items,
    ObColumnDef *column_def);


int resolve_multi_stmt(ResultPlan* result_plan, ParseNode* node)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  OB_ASSERT(node && node->type_ == T_STMT_LIST);
  if(node->num_child_ == 0)
  {
    ret = OB_ERROR;
  }
  else
  {
    result_plan->plan_tree_ = NULL;
    ObMultiLogicPlan* multi_plan = (ObMultiLogicPlan*)parse_malloc(sizeof(ObMultiLogicPlan), result_plan->name_pool_);
    if (multi_plan != NULL)
    {
      multi_plan = new(multi_plan) ObMultiLogicPlan;
      for(int32_t i = 0; i < node->num_child_; ++i)
      {
        ParseNode* child_node = node->children_[i];
        if (child_node == NULL)
          continue;

        if ((ret = resolve(result_plan, child_node)) != OB_SUCCESS)
        {
          multi_plan->~ObMultiLogicPlan();
          parse_free(multi_plan);
          multi_plan = NULL;
          break;
        }
        if(result_plan->plan_tree_ == NULL)
          continue;

        if ((ret = multi_plan->push_back((ObLogicalPlan*)(result_plan->plan_tree_))) != OB_SUCCESS)
        {
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Can not add logical plan to YaoMultiLogicPlan");
          break;
        }
        result_plan->plan_tree_ = NULL;
      }
      result_plan->plan_tree_ = multi_plan;
    }
    else
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc space for YaoMultiLogicPlan");
    }
  }
  return ret;
}

int resolve_explain_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  OB_ASSERT(node && node->type_ == T_EXPLAIN && node->num_child_ == 1);
  ObLogicalPlan* logical_plan = NULL;
  ObExplainStmt* explain_stmt = NULL;
  query_id = OB_INVALID_ID;


  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
  if (result_plan->plan_tree_ == NULL)
  {
    logical_plan = (ObLogicalPlan*)parse_malloc(sizeof(ObLogicalPlan), result_plan->name_pool_);
    if (logical_plan == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc YaoLogicalPlan");
    }
    else
    {
      logical_plan = new(logical_plan) ObLogicalPlan(name_pool);
      result_plan->plan_tree_ = logical_plan;
    }
  }
  else
  {
    logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  }

  if (ret == OB_SUCCESS)
  {
    explain_stmt = (ObExplainStmt*)parse_malloc(sizeof(ObExplainStmt), result_plan->name_pool_);
    if (explain_stmt == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc YaoExplainStmt");
    }
    else
    {
      explain_stmt = new(explain_stmt) ObExplainStmt();
      query_id = logical_plan->generate_query_id();
      explain_stmt->set_query_id(query_id);
      ret = logical_plan->add_query(explain_stmt);
      if (ret != OB_SUCCESS)
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Can not add YaoDeleteStmt to logical plan");
      }
      else
      {
        if (node->value_ > 0)
          explain_stmt->set_verbose(true);
        else
          explain_stmt->set_verbose(false);

        uint64_t sub_query_id = OB_INVALID_ID;
        switch (node->children_[0]->type_)
        {
          case T_SELECT:
            ret = resolve_select_stmt(result_plan, node->children_[0], sub_query_id);
            break;
          case T_DELETE:
            ret = resolve_delete_stmt(result_plan, node->children_[0], sub_query_id);
            break;
          case T_INSERT:
            ret = resolve_insert_stmt(result_plan, node->children_[0], sub_query_id);
            break;
          case T_UPDATE:
            ret = resolve_update_stmt(result_plan, node->children_[0], sub_query_id);
            break;
          default:
            ret = OB_ERR_PARSER_SYNTAX;
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                     "Wrong statement in explain statement");
            break;
        }
        if (ret == OB_SUCCESS)
          explain_stmt->set_explain_query_id(sub_query_id);
      }
    }
  }
  return ret;
}

int resolve_column_definition(
    ResultPlan * result_plan,
    ObColumnDef& col_def,
    ParseNode* node,
    bool *is_primary_key)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  OB_ASSERT(node->type_ == T_COLUMN_DEFINITION);
  OB_ASSERT(node->num_child_ >= 3);
  if (is_primary_key)
    *is_primary_key = false;

  col_def.action_ = ADD_ACTION;
  OB_ASSERT(node->children_[0]->type_== T_IDENT);
  col_def.column_name_.assign_ptr(
        (char*)(node->children_[0]->str_value_),
        static_cast<int32_t>(strlen(node->children_[0]->str_value_))
        );

  ParseNode *type_node = node->children_[1];
  OB_ASSERT(type_node != NULL);
  //add liu jun.[fix create table bug] 20150519:b
  int32_t len = static_cast<int32_t>(strlen(node->children_[0]->str_value_));
  ObString column_name(len, len, node->children_[0]->str_value_);
  if(len >= OB_MAX_COLUMN_NAME_LENGTH)
  {
    ret = OB_ERR_COLUMN_NAME_LENGTH;
    PARSER_LOG("Column name is too long, column_name=%.*s", column_name.length(), column_name.ptr());
  }
  else
  {
    //20150519:e
    switch(type_node->type_)
    {
      case T_TYPE_INTEGER:
        //mod lijianqiang [INT_32] 20151008:b
        //col_def.data_type_ = ObIntType;
        col_def.data_type_ = ObInt32Type;
        break;
      case T_TYPE_BIG_INTEGER:
        col_def.data_type_ = ObIntType;
        break;
        //mod 20151008:e
      case T_TYPE_DECIMAL:
        col_def.data_type_ = ObDecimalType;
        if (type_node->num_child_ >= 1 && type_node->children_[0] != NULL)
          col_def.precision_ = type_node->children_[0]->value_;
        if (type_node->num_child_ >= 2 && type_node->children_[1] != NULL)
          col_def.scale_ = type_node->children_[1]->value_;
        //add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
        /*
         *������ʱ��Ҫ���û������decimal�Ĳ�������ȷ�Լ��
         * report wrong info when input precision<scale
         * */
        if (col_def.precision_ <= col_def.scale_||col_def.precision_>MAX_DECIMAL_DIGIT||col_def.scale_>MAX_DECIMAL_SCALE||col_def.precision_<=0||type_node->num_child_==0)
        {
          ret = OB_ERR_WRONG_DYNAMIC_PARAM;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "You have an error in your SQL syntax; check the param of decimal! precision = %ld,scale=%ld",
                   col_def.precision_, col_def.scale_);
        }
        //add:e
        break;
      case T_TYPE_BOOLEAN:
        col_def.data_type_ = ObBoolType;
        break;
      case T_TYPE_FLOAT:
        col_def.data_type_ = ObFloatType;
        if (type_node->num_child_ >= 1 && type_node->children_[0] != NULL)
          col_def.precision_ = type_node->children_[0]->value_;
        break;
      case T_TYPE_DOUBLE:
        col_def.data_type_ = ObDoubleType;
        break;
        //mod peiouya [DATE_TIME] 20150906:b
      case T_TYPE_DATE:
        //col_def.data_type_ = ObPreciseDateTimeType;
        col_def.data_type_ = ObDateType;
        break;
      case T_TYPE_TIME:
        //col_def.data_type_ = ObPreciseDateTimeType;
        col_def.data_type_ = ObTimeType;
        //del peiouya [DATE_TIME] 20150831:b
        /*
        if (type_node->num_child_ >= 1 && type_node->children_[0] != NULL)
          col_def.precision_ = type_node->children_[0]->value_;
        */
        //del 20150831:e
        break;
        //mod 20150906:e
      case T_TYPE_TIMESTAMP:
        col_def.data_type_ = ObPreciseDateTimeType;
        if (type_node->num_child_ >= 1 && type_node->children_[0] != NULL)
          col_def.precision_ = type_node->children_[0]->value_;
        break;
      case T_TYPE_CHARACTER:
        col_def.data_type_ = ObVarcharType;
        if (type_node->num_child_ >= 1 && type_node->children_[0] != NULL)
          col_def.type_length_= type_node->children_[0]->value_;
        else
          col_def.type_length_= 1;
        break;
      case T_TYPE_VARCHAR:
        col_def.data_type_ = ObVarcharType;
        if (type_node->num_child_ >= 1 && type_node->children_[0] != NULL)
          //����varchar�ĳ���
          col_def.type_length_= type_node->children_[0]->value_;
        else
        {
          ret = OB_ERR_PARSE_SQL;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "You have an error in your SQL syntax; Check the length of varchar type");
        }
        break;
      case T_TYPE_CREATETIME:
        col_def.data_type_ = ObCreateTimeType;
        break;
      case T_TYPE_MODIFYTIME:
        col_def.data_type_ = ObModifyTimeType;
        break;
      default:
        ret = OB_ERR_ILLEGAL_TYPE;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Unsupport data type of column definiton, column name = %s", node->children_[0]->str_value_);
        break;
    }
  }

  ParseNode *attrs_node = node->children_[2];
  for(int32_t i = 0; ret == OB_SUCCESS && attrs_node && i < attrs_node->num_child_; i++)
  {
    ParseNode* attr_node = attrs_node->children_[i];
    switch(attr_node->type_)
    {
      case T_CONSTR_NOT_NULL:
        col_def.not_null_ = true;
        break;
      case T_CONSTR_NULL:
        col_def.not_null_ = false;
        break;
      case T_CONSTR_AUTO_INCREMENT:
        if (col_def.data_type_ != ObIntType && col_def.data_type_ != ObFloatType
            && col_def.data_type_ != ObDoubleType && col_def.data_type_ != ObDecimalType
            && col_def.data_type_ != ObInt32Type)//add lijianqiang [INT_32] create table bug fix 20151020
        {
          ret = OB_ERR_PARSER_SYNTAX;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Incorrect column specifier for column '%s'", node->children_[0]->str_value_);
          break;
        }
        col_def.atuo_increment_ = true;
        break;
      case T_CONSTR_PRIMARY_KEY:
        if (is_primary_key != NULL)
        {

          *is_primary_key = true;
        }
        break;
      case T_CONSTR_DEFAULT:
        //ret = resolve_const_value(result_plan, attr_node, col_def.default_value_);
        if (col_def.data_type_ == ObInt32Type || col_def.data_type_ == ObIntType
            || col_def.data_type_ == ObFloatType || col_def.data_type_ == ObDoubleType
            || col_def.data_type_ == ObPreciseDateTimeType || col_def.data_type_ == ObVarcharType
            || col_def.data_type_ == ObDateType || col_def.data_type_ == ObTimeType
            || col_def.data_type_ == ObBoolType || col_def.data_type_ == ObDecimalType)
        {
          ret = resolve_const_value_v2(result_plan, attr_node, col_def);
        }
        else
        {
          ret = OB_ERR_PARSER_SYNTAX;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Incorrect default value type specifier or not support auto increment or primary key for column '%s'",
                   node->children_[0]->str_value_);
          break;
        }
        break;
      default:  // won't be here
        ret = OB_ERR_PARSER_SYNTAX;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Wrong column constraint");
        break;
    }
    //del peiouya [NotNULL_check] [JHOBv0.1] 20131208:b
    /*expr:Remove null check constraints codes associated with default value*/
    //if (ret == OB_SUCCESS && col_def.default_value_.get_type() == ObNullType
    // && (col_def.not_null_ || col_def.primary_key_id_ > 0))
    //{
    //  ret = OB_ERR_ILLEGAL_VALUE;
    //  snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
    //      "Invalid default value for '%s'", node->children_[0]->str_value_);
    //}
    //del 20131208:e
  }
  return ret;
}

int resolve_const_value(
    ResultPlan * result_plan,
    ParseNode *def_node,
    ObObj& default_value)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  if (def_node != NULL)
  {
    ParseNode *def_val = def_node;
    if (def_node->type_ == T_CONSTR_DEFAULT)
      def_val = def_node->children_[0];
    ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
    ObString str;
    ObObj val;
    switch (def_val->type_)
    {
      case T_INT:
        default_value.set_int(def_val->value_);
        break;
      case T_STRING:
      case T_BINARY:
        if ((ret = ob_write_string(*name_pool,
                                   ObString::make_string(def_val->str_value_),
                                   str)) != OB_SUCCESS)
        {
          PARSER_LOG("Can not malloc space for default value");
          break;
        }
        default_value.set_varchar(str);
        break;
      case T_DATE:
        default_value.set_precise_datetime(def_val->value_);
        break;
        //add peiouya [DATE_TIME] 20150912:b
      case T_DATE_NEW:
        default_value.set_date (def_val->value_);
        break;
      case T_TIME:
        default_value.set_time (def_val->value_);
        break;
        //add 20150912:e
      case T_FLOAT:
        default_value.set_float(static_cast<float>(atof(def_val->str_value_)));
        break;
      case T_DOUBLE:
        default_value.set_double(atof(def_val->str_value_));
        break;
      case T_DECIMAL: // set as string
        if ((ret = ob_write_string(*name_pool,
                                   ObString::make_string(def_val->str_value_),
                                   str)) != OB_SUCCESS)
        {
          PARSER_LOG("Can not malloc space for default value");
          break;
        }
        default_value.set_varchar(str);
        default_value.set_type(ObDecimalType);
        break;
      case T_BOOL:
        default_value.set_bool(def_val->value_ == 1 ? true : false);
        break;
      case T_NULL:
        default_value.set_type(ObNullType);
        break;
      default:
        ret = OB_ERR_ILLEGAL_TYPE;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Illigeal type of default value");
        break;
    }
  }
  return ret;
}

int resolve_const_value_v2(
    ResultPlan * result_plan,
    ParseNode *def_node,
    ObColumnDef &col_def)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  if (def_node != NULL)
  {
    ParseNode *def_val = def_node;
    if (def_node->type_ == T_CONSTR_DEFAULT)
      def_val = def_node->children_[0];
    ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
    ObString str;
    ObObj val;

    ObExprObj in;
    int real_type = 0;
    ObDecimal od;

    ObObj &default_value = col_def.default_value_;
    ObObjType column_type = col_def.data_type_;
    int64_t type_length = col_def.type_length_;

    switch (def_val->type_)
    {
      case T_INT:
      case T_DATE:
      case T_BOOL:
      case T_DATE_NEW:
      case T_TIME:
        if (column_type == ObIntType)
        {
          if (LONG_MIN <= def_val->value_ && def_val->value_ <= LONG_MAX)
          {
          }
          else
          {
            ret = OB_ERR_ILLEGAL_DEFAULT_VALUE;
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                     "default value out of range");
            break;
          }
        }
        else if (column_type == ObInt32Type)
        {
          if (INT_MIN <= def_val->value_ && def_val->value_ <= INT_MAX)
          {
          }
          else
          {
            ret = OB_ERR_ILLEGAL_DEFAULT_VALUE;
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                     "default value out of range");
            break;
          }
        }
        else if (column_type == ObDateType || column_type == ObTimeType)
        {
          ret = OB_ERR_ILLEGAL_DEFAULT_VALUE;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "the date or time type of default value can not be int");
          break;
        }

        char temp[OB_MAX_DEBUG_MSG_LEN];
        snprintf(temp, sizeof(temp), "%ld", def_val->value_);
        if ((ret = ob_write_string(*name_pool,
                                   ObString::make_string(temp),
                                   str)) != OB_SUCCESS)
        {
          PARSER_LOG("Can not malloc space for default value");
          break;
        }
        default_value.set_varchar(str);
        break;
      case T_STRING:
      case T_BINARY:
      case T_FLOAT:
      case T_DOUBLE:
      case T_DECIMAL:
        if ((ret = ob_write_string(*name_pool,
                                   ObString::make_string(def_val->str_value_),
                                   str)) != OB_SUCCESS)
        {
          PARSER_LOG("Can not malloc space for default value");
          break;
        }
        if (column_type == ObIntType || column_type == ObInt32Type)
        {
          ret = OB_ERR_ILLEGAL_DEFAULT_VALUE;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "default value is not int or bigint or tinyint or smallint or mediumint type");
          break;
        }
        else if (column_type == ObFloatType
                 || column_type == ObDoubleType
                 || column_type == ObDecimalType)
        {
          if (!(str.str_is_float_or_double()))
          {
            ret = OB_ERR_ILLEGAL_DEFAULT_VALUE;
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                     "default value is not float or double or real or decimal type");
            break;
          }
          else if (column_type == ObDecimalType)
          {
            ret = od.from(def_val->str_value_, strlen(def_val->str_value_));
            if (ret != OB_SUCCESS
                || od.get_precision() <= 0
                || col_def.scale_ < od.get_scale() || col_def.precision_ < od.get_precision()
                || (col_def.precision_ - col_def.scale_) < (od.get_precision() - od.get_scale()))
            {
              ret = OB_ERR_ILLEGAL_DEFAULT_VALUE;
              snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                       "default value of decimal is illegal");
              break;
            }
          }
        }
        else if (column_type == ObPreciseDateTimeType
                 || column_type == ObDateType
                 || column_type == ObTimeType)
        {
          in.set_varchar(str);
          real_type = in.check_real_type(str);
          if (-1 == real_type)
          {
            ret = OB_ERR_ILLEGAL_DEFAULT_VALUE;
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                     "default value is not datetime or timestamp type");
            break;
          }
          else if ((column_type == ObPreciseDateTimeType && real_type != TIMESTAMP_CHECK)
                   || (column_type == ObDateType && real_type != DATE_CHECK)
                   || (column_type == ObTimeType && real_type != TIME_CHECK))
          {
            
            ret = OB_ERR_ILLEGAL_DEFAULT_VALUE;
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                     "default value type not equal the column type");
            break;
          }
          else if (column_type == ObPreciseDateTimeType
                   && (ret = check_format_and_content(in, TIMESTAMP_CHECK)) != OB_SUCCESS)
          {
            ret = OB_ERR_ILLEGAL_DEFAULT_VALUE;
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                     "default value is not datetime or timestamp type");
            break;
          }
          else if (column_type == ObDateType
                   && (ret = check_format_and_content(in, DATE_CHECK)) != OB_SUCCESS)
          {
            ret = OB_ERR_ILLEGAL_DEFAULT_VALUE;
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                     "default value is not date type");
            break;
          }
          else if (column_type == ObTimeType
                   && (ret = check_format_and_content(in, TIME_CHECK)) != OB_SUCCESS)
          {
            ret = OB_ERR_ILLEGAL_DEFAULT_VALUE;
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                     "default value is not time type");
            break;
          }
        }
        else if (column_type == ObBoolType)
        {
          ret = OB_ERR_ILLEGAL_DEFAULT_VALUE;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "default value is not bool type");
          break;
        }
        else if (column_type == ObVarcharType)
        {
          if (type_length < str.length())
          {
            ret = OB_ERR_ILLEGAL_DEFAULT_VALUE;
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                     "length of default value is too long");
            break;
          }
        }
        default_value.set_varchar(str);
        break;
      case T_NULL:
        default_value.set_type(ObNullType);
        break;
      default:
        ret = OB_ERR_ILLEGAL_TYPE;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Illigeal type of default value");
        break;
    }
  }
  return ret;
}

int resolve_table_elements(
    ResultPlan * result_plan,
    ObCreateTableStmt& create_table_stmt,
    ParseNode* node)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  OB_ASSERT(node->type_ == T_TABLE_ELEMENT_LIST);
  OB_ASSERT(node->num_child_ >= 1);

  ParseNode *primary_node = NULL;
  for(int32_t i = 0; ret == OB_SUCCESS && i < node->num_child_; i++)
  {
    ParseNode* element = node->children_[i];
    if (OB_LIKELY(element->type_ == T_COLUMN_DEFINITION))
    {
      ObColumnDef col_def;
      bool is_primary_key = false;
      col_def.column_id_ = create_table_stmt.gen_column_id();
      if ((ret = resolve_column_definition(result_plan, col_def, element, &is_primary_key)) != OB_SUCCESS)
      {
        break;
      }
      else if (is_primary_key)
      {
        if (create_table_stmt.get_primary_key_size() > 0)
        {
          ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
          PARSER_LOG("Multiple primary key defined");
          break;
        }
        else if ((ret = create_table_stmt.add_primary_key_part(col_def.column_id_)) != OB_SUCCESS)
        {
          PARSER_LOG("Add primary key failed");
          break;
        }
        else
        {
          col_def.primary_key_id_ = create_table_stmt.get_primary_key_size();
        }
      }
      ret = create_table_stmt.add_column_def(*result_plan, col_def);
    }
    else if (element->type_ == T_PRIMARY_KEY)
    {
      if (primary_node == NULL)
      {
        primary_node = element;
      }
      else
      {
        ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
        PARSER_LOG("Multiple primary key defined");
      }
    }
    else
    {
      /* won't be here */
      OB_ASSERT(0);
    }
  }

  if (ret == OB_SUCCESS)
  {
    if (OB_UNLIKELY(create_table_stmt.get_primary_key_size() > 0 && primary_node != NULL))
    {
      ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
      PARSER_LOG("Multiple primary key defined");
    }
    else if (primary_node != NULL)
    {
      ParseNode *key_node = NULL;
      for(int32_t i = 0; ret == OB_SUCCESS && i < primary_node->children_[0]->num_child_; i++)
      {
        key_node = primary_node->children_[0]->children_[i];
        ObString key_name;
        key_name.assign_ptr(
              (char*)(key_node->str_value_),
              static_cast<int32_t>(strlen(key_node->str_value_))
              );
        ret = create_table_stmt.add_primary_key_part(*result_plan, key_name);
      }
    }
  }

  return ret;
}
//add liu jun.[MultiUPS] [sql_api] 20150422:b      //uncertainty :b
template <class T>
int resolve_table_hash_list(ResultPlan* result_plan, T *&stmt, ParseNode* node)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  int pos = 0;
  ObString group_name_prefix;
  /* mod by wuna[MultiUPS] [sql_api] 20151130:b*/
  //  if(NULL == node)
  if(0 == node->num_child_)
    /* mod 20151130:e*/
  {
    //only set the partition type
    stmt->set_partition_type(OB_WITHOUT_PARTITION);
    group_name_prefix.assign_ptr(
          const_cast<char*>(OB_DEFAULT_GROUP_NAME),
          static_cast<int32_t>(strlen(OB_DEFAULT_GROUP_NAME)));
    if(OB_SUCCESS != (ret = stmt->set_group_name_prefix(*result_plan, group_name_prefix)))
    {
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "set prefix group name failed");
    }
  }
  else
  {
    /* mod by wuna[MultiUPS] [sql_api] 20151130:b*/
    // OB_ASSERT(node->type_==T_TABLE_HASH_LIST);
    OB_ASSERT(node->type_==T_TABLE_HASH_LIST || node->type_==T_RULE_MODIFY);
    /* mod 20151130:e*/
    if(1 == node->num_child_)
    {
      stmt->set_partition_type(OB_WITHOUT_PARTITION);//add by wuna[MultiUPS] [sql_api] 20151130
      group_name_prefix.assign_ptr(
            (char *) node->children_[0]->str_value_,
            static_cast<int32_t>(strlen(node->children_[0]->str_value_)));
      // create_table_stmt->set_partition_type(OB_WITHOUT_PARTITION);//del by wuna[MultiUPS] [sql_api] 20151130
      if(OB_SUCCESS != (ret = stmt->set_group_name_prefix(*result_plan, group_name_prefix)))
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "set prefix group name failed");
      }
    }
    else
    {
      if(2 == node->num_child_)
      {
        group_name_prefix.assign_ptr(
              const_cast<char*>(stmt->get_table_name().ptr()),
              stmt->get_table_name().length());
      }
      else if(3 == node->num_child_)
      {
        ParseNode *group_node = node->children_[pos++];
        OB_ASSERT(group_node && group_node->type_ == T_IDENT);
        group_name_prefix.assign_ptr(
              (char *) group_node->str_value_,
              static_cast<int32_t>(strlen(group_node->str_value_)));
      }
      //2:set partition function name
      //check if exists in rootserver.
      ParseNode *func_node = node->children_[pos++];
      OB_ASSERT(func_node && func_node->type_ == T_IDENT);
      ObString hash_func_name;
      const char* part_func_name = func_node->str_value_;
      if(strlen(part_func_name) >= 2 && part_func_name[0] == '_' && part_func_name[1] == '_')
      {
        ret = OB_ERROR;
        YYSYS_LOG(USER_ERROR,"Invalid partition function name.");
      }
      else
      {
        hash_func_name.assign_ptr(
              (char *) func_node->str_value_,
              static_cast<int32_t>(strlen(func_node->str_value_)));
      }

      if(OB_SUCCESS == ret)
      {
        if(OB_SUCCESS != (ret = stmt->set_group_name_prefix(*result_plan, group_name_prefix)))
        {
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "set prefix group name failed");
        }
        else if(OB_SUCCESS != (ret = stmt->set_func_name(*result_plan, hash_func_name)))
        {
          //do nothing.Cause the things handle in the set_hasn_func_name
        }
        else
        {
          //3:add partition columns.
          ParseNode* column_list = node->children_[pos++];
          OB_ASSERT(column_list && T_COLUMN_LIST == column_list->type_);
          for(int32_t i = 0;ret == OB_SUCCESS && i < column_list->num_child_; i++)
          {
            ParseNode* col_node = column_list->children_[i];

            if(OB_LIKELY(col_node->type_ == T_IDENT))
            {
              ObString col_name;
              col_name.assign_ptr(
                    (char *) col_node->str_value_,static_cast<int32_t>(strlen(col_node->str_value_)));
              ret = stmt->add_col_list(*result_plan, col_name);
              if(ret != OB_SUCCESS)
              {
                YYSYS_LOG(WARN,"hash partition function add col list failed.");
              }
            }
            else
            {
              //Could't be here.
              OB_ASSERT(0);
            }
          }
          //4:set partition type
          if(OB_SUCCESS == ret)
          {
            stmt->set_partition_type(OB_DIRECT_PARTITION);
          }
        }
      }//end if(OB_SUCCESS == ret)
    }
  }
  return ret;
}
//add 20141118:e

/*add wuna [MultiUPS] [sql_api] 20151130:b*/
template <class T>
int generate_part_func_stmt(
    ResultPlan* result_plan,
    uint64_t& query_id,
    ObLogicalPlan* logical_plan,
    T* &part_func_stmt
    )
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
  part_func_stmt = (T*)parse_malloc(sizeof(T), result_plan->name_pool_);
  if (NULL == part_func_stmt)
  {
    ret = OB_ERR_PARSER_MALLOC_FAILED;
    snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
             "Can not malloc create_part_func_stmt");
  }
  else
  {
    part_func_stmt = new(part_func_stmt)T(name_pool);
    query_id = logical_plan->generate_query_id();
    part_func_stmt->set_query_id(query_id);
    ret = logical_plan->add_query(part_func_stmt);
    if (OB_SUCCESS != ret)
    {
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not add part_func_stmt to logical plan.");
    }
  }
  return ret;
}

int resolve_table_list_partition(
    ResultPlan* result_plan,
    ObCreateTableStmt* &create_table_stmt,
    ObCreatePartFuncStmt* &create_part_func_stmt,
    ParseNode* node)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  OB_ASSERT(T_LIST_PARTITION == node->type_ || T_LIST_COLUMNS_PARTITION == node->type_);
  //generate __all_partition_rules info.
  //1.set rule_name of __all_partition_rules.
  ObString rule_name;
  int32_t list_str_length = static_cast<int32_t>(strlen(list_pre_name_str));
  int64_t func_name_length = static_cast<int64_t>(list_str_length+1);
  char func_name[func_name_length];
  memset(func_name,0,func_name_length);
  memcpy(func_name, list_pre_name_str, list_str_length);
  rule_name.assign_ptr(func_name, static_cast<int32_t>(strlen(func_name)));
  ret = create_part_func_stmt->set_funcion_name(*result_plan, rule_name);
  if(OB_SUCCESS != ret)
  {
    YYSYS_LOG(WARN,"set_funcion_name failed.ret = %d",ret);
  }
  //2.set param info of __all_partition_rules.
  if(OB_SUCCESS == ret && node->children_[0])
  {
    OB_ASSERT(node->children_[0]->type_ == T_IDENT || node->children_[0]->type_ == T_COLUMN_LIST);//todo: support sys_func type.
    ret = resolve_partition_parameters_list(result_plan, *create_part_func_stmt,
                                            *create_table_stmt, node->children_[0]);
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN,"resolve_partition_parameters_list failed.ret = %d",ret);
    }
  }
  //3.set rule_body of __all_partition_rules.
  if(OB_SUCCESS == ret && node->children_[1])
  {
    OB_ASSERT(node->children_[1]->type_ == T_LIST_DEF_LIST);
    ret = resolve_table_list_definition_list(result_plan,*create_part_func_stmt,
                                             *create_table_stmt,node->children_[1]);
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN,"resolve_table_list_definition_list failed.ret = %d",ret);
    }
  }

  //4.set partition_type of __all_partition_rules;
  if(OB_SUCCESS == ret)
  {
    ObFunctionPartitionType type = LISTFUNC;
    create_part_func_stmt->set_partition_type(type);
  }
  //generate __all_table_rules info.
  //1.set rule_name of __all_table_rules.
  if(OB_SUCCESS == ret)
  {
    ret = create_table_stmt->set_func_name(*result_plan, rule_name);
  }
  if(OB_SUCCESS == ret)
  {
    //2.set para_list of __all_table_rules.
    //already set in the function "resolve_partition_parameters_list()"
    //3.set prefix_name of __all_table_rules.
    char list_group_name[OB_MAX_VARCHAR_LENGTH];
    memset(list_group_name, 0, OB_MAX_VARCHAR_LENGTH);
    ObString prefix_name;
    if(NULL == node->children_[2])
    {
      const ObString& table_name = create_table_stmt->get_table_name();
      prefix_name = const_cast<ObString&>(table_name);
    }
    else
    {
      prefix_name = ObString::make_string(node->children_[2]->str_value_);
    }
    if(OB_SUCCESS != (ret=resolve_partition_prefix_name(result_plan,node,list_group_name,
                                                        prefix_name)))
    {
      YYSYS_LOG(WARN,"resolve range partition prefix name failed,ret=%d.",ret);
    }
    if(OB_SUCCESS == ret)
    {
      ObString group_name_prefix;
      group_name_prefix.assign_ptr(list_group_name, static_cast<int32_t>(strlen(list_group_name)));
      if(OB_SUCCESS != (ret = create_table_stmt->set_group_name_prefix(*result_plan,group_name_prefix)))
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "set prefix group name failed");
      }
      else
      {
        //4.set partition_type of __all_table_rules.
        create_table_stmt->set_partition_type(OB_DIRECT_PARTITION);
      }
    }
  }
  return ret;
}

int resolve_table_range_partition(
    ResultPlan* result_plan,
    ObCreateTableStmt* &create_table_stmt,
    ObCreatePartFuncStmt* &create_part_func_stmt,
    ParseNode* node)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  OB_ASSERT(T_RANGE_PARTITION == node->type_ || T_RANGE_COLUMNS_PARTITION == node->type_);
  //generate __all_partition_rules info.
  //1.set rule_name of __all_partition_rules.
  ObString rule_name;
  int32_t range_str_length = static_cast<int32_t>(strlen(range_pre_name_str));
  int64_t func_name_length = static_cast<int64_t>(range_str_length+1);
  char func_name[func_name_length];
  memset(func_name,0,func_name_length);
  memcpy(func_name, range_pre_name_str, range_str_length);
  rule_name.assign_ptr(func_name, static_cast<int32_t>(strlen(func_name)));
  ret = create_part_func_stmt->set_funcion_name(*result_plan, rule_name);
  if(OB_SUCCESS != ret)
  {
    YYSYS_LOG(WARN,"set_funcion_name failed.ret = %d",ret);
  }
  //2.set param info of __all_partition_rules.
  if(OB_SUCCESS == ret && node->children_[0])
  {
    OB_ASSERT(node->children_[0]->type_ == T_IDENT || node->children_[0]->type_ == T_COLUMN_LIST);//todo: support sys_func type.
    ret = resolve_partition_parameters_list(result_plan, *create_part_func_stmt,
                                            *create_table_stmt, node->children_[0]);
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN,"resolve_partition_parameters_list failed.ret = %d",ret);
    }
  }
  //3.set rule_body of __all_partition_rules.
  if(OB_SUCCESS == ret && node->children_[1])
  {
    OB_ASSERT(node->children_[1]->type_ == T_RANGE_DEF_LIST);
    ret = resolve_table_range_definition_list(result_plan,*create_part_func_stmt,
                                              *create_table_stmt,node->children_[1]);
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN,"resolve_table_range_definition_list failed.ret = %d",ret);
    }
  }
  //4.check if each range partition value is strictly increasing.
  if(OB_SUCCESS == ret)
  {
    ret = create_part_func_stmt->check_range_partition_value(*result_plan);
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(ERROR,"check range partition value failed,ret=%d",ret);
      // snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
      //     "VALUES LESS THAN value must be strictly increasing for each partition.");
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "VALUES LESS THAN value must be strictly increasing for each partition and the last value must be maxvalue.");
    }
  }
  //5.set partition_type of __all_partition_rules;
  if(OB_SUCCESS == ret)
  {
    ObFunctionPartitionType type = RANGEFUNC;
    create_part_func_stmt->set_partition_type(type);
  }
  //generate __all_table_rules info.
  //1.set rule_name of __all_table_rules.
  if(OB_SUCCESS == ret)
  {
    ret = create_table_stmt->set_func_name(*result_plan, rule_name);
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN,"set_func_name failed.ret = %d",ret);
    }
  }

  if(OB_SUCCESS == ret)
  {
    //2.set para_list of __all_table_rules.
    //already set in the function "resolve_partition_parameters_list()"
    //3.set prefix_name of __all_table_rules.
    char range_group_name[OB_MAX_VARCHAR_LENGTH];
    memset(range_group_name, 0, OB_MAX_VARCHAR_LENGTH);
    ObString prefix_name;
    if(NULL == node->children_[2])
    {
      const ObString& table_name = create_table_stmt->get_table_name();
      prefix_name = const_cast<ObString&>(table_name);
    }
    else
    {
      prefix_name = ObString::make_string(node->children_[2]->str_value_);
    }
    if(OB_SUCCESS != (ret=resolve_partition_prefix_name(result_plan,node,range_group_name,
                                                        prefix_name)))
    {
      YYSYS_LOG(WARN,"resolve range partition prefix name failed,ret=%d.",ret);
    }
    if(OB_SUCCESS == ret)
    {
      ObString group_name_prefix;
      group_name_prefix.assign_ptr(range_group_name, static_cast<int32_t>(strlen(range_group_name)));
      if(OB_SUCCESS != (ret = create_table_stmt->set_group_name_prefix(*result_plan,group_name_prefix)))
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "set prefix group name failed");
      }
      else
      {
        //4.set partition_type of __all_table_rules.
        create_table_stmt->set_partition_type(OB_DIRECT_PARTITION);
      }
    }
  }
  return ret;
}

int resolve_partition_parameters_list(
    ResultPlan * result_plan,
    ObCreatePartFuncStmt& create_part_func_stmt,
    ObCreateTableStmt& create_table_stmt,
    ParseNode* node
    )
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  OB_ASSERT(node &&(node->type_ == T_IDENT || node->type_ == T_COLUMN_LIST));//todo:add sysfunc
  int32_t param_num = node->num_child_;
  ObString column_name;
  ObObjType data_type = ObMinType;
  ObPartitionForm part_form = create_table_stmt.get_partition_form();
  if(OB_RANGE_PARTITION == part_form || OB_LIST_PARTITION == part_form)//range,list partition.can be column name or specific sys_funcs
  {
    if (node->str_value_ == NULL)
    {
      ret = OB_ERR_NULL_POINTER;
      YYSYS_LOG(ERROR,"ret=%d", ret);
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "str value can not be null");
    }
    else
    {
      column_name = ObString::make_string(node->str_value_);
      data_type = ObMinType;
    }
    
    // column_name = ObString::make_string(node->str_value_);
    // data_type = ObMinType;
    if (OB_SUCCESS != ret)
    {
    }
    else
      if (OB_SUCCESS != (ret = create_table_stmt.check_column_name_and_get_data_type(*result_plan,column_name,data_type)))
      {
        YYSYS_LOG(ERROR," check_column_name failed,ret=%d",ret);
      }
    // add by maosy [MultiUps 1.0] [#12]
    //else if(data_type!= ObIntType)
    //else if(data_type!= ObIntType && data_type != ObInt32Type)//todo: support sys_func type.
    // add e
      else if (!create_table_stmt.is_valid_data_type(data_type))
      {
        ret = OB_ERR_INVALID_COLUMN_TYPE;
        // YYSYS_LOG(ERROR,"data_type must be int,ret=%d,date_type = %d",ret,data_type);
        YYSYS_LOG(ERROR,"data_type is invalid, ret=%d,date_type = %d",ret,data_type);
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Field '%.*s' is not an allowed type for this type of partitioning.",
                 column_name.length(), column_name.ptr());
      }
      else
      {
        param_num = 1;
        int32_t param_length = static_cast<int32_t>(strlen(node->str_value_));
        ObString param_name;
        param_name.assign_ptr((char *)node->str_value_,param_length);
        if(OB_SUCCESS != (ret = create_table_stmt.add_col_list(param_name)))
        {
          YYSYS_LOG(ERROR,"ret=%d",ret);
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Can not add parameter name to create table stmt.");
        }
        else if(OB_SUCCESS != (ret = create_part_func_stmt.add_param_name(*result_plan,param_name)))
        {
          YYSYS_LOG(ERROR,"Can not add parameter name to create partition function stmt,ret=%d",ret);
        }
      }
  }
  else//range,list columns partition.
  {
    if(OB_MAX_ROWKEY_COLUMN_NUMBER < param_num)
    {
      ret = OB_ERROR;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Parameter num should less than 17.");
    }
    for(int32_t i = 0;i < param_num && OB_SUCCESS == ret;i++)
    {
      ParseNode* param_node = node->children_[i];
      OB_ASSERT(T_IDENT==param_node->type_);
      column_name = ObString::make_string(param_node->str_value_);
      data_type = ObMinType;
      if (OB_SUCCESS != (ret = create_table_stmt.check_column_name_and_get_data_type(*result_plan,column_name,data_type)))
      {
        YYSYS_LOG(ERROR," check_column_name failed,ret=%d",ret);
      }
      else if(!(create_table_stmt.is_valid_data_type(data_type)))
      {
        ret = OB_ERR_INVALID_COLUMN_TYPE;
        YYSYS_LOG(ERROR,"data_type must be int,varchar,datetime,ret=%d",ret);
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Field '%.*s' is not an allowed type for this type of partitioning.",
                 column_name.length(), column_name.ptr());
        break;
      }
      else
      {
        int32_t param_length = static_cast<int32_t>(strlen(param_node->str_value_));
        ObString param_name;
        param_name.assign_ptr((char *)param_node->str_value_,param_length);
        if(OB_SUCCESS != (ret = create_table_stmt.add_col_list(param_name)))
        {
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Can not add parameter name to create table stmt.");
          break;
        }
        else if(OB_SUCCESS != (ret = create_part_func_stmt.add_param_name(*result_plan,param_name)))
        {
          YYSYS_LOG(ERROR,"Can not add parameter name to create partition function stmt,ret=%d",ret);
          break;
        }
      }
    }
  }
  if(OB_SUCCESS == ret)
  {
    create_part_func_stmt.set_parameters_num(param_num);
  }
  return ret;
}

bool is_duplicate_partition_name(ObArray<ObString>& partition_name_list,const char* partition_name)
{
  int ret = false;
  for(int64_t i = 0;i < partition_name_list.count();i++)
  {
    const ObString& temp_partition_name = partition_name_list.at(i);
    if(temp_partition_name == partition_name)
    {
      ret = true;
      YYSYS_LOG(ERROR,"duplicate partition name '%s' ",partition_name);
      break;
    }
  }
  return ret;
}

int resolve_partition_prefix_name(
    ResultPlan* result_plan,
    ParseNode* node,
    char* const group_name,
    const ObString& prefix_name
    )
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  int64_t prefix_name_max_length=OB_MAX_VARCHAR_LENGTH;
  int64_t len=0;
  int64_t prefix_name_len=static_cast<int64_t>(prefix_name.length());
  int32_t num=node->children_[1]->num_child_;
  ObArray<ObString> partition_name_list;
  for(int64_t i=0;OB_SUCCESS==ret && i<num;i++)
  {
    ParseNode* def_node=node->children_[1]->children_[i];
    const char* partition_name=def_node->children_[0]->str_value_;
    int64_t partition_name_length=strlen(partition_name);
    if(NULL == partition_name)
    {
      ret=OB_ERROR;
      YYSYS_LOG(ERROR,"partition name can't be null.");
      break;
    }
    else if(true == is_duplicate_partition_name(partition_name_list,partition_name))
    {
      ret=OB_ERROR;
      YYSYS_LOG(USER_ERROR," Duplicate partition name %s",partition_name);
      break;
    }
    else
    {
      partition_name_list.push_back(ObString::make_string(partition_name));
      if(0 == i)
      {
        if(len+partition_name_length+1+prefix_name_len < prefix_name_max_length)
        {
          strncpy(group_name,prefix_name.ptr(),prefix_name_len);
          strcat(group_name,"#");
          strcat(group_name,partition_name);
          len=len+partition_name_length+1+prefix_name_len;
        }
        else
        {
          ret = OB_ERR_PARAMETER_LIST;
          YYSYS_LOG(USER_ERROR,"Partition parameter list is too long,ret=%d.",ret);
          break;
        }
      }
      else
      {
        if(len+partition_name_length+1+prefix_name_len+1 < prefix_name_max_length)
        {
          strcat(group_name,",");
          strncat(group_name,prefix_name.ptr(),prefix_name_len);
          strcat(group_name,"#");
          strcat(group_name,partition_name);
          len=len+partition_name_length+1+prefix_name_len+1;
        }
        else
        {
          ret = OB_ERR_PARAMETER_LIST;
          YYSYS_LOG(USER_ERROR,"Partition parameter list is too long,ret=%d.",ret);
          break;
        }
      }
    }
  }
  return ret;
}

int resolve_partition_body_expr(
    ResultPlan* result_plan,
    ParseNode* value,
    char* val_str,
    int64_t& val_str_len,
    ObObj& dest_obj,
    int& op_row_num
    )
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  uint64_t value_expr_id = OB_INVALID_ID;
  const ObObj* value_obj = NULL;
  ObLogicalPlan* logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  ObStmt ob_stmt(static_cast<ObStringBuf*>(result_plan->name_pool_), ObBasicStmt::T_CREATE_PART_FUNC);
  if(OB_SUCCESS !=(ret = resolve_independ_expr(result_plan,&ob_stmt,
                                               value,value_expr_id)))
  {
    YYSYS_LOG(WARN,"resolve independent expr failed.ret=%d.",ret);
  }
  else
  {
    ObSqlRawExpr* raw_val_expr = NULL;
    ObSqlExpression val_expr;
    ObRow val_row;
    oceanbase::sql::ObPostfixExpression& post_expr = val_expr.get_decoded_expression_v2();
    if ((raw_val_expr = logical_plan->get_expr(value_expr_id)) == NULL)
    {
      ret = OB_ERR_ILLEGAL_ID;
      YYSYS_LOG(ERROR,"Wrong expression id, id=%lu.", value_expr_id);
    }
    else if(OB_SUCCESS != (ret = raw_val_expr->fill_sql_expression(val_expr, NULL, logical_plan, NULL,true)))
    {
      YYSYS_LOG(WARN, "Failed to fill expr, ret=%d", ret);
    }
    else if(OB_SUCCESS != (ret = post_expr.check_partition_body_expr(op_row_num)))
    {
      YYSYS_LOG(WARN, "check partition body expr failed. ret=%d", ret);
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Not supported feature or function.");
    }
    else
    {
      //err = post_expr.check_partition_body_expr_without_op_row();
      if(OB_SUCCESS != (ret = val_expr.calc(val_row,value_obj)))
      {
        YYSYS_LOG(WARN, "Calculate variable value failed. ret=%d", ret);
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Unexpected error happened in calculating partition rules.");
      }
      else
      {
        int64_t int_val=0;
        ObDateTime datetime_val=0;
        int32_t int32_val=0; //uncertainty add for Partition column values of incorrect type
        ObString varchar_val;
        switch(value_obj->get_type())
        {
          case ObIntType:
            value_obj->get_int(int_val);
            val_str_len = sprintf(val_str,"%ld",int_val);
            break;
          case ObVarcharType:
            value_obj->get_varchar(varchar_val);
            sprintf(val_str,"%s",varchar_val.ptr());
            val_str_len = varchar_val.length();
            break;
          case ObDateTimeType:
            value_obj->get_datetime(datetime_val);
            val_str_len = sprintf(val_str,"%ld",datetime_val);
            break;
          case ObInt32Type:  //uncertainty add for Partition column values of incorrect type b:
            value_obj->get_int32(int32_val);
            val_str_len = sprintf(val_str,"%d",int32_val);
            YYSYS_LOG(INFO,"the type is int32 %d",int32_val);
            break;
            // uncertainty e
          default:
            ret = OB_ERR_INVALID_COLUMN_TYPE;
            YYSYS_LOG(USER_ERROR,"Partition column values of incorrect type.");
            break;
        }
      }
    }
  }
  if(OB_SUCCESS == ret &&
     OB_SUCCESS != (ret=ob_write_obj(*(static_cast<ObStringBuf*>(result_plan->name_pool_)),
                                     *value_obj,dest_obj)))
  {
    YYSYS_LOG(WARN, "fail to write obj:ret[%d]", ret);
  }
  return ret;
}

/*fill column 'rule_body' of table __all_partition_rules.
  eg:CREATE TABLE employees (id INT NOT NULL,store_id INT PRIMARY KEY)
     PARTITION BY LIST (store_id) (PARTITION p0 VALUES LESS IN (1,2,3),
     PARTITION p1 VALUES IN (4,5,6),PARTITION p2 VALUES IN (100));
     then column 'rule_body' will be fill with '[(1),(2),(3)],[(4),(5),(6)],[(100)]'*/
int resolve_table_list_definition_list(
    ResultPlan* result_plan,
    ObCreatePartFuncStmt& create_part_func_stmt,
    ObCreateTableStmt& create_table_stmt,
    ParseNode* node
    )
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  int32_t list_def_num = node->num_child_;
  int32_t param_num = create_part_func_stmt.get_parameters_num();
  const ObArray<ObString>& col_list = create_part_func_stmt.get_all_parameters();
  int64_t rule_body_length = OB_MAX_VARCHAR_LENGTH;
  char rule_body[OB_MAX_VARCHAR_LENGTH]={0};
  memset(rule_body, 0, rule_body_length);
  int64_t len = 0;
  ObPartitionForm part_form = create_table_stmt.get_partition_form();
  if(OB_LIST_PARTITION == part_form && 1!=param_num)
  {
    ret=OB_ERR_PARTITION_INCONSISTENCY_NUM;
    YYSYS_LOG(ERROR,"param_num must be one,ret=%d",ret);
    snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
             "Inconsistency num in usage of column lists for partitioning.");
  }
  else
  {
    char list_val_str[OB_MAX_VARCHAR_LENGTH]={0};
    for(int32_t i=0; i < list_def_num && OB_SUCCESS == ret; i++)
    {
      if(i==0)
      {
        strcpy(rule_body,"[");
        len=len+1;
      }
      else
      {
        if(len+1<rule_body_length)
        {
          strcat(rule_body,"[");
          len=len+1;
        }
        else
        {
          ret = OB_ERR_PARAMETER_LIST;
          YYSYS_LOG(USER_ERROR,"List Partition rule_body is too long,ret=%d.",ret);
          break;
        }
      }
      ParseNode* list_def_node=node->children_[i];
      ParseNode* expr_list_node=list_def_node->children_[1];
      int32_t expr_list_child_num=expr_list_node->num_child_;
      for(int32_t j=0; j < expr_list_child_num && OB_SUCCESS == ret; j++)
      {
        if(len+1 < rule_body_length)
        {
          strcat(rule_body,"(");
          len=len+1;
        }
        else
        {
          ret = OB_ERR_PARAMETER_LIST;
          YYSYS_LOG(USER_ERROR,"List Partition rule_body is too long,ret=%d.",ret);
          break;
        }
        ParseNode* expr_value=expr_list_node->children_[j];
        const ObString& column_name = col_list.at(0);
        int64_t list_val_str_len=0;
        ObObj dest_obj;
        int op_row_num=0;
        ObObjType column_type = ObMinType;
        if(1 == param_num)//list partition
        {
          memset(list_val_str,0,OB_MAX_VARCHAR_LENGTH);
          if(OB_SUCCESS != (ret=resolve_partition_body_expr(result_plan,expr_value,list_val_str,list_val_str_len,
                                                            dest_obj,op_row_num)))
          {
            YYSYS_LOG(WARN, "fail to resolve partition body expr:ret[%d]", ret);
            break;
          }
          else if(OB_SUCCESS == ret && 0 < op_row_num)
          {
            ret = OB_NOT_SUPPORTED;
            YYSYS_LOG(WARN, "fail to resolve partition body expr:ret[%d]", ret);
            break;
          }
          else if(OB_SUCCESS != (ret=create_table_stmt.get_column_type_by_name(*result_plan,column_name,column_type)))
          {
            YYSYS_LOG(ERROR,"get cloumn data type failed,ret=%d",ret);
            break;
          }
          //mod lqc  [MultiUPS] [value type compare] 20170516:b
          //else if(!ObExprObj::can_compare(column_type,dest_obj.get_type()))
          else if(!ObExprObj::is_equal_part_value_type(column_type,dest_obj.get_type()))
          {//mod e
            ret=OB_ERR_INVALID_COLUMN_TYPE;
            YYSYS_LOG(ERROR,"column_type not equal list value type,column_type=%d,list_value_type=%d,ret=%d",
                      column_type,dest_obj.get_type(),ret);
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                     "Partition column values of incorrect type.");
            break;
          }
          else
          {
            if(len+list_val_str_len < rule_body_length)
            {
              memcpy(rule_body+len,list_val_str,list_val_str_len);
              len=len+list_val_str_len;
            }
            else
            {
              ret = OB_ERR_PARAMETER_LIST;
              YYSYS_LOG(USER_ERROR,"List Partition rule_body is too long,ret=%d.",ret);
              break;
            }
          }
        }
        else//list columns partition
        {
          /*to match the parameter's num.eg sql:
           create table t4(c1 int,c2 varchar(20),c3 int,primary key(c1,c2)) partition by list columns(c1,c1)
           (partition qq0 values in((1,'a'),(2,'b')),partition qq1 values in((3,'c'),(4,'d')));
           if expr_value_child_num not equal to columns param_num,then the param num must be inconsistent,
           if expr_value_child_num equal to columns param_num,the param num may be consistent,such as"... values in(1+2)"
           the expr '1+2' expr_value_child_num=2,but its actually one param,not two.so we should process this condition.
          */
          memset(list_val_str,0,OB_MAX_VARCHAR_LENGTH);
          int32_t expr_value_child_num=expr_value->num_child_;
          if(expr_value_child_num != param_num)
          {
            ret=OB_ERR_PARTITION_INCONSISTENCY_NUM;
            YYSYS_LOG(ERROR,"value num doesn't match param num ,value_num=%d,param_num=%d,ret=%d",
                      expr_value_child_num,param_num,ret);
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                     "Inconsistency num in usage of column lists for partitioning.");
            break;
          }
          //if the expr is valid just as it must be one row and the operator T_OP_ROW num is 1.
          if(OB_SUCCESS == (ret=resolve_partition_body_expr(result_plan,expr_value,list_val_str,list_val_str_len,
                                                            dest_obj,op_row_num)) && 1 == op_row_num)
          {
            YYSYS_LOG(DEBUG,"resolve_partition_body_expr succeed,ret=%d.",ret);
          }
          else if(OB_NOT_SUPPORTED != ret)
          {
            ret=OB_ERR_PARTITION_INCONSISTENCY_NUM;
            YYSYS_LOG(ERROR,"resolve_partition_body_expr failed,ret=%d.",ret);
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                     "Inconsistency num in usage of column lists for partitioning.");
            break;
          }
          for(int32_t k=0; k < expr_value_child_num && OB_SUCCESS == ret; k++)
          {
            memset(list_val_str,0,OB_MAX_VARCHAR_LENGTH);
            ParseNode* expr_child_value=expr_value->children_[k];
            const ObString& col_name = col_list.at(k);
            if(OB_SUCCESS != (ret=resolve_partition_body_expr(result_plan,expr_child_value,list_val_str,list_val_str_len,
                                                              dest_obj,op_row_num)))
            {
              YYSYS_LOG(WARN, "fail to resolve partition body expr:ret[%d]", ret);
              break;
            }
            else if(OB_SUCCESS == ret && 0 < op_row_num)
            {
              ret = OB_NOT_SUPPORTED;
              YYSYS_LOG(WARN, "fail to resolve partition body expr:ret[%d]", ret);
              break;
            }
            else if(OB_SUCCESS != (ret=create_table_stmt.get_column_type_by_name(*result_plan,col_name,column_type)))
            {
              YYSYS_LOG(ERROR,"get cloumn data type failed,ret=%d",ret);
              break;
            }
            //mod lqc  [MultiUPS] [value type compare] 20170516:b
            //else if(!ObExprObj::can_compare(column_type,dest_obj.get_type()))
            else if(!ObExprObj::is_equal_part_value_type(column_type,dest_obj.get_type()))
            {//mod e
              ret=OB_ERR_INVALID_COLUMN_TYPE;
              YYSYS_LOG(ERROR,"column_type not equal list value type,column_type=%d,list_value_type=%d,ret=%d",
                        column_type,dest_obj.get_type(),ret);
              snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                       "Partition column values of incorrect type.");
              break;
            }
            else
            {
              if(k==param_num-1 && len+list_val_str_len < rule_body_length)
              {
                memcpy(rule_body+len,list_val_str,list_val_str_len);
                len=len+list_val_str_len;
              }
              else if(len+list_val_str_len+1 < rule_body_length)
              {
                memcpy(rule_body+len,list_val_str,list_val_str_len);
                strcat(rule_body,",");
                len=len+list_val_str_len+1;
              }
              else
              {
                ret = OB_ERR_PARAMETER_LIST;
                YYSYS_LOG(USER_ERROR,"List Partition rule_body is too long,ret=%d.",ret);
                break;
              }
            }
          }//for k end.
        }
        if(OB_SUCCESS == ret)
        {
          if(j==expr_list_child_num-1 && len+1<rule_body_length)
          {
            strcat(rule_body,")");
            len=len+1;
          }
          else if(len+2<rule_body_length)
          {
            strcat(rule_body,")");
            strcat(rule_body,",");
            len=len+2;
          }
          else
          {
            ret = OB_ERR_PARAMETER_LIST;
            YYSYS_LOG(USER_ERROR,"List Partition rule_body is too long,ret=%d.",ret);
            break;
          }
        }
      }//for j end.
      if(OB_SUCCESS==ret)
      {
        if(i==list_def_num-1 && len+1<rule_body_length)
        {
          strcat(rule_body,"]");
          len=len+2;
        }
        else if(len+2<rule_body_length)
        {
          strcat(rule_body,"]");
          strcat(rule_body,",");
          len=len+2;
        }
        else
        {
          ret = OB_ERR_PARAMETER_LIST;
          YYSYS_LOG(USER_ERROR,"List Partition rule_body is too long,ret=%d.",ret);
          break;
        }
      }
    }//for i end.
  }
  if(OB_SUCCESS == ret)
  {
    ObString func_context;
    func_context.assign_ptr(rule_body,static_cast<int32_t>(strlen(rule_body)));
    ret = create_part_func_stmt.set_func_context(*result_plan,func_context);
  }
  return ret;
}

/*fill column 'rule_body' of table __all_partition_rules.
  eg:CREATE TABLE employees (id INT NOT NULL,store_id PRIMARY KEY)
     PARTITION BY RANGE (store_id) (PARTITION p0 VALUES LESS THAN (6),
     PARTITION p1 VALUES LESS THAN (11),PARTITION p2 VALUES LESS THAN MAXVALUE);
     then column 'rule_body' will be fill with '(6),(11),(MAXVALUE)'*/
int resolve_table_range_definition_list(
    ResultPlan* result_plan,
    ObCreatePartFuncStmt& create_part_func_stmt,
    ObCreateTableStmt& create_table_stmt,
    ParseNode* node
    )
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  int32_t range_def_num = node->num_child_;
  int32_t param_num = create_part_func_stmt.get_parameters_num();
  const ObArray<ObString>& col_list = create_part_func_stmt.get_all_parameters();
  int64_t rule_body_length = OB_MAX_VARCHAR_LENGTH;
  char rule_body[OB_MAX_VARCHAR_LENGTH] = {0};
  memset(rule_body, 0, rule_body_length);
  int64_t len = 0;
  ObPartitionForm part_form = create_table_stmt.get_partition_form();
  char range_val_str[OB_MAX_VARCHAR_LENGTH]={0};
  for(int32_t i=0; i < range_def_num && OB_SUCCESS == ret; i++)
  {
    ParseNode* range_def_node=node->children_[i];
    RangePartitionNode* range_part_node = NULL;
    range_part_node = static_cast<RangePartitionNode*>
                      (ob_malloc(sizeof(RangePartitionNode),ObModIds::OB_PARTITION_MANAGER));
    if (range_part_node == NULL)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      YYSYS_LOG(WARN,"fail to allocate buffer,ret=%d", ret);
      break;
    }
    else
    {
      range_part_node = new(range_part_node)RangePartitionNode();
      if(param_num != range_def_node->children_[1]->num_child_)
      {
        ret=OB_ERR_PARTITION_INCONSISTENCY_NUM;
        YYSYS_LOG(ERROR,"range value num not equal param num,range_value_num=%d,param_num=%d,ret=%d",
                  range_def_node->children_[1]->num_child_,param_num,ret);
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Inconsistency num in usage of column lists for partitioning.");
        break;
      }
      else if(OB_RANGE_PARTITION == part_form && 1!=range_def_node->children_[1]->num_child_)
      {
        ret=OB_ERR_PARTITION_INCONSISTENCY_NUM;
        YYSYS_LOG(ERROR,"param num must be one,ret=%d.",ret);
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Cannot have more than one value for this type of RANGE partitioning.");
        break;
      }
      else
      {
        if(i==0)
        {
          strcpy(rule_body,"(");
          len=len+1;
        }
        else
        {
          if(len+1<rule_body_length)
          {
            strcat(rule_body,"(");
            len=len+1;
          }
          else
          {
            ret = OB_ERR_PARAMETER_LIST;
            YYSYS_LOG(USER_ERROR,"Range Partition rule_body is too long,ret=%d.",ret);
            break;
          }
        }
        ParseNode* range_value_list=range_def_node->children_[1];
        for(int32_t j=0; j < param_num && OB_SUCCESS == ret; j++)
        {
          memset(range_val_str,0,OB_MAX_VARCHAR_LENGTH);
          const ObString& column_name = col_list.at(j);
          ParseNode* range_value=range_value_list->children_[j];
          int64_t range_val_str_len=0;
          ObObj dest_obj;
          int op_row_num=0;
          ObObjType column_type = ObMinType;
          if (T_TYPE_MAXVALUE != range_value->type_ )
          {
            if(OB_SUCCESS != (ret=resolve_partition_body_expr(result_plan,range_value,range_val_str,range_val_str_len,
                                                              dest_obj,op_row_num)))
            {
              YYSYS_LOG(WARN, "fail to resolve partition body expr:ret[%d]", ret);
              break;
            }
            else if(OB_SUCCESS == ret && 0 < op_row_num)
            {
              ret = OB_NOT_SUPPORTED;
              YYSYS_LOG(USER_ERROR, "Not supported feature or function");
              break;
            }
            else if(OB_SUCCESS != (ret=create_table_stmt.get_column_type_by_name(*result_plan,column_name,column_type)))
            {
              YYSYS_LOG(ERROR,"get cloumn data type failed,ret=%d",ret);
              break;
            }
            //mod lqc  [MultiUPS] [value type compare] 20170516:b
            //else if(!ObExprObj::can_compare(column_type,dest_obj.get_type()))
            else if(!ObExprObj::is_equal_part_value_type(column_type,dest_obj.get_type()))
            {//mod e
              ret=OB_ERR_INVALID_COLUMN_TYPE;
              YYSYS_LOG(ERROR,"column_type not equal range value type,column_type=%d,range_value_type=%d,ret=%d",
                        column_type,dest_obj.get_type(),ret);
              snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                       "Partition column values of incorrect type.");
              break;
            }
          }
          else//is maxvalue
          {
            memcpy(range_val_str,"maxvalue",strlen("maxvalue"));
            range_val_str_len=strlen(range_val_str);
            dest_obj.set_max_value();
          }
          if(OB_SUCCESS == ret)
          {
            if(OB_SUCCESS != (ret = range_part_node->add_partition_value_obj(&create_part_func_stmt,dest_obj)))
            {
              YYSYS_LOG(ERROR,"Range Partition node add_partition_value_obj failed,ret=%d",ret);
              break;
            }
            else
            {
              if(j==param_num-1 && len+range_val_str_len<rule_body_length)
              {
                memcpy(rule_body+len,range_val_str,range_val_str_len);
                len = len+range_val_str_len;
              }
              else if(len+1+range_val_str_len<rule_body_length)
              {
                memcpy(rule_body+len,range_val_str,range_val_str_len);
                strcat(rule_body,",");
                len = len+range_val_str_len+1;
              }
              else
              {
                ret = OB_ERR_PARAMETER_LIST;
                YYSYS_LOG(USER_ERROR,"Range Partition rule_body is too long,ret=%d.",ret);
                break;
              }
            }
          }
        }//for j end.
        if(OB_SUCCESS==ret)
        {
          ObString partition_name = ObString::make_string(range_def_node->children_[0]->str_value_);
          if(OB_SUCCESS != (ret = range_part_node->add_partition_name(&create_part_func_stmt,partition_name)))
          {
            YYSYS_LOG(ERROR,"Range Partition node add_partition_name failed,ret=%d.",ret);
            break;
          }
          else
          {
            create_part_func_stmt.add_range_partition_node(range_part_node);
            if(i==range_def_num-1 && len+1<rule_body_length)
            {
              strcat(rule_body,")");
              len=len+1;
            }
            else if(len+2<rule_body_length)
            {
              strcat(rule_body,")");
              strcat(rule_body,",");
              len=len+2;
            }
            else
            {
              ret = OB_ERR_PARAMETER_LIST;
              YYSYS_LOG(USER_ERROR,"Range Partition rule_body is too long,ret=%d.",ret);
              break;
            }
          }
        }
      }//end else
    }//end else range_part_node!=NULL
  }//for i end.
  if(OB_SUCCESS == ret)
  {
    ObString func_context;
    func_context.assign_ptr(rule_body,static_cast<int32_t>(strlen(rule_body)));
    ret = create_part_func_stmt.set_func_context(*result_plan,func_context);
  }
  return ret;
}
/*add 20151130:e*/     //uncertainty e

int resolve_create_table_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  //mod peiouya [MultiUPS] [sql_api] 20141118:b
  //OB_ASSERT(node && node->type_ == T_CREATE_TABLE && node->num_child_ == 4);
  OB_ASSERT(node && node->type_ == T_CREATE_TABLE && node->num_child_ == 5);
  //mod 20141118:e
  ObLogicalPlan* logical_plan = NULL;
  ObCreateTableStmt* create_table_stmt = NULL;
  query_id = OB_INVALID_ID;


  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
  if (result_plan->plan_tree_ == NULL)
  {
    logical_plan = (ObLogicalPlan*)parse_malloc(sizeof(ObLogicalPlan), result_plan->name_pool_);
    if (logical_plan == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc YaoLogicalPlan");
    }
    else
    {
      logical_plan = new(logical_plan) ObLogicalPlan(name_pool);
      result_plan->plan_tree_ = logical_plan;
    }
  }
  else
  {
    logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  }

  if (ret == OB_SUCCESS)
  {
    create_table_stmt = (ObCreateTableStmt*)parse_malloc(sizeof(ObCreateTableStmt), result_plan->name_pool_);
    if (create_table_stmt == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc YaoExplainStmt");
    }
    else
    {
      create_table_stmt = new(create_table_stmt) ObCreateTableStmt(name_pool);
      query_id = logical_plan->generate_query_id();
      create_table_stmt->set_query_id(query_id);
      ret = logical_plan->add_query(create_table_stmt);
      if (ret != OB_SUCCESS)
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Can not add YaoCreateTableStmt to logical plan");
      }
    }
  }

  if (ret == OB_SUCCESS)
  {
    if (node->children_[0] != NULL)
    {
      OB_ASSERT(node->children_[0]->type_ == T_IF_NOT_EXISTS);
      create_table_stmt->set_if_not_exists(true);
    }
    //add dolphin [database manager]@20150614:b           // uncertainty  �����޸�
    ParseNode* relation = node->children_[1];
    OB_ASSERT(relation->type_ == T_RELATION);
    ObString db_name;
    if(relation->children_[0] != NULL)
      db_name.assign_ptr((char*)(relation->children_[0]->str_value_),
                         static_cast<int32_t>(strlen(relation->children_[0]->str_value_)));

    if(db_name.length() <= 0)
    {
      db_name = static_cast<ObSQLSessionInfo*>(result_plan->session_info_)->get_db_name();
    }
    if(db_name.length() <= 0 || db_name.length() >= OB_MAX_DATBASE_NAME_LENGTH)//mod liumz [name_length]
    {
      ret = OB_INVALID_ARGUMENT;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "database name is not valid,you must specify the correct database name when create table.");
    }
    //add:e
    ObString table_name;
    //modify dolphin [database manager]@20150614:b
    /**
    table_name.assign_ptr(
        (char*)(node->children_[1]->str_value_),
        static_cast<int32_t>(strlen(node->children_[1]->str_value_))
        );
    */
    if(OB_SUCCESS == ret)
    {//add dolphin [database manager]@20150616
      table_name.assign_ptr(
            (char *) (node->children_[1]->children_[1]->str_value_),
            static_cast<int32_t>(strlen(node->children_[1]->children_[1]->str_value_))
            );
      //add dolphin [database manager]@20150616:b
      if (table_name.length() <= 0 || table_name.length()>= OB_MAX_TABLE_NAME_LENGTH)
      {//mod liumz [name_length]
        ret = OB_INVALID_ARGUMENT;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "table_name is invalid.");
      }
    }
    if(ret == OB_SUCCESS)
    {
      //add:e
      //add liumz, [database manager.dolphin_bug_fix]20150708:b
      if ((ret = create_table_stmt->set_db_name(*result_plan, db_name)) != OB_SUCCESS)
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Add db name to YaoCreateTableStmt failed");
      }
      //add:e
      if ((ret = create_table_stmt->set_table_name(*result_plan, table_name)) != OB_SUCCESS)
      {
        //snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
        //    "Add table name to ObCreateTableStmt failed");
      }
      /* del liumz, [database manager.dolphin_bug_fix]20150708
      //add dolphin [database manager]@20150625
      if ((ret = create_table_stmt->set_db_name(*result_plan, db_name)) != OB_SUCCESS) {
        //snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
        //    "Add table name to ObCreateTableStmt failed");
      }*/
    }
  }

  if (ret == OB_SUCCESS)
  {
    OB_ASSERT(node->children_[2]->type_ == T_TABLE_ELEMENT_LIST);
    ret = resolve_table_elements(result_plan, *create_table_stmt, node->children_[2]);
  }
  
  //add peiouya [MultiUPS] [sql_api] 20150422:b
  //add wuna [MultiUPS] [sql_api] 20151130:b
  if( OB_SUCCESS == ret)
  {
    if(NULL == node->children_[4])
    {
      //only set the partition type
      ObString group_name_prefix;
      create_table_stmt->set_partition_type(OB_WITHOUT_PARTITION);
      group_name_prefix.assign_ptr(
            const_cast<char*>(OB_DEFAULT_GROUP_NAME),
            static_cast<int32_t>(strlen(OB_DEFAULT_GROUP_NAME)));
      if(OB_SUCCESS != (ret = create_table_stmt->set_group_name_prefix(*result_plan, group_name_prefix)))
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "set prefix group name failed");
      }
    }
    else
    {
      ObCreatePartFuncStmt* create_part_func_stmt = NULL;
      uint64_t part_func_query_id=OB_INVALID_ID;
      switch(node->children_[4]->type_)
      {
        case T_TABLE_HASH_LIST:
          create_table_stmt->set_partition_form(OB_HASH_PARTITION);
          ret = resolve_table_hash_list(result_plan, create_table_stmt, node->children_[4]);
          if(OB_SUCCESS != ret)
          {
            YYSYS_LOG(WARN,"resolve_table_hash_list failed.ret = %d",ret);
          }
          break;
        case T_RANGE_PARTITION:
        case T_RANGE_COLUMNS_PARTITION:
          ret = generate_part_func_stmt(result_plan,part_func_query_id,logical_plan,create_part_func_stmt);
          if(OB_SUCCESS != ret )
          {
            YYSYS_LOG(ERROR,"generate create_part_func_stmt failed.ret = %d",ret);
          }
          else
          {
            if(T_RANGE_PARTITION == node->children_[4]->type_)
            {
              create_table_stmt->set_partition_form(OB_RANGE_PARTITION);
            }
            else
            {
              create_table_stmt->set_partition_form(OB_RANGE_COLUMNS_PARTITION);
            }
            create_table_stmt->set_part_func_query_id(part_func_query_id);
            ret = resolve_table_range_partition(result_plan, create_table_stmt,
                                                create_part_func_stmt,node->children_[4]);
            if(OB_SUCCESS != ret)
            {
              YYSYS_LOG(WARN,"resolve_table_range_partition failed.ret = %d",ret);
            }
          }
          break;
        case T_LIST_PARTITION:
        case T_LIST_COLUMNS_PARTITION:
          ret = generate_part_func_stmt(result_plan,part_func_query_id,logical_plan,create_part_func_stmt);
          if(OB_SUCCESS != ret )
          {
            YYSYS_LOG(ERROR,"generate create_part_func_stmt failed.ret = %d",ret);
          }
          else
          {
            if(T_LIST_PARTITION == node->children_[4]->type_)
            {
              create_table_stmt->set_partition_form(OB_LIST_PARTITION);
            }
            else
            {
              create_table_stmt->set_partition_form(OB_LIST_COLUMNS_PARTITION);
            }
            create_table_stmt->set_part_func_query_id(part_func_query_id);
            ret = resolve_table_list_partition(result_plan, create_table_stmt,
                                               create_part_func_stmt,node->children_[4]);
            if(OB_SUCCESS != ret)
            {
              YYSYS_LOG(WARN,"resolve_table_list_partition failed.ret = %d",ret);
            }
          }
          break;
        default:
          /* won't be here */
          ret = OB_ERROR;
          OB_ASSERT(0);
          break;
      }
    }
  }
  //add 20151130:e
  //add 20150422:e

  if (ret == OB_SUCCESS && node->children_[3])
  {
    OB_ASSERT(node->children_[3]->type_ == T_TABLE_OPTION_LIST);
    ObString str;
    ParseNode *option_node = NULL;
    int32_t num = node->children_[3]->num_child_;
    for (int32_t i = 0; ret == OB_SUCCESS && i < num; i++)
    {
      option_node = node->children_[3]->children_[i];
      switch (option_node->type_)
      {
        case T_JOIN_INFO:
          str.assign_ptr(
                const_cast<char*>(option_node->children_[0]->str_value_),
                static_cast<int32_t>(option_node->children_[0]->value_));
          if ((ret = create_table_stmt->set_join_info(str)) != OB_SUCCESS)
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                     "Set JOIN_INFO failed");
          break;
        case T_EXPIRE_INFO:
          if (0 == option_node->num_child_)
          {
            ret = OB_INVALID_ARGUMENT;
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                     "EXPIRE_INFO must have arguments");
            break;
          }
          else
          {
            str.assign_ptr(
                  (char*)(option_node->children_[0]->str_value_),
                  static_cast<int32_t>(strlen(option_node->children_[0]->str_value_))
                  );
            if ((ret = create_table_stmt->set_expire_info(str)) != OB_SUCCESS)
              snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                       "Set EXPIRE_INFO failed");
            break;
          }
        case T_TABLET_MAX_SIZE:
          create_table_stmt->set_tablet_max_size(option_node->children_[0]->value_);
          break;
        case T_TABLET_BLOCK_SIZE:
          create_table_stmt->set_tablet_block_size(option_node->children_[0]->value_);
          break;
        case T_TABLET_ID:
          create_table_stmt->set_table_id(
                *result_plan,
                static_cast<uint64_t>(option_node->children_[0]->value_)
                );
          break;
        case T_REPLICA_NUM:
          create_table_stmt->set_replica_num(static_cast<int32_t>(option_node->children_[0]->value_));
          break;
        case T_COMPRESS_METHOD:
          str.assign_ptr(
                (char*)(option_node->children_[0]->str_value_),
                static_cast<int32_t>(strlen(option_node->children_[0]->str_value_))
                );
          if ((ret = create_table_stmt->set_compress_method(str)) != OB_SUCCESS)
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                     "Set COMPRESS_METHOD failed");
          break;
        case T_USE_BLOOM_FILTER:
          create_table_stmt->set_use_bloom_filter(option_node->children_[0]->value_ ? true : false);
          break;
        case T_USE_BLOCK_CACHE:
          create_table_stmt->set_use_block_cache(option_node->children_[0]->value_ ? true : false);
          break;
        case T_CONSISTENT_MODE:
          create_table_stmt->set_consistency_level(option_node->value_);
          break;
        case T_COMMENT:
          str.assign_ptr(
                (char*)(option_node->children_[0]->str_value_),
                static_cast<int32_t>(strlen(option_node->children_[0]->str_value_))
                );
          if ((ret = create_table_stmt->set_comment_str(str)) != OB_SUCCESS)
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                     "Set COMMENT failed");
          break;
        default:
          /* won't be here */
          OB_ASSERT(0);
          break;
      }
    }
  }
  return ret;
}
//add by wenghaixing[secondary index] 20141024
int resolve_create_index_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{

  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_CREATE_INDEX && node->num_child_ == 4);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObCreateIndexStmt* create_index_stmt = NULL;
  ObLogicalPlan* logical_plan = NULL;
  query_id = OB_INVALID_ID;
  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
  ObString indexed_table;
  //add wenghaixing [secondary index drop index]20141223
  uint64_t src_tid = OB_INVALID_ID;
  //add e
  if (NULL == result_plan->plan_tree_)
  {
    logical_plan = (ObLogicalPlan*)parse_malloc(sizeof(ObLogicalPlan), result_plan->name_pool_);
    if (NULL == logical_plan)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc YaoLogicalPlan");
    }
    else
    {
      logical_plan = new(logical_plan) ObLogicalPlan(name_pool);
      result_plan->plan_tree_ = logical_plan;
    }
  }
  else
  {
    logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  }
  if(OB_SUCCESS == ret)
  {
    create_index_stmt = (ObCreateIndexStmt*)parse_malloc(sizeof(ObCreateIndexStmt),result_plan->name_pool_);
    if(NULL == create_index_stmt)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc YaoCreateIndexStmt");
    }
    else
    {
      create_index_stmt = new(create_index_stmt)ObCreateIndexStmt(name_pool);
      query_id = logical_plan->generate_query_id();
      create_index_stmt->set_query_id(query_id);
      if(OB_SUCCESS != (ret = logical_plan->add_query(create_index_stmt)))
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Can not add YaoCreateIndexStmt to logical plan");
      }
    }
  }
  if(OB_SUCCESS == ret&&node->children_[0])
  {

    //add by zhangcd [multi_database.secondary_index] 20150701:b
    ObString db_name;
    ObString table_name;
    ObString complete_table_name;
    ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
    ParseNode *table_node = node->children_[0];
    if(table_node->num_child_ != 2)
    {
      ret = OB_ERROR;
      PARSER_LOG("Parse failed!");
    }
    else if(table_node->children_[0] == NULL && table_node->children_[1] != NULL)
    {
      ObSQLSessionInfo* session_info = static_cast<ObSQLSessionInfo*>(result_plan->session_info_);
      db_name = session_info->get_db_name();
      table_name = ObString::make_string(table_node->children_[1]->str_value_);
      char *ct_name = NULL;
      ct_name = static_cast<char *>(name_pool->alloc(OB_MAX_COMPLETE_TABLE_NAME_LENGTH));
      if(NULL == ct_name)
      {
        ret = OB_ERROR;
        PARSER_LOG("Memory over flow!");
      }
      else if(db_name.length() >= OB_MAX_DATBASE_NAME_LENGTH)
      {
        ret = OB_ERROR;
        PARSER_LOG("database name is too long!");
      }
      else if(table_name.length() >= OB_MAX_TABLE_NAME_LENGTH)
      {
        ret = OB_ERROR;
        PARSER_LOG("table name is too long!");
      }

      if(OB_SUCCESS == ret)
      {
        complete_table_name.assign_buffer(ct_name, OB_MAX_COMPLETE_TABLE_NAME_LENGTH);
        // mod zhangcd [multi_database.seconary_index] 20150721:b
        //complete_table_name.concat(db_name, table_name, '.');
        complete_table_name.write(db_name.ptr(), db_name.length());
        complete_table_name.write(".", 1);
        complete_table_name.write(table_name.ptr(), table_name.length());
        // mod:e
      }
    }
    else if((table_node->children_[0] != NULL && table_node->children_[1] != NULL))
    {
      db_name = ObString::make_string(table_node->children_[0]->str_value_);
      table_name = ObString::make_string(table_node->children_[1]->str_value_);
      char *ct_name = NULL;
      ct_name = static_cast<char *>(name_pool->alloc(OB_MAX_COMPLETE_TABLE_NAME_LENGTH));
      if(NULL == ct_name)
      {
        ret = OB_ERROR;
        PARSER_LOG("Memory over flow!");
      }
      else if(db_name.length() >= OB_MAX_DATBASE_NAME_LENGTH)
      {
        ret = OB_ERROR;
        PARSER_LOG("database name is too long!");
      }
      else if(table_name.length() >= OB_MAX_TABLE_NAME_LENGTH)
      {
        ret = OB_ERROR;
        PARSER_LOG("table name is too long!");
      }

      if(OB_SUCCESS == ret)
      {
        complete_table_name.assign_buffer(ct_name, OB_MAX_COMPLETE_TABLE_NAME_LENGTH);
        // mod zhangcd [multi_database.seconary_index] 20150721:b
        //        complete_table_name.concat(db_name, table_name, '.');
        complete_table_name.write(db_name.ptr(), db_name.length());
        complete_table_name.write(".", 1);
        complete_table_name.write(table_name.ptr(), table_name.length());
        // mod:e
      }
    }
    else
    {
      ret = OB_ERROR;
      PARSER_LOG("Parse failed!");
    }
    //add:e

    //del by zhangcd [multi_database.secondary_index] 20150701:b
    /*
    indexed_table.assign_ptr(
                    (char*)(node->children_[0]->str_value_),
                    static_cast<int32_t>(strlen(node->children_[0]->str_value_))
                );
*/
    //del:e

    //add liumz, 20150725:b
    if (OB_SUCCESS == ret)
    {//add:e
      //add by zhangcd [multi_database.secondary_index] 20150701:b
      indexed_table = complete_table_name;
      //add:e

      if(OB_SUCCESS != (ret = create_index_stmt->set_idxed_name(*result_plan,indexed_table)))
      {
      }
      //add wenghaixing[secondary index drop index]20141223
      else
      {
        ObSchemaChecker* schema_checker = NULL;
        schema_checker = static_cast<ObSchemaChecker*>(result_plan->schema_checker_);
        if (NULL == schema_checker)
        {
          ret = OB_ERR_SCHEMA_UNSET;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Schema(s) are not set");
        }
        if (OB_SUCCESS == ret)
        {
          if((src_tid = schema_checker->get_table_id(indexed_table)) == OB_INVALID_ID)
          {
            ret = OB_ENTRY_NOT_EXIST;
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                     "the table to create index '%.*s' is not exist", indexed_table.length(), indexed_table.ptr());
          }
        }

      }
      //add e
    }
  }
  if(OB_SUCCESS == ret)
  {
    OB_ASSERT(node->children_[1]&&node->children_[2]);
    ObString index_lable;
    //add wenghaixing[secondary index drop index]20141223
    char str[OB_MAX_TABLE_NAME_LENGTH];
    memset(str,0,OB_MAX_TABLE_NAME_LENGTH);
    int64_t str_len = 0;
    //add e
    index_lable.assign_ptr(
          (char*)(node->children_[2]->str_value_),
          static_cast<int32_t>(strlen(node->children_[2]->str_value_))
          );
    //add wenghaixing[secondary index drop index]20141223
    if(OB_SUCCESS != (ret = generate_index_table_name(index_lable,src_tid,str,str_len)))
    {
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "index name is invalid!");
    }
    else
    {
      index_lable.reset();
      index_lable.assign_ptr(str,(int32_t)str_len);
    }
    //add e
    if(OB_SUCCESS == ret)
    {
      if (OB_SUCCESS != (ret = create_index_stmt->set_index_label(*result_plan,index_lable)))
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "invalid index table name,maybe tname[%s] is already exist!",str);
      }
      else
      {
        ParseNode *column_node = NULL;
        ObString index_column;
        /*
        if(0 == node->children_[1]->num_child_)
        {
          column_node=node->children_[1];
          index_column.reset();
          index_column.assign_ptr(
                          (char*)(column_node->str_value_),
                          static_cast<int32_t>(strlen(column_node->str_value_))
                          );
          if(OB_SUCCESS != (ret = create_index_stmt->add_index_colums(*result_plan,indexed_table,index_column)))
          {
             //snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
             //"failed to add_index_colums!");
          }
         }*/
        for(int32_t i = 0; i < node->children_[1]->num_child_; i ++)
        {
          column_node = node->children_[1]->children_[i];
          index_column.reset();
          index_column.assign_ptr(
                (char*)(column_node->str_value_),
                static_cast<int32_t>(strlen(column_node->str_value_))
                );
          if(OB_SUCCESS != (ret = create_index_stmt->add_index_colums(*result_plan,indexed_table,index_column)))
          {
            // snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
            //"failed to add_index_colums!");
            break;
          }
        }
      }
    }
  }
  if(OB_SUCCESS == ret)
  {
    ObString storing_column;
    ParseNode *column_node = NULL;
    //add wenghaixing[secondary index create fix]20141225
    if(NULL == node->children_[3])
    {
      create_index_stmt->set_has_storing(false);
    }
    else
      //add e
      /*
      if(0 == node->children_[3]->num_child_)
      {
        column_node = node->children_[3];
        storing_column.reset();
        storing_column.assign_ptr(
                        (char*)(column_node->str_value_),
                        static_cast<int32_t>(strlen(column_node->str_value_))
                        );
        if(OB_SUCCESS != (ret = create_index_stmt->set_storing_columns(*result_plan,indexed_table,storing_column)))
        {
               // snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                         //"failed to add_index_colums!");
        }
      }
      else */
    {
      for(int32_t i = 0; i < node->children_[3]->num_child_; i ++)
      {
        column_node = node->children_[3]->children_[i];
        storing_column.reset();
        storing_column.assign_ptr(
              (char*)(column_node->str_value_),
              static_cast<int32_t>(strlen(column_node->str_value_))
              );
        if(OB_SUCCESS != (ret = create_index_stmt->set_storing_columns(*result_plan,indexed_table,storing_column)))
        {
          // snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
          // "failed to add_index_colums!");
          break;
        }
      }
    };
  }
  return ret;
}
//add e
int resolve_alter_table_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_ALTER_TABLE && node->num_child_ == 2);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObAlterTableStmt* alter_table_stmt = NULL;
  if (OB_SUCCESS != (ret = prepare_resolve_stmt(result_plan, query_id, alter_table_stmt)))
  {
  }
  else
  {
    alter_table_stmt->set_name_pool(static_cast<ObStringBuf*>(result_plan->name_pool_));
    OB_ASSERT(node->children_[0]);
    //OB_ASSERT(node->children_[1] && node->children_[1]->type_ == T_ALTER_ACTION_LIST);
    OB_ASSERT(node->children_[1] && (node->children_[1]->type_ == T_ALTER_ACTION_LIST || node->children_[1]->type_ == T_ALTER_TABLE_OPTION_LIST));
    //add dolphin [database manager]@20150617:b
    ObString db_name;
    if(node->children_[0]->children_[0] != NULL)
      db_name.assign_ptr((char*)(node->children_[0]->children_[0]->str_value_),
                         static_cast<int32_t>(strlen(node->children_[0]->children_[0]->str_value_)));

    if(db_name.length() <= 0)
    {
      db_name = static_cast<ObSQLSessionInfo*>(result_plan->session_info_)->get_db_name();
    }
    if(db_name.length() <= 0 || db_name.length() >= OB_MAX_DATBASE_NAME_LENGTH)//mod liumz [name_length]
    {
      ret = OB_INVALID_ARGUMENT;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "database name is not valid,you must specify the correct database name when alter table.");
    }
    alter_table_stmt->set_db_name(*result_plan, db_name);
    //add:e
    //modify dolphin [database manager]@20150617:b
    /*    int32_t name_len= static_cast<int32_t>(strlen(node->children_[0]->str_value_));
    ObString table_name(name_len, name_len, node->children_[0]->str_value_);*/

    ObString table_name;

    if(OB_SUCCESS == ret)
    {
      if(node->children_[0]->children_[1] != NULL)
        table_name.assign_ptr(
              (char *) (node->children_[0]->children_[1]->str_value_),
              static_cast<int32_t>(strlen(node->children_[0]->children_[1]->str_value_)));
      if (table_name.length() <= 0 || table_name.length() >= OB_MAX_TABLE_NAME_LENGTH)//mod liumz
      {
        ret = OB_INVALID_ARGUMENT;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "table_name is invalid.");
      }
    }

    if(OB_SUCCESS == ret && T_ALTER_TABLE_OPTION_LIST == node->children_[1]->type_)
    {
      if (NULL != node->children_[1]->children_[0])
      {
        if (OB_SUCCESS != (ret = alter_table_stmt->init()))
        {
          PARSER_LOG("Init alter table stmt failed, ret=%d", ret);
        }
        else if (OB_SUCCESS == (ret = alter_table_stmt->set_table_name(*result_plan, table_name)))
        {
          ParseNode *table_option_action_node = node->children_[1]->children_[0];
          if (1 != table_option_action_node->num_child_)
          {
            ret = OB_INVALID_ARGUMENT;
            PARSER_LOG("Resolve SQL error,only can have one table option, type=%d", table_option_action_node->type_);
          }
          else
          {
            ObString expire_info;
            ParseNode *table_option_node = table_option_action_node->children_[0];
            switch (table_option_action_node->type_)
            {
              case T_TABLE_OPTION_DROP:
              {
                if (T_EXPIRE_INFO == table_option_node->type_)
                {
                  if (0 != table_option_node->num_child_)
                  {
                    ret = OB_INVALID_ARGUMENT;
                    PARSER_LOG("drop expire info doesn't have any arguments! ret=%d", ret);
                  }
                  else
                  {
                    alter_table_stmt->set_is_drop_expire_info();
                    if (OB_SUCCESS != (ret = alter_table_stmt->set_expire_info_null()))
                    {
                      YYSYS_LOG(WARN, "Set expire info failed, ret=%d", ret);
                    }
                  }
                }
                else
                {
                  ret = OB_NOT_SUPPORTED;
                  PARSER_LOG("not support this function, ret=%d", ret);
                }
                break;
              }
              case T_TABLE_OPTION_ADD:
              {
                if (T_EXPIRE_INFO == table_option_node->type_)
                {
                  if (0 == table_option_node->num_child_)
                  {
                    ret = OB_INVALID_ARGUMENT;
                    PARSER_LOG("add expire condition must have arguments, ret=%d", ret);
                  }
                  else
                  {
                    if (NULL != table_option_node->children_[0]->str_value_ &&
                        OB_MAX_EXPIRE_CONDITION_LENGTH > (static_cast<int32_t> (strlen(table_option_node->children_[0]->str_value_))))
                    {
                      expire_info.assign_ptr( (char *) (table_option_node->children_[0]->str_value_),
                                              static_cast<int32_t> (strlen(table_option_node->children_[0]->str_value_)) + 1);
                      alter_table_stmt->set_is_add_expire_info();
                      if (OB_SUCCESS != (ret = alter_table_stmt->set_new_expire_info(expire_info)))
                      {
                        YYSYS_LOG(WARN, "Set expire info failed, ret=%d", ret);
                      }
                    }
                    else
                    {
                      ret = OB_INVALID_ARGUMENT;
                      PARSER_LOG("expire condition is unqualified, ret=%d", ret);
                    }
                  }
                }
                else
                {
                  ret = OB_NOT_SUPPORTED;
                  PARSER_LOG("not support this function, ret=%d", ret);
                }
                break;
              }
              case T_TABLE_OPTION_ALTER:
              {
                if (T_EXPIRE_INFO == table_option_node->type_)
                {
                  if (0 == table_option_node->num_child_)
                  {
                    ret = OB_INVALID_ARGUMENT;
                    PARSER_LOG("alter expire condition must have arguments, ret=%d", ret);
                  }
                  else
                  {
                    if (NULL != table_option_node->children_[0]->str_value_ &&
                        OB_MAX_EXPIRE_CONDITION_LENGTH > (static_cast<int32_t> (strlen(table_option_node->children_[0]->str_value_))))
                    {
                      expire_info.assign_ptr( (char *) (table_option_node->children_[0]->str_value_),
                                              static_cast<int32_t> (strlen(table_option_node->children_[0]->str_value_)) + 1);
                      alter_table_stmt->set_is_expire_info_modify();
                      if (OB_SUCCESS != (ret = alter_table_stmt->set_new_expire_info(expire_info)))
                      {
                        YYSYS_LOG(WARN, "Set expire info failed, ret=%d", ret);
                      }
                    }
                    else
                    {
                      ret = OB_INVALID_ARGUMENT;
                      PARSER_LOG("expire condition is unqualified, ret=%d", ret);
                    }
                  }
                }
                else if (T_USE_BLOCK_CACHE == table_option_node->type_)
                {
                  if (0 == table_option_node->num_child_)
                  {
                    ret = OB_INVALID_ARGUMENT;
                    PARSER_LOG("alter load type must have arguments,ret=%d", ret);
                  }
                  else
                  {
                    OB_ASSERT(table_option_node->children_[0]->type_ == T_BOOL);
                    alter_table_stmt->set_is_load_type_modify();
                    if (1 == table_option_node->children_[0]->value_)
                    {
                      alter_table_stmt->set_is_block_cache_use(true);
                    }
                    else
                    {
                      alter_table_stmt->set_is_block_cache_use(false);
                    }
                  }
                }
                else
                {
                  ret = OB_NOT_SUPPORTED;
                  PARSER_LOG("not support this function, ret=%d", ret);
                }
                break;
              }
              default:
                ret = OB_INVALID_ARGUMENT;
                PARSER_LOG("Unkown alter table_option type, type=%d", table_option_action_node->type_);
                break;
            }
          }
        }
      }
    }
    else if(ret == OB_SUCCESS)
    {

      if ((ret = alter_table_stmt->init()) != OB_SUCCESS)
      {
        PARSER_LOG("Init alter table stmt failed, ret=%d", ret);
      }
      else if ((ret = alter_table_stmt->set_table_name(*result_plan, table_name)) == OB_SUCCESS )
      {
        for (int32_t i = 0; ret == OB_SUCCESS && i < node->children_[1]->num_child_; i++)
        {
          ParseNode *action_node = node->children_[1]->children_[i];
          if (action_node == NULL)
            continue;
          ObColumnDef col_def;
          switch (action_node->type_)
          {
            case T_TABLE_RENAME:
            {
              //modify dolphin [database manager]@20150618:b
              /*int32_t len = static_cast<int32_t>(strlen(action_node->children_[0]->str_value_));
              if (len >= static_cast<int32_t>(OB_MAX_TABLE_NAME_LENGTH))//add liuj [Alter_Rename] [JHOBv0.1] 20150104
              {
                ret = OB_ERR_TABLE_NAME_LENGTH;//add liuj [Alter_Rename] [JHOBv0.1] 20150104
                PARSER_LOG("New table name is too long");//add liuj [Alter_Rename] [JHOBv0.1] 20150104
              } else//add liuj [Alter_Rename] [JHOBv0.1] 20150104
              {
                ObString new_name(len, len, action_node->children_[0]->str_value_);
                alter_table_stmt->set_has_table_rename();//add liuj [Alter_Rename] [JHOBv0.1] 20150104
                ret = alter_table_stmt->set_new_table_name(*result_plan, new_name);
              }*/
              // int32_t len = static_cast<int32_t>(strlen(action_node->children_[0]->children_[0]->str_value_));
              if(action_node->children_[0]->children_[0]!= NULL)
              {
                if(db_name != ObString(0, static_cast<int32_t>(strlen(action_node->children_[0]->children_[0]->str_value_)),action_node->children_[0]->children_[0]->str_value_))
                {
                  ret = OB_INVALID_ARGUMENT;
                  PARSER_LOG("NEW table name must be in the same database as the original table");
                }
              }
              if(OB_SUCCESS == ret)
              {

                if (strlen(action_node->children_[0]->children_[1]->str_value_) >=
                    static_cast<int32_t>(OB_MAX_TABLE_NAME_LENGTH)
                    || strlen(action_node->children_[0]->children_[1]->str_value_) == 0)
                {
                  ret = OB_ERR_TABLE_NAME_LENGTH;
                  PARSER_LOG("New table name is too long or empty");
                }
                else
                {
                  alter_table_stmt->set_has_table_rename();
                  ret = alter_table_stmt->set_new_table_name(*result_plan, ObString(0, static_cast<int32_t>(strlen(
                                                                                                              action_node->children_[0]->children_[1]->str_value_)),
                                                                                    action_node->children_[0]->children_[1]->str_value_));
                }
              }
              //modfiy:e
              break;
            }
            case T_COLUMN_DEFINITION:
            {
              bool is_primary_key = false;
              int32_t len = static_cast<int32_t>(strlen(action_node->children_[0]->str_value_));
              ObString col_name(len, len, action_node->children_[0]->str_value_);
              if (len >= OB_MAX_COLUMN_NAME_LENGTH)
              {
                ret = OB_ERR_COLUMN_NAME_LENGTH;
                PARSER_LOG("Column name is too long, column_name=%.*s", col_name.length(), col_name.ptr());
              }
              //20150519:e
              else if ((ret = resolve_column_definition(
                          result_plan,
                          col_def,
                          action_node,
                          &is_primary_key)) != OB_SUCCESS)
              {
              }
              else if (is_primary_key)
              {
                ret = OB_ERR_MODIFY_PRIMARY_KEY;
                PARSER_LOG("New added column can not be primary key");
              }
              else
              {
                ret = alter_table_stmt->add_column(*result_plan, col_def);
              }
              break;
            }
            case T_COLUMN_DROP:
            {
              int32_t len = static_cast<int32_t>(strlen(action_node->children_[0]->str_value_));
              ObString table_name(len, len, action_node->children_[0]->str_value_);
              col_def.action_ = DROP_ACTION;
              col_def.column_name_ = table_name;
              switch (action_node->value_)
              {
                case 0:
                  col_def.drop_behavior_ = NONE_BEHAVIOR;
                  break;
                case 1:
                  col_def.drop_behavior_ = RESTRICT_BEHAVIOR;
                  break;
                case 2:
                  col_def.drop_behavior_ = CASCADE_BEHAVIOR;
                  break;
                default:
                  break;
              }
              ret = alter_table_stmt->drop_column(*result_plan, col_def);
              break;
            }
            case T_COLUMN_ALTER:
            {
              int32_t table_len = static_cast<int32_t>(strlen(action_node->children_[0]->str_value_));
              //ObString table_name(table_len, table_len, action_node->children_[0]->str_value_);
              ObString column_name(table_len, table_len, action_node->children_[0]->str_value_);
              col_def.action_ = ALTER_ACTION;
              col_def.column_name_ = column_name;

              OB_ASSERT(action_node->children_[1]);

              switch (action_node->children_[1]->type_)
              {
                case T_CONSTR_NOT_NULL:
                  col_def.not_null_ = true;
                  //add fyd [NotNULL_check] [JHOBv0.1] 20140108:b
                  col_def.action_ = ALTER_ACTION_NULL;
                  //add 20140108:e
                  break;
                case T_CONSTR_NULL:
                  col_def.not_null_ = false;
                  //add fyd [NotNULL_check] [JHOBv0.1] 20140108:b
                  col_def.action_ = ALTER_ACTION_NULL;
                  //add 20140108:e
                  break;
                case T_CONSTR_DEFAULT:
                {
                  col_def.action_ = ALTER_ACTION_DEFAULT;
                  ObString dt;
                  char buf[OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1] = {0};
                  dt.assign_buffer(buf,OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1);
                  dt.write(db_name.ptr(), db_name.length());
                  dt.write(".", 1);
                  dt.write(table_name.ptr(), table_name.length());

                  ObSchemaChecker* schema_checker = NULL;
                  const ObColumnSchemaV2* column_schema = NULL;
                  if ((schema_checker = static_cast<ObSchemaChecker*>((*result_plan).schema_checker_)) == NULL)
                  {
                    ret = OB_ERR_SCHEMA_UNSET;
                  }
                  else if ((col_def.column_id_ = schema_checker->get_column_id(dt, col_def.column_name_))
                           == OB_INVALID_ID)
                  {
                    ret = OB_ERR_COLUMN_DUPLICATE;
                  }
                  else if ((column_schema = schema_checker->get_column_schema(dt, col_def.column_name_)) == NULL
                           || (col_def.column_id_ = column_schema->get_id()) == OB_INVALID_ID)
                  {
                    ret = OB_ERR_COLUMN_UNKNOWN;
                  }
                  if (column_schema != NULL)
                  {
                    col_def.data_type_ = column_schema->get_type();
                    col_def.type_length_ = column_schema->get_size();
                  }
                  ret = resolve_const_value_v2(result_plan, action_node->children_[1], col_def);
                }
                  // ret = resolve_const_value(result_plan, action_node->children_[1], col_def.default_value_);
                  break;
                case T_ALTER_VARCHAR_LENGTH:
                  col_def.action_ = ALTER_VARCHAR_LENGTH;
                  OB_ASSERT(action_node->children_[1]->children_[0]);
                  col_def.type_length_ = action_node->children_[1]->children_[0]->value_;
                  break;
                case T_ALTER_DECIMAL_PRECISION:
                  col_def.action_ = ALTER_DECIMAL_PRECISION;
                  OB_ASSERT(action_node->children_[1]->children_[0]);
                  OB_ASSERT(action_node->children_[1]->children_[1]);
                  col_def.precision_ = action_node->children_[1]->children_[0]->value_;
                  col_def.scale_ = action_node->children_[1]->children_[1]->value_;
                  break;
                default:
                  /* won't be here */
                  ret = OB_ERR_RESOLVE_SQL;
                  PARSER_LOG("Unkown alter table alter column action type, type=%d",
                             action_node->children_[1]->type_);
                  break;
              }
              if (OB_SUCCESS == ret)
              {
                ret = alter_table_stmt->alter_column(*result_plan, col_def);
              }
              break;
            }
            case T_COLUMN_RENAME:
            {
              int32_t table_len = static_cast<int32_t>(strlen(action_node->children_[0]->str_value_));
              ObString table_name(table_len, table_len, action_node->children_[0]->str_value_);
              int32_t new_len = static_cast<int32_t>(strlen(action_node->children_[1]->str_value_));
              ObString new_name(new_len, new_len, action_node->children_[1]->str_value_);
              col_def.action_ = RENAME_ACTION;
              col_def.column_name_ = table_name;
              col_def.new_column_name_ = new_name;
              //add liu jun. fix rename bug. 20150519:b
              if (new_len >= OB_MAX_COLUMN_NAME_LENGTH)
              {
                ret = OB_ERR_COLUMN_NAME_LENGTH;
                PARSER_LOG("New column name is too long, column_name=%.*s", new_name.length(), new_name.ptr());
              }
              else
                ret = alter_table_stmt->rename_column(*result_plan, col_def);
              //20150519:e
              break;
            }
              /*add wuna [MultiUps] [sql_api] 20160108:b*/
            case T_RULE_MODIFY:
            {
              alter_table_stmt->set_is_rule_modify();
              ret = resolve_table_hash_list(result_plan, alter_table_stmt, action_node);
              if(OB_SUCCESS != ret)
              {
                YYSYS_LOG(WARN,"resolve_table_hash_list failed.ret = %d",ret);
              }
              break;
            }
              /*add 20160108:e*/
            default:
              /* won't be here */
              ret = OB_ERR_RESOLVE_SQL;
              PARSER_LOG("Unkown alter table action type, type=%d", action_node->type_);
              break;
          }
        }
      }
    }
  }
  return ret;
}

int resolve_drop_table_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  OB_ASSERT(node && node->type_ == T_DROP_TABLE && node->num_child_ == 2);
  ObLogicalPlan* logical_plan = NULL;
  ObDropTableStmt* drp_tab_stmt = NULL;
  query_id = OB_INVALID_ID;


  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
  if (result_plan->plan_tree_ == NULL)
  {
    logical_plan = (ObLogicalPlan*)parse_malloc(sizeof(ObLogicalPlan), result_plan->name_pool_);
    if (logical_plan == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc YaoLogicalPlan");
    }
    else
    {
      logical_plan = new(logical_plan) ObLogicalPlan(name_pool);
      result_plan->plan_tree_ = logical_plan;
    }
  }
  else
  {
    logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  }

  if (ret == OB_SUCCESS)
  {
    drp_tab_stmt = (ObDropTableStmt*)parse_malloc(sizeof(ObDropTableStmt), result_plan->name_pool_);
    if (drp_tab_stmt == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc YaoDropTableStmt");
    }
    else
    {
      drp_tab_stmt = new(drp_tab_stmt) ObDropTableStmt(name_pool);
      query_id = logical_plan->generate_query_id();
      drp_tab_stmt->set_query_id(query_id);
      if ((ret = logical_plan->add_query(drp_tab_stmt)) != OB_SUCCESS)
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Can not add YaoDropTableStmt to logical plan");
      }
    }
  }

  if (ret == OB_SUCCESS && node->children_[0])
  {
    drp_tab_stmt->set_if_exists(true);
  }
  if (ret == OB_SUCCESS)
  {
    OB_ASSERT(node->children_[1] && node->children_[1]->num_child_ > 0);
    //add dolphin [database manager]@20150614:b
    OB_ASSERT(node->children_[1]->type_ == T_TABLE_LIST);
    ObString db_name;
    char buf[OB_MAX_DATBASE_NAME_LENGTH + OB_MAX_TABLE_NAME_LENGTH + 1];
    ObString dt;
    dt.assign_buffer(buf,OB_MAX_DATBASE_NAME_LENGTH + OB_MAX_TABLE_NAME_LENGTH + 1);
    //add:e
    ParseNode *table_node = NULL;
    ObString table_name;
    for (int32_t i = 0; i < node->children_[1]->num_child_ && OB_SUCCESS == ret; i ++)
    {
      table_node = node->children_[1]->children_[i];
      //add liumz, bugfix: drop table t1 t2; 20150626:b
      if (T_RELATION != table_node->type_)
      {
        ret = OB_ERR_TABLE_UNKNOWN;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Unkown table name!");
      }
      else
      {//add:e
        //add dolphin [database manager]@20150617:b
        if(table_node->children_[0] != NULL)
          db_name.assign_ptr((char*)(table_node->children_[0]->str_value_),
                             static_cast<int32_t>(strlen(table_node->children_[0]->str_value_)));
        YYSYS_LOG(DEBUG,"resolve create stmt dbname is %.*s",db_name.length(),db_name.ptr());

        if(db_name.length() <= 0)
        {
          db_name = static_cast<ObSQLSessionInfo*>(result_plan->session_info_)->get_db_name();
        }
        if(db_name.length() <= 0)
        {
          ret = OB_INVALID_ARGUMENT;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "database name is null,you must specify the database when create table.");
        }
        //add:e
        //modify dolphin [database manager]@20150617:b

        /**table_name.assign_ptr(
          (char*)(table_node->str_value_),
          static_cast<int32_t>(strlen(table_node->str_value_))
          );
     */
        if(table_node->children_[1] != NULL)
          table_name.assign_ptr(
                (char*)(table_node->children_[1]->str_value_),
                static_cast<int32_t>(strlen(table_node->children_[1]->str_value_)));
        if (table_name.length() <= 0)
        {
          ret = OB_INVALID_ARGUMENT;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "table_name is null,you must specify the table_name when drop table.");
        }
        //add liumz, [check table name length]20151130:b
        else if (table_name.length() >= OB_MAX_TABLE_NAME_LENGTH)
        {
          ret = OB_INVALID_ARGUMENT;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "table name is too long");
        }
        else
        {//add:e
          dt.concat(db_name, table_name);
          //modify:e
          if (OB_SUCCESS != (ret = drp_tab_stmt->add_table_name_id(
                               *result_plan, /**modify dolphin [database manager]@20150617 table_name */dt)))
          {
            break;
          }
        }
      }
    }
  }
  return ret;
}

//add zhaoqiong [Truncate Table]:20160318:b
int resolve_truncate_table_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  OB_ASSERT(node && node->type_ == T_TRUNCATE_TABLE && node->num_child_ == 3);
  ObLogicalPlan* logical_plan = NULL;
  ObTruncateTableStmt* trun_tab_stmt = NULL;
  query_id = OB_INVALID_ID;


  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
  if (result_plan->plan_tree_ == NULL)
  {
    logical_plan = (ObLogicalPlan*)parse_malloc(sizeof(ObLogicalPlan), result_plan->name_pool_);
    if (logical_plan == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc YaoLogicalPlan");
    }
    else
    {
      logical_plan = new(logical_plan) ObLogicalPlan(name_pool);
      result_plan->plan_tree_ = logical_plan;
    }
  }
  else
  {
    logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  }

  if (ret == OB_SUCCESS)
  {
    trun_tab_stmt = (ObTruncateTableStmt*)parse_malloc(sizeof(ObTruncateTableStmt), result_plan->name_pool_);
    if (trun_tab_stmt == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc YaoTruncateTableStmt");
    }
    else
    {
      trun_tab_stmt = new(trun_tab_stmt) ObTruncateTableStmt(name_pool);
      query_id = logical_plan->generate_query_id();
      trun_tab_stmt->set_query_id(query_id);
      if ((ret = logical_plan->add_query(trun_tab_stmt)) != OB_SUCCESS)
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Can not add YaoDropTableStmt to logical plan");
      }
    }
  }

  if (ret == OB_SUCCESS && node->children_[0])
  {
    trun_tab_stmt->set_if_exists(true);
  }
  if (ret == OB_SUCCESS)
  {
    OB_ASSERT(node->children_[1] && node->children_[1]->num_child_ > 0);
    //add dolphin [database manager]@20150614:b
    OB_ASSERT(node->children_[1]->type_ == T_TABLE_LIST);
    ObString db_name;
    char buf[OB_MAX_DATBASE_NAME_LENGTH + OB_MAX_TABLE_NAME_LENGTH + 1];
    ObString dt;
    dt.assign_buffer(buf,OB_MAX_DATBASE_NAME_LENGTH + OB_MAX_TABLE_NAME_LENGTH + 1);
    //add:e
    ParseNode *table_node = NULL;
    ObString table_name;
    for (int32_t i = 0; i < node->children_[1]->num_child_ && OB_SUCCESS == ret; i ++)
    {
      table_node = node->children_[1]->children_[i];
      if (T_RELATION != table_node->type_)
      {
        ret = OB_ERR_TABLE_UNKNOWN;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Unkown table name!");
      }
      else
      {
        //add dolphin [database manager]@20150617:b
        if(table_node->children_[0] != NULL)
          db_name.assign_ptr((char*)(table_node->children_[0]->str_value_),
                             static_cast<int32_t>(strlen(table_node->children_[0]->str_value_)));
        YYSYS_LOG(DEBUG,"resolve create stmt dbname is %.*s",db_name.length(),db_name.ptr());

        if(db_name.length() <= 0)
        {
          db_name = static_cast<ObSQLSessionInfo*>(result_plan->session_info_)->get_db_name();
        }
        if(db_name.length() <= 0)
        {
          ret = OB_INVALID_ARGUMENT;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "database name is null,you must specify the database when create table.");
        }
        //add:e
        //modify dolphin [database manager]@20150617:b

        /**table_name.assign_ptr(
          (char*)(table_node->str_value_),
          static_cast<int32_t>(strlen(table_node->str_value_))
          );
     */
        if(table_node->children_[1] != NULL)
          table_name.assign_ptr(
                (char*)(table_node->children_[1]->str_value_),
                static_cast<int32_t>(strlen(table_node->children_[1]->str_value_)));
        if (table_name.length() <= 0)
        {
          ret = OB_INVALID_ARGUMENT;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "table_name is null,you must specify the table_name when truncate table.");
        }
        //add liumz, [check table name length]20151130:b
        else if (table_name.length() >= OB_MAX_TABLE_NAME_LENGTH)
        {
          ret = OB_INVALID_ARGUMENT;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "table name is too long");
        }
        else
        {//add:e
          dt.concat(db_name, table_name);
          //modify:e
          if (OB_SUCCESS != (ret = trun_tab_stmt->add_table_name_id(
                               *result_plan, /**modify dolphin [database manager]@20150617 table_name */dt)))
          {
            YYSYS_LOG(WARN,"add table name id failed ret=[%d]",ret);
            break;
          }
        }
      }
    }
  }
  if (ret == OB_SUCCESS && node->children_[2])
  {

    ObString comment;
    if (static_cast<int32_t>(strlen(node->children_[2]->str_value_)) <= OB_MAX_TABLE_COMMENT_LENGTH)
    {
      if (OB_SUCCESS != (ret = ob_write_string(
                           *name_pool,
                           ObString::make_string(node->children_[2]->str_value_),
                           comment)))
      {
        PARSER_LOG("Can not malloc space for comment");
      }
    }
    else
    {
      if (OB_SUCCESS != (ret = ob_write_string(
                           *name_pool,
                           ObString::make_string(node->children_[2]->str_value_),
                           comment,0,static_cast<int32_t>(OB_MAX_TABLE_COMMENT_LENGTH))))
      {
        PARSER_LOG("Can not malloc space for comment");
      }
    }
    if (ret == OB_SUCCESS)
    {
      trun_tab_stmt->set_comment(comment);
    }

  }
  return ret;
}
//add:e


//add wenghaixing[secondary index drop index]20141222
int resolve_drop_index_stmt(ResultPlan *result_plan, ParseNode *node, uint64_t &query_id)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  OB_ASSERT(node && node->type_ == T_DROP_INDEX && node->num_child_ == 2);
  ObLogicalPlan* logical_plan = NULL;
  ObDropIndexStmt* drp_idx_stmt = NULL;
  query_id = OB_INVALID_ID;
  uint64_t data_table_id = OB_INVALID_ID;
  ObSchemaChecker* schema_checker=NULL;
  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
  //modify zhangcd [multi_datbase.secondary_index] 20150703:b
  //char tname_str[OB_MAX_DATBASE_NAME_LENGTH];
  char tname_str[OB_MAX_COMPLETE_TABLE_NAME_LENGTH];
  ObString db_name;
  //modify:e
  int64_t str_len=0;
  if (NULL == result_plan->plan_tree_)
  {
    logical_plan = (ObLogicalPlan*)parse_malloc(sizeof(ObLogicalPlan), result_plan->name_pool_);
    if (NULL == logical_plan)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc YaoLogicalPlan");
    }
    else
    {
      logical_plan = new(logical_plan) ObLogicalPlan(name_pool);
      result_plan->plan_tree_ = logical_plan;
    }
  }
  else
  {
    logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  }
  if (OB_SUCCESS == ret)
  {
    drp_idx_stmt = (ObDropIndexStmt*)parse_malloc(sizeof(ObDropIndexStmt), result_plan->name_pool_);
    if (NULL == drp_idx_stmt)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc YaoDropIndexStmt");
    }
    else
    {
      drp_idx_stmt = new(drp_idx_stmt) ObDropIndexStmt(name_pool);
      query_id = logical_plan->generate_query_id();
      drp_idx_stmt->set_query_id(query_id);
      if (OB_SUCCESS != (ret = logical_plan->add_query(drp_idx_stmt)))
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Can not add YaoDropIndexStmt to logical plan");
      }
    }
  }
  if(OB_SUCCESS == ret)
  {
    OB_ASSERT(node->children_[0]);
    ParseNode *table_node = NULL;
    ObString table_name;
    table_node =node->children_[0];
    // add by zhangcd [multi_database.secondary_index] 20150703
    if(NULL == table_node || table_node->num_child_ != 2)
    {
      ret = OB_ERROR;
      PARSER_LOG("Drop Index failed!");
    }
    // add:e
    if(OB_SUCCESS == ret)
    {
      //add by zhangcd [multi_database.secondary_index] 20150703:b
      ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
      ObString short_table_name;
      if(table_node->children_[0] == NULL && table_node->children_[1] != NULL)
      {
        ObSQLSessionInfo* session_info = static_cast<ObSQLSessionInfo*>(result_plan->session_info_);
        db_name = session_info->get_db_name();
        short_table_name = ObString::make_string(table_node->children_[1]->str_value_);
        char *ct_name = NULL;
        ct_name = static_cast<char *>(name_pool->alloc(OB_MAX_COMPLETE_TABLE_NAME_LENGTH));
        if(NULL == ct_name)
        {
          ret = OB_ERROR;
          PARSER_LOG("Memory over flow!");
        }
        else if(db_name.length() >= OB_MAX_DATBASE_NAME_LENGTH)
        {
          ret = OB_ERROR;
          PARSER_LOG("db name is too long!");
        }
        else if(short_table_name.length() >= OB_MAX_TABLE_NAME_LENGTH)
        {
          ret = OB_ERROR;
          PARSER_LOG("table name is too long!");
        }
        else
        {
          table_name.assign_buffer(ct_name, OB_MAX_COMPLETE_TABLE_NAME_LENGTH);
          // mod zhangcd [multi_database.seconary_index] 20150721:b
          //          table_name.concat(db_name, short_table_name, '.');
          table_name.write(db_name.ptr(), db_name.length());
          table_name.write(".", 1);
          table_name.write(short_table_name.ptr(), short_table_name.length());
          // mod:e
        }
      }
      else if(table_node->children_[0] != NULL && table_node->children_[1] != NULL)
      {
        db_name = ObString::make_string(table_node->children_[0]->str_value_);
        short_table_name = ObString::make_string(table_node->children_[1]->str_value_);
        char *ct_name = NULL;
        ct_name = static_cast<char *>(name_pool->alloc(OB_MAX_COMPLETE_TABLE_NAME_LENGTH));
        if(NULL == ct_name)
        {
          ret = OB_ERROR;
          PARSER_LOG("Memory over flow!");
        }
        else if(db_name.length() >= OB_MAX_DATBASE_NAME_LENGTH)
        {
          ret = OB_ERROR;
          PARSER_LOG("db name is too long!");
        }
        else if(short_table_name.length()  >= OB_MAX_TABLE_NAME_LENGTH)
        {
          ret = OB_ERROR;
          PARSER_LOG("table name is too long!");
        }
        else
        {
          table_name.assign_buffer(ct_name, OB_MAX_COMPLETE_TABLE_NAME_LENGTH);
          // mod zhangcd [multi_database.seconary_index] 20150721:b
          //          table_name.concat(db_name, short_table_name, '.');
          table_name.write(db_name.ptr(), db_name.length());
          table_name.write(".", 1);
          table_name.write(short_table_name.ptr(), short_table_name.length());
          // mod:e
        }
      }
      else
      {
        ret = OB_ERROR;
        PARSER_LOG("Parse failed!");
      }
      //add:e

      //del by zhangcd [multi_database.secondary_index] 20150703:b
      /*
      table_name.assign_ptr(
                (char*)(table_node->str_value_),
                static_cast<int32_t>(strlen(table_node->str_value_))
                );
      */
      //del:e

    }
    //add liumz, 20150725:b
    if (OB_SUCCESS == ret)
    {//add:e
      schema_checker = static_cast<ObSchemaChecker*>(result_plan->schema_checker_);
      if (schema_checker == NULL)
      {
        ret = OB_ERR_SCHEMA_UNSET;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Schema(s) are not set");
      }
      else if((data_table_id=schema_checker->get_table_id(table_name))==OB_INVALID_ID)
      {
        ret = OB_ERR_TABLE_UNKNOWN;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "table '%.*s' does not exist", table_name.length(), table_name.ptr());
      }
    }
  }
  if (OB_SUCCESS == ret)
  {
    OB_ASSERT(node->children_[1] && node->children_[1]->num_child_ > 0);
    ParseNode *table_node = NULL;
    ObString table_name;
    for (int32_t i = 0; i < node->children_[1]->num_child_; i ++)
    {
      uint64_t index_tid = OB_INVALID_ID;
      table_node = node->children_[1]->children_[i];
      table_name.assign_ptr(
            (char*)(table_node->str_value_),
            static_cast<int32_t>(strlen(table_node->str_value_))
            );
      //generate index name here

      //modify by zhangcd [multi_database.secondary_index] 20150703:b
      char *dt_name_buffer_ptr = NULL;
      if(NULL == (dt_name_buffer_ptr = (char *)name_pool->alloc(OB_MAX_COMPLETE_TABLE_NAME_LENGTH)))
      {
        ret = OB_ERROR;
      }
      else
      {
        memset(dt_name_buffer_ptr, 0, OB_MAX_COMPLETE_TABLE_NAME_LENGTH);
      }
      //memset(tname_str,0,OB_MAX_TABLE_NAME_LENGTH);
      //modify:e

      if(OB_SUCCESS == ret && OB_SUCCESS != (ret = generate_index_table_name(table_name,data_table_id,tname_str,str_len)))
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "generate index name failed '%.*s'", table_name.length(), table_name.ptr());
      }
      else if(db_name.length() >= OB_MAX_DATBASE_NAME_LENGTH)
      {
        ret = OB_ERROR;
        PARSER_LOG("db name is too long!");
      }
      else if(str_len >= OB_MAX_TABLE_NAME_LENGTH)
      {
        ret = OB_ERROR;
        PARSER_LOG("table name is too long!");
      }
      else
      {
        // add by zhangcd [multi_database.secondary_index] 20150703:b
        strncpy(dt_name_buffer_ptr, (const char*)db_name.ptr(), db_name.length());
        strncpy(dt_name_buffer_ptr + db_name.length(), ".", 1);
        strncpy(dt_name_buffer_ptr + db_name.length() + 1, tname_str, str_len);
        // add zhangcd [multi_database.seconary_index] 20150721:b
        //dt_name_buffer_ptr[db_name.length() + 1 + str_len] = 0;
        // add:e
        // add:e
        table_name.reset();
        table_name.assign_ptr(dt_name_buffer_ptr,(int32_t)str_len + db_name.length() + 1);
      }
      if(OB_SUCCESS == ret)
      {
        if(OB_INVALID_ID == (index_tid = schema_checker->get_table_id(table_name)))
        {
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "failed to get index id'%.*s'", table_name.length(), table_name.ptr());
        }
        else if (OB_SUCCESS != (ret = drp_idx_stmt->add_table_name_id(*result_plan, table_name)))
        {
          break;
        }
      }
    }
  }
  return ret;
}
//add e

bool is_show_table_view_type(ResultPlan *result_plan, ParseNode *show_table_node)
{
  ParseNode *db_node = show_table_node->children_[0];
  ParseNode *table_node = show_table_node->children_[1];
  ObString db_name;
  bool has_db_name = true;
  bool ret = false;

  if(NULL == db_node)
  {
    ObSQLSessionInfo *session_info = static_cast<ObSQLSessionInfo *>(result_plan->session_info_);
    if (NULL == session_info)
    {
      has_db_name = false;
    }
    else
    {
      db_name = session_info->get_db_name();
    }
  }
  else
  {
    db_name = ObString::make_string(db_node->str_value_);
  }

  if(has_db_name)
  {
    ObString table_name = ObString::make_string(table_node->str_value_);
    ObString db_table_name;
    int64_t buf_len = OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1;
    char name_buf[buf_len];
    db_table_name.assign_buffer(name_buf, static_cast<ObString::obstr_size_t>(buf_len));
    if (OB_SUCCESS == write_db_table_name(db_name, table_name, db_table_name))
    {
      ObSchemaChecker *schema_checker = static_cast<ObSchemaChecker *>(result_plan->schema_checker_);
      if(NULL != schema_checker)
      {
        const ObTableSchema *table_schema = schema_checker->get_table_schema(db_table_name);
        if(table_schema && table_schema->get_type() == ObTableSchema::VIEW)
        {
          ret = true;
        }
      }
    }
  }
  return ret;
}

int resolve_show_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  uint64_t  sys_table_id = OB_INVALID_ID;
  ParseNode *show_table_node = NULL;
  ParseNode *condition_node = NULL;
  OB_ASSERT(node && node->type_ >= T_SHOW_TABLES && node->type_ <= T_SHOW_PROCESSLIST);
  query_id = OB_INVALID_ID;

  ObLogicalPlan* logical_plan = NULL;
  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
  if (result_plan->plan_tree_ == NULL)
  {
    logical_plan = (ObLogicalPlan*)parse_malloc(sizeof(ObLogicalPlan), result_plan->name_pool_);
    if (logical_plan == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc YaoLogicalPlan");
    }
    else
    {
      logical_plan = new(logical_plan) ObLogicalPlan(name_pool);
      result_plan->plan_tree_ = logical_plan;
    }
  }
  else
  {
    logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  }

  if (ret == OB_SUCCESS)
  {
    ObShowStmt* show_stmt = (ObShowStmt*)parse_malloc(sizeof(ObShowStmt), result_plan->name_pool_);
    if (show_stmt == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc YaoShowStmt");
    }
    else
    {
      ParseNode sys_table_name;
      sys_table_name.type_ = T_IDENT;
      switch (node->type_)
      {
        case T_SHOW_TABLES:
          OB_ASSERT(node->num_child_ == 1);
          condition_node = node->children_[0];
          show_stmt = new(show_stmt) ObShowStmt(name_pool, ObBasicStmt::T_SHOW_TABLES);
          sys_table_name.str_value_ = OB_TABLES_SHOW_TABLE_NAME;
          break;
          // add by zhangcd [multi_database.show_tables] 20150616:b
        case T_SHOW_SYSTEM_TABLES:
          show_stmt = new(show_stmt) ObShowStmt(name_pool, ObBasicStmt::T_SHOW_SYSTEM_TABLES);
          sys_table_name.str_value_ = OB_TABLES_SHOW_TABLE_NAME;
          break;
          // add by zhangcd [multi_database.show_tables] 20150616:e
          // add by zhangcd [multi_database.show_databases] 20150616:b
        case T_SHOW_DATABASES:
          show_stmt = new(show_stmt) ObShowStmt(name_pool,ObBasicStmt::T_SHOW_DATABASES);
          sys_table_name.str_value_ = OB_DATABASES_SHOW_TABLE_NAME;
          break;
          // add by zhangcd [multi_database.show_databases] 20150616:e
          // add by zhangcd [multi_database.show_databases] 20150617:b
        case T_SHOW_CURRENT_DATABASE:
          show_stmt = new(show_stmt) ObShowStmt(name_pool,ObBasicStmt::T_SHOW_CURRENT_DATABASE);
          sys_table_name.str_value_ = OB_DATABASES_SHOW_TABLE_NAME;
          break;
          // add by zhangcd [multi_database.show_databases] 20150617:e
          //add liumengzhan_show_index [20141208]
        case T_SHOW_INDEX:
          OB_ASSERT(node->num_child_ == 2);
          show_table_node = node->children_[0];
          condition_node = node->children_[1];
          show_stmt = new(show_stmt) ObShowStmt(name_pool, ObBasicStmt::T_SHOW_INDEX);
          sys_table_name.str_value_ = OB_INDEX_SHOW_TABLE_NAME;
          break;
          //add:e
        case T_SHOW_ALL_INDEX:
          OB_ASSERT(node->num_child_ == 1);
          condition_node = node->children_[0];
          show_stmt = new(show_stmt) ObShowStmt(name_pool, ObBasicStmt::T_SHOW_ALL_INDEX);
          show_stmt->set_index_status(node->value_);
          sys_table_name.str_value_ = OB_ALL_INDEX_SHOW_TABLE_NAME;
          break;
        case T_SHOW_CREATE_INDEX:
          OB_ASSERT(node->num_child_ == 1);
          show_table_node = node->children_[0];
          show_stmt = new(show_stmt) ObShowStmt(name_pool, ObBasicStmt::T_SHOW_CREATE_INDEX);
          sys_table_name.str_value_ = OB_CREATE_INDEX_SHOW_TABLE_NAME;
          break;
        case T_SHOW_VARIABLES:
          OB_ASSERT(node->num_child_ == 1);
          condition_node = node->children_[0];
          show_stmt = new(show_stmt) ObShowStmt(name_pool, ObBasicStmt::T_SHOW_VARIABLES);
          show_stmt->set_global_scope(node->value_ == 1 ? true : false);
          sys_table_name.str_value_ = OB_VARIABLES_SHOW_TABLE_NAME;
          break;
        case T_SHOW_COLUMNS:
          OB_ASSERT(node->num_child_ == 2);
          show_table_node = node->children_[0];
          condition_node = node->children_[1];
          show_stmt = new(show_stmt) ObShowStmt(name_pool, ObBasicStmt::T_SHOW_COLUMNS);
          sys_table_name.str_value_ = OB_COLUMNS_SHOW_TABLE_NAME;
          break;
        case T_SHOW_SCHEMA:
          show_stmt = new(show_stmt) ObShowStmt(name_pool, ObBasicStmt::T_SHOW_SCHEMA);
          sys_table_name.str_value_ = OB_SCHEMA_SHOW_TABLE_NAME;
          break;
        case T_SHOW_CREATE_TABLE:
          OB_ASSERT(node->num_child_ == 1);
          show_table_node = node->children_[0];
          if(is_show_table_view_type(result_plan, show_table_node))
          {
            show_stmt = new(show_stmt) ObShowStmt(name_pool, ObBasicStmt::T_SHOW_CREATE_VIEW);
            sys_table_name.str_value_ = OB_CREATE_VIEW_SHOW_TABLE_NAME;
          }
          else
          {
            show_stmt = new(show_stmt) ObShowStmt(name_pool, ObBasicStmt::T_SHOW_CREATE_TABLE);
            sys_table_name.str_value_ = OB_CREATE_TABLE_SHOW_TABLE_NAME;
          }
          break;
        case T_SHOW_CREATE_VIEW:
          OB_ASSERT(node->num_child_ == 1);
          show_table_node = node->children_[0];
          show_stmt = new(show_stmt) ObShowStmt(name_pool, ObBasicStmt::T_SHOW_CREATE_VIEW);
          sys_table_name.str_value_ = OB_CREATE_VIEW_SHOW_TABLE_NAME;
          break;
        case T_SHOW_TABLE_STATUS:
          OB_ASSERT(node->num_child_ == 1);
          condition_node = node->children_[0];
          show_stmt = new(show_stmt) ObShowStmt(name_pool, ObBasicStmt::T_SHOW_TABLE_STATUS);
          sys_table_name.str_value_ = OB_TABLE_STATUS_SHOW_TABLE_NAME;
          break;
        case T_SHOW_SERVER_STATUS:
          OB_ASSERT(node->num_child_ == 1);
          condition_node = node->children_[0];
          show_stmt = new(show_stmt) ObShowStmt(name_pool, ObBasicStmt::T_SHOW_SERVER_STATUS);
          sys_table_name.str_value_ = OB_SERVER_STATUS_SHOW_TABLE_NAME;
          break;
        case T_SHOW_WARNINGS:
          OB_ASSERT(node->num_child_ == 0 || node->num_child_ == 1);
          show_stmt = new(show_stmt) ObShowStmt(name_pool, ObBasicStmt::T_SHOW_WARNINGS);
          break;
        case T_SHOW_GRANTS:
          OB_ASSERT(node->num_child_ == 1);
          show_stmt = new(show_stmt) ObShowStmt(name_pool, ObBasicStmt::T_SHOW_GRANTS);
          break;
        case T_SHOW_PARAMETERS:
          OB_ASSERT(node->num_child_ == 1);
          condition_node = node->children_[0];
          show_stmt = new(show_stmt) ObShowStmt(name_pool, ObBasicStmt::T_SHOW_PARAMETERS);
          sys_table_name.str_value_ = OB_PARAMETERS_SHOW_TABLE_NAME;
          break;
          //add liu jun.[MultiUPS] [sql_api] 20150424:b
        case T_SHOW_FUNCTIONS:
          OB_ASSERT(node->num_child_ == 1);
          condition_node = node->children_[0];
          show_stmt = new(show_stmt) ObShowStmt(name_pool, ObBasicStmt::T_SHOW_FUNCTIONS);
          sys_table_name.str_value_ = OB_FUNCTIONS_SHOW_TABLE_NAME;
          break;
          //add 20150424:e
          //add wuna.[MultiUPS] [sql_api] 20160223:b
        case T_SHOW_TABLE_RULES:
          OB_ASSERT(node->num_child_ == 1);
          condition_node = node->children_[0];
          show_stmt = new(show_stmt) ObShowStmt(name_pool, ObBasicStmt::T_SHOW_TABLE_RULES);
          sys_table_name.str_value_ = OB_TABLE_RULES_SHOW_TABLE_NAME;
          break;
        case T_SHOW_GROUPS:
          OB_ASSERT(node->num_child_ == 1);
          condition_node = node->children_[0];
          show_stmt = new(show_stmt) ObShowStmt(name_pool, ObBasicStmt::T_SHOW_GROUPS);
          sys_table_name.str_value_ = OB_PARTITION_GROUPS_SHOW_TABLE_NAME;
          break;
          //add 20160223:e
        case T_SHOW_CURRENT_PAXOS_ID:
          show_stmt = new(show_stmt) ObShowStmt(name_pool, ObBasicStmt::T_SHOW_CURRENT_PAXOS_ID);
          sys_table_name.str_value_ = OB_PAXOS_ID_SHOW_TABLE_NAME;
          break;
        case T_SHOW_PROCESSLIST:
          show_stmt = new(show_stmt) ObShowStmt(name_pool, ObBasicStmt::T_SHOW_PROCESSLIST);
          show_stmt->set_full_process(node->value_ == 1? true: false);
          show_stmt->set_show_table(OB_ALL_SERVER_SESSION_TID);
          break;
        default:
          /* won't be here */
          break;
      }
      if (node->type_ >= T_SHOW_TABLES && node->type_ <= T_SHOW_SERVER_STATUS
          && (ret = resolve_table(result_plan, show_stmt, &sys_table_name, sys_table_id)) == OB_SUCCESS)
      {
        show_stmt->set_sys_table(sys_table_id);
        query_id = logical_plan->generate_query_id();
        show_stmt->set_query_id(query_id);
      }
      if (ret == OB_SUCCESS && (ret = logical_plan->add_query(show_stmt)) != OB_SUCCESS)
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Can not add YaoShowStmt to logical plan");
      }
      if (ret != OB_SUCCESS && show_stmt != NULL)
      {
        show_stmt->~ObShowStmt();
      }
    }

    if (ret == OB_SUCCESS && sys_table_id != OB_INVALID_ID)
    {
      TableItem *table_item = show_stmt->get_table_item_by_id(sys_table_id);
      ret = resolve_table_columns(result_plan, show_stmt, *table_item);
    }
    /* modify liumengzhan_show_index [20141208],
     * add node->type_ == T_SHOW_INDEX, set show_stmt's table_id
     */
    if (ret == OB_SUCCESS && (node->type_ == T_SHOW_INDEX || node->type_ == T_SHOW_COLUMNS || node->type_ == T_SHOW_CREATE_TABLE
                              || node->type_ == T_SHOW_CREATE_INDEX || node->type_ == T_SHOW_CREATE_VIEW))
    {
      OB_ASSERT(show_table_node);
      //add liumz, [multi_database.sql]:20150613
      OB_ASSERT(show_table_node->children_[1]);
      ObSQLSessionInfo *session_info = static_cast<ObSQLSessionInfo*>(result_plan->session_info_);
      if (session_info == NULL)
      {
        ret = OB_ERR_SESSION_UNSET;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "Session not set");
      }
      //add:e
      ObSchemaChecker *schema_checker = static_cast<ObSchemaChecker*>(result_plan->schema_checker_);
      if (schema_checker == NULL)
      {
        ret = OB_ERR_SCHEMA_UNSET;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "Schema(s) are not set");
      }
      //del liumz, [multi_database.sql]:20150613
      /*
      int32_t len = static_cast<int32_t>(strlen(show_table_node->str_value_));
      ObString table_name(len, len, show_table_node->str_value_);
      uint64_t show_table_id = schema_checker->get_table_id(table_name);
      if (show_table_id == OB_INVALID_ID)
      {
        ret = OB_ERR_TABLE_UNKNOWN;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Unknown table \"%s\"", show_table_node->str_value_);
      }
      else
      {
        show_stmt->set_show_table(show_table_id);
      }*/
      //del:e
      //add liumz, [multi_database.sql]:20150613
      //add liumz, [multi_database.bugfix]20150713
      if (OB_SUCCESS == ret)
      {//add:e
        ParseNode *db_node = show_table_node->children_[0];
        ParseNode *table_node = show_table_node->children_[1];
        ObString db_name;
        ObString table_name = ObString::make_string(table_node->str_value_);
        uint64_t show_table_id = schema_checker->get_table_id(table_name);
        if (NULL == db_node)
        {
          //mod liumz, [multi_database.bugfix]20150713:b
          db_name = session_info->get_db_name();
          /*if ((ret = ob_write_string(*name_pool, session_info->get_db_name(), db_name)) != OB_SUCCESS) {
              PARSER_LOG("Can not malloc space for db name");
            }*/
          //mod:e
        }
        else
        {
          //mod liumz, [multi_database.bugfix]20150713:b
          db_name = ObString::make_string(db_node->str_value_);
          /*if ((ret = ob_write_string(*name_pool, ObString::make_string(db_node->str_value_), db_name)) != OB_SUCCESS) {
              PARSER_LOG("Can not malloc space for db name");
            }*/
          //mod:e
        }
        //if (OB_SUCCESS == ret) {
        if (!IS_SYS_TABLE_ID(show_table_id))
        {
          ObString db_table_name;
          int64_t buf_len = OB_MAX_TABLE_NAME_LENGTH + OB_MAX_DATBASE_NAME_LENGTH + 1;
          char name_buf[buf_len];
          db_table_name.assign_buffer(name_buf, static_cast<ObString::obstr_size_t>(buf_len));
          if (OB_SUCCESS != (ret = write_db_table_name(db_name, table_name, db_table_name))) {
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "Table name too long");
          }
          else
          {
            show_table_id = schema_checker->get_table_id(db_table_name);
          }

          const ObTableSchema *table_schema = schema_checker->get_table_schema(db_table_name);
          if ((table_schema && table_schema->get_type() == ObTableSchema::VIEW)
              && (node->type_ == T_SHOW_COLUMNS))
          {
            show_stmt->set_is_show_view(true);
            logical_plan->set_is_show_view(true);
            const char *text = table_schema->get_text();
            ObStringBuf &parser_mem_pool = session_info->get_parser_mem_pool();
            ParseResult parse_result;
            parse_result.malloc_pool_ = &parser_mem_pool;
            uint64_t ref_id = OB_INVALID_ID;
            if (0 != (ret = parse_init(&parse_result)))
            {
              ret = OB_ERR_PARSER_INIT;
              YYSYS_LOG(WARN, "parser init err, err=%s", strerror(errno));
            }
            else if (parse_sql(&parse_result, text, strlen(text)) != 0
                     || NULL == parse_result.result_tree_)
            {
              ret = OB_ERR_PARSE_SQL;
              YYSYS_LOG(WARN, "failed to parse sql=[%s] err=[%s]", text, parse_result.error_msg_);
            }
            else if (OB_SUCCESS != (ret = resolve_select_stmt(result_plan, parse_result.result_tree_->children_[0], ref_id)))
            {
              YYSYS_LOG(WARN, "failed to resolve select stmt, err=[%d]", ret);
            }
            else
            {
              ObSelectStmt *select_stmt = dynamic_cast<ObSelectStmt *>(logical_plan->get_query(ref_id));
              OB_ASSERT(select_stmt != NULL);
              ret = resolve_view_column_attribute(result_plan, show_stmt, select_stmt);
            }
            logical_plan->set_is_show_view(false);
          }
          else if(table_schema && (table_schema->get_type() == ObTableSchema::NORMAL) &&
                  (node->type_ == T_SHOW_CREATE_VIEW))
          {
            ret = OB_ERR_VIEW_UNKNOWN;
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                     "\"%.*s.%.*s\" is not view", db_name.length(), db_name.ptr(), table_name.length(), table_name.ptr());
          }
        }
        if (OB_SUCCESS == ret)
        {
          if (show_table_id == OB_INVALID_ID)
          {
            ret = OB_ERR_TABLE_UNKNOWN;
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                     "Unknown table \"%.*s.%.*s\"", db_name.length(), db_name.ptr(), table_name.length(), table_name.ptr());
          }
          else
          {
            show_stmt->set_db_name(db_name);
            show_stmt->set_show_table(show_table_id);
          }
        }
        //}
      }
      //add:e
    }

    /* modify liumengzhan_show_index [20141210]
     * support like expr: show index on table_name [ like | where ]
     *
     */
    if (ret == OB_SUCCESS && condition_node
        && (node->type_ == T_SHOW_TABLES || node->type_ == T_SHOW_INDEX || node->type_ == T_SHOW_VARIABLES || node->type_ == T_SHOW_COLUMNS
            || node->type_ == T_SHOW_TABLE_STATUS || node->type_ == T_SHOW_SERVER_STATUS
            || node->type_ == T_SHOW_PARAMETERS || node->type_ == T_SHOW_FUNCTIONS
            || node->type_ == T_SHOW_TABLE_RULES || node->type_ == T_SHOW_GROUPS
            || node->type_ == T_SHOW_ALL_INDEX))//add wuna.[MultiUPS] [sql_api] 20160223
    {
      if (condition_node->type_ == T_OP_LIKE && condition_node->num_child_ == 1)
      {
        OB_ASSERT(condition_node->children_[0]->type_ == T_STRING);
        ObString  like_pattern;
        like_pattern.assign_ptr(
              (char*)(condition_node->children_[0]->str_value_),
              static_cast<int32_t>(strlen(condition_node->children_[0]->str_value_))
              );
        ret = show_stmt->set_like_pattern(like_pattern);
      }
      else
      {
        ret = resolve_and_exprs(
                result_plan,
                show_stmt,
                condition_node->children_[0],
                show_stmt->get_where_exprs(),
                T_WHERE_LIMIT
                );
      }
    }

    if (ret == OB_SUCCESS && node->type_ == T_SHOW_WARNINGS)
    {
      show_stmt->set_count_warnings(node->value_ == 1 ? true : false);
      if (node->num_child_ == 1 && node->children_[0] != NULL)
      {
        ParseNode *limit = node->children_[0];
        OB_ASSERT(limit->num_child_ == 2);
        int64_t offset = limit->children_[0] == NULL ? 0 : limit->children_[0]->value_;
        int64_t count = limit->children_[1] == NULL ? -1 : limit->children_[1]->value_;
        show_stmt->set_warnings_limit(offset, count);
      }
    }

    if (ret == OB_SUCCESS && node->type_ == T_SHOW_GRANTS)
    {
      if (node->children_[0] != NULL)
      {
        ObString name;
        if ((ret = ob_write_string(*name_pool, ObString::make_string(node->children_[0]->str_value_), name)) != OB_SUCCESS)
        {
          PARSER_LOG("Can not malloc space for user name");
        }
        else
        {
          show_stmt->set_user_name(name);
        }
      }
    }
  }
  return ret;
}

int resolve_prepare_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_PREPARE && node->num_child_ == 2);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObPrepareStmt *stmt = NULL;
  if (OB_SUCCESS != (ret = prepare_resolve_stmt(result_plan, query_id, stmt)))
  {
  }
  else
  {
    ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
    if (ret == OB_SUCCESS)
    {
      OB_ASSERT(node->children_[0]);
      ObString name;
      if ((ret = ob_write_string(*name_pool, ObString::make_string(node->children_[0]->str_value_), name)) != OB_SUCCESS)
      {
        PARSER_LOG("Can not malloc space for stmt name");
      }
      else
      {
        stmt->set_stmt_name(name);
      }
    }
    if (ret == OB_SUCCESS)
    {
      uint64_t sub_query_id = OB_INVALID_ID;
      switch (node->children_[1]->type_)
      {
        case T_SELECT:
          ret = resolve_select_stmt(result_plan, node->children_[1], sub_query_id);
          break;
        case T_DELETE:
          ret = resolve_delete_stmt(result_plan, node->children_[1], sub_query_id);
          break;
        case T_INSERT:
          ret = resolve_insert_stmt(result_plan, node->children_[1], sub_query_id);
          break;
        case T_UPDATE:
          ret = resolve_update_stmt(result_plan, node->children_[1], sub_query_id);
          break;
        default:
          ret = OB_ERR_PARSER_SYNTAX;
          PARSER_LOG("Wrong statement type in prepare statement");
          break;
      }
      if (ret == OB_SUCCESS)
        stmt->set_prepare_query_id(sub_query_id);
    }
  }
  return ret;
}

int resolve_variable_set_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_VARIABLE_SET);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObVariableSetStmt *stmt = NULL;
  if (OB_SUCCESS != (ret = prepare_resolve_stmt(result_plan, query_id, stmt)))
  {
  }
  else
  {
    ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
    ParseNode* set_node = NULL;
    ObVariableSetStmt::VariableSetNode var_node;
    for (int32_t i = 0; ret == OB_SUCCESS && i < node->num_child_; i++)
    {
      set_node = node->children_[i];
      OB_ASSERT(set_node->type_ == T_VAR_VAL);
      switch (set_node->value_)
      {
        case 1:
          var_node.scope_type_ = ObVariableSetStmt::GLOBAL;
          break;
        case 2:
          var_node.scope_type_ = ObVariableSetStmt::SESSION;
          break;
        case 3:
          var_node.scope_type_ = ObVariableSetStmt::LOCAL;
          break;
        default:
          var_node.scope_type_ = ObVariableSetStmt::NONE_SCOPE;
          break;
      }

      ParseNode* var = set_node->children_[0];
      OB_ASSERT(var);
      var_node.is_system_variable_ = (var->type_ == T_SYSTEM_VARIABLE) ? true : false;
      if ((ret = ob_write_string(*name_pool, ObString::make_string(var->str_value_),
                                 var_node.variable_name_)) != OB_SUCCESS)
      {
        PARSER_LOG("Can not malloc space for variable name");
        break;
      }

      OB_ASSERT(node->children_[1]);
      if ((ret = resolve_independ_expr(result_plan, NULL, set_node->children_[1], var_node.value_expr_id_,
                                       T_VARIABLE_VALUE_LIMIT)) != OB_SUCCESS)
      {
        //PARSER_LOG("Resolve set value error");
        break;
      }

      if ((ret = stmt->add_variable_node(var_node)) != OB_SUCCESS)
      {
        PARSER_LOG("Add set entry failed");
        break;
      }
    }
  }
  return ret;
}

int resolve_execute_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_EXECUTE && node->num_child_ == 2);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObExecuteStmt *stmt = NULL;
  if (OB_SUCCESS != (ret = prepare_resolve_stmt(result_plan, query_id, stmt)))
  {
  }
  else
  {
    ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
    if (ret == OB_SUCCESS)
    {
      OB_ASSERT(node->children_[0]);
      ObString name;
      if ((ret = ob_write_string(*name_pool, ObString::make_string(node->children_[0]->str_value_), name)) != OB_SUCCESS)
      {
        PARSER_LOG("Can not malloc space for stmt name");
      }
      else
      {
        stmt->set_stmt_name(name);
      }
    }
    if (ret == OB_SUCCESS && NULL != node->children_[1])
    {
      OB_ASSERT(node->children_[1]->type_ == T_ARGUMENT_LIST);
      ParseNode *arguments = node->children_[1];
      for (int32_t i = 0; ret == OB_SUCCESS && i < arguments->num_child_; i++)
      {
        OB_ASSERT(arguments->children_[i] && arguments->children_[i]->type_ == T_TEMP_VARIABLE);
        ObString name;
        if ((ret = ob_write_string(*name_pool, ObString::make_string(arguments->children_[i]->str_value_), name)) != OB_SUCCESS)
        {
          PARSER_LOG("Resolve variable %s error", arguments->children_[i]->str_value_);
        }
        else if ((ret = stmt->add_variable_name(name)) != OB_SUCCESS)
        {
          PARSER_LOG("Add Using variable failed");
        }
      }
    }
  }
  return ret;
}

int resolve_deallocate_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_DEALLOCATE && node->num_child_ == 1);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObDeallocateStmt *stmt = NULL;
  if (OB_SUCCESS != (ret = prepare_resolve_stmt(result_plan, query_id, stmt)))
  {
    YYSYS_LOG(WARN, "fail to prepare resolve stmt. ret=%d", ret);
  }
  else
  {
    ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
    OB_ASSERT(node->children_[0]);
    ObString name;
    if ((ret = ob_write_string(*name_pool, ObString::make_string(node->children_[0]->str_value_), name)) != OB_SUCCESS)
    {
      PARSER_LOG("Can not malloc space for stmt name");
    }
    else
    {
      stmt->set_stmt_name(name);
    }
  }
  return ret;
}

int resolve_start_trans_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_BEGIN && node->num_child_ == 0);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObStartTransStmt *stmt = NULL;
  if (OB_SUCCESS != (ret = prepare_resolve_stmt(result_plan, query_id, stmt)))
  {
  }
  else
  {
    stmt->set_with_consistent_snapshot(0 != node->value_);
  }
  return ret;
}

int resolve_commit_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_COMMIT && node->num_child_ == 0);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObEndTransStmt *stmt = NULL;
  if (OB_SUCCESS != (ret = prepare_resolve_stmt(result_plan, query_id, stmt)))
  {
  }
  else
  {
    stmt->set_is_rollback(false);
  }
  return ret;
}

int resolve_rollback_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_ROLLBACK && node->num_child_ == 0);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObEndTransStmt *stmt = NULL;
  if (OB_SUCCESS != (ret = prepare_resolve_stmt(result_plan, query_id, stmt)))
  {
  }
  else
  {
    stmt->set_is_rollback(true);
  }
  return ret;
}

int resolve_alter_sys_cnf_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_ALTER_SYSTEM && node->num_child_ == 1);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObAlterSysCnfStmt* alter_sys_cnf_stmt = NULL;
  if (OB_SUCCESS != (ret = prepare_resolve_stmt(result_plan, query_id, alter_sys_cnf_stmt)))
  {
  }
  else if ((ret = alter_sys_cnf_stmt->init()) != OB_SUCCESS)
  {
    PARSER_LOG("Init alter system stmt failed, ret=%d", ret);
  }
  else
  {
    OB_ASSERT(node->children_[0] && node->children_[0]->type_ == T_SYTEM_ACTION_LIST);
    ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
    for (int32_t i = 0; ret == OB_SUCCESS && i < node->children_[0]->num_child_; i++)
    {
      ParseNode *action_node = node->children_[0]->children_[i];
      if (action_node == NULL)
        continue;
      OB_ASSERT(action_node->type_ == T_SYSTEM_ACTION && action_node->num_child_ == 5);
      ObSysCnfItem sys_cnf_item;
      ObString param_name;
      ObString comment;
      ObString server_ip;
      sys_cnf_item.config_type_ = static_cast<ObConfigType>(action_node->value_);
      if ((ret = ob_write_string(
             *name_pool,
             ObString::make_string(action_node->children_[0]->str_value_),
             sys_cnf_item.param_name_)) != OB_SUCCESS)
      {
        PARSER_LOG("Can not malloc space for param name");
        break;
      }
      else if (action_node->children_[2] != NULL
               && (ret = ob_write_string(
                     *name_pool,
                     ObString::make_string(action_node->children_[2]->str_value_),
                     sys_cnf_item.comment_)) != OB_SUCCESS)
      {
        PARSER_LOG("Can not malloc space for comment");
        break;
      }
      else if ((ret = resolve_const_value(
                  result_plan,
                  action_node->children_[1],
                  sys_cnf_item.param_value_)) != OB_SUCCESS)
      {
        break;
      }
      else if (action_node->children_[4] != NULL)
      {
        if (action_node->children_[4]->type_ == T_CLUSTER)
        {
          sys_cnf_item.cluster_id_ = action_node->children_[4]->children_[0]->value_;
        }
        else if (action_node->children_[4]->type_ == T_SERVER_ADDRESS)
        {
          if ((ret = ob_write_string(
                 *name_pool,
                 ObString::make_string(action_node->children_[4]->children_[0]->str_value_),
                 sys_cnf_item.server_ip_)) != OB_SUCCESS)
          {
            PARSER_LOG("Can not malloc space for IP");
            break;
          }
          else
          {
            sys_cnf_item.server_port_ = action_node->children_[4]->children_[1]->value_;
          }
        }
      }
      OB_ASSERT(action_node->children_[3]);
      switch (action_node->children_[3]->value_)
      {
        case 1:
          sys_cnf_item.server_type_ = OB_ROOTSERVER;
          break;
        case 2:
          sys_cnf_item.server_type_ = OB_CHUNKSERVER;
          break;
        case 3:
          sys_cnf_item.server_type_ = OB_MERGESERVER;
          break;
        case 4:
          sys_cnf_item.server_type_ = OB_UPDATESERVER;
          break;
        default:
          /* won't be here */
          ret = OB_ERR_RESOLVE_SQL;
          PARSER_LOG("Unkown server type");
          break;
      }
      if ((ret = alter_sys_cnf_stmt->add_sys_cnf_item(*result_plan, sys_cnf_item)) != OB_SUCCESS)
      {
        // PARSER_LOG("Add alter system config item failed");
        break;
      }
    }
  }
  return ret;
}

int resolve_change_obi(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  UNUSED(query_id);
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_CHANGE_OBI && node->num_child_ >= 2);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObChangeObiStmt* change_obi_stmt = NULL;
  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
  ObLogicalPlan *logical_plan = NULL;
  if (result_plan->plan_tree_ == NULL)
  {
    logical_plan = (ObLogicalPlan*)parse_malloc(sizeof(ObLogicalPlan), result_plan->name_pool_);
    if (logical_plan == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc YaoLogicalPlan");
    }
    else
    {
      logical_plan = new(logical_plan) ObLogicalPlan(name_pool);
      result_plan->plan_tree_ = logical_plan;
    }
  }
  else
  {
    logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  }

  if (ret == OB_SUCCESS)
  {
    change_obi_stmt = (ObChangeObiStmt*)parse_malloc(sizeof(ObChangeObiStmt), result_plan->name_pool_);
    if (change_obi_stmt == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc YaoChangeYaoiStmt");
    }
    else
    {
      change_obi_stmt = new(change_obi_stmt) ObChangeObiStmt(name_pool);
      query_id = logical_plan->generate_query_id();
      change_obi_stmt->set_query_id(query_id);
      if ((ret = logical_plan->add_query(change_obi_stmt)) != OB_SUCCESS)
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Can not add YaoChangeYaoiStmt to logical plan");
      }
      else
      {
        OB_ASSERT(node->children_[0]->type_ == T_SET_MASTER
                  || node->children_[0]->type_ == T_SET_SLAVE
                  || node->children_[0]->type_ == T_SET_MASTER_SLAVE);
        OB_ASSERT(node->children_[1]&& node->children_[1]->type_ == T_STRING);
        change_obi_stmt->set_target_server_addr(node->children_[1]->str_value_);
        if (node->children_[0]->type_ == T_SET_MASTER)
        {
          change_obi_stmt->set_target_role(ObiRole::MASTER);
        }
        else if (node->children_[0]->type_ == T_SET_SLAVE)
        {
          change_obi_stmt->set_target_role(ObiRole::SLAVE);
        }
        else // T_SET_MASTER_SLAVE
        {
          if (node->children_[2] != NULL)
          {
            OB_ASSERT(node->children_[2]->type_ == T_FORCE);
            change_obi_stmt->set_force(true);
          }
        }
      }
    }
  }
  return ret;
}
int resolve_kill_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_KILL && node->num_child_ == 3);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObKillStmt* kill_stmt = NULL;
  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
  ObLogicalPlan *logical_plan = NULL;
  if (result_plan->plan_tree_ == NULL)
  {
    logical_plan = (ObLogicalPlan*)parse_malloc(sizeof(ObLogicalPlan), result_plan->name_pool_);
    if (logical_plan == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc YaoLogicalPlan");
    }
    else
    {
      logical_plan = new(logical_plan) ObLogicalPlan(name_pool);
      result_plan->plan_tree_ = logical_plan;
    }
  }
  else
  {
    logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  }

  if (ret == OB_SUCCESS)
  {
    kill_stmt = (ObKillStmt*)parse_malloc(sizeof(ObKillStmt), result_plan->name_pool_);
    if (kill_stmt == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc YaoKillStmt");
    }
    else
    {
      kill_stmt = new(kill_stmt) ObKillStmt(name_pool);
      query_id = logical_plan->generate_query_id();
      kill_stmt->set_query_id(query_id);
      if ((ret = logical_plan->add_query(kill_stmt)) != OB_SUCCESS)
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Can not add YaoKillStmt to logical plan");
      }
    }
  }
  if (OB_SUCCESS == ret)
  {
    OB_ASSERT(node->children_[0]&& node->children_[0]->type_ == T_BOOL);
    OB_ASSERT(node->children_[1]&& node->children_[1]->type_ == T_BOOL);
    OB_ASSERT(node->children_[2]);
    kill_stmt->set_is_global(node->children_[0]->value_ == 1? true: false);
    kill_stmt->set_thread_id(node->children_[2]->value_);
    kill_stmt->set_is_query(node->children_[1]->value_ == 1? true: false);
  }
  return ret;
}

int resolve_alter_group_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_ALTER_GROUP && node->num_child_ == 2);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObAlterGroupStmt *alter_group_stmt = NULL;
  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
  ObLogicalPlan *logical_plan = NULL;
  if (result_plan->plan_tree_ == NULL)
  {
    logical_plan = (ObLogicalPlan*)parse_malloc(sizeof(ObLogicalPlan), result_plan->name_pool_);
    if (logical_plan == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc YaoLogicalPlan");
    }
    else
    {
      logical_plan = new(logical_plan) ObLogicalPlan(name_pool);
      result_plan->plan_tree_ = logical_plan;
    }
  }
  else
  {
    logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  }

  if (ret == OB_SUCCESS)
  {
    alter_group_stmt = (ObAlterGroupStmt *)parse_malloc(sizeof(ObAlterGroupStmt), result_plan->name_pool_);
    if (alter_group_stmt == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc YaoAlterGroupStmt");
    }
    else
    {
      alter_group_stmt = new(alter_group_stmt) ObAlterGroupStmt(name_pool);
      query_id = logical_plan->generate_query_id();
      alter_group_stmt->set_query_id(query_id);
      if ((ret = logical_plan->add_query(alter_group_stmt)) != OB_SUCCESS)
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Can not add YaoAlterGroupStmt to logical plan");
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    ObString name;
    if (NULL != node->children_[0])
    {
      name.assign_ptr(const_cast<char *>(node->children_[0]->str_value_),
                       static_cast<int32_t>(strlen(node->children_[0]->str_value_)));
    }
    
    int64_t idx = node->children_[1]->value_;
    if (idx < 0)
    {
      PARSER_LOG("idx can not be less 0");
      ret = OB_ERROR;
    }
    else if (OB_SUCCESS != (ret = alter_group_stmt->set_group_info(name, idx)))
    {
      PARSER_LOG("Failed to set alter_group_stmt");
    }
  }
  return ret;
}


//add liu jun. [MultiUPS] [sql_api] 20150421:b
int resolve_create_part_func_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id)
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  OB_ASSERT(node&&node->type_==T_CREATE_PART_FUNC&&node->num_child_==4);
  ObLogicalPlan* logical_plan = NULL;
  ObCreatePartFuncStmt* create_part_func_stmt = NULL;
  query_id = OB_INVALID_ID;

  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
  if (result_plan->plan_tree_ == NULL)
  {
    logical_plan = (ObLogicalPlan*)parse_malloc(sizeof(ObLogicalPlan), result_plan->name_pool_);
    if (logical_plan == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc YaoLogicalPlan");
    }
    else
    {
      logical_plan = new(logical_plan) ObLogicalPlan(name_pool);
      result_plan->plan_tree_ = logical_plan;
    }
  }
  else
  {
    logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  }

  if (ret == OB_SUCCESS)
  {
    //mod wuna [MultiUPS] [sql_api] 20151201:b
    //    create_part_func_stmt = (ObCreatePartFuncStmt*)parse_malloc(sizeof(ObCreatePartFuncStmt), result_plan->name_pool_);
    //    if (create_part_func_stmt == NULL)
    //    {
    //      ret = OB_ERR_PARSER_MALLOC_FAILED;
    //      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
    //          "Can not malloc ObCreatePartFuncStmt");
    //    }
    //    else
    //    {
    //      create_part_func_stmt = new(create_part_func_stmt) ObCreatePartFuncStmt(name_pool);
    //      query_id = logical_plan->generate_query_id();
    //      create_part_func_stmt->set_query_id(query_id);
    //      ret = logical_plan->add_query(create_part_func_stmt);
    //      if (ret != OB_SUCCESS)
    //      {
    //        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
    //          "Can not add ObCreatePartFuncStmt to logical plan");
    //      }
    //    }
    ret = generate_part_func_stmt(result_plan,query_id,logical_plan,create_part_func_stmt);
    if(ret != OB_SUCCESS)
    {
      YYSYS_LOG(ERROR,"generate create_part_func_stmt failed.ret = %d",ret);
    }
    //mod 20151201:e
  }

  if (OB_SUCCESS == ret && node->children_[0])
  {
    ObFunctionPartitionType type=OB_FUNC_TYPE_NUM;
    if(node->children_[0]->type_ == T_TYPE_HASH)
    {
      type=HASHFUNC;
    }
    else if(node->children_[0]->type_ == T_TYPE_ENUM)
    {
      type=ENUMFUNC;
    }
    else if(node->children_[0]->type_ == T_TYPE_RANGE)
    {
      type=PRERANGE;
    }
    else if(node->children_[0]->type_ == T_TYPE_HARDCODE)
    {
      type=HARDCODEFUNC;
    }
    create_part_func_stmt->set_partition_type(type);
  }

  if (OB_SUCCESS == ret && node->children_[1])
  {
    OB_ASSERT(node->children_[1]->type_ == T_IDENT);
    const char* part_func_name = node->children_[1]->str_value_;
    if(strlen(part_func_name) >= 2 && part_func_name[0] == '_' && part_func_name[1] == '_')
    {
      ret = OB_ERROR;
      YYSYS_LOG(USER_ERROR,"Invalid partition function name.");
    }
    else
    {
      ObString func_name;
      func_name.assign_ptr((char*)(node->children_[1]->str_value_),
                           static_cast<int32_t>(strlen(node->children_[1]->str_value_)));
      ret = create_part_func_stmt->set_funcion_name(*result_plan, func_name);
      if(OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN,"set_funcion_name failed.ret = %d",ret);
      }
    }
  }

  if(OB_SUCCESS == ret && node->children_[2])
  {
    OB_ASSERT(node->children_[2]->type_ == T_PARAMETER_LIST);
    ret = resolve_parameters_list(result_plan, *create_part_func_stmt, node->children_[2]);
  }

  if(OB_SUCCESS == ret && node->children_[3])
  {
    OB_ASSERT(node->children_[3]->type_ == T_FUNC_ENTITY);
    ObString func_context;
    func_context.assign_ptr((char *)(node->children_[3]->children_[0]->str_value_),
                            static_cast<int32_t>(strlen(node->children_[3]->children_[0]->str_value_)));
    ret = create_part_func_stmt->set_func_context(*result_plan,func_context);
  }
  return ret;
}
int resolve_parameters_list(
    ResultPlan * result_plan,
    ObCreatePartFuncStmt& create_part_func_stmt,
    ParseNode* node
    )
{
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  OB_ASSERT(node&&node->num_child_>0&&node->type_==T_PARAMETER_LIST);
  int32_t param_num = node->num_child_;
  if(OB_MAX_ROWKEY_COLUMN_NUMBER < param_num)
  {
    ret = OB_ERROR;
    snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
             "Parameter num should less than 17.");
  }
  for(int32_t i = 0; i < param_num && OB_SUCCESS == ret;i++)
  {
    ParseNode* param_node = node->children_[i];
    if(param_node == NULL)
    {
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Parameter Node is NULL");
    }
    else
    {
      int32_t param_length = static_cast<int32_t>(strlen(param_node->str_value_));
      ObString param_name;
      param_name.assign_ptr((char *)param_node->str_value_,param_length);
      if(OB_SUCCESS != (ret = create_part_func_stmt.add_param_name(*result_plan,param_name)))
      {
        YYSYS_LOG(ERROR,"Can not add parameter name to create partition function stmt,ret=%d",ret);
        break;
      }
    }
  }
  if(OB_SUCCESS == ret)
  {
    create_part_func_stmt.set_parameters_num(param_num);
  }

  return ret;
}
int resolve_drop_part_func_stmt(
    ResultPlan* result_plan,
    ParseNode* node,
    uint64_t& query_id
    )
{
  int ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  OB_ASSERT(node&&node->num_child_==1&&node->type_==T_DROP_PART_FUNC);
  ObLogicalPlan* logical_plan = NULL;
  ObDropPartFuncStmt* drp_func_stmt = NULL;
  query_id = OB_INVALID_ID;

  //generate a logical plan.
  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
  if (result_plan->plan_tree_ == NULL)
  {
    logical_plan = (ObLogicalPlan*)parse_malloc(sizeof(ObLogicalPlan), result_plan->name_pool_);
    if (logical_plan == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc YaoLogicalPlan");
    }
    else
    {
      logical_plan = new(logical_plan) ObLogicalPlan(name_pool);
      result_plan->plan_tree_ = logical_plan;
    }
  }
  else
  {
    logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  }

  //generate a drop part func stmt
  if (ret == OB_SUCCESS)
  {
    //mod wuna [MultiUPS] [sql_api] 20151215:b
    //    drp_func_stmt = (ObDropPartFuncStmt*)parse_malloc(sizeof(ObDropPartFuncStmt), result_plan->name_pool_);
    //    if (drp_func_stmt == NULL)
    //    {
    //      ret = OB_ERR_PARSER_MALLOC_FAILED;
    //      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
    //          "Can not malloc ObDropPartFuncStmt");
    //    }
    //    else
    //    {
    //      drp_func_stmt = new(drp_func_stmt) ObDropPartFuncStmt(name_pool);
    //      query_id = logical_plan->generate_query_id();
    //      drp_func_stmt->set_query_id(query_id);
    //      if ((ret = logical_plan->add_query(drp_func_stmt)) != OB_SUCCESS)
    //      {
    //        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
    //          "Can not add ObDropPartFuncStmt to logical plan");
    //      }
    //    }
    ret = generate_part_func_stmt(result_plan,query_id,logical_plan,drp_func_stmt);
    if(ret != OB_SUCCESS)
    {
      YYSYS_LOG(ERROR,"generate create_part_func_stmt failed.ret = %d",ret);
    }
    //mod 20151215:e
  }
  //add function name
  int32_t num = node->children_[0]->num_child_;
  ParseNode *func_list = node->children_[0];
  for(int32_t i = 0;i < num;i++)
  {
    const char* part_func_name = func_list->children_[i]->str_value_;
    if(strlen(part_func_name) >= 2 && part_func_name[0] == '_' && part_func_name[1] == '_')
    {
      ret = OB_ERROR;
      YYSYS_LOG(USER_ERROR,"Invalid partition function name.");
      break;
    }
    else
    {
      ObString func_name = ObString::make_string(func_list->children_[i]->str_value_);
      if(OB_SUCCESS != (ret = drp_func_stmt->add_func(func_name)))
      {
        PARSER_LOG("Failed to add user to DropPartFuncStmt");
        break;
      }
    }
  }
  return ret;
}
//add 20150421:e


int resolve_gather_statistics_stmt(ResultPlan* result_plan, ParseNode* node, uint64_t& query_id)
{
  OB_ASSERT(result_plan);
  OB_ASSERT(node && node->type_ == T_GATHER_STATISTICS && node->num_child_ == 2);
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObGatherStatisticsStmt *gather_statistics_stmt = NULL;
  ObLogicalPlan *logical_plan = NULL;
  query_id = OB_INVALID_ID;
  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
  ObString gather_table_name;
  uint64_t src_tid = OB_INVALID_ID;

  if (NULL == result_plan->plan_tree_ )
  {
    logical_plan = (ObLogicalPlan*)parse_malloc(sizeof(ObLogicalPlan), result_plan->name_pool_);
    if (NULL == logical_plan)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc YaoLogicalPlan");
    }
    else
    {
      logical_plan = new(logical_plan) ObLogicalPlan(name_pool);
      result_plan->plan_tree_ = logical_plan;
    }
  }
  else
  {
    logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
  }

  if (ret == OB_SUCCESS)
  {
    gather_statistics_stmt = (ObGatherStatisticsStmt *)parse_malloc(sizeof(ObGatherStatisticsStmt), result_plan->name_pool_);
    if (NULL == gather_statistics_stmt)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
               "Can not malloc YaoGatherStatisticsStmt");
    }
    else
    {
      gather_statistics_stmt = new(gather_statistics_stmt) ObGatherStatisticsStmt(name_pool);
      if (gather_statistics_stmt == NULL)
      {
        YYSYS_LOG(ERROR, "new gather_statistics_stmt failed");
      }
      else
      {
        YYSYS_LOG(INFO, "gather_statistics_stmt is %p", gather_statistics_stmt);
      }
      query_id = logical_plan->generate_query_id();
      gather_statistics_stmt->set_query_id(query_id);
      YYSYS_LOG(INFO, "Before push back, size is %d", logical_plan->get_stmts_count());
      ret = logical_plan->add_query(gather_statistics_stmt);
      if (OB_SUCCESS != ret)
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Can not add YaoGatherStatisticsStmt to logical plan");
      }
    }
  }

  if (OB_SUCCESS == ret && node->children_[0])
  {
    ObString db_name;
    ObString table_name;
    ObString complete_table_name;
    ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);
    ParseNode *table_node = node->children_[0];
    gather_statistics_stmt->set_if_exists(true);
    if(table_node->num_child_ != 2)
    {
      ret = OB_ERROR;
      PARSER_LOG("Parse Failed!");
    }
    else if(table_node->children_[0] == NULL && table_node->children_[1] != NULL)
    {
      ObSQLSessionInfo* session_info = static_cast<ObSQLSessionInfo*>(result_plan->session_info_);
      db_name = session_info->get_db_name();
      table_name = ObString::make_string(table_node->children_[1]->str_value_);
      char *ct_name = NULL;
      ct_name = static_cast<char *>(name_pool->alloc(OB_MAX_COMPLETE_TABLE_NAME_LENGTH));
      if(NULL == ct_name)
      {
        ret = OB_ERROR;
        PARSER_LOG("Memory over flow!");
      }
      else if(db_name.length() >= OB_MAX_DATBASE_NAME_LENGTH)
      {
        ret = OB_ERROR;
        PARSER_LOG("database name is too long!");
      }
      else if(table_name.length() >= OB_MAX_TABLE_NAME_LENGTH)
      {
        ret = OB_ERROR;
        PARSER_LOG("table name is too long!");
      }

      if(OB_SUCCESS == ret)
      {
        complete_table_name.assign_buffer(ct_name, OB_MAX_COMPLETE_TABLE_NAME_LENGTH);
        complete_table_name.write(db_name.ptr(), db_name.length());
        complete_table_name.write(".", 1);
        complete_table_name.write(table_name.ptr(), table_name.length());
      }
    }
    else if(table_node->children_[0] != NULL && table_node->children_[1] != NULL)
    {
      db_name = ObString::make_string(table_node->children_[0]->str_value_);
      table_name = ObString::make_string(table_node->children_[1]->str_value_);
      char *ct_name = NULL;
      ct_name = static_cast<char *>(name_pool->alloc(OB_MAX_COMPLETE_TABLE_NAME_LENGTH));
      if(NULL == ct_name)
      {
        ret = OB_ERROR;
        PARSER_LOG("Memory over flow!");
      }
      else if(db_name.length() >= OB_MAX_DATBASE_NAME_LENGTH)
      {
        ret = OB_ERROR;
        PARSER_LOG("database name is too long!");
      }
      else if(table_name.length() >= OB_MAX_TABLE_NAME_LENGTH)
      {
        ret = OB_ERROR;
        PARSER_LOG("table name is too long!");
      }
      if(OB_SUCCESS == ret)
      {
        complete_table_name.assign_buffer(ct_name, OB_MAX_COMPLETE_TABLE_NAME_LENGTH);
        complete_table_name.write(db_name.ptr(), db_name.length());
        complete_table_name.write(".", 1);
        complete_table_name.write(table_name.ptr(), table_name.length());
      }
    }
    else
    {
      ret = OB_ERROR;
      PARSER_LOG("Parse failed!");
    }

    if (OB_SUCCESS == ret)
    {
      gather_table_name = complete_table_name;
      if (OB_SUCCESS != (ret = gather_statistics_stmt->set_statistics_table(*result_plan, gather_table_name)))
      {
      }
      else if (OB_SUCCESS != (ret = gather_statistics_stmt->set_row_key_info(*result_plan, gather_table_name)))
      {
      }
      else
      {
        ObSchemaChecker* check = NULL;
        check = static_cast<ObSchemaChecker*>(result_plan->schema_checker_);
        if (NULL == check)
        {
          ret = OB_ERR_SCHEMA_UNSET;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "Schema(s) are not set");
        }
        if (OB_SUCCESS == ret)
        {
          if (OB_INVALID_ID == (src_tid = check->get_table_id(gather_table_name)))
          {
            ret = OB_ENTRY_NOT_EXIST;
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                     "the table to create index '%.*s' is not exist", gather_table_name.length(), gather_table_name.ptr());
          }
        }
      }
    }
  }
  if (OB_SUCCESS == ret)
  {
    OB_ASSERT(node->children_[1] != NULL);
    ParseNode* column_node = NULL;
    ObString statistics_column;
    for(int32_t i = 0; i < node->children_[1]->num_child_; i++)
    {
      column_node = node->children_[1]->children_[i];
      statistics_column.assign( (char*)(column_node->str_value_),
                                static_cast<int32_t>(strlen(column_node->str_value_)));
      if (OB_SUCCESS != (ret = gather_statistics_stmt->add_statistics_columns(*result_plan, gather_table_name, statistics_column)))
      {
        break;
      }
    }
  }
  return ret;
}

int resolve_create_view_stmt(
    ResultPlan *result_plan,
    ParseNode *node,
    uint64_t &query_id)
{
  int ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  OB_ASSERT(node && node->num_child_ == 5 && node->type_ == T_CREATE_VIEW);
  ObLogicalPlan *logical_plan = NULL;
  ObCreateViewStmt *create_view_stmt = NULL;
  query_id = OB_INVALID_ID;
  ObStringBuf *name_pool = static_cast<ObStringBuf *>(result_plan->name_pool_);
  if (result_plan->plan_tree_ == NULL)
  {
    logical_plan = (ObLogicalPlan *) parse_malloc(sizeof(ObLogicalPlan), result_plan->name_pool_);
    if (logical_plan == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "can not malloc YaoLogicalPlan");
    }
    else
    {
      logical_plan = new(logical_plan) ObLogicalPlan(name_pool);
      result_plan->plan_tree_ = logical_plan;
    }
  }
  else
  {
    logical_plan = static_cast<ObLogicalPlan *>(result_plan->plan_tree_);
  }
  ObSchemaChecker *schema_checker = NULL;
  if (NULL == (schema_checker = static_cast<ObSchemaChecker *>(result_plan->schema_checker_)))
  {
    ret = OB_ERR_SCHEMA_UNSET;
    snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "Schema(s) are not set");
  }
  if (ret == OB_SUCCESS)
  {
    create_view_stmt = (ObCreateViewStmt*)parse_malloc(sizeof(ObCreateViewStmt), result_plan->name_pool_);
    if(create_view_stmt == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "can not malloc YaoCreateViewStmt");
    }
    else
    {
      create_view_stmt = new(create_view_stmt)ObCreateViewStmt(name_pool);
      query_id = logical_plan->generate_query_id();
      if(OB_SUCCESS != (ret = logical_plan->add_query(create_view_stmt)))
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "can not add YaoCreateViewStat to logical plan");
      }
    }
  }
  if (ret == OB_SUCCESS)
  {
    if(node->children_[0])
    {
      OB_ASSERT(node->children_[0] && node->children_[0]->type_ == T_SEQUENCE_REPLACE);
      create_view_stmt->set_or_replace(true);
    }
    //child 1:relation_factor
    ParseNode * relation_node = node->children_[1];
    OB_ASSERT(relation_node && relation_node->num_child_ == 2 && relation_node->type_ == T_RELATION);
    if (ret == OB_SUCCESS)
    {
      ObString dbname;
      if (relation_node->children_[0])
      {
        int32_t dbname_length = static_cast<int32_t>(strlen(relation_node->children_[0]->str_value_));
        if(dbname_length <= 0 || dbname_length >= OB_MAX_DATBASE_NAME_LENGTH)
        {
          ret = OB_INVALID_ARGUMENT;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "database name is invalid.");
        }
        else
        {
          dbname.assign_ptr((char *)(relation_node->children_[0]->str_value_), dbname_length);
        }
      }
      else
      {
        ObSQLSessionInfo *session_info = NULL;
        if (NULL == (session_info = static_cast<ObSQLSessionInfo *>(result_plan->session_info_)))
        {
          ret = OB_ERR_SESSION_UNSET;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "Session not set");
        }
        else
        {
          dbname = session_info->get_db_name();
        }
      }
      ret = create_view_stmt->set_db_name(*result_plan, dbname);
    }
    if (ret == OB_SUCCESS)
    {
      int32_t view_name_length = static_cast<int32_t>(strlen(relation_node->children_[1]->str_value_));
      if(view_name_length <= 0 || view_name_length >= OB_MAX_TABLE_NAME_LENGTH)
      {
        ret = OB_INVALID_ARGUMENT;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "view name is invalid.");
      }
      else
      {
        ObString view_name;
        view_name.assign_ptr((char *)relation_node->children_[1]->str_value_, view_name_length);
        bool is_alter_view = static_cast<bool>(node->value_);
        ret = create_view_stmt->set_view_name(*result_plan, view_name, is_alter_view);
      }
    }
  }
  if (ret == OB_SUCCESS)
  {
    OB_ASSERT(node->children_[3] && node->children_[3]->type_ == T_SELECT);
    uint64_t ref_id = OB_INVALID_ID;
    logical_plan->set_use_for_view(true); //
    logical_plan->set_is_create_view(true);
    logical_plan->reset_view_list();
    ret = resolve_select_stmt(result_plan, node->children_[3], ref_id);
    logical_plan->set_use_for_view(false);
    if (ret == OB_SUCCESS)
    {
      create_view_stmt->set_select_query_id(ref_id);
    }
  }
  if (ret == OB_SUCCESS)
  {
    ret = resolve_view_elements(result_plan, node->children_[2], create_view_stmt);
  }
  if (ret == OB_SUCCESS && node->children_[4])
  {
    ObViewCheckOption with_check_option = static_cast<ObViewCheckOption>(node->children_[4]->value_);
    create_view_stmt->set_with_check_option(with_check_option);
    ObSelectStmt *select_stmt = dynamic_cast<ObSelectStmt *>(logical_plan->get_stmt(1));
    ObRewriteQueryStmt *rewrite_query_stmt = create_view_stmt->get_rewrite_query();
    ObArray<ObString> *column_list = create_view_stmt->get_column_list();
    ret = rewrite_query_stmt->init(logical_plan, schema_checker, column_list);
    char sql_text[OB_MAX_SQL_LENGTH] = {0};
    if (ret == OB_SUCCESS)
    {
      ret = rewrite_query_stmt->rewrite_select_stmt(select_stmt, sql_text, true);
    }
    if (ret == OB_SUCCESS)
    {
      create_view_stmt->set_sql_text(sql_text);
    }
  }
  return ret;
}

int resolve_view_elements(
    ResultPlan *result_plan,
    ParseNode *node,
    ObCreateViewStmt *create_view_stmt)
{
  int &ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObLogicalPlan *logical_plan = static_cast<ObLogicalPlan *>(result_plan->plan_tree_);
  ObSelectStmt *select_stmt = static_cast<ObSelectStmt *>(logical_plan->get_stmt(1));// 0:create_view_stmt 1:main select_stmt
  if(node)
  {
    if(node->num_child_ != select_stmt->get_select_item_size())
    {
      ret = OB_ERR_COLUMN_SIZE;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "view's SELECT and view's field list have different column counts");
    }
    else
    {
      ObString column_name;
      for (int i = 0; i < node->num_child_ && ret == OB_SUCCESS; i++)
      {
        ParseNode *column_node = node->children_[i];
        int32_t column_length = static_cast<int32_t>(strlen(column_node->str_value_));
        if (column_length >= OB_MAX_COLUMN_NAME_LENGTH)
        {
          ret = OB_ERR_COLUMN_NAME_LENGTH;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "column name is too long");
        }
        else
        {
          column_name.assign_ptr((char *)column_node->str_value_, column_length);
          ret = create_view_stmt->add_column_name(*result_plan, column_name);
        }
      }
    }
  }
  else
  {
    int32_t select_item_size = select_stmt->get_select_item_size();
    ObVector<SelectItem> &select_items = select_stmt->get_select_items();
    for(int32_t idx = 0; idx < select_item_size && ret == OB_SUCCESS; idx++)
    {
      SelectItem &select_item = select_items.at(idx);
      ret = create_view_stmt->add_column_name(*result_plan, select_item.expr_name_);
    }
  }
  return ret;
}

int resolve_view_column_attribute(
    ResultPlan *result_plan,
    ObShowStmt *show_stmt,
    ObSelectStmt *select_stmt)
{
  int &ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObLogicalPlan *logical_plan = static_cast<ObLogicalPlan *>(result_plan->plan_tree_);
  ObSelectStmt::SetOperator set_op = select_stmt->get_set_op();
  if(set_op != ObSelectStmt::NONE)
  {
    uint64_t left_query_id = select_stmt->get_left_query_id();
    ObSelectStmt *left_stmt = dynamic_cast<ObSelectStmt *>(logical_plan->get_query(left_query_id));
    ret = resolve_view_column_attribute(result_plan, show_stmt, left_stmt);
  }
  else
  {
    ObSchemaChecker *schema_checker = NULL;
    schema_checker = static_cast<ObSchemaChecker *>(result_plan->schema_checker_);
    if (schema_checker == NULL)
    {
      ret = OB_ERR_SCHEMA_UNSET;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "Schema(s) are not set");
    }
    else
    {
      ObVector<ColumnItem *> column_items;
      ObVector<TableItem *> table_items;
      for(int32_t idx = 0; idx < logical_plan->get_stmts_count(); idx++)
      {
        ObBasicStmt *basic_stmt = logical_plan->get_stmt(idx);
        if (basic_stmt->get_stmt_type() == ObBasicStmt::T_SELECT)
        {
          ObStmt *stmt = dynamic_cast<ObStmt *>(basic_stmt);
          for(int32_t i = 0; i < stmt->get_column_size(); i++)
          {
            const ColumnItem *item = stmt->get_column_item(i);
            if(item != NULL)
            {
              column_items.push_back(item);
            }
          }
          for (int32_t i = 0; i < stmt->get_table_size(); i++)
          {
            const TableItem *item = &(stmt->get_table_item(i));
            if(item != NULL)
            {
              table_items.push_back(item);
            }
          }
        }
      }
      int32_t select_item_size = select_stmt->get_select_item_size();
      ObVector<SelectItem> &select_items = select_stmt->get_select_items();
      for (int32_t idx = 0; idx < select_item_size && ret == OB_SUCCESS; idx++)
      {
        SelectItem &select_item = select_items.at(idx);
        ObColumnDef column_def;
        uint64_t expr_id = select_item.expr_id_;
        ObString *expr_name = &(select_item.expr_name_);
        if (select_item.is_real_alias_)
        {
          expr_name = const_cast<ObString *>(select_stmt->get_raw_column(expr_id));
        }
        ObSqlRawExpr *sql_expr = logical_plan->get_expr(expr_id);
        ObRawExpr *expr = sql_expr->get_expr();
        column_def.column_name_ = select_item.expr_name_;
        column_def.data_type_ = select_item.type_;
        column_def.type_length_ = static_cast<int64_t>(expr->get_length());
        column_def.precision_ = static_cast<int64_t>(expr->get_precision());
        column_def.scale_ = static_cast<int64_t>(expr->get_scale());
        ret = resolve_view_elements(result_plan, logical_plan, schema_checker,
                                    select_stmt, expr_name, sql_expr, column_items, table_items, &column_def);
        if (ret == OB_SUCCESS)
        {
          ret = show_stmt->add_column_def(*result_plan, column_def);
        }
      }
    }
  }
  return ret;
}

int resolve_view_elements(
    ResultPlan *result_plan,
    ObLogicalPlan *logical_plan,
    ObSchemaChecker *schema_checker,
    ObSelectStmt *select_stmt,
    ObString *expr_name,
    ObSqlRawExpr *sql_expr,
    ObVector<ColumnItem *> &column_items,
    ObVector<TableItem *> &table_items,
    ObColumnDef *column_def)
{
  int &ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  ObItemType expr_type = sql_expr->get_expr()->get_expr_type();
  if (!sql_expr->is_contain_aggr() && expr_type == T_REF_COLUMN)
  {
    ObBinaryRefRawExpr *col_expr = dynamic_cast<ObBinaryRefRawExpr *>(sql_expr->get_expr());
    uint64_t table_id = col_expr->get_first_ref_id();
    uint64_t column_id = col_expr->get_second_ref_id();
    const TableItem *table_item = select_stmt->get_table_item_by_id(table_id);
    const ColumnItem *column_item = select_stmt->get_column_item_by_id(table_id, column_id);
    if(table_item == NULL || column_item == NULL)
    {
      for(int32_t i = 0; i < table_items.size(); i++)
      {
        const TableItem *item = table_items.at(i);
        if (item->table_id_ == table_id)
        {
          table_item = item;
        }
      }
      for(int32_t i = 0; i < column_items.size(); i++)
      {
        const ColumnItem *item = column_items.at(i);
        if (item->table_id_ == table_id && item->column_id_ == column_id)
        {
          column_item = item;
        }
      }
    }
    if(table_item == NULL || column_item == NULL)
    {
      ret = OB_ERR_UNEXPECTED;
      YYSYS_LOG(WARN, "get table item and column_item failed, table_id=[%lu], column_id=[%lu]", table_id, column_id);
    }
    else
    {
      switch(table_item->type_)
      {
        case TableItem::BASE_TABLE:
        case TableItem::ALIAS_TABLE:
        {
          OB_ASSERT(schema_checker);
          const ObColumnSchemaV2 *column_schema = NULL;
          column_schema = schema_checker->get_column_schema(table_item->ref_id_, column_item->column_id_);
          if (column_schema == NULL)
          {
            ret = OB_ERR_UNEXPECTED;
            YYSYS_LOG(WARN, "column_schema is null.");
          }
          else
          {
            column_def->not_null_ = (!column_schema->is_nullable());
            if(!column_schema->get_default_value_is_nullable())
            {
              const_cast<ObColumnSchemaV2 *> (column_schema)->calc_default_val_obj(column_def->default_value_);
            }
            else
            {
              column_def->default_value_.set_null();
            }
          }
          break;
        }
        case TableItem::GENERATED_TABLE:
        {
          const SelectItem *select_item = NULL;
          ObSelectStmt *sub_select_stmt = dynamic_cast<ObSelectStmt *>(logical_plan->get_query(table_item->ref_id_));
          int32_t select_item_size = sub_select_stmt->get_select_item_size();
          for (int32_t i = 0; i < select_item_size; i++)
          {
            const SelectItem &sub_select_item = sub_select_stmt->get_select_item(i);
            if (sub_select_item.expr_name_.compare(column_item->column_name_) == 0
                || sub_select_item.expr_name_.compare(*expr_name) == 0)
            {
              select_item = &sub_select_item;
              sql_expr = logical_plan->get_expr(sub_select_item.expr_id_);
              break;
            }
          }
          if (select_item == NULL)
          {
            ret = OB_ERR_UNEXPECTED;
            snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "unexpected error");
          }
          else
          {
            if (select_item->is_real_alias_)
            {
              expr_name = const_cast<ObString *> (sub_select_stmt->get_raw_column(select_item->expr_id_));
            }
            else
            {
              expr_name = &((const_cast<SelectItem *> (select_item))->expr_name_);
            }
            ret = resolve_view_elements(result_plan, logical_plan, schema_checker, sub_select_stmt,
                                        expr_name, sql_expr, column_items, table_items, column_def);
          }
        }
        default:
          break;
      }
    }
  }
  else
  {
    column_def->not_null_ = true;
  }
  return ret;
}

int resolve_drop_view_stmt(
    ResultPlan *result_plan,
    ParseNode *node,
    uint64_t &query_id)
{
  int ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  OB_ASSERT(node && node->num_child_ == 2 && node->type_ == T_DROP_VIEW);
  ObLogicalPlan *logical_plan = NULL;
  ObDropViewStmt *drop_view_stmt = NULL;
  query_id = OB_INVALID_ID;
  ObStringBuf *name_pool = static_cast<ObStringBuf *>(result_plan->name_pool_);
  if (result_plan->plan_tree_ == NULL)
  {
    logical_plan = (ObLogicalPlan *) parse_malloc(sizeof(ObLogicalPlan), result_plan->name_pool_);
    if(logical_plan == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "can not malloc YaoLogicalPlan");
    }
    else
    {
      logical_plan = new(logical_plan) ObLogicalPlan(name_pool);
      result_plan->plan_tree_ = logical_plan;
    }
  }
  else
  {
    logical_plan = static_cast<ObLogicalPlan *>(result_plan->plan_tree_);
  }
  if (ret == OB_SUCCESS)
  {
    drop_view_stmt = (ObDropViewStmt *)parse_malloc(sizeof(ObDropViewStmt), result_plan->name_pool_);
    if (drop_view_stmt == NULL)
    {
      ret = OB_ERR_PARSER_MALLOC_FAILED;
      snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "can not malloc YaoDropViewStmt");
    }
    else
    {
      drop_view_stmt = new(drop_view_stmt)ObDropViewStmt(name_pool);
      query_id = logical_plan->generate_query_id();
      if (OB_SUCCESS != (ret = logical_plan->add_query(drop_view_stmt)))
      {
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "can not add YaoDropViewStmt to logical plan");
      }
    }
  }
  if (ret == OB_SUCCESS && node->children_[0])
  {
    drop_view_stmt->set_if_exists(true);
  }
  if (ret == OB_SUCCESS)
  {
    OB_ASSERT(node->children_[1] && node->children_[1]->num_child_ > 0);
    OB_ASSERT(node->children_[1]->type_ == T_TABLE_LIST);
    ObString db_name;
    char buf[OB_MAX_DATBASE_NAME_LENGTH + OB_MAX_TABLE_NAME_LENGTH + 1];
    ObString dt;
    dt.assign_buffer(buf, OB_MAX_DATBASE_NAME_LENGTH + OB_MAX_TABLE_NAME_LENGTH + 1);
    ParseNode *table_node = NULL;
    ObString table_name;
    for(int32_t i = 0; i < node->children_[1]->num_child_ && OB_SUCCESS == ret; i++)
    {
      table_node= node->children_[1]->children_[i];
      if (T_RELATION != table_node->type_)
      {
        ret = OB_ERR_TABLE_UNKNOWN;
        snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "Unkown view name!");
      }
      else
      {
        if (table_node->children_[0] != NULL)
        {
          db_name.assign_ptr((char *) (table_node->children_[0]->str_value_),
                             static_cast<int32_t>(strlen(table_node->children_[0]->str_value_)));
        }
        else
        {
          db_name = static_cast<ObSQLSessionInfo *>(result_plan->session_info_)->get_db_name();
        }
        if (db_name.length() <= 0)
        {
          ret = OB_INVALID_ARGUMENT;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "database name is null, you must specify the database when drop table.");
        }
        if(table_node->children_[1] != NULL)
        {
          table_name.assign_ptr((char *)(table_node->children_[1]->str_value_), static_cast<int32_t>(strlen(table_node->children_[1]->str_value_)));
        }
        if (table_name.length() <= 0)
        {
          ret  = OB_INVALID_ARGUMENT;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,
                   "view name is null, you must specify the view name when drop table.");
        }
        else if (table_name.length() >= OB_MAX_TABLE_NAME_LENGTH)
        {
          ret = OB_INVALID_ARGUMENT;
          snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "view name is too long");
        }
        else
        {
          dt.concat(db_name, table_name);
          if (OB_SUCCESS != (ret = drop_view_stmt->add_view_name_id(*result_plan, dt)))
          {
            break;
          }
        }
      }
    }
  }
  return ret;
}

////////////////////////////////////////////////////////////////
int resolve(ResultPlan* result_plan, ParseNode* node)
{
  if (!result_plan)
  {
    YYSYS_LOG(ERROR, "null result_plan");
    return OB_ERR_RESOLVE_SQL;
  }
  int& ret = result_plan->err_stat_.err_code_ = OB_SUCCESS;
  if (ret == OB_SUCCESS && result_plan->name_pool_ == NULL)
  {
    ret = OB_ERR_RESOLVE_SQL;
    PARSER_LOG("name_pool_ nust be set");
  }
  if (ret == OB_SUCCESS && result_plan->schema_checker_ == NULL)
  {
    ret = OB_ERR_RESOLVE_SQL;
    PARSER_LOG("schema_checker_ must be set");
  }

  if (OB_LIKELY(OB_SUCCESS == ret))
  {
    bool is_preparable = false;
    switch (node->type_)
    {
      case T_STMT_LIST:
      case T_SELECT:
      case T_DELETE:
      case T_INSERT:
      case T_UPDATE:
      case T_BEGIN:
      case T_COMMIT:
      case T_ROLLBACK:
        is_preparable = true;
        break;
      default:
        break;
    }
    if (result_plan->is_prepare_ && !is_preparable)
    {
      ret = OB_ERR_RESOLVE_SQL;
      PARSER_LOG("the statement can not be prepared");
    }
  }

  uint64_t query_id = OB_INVALID_ID;
  if (ret == OB_SUCCESS && node != NULL)
  {
    switch (node->type_)
    {
      case T_STMT_LIST:
      {
        ret = resolve_multi_stmt(result_plan, node);
        break;
      }
      case T_SELECT:
      {
        ret = resolve_select_stmt(result_plan, node, query_id);
        break;
      }
      case T_DELETE:
      {
        ret = resolve_delete_stmt(result_plan, node, query_id);
        break;
      }
      case T_INSERT:
      {
        ret = resolve_insert_stmt(result_plan, node, query_id);
        break;
      }
      case T_UPDATE:
      {
        ret = resolve_update_stmt(result_plan, node, query_id);
        break;
      }
      case T_EXPLAIN:
      {
        ret = resolve_explain_stmt(result_plan, node, query_id);
        break;
      }
      case T_CREATE_TABLE:
      {
        ret = resolve_create_table_stmt(result_plan, node, query_id);
        break;
      }
        //add liu jun. [MultiUPS] [sql_api] 20150421:b
      case T_CREATE_PART_FUNC:
      {
        ret = resolve_create_part_func_stmt(result_plan, node, query_id);
        break;
      }
      case T_DROP_PART_FUNC:
      {
        ret = resolve_drop_part_func_stmt(result_plan, node, query_id);
        break;
      }
        //add 20150421:e
        //add by wenghaixing [secondary index] 20141024
      case T_CREATE_INDEX:
      {
        ret=resolve_create_index_stmt(result_plan, node, query_id);
        break;
      }
        //add e
        //add wenghaixing [secondary index drop index]20141223
      case T_DROP_INDEX:
      {
        ret=resolve_drop_index_stmt(result_plan, node, query_id);
        break;
      }
        //add e
      case T_DROP_TABLE:
      {
        ret = resolve_drop_table_stmt(result_plan, node, query_id);
        break;
      }
      case T_ALTER_TABLE:
      {
        ret = resolve_alter_table_stmt(result_plan, node, query_id);
        break;
      }
        //add zhaoqiong [truncate stmt] 20151204:b
      case T_TRUNCATE_TABLE:
      {
        ret = resolve_truncate_table_stmt(result_plan, node, query_id);
        break;
      }
        //add:e
      case T_ALTER_GROUP:
      {
        ret = resolve_alter_group_stmt(result_plan, node, query_id);
        break;
      }
      case T_SHOW_TABLES:
        //add liumengzhan_show_index [20141208]
      case T_SHOW_INDEX:
        //add:e
      case T_SHOW_ALL_INDEX:
      case T_SHOW_VARIABLES:
      case T_SHOW_COLUMNS:
      case T_SHOW_SCHEMA:
      case T_SHOW_SYSTEM_TABLES:// add by zhangcd [multi_database.show_tables] 20150616
      case T_SHOW_CREATE_INDEX:
      case T_SHOW_CREATE_TABLE:
      case T_SHOW_CREATE_VIEW:
      case T_SHOW_TABLE_STATUS:
      case T_SHOW_SERVER_STATUS:
      case T_SHOW_WARNINGS:
      case T_SHOW_GRANTS:
      case T_SHOW_PARAMETERS:
      case T_SHOW_FUNCTIONS:
      case T_SHOW_TABLE_RULES://add wuna.[MultiUPS] [sql_api] 20160223
      case T_SHOW_GROUPS://add wuna.[MultiUPS] [sql_api] 20160223
      case T_SHOW_CURRENT_PAXOS_ID:
      case T_SHOW_PROCESSLIST :
      case T_SHOW_DATABASES://add dolphin [show database]@20150604
      case T_SHOW_CURRENT_DATABASE:// add by zhangcd [multi_database.show_databases] 20150617
      {
        ret = resolve_show_stmt(result_plan, node, query_id);
        break;
      }
      case T_CREATE_USER:
      {
        ret = resolve_create_user_stmt(result_plan, node, query_id);
        break;
      }
      case T_DROP_USER:
      {
        ret = resolve_drop_user_stmt(result_plan, node, query_id);
        break;
      }
        //add wenghaixing [database manage]20150608
      case T_CREATE_DATABASE:
      {
        ret =  resolve_create_db_stmt(result_plan, node, query_id);
        break;
      }
      case T_USE_DATABASE:
      {
        ret =  resolve_use_db_stmt(result_plan, node, query_id);
        break;
      }
      case T_DROP_DATABASE:
      {
        ret = resolve_drop_db_stmt(result_plan, node, query_id);
        break;
      }
        //add e
      case T_SET_PASSWORD:
      {
        ret = resolve_set_password_stmt(result_plan, node, query_id);
        break;
      }
      case T_RENAME_USER:
      {
        ret = resolve_rename_user_stmt(result_plan, node, query_id);
        break;
      }
      case T_LOCK_USER:
      {
        ret = resolve_lock_user_stmt(result_plan, node, query_id);
        break;
      }
      case T_GRANT:
      {
        ret = resolve_grant_stmt(result_plan, node, query_id);
        break;
      }
      case T_REVOKE:
      {
        ret = resolve_revoke_stmt(result_plan, node, query_id);
        break;
      }
      case T_PREPARE:
      {
        ret = resolve_prepare_stmt(result_plan, node, query_id);
        break;
      }
      case T_VARIABLE_SET:
      {
        ret = resolve_variable_set_stmt(result_plan, node, query_id);
        break;
      }
      case T_EXECUTE:
      {
        ret = resolve_execute_stmt(result_plan, node, query_id);
        break;
      }
      case T_DEALLOCATE:
      {
        ret = resolve_deallocate_stmt(result_plan, node, query_id);
        break;
      }
      case T_BEGIN:
        ret = resolve_start_trans_stmt(result_plan, node, query_id);
        break;
      case T_COMMIT:
        ret = resolve_commit_stmt(result_plan, node, query_id);
        break;
      case T_ROLLBACK:
        ret = resolve_rollback_stmt(result_plan, node, query_id);
        break;
      case T_ALTER_SYSTEM:
        ret = resolve_alter_sys_cnf_stmt(result_plan, node, query_id);
        break;
      case T_KILL:
        ret = resolve_kill_stmt(result_plan, node, query_id);
        break;
      case T_CHANGE_OBI:
        ret = resolve_change_obi(result_plan, node, query_id);
        break;
        //add liuzy [sequence] [JHOBv0.1] 20150327:b
      case T_SEQUENCE_CREATE:
        ret = resolve_sequence_create_stmt(result_plan, node, query_id);
        break;
      case T_SEQUENCE_DROP:
        ret = resolve_sequence_drop_stmt(result_plan, node, query_id);
        break;
        //add 20150327:e
      case T_GATHER_STATISTICS:
        ret = resolve_gather_statistics_stmt(result_plan, node, query_id);
        break;
      case T_CREATE_VIEW:
        ret = resolve_create_view_stmt(result_plan, node, query_id);
        break;
      case T_DROP_VIEW:
        ret = resolve_drop_view_stmt(result_plan, node, query_id);
        break;
      default:
        YYSYS_LOG(ERROR, "unknown top node type=%d", node->type_);
        ret = OB_ERR_UNEXPECTED;
        break;
    };
  }
  if (ret == OB_SUCCESS && result_plan->is_prepare_ != 1
      && node->type_ != T_STMT_LIST && node->type_ != T_PREPARE)
  {
    ObLogicalPlan* logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
    if (logical_plan != NULL && logical_plan->get_question_mark_size() > 0)
    {
      ret = OB_ERR_PARSE_SQL;
      PARSER_LOG("Uknown column '?'");
    }
  }
  if (ret != OB_SUCCESS && result_plan->plan_tree_ != NULL)
  {
    ObLogicalPlan* logical_plan = static_cast<ObLogicalPlan*>(result_plan->plan_tree_);
    logical_plan->~ObLogicalPlan();
    parse_free(result_plan->plan_tree_);
    result_plan->plan_tree_ = NULL;
  }
  return ret;
}

extern void destroy_plan(ResultPlan* result_plan)
{
  if (result_plan->plan_tree_ == NULL)
    return;

  //delete (static_cast<multi_plan*>(result_plan->plan_tree_));
  parse_free(result_plan->plan_tree_);

  result_plan->plan_tree_ = NULL;
  result_plan->name_pool_ = NULL;
  result_plan->schema_checker_ = NULL;
  result_plan->session_info_ = NULL;//add liumz, [multi_database.sql]:20150613
  memset(result_plan->err_stat_.err_msg_,0,MAX_ERROR_MSG);//add wuna  [MultiUPS] [sql_api] 20160627
}
