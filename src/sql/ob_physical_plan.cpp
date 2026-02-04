/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_physical_plan.cpp
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#include "ob_physical_plan.h"
#include "common/utility.h"
#include "ob_table_rpc_scan.h"
#include "ob_mem_sstable_scan.h"
#include "common/serialization.h"
#include "ob_phy_operator_factory.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::common::serialization;
namespace oceanbase
{
  namespace sql
  {
    //uint64_t phycount=0;
    REGISTER_CREATOR(oceanbase::sql::ObPhyPlanGFactory, ObPhysicalPlan, ObPhysicalPlan, 0);
  } // end namespace sql
} // end namespace oceanbase


ObPhysicalPlan::ObPhysicalPlan()
  :curr_frozen_version_(OB_INVALID_VERSION),
    ts_timeout_us_(0),
    main_query_(NULL),
    pre_query_id_(common::OB_INVALID_ID),
    operators_store_(common::OB_COMMON_MEM_BLOCK_SIZE, ModulePageAllocator(ObModIds::OB_SQL_PHY_PLAN)),
    allocator_(NULL),
    op_factory_(NULL),
    my_result_set_(NULL),
    start_trans_(false),
    in_ups_executor_(false),
    cons_from_assign_(false),
    next_phy_operator_id_(0),need_extend_time_(false)//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140713
  //add tianz [EXPORT_TOOL] 20141120:b
  ,has_range_(false),start_is_min_(false),end_is_max_(false)
  //add 20141120:e
   //add shili [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20150705:b
   ,table_id_(OB_INVALID_ID),stmt_type_(ObBasicStmt::T_NONE),row_mem_allocator_(ModuleArena::DEFAULT_PAGE_SIZE)
   //add 20150705:e
   //add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20151124:b
   ,ups_executor_op_id_(OB_INVALID_ID),expr_value_op_id_(OB_INVALID_ID),values_op_id_(OB_INVALID_ID),project_op_id_ (OB_INVALID_ID),table_rpc_scan_op_id_ (OB_INVALID_ID)
   //add 20151124:e
   ,is_insert_select_flag_(false),ob_multiple_merge_op_id_(OB_INVALID_ID)//add by maosy [MultiUps 1.0] [batch_iud_snapshot] 20170622 b:
  ,index_expr_value_op_id_(OB_INVALID_ID)
{
  //add shili [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20150705:b
  row_mem_allocator_.set_mod_id(ObModIds::OB_SQL_PHYSICAL_PLAN_ROW_STORE);
  //add 20150705:e
    //add tianz [EXPORT_TOOL] 20141120:b
    for (int i = 0; i < OB_MAX_ROWKEY_COLUMN_NUMBER; i++)
    {
      range_start_objs_[i].set_min_value();
      range_end_objs_[i].set_max_value();
    }
	//add 20141120:e
}

ObPhysicalPlan::~ObPhysicalPlan()
{
  clear();
}

int ObPhysicalPlan::deserialize_header(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = serialization::decode_bool(buf, data_len, pos, &start_trans_)))
  {
    YYSYS_LOG(WARN, "failed to decode start_trans_, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = start_trans_req_.deserialize(buf, data_len, pos)))
  {
    YYSYS_LOG(WARN, "failed to decode start_trans_, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = trans_id_.deserialize(buf, data_len, pos)))
  {
    YYSYS_LOG(WARN, "failed to decode trans_id_, err=%d", ret);
  }
  return ret;
}

int ObPhysicalPlan::add_phy_query(ObPhyOperator *phy_query, int32_t* idx, bool main_query)
{
  int ret = OB_SUCCESS;
  if ( (ret = phy_querys_.push_back(phy_query)) == OB_SUCCESS)
  {
    if (idx != NULL)
      *idx = static_cast<int32_t>(phy_querys_.count() - 1);
    if (main_query)
      main_query_ = phy_query;
  }
  return ret;
}

int ObPhysicalPlan::set_pre_phy_query(ObPhyOperator *phy_query, int32_t* idx)
{
  int ret = OB_SUCCESS;
  if ( (ret = phy_querys_.push_back(phy_query)) == OB_SUCCESS)
  {
    if (phy_query)
      pre_query_id_ = phy_query->get_id();
    if (idx != NULL)
    {
      *idx = static_cast<int32_t>(phy_querys_.count() - 1);
    }
  }
  return ret;
}

int ObPhysicalPlan::store_phy_operator(ObPhyOperator *op)
{
  op->set_id(++next_phy_operator_id_);
  return operators_store_.push_back(op);
}

ObPhyOperator* ObPhysicalPlan::get_phy_query(int32_t index) const
{
  ObPhyOperator *op = NULL;
  if (index >= 0 && index < phy_querys_.count())
    op = phy_querys_.at(index);
  return op;
}

ObPhyOperator* ObPhysicalPlan::get_phy_query_by_id(uint64_t id) const
{
  ObPhyOperator *op = NULL;
  for(int64_t i = 0; i < phy_querys_.count(); i++)
  {
    if (phy_querys_.at(i)->get_id() == id)
    {
      op = phy_querys_.at(i);
      break;
    }
  }
  //add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20140715:b
  /*Exp:after chang physical plan for insert ... select ,
  * the two operator do not in phy_querys_ any more,
  * need get them from operators_store_
  */
  if(NULL == op)
  {
  for(int64_t i = 0; i < operators_store_.count(); i++)
  {
    if (operators_store_.at(i)->get_id() == id)
    {
      op = operators_store_.at(i);
      break;
    }
  }
  }
  //add 20140715:e
  return op;
}

//add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20151124:b
ObPhyOperator* ObPhysicalPlan::get_phy_op_by_id_from_operator_store(uint64_t id) const
{
  ObPhyOperator *op = NULL;
  int64_t size = operators_store_.count();
  for(int64_t i = 0; i < size; i++)
  {
    if (NULL != operators_store_.at(i))
    {
      if (operators_store_.at(i)->get_id() == id)
      {
        op = operators_store_.at(i);
        break;
      }
    }
    else
    {
      YYSYS_LOG(WARN, "operators store[%ld] is NULL",i);
      break;
    }
  }
  return op;
}

int ObPhysicalPlan::check_version_range_validity(uint64_t active_mem_table_version, uint64_t curr_frozen_version)
{
  int ret = OB_SUCCESS;
  YYSYS_LOG(DEBUG,"acvive version is [%ld], plan frozen version is [%ld]",active_mem_table_version, curr_frozen_version);
  if (active_mem_table_version != (uint64_t)(curr_frozen_version + 1))
  {
    //ret = OB_INVALID_START_VERSION;
    ret = OB_CHECK_VERSION_RETRY;
    YYSYS_LOG(WARN, "plan version is not serial,acvive version is [%ld], plan frozen version is [%ld], ret=[%d]",
              active_mem_table_version, curr_frozen_version, ret);
  }
  return ret;
}
//add 20151124:e
ObPhyOperator* ObPhysicalPlan::get_phy_operator(int64_t index) const
{
  ObPhyOperator *op = NULL;
  if (index >= 0 && index < operators_store_.count())
    op = operators_store_.at(index);
  return op;
}

ObPhyOperator* ObPhysicalPlan::get_main_query() const
{
  return main_query_;
}

void ObPhysicalPlan::set_main_query(ObPhyOperator *query)
{
  main_query_ = query;
}

ObPhyOperator* ObPhysicalPlan::get_pre_query() const
{
  return get_phy_query_by_id(pre_query_id_);
}

int ObPhysicalPlan::remove_phy_query(ObPhyOperator *phy_query)
{
  int ret = OB_SUCCESS;
  UNUSED(phy_query);
  // if (OB_SUCCESS != (ret = phy_querys_.remove_if(phy_query)))
  // {
  //   YYSYS_LOG(WARN, "phy query not exist, phy_query=%p", phy_query);
  // }
  return ret;
}

int ObPhysicalPlan::remove_phy_query(int32_t index)
{
  int ret = OB_SUCCESS;
  UNUSED(index);
  if (OB_SUCCESS != (ret = phy_querys_.remove(index)))
  {
    YYSYS_LOG(WARN, "phy query not exist, index=%d", index);
  }
  return ret;
}

bool ObPhysicalPlan::is_terminate(int &ret) const
{
  bool bret = false;
  (void)bret;
  if (NULL != my_result_set_)
  {
    ObSQLSessionInfo *session = my_result_set_->get_session();
    if (NULL != session)
    {
      if (QUERY_KILLED == session->get_session_state())
      {
        bret = true;
        YYSYS_LOG(WARN, "query(%.*s) interrupted session id=%lu", session->get_current_query_string().length(),
                  session->get_current_query_string().ptr(), session->get_session_id());
        ret = OB_ERR_QUERY_INTERRUPTED;
      }
      else if (SESSION_KILLED == session->get_session_state())
      {
        bret = true;
        ret = OB_ERR_SESSION_INTERRUPTED;
      }
    }
    else
    {
      YYSYS_LOG(WARN, "can not get session_info for current query result set is %p", my_result_set_);
    }
  }
  return ret;
}

// must be unique table_id
int ObPhysicalPlan::add_base_table_version(int64_t table_id, int64_t version)
{
  ObTableVersion table_version;
  table_version.table_id_ = table_id;
  table_version.version_ = version;
  return table_store_.push_back(table_version);
}

int ObPhysicalPlan::add_base_table_version(const ObTableVersion table_version)
{
  return table_store_.push_back(table_version);
}

int ObPhysicalPlan::get_base_table_version(int64_t table_id, int64_t& version)
{
  int ret = OB_ERR_TABLE_UNKNOWN;
  for (int32_t i = 0; i < table_store_.count(); ++i)
  {
    ObTableVersion& table_version = table_store_.at(i);
    if (table_version.table_id_ == table_id)
    {
      ret = OB_SUCCESS;
      version = table_version.version_;
    }
  }
  return ret;
}

const ObPhysicalPlan::ObTableVersion& ObPhysicalPlan::get_base_table_version(int64_t index) const
{
  OB_ASSERT(0 <= index && index < table_store_.count());
  return table_store_.at(index);
}

int64_t ObPhysicalPlan::get_base_table_version_count()
{
  return table_store_.count();
}

bool ObPhysicalPlan::is_user_table_operation() const
{
  bool ret = true;
  for (int32_t i = 0; i < table_store_.count(); ++i)
  {
    const ObTableVersion& table_version = table_store_.at(i);
    if (IS_SYS_TABLE_ID(table_version.table_id_))
    {
      ret = false;
      break;
    }
  }
  return ret;
}

int64_t ObPhysicalPlan::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "PhysicalPlan(operators_num=%ld query_num=%ld "
                  "trans_id=%s start_trans=%c trans_req=%s)\n",
                  operators_store_.count(), phy_querys_.count(),
                  to_cstring(trans_id_), start_trans_?'Y':'N', to_cstring(start_trans_req_));
  for (int32_t i = 0; i < phy_querys_.count(); ++i)
  {
    if (main_query_ == phy_querys_.at(i))
      databuff_printf(buf, buf_len, pos, "====MainQuery====\n");
    else
      databuff_printf(buf, buf_len, pos, "====SubQuery%d====\n", i);
    int64_t pos2 = phy_querys_.at(i)->to_string(buf + pos, buf_len-pos);
    pos += pos2;
  }
  return pos;
}

int ObPhysicalPlan::deserialize_tree(const char *buf, int64_t data_len, int64_t &pos, ModuleArena &allocator,
                                     OperatorStore &operators_store, ObPhyOperator *&root)
{
  int ret = OB_SUCCESS;
  int32_t phy_operator_type = 0;
  if (NULL == op_factory_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR, "op_factory == NULL");
  }
  else if (OB_SUCCESS != (ret = decode_vi32(buf, data_len, pos, &phy_operator_type)))
  {
    YYSYS_LOG(WARN, "fail to decode phy operator type:ret[%d]", ret);
  }
  if (OB_SUCCESS == ret)
  {
    root = op_factory_->get_one(static_cast<ObPhyOperatorType>(phy_operator_type), allocator);
    if (NULL == root)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      YYSYS_LOG(WARN, "get operator fail:type[%d]", phy_operator_type);
    }
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = root->deserialize(buf, data_len, pos)))
    {
      YYSYS_LOG(WARN, "fail to deserialize operator:ret[%d]", ret);
    }
    else if (OB_SUCCESS != (ret = operators_store.push_back(root)))
    {
      YYSYS_LOG(WARN, "fail to push operator to operators_store:ret[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (root->get_type() <= PHY_INVALID || root->get_type() >= PHY_END)
    {
      ret = OB_ERR_UNEXPECTED;
      YYSYS_LOG(WARN, "invalid operator type:[%d]", root->get_type());
    }
  }

  if (OB_SUCCESS == ret)
  {
    for (int32_t i=0; OB_SUCCESS == ret && i<root->get_child_num(); i++)
    {
      ObPhyOperator *child = NULL;
      if (OB_SUCCESS != (ret = deserialize_tree(buf, data_len, pos, allocator, operators_store, child)))
      {
        YYSYS_LOG(WARN, "fail to deserialize tree:ret[%d]", ret);
      }
      //modify by fanqiushi_index
      else
      {
          //YYSYS_LOG(ERROR, "test::fanqs7 root type:[%d],,root->get_child_num=%d,,child=%s", root->get_type(),root->get_child_num(),to_cstring(*child));
          if (OB_SUCCESS != (ret = root->set_child(i, *child)))
          {
             YYSYS_LOG(WARN, "fail to set child:ret[%d]", ret);
          }
      }
      //modify:e
    }
  }
  return ret;
}

int ObPhysicalPlan::serialize_tree(char *buf, int64_t buf_len, int64_t &pos, const ObPhyOperator &root) const
{
  int ret = OB_SUCCESS;

  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = encode_vi32(buf, buf_len, pos, root.get_type())))
    {
      YYSYS_LOG(WARN, "fail to encode op type:ret[%d]", ret);
    }
    else if (OB_SUCCESS != (ret = root.serialize(buf, buf_len, pos)))
    {
      YYSYS_LOG(WARN, "fail to serialize root:ret[%d] type=%d op=%s", ret, root.get_type(), to_cstring(root));
    }
    else
    {
      YYSYS_LOG(DEBUG, "serialize operator succ, type=%d", root.get_type());
    }
  }

  for (int64_t i=0;OB_SUCCESS == ret && i<root.get_child_num();i++)
  {
    if (NULL != root.get_child(static_cast<int32_t>(i)) )
    {
      if (OB_SUCCESS != (ret = serialize_tree(buf, buf_len, pos, *(root.get_child(static_cast<int32_t>(i))))))
      {
        YYSYS_LOG(WARN, "fail to serialize tree:ret[%d]", ret);
      }
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      YYSYS_LOG(WARN, "this operator should has child:type[%d]", root.get_type());
    }
  }
  return ret;
}

int ObPhysicalPlan::assign(const ObPhysicalPlan& other)
{
  int ret = OB_SUCCESS;
  if (this == &other)
  {
    // skip
  }
  else if (phy_querys_.count() > 0 || operators_store_.count() > 0)
  {
    ret = OB_ERR_UNEXPECTED;
    YYSYS_LOG(WARN, "ObPhysicalPlan is not emptyo to assign, ret=%d", ret);
  }
  else
  {
    trans_id_ = other.trans_id_;
    curr_frozen_version_ = other.curr_frozen_version_;
    ts_timeout_us_ = other.ts_timeout_us_;
    main_query_ = NULL;
    pre_query_id_ = other.pre_query_id_;
    // already set before
    // allocator_;
    // op_factory_;
    // my_result_set_; // The result set who owns this physical plan
    start_trans_ = other.start_trans_;
    start_trans_req_ = other.start_trans_req_;
    for (int32_t i = 0; i < other.phy_querys_.count(); ++i)
    {
      const ObPhyOperator *subquery = other.phy_querys_.at(i);
      bool is_main_query = (subquery == other.main_query_);
      ObPhyOperator *out_op = NULL;
      ret = create_and_assign_tree(subquery, is_main_query, true, out_op);
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "failed to assign tree, err=%d", ret);
        break;
      }
      YYSYS_LOG(DEBUG, "copy subquery=%d", i);
    }
    for (int32_t i = 0; ret == OB_SUCCESS && i < other.table_store_.count(); ++i)
    {
      if ((ret = table_store_.push_back(other.table_store_.at(i))) != OB_SUCCESS)
      {
        YYSYS_LOG(WARN, "Assign table version failed, err=%d, idx=%d", ret, i);
        break;
      }
    }
    cons_from_assign_ = true;
    need_extend_time_ = other.need_extend_time_;//add tianz [SubQuery_for_Instmt] [JHOBv0.1] 20140713 
    ups_executor_op_id_ = other.ups_executor_op_id_;
    expr_value_op_id_= other.expr_value_op_id_;
    values_op_id_= other.values_op_id_;
    project_op_id_ = other.project_op_id_;
    table_rpc_scan_op_id_ = other.table_rpc_scan_op_id_;
    ob_multiple_merge_op_id_ = other.ob_multiple_merge_op_id_;//add by maosy [MultiUps 1.0] [batch_iud_snapshot] 20170622 b:
    index_expr_value_op_id_ = other.index_expr_value_op_id_;
  }
  return ret;
}

int ObPhysicalPlan::create_and_assign_tree(
    const ObPhyOperator *other,
    bool main_query,
    bool is_query,
    ObPhyOperator *&out_op)
{
  int ret = OB_SUCCESS;
  ObPhyOperator *op = NULL;
  if (!other)
  {
    ret = OB_ERR_UNEXPECTED;
    YYSYS_LOG(WARN, "Operator to be assigned cna not be NULL, ret=%d", ret);
  }
  else if ((op = ObPhyOperator::alloc(other->get_type())) == NULL)
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    YYSYS_LOG(WARN, "Create operator fail:type[%d]", other->get_type());
  }
  else if ((ret = operators_store_.push_back(op)) != OB_SUCCESS)
  {
    // should free op here
    ObPhyOperator::free(op);
    YYSYS_LOG(WARN, "Fail to push operator to operators_store:ret[%d]", ret);
  }
  else if (is_query && (ret = this->add_phy_query(op, NULL, main_query)) != OB_SUCCESS)
  {
    YYSYS_LOG(WARN, "Add operator to physical plan failed, ret=%d", ret);
  }
  else
  {
    op->set_phy_plan(this);
    op->set_id(other->get_id());
    out_op = op;

    if ((ret = op->assign(other)) != OB_SUCCESS)
    {
      YYSYS_LOG(WARN, "Assign operator of physical plan failed, ret=%d", ret);
    }
    YYSYS_LOG(DEBUG, "assign operator, type=%s main_query=%c", ob_phy_operator_type_str(other->get_type()),
              main_query?'Y':'N');
  }
  for (int32_t i = 0; ret == OB_SUCCESS && i < other->get_child_num(); i++)
  {
    ObPhyOperator *child = NULL;
    if (!other->get_child(i))
    {
      ret = OB_ERR_GEN_PLAN;
      YYSYS_LOG(WARN, "Wrong physical plan, ret=%d", ret);
    }
    else if ((ret = create_and_assign_tree(other->get_child(i), false, false, child)) != OB_SUCCESS)
    {
      YYSYS_LOG(WARN, "Fail to create_and_assign tree:ret[%d]", ret);
    }
    else if (OB_SUCCESS != (ret = op->set_child(i, *child)))
    {
      YYSYS_LOG(WARN, "Fail to set child:ret[%d]", ret);
    }
  }
  return ret;
}

DEFINE_SERIALIZE(ObPhysicalPlan)
{
  int ret = OB_SUCCESS;
  // @todo yzf, support multiple queries
  int32_t main_query_idx = 0;
  // get current trans id
  OB_ASSERT(my_result_set_);
  //mod peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150804:b
  //common::ObTransID trans_id = my_result_set_->get_session()->get_trans_id();
  common::ObTransID trans_id = my_result_set_->get_session()->get_current_trans_id();
  //mod 20150804:e
  FILL_TRACE_LOG("trans_id=%s", to_cstring(trans_id));
  if (OB_SUCCESS != (ret = serialization::encode_bool(buf, buf_len, pos, start_trans_)))
  {
    YYSYS_LOG(WARN, "failed to serialize trans_id_, err=%d buf_len=%ld pos=%ld",
              ret, buf_len, pos);
  }
  else if (OB_SUCCESS != (ret = start_trans_req_.serialize(buf, buf_len, pos)))
  {
    YYSYS_LOG(WARN, "serialize error, buf_len=%ld pos=%ld", buf_len, pos);
  }
  else if (OB_SUCCESS != (ret = trans_id.serialize(buf, buf_len, pos)))
  {
    YYSYS_LOG(ERROR, "trans_id.serialize(buf=%p[%ld-%ld])=>%d", buf, pos, buf_len, ret);
  }
  //add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20151207:b
  else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, curr_frozen_version_)))
  {
    YYSYS_LOG(WARN, "failed to encode curr_frozen_version_,err=%d", ret);
  }
  //add 20151207:e
  else if (OB_SUCCESS != (ret = encode_vi32(buf, buf_len, pos, main_query_idx)))
  {
    YYSYS_LOG(WARN, "fail to encode main query idx:ret[%d]", ret);
  }
  else if (OB_SUCCESS != (ret = encode_vi32(buf, buf_len, pos, 1)))
  {
    YYSYS_LOG(WARN, "fail to encode phy queryes size :ret[%d]", ret);
  }
  else if (OB_SUCCESS != (ret = serialize_tree(buf, buf_len, pos, *main_query_)))
  {
    YYSYS_LOG(WARN, "fail to serialize tree:ret[%d]", ret);
  }
  return ret;
}

DEFINE_DESERIALIZE(ObPhysicalPlan)
{
  int ret = OB_SUCCESS;
  int32_t main_query_idx = -1;
  int32_t phy_querys_size = 0;
  ObPhyOperator *root = NULL;
  clear();
  if (NULL == allocator_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(WARN, "allocator_ is not setted");
  }
  else if (OB_SUCCESS != (ret = serialization::decode_bool(buf, data_len, pos, &start_trans_)))
  {
    YYSYS_LOG(WARN, "failed to decode start_trans_, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = start_trans_req_.deserialize(buf, data_len, pos)))
  {
    YYSYS_LOG(WARN, "failed to decode start_trans_, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = trans_id_.deserialize(buf, data_len, pos)))
  {
    YYSYS_LOG(ERROR, "trans_id.deserialize(buf=%p[%ld-%ld])=>%d", buf, pos, data_len, ret);
  }
  //add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20151207:b
  else if (OB_SUCCESS != (ret = serialization::decode_vi64(buf, data_len, pos, &curr_frozen_version_)))
  {
    YYSYS_LOG(WARN, "failed to decode curr_frozen_version_, err=%d", ret);
  }
  //add 20151207:e
  else if (OB_SUCCESS != (ret = decode_vi32(buf, data_len, pos, &main_query_idx)))
  {
    YYSYS_LOG(WARN, "fail to decode main query idx:ret[%d]", ret);
  }
  else if (OB_SUCCESS != (ret = decode_vi32(buf, data_len, pos, &phy_querys_size)))
  {
    YYSYS_LOG(WARN, "fail to decode phy querys size:ret[%d]", ret);
  }

  for (int32_t i=0;OB_SUCCESS == ret && i<phy_querys_size;i++)
  {
    if (OB_SUCCESS != (ret = deserialize_tree(buf, data_len, pos, *allocator_, operators_store_, root)))
    {
      YYSYS_LOG(WARN, "fail to deserialize_tree:ret[%d]", ret);
    }
    else if (OB_SUCCESS != (ret = phy_querys_.push_back(root)))
    {
      YYSYS_LOG(WARN, "fail to push item to phy querys:ret[%d]", ret);
    }
  }

  if (OB_LIKELY(OB_SUCCESS == ret))
  {
    if (OB_SUCCESS != phy_querys_.at(main_query_idx, main_query_))
    {
      ret = OB_ERR_UNEXPECTED;
      YYSYS_LOG(WARN, "fail to get main query:main_query_idx[%d], size[%ld]", main_query_idx, phy_querys_.count());
    }
  }

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObPhysicalPlan)
{
  int64_t size = 0;
  return size;
}
