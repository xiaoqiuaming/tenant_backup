/**
 * ob_multi_ups_multi_get.h defined for select rpc data from MultiUPS to CS using GET,
 * in MultiUPS,for select,the UPS get become from 1 to N (N>=1)
 *
 * Version: $Id
 *
 * Authors:
 *   lijianqiang <lijianqiang@mail.nwpu.edu.cn>
 *     --some work detials if U want
 */

#include"ob_multi_ups_multi_get.h"
#include "chunkserver/ob_rpc_proxy.h"
#include "common/ob_row_fuse.h"
//add hongchen [PERFORMANCE_OPTI] 20170821:b
#include "common/ob_cached_allocator.h"
//add hongchen [PERFORMANCE_OPTI] 20170821:e
using namespace oceanbase;
using namespace sql;

//add hongchen [PERFORMANCE_OPTI] 20170821:b
static ObCachedAllocator<ObUpsMultiGet> MULTI_UPS_GET_ALLOC;
//add hongchen [PERFORMANCE_OPTI] 20170821:e

ObMultiUpsMultiGet::ObMultiUpsMultiGet()
  : get_param_(NULL),
    rpc_proxy_(NULL),
    row_desc_(NULL),
    ts_timeout_us_(0),
    stmt_type_(ObBasicStmt::T_SELECT),
    schema_manager_(NULL)

{
  //del hongchen [PERFORMANCE_OPTI] 20170821:b
  //allocator_.set_mod_id(common::ObModIds::OB_MULTI_UPS_PHY_GET);
  //del hongchen [PERFORMANCE_OPTI] 20170821:e
  trans_id_.reset();
  //add shili, [MultiUPS] [COLUMN_VERSION_MERGE]  20170423:b
  row_merger_.reset();
  //add e
}

ObMultiUpsMultiGet:: ~ObMultiUpsMultiGet()
{
  for (int64_t i=0;i<children_ops_.count();i++)
  {
    if (NULL != children_ops_.at(i))
    {
      //mod hongchen [PERFORMANCE_OPTI] 20170821:b
      //children_ops_.at(i)->~ObUpsMultiGet();
      MULTI_UPS_GET_ALLOC.free(children_ops_.at(i));
      children_ops_.at(i) = NULL;
      //mod hongchen [PERFORMANCE_OPTI] 20170821:e
    }
  }
  children_ops_.clear();
//  allocator_.free();//do not need free manual,the ModuleArena will free the mem when destruction the object
}

void ObMultiUpsMultiGet::reset()
{
    for (int64_t i=0;i<children_ops_.count();i++)
    {
      if (NULL != children_ops_.at(i))
      {
        //mod hongchen [PERFORMANCE_OPTI] 20170821:b
        //children_ops_.at(i)->~ObUpsMultiGet();
        MULTI_UPS_GET_ALLOC.free(children_ops_.at(i));
        //mod hongchen [PERFORMANCE_OPTI] 20170821:e
      }
      children_ops_.at(i) = NULL;
    }
    get_param_ = NULL;
    row_desc_ = NULL;
    trans_id_.reset();
    children_ops_.clear();
    //del hongchen [PERFORMANCE_OPTI] 20170821:b
    //allocator_.reuse();
    //del hongchen [PERFORMANCE_OPTI] 20170821:b
    //add shili, [MultiUPS] [COLUMN_VERSION_MERGE]  20170423:b
    row_merger_.reset();
    //add e
}

void ObMultiUpsMultiGet::reuse()
{
  this->reset();
}

int ObMultiUpsMultiGet::set_child(int32_t child_idx, ObPhyOperator &child_operator)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(child_idx);
  UNUSED(child_operator);
  YYSYS_LOG(ERROR, "not implement");
  return ret;
}

int ObMultiUpsMultiGet::open()
{
  int ret = OB_SUCCESS;
  row_merger_.reset();
  //1.create ups multi get ops according to the get_param for each paxos id
  if (OB_SUCCESS != (ret = create_ups_multi_get_ops()))
  {
    YYSYS_LOG(WARN, "create ups_multi_get ops failed, ret=%d",ret);
  }
  //2.open all paxos group multi get ops
  else
  {
    if (OB_UNLIKELY(children_ops_.count() <= 0))
    {
      ret = OB_NOT_INIT;
      YYSYS_LOG(ERROR, "No child op,ret=%d",ret);
    }
    else
    {
      YYSYS_LOG(DEBUG, "child op num is[%ld]",children_ops_.count());
      for (int32_t i = 0; ret == OB_SUCCESS && i < children_ops_.count(); i++)
      {
        if ((ret = children_ops_.at(i)->open()) != OB_SUCCESS)
        {
          YYSYS_LOG(WARN, "failed to open %dth child_op, err=%d", i, ret);
          break;
        }
      }
    }
  }
  return ret;

}


int ObMultiUpsMultiGet::create_ups_multi_get_ops()
{
  int ret = OB_SUCCESS;
  uint64_t table_id                               = OB_INVALID_ID;
  bool is_table_level                             = false;
  common::ObPartitionMonitor *part_monitor        =  NULL;
  chunkserver::ObCsPartitionManager *part_mgr     = NULL;
  if (NULL == (part_monitor = rpc_proxy_->get_cs_partition_monitor()))
  {
    ret = OB_NOT_EXIST_PARTITION_MANAGER;
    YYSYS_LOG(ERROR, "fail get part_monitor,ret=%d",ret);
  }
  else if (NULL == (part_mgr = dynamic_cast<chunkserver::ObCsPartitionManager *>(part_monitor->get_partition_manager())))
  {
    ret = OB_NOT_EXIST_PARTITION_MANAGER;
    YYSYS_LOG(ERROR, "fail partition manager error,ret=%d",ret);
  }
  else if (OB_SUCCESS != (ret = get_param_->get_table_id(table_id)))
  {
    YYSYS_LOG(WARN, "fail to get table id,ret=%d",ret);
  }

  if (OB_SUCCESS == ret)
  {
    int64_t frozen_data_version = OB_INVALID_VERSION;
    if(get_param_->get_version_range().border_flag_.is_min_value())
    {
        frozen_data_version = OB_INVALID_VERSION;
    }
    else if(!get_param_->get_version_range().border_flag_.inclusive_start())
    {
        frozen_data_version = get_param_->get_version_range().start_version_.major_ +1;
    }
    else
    {
        frozen_data_version = get_param_->get_version_range().start_version_.major_;
    }
    if (OB_SUCCESS != (ret = part_mgr->get_table_part_type_for_select(table_id, is_table_level,frozen_data_version)))
    {
      YYSYS_LOG(WARN, "fail partition type fail,ret=%d",ret);
    }
    else if (is_table_level)
    {
      ObArray<chunkserver::ObCsPartitionManager::VersionPaxosId> version_paxos_ids;
      if (OB_SUCCESS != (ret = part_mgr->get_paxos_id_without_partition_for_select(table_id, version_paxos_ids,frozen_data_version)))
      {
        YYSYS_LOG(WARN, "get paxos id without partition failed, ret=%d",ret);
      }
      else if (OB_SUCCESS != (ret = create_ups_multi_get_ops_(*get_param_, version_paxos_ids)))
      {
        YYSYS_LOG(WARN, "cereate multi get op failed, ret=%d",ret);
      }
    }
    else//row level
    {
      // distribute get_param
      if(OB_SUCCESS != (ret = distribute_get_param(part_mgr, table_id,frozen_data_version)))
      {
        YYSYS_LOG(WARN, "distribute get_param failed,ret=%d",ret);
      }
    }
  }
  //no matter what happen,must release
  if(NULL != part_monitor && NULL != part_mgr)
  {
    int tmp_ret = OB_SUCCESS;
    if(OB_SUCCESS != (tmp_ret = part_monitor->release_manager(part_mgr)))
    {
      YYSYS_LOG(WARN, "fail to release manager:ret[%d]", tmp_ret);
      ret = (ret != OB_SUCCESS ? ret : tmp_ret);
    }
  }
  return ret;
}


int ObMultiUpsMultiGet::create_ups_multi_get_ops_(const ObGetParam& get_param, const ObArray<chunkserver::ObCsPartitionManager::VersionPaxosId>& paxos_ids)
{
  int ret = OB_SUCCESS;
  ObArray <int32_t> added_cell_paxos_ids;
  int64_t row_size = get_param.get_row_size();

  if (0 == row_size)
  {
    ret = OB_ERR_UNEXPECTED;
    YYSYS_LOG(ERROR, "create get ops without get param info,ret=%d",ret);
  }

  for (int64_t i=0; OB_SUCCESS == ret && i<row_size; i++)//for each row
  {
    added_cell_paxos_ids.clear();
    int64_t paxos_ids_size = paxos_ids.count();
    if (paxos_ids_size < 1)
    {
      ret = OB_ERROR;
      YYSYS_LOG(WARN, "at least has one paxos group!,ret=%d",ret);
      break;
    }
    YYSYS_LOG(DEBUG, "the paxos size is=%ld",paxos_ids_size);//for debug
    ObCellInfo *cell = NULL;
    ObGetParam::ObRowIndex row_index = get_param.get_row_index()[i];
    for (int64_t idx = 0; idx < paxos_ids_size; idx++)
    {
      YYSYS_LOG(DEBUG,"paxos_id::%d  version::%ld", paxos_ids.at(idx).paxos_id_, paxos_ids.at(idx).version_);
    }
    for(int64_t j=0; OB_SUCCESS == ret && j<paxos_ids_size; j++)//for each paxos group
    {
      int64_t child_index = OB_INVALID_INDEX;
      //paxos id can't be negative,infact OB_INVALID_PAXOS_ID
      if (paxos_ids.at(j).paxos_id_ < 0)
      {
        YYSYS_LOG(INFO, "invalid paxos id [%d],skip,",paxos_ids.at(j).paxos_id_);
        continue;
      }
      // the same paxos group can add the same get cell only once
      if (has_add_cell(paxos_ids.at(j).paxos_id_, added_cell_paxos_ids))
      {
        continue;
      }
      if(!has_created_multi_get_op(paxos_ids.at(j), child_index))//current op not exist
      {
        ret = create_ups_multi_get_op_(paxos_ids.at(j).paxos_id_, &child_index);
        if (OB_SUCCESS != ret)
        {
          YYSYS_LOG(WARN, "create get op failed, ret=%d",ret);
        }
      }
      if (OB_SUCCESS == ret)//add current row to current op
      {
        YYSYS_LOG(DEBUG, "current version is[%ld],paxos id[%d]",paxos_ids.at(j).version_,paxos_ids.at(j).paxos_id_);//for debug
        ObUpsMultiGet *multi_get_op = children_ops_.at(child_index);

        for(int64_t k=0; OB_SUCCESS == ret && k<row_index.size_; k++)
        {
          cell = get_param[row_index.offset_ + k];
//          YYSYS_LOG(ERROR, "current get param cell is[%s]",print_cellinfo(cell));//for debug
          if (OB_SUCCESS != (ret = multi_get_op->add_cell(*cell)))
          {
            YYSYS_LOG(WARN, "add cell to multi get op failed,cell=[%s],ret=%d",print_cellinfo(cell),ret);
          }
        }
      }
    }
    if (OB_SUCCESS == ret)
    {
      YYSYS_LOG(DEBUG, "children_ops_ size:%ld", children_ops_.count());
    }
  }
  return ret;
}

bool ObMultiUpsMultiGet::has_add_cell(int32_t paxos_id, ObArray<int32_t>& added_cell_paxos_ids)
{
  bool has_add = false;
  for (int64_t i = 0; i<added_cell_paxos_ids.count(); i++)
  {
    if (paxos_id == added_cell_paxos_ids.at(i))
    {
      has_add = true;
      break;
    }
  }
  if (!has_add)
  {
    if (OB_SUCCESS != added_cell_paxos_ids.push_back(paxos_id))
    {
      YYSYS_LOG(WARN, "push paxos id failed");
    }
  }
  return has_add;
}

bool ObMultiUpsMultiGet::has_created_multi_get_op(const chunkserver::ObCsPartitionManager::VersionPaxosId& version_paxos, int64_t& index)
{
  bool has_created = false;
  int64_t child_num = children_ops_.count();
  for (int64_t i=0; i<child_num; i++)
  {
    if (children_ops_.at(i)->get_paxos_id() == version_paxos.paxos_id_)
    {
      has_created = true;
      index = i;
      break;
    }
  }
  return has_created;
}


int ObMultiUpsMultiGet::create_ups_multi_get_op_(int64_t paxos_id, int64_t *index/*NULL*/)
{
  int ret = OB_SUCCESS;
  ObUpsMultiGet *UMG = NULL;
  //mod hongchen [PERFORMANCE_OPTI] 20170821:b
  UMG = MULTI_UPS_GET_ALLOC.alloc();
  //UMG = (ObUpsMultiGet *)allocator_.alloc(sizeof(ObUpsMultiGet));
  if(NULL == UMG)
  {
    YYSYS_LOG(WARN, "alloc mem fail NULL");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else
  {
    UMG->set_paxos_id(paxos_id);
    UMG->set_row_desc(*row_desc_);
    UMG->set_rpc_proxy(rpc_proxy_);
    UMG->set_ts_timeout_us(ts_timeout_us_);
    UMG->set_version_range(get_param_->get_version_range());
    UMG->set_is_read_consistency(get_param_->get_is_read_consistency());
    //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151223:b
    UMG->set_read_atomic_param(get_param_->get_read_atomic_param());
    UMG->set_trans_id(trans_id_);
    //add duyr 20151223:e
    // add by maosy [MultiUps 1.0] [batch_udi] 20170420 b
    UMG->set_data_mark_param(get_param_->get_data_mark_param());
    // add by maosy 20170420 e
  }
  //mod hongchen [PERFORMANCE_OPTI] 20170821:e
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = children_ops_.push_back(UMG)))
    {
      YYSYS_LOG(WARN, "add child failed,ret=%d",ret);
    }
    else if (OB_SUCCESS != (ret = row_merger_.add_row_iterator(UMG)))
    {
      YYSYS_LOG(WARN, "add to row merger failed,ret=%d",ret);
    }
    else
    {
      if (NULL != index)
      {
        *index = (children_ops_.count() -1);
        YYSYS_LOG(DEBUG,"child index is[%ld]",*index);
      }
    }
  }
  return ret;
}


int ObMultiUpsMultiGet::distribute_get_param(chunkserver::ObCsPartitionManager *par_mgr, uint64_t table_id, int64_t frozen_data_version)
{
  int ret = OB_SUCCESS;
  ObCellInfo *cell = NULL;
  ObGetParam single_row_get_param;

  int64_t row_size = get_param_->get_row_size();
  for (int64_t i=0; OB_SUCCESS == ret && i<row_size; i++)//for each row
  {
    single_row_get_param.reset();
    //single_row_get_param.set_is_read_consistency(get_param_->get_is_read_consistency());
    //single_row_get_param.set_version_range(get_param_->get_version_range());
    ObGetParam::ObRowIndex row_index = get_param_->get_row_index()[i];

    int64_t column_size = row_index.size_;
    for (int64_t j=0; OB_SUCCESS == ret && j<column_size; j++)//for each column
    {
      cell = (*get_param_)[row_index.offset_ + j];
      if (OB_SUCCESS != (ret = single_row_get_param.add_cell(*cell)))
      {
        YYSYS_LOG(WARN, "add cell to single row get_param failed,ret=%d",ret);
      }
    }

    if (OB_SUCCESS == ret)
    {
      ObCalcInfo  cal_info;
      ObArray<chunkserver::ObCsPartitionManager::VersionPaxosId> version_paxos_ids;
      cell->row_key_.dump(YYSYS_LOG_LEVEL_DEBUG);//for debug
      cal_info.set_row_key(&cell->row_key_);
      cal_info.set_schema_manager(schema_manager_);
      cal_info.set_stmt_type(stmt_type_);
      cal_info.set_table_id(table_id);
      if (OB_SUCCESS != (ret = par_mgr->get_paxos_version_array(cal_info, version_paxos_ids,frozen_data_version)))
      {
        YYSYS_LOG(WARN, "get paxos version array failed,ret=%d",ret);
      }
      else if (OB_SUCCESS != (ret = create_ups_multi_get_ops_(single_row_get_param, version_paxos_ids)))
      {
        YYSYS_LOG(WARN, "create ups multi get ops falied, ret=%d",ret);
      }
    }
  }
  return ret;
}

int ObMultiUpsMultiGet::close()
{
  int ret = OB_SUCCESS;
//  YYSYS_LOG(ERROR,"close multi ups get");
  for (int64_t i = 0; ret == OB_SUCCESS && i < children_ops_.count(); i++)
  {
    if ((ret = children_ops_.at(i)->close()) != OB_SUCCESS)
    {
      YYSYS_LOG(WARN, "failed to close %ldth child_op, err=%d", i, ret);
    }
  }
  this->reuse();
  return ret;
}

int ObMultiUpsMultiGet::get_next_row(const ObRowkey *&rowkey, const ObRow *&row)
{
  int ret = OB_SUCCESS;
  /**
   * in GET,each UPS will return a ObNewScanner,if the row is not exist or deleted,
   * UPS will return a deleted row with all nop cells. In MultiUps, if U changed partiton
   * rule, one row maybe come from more than one paox group, the get_next_row() need to
   * return the min rowkey with max row_version.
   */
  ret = row_merger_.get_next_row(rowkey, row);
  return ret;
}

int ObMultiUpsMultiGet::set_sstable_rowkey(const ObRowkey *&rowkey)
{
  int ret = OB_SUCCESS;
  ret = row_merger_.set_cur_sstable_rowkey(rowkey);
  return ret;
}

void ObMultiUpsMultiGet::set_row_desc(const ObRowDesc &row_desc)
{
  row_desc_ = &row_desc;
  trans_id_.reset();
  row_merger_.set_merge_row_desc(*row_desc_);//add shili, [MultiUPS] [COLUMN_VERSION_MERGE]  20170423
}

int64_t ObMultiUpsMultiGet::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ObMultiUpsMultiGet()\n");
  for (int32_t i = 0; i < children_ops_.count(); i++)
  {
    if (NULL != children_ops_.at(i))
    {
      databuff_printf(buf, buf_len, pos, "--child(%d)--\n",i);
      pos += children_ops_.at(i)->to_string(buf+pos, buf_len-pos);
    }
  }
  return pos;
}

int ObMultiUpsMultiGet::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  row_desc = row_desc_;
  return ret;
}

int ObMultiUpsMultiGet::set_rpc_proxy(ObSqlUpsRpcProxy *rpc_proxy)
{
  int ret = OB_SUCCESS;
  if(NULL == rpc_proxy)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(WARN, "rpc_proxy is null");
  }
  else
  {
    rpc_proxy_ = rpc_proxy;
  }
  return ret;
}

namespace oceanbase
{
  namespace sql
  {
    REGISTER_PHY_OPERATOR(ObMultiUpsMultiGet, PHY_MULTI_UPS_MULTI_GET);
  }
}

