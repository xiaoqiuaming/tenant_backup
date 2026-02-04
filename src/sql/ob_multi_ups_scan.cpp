/**
 * ob_multi_ups_scan.h defined for select rpc data from MultiUPS to CS using SCAN,
 * in MultiUPS,for select,the UPS scan become from 1 to N (N>=1)
 *
 * Version: $Id
 *
 * Authors:
 *   lijianqiang <lijianqiang@mail.nwpu.edu.cn>
 *     --some work detials if U want
 */

#include "ob_multi_ups_scan.h"
#include "common/utility.h"
#include "common/ob_ups_info.h"
#include "chunkserver/ob_cs_partition_manager.h"
#include "chunkserver/ob_rpc_proxy.h"
#include "chunkserver/ob_chunk_server_main.h" //add liuzy [MultiUPS] [UPS_List_Info] 20160428

using namespace oceanbase;
using namespace sql;


ObMultiUpsScan::ObMultiUpsScan()
{
  rpc_proxy_ = NULL;
  ts_timeout_us_ = 0;
  row_counter_ = 0;
  is_read_consistency_ = true;
  allocator_.set_mod_id(common::ObModIds::OB_MULTI_UPS_PHY_SCAN);
}
ObMultiUpsScan::~ObMultiUpsScan()
{
  for (int64_t i=0;i<children_ops_.count();i++)
  {
    if (NULL != children_ops_.at(i))
    {
      children_ops_.at(i)->~ObUpsScan();
    }
    children_ops_.at(i) = NULL;
  }
  children_ops_.clear();
  //  allocator_.free();//do not need free manual,the ModuleArena will free the mem when destruction the object
}
void ObMultiUpsScan::reset()
{
  for (int64_t i=0;i<children_ops_.count();i++)
  {
    if (NULL != children_ops_.at(i))
    {
      children_ops_.at(i)->~ObUpsScan();
    }
    children_ops_.at(i) = NULL;
  }
  scan_param_.reset();
  row_desc_.reset();
  children_ops_.clear();
  allocator_.reuse();
  trans_id_.reset();
}

void ObMultiUpsScan::reuse()
{
  this->reset();
}

int ObMultiUpsScan::set_child(int32_t child_idx, ObPhyOperator &child_operator)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(child_idx);
  UNUSED(child_operator);
  YYSYS_LOG(ERROR, "not implement");
  return ret;
}

int64_t ObMultiUpsScan::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ObMultiUpsScan()\n");
  for (int32_t i = 0; i < children_ops_.count(); i++)
  {
    if (NULL != children_ops_.at(i))
    {
      databuff_printf(buf, buf_len, pos, "--child(%d)--\n",i);
      pos += children_ops_.at(i)->to_string(buf+pos, buf_len-pos);
      databuff_printf(buf, buf_len, pos, "\n");
    }
  }
  return pos;
}

int ObMultiUpsScan::set_ups_rpc_proxy(ObSqlUpsRpcProxy *rpc_proxy)
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

int ObMultiUpsScan::add_column(const uint64_t &column_id)
{
  int ret = OB_SUCCESS;

  if(OB_INVALID_ID == range_.table_id_)
  {
    ret = OB_ERROR;
    YYSYS_LOG(ERROR, "set range first,ret=%d",ret);
  }
  else if(OB_SUCCESS != (ret = scan_param_.add_column(column_id)))
  {
    YYSYS_LOG(WARN, "add column id fail:ret[%d] column_id[%lu],ret=%d", ret, column_id, ret);
  }
  else if(OB_SUCCESS != (ret = row_desc_.add_column_desc(range_.table_id_, column_id)))
  {
    YYSYS_LOG(WARN, "add column desc fail:ret[%d]", ret);
  }
  return ret;
}

int ObMultiUpsScan::open()
{
  int ret = OB_SUCCESS;
  row_merger_.reset();
  //1.create ups scan ops according to the scan_param for each paxos id
  if (OB_SUCCESS != (ret = create_ups_scan_ops()))
  {
    YYSYS_LOG(WARN, "create ups_multi_get ops failed, ret=%d",ret);
  }
  //2.open all paxos group scan ops
  else
  {
    YYSYS_LOG(DEBUG, "child op num is[%ld]",children_ops_.count());
    for (int32_t i = 0; ret == OB_SUCCESS && i < children_ops_.count(); i++)
    {
      if (OB_SUCCESS != (ret = children_ops_.at(i)->open()))
      {
        YYSYS_LOG(WARN, "failed to open %dth child_op, err=%d", i, ret);
        break;
      }
    }
  }
  return ret;
}


int ObMultiUpsScan::create_ups_scan_ops()
{
  int ret = OB_SUCCESS;
  //mod liuzy [Multiups] [UPS_List_Info] 20160427:b
//  oceanbase::common::ObUpsList  master_ups_list;
//  int64_t group_num = 0;
//  if (NULL == rpc_proxy_)
//  {
//    ret = OB_ERROR;
//    YYSYS_LOG(ERROR, "rpc proxy not init,ret=%d",ret);
//  }
//  else
//  {
//    ret = rpc_proxy_->get_master_ups_list(master_ups_list, group_num);
//    if (OB_SUCCESS != ret || 0 == group_num)
//    {
//      YYSYS_LOG(WARN, " get master ups failed, group num=%ld, ret=%d", group_num, ret);
//    }
//    else if (OB_SUCCESS != (ret = reduce_scan_paxos_num(master_ups_list, group_num)))
//    {
//      YYSYS_LOG(WARN, "reduce scan paxos num failed,ret=%d",ret);
//    }
//    else
//    {
//      /**
//       * @todo optimize the strategy scan data from MultiUps, now we scan all paxos groups,
//       * except the table in table level. can optimized by the partition rules so that
//       * reduce the UPS num as small as possible
//       */
//      for (int64_t i=0; OB_SUCCESS == ret && i<group_num; i++)
//      {
//        if (!has_create_ups_scan_op(master_ups_list.ups_array_[i].paxos_id_))
//        {
//          if (OB_SUCCESS != (ret = create_ups_scan_op(master_ups_list.ups_array_[i].paxos_id_)))
//          {
//            YYSYS_LOG(WARN, "create ups scan op failed, ret=%d",ret);
//            break;
//          }
//        }
//      }
//    }
//  }
  ObServer mergeserver;
  const ObClientManager *client_mgr = NULL;
  ObVersionRange new_version_range;
  const ObVersionRange version_range = scan_param_.get_version_range();
  ObArray<int64_t> paxos_idx_array;
  ObChunkServer &chunkserver = ObChunkServerMain::get_instance()->get_chunk_server();
  int64_t servering_data_version = chunkserver.get_serving_data_version();
  int64_t frozen_data_version =  chunkserver.get_frozen_version();
  chunkserver.get_config_mgr().get_ms(mergeserver);

  ObPartitionManager::convert_version_range(version_range, new_version_range,
                                            servering_data_version, frozen_data_version);
  YYSYS_LOG(DEBUG, "the scan version range[%s],the scan get_paxos_group version range[%s]",to_cstring(version_range), to_cstring(new_version_range));
  chunkserver.get_config_mgr().get_ms(mergeserver);
  if (IS_SYS_TABLE_ID(scan_param_.get_table_id()))
  {
    paxos_idx_array.push_back(0);
  }
  else
  {
    if (NULL == rpc_proxy_)
    {
      ret = OB_ERROR;
      YYSYS_LOG(ERROR, "rpc proxy not init,ret=%d",ret);
    }
    else if (0 == mergeserver.get_port())
    {
      ret = OB_MS_NOT_EXIST;
      YYSYS_LOG(WARN, "No mergeserver right now.ret=%d",ret);
    }
    else if (NULL == (client_mgr = chunkserver.get_config_mgr().get_client_mgr()))
    {
      ret = OB_NOT_INIT;
      YYSYS_LOG(WARN, "the client mgr is null,ret=%d",ret);
    }
    else
    {
      YYSYS_LOG(DEBUG, "begin to get paxos group usable info from system table");
      if (OB_SUCCESS != (ret = rpc_proxy_->get_paxos_group_offline_info(mergeserver, new_version_range,
                                                                        client_mgr, paxos_idx_array)))
      {
        YYSYS_LOG(ERROR, "get paxos group usable info failed, paxos idx array count:[%ld], ret=%d",
                  paxos_idx_array.count(), ret);
      }
      else if (OB_SUCCESS != (ret = reduce_scan_paxos_num(paxos_idx_array)))
      {
        YYSYS_LOG(WARN, "reduce scan paxos num failed,ret=%d",ret);
      }
    }
  }
  if (OB_SUCCESS == ret)
  {
    /**
     * @todo optimize the strategy scan data from MultiUps, now we scan all paxos groups,
     * except the table in table level. can optimized by the partition rules so that
     * reduce the UPS num as small as possible
     */
    for (int64_t i=0; OB_SUCCESS == ret && i<paxos_idx_array.count(); i++)
    {
      if (!has_create_ups_scan_op(paxos_idx_array.at(i)))
      {
        if (OB_SUCCESS != (ret = create_ups_scan_op(paxos_idx_array.at(i))))
        {
          YYSYS_LOG(WARN, "create ups scan op failed, ret=%d",ret);
          break;
        }
      }
    }
  }
  //mod 20160427:e
  return ret;
}

bool ObMultiUpsScan::has_create_ups_scan_op(int64_t paxos_id_)
{
  bool has_created = false;
  for (int64_t i=0; i<children_ops_.count(); i++)
  {
    if (children_ops_.at(i)->get_paxos_id() == paxos_id_)
    {
      has_created = true;
      break;
    }
  }
  return has_created;
}

//mod liuzy [Multiups] [UPS_List_Info] 20160427:b
//int ObMultiUpsScan::reduce_scan_paxos_num(oceanbase::common::ObUpsList& master_ups_list, int64_t& group_num)
int ObMultiUpsScan::reduce_scan_paxos_num(ObArray<int64_t> &paxos_idx_array)
//mod 20160427:e
{
  int ret = OB_SUCCESS;
  bool is_table_level = false;
  //del liuzy [Multiups] [UPS_List_Info] 20160427:b
//  int32_t reduce_count = 0;
  //del 20160427:e
  uint64_t table_id = scan_param_.get_table_id();
  common::ObPartitionMonitor *part_monitor        =  NULL;
  chunkserver::ObCsPartitionManager *partition_manager = NULL;
  int64_t frozen_data_version = OB_INVALID_VERSION;
  if(scan_param_.get_version_range().border_flag_.is_min_value())
  {
      frozen_data_version = OB_INVALID_VERSION;
  }
  else if(!scan_param_.get_version_range().border_flag_.inclusive_start())
  {
      frozen_data_version = scan_param_.get_version_range().start_version_.major_ +1;
  }
  else
  {
      frozen_data_version = scan_param_.get_version_range().start_version_.major_;
  }
  if (NULL == (part_monitor = rpc_proxy_->get_cs_partition_monitor()))
  {
    ret = OB_NOT_EXIST_PARTITION_MANAGER;
    YYSYS_LOG(ERROR, "fail get part_monitor,ret=%d",ret);
  }
  else if (NULL == (partition_manager = dynamic_cast<chunkserver::ObCsPartitionManager *>(part_monitor->get_partition_manager())))
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(WARN, "get partition manager failed, ret=%d",ret);
  }
  if (OB_SUCCESS == ret)
  {
    //if (OB_SUCCESS != (ret = partition_manager->get_table_part_type_for_select(table_id, is_table_level)))
    if(OB_SUCCESS != (ret = partition_manager->get_table_part_type_for_select(table_id,is_table_level,frozen_data_version)))
    {
      YYSYS_LOG(WARN, "get table part type failed, ret=%d",ret);
    }
    else if (is_table_level)
    {
      //mod liuzy [Multiups] [UPS_List_Info] 20160427:b
//      ObArray<chunkserver::ObCsPartitionManager::VersionPaxosId> version_paxos;
//      if (OB_SUCCESS != (ret = partition_manager->get_paxos_id_without_partition_for_select(table_id, version_paxos)))
//      {
//        YYSYS_LOG(WARN, "get paxos id without partition failed, ret=%d",ret);
//      }
//      else
//      {
//        for (int64_t i = 0; OB_SUCCESS == ret && i<version_paxos.count(); i++)
//        {
//          if (OB_INVALID_PAXOS_ID == version_paxos.at(i).paxos_id_)
//          {
//            YYSYS_LOG(INFO, "has no group node info,paxos_id=%d",version_paxos.at(i).paxos_id_);
//            continue;
//          }
//          else
//          {
//            master_ups_list.ups_array_[reduce_count++].paxos_id_ = version_paxos.at(i).paxos_id_;
//          }
//        }
//        group_num = (reduce_count < group_num ? reduce_count : group_num);
//      }
      paxos_idx_array.clear();
      ObArray<chunkserver::ObCsPartitionManager::VersionPaxosId> version_paxos;
      if (OB_SUCCESS != (ret = partition_manager->get_paxos_id_without_partition_for_select(table_id, version_paxos,frozen_data_version)))
      {
        YYSYS_LOG(WARN, "get paxos id without partition failed, ret=%d",ret);
      }
      else
      {
        for (int64_t i = 0; OB_SUCCESS == ret && i<version_paxos.count(); i++)
        {
          if (OB_INVALID_PAXOS_ID == version_paxos.at(i).paxos_id_)
          {
            YYSYS_LOG(INFO, "has no group node info,paxos_id=%d",version_paxos.at(i).paxos_id_);
            continue;
          }
          else
          {
            paxos_idx_array.push_back(version_paxos.at(i).paxos_id_);
          }
        }
      }
    }
    //mod 20160427:e
  }

  //no matter what happen,must release
  if(NULL != part_monitor && NULL != partition_manager)
  {
    int tmp_ret = OB_SUCCESS;
    if(OB_SUCCESS != (tmp_ret = part_monitor->release_manager(partition_manager)))
    {
      YYSYS_LOG(WARN, "fail to release manager:ret[%d]", tmp_ret);
      ret = (ret != OB_SUCCESS ? ret : tmp_ret);
    }
  }
  return ret;
}

int ObMultiUpsScan::create_ups_scan_op(int64_t paxos_id)
{
  int ret = OB_SUCCESS;
  ObUpsScan *US = NULL;
  US = (ObUpsScan *)allocator_.alloc(sizeof(ObUpsScan));
  if(NULL == US)
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    YYSYS_LOG(ERROR, "alloc mem fail NULL,ret=%d",ret);
  }
  else
  {
    US = new(US) ObUpsScan();
    if (!US)
    {
      ret = OB_ERROR;
      YYSYS_LOG(ERROR, "new obUpsMultiGet err,ret=%d",ret);
    }
    else
    {
      US->set_range(range_);
      US->set_paxos_id(paxos_id);
      US->set_ups_rpc_proxy(rpc_proxy_);
      US->set_ts_timeout_us(ts_timeout_us_);
      US->set_is_read_consistency(is_read_consistency_);
      US->set_version_range(scan_param_.get_version_range());
      //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151223:b
      US->set_read_atomic_param(scan_param_.get_read_atomic_param());
      US->set_trans_id(trans_id_);
      //add duyr  20151223:e
	  	// add by maosy [MultiUps 1.0] [batch_udi] 20170420 b
	  US->set_data_mark_param(scan_param_.get_data_mark_param());
	  	// add by maosy 20170420 e

      int64_t column_size = scan_param_.get_column_id_size();
      const uint64_t* const column_id = scan_param_.get_column_id();
      for (int64_t i=0; OB_SUCCESS == ret && i<column_size; i++)
      {
        if (OB_SUCCESS != (ret = US->add_column(column_id[i])))
        {
          YYSYS_LOG(WARN, "add column to ups scan failed, ret=%d",ret);
          break;
        }
      }
    }
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = children_ops_.push_back(US)))
    {
      YYSYS_LOG(WARN, "add child failed,ret=%d",ret);
    }
    else if (OB_SUCCESS != (ret = row_merger_.add_row_iterator(US)))
    {
      YYSYS_LOG(WARN, "add ups scan to row_merger failed,ret=%d",ret);
    }
  }
  return ret;
}

int ObMultiUpsScan::get_next_row(const common::ObRowkey *&rowkey, const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  /**
   * in SCAN,each UPS will return a ObNewScanner,if the row is not exist or deleted,
   * UPS will return nothing. In MultiUps, if U changed partiton, the row with same rowkey
   * maybe localted in different paxos group,each row contain a version,in get_next_row()
   * need to return the min rowkey with max_row_version
   *
   */
  ret = row_merger_.get_next_row(rowkey, row);
  return ret;
}
int ObMultiUpsScan::close()
{
  int ret = OB_SUCCESS;
  YYSYS_LOG(DEBUG, "colse multi ups scan");
  for (int64_t i = 0; ret == OB_SUCCESS && i < children_ops_.count(); i++)
  {
    if ((ret = children_ops_.at(i)->close()) != OB_SUCCESS)
    {
      YYSYS_LOG(WARN, "failed to close %ldth child_op, err=%d", i, ret);
    }
  }
  this->reset();
  return OB_SUCCESS;
}


int ObMultiUpsScan::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  row_desc = &row_desc_;
  return ret;
}

int ObMultiUpsScan::set_range(const ObNewRange &range)
{
  int ret = OB_SUCCESS;
  ObString table_name; //设置一个空的table name
  range_ = range;
  if(OB_SUCCESS != (ret = scan_param_.set(range_.table_id_, table_name, range_)))
  {
    YYSYS_LOG(WARN, "scan_param set range fail:ret[%d]", ret);
  }
  return ret;
}

void ObMultiUpsScan::set_version_range(const ObVersionRange &version_range)
{
  scan_param_.set_version_range(version_range);
}


//add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151223:b
void ObMultiUpsScan::set_read_atomic_param(const ObReadAtomicParam &read_atomic_param)
{
  scan_param_.set_read_atomic_param(read_atomic_param);
}
//add duyr 20151223:e
	// add by maosy [MultiUps 1.0] [batch_udi] 20170420 b
void ObMultiUpsScan::set_data_mark_param(const ObDataMarkParam &param)
{
    scan_param_.set_data_mark_param(param);
}
	// add by maosy 20170420 e
bool ObMultiUpsScan::is_result_empty() const
{
  bool is_result_empty = true;
  for (int64_t i=0; i<children_ops_.count(); i++)
  {
    is_result_empty = children_ops_.at(i)->is_result_empty();
    if (!is_result_empty)
    {
      break;
    }
  }
  return is_result_empty;
}


namespace oceanbase
{
  namespace sql
  {
    REGISTER_PHY_OPERATOR(ObMultiUpsScan, PHY_MULTI_UPS_SCAN);
  }
}
