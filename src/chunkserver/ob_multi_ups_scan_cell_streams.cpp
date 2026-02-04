/**
 * ob_multi_ups_scan_cell_stream.cpp defined for rpc data from MultiUPS to CS
 *
 * Version: $Id
 *
 * Authors:
 *   lijianqiang <lijianqiang@mail.nwpu.edu.cn>
 *     --some work detials if U want
 */

#include "ob_multi_ups_scan_cell_streams.h"
#include "ob_chunk_server_main.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace oceanbase::common;

    ObMultiUpsScanCellStreams::ObMultiUpsScanCellStreams(ObMergerRpcProxy * rpc_proxy,
                                                         const ObServerType server_type,
                                                         const int64_t time_out)
      : ObCellStream (rpc_proxy, server_type,time_out)
    {
      allocator_.set_mod_id(ObModIds::OB_MULTI_UPS_SCAN);//for mem tracks
    }

    ObMultiUpsScanCellStreams::~ObMultiUpsScanCellStreams()
    {
      for (int64_t i=0; i<multi_ups_streams_.count(); i++)
      {
        if (NULL != multi_ups_streams_.at(i))
        {
          multi_ups_streams_.at(i)->~ObScanCellStream();
        }
      }
      multi_ups_streams_.clear();
//      allocator_.free();//do not need free manual,the ModuleArena will free the mem when destruction the object
    }

    void ObMultiUpsScanCellStreams::reset_multi_inner_stat(void)
    {
      YYSYS_LOG(DEBUG,"clear all streams");
      ObCellStream::reset_inner_stat();//in fact, no use
      for (int64_t i=0; i<multi_ups_streams_.count(); i++)
      {
        if (NULL != multi_ups_streams_.at(i))
        {
          multi_ups_streams_.at(i)->~ObScanCellStream();
          multi_ups_streams_.at(i) = NULL;
        }
      }
      multi_ups_streams_.clear();
      allocator_.reuse();
    }

    void ObMultiUpsScanCellStreams::reset(void)
    {
      YYSYS_LOG(DEBUG,"reset mutiscancellsteams,do nothing");
      ObCellStream::reset();
      //do nothing
    }

    int ObMultiUpsScanCellStreams::scan(const ObScanParam& param, ObIterator *it_out[],  int64_t& size)
    {
      int ret = OB_SUCCESS;
      //add hongchen 20170906:b
      //import, first force reset size
      size = 0;
      //add hongchen 20170906:e
      YYSYS_LOG(DEBUG, "start scan!!!!!!");//for debug
      int64_t it_num = 0;
      int64_t group_num = 0;
      uint64_t table_id = param.get_table_id();
      const ObNewRange * range = param.get_range();
      const ObVersionRange version_range = param.get_version_range();
      YYSYS_LOG(DEBUG,"current table id is[%ld]",table_id);
      //param.dump();//for debug
      if (NULL == range)
      {
        YYSYS_LOG(WARN, "check scan param failed");
        ret = OB_INPUT_PARAM_ERROR;
      }
      else
      {
        /**
         * we free all scan cell streams before scan, then create new scan streams
         * according current cluster paxos_groups, for each cell stream, if no data
         * return from UPS, return OB_ITER_END,and add all cell streams which has data
         * to it_out[] for the merge operator;If the table has no partition rules,just
         * scan one paxos_group which contain all the data of the table.
         */
        this->reset_multi_inner_stat();
        if (OB_SUCCESS != (ret = create_all_scan_streams(table_id, version_range, group_num)))
        {
          YYSYS_LOG(WARN, "table_id:%ld,first create scanners  fail,ups_group_num_:%ld", param.get_table_id(), group_num);
        }
        else
        {
          for (int64_t i=0; i<group_num; i++)
          {
            ret = multi_ups_streams_.at(i)->scan(param);

            if(OB_ITER_END == ret)//no data, return OB_ITER_END
            {
              YYSYS_LOG(DEBUG, "table_id:%ld,multi_ups_streams_[%ld] has not data ", param.get_table_id(), i);
              ret = OB_SUCCESS;
              continue;
            }
            else if (OB_SUCCESS == ret)//has data and success
            {
              it_out[it_num++] = multi_ups_streams_.at(i);
              YYSYS_LOG(DEBUG, "find one cell stream,add to it array");
            }
            else//ret !=OB_SUCCESS && ret!= OB_ITER_END
            {
              YYSYS_LOG(ERROR, "multi_ups_streams_[%ld] scan error,ret=%d",i, ret);
              break;
            }
          }//end for

          if (OB_SUCCESS == ret)
          {
            size = it_num;
            if (0 == size)//all paxos ids return none,no data from ups return iter end
            {
              YYSYS_LOG(DEBUG, "no data from ups,ret=%d",ret);
              ret = OB_ITER_END;
            }
          }
        }
      }
      return ret;
    }

    int ObMultiUpsScanCellStreams::create_all_scan_streams(const uint64_t table_id, const ObVersionRange& version_reange, int64_t& paxos_group_num)
    {
      int ret = OB_SUCCESS;
      int64_t paxos_group_ids[MAX_UPS_COUNT_ONE_CLUSTER];
      int64_t paxos_group_count = 0;
      YYSYS_LOG(DEBUG, "create streams");//for debug
      if (OB_SUCCESS != (ret = get_paxos_group_id_array(table_id, version_reange, paxos_group_ids, paxos_group_count)))
      {
        YYSYS_LOG(WARN, "get paxos id array failed,paxos_group_count=%ld, ret=%d", paxos_group_count, ret);
      }
      else if (0 == paxos_group_count)
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(ERROR, "get none paxos_ids, paxos_group_count:%ld,ret=%d", paxos_group_count, ret);
      }
      else if (OB_SUCCESS != (ret = pruing_useless_paxos_group(table_id, paxos_group_ids, paxos_group_count)))
      {
        YYSYS_LOG(WARN, "pruing useless paxos group failed, ret=%d",ret);
      }
      else
      {
        YYSYS_LOG(DEBUG,"the paxos group count is[%ld]",paxos_group_count);//for debug
        for (int64_t i=0; i<paxos_group_count; i++)
        {
          ObScanCellStream *scan_cell_stream = (ObScanCellStream *)allocator_.alloc(sizeof(ObScanCellStream));
          if (NULL == scan_cell_stream)
          {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            YYSYS_LOG(ERROR, "alloc mem fail NULL,ret=%d",ret);
            break;
          }
          else
          {
            scan_cell_stream = new(scan_cell_stream) ObScanCellStream(rpc_proxy_, server_type_, time_out_, paxos_group_ids[i]);
            scan_cell_stream->set_timeout_time(this->timeout_time_);//set absolute timeout time,default "0"
            if(OB_SUCCESS != (ret = multi_ups_streams_.push_back(scan_cell_stream)))
            {
              YYSYS_LOG(ERROR, "push back scan cell stram failed,paxos id=%ld,paxos group count=%ld,index=%ld,ret=%d", paxos_group_ids[i], paxos_group_count, i, ret);
              break;
            }
          }
        }
        if (OB_SUCCESS == ret)
        {
          paxos_group_num = paxos_group_count;
        }
      }
      return ret;
    }

    int ObMultiUpsScanCellStreams::get_paxos_group_id_array(const uint64_t table_id, const ObVersionRange& version_range, int64_t *paxos_group_ids, int64_t& paxos_group_num)
    {
      int ret = OB_SUCCESS;
      paxos_group_num = 0;
      ObServer mergeserver;
      ObArray<int64_t> paxos_idx_array;
      ObVersionRange new_version_range;
      const ObClientManager *client_mgr = NULL;
      ObChunkServer &chunkserver = ObChunkServerMain::get_instance()->get_chunk_server();
      chunkserver.get_config_mgr().get_ms(mergeserver);
      int64_t servering_data_version = chunkserver.get_serving_data_version();
      int64_t frozen_data_version = chunkserver.get_frozen_version();
      /**
       * for daily merge, the version_range(merger version range) is (old_tablet.get_data_version, old_tablet.get_data_version +1],
       * we need to get the ups(s) which is(are) servering with the version old_tablet.get_data_version + 1,
       * our new_range is inclusive_start and inclusive_end to get the paxos group info, so do convert here!
       */

      ObPartitionManager::convert_version_range(version_range, new_version_range,
                                                servering_data_version, frozen_data_version);

      YYSYS_LOG(DEBUG, "the merge version range[%s],the get_paxos_group version range[%s]",to_cstring(version_range), to_cstring(new_version_range));

      if (IS_SYS_TABLE_ID(table_id))
      {
        //sys table's paxos group id is 0 for defalult value.
        paxos_idx_array.push_back(0);
      }
      else
      {
        if (!this->check_inner_stat())
        {
          ret = OB_ERROR;
          YYSYS_LOG(WARN,"check inner stat failed!ret=%d",ret);
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
        else if (OB_SUCCESS != (ret = rpc_proxy_->get_paxos_group_offline_info(mergeserver, new_version_range,
                                                                               client_mgr, paxos_idx_array)))
        {

            YYSYS_LOG(ERROR, "get paxos group usable info failed, paxos idx array count:[%ld], ret=%d",
                      paxos_idx_array.count(), ret);
        }
      }
      if (OB_SUCCESS == ret)
      {
        for (int64_t i=0; i<paxos_idx_array.count(); i++)
        {
          paxos_group_ids[i] = paxos_idx_array.at(i);
        }
        paxos_group_num = paxos_idx_array.count();
      }
      return ret;
    }

    int ObMultiUpsScanCellStreams::pruing_useless_paxos_group(const uint64_t table_id, int64_t *paxos_group_ids, int64_t& paxos_group_num)
    {
      int ret = OB_SUCCESS;
      bool is_table_level = false;
      ObPartitionMonitor *part_monitor        =  NULL;
      ObCsPartitionManager *partition_manager = NULL;

      if (NULL == (part_monitor = rpc_proxy_->get_cs_partition_monitor()))
      {
        ret = OB_NOT_EXIST_PARTITION_MANAGER;
        YYSYS_LOG(ERROR, "fail get part_monitor,ret=%d",ret);
      }
      else if (NULL == (partition_manager = dynamic_cast<chunkserver::ObCsPartitionManager *>(part_monitor->get_partition_manager())))
      {
        ret = OB_NOT_INIT;
        YYSYS_LOG(ERROR, "get partition manager failed, ret=%d",ret);
      }
      else if (OB_SUCCESS != (ret = partition_manager->get_table_part_type_for_daily_merge(table_id, is_table_level)))
      {
        YYSYS_LOG(WARN, "get table part type failed, ret=%d",ret);
      }
      else if (is_table_level)
      {
        chunkserver::ObCsPartitionManager::VersionPaxosId version_paxos;
        if (OB_SUCCESS != (ret = partition_manager->get_paxos_id_without_partition_for_daily_merge(table_id, version_paxos)))
        {
          YYSYS_LOG(WARN, "get paxos id without partition failed, ret=%d",ret);
        }
        else if (OB_INVALID_PAXOS_ID == version_paxos.paxos_id_)
        {
          ret = OB_ERROR;
          YYSYS_LOG(ERROR, "paxos is invalid,ret=%d",ret);
        }
        else
        {
          paxos_group_ids[0] = version_paxos.paxos_id_;
          paxos_group_num = 1;
        }
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

    int64_t ObMultiUpsScanCellStreams::get_data_version() const
    {
      int64_t size = multi_ups_streams_.count();
      int64_t data_version = OB_INVALID_VERSION;
      int64_t cur_ups_stream_data_version = OB_INVALID_VERSION;
      YYSYS_LOG(DEBUG,"get version,size is[%ld]",size);
      for (int64_t i=0; i<size; i++)
      {
        cur_ups_stream_data_version = multi_ups_streams_.at(i)->get_data_version();
        YYSYS_LOG(DEBUG, "the ups count is[%ld],current version is[%ld]", size, cur_ups_stream_data_version);
        if (cur_ups_stream_data_version > data_version)
        {
          data_version = cur_ups_stream_data_version;
        }
      }
      return data_version;
    }

  } // end namespace chunkserver
} // end namespace oceanbase

