#ifndef OCEANBASE_MERGESERVER_MERGESERVER_H_
#define OCEANBASE_MERGESERVER_MERGESERVER_H_

#include "common/ob_single_server.h"
#include "common/ob_packet_factory.h"
#include "common/ob_server.h"
#include "common/ob_timer.h"
#include "common/thread_buffer.h"
#include "common/ob_client_manager.h"
#include "common/ob_config_manager.h"
#include "common/ob_privilege_manager.h"
#include "common/ob_obj_pool.h"
#include "ob_merge_server_service.h"
#include "ob_frozen_data_cache.h"
#include "ob_insert_cache.h"
#include "ob_bloom_filter_task_queue_thread.h"
#include "ob_ms_sql_scan_request.h"
#include "ob_ms_sql_get_request.h"
//add pangtianze [Paxos rs_election] 20150710:b
#include "common/ob_rs_address_mgr.h"
//add:e
#include "ob_statistic_info_cache.h"

namespace oceanbase
{
  namespace mergeserver
  {
    class ObMergeServer : public common::ObSingleServer
    {
      public:
        ObMergeServer(ObConfigManager &config_mgr,
                      ObMergeServerConfig &ms_config);
      public:
        void set_privilege_manager(ObPrivilegeManager *privilege_manager);

      public:
        int initialize();
        int start_service();
        void destroy();
        int reload_config();

        int do_request(common::ObPacket* base_packet);
        //
        bool handle_overflow_packet(common::ObPacket* base_packet);
        void handle_timeout_packet(common::ObPacket* base_packet);
      public:
        common::ThreadSpecificBuffer::Buffer* get_response_buffer() const;

        common::ThreadSpecificBuffer* get_rpc_buffer();

        const common::ObServer& get_self() const;

        const common::ObServer& get_root_server() const;
        //add pangtianze [Paxos rs_election] 20150707:b
        void set_root_server(const common::ObServer &server);
        //add:e

        //add huangjianwei [Paxos rs_election] 20160513:b
        void set_new_root_server(const common::ObServer &server);
        //add:e
        common::ObTimer& get_timer();

        bool is_stoped() const;

        const common::ObClientManager& get_client_manager() const;

        ObMergeServerConfig& get_config();
        ObConfigManager& get_config_mgr();
        mergeserver::ObMergerRpcProxy  *get_rpc_proxy() const
        { return service_.get_rpc_proxy(); }
        mergeserver::ObMergerRootRpcProxy * get_root_rpc() const
        { return service_.get_root_rpc(); }
        mergeserver::ObMergerAsyncRpcStub   *get_async_rpc() const
        { return service_.get_async_rpc(); }
        common::ObMergerSchemaManager *get_schema_mgr() const
        { return service_.get_schema_mgr(); }
        //add liu jun. [MultiUPS] [part_cache] 20150525:b
        common::ObPartitionMonitor *get_partition_monitor() const
        { return service_.get_partition_monitor(); };
        //20150525:e
        common::ObTabletLocationCacheProxy *get_cache_proxy() const
        { return service_.get_cache_proxy(); }
        common::ObStatManager* get_stat_manager() const
        { return service_.get_stat_manager(); }

        int set_sql_session_mgr(sql::ObSQLSessionMgr* mgr);
        void set_sql_id_mgr(sql::ObSQLIdMgr* mgr)
        { service_.set_sql_id_mgr(mgr); };
        inline const ObMergeServerService &get_service() const
        {
          return service_;
        }

        inline ObPrivilegeManager* get_privilege_manager()
        {
          return &privilege_mgr_;
        }
        inline ObFrozenDataCache & get_frozen_data_cache()
        {
          return frozen_data_cache_;
        }
        //add wangyao [sequence for optimize]:20171008
        inline ObFrozenDataCache & get_sequence_frozen_data_cache()
        {
          return sequence_frozen_data_cache_;
        }
        //add e
        inline common::ObObjPool<mergeserver::ObMsSqlScanRequest> & get_scan_req_pool()
        {
          return scan_req_pool_;
        }
        inline common::ObObjPool<mergeserver::ObMsSqlGetRequest> & get_get_req_pool()
        {
          return get_req_pool_;
        }
        inline ObInsertCache & get_insert_cache()
        {
          return insert_cache_;
        }
        inline ObBloomFilterTaskQueueThread & get_bloom_filter_task_queue_thread()
        {
          return bloom_filter_queue_thread_;
        }
        //add pangtianze [Paxos rs_election] 20150710:b
        inline common::ObRsAddressMgr & get_rs_mgr()
        {
          return rs_mgr_;
        }
        //add:e

        inline int add_udi_index(uint64_t tid)
        {
            int ret=OB_SUCCESS;
            if(udi_counter_helper_.get_array_index() >= OB_MAX_TABLE_NUMBER - 1)
            {
                ret = OB_ERROR_OUT_OF_RANGE;
                YYSYS_LOG(WARN,"udi_count array has reach the max size! ret=%d",ret);
            }
            else if(hash::HASH_INSERT_SUCC!=(ret=udi_index_.set(tid,udi_counter_helper_.get_array_index())))
            {
                YYSYS_LOG(WARN,"failed to add tid[%ld] index[%ld] into udi_index_! ret=%d",tid,udi_counter_helper_.get_array_index(),ret);
            }
            else
            {
                udi_counter_helper_.push_back(0);
                YYSYS_LOG(INFO,"succeed pushing tid[%ld] into the udi_index_list!",tid);
            }
            return ret;
        }

        inline int inc_udi_counter(uint64_t tid)
        {
            int ret = OB_SUCCESS;
            int64_t index=0;
            if(hash::HASH_EXIST!=(ret=udi_index_.get(tid,index)))
            {
                YYSYS_LOG(WARN,"failed to get the index of tid[%ld]!, ret=%d",tid,ret);
            }
            else
            {
                if(index < udi_counter_helper_.get_array_index())
                {
                    YYSYS_LOG(DEBUG,"Succeed inc udi counter!");
                    udi_counter_[index]++;
                    ret = OB_SUCCESS;
                    YYSYS_LOG(INFO,"the udi_counter_[%ld]=%ld",index,udi_counter_[index]);
                }
                else
                {
                    ret = OB_ERROR_OUT_OF_RANGE;
                    YYSYS_LOG(WARN,"index out of the udi_counter's range! ret=%d",ret);
                }
            }
            return ret;
        }

        inline ObStatisticInfoCache* get_statistic_info_cache()
        {
            return &statistic_info_cache_;
        }

      private:
        DISALLOW_COPY_AND_ASSIGN(ObMergeServer);
        int init_root_server();
        int set_self(const char* dev_name, const int32_t port);
        // handle no response request add timeout as process time for monitor info
        void handle_no_response_request(common::ObPacket * base_packet);
        void on_ioth_start();

      private:
        static const int64_t DEFAULT_LOG_INTERVAL = 100;
        int64_t log_count_;
        int64_t log_interval_count_;
        /* ObMergeServerParams& ms_params_; */
        ObConfigManager& config_mgr_;
        ObMergeServerConfig& ms_config_;
        common::ObTimer task_timer_;
        common::ThreadSpecificBuffer response_buffer_;
        common::ThreadSpecificBuffer rpc_buffer_;
        common::ObClientManager client_manager_;
        common::ObServer self_;
        common::ObServer root_server_;
        ObMergeServerService service_;
        common::ObPrivilegeManager privilege_mgr_;
        ObFrozenDataCache frozen_data_cache_;
        ObFrozenDataCache sequence_frozen_data_cache_;//add wangyao:20171008
        ObInsertCache insert_cache_;
        ObBloomFilterTaskQueueThread bloom_filter_queue_thread_;
        common::ObObjPool<mergeserver::ObMsSqlScanRequest> scan_req_pool_;
        common::ObObjPool<mergeserver::ObMsSqlGetRequest> get_req_pool_;
        //add pangtianze [Paxos rs_election] 20150707:b
        common::ObRsAddressMgr rs_mgr_;
        //add:e
        ObStatisticInfoCache statistic_info_cache_;
        hash::ObHashMap<uint64_t, int64_t, hash::NoPthreadDefendMode> udi_index_;
        int64_t udi_counter_[OB_MAX_TABLE_NUMBER];
        common::ObArrayHelper<int64_t> udi_counter_helper_;

    };
  } /* mergeserver */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_MERGESERVER_MERGESERVER_H_ */
