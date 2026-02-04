/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_manager.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_UPS_MANAGER_H
#define _OB_UPS_MANAGER_H 1
#include "common/ob_server.h"
#include "common/ob_ups_info.h"
#include "common/ob_ups_info.h"
#include "rootserver/ob_root_rpc_stub.h"
#include "rootserver/ob_root_async_task_queue.h"
#include <yysys.h>

// forward declarations for unit test classes
class ObUpsManagerTest_test_basic_Test;
class ObUpsManagerTest_test_register_lease_Test;
class ObUpsManagerTest_test_register_lease2_Test;
class ObUpsManagerTest_test_offline_Test;
class ObUpsManagerTest_test_read_percent_Test;
class ObUpsManagerTest_test_read_percent2_Test;
namespace oceanbase
{
  namespace rootserver
  {
    enum ObUpsStatus
    {
      UPS_STAT_OFFLINE = 0,     ///< the ups is offline
      UPS_STAT_NOTSYNC = 1,     ///< slave & not sync
      UPS_STAT_SYNC = 2,        ///< slave & sync
      UPS_STAT_MASTER = 3,      ///< the master
    };
    //add pangtianze [Paxos Cluster.Flow.UPS] 20170817:b
    enum ObUpsFlowType
    {
      UPS_ALL_FLOW = 0,  ///< both ms and cs flow
      UPS_MS_FLOW = 1,
      UPS_CS_FLOW = 2,
    };
    //add:e
    struct ObUps
    {
        common::ObServer addr_;
        int32_t inner_port_;
        ObUpsStatus stat_;
        //del peiouya [MultiUPS] [DELETE_OBI] 20150701:b
        //common::ObiRole obi_role_;
        //del 20150701:e
        int64_t log_seq_num_;
        int64_t lease_;
        int32_t ms_read_percentage_;
        int32_t cs_read_percentage_;
        //add pangtianze [Paxos ups_replication] 20160926:b
        int64_t log_term_;
        int64_t quorum_scale_;
        //add:e
        bool did_renew_received_;
        int64_t cluster_id_;
        int64_t paxos_id_;
        //add peiouya [MultiUPS] [UPS_Manage_Function] 20150521:b
        int64_t last_frozen_version_;
        //add 20150521:e
        bool is_active_;
        //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160302:b
        int64_t offline_version_;
        //add 20160302:e
        int64_t minor_freeze_stat_;

        ObUps()
          :inner_port_(0), stat_(UPS_STAT_OFFLINE),
            log_seq_num_(0), lease_(0),
            ms_read_percentage_(0), cs_read_percentage_(0),
            //add pangtianze [Paxos ups_replication] 20160926:b
            log_term_(OB_INVALID_TERM),quorum_scale_(0),
            //add:e
            did_renew_received_(false),
            cluster_id_(-1), paxos_id_(-1),
            //mod peiouya [MultiUPS] [UPS_RESTART_BUG_FIX] 20150703:b
            //last_frozen_version_(0), //add peiouya [MultiUPS] [UPS_Manage_Function] 20150521
            last_frozen_version_(OB_INVALID_VERSION),
            //mod 20150703:e
            is_active_(false),   //add peiouya [MultiUPS] [UPS_Manage_Function] 20150527
            offline_version_(OB_DEFAULT_OFFLINE_VERSION),  //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160302
            minor_freeze_stat_(0)
        {
        }
        void reset();
        void reset_except_addr_and_ipt();
        void convert_to(common::ObUpsInfo &ups_info) const;
    };
    //add liuzy [MultiUPS] [UPS_Manager] 20151209:b
    //Exp:add struct ObUpsNode to manage information of each UPS
    enum ObPaxosClusterStatus
    {
      UPS_NODE_STAT_INVALID = -1,
      UPS_NODE_STAT_ACTIVE = 0,
      UPS_NODE_STAT_OFFLINE = 1,
      UPS_NODE_STAT_NEW = 2,
    };
    struct ObUpsNode
    {
      public:
        ObUpsNode():cluster_idx_(INVALID_INDEX), paxos_idx_(INVALID_INDEX)
        {
          initialize();
        }
        ~ObUpsNode()
        {
          ob_free(ups_array_);
        }
        inline void insert_right(ObUpsNode &node)
        {
          if (NULL == right_cluster_node_)
          {
            YYSYS_LOG(ERROR, "link ups node corrupt [node_addr:%p, right:%p]",
                      this, right_cluster_node_);
          }
          else
          {
            node.right_cluster_node_ = right_cluster_node_;
            right_cluster_node_ = &node;
          }
        }
        inline void insert_down(ObUpsNode &node)
        {
          if (NULL == down_paxos_node_)
          {
            YYSYS_LOG(ERROR, "link ups node corrupt [node_addr:%p, down:%p]",
                      this, down_paxos_node_);
          }
          else
          {
            node.down_paxos_node_ = down_paxos_node_;
            down_paxos_node_ = &node;
          }
        }

        inline ObUpsNode *next_cluster_node()
        {
          return right_cluster_node_;
        }
        inline ObUpsNode *next_paxos_group_node()
        {
          return down_paxos_node_;
        }
        inline void set_next_cluster_node(ObUpsNode *next_cluster)
        {
          right_cluster_node_= next_cluster;
        }
        inline void set_next_paxos_group_node(ObUpsNode *next_paxos)
        {
          down_paxos_node_ = next_paxos;
        }
      public:
        int64_t cluster_idx_;
        int64_t paxos_idx_;
        ObArray<ObUps> *ups_array_;
      private:
        inline void initialize()
        {
          right_cluster_node_ = this;
          down_paxos_node_ = this;
          ups_array_ = OB_NEW(ObArray<ObUps>, ObModIds::OB_RS_UPS_MANAGER);
        }
        ObUpsNode *right_cluster_node_;
        ObUpsNode *down_paxos_node_;
    };
    typedef int64_t ClusterIdx;
    typedef int64_t UpsIdx;
    //add 20151209:e

    class ObRootWorker;
    class ObRootAsyncTaskQueue;
    class ObUpsManager
    {
      public:
        //mod pangtianze [MulitUPS] [merge with paxos] 20170517:b
        //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
        //        ObUpsManager(ObRootRpcStub &rpc_stub, ObRootWorker *worker, const int64_t &revoke_rpc_timeout_us,
        //                     int64_t lease_duration, int64_t lease_reserved_us, bool is_use_paxos, int64_t use_cluster_num,
        //                     int64_t use_paxos_num, const common::ObiRole &obi_role,
        //                     const volatile int64_t& schema_version, const volatile int64_t &config_version);
        //        ObUpsManager(ObRootRpcStub &rpc_stub, ObRootWorker *worker, const int64_t &revoke_rpc_timeout_us,
        //                    int64_t lease_duration, int64_t lease_reserved_us, bool is_use_paxos,
        //                     int64_t use_cluster_num,
        //                     int64_t use_paxos_num, const volatile int64_t& schema_version, const volatile int64_t &config_version
        //                     //add pangtianze [Paxos rs_election] 20150820:b
        //                     ,const int64_t &max_deployed_ups_count
        //                     //add:e
        //                     );
        ObUpsManager(ObRootRpcStub &rpc_stub, ObRootWorker *worker, const int64_t &revoke_rpc_timeout_us,
                     const int64_t &lease_duration,
                     const int64_t &lease_reserved_us,
                     const int64_t &waiting_ups_register_duration,
                     bool is_use_paxos,
                     int64_t use_cluster_num,
                     int64_t use_paxos_num, const volatile int64_t& schema_version, const volatile int64_t &config_version
                     //add pangtianze [Paxos rs_election] 20150820:b
                     //, const int64_t &max_deployed_ups_count
                     //add:e
                     , int32_t (&paxos_ups_quorum_scales)[OB_MAX_PAXOS_GROUP_COUNT]
                     );
        //mod 20150701:e

        virtual ~ObUpsManager();
        //add pangtianze [Paxos ups_election] 20161010:b
        inline bool get_is_select_new_master() const
        {
          return is_select_new_master_;
        }
        inline void set_is_select_new_master(const bool is_select)
        {
          __sync_bool_compare_and_swap(&is_select_new_master_, !is_select, is_select);
        }
        //add:e
        //del peiouya [MultiUPS] [DELETE_OBI] 20150701:b
        //int  send_obi_role();
        //del 20150701:e
        int  get_ups_master_by_paxos_id(const int64_t& paxos_id, ObUps &ups_master) const;
        int  get_ups_master(ObUps &ups_master) const;
        int  get_sys_table_ups_master(ObUps &ups_master) const;
        int  set_ups_config(int32_t read_master_ups_percentage
                            ,int32_t merge_master_ups_percentage
                            //add bingo[Paxps Cluster.Flow.UPS] 20170116:b
                            ,const int32_t is_strong_consistent = 0, const ObUpsFlowType flow_type = UPS_ALL_FLOW //add pangtianze [Paxos Cluster.Flow.UPS 20170817:b
                                                                                                   //add:e
                                                                                                   );
        //add pangtianze [Paxos Cluster.Flow.UPS] 20170817:b
        inline bool get_is_flow_control_by_ip() const
        { return is_flow_control_by_ip_; }
        inline bool get_is_cs_flow_init() const
        { return is_cs_flow_init_; }
        inline void set_is_cs_flow_init(const bool init)
        { is_cs_flow_init_ = init; }
        //add:e
        int  get_random_paxos_id_for_group(int64_t& paxos_id);
        int  get_ups_list_by_paxos_id(common::ObUpsList &ups_list, const int64_t& paxos_id) const;
        void get_master_ups_list(common::ObUpsList &ups_list) const;
        void get_online_master_ups_list(common::ObUpsList &ups_list) const;

        //[492]
        void force_get_online_sys_table_ups_list(common::ObUpsList &ups_list) const;

        void set_async_queue(ObRootAsyncTaskQueue * queue);
        void set_root_server(ObRootServer2 *root_server);
        void check_lease();
        int64_t get_max_frozen_version();

        void check_minor_frozen_and_set_stat();

        //add peiouya [MultiUPS] [PARTITION_LOCK_FUCTION] 20150609:b
        int64_t get_new_frozen_version()
        { return new_frozen_version_; }
        bool get_initialized()
        { return initialized_; }
        //add 20150609:e

        //mod peiouya [MultiUPS] [PARTITION_LOCK_FUCTION] 20150609:b
        //void set_new_frozen_version(const int64_t new_frozen_version);
        void set_new_frozen_version(const int64_t new_frozen_version, const bool is_cmd_set = false);
        //mod 20150609:e
        //void reset_ups_read_percent();
        void check_and_set_last_frozen_version();
        void print(char* buf, const int64_t buf_len, int64_t &pos) const;
        int get_ups_list(common::ObUpsList &ups_list) const;
        int get_ups_list_for_cbtop(common::ObUpsList &ups_list)const;
        void check_ups_master_exist();
        void set_cluster_and_paxos_config_and_paxos_flag(int64_t use_cluster_num, int64_t use_paxos_id_num,
                                                         bool is_use_paxos);
        //add bingo [Paxos rs_admin all_server_in_clusters] 20170612:b
        void print_in_cluster(char* buf, const int64_t buf_len, int64_t &pos, int32_t cluster_id) const;
        //add:e

        //add liuzy [MultiUPS] [add_paxos_interface] 20160113:b
        int  add_new_paxos_group(int64_t new_paxos_num, int64_t new_config);
        int  cons_new_paxos_group(int64_t new_paxos_num, ObUpsNode *&new_paxos_head, ObArray<ObUpsNode*> *node_array);
        int  link_new_paxos_group(ObUpsNode * const new_paxos_groups_head);
        //add 20160113:e
        int delink_last_paxos_group(const int64_t paxos_idx);
        int del_last_paxos_group(const int64_t paxos_idx);

        //mod peiouya [MultiUPS] 20160113:b
        //int register_ups(const common::ObServer &addr, int32_t inner_port, int64_t log_seq_num, int64_t lease, const char *server_version); // uncertainty ups gaixie
        int  register_ups(const common::ObServer &addr, int32_t inner_port, int64_t log_seq_num,
                          int64_t lease, const char *server_version, const int64_t cluster_idx,
                          const int64_t paxos_idx, int64_t last_frozen_version);
        //mod 20160113:e
        //mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
        //        //mod peiouya [MultiUPS] [PARTITION_LOCK_FUCTION] 20150601:b
        //        //int renew_lease(const common::ObServer &addr, ObUpsStatus stat, const common::ObiRole &obi_role);
        //        //int  renew_lease(const common::ObServer &addr, ObUpsStatus stat, const common::ObiRole &obi_role,
        //        //                 const int64_t last_frozen_version, const bool is_active);
        //        int  renew_lease(const common::ObServer &addr, ObUpsStatus stat, const common::ObiRole &obi_role,
        //                         const int64_t last_frozen_version, const bool is_active, const bool partition_lock_flag);
        //     //mod 20150601:e
        int  renew_lease(const common::ObServer &addr, ObUpsStatus stat,
                         int64_t last_frozen_version, const bool is_active, const bool partition_lock_flag
                         //add chujiajia [Paxos rs_election] 20151119:b
                         , const int64_t &quorum_scale
                         //add:e
                         ,const int64_t minor_freeze_stat
                         );
        //mod 20150701:e
        int slave_failure(const common::ObServer &addr, const common::ObServer &slave_addr);
        //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160223:b
        int  recover_paxos_group_offline_version(const int64_t &paxos_idx, const ObBitSet<OB_MAX_CLUSTER_COUNT> &bitset);
        int  set_paxos_ups_offline_version(const int64_t &paxos_idx, const int64_t &offline_version);
        void set_paxos_status(const int64_t &paxos_idx, ObPaxosClusterStatus new_stat);
        void change_paxos_stat(const int64_t &paxos_idx, const ObPaxosClusterStatus new_stat);
        bool is_paxos_existent(const int64_t &paxos_idx) const;
        bool is_paxos_group_offline(const int64_t &paxos_idx) const;
        bool is_paxos_group_active(const int64_t &paxos_idx) const;
        ObPaxosClusterStatus get_paxos_status(const int64_t &paxos_idx) const;
        //add 20160223:e
        int set_ups_master(const common::ObServer &master, bool did_force);
        //add liuzy [MultiUPS] [add_cluster_interface] 20160311:b
        int add_new_cluster(int64_t new_cluster_num, int64_t new_config);
        int cons_new_cluster(int64_t new_cluster_num, ObUpsNode *&new_cluster_head, ObArray<ObUpsNode*> *node_array);
        int link_new_cluster(ObUpsNode * const new_cluster_head);
        void free_ups_node(ObArray<ObUpsNode*> *node_array);
        int64_t get_last_paxos_idx() const;
        int64_t get_last_cluster_idx() const;
        //add 20160311:e
        int delink_last_cluster(const int64_t cluster_idx);
        int del_last_cluster(const int64_t cluster_idx);
        int set_ups_config(const common::ObServer &addr, int32_t ms_read_percentage, int32_t cs_read_percentage);
        //del pangtianze [MultiUPS] [merge with paxos] 20170519:b
        //int set_ups_config(int32_t read_master_master_ups_percentage, int32_t read_slave_master_ups_percentage);
        //del:e
        void get_master_ups_config(int32_t &master_master_ups_read_percent
                                   //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
                                   //, int32_t &slave_master_ups_read_precent
                                   //del:e
                                   ) const;
        //int send_obi_role();
        //add liuzy [MultiUPS] [take_cluster_offline_interface] 20160325:b
        int  check_paxos_and_cluster_vaild(const int64_t cluster_idx , const int64_t paxos_idx);
        int  get_ups_offline_count_next_version_by_paxos_id(int64_t & ups_count, const int64_t& paxos_idx) const;
        int  get_ups_list_by_cluster_id(common::ObUpsList &ups_list, const int64_t& cluster_idx) const;
        int  set_cluster_ups_offline_version(const int64_t &cluster_idx, const int64_t &offline_version);
        int  recover_cluster_offline_version(const int64_t &cluster_idx,
                                             const ObBitSet<MAX_UPS_COUNT_ONE_CLUSTER> &bitset);
        bool is_initialized();

        bool is_cluster_online(const int64_t &cluster_idx) const;

        bool is_cluster_offline(const int64_t &cluster_idx);
        bool is_cluster_existent(const int64_t &cluster_idx) const;
        bool is_ups_offline_count_valid(const int64_t paxos_id, const int64_t down_count,
                                        const int64_t quorum_scale, int64_t &permit_count);
        
        bool is_cluster_ups_offline_count_valid(const int64_t cluster_idx, int32_t &permit_count, int64_t &paxos_id);

        void set_cluster_status(const int64_t &cluster_idx, ObPaxosClusterStatus new_stat);
        void change_cluster_stat(const int64_t &cluster_idx, const ObPaxosClusterStatus new_stat);
        ObPaxosClusterStatus get_cluster_status(const int64_t &cluster_idx) const;
        //add 20160325:e
        int grant_lease(bool did_force = false);     // for heartbeat thread
        //del peiouya [MultiUPS] 20160325:b
        //int grant_eternal_lease();     // for heartbeat thread
        //int check_lease();    // for check thread
        //int check_ups_master_exist(); // for check thread
        //del 20160325:e
        //add peiouya [Get_masterups_and_timestamp] 20141017:b
        // int64_t get_ups_set_time(const common::ObServer &addr);del lqc [multiups with paxos] 20170610
        //add 20141017:e
        //add pangtianze [Paxos ups_replication] 20150603:b
        bool check_most_ups_registered(const int64_t paxos_id);
        //add:e
        //add pangtianze [Paxos ups_replication] 20161010:b
        // inline int64_t get_max_deployed_ups_count() const
        // {
        //     return max_deployed_ups_count_;
        // }
        //add:e
        //add lqc [MultiUps 1.0] [#13] 20170405 b
        int64_t get_use_paxos_num() const ;
        // add e
        // add lqc [Multiups][check master] 201707012 b
        bool is_rs_master();
        //add e
        //add chujiajia [Paxos rs_election] 20151105:b
        inline bool get_is_ups_master_online(const int64_t paxos_id)
        {
          bool ret = true;
          int64_t now = yysys::CTimeUtil::getTime();
          ObUpsNode* ups_master = NULL;
          int64_t cluster_id = -1;
          int64_t ups_idx = -1;
          cluster_id = master_idx_array_new_[paxos_id].first;
          ups_idx = master_idx_array_new_[paxos_id].second;
          ups_master = find_node(cluster_id, paxos_id);
          if(ups_master != NULL)
          {
            if(now > ups_master->ups_array_->at(ups_idx).lease_)
            {
              ret = false;
            }
            else
            {
              ret = true;
            }
          }
          return ret;
        }
        inline bool get_is_all_ups_master_online()
        {
          bool ret = true;
          for(int64_t i = 0; i < use_paxos_num_; i++)
          {
            ret = get_is_ups_master_online(i);
            if(ret == false)
            {
              break;
            }
          }
          return ret;
        }
        //add:e
        //add chujiajia [Paxos rs_election] 20151222:b
        void ups_array_reset();
        void if_all_quorum_changed(char *buf, const int64_t buf_len, int64_t& pos) const;
        //void if_all_quorum_changed(char *buf, const int64_t buf_len, int64_t& pos, const int64_t& local_quorum_scale) const;
        //add:e
        //add chujiajia [Paxos rs_election] 20151230:b
        int64_t get_ups_count(const int64_t &paxos_id) const;
        int32_t get_active_ups_count(const int64_t paxos_id) const;
        //add:e
        //add jintianyang [paxos test] 20160530:b
        void print_ups_leader(char *buf, const int64_t buf_len,int64_t& pos);
        //add:e
        //add lbzhong [Paxos Cluster.Balance] 20160706:b
        void is_cluster_alive_with_ups(bool* is_cluster_alive);
        bool is_cluster_alive_with_ups(const int32_t cluster_id);
        //add:e
        //add pangtianze [Paxos inner table revise] 20170826
        void get_task_array_for_inner_table_revise(
            ObSEArray<ObRootAsyncTaskQueue::ObSeqTask, OB_MAX_UPS_COUNT>& task_arr);
        //add:e
        //add huangjianwei [Paxos rs_switch] 20160728:b
        //add:e
        void set_waiting_ups_finish_time(int64_t waiting_ups_finish_time)
        { waiting_ups_finish_time_ = waiting_ups_finish_time; }

        int get_ups_memory(int64_t &max_used_mem_size);
        int send_get_ups_memory_msg(const common::ObServer &addr, int64_t &mem_size);

        //[677]
        ObUpsNode* get_link_head() const;

        friend class ObRootInnerTableTask;
      private:
        // change the ups status and then log this change
        // del lqc [multiups with paxos] 20170610
        // void change_ups_stat(const int32_t index, const ObUpsStatus new_stat);//uncertainty  ups ?????
        // int find_ups_index(const common::ObServer &addr) const;
        // del e
        //bool did_ups_exist(const common::ObServer &addr) const;//uncertainty  ups ??��
        //bool is_ups_master(const common::ObServer &addr) const; //uncertainty  ups ??????
        bool has_master() const;
        // bool is_master_lease_valid() const; // del lqc [multiups with paxos]
        bool need_grant(int64_t now, const ObUps &ups) const;
        int send_granting_msg(const common::ObServer &addr, common::ObMsgUpsHeartbeat &msg);
        //int select_new_ups_master(); by pangtianze
        //int select_ups_master_with_highest_lsn();
        //del pangtianze [Paxos ups_replication] 20150605:b
        //void update_ups_lsn();
        //bool is_ups_with_highest_lsn(int ups_idx);
        //del:e
        //add pangtianze [Paxos ups_replication] 20150722:b
        //int update_ups_lsn();
        //add:e
        // add  lqc [multiups with paxos] 20170610
        int revoke_master_lease(int64_t &waiting_lease_us,int64_t& paxos_idx);
        int64_t find_master_ups_index(const int64_t &paxos_idx, ObUpsNode *&ups_node) const;
        ObUpsNode *find_master_node(const int64_t &paxos_idx, int64_t &array_idx) const;
        //add e
        int send_revoking_msg(const common::ObServer &addr, int64_t lease, const common::ObServer& master);
        const char* ups_stat_to_cstr(ObUpsStatus stat) const;
        //void check_all_ups_offline();// del lqc [multiups with paxos] 20170610
        //bool is_idx_valid(int ups_idx) const; // del lqc [multiups with paxos] 20170610
        // push update inner table task into queue
        int refresh_inner_table(const ObTaskType type, const ObUps & ups, const char *server_version);
        void init_master_idx_array();
        //mod [merge with paxos]
        //void reset_ups_read_percent_by_paxos_id(const int64_t& paxos_id);
        //mod pangtianze [Paxos Cluster.Flow.UPS] 20170817:b
        //void reset_ups_read_percent();
        void reset_ups_read_percent(const ObUpsFlowType flow_type = UPS_ALL_FLOW);
        //mod:e
        int32_t get_ups_count_in_one_node(ObUpsNode * ups_node)const;
        bool node_has_master(ObUpsNode * ups_node) const;
        void set_ups_read_percent(const int32_t master_read_percent,
                                  const int32_t slave_read_percent,
                                  const int32_t master_merge_percent,
                                  const int32_t slave_merge_percent,
                                  ObUpsNode * ups_node,
                                  const ObUpsFlowType flow_type //add pangtianze [Paxos Cluster.Flow.UPS] 20170817:b:e
                                  );
        //mod e
        
        //del liuzy [MultiUPS] [useless_func] 20160526:b
        //        bool did_ups_exist(const common::ObServer &addr) const;
        //del 20160526:e
        bool check_all_ups_to_special_version(const int64_t frozen_version);
        int64_t find_ups_index(const common::ObServer &addr, ObUpsNode *&ups_node, bool iter_server = false) const;
        //del chujiajia [Paxos rs_election] 20160122
        //int32_t get_active_ups_count(const int64_t paxos_id) const;
        //del:e
        

        //add liuzy [MultiUPS] [UPS_Manager] 20151211:b
        void init_ups_node();
        void print_ups_manager() const;
        void print_ups_registered() const;
        int  add_node(int64_t clu_idx, int64_t pax_idx, ObUpsNode * const head_node = NULL,
                      ObArray<ObUpsNode*> *node_array = NULL);
        int  insert_node(ObUpsNode *up_node, ObUpsNode *left_node, int64_t clu_idx,
                         int64_t pax_idx, ObArray<ObUpsNode*> *node_array = NULL);
        ObUpsNode *find_paxos_head_node(int64_t pax_idx) const;
        ObUpsNode *find_cluster_head_node(int64_t clu_idx) const;
        ObUpsNode *find_node(const common::ObServer &addr, int64_t &array_idx, ObUpsNode* const head_node = NULL, bool iter_all = false) const;
        ObUpsNode *find_node(int64_t clu_idx, int64_t pax_idx, ObUpsNode * const head_node = NULL) const;
        //add 20151211:e
        //add liuzy [MultiUPS] [UPS_Manager] 20151221:b
        bool    check_master_idx_array_info_valid(ObUpsNode *&master_node, int64_t &cluster_idx,
                                                  const int64_t paxos_idx, int64_t &master_ups_idx) const;
        int64_t master_last_frozen_version(const int64_t paxos_idx) const;
        //add 20151221:e

        //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160223:b
        void init_paxos_group_stat_array();
        void init_cluster_stat_array();
        //add 20160223:e

        void init_freeze_paxos_group_stat_array();
        int decide_freeze_stat(const int64_t &paxos_idx);
        void decide_minor_freeze_done();

        //mod liuzy [MultiUPS] [UPS_Manager] 20151211:b
        bool has_master(const int64_t& paxos_idx) const;
        bool is_ups_idx_valid(const int64_t ups_idx, const ObUpsNode *ups_node) const;
        bool is_ups_master(const common::ObServer &addr) const;
        void change_ups_stat(const int64_t &cluster_id, const int64_t &paxos_id,
                             const int64_t &ups_idx, const ObUpsStatus new_stat);
        //del chujiajia [Paxos rs_election] 20151230:b
        //int64_t get_ups_count(const int64_t paxos_idx) const;
        //del:e
        //mod 20151211:e
        //mod liuzy [MultiUPS] [UPS_Manager] 20160104:b
        //mod by pangtianze [MultiUPS] [merge with paxos] 20170518:b
        /**
         * @brief the parameter 'exp_ups_addr' means the ups that user expect,
         *        but still based on log and term
         * @param paxos_idx
         * @param exp_ups_addr
         * @return
         */
        int  select_new_ups_master(const int64_t &paxos_idx, const common::ObServer *exp_ups_addr = NULL);
        int  select_ups_master_with_highest_lsn(const int64_t &paxos_idx);
        int  grant_lease_by_paxos_id(const int64_t &paxos_idx, bool did_force = false);
        void set_initialized();
        //del pangtianze [Paxos ups_replication] 20150605:b
        //void update_ups_lsn(const int64_t paxos_idx);
        //del:e
        bool is_master_lease_valid(const int64_t paxos_idx) const;
        bool check_master_ups_valid(const int64_t paxos_idx) const;
        void check_ups_master_exist_by_paxos_id(const int64_t &paxos_idx);
        //mod 20160104:e
        void check_slave_frozen_version(int64_t &last_frozen_version, const int64_t paxos_idx, const common::ObServer &addr);


        //add peiouya [MultiUPS] [UPS_Manage_Function] 20150815:b
        int check_ups_info_valid(const int64_t last_frozen_version, const int64_t paxos_idx,
                                 const bool is_master = false);
        bool is_max_version_in_paxos(const int64_t frozen_version, const int64_t paxos_idx) const;
        //add 20150815:e
        // push update inner table task into queue
        //int refresh_inner_table(const ObTaskType type, const ObUps & ups, const char *server_version);
        // disallow copy
        // ObUpsManager(const ObUpsManager &other);
        ObUpsManager& operator=(const ObUpsManager &other);
        // for unit test
        // del lqc [multiups with paxos ]20170610
        // int32_t get_ups_count() const;
        // del e
        // int32_t get_active_ups_count() const; // uncertainty ups gaixie
        //add peiouya [MultiUPS] [UPS_Manage_Function] 20150521:b
        bool check_all_online_ups_version_valid(int64_t& frozen_version);
        //add 20150521:e
        //add pangtianze [Paxos ups_replication] 20150722:b
        void update_ups_lsn(const int64_t &paxos_idx);
        //add:e
        //add pangtianze [Paxos ups_replication] 20150603:b
        bool is_ups_with_highest_lsn_and_term(int64_t ups_idx, const int64_t paxos_id);
        int select_ups_master_with_highest_lsn_and_term(const int64_t paxos_id, const common::ObServer *exp_ups_addr = NULL);
        int update_ups_lsn_and_term(const int64_t paxos_idx);
        //add:e
        friend class ::ObUpsManagerTest_test_basic_Test;
        friend class ::ObUpsManagerTest_test_register_lease_Test;
        friend class ::ObUpsManagerTest_test_register_lease2_Test;
        friend class ::ObUpsManagerTest_test_offline_Test;
        friend class ::ObUpsManagerTest_test_read_percent_Test;
        friend class ::ObUpsManagerTest_test_read_percent2_Test;
      private:
        static const int32_t MAX_UPS_COUNT = 17;
        static const int64_t MAX_CLOCK_SKEW_US = 200000LL; // 200ms
        //add peiouya [MultiUPS] [UPS_Manage_Function] 20150815:b
        static const int64_t PRINT_INTERVAL = 60* 1000 * 1000LL; // 60s
        //add 20150815:e

        //mod liuzy [MultiUPS] [UPS_Manager] 20160314:b
        static const int64_t PRINT_INTERVAL_CHECK = 3 * 1000 * 1000LL; // 3s
        //mod 20160314:e
        static const int64_t FORCE_CHANGE_UPS_MASTER_INTERVAL = 10 * 1000 * 1000LL;
        // data members
        ObRootServer2 *root_server_;
        // timer task queue, aim to modify table __all_server. operate class ObRootInnerTableTask
        ObRootAsyncTaskQueue * queue_;
        // remote procedure call
        ObRootRpcStub &rpc_stub_;
        ObRootWorker *worker_;
        // only represent rs
        //del peiouya [MultiUPS] [DELETE_OBI] 20150701:b
        //const common::ObiRole &obi_role_;
        //del 20150701:e
        const int64_t &revoke_rpc_timeout_us_;
        mutable yysys::CThreadMutex ups_array_mutex_;
        ObUps ups_array_[MAX_UPS_COUNT];
        //add peiouya [Get_masterups_and_timestamp] 20141017:b
        // int64_t ups_mater_timestamp[MAX_UPS_COUNT]; // del lqc [multiups with paxos] 20170610
        //add 20141017:e
        //mod pangtianze 20170210:b
        /*int64_t lease_duration_us_;
        int64_t lease_reserved_us_;//8500ms*/
        const int64_t &lease_duration_us_;
        const int64_t &lease_reserved_us_;
        //mod:e
        //del peiouya [MultiUPS] [UPS_Manage_Function] 20150729:b
        //// sys tables do not partition, so choose a fixed paxos group for saving sys tables
        //int64_t sys_table_paxos_id_;
        //del 20150829:e
        int32_t ups_master_idx_;
        //mod pangtianze 20170210:b
        //int64_t waiting_ups_register_duration_;
        const int64_t &waiting_ups_register_duration_;
        //mod:e
        bool is_use_paxos_;
        // all paxos group use the same setting,
        int64_t use_cluster_num_;
        int64_t use_paxos_num_;
        int64_t waiting_ups_finish_time_;
        const volatile int64_t& schema_version_;
        const volatile int64_t& config_version_;
        int32_t master_master_ups_read_percentage_;
        volatile int64_t new_frozen_version_;
        // all master servers traffic use the setting
        int32_t slave_master_ups_read_percentage_;
        int32_t master_ups_read_percentage_;

        //[628]
        int32_t master_ups_merge_percentage_;

        bool is_flow_control_by_ip_;
        bool is_cs_flow_init_; //add pangtianze [Paxos Cluster.Flow.UPS] 20170817
        //only set value once, only when rs restart
        //true means system start ok, false means system initialize error(not all paxos ups online)
        //set true only when all use paxos group has master in rs initing
        volatile bool initialized_;

        //del liuzy [MultiUPS] [UPS_Manager] 20160118:b
        // save per paxos group`s master ups idx
        //        int64_t master_idx_array_[MAX_UPS_COUNT_ONE_CLUSTER];
        // save all cluster servers
        //        ObUps ups_array_[MAX_CLUSTER_COUNT][MAX_UPS_COUNT_ONE_CLUSTER];
        //del 20160118:e

        //add liuzy [MultiUPS] [UPS_Manager] 20151210:b
        //Exp:head node of the double linked list
        ObUpsNode *link_head_;
        std::pair<ClusterIdx, UpsIdx> master_idx_array_new_[MAX_UPS_COUNT_ONE_CLUSTER];
        //add 20151210:e
        //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160223:b
        ObPaxosClusterStatus paxos_group_stat_[MAX_UPS_COUNT_ONE_CLUSTER];
        //add 20160223:e
        //add liuzy [MultiUPS] [take_cluster_offline_interface] 20160325:b
        ObPaxosClusterStatus cluster_stat_[OB_MAX_CLUSTER_COUNT];
        //add 20160325:e
        //add pangtianze [Paxos Cluster.Flow.UPS] 20170119:b
        int32_t is_strong_consistency_;
        //add:e
        //add pangtianze [Paxos rs_election] 2016010:b
        bool is_select_new_master_;
        //add:e
        //add pangtianze [Paxos ups_replication] 20150603:b
        //const int64_t& max_deployed_ups_count_;
        //add:e
        int32_t (&paxos_ups_quorum_scales_)[OB_MAX_PAXOS_GROUP_COUNT];
        //add chujiajia [Paxos rs_election] 20151217:b
        //bool is_old_ups_master_regist_[MAX_UPS_COUNT_ONE_CLUSTER];
        //common::ObServer old_ups_master_[MAX_UPS_COUNT_ONE_CLUSTER];
        //add:e
        int64_t last_change_ups_master_;
        bool freeze_stat_[MAX_UPS_COUNT_ONE_CLUSTER];
    };
  } // end namespace rootserver
} // end namespace oceanbase

#endif /* _OB_UPS_MANAGER_H */

