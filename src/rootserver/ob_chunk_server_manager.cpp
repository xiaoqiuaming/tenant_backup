/*===============================================================
*   (C) 2007-2010 Taobao Inc.
*
*
*   Version: 0.1 2010-09-26
*
*   Authors:
*          daoan(daoan@taobao.com)
*
*
================================================================*/
#include <yylog.h>

#include "common/file_utils.h"
#include "common/ob_malloc.h"
#include "common/ob_record_header.h"
#include "common/utility.h"
#include "ob_chunk_server_manager.h"

namespace oceanbase
{
  namespace rootserver
  {
    using namespace common;
    ObBalanceInfo::ObBalanceInfo()
      :table_sstable_total_size_(0),
       table_sstable_count_(0),
       cur_in_(0),
       cur_out_(0)
    {
    }

    ObBalanceInfo::~ObBalanceInfo()
    {
      reset();
    }

    void ObBalanceInfo::reset()
    {
      reset_for_table();
      cur_in_ = 0;
      cur_out_ = 0;
    }

    void ObBalanceInfo::reset_for_table()
    {
      table_sstable_count_ = 0;
      table_sstable_total_size_ = 0;
    }

    void ObBalanceInfo::inc_in()
    {
      atomic_inc(reinterpret_cast<uint32_t*>(&cur_in_));
    }

    void ObBalanceInfo::inc_out()
    {
      atomic_inc(reinterpret_cast<uint32_t*>(&cur_out_));
    }

    void ObBalanceInfo::dec_in()
    {
      atomic_dec(reinterpret_cast<uint32_t*>(&cur_in_));
    }

    void ObBalanceInfo::dec_out()
    {
      atomic_dec(reinterpret_cast<uint32_t*>(&cur_out_));
    }

    ////////////////////////////////////////////////////////////////
    ObServerStatus::ObServerStatus()
      :cluster_id_(-1)/*add peiouya [MultiUPS] [SYS_Table_Modify] 20150507*/,last_hb_time_(0),last_hb_time_ms_(0),ms_status_(STATUS_DEAD),
      status_(STATUS_DEAD), port_cs_(0), port_ms_(0), port_ms_sql_(0), hb_retry_times_(0),
      register_time_(0), wait_restart_(false), can_restart_(false), lms_(false)
    {
    }
    void ObServerStatus::set_hb_time(int64_t hb_t)
    {
      last_hb_time_ = hb_t;
      hb_retry_times_ = 0;
    }

    void ObServerStatus::set_hb_time_ms(int64_t hb_t)
    {
      last_hb_time_ms_ = hb_t;
    }

    void ObServerStatus::set_lms(bool lms)
    {
      lms_ = lms;
    }

    bool ObServerStatus::is_alive(int64_t now, int64_t lease) const
    {
      return now - last_hb_time_ < lease;
    }

    bool ObServerStatus::is_ms_alive(int64_t now, int64_t lease) const
    {
      return now - last_hb_time_ms_ < lease;
    }

    bool ObServerStatus::is_lms() const
    {
      return lms_;
    }

    const char* ObServerStatus::get_cs_stat_str() const
    {
      const char* ret = "ERR";
      switch(status_)
      {
        case STATUS_DEAD:
          ret = "DEAD";
          break;
        case STATUS_WAITING_REPORT:
          ret = "WAIT";
          break;
        case STATUS_SERVING:
          ret = "SERV";
          break;
        case STATUS_REPORTING:
          ret = "REPORT";
          break;
        case STATUS_REPORTED:
          ret = "REPORTED";
          break;
        case STATUS_SHUTDOWN:
          ret = "SHUTDOWN";
          break;
        default:
          break;
      }
      return ret;
    }

    void ObServerStatus::dump(const int32_t index) const
    {
      //mod peiouya [MultiUPS] [SYS_Table_Modify] 20150507:b
      /*
      YYSYS_LOG(INFO, "index = %d server %s  status %d ms_status %d last_hb %ld "
          "port_cs %d port_ms %d port_ms_sql %d hb_ms=%ld register=%ld",
          index, to_cstring(server_), status_, ms_status_, last_hb_time_,
          port_cs_, port_ms_, port_ms_sql_, last_hb_time_ms_, register_time_);
      */
      YYSYS_LOG(INFO, "index = %d server %s  cluster_id %ld status %d ms_status %d last_hb %ld "
          "port_cs %d port_ms %d port_ms_sql %d hb_ms=%ld register=%ld",
          index, to_cstring(server_), cluster_id_,status_, ms_status_, last_hb_time_,
          port_cs_, port_ms_, port_ms_sql_, last_hb_time_ms_, register_time_);
      //mod 20150507:e
      disk_info_.dump();
    }

    DEFINE_SERIALIZE(ObServerStatus)
    {
      int ret = 0;
      int64_t tmp_pos = pos;
      //add peiouya [MultiUPS] [SYS_Table_Modify] 20150507:b
      /*mod :e*/
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos, cluster_id_);
      }
      //add 20150507:e
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi64(buf, buf_len, tmp_pos, last_hb_time_);
      }
      if (OB_SUCCESS == ret)
      {
        int64_t tmp_status = ms_status_;
        tmp_status <<= 32;
        tmp_status |= status_;

        ret = serialization::encode_vi64(buf, buf_len, tmp_pos, tmp_status);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi32(buf, buf_len, tmp_pos, port_cs_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi32(buf, buf_len, tmp_pos, port_ms_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::encode_vi32(buf, buf_len, tmp_pos, port_ms_sql_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = server_.serialize(buf, buf_len, tmp_pos);
      }
      if (OB_SUCCESS == ret)
      {
        ret = disk_info_.serialize(buf, buf_len, tmp_pos);
      }
      if (OB_SUCCESS == ret)
      {
        pos = tmp_pos;
      }
      return ret;
    }

    DEFINE_DESERIALIZE(ObServerStatus)
    {
      int ret = OB_SUCCESS;
      int64_t tmp_pos = pos;
      //add peiouya [MultiUPS] [SYS_Table_Modify] 20150507:b
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, &cluster_id_);;
      }
      //add 20150507:e
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, const_cast<int64_t*>(&last_hb_time_));
      }
      if (OB_SUCCESS == ret)
      {
        int64_t tmp_status = 0;
        ret = serialization::decode_vi64(buf, data_len, tmp_pos, &tmp_status);
        if (OB_SUCCESS == ret)
        {
          status_ = static_cast<EStatus>(tmp_status & 0xffffffff);
          ms_status_ = static_cast<EStatus>(tmp_status >> 32);
        }
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi32(buf, data_len, tmp_pos, &port_cs_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi32(buf, data_len, tmp_pos, &port_ms_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = serialization::decode_vi32(buf, data_len, tmp_pos, &port_ms_sql_);
      }
      if (OB_SUCCESS == ret)
      {
        ret = server_.deserialize(buf, data_len, tmp_pos);
      }
      if (OB_SUCCESS == ret)
      {
        ret = disk_info_.deserialize(buf, data_len, tmp_pos);
      }

      if (OB_SUCCESS == ret)
      {
        pos = tmp_pos;
      }
      return ret;
    }
    DEFINE_GET_SERIALIZE_SIZE(ObServerStatus)
    {
      int64_t len = 0;
      //add peiouya [MultiUPS] [SYS_Table_Modify] 20150507:b
      len += serialization::encoded_length_vi64(cluster_id_);
      //add 20150507:e
      len += serialization::encoded_length_vi64(last_hb_time_);
      int64_t tmp_status = ms_status_;
      tmp_status <<= 32;
      tmp_status |= status_;
      len += serialization::encoded_length_vi64(tmp_status);
      len += serialization::encoded_length_vi32(port_cs_);
      len += serialization::encoded_length_vi32(port_ms_);
      len += serialization::encoded_length_vi32(port_ms_sql_);
      len += server_.get_serialize_size();
      len += disk_info_.get_serialize_size();
      return len;
    }

    ObChunkServerManager::ObChunkServerManager()
    {
      servers_.init(MAX_SERVER_COUNT, data_holder_);
    }
    ObChunkServerManager::~ObChunkServerManager()
    {
    }
    ObChunkServerManager::iterator ObChunkServerManager::begin()
    {
      return servers_.get_base_address();
    }
    ObChunkServerManager::const_iterator ObChunkServerManager::begin() const
    {
      return servers_.get_base_address();
    }
    ObChunkServerManager::iterator ObChunkServerManager::end()
    {
      return servers_.get_base_address() +
        servers_.get_array_index();
    }
    ObChunkServerManager::const_iterator ObChunkServerManager::end() const
    {
      return servers_.get_base_address() +
        servers_.get_array_index();
    }
    int64_t ObChunkServerManager::size() const
    {
      return servers_.get_array_index();
    }
    ObChunkServerManager::iterator ObChunkServerManager::find_by_ip(const ObServer& server)
    {
      iterator res = end();
      for (int64_t i = 0; i < servers_.get_array_index(); i++)
      {
        if (servers_.at(i) == NULL)
        {
          YYSYS_LOG(ERROR, "never reach this, bugs");
        }
        else
        {
          //assert(servers_.at(i) != NULL);
          if (!server.compare_by_ip((servers_.at(i))->server_) && !(servers_.at(i))->server_.compare_by_ip(server))
          {
            res = servers_.at(i);
            break;
          }
        }
      }
      return res;
    }
    ObChunkServerManager::const_iterator ObChunkServerManager::find_by_ip(const ObServer& server) const
    {
      //YYSYS_LOG(DEBUG, "serid = %lu", server.get_ipv4_server_id());
      const_iterator res = end();
      for (int64_t i = 0; i < servers_.get_array_index(); i++)
      {
        if (servers_.at(i) == NULL)
        {
          YYSYS_LOG(ERROR, "never reach this, bugs");
        }
        else
        {
          //assert(servers_.at(i) != NULL);
          if (!server.compare_by_ip((servers_.at(i))->server_) && !(servers_.at(i))->server_.compare_by_ip(server))
          {
            res = servers_.at(i);
            break;
          }
        }
      }
      return res;
    }
    //ObChunkServerManager::const_iterator ObChunkServerManager::get_serving_ms() const
    ObChunkServerManager::const_iterator ObChunkServerManager::get_serving_ms(const int64_t cluster_id) const
    {
      const_iterator it = end();
      int ret = OB_ERROR;
      if (cluster_id != OB_ALL_CLUSTER_FLAG)
      {
        const_iterator local_ms_it = end();
        for (local_ms_it = begin(); end() != local_ms_it; ++local_ms_it)
        {
          if (local_ms_it->ms_status_ == ObServerStatus::STATUS_SERVING
                && local_ms_it->port_ms_ != 0
                && local_ms_it->server_.cluster_id_ == cluster_id)
          {
            YYSYS_LOG(DEBUG, "select serving ms port: [%d], server: [%s]",
              local_ms_it->port_ms_, to_cstring(local_ms_it->server_));
            it = local_ms_it;
            ret = OB_SUCCESS;
            break;
          }
        }
      }

      if ((cluster_id == OB_ALL_CLUSTER_FLAG) || (ret != OB_SUCCESS))
      {
        const_iterator all_ms_it = end();
        for (all_ms_it = begin(); end() != all_ms_it; ++all_ms_it)
        {
          if (all_ms_it->ms_status_ == ObServerStatus::STATUS_SERVING
                && all_ms_it->port_ms_ != 0)
          {
            YYSYS_LOG(DEBUG, "select serving ms port: [%d], server: [%s]",
              all_ms_it->port_ms_, to_cstring(all_ms_it->server_));
            it = all_ms_it;
            break;
          }
        }
      }

      // for (it = begin(); end() != it; ++it)
      // {
      //   if (it->ms_status_ == ObServerStatus::STATUS_SERVING
      //       && it->port_ms_ != 0)
      //   {
      //     YYSYS_LOG(DEBUG, "select serving ms port: [%d], server: [%s]",
      //         it->port_ms_, to_cstring(it->server_));
      //     break;
      //   }
      // }
      return it;
    }
    // mod zhaoqiong [MultiUPS] [MS_CS_Manage_Function] 20150528:b
    //int ObChunkServerManager::receive_hb(const common::ObServer& server, const int64_t time_stamp, const bool is_merge_server = false,
            //    const bool is_listen_ms = false, const int32_t sql_port = 0, const bool is_regist = false);
    /*
     * root server will call this when a server regist to root or echo heart beat
     * @return OB_SUCCESS; OB_CONFLICT_VALUE; OB_ARRAY_OUT_OF_RANGE
     * res:2 new server ;1 relive server; 0 heartbt
     */
    int ObChunkServerManager::receive_hb(int & res, const ObServer& server, const int64_t time_stamp,
        const bool is_merge_server, const bool is_listen_ms, const int32_t sql_port,
        const bool is_regist
        //add pangtianze [Paxos rs_election] 20170420:b
        ,const bool valid_flag
        //add:e
        )
    {
      // add zhaoqiong [MultiUPS] [MS_CS_Manage_Function] 20150528:b
      int ret = OB_SUCCESS;
      // add:e
      //add pangtianze [MultiUPS] [merge with paxos] 20170516:b
      int64_t cluster_id = server.cluster_id_;
      //add:e
      res = 0;
      iterator it = find_by_ip(server);
      if (it != end())
      {
        if (is_merge_server)
        {
          YYSYS_LOG(DEBUG, "receive hb from mergeserver, ts=%ld is_regist=%d", time_stamp, is_regist);
          it->set_hb_time_ms(time_stamp);
          if (it->ms_status_ == ObServerStatus::STATUS_DEAD || is_regist)
          {
            // add zhaoqiong [MultiUPS] [MS_CS_Manage_Function] 20150528:b
            if (it->status_ != ObServerStatus::STATUS_DEAD)
            {
              // check the cluster id is compatiable with the alive chunkserver
              if (-1 != it->cluster_id_ && it->cluster_id_ != cluster_id)
              {
                ret = OB_CONFLICT_VALUE;
                YYSYS_LOG(ERROR, "receive realive ms heartbeat, or new ms register. but its cluster id conflict.\
                               server=%s pre cluster id %ld  cur register cluster id %ld",
                               server.to_cstring(), it->cluster_id_, cluster_id);
              }
            }
            else
            {
              // update the cluster id
              it->cluster_id_ = cluster_id;
            }
            if (OB_SUCCESS == ret)
            {// add:e
              // mod zhaoqiong [MultiUPS] [MS_CS_Manage_Function] 20150528:b
              //res = 2;
              res = 1;
              // mod:e
              YYSYS_LOG(TRACE, "receive realive ms heartbeat, or new ms register. server=%s", server.to_cstring());
              it->register_time_ = time_stamp;
              it->ms_status_ = ObServerStatus::STATUS_SERVING;
              it->port_ms_ = server.get_port();
              it->port_ms_sql_ = sql_port;
              it->lms_ = is_listen_ms;
              //add pangtianze [Paxos rs_election] 20170420:b
              it->is_valid_ = valid_flag;
              //add:e
              if ((it->port_ms_sql_ != 0) && (it->port_ms_sql_ != sql_port))
              {
                YYSYS_LOG(ERROR, "check alive mergeserver port modified:old[%d], new[%d], server[%s], listener[%d]",
                    it->port_ms_sql_, sql_port, server.to_cstring(), is_listen_ms);
              }
            }
          }
        }
        else
        {
          YYSYS_LOG(DEBUG, "receive hb from chunkserver, ts=%ld is_regist=%d", time_stamp, is_regist);
          it->set_hb_time(time_stamp);
          if (it->status_ == ObServerStatus::STATUS_DEAD || is_regist)
          {
            // add zhaoqiong [MultiUPS] [MS_CS_Manage_Function] 20150528:b
            // check the cluster id is compatiable with the alive mergeserver
            if (it->ms_status_ != ObServerStatus::STATUS_DEAD)
            {
              if (-1 != it->cluster_id_ && it->cluster_id_ != cluster_id)
              {
                ret = OB_CONFLICT_VALUE;
                YYSYS_LOG(ERROR, "receive realive cs heartbeat, or new cs register. but its cluster id conflict.\
                               server=%s pre cluster id %ld  cur register cluster id %ld",
                               server.to_cstring(), it->cluster_id_, cluster_id);
              }
            }
            else
            {
              //update the cluster_id
              it->cluster_id_ = cluster_id;
            }
            if (OB_SUCCESS == ret)
            {//add:e
              // mod zhaoqiong [MultiUPS] [MS_CS_Manage_Function] 20150528:b
              //res = 2;
              res = 1;
              // mod:e
              YYSYS_LOG(TRACE, "receive realive cs heartbeat, or new cs register. server=%s", server.to_cstring());
              it->register_time_ = time_stamp;
              it->status_ = ObServerStatus::STATUS_WAITING_REPORT;
              it->server_.set_port(server.get_port());
              it->port_cs_ = server.get_port();
              //del lbzhong [Paxos Cluster.Balance] 20160707:b
              ///if lms and cs on the same, del this line and then cs cannot make lms ms.
              it->lms_ = false;
              //del:e
              //add pangtianze [Paxos rs_election] 20170420:b
              it->is_valid_ = valid_flag;
              //add:e
              if ((it->port_cs_ != 0) && (it->port_cs_ != server.get_port()))
              {
                YYSYS_LOG(ERROR, "check alive chunkserver port modified:old[%d], new[%d], server[%s]",
                    it->port_cs_, server.get_port(), server.to_cstring());
              }
            }
          }
        }
      }
      else
      {
        // new server entry
        res = 2;
        ObServerStatus tmp_server_status;
        tmp_server_status.server_ = server;
        if (is_merge_server)
        {
          YYSYS_LOG(TRACE, "receive hb from new mergeserver, server=%s ts=%ld is_lms=%d is_regist=%d",
              server.to_cstring(), time_stamp, is_listen_ms, is_regist);
          tmp_server_status.port_ms_ = server.get_port();
          tmp_server_status.port_ms_sql_ = sql_port;
          tmp_server_status.ms_status_ = ObServerStatus::STATUS_SERVING;
          tmp_server_status.set_hb_time_ms(time_stamp);
          tmp_server_status.set_lms(is_listen_ms);
          //add pangtianze [Paxos rs_election] 20170420:b
          tmp_server_status.is_valid_ = valid_flag;
          //add:e
        }
        else
        {
          YYSYS_LOG(TRACE, "receive hb from new chunkserver, server=%s ts=%ld is_regist=%d",
              server.to_cstring(), time_stamp, is_regist);
          tmp_server_status.port_cs_ = server.get_port();
          tmp_server_status.set_hb_time(time_stamp);
          tmp_server_status.set_lms(false);
          tmp_server_status.status_ = ObServerStatus::STATUS_WAITING_REPORT;
          tmp_server_status.register_time_ = time_stamp;
          //add pangtianze [Paxos rs_election] 20170420:b
          tmp_server_status.is_valid_ = valid_flag;
          //add:e
        }
        //add peiouya [MultiUPS] [MS_CS_Manage_Function] 20150512:b
        tmp_server_status.cluster_id_ = cluster_id;
        //add 20150512:e
        // mod zhaoqiong [MultiUPS] [MS_CS_Manage_Function] 20150528:b
        //res = servers_.push_back(tmp_server_status);
        if(!servers_.push_back(tmp_server_status))
        {
          ret = OB_ARRAY_OUT_OF_RANGE;
        }
        else
        {

        }
        //add:e

      }
      return ret;
    }

    int ObChunkServerManager::update_disk_info(const common::ObServer& server, const ObServerDiskInfo& disk_info)
    {
      int ret = OB_SUCCESS;
      iterator it = find_by_ip(server);
      if (it != end() )
      {
        it->disk_info_ = disk_info;
      }
      else
      {
        YYSYS_LOG(ERROR, " not find info about server %s", to_cstring(server));
        ret = OB_ERROR;
      }
      return ret;
    }
    int ObChunkServerManager::get_array_length() const
    {
      return static_cast<int32_t>(servers_.get_array_index());
    }
    ObServerStatus* ObChunkServerManager::get_server_status(const int32_t index)
    {
      ObServerStatus* ret = NULL;
      if (index < servers_.get_array_index())
      {
        ret = data_holder_ + index;
      }
      else
      {
        YYSYS_LOG(ERROR, "never should reach this, idx=%d", index);
      }
      return ret;
    }
    const ObServerStatus* ObChunkServerManager::get_server_status(const int32_t index) const
    {
      const ObServerStatus* ret = NULL;
      if (index < servers_.get_array_index())
      {
        ret = data_holder_ + index;
      }
      else
      {
        YYSYS_LOG(ERROR, "never should reach this");
      }
      return ret;
    }

    int ObChunkServerManager::get_server_index(const common::ObServer &server, int32_t &index) const
    {
      int ret = OB_SUCCESS;
      const_iterator it = find_by_ip(server);
      if (end() != it)
      {
        index = static_cast<int32_t>(it - begin());
      }
      else
      {
        index = OB_INVALID_INDEX;
        ret = OB_ENTRY_NOT_EXIST;
      }
      return ret;
    }

    int32_t ObChunkServerManager::get_alive_server_count(const bool chunkserver
                                                         //add lbzhong [Paxos Cluster.Flow.MS] 201607026:b
                                                         , const int32_t cluster_id
                                                         //add:e
                                                         ) const
    {
      int32_t count = 0;
      ObChunkServerManager::const_iterator it;
      for (it = begin(); end() != it; ++it)
      {
        if (true == chunkserver)
        {
          if (ObServerStatus::STATUS_DEAD != it->status_
              //add lbzhong [Paxos Cluster.Flow.MS] 201607026:b
              && (cluster_id == OB_ALL_CLUSTER_FLAG || cluster_id == it->server_.cluster_id_)
              //add:e
              )
          {
            ++count;
          }
        }
        else
        {
          if (ObServerStatus::STATUS_DEAD != it->ms_status_
              //add lbzhong [Paxos Cluster.Flow.MS] 201607026:b
              && (cluster_id == OB_ALL_CLUSTER_FLAG || cluster_id == it->server_.cluster_id_)
              //add:e
              )
          {
            ++count;
          }
        }
      }
      return count;
    }

    int ObChunkServerManager::get_next_alive_ms(int32_t & index, ObServer & server
                                                //add lbzhong [Paxos Cluster.Flow.MS] 201607026:b
                                                , const int32_t cluster_id
                                                //add:e
                                                ) const
    {
      int32_t count = static_cast<int32_t>(size());
      const ObServerStatus * st = NULL;
      int32_t i = 0;
      //add lbzhong [Paxos Cluster.Flow.MS] 201607026:b
      int32_t c_id = cluster_id;
      if(cluster_id >= 0 && get_alive_server_count(false, cluster_id) == 0)
      {
        c_id = OB_ALL_CLUSTER_FLAG;
      }
      //add:e
      for (i = 0; i < count; ++i)
      {
        st = get_server_status(index);
        if ((st != NULL) && (st->ms_status_ != ObServerStatus::STATUS_DEAD)
            //add lbzhong [Paxos Cluster.Flow.MS] 201607026:b
            && (c_id == OB_ALL_CLUSTER_FLAG || c_id == st->server_.cluster_id_)
            //add:e
            && st->is_valid_
            )
        {
          server = st->server_;
          server.set_port(st->port_ms_);
          break;
        }
        else
        {
          index = (index + 1) % count;
        }
      }
      return (i == count) ? OB_MS_NOT_EXIST : OB_SUCCESS;
    }

    common::ObServer ObChunkServerManager::get_cs(const int32_t index) const
    {
      ObServer server;
      const ObServerStatus* st = get_server_status(index);
      if (st != NULL)
      {
        server = st->server_;
        server.set_port(st->port_cs_);
      }
      return server;
    }

    void ObChunkServerManager::reset_balance_info()
    {
      int32_t cs_num = 0;
      ObChunkServerManager::iterator it;
      for (it = begin(); end() != it; ++it)
      {
        it->balance_info_.reset();
        if (ObServerStatus::STATUS_DEAD != it->status_)
        {
          cs_num++;
        }
      }
      yysys::CWLockGuard guard(migrate_infos_lock_);
      migrate_infos_.reset();
    }

    void ObChunkServerManager::reset_balance_info_for_table(int32_t &cs_num, int32_t &shutdown_num
                                                            //add lbzhong [Paxos Cluster.Balance] 20160705:b
                                                            , const int64_t cluster_id
                                                            )
    {
      cs_num = 0;
      shutdown_num = 0;
      ObChunkServerManager::iterator it;
      for (it = begin(); end() != it; ++it)
      {
        if (ObServerStatus::STATUS_DEAD != it->status_
            //add lbzhong [Paxos Cluster.Balance] 20160705:b
            && (cluster_id == OB_ALL_CLUSTER_FLAG || it->server_.cluster_id_ == cluster_id)
            //add:e
            )
        {
          it->balance_info_.reset_for_table();
          cs_num++;
          if (ObServerStatus::STATUS_SHUTDOWN == it->status_)
          {
            shutdown_num++;
          }
        }
      }
    }

    bool ObChunkServerManager::is_migrate_infos_full() const
    {
      yysys::CRLockGuard guard(migrate_infos_lock_);
      return migrate_infos_.is_full();
    }

    int64_t ObChunkServerManager::get_migrate_num() const
    {
      yysys::CRLockGuard guard(migrate_infos_lock_);
      return migrate_infos_.get_used_count();
    }

    int64_t ObChunkServerManager::get_max_migrate_num() const
    {
      return migrate_infos_.get_size();
    }

    void ObChunkServerManager::set_max_migrate_num(int64_t size)
    {
      yysys::CWLockGuard guard(migrate_infos_lock_);
      migrate_infos_.set_size(size);
    }

    void ObChunkServerManager::set_server_down(iterator& it)
    {
      if (it >= begin() && it < end())
      {
        if (YYSYS_LOGGER._level >= YYSYS_LOG_LEVEL_INFO)
        {
          ObServer tmp_server = it->server_;
          tmp_server.set_port(it->port_cs_);
          YYSYS_LOG(INFO, "chunkserver %s is down", to_cstring(tmp_server));
        }
        it->status_ = ObServerStatus::STATUS_DEAD;
        //it->balance_info_.reset();
      }
      return ;
    }

    void ObChunkServerManager::set_server_down_ms(iterator& it)
    {
      if (it >= begin() && it < end())
      {
        if (YYSYS_LOGGER._level >= YYSYS_LOG_LEVEL_INFO)
        {
          ObServer tmp_server = it->server_;
          tmp_server.set_port(it->port_ms_);
          YYSYS_LOG(INFO, "mergeserver %s is down", to_cstring(tmp_server));
        }
        it->ms_status_ = ObServerStatus::STATUS_DEAD;
      }
      return ;
    }

    void ObChunkServerManager::cancel_restart_all_cs()
    {
      for(iterator it = begin(); it != end(); ++it)
      {
        it->wait_restart_ = false;
      }
    }

    void ObChunkServerManager::restart_all_cs()
    {
      for(iterator it = begin(); it != end(); ++it)
      {
        it->wait_restart_ = true;
      }
    }

    int ObChunkServerManager::shutdown_cs(const common::ObArray<common::ObServer> &servers, enum ShutdownOperation op)
    {
      int ret = OB_SUCCESS;
      ObServer server;
      ObChunkServerManager::iterator it;
      for (int i = 0; i < servers.count(); ++i)
      {
        if (OB_SUCCESS != (ret = servers.at(i, server)))
        {
          YYSYS_LOG(ERROR, "fatal error=%d", ret);
          break;
        }
        else
        {
          if (end() == (it = this->find_by_ip(server)))
          {
            YYSYS_LOG(WARN, "server not exist, addr=%s", server.to_cstring());
            ret = OB_ENTRY_NOT_EXIST;
            break;
          }
          else if (ObServerStatus::STATUS_SHUTDOWN == it->status_)
          {
            YYSYS_LOG(INFO, "already shutdown, addr=%s", it->server_.to_cstring());
            continue;
          }
          else if (ObServerStatus::STATUS_SERVING != it->status_)
          {
            YYSYS_LOG(WARN, "server not serving, addr=%s stat=%d", server.to_cstring(), it->status_);
            ret = OB_ENTRY_NOT_EXIST;
            break;
          }
          else
          {
            if(SHUTDOWN == op)
            {
              YYSYS_LOG(INFO, "shutdown server=%s", it->server_.to_cstring());
              it->status_ = ObServerStatus::STATUS_SHUTDOWN;
            }
            else if(RESTART == op)
            {
              YYSYS_LOG(INFO, "restart server=%s", it->server_.to_cstring());
              it->wait_restart_ = true;
            }
            else
            {
              ret = OB_NOT_SUPPORTED;
              YYSYS_LOG(WARN, "not supported shutdown operation[%d]", op);
            }
          }
        }
      } // end for
      return ret;
    }

    int ObChunkServerManager::cancel_shutdown_cs(const common::ObArray<common::ObServer> &servers, enum ShutdownOperation op)
    {
      int ret = OB_SUCCESS;
      ObServer server;
      ObChunkServerManager::iterator it;
      for (int i = 0; i < servers.count(); ++i)
      {
        if (OB_SUCCESS != (ret = servers.at(i, server)))
        {
          YYSYS_LOG(ERROR, "fatal error=%d", ret);
          break;
        }
        else
        {
          if (end() == (it = this->find_by_ip(server)))
          {
            YYSYS_LOG(WARN, "server not exist, addr=%s", server.to_cstring());
            ret = OB_ENTRY_NOT_EXIST;
            break;
          }
          else if (ObServerStatus::STATUS_SHUTDOWN == it->status_)
          {
            switch(op)
            {
              case SHUTDOWN:
                YYSYS_LOG(INFO, "cancel shutting down server=%s", it->server_.to_cstring());
                it->status_ = ObServerStatus::STATUS_SERVING;
                break;
              case RESTART:
              default:
                YYSYS_LOG(INFO, "cancel restart server=%s", it->server_.to_cstring());
                it->wait_restart_ = false;
                break;
            }
          }
          else
          {
            YYSYS_LOG(INFO, "not shutting down, addr=%s", it->server_.to_cstring());
          }
        }
      } // end for
      return ret;
    }

    bool ObChunkServerManager::has_shutting_down_server() const
    {
      bool ret = false;
      for (const_iterator it = begin(); end() != it; ++it)
      {
        if (ObServerStatus::STATUS_SHUTDOWN == it->status_)
        {
          ret = true;
          break;
        }
      }
      return ret;
    }

    ObChunkServerManager& ObChunkServerManager::operator= (const ObChunkServerManager& other)
    {
      if (this != &other)
      {
        memcpy(data_holder_, other.data_holder_, sizeof(other.data_holder_));
        servers_ = other.servers_;
      }
      return *this;
    }

    int ObChunkServerManager::write_to_file(const char* filename)
    {
      int ret = OB_SUCCESS;
      if (filename == NULL)
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(INFO, "file name can not be NULL");
      }

      if (ret == OB_SUCCESS)
      {
        int64_t total_size = 0;
        int64_t total_count = servers_.get_array_index();

        total_size += serialization::encoded_length_vi64(total_count);
        for(int64_t i=0; i<total_count; ++i)
        {
          total_size += data_holder_[i].get_serialize_size();
        }

        char* data_buffer = static_cast<char*>(ob_malloc(total_size, ObModIds::OB_RS_SERVER_MANAGER));
        if (data_buffer == NULL)
        {
          ret = OB_ERROR;
          YYSYS_LOG(ERROR, "allocate memory failed");
        }

        if (ret == OB_SUCCESS)
        {
          common::ObDataBuffer buffer(data_buffer, total_size);
          ret = serialization::encode_vi64(buffer.get_data(), buffer.get_capacity(), buffer.get_position(), total_count);

          if (ret == OB_SUCCESS)
          {
            for(int64_t i=0; i<total_count; ++i)
            {
              ret = data_holder_[i].serialize(buffer.get_data(), buffer.get_capacity(), buffer.get_position());
              if (ret != OB_SUCCESS) break;
            }
          }
        }

        int64_t header_length = sizeof(common::ObRecordHeader);
        int64_t pos = 0;
        char header_buffer[header_length];
        if (ret == OB_SUCCESS)
        {
          common::ObRecordHeader header;

          header.set_magic_num(CHUNK_SERVER_MAGIC);
          header.header_length_ = static_cast<int16_t>(header_length);
          header.version_ = 0;
          header.reserved_ = 0;

          header.data_length_ = static_cast<int32_t>(total_size);
          header.data_zlength_ = static_cast<int32_t>(total_size);

          header.data_checksum_ = common::ob_crc64(data_buffer, total_size);
          header.set_header_checksum();

          ret = header.serialize(header_buffer, header_length, pos);
          if (ret != OB_SUCCESS)
          {
            YYSYS_LOG(ERROR, "serialization file header failed");
          }
        }

        if (ret == OB_SUCCESS)
        {
          common::FileUtils fu;
          int32_t rc = fu.open(filename, O_CREAT | O_WRONLY | O_TRUNC, 0644);
          if (rc < 0)
          {
            ret = OB_ERROR;
            YYSYS_LOG(ERROR, "create file [%s] failed", filename);
          }

          if (ret == OB_SUCCESS)
          {
            int64_t wl = fu.write(header_buffer, header_length);
            if (wl != header_length)
            {
              ret = OB_ERROR;
              YYSYS_LOG(ERROR, "write header into [%s] failed", filename);
            }
          }

          if (ret == OB_SUCCESS)
          {
            int64_t wl = fu.write(data_buffer, total_size);
            if (wl != total_size)
            {
              ret = OB_ERROR;
              YYSYS_LOG(ERROR, "write data info [%s] failed", filename);
            }
            else
            {
              YYSYS_LOG(DEBUG, "chunkserver list has been write into [%s]", filename);
            }
          }

          fu.close();

        }
        if (data_buffer != NULL)
        {
          ob_free(data_buffer);
          data_buffer = NULL;
        }
      }

      return ret;
    }

    int ObChunkServerManager::read_from_file(const char* filename, int32_t &cs_num, int32_t &ms_num)
    {
      int ret = OB_SUCCESS;
      cs_num = 0;
      ms_num = 0;

      if (filename == NULL)
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(INFO, "filename can not be NULL");
      }

      common::FileUtils fu;
      if (ret == OB_SUCCESS)
      {
        int32_t rc = fu.open(filename, O_RDONLY);
        if (rc < 0)
        {
          ret = OB_ERROR;
          YYSYS_LOG(ERROR, "open file [%s] failed", filename);
        }
      }

      int64_t header_length = sizeof(common::ObRecordHeader);
      common::ObRecordHeader header;
      if (ret == OB_SUCCESS)
      {
        char header_buffer[header_length];
        int64_t rl = fu.read(header_buffer, header_length);
        if (rl != header_length)
        {
          ret = OB_ERROR;
          YYSYS_LOG(ERROR, "read header from [%s] failed", filename);
        }

        if (ret == OB_SUCCESS)
        {
          int64_t pos = 0;
          ret = header.deserialize(header_buffer, header_length, pos);
        }
        if (ret == OB_SUCCESS)
        {
          ret = header.check_header_checksum();
        }
      }

      char* data_buffer = NULL;
      int64_t size = 0;
      if (ret == OB_SUCCESS)
      {
        size = header.data_length_;
        data_buffer = static_cast<char*>(ob_malloc(size, ObModIds::OB_RS_SERVER_MANAGER));
        if (data_buffer == NULL)
        {
          ret = OB_ERROR;
          YYSYS_LOG(ERROR, "allocate memory failed");
        }
      }

      if (ret == OB_SUCCESS)
      {
        int64_t rl = fu.read(data_buffer, size);
        if (rl != size)
        {
          ret = OB_ERROR;
          YYSYS_LOG(ERROR, "read data from file [%s] failed", filename);
        }
      }

      if (ret == OB_SUCCESS)
      {
        int cr = common::ObRecordHeader::check_record(header, data_buffer, size, CHUNK_SERVER_MAGIC);
        if (cr != OB_SUCCESS)
        {
          ret = OB_DESERIALIZE_ERROR;
          YYSYS_LOG(ERROR, "data check failed");
        }
      }

      int64_t count = 0;
      common::ObDataBuffer buffer(data_buffer, size);
      if (ret == OB_SUCCESS)
      {
        ret = serialization::decode_vi64(buffer.get_data(), buffer.get_capacity(), buffer.get_position(), &count);
      }

      if (ret == OB_SUCCESS)
      {
        ObServerStatus server;
        servers_.init(MAX_SERVER_COUNT, data_holder_);

        for(int64_t i=0; i<count; ++i)
        {
          ret = server.deserialize(buffer.get_data(), buffer.get_capacity(), buffer.get_position());
          if (ret == OB_SUCCESS)
          {
            if (ObServerStatus::STATUS_DEAD != server.status_
                && 0 < server.port_cs_)
            {
              cs_num++;
            }
            if (ObServerStatus::STATUS_DEAD != server.ms_status_
                && 0 < server.port_ms_)
            {
              ms_num++;
            }
            servers_.push_back(server);
          }
          else
          {
            YYSYS_LOG(ERROR, "record deserialize failed");
            break;
          }
        }
      }

      fu.close();
      if (data_buffer != NULL)
      {
        ob_free(data_buffer);
        data_buffer = NULL;
      }

      return ret;
    }

    DEFINE_SERIALIZE(ObChunkServerManager)
    {
      int ret = OB_SUCCESS;
      int64_t tmp_pos = pos;
      int64_t size = servers_.get_array_index();
      ret = serialization::encode_vi64(buf, buf_len, tmp_pos, size);
      if (OB_SUCCESS == ret)
      {

        for (int64_t i = 0; i < size; ++i)
        {
          ret = data_holder_[i].serialize(buf, buf_len, tmp_pos);
          if (ret != OB_SUCCESS)
            break;
        }
      }
      if (OB_SUCCESS == ret)
      {
        pos = tmp_pos;
      }
      return ret;
    }
    DEFINE_DESERIALIZE(ObChunkServerManager)
    {
      int ret = OB_ERROR;
      int64_t size = 0;
      ret = serialization::decode_vi64(buf, data_len, pos, &size);
      if (ret == OB_SUCCESS )
      {
        for (int64_t i = 0; i < size; ++i)
        {
          ObServerStatus server_status;
          ret = server_status.deserialize(buf, data_len, pos);
          if (ret != OB_SUCCESS)
          {
            break;
          }
          servers_.push_back(server_status);
        }
      }
      return ret;
    }
    DEFINE_GET_SERIALIZE_SIZE(ObChunkServerManager)
    {
      int64_t total_size = 0;
      int64_t size = servers_.get_array_index();
      total_size += serialization::encoded_length_vi64(size);
      for (int64_t i = 0; i < size; ++i)
      {
        total_size += data_holder_[i].get_serialize_size();
      }

      return total_size;
    }

    int ObChunkServerManager::serialize_cs(const ObServerStatus *it, char* buf, const int64_t buf_len, int64_t& pos) const
    {
      int ret = OB_SUCCESS;
      ObServer addr = it->server_;
      addr.set_port(it->port_cs_);
      int64_t reserve = 0;
      if (OB_SUCCESS != (ret = addr.serialize(buf, buf_len, pos)))
      {
        YYSYS_LOG(WARN, "serialize error");
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, reserve)))
      {
        YYSYS_LOG(WARN, "serialize error");
      }
      return ret;
    }

    int ObChunkServerManager::serialize_ms(const ObServerStatus *it, char* buf, const int64_t buf_len, int64_t& pos) const
    {
      int ret = OB_SUCCESS;
      ObServer addr = it->server_;
      addr.set_port(it->port_ms_);
      int64_t reserve = 0;
      //add pangtianze [Paxos rs_switch] 20170706:b
      reserve = it->port_ms_sql_;
      //add:e
      if (OB_SUCCESS != (ret = addr.serialize(buf, buf_len, pos)))
      {
        YYSYS_LOG(WARN, "serialize error");
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, reserve)))
      {
        YYSYS_LOG(WARN, "serialize error");
      }
      return ret;
    }

    int ObChunkServerManager::serialize_cs_list(char* buf, const int64_t buf_len, int64_t& pos) const
    {
      int ret = OB_SUCCESS;
      int32_t cs_num = 0;
      const ObServerStatus* it = NULL;
      for (it = begin(); it != end(); ++it)
      {
        if (ObServerStatus::STATUS_DEAD != it->status_)
        {
          cs_num++;
        }
      }
      if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, pos, cs_num)))
      {
        YYSYS_LOG(WARN, "serialize error");
      }
      else
      {
        int i = 0;
        for (it = begin(); it != end() && i < cs_num; ++it)
        {
          if (ObServerStatus::STATUS_DEAD != it->status_)
          {
            if (OB_SUCCESS != (ret = serialize_cs(it, buf, buf_len, pos)))
            {
              break;
            }
            else
            {
              ++i;
            }
          }
        }
      }
      return ret;
    }

    int ObChunkServerManager::serialize_cs_info_list(char* buf, const int64_t buf_len, int64_t& pos) const
    {
      int ret = OB_SUCCESS;
      int32_t cs_num = 0;
      const ObServerStatus* it = NULL;
      for (it = begin(); it != end(); ++it)
      {
        if (ObServerStatus::STATUS_DEAD != it->status_)
        {
          cs_num++;
        }
      }
      if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, pos, cs_num)))
      {
        YYSYS_LOG(WARN, "serialize error");
      }
      else
      {
        int i = 0;
        for (it = begin(); it != end() && i < cs_num; ++it)
        {
          if (ObServerStatus::STATUS_DEAD != it->status_)
          {
            if (OB_SUCCESS != (ret = serialize_cs_info(it, buf, buf_len, pos)))
            {
              break;
            }
            else
            {
              ++i;
            }
          }
        }
      }
      return ret;
    }

    int ObChunkServerManager::serialize_cs_info(const ObServerStatus *it, char* buf, const int64_t buf_len, int64_t& pos) const
    {
      int ret = OB_SUCCESS;
      ObServer addr = it->server_;
      addr.set_port(it->port_cs_);
      int32_t index = OB_INVALID_INDEX;
      get_server_index(addr, index);
      if (OB_SUCCESS != (ret = addr.serialize(buf, buf_len, pos)))
      {
        YYSYS_LOG(WARN, "serialize error");
      }
      else if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, pos, index)))
      {
        YYSYS_LOG(WARN, "serialize error");
      }
      return ret;
    }


    // mod zhaoqiong [MultiUPS] [MS_Manage_Function] 20150611:b
    //int ObChunkServerManager::serialize_ms_list(char* buf, const int64_t buf_len, int64_t& pos) const
    int ObChunkServerManager::serialize_ms_list(char* buf, const int64_t buf_len, int64_t& pos, int32_t ms_cluster
                                                //add lbzhong [Paxos Cluster.Flow.MS] 201607027:b
                                                , const int32_t cluster_id
                                                //add:e
    ) const
    //mod:e
    {
      int ret = OB_SUCCESS;
      int32_t ms_num = 0;
      const ObServerStatus* it = NULL;
      //add lbzhong [Paxos Cluster.Flow.MS] 201607027:b
      int32_t c_id = cluster_id;
      if(cluster_id >= 0 && get_alive_ms_count(cluster_id) == 0)
      {
        c_id = OB_ALL_CLUSTER_FLAG; //all ms
      }
      //add:e
      for (it = begin(); it != end(); ++it)
      {
        // mod zhaoqiong [MultiUPS] [MS_Manage_Function] 20150611:b
        //if (ObServerStatus::STATUS_DEAD != it->ms_status_
        //&& !it->lms_)
        if (ObServerStatus::STATUS_DEAD != it->ms_status_
            && !it->lms_ && (it->cluster_id_ == ms_cluster ||  OB_ALL_CLUSTER_FLAG == ms_cluster)
            //add lbzhong [Paxos Cluster.Flow.MS] 201607027:b
            && (c_id == OB_ALL_CLUSTER_FLAG || c_id == it->server_.cluster_id_)//adde:e
           )
        //mod:e
        {
          ms_num++;
        }
      }
      if (OB_SUCCESS != (ret = serialization::encode_vi32(buf, buf_len, pos, ms_num)))
      {
        YYSYS_LOG(WARN, "serialize error");
      }
      else
      {
        int i = 0;
        for (it = begin(); it != end() && i < ms_num; ++it)
        {
          // mod zhaoqiong [MultiUPS] [MS_Manage_Function] 20150611:b
          //if (ObServerStatus::STATUS_DEAD != it->ms_status_
          //&& !it->lms_)
          if (ObServerStatus::STATUS_DEAD != it->ms_status_
              && !it->lms_ && (it->cluster_id_ == ms_cluster || OB_ALL_CLUSTER_FLAG == ms_cluster)
                            //add lbzhong [Paxos Cluster.Flow.MS] 201607027:b
              && (c_id == OB_ALL_CLUSTER_FLAG || c_id == it->server_.cluster_id_)
              //add:e
              )
          //mod:e
          {
            if (OB_SUCCESS != (ret = serialize_ms(it, buf, buf_len, pos)))
            {
              break;
            }
            else
            {
              ++i;
            }
          }
        }
      }
      return ret;
    }
    int ObChunkServerManager::get_ms_port(const ObServer& server, int32_t &port)const
    {
      int ret = OB_ERROR;
      for (int64_t i = 0; i < servers_.get_array_index(); i++)
      {
        if (servers_.at(i) == NULL)
        {
          YYSYS_LOG(ERROR, "never reach this, bugs");
        }
        else
        {
          if (!server.compare_by_ip((servers_.at(i))->server_) && !(servers_.at(i))->server_.compare_by_ip(server))
          {
            ret = OB_SUCCESS;
            port = servers_.at(i)->port_ms_;
            break;
          }
        }
      }
      return ret;
    }

    int ObChunkServerManager::add_migrate_info(const common::ObServer& src_server,
        const common::ObServer& dest_server, const ObDataSourceDesc& data_source_desc)
    {
      yysys::CWLockGuard guard(migrate_infos_lock_);
      return migrate_infos_.add_migrate_info(src_server, dest_server, data_source_desc);
    }

    int ObChunkServerManager::free_migrate_info(const common::ObNewRange& range, const common::ObServer& src_server,
        const common::ObServer& dest_server)
    {
      yysys::CWLockGuard guard(migrate_infos_lock_);
      return migrate_infos_.free_migrate_info(range, src_server, dest_server);
    }

    bool ObChunkServerManager::check_migrate_info_timeout(int64_t timeout, common::ObServer& src_server,
            common::ObServer& dest_server, common::ObDataSourceDesc::ObDataSourceType& type)
    {
      yysys::CWLockGuard guard(migrate_infos_lock_);
      return migrate_infos_.check_migrate_info_timeout(timeout, src_server, dest_server, type);
    }

    void ObChunkServerManager::print_migrate_info()
    {
      yysys::CWLockGuard guard(migrate_infos_lock_);
      migrate_infos_.print_migrate_info();
    }

    //add bingo [Paxos Cluster.Balance] 20161118:b
    void ObChunkServerManager::is_cluster_alive_with_cs(bool* is_cluster_alive_with_cs) const
    {
      const ObServerStatus* it = NULL;
      for (it = begin(); it != end(); ++it)
      {
        if (ObServerStatus::STATUS_DEAD != it->status_
            && !is_cluster_alive_with_cs[it->server_.cluster_id_]
            ) // cs
        {
          is_cluster_alive_with_cs[it->server_.cluster_id_] = true;
        }
      }
    }
    //add:e

    //add lbzhong [Paxos Cluster.Balance] 20160706:b
    void ObChunkServerManager::is_cluster_alive_with_ms_cs(bool* is_cluster_alive) const
    {
      OB_ASSERT(NULL != is_cluster_alive);
      const ObServerStatus* it = NULL;
      for (it = begin(); it != end(); ++it)
      {
        if (ObServerStatus::STATUS_DEAD != it->status_
            && !is_cluster_alive[it->server_.cluster_id_]
            ) // cs
        {
          is_cluster_alive[it->server_.cluster_id_] = true;
        }
        if (ObServerStatus::STATUS_DEAD != it->ms_status_
            && !is_cluster_alive[it->server_.cluster_id_]
            ) // ms
        {
          is_cluster_alive[it->server_.cluster_id_] = true;
        }
      }
    }

    //add bingo [Paxos Cluster.Balance] 20170407:b
    void ObChunkServerManager::is_cluster_alive_with_ms_and_cs(bool* is_cluster_alive) const
    {
      OB_ASSERT(NULL != is_cluster_alive);
      for(int32_t cluster_id = 0; cluster_id < OB_CLUSTER_ARRAY_LEN; cluster_id++)
      {
        const ObServerStatus* it = NULL;
        bool cs_alive = false;
        bool ms_alive = false;
        for (it = begin(); it != end(); ++it)
        {
          if (ObServerStatus::STATUS_DEAD != it->status_
            && !it->is_lms()
            && it->server_.cluster_id_ == cluster_id
            ) 
          {
            cs_alive = true;
          }
          if (ObServerStatus::STATUS_DEAD != it->ms_status_
            && !it->is_lms()
            && it->server_.cluster_id_ == cluster_id
            ) 
          {
            ms_alive = true;
          }
        }
        if (cs_alive && ms_alive && !is_cluster_alive[cluster_id])
        {
          is_cluster_alive[cluster_id] = true;
        }
      }
    }
    //add:e

    //add bingo [Paxos Cluster.Balance] 20170410:b
//  bool ObChunkServerManager::is_cluster_alive_with_ms_and_cs(const int32_t cluster_id) const
//    {
//      const ObServerStatus* it = NULL;
//      for (it = begin(); it != end(); ++it)
//      {
//        if (ObServerStatus::STATUS_DEAD != it->status_
//            && ObServerStatus::STATUS_DEAD != it->ms_status_
//            && it->server_.cluster_id_ == cluster_id
//            ) // ms and cs
//        {
//          return true;
//        }
//      }
//      return false;
//    }
    //add:e

    //add bingo [Paxos Cluster.Balance] 20170410:b
    bool ObChunkServerManager::is_cluster_alive_with_ms_cs(const int32_t cluster_id) const
    {
      const ObServerStatus* it = NULL;
      for (it = begin(); it != end(); ++it)
      {
        if (ObServerStatus::STATUS_DEAD != it->status_
            && it->server_.cluster_id_ == cluster_id
            ) // cs
        {
          return true;
        }
        if (ObServerStatus::STATUS_DEAD != it->ms_status_
            && it->server_.cluster_id_ == cluster_id
            ) // ms
        {
          return true;
        }
      }
      return false;
    }
    //add:e

    bool ObChunkServerManager::is_cluster_alive_with_cs(const int32_t cluster_id) const
    {
      const ObServerStatus* it = NULL;
      for (it = begin(); it != end(); ++it)
      {
        if (ObServerStatus::STATUS_DEAD != it->status_
            && it->server_.cluster_id_ == cluster_id
            ) // cs
        {
          return true;
        }
      }
      return false;
    }

    //add bingo [Paxos Cluster.Balance] 20170407:b
    bool ObChunkServerManager::is_cluster_alive_with_ms(const int32_t cluster_id) const
    {
      const ObServerStatus* it = NULL;
      for (it = begin(); it != end(); ++it)
      {
        if (ObServerStatus::STATUS_DEAD != it->ms_status_
            && it->server_.cluster_id_ == cluster_id
            && !it->is_lms()) // except lms
        {
          return true;
        }
      }
      return false;
    }
    //add:e

    int32_t ObChunkServerManager::get_alive_ms_count(const int32_t cluster_id) const
    {
      int32_t ms_num = 0;
      const ObServerStatus* it = NULL;
      for (it = begin(); it != end(); ++it)
      {
        if (ObServerStatus::STATUS_DEAD != it->ms_status_
            && !it->lms_ && (cluster_id == OB_ALL_CLUSTER_FLAG || cluster_id == it->server_.cluster_id_))
        {
          ms_num++;
        }
      }
      return ms_num;
    }
    //add:e
    //add pangtianze [Paxos inner table revise] 20170826
    void ObChunkServerManager::get_task_array_for_inner_table_revise(
            ObSEArray<ObRootAsyncTaskQueue::ObSeqTask, MAX_SERVER_COUNT>& task_arr)
    {
      const_iterator it;
      for (it = begin(); end() != it; ++it)
      {
        if (!it->is_lms() && it->ms_status_ != ObServerStatus::STATUS_DEAD && it->is_valid_)
        {
          ObRootAsyncTaskQueue::ObSeqTask task;
          task.type_ = SERVER_ONLINE;
          task.role_ = OB_MERGESERVER;
          task.server_ = it->server_;
          task.inner_port_ = it->port_ms_;
          task.server_.set_port(it->port_ms_sql_);
          task_arr.push_back(task);

        }
        if (it->status_ != ObServerStatus::STATUS_DEAD && it->is_valid_)
        {
          ObRootAsyncTaskQueue::ObSeqTask task;
          task.type_ = SERVER_ONLINE;
          task.role_ = OB_CHUNKSERVER;
          task.server_ = it->server_;
          task.server_.set_port(it->port_cs_);
          task.inner_port_ = 0;
          task_arr.push_back(task);
        }
      }
    }
    //add:e
  }
}
