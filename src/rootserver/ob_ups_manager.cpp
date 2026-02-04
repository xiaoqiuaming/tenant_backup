/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_manager.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_ups_manager.h"
#include "common/ob_define.h"
#include "common/ob_array.h"
#include <yysys.h>
#include "ob_root_worker.h"
#include "ob_root_async_task_queue.h"

//add hongchen [RS_SWITCH_MASTER_BUGFIX] 20170726:b
#include "common/ob_role_mgr.h"
//add hongchen [RS_SWITCH_MASTER_BUGFIX] 20170726:e

using namespace oceanbase::rootserver;
using namespace oceanbase::common;

void ObUps::reset()
{
  addr_.reset();
  inner_port_ = 0;
  stat_ = UPS_STAT_OFFLINE;
  log_seq_num_ = 0;
  lease_ = 0;
  ms_read_percentage_ = 0;
  cs_read_percentage_ = 0;
  did_renew_received_ = false;
  cluster_id_ = -1;
  paxos_id_ = -1;
  //mod peiouya [MultiUPS] [UPS_RESTART_BUG_FIX] 20150703:b
  //last_frozen_version_ = 0;  //add peiouya [MultiUPS] [UPS_Manage_Function] 20150521
  last_frozen_version_ = OB_INVALID_VERSION;
  //mod 20150703:e
  is_active_ = false;         //add peiouya [MultiUPS] [UPS_Manage_Function] 20150527
  //add liuzy [MultiUPS] [add_paxos_interface] 20160119:b
  offline_version_ = OB_DEFAULT_OFFLINE_VERSION;
  //add 20160119:e
  //add pangtianze [Paxos ups_replication] 20160926:b
  log_term_ = OB_INVALID_TERM;
  quorum_scale_ = 0;
  //add:e
}

//add peiouya [MultiUPS] [UPS_Manage_Function] 20150527:b
void ObUps::reset_except_addr_and_ipt()
{
  stat_ = UPS_STAT_OFFLINE;
  log_seq_num_ = 0;
  lease_ = 0;
  ms_read_percentage_ = 0;
  cs_read_percentage_ = 0;
  did_renew_received_ = false;
  cluster_id_ = -1;
  paxos_id_ = -1;
  //mod peiouya [MultiUPS] [UPS_RESTART_BUG_FIX] 20150703:b
  //last_frozen_version_ = 0;
  last_frozen_version_ = OB_INVALID_VERSION;
  //mod 20150703:e
  is_active_ = false;
}
//add 20150527:e


/* // uncertainty  ups �и�д
ObUpsManager::ObUpsManager(ObRootRpcStub &rpc_stub, ObRootWorker *worker,
                           const int64_t &revoke_rpc_timeout_us,
                           int64_t lease_duration, int64_t lease_reserved_us,
                           int64_t waiting_ups_register_duration,
                           const ObiRole &obi_role,
                           const volatile int64_t& schema_version,
                           const volatile int64_t &config_version)
  :queue_(NULL), rpc_stub_(rpc_stub), worker_(worker), obi_role_(obi_role), revoke_rpc_timeout_us_(revoke_rpc_timeout_us),
   lease_duration_us_(lease_duration), lease_reserved_us_(lease_reserved_us),
   ups_master_idx_(-1), waiting_ups_register_duration_(waiting_ups_register_duration),
   waiting_ups_finish_time_(0), schema_version_(schema_version), config_version_(config_version),
   master_master_ups_read_percentage_(-1), slave_master_ups_read_percentage_(-1),
   is_flow_control_by_ip_(false)
{
}

ObUpsManager::~ObUpsManager()
{
}
*/
/*     //ucnertainty ups ��д
void ObUpsManager::set_async_queue(ObRootAsyncTaskQueue * queue)
{
  queue_ = queue;
}
*/
// del lqc [multiups with paxos] 20170610
//int ObUpsManager::find_ups_index(const ObServer &addr) const
//{
//  int ret = -1;
//  for (int32_t i = 0; i < MAX_UPS_COUNT; ++i)
//  {
//    if (ups_array_[i].addr_ == addr && UPS_STAT_OFFLINE != ups_array_[i].stat_)
//    {
//      ret = i;
//      break;
//    }
//  }
//  return ret;
//}
//
//add peiouya [Get_masterups_and_timestamp] 20141017:b
//int64_t ObUpsManager::get_ups_set_time(const common::ObServer &addr)
//{
//    int64_t ret = -1;
//    for (int32_t i = 0; i < MAX_UPS_COUNT; ++i)
//    {
//        if (ups_array_[i].addr_ == addr && UPS_STAT_OFFLINE != ups_array_[i].stat_)
//        {
//            ret = ups_mater_timestamp[i];
//            break;
//        }
//    }
//    return ret;
//}
//add 20141017:e
//del e

/*    //uncertainty  ups ����ȥ��
bool ObUpsManager::did_ups_exist(const ObServer &addr) const
{
  return -1 != find_ups_index(addr);
}

bool ObUpsManager::is_ups_master(const ObServer &addr) const
{
  bool ret = false;
  int i = find_ups_index(addr);
  if (-1 != i)
  {
    ret = (UPS_STAT_MASTER == ups_array_[i].stat_);
  }
  return ret;
}

*/
/*   // uncertainty   ups ��д
int ObUpsManager::register_ups(const ObServer &addr, int32_t inner_port, int64_t log_seq_num, int64_t lease,
    const char *server_version)
{
  int ret = OB_SUCCESS;
  yysys::CThreadGuard guard(&ups_array_mutex_);
  if (did_ups_exist(addr))
  {
    YYSYS_LOG(DEBUG, "the ups already registered, ups=%s", addr.to_cstring());
    ret = OB_ALREADY_REGISTERED;
  }
  else
  {
    ret = OB_SIZE_OVERFLOW;
    for (int32_t i = 0; i < MAX_UPS_COUNT; ++i)
    {
      if (UPS_STAT_OFFLINE == ups_array_[i].stat_)
      {
        int64_t now = yysys::CTimeUtil::getTime();
        if (0 == waiting_ups_finish_time_)
        {
          // first ups register, we will waiting for some time before select the master
          waiting_ups_finish_time_ = now + waiting_ups_register_duration_;
          YYSYS_LOG(INFO, "first ups register, waiting_finish=%ld duration=%ld",
              waiting_ups_finish_time_, waiting_ups_register_duration_);
        }
        ups_array_[i].addr_ = addr;
        ups_array_[i].inner_port_ = inner_port;
        ups_array_[i].log_seq_num_ = log_seq_num;
        ups_array_[i].lease_ = now + lease_duration_us_;
        ups_array_[i].did_renew_received_ = true;
        ups_array_[i].cs_read_percentage_ = 0;
        ups_array_[i].ms_read_percentage_ = 0;
        ObUps ups(ups_array_[i]);
        refresh_inner_table(SERVER_ONLINE, ups, server_version);
        // has valid lease
        if (lease > now)
        {
          if (has_master())
          {
            YYSYS_LOG(WARN, "ups claimed to have the master lease but we ignore, addr=%s lease=%ld master=%s",
                addr.to_cstring(), lease, to_cstring(ups_array_[ups_master_idx_].addr_));
            ret = OB_CONFLICT_VALUE;
          }
          else
          {
            change_ups_stat(i, UPS_STAT_MASTER);
            ups_master_idx_ = i;
            YYSYS_LOG(WARN, "ups claimed to have the master lease, addr=%s lease=%ld",
                addr.to_cstring(), lease);
            ObUps ups(ups_array_[ups_master_idx_]);
            refresh_inner_table(ROLE_CHANGE, ups, "null");
            // master selected
            waiting_ups_finish_time_ = -1;
            ret = OB_SUCCESS;
          }
        }
        else
        {
          change_ups_stat(i, UPS_STAT_NOTSYNC);
          ret = OB_SUCCESS;
        }
        YYSYS_LOG(INFO, "ups register, addr=%s inner_port=%d lsn=%ld lease=%ld",
            addr.to_cstring(), inner_port, log_seq_num, lease);
        reset_ups_read_percent();
        break;
      }
    }
  }
  return ret;
}
*/
/*  // uncertainty   ups ��д
int ObUpsManager::renew_lease(const common::ObServer &addr, ObUpsStatus stat, const ObiRole &obi_role)
{
  int ret = OB_SUCCESS;
  yysys::CThreadGuard guard(&ups_array_mutex_);
  int i = -1;
  if (-1 == (i = find_ups_index(addr)))
  {
    YYSYS_LOG(WARN, "not registered ups, addr=%s", addr.to_cstring());
  }
  else
  {
    ups_array_[i].did_renew_received_ = true;
    YYSYS_LOG(DEBUG, "renew lease, addr=%s stat=%d", addr.to_cstring(), stat);
    if ((UPS_STAT_SYNC == stat || UPS_STAT_NOTSYNC == stat)
        && (UPS_STAT_SYNC == ups_array_[i].stat_ || UPS_STAT_NOTSYNC == ups_array_[i].stat_))
    {
      if (ups_array_[i].stat_ != stat)
      {
        change_ups_stat(i, stat);
        if (!is_flow_control_by_ip_)
        {
          reset_ups_read_percent();
        }
      }
    }
    if (ObiRole::INIT != obi_role.get_role())
    {
      ups_array_[i].obi_role_ = obi_role;
    }
    else
    {
      YYSYS_LOG(WARN, "ups's obi role is INIT, ups=%s", addr.to_cstring());
    }
  }
  return ret;
}
*/
/*  //uncertainty ups��д
int ObUpsManager::refresh_inner_table(const ObTaskType type, const ObUps & ups, const char *server_version)
{
  int ret = OB_SUCCESS;
  if (queue_ != NULL)
  {
    ObRootAsyncTaskQueue::ObSeqTask task;
    task.type_ = type;
    task.role_ = OB_UPDATESERVER;
    task.server_ = ups.addr_;
    task.inner_port_ = ups.inner_port_;
    // set as slave
    task.server_status_ = 2;
    // set as master
    if (ups.stat_ == UPS_STAT_MASTER)
    {
      task.server_status_ = 1;
    }
    int64_t server_version_length = strlen(server_version);
    if (server_version_length < OB_SERVER_VERSION_LENGTH)
    {
      strncpy(task.server_version_, server_version, server_version_length + 1);
    }
    else
    {
      strncpy(task.server_version_, server_version, OB_SERVER_VERSION_LENGTH - 1);
      task.server_version_[OB_SERVER_VERSION_LENGTH - 1] = '\0';
    }
    ret = queue_->push(task);
    if (ret != OB_SUCCESS)
    {
      YYSYS_LOG(ERROR, "push update server task failed:server[%s], type[%d], ret[%d]",
          task.server_.to_cstring(), task.type_, ret);
    }
    else
    {
      YYSYS_LOG(INFO, "push update server task succ:server[%s]", task.server_.to_cstring());
    }
  }
  return ret;
}
*/
/*   // uncertainty  ups ��д
int ObUpsManager::slave_failure(const common::ObServer &addr, const common::ObServer &slave_addr)
{
  int ret = OB_SUCCESS;
  yysys::CThreadGuard guard(&ups_array_mutex_);
  int i = -1;
  if (!is_ups_master(addr))
  {
    YYSYS_LOG(WARN, "ups not exist or not master, addr=%s", addr.to_cstring());
    ret = OB_NOT_REGISTERED;
  }
  else if (-1 == (i = find_ups_index(slave_addr)))
  {
    YYSYS_LOG(WARN, "slave ups not exist, addr=%s", slave_addr.to_cstring());
    ret = OB_ENTRY_NOT_EXIST;
  }
  else
  {
    YYSYS_LOG(INFO, "ups master reporting slave ups failure, slave=%s", slave_addr.to_cstring());
    change_ups_stat(i, UPS_STAT_OFFLINE);
    ObUps ups(ups_array_[i]);
    reset_ups_read_percent();
    refresh_inner_table(SERVER_OFFLINE, ups, "null");
  }
  return ret;
}
*/
/*    // uncertainty  ups ��д
int ObUpsManager::send_granting_msg(const common::ObServer &addr,
                                    common::ObMsgUpsHeartbeat &msg)
{
  int ret = OB_SUCCESS;
  ret = rpc_stub_.grant_lease_to_ups(addr, msg);
  YYSYS_LOG(DEBUG, "send lease to ups, ups=%s master=%s "
            "self_lease=%ld, schema_version=%ld config_version=%ld",
            to_cstring(addr), to_cstring(msg.ups_master_),
            msg.self_lease_, msg.schema_version_, msg.config_version_);
  return ret;
}
*/

bool ObUpsManager::has_master() const
{
  /*return MAX_UPS_COUNT > ups_master_idx_
    && 0 <= ups_master_idx_;
    */
  return has_master(SYS_TABLE_PAXOS_ID);
}

/*     // uncertainty ups ��д
bool ObUpsManager::need_grant(int64_t now, const ObUps &ups) const
{
  bool ret = false;
  if (ups.did_renew_received_)
  {
    if (now > ups.lease_ - lease_reserved_us_
        && now < ups.lease_)
    {
      // the lease of this ups' is going to expire
      ret = true;
    }
    // else if (master_lease > ups.sent_master_lease_)
    // {
    //   // the master's lease has been extended
    //   ret = true;
    // }
  }
  return ret;
}
*/

//int ObUpsManager::grant_lease(bool did_force /*=false*/)    //uncertainty ups ��д
/*{
  int ret = OB_SUCCESS;
  yysys::CThreadGuard guard(&ups_array_mutex_);
  ObServer master;
  if (has_master())
  {
    master = ups_array_[ups_master_idx_].addr_;
  }
  int64_t now = yysys::CTimeUtil::getTime();
  for (int32_t i = 0; i < MAX_UPS_COUNT; ++i)
  {
    if (UPS_STAT_OFFLINE != ups_array_[i].stat_)
    {
      if (need_grant(now, ups_array_[i]) || did_force)
      {
        ups_array_[i].did_renew_received_ = false;
        ups_array_[i].lease_ = now + lease_duration_us_;

        ObMsgUpsHeartbeat msg;
        msg.ups_master_ = master;
        msg.self_lease_ = ups_array_[i].lease_;
        msg.obi_role_ = obi_role_;
        msg.schema_version_ = schema_version_;
        msg.config_version_ = config_version_;

        int ret2 = send_granting_msg(ups_array_[i].addr_, msg);
        if (OB_SUCCESS != ret2)
        {
          YYSYS_LOG(WARN, "grant lease to ups error, err=%d ups=%s",
                    ret2, ups_array_[i].addr_.to_cstring());
          // don't remove the ups right now
        }
      }
      else
      {
        YYSYS_LOG(DEBUG, "did_renew_received=%c ups=%s", ups_array_[i].did_renew_received_?'Y':'N',
                  ups_array_[i].addr_.to_cstring());
      }
    }
  }
  return ret;
}
*/
/*  //uncertainty ups ��д�߼�
int ObUpsManager::grant_eternal_lease()
{
  int ret = OB_SUCCESS;
  yysys::CThreadGuard guard(&ups_array_mutex_);
  ObServer master;
  if (has_master())
  {
    master = ups_array_[ups_master_idx_].addr_;
  }
  for (int32_t i = 0; i < MAX_UPS_COUNT; ++i)
  {
    if (UPS_STAT_OFFLINE != ups_array_[i].stat_)
    {
      ObMsgUpsHeartbeat msg;
      msg.ups_master_ = master;
      msg.self_lease_ = OB_MAX_UPS_LEASE_DURATION_US;
      msg.obi_role_ = obi_role_;
      msg.schema_version_ = schema_version_;
      msg.config_version_ = config_version_;

      int ret2 = send_granting_msg(ups_array_[i].addr_, msg);
      if (OB_SUCCESS != ret2)
      {
        YYSYS_LOG(WARN, "grant lease to ups error, err=%d ups=%s",
                  ret2, ups_array_[i].addr_.to_cstring());
      }
    }
  } // end for
  return ret;
}
*/
/*
int ObUpsManager::select_ups_master_with_highest_lsn()
{
  int ret = OB_ERROR;
  if (-1 != ups_master_idx_)
  {
    YYSYS_LOG(WARN, "cannot select master when there is already one");
    ret = OB_UPS_MASTER_EXISTS;
  }
  else
  {
    int64_t highest_lsn = -1;
    int master_idx = -1;
    for (int32_t i = 0; i < MAX_UPS_COUNT; ++i)
    {
      if (UPS_STAT_OFFLINE != ups_array_[i].stat_)
      {
        if (ups_array_[i].log_seq_num_ > highest_lsn)
        {
          highest_lsn = ups_array_[i].log_seq_num_;
          master_idx = i;
        }
      }
    } // end for
    if (-1 == master_idx)
    {
      YYSYS_LOG(WARN, "no master selected");
    }
    else
    {
      change_ups_stat(master_idx, UPS_STAT_MASTER);
      ups_master_idx_ = master_idx;
      //add peiouya [Get_masterups_and_timestamp] 20141017:b
      ups_mater_timestamp[master_idx] = yysys::CTimeUtil::getTime();
      //add 20141017:e
      YYSYS_LOG(INFO, "new ups master selected, master=%s lsn=%ld",
          ups_array_[ups_master_idx_].addr_.to_cstring(),
          ups_array_[ups_master_idx_].log_seq_num_);
      ObUps ups(ups_array_[ups_master_idx_]);
      refresh_inner_table(ROLE_CHANGE, ups, "null");
      reset_ups_read_percent();
      ret = OB_SUCCESS;
    }
  }
  return ret;
}
*/
/*
void ObUpsManager::reset_ups_read_percent()  //uncertainty ups ��д
{
  for (int i = 0; i < MAX_UPS_COUNT; i++)
  {
    ups_array_[i].ms_read_percentage_ = 0;
    ups_array_[i].cs_read_percentage_ = 0;
  }
  is_flow_control_by_ip_ = false;
  int32_t ups_count = get_active_ups_count();
  int32_t master_read_percent = 100;
  int32_t slave_read_percent = 0;
  if (ups_count < 1)
  {
    YYSYS_LOG(DEBUG, "No active UpdateServer");
  }
  else
  {
    if (ups_count == 1)
    {
      master_read_percent = 100;
      slave_read_percent = 100;
    }
    else
    {
      if (-1 == ups_master_idx_
          || (ObiRole::MASTER == obi_role_.get_role()
            && -1 == master_master_ups_read_percentage_)
          || (ObiRole::MASTER != obi_role_.get_role()
            && -1 == slave_master_ups_read_percentage_))
      {
        master_read_percent = 100 / ups_count;
        slave_read_percent = 100 / ups_count;
      }
      else if (-1 != ups_master_idx_)
      {
        if (ObiRole::MASTER == obi_role_.get_role())
        {
          master_read_percent = master_master_ups_read_percentage_;
        }
        else
        {
          master_read_percent = slave_master_ups_read_percentage_;
        }
        slave_read_percent = (100 - master_read_percent) / (ups_count - 1);
      }
    }
  }
  for (int32_t i = 0; i < MAX_UPS_COUNT; ++i)
  {
    if (UPS_STAT_OFFLINE != ups_array_[i].stat_
        && UPS_STAT_MASTER != ups_array_[i].stat_
        && UPS_STAT_NOTSYNC != ups_array_[i].stat_)
    {
      ups_array_[i].ms_read_percentage_ = slave_read_percent;
      ups_array_[i].cs_read_percentage_ = slave_read_percent;
    }
    else if (UPS_STAT_MASTER == ups_array_[i].stat_)
    {
      ups_array_[i].ms_read_percentage_ = master_read_percent;
      ups_array_[i].cs_read_percentage_ = master_read_percent;
    }
    else if (UPS_STAT_NOTSYNC == ups_array_[i].stat_)
    {
      ups_array_[i].ms_read_percentage_ = 0;
      ups_array_[i].cs_read_percentage_ = 0;
    }
  }

}
*/
//del pangtianze [MultiUPS] [merge with paxos] 20170519:b
/*
void ObUpsManager::update_ups_lsn()
{
  for (int32_t i = 0; i < MAX_UPS_COUNT; ++i)
  {
    if (UPS_STAT_OFFLINE != ups_array_[i].stat_)
    {
      uint64_t lsn = 0;
      if (OB_SUCCESS != rpc_stub_.get_ups_max_log_seq(ups_array_[i].addr_, lsn, revoke_rpc_timeout_us_))
      {
        YYSYS_LOG(WARN, "failed to get ups log seq, ups=%s", ups_array_[i].addr_.to_cstring());
      }
      else
      {
        ups_array_[i].log_seq_num_ = lsn;
      }
    } // end for
  }
}
*/
//del:e
// del lqc [multiups with paxos] 20170610
//bool ObUpsManager::is_master_lease_valid() const
//{
//    bool ret = false;
//    if (has_master())
//    {
//        int64_t now = yysys::CTimeUtil::getTime();
//        ret = (ups_array_[ups_master_idx_].lease_ > now);
//    }
//    return ret;
//}
// del e
/*
int ObUpsManager::select_new_ups_master()
{
  int ret = OB_ERROR;
  if (-1 == ups_master_idx_ && !is_master_lease_valid())
  {
    this->update_ups_lsn();
    ret = this->select_ups_master_with_highest_lsn();
  }
  return ret;
}
*/
// del lqc [multiups with paxos] 20170610
//void ObUpsManager::check_all_ups_offline()
//{
//  bool all_offline = true;
//  for (int32_t i = 0; i < MAX_UPS_COUNT; ++i)
//  {
//    if (UPS_STAT_OFFLINE != ups_array_[i].stat_)
//    {
//      all_offline = false;
//      break;
//    }
//  }
//  if (all_offline)
//  {
//    YYSYS_LOG(INFO, "all UPS offline");
//    waiting_ups_finish_time_ = 0;
//  }
//}
//del e
/*
int ObUpsManager::check_lease()  // uncertainty ups ��д
{
  int ret = OB_SUCCESS;
  bool did_select_new_master = false;
  for (int32_t i = 0; i < MAX_UPS_COUNT; ++i)
  {
    yysys::CThreadGuard guard(&ups_array_mutex_);
    int64_t now = yysys::CTimeUtil::getTime();
    if (UPS_STAT_OFFLINE != ups_array_[i].stat_)
    {
      if (now > ups_array_[i].lease_ + MAX_CLOCK_SKEW_US)
      {
        YYSYS_LOG(INFO, "ups is offline, ups=%s lease=%ld lease_duration=%ld now=%ld",
            ups_array_[i].addr_.to_cstring(),
            ups_array_[i].lease_,
            lease_duration_us_, now);

        // ups offline
        if (ups_array_[i].stat_ == UPS_STAT_MASTER)
        {
          ups_master_idx_ = -1;
          did_select_new_master = true;
        }
        YYSYS_LOG(INFO, "There's ups offline. ups: [%s], stat[%d], master_idx[%d]",
            to_cstring(ups_array_[i].addr_), ups_array_[i].stat_, ups_master_idx_);
        reset_ups_read_percent();
        change_ups_stat(i, UPS_STAT_OFFLINE);
        ObUps ups(ups_array_[i]);
        ups_array_[i].reset();
        check_all_ups_offline();
        refresh_inner_table(SERVER_OFFLINE, ups, "null");
      }
    }
  } // end for

  if (did_select_new_master)
  {
    yysys::CThreadGuard guard(&ups_array_mutex_);
    // select new ups master
    int ret2 = select_new_ups_master();
    if (OB_SUCCESS != ret2)
    {
      YYSYS_LOG(WARN, "no master selected");
    }
  }

  if (did_select_new_master && has_master())
  {
    // send lease immediately to notify the change of master
    this->grant_lease(true);
  }
  return ret;
}
*/
// mod lqc [multiups with paxos] 20170610
//void ObUpsManager::change_ups_stat(const int32_t index, const ObUpsStatus new_stat)
//{
//  YYSYS_LOG(INFO, "begin change ups status:master[%d], addr[%d:%s], stat[%s->%s]",
//      ups_master_idx_, index, ups_array_[index].addr_.to_cstring(),
//      ups_stat_to_cstr(ups_array_[index].stat_), ups_stat_to_cstr(new_stat));
//  ups_array_[index].stat_ = new_stat;
//}
//mod e

/*    uncertainty  ups ��ȥ
int ObUpsManager::check_ups_master_exist()
{
  int ret = OB_SUCCESS;
  yysys::CThreadGuard guard(&ups_array_mutex_);
  if (0 < waiting_ups_finish_time_)
  {
    int64_t now = yysys::CTimeUtil::getTime();
    if (now > waiting_ups_finish_time_)
    {
      if (OB_SUCCESS == (ret = select_ups_master_with_highest_lsn()))
      {
        waiting_ups_finish_time_ = -1;
      }
      else if (OB_UPS_MASTER_EXISTS == ret)
      {
        waiting_ups_finish_time_ = -1;
      }
    }
  }
  else if (0 > waiting_ups_finish_time_)
  {
    // check ups master exist
    ret = select_new_ups_master();
  }
  else
  {
    // 0 == waiting_ups_finish_time_ means all ups is offline, do nothing
  }
  return ret;
}
*/

int ObUpsManager::send_revoking_msg(const common::ObServer &addr, int64_t lease, const common::ObServer& master)
{
  int ret = OB_SUCCESS;
  ret = rpc_stub_.revoke_ups_lease(addr, lease, master, revoke_rpc_timeout_us_);
  return ret;
}

// del lqc [multiups with paxos] 20170610
//int ObUpsManager::revoke_master_lease(int64_t &waiting_lease_us)
//{
//  int ret = OB_SUCCESS;
//  waiting_lease_us = 0;
//  if (has_master())
//  {
//    if (is_master_lease_valid())
//    {
//      // the lease is valid now
//      int64_t master_lease = ups_array_[ups_master_idx_].lease_;
//      int ret2 = send_revoking_msg(ups_array_[ups_master_idx_].addr_,
//          master_lease, ups_array_[ups_master_idx_].addr_);
//      if (OB_SUCCESS != ret2)
//      {
//        YYSYS_LOG(WARN, "send lease revoking message to ups master error, err=%d ups=%s",
//            ret2, ups_array_[ups_master_idx_].addr_.to_cstring());
//        // we should wait for the lease timeout
//        int64_t now2 = yysys::CTimeUtil::getTime();
//        if (master_lease > now2)
//        {
//          waiting_lease_us = master_lease - now2;
//          waiting_ups_finish_time_ = 0; // tell the check thread don't select new master right now
//        }
//      }
//      else
//      {
//        YYSYS_LOG(INFO, "revoked lease, ups=%s", ups_array_[ups_master_idx_].addr_.to_cstring());
//      }
//    }
//    else
//    {
//      YYSYS_LOG(WARN, "has master but lease is invalid");
//    }
//    YYSYS_LOG(INFO, "revoke lease of old master, old_master=%s",
//              ups_array_[ups_master_idx_].addr_.to_cstring());
//    change_ups_stat(ups_master_idx_, UPS_STAT_SYNC);
//    ObUps ups(ups_array_[ups_master_idx_]);
//    refresh_inner_table(ROLE_CHANGE, ups, "null");
//    ups_master_idx_ = -1;
//  }
//  return ret;
//}
// del lqc [multiups with paxos] 20170610
//bool ObUpsManager::is_idx_valid(int ups_idx) const
//{
//  return (0 <= ups_idx && ups_idx < MAX_UPS_COUNT);
//}
//del pangtianze [MultiUPS] [merge with paxos] 20170519:b
/*
bool ObUpsManager::is_ups_with_highest_lsn(int ups_idx)
{
  bool ret = false;
  if (is_idx_valid(ups_idx))
  {
    this->update_ups_lsn();
    int64_t highest_lsn = -1;
    for (int32_t i = 0; i < MAX_UPS_COUNT; ++i)
    {
      if (UPS_STAT_OFFLINE != ups_array_[i].stat_)
      {
        if (ups_array_[i].log_seq_num_ > highest_lsn)
        {
          highest_lsn = ups_array_[i].log_seq_num_;
        }
      }
    } // end for
    ret = (highest_lsn == ups_array_[ups_idx].log_seq_num_);
  }
  return ret;
}
*/
//del:e
// del lqc [multiups with paxos ] 20100610
//this func is for rs_admin
//int ObUpsManager::set_ups_master(const common::ObServer &master, bool did_force)
//{
//  int ret = OB_SUCCESS;
//  int64_t waiting_lease_us = 0;
//  int i = -1;
//  //add pangtianze [Paxos rs_election] 20161008:b
//  worker_->get_root_server().get_rs_node_mgr()->set_can_select_master_ups(false);
//  //add:e
//  {
//    yysys::CThreadGuard guard(&ups_array_mutex_);
//    i = find_ups_index(master);
//    if (!is_idx_valid(i))
//    {
//      YYSYS_LOG(WARN, "ups not registered, addr=%s", master.to_cstring());
//      ret = OB_NOT_REGISTERED;
//    }
//    else if (UPS_STAT_MASTER == ups_array_[i].stat_)
//    {
//      YYSYS_LOG(WARN, "ups is already the master, ups=%s",
//                master.to_cstring());
//      //mod huangjianwei [Paxos ups_replication] 20160309:b
//      //ret = OB_INVALID_ARGUMENT;
//      ret = OB_IS_ALREADY_THE_MASTER;
//      //mod:e
//    }
//    //mod pangtianze [Paxos ups_replication] 20160914:b
//    /*
//    else if ((UPS_STAT_SYNC != ups_array_[i].stat_ || !is_ups_with_highest_lsn(i))
//             && !did_force)
//    {
//      YYSYS_LOG(WARN, "ups is not sync, ups=%s stat=%d lsn=%ld",
//                master.to_cstring(), ups_array_[i].stat_, ups_array_[i].log_seq_num_);
//      ret = OB_INVALID_ARGUMENT;
//    }
//    */
//    else if (UPS_STAT_SYNC != ups_array_[i].stat_ && !did_force)
//    {
//       YYSYS_LOG(WARN, "ups is not sync, ups=%s stat=%d lsn=%ld term=%ld",
//         master.to_cstring(), ups_array_[i].stat_, ups_array_[i].log_seq_num_, ups_array_[i].log_term_);
//       ret = OB_INVALID_ARGUMENT;
//    }
//    //mod:e
//    else
//    {
//      revoke_master_lease(waiting_lease_us);
//    }
//  }
//  if (OB_SUCCESS == ret && 0 < waiting_lease_us)
//  {
//    // wait current lease until timeout, sleep without locking so that the heartbeats will continue
//    YYSYS_LOG(INFO, "revoke lease failed and we should wait, usleep=%ld", waiting_lease_us);
//    usleep(static_cast<useconds_t>(waiting_lease_us));
//  }
//  bool new_master_selected = false;
//  if (OB_SUCCESS == ret && is_idx_valid(i))
//  {
//    yysys::CThreadGuard guard(&ups_array_mutex_);
//    // re-check status
//    if ((/*del pangtianze(UPS_STAT_SYNC == ups_array_[i].stat_ && is_ups_with_highest_lsn_and_term(i)) || */did_force)
//        && master == ups_array_[i].addr_
//        && !is_master_lease_valid())
//    {
//      change_ups_stat(i, UPS_STAT_MASTER);
//      ups_master_idx_ = i;
//      //add peiouya [Get_masterups_and_timestamp] 20141017:b
//      ups_mater_timestamp[i] = yysys::CTimeUtil::getTime();
//      //add 20141017:e
//      YYSYS_LOG(INFO, "set new ups master, master=%s force=%c",
//                master.to_cstring(), did_force?'Y':'N');
//      ObUps ups(ups_array_[ups_master_idx_]);
//      refresh_inner_table(ROLE_CHANGE, ups, "null");
//      new_master_selected = true;
//      waiting_ups_finish_time_ = -1;
//      reset_ups_read_percent();
//    }
//    //add pangtianze [Paxos ups_replication] 20170125:b
//    else if (!did_force)
//    {
//        ret = select_new_ups_master(i);
//    }
//    //add:e
//    else
//    {
//      // should rarely come here
//      waiting_ups_finish_time_ = -1;
//      YYSYS_LOG(WARN, "the ups removed or status changed after sleeping, try again, ups=%s", master.to_cstring());
//      ret = OB_CONFLICT_VALUE;
//    }
//    //add:e
//  }
//  //add pangtianze [Paxos rs_election] 20161008:b
//  worker_->get_root_server().get_rs_node_mgr()->set_can_select_master_ups(false);
//  //add:e
//  if (new_master_selected)
//  {
//    this->grant_lease(true);
//  }
//  return ret;
//}
/*
int32_t ObUpsManager::get_active_ups_count() const
{
  int32_t ret = 0;
  for (int32_t i = 0; i < MAX_UPS_COUNT; ++i)
  {
    if (UPS_STAT_OFFLINE != ups_array_[i].stat_
        && UPS_STAT_NOTSYNC != ups_array_[i].stat_)
    {
      ret++;
    }
  }
  return ret;
}
*/
// del lqc [multiusp with paxos] 20170610
//int32_t ObUpsManager::get_ups_count() const
//{
//  int32_t ret = 0;
//  for (int32_t i = 0; i < MAX_UPS_COUNT; ++i)
//  {
//    if (UPS_STAT_OFFLINE != ups_array_[i].stat_)
//    {
//      ret++;
//    }
//  }
//  return ret;
//}
//del e
//uncertainty �˴���Ҫ�޸ĳ�multiups ����
int ObUpsManager::get_ups_master(ObUps &ups_master) const
{
  /*
  int ret = OB_ENTRY_NOT_EXIST;
  yysys::CThreadGuard guard(&ups_array_mutex_);
  if (has_master())
  {
    ups_master = ups_array_[ups_master_idx_];
    ret = OB_SUCCESS;
  }
  return ret;
  */
  return get_ups_master_by_paxos_id(SYS_TABLE_PAXOS_ID, ups_master);  // uncertainty �޸�����
}
/*
const char* ObUpsManager::ups_stat_to_cstr(ObUpsStatus stat) const
{
  const char* ret = "";
  switch(stat)
  {
    case UPS_STAT_OFFLINE:
      ret = "offline";
      break;
    case UPS_STAT_MASTER:
      ret = "master";
      break;
    case UPS_STAT_SYNC:
      ret = "sync";
      break;
    case UPS_STAT_NOTSYNC:
      ret = "nsync";
      break;
    default:
      break;
  }
  return ret;
}
*/
/*
void ObUpsManager::print(char* buf, const int64_t buf_len, int64_t &pos) const
{
  yysys::CThreadGuard guard(&ups_array_mutex_);
  if (is_master_lease_valid())
  {
    int64_t now2 = yysys::CTimeUtil::getTime();
    databuff_printf(buf, buf_len, pos, "lease_left=%ld|", ups_array_[ups_master_idx_].lease_ - now2);
  }
  else
  {
    databuff_printf(buf, buf_len, pos, "lease_left=null|");
  }
  for (int32_t i = 0; i < MAX_UPS_COUNT; ++i)
  {
    if (UPS_STAT_OFFLINE != ups_array_[i].stat_)
    {
      databuff_printf(buf, buf_len, pos, "%s(%d %s %d %d %lu %s),", ups_array_[i].addr_.to_cstring(),
                      ups_array_[i].inner_port_, ups_stat_to_cstr(ups_array_[i].stat_),
                      ups_array_[i].ms_read_percentage_, ups_array_[i].cs_read_percentage_,
                      ups_array_[i].log_seq_num_, ups_array_[i].obi_role_.get_role_str());
    }
  }
}
*/
//del pangtianze [MultiUPS] [merge with paxos] 20170519:b
/*
int ObUpsManager::set_ups_config(int32_t master_master_ups_read_percentage, int32_t slave_master_ups_read_percentage)
{
  int ret = OB_SUCCESS;
  if ((-1 != master_master_ups_read_percentage)
      && (0 > master_master_ups_read_percentage
        || 100 < master_master_ups_read_percentage))
  {
    YYSYS_LOG(WARN, "invalid param, master_master_ups_read_percentage=%d", master_master_ups_read_percentage);
    ret = OB_INVALID_ARGUMENT;
  }
  else if ((-1 != slave_master_ups_read_percentage)
      && (0 > slave_master_ups_read_percentage
        || 100 < slave_master_ups_read_percentage))
  {
    YYSYS_LOG(WARN, "invalid param, slave_master_ups_read_percentage=%d", slave_master_ups_read_percentage);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    YYSYS_LOG(INFO, "change ups config, read_master_master_ups_percentage=%d read_slave_master_ups_percentage=%d",
        master_master_ups_read_percentage, slave_master_ups_read_percentage);
    master_master_ups_read_percentage_ = master_master_ups_read_percentage;
    slave_master_ups_read_percentage_ = slave_master_ups_read_percentage;
    yysys::CThreadGuard guard(&ups_array_mutex_);
    reset_ups_read_percent();
  }
  return ret;
}
*/
//del:e
void ObUpsManager::get_master_ups_config(int32_t &master_master_ups_read_percent
                                         //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
                                         //, int32_t &slave_master_ups_read_percent
                                         //del:e
                                         ) const
{
  master_master_ups_read_percent = master_master_ups_read_percentage_;
  //del lbzhong [Paxos Cluster.Flow.UPS] 201607025:b
  //slave_master_ups_read_percent = slave_master_ups_read_percentage_;
  //del:e
}

int ObUpsManager::set_ups_config(const common::ObServer &addr, int32_t ms_read_percentage, int32_t cs_read_percentage)
{
  int ret = OB_SUCCESS;
  if (0 > ms_read_percentage || 100 < ms_read_percentage)
  {
    YYSYS_LOG(WARN, "invalid param, ms_read_percentage=%d", ms_read_percentage);
    ret = OB_INVALID_ARGUMENT;
  }
  else if (0 > cs_read_percentage || 100 < cs_read_percentage)
  {
    YYSYS_LOG(WARN, "invalid param, cs_read_percentage=%d", cs_read_percentage);
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    // mod  lqc [multiups with paxos] 20170610
    int i = INVALID_INDEX;
    ObUpsNode *ups_node = NULL;
    is_flow_control_by_ip_ = true;
    yysys::CThreadGuard guard(&ups_array_mutex_);
    if(!addr.is_valid ())
    {
      YYSYS_LOG(WARN, "master is invalid");
      ret = OB_INVALID_ARGUMENT;
    }
    else if (INVALID_INDEX == (i = static_cast<int>(find_ups_index(addr,ups_node))))
    {
      YYSYS_LOG(WARN, "ups not exist, addr=%s", addr.to_cstring());
      ret = OB_ENTRY_NOT_EXIST;
    }
    else
    {
      YYSYS_LOG(INFO, "change ups config, ups=%s ms_read_percentage=%d cs_read_percentage=%d",
                addr.to_cstring(), ms_read_percentage, cs_read_percentage);
      ups_node->ups_array_->at(i).ms_read_percentage_ = ms_read_percentage;
      ups_node->ups_array_->at(i).cs_read_percentage_  = cs_read_percentage;
    }
  }//mod e
  return ret;
}

void ObUps::convert_to(ObUpsInfo &ups_info) const
{
  ups_info.addr_ = addr_;
  ups_info.inner_port_ = inner_port_;
  if (UPS_STAT_MASTER == stat_)
  {
    ups_info.stat_ = UPS_MASTER;
  }
  else
  {
    ups_info.stat_ = UPS_SLAVE;
  }
  ups_info.ms_read_percentage_ = static_cast<int8_t>(ms_read_percentage_);
  ups_info.cs_read_percentage_ = static_cast<int8_t>(cs_read_percentage_);
  ups_info.cluster_id_ = cluster_id_;
  ups_info.paxos_id_ = paxos_id_;
}

//mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
ObUpsManager::ObUpsManager(ObRootRpcStub &rpc_stub, ObRootWorker *worker, const int64_t &revoke_rpc_timeout_us,
                           const int64_t &lease_duration,
                           const int64_t &lease_reserved_us,
                           const int64_t &waiting_ups_register_duration,
                           bool is_use_paxos,
                           int64_t use_cluster_num,
                           int64_t use_paxos_num,
                           //const ObiRole &obi_role,
                           const volatile int64_t& schema_version,
                           const volatile int64_t &config_version
                           //add pangtianze [Paxos rs_election] 20150820:b
                           //,const int64_t &max_deployed_ups_count
                           ,int32_t (&paxos_ups_quorum_scales)[OB_MAX_PAXOS_GROUP_COUNT]
                           )
//add:e
  :queue_(NULL), rpc_stub_(rpc_stub), worker_(worker), /*obi_role_(obi_role),*/
    revoke_rpc_timeout_us_(revoke_rpc_timeout_us),
    lease_duration_us_(lease_duration), lease_reserved_us_(lease_reserved_us),
    waiting_ups_register_duration_(waiting_ups_register_duration),
    /*sys_table_paxos_id_(0),//mod peiouya [MultiUPS] [UPS_Manage_Function] 20150729*/
    is_use_paxos_(is_use_paxos), use_cluster_num_(use_cluster_num), use_paxos_num_(use_paxos_num),
    waiting_ups_finish_time_(0),
    schema_version_(schema_version), config_version_(config_version),
    new_frozen_version_(OB_INVALID_VERSION), //add peiouya [MultiUPS] [UPS_Manage_Function] 20150521
    master_ups_read_percentage_(-1),
    master_ups_merge_percentage_(-1),
    is_flow_control_by_ip_(false), is_cs_flow_init_(false) //add pangtianze [Paxos Cluster.Flow.UPS] 20170817:b:e
  ,initialized_(false), link_head_(NULL)//add liuzy [MultiUPS] [UPS_Manager]
  //add pangtianze [Paxos ups_replication] 20150818:b
  //, max_deployed_ups_count_(max_deployed_ups_count)
  //add:e
  , paxos_ups_quorum_scales_(paxos_ups_quorum_scales)
  ,last_change_ups_master_(0)
  //mod 20150701:e
{
  init_master_idx_array();
  //add liuzy [MultiUPS] [UPS_Manager] 20151211:b
  init_ups_node();
  print_ups_manager();/*Exp: this func is used to debug*/
  //add 20151211:e
  //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160223:b
  init_paxos_group_stat_array();
  init_cluster_stat_array();
  //add 20160223:e
  init_freeze_paxos_group_stat_array();
}

ObUpsManager::~ObUpsManager()
{
}

void ObUpsManager::set_async_queue(ObRootAsyncTaskQueue * queue)
{
  queue_ = queue;
}

//mod liuzy [MultiUPS] [UPS_Manager] [private] 20151221:b
/*
 * traverse ups_array_ to get the input addr index
 * return: -1, addr not exist or its stat not equal UPS_STAT_OFFLINE
 *         [0,MAX_CLUSTER_COUNT*MAX_UPS_COUNT_ONE_CLUSTER)  ok!
 */
int64_t ObUpsManager::find_ups_index(const common::ObServer &addr, ObUpsNode *&ups_node, bool iter_all_server) const
{
  //mod peiouya [MultiUPS] [UPS_Manage_Function] 20150729:b
  //int64_t ret = -1;
  int64_t ret = INVALID_INDEX;
  int64_t ups_idx = INVALID_INDEX;
  //mod 20150729:e
  if (NULL != (ups_node = find_node(addr, ups_idx, NULL, iter_all_server))
      && INVALID_INDEX != ups_idx)
  {
    ret = ups_idx;
  }
  else
  {
    YYSYS_LOG(INFO, "ups:[%s] is not existent.", addr.to_cstring());
  }
  return ret;
}
//del liuzy [MultiUPS] [useless_func] 20160526:b
//bool ObUpsManager::did_ups_exist(const common::ObServer &addr) const
//{
//  ObUpsNode *ups_node = NULL;//unused
//  return INVALID_INDEX != find_ups_index(addr, ups_node);
//}
//del 20160526:e
//add peiouya [MultiUPS] [UPS_Manage_Function] 20150521
bool ObUpsManager::is_ups_idx_valid(const int64_t ups_idx, const ObUpsNode *ups_node) const
{
  bool ret = false;
  if (NULL == ups_node)
  {
    YYSYS_LOG(WARN, "ups_node is NULL.");
  }
  else if (INVALID_INDEX != ups_idx &&
           ups_idx < ups_node->ups_array_->count() &&
           0 <= ups_node->cluster_idx_ && ups_node->cluster_idx_ < use_cluster_num_ &&
           0 <= ups_node->paxos_idx_ && ups_node->paxos_idx_ < use_paxos_num_)
  {
    ret = true;
  }
  else
  {
    YYSYS_LOG(WARN, "ups_idx is invalid.");
  }
  return ret;
}
//add 20150527:e

bool ObUpsManager::has_master(const int64_t& paxos_idx) const
{
  bool ret = false;
  if (0 <= paxos_idx && use_paxos_num_ > paxos_idx)
  {
    if (INVALID_INDEX != master_idx_array_new_[paxos_idx].first &&
        INVALID_INDEX != master_idx_array_new_[paxos_idx].second)
    {
      ret = true;
    }
  }
  return ret;
}
void ObUpsManager::check_slave_frozen_version(int64_t &last_frozen_version, const int64_t paxos_idx, const common::ObServer &addr)
{
  if(!is_ups_master(addr) && has_master(paxos_idx))
  {
    if(last_frozen_version == master_last_frozen_version(paxos_idx) + 1)
    {
      last_frozen_version = master_last_frozen_version(paxos_idx);
    }
    else if (last_frozen_version == master_last_frozen_version(paxos_idx))
    {
      YYSYS_LOG(DEBUG, "slave ups frozen version equal master ups version");
    }
  }
  else
  {
    YYSYS_LOG(DEBUG, "is master ups or this paxos group not contain master ups");
  }
}

//add peiouya [MultiUPS] [UPS_Manage_Function] 20150815:b
//the function should be call after report_flag=true
//only register_ups and renew_lease can call the function
int ObUpsManager::check_ups_info_valid(const int64_t last_frozen_version, const int64_t paxos_idx, const bool is_master)
{
  int ret = OB_INVALID_FROZEN_VERSION;
  int64_t rs_last_frozen_version = root_server_->get_last_frozen_version();
  //check frozen_version valid after set report_flag=true
  if (OB_INVALID_VERSION == rs_last_frozen_version)
  {
    ret = OB_RS_INVALID_LAST_FROZEN_VERSION;
    YYSYS_LOG(ERROR, "after rs set initialized_:true, rs_last_frozen_version must not be OB_INVALID_VERSION, ret=%d",
              ret);
    root_server_->kill_self();
  }
  //is_master:false
  else if (!is_master)
  {
    //has_mater, cur ups's last_frozen_version LE master ups'last_frozen_version
    //mod liuzy [MultiUPS] [add_cluster_interface] 20160719:b
    //      if (has_master(paxos_idx) && last_frozen_version <= master_last_frozen_version(paxos_idx))
    if (has_master(paxos_idx) && (last_frozen_version <= master_last_frozen_version(paxos_idx) ||
                                  last_frozen_version <= new_frozen_version_))
      //mod 20160719:e
    {
      ret = OB_SUCCESS;
    }
    //no master, cur ups's last_frozen_version LE rs_last_frozen_version + 1
    else if (!has_master(paxos_idx) && last_frozen_version <= rs_last_frozen_version + 1)
    {
      ret = OB_SUCCESS;
    }
    else
    {
      //add liuzy [MultiUPS] [UPS_REGISTER_VERSION_CHECK] 20160718:b
      YYSYS_LOG(WARN, "is_master:[false], last_froz_ver:[%ld], master_last_froz_ver:[%ld], rs_last_froz_ver:[%ld]",
                last_frozen_version, master_last_frozen_version(paxos_idx), rs_last_frozen_version);
      //add 20160718:e
      //nothing todo
    }
  }
  //is_master:true
  else
  {
    //master ups's last_frozen_version must be its paxos's max frozen version
    //and must be in [rs_last_frozen_version, rs_last_frozen_version + 1]
    //and must be ge >= 1 (This condition ">= 1" has been implicitly contained, without additional judgment)
    if ((last_frozen_version == rs_last_frozen_version
         || last_frozen_version == rs_last_frozen_version + 1)
        && is_max_version_in_paxos(last_frozen_version, paxos_idx))
    {
      ret = OB_SUCCESS;
    }
    //add liuzy [MultiUPS] [UPS_REGISTER_VERSION_CHECK] 20160718:b
    else
    {
      YYSYS_LOG(WARN, "is_master:[true], last_froz_ver:[%ld], rs_last_froz_ver:[%ld], is_max_ver_in_paxos:[%s]",
                last_frozen_version, rs_last_frozen_version,
                is_max_version_in_paxos(last_frozen_version, paxos_idx) ? "true" : "false");
    }
    //add 20160718:e
  }
  return ret;
}

bool ObUpsManager::is_max_version_in_paxos(const int64_t frozen_version, const int64_t paxos_idx) const
{
  bool is_max_version = true;
  ObUpsNode *ups_node = NULL, *paxos_head = NULL;
  if (NULL != (paxos_head = find_paxos_head_node(paxos_idx)))
  {
    ups_node = paxos_head;
    do
    {
      for (int64_t ups_idx = 0; ups_idx < ups_node->ups_array_->count(); ++ups_idx)
      {
        if (ups_node->ups_array_->at(ups_idx).is_active_
            && (UPS_STAT_SYNC == ups_node->ups_array_->at(ups_idx).stat_ ||
                UPS_STAT_MASTER == ups_node->ups_array_->at(ups_idx).stat_)
            && frozen_version < ups_node->ups_array_->at(ups_idx).last_frozen_version_)
        {
          is_max_version = false;
          break;
        }
      }
      ups_node = ups_node->next_cluster_node();
    }while (ups_node != paxos_head && is_max_version);
  }
  else
  {
    YYSYS_LOG(WARN, "Find paxos:[%ld] head node failed.", paxos_idx);
  }
  return is_max_version;
}
//add 20150815:e

bool ObUpsManager::is_ups_master(const common::ObServer &addr) const
{
  bool is_ups_master = false;
  int64_t ups_idx = INVALID_INDEX;
  ObUpsNode *temp_node = NULL;
  if (INVALID_INDEX == (ups_idx = find_ups_index(addr, temp_node)))
  {
    YYSYS_LOG(WARN, "Find ups addr:[%s] index in ups array failed.", addr.to_cstring());
  }
  else if (NULL == temp_node)
  {
    YYSYS_LOG(WARN, "Find node addr:[%s] failed.", addr.to_cstring());
  }
  else
  {
    is_ups_master = (UPS_STAT_MASTER == temp_node->ups_array_->at(ups_idx).stat_);
  }
  return is_ups_master;
}
void ObUpsManager::change_ups_stat(const int64_t& cluster_idx, const int64_t& paxos_idx,
                                   const int64_t& ups_idx, const ObUpsStatus new_stat)
{
  ObUpsNode *ups_node = NULL;
  if (NULL != (ups_node = find_node(cluster_idx, paxos_idx)))
  {
    //add liuzy [MultiUPS] [bugfix] 20160526:b
    if (is_ups_idx_valid(ups_idx, ups_node))
    {
      //add 20160526:e
      //add pangtianze [Paxos] 20170905:b
      if (ups_node->ups_array_->at(ups_idx).stat_ == UPS_STAT_MASTER && new_stat != UPS_STAT_MASTER)
      {
        YYSYS_LOG(ERROR, "ups[%s] abandon master, change to slave", ups_node->ups_array_->at(ups_idx).addr_.to_cstring());
      }
      else if (ups_node->ups_array_->at(ups_idx).stat_!= UPS_STAT_MASTER && new_stat == UPS_STAT_MASTER)
      {
        YYSYS_LOG(ERROR, "ups[%s] change to master", ups_node->ups_array_->at(ups_idx).addr_.to_cstring());
      }
      //add:e
      if (ups_node->ups_array_->at(ups_idx).stat_ == UPS_STAT_OFFLINE && new_stat == UPS_STAT_OFFLINE)
      {
        //nothing todo
      }
      else
      {
        YYSYS_LOG(INFO, "Begin change ups: addr[%ld][%ld]:%s, stat[%s->%s]",
                  cluster_idx, paxos_idx, ups_node->ups_array_->at(ups_idx).addr_.to_cstring(),
                  ups_stat_to_cstr(ups_node->ups_array_->at(ups_idx).stat_), ups_stat_to_cstr(new_stat));
      }
      //mod peiouya [MultiUPS] [UPS_Manage_Function] 20150811:b
      //if (UPS_STAT_MASTER == ups_array_[ver_idx][hor_idx].stat_)
      if (UPS_STAT_MASTER == ups_node->ups_array_->at(ups_idx).stat_
          && UPS_STAT_MASTER != new_stat)
        //mod 20150811:e
      {
        atomic_exchange(reinterpret_cast<volatile uint64_t*>(&(master_idx_array_new_[paxos_idx].first)), INVALID_INDEX);
        atomic_exchange(reinterpret_cast<volatile uint64_t*>(&(master_idx_array_new_[paxos_idx].second)), INVALID_INDEX);
      }
      ups_node->ups_array_->at(ups_idx).stat_ = new_stat;
      //add liuzy [MultiUPS] [bugfix] 20160526:b
    }
    else
    {
      YYSYS_LOG(ERROR, "ups array count:[%ld], ups idx:[%ld] must be in [0, %ld)",
                ups_node->ups_array_->count(), ups_idx, ups_node->ups_array_->count());
    }
    //add 20160526:e
  }
  else
  {
    YYSYS_LOG(WARN, "Change ups stat, find node[%ld][%ld] failed.", cluster_idx, paxos_idx);
  }
}

int32_t ObUpsManager::get_active_ups_count(const int64_t paxos_id) const
{
  int32_t count = 0;
  ObUpsNode *ups_node = NULL, *paxos_head = NULL;
  if (NULL != (paxos_head = find_paxos_head_node(paxos_id)))
  {
    ups_node = paxos_head;
    do
    {
      for (int64_t ups_idx = 0; ups_idx < ups_node->ups_array_->count(); ++ups_idx)
      {
        if (UPS_STAT_OFFLINE != ups_node->ups_array_->at(ups_idx).stat_ &&
            UPS_STAT_NOTSYNC != ups_node->ups_array_->at(ups_idx).stat_)
        {
          count++;
        }
      }
      ups_node = ups_node->next_cluster_node();
    } while(paxos_head != ups_node);
  }
  return count;
}
//mod lqc [merge with paxos] 20170526
//void ObUpsManager::reset_ups_read_percent_by_paxos_id(const int64_t& paxos_id)
//{
//    int32_t is_strong_consistent = (int32_t)worker_->get_root_server().get_config().is_strong_consistency_read;
//    for (int64_t paxos_idx = 0; paxos_idx < use_paxos_num_; ++paxos_idx)
//    {
//        ObUpsNode *ups_node = NULL, *paxos_head = NULL;
//        if (0 > paxos_id || MAX_UPS_COUNT_ONE_CLUSTER <= paxos_id)
//        {
//            YYSYS_LOG(ERROR, "paxos_id must be in [0, %ld)", MAX_UPS_COUNT_ONE_CLUSTER);
//        }
//        else
//        {
//            if (NULL != (paxos_head = find_paxos_head_node(paxos_idx)))
//            {
//                ups_node = paxos_head;
//                do
//                {
//                    for (int64_t ups_idx = 0; ups_idx < ups_node->ups_array_->count(); ++ups_idx)
//                    {
//                        ups_node->ups_array_->at(ups_idx).ms_read_percentage_ = 0;
//                        ups_node->ups_array_->at(ups_idx).cs_read_percentage_ = 0;
//                    }
//                    ups_node = ups_node->next_cluster_node();
//                } while(paxos_head != ups_node);
//            }
//            is_flow_control_by_ip_ = false;
//            int32_t ups_count = get_active_ups_count(paxos_idx);
//            int64_t master_read_percent = 100;
//            int64_t slave_read_percent = 0;
//            //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160303:b
//            if (!is_paxos_group_offline(paxos_idx))
//            {
//                //add 20160303:e
//                if (1 > ups_count )
//                {
//                    YYSYS_LOG(WARN, "paxos_id=%ld no active UpdateServer", paxos_id);
//                }
//                else if (1 == ups_count)
//                {
//                    master_read_percent = 100;
//                    slave_read_percent = 100;
//                }
//                else
//                {
//                    if (!has_master(paxos_id))
//                    {
//                        master_read_percent = 100 / ups_count;
//                        slave_read_percent = 100 / ups_count;
//                    }
//                    else
//                    {
//                        master_read_percent = master_ups_read_percentage_;
//                        slave_read_percent = (100 - master_read_percent) / (ups_count - 1);
//                    }
//                }
//            }//add liuzy [MultiUPS] [take_paxos_offline_interface] 20160303

//            //mod liuzy [MultiUPS] [take_paxos_offline_interface] 20160303:b
//            //if (NULL != paxos_head)
//            if (NULL != paxos_head && !is_paxos_group_offline(paxos_id))
//                //mod 20160303:e
//            {
//                ups_node = paxos_head;
//                do
//                {
//                    for (int64_t ups_idx = 0; ups_idx < ups_node->ups_array_->count(); ++ups_idx)
//                    {
//                        if (UPS_STAT_SYNC == ups_node->ups_array_->at(ups_idx).stat_)
//                        {
//                            ups_node->ups_array_->at(ups_idx).ms_read_percentage_ = static_cast<int32_t>(slave_read_percent);
//                            ups_node->ups_array_->at(ups_idx).cs_read_percentage_ = static_cast<int32_t>(slave_read_percent);
//                        }
//                        else if (UPS_STAT_MASTER == ups_node->ups_array_->at(ups_idx).stat_)
//                        {
//                            ups_node->ups_array_->at(ups_idx).ms_read_percentage_ = static_cast<int32_t>(master_read_percent);
//                            ups_node->ups_array_->at(ups_idx).cs_read_percentage_ = static_cast<int32_t>(master_read_percent);
//                        }
//                        else if (UPS_STAT_NOTSYNC == ups_node->ups_array_->at(ups_idx).stat_)
//                        {
//                            ups_node->ups_array_->at(ups_idx).ms_read_percentage_ = 0;
//                            ups_node->ups_array_->at(ups_idx).cs_read_percentage_ = 0;
//                        }
//                    }
//                    ups_node = ups_node->next_cluster_node();
//                } while(paxos_head != ups_node);
//            }
//        }
//    }
//}

//mod pangtianze [Paxos Cluster.Flow.UPS] 20170817:b
//void ObUpsManager::reset_ups_read_percent()
void ObUpsManager::reset_ups_read_percent(const ObUpsFlowType flow_type)
//mod:e
{
  int32_t is_strong_consistent = (int32_t)worker_->get_root_server().get_config().is_strong_consistency_read;
  for(int64_t cluster_idx = 0; cluster_idx < use_cluster_num_;cluster_idx++)
  {
    ObUpsNode *cluster_head = find_cluster_head_node(cluster_idx);
    if( NULL== cluster_head)
    {
      YYSYS_LOG(WARN, "Find cluster:[%ld] head failed,", cluster_idx);
    }
    else
    {
      ObUpsNode *ups_node = cluster_head;
      do
      {
        for (int64_t ups_idx = 0; ups_idx < ups_node->ups_array_->count(); ++ups_idx)
        {
          if (UPS_MS_FLOW == flow_type || UPS_ALL_FLOW == flow_type) //add pangtianze [Paxos Cluster.Flow.UPS] 20170817:b:e
          {
            ups_node->ups_array_->at(ups_idx).ms_read_percentage_ = 0;
          }
          if (UPS_CS_FLOW == flow_type || UPS_ALL_FLOW == flow_type) //add pangtianze [Paxos Cluster.Flow.UPS] 20170817:b:e
          {
            ups_node->ups_array_->at(ups_idx).cs_read_percentage_ = 0;
          }
        }
        ups_node =ups_node->next_paxos_group_node();
      }while(ups_node != cluster_head);
    }

    int32_t master_read_percent = 100;
    int32_t slave_read_percent = 0;

    //[628]
    int32_t master_merge_percent = 100;
    int32_t slave_merge_percent = 0;

    is_flow_control_by_ip_ = false;

    if(is_cluster_offline(cluster_idx))
    {
      YYSYS_LOG(INFO, "cluster[%ld] has been taken offline", cluster_idx);
    }
    else
    {//get the num of ups that belong to one obupsnode;
      ObUpsNode *cluster_head = find_cluster_head_node(cluster_idx);
      ObUpsNode *ups_node = cluster_head;
      if( NULL == ups_node)
      {
        YYSYS_LOG(WARN, "Find cluster:[%ld] head failed,", cluster_idx);
      }
      else
      {
        do
        {
          int32_t ups_count = get_ups_count_in_one_node(ups_node);
          if (ups_count < 1)
          {
            YYSYS_LOG(DEBUG, "No active UpdateServer");
          }
          else if (ups_count == 1) // only one ups
          {
            master_read_percent= 100;
            slave_read_percent= 100;

            //[628]
            master_merge_percent = 100;
            slave_merge_percent = 100;

            set_ups_read_percent(master_read_percent,slave_read_percent, master_merge_percent, slave_merge_percent, ups_node, flow_type //add:pangtianze [Paxos Cluster.Flow.UPS] 20170817:b
                                 );
          }
          else
          {
            if (node_has_master(ups_node))
            {
              master_read_percent = master_ups_read_percentage_;
              slave_read_percent= (100 - master_read_percent)/(ups_count -1);

              //[628]
              master_merge_percent = master_ups_merge_percentage_;
              slave_merge_percent = (100 - master_merge_percent)/(ups_count -1);

              if(is_strong_consistent == 1)
              {
                slave_read_percent = 0;
              }
              set_ups_read_percent(master_read_percent,slave_read_percent, master_merge_percent, slave_merge_percent, ups_node, flow_type //add:pangtianze [Paxos Cluster.Flow.UPS] 20170817:b
                                   );


            }
            else
            {
              //master_read_percent = master_read_percent/ups_count;//not used
              //slave_read_percent = slave_read_percent/ups_count;
              slave_read_percent = 100 /ups_count;

              //[628]
              slave_merge_percent = 100 / ups_count;

              if(is_strong_consistent == 1)
              {
                slave_read_percent = 0;
              }
              set_ups_read_percent(master_read_percent,slave_read_percent, master_merge_percent, slave_merge_percent, ups_node, flow_type //add:pangtianze [Paxos Cluster.Flow.UPS] 20170817:b
                                   );
            }
          }

          ups_node =ups_node->next_paxos_group_node();
        }while(ups_node != cluster_head);
      }

    }
  }
}

int32_t ObUpsManager::get_ups_count_in_one_node(ObUpsNode * ups_node)const
{
  int32_t count = 0;
  if( NULL == ups_node)
  {
    YYSYS_LOG(WARN, "Find cluster head failed ");
  }
  else
  {
    for (int64_t ups_idx = 0; ups_idx < ups_node->ups_array_->count(); ++ups_idx)
    {
      if (UPS_STAT_OFFLINE != ups_node->ups_array_->at(ups_idx).stat_ &&
          UPS_STAT_NOTSYNC != ups_node->ups_array_->at(ups_idx).stat_)
      {
        count++;
      }
    }
  }
  return count;
}

//add pangtianze [Paxos inner table revise] 20170826
void ObUpsManager::get_task_array_for_inner_table_revise(
    ObSEArray<ObRootAsyncTaskQueue::ObSeqTask, OB_MAX_UPS_COUNT>& task_arr)
{
  yysys::CThreadGuard guard(&ups_array_mutex_);
  if (initialized_)
  {
    ObUpsNode *paxos_node = NULL, *cluster_node = NULL;
    if (NULL != (paxos_node = link_head_))
    {
      do
      {
        cluster_node = paxos_node;
        do
        {
          common::ObArray<ObUps> *&ups_array = cluster_node->ups_array_;
          for (int64_t ups_idx = 0; ups_idx < ups_array->count(); ++ups_idx)
          {
            if (UPS_STAT_OFFLINE != ups_array->at(ups_idx).stat_)
            {
              ObRootAsyncTaskQueue::ObSeqTask task;
              task.type_ = SERVER_ONLINE;
              task.role_ = OB_UPDATESERVER;
              task.server_ = ups_array->at(ups_idx).addr_;
              task.paxos_id_ = ups_array->at(ups_idx).paxos_id_;
              task.inner_port_ = ups_array->at(ups_idx).inner_port_;
              // set as slave
              task.server_status_ = 2;
              // set as master
              if (ups_array->at(ups_idx).stat_ == UPS_STAT_MASTER)
              {
                task.server_status_ = 1;
              }
              task_arr.push_back(task);
            }
          }
          cluster_node = cluster_node->next_cluster_node();
        } while (paxos_node != cluster_node);
        paxos_node = paxos_node->next_paxos_group_node();
      } while (link_head_ != paxos_node);
    }
  }
}
//add:e

void ObUpsManager::set_ups_read_percent(const int32_t master_read_percent,
                                        const int32_t slave_read_percent,
                                        const int32_t master_merge_percent,
                                        const int32_t slave_merge_percent,
                                        ObUpsNode * ups_node, const ObUpsFlowType flow_type //add pangtianze [Paxos Cluster.Flow.UPS] 20170817:b:e
                                        )
{
  if( NULL == ups_node)
  {
    YYSYS_LOG(WARN, "Find cluster head failed ");
  }
  else
  {
    for (int64_t ups_idx = 0; ups_idx < ups_node->ups_array_->count(); ++ups_idx)
    {
      if (UPS_STAT_SYNC == ups_node->ups_array_->at(ups_idx).stat_)
      {
        if (UPS_MS_FLOW == flow_type || UPS_ALL_FLOW == flow_type) //add pangtianze [Paxos Cluster.Flow.UPS] 20170817:b:e
        {
          ups_node->ups_array_->at(ups_idx).ms_read_percentage_ = slave_read_percent;
        }
        if (UPS_CS_FLOW == flow_type || UPS_ALL_FLOW == flow_type) //add pangtianze [Paxos Cluster.Flow.UPS] 20170817:b:e
        {
          ups_node->ups_array_->at(ups_idx).cs_read_percentage_ = slave_merge_percent;
        }
      }
      else if (UPS_STAT_MASTER == ups_node->ups_array_->at(ups_idx).stat_)
      {
        if (UPS_MS_FLOW == flow_type || UPS_ALL_FLOW == flow_type) //add pangtianze [Paxos Cluster.Flow.UPS] 20170817:b:e
        {
          ups_node->ups_array_->at(ups_idx).ms_read_percentage_ = master_read_percent;
        }
        if (UPS_CS_FLOW == flow_type || UPS_ALL_FLOW == flow_type) //add pangtianze [Paxos Cluster.Flow.UPS] 20170817:b:e
        {
          ups_node->ups_array_->at(ups_idx).cs_read_percentage_ = master_merge_percent;
        }
      }
      else if (UPS_STAT_NOTSYNC == ups_node->ups_array_->at(ups_idx).stat_)
      {
        if (UPS_MS_FLOW == flow_type || UPS_ALL_FLOW == flow_type) //add pangtianze [Paxos Cluster.Flow.UPS] 20170817:b:e
        {
          ups_node->ups_array_->at(ups_idx).ms_read_percentage_ = 0;
        }
        if (UPS_CS_FLOW == flow_type || UPS_ALL_FLOW == flow_type) //add pangtianze [Paxos Cluster.Flow.UPS] 20170817:b:e
        {
          ups_node->ups_array_->at(ups_idx).cs_read_percentage_ = 0;
        }
      }
    }
  }
}

bool ObUpsManager::node_has_master(ObUpsNode *ups_node) const
{
  bool is_master = false;
  if( NULL == ups_node)
  {
    YYSYS_LOG(WARN, "Find cluster head failed ");
  }
  else
  {
    for (int64_t ups_idx = 0; ups_idx < ups_node->ups_array_->count(); ++ups_idx)
    {
      if( UPS_STAT_MASTER == ups_node->ups_array_->at(ups_idx).stat_)
      {
        is_master = true;
        break;
      }
    }
  }
  return is_master;
}
//mod e

int ObUpsManager::register_ups(const common::ObServer &addr, int32_t inner_port, int64_t log_seq_num,
                               int64_t lease, const char *server_version, const int64_t cluster_idx,
                               const int64_t paxos_idx, /*const*/ int64_t last_frozen_version)
{
  int ret = OB_SUCCESS;
  if (OB_MAX_CLUSTER_COUNT < use_cluster_num_ || MAX_UPS_COUNT_ONE_CLUSTER < use_paxos_num_)
  {
    ret = OB_INVALID_CONFIG;
    YYSYS_LOG(ERROR, "rs config use_cluster_num %ld  must in [1, %ld] use_paxos_num %ld must in [1, %ld], ret=%d",
              use_cluster_num_, OB_MAX_CLUSTER_COUNT, use_paxos_num_, MAX_UPS_COUNT_ONE_CLUSTER, ret);
  }
  else if (use_cluster_num_ <= cluster_idx || 0 > cluster_idx || use_paxos_num_ <= paxos_idx || 0 > paxos_idx)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(ERROR, "Ups:%s Cluster_idx:%ld must in [0, %ld) and Paxos_idx:%ld must in [0, %ld), ret=%d",
              addr.to_cstring(), cluster_idx, use_cluster_num_, paxos_idx, use_paxos_num_, ret);
  }
  else
  {
    yysys::CThreadGuard guard(&ups_array_mutex_);
    int64_t ups_idx = INVALID_INDEX;
    ObUpsNode *ups_node = NULL;
    //mod liuzy [MultiUPS] [UPS_Manager] 20160412:b
    //    if (did_ups_exist(addr))
    if (INVALID_INDEX != (ups_idx = find_ups_index(addr, ups_node, true)))
      //mod 20160412:e
    {
      if (NULL != ups_node && cluster_idx == ups_node->cluster_idx_ &&
          paxos_idx == ups_node->paxos_idx_ && is_ups_idx_valid(ups_idx, ups_node))
      {
        // if (!initialized_)
        // {
        //   ret = OB_ALREADY_REGISTERED;
        // }
        if (UPS_STAT_OFFLINE == ups_node->ups_array_->at(ups_idx).stat_
            && ((1 != paxos_ups_quorum_scales_[paxos_idx]) ? get_ups_count(paxos_idx) >= paxos_ups_quorum_scales_[paxos_idx]
            : get_ups_count(paxos_idx) >= 2 ))
        {
          ret = OB_CURRENT_PAXOS_HAS_ENOUGH_UPS;
          YYSYS_LOG(ERROR, "Current paxos group:[%ld] has enough ups, ups count:[%ld], max ups count:[%ld], ret=%d"
                    , paxos_idx, get_ups_count(paxos_idx),
                    (1 != paxos_ups_quorum_scales_[paxos_idx]) ?  static_cast<int64_t>(paxos_ups_quorum_scales_[paxos_idx]) : 2,
                    ret  );
        }
        else
        {
          if (UPS_STAT_OFFLINE == ups_node->ups_array_->at(ups_idx).stat_)
          {
            ups_node->ups_array_->at(ups_idx).log_seq_num_ = log_seq_num;
            ups_node->ups_array_->at(ups_idx).lease_ = yysys::CTimeUtil::getTime() + lease_duration_us_;
            ups_node->ups_array_->at(ups_idx).did_renew_received_ = true;
            ups_node->ups_array_->at(ups_idx).cs_read_percentage_ = 0;
            ups_node->ups_array_->at(ups_idx).ms_read_percentage_ = 0;
            ups_node->ups_array_->at(ups_idx).last_frozen_version_ = last_frozen_version;
            ups_node->ups_array_->at(ups_idx).is_active_ = false;
            change_ups_stat(ups_node->cluster_idx_, ups_node->paxos_idx_, ups_idx, UPS_STAT_NOTSYNC);
            refresh_inner_table(SERVER_ONLINE, ups_node->ups_array_->at(ups_idx), "null");
          }

          check_slave_frozen_version(last_frozen_version, paxos_idx, addr);

          //mod peiouya [MultiUPS] [UPS_Manage_Function] 20150815:b
          //if (OB_SUCCESS != (ret = check_ups_info_valid (last_frozen_version, paxos_idx)))
          if (initialized_ &&
                  OB_SUCCESS != (ret = check_ups_info_valid(last_frozen_version, paxos_idx, is_ups_master(addr))))
            //mod 20150815:e
          {
            if (OB_INVALID_FROZEN_VERSION == ret)
            {
              YYSYS_LOG(ERROR, "cur ups:%s last_frozen_version:%ld is invalid", addr.to_cstring(), last_frozen_version);
            }
            else
            {
              //nothing todo
            }
          }
          //mod 20150811:e
          else
          {
            ret = OB_ALREADY_REGISTERED;
          }
        }
      }
      else
      {
        ret = OB_CONFLICT_VALUE;
      }
      if (OB_INVALID_FROZEN_VERSION == ret
          //del liuzy [MultiUPS] [UPS_Manager] [BugFix]20160516:b
          /*Exp: need not take ups offline, when it's value is conflict*/
          //          || OB_CONFLICT_VALUE == ret
          //del 20160516:e
          //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160302:b
          || is_paxos_group_offline(paxos_idx) || is_cluster_offline(cluster_idx)
          //add 20160302:e
          )
      {
        change_ups_stat(cluster_idx, paxos_idx, ups_idx, UPS_STAT_OFFLINE);
        ObUps ups(ups_node->ups_array_->at(ups_idx));
        refresh_inner_table(SERVER_OFFLINE, ups, "null");
        //mod liuzy [MultiUPS] [UPS_Manager] 20160421:b
        //        ups_node->ups_array_->at(ups_idx).reset();
        //ups_node->ups_array_->remove(ups_idx);
        //mod 20160421:e
        //mod lqc [MultiUPS with paxos]
        // reset_ups_read_percent_by_paxos_id(paxos_idx);  //add peiouya [MultiUPS] [UPS_Manage_Function] 20150811
        //mod pangtianze [Paxos] 20170814:b
        //reset_ups_read_percent();
        if (!is_flow_control_by_ip_)
        {
          reset_ups_read_percent();
        }
        //mod:e
        // mod e
      }
      //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160302:b
      if (is_paxos_group_offline(paxos_idx))
      {
        ret = OB_CURRENT_PAXOS_GROUP_OFFLINE;
        YYSYS_LOG(INFO, "paxos group[%ld] has been taken offline, ret=%d", paxos_idx, ret);
      }
      //add 20160302:e
      //add liuzy [MultiUPS] [take_cluster_offline_interface] 20160325:b
      else if (is_cluster_offline(cluster_idx))
      {
        ret = OB_CURRENT_CLUSTER_OFFLINE;
        YYSYS_LOG(INFO, "cluster[%ld] has been taken offline, ret=%d", cluster_idx, ret);
      }
      //add 20160325:e
    }
    //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160325:b
    else if (OB_SUCCESS != (ret = check_paxos_and_cluster_vaild(cluster_idx, paxos_idx)))
    {
      YYSYS_LOG(WARN, "cluster[%ld] or paxos group[%ld] has been taken offline, ret=%d",cluster_idx, paxos_idx, ret);
    }
    //add 20160325:e
    else if ((1 != paxos_ups_quorum_scales_[paxos_idx]) ? get_ups_count(paxos_idx) >= paxos_ups_quorum_scales_[paxos_idx]
        : get_ups_count(paxos_idx) >= 2  )
    {
      ret = OB_CURRENT_PAXOS_HAS_ENOUGH_UPS;
      YYSYS_LOG(ERROR, "Current paxos group: [%ld] has enough ups, ups count:[%ld], max ups count:[%ld], ret=%d",
                paxos_idx, get_ups_count(paxos_idx),
                (1 != paxos_ups_quorum_scales_[paxos_idx]) ?  static_cast<int64_t>(paxos_ups_quorum_scales_[paxos_idx]) : 2,
                ret  );
    }
    //add liuzy [MultiUPS] [UPS_Manager] 20160721:b
    // else if (get_ups_count(paxos_idx) >= max_deployed_ups_count_ && 1 != max_deployed_ups_count_)
    // {
    //   ret = OB_CURRENT_PAXOS_HAS_ENOUGH_UPS;
    //   YYSYS_LOG(ERROR, "Current paxos group:[%ld] has enough ups, ups count:[%ld], max ups count:[%ld], ret=%d",
    //             paxos_idx, get_ups_count(paxos_idx), max_deployed_ups_count_, ret);
    // }
    //add 20160721:e
    else
    {
      //add chujiajia [Paxos rs_election] 20151217:b
      //if(lease > 0)
      //{
      //  is_old_ups_master_regist_[paxos_idx] = true;
      //  old_ups_master_[paxos_idx] = addr;
      //}
      //add:e
      ret = OB_INVALID_FROZEN_VERSION;
      int64_t now = yysys::CTimeUtil::getTime();
      ObUps new_ups;
      new_ups.addr_ = addr;
      new_ups.inner_port_ = inner_port;
      new_ups.log_seq_num_ = log_seq_num;
      new_ups.lease_ =  now + lease_duration_us_;
      new_ups.did_renew_received_ = true;
      new_ups.cs_read_percentage_ = 0;
      new_ups.ms_read_percentage_ = 0;
      new_ups.cluster_id_ = cluster_idx;
      new_ups.paxos_id_ = paxos_idx;
      new_ups.last_frozen_version_ = last_frozen_version;
      new_ups.stat_ = UPS_STAT_OFFLINE;
      new_ups.is_active_ = false;
      if (NULL != (ups_node = find_node(cluster_idx, paxos_idx)))
      {
        if (OB_SUCCESS != (ups_node->ups_array_->push_back(new_ups)))
        {
          YYSYS_LOG(ERROR, "Push new ups:[%s] manager node cluster:[%ld] paxos:[%ld] failed.",
                    new_ups.addr_.to_cstring(), ups_node->cluster_idx_, ups_node->paxos_idx_);
        }
        else
        {
          ups_idx = ups_node->ups_array_->count() - 1;
          refresh_inner_table(SERVER_ONLINE, new_ups, server_version);
          if (lease > now)
          {
            if (has_master(paxos_idx))
            {
              int64_t clu_idx = INVALID_INDEX, master_ups_idx = INVALID_INDEX;
              ObUpsNode *master_node = NULL;
              if (!check_master_idx_array_info_valid(master_node, clu_idx, paxos_idx, master_ups_idx))
              {
                YYSYS_LOG(WARN, "The master ups info: cluster_idx:[%ld], paxos_idx:[%ld], ups_idx[%ld] is invalid,",
                          clu_idx, paxos_idx, master_ups_idx);
                YYSYS_LOG(WARN, "ups claimed to have the master lease but we ignore, addr=%s lease=%ld",
                          addr.to_cstring(), lease);
              }
              else
              {
                YYSYS_LOG(WARN, "ups claimed to have the master lease but we ignore, addr=%s lease=%ld master=%s",
                          addr.to_cstring(), lease, master_node->ups_array_->at(master_ups_idx).addr_.to_cstring());
              }
              // ret = OB_CONFLICT_VALUE;
              ret = OB_UPS_MASTER_EXISTS;
            }
            else
            {
              if (!initialized_)
              {
                change_ups_stat(cluster_idx, paxos_idx, ups_idx, UPS_STAT_MASTER);
                atomic_exchange(reinterpret_cast<volatile uint64_t*>
                                (&(master_idx_array_new_[paxos_idx].first)), cluster_idx);
                atomic_exchange(reinterpret_cast<volatile uint64_t*>
                                (&(master_idx_array_new_[paxos_idx].second)), ups_idx);
                refresh_inner_table(ROLE_CHANGE, ups_node->ups_array_->at(ups_idx), "null");
                ret = OB_SUCCESS;
              }
              else
              {
                //mod peiouya [MultiUPS] [UPS_Manage_Function] 20150815:b
                //if (OB_SUCCESS != (ret = check_ups_info_valid (last_frozen_version, paxos_idx)))
                if (OB_SUCCESS != (ret = check_ups_info_valid(last_frozen_version, paxos_idx, true)))
                  //mod 20150815:e
                {
                  if (OB_INVALID_FROZEN_VERSION == ret)
                  {
                    YYSYS_LOG(ERROR, "cur ups:%s last_frozen_version:%ld is invalid",
                              addr.to_cstring(), last_frozen_version);
                  }
                  else
                  {
                    //nothing todo
                  }
                }
                //mod 20150811:e
                else
                {
                  change_ups_stat(cluster_idx, paxos_idx, ups_idx, UPS_STAT_MASTER);
                  atomic_exchange(reinterpret_cast<volatile uint64_t*>
                                  (&(master_idx_array_new_[paxos_idx].first)), cluster_idx);
                  atomic_exchange(reinterpret_cast<volatile uint64_t*>
                                  (&(master_idx_array_new_[paxos_idx].second)), ups_idx);
                  refresh_inner_table(ROLE_CHANGE, ups_node->ups_array_->at(ups_idx), "null");
                  ret = OB_SUCCESS;
                }
              }
            }
          }
          else
          {
            if (!initialized_)
            {
              change_ups_stat(cluster_idx, paxos_idx, ups_idx, UPS_STAT_NOTSYNC);
              ret = OB_SUCCESS;
            }
            else
            {
              //mod peiouya [MultiUPS] [UPS_Manage_Function] 20150815:b
              //if (OB_SUCCESS != (ret = check_ups_info_valid (last_frozen_version, paxos_idx)))
              if (OB_SUCCESS != (ret = check_ups_info_valid(last_frozen_version, paxos_idx, false)))
                //mod 20150815:e
              {
                if (OB_INVALID_FROZEN_VERSION == ret)
                {
                  YYSYS_LOG(ERROR, "cur ups:%s last_frozen_version:%ld is invalid",
                            addr.to_cstring(), last_frozen_version);
                }
                else
                {
                  //nothing todo
                }
              }
              //mod 20150811:e
              else
              {
                change_ups_stat(cluster_idx, paxos_idx, ups_idx, UPS_STAT_NOTSYNC);
                ret = OB_SUCCESS;
              }
            }
          }
        }
      }
      else
      {
        ret = OB_UPS_MANAGER_NODE_INEXISTENT;
        YYSYS_LOG(WARN, "Find ups manager node failed, cluster:[%ld], paxos:[%ld]", cluster_idx, paxos_idx);
      }
      YYSYS_LOG(INFO, "ups register, addr=%s inner_port=%d lsn=%ld lease=%ld"
                " cluster_idx=%ld paxos_idx=%ld last_frozen_version=%ld",
                addr.to_cstring(), inner_port, log_seq_num, lease, cluster_idx, paxos_idx, last_frozen_version);
      //mod peiouya [MultiUPS] [UPS_Manage_Function] 20150811:b
      //reset_ups_read_percent_by_paxos_id(paxos_id);
      if (OB_SUCCESS == ret)
      {
        //mod lqc [MultiUPS with paxos]
        // reset_ups_read_percent_by_paxos_id(paxos_idx);  //add peiouya [MultiUPS] [UPS_Manage_Function] 20150811
        //mod pangtianze [Paxos] 20170814:b
        //reset_ups_read_percent();
        if (!is_flow_control_by_ip_)
        {
          reset_ups_read_percent();
        }
        //mod:e
        // mod e
      }
      //mod 20150811:e
    }
  }
  if (REACH_TIME_INTERVAL(PRINT_INTERVAL))
  {
    print_ups_registered();//add liuzy [MultiUPS] [UPS_Manager] 20151221: debug
  }
  return ret;
}
//mod 20151221:e

//mod liuzy [MultiUPS] [UPS_Manager] 20160104:b
//mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
////mod peiouya [MultiUPS] [PARTITION_LOCK_FUCTION] 20150601:b
////int ObUpsManager::renew_lease(const common::ObServer &addr, ObUpsStatus stat, const ObiRole &obi_role,
////                              const int64_t last_frozen_version, const bool is_active)
//int ObUpsManager::renew_lease(const common::ObServer &addr, ObUpsStatus stat, const ObiRole &obi_role,
//                              const int64_t last_frozen_version, const bool is_active, const bool partition_lock_flag)
////mod 20150601:e
int ObUpsManager::renew_lease(const common::ObServer &addr, ObUpsStatus stat,
                              int64_t last_frozen_version, const bool is_active, const bool partition_lock_flag
                              //add chujiajia [Paxos rs_election] 20151119:b
                              , const int64_t &quorum_scale
                              //add:e
                              ,const int64_t minor_freeze_stat
                              )
{
  int ret = OB_SUCCESS;

  yysys::CThreadGuard guard(&ups_array_mutex_);
  ObUpsNode *ups_node = NULL;
  int64_t ups_idx = INVALID_INDEX;
  int64_t paxos_idx = INVALID_INDEX;
  int64_t cluster_idx = INVALID_INDEX;
  if (INVALID_INDEX == (ups_idx = find_ups_index(addr, ups_node)))
  {
    ret = OB_CONFLICT_VALUE;
    YYSYS_LOG(WARN, "not registered ups, addr=%s, ret=%d", addr.to_cstring(), ret);
  }
  else if (NULL == ups_node)
  {
    ret = OB_UPS_MANAGER_NODE_INEXISTENT;
    YYSYS_LOG(WARN, "Find node addr:[%s] failed, ret=%d.", addr.to_cstring(), ret);
  }
  else if (!is_ups_idx_valid(ups_idx, ups_node))
  {
    paxos_idx = ups_node->paxos_idx_;
    cluster_idx = ups_node->cluster_idx_;
    YYSYS_LOG(WARN, "cur ups=%s is invalid , not belong to cur cluster", addr.to_cstring());
    change_ups_stat(cluster_idx, paxos_idx, ups_idx, UPS_STAT_OFFLINE);
    ObUps ups(ups_node->ups_array_->at(ups_idx));
    refresh_inner_table(SERVER_OFFLINE, ups, "null");
    //mod liuzy [MultiUPS] [UPS_Manager] 20160421:b
    //    ups_node->ups_array_->at(ups_idx).reset_except_addr_and_ipt();
    //ups_node->ups_array_->remove(ups_idx);
    //mod 20160421:e
    //mod lqc [MultiUPS with paxos]
    // reset_ups_read_percent_by_paxos_id(paxos_idx);  //add peiouya [MultiUPS] [UPS_Manage_Function] 20150811
    if (!is_flow_control_by_ip_)
    {
      reset_ups_read_percent();
    }
    // mod e
    ret = OB_CONFLICT_VALUE;
  }
  else
  {
    paxos_idx = ups_node->paxos_idx_;
    cluster_idx = ups_node->cluster_idx_;
    ret = OB_INVALID_FROZEN_VERSION;
    if (!initialized_)
    {
      ret = OB_SUCCESS;
    }
    else
    {
      check_slave_frozen_version(last_frozen_version, paxos_idx, addr);
      //mod peiouya [MultiUPS] [UPS_Manage_Function] 20150815:b
      //if (OB_SUCCESS != (ret = check_ups_info_valid (last_frozen_version, paxos_idx)))
      if (OB_SUCCESS != (ret = check_ups_info_valid(last_frozen_version, paxos_idx, is_ups_master(addr))))
        //mod 20150815:e
      {
        if (OB_INVALID_FROZEN_VERSION == ret)
        {
          YYSYS_LOG(ERROR, "cur ups:%s last_frozen_version:%ld is invalid", addr.to_cstring(), last_frozen_version);
        }
        else
        {
          //nothing todo
        }
      }
      //mod 20150811:e
    }

    if (OB_SUCCESS == ret)
    {
      if (last_frozen_version > ups_node->ups_array_->at(ups_idx).last_frozen_version_)
      {
        YYSYS_LOG(INFO, "ups version upgrade:ups[%s], version[%ld]->version[%ld]",
                  to_cstring(ups_node->ups_array_->at(ups_idx).addr_), ups_node->ups_array_->at(ups_idx).last_frozen_version_, last_frozen_version);
      }
      ups_node->ups_array_->at(ups_idx).did_renew_received_ = true;
      //add chujiajia [Paxos rs_election] 20151119:b
      ups_node->ups_array_->at(ups_idx).quorum_scale_ = quorum_scale;
      //add:e
      ups_node->ups_array_->at(ups_idx).last_frozen_version_ = last_frozen_version;
      ups_node->ups_array_->at(ups_idx).is_active_ = is_active;
      //add peiouya [MultiUPS] [PARTITION_LOCK_FUCTION] 20150601:b
      root_server_->set_partition_lock_flag_for_ups(partition_lock_flag, last_frozen_version);
      //add 20150601:e
      YYSYS_LOG(DEBUG, "renew lease, addr=%s stat=%d", addr.to_cstring(), stat);
      ups_node->ups_array_->at(ups_idx).minor_freeze_stat_ = minor_freeze_stat;
      if ((UPS_STAT_SYNC == stat || UPS_STAT_NOTSYNC == stat)
          && (UPS_STAT_SYNC == ups_node->ups_array_->at(ups_idx).stat_
              || UPS_STAT_NOTSYNC == ups_node->ups_array_->at(ups_idx).stat_))
      {
        if (stat !=ups_node->ups_array_->at(ups_idx).stat_)
        {
          change_ups_stat(cluster_idx, paxos_idx, ups_idx, stat);
          if (!is_flow_control_by_ip_)
          {
            //mod lqc [MultiUPS with paxos]
            // reset_ups_read_percent_by_paxos_id(paxos_idx);  //add peiouya [MultiUPS] [UPS_Manage_Function] 20150811
            reset_ups_read_percent();
            // mod e
          }
        }
      }
    }
    else
    {
      change_ups_stat(cluster_idx, paxos_idx, ups_idx, UPS_STAT_OFFLINE);
      ObUps ups(ups_node->ups_array_->at(ups_idx));
      refresh_inner_table(SERVER_OFFLINE, ups, "null");
      //mod liuzy [MultiUPS] [UPS_Manager] 20160421:b
      //      ups_node->ups_array_->at(ups_idx).reset_except_addr_and_ipt();
      //ups_node->ups_array_->remove(ups_idx);
      //mod 20160421:e
      //mod lqc [MultiUPS with paxos]
      // reset_ups_read_percent_by_paxos_id(paxos_idx);  //add peiouya [MultiUPS] [UPS_Manage_Function] 20150811
      if (!is_flow_control_by_ip_)
      {
        reset_ups_read_percent();
      }
      // mod e
    }
  }
  return ret;
}
//mod 20150701:e

int ObUpsManager::slave_failure(const common::ObServer &addr, const common::ObServer &slave_addr)
{
  int ret = OB_SUCCESS;
  yysys::CThreadGuard guard(&ups_array_mutex_);

  ObUpsNode *ups_node = NULL;
  int64_t ups_idx = INVALID_INDEX;
  int64_t paxos_idx = INVALID_INDEX;
  int64_t cluster_idx = INVALID_INDEX;
  if (!is_ups_master(addr))
  {
    YYSYS_LOG(WARN, "ups not exist or not master, addr=%s", addr.to_cstring());
    ret = OB_NOT_REGISTERED;
  }
  else if (INVALID_INDEX == (ups_idx = find_ups_index(slave_addr, ups_node)))
  {
    YYSYS_LOG(WARN, "slave ups not exist, addr=%s", slave_addr.to_cstring());
    ret = OB_ENTRY_NOT_EXIST;
  }
  else if (NULL == ups_node)
  {
    YYSYS_LOG(WARN, "Find node addr:[%s] failed.", addr.to_cstring());
    ret = OB_UPS_MANAGER_NODE_INEXISTENT;
  }
  else
  {
    YYSYS_LOG(INFO, "ups master reporting slave ups failure, slave=%s", slave_addr.to_cstring());
    paxos_idx = ups_node->paxos_idx_;
    cluster_idx = ups_node->cluster_idx_;
    change_ups_stat(cluster_idx, paxos_idx, ups_idx, UPS_STAT_OFFLINE);
    ObUps ups(ups_node->ups_array_->at(ups_idx));
    refresh_inner_table(SERVER_OFFLINE, ups, "null");
    //mod liuzy [MultiUPS] [UPS_Manager] 20160421:b
    //      ups_node->ups_array_->at(ups_idx).reset();
    //ups_node->ups_array_->remove(ups_idx);
    //mod 20160421:e
    //mod lqc [MultiUPS with paxos]
    // reset_ups_read_percent_by_paxos_id(paxos_idx);  //add peiouya [MultiUPS] [UPS_Manage_Function] 20150811
    //mod pangtianze [Paxos] 20170814:b
    //reset_ups_read_percent();
    if (!is_flow_control_by_ip_)
    {
      reset_ups_read_percent();
    }
    //mod:e
    // mod e
  }
  return ret;
}
//mod 20160104:e

//mod liuzy [MultiUPS] [UPS_Manager] 20160105:b
int ObUpsManager::get_ups_master_by_paxos_id(const int64_t& paxos_idx, ObUps &ups_master) const
{
  int ret = OB_ENTRY_NOT_EXIST;
  yysys::CThreadGuard guard(&ups_array_mutex_);

  if (initialized_ && has_master(paxos_idx) && 0 <= paxos_idx && use_paxos_num_ > paxos_idx)
  {
    ObUpsNode *master_node = NULL;
    int64_t cluster_idx = INVALID_INDEX, master_ups_idx = INVALID_INDEX;
    if (!check_master_idx_array_info_valid(master_node, cluster_idx, paxos_idx, master_ups_idx))
    {
      YYSYS_LOG(WARN, "The master ups info: cluster idx:[%ld] paxos idx:[%ld] ups idx:[%ld] is invalid,",
                cluster_idx, master_ups_idx, paxos_idx);
    }
    else if (master_node->ups_array_->at(master_ups_idx).is_active_
             && (OB_UPS_START_MAJOR_VERSION - 1 <= master_node->ups_array_->at(master_ups_idx).last_frozen_version_))
    {
      ups_master = master_node->ups_array_->at(master_ups_idx);
      ret = OB_SUCCESS;
    }
    //add liuzy [MultiUPS] [get_online_ups_failed] 20160728:b
    else
    {
      if (REACH_TIME_INTERVAL(PRINT_INTERVAL_CHECK))
      {
        YYSYS_LOG(WARN, "paxos:[%ld] master ups:[%s] is:[%s], master ups last frozen vers:[%ld]",
                  paxos_idx, master_node->ups_array_->at(master_ups_idx).addr_.to_cstring(),
                  master_node->ups_array_->at(master_ups_idx).is_active_ ? "active" : "down",
                  master_node->ups_array_->at(master_ups_idx).last_frozen_version_);
      }
    }
    //add 20160728:e
  }
  return ret;
}
int ObUpsManager::get_sys_table_ups_master(ObUps &ups_master) const
{
  //mod peiouya [MultiUPS] [UPS_Manage_Function] 20150729:b
  //int ret = OB_ENTRY_NOT_EXIST;
  ////mod peiouya [MultiUPS] [UPS_Manage_Function] 20150729:b
  ////ret = get_ups_master_by_paxos_id(sys_table_paxos_id_, ups_master);
  //ret = get_ups_master_by_paxos_id(SYS_TABLE_PAXOS_ID, ups_master);
  ////mod 20150729:e
  //return ret;
  return get_ups_master_by_paxos_id(SYS_TABLE_PAXOS_ID, ups_master);
  //mod 20150729:e
}
/*  //uncertainty 
int ObUpsManager::set_ups_config(int32_t read_master_ups_percentage)
{
  int ret = OB_SUCCESS;
  if ((-1 != read_master_ups_percentage)
      && (0 > read_master_ups_percentage
        || 100 < read_master_ups_percentage))
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(WARN, "invalid param, read_master_ups_percentage=%d, ret=%d", read_master_ups_percentage, ret);
  }
  else
  {
    YYSYS_LOG(INFO, "change ups config, read_master_ups_percentage=%d", read_master_ups_percentage);
    master_ups_read_percentage_ = read_master_ups_percentage;
    yysys::CThreadGuard guard(&ups_array_mutex_);
    reset_ups_read_percent();
  }
  return ret;
}
*/
/*   // uncertainty ups �Ѹ�д
int  ObUpsManager::get_random_paxos_id_for_group(int64_t& paxos_idx)
{
  int ret = OB_SUCCESS;
  common::ObArray<int64_t> paxos_array;
  {
    for (int64_t paxos_idx = 0; initialized_ && paxos_idx < use_paxos_num_; paxos_idx++)
    {
      if (0 < get_ups_count(paxos_idx))
      {
        paxos_array.push_back(paxos_idx);
      }
    }
  }
  if (0 < paxos_array.count())
  {
    if (OB_SUCCESS != (ret = paxos_array.at(static_cast<int64_t>(::random() % paxos_array.count()), paxos_idx)))
    {
      YYSYS_LOG(WARN, "fail to get one paxos id for group, err=%d", ret);
    }
  }
  return ret;
  }
  */
/*
int ObUpsManager::get_ups_list(common::ObUpsList &ups_list) const
{
  int ret = OB_SUCCESS;
  int count = 0;
  yysys::CThreadGuard guard(&ups_array_mutex_);
  if (!is_flow_control_by_ip_)
  {
    for (int32_t i = 0; i < MAX_UPS_COUNT && count < ups_list.MAX_UPS_COUNT; ++i)
    {
      if (UPS_STAT_OFFLINE != ups_array_[i].stat_
          && UPS_STAT_NOTSYNC != ups_array_[i].stat_)
      {
        ups_array_[i].convert_to(ups_list.ups_array_[count]);
        count++;
      }
    }
  }
  else //�����ip���������Ļ����򷵻��������ߵ�UPS
  {
    for (int32_t i = 0; i < MAX_UPS_COUNT && count < ups_list.MAX_UPS_COUNT; ++i)
    {
      if (UPS_STAT_OFFLINE != ups_array_[i].stat_)
      {
        ups_array_[i].convert_to(ups_list.ups_array_[count]);
        count++;
      }
    }
  }
  ups_list.ups_count_ = count;
  return ret;
}
*/
//mod lqc [MultiUPS] [merge with paxos] 20170519:b
void ObUpsManager::ups_array_reset()
{
  yysys::CThreadGuard guard(&ups_array_mutex_);
  for (int64_t paxos_idx = 0; paxos_idx < use_paxos_num_; ++paxos_idx)
  {
    ObUpsNode *paxos_head = NULL;
    if(initialized_)
    {
      if(NULL !=(paxos_head = find_paxos_head_node(paxos_idx)))
      {
        ObUpsNode *cluster_node = paxos_head;
        do
        {
          for (int64_t ups_idx = 0;ups_idx < cluster_node->ups_array_->count(); ++ups_idx)
          {
            cluster_node->ups_array_->at(ups_idx).reset();
          }
          cluster_node = cluster_node->next_cluster_node();
        }while(cluster_node != paxos_head);
      }
    }
  }//mod e
  ups_master_idx_ = -1;
  waiting_ups_finish_time_ = 0;
  master_master_ups_read_percentage_ = -1;
  is_strong_consistency_ = 0;
  is_flow_control_by_ip_ = false;
  //add pangtianze [Paxos Cluster.Flow.UPS] 20170817:b
  is_cs_flow_init_ = false;
  //add:e
  //reset
  //add hongchen [STAT_RESET_FIX] 20170828:b
  init_cluster_stat_array ();
  init_paxos_group_stat_array ();
  init_master_idx_array();
  init_freeze_paxos_group_stat_array();
  initialized_ = false;
  //add hongchen [STAT_RESET_FIX] 20170828:e
  last_change_ups_master_ = 0;

}
void ObUpsManager::print_ups_leader(char *buf, const int64_t buf_len,int64_t& pos)
{
  int ret = OB_SUCCESS;
  ObUpsList ups_list;
  get_master_ups_list(ups_list);
  if (0 >= ups_list.ups_count_)
  {
    ret = OB_ENTRY_NOT_EXIST;
    databuff_printf(buf, buf_len, pos, "leader updateserver do not exist ! ret=[%d]",ret);
  }
  else
  {
    databuff_printf(buf, buf_len, pos, "leader updateserver: ");
    for (int i = 0; i < ups_list.ups_count_; i++)
    {
      databuff_printf(buf, buf_len, pos, "[%s]", ups_list.ups_array_[i].addr_.to_cstring());
    }
  }
}
//add:e
int ObUpsManager::get_ups_list(common::ObUpsList &ups_list) const
{
  int ret = OB_SUCCESS;
  int32_t count = 0;
  yysys::CThreadGuard guard(&ups_array_mutex_);
  if (initialized_ /*!is_flow_control_by_ip_*/)//del pangtianze [Paxos Cluster.Flow.UPS] 20170809:b
  {
    int64_t rs_last_frozen_version = root_server_->get_last_frozen_version();
    if (OB_INVALID_VERSION == rs_last_frozen_version)
    {
      ret = OB_RS_INVALID_LAST_FROZEN_VERSION;
      YYSYS_LOG(ERROR, "after rs set report_flag:true, "
                "root_server_->get_last_frozen_version() must not be OB_INVALID_VERSION, ret=%d", ret);
      root_server_->kill_self();
    }
    else
    {
      ObUpsNode *paxos_node = NULL, *cluster_node = NULL;
      if (NULL != (paxos_node = link_head_))
      {
        do
        {
          cluster_node = paxos_node;
          do
          {
            common::ObArray<ObUps> *&ups_array = cluster_node->ups_array_;
            for (int64_t ups_idx = 0; ups_idx < ups_array->count(); ++ups_idx)
            {
              if (UPS_STAT_OFFLINE != ups_array->at(ups_idx).stat_
                  && UPS_STAT_NOTSYNC != ups_array->at(ups_idx).stat_
                  && (rs_last_frozen_version == ups_array->at(ups_idx).last_frozen_version_
                      || rs_last_frozen_version == ups_array->at(ups_idx).last_frozen_version_ - 1))
              {
                ups_array->at(ups_idx).convert_to(ups_list.ups_array_[count]);
                count++;
              }
            }
            cluster_node = cluster_node->next_cluster_node();
          } while (paxos_node != cluster_node);
          paxos_node = paxos_node->next_paxos_group_node();
        } while (link_head_ != paxos_node);
      }
      else
      {
        ret = OB_UPS_MANAGER_POINTER_ERROR;
        YYSYS_LOG(ERROR, "The head node of the ups linked list is NULL, ret=%d.", ret);
      }
    }
  }
  ups_list.ups_count_ = count;
  return ret;
}

int ObUpsManager::get_ups_list_for_cbtop(common::ObUpsList &ups_list) const
{
  int ret = OB_SUCCESS;
  int32_t count = 0;
  yysys::CThreadGuard guard(&ups_array_mutex_);
  if (initialized_)
  {
    int64_t rs_last_frozen_version = root_server_->get_last_frozen_version();
    if (OB_INVALID_VERSION == rs_last_frozen_version)
    {
      ret = OB_RS_INVALID_LAST_FROZEN_VERSION;
      YYSYS_LOG(WARN, "after rs set report_flag:true, "
                "root_server_->get_last_frozen_version() must not be OB_INVALID_VERSION, ret=%d", ret);
      root_server_->kill_self();
    }

    else
    {
      ObUpsNode *paxos_node = NULL, *cluster_node = NULL;
      if (NULL != (paxos_node = link_head_))
      {
        do
        {
          cluster_node = paxos_node;
          do
          {
            common::ObArray<ObUps> *&ups_array = cluster_node->ups_array_;
            for (int64_t ups_idx = 0 ; ups_idx < ups_array->count() ; ++ups_idx)
            {
              if (UPS_STAT_OFFLINE != ups_array->at(ups_idx).stat_)
              {
                ups_array->at(ups_idx).convert_to(ups_list.ups_array_[count]);
                count++;
              }
            }
            cluster_node = cluster_node->next_cluster_node();
          } while (paxos_node != cluster_node);
          paxos_node = paxos_node->next_paxos_group_node();
        } while (link_head_ != paxos_node);
      }
      else
      {
        ret = OB_UPS_MANAGER_POINTER_ERROR;
        YYSYS_LOG(WARN, "the head node of the ups linked list is null, ret = %d", ret);
      }
    }
  }
  ups_list.ups_count_ = count;
  return ret;
}

int ObUpsManager::get_ups_list_by_paxos_id(common::ObUpsList &ups_list, const int64_t& paxos_idx) const
{
  int ret = OB_SUCCESS;
  int32_t count = 0;
  ObUpsNode *paxos_head = NULL;
  yysys::CThreadGuard guard(&ups_array_mutex_);
  if (initialized_)
  {
    if (NULL != (paxos_head = find_paxos_head_node(paxos_idx)))
    {
      ObUpsNode *cluster_node = paxos_head;
      do
      {
        common::ObArray<ObUps> *&ups_array = cluster_node->ups_array_;
        for (int64_t ups_idx = 0; ups_idx < ups_array->count(); ++ups_idx)
        {
          int64_t rs_last_frozen_version = root_server_->get_last_frozen_version();
          if (OB_INVALID_VERSION == rs_last_frozen_version)
          {
            ret = OB_RS_INVALID_LAST_FROZEN_VERSION;
            YYSYS_LOG(ERROR, "after rs set report_flag:true, "
                      "root_server_->get_last_frozen_version() must not be OB_INVALID_VERSION, ret=%d", ret);
            root_server_->kill_self();
          }
          else
          {
            if (UPS_STAT_OFFLINE != ups_array->at(ups_idx).stat_
                && UPS_STAT_NOTSYNC != ups_array->at(ups_idx).stat_
                && (rs_last_frozen_version == ups_array->at(ups_idx).last_frozen_version_
                    || rs_last_frozen_version == ups_array->at(ups_idx).last_frozen_version_ - 1))
            {
              ups_array->at(ups_idx).convert_to(ups_list.ups_array_[count]);
              count++;
            }
          }
        }
        cluster_node = cluster_node->next_cluster_node();
      } while (cluster_node != paxos_head);
    }
    else
    {
      ret = OB_UPS_MANAGER_NODE_INEXISTENT;
      YYSYS_LOG(WARN, "Find paxos:[%ld] head failed, ret=%d.", paxos_idx, ret);
    }
  }
  ups_list.ups_count_ = count;
  return ret;
}

int ObUpsManager::grant_lease(bool did_force /*=false*/)
{
  int tmp_ret = OB_SUCCESS;
  int ret = OB_SUCCESS;
  //add pangtianze [MultiUPS] [merge with paxos] 20170518:b
  if(ObRoleMgr::MASTER == worker_->get_role_manager()->get_role())
  {
    //add:e
    for (int64_t idx = 0; idx < use_paxos_num_; ++idx)
    {
      tmp_ret = this->grant_lease_by_paxos_id(idx, did_force);
      if (OB_SUCCESS != tmp_ret)
      {
        ret = tmp_ret;
        YYSYS_LOG(WARN, "paxos [%ld] grant lease failed, ret=%d.", idx, ret);
      }
    }
  }
  return ret;
}
void ObUpsManager::get_master_ups_list(common::ObUpsList &ups_list) const
{
  int count = 0;
  //check all used paxos group has master ups
  bool is_all_use_paxos_group_live = true;
  ObUpsNode *master_node = NULL;
  int64_t cluster_idx = INVALID_INDEX, master_ups_idx = INVALID_INDEX;
  yysys::CThreadGuard guard(&ups_array_mutex_);
  if (initialized_)
  {
    for (int64_t paxos_idx = 0; paxos_idx < use_paxos_num_; ++paxos_idx)
    {
      //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160306:b
      if (!is_paxos_group_active(paxos_idx))
      {
        continue;
      }
      //add 20160306:e
      //mod liuzy [MultiUPS] [take_paxos_offline_interface] 20160306:b
      //      if (INVALID_INDEX == (cluster_idx = master_idx_array_new_[paxos_idx].first)
      //          || INVALID_INDEX == (master_ups_idx = master_idx_array_new_[paxos_idx].second))
      else if (INVALID_INDEX == (cluster_idx = master_idx_array_new_[paxos_idx].first)
               || INVALID_INDEX == (master_ups_idx = master_idx_array_new_[paxos_idx].second))
        //mod 20160306:e
      {
        YYSYS_LOG(WARN, "Master ups info of paxos[%ld] is invalid:"
                  " cluster idx:[%ld] and master ups idx:[%ld] should not be INVALID_INDEX",
                  paxos_idx, INVALID_INDEX == cluster_idx ? INVALID_INDEX : cluster_idx,
                  INVALID_INDEX == master_ups_idx ? INVALID_INDEX : master_ups_idx);
        YYSYS_LOG(WARN, "no master ups or not all paxos group has master.");
        is_all_use_paxos_group_live = false;
        break;
      }
      else if (!check_master_ups_valid(paxos_idx))
      {
        is_all_use_paxos_group_live = false;
        YYSYS_LOG(ERROR, "master ups frozen version invalid.");
        break;
      }
    }

    for (int64_t paxos_idx = 0; is_all_use_paxos_group_live && paxos_idx < use_paxos_num_; ++paxos_idx)
    {
      //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160306:b
      if (!is_paxos_group_active(paxos_idx))
      {
        continue;
      }
      //add 20160306:e
      //mod liuzy [MultiUPS] [take_paxos_offline_interface] 20160306:b
      //      if (!check_master_idx_array_info_valid(master_node, cluster_idx, paxos_idx, master_ups_idx))
      else if (!check_master_idx_array_info_valid(master_node, cluster_idx, paxos_idx, master_ups_idx))
        //mod 20160306:e
      {
        YYSYS_LOG(WARN, "The master ups info: cluster idx:[%ld], master ups idx:[%ld] of paxos[%ld] is invalid,",
                  cluster_idx, master_ups_idx, paxos_idx);
      }
      else
      {
        master_node->ups_array_->at(master_ups_idx).convert_to(ups_list.ups_array_[count]);
        ++count;
      }
    }
    ups_list.ups_count_ = count;
  }
}
void ObUpsManager::get_online_master_ups_list(common::ObUpsList &ups_list) const
{
  int count = 0;
  int64_t master_ups_idx = INVALID_INDEX;
  int64_t cluster_idx = INVALID_INDEX;
  ObUpsNode *master_node = NULL;
  yysys::CThreadGuard guard(&ups_array_mutex_);
  if (initialized_)
  {
    for (int64_t paxos_idx = 0; paxos_idx < use_paxos_num_; paxos_idx++)
    {
      //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160303:b
      if (!is_paxos_group_active(paxos_idx))
      {
        continue;
      }
      //add 20160303:e
      //mod liuzy [MultiUPS] [take_paxos_offline_interface] 20160303:b
      //      if (!check_master_ups_valid(paxos_idx))
      else if (!check_master_ups_valid(paxos_idx))
        //mod 20160303:e
      {
        YYSYS_LOG(WARN, "The master ups of paxos:[%ld] is invalid.", paxos_idx);
      }
      else if (!check_master_idx_array_info_valid(master_node, cluster_idx, paxos_idx, master_ups_idx))
      {
        YYSYS_LOG(WARN, "The master ups info: cluster idx:[%ld] paxos idx:[%ld] ups idx:[%ld] is invalid,",
                  cluster_idx, master_ups_idx, paxos_idx);
      }
      else if (UPS_STAT_MASTER != master_node->ups_array_->at(master_ups_idx).stat_)
      {
        YYSYS_LOG(WARN, "The master ups stat_[%d] is not UPS_STAT_MASTER:[%d].",
                  master_node->ups_array_->at(master_ups_idx).stat_, UPS_STAT_MASTER);
      }
      else
      {
        master_node->ups_array_->at(master_ups_idx).convert_to(ups_list.ups_array_[count]);
        count++;
      }
    }
    ups_list.ups_count_ = count;
  }
}

//[492]
void ObUpsManager::force_get_online_sys_table_ups_list(common::ObUpsList &ups_list) const
{
    yysys::CThreadGuard guard(&ups_array_mutex_);
    int ret = OB_SUCCESS;
    int32_t count = 0;
    ObUpsNode *paxos_head = NULL;
    int64_t paxos_idx = SYS_TABLE_PAXOS_ID;

    if (!is_paxos_group_active(paxos_idx))
    {
        YYSYS_LOG(WARN, "The paxos:[%ld] is not active.", paxos_idx);
    }
    else if(NULL != (paxos_head = find_paxos_head_node(paxos_idx)))
    {
        ObUpsNode *cluster_node = paxos_head;
        do
        {
            common::ObArray<ObUps> *&ups_array = cluster_node->ups_array_;
            for(int64_t ups_idx = 0; ups_idx < ups_array->count(); ++ups_idx)
            {
                if(UPS_STAT_OFFLINE != ups_array->at(ups_idx).stat_
                        &&UPS_STAT_NOTSYNC != ups_array->at(ups_idx).stat_)
                {
                    ups_array->at(ups_idx).convert_to(ups_list.ups_array_[count]);
                    count++;
                }
            }
            cluster_node = cluster_node->next_cluster_node();
        }while(cluster_node != paxos_head);
    }
    else
    {
        ret = OB_UPS_MANAGER_NODE_INEXISTENT;
        YYSYS_LOG(WARN, "Find paxos:[%ld] head failed, ret=%d.", paxos_idx, ret);
    }
    ups_list.ups_count_ = count;
}


void ObUpsManager::check_lease()
{
  bool did_select_new_master = false;
  ObUpsNode *paxos_head = NULL;
  ObUpsNode *cluster_node = NULL;
  int64_t paxos_idx = INVALID_INDEX;
  //mod hongchen [LEASE_BUGFIX] 20170905:b
  /*
  if (
      //add chujiajia[Paxos rs_election]20151020:b
      worker_->get_role_manager()->is_master() &&
      //add:e
      NULL != (paxos_head = link_head_))
  */
  if (ObRoleMgr::MASTER == worker_->get_role_manager()->get_role()
      && NULL != (paxos_head = link_head_))
    //mod hongchen [LEASE_BUGFIX] 20170905:e
  {
    do
    {
      cluster_node = paxos_head;
      paxos_idx = paxos_head->paxos_idx_;
      //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160303:b
      if (!is_paxos_group_offline(paxos_idx))
      {
        //add 20160303:e
        do
        {
          yysys::CThreadGuard guard(&ups_array_mutex_);
          int64_t now = yysys::CTimeUtil::getTime();

          common::ObArray<ObUps> *&ups_array = cluster_node->ups_array_;
          for (int64_t ups_idx = 0; ups_idx < ups_array->count(); ++ups_idx)
          {
            if (ups_array->at(ups_idx).addr_.is_valid()
                && (now > ups_array->at(ups_idx).lease_ + MAX_CLOCK_SKEW_US)
                && UPS_STAT_OFFLINE != ups_array->at(ups_idx).stat_)
            {
              YYSYS_LOG(WARN, "ups is offline, ups=%s lease=%ld lease_duration=%ld now=%ld",
                        ups_array->at(ups_idx).addr_.to_cstring(),
                        ups_array->at(ups_idx).lease_, lease_duration_us_, now);
              if (UPS_STAT_MASTER == ups_array->at(ups_idx).stat_)
              {
                atomic_exchange(reinterpret_cast<volatile uint64_t*>(&(master_idx_array_new_[paxos_idx].first)),
                                INVALID_INDEX);
                atomic_exchange(reinterpret_cast<volatile uint64_t*>(&(master_idx_array_new_[paxos_idx].second)),
                                INVALID_INDEX);
                did_select_new_master = true;
              }

              ObUps ups(ups_array->at(ups_idx));
              //del liuzy [MultiUPS] [UPS_Manager] 20160418:b
              //              reset_ups_read_percent_by_paxos_id(paxos_idx);
              //20160418:e
              //add hongchen [MISS_CHANGE_STAT] 20170907:b
              int64_t cluster_idx = cluster_node->cluster_idx_;
              if (ups_array->at(ups_idx).stat_ != UPS_STAT_OFFLINE)
              {
                change_ups_stat(cluster_idx, paxos_idx, ups_idx, UPS_STAT_OFFLINE);
                refresh_inner_table(SERVER_OFFLINE, ups, "null");
              }
              //mod liuzy [MultiUPS] [UPS_Manager] 20160421:b
              //              ups_array->at(ups_idx).reset();
              //ups_array->remove(ups_idx);
              //mod 20160421:e
              //mod lqc [MultiUPS with paxos]
              // reset_ups_read_percent_by_paxos_id(paxos_idx);  //add peiouya [MultiUPS] [UPS_Manage_Function] 20150811
              //mod pangtianze [Paxos] 20170814:b
              //reset_ups_read_percent();
              if (!is_flow_control_by_ip_)
              {
                reset_ups_read_percent();
              }
              //mod:e
              // mod e
            }
          }
          cluster_node = cluster_node->next_cluster_node();
        } while (paxos_head != cluster_node);

        //add pangtianze [Paxos ups_replication] 20150609:b
        if (!check_most_ups_registered(paxos_idx) && (ObRoleMgr::MASTER == worker_->get_role_manager()->get_role()))
        {
          int64_t now = yysys::CTimeUtil::getTime();
          if (now % 2000000 == 0) //every 2s
          {
            YYSYS_LOG(WARN, "no most ups online.");
          }
        }
        //add:e

        if (did_select_new_master)
        {
          yysys::CThreadGuard guard(&ups_array_mutex_); //add pangtianze [merge with paxos] 20170725:b:e
          int tmpret = select_new_ups_master(paxos_idx);
          if (OB_SUCCESS != tmpret)
          {
            YYSYS_LOG(WARN, "paxos_idx = [%ld] no master selected, err=%d", paxos_idx, tmpret);
          }
        }

        if (did_select_new_master && has_master(paxos_idx))
        {
          this->grant_lease_by_paxos_id(paxos_idx);
        }
        did_select_new_master = false;
      }//add liuzy 20160303
      paxos_head = paxos_head->next_paxos_group_node();
    } while (link_head_ != paxos_head);
  }
}
int64_t ObUpsManager::get_max_frozen_version()
{
  int64_t last_frozen_version = OB_INVALID_VERSION;
  ObUpsNode *paxos_head = NULL;
  ObUpsNode *cluster_node = NULL;
  yysys::CThreadGuard guard(&ups_array_mutex_);

  if (initialized_ && NULL != (paxos_head = link_head_))
  {
    do
    {
      cluster_node = paxos_head;
      do
      {
        common::ObArray<ObUps> *&ups_array = cluster_node->ups_array_;
        for (int64_t ups_idx = 0; ups_idx < ups_array->count(); ++ups_idx)
        {
          if (UPS_STAT_OFFLINE != ups_array->at(ups_idx).stat_
              && last_frozen_version < ups_array->at(ups_idx).last_frozen_version_)
          {
            last_frozen_version = ups_array->at(ups_idx).last_frozen_version_;
          }
        }
        cluster_node = cluster_node->next_cluster_node();
      } while (paxos_head != cluster_node);
      paxos_head = paxos_head->next_paxos_group_node();
    } while (link_head_ != paxos_head);
  }
  return last_frozen_version;
}
//mod 20160105:e

//mod liuzy [MultiUPS] [UPS_Manager] 20160106:b
void ObUpsManager::check_and_set_last_frozen_version()
{
  int64_t last_frozen_version = root_server_->get_last_frozen_version();
  if (initialized_) //&& check_all_ups_to_special_version(last_frozen_version)) //del peiouya 20150811
  {
    int64_t new_frozen_version = last_frozen_version + 1;
    if (check_all_ups_to_special_version(new_frozen_version))
    {
      root_server_->set_last_frozen_version(new_frozen_version);
      root_server_->force_heartbeat_all_servers();

      //[492]
      root_server_->refresh_sys_cur_data_version_info();

      //add liumz, [MultiUPS] [merge index code]20170310:b
      if(OB_UNLIKELY(NULL == worker_ || NULL == worker_->get_monitor()))
      {
        YYSYS_LOG(WARN, "should not be here, null pointer of worker_ or index_monitor_");
      }
      else
      {
        worker_->get_monitor()->start();
      }
      //add:e
    }
  }
}
void ObUpsManager::print(char* buf, const int64_t buf_len, int64_t &pos) const
{
  yysys::CThreadGuard guard(&ups_array_mutex_);
  int64_t now = yysys::CTimeUtil::getTime();
  int64_t use_paxos_num = use_paxos_num_;
  ObUpsNode *master_node = NULL;
  ObUpsNode *paxos_head = NULL;
  ObUpsNode *tmp_node = NULL;
  int64_t master_ups_idx = INVALID_INDEX;
  int64_t cluster_idx = INVALID_INDEX;
  for (int64_t paxos_idx = 0; paxos_idx < use_paxos_num; paxos_idx++)
  {
    if (is_paxos_group_offline(paxos_idx))
    {
      continue;
    }
    databuff_printf(buf, buf_len, pos, "{");
    if (!check_master_idx_array_info_valid(master_node, cluster_idx, paxos_idx, master_ups_idx))
    {
      YYSYS_LOG(WARN, "The master ups info: cluster idx:[%ld], master ups idx:[%ld] of paxos[%ld] is invalid,",
                cluster_idx, master_ups_idx, paxos_idx);
    }
    else if (is_master_lease_valid(paxos_idx))
    {
      databuff_printf(buf, buf_len, pos, "paxos_idx:%ld lease_left=%ld|",
                      paxos_idx, master_node->ups_array_->at(master_ups_idx).lease_ - now);
    }
    else if(!is_master_lease_valid(paxos_idx) && 0 < get_ups_count(paxos_idx))
    {
      databuff_printf(buf, buf_len, pos, "paxos_idx:%ld lease_left=null|", paxos_idx);
    }
    else
    {
      //do nothing
    }

    if (NULL != (paxos_head = find_paxos_head_node(paxos_idx)))
    {
      tmp_node = paxos_head;
      do
      {
        common::ObArray<ObUps> *&ups_array = tmp_node->ups_array_;
        for (int64_t ups_idx = 0; ups_idx < ups_array->count(); ++ups_idx)
        {
          if (UPS_STAT_OFFLINE != ups_array->at(ups_idx).stat_)
          {
            databuff_printf(buf, buf_len, pos, "%s(%d %s %d %d %lu %ld),",
                            ups_array->at(ups_idx).addr_.to_cstring(),
                            ups_array->at(ups_idx).inner_port_,
                            ups_stat_to_cstr(ups_array->at(ups_idx).stat_),
                            ups_array->at(ups_idx).ms_read_percentage_,
                            ups_array->at(ups_idx).cs_read_percentage_,
                            ups_array->at(ups_idx).log_seq_num_,
                            ups_array->at(ups_idx).cluster_id_);
          }
        }
        tmp_node = tmp_node->next_cluster_node();
      } while (tmp_node != paxos_head);
    }
    else
    {
      YYSYS_LOG(WARN, "Find paoxs:[%ld] head node failed.", paxos_idx);
    }
    databuff_printf(buf, buf_len, pos, "} ");
  }
}

void ObUpsManager::check_ups_master_exist()
{
  set_initialized();
  for (int64_t paxos_idx = 0; paxos_idx < use_paxos_num_; ++paxos_idx)
  {
    //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160306:b
    if (is_paxos_group_offline(paxos_idx))
    {
      continue;
    }
    //add 20160306:e
    {
      yysys::CThreadGuard guard(&ups_array_mutex_); //add pangtianze [merge with paxos] 20170725:b:e
      this->check_ups_master_exist_by_paxos_id(paxos_idx);
    }
  }
}
//mod 20160106:e

//mod liuzy [MultiUPS] [UPS_Manager] [private] 20160104:b
int ObUpsManager::grant_lease_by_paxos_id(const int64_t& paxos_idx, bool did_force /*=false*/)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  yysys::CThreadGuard guard(&ups_array_mutex_);
  ObServer master;
  int64_t now = yysys::CTimeUtil::getTime();
  int64_t master_ups_idx = INVALID_INDEX;
  int64_t cluster_idx = INVALID_INDEX;
  ObUpsNode *master_node = NULL, *paxos_head = NULL;

  if(has_master(paxos_idx))
  {
    if (!check_master_idx_array_info_valid(master_node, cluster_idx, paxos_idx, master_ups_idx))
    {
      ret = OB_UPS_MANAGER_IDX_INVALID;
      YYSYS_LOG(WARN, "The master ups info: cluster idx:[%ld] paxos idx:[%ld] ups idx:[%ld] is invalid, ret=%d",
                cluster_idx, master_ups_idx, paxos_idx, ret);
    }
    else
    {
      master = master_node->ups_array_->at(master_ups_idx).addr_;
    }
    YYSYS_LOG(DEBUG, "master=%s  mater_idx_array_[%ld]: cluster_idx=[%ld], ups_idx=[%ld] cur_master=%s",
              master.to_cstring(), paxos_idx, cluster_idx, master_ups_idx,
              master_node->ups_array_->at(master_ups_idx).addr_.to_cstring());
  }
  if (NULL == (paxos_head = find_paxos_head_node(paxos_idx)) && OB_SUCCESS == ret)
  {
    ret = OB_UPS_MANAGER_NODE_INEXISTENT;
    YYSYS_LOG(WARN, "Find paxos:[%ld] head node addr failed, ret=%d.", paxos_idx, ret);
  }
  else
  {
    ObUpsNode *tmp_node = paxos_head;
    do
    {
      YYSYS_LOG(DEBUG, "do loop begin, tmp_mode ups array count:[%ld]", tmp_node->ups_array_->count());
      for (int64_t ups_idx = 0; ups_idx < tmp_node->ups_array_->count(); ++ups_idx)
      {
        YYSYS_LOG(DEBUG, "ups idx:[%ld], ups array count:[%ld]", ups_idx, tmp_node->ups_array_->count());
        if (UPS_STAT_OFFLINE != tmp_node->ups_array_->at(ups_idx).stat_ &&
            (need_grant(now, tmp_node->ups_array_->at(ups_idx)) || did_force))
        {
          tmp_node->ups_array_->at(ups_idx).did_renew_received_ = false;
          tmp_node->ups_array_->at(ups_idx).lease_ = now + lease_duration_us_;

          ObMsgUpsHeartbeat msg;
          msg.ups_master_ = master;
          msg.self_lease_ = tmp_node->ups_array_->at(ups_idx).lease_;
          //del peiouya [MultiUPS] [DELETE_OBI] 20150701:b
          //msg.obi_role_ = obi_role_;
          //del 20150701:e
          //add lbzhong [Paxos ups_replication] 20151030:b
          // msg.ups_quorum_ = (int32_t) max_deployed_ups_count_;
          msg.ups_quorum_ = paxos_ups_quorum_scales_[paxos_idx];
          //add:e
          msg.schema_version_ = schema_version_;
          msg.config_version_ = config_version_;
          //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160318:b
          msg.offline_version_ = tmp_node->ups_array_->at(ups_idx).offline_version_;
          //add 20160318:e

          if (!initialized_)
          {
            //mod peiouya [MultiUPS] [UPS_RESTART_BUG_FIX] 20150703:b
            //msg.last_frozen_version_ = -1;
            msg.last_frozen_version_ = OB_INVALID_VERSION;
            //mod 20150703:e
            msg.minor_freeze_flag_ = false; //[647]
          }
          else
          {
            //mod peiouya [MultiUPS] [UPS_Manage_Function] 20150615:b
            //msg.last_frozen_version_ = new_frozen_version_;
            msg.last_frozen_version_ = root_server_->get_last_frozen_version();
            //mod 20150615:e
            //add peiouya [MultiUPS] [UPS_Manage_Function] 20150615:b
            msg.to_be_frozen_version_ = new_frozen_version_;
            //add 20150605:e
            if (freeze_stat_[paxos_idx])
            {
              msg.minor_freeze_flag_ = false;
            }
            else
            {
              msg.minor_freeze_flag_ = root_server_->get_minor_freeze_flag();
            }
          }
          YYSYS_LOG(DEBUG, "send grant ups[%s] msg", tmp_node->ups_array_->at(ups_idx).addr_.to_cstring());
          YYSYS_LOG(DEBUG, "ObUpsManager::grant_lease_by_paxos_id_new: "
                    "msg.last_frozen_version_:[%ld], msg.to_be_frozen_version_:[%ld]",
                    msg.last_frozen_version_, msg.to_be_frozen_version_);
          tmp_ret = send_granting_msg(tmp_node->ups_array_->at(ups_idx).addr_, msg);
          if (OB_SUCCESS != tmp_ret)
          {
            ret = tmp_ret;//even though grant lease failed, we must grant lease continue
            YYSYS_LOG(WARN, "grant lease to ups error, err=%d ups=%s, ret=%d",
                      tmp_ret, tmp_node->ups_array_->at(ups_idx).addr_.to_cstring(), ret);
          }
          //add pangtianze [Paxos rs_election] 20170321:b
          //del pangtianze [Paxos] 20170711
          //          else
          //          {
          //              ObServer rs_servers[OB_MAX_RS_COUNT];
          //              int32_t rs_server_count = 0;
          //              worker_->get_root_server().get_rs_node_mgr()->get_all_alive_servers(rs_servers, rs_server_count);
          //              int ret3 = worker_->get_rpc_stub().send_rs_list_to_server(tmp_node->ups_array_->at(ups_idx).addr_, rs_servers, rs_server_count);
          //              if (OB_SUCCESS != ret3)
          //              {
          //                  YYSYS_LOG(WARN, "refresh rs list to update server [%s] failed, err=%d", tmp_node->ups_array_->at(ups_idx).addr_.to_cstring(), ret3);
          //              }
          //          }
          //add:e
        }
      }
      tmp_node = tmp_node->next_cluster_node();
    }while (tmp_node != paxos_head);
  }
  return ret;
}

int ObUpsManager::select_ups_master_with_highest_lsn(const int64_t& paxos_idx)
{
  int ret = OB_ERROR;

  if (!has_master(paxos_idx)
      && ((0 < get_ups_count(paxos_idx) && !is_use_paxos_)
          //|| (get_ups_count(paxos_idx) > max_deployed_ups_count_ / 2)
          || (get_ups_count(paxos_idx) > paxos_ups_quorum_scales_[paxos_idx] / 2))
      //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160303:b
      && !is_paxos_group_offline(paxos_idx))
    //add 20160303:e
  {
    int64_t highest_lsn = -1;
    int64_t cluster_idx = INVALID_INDEX;
    int64_t master_ups_idx = INVALID_INDEX;
    ObUpsNode *paxos_head = NULL, *master_node = NULL;
    yysys::CThreadGuard guard(&ups_array_mutex_);
    if (NULL == (paxos_head = find_paxos_head_node(paxos_idx)))
    {
      YYSYS_LOG(WARN, "Find paxos:[%ld] head node addr failed.", paxos_idx);
      ret = OB_UPS_MANAGER_NODE_INEXISTENT;
    }
    else
    {
      ObUpsNode *tmp_node = paxos_head;
      do
      {
        for (int64_t ups_idx = 0; ups_idx < tmp_node->ups_array_->count(); ++ups_idx)
        {
          if (UPS_STAT_OFFLINE != tmp_node->ups_array_->at(ups_idx).stat_ &&
              tmp_node->ups_array_->at(ups_idx).log_seq_num_ > highest_lsn)
          {
            if (!initialized_)
            {
              highest_lsn = tmp_node->ups_array_->at(ups_idx).log_seq_num_;
              cluster_idx = tmp_node->cluster_idx_;
              master_ups_idx = ups_idx;
              master_node = tmp_node;
            }
            else
            {
              if (root_server_->get_last_frozen_version() == tmp_node->ups_array_->at(ups_idx).last_frozen_version_ ||
                  root_server_->get_last_frozen_version() == tmp_node->ups_array_->at(ups_idx).last_frozen_version_ - 1)
              {
                highest_lsn = tmp_node->ups_array_->at(ups_idx).log_seq_num_;
                cluster_idx = tmp_node->cluster_idx_;
                master_ups_idx = ups_idx;
                master_node = tmp_node;
              }
            }
          }
        }
        tmp_node = tmp_node->next_cluster_node();
      }while (tmp_node != paxos_head);
    }

    if (NULL == master_node)
    {
      if (REACH_TIME_INTERVAL(PRINT_INTERVAL))
      {
        YYSYS_LOG(WARN, "paxos_idx=%ld no master selected", paxos_idx);
      }
    }
    else
    {
      change_ups_stat(cluster_idx, paxos_idx, master_ups_idx, UPS_STAT_MASTER);
      atomic_exchange(reinterpret_cast<volatile uint64_t*>(&(master_idx_array_new_[paxos_idx].first)), cluster_idx);
      atomic_exchange(reinterpret_cast<volatile uint64_t*>(&(master_idx_array_new_[paxos_idx].second)), master_ups_idx);
      YYSYS_LOG(INFO, "paxos_idx=%ld new ups master selected, master=%s lsn=%ld",
                paxos_idx, master_node->ups_array_->at(master_ups_idx).addr_.to_cstring(),
                master_node->ups_array_->at(master_ups_idx).log_seq_num_);

      ObUps ups(master_node->ups_array_->at(master_ups_idx));
      refresh_inner_table(ROLE_CHANGE, ups, "null");
      //mod lqc [MultiUPS with paxos]
      // reset_ups_read_percent_by_paxos_id(paxos_idx);  //add peiouya [MultiUPS] [UPS_Manage_Function] 20150811
      reset_ups_read_percent();
      // mod e
      ret = OB_SUCCESS;
    }
  }
  return ret;
}
int64_t ObUpsManager::get_ups_count(const int64_t& paxos_idx) const
{
  int32_t count = 0;
  ObUpsNode *paxos_head = NULL, *tmp_node = NULL;
  if (NULL == (paxos_head = find_paxos_head_node(paxos_idx)))
  {
    YYSYS_LOG(WARN, "Find paxos:[%ld] head node addr failed.", paxos_idx);
  }
  else
  {
    tmp_node = paxos_head;
    do
    {
      for (int64_t ups_idx = 0; ups_idx < tmp_node->ups_array_->count(); ++ups_idx)
      {
        if (UPS_STAT_OFFLINE != tmp_node->ups_array_->at(ups_idx).stat_)
        {
          ++count;
        }
      }
      tmp_node = tmp_node->next_cluster_node();
    } while (tmp_node != paxos_head);
  }
  return count;
}
//mod 20160104:e
int ObUpsManager::get_ups_memory(int64_t &max_used_mem_size)
{
  int ret = OB_SUCCESS;
  yysys::CThreadGuard guard(&ups_array_mutex_);
  ObUpsNode *paxos_head = NULL;
  int64_t paxos_idx = INVALID_INDEX;
  ObUpsNode *cluster_node = NULL;
  if (ObRoleMgr::MASTER == worker_->get_role_manager()->get_role()
      && NULL != (paxos_head = link_head_))
  {
    do
    {
      cluster_node = paxos_head;
      paxos_idx = paxos_head->paxos_idx_;
      if (!is_paxos_group_offline(paxos_idx))
      {
        do
        {
          common::ObArray<ObUps> *&ups_array = cluster_node->ups_array_;
          for (int64_t ups_idx = 0; ups_idx < ups_array->count(); ++ups_idx)
          {
            if (UPS_STAT_OFFLINE != ups_array->at(ups_idx).stat_)
            {
              int64_t used_mem_size = 0;
              int tmp_ret = send_get_ups_memory_msg(ups_array->at(ups_idx).addr_, used_mem_size);
              if (OB_SUCCESS != tmp_ret)
              {
                YYSYS_LOG(WARN, "send nmon memory to ups failed, tmp_ret=%d,ups=%s", tmp_ret, ups_array->at(ups_idx).addr_.to_cstring());
              }
              else
              {
                if (used_mem_size > max_used_mem_size)
                {
                  max_used_mem_size = used_mem_size;
                }
              }
            }
          }
          cluster_node = cluster_node->next_cluster_node();
        } while (paxos_head != cluster_node);
      }
      paxos_head = paxos_head->next_paxos_group_node();
    } while (link_head_ != paxos_head);
  }
  return ret;
}

int ObUpsManager::send_get_ups_memory_msg(const common::ObServer &addr, int64_t &mem_size)
{
  int ret = OB_SUCCESS;
  ret = rpc_stub_.get_ups_mem_msg(addr, mem_size, revoke_rpc_timeout_us_);
  return ret;
}

//[677]
ObUpsNode* ObUpsManager::get_link_head() const
{
    return link_head_;
}

//mod liuzy [MultiUPS] [UPS_Manager] [private] 20160105:b
void ObUpsManager::set_initialized()
{
  if (!initialized_)
  {
    //check to set report_flag_:true
    bool is_all_use_paxos_has_master = true;
    int64_t cluster_idx = INVALID_INDEX;
    int64_t master_ups_idx = INVALID_INDEX;
    ObUpsNode *master_node = NULL;
    for (int64_t paxos_idx = 0; paxos_idx < use_paxos_num_; ++paxos_idx)
    {
        //[492]
        if(UPS_NODE_STAT_ACTIVE != this->get_paxos_status(paxos_idx))
        {
            continue;
        }

      if (!check_master_idx_array_info_valid(master_node, cluster_idx, paxos_idx, master_ups_idx))
      {
        is_all_use_paxos_has_master = false;
        break;
      }
      else if (!master_node->ups_array_->at(master_ups_idx).is_active_)
      {
        YYSYS_LOG(INFO, "master ups:[%s] is not ative.", master_node->ups_array_->at(master_ups_idx).addr_.to_cstring());
        is_all_use_paxos_has_master = false;
        break;
      }
    }
    int64_t frozen_version = -1;
    if (is_all_use_paxos_has_master && check_all_online_ups_version_valid(frozen_version))
    {
      if (!initialized_)
      {
        root_server_->set_last_frozen_version(frozen_version, true);
        YYSYS_LOG(DEBUG, "ObUpsManager::set_report_flag(): frozen_version:[%ld], new_frozen_version:[%ld]",
                  frozen_version, new_frozen_version_);
        atomic_exchange(reinterpret_cast<volatile uint64_t*>(&new_frozen_version_), frozen_version);
      }
      __sync_bool_compare_and_swap(&initialized_, false, true);
    }
    else if (is_all_use_paxos_has_master)
    {
      YYSYS_LOG(DEBUG, "Cluster initing, becase UPS is replaying log, please wait.");
    }
  }
}
//mod //mod lqc [MultiUPS with paxos]
//void ObUpsManager::reset_ups_read_percent()
//{
//  for (int64_t paxos_idx = 0; paxos_idx < use_paxos_num_; paxos_idx++)
//  {
//    reset_ups_read_percent_by_paxos_id(paxos_idx);
//  }
//}
// mod e
bool ObUpsManager::check_master_ups_valid(const int64_t paxos_idx) const
{
  bool ret = false;
  int64_t master_ups_idx = INVALID_INDEX;
  int64_t cluster_idx = INVALID_INDEX;
  ObUpsNode *master_node = NULL;
  if (initialized_ && 0 <= paxos_idx && use_paxos_num_ > paxos_idx && has_master(paxos_idx))
  {
    int64_t rs_last_frozen_version = root_server_->get_last_frozen_version();
    if (OB_INVALID_VERSION == rs_last_frozen_version)
    {
      YYSYS_LOG(ERROR, "after rs set report_flag:true, "
                "root_server_->get_last_frozen_version() must not be OB_INVALID_VERSION");
      root_server_->kill_self();
    }
    else
    {
      if (!check_master_idx_array_info_valid(master_node, cluster_idx, paxos_idx, master_ups_idx))
      {
        YYSYS_LOG(WARN, "The master ups info: cluster idx:[%ld], master ups idx:[%ld] of paxos[%ld] is invalid,",
                  cluster_idx, master_ups_idx, paxos_idx);
      }
      else if ((master_node->ups_array_->at(master_ups_idx).is_active_)
               && (rs_last_frozen_version == master_node->ups_array_->at(master_ups_idx).last_frozen_version_
                   || rs_last_frozen_version == master_node->ups_array_->at(master_ups_idx).last_frozen_version_ - 1))
      {
        ret = true;
      }
      //add liuzy [MultiUPS] [get_online_ups_failed] 20160728:b
      else
      {
        if (REACH_TIME_INTERVAL(PRINT_INTERVAL_CHECK))
        {
          YYSYS_LOG(WARN, "paxos:[%ld] master ups:[%s] is:[%s], ups last frozen vers:[%ld], rs last frozen vers:[%ld]",
                    paxos_idx, master_node->ups_array_->at(master_ups_idx).addr_.to_cstring(),
                    master_node->ups_array_->at(master_ups_idx).is_active_ ? "active" : "down",
                    master_node->ups_array_->at(master_ups_idx).last_frozen_version_, rs_last_frozen_version);
        }
      }
      //add 20160728:e
    }
  }
  return ret;
}
//del pangtianze [MultiPaxos] [merge with paxos] 20170518:b
/*
int ObUpsManager::select_new_ups_master(const int64_t& paxos_idx, const int exp_ups_idx)
{
  int ret = OB_ERROR;

  if (!has_master(paxos_idx) && !is_master_lease_valid(paxos_idx))
  {
    this->update_ups_lsn(paxos_idx);
    ret = this->select_ups_master_with_highest_lsn(paxos_idx);
  }
  return ret;
}
*/
//del:e
//add pangtianze [MultiPaxos] [merge with paxos] 20170518:b
int ObUpsManager::select_new_ups_master(const int64_t& paxos_idx, const common::ObServer* exp_ups_addr)
{
  int ret = OB_SUCCESS;
  if (!has_master(paxos_idx) && !is_master_lease_valid(paxos_idx)
      && (worker_->get_root_server().get_rs_node_mgr()->get_can_select_master_ups()
          //add pangtianze [Paxos bugfix:allow to select when user change master] 20170802:b
          || exp_ups_addr != NULL)
      //add:e
      )
  {
    if (check_most_ups_registered(paxos_idx))
    {
      //add pangtianze [Paxos] 20161010:b
      set_is_select_new_master(true);
      //add:e
      if (OB_SUCCESS == this->update_ups_lsn_and_term(paxos_idx))
      {
        ret = this->select_ups_master_with_highest_lsn_and_term(paxos_idx, exp_ups_addr);
      }
      //add pangtianze [Paxos] 20161010:b
      set_is_select_new_master(false);
      //add:e
    }
    else
    {
      if (worker_ == NULL)
      {
        YYSYS_LOG(ERROR, "worker is NULL");
        ret = OB_ERROR;
      }
      else
      {
        ret = OB_NOT_ENOUGH_UPS_ONLINE;
        if (ObRoleMgr::MASTER == worker_->get_role_manager()->get_role())
        {
          YYSYS_LOG(WARN, "no most online ups, need %ld ups at least, but there are only %ld ups",
                    // (max_deployed_ups_count_ / 2 + 1),
                    static_cast<int64_t>(paxos_ups_quorum_scales_[paxos_idx]/2+1), get_ups_count(paxos_idx));
        }
      }
    }
  }
  return ret;
}
//add:e


bool ObUpsManager::is_master_lease_valid(const int64_t paxos_idx) const
{
  bool ret = false;

  if (has_master(paxos_idx))
  {
    int64_t now = yysys::CTimeUtil::getTime();
    ObUpsNode *master_node = NULL;
    int64_t cluster_idx = INVALID_INDEX;
    int64_t master_ups_idx = INVALID_INDEX;
    if (!check_master_idx_array_info_valid(master_node, cluster_idx, paxos_idx, master_ups_idx))
    {
      YYSYS_LOG(WARN, "The master ups info: cluster idx:[%ld], master ups idx:[%ld] of paxos[%ld] is invalid,",
                cluster_idx, master_ups_idx, paxos_idx);
    }
    else
    {
      ret = (master_node->ups_array_->at(master_ups_idx).lease_ > now);
    }
  }
  return ret;
}

void ObUpsManager::update_ups_lsn(const int64_t& paxos_idx)
{
  ObUpsNode *paxos_head = NULL;
  ObUpsNode *tmp_node = NULL;
  if (NULL != (paxos_head = find_paxos_head_node(paxos_idx)))
  {
    tmp_node = paxos_head;
    do
    {
      common::ObArray<ObUps> *&ups_array = tmp_node->ups_array_;
      for (int64_t ups_idx = 0; ups_idx < ups_array->count(); ++ups_idx)
      {
        if (UPS_STAT_OFFLINE != ups_array->at(ups_idx).stat_)
        {
          uint64_t lsn = 0;
          if (OB_SUCCESS != rpc_stub_.get_ups_max_log_seq(ups_array->at(ups_idx).addr_, lsn, revoke_rpc_timeout_us_))
          {
            YYSYS_LOG(WARN, "Failed to get ups log seq, ups=%s, paxos_idx=%ld",
                      ups_array->at(ups_idx).addr_.to_cstring(), paxos_idx);
          }
          else
          {
            ups_array->at(ups_idx).log_seq_num_ = lsn;
          }
        }
      }
      tmp_node = tmp_node->next_cluster_node();
    } while (tmp_node != paxos_head);
  }
  else
  {
    YYSYS_LOG(WARN, "Find paxos [%ld] head node failed.", paxos_idx);
  }
}
//mod 20160105:e

//add pangtianze [Paxos ups_replication] 20150722:b
int ObUpsManager::update_ups_lsn_and_term(const int64_t paxos_idx)
{
  ObUpsNode *paxos_head = NULL;
  ObUpsNode *tmp_node = NULL;
  //add pangtianze [MultiUPS] [merge with paxos] 20170519:b
  int ret = OB_SUCCESS;
  int32_t count = 0; //calculate ups count that successed return seq
  //add:e
  if (NULL != (paxos_head = find_paxos_head_node(paxos_idx)))
  {
    tmp_node = paxos_head;
    do
    {
      common::ObArray<ObUps> *&ups_array = tmp_node->ups_array_;
      for (int64_t ups_idx = 0; ups_idx < ups_array->count(); ++ups_idx)
      {
        if (UPS_STAT_OFFLINE != ups_array->at(ups_idx).stat_)
        {
          uint64_t lsn = 0;
          int64_t ups_term = -1;
          ups_array->at(ups_idx).log_seq_num_ = lsn;
          ups_array->at(ups_idx).log_term_ = ups_term;

          if (OB_SUCCESS != rpc_stub_.get_ups_max_log_seq_and_term(ups_array->at(ups_idx).addr_, lsn, ups_term, revoke_rpc_timeout_us_))
          {
            YYSYS_LOG(WARN, "Failed to get ups log seq, ups=%s, paxos_idx=%ld",
                      ups_array->at(ups_idx).addr_.to_cstring(), paxos_idx);
          }
          else
          {
            ups_array->at(ups_idx).log_seq_num_ = lsn;
            ups_array->at(ups_idx).log_term_ = ups_term;
            //add pangtianze [MultiUPS] [merge with paxos] 20170519:b
            if (ups_term != -1)
            {
              count++;
            }
            //add:e
          }
        }
      }
      tmp_node = tmp_node->next_cluster_node();
    } while (tmp_node != paxos_head);
    //add pangtianze [MultiUPS] [merge with paxos] 20170519:b
    //if (count <= max_deployed_ups_count_ / 2)
    if (count <= paxos_ups_quorum_scales_[paxos_idx] / 2)
    {
      ret = OB_ERROR;
      YYSYS_LOG(WARN, "failed to get seq from most ups, paxos_id=%ld, count=%d, max_ups_count=%d", paxos_idx, count, paxos_ups_quorum_scales_[paxos_idx]);
    }
    else
    {
      ret = OB_SUCCESS;
      YYSYS_LOG(INFO, "get seq from most ups success, count=%d, max_ups_count=%d", count, paxos_ups_quorum_scales_[paxos_idx]);
    }
    
    //add:e
  }
  else
  {
    //add pangtianze [MultiUPS] [merge with paxos] 20170519:b
    ret = OB_ERROR;
    //add:e
    YYSYS_LOG(WARN, "Find paxos [%ld] head node failed.", paxos_idx);
  }
  //add pangtianze [MultiUPS] [merge with paxos] 20170519:b
  return ret;
  //add:e
}
//add:e

//mod liuzy [MultiUPS] [UPS_Manager] [private] 20160106:b
void ObUpsManager::init_master_idx_array()
{
  for (int64_t paxos_idx = 0; paxos_idx < MAX_UPS_COUNT_ONE_CLUSTER; paxos_idx++)
  {
    master_idx_array_new_[paxos_idx].first = INVALID_INDEX;
    master_idx_array_new_[paxos_idx].second = INVALID_INDEX;
  }
}
//mod hongchen [VERSION_VALID_CHECK_FIX] 20170826:b
//add peiouya [MultiUPS] [UPS_Manage_Function] 20150521:b
bool ObUpsManager::check_all_online_ups_version_valid(int64_t& frozen_version)
{
  bool ret = false;
  int64_t last_frozen_version = 0;
  int64_t cluster_idx = INVALID_INDEX;
  int64_t master_ups_idx = INVALID_INDEX;
  ObUpsNode *master_node = NULL;
  int64_t min_frozen_version = 0;
  int64_t max_frozen_version = 0;
  //mod peiouya [MultiUPS] [UPS_Manage_Function] 20150811:b
  //for (int64_t i = 0; i < use_cluster_num_; ++i)
  for (int64_t paxos_idx = 0; paxos_idx < use_paxos_num_; ++paxos_idx)
    //mod 20150811:e
  {
      //[492]
      if(UPS_NODE_STAT_ACTIVE != this->get_paxos_status(paxos_idx))
      {
          continue;
      }

    if (!check_master_idx_array_info_valid(master_node, cluster_idx, paxos_idx, master_ups_idx))
    {
      YYSYS_LOG(WARN, "The master ups info: cluster idx:[%ld], master ups idx:[%ld] of paxos[%ld] is invalid,",
                cluster_idx, master_ups_idx, paxos_idx);
      ret = false;
      break;
    }
    else
    {
      last_frozen_version = master_node->ups_array_->at(master_ups_idx).last_frozen_version_;
      if (0 == min_frozen_version)
      {
        min_frozen_version = last_frozen_version;
      }
      else if (last_frozen_version < min_frozen_version)
      {
        min_frozen_version = last_frozen_version;
      }
      
      if (0 == max_frozen_version)
      {
        max_frozen_version = last_frozen_version;
      }
      else if (last_frozen_version > max_frozen_version)
      {
        max_frozen_version = last_frozen_version;
      }
      
      ret = true;
    }
  }
  
  if (ret
      && ((1 == max_frozen_version - min_frozen_version)
          || (0 == max_frozen_version - min_frozen_version)))
  {
    ret = true;
    frozen_version = min_frozen_version;
  }
  return ret;
}
//mod hongchen [VERSION_VALID_CHECK_FIX] 20170826:e

void ObUpsManager::check_ups_master_exist_by_paxos_id(const int64_t& paxos_idx)
{
  int64_t count = get_ups_count(paxos_idx);
  YYSYS_LOG(DEBUG, "paxos group[%ld] has [%ld] ups.", paxos_idx, count);
  //add chujiajia [Paxos rs_election] 20160125:b
  if(!has_master(paxos_idx))
  {
    if(!is_use_paxos_)
    {
      //add:e
      //mod liuzy [Multiups] [BugFix] 20160704:b
      /*Exp: this part has logical error*/
      //      if ((0 < count && !is_use_paxos_) || (count > use_cluster_num_ / 2))
      //      {
      //        this->select_ups_master_with_highest_lsn(paxos_idx);
      //      }
      if (0 < count)
      {
        this->select_ups_master_with_highest_lsn_and_term(paxos_idx);
      }
      //mod 20160704:e
      //add chujiajia [Paxos rs_election] 20160125:b
    }
    else
    {
      if(count > (paxos_ups_quorum_scales_[paxos_idx] / 2))
      {
        select_new_ups_master(paxos_idx);
        //this->update_ups_lsn_and_term(paxos_idx);
        //this->select_ups_master_with_highest_lsn_and_term(paxos_idx);
      }
    }
  }
  //add:e
}

bool ObUpsManager::check_all_ups_to_special_version(const int64_t frozen_version)
{
  bool is_valid = true;
  int64_t count = 0;
  OB_ASSERT(1 <= use_cluster_num_);

  ObUpsNode *paxos_node = NULL, *cluster_node = NULL;
  if (NULL != (paxos_node = link_head_))
  {
    do
    {
      count = 0;
      cluster_node = paxos_node;
      do
      {
        common::ObArray<ObUps> *&ups_array = cluster_node->ups_array_;
        for (int64_t ups_idx = 0; ups_idx < ups_array->count(); ++ups_idx)
        {
          if (UPS_STAT_OFFLINE != ups_array->at(ups_idx).stat_
              && frozen_version <= ups_array->at(ups_idx).last_frozen_version_)
          {
            ++count;
          }
        }
        cluster_node = cluster_node->next_cluster_node();
      } while (paxos_node != cluster_node);
      //mod peiouya [MultiUPS] [UPS_Manage_Function] 20150811:b
      /*
      if (1 == use_cluster_num_ && 0 == count)
      {
        is_valid = false;
        break;
      }
      else if(1 != use_cluster_num_ && (use_cluster_num_/2  + 1 > count))
      {
        is_valid = false;
        break;
      }
      */
      if (1 == use_cluster_num_)
      {
        if (0 == count)
        {
          is_valid = false;
          break;
        }
      }
      //check if majority ups arrive in range
      //else if(1 != use_cluster_num_ && (use_cluster_num_/2  + 1 > count))
      else
      {
        if (!is_use_paxos_ && 0 == count)
        {
          is_valid = false;
          break;
        }
        //mod liuzy [MultiUPS] [UPS_Manager] 20160422:b
        /*Exp: each ups node has several ups, we should use sum of them to judge PAXOS*/
        //        else if (is_use_paxos_ && (use_cluster_num_/2  + 1 > count))
        //        {
        //          is_valid = false;
        //          break;
        //        }
        else if (is_use_paxos_ && (paxos_ups_quorum_scales_[paxos_node->paxos_idx_]/2 + 1 > count))
        {
          is_valid = false;
          break;
        }
        //mod 20160422:e
      }
      //mod liuzy [MultiUPS] [UPS_Manager] 20160422:b
      //      paxos_node = paxos_node->next_paxos_group_node();
      do
      {
        paxos_node = paxos_node->next_paxos_group_node();
      } while (link_head_ != paxos_node && !is_paxos_group_active(paxos_node->paxos_idx_));
      //mod 20160422:e
    } while (link_head_ != paxos_node);
  }
  return is_valid;
}

//mod 20160106:e

//add peiouya [MultiUPS] [UPS_Manage_Function] 20150521:b
void ObUpsManager::set_root_server(ObRootServer2 *root_server)
{
  root_server_ = root_server;
}
//add 20150527:e

int ObUpsManager::refresh_inner_table(const ObTaskType type, const ObUps & ups, const char *server_version)
{
  int ret = OB_SUCCESS;
  if (queue_ != NULL)
  {
    ObRootAsyncTaskQueue::ObSeqTask task;
    task.type_ = type;
    task.role_ = OB_UPDATESERVER;
    task.server_ = ups.addr_;
    task.inner_port_ = ups.inner_port_;
    //add peiouya [MultiUPS] [UPS_Manage_Function] 20150507:b
    task.cluster_id_ = ups.cluster_id_;
    task.paxos_id_ = ups.paxos_id_;
    //add 20150507:e
    // set as slave
    task.server_status_ = 2;
    // set as master
    if (ups.stat_ == UPS_STAT_MASTER)
    {
      task.server_status_ = 1;
    }
    int64_t server_version_length = strlen(server_version);
    if (server_version_length < OB_SERVER_VERSION_LENGTH)
    {
      strncpy(task.server_version_, server_version, server_version_length + 1);
    }
    else
    {
      strncpy(task.server_version_, server_version, OB_SERVER_VERSION_LENGTH - 1);
      task.server_version_[OB_SERVER_VERSION_LENGTH - 1] = '\0';
    }
    ret = queue_->push(task);
    if (ret != OB_SUCCESS)
    {
      YYSYS_LOG(ERROR, "push update server task failed:server[%s], type[%d], ret[%d]",
                task.server_.to_cstring(), task.type_, ret);
    }
    else
    {
      YYSYS_LOG(INFO, "push update server task succ:server[%s]", task.server_.to_cstring());
    }
  }
  return ret;
}

int ObUpsManager::send_granting_msg(const common::ObServer &addr,
                                    common::ObMsgUpsHeartbeat &msg)
{
  int ret = OB_SUCCESS;
  ret = rpc_stub_.grant_lease_to_ups(addr, msg);
  YYSYS_LOG(DEBUG, "send lease to ups, ups=%s master=%s "
            "self_lease=%ld, schema_version=%ld config_version=%ld",
            to_cstring(addr), to_cstring(msg.ups_master_),
            msg.self_lease_, msg.schema_version_, msg.config_version_);
  return ret;
}

bool ObUpsManager::need_grant(int64_t now, const ObUps &ups) const
{
  bool ret = false;
  if (ups.did_renew_received_)
  {
    if (now > ups.lease_ - lease_reserved_us_ && now < ups.lease_)
    {
      ret = true;
    }
  }
  return ret;
}

//mod peiouya [MultiUPS] [PARTITION_LOCK_FUCTION] 20150609:b
//void ObUpsManager::set_new_frozen_version(const int64_t new_frozen_version)
void ObUpsManager::set_new_frozen_version(const int64_t new_frozen_version, const bool is_cmd_set)
{
  //assert()
  if (initialized_)
  {
    int64_t last_frozen_version = root_server_->get_last_frozen_version();
    YYSYS_LOG(DEBUG, "ObUpsManager::set_new_frozen_version "
              "last frozen version:[%ld], new frozen version:[%ld], new_frozen_version_:[%ld]",
              last_frozen_version, new_frozen_version, new_frozen_version_);
    if (OB_INVALID_VERSION == last_frozen_version)
    {
      YYSYS_LOG(ERROR, "killself. after rs set report_flag_:true, "
                "new_frozen_version can't be %ld, and last_frozen_version can't be %ld",
                OB_INVALID_VERSION, OB_INVALID_VERSION);
      root_server_->kill_self();
    }
    else if (OB_INVALID_VERSION == new_frozen_version)
    {
      //mod peiouya [MultiUPS] [UPS_Manage_Function] 20150815:b
      //YYSYS_LOG(ERROR, "all ups offline");
      if (REACH_TIME_INTERVAL(PRINT_INTERVAL))
      {
        YYSYS_LOG(ERROR, "all ups offline");
      }
      //mod 20150815:e
    }
    else if (((last_frozen_version == new_frozen_version - 1
               || last_frozen_version == new_frozen_version)
              && (new_frozen_version_ == new_frozen_version
                  || new_frozen_version_ == new_frozen_version -1
                  //mod liuzy [MultiUPS] [add_cluster_interface] 20160720:b
                  /*Exp: when meger done, new_frozen_version is the max last_frozen_version from ups,
                                        this values will be less than new_frozen_version_(member) and lead to WARNING,
                                        so I cancel below annotation*/
                  //add peiouya [MultiUPS] [PARTITION_LOCK_FUCTION] 20150609
                  /*|| new_frozen_version_ == new_frozen_version +1)*/
                  || new_frozen_version_ == new_frozen_version +1
                  //mod 20160720:e
                  /*&& (last_frozen_version <= new_frozen_version)*/))
             || is_cmd_set)  //add peiouya [MultiUPS] [PARTITION_LOCK_FUCTION] 20150609
    {
      if (new_frozen_version_ < new_frozen_version)
      {
        //add peiouya [MultiUPS] [PARTITION_LOCK_FUCTION] 20150601:b
        root_server_->set_partition_lock_flag_for_ups(true, last_frozen_version);
        //add 20150601:e
        atomic_exchange(reinterpret_cast<volatile uint64_t*>(&new_frozen_version_), new_frozen_version);
        //mod peiouya [MultiUPS] [STAT_MERGE_BUG_FIX] 20150725:b
        int64_t frozen_time = yysys::CTimeUtil::getTime();
        root_server_->set_last_frozen_time(frozen_time);
        //mod 20150725:e
      }
    }
    else
    {
      //mod peiouya [MultiUPS] [UPS_RESTART_BUG_FIX] 20150703:b
      //YYSYS_LOG(ERROR, "killself. the gap between all ups frozen version > 1");
      //root_server_->kill_self();
      YYSYS_LOG(WARN, "most ups are in recovering");
      //mod 20150703:e
      //add liuzy [MultiUPS] [UPS_RESTART_INFO_CHECK] 20160718:b
      YYSYS_LOG(WARN, "last_froz_ver:[%ld], new_froz_ver(argument):[%ld], new_froz_ver_(member):[%ld]",
                last_frozen_version, new_frozen_version, new_frozen_version_);
      //add 20160718:e
    }
  }
}
//mod 20150609:e

void ObUpsManager::set_cluster_and_paxos_config_and_paxos_flag(int64_t use_cluster_num, int64_t use_paxos_id_num,
                                                               bool is_use_paxos)
{
  //atomic_exchange(reinterpret_cast<volatile uint64_t*>(&use_cluster_num_), use_cluster_num);
  //atomic_exchange(reinterpret_cast<volatile uint64_t*>(&use_paxos_num_), use_paxos_id_num);
  use_cluster_num_ = use_cluster_num;
  use_paxos_num_ = use_paxos_id_num;
  is_use_paxos_ = is_use_paxos;
  init_ups_node();
}

const char* ObUpsManager::ups_stat_to_cstr(ObUpsStatus stat) const
{
  const char* ret = "";

  switch(stat)
  {
    case UPS_STAT_OFFLINE:
      ret = "offline";
      break;
    case UPS_STAT_MASTER:
      ret = "master";
      break;
    case UPS_STAT_SYNC:
      ret = "sync";
      break;
    case UPS_STAT_NOTSYNC:
      ret = "nsync";
      break;
    default:
      break;
  }

  return ret;
}
//add e 
//add bingo [Paxos rs_admin all_server_in_clusters] 20170612:b
void ObUpsManager::print_in_cluster(char* buf, const int64_t buf_len, int64_t &pos, int32_t cluster_id) const
{
  yysys::CThreadGuard guard(&ups_array_mutex_);
  // mod lqc [multiups] 20170623 b
  ObUpsNode *cluster_head = find_cluster_head_node(cluster_id);
  if(cluster_head == NULL)
  {

    YYSYS_LOG(WARN, "find cluster[%d] head node failed", cluster_id);
  }
  else
  {
    ObUpsNode *tmp_node = cluster_head;
    do
    {
      bool is_master_lease_valid = false;
      int32_t ups_master_idx = INVALID_INDEX;
      if(node_has_master(tmp_node))
      {
        for(int32_t idx = 0; idx <= tmp_node->ups_array_->count(); idx++)
        {
          if (UPS_STAT_MASTER == tmp_node->ups_array_->at(idx).stat_
               && cluster_id == tmp_node->ups_array_->at(idx).cluster_id_)
          {
            int64_t now = yysys::CTimeUtil::getTime();
            is_master_lease_valid = (tmp_node->ups_array_->at(idx).lease_ > now);
            ups_master_idx = idx;
            break;
          }
        }
      }
      if(is_master_lease_valid)
      {
        int64_t now2 = yysys::CTimeUtil::getTime();
        databuff_printf(buf, buf_len, pos, "lease_left=%ld|", tmp_node->ups_array_->at(ups_master_idx).lease_ - now2);
      }
      else
      {
        databuff_printf(buf, buf_len, pos, "lease_left=null|");
      }
      common::ObArray<ObUps> *&ups_array = tmp_node->ups_array_;
      for(int32_t idx = 0; idx < ups_array->count();idx++)
      {
        if (UPS_STAT_OFFLINE != ups_array->at(idx).stat_
            && cluster_id == ups_array->at(idx).cluster_id_)
        {
          databuff_printf(buf, buf_len, pos, "%s(%d %s %d %d %lu )",ups_array->at(idx).addr_.to_cstring(),
                          ups_array->at(idx).inner_port_, ups_stat_to_cstr(ups_array->at(idx).stat_),
                          ups_array->at(idx).ms_read_percentage_,
                          ups_array->at(idx).cs_read_percentage_,
                          ups_array->at(idx).log_seq_num_);
        }
      }
      tmp_node = tmp_node->next_paxos_group_node();
    } while(cluster_head != tmp_node);
  }//mod e
}
//add:e
//add liuzy [MultiUPS] [UPS_Manager] 20151211:b
void ObUpsManager::init_ups_node()
{
  YYSYS_LOG(INFO, "clu_num = %ld, pax_num = %ld", use_cluster_num_, use_paxos_num_);
  int ret = OB_SUCCESS;
  (void)ret;
  for (int64_t clu_id = 0; clu_id < use_cluster_num_; ++clu_id)
  {
    for (int64_t pax_id = 0; pax_id < use_paxos_num_; ++pax_id)
    {
      ret = add_node(clu_id, pax_id);
    }
  }
}

void ObUpsManager::print_ups_manager() const
{
  YYSYS_LOG(INFO, "/**********print ups manager : BEGIN**********/");
  ObUpsNode *cluster = NULL, *paxos = NULL;
  if (NULL != link_head_)
  {
    cluster = link_head_;
    do
    {
      paxos = cluster;
      do
      {
        YYSYS_LOG(INFO, "cluster idx:[%ld], paxos idx:[%ld]",
                  paxos->cluster_idx_, paxos->paxos_idx_);
        paxos = paxos->next_paxos_group_node();
      }while (paxos != cluster && NULL != paxos);
      cluster = cluster->next_cluster_node();
    }while (cluster != link_head_ && NULL != cluster);
  }
  YYSYS_LOG(INFO, "/********** print ups manager : END **********/");
}

void ObUpsManager::print_ups_registered() const
{
  YYSYS_LOG(DEBUG, "/**********print ups registered : BEGIN**********/");
  ObUpsNode *cluster = NULL, *paxos = NULL;
  if (NULL != link_head_)
  {
    cluster = link_head_;
    do
    {
      paxos = cluster;
      do
      {
        YYSYS_LOG(DEBUG, "cluster idx:[%ld], paxos idx:[%ld] ups info:", paxos->cluster_idx_, paxos->paxos_idx_);
        for (int64_t ups_idx = 0; ups_idx < paxos->ups_array_->count(); ++ups_idx)
        {
          ObUps &ups = paxos->ups_array_->at(ups_idx);
          YYSYS_LOG(DEBUG, "ups info: addr=%s inner_port=%d lsn=%ld lease=%ld cluster_idx=%ld paxos_idx=%ld",
                    ups.addr_.to_cstring(), ups.inner_port_, ups.log_seq_num_,
                    ups.lease_, ups.cluster_id_, ups.paxos_id_);
        }
        paxos = paxos->next_paxos_group_node();
      }while (paxos != cluster && NULL != paxos);
      cluster = cluster->next_cluster_node();
    }while (cluster != link_head_ && NULL != cluster);
  }
  YYSYS_LOG(DEBUG, "/********** print ups registered : END **********/");
}
int ObUpsManager::add_node(int64_t clu_idx, int64_t pax_idx, ObUpsNode * const head_node /*= NULL*/,
                           ObArray<ObUpsNode*> *node_array /*= NULL*/)
{
  YYSYS_LOG(INFO, "add node cluster idx:[%ld], paxos idx[%ld]", clu_idx, pax_idx);
  int ret = OB_SUCCESS;
  ObUpsNode *up_node = NULL;
  ObUpsNode *left_node = NULL;
  if (NULL != find_node(clu_idx, pax_idx, head_node))
  {
    ret = OB_UPS_MANAGER_NODE_EXISTS;
    YYSYS_LOG(WARN, "Add node failed, node is existent, ret=%d.", ret);
  }
  else
  {
    up_node = find_node(clu_idx, pax_idx - 1, head_node);
    left_node = find_node(clu_idx - 1, pax_idx, head_node);
    if (NULL == up_node && 0 != pax_idx && NULL == head_node)
    {
      ret = OB_UPS_MANAGER_UP_NODE_INEXISTENT;
      YYSYS_LOG(WARN, "Add UPS manager node failed, up node is inexistent, ret=%d.", ret);
    }
    else if (NULL == left_node && 0 != clu_idx && NULL == head_node)
    {
      ret = OB_UPS_MANAGER_LEFT_NODE_INEXISTENT;
      YYSYS_LOG(WARN, "Add UPS manager node failed, left node is inexistent, ret=%d.", ret);
    }
    else if (OB_SUCCESS != (ret = insert_node(up_node, left_node, clu_idx, pax_idx, node_array)))
    {
      YYSYS_LOG(WARN, "Add UPS manager node failed, ret=%d.", ret);
    }
    else
    {
      YYSYS_LOG(INFO, "Add UPS manager node succeeded, cluster:[%ld], paxos idx[%ld], ret=%d.",  clu_idx, pax_idx, ret);
    }
  }
  return ret;
}

int ObUpsManager::insert_node(ObUpsNode *up_node, ObUpsNode *left_node, int64_t clu_idx,
                              int64_t pax_idx, ObArray<ObUpsNode*> *node_array)
{
  int ret = OB_SUCCESS;
  ObUpsNode *new_node = OB_NEW(ObUpsNode, ObModIds::OB_RS_UPS_MANAGER);
  if (NULL == new_node)
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    YYSYS_LOG(ERROR, "failed to allocate memory for ObUpsNode, ret=%d", ret);
  }
  else if (NULL != node_array)
  {
    if (OB_SUCCESS != (ret = node_array->push_back(new_node)))
    {
      OB_DELETE(ObUpsNode, ObModIds::OB_RS_UPS_MANAGER, new_node);
      YYSYS_LOG(ERROR, "push new ups node pointer to array failed, ret=%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    new_node->cluster_idx_ = clu_idx;
    new_node->paxos_idx_ = pax_idx;
    if (NULL == up_node && NULL == left_node)
    {
      link_head_ = new_node;
      YYSYS_LOG(INFO, "insert node_head, cluster : [%ld], paxos : [%ld]",
                link_head_->cluster_idx_, link_head_->paxos_idx_);
    }
    if (NULL != up_node)
    {
      up_node->insert_down(*new_node);
      YYSYS_LOG(INFO, "up node, cluster : [%ld], paxos : [%ld]",
                up_node->cluster_idx_, up_node->paxos_idx_);
    }
    if (NULL != left_node)
    {
      left_node->insert_right(*new_node);
      YYSYS_LOG(INFO, "left node cluster : [%ld], paxos : [%ld]",
                left_node->cluster_idx_, left_node->paxos_idx_);
    }
  }
  return ret;
}

ObUpsNode* ObUpsManager::find_paxos_head_node(int64_t pax_idx) const
{
  int64_t clu_idx = 0;
  return find_node(clu_idx, pax_idx);
}

ObUpsNode* ObUpsManager::find_cluster_head_node(int64_t clu_idx) const
{
  int64_t pax_idx = 0;
  return find_node(clu_idx, pax_idx);
}

int64_t ObUpsManager::get_last_cluster_idx() const
{
  int64_t pax_idx = 0;
  ObUpsNode *paxos_group_head_node = NULL;
  ObUpsNode *paxos_group_tail_node = NULL;
  if (NULL != (paxos_group_head_node = find_paxos_head_node(pax_idx)))
  {
    paxos_group_tail_node = paxos_group_head_node;
    while (paxos_group_head_node != paxos_group_tail_node->next_cluster_node())
    {
      paxos_group_tail_node = paxos_group_tail_node->next_cluster_node();
    }
  }
  return paxos_group_tail_node->cluster_idx_;
}

int64_t ObUpsManager::get_last_paxos_idx() const
{
  int64_t clu_idx = 0;
  ObUpsNode *cluster_head_node = NULL;
  ObUpsNode *cluster_tail_node = NULL;
  if (NULL != (cluster_head_node = find_cluster_head_node(clu_idx)))
  {
    cluster_tail_node = cluster_head_node;
    while (cluster_head_node != cluster_tail_node->next_paxos_group_node())
    {
      cluster_tail_node = cluster_tail_node->next_paxos_group_node();
    }
  }
  return cluster_tail_node->paxos_idx_;
}

ObUpsNode* ObUpsManager::find_node(int64_t clu_idx, int64_t pax_idx, ObUpsNode *const head_node /*= NULL*/) const
{
  ObUpsNode *ret = NULL, *cluster = NULL, *paxos = NULL, *first_cluster = NULL;
  if (NULL != link_head_ && INVALID_INDEX != clu_idx && INVALID_INDEX != pax_idx)
  {
    if (NULL != head_node)
    {
      first_cluster = head_node;
    }
    else
    {
      first_cluster = link_head_;
    }
    cluster = first_cluster;
    do
    {
      if (clu_idx == cluster->cluster_idx_)
      {
        paxos = cluster;
        do
        {
          if (pax_idx == paxos->paxos_idx_)
          {
            ret = paxos;
            break;
          }
          paxos = paxos->next_paxos_group_node();
        }while (paxos != cluster);
      }
      //del chujiajia [Paxos rs_election] 20160129:b
      //else
      //{
      //del:e
      cluster = cluster->next_cluster_node();
      //}
    }while (cluster != first_cluster && NULL == ret);
  }
  return ret;
}

ObUpsNode* ObUpsManager::find_node(const common::ObServer &addr, int64_t &array_idx,
                                   ObUpsNode *const head_node /*= NULL*/,bool iter_all) const
{
  ObUpsNode *ret = NULL;
  ObUpsNode *cluster = NULL, *paxos = NULL, *first_cluster = NULL;
  if (NULL != link_head_)
  {
    if (NULL != head_node)
    {
      first_cluster = head_node;
    }
    else
    {
      first_cluster = link_head_;
    }
    cluster = first_cluster;
    do
    {
      paxos = cluster;
      do
      {
        for (int64_t ups_idx = 0; ups_idx < paxos->ups_array_->count(); ++ups_idx)
        {
          if (addr == paxos->ups_array_->at(ups_idx).addr_
              && (iter_all
                  || UPS_STAT_OFFLINE != paxos->ups_array_->at(ups_idx).stat_))
          {
            ret = paxos;
            array_idx = ups_idx;
            break;
          }
        }
        paxos = paxos->next_paxos_group_node();
      } while (paxos != cluster && NULL == ret);
      cluster = cluster->next_cluster_node();
    } while (first_cluster != cluster && NULL == ret);
  }
  return ret;
}
//add 20151211:b

//add liuzy [MultiUPS] [UPS_Manager] 20151221:b
int64_t ObUpsManager::master_last_frozen_version(const int64_t paxos_idx) const
{
  int64_t ret = OB_INVALID_VERSION;
  int64_t cluster_idx = master_idx_array_new_[paxos_idx].first;
  ObUpsNode *ups_node = NULL;
  if (NULL != (ups_node = find_node(cluster_idx, paxos_idx)))
  {
    int64_t ups_idx = master_idx_array_new_[paxos_idx].second;
    ret = ups_node->ups_array_->at(ups_idx).last_frozen_version_;
  }
  else
  {
    YYSYS_LOG(WARN, "Get paxos:[%ld] master last frozen version failed.", paxos_idx);
  }
  return ret;
}

bool ObUpsManager::check_master_idx_array_info_valid(ObUpsNode *&master_node, int64_t &cluster_idx,
                                                     const int64_t paxos_idx, int64_t &master_ups_idx) const
{
  bool ret = false;
  if (INVALID_INDEX == (cluster_idx = master_idx_array_new_[paxos_idx].first)
      || INVALID_INDEX == (master_ups_idx = master_idx_array_new_[paxos_idx].second))
  {
    if (REACH_TIME_INTERVAL(PRINT_INTERVAL_CHECK))
    {
      YYSYS_LOG(WARN, "Master ups info of paxos idx[%ld] is invalid:"
                " cluster idx:[%ld] or master ups idx:[%ld] should not be INVALID_INDEX",
                paxos_idx, INVALID_INDEX == cluster_idx ? INVALID_INDEX : cluster_idx,
                INVALID_INDEX == master_ups_idx ? INVALID_INDEX : master_ups_idx);
    }
  }
  else if (NULL == (master_node = find_node(cluster_idx, paxos_idx)))
  {
    YYSYS_LOG(WARN, "Find master ups node cluster idx:[%ld], paxos idx:[%ld] failed.", cluster_idx, paxos_idx);
  }
  else if (master_node->ups_array_->count() <= master_ups_idx || 0 > master_ups_idx)
  {
    YYSYS_LOG(WARN, "Master ups idx:[%ld] must be in[0, %ld).", master_ups_idx, master_node->ups_array_->count());
  }
  else
  {
    ret = true;
  }
  return ret;
}
//add 20151221:e

//add liuzy [MultiUPS] [add_paxos_interface] 20160112:b
int ObUpsManager::add_new_paxos_group(int64_t new_paxos_num, int64_t new_config)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool is_renew_config = true;
  ObUpsNode *new_paxos_head = NULL;
  ObArray<ObUpsNode*> node_array;
  if (root_server_->get_rs_role() == ObRoleMgr::SLAVE)
  {
    is_renew_config = false;
  }
  if (OB_SUCCESS != (ret = cons_new_paxos_group(new_paxos_num, new_paxos_head, &node_array)))
  {
    YYSYS_LOG(ERROR, "construct new paxos group failed, ret=%d", ret);
  }
  else if (is_renew_config && OB_SUCCESS != (ret = root_server_->renew_config_use_num(new_config, UPS_PAXOS_SCALE)))
  {
    YYSYS_LOG(ERROR, "alter system config 'use_paxos_num' failed, ret=%d", ret);
  }
  else if (is_renew_config && OB_SUCCESS != (ret = root_server_->update_use_num_of_cluster_stat_info(get_new_frozen_version()+1,
                                                                                                     new_config,UPS_PAXOS_SCALE)))
  {
    YYSYS_LOG(ERROR, "update use_num in __all_cluster_stat_info failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = link_new_paxos_group(new_paxos_head)))
  {
    YYSYS_LOG(ERROR, "add new paxos groups to link failed, ret=%d", ret);
    if (is_renew_config && OB_SUCCESS != (tmp_ret = root_server_->renew_config_use_num(new_config - new_paxos_num,
                                                                                       UPS_PAXOS_SCALE)))
    {
      YYSYS_LOG(ERROR, "recover 'use_paxos_num' failed, please alter this system config to [%ld] on client, ret=%d",
                new_config - new_paxos_num, tmp_ret);
    }
    if (is_renew_config && OB_SUCCESS != (tmp_ret = root_server_->update_use_num_of_cluster_stat_info(get_new_frozen_version()+1,
                                                                                                      new_config  -new_paxos_num, UPS_PAXOS_SCALE)))
    {
      YYSYS_LOG(ERROR, "recover 'use_paxos_num' failed, please alter this system config in __all_cluster_stat_info to [%ld] on client, ret=%d",
                new_config - new_paxos_num, tmp_ret);
    }
  }
  if (OB_SUCCESS != ret)
  {
    free_ups_node(&node_array);
  }
  return ret;
}
int ObUpsManager::cons_new_paxos_group(int64_t new_paxos_num, ObUpsNode *&new_paxos_head,
                                       ObArray<ObUpsNode*> *node_array)
{
  if (root_server_->get_rs_role() == ObRoleMgr::SLAVE)
  {
    YYSYS_LOG(INFO, "current use_paxos_num:[%ld]", use_paxos_num_);
  }
  int ret = OB_SUCCESS;
  ObUpsNode *head_node = OB_NEW(ObUpsNode, ObModIds::OB_RS_UPS_MANAGER);
  if (NULL == head_node)
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    YYSYS_LOG(ERROR, "Failed to allocate memory for ObUpsNode, ret=%d.", ret);
  }
  else if (NULL == node_array)
  {
    OB_DELETE(ObUpsNode, ObModIds::OB_RS_UPS_MANAGER, head_node);
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(ERROR, "node array pointer is NULL, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = node_array->push_back(head_node)))
  {
    OB_DELETE(ObUpsNode, ObModIds::OB_RS_UPS_MANAGER, head_node);
    YYSYS_LOG(ERROR, "push new ups node pointer to array failed, ret=%d", ret);
  }
  else
  {
    new_paxos_head = head_node;
    for (int64_t num = 0; OB_SUCCESS == ret && num < new_paxos_num; ++num)
    {
      int64_t paxos_idx = use_paxos_num_ + num;
      for (int64_t cluster_idx = 0; OB_SUCCESS == ret && cluster_idx < use_cluster_num_; ++cluster_idx)
      {
        if (0 == num && 0 == cluster_idx)
        {
          head_node->cluster_idx_ = cluster_idx;
          head_node->paxos_idx_ = paxos_idx;
        }
        else if (OB_SUCCESS != (ret = add_node(cluster_idx, paxos_idx, head_node, node_array)))
        {
          YYSYS_LOG(WARN, "Add node cluster_idx:[%ld] paxos_idx:[%ld] failed, ret=%d.",
                    cluster_idx, head_node->paxos_idx_, ret);
          break;
        }
        else
        {
          YYSYS_LOG(INFO, "Add node cluster_idx:[%ld], paxos_idx:[%ld] succeed.", cluster_idx, paxos_idx);
        }
      }
    }
  }
  return ret;
}

int ObUpsManager::link_new_paxos_group(ObUpsNode * const new_paxos_groups_head)
{
  int ret = OB_SUCCESS;
  bool find = false;
  ObUpsNode *first_new_paxos_head = new_paxos_groups_head;
  ObUpsNode *last_new_paxos_head = new_paxos_groups_head;
  ObUpsNode *pre_paxos_group_head = NULL;
  yysys::CThreadGuard guard(&ups_array_mutex_);
  if (NULL == new_paxos_groups_head)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(ERROR, "new paxos groups head pointer is NULL, ret=%d", ret);
  }
  else if (NULL == (pre_paxos_group_head = find_paxos_head_node(new_paxos_groups_head->paxos_idx_ - 1)))
  {
    ret = OB_UPS_MANAGER_NODE_INEXISTENT;
    YYSYS_LOG(ERROR, "paxos:[%ld] head node is not existent, ret=%d", new_paxos_groups_head->paxos_idx_ - 1, ret);
  }
  else if (NULL != find_paxos_head_node(new_paxos_groups_head->paxos_idx_))
  {
    find = true;
    YYSYS_LOG(WARN, "new paxos[%ld] already linked", new_paxos_groups_head->paxos_idx_);
  }
  else
  {
    while (first_new_paxos_head != last_new_paxos_head->next_paxos_group_node())
    {
      last_new_paxos_head = last_new_paxos_head->next_paxos_group_node();
    }
  }
  if (OB_SUCCESS == ret && !find)
  {
    do
    {
      YYSYS_LOG(DEBUG, "first new paxos idx:[%ld], final new paxos idx:[%ld]",
                first_new_paxos_head->paxos_idx_, last_new_paxos_head->paxos_idx_);
      last_new_paxos_head->set_next_paxos_group_node(pre_paxos_group_head->next_paxos_group_node());
      pre_paxos_group_head->set_next_paxos_group_node(first_new_paxos_head);
      pre_paxos_group_head = pre_paxos_group_head->next_cluster_node();
      first_new_paxos_head = first_new_paxos_head->next_cluster_node();
      last_new_paxos_head = last_new_paxos_head->next_cluster_node();
    } while (new_paxos_groups_head != first_new_paxos_head);
  }
  print_ups_manager();
  return ret;
}
//add 20160112:e
int ObUpsManager::delink_last_paxos_group(const int64_t paxos_idx)
{
  int ret = OB_SUCCESS;
  ObUpsNode *last_paxos_head = find_paxos_head_node(paxos_idx);
  ObUpsNode *pre_paxos_head = NULL, *cluster_node = NULL;
  if (NULL == last_paxos_head)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(ERROR, "last paxos:[%ld] head node is not existent, ret=%d",paxos_idx, ret);
  }
  else if (NULL == (pre_paxos_head = find_paxos_head_node(paxos_idx - 1)))
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(ERROR, "previous paxos:[%ld] head node is not existent, ret=%d",paxos_idx - 1, ret);
  }
  else
  {
    cluster_node = last_paxos_head;
    YYSYS_LOG(INFO, "to delete paxos group[%ld], the previous paxos group[%ld]",paxos_idx, paxos_idx - 1);
    while (true)
    {
      pre_paxos_head->set_next_paxos_group_node(cluster_node->next_paxos_group_node());
      YYSYS_LOG(INFO, "node[cid:%ld][pid:%ld] --link to--> node[cid:%ld][pid:%ld]", pre_paxos_head->cluster_idx_, pre_paxos_head->paxos_idx_,
                cluster_node->next_paxos_group_node()->cluster_idx_, cluster_node->next_paxos_group_node()->paxos_idx_);
      cluster_node->set_next_paxos_group_node(NULL);
      pre_paxos_head = pre_paxos_head->next_cluster_node();
      if (cluster_node->next_cluster_node() != last_paxos_head)
      {
        cluster_node = cluster_node->next_cluster_node();
      }
      else
      {
        cluster_node->set_next_cluster_node(NULL);
        break;
      }
    }
    
    while (last_paxos_head)
    {
      cluster_node = last_paxos_head->next_cluster_node();
      YYSYS_LOG(INFO, "delete node[cid:%ld][pid:%ld]", last_paxos_head->cluster_idx_, last_paxos_head->paxos_idx_);
      ob_free(last_paxos_head);
      last_paxos_head = cluster_node;
    }
  }
  print_ups_manager();
  return ret;
}

int ObUpsManager::del_last_paxos_group(const int64_t paxos_idx)
{
  int ret = OB_SUCCESS;
  int temp_ret = OB_SUCCESS;
  int64_t use_paxos_num = root_server_->get_config().use_paxos_num - 1;
  bool is_master = (root_server_->get_rs_role() == ObRoleMgr::MASTER);
  if (is_master && OB_SUCCESS != (ret = root_server_->renew_config_use_num(use_paxos_num, UPS_PAXOS_SCALE)))
  {
    YYSYS_LOG(ERROR, "alter system config 'use_paxos_num' failed, ret=%d", ret);
  }
  if (is_master && OB_SUCCESS != (ret = root_server_->update_use_num_of_cluster_stat_info(get_new_frozen_version()+1,
                                                                                               use_paxos_num, UPS_PAXOS_SCALE)))
  {
    YYSYS_LOG(ERROR, "update use_num in __all_cluster_stat_info failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = delink_last_paxos_group(paxos_idx)))
  {
    YYSYS_LOG(ERROR, "delink_last_paxos_group failed, ret=%d", ret);
    if (is_master && OB_SUCCESS != (temp_ret = root_server_->renew_config_use_num(use_paxos_num + 1, UPS_PAXOS_SCALE)))
    {
      YYSYS_LOG(ERROR, "recover 'use_paxos_num' failed, please alter this system config to [%ld] on client, ret=%d",
                use_paxos_num + 1, temp_ret);
    }
    if (is_master && OB_SUCCESS != (temp_ret = root_server_->update_use_num_of_cluster_stat_info(get_new_frozen_version()+1,
                                                                                                 use_paxos_num + 1,UPS_PAXOS_SCALE)))
    {
      YYSYS_LOG(ERROR, "recover 'use_paxos_num' failed, please alter this system config in __all_cluster_stat_info to [%ld] on client, ret=%d",
                use_paxos_num + 1, temp_ret);
    }
  }
  return ret;
}

//add liuzy [MultiUPS] [take_paxos_offline_interface] 20160223:b
int ObUpsManager::recover_paxos_group_offline_version(const int64_t &paxos_idx,
                                                      const ObBitSet<OB_MAX_CLUSTER_COUNT> &bitset)
{
  int ret = OB_SUCCESS;
  ObUpsNode *tmp_node = NULL;
  ObUpsNode * const paxos_head = find_paxos_head_node(paxos_idx);
  if (NULL != paxos_head)
  {
    tmp_node = paxos_head;
    do
    {
      if (is_cluster_offline(tmp_node->cluster_idx_) || bitset.has_member((int32_t)tmp_node->cluster_idx_))
      {
        tmp_node = tmp_node->next_cluster_node();
      }
      else
      {
        for (int64_t idx = 0; idx < tmp_node->ups_array_->count(); ++idx)
        {
          tmp_node->ups_array_->at(idx).offline_version_ = OB_DEFAULT_OFFLINE_VERSION;
        }
        tmp_node = tmp_node->next_cluster_node();
      }
    } while(paxos_head != tmp_node);
  }
  else
  {
    ret = OB_UPS_MANAGER_NODE_INEXISTENT;
    YYSYS_LOG(WARN, "paxos group:[%ld] is not existent, ret=%d", paxos_idx, ret);
  }
  return ret;
}
int ObUpsManager::set_paxos_ups_offline_version(const int64_t &paxos_idx, const int64_t &offline_version)
{
  int ret = OB_SUCCESS;
  ObUpsNode *paxos_head = find_paxos_head_node(paxos_idx);
  if (NULL == paxos_head)
  {
    ret = OB_UPS_MANAGER_NODE_INEXISTENT;
    YYSYS_LOG(ERROR, "find paxos[%ld] head node failed", paxos_idx);
  }
  else
  {
    ObUpsNode *tmp_node = paxos_head;
    do
    {
      for (int32_t i = 0; i < tmp_node->ups_array_->count(); ++i)
      {
        tmp_node->ups_array_->at(i).offline_version_ = offline_version;
        YYSYS_LOG(DEBUG, "set ups offline version:[%ld]", tmp_node->ups_array_->at(i).offline_version_);
      }
      tmp_node = tmp_node->next_cluster_node();
    } while (paxos_head != tmp_node);
  }
  return ret;
}

void ObUpsManager::set_paxos_status(const int64_t &paxos_idx, ObPaxosClusterStatus new_stat)
{
  paxos_group_stat_[paxos_idx] = new_stat;
}

void ObUpsManager::change_paxos_stat(const int64_t &paxos_idx, const ObPaxosClusterStatus new_stat)
{
  if (get_paxos_status(paxos_idx) != UPS_NODE_STAT_INVALID)
  {
    set_paxos_status(paxos_idx, new_stat);
  }
}

bool ObUpsManager::is_paxos_existent(const int64_t &paxos_idx) const
{
  bool ret = false;
  if (NULL != find_paxos_head_node(paxos_idx))
  {
    ret = true;
  }
  return ret;
}

bool ObUpsManager::is_paxos_group_offline(const int64_t &paxos_idx) const
{
  return paxos_group_stat_[paxos_idx] == UPS_NODE_STAT_OFFLINE ? true : false;
}

bool ObUpsManager::is_paxos_group_active(const int64_t &paxos_idx) const
{
  //  return paxos_group_stat_[paxos_idx] != UPS_NODE_STAT_OFFLINE ? true : false;
  return paxos_group_stat_[paxos_idx] == UPS_NODE_STAT_ACTIVE ? true : false;
}

ObPaxosClusterStatus ObUpsManager::get_paxos_status(const int64_t &paxos_idx) const
{
  return paxos_group_stat_[paxos_idx];
}

void ObUpsManager::init_paxos_group_stat_array()
{
  for (int64_t paxos_idx = 0; paxos_idx < MAX_UPS_COUNT_ONE_CLUSTER; ++paxos_idx)
  {
    paxos_group_stat_[paxos_idx] = UPS_NODE_STAT_ACTIVE;
  }
}

void ObUpsManager::init_cluster_stat_array()
{
  for (int64_t cluster_idx = 0; cluster_idx < OB_MAX_CLUSTER_COUNT; ++cluster_idx)
  {
    cluster_stat_[cluster_idx] = UPS_NODE_STAT_ACTIVE;
  }
}
//add 20160223:e
void ObUpsManager::init_freeze_paxos_group_stat_array()
{
  for (int64_t paxos_idx = 0; paxos_idx < MAX_UPS_COUNT_ONE_CLUSTER ;++paxos_idx)
  {
    freeze_stat_[paxos_idx] = false;
  }
}

int ObUpsManager::decide_freeze_stat(const int64_t &paxos_idx)
{
  int ret = OB_SUCCESS;
  yysys::CThreadGuard guard(&ups_array_mutex_);
  ObUpsNode *paxos_head = NULL;
  int64_t count = 0;
  bool freeze_stat = false;
  if (!is_paxos_group_offline(paxos_idx))
  {
    if (NULL == (paxos_head = find_paxos_head_node(paxos_idx)))
    {
      ret = OB_UPS_MANAGER_NODE_INEXISTENT;
      YYSYS_LOG(WARN, "Find paxos:[%ld] head node addr failed, ret=%d.", paxos_idx, ret);
    }
    else
    {
      ObUpsNode *tmp_node = paxos_head;
      common::ObArray<ObUps> *&ups_array = tmp_node->ups_array_;
      do
      {
        for (int64_t ups_idx = 0; ups_idx < ups_array->count(); ++ups_idx)
        {
          if (UPS_STAT_OFFLINE != ups_array->at(ups_idx).stat_
              && UPS_FROZEN == ups_array->at(ups_idx).minor_freeze_stat_)
          {
            ++count;
          }
          else if (UPS_STAT_OFFLINE != ups_array->at(ups_idx).stat_
                   && UPS_FROZEN_FAIL == ups_array->at(ups_idx).minor_freeze_stat_)
          {
            freeze_stat = true;
            break;
          }
        }
        tmp_node = tmp_node->next_cluster_node();
      } while (tmp_node != paxos_head);
    }
  }
  if (freeze_stat)
  {
    freeze_stat_[paxos_idx] = true;
    YYSYS_LOG(WARN, "frozen memtable failed,paxos_id=%ld",paxos_idx);
  }
  else if (paxos_ups_quorum_scales_[paxos_idx]/2 + 1 <= count)
  {
    freeze_stat_[paxos_idx] = true;
  }

  return ret;
}

void ObUpsManager::decide_minor_freeze_done()
{
  bool minor_freeze_done = false;
  for (int64_t idx = 0; idx < use_paxos_num_ ;++idx)
  {
    YYSYS_LOG(DEBUG, "idx=%ld,freeze_stat_[idx]=%d", idx, freeze_stat_[idx]);
    if (freeze_stat_[idx])
    {
      minor_freeze_done = true;
    }
    else
    {
      minor_freeze_done = false;
      break;
    }
  }
  if (minor_freeze_done)
  {
    root_server_->set_minor_freeze_flag(false);
    init_freeze_paxos_group_stat_array();
  }
}

void ObUpsManager::check_minor_frozen_and_set_stat()
{
  if (root_server_->get_minor_freeze_flag())
  {
    for (int64_t idx = 0 ; idx < use_paxos_num_ ;++ idx)
    {
      if (freeze_stat_[idx])
      {
        YYSYS_LOG(INFO, "this paxos group minor freeze success,idx=%ld", idx);
      }
      else if (OB_SUCCESS != decide_freeze_stat(idx))
      {
        YYSYS_LOG(WARN, "decide freeze stat failed");
      }
    }
    decide_minor_freeze_done();
    YYSYS_LOG(DEBUG, "root_server_->get_minor_freeze_flag()=%d", root_server_->get_minor_freeze_flag());
  }
}

//add liuzy [MultiUPS] [add_cluster_interface] 20160311:b
int ObUpsManager::add_new_cluster(int64_t new_cluster_num, int64_t new_config)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool is_renew_config = true;
  ObArray<ObUpsNode *> node_array;
  ObUpsNode *new_cluster_head = NULL;
  if (root_server_->get_rs_role() == ObRoleMgr::SLAVE)
  {
    is_renew_config = false;
  }
  if (OB_SUCCESS != (ret = cons_new_cluster(new_cluster_num, new_cluster_head, &node_array)))
  {
    YYSYS_LOG(ERROR, "construct new cluster failed, ret=%d", ret);
  }
  else if (is_renew_config && OB_SUCCESS != (ret = root_server_->renew_config_use_num(new_config, UPS_CLUSTER_SCALE)))
  {
    YYSYS_LOG(ERROR, "alter system config 'use_cluster_num' failed, ret=%d", ret);
  }
  else if (is_renew_config && OB_SUCCESS != (ret = root_server_->update_use_num_of_cluster_stat_info(get_new_frozen_version()+1,
                                                                                                     new_config,UPS_CLUSTER_SCALE)))
  {
    YYSYS_LOG(ERROR, "update use_num in __all_cluster_stat_info failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = link_new_cluster(new_cluster_head)))
  {
    YYSYS_LOG(ERROR, "add new cluster to link failed, ret=%d", ret);
    if (is_renew_config && OB_SUCCESS != (tmp_ret = root_server_->renew_config_use_num(new_config - new_cluster_num,
                                                                                       UPS_CLUSTER_SCALE)))
    {
      YYSYS_LOG(ERROR, "recover 'use_cluster_num' failed, please alter this system config to [%ld] on client, ret=%d",
                new_config - new_cluster_num, tmp_ret);
    }
    if (is_renew_config && OB_SUCCESS != (tmp_ret = root_server_->update_use_num_of_cluster_stat_info(get_new_frozen_version()+1,
                                                                                                      new_config - new_cluster_num, UPS_CLUSTER_SCALE  )))
    {
      YYSYS_LOG(ERROR, "recover 'use_cluster_num' failed, please alter this system config in __all_cluster_stat_info to [%ld] on client, ret=%d",
                new_config - new_cluster_num, tmp_ret);
    }
  }


  if (OB_SUCCESS != ret)
  {
    free_ups_node(&node_array);
  }
  return ret;
}
int ObUpsManager::cons_new_cluster(int64_t new_cluster_num, ObUpsNode *&new_cluster_head,
                                   ObArray<ObUpsNode*> *node_array)
{
  if (root_server_->get_rs_role() == ObRoleMgr::SLAVE)
  {
    YYSYS_LOG(INFO, "current use_cluster_num:[%ld]", use_cluster_num_);
  }
  int ret = OB_SUCCESS;
  ObUpsNode *head_node = OB_NEW(ObUpsNode, ObModIds::OB_RS_UPS_MANAGER);
  if (NULL == head_node)
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    YYSYS_LOG(ERROR, "Failed to allocate memory for ObUpsNode, ret=%d.", ret);
  }
  else if (NULL == node_array)
  {
    OB_DELETE(ObUpsNode, ObModIds::OB_RS_UPS_MANAGER, head_node);
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(ERROR, "node array pointer is NULL, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = node_array->push_back(head_node)))
  {
    OB_DELETE(ObUpsNode, ObModIds::OB_RS_UPS_MANAGER, head_node);
    YYSYS_LOG(ERROR, "push new ups node pointer to array failed, ret=%d", ret);
  }
  else
  {
    new_cluster_head = head_node;
    for (int64_t num = 0; OB_SUCCESS == ret && num < new_cluster_num; ++num)
    {
      int64_t cluster_idx = use_cluster_num_ + num;
      for (int64_t paxos_idx = 0; OB_SUCCESS == ret && paxos_idx < use_paxos_num_; ++paxos_idx)
      {
        if (0 == num && 0 == paxos_idx)
        {
          head_node->cluster_idx_ = cluster_idx;
          head_node->paxos_idx_ = paxos_idx;
        }
        else if (OB_SUCCESS != (ret = add_node(cluster_idx, paxos_idx, head_node, node_array)))
        {
          YYSYS_LOG(WARN, "Add node cluster_idx:[%ld] paxos_idx:[%ld] failed, ret=%d.",
                    cluster_idx, head_node->paxos_idx_, ret);
          break;
        }
      }
    }
  }
  return ret;
}
int ObUpsManager::link_new_cluster(ObUpsNode * const new_cluster_head)
{
  int ret = OB_SUCCESS;
  bool find = false;
  ObUpsNode *first_cluster_head = new_cluster_head;
  ObUpsNode *final_cluster_head = new_cluster_head;
  ObUpsNode *last_cluster_head = NULL;

  yysys::CThreadGuard guard(&ups_array_mutex_);
  if (NULL == new_cluster_head)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(ERROR, "new cluster head pointer is NULL, ret=%d", ret);
  }
  else if (NULL == (last_cluster_head = find_cluster_head_node(new_cluster_head->cluster_idx_ - 1)))
  {
    ret = OB_UPS_MANAGER_NODE_INEXISTENT;
    YYSYS_LOG(ERROR, "cluster:[%ld] head node is not existent, ret=%d", new_cluster_head->cluster_idx_ - 1, ret);
  }
  else if (NULL != find_cluster_head_node(new_cluster_head->cluster_idx_))
  {
    find = true;
    YYSYS_LOG(WARN, "new cluster[%ld] already linked", new_cluster_head->cluster_idx_);
  }
  else
  {
    while (first_cluster_head != final_cluster_head->next_cluster_node())
    {
      final_cluster_head = final_cluster_head->next_cluster_node();
    }
  }
  if (OB_SUCCESS == ret && !find)
  {
    do
    {
      YYSYS_LOG(DEBUG, "first new cluster idx:[%ld], final new cluster idx:[%ld]",
                first_cluster_head->cluster_idx_, final_cluster_head->cluster_idx_);
      final_cluster_head->set_next_cluster_node(last_cluster_head->next_cluster_node());
      last_cluster_head->set_next_cluster_node(first_cluster_head);
      last_cluster_head = last_cluster_head->next_paxos_group_node();
      first_cluster_head = first_cluster_head->next_paxos_group_node();
      final_cluster_head = final_cluster_head->next_paxos_group_node();
    } while (new_cluster_head != first_cluster_head);
  }
  print_ups_manager();
  return ret;
}
void ObUpsManager::free_ups_node(ObArray<ObUpsNode*> *node_array)
{
  if (NULL != node_array)
  {
    for (int64_t idx = 0; idx < node_array->count(); ++idx)
    {
      ob_free(node_array->at(idx));
    }
  }
  else
  {
    YYSYS_LOG(WARN, "node array is null, have no ups node to free");
  }
}
//add 20160311:e

int ObUpsManager::delink_last_cluster(const int64_t cluster_idx)
{
  int ret = OB_SUCCESS;
  ObUpsNode *last_cluster_head = find_cluster_head_node(cluster_idx);
  ObUpsNode *pre_cluster_head = NULL, *paxos_group_node = NULL;
  if (NULL == last_cluster_head)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(ERROR, "last cluster:[%ld] head node is not existent, ret=%d",cluster_idx, ret);
  }
  else if (NULL == (pre_cluster_head = find_cluster_head_node(cluster_idx - 1)))
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(ERROR, "previous cluster:[%ld] head node is not existent, ret=%d",cluster_idx - 1, ret);
  }
  else
  {
    paxos_group_node = last_cluster_head;
    YYSYS_LOG(INFO, "to delete cluster [%ld], the previous cluster[%ld]", cluster_idx, cluster_idx - 1);
    while (true)
    {
      pre_cluster_head->set_next_cluster_node(paxos_group_node->next_cluster_node());
      YYSYS_LOG(INFO, "node[cid:%ld][pid:%ld] --link to--> node[cid:%ld][pid:%ld]", pre_cluster_head->cluster_idx_, pre_cluster_head->paxos_idx_,
                paxos_group_node->next_cluster_node()->cluster_idx_, paxos_group_node->next_cluster_node()->paxos_idx_);
      paxos_group_node->set_next_cluster_node(NULL);
      pre_cluster_head = pre_cluster_head->next_paxos_group_node();

      if (paxos_group_node->next_paxos_group_node() != last_cluster_head)
      {
        paxos_group_node = paxos_group_node->next_paxos_group_node();
      }
      else
      {
        paxos_group_node->set_next_paxos_group_node(NULL);
        break;
      }
    }
    
    while (last_cluster_head)
    {
      paxos_group_node = last_cluster_head->next_paxos_group_node();
      YYSYS_LOG(INFO, "delete node[cid:%ld][pid:%ld]", last_cluster_head->cluster_idx_, last_cluster_head->paxos_idx_);
      ob_free(last_cluster_head);
      last_cluster_head = paxos_group_node;
    }
  }
  print_ups_manager();
  return ret;
}

int ObUpsManager::del_last_cluster(const int64_t cluster_idx)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t use_cluster_num = root_server_->get_config().use_cluster_num - 1;
  bool is_master = root_server_->is_master();
  if (is_master && OB_SUCCESS != (ret = root_server_->renew_config_use_num(use_cluster_num, UPS_CLUSTER_SCALE)))
  {
    YYSYS_LOG(ERROR, "alter system config 'use_cluster_num' failed, ret=%d", ret);
  }
  if (is_master && OB_SUCCESS != (ret = root_server_->update_use_num_of_cluster_stat_info(get_new_frozen_version()+1,
                                                                                          use_cluster_num,UPS_CLUSTER_SCALE)))
  {
    YYSYS_LOG(ERROR, "update use_num in __all_cluster_stat_info failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = delink_last_cluster(cluster_idx)))
  {
    YYSYS_LOG(ERROR, "delink last cluster failed, ret=%d", ret);
    if (is_master && OB_SUCCESS != (tmp_ret = root_server_->renew_config_use_num(use_cluster_num + 1, UPS_CLUSTER_SCALE)))
    {
      YYSYS_LOG(ERROR, "recover 'use_cluster_num' failed, please alter this system config to [%ld] on client, ret=%d",
                use_cluster_num + 1, tmp_ret);
    }
    if (is_master && OB_SUCCESS != (tmp_ret = root_server_->update_use_num_of_cluster_stat_info(get_new_frozen_version()+1,
                                                                                                use_cluster_num + 1,UPS_CLUSTER_SCALE)))
    {
      YYSYS_LOG(ERROR, "recover 'use_cluster_num' failed, please alter this system config in __all_cluster_stat_info to [%ld] on client, ret=%d",
               use_cluster_num + 1, tmp_ret);
    }
  }
  return ret;
}

//add liuzy [MultiUPS] [take_cluster_offline_interface] 20160325:b
int ObUpsManager::check_paxos_and_cluster_vaild(const int64_t cluster_idx, const int64_t paxos_idx)
{
  int ret = OB_SUCCESS;
  if (0 <= cluster_idx && use_cluster_num_ > cluster_idx)
  {
    ret = cluster_stat_[cluster_idx] != UPS_NODE_STAT_OFFLINE ? OB_SUCCESS : OB_CURRENT_CLUSTER_OFFLINE;
  }
  //add liuzy [MultiUPS] [UPS_Manager] [BugFix]20160516:b
  else
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(ERROR, "cluster_idx must be in [0,%ld), ret=%d", use_cluster_num_, ret);
  }
  //add 20160516:e
  if (OB_SUCCESS == ret && 0 <= paxos_idx && use_paxos_num_ > paxos_idx)
  {
    ret = paxos_group_stat_[paxos_idx] != UPS_NODE_STAT_OFFLINE ? OB_SUCCESS : OB_CURRENT_PAXOS_GROUP_OFFLINE;
  }
  //add liuzy [MultiUPS] [UPS_Manager] [BugFix]20160516:b
  else if (OB_SUCCESS == ret)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(ERROR, "paxos_idx must be in [0,%ld), ret=%d", use_paxos_num_, ret);
  }
  //add 20160516:e
  return ret;
}

int ObUpsManager::set_cluster_ups_offline_version(const int64_t &cluster_idx, const int64_t &offline_version)
{
  int ret = OB_SUCCESS;
  ObUpsNode *cluster_head = find_cluster_head_node(cluster_idx);
  if (NULL == cluster_head)
  {
    ret = OB_UPS_MANAGER_NODE_INEXISTENT;
    YYSYS_LOG(WARN, "find cluster[%ld] head node failed", cluster_idx);
  }
  else
  {
    ObUpsNode *tmp_node = cluster_head;
    do
    {
      for (int32_t i = 0; i < tmp_node->ups_array_->count(); ++i)
      {
        tmp_node->ups_array_->at(i).offline_version_ = offline_version;
        YYSYS_LOG(DEBUG, "set ups offline version:[%ld]", tmp_node->ups_array_->at(i).offline_version_);
      }
      tmp_node = tmp_node->next_paxos_group_node();
    } while (cluster_head != tmp_node);
  }
  return ret;
}
int ObUpsManager::get_ups_offline_count_next_version_by_paxos_id(int64_t &ups_count, const int64_t& paxos_idx) const
{
  int ret = OB_SUCCESS;
  ups_count = 0;
  ObUpsNode * const paxos_head = find_paxos_head_node(paxos_idx);
  if (initialized_)
  {
    if (NULL != paxos_head)
    {
      ObUpsNode *tmp_node = paxos_head;
      do
      {
        if (root_server_->is_cluster_offline(tmp_node->cluster_idx_))
        {
          common::ObArray<ObUps> *&ups_array = tmp_node->ups_array_;
          for (int64_t ups_idx = 0; ups_idx < ups_array->count(); ++ups_idx)
          {
            if (UPS_STAT_NOTSYNC == ups_array->at(ups_idx).stat_
                && UPS_STAT_SYNC == ups_array->at(ups_idx).stat_)
            {
              ++ups_count;
            }
          }
        }
        tmp_node = tmp_node->next_cluster_node();
      } while (paxos_head != tmp_node);
    }
    else
    {
      ret = OB_UPS_MANAGER_NODE_INEXISTENT;
      YYSYS_LOG(WARN, "Find paxos:[%ld] head failed, ret=%d.", paxos_idx, ret);
    }
  }
  return ret;
}
int ObUpsManager::get_ups_list_by_cluster_id(common::ObUpsList &ups_list, const int64_t& cluster_idx) const
{
  int ret = OB_SUCCESS;
  int count = 0;
  ObUpsNode *cluster_head = find_cluster_head_node(cluster_idx);
  if (initialized_)
  {
    if (NULL != cluster_head)
    {
      ObUpsNode *tmp_node = cluster_head;
      do
      {
        common::ObArray<ObUps> *&ups_array = tmp_node->ups_array_;
        for (int64_t ups_idx = 0; ups_idx < ups_array->count(); ++ups_idx)
        {
          int64_t rs_last_frozen_version = root_server_->get_last_frozen_version();
          if (OB_INVALID_VERSION == rs_last_frozen_version)
          {
            ret = OB_RS_INVALID_LAST_FROZEN_VERSION;
            YYSYS_LOG(ERROR, "after rs set report_flag:true, "
                      "root_server_->get_last_frozen_version() must not be OB_INVALID_VERSION, ret=%d", ret);
            root_server_->kill_self();
          }
          else
          {
            if (UPS_STAT_OFFLINE != ups_array->at(ups_idx).stat_
                && UPS_STAT_NOTSYNC != ups_array->at(ups_idx).stat_
                && (rs_last_frozen_version == ups_array->at(ups_idx).last_frozen_version_
                    || rs_last_frozen_version == ups_array->at(ups_idx).last_frozen_version_ - 1))
            {
              ups_array->at(ups_idx).convert_to(ups_list.ups_array_[count++]);
            }
          }
        }
        tmp_node = tmp_node->next_paxos_group_node();
      } while (cluster_head != tmp_node);
    }
    else
    {
      ret = OB_UPS_MANAGER_NODE_INEXISTENT;
      YYSYS_LOG(WARN, "Find cluster:[%ld] head failed, ret=%d.", cluster_idx, ret);
    }
  }
  ups_list.ups_count_ = count;
  return ret;
}

int ObUpsManager::recover_cluster_offline_version(const int64_t &cluster_idx,
                                                  const ObBitSet<MAX_UPS_COUNT_ONE_CLUSTER> &bitset)
{
  int ret = OB_SUCCESS;
  ObUpsNode *tmp_node = NULL;
  ObUpsNode * const cluster_head = find_cluster_head_node(cluster_idx);
  if (NULL != cluster_head)
  {
    tmp_node = cluster_head;
    do
    {
      if (is_paxos_group_offline(tmp_node->paxos_idx_) || bitset.has_member((int32_t)tmp_node->paxos_idx_))
      {
        tmp_node = tmp_node->next_paxos_group_node();
      }
      else
      {
        for (int64_t idx = 0; idx < tmp_node->ups_array_->count(); ++idx)
        {
          tmp_node->ups_array_->at(idx).offline_version_ = OB_DEFAULT_OFFLINE_VERSION;
        }
        tmp_node = tmp_node->next_paxos_group_node();
      }
    } while(cluster_head != tmp_node);
  }
  else
  {
    ret = OB_UPS_MANAGER_NODE_INEXISTENT;
    YYSYS_LOG(WARN, "cluster:[%ld] is not existent, ret=%d", cluster_idx, ret);
  }
  return ret;
}

bool ObUpsManager::is_initialized()
{
  return initialized_;
}

bool ObUpsManager::is_cluster_online(const int64_t &cluster_idx) const
{
    return (cluster_idx < use_cluster_num_ && cluster_stat_[cluster_idx] == UPS_NODE_STAT_ACTIVE) ? true : false;
}

bool ObUpsManager::is_cluster_offline(const int64_t &cluster_idx)
{
  return cluster_stat_[cluster_idx] == UPS_NODE_STAT_OFFLINE ? true : false;
}

bool ObUpsManager::is_cluster_existent(const int64_t &cluster_idx) const
{
  bool ret = false;
  if (NULL != find_cluster_head_node(cluster_idx))
  {
    ret = true;
  }
  else
  {
    YYSYS_LOG(WARN, "cluster [%ld] is not existent", cluster_idx);
  }
  return ret;
}

bool ObUpsManager::is_ups_offline_count_valid(const int64_t paxos_id, const int64_t down_count,
                                              const int64_t quorum_scale, int64_t &permit_count)
{
  bool ret = false;
  int64_t offline_count_next_version = 0;
  if (OB_SUCCESS == get_ups_offline_count_next_version_by_paxos_id(offline_count_next_version, paxos_id))
  {
    int64_t ups_count = get_ups_count(paxos_id);
    permit_count = ups_count - offline_count_next_version - (quorum_scale / 2 + 1);
    if (is_use_paxos_)
    {
      if(down_count <= permit_count)
      {
        ret = true;
      }
    }
    else
    {
      if(down_count < ups_count)
      {
        ret = true;
      }
    }
  }
  YYSYS_LOG(DEBUG, "paxos id[%ld], down count[%ld], quorum scale[%ld], permit count[%ld]",
            paxos_id, down_count, quorum_scale, permit_count);
  return ret;
}

bool ObUpsManager::is_cluster_ups_offline_count_valid(const int64_t cluster_idx, int32_t &permit_count, int64_t &paxos_id)
{
  bool ret = true;
  int32_t paxos_group_array[MAX_UPS_COUNT_ONE_CLUSTER];
  memset(paxos_group_array, 0, MAX_UPS_COUNT_ONE_CLUSTER*sizeof(int32_t));
  ObUpsNode *cluster_head = NULL, *tmp_node = NULL;
  cluster_head = find_cluster_head_node(cluster_idx);
  if (NULL == cluster_head)
  {
    YYSYS_LOG(WARN, "find cluster failed, idx[%ld]", cluster_idx);
  }
  else
  {
    tmp_node = cluster_head;
    do
    {
      for (int64_t ups_idx = 0 ; ups_idx < tmp_node->ups_array_->count() ;++ups_idx)
      {
        if (UPS_STAT_OFFLINE != tmp_node->ups_array_->at(ups_idx).stat_)
        {
          paxos_group_array[tmp_node->ups_array_->at(ups_idx).paxos_id_] ++;
        }
      }
      tmp_node = tmp_node->next_paxos_group_node();
    } while (tmp_node != cluster_head);
  }
  int64_t tmp_permit_count = 0;
  for (int64_t pid = 0 ;pid < use_paxos_num_ ;pid ++)
  {
    if ( !is_ups_offline_count_valid(pid, (int64_t)paxos_group_array[pid], paxos_ups_quorum_scales_[pid], tmp_permit_count ) )
    {
      ret = false;
      paxos_id = pid;
      permit_count = static_cast<int32_t>(tmp_permit_count);
      break;
    }
  }
  return ret;
}

void ObUpsManager::set_cluster_status(const int64_t &cluster_idx, ObPaxosClusterStatus new_stat)
{
  cluster_stat_[cluster_idx] = new_stat;
}

void ObUpsManager::change_cluster_stat(const int64_t &cluster_idx, const ObPaxosClusterStatus new_stat)
{
  if (get_cluster_status(cluster_idx) != UPS_NODE_STAT_INVALID)
  {
    set_cluster_status(cluster_idx, new_stat);
  }
}

ObPaxosClusterStatus ObUpsManager::get_cluster_status(const int64_t &cluster_idx) const
{
  return cluster_stat_[cluster_idx];
}
//add 20160325:e

//add pangtianze [Paxos ups_replication] 20150603:b
bool ObUpsManager::check_most_ups_registered(const int64_t paxos_id)
{
  bool majority_ups_registered = false;
  int64_t ups_registered_count = 0;
  ObUpsNode *paxos_head = NULL, *tmp_node = NULL;
  if (NULL == (paxos_head = find_paxos_head_node(paxos_id)))
  {
    YYSYS_LOG(WARN, "Find paxos:[%ld] head node addr failed.", paxos_id);
  }
  else
  {
    tmp_node = paxos_head;
    do
    {
      for (int64_t ups_idx = 0; ups_idx < tmp_node->ups_array_->count(); ++ups_idx)
      {
        if (UPS_STAT_OFFLINE != tmp_node->ups_array_->at(ups_idx).stat_)
        {
          ++ups_registered_count;
        }
      }
      tmp_node = tmp_node->next_cluster_node();
    } while (tmp_node != paxos_head);
  }
  if (ups_registered_count > paxos_ups_quorum_scales_[paxos_id] / 2)
  {
    majority_ups_registered = true;
  }
  return majority_ups_registered;
}
//add:e
//add pangtianze [MultiUPS][merge with paxos] 20170519:b
void ObUpsManager::is_cluster_alive_with_ups(bool* is_cluster_alive)
{
  for (int32_t i = 0; i < use_cluster_num_; i++)
  {
    if (is_cluster_offline(i))
    {
      is_cluster_alive[i] = true;
    }
  }
}
bool ObUpsManager::is_cluster_alive_with_ups(const int32_t cluster_id)
{
  return is_cluster_offline(cluster_id);
}
//add:e
//add chujiajia, mod pangtianze [MultiUPS][merge with paxos] 20170518:b
int ObUpsManager::select_ups_master_with_highest_lsn_and_term(const int64_t paxos_idx, const common::ObServer *exp_ups_addr)
{
  int ret = OB_ERROR;
  //add liuzy [MultiUPS] [take_paxos_offline_interface] 20160306:b
  if (is_paxos_group_offline(paxos_idx))
  {
    ret = OB_CURRENT_PAXOS_GROUP_OFFLINE;
    YYSYS_LOG(WARN, "current paxos group:[%ld] is offline", paxos_idx);
  }
  else if (0 < waiting_ups_finish_time_)
  {
    int64_t now = yysys::CTimeUtil::getTime();
    if (now > waiting_ups_finish_time_)
    {
      waiting_ups_finish_time_ = 0;
    }
  }
  //add 20160306:e
  //mod liuzy [MultiUPS] [take_paxos_offline_interface] 20160306:b
  //  if (!has_master(paxos_idx)
  //      && is_use_paxos_
  //      && (get_ups_count(paxos_idx) > max_deployed_ups_count_ / 2))
  else if (!has_master(paxos_idx)
           && is_use_paxos_
           && (get_ups_count(paxos_idx) > paxos_ups_quorum_scales_[paxos_idx] / 2))
    //mod 20160306:e

  {
    int64_t highest_lsn = OB_INVALID_VALUE;
    int64_t highest_term = OB_DEFAULT_TERM;
    int64_t cluster_idx = INVALID_INDEX;
    int64_t master_ups_idx = INVALID_INDEX;
    ObUpsNode *paxos_head = NULL, *master_node = NULL;
    (void)cluster_idx;
    //yysys::CThreadGuard guard(&ups_array_mutex_); //del pangtianze [merge with paxos] 20170725:b:e
    if (NULL == (paxos_head = find_paxos_head_node(paxos_idx)))
    {
      YYSYS_LOG(WARN, "Find paxos:[%ld] head node addr failed.", paxos_idx);
      ret = OB_UPS_MANAGER_NODE_INEXISTENT;
    }
    else
    {
      ObUpsNode *tmp_node = paxos_head;
      do
      {
        for (int64_t ups_idx = 0; ups_idx < tmp_node->ups_array_->count(); ++ups_idx)
        {
          if (UPS_STAT_OFFLINE != tmp_node->ups_array_->at(ups_idx).stat_ &&
              tmp_node->ups_array_->at(ups_idx).log_term_ >= highest_term &&
              tmp_node->ups_array_->at(ups_idx).log_seq_num_ > highest_lsn)
          {
            highest_lsn = tmp_node->ups_array_->at(ups_idx).log_seq_num_;
            highest_term = tmp_node->ups_array_->at(ups_idx).log_term_;
            cluster_idx = tmp_node->cluster_idx_;
            master_ups_idx = ups_idx;
            master_node = tmp_node;
            master_idx_array_new_[paxos_idx].first = tmp_node->cluster_idx_;
            master_idx_array_new_[paxos_idx].second = ups_idx;
          }
        }
        tmp_node = tmp_node->next_cluster_node();
      }while (tmp_node != paxos_head);
    }
    //add pangtianze [paxos rs_election] 20160918:b
    if (exp_ups_addr != NULL) ///select master ups that user expected
    {
      ObUpsNode *temp_node = NULL;
      int64_t ups_index = find_ups_index(*exp_ups_addr, temp_node);
      if (temp_node != NULL
          && highest_term == temp_node->ups_array_->at(ups_index).log_term_
          && highest_lsn == temp_node->ups_array_->at(ups_index).log_seq_num_
          && UPS_STAT_OFFLINE != temp_node->ups_array_->at(ups_index).stat_)
      {
        cluster_idx = temp_node->cluster_idx_;
        master_ups_idx = ups_index;
        master_node = temp_node;
        master_idx_array_new_[paxos_idx].first = temp_node->cluster_idx_;
        master_idx_array_new_[paxos_idx].second = ups_index;
      }
    }
    else
    {
      ///first select ups in main cluster
      int64_t master_cluster_id = worker_->get_root_server().get_master_cluster_id();
      if (master_cluster_id != master_idx_array_new_[paxos_idx].first)

      {
        ObUpsNode* cluster_head = NULL;
        cluster_head = find_node(master_cluster_id, paxos_idx);
        for (int64_t ups_idx = 0; cluster_head != NULL && ups_idx < cluster_head->ups_array_->count(); ++ups_idx)
        {
          if (highest_term == cluster_head->ups_array_->at(ups_idx).log_term_
              && highest_lsn == cluster_head->ups_array_->at(ups_idx).log_seq_num_
              && UPS_STAT_OFFLINE != cluster_head->ups_array_->at(ups_idx).stat_
              && master_cluster_id == cluster_head->ups_array_->at(ups_idx).cluster_id_
              )
          {
            cluster_idx = cluster_head->cluster_idx_;
            master_ups_idx = ups_idx;
            master_node = cluster_head;
            master_idx_array_new_[paxos_idx].first = cluster_head->cluster_idx_;
            master_idx_array_new_[paxos_idx].second = ups_idx;
          }

        }
      }
    }
    //add:e
    //del pangtianze [MultiUPS] [merge with paxos] 20170518:b
    /*//add chujiajia [Paxos rs_election] 20151217:b
    else if(is_old_ups_master_regist_[paxos_idx])
      ObUpsNode *temp_node = NULL;
      int64_t ups_index = find_ups_index(old_ups_master_[paxos_idx], temp_node);
      //mod liuzy [MultiUPS] [BugFix] 20160727:b
//      if((temp_node->ups_array_->at(ups_index).log_term_ >= highest_term) &&
//          (temp_node->ups_array_->at(ups_index).log_seq_num_ >= highest_lsn))
      if(NULL != temp_node && temp_node->ups_array_->count() > ups_index &&
         (temp_node->ups_array_->at(ups_index).log_term_ >= highest_term) &&
          (temp_node->ups_array_->at(ups_index).log_seq_num_ >= highest_lsn))
      //mod 20160727:e
        {
          master_idx_array_new_[paxos_idx].first = temp_node->cluster_idx_;
          master_idx_array_new_[paxos_idx].second = ups_index;
          highest_lsn = temp_node->ups_array_->at(ups_index).log_seq_num_;
          highest_term = temp_node->ups_array_->at(ups_index).log_term_;
          cluster_idx = master_idx_array_new_[paxos_idx].first;
          master_ups_idx = master_idx_array_new_[paxos_idx].second;
          master_node = temp_node;
          YYSYS_LOG(INFO, "old_master_ups will be new ups master [%s]!", temp_node->ups_array_->at(ups_index).addr_.to_cstring());
          //LBZ_LOG(INFO, "old_master_ups will be new ups master [%s]!", temp_node->ups_array_->at(ups_index).addr_.to_cstring());
        }
    }*/
    //del 20170518:e

    //mod liuzy [Paxos rs_election] 20160308:b
    /*Exp: judge master_ups_idx validity*/
    //    YYSYS_LOG(INFO, "paxos_idx:%ld, term[%ld], lsn[%ld], master_ups[%s]",
    //              paxos_idx, highest_term, highest_lsn, master_node->ups_array_->at(master_ups_idx).addr_.to_cstring());
    if (0 <= master_ups_idx && master_node->ups_array_->count() > master_ups_idx)
    {
      YYSYS_LOG(INFO, "paxos_idx:%ld, term[%ld], lsn[%ld], master_ups[%s]",
                paxos_idx, highest_term, highest_lsn, master_node->ups_array_->at(master_ups_idx).addr_.to_cstring());
    }
    //mod 20160308:e
    //add chujiajia [Paxos rs_election] 20151217:b
    //old_ups_master_[paxos_idx].reset();
    //is_old_ups_master_regist_[paxos_idx] = false;
    //add:e
    if (NULL == master_node)
    {
      YYSYS_LOG(WARN, "no master selected");
    }
    else
    {
      change_ups_stat(master_idx_array_new_[paxos_idx].first, paxos_idx, master_idx_array_new_[paxos_idx].second, UPS_STAT_MASTER);
      YYSYS_LOG(INFO, "paxos_idx:%ld, new ups master selected, master=%s lsn=%ld term=%ld",
                paxos_idx,
                master_node->ups_array_->at(master_ups_idx).addr_.to_cstring(),
                master_node->ups_array_->at(master_ups_idx).log_seq_num_,
                master_node->ups_array_->at(master_ups_idx).log_term_);
      //LBZSYS_LOG(INFO, "paxos_idx:%ld, new ups master selected, master=%s lsn=%ld term=%ld",
      //          paxos_idx,
      //          master_node->ups_array_.at(master_ups_idx).addr_.to_cstring(),
      //          master_node->ups_array_.at(master_ups_idx).log_seq_num_,
      //          master_node->ups_array_.at(master_ups_idx).log_term_);
      ObUps ups(master_node->ups_array_->at(master_ups_idx));
      refresh_inner_table(ROLE_CHANGE, ups, "null");
      //mod lqc [MultiUPS with paxos]
      // reset_ups_read_percent_by_paxos_id(paxos_idx);  //add peiouya [MultiUPS] [UPS_Manage_Function] 20150811
      reset_ups_read_percent();
      // mod e
      ret = OB_SUCCESS;
    }
  }
  return ret;
}
//add:e


//add chujiajia [Paxos rs_election] 20151229:b
void ObUpsManager::if_all_quorum_changed(char *buf, const int64_t buf_len, int64_t& pos) const // , const int64_t& local_quorum_scale) const
{
  bool all_changed = true;
  int64_t error_paxos_id = -1;
  int64_t error_ups_index = -1;
  ObUpsNode *tmp_node = NULL;
  ObUpsNode *paxos_node = NULL, *cluster_node = NULL;
  (void)error_paxos_id;
  if (NULL != (paxos_node = link_head_))
  {
    do
    {
      cluster_node = paxos_node;
      do
      {
        common::ObArray<ObUps> *&ups_array = cluster_node->ups_array_;
        for (int64_t ups_idx = 0; ups_idx < ups_array->count(); ++ups_idx)
        {
          //if (ups_array->at(ups_idx).quorum_scale_ != local_quorum_scale)
          if (ups_array->at(ups_idx).stat_ != UPS_STAT_OFFLINE
              && ups_array->at(ups_idx).quorum_scale_ != paxos_ups_quorum_scales_[paxos_node->paxos_idx_] )
          {
            all_changed = false;
            error_paxos_id = cluster_node->paxos_idx_;
            error_ups_index = ups_idx;
            tmp_node = cluster_node;
            YYSYS_LOG(WARN, "quorum scale not be changed, ups[%s], quorum_scale[%ld]",
                      cluster_node->ups_array_->at(ups_idx).addr_.to_cstring(), cluster_node->ups_array_->at(ups_idx).quorum_scale_);
            break;
          }
        }
        cluster_node = cluster_node->next_cluster_node();
      } while (paxos_node != cluster_node && all_changed);
      paxos_node = paxos_node->next_paxos_group_node();
    } while (link_head_ != paxos_node && all_changed);
  }

  if(all_changed)
  {
    //databuff_printf(buf, buf_len, pos, "all ups's quorum_scale are same, quorum_scale=%ld\n", local_quorum_scale);// mod lqc[merge with paxos]
    char tmp_str[OB_MAX_PAXOS_UPS_QUORUM_SCALES_LENGTH];
    root_server_->get_ups_quorum_scales_from_array(tmp_str);
    databuff_printf(buf, buf_len, pos, "all ups's quorum_scale in each paxos group are same, ups's quorum_scale in each paxos group = [%s]\n", tmp_str);
  }
  else
  {
    // databuff_printf(buf, buf_len, pos, "quorum_scale in some ups is different from rs_leader, some ups[%s]'s quorum_scale[%ld]",
    //                 tmp_node->ups_array_->at(error_ups_index).addr_.to_cstring(), tmp_node->ups_array_->at(error_ups_index).quorum_scale_);
    char tmp_str[OB_MAX_PAXOS_UPS_QUORUM_SCALES_LENGTH];
    root_server_->get_ups_quorum_scales_from_array(tmp_str);
    ObUps *tmp_ups = &(tmp_node->ups_array_->at(error_ups_index));
    databuff_printf(buf, buf_len, pos, "quorum_scale in some ups is different from rs_leader, [cid|pid]=[%ld|%ld]:ups[%s]'s quorum_scale[%ld], ups quorum scales in each paxos group = %s\n",
                    (*tmp_ups).cluster_id_, (tmp_ups)->paxos_id_, (*tmp_ups).addr_.to_cstring(),(*tmp_ups).quorum_scale_, tmp_str);
  }
}

//add:e

////add chujiajia[Paxos rs_election] 20150217:b
//bool ObUpsManager::get_is_rs_leader()
//{
//  return  worker_->get_role_manager()->is_rs_leader();
//}
//add:e

/*
int ObUpsManager::send_obi_role()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = this->grant_lease(true)))
  {
    YYSYS_LOG(WARN, "failed to send lease msg, err=%d", ret);
  }
  else
  {
    YYSYS_LOG(INFO, "sent lease grant to change obi role");
    ret = OB_RESPONSE_TIME_OUT;
    // wait for the master
    const int64_t sleep_us = 111000; // 111ms
    int64_t total_sleep_us = 0;
    do
    {
      {                         // scoped lock
        yysys::CThreadGuard guard(&ups_array_mutex_);
        if (has_master())
        {
          if (obi_role_ == ups_array_[ups_master_idx_].obi_role_)
          {
            ret = OB_SUCCESS;
            YYSYS_LOG(INFO, "ups master has changed obi_role, master_ups=%s obi_role=%s",
                      ups_array_[ups_master_idx_].addr_.to_cstring(),
                      ups_array_[ups_master_idx_].obi_role_.get_role_str());
            break;
          }
        }
        else
        {
          // no master
          ret = OB_SUCCESS;
          YYSYS_LOG(INFO, "no ups master and don't wait");
          break;
        }
      }
      usleep(sleep_us);
      total_sleep_us += sleep_us;
      YYSYS_LOG(INFO, "waiting ups for changing the obi role, wait_us=%ld", total_sleep_us);
    } while (total_sleep_us < lease_duration_us_);
  }
  reset_ups_read_percent();
  return ret;
}
*/

int ObUpsManager::set_ups_config(int32_t read_master_ups_percentage
                                 ,int32_t merge_master_ups_percentage //[628]
                                 //add bingo[Paxps Cluster.Flow.UPS] 20170116:b
                                 , const int32_t is_strong_consistent
                                 //add:e
                                 ,const ObUpsFlowType flow_type //add pangtianze [Paxos Cluster.Flow.UPS 20170817:b
                                 )
{
  int ret = OB_SUCCESS;
  if ((-1 != read_master_ups_percentage)
      && (0 > read_master_ups_percentage
          || 100 < read_master_ups_percentage))
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(WARN, "invalid param, read_master_ups_percentage=%d, ret=%d", read_master_ups_percentage, ret);
  }
  else
  {
    YYSYS_LOG(INFO, "change ups config, read_master_ups_percentage=%d, merge_master_ups_percentage=%d", read_master_ups_percentage, merge_master_ups_percentage);
    master_ups_read_percentage_ = read_master_ups_percentage;

    master_ups_merge_percentage_ = merge_master_ups_percentage;

    //add pangtianze [Paxos Cluster.Flow.UPS] 20170119:b
    is_strong_consistency_ =  is_strong_consistent;
    //add:e
    yysys::CThreadGuard guard(&ups_array_mutex_);
    //mod pangtianze [Paxos Cluster.Flow.UPS] 20170817:b
    //reset_ups_read_percent();
    reset_ups_read_percent(flow_type);
    //mod:e
  }
  return ret;
}
int  ObUpsManager::get_random_paxos_id_for_group(int64_t& paxos_idx)
{
  int ret = OB_SUCCESS;
  common::ObArray<int64_t> paxos_array;
  {
    for (int64_t paxos_idx = 0; initialized_ && paxos_idx < use_paxos_num_; paxos_idx++)
    {
      if (0 < get_ups_count(paxos_idx))
      {
        paxos_array.push_back(paxos_idx);
      }
    }
  }
  if (0 < paxos_array.count())
  {
    if (OB_SUCCESS != (ret = paxos_array.at(static_cast<int64_t>(::random() % paxos_array.count()), paxos_idx)))
    {
      YYSYS_LOG(WARN, "fail to get one paxos id for group, err=%d", ret);
    }
  }
  return ret;
}
//add lqc [MultiUps 1.0] [#13] 20170420 b
int64_t ObUpsManager::get_use_paxos_num()const
{
  return use_paxos_num_;
}
// add e
// mod lqc [multiups with paxos] 20170610
int ObUpsManager::set_ups_master(const common::ObServer &master, bool did_force)
{
  int ret = OB_SUCCESS;
  int64_t waiting_lease_us = 0;
  int ups_idx = INVALID_INDEX;
  ObUpsNode *ups_node = NULL;
  int64_t now = yysys::CTimeUtil::getTime();
  //add pangtianze [Paxos rs_election] 20161008:b
  worker_->get_root_server().get_rs_node_mgr()->set_can_select_master_ups(false);
  //add:e
  {
    yysys::CThreadGuard guard(&ups_array_mutex_);
    if(NULL == &master)
    {
      YYSYS_LOG(WARN, "master is NULL");
      ret = OB_INVALID_ARGUMENT;
    }
    else if( INVALID_INDEX == (ups_idx = static_cast<int>(find_ups_index (master,ups_node))  ))//get the ups node
    {
      YYSYS_LOG(WARN, "find ups:[%s] failed ", master.to_cstring());
      ret = OB_ENTRY_NOT_EXIST;
    }
    else if (NULL == ups_node)
    {
      YYSYS_LOG(ERROR, "find ups_node is null");
      ret = OB_ERROR;
    }
    else if (UPS_STAT_MASTER == ups_node->ups_array_->at(ups_idx).stat_)
    {
      YYSYS_LOG(WARN, "ups is already the master, ups=%s",master.to_cstring());
      ret = OB_INVALID_ARGUMENT;
    }
    else if ((UPS_STAT_SYNC != ups_node->ups_array_->at(ups_idx).stat_) && !did_force)
    {
      YYSYS_LOG(WARN, "ups is not sync, ups=%s stat=%d lsn=%ld",
                master.to_cstring(), ups_node->ups_array_->at(ups_idx).stat_, ups_node->ups_array_->at(ups_idx).log_seq_num_);
      ret = OB_INVALID_ARGUMENT;
    }
    else if (last_change_ups_master_ + FORCE_CHANGE_UPS_MASTER_INTERVAL > now)
    {
      YYSYS_LOG(WARN, "change ups master too fast,last_change_ups_master = %ld,change_interval = %ld",
                last_change_ups_master_, FORCE_CHANGE_UPS_MASTER_INTERVAL);
      ret = OB_EAGAIN;
    }
    else
    {
      ret = revoke_master_lease(waiting_lease_us,ups_node->paxos_idx_);
    }
  }
  // if (OB_SUCCESS == ret && 0 < waiting_lease_us)
  // {
  //   // wait current lease until timeout, sleep without locking so that the heartbeats will continue
  //   YYSYS_LOG(INFO, "revoke lease failed and we should wait, usleep=%ld", waiting_lease_us);
  //   usleep(static_cast<useconds_t>(waiting_lease_us));
  // }
  bool new_master_selected = false;
  if (OB_SUCCESS == ret)
  {
    yysys::CThreadGuard guard(&ups_array_mutex_);
    // re-check status
    int64_t paxos_idx = ups_node->paxos_idx_;
    if ((did_force) && master == ups_node->ups_array_->at(ups_idx).addr_ && !is_master_lease_valid(ups_node->paxos_idx_))
    {
      change_ups_stat(ups_node->cluster_idx_,ups_node->paxos_idx_,ups_idx,UPS_STAT_MASTER);
      YYSYS_LOG(INFO, "set new ups master, master=%s force=%c",
                master.to_cstring(), did_force?'Y':'N');
      ObUps ups(ups_node->ups_array_->at(ups_idx));
      refresh_inner_table(ROLE_CHANGE, ups, "null");
      new_master_selected = true;
      waiting_ups_finish_time_ = -1;
      reset_ups_read_percent();
    }
    //add pangtianze [Paxos ups_replication] 20170125:b
    else if (!did_force)
    {
      ret = select_new_ups_master(paxos_idx,&master);
      last_change_ups_master_ = yysys::CTimeUtil::getTime();
    }
    //add:e
    else
    {
      // should rarely come here
      waiting_ups_finish_time_ = -1;
      YYSYS_LOG(WARN, "the ups removed or status changed after sleeping, try again, ups=%s", master.to_cstring());
      ret = OB_CONFLICT_VALUE;
    }
    //add:e
  }
  //add pangtianze [Paxos rs_election] 20161008:b
  worker_->get_root_server().get_rs_node_mgr()->set_can_select_master_ups(true); //mod pangtianze [Paxos bugfix] 20170802
  //add:e
  if (new_master_selected)
  {
    this->grant_lease(true);
    last_change_ups_master_ = yysys::CTimeUtil::getTime();
  }

  return ret;
}

int ObUpsManager::revoke_master_lease(int64_t &waiting_lease_us,int64_t& paxos_idx)
{
  int ret = OB_SUCCESS;
  waiting_lease_us = 0;
  //mod lqc [merge multiups with paxos] 20170724 b
  int64_t master_ups_idx = INVALID_INDEX;
  ObUpsNode *ups_node = NULL;
  if (has_master(paxos_idx))
  {
    if (is_master_lease_valid(paxos_idx))
    {
      // the lease is valid now
      if(INVALID_INDEX == (master_ups_idx = find_master_ups_index(paxos_idx,ups_node)))
      {
        YYSYS_LOG(WARN, "find ups:[%ld] failed ",paxos_idx);
      }
      int64_t master_lease = ups_node->ups_array_->at(master_ups_idx).lease_;
      ret = send_revoking_msg(ups_node->ups_array_->at(master_ups_idx).addr_,
                                   master_lease, ups_node->ups_array_->at(master_ups_idx).addr_);
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "send lease revoking message to ups master error, err=%d ups=%s",
                  ret, ups_node->ups_array_->at(master_ups_idx).addr_.to_cstring());
        // we should wait for the lease timeout
        // int64_t now2 = yysys::CTimeUtil::getTime();
        // if (master_lease > now2)
        // {
        //   waiting_lease_us = master_lease - now2;
        //   waiting_ups_finish_time_ = 0; // tell the check thread don't select new master right now
        // }
      }
      else
      {
        YYSYS_LOG(INFO, "revoked lease, ups=%s", ups_node->ups_array_->at(master_ups_idx).addr_.to_cstring());
      }
    }
    else
    {
      YYSYS_LOG(WARN, "has master but lease is invalid");
    }
    if (OB_SUCCESS == ret)
    {
      YYSYS_LOG(INFO, "revoke lease of old master, old_master=%s",
                ups_node->ups_array_->at(master_ups_idx).addr_.to_cstring());
      change_ups_stat(ups_node->cluster_idx_,ups_node->paxos_idx_,master_ups_idx, UPS_STAT_SYNC);
      ObUps ups(ups_node->ups_array_->at(master_ups_idx));
      refresh_inner_table(ROLE_CHANGE, ups, "null");
    }
    // ups_master_idx_ = -1;//del lqc [merge multiups with paxos] 20170724
    //mod e
  }
  return ret;
}

int64_t ObUpsManager::find_master_ups_index(const int64_t &paxos_idx, ObUpsNode *&ups_node) const
{
  int64_t ret = INVALID_INDEX;
  int64_t ups_idx = INVALID_INDEX;
  if (NULL != (ups_node = find_master_node(paxos_idx, ups_idx)) && INVALID_INDEX != ups_idx)
  {
    ret = ups_idx;
  }
  else
  {
    YYSYS_LOG(INFO, "master is not existent:");
  }
  return ret;
}
ObUpsNode* ObUpsManager::find_master_node(const int64_t &paxos_idx, int64_t &array_idx) const
{
  ObUpsNode *ret = NULL,*ups_node =NULL;
  if(INVALID_INDEX != paxos_idx)
  {
    ObUpsNode *paxos_head = find_paxos_head_node (paxos_idx);
    if(NULL == paxos_head)
    {
      YYSYS_LOG(WARN,"get paxos head failed: paxos[%ld]",paxos_idx);
    }
    else
    {
      ups_node = paxos_head;
      do
      {
        for (int64_t ups_idx = 0; ups_idx < ups_node->ups_array_->count ();ups_idx++)
        {
          if (UPS_STAT_MASTER == ups_node->ups_array_->at(ups_idx).stat_)
          {
            array_idx = ups_idx;
            ret =ups_node;
            break;
          }
        }
        ups_node = ups_node->next_cluster_node();
      }while (ups_node != paxos_head);
    }
  }
  return ret;
}
// mod e
//add lqc [multiups][check master] 20170712 b
bool ObUpsManager::is_rs_master ()
{
  //mod hongchen [RS_SWITCH_MASTER_BUGFIX] 20170726:b
  //return worker_->get_role_manager ()->is_master ();
  return ((ObRoleMgr::MASTER == worker_->get_role_manager()->get_role())
          && ((ObRoleMgr::SWITCHING == worker_->get_role_manager()->get_state())
              || (ObRoleMgr::ACTIVE == worker_->get_role_manager()->get_state())));
  //mod hongchen [RS_SWITCH_MASTER_BUGFIX] 20170726:e
}
//add e
