#include "ob_ms_rpc_proxy.h"
#include "common/ob_general_rpc_stub.h"

#include "common/utility.h"
#include "common/ob_crc64.h"
#include "common/ob_schema.h"
#include "common/ob_scanner.h"
#include "common/ob_mutator.h"
#include "common/ob_obi_role.h"
#include "common/ob_read_common_data.h"
#include "common/ob_trace_log.h"

#include "common/ob_schema_manager.h"
#include "common/location/ob_tablet_location_cache_proxy.h"
#include "ob_ms_service_monitor.h"
#include "common/location/ob_tablet_location_cache.h"
#include "common/location/ob_tablet_location_list.h"
#include "common/ob_rpc_macros.h"
using namespace oceanbase::common;
using namespace oceanbase::mergeserver;

//mod shili [MUTIUPS] [PHYSICAL_PLAN_TRANSFORM] 20150609:b
//ObMergerRpcProxy::ObMergerRpcProxy(const ObServerType type):ups_list_lock_(yysys::WRITE_PRIORITY),
//    mm_ups_list_lock_(yysys::WRITE_PRIORITY)
ObMergerRpcProxy::ObMergerRpcProxy(const ObServerType type):ups_list_lock_(yysys::WRITE_PRIORITY),
    mm_ups_list_lock_(yysys::WRITE_PRIORITY),master_ups_list_lock_(yysys::WRITE_PRIORITY)
//mod 20150609:e
{
  init_ = false;
  rpc_stub_ = NULL;
  rpc_retry_times_ = 0;
  rpc_timeout_ = 0;
  min_fetch_interval_ = 10 * 1000 * 1000L;
  max_fetch_mm_ups_interval_ = 10 * 1000 * 1000L;
  min_fetch_mm_ups_interval_ = 500 * 1000L;
  call_get_master_ups_times_ = 0;
  fetch_ups_timestamp_ = 0;
  fetch_mm_ups_timestamp_ = 0;
  cur_finger_print_ = 0;
  server_type_ = type;
}


//mod shili [MUTIUPS] [PHYSICAL_PLAN_TRANSFORM] 20150609:b
//ObMergerRpcProxy::ObMergerRpcProxy(const int64_t retry_times, const int64_t timeout,
//    const ObServer & root_server, const ObServer & merge_server,
//    const ObServerType type): ups_list_lock_(yysys::WRITE_PRIORITY),
//    mm_ups_list_lock_(yysys::WRITE_PRIORITY)
ObMergerRpcProxy::ObMergerRpcProxy(const int64_t retry_times, const int64_t timeout,
    const ObServer & root_server, const ObServer & merge_server,
    const ObServerType type): ups_list_lock_(yysys::WRITE_PRIORITY),
    mm_ups_list_lock_(yysys::WRITE_PRIORITY),master_ups_list_lock_(yysys::WRITE_PRIORITY)
//mod 20150609:e

{
  init_ = false;
  rpc_stub_ = NULL;
  rpc_retry_times_ = retry_times;
  rpc_timeout_ = timeout;
  root_server_ = root_server;
  merge_server_ = merge_server;
  server_type_ = type;
  min_fetch_interval_ = 10 * 1000 * 1000L;
  max_fetch_mm_ups_interval_ = 10 * 1000 * 1000L;
  min_fetch_mm_ups_interval_ = 500 * 1000L;
  call_get_master_ups_times_ = 0;
  fetch_ups_timestamp_ = 0;
  fetch_mm_ups_timestamp_ = 0;
  cur_finger_print_ = 0;
}

ObMergerRpcProxy::~ObMergerRpcProxy()
{
}

bool ObMergerRpcProxy::check_inner_stat(void) const
{
  return (init_ && (NULL != rpc_stub_));
}

void ObMergerRpcProxy::set_min_interval(const int64_t interval)
{
  min_fetch_interval_ = interval;
  max_fetch_mm_ups_interval_ = interval;
}

int ObMergerRpcProxy::init(common::ObGeneralRpcStub *rpc_stub)
{
  int ret = OB_SUCCESS;
  if (NULL == rpc_stub)
  {
    YYSYS_LOG(ERROR, "check rpc failed: rpc[%p]", rpc_stub);
    ret = OB_INPUT_PARAM_ERROR;
  }
  else if (true == init_)
  {
    YYSYS_LOG(ERROR, "%s", "check already inited");
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    rpc_stub_ = rpc_stub;
    init_ = true;
  }
  return ret;
}

int ObMergerRpcProxy::fetch_update_server_list(int32_t & count)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    YYSYS_LOG(ERROR, "%s", "check inner stat failed");
    ret = OB_ERROR;
  }
  else
  {
    ObUpsList list;
    ret = rpc_stub_->fetch_server_list(rpc_timeout_, root_server_, list);
    if (ret != OB_SUCCESS)
    {
      YYSYS_LOG(WARN, "fetch server list from root server failed:ret[%d]", ret);
    }
    else
    {
      count = list.ups_count_;
      // if has error modify the list
      modify_ups_list(list);
      // check finger print changed
      uint64_t finger_print = ob_crc64(&list, sizeof(list));
      if (finger_print != cur_finger_print_)
      {
        YYSYS_LOG(INFO, "ups list changed succ:cur[%lu], new[%lu]", cur_finger_print_, finger_print);
        list.print();
        yysys::CWLockGuard lock(ups_list_lock_);
        find_master_ups(list, master_update_server_);
        cur_finger_print_ = finger_print;
        memcpy(&update_server_list_, &list, sizeof(update_server_list_));
      }
      else
      {
        YYSYS_LOG(TRACE, "ups list not changed:crc[%lu], count[%d]", finger_print, count);
      }
    }
  }
  return ret;
}

void ObMergerRpcProxy::find_master_ups(const ObUpsList & list, ObServer & master)
{
  for (int64_t i = 0; i < list.ups_count_; ++i)
  {
    if (UPS_MASTER == list.ups_array_[i].stat_)
    {
      master = list.ups_array_[i].addr_;
      break;
    }
  }
}

int ObMergerRpcProxy::get_last_frozen_memtable_version(int64_t & frozen_version)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    YYSYS_LOG(ERROR, "%s", "check inner stat failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    ObServer update_server;
    ret = get_master_ups(false, update_server);
    if (ret != OB_SUCCESS)
    {
      YYSYS_LOG(WARN, "get master update server failed:ret[%d]", ret);
    }
    else
    {
      ret = rpc_stub_->get_last_frozen_memtable_version(rpc_timeout_, update_server, frozen_version);
      if (ret != OB_SUCCESS)
      {
        YYSYS_LOG(WARN, "fetch frozen version failed:ret[%d]", ret);
      }
      else
      {
        YYSYS_LOG(DEBUG, "fetch frozen version succ:version[%ld]", frozen_version);
      }
    }
  }
  return ret;
}


bool ObMergerRpcProxy::check_range_param(const ObNewRange & range_param)
{
  bool bret = true;
  if (((!range_param.start_key_.is_min_row()) && (0 == range_param.start_key_.length()))
      || (!range_param.end_key_.is_max_row() && (0 == range_param.end_key_.length())))
  {
    YYSYS_LOG(ERROR, "%s", "check range param failed");
    bret = false;
  }
  return bret;
}


bool ObMergerRpcProxy::check_scan_param(const ObScanParam & scan_param)
{
  bool bret = true;
  const ObNewRange * range = scan_param.get_range();
  // the min/max value length is zero
  if (NULL == range)// || (0 == range->start_key_.length()))
  {
    YYSYS_LOG(ERROR, "%s", "check scan range failed");
    bret = false;
  }
  else
  {
    bret = check_range_param(*range);
  }
  return bret;
}

int ObMergerRpcProxy::ups_mutate(const ObMutator & mutate_param, const bool has_data, ObScanner & scanner)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    YYSYS_LOG(ERROR, "%s", "check inner stat failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    ret = OB_PROCESS_TIMEOUT;
    ObServer update_server;
    int64_t end_time = rpc_timeout_ + yysys::CTimeUtil::getTime();
    for (int64_t i = 0; yysys::CTimeUtil::getTime() < end_time; ++i)
    {
      // may be need update server list
      ret = get_master_ups((OB_NOT_MASTER == ret || OB_RESPONSE_TIME_OUT == ret), update_server);
      if (ret != OB_SUCCESS)
      {
        YYSYS_LOG(WARN, "get master update server failed:ret[%d]", ret);
        break;
      }
      ret = rpc_stub_->mutate(rpc_timeout_, update_server, mutate_param, has_data, scanner);
      if (false == check_need_retry_ups(ret))
      {
        break;
      }
      else
      {
        YYSYS_LOG(WARN, "mutate fail. retry. ret=%d, i=%ld, rpc_timeout_=%ld.", ret, i, rpc_timeout_);
        usleep(static_cast<useconds_t>(RETRY_INTERVAL_TIME + (i + 1)));
      }
    }
  }

  if (OB_SUCCESS == ret && YYSYS_LOGGER._level >= YYSYS_LOG_LEVEL_DEBUG)
  {
    YYSYS_LOG(DEBUG, "%s", "ups_mutate");
    output(scanner);
  }
  return ret;
}

void ObMergerRpcProxy::set_master_ups(const ObServer & server)
{
  master_update_server_ = server;
}

int ObMergerRpcProxy::get_master_ups_old(const bool renew, ObServer & server)
{
  int ret = OB_SUCCESS;
  bool is_master_addr_invalid = false;
  {
    yysys::CRLockGuard lock(ups_list_lock_);
    is_master_addr_invalid = (0 == master_update_server_.get_ipv4());
  }
  if (true == renew || is_master_addr_invalid)
  {
    int64_t timestamp = yysys::CTimeUtil::getTime();
    if (timestamp - fetch_ups_timestamp_ > min_fetch_interval_)
    {
      int32_t server_count = 0;
      yysys::CThreadGuard lock(&update_lock_);
      if (timestamp - fetch_ups_timestamp_ > min_fetch_interval_)
      {
        YYSYS_LOG(DEBUG, "need renew the update server list");
        fetch_ups_timestamp_ = yysys::CTimeUtil::getTime();
        // renew the udpate server list
        ret = fetch_update_server_list(server_count);
        if (ret != OB_SUCCESS)
        {
          YYSYS_LOG(WARN, "fetch update server list failed:ret[%d]", ret);
        }
        else if (server_count == 0)
        {
          YYSYS_LOG(DEBUG, "new server list empty retry fetch vip server");
          // using old protocol get update server vip
          ret = rpc_stub_->find_server(rpc_timeout_, root_server_, server);
          if (ret != OB_SUCCESS)
          {
            YYSYS_LOG(WARN, "find update server vip failed:ret[%d]", ret);
          }
          else
          {
            yysys::CWLockGuard lock(ups_list_lock_);
            master_update_server_ = server;
          }
        }
      }
      else
      {
        YYSYS_LOG(DEBUG, "fetch update server list by other thread");
      }
    }
  }
  // renew master update server addr
  yysys::CRLockGuard lock(ups_list_lock_);
  server = master_update_server_;
  return ret;
}

void ObMergerRpcProxy::modify_ups_list(ObUpsList & list)
{
  if (0 == list.ups_count_)
  {
    // add vip update server to list
    YYSYS_LOG(DEBUG, "check ups count is zero:count[%d]", list.ups_count_);
    ObUpsInfo info;
    info.addr_ = master_update_server_;
    // set inner port to update server port
    info.inner_port_ = master_update_server_.get_port();
    info.ms_read_percentage_ = 100;
    info.cs_read_percentage_ = 100;
    list.ups_count_ = 1;
    list.ups_array_[0] = info;
    list.sum_ms_percentage_ = 100;
    list.sum_cs_percentage_ = 100;
  }
  else if (list.get_sum_percentage(server_type_) <= 0)
  {
    for (int32_t i = 0; i < list.ups_count_; ++i)
    {
      // reset all ms and cs to equal
      list.ups_array_[i].ms_read_percentage_ = 1;
      list.ups_array_[i].cs_read_percentage_ = 1;
    }
    // reset all ms and cs sum percentage to count
    list.sum_ms_percentage_ = list.ups_count_;
    list.sum_cs_percentage_ = list.ups_count_;
  }
}

int ObMergerRpcProxy::set_rpc_param(const int64_t retry_times, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  if ((retry_times < 0) || (timeout <= 0))
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(WARN, "check rpc timeout param failed:retry_times[%ld], timeout[%ld]",
      retry_times, timeout);
  }
  else
  {
    rpc_retry_times_ = retry_times;
    rpc_timeout_ = timeout;
  }
  return ret;
}

void ObMergerRpcProxy::output(common::ObScanner & result)
{
  int ret = OB_SUCCESS;
  ObCellInfo *cur_cell = NULL;
  while (result.next_cell() == OB_SUCCESS)
  {
    ret = result.get_cell(&cur_cell);
    if (OB_SUCCESS == ret)
    {
      YYSYS_LOG(DEBUG, "tableid:%lu,rowkey:%s,column_id:%lu,ext:%ld,type:%d",
          cur_cell->table_id_,
          to_cstring(cur_cell->row_key_),  cur_cell->column_id_,
          cur_cell->value_.get_ext(),cur_cell->value_.get_type());
      cur_cell->value_.dump();
    }
    else
    {
      YYSYS_LOG(WARN, "get cell failed:ret[%d]", ret);
      break;
    }
  }
  result.reset_iter();
}

int ObMergerRpcProxy::get_master_obi_rs(const common::ObServer &rootserver,
    common::ObServer &master_obi_rs)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS !=
      (ret = rpc_stub_->get_master_obi_rs(rpc_timeout_,
                                          rootserver, master_obi_rs)))
  {
    YYSYS_LOG(WARN, "get master ob rootservre fail, ret: [%d]", ret);
  }
  return ret;
}

int ObMergerRpcProxy::get_inst_master_ups(const common::ObServer &root_server, common::ObServer &ups_master)
{
  int err = OB_SUCCESS;
  common::ObServer master_inst_rs;

  // query who is master instance rootserver according to OBI_ROLE
  if (OB_SUCCESS == err)
  {
    if (OB_SUCCESS != (err = get_master_obi_rs(root_server, master_inst_rs)))
    {
      YYSYS_LOG(WARN, "fail to get master obi rootserver addr. err=%d", err);
    }
  }
  // ask the master instance rs for master master ups addr
  if (OB_SUCCESS == err)
  {
    if (OB_SUCCESS != (err = rpc_stub_->get_master_ups_info(rpc_timeout_, master_inst_rs, ups_master)))
    {
      YYSYS_LOG(WARN, "fail to get master obi ups addr. master_inst_rs=%s, err=%d", master_inst_rs.to_cstring(), err);
    }
  }
  return err;
}

int ObMergerRpcProxy::get_master_ups(const bool force_renew, common::ObServer &master_master_ups)
{
  int ret = OB_SUCCESS;
  int64_t last_access_count = 0;
  int64_t timestamp = 0;
  bool is_master_addr_invalid = false;
  {
    yysys::CRLockGuard lock(mm_ups_list_lock_);
    is_master_addr_invalid = (0 == master_master_ups_.get_ipv4());
  }
  timestamp = yysys::CTimeUtil::getTime();
  if ((timestamp - fetch_mm_ups_timestamp_ < min_fetch_mm_ups_interval_)
      && !is_master_addr_invalid)
  {
    // bypass to prevent retry storm when switching OBI role
    // in consequence of this, some call will fail for a short period
  }
  else  if (true == force_renew || is_master_addr_invalid ||
      timestamp - fetch_mm_ups_timestamp_ > max_fetch_mm_ups_interval_)
  {
  /* always update master ups when:
   *  - force renew
   *  - add invalid
   *  - exceed the fetch ups interval
   */

    YYSYS_LOG(DEBUG, "need renew the update server list");
    last_access_count = call_get_master_ups_times_; // need not to be guarded.

    yysys::CThreadGuard lock(&mm_update_lock_);

    /* multi-thread optimization
     *  - in case some other thread has updated the ups addr
     */
    timestamp = yysys::CTimeUtil::getTime();
    if ((last_access_count < call_get_master_ups_times_) ||
       (timestamp - fetch_mm_ups_timestamp_ < min_fetch_mm_ups_interval_))
    {
      YYSYS_LOG(DEBUG, "some other thread has update master ups prio to this thread "
          "last_access_count=%ld, call_get_master_ups_times_=%ld", last_access_count, call_get_master_ups_times_);
    }
    else
    {
      // renew the udpate server list
      fetch_mm_ups_timestamp_ = yysys::CTimeUtil::getTime();
      if (OB_SUCCESS != (ret = get_inst_master_ups(root_server_, master_master_ups)))
      {
        YYSYS_LOG(WARN, "fail to get master master ups. ret=%d", ret);
      }
      else
      {
        yysys::CWLockGuard lock(mm_ups_list_lock_);
        master_master_ups_ = master_master_ups;
        YYSYS_LOG(DEBUG, "master_master_ups_=%s", to_cstring(master_master_ups_));
      }
      YYSYS_LOG(TRACE, "fetch master ups:addr[%s], err[%d]", to_cstring(master_master_ups_), ret);
    }
    call_get_master_ups_times_++;
  }
  // renew master update server addr
  yysys::CRLockGuard lock(mm_ups_list_lock_);
  master_master_ups = master_master_ups_;
  YYSYS_LOG(DEBUG, "(out) master_master_ups=%s", to_cstring(master_master_ups_));
  return ret;
}

//add shili [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20150707:b
//@berif 根据paxos_id 获取 master_ups 的地址
int ObMergerRpcProxy::get_master_update_server(const int64_t paxos_id, common::ObServer &update_server)
{
  int ret = OB_SUCCESS;
  bool had_found = false;
  int32_t server_count = master_update_server_list_.ups_count_;
  //mod liuzy [MultiUPS] [add_paxos_interface] 20160323:b
  /*Exp: fetch master ups, when server_count less than paxos_id + 1*/
//  if(0 == server_count)
  if ((paxos_id + 1) > server_count )
  //mod 20160323:e
  {
    YYSYS_LOG(DEBUG, "no master_ups right now local, updating...");
    if(OB_SUCCESS != (ret = fetch_master_update_server_list(server_count)))
    {
      YYSYS_LOG(WARN, "get master update server list failed:ret[%d]", ret);
    }
    else if(server_count == 0)
    {
      ret = OB_MASTER_UPS_NOT_EXIST;
      YYSYS_LOG(WARN, "no mast update server available right now, ret: [%d]", ret);
    }
    else
    {
      YYSYS_LOG(DEBUG, "master update local ups list"
                " info successfully! Got [%d] ups.", server_count);
    }
  }
  if(OB_SUCCESS == ret)
  {
    yysys::CRLockGuard lock(master_ups_list_lock_);
    for(int32_t k = 0; k < master_update_server_list_.ups_count_; k++) //get ups
    {
      if(master_update_server_list_.ups_array_[k].paxos_id_ == paxos_id)
      {
        update_server = master_update_server_list_.ups_array_[k].addr_;
        had_found =true;
        break;
      }
    }
    if(!had_found)
    {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}
//add 20150707:e

//add shili [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20150630:b
//@berif 向rs请求 master_list
//count [out] master_list的数量
int ObMergerRpcProxy::fetch_master_update_server_list(int32_t & count)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    YYSYS_LOG(ERROR, "%s", "check inner stat failed");
    ret = OB_ERROR;
  }
  else
  {
    ObUpsList list;
    if (OB_SUCCESS != (ret = rpc_stub_->fetch_master_server_list(rpc_timeout_, root_server_, list)))
    {
      YYSYS_LOG(WARN, "fetch master server list from root server %s failed:ret[%d]",
          to_cstring(root_server_), ret);
    }
    else
    {
      count = list.ups_count_;
      if (0 != list.ups_count_)
      {
        yysys::CWLockGuard lock(master_ups_list_lock_);
        memcpy(&master_update_server_list_, &list, sizeof(master_update_server_list_));
      }
      else
      {
        master_update_server_list_.ups_count_ =0;
        YYSYS_LOG(DEBUG, "master ups list not changed count[%d]",list.ups_count_);
      }
    }
  }
  return ret;
}
//add 20150630:e

int ObMergerRpcProxy::kill_session(const uint32_t ip, const int32_t session_id, const bool is_query)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    YYSYS_LOG(ERROR, "%s", "check inner stat failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    ObServer server;
    server.set_ipv4_addr(ip, merge_server_.get_port());
    ret = rpc_stub_->kill_session(rpc_timeout_, server, session_id, is_query);
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "kill session failed: ret[%d] server is %s, session id=%d", ret,
                to_cstring(server), session_id);
    }
    else
    {
      YYSYS_LOG(DEBUG, "kill session succ: server is %s, session id=%d",
                to_cstring(server), session_id);
    }
  }
  return ret;
}

int ObMergerRpcProxy::ups_plan_execute(int64_t timeout, const sql::ObPhysicalPlan &plan, sql::ObUpsResult &result)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    YYSYS_LOG(ERROR, "%s", "check inner stat failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    ret = OB_PROCESS_TIMEOUT;
    ObServer update_server;
    int64_t end_time = timeout + yysys::CTimeUtil::getTime();
    for (int64_t i = 0; yysys::CTimeUtil::getTime() < end_time; ++i)
    {
      // may be need update server list
      ret = get_master_ups((OB_NOT_MASTER == ret || OB_RESPONSE_TIME_OUT == ret), update_server);
      if (ret != OB_SUCCESS)
      {
        YYSYS_LOG(WARN, "get master update server failed:ret[%d]", ret);
        break;
      }
        YYSYS_LOG(WARN, "get table level update server %s ", to_cstring(update_server));
      ret = rpc_stub_->ups_plan_execute(timeout, update_server, plan, result);
      if (false == check_need_retry_ups(ret))
      {
        break;
      }
      else
      {
        YYSYS_LOG(WARN, "ups plan execute fail. retry. ret=%d, i=%ld, timeout=%ld.", ret, i, timeout);
        usleep(static_cast<useconds_t>(RETRY_INTERVAL_TIME + (i + 1)));
      }
    } // end for
  }
  return ret;
}

//add shili [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20150717:b
//@berif 向update_server 发送inner_plan
//plan [in] 即将发送的物理计划
//update_server [in] 目标ups的地址
int ObMergerRpcProxy::ups_plan_execute(const int64_t timeout,const sql::ObPhysicalPlan &plan,const ObServer &update_server, sql::ObUpsResult &result)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    YYSYS_LOG(ERROR, "%s", "check inner stat failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else if(OB_SUCCESS!=(ret = rpc_stub_->ups_plan_execute(timeout, update_server, plan, result)))
  {
    //mod lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160422:b
    //YYSYS_LOG(WARN, "ups plan execute failed,ret=%d",ret);
    if (!IS_SQL_ERR(ret))
    {
      YYSYS_LOG(WARN, "ups plan execute failed,ret=%d",ret);
    }
    //mod 20160422:e
  }
  return ret;
}
//add 20150717:e

int ObMergerRpcProxy::ups_start_trans(const common::ObTransReq &req, common::ObTransID &trans_id)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    YYSYS_LOG(ERROR, "%s", "check inner stat failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    ret = OB_PROCESS_TIMEOUT;
    ObServer update_server;
    int64_t end_time = rpc_timeout_ + yysys::CTimeUtil::getTime();
    for (int64_t i = 0; yysys::CTimeUtil::getTime() < end_time; ++i)
    {
      // may be need update server list
      ret = get_master_ups((OB_NOT_MASTER == ret || OB_RESPONSE_TIME_OUT == ret), update_server);
      if (ret != OB_SUCCESS)
      {
        YYSYS_LOG(WARN, "get master update server failed:ret[%d]", ret);
        break;
      }
      ret = rpc_stub_->ups_start_trans(rpc_timeout_, update_server, req, trans_id);
      if (false == check_need_retry_ups(ret))
      {
        break;
      }
      else
      {
        YYSYS_LOG(WARN, "ups start trans fail. retry. ret=%d, i=%ld, rpc_timeout_=%ld.", ret, i, rpc_timeout_);
        usleep(static_cast<useconds_t>(RETRY_INTERVAL_TIME + (i + 1)));
      }
    } // end for
  }
  return ret;
}
int ObMergerRpcProxy::get_bloom_filter(const common::ObServer &server, const common::ObNewRange &range, const int64_t tablet_version, const int64_t bf_version, ObString &bf_buffer) const
{
  return rpc_stub_->get_bloom_filter(rpc_timeout_, server, range, tablet_version, bf_version, bf_buffer);
}
int ObMergerRpcProxy::get_ups_log_seq(const common::ObServer &ups, const int64_t timeout, int64_t & log_seq)
{
  return rpc_stub_->get_ups_log_seq(ups, timeout, log_seq);
}

int ObMergerRpcProxy::ups_end_trans(const common::ObEndTransReq &req)
{
  int ret = OB_SUCCESS;
  if (!check_inner_stat())
  {
    YYSYS_LOG(ERROR, "%s", "check inner stat failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    ret = OB_PROCESS_TIMEOUT;
    ObServer update_server;
    //mod peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150721:b
    /*
    int64_t end_time = rpc_timeout_ + yysys::CTimeUtil::getTime();
    for (int64_t i = 0; yysys::CTimeUtil::getTime() < end_time; ++i)
    {
      // may be need update server list
      ret = get_master_ups((OB_NOT_MASTER == ret || OB_RESPONSE_TIME_OUT == ret), update_server);
      if (ret != OB_SUCCESS)
      {
        YYSYS_LOG(WARN, "get master update server failed:ret[%d]", ret);
        continue;
      }
      ret = rpc_stub_->ups_end_trans(rpc_timeout_, update_server, req);
      if (false == check_need_retry_ups(ret))
      {
        break;
      }
      else
      {
        YYSYS_LOG(WARN, "ups end trans fail. retry. ret=%d, i=%ld, rpc_timeout_=%ld.", ret, i, rpc_timeout_);
        usleep(static_cast<useconds_t>(RETRY_INTERVAL_TIME + (i + 1)));
      }
    } // end for
    */

    if (req.participant_num_ == 1)
    {
      //single point transaction
      update_server = req.trans_id_.ups_;
      YYSYS_LOG(DEBUG, "enter single ups transaction end");
      ret = rpc_stub_->ups_end_trans(rpc_timeout_, update_server, req);
    }
    else if (req.participant_num_ > 1 && req.rollback_)
    {
      //rollback distributed trans
      YYSYS_LOG(INFO, " rollback distributed transaction");
      ret = ups_rollback_distributed_trans(req);
    }
    else if (req.participant_num_ > 1)
    {
      //end distributed trans to coordinator ups
      update_server = req.participant_trans_id_[0].ups_;
      YYSYS_LOG(DEBUG, "enter multi ups transaction end");
      ret = rpc_stub_->ups_end_dis_trans(rpc_timeout_*5, update_server, req);
    }
    else
    {
      //ERROR should not be here
      YYSYS_LOG(ERROR, "error branch, req.participant_num_ :%d",req.participant_num_);
    }
    //mod 20150721:e
  }
  return ret;
}

//add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
int ObMergerRpcProxy::ups_rollback_distributed_trans(const common::ObEndTransReq &req)
{
  int ret = OB_SUCCESS;
  if(req.participant_num_ <= 1 || !req.rollback_)
  {
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    ObEndTransReq tmp_req;
    tmp_req.rollback_ = true;
    tmp_req.participant_num_ = 1;
    for (int i = 0; i < req.participant_num_; i++)
    {
      tmp_req.trans_id_ = req.participant_trans_id_[i];
      if (OB_SUCCESS != (ret = rpc_stub_->ups_end_trans(rpc_timeout_,tmp_req.trans_id_.ups_,tmp_req)))
      {
        if (OB_TRANS_ROLLBACKED != ret)
        {
          YYSYS_LOG(WARN, "failed to rollback ups transaction, err=%d trans=%s",
                        ret, to_cstring(req));
        }
      }
    }
  }
  return ret;
}
 //add 20150701:e
//add by maosy [MultiUps 1.0] [batch_iud_snapshot] 20170622 b:
int ObMergerRpcProxy::get_stmt_start_time(ObServer ups_server, int64_t &public_transid)
{
    int ret = OB_SUCCESS;
    if (!check_inner_stat())
    {
        YYSYS_LOG(ERROR, "%s", "check inner stat failed");
        ret = OB_INNER_STAT_ERROR;
    }
    else if(OB_SUCCESS != (ret = rpc_stub_->ups_get_stmt_start_time(rpc_timeout_,ups_server,public_transid)))
    {
        YYSYS_LOG(WARN,"failed to get publised transid,ret = %d,ups=%s",ret,to_cstring(ups_server));
    }
    return ret ;
}
// add e

