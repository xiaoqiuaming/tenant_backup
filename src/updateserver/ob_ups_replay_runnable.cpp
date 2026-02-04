/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *     - some work details if you want
 */

#include "ob_ups_replay_runnable.h"

#include "ob_ups_log_mgr.h"
#include "common/utility.h"
#include "ob_update_server.h"
#include "ob_update_server_main.h"
using namespace oceanbase::common;
using namespace oceanbase::updateserver;

//add wangdonghui [ups_replication] 20170527:b
#define UPS ObUpdateServerMain::get_instance()->get_update_server()
//add :e

ObUpsReplayRunnable::ObUpsReplayRunnable()
{
  is_initialized_ = false;
  replay_wait_time_us_ = DEFAULT_REPLAY_WAIT_TIME_US;
  fetch_log_wait_time_us_ = DEFAULT_FETCH_LOG_WAIT_TIME_US;
}

ObUpsReplayRunnable::~ObUpsReplayRunnable()
{
}
//mod peiouya [MultiUPS] [DELETE_OBI] 20150701:b
//int ObUpsReplayRunnable::init(ObUpsLogMgr* log_mgr, ObiRole *obi_role, ObUpsRoleMgr *role_mgr)
int ObUpsReplayRunnable::init(ObUpsLogMgr* log_mgr, ObUpsRoleMgr *role_mgr)
{
  int ret = OB_SUCCESS;
  if (is_initialized_)
  {
    YYSYS_LOG(ERROR, "ObLogReplayRunnable has been initialized"); 
    ret = OB_INIT_TWICE;
  }

  if (OB_SUCCESS == ret)
  {
//    if (NULL == log_mgr || NULL == role_mgr || NULL == obi_role)
//    {
//      YYSYS_LOG(ERROR, "Parameter is invalid[obi_role=%p][role_mgr=%p]", obi_role, role_mgr);
//    }
    if (NULL == log_mgr || NULL == role_mgr)
    {
      YYSYS_LOG(ERROR, "Parameter is invalid[role_mgr=%p]", role_mgr);
    }
  }
  if (OB_SUCCESS == ret)
  {
    log_mgr_ = log_mgr;
    role_mgr_ = role_mgr;
    //conflict_level A 
    //obi_role_ = obi_role; 
    is_initialized_ = true;
  }
  return ret;
}

void ObUpsReplayRunnable::clear()
{
  if (NULL != _thread)
  {
    delete[] _thread;
    _thread = NULL;
  }
}

void ObUpsReplayRunnable::stop()
{
  _stop = true;
}

bool ObUpsReplayRunnable::wait_stop()
{
  return switch_.wait_off();
}

bool ObUpsReplayRunnable::wait_start()
{
  return switch_.wait_on();
}

void ObUpsReplayRunnable::run(yysys::CThread* thread, void* arg)
{
  int err = OB_SUCCESS;
  UNUSED(thread);
  UNUSED(arg);

  YYSYS_LOG(INFO, "ObUpsLogReplayRunnable start to run");
  if (!is_initialized_)
  {
    YYSYS_LOG(ERROR, "ObUpsLogReplayRunnable has not been initialized");
    err = OB_NOT_INIT;
  }
  SET_THD_NAME_ONCE("ups-log2disk");
  while (!_stop && (OB_SUCCESS == err || OB_NEED_RETRY == err || OB_NEED_WAIT == err))
  {
    //mod wangjiahao [Paxos ups_replication_tmplog] 20150716 :b
    //needn't to care about log in recent_log_cache
    //if (switch_.check_off(log_mgr_->has_nothing_in_buf_to_replay()))
    if (switch_.check_off(log_mgr_->has_nothing_to_replay()))
    //mod :e
    {
      usleep(static_cast<useconds_t>(replay_wait_time_us_)); //default 50ms
    }
    else if (OB_SUCCESS != (err = log_mgr_->replay_log())
        && OB_NEED_RETRY != err && OB_NEED_WAIT != err)
    {
      if (OB_CANCELED != err)
      {
        YYSYS_LOG(ERROR, "log_mgr.replay()=>%d", err);
      }
    }
    else if (OB_NEED_RETRY == err)
    {
      log_mgr_->wait_new_log_to_replay(replay_wait_time_us_);
    }
    else if (OB_NEED_WAIT == err)
    {
      YYSYS_LOG(WARN, "slave need wait for master ready %ld, %s", fetch_log_wait_time_us_, to_cstring(*log_mgr_));
      usleep(static_cast<useconds_t>(fetch_log_wait_time_us_));
    }
  }
  YYSYS_LOG(INFO, "ObLogReplayRunnable finished[stop=%d ret=%d]", _stop, err); 
}


//add wangdonghui [paxos ups_replication_optimize] 20161009:b
ObUpsWaitFlushRunnable::ObUpsWaitFlushRunnable():task_queue_(),allocator_()
{
  is_initialized_ = false;
}

ObUpsWaitFlushRunnable::~ObUpsWaitFlushRunnable()
{
    allocator_.destroy();
}

int ObUpsWaitFlushRunnable::init(ObUpsLogMgr* log_mgr, ObUpsRoleMgr *role_mgr,
                                 ObUpdateServer* updateserver, ObLogReplayWorker* replay_worker)
{
  int ret = OB_SUCCESS;
  if (is_initialized_)
  {
    YYSYS_LOG(ERROR, "ObUpsWaitFlushRunnable has been initialized");
    ret = OB_INIT_TWICE;
  }

  if (OB_SUCCESS == ret)
  {
    if (NULL == log_mgr || NULL == role_mgr || NULL == replay_worker)
    {
      YYSYS_LOG(ERROR, "Parameter is invalid[role_mgr=%p] [replay_worker=%p]", role_mgr, replay_worker);
    }
  }
  if (OB_SUCCESS != (ret = task_queue_.init(10240)))
  {
    YYSYS_LOG(WARN, "task_queue_ init fail=>%d", ret);
  }
  else if (OB_SUCCESS != (ret = allocator_.init(ALLOCATOR_TOTAL_LIMIT, ALLOCATOR_HOLD_LIMIT, ALLOCATOR_PAGE_SIZE)))
  {
    YYSYS_LOG(WARN, "init allocator fail ret=%d", ret);
  }
  else
  {
    log_mgr_ = log_mgr;
    role_mgr_ = role_mgr;
    updateserver_ = updateserver;
    replay_worker_ = replay_worker;
    is_initialized_ = true;
  }
  return ret;
}

void ObUpsWaitFlushRunnable::clear()
{
  if (NULL != _thread)
  {
    delete[] _thread;
    _thread = NULL;
  }
}

void ObUpsWaitFlushRunnable::stop()
{
  _stop = true;
}

bool ObUpsWaitFlushRunnable::wait_stop()
{
  return switch_.wait_off();
}

bool ObUpsWaitFlushRunnable::wait_start()
{
  return switch_.wait_on();
}

void ObUpsWaitFlushRunnable::run(yysys::CThread* thread, void* arg)
{
  int err = OB_SUCCESS;
  UNUSED(thread);
  UNUSED(arg);

  YYSYS_LOG(INFO, "ObUpsWaitFlushRunnable start to run");
  if (!is_initialized_)
  {
    YYSYS_LOG(ERROR, "ObUpsWaitFlushRunnable has not been initialized");
    err = OB_NOT_INIT;
  }
  SET_THD_NAME_ONCE("ups-waitflush");
  while (!_stop)
  {
    if (0 == task_queue_.size())
    {
      usleep(static_cast<useconds_t>(100)); //default 0.1ms
    }
    else if (OB_SUCCESS != (err = process_head_task()))
    {
      YYSYS_LOG(WARN, "process task=>%d", err);
    }
  }
  YYSYS_LOG(INFO, "ObUpsWaitFlushRunnable finished[stop=%d ret=%d]", _stop, err);
}

int ObUpsWaitFlushRunnable::process_head_task()
{
  int ret = OB_SUCCESS;
  TransExecutor::Task *task = NULL;
  _cond.lock();
  task_queue_.pop(task);
  _cond.unlock();
  if(NULL == task)
  {
    YYSYS_LOG(WARN, "task is NULL.");
  }
  else
  {
    int response_err = OB_SUCCESS;
    int64_t wait_sync_time_us =  updateserver_->get_wait_sync_time();
    int64_t wait_event_type = updateserver_->get_wait_sync_type();
    //593
    int64_t max_n_lagged_disk_log_allowed = updateserver_->get_max_n_lagged_disk_log_allowed();

    int64_t start_id = 0;
    int64_t end_id = 0;
    int64_t last_log_term = OB_INVALID_LOG;
    common::ObDataBuffer* in_buff = task->pkt.get_buffer();
    char* buf = in_buff->get_data() + in_buff->get_position();
    int64_t len = in_buff->get_capacity() - in_buff->get_position();
    //int64_t next_commit_log_id;
    int64_t next_flush_log_id = 0;
    easy_request_t* req = task->pkt.get_request();
    uint32_t channel_id = task->pkt.get_channel_id();
    if( task->pkt.get_api_version() != MY_VERSION)
    {
      ret = OB_ERROR_FUNC_VERSION;
    }
    else if (OB_SUCCESS != (ret = parse_log_buffer(buf, len, start_id, end_id, last_log_term))) //mod1 wangjiahao [Paxos ups_replication_tmplog] 20150804 add last_log_term
    {
      YYSYS_LOG(ERROR, "parse_log_buffer(log_data=%p[%ld])=>%d", buf, len, ret);
    }
    else if (wait_sync_time_us <= 0 || ObUpsLogMgr::WAIT_NONE == wait_event_type)
    {}
    else if (ObUpsRoleMgr::STOP == role_mgr_->get_state() || ObUpsRoleMgr::FATAL == role_mgr_->get_state())
    {
        ret = OB_ERR_UNEXPECTED;
    }

    //del pangtianze [Paxos replication] 20161221:b remove waitcommit model
    /*else if (ObUpsLogMgr::WAIT_COMMIT == wait_event_type)
    {
      if (end_id > (next_commit_log_id = replay_worker_->wait_next_commit_log_id(end_id, wait_sync_time_us)))
      {
        YYSYS_LOG(WARN, "wait_flush_log_id(end_id=%ld, next_flush_log_id=%ld, timeout=%ld) Fail.",
                  end_id, next_commit_log_id, wait_sync_time_us);
      }
    }*/
    //del:e
    else if (ObUpsLogMgr::WAIT_FLUSH == wait_event_type)
    {
        //[593]
        if(end_id - replay_worker_->get_next_flush_log_id() > max_n_lagged_disk_log_allowed)
        {
            YYSYS_LOG(WARN, "flushed log on slave lag behind manster(end_id=%ld, slave_flushed_log_id=%ld, max_n_logged_disk_log_allowed=%ld).",
                      end_id, replay_worker_->get_next_flush_log_id(), max_n_lagged_disk_log_allowed);
            ret = OB_LOG_NOT_SYNC;
        }
      else if (end_id > (next_flush_log_id = replay_worker_->wait_next_flush_log_id(end_id, wait_sync_time_us)))
      {
        YYSYS_LOG(WARN, "wait_flush_log_id(end_id=%ld, next_flush_log_id=%ld, timeout=%ld) Fail.",
                  end_id, next_flush_log_id, wait_sync_time_us);
        //add wangjiahao [Paxos ups_replication_tmplog] 20150716 :b
        ret = OB_LOG_NOT_SYNC;
        //add :e
      }
      else
      {
        //YYSYS_LOG(INFO, "WDH_TEST:: slave_process_time(%s): %ld log: %ld %ld", updateserver_->get_self().to_cstring(),
        //          yysys::CTimeUtil::getTime()-packet->get_receive_ts(), start_id, end_id);
      }
    }
    int64_t message_residence_time_us = yysys::CTimeUtil::getTime()-task->pkt.get_receive_ts();
    if (OB_SUCCESS != (response_err = updateserver_->response_result1_(ret, OB_SEND_LOG_RES, MY_VERSION, req, channel_id, message_residence_time_us)))
    {
      ret = response_err;
      YYSYS_LOG(ERROR, "response_result_()=>%d", ret);
    }
    UPS.get_trans_executor().get_allocator()->free(task);
    task = NULL;
  }

  return ret;
}

int ObUpsWaitFlushRunnable::push(TransExecutor::Task* task)
{
  int ret = OB_SUCCESS;
  /*int64_t packet_size = sizeof(ObPacket) + packet->get_buffer()->get_capacity();
  ObPacket *req = (ObPacket *)allocator_.alloc(packet_size);
  if (NULL == req)
  {
    ret = OB_MEM_OVERFLOW;
  }
  else
  {
    req->set_api_version(packet->get_api_version());
    req->set_channel_id(packet->get_channel_id());
    req->set_receive_ts(packet->get_receive_ts());
    req->set_request(packet->get_request());
    char *data_buffer = (char*)req + sizeof(ObPacket);
    memcpy(data_buffer, packet->get_buffer()->get_data(), packet->get_buffer()->get_capacity());
    req->get_buffer()->set_data(data_buffer, packet->get_buffer()->get_capacity());
    req->get_buffer()->get_position() = packet->get_buffer()->get_position();
    YYSYS_LOG(DEBUG, "packet_size=%ld data_size=%ld pos=%ld",
                packet_size, req->get_buffer()->get_capacity(), req->get_buffer()->get_position());
    */
    _cond.lock();
    ret = task_queue_.push(task);
    _cond.unlock();
  //}
  return ret;
//  YYSYS_LOG(INFO, "WDH_INFO: push wait_flush_thread succ, size: %d", packet_queue_.size());
}

//add :e

//add lxb [slave_ups_optimizer] 20180109:b
ObUpsReplayLogRunnable::ObUpsReplayLogRunnable()
{
    is_initialized_ = false;
    replay_to_queue_wait_time_us_ = DEFAULT_REPLAY_TO_QUEUE_WAIT_TIME_US;
    replay_to_disk_wait_time_us_ = DEFAULT_REPLAY_TO_DISK_WAIT_TIME_US;
}

ObUpsReplayLogRunnable::~ObUpsReplayLogRunnable()
{
}

int ObUpsReplayLogRunnable::init(ObUpsLogMgr *log_mgr)
{
    int ret = OB_SUCCESS;
    if(is_initialized_)
    {
        YYSYS_LOG(ERROR, "ObUpsReplayLogRunnable has been initialized");
        ret = OB_INIT_TWICE;
    }

    if(OB_SUCCESS == ret)
    {
        if(NULL == log_mgr)
        {
            YYSYS_LOG(ERROR, "Parameter is invalid[log_mgr=%p]",log_mgr);
        }
        else
        {
            log_mgr_ = log_mgr;
            is_initialized_ = true;
        }
    }
    return ret;
}

void ObUpsReplayLogRunnable::clear()
{
    if(NULL != _thread)
    {
        delete[] _thread;
        _thread = NULL;
    }
}

void ObUpsReplayLogRunnable::stop()
{
    _stop = true;
}

bool ObUpsReplayLogRunnable::wait_stop()
{
    return switch_.wait_off();
}

bool ObUpsReplayLogRunnable::wait_start()
{
    return switch_.wait_on();
}

void ObUpsReplayLogRunnable::run(yysys::CThread *thread, void *arg)
{
    int err = OB_SUCCESS;
    UNUSED(thread);
    UNUSED(arg);

    YYSYS_LOG(INFO,"ObUpsReplayLogRunnable start to run");
    if(!is_initialized_)
    {
        YYSYS_LOG(ERROR,"ObUpsReplayLogRunnable has not been initialized");
        err = OB_NOT_INIT;
    }
    SET_THD_NAME_ONCE("ups-log2que");

    while(!_stop)
    {
        if(switch_.check_off(true))
        {
            usleep(static_cast<useconds_t>(replay_to_queue_wait_time_us_));
        }
        else if(OB_SUCCESS !=(err = log_mgr_->replay_log_to_queue())
                && OB_NEED_RETRY != err && OB_NEED_WAIT != err)
        {
            if(OB_CANCELED != err)
            {
                YYSYS_LOG(ERROR,"log_mgr.replay_log_to_queue()=>%d",err);
            }
        }
        else if (OB_NEED_RETRY == err || OB_NEED_WAIT == err)
        {
            if(REACH_TIME_INTERVAL(50*1000*1000))
            {
                YYSYS_LOG(INFO,"replay need wait for write log ready %ld,%s, ret[%d]",replay_to_disk_wait_time_us_,to_cstring(*log_mgr_), err);
            }
            usleep(static_cast<useconds_t>(replay_to_disk_wait_time_us_));
        }
    }
    YYSYS_LOG(INFO,"ObUpsReplayLogRunnable finished[stop=%d ret=%d]",_stop,err);
}
//add:e


//add hxlong [asyn load log]
ObUpsLoadLogRunnable::ObUpsLoadLogRunnable()
{
    is_initialized_ = false;
    is_append_ = true;
    load_log_to_buff_wait_time_us_ = DEFAULT_LOAD_LOG_TO_BUFF_WAIT_TIME_US;
}
ObUpsLoadLogRunnable::~ObUpsLoadLogRunnable()
{

}

void ObUpsLoadLogRunnable::stop()
{
    _stop = true;
}

bool ObUpsLoadLogRunnable::wait_stop()
{
    return switch_.wait_off();
}

bool ObUpsLoadLogRunnable::wait_start()
{
    return switch_.wait_on();
}

void ObUpsLoadLogRunnable::clear()
{
    if(NULL != _thread)
    {
        delete[] _thread;
        _thread = NULL;
    }
}

int ObUpsLoadLogRunnable::init(ObUpsLogMgr *log_mgr)
{
    int ret = OB_SUCCESS;
    if(is_initialized_)
    {
        YYSYS_LOG(ERROR,"ObUpsLoadLogRunnable has been initialized");
        ret = OB_INIT_TWICE;
    }
    if(OB_SUCCESS == ret)
    {
        if(NULL == log_mgr)
        {
            YYSYS_LOG(ERROR, "Parameter is invalid[log_mgr=%p]",log_mgr);
        }
        else
        {
            log_mgr_ = log_mgr;
            is_initialized_ = true;
        }
    }
    return ret;
}

void ObUpsLoadLogRunnable::run(yysys::CThread *thread, void *arg)
{
    int err = OB_SUCCESS;
    UNUSED(thread);
    UNUSED(arg);

    YYSYS_LOG(INFO,"ObUpsLoadLogRunnable start to run");
    if(!is_initialized_)
    {
        YYSYS_LOG(ERROR,"ObUpsLoadLogRunnable has not been initialized");
        err = OB_NOT_INIT;
    }
    SET_THD_NAME_ONCE("ups-loadlog");

    while(!_stop)
    {
        //ÿ��ѭ������Ҫ�ж��Ƿ������߳̽���ر�
        //��������߳��뽫�ù���ֹͣ����Ҫ����wait_stop()��switch��״̬����ΪSWITCH_REQ_OFF
        //�´�ѭ��ʱ���̼߳��ʱ���ܽ���رգ�Ȼ��ȴ�����ʱ��һֱ�ж�
        //���ڱ�����ʱ����Ҫ����ر�
        if(switch_.check_off(true))
        {
            usleep(static_cast<useconds_t>(load_log_to_buff_wait_time_us_));
        }
        else if(OB_SUCCESS !=(err = log_mgr_->load_log_from_disk(is_append_))
                && OB_NEED_RETRY != err && OB_NEED_WAIT != err)
        {
            if(OB_CANCELED != err)
            {
                YYSYS_LOG(ERROR,"log_mgr.load_log_from_disk()=>%d",err);
            }
        }
        else if (OB_NEED_RETRY == err || OB_NEED_WAIT == err || !is_append_)
        {
            if(REACH_TIME_INTERVAL(50*1000*1000))
            {
                YYSYS_LOG(INFO, "load log need wait for replay log ready %ld, %s",load_log_to_buff_wait_time_us_,to_cstring(*log_mgr_));
            }
            usleep(static_cast<useconds_t>(load_log_to_buff_wait_time_us_));
        }
    }
    YYSYS_LOG(INFO,"ObUpsLoadLogRunnable finished[stop=%d ret=%d]",_stop,err);
}

//add:e


