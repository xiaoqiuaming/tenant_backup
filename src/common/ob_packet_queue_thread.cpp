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
 *   ruohai <ruohai@taobao.com>
 */

#include "ob_packet_queue_thread.h"
#include "ob_atomic.h"
#include "easy_io.h"
#include "ob_profile_log.h"
#include "ob_profile_type.h"
#include "ob_tsi_factory.h"

using namespace oceanbase::common;

static long *get_no_ptr(void)
{
  static __thread long p = 0;
  return &p;
}

static void set_thread_no(long n)
{
  long * p = get_no_ptr();
  if (NULL != p) *p = n;
}

static long get_thread_no(void)
{
  long *p = get_no_ptr();
  long no = 0;
  if (NULL != p)
  {
    no = *p;
  }
  return no;
}

timespec* mktimespec(timespec* ts, const int64_t time_ns)
{
  const int64_t NS_PER_SEC = 1000000000;
  ts->tv_sec = time_ns / NS_PER_SEC;
  ts->tv_nsec = time_ns % NS_PER_SEC;
  return ts;
}

ObPacketQueueThread::ObPacketQueueThread(int queue_capacity)
{
  _stop = 0;
  mktimespec(&timeout_, 3000000000);
  wait_finish_ = false;
  waiting_ = false;
  handler_ = NULL;
  args_ = NULL;
  queue_.init(queue_capacity, NULL);
  session_id_ = 1;
  next_wait_map_.create(MAX_THREAD_COUNT);
  max_waiting_thread_count_  = 0;
  waiting_thread_count_ = 0;
  next_packet_buffer_ = NULL;
}

ObPacketQueueThread::~ObPacketQueueThread()
{
  stop();
  queue_.destroy();
  next_wait_map_.destroy();
  if (NULL != next_packet_buffer_)
  {
    ob_free(next_packet_buffer_);
    next_packet_buffer_ = NULL;
  }
}

void ObPacketQueueThread::setThreadParameter(int thread_count, ObPacketQueueHandler *handler, void* args)
{
  setThreadCount(thread_count);
  handler_ = handler;
  args_ = args;

  //default all of threads for wait..
  //FIXME: streaming interface may use all the work threads, we will add
  //extra packet queue to handle the control packets in the funture
  if (0 == max_waiting_thread_count_)
  {
    max_waiting_thread_count_  = thread_count;
  }

  if (NULL == next_packet_buffer_)
  {
    next_packet_buffer_ = reinterpret_cast<char*>(ob_malloc(thread_count * MAX_PACKET_SIZE, ObModIds::OB_PACKET_QUEUE));
  }
}

void ObPacketQueueThread::stop(bool wait_finish)
{
  _stop = true;
  wait_finish_ = wait_finish;
}

bool ObPacketQueueThread::push(ObPacket* packet,int max_queue_len, bool block, bool deep_copy)
{
  if (_stop || _thread == NULL)
  {
    return false;
  }

  if (is_next_packet(packet))
  {
    if (!wakeup_next_thread(packet))
    {
      // free packet;
    }
    return true;
  }
  if (max_queue_len > 0 && queue_.size() >= max_queue_len)
  {
    if (!block)
    {
      return false;
    }
  }
  queue_.push(packet, NULL, block, deep_copy);
  return true;
}

void ObPacketQueueThread::set_ip_port(const IpPort & ip_port)
{
  ip_port_ = ip_port;
}
void ObPacketQueueThread::set_host(const ObServer &host)
{
  host_ = host;
}
bool ObPacketQueueThread::is_next_packet(ObPacket* packet) const
{
  bool ret = false;
  if (NULL != packet)
  {
    int32_t pcode = packet->get_packet_code();
    int64_t session_id = packet->get_session_id();
    ret = (((pcode == OB_SESSION_NEXT_REQUEST) || (OB_SESSION_END == pcode)) && (session_id != 0));
  }
  return ret;
}

int64_t ObPacketQueueThread::generate_session_id()
{
  return atomic_inc(&session_id_);
}

int64_t ObPacketQueueThread::get_session_id(ObPacket* packet) const
{
  int64_t session_id = packet->get_session_id();
  return session_id;
}

//�����������̳߳��ڲ�
ObPacket* ObPacketQueueThread::clone_next_packet(ObPacket* packet, int64_t thread_no) const
{
  char *clone_packet_ptr = NULL;
  ObPacket* clone_packet = NULL;

  ObDataBuffer* buf = packet->get_packet_buffer();
  int64_t inner_buf_size = buf->get_position();
  int64_t total_size = sizeof(ObPacket) + inner_buf_size;

  if (NULL != next_packet_buffer_ && total_size <= MAX_PACKET_SIZE)
  {
    clone_packet_ptr = next_packet_buffer_ + thread_no * MAX_PACKET_SIZE;
    memcpy(clone_packet_ptr, packet, total_size);
    clone_packet = reinterpret_cast<ObPacket*>(clone_packet_ptr);
    clone_packet->set_packet_buffer(clone_packet_ptr + sizeof(ObPacket), inner_buf_size);
  }

  return clone_packet;
}

//[701]-a
// ��IO�̵߳��ã��������ڵȴ�wait-next�����ObPacket�߳�
// ����ĳ��·�Ͽ���conn����ʱ�ͷ�ǰ����ֹ��������wait-next������
bool ObPacketQueueThread::wakeup_next_thread(int32_t conn_seq)
{
  bool ret = false;
  hash::ObHashMap<int64_t, WaitObject, hash::SpinReadWriteDefendMode>::iterator iter;
  for (iter = next_wait_map_.begin();
       iter != next_wait_map_.end();
       iter++)
  {
    WaitObject &wait_object = iter->second;
    if (conn_seq > 0 && wait_object.conn_seq_ == conn_seq)
    {
      if (NULL == wait_object.packet_)
      {
        next_cond_[wait_object.thread_no_].lock();
        next_cond_[wait_object.thread_no_].signal();
        next_cond_[wait_object.thread_no_].unlock();
        wait_object.errno_ = OB_ERR_SESSION_INTERRUPTED;
        ret = true;
        YYSYS_LOG(WARN, "wakeup thread by disconn seq[%d], thread_no=%ld ", conn_seq, wait_object.thread_no_);
      }
      else
      {
        YYSYS_LOG(WARN, "wakeup thread by disconn seq[%d], thread_no=%ld, pkt[%p]", conn_seq, wait_object.thread_no_, wait_object.packet_);
      }
      //���յ�ǰ�������ƣ�ÿ�߳��Ͻ���һ����·wait���ҵ�һ����·���˳�
      break;
    }
  }
  return ret;
}

int ObPacketQueueThread::disconn_clear(int32_t conn_seq, int64_t ref)
{
  UNUSED(ref);
  if (conn_seq > 0)
  {
    wakeup_next_thread(conn_seq);
    YYSYS_LOG(WARN, "disconnect cleanup c-seq[%d] conn-ref[%ld].", conn_seq, ref);
  }
  return OB_SUCCESS;
}
//[701]-E

bool ObPacketQueueThread::wakeup_next_thread(ObPacket* packet)
{
  bool ret = false;
  int64_t session_id = get_session_id(packet);
  WaitObject wait_object;
  int hash_ret = next_wait_map_.get(session_id, wait_object);
  if (hash::HASH_EXIST != hash_ret)
  {
    // no thread wait for this packet;
    // packet will not handled by server just tell libeasy request is done
      YYSYS_LOG(WARN, "wakeup thread not found, session_id=%ld error, packet(%p), request(%p)",
                session_id, packet, packet->get_request());
    easy_request_wakeup(packet->get_request());
    ret = false;
  }
  else
  {
    next_cond_[wait_object.thread_no_].lock();
    if ((NULL != wait_object.packet_) && (OB_SESSION_END != packet->get_packet_code()))
    {
      YYSYS_LOG(ERROR, "wakeup thread no = %ld, session_id=%ld error, packet=%p",
        wait_object.thread_no_, session_id, wait_object.packet_);
      // impossible! notify early by another next packet.
      ret = false;
    }
    else
    {
      if(OB_SESSION_END == packet->get_packet_code())
      {
        wait_object.packet_ = clone_next_packet(packet, wait_object.thread_no_);
        wait_object.end_session();
      }
      else
      {
        // got next packet, wakeup wait thread;
        wait_object.packet_ = clone_next_packet(packet, wait_object.thread_no_);
      }
      int overwrite = 1;
      hash_ret = next_wait_map_.set(session_id, wait_object, overwrite);
      if (hash::HASH_OVERWRITE_SUCC != hash_ret)
      {
        YYSYS_LOG(WARN, "rewrite set session =%ld packet=%p error,hash_ret=%d",
            session_id, wait_object.packet_, hash_ret);
      }
      //���Ѷ�Ӧ�Ĺ����߳̿�ʼ����
      next_cond_[wait_object.thread_no_].signal();
      ret = true;
    }
    next_cond_[wait_object.thread_no_].unlock();
  }
  return ret;
}

//[701]
//int ObPacketQueueThread::prepare_for_next_request(int64_t session_id)
int ObPacketQueueThread::prepare_for_next_request(int64_t session_id, int32_t conn_seq)
{
  int ret = OB_SUCCESS;

  WaitObject wait_object;
  wait_object.thread_no_ = get_thread_no();
  wait_object.packet_ = NULL;
  wait_object.conn_seq_ = conn_seq; //[701]

  int hash_ret = next_wait_map_.set(session_id, wait_object);

  if (-1 == hash_ret)
  {
    YYSYS_LOG(ERROR, "insert thread no = %ld, session_id=%ld error",
      wait_object.thread_no_, session_id);
    ret = OB_ERROR;
  }
  else if (hash::HASH_EXIST == hash_ret)
  {
    hash_ret = next_wait_map_.get(session_id, wait_object);
    if (hash::HASH_EXIST == hash_ret && NULL != wait_object.packet_ && (!wait_object.is_session_end()))
    {
      YYSYS_LOG(ERROR, "insert thread no = %ld, session_id=%ld exist, and packet not null and not end session = %p",
        wait_object.thread_no_, session_id, wait_object.packet_);
      // impossible , either last wait not set to NULL or next request
      // reached before pepare..
      ret = OB_ERROR;
    }
  }

  return ret;
}

int ObPacketQueueThread::destroy_session(int64_t session_id)
{
  int ret = OB_SUCCESS;

  ret = next_wait_map_.erase(session_id);
  if (-1 == ret)
  {
    ret = OB_ERROR;
  }
  else if (hash::HASH_NOT_EXIST == ret)
  {
    YYSYS_LOG(WARN, "session_id = %ld not exist, destroy do nothing.", session_id);
    ret = OB_ERROR;
  }
  else
  {
    ret = OB_SUCCESS;
  }

  return ret;
}

int ObPacketQueueThread::wait_for_next_request(int64_t session_id, ObPacket* &next_request, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  int hash_ret = 0;
  WaitObject wait_object;
  next_request = NULL;
  uint64_t oldv = 0;

  hash_ret = next_wait_map_.get(session_id, wait_object);
  if (hash::HASH_NOT_EXIST == hash_ret)
  {
    YYSYS_LOG(WARN, "session_id =%ld not exist, prepare first.", session_id);
    ret = OB_ERR_UNEXPECTED; //NOT_PREPARED;
  }

  while (OB_SUCCESS == ret)
  {
    //waiting_thread_count_ �ȴ���ʽ�ӿڵİ����̵߳ĸ���
    oldv = waiting_thread_count_;
    if (oldv > max_waiting_thread_count_)
    {
      YYSYS_LOG(INFO, "current wait thread =%ld >= max wait =%ld",
        oldv, max_waiting_thread_count_);
      ret = OB_EAGAIN;
    }
    else if (atomic_compare_exchange(&waiting_thread_count_, oldv+1, oldv) == oldv)
    {
      next_cond_[wait_object.thread_no_].lock();
      hash_ret = next_wait_map_.get(session_id, wait_object);
      if (hash::HASH_NOT_EXIST == hash_ret)
      {
        YYSYS_LOG(WARN, "session_id =%ld not exist, prepare first.", session_id);
        ret = OB_ERR_UNEXPECTED; //NOT_PREPARED;
      }
      else if(wait_object.is_session_end())
      {
        next_request = wait_object.packet_;
        ret = OB_NET_SESSION_END;
      }
      // prepare_for_next_request��ʱ��packet_��ΪNULL
      else if (NULL == wait_object.packet_)
      {
        int32_t timeout_ms = static_cast<int32_t>(timeout/1000);
        //��IO�̻߳���
        if (timeout_ms > 0 && next_cond_[wait_object.thread_no_].wait(timeout_ms))
        {
          hash_ret = next_wait_map_.get(session_id, wait_object);
          if((hash::HASH_EXIST == hash_ret) && wait_object.is_session_end())
          {
            next_request = wait_object.packet_;
            ret = OB_NET_SESSION_END;
          }
          else if (hash::HASH_EXIST != hash_ret || NULL == wait_object.packet_)
          {
            YYSYS_LOG(ERROR, "cannot get packet of session = %ld.", session_id);
            //[701]
            if (wait_object.errno_ == OB_ERR_SESSION_INTERRUPTED){
              ret = OB_ERR_SESSION_INTERRUPTED;
            }else {
            ret = OB_ERROR;
            }
          }
          else
          {
            //��hash���д��ڣ�Ҫô��session end��Ҫô����ͨ����ʽ�ӿڰ�
            if (wait_object.is_session_end())
            {
              next_request = wait_object.packet_;
              ret = OB_NET_SESSION_END;
            }
            else
            {
              next_request = wait_object.packet_;
              ret = OB_SUCCESS;
            }
          }
        }
        else
        {
          // ��ʱ
          next_request = NULL;
          ret = OB_RESPONSE_TIME_OUT; //WAIT_TIMEOUT;
        }
      }
      else
      {
        //�Ѿ�ȡ������
        // check if already got the request..
        next_request = wait_object.packet_;
      }

      // clear last packet,�յ���ʽ�ӿں��ֽ�packet_ ��ΪNULL
      wait_object.packet_ = NULL;
      int overwrite = 1;
      hash_ret = next_wait_map_.set(session_id, wait_object, overwrite);
      if (hash::HASH_OVERWRITE_SUCC != hash_ret)
      {
        YYSYS_LOG(WARN, "rewrite clear session =%ld packet=%p error, hash_ret=%d",
            session_id, next_request, hash_ret);
      }

      next_cond_[wait_object.thread_no_].unlock();
      atomic_dec(&waiting_thread_count_);
      break;
    }
  }
  if((NULL != next_request) && (OB_SESSION_END == next_request->get_packet_code()))
  {
    ret = OB_NET_SESSION_END;
  }
  return ret;
}

void ObPacketQueueThread::run(yysys::CThread* thread, void* args)
{
  UNUSED(thread);

  long thread_no = reinterpret_cast<long>(args);
  set_thread_no(thread_no);
  ObServer *host = GET_TSI_MULT(ObServer, TSI_COMMON_OBSERVER_1);
  *host = host_;
  ObPacket* packet = NULL;
  void *task = NULL;
  SET_THD_NAME("obpacket");//[647]
  while (!_stop)
  {
    if (OB_SUCCESS != queue_.pop(task, &timeout_))
    {
      continue;
    }
    packet = reinterpret_cast<ObPacket*>(task);
    if (handler_)
    {
      YYSYS_LOG(DEBUG, "pop packet code is %d", packet->get_packet_code());
      int64_t trace_id = packet->get_trace_id();
      if (0 == trace_id)
      {
        TraceId *new_id = GET_TSI_MULT(TraceId, TSI_COMMON_PACKET_TRACE_ID_1);
        (new_id->id).seq_ = atomic_inc(&(SeqGenerator::seq_generator_));
        (new_id->id).ip_ = ip_port_.ip_;
        (new_id->id).port_ = ip_port_.port_;
        packet->set_trace_id(new_id->uval_);
      }
      else
      {
        TraceId *id = GET_TSI_MULT(TraceId, TSI_COMMON_PACKET_TRACE_ID_1);
        id->uval_ = static_cast<uint64_t>(trace_id);
      }
      uint32_t *src_channel_id = GET_TSI_MULT(uint32_t, TSI_COMMON_PACKET_SOURCE_CHID_1);
      //����Դ����chid���õ��߳���
      *src_channel_id = packet->get_channel_id();
      // reset
      uint32_t *channel_id = GET_TSI_MULT(uint32_t, TSI_COMMON_PACKET_CHID_1);
      *channel_id = 0;
      int64_t st = yysys::CTimeUtil::getTime();
      PROFILE_LOG(DEBUG, HANDLE_PACKET_START_TIME PCODE, st, packet->get_packet_code());
      //[701]-b
      if (packet->get_disconn_time() == 0) {
        handler_->handlePacketQueue(packet, args_);
      } else if (st - packet->get_disconn_time() < OB_CONNECTION_FREE_TIME_S*1000000){
        easy_request_wakeup(packet->get_request());
        YYSYS_LOG(WARN, "pop packet[%p] code[%d] discard recv[%ld] conn-break[%ld] now[%ld], que-size[%d].",
                  packet, packet->get_packet_code(), packet->get_receive_ts(), packet->get_disconn_time(), st, queue_.size());
      } else {
        YYSYS_LOG(WARN, "pop packet code:%d, disconnect timeout recv[%ld] conn-break[%ld] now[%ld], que-size[%d].",
                  packet->get_packet_code(), packet->get_receive_ts(), packet->get_disconn_time(), st, queue_.size());
      }
      //[701]-e
      int64_t ed = yysys::CTimeUtil::getTime();
      PROFILE_LOG(DEBUG, HANDLE_PACKET_END_TIME PCODE, ed, packet->get_packet_code());
    }
  }
  while (queue_.size() > 0)
  {
    if (OB_SUCCESS != queue_.pop(task, &timeout_))
    {
      continue;
    }
    packet = reinterpret_cast<ObPacket*>(task);
    if (handler_ && wait_finish_)
    {
      YYSYS_LOG(DEBUG, "pop packet code is %d", packet->get_packet_code());
      int64_t trace_id = packet->get_trace_id();
      if (0 == trace_id)
      {
        //���ⲿ������packet��trace idΪ0
        TraceId *new_id = GET_TSI_MULT(TraceId, TSI_COMMON_PACKET_TRACE_ID_1);
        (new_id->id).seq_ = atomic_inc(&(SeqGenerator::seq_generator_));
        (new_id->id).ip_ = ip_port_.ip_;
        (new_id->id).port_ = ip_port_.port_;
        //����һ��trace id
        packet->set_trace_id(new_id->uval_);
      }
      else
      {
        TraceId *id = GET_TSI_MULT(TraceId, TSI_COMMON_PACKET_TRACE_ID_1);
        id->uval_ = static_cast<uint64_t>(trace_id);
      }
      uint32_t *src_channel_id = GET_TSI_MULT(uint32_t, TSI_COMMON_PACKET_SOURCE_CHID_1);
      *src_channel_id = packet->get_channel_id();
      // reset
      uint32_t *channel_id = GET_TSI_MULT(uint32_t, TSI_COMMON_PACKET_CHID_1);
      *channel_id = 0;
      int64_t st = yysys::CTimeUtil::getTime();
      //���ʱ�����������Դ������û�п�ʼ����������chid id����Ϊ0
      PROFILE_LOG(DEBUG, HANDLE_PACKET_START_TIME PCODE, st, packet->get_packet_code());
      //handler_->handlePacketQueue(packet, args_);
      //[701]-b
      if (packet->get_disconn_time() == 0) {
        handler_->handlePacketQueue(packet, args_);
      } else if (st - packet->get_disconn_time() < OB_CONNECTION_FREE_TIME_S*1000000){
        easy_request_wakeup(packet->get_request());
        YYSYS_LOG(WARN, "pop packet[%p] code[%d] discard recv[%ld] conn-break[%ld] now[%ld], que-size[%d].",
                  packet, packet->get_packet_code(), packet->get_receive_ts(), packet->get_disconn_time(), st, queue_.size());
      } else {
        YYSYS_LOG(WARN, "pop packet code:%d, disconnect timeout recv[%ld] conn-break[%ld] now[%ld], que-size[%d].",
                  packet->get_packet_code(), packet->get_receive_ts(), packet->get_disconn_time(), st, queue_.size());
      }
      //[701]-e
      int64_t ed = yysys::CTimeUtil::getTime();
      //�����Ѿ�����chid id��
      PROFILE_LOG(DEBUG, HANDLE_PACKET_END_TIME PCODE, ed);
    }
  }
}
void ObPacketQueueThread::clear()
{
  _stop = false;
  delete[] _thread;
  _thread = NULL;
}
