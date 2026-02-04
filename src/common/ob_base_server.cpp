#include "ob_base_server.h"
#include "ob_tbnet_callback.h"
#include "ob_tsi_factory.h"
#include "ob_libeasy_mem_pool.h"
#include "easy_pool.h"

#ifdef __COVERAGE__
extern "C" void __gcov_flush();
#endif

namespace oceanbase
{
  namespace common
  {
    ObBaseServer::ObBaseServer() : stoped_(false), batch_(false), port_(0), eio_(NULL), local_ip_(0)
    , disconn_pkt_que_(NULL) //[701]
    {
    }

    ObBaseServer::~ObBaseServer()
    {
      if (NULL != eio_)
      {
        easy_eio_destroy(eio_);
        eio_ = NULL;
      }
    }

    int ObBaseServer::initialize()
    {
      tzset();
      return OB_SUCCESS;
    }
    int ObBaseServer::start_service()
    { return OB_SUCCESS; }

    void ObBaseServer::wait_for_queue()
    {}

    void ObBaseServer::wait()
    {
      if (NULL != eio_)
      {
        easy_eio_wait(eio_);
      }
    }


    void ObBaseServer::destroy()
    {
      YYSYS_LOG(WARN, "base server destroyed");
    }

    int ObBaseServer::start(bool need_wait)
    {
      int rc = OB_SUCCESS;
      int ret = EASY_OK;
      easy_pool_set_allocator(ob_easy_realloc);
      easy_listen_t *listen = NULL;
      //create io thread
      eio_ = easy_eio_create(eio_, io_thread_count_);
      eio_->do_signal = 0;
      eio_->force_destroy_second = OB_CONNECTION_FREE_TIME_S;
      eio_->checkdrc = 1;
      eio_->support_ipv6 = 0;
      eio_->no_redispatch = 1;
      eio_->no_delayack = 1;
      easy_eio_set_uthread_start(eio_, easy_on_ioth_start, this);
      eio_->uthread_enable = 0;
      if (NULL == eio_)
      {
        rc = OB_ERROR;
        YYSYS_LOG(ERROR, "easy_io_create error");
      }

      if (OB_SUCCESS == rc)
      {
        rc = initialize();
      }

      if (rc != OB_SUCCESS)
      {
        YYSYS_LOG(WARN, "initialize failed, ret = %d", rc);
      }
      else
      {
        //add listen
        uint32_t local_ip = yysys::CNetUtil::getLocalAddr(dev_name_);
        local_ip_ = local_ip;
        server_id_ = yysys::CNetUtil::ipToAddr(local_ip, port_);
        if (OB_SUCCESS == rc)
        {
          if (NULL == (listen = easy_connection_add_listen(eio_, NULL, port_, &server_handler_)))
          {
            YYSYS_LOG(ERROR, "easy_connection_add_listen error, port: %d, %s", port_, strerror(errno));
            rc = OB_SERVER_LISTEN_ERROR;
          }
          else
          {
            YYSYS_LOG(INFO, "listen start, port = %d", port_);
          }
        }
        if (OB_SUCCESS == rc)
        {
          easy_io_thread_t *ioth = NULL;
          easy_thread_pool_for_each(ioth, eio_->io_thread_pool, 0)
          {
            ev_timer_init(&ioth->user_timer, easy_timer_cb, OB_LIBEASY_STATISTICS_TIMER, OB_LIBEASY_STATISTICS_TIMER);
            ev_timer_start(ioth->loop, &(ioth->user_timer));
          }
        }

        //start io thread
        if (rc == OB_SUCCESS)
        {
          ret = easy_eio_start(eio_);
          if (EASY_OK == ret)
          {
            rc = OB_SUCCESS;
            YYSYS_LOG(INFO, "start io thread");
          }
          else
          {
            YYSYS_LOG(ERROR, "easy_eio_start failed");
            rc = OB_ERROR;
          }
        }
        if (rc == OB_SUCCESS)
        {
          rc = start_service();
        }

        if (rc == OB_SUCCESS)
        {
          //wait for io thread exit
          if (need_wait)
          {
            easy_eio_wait(eio_);
          }
        }
      }
      //stop
      if (need_wait)
      {
        stop();
      }

      return rc;
    }

    void ObBaseServer::stop_eio()
    {
      if (!stoped_)
      {
        stoped_ = true;
        destroy();
        if (eio_ != NULL && NULL != eio_->pool)
        {
          easy_eio_stop(eio_);
        }
#ifdef __COVERAGE__
__gcov_flush();
#endif
        YYSYS_LOG(INFO, "stop eio.");
      }
    }

    void ObBaseServer::stop()
    {
      if (!stoped_)
      {
        stoped_ = true;
        YYSYS_LOG(WARN, "start to stop the server");
        destroy();
        YYSYS_LOG(WARN, "start to stop eio");
        if (eio_ != NULL && NULL != eio_->pool)
        {
          easy_eio_stop(eio_);
          easy_eio_wait(eio_);
          easy_eio_destroy(eio_);
          eio_ = NULL;
        }
#ifdef __COVERAGE__
__gcov_flush();
#endif
        YYSYS_LOG(WARN, "server stoped.");
      }
    }

    void ObBaseServer::set_batch_process(const bool batch)
    {
      batch_ = batch;
      YYSYS_LOG(INFO, "batch process mode %s", batch ? "enabled" : "disabled");
    }

    int ObBaseServer::set_dev_name(const char* dev_name)
    {
      int rc = OB_INVALID_ARGUMENT;

      if (dev_name != NULL)
      {
        YYSYS_LOG(INFO, "set interface name to [%s]", dev_name);
        strncpy(dev_name_, dev_name, DEV_NAME_LENGTH);
        // modify for [build warning]-b
//        int end = sizeof(dev_name_) - 1;
        int end = static_cast<int>(sizeof(dev_name_) - 1);
        // modify for [build warning]-e
        dev_name_[end] = '\0';
        rc = OB_SUCCESS;
      }
      else
      {
        YYSYS_LOG(WARN, "interface name is NULL, will use default interface.");
      }

      return rc;
    }

    int ObBaseServer::set_listen_port(const int port)
    {
      int rc = OB_INVALID_ARGUMENT;

      if (port > 0)
      {
        port_ = port;
        rc = OB_SUCCESS;

        if (port_ < 1024)
        {
          YYSYS_LOG(WARN, "listen port less than 1024--[%d], make sure this is what you want.", port_);
        }
        else
        {
          YYSYS_LOG(INFO, "listen port set to [%d]", port_);
        }
      }
      else
      {
        YYSYS_LOG(WARN, "listen post should be positive, you provide: [%d]", port);
      }

      return rc;
    }

    uint64_t ObBaseServer::get_server_id() const
    {
      return server_id_;
    }

    //[648]
    int ObBaseServer::check_request_cleanup(easy_request_t *req, const char *fn)
    {
        int rc = OB_SUCCESS;
        ObPacket *packet = (ObPacket *)req->ipacket;
        if(packet == NULL || packet->is_cleanup() || req->ms->c->fd == -1)
        {
            YYSYS_LOG(ERROR, "call by(%s) request (%p) packet(%p) had cleanup(%d).", fn, req, packet, packet->is_cleanup());
            rc = OB_LIBEASY_ERROR;
        }
        //[701]-b
        if (req->ms->c->status == EASY_CONN_CLOSE &&
            packet->get_disconn_time() > 0 &&
            yysys::CTimeUtil::getTime() - packet->get_disconn_time() > 30000000)
        {//������ڴ����У��ȴ�����30s������·����
          YYSYS_LOG(ERROR, "conn close a(%s) request(%p) packet(%p) ", fn, req, packet);
          rc = OB_CONNECT_ERROR;
        }
        //[701]-e
        return rc;
    }

    int ObBaseServer::send_response(const int32_t pcode, const int32_t version, const ObDataBuffer& buffer, easy_request_t* req, const uint32_t channel_id, const int64_t session_id)
    {
      int rc = OB_SUCCESS;

      if (req == NULL)
      {
        rc = OB_ERROR;
        YYSYS_LOG(WARN, "req is NULL");
      }
      else
      {
          easy_connection_t *conn = req->ms->c;
        //copy packet into req buffer
        int64_t size = buffer.get_position() + OB_RECORD_HEADER_LENGTH + sizeof(ObPacket);
        char* ibuffer = reinterpret_cast<char*>(easy_pool_alloc(req->ms->pool, static_cast<uint32_t>(size)));
        if (NULL == ibuffer)
        {
          YYSYS_LOG(WARN, "alloc buffer from req->ms->pool failed");
          rc = OB_LIBEASY_ERROR;
        }

        if (OB_SUCCESS == rc)
        {
          ObPacket* packet = new(ibuffer)ObPacket();
          packet->set_packet_code(pcode);
          packet->set_channel_id(channel_id);
          packet->set_api_version(version);
          packet->set_data(buffer);
          packet->set_session_id(session_id);
          TraceId *trace_id = GET_TSI_MULT(TraceId, TSI_COMMON_PACKET_TRACE_ID_1);
          if (NULL == trace_id)
          {
            rc = OB_ALLOCATE_MEMORY_FAILED;
            YYSYS_LOG(ERROR, "trace id == NULL, ret=%d", rc);
          }
          else
          {
            packet->set_trace_id(trace_id->uval_);
            ObDataBuffer ibuf(reinterpret_cast<char*>(packet + 1), size - sizeof(ObPacket));
            rc = packet->serialize(&ibuf);
            if (rc != OB_SUCCESS)
            {
              YYSYS_LOG(ERROR, "packet serialize error, error: %d", rc);
            }
            else
            {
              packet->set_packet_buffer(ibuf.get_data(), ibuf.get_position());
              packet->get_inner_buffer()->get_position() = ibuf.get_position();
              req->opacket = packet;
              YYSYS_LOG(DEBUG, "packet channel is %u, req is %p, c is %p", packet->get_channel_id(), req, conn);
            }
          }
        }
        if((rc = check_request_cleanup(req, __FUNCTION__)) == OB_SUCCESS){
            //wakeup the ioth to send response
            req->retcode = EASY_OK;
            easy_request_wakeup(req);
        }
        else
        {
            YYSYS_LOG(WARN, "packet send failed, req(%p), c(%p).", req, conn);
        }
      }
      return rc;
    }
    void ObBaseServer::easy_timer_cb(EV_P_ ev_timer *w, int revents)
    {
      UNUSED(w);
      UNUSED(loop);
      UNUSED(revents);
      char buffer[FD_BUFFER_SIZE];
      int64_t pos = 0;
      databuff_printf(buffer, FD_BUFFER_SIZE, pos, "tid=%ld fds=", EASY_IOTH_SELF->tid);
      easy_connection_t *c = NULL;
      easy_connection_t *c2 = NULL;
      easy_list_for_each_entry_safe(c, c2, &(EASY_IOTH_SELF->connected_list), conn_list_node)
      {
        //[701]
        if(c->status == EASY_CONN_CLOSE){
          databuff_printf(buffer, FD_BUFFER_SIZE, pos, "%d/c,", c->fd);
        }
        else
        {
          databuff_printf(buffer, FD_BUFFER_SIZE, pos, "%d,", c->fd);
        }

      }
      YYSYS_LOG(INFO, "%.*s", static_cast<int>(pos), buffer);
    }
  } /* common */
} /* oceanbase */
