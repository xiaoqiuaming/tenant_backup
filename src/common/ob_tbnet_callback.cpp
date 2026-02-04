#include "ob_tbnet_callback.h"
#include "data_buffer.h"
#include "yylog.h"
#include "ob_define.h"
#include "easy_io.h"
#include "easy_socket.h"
#include "easy_define.h"
#include "ob_single_server.h"
#include <byteswap.h>
#include "utility.h"
#include "ob_shadow_server.h"
#include "ob_trace_id.h"
#include "ob_profile_log.h"
#include "ob_common_stat.h"
namespace oceanbase
{
  namespace common
  {

    //decode packet from networ using m->pool memory for packet
    //m->pool will destroyed when request done
    void* ObTbnetCallback::decode(easy_message_t *m)
    {
      int flag = 0;
      int pcode = 0;
      int packetlen = 0;
      int datalen = 0;
      int len = 0;
      int buflen = 0;
      uint32_t chid = 0;
      uint16_t ob_packet_header_size = 0;
      int32_t api_version = 0;
      int32_t timeout = 0;
      int64_t session_id = 0;
      uint64_t trace_id = 0;
      uint64_t req_sign = 0;
      ObDataBuffer* buffer = NULL;
      ObPacket* packet = NULL;
      char* buff = NULL;

      if (NULL == m || NULL == m->input)
      {
        YYSYS_LOG(ERROR, "invalid argument m is %p", m);
      }
      else if (NULL == m->input)
      {
        YYSYS_LOG(ERROR, "invalid argument m->input is %p", m->input);
      }
      else
      {
        //check yynet header readed
        if ((len = static_cast<int>(m->input->last - m->input->pos)) >= OB_TBNET_HEADER_LENGTH)
        {
          //decode member from m->input buffer
          flag = bswap_32(*((uint32_t *)m->input->pos));
          chid = bswap_32(*((uint32_t *)(m->input->pos + 4)));
          pcode = bswap_32(*((uint32_t *)(m->input->pos + 8)));
          packetlen = bswap_32(*((uint32_t *)(m->input->pos + 12)));

          //check yynet flag and packet len
          if (OB_TBNET_PACKET_FLAG != flag || packetlen <= 0 || packetlen > 0x4000000)
          {
            YYSYS_LOG(ERROR, "yynet flag:%x<>%x, datalen:%d, peer is %s", flag, OB_TBNET_PACKET_FLAG,
                      packetlen, inet_ntoa_r(m->c->addr));
            m->status = EASY_ERROR;
          }
          else
          {
            len -= OB_TBNET_HEADER_LENGTH;

            if (len < packetlen)
            {
              m->next_read_len = packetlen - len;
              //YYSYS_LOG(WARN, "not enough data in socket packetlen is %d, data in socket is %d", packetlen, len);
            }
            else
            {
              //add rpc bytes in statistics
              OB_STAT_INC(COMMON, RPC_BYTES_IN, packetlen + OB_TBNET_HEADER_LENGTH);
              int64_t header_size = 0;
              m->input->pos += OB_TBNET_HEADER_LENGTH;
              uint32_t tmp = 0;
              //̫�����ˣ�bswap_16 �ò���
              tmp = bswap_32(*reinterpret_cast<int32_t*>(m->input->pos));
              ob_packet_header_size = static_cast<uint16_t>(tmp >> 16);
              api_version = 0x0000FFFF & tmp;
              session_id = bswap_64(*reinterpret_cast<uint64_t *>(m->input->pos + 4));
              timeout = bswap_32(*reinterpret_cast<uint32_t *>(m->input->pos + 12));
#if !defined(_OB_VERSION) || _OB_VERSION<=300
              // 0.3 server �ӵ���0.3 �İ�
              if (0 == ob_packet_header_size)
              {
                /* 0.3 ObPacket ͷ��ʽ: 16 bytes */
                /*         2 bytes                2 bytes         8 bytes     4 bytes
                 *----------------------------------------------------------------------
                 *| ob_packet_header_size_== 0 | api_version_ | session_id_ | timeout_ |
                 *----------------------------------------------------------------------
                */
                header_size = sizeof(ob_packet_header_size) + sizeof(int16_t)/* api_version_ */ + sizeof(session_id) + sizeof(timeout);
              }
              // 0.3 server �ӵ��� 0.4 �İ�
              else
              {
                /* 0.4 ��ObPacket ͷ��ʽ: 32 bytes */
                /*         2 bytes            2 bytes         8 bytes     4 bytes   8 bytes   8 bytes
                 *--------------------------------------------------------------------------------------
                 *| ob_packet_header_size_ | api_version_ | session_id_ | timeout_ |trace_id_|req_sign_|
                 *--------------------------------------------------------------------------------------
                */
                trace_id = bswap_64(*reinterpret_cast<uint64_t *>(m->input->pos + 16));
                req_sign = bswap_64(*reinterpret_cast<uint64_t *>(m->input->pos + 16 + 8));
                UNUSED(trace_id);
                UNUSED(req_sign);
                header_size = ob_packet_header_size;
              }
#elif _OB_VERSION>300
              // 0.4 server �ӵ��� 0.3 �İ�
              if (0 == ob_packet_header_size)
              {
                header_size = sizeof(ob_packet_header_size) + sizeof(int16_t)/* api_version_ */ + sizeof(session_id) + sizeof(timeout);
              }
              // 0.4 server �ӵ��� 0.4 �İ�
              else
              {
                trace_id = bswap_64(*reinterpret_cast<uint64_t *>(m->input->pos + 16));
                req_sign = bswap_64(*reinterpret_cast<uint64_t *>(m->input->pos + 16 + 8));
                header_size = ob_packet_header_size;
              }
#endif
              buflen = packetlen - static_cast<int>(header_size);
              //alloc mem from m->pool
              buff = reinterpret_cast<char*>(easy_pool_alloc(m->pool,
                                                              static_cast<uint32_t>(sizeof(ObPacket) + buflen)));
              //alloc mem failed just set libeasy message status to EASY_ERROR will destroy current connection
              if (NULL == buff)
              {
                YYSYS_LOG(WARN, "alloc packet buffer from m->pool failed");
                m->status = EASY_ERROR;
                m->input->pos -= OB_TBNET_HEADER_LENGTH;
              }
              else
              {
                packet = new(buff)ObPacket();
                // inner buffer
                packet->set_packet_buffer(buff + sizeof(ObPacket), buflen);

                packet->set_channel_id(chid);
                packet->set_packet_code(pcode);
                packet->set_packet_len(packetlen);

                packet->set_api_version(api_version);
                packet->set_session_id(session_id);
                packet->set_source_timeout(timeout);
                packet->set_ob_packet_header_size(ob_packet_header_size);
                packet->set_conn_seq(m->c->seq); //[701]
#if !defined(_OB_VERSION) || _OB_VERSION<=300
                // nothing
#elif _OB_VERSION>300
                // 0.4 server �ӵ��� 0.4 �İ�
                if (ob_packet_header_size > 0)
                {
                  packet->set_trace_id(trace_id);
                  packet->set_req_sign(req_sign);
                }
#endif
                m->input->pos += header_size;

                datalen = buflen - OB_RECORD_HEADER_LENGTH;
                packet->set_data_length(datalen);

                buffer = packet->get_inner_buffer();
                if (buffer->get_remain() >= buflen)
                {
                  memcpy(buffer->get_data(), m->input->pos, buflen);
                  m->input->pos += buflen;
                  buffer->get_position() = buflen;
                  int64_t recv_time = yysys::CTimeUtil::getTime();
                  packet->set_receive_ts(recv_time);
                  //deserialize record header and do sum check
                  int err = packet->deserialize();
                  //deserialize packet failed dataflow error close current connection
                  if (OB_SUCCESS != err)
                  {
                    m->status = EASY_ERROR;
                    YYSYS_LOG(ERROR, "deserialize packet failed packet channel is %d packet code is %d, err=%d", packet->get_channel_id(),
                              packet->get_packet_code(), err);
                    packet = NULL;
                  }
                  else
                  {
                    YYSYS_LOG(DEBUG, "decode packet from network packet channel is %d packet code is %d, err=%d", packet->get_channel_id(),
                            packet->get_packet_code(), err);
                  }
                }
                else
                {
                  //buff not enough
                  m->input->pos += buflen;
                  YYSYS_LOG(WARN, "inner buffer is not enough, need: %d, real: %ld skip it", buflen, buffer->get_remain());
                }
              }
            }
          }
        }
      }
      return packet;
    }

    int ObTbnetCallback::encode(easy_request_t *r, void *data)
    {
      int ret = EASY_OK;
      easy_buf_t* b = NULL;
      char* buff = NULL;
      ObPacket* packet = reinterpret_cast<ObPacket*>(data);
      int64_t header_size = 0;
      uint16_t ob_packet_header_size = 0;
#if !defined(_OB_VERSION) || _OB_VERSION<=300
      /* 0.3 ObPacket ͷ��ʽ: 16 bytes */
      /*         2 bytes                 2 bytes       8 bytes     4 bytes
       *----------------------------------------------------------------------
       *| ob_packet_header_size_== 0 | api_version_ | session_id_ | timeout_ |
       *----------------------------------------------------------------------
       */
      ob_packet_header_size = 0;
      header_size = sizeof(uint16_t)/* ob_packet_header_size_ */ + sizeof(int16_t) /* api_version_ */+ sizeof(int64_t) /* session_id_ */+ sizeof(int32_t)/* timeout_ */;
#elif _OB_VERSION>300
      /* 0.4 ObPacket ͷ��ʽ�� 24 bytes */
      /*         2 bytes            2 bytes       8 bytes     4 bytes   8 bytes     8 bytes
       *--------------------------------------------------------------------------------------
       *| ob_packet_header_size_ | api_version_ | session_id_ | timeout_ |trace_id_|req_sign_|
       *--------------------------------------------------------------------------------------
       */
      ob_packet_header_size = static_cast<uint16_t>(sizeof(ob_packet_header_size) + sizeof(int16_t)/* api_version_ */ + sizeof(int64_t) /* session_id_ */ + sizeof(int32_t)/* timeout_ */ + sizeof(uint64_t)/* trace_id_ */ + sizeof(uint64_t) /* req_sign_ */);
      header_size = ob_packet_header_size;
#endif
      packet->set_ob_packet_header_size(ob_packet_header_size);
      int64_t size = packet->get_inner_buffer()->get_position() + OB_TBNET_HEADER_LENGTH + header_size;
      b = reinterpret_cast<easy_buf_t *>(easy_pool_alloc(r->ms->pool,
                                                          static_cast<uint32_t>(sizeof(easy_buf_t) + size)));
      if (NULL == b)
      {
        YYSYS_LOG(WARN, "alloc mem for send buffer failed buf=%p size is %lu", b,
                  sizeof(easy_buf_t) + size);
        ret = EASY_ERROR;
      }
      else
      {
        //skip sizeof(easy_buf_t) bytes
        buff = reinterpret_cast<char *>(b + 1);
        //b->pos = buff;
        //b->end = buff + size;
        //set packet length
        init_easy_buf(b, buff, r, size);
        packet->set_packet_len(static_cast<int>(
                                 packet->get_inner_buffer()->get_position() + header_size));

        if (false == packet->encode(buff, size))
        {
          ret = EASY_ERROR;
          YYSYS_LOG(WARN, "encode failed packet is %p, buff is %p, size is %ld", packet, buff, size);
        }

        if (EASY_OK == ret)
        {
          easy_buf_set_data(r->ms->pool, b, buff, static_cast<uint32_t>(size));
          easy_request_addbuf(r, b);
          //add rpc bytes out statistics
          OB_STAT_INC(COMMON, RPC_BYTES_OUT, size);
          YYSYS_LOG(DEBUG, "encode packet success packet code is %d, c is %s",packet->get_packet_code(),
                    easy_connection_str(r->ms->c));
        }
      }

      return ret;
    }

    //[701]
    void ObTbnetCallback::easy_disconn_cb(easy_connection_t *c, int64_t now)
    {
      //��ObPacket�ͻ��ˣ������������ж������ͨ�����е�disconn_time==0�ж��Ƿ����
      easy_message_t  *m, *m2;
      easy_request_t *r, *n;

      easy_list_for_each_entry_safe(m, m2, &c->message_list, message_list_node) {
        if(m->request_list_count > 0){
          easy_list_for_each_entry_safe(r, n, &m->all_list, all_node) {
            if (r->ipacket != NULL) {
              ObPacket *ptask = reinterpret_cast<ObPacket*>(r->ipacket);
              if (ptask != NULL && c->seq == ptask->get_conn_seq()){
                ptask->set_disconn_time(now);
                YYSYS_LOG(WARN, "obpacket disconn task set disconnect time task[%p]", ptask);
              }
            }
          }
        }

      }
    }

    int ObTbnetCallback::on_disconnect(easy_connection_t* c)
    {
      int ret = EASY_OK;
      if (NULL == c)
      {
        YYSYS_LOG(WARN, "invalid argument c is %p", c);
        ret = EASY_ERROR;
      }
      else
      {
        //[701]
        //c->status ������EASY_CONN_CLOSE, on-disconnect�޷���⵽EASY_CONN_CLOSE
        if (c->type == EASY_TYPE_SERVER && c->pool->ref > 0)
        {
          YYSYS_LOG(WARN, "disconnect c[%s] pool->ref[%ld].", easy_connection_str(c), c->pool->ref);
          //�д����е���Ϣ��ֹͣ��Ȼ�ڵȴ���ִ�е�session
          ObBaseServer *server = reinterpret_cast<ObBaseServer*>(c->handler->user_data);
          if(server != NULL){
            int64_t now = yysys::CTimeUtil::getTime();
            server->disconn_clear_task(c->seq, c->pool->ref);
            easy_disconn_cb(c, now);
          }
        }
        if (c->reconn_fail > 10)
        {
          c->auto_reconn = 0;
        }
      }
      return ret;
    }

    int ObTbnetCallback::on_connect(easy_connection_t *c)
    {
      int ret = EASY_OK;
      if (NULL == c)
      {
        YYSYS_LOG(WARN, "invalid argument c is %p", c);
        ret = EASY_ERROR;
      }
      else
      {
        if (easy_socket_set_opt(c->fd, SO_KEEPALIVE, 1) < 0){
          YYSYS_LOG(WARN, "set SO_KEEPALIVE to %s failure: %s (%d)\n", easy_connection_str(c), strerror(errno), errno);
        }
      }
      return ret;
    }

    int ObTbnetCallback::batch_process(easy_message_t *m)
    {
      UNUSED(m);
      return EASY_OK;
    }

    uint64_t ObTbnetCallback::get_packet_id(easy_connection_t *c, void *packet)
    {
      UNUSED(c);
      return ((ObPacket*)packet)->get_channel_id();
    }

    int ObTbnetCallback::default_callback(easy_request_t* r)
    {
      int ret = EASY_OK;
      if (NULL == r || NULL == r->ms)
      {
        YYSYS_LOG(WARN, "request is null or r->ms is null");
      }
      else
      {
        easy_session_destroy(r->ms);
      }
      return ret;
    }

    int ObTbnetCallback::clean_up(easy_request_t *r, void *apacket)
    {
        int ret = EASY_OK;
        UNUSED(apacket);
        if(NULL == r || NULL == r->ms)
        {
            YYSYS_LOG(WARN, "request is null or r->ms is null");
        }
        else
        {
            ObPacket *packet = (ObPacket *)r->ipacket;
            if(packet != NULL){
                packet->set_cleanup();
            }
        }
        return ret;
    }

    int ObTbnetCallback::shadow_process(easy_request_t* r)
    {
      int ret = EASY_OK;
      if (NULL == r || NULL == r->ipacket)
      {
        YYSYS_LOG(ERROR, "request is empty, r = %p, r->ipacket = %p", r, r->ipacket);
        ret = EASY_BREAK;
      }
      else
      {
        ObShadowServer *server = (ObShadowServer*)r->ms->c->handler->user_data;
        ObPacket *req = (ObPacket*) r->ipacket;
        req->set_request(r);
        ret = server->handlePacket(req);
        if (OB_SUCCESS == ret)
        {
          r->ms->c->pool->ref++;
          easy_atomic_inc(&r->ms->pool->ref);
          easy_pool_set_lock(r->ms->pool);
          ret = EASY_AGAIN;
        }
        else
        {
          ret = EASY_OK;
          YYSYS_LOG(WARN, "can not push packet(src is %s, pcode is %u) to packet queue",
                    inet_ntoa_r(r->ms->c->addr), req->get_packet_code());
        }
      }
      return ret;
    }
  }
}
