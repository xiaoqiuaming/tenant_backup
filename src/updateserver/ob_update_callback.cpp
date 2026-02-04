#include "ob_update_callback.h"
#include "yylog.h"
#include "ob_update_server.h"
#include "common/ob_packet.h"

using namespace oceanbase::common;

namespace oceanbase
{
  namespace updateserver
  {     
    int ObUpdateCallback::process(easy_request_t* r)
    {
      int ret = EASY_OK;
      if (NULL == r)
      {
        YYSYS_LOG(ERROR, "request is empty, r = %p", r);
        ret = EASY_BREAK;
      }
      else if (NULL == r->ipacket)
      {
        YYSYS_LOG(ERROR, "request is empty, r->ipacket = %p", r->ipacket);
        ret = EASY_BREAK;
      }
      else
      {
        ObUpdateServer *server = reinterpret_cast<ObUpdateServer*>(r->ms->c->handler->user_data);
        ObPacket *req = reinterpret_cast<ObPacket*>(r->ipacket);
        req->set_request(r);
        r->ms->c->pool->ref ++;
        easy_atomic_inc(&r->ms->pool->ref);
        easy_pool_set_lock(r->ms->pool);
        ret = server->handlePacket(req);
        if (OB_SUCCESS == ret)
        {
          // enqueue success
          ret = EASY_AGAIN;
        }
        else if (OB_ENQUEUE_FAILED == ret)
        {
          YYSYS_LOG(WARN, "can not push packet(src is %s, pcode is %u) to packet queue", 
                    inet_ntoa_r(r->ms->c->addr), req->get_packet_code());
          r->ms->c->pool->ref --;
          easy_atomic_dec(&r->ms->pool->ref);
          ret = EASY_OK;
        }
        else /* OB_ERROR */
        {
          ret = EASY_AGAIN;
        }
      }
      return ret;
    }
  }
}
