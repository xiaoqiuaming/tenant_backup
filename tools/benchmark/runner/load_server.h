#ifndef LOAD_SERVER_H_
#define LOAD_SERVER_H_

#include "yysys.h"
#include "yynet.h"

#include "common/thread_buffer.h"
#include "common/ob_client_manager.h"
#include "common/ob_packet_factory.h"
#include "common/ob_base_server.h"
#include "common/ob_single_server.h"

namespace oceanbase
{
  namespace tools 
  {
    class LoadServer : public common::ObSingleServer
    {
      public:
        /// init
        virtual int initialize();
        /// task dispatch
        virtual int do_request(common::ObPacket * base_packet) = 0;
        /// client manager
        common::ObClientManager * get_client(void)
        {
          return &client_manager_;
        }
        /// rpc buffer
        common::ThreadSpecificBuffer * get_buffer(void)
        {
          return &rpc_buffer_;
        }
      protected:
        common::ThreadSpecificBuffer rpc_buffer_;
      
      private:
        common::ObPacketFactory factory_;
        common::ObClientManager client_manager_;
    };

    class LoadServerRunner : public yysys::Runnable
    {
      public:
        LoadServerRunner(LoadServer & server) : server_(server)
        {
        }

        virtual ~LoadServerRunner()
        {
          server_.stop();
        }

        virtual void run(yysys::CThread *thread, void *arg)
        {
          int ret = server_.start();
          if (ret != common::OB_SUCCESS)
          {
            YYSYS_LOG(ERROR, "server start failed:ret[%d]", ret);
          }
        }

      private:
        LoadServer & server_;
    };
  }
}


#endif // LOAD_SERVER_H_
