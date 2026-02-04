#include "base_server.h"

using namespace oceanbase::common;
using namespace oceanbase::tools;

int BaseServer::initialize()
{
  int ret = client_manager_.initialize(eio_, &server_handler_);
  if (ret != OB_SUCCESS)
  {
    YYSYS_LOG(WARN, "init client manager failed:ret[%d]", ret);
  }
  else
  {
    YYSYS_LOG(INFO, "init client manager succ");
  }
  return ret;
}

