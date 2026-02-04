#include <getopt.h>
#include <string.h>

#include "ob_client.h"
#include "common/ob_server.h"

using namespace oceanbase::tools;
using namespace oceanbase::common;

void print_usage()
{
  fprintf(stderr, "ob_data [OPTION]\n");
  fprintf(stderr, "   -h|--host ob server addr\n");
  fprintf(stderr, "   -p|--port ob server port\n");
  fprintf(stderr, "   -t|--table db table name\n");
}

class ClientRunner : public yysys::Runnable
{
  public:
    ClientRunner(ObClient & client, int scan_count, ObString & table_name) : client_(client), scan_count_(scan_count), table_name_(table_name)
  {

  }
    virtual void run(yysys::CThread *thread, void *arg)
    {
      ObRange range;
      range.border_flag_.set_min_value();
      range.border_flag_.set_max_value();
      for (int i = 0 ;i < scan_count_ ; ++i)
      {
        int ret = OB_SUCCESS;
        ObScanner ob_scanner;
        int64_t startTime = yysys::CTimeUtil::getTime();
        ret = client_.scan(table_name_, range, ob_scanner);
        if (ret != OB_SUCCESS)
        {
          YYSYS_LOG(WARN, "scan table[%s] FAILED, ret[%d]", table_name_.ptr(), ret);
        }
        else
        {
          int64_t endTime = yysys::CTimeUtil::getTime();
          YYSYS_LOG(INFO, "used time =%ld", endTime - startTime);
          //YYSYS_LOG(INFO, "scan table[%s] SUCCESS", table_name.ptr());
        }
      }

    }
  private:
    ObClient & client_;
    int scan_count_;
    ObString & table_name_;

};
int main(int argc, char ** argv)
{
  /*
  if (argc < 7)
  {
    print_usage();
    exit(1);
  }
  */
  int ret = OB_SUCCESS;
  const char * opt_string = "h:p:t:c:m:n:a:";
  struct option longopts[] =
  {
    {"host", 1, NULL, 'h'},
    {"port", 1, NULL, 'p'},
    {"table", 1, NULL, 't'},
    {"command", 1, NULL, 'c'},
    {"mergeserver", 1, NULL, 'm'},
    {"scancount", 1, NULL, 'n'},
    {"threadcount", 1, NULL, 'a'},
    {0, 0, 0, 0}
  };

  int opt = 0;
  int number = 1;
  const char * hostname = NULL;
  int32_t port = 0;
  ObString table_name;
  char * tablename = NULL;
  char * command = NULL;
  char * mergeserver = NULL;
  int threadcount = 1;
  while ((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1)
  {
    switch (opt)
    {
      case 'h':
        hostname = optarg;
        break;
      case 'p':
        port = atoi(optarg);
        break;
      case 't':
        tablename = optarg;
        table_name.assign(tablename, strlen(tablename));
        break;
      case 'c':
        command = optarg;
        break;
      case 'm':
        mergeserver = optarg;
        break;
      case 'n':
        number = atoi(optarg);
        break;
      case 'a':
        threadcount = atoi(optarg);
        break;
      default:
        print_usage();
        exit(1);
    }
  }
  ObServer root_server;
  root_server.set_ipv4_addr(hostname, port);
  ob_init_memory_pool();
  YYSYS_LOGGER.setLogLevel("INFO");
  if (!strncasecmp(command, "delete", 6))
  {
    ObClient client(root_server);
    ret = client.prepare();
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(ERROR, "client initialization failed, ret = %d", ret);
      exit(1);
    }
    int64_t count = 0;
    ObRange range;
    range.border_flag_.set_min_value();
    range.border_flag_.set_max_value();
    ret = client.del(table_name , range, count);
    if (ret != OB_SUCCESS)
    {
      YYSYS_LOG(WARN, "delete table[%s] FAILED, ret[%d]", table_name.ptr(), ret);
    }
    else
    {
      YYSYS_LOG(INFO, "delete table[%s] SUCCESS, total delete row count = %ld", table_name.ptr(), count);
    }
  }
  else if(!strncasecmp(command , "scan", 4))
  {
    YYSYS_LOGGER.setFileName("client.log");
    ObClient *clients = (ObClient*)malloc(sizeof(ObClient) * threadcount);
    yysys::CThread *threads = (yysys::CThread*)malloc(sizeof(yysys::CThread) * threadcount);
    ClientRunner *runners = (ClientRunner*)malloc(sizeof(ClientRunner) * threadcount);
    ObServer ms;
    char *index = strstr(mergeserver, ":");
    int port = atoi(index + 1);
    mergeserver[index - mergeserver] = '\0';
    ms.set_ipv4_addr(mergeserver, port);
    YYSYS_LOG(INFO, "%s", ms.to_cstring());
    for (int i = 0 ;i < threadcount ; ++i)
    {
      new (clients + i) ObClient(root_server);
      clients[i].setMergeserver(ms);
      new (runners + i) ClientRunner(clients[i], number, table_name);
      threads[i].start(runners + i, NULL);
    }
    for (int i = 0 ;i < threadcount ; ++i)
    {
      threads[i].join();
    }
    free(clients);
    free(threads);
    free(runners);

  }
  return ret;
}
