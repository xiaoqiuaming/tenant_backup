#include "dumprt.h"
#include <stdio.h>
#include <stdint.h>
#include <getopt.h>
#include <dirent.h>
#include <string.h>
#include <stdlib.h>
#include <cstdio>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>

using namespace oceanbase::roottable;

void __attribute__((constructor(101))) init_log_file()
{
  YYSYS_LOGGER.setLogLevel("INFO");
  YYSYS_LOGGER.setFileName("dumprt.log", true);
  YYSYS_LOGGER.setMaxFileSize(256 * 1024L * 1024L);
}

DumpRootTable::DumpRootTable()
{
  pserver_ = NULL;
  if (NULL == (rt_tim_ = new(std::nothrow) ObTabletInfoManager()))
  {
    YYSYS_LOG(ERROR, "failed to init ObTabletInfoManager");
    fprintf(stderr, "Program exited abnormally, please refer to dumprt.log for details.\n");
  }
  else if (NULL == (rt_table_ = new(std::nothrow) ObRootTable2(rt_tim_)))
  {
    YYSYS_LOG(ERROR, "failed to init ObRootTable2");
    fprintf(stderr, "Program exited abnormally, please refer to dumprt.log for details.\n");
  }
}

DumpRootTable::~DumpRootTable()
{
  if (NULL != pserver_)
  {
    delete pserver_;
    pserver_ = NULL;
  }
  if (NULL != rt_table_)
  {
    delete rt_table_;
    rt_table_ = NULL;
  }
  if (NULL != rt_tim_)
  {
    delete rt_tim_;
    rt_tim_ = NULL;
  }
}

void DumpRootTable::usage()
{
  printf("\n");
  printf("Usage: bin/dumprt [OPTION]\n");
  printf("   -r| --rootserver_ip, default value is 127.0.0.1\n");
  printf("   -p| --rootserver_port, default value is 2500\n");
  printf("   -c| --cmd_type,command type:[dump_roottable|check_roottable]\n");
  printf("   -d| --cmd_content,command content:[tablet_stat|tablet_detail|cs_tablet_stat|check_integrity|check_lost|check_crc]\n");
  printf("       When cmd_type is set to dump_roottable, cmd_content should be tablet_stat|tablet_detail|cs_tablet_stat\n");
  printf("       When cmd_type is set to check_roottable, cmd_content should be check_integrity|check_lost|check_crc\n");
  printf("   -t| --table_id\n");
  printf("   -o| --output_format,output format:[plain|json|command]\n");
  printf("   -F| --roottable_directory,default is data/rs_commitlog\n");
  printf("   -n| --newest,send msg to rs to get new data, this option is mutually exclusive with roottable directory option\n");
  printf("   -h| --help,print the help info\n");
  printf("   samples:\n");
  printf("   bin/dumprt -c dump_roottable -d tablet_stat -o json -t 3001\n");
  printf("   bin/dumprt -c dump_roottable -d tablet_stat -o json\n");
  printf("   bin/dumprt -c dump_roottable -d tablet_stat -o plain\n");
  printf("   bin/dumprt -r 127.0.0.1 -p 2500 -c dump_roottable -d tablet_stat -o plain -n\n");
  printf("   bin/dumprt -c dump_roottable -d tablet_stat -o plain -F /home/admin/roottable\n");
  printf("\n");
}

void DumpRootTable::parse_cmd_line(int argc, char **argv)
{
  YYSYS_LOG(INFO, "parse command line:");
  YYSYS_LOG(INFO, "%s\n", argv[0]);
  for (int i = 1; i < argc; i++)
  {
    YYSYS_LOG(INFO, "%s\n", argv[i]);
  }

  int opt = 0;
  const char *opt_string = "r:p:c:d:t:o:F:nh";
  struct option longopts[] = {{"rootserver_ip", 1, NULL, 'r'}, {"rootserver_port", 1, NULL, 'p'},
   {"cmd_type", 1, NULL, 'c'},{"cmd_content", 1, NULL, 'd'},{"table_id", 1, NULL, 't'},
   {"output_format", 1, NULL, 'o'}, {"roottable_directory", 1, NULL, 'F'},
   {"newest", 0, NULL, 'n'}, {"help", 0, NULL, 'h'},{0, 0, 0, 0}};

  while ( -1 != (opt = getopt_long(argc, argv, opt_string, longopts, NULL)))
  {
    switch (opt)
    {
      case 'r':
        clp_.rootserver_ip_ = optarg;
        break;
      case 'p':
        clp_.rootserver_port_ = static_cast<int32_t>(strtoll(optarg, NULL, 10));
        break;
      case 'c':
        clp_.cmd_type_ = optarg;
        break;
      case 'd':
        clp_.cmd_content_ = optarg;
        break;
      case 't':
        clp_.table_id_ = static_cast<int64_t>(strtoll(optarg, NULL, 10));
        break;
      case 'o':
        clp_.output_format_ = optarg;
        break;
      case 'F':
        clp_.roottable_directory_ = optarg;
        break;
      case 'n':
        clp_.newest_ = true;
        break;
      case 'h':
        usage();
        exit(OB_SUCCESS);
        break;
      default:
        usage();
        exit(OB_ERROR);
    }
  }

  if (NULL == clp_.rootserver_ip_)
  {
    clp_.rootserver_ip_ = DumpRootTable::DEFAULT_IP;
  }
  if (0 == clp_.rootserver_port_)
  {
    clp_.rootserver_port_ = DEFAULT_PORT;
  }
  else if (DumpRootTable::MIN_PORT > clp_.rootserver_port_ || DumpRootTable::MAX_PORT < clp_.rootserver_port_)
  {
    fprintf(stderr, "rootserver_port not set correctly.\n");
    usage();
    exit(OB_ERROR);
  }

  if (NULL == clp_.cmd_type_ ||
     (0 != strcmp(clp_.cmd_type_, CMD_TYPE_DUMP) &&
      0 != strcmp(clp_.cmd_type_, CMD_TYPE_CHECK)))
  {
    fprintf(stderr, "cmd_type not set correctly.\n");
    usage();
    exit(OB_ERROR);
  }

  else if (0 == strcmp(clp_.cmd_type_, CMD_TYPE_DUMP))
  {
     if (0 != strcmp(clp_.cmd_content_, CMD_CONTENT_TABLET_DETAIL) &&
         0 != strcmp(clp_.cmd_content_, CMD_CONTENT_TABLET_STAT) &&
         0 != strcmp(clp_.cmd_content_, CMD_CONTENT_CS_TABLET_STAT))
     {
       fprintf(stderr, "cmd_type and cmd_content options not matched.\n");
       usage();
       exit(OB_ERROR);
     }
  }
  else if (0 == strcmp(clp_.cmd_type_, CMD_TYPE_CHECK))
  {
     if (0 != strcmp(clp_.cmd_content_, CMD_CONTENT_CHECK_INTEGRITY) &&
         0 != strcmp(clp_.cmd_content_, CMD_CONTENT_CHECK_LOST) &&
         0 != strcmp(clp_.cmd_content_, CMD_CONTENT_CHECK_CRC))
     {
       fprintf(stderr, "cmd_type and cmd_content options not matched.\n");
       usage();
       exit(OB_ERROR);
     }
  }

  if (NULL == clp_.output_format_)
  {
    clp_.output_format_ = OUTPUT_FORMAT_PLAIN;
  }
  else if (0 != strcmp(clp_.output_format_, OUTPUT_FORMAT_PLAIN) &&
           0 != strcmp(clp_.output_format_, OUTPUT_FORMAT_JSON) &&
           0 != strcmp(clp_.output_format_, OUTPUT_FORMAT_COMMAND))
  {
    fprintf(stderr, "output_format not set correctly.\n");
    usage();
    exit(OB_ERROR);
  }

  if (NULL != clp_.roottable_directory_ && true == clp_.newest_)
  {
    fprintf(stderr, "roottable directory and newest options are mutually exclusive.\n");
    usage();
    exit(OB_ERROR);
  }

  if (NULL == clp_.roottable_directory_)
  {
    clp_.roottable_directory_ = config_.commit_log_dir;
  }
}

int DumpRootTable::init()
{
  int ret = OB_SUCCESS;
  YYSYS_LOG(INFO, "init according to parameters");

  if (0 == strcmp(clp_.cmd_type_, CMD_TYPE_DUMP))
  {
    if (true == clp_.newest_)
    {
      YYSYS_LOG(INFO, "init root server and do check point");
      if (OB_SUCCESS != (ret = init_root_server()))
      {
        YYSYS_LOG(ERROR, "failed to init root server");
      }
      else if (OB_SUCCESS != (ret = do_check_point()))
      {
        YYSYS_LOG(ERROR, "failed to do check point");
      }
    }
    if (OB_SUCCESS == ret)
    {
      YYSYS_LOG(INFO, "init root table and cs info");
      if (OB_SUCCESS != (ret = init_root_table_info()))
      {
        YYSYS_LOG(ERROR, "failed to init root table info");
      }
      else if (OB_SUCCESS != (ret = init_cs_info()))
      {
        YYSYS_LOG(ERROR, "failed to init cs info");
      }
    }
  }
  else
  {
    YYSYS_LOG(ERROR, "Parameters are not supported");
    ret = OB_ERROR;
  }
  return ret;
}

int DumpRootTable::handle()
{
  int ret = OB_SUCCESS;

  YYSYS_LOG(INFO, "handle according to parameters");
  if (0 == strcmp(clp_.cmd_type_, CMD_TYPE_DUMP) &&
      0 == strcmp(clp_.cmd_content_, CMD_CONTENT_TABLET_STAT))
  {
    ret = handle_tablet_stat_by_table();
  }
  else
  {
    YYSYS_LOG(ERROR, "Parameters are not supported");
    ret = OB_ERROR;
  }
  return ret;
}

int DumpRootTable::init_root_server()
{
  int ret = OB_SUCCESS;

  YYSYS_LOG(INFO, "init root server");

  pserver_ = new(std::nothrow) ObServer(ObServer::IPV4, clp_.rootserver_ip_, clp_.rootserver_port_);
  if (NULL == pserver_)
  {
    YYSYS_LOG(ERROR, "failed to init ObServer");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  }
  else if (OB_SUCCESS != (ret = client_.initialize(*pserver_)))
  {
    YYSYS_LOG(ERROR, "failed to init client, err=%d", ret);
  }
  else
  {
    YYSYS_LOG(INFO, "client will connect to root server:[%s], timeout:%ld",
              pserver_->to_cstring(), DumpRootTable::DEFAULT_REQUEST_TIMEOUT_US);
  }
  return ret;
}

int DumpRootTable::init_root_table_info()
{
  int ret = OB_SUCCESS;

  YYSYS_LOG(INFO, "init root table info");

  const char *log_dir = clp_.roottable_directory_;
  char rt_filename[OB_MAX_FILE_NAME_LENGTH];
  int len = 0;
  len = snprintf(rt_filename, OB_MAX_FILE_NAME_LENGTH, "%s/%d.%s", log_dir, 0, ObRootServer2::ROOT_TABLE_EXT);
  if (0 > len || OB_MAX_FILE_NAME_LENGTH <= len)
  {
    ret = OB_BUF_NOT_ENOUGH;
    YYSYS_LOG(ERROR, "root table file name is too long, length=%d", len);
  }
  else
  {
    YYSYS_LOG(INFO, "root table file name is %s", rt_filename);
  }

  if (OB_SUCCESS == ret)
  {
    if (false == is_file_existed(rt_filename))
    {
      YYSYS_LOG(ERROR, "root table file %s is not existed", rt_filename);
      ret = OB_ERROR;
    }
    else
    {
      if (OB_SUCCESS != (ret = rt_table_->read_from_file(rt_filename)))
      {
        YYSYS_LOG(ERROR, "init read root table from file [%s] failed", rt_filename);
      }
      else
      {
        YYSYS_LOG(INFO, "init read root table from file [%s] successed", rt_filename);
      }
    }
  }
  return ret;
}

int DumpRootTable::init_cs_info()
{
  int ret = OB_SUCCESS;

  YYSYS_LOG(INFO, "init cs info");

  const char *log_dir = clp_.roottable_directory_;
  char clist_filename[OB_MAX_FILE_NAME_LENGTH];
  int len = 0;
  len = snprintf(clist_filename, OB_MAX_FILE_NAME_LENGTH, "%s/%d.%s", log_dir, 0, ObRootServer2::CHUNKSERVER_LIST_EXT);
  if (0 > len || OB_MAX_FILE_NAME_LENGTH <= len)
  {
    ret = OB_BUF_NOT_ENOUGH;
    YYSYS_LOG(ERROR, "cs file name is too long, length=%d", len);
  }
  else
  {
    YYSYS_LOG(INFO, "cs file name is %s", clist_filename);
  }

  if (OB_SUCCESS == ret)
  {
    if (false == is_file_existed(clist_filename))
    {
      YYSYS_LOG(ERROR, "cs file %s is not existed", clist_filename);
      ret = OB_ERROR;
    }
    else
    {
      int32_t cs_num = 0;
      int32_t ms_num = 0;
      ret = server_manager_.read_from_file(clist_filename, cs_num, ms_num);
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(ERROR, "init read cs from file [%s] failed", clist_filename);
      }
      else
      {
        YYSYS_LOG(INFO, "init read cs from file [%s] successed", clist_filename);
      }
    }
  }
  return ret;
}

int DumpRootTable::do_check_point()
{
  int ret = OB_SUCCESS;
  YYSYS_LOG(INFO, "send message to rs to generate 0,rtable and 0.clist");
  YYSYS_LOG(INFO, "do check_point, pcode=%d", OB_RS_ADMIN_CHECKPOINT);
  const int32_t MY_VERSION = 1;
  const int buff_size = static_cast<int>(sizeof(ObPacket)) + 32;
  char buff[buff_size];
  ObDataBuffer msgbuf(buff, buff_size);

  if (OB_SUCCESS != (ret = serialization::encode_vi32(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position(), OB_RS_ADMIN_CHECKPOINT)))
  {
     YYSYS_LOG(ERROR, "failed to serialize, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = client_.send_recv(OB_RS_ADMIN, MY_VERSION, DumpRootTable::DEFAULT_REQUEST_TIMEOUT_US, msgbuf)))
  {
    YYSYS_LOG(ERROR, "failed to send request, err=%d, timeout=%ld", ret, DumpRootTable::DEFAULT_REQUEST_TIMEOUT_US);
  }
  else
  {
    ObResultCode result_code;
    msgbuf.get_position() = 0;
    if (OB_SUCCESS != (ret = result_code.deserialize(msgbuf.get_data(), msgbuf.get_capacity(), msgbuf.get_position())))
    {
       YYSYS_LOG(ERROR, "failed to deserialize response, err=%d", ret);
    }
    else if (OB_SUCCESS != (ret = result_code.result_code_))
    {
      YYSYS_LOG(ERROR, "failed to do_check_point, err=%d", result_code.result_code_);
    }
    else
    {
      YYSYS_LOG(INFO, "Okay");
    }
  }
  return ret;
}

int DumpRootTable::handle_tablet_stat_by_table()
{
  int ret = OB_SUCCESS;
  YYSYS_LOG(INFO, "handle tablet stat by table");

  int64_t len = 0;
  struct TabletStatByTable *temp_tablet_stat = NULL;
  TabletStatByTable *tablet_stat = static_cast<TabletStatByTable *>(ob_malloc(sizeof(struct TabletStatByTable)*common::OB_MAX_TABLE_NUMBER, ObModIds::OB_RS_ROOT_TABLE));
  if (NULL == tablet_stat)
  {
    ret = OB_ERROR;
    YYSYS_LOG(ERROR, "failed to init TabletStatByTable");
  }
  else
  {
    memset(tablet_stat, 0, sizeof(struct TabletStatByTable) * common::OB_MAX_TABLE_NUMBER);
    ObRootTable2::const_iterator it = rt_table_->begin();
    ObTabletInfo *tablet = NULL;
    ObNewRange *range = NULL;
    int64_t tablet_size = 0;
    int64_t table_id = 0;
    temp_tablet_stat = NULL;
    bool found = false;
    int32_t i = 0;
    int32_t j = 0;
    for (; it != rt_table_->end(); it++)
    {
      tablet = rt_tim_->get_tablet_info(it->tablet_info_index_);
      range = &(tablet->range_);
      tablet_size = tablet->occupy_size_;
      table_id = range->table_id_;
      found = false;
      for (i = 0; i < len; i++)
      {
        if (table_id == tablet_stat[i].table_id_)
        {
          found = true;
          break;
        }
      }
      if (false == found)
      {
        temp_tablet_stat = new(tablet_stat + len) TabletStatByTable();
        if (NULL == temp_tablet_stat)
        {
          ret = OB_ERROR;
          break;
        }
        len++;
        temp_tablet_stat->table_id_ = table_id;
      }
      for (j = 0; j < OB_MAX_COPY_COUNT; j++)
      {
        if ( -1 != it->server_info_indexes_[j])
        {
          temp_tablet_stat->tablet_num_per_cs_[it->server_info_indexes_[j]]++;
          temp_tablet_stat->tablet_size_per_cs_[it->server_info_indexes_[j]] += tablet_size;
        }
      }
    }
    if (OB_SUCCESS == ret)
    {
      print_tablet_stat_by_table(tablet_stat, len);
    }
  }
  if (NULL != tablet_stat)
  {
    for (int64_t i = 0; i < len; i++)
    {
      temp_tablet_stat = static_cast<struct TabletStatByTable *>(tablet_stat + i *sizeof(struct TabletStatByTable));
      temp_tablet_stat->~TabletStatByTable();
      temp_tablet_stat = NULL;
    }
    ob_free(tablet_stat);
    tablet_stat = NULL;
  }
  return ret;
}

int DumpRootTable::print_tablet_stat_by_table(const TabletStatByTable *tablet_stat, int64_t len)
{
  int ret = OB_SUCCESS;

  int32_t i = 0;
  int32_t j = 0;
  int64_t max_int64 = (__UINT64_C(1) << 63) - 1;
  int64_t max_int64_len = 0;
  while (0 < max_int64)
  {
    max_int64_len++;
    max_int64 /= 10;
  }
  char *formatted_size = static_cast<char *>(ob_malloc(max_int64_len + 5, ObModIds::OB_RS_ROOT_TABLE));
  if (NULL == formatted_size)
  {
    ret = OB_ERROR;
    YYSYS_LOG(ERROR, "failed to init formatted size");
  }
  else if (0 == strcmp(clp_.output_format_, OUTPUT_FORMAT_PLAIN))
  {
    char ip[16];
    char seperator = ' ';
    for (i = 0; i< len; i++)
    {
      if (0 != clp_.table_id_)
      {
        if (tablet_stat[i].table_id_ != clp_.table_id_)
        {
          continue;
        }
      }
      printf("table_id:%ld\n", tablet_stat[i].table_id_);
      printf("tablet info on cs:\n");
      ObChunkServerManager::const_iterator it = server_manager_.begin();
      for (j = 0; it < server_manager_.end(); it++, j++)
      {
        if (it->status_ == ObServerStatus::STATUS_DEAD)
        {
          continue;
        }
        memset(ip, 0, 16);
        it->server_.ip_to_string(ip, 16);
        memset(formatted_size, 0, max_int64_len + 5);
        ret = format_size(tablet_stat[i].tablet_size_per_cs_[j], formatted_size, max_int64_len + 5);
        if (OB_SUCCESS == ret)
        {
          printf("cs:%s%ctablet_num:%ld%coccupied_size:%s\n", ip, seperator, tablet_stat[i].tablet_num_per_cs_[j], seperator, formatted_size);
        }
        else
        {
          YYSYS_LOG(WARN, "failed to format size");
          printf("cs:%s%ctablet_num:%ld%coccupied_size:%s\n", ip, seperator, tablet_stat[i].tablet_num_per_cs_[j], seperator, "error");
        }
      }
      printf("\n");
    }
    printf("tablet information printed over!\n");
  }
  else if (0 == strcmp(clp_.output_format_, OUTPUT_FORMAT_JSON))
  {
    printf("[");
    char ip[16];
    for (i = 0; i< len; i++)
    {
      if (0 != clp_.table_id_)
      {
        if (tablet_stat[i].table_id_ != clp_.table_id_)
        {
          continue;
        }
      }
      ObChunkServerManager::const_iterator it = server_manager_.begin();
      printf("{\"table_id\":%ld,\"data\":[", tablet_stat[i].table_id_);
      int32_t non_dead_cs_num = 0;
      for (j = 0; it < server_manager_.end(); it++, j++)
      {
        if (it->status_ == ObServerStatus::STATUS_DEAD)
        {
          continue;
        }
        non_dead_cs_num++;
        if (1 != non_dead_cs_num)
        {
          printf(",");
        }
        memset(ip, 0, 16);
        it->server_.ip_to_string(ip, 16);
        memset(formatted_size, 0, max_int64_len + 5);
        ret = format_size(tablet_stat[i].tablet_size_per_cs_[j], formatted_size, max_int64_len + 5);
        if (OB_SUCCESS == ret)
        {
          printf("{\"cs\":\"%s\",\"tablet_num\":%ld,\"occupied_size\":\"%s\"}", ip, tablet_stat[i].tablet_num_per_cs_[j], formatted_size);
        }
        else
        {
          YYSYS_LOG(WARN, "failed to format size");
          printf("{\"cs\":\"%s\",\"tablet_num\":%ld,\"occupied_size\":\"%s\"}", ip, tablet_stat[i].tablet_num_per_cs_[j], "error");
        }
      }
      printf("]}");
      if (0 == clp_.table_id_)
      {
        if (i < (len - 1))
        {
          printf(",");
        }
      }
    }
    printf("]\n");
  }
  else
  {
    printf("not supported\n");
  }

  if (NULL != formatted_size)
  {
    ob_free(formatted_size);
    formatted_size = NULL;
  }

  return ret;
}

bool DumpRootTable::is_file_existed(const char *filename)
{
  bool ret = false;
  FILE *fp = NULL;
  struct stat buf;
  memset(&buf, 0, sizeof(struct stat));
  fp = fopen(filename, "r");
  if (NULL != fp)
  {
    ret = true;
    fclose(fp);
  }
  return ret;
}

int DumpRootTable::format_size(int64_t raw_size, char *formatted_size, int64_t max_formatted_size_length)
{
  int ret = OB_SUCCESS;
  const int64_t KB = 1024;
  const int64_t MB = KB * 1024;
  const int64_t GB = MB * 1024;
  const int64_t TB = GB * 1024;
  double devided_size = 0.0;
  int len = 0;

  if(TB <= raw_size)
  {
    devided_size = static_cast<double>(raw_size) / TB;
    len = snprintf(formatted_size, max_formatted_size_length, "%.1fT", devided_size);
  }
  else if(GB <= raw_size)
  {
    devided_size = static_cast<double>(raw_size) / GB;
    len = snprintf(formatted_size, max_formatted_size_length, "%.1fG", devided_size);
  }
  else if(MB <= raw_size)
  {
    devided_size = static_cast<double>(raw_size) / MB;
    len = snprintf(formatted_size, max_formatted_size_length, "%.1fM", devided_size);
  }
  else if(KB <= raw_size)
  {
    devided_size = static_cast<double>(raw_size) / KB;
    len = snprintf(formatted_size, max_formatted_size_length, "%.1fK", devided_size);
  }
  else
  {
    devided_size = static_cast<double>(raw_size);
    len = snprintf(formatted_size, max_formatted_size_length, "%.1fB", devided_size);
  }
  if ( 0 > len || max_formatted_size_length <= len)
  {
    ret = OB_BUF_NOT_ENOUGH;
    YYSYS_LOG(ERROR, "formatted size is too long, length=%d", len);
  }
  return ret;
}

int main(const int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  ob_init_memory_pool();

  DumpRootTable drt;
  drt.parse_cmd_line(argc, argv);
  ret = drt.init();
  if (OB_SUCCESS == ret)
  {
    ret = drt.handle();
  }

  if (OB_SUCCESS != ret)
  {
    fprintf(stderr, "Program exited abnormally, please refer to dumprt.log for details.\n");
  }

  return ret;
}
