#include "cbrecovery.h"
#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include "common/ob_obj_cast.h"

using namespace oceanbase::common;

class CmdArgsParser
{
  const static int64_t MAX_N_ARGS = 1 << 10;
  struct arg_t
  {
    arg_t(): name_(NULL), value_(null_), default_value_(NULL){}
    ~arg_t(){}
    const char *name_;
    const char *value_;
    const char *default_value_;
  };
  public:
  CmdArgsParser(): parse_seq_(0), n_args_(0){
    default_arg_.name_ = "*default*";
    default_arg_.value_ = null_;
    default_arg_.default_value_ = null_;
  }
  ~CmdArgsParser() {}
  bool reset()
  {
    memset(args_, 0, sizeof(args_));
    n_args_ = 0;
    parse_seq_ |= 1;
    return true;
  }

  bool check(int argc, char** argv, ...)
  {
    bool args_is_valid = true;
    char *p = NULL;
    parse_seq_ = (parse_seq_&~1) + 2;

    for (int64_t i = 0; i < n_args_/2; i++)
    {
      arg_t arg = args_[i];
      args_[i] = args_[n_args_ - 1 - i];
      args_[n_args_ - 1 - i] = arg;
    }

    for (int64_t i = 0; i < argc; i++)
    {
      if (argv[i][0] == ':' || NULL == (p == strchr(argv[i], '=')))continue;
      *p++ = 0;
      arg_t *arg = get_arg(argv[i]);
      if (arg && &default_arg_ != arg) arg->value_ = p;
      *--p = '=';
    }

    for (int64_t i = 0; i < argc; i++)
    {
      if (argv[i][0] != ':' && (p = strchr(argv[i], '=')))continue;
      p = argv[i][0] == ':' ? argv[i] + 1 : argv[i];
      arg_t *arg = get_next_unset_arg();
      if (arg && arg->name_) arg->value_ = p;
    }

    for (int64_t i = 0; i < n_args_; i++)
    {
      if (null_ == args_[i].value_ && args_[i].default_value_)
        args_[i].value_ = args_[i].default_value_;
      if (null_ == args_[i].value_)args_is_valid = false;
    }
    if (0 == strcmp("true", getenv("dump_args") ? : "false"))
    {
      dump(argc, argv);
    }
    return args_is_valid;
  }

  void dump(int argc, char **argv)
  {
    printf("cmd_args_parser.dump:\n");
    for (int64_t i = 0; i < argc; i++)
    {
      printf("argv[%ld]=%s\n", i, argv[i]);
    }
    for (int64_t i = 0; i < n_args_; i++)
    {
      printf("args[%ld]={name=%s, value=%s, default=%s}\n",
             i, args_[i].name_, args_[i].value_, args_[i].default_value_);
    }
  }

  arg_t *get_next_unset_arg()
  {
    for (int64_t i = 0; i < n_args_; i++)
    {
      if (null_ == args_[i].value_)
      {
        return args_ + i;
      }
    }
    return NULL;
  }

  arg_t *get_arg(const char *name, const char *default_value = NULL)
  {
    assert(n_args_ < MAX_N_ARGS && name);
    if (parse_seq_&1)
    {
      args_[n_args_].name_ = name;
      args_[n_args_].default_value_ = default_value;
      args_[n_args_].value_ = null_;
      return args_ + (n_args_++);
    }
    for (int64_t i = 0; i < n_args_; i++)
    {
      if (0 == strcmp(args_[i].name_, name))
        return args_ + i;
    }
    return &default_arg_;
  }
  private:
  static const char* null_;
  int64_t parse_seq_;
  int64_t n_args_;
  arg_t default_arg_;
  arg_t args_[MAX_N_ARGS];
};

bool argv1_match_func(const char* argv1, const char* func)
{
  const char *last_part = strrchr(func, '.');
  if (NULL != last_part)
  {
    last_part++;
  }
  else
  {
    last_part = func;
  }
  return 0 == strcmp(last_part, argv1);
}

const char* CmdArgsParser::null_ = "*null*";
CmdArgsParser __cmd_args_parser;
#define _Arg(name, ...) __cmd_args_parser.get_arg(#name,  ##__VA_ARGS__)
#define IntArg(name, ...) atoll(__cmd_args_parser.get_arg(#name,  ##__VA_ARGS__)->value_)
#define StrArg(name, ...) __cmd_args_parser.get_arg(#name,  ##__VA_ARGS__)->value_
#define CmdCall_Without_func(argc, argv, agent, ...) \
  (argc >= 2 && __cmd_args_parser.reset() && __cmd_args_parser.check(argc - 1, argv + 1, ##__VA_ARGS__)) ? agent.start_client(__VA_ARGS__)
#define report_error(err, ...) if (OB_SUCCESS != err)YYSYS_LOG(ERROR, __VA_ARGS__);

void __attribute__((constructor(101))) init_log_file()
{
  YYSYS_LOGGER.setLogLevel("INFO");
  YYSYS_LOGGER.setFileName("dr_client.log", true);
  YYSYS_LOGGER.setMaxFileSize(256 * 1024L * 1024L);
}

int parse_time(const char *checkpoint_time1, int64_t &checktime)
{
  char *checkpoint_time = const_cast<char *>(checkpoint_time1);
  ObExprObj casted_obj;
  ObExprObj out;
  ObString tmp_str;
  ObObjCastParams params;
  int ret = OB_SUCCESS;
  tmp_str.assign_ptr(checkpoint_time, static_cast<int32_t>(strlen(checkpoint_time)));
  casted_obj.set_varchar(tmp_str);
  if (OB_SUCCESS != (ret = OB_OBJ_CAST[ObVarcharType][ObPreciseDateTimeType](params, casted_obj, out)))
  {
    YYSYS_LOG(ERROR, "fail to convert to timestamp, maybe format or content is incorrect, ret=%d", ret);
  }
  else
  {
    checktime = out.get_precise_datetime();
    YYSYS_LOG(INFO, "checktime:[%ld]", checktime);
  }
  return ret;
}

int realtime(int64_t checktime, char *checkpoint_time1)
{
  char *date_user_temp = (char *) malloc(sizeof(char) * 40);
  int ret = OB_SUCCESS;
  ObString obstr;
  ObExprObj casted_obj;
  ObExprObj out;
  ObString tmp_str;
  ObObjCastParams params;

  obstr.assign_ptr(date_user_temp, 40);
  out.set_varchar(obstr);
  casted_obj.set_precise_datetime(checktime);
  if (OB_SUCCESS != (ret = OB_OBJ_CAST[ObPreciseDateTimeType][ObVarcharType](params, casted_obj, out)))
  {
    YYSYS_LOG(ERROR, "fail to convert to real time, maybe format or content is incorrect, ret=%d", ret);
  }
  else
  {
    tmp_str = out.get_varchar();
    sprintf(checkpoint_time1, "%.*s", tmp_str.length(), tmp_str.ptr());
    YYSYS_LOG(INFO, "checktime:%s", checkpoint_time1);
  }
  return ret;
}

int parse_file(const char *src_file, IpPortmap& ip_port, BreakPoint& ruined_map, int& cnt, bool &seq_id)
{
  int ret = OB_SUCCESS;
  FILE *fp = NULL;
  if (NULL == (fp = fopen(src_file, "r")))
  {
    YYSYS_LOG(ERROR, "open conf file:%s fail", src_file);
    ret = OB_ERROR;
  }
  else
  {
    char log_info[100] = {0};
    bool first_parse = true;
    int64_t ruined_point = 0;
    int dest_port = 0;
    while(OB_SUCCESS == ret && NULL != fgets(log_info, 120, fp))
    {
      char log_ip[30] = {0}, recovery_point[30] = {0};
      sscanf(log_info, "%[^:]:%d   %[^\n]", log_ip, &dest_port, recovery_point);
      if (0 == strcmp(recovery_point, ""))
      {
        ruined_point = 0;
        YYSYS_LOG(INFO, "ruined_point = %ld", ruined_point);
      }
      else
      {
        YYSYS_LOG(INFO, "need to recover");
        if (first_parse)
        {
          seq_id = is_all_digit(recovery_point);
          first_parse = false;
        }
        if (seq_id)
        {
          sscanf(recovery_point, "%ld", &ruined_point);
          YYSYS_LOG(INFO, "input is a sequence id[%ld]", ruined_point);
        }
        else
        {
          YYSYS_LOG(INFO, "timestamp:[%s]", recovery_point);
          if (OB_SUCCESS != (ret = parse_time(recovery_point, ruined_point)))
          {
            YYSYS_LOG(ERROR, "fail to parse_time, ret = %d", ret);
          }
        }
      }
      if (OB_SUCCESS == ret)
      {
        if (-1 == (ret = ruined_map.set(string(log_ip), ruined_point)))
        {
          ret = OB_ERROR;
          YYSYS_LOG(ERROR, "set log_ip:%s to map fail, ret=%d", log_ip, ret);
        }
        else if (HASH_EXIST == ret)
        {
          YYSYS_LOG(ERROR, "log_ip:%s duplicate, use the first one", log_ip);
          ret = OB_SUCCESS;
        }
        else if (-1 == (ret = ip_port.set(string(log_ip), dest_port)))
        {
          ret = OB_ERROR;
          YYSYS_LOG(ERROR, "set log_ip:%s's port to map fail, ret=%d", log_ip, ret);
        }
        else
        {
          cnt++;
          ret = OB_SUCCESS;
        }
      }
    }
  }
  return ret;
}

int Agent::start_client(const char *src_file)
{
  int ret = OB_SUCCESS;
  int ip_cnt = 0;

  BreakPoint ip_seq_map;
  IpPortmap ip_port_map;
  TransSet ruined_dis_trans_set;
  BreakPoint dr_time_map;
  bool seq_id = true;

  if (OB_SUCCESS != (ret = ip_seq_map.create(hash::cal_next_prime(MAX_UPS_COUNT_ONE_CLUSTER))))
  {
    YYSYS_LOG(ERROR, "create ip_seq_map failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = ip_port_map.create(hash::cal_next_prime(MAX_UPS_COUNT_ONE_CLUSTER))))
  {
    YYSYS_LOG(ERROR, "create ip_port_map failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = ruined_dis_trans_set.create(hash::cal_next_prime(TRANS_BUCKET))))
  {
    YYSYS_LOG(ERROR, "create ruined_dis_trans_set failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = dr_time_map.create(hash::cal_next_prime(MAX_UPS_COUNT_ONE_CLUSTER))))
  {
    YYSYS_LOG(ERROR, "create dr_time_map failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = parse_file(src_file, ip_port_map, ip_seq_map, ip_cnt, seq_id)))
  {
    YYSYS_LOG(ERROR, "parse_file()=>%d", ret);
  }
  else
  {
    int* server_sock = new int[ip_cnt];
    int i = 0;
    IpPortmap::iterator ip_port = ip_port_map.begin();
    while (OB_SUCCESS == ret && i < ip_cnt)
    {
      struct sockaddr_in server_addr;
      bzero(&server_addr, sizeof(server_addr));
      server_addr.sin_family = AF_INET;
      server_addr.sin_addr.s_addr = inet_addr(ip_port->first.c_str());
      server_addr.sin_port = htons(ip_port->second);

      server_sock[i] = socket(AF_INET, SOCK_STREAM, 0);
      int cur_sock = server_sock[i];
      if ( -1 == cur_sock)
      {
        YYSYS_LOG(ERROR, "socket fail, errno:%d-%s", errno, strerror(errno));
        ret = errno;
      }
      if (connect(cur_sock, (struct sockaddr *)&server_addr, sizeof(server_addr)) == -1)
      {
        YYSYS_LOG(ERROR, "connect fail, errno:%d-%s", errno, strerror(errno));
        ret = errno;
      }
      i++;
      ip_port++;
    }
    YYSYS_LOG(INFO, "before init, ruined_dis_trans_set [%ld]", ruined_dis_trans_set.size());
    int32_t s_no = 1;

    BreakPoint::iterator ip_seq = ip_seq_map.begin();
    for(i = 0; OB_SUCCESS == ret && i < ip_cnt; i++)
    {
      PACKET packet_send;
      YYSYS_LOG(INFO, "seq_id: [%d]", seq_id);
      if (seq_id)
      {
        packet_send.seq = ip_seq->second;
        packet_send.mutate_timestamp = 0;
      }
      else
      {
        packet_send.mutate_timestamp = ip_seq->second;
        packet_send.seq = 0;
      }
      packet_send.sno = s_no;
      packet_send.ts = &ruined_dis_trans_set;
      packet_send.ts_size = ruined_dis_trans_set.size();
      packet_send.type = TRANS_SET_INIT;
      YYSYS_LOG(INFO, "send packet:(sno=%d, type=%d, mutate_timestamp=%ld, seq=%ld, ts_size=%ld)",
                packet_send.sno, packet_send.type, packet_send.mutate_timestamp, packet_send.seq,
                packet_send.ts_size);
      PACKET packet_receive;
      packet_receive.ts = &ruined_dis_trans_set;

      int cur_sock = server_sock[i];
      if (OB_SUCCESS != (ret = send_packet(cur_sock, packet_send)))
      {
        YYSYS_LOG(ERROR, "send schema result response error");
      }
      else if (OB_SUCCESS != (ret = receive_packet(cur_sock, packet_receive)))
      {
        YYSYS_LOG(ERROR, "receive packet error");
      }
      else if (OB_SUCCESS == packet_receive.type)
      {
        YYSYS_LOG(INFO, "receive packet:(sno=%d, type=%d, mutate_timestamp=%ld, seq=%ld, ts_size=%ld)",
                  packet_receive.sno, packet_receive.type, packet_receive.mutate_timestamp, packet_receive.seq,
                  packet_receive.ts_size);
        if (seq_id)
        {
          if (packet_receive.seq < ip_seq->second)
          {
            ip_seq_map.set(ip_seq->first, packet_receive.seq, 1);
          }
        }
        else
        {
          ip_seq_map.set(ip_seq->first, packet_receive.seq, 1);
        }
        dr_time_map.set(ip_seq->first, packet_receive.mutate_timestamp, 1);
      }
      else
      {
        ret = packet_receive.type;
        YYSYS_LOG(ERROR, "receive packet from server[%s], ret = %d", ip_seq->first.c_str(), ret);
      }
      ip_seq++;
    }

    YYSYS_LOG(INFO, "before continue searching, ruined_dis_trans_set [%ld]", ruined_dis_trans_set.size());

    while(OB_SUCCESS == ret)
    {
      int64_t before_ruined_trans_count = ruined_dis_trans_set.size();
      s_no++;
      ip_seq = ip_seq_map.begin();
      for (i = 0; i < ip_cnt; i++)
      {
        PACKET packet_send;
        packet_send.seq = ip_seq->second;
        packet_send.mutate_timestamp = 0;
        packet_send.sno = s_no;
        packet_send.ts = &ruined_dis_trans_set;
        packet_send.ts_size = ruined_dis_trans_set.size();
        packet_send.type = TRANS_SET_SEARCH;

        PACKET packet_receive;
        packet_receive.ts = &ruined_dis_trans_set;

        int cur_sock = server_sock[i];
        if (OB_SUCCESS != (ret = send_packet(cur_sock, packet_send)))
        {
          YYSYS_LOG(ERROR, "send schema result response error");
        }
        else if (OB_SUCCESS != (ret = receive_packet(cur_sock, packet_receive)))
        {
          YYSYS_LOG(ERROR, "receive packet error");
        }
        else if (OB_SUCCESS == packet_receive.type)
        {
          YYSYS_LOG(INFO, "receive packet:(sno=%d, type=%d, mutate_timestamp=%ld, seq=%ld, ts_size=%ld)",
                    packet_receive.sno, packet_receive.type, packet_receive.mutate_timestamp, packet_receive.seq,
                    packet_receive.ts_size);
          if (packet_receive.seq != 0 && -1 == ip_seq_map.set(ip_seq->first, packet_receive.seq, 1))
          {
            ret = OB_ERROR;
            YYSYS_LOG(ERROR, "set [%s]'s cut_seq:%ld to map fail, ret=%d", ip_seq->first.c_str(), packet_receive.seq, ret);
          }
          else if (packet_receive.mutate_timestamp > 0 && -1 == dr_time_map.set(ip_seq->first, packet_receive.mutate_timestamp, 1))
          {
            ret = OB_ERROR;
            YYSYS_LOG(ERROR, "set [%s]'s dr_time:%ld to map fail, ret=%d", ip_seq->first.c_str(), packet_receive.mutate_timestamp, ret);
          }
        }
        else
        {
          ret = packet_receive.type;
          YYSYS_LOG(ERROR, "receive packet from server[%s], ret = %d", ip_seq->first.c_str(), ret);
        }
        ip_seq++;
      }
      if (ruined_dis_trans_set.size() == before_ruined_trans_count)
      {
        break;
      }
      else if (ruined_dis_trans_set.size() < before_ruined_trans_count)
      {
        ret = OB_ERR_UNEXPECTED;
        YYSYS_LOG(ERROR, "the ruined_dis_trans_set smaller than before, wired");
      }
    }

    if (OB_SUCCESS == ret)
    {
      YYSYS_LOG(INFO, "Already find the consistent point");
      ip_seq = ip_seq_map.begin();
      while (OB_SUCCESS == ret && ip_seq != ip_seq_map.end())
      {
        int64_t time_stamp = 0;
        if (0 != ip_seq->second && !seq_id && HASH_EXIST == dr_time_map.get(ip_seq->first, time_stamp) && time_stamp > 0)
        {
          char real_time_str[30] = {0};
          if (OB_SUCCESS != (ret = realtime(time_stamp, real_time_str)))
          {
            YYSYS_LOG(ERROR, "the ruined_dis_trans_set smaller than before, wired");
            break;
          }
          else
          {
            fprintf(stdout, "End, ip:[%s]\t  seq_id:[%ld]\t  time_stamp:[%s]\n", ip_seq->first.c_str(), ip_seq->second, real_time_str);
          }
        }
        else if (0 < ip_seq->second)
        {
          fprintf(stdout, "End, ip:[%s]\t  seq_id:[%ld]\n", ip_seq->first.c_str(), ip_seq->second);
        }
        else
        {
          fprintf(stdout, "End, ip:[%s]\t  no need for recovery\n", ip_seq->first.c_str());
        }
        ip_seq++;
      }

      if (OB_SUCCESS == ret)
      {
        fprintf(stdout, "please decide whether recover or not? yes/no:");
        char cmd[20];
        fgets(cmd, 20, stdin);
        cmd[strlen(cmd) - 1] = '\0';
        if (0 == strcasecmp(cmd, "yes"))
        {
          YYSYS_LOG(INFO, "End, ruined_dis_trans_set [%ld]", ruined_dis_trans_set.size());
          i = 0;
          s_no++;
          ip_seq = ip_seq_map.begin();
          while (OB_SUCCESS == ret && i < ip_cnt)
          {
            PACKET packet_send;
            packet_send.seq = ip_seq->second;
            packet_send.mutate_timestamp = 0;
            packet_send.sno = s_no;
            packet_send.ts = NULL;
            packet_send.ts_size = 0;
            packet_send.type = START_CUTTING;

            PACKET packet_receive;
            packet_receive.ts = &ruined_dis_trans_set;

            int cur_sock = server_sock[i];
            if (OB_SUCCESS != (ret = send_packet(cur_sock, packet_send)))
            {
              YYSYS_LOG(ERROR, "send result packet error");
            }
            else if (OB_SUCCESS != (ret = receive_packet(cur_sock, packet_receive)))
            {
              YYSYS_LOG(ERROR, "receive packet error");
            }
            else if (OB_SUCCESS != packet_receive.type)
            {
              fprintf(stdout, "server[%s] error, err=%d\n", ip_seq->first.c_str(), packet_receive.type);
            }
            else
            {
              if (packet_receive.seq == 0)
              {
                fprintf(stdout, "server[%s] no need for recovery\n", ip_seq->first.c_str());
              }
              else
              {
                fprintf(stdout, "server[%s] already recover to log %lu\n", ip_seq->first.c_str(), packet_receive.seq);
              }
            }
            close(cur_sock);
            i++;
            ip_seq++;
          }
        }
        else if (0 == strcasecmp(cmd, "no"))
        {
          YYSYS_LOG(WARN, "Don't allow to cat");
          i = 0;
          ip_seq = ip_seq_map.begin();
          while (OB_SUCCESS == ret && i < ip_cnt)
          {
            PACKET packet_send;
            packet_send.seq = 0;
            packet_send.mutate_timestamp = 0;
            packet_send.sno = ++s_no;
            packet_send.ts = NULL;
            packet_send.ts_size = 0;
            packet_send.type = NO_CUTTING;

            int cur_sock = server_sock[i];
            if (OB_SUCCESS != (ret = send_packet(cur_sock, packet_send)))
            {
              YYSYS_LOG(ERROR, "send result packet error");
            }
            else
            {
              YYSYS_LOG(INFO, "send result packet succeed");
            }
            close(cur_sock);
            i++;
            ip_seq++;
          }
        }
      }
    }
    delete[] server_sock;
    server_sock = NULL;
    fprintf(stdout, "client over\n");
  }
  if (OB_SUCCESS != ret)
  {
    fprintf(stdout, "something wrong on client, ret=%d\n", ret);
  }
  return ret;
}

int main(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  fprintf(stderr, "client run\n");
  ob_init_memory_pool();
  Agent agent;
  if (OB_NEED_RETRY != (ret = CmdCall_Without_func(argc, argv, agent, StrArg(src_file)):OB_NEED_RETRY))
  {
    report_error(ret, "client()=>%d", ret);
  }
  else
  {
    //TODO
  }
  return ret;
}
