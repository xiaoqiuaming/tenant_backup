#include <string>
#include <vector>
#include <sstream>
#include <stdlib.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <getopt.h>
#include <sys/types.h>
#include <signal.h>
#include <sys/wait.h>

#include "common/ob_repeated_log_reader.h"
#include "common/ob_direct_log_reader.h"
#include "common/ob_log_reader.h"
#include "common/utility.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_transaction.h"
#include "common/ob_define.h"
#include "common/ob_version.h"


#include "updateserver/ob_ups_mutator.h"
#include "updateserver/ob_ups_utils.h"
#include "updateserver/ob_big_log_writer.h"
#include "updateserver/ob_trans_executor.h"
#include "updateserver/ob_update_server_main.h"
#include "updateserver/ob_ups_log_utils.h"
#include "updateserver/ob_ups_table_mgr.h"

#include "sql_builder.h"

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::updateserver;
using namespace std;


//��������
#define PULL_LOG_REQUEST 1000
#define PULL_LOG_RESPONSE 1001
#define PULL_LOG_CRC_REQUEST 1002
#define PULL_LOG_CRC_RESPONSE 1003

#define SCHEMA_REPORT_REQUEST 2000
#define SCHEMA_REPORT_RESPONSE 2001
#define SCHEMA_SWITCH_RESPONSE 2004

//Ĭ�ϻ�������С
static const int64_t MAX_BUFFER_LEN =  4L*(1<<30);//4GB
static int64_t DEFAULT_BUFFER_LEN =  1024 * 1024 * 512;//512MB

//��������С
static int64_t WRITE_BUFFER_LEN = DEFAULT_BUFFER_LEN + 44;//512MB + 44

//��д��־�ļ��Ļ������� �������������schema��С�����������л�����
//static const int READ_BUFFER_LEN = 1024 * 1024 * 8;
static const int64_t RW_BUFFER_LEN = 1024 * 1024 * 16;

//�洢���ݵĻ�������С
static int64_t DATA_BUFFER_LEN = DEFAULT_BUFFER_LEN;//512MB
static const int SQL_BUFFER_LEN  = 100*10000;//100��������
//��־�ļ���С
static const int MAX_LOG_SIZE = 1024 * 1024 * 256;//256MB

void HandleExit();
void HandleTerm();
void Wait();
#define MAX_CHD_NUM 10

int currChdNum = 0, maxChdNum = 0, logId = 0;
int sonPid[MAX_CHD_NUM];

//�汾��
#define VERSION_N 171717

//����״̬
//#define CONNECTED 0

#define DISCONNECTED -1

//flag
#define BUFFER_OVERFLOW -6
#define AGENT_ERROR -5
#define SCHEMA_INVAILD -4
//#define SCHEMA_SWITCH -3
//#define SEQ_INVALID -2
#define LOG_END -1
#define NORMAL 0
#define LOG_SWITCH 101
#define BIG_LOG_SWITCH 90000

//################################�����ķָ��� 1#########################################

//��ȡ����
struct pull_request
{
    int32_t sno;        //���к�

    int32_t type;        //����

    char* log_name;     //��־�ļ���

    int64_t seq;        //��־��

    int32_t version;    //�汾

    int32_t count;        //����

    char* extend;       //�����ֶ�

    int32_t extend_size;//�����ֶδ�С
};

//���صĽ��
struct pull_response
{
    int32_t sno;        //���к�

    int32_t type;        //����

    int32_t version;    //�汾

    int32_t count;        //SQL����

    int32_t flag;        //�л���־

    int64_t seq;        //��������ȡ����־��

    int64_t real_seq;    //��������ȡ��Ч��mutator

    int64_t body_size;  //�������

    char* body;         //��������ܶ���SQL�����ݣ���ʽΪ|Length:4|SEQ:4|Data:N|...|
};


class LogAgent
{
  public:
    LogAgent();
    int start(const int argc, char *argv[]);
    //int reconnect();//����
    int handle_request(int sockfd);
    int balanceBuffer();//��̬����succ_num=0 over_flow buf increase;=1 buf default
    DISALLOW_COPY_AND_ASSIGN(LogAgent);
  private:
    int succ_num;
    CommonSchemaManagerWrapper schema;

    int schema_flag;//0 ��ʾ����  -1 ��ʾΪ��

    //���ӱ�־λ
    //          int connected_flag;
    //socket���
    int server_sock;
    SqlBuilder builder;
    int current_file_id;
    char* socket_send_buf;
    char* sql_buf;
    //char master_ip[OB_IP_STR_BUFF];
    uint16_t master_port;
    char ups_log_dir[OB_MAX_FILE_NAME_LENGTH];
    char usr_filter_dir[OB_MAX_FILE_NAME_LENGTH];
    TableIdSet tableidset;
    char agent_log_dir[OB_MAX_FILE_NAME_LENGTH];

    yysys::CThreadCond cond_;
    ObBigLogWriter big_log_writer;//>2M log

  private:
    //���л�����
    int build_response(char* buf, const pull_response response, int64_t &size);

    //�ع�schema����
    //int rebuild_schema(LogCommand cmd, uint64_t seq, const char* log_data, int64_t data_len);
    int rebuild_schema(int sockfd, LogCommand cmd, uint64_t seq, const char* log_data, int64_t data_len);

    //׷��schema��Ϣ
    //int schema_append(int fd, char* log_dir, pull_request request);
    //�ع�SQL
    int rebuild_sql(uint64_t seq, const char* log_data, int64_t data_len, vector<string> &sqls, LogCommand cmd);

    int get_checksum(uint64_t seq,
                     const char* log_data,
                     int64_t data_len,
                     int64_t &mutator_time,
                     int64_t &data_checksum_before,
                     int64_t &data_checksum_after,
                     LogCommand cmd);

    //���л�TransID
    int encode_dis_trans_type_and_id(char *buf, const int64_t buf_len, int64_t &pos, int8_t log_type, ObTransID tid);

    //��SQL������л�
    int encode_data(char *buf, const int64_t buf_len, int64_t& pos, const char *sql, int64_t seq, int32_t index);

    //���л�ͷ������
    int encode_data_header(char *buf,
                           const int64_t buf_len,
                           int64_t& pos,
                           int64_t seq,
                           int64_t mutator_time,
                           int64_t checksum_before,
                           int64_t checksum_after,
                           int32_t count);

    int encode_log_type(char *buf, const int64_t buf_len, int64_t &pos, int8_t log_type);

    //���ӷ�����
    //int connect_server(int &fd ,const char* server_ip, uint32_t port);

    //�����յ�������
    int parse_request(char* buf, int64_t data_len, pull_request &request);

    //������ȡSQL������
    int handler_pull_request(int fd, char* log_dir, pull_request request);

    //������ȡ��־CRCУ���������
    int handler_pull_crc_request(int fd, char* log_dir, pull_request request);

    //��ȡn���ֽ�
    ssize_t readn(int fd, void *vptr, size_t n);

    //д��n���ֽ�
    ssize_t writen(int fd,const void *vptr, size_t n);

    //��ȡһ�����ݰ�
    int receive_packet(int fd, pull_request &request);

    //����schema��Ϣ��Server(Server�洢Schema��Ϣ����Agent������ʱ����Ի�ȡ)
    int send_schema(int fd, uint64_t seq ,CommonSchemaManagerWrapper &schema);

    void parse_cmd_line(const int argc,  char *const argv[]);
    void parse_filter_file(const char *dir);
    void print_usage(const char *prog_name);

    void print_version();
  public:
    //����һ�����ݰ�
    int send_packet(int fd, const pull_response response);
    int setsock(int);

};



//################################�����ķָ��� 2#########################################


LogAgent::LogAgent()
{
  succ_num=100;
  memset(agent_log_dir, 0, sizeof(agent_log_dir));
  memset(usr_filter_dir, 0, sizeof(usr_filter_dir));
  schema_flag = -1;
  server_sock = -1;
  socket_send_buf = new char[WRITE_BUFFER_LEN];

  memset(socket_send_buf, 0, WRITE_BUFFER_LEN);//��Ϊ0

  sql_buf = new char[DATA_BUFFER_LEN];
  memset(sql_buf, 0, DATA_BUFFER_LEN);//��Ϊ0
  tableidset.create(hash::cal_next_prime(TABLE_BUCKET));
  big_log_writer.init();
}

int LogAgent::balanceBuffer()//succ_num:=0OB_MEM_OVERFLOW buf increase;=1 buf set default
{
  int ret=OB_SUCCESS;

  if(this->succ_num==100 && DATA_BUFFER_LEN != DEFAULT_BUFFER_LEN) //buf��һ��������
  {
    WRITE_BUFFER_LEN = DEFAULT_BUFFER_LEN + 44;//512MB + 44
    DATA_BUFFER_LEN = DEFAULT_BUFFER_LEN;//512MB
  }
  else if(this->succ_num>0)//buf ����
  {
    if(this->succ_num>1000) this->succ_num=200;
    return ret;
  }
  else if(this->succ_num==0)//buf������
  {
    WRITE_BUFFER_LEN += DEFAULT_BUFFER_LEN;//+512MB
    DATA_BUFFER_LEN  += DEFAULT_BUFFER_LEN;//+512MB
  }
  else {
    YYSYS_LOG(ERROR, "#balanceBuffer,succ_num=%d#",this->succ_num);
    exit(0);
  }

  if(DATA_BUFFER_LEN>MAX_BUFFER_LEN)// > 4GB
  {
    YYSYS_LOG(ERROR, "balanceBuffer too large,DATA_BUFFER_LEN=%ld,MAX_BUFFER_LEN=%ld",
              DATA_BUFFER_LEN,MAX_BUFFER_LEN);
    exit(0);
  }

  YYSYS_LOG(INFO, "Reset LogAgent buffer,current_buffer=%ld",DATA_BUFFER_LEN);
  delete [] this->socket_send_buf;
  this->socket_send_buf = new char[WRITE_BUFFER_LEN];
  memset(this->socket_send_buf, 0, WRITE_BUFFER_LEN);//��Ϊ0

  delete [] this->sql_buf;
  this->sql_buf = new char[DATA_BUFFER_LEN];
  memset(this->sql_buf, 0, DATA_BUFFER_LEN);//��Ϊ0
  return ret;
}

int LogAgent::setsock(int sdNew)
{
  struct linger soLinger;
  int soKeepAlive, soReuseAddr;

  soLinger.l_onoff = true;
  soLinger.l_linger = true;
  if (-1 == setsockopt(sdNew, SOL_SOCKET, SO_LINGER,
                       (char *)&soLinger, sizeof(soLinger))) {
    YYSYS_LOG(ERROR, "[setsockopt]set linger option failed, errno[%d]", errno);
    return -1;
  }
  soKeepAlive = true;
  if (-1 == setsockopt(sdNew, SOL_SOCKET, SO_KEEPALIVE,
                       &soKeepAlive, (int)sizeof(soKeepAlive))) {
    YYSYS_LOG(ERROR, "[setsockopt]set keepalive option failed, errno[%d]", errno);
    return -1;
  }
  soReuseAddr = true;
  if (-1 == setsockopt(sdNew, SOL_SOCKET, SO_REUSEADDR,
                       &soReuseAddr, (int)sizeof(soReuseAddr))) {
    YYSYS_LOG(ERROR, "[setsockopt]set reuse addr option failed, errno[%d]", errno);
    return -1;
  }
  int chOpt = 1;
  if (-1 == setsockopt(sdNew, IPPROTO_TCP, TCP_NODELAY, &chOpt, sizeof(chOpt))) {
    YYSYS_LOG(ERROR, "[setsockopt]set TCP_NODELAY option failed, errno[%d]", errno);
    return -1;
  }
  return 0;
}

int LogAgent::start(const int argc, char *argv[])
{
  //��������
  parse_cmd_line(argc,argv);

  YYSYS_LOGGER.setLogLevel("INFO");
  YYSYS_LOGGER.setMaxFileSize(MAX_LOG_SIZE);
  char father_log_dir[OB_MAX_FILE_NAME_LENGTH] = {0};
  if (strlen(agent_log_dir) == 0) {
    snprintf(agent_log_dir, sizeof(agent_log_dir), "%s", "logs/agent");
  }
  strcat(father_log_dir, agent_log_dir);
  strcat(father_log_dir, ".log");
  YYSYS_LOGGER.setFileName(father_log_dir);

  int client_sock = -1;
  struct sockaddr_in server_addr;
  bzero(&server_addr, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = inet_addr("0.0.0.0");
  server_addr.sin_port = htons(master_port);

  server_sock = socket(AF_INET, SOCK_STREAM, 0);
  if (-1 == server_sock) {
    YYSYS_LOG(ERROR, "socket fail, errno:%d-%s", errno, strerror(errno));
    return -1;
  }

  if (setsock(server_sock) == -1) {
    YYSYS_LOG(ERROR, "setsockopt fail, errno:%d-%s", errno, strerror(errno));
    return -1;
  }
  if (bind(server_sock, (struct sockaddr *)(&server_addr), sizeof(server_addr)) == -1) {
    YYSYS_LOG(ERROR, "bind fail, errno:%d-%s", errno, strerror(errno));
    return -1;
  }
  if (listen(server_sock, 32) == -1) {
    YYSYS_LOG(ERROR, "listen fail, errno:%d-%s", errno, strerror(errno));
    return -1;
  }
  while (true) {
    while (currChdNum >= MAX_CHD_NUM) {
      sleep(1);
    }
    for (logId = 0; logId < MAX_CHD_NUM; logId++) {
      if(sonPid[logId] == -1) {
        break;
      }
    }
    sighold(SIGCLD);

    socklen_t server_addr_size = sizeof(server_addr);
    if ((client_sock = accept(server_sock, (struct sockaddr *)(&server_addr), &server_addr_size)) == -1) {
      YYSYS_LOG(ERROR, "accept fail, errno:%d-%s", errno, strerror(errno));
      sleep(1);
      continue;
    }
    YYSYS_LOG(INFO, "accept server_sock:%d, client_sock:%d", server_sock, client_sock);
    pid_t child = fork();
    if (child == 0) {
      char log_dir[OB_MAX_FILE_NAME_LENGTH];
      snprintf(log_dir, sizeof(log_dir), "%s.%d.log", agent_log_dir, logId);
      YYSYS_LOGGER.setFileName(log_dir, true);

      signal(SIGTERM, SIG_IGN);
      signal(SIGCHLD, SIG_IGN);
      signal(SIGUSR2, (sighandler_t) HandleExit);
      YYSYS_LOG(INFO, "subprocess accept server_sock:%d, client_sock:%d", server_sock, client_sock);
      handle_request(client_sock);
      sleep(1);
      close(client_sock);
      exit(0);
    }
    else if (child < 0) {
      close(client_sock);
      continue;
    }
    else {
      //������
      currChdNum++;
      sonPid[logId] = child;
      YYSYS_LOG(INFO, "server_sock:%d, client_sock:%d", server_sock, client_sock);
    }
    sigrelse(SIGCLD);
  }
}

int LogAgent::handle_request(int sockfd)
{
  char *log_dir = ups_log_dir;
  struct sockaddr_in cliaddr;
  char cli_ip[20];
  memset(&cliaddr, 0x00, sizeof(cliaddr));
  memset(cli_ip, 0x00, sizeof(cli_ip));
  unsigned int len_cliaddr = sizeof(cliaddr);
  getpeername(sockfd, (struct sockaddr *) &cliaddr, &len_cliaddr);
  inet_ntop(AF_INET, &cliaddr.sin_addr, cli_ip, sizeof(cliaddr));
  pid_t pid = getpid();

  while (true) {
    pull_request request;
    if (OB_SUCCESS != receive_packet(sockfd, request))
    {
      YYSYS_LOG(ERROR, "receive_packet fail");
      return -1;
    }
    else {
      YYSYS_LOG(INFO,
                "pid:%d, receive request:(sno=%d, type=%d, log_name=%s, seq=%ld, count=%d), client:%s:%d",
                pid,
                request.sno,
                request.type,
                request.log_name,
                request.seq,
                request.count,
                cli_ip,
                cliaddr.sin_port);
      switch(request.type) {
        case PULL_LOG_REQUEST:
          if (OB_SUCCESS != handler_pull_request(sockfd, log_dir, request))
          {
            YYSYS_LOG(ERROR, "handler_pull_request error!");
          }
          break;
        case PULL_LOG_CRC_REQUEST:
          if (OB_SUCCESS != handler_pull_crc_request(sockfd, log_dir, request))
          {
            YYSYS_LOG(ERROR, "handler_pull_crc_request error!");
          }
          break;
        case SCHEMA_REPORT_REQUEST:
          pull_response response;
          response.sno = request.sno;
          response.type = SCHEMA_REPORT_RESPONSE;
          response.version = VERSION_N;
          response.seq = request.seq;
          response.real_seq = 0;
          response.flag = NORMAL;
          response.count = 0;
          response.body = (char *) "schema result response";
          response.body_size = (int64_t) strlen(response.body);
          if (request.seq == -1)
          {
            schema_flag = -1;
            YYSYS_LOG(INFO, "receive empty schema");
            response.flag = SCHEMA_INVAILD;
          }
          else {
            schema.reset();
            int64_t pos = 0;
            if (OB_SUCCESS != schema.deserialize(request.extend, request.extend_size, pos))
            {
              YYSYS_LOG(ERROR, "ObSchemaManagerWrapper deserialize error");
              response.flag = SCHEMA_INVAILD;
            }
            else {
              YYSYS_LOG(INFO, "receive schema success, size:%ld bytes", pos);
              schema_flag = 1;
            }
          }
          if (OB_SUCCESS != send_packet(sockfd, response)) {
            YYSYS_LOG(ERROR, "send schema result response error");
          }
          else {
            YYSYS_LOG(INFO, "send schema response success, last seq=%lu, sno=%d, read couont=%d",
                      response.seq, response.sno, response.count);
          }
          break;
        default:YYSYS_LOG(ERROR, "request type error!");
          return -1;
      }
      //�ͷ��ڴ�
      if (request.log_name != NULL) {
        delete[] request.log_name;
      }
      if (request.extend != NULL) {
        delete[] request.extend;
      }
    }
  }
}

//������������
int LogAgent::build_response(char* buf, const pull_response response, int64_t &size)
{
  int ret = OB_SUCCESS;
  int64_t pos = 4;
  int header_size = 44;//4 + 4 + 4 + 4 + 4 + 8 + 8 + 8
  if(header_size + response.body_size > WRITE_BUFFER_LEN) {
    ret = OB_ERROR;
    YYSYS_LOG(ERROR,
              "buffer size not enough, fixed size=%ld, need size=%ld",
              WRITE_BUFFER_LEN,
              header_size + response.body_size);
  }
  if (OB_SUCCESS != (ret = serialization::encode_i32(buf, WRITE_BUFFER_LEN, pos, response.sno))) {
    YYSYS_LOG(ERROR,  "fail to serialize sno. ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_i32(buf, WRITE_BUFFER_LEN, pos, response.type))) {
    YYSYS_LOG(ERROR,  "fail to serialize type. ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_i32(buf, WRITE_BUFFER_LEN, pos, response.version))) {
    YYSYS_LOG(ERROR, "fail to serialize version. ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_i32(buf, WRITE_BUFFER_LEN, pos, response.count))) {
    YYSYS_LOG(ERROR, "fail to serialize count. ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_i32(buf, WRITE_BUFFER_LEN, pos, response.flag))) {
    YYSYS_LOG(ERROR, "fail to serialize flag. ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_i64(buf, WRITE_BUFFER_LEN, pos, response.seq))) {
    YYSYS_LOG(ERROR, "fail to serialize size. ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_i64(buf, WRITE_BUFFER_LEN, pos, response.real_seq))) {
    YYSYS_LOG(ERROR, "fail to serialize size. ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_i64(buf, WRITE_BUFFER_LEN, pos, response.body_size))) {
    YYSYS_LOG(ERROR, "fail to serialize len. ret=%d", ret);
  }
  else {
    if(response.body != NULL && response.body_size > 0)
    {
      memcpy(buf + pos, response.body, response.body_size);
      pos += response.body_size;
    }
    int64_t temp_pos = 0;
    if (OB_SUCCESS != (ret = serialization::encode_i32(buf,
                                                       WRITE_BUFFER_LEN,
                                                       temp_pos,
                                                       (int32_t)(pos - 4))))//first 4 byte wite length
    {
      YYSYS_LOG(ERROR, "fail to serialize len. ret=%d", ret);
    }
    else {
      size = pos;
    }
  }
  return ret;
}

/**
 * @brief �ع�schema��Ϣ
 * @param cmd
 * @param seq
 * @param log_data
 * @param data_len
 * @return
 */
int LogAgent::rebuild_schema(int sockfd, LogCommand cmd, uint64_t seq, const char* log_data, int64_t data_len)
{
  UNUSED(seq);
  int ret = OB_SUCCESS;
  ObSchemaMutator schema_mutator;
  int64_t pos = 0;
  switch(cmd) {
    case OB_UPS_SWITCH_SCHEMA://Schemaȫ��
      schema.reset();
      if (OB_SUCCESS != (ret = schema.deserialize(log_data, data_len, pos)))
      {
        YYSYS_LOG(ERROR, "OB_UPS_SWITCH_SCHEMA deserialize error[ret=%d log_data=%p data_len=%ld]",
                  ret, log_data, data_len);
      }
      else
      {
        schema_flag = 0;
        YYSYS_LOG(INFO, "OB_UPS_SWITCH_SCHEMA deserialize success, table_count %ld need_next_serialize:%d, is_completion:%d, schema_size:%ld",
                  schema.get_impl_ref().get_table_count(),
                  schema.get_impl_ref().need_next_serialize(),
                  schema.get_impl_ref().is_completion(),
                  schema.get_impl_ref().get_serialize_size());
      }
      break;
    case OB_UPS_SWITCH_SCHEMA_NEXT:
      if (OB_SUCCESS != (ret = schema.get_impl_ref().deserialize(log_data, data_len, pos)))
      {
        YYSYS_LOG(WARN, "OB_UPS_SWITCH_SCHEMA_NEXT deserialize fail log_buffer=%p data_len=%ld pos=%ld ret=%d, cmd=%d",
                  log_data, data_len, pos, ret, cmd);
      }
      else
      {
        schema_flag = 0;
        YYSYS_LOG(INFO, "OB_UPS_SWITCH_SCHEMA_NEXT deserialize success, table_count %ld need_next_serialize:%d, is_completion:%d, schema_size:%ld",
                  schema.get_impl_ref().get_table_count(),
                  schema.get_impl_ref().need_next_serialize(),
                  schema.get_impl_ref().is_completion(),
                  schema.get_impl_ref().get_serialize_size());
      }
      break;
    case OB_UPS_WRITE_SCHEMA_NEXT:
      YYSYS_LOG(ERROR, "OB_UPS_WRITE_SCHEMA_NEXT not process!");
      break;

    case OB_UPS_SWITCH_SCHEMA_MUTATOR://Schema����
      if (OB_SUCCESS != (ret = schema_mutator.deserialize(log_data, data_len, pos))) {
        YYSYS_LOG(ERROR, "OB_UPS_SWITCH_SCHEMA_MUTATOR deserialize error[ret=%d log_data=%p data_len=%ld]",
                  ret, log_data, data_len);
      }
      else if(schema.get_version()==schema_mutator.get_end_version())
      {
        YYSYS_LOG(INFO,
                  "schema_mutator has already been applyed,schema version=%ld, schema_mutator end version=%ld",
                  schema.get_version(),
                  schema_mutator.get_end_version());
      }
      else if(OB_SUCCESS != (ret = schema.get_impl_ref().apply_schema_mutator(schema_mutator))) {
        YYSYS_LOG(ERROR,
                  "apply_schema_mutator error ret=%d, schema version=%ld, schema_mutator start version=%ld, end version=%ld",
                  ret,
                  schema.get_version(),
                  schema_mutator.get_start_version(),
                  schema_mutator.get_end_version());
      }
      else {
        YYSYS_LOG(INFO,
                  "apply_schema_mutator succ, schema version=%ld, schema_mutator start version=%ld, end version=%ld",
                  schema.get_version(),
                  schema_mutator.get_start_version(),
                  schema_mutator.get_end_version());
      }
      break;

    default:ret = OB_ERROR;
      break;
  }

  if(OB_SUCCESS == ret) {
    //����schema��Ϣ��������
    ret = send_schema(sockfd, seq, schema);
  }
  else {
    //schema �쳣�����´�capture��ȡ����schema
    //pull_schema(sockfd);
    ret = OB_SCHEMA_ERROR;
  }

  return ret;
}

//��������SQL
int LogAgent::rebuild_sql(uint64_t seq, const char* log_data, int64_t data_len, vector<string> &sqls, LogCommand cmd)
{
  int ret = OB_SUCCESS;
  if(seq == 1) {
    YYSYS_LOG(INFO, "fuck this log is bad.");
    //ret = OB_INVALID_ERROR;
  }
  else {
    ObUpsMutator mut;
    int64_t pos = 0;
    if (OB_UPS_MUTATOR_PREPARE == cmd) {
      ObTransID coordinator_tid;
      ret = coordinator_tid.deserialize_for_prepare(log_data, data_len, pos);
    }
    ret = mut.deserialize(log_data, data_len, pos);
    if (OB_SUCCESS != ret) {
      YYSYS_LOG(ERROR, "Error occured when deserializing ObUpsMutator.");
    }
    else {
      if (mut.is_freeze_memtable()) {
        //Freeze Operation
        YYSYS_LOG(WARN, "mutator is freeze memtable.");
      }
      else if (mut.is_drop_memtable()) {
        //Drop Operation
        YYSYS_LOG(WARN, "mutator is drop memtable.");
      }
      else {
        //����SQL
        if(schema.get_version() > 0 && (schema_flag == 0 || schema_flag == 1)) {
          ret = builder.ups_mutator_to_sql(seq, mut, sqls, &schema, &tableidset);
          //YYSYS_LOG(ERROR, "SEQ: %lu",seq);

        }
        else {
          ret = OB_SCHEMA_ERROR;
          YYSYS_LOG(WARN, "No Schema.");
        }
      }

    }
  }
  return ret;
}

int LogAgent::get_checksum(uint64_t seq,
                           const char* log_data,
                           int64_t data_len,
                           int64_t &mutator_time,
                           int64_t &data_checksum_before,
                           int64_t &data_checksum_after,
                           LogCommand cmd)
{
  int ret = OB_SUCCESS;
  UNUSED(seq);
  ObUpsMutator mut;
  int64_t pos = 0;

  if (OB_UPS_MUTATOR_PREPARE == cmd) {
    ObTransID coordinator_tid;
    ret = coordinator_tid.deserialize_for_prepare(log_data, data_len, pos);
  }

  ret = mut.deserialize(log_data, data_len, pos);
  if (OB_SUCCESS != ret) {
    YYSYS_LOG(ERROR, "Error occured when deserializing ObUpsMutator.");
  }
  else {
    mutator_time = mut.get_mutate_timestamp();
    data_checksum_before = mut.get_memtable_checksum_before_mutate();
    data_checksum_after = mut.get_memtable_checksum_after_mutate();
  }
  return ret;
}

//ϵ�л�TransID
int LogAgent::encode_dis_trans_type_and_id(char *buf,
                                           const int64_t buf_len,
                                           int64_t &pos,
                                           int8_t log_type,
                                           ObTransID tid)
{
  const char *trans_id = to_cstring(tid);
  int ret = OB_SUCCESS;
  int32_t length = (int32_t) strlen(trans_id);
  if (buf_len - pos < (length + 5)) {
    ret = OB_MEM_OVERFLOW;
    YYSYS_LOG(WARN, "buffer size not enough, fixed size=%ld, need size=%d, pos=%ld", buf_len, length, pos);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_i8(buf, buf_len, pos, log_type))) {
    YYSYS_LOG(ERROR, "fail to serialize log type. ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_i32(buf, buf_len, pos, length))) {
    YYSYS_LOG(ERROR, "fail to serialize trans_id length. ret=%d", ret);
  }
  else {
    memcpy(buf + pos, trans_id, length);
    pos += length;
  }
  return ret;
}

//���л�SQL
int LogAgent::encode_data(char *buf, const int64_t buf_len, int64_t& pos, const char *sql, int64_t seq, int32_t index)
{
  //index:4 | length:4 | seq:8 | sql_data:n
  int ret = OB_SUCCESS;
  int32_t length = (int32_t)strlen(sql);
  if(buf_len - pos < (length + 12)) {
    ret = OB_MEM_OVERFLOW;
    YYSYS_LOG(WARN,  "buffer size not enough, fixed size=%ld, need size=%d, pos=%ld", buf_len, length, pos);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_i32(buf, buf_len, pos, index))) {
    YYSYS_LOG(ERROR,  "fail to serialize sql length. ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_i32(buf, buf_len, pos, length))) {
    YYSYS_LOG(ERROR,  "fail to serialize sql length. ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, pos, seq))) {
    YYSYS_LOG(ERROR,  "fail to serialize seq. ret=%d", ret);
  }
  else {
    memcpy(buf + pos, sql, length);
    pos += length;
  }
  return ret;
}

int LogAgent::encode_data_header(char *buf,
                                 const int64_t buf_len,
                                 int64_t& pos,
                                 int64_t seq,
                                 int64_t mutator_time,
                                 int64_t checksum_before,
                                 int64_t checksum_after,
                                 int32_t count)
{
  //seq:8 | check_sum_before:8 | check_sum_after:8 | sql_count:4
  int ret = OB_SUCCESS;
  if(buf_len - pos < 28) {
    ret = OB_MEM_OVERFLOW;
    YYSYS_LOG(ERROR,  "buffer size not enough, fixed size=%ld, pos=%ld", buf_len, pos);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, pos, seq))) {
    YYSYS_LOG(ERROR,  "fail to serialize seq. ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, pos, mutator_time))) {
    YYSYS_LOG(ERROR,  "fail to serialize mutator_time. ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, pos, checksum_before))) {
    YYSYS_LOG(ERROR,  "fail to serialize checksum_before. ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, pos, checksum_after))) {
    YYSYS_LOG(ERROR,  "fail to serialize checksum_after. ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_i32(buf, buf_len, pos, count))) {
    YYSYS_LOG(ERROR,  "fail to serialize count. ret=%d", ret);
  }
  return ret;
}

int LogAgent::encode_log_type(char *buf, const int64_t buf_len, int64_t &pos, int8_t log_type)
{
  int ret = OB_SUCCESS;
  if (buf_len - pos < 1) {
    ret = OB_MEM_OVERFLOW;
    YYSYS_LOG(ERROR, "buffer size not enough, fixed size=%ld, pos=%ld", buf_len, pos);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_i8(buf, buf_len, pos, log_type))) {
    YYSYS_LOG(ERROR, "fail to serialize log type. ret=%d", ret);
  }
  return ret;
}

int LogAgent::handler_pull_request(int fd, char* log_dir, pull_request request)
{
  int ret = OB_SUCCESS;
  ObLogCommitPoint commit_point_;
  int64_t clog_point = -1;

  //2Mlog
  bool is_big_log=false;
  int  big_log_switch_num=0;
  int  big_log_num=0;

  //��־��ȡ���
  ObLogReader log_reader;
  ObRepeatedLogReader reader;
  ObLogCursor end_cursor;

  LogCommand cmd;
  uint64_t log_seq = 0;
  char* log_data = NULL;
  int64_t data_len = 0;

  //buf �Զ�����
  this->balanceBuffer();

  int log_name = atoi(request.log_name);//������ļ���
  uint64_t last_log_seq = (const uint64_t)request.seq;//��������һ�ε���־��

  //��ȡ����־��
  int count = request.count;
  int64_t limit_size = DATA_BUFFER_LEN;
  //��Ӧ
  pull_response response;
  response.sno = request.sno;
  response.type = PULL_LOG_RESPONSE;
  response.version = VERSION_N;
  response.seq = last_log_seq;
  response.real_seq = 0;
  response.flag = NORMAL;
  response.count = 0;
  response.body = sql_buf;
  response.body_size = 0;
  int32_t cell_info_count = 0;
  int64_t old_pos = response.body_size;
  int8_t log_type;
  //��ȡ��־����
  if (OB_SUCCESS != (ret = log_reader.init(&reader, log_dir, log_name, 0, false))) {
    YYSYS_LOG(ERROR, "ObLogReader init error[err=%d]", ret);
  }
  else if (OB_SUCCESS != (ret = commit_point_.init(log_dir, "commit_point"))) {
    YYSYS_LOG(ERROR, "commit_point_file.init error, ret = %d", ret);
  }

  //init log reader,
  if( OB_SUCCESS == ret && last_log_seq > 0) {
    while (OB_SUCCESS == (ret = log_reader.read_log(cmd, log_seq, log_data, data_len))) {
      if(log_seq>=last_log_seq)
        break;
    }
    if(log_seq!=last_log_seq) {
      YYSYS_LOG(ERROR, "ObLogReader init error, last_log_seq[%lu] cur_log_seq[%lu]",
                last_log_seq,log_seq);
      ret=OB_INVALID_LOG;
    }
  }
  big_log_writer.reset_Last_tranID();//��ʼ��big log,last tran id
  if (OB_SUCCESS == ret && OB_SUCCESS != (ret = commit_point_.get(clog_point))) {
    YYSYS_LOG(ERROR, "get_commit_point_func error ret => %d", ret);
  }
  while (big_log_num < 1 && count > 0 && OB_SUCCESS == ret ) {
    if (log_seq > (const uint64_t) clog_point)
    {
      YYSYS_LOG(INFO,
                "log_seq is bigger than commit_point, log_seq is %ld, commit_point is %ld",
                log_seq,
                clog_point);
      response.flag = LOG_END;
      break;
    }
    if(is_big_log==false && cell_info_count > SQL_BUFFER_LEN) {
      break;
    }

    if (OB_SUCCESS != (ret = log_reader.read_log(cmd, log_seq, log_data, data_len)) &&
        OB_FILE_NOT_EXIST != ret && OB_READ_NOTHING != ret && OB_LAST_LOG_RUINNED != ret) {
      YYSYS_LOG(ERROR, "ObLogReader read error[ret=%d]", ret);
    }
    else if (OB_LAST_LOG_RUINNED == ret) {
      YYSYS_LOG(ERROR, "last_log[%s] broken!", end_cursor.to_str());
    }
    else if (OB_FILE_NOT_EXIST == ret) {
      YYSYS_LOG(WARN, "log file not exist");
    }
    else if (OB_READ_NOTHING == ret) {
      //YYSYS_LOG(ERROR, "waiting log data");
      //���ö�ȡ�������һ����־��
      if(!is_big_log) {
        response.seq = (int64_t)log_seq > request.seq ? log_seq : request.seq;
      }
    }
    else if (OB_SUCCESS != (ret = log_reader.get_next_cursor(end_cursor))) {
      YYSYS_LOG(ERROR, "log_reader.get_cursor()=>%d",  ret);
    }
    else {
      current_file_id = log_name;
      //����2M��־����������
      //��ͨ��־
      if (OB_LOG_UPS_MUTATOR == cmd || OB_UPS_MUTATOR_PREPARE == cmd || OB_UPS_BIG_LOG_DATA == cmd)
      {
        //����2M��־����������
        if( OB_UPS_BIG_LOG_DATA == cmd ) {
          is_big_log=true;
          bool is_all_completed=false;
          if(OB_SUCCESS!=(ret=big_log_writer.package_big_log(log_data,data_len,is_all_completed))) {
            YYSYS_LOG(ERROR, "SEQ:%lu,package_big_log fail,ret=%d",log_seq,ret);
          }
          else if(is_all_completed) {
            int32_t temp_len;
            big_log_writer.get_buffer(log_data,temp_len);
            data_len=(int64_t)temp_len;
            is_big_log=false;
            big_log_num++;
            YYSYS_LOG(DEBUG, "package_big_log completed SEQ:%lu ,len=%lu",log_seq,data_len);
            if(big_log_switch_num>0) {
              response.flag=BIG_LOG_SWITCH;
              response.flag+=big_log_switch_num;
            }
          }
          else
          {
            continue;
          }
        }

        //YYSYS_LOG(INFO, "read log => SEQ: %lu Payload Length: %ld TYPE: %s", log_seq, data_len, "OB_LOG_UPS_MUTATOR");
        //����sql
        vector<string> sqls;
        int64_t mutator_time;
        int64_t data_checksum_before;
        int64_t data_checksum_after;
        old_pos = response.body_size;
        ObTransID coordinator_tid;
        int64_t pos = 0;
        if (OB_UPS_MUTATOR_PREPARE == cmd) {
          log_type = 1;
          ret = coordinator_tid.deserialize_for_prepare(log_data, data_len, pos);
        }
        else {
          log_type = 0;
        }
        if (OB_SUCCESS != (ret = rebuild_sql(log_seq, log_data, data_len, sqls, cmd))) {
          YYSYS_LOG(ERROR, "rebuild_sql error, ret=%d", ret);
        }
        else if(sqls.size() <= 0) {
          YYSYS_LOG(DEBUG, "rebuild_sql maybe error, count:%d", (int32_t)sqls.size());
        }
        else if(sqls.size() > 0 && OB_SUCCESS != (ret = get_checksum(log_seq,
                                                                     log_data,
                                                                     data_len,
                                                                     mutator_time,
                                                                     data_checksum_before,
                                                                     data_checksum_after,
                                                                     cmd))) {
          YYSYS_LOG(ERROR, "get_checksum error, ret=%d", ret);
        }
        else if (OB_UPS_MUTATOR_PREPARE == cmd && OB_SUCCESS != (ret =
                                                                 encode_dis_trans_type_and_id(response
                                                                                              .body,
                                                                                              limit_size,
                                                                                              response
                                                                                              .body_size,
                                                                                              log_type,
                                                                                              coordinator_tid))) {
          YYSYS_LOG(ERROR, "encode trans id error, ret=%d", ret);
        }
        else if ((OB_LOG_UPS_MUTATOR == cmd || OB_UPS_BIG_LOG_DATA == cmd)
                 && OB_SUCCESS
                 != (ret = encode_log_type(response.body, limit_size, response.body_size, log_type))) {
          YYSYS_LOG(ERROR, "encode_log_type error, ret=%d", ret);
        }
        else if(sqls.size() > 0 && OB_SUCCESS != (ret = encode_data_header(response.body,
                                                                           limit_size,
                                                                           response.body_size,
                                                                           log_seq,
                                                                           mutator_time,
                                                                           data_checksum_before,
                                                                           data_checksum_after,
                                                                           (int32_t)sqls.size()))) {
          YYSYS_LOG(ERROR, "encode_data_header error, ret=%d", ret);
        }
        else {
          for (vector<string>::iterator iter = sqls.begin(); iter != sqls.end(); iter++)
          {
            string sql = *iter;
            if(OB_SUCCESS != (ret = encode_data(response.body,
                                                limit_size,
                                                response.body_size,
                                                sql.c_str(),
                                                log_seq,
                                                cell_info_count))) {
              YYSYS_LOG(WARN, "encode_data error, ret=%d", ret);
              break;
            }
          }
          response.count++;
          cell_info_count+=(int32_t)sqls.size();
          count--;//��־����
        }
        //buffer����
        if(OB_MEM_OVERFLOW == ret) {
          //���������л�������
          response.body_size = old_pos;
        }
        else {
          //���ö�ȡ�������һ����־��
          response.seq = (int64_t)log_seq > request.seq ? log_seq : request.seq;
          //��ǰmutator��SQL������real_seq
          if(sqls.size() > 0)
            response.real_seq = response.seq;
        }
      }
      else if (OB_PART_TRANS_COMMIT == cmd || OB_PART_TRANS_ROLLBACK == cmd) {
        old_pos = response.body_size;
        if (OB_PART_TRANS_COMMIT == cmd)
        {
          log_type = 2;
        }
        else {
          log_type = 3;
        }
        int64_t mutator_time = 0;
        int64_t data_checksum_before = 0;
        int64_t data_checksum_after = 0;
        int32_t sql_count = 0;
        int64_t pos = 0;
        ObTransID coordinator_tid;
        ObUpsMutator mut;
        if (schema.get_version() <= 0 || schema_flag < 0) {
          ret = OB_SCHEMA_ERROR;
          YYSYS_LOG(WARN, "No Schema");
        }
        else if (OB_SUCCESS != (ret = coordinator_tid.deserialize_for_prepare(log_data, data_len, pos))) {
          YYSYS_LOG(ERROR, "deserialize tid error, ret=%d", ret);
        }
        else if (OB_PART_TRANS_COMMIT == cmd
                 && OB_SUCCESS != (ret = mut.deserialize_header(log_data, data_len, pos))) {
          YYSYS_LOG(ERROR, "deserialize header error, ret=%d", ret);
        }
        else if (OB_PART_TRANS_COMMIT == cmd) {
          mutator_time = mut.get_mutate_timestamp();
          data_checksum_before = mut.get_memtable_checksum_before_mutate();
          data_checksum_after = mut.get_memtable_checksum_after_mutate();
        }

        if (OB_SUCCESS == ret) {
          if (OB_SUCCESS != (ret = encode_dis_trans_type_and_id(response.body,
                                                                limit_size,
                                                                response.body_size,
                                                                log_type,
                                                                coordinator_tid))) {
            YYSYS_LOG(ERROR, "encode dis_trans_type_and_id error, ret=%d", ret);
          }
          else if (OB_SUCCESS != (ret = encode_data_header(response.body,
                                                           limit_size,
                                                           response.body_size,
                                                           log_seq,
                                                           mutator_time,
                                                           data_checksum_before,
                                                           data_checksum_after,
                                                           sql_count))) {
            YYSYS_LOG(ERROR, "encode data_header error, ret=%d", ret);
          }
        }
        if (OB_MEM_OVERFLOW == ret) {
          response.body_size = old_pos;
        }
        else {
          response.seq = (int64_t) log_seq > request.seq ? log_seq : request.seq;
          response.real_seq = response.seq;
          response.count++;
          count--;
        }
      }
      //��־�л�
      else if (OB_LOG_SWITCH_LOG == cmd) {
        if(is_big_log) {
          big_log_switch_num++;
          YYSYS_LOG(INFO,
                    "read log => SEQ: %lu Payload Length: %ld TYPE: %s",
                    response.seq,
                    data_len,
                    "BIG_LOG_SWITCH_LOG");
        }
        else {
          YYSYS_LOG(INFO,
                    "read log => SEQ: %lu Payload Length: %ld TYPE: %s",
                    response.seq,
                    data_len,
                    "OB_LOG_SWITCH_LOG");
          response.flag = LOG_SWITCH;
          //���ö�ȡ�������һ����־��
          response.seq = (int64_t)log_seq > request.seq ? log_seq : request.seq;
          break;
        }

      }
      //schema���
      else if (OB_UPS_SWITCH_SCHEMA == cmd || OB_UPS_SWITCH_SCHEMA_MUTATOR == cmd
               || OB_UPS_SWITCH_SCHEMA_NEXT == cmd || OB_UPS_WRITE_SCHEMA_NEXT == cmd) {
        //����schema���������ǰ����sqlֱ���˳���
        if( response.count > 0 ) break;
        YYSYS_LOG(INFO,
                  "read log => SEQ: %lu Payload Length: %ld, cmd:%d, TYPE: %s",
                  log_seq,
                  data_len,
                  cmd,
                  "OB_SCHEMA");
        ret = rebuild_schema(fd, cmd, log_seq, log_data, data_len);
        if (ret == OB_SUCCESS) {
          //response.flag = SCHEMA_SWITCH;
          //���ö�ȡ�������һ����־��
          response.seq = (int64_t)log_seq > request.seq ? log_seq : request.seq;
          break;
        }
      }
      else if (OB_LOG_NOP == cmd) {
        //todo nothing
        //���ö�ȡ�������һ����־��
        if(!is_big_log) {
          response.seq = (int64_t)log_seq > request.seq ? log_seq : request.seq;
        }
      }
      else {
        YYSYS_LOG(ERROR,
                  "read log => SEQ: %lu Payload Length: %ld TYPE: %s",
                  log_seq,
                  data_len,
                  "UNKOWN LOG");
      }
    }
  }
  this->succ_num+=1;
  if (OB_FILE_NOT_EXIST == ret) {
    response.flag = LOG_END;
    ret = OB_SUCCESS;
  }
  else if (OB_READ_NOTHING == ret) {
    response.flag = LOG_END;
    ret = OB_SUCCESS;
  }
  else if(OB_SCHEMA_ERROR == ret) {
    response.flag = SCHEMA_INVAILD;
    //schema �쳣�����´�capture��ȡ����schema
    //pull_schema(sockfd);
    ret = OB_SUCCESS;
  }
  else if(OB_MEM_OVERFLOW == ret) {
    response.flag = BUFFER_OVERFLOW;
    this->succ_num=0;//buf ������
    ret = OB_SUCCESS;
  }


  //���ö�ȡ�������һ����־��
  //response.seq = (int64_t)log_seq > request.seq ? log_seq : request.seq;
  //��Ӧ����
  if(OB_SUCCESS == ret) {
    YYSYS_LOG(DEBUG, "read log finished, then start send packet");
    if (OB_SUCCESS != (ret = send_packet(fd, response)))
    {
      YYSYS_LOG(ERROR, "send response error, ret=%d", ret);
    }
    else {
      if(response.body != NULL && response.body_size > 0) {
        memset(response.body, 0, response.body_size);
      }
      if(response.count > 0) {
        YYSYS_LOG(INFO,
                  "send response success, last read seq=%lu, sno=%d, count=%d ,cell_info_count=%d, size=%ld",
                  response.seq,
                  request.sno,
                  response.count,
                  cell_info_count,
                  response.body_size);
      }
      else {
        YYSYS_LOG(INFO,
                  "send empty response success, last read seq=%lu, sno=%d, count=%d",
                  response.seq,
                  request.sno,
                  response.count);
      }
    }
  }
  else {
    response.body = const_cast<char*>("error");
    response.body_size = (int64_t)strlen(response.body);
    response.flag = AGENT_ERROR;
    response.count = 0;
    if (OB_SUCCESS != (ret = send_packet(fd, response))) {
      YYSYS_LOG(ERROR, "send error response error, ret=%d", ret);
    }
  }

  //�ر���־�ļ�
  if (OB_SUCCESS == ret && reader.is_opened()) {
    ret = reader.close();
    if(OB_SUCCESS != ret) {
      YYSYS_LOG(ERROR, "log reader close error!");
    }
  }

  return ret;
}


//���ݸ�ʽΪ
int LogAgent::handler_pull_crc_request(int fd, char* log_dir, pull_request request)
{
  int ret = OB_SUCCESS;
  ObLogCommitPoint commit_point_;
  int64_t clog_point;

  //2Mlog
  bool is_big_log=false;
  int  big_log_switch_num=0;
  int  big_log_num=0;

  //��־��ȡ���
  ObLogReader log_reader;
  ObRepeatedLogReader reader;
  ObLogCursor end_cursor;

  LogCommand cmd;
  uint64_t log_seq = 0;
  char* log_data = NULL;
  int64_t data_len = 0;

  int log_name = atoi(request.log_name);//������ļ���
  uint64_t last_log_seq = (const uint64_t)request.seq;//��������һ�ε���־��

  //��ȡ����־��
  int count = request.count;

  int64_t limit_size = DATA_BUFFER_LEN;
  //��Ӧ
  pull_response response;
  response.sno = request.sno;
  response.type = PULL_LOG_CRC_RESPONSE;
  response.version = VERSION_N;
  response.seq = last_log_seq;
  response.real_seq = 0;
  response.flag = NORMAL;
  response.count = 0;
  response.body = sql_buf;
  response.body_size = 0;
  int32_t cell_info_count = 0;
  int64_t old_pos = response.body_size;
  int8_t log_type;
  //��ȡ��־����
  if (OB_SUCCESS != (ret = log_reader.init(&reader, log_dir, log_name, 0, false))) {
    YYSYS_LOG(ERROR, "ObLogReader init error[err=%d]", ret);
  }
  else if (OB_SUCCESS != (ret = commit_point_.init(ups_log_dir, "commit_point"))) {
    YYSYS_LOG(ERROR, "commit_point_file init error, ret=%d", ret);
  }

  //init log reader,
  if(last_log_seq > 0) {
    while (OB_SUCCESS == (ret = log_reader.read_log(cmd, log_seq, log_data, data_len))) {
      if(log_seq>=last_log_seq)
        break;
    }
    if(log_seq!=last_log_seq) {
      YYSYS_LOG(ERROR, "ObLogReader init error, last_log_seq[%lu] cur_log_seq[%lu]",
                last_log_seq,log_seq);
      ret=OB_INVALID_LOG;
    }
  }
  big_log_writer.reset_Last_tranID();//��ʼ��big log,last tran id
  if (OB_SUCCESS != (ret = commit_point_.get(clog_point))) {
    YYSYS_LOG(ERROR, "get_commit_point_func error, ret=%d", ret);
  }
  while (big_log_num < 1 && count > 0 && OB_SUCCESS == ret ) {
    if (log_seq > (const uint64_t) clog_point)
    {
      YYSYS_LOG(ERROR,
                "log_seq is bigger than commit_point, log_seq is %ld, commit_point is %ld",
                log_seq,
                clog_point);

      response.flag = LOG_END;
      break;
    }
    if(is_big_log==false && cell_info_count > SQL_BUFFER_LEN) {
      break;
    }

    if (OB_SUCCESS != (ret = log_reader.read_log(cmd, log_seq, log_data, data_len)) &&
        OB_FILE_NOT_EXIST != ret && OB_READ_NOTHING != ret && OB_LAST_LOG_RUINNED != ret) {
      YYSYS_LOG(ERROR, "ObLogReader read error[ret=%d]", ret);
    }
    else if (OB_LAST_LOG_RUINNED == ret) {
      YYSYS_LOG(ERROR, "last_log[%s] broken!", end_cursor.to_str());
    }
    else if (OB_FILE_NOT_EXIST == ret) {
      YYSYS_LOG(WARN, "log file not exist");
    }
    else if (OB_READ_NOTHING == ret) {
      //YYSYS_LOG(ERROR, "waiting log data");
      //���ö�ȡ�������һ����־��
      if(!is_big_log) {
        response.seq = (int64_t)log_seq > request.seq ? log_seq : request.seq;
      }
    }
    else if (OB_SUCCESS != (ret = log_reader.get_next_cursor(end_cursor))) {
      YYSYS_LOG(ERROR, "log_reader.get_cursor()=>%d",  ret);
    }
    else {
      current_file_id = log_name;
      //����2M��־����������
      //��ͨ��־
      if (OB_LOG_UPS_MUTATOR == cmd || OB_UPS_MUTATOR_PREPARE == cmd || OB_UPS_BIG_LOG_DATA == cmd) {
        //����2M��־����������
        if( OB_UPS_BIG_LOG_DATA == cmd ) {
          is_big_log=true;
          bool is_all_completed=false;
          if(OB_SUCCESS!=(ret=big_log_writer.package_big_log(log_data,data_len,is_all_completed))) {
            YYSYS_LOG(ERROR, "SEQ:%lu,package_big_log fail,ret=%d",log_seq,ret);
          }
          if(is_all_completed) {
            int32_t temp_len;
            big_log_writer.get_buffer(log_data,temp_len);
            data_len=(int64_t)temp_len;
            is_big_log=false;
            big_log_num++;
            YYSYS_LOG(DEBUG, "package_big_log completed SEQ:%lu ,len=%lu",log_seq,data_len);
            if(big_log_switch_num>0) {
              response.flag=BIG_LOG_SWITCH;
              response.flag+=big_log_switch_num;
            }
          }
          else {
            continue;
          }
        }
        //YYSYS_LOG(INFO, "read log => SEQ: %lu Payload Length: %ld TYPE: %s", log_seq, data_len, "OB_LOG_UPS_MUTATOR");
        //����sql
        vector<string> sqls;
        int64_t mutator_time = 0;
        int64_t data_checksum_before;
        int64_t data_checksum_after;
        old_pos = response.body_size;
        ObTransID coordinator_tid;
        int64_t pos = 0;
        if (OB_UPS_MUTATOR_PREPARE == cmd) {
          log_type = 1;
          ret = coordinator_tid.deserialize_for_prepare(log_data, data_len, pos);
        }
        else {
          log_type = 0;
        }
        if (OB_SUCCESS != (ret = rebuild_sql(log_seq, log_data, data_len, sqls, cmd))) {
          YYSYS_LOG(ERROR, "rebuild_sql error, ret=%d", ret);
        }
        else if(sqls.size() <= 0) {
          YYSYS_LOG(DEBUG, "rebuild_sql maybe error, count:%d", (int32_t)sqls.size());
        }
        else if(sqls.size() > 0 && OB_SUCCESS != (ret = get_checksum(log_seq,
                                                                     log_data,
                                                                     data_len,
                                                                     mutator_time,
                                                                     data_checksum_before,
                                                                     data_checksum_after,
                                                                     cmd))) {
          YYSYS_LOG(ERROR, "get_checksum error, ret=%d", ret);
        }
        else if (OB_UPS_MUTATOR_PREPARE == cmd && OB_SUCCESS != (ret =
                                                                 encode_dis_trans_type_and_id(response
                                                                                              .body,
                                                                                              limit_size,
                                                                                              response
                                                                                              .body_size,
                                                                                              log_type,
                                                                                              coordinator_tid))) {
          YYSYS_LOG(ERROR, "encode trans id error, ret=%d", ret);
        }
        else if ((OB_LOG_UPS_MUTATOR == cmd || OB_UPS_BIG_LOG_DATA == cmd)
                 && OB_SUCCESS
                 != (ret = encode_log_type(response.body, limit_size, response.body_size, log_type))) {
          YYSYS_LOG(ERROR, "encode_log_type error, ret=%d", ret);
        }
        else if(sqls.size() > 0 && OB_SUCCESS != (ret = encode_data_header(response.body,
                                                                           limit_size,
                                                                           response.body_size ,
                                                                           log_seq,
                                                                           mutator_time,
                                                                           data_checksum_before,
                                                                           data_checksum_after,
                                                                           0))) {
          YYSYS_LOG(ERROR, "encode_data_header error, ret=%d", ret);
        }
        else {
          response.count++;
          cell_info_count+=(int32_t)sqls.size();
          count--;//��־����
        }
        //buffer����
        if(OB_MEM_OVERFLOW == ret) {
          //���������л�������
          response.body_size = old_pos;
        }
        else {
          //���ö�ȡ�������һ����־��
          response.seq = (int64_t)log_seq > request.seq ? log_seq : request.seq;
          //��ǰmutator��SQL������real_seq
          if(sqls.size() > 0)
            response.real_seq = response.seq;
        }
      }
      else if (OB_PART_TRANS_COMMIT == cmd || OB_PART_TRANS_ROLLBACK == cmd) {
        old_pos = response.body_size;
        if (OB_PART_TRANS_COMMIT == cmd) {
          log_type = 2;
        }
        else {
          log_type = 3;
        }
        int64_t mutator_time = 0;
        int64_t data_checksum_before = 0;
        int64_t data_checksum_after = 0;
        int32_t sql_count = 0;
        int64_t pos = 0;
        ObTransID coordinator_tid;
        ObUpsMutator mut;
        if (schema.get_version() <= 0 || schema_flag < 0) {
          ret = OB_SCHEMA_ERROR;
          YYSYS_LOG(WARN, "No Schema");
        }
        else if (OB_SUCCESS != (ret = coordinator_tid.deserialize_for_prepare(log_data, data_len, pos))) {
          YYSYS_LOG(ERROR, "deserialize tid error, ret=%d", ret);
        }
        else if (OB_PART_TRANS_COMMIT == cmd
                 && OB_SUCCESS != (ret = mut.deserialize_header(log_data, data_len, pos))) {
          YYSYS_LOG(ERROR, "deserialize header error, ret=%d", ret);
        }
        else if (OB_PART_TRANS_COMMIT == cmd) {
          mutator_time = mut.get_mutate_timestamp();
          data_checksum_before = mut.get_memtable_checksum_before_mutate();
          data_checksum_after = mut.get_memtable_checksum_after_mutate();
        }

        if (OB_SUCCESS == ret) {
          if (OB_SUCCESS != (ret = encode_dis_trans_type_and_id(response.body,
                                                                limit_size,
                                                                response.body_size,
                                                                log_type,
                                                                coordinator_tid))) {
            YYSYS_LOG(ERROR, "encode dis_trans_type_and_id error, ret=%d", ret);
          }
          else if (OB_SUCCESS != (ret = encode_data_header(response.body,
                                                           limit_size,
                                                           response.body_size,
                                                           log_seq,
                                                           mutator_time,
                                                           data_checksum_before,
                                                           data_checksum_after,
                                                           sql_count))) {
            YYSYS_LOG(ERROR, "encode data_header error, ret=%d", ret);
          }
        }
        if (OB_MEM_OVERFLOW == ret) {
          response.body_size = old_pos;
        }
        else {
          response.seq = (int64_t) log_seq > request.seq ? log_seq : request.seq;
          response.real_seq = response.seq;
          response.count++;
          count--;
        }
      }
      //��־�л�
      else if (OB_LOG_SWITCH_LOG == cmd) {
        if(is_big_log) {
          big_log_switch_num++;
          YYSYS_LOG(INFO,
                    "read log => SEQ: %lu Payload Length: %ld TYPE: %s",
                    request.seq,
                    data_len,
                    "BIG_LOG_SWITCH_LOG");
        }
        else {
          YYSYS_LOG(INFO,
                    "read log => SEQ: %lu Payload Length: %ld TYPE: %s",
                    request.seq,
                    data_len,
                    "OB_LOG_SWITCH_LOG");
          response.flag = LOG_SWITCH;
          //���ö�ȡ�������һ����־��
          response.seq = (int64_t)log_seq > request.seq ? log_seq : request.seq;
          break;
        }
      }
      //schema���
      else if (OB_UPS_SWITCH_SCHEMA == cmd || OB_UPS_SWITCH_SCHEMA_MUTATOR == cmd
               || OB_UPS_SWITCH_SCHEMA_NEXT == cmd || OB_UPS_WRITE_SCHEMA_NEXT == cmd) {
        //����schema���������ǰ����sqlֱ���˳���
        if( response.count > 0 ) break;
        YYSYS_LOG(INFO,
                  "read log => SEQ: %lu Payload Length: %ld, cmd:%d, TYPE: %s",
                  log_seq,
                  data_len,
                  cmd,
                  "OB_SCHEMA");
        ret = rebuild_schema(fd, cmd, log_seq, log_data, data_len);
        if (ret == OB_SUCCESS) {
          //response.flag = SCHEMA_SWITCH;
          //���ö�ȡ�������һ����־��
          response.seq = (int64_t)log_seq > request.seq ? log_seq : request.seq;
          break;
        }
      }
      else if (OB_LOG_NOP == cmd) {
        //todo nothing
        //���ö�ȡ�������һ����־��
        if(!is_big_log)
        {
          response.seq = (int64_t)log_seq > request.seq ? log_seq : request.seq;
        }
      }
      else {
        YYSYS_LOG(ERROR,
                  "read log => SEQ: %lu Payload Length: %ld TYPE: %s",
                  log_seq,
                  data_len,
                  "UNKOWN LOG");
      }
    }
  }

  if (OB_FILE_NOT_EXIST == ret) {
    response.flag = LOG_END;
    ret = OB_SUCCESS;
  }
  else if (OB_READ_NOTHING == ret) {
    response.flag = LOG_END;
    ret = OB_SUCCESS;
  }
  else if(OB_SCHEMA_ERROR == ret) {
    response.flag = SCHEMA_INVAILD;
    //schema �쳣�����´�capture��ȡ����schema
    //pull_schema(sockfd);
    ret = OB_SUCCESS;
  }
  else if(OB_MEM_OVERFLOW == ret) {
    response.flag = BUFFER_OVERFLOW;
    ret = OB_SUCCESS;
  }


  //���ö�ȡ�������һ����־��
  //response.seq = (int64_t)log_seq > request.seq ? log_seq : request.seq;
  //��Ӧ����
  if(OB_SUCCESS == ret) {
    if (OB_SUCCESS != (ret = send_packet(fd, response))) {
      YYSYS_LOG(ERROR, "send response error, ret=%d", ret);
    }
    else {
      if(response.body != NULL && response.body_size > 0) {
        memset(response.body, 0, response.body_size);
      }
      if(response.count > 0) {
        YYSYS_LOG(INFO,
                  "send response success, last read seq=%lu, sno=%d, count=%d ,cell_info_count=%d, size=%ld",
                  request.seq,
                  request.sno,
                  response.count,
                  cell_info_count,
                  response.body_size);
      }
      else {
        YYSYS_LOG(INFO,
                  "send empty response success, last read seq=%lu, sno=%d, count=%d",
                  request.seq,
                  request.sno,
                  response.count);
      }
    }
  }
  else {
    response.body = const_cast<char*>("error");
    response.body_size = (int64_t)strlen(response.body);
    response.flag = AGENT_ERROR;
    response.count = 0;
    if (OB_SUCCESS != (ret = send_packet(fd, response))) {
      YYSYS_LOG(ERROR, "send error response error, ret=%d", ret);
    }
  }

  //�ر���־�ļ�
  if (OB_SUCCESS == ret && reader.is_opened()) {
    ret = reader.close();
    if(OB_SUCCESS != ret) {
      YYSYS_LOG(ERROR, "log reader close error!");
    }
  }

  return ret;
}


int LogAgent::parse_request(char* buf, int64_t data_len, pull_request &request)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int32_t length = 0;//���ݳ���
  ret = serialization::decode_i32(buf, data_len, pos, &length);
  if (ret != OB_SUCCESS || data_len - 4 != length) {
    YYSYS_LOG(INFO, "buffer not compelete, data_len=%ld , length=%d!" ,data_len ,length);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_i32(buf, data_len, pos, &request.sno))) {
    YYSYS_LOG(INFO, "deserialize sno error! (buf=%p[%ld-%ld])=>%d", buf, pos, data_len, ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_i32(buf, data_len, pos, &request.type))) {
    YYSYS_LOG(INFO, "deserialize type error! (buf=%p[%ld-%ld])=>%d", buf, pos, data_len, ret);
  }
  else {
    int32_t log_name_len;
    if (OB_SUCCESS == (ret = serialization::decode_i32(buf, data_len, pos, &log_name_len)))
    {
      request.log_name = new char[log_name_len + 1];
      memcpy(request.log_name, buf + pos, log_name_len);
      request.log_name[log_name_len] = '\0';
      pos += log_name_len;


      if (OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, pos, &request.seq))) {
        YYSYS_LOG(INFO, "deserialize seq error! (buf=%p[%ld-%ld])=>%d", buf, pos, data_len, ret);
      }
      else if (OB_SUCCESS != (ret = serialization::decode_i32(buf, data_len, pos, &request.version))) {
        YYSYS_LOG(INFO, "deserialize version error! (buf=%p[%ld-%ld])=>%d", buf, pos, data_len, ret);
      }
      else if (OB_SUCCESS != (ret = serialization::decode_i32(buf, data_len, pos, &request.count))) {
        YYSYS_LOG(INFO, "deserialize count error! (buf=%p[%ld-%ld])=>%d", buf, pos, data_len, ret);
      }
      else {
        int32_t extend_len;
        if (OB_SUCCESS != (ret = serialization::decode_i32(buf, data_len, pos, &extend_len))) {
          YYSYS_LOG(INFO, "deserialize extend_len error! (buf=%p[%ld-%ld])=>%d", buf, pos, data_len, ret);
        }
        else {
          request.extend = new char[extend_len];
          request.extend_size = extend_len;
          memcpy(request.extend, buf + pos, extend_len);
          pos += extend_len;
        }

        //fprintf(stdout, "sno=%d, type=%d, log_name=%s, seq=%d, version=%d, count=%d, extend_len=%d, ret=%d (pos=%ld,length=%d,data_len=%ld)" ,request.sno, request.type, request.log_name, request.seq, request.version, request.count, extend_len, ret, pos, length, data_len);
      }
    }
  }
  return ret;

}

ssize_t LogAgent::readn(int fd, void *vptr, size_t n)
{
  size_t nleft;
  ssize_t nread;
  char *ptr;
  ptr = (char*)vptr;
  nleft = n;
  while (nleft > 0) {
    if((nread = read(fd, ptr, nleft)) < 0) {
      if(errno == EINTR)
        nread = 0;
      else
        return -1;
    }
    else if(nread == 0)  {
      break;
    }
    nleft -= nread;
    ptr += nread;
  }
  return n - nleft;
}

ssize_t LogAgent::writen(int fd,const void *vptr, size_t n)
{
  size_t nleft;
  ssize_t nwrite;
  const char *ptr;
  ptr = (const char*)vptr;
  nleft = n;
  while (nleft > 0) {
    if((nwrite = write(fd, ptr, nleft)) <= 0) {
      if(nwrite < 0 && errno == EINTR)
        nwrite = 0;
      else
        return -1;
    }

    nleft -= nwrite;
    ptr += nwrite;
  }
  return n;
}

/**
 * @brief ��ȡ������һ�����ݰ�
 * @param fd
 * @param vptr
 * @param n
 * @return
 */
int LogAgent::receive_packet(int fd, pull_request &request)
{
  char* buf = new char[RW_BUFFER_LEN];
  int32_t length = 0;//���ݳ���
  int64_t length_data_len = 4;//ǰ4���ֽڱ�ʾ���ĳ���
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  if((int64_t)readn(fd, buf, (size_t)length_data_len) != length_data_len)//�ȶ�ȡǰ4���ֽ�
  {
    YYSYS_LOG(ERROR, "readn length error!");
    ret = OB_ERROR;
  }
  else if(OB_SUCCESS != serialization::decode_i32(buf, length_data_len, pos, &length))//��������
  {
    YYSYS_LOG(ERROR, "decode packet length error!");
    ret = OB_ERROR;
  }
  else if((int32_t)readn(fd, buf + length_data_len, (size_t)length) != length)//��ȡ��ʵ�İ�����
  {
    YYSYS_LOG(ERROR, "readn data error!");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != parse_request(buf, length + length_data_len, request))//�������ݰ�Ϊrequest�ṹ��
  {
    YYSYS_LOG(ERROR, "parse_request error!");
    ret = OB_ERROR;
  }
  delete[] buf;
  return ret;
}

int LogAgent::send_packet(int fd, const pull_response response)
{
  //cond_.lock();//��ֹ������������
  int ret = OB_SUCCESS;
  //char* send_buf = new char[WRITE_BUFFER_LEN];
  int64_t size = 0;

  if(OB_SUCCESS != (ret = build_response(socket_send_buf, response, size))) {
    YYSYS_LOG(ERROR, "build_response error!");
  }
  else if((int32_t)writen(fd, socket_send_buf, size) != size) {
    YYSYS_LOG(ERROR, "send packet error!");
    ret = DISCONNECTED;
  }
  //delete[] send_buf;
  memset(socket_send_buf, 0, size);

  return ret;
}

int LogAgent::send_schema(int fd, uint64_t seq, CommonSchemaManagerWrapper &tmpschema)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;

  //ȷ����С������ȷ
  if (tmpschema.get_impl_ref().is_completion()){
    tmpschema.get_impl_ref().set_serialize_whole();
  }
  int64_t buffer_len = tmpschema.get_impl_ref().get_serialize_size();
  YYSYS_LOG(INFO, "length of schema:%ld, need_next_serialize:%d, is_completion:%d", buffer_len,
            tmpschema.get_impl_ref().need_next_serialize(), tmpschema.get_impl_ref().is_completion());

  pull_response response;
  response.sno = 0;
  response.type = SCHEMA_SWITCH_RESPONSE;
  response.seq = seq;
  response.real_seq = 0;
  response.version = VERSION_N;
  response.count = 0;
  response.flag = NORMAL;//����Ϊ������ļ���
  response.body = new char[buffer_len];
  response.body_size = 0;

  ObSchemaManagerV2 *schema_manager = tmpschema.schema_mgr_impl_;
  if (schema_manager == NULL){
    YYSYS_LOG(ERROR, "schema get schema_mgr_impl_ failed, ret=%d", ret);
    ret = OB_ERROR;
  }
  else if(OB_SUCCESS != (ret = schema_manager->serialize(response.body, buffer_len, pos))) {
    YYSYS_LOG(ERROR, "schema_manager serialize failed, body=%p, pos=%ld, ret=%d", response.body, pos, ret);
  }
  else {
    response.body_size = pos;
    if(OB_SUCCESS != (ret = send_packet(fd, response))) {
      YYSYS_LOG(ERROR,
                "send schema failed, size=%ld bytes, current_file_id=%d, ret=%d",
                response.body_size,
                current_file_id,
                ret);
    }
    else {
      YYSYS_LOG(INFO,
                "send schema success, size:%ld bytes, current_file_id=%d",
                response.body_size,
                current_file_id);
    }
  }
  delete[] response.body;
  return ret;
}


//################################�����ķָ��� 3#########################################

/**
 * @brief �ź����Ĵ���
 * @param sig
 */
void handle(const int sig)
{
  switch(sig) {
    case SIGHUP:
      YYSYS_LOG(INFO, "receive signal sig=%d, ignore this!", sig);
      break;
    case SIGTERM:
      HandleTerm();
      break;
    case SIGINT:
      HandleTerm();
      break;
    case SIGPIPE:
      YYSYS_LOG(INFO, "receive signal sig=%d, ignore this!", sig);
      break;
    case SIGCHLD:
      YYSYS_LOG(INFO, "receive signal sig=%d, wait it", sig);
      Wait();
      break;
    case 41:
      YYSYS_LOGGER._level++;
      YYSYS_LOG(INFO, "receive signal sig=%d, YYSYS_LOGGER._level: %d", sig, YYSYS_LOGGER._level);
      break;
    case 42:
      YYSYS_LOGGER._level--;
      YYSYS_LOG(INFO, "receive signal sig=%d, YYSYS_LOGGER._level: %d", sig, YYSYS_LOGGER._level);
      break;
    default:
      YYSYS_LOG(INFO, "receive signal sig=%d, please check!", sig);
      //exit(0);
      break;
  }

}

char *trim(char *str)
{
  char *ptr;
  if(str == NULL)
  {
    return NULL;
  }
  ptr = str + strlen(str) - 1;

  while(isspace(*str))
    str++;
  while((ptr > str) && isspace(*ptr))
    *ptr-- = '\0';

  return str;
}

void LogAgent::print_version()
{
  fprintf(stderr, "BUILD_TIME: %s %s\n\n", __DATE__, __TIME__);
}

void LogAgent::print_usage(const char *prog_name)
{
  fprintf(stderr, "%s\n"
          "\t-b|--sql buffer MB\n"
          "\t-d|--ups commit log dir, must input\n"
          "\t-l|--agent log filename with dirpath \n"
          "\t-p|--listen port, must input\n"
          "\t-v|--version\n"
          "\t-h|--help\n",prog_name);
}

void LogAgent::parse_filter_file(const char *dir)
{
  FILE *fp = NULL;
  if(NULL == (fp = fopen(dir, "r")))
  {
    YYSYS_LOG(ERROR, "open filter files:%s fail", dir);
  }
  else
  {
    char log_info[120] = {0};
    uint64_t table_id = 0;
    char tablename[100] = {0};

    while(NULL != fgets(log_info, 120, fp))
    {
      if(strlen(trim(log_info)) <= 0)
      {
        continue;
      }
      sscanf(log_info, "%[^:]:%lu", tablename, &table_id);

      if (-1 == tableidset.set(table_id))
      {
        YYSYS_LOG(ERROR, "set table_id:%lu to set fail", table_id);
      }
    }
    fclose(fp);
  }
}

void LogAgent::parse_cmd_line(const int argc,  char *const argv[])
{
  int opt = 0;
  const char* opt_string = "b:d:f:l:m:p:vh";
  struct option longopts[] =
  {
  {"buffer", 1, NULL, 'b'},
  {"dir", 1, NULL, 'd'},
  {"usr_filter_dir", 1, NULL, 'f'},
  {"agent_log_dir", 1, NULL, 'l'},
  {"master", 1, NULL, 'm'},
  {"port", 1, NULL, 'p'},
  {"version", 1, NULL, 'v'},
  {"help", 1, NULL, 'h'},
  {0, 0, 0, 0}
};
  bool set_port = false;
  bool set_dir = false;
  while((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1) {
    switch (opt) {
      case 'b':
        DEFAULT_BUFFER_LEN = 1024*1024*static_cast<int64_t>(strtol(optarg, NULL, 0));
        this->balanceBuffer();
        break;
        //      case 'm':
        //        snprintf(master_ip, OB_IP_STR_BUFF, "%s", optarg);
        //        set_ip = true;
        //        break;
      case 'p':
        master_port = static_cast<uint16_t>(strtol(optarg, NULL, 0));
        set_port = true;
        break;
      case 'd':
        snprintf(ups_log_dir, sizeof (ups_log_dir), "%s", optarg);
        set_dir = true;
        break;
      case 'f':
        snprintf(usr_filter_dir, sizeof(usr_filter_dir), "%s", optarg);
        parse_filter_file(usr_filter_dir);
        break;

      case 'l':
        snprintf(agent_log_dir, sizeof(agent_log_dir), "%s", optarg);
        break;
      case 'v':
        print_version();
        exit(0);
      case 'h':
        print_usage(basename(argv[0]));
        exit(0);
      default:print_usage(basename(argv[0]));
        exit(-1);
    }
  }

  if(!set_port || !set_dir){
    print_usage(basename(argv[0]));
    exit(1);
  }

}
void checkCmd(const int argc,  char *const argv[])
{
  if(argc==2) {
    string op=argv[1];
    if(op=="-v"||op=="-V") {
      printf("BUILD_TIME: %s %s\n\n", __DATE__, __TIME__);
      printf("CDCAgent GIT_VERSION: %s\n", git_version());
      printf("BUILD_TIME: %s %s\n", build_date(), build_time());
      printf("BUILD_FLAGS: %s\n\n", build_flags());
    }
    else {
      printf("%s\n"
             "\t-b|--sql buffer MB\n"
             "\t-d|--ups commit log dir, must input\n"
             "\t-l|--agent log dir \n"
             "\t-f|--user filter file \n"
             "\t-p|--listen port, must input\n"
             "\t-v|--version\n"
             "\t-h|--help\n",argv[0]);
    }
    exit(1);
  }
  int fd = open("logs", 0);
  if (fd != -1) close(fd);
  else system("mkdir logs");
}

void
HandleTerm()
{
  YYSYS_LOG(INFO, "parent process exit, pid:%d", getpid());
  sigset(SIGCLD, SIG_IGN);
  int i = 0;
  for ( i = 0; i < MAX_CHD_NUM; i++) {
    if (sonPid[i] > 0)
    {
      kill(sonPid[i], SIGUSR2);
    }

    sleep(1);

    while (waitpid(-1, NULL, WNOHANG) > 0) {
      ;
    }
    exit(1);
  }
}

void
Wait()
{
  int nPid, Status;
  sighold(SIGCLD);
  while (TRUE) {
    nPid = waitpid((pid_t)(-1), &Status, WNOHANG | WUNTRACED);
    if (nPid > 0) {
      if (--currChdNum == 0) {};
    }
    else
    {
      break;
    }
  }

  int i = 0;
  for (i = 0; i < MAX_CHD_NUM; i++) {
    if (kill(sonPid[i], 0) == -1) {
      sonPid[i] = -1;
    }
  }
  sigrelse(SIGCLD);
}

void
HandleExit()
{
  YYSYS_LOG(INFO, "child process exit, pid:%d", getpid());
  exit(1);
}

/**
 * @brief ������
 * @param argc
 * @param argv
 * @return
 */
int main(int argc, char* argv[])
{
  checkCmd(argc, argv);
  signal(SIGABRT, handle);
  signal(SIGTERM, handle);
  signal(SIGINT, handle);
  signal(SIGPIPE, handle);
  signal(SIGHUP, handle);
  signal(SIGCHLD, handle);
  signal(41, handle);
  signal(42, handle);

  //  int ret = 0;

  //  YYSYS_LOGGER.setLogLevel("INFO");
  //  YYSYS_LOGGER.setFileName("logs/agent.log");
  //  YYSYS_LOGGER.setMaxFileSize(MAX_LOG_SIZE);
  //��ʼ���ڴ��
  ob_init_memory_pool();
  //===============�ػ��߳�==================

  for (int i = 0; i < MAX_CHD_NUM; i++) {
    sonPid[i] = -1;
  }

  LogAgent agent;
  return agent.start(argc, argv);
}
