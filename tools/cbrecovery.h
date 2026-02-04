#ifndef CBRECOVERY_H
#define CBRECOVERY_H
#include <map>
#include <string>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <getopt.h>
#include <sys/types.h>
#include <signal.h>

#include <unistd.h>
#include <netdb.h>

#include "common/hash/ob_hashmap.h"
#include "common/hash/ob_hashset.h"
#include "common/ob_transaction.h"
#include "common/ob_array.h"
#include "updateserver/ob_ups_mutator.h"
#include "updateserver/ob_ups_utils.h"
#include "updateserver/ob_big_log_writer.h"
#include "updateserver/ob_on_disk_log_locator.h"

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::updateserver;
using namespace std;

struct hash_string
{
    int64_t operator()(const std::string &key) const
    {
      return murmurhash2(key.c_str(), (int32_t)key.length(), 0);
    }
};

typedef ObHashSet<ObTransID, hash::NoPthreadDefendMode> TransSet;
typedef ObHashMap<std::string, int64_t, hash::NoPthreadDefendMode, hash_string> BreakPoint;
typedef ObHashMap<std::string, int, hash::NoPthreadDefendMode, hash_string> IpPortmap;
typedef ObHashMap<std::string, std::string, hash::NoPthreadDefendMode, hash_string> DirSstable;

#define TRANS_BUCKET 1024

static int64_t DEFAULT_BUFFER_LEN = 1024 * 1024 * 512;
static int64_t WRITE_BUFFER_LEN = DEFAULT_BUFFER_LEN + 44;
static const int64_t RW_BUFFER_LEN = 1024 * 1024 * 16;

#define DISCONNECTED -1
#define TRANS_SET_INIT 1001
#define TRANS_SET_SEARCH 1002
#define START_CUTTING 1003
#define NO_CUTTING 1004

int serialization_TransSet(char *buf, const int64_t data_len, int64_t &pos, TransSet *tran_set)
{
  int ret = OB_SUCCESS;
  if (NULL == tran_set)
  {
    YYSYS_LOG(INFO, "TransSet is null");
  }
  else
  {
    TransSet::iterator iter = tran_set->begin();
    for (; iter != tran_set->end(); iter++)
    {
      ObTransID tid = iter->first;
      if (OB_SUCCESS != (ret = tid.serialize(buf, data_len, pos)))
      {
        YYSYS_LOG(ERROR, "Error occured when serialize coordinator tid, ret=%d", ret);
      }
    }
  }
  return ret;
}

int deserialization_TransSet(const char *buf, const int64_t data_len, int64_t &pos, TransSet *tran_set, int64_t size)
{
  int ret = OB_SUCCESS;
  if (NULL == tran_set)
  {
    YYSYS_LOG(INFO, "TransSet is null");
  }
  else
  {
    for (int i = 0; i < size; i++)
    {
      ObTransID tid;
      if (OB_SUCCESS != (ret = tid.deserialize(buf, data_len, pos)))
      {
        YYSYS_LOG(ERROR, "Error occured when deserialize tid, ret=%d", ret);
      }
      else if (-1 == tran_set->set(tid))
      {
        ret = OB_ERROR;
        YYSYS_LOG(ERROR, "Fail to set tid to tran_set");
      }
    }
  }
  return ret;
}

bool is_all_digit(const char *str)
{
  bool ret = true;
  for (int i = 0; ; i++)
  {
    if (str[i] == '\0')
    {
      break;
    }
    else if (str[i] < '0' || str[i] > '9')
    {
      ret = false;
      break;
    }
  }
  return ret;
}

struct PACKET
{
  int32_t sno;              //���к�
  int32_t type;             //����
  int64_t mutate_timestamp; //ʱ���(�״β���Ϊ�����ļ��е�ʱ��㣬����ȡʱ���)
  int64_t seq;              //����ȷ����ȡ����־��
  int64_t ts_size;          //�������
  TransSet *ts;             //����
};

class Agent
{
  public:
    Agent();
    void reset();
    int start_server();
    int start_client(const char *src_file);
    int parse_server_cmd(const int argc, char *const argv[]);
    int handle_packet(int sockfd);
    int balanceBuffer();
    DISALLOW_COPY_AND_ASSIGN(Agent);
  private:
    int server_sock;
    char *socket_send_buf;
    char *commit_log_dir;
    char *sstable_root_dir;
    uint64_t last_end_seq;
    uint64_t max_major_freezing_min_clog_id;
    int64_t local_dis_trans_num;
    bool need_delete_sstable;
    int port;
    yysys::CThreadCond cond_;

  private:
    int build_packet(char *buf, const PACKET packet, int64_t &size);
    int init_ip_cuttrans(ObArray<string> &log_files, TransSet &cut_trans, uint64_t &cutted_seq, int64_t &mutate_timestamp);
    int get_ip_cuttrans(ObArray<string> &log_files, TransSet &cut_trans, int64_t& mutate_timestamp, ObArray<ObTransID> &before_exist_array);
    int init_log_cuttrans(const char *log_name, TransSet &cut_trans, uint64_t &cutted_seq, int64_t &mutate_timestamp, bool &earlist_log, bool seq_id, bool &begin_cut);
    int get_log_cuttrans(const char *log_name, TransSet &cut_trans, int64_t &mutate_timestamp, int64_t &dis_tran_num, ObArray<ObTransID> &before_exist_array);

    int parse_packet(char *buf, int64_t data_len, PACKET &packet);
    int handler_init_request(int fd, PACKET packet, ObArray<string> log_files);
    int handler_search_request(int fd, PACKET packet, ObArray<string> log_files, ObArray<ObTransID> &before_exist_array);
    int handler_cut_request(int fd, PACKET packet);
    ssize_t readn(int fd, void *vptr, size_t n);
    ssize_t writen(int fd, const void *vptr, size_t n);
    int receive_packet(int fd, PACKET &packet);
    int backup_log_file_and_clrpoint_file(int64_t start_copy_file_id, int64_t end_file_id, const char *time);
    int delete_log_file_and_clrpoint_file(int64_t start_copy_file_id, int64_t end_file_id, uint64_t start_cl_id);
    int backup_sstable(int64_t cut_log_id, DirSstable &sst_dir_map, const char *time, uint64_t &start_cl_id);
    int delete_sstable(int64_t cut_log_id, DirSstable &sst_dir_map);
    int setsock(int);
    int send_packet(int fd, const PACKET packet);
};

Agent::Agent()
{
  commit_log_dir = NULL;
  sstable_root_dir = NULL;
  need_delete_sstable = false;
  server_sock = -1;
  last_end_seq = 0;
  max_major_freezing_min_clog_id = 0;
  local_dis_trans_num = 0;
  port = -1;
  socket_send_buf = new char[WRITE_BUFFER_LEN];
  memset(socket_send_buf, 0, WRITE_BUFFER_LEN);
}

void Agent::reset()
{
  need_delete_sstable = false;
  last_end_seq = 0;
  max_major_freezing_min_clog_id = 0;
  local_dis_trans_num = 0;
  memset(socket_send_buf, 0, WRITE_BUFFER_LEN);
}

int Agent::setsock(int sdNew)
{
  struct linger soLinger;
  int soKeepAlive, soReuseAddr;

  soLinger.l_onoff = true;
  soLinger.l_linger = true;
  if (-1 == setsockopt(sdNew, SOL_SOCKET, SO_LINGER,
                       &soLinger, sizeof(soLinger))) {
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
                       (char *)&soReuseAddr, (int)sizeof(soReuseAddr))) {
    YYSYS_LOG(ERROR, "[setsockopt]set reuse addr option failed, errno[%d]", errno);
    return -1;
  }
  int chOpt = 1;
  if (-1 == setsockopt(sdNew, IPPROTO_TCP, TCP_NODELAY, &chOpt, sizeof(chOpt)))
  {
    YYSYS_LOG(ERROR, "[setsockopt]set TCP_NODELAY option failed, errno[%d]", errno);
    return -1;
  }
  return 0;
}

int Agent::build_packet(char *buf, const PACKET packet, int64_t &size)
{
  int ret = OB_SUCCESS;
  int64_t pos = 4;
  int header_size = 32;
  if (header_size + packet.ts_size > WRITE_BUFFER_LEN) {
    ret = OB_ERROR;
    YYSYS_LOG(ERROR,
              "buffer size not enough, fixed size=%ld, need size=%ld",
              WRITE_BUFFER_LEN,header_size + packet.ts_size);
  }
  if (OB_SUCCESS != (ret = serialization::encode_i32(buf, WRITE_BUFFER_LEN, pos, packet.sno))) {
    YYSYS_LOG(ERROR, "fail to serialize sno. ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_i32(buf, WRITE_BUFFER_LEN, pos, packet.type))) {
    YYSYS_LOG(ERROR, "fail to serialize type. ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_i64(buf, WRITE_BUFFER_LEN, pos, packet.mutate_timestamp))) {
    YYSYS_LOG(ERROR, "fail to serialize version. ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_i64(buf, WRITE_BUFFER_LEN, pos, packet.seq))) {
    YYSYS_LOG(ERROR, "fail to serialize seq. ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_i64(buf, WRITE_BUFFER_LEN, pos, packet.ts_size))) {
    YYSYS_LOG(ERROR, "fail to serialize size. ret=%d", ret);
  }
  else
  {
    if (OB_SUCCESS != (ret = serialization_TransSet(buf, WRITE_BUFFER_LEN, pos, packet.ts))) {
      YYSYS_LOG(INFO, "serialize ts_len error! (buf=%p[%ld])=>%d", buf, pos, ret);
    }
    int64_t temp_pos = 0;
    if (OB_SUCCESS != (ret = serialization::encode_i32(buf,
                                                       WRITE_BUFFER_LEN, temp_pos, (int32_t)(pos - 4)))) //first 4 byte write length
    {
      YYSYS_LOG(ERROR, "fail to serialize len. ret=%d", ret);
    }
    else {
      size = pos;
    }
  }
  return ret;
}

int Agent::parse_packet(char *buf, int64_t data_len, PACKET &packet)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int32_t length = 0;
  ret = serialization::decode_i32(buf, data_len, pos, &length);
  if (ret != OB_SUCCESS || data_len -4 != length) {
    YYSYS_LOG(INFO, "buffer not compelete, data_len=%ld, length=%d!", data_len, length);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_i32(buf, data_len, pos, &packet.sno))) {
    YYSYS_LOG(INFO, "deserialize sno error! (buf=%p[%ld-%ld])=>%d", buf, pos, data_len, ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_i32(buf, data_len, pos, &packet.type))) {
    YYSYS_LOG(INFO, "deserialize type error! (buf=%p[%ld-%ld])=>%d", buf, pos, data_len, ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, pos, &packet.mutate_timestamp))) {
    YYSYS_LOG(INFO, "deserialize mutate_timestamp error! (buf=%p[%ld-%ld])=>%d", buf, pos, data_len, ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, pos, &packet.seq))) {
    YYSYS_LOG(INFO, "deserialize seq error! (buf=%p[%ld-%ld])=>%d", buf, pos, data_len, ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, pos, &packet.ts_size))) {
    YYSYS_LOG(INFO, "deserialize size error! (buf=%p[%ld-%ld])=>%d", buf, pos, data_len, ret);
  }
  else if (OB_SUCCESS != (ret = deserialization_TransSet(buf, data_len, pos, packet.ts, packet.ts_size))) {
    YYSYS_LOG(INFO, "deserialize ts_len error!");
  }
  return ret;
}

ssize_t Agent::readn(int fd, void *vptr, size_t n)
{
  size_t nleft;
  ssize_t nread;
  char *ptr;
  ptr = (char *)vptr;
  nleft = n;
  while (nleft > 0) {
    if ((nread = read(fd, ptr, nleft)) < 0)
    {
      if (errno == EINTR)
      {
        nread = 0;
      }
      else
      {
        return -1;
      }
    }
    else if (nread == 0)
    {
      break;
    }
    nleft -= nread;
    ptr += nread;
  }
  return n - nleft;
}

ssize_t Agent::writen(int fd, const void *vptr, size_t n)
{
  size_t nleft;
  ssize_t nwrite;
  const char *ptr;
  ptr = (const char *)vptr;
  nleft = n;
  while (nleft > 0) {
    if ((nwrite = write(fd, ptr, nleft)) <= 0)
    {
      if (nwrite <0 && errno == EINTR)
      {
        nwrite = 0;
      }
      else
      {
        return -1;
      }
    }
    nleft -= nwrite;
    ptr += nwrite;
  }
  return n;
}

int Agent::receive_packet(int fd, PACKET &packet)
{
  char *buf = new char[RW_BUFFER_LEN];
  int32_t length = 0;
  int64_t length_data_len = 4;
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  if ((int64_t) readn(fd, buf, (size_t)length_data_len) != length_data_len)
  {
    YYSYS_LOG(ERROR, "readn length error!");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != serialization::decode_i32(buf, length_data_len, pos, &length))
  {
    YYSYS_LOG(ERROR, "decode packet length error!");
    ret = OB_ERROR;
  }
  else if ((int32_t) readn(fd, buf + length_data_len, (size_t)length) != length)
  {
    YYSYS_LOG(ERROR, "readn data error!");
    ret = OB_ERROR;
  }
  else if (OB_SUCCESS != parse_packet(buf, length + length_data_len, packet))
  {
    YYSYS_LOG(ERROR, "parse_packet error!");
    ret = OB_ERROR;
  }
  delete[] buf;
  return ret;
}

int Agent::send_packet(int fd, const PACKET packet)
{
  int ret = OB_SUCCESS;
  int64_t size = 0;

  if (OB_SUCCESS != (ret = build_packet(socket_send_buf, packet, size))) {
    YYSYS_LOG(ERROR, "build_packet error!");
  }
  else if ((int32_t) writen(fd, socket_send_buf, size) != size) {
    YYSYS_LOG(ERROR, "send packet error!");
    ret = DISCONNECTED;
  }
  memset(socket_send_buf, 0, size);
  return ret;
}

#endif // CBRECOVERY_H
