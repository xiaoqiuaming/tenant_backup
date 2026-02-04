#include "utility.h"
#include "ob_ms_list.h"

using namespace oceanbase;
using namespace common;

//mod zhaoqiong [MultiUPS] [MS_Manage_Function] 20150611:b
MsList::MsList()
//    : rs_(), ms_list_(), ms_iter_(0), client_(NULL), buff_(),
//      rwlock_(), initialized_(false)
  : rs_(), cluster_id_(-1), ms_list_(),
    //add for [582-get local cluster ms]-b
    cluster_ms_list_(), cluster_ms_iter_(0), local_cluster_id_(-1),
    //add for [582-get local cluster ms]-e
    ms_iter_(0), client_(NULL), buff_(),
    rwlock_(), initialized_(false)
  //add lbzhong [Paxos Cluster.Flow.MS] 201607027:b
  , master_cluster_id_(OB_ALL_CLUSTER_FLAG)
  //add:e
  //mod:e
{
}

MsList::~MsList()
{
  clear();
}
//mod zhaoqiong [MultiUPS] [MS_Manage_Function] 20150611:b
//int MsList::init(const ObServer &rs, const ObClientManager *client, bool do_update)
//int MsList::init(const ObServer &rs, const ObClientManager *client, int32_t cluster_id, bool do_update)
int MsList::init(const ObServer &rs, const ObClientManager *client, int32_t cluster_id, bool do_update
                 ,int64_t local_cluster_id)
//mod:e
{
  int ret = OB_SUCCESS;
  if (0 == rs.get_ipv4() || 0 == rs.get_port() || NULL == client)
  {
    YYSYS_LOG(ERROR, "init error, arguments are invalid, rs=%s client=NULL",
              to_cstring(rs));
    ret = OB_INVALID_ARGUMENT;
  }
  else
  {
    rs_ = rs;
    //add zhaoqiong [MultiUPS] [MS_Manage_Function] 20150611:b
    cluster_id_ = cluster_id;
    //add:e
    client_ = client;
    initialized_ = true;
    local_cluster_id_ = local_cluster_id; //add for [582-get local cluster ms]
    //mod zhaoqiong [MultiUPS] [MS_Manage_Function] 20150611:b
    //    YYSYS_LOG(INFO, "MsList initialized succ, rs=%s client=%p",
    //            to_cstring(rs_), client_);
    YYSYS_LOG(INFO, "MsList initialized succ, rs=%s client=%p, cluster_id=%d",
              to_cstring(rs_), client_, cluster_id_);
    //mod:e
  }

  if (OB_SUCCESS == ret && do_update)
  {
    ret = update();
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(ERROR, "update ms list failed");
    }
  }

  return ret;
}

void MsList::clear()
{
  rs_ = ObServer();
  cluster_id_ = -1; //add zhaoqiong [MultiUPS] [MS_Manage_Function] 20150811
  ms_list_.clear();
  cluster_ms_list_.clear();//add for [582-get local cluster ms]
  atomic_exchange(&ms_iter_, 0);
  initialized_ = false;
}

int MsList::update()
{
  int ret = OB_SUCCESS;
  static const int32_t MY_VERSION = 1;
  const int64_t timeout = 1000000;

  update_mutex_.lock();

  std::vector<common::ObServer> new_ms;

  ObDataBuffer data_buff(buff_.ptr(), buff_.capacity());
  ObResultCode res;
  //add pangtianze [MultiUPS] [merge with paxos] 20170518:b
  cluster_id_ = OB_ALL_CLUSTER_FLAG;
  //add:e
  if (!initialized_)
  {
    ret = OB_NOT_INIT;
    YYSYS_LOG(ERROR, "MsList has not been initialized, "
              "this should not be reached");
  }
  //add zhaoqiong [MultiUPS] [MS_Manage_Function] 20150611:b
  else if (OB_SUCCESS != (ret = serialization::encode_vi32(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position(), cluster_id_)))
  {
    YYSYS_LOG(WARN, "failed to serialize cluster_id, err=%d", ret);
  }
  //add:e

  if (OB_SUCCESS == ret)
  {
    ret = client_->send_request(rs_, OB_GET_MS_LIST, MY_VERSION, timeout, data_buff);
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "failed to send request, ret=%d", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    data_buff.get_position() = 0;
    ret = res.deserialize(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position());
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "ObResultCode deserialize error, ret=%d", ret);
    }
  }

  int32_t ms_num = 0;
  if (OB_SUCCESS == ret)
  {
    ret = serialization::decode_vi32(data_buff.get_data(), data_buff.get_capacity(), data_buff.get_position(), &ms_num);
    if(OB_SUCCESS != ret)
    {
      YYSYS_LOG(WARN, "decode ms num fail:ret[%d]", ret);
    }
    else
    {
      YYSYS_LOG(DEBUG, "ms server number[%d]", ms_num);
    }
  }

  ObServer ms;
  int64_t reserved = 0;
  for(int32_t i = 0;i<ms_num && OB_SUCCESS == ret;i++)
  {
    if (OB_SUCCESS != (ret = ms.deserialize(data_buff.get_data(),
                                            data_buff.get_capacity(),
                                            data_buff.get_position())))
    {
      YYSYS_LOG(WARN, "deserialize merge server fail, ret: [%d]", ret);
    }
    else if (OB_SUCCESS !=
             (ret = serialization::decode_vi64(data_buff.get_data(),
                                               data_buff.get_capacity(),
                                               data_buff.get_position(),
                                               &reserved)))
    {
      YYSYS_LOG(WARN, "deserializ merge server"
                " reserver int64 fail, ret: [%d]", ret);
    }
    else
    {
      new_ms.push_back(ms);
    }
  }

  //add lbzhong [Paxos Cluster.Flow.MS] 201607027:b
  if(OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = serialization::decode_vi32(data_buff.get_data(), data_buff.get_capacity(),
                                                        data_buff.get_position(), &master_cluster_id_)))
    {
      YYSYS_LOG(WARN, "failed to decode master_cluster_id, err=%d", ret);
    }
  }
  //add:e

  update_mutex_.unlock();

  if (OB_SUCCESS == ret)
  {
    rwlock_.wrlock();
    if (!list_equal_(new_ms))
    {
      YYSYS_LOG(TRACE, "Mergeserver List is modified, get the most updated, "
                "MS num=%zu", new_ms.size());
      list_copy_(new_ms);
    }
    else
    {
      YYSYS_LOG(DEBUG, "Mergeserver List do not change, MS num=%zu",
                new_ms.size());
    }
    rwlock_.unlock();
  }

  return ret;
}

const ObServer MsList::get_one()
{
  ObServer ret;
  rwlock_.rdlock();
  if (ms_list_.size() > 0)
  {
    uint64_t i = atomic_inc(&ms_iter_);
    ret = ms_list_[i % ms_list_.size()];
  }
  rwlock_.unlock();
  return ret;
}

//add for [582-get local cluster ms]-b
const ObServer MsList::get_cluster_one()
{
  ObServer ret;
  if (cluster_ms_list_.size() > 0)
  {
    rwlock_.rdlock();
    uint64_t i = atomic_inc(&cluster_ms_iter_);
    ret = cluster_ms_list_[i % cluster_ms_list_.size()];
    rwlock_.unlock();
  }
  else
  {
    ret = get_one();
  }
  return ret;
}
//add for [582-get local cluster ms]-e

//add for [secondary index get old cchecksum opt]-b
bool MsList::is_local_ms_alive(const ObServer &cs, ObServer &ms)
{
  bool ret = false;
  rwlock_.rdlock();
  for (unsigned i = 0; i < ms_list_.size(); i++)
  {
    if (cs.is_same_ip(ms_list_[i]))
    {
      ms = ms_list_[i];
      ret = true;
      break;
    }
  }
  rwlock_.unlock();
  return ret;
}
//add for [secondary index get old cchecksum opt]-e

//add jinty [Paxos Cluster.Balance]20160708:b
void MsList::get_list(std::vector<ObServer> &list)
{
  list = ms_list_;
}
//add e
void MsList::runTimerTask()
{
  update();
}

bool MsList::list_equal_(const std::vector<ObServer> &list)
{
  bool ret = true;
  if (list.size() != ms_list_.size())
  {
    ret = false;
  }
  else
  {
    for (unsigned i = 0; i < ms_list_.size(); i++)
    {
      if (!(list[i] == ms_list_[i]))
      {
        ret = false;
        break;
      }
    }
  }
  return ret;
}

void MsList::list_copy_(const std::vector<ObServer> &list)
{
  ms_list_.clear();
  cluster_ms_list_.clear(); //add for [582-get local cluster ms]
  std::vector<ObServer>::const_iterator iter;
  for (iter = list.begin(); iter != list.end(); iter++)
  {
    ms_list_.push_back(*iter);
    YYSYS_LOG(TRACE, "Add Mergeserver %s", to_cstring(*iter));
    if (iter->cluster_id_ == local_cluster_id_)
    {
      cluster_ms_list_.push_back(*iter);
    }
  }
}
