#include "ob_chunk_server_task_dispatcher.h"
#include "ob_ms_tsi.h"
#include "common/ob_malloc.h"
#include "common/ob_crc64.h"
#include "common/murmur_hash.h"
#include "mergeserver/ob_merge_server_main.h" //add zhaoqiong [MultiUPS] [RootTable_Cache] 20150703:b

using namespace oceanbase;
using namespace oceanbase::mergeserver;
using namespace oceanbase::common;

ObChunkServerTaskDispatcher ObChunkServerTaskDispatcher::task_dispacher_;

ObChunkServerTaskDispatcher * ObChunkServerTaskDispatcher::get_instance()
{
  return &task_dispacher_;
}

ObChunkServerTaskDispatcher::ObChunkServerTaskDispatcher()
{
  local_ip_ = 0;
  using_new_balance_ = false;
}

void ObChunkServerTaskDispatcher::set_factor(const bool use_new_method)
{
  using_new_balance_ = use_new_method;
  YYSYS_LOG(INFO, "open new balance method:on[%d]", use_new_method);
}

ObChunkServerTaskDispatcher::~ObChunkServerTaskDispatcher()
{
}

int32_t ObChunkServerTaskDispatcher::select_cs(ObTabletLocationList & list)
{
  //add zhaoqiong [MultiUPS] [RootTable_Cache] 20150703:b
  int32_t cluster_id = (int32_t)ObMergeServerMain::get_instance()->get_merge_server().get_config().cluster_id;
  //add:e
  int32_t ret = 0;
  if (using_new_balance_)
  {
    //mod zhaoqiong [MultiUPS] [RootTable_Cache] 20150703:b
    //ret = new_select_cs(list);
    ret = new_select_cs(list,cluster_id);
    //mod:e
  }
  else
  {
    //mod zhaoqiong [MultiUPS] [RootTable_Cache] 20150703:b
    //ret = old_select_cs(list);
    ret = old_select_cs(list,cluster_id);
    //mod:e
  }
  return ret;
}

//mod zhaoqiong [MultiUPS] [RootTable_Cache] 20150703:b
//int32_t ObChunkServerTaskDispatcher::old_select_cs(ObTabletLocationList & list)
int32_t ObChunkServerTaskDispatcher::old_select_cs(ObTabletLocationList & list,int32_t cluster_id)
//mod:e
{
  int32_t list_size = static_cast<int32_t>(list.size());
  OB_ASSERT(0 < list_size);
  //add zhaoqiong [MultiUPS] [RootTable_Cache] 20150703:b
  int32_t ret = 0;
  int32_t rand_offset = 0;
  if (list[0].server_.cluster_id_ == cluster_id)
  {
    YYSYS_LOG(INFO, " using old balance method, get chunk server from cluster [%d]", cluster_id);
    int32_t cs_num_in_cluster = 0;
    for (int32_t i = 0; i < list_size ; ++i)
    {
      if (list[i].server_.cluster_id_ == cluster_id)
      {
        cs_num_in_cluster++;
      }
      else
        break;
    }
    ret = static_cast<int32_t>(random() % cs_num_in_cluster);
    rand_offset = ret;
    for (int32_t i = rand_offset; i < cs_num_in_cluster + rand_offset; ++i)
    {
      int32_t pos = i % cs_num_in_cluster;
      if (list[pos].err_times_ >= ObTabletLocationItem::MAX_ERR_TIMES)
      {
        continue;
      }
      else
      {
        ret = pos;
        break;
      }
    }
  }
  else
  {
    YYSYS_LOG(INFO, " using old balance method, randomly get chunk server");
    //add:e
    ret = static_cast<int32_t>(random() % list_size);
    rand_offset = ret; // prevent hotspot
    for (int32_t i = rand_offset; i < list_size + rand_offset; ++i)
    {
      int32_t pos = i % list_size;
      if (list[pos].err_times_ >= ObTabletLocationItem::MAX_ERR_TIMES)
      {
        continue;
      }
      else
      {
        ret = pos;
        break;
      }
    }
  }
  return ret;
}

//mod zhaoqiong [MultiUPS] [RootTable_Cache] 20150703:b
//int32_t ObChunkServerTaskDispatcher::new_select_cs(ObTabletLocationList & list)
int32_t ObChunkServerTaskDispatcher::new_select_cs(ObTabletLocationList & list, int32_t cluster_id)
//mod:e
{
  int32_t list_size = static_cast<int32_t>(list.size());
  OB_ASSERT(0 < list_size);
  int32_t ret = static_cast<int32_t>(random() % list_size);
  ObMergerServerCounter * counter = GET_TSI_MULT(ObMergerServerCounter, SERVER_COUNTER_ID);
  if (NULL == counter)
  {
    YYSYS_LOG(WARN, "get tsi server counter failed:counter[%p]", counter);
  }
  //mod zhaoqiong [MultiUPS] [RootTable_Cache] 20150703:b
//  else
//  {
//    int64_t min_count = (((uint64_t)1) << 63) - 1;
//    int64_t cur_count = 0;
//    for (int32_t i = 0; i < list_size; ++i)
//    {
//      if (list[i].err_times_ >= ObTabletLocationItem::MAX_ERR_TIMES)
//      {
//        continue;
//      }
//      cur_count = counter->get(list[i].server_.chunkserver_);
//      if (0 == cur_count)
//      {
//        ret = i;
//        break;
//      }
//      if (cur_count < min_count)
//      {
//        min_count = cur_count;
//        ret = i;
//      }
//    }
  else if (list[0].server_.cluster_id_ == cluster_id)
  {
    YYSYS_LOG(DEBUG, " using new balance method, get chunk server from cluster [%d]", cluster_id);
    int32_t cs_num_in_cluster = 0;
    for (int32_t i = 0; i < list_size ; ++i)
    {
      if (list[i].server_.cluster_id_ == cluster_id)
      {
        cs_num_in_cluster++;
      }
      else
        break;
    }
    ret = inner_select_cs(list,cs_num_in_cluster,counter);
  }
  else
  {
    YYSYS_LOG(DEBUG, " using new balance method, randomly get chunk server");
    ret = inner_select_cs(list,list_size,counter);
  }
  //mod:e
  return ret;
}

int ObChunkServerTaskDispatcher::select_cs(const bool open, ObChunkServerItem * replicas_in_out, const int32_t replica_count_in,
    ObMergerServerCounter * counter)
{
    UNUSED(counter);
    UNUSED(replicas_in_out);
    UNUSED(open);
  int ret = static_cast<int32_t>(random()%replica_count_in);
  if (open && (NULL != counter))
  {
    //mod zhaoqiong [MultiUPS] [RootTable_Cache] 20150703:b
//    int64_t min_count = (((uint64_t)1) << 63) - 1;
//    int64_t cur_count = 0;
//    for (int32_t i = 0; i < replica_count_in; ++i)
//    {
//      cur_count = counter->get(replicas_in_out[i].addr_);
//      if (0 == cur_count)
//      {
//        ret = i;
//        break;
//      }
//      if (cur_count < min_count)
//      {
//        min_count = cur_count;
//        ret = i;
//      }
//    }
    int32_t cluster_id = (int32_t)ObMergeServerMain::get_instance()->get_merge_server().get_config().cluster_id;
    if (replicas_in_out[0].cluster_id_ == cluster_id)
    {
      YYSYS_LOG(DEBUG, "using new balance method, get chunk server item from cluster [%d]", cluster_id);
      int32_t cs_num_in_cluster = 0;
      for (int32_t i = 0; i < replica_count_in ; ++i)
      {
        if (replicas_in_out[i].cluster_id_ == cluster_id)
        {
          cs_num_in_cluster++;
        }
        else
          break;
      }
      ret = inner_select_cs(replicas_in_out,cs_num_in_cluster,counter);
    }
    else
    {
      YYSYS_LOG(INFO, "using new balance method, randomly get chunk server item");
      ret = inner_select_cs(replicas_in_out,replica_count_in,counter);
    }
    //mod:e
  }
  return ret;
}

int ObChunkServerTaskDispatcher::select_cs(ObChunkServerItem * replicas_in_out,
    const int32_t replica_count_in, const int32_t last_query_idx_in)
{
  int ret = OB_SUCCESS;
  if(OB_SUCCESS == ret && NULL == replicas_in_out)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(WARN, "parameter replicas_in_out is null");
  }

  if(OB_SUCCESS == ret && replica_count_in <= 0)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(WARN, "replica_count_in should be positive:replica_count_in[%d]", replica_count_in);
  }

  if(OB_SUCCESS == ret && last_query_idx_in >= replica_count_in)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(WARN, "last_query_idx_in should be less than replica_count_in:last_query_idx_in[%d], replica_count_in[%d]",
      last_query_idx_in, replica_count_in);
  }
  if(OB_SUCCESS == ret)
  {
    ObMergerServerCounter * counter = GET_TSI_MULT(ObMergerServerCounter, SERVER_COUNTER_ID);
    if (NULL == counter)
    {
      YYSYS_LOG(WARN, "get tsi server counter failed:counter[%p]", counter);
    }
    if(last_query_idx_in < 0) // The first time request
    {
      /// select the min request counter server
      ret = select_cs(using_new_balance_, replicas_in_out, replica_count_in, counter);
      replicas_in_out[ret].status_ = ObChunkServerItem::REQUESTED;
    }
    else
    {
      ret = OB_ENTRY_NOT_EXIST;
      for(int i=1;i<=replica_count_in;i++)
      {
        int next = (last_query_idx_in + i) % replica_count_in;
        if(ObChunkServerItem::UNREQUESTED == replicas_in_out[next].status_)
        {
          ret = next;
          replicas_in_out[ret].status_ = ObChunkServerItem::REQUESTED;
          //add zhaoqiong [MultiUPS] [RootTable_Cache] 20150703:b
          YYSYS_LOG(INFO, "retry, get chunk server item [%s], cluster_id = %d",
                    to_cstring(replicas_in_out[ret].addr_),replicas_in_out[ret].cluster_id_);
          //add:e
          break;
        }
      }
      if(OB_ENTRY_NOT_EXIST == ret)
      {
        YYSYS_LOG(INFO, "There is no chunkserver which is never requested");
      }
    }
  }
  return ret;
}

//add zhaoqiong [MultiUPS] [RootTable_Cache] 20150703:b
int ObChunkServerTaskDispatcher::inner_select_cs(ObTabletLocationList & list,
    const int32_t replica_count_in, ObMergerServerCounter * counter)
{
  int32_t ret = static_cast<int32_t>(random() % replica_count_in);
  int64_t min_count = (((uint64_t)1) << 63) - 1;
  int64_t cur_count = 0;
  for (int32_t i = 0; i < replica_count_in; ++i)
  {
    if (list[i].err_times_ >= ObTabletLocationItem::MAX_ERR_TIMES)
    {
      continue;
    }
    cur_count = counter->get(list[i].server_.chunkserver_);
    if (0 == cur_count)
    {
      ret = i;
      break;
    }
    if (cur_count < min_count)
    {
      min_count = cur_count;
      ret = i;
    }
  }
  return ret;
}
//add:e

//add zhaoqiong [MultiUPS] [RootTable_Cache] 20150703:b
int ObChunkServerTaskDispatcher::inner_select_cs(ObChunkServerItem * replicas_in_out,
    const int32_t replica_count_in, ObMergerServerCounter * counter)
{
  int32_t ret = static_cast<int32_t>(random() % replica_count_in);
  int64_t min_count = (((uint64_t)1) << 63) - 1;
  int64_t cur_count = 0;
  for (int32_t i = 0; i < replica_count_in; ++i)
  {
    cur_count = counter->get(replicas_in_out[i].addr_);
    if (0 == cur_count)
    {
      ret = i;
      break;
    }
    if (cur_count < min_count)
    {
      min_count = cur_count;
      ret = i;
    }
  }
  return ret;
}
//add:e
