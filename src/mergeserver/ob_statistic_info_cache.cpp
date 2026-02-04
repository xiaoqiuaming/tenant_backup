#include "ob_statistic_info_cache.h"
#include "ob_analysis_statistic_info.h"
#include "ob_merge_server.h"
#include "ob_merge_server_main.h"

namespace oceanbase
{
  namespace mergeserver
  {
    ObStatisticInfoCache::ObStatisticInfoCache() :
      inited_(false)
    {

    }

    ObStatisticInfoCache::~ObStatisticInfoCache()
    {

    }

    int ObStatisticInfoCache::init(ObMsSQLProxy *sql_proxy, const int64_t max_cache_size, const int64_t timeout)
    {
      int ret = OB_SUCCESS;
      kvcolumncache_.init(max_cache_size);
      kvtablecache_.init(max_cache_size);
      sql_proxy_ = sql_proxy;
      cache_timeout_ = timeout;
      inited_ = true;
      return ret;
    }

    int ObStatisticInfoCache::clear()
    {
      int ret = OB_SUCCESS;
      kvtablecache_.clear();
      kvcolumncache_.clear();
      return ret;
    }

    int ObStatisticInfoCache::destroy()
    {
      int ret = OB_SUCCESS;
      return ret;
    }

    int ObStatisticInfoCache::put_analysised_column_sinfo_into_cache(uint64_t tid, uint64_t cid)
    {
      int ret = OB_SUCCESS;

      ObAnalysisStatisticInfo analysis_stat_info;
      ObAnalysisStatisticInfo * analysis_manager_ = NULL;
      analysis_manager_ = &analysis_stat_info;
      if (OB_SUCCESS != (ret=analysis_manager_->analysis_column_statistic_info(tid,cid,sql_proxy_)))
      {
        YYSYS_LOG(WARN, "fail to analysis_column_statistic_info,ret=[%d]",ret);
      }
      else if(analysis_manager_->is_no_gather_statinfo())
      {
        YYSYS_LOG(WARN, "the table no gather statistic info, tid=%lu",tid);
        ret = OB_ERROR;
      }
      else
      {
        StatisticColumnValue scv;
        common::ModuleArena allocator;
        common::ObTopValue* top_value=analysis_manager_->get_top_value();
        for(int i=0;i<analysis_manager_->get_top_value_num();i++)
        {
          scv.top_value_[i].deep_copy(allocator, top_value[i]);
        }
        scv.top_value_num_ = analysis_manager_->get_top_value_num();

        ob_write_obj_v2(allocator, analysis_manager_->get_min(), scv.min_);
        ob_write_obj_v2(allocator, analysis_manager_->get_max(), scv.max_);
        scv.row_count_ = analysis_manager_->get_row_count();
        scv.different_num_ = analysis_manager_->get_different_num();
        scv.size_ = analysis_manager_->get_size();
        scv.timestamp_ = yysys::CTimeUtil::getTime();

        if(!inited_)
        {
          YYSYS_LOG(WARN, "have not init!");
          ret = OB_ERROR;
        }
        else if(OB_SUCCESS!=(ret=add_column_statistic_info_into_cache(tid,cid,scv)))
        {
          YYSYS_LOG(WARN, "failed to add top_value into map!,ret=[%d]",ret);
        }
        allocator.free();
        scv.allocator_.free();
      }
      analysis_manager_->reset();
      return ret;
    }

    int ObStatisticInfoCache::put_analysised_table_sinfo_into_cache(uint64_t tid)
    {
      int ret=OB_SUCCESS;
      ObAnalysisStatisticInfo analysis_stat_info;
      ObAnalysisStatisticInfo * analysis_manager_ = NULL;
      analysis_manager_ = &analysis_stat_info;
      if(OB_SUCCESS!=(ret=analysis_manager_->analysis_table_statistic_info(tid,sql_proxy_)))
      {
        YYSYS_LOG(WARN, "fail to analysis_table_statistic_info");
      }
      else
      {
        StatisticTableValue stv;
        stv.row_count_ = analysis_manager_->get_row_count();
        stv.size_ = analysis_manager_->get_size();
        stv.statistic_columns_num_ = analysis_manager_->get_statistic_columns_num();
        uint64_t * statistic_columns = analysis_manager_->get_statistic_columns();

        if(stv.row_count_ == 0)
        {
          stv.mean_row_size_ = 0;
        }
        else
        {
          stv.mean_row_size_ = (stv.size_)/(stv.row_count_);
        }

        for(int i=0;i<stv.statistic_columns_num_;i++)
        {
          stv.statistic_columns_[i] = statistic_columns[i];
        }

        stv.timestamp_ = yysys::CTimeUtil::getTime();
        if(!inited_)
        {
          YYSYS_LOG(WARN, "table_statistic_info_hashmap is not init!");
          ret = OB_ERROR;
        }
        else if(OB_SUCCESS!=(ret=add_table_statistic_info_into_cache(tid,stv)))
        {
          YYSYS_LOG(WARN, "failed to add table_statistic_info into map!,ret=[%d]",ret);
        }
      }
      analysis_manager_->reset();
      return ret;
    }

    int ObStatisticInfoCache::add_column_statistic_info_into_cache(uint64_t tid, uint64_t cid, StatisticColumnValue &scv)
    {
      int ret=OB_SUCCESS;
      StatisticColumnKey key;
      if(!inited_)
      {
        YYSYS_LOG(WARN, "have not init!");
        ret=OB_ERROR;
      }
      else if(OB_INVALID_ID == tid || OB_INVALID_ID == cid)
      {
        YYSYS_LOG(WARN, "table_id or column_id invalid!");
        ret=OB_ERROR;
      }
      else
      {
        key.table_id_ = tid;
        key.column_id_ = cid;
        if(OB_SUCCESS!=(ret=kvcolumncache_.put(key,scv,true)))
        {
          YYSYS_LOG(WARN, "failed to put column statistic info to cache, err=%d",ret);
        }
      }
      return ret;
    }

    int ObStatisticInfoCache::get_column_statistic_info_from_cache(uint64_t tid, uint64_t cid, StatisticColumnValue &scv)
    {
      int ret=OB_SUCCESS;
      StatisticColumnKey key;
      Handle handle;
      if(!inited_)
      {
        YYSYS_LOG(WARN, "have not init!");
        ret=OB_ERROR;
      }
      else if(OB_INVALID_ID == tid || OB_INVALID_ID == cid)
      {
        YYSYS_LOG(WARN, "table_id or column_id invalid!");
        ret=OB_ERROR;
      }
      else
      {
        key.table_id_ = tid;
        key.column_id_ = cid;
        if(OB_SUCCESS==(ret=kvcolumncache_.get(key,scv,handle)))
        {
          YYSYS_LOG(DEBUG, "get column statistic info success");
          int64_t timestamp = yysys::CTimeUtil::getTime();
          if (timestamp - scv.timestamp_ > cache_timeout_)
          {
            YYSYS_LOG(DEBUG, "the column statistic info timeout!");
            if (OB_SUCCESS!=(ret=put_analysised_column_sinfo_into_cache(tid,cid)))
            {
              YYSYS_LOG(WARN, "failed to put column statistic info!");
            }
            else if(OB_SUCCESS==(ret=kvcolumncache_.get(key,scv,handle)))
            {
              YYSYS_LOG(DEBUG, "get column statistic info success");
            }
            else if(ret==OB_ENTRY_NOT_EXIST)
            {
              YYSYS_LOG(WARN, "the column statistic info not exist");
            }
            else
            {
              YYSYS_LOG(WARN, "get column statistic info err = %d",ret);
            }
          }
        }
        else if (ret == OB_ENTRY_NOT_EXIST)
        {
          if (OB_SUCCESS!=(ret=put_analysised_column_sinfo_into_cache(tid,cid)))
          {
            YYSYS_LOG(WARN, "failed to put column statistic info");
          }
          else if(OB_SUCCESS==(ret=kvcolumncache_.get(key,scv,handle)))
          {
            YYSYS_LOG(DEBUG, "get column statistic info success");
          }
          else if(ret==OB_ENTRY_NOT_EXIST)
          {
            YYSYS_LOG(WARN, "the column statistic info not exist");
          }
          else
          {
            YYSYS_LOG(WARN, "get column statistic info err = %d",ret);
          }
        }
        else
        {
          YYSYS_LOG(ERROR, "get column statistic info err = %d",ret);
        }

        if(OB_SUCCESS == ret)
        {
          kvcolumncache_.revert(handle);
        }
      }
      return ret;
    }

    int ObStatisticInfoCache::renew_column_statistic_asy(uint64_t tid, uint64_t cid)
    {
      YYSYS_LOG(INFO, "begin renew column statistic info asynchronously!");
      int err=OB_SUCCESS;
      const common::ObClientManager * rpc_frame=NULL;
      ObMergeServer & merge_server = ObMergeServerMain::get_instance()->get_merge_server();
      rpc_frame=&(merge_server.get_client_manager());
      char buf[OB_MAX_PACKET_LENGTH]={0};
      ObDataBuffer msgbuf(buf,OB_MAX_PACKET_LENGTH);
      msgbuf.get_position() = 0;

      if(NULL == rpc_frame)
      {
        YYSYS_LOG(WARN, "rpc_frame=NULL");
        err = OB_ERROR;
      }
      else if(OB_SUCCESS!=(err = serialization::encode_i64(msgbuf.get_data(),msgbuf.get_capacity(),msgbuf.get_position(),static_cast<int64_t>(tid))))
      {
        YYSYS_LOG(WARN, "failed to serialization table_id[%lu], err=%d",tid,err);
      }
      else if(OB_SUCCESS!=(err = serialization::encode_i64(msgbuf.get_data(),msgbuf.get_capacity(),msgbuf.get_position(),static_cast<int64_t>(cid))))
      {
        YYSYS_LOG(WARN, "failed to serialization column_id[%lu], err=%d",cid,err);
      }
      err = rpc_frame->post_request(merge_server.get_self(),
                                    OB_PUT_ANALYSISED_COLUMN_SINFO_INTO_CACHE, DEFAULT_VERSION,msgbuf);
      if (OB_SUCCESS!=err)
      {
        YYSYS_LOG(ERROR, "post request to merge server "
                  "for put analysised column sinfo into cache failed:err[%d].", err);
      }
      return err;
    }

    int ObStatisticInfoCache::renew_table_statistic_asy(uint64_t tid)
    {
      YYSYS_LOG(INFO, "begin renew table statistic info asynchronously!");
      int err=OB_SUCCESS;
      const common::ObClientManager * rpc_frame=NULL;
      ObMergeServer & merge_server = ObMergeServerMain::get_instance()->get_merge_server();
      rpc_frame=&(merge_server.get_client_manager());
      char buf[OB_MAX_PACKET_LENGTH]={0};
      ObDataBuffer msgbuf(buf,OB_MAX_PACKET_LENGTH);
      msgbuf.get_position() = 0;
      if(NULL == rpc_frame)
      {
        YYSYS_LOG(WARN, "rpc_frame=NULL");
        err = OB_ERROR;
      }
      else if(OB_SUCCESS!=(err = serialization::encode_i64(msgbuf.get_data(),msgbuf.get_capacity(),msgbuf.get_position(),static_cast<int64_t>(tid))))
      {
        YYSYS_LOG(WARN, "failed to serialization table_id[%lu], err=%d",tid,err);
      }
      err = rpc_frame->post_request(merge_server.get_self(),
                                    OB_PUT_ANALYSISED_TABLE_SINFO_INTO_CACHE, DEFAULT_VERSION,msgbuf);
      if (OB_SUCCESS!=err)
      {
        YYSYS_LOG(ERROR, "post request to merge server "
                  "for put analysised table sinfo into cache failed:err[%d].", err);
      }
      return err;
    }

    int ObStatisticInfoCache::add_table_statistic_info_into_cache(uint64_t tid, StatisticTableValue &stv)
    {
      int ret=OB_SUCCESS;
      if(!inited_)
      {
        YYSYS_LOG(WARN, "have not init!");
        ret=OB_ERROR;
      }
      else if(OB_INVALID_ID == tid)
      {
        YYSYS_LOG(WARN, "table_id invalid!");
        ret=OB_ERROR;
      }
      else
      {
        if(OB_SUCCESS!=(ret=kvtablecache_.put(tid,stv,true)))
        {
          YYSYS_LOG(WARN, "failed to put table statistic info to cache, err=%d", ret);
        }
      }
      return ret;
    }

    int ObStatisticInfoCache::get_table_statistic_info_from_cache(uint64_t tid, StatisticTableValue &stv)
    {
      int ret=OB_SUCCESS;
      Handle handle;
      if(!inited_)
      {
        YYSYS_LOG(WARN, "have not init!");
        ret=OB_ERROR;
      }
      else if(OB_INVALID_ID == tid)
      {
        YYSYS_LOG(WARN, "table_id invalid!");
      }
      else if(OB_SUCCESS==(ret=kvtablecache_.get(tid,stv,handle)))
      {
        YYSYS_LOG(DEBUG, "get table statistic info success");
        kvtablecache_.revert(handle);
        int64_t timestamp = yysys::CTimeUtil::getTime();
        if (timestamp - stv.timestamp_ > cache_timeout_)
        {
          YYSYS_LOG(DEBUG, "the table statistic info timeout!");
          if (OB_SUCCESS!=(ret=put_analysised_table_sinfo_into_cache(tid)))
          {
            YYSYS_LOG(WARN, "failed to put table statistic info ");
          }
          else if (OB_SUCCESS == (ret=kvtablecache_.get(tid,stv,handle)))
          {
            YYSYS_LOG(DEBUG, "get table statistic info success");
            kvtablecache_.revert(handle);
          }
          else if(ret==OB_ENTRY_NOT_EXIST)
          {
            YYSYS_LOG(WARN, "the table statistic info not exist");
          }
          else
          {
            YYSYS_LOG(WARN, "get table statistic info err=%d",ret);
          }
        }
      }
      else if(ret == OB_ENTRY_NOT_EXIST)
      {
        if(OB_SUCCESS!=(ret=put_analysised_table_sinfo_into_cache(tid)))
        {
          YYSYS_LOG(WARN, "failed to put table statistic info ");
        }
        else if (OB_SUCCESS == (ret=kvtablecache_.get(tid,stv,handle)))
        {
          YYSYS_LOG(DEBUG, "get table statistic info success");
          kvtablecache_.revert(handle);
        }
        else if(ret==OB_ENTRY_NOT_EXIST)
        {
          YYSYS_LOG(WARN, "the table statistic info not exist");
        }
        else
        {
          YYSYS_LOG(WARN, "get table statistic info err=%d",ret);
        }
      }
      else
      {
        YYSYS_LOG(ERROR, "get table statistic info err=%d",ret);
      }
      return ret;
    }
  }
}
