/*
 * (C) 2007-2010 TaoBao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_chunk_merge.cpp is for what ...
 *
 * Version: $id$
 *
 * Authors:
 *   MaoQi maoqi@taobao.com
 *
 */

#include "ob_chunk_server_main.h"
#include "common/ob_read_common_data.h"
#include "ob_tablet_image.h"
#include "common/utility.h"
#include "ob_chunk_merge.h"
#include "sstable/ob_disk_path.h"
#include "common/ob_trace_log.h"
#include "ob_tablet_manager.h"
#include "common/ob_atomic.h"
#include "common/file_directory_utils.h"
#include "ob_tablet_merger_v1.h"
#include "ob_tablet_merger_v2.h"

namespace oceanbase
{
  namespace chunkserver
  {
    using namespace yyutil;
    using namespace common;
    using namespace sstable;

    /*-----------------------------------------------------------------------------
     *  ObChunkMerge
     *-----------------------------------------------------------------------------*/

    ObChunkMerge::ObChunkMerge() : inited_(false),tablets_num_(0),
                                   tablet_index_(0),thread_num_(0),
                                   tablets_have_got_(0), active_thread_num_(0),
                                   frozen_version_(0), newest_frozen_version_(0), frozen_timestamp_(0),
                                   write_sstable_version_(0), merge_start_time_(0),merge_last_end_time_(0),
                                   round_start_(true),round_end_(true), pending_in_upgrade_(false),
                                   merge_load_high_(0),request_count_high_(0), merge_adjust_ratio_(0),
                                   merge_load_adjust_(0), merge_pause_row_count_(0), merge_pause_sleep_time_(0),
                                   merge_highload_sleep_time_(0), tablet_manager_(NULL)
    {
      //memset(reinterpret_cast<void *>(&pending_merge_),0,sizeof(pending_merge_));
      for(uint32_t i=0; i < sizeof(pending_merge_) / sizeof(pending_merge_[0]); ++i)
      {
        pending_merge_[i] = 0;
      }
      memset(mergers_, 0, sizeof(mergers_));
    }

    void ObChunkMerge::set_config_param()
    {
      ObChunkServer& chunk_server = ObChunkServerMain::get_instance()->get_chunk_server();
      merge_load_high_ = chunk_server.get_config().merge_threshold_load_high;
      request_count_high_ = chunk_server.get_config().merge_threshold_request_high;
      merge_adjust_ratio_ = chunk_server.get_config().merge_adjust_ratio;
      merge_load_adjust_ = (merge_load_high_ * merge_adjust_ratio_) / 100;
      merge_pause_row_count_ = chunk_server.get_config().merge_pause_row_count;
      merge_pause_sleep_time_ = chunk_server.get_config().merge_pause_sleep_time;
      merge_highload_sleep_time_ = chunk_server.get_config().merge_highload_sleep_time;
    }

    int ObChunkMerge::create_merge_threads(const int64_t max_merge_thread)
    {
      int ret = OB_SUCCESS;

      setThreadCount(static_cast<int32_t>(max_merge_thread));
      active_thread_num_ = max_merge_thread;
      thread_num_ = start();

      if (thread_num_ <= 0)
      {
        YYSYS_LOG(ERROR, "cannot create merge thread , max_merge_thread=%ld", max_merge_thread);
        ret = OB_ERROR;
      }
      else 
      {
        if (thread_num_ != max_merge_thread)
        {
          YYSYS_LOG(WARN, "start %ld merge threads, less than expected %ld", thread_num_, max_merge_thread);
        }
        min_merge_thread_num_ = thread_num_ / 3;
        if (min_merge_thread_num_ == 0) min_merge_thread_num_ = 1;
        YYSYS_LOG(INFO, "create_merge_threads thread_num_=%ld, "
            "active_thread_num_=%ld, min_merge_thread_num_=%ld",
            thread_num_, active_thread_num_, min_merge_thread_num_);
      }
      return ret;
    }

    int ObChunkMerge::init(ObTabletManager *manager)
    {
      int ret = OB_SUCCESS;
      ObChunkServer& chunk_server = ObChunkServerMain::get_instance()->get_chunk_server();

      if (NULL == manager)
      {
        YYSYS_LOG(ERROR,"input error,manager is null");
        ret = OB_ERROR;
      }
      else if (!inited_)
      {
        inited_ = true;

        tablet_manager_ = manager;
        frozen_version_ = manager->get_serving_data_version();
        newest_frozen_version_ = frozen_version_;

        pthread_mutex_init(&mutex_,NULL);
        pthread_cond_init(&cond_,NULL);

        int64_t max_merge_thread = chunk_server.get_config().max_merge_thread_num;
        if (max_merge_thread <= 0 || max_merge_thread > MAX_MERGE_THREAD)
          max_merge_thread = MAX_MERGE_THREAD;

        set_config_param();
        if (OB_SUCCESS != (ret = create_merge_threads(max_merge_thread)))
        {
          YYSYS_LOG(ERROR, "create_merge_threads error, ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = create_all_tablet_mergers()))
        {
          YYSYS_LOG(ERROR, "create_all_tablet_mergers error, ret=%d", ret);
        }
      }
      else
      {
        YYSYS_LOG(WARN,"ObChunkMerge have been inited");
      }

      if (OB_SUCCESS != ret && inited_)
      {
        pthread_mutex_destroy(&mutex_);
        pthread_cond_destroy(&cond_);
        inited_ = false;
      }
      return ret;
    }

    void ObChunkMerge::destroy()
    {
      if (inited_)
      {
        if (false == THE_CHUNK_SERVER.get_config().each_tablet_sync_meta
            && !is_merge_stoped())
        {
          tablet_manager_->sync_all_tablet_images();
        }
        inited_ = false;
        pthread_cond_broadcast(&cond_);
        usleep(50);

        wait();
        pthread_cond_destroy(&cond_);
        pthread_mutex_destroy(&mutex_);
        destroy_all_tablets_mergers();
      }
    }

    int ObChunkMerge::get_tablet_merger(const int64_t thread_no, ObTabletMerger* &merger)
    {
      int ret = OB_SUCCESS;
      merger = NULL;
      ObTabletMerger** mergers = NULL;
      if (write_sstable_version_ < SSTableReader::COMPACT_SSTABLE_VERSION)
      {
        mergers = mergers_;
      }
      else
      {
        mergers = mergers_ + MAX_MERGE_THREAD;
      }

      if (thread_no >= MAX_MERGE_THREAD)
      {
        YYSYS_LOG(ERROR, "thread_no=%ld >= max_merge_thread_num=%ld", thread_no, MAX_MERGE_THREAD);
        ret = OB_SIZE_OVERFLOW;
      }
      else if (NULL == mergers)
      {
        YYSYS_LOG(ERROR, "thread_no=%ld mergers is NULL, version=%ld", thread_no, write_sstable_version_);
        ret = OB_SIZE_OVERFLOW;
      }
      else if (NULL == (merger = mergers[thread_no]))
      {
        YYSYS_LOG(ERROR, "thread_no=%ld merger is NULL, version=%ld", thread_no, write_sstable_version_);
        ret = OB_INVALID_ARGUMENT;
      }
      return ret;
    }

    int ObChunkMerge::create_all_tablet_mergers()
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = create_tablet_mergers<ObTabletMergerV1>(mergers_, MAX_MERGE_THREAD)))
      {
        YYSYS_LOG(ERROR, "create v1 Merger error, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = create_tablet_mergers<ObTabletMergerV2>(mergers_ + MAX_MERGE_THREAD, MAX_MERGE_THREAD)))
      {
        YYSYS_LOG(ERROR, "create v2 Merger error, ret=%d", ret);
      }
      return ret;
    }

    int ObChunkMerge::destroy_tablet_mergers(ObTabletMerger** mergers, const int64_t size)
    {
      int ret = OB_SUCCESS;
      if (NULL == mergers || 0 >= size)
      {
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        for (int64_t i = 0 ; i < size; ++i)
        {
          ObTabletMerger* merger = mergers[i];
          if (NULL != merger)
          {
            merger->~ObTabletMerger();
          }
        }
        void * ptr = mergers[0];
        ob_free(ptr);
        memset(mergers, 0, size * sizeof(ObTabletMerger*));
      }
      return ret;
    }

    int ObChunkMerge::destroy_all_tablets_mergers()
    {
      int ret = OB_SUCCESS;
      destroy_tablet_mergers(mergers_, MAX_MERGE_THREAD );
      destroy_tablet_mergers(mergers_ + MAX_MERGE_THREAD, MAX_MERGE_THREAD );
      return ret;
    }

    bool ObChunkMerge::can_launch_next_round(const int64_t frozen_version)
    {
      bool ret = false;
      int64_t now = yysys::CTimeUtil::getTime();


      if (inited_ && frozen_version > frozen_version_ && is_merge_stoped()
          && frozen_version > tablet_manager_->get_serving_data_version()
          && now - merge_last_end_time_ > THE_CHUNK_SERVER.get_config().min_merge_interval
          && THE_CHUNK_SERVER.get_tablet_manager().get_bypass_sstable_loader().is_loader_stoped()
          //add liuxiao [secondary index] 20150626
          && !tablet_manager_->if_is_building_index()
          //add e
              )
      {
        ret = true;
      }
      return ret;
    }

    int ObChunkMerge::schedule(const int64_t frozen_version)
    {

      int ret = OB_SUCCESS;

      if (frozen_version > newest_frozen_version_)
      {
        newest_frozen_version_ = frozen_version;
      }

      // empty chunkserver, reset current frozen_version_ if
      // chunkserver got new tablet by migrate in or create new table;
      frozen_version_ = tablet_manager_->get_serving_data_version();

      if (1 >= frozen_version || (!can_launch_next_round(frozen_version)))
      {
        // do nothing
        YYSYS_LOG(INFO, "frozen_version=%ld, current frozen version = %ld, "
            "serving data version=%ld cannot launch next round.",
            frozen_version, frozen_version_, tablet_manager_->get_serving_data_version());
        ret = OB_CS_EAGAIN;
      }
      else if (0 == tablet_manager_->get_serving_data_version()) //new chunkserver
      {
        // empty chunkserver, no need to merge.
        YYSYS_LOG(INFO, "empty chunkserver , wait for migrate in.");
        ret = OB_CS_EAGAIN;
      }
      else
      {
        if (frozen_version - frozen_version_ > 1)
        {
          YYSYS_LOG(WARN, "merge is too slow,[%ld:%ld]", frozen_version_, frozen_version);
        }
        // only plus 1 in one merge process.
        frozen_version_ += 1;

        ret = start_round(frozen_version_);
        if (OB_SUCCESS != ret)
        {
          // start merge failed, maybe rootserver or updateserver not in serving.
          // restore old frozen_version_ for next merge process.
          frozen_version_ -= 1;
        }
      }

      if (inited_ && OB_SUCCESS == ret && thread_num_ > 0)
      {
        merge_start_time_ = yysys::CTimeUtil::getTime();
        write_sstable_version_ = THE_CHUNK_SERVER.get_config().merge_write_sstable_version;
        round_start_ = true;
        round_end_ = false;

        is_really_merged_ = false;

        YYSYS_LOG(INFO, "start new round ,wake up all merge threads, "
            "run new merge process with version=%ld, write sstable version=%ld", 
            frozen_version_, write_sstable_version_);
        pthread_cond_broadcast(&cond_);
      }

      return ret;
    }

    void ObChunkMerge::run(yysys::CThread* thread, void *arg)
    {
      UNUSED(thread);
      SET_THD_NAME("cswork-merg"); //[647]
      int64_t thread_no = reinterpret_cast<int64_t>(arg);
      merge_tablets(thread_no);
    }

    void ObChunkMerge::merge_tablets(const int64_t thread_no)
    {
      int ret = OB_SUCCESS;
      const int64_t sleep_interval = 5000000;
      ObTablet *tablet = NULL;
      ObTabletMerger *merger = NULL;
      int64_t merge_fail_count = 0;
      //add wenghaixing [secondary index static_index_build.merge]20150422
      //bool invalid = false;
      //add e
      ObChunkServer&  chunk_server = ObChunkServerMain::get_instance()->get_chunk_server();

      while(OB_SUCCESS == ret)
      {
        if (!inited_)
        {
          break;
        }

        if ( !check_load())
        {
          YYSYS_LOG(INFO,"load is too high, go to sleep");
          pthread_mutex_lock(&mutex_);

          if (1 == active_thread_num_)
          {
            pthread_mutex_unlock(&mutex_);
            usleep(sleep_interval); //5s
          }
          else
          {
            --active_thread_num_;
            pthread_cond_wait(&cond_, &mutex_);
            YYSYS_LOG(INFO,"to merge,active_thread_num_ :%ld", active_thread_num_);
            ++active_thread_num_;
            pthread_mutex_unlock(&mutex_);
          }
        }

        pthread_mutex_lock(&mutex_);
        ret = get_tablets(tablet);
        while (true)
        {
          if (!inited_)
          {
            break;
          }
          if (OB_SUCCESS != ret)
          {
            pthread_mutex_unlock(&mutex_);
            usleep(sleep_interval);
            // retry to get tablet until got one or got nothing.
            pthread_mutex_lock(&mutex_);
          }
          else if (NULL == tablet) // got nothing
          {
            --active_thread_num_;
            YYSYS_LOG(DEBUG,"there is no tablet need merge, sleep wait for new merge proecess.");
            pthread_cond_wait(&cond_,&mutex_);
            YYSYS_LOG(DEBUG,"awake by signal, active_thread_num_:%ld",active_thread_num_);
            // retry to get new tablet for merge.
            ++active_thread_num_;
          }
          else // got tablet for merge
          {
            break;
          }
          ret = get_tablets(tablet);
        }
        pthread_mutex_unlock(&mutex_);

        int64_t retry_times = chunk_server.get_config().retry_times;
        int64_t merge_per_disk = chunk_server.get_config().merge_thread_per_disk;

        // okay , we got a tablet for merge finally.
        if (NULL != tablet)
        {
          if (tablet->get_data_version() > frozen_version_)
          {
            //impossible
            YYSYS_LOG(ERROR,"local tablet version (%ld) > frozen_version_(%ld)",tablet->get_data_version(),frozen_version_);
            kill(getpid(),2);
          }
          else if ((tablet->get_merge_count() > retry_times) && (have_new_version_in_othercs(tablet)))
          {
            ObVector<ObTablet*> tablet_list;
            tablet_list.push_back(tablet);
            YYSYS_LOG(WARN,"too many times(%d),discard this tablet,wait for migrate copy.", tablet->get_merge_count());
            if (OB_SUCCESS == tablet_manager_->delete_tablet_on_rootserver(tablet_list))
            {
              YYSYS_LOG(INFO, "delete tablet (version=%ld) on rs succeed. ", tablet->get_data_version());
              tablet->set_merged();
              tablet->set_removed();

              int32_t disk_no = 0;
              if (OB_SUCCESS != tablet_manager_->get_serving_tablet_image().remove_tablet(
                tablet->get_range(), tablet->get_data_version(), disk_no))
              {
                YYSYS_LOG(WARN, "failed to remove tablet from tablet image, "
                                "version=%ld, disk=%d, range=%s",
                          tablet->get_data_version(), tablet->get_disk_no(), 
                          to_cstring(tablet->get_range()));
              }
            }
          }
          else if (OB_SUCCESS != (ret = get_tablet_merger(thread_no, merger)))
          {
            YYSYS_LOG(ERROR, "cannot get_tablet_merger thread_no=%ld", thread_no);
          }
          /*
          //add wenghaixing [secondary index static_index_build.merge]20150422
          else if(OB_SUCCESS != (ret = merger->stop_invalid_index_tablet_merge(tablet, invalid)))
          {
            YYSYS_LOG(WARN, "stop_invalid_index_tablet_merge failed,ret [%d]", ret);
          }
          //add e
          //modify wenghaixing [secondary index static_index_build.merge]20150422
          else if(!invalid)
          //else
          //modify e
          */
          else
          {
            if ( (newest_frozen_version_ - tablet->get_data_version())
                 > chunk_server.get_config().max_version_gap )
            {
              YYSYS_LOG(WARN,"this tablet version (%ld : %ld) is too old,maybe don't need to merge",
                  tablet->get_data_version(),newest_frozen_version_);
            }

            volatile uint32_t *ref = &pending_merge_[ tablet->get_disk_no() ];
            int err = OB_SUCCESS;
            if (*ref < merge_per_disk)
            {
              atomic_inc(ref);
              YYSYS_LOG(DEBUG,"get a tablet, start merge");
              //mod by zhaoqiong [bugfix: create table][schema sync]20160106:b
//              if ((err = merger->merge(tablet,tablet->get_data_version() + 1)) != OB_SUCCESS
//                  && OB_CS_TABLE_HAS_DELETED != err)
              if ((err = merger->merge(tablet,tablet->get_data_version() + 1)) != OB_SUCCESS
                  && OB_CS_TABLE_HAS_DELETED != err && OB_CS_MERGE_CANCELED != err)
                //mod:e
              {
                YYSYS_LOG(WARN,"merge tablet failed");
                if (++merge_fail_count > 5)
                {
                  usleep(sleep_interval);
                }
              }
              else
              {
                merge_fail_count = 0;
              }
              tablet->inc_merge_count();
              atomic_dec(ref);
            }
          }
          pthread_mutex_lock(&mutex_);
          if(!tablet->is_merged())
          {
              tablet->set_merged(0);
          }
          if (tablet_manager_->get_serving_tablet_image().release_tablet(tablet) != OB_SUCCESS)
          {
            YYSYS_LOG(WARN,"release tablet failed");
          }

          //pthread_mutex_lock(&mutex_);
          //++tablets_have_got_;
          pthread_mutex_unlock(&mutex_);
          is_really_merged_ = true;

        }

        if ( tablet_manager_->is_stoped() )
        {
          YYSYS_LOG(WARN,"stop in merging");
          ret = OB_CS_MERGE_CANCELED;
        }
      }

    }

    int ObChunkMerge::fetch_frozen_time_busy_wait(const int64_t frozen_version, int64_t &frozen_time)
    {
      int ret = OB_SUCCESS;
      if (0 == frozen_version)
      {
        YYSYS_LOG(ERROR,"invalid argument, frozen_version is 0");
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        ObMergerRpcProxy* rpc_proxy = THE_CHUNK_SERVER.get_rpc_proxy();
        if (NULL != rpc_proxy)
        {
          int64_t retry_times  = THE_CHUNK_SERVER.get_config().retry_times;
          RPC_RETRY_WAIT(inited_, retry_times, ret,
                         rpc_proxy->get_frozen_time(frozen_version, frozen_time));
        }
        else
        {
          YYSYS_LOG(WARN, "get rpc proxy from chunkserver failed");
          ret = OB_ERROR;
        }
      }
      return ret;
    }

    int ObChunkMerge::fetch_frozen_schema_busy_wait(
      const int64_t frozen_version, ObSchemaManagerV2& schema)
    {
      int ret = OB_SUCCESS;
      if (0 == frozen_version)
      {
        YYSYS_LOG(ERROR,"invalid argument, frozen_version is 0");
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        ObMergerRpcProxy* rpc_proxy = THE_CHUNK_SERVER.get_rpc_proxy();
        if (NULL != rpc_proxy)
        {
          int64_t retry_times  = THE_CHUNK_SERVER.get_config().retry_times;
          RPC_RETRY_WAIT(inited_, retry_times, ret,
            rpc_proxy->get_frozen_schema(frozen_version, schema));
        }
        else
        {
          YYSYS_LOG(WARN, "get rpc proxy from chunkserver failed");
          ret = OB_ERROR;
        }
      }
      return ret;
    }

    /**
     * luanch next merge round, do something before doing actual
     * merge stuff .
     * 1. fetch new schema with current frozen version; must be
     * compatible with last schema;
     * 2. fetch new freeze timestamp with current frozen version,
     * for TTL (filter expired line);
     * 3. prepare for next merge, drop block cache used in pervoius
     * merge round, drop image slot used in prevoius version;
     * 4. initialize import sstable instance;
     */
    int ObChunkMerge::start_round(const int64_t frozen_version)
    {
      int ret = OB_SUCCESS;
      // save schema used by last merge process .
      if (current_schema_.get_version() > 0)
      {
        last_schema_ = current_schema_;
      }

      // fetch frozen schema with frozen_version_;
      ret = fetch_frozen_schema_busy_wait(frozen_version, current_schema_);
      if (OB_SUCCESS == ret)
      {
        if(current_schema_.get_version() > 0 && last_schema_.get_version() > 0)
        {
          if (current_schema_.get_version() < last_schema_.get_version())
          {
            YYSYS_LOG(ERROR,"the new schema old than last schema, current=%ld, last=%ld",
                current_schema_.get_version(), last_schema_.get_version());
            ret = OB_CS_SCHEMA_INCOMPATIBLE;
          }
          else if (!last_schema_.is_compatible(current_schema_))
          {
            YYSYS_LOG(ERROR,"the new schema and old schema is not compatible");
            ret = OB_CS_SCHEMA_INCOMPATIBLE;
          }
        }
      }

      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(ERROR, "cannot luanch next merge round cause schema issue.");
      }
      else if (OB_SUCCESS != (ret = fetch_frozen_time_busy_wait(frozen_version, frozen_timestamp_)))
      {
        YYSYS_LOG(ERROR, "cannot fetch frozen %ld timestamp from updateserver.", frozen_version);
      }
      else if (OB_SUCCESS != (ret = tablet_manager_->prepare_merge_tablets(frozen_version)))
      {
        YYSYS_LOG(ERROR, "not prepared for new merge process, version=%ld", frozen_version);
      }
      else
      {
        YYSYS_LOG(INFO, "new merge process, version=%ld, frozen_timestamp_=%ld",
            frozen_version, frozen_timestamp_);
      }

      return ret;
    }

    int ObChunkMerge::finish_round(const int64_t frozen_version)
    { 
      //add liuxiao [secondary index static index builder] 20150611
      //����Ϊ�˲�Ӱ��㱨���̣�����ret���и�ֵ
      bool if_has_new_index_this_time = false;
      ObArray<uint64_t> index_id_list;
      current_schema_.get_all_init_index_tid(index_id_list);
      if(OB_SUCCESS != (current_schema_.get_all_init_index_tid(index_id_list)))
      {
        //�����������ξ��Ȳ���recycle
        if_has_new_index_this_time = true;
      }
      else
      {
        if(index_id_list.count()>0)
        {
          //�����������������������0�����Ҳ����recycle
          if_has_new_index_this_time = true;
        }
        else
        {
          //�����������recycle
          if_has_new_index_this_time = false;
        }
      }
      //add e


      int ret = OB_SUCCESS;
      pending_in_upgrade_ = true;
      tablet_manager_->sync_all_tablet_images();  //ignore err
      if ( OB_SUCCESS != (ret = tablet_manager_->build_unserving_cache()) )
      {
        YYSYS_LOG(WARN,"switch cache failed");
      }
      else if ( OB_SUCCESS != (ret = tablet_manager_->get_serving_tablet_image().upgrade_service(is_really_merged_)) )
      {
        //[bugfix_empty_merge]
          if(ret == OB_CS_EMPTY_MERGE)
          {
              tablet_manager_->drop_unserving_cache();
              int tmp_ret = OB_SUCCESS;
              ObTablet *tmp_tablet = NULL;
              ObTablet *tmp_tablet_array[TABLET_COUNT_PER_MERGE];
              int64_t tmp_tablets_num = sizeof(tmp_tablet_array)/sizeof(tmp_tablet_array[0]);
              tmp_ret = tablet_manager_->get_serving_tablet_image().get_tablets_for_merge(
                          frozen_version_, tmp_tablets_num, tmp_tablet_array, false);
              if(tmp_ret != OB_SUCCESS)
              {
                  YYSYS_LOG(WARN, "get tablets failed : [%d]", tmp_ret);
              }
              else if(OB_SUCCESS == tmp_ret && tmp_tablets_num > 0)
              {
                  for(int64_t it = 0; it < tmp_tablets_num; it++)
                  {
                      tmp_tablet = tmp_tablet_array[it];
                      if(tmp_tablet->is_merged())
                      {
                          tmp_tablet->set_merged(0);
                      }
                      if(tablet_manager_->get_serving_tablet_image().release_tablet(tmp_tablet) != OB_SUCCESS)
                      {
                          YYSYS_LOG(WARN, "release tablet failed");
                      }
                  }
                  YYSYS_LOG(INFO, "mark %ld merged tablets as unmerged successfully", tmp_tablets_num);
              }
          }
          else
          {
              YYSYS_LOG(ERROR, "upgrade_service to version = %ld failed", frozen_version);
          }

      }
      else
      {
        YYSYS_LOG(INFO,"this version (%ld) is merge done,to switch cache and upgrade",frozen_version);
        // wait for other worker threads release old cache,
        // switch to use new cache.
        tablet_manager_->get_join_cache().destroy();
        tablet_manager_->switch_cache(); //switch cache
        tablet_manager_->get_disk_manager().scan(THE_CHUNK_SERVER.get_config().datadir,
                                                 OB_DEFAULT_MAX_TABLET_SIZE);
        // report tablets
        if (OB_RESPONSE_TIME_OUT == tablet_manager_->report_tablets())
        {
          THE_CHUNK_SERVER.schedule_report_tablet();
        }
        else
        {
          tablet_manager_->report_capacity_info();
        }

        // upgrade complete, no need pending, for migrate in.
        pending_in_upgrade_ = false;
        round_end_ = true;

        //[433]
        //::usleep(static_cast<useconds_t>(THE_CHUNK_SERVER.get_config().min_drop_cache_wait_time));
        const int32_t min_drop_cache_wait_time = (int32_t)(THE_CHUNK_SERVER.get_config().min_drop_cache_wait_time / 1000000);
        sleep(min_drop_cache_wait_time);

        // now we suppose old cache not used by others,
        // drop it.
        tablet_manager_->drop_unserving_cache(); //ignore err
        // re scan all local disk to recycle sstable
        // left by RegularRecycler. e.g. migrated sstable.
        //modify liuxiao [secondary index static index builder] 20150611
        //tablet_manager_->get_scan_recycler().recycle();
        if(!if_has_new_index_this_time)
        {
          //������µ���������recycle
          tablet_manager_->get_scan_recycler().recycle();
        }
        //modify end
        merge_last_end_time_ = yysys::CTimeUtil::getTime();
      }
      return ret;
    }

    /**
     * @brief get a tablet to merge,get lock first
     *
     * @return
     */
    int ObChunkMerge::get_tablets(ObTablet* &tablet)
    {
      tablet = NULL;
      int err = OB_SUCCESS;
      //int64_t print_step = thread_num_ > 0 ? thread_num_ : 10;
      if (tablets_num_ > 0 && tablet_index_ < tablets_num_)
      {
          int64_t tablet_num = 0;
          int64_t merged_num = 0;
          tablet_manager_->get_serving_tablet_image().get_tablet_merge_status(tablet_num, merged_num);
          if(merged_num % (tablet_num / 10 +1) == 0)
          {
              int64_t seconds = (yysys::CTimeUtil::getTime() - merge_start_time_) / 1000L / 1000L;
              YYSYS_LOG(INFO, "merge consume seconds:%lds, minutes:%.2fm, hours:%.2fh, merge process:%s",
                        seconds, (double)seconds / 60.0, (double)seconds / 3600.0,
                        tablet_manager_->get_serving_tablet_image().print_tablet_image_stat());
          }
          for(; tablet_index_ < tablets_num_;)
          {
              usleep(10000);
              tablet = tablet_array_[tablet_index_++];
              //
              if(NULL == tablet || (!tablet->is_merging() && !tablet->is_merged()))
              {
                  //
                  if(NULL != tablet)
                  {
                      tablet->set_merging();
                  }
                  else
                  {
                      YYSYS_LOG(WARN, "tablet that get from tablet image is null");
                  }
                  return err; //dduuhhtt
              }
              if(tablet_manager_->get_serving_tablet_image().release_tablet(tablet) != OB_SUCCESS)
              {
                  YYSYS_LOG(WARN, "dec reference count failed");
              }
          }
          tablet = NULL;
      }
      else if ( (tablet_index_ == tablets_num_) &&
          (frozen_version_ > tablet_manager_->get_serving_data_version()) )
      {

        while(OB_SUCCESS == err)
        {
          if (round_start_)
          {
            round_start_ = false;
          }

          tablets_num_ = sizeof(tablet_array_) / sizeof(tablet_array_[0]);
          tablet_index_ = 0;
          //tablets_have_got_ = 0;

          YYSYS_LOG(DEBUG,"get tablet from tablet image, frozen_version_=%ld, tablets_num_=%ld",
              frozen_version_, tablets_num_);

          //block for migrating to add tablet
          SpinWLockGuard rwlock(migrate_lock_);

          err = tablet_manager_->get_serving_tablet_image().get_tablets_for_merge(
              frozen_version_, tablets_num_,tablet_array_);

          if (err != OB_SUCCESS)
          {
            YYSYS_LOG(WARN,"get tablets failed : [%d]",err);
          }
          else if (OB_SUCCESS == err && tablets_num_ > 0)
          {
            YYSYS_LOG(DEBUG,"get %ld tablets from tablet image",tablets_num_);
            for(; tablet_index_ < tablets_num_;)
            {
                usleep(10000);
                tablet = tablet_array_[tablet_index_++];
                if(NULL == tablet || (!tablet->is_merging() && !tablet->is_merged()))
                {
                    //
                    if(NULL != tablet)
                    {
                        tablet->set_merging();
                    }

                    return err; //dduuhhtt
                }
                if(tablet_manager_->get_serving_tablet_image().release_tablet(tablet) != OB_SUCCESS)
                {
                    YYSYS_LOG(WARN, "dec reference count failed");
                }
            }
            tablet = NULL;
            return err;

            //break; //got it
          }
          else if (!round_end_)
          {
            if (OB_SUCCESS == (err = finish_round(frozen_version_)))
            {
              break;
            }
          }
          else
          {
            //impossible
            YYSYS_LOG(WARN,"can't get tablets and is not round end");
            break;
          }
        }
      }
      else
      {
        YYSYS_LOG(DEBUG,"tablets_num_:%ld,tablets_have_got_:%ld,"
                       "frozen_version_:%ld,serving data version:%ld",
                       tablets_num_,tablets_have_got_,
                       frozen_version_,tablet_manager_->get_serving_data_version());
      }

      return err;
    }

    bool ObChunkMerge::have_new_version_in_othercs(const ObTablet* tablet)
    {
      bool ret = false;
      //add zhaoqiong [MultiUPS] [RootTable_Cache] 20150703:b
      int32_t cluster_id = (int32_t)ObChunkServerMain::get_instance()->get_chunk_server().get_config().cluster_id;
      //add:e

      if (tablet != NULL)
      {
        //add zhaoqiong[roottable tablet management]20150302:b
	    //ObTabletLocation list[OB_SAFE_COPY_COUNT];
        //int32_t size = OB_SAFE_COPY_COUNT;
        ObTabletLocation list[OB_MAX_COPY_COUNT];
        int32_t size = OB_MAX_COPY_COUNT;
		//add e
		
        int32_t new_copy = 0;
        int err = CS_RPC_CALL_RS(get_tablet_info,  current_schema_,
            tablet->get_range().table_id_, tablet->get_range(), list, size);
        if (OB_SUCCESS == err)
        {
          for(int32_t i=0; i<size; ++i)
          {
            YYSYS_LOG(INFO,"version:%ld",list[i].tablet_version_); //for test
            //mod zhaoqiong [MultiUPS] [RootTable_Cache] 20150703:b
            //if (list[i].tablet_version_ > tablet->get_data_version())
            if (list[i].tablet_version_ > tablet->get_data_version() && list[i].cluster_id_ == cluster_id )
            //mod:e
            {
              ++new_copy;
            }
          }
        }
        //mod zhaoqiong [MultiUPS] [RootTable_Cache] 20150703:b
        //if (OB_SAFE_COPY_COUNT - 1 <= new_copy)
        if (OB_DEFAULT_COPY_COUNT - 1 <= new_copy)
        //mod:e
        {
          ret = true;
        }
      }
      return ret;
    }

    bool ObChunkMerge::check_load()
    {
      bool ret = false;
      double loadavg[3];

      // ObChunkServer&  chunk_server = ObChunkServerMain::get_instance()->get_chunk_server();
      volatile int64_t current_request_count_ = 0;

      if (getloadavg(loadavg, static_cast<int>(sizeof(loadavg)/sizeof(loadavg[0]))) < 0)
      {
        YYSYS_LOG(WARN,"getloadavg failed");
        loadavg[0] = 0;
      }

      ObStat *stat = NULL;
      
      OB_STAT_GET(CHUNKSERVER, stat);
      if (NULL == stat)
      {
        //YYSYS_LOG(WARN,"get stat failed");
        current_request_count_  = 0;
      }
      else
      {
        current_request_count_ = stat->get_value(INDEX_META_REQUEST_COUNT_PER_SECOND);
      }


      pthread_mutex_lock(&mutex_);
      if (active_thread_num_ <= min_merge_thread_num_)
      {
        YYSYS_LOG(INFO, "current active thread :%ld < min merge thread: %ld, continue run.",
            active_thread_num_, min_merge_thread_num_);
        ret = true;
      }
      // check load and request if match the merge conditions.
      if ((loadavg[0]  < merge_load_high_)
          && (current_request_count_ < request_count_high_))
      {
        ret = true;
        int64_t sleep_thread = thread_num_ - active_thread_num_;
        int64_t remain_load = merge_load_high_ - static_cast<int64_t>(loadavg[0]) - 1; //loadavg[0] double to int
        //int64_t remain_tablet = tablets_num_ - tablets_have_got_;
        int64_t tablet_num = 0;
        int64_t merged_num = 0;
        if(NULL != tablet_manager_)
        {
            const ObMultiVersionTabletImage &tablet_image = tablet_manager_->get_serving_tablet_image();
            tablet_image.get_tablet_merge_status(tablet_num, merged_num);
        }
        else
        {
            YYSYS_LOG(WARN, "tablet_manager_ is null");
        }
        int64_t remain_tablet = tablet_num - merged_num;


        if ((loadavg[0] < merge_load_adjust_) &&
            (remain_tablet > active_thread_num_) &&
            (sleep_thread > 0) && (remain_load > 0) )
        {
          YYSYS_LOG(INFO,"wake up %ld thread(s)",sleep_thread > remain_load ? remain_load : sleep_thread);
          while(sleep_thread-- > 0 && remain_load-- > 0)
          {
            pthread_cond_signal(&cond_);
          }
        }
      }
      else
      {
        YYSYS_LOG(INFO,"loadavg[0] : %f,merge_load_high_:%ld,current_request_count_:%ld,request_count_high_:%ld",
            loadavg[0],merge_load_high_,current_request_count_,request_count_high_);
      }
      pthread_mutex_unlock(&mutex_);
      return ret;
    }


    int ObChunkMerge::serialize_cs_merge_stat(char *buf, const int64_t buf_len, int64_t &pos) const
    {
        int ret = OB_SUCCESS;
        int64_t merge_start_time = 0;
        int64_t tablet_num = 0;
        int64_t merged_num = 0;
        int64_t serving_version = 0;
        int64_t merging_version = 0;
        int64_t latest_frozen_version = 0;

        //

        merge_start_time = merge_start_time_;
        tablet_manager_->get_serving_tablet_image().get_tablet_merge_status(tablet_num, merged_num);
        serving_version = tablet_manager_->get_serving_data_version();
        merging_version = frozen_version_;
        latest_frozen_version = newest_frozen_version_;

        if(OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, merge_start_time)))
        {
            YYSYS_LOG(WARN, "serialize merge_start_time error");
        }
        else if(OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, tablet_num)))
        {
            YYSYS_LOG(WARN, "serialize tablet_num error");
        }
        else if(OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, merged_num)))
        {
            YYSYS_LOG(WARN, "serialize merged_num error");
        }
        else if(OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, serving_version)))
        {
            YYSYS_LOG(WARN, "serialize serving_version error");
        }
        else if(OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, merging_version)))
        {
            YYSYS_LOG(WARN, "serialize merging_version error");
        }
        else if(OB_SUCCESS != (ret = serialization::encode_vi64(buf, buf_len, pos, latest_frozen_version)))
        {
            YYSYS_LOG(WARN, "serialize latest_frozen_version error");
        }
        //
        return ret;
    }

  } /* chunkserver */
} /* oceanbase */
