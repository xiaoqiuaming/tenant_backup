



#include "ob_chunk_statistics_collector.h"
#include "ob_chunk_server_main.h"
#include "common/ob_read_common_data.h"
#include "ob_tablet_image.h"
#include "common/utility.h"
#include "sstable/ob_disk_path.h"
#include "common/ob_trace_log.h"
#include "ob_tablet_manager.h"
#include "common/ob_atomic.h"
#include "common/file_directory_utils.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_scanner.h"
#include "common/ob_define.h"
#include "common/ob_mod_define.h"
#include "ob_statistics_builder.h"

namespace oceanbase
{
   namespace chunkserver
   {
      using namespace yyutil;
      using namespace common;
      using namespace sstable;

   /*-------------------------------------
    * Thread for statistics building
    *-------------------------------------*/

      ObStatisticsCollector::ObStatisticsCollector():inited_(false),is_start_gather_(false),is_start_merging_(false),thread_num_(0),min_work_thread_num_(0)
      {

          finished_tasks_num_ = 0;
          total_tasks_num_ = 0;

      }

      int ObStatisticsCollector::init(ObTabletManager *manager)
      {
          int ret = OB_SUCCESS;
          ObChunkServer& chunk_server = ObChunkServerMain::get_instance()->get_chunk_server();
          if(NULL == manager)
          {
              YYSYS_LOG(ERROR, "initialize index worker failed,null pointer");
              ret = OB_ERROR;
          }
          else if(!inited_)
          {
              inited_ = true;
              tablet_manager_ = manager;
              pthread_mutex_init(&mutex_, NULL);
              pthread_mutex_init(&phase_mutex_, NULL);
              pthread_cond_init(&cond_, NULL);
              int64_t max_work_thread_num = chunk_server.get_config().gather_thread_num;
              if(max_work_thread_num <= 0 || max_work_thread_num > MAX_WORK_THREAD)
              {
                  max_work_thread_num = MAX_WORK_THREAD;
              }
              if(OB_SUCCESS != (ret = set_config_param()))
              {
                  YYSYS_LOG(ERROR,"failed to set index work param[%d]", ret);
              }
              else if(OB_SUCCESS != (ret = create_work_thread(MAX_WORK_THREAD)))
              {
                  YYSYS_LOG(ERROR,"failed to initialize thread for statistics[%d]",ret);
              }
              else if(OB_SUCCESS != (ret = create_all_gather_workers()))
              {
                  YYSYS_LOG(ERROR,"failed to create all index builder[%d]",ret);
              }
          }
          else
          {
              YYSYS_LOG(WARN, "statistics worker has been inited");
          }
          if(OB_SUCCESS != ret && inited_)
          {
              pthread_mutex_destroy(&mutex_);
              pthread_mutex_destroy(&phase_mutex_);
              pthread_cond_destroy(&cond_);
              inited_ = false;
          }
          return ret;
      }

      int ObStatisticsCollector::schedule()
      {
          int ret = OB_SUCCESS;
          if(!can_launch_next_round())
          {
              YYSYS_LOG(INFO, "can't launch next round.");
              ret = OB_CS_EAGAIN;
          }
          else if(0 == tablet_manager_->get_serving_data_version())
          {



              YYSYS_LOG(INFO, "chunkserver empty tablet!");
              is_start_gather_ = false;
              gather_list_.clear();
              ret = OB_CS_EMPTY_TABLET;

          }
          if(inited_ && OB_SUCCESS == ret && thread_num_ > 0)
          {
              local_work_start_time_ = yysys::CTimeUtil::getTime();
              set_merging_tag(false);
              pthread_cond_broadcast(&cond_);
          }
          return ret;
      }


      void ObStatisticsCollector::destroy()
      {
          local_work_complete_time_ = yysys::CTimeUtil::getTime();
          pthread_mutex_destroy(&mutex_);
          pthread_mutex_destroy(&phase_mutex_);
          pthread_cond_destroy(&cond_);
          inited_ = false;
      }


      int ObStatisticsCollector::set_config_param()
      {
          int ret = OB_SUCCESS;
          gather_list_.init(OB_MAX_COLLECT_COLUMN_NUM, gather_info_);
          return ret;
      }





      int ObStatisticsCollector::create_work_thread(const int64_t max_work_thread)
      {
          int ret = OB_SUCCESS;
          setThreadCount(static_cast<int32_t>(max_work_thread));
          active_thread_num_ = max_work_thread;
          thread_num_ = start();

          if(0 >= thread_num_)
          {
              YYSYS_LOG(ERROR, "start thread failed");
              ret = OB_ERROR;
          }
          else
          {
              if(thread_num_ != max_work_thread)
              {
                  YYSYS_LOG(WARN, "failed to start [%ld] threads to build statistics,there is [%ld] threads", max_work_thread, thread_num_);
              }
              min_work_thread_num_ = thread_num_ / 3;
              if(0 == min_work_thread_num_) min_work_thread_num_ = 1;
              YYSYS_LOG(INFO, "Statistics work thread_num=%ld "
                        "active_thread_num_=%ld, min_merge_thread_num_=%ld",
                        thread_num_, active_thread_num_, min_work_thread_num_);
          }
          return ret;
      }

      int ObStatisticsCollector::create_all_gather_workers()
      {
          int ret = OB_SUCCESS;
          YYSYS_LOG(INFO, "NOW START CREATE STATISTICS BUILDERS");
          if(OB_SUCCESS != (ret = create_statistics_builders(builder_, MAX_WORK_THREAD)))
          {
              YYSYS_LOG(ERROR, "failed to create statistics builders");
          }
          return ret;
      }

      int ObStatisticsCollector::create_statistics_builders(ObStatisticsBuilder **builder, const int64_t size)
      {
          int ret = OB_SUCCESS;
          char* ptr = NULL;
          if(NULL == builder || 0 > size)
          {
              ret = OB_INVALID_ARGUMENT;
              YYSYS_LOG(ERROR, "the pointer of statistics builder is null");
          }
          else if(NULL == (ptr = reinterpret_cast<char*>(ob_malloc(sizeof(ObStatisticsBuilder)*size, ObModIds::OB_STATISTICS_BUILD))))
          {
              YYSYS_LOG(WARN, "allocate memory for statistics builder object error");
              ret = OB_ALLOCATE_MEMORY_FAILED;
          }
          else if(NULL == tablet_manager_)
          {
              ret = OB_INVALID_ARGUMENT;
          }
          else
          {
              for(int64_t i = 0; i < size; i++)
              {
                  ObStatisticsBuilder* builders = new(ptr+i*sizeof(ObStatisticsBuilder)) ObStatisticsBuilder(this, tablet_manager_);
                  if(NULL == builders || OB_SUCCESS != (ret= builders->init()))
                  {
                      YYSYS_LOG(WARN, "Init statistics builder error, ret[%d]", ret);
                      ret = OB_ERROR;
                      break;
                  }
                  else
                  {
                      builder[i] = builders;
                      YYSYS_LOG(INFO,"Create statistics builder[%ld]",i);
                  }
              }
          }
          return ret;
      }

      void ObStatisticsCollector::construct_statistics(const int64_t thread_no)
      {
          int ret = OB_SUCCESS;
          int err = OB_SUCCESS;
          const int64_t sleep_interval = 5000000;
          ObVector<ObTablet*> tablet_list;
          ObGatherTableInfo* gather_info = NULL;
          ObStatisticsBuilder* builder = NULL;


          pthread_mutex_lock(&mutex_);
          ObChunkServerMain::get_instance()->get_chunk_server().reload_config();
          while(thread_no > ObChunkServerMain::get_instance()->get_chunk_server().get_config().gather_thread_num)
          {
              --active_thread_num_;
              YYSYS_LOG(INFO, "Thread[%ld] exceeds the limitation of No[].", thread_no);
              pthread_cond_wait(&cond_, &mutex_);
              YYSYS_LOG(INFO, "Awaked by signal,active_thread_num_=:%ld", active_thread_num_);
              ObChunkServerMain::get_instance()->get_chunk_server().reload_config();
              ++active_thread_num_;
          }
          pthread_mutex_unlock(&mutex_);


          while(true)
          {
              if(!inited_)
              {
                  break;
              }
              pthread_mutex_lock(&mutex_);
              ret = get_collection_task(gather_info, err);
              while(true)
              {
                  if(OB_GET_NOTHING == err && OB_SUCCESS != ret)
                  {
                      pthread_mutex_unlock(&mutex_);
                      usleep(sleep_interval);
                      pthread_mutex_lock(&mutex_);
                  }
                  else if(OB_GET_NOTHING == err)
                  {
                      --active_thread_num_;
                      YYSYS_LOG(INFO, "there is no tablet need gather statistics,sleep wait for new statistics process.");
                      pthread_cond_wait(&cond_,&mutex_);
                      ++active_thread_num_;
                      YYSYS_LOG(INFO, "awake by signal,active_thread_num_=:%ld", active_thread_num_);
                      ObChunkServerMain::get_instance()->get_chunk_server().reload_config();
                      while(thread_no > ObChunkServerMain::get_instance()->get_chunk_server().get_config().gather_thread_num)
                      {
                          --active_thread_num_;
                          YYSYS_LOG(INFO, "Thread[%ld] exceeds the limitation of No[].", thread_no);
                          pthread_cond_wait(&cond_, &mutex_);
                          YYSYS_LOG(INFO, "awake by signal,active_thread_num_=:%ld", active_thread_num_);
                          ObChunkServerMain::get_instance()->get_chunk_server().reload_config();
                          ++active_thread_num_;
                      }
                  }
                  else
                  {
                      break;
                  }
                  ret = get_collection_task(gather_info, err);
              }
              pthread_mutex_unlock(&mutex_);
              if(OB_SUCCESS == err)
              {
                  tablet_list.clear();
                  if(OB_SUCCESS != (ret = fetch_tablet_by_tid(tablet_list,gather_info->table_id_,gather_info->replication_idx_)))
                  {


                      YYSYS_LOG(WARN, "get collection tablet_list failed, ret[%d]",ret);

                  }
                  else if(OB_SUCCESS != (ret = get_statistics_builder(thread_no, builder)))
                  {
                      YYSYS_LOG(ERROR, "get statistics builder failed, ret[%d]",ret);
                  }
                  else
                  {
                      int err_local = OB_SUCCESS;
                      YYSYS_LOG(INFO, "now statistics builder begin to work!");
                      if(0 == tablet_list.size())
                      {




                      }
                      else
                      {
                          for(int32_t i = 0; i < tablet_list.size(); i++)
                          {
                              if(OB_SUCCESS != (err_local = builder->start(tablet_list.at(i), *gather_info)))
                              {
                                  YYSYS_LOG(WARN, "build partitional statistics failed,err[%d]", err_local);
                              }







                          }
                      }
                      ++finished_tasks_num_;
                      tablet_list.reset();
                  }
                  if(tablet_manager_->is_stoped())
                  {
                      YYSYS_LOG(WARN, "stop in Statistics");
                      ret = OB_ERROR;
                  }
              }
          }
      }

      void ObStatisticsCollector::run(yysys::CThread *thread, void *arg)
      {
          UNUSED(thread);
          SET_THD_NAME("csstat-col");//[647]
          int64_t thread_no = reinterpret_cast<int64_t>(arg);
          construct_statistics(thread_no);
      }

      int ObStatisticsCollector::get_statistics_builder(const int64_t thread_no, ObStatisticsBuilder *&builder)
      {
          int ret = OB_SUCCESS;
          builder = NULL;
          ObStatisticsBuilder **builders = NULL;
          builders = builder_;
          if(thread_no >= MAX_WORK_THREAD)
          {
              YYSYS_LOG(ERROR, "thread_no=%ld >= max_work_thread_num=%ld", thread_no, MAX_WORK_THREAD);
              ret = OB_SIZE_OVERFLOW;
          }
          else if(NULL == builders)
          {
              YYSYS_LOG(ERROR, "thread_no=%ld >= max_work_thread_num=%ld", thread_no, MAX_WORK_THREAD);
              ret = OB_SIZE_OVERFLOW;
          }
          else if(NULL == (builder = builders[thread_no]))
          {
              YYSYS_LOG(ERROR, "thread_no=%ld builders is NULL", thread_no);
              ret = OB_INVALID_ARGUMENT;
          }
          return ret;
      }

      int ObStatisticsCollector::get_collection_task(ObGatherTableInfo *&gather_info, int &err)
      {
          int ret = OB_SUCCESS;
          err = OB_GET_NOTHING;
          YYSYS_LOG(DEBUG, "The merge stat is %d",is_start_merging_);
          if(is_start_gather_ && !is_start_merging_)
          {
              if(gather_list_.get_array_index()>0)
              {
                  gather_info = gather_list_.pop();
                  err = OB_SUCCESS;
              }
              else
              {
                  err = OB_GET_NOTHING;
                  is_start_gather_ = false;
                  gather_list_.clear();
              }
          }
          else if(is_start_merging_)
          {
              gather_list_.clear();
              YYSYS_LOG(INFO, "Is merging, clear the task list!");
              is_start_gather_ = false;
              err = OB_GET_NOTHING;
          }
          return ret;
      }

      int ObStatisticsCollector::fetch_tablet_by_tid(ObVector<ObTablet *> &tablet_list, uint64_t table_id, uint64_t replication_idx)
      {
          int ret = OB_SUCCESS;
          const int64_t timeout = 2000000;
          bool need_release_all_tablet = false;
          ObVector<ObTablet*> tmp_list;
          hash::ObHashMap<ObNewRange, ObTabletLocationList> range_hash;
          common::ModulePageAllocator mod(ObModIds::OB_STATISTICS_BUILD);
          ModuleArena temp_allocator(ModuleArena::DEFAULT_PAGE_SIZE,mod);
          if(OB_SUCCESS != (ret = range_hash.create(hash::cal_next_prime(512))))
          {
              YYSYS_LOG(ERROR, "init collection range hash error, ret=%d", ret);
          }
          if(OB_INVALID_ID == table_id)
          {
              ret = OB_ERROR;
              YYSYS_LOG(WARN, "fetch table_id is INVALID!");
          }
          else
          {
              tmp_list.clear();
              if(OB_SUCCESS != (ret = tablet_manager_->get_serving_tablet_image().get_serving_image().acquire_tablets_pub(table_id,tmp_list)))
              {
                  for(ObVector<ObTablet*>::iterator tablet = tmp_list.begin(); tablet != tmp_list.end(); ++tablet)
                  {
                      if(OB_SUCCESS != (ret = tablet_manager_->get_serving_tablet_image().release_tablet(*tablet)))
                      {
                          YYSYS_LOG(WARN, "release tablet array failed, ret = [%d]", ret);
                      }
                  }
                  YYSYS_LOG(WARN, "failed to acquire tablets, ret=%d", ret);
              }

              if(OB_SUCCESS == ret)
              {
                  ObGeneralRpcStub rpc_stub = THE_CHUNK_SERVER.get_rpc_stub();
                  ObScanner scanner;
                  ObRowkey start_key;
                  tablet_list.clear();

                  start_key.set_min_row();
                  {
                      do
                      {
                          ret = rpc_stub.fetch_tablet_location(timeout, THE_CHUNK_SERVER.get_root_server(),0,table_id,start_key,scanner);
                          if(OB_SUCCESS != ret)
                          {
                              YYSYS_LOG(WARN, "fetch location failed! there is no tablet in root_table, ret=%d",ret);
                              need_release_all_tablet = true;
                              break;
                          }
                          else
                          {
                              ret = parse_location_from_scanner(scanner, start_key, table_id, replication_idx, range_hash, temp_allocator);
                          }
                          if(ret != OB_SUCCESS)
                          {
                              YYSYS_LOG(WARN, "parse tablet info from ObScanner failed, ret=[%d]", ret);
                              need_release_all_tablet = true;
                              break;
                          }
                          else if(ObRowkey::MAX_ROWKEY == start_key)
                          {
                              YYSYS_LOG(INFO, "get all tablets info from rootserver success");
                              break;
                          }
                          else
                          {
                              YYSYS_LOG(DEBUG, "nee more request to get next tablet info");
                          }
                          scanner.reset();
                      }
                      while(true);
                  }
                  if(OB_SUCCESS == ret)
                  {
                      bool is_primary_replication = false;
                      for(ObVector<ObTablet*>::iterator it = tmp_list.begin(); it != tmp_list.end(); ++it)
                      {
                          ObTabletLocationList list;
                          if(OB_SUCCESS != (ret = is_tablet_need_gather_statistics(*it, list, is_primary_replication, range_hash)))
                          {
                              YYSYS_LOG(ERROR, "error in is_tablet_need_gather_statistics, ret[%d]", ret);
                          }
                          else if(is_primary_replication)
                          {
                              tablet_list.push_back(*it);
                          }
                          else
                          {
                              if(OB_SUCCESS != (ret = tablet_manager_->get_serving_tablet_image().release_tablet(*it)))
                              {
                                  YYSYS_LOG(WARN,"release tablet array failed, ret = [%d]", ret);
                              }
                          }
                      }
                  }
                  else if(need_release_all_tablet)
                  {
                      for(ObVector<ObTablet*>::iterator tmp_it = tmp_list.begin(); tmp_it != tmp_list.end(); ++tmp_it)
                      {
                          if(OB_SUCCESS != (ret = tablet_manager_->get_serving_tablet_image().release_tablet(*tmp_it)))
                          {
                              YYSYS_LOG(WARN, "release tablet array failed, ret = [%d]", ret);
                              break;
                          }
                      }
                  }
              }
          }
          temp_allocator.free();
          range_hash.destroy();
          return ret;
      }

      int ObStatisticsCollector::is_tablet_need_gather_statistics(ObTablet *tablet, ObTabletLocationList &list, bool &is_primary_replication,
                                                                  hash::ObHashMap<ObNewRange, ObTabletLocationList> &range_hash)
      {
          int ret = OB_SUCCESS;
          int err = hash::HASH_EXIST;
          is_primary_replication = false;
          if(NULL == tablet)
          {
              YYSYS_LOG(ERROR, "null pointer for tablet");
              ret = OB_ERROR;
          }
          else
          {
              ObNewRange range = tablet->get_range();
              if(hash::HASH_EXIST == (err = range_hash.get(range, list)))
              {
                  is_primary_replication = true;
              }
              else
              {
                  is_primary_replication = false;
              }
          }
          YYSYS_LOG(DEBUG, " sstable count=%ld",tablet->get_sstable_id_list().count());
          return ret;
      }

      bool ObStatisticsCollector::can_launch_next_round()
      {
          bool ret = false;
          if(inited_ && can_work_start())
          {
              ret = true;
          }
          else
          {
              YYSYS_LOG(ERROR, "Failed to launch, init[%d],work_stat[%d]", inited_, can_work_start());
          }
          return ret;
      }

      int ObStatisticsCollector::check_self()
      {
          int ret = OB_SUCCESS;
          //@TO DO
          return ret;
      }

      template <typename Allocator>
      int ObStatisticsCollector::parse_location_from_scanner(ObScanner &scanner, ObRowkey& row_key, uint64_t table_id
               , uint64_t replication_idx, hash::ObHashMap<ObNewRange, ObTabletLocationList> &range_hash, Allocator& temp_allocator)
      {
          int ret = OB_SUCCESS;
          int64_t idx;
          ObRowkey start_key;
          start_key = ObRowkey::MIN_ROWKEY;
          ObRowkey end_key;
          ObServer server;
          ObCellInfo* cell = NULL;
          bool row_change = false;
          ObTabletLocationList list;
          ObScannerIterator iter = scanner.begin();
          ObNewRange range;
          ++iter;
          while((iter != scanner.end())
                && (OB_SUCCESS == (ret = iter.get_cell(&cell, &row_change))) && !row_change)
          {
              if(NULL == cell)
              {
                  ret = OB_INNER_STAT_ERROR;
                  break;
              }
              cell->row_key_.deep_copy(start_key, temp_allocator);
              ++iter;
          }

          if(OB_SUCCESS == ret)
          {
              int64_t ip = 0;
              int64_t port = 0;
              //next cell
              for(++iter; iter != scanner.end(); ++iter)
              {
                  ret = iter.get_cell(&cell, &row_change);
                  if(OB_SUCCESS != ret)
                  {
                      YYSYS_LOG(ERROR, "get cell from scanner iterator failed:ret[%d]", ret);
                      break;
                  }
                  else if(row_change)
                  {
                      construct_tablet_item(table_id, start_key, end_key, range, list);
                      idx = replication_idx%list.size();
                      YYSYS_LOG(DEBUG, "The size is %ld idx is %ld",list.size(),idx);
                      if(list[idx].server_.chunkserver_.get_ipv4() == THE_CHUNK_SERVER.get_self().get_ipv4())
                      {
                          if(-1 == range_hash.set(list.get_tablet_range(), list, 1))
                          {
                              YYSYS_LOG(ERROR, "insert range_hash_ error!");
                          }
                      }
                      list.clear();
                      start_key = end_key;
                  }
                  else
                  {
                      cell->row_key_.deep_copy(end_key, temp_allocator);
                      if((cell->column_name_.compare("1_port") == 0)
                              ||(cell->column_name_.compare("2_port")== 0)
                              ||(cell->column_name_.compare("3_port")== 0)
                              ||(cell->column_name_.compare("4_port")== 0)
                              ||(cell->column_name_.compare("5_port")== 0)
                              ||(cell->column_name_.compare("6_port")== 0))
                      {
                          ret = cell->value_.get_int(port);
                      }
                      else if((cell->column_name_.compare("1_ipv4") == 0)
                              ||(cell->column_name_.compare("2_ipv4")== 0)
                              ||(cell->column_name_.compare("3_ipv4")== 0)
                              ||(cell->column_name_.compare("4_ipv4")== 0)
                              ||(cell->column_name_.compare("5_ipv4")== 0)
                              ||(cell->column_name_.compare("6_ipv4")== 0))
                      {
                          ret = cell->value_.get_int(ip);
                          if(OB_SUCCESS == ret)
                          {
                              if(port == 0)
                              {
                                  YYSYS_LOG(WARN, "check port failed:ip[%ld], port[%ld]", ip, port);
                              }
                              server.set_ipv4_addr(static_cast<int32_t>(ip),static_cast<int32_t>(port));
                              ObTabletLocation addr(0, server);
                              if(OB_SUCCESS != (ret = list.add(addr)))
                              {
                                  YYSYS_LOG(ERROR, "add addr failed:ip[%ld], port[%ld], ret[%d]",
                                            ip, port, ret);
                                  break;
                              }
                              else
                              {
                                  YYSYS_LOG(DEBUG,"add addr succ:ip[%ld], port[%ld], server;%s", ip, port, to_cstring(server));
                              }
                              ip = port = 0;
                          }
                      }
                      if(ret != OB_SUCCESS)
                      {
                          YYSYS_LOG(ERROR, "check get value failed, ret[%d]",ret);
                          break;
                      }
                  }
              }

              YYSYS_LOG(DEBUG, "get a new tablet start_key[%s], end_key[%s]",
                        to_cstring(start_key), to_cstring(end_key));
              if((OB_SUCCESS == ret) && (start_key != end_key))
              {
                  construct_tablet_item(table_id, start_key, end_key, range, list);
                  idx = replication_idx%list.size();
                  YYSYS_LOG(DEBUG,"The size is %ld idx is %ld",list.size(),idx);
                  if(list[idx].server_.chunkserver_.get_ipv4() == THE_CHUNK_SERVER.get_self().get_ipv4())
                  {
                      if(-1 == range_hash.set(list.get_tablet_range(), list, 1))
                      {
                          YYSYS_LOG(ERROR, "insert range hash error!");
                          ret = OB_ERROR;
                      }
                  }
              }
          }
          else
          {
              YYSYS_LOG(ERROR, "check get first row cell failed:ret[%d]", ret);
          }
          if(OB_SUCCESS == ret)
          {
              row_key = end_key;
          }
          return ret;
      }


      void ObStatisticsCollector::construct_tablet_item(const uint64_t table_id,
             const ObRowkey &start_key, const ObRowkey &end_key, ObNewRange &range,
             ObTabletLocationList &list)
      {
          range.table_id_ = table_id;
          range.border_flag_.unset_inclusive_start();
          range.border_flag_.set_inclusive_end();
          range.start_key_ = start_key;
          range.end_key_ = end_key;
          if(range.end_key_.is_max_row())
          {
              range.border_flag_.unset_inclusive_end();
          }
          list.set_timestamp(yysys::CTimeUtil::getTime());
          list.set_tablet_range(range);
          if(range.start_key_ > range.end_key_)
          {
              YYSYS_LOG(WARN, "check range invalid:start[%s], end[%s]",
                        to_cstring(range.start_key_), to_cstring(range.end_key_));
          }
          else
          {
              YYSYS_LOG(DEBUG,"got a tablet:%s, with location list:%ld",to_cstring(range), list.size());
          }
      }

      bool ObStatisticsCollector::can_work_start() const
      {
          ObChunkServerMain::get_instance()->get_chunk_server().reload_config();
          int64_t num = ObChunkServerMain::get_instance()->get_chunk_server().get_config().gather_thread_num;
          YYSYS_LOG(INFO, "activeNo[%ld],allowed number[%ld]", active_thread_num_, num);
          return 0 == active_thread_num_ || (gather_list_.get_array_index() > 0 && !is_start_gather_);
      }


   }
}



























