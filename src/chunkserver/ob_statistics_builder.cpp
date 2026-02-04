
#include "ob_statistics_builder.h"
#include "ob_tablet_manager.h"
#include "ob_chunk_statistics_collector.h"
#include "ob_tablet.h"
#include "ob_chunk_server_main.h"

#include "common/ob_gather_table_info.h"
#include "common/ob_object.h"
#include "common/ob_mutator.h"


namespace oceanbase
{
   namespace chunkserver
   {
      using namespace yyutil;
      using namespace common;
      /*------------------------------------------------------------
      * ObIndexBuilder
      *-------------------------------------------------------------*/
      ObStatisticsBuilder::ObStatisticsBuilder(ObStatisticsCollector *worker, ObTabletManager *tablet_manager)
       :worker_(worker),tablet_manager_(tablet_manager),mod_(ObModIds::OB_STATISTICS_BUILD),allocator_(ModuleArena::DEFAULT_PAGE_SIZE,mod_)
       ,offset_(0),row_count_(0),column_type_(0)
      {
          top_value_list_.clear();
      }

      int ObStatisticsBuilder::init()
      {
          int ret = OB_SUCCESS;
          top_value_list_.init(10, top_value_);
          offset_ = 0;
          row_count_ = 0;
          different_num_ = 0;

          sort_.reset();
          min_value_.set_null();
          max_value_.set_null();
          if(NULL == worker_)
          {
              YYSYS_LOG(ERROR, "null pointer of worker!");
              ret = OB_ERROR;
          }

          allocator_.free();


          for(int i=0;i<10;i++)
          {
              top_value_[i].free();
          }

          return ret;
      }

      int ObStatisticsBuilder::update_empty_tablet_statistics_meta(uint64_t table_id, uint64_t column_id)
      {
          int ret = OB_SUCCESS;
          std::string start_key;
          int64_t version = THE_CHUNK_SERVER.get_tablet_manager().get_serving_data_version();

          start_key = "MIN";
          size_t start_key_len_1 = start_key.length();
          char start_key_result[start_key_len_1+1];
          strcpy(start_key_result, start_key.c_str());
          int32_t start_key_len_2 = static_cast<int32_t>(strlen(start_key_result));
          ObString start_key_value(start_key_len_2, start_key_len_2, start_key_result);

          ObRowkey rowkey;
          ObObj value[4];
          value[0].set_int(table_id);
          value[1].set_int(column_id);
          value[2].set_varchar(start_key_value);
          value[3].set_int(version);
          rowkey.assign(value, 4);

          ObObj type;
          type.set_int32(ObNullType);
          ObObj row_count;
          row_count.set_int(0);
          ObObj different_num;
          different_num.set_int(0);
          ObObj size;
          size.set_int(0);
          ObObj cardinarity;
          cardinarity.set_null();
          ObObj min_join_max;
          min_join_max.set_null();

          ObMutator* mutator = NULL;
          if(OB_SUCCESS == ret)
          {
              mutator = GET_TSI_MULT(ObMutator, TSI_COMMON_MUTATOR_1);
              if(NULL == mutator)
              {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  YYSYS_LOG(WARN, "get thread specific ObMutator fail");
              }
          }

          if(OB_SUCCESS == ret)
          {
              ret = mutator->reset();
              if(OB_SUCCESS != ret)
              {
                  YYSYS_LOG(WARN, "reset ob mutator fail:ret[%d]", ret);
              }
          }
          //YYSYS_LOG(DEBUG)



          if(OB_SUCCESS != (ret = mutator->insert(OB_ALL_STATISTIC_INFO_TID, rowkey, OB_APP_MIN_COLUMN_ID+4, type)))
          {
              YYSYS_LOG(WARN, "failed to add trun op, err=%d", ret);
          }
          else if(OB_SUCCESS != (ret = mutator->insert(OB_ALL_STATISTIC_INFO_TID, rowkey, OB_APP_MIN_COLUMN_ID+5, row_count)))
          {
              YYSYS_LOG(WARN, "failed to add trun op, err=%d", ret);
          }
          else if(OB_SUCCESS != (ret = mutator->insert(OB_ALL_STATISTIC_INFO_TID, rowkey, OB_APP_MIN_COLUMN_ID+6, different_num)))
          {
              YYSYS_LOG(WARN, "failed to add trun op, err=%d", ret);
          }
          else if(OB_SUCCESS != (ret = mutator->insert(OB_ALL_STATISTIC_INFO_TID, rowkey, OB_APP_MIN_COLUMN_ID+7, size)))
          {
              YYSYS_LOG(WARN, "failed to add trun op, err=%d", ret);
          }
          else if(OB_SUCCESS != (ret = mutator->insert(OB_ALL_STATISTIC_INFO_TID, rowkey, OB_APP_MIN_COLUMN_ID+8, cardinarity)))
          {
              YYSYS_LOG(WARN, "failed to add trun op, err=%d", ret);
          }
          else if(OB_SUCCESS != (ret = mutator->insert(OB_ALL_STATISTIC_INFO_TID, rowkey, OB_APP_MIN_COLUMN_ID+9, min_join_max)))
          {
              YYSYS_LOG(WARN, "failed to add trun op, err=%d", ret);
          }
          else
          {
              //send mutator to UPS
              ObScanner scanner;
              rpc_proxy_ = THE_CHUNK_SERVER.get_rpc_proxy();
              if(NULL == rpc_proxy_)
              {
                  YYSYS_LOG(WARN, "get rpc proxy from chunkserver failed");
                  ret = OB_ERROR;
              }
              else if(OB_SUCCESS != (ret = rpc_proxy_->ups_mutate(*mutator, false, scanner)))
              {
                  YYSYS_LOG(WARN, "failed to send mutator to ups, err=%d", ret);
              }
              scanner.clear();
          }
          if((ret == OB_SUCCESS) && OB_SUCCESS != (ret = mutator->reset()))
          {
              YYSYS_LOG(WARN, "reset ob mutator fail:ret[%d]", ret);
          }
          mutator->clear();
          allocator_.free();
          return ret;
      }

      int ObStatisticsBuilder::update_tablet_statistics_meta(ObTablet *tablet, uint64_t column_id)
      {
          int ret = OB_SUCCESS;
          std::string min;
          std::string max;
          std::string start_key;
          char buf[10240];
          ObNewRange range = tablet->get_range();
          int64_t version = tablet->get_data_version();

          ObRowkey rowkey;
          ObObj value[4];
          value[0].set_int(range.table_id_);
          value[1].set_int(column_id);

          value[3].set_int(version);
          rowkey.assign(value, 4);
          ObObj different_num;
          ObObj size;


          if(range.start_key_.is_min_row())
          {
              start_key = "MIN";
              strcpy(buf, start_key.c_str());
              int32_t start_key_len_2 = static_cast<int32_t>(strlen(buf));
              ObString start_key_value(start_key_len_2,start_key_len_2,buf);
              value[2].set_varchar(start_key_value);
          }
          else
          {
              range.start_key_.to_string(buf,10240);
              int32_t tmp_len = static_cast<int32_t>(strlen(buf));
              ObString start_key_value_v1(tmp_len,tmp_len,buf);
              value[2].set_varchar(start_key_value_v1);
          }





          different_num.set_int(different_num_);
          size.set_int(size_);
          //DEBUG

          ObObj type;
          type.set_int32(column_type_);

          ObObj row_count;
          row_count.set_int(row_count_);

          std::string info;
          std::string min_max;

          char splitchar = 31;

          if(min_value_.is_null())
          {
              min = "NULL";
          }
          else
          {
              min_value_.to_string_v2(min);
          }
          if(max_value_.is_null())
          {
              max = "NULL";
          }
          else
          {
              max_value_.to_string_v2(max);
          }
          for(int64_t i = 0; i < offset_; i++)
          {
              std::string tmp_col;
              std::string tmp_num;
              ObObj num;
              num.set_int(top_value_[i].num_);
              if(top_value_[i].obj_.is_null())
              {
                  tmp_col = "NULL";
              }
              else
              {
                  top_value_[i].obj_.to_string_v2(tmp_col);
              }
              num.to_string_v2(tmp_num);
              info = info + tmp_col + splitchar;
              if(i == offset_ - 1)
              {
                  info = info +tmp_num;
              }
              else
              {
                  info = info + tmp_num + splitchar;
              }
          }
          min_max = min + splitchar + max;
          size_t len_1 = info.length();
          char result[len_1+1];
          strcpy(result,info.c_str());
          int32_t len_2 = static_cast<int32_t>(strlen(result));
          ObString var(len_2, len_2, result);
          ObObj cardinarity;
          cardinarity.set_varchar(var);

          size_t len_3 = min_max.length();
          char result_1[len_3];
          strcpy(result_1, min_max.c_str());
          int32_t len_4 = static_cast<int32_t>(strlen(result_1));
          ObString var_1(len_4, len_4, result_1);
          ObObj min_join_max;
          min_join_max.set_varchar(var_1);

          ObMutator* mutator = NULL;
          if(OB_SUCCESS == ret)
          {
              mutator = GET_TSI_MULT(ObMutator, TSI_COMMON_MUTATOR_1);
              if(NULL == mutator)
              {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  YYSYS_LOG(WARN, "get thread specific ObMutator fail");
              }
          }

          if(OB_SUCCESS == ret)
          {
              ret = mutator->reset();
              if(OB_SUCCESS != ret)
              {
                  YYSYS_LOG(WARN, "reset ob mutator fail:ret[%d]", ret);
              }
          }
          //DEBUG



          if(OB_SUCCESS != (ret = mutator->insert(OB_ALL_STATISTIC_INFO_TID, rowkey, OB_APP_MIN_COLUMN_ID+4, type)))
          {
              YYSYS_LOG(WARN, "failed to add trun op, err=%d", ret);
          }
          else if(OB_SUCCESS != (ret = mutator->insert(OB_ALL_STATISTIC_INFO_TID, rowkey, OB_APP_MIN_COLUMN_ID+5, row_count)))
          {
              YYSYS_LOG(WARN, "failed to add trun op, err=%d", ret);
          }
          else if(OB_SUCCESS != (ret = mutator->insert(OB_ALL_STATISTIC_INFO_TID, rowkey, OB_APP_MIN_COLUMN_ID+6, different_num)))
          {
              YYSYS_LOG(WARN, "failed to add trun op, err=%d", ret);
          }
          else if(OB_SUCCESS != (ret = mutator->insert(OB_ALL_STATISTIC_INFO_TID, rowkey, OB_APP_MIN_COLUMN_ID+7, size)))
          {
              YYSYS_LOG(WARN, "failed to add trun op, err=%d", ret);
          }
          else if(OB_SUCCESS != (ret = mutator->insert(OB_ALL_STATISTIC_INFO_TID, rowkey, OB_APP_MIN_COLUMN_ID+8, cardinarity)))
          {
              YYSYS_LOG(WARN, "failed to add trun op, err=%d", ret);
          }
          else if(OB_SUCCESS != (ret = mutator->insert(OB_ALL_STATISTIC_INFO_TID, rowkey, OB_APP_MIN_COLUMN_ID+9, min_join_max)))
          {
              YYSYS_LOG(WARN, "failed to add trun op, err=%d", ret);
          }
          else
          {
              //send mutator to UPS
              ObScanner scanner;
              rpc_proxy_ = THE_CHUNK_SERVER.get_rpc_proxy();
              if(NULL == rpc_proxy_)
              {
                  YYSYS_LOG(WARN, "get rpc proxy from chunkserver failed");
                  ret = OB_ERROR;
              }
              else if(OB_SUCCESS != (ret = rpc_proxy_->ups_mutate(*mutator, false, scanner)))
              {
                  YYSYS_LOG(WARN, "failed to send mutator to ups, err=%d", ret);
              }
              scanner.clear();
          }
          if((ret == OB_SUCCESS) && OB_SUCCESS != (ret = mutator->reset()))
          {
              YYSYS_LOG(WARN, "reset ob mutator fail:ret[%d]", ret);
          }
          mutator->clear();
          allocator_.free();
          return ret;
      }

      int ObStatisticsBuilder::start(ObTablet *tablet, ObGatherTableInfo gather_info)
      {
          int ret = OB_SUCCESS;
          tablet->dump();
          tablet->dump(true);
          size_ = tablet->get_extend_info().occupy_size_;
          //DEBUG
          gather_info.dump();
          YYSYS_LOG(INFO, "Gathering!");
          int32_t disk_no = -1;
          if(NULL == tablet)
          {
              YYSYS_LOG(ERROR, "tablet pointer is null !");
          }
          else if (gather_info.columns_list_.get_array_index() <= 0)
          {
              YYSYS_LOG(ERROR, "No Column Need to be Gathered !");
              ret = OB_INVALID_ARGUMENT;
          }
          else
          {
              disk_no = tablet->get_disk_no();
              for(int i = 0; i < gather_info.columns_list_.get_array_index(); i++)
              {

                  if(tablet->get_row_count() > 0)
                  {

                      if(OB_SUCCESS != (ret = gather_partition_statistics_v2(tablet,gather_info.columns_[i],disk_no)))
                      {
                          YYSYS_LOG(ERROR, "Failed to Gather Partition Statistics, ret = %d", ret);
                      }
                  }
                  else if(OB_SUCCESS != (ret = update_empty_tablet_statistics_meta(gather_info.table_id_, gather_info.columns_[i])))
                  {
                      YYSYS_LOG(ERROR, "Failed to Send Partition Statistics, ret = %d", ret);
                  }















                  init();
              }

              if(OB_SUCCESS != (ret = tablet_manager_->get_serving_tablet_image().release_tablet(tablet)))
              {

                  YYSYS_LOG(WARN, "Failed to release tablet, ret = %d", ret);

                  tablet->dump(true);
              }


          }
          return ret;
      }

      int ObStatisticsBuilder::gather_partition_statistics(ObNewRange range, uint64_t column_id, int32_t disk)
      {
          int ret = OB_SUCCESS;
          init();
          UNUSED(disk);
          YYSYS_LOG(INFO,"TO DO sort_");
          ObTabletScan tmp_table_scan;
          ObSSTableScan ob_sstable_scan;
          ObSqlScanParam ob_sql_scan_param;
          sstable::ObSSTableScanParam sstable_scan_param;
          ObArray<uint64_t> basic_columns;
          ObTabletManager *tablet_mgr = worker_->get_tablet_manager();
          ScanContext sc;
          tablet_mgr->build_scan_context(sc);
          const ObRow *cur_row = NULL;
          sort_.set_child(0, ob_sstable_scan);
          common::ObObj single_value;
          common::ObObj tmp_value;
          const common::ObObj *got_value = NULL;
          common::ObTopValue last_value;
          common::ObObj pre_value;
          int64_t num = 0;
          pre_value.set_null();
          tmp_value.set_null();
          if(OB_SUCCESS != (ret = ob_sql_scan_param.set_range(range)))
          {
              YYSYS_LOG(WARN,"set ob_sql_param_range failed,ret= %d,range = %s", ret, to_cstring(range));
          }
          else
          {
              ob_sql_scan_param.set_is_result_cached(false);
              basic_columns.clear();
              basic_columns.push_back(column_id);
              if(OB_SUCCESS != (ret = sort_.add_sort_column(range.table_id_, column_id, true)))
              {
                  YYSYS_LOG(WARN, "Failed to add sort column!");
              }
              else if(OB_SUCCESS != (ret = tmp_table_scan.build_sstable_scan_param_pub(basic_columns,ob_sql_scan_param,sstable_scan_param)))
              {
                  YYSYS_LOG(WARN, "Failed to build sstable scan param!");
              }
              else if(OB_SUCCESS != (ret = ob_sstable_scan.open_scan_context(sstable_scan_param,sc)))
              {
                  YYSYS_LOG(WARN, "failed to open scan context");
              }
              else if(OB_SUCCESS != (ret = sort_.open()))
              {
                  YYSYS_LOG(WARN, "failed to open ObSsTableScan! ,ret = %d", ret);
              }
              else
              {
                  if(0 == sort_.get_sort_column_size())
                  {
                      //
                  }
                  //
                  offset_ = 0;
                  if(OB_SUCCESS == (ret = sort_.get_next_row(cur_row)))
                  {
                      if(OB_SUCCESS != (ret = cur_row->get_cell(range.table_id_, column_id, got_value)))
                      {
                          YYSYS_LOG(ERROR, "Failed to get min value!");
                      }
                      else
                      {
                          column_type_ = got_value->get_type();
                          got_value->obj_copy(tmp_value);
                          if(OB_SUCCESS != (ret = ob_write_obj_v2(allocator_, tmp_value, min_value_)))
                          {
                              YYSYS_LOG(ERROR, "write obj failed, ret [%d]", ret);
                          }
                          else
                          {
                              num++;
                              row_count_++;
                          }
                      }
                  }
                  while(OB_SUCCESS == ret && OB_SUCCESS == (ret = sort_.get_next_row(cur_row)))
                  {

                      common::ObTopValue value;
                      if(NULL != cur_row)
                      {
                          row_count_++;
                          if(OB_SUCCESS != (ret = cur_row->get_cell(range.table_id_, column_id, got_value)))
                          {
                              YYSYS_LOG(WARN, "failed to get value from cell! range.table_id_ = %ld column_id = %ld ret = %d", range.table_id_, column_id, ret);
                              continue;
                          }


                          if(row_count_ == 2)
                          {
                              min_value_.obj_copy(tmp_value);
                              if(OB_SUCCESS != (ret = ob_write_obj_v2(allocator_, tmp_value, single_value)))
                              {
                                  YYSYS_LOG(ERROR, "write obj failed, ret [%d]", ret);
                                  break;
                              }
                          }
                          if(got_value->compare(single_value) == 0)
                          {
                              num++;
                          }
                          else
                          {
                              if(OB_SUCCESS != (ret = ob_write_obj_v2(allocator_, single_value, value.obj_)))
                              {
                                  YYSYS_LOG(ERROR,"write obj failed, ret [%d]", ret);
                                  break;
                              }
                              else if(OB_SUCCESS != (ret = ob_write_obj_v2(allocator_, single_value, pre_value)))
                              {
                                  YYSYS_LOG(ERROR,"write obj failed, ret [%d]", ret);
                                  break;
                              }
                              else
                              {
                                  value.num_ = num;

                                  insert_top_value(value);
                                  different_num_++;
                                  got_value->obj_copy(tmp_value);
                                  if(OB_SUCCESS != (ret = ob_write_obj_v2(allocator_, tmp_value, single_value)))
                                  {
                                      YYSYS_LOG(ERROR,"write obj failed, ret [%d]", ret);
                                      break;
                                  }
                                  num = 1;
                              }
                          }
                      }
                  }
                  if(OB_ITER_END == ret)
                  {
                      if(OB_SUCCESS != (ret = ob_write_obj_v2(allocator_, tmp_value, last_value.obj_)))
                      {
                          YYSYS_LOG(ERROR,"write obj failed, ret [%d]", ret);
                      }
                      else
                      {
                          last_value.num_ = num;
                          //
                          insert_top_value(last_value);
                          different_num_++;
                          num = 0;
                      }
                      if(offset_ > 1 && last_value.obj_.is_null())
                      {
                          if(OB_SUCCESS != (ret = ob_write_obj_v2(allocator_, pre_value, max_value_)))
                          {
                              YYSYS_LOG(ERROR,"failed to write max_value! ret [%d]", ret);
                          }
                      }
                      else
                      {
                          if(OB_SUCCESS != (ret = ob_write_obj_v2(allocator_, tmp_value, max_value_)))
                          {
                              YYSYS_LOG(ERROR,"failed to write max_value! ret [%d]", ret);
                          }
                      }
                      ret = OB_SUCCESS;
                  }
              }
              sort_.reset();
              sort_.close();
              tmp_table_scan.close();
              ob_sstable_scan.reset_scanner();
              ob_sstable_scan.reset();
              ob_sstable_scan.close();
              ob_sql_scan_param.reset();
              sstable_scan_param.reset();
              YYSYS_LOG(INFO, "size is %ld , num_ is %ld", offset_, top_value_[0].num_);
              for(int64_t q = 0; q < offset_; q++)
              {
                  //
                  top_value_[q].obj_.dump(ERROR);
              }
          }
          return ret;
      }


      int ObStatisticsBuilder::gather_partition_statistics_v2(ObTablet *tablet, uint64_t column_id, int32_t disk)
      {
          int ret = OB_SUCCESS;
          init();
          UNUSED(disk);
          YYSYS_LOG(INFO,"TO DO sort_");
          ObNewRange range = tablet->get_range();
          ObTabletScan tmp_table_scan;
          ObSSTableScan ob_sstable_scan;
          ObSqlScanParam ob_sql_scan_param;
          sstable::ObSSTableScanParam sstable_scan_param;
          ObArray<uint64_t> basic_columns;
          ObTabletManager *tablet_mgr = worker_->get_tablet_manager();
          ScanContext sc;
          tablet_mgr->build_scan_context(sc);
          const ObRow *cur_row = NULL;
          sort_.set_child(0, ob_sstable_scan);
          common::ObObj single_value;
          common::ObObj tmp_value;
          const common::ObObj *got_value = NULL;
          common::ObTopValue last_value;
          common::ObTopValue value;
          common::ObObj pre_value;
          int64_t num = 0;
          pre_value.set_null();
          tmp_value.set_null();
          if(OB_SUCCESS != (ret = ob_sql_scan_param.set_range(range)))
          {
              YYSYS_LOG(WARN,"set ob_sql_param_range failed,ret= %d,range = %s", ret, to_cstring(range));
          }
          else
          {
              ob_sql_scan_param.set_is_result_cached(false);
              basic_columns.clear();
              basic_columns.push_back(column_id);
              if(OB_SUCCESS != (ret = sort_.add_sort_column(range.table_id_, column_id, true)))
              {
                  YYSYS_LOG(WARN, "Failed to add sort column!");
              }
              else if(OB_SUCCESS != (ret = tmp_table_scan.build_sstable_scan_param_pub(basic_columns,ob_sql_scan_param,sstable_scan_param)))
              {
                  YYSYS_LOG(WARN, "Failed to build sstable scan param!");
              }
              else if(OB_SUCCESS != (ret = ob_sstable_scan.open_scan_context(sstable_scan_param,sc)))
              {
                  YYSYS_LOG(WARN, "failed to open scan context");
              }
              else if(OB_SUCCESS != (ret = sort_.open()))
              {
                  YYSYS_LOG(WARN, "failed to open ObSsTableScan! ,ret = %d", ret);
              }
              //start gather information
              else
              {
                  if(0 == sort_.get_sort_column_size())
                  {
                      //
                  }
                  //
                  offset_ = 0;

                  //first line
                  if(OB_SUCCESS == (ret = sort_.get_next_row(cur_row)))
                  {
                      if(OB_SUCCESS != (ret = cur_row->get_cell(range.table_id_, column_id, got_value)))
                      {
                          YYSYS_LOG(ERROR, "Failed to get min value!");
                      }
                      else
                      {
                          //
                          column_type_ = got_value->get_type();

                          if(OB_SUCCESS != (ret = got_value->obj_copy(min_value_)))
                          {
                              YYSYS_LOG(ERROR, "write obj failed, ret [%d]", ret);
                          }
                          else if(OB_SUCCESS != (ret = got_value->obj_copy(single_value)))
                          {
                              YYSYS_LOG(ERROR, "write obj failed, ret [%d]", ret);
                          }
                          else
                          {
                              //
                              num++;
                              row_count_++;
                          }
                      }
                  }

                  //second line
                  while(OB_SUCCESS == ret && OB_SUCCESS == (ret = sort_.get_next_row(cur_row)))
                  {
                      if(NULL != cur_row)
                      {
                          row_count_++;
                          if(OB_SUCCESS != (ret = cur_row->get_cell(range.table_id_, column_id, got_value)))
                          {
                              YYSYS_LOG(WARN, "failed to get value from cell! range.table_id_ = %ld column_id = %ld  ret = %d", range.table_id_, column_id, ret);
                              continue;
                          }



                          if(got_value->compare(single_value) == 0)
                          {
                              num++;
                          }
                          else
                          {
                              if(OB_SUCCESS != (ret = single_value.obj_copy(value.obj_)))
                              {
                                  YYSYS_LOG(ERROR,"write obj failed, ret [%d]", ret);
                                  break;
                              }
                              else if(OB_SUCCESS != (ret = single_value.obj_copy(pre_value)))
                              {
                                  YYSYS_LOG(ERROR,"write obj failed, ret [%d]", ret);
                                  break;
                              }
                              else
                              {
                                  value.num_ = num;
                                  insert_top_value_v2(value);
                                  //
                                  different_num_++;
                                  if(OB_SUCCESS != (ret = got_value->obj_copy(single_value)))
                                  {
                                      YYSYS_LOG(ERROR,"write obj failed, ret [%d]", ret);
                                      break;
                                  }
                                  num = 1;
                                  //

                              }
                          }
                      }
                  }

                  //last line
                  if(OB_ITER_END == ret)
                  {
                      if(OB_SUCCESS != (ret = single_value.obj_copy(last_value.obj_)))
                      {
                          YYSYS_LOG(ERROR,"write obj failed, ret [%d]", ret);
                      }
                      else
                      {
                          last_value.num_ = num;
                          //
                          insert_top_value_v2(last_value);
                          //
                          different_num_++;
                          num = 0;
                      }
                      if(offset_ > 1 && last_value.obj_.is_null())
                      {
                          if(OB_SUCCESS != (ret = pre_value.obj_copy(max_value_)))
                          {
                              YYSYS_LOG(ERROR,"failed to write max_value! ret [%d]", ret);
                          }
                      }
                      else
                      {
                          if(OB_SUCCESS != (ret = last_value.obj_.obj_copy(max_value_)))
                          {
                              YYSYS_LOG(ERROR,"failed to write max_value! ret [%d]", ret);
                          }
                      }
                      ret = OB_SUCCESS;
                  }
              }

              //send to UPS
              if(OB_SUCCESS != ret)
              {
                  YYSYS_LOG(ERROR, "Failed to Gather Statistics, ret = %d",ret);
              }
              else if(OB_SUCCESS != (ret = update_tablet_statistics_meta(tablet, column_id)))
              {
                  YYSYS_LOG(ERROR, "Failed to Send Partition Statistics, ret = %d", ret);
              }

          }

          //
          for(int64_t q = 0; q < offset_; q++)
          {
              //
              //
          }

          sort_.reset();
          sort_.close();
          tmp_table_scan.close();
          ob_sstable_scan.reset_scanner();
          ob_sstable_scan.reset();
          ob_sstable_scan.close();
          ob_sql_scan_param.reset();
          sstable_scan_param.reset();
          return ret;
      }




      void ObStatisticsBuilder::insert_top_value(const ObTopValue &value)
      {
          //

          if(offset_ == 0)
          {

              top_value_[0].deep_copy(allocator_, value);
              offset_ = 1;
          }
          else if(offset_ == 1)
          {
              if(value.num_ >= top_value_[0].num_)
              {
                  top_value_[1].deep_copy(allocator_, value);
                  offset_ = 2;
              }
              else
              {
                  top_value_[1].deep_copy(allocator_, top_value_[0]);
                  top_value_[0].deep_copy(allocator_, value);
                  offset_ = 2;
              }
          }
          else
          {
              if(offset_ < 10)
              {
                  if(value.num_ > top_value_[offset_ - 1].num_)
                  {
                     top_value_[offset_].deep_copy(allocator_, value);
                     offset_++;
                  }
                  else if(value.num_ < top_value_[0].num_)
                  {
                      for(int64_t t = offset_ - 1; t >= 0; t--)
                      {
                          top_value_[t+1].deep_copy(allocator_, top_value_[t]);
                      }
                      top_value_[0].deep_copy(allocator_, value);
                      offset_++;
                  }
                  else
                  {
                      for(int64_t i = 0; i < offset_; i++)
                      {
                          if(value.num_ >= top_value_[i].num_ && value.num_ <= top_value_[i+1].num_)
                          {
                              for(int64_t j = offset_ - 1; j > i; j--)
                              {
                                  top_value_[j+1].deep_copy(allocator_, top_value_[j]);
                              }
                              top_value_[i+1].deep_copy(allocator_, value);
                              offset_++;
                              break;
                          }
                      }
                  }
              }
              else if(offset_ == 10)
              {
                  if(value.num_ >= top_value_[9].num_)
                  {
                      for(int64_t k = 0; k < 9; k++)
                      {
                          top_value_[k].deep_copy(allocator_, top_value_[k+1]);
                      }
                      top_value_[9].deep_copy(allocator_, value);
                  }
                  else if(value.num_ <= top_value_[0].num_)
                  {

                  }
                  else
                  {
                      for(int64_t n = 0; n < offset_ - 1; n++)
                      {
                          if(value.num_ > top_value_[n].num_ && value.num_ <= top_value_[n+1].num_)
                          {
                              for(int64_t m = 0; m < n; m++)
                              {
                                  top_value_[m].deep_copy(allocator_, top_value_[m+1]);
                              }
                              top_value_[n].deep_copy(allocator_, value);
                              break;
                          }
                      }
                  }
              }
          }
      }


      void ObStatisticsBuilder::insert_top_value_v2(const ObTopValue &value)
      {
          if(offset_ == 0)
          {
              top_value_[0].copy(value);
              offset_ = 1;
          }
          else if(offset_ == 1)
          {
              if(value.num_ >= top_value_[0].num_)
              {
                  top_value_[1].copy(value);
                  offset_ = 2;
              }
              else
              {
                  top_value_[1].copy(top_value_[0]);
                  top_value_[0].copy(value);
                  offset_ = 2;
              }
          }
          else
          {
              if(offset_ < 10)
              {
                  if(value.num_ > top_value_[offset_ - 1].num_)
                  {
                      top_value_[offset_].copy(value);
                      offset_++;
                  }
                  else if(value.num_ < top_value_[0].num_)
                  {
                      for(int64_t t = offset_-1; t >= 0; t--)
                      {
                          top_value_[t+1].copy(top_value_[t]);
                      }
                      top_value_[0].copy(value);
                      offset_++;
                  }
                  else
                  {
                      for(int64_t i = 0; i < offset_; i++)
                      {
                        if(value.num_ >= top_value_[i].num_ && value.num_ <= top_value_[i+1].num_)
                        {
                            for(int64_t j = offset_-1; j > i; j--)
                            {
                                top_value_[j+1].copy(top_value_[j]);
                            }
                            top_value_[i+1].copy(value);
                            offset_++;
                            break;
                        }
                      }
                  }
              }
              else if(offset_ == 10)
              {
                  if(value.num_ >= top_value_[9].num_)
                  {
                      for(int64_t k = 0; k < 9 ; k++)
                      {
                          top_value_[k].copy(top_value_[k+1]);
                      }
                      top_value_[9].copy(value);
                  }
                  else if(value.num_ <= top_value_[0].num_)
                  {

                  }
                  else
                  {
                      for(int64_t n = 0; n < offset_-1; n++)
                      {
                          if(value.num_ > top_value_[n].num_ && value.num_ <= top_value_[n+1].num_)
                          {
                              for(int64_t m = 0; m < n; m++)
                              {
                                  top_value_[m].copy(top_value_[m+1]);
                              }
                              top_value_[n].copy(value);
                              break;
                          }
                      }
                  }
              }
          }
      }





   }
}
