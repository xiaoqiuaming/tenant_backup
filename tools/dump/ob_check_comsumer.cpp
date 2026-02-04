#include "ob_check_comsumer.h"
#include "slice.h"

#include <string>
#include "common/ob_tsi_factory.h"
#include "ob_check.h"
using namespace std;
using namespace oceanbase;
using namespace common;

ImportComsumer::ImportComsumer(oceanbase::api::OceanbaseDb *db, ObRowBuilder *builder, AppendableFile *bad_file, AppendableFile *correct_file, const TableParam &param) : param_(param)
{
  db_ = db;
  builder_ = builder;
  assert(db_ != NULL);
  assert(builder_ != NULL);
  bad_file_ = bad_file;
  line_buffer_ = NULL;
  correct_file_ = correct_file;
  correct_line_buffer_ = NULL;
  this->db_->set_max_ups_mem(this->param_.import_limit_ups_memory);
}

int ImportComsumer::init()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == ret) {
    line_buffer_ = (char *)malloc(sizeof(char) * 1024 * 1024 * 2);  // 2M
    if (NULL == line_buffer_) {
      error_arr[ALLOCATE_MEMORY_ERROR] = false;
      ret = OB_ALLOCATE_MEMORY_FAILED;
      YYSYS_LOG(ERROR, "fail to allocate line buffer");
    }
  }

  if (OB_SUCCESS == ret)
  {
    correct_line_buffer_ = (char *)malloc(sizeof(char) * 1024 * 1024 * 2);  // 2M
    if (NULL == correct_line_buffer_)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      YYSYS_LOG(ERROR, "fail to allocate memory!");
    }
  }

  if (OB_SUCCESS != (ret = ms_manager_.init(db_)))
  {
    error_arr[MS_MANAGER_INIT_ERROR] = false;
    YYSYS_LOG(ERROR, "MergeServerManager init failed!");
  }
  return ret;
}

ImportComsumer::~ImportComsumer()
{
  if (line_buffer_ != NULL) {
    free(line_buffer_);
  }
}

int ImportComsumer::write_bad_record(RecordBlock &rec)
{
  CThreadGuard guard(&bad_lock_);
  Slice slice;
  size_t rec_delima_len = param_.rec_delima.length();
  size_t delima_len = param_.delima.length();
  char delima_buf[4];
  int ret = OB_SUCCESS;

  if (bad_file_ == NULL) {
    return 0;
  }

  param_.rec_delima.append_delima(delima_buf, 0, 4);
  rec.reset();
  while (rec.next_record(slice)) {
    if (delima_len > slice.size()) {
      ret = OB_ERROR;
      YYSYS_LOG(WARN, "empty or currupted row meet, skiping");
      continue;
    }

    memcpy(line_buffer_, slice.data(), slice.size() - delima_len);
    memcpy(line_buffer_ + slice.size() - delima_len, delima_buf, rec_delima_len);
    if(bad_file_->AppendNoLock(line_buffer_, slice.size() - delima_len + rec_delima_len) != OB_SUCCESS) {
      ret = OB_ERROR;
      error_arr[BAD_FILE_ERROR] = false;
      YYSYS_LOG(ERROR, "can't write to bad_file name = %s", param_.bad_file_);
      break;
    }

    //builder_->add_bad_lineno();
     //add e
  }

  return ret;
}

int ImportComsumer::write_correct_record(RecordBlock &rec)
{
  CThreadGuard guard(&correct_lock_);
  Slice slice;
  size_t rec_delima_len = param_.rec_delima.length();
  size_t delima_len = param_.delima.length();
  char delima_buf[4];
  int ret = OB_SUCCESS;

  if (correct_file_ == NULL) {
    return ret;
  }

  param_.rec_delima.append_delima(delima_buf, 0, 4);
  rec.reset();
  while (rec.next_record(slice)) {
    if (delima_len > slice.size()) {
      ret = OB_ERROR;
      YYSYS_LOG(WARN, "empty or currupted row meet, skiping");
      continue;
    }

    memcpy(correct_line_buffer_, slice.data(), slice.size() - delima_len);
    memcpy(correct_line_buffer_ + slice.size() - delima_len, delima_buf, rec_delima_len);
    if(correct_file_->AppendNoLock(correct_line_buffer_, slice.size() - delima_len + rec_delima_len) != OB_SUCCESS) {
      ret = OB_ERROR;
      //error_arr[BAD_FILE_ERROR] = false;
      YYSYS_LOG(ERROR, "can't write to correct_file name = %s", param_.correctfile_);
      break;
    }
  }
  return ret;
}

int ImportComsumer::comsume(RecordBlock &obj)
{
    int ret = OB_SUCCESS;

    MaxLenSQLBuffer *struct_buffer = GET_TSI_MULT(MaxLenSQLBuffer, 0);
    char *sql_buffer = NULL;
    if (NULL != struct_buffer)
    {
      sql_buffer = struct_buffer->buf;
    }
    ObString sql;
    if (NULL == sql_buffer)
    {
      ret = OB_MEM_OVERFLOW;
      error_arr[MEMORY_OVERFLOW] = false;
      YYSYS_LOG(ERROR, "allocate memory failed!");
    }
    else
    {
      sql.assign_buffer(sql_buffer, MAX_CONSTRUCT_SQL_LENGTH);
    }

    if (ret == OB_SUCCESS)
    {
      ret = builder_->build_sql(obj, sql);
      if (ret != OB_SUCCESS)
      {
        error_arr[SQL_BUILDING_ERROR] = false;
        YYSYS_LOG(ERROR, "ObRowBuilder can't build sql");
      }
    }

    if (ret != OB_SUCCESS)
    {
      int err = write_bad_record(obj);
      builder_->add_bad_lineno((int)obj.get_rec_num());
      if (err != OB_SUCCESS)
      {
        error_arr[BAD_FILE_ERROR] = false;
        YYSYS_LOG(ERROR, "can't write bad record to file");
      }
    }
    else
    {
      int err = write_correct_record(obj);
      if (err != OB_SUCCESS)
      {
        error_arr[SYSTEM_ERROR] = false;
        YYSYS_LOG(ERROR, "can't write correct record to correct file");
      }
    }

    return ret;
}

int ImportComsumer::get_lineno() const
{
  return builder_->get_lineno();
}

bool ImportComsumer::if_print_progress() const
{
  return param_.progress;
}

int ImportComsumer::get_bad_lineno() const
{
  return static_cast<int>(builder_->get_bad_lineno());
}

MergeServerRef::MergeServerRef(const ObServer &server)
{
  ref_count_ = 0;
  server_ = server;
}

MergeServerRef::MergeServerRef()
{
  ref_count_ = 0;
}

MergeServerRef::~MergeServerRef()
{

}

int MergeServerRef::inc_ref()
{
  ref_count_++;
  return OB_SUCCESS;
}

int MergeServerRef::dec_ref()
{
  int ret = OB_SUCCESS;
  ref_count_--;
  if (ref_count_ < 0)
  {
    ret = OB_ERROR;
    error_arr[MS_REF_COUNT_ERROR] = false;
    YYSYS_LOG(ERROR, "MergeServer ref_count less than zero!");
  }
  return ret;
}

int MergeServerManager::init(OceanbaseDb *db)
{
  int ret = OB_SUCCESS;
  std::vector<ObServer> ms_array;
  db_ = db;

  if (OB_SUCCESS != (ret = db_->fetch_mysql_ms_list(100, ms_array)))
  {
    error_arr[FETCH_MS_LIST_ERROR] = false;
    YYSYS_LOG(ERROR, "fetch ms list failed! ret=[%d]", ret);
  }
  else if (ms_array.size() == 0)
  {
    ret = OB_ERROR;
    YYSYS_LOG(ERROR, "there is no mergeserver now!");
  }
  if (OB_SUCCESS == ret)
  {
    for (unsigned int i = 0; i < ms_array.size(); i++)
    {
      ms_ref_array_[i] = ms_array[i];
      sort_array_.push_back(ms_ref_array_ + i);
    }
  }
  return ret;
}

MergeServerManager::MergeServerManager()
{

}

MergeServerManager::~MergeServerManager()
{

}
