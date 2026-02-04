#include "cbrecovery.h"
#include "common/ob_define.h"
#include "common/ob_malloc.h"
#include <dirent.h>
#include "common/file_directory_utils.h"
#include "common/ob_log_dir_scanner.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>

using namespace oceanbase::common;

#define MAX_STORE_COUNT_ONE_UPS 81

void __attribute__((constructor(101))) init_log_file()
{
  YYSYS_LOGGER.setLogLevel("INFO");
  YYSYS_LOGGER.setFileName("dr_server.log", true);
  YYSYS_LOGGER.setMaxFileSize(256 * 1024L * 1024L);
}

int get_max_log_file_id(const char *log_dir, uint64_t &max_log_file_id)
{
  int err = OB_SUCCESS;
  ObLogDirScanner log_dir_scanner;
  if (NULL == log_dir)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (err = log_dir_scanner.init(log_dir)))
  {
    if (OB_DISCONTINUOUS_LOG == err)
    {
      YYSYS_LOG(WARN, "log_dir_scanner.init(%s): log file not continuous", log_dir);
    }
    else
    {
      YYSYS_LOG(ERROR, "log_dir_scanner.init(%s)=>%d", log_dir, err);
    }
  }
  else if (OB_SUCCESS != (err = log_dir_scanner.get_max_log_id(max_log_file_id)))
  {
    if (OB_ENTRY_NOT_EXIST == err)
    {
      YYSYS_LOG(WARN, "log_dir_scanner.get_max_log_id(log_dir=%s): no log file in dir", log_dir);
    }
    else
    {
      YYSYS_LOG(ERROR, "log_dir_scanner.get_max_log_id(log_dir=%s)=>%d", log_dir, err);
    }
  }
  return err;
}

int get_log_location(ObOnDiskLogLocator &log_locator, uint64_t log_id, uint64_t &log_file)
{
  int err = OB_SUCCESS;
  ObLogLocation location;
  if (OB_SUCCESS != (err = log_locator.get_location(int64_t(log_id), location)))
  {
    YYSYS_LOG(ERROR, "log_locator.get_location(%lu)=>%d", log_id, err);
  }
  else
  {
    log_file = location.file_id_;
  }
  return err;
}

int locate(const char *log_dir, uint64_t log_id, uint64_t &log_file)
{
  int err = OB_SUCCESS;
  ObOnDiskLogLocator log_locator;
  if (NULL == log_dir || 0 >= log_id)
  {
    err = OB_INVALID_ARGUMENT;
    YYSYS_LOG(ERROR, "find_log(log_dir=%s, log_id=%lu): INVALID ARGUMENT.", log_dir, log_id);
  }
  else if (OB_SUCCESS != (err = log_locator.init(log_dir)))
  {
    YYSYS_LOG(ERROR, "log_locator.init(%s)=>%d", log_dir, err);
  }
  else if (OB_SUCCESS != (err = get_log_location(log_locator, log_id, log_file)))
  {
    YYSYS_LOG(ERROR, "get_log_location(%ld)=>%d", log_id, err);
  }
  return err;
}

int get_first_log_time_stamp_func(const char *log_dir, uint64_t file_id, int64_t &mutate_time)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == ret)
  {
    ObRepeatedLogReader reader;
    ret = reader.init(log_dir);
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(ERROR, "Error occured when init, ret=%d", ret);
    }
    else if (OB_SUCCESS != (ret = reader.open(file_id)))
    {
      YYSYS_LOG(ERROR, "Error occured when opening, ret=%d", ret);
    }
    LogCommand cmd;
    uint64_t log_seq;
    char *log_data;
    int64_t data_len;
    bool get_first_time = false;
    ret = reader.read_log(cmd, log_seq, log_data, data_len);
    while (OB_SUCCESS == ret)
    {
      if (OB_LOG_SWITCH_LOG == cmd || get_first_time)
      {
        break;
      }
      else
      {
        int64_t pos = 0;
        switch(cmd)
        {
          case OB_UPS_MUTATOR_PREPARE:
          {
            ObTransID coordinator_tid;
            if (OB_SUCCESS != (ret = coordinator_tid.deserialize_for_prepare(log_data, data_len, pos)))
            {
              YYSYS_LOG(ERROR, "Error occured when deserialize coordinator tid, ret=%d", ret);
            }
            ObUpsMutator ups_mutator;
            if (OB_SUCCESS != (ret = ups_mutator.deserialize(log_data, data_len, pos)))
            {
              YYSYS_LOG(ERROR, "Error occured when deserialize ups_mutator, ret=%d", ret);
            }
            else {
              mutate_time = ups_mutator.get_mutate_timestamp();
              get_first_time = true;
            }
            break;
          }
          case OB_PART_TRANS_COMMIT:
          case OB_PART_TRANS_ROLLBACK:
          {
            PartCommitInfo commitinfo;
            if (OB_SUCCESS != (ret = commitinfo.deserialize(log_data, data_len, pos)))
            {
              YYSYS_LOG(ERROR, "Error occured when deserialize commitinfo, ret=%d", ret);
            }
            else
            {
              mutate_time = commitinfo.mutate_timestamp_;
              get_first_time = true;
            }
            break;
          }
          case OB_LOG_UPS_MUTATOR:
          {
            ObUpsMutator ups_mutator;
            if (OB_SUCCESS != (ret = ups_mutator.deserialize(log_data, data_len, pos)))
            {
              YYSYS_LOG(ERROR, "Error occured when deserialize ups_mutator, ret=%d", ret);
            }
            else if (0 != ups_mutator.get_flag())
            {
              YYSYS_LOG(INFO, "ups_mutator.get_flag()=%d, no valid mutate_timestamp", ups_mutator.get_flag());
            }
            else {
              mutate_time = ups_mutator.get_mutate_timestamp();
              get_first_time = true;
            }
            break;
          }
          default:
            break;
        }
      }
      ret = reader.read_log(cmd, log_seq, log_data, data_len);
    }
    if (!get_first_time)
    {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int locate_by_time(const char *log_dir, ObArray<string>& log_files, int64_t mutate_time, uint64_t &log_file)
{
  int ret = OB_SUCCESS;
  string log_name;
  uint64_t start = 0;
  uint64_t max_log_file = 0;
  uint64_t end = 0;
  int64_t first_time = 0;
  bool no_time_found = false;

  if (OB_SUCCESS != (ret = log_files.at(0, log_name)))
  {
    YYSYS_LOG(INFO, "Error occured when get log_name from array, ret=%d", ret);
  }
  else
  {
    sscanf(log_name.c_str(), "%lu", &start);
  }
  if (OB_SUCCESS != (ret = log_files.at(log_files.count() - 1, log_name)))
  {
    YYSYS_LOG(INFO, "Error occured when get log_name from array, ret=%d", ret);
  }
  else
  {
    sscanf(log_name.c_str(), "%lu", &max_log_file);
    end = max_log_file;
  }

  for (; start < end && OB_SUCCESS == ret;)
  {
    if (no_time_found)
    {
      log_file += 1;
    }
    else
    {
      log_file = (start + end + 1) / 2;
    }
    if (OB_SUCCESS != (ret = get_first_log_time_stamp_func(log_dir, log_file, first_time))
        && OB_ENTRY_NOT_EXIST != ret)
    {
      YYSYS_LOG(ERROR, "get_first_log_time_stamp_func(log_dir=%s, file_id=%lu)=>%d", log_dir, log_file, ret);
    }
    else if (OB_ENTRY_NOT_EXIST ==  ret)
    {
      if (max_log_file == log_file)
      {
        ret = OB_SUCCESS;
        end = log_file - 1;
        YYSYS_LOG(WARN, "last_log_file[%ld] is empty", max_log_file);
      }
      else
      {
        YYSYS_LOG(WARN, "get_first_log_id(file_id=%ld): ENTRY_NOT_EXIST", log_file);
        no_time_found = true;
        ret = OB_SUCCESS;
      }
    }
    else if (mutate_time < first_time)
    {
      end = log_file - 1;
    }
    else
    {
      start = log_file;
    }
  }
  log_file = start;
  YYSYS_LOG(INFO, "get_file_id_by_time_stamp(last_candinate=%lu)", log_file);
  if (OB_SUCCESS != ret)
  {}
  else if (OB_SUCCESS != (ret = get_first_log_time_stamp_func(log_dir, log_file, first_time))
           && OB_ENTRY_NOT_EXIST != ret)
  {
    YYSYS_LOG(ERROR, "get_first_log_time_stamp_func(log_dir=%s, file_id=%lu)=>%d", log_dir, log_file, ret);
  }
  else if (OB_ENTRY_NOT_EXIST ==  ret)
  {}
  else if (mutate_time < first_time)
  {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

void get_sys_time(char *time_str)
{
  time_t tt;
  time(&tt);
  tt = tt + 8 * 3600; //transform the time_zone
  tm *t = gmtime(&tt);
  sprintf(time_str, "%d%02d%02d%02d%02d%02d",
          t->tm_year + 1900,
          t->tm_mon + 1,
          t->tm_mday,
          t->tm_hour,
          t->tm_min,
          t->tm_sec);
}

int find_max_major_version(const char *path, uint64_t &mf_max_clog_id, uint64_t &mf_min_clog_id)
{
  int ret = OB_SUCCESS;
  uint64_t max_major_version = 0;
  DIR *d =NULL;
  struct dirent *dp = NULL;
  struct stat st;
  char raid_path[150] = {0};
  char store_path[200] = {0};
  char full_path[256] = {0};

  if (stat(path, &st) < 0 || !S_ISDIR(st.st_mode))
  {
    YYSYS_LOG(ERROR, "path:%s is not a directory", path);
    ret = OB_ERROR;
  }
  for (int i = 0; OB_SUCCESS == ret && i < 10; i++)
  {
    sprintf(raid_path, "%s/raid%d", path, i);
    if (stat(raid_path, &st) < 0 ||  !S_ISDIR(st.st_mode))
    {
      continue;
    }
    for (int j = 0; j < 10; j++)
    {
      sprintf(store_path, "%s/store%d", raid_path, j);
      if (stat(store_path, &st) < 0 ||  !S_ISDIR(st.st_mode))
      {
        continue;
      }
      if (!(d = opendir(store_path)))
      {
        ret = OB_ERROR;
        YYSYS_LOG(ERROR, "open dir:%s fail, error:%m", store_path);
      }
      while (OB_SUCCESS == ret && (dp = readdir(d)) != NULL)
      {
        snprintf(full_path, sizeof(full_path) - 1, "%s/%s", store_path, dp->d_name);
        stat(full_path, &st);
        if (S_ISDIR(st.st_mode))
        {
          continue;
        }

        char fname[256] = {0};
        memcpy(fname, dp->d_name, strlen(dp->d_name));
        char *postfix = strtok(fname, ".");
        char *suffix = NULL;
        if (postfix != NULL)
        {
          suffix = strtok(NULL, ".");
        }
        if (NULL == suffix)
        {
          continue;
        }
        else if (!strcmp(suffix, "sst"))
        {
          uint64_t sstable_id = OB_INVALID_ID;
          uint64_t clog_id = OB_INVALID_ID;
          uint64_t last_clog_id = OB_INVALID_ID;
          uint64_t major_version = OB_INVALID_ID;
          if (!SSTableMgr::sstable_str2id(dp->d_name, sstable_id, clog_id, last_clog_id))
          {
            YYSYS_LOG(ERROR, "fname=[%s/%s] trans to sstable_id fail", store_path, dp->d_name);
          }
          major_version = (sstable_id & 0xffffffff00000000) >> 32;
          if (major_version >= max_major_version)
          {
            if (major_version > max_major_version)
            {
              mf_min_clog_id = last_clog_id;
            }
            else if (mf_min_clog_id > last_clog_id)
            {
              mf_min_clog_id = last_clog_id;
            }
            max_major_version = major_version;
            if (clog_id > mf_max_clog_id)
            {
              mf_max_clog_id = clog_id;
            }
          }
        }
      }
    }
  }
  closedir(d);
  return ret;
}

int travel_commitlog_dir(const char *cl_path, const char *sst_path, ObArray<string> &log_files, uint64_t &mf_min_clog_id)
{
  int ret = OB_SUCCESS;
  DIR *d = NULL;
  struct dirent *dp =NULL;
  struct stat st;
  char full_path[256] = {0};
  uint64_t mf_max_clog_id = 0;
  if (stat(cl_path, &st) < 0 || !S_ISDIR(st.st_mode))
  {
    YYSYS_LOG(ERROR, "cl_path:%s is not a directory", cl_path);
    ret = OB_DIR_NOT_EXIST;
  }
  else if (!(d = opendir(cl_path)))
  {
    YYSYS_LOG(ERROR, "open dir:%s fail, error: %m", cl_path);
    ret = OB_DIR_NOT_EXIST;
  }
  else if (OB_SUCCESS != (ret = find_max_major_version(sst_path, mf_max_clog_id, mf_min_clog_id)))
  {
    YYSYS_LOG(ERROR, "find_max_major_version fail, error: %m", cl_path);
  }
  YYSYS_LOG(INFO, "max major freezing version's end commit_log is %lu, start commit_log is %lu", mf_max_clog_id, mf_min_clog_id);

  while (OB_SUCCESS == ret && (dp = readdir(d)) != NULL)
  {
    if ((!strncmp(dp->d_name, ".", 1)) || (!strncmp(dp->d_name, "..", 2)) || (!is_all_digit(dp->d_name)))
      continue;
    snprintf(full_path, sizeof(full_path) - 1, "%s/%s", cl_path, dp->d_name);
    stat(full_path, &st);
    if (S_ISDIR(st.st_mode))
    {
      continue;
    }
    else
    {
      char log_file[100] = {0};
      memcpy(log_file, dp->d_name, 100);
      uint64_t clog_id = 0;
      sscanf(log_file, "%lu", &clog_id);
      if (mf_min_clog_id <= clog_id)
      {
        if (OB_SUCCESS != (ret = log_files.push_back(std::string(log_file))))
        {
          YYSYS_LOG(ERROR, "push file name:%s to array fail, ret = %d", log_file, ret);
        }
      }
    }
  }
  if (OB_SUCCESS == ret)
  {
    sort(&log_files.at(0), (&log_files.at(0) + log_files.count()));
  }
  closedir(d);
  return ret;
}

int copy_sstable(uint64_t cut_log_id, const char *time, char *postfix, char *store_path, bool &create_back_dir, uint64_t &start_cl_id)
{
  YYSYS_LOG(INFO, "start copy sstable");
  int ret = OB_SUCCESS;
  char dest_fname[200];
  sprintf(dest_fname, "%s/%s_%s", store_path, "drbak", time);
  const char *backup_log_dir = dest_fname;
  YYSYS_LOG(INFO, "back up dir is %s", backup_log_dir);
  if (create_back_dir && FSU::create_directory(backup_log_dir))
  {
    if (errno == EEXIST)
    {
      ret = OB_ERROR;
      YYSYS_LOG(ERROR, "%s dir is already exist, errno=%u", backup_log_dir, errno);
    }
    else {
      create_back_dir = false;
    }
  }

  string sstable_postfix = postfix;
  string sstable = sstable_postfix + ".sst";
  string sst_schema = sstable_postfix + ".schema";
  uint64_t sstable_id = OB_INVALID_ID;
  uint64_t clog_id = OB_INVALID_ID;
  uint64_t last_clog_id = OB_INVALID_ID;
  if (!SSTableMgr::sstable_str2id(sstable.c_str(), sstable_id, clog_id, last_clog_id))
  {
    YYSYS_LOG(ERROR, "fname=[%s/%s] trans to sstable_id fail", store_path, sstable.c_str());
  }
  else if (clog_id > cut_log_id)
  {
    if (last_clog_id < start_cl_id)
    {
      start_cl_id = last_clog_id;
    }
    YYSYS_LOG(INFO, "clog_id[%lu] > cut_log_id[%lu], start_cl_id[%lu], need to copy for back_up", clog_id, cut_log_id, start_cl_id);
    if (OB_SUCCESS != (ret = FSU::cp(store_path, sstable.c_str(), backup_log_dir, sstable.c_str())))
    {
      YYSYS_LOG(ERROR, "copy %s/%s to %s/%s fail errno=%u", store_path, sstable.c_str(), backup_log_dir, sstable.c_str(), errno);
    }
    else if (OB_SUCCESS != (ret = FSU::cp(store_path, sst_schema.c_str(), backup_log_dir, sst_schema.c_str())))
    {
      YYSYS_LOG(ERROR, "copy %s/%s to %s/%s fail errno=%u", store_path, sst_schema.c_str(), backup_log_dir, sst_schema.c_str(), errno);
    }
  }
  return ret;
}

int travel_sstable_dir(const char *path, const char *time, DirSstable &sst_dir_map, uint64_t cut_log_id, uint64_t &start_cl_id)
{
  int ret = OB_SUCCESS;
  DIR *d = NULL;
  struct dirent *dp = NULL;
  struct stat st;
  char raid_path[150] = {0};
  char store_path[200] = {0};
  char full_path[256] = {0};
  if (stat(path, &st) < 0 || !S_ISDIR(st.st_mode))
  {
    YYSYS_LOG(ERROR, "path:%s is not a directory", path);
    ret = OB_DIR_NOT_EXIST;
  }
  for (int i = 0; OB_SUCCESS == ret && i < 10; i++)
  {
    sprintf(raid_path, "%s/raid%d", path, i);
    if (stat(raid_path, &st) < 0 || !S_ISDIR(st.st_mode))
    {
      YYSYS_LOG(INFO, "store_path:%s is not a directory", raid_path);
      continue;
    }
    for (int j = 0; j < 10; j++)
    {
      sprintf(store_path, "%s/store%d", raid_path, j);
      if (stat(store_path, &st) < 0 || !S_ISDIR(st.st_mode))
      {
        YYSYS_LOG(INFO, "store_path:%s is not a directory", store_path);
        continue;
      }
      if (!(d = opendir(store_path)))
      {
        ret = OB_DIR_NOT_EXIST;
        YYSYS_LOG(ERROR, "open dir:%s fail, error: %m", store_path);
      }
      bool create_back_dir = true;
      while (OB_SUCCESS == ret && (dp = readdir(d)) != NULL)
      {
        snprintf(full_path, sizeof(full_path) - 1, "%s/%s", store_path, dp->d_name);
        stat(full_path, &st);
        if (S_ISDIR(st.st_mode))
        {
          continue;
        }

        char fname[256] = {0};
        memcpy(fname, dp->d_name, strlen(dp->d_name));
        char *postfix = strtok(fname, ".");
        char *suffix = NULL;
        if (postfix != NULL)
        {
          suffix = strtok(NULL, ".");
        }
        if (NULL == suffix)
        {
          continue;
        }
        else if (!strcmp(suffix, "sst"))
        {
          if (-1 == sst_dir_map.set(string(postfix), string(store_path), 1))
          {
            ret = OB_ERROR;
            YYSYS_LOG(ERROR, "set [%s]'s store_path:%s to map fail, ret=%d", postfix, store_path, ret);
          }
          else if (OB_SUCCESS != (ret = copy_sstable(cut_log_id, time, postfix, store_path, create_back_dir, start_cl_id)))
          {
            YYSYS_LOG(ERROR, "copy sstable:%s fail, ret = %d", store_path, ret);
          }
        }
      }
    }
  }
  closedir(d);
  return ret;
}

int get_file_len(const char *path, int64_t &len)
{
  int err = OB_SUCCESS;
  struct stat _stat;
  if (NULL == path)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if (0 != stat(path, &_stat))
  {
    err = OB_IO_ERROR;
    YYSYS_LOG(ERROR, "fstat(): %s", strerror(errno));
  }
  else
  {
    len = _stat.st_size;
  }
  return err;
}

int file_map_read(const char *path, const int64_t len, const char*& buf)
{
  int err = OB_SUCCESS;
  int fd = 0;
  if (NULL == path)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if ((fd = open(path, O_RDONLY)) < 0)
  {
    err = OB_IO_ERROR;
    YYSYS_LOG(ERROR, "open(%s):%s", path, strerror(errno));
  }
  else if (NULL == (buf = (char *)mmap(NULL, len, PROT_READ, MAP_SHARED, fd, 0)) || MAP_FAILED == buf)
  {
    buf = NULL;
    err = OB_IO_ERROR;
    YYSYS_LOG(ERROR, "mmap[%s] failed:%s", path, strerror(errno));
  }

  if (fd > 0)
  {
    close(fd);
  }
  return err;
}

int get_end_log_id(const char *log_data, int64_t data_len, int64_t &end_log_id, const bool skip_broken=true)
{
  int err = OB_SUCCESS;
  int64_t pos = 0;
  ObLogEntry log_entry;
  const bool check_data_integrity = false;
  end_log_id = 0;

  if (NULL == log_data || data_len <= 0)
  {
    err = OB_INVALID_ARGUMENT;
  }
  while (OB_SUCCESS == err && pos < data_len)
  {
    if (ObLogGenerator::is_eof(log_data + pos, data_len - pos))
    {
      err = OB_READ_NOTHING;
      YYSYS_LOG(WARN, "read eof(buf=%p, pos=%ld, len=%ld)", log_data, pos, data_len);
    }
    else if (OB_SUCCESS != (err = log_entry.deserialize(log_data, data_len, pos)))
    {
      YYSYS_LOG(ERROR, "log_entry.deserialize(log_data=%p, data_len=%ld, pos=%ld)=>%d", log_data, data_len, pos, err);
    }
    else if (pos + log_entry.get_log_data_len() > data_len)
    {
      err = OB_LAST_LOG_RUINNED;
      YYSYS_LOG(ERROR, "last log broken, last_log_id=%ld, offset=%ld", end_log_id, pos - log_entry.get_serialize_size());
    }
    else if (check_data_integrity && OB_SUCCESS != (err = log_entry.check_data_integrity(log_data + pos)))
    {
      YYSYS_LOG(ERROR, "log_entry.check_data_integrity()=>%d", err);
    }
    else
    {
      pos += log_entry.get_log_data_len();
      end_log_id = (int64_t)log_entry.seq_;
    }
  }
  if (end_log_id > 0)
  {
    end_log_id++;
  }
  if (skip_broken && end_log_id > 0)
  {
    err = OB_SUCCESS;
  }
  return err;
}

int locate_in_buf(const char* log_data, int64_t data_len, int64_t end_log_id, int64_t& offset)
{
  int err = OB_SUCCESS;
  int64_t pos = 0;
  ObLogEntry log_entry;
  const bool check_data_integrity = false;
  offset = 0;

  if (NULL == log_data || data_len <= 0)
  {
    err = OB_INVALID_ARGUMENT;
  }
  while (OB_SUCCESS == err && pos < data_len)
  {
    int64_t cur_start = pos;
    if (OB_SUCCESS != (err = log_entry.deserialize(log_data, data_len, pos)))
    {
      YYSYS_LOG(ERROR, "log_entry.deserialize(log_data=%p, data_len=%ld, pos=%ld)=>%d", log_data, data_len, pos, err);
    }
    else if (pos + log_entry.get_log_data_len() > data_len)
    {
      err = OB_LAST_LOG_RUINNED;
      YYSYS_LOG(ERROR, "last log broken, log_id=%ld, offset=%ld", log_entry.seq_, cur_start);
    }
    else if (check_data_integrity && OB_SUCCESS != (err = log_entry.check_data_integrity(log_data + pos)))
    {
      YYSYS_LOG(ERROR, "log_entry.check_data_integrity()=>%d", err);
    }
    else if ((int64_t)log_entry.seq_ == end_log_id)
    {
      offset = cur_start;
      break;
    }
    else if ((int64_t)log_entry.seq_ == end_log_id - 1)
    {
      offset = pos + log_entry.get_log_data_len();
      break;
    }
    else
    {
      pos += log_entry.get_log_data_len();
    }
  }
  return err;
}

static bool is_align(int64_t x, int64_t n_bits)
{
  return 0 == (x & ((1 << n_bits) - 1));
}

int gen_nop(const char *log_file, const int64_t end_log_id, int64_t& next_offset)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  ObLogEntry entry;
  int64_t len = 0;
  int64_t pos = 0;
  int64_t header_size = 0;
  int64_t nop_data_len = 0;
  int fd = 0;

  header_size = entry.get_serialize_size();
  nop_data_len = (-(next_offset + header_size) & ObLogGenerator::LOG_FILE_ALIGN_MASK);
  len = next_offset + nop_data_len + header_size;
  buf = (char *)malloc((nop_data_len + header_size) * sizeof(char));
  memset(buf, 0, nop_data_len + header_size);
  if (NULL == log_file)
  {
    ret = OB_INVALID_ARGUMENT;
  }
  else if ((fd = open(log_file, O_RDWR)) < 0)
  {
    ret = OB_IO_ERROR;
    YYSYS_LOG(ERROR, "open(%s):%s", log_file, strerror(errno));
  }

  //write first nop
  if (OB_SUCCESS == ret)
  {
    entry.set_log_seq(end_log_id);
    entry.set_log_command(OB_LOG_NOP);
    if (OB_SUCCESS != (ret = entry.fill_header(buf + pos + header_size, nop_data_len)))
    {
      YYSYS_LOG(ERROR, "entry.fill_header()=>%d", ret);
    }
    else if (OB_SUCCESS != (ret = entry.serialize(buf, len, pos)))
    {
      YYSYS_LOG(ERROR, "entry.serialize()=>%d", ret);
    }
    else
    {
      pos += nop_data_len;
      YYSYS_LOG(INFO, "write nop log done : file[%s] log_id[%ld] cmd[%d] data_len[%ld] pos[%ld] end_pos[%ld]",
                log_file,  entry.seq_, entry.cmd_, nop_data_len, next_offset, pos + next_offset);
    }
  }
  if (fd > 0)
  {
    lseek(fd, 0, SEEK_END);//end of file
    write(fd, buf, nop_data_len + header_size); //write empty log into file
    YYSYS_LOG(INFO, "nop_data_len =>%ld, header_size =>%ld, sum =>%ld", nop_data_len, header_size, nop_data_len + header_size);
    next_offset = pos + next_offset;
    close(fd);
  }
  return ret;
}

static int cut(const char* log_file, uint64_t& end_seq)
{
  int err = OB_SUCCESS;
  int64_t end_log_id = end_seq;
  const char* src_buf = NULL;
  int64_t len = 0;
  int64_t offset = 0;
  int64_t last_log_id = 0;
  YYSYS_LOG(INFO, "cut(src=%s, end_log_id=%ld)", log_file, end_log_id);
  if (NULL == log_file)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (err = get_file_len(log_file, len)))
  {
    YYSYS_LOG(ERROR, "get_file_len(%s)=>%d", log_file, err);
  }
  else if (OB_SUCCESS != (err = file_map_read(log_file, len, src_buf)))
  {
    YYSYS_LOG(ERROR, "file_map_read(%s)=>%d", log_file, err);
  }
  else if (end_log_id <= 0 && OB_SUCCESS != (err = get_end_log_id(src_buf, len, last_log_id)))
  {
    YYSYS_LOG(ERROR, "get_end_log_id()=>%d", err);
  }
  else if (OB_SUCCESS != (err = locate_in_buf(src_buf, len, end_log_id > 0 ? end_log_id : last_log_id + end_log_id, offset)))
  {
    YYSYS_LOG(ERROR, "dump_log(buf=%p[%ld])=>%d", src_buf, len, err);
  }
  else if (truncate(log_file, offset))
  {
    err = OB_ERROR;
    YYSYS_LOG(ERROR, "fail to do truncate, offset = %ld, ret = %u", offset, errno);
  }
  else if (!is_align(offset, OB_DIRECT_IO_ALIGN_BITS))
  {
    YYSYS_LOG(INFO, "Offset[%ld] is not aligned, need to gen nop log", offset);
    if (OB_SUCCESS != (err = gen_nop(log_file, end_log_id, offset)))
    {
      YYSYS_LOG(ERROR, "fail to gen nop log, ret = %d", err);
    }
    else
    {
      YYSYS_LOG(INFO, "gen nop log successfully");
      end_seq++;
    }
  }
  return err;
}

int Agent::parse_server_cmd(const int argc, char *const argv[])
{
  int ret = OB_SUCCESS;
  int ch = -1;
  while ( -1 != (ch = getopt(argc, argv, "p:c:s:")))
  {
    switch(ch)
    {
      case '?':
        exit(-1);
        break;
      case 'p':
        port = atoi(optarg);
        break;
      case 'c':
        commit_log_dir = optarg;
        break;
      case 's':
        sstable_root_dir = optarg;
        break;
      default:
        break;
    }
  }
  if (OB_SUCCESS != ret)
  {
    //todo
  }
  else if (optind > argc)
  {
    printf("no command specified\n");
    ret = OB_INVALID_ARGUMENT;
  }
  return ret;
}

int Agent::init_log_cuttrans(const char *log_name, TransSet &cut_trans, uint64_t &cutted_seq, int64_t &mutate_timestamp, bool &earlist_log, bool seq_id, bool &begin_cut)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == ret)
  {
    ObRepeatedLogReader reader;
    ret = reader.init(commit_log_dir);
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(ERROR, "Error occured when init, ret = %d", ret);
    }
    else if (OB_SUCCESS != (ret = reader.open(atoi(log_name))))
    {
      YYSYS_LOG(ERROR, "Error occured when opening, ret = %d", ret);
    }

    if (OB_SUCCESS == ret)
    {
      LogCommand cmd;
      uint64_t log_seq;
      uint64_t end_seq = 0;
      char* log_data;
      int64_t data_len;

      YYSYS_LOG(INFO, "begin_cut is %d, seq_id is %d", begin_cut, seq_id);
      end_seq = last_end_seq;

      ret = reader.read_log(cmd, log_seq, log_data, data_len);
      while (OB_SUCCESS == ret)
      {
        if (seq_id)
        {
          if (OB_LOG_SWITCH_LOG == cmd)
          {
            break;
          }
          else if (log_seq >= cutted_seq)
          {
            int64_t pos = 0;
            switch(cmd)
            {
              case OB_UPS_MUTATOR_PREPARE:
              {
                ObTransID coordinator_tid;
                if (OB_SUCCESS != (ret = coordinator_tid.deserialize_for_prepare(log_data, data_len, pos)))
                {
                  YYSYS_LOG(ERROR, "Error occured when deserialize coordinator tid, ret=%d", ret);
                }
                else if (begin_cut)
                {
                  if (-1 == cut_trans.set(coordinator_tid))
                  {
                    ret = OB_ERROR;
                    YYSYS_LOG(ERROR, "Fail to set coordinator_tid to trans_set");
                  }
                  else{
                    local_dis_trans_num++;
                  }
                }
                else if (!begin_cut && (HASH_EXIST == cut_trans.exist(coordinator_tid)))
                {
                  begin_cut = true;
                  cutted_seq = log_seq;
                  ObUpsMutator ups_mutator;
                  if (OB_SUCCESS != (ret = ups_mutator.deserialize(log_data, data_len, pos)))
                  {
                    YYSYS_LOG(ERROR, "Error occured when deserialize ups_mutator, ret=%d", ret);
                  }
                  else{
                    int64_t mutation_ts = ups_mutator.get_mutate_timestamp();
                    YYSYS_LOG(INFO, "ups_mutator.get_mutate_timestamp = [%ld], log_seq = %lu", ups_mutator.get_mutate_timestamp(), log_seq);
                    mutate_timestamp = mutation_ts;
                  }
                }
                break;
              }
              case OB_PART_TRANS_COMMIT:
              case OB_PART_TRANS_ROLLBACK:
              {
                PartCommitInfo commitinfo;
                if (OB_SUCCESS != (ret = commitinfo.deserialize(log_data, data_len, pos)))
                {
                  YYSYS_LOG(ERROR, "Error occured when deserialize commitinfo, ret=%d", ret);
                }
                else{
                  int64_t mutation_ts = commitinfo.mutate_timestamp_;
                  if (begin_cut)
                  {
                    if (-1 == cut_trans.set(commitinfo.coordinator_))
                    {
                      ret = OB_ERROR;
                      YYSYS_LOG(ERROR, "Fail to set coordinator_tid to trans_set");
                    }
                  }
                  else if (!begin_cut && (HASH_EXIST == cut_trans.exist(commitinfo.coordinator_)))
                  {
                    begin_cut = true;
                    cutted_seq = log_seq;
                    mutate_timestamp = mutation_ts;
                    YYSYS_LOG(INFO, "ups_mutator.get_mutate_timestamp = [%ld], log_seq = %lu", commitinfo.mutate_timestamp_, log_seq);
                  }
                }
                break;
              }
              default:
                break;
            }
          }
          else if (log_seq > end_seq)
          {
            break;
          }
          ret = reader.read_log(cmd, log_seq, log_data, data_len);
        }
        else
        {
          if (OB_LOG_SWITCH_LOG == cmd)
          {
            break;
          }
          else
          {
            int64_t pos = 0;
            switch(cmd)
            {
              case OB_UPS_MUTATOR_PREPARE:
              {
                ObTransID coordinator_tid;
                if (OB_SUCCESS != (ret = coordinator_tid.deserialize_for_prepare(log_data, data_len, pos)))
                {
                  YYSYS_LOG(ERROR, "Error occured when deserialize coordinator tid, ret=%d", ret);
                }

                ObUpsMutator ups_mutator;
                if (OB_SUCCESS != (ret = ups_mutator.deserialize(log_data, data_len, pos)))
                {
                  YYSYS_LOG(ERROR, "Error occured when deserialize ups_mutator, ret=%d", ret);
                }
                else{
                  int64_t mutation_ts = ups_mutator.get_mutate_timestamp();
                  if (mutation_ts >= mutate_timestamp)
                  {
                    if (begin_cut)
                    {
                      if (-1 == cut_trans.set(coordinator_tid))
                      {
                        ret = OB_ERROR;
                        YYSYS_LOG(ERROR, "Fail to set coordinator_tid to trans_set");
                      }
                      else
                      {
                        local_dis_trans_num++;
                        if (earlist_log)
                        {
                          earlist_log = false;
                          last_end_seq = log_seq;
                          mutate_timestamp = mutation_ts;
                          YYSYS_LOG(INFO, "ups_mutator.get_mutate_timestamp = [%ld], log_seq = %lu", mutation_ts, log_seq);
                        }
                      }
                    }
                    else if (!begin_cut && (HASH_EXIST == cut_trans.exist(coordinator_tid)))
                    {
                      earlist_log = false;
                      begin_cut = true;
                      last_end_seq = log_seq;
                      mutate_timestamp = mutation_ts;
                      YYSYS_LOG(INFO, "ups_mutator.get_mutate_timestamp = [%ld], log_seq = %lu", mutation_ts, log_seq);
                    }
                  }
                }
                break;
              }
              case OB_PART_TRANS_COMMIT:
              case OB_PART_TRANS_ROLLBACK:
              {
                PartCommitInfo commitinfo;
                if (OB_SUCCESS != (ret = commitinfo.deserialize(log_data, data_len, pos)))
                {
                  YYSYS_LOG(ERROR, "Error occured when deserialize commitinfo, ret=%d", ret);
                }
                else{
                  int64_t mutation_ts = commitinfo.mutate_timestamp_;
                  if (mutation_ts >= mutate_timestamp)
                  {
                    if (begin_cut)
                    {
                      if (-1 == cut_trans.set(commitinfo.coordinator_))
                      {
                        ret = OB_ERROR;
                        YYSYS_LOG(ERROR, "Fail to set coordinator_tid to trans_set");
                      }
                      else
                      {
                        if (earlist_log)
                        {
                          earlist_log = false;
                          last_end_seq = log_seq;
                          mutate_timestamp = mutation_ts;
                          YYSYS_LOG(INFO, "ups_mutator.get_mutate_timestamp = [%ld], log_seq = %lu", mutation_ts, log_seq);
                        }
                      }
                    }
                    else if (!begin_cut && (HASH_EXIST == cut_trans.exist(commitinfo.coordinator_)))
                    {
                      earlist_log = false;
                      begin_cut = true;
                      last_end_seq = log_seq;
                      mutate_timestamp = mutation_ts;
                      YYSYS_LOG(INFO, "ups_mutator.get_mutate_timestamp = [%ld], log_seq = %lu", mutation_ts, log_seq);
                    }
                  }
                }
                break;
              }
              case OB_LOG_UPS_MUTATOR:
              {
                ObUpsMutator ups_mutator;
                if (OB_SUCCESS != (ret = ups_mutator.deserialize(log_data, data_len, pos)))
                {
                  YYSYS_LOG(ERROR, "Error occured when deserialize ups_mutator, ret = %d", ret);
                }
                else if (0 != ups_mutator.get_flag())
                {
                  YYSYS_LOG(INFO, "ups_mutator.get_flag() = %d, no valid mutate_timestamp", ups_mutator.get_flag());
                }
                else{
                  int64_t mutation_ts = ups_mutator.get_mutate_timestamp();
                  if (mutation_ts >= mutate_timestamp)
                  {
                    if (earlist_log)
                    {
                      earlist_log = false;
                      begin_cut = true;
                      last_end_seq = log_seq;
                      mutate_timestamp = mutation_ts;
                      YYSYS_LOG(INFO, "ups_mutator.get_flag() = %d, ups_mutator.get_mutate_timestamp = [%ld], log_seq = %lu",
                                ups_mutator.get_flag(), ups_mutator.get_mutate_timestamp(), log_seq);
                    }
                  }
                }
                break;
              }
              default:
                break;
            }
          }
          ret = reader.read_log(cmd, log_seq, log_data, data_len);
        }
      }
      if (OB_READ_NOTHING == ret || OB_SUCCESS == ret)
      {
        ret = OB_SUCCESS;
      }
      else
      {
        YYSYS_LOG(ERROR, "Error occured when reading, ret=%d", ret);
      }
    }
  }
  return ret;
}

int Agent::get_log_cuttrans(const char *log_name, TransSet &cut_trans, int64_t &mutate_timestamp, int64_t &dis_tran_num, ObArray<ObTransID> &before_exist_array)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == ret)
  {
    ObRepeatedLogReader reader;
    ret = reader.init(commit_log_dir);
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(ERROR, "Error occured when init, ret = %d", ret);
    }
    else if (OB_SUCCESS != (ret = reader.open(atoi(log_name))))
    {
      YYSYS_LOG(ERROR, "Error occured when opening, ret = %d", ret);
    }

    if (OB_SUCCESS == ret)
    {
      LogCommand cmd;
      uint64_t log_seq;
      bool first_log = true;
      uint64_t end_seq = 0;
      char* log_data;
      int64_t data_len;
      int64_t trans_size = cut_trans.size();
      ret = reader.read_log(cmd, log_seq, log_data, data_len);
      YYSYS_LOG(INFO, "get_log_cuttrans last_end_seq = %lu", last_end_seq);
      while (OB_SUCCESS == ret)
      {
        if (OB_LOG_SWITCH_LOG == cmd)
        {
          break;
        }
        else if (log_seq < last_end_seq)
        {
          int64_t pos = 0;
          switch(cmd)
          {
            case OB_UPS_MUTATOR_PREPARE:
            {
              ObTransID coordinator_tid;
              ObUpsMutator ups_mutator;
              if (OB_SUCCESS != (ret = coordinator_tid.deserialize_for_prepare(log_data, data_len, pos)))
              {
                YYSYS_LOG(ERROR, "Error occured when deserialize coordinator tid, ret=%d", ret);
              }
              else  if (OB_SUCCESS != (ret = ups_mutator.deserialize(log_data, data_len, pos)))
              {
                YYSYS_LOG(ERROR, "Error occured when deserialize ups_mutator, ret=%d", ret);
              }
              else if (HASH_EXIST == cut_trans.exist(coordinator_tid))
              {
                dis_tran_num++;
                if (first_log)
                {
                  first_log = false;
                  mutate_timestamp = ups_mutator.get_mutate_timestamp();
                  end_seq = log_seq;
                  YYSYS_LOG(INFO, "ups_mutator.get_mutate_timestamp = [%ld], log_seq = %lu", mutate_timestamp, log_seq);
                }
              }
              else if (!first_log)
              {
                if (-1 == cut_trans.set(coordinator_tid))
                {
                  ret = OB_ERROR;
                  YYSYS_LOG(ERROR, "Fail to set coordinator_tid to trans_set");
                }
                else{
                  dis_tran_num++;
                }
              }
              else{
                if (OB_SUCCESS != (ret = before_exist_array.push_back(coordinator_tid)))
                {
                  YYSYS_LOG(ERROR, "push coordinator_tid:%s to array fail, ret = %d", to_cstring(coordinator_tid), ret);
                }
              }
              break;
            }
            case OB_PART_TRANS_COMMIT:
            case OB_PART_TRANS_ROLLBACK:
            {
              PartCommitInfo commitinfo;
              if (OB_SUCCESS != (ret = commitinfo.deserialize(log_data, data_len, pos)))
              {
                YYSYS_LOG(ERROR, "Error occured when deserialize commitinfo, ret=%d", ret);
              }
              else if (HASH_EXIST == cut_trans.exist(commitinfo.coordinator_))
              {
                if (first_log)
                {
                  first_log = false;
                  mutate_timestamp = commitinfo.mutate_timestamp_;
                  end_seq = log_seq;
                  YYSYS_LOG(INFO, "commitinfo.mutate_timestamp_ = [%ld], log_seq = %lu", mutate_timestamp, log_seq);
                }
              }
              else if (!first_log)
              {
                if (-1 == cut_trans.set(commitinfo.coordinator_))
                {
                  ret = OB_ERROR;
                  YYSYS_LOG(ERROR, "Fail to set coordinator_tid to trans_set");
                }
              }
              else{
                if (OB_SUCCESS != (ret = before_exist_array.push_back(commitinfo.coordinator_)))
                {
                  YYSYS_LOG(ERROR, "push commitinfo.coordinator_:%s to array fail, ret = %d", to_cstring(commitinfo.coordinator_), ret);
                }
              }
              break;
            }
            default:
              break;
          }
        }
        ret = reader.read_log(cmd, log_seq, log_data, data_len);
      }
      if (end_seq != 0)
      {
        last_end_seq = end_seq;
      }
      if ((local_dis_trans_num + dis_tran_num) < trans_size)
      {
        ObTransID tid;
        for (int64_t i = before_exist_array.count() - 1; i >= 0 && OB_SUCCESS ==  ret; i--)
        {
          if (OB_SUCCESS != (ret = before_exist_array.at(i, tid)))
          {
            YYSYS_LOG(INFO, "Error occured when get log_name from array, ret=%d", ret);
          }
          else if (-1 == cut_trans.set(tid))
          {
            ret = OB_ERROR;
            YYSYS_LOG(ERROR, "Fail to set coordinator_tid to trans_set");
          }
        }
      }
      if (OB_READ_NOTHING == ret || OB_SUCCESS == ret)
      {
        ret = OB_SUCCESS;
      }
      else
      {
        YYSYS_LOG(ERROR, "Error occured when reading, ret=%d", ret);
      }
      before_exist_array.clear();
    }
  }
  return ret;
}

int Agent::init_ip_cuttrans(ObArray<string> &log_files, TransSet &cut_trans, uint64_t &cutted_seq, int64_t &mutate_timestamp)
{
  int ret = OB_SUCCESS;
  string log_name;
  bool earlist_log = true;
  bool seq_id = true;
  bool begin_cut = false;
  if (mutate_timestamp == 0 || cutted_seq > 0)
  {
    seq_id = true;
    begin_cut = cutted_seq > 0 ? true : false;
  }
  else if (mutate_timestamp > 0)
  {
    seq_id = false;
    begin_cut = true;
  }

  uint64_t cur_cut_log = UINT64_MAX;
  uint64_t start_cut_log = UINT64_MAX;
  if (cutted_seq > 0)
  {
    if (OB_SUCCESS != (ret = locate(commit_log_dir, cutted_seq, cur_cut_log)))
    {
      YYSYS_LOG(WARN, "locate(log_dir=%s, log_id=%lu), ret=[%d]", commit_log_dir, cutted_seq, ret);
      cur_cut_log = 0;
      ret = OB_SUCCESS;
    }
  }
  else if (mutate_timestamp > 0)
  {
    if (OB_SUCCESS != (ret = locate_by_time(commit_log_dir, log_files, mutate_timestamp, cur_cut_log)))
    {
      YYSYS_LOG(WARN, "locate_by_time(log_dir=%s, mutate_timestamp=%ld), ret=[%d]", commit_log_dir, mutate_timestamp, ret);
      cur_cut_log = 0;
      ret = OB_SUCCESS;
    }
  }

  for (int64_t i = 0; i <log_files.count() && OB_SUCCESS == ret; i++)
  {
    if (OB_SUCCESS != (ret = log_files.at(i, log_name)))
    {
      YYSYS_LOG(INFO, "Error occured when get log_name from array, ret=%d", ret);
    }
    else
    {
      sscanf(log_name.c_str(), "%lu", &start_cut_log);
      if (start_cut_log < cur_cut_log)
      {
        YYSYS_LOG(INFO, "start_cut_log[%lu] < cur_cut_log[%lu]", start_cut_log, cur_cut_log);
        continue;
      }
      else if (OB_SUCCESS != (ret = init_log_cuttrans(log_name.c_str(), cut_trans, cutted_seq, mutate_timestamp, earlist_log, seq_id, begin_cut)))
      {
        YYSYS_LOG(INFO, "Error occured when get cut_trans from log:%s, ret=%d", log_name.c_str(), ret);
      }
    }
  }
  YYSYS_LOG(INFO, "cut_trans.size[%ld], cutted_seq = %lu", cut_trans.size(), cutted_seq);
  return ret;
}

int Agent::get_ip_cuttrans(ObArray<string> &log_files, TransSet &cut_trans, int64_t& mutate_timestamp, ObArray<ObTransID> &before_exist_array)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == ret)
  {
    string log_name;
    uint64_t cur_cut_log = UINT64_MAX;
    uint64_t clog_id = 0;

    if (0 != last_end_seq && OB_SUCCESS != (ret = locate(commit_log_dir, last_end_seq, cur_cut_log)))
    {
      YYSYS_LOG(WARN, "locate(log_dir=%s, log_id=%lu), ret=[%d]", commit_log_dir, last_end_seq, ret);
      if (cur_cut_log == UINT64_MAX)
        ret = OB_SUCCESS;
    }
    if (0 == last_end_seq)
    {
      last_end_seq = UINT64_MAX;
    }
    YYSYS_LOG(INFO, "ranging from %s to %lu, start from %lu", log_files.at(0).c_str(), cur_cut_log, cur_cut_log);
    for (int64_t i = log_files.count() - 1; i >= 0 && OB_SUCCESS == ret; i--)
    {
      int64_t dis_tran_num = 0;
      int64_t cur_size = cut_trans.size();
      YYSYS_LOG(INFO, "local_dis_trans_num = %ld, cut_trans = %ld", local_dis_trans_num, cut_trans.size());
      if (OB_SUCCESS != (ret = log_files.at(i, log_name)))
      {
        YYSYS_LOG(INFO, "Error occured when get log_name from array, ret=%d", ret);
      }
      else if (!sscanf(log_name.c_str(), "%lu", &clog_id))
      {
        YYSYS_LOG(INFO, "Error occured when transfer logfile str to uint64_t");
      }
      else if (clog_id > cur_cut_log)
      {
        YYSYS_LOG(INFO, "clog_id[%lu] > cur_cut_log[%lu]", clog_id, cur_cut_log);
      }
      else if (OB_SUCCESS != (ret = get_log_cuttrans(log_name.c_str(), cut_trans, mutate_timestamp, dis_tran_num, before_exist_array)))
      {
        YYSYS_LOG(INFO, "Error occured when get cut_trans from log:%s, ret=%d", log_name.c_str(), ret);
      }
      else if ((local_dis_trans_num + dis_tran_num) < cur_size)
      {
        YYSYS_LOG(INFO, "(local_dis_trans_num[%ld] + dis_tran_num[%ld]) < cut_trans.size()[%ld]", local_dis_trans_num, dis_tran_num, cut_trans.size());
        local_dis_trans_num += dis_tran_num;
      }
      else if ((local_dis_trans_num + dis_tran_num) == cur_size)
      {
        YYSYS_LOG(INFO, "(local_dis_trans_num[%ld] + dis_tran_num[%ld]) = cut_trans.size()[%ld]", local_dis_trans_num, dis_tran_num, cut_trans.size());
        local_dis_trans_num = cur_size;
        if (cur_size < cut_trans.size())
        {
          YYSYS_LOG(INFO, "more dis_trans setted into cut_trans, num = %ld", cut_trans.size() - cur_size);
        }
        else if (cur_size == cut_trans.size())
        {
          YYSYS_LOG(INFO, "no more dis_trans");
          break;
        }
      }
      else if ((local_dis_trans_num + dis_tran_num) == cut_trans.size())
      {
        YYSYS_LOG(INFO, "already find all dis_trans");
        break;
      }
    }
    if (UINT64_MAX == last_end_seq)
    {
      last_end_seq = 0;
    }
    YYSYS_LOG(INFO, "cut_trans.size[%ld]", cut_trans.size());
  }
  return ret;
}

int Agent::handler_init_request(int fd, PACKET packet, ObArray<string> log_files)
{
  int ret = OB_SUCCESS;
  PACKET res_packet;
  res_packet.sno = packet.sno;
  res_packet.ts = packet.ts;
  last_end_seq = packet.seq;
  YYSYS_LOG(INFO, "last_end_seq = %lu, packet.seq = %ld, packet.mutate_timestamp = %ld",
            last_end_seq, packet.seq, packet.mutate_timestamp);
  uint64_t cutted_seq = packet.seq;
  int64_t mutate_timestamp = packet.mutate_timestamp;

  if (OB_SUCCESS != (ret = init_ip_cuttrans(log_files, *(res_packet.ts), cutted_seq, mutate_timestamp)))
  {
    YYSYS_LOG(ERROR, "get cutted trans of from dir:[%s]'s log fail", commit_log_dir);
    res_packet.ts = NULL;
    res_packet.ts_size = 0;
    res_packet.seq = 0;
    res_packet.mutate_timestamp = 0;
  }
  else{
    res_packet.ts_size = res_packet.ts->size();
    res_packet.seq = last_end_seq;
    res_packet.mutate_timestamp = mutate_timestamp;
  }
  res_packet.type = ret;
  if (DISCONNECTED == send_packet(fd, res_packet))
  {
    YYSYS_LOG(ERROR, "send_packet fail");
  }
  return ret;
}

int Agent::handler_search_request(int fd, PACKET packet, ObArray<string> log_files, ObArray<ObTransID> &before_exist_array)
{
  int ret = OB_SUCCESS;
  PACKET res_packet;
  res_packet.sno = packet.sno;
  res_packet.ts = packet.ts;
  last_end_seq = packet.seq;

  int64_t mutate_timestamp = 0;

  if (OB_SUCCESS != (ret = get_ip_cuttrans(log_files, *(res_packet.ts), mutate_timestamp, before_exist_array)))
  {
    YYSYS_LOG(ERROR, "get cutted trans of from dir:[%s]'s log fail", commit_log_dir);
    res_packet.ts = NULL;
    res_packet.ts_size = 0;
    res_packet.seq = 0;
    res_packet.mutate_timestamp = 0;
  }
  else{
    res_packet.ts_size = res_packet.ts->size();
    res_packet.seq = last_end_seq;
    res_packet.mutate_timestamp = mutate_timestamp;
  }
  res_packet.type = ret;
  if (DISCONNECTED == send_packet(fd, res_packet))
  {
    YYSYS_LOG(ERROR, "send_packet fail");
  }
  return ret;
}

int Agent::handler_cut_request(int fd, PACKET packet)
{
  int ret = OB_SUCCESS;
  PACKET res_packet;
  last_end_seq = packet.seq;
  res_packet.seq = packet.seq;
  uint64_t log_file = 0;
  uint64_t start_log_file = UINT64_MAX;
  uint64_t max_log_file_id = 0;
  if (0 == last_end_seq)
  {
    YYSYS_LOG(INFO, "no need for cutting");
  }
  else if (OB_SUCCESS != (ret = locate(commit_log_dir, last_end_seq, log_file)))
  {
    YYSYS_LOG(ERROR, "locate(log_dir=%s, log_id=%lu), ret=[%d]", commit_log_dir, last_end_seq, ret);
  }
  else if (log_file < max_major_freezing_min_clog_id)
  {
    ret = OB_ENTRY_NOT_EXIST;
    YYSYS_LOG(ERROR, "log_file[%lu] < major_freezing_version[%lu], ret=[%d]", log_file, max_major_freezing_min_clog_id, ret);
  }
  else if (OB_SUCCESS != (ret = get_max_log_file_id(commit_log_dir, max_log_file_id)))
  {
    if (OB_ENTRY_NOT_EXIST != ret)
    {
      YYSYS_LOG(ERROR, "get_max_log_file_id(log_dir=%s)=>%d", commit_log_dir, ret);
    }
  }
  else
  {
    DirSstable sst_dir_map;
    char time[30] = {0};
    get_sys_time(time);
    if (OB_SUCCESS != (ret = backup_log_file_and_clrpoint_file(log_file, max_log_file_id, time)))
    {
      YYSYS_LOG(ERROR, "fail to back log files or commit_point or log_replay_point, ret = %d", ret);
    }
    else if (OB_SUCCESS != (ret = backup_sstable(log_file, sst_dir_map, time, start_log_file)))
    {
      YYSYS_LOG(ERROR, "fail to back sstables, ret = %d", ret);
    }
    else
    {
      char file_path[100];
      sprintf(file_path, "%s/%ld", commit_log_dir, log_file);
      YYSYS_LOG(INFO, "The log_file to be cutted is [%s]", file_path);
      if (OB_SUCCESS != (ret = cut(file_path, last_end_seq)))
      {
        YYSYS_LOG(ERROR, "Failed to cut log, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = delete_log_file_and_clrpoint_file(log_file + 1, max_log_file_id, start_log_file)))
      {
        YYSYS_LOG(ERROR, "Failed to delete log files and commit_point, ret=%d", ret);
      }
      else if (OB_SUCCESS != (ret = delete_sstable(log_file, sst_dir_map)))
      {
        YYSYS_LOG(ERROR, "Failed to delete sstables, ret=%d", ret);
      }
      else
      {
        res_packet.seq = last_end_seq - 1;
        YYSYS_LOG(INFO, "The log file [%s] already recover to log %lu", file_path, res_packet.seq);
      }
    }
  }

  res_packet.sno = packet.sno;
  res_packet.mutate_timestamp = 0;
  res_packet.type = ret;
  res_packet.ts_size = 0;
  res_packet.ts = NULL;
  if (DISCONNECTED == send_packet(fd, res_packet))
  {
    YYSYS_LOG(ERROR, "send_packet fail");
  }
  return ret;
}

int Agent::handle_packet(int sockfd)
{
  int ret = OB_SUCCESS;
  struct sockaddr_in cliaddr;
  char cli_ip[20];
  memset(&cliaddr, 0x00, sizeof(cliaddr));
  memset(cli_ip, 0x00, sizeof(cli_ip));
  unsigned int len_cliaddr = sizeof(cliaddr);
  getpeername(sockfd, (struct sockaddr *)&cliaddr, &len_cliaddr);
  inet_ntop(AF_INET, &cliaddr.sin_addr, cli_ip, sizeof(cliaddr));

  TransSet ruined_dis_trans_set;
  ObArray<ObTransID> before_exist_trans_array;
  ObArray<string> log_files;

  if (OB_SUCCESS != (ret = ruined_dis_trans_set.create(hash::cal_next_prime(TRANS_BUCKET))))
  {
    YYSYS_LOG(ERROR, "create ruined_dis_trans_set failed, ret=%d", ret);
  }

  while (OB_SUCCESS == ret) {
    PACKET packet;
    packet.ts = &ruined_dis_trans_set;
    if (OB_SUCCESS != (ret = receive_packet(sockfd, packet))) {
      YYSYS_LOG(ERROR, "receive_packet fail");
      return ret;
    }
    else {
      YYSYS_LOG(INFO, "receive_packet:(sno=%d, type=%d, mutate_timestamp=%ld, seq=%ld, ts_size=%ld), client:%s%d",
                packet.sno, packet.type, packet.mutate_timestamp, packet.seq,
                packet.ts_size, cli_ip, cliaddr.sin_port);

      switch (packet.type) {
        case TRANS_SET_INIT:
          if (OB_SUCCESS != (ret = travel_commitlog_dir(commit_log_dir, sstable_root_dir, log_files, max_major_freezing_min_clog_id)))
          {
            YYSYS_LOG(ERROR, "travel commitlog dir:%s fail, ret=%d", commit_log_dir, ret);
            log_files.clear();
            ruined_dis_trans_set.clear();
          }
          else if (OB_SUCCESS != (ret = handler_init_request(sockfd, packet, log_files)))
          {
            YYSYS_LOG(ERROR, "handler_init_request fail, ret = [%d]", ret);
            log_files.clear();
            ruined_dis_trans_set.clear();
          }
          else{
            YYSYS_LOG(INFO, "finish initialization");
          }
          break;
        case TRANS_SET_SEARCH:
          if (OB_SUCCESS != (ret = handler_search_request(sockfd, packet, log_files, before_exist_trans_array)))
          {
            YYSYS_LOG(ERROR, "handler_search_request fail, ret = [%d]", ret);
            log_files.clear();
            ruined_dis_trans_set.clear();
          }
          break;
        case START_CUTTING:
          ret = OB_SUCCESS;
          if (OB_SUCCESS != (ret = handler_cut_request(sockfd, packet)))
          {
            YYSYS_LOG(ERROR, "handler_cut_request fail, ret = [%d]", ret);
          }
          log_files.clear();
          ruined_dis_trans_set.clear();
          return ret;
        case NO_CUTTING:
          ret = OB_SUCCESS;
          log_files.clear();
          ruined_dis_trans_set.clear();
          YYSYS_LOG(WARN, "No cutting!");
          return ret;
        default:
          ret = OB_ERROR;
          log_files.clear();
          ruined_dis_trans_set.clear();
          YYSYS_LOG(ERROR, "packet type error!");
          return ret;
      }
    }
  }
  return ret;
}

int Agent::backup_log_file_and_clrpoint_file(int64_t start_copy_file_id, int64_t end_file_id, const char *time)
{
  int ret = OB_SUCCESS;
  YYSYS_LOG(INFO, "start copy log_file ranging from %ld to %ld", start_copy_file_id, end_file_id);
  if (start_copy_file_id > end_file_id)
  {
    ret = OB_ERROR;
    YYSYS_LOG(ERROR, "start copy file id[%ld] larger than end file id[%ld]", start_copy_file_id, end_file_id);
  }
  char dest_fname[100];
  sprintf(dest_fname, "%s/%s_%s", commit_log_dir, "drbak", time);
  const char *backup_log_dir = dest_fname;
  YYSYS_LOG(INFO, "back up dir is %s", backup_log_dir);
  if (FSU::create_directory(backup_log_dir))
  {
    if (errno == EEXIST)
    {
      ret = OB_ERROR;
      YYSYS_LOG(ERROR, "%s dir is already exist, errno = %u", backup_log_dir, errno);
    }
  }
  int64_t start_id = start_copy_file_id;
  while (start_id <= end_file_id && ret == OB_SUCCESS)
  {
    char *src_fname;
    src_fname = const_cast<char *>(to_cstring(start_id));
    if (src_fname == NULL || commit_log_dir == NULL || backup_log_dir == NULL || src_fname == NULL)
    {
      ret = OB_INVALID_ARGUMENT;
      YYSYS_LOG(ERROR, "src_fname is null, ret is %d", ret);
    }
    else if (OB_SUCCESS != (ret = FSU::cp(commit_log_dir, src_fname, backup_log_dir, src_fname)))
    {
      YYSYS_LOG(ERROR, "copy %s/%s to %s/%s fail errno=%u", commit_log_dir, src_fname, backup_log_dir, src_fname, errno);
    }
    ++start_id;
  }
  if (ret == OB_SUCCESS)
  {
    YYSYS_LOG(INFO, "copy log files to dir[%s] END", backup_log_dir);
    const char *src_fname = "commit_point";
    if (OB_SUCCESS != (ret = FSU::cp(commit_log_dir, src_fname, backup_log_dir, src_fname)))
    {
      YYSYS_LOG(ERROR, "copy %s/commit_point to %s/commit_point fail errno=%u", commit_log_dir, backup_log_dir, errno);
    }
  }
  if (ret == OB_SUCCESS)
  {
    YYSYS_LOG(INFO, "copy commit point to dir[%s] END", backup_log_dir);
    char lr_fname[100];
    sprintf(lr_fname, "%s/%s", commit_log_dir, "log_replay_point");
    FILE *lrp_file = NULL;
    if (NULL != (lrp_file = fopen(lr_fname, "r")))
    {
      char lr_info[30] = {0};
      int64_t lr_point = 0;
      while (OB_SUCCESS == ret && NULL != fgets(lr_info, 120, lrp_file))
      {
        sscanf(lr_info, "%ld", &lr_point);
      }
      fclose(lrp_file);
      lrp_file = NULL;
      YYSYS_LOG(INFO, "log_replay_point is %ld", lr_point);
      if (lr_point > start_copy_file_id)
      {
        need_delete_sstable = true;
      }
      else
      {
        need_delete_sstable = false;
      }
      if (!need_delete_sstable)
      {
        YYSYS_LOG(INFO, "don't need to copy log_replay_point");
      }
      else if (OB_SUCCESS != (ret = FSU::cp(commit_log_dir, "log_replay_point", backup_log_dir, "log_replay_point")))
      {
        YYSYS_LOG(ERROR, "copy %s/log_replay_point to %s/log_replay_point fail errno=%u", commit_log_dir, backup_log_dir, errno);
      }
      else
      {
        YYSYS_LOG(INFO, "copy log_replay_point to dir[%s] DONE", backup_log_dir);
      }
    }
    else
    {
      YYSYS_LOG(INFO, "No log_replay_point");
    }
  }
  YYSYS_LOG(INFO, "copy end, ret = %d", ret);
  return ret;
}

int Agent::delete_log_file_and_clrpoint_file(int64_t start_delete_file_id, int64_t end_file_id, uint64_t start_cl_id)
{
  int ret = OB_SUCCESS;
  YYSYS_LOG(INFO, "start delete log_file");
  if (start_delete_file_id > end_file_id)
  {
    YYSYS_LOG(WARN, "start delete file id[%ld] larger than end file id[%ld]", start_delete_file_id, end_file_id);
  }
  else
  {
    int64_t start_id = start_delete_file_id;
    while (ret == OB_SUCCESS && start_id <= end_file_id)
    {
      char *src_fname;
      src_fname = const_cast<char *>(to_cstring(start_id));
      char dest_fname[100];
      sprintf(dest_fname, "%s/%s", commit_log_dir, src_fname);
      if (src_fname == NULL)
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(ERROR, "src_fname is null, ret is %d", ret);
      }
      else if (!FSU::delete_file(dest_fname))
      {
        ret = OB_ERROR;
        YYSYS_LOG(ERROR, "fail to do delete log file");
      }
      else
      {
        YYSYS_LOG(INFO, "Delete file %ld, ret = %d, file_name = [%s]", start_id, ret, dest_fname);
      }
      ++start_id;
    }
  }
  FILE *point;
  char point_fname[100];
  sprintf(point_fname, "%s/%s", commit_log_dir, "commit_point");
  if (NULL == (point = fopen(point_fname, "w+")))
  {
    ret = OB_ERROR;
    YYSYS_LOG(ERROR, "failed to open file commit_point");
  }
  else
  {
    fprintf(point, "%lu", last_end_seq);
    fclose(point);
    point = NULL;
  }
  sprintf(point_fname, "%s/%s", commit_log_dir, "log_replay_point");
  if (!need_delete_sstable)
  {
    YYSYS_LOG(INFO, "don't need to delete log_replay_point");
  }
  else if (FSU::exists(point_fname))
  {
    if (NULL == (point = fopen(point_fname, "w+")))
    {
      ret = OB_ERROR;
      YYSYS_LOG(ERROR, "failed to open file log_replay_point");
    }
    else
    {
      fprintf(point, "%lu", start_cl_id);
      fclose(point);
      point = NULL;
    }
  }
  else
  {
    YYSYS_LOG(ERROR, "No log_replay_point");
  }
  return ret;
}

int Agent::backup_sstable(int64_t cut_log_id, DirSstable &sst_dir_map, const char *time, uint64_t &start_cl_id)
{
  int ret = OB_SUCCESS;
  if (!need_delete_sstable)
  {
    YYSYS_LOG(INFO, "don't need to backup sstable");
  }
  else if (NULL == sstable_root_dir)
  {
    YYSYS_LOG(ERROR, "sstable_dir is null");
  }
  else if (OB_SUCCESS != (ret = sst_dir_map.create(hash::cal_next_prime(MAX_STORE_COUNT_ONE_UPS))))
  {
    YYSYS_LOG(ERROR, "create dir_sst_map failed, ret=%d", ret);
  }
  else if (OB_SUCCESS != (ret = travel_sstable_dir(sstable_root_dir, time, sst_dir_map, uint64_t(cut_log_id), start_cl_id)))
  {
    YYSYS_LOG(ERROR, "travel sstable root dir:%s fail, ret=%d", sstable_root_dir, ret);
  }
  return ret;
}

int Agent::delete_sstable(int64_t cut_log_id, DirSstable &sst_dir_map)
{
  int ret = OB_SUCCESS;
  if (!need_delete_sstable)
  {
    YYSYS_LOG(INFO, "don't need to delete sstable");
  }
  else if (NULL == sstable_root_dir)
  {
    YYSYS_LOG(ERROR, "sstable_dir is null");
  }
  else
  {
    YYSYS_LOG(INFO, "start delete sstable");
    string sstable_postfix;
    for (DirSstable::iterator sst_dir = sst_dir_map.begin(); sst_dir != sst_dir_map.end() && OB_SUCCESS == ret; sst_dir++)
    {
      sstable_postfix = sst_dir->first;
      string sstable = sstable_postfix + ".sst";
      string sst_schema = sstable_postfix + ".schema";
      uint64_t sstable_id = OB_INVALID_ID;
      uint64_t clog_id = OB_INVALID_ID;
      uint64_t last_clog_id = OB_INVALID_ID;
      if (!SSTableMgr::sstable_str2id(sstable.c_str(), sstable_id, clog_id, last_clog_id))
      {
        YYSYS_LOG(ERROR, "fname=[%s/%s] trans to sstable_id fail", sst_dir->second.c_str(), sstable.c_str());
      }
      else if (clog_id > uint64_t(cut_log_id))
      {
        char sst_fpath[100], schema_fpath[100];
        sprintf(sst_fpath, "%s/%s", sst_dir->second.c_str(), sstable.c_str());
        sprintf(schema_fpath, "%s/%s", sst_dir->second.c_str(), sst_schema.c_str());
        if (NULL == sst_fpath || NULL == schema_fpath)
        {
          ret = OB_INVALID_ARGUMENT;
          YYSYS_LOG(ERROR, "sstable is null, ret is %d", ret);
        }
        else if (!FSU::delete_file(sst_fpath))
        {
          ret = OB_ERROR;
          YYSYS_LOG(ERROR, "fail to do delete [%s]", sst_fpath);
        }
        else if (!FSU::delete_file(schema_fpath))
        {
          ret = OB_ERROR;
          YYSYS_LOG(ERROR, "fail to do delete [%s]", schema_fpath);
        }
        else
        {
          YYSYS_LOG(INFO, "Delete sst_name = [%s], schema_name = [%s], ret = %d", sst_fpath, schema_fpath, ret);
        }
      }
    }
  }
  return ret;
}

int Agent::start_server()
{
  int client_sock = -1;
  struct sockaddr_in server_addr;
  bzero(&server_addr, sizeof(server_addr));
  server_addr.sin_family =AF_INET;
  server_addr.sin_addr.s_addr = inet_addr("0.0.0.0");
  server_addr.sin_port = htons(port);
  int ret = OB_SUCCESS;

  server_sock = socket(AF_INET, SOCK_STREAM, 0);
  if (DISCONNECTED == server_sock) {
    YYSYS_LOG(ERROR, "socket fail, errno:%d-%s", errno, strerror(errno));
    return DISCONNECTED;
  }
  if (setsock(server_sock) == DISCONNECTED) {
    YYSYS_LOG(ERROR, "set sockopt fail, errno:%d-%s", errno, strerror(errno));
    return DISCONNECTED;
  }
  if (bind(server_sock, (struct sockaddr *)(&server_addr), sizeof(server_addr)) == DISCONNECTED) {
    YYSYS_LOG(ERROR, "bind fail, errno:%d-%s", errno, strerror(errno));
    return DISCONNECTED;
  }
  if (listen(server_sock, 32) == DISCONNECTED) {
    YYSYS_LOG(ERROR, "listen fail, errno:%d-%s", errno, strerror(errno));
    return DISCONNECTED;
  }

  socklen_t server_addr_size = sizeof(server_addr);

  while (true)
  {
    if ((client_sock = accept(server_sock, (struct sockaddr *) (&server_addr), &server_addr_size)) == DISCONNECTED) {
      YYSYS_LOG(ERROR, "accept fail, errno:%d-%s", errno, strerror(errno));
      return DISCONNECTED;
    }
    if (OB_SUCCESS != (ret = handle_packet(client_sock)))
    {
      YYSYS_LOG(ERROR, "handle_packet error");
    }
    else {
      reset();
    }
    close(client_sock);
  }
  return 0;
}

int main(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  ob_init_memory_pool();
  Agent agent;

  if (OB_SUCCESS != (ret = agent.parse_server_cmd(argc, argv)))
  {
    printf("parse cmd line error, err=%d\n", ret);
  }
  else
  {
    agent.start_server();
  }
  return ret == OB_SUCCESS ? 0 : -1;
}
