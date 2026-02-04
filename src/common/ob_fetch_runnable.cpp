/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yanran <yanran.hfs@taobao.com>
 *     - some work details if you want
 */

#include "ob_fetch_runnable.h"
#include "file_directory_utils.h"
#include "ob_log_dir_scanner.h"
#include "ob_log_entry.h"
#include "file_directory_utils.h"

using namespace oceanbase::common;

const char* ObFetchRunnable::DEFAULT_FETCH_OPTION = "-e \"ssh -oStrictHostKeyChecking=no\" -avz --inplace";

int64_t ObFetchParam::get_serialize_size(void) const
{
  return sizeof(*this);
}

int ObFetchParam::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
  int err = OB_SUCCESS;

  if (pos + (int64_t) sizeof(*this) > buf_len)
  {
    YYSYS_LOG(WARN, "buf is not enough, pos=%ld, buf_len=%ld, need_len=%ld",
              pos, buf_len, sizeof(*this));
    err = OB_ERROR;
  }
  else
  {
    *(reinterpret_cast<ObFetchParam*> (buf + pos)) = *this;
    pos += sizeof(*this);
  }

  return err;
}

int ObFetchParam::deserialize(const char* buf, const int64_t buf_len, int64_t& pos)
{
  int err = OB_SUCCESS;

  if (pos + (int64_t) sizeof(*this) > buf_len)
  {
    YYSYS_LOG(WARN, "buf is not enough, pos=%ld, buf_len=%ld, need_len=%ld",
              pos, buf_len, sizeof(*this));
    err = OB_ERROR;
  }
  else
  {
    *this = *(reinterpret_cast<const ObFetchParam*> (buf + pos));
    pos += sizeof(*this);
  }

  return err;
}


ObFetchRunnable::ObFetchRunnable()
{
  limit_rate_ = DEFAULT_LIMIT_RATE;
  param_.min_log_id_ = 0;
  param_.max_log_id_ = 0;
  param_.ckpt_id_ = 0;
  param_.fetch_log_ = 0;
  param_.fetch_ckpt_ = 0;
  is_initialized_ = false;
  cwd_[0] = '\0';
  log_dir_[0] = '\0';
  role_mgr_ = NULL;
  replay_thread_ = NULL;

  paxos_id_ = -1; //add for [multi backup recovery]
}

ObFetchRunnable::~ObFetchRunnable()
{
}

void ObFetchRunnable::run(yysys::CThread* thread, void* arg)
{
  int ret = OB_SUCCESS;

  UNUSED(thread);
  UNUSED(arg);

  if (!is_initialized_)
  {
    YYSYS_LOG(ERROR, "ObFetchRunnable has not been initialized");
    ret = OB_NOT_INIT;
  }

  if (OB_SUCCESS == ret)
  {
    if (!_stop && param_.fetch_ckpt_)
    {
      ret = get_ckpt_();
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(ERROR, "get_ckpt_ error, ret=%d", ret);
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (!_stop && param_.fetch_log_)
    {
      ret = get_log_();
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(ERROR, "get_log_ error, ret=%d", ret);
      }
    }
  }

  if (OB_SUCCESS != ret)
  {
    if (NULL != role_mgr_) // double check
    {
      YYSYS_LOG(INFO, "role_mgr=%p", role_mgr_);
      role_mgr_->set_state(ObRoleMgr::ERROR);
    }
  }
  else
  {
    if (NULL == replay_thread_)
    {
      ret = OB_ERROR;
    }
    else
    {
      replay_thread_->set_has_no_max();
    }
  }

  YYSYS_LOG(INFO, "ObFetchRunnable finished[stop=%d ret=%d]", _stop, ret);
}

void ObFetchRunnable::clear()
{
  if (NULL != _thread)
  {
    delete[] _thread;
    _thread = NULL;
  }
}

int ObFetchRunnable::init(const ObServer& master,
                          const char* log_dir,
                          const ObFetchParam &param,
                          ObRoleMgr *role_mgr,
                          common::ObLogReplayRunnable *replay_thread,
                          int64_t paxos_id)
{
  int ret = 0;

  int log_dir_len = 0;
  if (is_initialized_)
  {
    YYSYS_LOG(ERROR, "ObFetchRunnable has been initialized");
    ret = OB_INIT_TWICE;
  }
  else
  {
    if (NULL == log_dir || NULL == role_mgr || NULL == replay_thread)
    {
      YYSYS_LOG(ERROR, "Parameter are invalid[log_dir=%p role_mgr=%p replay_thread=%p]",
                log_dir, role_mgr, replay_thread);
      ret = OB_INVALID_ARGUMENT;
    }
    else
    {
      log_dir_len = static_cast<int32_t>(strlen(log_dir));
      if (log_dir_len >= OB_MAX_FILE_NAME_LENGTH)
      {
        YYSYS_LOG(ERROR, "Parameter is invalid[log_dir_len=%d log_dir=%s]", log_dir_len, log_dir);
        ret = OB_INVALID_ARGUMENT;
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    usr_opt_ = (char*)ob_malloc(OB_MAX_FETCH_CMD_LENGTH, ObModIds::OB_FETCH_RUNABLE);
    if (NULL == usr_opt_)
    {
      YYSYS_LOG(ERROR, "ob_malloc for usr_opt_ error, size=%ld", OB_MAX_FETCH_CMD_LENGTH);
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    else
    {
      ret = set_usr_opt(DEFAULT_FETCH_OPTION);
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(ERROR, "set default user option error, DEFAULT_FETCH_OPTION=%s ret=%d", DEFAULT_FETCH_OPTION, ret);
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    strncpy(log_dir_, log_dir, log_dir_len);
    log_dir_[log_dir_len] = '\0';
    master_ = master;
    param_ = param;
    role_mgr_ = role_mgr;
    replay_thread_ = replay_thread;

    is_initialized_ = true;
    paxos_id_ = paxos_id; //add for [multi backup recovery]
  }

  return ret;
}

int ObFetchRunnable::set_fetch_param(const ObFetchParam& param)
{
  int ret = OB_SUCCESS;

  param_ = param;

  return ret;
}

int ObFetchRunnable::add_ckpt_ext(const char* ext)
{
  int ret = OB_SUCCESS;

  if (NULL == ext)
  {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(ERROR, "Paramter is invalid[ext=NULL]");
  }
  else
  {
    int ext_len = static_cast<int32_t>(strlen(ext));
    ObString new_ext(0, ext_len + 1, const_cast<char*>(ext));
    for (CkptIter i = ckpt_ext_.begin(); i != ckpt_ext_.end(); i++)
    {
      if (new_ext == *i)
      {
        ret = OB_ENTRY_EXIST;
        YYSYS_LOG(WARN, "\"%s\" exists", ext);
        break;
      }
    }

    if (OB_SUCCESS == ret)
    {
      ObString new_entry;
      ret = ckpt_buf_.write_string(new_ext, &new_entry);
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(ERROR, "ObStringBuf write_string error[ret=%d]", ret);
      }
      else
      {
        ret = ckpt_ext_.push_back(new_entry);
        if (OB_SUCCESS != ret)
        {
          YYSYS_LOG(ERROR, "ObVector push_back error[ret=%d]", ret);
        }
      }
    }
  }

  return ret;
}

int ObFetchRunnable::got_ckpt(uint64_t ckpt_id)
{
  UNUSED(ckpt_id);
  return OB_SUCCESS;
}

int ObFetchRunnable::got_log(uint64_t log_id)
{
  int ret = OB_SUCCESS;

  if (NULL == replay_thread_)
  {
    ret = OB_ERROR;
  }
  else
  {
    replay_thread_->set_max_log_file_id(log_id);
  }

  return ret;
}

int ObFetchRunnable::set_usr_opt(const char* opt)
{
  int ret = OB_SUCCESS;

  int opt_len = static_cast<int32_t>(strlen(opt));
  if (opt_len >= OB_MAX_FETCH_CMD_LENGTH)
  {
    YYSYS_LOG(WARN, "usr_option is too long, opt_len=%d maximum_length=%ld", opt_len, OB_MAX_FETCH_CMD_LENGTH);
    ret = OB_BUF_NOT_ENOUGH;
  }
  else
  {
    strncpy(usr_opt_, opt, opt_len);
    usr_opt_[opt_len] = '\0';
  }

  return ret;
}

int ObFetchRunnable::gen_fetch_cmd_(const uint64_t id, const char* fn_ext, char* cmd, const int64_t size)
{
  int ret = OB_SUCCESS;

  int err = 0;
  const int MAX_SERVER_ADDR_SIZE = 128;
  char master_addr[MAX_SERVER_ADDR_SIZE];
  const int MAX_FETCH_OPTION_SIZE = 128;
  char fetch_option[MAX_FETCH_OPTION_SIZE];
  char full_log_dir[OB_MAX_FILE_NAME_LENGTH];
  char log_dir[OB_MAX_FILE_NAME_LENGTH];//add for [multi backup recovery]

  if (NULL == cmd || size <= 0)
  {
    YYSYS_LOG(ERROR, "Parameters are invalid[id=%lu fn_ext=%s cmd=%p size=%ld", id, fn_ext, cmd, size);
    ret = OB_INVALID_ARGUMENT;
  }

  if (OB_SUCCESS == ret)
  {
    if ('\0' == cwd_[0])
    {
      if (NULL == getcwd(cwd_, OB_MAX_FILE_NAME_LENGTH))
      {
        YYSYS_LOG(ERROR, "getcwd error[%s]", strerror(errno));
        ret = OB_ERROR;
      }
    }
  }

  // gen log directory
  if (OB_SUCCESS == ret)
  {
    if (log_dir_[0] == '/')
    {
      //modify for [multi backup recovery]-b
      //err = snprintf(full_log_dir, OB_MAX_FILE_NAME_LENGTH, "%s", log_dir_);
      err = snprintf(full_log_dir, OB_MAX_FILE_NAME_LENGTH, "%s/%s", "data", "ts_commitlog");
      //mod linhx [desi] 20211209:"ups_commitlog"->ts_commitlog
    }
    else
    {
      //err = snprintf(full_log_dir, OB_MAX_FILE_NAME_LENGTH, "%s/%s", cwd_,  log_dir_);
      err = snprintf(full_log_dir, OB_MAX_FILE_NAME_LENGTH, "%s/%s/%s", cwd_, "data", "ts_commitlog");
      //mod linhx [desi] 20211209:"ups_commitlog"->ts_commitlog
      //modify for [multi backup recovery]-e
    }

    if (err < 0)
    {
      YYSYS_LOG(ERROR, "snprintf full_log_dir[OB_MAX_FILE_NAME_LENGTH=%ld err=%d] error[%s]",
                OB_MAX_FILE_NAME_LENGTH, err, strerror(errno));
      ret = OB_ERROR;
    }
    else if (err >= OB_MAX_FILE_NAME_LENGTH)
    {
      YYSYS_LOG(ERROR, "full_log_dir buffer is not enough[OB_MAX_FILE_NAME_LENGTH=%ld err=%d]", OB_MAX_FILE_NAME_LENGTH, err);
      ret = OB_ERROR;
    }
  }

  // get master address and generate fetch option
  if (OB_SUCCESS == ret)
  {
    if (!master_.ip_to_string(master_addr, MAX_SERVER_ADDR_SIZE))
    {
      YYSYS_LOG(ERROR, "ObServer to_string error[master_=%p]", &master_);
      ret = OB_ERROR;
    }
    else
    {
      const char* FETCH_OPTION_FORMAT = "%s --bwlimit=%ld";
      err = snprintf(fetch_option, MAX_FETCH_OPTION_SIZE, FETCH_OPTION_FORMAT, usr_opt_, limit_rate_);
      if (err < 0)
      {
        YYSYS_LOG(ERROR, "snprintf fetch_option[MAX_FETCH_OPTION_SIZE=%d] error[%s]", MAX_FETCH_OPTION_SIZE, strerror(errno));
        ret = OB_ERROR;
      }
      else if (err >= MAX_FETCH_OPTION_SIZE)
      {
        YYSYS_LOG(ERROR, "fetch_option buffer size is not enough[MAX_FETCH_OPTION_SIZE=%d real=%d]", MAX_FETCH_OPTION_SIZE, err);
        ret = OB_ERROR;
      }
    }
  }
  //add for [multi backup recovery]-b
  if (OB_SUCCESS == ret)
  {
    if(log_dir_[0] == '/')
    {
      err = snprintf(log_dir, OB_MAX_FILE_NAME_LENGTH, "/%s/%s%ld/%s", log_dir_, "paxos", paxos_id_, "ts_commitlog");
      //mod linhx [desi] 20211209:"ups_commitlog"->ts_commitlog
    }
    else
    {
      err = snprintf(log_dir, OB_MAX_FILE_NAME_LENGTH, "%s/%s/%s%ld/%s", cwd_, log_dir_, "paxos", paxos_id_, "ts_commitlog");
      //mod linhx [desi] 20211209:"ups_commitlog"->ts_commitlog
    }

    if (err < 0)
    {
      YYSYS_LOG(ERROR, "snprintf full_log_dir[OB_MAX_FILE_NAME_LENGTH=%ld err=%d] error[%s]",
                OB_MAX_FILE_NAME_LENGTH, err, strerror(errno));
      ret = OB_ERROR;
    }
    else if (err >= OB_MAX_FILE_NAME_LENGTH)
    {
      YYSYS_LOG(ERROR, "full_log_dir buffer is not enough[OB_MAX_FILE_NAME_LENGTH=%ld err=%d]", OB_MAX_FILE_NAME_LENGTH, err);
      ret = OB_ERROR;
    }
  }
  //add for [multi backup recovery]-e

  // generate fetch command
  if (OB_SUCCESS == ret)
  {
    if (NULL == fn_ext || 0 == strlen(fn_ext))
    {
      const char* FETCH_CMD_WITHOUTEXT_FORMAT = "rsync %s %s:%s/%lu %s/";
      //err = snprintf(cmd, size, FETCH_CMD_WITHOUTEXT_FORMAT, fetch_option, master_addr, full_log_dir, id, log_dir_);
      err = snprintf(cmd, size, FETCH_CMD_WITHOUTEXT_FORMAT, fetch_option, master_addr, full_log_dir, id, log_dir);
    }
    else
    {
      const char* FETCH_CMD_WITHEXT_FORMAT = "rsync %s %s:%s/%lu.%s %s/";
      //      err = snprintf(cmd, size, FETCH_CMD_WITHEXT_FORMAT, fetch_option, master_addr, full_log_dir, id, fn_ext, log_dir_);
      err = snprintf(cmd, size, FETCH_CMD_WITHEXT_FORMAT, fetch_option, master_addr, full_log_dir, id, fn_ext, log_dir);
    }

    if (err < 0)
    {
      YYSYS_LOG(ERROR, "snprintf cmd[size=%ld err=%d] error[%s]", size, err, strerror(errno));
      ret = OB_ERROR;
    }
    else if (err >= size)
    {
      YYSYS_LOG(ERROR, "cmd buffer is not enough[size=%ld err=%d]", size, err);
      ret = OB_ERROR;
    }
  }

  return ret;
}

bool ObFetchRunnable::exists_(const uint64_t id, const char* fn_ext) const
{
  bool res = false;

  char fn[OB_MAX_FILE_NAME_LENGTH];
  int ret = gen_full_name_(id, fn_ext, fn, OB_MAX_FILE_NAME_LENGTH);
  if (OB_SUCCESS != ret)
  {
    YYSYS_LOG(WARN, "gen_full_name_ error[ret=%d id=%lu fn_ext=%s]", ret, id, fn_ext);
  }
  else
  {
    res = FileDirectoryUtils::exists(fn);
  }

  return res;
}

bool ObFetchRunnable::remove_(const uint64_t id, const char* fn_ext) const
{
  bool res = false;

  char fn[OB_MAX_FILE_NAME_LENGTH];
  int ret = gen_full_name_(id, fn_ext, fn, OB_MAX_FILE_NAME_LENGTH);
  if (OB_SUCCESS != ret)
  {
    YYSYS_LOG(WARN, "gen_full_name_ error[ret=%d id=%lu fn_ext=%s]", ret, id, fn_ext);
  }
  else
  {
    res = FileDirectoryUtils::delete_file(fn);
  }

  return res;
}

int ObFetchRunnable::gen_full_name_(const uint64_t id, const char* fn_ext, char *buf, const int buf_len) const
{
  int ret = OB_SUCCESS;

  int fn_ext_len = 0;
  int err = 0;

  if (NULL == buf || buf_len <= 0)
  {
    YYSYS_LOG(ERROR, "Parameters are invalid[buf=%p buf_len=%d]", buf, buf_len);
    ret = OB_ERROR;
  }
  else
  {
    if (NULL != fn_ext)
    {
      fn_ext_len = static_cast<int32_t>(strlen(fn_ext));
    }

    if (NULL == fn_ext || 0 == fn_ext_len)
    {
      const char* FN_WITHOUTEXT_FORMAT = "%s/%lu";
      err = snprintf(buf, buf_len, FN_WITHOUTEXT_FORMAT, log_dir_, id);
    }
    else
    {
      const char* FN_WITHEXT_FORMAT = "%s/%lu.%s";
      err = snprintf(buf, buf_len, FN_WITHEXT_FORMAT, log_dir_, id, fn_ext);
    }

    if (err < 0)
    {
      YYSYS_LOG(ERROR, "snprintf error[err=%d]", err);
      ret = OB_ERROR;
    }
    else if (err >= buf_len)
    {
      YYSYS_LOG(ERROR, "buf is not enough[err=%d buf_len=%d]", err, buf_len);
      ret = OB_BUF_NOT_ENOUGH;
    }
  }

  return ret;
}

int ObFetchRunnable::get_log_()
{
  int ret = OB_SUCCESS;

  int err = 0;
  char *cmd = NULL;
  
  if (!is_initialized_)
  {
    YYSYS_LOG(ERROR, "ObFetchRunnable has not been initialized");
    ret = OB_NOT_INIT;
  }

  if (OB_SUCCESS == ret)
  {
    cmd = static_cast<char*>(ob_malloc(OB_MAX_FETCH_CMD_LENGTH, ObModIds::OB_FETCH_RUNABLE));
    if (NULL == cmd)
    {
      YYSYS_LOG(WARN, "ob_malloc error, OB_MAX_FETCH_CMD_LENGTH=%ld", OB_MAX_FETCH_CMD_LENGTH);
      ret = OB_ERROR;
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (param_.fetch_log_)
    {
      YYSYS_LOG(INFO, "fetch param,min_log_id=%ld, max_log_id=%ld", param_.min_log_id_, param_.max_log_id_);
      for (uint64_t i = param_.min_log_id_; !_stop && i <= param_.max_log_id_; i++)
      {
        //YYSYS_LOG(INFO, "lc: fetch %d log", i);
        ret = gen_fetch_cmd_(i, NULL, cmd, OB_MAX_FETCH_CMD_LENGTH);
        if (OB_SUCCESS != ret)
        {
          YYSYS_LOG(ERROR, "gen_fetch_cmd_[id=%lu fn_ext=NULL] error[ret=%d]", i, ret);
          ret = OB_ERROR;
        }
        else
        {
          YYSYS_LOG(INFO, "fetch log[id=%lu]: \"%s\"", i, cmd);
          err = FSU::vsystem(cmd);
          if (0 != err)
          {
            YYSYS_LOG(ERROR, "fetch[\"%s\"] error[err=%d]", cmd, err);
            ret = OB_ERROR;
          }
          else
          {
            ret = got_log(i);
            if (OB_SUCCESS != ret)
            {
              YYSYS_LOG(ERROR, "got_log error[err=%d]", ret);
            }
          }
        }
        //add for [multi backup recovery]-b
        if (i == param_.max_log_id_)
        {
          char log_dir[OB_MAX_FILE_NAME_LENGTH];
          ret = snprintf(log_dir, OB_MAX_FILE_NAME_LENGTH, "%s/%s%ld/%s", log_dir_, "paxos", paxos_id_, "ts_commitlog");
          //mod linhx [desi] 20211209:"ups_commitlog"->ts_commitlog
          ret = snprintf(log_dir_, OB_MAX_FILE_NAME_LENGTH, "%s", log_dir);
          YYSYS_LOG(INFO, "fetch log[id=%lu]: \"%s\"", i, log_dir_);
          get_commit_point(i);
        }
        //add for [multi backup recovery]-e
      }
    }
  }

  if (NULL != cmd)
  {
    ob_free(cmd);
    cmd = NULL;
  }

  return ret;
}

int ObFetchRunnable::get_ckpt_()
{
  int ret = OB_SUCCESS;

  int err = 0;
  char *cmd = NULL;
  
  if (!is_initialized_)
  {
    YYSYS_LOG(ERROR, "ObFetchRunnable has not been initialized");
    ret = OB_NOT_INIT;
  }

  if (OB_SUCCESS == ret)
  {
    cmd = static_cast<char*>(ob_malloc(OB_MAX_FETCH_CMD_LENGTH, ObModIds::OB_FETCH_RUNABLE));
    if (NULL == cmd)
    {
      YYSYS_LOG(WARN, "ob_malloc error, OB_MAX_FETCH_CMD_LENGTH=%ld", OB_MAX_FETCH_CMD_LENGTH);
      ret = OB_ERROR;
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (param_.fetch_ckpt_)
    {
      ret = gen_fetch_cmd_(param_.ckpt_id_, DEFAULT_CKPT_EXTENSION, cmd, OB_MAX_FETCH_CMD_LENGTH);
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(ERROR, "gen_fetch_cmd_[id=%lu fn_ext=%s] error[ret=%d]", param_.ckpt_id_, DEFAULT_CKPT_EXTENSION, ret);
        ret = OB_ERROR;
      }
      else
      {
        if (exists_(param_.ckpt_id_, DEFAULT_CKPT_EXTENSION))
        {
          if (remove_(param_.ckpt_id_, DEFAULT_CKPT_EXTENSION))
          {
            YYSYS_LOG(INFO, "remove checkpoint file[id=%lu ext=%s]", param_.ckpt_id_, DEFAULT_CKPT_EXTENSION);
          }
          else
          {
            YYSYS_LOG(WARN, "remove checkpoint file error[id=%lu ext=%s]", param_.ckpt_id_, DEFAULT_CKPT_EXTENSION);
          }
        }

        YYSYS_LOG(INFO, "fetch checkpoint[id=%lu ext=\"checkpoint\"]: \"%s\"", param_.ckpt_id_, cmd);
        for (int i = 0; i < DEFAULT_FETCH_RETRY_TIMES; i++)
        {
          err = FSU::vsystem(cmd);
          if (0 == err)
          {
            break;
          }
        }
        if (0 != err)
        {
          YYSYS_LOG(ERROR, "fetch[\"%s\"] error[err=%d]", cmd, err);
          ret = OB_ERROR;
        }
      }

      for (CkptIter i = ckpt_ext_.begin(); !_stop && i != ckpt_ext_.end(); i++)
      {
        ret = gen_fetch_cmd_(param_.ckpt_id_, i->ptr(), cmd, OB_MAX_FETCH_CMD_LENGTH);
        if (OB_SUCCESS != ret)
        {
          YYSYS_LOG(ERROR, "gen_fetch_cmd_[id=%lu fn_ext=%s] error[ret=%d]", param_.ckpt_id_, i->ptr(), ret);
          ret = OB_ERROR;
        }
        else
        {
          if (exists_(param_.ckpt_id_, i->ptr()))
          {
            if (remove_(param_.ckpt_id_, i->ptr()))
            {
              YYSYS_LOG(INFO, "remove checkpoint file[id=%lu ext=%s]", param_.ckpt_id_, i->ptr());
            }
            else
            {
              YYSYS_LOG(WARN, "remove checkpoint file error[id=%lu ext=%s]", param_.ckpt_id_, i->ptr());
            }
          }

          YYSYS_LOG(INFO, "fetch checkpoint[id=%lu ext=\"%s\"]: \"%s\"", param_.ckpt_id_, i->ptr(), cmd);
          for (int i = 0; i < DEFAULT_FETCH_RETRY_TIMES; i++)
          {
            err = FSU::vsystem(cmd);
            if (0 == err)
            {
              break;
            }
          }
          if (0 != err)
          {
            YYSYS_LOG(ERROR, "fetch[\"%s\"] error[err=%d]", cmd, err);
            ret = OB_ERROR;
          }
        }
      }

      if (OB_SUCCESS == ret)
      {
        ret = got_ckpt(param_.ckpt_id_);
        if (OB_SUCCESS != ret)
        {
          YYSYS_LOG(ERROR, "got_ckpt error[err=%d]", ret);
        }
      }
    }
  }

  if (NULL != cmd)
  {
    ob_free(cmd);
    cmd = NULL;
  }

  return ret;
}

//add for [multi backup recovery]-b
int ObFetchRunnable::get_commit_point(uint64_t max_log_file_id)
{
  int err = OB_SUCCESS;
  ObRepeatedLogReader log_reader;
  uint64_t log_seq = 0;
  char *log_data = NULL;
  int64_t data_len = 0;
  LogCommand cmd;

  if (NULL == log_dir_ || 0 >= max_log_file_id)
  {
    err = OB_INVALID_ARGUMENT;
  }
  else if (OB_SUCCESS != (err = log_reader.init(log_dir_)))
  {
    YYSYS_LOG(ERROR, "log_reader.init(log_dir=%s)=>%d", log_dir_, err);
  }
  else if (OB_SUCCESS != (err = log_reader.open(max_log_file_id)))
  {
    if (OB_FILE_NOT_EXIST != err)
    {
      YYSYS_LOG(ERROR, "log_reader.open(file_id=%lu)=>%d", max_log_file_id, err);
    }
    else
    {
      err = OB_ENTRY_NOT_EXIST;
    }
  }
  while (err == OB_SUCCESS)
  {
    if (OB_SUCCESS != (err = log_reader.read_log(cmd, log_seq, log_data, data_len)))
    {
      if (OB_READ_NOTHING != err)
      {
        YYSYS_LOG(ERROR, "log_reader.read_log()=>%d", err);
      }
      else
      {
        err = OB_SUCCESS;
        break;
      }
    }
  }
  if (log_reader.is_opened())
  {
    log_reader.close();
  }
  if (err == OB_SUCCESS)
  {
    const int UINT64_MAX_LEN = 20;
    int len = 0;
    int open_ret = 0;
    FileUtils file_file;
    char file_fn[OB_MAX_FILE_NAME_LENGTH];
    char file_str[UINT64_MAX_LEN];
    int64_t file_str_len = 0;

    if ((len = snprintf(file_fn, sizeof(file_fn), "%s/%s", log_dir_, "commit_point") < 0)
        && len >= (int64_t)sizeof(file_fn))
    {
      err = OB_BUF_NOT_ENOUGH;
      YYSYS_LOG(ERROR, "generate_data_fn()=>%d", err);
    }
    else if (0 > (open_ret = file_file.open(file_fn, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH)))
    {
      err = OB_FILE_NOT_EXIST;
      YYSYS_LOG(ERROR, "open file[\"%s\"] error[%s]", file_fn, strerror(errno));
    }
    else if ((file_str_len = snprintf(file_str, sizeof(file_str), "%lu", log_seq)) < 0
             || file_str_len >= (int64_t)sizeof(file_str))
    {
      err = OB_BUF_NOT_ENOUGH;
      YYSYS_LOG(ERROR, "snprintf file_str error[%s][data=%lu]", strerror(errno), log_seq);
    }
    else if (0 > (file_str_len = file_file.write(file_str, file_str_len)))
    {
      err = OB_ERR_SYS;
      YYSYS_LOG(ERROR, "write error[%s][file_str=%p file_str_len=%ld]", strerror(errno), file_str, file_str_len);
    }
    if (0 > open_ret)
    {
      file_file.close();
    }
  }
  return err;
}
//add for [multi backup recovery]-e
