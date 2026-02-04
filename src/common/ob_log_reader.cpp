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

#include "ob_log_reader.h"

using namespace oceanbase::common;

ObLogReader::ObLogReader()
{
  is_initialized_ = false;
  cur_log_file_id_ = 0;
  is_wait_ = false;
  log_file_reader_ = NULL;
}

ObLogReader::~ObLogReader()
{
}

int ObLogReader::init(ObSingleLogReader *reader, const char* log_dir,
                      const uint64_t log_file_id_start, const uint64_t log_seq,bool is_wait)
{
  int ret = OB_SUCCESS;

  if (is_initialized_)
  {
    YYSYS_LOG(ERROR, "ObLogReader has been initialized before");
    ret = OB_INIT_TWICE;
  }
  else
  {
    if (NULL == reader || NULL == log_dir)
    {
      YYSYS_LOG(ERROR, "Parameter is invalid[reader=%p log_dir=%p]",
                reader, log_dir);
      ret = OB_INVALID_ARGUMENT;
    }
    else
    {
      ret = reader->init(log_dir);
      if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(ERROR, "reader init[log_dir=%s] error[ret=%d]",
                  log_dir, ret);
      }
      else
      {
        log_file_reader_ = reader;
        cur_log_file_id_ = log_file_id_start;
        cur_log_seq_id_ = log_seq;
        max_log_file_id_ = 0;
        is_wait_ = is_wait;
        has_max_ = false;
        is_initialized_ = true;
      }
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (0 != log_seq)
    {
      if (OB_SUCCESS != (ret = seek(log_seq)))
      {
        YYSYS_LOG(ERROR, "seek log seq error, log_seq=%lu, ret=%d", log_seq, ret);
      }
    }
  }

  if (OB_SUCCESS != ret)
  {
    is_initialized_ = false;
  }

  return ret;
}

int ObLogReader::read_log(LogCommand &cmd, uint64_t &seq, char* &log_data, int64_t &data_len)
{
  int ret = OB_SUCCESS;
  int open_err = OB_SUCCESS;

  if (!is_initialized_)
  {
    YYSYS_LOG(ERROR, "ObLogReader has not been initialized");
    ret = OB_NOT_INIT;
  }
  else if (NULL == log_file_reader_)
  {
    YYSYS_LOG(ERROR, "log_file_reader_ is NULL, this should not be reached");
    ret = OB_ERROR;
  }
  else
  {
    if (!log_file_reader_->is_opened())
    {
      ret = open_log_(cur_log_file_id_);
    }
    if (OB_SUCCESS == ret)
    {
      ret = read_log_(cmd, seq, log_data, data_len);
      if (OB_SUCCESS == ret)
      {
        cur_log_seq_id_ = seq;
      }
      if (OB_SUCCESS == ret && OB_LOG_SWITCH_LOG == cmd)
      {
        YYSYS_LOG(INFO, "reach the end of log[cur_log_file_id_=%lu]", cur_log_file_id_);
        // ���ܴ򿪳ɹ���ʧ��: cur_log_file_id++, log_file_reader_->pos�ᱻ����,
        if (OB_SUCCESS != (ret = log_file_reader_->close()))
        {
          YYSYS_LOG(ERROR, "log_file_reader_ close error[ret=%d]", ret);
        }
        else if (OB_SUCCESS != (open_err = open_log_(++cur_log_file_id_, seq))
                 && OB_READ_NOTHING != open_err)
        {
          YYSYS_LOG(WARN, "open_log(file_id=%ld, seq=%ld)=>%d", cur_log_file_id_, seq, open_err);
        }
      }
    }
  }

  return ret;
}

int ObLogReader::reset_file_id(const uint64_t log_id_start, const uint64_t log_seq_start)
{
  int ret = OB_SUCCESS;
  if (log_file_reader_->is_opened())
  {
    ret = log_file_reader_->close();
    YYSYS_LOG(INFO, "reset log file to %ld, seq =%ld", log_id_start, log_seq_start);
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(ERROR, "log file reader close error. ret =%d", ret);
    }
  }
  if (OB_SUCCESS == ret)
  {
    ret = open_log_(log_id_start);
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(ERROR, "fail to open log file. cur_log_file_id=%ld, ret=%d", cur_log_file_id_, ret);
    }
    else
    {
      cur_log_file_id_ = log_id_start;
      YYSYS_LOG(INFO, "lc: cur_log_file_id_ =%ld", cur_log_file_id_);
    }
  }
  if (OB_SUCCESS == ret)
  {
    if (0 != log_seq_start)
    {
      ret = seek(log_seq_start);
    }
    if (OB_SUCCESS != ret)
    {
      YYSYS_LOG(ERROR, "fail to seek seq. ret =%d", ret);
    }
  }
  return ret;
}

int ObLogReader::seek(uint64_t log_seq)
{
  int ret = OB_SUCCESS;

  if (OB_SUCCESS == ret)
  {
    LogCommand cmd;
    uint64_t seq = 0;
    char* log_data;
    int64_t data_len;

    if (0 == log_seq)
    {
      ret = OB_SUCCESS;
    }
    else
    {
      ret = read_log(cmd, seq, log_data, data_len);
      if (OB_READ_NOTHING == ret)
      {
        ret = OB_SUCCESS;
      }
      else if (OB_SUCCESS != ret)
      {
        YYSYS_LOG(WARN, "seek failed, log_seq=%lu", log_seq);
      }
      else if (seq - 1 == log_seq) // log_seq to seek is in the previous log file
      {
        log_file_reader_->close();
        open_log_(cur_log_file_id_);
      }
      else if (seq >= log_seq)
      {
        YYSYS_LOG(WARN, "seek failed, the initial seq is bigger than log_seq, "
                  "seq=%lu log_seq=%lu", seq, log_seq);
        ret = OB_ERROR;
      }
      else
      {
        while(seq < log_seq)
        {
          ret = read_log(cmd, seq, log_data, data_len);
          if (OB_SUCCESS != ret)
          {
            YYSYS_LOG(ERROR, "read_log failed, seq =%ld, ret=%d", seq, ret);
            break;
          }
        }
      }
    }
  }
  return ret;
}

int ObLogReader::open_log_(const uint64_t log_file_id, const uint64_t last_log_seq/* = 0*/)
{
  int ret = OB_SUCCESS;

  if (is_wait_ && has_max_ && log_file_id > max_log_file_id_)
  {
    ret = OB_READ_NOTHING;
  }
  else
  {
    if (NULL == log_file_reader_)
    {
      YYSYS_LOG(ERROR, "log_file_reader_ is NULL, this should not be reached");
      ret = OB_ERROR;
    }
    else
    {
      ret = log_file_reader_->open(log_file_id, last_log_seq);
      if (is_wait_)
      {
        if (OB_FILE_NOT_EXIST == ret)
        {
          YYSYS_LOG(DEBUG, "log file doesnot exist, id=%lu", log_file_id);
          ret = OB_READ_NOTHING;
        }
        else if (ret != OB_SUCCESS)
        {
          YYSYS_LOG(WARN, "log_file_reader_ open[id=%lu] error[ret=%d]", cur_log_file_id_, ret);
        }
      }
      else
      {
        if (ret != OB_SUCCESS)
        {
          YYSYS_LOG(WARN, "log_file_reader_ open[id=%lu] error[ret=%d]", cur_log_file_id_, ret);
        }
      }
    }
  }

  return ret;
}

int ObLogReader::read_log_(LogCommand &cmd, uint64_t &log_seq, char *&log_data, int64_t &data_len)
{
  int ret = OB_SUCCESS;

  if (NULL == log_file_reader_)
  {
    YYSYS_LOG(ERROR, "log_file_reader_ is NULL, this should not be reached");
    ret = OB_ERROR;
  }
  else
  {
    int err = log_file_reader_->read_log(cmd, log_seq, log_data, data_len);
    //while (OB_READ_NOTHING == err && is_wait_)
    //{
    //  usleep(WAIT_TIME);
    //  err = log_file_reader_->read_log(cmd, log_seq, log_data, data_len);
    //}
    ret = err;
    if (OB_SUCCESS != ret && OB_READ_NOTHING != ret)
    {
      YYSYS_LOG(WARN, "log_file_reader_ read_log error[ret=%d]", ret);
    }
  }

  return ret;
}
