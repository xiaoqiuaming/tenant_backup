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

#ifndef OCEANBASE_COMMON_OB_SINGLE_LOG_READER_H_
#define OCEANBASE_COMMON_OB_SINGLE_LOG_READER_H_

#include "yylog.h"

#include "ob_define.h"
#include "data_buffer.h"
#include "ob_malloc.h"
#include "ob_log_entry.h"
#include "ob_file.h"

namespace oceanbase
{
  namespace common
  {
    const int64_t DEFAULT_LOG_SIZE = 2 * 1024 * 1024;
    class ObSingleLogReader
    {
    public:
      static const int64_t LOG_BUFFER_MAX_LENGTH;
    public:
      ObSingleLogReader();
      virtual ~ObSingleLogReader();

      /**
       * @brief ObSingleLogReader��ʼ��
       * ObSingleLogReader����Ҫ�ȵ���init�������г�ʼ����, �ſ��Խ���open��read_log����
       * ��ʼ��ʱ, �����LOG_BUFFER_MAX_LENGTH���ȵĶ�������
       * �������������ͷŶ�������
       * @param [in] log_dir ��־�ļ���
       * @return OB_SUCCESS ��ʼ���ɹ�
       * @return OB_INIT_TWICE �Ѿ���ʼ��
       * @return OB_ERROR ��ʼ��ʧ��
       */
      int init(const char* log_dir);

      /**
       * @brief ��һ���ļ�
       * open���������־�ļ�
       * ����close�����ر���־�ļ���, �����ٴε���open������������־�ļ�, ����������
       * @param [in] file_id ��ȡ�Ĳ�����־�ļ�id
       * @param [in] last_log_seq ��һ����־���, �����ж���־�Ƿ�����, Ĭ��ֵ0��ʾ��Ч
       */
      int open(const uint64_t file_id, const uint64_t last_log_seq = 0);

      /**
       * @brief �ر���־�ļ�
       * �ر��Ѿ��򿪵���־�ļ�, ֮������ٴε���init����, ��ȡ������־�ļ�
       */
      int close();

      /**
       * @brief �����ڲ�״̬, �ͷŻ������ڴ�
       */
      int reset();

      /**
       * @brief �Ӳ�����־�ж�ȡһ�����²���
       * @param [out] cmd ��־����
       * @param [out] log_seq ��־���
       * @param [out] log_data ��־����
       * @param [out] data_len ����������
       * @return OB_SUCCESS: ����ɹ�;
       *         OB_READ_NOTHING: ���ļ���û�ж�������
       *         others: �����˴���.
       */
      virtual int read_log(LogCommand &cmd, uint64_t &log_seq, char *&log_data, int64_t &data_len) = 0;
      inline uint64_t get_cur_log_file_id()
      {
        return file_id_;
      }
      inline uint64_t get_last_log_seq_id()
      {
        return last_log_seq_;
      }
      inline uint64_t get_last_log_offset()
      {
        return pos;
      }

      /// @brief is log file opened
      inline bool is_opened() const
      { return file_.is_opened(); }

      ///@brief ��ʼ��ʱ����ȡ��ǰĿ¼�µ������־�ļ���
      int get_max_log_file_id(uint64_t &max_log_file_id);
        int64_t get_cur_offset() const;

      inline void unset_dio()
      { dio_ = false; };

      protected:
        int read_header(ObLogEntry& entry);
        int trim_last_zero_padding(int64_t header_size);
        int open_with_lucky(const uint64_t file_id, const uint64_t last_log_seq);
    protected:
      /**
       * ����־�ļ��ж�ȡ���ݵ���������
       * @return OB_SUCCESS: ����ɹ�;
       *         OB_READ_NOTHING: ���ļ���û�ж�������
       *         others: �����˴���.
       */
      int read_log_();

      inline int check_inner_stat_()
      {
        int ret = OB_SUCCESS;
        if (!is_initialized_)
        {
          YYSYS_LOG(ERROR, "ObSingleLogReader has not been initialized");
          ret = OB_NOT_INIT;
        }
        return ret;
      }

    protected:
      uint64_t file_id_;  //��־�ļ�id
      uint64_t last_log_seq_;  //��һ����־(Mutator)���
      ObDataBuffer log_buffer_;  //��������
      char file_name_[OB_MAX_FILE_NAME_LENGTH];  //��־�ļ���
      char log_dir_[OB_MAX_FILE_NAME_LENGTH];  //��־Ŀ¼
      int64_t pos;
      int64_t pread_pos_;
      ObFileReader file_;
      bool is_initialized_;  //��ʼ�����
      bool dio_;
    };
  } // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_OB_SINGLE_LOG_READER_H_
