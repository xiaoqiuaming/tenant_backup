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

#ifndef OCEANBASE_COMMON_OB_LOG_WRITER_H_
#define OCEANBASE_COMMON_OB_LOG_WRITER_H_

#include "yylog.h"

#include "ob_define.h"
#include "data_buffer.h"
#include "ob_slave_mgr.h"
#include "ob_log_entry.h"
#include "ob_file.h"
#include "ob_log_cursor.h"
#include "ob_log_generator.h"
#include "ob_log_data_writer.h"

namespace oceanbase
{
  namespace common
  {
    class ObILogWriter
    {
      public:
        virtual ~ObILogWriter()
        {};
      public:
        virtual int switch_log_file(uint64_t &new_log_file_id) = 0;
        virtual int write_replay_point(uint64_t replay_point) = 0;
    };

    class ObLogWriter : public ObILogWriter
    {
      public:
        static const int64_t LOG_BUFFER_SIZE = OB_MAX_LOG_BUFFER_SIZE;
        static const int64_t LOG_FILE_ALIGN_BIT = 9;
        static const int64_t LOG_FILE_ALIGN_SIZE = 1 << LOG_FILE_ALIGN_BIT;
        static const int64_t LOG_FILE_ALIGN_MASK = LOG_FILE_ALIGN_SIZE - 1;
        static const int64_t DEFAULT_DU_PERCENT = 60;

      public:
        ObLogWriter();
        virtual ~ObLogWriter();

        /// ��ʼ��
        /// @param [in] log_dir ��־����Ŀ¼
        /// @param [in] log_file_max_size ��־�ļ���󳤶�����
        /// @param [in] slave_mgr ObSlaveMgr����ͬ����־
        int init(const char* log_dir, const int64_t log_file_max_size, ObSlaveMgr *slave_mgr,
                 int64_t log_sync_type, MinAvailFileIdGetter* min_avail_fid_getter = NULL, const ObServer* server=NULL);

        int reset_log();
        //add wangdonghui [ups_replication] 20170220 :b
        int clear();
        //add :e
        virtual int write_log_hook(const bool is_master,
                                   const ObLogCursor start_cursor, const ObLogCursor end_cursor,
                                   const char* log_data, const int64_t data_len)
        {
          UNUSED(is_master);
          UNUSED(start_cursor);
          UNUSED(end_cursor);
          UNUSED(log_data);
          UNUSED(data_len);
          return OB_SUCCESS;
        }
        bool check_log_size(const int64_t size) const
        { return log_generator_.check_log_size(size); }
        //add shili [LONG_TRANSACTION_LOG]  20160926:b
        bool check_mutator_size(const int64_t size, const int64_t max_mutator_size) const
        {
          return log_generator_.check_log_size(size, max_mutator_size);
        }
        bool is_normal_mutator_size(const int64_t size) const
        { return log_generator_.is_normal_mutator_size(size); }
        //add e
        int start_log(const ObLogCursor& start_cursor);
        int start_log_maybe(const ObLogCursor& start_cursor);
        int get_flushed_cursor(ObLogCursor& log_cursor) const;
        int get_filled_cursor(ObLogCursor& log_cursor) const;
        bool is_all_flushed() const;
        int64_t get_file_size() const
        { return log_writer_.get_file_size(); };
        int64_t to_string(char* buf, const int64_t len) const;

        /// @brief д��־
        /// write_log��������־�����Լ��Ļ�����, ��������СLOG_BUFFER_SIZE����
        /// ���ȼ����־��С�Ƿ���������һ����������, ������������־
        /// Ȼ����־����д�뻺����
        /// @param [in] log_data ��־����
        /// @param [in] data_len ����
        /// @retval OB_SUCCESS �ɹ�
        /// @retval OB_BUF_NOT_ENOUGH �ڲ�����������
        /// @retval otherwise ʧ��
        int write_log(const LogCommand cmd, const char* log_data, const int64_t data_len);

        template<typename T>
        int write_log(const LogCommand cmd, const T& data);
        //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
        /**
       * @brief write prepare log
       * @param cmd
       * @param data trans mutator
       * @param coo_trans_id coordinator trans id
       * @return
       */
        template<typename T>
        int write_log(const LogCommand cmd, const T& data, const common::ObTransID& coo_trans_id);
        //add 20150701:e

        int write_keep_alive_log();

        int async_flush_log(int64_t& end_log_id, TraceLog::LogBuffer &tlog_buffer = oceanbase::common::TraceLog::get_logbuffer());
        int64_t get_flushed_clog_id();
        /// @brief ���������е���־д�����
        /// flush_log���ཫ�������е�����ͬ����Slave����
        /// Ȼ��д�����
        /// @retval OB_SUCCESS �ɹ�
        /// @retval otherwise ʧ��
        int flush_log(TraceLog::LogBuffer &tlog_buffer = oceanbase::common::TraceLog::get_logbuffer(),
                      const bool sync_to_slave = true, const bool is_master = true);

        /// @brief д��־����д��
        /// ���л���־����д��
        /// �ڲ�������ԭ����������, �ᱻ���
        int write_and_flush_log(const LogCommand cmd, const char* log_data, const int64_t data_len);

        //add shili [LONG_TRANSACTION_LOG]  20160926:b
        /// @brief  log buffer �Ƿ�Ϊ��
        bool is_buffer_clear()
        {
          return log_generator_.is_clear();
        }
        //add e

        /// @brief ������������д����־�ļ�
        /// Ȼ�󽫻���������ˢ�����
        /// @retval OB_SUCCESS �ɹ�
        /// @retval otherwise ʧ��
        int store_log(const char* buf, const int64_t buf_len, const bool sync_to_slave=false);

        void set_disk_warn_threshold_us(const int64_t warn_us);
        void set_net_warn_threshold_us(const int64_t warn_us);
        /// @brief Master�л���־�ļ�
        /// ����һ���л���־�ļ���commit log
        /// ͬ����Slave�������ȴ�����
        /// �رյ�ǰ���ڲ�������־�ļ�, ��־���+1, ���µ���־�ļ�
        /// @retval OB_SUCCESS �ɹ�
        /// @retval otherwise ʧ��
        virtual int switch_log_file(uint64_t &new_log_file_id);
        virtual int write_replay_point(uint64_t replay_point)
        {
          UNUSED(replay_point);
          YYSYS_LOG(WARN, "not implement");
          return common::OB_NOT_IMPLEMENT;
        };

        /// @brief дһ��checkpoint��־�����ص�ǰ��־��
        /// д��checkpoint��־������־
        int write_checkpoint_log(uint64_t &log_file_id);

        inline ObSlaveMgr* get_slave_mgr()
        { return slave_mgr_; }

        inline int64_t get_last_net_elapse()
        { return last_net_elapse_; }
        inline int64_t get_last_disk_elapse()
        { return last_disk_elapse_; }
        inline int64_t get_last_flush_log_time()
        { return last_flush_log_time_; }
        //add wangjiahao [Paxos ups_replication_tmplog] 20150724 :b
        inline int64_t* get_log_term_ptr()
        {
          return log_generator_.get_log_term_ptr();
        }
        //add :e
        //add wangdonghui [ups_replication] 20170420 :b
        inline int64_t log_size()
        {
          return log_generator_.get_log_size();
        }

        //add :e

        //add for [change ups master]-b
        inline int64_t get_max_log_id()
        {
          return log_generator_.get_max_log_id();
        }
        //add for [change ups master]-e

      protected:
        inline int check_inner_stat() const
        {
          int ret = OB_SUCCESS;
          if (!is_initialized_)
          {
            YYSYS_LOG(ERROR, "ObLogWriter has not been initialized");
            ret = OB_NOT_INIT;
          }
          return ret;
        }

      protected:
        bool is_initialized_;
        ObSlaveMgr *slave_mgr_;
        ObLogGenerator log_generator_;
        ObLogDataWriter log_writer_;
        int64_t net_warn_threshold_us_;
        int64_t disk_warn_threshold_us_;
        int64_t last_net_elapse_;  //��һ��д��־����ͬ����ʱ
        int64_t last_disk_elapse_;  //��һ��д��־���̺�ʱ
        int64_t last_flush_log_time_; // �ϴ�ˢ���̵�ʱ��
    };
    template<typename T>
    int ObLogWriter::write_log(const LogCommand cmd, const T& data)
    {
      int ret = OB_SUCCESS;

      if (OB_SUCCESS != (ret = check_inner_stat()))
      {
        YYSYS_LOG(ERROR, "check_inner_stat()=>%d", ret);
      }
      else if (OB_SUCCESS != (ret = log_generator_.write_log(cmd, data))
               && OB_BUF_NOT_ENOUGH != ret)
      {
        YYSYS_LOG(WARN, "log_generator.write_log(cmd=%d, data=%p)=>%d", cmd, &data, ret);
      }
      return ret;
    }
    //add peiouya [MultiUPS] [DISTRIBUTED_TRANS] 20150701:b
    template<typename T>
    int ObLogWriter::write_log(const LogCommand cmd, const T& data, const common::ObTransID& coo_trans_id)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = check_inner_stat()))
      {
        YYSYS_LOG(ERROR, "check_inner_stat()=>%d", ret);
      }
      else if (OB_SUCCESS != (ret = log_generator_.write_log(cmd, data, coo_trans_id))
               && OB_BUF_NOT_ENOUGH != ret)
      {
        YYSYS_LOG(WARN, "log_generator.write_log(cmd=%d, data=%p)=>%d", cmd, &data, ret);
      }
      return ret;
    }
    //add 20150701:e
  } // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_OB_LOG_WRITER_H_
