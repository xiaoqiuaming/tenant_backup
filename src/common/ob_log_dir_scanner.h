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

#ifndef OCEANBASE_COMMON_OB_LOG_DIR_SCANNER_H_
#define OCEANBASE_COMMON_OB_LOG_DIR_SCANNER_H_

#include "ob_vector.h"

namespace oceanbase
{
  namespace common
  {
    struct ObLogFile
    {
        char name[OB_MAX_FILE_NAME_LENGTH];
        uint64_t id;

        enum FileType
        {
          UNKNOWN = 1,
          LOG = 2,
          CKPT = 3
        };

        int assign(const char* filename, FileType &type);
        bool isLogId(const char* str) const ;
        uint64_t strToUint64(const char* str) const;
        bool operator< (const ObLogFile& r) const;
    };

    template <>
    struct ob_vector_traits<ObLogFile>
    {
      public:
        typedef ObLogFile& pointee_type;
        typedef ObLogFile value_type;
        typedef const ObLogFile const_value_type;
        typedef value_type* iterator;
        typedef const value_type* const_iterator;
        typedef int32_t difference_type;
    };

    class ObLogDirScanner
    {
      public:
        ObLogDirScanner();
        virtual ~ObLogDirScanner();

        /**
       * ��ʼ��������־Ŀ¼, ObLogScanner���ɨ������Ŀ¼
       * ����־�ļ�����id��������, �õ������־�ź���С��־��
       * ͬʱ�������checkpoint��
       *
       * !!! ɨ���������һ���쳣���, ����־Ŀ¼�ڵ��ļ�������
       * ��ʱ, ��С��������־����Ϊ����Ч��־
       * �����������ʱ, init����ֵ��OB_DISCONTINUOUS_LOG
       *
       * @param [in] log_dir ��־Ŀ¼
       * @return OB_SUCCESS �ɹ�
       * @return OB_DISCONTINUOUS_LOG ɨ��ɹ�, ������־�ļ�������
       * @return otherwise ʧ��
       */
        int init(const char* log_dir);

        /// @brief get minimum commit log id
        /// @retval OB_SUCCESS
        /// @retval OB_ENTRY_NOT_EXIST no commit log in directory
        int get_min_log_id(uint64_t &log_id) const;

        /// @brief get maximum commit log id
        /// @retval OB_SUCCESS
        /// @retval OB_ENTRY_NOT_EXIST no commit log in directory
        int get_max_log_id(uint64_t &log_id) const;

        /// @brief get maximum checkpoint id
        /// @retval OB_SUCCESS
        /// @retval OB_ENTRY_NOT_EXIST no commit log in directory
        int get_max_ckpt_id(uint64_t &ckpt_id) const;

        bool has_log() const;
        bool has_ckpt() const;

      private:
        /**
       * ������־Ŀ¼�µ������ļ�, �ҵ�������־�ļ�
       */
        int search_log_dir_(const char* log_dir);

        /**
       * �����־�ļ��Ƿ�����
       * ͬʱ��ȡ��С�������־�ļ���
       */
        int check_continuity_(const ObVector<ObLogFile> &files, uint64_t &min_file_id, uint64_t &max_file_id);

        inline int check_inner_stat() const
        {
          int ret = OB_SUCCESS;
          if (!is_initialized_)
          {
            YYSYS_LOG(ERROR, "ObLogDirScanner has not been initialized.");
            ret = OB_NOT_INIT;
          }
          return ret;
        }

      private:
        uint64_t min_log_id_;
        uint64_t max_log_id_;
        uint64_t max_ckpt_id_;
        bool has_log_;
        bool has_ckpt_;
        bool is_initialized_;
    };
  } // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_OB_LOG_DIR_SCANNER_H_
