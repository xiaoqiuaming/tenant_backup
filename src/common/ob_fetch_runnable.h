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

#ifndef OCEANBASE_COMMON_OB_FETCH_RUNNABLE_H_
#define OCEANBASE_COMMON_OB_FETCH_RUNNABLE_H_

#include "ob_define.h"
#include "ob_server.h"
#include "ob_vector.h"
#include "ob_string.h"
#include "ob_string_buf.h"
#include "ob_role_mgr.h"
#include "ob_log_replay_runnable.h"

#include "common/file_utils.h"
#include "yysys.h"

namespace oceanbase
{
  //forward decleration
  namespace tests
  {
    namespace common
    {
      class TestObFetchRunnable_test_fill_fetch_cmd__Test;
      class TestObFetchRunnable_test_vsystem__Test;
    } // end namespace common
  } // end namespace tests
  namespace common
  {
    /// Fetch�߳���Ҫ��ȡ����־�ŷ�Χ��checkpoint��
    /// ��һ��checkpoint�ж���ļ�ʱ, ckpt_ext_�������������ļ��ĺ�׺��
    /// Fetch�̻߳Ὣckpt_id_�µĶ����ͬ��׺����checkpoint�ļ�����ȡ��
    struct ObFetchParam
    {
      uint64_t min_log_id_;
      uint64_t max_log_id_;
      uint64_t ckpt_id_;
      bool fetch_log_; // whether to fetch log files
      bool fetch_ckpt_; // whether to fetch chekpoint files

      ObFetchParam() : min_log_id_(0), max_log_id_(0), ckpt_id_(0), fetch_log_(false), fetch_ckpt_(false)
      {}

      NEED_SERIALIZE_AND_DESERIALIZE;
    };

    template <>
    struct ob_vector_traits<ObString>
    {
    public:
      typedef ObString& pointee_type;
      typedef ObString value_type;
      typedef const ObString const_value_type;
      typedef value_type* iterator;
      typedef const value_type* const_iterator;
      typedef int32_t difference_type;
    };

    /// @brief Slave��ȡMaster�����ϵ���־��checkpoint
    /// ��rsyncʵ����ӦԶ�̻�ȡ����
    /// ��Ҫ����ʱ��֤
    ///     1. Slave���̺�Master����ʹ����ͬ���˻�����
    ///     2. Slave���̵Ĺ���Ŀ¼��Master���̹���Ŀ¼����ȫ��ͬ��
    ///     3. Slave������Master����֮�佨�������ι�ϵ
    class ObFetchRunnable : public yysys::CDefaultRunnable
    {
      friend class tests::common::TestObFetchRunnable_test_fill_fetch_cmd__Test;
      friend class tests::common::TestObFetchRunnable_test_vsystem__Test;
    public:
      static const int64_t DEFAULT_LIMIT_RATE = 1L << 14; // 16 * 1024 KBps
      static const int DEFAULT_FETCH_RETRY_TIMES = 3;
      static const char* DEFAULT_FETCH_OPTION;
    public:
      ObFetchRunnable();
      virtual ~ObFetchRunnable();

      virtual void run(yysys::CThread* thread, void* arg);

      virtual void clear();

      /// @brief ��ʼ��
      /// @param [in] master Master��ַ
      /// @param [in] log_dir ��־Ŀ¼
      /// @param [in] param Fetch�߳���Ҫ��ȡ����־��ŷ�Χ��checkpoint��
      //modify for [multi backup recovery]-b
//      virtual int init(const ObServer &master, const char* log_dir, const ObFetchParam &param, common::ObRoleMgr *role_mgr, common::ObLogReplayRunnable *replay_thread);
      virtual int init(const ObServer &master, const char* log_dir, const ObFetchParam &param, common::ObRoleMgr *role_mgr, common::ObLogReplayRunnable *replay_thread, int64_t paxos_id = 0);
      //modify for [multi backup recovery]-e

      /// @brief set fetch param
      /// @param [in] param fetch param indicates the log range to be fetched
      virtual int set_fetch_param(const ObFetchParam& param);

      /// @brief ����һ��checkpoint�ļ���׺��
      /// @param [in] ext �ļ���׺��
      virtual int add_ckpt_ext(const char* ext);

      /// @brief this func is called when all ckpt files are successfully got
      /// @param [in] ckpt_id ckpt id
      virtual int got_ckpt(uint64_t ckpt_id);

      /// @brief this func is called when a log file is successfully got
      /// @param [in] log_id log id
      virtual int got_log(uint64_t log_id);

      /// @brief set user defined option
      /// @param [in] option user defined option, ended with '\0' and maximum length is 2048B
      virtual int set_usr_opt(const char* opt);

      inline void set_limit_rate(const int64_t new_limit)
      {
        limit_rate_ = new_limit;
      }

      inline void set_master(const ObServer master)
      {
        master_ = master;
      }

      inline int64_t get_limit_rate()
      {
        return limit_rate_;
      }
    protected:
      int gen_fetch_cmd_(const uint64_t id, const char* fn_ext, char* cmd, const int64_t size);

      bool exists_(const uint64_t id, const char* fn_ext) const;

      bool remove_(const uint64_t id, const char* fn_ext) const;

      int gen_full_name_(const uint64_t id, const char* fn_ext, char *buf, const int buf_len) const;

      //add for [multi backup recovery]-b
      int get_commit_point(uint64_t max_log_file_id);
      //add for [multi backup recovery]-e

      virtual int get_log_();

      virtual int get_ckpt_();

    protected:
      typedef ObVector<ObString> CkptList;
      typedef CkptList::iterator CkptIter;

      common::ObRoleMgr *role_mgr_;
      common::ObLogReplayRunnable *replay_thread_;
      int64_t limit_rate_;
      ObFetchParam param_;
      CkptList ckpt_ext_;
      ObStringBuf ckpt_buf_; // string_buf ���ڴ洢ckpt_ext_
      ObServer master_;
      bool is_initialized_;
      char cwd_[OB_MAX_FILE_NAME_LENGTH];
      char log_dir_[OB_MAX_FILE_NAME_LENGTH];
      char *usr_opt_;
      //add for [multi backup recovery]-b
      int64_t paxos_id_;
      //add for [multi backup recovery]-e
    };
  } // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_COMMON_OB_FETCH_RUNNABLE_H_
