/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_ups_multi_get.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_UPS_MULTI_GET_H
#define _OB_UPS_MULTI_GET_H 1

#include "common/ob_define.h"
#include "common/ob_get_param.h"
#include "common/ob_sql_ups_rpc_proxy.h"
#include "ob_rowkey_phy_operator.h"
#include "common/ob_row.h"
#include "chunkserver/ob_cs_partition_manager.h"
#include "chunkserver/ob_rpc_proxy.h"
//add lijianqiang [MultiUPS] [SELECT_MERGE] 20160107:b
#include "common/ob_row_merge_iterator.h"
//add 20160107:e

namespace oceanbase
{
  using namespace common;
  namespace sql
  {
    // 用于CS从UPS获取多行数据
    //mod lijianqiang [MultiUPS] [SELECT_MERGE] 20160107:b
    //class ObUpsMultiGet: public ObRowkeyPhyOperator
    class ObUpsMultiGet: public ObRowkeyPhyOperator, public ObRowMergeIterator
    //mod 20160107:e
    {
      public:
        ObUpsMultiGet();
        virtual ~ObUpsMultiGet();
        virtual void reset();
        virtual void reuse();
        virtual int set_child(int32_t child_idx, ObPhyOperator &child_operator);
        virtual int open();
        virtual int close();
        virtual ObPhyOperatorType get_type() const
        { return PHY_UPS_MULTI_GET; }
        virtual int get_next_row(const ObRowkey *&rowkey, const ObRow *&row);
        virtual void set_row_desc(const ObRowDesc &row_desc);
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;

        inline int set_rpc_proxy(ObSqlUpsRpcProxy *rpc_proxy);

        inline bool is_timeout(int64_t *remain_us /*= NULL*/) const
        {
          int64_t now = yysys::CTimeUtil::getTime();
          if (NULL != remain_us)
          {
            if (OB_LIKELY(ts_timeout_us_ > 0))
            {
              *remain_us = ts_timeout_us_ - now;
            }
            else
            {
              *remain_us = INT64_MAX; // no timeout
            }
          }
          return (ts_timeout_us_ > 0 && now > ts_timeout_us_);
        }

        inline int set_ts_timeout_us(int64_t ts_timeout_us)
        {
          int ret = OB_SUCCESS;
          if (ts_timeout_us > 0)
          {
            ts_timeout_us_ = ts_timeout_us;
          }
          else
          {
            ret = OB_INVALID_ARGUMENT;
          }
          return ret;
        }
        /**
         * 设置MultiGet的参数
         */
        //del lijianqiang [MultiUPS] [SELECT_MERGE] 20160503:b
        //inline void set_get_param(const ObGetParam &get_param);
        //del 20160503:e

        //add zhujun[transaction read uncommit]2016/3/28
        inline int set_trans_id(ObSEArray< ObPartitionTransID,MAX_UPS_COUNT_ONE_CLUSTER>& trans_id)
        {
            cur_get_param_.set_trans_id(trans_id);
            get_param_.set_trans_id(trans_id);
            return OB_SUCCESS;
        }
        //add:e

        //add lijianqiang [MultiUPS] [SELECT_MERGE] 20160104:b
        void set_is_read_consistency(const bool is_cos)
        {get_param_.set_is_read_consistency(is_cos);}
        void set_version_range(const ObVersionRange& range)
        {get_param_.set_version_range(range);}
        int add_cell(const ObCellInfo& cell_info);
        void set_paxos_id(int64_t paxos_id)
        {paxos_id_ = paxos_id;}
        int64_t get_paxos_id()
        {return paxos_id_;}
        int next_row();
        int get_row(const ObRowkey *&rowkey, const ObRow *&row);
        int get_row(const ObRowkey *&rowkey, const ObRow *&row, int64_t& row_version);
        //add 20160104:e

        //add shili, [MultiUPS] [COLUMN_VERSION_MERGE]  20170423:b
        int get_row(const ObRowkey *&rowkey, const ObRow *&row, int64_t& row_version,const ObRowVersion *&version);
        //add e

        //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151223:b
        void set_read_atomic_param(const ObReadAtomicParam &param)
        {get_param_.set_read_atomic_param(param);}
        //add duyr 20151223:e
		// add by maosy [MultiUps 1.0] [batch_udi] 20170420 b
        void set_data_mark_param(const ObDataMarkParam &param)
        {get_param_.set_data_mark_param(param);}
		// add by maosy 20170420 e
      private:
        // disallow copy
        ObUpsMultiGet(const ObUpsMultiGet &other);
        ObUpsMultiGet& operator=(const ObUpsMultiGet &other);

      protected:
        int next_get_param();
        bool check_inner_stat();

      protected:
        // data members
        //del lijianqiang [MultiUPS] [SELECT_MERGE] 20160104:b
        //const ObGetParam *get_param_;
        //del 20160104:e
        ObGetParam cur_get_param_;
        ObNewScanner cur_new_scanner_;
        ObSqlUpsRpcProxy *rpc_proxy_;
        ObUpsRow cur_ups_row_;
        int64_t got_row_count_;
        const ObRowDesc *row_desc_;
        int64_t ts_timeout_us_;
        //add lijianqiang [MultiUPS] [SELECT_MERGE] 20160104:b
        ObGetParam get_param_;//need deep copy
        int64_t paxos_id_;
        ObRowkey *cur_rowkey_;  //for compare
        int64_t cur_row_version_;  //the version for current row
        int64_t cur_row_version_index_;  //the index for current row in ObNewScanner row_version vector
        bool has_row_version_;
        //add 20160104:e
        //add shili, [MultiUPS] [COLUMN_VERSION_MERGE]  20170423:b
        ObRowVersion cur_row_version_v2_;
        //add e
    };

    int ObUpsMultiGet::set_rpc_proxy(ObSqlUpsRpcProxy *rpc_proxy)
    {
      int ret = OB_SUCCESS;
      if(NULL == rpc_proxy)
      {
        ret = OB_INVALID_ARGUMENT;
        YYSYS_LOG(WARN, "rpc_proxy is null");
      }
      else
      {
        rpc_proxy_ = rpc_proxy;
      }
      return ret;
    }

    //del lijianqiang [MultiUPS] [SELECT_MERGE] 20160104:b
    //void ObUpsMultiGet::set_get_param(const ObGetParam &get_param)
    //{
    //  get_param_ = &get_param;
    //}
    //del 20160104:e
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_UPS_MULTI_GET_H */
