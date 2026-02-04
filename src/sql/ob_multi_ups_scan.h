/**
 * ob_multi_ups_scan.h defined for select rpc data from MultiUPS to CS using SCAN,
 * in MultiUPS,for select,the UPS scan become from 1 to N (N>=1)
 *
 * Version: $Id
 *
 * Authors:
 *   lijianqiang <lijianqiang@mail.nwpu.edu.cn>
 *     --some work detials if U want
 */

#ifndef OB_OCEANBASE_SQL_OB_MULTI_UPS_SCAN_H
#define OB_OCEANBASE_SQL_OB_MULTI_UPS_SCAN_H

#include "ob_rowkey_phy_operator.h"
#include "common/ob_string.h"
#include "common/ob_se_array.h"
#include "common/ob_sql_ups_rpc_proxy.h"
#include "common/ob_scan_param.h"
#include "common/ob_range.h"
#include "ob_ups_scan.h"
#include "common/ob_row_merger.h"
namespace oceanbase
{
  using namespace common;
  namespace sql
  {
    class ObMultiUpsScan: public ObRowkeyPhyOperator
    {
      public:
        ObMultiUpsScan();
        virtual ~ObMultiUpsScan();
        virtual void reset();
        virtual void reuse();
        virtual int set_child(int32_t child_idx, ObPhyOperator &child_operator);
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;

        int set_ups_rpc_proxy(ObSqlUpsRpcProxy *rpc_proxy);
        virtual int add_column(const uint64_t &column_id);
        virtual int open();
        virtual int close();
        virtual ObPhyOperatorType get_type() const
        { return PHY_MULTI_UPS_SCAN; }
        virtual int get_next_row(const common::ObRowkey *&rowkey, const common::ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        /**
         * 设置要扫描的range
         */
        virtual int set_range(const ObNewRange &range);
        void set_version_range(const ObVersionRange &version_range);
        void set_data_mark_param(const ObDataMarkParam &param);//uncertainty  add formultiups
        //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151223:b
        void set_read_atomic_param(const ObReadAtomicParam &read_atomic_param);
        //add duyr 20151223:e

        bool is_result_empty() const;

        inline void set_is_read_consistency(bool is_read_consistency)
        {
          is_read_consistency_ = is_read_consistency;
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

        //add shili, [MultiUPS] [merge transaction code]20170309
        inline int set_trans_id(ObSEArray< ObPartitionTransID,MAX_UPS_COUNT_ONE_CLUSTER> &trans_id)
        {
          int ret = OB_SUCCESS;
          trans_id_.reset();
          trans_id_ = trans_id;
          return ret;
        }

        inline void set_meger_row_desc()
        {
          row_merger_.set_merge_row_desc(row_desc_);
        }
        //add e

        private:
        bool has_create_ups_scan_op(int64_t paxos_id_);
        int create_ups_scan_ops();
        int create_ups_scan_op(int64_t paxos_id);
        //mod liuzy [Multiups] [UPS_List_Info] 20160427:b
//        int reduce_scan_paxos_num(oceanbase::common::ObUpsList& master_ups_list, int64_t& group_num);
        int reduce_scan_paxos_num(ObArray<int64_t> &paxos_idx_array);
        //mod 20160427:e


      private:
        // disallow copy
        ObMultiUpsScan(const ObMultiUpsScan &other);
        ObMultiUpsScan& operator=(const ObMultiUpsScan &other);
      private:
        common::ObSEArray<ObUpsScan*, common::OB_PREALLOCATED_NUM> children_ops_;
        ObNewRange range_;
        ObScanParam scan_param_;
//        ObUpsRow cur_ups_row_;
        ObRowDesc row_desc_;
        ObSqlUpsRpcProxy *rpc_proxy_;
        int64_t ts_timeout_us_;
        int64_t row_counter_;
        bool is_read_consistency_;
        common::ModuleArena  allocator_;          //for malloc
        ObRowMerger row_merger_;
		//add by maosy [MultiUPS 1.0] [read uncommit]20170525 b:
		//ObTransID trans_id_;
        ObSEArray< ObPartitionTransID,MAX_UPS_COUNT_ONE_CLUSTER> trans_id_;
		// add by maosy e
    };
  }
}
#endif // OB_OCEANBASE_SQL_OB_MULTI_UPS_SCAN_H
