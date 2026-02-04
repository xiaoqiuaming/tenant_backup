/**
 * ob_multi_ups_multi_get.h defined for select rpc data from MultiUPS to CS using GET,
 * in MultiUPS,for select,the UPS get become from 1 to N (N>=1)
 *
 * Version: $Id
 *
 * Authors:
 *   lijianqiang <lijianqiang@mail.nwpu.edu.cn>
 *     --some work detials if U want
 */

#ifndef OB_OCEANBASE_SQL_OB_MULTI_UPS_MULTI_GET_H
#define OB_OCEANBASE_SQL_OB_MULTI_UPS_MULTI_GET_H

#include "common/ob_define.h"
#include "ob_rowkey_phy_operator.h"
#include "common/ob_se_array.h"
#include "common/ob_get_param.h"
#include "common/ob_sql_ups_rpc_proxy.h"
#include "chunkserver/ob_cs_partition_manager.h"
#include "ob_ups_multi_get.h"
#include "common/ob_row_merger.h"

namespace oceanbase
{
  using namespace common;
  namespace sql
  {

    class ObMultiUpsMultiGet: public ObRowkeyPhyOperator
    {
      public:
        ObMultiUpsMultiGet();
        virtual ~ObMultiUpsMultiGet();
        virtual void reset();
        virtual void reuse();
        virtual int set_child(int32_t child_idx, ObPhyOperator &child_operator);
        virtual int open();
        virtual int close();
        virtual ObPhyOperatorType get_type() const
        { return PHY_MULTI_UPS_MULTI_GET; }
        virtual int get_next_row(const ObRowkey *&rowkey, const ObRow *&row);
        virtual void set_row_desc(const ObRowDesc &row_desc);
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;

        virtual int set_sstable_rowkey(const ObRowkey *&rowkey);
        void set_get_param(const ObGetParam &get_param)
        {get_param_ = &get_param;}
        void set_stmt_type(ObBasicStmt::StmtType stmt_type)
        {stmt_type_=stmt_type;}
        ObBasicStmt::StmtType  get_stmt_type() const
        {return stmt_type_;}
        void set_schema_manager(const ObSchemaManagerV2* sm)
        {schema_manager_ = sm;}
        int set_rpc_proxy(ObSqlUpsRpcProxy *rpc_proxy);

        int set_ts_timeout_us(int64_t ts_timeout_us)
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
         * distribute get_param row by row,and add sub_param to ObUpsMultiGet ops
         *
         */
        int distribute_get_param(chunkserver::ObCsPartitionManager *par_mgr, uint64_t table_id, int64_t frozen_data_version);
        int create_ups_multi_get_ops();

       //add shili, [MultiUPS] [merge transaction code]20170309
        inline int set_trans_id(const ObSEArray< ObPartitionTransID,MAX_UPS_COUNT_ONE_CLUSTER> &trans_id)
        {
          int ret = OB_SUCCESS;
          trans_id_ = trans_id;
          YYSYS_LOG(DEBUG,"test =%s",to_cstring(trans_id));
          return ret;
        }
        //add e

        /**
         * add the get params to ObUpsMultiGet ops row by row,
         * one row may mapping more than one paxos group
         *
         */
        int create_ups_multi_get_ops_(const ObGetParam& get_param, const ObArray<chunkserver::ObCsPartitionManager::VersionPaxosId>& paxos_ids);
        int create_ups_multi_get_op_(int64_t paxos_id, int64_t *index = NULL);
        bool has_created_multi_get_op(const chunkserver::ObCsPartitionManager::VersionPaxosId& version_paxos, int64_t& index);
        int add_child(ObUpsMultiGet *child_op)
        {return children_ops_.push_back(child_op);}//for google test
      private:
        bool has_add_cell(int32_t paxos_id, ObArray<int32_t>& added_cell_paxos_ids);
      private:
        // disallow copy
        ObMultiUpsMultiGet(const ObUpsMultiGet &other);
        ObMultiUpsMultiGet& operator=(const ObUpsMultiGet &other);

      private:
        // data members
        common::ObSEArray<ObUpsMultiGet*, common::OB_PREALLOCATED_NUM> children_ops_;
        const ObGetParam *get_param_;
        ObSqlUpsRpcProxy *rpc_proxy_;
//        ObUpsRow cur_ups_row_;
        const ObRowDesc *row_desc_;
        int64_t ts_timeout_us_;
        //del hongchen [PERFORMANCE_OPTI] 20170821:b
        //common::ModuleArena  allocator_;          //for malloc
        //DEL hongchen [PERFORMANCE_OPTI] 20170821:e
        sql::ObBasicStmt::StmtType  stmt_type_;
        const ObSchemaManagerV2* schema_manager_;
        ObRowMerger row_merger_;
        //add by maosy [MultiUPS 1.0] [read uncommit]20170525 b:
        // ObTransID trans_id_;
        ObSEArray< ObPartitionTransID,MAX_UPS_COUNT_ONE_CLUSTER> trans_id_;
		// add by maosy e
    };
  }
}



#endif // OB_OCEANBASE_SQL_OB_MULTI_UPS_MULTI_GET_H
