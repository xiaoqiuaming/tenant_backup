/**
 * ob_multi_ups_scan_cell_stream.h defined for rpc data from MultiUPS to CS,
 * in MultiUPS,for tablet merge,the UPS stream become from 1 to N (N>=1)
 *
 * Version: $Id
 *
 * Authors:
 *   lijianqiang <lijianqiang@mail.nwpu.edu.cn>
 *     --some work detials if U want
 */

#ifndef OB_CHUNKSERVER_OB_MULTI_UPS_SCAN_CELL_STREAM_H
#define OB_CHUNKSERVER_OB_MULTI_UPS_SCAN_CELL_STREAM_H

#include "ob_cell_stream.h"
#include "ob_scan_cell_stream.h"

namespace oceanbase
{
  namespace chunkserver
  {
    /**
     * this class is used for dalily merge for MultiUPS,in dalily merge, just use
     * the scan strategy,for each tablet, need to get the all datas from all paxos
     * groups, also, U need to konw that the merger only scan static data with version
     * STATIC_VERSION in CS and inc data with version STATIC_VERSION +1 in UPS, which
     * means the row with the same rowKey will not be located in different UPS(different
     * paxos group)in current partition rules. Using this property, U can have a better
     * understanding of dalily merge.
     *
     */
    class ObMultiUpsScanCellStreams : public ObCellStream
    {
      public:
        ObMultiUpsScanCellStreams(ObMergerRpcProxy * rpc_proxy,
                                 const common::ObServerType server_type = common::MERGE_SERVER,
                                 const int64_t time_out = 0);
        virtual ~ObMultiUpsScanCellStreams();

      public:

        /**
         * create all scan_cell_steams and do scan for each scan_cell_stream
         *
         * @param param [IN] scan param
         * @param it_out [OUT] the iterators for all paxos_ids
         * @param size [OUT] the count of iterators
         * @return OB_SUCCESS if succeed
         */
         virtual int scan(const common::ObScanParam& param, ObIterator *it_out[], int64_t& size);

         /**
          * get all paxos ids and create scan_stream for each id
          *
          * @param paxos_group_num [OUT] the group num
          */
         int create_all_scan_streams(const uint64_t table_id, const ObVersionRange& version_reange, int64_t& paxos_group_num);

         /**
          * get all paxos ids by rpc_proxy_
          *
          * @param paxos_group_ids [OUT] all valid paxos ids
          * @param paxos_group_num [OUT] the count of paxos ids
          */
         int get_paxos_group_id_array(const uint64_t table_id, const ObVersionRange& version_range, int64_t *paxos_group_ids, int64_t& paxos_group_num);

         /**
          * in daily merge, if the table is has no rules, which means partition in table level,
          * the table only located in one ups,we do not need to scan all ups,puring the ups without data
          *
          * @param table_id [in] the table which will be mergered
          * @param paxos_group_ids [IN]][OUT] in with all master ups id, out with puring
          * @param paxos_group_num [IN][OUT] the master ups number
          */
         int pruing_useless_paxos_group(const uint64_t table_id, int64_t *paxos_group_ids, int64_t& paxos_group_num);

         /**
          * reset inner stat for new usage
          */
         void reset_multi_inner_stat(void);

         /**
          * reset,do nothing,all scan cell streams shouldn't be reset,
          * which will be reset_inner_stat()  before next scan.
          *
          * @warning do not free all streams, the data version is needed later
          */
         virtual void reset(void);

         /**
          * can't be called
          */
         int get_cell(common::ObCellInfo** cell)
         {
           UNUSED(cell);
           YYSYS_LOG(ERROR,"not supported");
           return OB_NOT_SUPPORTED;
         }

         /**
          * can't be called
          */
         int get_cell(common::ObCellInfo** cell, bool * is_row_changed)
         {
           UNUSED(cell);
           UNUSED(is_row_changed);
           YYSYS_LOG(ERROR, "not supported");
           return OB_NOT_SUPPORTED;
         }

         /**
          * can't be called
          */
         virtual int next_cell(void)
         {
           YYSYS_LOG(ERROR, "not supported");
           return OB_NOT_SUPPORTED;
         }

         /**
          * return the biggest data version of all streams(data scan)
          */
         virtual int64_t get_data_version() const;

      private:
        DISALLOW_COPY_AND_ASSIGN(ObMultiUpsScanCellStreams);

        ObArray<ObScanCellStream*> multi_ups_streams_; // all ups streams,remember free the mem manual
        common::ModuleArena  allocator_;               //for mem alloc
    };

  }// end of chunkserver namespace
} //end of oceanbase namesapace
#endif // OB_CHUNKSERVER_OB_MULTI_UPS_SCAN_CELL_STREAM_H
