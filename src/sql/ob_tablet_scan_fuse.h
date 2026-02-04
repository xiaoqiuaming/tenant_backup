
/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_tablet_fuse.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_TABLET_SCAN_FUSE_H
#define _OB_TABLET_SCAN_FUSE_H 1

#include "ob_rowkey_phy_operator.h"
#include "common/ob_ups_row.h"
#include "common/ob_range.h"
#include "ob_sstable_scan.h"
#include "ob_ups_scan.h"
#include "ob_tablet_fuse.h"
#include "ob_multi_ups_scan.h"//lijianqiang [MultiUPS] [SELECT_MERGE] 20160105

namespace oceanbase
{
  using namespace common;

  namespace sql
  {
    // 用于CS从合并sstable中的静态数据和UPS中的增量数据
    class ObTabletScanFuse: public ObTabletFuse
    {
      public:
        ObTabletScanFuse();
        virtual ~ObTabletScanFuse();
        virtual void reset();
        virtual void reuse();
        int set_child(int32_t child_idx, ObPhyOperator &child_operator);
        int set_sstable_scan(ObSSTableScan *sstable_scan);
        //mod lijianqiang [MultiUPS] [SELECT_MERGE] 20160107:b
        //int ObTabletScanFuse::set_incremental_scan(ObUpsScan *incremental_scan)
        //int set_incremental_scan(ObUpsScan *incremental_scan);
        int set_incremental_scan(ObMultiUpsScan *incremental_scan);
        //mod 20160107:e

        int open();
        int close();
        virtual ObPhyOperatorType get_type() const
        { return PHY_TABLET_SCAN_FUSE; }
        int get_next_row(const ObRow *&row);
        int get_last_rowkey(const ObRowkey *&rowkey);
        int64_t to_string(char* buf, const int64_t buf_len) const;
        int get_row_desc(const common::ObRowDesc *&row_desc) const;
        //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151223:b
        inline ObReadAtomicParam &get_read_atomic()
        {
          return read_atomic_param_;
        }

        inline int set_read_atomic_param(const ObReadAtomicParam &read_atomic_param)
        {
          int ret = OB_SUCCESS;
          read_atomic_param_ = read_atomic_param;
          if (read_atomic_param.is_valid())
            YYSYS_LOG(DEBUG,"read_atomic::debug,scan fuse read atomic param=[%s],ret=%d",
                      to_cstring(read_atomic_param_),ret);
          return ret;
        }

        //add duyr 20151223:e

      private:
        // disallow copy
        ObTabletScanFuse(const ObTabletScanFuse &other);
        ObTabletScanFuse& operator=(const ObTabletScanFuse &other);

        int compare_rowkey(const ObRowkey &rowkey1, const ObRowkey &rowkey2);
        bool check_inner_stat();
        //add hongchen [MEM_EARLY_RELEASE_BUGFIX] 20170901:b
        int  deep_copy_incr_rowkey();
        //add hongchen [MEM_EARLY_RELEASE_BUGFIX] 20170901:e

      private:
        // data members
        ObSSTableScan *sstable_scan_; // 从sstable读取的静态数据
        //mod lijianqiang [MultiUPS] [SELECT_MERGE] 20160105:b
        //ObUpsScan *incremental_scan_; // 从UPS读取的增量数据
        ObMultiUpsScan *incremental_scan_;
        //mod 20160105:e
        const ObRow *last_sstable_row_;
        const ObUpsRow *last_incr_row_;
        const ObRowkey *sstable_rowkey_;
        const ObRowkey *incremental_rowkey_;
        const ObRowkey *last_rowkey_;
        ObRow curr_row_;
        //add hongchen [MEM_EARLY_RELEASE_BUGFIX] 20170901:b
        //this buf only use for incremental_rowkey_
        ObStringBuf    str_buf_;
        //add hongchen [MEM_EARLY_RELEASE_BUGFIX] 20170901:e
        //add duyr [MultiUPS] [READ_ATOMIC] [read_part] 20151223:b
        ObReadAtomicParam read_atomic_param_;
        //add duyr 20151223:e
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_TABLET_SCAN_FUSE_H */
