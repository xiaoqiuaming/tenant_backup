/**
 * ob_row_merger.h defined to merge all rows with same rowkey(different row version)
 * or different rowkey, in brief, merge data in row level.
 *
 * Version: $Id
 *
 * Authors: hongchen
 *     --some work detials if U want
 */

#ifndef OB_ROW_MERGER_H
#define OB_ROW_MERGER_H

#include "ob_define.h"
#include "ob_row_merge_iterator.h"
#include "ob_ups_row.h"
#include "common/ob_new_scanner.h"
namespace oceanbase
{
  namespace common
  {
    /**
     * merge data row by row, compare  rowkey first, if in the same rows, return the row with biggest row version
     */
    class ObRowMerger: public ObRowMergeIterator
    {
      public:
        ObRowMerger();
        virtual ~ObRowMerger();
        virtual int next_row();
        virtual int get_row(const ObRowkey *&rowkey, const ObRow *&row);
        virtual int get_row(const ObRowkey *&rowkey, const ObRow *&row, int64_t& row_version);
        virtual int get_row(const ObRowkey *&rowkey, const ObRow *&row, int64_t& row_version,const ObRowVersion *&version);

        void reset();
        void set_merge_row_desc(const ObRowDesc& row_desc);
        int  add_row_iterator(ObRowMergeIterator *row_iter);

        int set_cur_sstable_rowkey(const ObRowkey *&rowkey);//[693]

        int  get_next_row(const common::ObRowkey *&rowkey, const common::ObRow *&row);
      private:
        int  next_row_(int64_t row_iter_idx);
        int  pop_current_row(const int64_t index);
        int  do_apply_delete(const int64_t delete_version);
        int  do_apply_row(const common::ObRow *&row, const ObRowVersion &row_version);
        int  apply_row(const common::ObRow *&row,const ObRowVersion &row_version);
//        int  skew();
        int skew(const ObRowkey *rowkey);
        int do_apply_row(const common::ObRow *&row);//add for [215-bug fix]

      private:
        enum RowStatus
        {
          BEFORE_NEXT = 0,  // before next_row called
          AFTER_NEXT,       // after next_row called
          ITER_END
        };
      private:
        ObRowMergeIterator*  row_iters_[MAX_UPS_COUNT_ONE_CLUSTER];
        RowStatus            row_iter_status_[MAX_UPS_COUNT_ONE_CLUSTER];
        int64_t              row_iter_num_;

        ObRowkey*            cur_rowkey_;        
        ObUpsRow             result_row_;
        ObRowVersion         row_version_;
        ObStringBuf          str_buf_;

        //[693]
        ObStringBuf          sstable_rowkey_buf_;
        ObRowkey*            cur_sstable_rowkey_;
    };
  }
}

#endif // OB_ROW_MERGER_H
