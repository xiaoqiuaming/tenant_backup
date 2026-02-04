/**
 * ob_row_merge_iterator.h
 *
 * Version: $Id
 *
 * Authors:
 *   lijianqiang <lijianqiang@mail.nwpu.edu.cn>
 *     --some work detials if U want
 */

#ifndef OB_ROW_MERGE_ITERATOR_H
#define OB_ROW_MERGE_ITERATOR_H

#include "ob_row.h"
#include "ob_new_scanner.h"

namespace oceanbase
{
  namespace common
  {
    /**
     * iter data in row level
     */
    class ObRowMergeIterator
    {
      public:
        ObRowMergeIterator()
        {}
        virtual ~ObRowMergeIterator()
        {}

        virtual int next_row() = 0;
        virtual int get_row(const ObRowkey *&rowkey, const ObRow *&row) = 0;
        virtual int get_row(const ObRowkey *&rowkey, const ObRow *&row, int64_t& row_version) = 0;
        virtual int get_row(const ObRowkey *&rowkey, const ObRow *&row, int64_t& row_version,const ObRowVersion *&version) = 0;
    };
  }
}

#endif // OB_ROW_MERGE_ITERATOR_H
