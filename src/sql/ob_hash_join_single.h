
#ifndef OB_HASH_JOIN_SINGLE_H
#define OB_HASH_JOIN_SINGLE_H

#include "ob_join.h"
#include "sql/ob_phy_operator.h"
#include "common/ob_row.h"
#include "common/ob_row_store.h"
#include "common/ob_rowkey.h"
#include "common/ob_array.h"
#include "ob_filter.h"

#include "common/hash/ob_hashmap.h"
#include "common/ob_custom_allocator.h"

namespace oceanbase
{
  namespace sql
  {
    class ObHashJoinSingle: public ObJoin
    {
      typedef std::pair<const ObRowStore::StoredRow *, int8_t> HashTableRowPair;
      typedef common::hash::ObHashTableNode<common::hash::HashMapTypes<uint64_t, HashTableRowPair*>::pair_type> hashnode;
      typedef common::hash::ObHashMap<uint64_t, HashTableRowPair*, common::hash::NoPthreadDefendMode> HashTableRowMap;
    
    public:
      ObHashJoinSingle();
      ~ObHashJoinSingle();
      virtual int open();
      virtual int close();
      virtual void reset(); 
      virtual void reuse();
      virtual int set_join_type(const ObJoin::JoinType join_type);
      virtual ObPhyOperatorType get_type() const { return PHY_HASH_JOIN_SINGLE; }
      
      DECLARE_PHY_OPERATOR_ASSIGN;
      
      int get_next_row(const common::ObRow *&row);
      int get_row_desc(const common::ObRowDesc *&row_desc) const;
      int64_t to_string(char* buf, const int64_t buf_len) const;
    private:
      int inner_hash_get_next_row(const common::ObRow *&row);
      int left_hash_outer_get_next_row(const common::ObRow *&row);
      int right_hash_outer_get_next_row(const common::ObRow *&row);
      int full_hash_outer_get_next_row(const common::ObRow *&row);
      int compare_equijoin_cond(const ObRow& r1, const ObRow& r2, int &cmp) const;
      int left_row_compare_equijoin_cond(const ObRow& r1, const ObRow& r2, int &cmp) const;
      int right_row_compare_equijoin_cond(const ObRow& r1, const ObRow& r2, int &cmp) const;
      int get_next_equijoin_left_row(const ObRow *&r1, const ObRow& r2, uint64_t& bucket_hash_key, HashTableRowPair*& pair);
      int get_next_leftouterjoin_left_row(const common::ObRow*& row);
      int curr_row_is_qualified(bool &is_qualified);
      int cons_row_desc(const ObRowDesc &rd1, const ObRowDesc &rd2);
      int join_rows(const ObRow& r1, const ObRow& r2);
      int left_join_rows(const ObRow& r1);
      int right_join_rows(const ObRow& r2);
      int process_sub_query();

      DISALLOW_COPY_AND_ASSIGN(ObHashJoinSingle);
    
    private:
      static const int64_t MAX_SINGLE_ROW_SIZE = common::OB_ROW_MAX_COLUMNS_COUNT*(common::OB_MAX_VARCHAR_LENGTH+4);
      typedef int (ObHashJoinSingle::*get_next_row_func_type)(const common::ObRow *&row);
      get_next_row_func_type get_next_row_func_;
      const common::ObRow *last_left_row_;
      const common::ObRow *last_right_row_;
      ObSqlExpression *table_filter_expr_ ;
      common::ObRow curr_cached_left_row_;
      common::ObRowDesc row_desc_;
      bool left_hash_key_cache_valid_;
      uint64_t last_left_hash_key_;
      bool use_bloomfilter_;
      int64_t left_bucket_pos_for_left_outer_join_;
      bool is_left_iter_end_;
      static const int MAX_SUB_QUERY_NUM = 5;
      static const int64_t HASH_BUCKET_NUM = 100000;
      static const int BIG_RESULTSET_THRESHOLD = 50;
      int hashmap_num_ ;
      common::CustomAllocator arena_;
      common::ObArray<common::ObRowkey>  sub_result_;
      common::hash::ObHashMap<common::ObRowkey,common::ObRowkey,common::hash::NoPthreadDefendMode> sub_query_map_[MAX_SUB_QUERY_NUM];
      common::ObArray<ObObjType> sub_query_map_and_bloomfilter_column_type[MAX_SUB_QUERY_NUM];  
      bool is_subquery_result_contain_null[MAX_SUB_QUERY_NUM];
      static const int64_t BLOOMFILTER_ELEMENT_NUM = 1000000;
      common::ObRow curr_row_;
      hashnode* bucket_node_;
      bool is_mached_;
      common::ObRowStore row_store_;
      HashTableRowMap hash_table_;
      int64_t left_row_count_;
    };
  } // end namespace sql
} // end namespace oceanbase
#endif /*  */
