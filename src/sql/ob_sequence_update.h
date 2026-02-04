//add lijianqiang [sequence] 20150512:b
/**
 * ob_sequence_update.h
 *
 * used for "UPDATE table_name SET C2 = a, C3 = NEXTVAL/PREVVAL FOR sequence_name... WHERE C1 = PREVVAL FOR sequence_name AND ..."
 *
 * Authors:
 * lijianqiang <lijianqiang@mail.nwpu.edu.cn>
 *
 */
#ifndef OB_OCEANBASE_SQL_SEQUENCE_UPDATE_H
#define OB_OCEANBASE_SQL_SEQUENCE_UPDATE_H

#include "ob_update_stmt.h"
#include "ob_sequence.h"
#include "ob_index_trigger_upd.h"

namespace oceanbase
{
  namespace sql
  {
    class ObSequenceUpdate: public ObSequence
    {
      public:
        ObSequenceUpdate();
        virtual ~ObSequenceUpdate();
        virtual int open();
        virtual int close();
        virtual void reset();
        virtual void reuse();
        int64_t to_string(char* buf, const int64_t buf_len) const;
        enum ObPhyOperatorType get_type() const
        { return PHY_SEQUENCE_UPDATE; }
        int copy_sequence_info_from_update_stmt();
        bool is_sequence_cond_id(uint64_t cond_id);
        int fill_the_sequence_info_to_cond_expr(ObSqlExpression *filter, uint64_t cond_expr_id);
        void gen_sequence_condition_names_idx();
        void reset_sequence_condition_names_idx();
        void set_index_trigger_update(ObIndexTriggerUpd * index_trigger_updat,bool is_column_hint_index)
        {
            index_trigger_update_ = index_trigger_updat;
            is_column_hint_index_ = is_column_hint_index;
        }
        int handle_the_set_clause_of_seuqence(const int64_t& row_num);

      private:
        int64_t condition_types_names_idx_;//sequence names 和 types 的下标跟踪标记，没次出理完毕一个含sequence的表达式就自增1
        int64_t const_condition_types_names_idx_;//has the same init value with condition_types_names_idx_,but not increase auto,for rest
        ObIndexTriggerUpd * index_trigger_update_;
        bool is_column_hint_index_;
        common::ObArray<uint64_t> sequecne_expr_ids_;//if current expr has sequence ,push expr_id,else push 0 (copy from logical plan)
    };
    inline bool ObSequenceUpdate::is_sequence_cond_id(uint64_t cond_id)
    {
      bool ret = false;
      ObUpdateStmt *update_stmt = NULL;
      update_stmt = dynamic_cast<ObUpdateStmt *>(sequence_stmt_);
      if (NULL == update_stmt)
      {
        YYSYS_LOG(ERROR, "update_stmt not init!");
      }
      else
      {
        int64_t num = update_stmt->get_sequence_expr_ids_size();
        for (int64_t i = 0; i < num; i++)
        {
          if (cond_id == update_stmt->get_sequence_expr_id(i))
          {
            ret = true;
            break;
          }
        }
      }
      return ret;
    }

    inline void ObSequenceUpdate::gen_sequence_condition_names_idx()
    {
      condition_types_names_idx_++;
    }
    /**
     * if U want to resue the sequence in where conditions in update_stmt
     * please call this fuction before reusing
     */
    inline void ObSequenceUpdate::reset_sequence_condition_names_idx()
    {
      condition_types_names_idx_ = const_condition_types_names_idx_;
    }



  } // end namespace sql
}
#endif // OB_SEQUENCE_UPDATE_H
//add  20150512:e
