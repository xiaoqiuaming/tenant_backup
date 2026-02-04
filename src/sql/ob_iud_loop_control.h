//add gaojt [Insert_Subquery_Function] [JHOBv0.1] 20150507:b
/**
 * ob_bind_values.h
 *
 * Authors:
 *   gaojt
 * function:Insert_Subquery_Function
 *
 * used for insert ... select
 */
#ifndef _OB_IUD_LOOP_CONTROL_H_
#define _OB_IUD_LOOP_CONTROL_H_
#include "ob_single_child_phy_operator.h"
#include "common/ob_row.h"
#include "ob_fill_values.h"
#include "ob_ups_executor.h"
#include "ob_bind_values.h"

namespace oceanbase
{
namespace sql
{
class ObIudLoopControl: public ObSingleChildPhyOperator
{
public:
  ObIudLoopControl();
  ~ObIudLoopControl();
  virtual int open();
  virtual int close();
  virtual void reset();
  virtual void reuse();

  int get_next_row(const common::ObRow *&row);
  int get_row_desc(const common::ObRowDesc *&row_desc) const;
  int64_t to_string(char* buf, const int64_t buf_len) const;
  void set_need_start_trans(bool need_start)
  {need_start_trans_ = need_start ; }
  void get_affect_rows(bool& is_ud_original,
                       ObFillValues *fill_values,
                       ObBindValues *bind_values);
  void set_affect_row();
  bool is_batch_over(ObFillValues*& fill_values, ObBindValues *&bind_values);
  void get_fill_bind_values(ObFillValues*& fill_values,ObBindValues*& bind_values);
  int set_stmt_start_time();//add by maosy [MultiUps 1.0] [batch_iud_snapshot] 20170622 
  int execute_stmt_no_return_rows(const ObString &stmt);
  int start_transaction();
  int commit();
  int rollback();
  void set_sql_context(ObSqlContext sql_context)
  {sql_context_ = sql_context;};
  enum ObPhyOperatorType get_type() const
  {return PHY_IUD_LOOP_CONTROL;}
  DECLARE_PHY_OPERATOR_ASSIGN;
private:
  DISALLOW_COPY_AND_ASSIGN(ObIudLoopControl);
private:
  int64_t batch_num_;
  int64_t affected_row_;
  bool need_start_trans_;//是否需要开始事务，只有在非hint的批量时在为true
  ObSqlContext sql_context_;
};
} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_IUD_LOOP_CONTROL_H_*/
//add gaojt 20150507:e
