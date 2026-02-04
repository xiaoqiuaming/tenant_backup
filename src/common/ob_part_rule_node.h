/*
*   Version: 0.1
*
*   Authors:
*      liu jun
*        - some work details if you want
*/

#ifndef OB_PART_RULE_NODE_H
#define OB_PART_RULE_NODE_H

#include "ob_fifo_allocator.h"
#include "ob_string.h"
#include "ob_row.h"
#include "ob_calc_expression.h"
#include "ob_hash_expression.h"
#include "ob_range_expression.h"
#include "ob_enum_expression.h"
#include "ob_table_rule_node.h"//add wuna [MultiUps][sql_api] 20151217
#include "ob_hardcode_expression.h" //add for [577-hardcode partition]
#define OB_NEW_EXPRESSION(T)                    \
  ({                                            \
  T* ret = NULL;                                \
  ret = new(std::nothrow)T() ;                             \
  ret;                                            \
  })

#define OB_DELETE_EXPRESSION(T)                 \
  ({                                      \
  if(NULL != T)                         \
{                                      \
  delete(T);                            \
  }                                       \
  })

using namespace oceanbase::common;

namespace oceanbase
{
  namespace common
  {
    class ObPartRuleNode
    {
      public:
        ObPartRuleNode();
        ~ObPartRuleNode();
        //add hongchen [PERFORMANCE_OPTI] 20170821:b
        void reset();
        //add hongchen [PERFORMANCE_OPTI] 20170821:e
        //add lijianqiang [MultiUPS] [PHYSICAL_PLAN_TRANSFORM] 20160412:b
        int assign(const ObPartRuleNode& other);
        //add 20160412:e
        //mod wuna [MultiUps][sql_api] 20151217:b
        //int row_to_entity(const ObRow &row, FIFOAllocator &allocator);
        int row_to_entity(const ObRow &row, PageArena<char> &allocator);
        //mod 20151217:e
        const ObString &get_rule_name() const;
        int32_t get_rule_para_num() const;
        const ObString &get_rule_para_list() const;
        const ObString &get_rule_body() const;
        int equals(const ObPartRuleNode &part_entity);
        int separate(const ObString &src, const char separator, ObArray<ObString> &result) const;
        //mod wuna [MultiUps][sql_api] 20151217:b
        //int parser(FIFOAllocator &allocator);
        int parser(ObTableRuleNode *&table_node,ObCellArray& cell_array);
        //mod 20151217:e
        ObCalcExpression::ExpressionType& get_type(); //add wuna [MultiUps][sql_api] 20151217

        ObCalcExpression *get_calc_expr();
        //del wuna [MultiUps][sql_api] 20151217:b
        //const ObArray<ObString> &get_parameters() const;
        //del 20151217:e
      private:
        ObCalcExpression *calc_expr_;
        ObCalcExpression::ExpressionType type_;
        ObString rule_name_;
        int32_t rule_para_num_;
        ObString rule_para_list_;
        ObString rule_body_;
        //ObArray<ObString> params_;//del wuna [MultiUps][sql_api] 20151217
        mutable yysys::CThreadMutex init_mutex_;  // bug fix multithread op peiouya 20150826
        bool inited_;
    };
    //add wuna [MultiUps][sql_api] 20151217:b
    inline ObCalcExpression::ExpressionType& ObPartRuleNode::get_type()
    {
      return type_;
    }
    //add 20151217:e
    inline ObCalcExpression *ObPartRuleNode::get_calc_expr()
    {
      return calc_expr_;
    }

    //del wuna [MultiUps][sql_api] 20151217:b
    //inline const ObArray<ObString> &ObPartRuleNode::get_parameters() const
    //{
    //  return params_;
    //}
    //del 20151217:e
  }
}
#endif // OB_PART_RULE_NODE_H
