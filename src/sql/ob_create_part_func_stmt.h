#ifndef OB_CREATE_PART_FUNC_H
#define OB_CREATE_PART_FUNC_H 1

#include "sql/ob_basic_stmt.h"
#include "common/ob_array.h"
#include "common/ob_string.h"
#include "common/ob_string_buf.h"
#include "parse_node.h"
#include "common/ob_calc_expression.h"//add wuna [MultiUps] [sql_api] 20151228
#include "common/ob_part_rule_node.h"//add wuna [MultiUps] [sql_api] 20151228

namespace oceanbase
{
  namespace sql
  {
   class ObCreatePartFuncStmt;
   class RangePartitionNode
   {
   public:
      common::ObString partition_name_;
      common::ObArray<common::ObObj> partition_value_obj_;
   public:
      RangePartitionNode()
      {
      }
      int add_partition_name(ObCreatePartFuncStmt* create_part_stmt,const ObString& part_name);
      int add_partition_value_obj(ObCreatePartFuncStmt* create_part_stmt,const ObObj& src_obj);
    };
    class ObCreatePartFuncStmt : public ObBasicStmt
    {
      friend class RangePartitionNode;
    public:
      ObCreatePartFuncStmt(common::ObStringBuf* name_pool);
      ObCreatePartFuncStmt(const ObCreatePartFuncStmt& other);
      ObCreatePartFuncStmt& operator=(const ObCreatePartFuncStmt& other);
      virtual ~ObCreatePartFuncStmt();
      int set_funcion_name(ResultPlan& result_plan, const common::ObString& function_name);
      int add_param_name(ResultPlan &result_plan, const common::ObString& param_name);
      int add_param_name(const common::ObString& param);
      int set_func_context(ResultPlan& result_plan, const common::ObString& func_context);

      void set_parameters_num(const int32_t &param_num);
      int32_t get_parameters_num() const;
      const common::ObString& get_function_name() const;
      const int get_parameter(int64_t idx, common::ObString& obj) const;
      const common::ObArray<common::ObString>& get_all_parameters() const;
      const common::ObString& get_func_context() const;
      int check_expression() ;
      void print(FILE* fp, int32_t level, int32_t index);
      /*add wuna [MultiUps][sql_api] 20151202:b */
      int check_range_partition_value(ResultPlan& result_plan);
      void set_partition_type(ObFunctionPartitionType type);
      const ObFunctionPartitionType get_partition_type() const;
      int add_range_partition_node( RangePartitionNode*& node);
      const RangePartitionNode* get_range_partition_node(int32_t index) const;
      const int64_t get_range_partition_nodes_size() const;
      bool is_duplicate_partition_param_name(const ObString& param_name);
     /*add 20151202:e */
    private:
      common::ObStringBuf* name_pool_;
      common::ObString function_name_;
      common::ObString function_context_;
      int32_t param_num_;
      common::ObArray<common::ObString> parameters_;
      /*add wuna [MultiUps][sql_api] 20151202:b */
      ObFunctionPartitionType type_;
      common::ObArray<RangePartitionNode*> range_partition_nodes_;
      /*add 20151202:e */
    };
    /*add wuna [MultiUps][sql_api] 20151202:b */
    inline void ObCreatePartFuncStmt::set_partition_type(ObFunctionPartitionType type)
    {
      type_ = type;
    }
    inline const ObFunctionPartitionType ObCreatePartFuncStmt::get_partition_type()const
    {
      return type_;
    }
    inline int ObCreatePartFuncStmt::add_range_partition_node(RangePartitionNode*& node)
    {
      return range_partition_nodes_.push_back(node);
    }
    inline const RangePartitionNode* ObCreatePartFuncStmt::get_range_partition_node(int32_t index) const
    {
      OB_ASSERT(0 <= index && index < range_partition_nodes_.count());
      return range_partition_nodes_.at(index);
    }
    inline const int64_t ObCreatePartFuncStmt::get_range_partition_nodes_size() const
    {
      return range_partition_nodes_.count();
    }
    /*add 20151202:e */
    inline void ObCreatePartFuncStmt::set_parameters_num(const int32_t &param_num)
    {
      param_num_ = param_num;
    }

    inline int32_t ObCreatePartFuncStmt::get_parameters_num() const
    {
      return param_num_;
    }

    inline const common::ObString& ObCreatePartFuncStmt::get_function_name() const
    {
      return function_name_;
    }

    inline const int ObCreatePartFuncStmt::get_parameter(int64_t idx, common::ObString& obj) const
    {
      return parameters_.at(idx, obj);
    }

    inline const common::ObString& ObCreatePartFuncStmt::get_func_context() const
    {
      return function_context_;
    }

    inline const common::ObArray<common::ObString>& ObCreatePartFuncStmt::get_all_parameters() const
    {
      return parameters_;
    }
    /*add wuna [MultiUps][sql_api] 20151202:b */
    inline int RangePartitionNode::add_partition_name(ObCreatePartFuncStmt* create_part_stmt,const ObString& part_name)
    {
      int ret = OB_SUCCESS;
      if (OB_SUCCESS != (ret = ob_write_string(*(create_part_stmt->name_pool_), part_name, partition_name_)))
      {
        YYSYS_LOG(WARN,"write part_name failed,ret=%d",ret);
      }
      return ret;
    }

    inline int RangePartitionNode::add_partition_value_obj(ObCreatePartFuncStmt* create_part_stmt,const ObObj& src_obj)
    {
      int ret = OB_SUCCESS;
      ObObj dest_obj;
      if(OB_SUCCESS != (ret = ob_write_obj(*(create_part_stmt->name_pool_),src_obj,dest_obj)))
      {
        YYSYS_LOG(WARN,"write obj failed,ret=%d",ret);
      }
      else
      {
        partition_value_obj_.push_back(dest_obj);
      }
      return ret;
    }
    /*add 20151202:e */
  }
}

#endif // OB_CREATE_HASH_FUNC_H
