#include "sql/ob_optimizer.h"
#include "sql/ob_optimizer_logical.h"
#include "sql/ob_multi_logic_plan.h"

namespace oceanbase
{
namespace sql
{
    int ObOptimizer::optimizer(ResultPlan &result_plan,ObResultSet &result,const ObSchemaManagerV2 *schema)
    {
        int ret = OB_SUCCESS;
        if(schema != NULL)
        {
            ret = standard_optimizer(result_plan,result,schema);
        }
        return ret;
    }

    int ObOptimizer::standard_optimizer(ResultPlan &result_plan,ObResultSet &result,const ObSchemaManagerV2 *schema)
    {
        int ret = OB_SUCCESS;

        ret = logical_optimizer(result_plan,result,schema);

        return ret;
    }

    int ObOptimizer::logical_optimizer(ResultPlan &result_plan,ObResultSet &result,const ObSchemaManagerV2 *schema)
    {
        int ret = OB_SUCCESS;
        ObMultiLogicPlan *logical_plans = static_cast<ObMultiLogicPlan*>(result_plan.plan_tree_);
        ObLogicalPlan *logical_plan = NULL;
        for(int32_t i=0;ret == OB_SUCCESS && i< logical_plans->size();i++)
        {
            logical_plan = logical_plans->at(i);
            ret = ObOptimizerLogical::pull_up_subqueries(logical_plan,result,result_plan);
            if(ret == OB_SUCCESS)
            {
                ret = ObOptimizerLogical::rule_max_min_eliminate(logical_plan,&result_plan,schema);
            }
        }
        return ret;
    }
}
}
