#ifndef OB_OPTIMIZER_H
#define OB_OPTIMIZER_H 1

#include "sql/ob_result_set.h"
#include "sql/parse_node.h"

namespace oceanbase
{
namespace sql
{
    class ObOptimizer
    {
    public:
        ObOptimizer(){}
        ~ObOptimizer(){}
        static int optimizer(ResultPlan &result_plan,ObResultSet &result,const ObSchemaManagerV2 *schema);

        static int standard_optimizer(ResultPlan &result_plan,ObResultSet &result,const ObSchemaManagerV2 *schema);

        static int logical_optimizer(ResultPlan &result_plan,ObResultSet &result,const ObSchemaManagerV2 *schema);

    private:

    };
}
}

#endif
