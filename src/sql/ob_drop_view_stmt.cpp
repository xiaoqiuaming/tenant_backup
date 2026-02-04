#include "ob_drop_view_stmt.h"
#include "ob_schema_checker.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

ObDropViewStmt::ObDropViewStmt(ObStringBuf *name_pool)
    : ObBasicStmt(ObBasicStmt::T_DROP_VIEW)
{
    name_pool_ = name_pool;
    if_exists_ = false;
}

ObDropViewStmt::~ObDropViewStmt()
{

}

int ObDropViewStmt::add_view_name_id(ResultPlan &result_plan, const ObString &table_name)
{
    int &ret = result_plan.err_stat_.err_code_ = OB_SUCCESS;
    uint64_t table_id = OB_INVALID_ID;
    const ObTableSchema *table_schema = NULL;

    ObSchemaChecker *schema_checker = NULL;
    schema_checker = static_cast<ObSchemaChecker *>(result_plan.schema_checker_);
    if (schema_checker == NULL)
    {
        ret = OB_ERR_SCHEMA_UNSET;
        snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
                 "Schema(s) are not set");
    }

    if (ret == OB_SUCCESS)
    {
        table_schema = schema_checker->get_table_schema(table_name);
        if (table_schema == NULL && !if_exists_)
        {
            ret = OB_ERR_TABLE_UNKNOWN;
            snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
                     "view '%.*s' does not exist", table_name.length(), table_name.ptr());
        }
        else if (table_schema != NULL && table_schema->get_type() != ObTableSchema::VIEW)
        {
            ret = OB_ERR_OBJECT_TYPE;
            snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
                     "'%.*s' is not a view", table_name.length(), table_name.ptr());
        }
        else if (table_schema != NULL)
        {
            table_id = table_schema->get_table_id();
        }
    }

    if (ret == OB_SUCCESS && views_.count() > 0)
    {
        for (int32_t i = 0; i < views_.count(); i++)
        {
            if (views_.at(i) == table_name)
            {
                ret = OB_ERR_TABLE_DUPLICATE;
                snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
                         "Not unique view: '%.*s'", table_name.length(), table_name.ptr());
                break;
            }
        }
    }

    if (ret == OB_SUCCESS)
    {
        ObString str;
        if ((ret = ob_write_string(*name_pool_, table_name, str)) != OB_SUCCESS)
        {
            snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
                     "Make space for %.*s failed", table_name.length(), table_name.ptr());
        }
        else if ((ret = views_.push_back(str)) != OB_SUCCESS)
        {
            snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
                     "Add view %.*s failed", table_name.length(), table_name.ptr());
        }
        else if (OB_SUCCESS != (ret = view_ids_.push_back(table_id)))
        {
            snprintf(result_plan.err_stat_.err_msg_, MAX_ERROR_MSG,
                     "Add view id %lu failed", table_id);
        }
    }

    return ret;
}

void ObDropViewStmt::print(FILE *fp, int32_t level, int32_t index)
{
    UNUSED(index);
    print_indentation(fp, level);
    fprintf(fp, "ObDropViewStmt %d Begin\n",index);
    if (if_exists_)
    {
        print_indentation(fp, level + 1);
        fprintf(fp, "if_exists_ = TRUE\n");
    }
    for (int64_t i = 0; i < views_.count(); i++)
    {
        if(i == 0)
        {
            print_indentation(fp, level + 1);
            fprintf(fp, "Views := %.*s", views_.at(i).length(), views_.at(i).ptr());
        }
        else
        {
            print_indentation(fp, level + 1);
            fprintf(fp, ", %.*s", views_.at(i).length(), views_.at(i).ptr());
        }
    }
    fprintf(fp, "\n");
    print_indentation(fp, level);
    fprintf(fp, "ObDropViewStmt %d End\n",index);
}
