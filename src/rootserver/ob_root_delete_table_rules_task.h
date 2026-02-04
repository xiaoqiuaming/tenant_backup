/* add wuna [MultiUPS] [sql_api] 20160421*/

#ifndef OB_ROOT_DELETE_TABLE_RULES_TASK_H
#define OB_ROOT_DELETE_TABLE_RULES_TASK_H

#include "common/ob_timer.h"
#include "common/ob_server.h"


namespace oceanbase
{
  namespace rootserver
  {
    class ObRootWorker;
    class ObRootTableRulesDeleteTask : public common::ObTimerTask
    {
    public:
      ObRootTableRulesDeleteTask() :worker_(NULL)
      {}
      virtual ~ObRootTableRulesDeleteTask()
      {}
      int init(ObRootWorker *worker);
      int execute_delete_table_rules(const common::ObServer& ms_server,int64_t table_id,int64_t start_version);
      virtual void runTimerTask(void);
    private:
      ObRootWorker* worker_;
      const static int64_t TIMEOUT = 3000000; // 3s
      const static uint64_t TABLE_INTERVAL = 50;  //add yuanzp 20170922
    };
  }
}


#endif // OB_ROOT_DELETE_TABLE_RULES_TASK_H
