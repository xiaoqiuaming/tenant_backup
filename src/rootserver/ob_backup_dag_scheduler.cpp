/**
 * Copyright (C) 2026 YaoBase
 * 
 * Backup DAG scheduler implementation
 */

#include "rootserver/ob_backup_dag_scheduler.h"
#include "common/ob_malloc.h"
#include "yysys/ob_log.h"
#include "yysys/ob_timeutil.h"

namespace yaobase {
namespace rootserver {

//==============================================================================
// ObBackupDag Implementation
//==============================================================================

ObBackupDag::ObBackupDag()
  : dag_id_(0), next_task_id_(1) {
}

ObBackupDag::~ObBackupDag() {
  destroy();
}

int ObBackupDag::init(int64_t dag_id) {
  int ret = OB_SUCCESS;
  
  if (dag_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(WARN, "invalid dag id", K(ret), K(dag_id));
  } else {
    dag_id_ = dag_id;
    next_task_id_ = 1;
  }
  
  return ret;
}

void ObBackupDag::destroy() {
  // Clean up all tasks
  for (std::map<int64_t, ObDagTask*>::iterator it = tasks_.begin();
       it != tasks_.end(); ++it) {
    if (nullptr != it->second) {
      delete it->second;
    }
  }
  
  tasks_.clear();
  dependencies_.clear();
  reverse_dependencies_.clear();
  dag_id_ = 0;
  next_task_id_ = 0;
}

int64_t ObBackupDag::add_task(ObDagTask* task) {
  if (nullptr == task) {
    YYSYS_LOG(WARN, "null task pointer");
    return -1;
  }
  
  int64_t task_id = next_task_id_++;
  task->set_task_id(task_id);
  task->set_status(DAG_TASK_PENDING);
  tasks_[task_id] = task;
  
  YYSYS_LOG(DEBUG, "added task to DAG", K(dag_id_), K(task_id), "desc", task->get_task_desc());
  
  return task_id;
}

int ObBackupDag::add_dependency(int64_t task_id, int64_t dependent_task_id) {
  int ret = OB_SUCCESS;
  
  if (tasks_.find(task_id) == tasks_.end() ||
      tasks_.find(dependent_task_id) == tasks_.end()) {
    ret = OB_ENTRY_NOT_EXIST;
    YYSYS_LOG(WARN, "task not found", K(ret), K(task_id), K(dependent_task_id));
  } else {
    // dependent_task_id depends on task_id
    dependencies_[task_id].insert(dependent_task_id);
    reverse_dependencies_[dependent_task_id].insert(task_id);
    
    YYSYS_LOG(DEBUG, "added dependency", K(dag_id_), K(task_id), K(dependent_task_id));
  }
  
  return ret;
}

int ObBackupDag::get_ready_tasks(common::ObArray<int64_t>& ready_tasks) {
  int ret = OB_SUCCESS;
  
  ready_tasks.clear();
  
  for (std::map<int64_t, ObDagTask*>::iterator it = tasks_.begin();
       it != tasks_.end(); ++it) {
    int64_t task_id = it->first;
    ObDagTask* task = it->second;
    
    if (nullptr == task) {
      continue;
    }
    
    // Check if task is pending
    if (task->get_status() != DAG_TASK_PENDING) {
      continue;
    }
    
    // Check if all dependencies are completed
    bool all_deps_completed = true;
    std::map<int64_t, std::set<int64_t> >::iterator dep_it = reverse_dependencies_.find(task_id);
    if (dep_it != reverse_dependencies_.end()) {
      const std::set<int64_t>& deps = dep_it->second;
      for (std::set<int64_t>::const_iterator dep = deps.begin(); dep != deps.end(); ++dep) {
        ObDagTask* dep_task = tasks_[*dep];
        if (nullptr == dep_task || dep_task->get_status() != DAG_TASK_COMPLETED) {
          all_deps_completed = false;
          break;
        }
      }
    }
    
    if (all_deps_completed) {
      if (OB_SUCCESS != (ret = ready_tasks.push_back(task_id))) {
        YYSYS_LOG(WARN, "push back ready task failed", K(ret), K(task_id));
        break;
      }
    }
  }
  
  return ret;
}

int ObBackupDag::mark_task_completed(int64_t task_id, int error_code) {
  int ret = OB_SUCCESS;
  
  std::map<int64_t, ObDagTask*>::iterator it = tasks_.find(task_id);
  if (it == tasks_.end() || nullptr == it->second) {
    ret = OB_ENTRY_NOT_EXIST;
    YYSYS_LOG(WARN, "task not found", K(ret), K(task_id));
  } else {
    ObDagTask* task = it->second;
    
    if (OB_SUCCESS == error_code) {
      task->set_status(DAG_TASK_COMPLETED);
      YYSYS_LOG(INFO, "task completed", K(dag_id_), K(task_id));
    } else {
      task->set_status(DAG_TASK_FAILED);
      task->set_error_code(error_code);
      YYSYS_LOG(WARN, "task failed", K(dag_id_), K(task_id), K(error_code));
    }
  }
  
  return ret;
}

bool ObBackupDag::is_complete() const {
  for (std::map<int64_t, ObDagTask*>::const_iterator it = tasks_.begin();
       it != tasks_.end(); ++it) {
    if (nullptr == it->second) {
      continue;
    }
    
    ObDagTaskStatus status = it->second->get_status();
    if (status != DAG_TASK_COMPLETED && status != DAG_TASK_FAILED) {
      return false;
    }
  }
  
  return true;
}

bool ObBackupDag::has_failed_tasks() const {
  for (std::map<int64_t, ObDagTask*>::const_iterator it = tasks_.begin();
       it != tasks_.end(); ++it) {
    if (nullptr != it->second && it->second->get_status() == DAG_TASK_FAILED) {
      return true;
    }
  }
  
  return false;
}

ObDagTask* ObBackupDag::get_task(int64_t task_id) {
  std::map<int64_t, ObDagTask*>::iterator it = tasks_.find(task_id);
  return (it != tasks_.end()) ? it->second : nullptr;
}

//==============================================================================
// ObBackupDagScheduler Implementation
//==============================================================================

ObBackupDagScheduler::ObBackupDagScheduler()
  : is_inited_(false),
    is_stop_(false),
    worker_thread_count_(4),
    next_dag_id_(1) {
}

ObBackupDagScheduler::~ObBackupDagScheduler() {
  destroy();
}

int ObBackupDagScheduler::init(int worker_thread_count) {
  int ret = OB_SUCCESS;
  
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    YYSYS_LOG(WARN, "backup dag scheduler already initialized", K(ret));
  } else if (worker_thread_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(WARN, "invalid worker thread count", K(ret), K(worker_thread_count));
  } else {
    worker_thread_count_ = worker_thread_count;
    is_stop_ = false;
    next_dag_id_ = 1;
    
    is_inited_ = true;
    YYSYS_LOG(INFO, "backup dag scheduler initialized", K(worker_thread_count_));
  }
  
  return ret;
}

void ObBackupDagScheduler::destroy() {
  if (is_inited_) {
    is_stop_ = true;
    
    // Clean up all DAGs
    for (std::map<int64_t, ObBackupDag*>::iterator it = dags_.begin();
         it != dags_.end(); ++it) {
      if (nullptr != it->second) {
        delete it->second;
      }
    }
    dags_.clear();
    
    is_inited_ = false;
    YYSYS_LOG(INFO, "backup dag scheduler destroyed");
  }
}

int64_t ObBackupDagScheduler::submit_dag(ObBackupDag* dag) {
  if (!is_inited_) {
    YYSYS_LOG(WARN, "backup dag scheduler not initialized");
    return -1;
  }
  
  if (nullptr == dag) {
    YYSYS_LOG(WARN, "null dag pointer");
    return -1;
  }
  
  yysys::CThreadGuard guard(&mutex_);
  
  int64_t dag_id = next_dag_id_++;
  dag->init(dag_id);
  dags_[dag_id] = dag;
  
  YYSYS_LOG(INFO, "submitted DAG", K(dag_id));
  
  return dag_id;
}

int ObBackupDagScheduler::cancel_dag(int64_t dag_id) {
  int ret = OB_SUCCESS;
  
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    YYSYS_LOG(WARN, "backup dag scheduler not initialized", K(ret));
  } else {
    yysys::CThreadGuard guard(&mutex_);
    
    std::map<int64_t, ObBackupDag*>::iterator it = dags_.find(dag_id);
    if (it == dags_.end()) {
      ret = OB_ENTRY_NOT_EXIST;
      YYSYS_LOG(WARN, "dag not found", K(ret), K(dag_id));
    } else {
      // TODO: Mark all pending/running tasks as cancelled
      YYSYS_LOG(INFO, "cancelled DAG", K(dag_id));
    }
  }
  
  return ret;
}

int ObBackupDagScheduler::get_dag_status(int64_t dag_id, bool& is_complete, bool& has_failed) {
  int ret = OB_SUCCESS;
  
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    YYSYS_LOG(WARN, "backup dag scheduler not initialized", K(ret));
  } else {
    yysys::CThreadGuard guard(&mutex_);
    
    std::map<int64_t, ObBackupDag*>::iterator it = dags_.find(dag_id);
    if (it == dags_.end() || nullptr == it->second) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      is_complete = it->second->is_complete();
      has_failed = it->second->has_failed_tasks();
    }
  }
  
  return ret;
}

int ObBackupDagScheduler::wait_dag(int64_t dag_id, int64_t timeout_us) {
  int ret = OB_SUCCESS;
  
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    YYSYS_LOG(WARN, "backup dag scheduler not initialized", K(ret));
  } else {
    int64_t start_time = yysys::CTimeUtil::getTime();
    
    while (true) {
      bool is_complete = false;
      bool has_failed = false;
      
      if (OB_SUCCESS != (ret = get_dag_status(dag_id, is_complete, has_failed))) {
        break;
      }
      
      if (is_complete) {
        ret = has_failed ? OB_ERROR : OB_SUCCESS;
        break;
      }
      
      int64_t elapsed = yysys::CTimeUtil::getTime() - start_time;
      if (elapsed >= timeout_us) {
        ret = OB_TIMEOUT;
        break;
      }
      
      // Wait a bit before checking again
      usleep(100000);  // 100ms
    }
  }
  
  return ret;
}

void ObBackupDagScheduler::run(yysys::CThread* thread, void* arg) {
  UNUSED(thread);
  UNUSED(arg);
  
  YYSYS_LOG(INFO, "backup dag scheduler thread started");
  
  while (!is_stop_) {
    int ret = schedule_tasks();
    if (OB_SUCCESS != ret) {
      YYSYS_LOG(WARN, "schedule tasks failed", K(ret));
    }
    
    usleep(100000);  // 100ms
  }
  
  YYSYS_LOG(INFO, "backup dag scheduler thread stopped");
}

int ObBackupDagScheduler::schedule_tasks() {
  int ret = OB_SUCCESS;
  
  yysys::CThreadGuard guard(&mutex_);
  
  // Process each active DAG
  for (std::map<int64_t, ObBackupDag*>::iterator it = dags_.begin();
       it != dags_.end(); ++it) {
    int64_t dag_id = it->first;
    ObBackupDag* dag = it->second;
    
    if (nullptr == dag || dag->is_complete()) {
      continue;
    }
    
    // Get ready tasks
    common::ObArray<int64_t> ready_tasks;
    if (OB_SUCCESS != (ret = dag->get_ready_tasks(ready_tasks))) {
      YYSYS_LOG(WARN, "get ready tasks failed", K(ret), K(dag_id));
      continue;
    }
    
    // Execute ready tasks (up to worker_thread_count_ in parallel)
    // For simplicity, execute sequentially here
    // In production, would dispatch to thread pool
    for (int64_t i = 0; i < ready_tasks.count(); ++i) {
      int64_t task_id = ready_tasks.at(i);
      
      if (OB_SUCCESS != (ret = execute_task(dag_id, task_id))) {
        YYSYS_LOG(WARN, "execute task failed", K(ret), K(dag_id), K(task_id));
      }
    }
  }
  
  return ret;
}

int ObBackupDagScheduler::execute_task(int64_t dag_id, int64_t task_id) {
  int ret = OB_SUCCESS;
  
  std::map<int64_t, ObBackupDag*>::iterator dag_it = dags_.find(dag_id);
  if (dag_it == dags_.end() || nullptr == dag_it->second) {
    ret = OB_ENTRY_NOT_EXIST;
    return ret;
  }
  
  ObBackupDag* dag = dag_it->second;
  ObDagTask* task = dag->get_task(task_id);
  
  if (nullptr == task) {
    ret = OB_ENTRY_NOT_EXIST;
    return ret;
  }
  
  // Mark as running
  task->set_status(DAG_TASK_RUNNING);
  
  YYSYS_LOG(INFO, "executing task", K(dag_id), K(task_id), "desc", task->get_task_desc());
  
  // Execute task
  ret = task->execute();
  
  // Mark as completed or failed
  dag->mark_task_completed(task_id, ret);
  
  return ret;
}

}  // namespace rootserver
}  // namespace yaobase
