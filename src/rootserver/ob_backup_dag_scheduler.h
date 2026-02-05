/**
 * Copyright (C) 2026 YaoBase
 * 
 * Backup DAG scheduler
 * Manages complex backup/restore tasks with dependency graphs
 */

#ifndef YAOBASE_ROOTSERVER_OB_BACKUP_DAG_SCHEDULER_H_
#define YAOBASE_ROOTSERVER_OB_BACKUP_DAG_SCHEDULER_H_

#include "common/ob_define.h"
#include "common/ob_array.h"
#include "yysys/ob_runnable.h"
#include "yysys/ob_mutex.h"
#include <map>
#include <set>

namespace yaobase {
namespace rootserver {

/**
 * DAG task status
 */
enum ObDagTaskStatus {
  DAG_TASK_PENDING = 0,    // Waiting for dependencies
  DAG_TASK_READY,          // Ready to execute
  DAG_TASK_RUNNING,        // Currently executing
  DAG_TASK_COMPLETED,      // Completed successfully
  DAG_TASK_FAILED,         // Failed
  DAG_TASK_CANCELLED       // Cancelled
};

/**
 * DAG task interface
 * All backup/restore tasks implement this interface
 */
class ObDagTask {
public:
  ObDagTask() : task_id_(0), status_(DAG_TASK_PENDING), error_code_(OB_SUCCESS) {}
  virtual ~ObDagTask() {}

  /**
   * Execute the task
   * @return OB_SUCCESS on success
   */
  virtual int execute() = 0;

  /**
   * Get task description for logging
   */
  virtual const char* get_task_desc() const = 0;

  int64_t get_task_id() const { return task_id_; }
  void set_task_id(int64_t task_id) { task_id_ = task_id; }

  ObDagTaskStatus get_status() const { return status_; }
  void set_status(ObDagTaskStatus status) { status_ = status; }

  int get_error_code() const { return error_code_; }
  void set_error_code(int error_code) { error_code_ = error_code; }

private:
  int64_t task_id_;
  ObDagTaskStatus status_;
  int error_code_;
};

/**
 * DAG (Directed Acyclic Graph) for task dependencies
 */
class ObBackupDag {
public:
  ObBackupDag();
  ~ObBackupDag();

  int init(int64_t dag_id);
  void destroy();

  /**
   * Add a task to the DAG
   * @param task Task to add (ownership transferred)
   * @return task_id
   */
  int64_t add_task(ObDagTask* task);

  /**
   * Add dependency: task must complete before dependent_task can start
   * @param task_id Task that must complete first
   * @param dependent_task_id Task that depends on task_id
   * @return OB_SUCCESS on success
   */
  int add_dependency(int64_t task_id, int64_t dependent_task_id);

  /**
   * Get all tasks that are ready to execute (no pending dependencies)
   * @param ready_tasks Output array of ready task IDs
   * @return OB_SUCCESS on success
   */
  int get_ready_tasks(common::ObArray<int64_t>& ready_tasks);

  /**
   * Mark a task as completed
   * @param task_id Task ID
   * @param error_code Error code (OB_SUCCESS if successful)
   * @return OB_SUCCESS on success
   */
  int mark_task_completed(int64_t task_id, int error_code);

  /**
   * Check if DAG is complete (all tasks finished)
   */
  bool is_complete() const;

  /**
   * Check if DAG has any failed tasks
   */
  bool has_failed_tasks() const;

  /**
   * Get task by ID
   */
  ObDagTask* get_task(int64_t task_id);

  int64_t get_dag_id() const { return dag_id_; }

private:
  int64_t dag_id_;
  int64_t next_task_id_;
  
  // Tasks: task_id -> task
  std::map<int64_t, ObDagTask*> tasks_;
  
  // Dependencies: task_id -> set of dependent task_ids
  std::map<int64_t, std::set<int64_t> > dependencies_;
  
  // Reverse dependencies: dependent_task_id -> set of task_ids it depends on
  std::map<int64_t, std::set<int64_t> > reverse_dependencies_;
};

/**
 * DAG scheduler
 * Executes DAG tasks with parallelism and dependency management
 */
class ObBackupDagScheduler : public yysys::CDefaultRunnable {
public:
  ObBackupDagScheduler();
  ~ObBackupDagScheduler();

  int init(int worker_thread_count = 4);
  void destroy();
  bool is_inited() const { return is_inited_; }

  /**
   * Submit a DAG for execution
   * @param dag DAG to execute (ownership transferred)
   * @return DAG ID
   */
  int64_t submit_dag(ObBackupDag* dag);

  /**
   * Cancel a DAG
   * @param dag_id DAG ID
   * @return OB_SUCCESS on success
   */
  int cancel_dag(int64_t dag_id);

  /**
   * Get DAG status
   * @param dag_id DAG ID
   * @param is_complete Output whether DAG is complete
   * @param has_failed Output whether DAG has failed tasks
   * @return OB_SUCCESS on success, OB_ENTRY_NOT_EXIST if not found
   */
  int get_dag_status(int64_t dag_id, bool& is_complete, bool& has_failed);

  /**
   * Wait for DAG completion
   * @param dag_id DAG ID
   * @param timeout_us Timeout in microseconds
   * @return OB_SUCCESS on success, OB_TIMEOUT on timeout
   */
  int wait_dag(int64_t dag_id, int64_t timeout_us);

  /**
   * Thread run function
   */
  void run(yysys::CThread* thread, void* arg) override;

private:
  /**
   * Schedule tasks from pending DAGs
   */
  int schedule_tasks();

  /**
   * Execute a single task
   */
  int execute_task(int64_t dag_id, int64_t task_id);

private:
  bool is_inited_;
  volatile bool is_stop_;
  int worker_thread_count_;
  
  // Active DAGs: dag_id -> dag
  std::map<int64_t, ObBackupDag*> dags_;
  
  // Next DAG ID
  int64_t next_dag_id_;
  
  // Thread safety
  mutable yysys::CThreadMutex mutex_;
  yysys::CThreadCond dag_complete_cond_;
  
  DISALLOW_COPY_AND_ASSIGN(ObBackupDagScheduler);
};

}  // namespace rootserver
}  // namespace yaobase

#endif  // YAOBASE_ROOTSERVER_OB_BACKUP_DAG_SCHEDULER_H_
