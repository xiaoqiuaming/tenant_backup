/**
 * Copyright (C) 2026 YaoBase
 * 
 * Log restore worker
 * Parallel worker threads that rewrite commit logs for tenant restore
 */

#ifndef YAOBASE_ROOTSERVER_OB_LOG_RESTORE_WORKER_H_
#define YAOBASE_ROOTSERVER_OB_LOG_RESTORE_WORKER_H_

#include "common/ob_define.h"
#include "common/ob_log_entry.h"
#include "common/ob_tenant_schema_rewriter.h"
#include "common/ob_reordered_log_queue.h"
#include "yysys/ob_runnable.h"
#include "yysys/ob_mutex.h"
#include "yysys/ob_queue_thread.h"
#include <queue>

namespace yaobase {
namespace rootserver {

/**
 * Input log entry for rewriting
 */
struct ObRestoreLogInput {
  common::ObLogEntry log_entry_;
  int64_t log_id_;
  
  ObRestoreLogInput() : log_id_(0) {}
};

/**
 * Single log restore worker thread
 * Deserializes, rewrites schema, and serializes log entries
 */
class ObLogRestoreWorkerThread : public yysys::CDefaultRunnable {
public:
  ObLogRestoreWorkerThread();
  ~ObLogRestoreWorkerThread();

  int init(int worker_id,
          const common::ObSchemaMapping* schema_mapping,
          common::ObReorderedLogQueue* output_queue);
  void destroy();

  /**
   * Push input log for processing
   * 
   * @param input Input log entry
   * @return OB_SUCCESS on success
   */
  int push_input(const ObRestoreLogInput& input);

  /**
   * Signal worker to stop
   */
  void stop();

  /**
   * Thread run function
   */
  void run(yysys::CThread* thread, void* arg) override;

private:
  /**
   * Rewrite a single log entry
   * 
   * @param input Input log
   * @param output Output rewritten log
   * @return OB_SUCCESS on success
   */
  int rewrite_log_entry(const ObRestoreLogInput& input,
                       common::ObRewrittenLogEntry& output);

private:
  bool is_inited_;
  volatile bool is_stop_;
  int worker_id_;
  
  const common::ObSchemaMapping* schema_mapping_;
  common::ObReorderedLogQueue* output_queue_;
  
  // Input queue
  std::queue<ObRestoreLogInput> input_queue_;
  mutable yysys::CThreadMutex input_mutex_;
  yysys::CThreadCond input_cond_;

  DISALLOW_COPY_AND_ASSIGN(ObLogRestoreWorkerThread);
};

/**
 * Log restore worker pool
 * Manages multiple worker threads for parallel log rewriting
 */
class ObLogRestoreWorker {
public:
  ObLogRestoreWorker();
  ~ObLogRestoreWorker();

  int init(int worker_count,
          const common::ObSchemaMapping& schema_mapping,
          common::ObReorderedLogQueue& output_queue);
  void destroy();
  bool is_inited() const { return is_inited_; }

  /**
   * Start worker threads
   */
  int start();

  /**
   * Stop worker threads
   */
  void stop();

  /**
   * Submit log entry for parallel rewriting
   * 
   * @param log_entry Log entry to rewrite
   * @param log_id Log ID
   * @return OB_SUCCESS on success
   */
  int submit_log(const common::ObLogEntry& log_entry, int64_t log_id);

  /**
   * Wait for all submitted logs to be processed
   * 
   * @param timeout_us Timeout in microseconds
   * @return OB_SUCCESS on success, OB_TIMEOUT on timeout
   */
  int wait_completion(int64_t timeout_us);

private:
  /**
   * Select next worker for load balancing (round-robin)
   */
  int get_next_worker_id();

private:
  bool is_inited_;
  int worker_count_;
  int next_worker_idx_;
  
  common::ObSchemaMapping schema_mapping_;
  common::ObReorderedLogQueue* output_queue_;
  
  // Worker threads
  ObLogRestoreWorkerThread** workers_;
  yysys::CThread** threads_;
  
  mutable yysys::CThreadMutex mutex_;

  DISALLOW_COPY_AND_ASSIGN(ObLogRestoreWorker);
};

}  // namespace rootserver
}  // namespace yaobase

#endif  // YAOBASE_ROOTSERVER_OB_LOG_RESTORE_WORKER_H_
