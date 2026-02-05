/**
 * Copyright (C) 2026 YaoBase
 * 
 * Log restore applier
 * Applies rewritten logs to standby tenant in strict order
 */

#ifndef YAOBASE_ROOTSERVER_OB_LOG_RESTORE_APPLIER_H_
#define YAOBASE_ROOTSERVER_OB_LOG_RESTORE_APPLIER_H_

#include "common/ob_define.h"
#include "common/ob_reordered_log_queue.h"
#include "yysys/ob_runnable.h"
#include "yysys/ob_mutex.h"

namespace yaobase {
namespace rootserver {

/**
 * Log restore applier
 * Applies rewritten logs from reordered queue to standby tenant
 */
class ObLogRestoreApplier : public yysys::CDefaultRunnable {
public:
  ObLogRestoreApplier();
  ~ObLogRestoreApplier();

  int init(int64_t dest_tenant_id, common::ObReorderedLogQueue* input_queue);
  void destroy();
  bool is_inited() const { return is_inited_; }

  /**
   * Start applier thread
   */
  int start();

  /**
   * Stop applier thread
   */
  void stop();

  /**
   * Get current applied log ID
   */
  int64_t get_applied_log_id() const { return applied_log_id_; }

  /**
   * Get total applied log count
   */
  int64_t get_applied_count() const { return applied_count_; }

  /**
   * Thread run function
   */
  void run(yysys::CThread* thread, void* arg) override;

private:
  /**
   * Apply a single rewritten log entry
   * 
   * @param log_entry Rewritten log entry
   * @return OB_SUCCESS on success
   */
  int apply_log_entry(const common::ObRewrittenLogEntry& log_entry);

  /**
   * Handle transaction commit
   * 
   * @param log_entry Commit log entry
   * @return OB_SUCCESS on success
   */
  int handle_transaction_commit(const common::ObRewrittenLogEntry& log_entry);

  /**
   * Apply mutator to standby tenant
   * 
   * @param log_entry Mutator log entry
   * @return OB_SUCCESS on success
   */
  int apply_mutator(const common::ObRewrittenLogEntry& log_entry);

private:
  bool is_inited_;
  volatile bool is_stop_;
  int64_t dest_tenant_id_;
  
  common::ObReorderedLogQueue* input_queue_;
  
  // Apply progress tracking
  int64_t applied_log_id_;
  int64_t applied_count_;
  
  // Thread
  yysys::CThread* thread_;
  
  mutable yysys::CThreadMutex mutex_;

  DISALLOW_COPY_AND_ASSIGN(ObLogRestoreApplier);
};

}  // namespace rootserver
}  // namespace yaobase

#endif  // YAOBASE_ROOTSERVER_OB_LOG_RESTORE_APPLIER_H_
