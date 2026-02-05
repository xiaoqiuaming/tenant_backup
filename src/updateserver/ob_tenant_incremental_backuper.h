/**
 * Copyright (C) 2026 YaoBase
 * 
 * Tenant incremental backuper
 * Continuously backs up commit log for a tenant to backup storage
 */

#ifndef YAOBASE_UPDATESERVER_OB_TENANT_INCREMENTAL_BACKUPER_H_
#define YAOBASE_UPDATESERVER_OB_TENANT_INCREMENTAL_BACKUPER_H_

#include "common/ob_define.h"
#include "common/ob_tenant_backup_msg.h"
#include "updateserver/ob_tenant_log_filter.h"
#include "yysys/ob_runnable.h"
#include "yysys/ob_mutex.h"
#include <map>

namespace yaobase {
namespace updateserver {

/**
 * Incremental backup task for a single tenant
 */
struct ObIncrementalBackupTask {
  int64_t tenant_id_;               // Tenant ID
  int64_t backup_set_id_;           // Base backup set ID
  char archive_path_[OB_MAX_URI_LENGTH];  // Archive destination path
  int64_t checkpoint_interval_;     // Checkpoint interval (seconds)
  int64_t start_log_id_;            // Starting log ID
  int64_t last_archived_log_id_;    // Last successfully archived log ID
  int64_t last_checkpoint_time_;    // Last checkpoint timestamp
  bool is_running_;                 // Is task running
  
  ObIncrementalBackupTask()
    : tenant_id_(0), backup_set_id_(0),
      checkpoint_interval_(60),
      start_log_id_(0), last_archived_log_id_(0),
      last_checkpoint_time_(0), is_running_(false) {
    archive_path_[0] = '\0';
  }
};

/**
 * Tenant incremental backuper
 * Background daemon that continuously archives commit log for tenants
 */
class ObTenantIncrementalBackuper : public yysys::CDefaultRunnable {
public:
  ObTenantIncrementalBackuper();
  ~ObTenantIncrementalBackuper();

  int init();
  void destroy();
  bool is_inited() const { return is_inited_; }

  /**
   * Start incremental backup for a tenant
   * 
   * @param params Incremental backup parameters
   * @return OB_SUCCESS on success
   */
  int start_incremental_backup(const common::ObIncrBackupParams& params);

  /**
   * Stop incremental backup for a tenant
   * 
   * @param tenant_id Tenant ID
   * @return OB_SUCCESS on success
   */
  int stop_incremental_backup(int64_t tenant_id);

  /**
   * Get incremental backup status for a tenant
   * 
   * @param tenant_id Tenant ID
   * @param last_archived_log_id Output last archived log ID
   * @param last_checkpoint_time Output last checkpoint timestamp
   * @return OB_SUCCESS on success
   */
  int get_backup_status(int64_t tenant_id,
                       int64_t& last_archived_log_id,
                       int64_t& last_checkpoint_time);

  /**
   * Thread run function
   * Continuously archives commit logs for active tenants
   */
  void run(yysys::CThread* thread, void* arg) override;

private:
  /**
   * Subscribe to commit log stream
   * 
   * @param start_log_id Starting log ID
   * @return OB_SUCCESS on success
   */
  int subscribe_commit_log(int64_t start_log_id);

  /**
   * Process one log entry
   * 
   * @param log_entry Commit log entry
   * @return OB_SUCCESS on success
   */
  int process_log_entry(const common::ObLogEntry& log_entry);

  /**
   * Archive log data for a tenant
   * 
   * @param tenant_id Tenant ID
   * @param log_data Log data to archive
   * @param log_data_len Log data length
   * @param log_id Log ID
   * @return OB_SUCCESS on success
   */
  int archive_log_data(int64_t tenant_id,
                      const char* log_data,
                      int64_t log_data_len,
                      int64_t log_id);

  /**
   * Perform checkpoint for a tenant
   * Updates archived log progress metadata
   * 
   * @param tenant_id Tenant ID
   * @return OB_SUCCESS on success
   */
  int do_checkpoint(int64_t tenant_id);

  /**
   * Check if checkpoint is needed for any tenant
   * 
   * @return true if checkpoint should be performed
   */
  bool need_checkpoint();

  /**
   * Get next log entry from commit log
   * 
   * @param log_entry Output log entry
   * @param timeout_us Timeout in microseconds
   * @return OB_SUCCESS on success, OB_TIMEOUT on timeout
   */
  int get_next_log_entry(common::ObLogEntry& log_entry, int64_t timeout_us);

private:
  bool is_inited_;
  volatile bool is_stop_;
  
  // Active backup tasks: tenant_id -> task
  std::map<int64_t, ObIncrementalBackupTask> backup_tasks_;
  
  // Log filter for tenant identification
  ObTenantLogFilter log_filter_;
  
  // Current log position
  int64_t current_log_id_;
  
  // Thread safety
  mutable yysys::CThreadMutex mutex_;
  
  DISALLOW_COPY_AND_ASSIGN(ObTenantIncrementalBackuper);
};

}  // namespace updateserver
}  // namespace yaobase

#endif  // YAOBASE_UPDATESERVER_OB_TENANT_INCREMENTAL_BACKUPER_H_
