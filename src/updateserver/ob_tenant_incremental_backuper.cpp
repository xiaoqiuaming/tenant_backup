/**
 * Copyright (C) 2026 YaoBase
 * 
 * Tenant incremental backuper implementation
 */

#include "updateserver/ob_tenant_incremental_backuper.h"
#include "common/ob_malloc.h"
#include "yysys/ob_log.h"
#include "yysys/ob_timeutil.h"

namespace yaobase {
namespace updateserver {

ObTenantIncrementalBackuper::ObTenantIncrementalBackuper()
  : is_inited_(false),
    is_stop_(false),
    current_log_id_(0) {
}

ObTenantIncrementalBackuper::~ObTenantIncrementalBackuper() {
  destroy();
}

int ObTenantIncrementalBackuper::init() {
  int ret = OB_SUCCESS;
  
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    YYSYS_LOG(WARN, "tenant incremental backuper already initialized", K(ret));
  } else {
    if (OB_SUCCESS != (ret = log_filter_.init())) {
      YYSYS_LOG(WARN, "initialize log filter failed", K(ret));
    } else {
      is_inited_ = true;
      is_stop_ = false;
      current_log_id_ = 0;
      
      YYSYS_LOG(INFO, "tenant incremental backuper initialized");
    }
  }
  
  return ret;
}

void ObTenantIncrementalBackuper::destroy() {
  if (is_inited_) {
    is_stop_ = true;
    
    // Wait for background thread to stop
    // In production, would use proper thread join mechanism
    
    log_filter_.destroy();
    backup_tasks_.clear();
    
    is_inited_ = false;
    YYSYS_LOG(INFO, "tenant incremental backuper destroyed");
  }
}

int ObTenantIncrementalBackuper::start_incremental_backup(
    const common::ObIncrBackupParams& params) {
  int ret = OB_SUCCESS;
  
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    YYSYS_LOG(WARN, "tenant incremental backuper not initialized", K(ret));
  } else if (params.tenant_id_ <= 0 || params.backup_set_id_ <= 0) {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(WARN, "invalid incremental backup parameters",
              K(ret), K(params.tenant_id_), K(params.backup_set_id_));
  } else {
    yysys::CThreadGuard guard(&mutex_);
    
    // Check if task already exists
    if (backup_tasks_.find(params.tenant_id_) != backup_tasks_.end()) {
      ret = OB_ENTRY_EXIST;
      YYSYS_LOG(WARN, "incremental backup already running for tenant",
                K(ret), K(params.tenant_id_));
    } else {
      // Create new backup task
      ObIncrementalBackupTask task;
      task.tenant_id_ = params.tenant_id_;
      task.backup_set_id_ = params.backup_set_id_;
      snprintf(task.archive_path_, OB_MAX_URI_LENGTH, "%s", params.archive_path_);
      task.checkpoint_interval_ = params.checkpoint_interval_;
      task.start_log_id_ = current_log_id_;
      task.last_archived_log_id_ = current_log_id_;
      task.last_checkpoint_time_ = yysys::CTimeUtil::getTime();
      task.is_running_ = true;
      
      backup_tasks_[params.tenant_id_] = task;
      
      YYSYS_LOG(INFO, "started incremental backup for tenant",
                K(params.tenant_id_), K(params.backup_set_id_),
                "archive_path", params.archive_path_,
                K(current_log_id_));
    }
  }
  
  return ret;
}

int ObTenantIncrementalBackuper::stop_incremental_backup(int64_t tenant_id) {
  int ret = OB_SUCCESS;
  
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    YYSYS_LOG(WARN, "tenant incremental backuper not initialized", K(ret));
  } else if (tenant_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(WARN, "invalid tenant id", K(ret), K(tenant_id));
  } else {
    yysys::CThreadGuard guard(&mutex_);
    
    std::map<int64_t, ObIncrementalBackupTask>::iterator it = backup_tasks_.find(tenant_id);
    if (it == backup_tasks_.end()) {
      ret = OB_ENTRY_NOT_EXIST;
      YYSYS_LOG(WARN, "incremental backup not found for tenant", K(ret), K(tenant_id));
    } else {
      // Perform final checkpoint before stopping
      do_checkpoint(tenant_id);
      
      backup_tasks_.erase(it);
      
      YYSYS_LOG(INFO, "stopped incremental backup for tenant", K(tenant_id));
    }
  }
  
  return ret;
}

int ObTenantIncrementalBackuper::get_backup_status(int64_t tenant_id,
                                                   int64_t& last_archived_log_id,
                                                   int64_t& last_checkpoint_time) {
  int ret = OB_SUCCESS;
  
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    YYSYS_LOG(WARN, "tenant incremental backuper not initialized", K(ret));
  } else if (tenant_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(WARN, "invalid tenant id", K(ret), K(tenant_id));
  } else {
    yysys::CThreadGuard guard(&mutex_);
    
    std::map<int64_t, ObIncrementalBackupTask>::const_iterator it = backup_tasks_.find(tenant_id);
    if (it == backup_tasks_.end()) {
      ret = OB_ENTRY_NOT_EXIST;
      last_archived_log_id = 0;
      last_checkpoint_time = 0;
    } else {
      last_archived_log_id = it->second.last_archived_log_id_;
      last_checkpoint_time = it->second.last_checkpoint_time_;
    }
  }
  
  return ret;
}

void ObTenantIncrementalBackuper::run(yysys::CThread* thread, void* arg) {
  UNUSED(thread);
  UNUSED(arg);
  
  YYSYS_LOG(INFO, "tenant incremental backuper thread started");
  
  const int64_t LOOP_INTERVAL_US = 100000;  // 100ms
  
  while (!is_stop_) {
    int ret = OB_SUCCESS;
    
    // Get next log entry with timeout
    common::ObLogEntry log_entry;
    ret = get_next_log_entry(log_entry, LOOP_INTERVAL_US);
    
    if (OB_SUCCESS == ret) {
      // Process log entry
      if (OB_SUCCESS != (ret = process_log_entry(log_entry))) {
        YYSYS_LOG(WARN, "process log entry failed", K(ret), "log_id", current_log_id_);
      }
    } else if (OB_TIMEOUT == ret) {
      // Normal timeout, continue
    } else {
      YYSYS_LOG(WARN, "get next log entry failed", K(ret));
      usleep(LOOP_INTERVAL_US);
    }
    
    // Check if checkpoint is needed
    if (need_checkpoint()) {
      yysys::CThreadGuard guard(&mutex_);
      for (std::map<int64_t, ObIncrementalBackupTask>::iterator it = backup_tasks_.begin();
           it != backup_tasks_.end(); ++it) {
        do_checkpoint(it->first);
      }
    }
  }
  
  YYSYS_LOG(INFO, "tenant incremental backuper thread stopped");
}

int ObTenantIncrementalBackuper::subscribe_commit_log(int64_t start_log_id) {
  int ret = OB_SUCCESS;
  
  // TODO: Subscribe to UpdateServer's commit log stream
  // This requires integration with ob_ups_log_mgr or similar
  
  current_log_id_ = start_log_id;
  YYSYS_LOG(INFO, "subscribed to commit log", K(start_log_id));
  
  return ret;
}

int ObTenantIncrementalBackuper::process_log_entry(const common::ObLogEntry& log_entry) {
  int ret = OB_SUCCESS;
  
  yysys::CThreadGuard guard(&mutex_);
  
  // Process log for each active tenant backup task
  for (std::map<int64_t, ObIncrementalBackupTask>::iterator it = backup_tasks_.begin();
       it != backup_tasks_.end(); ++it) {
    int64_t tenant_id = it->first;
    
    // Check if log belongs to this tenant
    if (log_filter_.is_tenant_log(log_entry, tenant_id)) {
      // Archive log data
      const char* log_data = log_entry.buf_;
      int64_t log_data_len = log_entry.data_len_;
      
      if (OB_SUCCESS != (ret = archive_log_data(tenant_id, log_data, log_data_len,
                                                current_log_id_))) {
        YYSYS_LOG(WARN, "archive log data failed", K(ret), K(tenant_id), K(current_log_id_));
      } else {
        it->second.last_archived_log_id_ = current_log_id_;
      }
    }
  }
  
  current_log_id_++;
  
  return ret;
}

int ObTenantIncrementalBackuper::archive_log_data(int64_t tenant_id,
                                                  const char* log_data,
                                                  int64_t log_data_len,
                                                  int64_t log_id) {
  int ret = OB_SUCCESS;
  
  // TODO: Write log data to backup storage
  // This requires:
  // 1. Open archive file for tenant (create new file periodically)
  // 2. Append log data with metadata (log_id, timestamp)
  // 3. Compress if configured
  // 4. Flush to storage
  
  YYSYS_LOG(DEBUG, "archive log data (placeholder)",
            K(tenant_id), K(log_id), K(log_data_len));
  
  return ret;
}

int ObTenantIncrementalBackuper::do_checkpoint(int64_t tenant_id) {
  int ret = OB_SUCCESS;
  
  std::map<int64_t, ObIncrementalBackupTask>::iterator it = backup_tasks_.find(tenant_id);
  if (it != backup_tasks_.end()) {
    // TODO: Write checkpoint metadata to backup storage
    // This includes:
    // 1. Last archived log_id
    // 2. Current timestamp
    // 3. Archive file list
    
    it->second.last_checkpoint_time_ = yysys::CTimeUtil::getTime();
    
    YYSYS_LOG(INFO, "checkpoint performed",
              K(tenant_id),
              "last_archived_log_id", it->second.last_archived_log_id_,
              "checkpoint_time", it->second.last_checkpoint_time_);
  }
  
  return ret;
}

bool ObTenantIncrementalBackuper::need_checkpoint() {
  yysys::CThreadGuard guard(&mutex_);
  
  int64_t current_time = yysys::CTimeUtil::getTime();
  
  for (std::map<int64_t, ObIncrementalBackupTask>::const_iterator it = backup_tasks_.begin();
       it != backup_tasks_.end(); ++it) {
    int64_t interval_us = it->second.checkpoint_interval_ * 1000000;
    int64_t elapsed = current_time - it->second.last_checkpoint_time_;
    
    if (elapsed >= interval_us) {
      return true;
    }
  }
  
  return false;
}

int ObTenantIncrementalBackuper::get_next_log_entry(common::ObLogEntry& log_entry,
                                                    int64_t timeout_us) {
  int ret = OB_SUCCESS;
  
  // TODO: Read next log entry from UpdateServer's commit log
  // This requires integration with commit log reader infrastructure
  
  UNUSED(log_entry);
  UNUSED(timeout_us);
  
  // Placeholder: simulate timeout
  usleep(timeout_us);
  ret = OB_TIMEOUT;
  
  return ret;
}

}  // namespace updateserver
}  // namespace yaobase
