/**
 * Copyright (C) 2026 YaoBase
 * 
 * Tenant backup and restore message structures
 * Used for communication between backup/restore components
 */

#ifndef YAOBASE_COMMON_OB_TENANT_BACKUP_MSG_H_
#define YAOBASE_COMMON_OB_TENANT_BACKUP_MSG_H_

#include "common/ob_define.h"
#include "common/ob_server.h"
#include "common/ob_range.h"
#include "common/ob_array.h"
#include "common/serialization.h"

namespace yaobase {
namespace common {

/**
 * Backup task status enumeration
 */
enum ObBackupTaskStatus {
  BACKUP_INIT = 0,           // Initialization
  BACKUP_PREPARING,          // Preparing phase
  BACKUP_DOING,              // Executing
  BACKUP_VERIFYING,          // Verifying
  BACKUP_COMPLETED,          // Completed
  BACKUP_FAILED,             // Failed
  BACKUP_CANCELLED           // Cancelled
};

/**
 * Restore task status enumeration
 */
enum ObRestoreTaskStatus {
  RESTORE_INIT = 0,          // Initialization
  RESTORE_PREPARING,         // Preparing standby tenant
  RESTORE_BASELINE,          // Restoring baseline
  RESTORE_INCREMENTAL,       // Restoring incremental
  RESTORE_VERIFYING,         // Verifying data
  RESTORE_COMPLETED,         // Completed
  RESTORE_FAILED,            // Failed
  RESTORE_CANCELLED          // Cancelled
};

/**
 * Tablet backup information
 */
struct ObTabletBackupInfo {
  ObNewRange range_;                     // Tablet range
  int64_t sstable_count_;                // SSTable count
  ObArray<int64_t> sstable_ids_;         // SSTable ID list
  int64_t data_size_;                    // Data size in bytes
  uint64_t checksum_;                    // Checksum

  ObTabletBackupInfo() 
    : sstable_count_(0), data_size_(0), checksum_(0) {}

  NEED_SERIALIZE_AND_DESERIALIZE;
};

/**
 * Schema information for backup
 */
struct ObSchemaBackupInfo {
  uint64_t table_id_;                    // Table ID
  char table_name_[OB_MAX_TABLE_NAME_LENGTH];  // Table name
  int64_t schema_version_;               // Schema version

  ObSchemaBackupInfo() 
    : table_id_(OB_INVALID_ID), schema_version_(0) {
    table_name_[0] = '\0';
  }

  NEED_SERIALIZE_AND_DESERIALIZE;
};

/**
 * Baseline backup manifest
 * Contains metadata for a complete baseline backup set
 */
struct ObBaselineBackupManifest {
  int64_t backup_set_id_;                // Backup set ID
  int64_t tenant_id_;                    // Tenant ID
  int64_t frozen_version_;               // Frozen version number
  int64_t backup_start_time_;            // Backup start timestamp (us)
  int64_t backup_end_time_;              // Backup end timestamp (us)
  int64_t data_size_;                    // Total backup data size (bytes)
  int64_t tablet_count_;                 // Number of tablets
  ObArray<ObTabletBackupInfo> tablet_list_;  // Tablet metadata list
  ObArray<ObSchemaBackupInfo> schema_list_;  // Schema information list
  char backup_path_[OB_MAX_URI_LENGTH];  // Backup storage path

  ObBaselineBackupManifest()
    : backup_set_id_(0), tenant_id_(0), frozen_version_(0),
      backup_start_time_(0), backup_end_time_(0),
      data_size_(0), tablet_count_(0) {
    backup_path_[0] = '\0';
  }

  NEED_SERIALIZE_AND_DESERIALIZE;
};

/**
 * Incremental backup manifest
 * Contains metadata for incremental log archives
 */
struct ObIncrementalBackupManifest {
  int64_t backup_set_id_;                // Base backup set ID
  int64_t tenant_id_;                    // Tenant ID
  int64_t start_log_id_;                 // Start log ID
  int64_t end_log_id_;                   // End log ID
  int64_t start_timestamp_;              // Start timestamp (us)
  int64_t end_timestamp_;                // End timestamp (us)
  int64_t log_file_count_;               // Number of log files
  int64_t total_size_;                   // Total size (bytes)
  char archive_path_[OB_MAX_URI_LENGTH]; // Archive storage path

  ObIncrementalBackupManifest()
    : backup_set_id_(0), tenant_id_(0),
      start_log_id_(0), end_log_id_(0),
      start_timestamp_(0), end_timestamp_(0),
      log_file_count_(0), total_size_(0) {
    archive_path_[0] = '\0';
  }

  NEED_SERIALIZE_AND_DESERIALIZE;
};

/**
 * Tenant backup task
 * Represents a backup operation in progress
 */
struct ObTenantBackupTask {
  int64_t task_id_;                      // Task ID
  int64_t tenant_id_;                    // Tenant ID
  int64_t backup_set_id_;                // Backup set ID
  ObBackupTaskStatus status_;            // Task status
  int64_t start_time_;                   // Start timestamp (us)
  int64_t end_time_;                     // End timestamp (us)
  int64_t total_tablets_;                // Total tablets to backup
  int64_t finished_tablets_;             // Finished tablets
  int64_t data_size_;                    // Total data size (bytes)
  int error_code_;                       // Error code if failed
  char backup_path_[OB_MAX_URI_LENGTH];  // Backup storage path
  char error_msg_[OB_MAX_ERROR_MSG_LEN]; // Error message

  ObTenantBackupTask()
    : task_id_(0), tenant_id_(0), backup_set_id_(0),
      status_(BACKUP_INIT), start_time_(0), end_time_(0),
      total_tablets_(0), finished_tablets_(0), data_size_(0),
      error_code_(OB_SUCCESS) {
    backup_path_[0] = '\0';
    error_msg_[0] = '\0';
  }

  bool is_finished() const {
    return status_ == BACKUP_COMPLETED || 
           status_ == BACKUP_FAILED ||
           status_ == BACKUP_CANCELLED;
  }

  NEED_SERIALIZE_AND_DESERIALIZE;
};

/**
 * Tenant restore task
 * Represents a restore operation in progress
 */
struct ObTenantRestoreTask {
  int64_t task_id_;                      // Task ID
  int64_t src_tenant_id_;                // Source tenant ID
  int64_t dest_tenant_id_;               // Destination (standby) tenant ID
  int64_t backup_set_id_;                // Backup set ID to restore
  int64_t restore_timestamp_;            // Target restore timestamp (PITR)
  ObRestoreTaskStatus status_;           // Task status
  int64_t start_time_;                   // Start timestamp (us)
  int64_t end_time_;                     // End timestamp (us)
  int64_t baseline_progress_;            // Baseline restore progress (%)
  int64_t incremental_progress_;         // Incremental restore progress (%)
  int64_t restored_log_count_;           // Number of logs restored
  int64_t last_applied_log_id_;          // Last applied log ID
  int error_code_;                       // Error code if failed
  char backup_path_[OB_MAX_URI_LENGTH];  // Backup source path
  char error_msg_[OB_MAX_ERROR_MSG_LEN]; // Error message

  ObTenantRestoreTask()
    : task_id_(0), src_tenant_id_(0), dest_tenant_id_(0),
      backup_set_id_(0), restore_timestamp_(0),
      status_(RESTORE_INIT), start_time_(0), end_time_(0),
      baseline_progress_(0), incremental_progress_(0),
      restored_log_count_(0), last_applied_log_id_(0),
      error_code_(OB_SUCCESS) {
    backup_path_[0] = '\0';
    error_msg_[0] = '\0';
  }

  bool is_finished() const {
    return status_ == RESTORE_COMPLETED ||
           status_ == RESTORE_FAILED ||
           status_ == RESTORE_CANCELLED;
  }

  NEED_SERIALIZE_AND_DESERIALIZE;
};

/**
 * Backup parameters for starting a backup task
 */
struct ObBackupParams {
  int64_t tenant_id_;                    // Tenant ID
  char backup_path_[OB_MAX_URI_LENGTH];  // Backup destination path
  int64_t frozen_version_;               // Target frozen version (0 = latest)
  int parallel_degree_;                  // Parallelism
  bool enable_compression_;              // Enable compression
  char compression_algorithm_[32];       // Compression algorithm (e.g., "lz4")

  ObBackupParams()
    : tenant_id_(0), frozen_version_(0),
      parallel_degree_(1), enable_compression_(false) {
    backup_path_[0] = '\0';
    compression_algorithm_[0] = '\0';
  }

  NEED_SERIALIZE_AND_DESERIALIZE;
};

/**
 * Incremental backup parameters
 */
struct ObIncrBackupParams {
  int64_t tenant_id_;                    // Tenant ID
  int64_t backup_set_id_;                // Base backup set ID
  char archive_path_[OB_MAX_URI_LENGTH]; // Archive destination path
  int64_t checkpoint_interval_;          // Checkpoint interval (seconds)

  ObIncrBackupParams()
    : tenant_id_(0), backup_set_id_(0),
      checkpoint_interval_(60) {
    archive_path_[0] = '\0';
  }

  NEED_SERIALIZE_AND_DESERIALIZE;
};

/**
 * Backup status query result
 */
struct ObBackupStatus {
  int64_t tenant_id_;                    // Tenant ID
  int64_t baseline_backup_set_count_;    // Number of baseline backups
  int64_t latest_backup_set_id_;         // Latest backup set ID
  int64_t latest_backup_time_;           // Latest backup timestamp
  bool incremental_backup_running_;      // Is incremental backup running
  int64_t last_archived_log_id_;         // Last archived log ID
  int64_t last_archive_timestamp_;       // Last archive timestamp

  ObBackupStatus()
    : tenant_id_(0), baseline_backup_set_count_(0),
      latest_backup_set_id_(0), latest_backup_time_(0),
      incremental_backup_running_(false),
      last_archived_log_id_(0), last_archive_timestamp_(0) {}

  NEED_SERIALIZE_AND_DESERIALIZE;
};

}  // namespace common
}  // namespace yaobase

#endif  // YAOBASE_COMMON_OB_TENANT_BACKUP_MSG_H_
