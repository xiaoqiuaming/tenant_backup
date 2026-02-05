/**
 * Copyright (C) 2013-2025 YaoBase Inc. All Rights Reserved.
 * This file defines internal tables for tenant backup and restore metadata persistence.
 * These tables track backup tasks, backup sets, restore tasks, and checkpoints.
 */

#ifndef YAOBASE_COMMON_OB_TENANT_BACKUP_TABLES_H_
#define YAOBASE_COMMON_OB_TENANT_BACKUP_TABLES_H_

#include "common/ob_define.h"
#include "common/ob_string.h"

namespace yaobase
{
namespace common
{

/**
 * Internal table: __all_tenant_backup_task
 * Tracks all backup tasks (baseline and incremental)
 * 
 * Columns:
 * - task_id: Unique backup task identifier
 * - tenant_id: Source tenant ID
 * - task_type: 0=baseline, 1=incremental
 * - status: 0=init, 1=running, 2=success, 3=failed, 4=canceled
 * - backup_dest: Backup storage destination path
 * - start_time: Task start timestamp (microseconds since epoch)
 * - end_time: Task end timestamp (0 if still running)
 * - backup_set_id: Baseline backup set ID (0 for incremental)
 * - start_log_id: Starting commit log ID (for incremental)
 * - end_log_id: Ending commit log ID (for incremental)
 * - progress: Progress percentage (0-100)
 * - error_msg: Error message if failed (varchar 1024)
 */
static const char* ALL_TENANT_BACKUP_TASK_SCHEMA =
  "CREATE TABLE __all_tenant_backup_task ("
  "  task_id BIGINT NOT NULL,"
  "  tenant_id BIGINT NOT NULL,"
  "  task_type INT NOT NULL,"
  "  status INT NOT NULL,"
  "  backup_dest VARCHAR(1024) NOT NULL,"
  "  start_time BIGINT NOT NULL,"
  "  end_time BIGINT DEFAULT 0,"
  "  backup_set_id BIGINT DEFAULT 0,"
  "  start_log_id BIGINT DEFAULT 0,"
  "  end_log_id BIGINT DEFAULT 0,"
  "  progress INT DEFAULT 0,"
  "  error_msg VARCHAR(1024) DEFAULT '',"
  "  PRIMARY KEY (task_id),"
  "  INDEX idx_tenant_status (tenant_id, status),"
  "  INDEX idx_backup_set (backup_set_id)"
  ") ENGINE=InnoDB DEFAULT CHARSET=utf8;";

/**
 * Internal table: __all_tenant_backup_set
 * Tracks baseline backup sets with metadata
 * 
 * Columns:
 * - backup_set_id: Unique backup set identifier
 * - tenant_id: Source tenant ID
 * - backup_type: 0=full, 1=incremental-merge
 * - status: 0=creating, 1=available, 2=deleting, 3=deleted
 * - backup_dest: Backup storage destination
 * - start_time: Backup start timestamp
 * - end_time: Backup end timestamp
 * - frozen_version: Frozen version at backup time
 * - schema_version: Schema version at backup time
 * - data_size: Total data size in bytes
 * - tablet_count: Number of tablets backed up
 * - manifest_path: Path to baseline manifest file
 */
static const char* ALL_TENANT_BACKUP_SET_SCHEMA =
  "CREATE TABLE __all_tenant_backup_set ("
  "  backup_set_id BIGINT NOT NULL,"
  "  tenant_id BIGINT NOT NULL,"
  "  backup_type INT NOT NULL,"
  "  status INT NOT NULL,"
  "  backup_dest VARCHAR(1024) NOT NULL,"
  "  start_time BIGINT NOT NULL,"
  "  end_time BIGINT NOT NULL,"
  "  frozen_version BIGINT NOT NULL,"
  "  schema_version BIGINT NOT NULL,"
  "  data_size BIGINT DEFAULT 0,"
  "  tablet_count INT DEFAULT 0,"
  "  manifest_path VARCHAR(1024) NOT NULL,"
  "  PRIMARY KEY (backup_set_id),"
  "  INDEX idx_tenant_status (tenant_id, status),"
  "  INDEX idx_backup_time (tenant_id, end_time DESC)"
  ") ENGINE=InnoDB DEFAULT CHARSET=utf8;";

/**
 * Internal table: __all_tenant_incremental_checkpoint
 * Tracks incremental backup checkpoints for each tenant
 * 
 * Columns:
 * - tenant_id: Tenant ID
 * - checkpoint_log_id: Last archived commit log ID
 * - checkpoint_time: Checkpoint timestamp
 * - archive_dest: Archive storage destination
 * - archive_file_id: Current archive file ID
 * - archived_log_count: Total number of logs archived
 * - archived_data_size: Total archived data size in bytes
 */
static const char* ALL_TENANT_INCREMENTAL_CHECKPOINT_SCHEMA =
  "CREATE TABLE __all_tenant_incremental_checkpoint ("
  "  tenant_id BIGINT NOT NULL,"
  "  checkpoint_log_id BIGINT NOT NULL,"
  "  checkpoint_time BIGINT NOT NULL,"
  "  archive_dest VARCHAR(1024) NOT NULL,"
  "  archive_file_id BIGINT NOT NULL,"
  "  archived_log_count BIGINT DEFAULT 0,"
  "  archived_data_size BIGINT DEFAULT 0,"
  "  PRIMARY KEY (tenant_id)"
  ") ENGINE=InnoDB DEFAULT CHARSET=utf8;";

/**
 * Internal table: __all_tenant_restore_task
 * Tracks all restore tasks
 * 
 * Columns:
 * - task_id: Unique restore task identifier
 * - src_tenant_id: Source tenant ID
 * - dest_tenant_id: Destination (standby) tenant ID
 * - backup_set_id: Baseline backup set to restore from
 * - restore_timestamp: PITR target timestamp (0 = latest)
 * - status: 0=init, 1=prepare, 2=baseline, 3=incremental, 4=verify, 5=success, 6=failed
 * - backup_dest: Backup storage location
 * - start_time: Restore start timestamp
 * - end_time: Restore end timestamp (0 if still running)
 * - baseline_progress: Baseline restore progress (0-100)
 * - incremental_progress: Incremental restore progress (0-100)
 * - restored_log_id: Last applied commit log ID
 * - error_msg: Error message if failed
 */
static const char* ALL_TENANT_RESTORE_TASK_SCHEMA =
  "CREATE TABLE __all_tenant_restore_task ("
  "  task_id BIGINT NOT NULL,"
  "  src_tenant_id BIGINT NOT NULL,"
  "  dest_tenant_id BIGINT NOT NULL,"
  "  backup_set_id BIGINT NOT NULL,"
  "  restore_timestamp BIGINT DEFAULT 0,"
  "  status INT NOT NULL,"
  "  backup_dest VARCHAR(1024) NOT NULL,"
  "  start_time BIGINT NOT NULL,"
  "  end_time BIGINT DEFAULT 0,"
  "  baseline_progress INT DEFAULT 0,"
  "  incremental_progress INT DEFAULT 0,"
  "  restored_log_id BIGINT DEFAULT 0,"
  "  error_msg VARCHAR(1024) DEFAULT '',"
  "  PRIMARY KEY (task_id),"
  "  INDEX idx_dest_tenant (dest_tenant_id, status),"
  "  INDEX idx_src_tenant (src_tenant_id)"
  ") ENGINE=InnoDB DEFAULT CHARSET=utf8;";

/**
 * Internal table: __all_tenant_promotion_history
 * Tracks tenant promotion events (standby to primary)
 * 
 * Columns:
 * - promotion_id: Unique promotion event identifier
 * - standby_tenant_id: Promoted standby tenant ID
 * - old_primary_tenant_id: Old primary tenant ID (decommissioned)
 * - promotion_time: Promotion timestamp
 * - verification_passed: Whether data verification passed (0=no, 1=yes)
 * - row_count_match: Row count verification result
 * - checksum_match: Checksum verification result
 * - sample_match: Sample data verification result
 * - downtime_ms: Total downtime in milliseconds
 */
static const char* ALL_TENANT_PROMOTION_HISTORY_SCHEMA =
  "CREATE TABLE __all_tenant_promotion_history ("
  "  promotion_id BIGINT NOT NULL,"
  "  standby_tenant_id BIGINT NOT NULL,"
  "  old_primary_tenant_id BIGINT NOT NULL,"
  "  promotion_time BIGINT NOT NULL,"
  "  verification_passed TINYINT NOT NULL,"
  "  row_count_match TINYINT NOT NULL,"
  "  checksum_match TINYINT NOT NULL,"
  "  sample_match TINYINT NOT NULL,"
  "  downtime_ms BIGINT DEFAULT 0,"
  "  PRIMARY KEY (promotion_id),"
  "  INDEX idx_standby_tenant (standby_tenant_id, promotion_time DESC)"
  ") ENGINE=InnoDB DEFAULT CHARSET=utf8;";

/**
 * Internal table: __all_tenant_backup_tablet
 * Tracks individual tablet backup details
 * 
 * Columns:
 * - backup_set_id: Backup set this tablet belongs to
 * - tablet_id: Tablet identifier
 * - tenant_id: Tenant ID
 * - table_id: Table ID this tablet belongs to
 * - start_key: Tablet range start key (serialized)
 * - end_key: Tablet range end key (serialized)
 * - row_count: Number of rows in this tablet
 * - data_size: Tablet data size in bytes
 * - checksum: Tablet data checksum
 * - sstable_path: Path to backed up SSTable file
 * - backup_time: Tablet backup completion time
 */
static const char* ALL_TENANT_BACKUP_TABLET_SCHEMA =
  "CREATE TABLE __all_tenant_backup_tablet ("
  "  backup_set_id BIGINT NOT NULL,"
  "  tablet_id BIGINT NOT NULL,"
  "  tenant_id BIGINT NOT NULL,"
  "  table_id BIGINT NOT NULL,"
  "  start_key VARCHAR(512) NOT NULL,"
  "  end_key VARCHAR(512) NOT NULL,"
  "  row_count BIGINT DEFAULT 0,"
  "  data_size BIGINT DEFAULT 0,"
  "  checksum BIGINT NOT NULL,"
  "  sstable_path VARCHAR(1024) NOT NULL,"
  "  backup_time BIGINT NOT NULL,"
  "  PRIMARY KEY (backup_set_id, tablet_id),"
  "  INDEX idx_tenant_table (tenant_id, table_id)"
  ") ENGINE=InnoDB DEFAULT CHARSET=utf8;";

// Table name constants
static const char* TABLE_ALL_TENANT_BACKUP_TASK = "__all_tenant_backup_task";
static const char* TABLE_ALL_TENANT_BACKUP_SET = "__all_tenant_backup_set";
static const char* TABLE_ALL_TENANT_INCREMENTAL_CHECKPOINT = "__all_tenant_incremental_checkpoint";
static const char* TABLE_ALL_TENANT_RESTORE_TASK = "__all_tenant_restore_task";
static const char* TABLE_ALL_TENANT_PROMOTION_HISTORY = "__all_tenant_promotion_history";
static const char* TABLE_ALL_TENANT_BACKUP_TABLET = "__all_tenant_backup_tablet";

// Table count
static const int TENANT_BACKUP_TABLE_COUNT = 6;

// Array of all table schemas for bulk creation
static const char* TENANT_BACKUP_TABLE_SCHEMAS[] = {
  ALL_TENANT_BACKUP_TASK_SCHEMA,
  ALL_TENANT_BACKUP_SET_SCHEMA,
  ALL_TENANT_INCREMENTAL_CHECKPOINT_SCHEMA,
  ALL_TENANT_RESTORE_TASK_SCHEMA,
  ALL_TENANT_PROMOTION_HISTORY_SCHEMA,
  ALL_TENANT_BACKUP_TABLET_SCHEMA
};

// Array of table names corresponding to schemas
static const char* TENANT_BACKUP_TABLE_NAMES[] = {
  TABLE_ALL_TENANT_BACKUP_TASK,
  TABLE_ALL_TENANT_BACKUP_SET,
  TABLE_ALL_TENANT_INCREMENTAL_CHECKPOINT,
  TABLE_ALL_TENANT_RESTORE_TASK,
  TABLE_ALL_TENANT_PROMOTION_HISTORY,
  TABLE_ALL_TENANT_BACKUP_TABLET
};

}  // namespace common
}  // namespace yaobase

#endif  // YAOBASE_COMMON_OB_TENANT_BACKUP_TABLES_H_
