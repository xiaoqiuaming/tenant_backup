/**
 * Copyright (C) 2026 YaoBase
 * 
 * Tenant backup manager
 * Central coordinator for tenant-level backup and restore operations
 */

#ifndef YAOBASE_ROOTSERVER_OB_TENANT_BACKUP_MANAGER_H_
#define YAOBASE_ROOTSERVER_OB_TENANT_BACKUP_MANAGER_H_

#include "common/ob_define.h"
#include "common/ob_tenant_backup_msg.h"
#include "common/ob_array.h"
#include "common/hash/ob_hashmap.h"
#include "yysys/ob_mutex.h"

namespace yaobase {
namespace rootserver {

/**
 * Tenant backup manager
 * Runs in RootServer, manages tenant-level backup and restore tasks
 */
class ObTenantBackupManager {
public:
  ObTenantBackupManager();
  ~ObTenantBackupManager();

  /**
   * Initialize the backup manager
   * @return OB_SUCCESS on success
   */
  int init();
  
  void destroy();
  bool is_inited() const { return is_inited_; }

  /**
   * Start a full baseline backup for a tenant
   * @param params Backup parameters
   * @param task_id Output task ID
   * @return OB_SUCCESS on success
   */
  int start_full_backup(const common::ObBackupParams& params, int64_t& task_id);

  /**
   * Start continuous incremental backup for a tenant
   * @param params Incremental backup parameters
   * @param task_id Output task ID
   * @return OB_SUCCESS on success
   */
  int start_incremental_backup(const common::ObIncrBackupParams& params, int64_t& task_id);

  /**
   * Stop incremental backup for a tenant
   * @param tenant_id Tenant ID
   * @return OB_SUCCESS on success
   */
  int stop_incremental_backup(int64_t tenant_id);

  /**
   * Restore tenant data to a standby tenant
   * @param src_tenant_id Source tenant ID
   * @param dest_tenant_id Destination (standby) tenant ID
   * @param backup_set_id Backup set ID to restore from
   * @param restore_timestamp Target restore timestamp (PITR)
   * @param backup_path Backup storage path
   * @param task_id Output task ID
   * @return OB_SUCCESS on success
   */
  int start_restore_to_standby(int64_t src_tenant_id,
                               int64_t dest_tenant_id,
                               int64_t backup_set_id,
                               int64_t restore_timestamp,
                               const char* backup_path,
                               int64_t& task_id);

  /**
   * Promote standby tenant to primary
   * Switches schema and routing to make standby tenant the active one
   * 
   * @param standby_tenant_id Standby tenant ID
   * @param primary_tenant_id Current primary tenant ID (for verification)
   * @return OB_SUCCESS on success
   */
  int promote_standby_to_primary(int64_t standby_tenant_id, int64_t primary_tenant_id);

  /**
   * Get backup status for a tenant
   * @param tenant_id Tenant ID
   * @param status Output backup status
   * @return OB_SUCCESS on success
   */
  int get_backup_status(int64_t tenant_id, common::ObBackupStatus& status);

  /**
   * Get backup task details
   * @param task_id Task ID
   * @param task Output task details
   * @return OB_SUCCESS on success, OB_ENTRY_NOT_EXIST if not found
   */
  int get_backup_task(int64_t task_id, common::ObTenantBackupTask& task);

  /**
   * Get restore task details
   * @param task_id Task ID
   * @param task Output task details
   * @return OB_SUCCESS on success, OB_ENTRY_NOT_EXIST if not found
   */
  int get_restore_task(int64_t task_id, common::ObTenantRestoreTask& task);

  /**
   * Update backup task status
   * Called by backup workers to report progress
   */
  int update_backup_task(const common::ObTenantBackupTask& task);

  /**
   * Update restore task status
   * Called by restore workers to report progress
   */
  int update_restore_task(const common::ObTenantRestoreTask& task);

  /**
   * List all backup sets for a tenant
   * @param tenant_id Tenant ID
   * @param backup_sets Output array of backup set IDs
   * @return OB_SUCCESS on success
   */
  int list_backup_sets(int64_t tenant_id, common::ObArray<int64_t>& backup_sets);

  /**
   * Get backup set manifest
   * @param backup_set_id Backup set ID
   * @param manifest Output manifest
   * @return OB_SUCCESS on success
   */
  int get_backup_manifest(int64_t backup_set_id, common::ObBaselineBackupManifest& manifest);

private:
  /**
   * Generate unique task ID
   */
  int64_t generate_task_id();

  /**
   * Generate unique backup set ID
   */
  int64_t generate_backup_set_id();

  /**
   * Validate backup parameters
   */
  int validate_backup_params(const common::ObBackupParams& params);

  /**
   * Validate restore parameters
   */
  int validate_restore_params(int64_t src_tenant_id,
                              int64_t dest_tenant_id,
                              int64_t backup_set_id,
                              const char* backup_path);

private:
  bool is_inited_;
  
  // Task ID generation
  volatile int64_t next_task_id_;
  volatile int64_t next_backup_set_id_;
  
  // Active backup tasks: task_id -> task
  common::hash::ObHashMap<int64_t, common::ObTenantBackupTask> backup_tasks_;
  
  // Active restore tasks: task_id -> task
  common::hash::ObHashMap<int64_t, common::ObTenantRestoreTask> restore_tasks_;
  
  // Tenant backup status: tenant_id -> status
  common::hash::ObHashMap<int64_t, common::ObBackupStatus> tenant_status_;
  
  // Backup set manifests: backup_set_id -> manifest
  common::hash::ObHashMap<int64_t, common::ObBaselineBackupManifest> backup_manifests_;
  
  // Thread safety
  mutable yysys::CThreadMutex mutex_;
  
  DISALLOW_COPY_AND_ASSIGN(ObTenantBackupManager);
};

}  // namespace rootserver
}  // namespace yaobase

#endif  // YAOBASE_ROOTSERVER_OB_TENANT_BACKUP_MANAGER_H_
