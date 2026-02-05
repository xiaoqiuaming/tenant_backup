/**
 * Copyright (C) 2026 YaoBase
 * 
 * Tenant restore coordinator
 * Orchestrates multi-phase restore process: baseline + incremental
 */

#ifndef YAOBASE_ROOTSERVER_OB_TENANT_RESTORE_COORDINATOR_H_
#define YAOBASE_ROOTSERVER_OB_TENANT_RESTORE_COORDINATOR_H_

#include "common/ob_define.h"
#include "common/ob_tenant_backup_msg.h"
#include "common/ob_tenant_schema_rewriter.h"
#include "common/ob_array.h"

namespace yaobase {
namespace rootserver {

/**
 * Restore phase enumeration
 */
enum ObRestorePhase {
  RESTORE_PHASE_PREPARE = 0,      // Prepare standby tenant
  RESTORE_PHASE_BASELINE,         // Restore baseline data
  RESTORE_PHASE_INCREMENTAL,      // Restore incremental logs
  RESTORE_PHASE_VERIFY,           // Verify data consistency
  RESTORE_PHASE_COMPLETE          // Restore complete
};

/**
 * Tenant restore coordinator
 * Orchestrates the complete restore process for a tenant
 */
class ObTenantRestoreCoordinator {
public:
  ObTenantRestoreCoordinator();
  ~ObTenantRestoreCoordinator();

  int init();
  void destroy();
  bool is_inited() const { return is_inited_; }

  /**
   * Main restore entry point
   * Coordinates all phases of tenant restore
   * 
   * @param task Restore task parameters
   * @return OB_SUCCESS on success
   */
  int restore_tenant(const common::ObTenantRestoreTask& task);

  /**
   * Get current restore phase
   * 
   * @param task_id Task ID
   * @param phase Output current phase
   * @return OB_SUCCESS on success
   */
  int get_restore_phase(int64_t task_id, ObRestorePhase& phase);

private:
  /**
   * Phase 1: Prepare standby tenant
   * Creates tenant schema with new table_ids
   * 
   * @param task Restore task
   * @param schema_mapping Output schema mapping
   * @return OB_SUCCESS on success
   */
  int prepare_standby_tenant(const common::ObTenantRestoreTask& task,
                             common::ObSchemaMapping& schema_mapping);

  /**
   * Phase 2: Restore baseline data
   * Downloads and loads SSTable files
   * 
   * @param task Restore task
   * @param schema_mapping Schema mapping for rewriting
   * @return OB_SUCCESS on success
   */
  int restore_baseline(const common::ObTenantRestoreTask& task,
                      const common::ObSchemaMapping& schema_mapping);

  /**
   * Phase 3: Restore incremental logs
   * Replays archived commit logs
   * 
   * @param task Restore task
   * @param schema_mapping Schema mapping for rewriting
   * @return OB_SUCCESS on success
   */
  int restore_incremental(const common::ObTenantRestoreTask& task,
                         const common::ObSchemaMapping& schema_mapping);

  /**
   * Phase 4: Verify restored data
   * Checks data integrity and consistency
   * 
   * @param task Restore task
   * @return OB_SUCCESS on success
   */
  int verify_restored_data(const common::ObTenantRestoreTask& task);

  /**
   * Download baseline manifest from backup storage
   * 
   * @param backup_path Backup storage path
   * @param backup_set_id Backup set ID
   * @param manifest Output manifest
   * @return OB_SUCCESS on success
   */
  int download_baseline_manifest(const char* backup_path,
                                 int64_t backup_set_id,
                                 common::ObBaselineBackupManifest& manifest);

  /**
   * Download and restore a single tablet
   * 
   * @param tablet_info Tablet backup information
   * @param backup_path Backup storage path
   * @param schema_mapping Schema mapping
   * @return OB_SUCCESS on success
   */
  int restore_tablet(const common::ObTabletBackupInfo& tablet_info,
                    const char* backup_path,
                    const common::ObSchemaMapping& schema_mapping);

  /**
   * Replay incremental logs from backup storage
   * 
   * @param task Restore task
   * @param schema_mapping Schema mapping
   * @param start_log_id Starting log ID
   * @param end_timestamp End timestamp for PITR
   * @return OB_SUCCESS on success
   */
  int replay_incremental_logs(const common::ObTenantRestoreTask& task,
                              const common::ObSchemaMapping& schema_mapping,
                              int64_t start_log_id,
                              int64_t end_timestamp);

private:
  bool is_inited_;
  common::ObSchemaRewriter schema_rewriter_;

  DISALLOW_COPY_AND_ASSIGN(ObTenantRestoreCoordinator);
};

}  // namespace rootserver
}  // namespace yaobase

#endif  // YAOBASE_ROOTSERVER_OB_TENANT_RESTORE_COORDINATOR_H_
