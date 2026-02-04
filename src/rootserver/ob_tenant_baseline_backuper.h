/**
 * Copyright (C) 2026 YaoBase
 * 
 * Tenant baseline backuper
 * Backs up L1 layer (ChunkServer) SSTable data for a tenant
 */

#ifndef YAOBASE_ROOTSERVER_OB_TENANT_BASELINE_BACKUPER_H_
#define YAOBASE_ROOTSERVER_OB_TENANT_BASELINE_BACKUPER_H_

#include "common/ob_define.h"
#include "common/ob_tenant_backup_msg.h"
#include "common/ob_array.h"
#include "common/ob_tablet_info.h"
#include "yysys/ob_runnable.h"

namespace yaobase {
namespace rootserver {

/**
 * Tablet backup worker
 * Backs up a single tablet's SSTables
 */
class ObTabletBackupWorker {
public:
  ObTabletBackupWorker();
  ~ObTabletBackupWorker();

  /**
   * Backup a tablet
   * @param tablet Tablet information
   * @param backup_path Backup storage path
   * @param frozen_version Target frozen version
   * @param backup_info Output backup information
   * @return OB_SUCCESS on success
   */
  int backup_tablet(const common::ObTabletInfo& tablet,
                   const char* backup_path,
                   int64_t frozen_version,
                   common::ObTabletBackupInfo& backup_info);

private:
  /**
   * Copy SSTable files from ChunkServer to backup storage
   */
  int copy_sstable_files(const common::ObTabletInfo& tablet,
                        const char* backup_path,
                        int64_t frozen_version,
                        common::ObArray<int64_t>& sstable_ids);

  /**
   * Calculate checksum for backup verification
   */
  int calculate_checksum(const common::ObArray<int64_t>& sstable_ids,
                        uint64_t& checksum);
};

/**
 * Tenant baseline backuper
 * Orchestrates backup of all tablets for a tenant
 */
class ObTenantBaselineBackuper {
public:
  ObTenantBaselineBackuper();
  ~ObTenantBaselineBackuper();

  int init();
  void destroy();
  bool is_inited() const { return is_inited_; }

  /**
   * Backup tenant baseline data
   * 
   * @param tenant_id Tenant ID
   * @param frozen_version Target frozen version (0 = latest)
   * @param backup_path Backup storage path
   * @param backup_set_id Backup set ID (for manifest)
   * @param manifest Output backup manifest
   * @return OB_SUCCESS on success
   */
  int backup_tenant_baseline(int64_t tenant_id,
                             int64_t frozen_version,
                             const char* backup_path,
                             int64_t backup_set_id,
                             common::ObBaselineBackupManifest& manifest);

private:
  /**
   * Fetch all tablets for a tenant from RootServer
   */
  int fetch_tenant_tablets(int64_t tenant_id,
                          common::ObArray<common::ObTabletInfo>& tablets);

  /**
   * Get current frozen version if not specified
   */
  int get_current_frozen_version(int64_t tenant_id, int64_t& frozen_version);

  /**
   * Backup all tablets (parallel execution)
   */
  int backup_tablets_parallel(const common::ObArray<common::ObTabletInfo>& tablets,
                              const char* backup_path,
                              int64_t frozen_version,
                              int parallel_degree,
                              common::ObArray<common::ObTabletBackupInfo>& backup_infos);

  /**
   * Backup tenant schema information
   */
  int backup_tenant_schema(int64_t tenant_id,
                          const char* backup_path,
                          common::ObArray<common::ObSchemaBackupInfo>& schema_list);

  /**
   * Generate and save backup manifest
   */
  int generate_and_save_manifest(int64_t tenant_id,
                                 int64_t backup_set_id,
                                 int64_t frozen_version,
                                 const char* backup_path,
                                 const common::ObArray<common::ObTabletBackupInfo>& tablet_infos,
                                 const common::ObArray<common::ObSchemaBackupInfo>& schema_infos,
                                 common::ObBaselineBackupManifest& manifest);

  /**
   * Verify backup integrity
   */
  int verify_backup_integrity(const common::ObBaselineBackupManifest& manifest,
                              const char* backup_path);

private:
  bool is_inited_;
  ObTabletBackupWorker worker_;

  DISALLOW_COPY_AND_ASSIGN(ObTenantBaselineBackuper);
};

}  // namespace rootserver
}  // namespace yaobase

#endif  // YAOBASE_ROOTSERVER_OB_TENANT_BASELINE_BACKUPER_H_
