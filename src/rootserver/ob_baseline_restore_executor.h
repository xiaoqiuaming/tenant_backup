/**
 * Copyright (C) 2013-2026 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_baseline_restore_executor.h - Baseline restore execution worker
 *
 * Authors:
 *   GitHub Copilot <copilot@github.com>
 *
 */

#ifndef YAOBASE_ROOTSERVER_OB_BASELINE_RESTORE_EXECUTOR_H_
#define YAOBASE_ROOTSERVER_OB_BASELINE_RESTORE_EXECUTOR_H_

#include "common/ob_define.h"
#include "common/ob_string.h"
#include "common/ob_server.h"
#include "common/ob_tenant_backup_msg.h"
#include "common/ob_tenant_schema_rewriter.h"

namespace yaobase {
namespace rootserver {

// Forward declarations
class ObTabletInfoManager;
class ObChunkServerManager;

/**
 * @brief Tablet restore worker - downloads and loads a single tablet
 * 
 * Handles the complete restore workflow for one tablet:
 * 1. Download SSTable files from backup storage
 * 2. Rewrite schema metadata using ObSchemaRewriter
 * 3. Load to target ChunkServer via bypass loader
 * 4. Verify loaded data integrity
 */
class ObTabletRestoreWorker {
public:
  ObTabletRestoreWorker();
  ~ObTabletRestoreWorker();
  
  int init(const common::ObServer& chunk_server,
           const common::ObSchemaRewriter* schema_rewriter,
           const char* backup_dest);
  
  void destroy();
  
  /**
   * @brief Restore a single tablet
   * @param tablet_info Tablet metadata from manifest
   * @param tablet_range Tablet key range
   * @return OB_SUCCESS on success
   */
  int restore_tablet(const common::ObTabletInfo& tablet_info,
                     const common::ObRange& tablet_range);
  
private:
  /**
   * @brief Download SSTable files from backup storage
   * @param tablet_info Tablet to download
   * @param local_path Output path for downloaded files
   * @return OB_SUCCESS on success
   */
  int download_sstable_files_(const common::ObTabletInfo& tablet_info,
                              common::ObString& local_path);
  
  /**
   * @brief Rewrite SSTable schema metadata
   * @param sstable_path Path to SSTable files
   * @param rewritten_path Output path for rewritten SSTables
   * @return OB_SUCCESS on success
   */
  int rewrite_sstable_schema_(const common::ObString& sstable_path,
                              common::ObString& rewritten_path);
  
  /**
   * @brief Load rewritten SSTable to ChunkServer
   * @param sstable_path Path to rewritten SSTable
   * @param tablet_range Tablet key range
   * @return OB_SUCCESS on success
   */
  int load_sstable_to_chunkserver_(const common::ObString& sstable_path,
                                   const common::ObRange& tablet_range);
  
  /**
   * @brief Verify loaded tablet data
   * @param tablet_info Expected tablet metadata
   * @return OB_SUCCESS if verification passes
   */
  int verify_tablet_data_(const common::ObTabletInfo& tablet_info);
  
private:
  bool is_inited_;
  common::ObServer chunk_server_;
  const common::ObSchemaRewriter* schema_rewriter_;
  char backup_dest_[common::OB_MAX_FILE_NAME_LENGTH];
  
  DISALLOW_COPY_AND_ASSIGN(ObTabletRestoreWorker);
};

/**
 * @brief Baseline restore executor - orchestrates parallel tablet restore
 * 
 * Coordinates the restore of all tablets for a tenant's baseline backup:
 * - Downloads baseline manifest from backup storage
 * - Spawns parallel tablet restore workers
 * - Tracks progress and handles failures
 * - Verifies overall restore integrity
 */
class ObBaselineRestoreExecutor {
public:
  ObBaselineRestoreExecutor();
  ~ObBaselineRestoreExecutor();
  
  int init(ObTabletInfoManager* tablet_mgr,
           ObChunkServerManager* cs_mgr,
           const common::ObSchemaRewriter* schema_rewriter);
  
  void destroy();
  
  /**
   * @brief Execute baseline restore for a tenant
   * @param src_tenant_id Source tenant ID from backup
   * @param dest_tenant_id Destination (standby) tenant ID
   * @param backup_set_id Backup set to restore from
   * @param backup_dest Backup storage path
   * @param manifest Output manifest structure
   * @return OB_SUCCESS on success
   */
  int restore_baseline(int64_t src_tenant_id,
                      int64_t dest_tenant_id,
                      int64_t backup_set_id,
                      const char* backup_dest,
                      common::ObBaselineBackupManifest& manifest);
  
private:
  /**
   * @brief Download baseline manifest from backup storage
   * @param backup_dest Backup storage path
   * @param backup_set_id Backup set identifier
   * @param manifest Output manifest structure
   * @return OB_SUCCESS on success
   */
  int download_manifest_(const char* backup_dest,
                        int64_t backup_set_id,
                        common::ObBaselineBackupManifest& manifest);
  
  /**
   * @brief Restore all tablets in parallel
   * @param manifest Baseline manifest with tablet list
   * @param parallelism Degree of parallelism
   * @return OB_SUCCESS on success
   */
  int restore_tablets_parallel_(const common::ObBaselineBackupManifest& manifest,
                                int32_t parallelism);
  
  /**
   * @brief Restore tenant schema metadata
   * @param manifest Manifest with schema info
   * @param dest_tenant_id Destination tenant
   * @return OB_SUCCESS on success
   */
  int restore_tenant_schema_(const common::ObBaselineBackupManifest& manifest,
                             int64_t dest_tenant_id);
  
  /**
   * @brief Verify overall restore integrity
   * @param manifest Expected manifest
   * @return OB_SUCCESS if all checks pass
   */
  int verify_restore_integrity_(const common::ObBaselineBackupManifest& manifest);
  
  /**
   * @brief Select target ChunkServer for a tablet
   * @param tablet_range Tablet key range
   * @param chunk_server Output selected server
   * @return OB_SUCCESS on success
   */
  int select_target_chunkserver_(const common::ObRange& tablet_range,
                                 common::ObServer& chunk_server);
  
private:
  bool is_inited_;
  ObTabletInfoManager* tablet_mgr_;
  ObChunkServerManager* cs_mgr_;
  const common::ObSchemaRewriter* schema_rewriter_;
  
  // Progress tracking
  int64_t total_tablets_;
  int64_t completed_tablets_;
  int64_t failed_tablets_;
  
  DISALLOW_COPY_AND_ASSIGN(ObBaselineRestoreExecutor);
};

}  // namespace rootserver
}  // namespace yaobase

#endif  // YAOBASE_ROOTSERVER_OB_BASELINE_RESTORE_EXECUTOR_H_
