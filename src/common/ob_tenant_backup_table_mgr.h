/**
 * Copyright (C) 2013-2025 YaoBase Inc. All Rights Reserved.
 * Manager for initializing and maintaining tenant backup/restore internal tables.
 */

#ifndef YAOBASE_COMMON_OB_TENANT_BACKUP_TABLE_MGR_H_
#define YAOBASE_COMMON_OB_TENANT_BACKUP_TABLE_MGR_H_

#include "common/ob_define.h"
#include "common/ob_string.h"
#include "common/ob_tenant_backup_tables.h"

namespace yaobase
{
namespace common
{

/**
 * ObTenantBackupTableMgr
 * 
 * Manages creation and maintenance of internal tables for tenant backup/restore.
 * Provides APIs for initializing tables, checking existence, and performing
 * metadata operations.
 */
class ObTenantBackupTableMgr
{
public:
  ObTenantBackupTableMgr();
  ~ObTenantBackupTableMgr();

  /**
   * Initialize the table manager
   * @return OB_SUCCESS on success
   */
  int init();

  /**
   * Destroy and cleanup resources
   */
  void destroy();

  /**
   * Check if all backup/restore internal tables exist
   * @param exist [out] true if all tables exist
   * @return OB_SUCCESS on success
   */
  int check_tables_exist(bool& exist) const;

  /**
   * Create all backup/restore internal tables
   * Idempotent: safe to call if tables already exist
   * @return OB_SUCCESS on success
   */
  int create_tables();

  /**
   * Drop all backup/restore internal tables
   * WARNING: This deletes all backup/restore metadata
   * @return OB_SUCCESS on success
   */
  int drop_tables();

  /**
   * Verify table schemas match expected definitions
   * @param all_match [out] true if all schemas match
   * @return OB_SUCCESS on success
   */
  int verify_table_schemas(bool& all_match) const;

  /**
   * Get table count
   * @return Number of backup/restore internal tables
   */
  int get_table_count() const { return TENANT_BACKUP_TABLE_COUNT; }

  /**
   * Check if manager is initialized
   * @return true if initialized
   */
  bool is_inited() const { return is_inited_; }

private:
  /**
   * Execute SQL statement
   * @param sql SQL statement to execute
   * @return OB_SUCCESS on success
   */
  int execute_sql_(const char* sql);

  /**
   * Check if a specific table exists
   * @param table_name Table name to check
   * @param exist [out] true if table exists
   * @return OB_SUCCESS on success
   */
  int check_table_exists_(const char* table_name, bool& exist) const;

  /**
   * Create a single table
   * @param schema Table schema SQL
   * @return OB_SUCCESS on success
   */
  int create_table_(const char* schema);

  /**
   * Drop a single table
   * @param table_name Table name to drop
   * @return OB_SUCCESS on success
   */
  int drop_table_(const char* table_name);

private:
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObTenantBackupTableMgr);
};

}  // namespace common
}  // namespace yaobase

#endif  // YAOBASE_COMMON_OB_TENANT_BACKUP_TABLE_MGR_H_
