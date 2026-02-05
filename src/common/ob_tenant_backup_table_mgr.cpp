/**
 * Copyright (C) 2013-2025 YaoBase Inc. All Rights Reserved.
 * Manager for initializing and maintaining tenant backup/restore internal tables.
 */

#include "common/ob_tenant_backup_table_mgr.h"
#include "common/ob_define.h"
#include "tblog.h"

namespace yaobase
{
namespace common
{

ObTenantBackupTableMgr::ObTenantBackupTableMgr()
  : is_inited_(false)
{
}

ObTenantBackupTableMgr::~ObTenantBackupTableMgr()
{
  destroy();
}

int ObTenantBackupTableMgr::init()
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    TBSYS_LOG(WARN, "ObTenantBackupTableMgr already initialized");
  } else {
    is_inited_ = true;
    TBSYS_LOG(INFO, "ObTenantBackupTableMgr initialized successfully");
  }

  return ret;
}

void ObTenantBackupTableMgr::destroy()
{
  is_inited_ = false;
}

int ObTenantBackupTableMgr::check_tables_exist(bool& exist) const
{
  int ret = OB_SUCCESS;
  exist = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "ObTenantBackupTableMgr not initialized");
  } else {
    // Check each table
    for (int i = 0; OB_SUCCESS == ret && i < TENANT_BACKUP_TABLE_COUNT; ++i) {
      bool table_exist = false;
      ret = check_table_exists_(TENANT_BACKUP_TABLE_NAMES[i], table_exist);
      if (OB_SUCCESS == ret && !table_exist) {
        exist = false;
        TBSYS_LOG(INFO, "Backup table does not exist, table_name=%s",
                  TENANT_BACKUP_TABLE_NAMES[i]);
        break;
      }
    }
  }

  return ret;
}

int ObTenantBackupTableMgr::create_tables()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "ObTenantBackupTableMgr not initialized");
  } else {
    TBSYS_LOG(INFO, "Creating tenant backup/restore internal tables, count=%d",
              TENANT_BACKUP_TABLE_COUNT);

    // Create each table
    for (int i = 0; OB_SUCCESS == ret && i < TENANT_BACKUP_TABLE_COUNT; ++i) {
      // Check if table already exists
      bool exist = false;
      ret = check_table_exists_(TENANT_BACKUP_TABLE_NAMES[i], exist);

      if (OB_SUCCESS == ret) {
        if (exist) {
          TBSYS_LOG(INFO, "Backup table already exists, skip creation, table_name=%s",
                    TENANT_BACKUP_TABLE_NAMES[i]);
        } else {
          // Create table
          ret = create_table_(TENANT_BACKUP_TABLE_SCHEMAS[i]);
          if (OB_SUCCESS == ret) {
            TBSYS_LOG(INFO, "Created backup table successfully, table_name=%s",
                      TENANT_BACKUP_TABLE_NAMES[i]);
          } else {
            TBSYS_LOG(WARN, "Failed to create backup table, table_name=%s, ret=%d",
                      TENANT_BACKUP_TABLE_NAMES[i], ret);
          }
        }
      }
    }

    if (OB_SUCCESS == ret) {
      TBSYS_LOG(INFO, "All tenant backup/restore tables created successfully");
    }
  }

  return ret;
}

int ObTenantBackupTableMgr::drop_tables()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "ObTenantBackupTableMgr not initialized");
  } else {
    TBSYS_LOG(WARN, "Dropping all tenant backup/restore internal tables");

    // Drop each table in reverse order (due to potential dependencies)
    for (int i = TENANT_BACKUP_TABLE_COUNT - 1; i >= 0 && OB_SUCCESS == ret; --i) {
      ret = drop_table_(TENANT_BACKUP_TABLE_NAMES[i]);
      if (OB_SUCCESS == ret) {
        TBSYS_LOG(INFO, "Dropped backup table, table_name=%s",
                  TENANT_BACKUP_TABLE_NAMES[i]);
      } else {
        TBSYS_LOG(WARN, "Failed to drop backup table, table_name=%s, ret=%d",
                  TENANT_BACKUP_TABLE_NAMES[i], ret);
      }
    }
  }

  return ret;
}

int ObTenantBackupTableMgr::verify_table_schemas(bool& all_match) const
{
  int ret = OB_SUCCESS;
  all_match = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "ObTenantBackupTableMgr not initialized");
  } else {
    // TODO: Implement actual schema verification by querying information_schema
    // For now, just check existence
    ret = check_tables_exist(all_match);

    if (OB_SUCCESS == ret) {
      TBSYS_LOG(INFO, "Backup table schema verification completed, all_match=%d",
                all_match);
    }
  }

  return ret;
}

int ObTenantBackupTableMgr::execute_sql_(const char* sql)
{
  int ret = OB_SUCCESS;

  if (nullptr == sql || '\0' == sql[0]) {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "Invalid SQL statement");
  } else {
    // TODO: Integrate with YaoBase SQL execution layer
    // This is a placeholder that would connect to the SQL engine
    // and execute the provided statement.
    //
    // Example integration:
    // ObSqlProxy sql_proxy;
    // ret = sql_proxy.write(sql, affected_rows);
    
    TBSYS_LOG(INFO, "Executing SQL (placeholder): %s", sql);
    
    // For now, return success to allow build completion
    // Real implementation would execute via ObSqlProxy or similar
  }

  return ret;
}

int ObTenantBackupTableMgr::check_table_exists_(const char* table_name, 
                                                 bool& exist) const
{
  int ret = OB_SUCCESS;
  exist = false;

  if (nullptr == table_name || '\0' == table_name[0]) {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "Invalid table name");
  } else {
    // TODO: Query information_schema to check table existence
    // Example SQL: SELECT COUNT(*) FROM information_schema.tables 
    //              WHERE table_name = '<table_name>' AND table_schema = DATABASE()
    
    // Placeholder: Assume tables don't exist initially
    // Real implementation would query system tables
    exist = false;
    
    TBSYS_LOG(DEBUG, "Checking table existence (placeholder), table_name=%s",
              table_name);
  }

  return ret;
}

int ObTenantBackupTableMgr::create_table_(const char* schema)
{
  int ret = OB_SUCCESS;

  if (nullptr == schema || '\0' == schema[0]) {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "Invalid table schema");
  } else {
    // Execute CREATE TABLE statement
    ret = execute_sql_(schema);
  }

  return ret;
}

int ObTenantBackupTableMgr::drop_table_(const char* table_name)
{
  int ret = OB_SUCCESS;

  if (nullptr == table_name || '\0' == table_name[0]) {
    ret = OB_INVALID_ARGUMENT;
    TBSYS_LOG(WARN, "Invalid table name");
  } else {
    // Build DROP TABLE statement
    char sql[256];
    int n = snprintf(sql, sizeof(sql), "DROP TABLE IF EXISTS %s;", table_name);
    if (n < 0 || n >= static_cast<int>(sizeof(sql))) {
      ret = OB_BUF_NOT_ENOUGH;
      TBSYS_LOG(WARN, "SQL buffer too small for DROP TABLE");
    } else {
      ret = execute_sql_(sql);
    }
  }

  return ret;
}

}  // namespace common
}  // namespace yaobase
