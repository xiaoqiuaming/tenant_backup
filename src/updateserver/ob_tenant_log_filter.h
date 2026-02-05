/**
 * Copyright (C) 2026 YaoBase
 * 
 * Tenant log filter
 * Filters commit log entries by tenant_id for multi-tenant incremental backup
 */

#ifndef YAOBASE_UPDATESERVER_OB_TENANT_LOG_FILTER_H_
#define YAOBASE_UPDATESERVER_OB_TENANT_LOG_FILTER_H_

#include "common/ob_define.h"
#include "common/ob_log_entry.h"
#include "common/ob_mutator.h"

namespace yaobase {
namespace updateserver {

/**
 * Tenant log filter
 * Extracts tenant_id from commit log entries and filters by tenant
 */
class ObTenantLogFilter {
public:
  ObTenantLogFilter();
  ~ObTenantLogFilter();

  int init();
  void destroy();
  bool is_inited() const { return is_inited_; }

  /**
   * Check if a log entry belongs to a specific tenant
   * 
   * @param log_entry Commit log entry to check
   * @param tenant_id Target tenant ID
   * @return true if log belongs to tenant, false otherwise
   */
  bool is_tenant_log(const common::ObLogEntry& log_entry, int64_t tenant_id);

  /**
   * Extract tenant_id from a commit log entry
   * 
   * @param log_entry Commit log entry
   * @param tenant_id Output tenant ID
   * @return OB_SUCCESS on success, OB_ENTRY_NOT_EXIST if no tenant info
   */
  int extract_tenant_id(const common::ObLogEntry& log_entry, int64_t& tenant_id);

  /**
   * Check if a mutator belongs to a specific tenant
   * 
   * @param mutator Mutator to check
   * @param tenant_id Target tenant ID
   * @return true if mutator belongs to tenant, false otherwise
   */
  bool is_tenant_mutator(const common::ObMutator& mutator, int64_t tenant_id);

  /**
   * Extract tenant_id from a mutator by examining table_ids
   * 
   * @param mutator Mutator to examine
   * @param tenant_id Output tenant ID
   * @return OB_SUCCESS on success
   */
  int extract_tenant_id_from_mutator(const common::ObMutator& mutator, int64_t& tenant_id);

private:
  /**
   * Get tenant_id from table_id
   * In YaoBase, table_id typically encodes tenant information
   */
  int64_t get_tenant_id_from_table_id(uint64_t table_id) const;

  /**
   * Check if table_id belongs to system tables
   */
  bool is_system_table(uint64_t table_id) const;

private:
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObTenantLogFilter);
};

}  // namespace updateserver
}  // namespace yaobase

#endif  // YAOBASE_UPDATESERVER_OB_TENANT_LOG_FILTER_H_
