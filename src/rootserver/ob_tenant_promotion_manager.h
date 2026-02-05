/**
 * Copyright (C) 2013-2026 YaoBase Development Team
 * 
 * Tenant Promotion Manager - Promotes standby tenant to primary after restore
 */

#ifndef YAOBASE_ROOTSERVER_OB_TENANT_PROMOTION_MANAGER_H_
#define YAOBASE_ROOTSERVER_OB_TENANT_PROMOTION_MANAGER_H_

#include "common/ob_define.h"
#include "common/ob_server.h"
#include "tbsys.h"
#include <string>

namespace yaobase {
namespace rootserver {

/**
 * @brief Manages tenant promotion from standby to primary status
 * 
 * After a tenant restore is complete, the standby tenant needs to be promoted
 * to become the new primary. This involves:
 * 1. Verifying data consistency between source and standby
 * 2. Switching schema/routing to point to standby tenant
 * 3. Marking old tenant as read-only or decommissioned
 * 4. Enabling writes on standby tenant
 */
class ObTenantPromotionManager
{
public:
  ObTenantPromotionManager();
  ~ObTenantPromotionManager();
  
  /**
   * @brief Initialize promotion manager
   * @return OB_SUCCESS on success, error code otherwise
   */
  int init();
  
  /**
   * @brief Destroy and cleanup resources
   */
  void destroy();
  
  /**
   * @brief Promote standby tenant to primary
   * @param standby_tenant_id Standby tenant to promote
   * @param old_primary_tenant_id Old primary tenant (optional, for decommission)
   * @param verify_data Whether to verify data before promotion
   * @return OB_SUCCESS on success, error code otherwise
   */
  int promote_to_primary(int64_t standby_tenant_id,
                         int64_t old_primary_tenant_id,
                         bool verify_data);
  
  /**
   * @brief Verify data consistency between source and restored tenant
   * @param source_tenant_id Source tenant ID
   * @param restored_tenant_id Restored tenant ID
   * @param[out] row_count_match Whether row counts match
   * @param[out] checksum_match Whether checksums match
   * @return OB_SUCCESS on success, error code otherwise
   */
  int verify_data_consistency(int64_t source_tenant_id,
                              int64_t restored_tenant_id,
                              bool& row_count_match,
                              bool& checksum_match);
  
  /**
   * @brief Decommission old primary tenant
   * @param tenant_id Tenant to decommission
   * @param make_readonly Whether to make read-only or fully decommission
   * @return OB_SUCCESS on success, error code otherwise
   */
  int decommission_tenant(int64_t tenant_id, bool make_readonly);
  
  /**
   * @brief Get promotion status
   * @param standby_tenant_id Standby tenant ID
   * @param[out] status Status string
   * @return OB_SUCCESS on success, error code otherwise
   */
  int get_promotion_status(int64_t standby_tenant_id, std::string& status);
  
private:
  // Verify row counts match between tenants
  int verify_row_counts_(int64_t source_tenant_id,
                         int64_t restored_tenant_id,
                         bool& match);
  
  // Verify checksums match between tenants
  int verify_checksums_(int64_t source_tenant_id,
                        int64_t restored_tenant_id,
                        bool& match);
  
  // Compare sample data between tenants
  int verify_sample_data_(int64_t source_tenant_id,
                          int64_t restored_tenant_id,
                          bool& match);
  
  // Switch schema to point to new primary
  int switch_schema_(int64_t standby_tenant_id);
  
  // Update routing to direct traffic to new primary
  int update_routing_(int64_t standby_tenant_id);
  
  // Enable writes on standby tenant
  int enable_writes_(int64_t tenant_id);
  
  // Disable writes on old primary tenant
  int disable_writes_(int64_t tenant_id);
  
  // Mark tenant as read-only
  int mark_readonly_(int64_t tenant_id);
  
  // Remove tenant from active service
  int remove_from_service_(int64_t tenant_id);
  
private:
  bool is_inited_;
  tbsys::CThreadMutex mutex_;
  
  DISALLOW_COPY_AND_ASSIGN(ObTenantPromotionManager);
};

} // namespace rootserver
} // namespace yaobase

#endif // YAOBASE_ROOTSERVER_OB_TENANT_PROMOTION_MANAGER_H_
