/**
 * Copyright (C) 2026 YaoBase
 * 
 * Schema rewriter for tenant backup/restore
 * Rewrites table_id, column_id, and schema_version in SSTable and commit logs
 */

#ifndef YAOBASE_COMMON_OB_TENANT_SCHEMA_REWRITER_H_
#define YAOBASE_COMMON_OB_TENANT_SCHEMA_REWRITER_H_

#include "common/ob_define.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_schema.h"

namespace yaobase {
namespace common {

/**
 * Schema mapping between source and destination tenant
 * Maps table IDs, column IDs, and schema versions
 */
class ObSchemaMapping {
public:
  ObSchemaMapping();
  ~ObSchemaMapping();

  int init(int64_t src_tenant_id, int64_t dest_tenant_id);
  void destroy();
  bool is_inited() const { return is_inited_; }

  /**
   * Add table ID mapping
   * @param src_table_id Source table ID
   * @param dest_table_id Destination table ID
   */
  int add_table_mapping(uint64_t src_table_id, uint64_t dest_table_id);

  /**
   * Add schema version mapping
   * @param src_version Source schema version
   * @param dest_version Destination schema version
   */
  int add_schema_version_mapping(int64_t src_version, int64_t dest_version);

  /**
   * Map table ID from source to destination
   * @param src_table_id Source table ID
   * @param dest_table_id Output destination table ID
   * @return OB_SUCCESS on success, OB_ENTRY_NOT_EXIST if not found
   */
  int map_table_id(uint64_t src_table_id, uint64_t& dest_table_id) const;

  /**
   * Map schema version from source to destination
   * @param src_version Source schema version
   * @param dest_version Output destination schema version
   * @return OB_SUCCESS on success, OB_ENTRY_NOT_EXIST if not found
   */
  int map_schema_version(int64_t src_version, int64_t& dest_version) const;

  int64_t get_src_tenant_id() const { return src_tenant_id_; }
  int64_t get_dest_tenant_id() const { return dest_tenant_id_; }

  // Get statistics
  int64_t get_table_mapping_count() const;
  int64_t get_schema_version_mapping_count() const;

private:
  bool is_inited_;
  int64_t src_tenant_id_;
  int64_t dest_tenant_id_;

  // Hash maps for ID mappings
  hash::ObHashMap<uint64_t, uint64_t> table_id_map_;  // src_table_id -> dest_table_id
  hash::ObHashMap<int64_t, int64_t> schema_version_map_;  // src_version -> dest_version

  DISALLOW_COPY_AND_ASSIGN(ObSchemaMapping);
};

/**
 * Schema rewriter for tenant backup/restore
 * Rewrites schema information in SSTable files and commit logs
 */
class ObSchemaRewriter {
public:
  ObSchemaRewriter();
  ~ObSchemaRewriter();

  int init();
  void destroy();
  bool is_inited() const { return is_inited_; }

  /**
   * Create schema mapping from source tenant to destination tenant
   * Queries both tenants' schemas and builds ID mappings
   * 
   * @param src_tenant_id Source tenant ID
   * @param dest_tenant_id Destination tenant ID
   * @param mapping Output schema mapping
   * @return OB_SUCCESS on success
   */
  int create_schema_mapping(int64_t src_tenant_id, 
                           int64_t dest_tenant_id,
                           ObSchemaMapping& mapping);

  /**
   * Rewrite SSTable schema information
   * Reads source SSTable, rewrites schema IDs, writes to destination
   * 
   * @param src_sstable_path Source SSTable file path
   * @param dest_sstable_path Destination SSTable file path
   * @param mapping Schema mapping to apply
   * @return OB_SUCCESS on success
   */
  int rewrite_sstable_schema(const char* src_sstable_path,
                             const char* dest_sstable_path,
                             const ObSchemaMapping& mapping);

  /**
   * Rewrite commit log schema information
   * Rewrites table_id and tenant_id in mutator
   * 
   * @param src_mutator Source mutator (serialized)
   * @param src_len Source buffer length
   * @param dest_mutator Destination buffer (pre-allocated)
   * @param dest_len Destination buffer length
   * @param pos Output position after writing
   * @param mapping Schema mapping to apply
   * @return OB_SUCCESS on success
   */
  int rewrite_log_schema(const char* src_mutator,
                        int64_t src_len,
                        char* dest_mutator,
                        int64_t dest_len,
                        int64_t& pos,
                        const ObSchemaMapping& mapping);

private:
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObSchemaRewriter);
};

}  // namespace common
}  // namespace yaobase

#endif  // YAOBASE_COMMON_OB_TENANT_SCHEMA_REWRITER_H_
