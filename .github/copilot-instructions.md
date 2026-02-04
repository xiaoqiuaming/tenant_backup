# YaoBase AI Coding Agent Instructions

**Project**: YaoBase - Multi-tenant distributed relational database (fork of OceanBase 0.4)  
**Language**: C/C++ with autotools build system  
**Architecture**: Four-tier distributed system with two-layer LSM-tree storage

## System Architecture Overview

YaoBase implements a distributed database with extreme decoupling:

### Core Components (src/)
- **RootServer** (`src/rootserver/`) - Cluster manager: node heartbeats, metadata management, replica load balancing
- **UpdateServer** (`src/updateserver/`) - L0 layer: MemTable + frozen SSTable for incremental data, manages transaction commits
- **ChunkServer** (`src/chunkserver/`) - L1 layer: Baseline data in 256MB range shards, SSTable storage
- **MergeServer** (`src/mergeserver/`) - SQL parser and distributed query execution planner
- **Common** (`src/common/`) - Shared utilities: RPC stubs, schema management, serialization, tenant context

### Key Architectural Patterns
- **Two-layer LSM-tree**: L0 (UpdateServer incremental) + L1 (ChunkServer baseline), separated storage and merging
- **Tenant context propagation**: `tenant_id` flows through RPC chains from MergeServer → UpdateServer/ChunkServer
- **RPC communication**: All inter-server communication uses `ObGeneralRpcStub` (see `src/common/ob_general_rpc_stub.{h,cpp}`)
- **Server registration**: All servers register with RootServer via heartbeat (`heartbeat_server()`, `heartbeat_merge_server()`)

### Multi-Tenant Isolation (see MULTI_TENANT_ARCHITECTURE.md)
- **Resource isolation**: CPU/memory at MergeServer (via cgroup), disk at ChunkServer
- **Shared components**: UpdateServer is shared across tenants (auto-merges free memory)
- **Tenant context**: Always pass `tenant_id` in request chains; use `TenantContext` struct in `src/common/`
- **Current limitation**: Database names are globally unique (no same-name DBs across tenants yet)

## Build & Test Workflow

### Initial Setup (Autotools)
```bash
# Generate build files
./build.sh init

# Configure (release build)
./configure --with-release

# Or debug build
./configure

# Build all
make -j$(nproc)

# Run tests
./run_tests.sh
./run_tests.sh --print-failed           # Show failures only
./run_tests.sh base_main_test           # Run specific test
```

### Key Build Scripts
- `build.sh init` - Runs aclocal, libtoolize, autoconf, automake; generates SQL parser from `src/sql/gen_parser.sh`
- `build.sh clean` - Deep clean (removes all generated files)
- `configure.ac` - Main autotools config; defines all subdirectories and feature flags
- `Makefile.am` files - Distributed throughout src/tests/tools for component builds

### Testing
- `run_tests.sh` - Discovers and runs all test binaries in `tests/` subdirectories
- `auto_tests.sh` - Automated test runner for CI
- Tests mirror source structure: `tests/chunkserver/`, `tests/updateserver/`, etc.

## C++ Coding Standards (CRITICAL)

**See `.github/instructions/oceanbase-cpp-coding-standard.instructions.md` for full details**

### Quick Reference
```cpp
// Naming
class ObMyClass { };              // Classes: ObClassName prefix
int my_var_;                      // Members: trailing underscore
int local_var;                    // Locals: lowercase_with_underscores
MY_CONSTANT                       // Constants: UPPERCASE

// Namespace (matches directory structure)
namespace yaobase {
namespace common {  // No indentation inside namespace

class ObFoo {
  int func();
};

}  // namespace common
}  // namespace yaobase

// Resource management pattern
class ObFoo {
public:
  ObFoo() : ptr_(NULL), is_inited_(false) {}
  int init();                // Two-phase init (never throw in constructor)
  void destroy();            // Explicit cleanup
  void reuse();              // Reset for reuse (avoid re-allocation in loops)
  bool is_inited() const { return is_inited_; }
  
private:
  void *ptr_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObFoo);  // Always add for non-copyable classes
};
```

### CRITICAL Rules
- ❌ **No anonymous namespaces** (breaks debugging)
- ❌ **No global variables** (use singletons or `ob_define.h` constants)
- ✅ **Single entry, single exit** for all functions
- ✅ **Two-phase init pattern**: Constructor + `init()`, explicit `destroy()`
- ✅ **Always check return values** and parameters
- ✅ Use `DISALLOW_COPY_AND_ASSIGN` macro for non-copyable classes

## Project-Specific Patterns

### RPC Communication
```cpp
// Pattern: All inter-server calls use ObGeneralRpcStub
// Example from src/common/ob_general_rpc_stub.cpp
int ObGeneralRpcStub::heartbeat_server(
    const int64_t timeout,
    const ObServer& root_server,
    const ObServer& chunk_server,
    const ObRole server_role) const
{
  return post_request_2(root_server, timeout, OB_HEARTBEAT, NEW_VERSION,
                        ObTbnetCallback::default_callback, NULL,
                        chunk_server, static_cast<int32_t>(server_role));
}
```

### Tenant Context Pattern
```cpp
// Always propagate tenant_id through request chains
struct ObRequest {
  int64_t tenant_id_;  // Add to all request structs
  // ... other fields
};

// In RPC handlers, extract and pass tenant context
int handle_request(const ObRequest& req) {
  TenantContext ctx(req.tenant_id_);
  // Use ctx for resource accounting, isolation
}
```

### SSTable Backup/Restore
- **Bypass loader**: `src/chunkserver/ob_bypass_sstable_loader.{h,cpp}` - Loads SSTable via hardlinks outside normal flow
- **Baseline backup**: `src/backupserver/ob_tablet_backup_manager.{h,cpp}` - Reference for tablet-level backup tasks
- **Tenant backup message**: `src/common/ob_tenant_backup_msg.h` - Task structure for tenant-scoped operations

## Key Files & Entry Points

### Server Main Functions
- `src/rootserver/main.cpp` - RootServer entry
- `src/updateserver/main.cpp` - UpdateServer entry  
- `src/chunkserver/ob_chunk_server_main.cpp` - ChunkServer entry
- `src/mergeserver/ob_merge_server_main.cpp` - MergeServer entry

### Configuration Templates
- `script/deploy/oceanbase.conf.template` - Cluster configuration
- `src/backupserver/backupserver.conf.template` - Backup server config

### Critical Shared Code
- `src/common/ob_packet.h` - RPC packet codes (OB_HEARTBEAT, OB_GET_UPDATE_SERVER_INFO, etc.)
- `src/common/ob_server.h` - Server address/identity representation
- `src/common/ob_schema_manager.h` - Schema metadata management
- `src/sstable/` - SSTable format and readers/writers

## Common Tasks

### Adding RPC Support
1. Define packet code in `src/common/ob_packet.h`
2. Add stub method in `src/common/ob_general_rpc_stub.{h,cpp}`
3. Implement handler in target server's `ob_*_service.cpp`
4. Register handler in server's packet dispatcher

### Multi-Tenant Feature Development
1. Read `MULTI_TENANT_ARCHITECTURE.md` for isolation boundaries
2. Add `tenant_id` to request/response structs
3. Update RPC stubs to propagate tenant context
4. Implement resource accounting at appropriate layer (MergeServer for CPU/mem, ChunkServer for disk)
5. Add tenant-filtered queries for metadata tables

### Testing New Features
1. Add unit tests in `tests/<component>/` mirroring `src/<component>/`
2. Use GTest framework (see existing `*_test.cpp` files)
3. Mock servers are in `tests/*/mock_*.{h,cpp}` (e.g., `MockRootServer`)
4. Run with `./run_tests.sh <test_name_pattern>`

## External Dependencies

- **Network**: yynet/libeasy (replaced old tbnet)
- **Serialization**: Custom ObDataBuffer/ObSerializer in `src/common/`
- **Storage**: Custom SSTable format (not RocksDB)
- **Schema**: Internal table mechanism (no external schema files)
- **MySQL protocol**: `src/obmysql/` implements wire protocol compatibility

---
Generated: 2026-02-05 — Comprehensive instructions based on codebase analysis
