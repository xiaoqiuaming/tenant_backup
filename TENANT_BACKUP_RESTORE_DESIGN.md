# YaoBase 多租户不停服备份恢复设计

**版本**: 1.0  
**日期**: 2026-02-05  
**状态**: 设计阶段

## 目录
- [1. 概述](#1-概述)
- [2. 设计目标与约束](#2-设计目标与约束)
- [3. 架构设计](#3-架构设计)
- [4. 备份策略](#4-备份策略)
- [5. 恢复策略](#5-恢复策略)
  - [5.3 增量恢复流水线详细设计](#53-增量恢复流水线详细设计)
- [6. DAG任务调度](#6-dag任务调度)
- [7. Schema重写机制](#7-schema重写机制)
- [8. 数据一致性保证](#8-数据一致性保证)
- [9. 详细设计](#9-详细设计)
- [10. 实现计划](#10-实现计划)

---

## 1. 概述

### 1.1 背景

YaoBase是一个多租户分布式数据库系统，采用两层LSM-tree架构：
- **L0层（增量）**: TransServer管理MemTable和SSTable
- **L1层（基线）**: DataServer管理256MB范围分片的SSTable

当前多租户架构特点：
- CPU/内存在SqlServer隔离，磁盘在DataServer隔离
- **TransServer为共享资源**，所有租户共用
- 租户间库表通过权限隔离，不支持同名库
- 备份恢复功能是集群级别，不支持租户粒度
- 备份恢复均在原集群内执行，不引入独立BackupServer进程

### 1.2 需求

实现租户级别的不停服备份恢复能力：

1. **基线备份**: 完整的L1层SSTable数据备份
2. **增量备份**: TransServer的commit log备份
3. **PITR恢复**: Point-In-Time Recovery到指定时间点
4. **备租户验证**: 先恢复到备用租户验证数据正确性
5. **主备切换**: 验证通过后将备租户转为主租户
6. **Schema重写**: 备租户是全新库表，需重写SSTable的schema信息
7. **不停服**: 备份恢复过程不影响原租户的正常服务

---

## 2. 设计目标与约束

### 2.1 功能目标

| 目标 | 描述 | 优先级 |
|-----|------|--------|
| 租户级备份 | 支持指定租户的完整备份 | P0 |
| 增量备份 | 持续备份TransServer的commit log | P0 |
| 时间点恢复 | 恢复到任意历史时间点 | P0 |
| 备租户验证 | 在备租户中验证数据一致性 | P0 |
| 不停服操作 | 备份恢复不影响业务 | P0 |
| Schema隔离 | 备租户使用独立schema | P0 |
| 任务调度 | DAG调度复杂备份恢复任务 | P1 |
| 监控告警 | 备份恢复状态监控 | P1 |

### 2.2 技术约束

| 约束类型 | 约束内容 | 影响 |
|----------|----------|------|
| **TransServer共享** | 所有租户共享TS，无法直接隔离增量数据 | 需要从commit log过滤租户数据 |
| **Schema重写** | 备租户是全新库表，SSTable中的table_id、schema_version需重写 | 恢复时需解析并重写SSTable |
| **无同名库** | 多个租户不能创建同名数据库 | 备租户使用不同库名或独立namespace |
| **在线恢复** | 备份恢复任务运行在原集群（不使用BackupServer进程） | 需要资源隔离避免影响主租户 |
| **两层合并** | L0/L1数据需协同恢复 | 基线+增量恢复需保证一致性 |

### 2.3 性能目标

| 指标 | 目标 | 说明 |
|------|------|------|
| 备份吞吐 | ≥100MB/s/租户 | 基线备份速度 |
| 恢复吞吐 | ≥80MB/s/租户 | 基线恢复速度 |
| RPO | ≤60秒 | 最大数据丢失时间 |
| RTO | ≤30分钟 | 恢复时间目标（1TB数据） |
| 业务影响 | ≤5% | 备份恢复对主租户的性能影响 |

---

## 3. 架构设计

### 3.1 组件架构

```
┌─────────────────────────────────────────────────────────────────┐
│                         YaoBase 集群                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐  │
│  │  SqlServer   │      │ TransServer  │      │  DataServer  │  │
│  │  (租户隔离)  │◄────►│  (共享资源)  │◄────►│  (租户隔离)  │  │
│  └──────────────┘      └──────────────┘      └──────────────┘  │
│         │                      │                      │          │
│         │                      │                      │          │
│         │                      ▼                      │          │
│         │              ┌──────────────┐              │          │
│         │              │  CommitLog   │              │          │
│         │              │  (增量数据)  │              │          │
│         │              └──────────────┘              │          │
│         │                      │                      │          │
│         └──────────────────────┼──────────────────────┘          │
│                                │                                  │
│                                ▼                                  │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │         租户备份恢复管理器 (ObTenantBackupManager)          ││
│  ├─────────────────────────────────────────────────────────────┤│
│  │  ┌───────────────┐  ┌───────────────┐  ┌─────────────────┐ ││
│  │  │ DAG调度器     │  │ Schema重写器  │  │ 一致性验证器    │ ││
│  │  └───────────────┘  └───────────────┘  └─────────────────┘ ││
│  │  ┌───────────────┐  ┌───────────────┐  ┌─────────────────┐ ││
│  │  │ 基线备份器    │  │ 增量备份器    │  │ 恢复协调器      │ ││
│  │  └───────────────┘  └───────────────┘  └─────────────────┘ ││
│  └─────────────────────────────────────────────────────────────┘│
│                                │                                  │
└────────────────────────────────┼──────────────────────────────────┘
                                 │
                                 ▼
                    ┌────────────────────────┐
                    │   备份存储 (OSS/NFS)   │
                    ├────────────────────────┤
                    │  /backup/tenant_1/     │
                    │    ├── baseline/       │
                    │    │   └── sstable_*   │
                    │    ├── incremental/    │
                    │    │   └── log_*       │
                    │    └── meta/           │
                    │        ├── schema.json │
                    │        └── manifest    │
                    └────────────────────────┘
```

### 3.2 核心组件

#### 3.2.1 ObTenantBackupManager

租户备份恢复的中央协调器，运行在AdminServer节点。

**职责**：
- 管理租户级备份恢复任务生命周期
- 协调DAG任务调度
- 维护备份元数据（备份集、恢复点）
- 监控备份恢复进度和状态

**接口**：
```cpp
class ObTenantBackupManager {
public:
  // 创建全量备份任务
  int start_full_backup(int64_t tenant_id, const ObBackupParams& params);
  
  // 创建增量备份任务（持续运行）
  int start_incremental_backup(int64_t tenant_id, const ObIncrBackupParams& params);
  
  // 恢复到备租户
  int start_restore_to_standby(int64_t src_tenant_id, int64_t dest_tenant_id,
                                int64_t backup_set_id, int64_t restore_timestamp);
  
  // 将备租户提升为主租户
  int promote_standby_to_primary(int64_t standby_tenant_id, int64_t primary_tenant_id);
  
  // 查询备份任务状态
  int get_backup_status(int64_t tenant_id, ObBackupStatus& status);
};
```

#### 3.2.2 ObTenantBaselineBackuper

基线备份器，负责L1层SSTable的备份。

**职责**：
- 遍历租户的所有tablet
- 从DataServer拉取SSTable文件
- 生成备份元数据（tablet范围、sstable列表）
- 校验备份数据完整性

**关键逻辑**：
```cpp
class ObTenantBaselineBackuper {
public:
  int backup_tenant_baseline(int64_t tenant_id, int64_t frozen_version);
  
private:
  // 1. 获取租户的所有tablet信息
  int fetch_tenant_tablets(int64_t tenant_id, ObArray<ObTabletInfo>& tablets);
  
  // 2. 并发备份每个tablet的SSTable
  int backup_tablet_sstables(const ObTabletInfo& tablet, const char* backup_path);
  
  // 3. 生成基线备份元数据
  int generate_baseline_manifest(int64_t tenant_id, int64_t frozen_version);
};
```

#### 3.2.3 ObTenantIncrementalBackuper

增量备份器，持续备份TransServer的commit log。

**职责**：
- 实时订阅TransServer的commit log
- 过滤租户相关的日志记录
- 归档到备份存储（按时间分片）
- 维护日志序列号连续性

**关键逻辑**：
```cpp
class ObTenantIncrementalBackuper : public yysys::CDefaultRunnable {
public:
  int start_incremental_backup(int64_t tenant_id);
  void run(yysys::CThread* thread, void* arg) override;
  
private:
  // 1. 订阅TransServer commit log
  int subscribe_commit_log(int64_t start_log_id);
  
  // 2. 过滤租户事务
  bool is_tenant_log(const ObLogEntry& log, int64_t tenant_id);
  
  // 3. 归档日志文件
  int archive_log_file(const char* log_data, int64_t length, int64_t log_id);
  
  // 4. 更新备份进度
  int update_backup_progress(int64_t last_archived_log_id, int64_t timestamp);
};
```

#### 3.2.4 ObTenantRestoreCoordinator

恢复协调器，负责编排基线+增量的恢复流程。

**职责**：
- 创建备租户的schema（新的table_id）
- 协调基线SSTable的恢复
- 协调增量日志的replay
- 验证恢复数据的一致性

**关键流程**：
```cpp
class ObTenantRestoreCoordinator {
public:
  // 主恢复流程
  int restore_tenant(const ObRestoreTask& task);
  
private:
  // 阶段1: 准备备租户
  int prepare_standby_tenant(int64_t standby_tenant_id);
  
  // 阶段2: 恢复基线数据
  int restore_baseline(int64_t standby_tenant_id, int64_t backup_set_id);
  
  // 阶段3: 恢复增量日志
  int restore_incremental(int64_t standby_tenant_id, int64_t target_timestamp);
  
  // 阶段4: 验证数据一致性
  int verify_restored_data(int64_t standby_tenant_id);
};
```

#### 3.2.5 ObSchemaRewriter

Schema重写器，处理备租户的schema映射。

**职责**：
- 映射源租户table_id到备租户table_id
- 重写SSTable元数据中的schema信息
- 维护schema版本映射关系

**核心逻辑**：
```cpp
class ObSchemaRewriter {
public:
  // 创建schema映射关系
  int create_schema_mapping(int64_t src_tenant_id, int64_t dest_tenant_id);
  
  // 重写SSTable的schema信息
  int rewrite_sstable_schema(const char* src_sstable_path,
                             const char* dest_sstable_path,
                             const ObSchemaMapping& mapping);
  
private:
  // Schema映射结构
  struct ObSchemaMapping {
    hash::ObHashMap<uint64_t, uint64_t> table_id_map_;  // 旧table_id -> 新table_id
    hash::ObHashMap<uint64_t, uint64_t> schema_version_map_;
    int64_t src_tenant_id_;
    int64_t dest_tenant_id_;
  };
};
```

#### 3.2.6 ObBackupDagScheduler

DAG任务调度器，管理复杂的依赖任务流。

**职责**：
- 将备份恢复任务分解为DAG图
- 管理任务依赖关系和执行顺序
- 并发执行无依赖的子任务
- 处理任务失败重试

---

## 4. 备份策略

### 4.1 备份架构

```
租户备份架构:

Time ──────────────────────────────────────────────►

基线备份:  [Full Backup]────────────[Full Backup]─────
              ↓ T0                      ↓ T0+7d
              
增量备份:  ├──Log1──Log2──Log3──...──Log100──Log101...
           │  ↓1min ↓2min ↓3min      ↓100min
           │
           └─► 持续归档TransServer Commit Log

恢复能力:  任意时间点 T0 ≤ t ≤ Now
```

### 4.2 全量基线备份

#### 4.2.1 备份流程

```
┌─────────────────────────────────────────────────────────────┐
│                   全量基线备份流程                            │
└─────────────────────────────────────────────────────────────┘

1. 准备阶段
   ├── 创建备份任务记录
   ├── 获取当前frozen_version
   ├── 锁定schema版本
   └── 获取租户所有tablet列表

2. 数据备份阶段
   ├── 并发遍历所有DataServer
   │   ├── 按tenant_id过滤tablet
   │   ├── 拷贝SSTable文件到备份存储
   │   ├── 计算SSTable校验和
   │   └── 记录tablet元数据
   │
   └── 备份租户schema信息
       ├── 导出表定义（CREATE TABLE语句）
       ├── 导出索引定义
       └── 导出权限信息

3. 元数据生成阶段
   ├── 生成Manifest文件
   │   ├── 备份集ID
   │   ├── 租户ID
   │   ├── frozen_version
   │   ├── 备份时间戳
   │   ├── tablet列表
   │   └── SSTable文件列表
   │
   └── 上传元数据到备份存储

4. 验证阶段
   ├── 校验备份文件完整性
   ├── 验证所有tablet都已备份
   └── 标记备份任务完成
```

#### 4.2.2 关键实现

##### DataServer端备份接口

```cpp
// src/chunkserver/ob_chunk_service.h
class ObChunkService {
public:
  // 新增：租户级tablet备份接口
  int backup_tenant_tablets(const int64_t tenant_id,
                            const int64_t frozen_version,
                            const common::ObServer& backup_server,
                            common::ObDataBuffer& result_buffer);
  
private:
  // 获取租户的tablet列表
  int get_tenant_tablets(int64_t tenant_id, 
                         ObArray<ObTablet*>& tablets);
  
  // 拷贝tablet的SSTable到备份路径
  int copy_tablet_sstable(const ObTablet* tablet,
                         const char* backup_path);
};
```

##### 备份元数据结构

```cpp
// src/common/ob_tenant_backup_msg.h (扩展)

struct ObBaselineBackupManifest {
  int64_t backup_set_id_;           // 备份集ID
  int64_t tenant_id_;                // 租户ID
  int64_t frozen_version_;           // 冻结版本号
  int64_t backup_start_time_;        // 备份开始时间
  int64_t backup_end_time_;          // 备份结束时间
  int64_t data_size_;                // 备份数据大小
  int64_t tablet_count_;             // tablet数量
  
  // tablet元数据列表
  struct TabletBackupInfo {
    common::ObNewRange range_;       // tablet范围
    int64_t sstable_count_;          // SSTable数量
    ObArray<int64_t> sstable_ids_;   // SSTable ID列表
    int64_t data_size_;              // 数据大小
    uint64_t checksum_;              // 校验和
  };
  ObArray<TabletBackupInfo> tablet_list_;
  
  // Schema映射信息
  struct SchemaInfo {
    uint64_t table_id_;
    char table_name_[OB_MAX_TABLE_NAME_LENGTH];
    int64_t schema_version_;
  };
  ObArray<SchemaInfo> schema_list_;
  
  NEED_SERIALIZE_AND_DESERIALIZE;
};
```

##### 备份任务状态机

```cpp
enum ObBackupTaskStatus {
  BACKUP_INIT = 0,           // 初始化
  BACKUP_PREPARING,          // 准备阶段
  BACKUP_DOING,              // 执行中
  BACKUP_VERIFYING,          // 验证中
  BACKUP_COMPLETED,          // 完成
  BACKUP_FAILED,             // 失败
  BACKUP_CANCELLED           // 已取消
};

struct ObTenantBackupTask {
  int64_t task_id_;
  int64_t tenant_id_;
  int64_t backup_set_id_;
  ObBackupTaskStatus status_;
  int64_t start_time_;
  int64_t end_time_;
  int64_t total_tablets_;
  int64_t finished_tablets_;
  int64_t data_size_;
  int error_code_;
  char backup_path_[OB_MAX_URI_LENGTH];
  
  NEED_SERIALIZE_AND_DESERIALIZE;
};
```

### 4.3 增量日志备份

#### 4.3.1 备份流程

```
┌─────────────────────────────────────────────────────────────┐
│                   增量日志备份流程                            │
└─────────────────────────────────────────────────────────────┘

TransServer                   IncrBackuper              备份存储
     │                              │                        │
     │   commit log stream          │                        │
     ├─────────────────────────────►│                        │
     │   [tenant_id, log_id, data]  │                        │
     │                              │                        │
     │                              │ 1. 过滤租户日志         │
     │                              │ 2. 按时间分片          │
     │                              │ 3. 压缩日志            │
     │                              │                        │
     │                              ├───────────────────────►│
     │                              │  archive log file      │
     │                              │                        │
     │                              ◄───────────────────────┤
     │                              │  ACK                   │
     │                              │                        │
     │                              │ 4. 更新进度            │
     │                              │ 5. 清理过期日志        │
     │                              │                        │
```

#### 4.3.2 关键实现

##### 日志过滤器

```cpp
// src/updateserver/ob_tenant_log_filter.h

class ObTenantLogFilter {
public:
  // 从commit log提取租户事务
  int filter_tenant_log(const char* log_data, 
                       int64_t log_len,
                       int64_t tenant_id,
                       char* filtered_log,
                       int64_t& filtered_len);
  
private:
  // 解析日志中的tenant_id
  int extract_tenant_id_from_log(const char* log_data, 
                                 int64_t log_len,
                                 int64_t& tenant_id);
  
  // 判断是否为租户相关的日志记录
  bool is_tenant_related_log(const ObLogEntry& log, 
                            int64_t tenant_id);
};
```

##### 日志归档器

```cpp
// src/backupserver/ob_tenant_log_archiver.h

class ObTenantLogArchiver {
public:
  int start_archive(int64_t tenant_id, int64_t start_log_id);
  int stop_archive();
  
private:
  // 归档日志到文件（每小时一个文件）
  int archive_to_file(const char* log_data, 
                     int64_t log_len, 
                     int64_t log_id);
  
  // 日志文件命名: log_<tenant_id>_<yyyymmdd_hh>_<start_log_id>_<end_log_id>.arc
  int generate_log_filename(int64_t tenant_id,
                           int64_t start_log_id,
                           int64_t end_log_id,
                           char* filename);
  
  // 压缩日志文件
  int compress_log_file(const char* src_file, const char* dst_file);
  
  // 上传到备份存储
  int upload_log_file(const char* local_file, const char* remote_path);
};
```

##### 日志元数据

```cpp
// 增量日志备份元数据
struct ObIncrementalBackupMeta {
  int64_t tenant_id_;
  int64_t backup_set_id_;             // 对应的基线备份集ID
  int64_t min_log_id_;                // 最小日志序列号
  int64_t max_log_id_;                // 最大日志序列号
  int64_t start_timestamp_;           // 开始时间
  int64_t end_timestamp_;             // 结束时间
  
  struct LogFileInfo {
    char filename_[OB_MAX_FILE_NAME_LENGTH];
    int64_t start_log_id_;
    int64_t end_log_id_;
    int64_t file_size_;
    uint64_t checksum_;
  };
  ObArray<LogFileInfo> log_files_;
  
  NEED_SERIALIZE_AND_DESERIALIZE;
};
```

### 4.4 备份存储布局

```
/backup/
└── tenant_<tenant_id>/
    ├── baseline/                          # 基线备份目录
    │   ├── backup_set_<id>/               # 每个备份集一个目录
    │   │   ├── manifest.json              # 备份元数据
    │   │   ├── schema.json                # Schema信息
    │   │   ├── tablet_0_0/                # tablet备份数据
    │   │   │   ├── sstable_1001.sst
    │   │   │   ├── sstable_1002.sst
    │   │   │   └── checksum.txt
    │   │   ├── tablet_0_1/
    │   │   └── ...
    │   └── backup_set_<id+1>/
    │
    ├── incremental/                       # 增量备份目录
    │   ├── log_20260205_00_*.arc         # 按小时归档的日志
    │   ├── log_20260205_01_*.arc
    │   ├── log_20260205_02_*.arc
    │   └── ...
    │
    └── meta/                              # 全局元数据
        ├── backup_sets.json               # 所有备份集列表
        ├── checkpoint.json                # 增量备份检查点
        └── restore_history.json           # 恢复历史记录
```

---

## 5. 恢复策略

### 5.1 恢复架构

```
┌─────────────────────────────────────────────────────────────┐
│                  恢复到备租户流程                             │
└─────────────────────────────────────────────────────────────┘

阶段1: 准备备租户
  ├── 创建新租户（standby_tenant_id）
  ├── 创建全新的database和table结构
  ├── 分配新的table_id（避免冲突）
  └── 建立schema映射关系（old_table_id -> new_table_id）

阶段2: 恢复基线数据
  ├── 读取备份集manifest
  ├── 并发恢复tablet
  │   ├── 下载SSTable文件
  │   ├── **重写SSTable元数据**（table_id, schema_version）
  │   ├── 创建hardlink到DataServer数据目录
  │   └── 注册tablet到TabletManager
  └── 等待所有tablet恢复完成

阶段3: 恢复增量日志
  ├── 确定日志恢复范围（baseline_ts -> target_ts）
  ├── 顺序下载归档日志文件
  ├── **重写日志中的table_id**（映射到新table_id）
  ├── Replay日志到TransServer
  └── 等待日志应用完成

阶段4: 验证数据一致性
  ├── 校验恢复的数据量
  ├── 执行数据校验查询
  ├── 验证索引完整性
  └── 标记恢复成功

备份租户可用 ✓
  ├── 支持只读查询
  └── 等待提升为主租户
```

### 5.2 关键技术点

#### 5.2.1 Schema映射与重写

**挑战**: 备租户使用全新的table_id，但SSTable和commit log中存储的是旧table_id。

**解决方案**: 建立schema映射表，在恢复时重写。

```cpp
// src/backupserver/ob_tenant_schema_rewriter.h

class ObTenantSchemaRewriter {
public:
  // 创建schema映射
  int create_schema_mapping(int64_t src_tenant_id, 
                           int64_t dest_tenant_id,
                           const ObSchemaManagerV2& src_schema,
                           ObSchemaManagerV2& dest_schema);
  
  // 重写SSTable元数据
  int rewrite_sstable(const char* src_sstable_path,
                     const char* dest_sstable_path,
                     const ObSchemaMapping& mapping);
  
  // 重写commit log
  int rewrite_commit_log(const char* src_log,
                        int64_t src_len,
                        const ObSchemaMapping& mapping,
                        char* dest_log,
                        int64_t& dest_len);
  
private:
  struct ObSchemaMapping {
    // table_id映射
    hash::ObHashMap<uint64_t, uint64_t> table_id_map_;
    
    // column_id映射（table维度）
    struct ColumnMapping {
      hash::ObHashMap<uint64_t, uint64_t> column_id_map_;
    };
    hash::ObHashMap<uint64_t, ColumnMapping> column_maps_;
    
    // schema_version映射
    hash::ObHashMap<int64_t, int64_t> schema_version_map_;
  };
  
  // 重写SSTable的trailer（元数据在文件尾部）
  int rewrite_sstable_trailer(sstable::ObSSTableReader& reader,
                              sstable::ObSSTableWriter& writer,
                              const ObSchemaMapping& mapping);
};
```

**SSTable重写细节**:

YaoBase的SSTable格式（参考 `src/sstable/ob_sstable_schema.h`）：
```
[Data Blocks] [Bloom Filter] [Trailer]
                               ↑ 需要重写这部分
```

Trailer包含：
- table_id
- schema_version
- column schema信息

重写步骤：
1. 读取原SSTable的trailer
2. 映射table_id和schema_version
3. 重新生成bloom filter（如果table_id影响hash）
4. 写入新SSTable

#### 5.2.2 增量日志重写

**Commit Log格式** (参考 `src/updateserver/ob_ups_log_mgr.h`):
```cpp
struct ObLogEntry {
  int64_t log_id_;
  int64_t log_seq_;
  int64_t timestamp_;
  int32_t cmd_;              // OB_UPS_MUTATOR, OB_TRANS_COMMIT等
  char data_[0];             // 序列化的ObMutator
};

struct ObMutator {
  int64_t tenant_id_;        // 租户ID（在多租户版本中）
  ObArray<ObMutatorCell> cells_;
};

struct ObMutatorCell {
  uint64_t table_id_;        // ← 需要重写
  ObRowkey row_key_;
  uint64_t column_id_;       // ← 可能需要重写
  ObObj value_;
};
```

**重写流程**:
```cpp
int ObTenantSchemaRewriter::rewrite_commit_log(
    const char* src_log, int64_t src_len,
    const ObSchemaMapping& mapping,
    char* dest_log, int64_t& dest_len) 
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  
  // 1. 反序列化原日志
  ObLogEntry log_entry;
  if (OB_SUCCESS != (ret = log_entry.deserialize(src_log, src_len, pos))) {
    YYSYS_LOG(ERROR, "deserialize log entry failed, ret=%d", ret);
    return ret;
  }
  
  // 2. 解析mutator
  ObMutator mutator;
  pos = 0;
  if (OB_SUCCESS != (ret = mutator.deserialize(log_entry.data_, 
                                                log_entry.get_data_len(), pos))) {
    YYSYS_LOG(ERROR, "deserialize mutator failed, ret=%d", ret);
    return ret;
  }
  
  // 3. 重写每个cell的table_id和column_id
  for (int64_t i = 0; i < mutator.cells_.count(); ++i) {
    ObMutatorCell& cell = mutator.cells_.at(i);
    
    // 映射table_id
    uint64_t new_table_id = 0;
    if (OB_SUCCESS != (ret = mapping.table_id_map_.get_refactored(
                              cell.table_id_, new_table_id))) {
      YYSYS_LOG(ERROR, "table_id mapping not found, old_id=%lu", cell.table_id_);
      return ret;
    }
    cell.table_id_ = new_table_id;
    
    // 映射column_id（如果需要）
    // ...
  }
  
  // 4. 序列化新日志
  pos = 0;
  if (OB_SUCCESS != (ret = mutator.serialize(dest_log, dest_len, pos))) {
    YYSYS_LOG(ERROR, "serialize mutator failed, ret=%d", ret);
    return ret;
  }
  dest_len = pos;
  
  return ret;
}
```

#### 5.2.3 备租户恢复协调

```cpp
// src/backupserver/ob_tenant_restore_coordinator.h

class ObTenantRestoreCoordinator {
public:
  int restore_tenant_to_standby(const ObRestoreTask& task);
  
private:
  // 阶段1: 准备
  int prepare_standby_tenant(const ObRestoreTask& task,
                            ObSchemaMapping& schema_mapping);
  
  // 阶段2: 恢复基线
  int restore_baseline_phase(const ObRestoreTask& task,
                            const ObSchemaMapping& schema_mapping);
  
  // 阶段3: 恢复增量
  int restore_incremental_phase(const ObRestoreTask& task,
                               const ObSchemaMapping& schema_mapping);
  
  // 阶段4: 验证
  int verify_restore_phase(const ObRestoreTask& task);
  
  // 子任务: 恢复单个tablet
  int restore_single_tablet(const ObTabletBackupInfo& backup_info,
                           int64_t standby_tenant_id,
                           const ObSchemaMapping& schema_mapping);
  
  // 子任务: Replay日志段
  int replay_log_segment(const char* log_file_path,
                        int64_t standby_tenant_id,
                        const ObSchemaMapping& schema_mapping);
};
```

#### 5.2.4 不停服保证

**关键机制**:

1. **数据隔离**: 备租户使用独立的tenant_id和table_id，不影响原租户。

2. **资源限制**: 恢复任务在DAG调度器中设置低优先级，限制CPU/IO资源占用。

3. **渐进式恢复**: 分批恢复tablet，每批完成后暂停一段时间，避免集中占用资源。

4. **读写隔离**: 
   - 基线恢复：直接写入 DataServer，不经过TransServer
   - 增量恢复：通过单独的replay线程，与正常业务隔离

```cpp
// 资源限制配置
struct ObRestoreResourceLimit {
  int64_t max_concurrent_tablets_;    // 最大并发恢复tablet数，默认5
  int64_t io_bandwidth_limit_mb_;     // IO带宽限制，默认100MB/s
  int64_t cpu_quota_percent_;         // CPU配额百分比，默认10%
  int64_t batch_pause_interval_ms_;   // 批次间暂停时间，默认1000ms
};
```

### 5.3 增量恢复流水线详细设计

#### 5.3.1 流水线架构

基于你的设计思路，增量恢复采用 **单生产者-多消费者-单提交者** 的流水线模型：

```
┌─────────────────────────────────────────────────────────────────┐
│                    增量恢复流水线架构                             │
└─────────────────────────────────────────────────────────────────┘

[Reader Thread]
      │
      │ 读取归档日志文件
      ▼
┌───────────┐
│ Log Entry │──► Queue 1 (Raw Log Buffer, 容量1000)
└───────────┘
      │
      │ 多线程并行处理
      ├──────────────┬──────────────┬──────────────┐
      ▼              ▼              ▼              ▼
[Worker-1]      [Worker-2]      [Worker-3]   ... [Worker-N]
   │               │               │               │
   │ 1.反序列化    │               │               │
   │ 2.重写schema  │               │               │
   │ 3.序列化      │               │               │
   ▼               ▼               ▼               ▼
┌──────────────────────────────────────────────────┐
│  Reordered Queue (按原始log_id有序, 容量5000)    │
└──────────────────────────────────────────────────┘
      │
      │ 单线程顺序提交（保证顺序）
      ▼
[Applier Thread]
      │
      │ 1. 按序取出重写后的log
      │ 2. 构造ObMutator提交到TransServer
      │ 3. 等待TransServer返回commit结果
      │ 4. 记录checkpoint（log_id + 时间戳）
      ▼
[TransServer]
      │
      │ 正常事务处理流程
      ├─► Apply到MemTable
      ├─► 获取log_seq
      └─► 同步给备机（Paxos/Raft）
```

#### 5.3.2 详细组件设计

##### Reader Thread - 日志读取线程

```cpp
// src/backupserver/ob_log_restore_reader.h

class ObLogRestoreReader : public yysys::CDefaultRunnable {
public:
  int init(const char* log_file_path,
          ObLogRestoreQueue* raw_log_queue);
  
  void run(yysys::CThread* thread, void* arg) override;
  void stop();
  
private:
  // 读取一个归档日志文件
  int read_archived_log_file(const char* file_path);
  
  // 解压日志文件
  int decompress_log_file(const char* compressed_file,
                         char* decompressed_buf,
                         int64_t& decompressed_len);
  
  // 解析日志entry（不反序列化mutator内容）
  int parse_log_entry(const char* log_data,
                     int64_t log_len,
                     ObRawLogEntry& entry);
  
  // 推入原始日志队列
  int push_raw_log_entry(const ObRawLogEntry& entry);
  
private:
  bool is_stop_;
  const char* log_file_path_;
  ObLogRestoreQueue* raw_log_queue_;  // 原始日志队列
  int64_t read_log_count_;
  int64_t read_bytes_;
};

// 原始日志entry（尚未反序列化mutator）
struct ObRawLogEntry {
  int64_t log_id_;              // 原始日志ID（用于排序）
  int64_t log_seq_;
  int64_t timestamp_;
  int32_t cmd_;                 // OB_UPS_MUTATOR, OB_TRANS_COMMIT等
  char* log_data_;              // 原始日志数据（需要反序列化）
  int64_t log_data_len_;
  
  ObRawLogEntry() 
    : log_id_(0), log_seq_(0), timestamp_(0), cmd_(0),
      log_data_(nullptr), log_data_len_(0) {}
  
  ~ObRawLogEntry() {
    if (nullptr != log_data_) {
      ob_free(log_data_);
      log_data_ = nullptr;
    }
  }
};
```

##### Worker Threads - 并行处理线程池

```cpp
// src/backupserver/ob_log_restore_worker.h

class ObLogRestoreWorker : public yysys::CDefaultRunnable {
public:
  int init(int worker_id,
          ObLogRestoreQueue* input_queue,
          ObReorderedLogQueue* output_queue,
          const ObSchemaMapping* schema_mapping);
  
  void run(yysys::CThread* thread, void* arg) override;
  void stop();
  
private:
  // 处理一条日志
  int process_log_entry(const ObRawLogEntry& raw_entry,
                       ObRewrittenLogEntry& rewritten_entry);
  
  // 1. 反序列化mutator
  int deserialize_mutator(const char* log_data,
                         int64_t log_data_len,
                         ObMutator& mutator);
  
  // 2. 重写schema信息（tenant_id, table_id, column_id）
  int rewrite_mutator_schema(ObMutator& mutator,
                            const ObSchemaMapping& mapping);
  
  // 3. 序列化重写后的mutator
  int serialize_mutator(const ObMutator& mutator,
                       char* buffer,
                       int64_t buffer_len,
                       int64_t& pos);
  
  // 推入有序队列（会自动按log_id排序）
  int push_reordered_log(const ObRewrittenLogEntry& entry);
  
private:
  bool is_stop_;
  int worker_id_;
  ObLogRestoreQueue* input_queue_;
  ObReorderedLogQueue* output_queue_;
  const ObSchemaMapping* schema_mapping_;
  
  // 统计信息
  int64_t processed_count_;
  int64_t rewrite_error_count_;
};

// 重写后的日志entry
struct ObRewrittenLogEntry {
  int64_t log_id_;              // 原始日志ID（用于排序）
  int64_t log_seq_;             // 原始log_seq（恢复时会重新分配）
  int64_t timestamp_;
  int32_t cmd_;
  char* rewritten_data_;        // 重写后的日志数据
  int64_t rewritten_data_len_;
  int64_t tenant_id_;           // 新租户ID（备租户）
  
  ObRewrittenLogEntry()
    : log_id_(0), log_seq_(0), timestamp_(0), cmd_(0),
      rewritten_data_(nullptr), rewritten_data_len_(0), tenant_id_(0) {}
  
  ~ObRewrittenLogEntry() {
    if (nullptr != rewritten_data_) {
      ob_free(rewritten_data_);
      rewritten_data_ = nullptr;
    }
  }
};
```

##### Reordered Queue - 有序队列（关键）

```cpp
// src/backupserver/ob_reordered_log_queue.h

/**
 * 有序日志队列：保证输出的日志按log_id严格递增
 * 
 * 设计要点：
 * 1. 内部使用priority_queue + HashMap维护
 * 2. Pop时只返回连续的log（避免空洞）
 * 3. 支持超时检测（如果某个log_id长时间不到，告警）
 */
class ObReorderedLogQueue {
public:
  ObReorderedLogQueue() 
    : next_expected_log_id_(0), queue_size_(0), max_size_(5000) {}
  
  // 推入重写后的日志（可能乱序）
  int push(const ObRewrittenLogEntry& entry);
  
  // 弹出下一条连续的日志（阻塞，直到有连续日志或超时）
  int pop(ObRewrittenLogEntry& entry, int64_t timeout_us);
  
  // 尝试非阻塞弹出
  int try_pop(ObRewrittenLogEntry& entry);
  
  // 检查是否有连续日志可取
  bool has_continuous_log() const;
  
  // 获取队列状态
  struct QueueStatus {
    int64_t next_expected_log_id_;  // 期望的下一个log_id
    int64_t queue_size_;             // 当前队列大小
    int64_t max_log_id_in_queue_;    // 队列中最大的log_id
    int64_t hole_count_;             // 空洞数量（未连续的log）
  };
  int get_status(QueueStatus& status) const;
  
private:
  // 内部存储：按log_id排序的map
  std::map<int64_t, ObRewrittenLogEntry> log_map_;
  
  // 下一个期望的log_id（用于保证连续性）
  int64_t next_expected_log_id_;
  
  // 队列大小和容量
  volatile int64_t queue_size_;
  int64_t max_size_;
  
  // 同步原语
  mutable yysys::CThreadMutex mutex_;
  yysys::CThreadCond has_continuous_log_cond_;  // 有连续日志可用时通知
  
  DISALLOW_COPY_AND_ASSIGN(ObReorderedLogQueue);
};

// 实现要点
int ObReorderedLogQueue::push(const ObRewrittenLogEntry& entry) {
  int ret = OB_SUCCESS;
  yysys::CThreadGuard guard(&mutex_);
  
  // 1. 检查队列容量
  if (queue_size_ >= max_size_) {
    ret = OB_SIZE_OVERFLOW;
    YYSYS_LOG(WARN, "reordered queue is full, size=%ld", queue_size_);
    return ret;
  }
  
  // 2. 插入map（按log_id排序）
  log_map_[entry.log_id_] = entry;
  queue_size_++;
  
  // 3. 检查是否产生了连续日志
  if (entry.log_id_ == next_expected_log_id_) {
    // 正好是期望的log_id，通知applier线程
    has_continuous_log_cond_.signal();
  }
  
  return ret;
}

int ObReorderedLogQueue::pop(ObRewrittenLogEntry& entry, int64_t timeout_us) {
  int ret = OB_SUCCESS;
  yysys::CThreadGuard guard(&mutex_);
  
  int64_t start_time = yysys::CTimeUtil::getTime();
  
  while (true) {
    // 1. 检查是否有连续日志
    auto it = log_map_.find(next_expected_log_id_);
    if (it != log_map_.end()) {
      // 找到连续的log
      entry = it->second;
      log_map_.erase(it);
      queue_size_--;
      next_expected_log_id_++;
      return OB_SUCCESS;
    }
    
    // 2. 没有连续日志，检查超时
    int64_t elapsed = yysys::CTimeUtil::getTime() - start_time;
    if (elapsed >= timeout_us) {
      ret = OB_TIMEOUT;
      
      // 告警：可能有日志丢失或处理失败
      if (!log_map_.empty()) {
        YYSYS_LOG(WARN, "wait for continuous log timeout, "
                       "expected_log_id=%ld, min_log_id_in_queue=%ld, "
                       "queue_size=%ld",
                  next_expected_log_id_, log_map_.begin()->first, queue_size_);
      }
      return ret;
    }
    
    // 3. 等待通知（有新log到达）
    int64_t remain_time = timeout_us - elapsed;
    has_continuous_log_cond_.wait(remain_time);
  }
  
  return ret;
}
```

##### Applier Thread - 顺序应用线程

```cpp
// src/backupserver/ob_log_restore_applier.h

class ObLogRestoreApplier : public yysys::CDefaultRunnable {
public:
  int init(int64_t standby_tenant_id,
          ObReorderedLogQueue* input_queue,
          ObUpdateServer* update_server);  // TransServer引用
  
  void run(yysys::CThread* thread, void* arg) override;
  void stop();
  
private:
  // 应用一条日志到TransServer
  int apply_log_entry(const ObRewrittenLogEntry& entry);
  
  // 提交mutator到TransServer（走正常事务流程）
  int submit_mutator_to_trans_server(const ObMutator& mutator,
                                     int64_t& assigned_log_seq);
  
  // 等待事务提交完成
  int wait_trans_commit(const ObTransID& trans_id, int64_t timeout_ms);
  
  // 记录恢复进度（checkpoint）
  int update_restore_checkpoint(int64_t last_applied_log_id,
                                int64_t timestamp);
  
  // 批量提交优化（可选）
  int apply_log_batch(const ObArray<ObRewrittenLogEntry>& entries);
  
private:
  bool is_stop_;
  int64_t standby_tenant_id_;
  ObReorderedLogQueue* input_queue_;
  ObUpdateServer* update_server_;
  
  // 恢复进度
  int64_t last_applied_log_id_;
  int64_t applied_log_count_;
  int64_t applied_trans_count_;
  
  // 性能统计
  int64_t total_apply_time_us_;
  int64_t avg_apply_latency_us_;
};

// 关键实现：应用日志
int ObLogRestoreApplier::apply_log_entry(const ObRewrittenLogEntry& entry) {
  int ret = OB_SUCCESS;
  int64_t start_time = yysys::CTimeUtil::getTime();
  
  // 1. 反序列化重写后的mutator
  ObMutator mutator;
  int64_t pos = 0;
  if (OB_SUCCESS != (ret = mutator.deserialize(entry.rewritten_data_,
                                               entry.rewritten_data_len_,
                                               pos))) {
    YYSYS_LOG(ERROR, "deserialize mutator failed, log_id=%ld, ret=%d",
              entry.log_id_, ret);
    return ret;
  }
  
  // 2. 设置租户上下文
  mutator.set_tenant_id(standby_tenant_id_);
  
  // 3. 提交到TransServer（走正常写入流程）
  int64_t new_log_seq = 0;
  if (OB_SUCCESS != (ret = submit_mutator_to_trans_server(mutator, new_log_seq))) {
    YYSYS_LOG(ERROR, "submit mutator to trans server failed, log_id=%ld, ret=%d",
              entry.log_id_, ret);
    return ret;
  }
  
  // 4. 等待TransServer同步到备机（如果需要强一致性）
  // 注意：这里可以选择异步，提高吞吐量
  ObTransID trans_id = mutator.get_trans_id();
  if (OB_SUCCESS != (ret = wait_trans_commit(trans_id, 5000 /* 5秒超时 */))) {
    YYSYS_LOG(ERROR, "wait trans commit failed, trans_id=%s, ret=%d",
              to_cstring(trans_id), ret);
    return ret;
  }
  
  // 5. 更新恢复进度
  last_applied_log_id_ = entry.log_id_;
  applied_log_count_++;
  
  // 每1000条日志记录一次checkpoint
  if (applied_log_count_ % 1000 == 0) {
    if (OB_SUCCESS != (ret = update_restore_checkpoint(entry.log_id_,
                                                       entry.timestamp_))) {
      YYSYS_LOG(WARN, "update restore checkpoint failed, ret=%d", ret);
      // 不影响继续恢复
    }
  }
  
  // 6. 统计性能
  int64_t elapsed = yysys::CTimeUtil::getTime() - start_time;
  total_apply_time_us_ += elapsed;
  avg_apply_latency_us_ = total_apply_time_us_ / applied_log_count_;
  
  if (applied_log_count_ % 10000 == 0) {
    YYSYS_LOG(INFO, "restore progress: applied_log_count=%ld, "
                    "last_log_id=%ld, avg_latency=%ldus, throughput=%ld ops/s",
              applied_log_count_, last_applied_log_id_, avg_apply_latency_us_,
              1000000 / avg_apply_latency_us_);
  }
  
  return ret;
}

// 提交到TransServer
int ObLogRestoreApplier::submit_mutator_to_trans_server(
    const ObMutator& mutator,
    int64_t& assigned_log_seq)
{
  int ret = OB_SUCCESS;
  
  // 构造写入请求（类似普通客户端写入）
  ObWriteParam write_param;
  write_param.set_tenant_id(standby_tenant_id_);
  write_param.set_mutator(mutator);
  write_param.set_is_restore_replay(true);  // 标记为恢复replay
  
  // 调用TransServer的apply接口
  // 注意：这里需要TransServer支持特殊的恢复模式
  if (OB_SUCCESS != (ret = update_server_->apply(write_param, assigned_log_seq))) {
    YYSYS_LOG(ERROR, "trans server apply failed, ret=%d", ret);
    return ret;
  }
  
  // TransServer内部会：
  // 1. Apply到MemTable
  // 2. 分配新的log_seq（不使用备份中的旧log_seq）
  // 3. 写入commit log
  // 4. 通过Paxos/Raft同步给备机
  
  return ret;
}
```

#### 5.3.3 并发控制与性能优化

##### 线程配置建议

```cpp
struct ObLogRestoreConcurrency {
  int64_t reader_thread_count_;     // 读取线程数：1（单生产者）
  int64_t worker_thread_count_;     // 处理线程数：8-16（根据CPU核数）
  int64_t applier_thread_count_;    // 应用线程数：1（保证顺序）
  
  int64_t raw_log_queue_size_;      // 原始日志队列：1000
  int64_t reordered_queue_size_;    // 有序队列：5000
  
  // 推荐配置
  static ObLogRestoreConcurrency get_default_config() {
    ObLogRestoreConcurrency config;
    config.reader_thread_count_ = 1;
    config.worker_thread_count_ = 
        std::min(16L, sysconf(_SC_NPROCESSORS_ONLN) / 2);  // CPU核数的一半
    config.applier_thread_count_ = 1;
    config.raw_log_queue_size_ = 1000;
    config.reordered_queue_size_ = 5000;
    return config;
  }
};
```

##### 性能优化技巧

```cpp
// 优化1: 批量应用（可选，需要评估收益）
int ObLogRestoreApplier::apply_log_batch(
    const ObArray<ObRewrittenLogEntry>& entries)
{
  int ret = OB_SUCCESS;
  
  // 将多条日志合并为一个大的mutator提交
  // 优点：减少RPC次数，提高吞吐量
  // 缺点：增加内存占用，延迟变高
  
  ObMutator batch_mutator;
  for (int64_t i = 0; i < entries.count(); ++i) {
    ObMutator single_mutator;
    // 反序列化并合并到batch_mutator
    batch_mutator.append(single_mutator);
  }
  
  // 提交批量mutator
  int64_t assigned_log_seq = 0;
  if (OB_SUCCESS != (ret = submit_mutator_to_trans_server(batch_mutator,
                                                          assigned_log_seq))) {
    YYSYS_LOG(ERROR, "submit batch mutator failed, ret=%d", ret);
    return ret;
  }
  
  return ret;
}

// 优化2: 预取下一批日志（流水线优化）
class ObLogRestoreReader {
private:
  // 异步预取下一个日志文件
  int prefetch_next_log_file(const char* next_file_path);
  
  // 使用双缓冲：当前读取buffer + 预取buffer
  char* current_buffer_;
  char* prefetch_buffer_;
};

// 优化3: Zero-copy传递（避免内存拷贝）
// 使用智能指针共享log_data，避免频繁拷贝
struct ObRawLogEntry {
  std::shared_ptr<char[]> log_data_ptr_;  // 使用shared_ptr
  int64_t log_data_len_;
};
```

#### 5.3.4 完整流水线的协调器

```cpp
// src/backupserver/ob_incremental_restore_pipeline.h

class ObIncrementalRestorePipeline {
public:
  int init(int64_t standby_tenant_id,
          const ObSchemaMapping& schema_mapping,
          ObUpdateServer* update_server);
  
  // 启动流水线
  int start_restore(const ObArray<const char*>& log_file_paths);
  
  // 停止流水线
  int stop();
  
  // 等待完成
  int wait_finish();
  
  // 获取恢复进度
  struct RestoreProgress {
    int64_t total_log_files_;
    int64_t processed_log_files_;
    int64_t total_log_entries_;
    int64_t applied_log_entries_;
    int64_t current_throughput_;  // 当前吞吐量（ops/s）
  };
  int get_progress(RestoreProgress& progress) const;
  
private:
  // 组件
  ObLogRestoreReader* reader_;
  ObArray<ObLogRestoreWorker*> workers_;
  ObLogRestoreApplier* applier_;
  
  // 队列
  ObLogRestoreQueue raw_log_queue_;
  ObReorderedLogQueue reordered_queue_;
  
  // 线程
  yysys::CThread* reader_thread_;
  ObArray<yysys::CThread*> worker_threads_;
  yysys::CThread* applier_thread_;
  
  // 配置
  int64_t standby_tenant_id_;
  ObSchemaMapping schema_mapping_;
  ObUpdateServer* update_server_;
  ObLogRestoreConcurrency concurrency_config_;
  
  // 状态
  volatile bool is_running_;
  int64_t last_applied_log_id_;
};

// 启动流水线
int ObIncrementalRestorePipeline::start_restore(
    const ObArray<const char*>& log_file_paths)
{
  int ret = OB_SUCCESS;
  
  // 1. 初始化队列
  if (OB_SUCCESS != (ret = raw_log_queue_.init(
                            concurrency_config_.raw_log_queue_size_))) {
    YYSYS_LOG(ERROR, "init raw log queue failed, ret=%d", ret);
    return ret;
  }
  
  if (OB_SUCCESS != (ret = reordered_queue_.init(
                            concurrency_config_.reordered_queue_size_))) {
    YYSYS_LOG(ERROR, "init reordered queue failed, ret=%d", ret);
    return ret;
  }
  
  // 2. 创建reader线程
  reader_ = new ObLogRestoreReader();
  reader_->init(log_file_paths, &raw_log_queue_);
  reader_thread_ = new yysys::CThread();
  reader_thread_->start(reader_, nullptr);
  
  // 3. 创建worker线程池
  for (int64_t i = 0; i < concurrency_config_.worker_thread_count_; ++i) {
    ObLogRestoreWorker* worker = new ObLogRestoreWorker();
    worker->init(i, &raw_log_queue_, &reordered_queue_, &schema_mapping_);
    
    yysys::CThread* thread = new yysys::CThread();
    thread->start(worker, nullptr);
    
    workers_.push_back(worker);
    worker_threads_.push_back(thread);
  }
  
  // 4. 创建applier线程
  applier_ = new ObLogRestoreApplier();
  applier_->init(standby_tenant_id_, &reordered_queue_, update_server_);
  applier_thread_ = new yysys::CThread();
  applier_thread_->start(applier_, nullptr);
  
  is_running_ = true;
  
  YYSYS_LOG(INFO, "incremental restore pipeline started, "
                  "worker_count=%ld, log_file_count=%ld",
            concurrency_config_.worker_thread_count_, log_file_paths.count());
  
  return ret;
}
```

#### 5.3.5 异常处理与容错

```cpp
// 关键异常场景处理

// 1. Worker线程处理失败
int ObLogRestoreWorker::process_log_entry(
    const ObRawLogEntry& raw_entry,
    ObRewrittenLogEntry& rewritten_entry)
{
  int ret = OB_SUCCESS;
  
  // 尝试3次重试
  for (int retry = 0; retry < 3; ++retry) {
    ret = do_rewrite_log(raw_entry, rewritten_entry);
    if (OB_SUCCESS == ret) {
      break;
    }
    
    YYSYS_LOG(WARN, "rewrite log failed, log_id=%ld, retry=%d, ret=%d",
              raw_entry.log_id_, retry, ret);
    usleep(100000);  // 100ms
  }
  
  if (OB_SUCCESS != ret) {
    // 持久化失败的日志，人工介入
    persist_failed_log(raw_entry);
    
    // 标记为skip（允许跳过某些无关紧要的日志）
    // 或者终止恢复（严格模式）
    if (is_strict_mode_) {
      YYSYS_LOG(ERROR, "rewrite log failed after retries, abort restore, "
                       "log_id=%ld", raw_entry.log_id_);
      abort_restore();
    }
  }
  
  return ret;
}

// 2. 有序队列检测空洞（日志丢失）
int ObReorderedLogQueue::detect_log_hole() {
  yysys::CThreadGuard guard(&mutex_);
  
  if (log_map_.empty()) {
    return OB_SUCCESS;
  }
  
  int64_t min_log_id = log_map_.begin()->first;
  int64_t gap = min_log_id - next_expected_log_id_;
  
  if (gap > 100) {  // 超过100条日志的空洞，告警
    YYSYS_LOG(ERROR, "detected large log hole, expected=%ld, min_in_queue=%ld, "
                     "gap=%ld, may indicate log loss or worker failure",
              next_expected_log_id_, min_log_id, gap);
    
    // 触发告警，暂停恢复
    return OB_LOG_HOLE_DETECTED;
  }
  
  return OB_SUCCESS;
}

// 3. Applier应用超时
int ObLogRestoreApplier::apply_with_timeout(
    const ObRewrittenLogEntry& entry,
    int64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  int64_t start_time = yysys::CTimeUtil::getTime();
  
  ret = apply_log_entry(entry);
  
  int64_t elapsed_ms = (yysys::CTimeUtil::getTime() - start_time) / 1000;
  if (elapsed_ms > timeout_ms) {
    YYSYS_LOG(ERROR, "apply log timeout, log_id=%ld, elapsed=%ldms, "
                     "timeout=%ldms, trans_server may be overloaded",
              entry.log_id_, elapsed_ms, timeout_ms);
    ret = OB_TIMEOUT;
  }
  
  return ret;
}
```

#### 5.3.6 性能监控与调优

```cpp
// 实时监控指标
struct ObRestorePipelineMetrics {
  // Reader指标
  int64_t read_log_count_;
  int64_t read_throughput_mb_;  // MB/s
  
  // Worker指标（聚合）
  int64_t rewrite_log_count_;
  int64_t rewrite_error_count_;
  int64_t avg_rewrite_latency_us_;
  
  // 队列指标
  int64_t raw_queue_size_;
  int64_t raw_queue_capacity_;
  int64_t reordered_queue_size_;
  int64_t reordered_queue_hole_count_;  // 空洞数量
  
  // Applier指标
  int64_t applied_log_count_;
  int64_t apply_throughput_ops_;  // ops/s
  int64_t avg_apply_latency_us_;
  
  // 整体指标
  int64_t end_to_end_latency_ms_;  // 端到端延迟
  double pipeline_efficiency_;     // 流水线效率（0-1）
};

// 动态调优：根据队列状态调整并发度
class ObPipelineAutoTuner {
public:
  void tune(ObIncrementalRestorePipeline* pipeline) {
    ObRestorePipelineMetrics metrics;
    pipeline->get_metrics(metrics);
    
    // 如果raw_queue经常满，增加worker线程
    if (metrics.raw_queue_size_ > metrics.raw_queue_capacity_ * 0.9) {
      increase_worker_threads(pipeline);
    }
    
    // 如果reordered_queue经常满，说明applier太慢
    if (metrics.reordered_queue_size_ > metrics.reordered_queue_capacity_ * 0.9) {
      // Applier是单线程，考虑批量优化或优化apply逻辑
      enable_batch_apply(pipeline);
    }
    
    // 如果空洞太多，减少worker并发（避免乱序太严重）
    if (metrics.reordered_queue_hole_count_ > 500) {
      decrease_worker_threads(pipeline);
    }
  }
};
```

#### 5.3.7 TransServer改造需求

为了支持增量恢复流水线，TransServer需要新增以下能力：

```cpp
// src/updateserver/ob_update_server.h

class ObUpdateServer {
public:
  /**
   * 恢复模式apply接口
   * 与普通写入的区别：
   * 1. 不需要客户端连接和会话
   * 2. 不执行权限检查（因为是管理员触发的恢复）
   * 3. 可以批量提交
   * 4. 需要保持原有的事务语义（ACID）
   */
  int apply_restore_mutator(const ObRestoreApplyParam& param,
                           int64_t& assigned_log_seq);
  
private:
  // 恢复apply的内部实现
  int do_restore_apply(const ObMutator& mutator,
                      int64_t tenant_id,
                      bool need_wait_sync,
                      int64_t& assigned_log_seq);
};

// 恢复apply参数
struct ObRestoreApplyParam {
  int64_t tenant_id_;               // 备租户ID
  ObMutator mutator_;               // 重写后的mutator
  bool need_wait_sync_;             // 是否等待同步到备机
  int64_t restore_task_id_;         // 恢复任务ID（用于追踪）
  
  NEED_SERIALIZE_AND_DESERIALIZE;
};
```

**关键实现要点**：

```cpp
int ObUpdateServer::apply_restore_mutator(
    const ObRestoreApplyParam& param,
    int64_t& assigned_log_seq)
{
  int ret = OB_SUCCESS;
  
  // 1. 验证恢复任务有效性
  if (!restore_task_manager_.is_valid_task(param.restore_task_id_)) {
    ret = OB_INVALID_ARGUMENT;
    YYSYS_LOG(ERROR, "invalid restore task id=%ld", param.restore_task_id_);
    return ret;
  }
  
  // 2. 设置租户上下文
  ObTenantContext tenant_ctx(param.tenant_id_);
  
  // 3. apply到MemTable（与普通写入相同）
  ObMemTableCtx mt_ctx;
  if (OB_SUCCESS != (ret = table_mgr_.apply(param.mutator_, mt_ctx))) {
    YYSYS_LOG(ERROR, "apply to memtable failed, tenant_id=%ld, ret=%d",
              param.tenant_id_, ret);
    return ret;
  }
  
  // 4. 分配新log_seq（重要：不使用备份中的旧log_seq）
  assigned_log_seq = log_mgr_.get_next_log_seq();
  
  // 5. 写入commit log
  ObLogEntry log_entry;
  log_entry.set_log_seq(assigned_log_seq);
  log_entry.set_log_command(OB_LOG_UPS_MUTATOR);
  // 序列化mutator到log
  if (OB_SUCCESS != (ret = log_mgr_.write_log(log_entry))) {
    YYSYS_LOG(ERROR, "write commit log failed, log_seq=%ld, ret=%d",
              assigned_log_seq, ret);
    return ret;
  }
  
  // 6. 通过Paxos/Raft同步给备机（如果需要）
  if (param.need_wait_sync_) {
    if (OB_SUCCESS != (ret = log_mgr_.wait_log_sync(assigned_log_seq, 5000))) {
      YYSYS_LOG(ERROR, "wait log sync failed, log_seq=%ld, ret=%d",
                assigned_log_seq, ret);
      return ret;
    }
  }
  
  // 7. 更新统计（恢复进度）
  restore_task_manager_.update_progress(param.restore_task_id_, 1);
  
  return ret;
}
```

**性能优化建议**：

```cpp
// 批量apply接口（可选）
int ObUpdateServer::batch_apply_restore_mutators(
    const ObArray<ObRestoreApplyParam>& params,
    ObArray<int64_t>& assigned_log_seqs)
{
  int ret = OB_SUCCESS;
  
  // 1. 批量apply到MemTable（减少锁开销）
  for (int64_t i = 0; i < params.count(); ++i) {
    // ... apply逻辑
  }
  
  // 2. 批量写入commit log（减少磁盘IO）
  if (OB_SUCCESS != (ret = log_mgr_.batch_write_log(log_entries))) {
    YYSYS_LOG(ERROR, "batch write log failed, ret=%d", ret);
    return ret;
  }
  
  // 3. 一次性同步（减少网络开销）
  if (OB_SUCCESS != (ret = log_mgr_.wait_batch_log_sync(assigned_log_seqs))) {
    YYSYS_LOG(ERROR, "wait batch log sync failed, ret=%d", ret);
    return ret;
  }
  
  return ret;
}
```

---

### 5.3.8 流水线性能预估

基于设计参数，预估增量恢复性能：

| 组件 | 处理能力 | 瓶颈分析 |
|------|---------|---------|
| **Reader** | ~200MB/s（磁盘顺序读） | 磁盘IO，可通过SSD/NVMe优化 |
| **Workers** | ~50K ops/s/线程（反序列化+重写） | CPU密集，可扩展线程数 |
| **Reordered Queue** | ~1M ops/s（内存操作） | 非瓶颈 |
| **Applier** | ~20K ops/s（单线程） | **主要瓶颈**（单线程顺序提交） |
| **TransServer** | ~30K ops/s（写入吞吐） | MemTable锁争用，日志io |

**整体吞吐量预估**：

```
端到端吞吐 = min(Reader, Workers, Applier, TransServer)
          ≈ 20K ops/s（受限于Applier单线程）

如果使用批量apply优化（batch_size=10）：
端到端吞吐 ≈ 50K-80K ops/s

恢复1TB数据（假设平均100字节/操作）：
数据量 = 1TB = 10^12字节
操作数 = 10^12 / 100 = 10^10 ops
耗时 = 10^10 / (20K ops/s) = 500K秒 ≈ 139小时 ≈ 6天

使用批量优化后：
耗时 = 10^10 / (50K ops/s) = 200K秒 ≈ 56小时 ≈ 2.3天
```

**优化方向**：

1. **Applier并行化（复杂）**：
   - 按table_id分片，多个applier并行
   - 需要解决跨表事务的顺序问题
   - 实现难度高，收益大

2. **批量apply（推荐）**：
   - 实现简单，收益明显（2-4倍）
   - 适合大部分场景

3. **异步apply**：
   - Applier不等待TransServer sync完成
   - 提高吞吐，但可能影响数据一致性保证
   - 需要额外的checkpoint机制

---

### 5.3.9 完整流程示例

```
时间线展示增量恢复流程：

T0: 启动流水线
    ├─ 初始化3个队列
    ├─ 启动1个Reader线程
    ├─ 启动8个Worker线程
    └─ 启动1个Applier线程

T0+1s: Reader开始读取归档日志
    log_file_1.arc (log_id: 1000-2000, 1000条日志, 10MB)
    ├─ 读取速度: ~200MB/s
    └─ 推入raw_log_queue: 平均100条/秒

T0+5s: Workers开始并发处理
    Worker-1: 处理log_1005 (反序列化 → 重写tenant_id/table_id → 序列化)
    Worker-2: 处理log_1003
    Worker-3: 处理log_1007
    ...
    Worker-8: 处理log_1009
    ├─ 处理速度: ~50K ops/s (8线程 * 6K ops/s/线程)
    └─ 推入reordered_queue (自动按log_id排序)

T0+6s: Applier开始顺序应用
    从reordered_queue取出: log_1000 (已重写)
    ├─ 提交到TransServer (走正常事务流程)
    ├─ TransServer分配新log_seq: 50001
    ├─ Apply到MemTable
    ├─ 写入commit log
    ├─ 同步给备机 (Paxos/Raft)
    └─ 等待commit完成
    
T0+6.05s: 继续应用log_1001
    (重复上述流程)
    
T0+10s: 完成第一批1000条日志
    ├─ 记录checkpoint: log_id=2000, timestamp=xxx
    ├─ Reader继续读取log_file_2.arc
    └─ 流水线持续运行...

T0+1小时: 恢复100万条日志
    ├─ 平均吞吐: ~278 ops/s (100万/3600秒)
    ├─ Reader瓶颈？否 (200MB/s >> 实际需求)
    ├─ Workers瓶颈？否 (50K ops/s >> 278 ops/s)
    └─ Applier瓶颈？否 (20K ops/s >> 278 ops/s, 还有优化空间)

性能瓶颈分析：
  当前配置下，Applier的单线程吞吐20K ops/s已足够
  如果日志规模更大，可考虑：
    - 启用批量apply (提升到50K ops/s)
    - 或按table_id分片并行apply (复杂，提升到100K+ ops/s)
```

**关键监控指标实时输出**：

```bash
# 恢复过程中的监控输出示例
[2026-02-05 12:00:00] Incremental Restore Pipeline Status:
  ├─ Reader:
  │    ├─ Files: 5/100 (5% completed)
  │    ├─ Read throughput: 185 MB/s
  │    └─ Raw queue: 234/1000 (23% full)
  │
  ├─ Workers (8 threads):
  │    ├─ Processed: 125,678 logs
  │    ├─ Throughput: 48,523 ops/s
  │    ├─ Avg latency: 165 us
  │    ├─ Error count: 3 (0.002%)
  │    └─ Reordered queue: 1,234/5,000 (25% full, 12 holes)
  │
  ├─ Applier:
  │    ├─ Applied: 124,000 logs (99% of processed)
  │    ├─ Throughput: 18,765 ops/s
  │    ├─ Avg apply latency: 53 us
  │    ├─ Last log_id: 124,000
  │    └─ Checkpoint: log_id=123,000, ts=2026-02-05 11:59:50
  │
  └─ Overall:
       ├─ E2E latency: 320 ms (from read to applied)
       ├─ Pipeline efficiency: 94.2%
       ├─ Estimated completion: 4.5 hours
       └─ ETA: 2026-02-05 16:30:00

[2026-02-05 12:00:10] ⚠️  Alert: Reordered queue has 25 holes (threshold: 20)
  ├─ Expected log_id: 124,001
  ├─ Next available: 124,026 (gap=25)
  └─ Action: Checking Worker-3 status (may be slow)

[2026-02-05 12:00:15] ✓ Hole resolved, continue...
```

---

### 5.4 数据一致性验证

恢复完成后，执行以下验证：

```cpp
class ObRestoreDataVerifier {
public:
  int verify_restored_tenant(int64_t standby_tenant_id,
                            int64_t src_tenant_id,
                            int64_t restore_timestamp);
  
private:
  // 1. 验证数据量
  int verify_row_count(int64_t standby_tenant_id, int64_t src_tenant_id);
  
  // 2. 抽样验证数据正确性
  int verify_data_sample(int64_t standby_tenant_id, 
                         int64_t src_tenant_id,
                         int sample_rate);
  
  // 3. 验证schema一致性
  int verify_schema(int64_t standby_tenant_id);
  
  // 4. 验证索引完整性
  int verify_index(int64_t standby_tenant_id);
  
  // 5. 生成验证报告
  int generate_verification_report(const ObVerifyResult& result);
};
```

---

## 6. DAG任务调度

### 6.1 DAG模型

使用有向无环图（DAG）建模复杂的备份恢复任务流。

```
备份任务DAG示例:

                    [StartBackupTask]
                           │
                           ▼
                  [PrepareBackupTask]
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
    [BackupTablet_1] [BackupTablet_2] ... [BackupTablet_N]
              │            │            │
              └────────────┼────────────┘
                           ▼
                [GenerateManifestTask]
                           │
                           ▼
                  [VerifyBackupTask]
                           │
                           ▼
                 [FinishBackupTask]


恢复任务DAG示例:

                    [StartRestoreTask]
                           │
                           ▼
               [PrepareStandbyTenantTask]
                           │
                           ▼
                [CreateSchemaMapping Task]
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
   [RestoreTablet_1] [RestoreTablet_2] ... [RestoreTablet_N]
              │            │            │
              └────────────┼────────────┘
                           ▼
                [WaitBaselineCompleteTask]
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
   [ReplayLogFile_1] [ReplayLogFile_2] ... [ReplayLogFile_M]
              │            │            │
              └────────────┼────────────┘
                           ▼
                [VerifyRestoredDataTask]
                           │
                           ▼
                 [FinishRestoreTask]
```

### 6.2 DAG调度器设计

```cpp
// src/backupserver/ob_backup_dag_scheduler.h

class ObBackupDagScheduler {
public:
  int init(int64_t thread_count);
  int start();
  void stop();
  void wait();
  
  // 提交DAG任务
  int add_dag(ObIDag* dag);
  
  // 取消DAG任务
  int cancel_dag(int64_t dag_id);
  
  // 查询DAG状态
  int get_dag_status(int64_t dag_id, ObDagStatus& status);
  
private:
  // 工作线程
  class ObDagWorker : public yysys::CDefaultRunnable {
  public:
    void run(yysys::CThread* thread, void* arg) override;
  };
  
  // DAG调度逻辑
  int schedule_next_task(ObDagTask*& task);
  
  // 执行任务
  int execute_task(ObDagTask* task);
  
  // 任务完成回调
  int on_task_finish(ObDagTask* task, int ret_code);
  
private:
  bool inited_;
  bool is_running_;
  int64_t thread_count_;
  ObDagWorker* workers_;
  
  // DAG存储
  common::ObList<ObIDag*> dag_list_;
  yysys::CThreadMutex dag_list_mutex_;
  
  // 就绪任务队列（无依赖的任务）
  common::ObQueue<ObDagTask*> ready_queue_;
  yysys::CThreadCond ready_cond_;
};
```

### 6.3 DAG任务基类

```cpp
// src/backupserver/ob_backup_dag.h

// DAG任务接口
class ObDagTask {
public:
  virtual ~ObDagTask() {}
  
  // 执行任务
  virtual int execute() = 0;
  
  // 任务描述
  virtual const char* get_task_type() const = 0;
  
  // 依赖的任务列表
  void add_dependency(ObDagTask* task);
  const ObArray<ObDagTask*>& get_dependencies() const;
  
  // 任务状态
  enum TaskStatus {
    TASK_WAITING,      // 等待依赖完成
    TASK_READY,        // 就绪，可执行
    TASK_RUNNING,      // 执行中
    TASK_FINISHED,     // 已完成
    TASK_FAILED        // 失败
  };
  
  TaskStatus get_status() const { return status_; }
  void set_status(TaskStatus status) { status_ = status; }
  
  int get_ret_code() const { return ret_code_; }
  void set_ret_code(int ret) { ret_code_ = ret; }
  
protected:
  int64_t task_id_;
  TaskStatus status_;
  int ret_code_;
  ObArray<ObDagTask*> dependencies_;
  yysys::CThreadMutex mutex_;
};

// DAG图接口
class ObIDag {
public:
  virtual ~ObIDag() {}
  
  // 获取DAG ID
  virtual int64_t get_dag_id() const = 0;
  
  // 获取DAG类型
  virtual const char* get_dag_type() const = 0;
  
  // 获取所有任务
  virtual int get_all_tasks(ObArray<ObDagTask*>& tasks) = 0;
  
  // DAG是否完成
  virtual bool is_finished() const = 0;
  
  // 取消DAG
  virtual int cancel() = 0;
};
```

### 6.4 具体任务实现

#### 6.4.1 备份Tablet任务

```cpp
class ObBackupTabletTask : public ObDagTask {
public:
  ObBackupTabletTask(int64_t tenant_id,
                     const ObTabletInfo& tablet_info,
                     const char* backup_path)
    : tenant_id_(tenant_id),
      tablet_info_(tablet_info),
      backup_path_(backup_path)
  {}
  
  int execute() override {
    int ret = OB_SUCCESS;
    
    // 1. 查找tablet所在的DataServer
    ObServer data_server;
    if (OB_SUCCESS != (ret = locate_tablet_server(tablet_info_, data_server))) {
      YYSYS_LOG(ERROR, "locate tablet server failed, ret=%d", ret);
      return ret;
    }
    
    // 2. 发送备份请求到DataServer
    ObDataBuffer result_buffer;
    if (OB_SUCCESS != (ret = request_backup_tablet(data_server, 
                                                   tablet_info_, 
                                                   result_buffer))) {
      YYSYS_LOG(ERROR, "request backup tablet failed, ret=%d", ret);
      return ret;
    }
    
    // 3. 拷贝SSTable文件
    if (OB_SUCCESS != (ret = copy_sstable_files(data_server, backup_path_))) {
      YYSYS_LOG(ERROR, "copy sstable files failed, ret=%d", ret);
      return ret;
    }
    
    // 4. 计算校验和
    uint64_t checksum = 0;
    if (OB_SUCCESS != (ret = calculate_checksum(backup_path_, checksum))) {
      YYSYS_LOG(ERROR, "calculate checksum failed, ret=%d", ret);
      return ret;
    }
    
    YYSYS_LOG(INFO, "backup tablet succeed, tablet_range=%s, checksum=%lu",
              to_cstring(tablet_info_.range_), checksum);
    
    return ret;
  }
  
  const char* get_task_type() const override {
    return "BackupTabletTask";
  }
  
private:
  int64_t tenant_id_;
  ObTabletInfo tablet_info_;
  const char* backup_path_;
};
```

#### 6.4.2 恢复Tablet任务

```cpp
class ObRestoreTabletTask : public ObDagTask {
public:
  ObRestoreTabletTask(int64_t standby_tenant_id,
                     const ObTabletBackupInfo& backup_info,
                     const ObSchemaMapping& schema_mapping)
    : standby_tenant_id_(standby_tenant_id),
      backup_info_(backup_info),
      schema_mapping_(schema_mapping)
  {}
  
  int execute() override {
    int ret = OB_SUCCESS;
    
    // 1. 下载SSTable文件到临时目录
    char temp_dir[OB_MAX_FILE_NAME_LENGTH];
    if (OB_SUCCESS != (ret = download_sstable_files(backup_info_, temp_dir))) {
      YYSYS_LOG(ERROR, "download sstable files failed, ret=%d", ret);
      return ret;
    }
    
    // 2. 重写SSTable的schema信息
    if (OB_SUCCESS != (ret = rewrite_sstable_schema(temp_dir, schema_mapping_))) {
      YYSYS_LOG(ERROR, "rewrite sstable schema failed, ret=%d", ret);
      return ret;
    }
    
    // 3. 使用bypass loader加载SSTable
    ObBypassSSTableLoader& loader = get_bypass_loader();
    if (OB_SUCCESS != (ret = loader.load_tablet_sstable(temp_dir, 
                                                         standby_tenant_id_))) {
      YYSYS_LOG(ERROR, "load tablet sstable failed, ret=%d", ret);
      return ret;
    }
    
    YYSYS_LOG(INFO, "restore tablet succeed, tenant_id=%ld, range=%s",
              standby_tenant_id_, to_cstring(backup_info_.range_));
    
    return ret;
  }
  
  const char* get_task_type() const override {
    return "RestoreTabletTask";
  }
  
private:
  int64_t standby_tenant_id_;
  ObTabletBackupInfo backup_info_;
  ObSchemaMapping schema_mapping_;
};
```

#### 6.4.3 Replay日志任务

```cpp
class ObReplayLogTask : public ObDagTask {
public:
  ObReplayLogTask(int64_t standby_tenant_id,
                 const char* log_file_path,
                 const ObSchemaMapping& schema_mapping)
    : standby_tenant_id_(standby_tenant_id),
      log_file_path_(log_file_path),
      schema_mapping_(schema_mapping)
  {}
  
  int execute() override {
    int ret = OB_SUCCESS;
    
    // 1. 下载归档日志文件
    char local_log_file[OB_MAX_FILE_NAME_LENGTH];
    if (OB_SUCCESS != (ret = download_log_file(log_file_path_, local_log_file))) {
      YYSYS_LOG(ERROR, "download log file failed, ret=%d", ret);
      return ret;
    }
    
    // 2. 解压日志文件
    char decompressed_file[OB_MAX_FILE_NAME_LENGTH];
    if (OB_SUCCESS != (ret = decompress_log_file(local_log_file, decompressed_file))) {
      YYSYS_LOG(ERROR, "decompress log file failed, ret=%d", ret);
      return ret;
    }
    
    // 3. 读取并重写日志
    ObFileReader reader;
    if (OB_SUCCESS != (ret = reader.open(decompressed_file))) {
      YYSYS_LOG(ERROR, "open log file failed, ret=%d", ret);
      return ret;
    }
    
    // 4. 逐条replay日志
    int64_t replayed_count = 0;
    while (OB_SUCCESS == ret) {
      char log_buf[OB_MAX_LOG_BUFFER_SIZE];
      int64_t log_len = 0;
      
      ret = reader.read_log(log_buf, sizeof(log_buf), log_len);
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else if (OB_SUCCESS != ret) {
        YYSYS_LOG(ERROR, "read log failed, ret=%d", ret);
        break;
      }
      
      // 重写log中的table_id
      char rewritten_log[OB_MAX_LOG_BUFFER_SIZE];
      int64_t rewritten_len = 0;
      if (OB_SUCCESS != (ret = schema_rewriter_.rewrite_commit_log(
                                log_buf, log_len, schema_mapping_,
                                rewritten_log, rewritten_len))) {
        YYSYS_LOG(ERROR, "rewrite commit log failed, ret=%d", ret);
        break;
      }
      
      // Replay到TransServer
      if (OB_SUCCESS != (ret = replay_log_to_trans_server(rewritten_log, 
                                                           rewritten_len))) {
        YYSYS_LOG(ERROR, "replay log to trans server failed, ret=%d", ret);
        break;
      }
      
      replayed_count++;
    }
    
    YYSYS_LOG(INFO, "replay log file succeed, file=%s, replayed_count=%ld",
              log_file_path_, replayed_count);
    
    return ret;
  }
  
  const char* get_task_type() const override {
    return "ReplayLogTask";
  }
  
private:
  int64_t standby_tenant_id_;
  const char* log_file_path_;
  ObSchemaMapping schema_mapping_;
  ObTenantSchemaRewriter schema_rewriter_;
};
```

### 6.5 DAG示例

#### 完整备份DAG

```cpp
class ObFullBackupDag : public ObIDag {
public:
  int init(int64_t tenant_id, const ObBackupParams& params) {
    int ret = OB_SUCCESS;
    
    // 1. 创建任务
    ObPrepareBackupTask* prepare_task = new ObPrepareBackupTask(tenant_id);
    ObGenerateManifestTask* manifest_task = new ObGenerateManifestTask(tenant_id);
    ObVerifyBackupTask* verify_task = new ObVerifyBackupTask(tenant_id);
    ObFinishBackupTask* finish_task = new ObFinishBackupTask(tenant_id);
    
    // 2. 获取租户的所有tablet
    ObArray<ObTabletInfo> tablets;
    if (OB_SUCCESS != (ret = get_tenant_tablets(tenant_id, tablets))) {
      return ret;
    }
    
    // 3. 为每个tablet创建备份任务
    ObArray<ObBackupTabletTask*> tablet_tasks;
    for (int64_t i = 0; i < tablets.count(); ++i) {
      ObBackupTabletTask* task = new ObBackupTabletTask(tenant_id, 
                                                         tablets.at(i),
                                                         backup_path_);
      // tablet任务依赖prepare任务
      task->add_dependency(prepare_task);
      tablet_tasks.push_back(task);
    }
    
    // 4. 建立依赖关系
    // manifest任务依赖所有tablet任务
    for (int64_t i = 0; i < tablet_tasks.count(); ++i) {
      manifest_task->add_dependency(tablet_tasks.at(i));
    }
    
    // verify依赖manifest
    verify_task->add_dependency(manifest_task);
    
    // finish依赖verify
    finish_task->add_dependency(verify_task);
    
    // 5. 加入DAG
    all_tasks_.push_back(prepare_task);
    for (int64_t i = 0; i < tablet_tasks.count(); ++i) {
      all_tasks_.push_back(tablet_tasks.at(i));
    }
    all_tasks_.push_back(manifest_task);
    all_tasks_.push_back(verify_task);
    all_tasks_.push_back(finish_task);
    
    return ret;
  }
  
  int64_t get_dag_id() const override { return dag_id_; }
  const char* get_dag_type() const override { return "FullBackupDag"; }
  
  int get_all_tasks(ObArray<ObDagTask*>& tasks) override {
    return tasks.assign(all_tasks_);
  }
  
  bool is_finished() const override {
    for (int64_t i = 0; i < all_tasks_.count(); ++i) {
      if (all_tasks_.at(i)->get_status() != ObDagTask::TASK_FINISHED &&
          all_tasks_.at(i)->get_status() != ObDagTask::TASK_FAILED) {
        return false;
      }
    }
    return true;
  }
  
  int cancel() override {
    // 取消所有未完成的任务
    for (int64_t i = 0; i < all_tasks_.count(); ++i) {
      if (all_tasks_.at(i)->get_status() == ObDagTask::TASK_WAITING ||
          all_tasks_.at(i)->get_status() == ObDagTask::TASK_READY) {
        all_tasks_.at(i)->set_status(ObDagTask::TASK_FAILED);
        all_tasks_.at(i)->set_ret_code(OB_CANCELED);
      }
    }
    return OB_SUCCESS;
  }
  
private:
  int64_t dag_id_;
  int64_t tenant_id_;
  ObArray<ObDagTask*> all_tasks_;
  char backup_path_[OB_MAX_URI_LENGTH];
};
```

---

## 7. Schema重写机制

### 7.1 Schema映射创建

```cpp
int ObTenantSchemaRewriter::create_schema_mapping(
    int64_t src_tenant_id,
    int64_t dest_tenant_id,
    const ObSchemaManagerV2& src_schema,
    ObSchemaManagerV2& dest_schema)
{
  int ret = OB_SUCCESS;
  ObSchemaMapping mapping;
  
  // 1. 在备租户中创建database
  const ObDatabaseSchema* src_db_schema = nullptr;
  for (int64_t i = 0; OB_SUCCESS == ret && i < src_schema.get_database_count(); ++i) {
    src_db_schema = src_schema.get_database_schema(i);
    if (nullptr == src_db_schema || src_db_schema->get_tenant_id() != src_tenant_id) {
      continue;
    }
    
    // 创建新database（不同名称避免冲突）
    char new_db_name[OB_MAX_DATBASE_NAME_LENGTH];
    snprintf(new_db_name, sizeof(new_db_name), "%s_restore_%ld",
             src_db_schema->get_database_name(), dest_tenant_id);
    
    ObDatabaseSchema new_db_schema;
    new_db_schema.set_tenant_id(dest_tenant_id);
    new_db_schema.set_database_name(new_db_name);
    
    if (OB_SUCCESS != (ret = dest_schema.add_database(new_db_schema))) {
      YYSYS_LOG(ERROR, "add database failed, ret=%d", ret);
      return ret;
    }
  }
  
  // 2. 在备租户中创建table，建立table_id映射
  const ObTableSchema* src_table_schema = nullptr;
  for (int64_t i = 0; OB_SUCCESS == ret && i < src_schema.get_table_count(); ++i) {
    src_table_schema = src_schema.get_table_schema(i);
    if (nullptr == src_table_schema || src_table_schema->get_tenant_id() != src_tenant_id) {
      continue;
    }
    
    // 创建新table schema（分配新table_id）
    ObTableSchema new_table_schema;
    if (OB_SUCCESS != (ret = copy_table_schema(*src_table_schema, new_table_schema))) {
      YYSYS_LOG(ERROR, "copy table schema failed, ret=%d", ret);
      return ret;
    }
    
    new_table_schema.set_tenant_id(dest_tenant_id);
    uint64_t new_table_id = generate_new_table_id(dest_tenant_id);
    new_table_schema.set_table_id(new_table_id);
    
    if (OB_SUCCESS != (ret = dest_schema.add_table(new_table_schema))) {
      YYSYS_LOG(ERROR, "add table failed, ret=%d", ret);
      return ret;
    }
    
    // 记录映射关系
    if (OB_SUCCESS != (ret = mapping.table_id_map_.set_refactored(
                              src_table_schema->get_table_id(), new_table_id))) {
      YYSYS_LOG(ERROR, "set table_id mapping failed, ret=%d", ret);
      return ret;
    }
    
    // 建立column_id映射（如果需要）
    if (OB_SUCCESS != (ret = create_column_mapping(*src_table_schema, 
                                                    new_table_schema, 
                                                    mapping))) {
      YYSYS_LOG(ERROR, "create column mapping failed, ret=%d", ret);
      return ret;
    }
  }
  
  // 3. 持久化映射关系
  if (OB_SUCCESS != (ret = persist_schema_mapping(dest_tenant_id, mapping))) {
    YYSYS_LOG(ERROR, "persist schema mapping failed, ret=%d", ret);
    return ret;
  }
  
  YYSYS_LOG(INFO, "create schema mapping succeed, src_tenant=%ld, dest_tenant=%ld, "
                  "table_count=%ld", src_tenant_id, dest_tenant_id, 
                  mapping.table_id_map_.size());
  
  return ret;
}
```

### 7.2 SSTable Schema重写

```cpp
int ObTenantSchemaRewriter::rewrite_sstable(
    const char* src_sstable_path,
    const char* dest_sstable_path,
    const ObSchemaMapping& mapping)
{
  int ret = OB_SUCCESS;
  
  // 1. 打开原SSTable
  sstable::ObSSTableReader src_reader;
  if (OB_SUCCESS != (ret = src_reader.open(src_sstable_path))) {
    YYSYS_LOG(ERROR, "open src sstable failed, path=%s, ret=%d", 
              src_sstable_path, ret);
    return ret;
  }
  
  // 2. 读取trailer（元数据在文件尾部）
  sstable::ObSSTableTrailer src_trailer;
  if (OB_SUCCESS != (ret = src_reader.get_trailer(src_trailer))) {
    YYSYS_LOG(ERROR, "get trailer failed, ret=%d", ret);
    return ret;
  }
  
  // 3. 重写trailer中的schema信息
  sstable::ObSSTableTrailer new_trailer = src_trailer;
  
  // 映射table_id
  uint64_t new_table_id = 0;
  if (OB_SUCCESS != (ret = mapping.table_id_map_.get_refactored(
                            src_trailer.get_table_id(), new_table_id))) {
    YYSYS_LOG(ERROR, "table_id not found in mapping, old_id=%lu, ret=%d",
              src_trailer.get_table_id(), ret);
    return ret;
  }
  new_trailer.set_table_id(new_table_id);
  
  // 映射schema_version（可选，取决于实现）
  int64_t new_schema_version = 0;
  if (OB_SUCCESS == mapping.schema_version_map_.get_refactored(
                      src_trailer.get_schema_version(), new_schema_version)) {
    new_trailer.set_schema_version(new_schema_version);
  }
  
  // 4. 创建新SSTable writer
  sstable::ObSSTableWriter writer;
  if (OB_SUCCESS != (ret = writer.open(dest_sstable_path, new_table_id))) {
    YYSYS_LOG(ERROR, "open dest sstable failed, path=%s, ret=%d",
              dest_sstable_path, ret);
    return ret;
  }
  
  // 5. 拷贝数据块（data blocks）
  // SSTable格式: [Data Blocks] [Bloom Filter] [Index] [Trailer]
  int64_t data_size = src_trailer.get_data_size();
  char* data_buf = static_cast<char*>(ob_malloc(data_size, ObModIds::OB_BACKUP_RESTORE));
  if (nullptr == data_buf) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    YYSYS_LOG(ERROR, "allocate memory failed, size=%ld", data_size);
    return ret;
  }
  
  if (OB_SUCCESS != (ret = src_reader.read_data_blocks(data_buf, data_size))) {
    YYSYS_LOG(ERROR, "read data blocks failed, ret=%d", ret);
    ob_free(data_buf);
    return ret;
  }
  
  if (OB_SUCCESS != (ret = writer.write_data_blocks(data_buf, data_size))) {
    YYSYS_LOG(ERROR, "write data blocks failed, ret=%d", ret);
    ob_free(data_buf);
    return ret;
  }
  ob_free(data_buf);
  
  // 6. 重建bloom filter（如果table_id影响hash，需要重建）
  if (is_bloom_filter_affected_by_table_id()) {
    if (OB_SUCCESS != (ret = rebuild_bloom_filter(src_reader, writer, mapping))) {
      YYSYS_LOG(ERROR, "rebuild bloom filter failed, ret=%d", ret);
      return ret;
    }
  } else {
    // 直接拷贝bloom filter
    if (OB_SUCCESS != (ret = copy_bloom_filter(src_reader, writer))) {
      YYSYS_LOG(ERROR, "copy bloom filter failed, ret=%d", ret);
      return ret;
    }
  }
  
  // 7. 写入新trailer
  if (OB_SUCCESS != (ret = writer.write_trailer(new_trailer))) {
    YYSYS_LOG(ERROR, "write trailer failed, ret=%d", ret);
    return ret;
  }
  
  // 8. 关闭文件
  writer.close();
  src_reader.close();
  
  YYSYS_LOG(INFO, "rewrite sstable succeed, src=%s, dest=%s, "
                  "old_table_id=%lu, new_table_id=%lu",
            src_sstable_path, dest_sstable_path,
            src_trailer.get_table_id(), new_table_id);
  
  return ret;
}
```

**注意**：实际实现中，如果SSTable格式允许，可以考虑只重写trailer部分，通过hardlink共享数据块，避免大量数据拷贝。

---

## 8. 数据一致性保证

### 8.1 一致性挑战

| 场景 | 一致性问题 | 解决方案 |
|------|-----------|---------|
| **基线+增量一致** | 基线备份时刻T0，增量日志从T1开始，T0和T1之间有gap | 基线备份前先记录log_id，增量从该log_id开始 |
| **TransServer共享** | 多租户共享TS，日志混在一起 | 通过tenant_id过滤日志 |
| **Schema变更** | 恢复期间源租户Schema可能变更 | 备份时锁定Schema版本；记录Schema变更历史 |
| **事务一致性** | 分布式事务跨tablet | 按事务完整性replay日志（所有参与者的log都齐全才commit） |

### 8.2 一致性保证机制

#### 8.2.1 基线-增量一致性

```cpp
// 备份协调器保证一致性
class ObBackupConsistencyCoordinator {
public:
  // 启动全量备份
  int start_full_backup_with_consistency(int64_t tenant_id) {
    int ret = OB_SUCCESS;
    
    // 1. 先启动增量备份（记录起始log_id）
    int64_t start_log_id = 0;
    if (OB_SUCCESS != (ret = start_incremental_backup(tenant_id, start_log_id))) {
      YYSYS_LOG(ERROR, "start incremental backup failed, ret=%d", ret);
      return ret;
    }
    
    // 2. 等待一个checkpoint（确保增量备份已稳定）
    sleep(checkpoint_interval_sec_);
    
    // 3. 触发TransServer freeze（生成一个一致性快照版本）
    int64_t frozen_version = 0;
    if (OB_SUCCESS != (ret = trigger_freeze(&frozen_version))) {
      YYSYS_LOG(ERROR, "trigger freeze failed, ret=%d", ret);
      return ret;
    }
    
    // 4. 等待freeze完成（active MemTable冻结为SSTable）
    if (OB_SUCCESS != (ret = wait_freeze_complete(frozen_version))) {
      YYSYS_LOG(ERROR, "wait freeze complete failed, ret=%d", ret);
      return ret;
    }
    
    // 5. 启动基线备份（此时L0和L1数据是frozen_version时刻的一致快照）
    ObBaselineBackupTask baseline_task;
    baseline_task.tenant_id_ = tenant_id;
    baseline_task.frozen_version_ = frozen_version;
    baseline_task.consistency_log_id_ = start_log_id;  // 记录对应的log_id
    
    if (OB_SUCCESS != (ret = baseline_backuper_.backup_tenant_baseline(baseline_task))) {
      YYSYS_LOG(ERROR, "backup tenant baseline failed, ret=%d", ret);
      return ret;
    }
    
    YYSYS_LOG(INFO, "start full backup with consistency succeed, "
                    "tenant_id=%ld, frozen_version=%ld, start_log_id=%ld",
              tenant_id, frozen_version, start_log_id);
    
    return ret;
  }
  
private:
  ObTenantIncrementalBackuper incremental_backuper_;
  ObTenantBaselineBackuper baseline_backuper_;
  int64_t checkpoint_interval_sec_;
};
```

#### 8.2.2 事务一致性保证

```cpp
// 增量恢复时保证事务完整性
class ObTransactionConsistencyChecker {
public:
  // Replay日志前检查事务完整性
  int check_transaction_consistency(const ObLogEntry& log_entry) {
    int ret = OB_SUCCESS;
    
    // 1. 解析日志类型
    if (log_entry.cmd_ == OB_TRANS_PREPARE) {
      // PREPARE阶段：记录事务ID
      ObTransID trans_id;
      if (OB_SUCCESS != (ret = extract_trans_id(log_entry, trans_id))) {
        return ret;
      }
      
      // 加入pending事务集合
      if (OB_SUCCESS != (ret = pending_trans_set_.set_refactored(trans_id, log_entry))) {
        YYSYS_LOG(ERROR, "add pending trans failed, trans_id=%s", to_cstring(trans_id));
        return ret;
      }
      
    } else if (log_entry.cmd_ == OB_TRANS_COMMIT) {
      // COMMIT阶段：检查是否有对应的PREPARE
      ObTransID trans_id;
      if (OB_SUCCESS != (ret = extract_trans_id(log_entry, trans_id))) {
        return ret;
      }
      
      ObLogEntry prepare_log;
      ret = pending_trans_set_.get_refactored(trans_id, prepare_log);
      if (OB_HASH_NOT_EXIST == ret) {
        YYSYS_LOG(WARN, "commit log without prepare, trans_id=%s, may need wait",
                  to_cstring(trans_id));
        // 等待PREPARE日志到达
        return OB_EAGAIN;
      } else if (OB_SUCCESS != ret) {
        YYSYS_LOG(ERROR, "get pending trans failed, ret=%d", ret);
        return ret;
      }
      
      // PREPARE和COMMIT都齐全，可以应用
      pending_trans_set_.erase_refactored(trans_id);
      
    } else if (log_entry.cmd_ == OB_TRANS_ABORT) {
      // ABORT: 清理pending事务
      ObTransID trans_id;
      extract_trans_id(log_entry, trans_id);
      pending_trans_set_.erase_refactored(trans_id);
    }
    
    return OB_SUCCESS;
  }
  
  // 检查是否还有未完成的事务
  bool has_pending_transactions() const {
    return !pending_trans_set_.empty();
  }
  
private:
  // 未提交的事务集合
  hash::ObHashMap<ObTransID, ObLogEntry> pending_trans_set_;
};
```

#### 8.2.3 恢复验证

```cpp
class ObRestoreDataVerifier {
public:
  int verify_restored_tenant(int64_t standby_tenant_id,
                            const ObRestoreTask& restore_task) {
    int ret = OB_SUCCESS;
    ObVerifyResult result;
    
    // 1. 验证表数量
    if (OB_SUCCESS != (ret = verify_table_count(standby_tenant_id, 
                                                restore_task.src_tenant_id_,
                                                result))) {
      YYSYS_LOG(ERROR, "verify table count failed, ret=%d", ret);
      return ret;
    }
    
    // 2. 验证数据行数（每个表）
    if (OB_SUCCESS != (ret = verify_row_counts(standby_tenant_id, 
                                               restore_task.src_tenant_id_,
                                               result))) {
      YYSYS_LOG(ERROR, "verify row counts failed, ret=%d", ret);
      return ret;
    }
    
    // 3. 抽样验证数据正确性（随机抽取1%的行）
    if (OB_SUCCESS != (ret = verify_data_sample(standby_tenant_id,
                                                restore_task.src_tenant_id_,
                                                0.01,  // 1%抽样
                                                result))) {
      YYSYS_LOG(ERROR, "verify data sample failed, ret=%d", ret);
      return ret;
    }
    
    // 4. 验证索引完整性
    if (OB_SUCCESS != (ret = verify_indexes(standby_tenant_id, result))) {
      YYSYS_LOG(ERROR, "verify indexes failed, ret=%d", ret);
      return ret;
    }
    
    // 5. 生成验证报告
    if (OB_SUCCESS != (ret = generate_verification_report(standby_tenant_id, result))) {
      YYSYS_LOG(ERROR, "generate verification report failed, ret=%d", ret);
      return ret;
    }
    
    if (result.is_all_passed()) {
      YYSYS_LOG(INFO, "restore data verification PASSED, tenant_id=%ld", 
                standby_tenant_id);
    } else {
      YYSYS_LOG(ERROR, "restore data verification FAILED, tenant_id=%ld, details=%s",
                standby_tenant_id, result.to_cstring());
      ret = OB_RESTORE_VERIFY_FAILED;
    }
    
    return ret;
  }
  
private:
  struct ObVerifyResult {
    bool table_count_matched_;
    ObArray<TableVerifyInfo> table_results_;
    
    struct TableVerifyInfo {
      char table_name_[OB_MAX_TABLE_NAME_LENGTH];
      bool row_count_matched_;
      int64_t expected_row_count_;
      int64_t actual_row_count_;
      bool sample_data_matched_;
      int64_t sample_size_;
      int64_t sample_mismatch_count_;
      bool index_valid_;
    };
    
    bool is_all_passed() const {
      if (!table_count_matched_) return false;
      for (int64_t i = 0; i < table_results_.count(); ++i) {
        const TableVerifyInfo& info = table_results_.at(i);
        if (!info.row_count_matched_ || !info.sample_data_matched_ || !info.index_valid_) {
          return false;
        }
      }
      return true;
    }
  };
};
```

---

## 9. 详细设计

### 9.1 核心接口定义

#### 9.1.1 Backup接口

```cpp
// src/backupserver/ob_tenant_backup_manager.h

class ObTenantBackupManager {
public:
  // 初始化
  int init(ObBackupServer* backup_server);
  
  // 全量备份
  int start_full_backup(const ObFullBackupParams& params,
                       int64_t& backup_task_id);
  
  // 增量备份
  int start_incremental_backup(const ObIncrBackupParams& params,
                              int64_t& backup_task_id);
  
  // 停止备份
  int stop_backup(int64_t backup_task_id);
  
  // 查询备份状态
  int get_backup_status(int64_t backup_task_id,
                       ObBackupStatus& status);
  
  // 列出备份集
  int list_backup_sets(int64_t tenant_id,
                      ObArray<ObBackupSetInfo>& backup_sets);
  
private:
  ObBackupDagScheduler dag_scheduler_;
  ObTenantBaselineBackuper baseline_backuper_;
  ObTenantIncrementalBackuper incremental_backuper_;
  ObBackupMetaManager meta_manager_;
};

// 备份参数
struct ObFullBackupParams {
  int64_t tenant_id_;
  char backup_dest_[OB_MAX_URI_LENGTH];  // 备份目标（OSS/NFS路径）
  int64_t parallel_degree_;              // 并发度（默认10）
  bool enable_compression_;              // 是否压缩
  int64_t bandwidth_limit_mb_;           // 带宽限制
};

struct ObIncrBackupParams {
  int64_t tenant_id_;
  int64_t backup_set_id_;               // 关联的基线备份集ID
  char backup_dest_[OB_MAX_URI_LENGTH];
  int64_t checkpoint_interval_sec_;     // 检查点间隔（默认60秒）
  int64_t log_archive_interval_sec_;    // 日志归档间隔（默认3600秒）
};
```

#### 9.1.2 Restore接口

```cpp
// src/backupserver/ob_tenant_restore_manager.h

class ObTenantRestoreManager {
public:
  // 初始化
  int init(ObBackupServer* backup_server);
  
  // 恢复到备租户
  int restore_to_standby(const ObRestoreParams& params,
                        int64_t& restore_task_id);
  
  // 提升备租户为主租户
  int promote_to_primary(int64_t standby_tenant_id,
                        int64_t primary_tenant_id);
  
  // 查询恢复状态
  int get_restore_status(int64_t restore_task_id,
                        ObRestoreStatus& status);
  
  // 取消恢复
  int cancel_restore(int64_t restore_task_id);
  
private:
  ObBackupDagScheduler dag_scheduler_;
  ObTenantRestoreCoordinator restore_coordinator_;
  ObTenantSchemaRewriter schema_rewriter_;
  ObRestoreDataVerifier data_verifier_;
};

// 恢复参数
struct ObRestoreParams {
  int64_t src_tenant_id_;               // 源租户ID
  int64_t dest_tenant_id_;              // 目标租户ID（备租户）
  int64_t backup_set_id_;               // 基线备份集ID
  int64_t restore_timestamp_;           // 恢复目标时间戳（PITR）
  char backup_src_[OB_MAX_URI_LENGTH];  // 备份源路径
  int64_t parallel_degree_;             // 并发度
  bool verify_data_;                    // 是否验证数据
};
```

### 9.2 RPC消息定义

```cpp
// src/common/ob_backup_restore_packet.h

// Packet codes (添加到 ob_packet.h)
enum ObBackupRestorePacket {
  OB_BACKUP_TENANT_BASELINE = 2000,
  OB_BACKUP_TENANT_INCREMENTAL,
  OB_RESTORE_TENANT_BASELINE,
  OB_RESTORE_TENANT_INCREMENTAL,
  OB_QUERY_BACKUP_STATUS,
  OB_CANCEL_BACKUP_TASK,
};

// 基线备份请求
struct ObBackupBaselineRequest {
  int64_t tenant_id_;
  int64_t frozen_version_;
  char backup_dest_[OB_MAX_URI_LENGTH];
  int64_t parallel_degree_;
  
  NEED_SERIALIZE_AND_DESERIALIZE;
};

// 基线备份响应
struct ObBackupBaselineResponse {
  int64_t backup_set_id_;
  int64_t data_size_;
  int64_t tablet_count_;
  int ret_code_;
  
  NEED_SERIALIZE_AND_DESERIALIZE;
};

// 恢复请求
struct ObRestoreRequest {
  int64_t src_tenant_id_;
  int64_t dest_tenant_id_;
  int64_t backup_set_id_;
  int64_t restore_timestamp_;
  char backup_src_[OB_MAX_URI_LENGTH];
  
  NEED_SERIALIZE_AND_DESERIALIZE;
};

// 恢复响应
struct ObRestoreResponse {
  int64_t restore_task_id_;
  int ret_code_;
  
  NEED_SERIALIZE_AND_DESERIALIZE;
};
```

### 9.3 内部表设计

用于持久化备份恢复元数据。

```sql
-- 备份集信息表
CREATE TABLE __all_backup_set (
  tenant_id BIGINT NOT NULL,
  backup_set_id BIGINT NOT NULL,
  backup_type INT NOT NULL,             -- 0: full, 1: incremental
  backup_status INT NOT NULL,           -- 0: running, 1: completed, 2: failed
  frozen_version BIGINT,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  data_size BIGINT,
  tablet_count BIGINT,
  backup_dest VARCHAR(4096),
  consistency_log_id BIGINT,            -- 对应的增量日志起始ID
  PRIMARY KEY (tenant_id, backup_set_id)
);

-- 备份任务表
CREATE TABLE __all_backup_task (
  task_id BIGINT NOT NULL PRIMARY KEY,
  tenant_id BIGINT NOT NULL,
  backup_set_id BIGINT,
  task_type INT NOT NULL,               -- 0: baseline, 1: incremental
  task_status INT NOT NULL,             -- 0: init, 1: running, 2: completed, 3: failed
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  progress_percent INT,
  error_msg VARCHAR(4096)
);

-- 恢复任务表
CREATE TABLE __all_restore_task (
  task_id BIGINT NOT NULL PRIMARY KEY,
  src_tenant_id BIGINT NOT NULL,
  dest_tenant_id BIGINT NOT NULL,
  backup_set_id BIGINT NOT NULL,
  restore_timestamp BIGINT,
  task_status INT NOT NULL,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  progress_percent INT,
  is_verified BOOLEAN,
  error_msg VARCHAR(4096)
);

-- Schema映射表
CREATE TABLE __all_tenant_schema_mapping (
  dest_tenant_id BIGINT NOT NULL,
  src_table_id BIGINT NOT NULL,
  dest_table_id BIGINT NOT NULL,
  src_table_name VARCHAR(256),
  dest_table_name VARCHAR(256),
  create_time TIMESTAMP,
  PRIMARY KEY (dest_tenant_id, src_table_id)
);

-- 增量备份检查点表
CREATE TABLE __all_incremental_backup_checkpoint (
  tenant_id BIGINT NOT NULL,
  backup_set_id BIGINT NOT NULL,
  last_archived_log_id BIGINT,
  last_archived_timestamp BIGINT,
  update_time TIMESTAMP,
  PRIMARY KEY (tenant_id, backup_set_id)
);
```

### 9.4 配置参数

```ini
# script/deploy/oceanbase.conf.template (新增备份恢复相关配置)

[backupserver]
# 备份目标路径（支持NFS/OSS）
backup_dest = file:///backup/oceanbase

# 备份并发度
backup_parallel_degree = 10

# 备份带宽限制（MB/s）
backup_bandwidth_limit = 100

# 增量备份检查点间隔（秒）
incremental_backup_checkpoint_interval = 60

# 增量日志归档间隔（秒，每小时归档一次）
incremental_log_archive_interval = 3600

# 恢复并发度
restore_parallel_degree = 10

# 是否启用备份压缩
enable_backup_compression = true

# 备份压缩算法（lz4/zstd/snappy）
backup_compression_algorithm = lz4

# DAG调度器线程数
dag_scheduler_thread_count = 20

# 恢复时是否自动验证数据
enable_restore_verification = true

# 恢复资源限制（CPU百分比）
restore_cpu_quota_percent = 10

# 恢复IO带宽限制（MB/s）
restore_io_bandwidth_limit = 80
```

---

## 10. 实现计划

### 10.1 阶段划分

#### 阶段1: 基础设施（4周）

| 任务 | 目标 | 工作量 |
|------|------|--------|
| DAG调度器 | 实现通用DAG任务调度框架 | 2周 |
| Schema重写器 | 实现SSTable和日志的schema重写 | 2周 |
| 内部表设计 | 设计并创建备份恢复元数据表 | 1周 |
| 基础工具类 | 文件操作、网络传输、压缩等工具 | 1周 |

**里程碑**：DAG调度器可用，Schema重写器通过单元测试。

#### 阶段2: 基线备份恢复（4周）

| 任务 | 目标 | 工作量 |
|------|------|--------|
| 基线备份器 | 实现租户级基线备份 | 2周 |
| DataServer备份接口 | ChunkServer支持租户tablet备份 | 1周 |
| 基线恢复器 | 实现基线数据恢复 | 2周 |
| Bypass Loader适配 | 适配多租户的bypass loader | 1周 |

**里程碑**：完成租户级基线备份和恢复功能，通过基础测试。

#### 阶段3: 增量备份恢复（4周）

| 任务 | 目标 | 工作量 |
|------|------|--------|
| 日志订阅器 | 订阅TransServer commit log | 1周 |
| 日志过滤器 | 按租户过滤日志 | 1周 |
| 日志归档器 | 归档增量日志到备份存储 | 1周 |
| 日志Replay | 实现增量日志replay | 2周 |

**里程碑**：完成增量备份和PITR恢复功能。

#### 阶段4: 一致性验证与测试（3周）

| 任务 | 目标 | 工作量 |
|------|------|--------|
| 一致性机制 | 实现基线-增量一致性保证 | 1周 |
| 数据验证器 | 实现恢复数据验证 | 1周 |
| 集成测试 | 完整的备份恢复流程测试 | 2周 |

**里程碑**：通过完整的备份恢复集成测试，数据一致性验证通过。

#### 阶段5: 备租户提升与监控（2周）

| 任务 | 目标 | 工作量 |
|------|------|--------|
| 备租户提升 | 实现备租户到主租户的切换 | 1周 |
| 监控告警 | 备份恢复状态监控和告警 | 1周 |
| 文档完善 | 用户手册和运维文档 | 1周 |

**里程碑**：完整功能交付，文档齐全。

### 10.2 关键风险与缓解

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| **Schema重写复杂度** | SSTable格式复杂，重写困难 | 先支持简单schema，分阶段实现；考虑只重写trailer |
| **TransServer共享** | 难以精确过滤租户日志 | 在commit log中明确标记tenant_id；前期可能有误判，需充分测试 |
| **性能影响** | 备份恢复影响主租户性能 | 严格资源限制；低优先级调度；分批处理 |
| **数据一致性** | 跨tablet事务一致性保证难 | 基于MVCC和事务日志保证；充分测试边界case |
| **大规模测试** | 缺乏大规模测试环境 | 构造压测工具；云上测试；灰度发布 |

### 10.3 交付物

1. **代码**：
   - `src/backupserver/ob_tenant_backup_manager.*`
   - `src/backupserver/ob_tenant_restore_manager.*`
   - `src/backupserver/ob_backup_dag_scheduler.*`
   - `src/backupserver/ob_tenant_schema_rewriter.*`
   - 其他相关组件

2. **文档**：
   - 《YaoBase多租户备份恢复用户手册》
   - 《备份恢复运维指南》
   - 《API参考文档》
   - 《性能调优指南》

3. **工具**：
   - 备份恢复命令行工具
   - 备份验证工具
   - 数据一致性检查工具

4. **测试**：
   - 单元测试（覆盖率≥80%）
   - 集成测试
   - 性能测试
   - 可靠性测试

---

## 11. 使用示例

### 11.1 全量备份

```bash
# 创建租户级全量备份
obadmin> CREATE BACKUP TENANT tenant_1 
         TO 'file:///backup/tenant_1'
         WITH PARALLEL = 10, COMPRESSION = 'lz4';

-- 查询备份状态
obadmin> SELECT * FROM __all_backup_task WHERE tenant_id = 1;

-- 列出备份集
obadmin> SHOW BACKUP SETS FOR TENANT tenant_1;
```

### 11.2 增量备份

```bash
# 启动增量备份（基于备份集ID）
obadmin> START INCREMENTAL BACKUP TENANT tenant_1
         BASED ON BACKUP_SET 10001
         TO 'file:///backup/tenant_1/incremental'
         WITH CHECKPOINT_INTERVAL = 60;

-- 查询增量备份进度
obadmin> SELECT * FROM __all_incremental_backup_checkpoint WHERE tenant_id = 1;
```

### 11.3 恢复到备租户

```bash
# 恢复到备租户
obadmin> RESTORE TENANT tenant_1_standby 
         FROM TENANT tenant_1 
         BACKUP_SET 10001
         RESTORE_TO_TIME '2026-02-05 12:00:00'
         SOURCE 'file:///backup/tenant_1'
         WITH PARALLEL = 10, VERIFY = true;

-- 查询恢复进度
obadmin> SELECT * FROM __all_restore_task WHERE dest_tenant_id = 2;
```

### 11.4 验证并提升备租户

```bash
# 验证恢复数据
obadmin> VERIFY RESTORED TENANT tenant_1_standby;

-- 如果验证通过，提升为主租户
obadmin> PROMOTE STANDBY TENANT tenant_1_standby TO PRIMARY AS tenant_1_new;
```

---

## 12. 总结

本设计方案为YaoBase提供了完整的多租户不停服备份恢复能力：

### 12.1 核心能力

1. **租户级备份**: 支持单个租户的独立备份，不影响其他租户
2. **PITR恢复**: 基于增量日志实现任意时间点恢复
3. **备租户验证**: 先恢复到备租户，验证数据正确性后再切换
4. **Schema隔离**: 备租户使用全新的table_id，避免与主租户冲突
5. **不停服**: 备份恢复过程中主租户继续提供服务
6. **DAG调度**: 复杂任务流通过DAG图管理，支持并发和依赖

### 12.2 技术亮点

1. **两层LSM-tree协同**: 基线（DataServer）+ 增量（TransServer）分离备份
2. **Schema动态重写**: 恢复时重写SSTable和日志的schema信息
3. **一致性保证**: 基于frozen_version和log_id的一致性机制
4. **资源隔离**: 低优先级调度，限制CPU/IO占用，不影响主租户
5. **可扩展性**: DAG框架支持未来更复杂的任务编排

### 12.3 下一步工作

1. 完成详细设计评审
2. 按阶段实施开发计划
3. 充分的单元测试和集成测试
4. 性能测试和调优
5. 文档完善和用户培训

---

**文档维护者**: YaoBase开发团队  
**最后更新**: 2026-02-05  
**版本**: 1.0
