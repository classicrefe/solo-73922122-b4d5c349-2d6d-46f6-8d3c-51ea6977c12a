# 测序中心归档回收 API（Archive Reclamation Service）

按批次号管理测序 ZIP 归档的 FastAPI + SQLite 服务。两个实例共享同一数据卷，
内容按 SHA-256 去重，批次撤下后只回收**已无任何有效批次引用**的原件，并支持
进程崩溃后重启续清。归档员发现磁盘坏件后，可**按批巡检**核对共享原件并停供
受影响批次，再用留存 ZIP **修复**原件、恢复供数。

## 功能与接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/batches/{batch_id}/archive` | 上传 ZIP（≤ 2 MiB），multipart 字段 `file`；同号同内容重传兼具修复语义 |
| GET  | `/batches/{batch_id}/archive` | 下载该批次的原始字节（停供批次返回 409 及原因） |
| POST | `/batches/{batch_id}/inspect` | **巡检**：按登记长度和 SHA-256 核对共享原件，缺失或不符即停供所有有效引用批次 |
| POST | `/batches/{batch_id}/repair` | **修复**：重传与原登记完全一致的 ZIP 恢复共享原件，multipart 字段 `file` |
| POST | `/batches/{batch_id}/retire` | 撤下批次（幂等），随后内联执行回收 |
| POST | `/gc/run` | 手动触发标记-清扫式垃圾回收 |
| GET  | `/batches?status=active\|suspended\|retired` | 批次摘要列表（含停供原因） |
| GET  | `/batches/{batch_id}` | 单个批次摘要（含 `serving`、`suspend_reason`） |
| GET  | `/storage/summary` | 台账核对：批次数、停供数、blob 行数、磁盘原件数等 |
| GET  | `/healthz` | 健康检查 |
| GET  | `/docs` | Swagger UI（OpenAPI 文档，可交互演示巡检/修复/状态/下载） |

### 业务规则

- 上传仅接受合法 ZIP，单批次上限 **2 MiB**（超限返回 413，非 ZIP 返回 400）。
- 相同 SHA-256 内容在共享卷上**只存一份**（`blobs/<sha256>`），多批次共用。
- 同号同内容重传：返回 200 且 `reused_existing_content=true`。
- 同号异内容：拒绝并返回 **409**，原内容不受影响。
- 撤下的批次号**永久不可复用**：再传返回 409，新下载返回 **404**。
- 回收只删除无 active 批次引用的原件；仍被其他批次共用的原件保留。
- 下载一旦开始即持有独立文件描述符流式传输，**不持有数据库事务/锁**；
  即使期间原件被移入隔离区或删除（POSIX 语义），慢速下载仍可完整结束，
  且不会阻塞其他批次的撤下与回收。

### 巡检与修复（坏件处置流程）

1. **巡检** `POST /batches/{id}/inspect`：流式核对共享原件的实际长度与
   SHA-256 是否等于登记值。
   - 原件**缺失**（`missing`）或**不符**（`mismatch`）：所有引用该原件的
     **有效批次立即停供**——批次摘要显示 `serving=false` 与
     `suspend_reason`，新下载返回 **409** 及原因；批号与引用关系保留，
     blob 行与文件保留（停供批次仍是有效引用，GC 不会回收该原件）。
   - 核对一致（`ok`）：不改变任何状态。
   - 若核对期间原件已被修复/替换（`superseded`）：旧结论作废，不产生停供。
2. **修复** `POST /batches/{id}/repair`：向**有效批次**重传与原登记完全一致
   （SHA-256 相同）的留存 ZIP。
   - 字节先经原子 `rename` 落盘（读者永远看不到写了一半的文件），再于单个
     事务内刷新 blob 代际（generation）并**恢复全部有效引用批次的供数**。
   - **异文拒绝**：内容与登记不符返回 409，状态不变。
   - **撤下批次不得复活**：对已撤下批次修复返回 409；修复只恢复 active
     批次的供数，retired 批次保持 404。
   - 普通上传口对"同号同内容重传到有效批次"同样执行修复（重传即修复）。
3. 崩溃安全：修复在"换文件"与"切换供数状态"之间崩溃或被写盘失败中断时，
   批次保持停供（**不开放半件**），重启后亦然；修复可原样重试直至完成。

## 并发与崩溃安全设计

- SQLite 启用 **WAL** 与 `busy_timeout`；所有写操作在单个
  `BEGIN IMMEDIATE` 事务内完成判定与写入，两个实例靠写锁串行化，
  不会互相读到半成品状态。
- 上传顺序：先写 `tmp/` 临时分片 → 事务内插入/认领 blob 行与批次行并提交
  → 再原子 `rename` 到 `blobs/`。行先于文件存在，回收扫描不会误判新上传。
- **巡检全程只在落结论时持短写锁**：读登记与核对文件（可能很慢）均在锁外，
  因此巡检停顿时修复及无关批次照常完成。提交结论前复核 blob 的
  **generation 令牌**——原件每次落盘/修复都会刷新令牌；令牌已变说明结论
  基于旧字节，巡检结果按 `superseded` 作废，**迟到的旧巡检不会把已修复的
  原件再次停供**。
- **修复顺序**：先原子 `rename` 替换文件，再在单事务内刷新 generation 并
  清除停供。崩溃于两步之间时批次保持停供（安全），重试即可完成；
  `blobs/` 中只存在完整 fsync 后经原子 rename 落盘的文件。
- 回收分两阶段：
  1. **标记**：把无 active 引用的 ready blob 写入 `gc_queue`；
  2. **清扫**：事务内复查引用后置 `deleting`，文件移入 `quarantine/`，
     再在第二个事务内删行并删除隔离文件。每一步都幂等，两个实例可并发回收。
- 若清扫中内容被新批次重新引用（同哈希上传），事务内复查会把行恢复为
  `ready` 并把隔离文件移回，内容得以保留。
- 进程在任何阶段退出后，重启时 lifespan 调用 `reconcile()`：
  清理临时分片、续清 `deleting` 残留、重新被引用的内容恢复、
  清理无主隔离文件，然后继续常规标记-清扫。
- `gc_queue` 与 `deleting` 状态持久化在 SQLite 中，回收进度跨重启可续。

## 数据卷布局

```
/data
├── archives.db          # SQLite（WAL 模式，含 -wal/-shm 旁车文件）
├── blobs/<sha256>       # 内容寻址的原件，每个哈希一份
├── quarantine/<sha256>  # 回收中的原件（崩溃可恢复）
└── tmp/part-<uuid>      # 上传/修复临时分片
```

`/storage/summary` 同时返回 `blob_rows`（台账行数）与
`blob_files_on_disk`（实际原件数），两者一致即说明回收已收敛；
`suspended_batches` 给出当前停供批次数。

## 启动

### Docker Compose（推荐，两个实例 + 共享命名卷）

```bash
docker compose up --build
# 实例 A: http://localhost:8001/docs
# 实例 B: http://localhost:8002/docs
# 共享卷: archive-data -> /data
```

### 本地直接运行

```bash
pip install -r requirements.txt
ARCHIVE_DATA_DIR=./data uvicorn app.api:app --port 8000
# 第二个实例换端口、同一 ARCHIVE_DATA_DIR 即可
```

### 快速冒烟（巡检-停供-修复全流程）

```bash
curl -F "file=@batch.zip" http://localhost:8001/batches/B2026-001/archive
curl -F "file=@batch.zip" http://localhost:8002/batches/B2026-002/archive
# 模拟坏件后巡检：两个引用批次一起停供
curl -X POST http://localhost:8001/batches/B2026-001/inspect
curl -i http://localhost:8002/batches/B2026-002/archive     # 409 + 原因
curl http://localhost:8001/batches/B2026-002                # serving=false + suspend_reason
# 用留存 ZIP 修复：全部有效引用恢复供数
curl -F "file=@batch.zip" http://localhost:8002/batches/B2026-002/repair
curl http://localhost:8001/batches/B2026-001/archive -o roundtrip.zip
curl http://localhost:8001/storage/summary
```

## 测试

```bash
pip install -r requirements.txt pytest httpx
python -m pytest -q
```

测试覆盖：

- 上传/下载往返、2 MiB 限制、ZIP 校验、去重与单原件存储；
- 同号同内容复用、同号异内容 409、撤下号不可复用、撤下后 404；
- 共用原件只在最后一个引用批次撤下后才回收；
- **巡检**：健康核对、缺失/异文停供全部有效引用批次、查询显原因、
  下载 409、批号与引用保留、停供原件不被 GC 回收；
- **修复**：重传一致 ZIP 恢复全部有效引用、异文 409、撤下批次 409 且不复活、
  普通上传口重传即修复、健康原件幂等修复；
- **迟到巡检**：修复抢先完成后旧坏件结论按 `superseded` 作废，不误停供；
- **两个独立 OS 进程**共享同一数据目录的并发：相同内容并发上传、
  并发撤下、混合压力、**巡检停顿期间修复与无关批次照常完成**、
  跨实例巡检-停供-修复-恢复全流程、巡检与修复并发最终一致供数；
- 慢速下载不阻塞其他批次撤下，且开始的下载可完整结束；
- 回收两阶段中 `os._exit` 硬崩溃后的重启续清；**修复在换文件后、状态切换前
  崩溃**：重启保持停供（不开放半件）、可重试完成；临时分片与隔离区孤儿清理。

