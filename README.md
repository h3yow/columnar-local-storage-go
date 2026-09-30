# columnar-local-storage-go

一个零外部服务依赖的**本地列式数据存储组件**。结构化记录按列编码成段文件落盘，
支持批次写入、按列扫描读回、跨文件/跨批次的 schema 演进，以及在持续写入过程中的
**后台合并压缩（compaction）**。合并与读写通过快照与读写屏障协调，保证：

- 写入的记录读回与写入完全一致（含 null）；
- 合并期间不重复、不丢失，扫描不会读到未写完的中间状态；
- 合并失败时可见数据回滚到合并前；
- 一次扫描在任何时刻只打开 **1 个**段文件，与小文件数量无关（合并时 ≤ 2 个）。

仅依赖 Go 标准库。

## 快速开始

```go
schema := columnar.Schema{Columns: []columnar.ColumnDef{
    {ID: "id",   Name: "id",   Type: columnar.TypeInt64,   Nullable: false},
    {ID: "note", Name: "note", Type: columnar.TypeString,  Nullable: true},
}}

s, _ := columnar.Create("data/db", schema, columnar.Options{
    FlushThreshold:      1000, // 攒够多少行滚动一个段
    CompactionThreshold: 4,    // 存活段达到多少触发合并
    MergeMaxRows:        100000,
})
defer s.Close()

_ = s.Append(ctx, []columnar.Record{
    {"id": int64(1), "note": "a"},
    {"id": int64(2), "note": nil}, // 合法 null
})

sc, _ := s.Scan(ctx)
defer sc.Close()
for {
    var r columnar.Record
    if err := sc.Next(&r); err != nil { break } // io.EOF 结束
    // r 按“当前最新 schema”投影：新增列自动补 null
}
```

重新打开数据集用 `columnar.Open(dir, opts)`；手动触发合并用 `s.Compact(ctx)`，
把内存行刷盘用 `s.Flush()`；演进 schema 用 `s.EvolveSchema(next)`。

## 支持的列类型

| 逻辑类型        | Go 值类型 |
|-----------------|-----------|
| `TypeInt64`     | `int64`   |
| `TypeFloat64`   | `float64` |
| `TypeString`    | `string`  |
| `TypeBool`      | `bool`    |

null 用 Go 的 `nil` 表示。类型按上表**严格**匹配（`int` 不会被隐式当作 `int64`）。

## 数据文件布局

数据集就是一个普通目录（默认含一个进程级 `flock` 锁文件）：

```
<dir>/
├── lock                         # flock 排他锁，防止多进程并发写同一数据集
├── MANIFEST                     # 数据集元数据（JSON，原子替换）
├── seg-0000000000000001.col     # 正式列式段文件
├── seg-0000000000000002.col
├── seg-...col.tmp               # 普通写入的临时段（rename 前）
└── comp-0000000000000000.tmp    # 合并输出临时段（发布前）
```

### MANIFEST

记录数据集的全部持久化状态，与数据本体分离：

- `nextSegmentId`：单调递增的段 ID 分配器；
- `nextSeq`：下一条写入记录的全局序号（扫描顺序的依据）；
- `schemaVersion` 与 `schemas`：当前版本和**全部历史 schema**（旧段需要据此对齐）；
- `segments`：所有段的元数据（`id / fileName / schemaVersion / rowCount /
  minSeq / maxSeq / deleted`）。

MANIFEST 的每次更新都是 **写临时文件 → fsync → 原子 rename → fsync 目录**，
崩溃后只有“旧版本”或“新版本”两种结果，不存在半截状态。

### 段文件格式（小端二进制，自描述）

```
magic="CLS1" | formatVersion u16 | schemaVersion u32 | rowCount u32 | colCount u32
列定义 × colCount:
    id:string  name:string  type:u8  nullable:u8        # string = u32 长度 + UTF8
列数据块 × colCount（按列存放，而非按行）:
    presenceBitmap((rowCount+7)/8)   # bit=1 表示该行非 null
    nonNullCount u32
    values（仅紧凑存放非 null 值，按行顺序）:
        int64/float64 -> 8 字节定长；bool -> 1 字节；string -> u32 长度 + UTF8
```

普通写入：先写 `seg-<id>.col.tmp` 并 fsync，再原子 rename 为
`seg-<id>.col`，因此读者永远不会看到写一半的段。

## 写入、空值与错误语义

`Append(ctx, rows)` 对整批先逐条按当前 schema 校验，**任何一条不合法则整批拒绝**
（不会部分写入）。以下情况都不会被静默丢弃，而是返回可类型断言、可区分原因的
`*columnar.SchemaError`（字段 `Reason / Column / Detail`）：

| 情况                              | Reason                         |
|-----------------------------------|--------------------------------|
| 记录缺少 schema 中的某列           | `column_missing`               |
| null 写入 `Nullable=false` 的列    | `null_into_not_null_column`    |
| Go 值类型与列类型不一致            | `type_mismatch`                |
| 记录带了 schema 中不存在的列（拼写错误等） | `type_mismatch`（Column 指向该未知列） |

合法的 `nil` 会精确保留，扫描时仍读回 `nil`。扫描以**最新 schema** 做投影，
因此老段缺失的新增列会补出 `nil`。

## Schema 演进与兼容规则

列的身份由稳定的 **`ID`** 决定，`Name` 只是展示名。所有数据按 ID 对齐，
因此改名不会造成列错位或数据串列。`EvolveSchema` 只允许**加性（additive）**演进：

兼容（允许）：

- **新增列**：必须 `Nullable=true`，旧数据以 **null 作为稳定缺省值**；
- **改名**：仅 `Name` 改变、`ID` 不变；
- **放宽 nullable**：`false -> true`。

不兼容（明确拒绝，返回 `*columnar.CompatibilityError`，且数据集状态完全不变）：

| 变化                          | Reason                 |
|-------------------------------|------------------------|
| 删除列 / 列 ID 消失（含改名成新 ID） | `column_dropped`       |
| 列类型改变                     | `column_type_changed`  |
| nullable 由可空收紧为非空       | `nullable_tightened`   |
| 新增列声明为非空                | `nullable_tightened`   |

演进发生时，已 `Append` 但还在内存中未落盘的行会自动按新 schema 物化（新列补 null）。
段文件在头部写入其写入时的 `schemaVersion`，扫描时统一投影到当前最新版本。

## 合并（Compaction）与可见性语义

存活段数达到 `CompactionThreshold` 后，后台 goroutine（默认每 50ms 检查一次，
可用 `DisableAutoCompaction` 关闭）或手动 `Compact` 会把全部存活段合并为
一个/多个新段（每个输出段最多 `MergeMaxRows` 行）。

合并是一个**两阶段事务**：

1. **流式读取**：逐段打开输入（读完即关），写入 `comp-*.tmp` 输出段并 fsync。
   这一阶段 MANIFEST 完全不变，扫描方只看到旧段。同时打开的文件数
   = 1 个输入 + 1 个输出（≤ 2）。
2. **rename + 原子发布**：把输出 rename 成正式段名，然后一次原子更新 MANIFEST：
   新段转 live、旧段转 `deleted`。
3. **屏障下物理删除旧文件**：物理删除需要拿到 `scanMu` 写锁，必须等待所有
   在途扫描结束。扫描持有打开时刻的快照与读锁，因此引用的文件在扫描结束前
   绝不会被删除。
4. **清理 MANIFEST 中的 deleted 条目**（失败也不影响正确性，重开时兜底）。

由此得到的保证：

- **一致性快照**：扫描在开始时固定看到“MANIFEST 存活段 + 当时内存待落盘行”，
  合并期间开始的扫描要么看到合并前、要么看到合并后，不会读到半切换状态；
  已 `Append`（即使尚未滚成段）的行也立即可见。
- **不重复 / 不丢失**：每条记录在 MANIFEST 的存活集合中恰好出现一次；
  全局序号 `Seq` 连续，扫描按 `(MinSeq, ID)` 排序，顺序即写入顺序。
- **失败回滚**：MANIFEST 发布前的任何失败都会删除已写输出，可见数据与合并前
  完全一致，且不留临时/孤儿文件。发布后若物理删除失败，数据仍然正确
  （旧段已标记 deleted、对扫描不可见），下次 `Open` 时清理。
- **打开文件数有上界**：一次扫描任何时刻只打开 1 个段文件（流式逐段），
  不随小文件数量线性增长；合并过程 ≤ 2。

### 崩溃恢复

`Open` 时执行 `reconcileOnOpen`：删除所有 `*.tmp`、删除未被 MANIFEST 引用的
孤儿段、物理删除已标记 deleted 的段并移除其条目；若发现存活段文件丢失则
返回损坏错误。MANIFEST 始终是最后一步发布，因此段文件孤儿只会是“尚未可见”
的数据，删除安全。

### 锁模型

- `lock`（`flock`）：进程级互斥，同一数据集同一时刻只有一个写入进程；
- `mu`：保护 MANIFEST 发布与 append/compact 的元数据更新；
- `scanMu`：读写屏障，扫描全程持 `RLock`，物理删除旧段持 `Lock`。

## 错误分类总览

- `*columnar.SchemaError`：记录与 schema 不匹配（见上表）；
- `*columnar.CompatibilityError`：不兼容的 schema 演进；
- `*columnar.CompactionError`：合并过程失败（回滚后可见数据不变）；
- `columnar.ErrStoreClosed`：在已关闭的数据集上操作；
- 其余 I/O / 损坏错误直接返回底层 error。

## 本地验证

无需任何外部服务或凭据，纯标准库：

```bash
go build ./...
go vet ./...
go test ./... -race -v        # 含竞态检测的完整用例
go test ./... -race -count=10 # 压一压并发用例的稳定性
```

测试覆盖（日志中会打印关键输入、当时的可见状态与判定依据）：

- `TestWriteAndScanRoundTrip`：多批次写入后按列读回，逐行逐列与输入一致；
- `TestNullAndMissingAndTypeErrorsAreDistinguishable`：null、缺列、类型不符、
  未知列四类错误的 Reason/Column 可区分，且坏数据不落盘、合法 null 精确保留；
- `TestSchemaEvolution_AddColumnDefaultNull`：新增列缺省 null、改名不串列、
  跨段/跨批次演进、关闭重开后语义持久；
- `TestSchemaEvolution_IncompatibleChangesRejected`：删列/改类型/收紧 nullable/
  新增非空列均被拒绝且状态不变；
- `TestCompactionReducesSegmentsAndPreservesData`：段数下降、行数与顺序不变、
  旧文件物理删除、重开一致；
- `TestCompactionConcurrentWrites`：合并进行中多写者并发追加 + 并发扫描，
  最终每条记录恰好出现一次，并断言打开段文件数峰值 ≤ 2；
- `TestCompactionFailureRollsBack`：在“发布前”和“rename 后发布前”两个点注入失败，
  断言可见状态、段文件数与回滚前一致，故障解除后能继续合并成功；
- `TestScanSnapshotAcrossCompaction`：在途扫描跨越合并时读屏障保护旧视图，
  结束后新扫描看到合并结果；
- `TestBackgroundAutoCompaction`：不手动触发，持续写入期间后台自动压缩，
  任意时刻数据自洽，最终段数有上界。
