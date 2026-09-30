# columnar-local-storage-go

纯本地、仅依赖 Go 标准库的列式记录存储组件。结构化记录按**批次写入、列式落盘**，
可**按列扫描读回**，并在持续写入过程中做**后台合并压缩（compaction）**，
使读取性能与同时打开的文件数不随小文件数量线性增长。

无外部服务、无真实凭据，全部行为可在本地用 `go test` 验证。

---

## 1. 快速开始

```go
schema, _ := columnstore.NewSchema([]columnstore.Column{
    {Name: "id",    Type: columnstore.TypeInt64,   Nullable: false},
    {Name: "name",  Type: columnstore.TypeString,  Nullable: true},
    {Name: "score", Type: columnstore.TypeFloat64, Nullable: true},
})

st, _ := columnstore.Open("./data", schema, &columnstore.Options{
    MaxSegments: 4,                 // 可见段达到该数量时触发后台合并
    Logger:      log.Default(),     // 可选；关键状态日志
})

// 按批次写入（任一行非法则整批拒绝，不产生部分写入）
err := st.Write(columnstore.Batch{
    {"id": int64(1), "name": "alice", "score": float64(1.5)},
    {"id": int64(2), "name": nil},                 // 显式 null
    {"id": int64(3), "score": float64(3.5)},       // 缺列 -> 缺省 null
})

// schema 演进
_ = st.AddColumn("tag", columnstore.TypeString, true, nil) // 旧行补 null
_ = st.RenameColumn("name", "user")                        // 仅改名，列 ID 不变
_ = st.DropColumn("score")                                 // 逻辑删除

// 按列读回（行序 = 段在清单中的顺序 + 段内行序，即写入先后顺序）
rs, _ := st.ReadColumns([]string{"id", "user", "tag"})

_ = st.ForceCompaction(context.Background()) // 也可等待后台自动合并
_ = st.Close()
```

支持的列类型：`TypeInt64`、`TypeFloat64`、`TypeString`、`TypeBool`、`TypeBytes`。
类型检查是**严格**的：`int` 不会自动转成 `int64`，`float64` 不会转成 `int64`。

---

## 2. 数据文件布局

```
<data-dir>/
├── MANIFEST.json              # 唯一的可见性事实来源（当前 schema + 可见段列表）
├── seg-0000000001.cs          # 不可变列式段文件
├── seg-0000000002.cs
└── tmp/
    └── seg-000000000N.cs.compacting   # 合并产生的临时段，发布前对读不可见
```

### 2.1 清单 MANIFEST.json

JSON，原子写入（写临时文件 + `fsync` + `rename` + 目录 `fsync`）：

```json
{
  "generation": 7,
  "next_segment_id": 12,
  "schema": { "version": 3, "columns": [ ... ] },
  "segments": [
    { "id": 10, "path": ".../seg-0000000010.cs", "rows": 128 }
  ]
}
```

- **只有出现在清单里的段才是可见的**。未关闭完、未 rename 成功、未写入清单的
  临时文件对读取方完全不可见。
- 每次发布 `generation` 单调递增。崩溃后以磁盘上的清单为准；`tmp/` 中
  未被引用的临时段在打开时被清理。
- 列的缺省值（`DefaultValue`）带**类型标记**序列化，保证 `int64` 经 JSON
  往返后仍是 `int64`（而不是 `float64`），从而满足"写回读回完全一致"。

### 2.2 段文件二进制布局（魔数 `CSSEG001`）

```
[ 8B  魔数 "CSSEG001" ]
[ 4B  schema 长度 ][ schema 的 gob（完整列定义，含稳定列 ID、类型、缺省值）]
对每一列（按列 ID 顺序）：
    [ 4B 块长度 ][ Block 的 gob ]
[ segmentIndex 的 gob ][ 末尾 4B 索引体长度 ]
```

- `Block` 是一列的列式数据：`Nulls []bool` + 对应类型的切片
  （`Ints/Floats/Strings/Bools/Bytes`），所有切片长度都等于行数；
  null 位置在类型切片中为零值。
- 末尾索引记录每个列块的**文件偏移与长度**。按列读取时只 `ReadAt` 目标列块
  所在区间，不解码其他列。
- 段一经成功 `Close` 即**不可变**（immutable）。列对齐永远依据**稳定列 ID**，
  不依赖列在文件中的位置或序号——这是改名/删列不串列的基础。

---

## 3. Schema 兼容与演进规则

列的**身份是稳定的数字 ID**，不是列名；ID 一经分配永不复用。
演进只改清单中的 schema 映射，**不重写任何历史段**。

| 变更 | 语义 | 历史数据如何读回 |
|------|------|------------------|
| **加列** `AddColumn` | 分配新 ID（当前最大 ID + 1），schema 版本 +1 | 旧段没有该列块，按 `DefaultValue` 补值；未提供缺省值且可空时补 `null`。**后续新增的非空列必须给出同类型非空缺省值**（否则拒绝），因为要为已存在的行回填 |
| **改名** `RenameColumn` | 只改 `Name`，**ID 不变**，版本 +1 | 旧名变为未知列（读返回 `ErrNotFound`、写返回 `ErrUnknownColumn`）；新名读到的就是旧数据，与其它列严格按 ID 对齐，不错位、不串列 |
| **删列** `DropColumn` | 置 `Deleted=true`（保留历史块），版本 +1，ID 不复用 | 读取不再暴露、写入拒绝该列；之后再新增同名列会分配**新 ID**，旧列数据绝不会被误读成新列 |
| **改类型** | 无 API 入口；手工破坏段文件造成同 ID 类型漂移时，读取/合并返回 `ErrIncompatibleSchema` | **明确拒绝，绝不按错误结构读出** |

初始建表时，非空列允许不提供缺省值（缺失在写入时按 `ErrColumnMissing` 拒绝）；
只有"对已有数据集追加非空列"才强制要求缺省值。

### 3.1 写入时的可区分错误

`errors.Is(err, …)` 可判定，且错误经 `*ColumnError` 携带列名与操作：

| 情况 | 哨兵错误 |
|------|----------|
| 缺少必填列（无缺省值） | `ErrColumnMissing` |
| 非空列写入 `null` | `ErrNullInNonNullable` |
| Go 值类型与列类型不一致（含拒绝隐式数值转换） | `ErrTypeMismatch` |
| 出现 schema 中不存在/已删除/改名前的列名 | `ErrUnknownColumn` |
| 不兼容 schema 变更（重复加列、改名冲突、非空加列无缺省、同 ID 类型漂移） | `ErrIncompatibleSchema` |
| 列/数据集不存在 | `ErrNotFound` |
| 存储已关闭 / 已有合并进行 | `ErrClosed` / `ErrCompactionBusy` |

空值、列缺失、类型不一致的记录**绝不会被静默丢弃**：整批原子拒绝，
可见状态保持不变；合法数据写回读回逐字段一致（含 null 位图与 `[]byte`）。

---

## 4. 合并与可见性语义

### 4.1 触发

- **自动**：一次 `Write` 发布后可见段数量达到 `Options.MaxSegments`，
  在后台 goroutine 触发一次合并。
- **手动**：`ForceCompaction(ctx)` 同步执行，便于确定性测试。

### 4.2 协议（为什么不重复、不丢失、读不到半成品）

1. **抓取快照 + 占用合并位**：在互斥区内记录"目标段有序集合、当时 schema
   版本、预分配的新段 ID"。同一时刻只有一个合并在运行。
2. **锁外合并**：旧段不可变，逐个顺序打开，按行重组，以当时 schema 写入
   `tmp/` 下的单个新临时段。旧段缺列（加列前数据）按缺省值/null 补齐。
3. **一致性检查 + 原子发布**：回到互斥区，校验段集合与 schema 版本未变；
   将临时段 `rename` 为正式段，再用"临时清单 + rename"**原子替换**
   MANIFEST。清单切换的瞬间，读者看到的要么是替换前的完整旧状态，
   要么是替换后的完整新状态——不会看到中间态。
4. **删除旧段**：清单切换后旧段对新读者已不可见。若仍有在途扫描持有这些
   文件（引用计数 pin），删除会**延迟到最后一个扫描结束**，绝不打断读取。

### 4.3 并发读写

- `ReadColumns` 在读锁下取清单快照并 `pin` 其中所有段，扫描结束才 `unpin`。
  读取不进入写入/合并的互斥区，因此不会被写阻塞。
- 写入与合并共享一把"段发布锁"，串行化 **ID 预留 + 清单发布**，保证段 ID
  不冲突、清单替换不交错。
- `Close` 会等待在途后台合并收敛（完成发布或清理临时段回滚），返回后不残留
  未引用的临时文件；关闭后排队的写入/合并直接返回 `ErrClosed`。

### 4.4 失败回滚

合并在"新段写完、清单替换前"的任何失败（含测试注入故障）：

- 清单从未指向新段，删除 `tmp/` 下的临时段即可；
- 旧清单、旧段全部保留，**可见数据完全回到合并前**（段数、行数、schema
  版本不变），随后的写入与再次合并照常工作。

### 4.5 打开文件数有上界

读取**逐段顺序打开、读完立即关闭**，并只读取被请求列的文件区间。
因此无论磁盘上累积了多少个小文件，一次扫描**任意时刻只打开 1 个段文件**；
磁盘上现存文件数也会被合并压缩到有界（当前可见段 + 至多一批等待在途读者
的旧段）。`TestReadOpenFilesBounded` 通过文件打开/关闭钩子实测句柄峰值恒为 1。

---

## 5. 本地验证

```bash
go build ./...
go vet ./...
go test ./... -count=1

# 竞态检测（并发写入/读取/合并/演进）
go test ./... -race -count=5

# 只跑关键场景
go test ./... -v -run 'TestEvolution'                 # schema 演进
go test ./... -v -run 'TestCompactionConcurrent'      # 合并期间并发读写
go test ./... -v -run 'TestCompactionFailureRollback' # 合并失败回滚
go test ./... -v -run 'TestReadOpenFilesBounded'      # 打开文件数上界
go test ./... -v -run 'TestReadRejects|TestMissing'   # 不兼容/损坏路径
```

### 测试覆盖一览

| 测试 | 覆盖点 |
|------|--------|
| `TestWriteAndReadRoundTrip` / `TestAllTypesRoundTrip` | 全部类型、null、缺列缺省、跨批次读回一致 |
| `TestWriteValidationErrors` / `TestBatchAtomicReject` | 五类可区分写入错误、整批原子拒绝、无部分写入 |
| `TestEvolutionAddColumn/Rename/Drop/Errors` | 加列缺省语义、改名不串列、删列 ID 不复用、持久化重开 |
| `TestCompactionConcurrentWithEvolution` | 合并与加列并发，演进不丢失 |
| `TestCompactionBasic/AutoKeepsSegmentsBounded` | 合并正确性、段数/文件数有界 |
| `TestCompactionConcurrentWriters/Readers` | 合并期间并发写不重不丢；并发读不回退、不错位 |
| `TestCompactionFailureRollback/RollbackThenEvolve` | 注入失败回滚、回滚后可继续演进与合并 |
| `TestReadRejectsCrossFileTypeChange` / `TestMissingSegmentFileIsError` | 同 ID 类型漂移被拒、清单引用段缺失显式报错 |
| `TestReadOpenFilesBounded` | 单次扫描并发打开文件句柄峰值 = 1 |

测试通过 `Options.Logger` 打印关键输入、当前可见状态（schema 版本 /
可见段数 / 总行数）与判定依据；失败时日志会随 `t.Logf` 一并输出。

---

## 6. 范围与取舍

- 面向**单进程、本地文件系统**的数据集；不提供多进程写协调与网络服务。
- 侧重正确性与可断言的可见性/回滚语义，而非极限吞吐：写入以"段"为粒度
  原子发布，合并是全量重组。
- 不做跨进程崩溃恢复的 WAL；崩溃后以最近一次成功 `fsync+rename` 的清单为准，
  `tmp/` 中未发布的临时段会被清理。
