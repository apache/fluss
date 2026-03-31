# Fluss Variant Shredding 设计方案

## 1. Parquet Variant Shredding 机制

### 1.1 Variant 是什么

Variant 是一种半结构化数据类型，将 JSON 等数据以二进制编码存储。每个 Variant 值由两部分组成：

- **metadata**：字段名字典
- **value**：自描述的二进制编码值（对象、数组、基础类型等）

#### metadata 二进制格式

metadata 是**每行独立的**二进制字段，存储该行 Variant 对象中所有字段名的去重字典。二进制布局：

```
+----------+------------------+--------------------------------------+--------------------+
| Header   | Dictionary Size  | Offsets Array                        | String Bytes       |
| (1 byte) | (offset_size B)  | ((dict_size + 1) * offset_size B)    | (变长)              |
+----------+------------------+--------------------------------------+--------------------+
```

**Header 字节**：

```
位   7   6   5   4   3   2   1   0
     └─offset_size─┘   │   └─── version ───┘
        minus_1        sorted
```

| 位 | 含义 | 取值 |
|-----|------|------|
| 0-3 | 版本号 | 必须为 1 |
| 4 | sorted_strings 标志 | 1 = 字典按字母序排列（支持二分查找） |
| 5 | 保留 | 0 |
| 6-7 | offset_size_minus_1 | 0→1字节, 1→2, 2→3, 3→4字节 |

Fluss 始终使用 4 字节偏移 + 已排序，因此 header 固定为 `0xD1`（version=1 | sorted=0x10 | offset_size_bits=0xC0）。

**字典内容示例**：

对于 JSON `{"name":"alice", "age":30, "city":"beijing"}`，metadata 存储的是 3 个字段名按字母序排列的字典：

```
Header: 0xD1
Dictionary Size: 3 (小端序 4字节: 03 00 00 00)
Offsets: [0, 3, 7, 11]         ← 4 个偏移（dict_size+1）指向 string bytes 内的位置
String Bytes: "age" + "city" + "name" = [61 67 65 | 63 69 74 79 | 6E 61 6D 65]
                                        offset 0    offset 3     offset 7     offset 11
字典索引:                               id=0(age)   id=1(city)   id=2(name)
```

value 中的对象字段通过 **字典索引 id** 引用字段名，而非存储字段名字符串本身。如 `"name"` 字段在 value 中用 id=2 表示。查找字段时，因为字典已排序，可以二分查找字段名对应的 id。

> **重要：metadata 是每行独立的**。同一列中，第 0 行的 metadata 可能含 `{age, name}`，第 1 行含 `{city, email, name}`，完全独立。这是 Variant 作为半结构化类型的核心 —— 每行的字段集合可以不同。

### 1.2 为什么需要 Shredding

将 Variant 整块存为一个 Binary 列时，即使只查一个字段也必须完整读取和反序列化整个 Variant。Shredding（切碎）将高频访问的字段提取到独立的 **强类型列（typed_value）** 中，从而：

- 利用列式压缩、编码（RLE、字典编码等）
- 支持列统计信息用于 Data Skipping
- 支持列裁剪（Column Pruning），只读需要的字段

### 1.3 typed_value 的存储结构

Parquet 中每个 Variant 字段用一个 **group** 表示，包含三个子列协作工作：

```
required group event (VARIANT) {
  required binary metadata;        -- 字段名字典（所有字段共用）
  optional binary value;           -- 兜底：Variant 二进制编码
  optional <type> typed_value;     -- 强类型列 / shredded 对象组
}
```

#### value 与 typed_value 的语义矩阵

| value    | typed_value | 含义                                         |
|----------|-------------|----------------------------------------------|
| null     | null        | 字段不存在（仅 object 的 shredded field 合法） |
| non-null | null        | 值存在，类型不匹配预期，退回二进制编码           |
| null     | non-null    | 值存在，类型匹配，存在强类型列中                |
| non-null | non-null    | 部分 shredded 对象：typed_value 存已拆字段，value 存残余 |

这是 Parquet Variant Shredding 的核心设计理念：**每一层都有“正轨”（typed_value）和“兜底”（value）两条路**。类型匹配走正轨享受列式优化，不匹配走兜底保证不丢数据。

#### metadata、value、typed_value 的协作关系

- **metadata**：专门用来解码 `value` 二进制的字段名字典。只包含 residual 引用的字段名。
- **value**：残余/兜底的 Variant 二进制编码，通过 metadata 字典 id 引用字段名。
- **typed_value**：强类型列，字段名由 Struct 列名决定，**不依赖 metadata**。

三者的分工决定了读取时的数据流向：
- 查询已 shred 字段 → 只读 typed_value，不需要 metadata 和 value
- 查询未 shred 字段 → 只读 metadata + value，不需要 typed_value
- 完整还原 → 三者合并

> 具体的列存存储布局和多行读写示例参见 [3.1 核心思路](#31-核心思路)。

#### 对象的 Shredding

假设要 shred `event_type`(string) 和 `event_ts`(timestamp)：

```
optional group event (VARIANT) {
  required binary metadata;
  optional binary value;                       -- 存残余字段（如 email）
  optional group typed_value {                 -- shredded 对象
    required group event_type {                -- 每个字段也是 value + typed_value 对
      optional binary value;                   -- 类型不匹配时的兜底
      optional binary typed_value (STRING);    -- 匹配时直接存 string
    }
    required group event_ts {
      optional binary value;
      optional int64 typed_value (TIMESTAMP);
    }
  }
}
```

实际存储示例：

| 原始 JSON | value (残余) | event_type.typed_value | event_ts.typed_value |
|---|---|---|---|
| `{"event_type":"login", "event_ts":1729..., "email":"a@b.com"}` | `{"email":"a@b.com"}` | "login" | 1729... |
| `{"event_type":"login", "event_ts":1729...}` | null（完全 shred） | "login" | 1729... |
| `{"error_msg":"bad"}` | `{"error_msg":"bad"}` | null（无此字段） | null |
| `"not an object"` | `0x13 0x6E...`（string 二进制） | null | null |

#### 数组的 Shredding

数组用 Parquet 3-level LIST 表示，每个元素也有 value + typed_value 对：

```
optional group tags (VARIANT) {
  required binary metadata;
  optional binary value;
  optional group typed_value (LIST) {
    repeated group list {
      required group element {
        optional binary value;                  -- 元素类型不匹配时的兜底
        optional binary typed_value (STRING);   -- 元素是 string 时走这里
      }
    }
  }
}
```

#### 递归嵌套

typed_value 结构支持任意层级的递归嵌套——typed_value 里的每个字段可以进一步包含自己的 value + typed_value 对，实现深层对象/数组的 shredding。

### 1.4 typed_value 的类型映射

| Variant Type | Parquet Physical Type | Parquet Logical Type |
|---|---|---|
| boolean | BOOLEAN | |
| int8 | INT32 | INT(8, signed=true) |
| int16 | INT32 | INT(16, signed=true) |
| int32 | INT32 | |
| int64 | INT64 | |
| float | FLOAT | |
| double | DOUBLE | |
| decimal4 | INT32 | DECIMAL(P, S) |
| decimal8 | INT64 | DECIMAL(P, S) |
| decimal16 | BYTE_ARRAY / FIXED_LEN_BYTE_ARRAY | DECIMAL(P, S) |
| date | INT32 | DATE |
| timestamp | INT64 | TIMESTAMP(true/false, MICROS/NANOS) |
| binary | BINARY | |
| string | BINARY | STRING |
| array | GROUP (LIST) | |
| object | GROUP (Struct) | |

---

## 2. Arrow 对 Variant 的支持现状

### 2.1 Canonical Extension Type: `arrow.parquet.variant`

Arrow 社区在 **v22.0.0（2025年10月）** 将 Variant 纳入 Canonical Extension Type，扩展名为 `arrow.parquet.variant`。

在 Arrow 内存中，Variant 用 Struct 表示：

```
// 非 shredded（基础形态）
Struct {
  metadata: Binary/BinaryView,    -- 字段名字典
  value: Binary/BinaryView        -- Variant 二进制编码
}

// Shredded（带类型提取）
Struct {
  metadata: Binary/BinaryView,
  value: Binary/BinaryView,       -- 残余/兜底
  typed_value: Struct {            -- 与 Parquet shredding 规则对齐
    field_a: Struct {
      value: Binary,
      typed_value: Int64
    },
    field_b: Struct {
      value: Binary,
      typed_value: Utf8
    }
  }
}
```

### 2.2 各语言实现进展

| 语言 | Variant 基础 | Shredding (typed_value) | 说明 |
|------|-------------|------------------------|------|
| **arrow-rs (Rust)** | ✅ 完整 | ✅ 最成熟 | `shred_variant` 函数、`VariantArrayBuilder`、typed_access |
| **parquet-java** | ✅ 完整 | ✅ 完整 | Parquet 层面 shredded read/write |
| **arrow-go** | ✅ 完整 | ✅ 完整 | VariantType Extension + shredded array builder |
| **arrow-java 19.0.0** | ✅ 基础 | ❌ 未实现 | 仅 metadata + value，无 typed_value |
| **arrow C++** | ⚠️ 仅类型定义 | ❌ 未实现 | decoding/encoding/shredding 均 Open |
| **DuckDB** | ✅ 完整 | ✅ 完整 | |

> **来源**: [Parquet Implementation Status](https://parquet.apache.org/docs/file-format/implementationstatus/)

### 2.3 Arrow Java 19.0.0 的 Variant 支持

Issue: [apache/arrow-java#946](https://github.com/apache/arrow-java/issues/946)
PR: [apache/arrow-java#947](https://github.com/apache/arrow-java/pull/947) — 已合并，发布于 2026年3月16日。

提供的能力：

- **`arrow-vector` 模块**：`VariantType` Extension Type 定义
- **`arrow-variant` 模块**：`VariantVector`（metadata/value pair）、`Variant` 解析类、Reader/Writer 支持
- **不包含**：shredded variant（typed_value Struct）的构建和读取

Maven 坐标：

```xml
<dependency>
    <groupId>org.apache.arrow</groupId>
    <artifactId>arrow-vector</artifactId>
    <version>19.0.0</version>
</dependency>
<dependency>
    <groupId>org.apache.arrow</groupId>
    <artifactId>arrow-variant</artifactId>
    <version>19.0.0</version>
</dependency>
```

---

## 3. Fluss Variant Shredding 实现方案

### 3.1 核心思路

基于 `arrow.parquet.variant` Extension Type 规范，将 shredded 列**内嵌到 Variant 列的 Struct 内部**。Shredding 对用户完全透明，表的对外 schema 始终不变：

```
(id INT, data VARIANT)    -- 无论是否 shredding，用户看到的 schema 始终如此
```

Variant 列内部结构：

```
StructVector("data") {                      -- arrow.parquet.variant Extension Type
  metadata: VarBinaryVector                 -- 字段名字典
  value: VarBinaryVector                    -- 残余/兜底 Variant 二进制
  typed_value: StructVector {               -- shredded 字段（新增）
    "name": StructVector {
      value: VarBinaryVector                -- 类型不匹配时的兜底
      typed_value: VarCharVector            -- String
    }
    "age": StructVector {
      value: VarBinaryVector
      typed_value: BigIntVector             -- Int64
    }
  }
}
```

#### value 与 typed_value 的行级存储示例

每个 shredded 字段的 `value` 和 `typed_value` 是**等长的两个 Vector**，按行索引一一对应。每一行只有其中一个非 null（或两个都 null 表示字段不存在）。

以 `"name"` 字段（期望类型 String）为例，假设一个 batch 有 5 行：

```
行号  原始 JSON                        name.value (VarBinary)        name.typed_value (VarChar)
──────────────────────────────────────────────────────────────────────────────────────────
  0    {"name":"alice", "age":30}     null                          "alice"    ← 类型匹配，走 typed_value
  1    {"name":"bob"}                 null                          "bob"      ← 类型匹配
  2    {"name":12345}                 0x05 0x30... (int 二进制)    null       ← 类型不匹配，走 value 兜底
  3    {"age":25}                     null                          null       ← name 不存在，两个都 null
  4    {"name":"eve", "age":20}       null                          "eve"      ← 类型匹配
```

读取逻辑很简单 —— 对每一行，优先检查 `typed_value`，不为 null 则直接读；否则检查 `value` 反序列化；两者都为 null 则字段不存在：

```java
Variant readShreddedField(int row, StructVector fieldStruct) {
    VarCharVector typedValue = (VarCharVector) fieldStruct.getChild("typed_value");
    VarBinaryVector value = (VarBinaryVector) fieldStruct.getChild("value");

    if (!typedValue.isNull(row)) {
        // 快速路径：类型匹配，直接读强类型值
        return Variant.fromString(typedValue.get(row));
    } else if (!value.isNull(row)) {
        // 兜底路径：类型不匹配，反序列化 Variant 二进制
        return Variant.fromBinary(value.get(row));
    } else {
        // 该行没有这个字段
        return null;
    }
}
```

> null 的行不占实际数据空间，只占 validity bitmap 中的 1 bit 标记。

#### 列存视角的完整读写示例

以两行数据为例，shredding schema 决定 shred `name`(STRING) 和 `age`(BIGINT)：

```
Row 0: {"name":"alice", "age":30,        "email":"a@b.com"}
Row 1: {"name":"bob",   "age":"unknown", "city":"beijing"}
                          ↑ 类型不匹配 BIGINT
```

**写入后的 Arrow StructVector 布局（每列是一个 Vector，每行是一个 slot）：**

```
StructVector("data") —— arrow.parquet.variant Extension Type
│
├─ metadata: VarBinaryVector (2 rows)
│     Row 0:  [0xD1, 01 00 00 00, ...]  ← 字典 = ["email"]，只含 residual 的字段名
│     Row 1:  [0xD1, 01 00 00 00, ...]  ← 字典 = ["city"]，每行 metadata 独立
│
├─ value: VarBinaryVector (2 rows)
│     Row 0:  <{"email":"a@b.com"} 的 Variant 二进制>  ← 通过 metadata 字典 id=0 引用 "email"
│     Row 1:  <{"city":"beijing"} 的 Variant 二进制>   ← 通过 metadata 字典 id=0 引用 "city"
│
└─ typed_value: StructVector
      │
      ├─ "name": StructVector
      │     ├─ value: VarBinaryVector
      │     │     Row 0: null
      │     │     Row 1: null
      │     └─ typed_value: VarCharVector
      │           Row 0: "alice"   ← STRING 匹配，走正轨
      │           Row 1: "bob"     ← STRING 匹配，走正轨
      │
      └─ "age": StructVector
            ├─ value: VarBinaryVector
            │     Row 0: null
            │     Row 1: <"unknown" 的 Variant 二进制>  ← STRING 不匹配 BIGINT，走兜底
            └─ typed_value: BigIntVector
                  Row 0: 30       ← BIGINT 匹配，走正轨
                  Row 1: null     ← 类型不匹配，null
```

关键观察：
- **metadata VarBinaryVector**：每行是一个独立的二进制字典。Row 0 和 Row 1 的 metadata 完全不同，因为它们的 residual 字段不同
- **value VarBinaryVector**：每行是 residual 对象的 Variant 二进制，内部通过该行 metadata 的字典 id 引用字段名
- **typed_value 内的每个字段**：字段名由 StructVector 的 child name 决定，与 metadata 无关
- **同一列内 Vector 长度严格相等**：VarBinaryVector、VarCharVector、BigIntVector 都是 2 行，通过 validity bitmap 标记 null

**读取场景 1：只读 `name`（已 shred 字段）**

```
只需读 typed_value."name".typed_value 这一个 VarCharVector：
  Row 0: VarCharVector.get(0) = "alice"
  Row 1: VarCharVector.get(1) = "bob"
不需要触碰 metadata 和 value Vector，零反序列化。
```

**读取场景 2：只读 `age`（已 shred，存在类型不匹配）**

```
只需读 typed_value."age" 的两个子 Vector，逐行判断：
  Row 0: BigIntVector.isNull(0)=false     → 走正轨，BigIntVector.get(0)=30
  Row 1: BigIntVector.isNull(1)=true      → 检查 VarBinaryVector.get(1)，解码得到 "unknown"
仍然不需要触碰顶层 metadata 和 value。
```

**读取场景 3：只读 `email`（未 shred 字段）**

```
只需读顶层 metadata + value 两个 VarBinaryVector，不需要 typed_value：
  Row 0: metadata=["email"], value=<residual 二进制>
         → 用 metadata 字典解码 value，查找 "email" → id=0 → 提取得到 "a@b.com"
  Row 1: metadata=["city"], value=<residual 二进制>
         → 用 metadata 字典解码 value，查找 "email" → 字典中没有 → 返回 null
```

**读取场景 4：读全部字段（完整还原）**

```
三者都需要读，逐行合并：

Row 0:
  1. 解码 residual: metadata VarBinaryVector.get(0) + value VarBinaryVector.get(0)
     → 字典=["email"] + value 二进制 → {"email":"a@b.com"}
  2. 读 typed_value: name="alice" (VarCharVector), age=30 (BigIntVector)
  3. 合并: {"name":"alice", "age":30, "email":"a@b.com"}

Row 1:
  1. 解码 residual: metadata VarBinaryVector.get(1) + value VarBinaryVector.get(1)
     → 字典=["city"] + value 二进制 → {"city":"beijing"}
  2. 读 typed_value: name="bob" (VarCharVector), age 兜底解码 → "unknown"
  3. 合并: {"name":"bob", "age":"unknown", "city":"beijing"}
```

### 3.2 前提条件

| 前提 | 状态 | 说明 |
|------|------|------|
| Arrow StructVector 嵌套 | ✅ 已具备 | Arrow Java 15.0.0 就支持 |
| Variant 二进制编解码 | ✅ 已具备 | VariantUtil / VariantBuilder |
| Shredding 统计推断 | ✅ 已具备 | VariantStatisticsCollector / ShreddingSchemaInferrer |
| ShreddedVariant 合并读取 | ✅ 已具备 | ShreddedVariantColumnVector（需适配） |
| Arrow Java Variant Extension Type | ⚠️ 需升级 | Arrow Java 15.0.0 → 19.0.0 |
| typed_value Writer 拆分逻辑 | ❌ 需实现 | 将字段写入 Struct 内部子 Vector |
| typed_value Reader 逻辑 | ❌ 需适配 | 从 Struct 内部读取替代从顶层列读取 |
| Struct 内部 schema 管理 | ❌ 需实现 | typed_value 的子字段增减机制 |

### 3.3 第一期能力边界（Scope Limitation）

第一期实现聚焦于 **最常见、最有收益的场景**，有意限制 typed_value 的复杂度，避免引入过多边界情况。

#### 支持范围

| 维度 | 第一期支持 | 说明 |
|------|-----------|------|
| Shredding 深度 | **仅顶层字段** | 只分析 Variant 对象的第一层 key，不递归进入嵌套对象 |
| typed_value 类型 | **标量基础类型** | BOOLEAN, BIGINT, FLOAT, DOUBLE, STRING, DATE, TIMESTAMP |
| 整数类型处理 | **统一拓宽为 BIGINT** | INT8/INT16/INT32/INT64 全部映射为 BIGINT，避免类型不一致 |
| typed_value 结构 | **Struct { value: Binary, typed_value: <scalar> }** | 每个 shredded 字段是固定的 value + typed_value 二元结构 |
| Variant 顶层类型 | **仅对象（Object）** | 顶层 Variant 是对象时才触发 shredding，非对象（如字符串、数组）直接走 value 兜底 |

#### 明确不支持（后续版本）

| 场景 | 原因 | 预计支持版本 |
|------|------|-------------|
| **嵌套对象 shredding** | typed_value 内部再嵌套 Struct，StructVector 层级加深，读写/合并逻辑复杂度指数增长 | v2+ |
| **数组 shredding** | 需要 ListVector 嵌套 Struct（Parquet 3-level LIST），Arrow 的 ListVector 投影、split/merge 逻辑复杂 | v2+ |
| **Map 类型 shredding** | Variant 规范中无原生 Map，且 Parquet Map shredding 缺乏社区共识 | 待定 |
| **DECIMAL / BINARY 类型** | 出现频率低，且 DECIMAL 的精度/标度推断规则复杂 | v2+ |
| **多层递归 typed_value** | 如 `typed_value.address.typed_value.city.typed_value`，Arrow StructVector 嵌套过深影响性能和可维护性 | v2+ |
| **Schema 降级（删除 shredded 字段）** | 第一期字段只增不减，已 shredded 的字段保持到数据过期。降级需处理存量 batch 兼容性 | v2+ |

#### 第一期 StructVector 实际布局

基于以上限制，第一期的 StructVector 结构是扁平的、可预测的：

```
StructVector("data") {                         -- Variant Extension Type
  metadata: VarBinaryVector                    -- 字段名字典
  value: VarBinaryVector                       -- 残余/兜底（去除已 shred 字段的对象）
  typed_value: StructVector {                  -- 最多 N 个子字段（默认上限 20）
    "name": StructVector {
      value: VarBinaryVector                   -- 类型不匹配时的兜底
      typed_value: VarCharVector               -- String（标量）
    }
    "age": StructVector {
      value: VarBinaryVector
      typed_value: BigIntVector                -- Int64（标量）
    }
    "is_active": StructVector {
      value: VarBinaryVector
      typed_value: BitVector                   -- Boolean（标量）
    }
    -- 注意：不会出现 typed_value 内部再嵌套 StructVector 的子 typed_value
  }
}
```

关键约束：
- **typed_value 内部每个子字段的 typed_value 一定是标量 Vector**（VarCharVector, BigIntVector, BitVector 等），不会是 StructVector 或 ListVector
- **typed_value 的最大子字段数**由 `maxShreddedFields` 控制（默认 20），防止字段爆炸
- **统计采样阈值**：字段出现率 ≥ 0.5 且类型一致性 ≥ 0.9 才会被选为 shredded 字段

#### 第一期边界带来的简化

| 方面 | 简化效果 |
|------|----------|
| Writer | 只需处理「对象 → 提取标量字段 → 写入对应 Vector」，无需递归拆解 |
| Reader | 合并逻辑只有一层：residual + 标量 typed_value，无需递归合并 |
| 列裁剪 | 投影路径最多两层（`typed_value.name.typed_value`），无需多层嵌套投影 |
| Schema 管理 | typed_value 的 Struct children 只增不减，无需处理降级/迁移 |
| 测试覆盖 | 有限的类型组合（~7 种标量 × 兜底），可完整覆盖边界场景 |

### 3.4 动态 Shredding：StructVector 如何实现运行时决策

#### 核心约束

Arrow Java 的 `VectorSchemaRoot` **schema 在创建时确定且不可变**。`StructVector` 的子 Vector 集合在分配时固定，不支持运行时动态添加子列。因此，typed_value 的子字段不能在写入过程中动态增减。

#### 解决方案：Per-Batch Schema + Writer 切换

关键思路：**不同的 Arrow Batch 可以有不同的 Struct 布局**。Shredding 决策不改变表级 schema，而是改变 ArrowWriter 内部的 VectorSchemaRoot 布局。

##### 阶段流转

```
阶段 0 (无 shredding):
  Variant 列 = Struct { metadata: Binary, value: Binary }
  ↓ 客户端统计收集 (VariantStatisticsCollector)
  ↓ 推断触发 (ShreddingSchemaInferrer, 默认采样 1000 行)
  ↓ 获得 shredding schema（具体来源见 3.6 节）
阶段 1 (shredding 激活):
  Variant 列 = Struct { metadata: Binary, value: Binary,
                        typed_value: Struct { name: Struct{value, typed_value}, ... } }
```

##### Writer 端实现

`ArrowWriterPool` 按 `tableId-schemaId-compressionInfo-shreddingSchemaHash` 缓存 writer。当 shredding schema 变化时（首次推断、增加字段），会创建一个**新的 ArrowWriter**，其 VectorSchemaRoot 使用新的 Struct 布局：

```java
// 根据 shredding schema 构造 Variant 列的 Arrow Field
List<Field> variantChildren = new ArrayList<>();
variantChildren.add(Field.nullable("metadata", ArrowType.Binary.INSTANCE));
variantChildren.add(Field.nullable("value", ArrowType.Binary.INSTANCE));

if (shreddingSchema != null && !shreddingSchema.getFields().isEmpty()) {
    List<Field> typedValueChildren = new ArrayList<>();
    for (ShreddedField sf : shreddingSchema.getFields()) {
        typedValueChildren.add(new Field(sf.getFieldPath(),
            FieldType.notNullable(ArrowType.Struct.INSTANCE),
            List.of(
                Field.nullable("value", ArrowType.Binary.INSTANCE),
                Field.nullable("typed_value", toArrowType(sf.getType()))
            )));
    }
    variantChildren.add(new Field("typed_value",
        FieldType.notNullable(ArrowType.Struct.INSTANCE), typedValueChildren));
}

Field variantField = new Field("data",
    FieldType.nullable(ArrowType.Struct.INSTANCE), variantChildren);
```

##### Reader 端实现

Reader 从 Arrow IPC batch 的 **schema 自动发现** typed_value 结构，无需额外元数据：

```java
StructVector variantVector = (StructVector) root.getVector("data");
VarBinaryVector metadata = (VarBinaryVector) variantVector.getChild("metadata");
VarBinaryVector value = (VarBinaryVector) variantVector.getChild("value");

// 检查是否有 typed_value（不同 batch 可能有/没有）
StructVector typedValue = (StructVector) variantVector.getChild("typed_value");
if (typedValue != null) {
    // 发现 shredded 字段，从 typed_value 子列读取
    for (FieldVector child : typedValue.getChildrenFromFields()) {
        StructVector field = (StructVector) child;
        String fieldName = field.getName();
        // field.getChild("typed_value") → 强类型值
        // field.getChild("value") → 兜底 Variant 二进制
    }
} else {
    // 无 shredding，从 value 列反序列化完整 Variant
}
```

##### 新旧 Batch 共存

同一张表的 log 中，不同 batch 可以有不同的 typed_value 布局：

| Batch | 写入时间 | typed_value 布局 |
|-------|---------|------------------|
| Batch 1-100 | 阶段 0 | 无 typed_value |
| Batch 101-200 | 阶段 1 | typed_value: {name, age} |
| Batch 201+ | 阶段 2 (新增字段) | typed_value: {name, age, city} |

Reader 逐 batch 检查 Arrow schema，自适应处理。旧 batch 没有 typed_value → 完全从 value 反序列化。新 batch 有 typed_value → 从强类型列读取。

##### initFieldVector 的适配

`ArrowWriter.initFieldVector()` 需要支持 StructVector 的子列递归初始化：

```java
private void initFieldVector(FieldVector fieldVector) {
    fieldVector.setInitialCapacity(INITIAL_CAPACITY);
    if (fieldVector instanceof BaseFixedWidthVector) {
        ((BaseFixedWidthVector) fieldVector).allocateNew(INITIAL_CAPACITY);
    } else if (fieldVector instanceof BaseVariableWidthVector) {
        ((BaseVariableWidthVector) fieldVector).allocateNew(INITIAL_CAPACITY);
    } else if (fieldVector instanceof ListVector) {
        // ... 现有逻辑
    } else if (fieldVector instanceof StructVector) {
        // 新增：递归初始化 StructVector 的所有子列
        StructVector structVector = (StructVector) fieldVector;
        structVector.allocateNew();
        for (FieldVector child : structVector.getChildrenFromFields()) {
            initFieldVector(child);
        }
    } else {
        fieldVector.allocateNew();
    }
}
```

### 3.5 Arrow 升级说明

Fluss 需从 Arrow Java 15.0.0 升级到 19.0.0，跨 4 个大版本。19.0.0 包含多个 Breaking Changes：

- `BitVectorHelper` API 变更
- `ExtensionTypeWriterFactory` 重构（PR #891 → #892）
- `MetadataAdapter.getAll()` 不再返回 null

升级后可直接使用 `arrow-variant` 模块的 `VariantType` 和 `VariantVector`，但 **shredding（typed_value）需要 Fluss 自行实现**，因为 Arrow Java 19.0.0 尚未支持。

### 3.6 Shredding Schema 的协调策略

Writer 需要知道「该 shred 哪些字段、每个字段什么类型」来构造 StructVector 布局。这个信息即 **shredding schema**。它不是表级 schema（对外 schema 始终不变），而是内部元数据，指导 Writer 如何拆分 Variant 字段。

有两种候选方案：

#### 方案 A：Writer 独立决策（纯客户端）

每个 Writer 独立采样、独立推断、独立决定自己 batch 的 typed_value 布局。不涉及任何 Server 端存储或分发。

```
Writer A:                               Writer B:
  采样 1000 行                           采样 1000 行
  → 推断: {name: STRING, age: BIGINT}    → 推断: {name: STRING, city: STRING}
  → 写入 batch: typed_value={name,age}   → 写入 batch: typed_value={name,city}
```

优势：
- **实现最简单**：无 RPC、无持久化、无多 Writer 协调
- **无单点**：Writer 完全自治，server 故障不影响 shredding 决策
- **启动快**：新 Writer 无需等待 server 返回 shredding schema，采样完即可开始

劣势：
- **batch 布局不统一**：不同 Writer 可能 shred 不同字段，同一字段的 data skipping 统计被分散
- **重复采样**：每个 Writer 重启后都需要重新采样推断，前 1000 行无 shredding
- **Tiering 复杂度**：合并不同布局的 batch 时需要重新组织

#### 方案 B：Server 协调（集中式）

Writer 推断后上报 server，server 合并后统一分发给所有 Writer。Shredding schema 持久化在 server 端。

```
Writer A:                        Server:                          Writer B:
  采样 → 推断 {name, age}         merge({name,age}, {name,city})     采样 → 推断 {name, city}
  → 上报 server   ───────→    → 统一 schema: {name,age,city}   ←───────  上报 server ←
  ← 获取 {name,age,city}       持久化到 table metadata          获取 {name,age,city} →
  → 所有 batch 统一布局                                        所有 batch 统一布局 ←
```

优势：
- **全局统一**：所有 Writer 产出的 batch 布局一致，利于列统计和 Tiering compaction
- **新 Writer 快速启动**：从 server 拉取已有 shredding schema，无需重新采样
- **schema 稳定**：多个 Writer 的统计信息汇聚后推断更准确

劣势：
- **实现复杂度高**：需要 RPC 接口、server 端合并逻辑、持久化存储、变更通知
- **依赖 server 可用性**：server 不可用时，新 Writer 无法获取 shredding schema（需回退无 shredding）
- **协调延迟**：推断完成到全局生效有一个 RPC 往返的窗口期

#### 对比总结

| 方面 | 方案 A：Writer 独立决策 | 方案 B：Server 协调 |
|------|----------------------|------------------|
| 实现复杂度 | 低（纯客户端） | 高（RPC + 持久化 + 通知） |
| batch 布局一致性 | 不一致（各 Writer 独立） | 全局一致 |
| Writer 重启 | 需重新采样（前 N 行无 shredding） | 直接拉取已有 schema |
| Tiering compaction | 需处理异构 batch | batch 布局统一，合并简单 |
| Server 依赖 | 无 | shredding 依赖 server 可用 |
| Reader | 两种方案相同：从 batch schema 自动发现 typed_value | 同左 |

> **注意**：Reader 端两种方案完全相同 —— 都是从每个 batch 的 Arrow schema 自动发现 typed_value 结构，不依赖外部元数据。因此方案选择仅影响 Writer 端。

### 3.7 待确认的设计细节

以下问题在实现时需要明确：

#### metadata 处理策略

当字段被 shred 出去后，residual 的 metadata（字段名字典）如何处理：
- **复用原始 metadata**：不改 metadata，多余字段名白占空间但实现简单
- **重建 metadata**：只保留 residual 涉及的字段名，省空间但多一次编码

#### 完全 shredded 的边界

当一行的所有字段都被 shred（如 `{"name":"alice","age":30}` 且 name/age 都是 shredded 字段）：
- 顶层 `value` 应为 **null**（而非空对象 `{}`），符合 Parquet 规范语义：value=null 且 typed_value=non-null 表示「值完全由 typed_value 表达」

#### Variant 列本身为 NULL

整行的 Variant 列是 SQL NULL（不是 `{}`，而是无值）时，StructVector 的 validity bit 为 0，内部 metadata/value/typed_value 三个子 Vector 的对应行均无意义。Writer 端需正确设置 StructVector 的 null bit。

#### Residual 编码成本

Writer 对每行需要：解析原始 Variant → 提取 shredded 字段 → 重新编码剩余字段为 Variant 二进制。这个「拆一个小字段 + 重建整个对象」的逻行编码有 CPU 开销，需在实现时评估是否可通过零拷贝优化。

#### 下游引擎适配

Flink/Spark Connector 读 Variant 列时，现在拿到的是 StructVector 而非纯 Binary。需确认：
- Connector 层是否需要先把 StructVector 还原为完整 Variant 再传给引擎
- 还是引擎侧的 `variant_get` 等 UDF 能透明地从 StructVector 中提取值

---

## 4. 列裁剪（Column Pruning）方案

Struct 方案下，Arrow 原生支持嵌套列裁剪。有三个层级：

#### Level 1: Variant 列级裁剪

用户不选 `data` 列时，整个 StructVector 不读取（与当前一致）。

```
SELECT id FROM table
→ 只读 id 列，data 的 Struct 完全跳过
```

#### Level 2: Variant 内部子列裁剪（核心优势）

如果用户只需要 `data.name`，可以利用 Arrow 的 Struct 嵌套投影：

```
SELECT id, variant_get(data, '$.name', 'string') FROM table
→ 读 id + data.metadata + data.typed_value.name.typed_value
→ 跳过 data.value（残余）和其他 shredded 字段
```

这正是 Parquet shredding 的核心价值：**只读需要的子列，无需反序列化完整 Variant**。

实现方式：

```java
// 构造 Struct 投影：只读 metadata + typed_value.name
StructVector dataVector = (StructVector) root.getVector("data");
VarBinaryVector metadata = (VarBinaryVector) dataVector.getChild("metadata");
StructVector typedValue = (StructVector) dataVector.getChild("typed_value");
StructVector nameField = (StructVector) typedValue.getChild("name");
VarCharVector nameTypedValue = (VarCharVector) nameField.getChild("typed_value");

// 直接从强类型列读取，零反序列化
String name = new String(nameTypedValue.get(rowIndex), StandardCharsets.UTF_8);
```

#### Level 3: 兜底路径

当查询的字段没有被 shredded，或者类型不匹配时，回退到读取 `value`（残余 Variant 二进制）：

```
SELECT variant_get(data, '$.email', 'string') FROM table
-- email 未被 shred
→ 读 data.metadata + data.value
→ 反序列化 value 提取 email 字段
```

### 4.1 Projection Pushdown 实现思路

```
用户查询: SELECT id, variant_get(data, '$.name') FROM table

1. 查询优化器识别 variant_get 引用的字段路径: $.name
2. 查找 data 列的 shredding schema: {name: STRING, age: BIGINT}
3. name 在 shredded 字段中 → 构造 Struct 子列投影:
   [metadata, typed_value.name.typed_value]
4. 发送投影到 server → server 只返回指定子列
5. Reader 直接从 typed_value.name.typed_value 读取

如果查询的字段不在 shredded 列中:
1. 查询引用 $.email → 不在 shredding schema 中
2. 构造投影: [metadata, value]
3. Reader 从 value 反序列化提取 email
```

---

## 5. 总结

### 5.1 实现路径

```
Task 1: 升级 Arrow Java 15.0.0 → 19.0.0
        - 适配 Breaking Changes
        - 引入 arrow-variant 模块

Task 2: 实现 Struct 内嵌 typed_value（仅标量，仅顶层字段）
        - VariantType → 底层改为 StructVector 存储
        - Writer: 将字段拆分写入 Struct 内部子 Vector（仅标量类型）
        - Reader: 从 Struct 内部子 Vector 合并读取（单层合并）
        - 能力边界: 不支持嵌套对象/数组 shredding

Task 3: 实现 Struct 级别的列裁剪
        - Server 端支持 Struct 子列投影下推
        - 查询优化器生成 variant field → Struct 子列的映射

Task 4（后续）: 扩展 typed_value 能力
        - 支持嵌套对象递归 shredding
        - 支持数组 shredding（ListVector 嵌套）
        - 支持 DECIMAL / BINARY 等扩展类型
        - 支持 shredded 字段降级（移除不再高频的字段）
```

### 5.2 收益

| 收益 | 说明 |
|------|------|
| 对外 schema 不变 | 始终 `(id INT, data VARIANT)`，shredding 完全透明 |
| Shredding 内部完成 | typed_value 的子字段增减在 Struct 内部，不影响表级 schema |
| 细粒度列裁剪 | 只读需要的 shredded 子列，跳过其他 |
| 社区标准对齐 | 符合 `arrow.parquet.variant` 规范，利于与 Parquet 生态互通 |
| Arrow 原生投影 | 复用 Arrow Struct 嵌套投影机制，无需额外投影扩展逻辑 |

### 5.3 Flink Connector 高效对接

#### 问题

Flink 读 shredded Variant 的路径存在三重编解码浪费：

```
ShreddedVariantColumnVector.getVariant(i)
  → mergeVariant(): 把 typed_value 编码回 Variant 二进制 (O(fields))
    → FlussRowToFlinkRowConverter: new BinaryVariant(value, metadata) (byte[] 拷贝)
      → Flink variant_get('$.name'): 重新解析二进制找字段 (O(fields))
```

#### ShreddedVariant 惰性合并

`ShreddedVariant` 继承 `Variant`，携带 per-field typed 数据（Java 对象），仅在调用 `metadata()`/`value()` 时才执行合并：

- `getVariant(i)` 从 Arrow 向量提取字段数据，构造 `ShreddedVariant`
- `hasShreddedField(name)` / `getTypedFieldValue(name)` — O(1) 直接访问
- `ensureMerged()` — 惰性触发，将 shredded 数据编码回完整 Variant 二进制

#### FlussVariant 实现 Flink Variant 接口

`FlussVariant`（在 fluss-flink-2.2 模块）实现 `org.apache.flink.types.variant.Variant` 接口：

```
getField("name")
  → ShreddedVariant.hasShreddedField("name")?
    → YES: getTypedFieldValue → toPrimitiveVariant()   [O(1), 无 merge]
    → NO:  ensureBinaryVariant().getField("name")      [回退路径]
```

- 快速路径：shredded 字段直接从 Java 对象创建 `BinaryVariant`，跳过 `ensureMerged()`
- 慢速路径：非 shredded 字段或完整访问（`toJson()`、嵌套对象）时触发合并

`FlussVariantFactory` 通过反射桥接 `fluss-flink-common`（避免编译期依赖 Flink 2.2 API）。

#### Converter 集成

`FlussRowToFlinkRowConverter` 的 VARIANT 分支优先使用 `FlussVariant`：

1. 尝试反射加载 `FlussVariantFactory`（Flink 2.2+ 可用）
2. 回退到 `BinaryVariant`（Flink 2.1+）

#### Variant 子列投影 API

Scan API 新增 `variantFieldProjection(Map<Integer, List<String>>)` 方法，指定 Variant 列的子字段投影：

```java
table.newScan()
    .project(projectedFields)
    .variantFieldProjection(columnToFields)  // columnIndex → field names
    .createLogScanner();
```

数据流路径：`Scan` → `TableScan` → `LogScannerImpl` → `LogFetcher` → `PbFetchLogReqForTable`（含 `PbVariantFieldProjection`）→ Server `FetchParams` → `FileLogProjection`

初始版本 `variantFieldHints` 为 null（不做子列投影），后续由 Flink planner rule 或 table properties 激活。

#### 优化前后数据路径对比

| | 优化前 | 优化后 |
|---|---|---|
| `getVariant(i)` | 立即 merge → `Variant(metadata, value)` | 构造 `ShreddedVariant`（惰性） |
| Converter | `new BinaryVariant(value, metadata)` | `new FlussVariant(shreddedVariant)` |
| Flink `variant_get('$.name')` | 解析二进制 O(fields) | `hasShreddedField` → O(1) 直接读取 |
| 编解码次数 | 3 次 | 0 次（shredded 字段） |
