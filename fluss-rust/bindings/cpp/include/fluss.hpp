/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <chrono>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

// Forward declare Arrow classes to avoid including heavy Arrow headers in header
namespace arrow {
class RecordBatch;
class Schema;
class DataType;
}

namespace fluss {

namespace ffi {
struct Connection;
struct Admin;
struct Table;
struct AppendWriter;
struct WriteResult;
struct LogScanner;
struct BatchScanner;
struct UpsertWriter;
struct Lookuper;
struct PrefixLookuper;
struct ScanResultInner;
struct GenericRowInner;
struct LookupResultInner;
struct PrefixLookupResultInner;
struct ArrayWriterInner;
struct MapWriterInner;
struct ValueInner;
}  // namespace ffi

/// Named constants for Fluss API error codes.
///
/// Server API errors have error_code > 0 or == -1.
/// Client-side errors have error_code == CLIENT_ERROR (-2).
/// These constants match the Rust core FlussError enum and are stable across protocol versions.
/// New server error codes work automatically (error_code is a raw int, not a closed enum) —
/// these constants are convenience names, not an exhaustive list.
struct ErrorCode {
    /// Client-side error (not from server API protocol). Check error_message for details.
    static constexpr int CLIENT_ERROR = -2;
    /// No error.
    static constexpr int NONE = 0;
    /// The server experienced an unexpected error when processing the request.
    static constexpr int UNKNOWN_SERVER_ERROR = -1;
    /// The server disconnected before a response was received.
    static constexpr int NETWORK_EXCEPTION = 1;
    /// The version of API is not supported.
    static constexpr int UNSUPPORTED_VERSION = 2;
    /// This message has failed its CRC checksum, exceeds the valid size, or is otherwise corrupt.
    static constexpr int CORRUPT_MESSAGE = 3;
    /// The database does not exist.
    static constexpr int DATABASE_NOT_EXIST = 4;
    /// The database is not empty.
    static constexpr int DATABASE_NOT_EMPTY = 5;
    /// The database already exists.
    static constexpr int DATABASE_ALREADY_EXIST = 6;
    /// The table does not exist.
    static constexpr int TABLE_NOT_EXIST = 7;
    /// The table already exists.
    static constexpr int TABLE_ALREADY_EXIST = 8;
    /// The schema does not exist.
    static constexpr int SCHEMA_NOT_EXIST = 9;
    /// Exception occurred while storing data for log in server.
    static constexpr int LOG_STORAGE_EXCEPTION = 10;
    /// Exception occurred while storing data for kv in server.
    static constexpr int KV_STORAGE_EXCEPTION = 11;
    /// Not leader or follower.
    static constexpr int NOT_LEADER_OR_FOLLOWER = 12;
    /// The record is too large.
    static constexpr int RECORD_TOO_LARGE_EXCEPTION = 13;
    /// The record is corrupt.
    static constexpr int CORRUPT_RECORD_EXCEPTION = 14;
    /// The client has attempted to perform an operation on an invalid table.
    static constexpr int INVALID_TABLE_EXCEPTION = 15;
    /// The client has attempted to perform an operation on an invalid database.
    static constexpr int INVALID_DATABASE_EXCEPTION = 16;
    /// The replication factor is larger than the number of available tablet servers.
    static constexpr int INVALID_REPLICATION_FACTOR = 17;
    /// Produce request specified an invalid value for required acks.
    static constexpr int INVALID_REQUIRED_ACKS = 18;
    /// The log offset is out of range.
    static constexpr int LOG_OFFSET_OUT_OF_RANGE_EXCEPTION = 19;
    /// The table is not a primary key table.
    static constexpr int NON_PRIMARY_KEY_TABLE_EXCEPTION = 20;
    /// The table or bucket does not exist.
    static constexpr int UNKNOWN_TABLE_OR_BUCKET_EXCEPTION = 21;
    /// The update version is invalid.
    static constexpr int INVALID_UPDATE_VERSION_EXCEPTION = 22;
    /// The coordinator is invalid.
    static constexpr int INVALID_COORDINATOR_EXCEPTION = 23;
    /// The leader epoch is invalid.
    static constexpr int FENCED_LEADER_EPOCH_EXCEPTION = 24;
    /// The request timed out.
    static constexpr int REQUEST_TIME_OUT = 25;
    /// The general storage exception.
    static constexpr int STORAGE_EXCEPTION = 26;
    /// The server did not attempt to execute this operation.
    static constexpr int OPERATION_NOT_ATTEMPTED_EXCEPTION = 27;
    /// Records are written to the server already, but to fewer in-sync replicas than required.
    static constexpr int NOT_ENOUGH_REPLICAS_AFTER_APPEND_EXCEPTION = 28;
    /// Messages are rejected since there are fewer in-sync replicas than required.
    static constexpr int NOT_ENOUGH_REPLICAS_EXCEPTION = 29;
    /// Get file access security token exception.
    static constexpr int SECURITY_TOKEN_EXCEPTION = 30;
    /// The tablet server received an out of order sequence batch.
    static constexpr int OUT_OF_ORDER_SEQUENCE_EXCEPTION = 31;
    /// The tablet server received a duplicate sequence batch.
    static constexpr int DUPLICATE_SEQUENCE_EXCEPTION = 32;
    /// The tablet server could not locate the writer metadata.
    static constexpr int UNKNOWN_WRITER_ID_EXCEPTION = 33;
    /// The requested column projection is invalid.
    static constexpr int INVALID_COLUMN_PROJECTION = 34;
    /// The requested target column to write is invalid.
    static constexpr int INVALID_TARGET_COLUMN = 35;
    /// The partition does not exist.
    static constexpr int PARTITION_NOT_EXISTS = 36;
    /// The table is not partitioned.
    static constexpr int TABLE_NOT_PARTITIONED_EXCEPTION = 37;
    /// The timestamp is invalid.
    static constexpr int INVALID_TIMESTAMP_EXCEPTION = 38;
    /// The config is invalid.
    static constexpr int INVALID_CONFIG_EXCEPTION = 39;
    /// The lake storage is not configured.
    static constexpr int LAKE_STORAGE_NOT_CONFIGURED_EXCEPTION = 40;
    /// The kv snapshot does not exist.
    static constexpr int KV_SNAPSHOT_NOT_EXIST = 41;
    /// The partition already exists.
    static constexpr int PARTITION_ALREADY_EXISTS = 42;
    /// The partition spec is invalid.
    static constexpr int PARTITION_SPEC_INVALID_EXCEPTION = 43;
    /// There is no currently available leader for the given partition.
    static constexpr int LEADER_NOT_AVAILABLE_EXCEPTION = 44;
    /// Exceed the maximum number of partitions.
    static constexpr int PARTITION_MAX_NUM_EXCEPTION = 45;
    /// Authentication failed.
    static constexpr int AUTHENTICATE_EXCEPTION = 46;
    /// Security is disabled.
    static constexpr int SECURITY_DISABLED_EXCEPTION = 47;
    /// Authorization failed.
    static constexpr int AUTHORIZATION_EXCEPTION = 48;
    /// Exceed the maximum number of buckets.
    static constexpr int BUCKET_MAX_NUM_EXCEPTION = 49;
    /// The tiering epoch is invalid.
    static constexpr int FENCED_TIERING_EPOCH_EXCEPTION = 50;
    /// Authentication failed with retriable exception.
    static constexpr int RETRIABLE_AUTHENTICATE_EXCEPTION = 51;
    /// The server rack info is invalid.
    static constexpr int INVALID_SERVER_RACK_INFO_EXCEPTION = 52;
    /// The lake snapshot does not exist.
    static constexpr int LAKE_SNAPSHOT_NOT_EXIST = 53;
    /// The lake table already exists.
    static constexpr int LAKE_TABLE_ALREADY_EXIST = 54;
    /// The new ISR contains at least one ineligible replica.
    static constexpr int INELIGIBLE_REPLICA_EXCEPTION = 55;
    /// The alter table is invalid.
    static constexpr int INVALID_ALTER_TABLE_EXCEPTION = 56;
    /// Deletion operations are disabled on this table.
    static constexpr int DELETION_DISABLED_EXCEPTION = 57;

    /// Returns true if retrying the request may succeed. Mirrors Java's RetriableException hierarchy.
    static constexpr bool IsRetriable(int32_t code) {
        return code == NETWORK_EXCEPTION || code == CORRUPT_MESSAGE ||
               code == SCHEMA_NOT_EXIST || code == LOG_STORAGE_EXCEPTION ||
               code == KV_STORAGE_EXCEPTION || code == NOT_LEADER_OR_FOLLOWER ||
               code == CORRUPT_RECORD_EXCEPTION ||
               code == UNKNOWN_TABLE_OR_BUCKET_EXCEPTION || code == REQUEST_TIME_OUT ||
               code == STORAGE_EXCEPTION ||
               code == NOT_ENOUGH_REPLICAS_AFTER_APPEND_EXCEPTION ||
               code == NOT_ENOUGH_REPLICAS_EXCEPTION || code == LEADER_NOT_AVAILABLE_EXCEPTION;
    }
};

struct Date {
    int32_t days_since_epoch{0};

    static Date FromDays(int32_t days) { return {days}; }
    static Date FromYMD(int year, int month, int day);

    int Year() const;
    int Month() const;
    int Day() const;
};

struct Time {
    static constexpr int32_t kMillisPerSecond = 1000;
    static constexpr int32_t kMillisPerMinute = 60 * kMillisPerSecond;
    static constexpr int32_t kMillisPerHour = 60 * kMillisPerMinute;

    int32_t millis_since_midnight{0};

    static Time FromMillis(int32_t ms) { return {ms}; }
    static Time FromHMS(int hour, int minute, int second, int millis = 0) {
        return {hour * kMillisPerHour + minute * kMillisPerMinute + second * kMillisPerSecond +
                millis};
    }

    int Hour() const { return millis_since_midnight / kMillisPerHour; }
    int Minute() const { return (millis_since_midnight % kMillisPerHour) / kMillisPerMinute; }
    int Second() const { return (millis_since_midnight % kMillisPerMinute) / kMillisPerSecond; }
    int Millis() const { return millis_since_midnight % kMillisPerSecond; }
};

struct Timestamp {
    static constexpr int32_t kMaxNanoOfMillisecond = 999999;
    static constexpr int64_t kNanosPerMilli = 1000000;

    int64_t epoch_millis{0};
    int32_t nano_of_millisecond{0};

    static Timestamp FromMillis(int64_t ms) { return {ms, 0}; }
    static Timestamp FromMillisNanos(int64_t ms, int32_t nanos) {
        if (nanos < 0) nanos = 0;
        if (nanos > kMaxNanoOfMillisecond) nanos = kMaxNanoOfMillisecond;
        return {ms, nanos};
    }
    static Timestamp FromTimePoint(std::chrono::system_clock::time_point tp) {
        auto duration = tp.time_since_epoch();
        auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
        auto ms = ns / kNanosPerMilli;
        auto nano_of_ms = static_cast<int32_t>(ns % kNanosPerMilli);
        if (nano_of_ms < 0) {
            nano_of_ms += kNanosPerMilli;
            ms -= 1;
        }
        return {ms, nano_of_ms};
    }
};

enum class ChangeType {
    AppendOnly = 0,
    Insert = 1,
    UpdateBefore = 2,
    UpdateAfter = 3,
    Delete = 4,
};

enum class TypeId {
    Unknown = 0,
    Boolean = 1,
    TinyInt = 2,
    SmallInt = 3,
    Int = 4,
    BigInt = 5,
    Float = 6,
    Double = 7,
    String = 8,
    Bytes = 9,
    Date = 10,
    Time = 11,
    Timestamp = 12,
    TimestampLtz = 13,
    Decimal = 14,
    Char = 15,
    Binary = 16,
    Array = 17,
    Map = 18,
    Row = 19,
};

class DataType {
   public:
    explicit DataType(TypeId id, int32_t p = 0, int32_t s = 0, bool nullable = true)
        : id_(id), precision_(p), scale_(s), nullable_(nullable) {}

    static DataType Boolean() { return DataType(TypeId::Boolean); }
    static DataType TinyInt() { return DataType(TypeId::TinyInt); }
    static DataType SmallInt() { return DataType(TypeId::SmallInt); }
    static DataType Int() { return DataType(TypeId::Int); }
    static DataType BigInt() { return DataType(TypeId::BigInt); }
    static DataType Float() { return DataType(TypeId::Float); }
    static DataType Double() { return DataType(TypeId::Double); }
    static DataType String() { return DataType(TypeId::String); }
    static DataType Bytes() { return DataType(TypeId::Bytes); }
    static DataType Date() { return DataType(TypeId::Date); }
    static DataType Time(int32_t precision = 0) { return DataType(TypeId::Time, precision, 0); }
    static DataType Timestamp(int32_t precision = 6) {
        return DataType(TypeId::Timestamp, precision, 0);
    }
    static DataType TimestampLtz(int32_t precision = 6) {
        return DataType(TypeId::TimestampLtz, precision, 0);
    }
    static DataType Decimal(int32_t precision, int32_t scale) {
        return DataType(TypeId::Decimal, precision, scale);
    }
    static DataType Char(int32_t length) { return DataType(TypeId::Char, length, 0); }
    static DataType Binary(int32_t length) { return DataType(TypeId::Binary, length, 0); }
    /// Constructs an `ARRAY<element>` type. The element DataType (possibly
    /// itself an array) is deep-copied into a shared owning handle so that
    /// copies of the outer DataType remain cheap while the element lives
    /// as long as any reference exists.
    static DataType Array(DataType element) {
        DataType dt(TypeId::Array, 0, 0);
        dt.element_type_ = std::make_shared<DataType>(std::move(element));
        return dt;
    }
    /// Constructs a `MAP<key, value>` type. Either side may itself be complex.
    static DataType Map(DataType key, DataType value);
    /// Constructs a `ROW<name: type, ...>` type from `{name, type}` fields.
    static DataType Row(std::vector<std::pair<std::string, DataType>> fields);

    TypeId id() const { return id_; }
    int32_t precision() const { return precision_; }
    int32_t scale() const { return scale_; }
    bool nullable() const { return nullable_; }
    /// Returns the element type of an ARRAY. Returns `nullptr` for non-array
    /// types. The returned pointer is valid as long as this DataType (or a
    /// copy holding the same shared element) is alive.
    const DataType* element_type() const { return element_type_.get(); }
    /// MAP key / value types. Return `nullptr` for non-MAP types.
    const DataType* key_type() const { return key_type_.get(); }
    const DataType* value_type() const { return value_type_.get(); }
    /// ROW fields (empty for non-ROW types).
    size_t field_count() const { return row_field_types_.size(); }
    const std::string& field_name(size_t i) const { return row_field_names_.at(i); }
    const DataType* field_type(size_t i) const { return row_field_types_.at(i).get(); }

    /// Returns a copy of this DataType with nullable set to false.
    DataType NotNull() const {
        DataType dt(id_, precision_, scale_, false);
        dt.element_type_ = element_type_;
        dt.key_type_ = key_type_;
        dt.value_type_ = value_type_;
        dt.row_field_names_ = row_field_names_;
        dt.row_field_types_ = row_field_types_;
        return dt;
    }

   private:
    TypeId id_;
    int32_t precision_{0};
    int32_t scale_{0};
    bool nullable_{true};
    std::shared_ptr<DataType> element_type_;
    std::shared_ptr<DataType> key_type_;
    std::shared_ptr<DataType> value_type_;
    std::vector<std::string> row_field_names_;
    std::vector<std::shared_ptr<DataType>> row_field_types_;
};

inline DataType DataType::Map(DataType key, DataType value) {
    DataType dt(TypeId::Map, 0, 0);
    dt.key_type_ = std::make_shared<DataType>(std::move(key));
    dt.value_type_ = std::make_shared<DataType>(std::move(value));
    return dt;
}

inline DataType DataType::Row(std::vector<std::pair<std::string, DataType>> fields) {
    DataType dt(TypeId::Row, 0, 0);
    dt.row_field_names_.reserve(fields.size());
    dt.row_field_types_.reserve(fields.size());
    for (auto& f : fields) {
        dt.row_field_names_.push_back(std::move(f.first));
        dt.row_field_types_.push_back(std::make_shared<DataType>(std::move(f.second)));
    }
    return dt;
}

constexpr int64_t EARLIEST_OFFSET = -2;

enum class OffsetType {
    Earliest = 0,
    Latest = 1,
    Timestamp = 2,
};

struct OffsetSpec {
    OffsetType type;
    int64_t timestamp{0};

    static OffsetSpec Earliest() { return {OffsetType::Earliest, 0}; }
    static OffsetSpec Latest() { return {OffsetType::Latest, 0}; }
    static OffsetSpec Timestamp(int64_t ts) { return {OffsetType::Timestamp, ts}; }
};

struct Result {
    int32_t error_code{0};
    std::string error_message;

    bool Ok() const { return error_code == 0; }

    /// Returns true if retrying the request may succeed. Client-side errors always return false.
    bool IsRetriable() const { return ErrorCode::IsRetriable(error_code); }
};

struct TablePath {
    std::string database_name;
    std::string table_name;

    TablePath() = default;
    TablePath(std::string db, std::string tbl)
        : database_name(std::move(db)), table_name(std::move(tbl)) {}

    std::string ToString() const { return database_name + "." + table_name; }
};

struct Column {
    std::string name;
    DataType data_type;
    std::string comment;
};

struct Schema {
    std::vector<Column> columns;
    std::vector<std::string> primary_keys;

    class Builder {
       public:
        Builder& AddColumn(std::string name, DataType type, std::string comment = "") {
            columns_.push_back({std::move(name), std::move(type), std::move(comment)});
            return *this;
        }

        Builder& SetPrimaryKeys(std::vector<std::string> keys) {
            primary_keys_ = std::move(keys);
            return *this;
        }

        Schema Build() { return Schema{std::move(columns_), std::move(primary_keys_)}; }

       private:
        std::vector<Column> columns_;
        std::vector<std::string> primary_keys_;
    };

    static Builder NewBuilder() { return Builder(); }
};

struct TableDescriptor {
    Schema schema;
    std::vector<std::string> partition_keys;
    int32_t bucket_count{0};
    std::vector<std::string> bucket_keys;
    std::unordered_map<std::string, std::string> properties;
    std::unordered_map<std::string, std::string> custom_properties;
    std::string comment;

    class Builder {
       public:
        Builder& SetSchema(Schema s) {
            schema_ = std::move(s);
            return *this;
        }

        Builder& SetPartitionKeys(std::vector<std::string> keys) {
            partition_keys_ = std::move(keys);
            return *this;
        }

        Builder& SetBucketCount(int32_t count) {
            bucket_count_ = count;
            return *this;
        }

        Builder& SetBucketKeys(std::vector<std::string> keys) {
            bucket_keys_ = std::move(keys);
            return *this;
        }

        Builder& SetProperty(std::string key, std::string value) {
            properties_[std::move(key)] = std::move(value);
            return *this;
        }

        Builder& SetCustomProperty(std::string key, std::string value) {
            custom_properties_[std::move(key)] = std::move(value);
            return *this;
        }

        Builder& SetLogFormat(std::string format) {
            return SetProperty("table.log.format", std::move(format));
        }

        Builder& SetKvFormat(std::string format) {
            return SetProperty("table.kv.format", std::move(format));
        }

        Builder& SetComment(std::string comment) {
            comment_ = std::move(comment);
            return *this;
        }

        TableDescriptor Build() {
            return TableDescriptor{std::move(schema_),     std::move(partition_keys_),
                                   bucket_count_,          std::move(bucket_keys_),
                                   std::move(properties_), std::move(custom_properties_),
                                   std::move(comment_)};
        }

       private:
        Schema schema_;
        std::vector<std::string> partition_keys_;
        int32_t bucket_count_{0};
        std::vector<std::string> bucket_keys_;
        std::unordered_map<std::string, std::string> properties_;
        std::unordered_map<std::string, std::string> custom_properties_;
        std::string comment_;
    };

    static Builder NewBuilder() { return Builder(); }
};

struct TableInfo {
    int64_t table_id;
    int32_t schema_id;
    TablePath table_path;
    int64_t created_time;
    int64_t modified_time;
    std::vector<std::string> primary_keys;
    std::vector<std::string> bucket_keys;
    std::vector<std::string> partition_keys;
    bool has_partition_expressions = false;
    int32_t num_buckets;
    bool has_primary_key;
    bool is_partitioned;
    std::unordered_map<std::string, std::string> properties;
    std::unordered_map<std::string, std::string> custom_properties;
    std::string comment;
    Schema schema;
};

namespace detail {
struct ColumnInfo {
    size_t index;
    TypeId type_id;
};
using ColumnMap = std::unordered_map<std::string, ColumnInfo>;

inline size_t ResolveColumn(const ColumnMap& map, const std::string& name) {
    auto it = map.find(name);
    if (it == map.end()) {
        throw std::runtime_error("Unknown column '" + name + "'");
    }
    return it->second.index;
}

}  // namespace detail
class Value;
class GenericRow;
class ArrayWriter;
class MapWriter;
namespace detail {

/// CRTP mixin that adds name-based getters to any class with index-based getters.
/// Derived must provide: `size_t Resolve(const std::string&) const`
/// and all the index-based getters (IsNull(idx), GetBool(idx), etc.).
template <typename Derived>
struct NamedGetters {
    bool IsNull(const std::string& n) const { return Self().IsNull(Self().Resolve(n)); }
    bool GetBool(const std::string& n) const { return Self().GetBool(Self().Resolve(n)); }
    int32_t GetInt32(const std::string& n) const { return Self().GetInt32(Self().Resolve(n)); }
    int64_t GetInt64(const std::string& n) const { return Self().GetInt64(Self().Resolve(n)); }
    float GetFloat32(const std::string& n) const { return Self().GetFloat32(Self().Resolve(n)); }
    double GetFloat64(const std::string& n) const { return Self().GetFloat64(Self().Resolve(n)); }
    std::string_view GetString(const std::string& n) const {
        return Self().GetString(Self().Resolve(n));
    }
    std::pair<const uint8_t*, size_t> GetBytes(const std::string& n) const {
        return Self().GetBytes(Self().Resolve(n));
    }
    fluss::Date GetDate(const std::string& n) const { return Self().GetDate(Self().Resolve(n)); }
    fluss::Time GetTime(const std::string& n) const { return Self().GetTime(Self().Resolve(n)); }
    fluss::Timestamp GetTimestamp(const std::string& n) const {
        return Self().GetTimestamp(Self().Resolve(n));
    }
    std::string GetDecimalString(const std::string& n) const {
        return Self().GetDecimalString(Self().Resolve(n));
    }

   private:
    const Derived& Self() const { return static_cast<const Derived&>(*this); }
};

struct ScanData {
    ffi::ScanResultInner* raw;
    ColumnMap columns;

    ScanData(ffi::ScanResultInner* r, ColumnMap cols) : raw(r), columns(std::move(cols)) {}
    ~ScanData();

    ScanData(const ScanData&) = delete;
    ScanData& operator=(const ScanData&) = delete;
};

/// Backing store for a prefix lookup result; mirrors ScanData.
struct PrefixData {
    ffi::PrefixLookupResultInner* raw;
    ColumnMap columns;

    PrefixData(ffi::PrefixLookupResultInner* r, ColumnMap cols)
        : raw(r), columns(std::move(cols)) {}
    ~PrefixData();

    PrefixData(const PrefixData&) = delete;
    PrefixData& operator=(const PrefixData&) = delete;
};
}  // namespace detail

/// One recursive handle for reading a complex (ARRAY / MAP / ROW) column value.
/// `Navigate` with At/KeyAt/ValueAt/Field — each returns a child `Value`; read a
/// leaf with the Get* methods (no index — the handle points at one value).
/// Obtained from LookupResult::GetValue() / RowView::GetValue(). Move-only;
/// owns an opaque Rust handle released on destruction.
class Value {
   public:
    ~Value() noexcept;
    Value(const Value&) = delete;
    Value& operator=(const Value&) = delete;
    Value(Value&& other) noexcept;
    Value& operator=(Value&& other) noexcept;

    TypeId Type() const noexcept;
    bool IsNull() const noexcept;

    // ── Leaf reads ──
    bool GetBool() const;
    int32_t GetInt32() const;
    int64_t GetInt64() const;
    float GetFloat32() const;
    double GetFloat64() const;
    std::string GetString() const;
    std::vector<uint8_t> GetBytes() const;
    fluss::Date GetDate() const;
    fluss::Time GetTime() const;
    fluss::Timestamp GetTimestamp() const;
    std::string GetDecimalString() const;

    // ── Navigation ──
    size_t Size() const;             // ARRAY / MAP entry count
    size_t FieldCount() const;       // ROW
    Value At(size_t i) const;        // ARRAY element
    Value KeyAt(size_t i) const;     // MAP key
    Value ValueAt(size_t i) const;   // MAP value
    Value Field(size_t i) const;     // ROW field
    Value Field(const std::string& name) const;  // ROW field by name

   private:
    friend class LookupResult;
    friend class RowView;
    friend class PrefixRowView;
    explicit Value(ffi::ValueInner* inner) : inner_(inner) {}
    void Destroy() noexcept;
    ffi::ValueInner* inner_{nullptr};
};

class ArrayWriter {
   public:
    ArrayWriter(size_t size, DataType element_type);
    /// Builds an array whose element is a ROW / MAP (which DataType can't
    /// express). Pass the element type as an Arrow type, e.g.
    /// `arrow::struct_({...})` or `arrow::map(...)`.
    ArrayWriter(size_t size, std::shared_ptr<arrow::DataType> element_type);
    ~ArrayWriter() noexcept;

    ArrayWriter(const ArrayWriter&) = delete;
    ArrayWriter& operator=(const ArrayWriter&) = delete;
    ArrayWriter(ArrayWriter&& other) noexcept;
    ArrayWriter& operator=(ArrayWriter&& other) noexcept;

    bool Available() const;
    size_t Size() const noexcept;

    void SetNull(size_t idx);
    void SetBool(size_t idx, bool v);
    void SetInt32(size_t idx, int32_t v);
    void SetInt64(size_t idx, int64_t v);
    void SetFloat32(size_t idx, float v);
    void SetFloat64(size_t idx, double v);
    void SetString(size_t idx, const std::string& v);
    void SetBytes(size_t idx, const std::vector<uint8_t>& v);
    void SetDate(size_t idx, fluss::Date d);
    void SetTime(size_t idx, fluss::Time t);
    void SetTimestampNtz(size_t idx, fluss::Timestamp ts);
    void SetTimestampLtz(size_t idx, fluss::Timestamp ts);
    void SetDecimal(size_t idx, const std::string& value);
    void SetArray(size_t idx, ArrayWriter&& nested);
    /// Sets a ROW / MAP element. The nested row/map is consumed (moved-from).
    void SetRow(size_t idx, GenericRow&& row);
    void SetMap(size_t idx, MapWriter&& map);

   private:
    friend class GenericRow;
    friend class MapWriter;
    void Destroy() noexcept;
    ffi::ArrayWriterInner* inner_{nullptr};
    DataType element_type_;
};

/// Builder for a MAP column value. Construct with the key/value types, then for
/// each entry set the key and value and call Commit(). Keys cannot be null.
/// Move-only; consumed by GenericRow::SetMap / Set(name, MapWriter&&).
class MapWriter {
   public:
    MapWriter(size_t capacity, DataType key_type, DataType value_type);
    /// Builds a map whose key/value is a ROW / MAP / ARRAY (which DataType
    /// can't express). Pass the key and value types as Arrow types.
    MapWriter(size_t capacity, std::shared_ptr<arrow::DataType> key_type,
              std::shared_ptr<arrow::DataType> value_type);
    ~MapWriter() noexcept;

    MapWriter(const MapWriter&) = delete;
    MapWriter& operator=(const MapWriter&) = delete;
    MapWriter(MapWriter&& other) noexcept;
    MapWriter& operator=(MapWriter&& other) noexcept;

    bool Available() const;

    // ── Key setters ──────────────────────────────────────────────────────
    void SetKeyBool(bool k);
    void SetKeyInt32(int32_t k);
    void SetKeyInt64(int64_t k);
    void SetKeyFloat32(float k);
    void SetKeyFloat64(double k);
    void SetKeyString(const std::string& k);
    void SetKeyBytes(const std::vector<uint8_t>& k);
    void SetKeyDate(fluss::Date k);
    void SetKeyTime(fluss::Time k);
    /// NTZ vs LTZ is chosen from the map's declared key type.
    void SetKeyTimestamp(fluss::Timestamp k);
    void SetKeyDecimal(const std::string& k);

    // ── Value setters ──────────────────────────────────────────────────────
    void SetValueNull();
    void SetValueBool(bool v);
    void SetValueInt32(int32_t v);
    void SetValueInt64(int64_t v);
    void SetValueFloat32(float v);
    void SetValueFloat64(double v);
    void SetValueString(const std::string& v);
    void SetValueBytes(const std::vector<uint8_t>& v);
    void SetValueDate(fluss::Date v);
    void SetValueTime(fluss::Time v);
    /// NTZ vs LTZ is chosen from the map's declared value type.
    void SetValueTimestamp(fluss::Timestamp v);
    void SetValueDecimal(const std::string& v);
    // Compound values: the writer/row is consumed (moved-from).
    void SetValueRow(GenericRow&& v);
    void SetValueMap(MapWriter&& v);
    void SetValueArray(ArrayWriter&& v);

    void Commit();

   private:
    friend class GenericRow;
    friend class ArrayWriter;
    void Destroy() noexcept;
    ffi::MapWriterInner* inner_{nullptr};
    DataType key_type_;
    DataType value_type_;
};

class GenericRow {
   public:
    GenericRow();
    explicit GenericRow(size_t field_count);
    ~GenericRow() noexcept;

    GenericRow(const GenericRow&) = delete;
    GenericRow& operator=(const GenericRow&) = delete;
    GenericRow(GenericRow&& other) noexcept;
    GenericRow& operator=(GenericRow&& other) noexcept;

    bool Available() const;
    void Reset();

    // ── Index-based setters ──────────────────────────────────────────
    void SetNull(size_t idx);
    void SetBool(size_t idx, bool v);
    void SetInt32(size_t idx, int32_t v);
    void SetInt64(size_t idx, int64_t v);
    void SetFloat32(size_t idx, float v);
    void SetFloat64(size_t idx, double v);
    void SetString(size_t idx, std::string v);
    void SetBytes(size_t idx, std::vector<uint8_t> v);
    void SetDate(size_t idx, fluss::Date d);
    void SetTime(size_t idx, fluss::Time t);
    void SetTimestampNtz(size_t idx, fluss::Timestamp ts);
    void SetTimestampLtz(size_t idx, fluss::Timestamp ts);
    void SetDecimal(size_t idx, const std::string& value);
    void SetArray(size_t idx, ArrayWriter&& writer);
    void SetMap(size_t idx, MapWriter&& writer);
    /// Sets a ROW-typed field from a nested row built with GenericRow. The
    /// nested row is consumed (moved-from) by this call.
    void SetRow(size_t idx, GenericRow&& nested);

    // ── Name-based setters (require schema — see Table::NewRow()) ───
    void Set(const std::string& name, std::nullptr_t) { SetNull(Resolve(name)); }
    void Set(const std::string& name, bool v) { SetBool(Resolve(name), v); }
    void Set(const std::string& name, int32_t v) { SetInt32(Resolve(name), v); }
    void Set(const std::string& name, int64_t v) { SetInt64(Resolve(name), v); }
    void Set(const std::string& name, float v) { SetFloat32(Resolve(name), v); }
    void Set(const std::string& name, double v) { SetFloat64(Resolve(name), v); }
    // const char* overload to prevent "string literal" -> bool conversion
    void Set(const std::string& name, const char* v) {
        auto [idx, type] = ResolveColumn(name);
        if (type == TypeId::Decimal) {
            SetDecimal(idx, v);
        } else if (type == TypeId::String) {
            SetString(idx, v);
        } else {
            throw std::runtime_error("GenericRow::Set: column '" + name +
                                     "' is not a string or decimal column");
        }
    }
    void Set(const std::string& name, std::string v) {
        auto [idx, type] = ResolveColumn(name);
        if (type == TypeId::Decimal) {
            SetDecimal(idx, v);
        } else if (type == TypeId::String) {
            SetString(idx, std::move(v));
        } else {
            throw std::runtime_error("GenericRow::Set: column '" + name +
                                     "' is not a string or decimal column");
        }
    }
    void Set(const std::string& name, std::vector<uint8_t> v) {
        SetBytes(Resolve(name), std::move(v));
    }
    void Set(const std::string& name, fluss::Date d) { SetDate(Resolve(name), d); }
    void Set(const std::string& name, fluss::Time t) { SetTime(Resolve(name), t); }
    void Set(const std::string& name, fluss::Timestamp ts) {
        auto [idx, type] = ResolveColumn(name);
        if (type == TypeId::TimestampLtz) {
            SetTimestampLtz(idx, ts);
        } else if (type == TypeId::Timestamp) {
            SetTimestampNtz(idx, ts);
        } else {
            throw std::runtime_error("GenericRow::Set: column '" + name +
                                     "' is not a timestamp column");
        }
    }
    void Set(const std::string& name, ArrayWriter&& writer) { SetArray(Resolve(name), std::move(writer)); }
    void Set(const std::string& name, MapWriter&& writer) { SetMap(Resolve(name), std::move(writer)); }
    void Set(const std::string& name, GenericRow&& nested) { SetRow(Resolve(name), std::move(nested)); }

   private:
    friend class Table;
    friend class AppendWriter;
    friend class UpsertWriter;
    friend class Lookuper;
    friend class PrefixLookuper;
    friend class ArrayWriter;
    friend class MapWriter;

    using ColumnInfo = detail::ColumnInfo;
    using ColumnMap = detail::ColumnMap;

    size_t Resolve(const std::string& name) const { return ResolveColumn(name).index; }

    const ColumnInfo& ResolveColumn(const std::string& name) const {
        if (!column_map_) {
            throw std::runtime_error(
                "GenericRow: name-based Set() requires a schema. "
                "Use Table::NewRow() to create a schema-aware row.");
        }
        auto it = column_map_->find(name);
        if (it == column_map_->end()) {
            throw std::runtime_error("GenericRow: unknown column '" + name + "'");
        }
        return it->second;
    }

    void Destroy() noexcept;

    ffi::GenericRowInner* inner_{nullptr};
    std::shared_ptr<ColumnMap> column_map_;
};

/// Read-only row view for scan results. Zero-copy access to string and bytes data.
///
/// RowView shares ownership of the underlying scan data via reference counting,
/// so it can safely outlive the ScanRecords that produced it.
class RowView : public detail::NamedGetters<RowView> {
    friend struct detail::NamedGetters<RowView>;

   public:
    RowView(std::shared_ptr<const detail::ScanData> data, size_t bucket_idx, size_t rec_idx)
        : data_(std::move(data)), bucket_idx_(bucket_idx), rec_idx_(rec_idx) {}

    // ── Index-based getters ──────────────────────────────────────────
    size_t FieldCount() const;
    TypeId GetType(size_t idx) const;
    bool IsNull(size_t idx) const;
    bool GetBool(size_t idx) const;
    int32_t GetInt32(size_t idx) const;
    int64_t GetInt64(size_t idx) const;
    float GetFloat32(size_t idx) const;
    double GetFloat64(size_t idx) const;
    std::string_view GetString(size_t idx) const;
    std::pair<const uint8_t*, size_t> GetBytes(size_t idx) const;
    fluss::Date GetDate(size_t idx) const;
    fluss::Time GetTime(size_t idx) const;
    fluss::Timestamp GetTimestamp(size_t idx) const;
    bool IsDecimal(size_t idx) const;
    std::string GetDecimalString(size_t idx) const;

    /// One recursive handle for any complex (ARRAY/MAP/ROW) column.
    Value GetValue(size_t idx) const;
    Value GetValue(const std::string& name) const;

    // Name-based getters inherited from detail::NamedGetters<RowView>
    using detail::NamedGetters<RowView>::IsNull;
    using detail::NamedGetters<RowView>::GetBool;
    using detail::NamedGetters<RowView>::GetInt32;
    using detail::NamedGetters<RowView>::GetInt64;
    using detail::NamedGetters<RowView>::GetFloat32;
    using detail::NamedGetters<RowView>::GetFloat64;
    using detail::NamedGetters<RowView>::GetString;
    using detail::NamedGetters<RowView>::GetBytes;
    using detail::NamedGetters<RowView>::GetDate;
    using detail::NamedGetters<RowView>::GetTime;
    using detail::NamedGetters<RowView>::GetTimestamp;
    using detail::NamedGetters<RowView>::GetDecimalString;

   private:
    size_t Resolve(const std::string& name) const {
        if (!data_) {
            throw std::runtime_error("RowView: name-based access not available");
        }
        return detail::ResolveColumn(data_->columns, name);
    }
    std::shared_ptr<const detail::ScanData> data_;
    size_t bucket_idx_;
    size_t rec_idx_;
};

/// Read-only view over one row of a prefix lookup result.
///
/// Like RowView, but backed by PrefixData (a prefix lookup result) rather than
/// scan data. Shares ownership of the underlying handle via reference counting,
/// so it can safely outlive the PrefixLookupResult that produced it.
class PrefixRowView : public detail::NamedGetters<PrefixRowView> {
    friend struct detail::NamedGetters<PrefixRowView>;

   public:
    PrefixRowView(std::shared_ptr<const detail::PrefixData> data, size_t rec_idx)
        : data_(std::move(data)), rec_idx_(rec_idx) {}

    // ── Index-based getters ──────────────────────────────────────────
    size_t FieldCount() const;
    TypeId GetType(size_t idx) const;
    bool IsNull(size_t idx) const;
    bool GetBool(size_t idx) const;
    int32_t GetInt32(size_t idx) const;
    int64_t GetInt64(size_t idx) const;
    float GetFloat32(size_t idx) const;
    double GetFloat64(size_t idx) const;
    std::string_view GetString(size_t idx) const;
    std::pair<const uint8_t*, size_t> GetBytes(size_t idx) const;
    fluss::Date GetDate(size_t idx) const;
    fluss::Time GetTime(size_t idx) const;
    fluss::Timestamp GetTimestamp(size_t idx) const;
    bool IsDecimal(size_t idx) const;
    std::string GetDecimalString(size_t idx) const;

    /// One recursive handle for any complex (ARRAY/MAP/ROW) column, by index or name.
    Value GetValue(size_t idx) const;
    Value GetValue(const std::string& name) const;

    // Name-based getters inherited from detail::NamedGetters<PrefixRowView>
    using detail::NamedGetters<PrefixRowView>::IsNull;
    using detail::NamedGetters<PrefixRowView>::GetBool;
    using detail::NamedGetters<PrefixRowView>::GetInt32;
    using detail::NamedGetters<PrefixRowView>::GetInt64;
    using detail::NamedGetters<PrefixRowView>::GetFloat32;
    using detail::NamedGetters<PrefixRowView>::GetFloat64;
    using detail::NamedGetters<PrefixRowView>::GetString;
    using detail::NamedGetters<PrefixRowView>::GetBytes;
    using detail::NamedGetters<PrefixRowView>::GetDate;
    using detail::NamedGetters<PrefixRowView>::GetTime;
    using detail::NamedGetters<PrefixRowView>::GetTimestamp;
    using detail::NamedGetters<PrefixRowView>::GetDecimalString;

   private:
    size_t Resolve(const std::string& name) const {
        if (!data_) {
            throw std::runtime_error("PrefixRowView: name-based access not available");
        }
        return detail::ResolveColumn(data_->columns, name);
    }
    std::shared_ptr<const detail::PrefixData> data_;
    size_t rec_idx_;
};

/// Read-only result of a prefix lookup: zero-or-more matched rows, via Size()/GetRow(i).
class PrefixLookupResult {
   public:
    PrefixLookupResult() = default;

    /// Number of matched rows.
    size_t Size() const;
    bool IsEmpty() const { return Size() == 0; }

    /// Returns a view over the row at `index`. Throws std::out_of_range if
    /// `index >= Size()`.
    PrefixRowView GetRow(size_t index) const {
        if (!data_) {
            throw std::logic_error("PrefixLookupResult: not available (moved-from or null)");
        }
        if (index >= Size()) {
            throw std::out_of_range("PrefixLookupResult::GetRow: index out of range");
        }
        return PrefixRowView(data_, index);
    }

   private:
    friend class PrefixLookuper;
    std::shared_ptr<const detail::PrefixData> data_;
};

/// Identifies a specific bucket, optionally within a partition.
struct TableBucket {
    int64_t table_id;
    int32_t bucket_id;
    std::optional<int64_t> partition_id;

    bool operator==(const TableBucket& other) const {
        return table_id == other.table_id && bucket_id == other.bucket_id &&
               partition_id == other.partition_id;
    }

    bool operator!=(const TableBucket& other) const { return !(*this == other); }
};

/// A single scan record. Contains metadata and a RowView for field access.
///
/// ScanRecord is a value type that can be freely copied, stored, and
/// accumulated across multiple Poll() calls.
struct ScanRecord {
    int64_t offset;
    int64_t timestamp;
    ChangeType change_type;
    RowView row;
};

/// A bundle of scan records belonging to a single bucket.
///
/// BucketRecords is a value type — it shares ownership of the underlying scan data
/// via reference counting, so it can safely outlive the ScanRecords that produced it.
class BucketRecords {
   public:
    BucketRecords(std::shared_ptr<const detail::ScanData> data, TableBucket bucket,
                  size_t bucket_idx, size_t count)
        : data_(std::move(data)),
          bucket_(std::move(bucket)),
          bucket_idx_(bucket_idx),
          count_(count) {}

    /// The bucket these records belong to.
    const TableBucket& Bucket() const { return bucket_; }

    /// Number of records in this bucket.
    size_t Size() const { return count_; }
    bool Empty() const { return count_ == 0; }

    /// Access a record by its position within this bucket (0-based).
    ScanRecord operator[](size_t idx) const;

    class Iterator {
       public:
        ScanRecord operator*() const;
        Iterator& operator++() {
            ++idx_;
            return *this;
        }
        bool operator!=(const Iterator& other) const { return idx_ != other.idx_; }

       private:
        friend class BucketRecords;
        Iterator(const BucketRecords* owner, size_t idx) : owner_(owner), idx_(idx) {}
        const BucketRecords* owner_;
        size_t idx_;
    };

    Iterator begin() const { return Iterator(this, 0); }
    Iterator end() const { return Iterator(this, count_); }

   private:
    std::shared_ptr<const detail::ScanData> data_;
    TableBucket bucket_;
    size_t bucket_idx_;
    size_t count_;
};

class ScanRecords {
   public:
    ScanRecords() noexcept = default;
    ~ScanRecords() noexcept = default;

    ScanRecords(const ScanRecords&) = delete;
    ScanRecords& operator=(const ScanRecords&) = delete;
    ScanRecords(ScanRecords&&) noexcept = default;
    ScanRecords& operator=(ScanRecords&&) noexcept = default;

    /// Total number of records across all buckets.
    size_t Count() const;
    bool IsEmpty() const;

    /// Number of distinct buckets with records.
    size_t BucketCount() const;

    /// List of distinct buckets that have records.
    std::vector<TableBucket> Buckets() const;

    /// Get records for a specific bucket.
    ///
    /// Returns an empty BucketRecords if the bucket is not present (matches Rust/Java).
    /// Note: O(B) linear scan. For iteration over all buckets, prefer BucketAt(idx).
    BucketRecords Records(const TableBucket& bucket) const;

    /// Get records by bucket index (0-based). O(1).
    ///
    /// Throws std::out_of_range if idx >= BucketCount().
    BucketRecords BucketAt(size_t idx) const;

    /// Flat iterator over all records across all buckets (matches Java Iterable<ScanRecord>).
    class Iterator {
       public:
        ScanRecord operator*() const;
        Iterator& operator++();
        bool operator!=(const Iterator& other) const {
            return bucket_idx_ != other.bucket_idx_ || rec_idx_ != other.rec_idx_;
        }

       private:
        friend class ScanRecords;
        Iterator(const ScanRecords* owner, size_t bucket_idx, size_t rec_idx)
            : owner_(owner), bucket_idx_(bucket_idx), rec_idx_(rec_idx) {}
        const ScanRecords* owner_;
        size_t bucket_idx_;
        size_t rec_idx_;
    };

    Iterator begin() const;
    Iterator end() const { return Iterator(this, BucketCount(), 0); }

   private:
    friend class LogScanner;
    ScanRecord RecordAt(size_t bucket, size_t rec_idx) const;
    std::shared_ptr<const detail::ScanData> data_;
};

namespace detail {
// Defined in table.cpp; builds ArrowRecordBatch wrappers from FFI Arrow batches.
struct ArrowBatchImporter;
}  // namespace detail

class ArrowRecordBatch {
   public:
    std::shared_ptr<arrow::RecordBatch> GetArrowRecordBatch() const { return batch_; }

    bool Available() const;

    // Get number of rows in the batch
    int64_t NumRows() const;

    // Get ScanBatch metadata
    int64_t GetTableId() const;
    int64_t GetPartitionId() const;
    int32_t GetBucketId() const;
    int64_t GetBaseOffset() const;
    int64_t GetLastOffset() const;

   private:
    friend class LogScanner;
    friend struct detail::ArrowBatchImporter;
    explicit ArrowRecordBatch(std::shared_ptr<arrow::RecordBatch> batch, int64_t table_id,
                              int64_t partition_id, int32_t bucket_id,
                              int64_t base_offset) noexcept;

    std::shared_ptr<arrow::RecordBatch> batch_{nullptr};

    int64_t table_id_;
    int64_t partition_id_;
    int32_t bucket_id_;
    int64_t base_offset_;
};

struct ArrowRecordBatches {
    std::vector<std::unique_ptr<ArrowRecordBatch>> batches;

    size_t Size() const { return batches.size(); }
    bool Empty() const { return batches.empty(); }
    const std::unique_ptr<ArrowRecordBatch>& operator[](size_t idx) const { return batches[idx]; }

    auto begin() const { return batches.begin(); }
    auto end() const { return batches.end(); }
};

struct BucketOffset {
    int64_t table_id;
    int64_t partition_id;
    int32_t bucket_id;
    int64_t offset;
};

struct BucketSubscription {
    int32_t bucket_id;
    int64_t offset;
};

struct PartitionBucketSubscription {
    int64_t partition_id;
    int32_t bucket_id;
    int64_t offset;
};

struct LakeSnapshot {
    int64_t snapshot_id;
    std::vector<BucketOffset> bucket_offsets;
};

struct PartitionInfo {
    int64_t partition_id;
    std::string partition_name;
};

struct ServerNode {
    int32_t id;
    std::string host;
    uint32_t port;
    std::string server_type;
    std::string uid;
};

/// Descriptor for create_database (optional). Leave comment and properties empty for default.
struct DatabaseDescriptor {
    std::string comment;
    std::unordered_map<std::string, std::string> properties;
};

/// Metadata returned by GetDatabaseInfo.
struct DatabaseInfo {
    std::string database_name;
    std::string comment;
    std::unordered_map<std::string, std::string> properties;
    int64_t created_time{0};
    int64_t modified_time{0};
};

/// Read-only result for lookup operations.
class LookupResult : public detail::NamedGetters<LookupResult> {
    friend struct detail::NamedGetters<LookupResult>;

   public:
    LookupResult() noexcept;
    ~LookupResult() noexcept;

    LookupResult(const LookupResult&) = delete;
    LookupResult& operator=(const LookupResult&) = delete;
    LookupResult(LookupResult&& other) noexcept;
    LookupResult& operator=(LookupResult&& other) noexcept;

    bool Found() const;
    size_t FieldCount() const;

    // ── Index-based getters ──────────────────────────────────────────
    TypeId GetType(size_t idx) const;
    bool IsNull(size_t idx) const;
    bool GetBool(size_t idx) const;
    int32_t GetInt32(size_t idx) const;
    int64_t GetInt64(size_t idx) const;
    float GetFloat32(size_t idx) const;
    double GetFloat64(size_t idx) const;
    std::string_view GetString(size_t idx) const;
    std::pair<const uint8_t*, size_t> GetBytes(size_t idx) const;
    fluss::Date GetDate(size_t idx) const;
    fluss::Time GetTime(size_t idx) const;
    fluss::Timestamp GetTimestamp(size_t idx) const;
    bool IsDecimal(size_t idx) const;
    std::string GetDecimalString(size_t idx) const;

    /// One recursive handle for any complex (ARRAY/MAP/ROW) column, by index or name.
    Value GetValue(size_t idx) const;
    Value GetValue(const std::string& name) const;

    // Name-based getters inherited from detail::NamedGetters<LookupResult>
    using detail::NamedGetters<LookupResult>::IsNull;
    using detail::NamedGetters<LookupResult>::GetBool;
    using detail::NamedGetters<LookupResult>::GetInt32;
    using detail::NamedGetters<LookupResult>::GetInt64;
    using detail::NamedGetters<LookupResult>::GetFloat32;
    using detail::NamedGetters<LookupResult>::GetFloat64;
    using detail::NamedGetters<LookupResult>::GetString;
    using detail::NamedGetters<LookupResult>::GetBytes;
    using detail::NamedGetters<LookupResult>::GetDate;
    using detail::NamedGetters<LookupResult>::GetTime;
    using detail::NamedGetters<LookupResult>::GetTimestamp;
    using detail::NamedGetters<LookupResult>::GetDecimalString;

   private:
    friend class Lookuper;
    size_t Resolve(const std::string& name) const {
        if (!column_map_) {
            BuildColumnMap();
        }
        return detail::ResolveColumn(*column_map_, name);
    }
    void Destroy() noexcept;
    void BuildColumnMap() const;
    ffi::LookupResultInner* inner_{nullptr};
    mutable std::shared_ptr<detail::ColumnMap> column_map_;
};

class AppendWriter;
class UpsertWriter;
class Lookuper;
class PrefixLookuper;
class WriteResult;
class LogScanner;
class BatchScanner;
class Admin;
class Table;
class TableAppend;
class TableUpsert;
class TableLookup;
class TableScan;

struct Configuration {
    // Coordinator server address
    std::string bootstrap_servers{"127.0.0.1:9123"};
    // Max request size in bytes (10 MB)
    int32_t writer_request_max_size{10 * 1024 * 1024};
    // Writer acknowledgment mode: "all", "0", "1", or "-1"
    std::string writer_acks{"all"};
    // Max number of writer retries
    int32_t writer_retries{std::numeric_limits<int32_t>::max()};
    // Writer batch size in bytes (2 MB), also the upper bound when dynamic sizing is on
    int32_t writer_batch_size{2 * 1024 * 1024};
    // Tune the per-table writer batch size from observed fill ratios
    bool writer_dynamic_batch_size_enabled{true};
    // Lower bound (256 KB) for the dynamic batch size estimator
    int32_t writer_dynamic_batch_size_min{256 * 1024};
    // Bucket assigner for tables without bucket keys: "sticky" or "round_robin"
    std::string writer_bucket_no_key_assigner{"sticky"};
    // Number of remote log batches to prefetch during scanning
    size_t scanner_remote_log_prefetch_num{4};
    // Number of threads for downloading remote log data
    size_t remote_file_download_thread_num{3};
    // Remote log read concurrency within one file (streaming read path)
    size_t scanner_remote_log_read_concurrency{4};
    // Maximum number of records returned in a single call to Poll() for LogScanner
    size_t scanner_log_max_poll_records{500};
    // Maximum bytes per fetch response for LogScanner (16 MB)
    int32_t scanner_log_fetch_max_bytes{16 * 1024 * 1024};
    // Minimum bytes to accumulate before server returns a fetch response
    int32_t scanner_log_fetch_min_bytes{1};
    // Maximum time (ms) the server may wait to satisfy min bytes
    int32_t scanner_log_fetch_wait_max_time_ms{500};
    // Maximum bytes per fetch response per bucket for LogScanner (1 MB)
    int32_t scanner_log_fetch_max_bytes_for_bucket{1024 * 1024};
    int64_t writer_batch_timeout_ms{100};
    // Whether to enable idempotent writes
    bool writer_enable_idempotence{true};
    // Maximum number of in-flight requests per bucket for idempotent writes
    size_t writer_max_inflight_requests_per_bucket{5};
    // Total memory available for buffering write batches (default 64MB)
    size_t writer_buffer_memory_size{64 * 1024 * 1024};
    // Maximum time in milliseconds to block waiting for buffer memory
    uint64_t writer_buffer_wait_timeout_ms{std::numeric_limits<uint64_t>::max()};
    // Connect timeout in milliseconds for TCP transport connect
    uint64_t connect_timeout_ms{120000};
    // Security protocol: "PLAINTEXT" (default, no auth) or "sasl" (SASL auth)
    std::string security_protocol{"PLAINTEXT"};
    // SASL mechanism (only "PLAIN" is supported)
    std::string security_sasl_mechanism{"PLAIN"};
    // SASL username (required when security_protocol is "sasl")
    std::string security_sasl_username;
    // SASL password (required when security_protocol is "sasl")
    std::string security_sasl_password;
    // Maximum number of pending lookup operations
    size_t lookup_queue_size{25600};
    // Maximum batch size of merging lookup operations to one lookup request
    size_t lookup_max_batch_size{128};
    // Maximum time to wait for the lookup batch to fill (in milliseconds)
    uint64_t lookup_batch_timeout_ms{100};
    // Maximum number of unacknowledged lookup requests
    size_t lookup_max_inflight_requests{128};
    // Maximum number of lookup retries
    int32_t lookup_max_retries{std::numeric_limits<int32_t>::max()};
};

class Connection {
   public:
    Connection() noexcept;
    ~Connection() noexcept;

    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;
    Connection(Connection&& other) noexcept;
    Connection& operator=(Connection&& other) noexcept;

    static Result Create(const Configuration& config, Connection& out);

    bool Available() const;

    Result GetAdmin(Admin& out);
    Result GetTable(const TablePath& table_path, Table& out);

   private:
    void Destroy() noexcept;
    ffi::Connection* conn_{nullptr};
};

class Admin {
   public:
    Admin() noexcept;
    ~Admin() noexcept;

    Admin(const Admin&) = delete;
    Admin& operator=(const Admin&) = delete;
    Admin(Admin&& other) noexcept;
    Admin& operator=(Admin&& other) noexcept;

    bool Available() const;

    /// Creates a table. Column types — including nested ARRAY/MAP/ROW built
    /// with `DataType::Array`/`Map`/`Row` — are carried to the server exactly
    /// as declared (precision, scale, length, nullability, and field names).
    Result CreateTable(const TablePath& table_path, const TableDescriptor& descriptor,
                       bool ignore_if_exists = false);

    Result DropTable(const TablePath& table_path, bool ignore_if_not_exists = false);

    Result GetTableInfo(const TablePath& table_path, TableInfo& out);

    Result GetLatestLakeSnapshot(const TablePath& table_path, LakeSnapshot& out);

    Result ListOffsets(const TablePath& table_path, const std::vector<int32_t>& bucket_ids,
                       const OffsetSpec& offset_spec, std::unordered_map<int32_t, int64_t>& out);

    Result ListPartitionOffsets(const TablePath& table_path, const std::string& partition_name,
                                const std::vector<int32_t>& bucket_ids,
                                const OffsetSpec& offset_spec,
                                std::unordered_map<int32_t, int64_t>& out);

    Result ListPartitionInfos(const TablePath& table_path, std::vector<PartitionInfo>& out);

    Result ListPartitionInfos(const TablePath& table_path,
                              const std::unordered_map<std::string, std::string>& partition_spec,
                              std::vector<PartitionInfo>& out);

    Result CreatePartition(const TablePath& table_path,
                           const std::unordered_map<std::string, std::string>& partition_spec,
                           bool ignore_if_exists = false);

    Result DropPartition(const TablePath& table_path,
                         const std::unordered_map<std::string, std::string>& partition_spec,
                         bool ignore_if_not_exists = false);

    Result CreateDatabase(const std::string& database_name, const DatabaseDescriptor& descriptor,
                          bool ignore_if_exists = false);

    Result DropDatabase(const std::string& database_name, bool ignore_if_not_exists = false,
                        bool cascade = true);

    Result ListDatabases(std::vector<std::string>& out);

    Result DatabaseExists(const std::string& database_name, bool& out);

    Result GetDatabaseInfo(const std::string& database_name, DatabaseInfo& out);

    Result ListTables(const std::string& database_name, std::vector<std::string>& out);

    Result TableExists(const TablePath& table_path, bool& out);

    Result GetServerNodes(std::vector<ServerNode>& out);

   private:
    Result DoListOffsets(const TablePath& table_path, const std::vector<int32_t>& bucket_ids,
                         const OffsetSpec& offset_spec, std::unordered_map<int32_t, int64_t>& out,
                         const std::string* partition_name = nullptr);

    friend class Connection;
    Admin(ffi::Admin* admin) noexcept;

    void Destroy() noexcept;
    ffi::Admin* admin_{nullptr};
};

class Table {
   public:
    Table() noexcept;
    ~Table() noexcept;

    Table(const Table&) = delete;
    Table& operator=(const Table&) = delete;
    Table(Table&& other) noexcept;
    Table& operator=(Table&& other) noexcept;

    bool Available() const;

    GenericRow NewRow() const;

    TableAppend NewAppend();
    TableUpsert NewUpsert();
    TableLookup NewLookup();
    TableScan NewScan();

    /// Creates a prefix lookuper. `lookup_columns` must be the table's bucket-key
    /// columns (a strict prefix of the primary key); fails otherwise.
    Result NewPrefixLookup(std::vector<std::string> lookup_columns, PrefixLookuper& out);

    TableInfo GetTableInfo() const;
    TablePath GetTablePath() const;
    bool HasPrimaryKey() const;

   private:
    friend class Connection;
    friend class TableAppend;
    friend class TableUpsert;
    friend class TableLookup;
    friend class TableScan;
    Table(ffi::Table* table) noexcept;

    void Destroy() noexcept;
    const std::shared_ptr<GenericRow::ColumnMap>& GetColumnMap() const;

    ffi::Table* table_{nullptr};
    mutable std::shared_ptr<GenericRow::ColumnMap> column_map_;
};

class TableAppend {
   public:
    TableAppend(const TableAppend&) = delete;
    TableAppend& operator=(const TableAppend&) = delete;
    TableAppend(TableAppend&&) noexcept = default;
    TableAppend& operator=(TableAppend&&) noexcept = default;

    Result CreateWriter(AppendWriter& out);

   private:
    friend class Table;
    explicit TableAppend(ffi::Table* table) noexcept;

    ffi::Table* table_{nullptr};
};

class TableUpsert {
   public:
    TableUpsert(const TableUpsert&) = delete;
    TableUpsert& operator=(const TableUpsert&) = delete;
    TableUpsert(TableUpsert&&) noexcept = default;
    TableUpsert& operator=(TableUpsert&&) noexcept = default;

    TableUpsert& PartialUpdateByIndex(std::vector<size_t> column_indices);
    TableUpsert& PartialUpdateByName(std::vector<std::string> column_names);

    Result CreateWriter(UpsertWriter& out);

   private:
    friend class Table;
    explicit TableUpsert(ffi::Table* table) noexcept;

    std::vector<size_t> ResolveNameProjection() const;

    ffi::Table* table_{nullptr};
    std::vector<size_t> column_indices_;
    std::vector<std::string> column_names_;
};

class TableLookup {
   public:
    TableLookup(const TableLookup&) = delete;
    TableLookup& operator=(const TableLookup&) = delete;
    TableLookup(TableLookup&&) noexcept = default;
    TableLookup& operator=(TableLookup&&) noexcept = default;

    Result CreateLookuper(Lookuper& out);

   private:
    friend class Table;
    explicit TableLookup(ffi::Table* table) noexcept;

    ffi::Table* table_{nullptr};
};

class TableScan {
   public:
    TableScan(const TableScan&) = delete;
    TableScan& operator=(const TableScan&) = delete;
    TableScan(TableScan&&) noexcept = default;
    TableScan& operator=(TableScan&&) noexcept = default;

    TableScan& ProjectByIndex(std::vector<size_t> column_indices);
    TableScan& ProjectByName(std::vector<std::string> column_names);

    TableScan& Limit(int32_t row_number);

    /// Creates a record-mode log scanner, polled for individual `ScanRecord`s.
    ///
    /// Works on log tables and on primary-key (KV) tables. For a primary-key
    /// table this subscribes to its CDC changelog: each `ScanRecord` carries a
    /// `ChangeType` -- `+I` (insert), `-U` (update-before), `+U` (update-after)
    /// or `-D` (delete). A log table yields `+A` (append-only). Requires the
    /// ARROW log format.
    Result CreateLogScanner(LogScanner& out);

    /// Creates a batch-mode log scanner that yields Arrow record batches.
    ///
    /// Log tables only. Primary-key tables are rejected because the Arrow batch
    /// path carries no per-record change types; read a primary-key table's
    /// changelog with `CreateLogScanner()` instead. Requires the ARROW log
    /// format.
    Result CreateRecordBatchLogScanner(LogScanner& out);

    Result CreateBucketBatchScanner(const TableBucket& bucket, BatchScanner& out);

   private:
    friend class Table;
    explicit TableScan(ffi::Table* table) noexcept;

    std::vector<size_t> ResolveNameProjection() const;
    Result DoCreateScanner(LogScanner& out, bool is_record_batch);

    ffi::Table* table_{nullptr};
    std::vector<size_t> projection_;
    std::vector<std::string> name_projection_;
    std::optional<int32_t> limit_;
};

class WriteResult {
   public:
    WriteResult() noexcept;
    ~WriteResult() noexcept;

    WriteResult(const WriteResult&) = delete;
    WriteResult& operator=(const WriteResult&) = delete;
    WriteResult(WriteResult&& other) noexcept;
    WriteResult& operator=(WriteResult&& other) noexcept;

    bool Available() const;

    /// Wait for server acknowledgment of the write.
    /// For fire-and-forget, simply let the WriteResult go out of scope.
    Result Wait();

   private:
    friend class AppendWriter;
    friend class UpsertWriter;
    WriteResult(ffi::WriteResult* inner) noexcept;

    void Destroy() noexcept;
    ffi::WriteResult* inner_{nullptr};
};

class AppendWriter {
   public:
    AppendWriter() noexcept;
    ~AppendWriter() noexcept;

    AppendWriter(const AppendWriter&) = delete;
    AppendWriter& operator=(const AppendWriter&) = delete;
    AppendWriter(AppendWriter&& other) noexcept;
    AppendWriter& operator=(AppendWriter&& other) noexcept;

    bool Available() const;

    Result Append(const GenericRow& row);
    Result Append(const GenericRow& row, WriteResult& out);
    Result AppendArrowBatch(const std::shared_ptr<arrow::RecordBatch>& batch);
    Result AppendArrowBatch(const std::shared_ptr<arrow::RecordBatch>& batch, WriteResult& out);
    Result Flush();

   private:
    friend class Table;
    friend class TableAppend;
    AppendWriter(ffi::AppendWriter* writer) noexcept;

    void Destroy() noexcept;
    ffi::AppendWriter* writer_{nullptr};
};

class UpsertWriter {
   public:
    UpsertWriter() noexcept;
    ~UpsertWriter() noexcept;

    UpsertWriter(const UpsertWriter&) = delete;
    UpsertWriter& operator=(const UpsertWriter&) = delete;
    UpsertWriter(UpsertWriter&& other) noexcept;
    UpsertWriter& operator=(UpsertWriter&& other) noexcept;

    bool Available() const;

    Result Upsert(const GenericRow& row);
    Result Upsert(const GenericRow& row, WriteResult& out);
    Result Delete(const GenericRow& row);
    Result Delete(const GenericRow& row, WriteResult& out);
    Result Flush();

   private:
    friend class Table;
    friend class TableUpsert;
    UpsertWriter(ffi::UpsertWriter* writer) noexcept;
    void Destroy() noexcept;
    ffi::UpsertWriter* writer_{nullptr};
};

class Lookuper {
   public:
    Lookuper() noexcept;
    ~Lookuper() noexcept;

    Lookuper(const Lookuper&) = delete;
    Lookuper& operator=(const Lookuper&) = delete;
    Lookuper(Lookuper&& other) noexcept;
    Lookuper& operator=(Lookuper&& other) noexcept;

    bool Available() const;

    Result Lookup(const GenericRow& pk_row, LookupResult& out);

   private:
    friend class Table;
    friend class TableLookup;
    Lookuper(ffi::Lookuper* lookuper) noexcept;
    void Destroy() noexcept;
    ffi::Lookuper* lookuper_{nullptr};
};

/// Performs bucket-key prefix lookups; create via Table::NewPrefixLookup(). Move-only.
class PrefixLookuper {
   public:
    PrefixLookuper() noexcept;
    ~PrefixLookuper() noexcept;

    PrefixLookuper(const PrefixLookuper&) = delete;
    PrefixLookuper& operator=(const PrefixLookuper&) = delete;
    PrefixLookuper(PrefixLookuper&& other) noexcept;
    PrefixLookuper& operator=(PrefixLookuper&& other) noexcept;

    bool Available() const;

    /// Looks up all rows matching the prefix columns set on `prefix_row`.
    Result PrefixLookup(const GenericRow& prefix_row, PrefixLookupResult& out);

   private:
    friend class Table;
    PrefixLookuper(ffi::PrefixLookuper* lookuper) noexcept;
    void Destroy() noexcept;
    ffi::PrefixLookuper* lookuper_{nullptr};
};

class LogScanner {
   public:
    LogScanner() noexcept;
    ~LogScanner() noexcept;

    LogScanner(const LogScanner&) = delete;
    LogScanner& operator=(const LogScanner&) = delete;
    LogScanner(LogScanner&& other) noexcept;
    LogScanner& operator=(LogScanner&& other) noexcept;

    bool Available() const;

    Result Subscribe(int32_t bucket_id, int64_t start_offset);
    Result Subscribe(const std::vector<BucketSubscription>& bucket_offsets);
    Result SubscribePartitionBuckets(int64_t partition_id, int32_t bucket_id, int64_t start_offset);
    Result SubscribePartitionBuckets(const std::vector<PartitionBucketSubscription>& subscriptions);
    Result Unsubscribe(int32_t bucket_id);
    Result UnsubscribePartition(int64_t partition_id, int32_t bucket_id);
    Result Poll(int64_t timeout_ms, ScanRecords& out);
    Result PollRecordBatch(int64_t timeout_ms, ArrowRecordBatches& out);

   private:
    friend class Table;
    friend class TableScan;
    LogScanner(ffi::LogScanner* scanner) noexcept;

    void Destroy() noexcept;
    ffi::LogScanner* scanner_{nullptr};
};

// One-shot bounded scan of a single bucket, from TableScan::CreateBucketBatchScanner.
class BatchScanner {
   public:
    BatchScanner() noexcept;
    ~BatchScanner() noexcept;

    BatchScanner(const BatchScanner&) = delete;
    BatchScanner& operator=(const BatchScanner&) = delete;
    BatchScanner(BatchScanner&& other) noexcept;
    BatchScanner& operator=(BatchScanner&& other) noexcept;

    bool Available() const;
    const TableBucket& Bucket() const { return bucket_; }

    Result NextBatch(ArrowRecordBatches& out);
    Result CollectAllBatches(ArrowRecordBatches& out);

   private:
    friend class TableScan;
    explicit BatchScanner(ffi::BatchScanner* scanner) noexcept;

    void Destroy() noexcept;
    ffi::BatchScanner* scanner_{nullptr};
    TableBucket bucket_{};
};

}  // namespace fluss
