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

#include <arrow/c/bridge.h>

#include <cassert>
#include <ctime>

#include "ffi_converter.hpp"
#include "fluss.hpp"
#include "lib.rs.h"
#include "rust/cxx.h"
// todo:  bindings/cpp/BUILD.bazel still doesn't declare Arrow include/link dependencies.
// In environments where Bazel does not already have Arrow available, this will fail at compile/link
// time.
#include <arrow/record_batch.h>

namespace fluss {

static constexpr int kSecondsPerDay = 24 * 60 * 60;

static std::time_t timegm_utc(std::tm* tm) {
#if defined(_WIN32)
    return _mkgmtime(tm);
#else
    return ::timegm(tm);
#endif
}

static std::tm gmtime_utc(std::time_t epoch_seconds) {
    std::tm tm{};
#if defined(_WIN32)
    gmtime_s(&tm, &epoch_seconds);
#else
    ::gmtime_r(&epoch_seconds, &tm);
#endif
    return tm;
}

Date Date::FromYMD(int year, int month, int day) {
    std::tm tm{};
    tm.tm_year = year - 1900;
    tm.tm_mon = month - 1;
    tm.tm_mday = day;
    std::time_t epoch_seconds = timegm_utc(&tm);
    return {static_cast<int32_t>(epoch_seconds / kSecondsPerDay)};
}

int Date::Year() const {
    std::time_t epoch_seconds = static_cast<std::time_t>(days_since_epoch) * kSecondsPerDay;
    std::tm tm = gmtime_utc(epoch_seconds);
    return tm.tm_year + 1900;
}

int Date::Month() const {
    std::time_t epoch_seconds = static_cast<std::time_t>(days_since_epoch) * kSecondsPerDay;
    std::tm tm = gmtime_utc(epoch_seconds);
    return tm.tm_mon + 1;
}

int Date::Day() const {
    std::time_t epoch_seconds = static_cast<std::time_t>(days_since_epoch) * kSecondsPerDay;
    std::tm tm = gmtime_utc(epoch_seconds);
    return tm.tm_mday;
}

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define CHECK_INNER(name)                                                                 \
    do {                                                                                  \
        if (!inner_) throw std::logic_error(name ": not available (moved-from or null)"); \
    } while (0)

// ============================================================================
// ArrayWriter — builder for array values backed by Rust ArrayWriterInner
// ============================================================================

ArrayWriter::ArrayWriter(size_t size, DataType element_type) : element_type_(std::move(element_type)) {
    auto flat = utils::flatten_array_type(element_type_);
    int32_t leaf_type_id = flat.nesting > 0 ? flat.leaf_type : static_cast<int32_t>(element_type_.id());
    uint32_t leaf_precision = static_cast<uint32_t>(flat.nesting > 0 ? flat.leaf_precision
                                                                      : element_type_.precision());
    uint32_t leaf_scale = static_cast<uint32_t>(flat.nesting > 0 ? flat.leaf_scale : element_type_.scale());
    uint32_t array_nesting = static_cast<uint32_t>(flat.nesting);

    auto box = ffi::new_array_writer(size, leaf_type_id, leaf_precision, leaf_scale, array_nesting);
    inner_ = box.into_raw();
}

ArrayWriter::~ArrayWriter() noexcept { Destroy(); }

void ArrayWriter::Destroy() noexcept {
    if (inner_) {
        rust::Box<ffi::ArrayWriterInner>::from_raw(inner_);
        inner_ = nullptr;
    }
}

ArrayWriter::ArrayWriter(ArrayWriter&& other) noexcept
    : inner_(other.inner_), element_type_(std::move(other.element_type_)) {
    other.inner_ = nullptr;
}

ArrayWriter& ArrayWriter::operator=(ArrayWriter&& other) noexcept {
    if (this != &other) {
        Destroy();
        inner_ = other.inner_;
        element_type_ = std::move(other.element_type_);
        other.inner_ = nullptr;
    }
    return *this;
}

bool ArrayWriter::Available() const { return inner_ != nullptr; }

size_t ArrayWriter::Size() const noexcept {
    assert(inner_ && "ArrayWriter::Size called on moved-from instance");
    return inner_->aw_size();
}

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define CHECK_AW(name)                                                                    \
    do {                                                                                  \
        if (!inner_) throw std::logic_error(name ": not available (moved-from or null)"); \
    } while (0)

void ArrayWriter::SetNull(size_t idx) { CHECK_AW("ArrayWriter"); inner_->aw_set_null(idx); }
void ArrayWriter::SetBool(size_t idx, bool v) { CHECK_AW("ArrayWriter"); inner_->aw_set_bool(idx, v); }
void ArrayWriter::SetInt32(size_t idx, int32_t v) { CHECK_AW("ArrayWriter"); inner_->aw_set_i32(idx, v); }
void ArrayWriter::SetInt64(size_t idx, int64_t v) { CHECK_AW("ArrayWriter"); inner_->aw_set_i64(idx, v); }
void ArrayWriter::SetFloat32(size_t idx, float v) { CHECK_AW("ArrayWriter"); inner_->aw_set_f32(idx, v); }
void ArrayWriter::SetFloat64(size_t idx, double v) { CHECK_AW("ArrayWriter"); inner_->aw_set_f64(idx, v); }

void ArrayWriter::SetString(size_t idx, const std::string& v) {
    CHECK_AW("ArrayWriter");
    inner_->aw_set_str(idx, v);
}

void ArrayWriter::SetBytes(size_t idx, const std::vector<uint8_t>& v) {
    CHECK_AW("ArrayWriter");
    inner_->aw_set_bytes(idx, rust::Slice<const uint8_t>(v.data(), v.size()));
}

void ArrayWriter::SetDate(size_t idx, fluss::Date d) {
    CHECK_AW("ArrayWriter");
    inner_->aw_set_date(idx, d.days_since_epoch);
}

void ArrayWriter::SetTime(size_t idx, fluss::Time t) {
    CHECK_AW("ArrayWriter");
    inner_->aw_set_time(idx, t.millis_since_midnight);
}

void ArrayWriter::SetTimestampNtz(size_t idx, fluss::Timestamp ts) {
    CHECK_AW("ArrayWriter");
    inner_->aw_set_ts_ntz(idx, ts.epoch_millis, ts.nano_of_millisecond);
}

void ArrayWriter::SetTimestampLtz(size_t idx, fluss::Timestamp ts) {
    CHECK_AW("ArrayWriter");
    inner_->aw_set_ts_ltz(idx, ts.epoch_millis, ts.nano_of_millisecond);
}

void ArrayWriter::SetDecimal(size_t idx, const std::string& value) {
    CHECK_AW("ArrayWriter");
    inner_->aw_set_decimal_str(idx, value);
}

void ArrayWriter::SetArray(size_t idx, ArrayWriter&& nested) {
    CHECK_AW("ArrayWriter");
    if (!nested.inner_) {
        throw std::logic_error("ArrayWriter::SetArray: nested writer not available");
    }
    inner_->aw_set_array(idx, *nested.inner_);
    nested.Destroy();
}

// ============================================================================
// ArrayView — read-only recursive view into an array column value
// ============================================================================

ArrayView::~ArrayView() noexcept { Destroy(); }

void ArrayView::Destroy() noexcept {
    if (inner_) {
        rust::Box<ffi::ArrayViewInner>::from_raw(inner_);
        inner_ = nullptr;
    }
}

ArrayView::ArrayView(ArrayView&& other) noexcept : inner_(other.inner_) { other.inner_ = nullptr; }

ArrayView& ArrayView::operator=(ArrayView&& other) noexcept {
    if (this != &other) {
        Destroy();
        inner_ = other.inner_;
        other.inner_ = nullptr;
    }
    return *this;
}

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define CHECK_AV()                                                                      \
    do {                                                                                \
        if (!inner_) throw std::logic_error("ArrayView: not available (moved-from)");  \
    } while (0)

size_t ArrayView::Size() const noexcept {
    assert(inner_ && "ArrayView::Size called on moved-from instance");
    return inner_->av_size();
}

TypeId ArrayView::ElementType() const noexcept {
    assert(inner_ && "ArrayView::ElementType called on moved-from instance");
    return static_cast<TypeId>(inner_->av_element_type_id());
}

bool ArrayView::IsNull(size_t element) const {
    CHECK_AV();
    return inner_->av_is_null(element);
}

bool ArrayView::GetBool(size_t element) const {
    CHECK_AV();
    return inner_->av_get_bool(element);
}

int32_t ArrayView::GetInt32(size_t element) const {
    CHECK_AV();
    return inner_->av_get_i32(element);
}

int64_t ArrayView::GetInt64(size_t element) const {
    CHECK_AV();
    return inner_->av_get_i64(element);
}

float ArrayView::GetFloat32(size_t element) const {
    CHECK_AV();
    return inner_->av_get_f32(element);
}

double ArrayView::GetFloat64(size_t element) const {
    CHECK_AV();
    return inner_->av_get_f64(element);
}

std::string ArrayView::GetString(size_t element) const {
    CHECK_AV();
    return std::string(inner_->av_get_str(element));
}

std::vector<uint8_t> ArrayView::GetBytes(size_t element) const {
    CHECK_AV();
    auto rv = inner_->av_get_bytes(element);
    return {rv.data(), rv.data() + rv.size()};
}

fluss::Date ArrayView::GetDate(size_t element) const {
    CHECK_AV();
    return fluss::Date{inner_->av_get_date_days(element)};
}

fluss::Time ArrayView::GetTime(size_t element) const {
    CHECK_AV();
    return fluss::Time{inner_->av_get_time_millis(element)};
}

fluss::Timestamp ArrayView::GetTimestampNtz(size_t element) const {
    CHECK_AV();
    return fluss::Timestamp{inner_->av_get_ts_millis(element),
                            inner_->av_get_ts_nanos(element)};
}

fluss::Timestamp ArrayView::GetTimestampLtz(size_t element) const {
    CHECK_AV();
    return fluss::Timestamp{inner_->av_get_ts_millis(element),
                            inner_->av_get_ts_nanos(element)};
}

std::string ArrayView::GetDecimalString(size_t element) const {
    CHECK_AV();
    return std::string(inner_->av_get_decimal_str(element));
}

ArrayView ArrayView::GetArray(size_t element) const {
    CHECK_AV();
    auto box = inner_->av_get_nested(element);
    return ArrayView(box.into_raw());
}

#undef CHECK_AV

// ============================================================================
// GenericRow — write-only row backed by opaque Rust GenericRowInner
// ============================================================================

GenericRow::GenericRow() {
    auto box = ffi::new_generic_row(0);
    inner_ = box.into_raw();
}

GenericRow::GenericRow(size_t field_count) {
    auto box = ffi::new_generic_row(field_count);
    inner_ = box.into_raw();
}

GenericRow::~GenericRow() noexcept { Destroy(); }

void GenericRow::Destroy() noexcept {
    if (inner_) {
        rust::Box<ffi::GenericRowInner>::from_raw(inner_);
        inner_ = nullptr;
    }
    column_map_.reset();
}

GenericRow::GenericRow(GenericRow&& other) noexcept
    : inner_(other.inner_), column_map_(std::move(other.column_map_)) {
    other.inner_ = nullptr;
}

GenericRow& GenericRow::operator=(GenericRow&& other) noexcept {
    if (this != &other) {
        Destroy();
        inner_ = other.inner_;
        column_map_ = std::move(other.column_map_);
        other.inner_ = nullptr;
    }
    return *this;
}

bool GenericRow::Available() const { return inner_ != nullptr; }

void GenericRow::Reset() {
    CHECK_INNER("GenericRow");
    inner_->gr_reset();
}

void GenericRow::SetNull(size_t idx) {
    CHECK_INNER("GenericRow");
    inner_->gr_set_null(idx);
}
void GenericRow::SetBool(size_t idx, bool v) {
    CHECK_INNER("GenericRow");
    inner_->gr_set_bool(idx, v);
}
void GenericRow::SetInt32(size_t idx, int32_t v) {
    CHECK_INNER("GenericRow");
    inner_->gr_set_i32(idx, v);
}
void GenericRow::SetInt64(size_t idx, int64_t v) {
    CHECK_INNER("GenericRow");
    inner_->gr_set_i64(idx, v);
}
void GenericRow::SetFloat32(size_t idx, float v) {
    CHECK_INNER("GenericRow");
    inner_->gr_set_f32(idx, v);
}
void GenericRow::SetFloat64(size_t idx, double v) {
    CHECK_INNER("GenericRow");
    inner_->gr_set_f64(idx, v);
}

void GenericRow::SetString(size_t idx, std::string v) {
    CHECK_INNER("GenericRow");
    inner_->gr_set_str(idx, v);
}

void GenericRow::SetBytes(size_t idx, std::vector<uint8_t> v) {
    CHECK_INNER("GenericRow");
    inner_->gr_set_bytes(idx, rust::Slice<const uint8_t>(v.data(), v.size()));
}

void GenericRow::SetDate(size_t idx, fluss::Date d) {
    CHECK_INNER("GenericRow");
    inner_->gr_set_date(idx, d.days_since_epoch);
}

void GenericRow::SetTime(size_t idx, fluss::Time t) {
    CHECK_INNER("GenericRow");
    inner_->gr_set_time(idx, t.millis_since_midnight);
}

void GenericRow::SetTimestampNtz(size_t idx, fluss::Timestamp ts) {
    CHECK_INNER("GenericRow");
    inner_->gr_set_ts_ntz(idx, ts.epoch_millis, ts.nano_of_millisecond);
}

void GenericRow::SetTimestampLtz(size_t idx, fluss::Timestamp ts) {
    CHECK_INNER("GenericRow");
    inner_->gr_set_ts_ltz(idx, ts.epoch_millis, ts.nano_of_millisecond);
}

void GenericRow::SetDecimal(size_t idx, const std::string& value) {
    CHECK_INNER("GenericRow");
    inner_->gr_set_decimal_str(idx, value);
}

void GenericRow::SetArray(size_t idx, ArrayWriter&& writer) {
    CHECK_INNER("GenericRow");
    if (!writer.inner_) {
        throw std::logic_error("GenericRow::SetArray: ArrayWriter not available");
    }
    inner_->gr_set_array(idx, *writer.inner_);
    writer.Destroy();
}

// ============================================================================
// ScanData — destructor must live in .cpp where rust::Box is visible
// ============================================================================

detail::ScanData::~ScanData() {
    if (raw) {
        rust::Box<ffi::ScanResultInner>::from_raw(raw);
    }
}

// ============================================================================
// RowView — zero-copy read-only row view for scan results
// ============================================================================

// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define CHECK_DATA(name)                                                                 \
    do {                                                                                 \
        if (!data_) throw std::logic_error(name ": not available (moved-from or null)"); \
    } while (0)

size_t RowView::FieldCount() const { return data_ ? data_->raw->sv_field_count() : 0; }

TypeId RowView::GetType(size_t idx) const {
    CHECK_DATA("RowView");
    return static_cast<TypeId>(data_->raw->sv_column_type(idx));
}

bool RowView::IsNull(size_t idx) const {
    CHECK_DATA("RowView");
    return data_->raw->sv_is_null(bucket_idx_, rec_idx_, idx);
}
bool RowView::GetBool(size_t idx) const {
    CHECK_DATA("RowView");
    return data_->raw->sv_get_bool(bucket_idx_, rec_idx_, idx);
}
int32_t RowView::GetInt32(size_t idx) const {
    CHECK_DATA("RowView");
    return data_->raw->sv_get_i32(bucket_idx_, rec_idx_, idx);
}
int64_t RowView::GetInt64(size_t idx) const {
    CHECK_DATA("RowView");
    return data_->raw->sv_get_i64(bucket_idx_, rec_idx_, idx);
}
float RowView::GetFloat32(size_t idx) const {
    CHECK_DATA("RowView");
    return data_->raw->sv_get_f32(bucket_idx_, rec_idx_, idx);
}
double RowView::GetFloat64(size_t idx) const {
    CHECK_DATA("RowView");
    return data_->raw->sv_get_f64(bucket_idx_, rec_idx_, idx);
}

std::string_view RowView::GetString(size_t idx) const {
    CHECK_DATA("RowView");
    auto s = data_->raw->sv_get_str(bucket_idx_, rec_idx_, idx);
    return std::string_view(s.data(), s.size());
}

std::pair<const uint8_t*, size_t> RowView::GetBytes(size_t idx) const {
    CHECK_DATA("RowView");
    auto bytes = data_->raw->sv_get_bytes(bucket_idx_, rec_idx_, idx);
    return {bytes.data(), bytes.size()};
}

Date RowView::GetDate(size_t idx) const {
    CHECK_DATA("RowView");
    return Date{data_->raw->sv_get_date_days(bucket_idx_, rec_idx_, idx)};
}

Time RowView::GetTime(size_t idx) const {
    CHECK_DATA("RowView");
    return Time{data_->raw->sv_get_time_millis(bucket_idx_, rec_idx_, idx)};
}

Timestamp RowView::GetTimestamp(size_t idx) const {
    CHECK_DATA("RowView");
    return Timestamp{data_->raw->sv_get_ts_millis(bucket_idx_, rec_idx_, idx),
                     data_->raw->sv_get_ts_nanos(bucket_idx_, rec_idx_, idx)};
}

bool RowView::IsDecimal(size_t idx) const { return GetType(idx) == TypeId::Decimal; }

std::string RowView::GetDecimalString(size_t idx) const {
    CHECK_DATA("RowView");
    return std::string(data_->raw->sv_get_decimal_str(bucket_idx_, rec_idx_, idx));
}

size_t RowView::GetArraySize(size_t idx) const {
    CHECK_DATA("RowView");
    return data_->raw->sv_get_array_size(bucket_idx_, rec_idx_, idx);
}

TypeId RowView::GetArrayElementType(size_t idx) const {
    CHECK_DATA("RowView");
    return static_cast<TypeId>(data_->raw->sv_get_array_element_type(idx));
}

bool RowView::IsArrayElementNull(size_t idx, size_t element) const {
    CHECK_DATA("RowView");
    return data_->raw->sv_get_array_is_null(bucket_idx_, rec_idx_, idx, element);
}

bool RowView::GetArrayBool(size_t idx, size_t element) const {
    CHECK_DATA("RowView");
    return data_->raw->sv_get_array_bool(bucket_idx_, rec_idx_, idx, element);
}

int32_t RowView::GetArrayInt32(size_t idx, size_t element) const {
    CHECK_DATA("RowView");
    return data_->raw->sv_get_array_i32(bucket_idx_, rec_idx_, idx, element);
}

int64_t RowView::GetArrayInt64(size_t idx, size_t element) const {
    CHECK_DATA("RowView");
    return data_->raw->sv_get_array_i64(bucket_idx_, rec_idx_, idx, element);
}

float RowView::GetArrayFloat32(size_t idx, size_t element) const {
    CHECK_DATA("RowView");
    return data_->raw->sv_get_array_f32(bucket_idx_, rec_idx_, idx, element);
}

double RowView::GetArrayFloat64(size_t idx, size_t element) const {
    CHECK_DATA("RowView");
    return data_->raw->sv_get_array_f64(bucket_idx_, rec_idx_, idx, element);
}

std::string RowView::GetArrayString(size_t idx, size_t element) const {
    CHECK_DATA("RowView");
    return std::string(data_->raw->sv_get_array_str(bucket_idx_, rec_idx_, idx, element));
}

std::vector<uint8_t> RowView::GetArrayBytes(size_t idx, size_t element) const {
    CHECK_DATA("RowView");
    auto rv = data_->raw->sv_get_array_bytes(bucket_idx_, rec_idx_, idx, element);
    return {rv.data(), rv.data() + rv.size()};
}

fluss::Date RowView::GetArrayDate(size_t idx, size_t element) const {
    CHECK_DATA("RowView");
    return fluss::Date{data_->raw->sv_get_array_date_days(bucket_idx_, rec_idx_, idx, element)};
}

fluss::Time RowView::GetArrayTime(size_t idx, size_t element) const {
    CHECK_DATA("RowView");
    return fluss::Time{data_->raw->sv_get_array_time_millis(bucket_idx_, rec_idx_, idx, element)};
}

fluss::Timestamp RowView::GetArrayTimestamp(size_t idx, size_t element) const {
    CHECK_DATA("RowView");
    auto millis = data_->raw->sv_get_array_ts_millis(bucket_idx_, rec_idx_, idx, element);
    auto nanos = data_->raw->sv_get_array_ts_nanos(bucket_idx_, rec_idx_, idx, element);
    return fluss::Timestamp{millis, nanos};
}

std::string RowView::GetArrayDecimalString(size_t idx, size_t element) const {
    CHECK_DATA("RowView");
    return std::string(data_->raw->sv_get_array_decimal_str(bucket_idx_, rec_idx_, idx, element));
}

ArrayView RowView::GetArrayView(size_t idx) const {
    CHECK_DATA("RowView");
    auto box = data_->raw->sv_get_array_view(bucket_idx_, rec_idx_, idx);
    return ArrayView(box.into_raw());
}

// ============================================================================
// ScanRecords — backed by opaque Rust ScanResultInner
// ============================================================================

// ScanRecords constructor, destructor, move operations are all defaulted in the header.

size_t ScanRecords::Count() const { return data_ ? data_->raw->sv_record_count() : 0; }

bool ScanRecords::IsEmpty() const { return Count() == 0; }

ScanRecord ScanRecords::RecordAt(size_t bucket, size_t rec_idx) const {
    if (!data_) {
        throw std::logic_error("ScanRecords: not available (moved-from or null)");
    }
    return ScanRecord{data_->raw->sv_offset(bucket, rec_idx),
                      data_->raw->sv_timestamp(bucket, rec_idx),
                      static_cast<ChangeType>(data_->raw->sv_change_type(bucket, rec_idx)),
                      RowView(data_, bucket, rec_idx)};
}

static TableBucket to_table_bucket(const ffi::FfiBucketInfo& g) {
    return TableBucket{g.table_id, g.bucket_id,
                       g.has_partition_id ? std::optional<int64_t>(g.partition_id) : std::nullopt};
}

size_t ScanRecords::BucketCount() const { return data_ ? data_->raw->sv_bucket_infos().size() : 0; }

ScanRecord ScanRecords::Iterator::operator*() const {
    return owner_->RecordAt(bucket_idx_, rec_idx_);
}

ScanRecords::Iterator ScanRecords::begin() const { return Iterator(this, 0, 0); }

ScanRecords::Iterator& ScanRecords::Iterator::operator++() {
    ++rec_idx_;
    if (owner_->data_) {
        const auto& infos = owner_->data_->raw->sv_bucket_infos();
        while (bucket_idx_ < infos.size() && rec_idx_ >= infos[bucket_idx_].record_count) {
            rec_idx_ = 0;
            ++bucket_idx_;
        }
    }
    return *this;
}

std::vector<TableBucket> ScanRecords::Buckets() const {
    std::vector<TableBucket> result;
    if (!data_) return result;
    const auto& infos = data_->raw->sv_bucket_infos();
    result.reserve(infos.size());
    for (const auto& g : infos) {
        result.push_back(to_table_bucket(g));
    }
    return result;
}

BucketRecords ScanRecords::Records(const TableBucket& bucket) const {
    if (!data_) {
        return BucketRecords({}, bucket, 0, 0);
    }
    const auto& infos = data_->raw->sv_bucket_infos();
    for (size_t i = 0; i < infos.size(); ++i) {
        TableBucket tb = to_table_bucket(infos[i]);
        if (tb == bucket) {
            return BucketRecords(data_, std::move(tb), i, infos[i].record_count);
        }
    }
    return BucketRecords({}, bucket, 0, 0);
}

BucketRecords ScanRecords::BucketAt(size_t idx) const {
    if (!data_) {
        throw std::logic_error("ScanRecords: not available (moved-from or null)");
    }
    const auto& infos = data_->raw->sv_bucket_infos();
    if (idx >= infos.size()) {
        throw std::out_of_range("ScanRecords::BucketAt: index " + std::to_string(idx) +
                                " out of range (" + std::to_string(infos.size()) + " buckets)");
    }
    return BucketRecords(data_, to_table_bucket(infos[idx]), idx, infos[idx].record_count);
}

ScanRecord BucketRecords::operator[](size_t idx) const {
    if (idx >= count_) {
        throw std::out_of_range("BucketRecords: index " + std::to_string(idx) + " out of range (" +
                                std::to_string(count_) + " records)");
    }
    return ScanRecord{data_->raw->sv_offset(bucket_idx_, idx),
                      data_->raw->sv_timestamp(bucket_idx_, idx),
                      static_cast<ChangeType>(data_->raw->sv_change_type(bucket_idx_, idx)),
                      RowView(data_, bucket_idx_, idx)};
}

ScanRecord BucketRecords::Iterator::operator*() const { return owner_->operator[](idx_); }

// ============================================================================
// LookupResult — backed by opaque Rust LookupResultInner
// ============================================================================

LookupResult::LookupResult() noexcept = default;

LookupResult::~LookupResult() noexcept { Destroy(); }

void LookupResult::Destroy() noexcept {
    if (inner_) {
        rust::Box<ffi::LookupResultInner>::from_raw(inner_);
        inner_ = nullptr;
        column_map_.reset();
    }
}

LookupResult::LookupResult(LookupResult&& other) noexcept
    : inner_(other.inner_), column_map_(std::move(other.column_map_)) {
    other.inner_ = nullptr;
}

LookupResult& LookupResult::operator=(LookupResult&& other) noexcept {
    if (this != &other) {
        Destroy();
        inner_ = other.inner_;
        column_map_ = std::move(other.column_map_);
        other.inner_ = nullptr;
    }
    return *this;
}

void LookupResult::BuildColumnMap() const {
    if (!inner_) return;
    auto map = std::make_shared<detail::ColumnMap>();
    auto count = inner_->lv_field_count();
    for (size_t i = 0; i < count; ++i) {
        auto name = inner_->lv_column_name(i);
        (*map)[std::string(name.data(), name.size())] = {
            i, static_cast<TypeId>(inner_->lv_column_type(i))};
    }
    column_map_ = std::move(map);
}

bool LookupResult::Found() const { return inner_ && inner_->lv_found(); }

size_t LookupResult::FieldCount() const { return inner_ ? inner_->lv_field_count() : 0; }

TypeId LookupResult::GetType(size_t idx) const {
    CHECK_INNER("LookupResult");
    return static_cast<TypeId>(inner_->lv_column_type(idx));
}

bool LookupResult::IsNull(size_t idx) const {
    CHECK_INNER("LookupResult");
    return inner_->lv_is_null(idx);
}
bool LookupResult::GetBool(size_t idx) const {
    CHECK_INNER("LookupResult");
    return inner_->lv_get_bool(idx);
}
int32_t LookupResult::GetInt32(size_t idx) const {
    CHECK_INNER("LookupResult");
    return inner_->lv_get_i32(idx);
}
int64_t LookupResult::GetInt64(size_t idx) const {
    CHECK_INNER("LookupResult");
    return inner_->lv_get_i64(idx);
}
float LookupResult::GetFloat32(size_t idx) const {
    CHECK_INNER("LookupResult");
    return inner_->lv_get_f32(idx);
}
double LookupResult::GetFloat64(size_t idx) const {
    CHECK_INNER("LookupResult");
    return inner_->lv_get_f64(idx);
}

std::string_view LookupResult::GetString(size_t idx) const {
    CHECK_INNER("LookupResult");
    auto s = inner_->lv_get_str(idx);
    return std::string_view(s.data(), s.size());
}

std::pair<const uint8_t*, size_t> LookupResult::GetBytes(size_t idx) const {
    CHECK_INNER("LookupResult");
    auto bytes = inner_->lv_get_bytes(idx);
    return {bytes.data(), bytes.size()};
}

Date LookupResult::GetDate(size_t idx) const {
    CHECK_INNER("LookupResult");
    return Date{inner_->lv_get_date_days(idx)};
}

Time LookupResult::GetTime(size_t idx) const {
    CHECK_INNER("LookupResult");
    return Time{inner_->lv_get_time_millis(idx)};
}

Timestamp LookupResult::GetTimestamp(size_t idx) const {
    CHECK_INNER("LookupResult");
    return Timestamp{inner_->lv_get_ts_millis(idx), inner_->lv_get_ts_nanos(idx)};
}

bool LookupResult::IsDecimal(size_t idx) const { return GetType(idx) == TypeId::Decimal; }

std::string LookupResult::GetDecimalString(size_t idx) const {
    CHECK_INNER("LookupResult");
    return std::string(inner_->lv_get_decimal_str(idx));
}

size_t LookupResult::GetArraySize(size_t idx) const {
    CHECK_INNER("LookupResult");
    return inner_->lv_get_array_size(idx);
}

TypeId LookupResult::GetArrayElementType(size_t idx) const {
    CHECK_INNER("LookupResult");
    return static_cast<TypeId>(inner_->lv_get_array_element_type(idx));
}

bool LookupResult::IsArrayElementNull(size_t idx, size_t element) const {
    CHECK_INNER("LookupResult");
    return inner_->lv_get_array_is_null(idx, element);
}

bool LookupResult::GetArrayBool(size_t idx, size_t element) const {
    CHECK_INNER("LookupResult");
    return inner_->lv_get_array_bool(idx, element);
}

int32_t LookupResult::GetArrayInt32(size_t idx, size_t element) const {
    CHECK_INNER("LookupResult");
    return inner_->lv_get_array_i32(idx, element);
}

int64_t LookupResult::GetArrayInt64(size_t idx, size_t element) const {
    CHECK_INNER("LookupResult");
    return inner_->lv_get_array_i64(idx, element);
}

float LookupResult::GetArrayFloat32(size_t idx, size_t element) const {
    CHECK_INNER("LookupResult");
    return inner_->lv_get_array_f32(idx, element);
}

double LookupResult::GetArrayFloat64(size_t idx, size_t element) const {
    CHECK_INNER("LookupResult");
    return inner_->lv_get_array_f64(idx, element);
}

std::string LookupResult::GetArrayString(size_t idx, size_t element) const {
    CHECK_INNER("LookupResult");
    return std::string(inner_->lv_get_array_str(idx, element));
}

std::vector<uint8_t> LookupResult::GetArrayBytes(size_t idx, size_t element) const {
    CHECK_INNER("LookupResult");
    auto rv = inner_->lv_get_array_bytes(idx, element);
    return {rv.data(), rv.data() + rv.size()};
}

fluss::Date LookupResult::GetArrayDate(size_t idx, size_t element) const {
    CHECK_INNER("LookupResult");
    return fluss::Date{inner_->lv_get_array_date_days(idx, element)};
}

fluss::Time LookupResult::GetArrayTime(size_t idx, size_t element) const {
    CHECK_INNER("LookupResult");
    return fluss::Time{inner_->lv_get_array_time_millis(idx, element)};
}

fluss::Timestamp LookupResult::GetArrayTimestamp(size_t idx, size_t element) const {
    CHECK_INNER("LookupResult");
    auto millis = inner_->lv_get_array_ts_millis(idx, element);
    auto nanos = inner_->lv_get_array_ts_nanos(idx, element);
    return fluss::Timestamp{millis, nanos};
}

std::string LookupResult::GetArrayDecimalString(size_t idx, size_t element) const {
    CHECK_INNER("LookupResult");
    return std::string(inner_->lv_get_array_decimal_str(idx, element));
}

ArrayView LookupResult::GetArrayView(size_t idx) const {
    CHECK_INNER("LookupResult");
    auto box = inner_->lv_get_array_view(idx);
    return ArrayView(box.into_raw());
}

// ============================================================================
// Table
// ============================================================================

Table::Table() noexcept = default;

Table::Table(ffi::Table* table) noexcept : table_(table) {}

Table::~Table() noexcept { Destroy(); }

void Table::Destroy() noexcept {
    if (table_) {
        ffi::delete_table(table_);
        table_ = nullptr;
    }
}

Table::Table(Table&& other) noexcept
    : table_(other.table_), column_map_(std::move(other.column_map_)) {
    other.table_ = nullptr;
}

Table& Table::operator=(Table&& other) noexcept {
    if (this != &other) {
        Destroy();
        table_ = other.table_;
        column_map_ = std::move(other.column_map_);
        other.table_ = nullptr;
    }
    return *this;
}

bool Table::Available() const { return table_ != nullptr; }

TableAppend Table::NewAppend() { return TableAppend(table_); }

TableUpsert Table::NewUpsert() { return TableUpsert(table_); }

TableLookup Table::NewLookup() { return TableLookup(table_); }

TableScan Table::NewScan() { return TableScan(table_); }

const std::shared_ptr<GenericRow::ColumnMap>& Table::GetColumnMap() const {
    if (!column_map_ && Available()) {
        auto info = GetTableInfo();
        column_map_ = std::make_shared<GenericRow::ColumnMap>();
        for (size_t i = 0; i < info.schema.columns.size(); ++i) {
            (*column_map_)[info.schema.columns[i].name] = {i,
                                                           info.schema.columns[i].data_type.id()};
        }
    }
    return column_map_;
}

GenericRow Table::NewRow() const {
    GenericRow row;
    row.column_map_ = GetColumnMap();
    return row;
}

TableInfo Table::GetTableInfo() const {
    if (!Available()) {
        return TableInfo{};
    }
    auto ffi_info = table_->get_table_info_from_table();
    return utils::from_ffi_table_info(ffi_info);
}

TablePath Table::GetTablePath() const {
    if (!Available()) {
        return TablePath{};
    }
    auto ffi_path = table_->get_table_path();
    return TablePath{std::string(ffi_path.database_name), std::string(ffi_path.table_name)};
}

bool Table::HasPrimaryKey() const {
    if (!Available()) {
        return false;
    }
    return table_->has_primary_key();
}

// ============================================================================
// TableAppend
// ============================================================================

TableAppend::TableAppend(ffi::Table* table) noexcept : table_(table) {}

Result TableAppend::CreateWriter(AppendWriter& out) {
    if (table_ == nullptr) {
        return utils::make_client_error("Table not available");
    }

    auto ffi_result = table_->new_append_writer();
    auto result = utils::from_ffi_result(ffi_result.result);
    if (result.Ok()) {
        out = AppendWriter(utils::ptr_from_ffi<ffi::AppendWriter>(ffi_result));
    }
    return result;
}

// ============================================================================
// TableUpsert
// ============================================================================

TableUpsert::TableUpsert(ffi::Table* table) noexcept : table_(table) {}

TableUpsert& TableUpsert::PartialUpdateByIndex(std::vector<size_t> column_indices) {
    if (column_indices.empty()) {
        throw std::invalid_argument("PartialUpdateByIndex requires at least one column");
    }
    column_indices_ = std::move(column_indices);
    column_names_.clear();
    return *this;
}

TableUpsert& TableUpsert::PartialUpdateByName(std::vector<std::string> column_names) {
    if (column_names.empty()) {
        throw std::invalid_argument("PartialUpdateByName requires at least one column");
    }
    column_names_ = std::move(column_names);
    column_indices_.clear();
    return *this;
}

std::vector<size_t> TableUpsert::ResolveNameProjection() const {
    auto ffi_info = table_->get_table_info_from_table();
    const auto& columns = ffi_info.schema.columns;

    std::vector<size_t> indices;
    for (const auto& name : column_names_) {
        bool found = false;
        for (size_t i = 0; i < columns.size(); ++i) {
            if (std::string(columns[i].name) == name) {
                indices.push_back(i);
                found = true;
                break;
            }
        }
        if (!found) {
            throw std::runtime_error("Column '" + name + "' not found");
        }
    }
    return indices;
}

Result TableUpsert::CreateWriter(UpsertWriter& out) {
    if (table_ == nullptr) {
        return utils::make_client_error("Table not available");
    }

    try {
        auto resolved_indices = !column_names_.empty() ? ResolveNameProjection() : column_indices_;

        rust::Vec<size_t> rust_indices;
        for (size_t idx : resolved_indices) {
            rust_indices.push_back(idx);
        }
        auto ffi_result = table_->create_upsert_writer(std::move(rust_indices));
        auto result = utils::from_ffi_result(ffi_result.result);
        if (result.Ok()) {
            out = UpsertWriter(utils::ptr_from_ffi<ffi::UpsertWriter>(ffi_result));
        }
        return result;
    } catch (const std::exception& e) {
        // ResolveNameProjection() may throw
        return utils::make_client_error(e.what());
    }
}

// ============================================================================
// TableLookup
// ============================================================================

TableLookup::TableLookup(ffi::Table* table) noexcept : table_(table) {}

Result TableLookup::CreateLookuper(Lookuper& out) {
    if (table_ == nullptr) {
        return utils::make_client_error("Table not available");
    }

    auto ffi_result = table_->new_lookuper();
    auto result = utils::from_ffi_result(ffi_result.result);
    if (result.Ok()) {
        out = Lookuper(utils::ptr_from_ffi<ffi::Lookuper>(ffi_result));
    }
    return result;
}

// ============================================================================
// TableScan
// ============================================================================

TableScan::TableScan(ffi::Table* table) noexcept : table_(table) {}

TableScan& TableScan::ProjectByIndex(std::vector<size_t> column_indices) {
    projection_ = std::move(column_indices);
    name_projection_.clear();
    return *this;
}

TableScan& TableScan::ProjectByName(std::vector<std::string> column_names) {
    name_projection_ = std::move(column_names);
    projection_.clear();
    return *this;
}

std::vector<size_t> TableScan::ResolveNameProjection() const {
    auto ffi_info = table_->get_table_info_from_table();
    const auto& columns = ffi_info.schema.columns;

    std::vector<size_t> indices;
    for (const auto& name : name_projection_) {
        bool found = false;
        for (size_t i = 0; i < columns.size(); ++i) {
            if (std::string(columns[i].name) == name) {
                indices.push_back(i);
                found = true;
                break;
            }
        }
        if (!found) {
            throw std::runtime_error("Column '" + name + "' not found");
        }
    }
    return indices;
}

Result TableScan::CreateLogScanner(LogScanner& out) { return DoCreateScanner(out, false); }

Result TableScan::CreateRecordBatchLogScanner(LogScanner& out) {
    return DoCreateScanner(out, true);
}

Result TableScan::DoCreateScanner(LogScanner& out, bool is_record_batch) {
    if (table_ == nullptr) {
        return utils::make_client_error("Table not available");
    }

    try {
        auto resolved_indices = !name_projection_.empty() ? ResolveNameProjection() : projection_;
        rust::Vec<size_t> rust_indices;
        for (size_t idx : resolved_indices) {
            rust_indices.push_back(idx);
        }
        auto ffi_result = table_->create_scanner(std::move(rust_indices), is_record_batch);
        auto result = utils::from_ffi_result(ffi_result.result);
        if (result.Ok()) {
            out.scanner_ = utils::ptr_from_ffi<ffi::LogScanner>(ffi_result);
        }
        return result;
    } catch (const std::exception& e) {
        // ResolveNameProjection() may throw
        return utils::make_client_error(e.what());
    }
}

// ============================================================================
// WriteResult
// ============================================================================

WriteResult::WriteResult() noexcept = default;

WriteResult::WriteResult(ffi::WriteResult* inner) noexcept : inner_(inner) {}

WriteResult::~WriteResult() noexcept { Destroy(); }

void WriteResult::Destroy() noexcept {
    if (inner_) {
        ffi::delete_write_result(inner_);
        inner_ = nullptr;
    }
}

WriteResult::WriteResult(WriteResult&& other) noexcept : inner_(other.inner_) {
    other.inner_ = nullptr;
}

WriteResult& WriteResult::operator=(WriteResult&& other) noexcept {
    if (this != &other) {
        Destroy();
        inner_ = other.inner_;
        other.inner_ = nullptr;
    }
    return *this;
}

bool WriteResult::Available() const { return inner_ != nullptr; }

Result WriteResult::Wait() {
    if (!Available()) {
        return utils::make_ok();
    }

    auto ffi_result = inner_->wait();
    return utils::from_ffi_result(ffi_result);
}

// ============================================================================
// AppendWriter
// ============================================================================

AppendWriter::AppendWriter() noexcept = default;

AppendWriter::AppendWriter(ffi::AppendWriter* writer) noexcept : writer_(writer) {}

AppendWriter::~AppendWriter() noexcept { Destroy(); }

void AppendWriter::Destroy() noexcept {
    if (writer_) {
        ffi::delete_append_writer(writer_);
        writer_ = nullptr;
    }
}

AppendWriter::AppendWriter(AppendWriter&& other) noexcept : writer_(other.writer_) {
    other.writer_ = nullptr;
}

AppendWriter& AppendWriter::operator=(AppendWriter&& other) noexcept {
    if (this != &other) {
        Destroy();
        writer_ = other.writer_;
        other.writer_ = nullptr;
    }
    return *this;
}

bool AppendWriter::Available() const { return writer_ != nullptr; }

Result AppendWriter::Append(const GenericRow& row) {
    WriteResult wr;
    return Append(row, wr);
}

Result AppendWriter::Append(const GenericRow& row, WriteResult& out) {
    if (!Available()) {
        return utils::make_client_error("AppendWriter not available");
    }
    if (!row.Available()) {
        return utils::make_client_error("GenericRow not available");
    }

    auto ffi_result = writer_->append(*row.inner_);
    auto result = utils::from_ffi_result(ffi_result.result);
    if (result.Ok()) {
        out = WriteResult(utils::ptr_from_ffi<ffi::WriteResult>(ffi_result));
    }
    return result;
}

Result AppendWriter::AppendArrowBatch(const std::shared_ptr<arrow::RecordBatch>& batch) {
    WriteResult wr;
    return AppendArrowBatch(batch, wr);
}

Result AppendWriter::AppendArrowBatch(const std::shared_ptr<arrow::RecordBatch>& batch,
                                      WriteResult& out) {
    if (!Available()) {
        return utils::make_client_error("AppendWriter not available");
    }
    if (!batch) {
        return utils::make_client_error("Arrow RecordBatch is null");
    }

    // Export via Arrow C Data Interface
    struct ArrowArray c_array;
    struct ArrowSchema c_schema;
    auto status = arrow::ExportRecordBatch(*batch, &c_array, &c_schema);
    if (!status.ok()) {
        return utils::make_client_error("Failed to export Arrow batch: " + status.ToString());
    }

    // Heap-allocate for Rust ownership transfer
    auto* array_heap = new ArrowArray(std::move(c_array));
    auto* schema_heap = new ArrowSchema(std::move(c_schema));

    // Rust takes ownership of both pointers immediately via Box::from_raw(),
    // so after this call C++ must NOT free them.
    auto ffi_result = writer_->append_arrow_batch(reinterpret_cast<size_t>(array_heap),
                                                  reinterpret_cast<size_t>(schema_heap));
    auto result = utils::from_ffi_result(ffi_result.result);
    if (result.Ok()) {
        out.Destroy();
        out.inner_ = utils::ptr_from_ffi<ffi::WriteResult>(ffi_result);
    }
    return result;
}

Result AppendWriter::Flush() {
    if (!Available()) {
        return utils::make_client_error("AppendWriter not available");
    }

    auto ffi_result = writer_->flush();
    return utils::from_ffi_result(ffi_result);
}

// ============================================================================
// UpsertWriter
// ============================================================================

UpsertWriter::UpsertWriter() noexcept = default;

UpsertWriter::UpsertWriter(ffi::UpsertWriter* writer) noexcept : writer_(writer) {}

UpsertWriter::~UpsertWriter() noexcept { Destroy(); }

void UpsertWriter::Destroy() noexcept {
    if (writer_) {
        ffi::delete_upsert_writer(writer_);
        writer_ = nullptr;
    }
}

UpsertWriter::UpsertWriter(UpsertWriter&& other) noexcept : writer_(other.writer_) {
    other.writer_ = nullptr;
}

UpsertWriter& UpsertWriter::operator=(UpsertWriter&& other) noexcept {
    if (this != &other) {
        Destroy();
        writer_ = other.writer_;
        other.writer_ = nullptr;
    }
    return *this;
}

bool UpsertWriter::Available() const { return writer_ != nullptr; }

Result UpsertWriter::Upsert(const GenericRow& row) {
    WriteResult wr;
    return Upsert(row, wr);
}

Result UpsertWriter::Upsert(const GenericRow& row, WriteResult& out) {
    if (!Available()) {
        return utils::make_client_error("UpsertWriter not available");
    }
    if (!row.Available()) {
        return utils::make_client_error("GenericRow not available");
    }

    auto ffi_result = writer_->upsert(*row.inner_);
    auto result = utils::from_ffi_result(ffi_result.result);
    if (result.Ok()) {
        out = WriteResult(utils::ptr_from_ffi<ffi::WriteResult>(ffi_result));
    }
    return result;
}

Result UpsertWriter::Delete(const GenericRow& row) {
    WriteResult wr;
    return Delete(row, wr);
}

Result UpsertWriter::Delete(const GenericRow& row, WriteResult& out) {
    if (!Available()) {
        return utils::make_client_error("UpsertWriter not available");
    }
    if (!row.Available()) {
        return utils::make_client_error("GenericRow not available");
    }

    auto ffi_result = writer_->delete_row(*row.inner_);
    auto result = utils::from_ffi_result(ffi_result.result);
    if (result.Ok()) {
        out = WriteResult(utils::ptr_from_ffi<ffi::WriteResult>(ffi_result));
    }
    return result;
}

Result UpsertWriter::Flush() {
    if (!Available()) {
        return utils::make_client_error("UpsertWriter not available");
    }

    auto ffi_result = writer_->upsert_flush();
    return utils::from_ffi_result(ffi_result);
}

// ============================================================================
// Lookuper
// ============================================================================

Lookuper::Lookuper() noexcept = default;

Lookuper::Lookuper(ffi::Lookuper* lookuper) noexcept : lookuper_(lookuper) {}

Lookuper::~Lookuper() noexcept { Destroy(); }

void Lookuper::Destroy() noexcept {
    if (lookuper_) {
        ffi::delete_lookuper(lookuper_);
        lookuper_ = nullptr;
    }
}

Lookuper::Lookuper(Lookuper&& other) noexcept : lookuper_(other.lookuper_) {
    other.lookuper_ = nullptr;
}

Lookuper& Lookuper::operator=(Lookuper&& other) noexcept {
    if (this != &other) {
        Destroy();
        lookuper_ = other.lookuper_;
        other.lookuper_ = nullptr;
    }
    return *this;
}

bool Lookuper::Available() const { return lookuper_ != nullptr; }

Result Lookuper::Lookup(const GenericRow& pk_row, LookupResult& out) {
    if (!Available()) {
        return utils::make_client_error("Lookuper not available");
    }
    if (!pk_row.Available()) {
        return utils::make_client_error("GenericRow not available");
    }

    auto result_box = lookuper_->lookup(*pk_row.inner_);
    if (result_box->lv_has_error()) {
        return utils::make_error(result_box->lv_error_code(),
                                 std::string(result_box->lv_error_message()));
    }

    out.Destroy();
    out.inner_ = result_box.into_raw();
    return utils::make_ok();
}

// ============================================================================
// LogScanner
// ============================================================================

LogScanner::LogScanner() noexcept = default;

LogScanner::LogScanner(ffi::LogScanner* scanner) noexcept : scanner_(scanner) {}

LogScanner::~LogScanner() noexcept { Destroy(); }

void LogScanner::Destroy() noexcept {
    if (scanner_) {
        ffi::delete_log_scanner(scanner_);
        scanner_ = nullptr;
    }
}

LogScanner::LogScanner(LogScanner&& other) noexcept : scanner_(other.scanner_) {
    other.scanner_ = nullptr;
}

LogScanner& LogScanner::operator=(LogScanner&& other) noexcept {
    if (this != &other) {
        Destroy();
        scanner_ = other.scanner_;
        other.scanner_ = nullptr;
    }
    return *this;
}

bool LogScanner::Available() const { return scanner_ != nullptr; }

Result LogScanner::Subscribe(int32_t bucket_id, int64_t start_offset) {
    if (!Available()) {
        return utils::make_client_error("LogScanner not available");
    }

    auto ffi_result = scanner_->subscribe(bucket_id, start_offset);
    return utils::from_ffi_result(ffi_result);
}

Result LogScanner::Subscribe(const std::vector<BucketSubscription>& bucket_offsets) {
    if (!Available()) {
        return utils::make_client_error("LogScanner not available");
    }

    rust::Vec<ffi::FfiBucketSubscription> rust_subs;
    for (const auto& sub : bucket_offsets) {
        ffi::FfiBucketSubscription ffi_sub;
        ffi_sub.bucket_id = sub.bucket_id;
        ffi_sub.offset = sub.offset;
        rust_subs.push_back(ffi_sub);
    }

    auto ffi_result = scanner_->subscribe_buckets(std::move(rust_subs));
    return utils::from_ffi_result(ffi_result);
}

Result LogScanner::SubscribePartitionBuckets(int64_t partition_id, int32_t bucket_id,
                                             int64_t start_offset) {
    if (!Available()) {
        return utils::make_client_error("LogScanner not available");
    }

    auto ffi_result = scanner_->subscribe_partition(partition_id, bucket_id, start_offset);
    return utils::from_ffi_result(ffi_result);
}

Result LogScanner::SubscribePartitionBuckets(
    const std::vector<PartitionBucketSubscription>& subscriptions) {
    if (!Available()) {
        return utils::make_client_error("LogScanner not available");
    }

    rust::Vec<ffi::FfiPartitionBucketSubscription> rust_subs;
    for (const auto& sub : subscriptions) {
        ffi::FfiPartitionBucketSubscription ffi_sub;
        ffi_sub.partition_id = sub.partition_id;
        ffi_sub.bucket_id = sub.bucket_id;
        ffi_sub.offset = sub.offset;
        rust_subs.push_back(ffi_sub);
    }

    auto ffi_result = scanner_->subscribe_partition_buckets(std::move(rust_subs));
    return utils::from_ffi_result(ffi_result);
}

Result LogScanner::Unsubscribe(int32_t bucket_id) {
    if (!Available()) {
        return utils::make_client_error("LogScanner not available");
    }

    auto ffi_result = scanner_->unsubscribe(bucket_id);
    return utils::from_ffi_result(ffi_result);
}

Result LogScanner::UnsubscribePartition(int64_t partition_id, int32_t bucket_id) {
    if (!Available()) {
        return utils::make_client_error("LogScanner not available");
    }

    auto ffi_result = scanner_->unsubscribe_partition(partition_id, bucket_id);
    return utils::from_ffi_result(ffi_result);
}

Result LogScanner::Poll(int64_t timeout_ms, ScanRecords& out) {
    if (!Available()) {
        return utils::make_client_error("LogScanner not available");
    }

    auto result_box = scanner_->poll(timeout_ms);
    if (result_box->sv_has_error()) {
        return utils::make_error(result_box->sv_error_code(),
                                 std::string(result_box->sv_error_message()));
    }

    // Wrap raw pointer in ScanData immediately so it's never leaked on exception.
    auto data = std::make_shared<detail::ScanData>(result_box.into_raw(), detail::ColumnMap{});
    // Build column map eagerly — shared by all RowViews/BucketRecords.
    auto col_count = data->raw->sv_column_count();
    for (size_t i = 0; i < col_count; ++i) {
        auto name = data->raw->sv_column_name(i);
        data->columns[std::string(name.data(), name.size())] = {
            i, static_cast<TypeId>(data->raw->sv_column_type(i))};
    }
    out.data_ = std::move(data);
    return utils::make_ok();
}

ArrowRecordBatch::ArrowRecordBatch(std::shared_ptr<arrow::RecordBatch> batch, int64_t table_id,
                                   int64_t partition_id, int32_t bucket_id,
                                   int64_t base_offset) noexcept
    : batch_(std::move(batch)),
      table_id_(table_id),
      partition_id_(partition_id),
      bucket_id_(bucket_id),
      base_offset_(base_offset) {}

bool ArrowRecordBatch::Available() const { return batch_ != nullptr; }

int64_t ArrowRecordBatch::NumRows() const {
    if (!Available()) return 0;
    return batch_->num_rows();
}

int64_t ArrowRecordBatch::GetTableId() const {
    if (!Available()) return 0;
    return this->table_id_;
}

int64_t ArrowRecordBatch::GetPartitionId() const {
    if (!Available()) return -1;
    return this->partition_id_;
}

int32_t ArrowRecordBatch::GetBucketId() const {
    if (!Available()) return -1;
    return this->bucket_id_;
}

int64_t ArrowRecordBatch::GetBaseOffset() const {
    if (!Available()) return -1;
    return this->base_offset_;
}

int64_t ArrowRecordBatch::GetLastOffset() const {
    if (!Available()) return -1;
    return this->base_offset_ + this->NumRows() - 1;
}

Result LogScanner::PollRecordBatch(int64_t timeout_ms, ArrowRecordBatches& out) {
    if (!Available()) {
        return utils::make_client_error("LogScanner not available");
    }

    auto ffi_result = scanner_->poll_record_batch(timeout_ms);
    auto result = utils::from_ffi_result(ffi_result.result);
    if (!result.Ok()) {
        return result;
    }

    // Convert the FFI Arrow record batches to C++ ArrowRecordBatch objects
    out.batches.clear();
    for (const auto& ffi_batch : ffi_result.arrow_batches.batches) {
        auto* c_array = reinterpret_cast<struct ArrowArray*>(ffi_batch.array_ptr);
        auto* c_schema = reinterpret_cast<struct ArrowSchema*>(ffi_batch.schema_ptr);

        auto import_result = arrow::ImportRecordBatch(c_array, c_schema);
        if (import_result.ok()) {
            auto batch_ptr = import_result.ValueOrDie();
            auto batch_wrapper = std::unique_ptr<ArrowRecordBatch>(new ArrowRecordBatch(
                std::move(batch_ptr), ffi_batch.table_id, ffi_batch.partition_id,
                ffi_batch.bucket_id, ffi_batch.base_offset));
            out.batches.push_back(std::move(batch_wrapper));

            // Free the container structures that were allocated in Rust after successful import
            ffi::free_arrow_ffi_structures(ffi_batch.array_ptr, ffi_batch.schema_ptr);
        } else {
            // Import failed, free the container structures to avoid leaks and return error
            ffi::free_arrow_ffi_structures(ffi_batch.array_ptr, ffi_batch.schema_ptr);

            // Return an error indicating that the import failed
            std::string error_msg =
                "Failed to import Arrow record batch: " + import_result.status().ToString();
            return utils::make_client_error(error_msg);
        }
    }

    return utils::make_ok();
}

}  // namespace fluss
