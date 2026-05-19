#pragma once

#include <Core/Field.h>
#include <Storages/Statistics/Statistics.h>

#include <optional>


namespace DB
{

/// Single-value column statistics that subsume the older MinMax type.
/// Stores, depending on the column type:
///   - numeric `min` and `max` (numeric columns, including their Nullable / LowCardinality wrappers)
///   - `null_count` (any Nullable / LowCardinality(Nullable) column)
///   - shortest and longest byte length (`min_length` / `max_length`) of values in String / FixedString columns
///
/// Total row count is stored on the enclosing `ColumnStatistics::rows`. The non-NULL row count is
/// derived as `rows - null_count.value_or(0)`; `Basic` does not store it separately.
class StatisticsBasic : public IStatistics
{
public:
    StatisticsBasic(const SingleStatisticsDescription & description, const DataTypePtr & data_type_);

    /// Test-only constructor: numeric min/max with optional null_count. data_type-less estimation is fine.
    StatisticsBasic(Field min_, Field max_, std::optional<UInt64> null_count_);

    void build(const ColumnPtr & column) override;
    void merge(const StatisticsPtr & other_stats) override;

    void serialize(WriteBuffer & buf) override;
    void deserialize(ReadBuffer & buf, StatisticsFileVersion version) override;

    bool hasMinMax() const { return !min.isNull() && !max.isNull(); }
    const Field & getMin() const { return min; }
    const Field & getMax() const { return max; }

    bool hasNullCount() const { return null_count.has_value(); }
    UInt64 getNullCount() const { return null_count.value_or(0); }

    bool hasStringLengths() const { return min_length.has_value(); }
    UInt64 getMinLength() const { return min_length.value_or(0); }
    UInt64 getMaxLength() const { return max_length.value_or(0); }

    /// Setters used by ColumnStatistics::deserialize when constructing a Basic from V1/V2 legacy bytes.
    void setMinMax(Field min_, Field max_) { min = std::move(min_); max = std::move(max_); }
    void setNullCount(UInt64 null_count_) { null_count = null_count_; }

    std::optional<Float64> estimateLess(const Field & val) const override;
    String getNameForLogs() const override;

private:
    DataTypePtr data_type;          /// removeNullable-d inner type (mirrors today's StatisticsMinMax::data_type)

    Field min;                      /// null Field == "no observation"
    Field max;                      /// null Field == "no observation"
    std::optional<UInt64> null_count;
    std::optional<UInt64> min_length;
    std::optional<UInt64> max_length;
};

bool basicStatisticsValidator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);
StatisticsPtr basicStatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & data_type);

}
