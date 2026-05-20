#include <Storages/Statistics/StatisticsBasic.h>

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/convertFieldToType.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_STATISTICS;
}

StatisticsBasic::StatisticsBasic(const SingleStatisticsDescription & description, const DataTypePtr & data_type_)
    : IStatistics(description)
    , data_type(removeNullable(removeLowCardinality(data_type_)))
{
}

StatisticsBasic::StatisticsBasic(Field min_, Field max_, std::optional<UInt64> null_count_)
    : IStatistics(SingleStatisticsDescription(StatisticsType::Basic, nullptr, false))
    , min(std::move(min_))
    , max(std::move(max_))
    , null_count(null_count_)
{
}

void StatisticsBasic::build(const ColumnPtr & column)
{
    auto full_column = column->convertToFullColumnIfSparse();

    /// Numeric min/max
    if (data_type->isValueRepresentedByNumber())
    {
        Field min_field;
        Field max_field;
        full_column->getExtremes(min_field, max_field, 0, full_column->size());

        if (!min_field.isNull() && (min.isNull() || min_field < min))
            min = min_field;
        if (!max_field.isNull() && (max.isNull() || max_field > max))
            max = max_field;
    }

    /// `null_count` for `Nullable` / `LowCardinality(Nullable)`. Also records the null-map / LC pointers so the
    /// string-length block below can skip NULL rows without re-detecting the wrapper.
    UInt64 nulls_in_chunk = 0;
    bool is_nullable_col = false;
    const NullMap * null_map_ptr = nullptr;
    const ColumnLowCardinality * lc_with_null = nullptr;

    if (const auto * nullable = checkAndGetColumn<ColumnNullable>(full_column.get()))
    {
        const auto & null_map = nullable->getNullMapData();
        nulls_in_chunk = std::count(null_map.begin(), null_map.end(), 1);
        null_map_ptr = &null_map;
        is_nullable_col = true;
    }
    else if (const auto * lc = checkAndGetColumn<ColumnLowCardinality>(full_column.get()))
    {
        if (lc->nestedIsNullable())
        {
            size_t null_index = lc->getDictionary().getNullValueIndex();
            const auto & indexes = lc->getIndexes();
            for (size_t i = 0; i < indexes.size(); ++i)
                if (indexes.getUInt(i) == null_index)
                    ++nulls_in_chunk;
            lc_with_null = lc;
            is_nullable_col = true;
        }
    }

    if (is_nullable_col)
    {
        if (!null_count.has_value())
            null_count = 0;
        *null_count += nulls_in_chunk;
    }

    /// `String` / `FixedString` min/max byte length. Resolves through `Nullable` / `LowCardinality` wrappers
    /// so iteration is row-aligned regardless of how the column is wrapped.
    if (isStringOrFixedString(data_type))
    {
        const IColumn * row_aligned = full_column.get();
        if (const auto * nullable = checkAndGetColumn<ColumnNullable>(row_aligned))
            row_aligned = &nullable->getNestedColumn();
        /// For `LowCardinality` wrappers (with or without `Nullable`), call `getDataAt` on the LC column
        /// itself — it dispatches through the dictionary index for each row, so iteration is row-aligned.

        const size_t rows = full_column->size();

        const IColumn * lc_indexes = nullptr;
        size_t lc_null_index = 0;
        if (lc_with_null)
        {
            lc_indexes = &lc_with_null->getIndexes();
            lc_null_index = lc_with_null->getDictionary().getNullValueIndex();
        }

        for (size_t i = 0; i < rows; ++i)
        {
            if (null_map_ptr && (*null_map_ptr)[i])
                continue;
            if (lc_indexes && lc_indexes->getUInt(i) == lc_null_index)
                continue;

            /// `getDataAt(i).size()` returns the variable-length payload for `ColumnString` and the
            /// fixed (padded) width for `ColumnFixedString` — both are byte counts, matching the spec's
            /// "shortest/longest value length" semantics.
            UInt64 len = row_aligned->getDataAt(i).size();
            if (!min_length.has_value() || len < *min_length)
                min_length = len;
            if (!max_length.has_value() || len > *max_length)
                max_length = len;
        }
    }
}

void StatisticsBasic::merge(const StatisticsPtr & other_stats)
{
    const auto * other = typeid_cast<const StatisticsBasic *>(other_stats.get());
    if (!other)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot merge StatisticsBasic with a different statistics type");

    /// Numeric min/max
    if (!other->min.isNull() && (min.isNull() || other->min < min))
        min = other->min;
    if (!other->max.isNull() && (max.isNull() || other->max > max))
        max = other->max;

    /// null_count: sum
    if (other->null_count.has_value())
    {
        if (!null_count.has_value())
            null_count = 0;
        *null_count += *other->null_count;
    }

    /// String lengths: min of mins, max of maxes. Assign the optional directly (rather than
    /// dereferencing then re-wrapping) to avoid `bugprone-optional-value-conversion`.
    if (other->min_length.has_value())
    {
        if (!min_length.has_value() || *other->min_length < *min_length)
            min_length = other->min_length;
    }
    if (other->max_length.has_value())
    {
        if (!max_length.has_value() || *other->max_length > *max_length)
            max_length = other->max_length;
    }
}

void StatisticsBasic::serialize(WriteBuffer & buf)
{
    /// Layout (V3 Basic payload):
    ///   [u8 has_minmax]
    ///   if has_minmax:
    ///       [string type_name][Field min][Field max]
    ///   [u8 has_null_count]
    ///   if has_null_count:
    ///       [u64 null_count]
    ///   [u8 has_string_lengths]
    ///   if has_string_lengths:
    ///       [u64 min_length][u64 max_length]
    ///
    /// The outer envelope (version + mask + total_rows + per-stat size prefix) is written by
    /// `ColumnStatistics::serialize`.

    UInt8 has_minmax = hasMinMax() ? 1 : 0;
    writeIntBinary(has_minmax, buf);
    if (has_minmax)
    {
        writeStringBinary(data_type->getName(), buf);
        writeFieldBinary(min, buf);
        writeFieldBinary(max, buf);
    }

    UInt8 has_null_count = hasNullCount() ? 1 : 0;
    writeIntBinary(has_null_count, buf);
    if (has_null_count)
        writeIntBinary(*null_count, buf);

    UInt8 has_string_lengths = hasStringLengths() ? 1 : 0;
    writeIntBinary(has_string_lengths, buf);
    if (has_string_lengths)
    {
        writeIntBinary(*min_length, buf);
        writeIntBinary(*max_length, buf);
    }
}

void StatisticsBasic::deserialize(ReadBuffer & buf, StatisticsFileVersion version)
{
    if (version != StatisticsFileVersion::V3)
        throw Exception(
            ErrorCodes::ILLEGAL_STATISTICS,
            "`StatisticsBasic::deserialize`: only V3 is supported here. Legacy V1/V2 payloads must be "
            "translated by `ColumnStatistics::deserialize`. Got version {}",
            static_cast<UInt16>(version));

    UInt8 has_minmax;
    readIntBinary(has_minmax, buf);
    if (has_minmax)
    {
        String stored_type_name;
        readStringBinary(stored_type_name, buf);
        min = readFieldBinary(buf);
        max = readFieldBinary(buf);

        if (stored_type_name != data_type->getName())
        {
            /// Column type changed since stats were written — convert the stored fields.
            auto stored_type = DataTypeFactory::instance().get(stored_type_name);
            if (!min.isNull())
                min = convertFieldToType(min, *data_type, stored_type.get());
            if (!max.isNull())
                max = convertFieldToType(max, *data_type, stored_type.get());
        }
    }

    UInt8 has_null_count;
    readIntBinary(has_null_count, buf);
    if (has_null_count)
    {
        UInt64 nc;
        readIntBinary(nc, buf);
        null_count = nc;
    }

    UInt8 has_string_lengths;
    readIntBinary(has_string_lengths, buf);
    if (has_string_lengths)
    {
        UInt64 mn;
        UInt64 mx;
        readIntBinary(mn, buf);
        readIntBinary(mx, buf);
        min_length = mn;
        max_length = mx;
    }
}

namespace
{

template <typename T> struct WiderIntType { using type = T; };
template <> struct WiderIntType<UInt64>  { using type = UInt128; };
template <> struct WiderIntType<Int64>   { using type = Int128; };
template <> struct WiderIntType<UInt128> { using type = UInt256; };
template <> struct WiderIntType<Int128>  { using type = Int256; };

template <typename T>
Float64 interpolateLinear(Field val, Field min, Field max, UInt64 row_count)
{
    T v = val.safeGet<T>();
    T mn = min.safeGet<T>();
    T mx = max.safeGet<T>();
    if (v < mn) return 0.0;
    if (v > mx) return static_cast<Float64>(row_count);
    if (mn == mx) return (v == mx) ? static_cast<Float64>(row_count) : 0.0;
    using W = typename WiderIntType<T>::type;
    return static_cast<Float64>(static_cast<W>(v) - static_cast<W>(mn))
         / static_cast<Float64>(static_cast<W>(mx) - static_cast<W>(mn))
         * static_cast<Float64>(row_count);
}

}

std::optional<Float64> StatisticsBasic::estimateLess(const Field & val) const
{
    if (non_null_row_count == 0 || min.isNull() || max.isNull())
        return std::nullopt;

    if (val.getType() == min.getType() && val.getType() == max.getType())
    {
        switch (val.getType())
        {
            case Field::Types::UInt64:  return interpolateLinear<UInt64>(val, min, max, non_null_row_count);
            case Field::Types::Int64:   return interpolateLinear<Int64>(val, min, max, non_null_row_count);
            case Field::Types::UInt128: return interpolateLinear<UInt128>(val, min, max, non_null_row_count);
            case Field::Types::Int128:  return interpolateLinear<Int128>(val, min, max, non_null_row_count);
            case Field::Types::UInt256: return interpolateLinear<UInt256>(val, min, max, non_null_row_count);
            case Field::Types::Int256:  return interpolateLinear<Int256>(val, min, max, non_null_row_count);
            case Field::Types::Float64: return interpolateLinear<Float64>(val, min, max, non_null_row_count);
            default: break;
        }
    }

    auto val_as_float = StatisticsUtils::tryConvertToFloat64(val, data_type);
    auto min_as_float = StatisticsUtils::tryConvertToFloat64(min, data_type);
    auto max_as_float = StatisticsUtils::tryConvertToFloat64(max, data_type);
    if (!val_as_float || !min_as_float || !max_as_float)
        return std::nullopt;
    return interpolateLinear<Float64>(*val_as_float, *min_as_float, *max_as_float, non_null_row_count);
}

String StatisticsBasic::getNameForLogs() const
{
    return "Basic";
}

bool basicStatisticsValidator(const SingleStatisticsDescription &, const DataTypePtr & data_type)
{
    auto inner = removeLowCardinalityAndNullable(data_type);
    if (inner->isValueRepresentedByNumber())
        return true;
    if (isStringOrFixedString(inner))
        return true;
    /// Catches Nullable / LowCardinality(Nullable) wrappers whose inner is neither numeric
    /// nor string (e.g. Nullable(UUID), Nullable(Tuple(...))) — `Basic` still tracks `null_count`
    /// for these in `build()`.
    if (isNullableOrLowCardinalityNullable(data_type))
        return true;
    return false;
}

StatisticsPtr basicStatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & data_type)
{
    return std::make_shared<StatisticsBasic>(description, data_type);
}

}
