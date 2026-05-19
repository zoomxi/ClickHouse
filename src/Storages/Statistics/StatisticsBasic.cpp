#include <Storages/Statistics/StatisticsBasic.h>

#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>

#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

StatisticsBasic::StatisticsBasic(const SingleStatisticsDescription & description, const DataTypePtr & data_type_)
    : IStatistics(description)
    , data_type(removeNullable(removeLowCardinality(data_type_)))
{
}

StatisticsBasic::StatisticsBasic(Field min_, Field max_, std::optional<UInt64> null_count_)
    /// `StatisticsType::MinMax` is the slot `Basic` will eventually claim — Task 8 globally renames the enum value to `Basic`.
    : IStatistics(SingleStatisticsDescription(StatisticsType::MinMax, nullptr, false))
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

    /// null_count for Nullable / LowCardinality(Nullable)
    UInt64 nulls_in_chunk = 0;
    bool is_nullable_col = false;

    if (const auto * nullable = checkAndGetColumn<ColumnNullable>(full_column.get()))
    {
        const auto & null_map = nullable->getNullMapData();
        nulls_in_chunk = std::count(null_map.begin(), null_map.end(), 1);
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
            is_nullable_col = true;
        }
    }

    if (is_nullable_col)
    {
        if (!null_count.has_value())
            null_count = 0;
        *null_count += nulls_in_chunk;
    }

    /// String lengths are filled in Task 4.
}

void StatisticsBasic::merge(const StatisticsPtr & /*other_stats*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "StatisticsBasic::merge not implemented yet");
}

void StatisticsBasic::serialize(WriteBuffer & /*buf*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "StatisticsBasic::serialize not implemented yet");
}

void StatisticsBasic::deserialize(ReadBuffer & /*buf*/, StatisticsFileVersion /*version*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "StatisticsBasic::deserialize not implemented yet");
}

std::optional<Float64> StatisticsBasic::estimateLess(const Field & /*val*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "StatisticsBasic::estimateLess not implemented yet");
}

/// TODO: when Tasks 2-7 populate min/max/null_count/string lengths, evolve this to print actual field values
/// (similar to today's `StatisticsMinMax::getNameForLogs` which prints "MinMax: ({min}, {max})").
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
