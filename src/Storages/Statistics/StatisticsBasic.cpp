#include <Storages/Statistics/StatisticsBasic.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>

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
    : IStatistics(SingleStatisticsDescription(static_cast<StatisticsType>(3), nullptr, false))
    , min(std::move(min_))
    , max(std::move(max_))
    , null_count(null_count_)
{
}

void StatisticsBasic::build(const ColumnPtr & /*column*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "StatisticsBasic::build not implemented yet");
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

String StatisticsBasic::getNameForLogs() const
{
    return "Basic";
}

bool basicStatisticsValidator(const SingleStatisticsDescription &, const DataTypePtr &)
{
    return false;   /// will be implemented in Task 2
}

StatisticsPtr basicStatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & data_type)
{
    return std::make_shared<StatisticsBasic>(description, data_type);
}

}
