#pragma once

#include <Common/MapWithMemoryTracking.h>
#include <Core/Field.h>
#include <Core/Names.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/Statistics/Statistics.h>

namespace DB
{

/// Skips parts whose `basic` (or deprecated `minmax`) column statistics cannot match the filter.
/// Uses min/max, NULL count, and the count of rows equal to the column default.
class StatisticsPartPruner
{
public:
    StatisticsPartPruner(const StorageMetadataPtr & metadata, const ActionsDAG::Node & filter_node, ContextPtr context);

    /// Check if the part can potentially match the filter condition based on statistics.
    /// Returns BoolMask with the same semantics as KeyCondition::checkInHyperrectangle:
    ///   - can_be_true: whether any rows in the part might satisfy the condition
    ///   - can_be_false: whether any rows in the part might not satisfy the condition
    BoolMask checkPartCanMatch(const Estimates & estimates);

    /// Returns true if no columns with usable statistics are used in the filter, then all parts will match.
    [[nodiscard]] bool isUseless() const { return useless; }

    /// Get the list of column names used in the filter condition that have statistics.
    [[nodiscard]] Names getUsedColumns() const { return {used_column_names.begin(), used_column_names.end()}; }

    /// Get the columns whose estimates `checkPartCanMatch` may use.
    [[nodiscard]] NameSet getRequiredColumns() const;

private:
    struct ColumnInfo
    {
        DataTypePtr type;
        /// Column stored in `Estimates`. For `s.size` / `arr.size0` this is the parent column.
        String estimate_column_name;
        /// `IColumn::insertDefault` for a non-Nullable column with `basic` statistics. `NULL` when unused.
        Field default_field;
        bool default_is_minimum = false;
        bool is_size_subcolumn = false;
    };

    /// Get or create a KeyCondition for the given columns, using cache to avoid recreating for each part.
    KeyCondition * getKeyConditionForEstimates(const NamesAndTypesList & columns_and_types, bool record_used_columns = true);

    /// Cache key_condition by column names to avoid recreating them for each part.
    std::unordered_map<Names, std::unique_ptr<KeyCondition>, NamesHash> key_condition_cache;

    std::vector<std::pair<String, bool>> null_predicates;

    const ActionsDAGWithInversionPushDown filter_dag;
    const ContextPtr context;
    MapWithMemoryTracking<String, ColumnInfo> stats_columns;
    NameOrderedSet used_column_names;
    bool useless = true;
};

}
