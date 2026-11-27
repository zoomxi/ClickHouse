#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Core/Field.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/Statistics/Statistics.h>
#include <Storages/Statistics/StatisticsPartPruner.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <map>
#include <unordered_set>

namespace DB
{

namespace
{

/// Range from min/max and a NULL count that covers every row. NULL sorts as +inf.
/// No NULLs: [min, max], or (-inf, +inf) when min/max are absent.
/// All NULLs: [+inf, +inf]. Some NULLs: [min, +inf], or nullopt without min/max.
/// nullopt: statistics are missing or corrupt, and the caller must not prune.
std::optional<Range> createRangeFromEstimate(const Estimate & estimate, bool can_contain_null)
{
    if (estimate.rows_count == 0)
        return std::nullopt;

    const std::optional<UInt64> null_count = estimate.null_count_covers_all_rows ? estimate.estimated_null_count : std::nullopt;
    if (null_count.has_value() && *null_count > estimate.rows_count)
        return std::nullopt;

    if (can_contain_null && null_count.has_value() && *null_count == estimate.rows_count)
        return Range(POSITIVE_INFINITY, true, POSITIVE_INFINITY, true);

    const bool no_nulls = !can_contain_null || (null_count.has_value() && *null_count == 0);

    if (estimate.estimated_min.has_value() && estimate.estimated_max.has_value())
    {
        const Field & min_value = estimate.estimated_min.value();
        const Field & max_value = estimate.estimated_max.value();

        /// min > max is a legacy sentinel pair or corrupted statistics.
        if (min_value > max_value)
            return std::nullopt;

        if (no_nulls)
            return Range(min_value, true, max_value, true);

        /// Some NULLs: keep the right bound at the NULL sentinel.
        return Range(min_value, true, POSITIVE_INFINITY, true);
    }

    /// No min/max: a known-zero NULL count is still (-inf, +inf), excluding the NULL sentinel.
    if (can_contain_null && no_nulls)
        return Range::createWholeUniverseWithoutNull();

    return std::nullopt;
}

/// `<col>.null` (`UInt8`) from the parent NULL count. All-NULL is `[1, 255]`: a null-map byte only has to be non-zero.
std::optional<Range> createNullSubcolumnRangeFromEstimate(const Estimate & estimate)
{
    if (estimate.rows_count == 0 || !estimate.null_count_covers_all_rows
        || !estimate.estimated_null_count.has_value() || *estimate.estimated_null_count > estimate.rows_count)
        return std::nullopt;

    if (*estimate.estimated_null_count == 0)
        return Range(Field(UInt64(0)), true, Field(UInt64(0)), true);
    if (*estimate.estimated_null_count == estimate.rows_count)
        return Range(Field(UInt64(1)), true, Field(UInt64(255)), true);
    return std::nullopt;
}

/// Returns true when a column's statistics description can produce a numeric min/max range for
/// part pruning: an explicit `MinMax` statistic, or `Basic` on a numeric/temporal column.
/// Nullable non-numeric `Basic` statistics (NULL count only) are not sufficient on their own:
/// `LIKE '%x%'` on `Nullable(String)` cannot use that count, and treating it as prunable would
/// load per-part statistics with no chance to prune.
bool statisticsHasMinMaxForPartPruning(const ColumnStatisticsDescription & stats_desc)
{
    if (stats_desc.types_to_desc.contains(StatisticsType::MinMax))
        return true;
    if (stats_desc.types_to_desc.contains(StatisticsType::Basic))
        return removeLowCardinalityAndNullable(stats_desc.data_type)->isValueRepresentedByNumber();
    return false;
}

/// Virtual `<parent>.null` from `optimize_functions_to_subcolumns`. A real column `foo.null` wins.
/// A parent whose nested type has its own `null` subcolumn (`Nullable(JSON)`) is not a null map.
std::optional<String> tryResolveNullMapParent(const ColumnsDescription & columns, const String & name)
{
    static constexpr std::string_view suffix = ".null";
    if (!name.ends_with(suffix) || columns.tryGet(name))
        return std::nullopt;

    String parent = name.substr(0, name.size() - suffix.size());
    const auto * parent_col = columns.tryGet(parent);
    if (!parent_col || !isNullableOrLowCardinalityNullable(parent_col->type)
        || removeLowCardinalityAndNullable(parent_col->type)->hasSubcolumn("null"))
        return std::nullopt;
    return parent;
}

bool hasBasicNullCount(const ColumnDescription & col)
{
    return col.statistics.types_to_desc.contains(StatisticsType::Basic) && canContainNull(*col.type);
}

/// True when all-NULL or no-NULL is not a trivial maybe, so the column can prune inside an `OR`.
bool respondsToNullCountWitness(const KeyCondition & key_condition, const DataTypePtr & type)
{
    const DataTypes types{type};
    const auto all_null = key_condition.checkInHyperrectangle(
        Hyperrectangle(1, Range(POSITIVE_INFINITY, true, POSITIVE_INFINITY, true)), types);
    const auto no_null = key_condition.checkInHyperrectangle(
        Hyperrectangle(1, Range::createWholeUniverseWithoutNull()), types);
    return !all_null.can_be_true || !all_null.can_be_false || !no_null.can_be_true || !no_null.can_be_false;
}

/// Functions that negate a comparison, i.e. can be `true` for a `NaN` operand. `NaN` never
/// satisfies a plain comparison (`NaN < c`, `NaN = c`, ... are all `false`), so only a negation can
/// make a floating-point predicate `true` for `NaN`. These are exactly the negating entries of
/// `KeyCondition::atom_map` that produce a prunable `NOT_IN_RANGE` / `NOT_IN_SET` atom (`notLike`
/// and `notEmpty` are omitted: they apply only to String/Array, never to a floating-point column).
bool isNegatingFunction(std::string_view name)
{
    return name == "not"
        || name == "notEquals"
        || name == "notIn"
        || name == "globalNotIn"
        || name == "notNullIn"
        || name == "globalNotNullIn";
}

bool isFloatingPointColumn(const DataTypePtr & type)
{
    return type && isFloat(removeLowCardinalityAndNullable(type));
}

/// Collect floating-point columns that appear anywhere beneath a negating function in the original
/// (non-inverted) filter tree.
///
/// `MinMax`/`Basic` statistics compute min/max via `IColumn::getExtremes`, which deliberately skips
/// `NaN`. So the stored range excludes `NaN`, yet `NaN` sorts after `+inf` and satisfies negated
/// predicates such as `NOT (f < c)` or `f <> c`. Pruning a part by that range would then drop rows
/// that actually match. Statistics-based pruning is therefore disabled for such columns; the range
/// analysis stays sound for a plain comparison, where `NaN` cannot match anyway.
///
/// A positive `IN` also matches `NaN` (`SELECT nan IN (nan)` is `1`), but it needs no exclusion
/// here: the estimates are checked through a `KeyCondition` built with `require_ready_sets`, and
/// `KeyCondition::tryPrepareSetIndexForIn` declines a set atom whose elements contain a `NaN`, so
/// such a predicate becomes unknown for every range-based check at once. Excluding the column
/// instead would also forgo pruning for the common `NaN`-free set.
///
/// The traversal is intentionally conservative: once under a negation it stays under it for the whole
/// subtree, so a column may be excluded even where an even number of negations would cancel out.
/// Excluding a column only forgoes a pruning opportunity, never correctness.
void collectFloatColumnsUnderNegation(
    const ActionsDAG::Node & node,
    bool under_negation,
    NameSet & unsafe_columns,
    std::unordered_set<const ActionsDAG::Node *> & visited_under_negation,
    std::unordered_set<const ActionsDAG::Node *> & visited)
{
    auto & visited_set = under_negation ? visited_under_negation : visited;
    if (!visited_set.insert(&node).second)
        return;

    if (under_negation
        && node.type == ActionsDAG::ActionType::INPUT
        && isFloatingPointColumn(node.result_type))
    {
        unsafe_columns.insert(node.result_name);
    }

    const bool child_under_negation = under_negation
        || (node.type == ActionsDAG::ActionType::FUNCTION && node.function_base
            && isNegatingFunction(node.function_base->getName()));

    for (const auto * child : node.children)
        collectFloatColumnsUnderNegation(*child, child_under_negation, unsafe_columns, visited_under_negation, visited);
}

} /// anonymous namespace

StatisticsPartPruner::StatisticsPartPruner(const StorageMetadataPtr & metadata_, const ActionsDAG::Node & filter_node_, ContextPtr context_)
    : filter_dag(&filter_node_, context_, /* boolean_context */ true)
    , context(context_)
{
    if (!metadata_ || !filter_dag.dag)
        return;

    const auto & columns = metadata_->getColumns();
    Names filter_columns = filter_dag.dag->getRequiredColumnsNames();

    /// Floating-point columns that a negated predicate (`NOT (f < c)`, `f <> c`, `f NOT IN (...)`)
    /// could match via `NaN`. Their min/max statistics exclude `NaN`, so pruning them is unsound.
    NameSet nan_unsafe_columns;
    {
        std::unordered_set<const ActionsDAG::Node *> visited_under_negation;
        std::unordered_set<const ActionsDAG::Node *> visited;
        collectFloatColumnsUnderNegation(filter_node_, /*under_negation=*/ false, nan_unsafe_columns, visited_under_negation, visited);
    }

    std::map<String, DataTypePtr> nullable_only_columns;

    for (const auto & name : filter_columns)
    {
        if (nan_unsafe_columns.contains(name))
            continue;

        if (const auto * col = columns.tryGet(name))
        {
            if (statisticsHasMinMaxForPartPruning(col->statistics))
                stats_column_name_to_type_map[col->name] = col->type;
            else if (hasBasicNullCount(*col))
                nullable_only_columns[col->name] = col->type;
        }
        else if (auto parent = tryResolveNullMapParent(columns, name))
        {
            const auto * parent_col = columns.tryGet(*parent);
            if (parent_col && parent_col->statistics.types_to_desc.contains(StatisticsType::Basic))
            {
                stats_column_name_to_type_map[name] = std::make_shared<DataTypeUInt8>();
                null_subcolumn_to_parent.emplace(name, std::move(*parent));
            }
        }
    }

    for (const auto & [col_name, col_type] : nullable_only_columns)
    {
        NamesAndTypesList one_col;
        one_col.emplace_back(col_name, col_type);
        ActionsDAG actions_dag(one_col);
        auto expression = std::make_shared<ExpressionActions>(std::move(actions_dag));
        KeyCondition probe(
            filter_dag, context, one_col.getNames(), expression,
            /* single_point_ */ false, /* skip_analysis_ */ false, /* require_ready_sets_ */ true);
        probe.relaxAtomsOverNaNHidingColumns(one_col.getTypes());
        if (respondsToNullCountWitness(probe, col_type))
            stats_column_name_to_type_map[col_name] = col_type;
    }

    if (stats_column_name_to_type_map.empty())
        return;

    NamesAndTypesList all_columns;
    for (const auto & [col_name, col_type] : stats_column_name_to_type_map)
        all_columns.emplace_back(col_name, col_type);

    KeyCondition * key_condition = getKeyConditionForEstimates(all_columns, /*record_used_columns=*/ false);
    if (!key_condition)
    {
        stats_column_name_to_type_map.clear();
        null_subcolumn_to_parent.clear();
        return;
    }

    /// Drop keys used only in an unprunable branch (`a.null OR u = 1`) so those statistics are not loaded.
    const auto used_keys = key_condition->getDescription().used_keys;
    if (!used_keys.empty() && used_keys.size() < stats_column_name_to_type_map.size())
    {
        UnorderedSetWithMemoryTracking<String> used_set(used_keys.begin(), used_keys.end());
        std::erase_if(stats_column_name_to_type_map, [&](const auto & item) { return !used_set.contains(item.first); });
        std::erase_if(null_subcolumn_to_parent, [&](const auto & item) { return !used_set.contains(item.first); });
    }

    useless = stats_column_name_to_type_map.empty();
}

const String & StatisticsPartPruner::resolveParentColumn(const String & name) const
{
    auto it = null_subcolumn_to_parent.find(name);
    return it != null_subcolumn_to_parent.end() ? it->second : name;
}

NameSet StatisticsPartPruner::getRequiredColumns() const
{
    NameSet result;
    for (const auto & [col_name, _] : stats_column_name_to_type_map)
        result.insert(resolveParentColumn(col_name));
    return result;
}

KeyCondition * StatisticsPartPruner::getKeyConditionForEstimates(const NamesAndTypesList & columns, bool record_used_columns)
{
    const auto column_names = columns.getNames();

    auto [it, inserted] = key_condition_cache.try_emplace(column_names);
    if (inserted)
    {
        ActionsDAG actions_dag(columns);
        auto expression = std::make_shared<ExpressionActions>(std::move(actions_dag));

        /// Pruning estimates must not run a query pipeline: only state that is already computed may be
        /// read here.
        auto new_key_condition = std::make_unique<KeyCondition>(
            filter_dag, context, column_names, expression,
            /* single_point_ */ false, /* skip_analysis_ */ false, /* require_ready_sets_ */ true);

        /// A statistic's min/max is aggregated the same way `getExtremes` is, so it skips NaN.
        /// Must run before `alwaysUnknownOrTrue()`, whose verdict this can change.
        new_key_condition->relaxAtomsOverNaNHidingColumns(columns.getTypes());

        if (!new_key_condition->alwaysUnknownOrTrue())
            it->second.condition = std::move(new_key_condition);
    }

    auto * key_condition_ptr = it->second.condition.get();
    if (key_condition_ptr && record_used_columns && !it->second.used_columns_recorded)
    {
        for (size_t col_idx : key_condition_ptr->getUsedColumns())
        {
            if (col_idx < column_names.size())
                used_column_names.insert(resolveParentColumn(column_names[col_idx]));
        }
        it->second.used_columns_recorded = true;
    }

    return key_condition_ptr;
}

BoolMask StatisticsPartPruner::checkPartCanMatch(const Estimates & estimates)
{
    NamesAndTypesList columns;
    for (const auto & [col_name, col_type] : stats_column_name_to_type_map)
    {
        if (estimates.contains(resolveParentColumn(col_name)))
            columns.emplace_back(col_name, col_type);
    }

    if (columns.empty())
        return {true, true};

    KeyCondition * key_condition = getKeyConditionForEstimates(columns);
    if (!key_condition)
        return {true, true};

    Hyperrectangle hyperrectangle;
    DataTypes types;
    hyperrectangle.reserve(columns.size());
    types.reserve(columns.size());

    for (const auto & [col_name, col_type] : columns)
    {
        const auto parent_it = null_subcolumn_to_parent.find(col_name);
        const bool is_null_subcolumn = parent_it != null_subcolumn_to_parent.end();
        const Estimate & estimate = estimates.at(is_null_subcolumn ? parent_it->second : col_name);

        auto range = is_null_subcolumn
            ? createNullSubcolumnRangeFromEstimate(estimate)
            : createRangeFromEstimate(estimate, canContainNull(*col_type));

        if (range.has_value())
            hyperrectangle.push_back(std::move(*range));
        else if (canContainNull(*col_type))
            hyperrectangle.emplace_back(Range::createWholeUniverse());
        else
            hyperrectangle.emplace_back(Range::createWholeUniverseWithoutNull());
        types.push_back(col_type);
    }

    return key_condition->checkInHyperrectangle(hyperrectangle, types);
}

}
