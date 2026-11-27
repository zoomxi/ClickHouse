#include <Columns/ColumnConst.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Core/Field.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/Statistics/Statistics.h>
#include <Storages/Statistics/StatisticsPartPruner.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <unordered_set>

namespace DB
{

namespace
{

/// Whether every non-default value of `type` compares strictly greater than its column-level default.
bool isDefaultMinimumValue(const DataTypePtr & type)
{
    const WhichDataType which(removeLowCardinality(type));
    return which.isStringOrFixedString() || which.isUUID() || which.isIPv6()
        || which.isArray() || which.isMap();
}

/// Create a Range from a column's statistics estimate for part pruning.
/// Returns std::nullopt when statistics are unavailable or corrupted (whole-universe fallback).
std::optional<Range> createRangeFromEstimate(
    const Estimate & estimate,
    const DataTypePtr & col_type,
    const Field & default_field,
    bool default_is_minimum,
    bool is_size_subcolumn)
{
    if (estimate.rows_count == 0)
        return std::nullopt;

    if (is_size_subcolumn)
    {
        if (!estimate.estimated_default_count.has_value() || *estimate.estimated_default_count > estimate.rows_count)
            return std::nullopt;
        if (*estimate.estimated_default_count == estimate.rows_count)
            return Range(default_field, true, default_field, true);
        if (*estimate.estimated_default_count == 0)
            return Range::createLeftBounded(default_field, /*left_included=*/ false);
        return std::nullopt;
    }

    const bool is_nullable = isNullableOrLowCardinalityNullable(col_type);
    const std::optional<UInt64> & null_count = estimate.estimated_null_count;
    if (null_count.has_value() && *null_count > estimate.rows_count)
        return std::nullopt;

    if (is_nullable && null_count.has_value() && *null_count == estimate.rows_count)
        return Range(POSITIVE_INFINITY, true, POSITIVE_INFINITY, true);

    const bool no_nulls = !is_nullable || (null_count.has_value() && *null_count == 0);

    if (estimate.estimated_min.has_value() && estimate.estimated_max.has_value())
    {
        const Field & min_value = estimate.estimated_min.value();
        const Field & max_value = estimate.estimated_max.value();

        /// min > max is a legacy sentinel pair or corrupted statistics.
        if (min_value > max_value)
            return std::nullopt;

        if (no_nulls)
            return Range(min_value, true, max_value, true);

        /// Nullable column that may contain NULLs: keep the right bound at the NULL sentinel.
        return Range(min_value, true, POSITIVE_INFINITY, true);
    }

    if (is_nullable && no_nulls)
        return Range::createWholeUniverseWithoutNull();

    if (!is_nullable && !default_field.isNull()
        && estimate.estimated_default_count.has_value()
        && *estimate.estimated_default_count <= estimate.rows_count)
    {
        if (*estimate.estimated_default_count == estimate.rows_count)
            return Range(default_field, true, default_field, true);
        if (*estimate.estimated_default_count == 0 && default_is_minimum)
            return Range::createLeftBounded(default_field, /*left_included=*/ false);
    }

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

/// Returns true if `col` is a non-Nullable column with `Basic` statistics whose `default_count`
/// can be mapped to a comparable non-NULL column-level default `Field`.
bool canUseDefaultCountForPartPruning(const ColumnDescription & col, Field & out_default_field)
{
    if (!col.statistics.types_to_desc.contains(StatisticsType::Basic))
        return false;
    if (isNullableOrLowCardinalityNullable(col.type))
        return false;
    const auto inner = removeLowCardinality(col.type);
    if (!inner->isComparable() || isObject(inner))
        return false;
    out_default_field = StatisticsUtils::getColumnDefaultField(col.type);
    return !out_default_field.isNull();
}

/// Parent of virtual `<col>.size` (non-Nullable `String`) or `<col>.size0` (`Array`/`Map`).
/// A physical column with that name wins.
std::optional<String> tryResolveSizeSubcolumnParent(const ColumnsDescription & columns, const String & name)
{
    if (columns.tryGet(name))
        return std::nullopt;

    if (name.ends_with(".size"))
    {
        String parent = name.substr(0, name.size() - 5);
        const auto * parent_col = columns.tryGet(parent);
        if (parent_col && isString(parent_col->type))
            return parent;
    }
    else if (name.ends_with(".size0"))
    {
        String parent = name.substr(0, name.size() - 6);
        const auto * parent_col = columns.tryGet(parent);
        if (parent_col && (isArray(parent_col->type) || isMap(parent_col->type)))
            return parent;
    }
    return std::nullopt;
}

/// If `name` is the virtual `<parent>.null` null-map (from `IS NULL` rewritten by
/// `optimize_functions_to_subcolumns`), return the parent column. A physical column
/// literally named `foo.null` wins. A parent whose nested type owns a `null` subcolumn
/// (e.g. `Nullable(JSON)`, where `json.null` is a `Dynamic` path) is excluded.
std::optional<String> tryResolveNullMapParent(const ColumnsDescription & columns, const String & name)
{
    static constexpr std::string_view suffix = ".null";
    if (!name.ends_with(suffix) || columns.tryGet(name))
        return std::nullopt;

    String parent = name.substr(0, name.size() - suffix.size());
    const auto * parent_col = columns.tryGet(parent);
    if (!parent_col || removeLowCardinalityAndNullable(parent_col->type)->hasSubcolumn("null"))
        return std::nullopt;
    return parent;
}

bool hasBasicStatsOnNullableType(const ColumnDescription & col)
{
    return col.statistics.types_to_desc.contains(StatisticsType::Basic)
        && isNullableOrLowCardinalityNullable(col.type);
}

bool rejectsNullCountWitness(const KeyCondition & key_condition, const DataTypePtr & type)
{
    const DataTypes types{type};
    const bool all_null_possible = key_condition.checkInHyperrectangle(
        Hyperrectangle(1, Range(POSITIVE_INFINITY, true, POSITIVE_INFINITY, true)), types).can_be_true;
    const bool no_null_possible = key_condition.checkInHyperrectangle(
        Hyperrectangle(1, Range::createWholeUniverseWithoutNull()), types).can_be_true;
    return !all_null_possible || !no_null_possible;
}

bool rejectsDefaultCountWitness(
    const KeyCondition & key_condition, const DataTypePtr & type, const Field & default_field, bool default_is_minimum)
{
    const DataTypes types{type};
    if (!key_condition.checkInHyperrectangle(
            Hyperrectangle(1, Range(default_field, true, default_field, true)), types).can_be_true)
        return true;

    if (default_is_minimum
        && !key_condition.checkInHyperrectangle(
            Hyperrectangle(1, Range::createLeftBounded(default_field, /*left_included=*/ false)), types).can_be_true)
        return true;

    return false;
}

bool isConstantZero(const ActionsDAG::Node & node)
{
    if (node.type != ActionsDAG::ActionType::COLUMN || !node.column)
        return false;

    const Field value = (*node.column)[0];
    if (value.getType() == Field::Types::UInt64)
        return value.safeGet<UInt64>() == 0;
    if (value.getType() == Field::Types::Int64)
        return value.safeGet<Int64>() == 0;
    return false;
}

/// Collect top-level `AND` conjuncts testing a column's NULL-ness: a bare `<col>.null`
/// input, `not(<col>.null)`, `<col>.null != 0` / `<col>.null = 0` (the
/// `optimize_functions_to_subcolumns` rewrite of `isNull` / the complementary form),
/// or `isNull(<col>)` / `isNotNull(<col>)` on a bare column.
/// The constructor keeps only columns with `Basic` statistics on a nullable type, so a
/// lone `IS NULL` that cannot prune does not load per-part statistics.
void collectNullPredicates(
    const ActionsDAG::Node & node,
    const ColumnsDescription & columns,
    std::vector<std::pair<String, bool>> & out)
{
    if (node.type == ActionsDAG::ActionType::INPUT)
    {
        if (auto parent = tryResolveNullMapParent(columns, node.result_name))
            out.emplace_back(*parent, true);
        return;
    }

    if (node.type != ActionsDAG::ActionType::FUNCTION || !node.function_base)
        return;

    const auto & name = node.function_base->getName();
    if (name == "and")
    {
        for (const auto * child : node.children)
            collectNullPredicates(*child, columns, out);
        return;
    }

    if (node.children.size() == 1 && node.children.front()->type == ActionsDAG::ActionType::INPUT)
    {
        const String & arg_name = node.children.front()->result_name;
        if (name == "not")
        {
            if (auto parent = tryResolveNullMapParent(columns, arg_name))
                out.emplace_back(*parent, false);
        }
        else if (name == "isNull" || name == "isNotNull")
        {
            out.emplace_back(arg_name, name == "isNull");
        }
        return;
    }

    /// `isNull(col)` with `optimize_functions_to_subcolumns = 1` is rewritten to
    /// `<col>.null != 0`. Only 0 qualifies: a NULL-map byte only has to be non-zero
    /// to mean NULL. `= 0` is the complementary `IS NOT NULL` form.
    if (node.children.size() == 2 && (name == "equals" || name == "notEquals"))
    {
        const ActionsDAG::Node * input = nullptr;
        if (node.children[0]->type == ActionsDAG::ActionType::INPUT && isConstantZero(*node.children[1]))
            input = node.children[0];
        else if (node.children[1]->type == ActionsDAG::ActionType::INPUT && isConstantZero(*node.children[0]))
            input = node.children[1];

        if (input)
        {
            if (auto parent = tryResolveNullMapParent(columns, input->result_name))
                out.emplace_back(*parent, name == "notEquals");
        }
    }
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

    MapWithMemoryTracking<String, DataTypePtr> nullable_only_columns;
    MapWithMemoryTracking<String, ColumnInfo> default_only_columns;

    for (const auto & name : filter_columns)
    {
        if (nan_unsafe_columns.contains(name))
            continue;

        if (const auto * col = columns.tryGet(name))
        {
            if (statisticsHasMinMaxForPartPruning(col->statistics))
            {
                Field default_field;
                canUseDefaultCountForPartPruning(*col, default_field);
                stats_columns.emplace(
                    col->name,
                    ColumnInfo{col->type, col->name, std::move(default_field), isDefaultMinimumValue(col->type), /*is_size_subcolumn=*/ false});
                useless = false;
            }
            else if (hasBasicStatsOnNullableType(*col))
            {
                nullable_only_columns[col->name] = col->type;
            }
            else
            {
                Field default_field;
                if (canUseDefaultCountForPartPruning(*col, default_field))
                {
                    default_only_columns.emplace(
                        col->name,
                        ColumnInfo{col->type, col->name, std::move(default_field), isDefaultMinimumValue(col->type), /*is_size_subcolumn=*/ false});
                }
            }
        }
        else if (auto parent = tryResolveSizeSubcolumnParent(columns, name))
        {
            const auto * parent_col = columns.tryGet(*parent);
            if (parent_col && parent_col->statistics.types_to_desc.contains(StatisticsType::Basic))
            {
                default_only_columns.emplace(
                    name,
                    ColumnInfo{std::make_shared<DataTypeUInt64>(), *parent, Field(UInt64(0)), /*default_is_minimum=*/ true, /*is_size_subcolumn=*/ true});
            }
        }
    }

    std::vector<std::pair<String, bool>> collected_null_predicates;
    collectNullPredicates(*filter_dag.predicate, columns, collected_null_predicates);
    for (const auto & pred : collected_null_predicates)
    {
        const auto * col = columns.tryGet(pred.first);
        if (!col || !hasBasicStatsOnNullableType(*col))
            continue;

        null_predicates.push_back(pred);
        used_column_names.insert(pred.first);
        useless = false;
    }

    /// Keep a nullable-only column only when it can exclude all-NULL or no-NULL parts by itself.
    for (const auto & [col_name, col_type] : nullable_only_columns)
    {
        NamesAndTypesList one_col;
        one_col.emplace_back(col_name, col_type);
        KeyCondition * key_condition = getKeyConditionForEstimates(one_col, /*record_used_columns=*/ false);
        if (!key_condition || !rejectsNullCountWitness(*key_condition, col_type))
            continue;

        stats_columns.emplace(col_name, ColumnInfo{col_type, col_name, Field{}, false, false});
        used_column_names.insert(col_name);
        useless = false;
    }

    /// Keep a default-count-only column or virtual size subcolumn only when it can exclude
    /// all-default or zero-default parts by itself.
    for (auto & [col_name, info] : default_only_columns)
    {
        NamesAndTypesList one_col;
        one_col.emplace_back(col_name, info.type);
        KeyCondition * key_condition = getKeyConditionForEstimates(one_col, /*record_used_columns=*/ false);
        if (!key_condition || !rejectsDefaultCountWitness(*key_condition, info.type, info.default_field, info.default_is_minimum))
            continue;

        used_column_names.insert(info.estimate_column_name);
        stats_columns.emplace(col_name, std::move(info));
        useless = false;
    }
}

NameSet StatisticsPartPruner::getRequiredColumns() const
{
    NameSet result;
    for (const auto & [_, info] : stats_columns)
        result.insert(info.estimate_column_name);
    for (const auto & [col_name, _] : null_predicates)
        result.insert(col_name);
    return result;
}

KeyCondition * StatisticsPartPruner::getKeyConditionForEstimates(const NamesAndTypesList & columns, bool record_used_columns)
{
    const auto column_names = columns.getNames();

    auto it = key_condition_cache.find(column_names);
    if (it != key_condition_cache.end())
        return it->second.get();

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

    if (new_key_condition->alwaysUnknownOrTrue())
    {
        key_condition_cache[column_names] = nullptr;
        return nullptr;
    }

    auto & cached_key_condition = key_condition_cache[column_names];
    cached_key_condition = std::move(new_key_condition);
    auto * key_condition_ptr = cached_key_condition.get();

    if (record_used_columns)
    {
        for (size_t col_idx : key_condition_ptr->getUsedColumns())
        {
            if (col_idx < column_names.size())
            {
                auto col_it = stats_columns.find(column_names[col_idx]);
                used_column_names.insert(col_it != stats_columns.end() ? col_it->second.estimate_column_name : column_names[col_idx]);
            }
        }
    }

    return key_condition_ptr;
}

BoolMask StatisticsPartPruner::checkPartCanMatch(const Estimates & estimates)
{
    for (const auto & [column, is_null] : null_predicates)
    {
        auto est_it = estimates.find(column);
        if (est_it == estimates.end())
            continue;

        const Estimate & estimate = est_it->second;
        if (!estimate.estimated_null_count.has_value() || estimate.rows_count == 0
            || *estimate.estimated_null_count > estimate.rows_count)
            continue;

        if (is_null && *estimate.estimated_null_count == 0)
            return {false, true};
        if (!is_null && *estimate.estimated_null_count == estimate.rows_count)
            return {false, true};
    }

    /// Use only columns that are both in filter and have estimates.
    NamesAndTypesList columns;
    VectorWithMemoryTracking<const ColumnInfo *> column_infos;
    for (const auto & [col_name, info] : stats_columns)
    {
        if (estimates.contains(info.estimate_column_name))
        {
            columns.emplace_back(col_name, info.type);
            column_infos.push_back(&info);
        }
    }

    if (columns.empty())
        return {true, true};

    KeyCondition * key_condition = getKeyConditionForEstimates(columns);
    if (!key_condition)
        return {true, true};

    Hyperrectangle hyperrectangle;
    DataTypes types;
    /// `default_count == 0` and `default` strictly inside `[min, max]`: rows lie in `[min, default)` or `(default, max]`.
    VectorWithMemoryTracking<std::pair<size_t, const Field *>> split_points;

    for (size_t i = 0; i < column_infos.size(); ++i)
    {
        const ColumnInfo & info = *column_infos[i];
        const Estimate & estimate = estimates.at(info.estimate_column_name);
        auto range = createRangeFromEstimate(estimate, info.type, info.default_field, info.default_is_minimum, info.is_size_subcolumn);

        if (range.has_value())
            hyperrectangle.push_back(std::move(*range));
        else if (isNullableOrLowCardinalityNullable(info.type))
            hyperrectangle.emplace_back(Range::createWholeUniverse());
        else
            hyperrectangle.emplace_back(Range::createWholeUniverseWithoutNull());
        types.push_back(info.type);

        /// `-0.0` is not a default (`isDefaultAt` compares bits) but equals `0` in SQL, so floats are not split.
        if (!info.is_size_subcolumn
            && !info.default_field.isNull()
            && !isFloat(removeLowCardinality(info.type))
            && estimate.rows_count > 0
            && estimate.estimated_default_count.has_value()
            && *estimate.estimated_default_count == 0
            && hyperrectangle.back().fullBounded()
            && Range::less(hyperrectangle.back().left, info.default_field)
            && Range::less(info.default_field, hyperrectangle.back().right))
        {
            split_points.emplace_back(i, &info.default_field);
        }
    }

    BoolMask res = key_condition->checkInHyperrectangle(hyperrectangle, types);
    if (!res.can_be_true || split_points.empty())
        return res;

    for (const auto & [col_idx, default_field] : split_points)
    {
        const Range original = hyperrectangle[col_idx];
        hyperrectangle[col_idx] = Range(original.left, original.left_included, *default_field, false);
        if (!key_condition->checkInHyperrectangle(hyperrectangle, types).can_be_true)
        {
            hyperrectangle[col_idx] = Range(*default_field, false, original.right, original.right_included);
            if (!key_condition->checkInHyperrectangle(hyperrectangle, types).can_be_true)
                return {false, true};
        }
        hyperrectangle[col_idx] = original;
    }

    return res;
}

}
