"""
This module contains the code for the measure F1, dimension F1 and observation F1 metrics.
"""
import regex as re
import sqlglot
from rdflib.namespace import QB, SKOS
from sqlglot.expressions import (EQ, NEQ, In, Column, Literal, Pivot, Where, Table, Like, ILike, RegexpLike, StartsWith,
                                 Not, Paren, And, Or, Expression)
from typing import Callable, Optional, Set, Tuple, Union

from odata_graph import engine
from s_expression import Expression as SExpression, Table as GraphTable, Dimension, uri_to_code
from s_expression.operators import Value
from s_expression.parser import parse, eval


def calculate_f1(predicted: set, ground_truth: set) -> float:
    """Calculates F1 score for two sets."""
    if not ground_truth and not predicted:
        return 1.0
    if not ground_truth or not predicted:
        return 0.0

    tp = len(predicted.intersection(ground_truth))
    fp = len(predicted.difference(ground_truth))
    fn = len(ground_truth.difference(predicted))

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0

    f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
    return f1


def _pattern_matcher(condition: Union[Like, ILike, RegexpLike, StartsWith]) -> Optional[Callable[[str], bool]]:
    """
        Code matcher for a pattern filter on a dimension column, e.g. a whole time/geo level (`RegioS LIKE 'GM%'` or
        `regexp_matches(Perioden, '^\\d{4}KW\\d{2}$')`) instead of enumerated codes. Returns None if the pattern can't
        be evaluated, i.e. when it is no string literal or an invalid regular expression.
    """
    pattern = condition.expression
    if not isinstance(pattern, Literal) or not pattern.is_string:
        return None
    pattern = pattern.this

    try:
        if isinstance(condition, (Like, ILike)):
            # SQL wildcards: % matches any sequence of characters, _ a single character. LIKE matches the whole code
            regex = re.compile(''.join('.*' if c == '%' else '.' if c == '_' else re.escape(c) for c in pattern),
                               flags=re.IGNORECASE if isinstance(condition, ILike) else 0)
            return lambda code: regex.fullmatch(code) is not None
        if isinstance(condition, RegexpLike):
            regex = re.compile(pattern)  # like DuckDB's regexp_matches, the pattern can match any part of the code
            return lambda code: regex.search(code) is not None
        return lambda code: code.startswith(pattern)
    except re.error:
        return None


def _predicate_matcher(predicate: Expression) -> Optional[Tuple[str, Callable[[str], bool]]]:
    """
        The filtered column and a code matcher of a single filter predicate on a column (=, !=, IN or a pattern filter).
        Returns None for any other expression.
    """
    if not isinstance(predicate, (EQ, NEQ, In, Like, ILike, RegexpLike, StartsWith)) or not isinstance(predicate.this, Column):
        return None

    column = predicate.this.name
    if isinstance(predicate, (EQ, NEQ)):
        if not isinstance(predicate.expression, Literal):
            return None
        code = predicate.expression.this
        return column, (lambda c: c == code) if isinstance(predicate, EQ) else (lambda c: c != code)
    if isinstance(predicate, In):
        codes = {lit.this for lit in predicate.expressions if isinstance(lit, Literal)}
        return column, lambda c: c in codes

    matcher = _pattern_matcher(predicate)
    return (column, matcher) if matcher else None


def _compile_filter(condition: Expression, column: str) -> Tuple[Callable[[str], Optional[bool]], bool]:
    """
        Compile a WHERE condition into a three-valued (Kleene) evaluation for the codes of a single column: True or
        False when the condition's outcome only depends on the code, and None (unknown) when it depends on the values of
        other columns, e.g. `RegioS = 'NL01' OR Perioden = '2021JJ00'` for any other RegioS code.

        :returns: tuple containing the evaluation and whether the condition filters the column at all
    """
    while isinstance(condition, Paren):
        condition = condition.this

    if isinstance(condition, Not):
        evaluate, filters_column = _compile_filter(condition.this, column)
        return (lambda code: None if (outcome := evaluate(code)) is None else not outcome), filters_column

    if isinstance(condition, (And, Or)):
        (left, left_filters), (right, right_filters) = (_compile_filter(condition.left, column),
                                                        _compile_filter(condition.right, column))
        decisive = isinstance(condition, Or)  # the outcome that decides the AND (False) or OR (True) on its own

        def evaluate(code: str) -> Optional[bool]:
            outcomes = (left(code), right(code))
            if decisive in outcomes:
                return decisive
            return None if None in outcomes else not decisive

        return evaluate, left_filters or right_filters

    predicate = _predicate_matcher(condition)
    if predicate and predicate[0] == column:
        return predicate[1], True
    return lambda code: None, False  # a predicate on another column (or one that can't be evaluated)


def _filter_matcher(condition: Expression, column: str) -> Optional[Callable[[str], bool]]:
    """
        Compile a WHERE condition into a matcher for the codes of a single column that it selects, following the AND,
        OR and NOT operators between the filter predicates, e.g. all gemeenten except Amsterdam:
        `RegioS LIKE 'GM%' AND RegioS NOT IN ('GM0363')`. A code is selected unless the condition is always false for
        it: predicates on other columns can select rows with any code, e.g. `RegioS = 'NL01' OR Perioden = '2021JJ00'`
        selects all RegioS codes.

        :returns: the code matcher, or None if the condition doesn't filter the column
    """
    evaluate, filters_column = _compile_filter(condition, column)
    if not filters_column:
        return None
    return lambda code: evaluate(code) is not False


def _is_negated(node: Expression, root: Expression) -> bool:
    """Whether a node is negated by an odd number of NOT operators between the node and the root expression."""
    negated = False
    while node is not root and node.parent is not None:
        node = node.parent
        negated ^= isinstance(node, Not)
    return negated


def _table_ids(node: Expression) -> Set[str]:
    """IDs of the tables that are queried from their parquet files within an expression."""
    table_ids = set()
    for table in node.find_all(Table):
        file = table.this.this
        pattern = r"(?<=[\/\\])(?:.(?![\/\\]))+(?=\.parquet)"
        table = re.search(pattern, file)
        if table:
            table_ids.add(table.group())
    return table_ids


def _where_scope(where: Where) -> Optional[Set[str]]:
    """
        The tables filtered by a WHERE clause, i.e. the tables in the FROM and JOIN clauses of its SELECT. In a JOIN over
        subqueries (e.g. `(SELECT * FROM A WHERE ...) JOIN (SELECT * FROM B WHERE ...)`), each WHERE only filters its
        own table. Returns None if the tables can't be determined, e.g. when filtering a CTE.
    """
    select = where.parent
    sources = [select.args.get('from')] + list(select.args.get('joins') or []) if select else []
    return set().union(*(_table_ids(source) for source in sources if source)) or None


def extract_sql_components(query: str) -> Tuple[Set[str], Set[str]]:
    """Extracts measures and dimension values from a SQL query."""
    # Stored all selected and filtered cols and split them using the graph into measures and dimensions
    selected_cols = set()
    pivot_codes = set()
    measures = set()
    dims = set()

    try:
        parsed = sqlglot.parse_one(query, read='duckdb', error_level=sqlglot.ErrorLevel.IGNORE)
        if not parsed:
            return measures, dims

        # 1. Find columns referenced by the SQL globally
        for col in parsed.find_all(Column):
            vals = {col.this.this}
            selected_cols |= vals

        # 2. Find filters in WHERE clauses. The codes they select are resolved per dimension group in step 4; the codes
        #    of the (non-negated) =/IN filters are needed here to find the filtered measures, e.g. `Measure IN (...)`
        conditions = [(where.this, _where_scope(where)) for where in parsed.find_all(Where)]
        for condition, _ in conditions:
            for predicate in condition.find_all(EQ, In):
                if isinstance(predicate.this, Column) and not _is_negated(predicate, condition):
                    if isinstance(predicate, EQ) and isinstance(predicate.expression, Literal):
                        vals = {predicate.expression.this}
                    elif isinstance(predicate, In):
                        vals = {lit.this for lit in predicate.expressions if isinstance(lit, Literal)}
                    else:
                        continue

                    selected_cols |= vals

        # 3. Find PIVOT and UNPIVOT clauses
        for pivot in parsed.find_all(Pivot):
            # The structure is Pivot(fields=[In(...)])
            for field in pivot.args.get('fields', []):
                if isinstance(field, In) and isinstance(field.this, Column):
                    vals = {lit.this for lit in field.expressions if isinstance(lit, Literal)}
                    selected_cols |= vals
                    pivot_codes |= vals

        # 4. Determine for every referenced column if it is a measure or a dimension using the graph
        for table_id in _table_ids(parsed):
            table_graph = engine.get_table_graph(GraphTable(table_id), include_time_geo_dims=True)

            table_measures = set(uri_to_code(m) for m in table_graph.objects(GraphTable(table_id).uri, QB.measure))
            measures |= table_measures & selected_cols

            table_dim_groups = {uri_to_code(d) for d in table_graph.subjects(SKOS.narrower, None)}
            filtered_codes = set()
            for dim_group in table_dim_groups:
                dim_group_children = {uri_to_code(d) for d in table_graph.objects(Dimension(dim_group).uri, SKOS.narrower)}
                matchers = [m for condition, scope in conditions if scope is None or table_id in scope
                            if (m := _filter_matcher(condition, dim_group))]
                if matchers:
                    # A DIM group filtered by the WHERE clauses of this table queries the codes selected by any of them,
                    # even if none match, plus the codes in PIVOT clauses
                    dims |= {code for code in dim_group_children if any(matches(code) for matches in matchers)}
                    dims |= dim_group_children & pivot_codes
                    filtered_codes |= dim_group_children
                elif len(selected_cols & dim_group_children) == 0:
                    # If no specific filtering is done on a selected DIM group, all its children are queried
                    dims |= dim_group_children

            table_dims = {uri_to_code(d) for d in table_graph.objects(GraphTable(table_id).uri, QB.dimension)} - table_dim_groups
            dims |= (table_dims - filtered_codes) & selected_cols
    except Exception:
        pass # Ignore parsing errors

    return measures, dims


def extract_sexp_components(query: Union[str, SExpression]) -> Tuple[Set[str], Set[str]]:
    """Extracts measures and dimension values from an S-expression query."""
    measures = set()
    dimensions = set()
    if isinstance(query, str):
        try:
            query, _ = eval(parse(query), offline=True)
        except:
            return measures, dimensions

    if isinstance(query, Value):
        sub_exps = [query]
    elif hasattr(query, 'sub_expressions'):
        sub_exps = query.sub_expressions
    else:
        sub_exps = [query.sub_expression]

    for sub_exp in sub_exps:  # Get inner VALUE expression(s)
        if isinstance(sub_exp, Value):
            measures |= set(map(str, sub_exp.measures))
            selected_dim_groups = set()
            for dim_group, dim_codes in sub_exp.dimensions:
                selected_dim_groups |= {dim_group}
                dimensions |= set(map(str, dim_codes))

            table_graph = engine.get_table_graph(sub_exp.table, include_time_geo_dims=True)
            table_dim_groups = {uri_to_code(d) for d in table_graph.subjects(SKOS.narrower, None)}
            for dim_group in table_dim_groups:
                # If no specific filtering is done on a selected DIM group, all its children are queried
                dim_group_children = {uri_to_code(d) for d in table_graph.objects(Dimension(dim_group).uri, SKOS.narrower)}
                if len(dimensions & dim_group_children) == 0:
                    dimensions |= dim_group_children

            continue

        if hasattr(sub_exp, 'sub_expressions'):
            sub_exps.extend(sub_exp.sub_expressions)
        else:
            sub_exps.append(sub_exp.sub_expression)

    return {str(m) for m in measures}, dimensions


def get_selection_metrics(predicted_query: str, ground_truth_query: str, query_type: str) -> Tuple[dict, dict]:
    """Calculates all selection-based F1 metrics."""
    if query_type in ['sql', 'pivot_sql']:
        pred_measures, pred_dims = extract_sql_components(predicted_query)
        gt_measures, gt_dims = extract_sql_components(ground_truth_query)
    elif query_type == 'sexp':
        pred_measures, pred_dims = extract_sexp_components(predicted_query)
        gt_measures, gt_dims = extract_sexp_components(ground_truth_query)
    else:
        raise ValueError(f"Unknown query type: {query_type}")

    measure_f1 = calculate_f1(pred_measures, gt_measures)
    dimension_f1 = calculate_f1(pred_dims, gt_dims)

    # For error analysis, check if there are too many/little measures or dimensions
    error_scores = {
        'extra_measures': len(pred_measures - gt_measures),
        'missing_measures': len(gt_measures - pred_measures),
        'extra_dimensions': len(pred_dims - gt_dims),
        'missing_dimensions': len(gt_dims - pred_dims),
        # Raw component counts, so alternative observation F1 definitions can be derived afterwards
        #  (see scripts/metric_agreement_analysis.py) without re-running the evaluation
        'tp_measures': len(pred_measures & gt_measures),
        'tp_dimensions': len(pred_dims & gt_dims),
        'n_pred_measures': len(pred_measures),
        'n_gt_measures': len(gt_measures),
        'n_pred_dimensions': len(pred_dims),
        'n_gt_dimensions': len(gt_dims),
    }

    # If the measures do not match, the observation F1 is 0.
    if measure_f1 <= 0.0:
        observation_f1 = 0.0
    else:
        # If measures match, observation F1 is a combination of measure and dimension matching.
        tp_measure = len(pred_measures.intersection(gt_measures))
        fp_measure = len(pred_measures.difference(gt_measures))
        fn_measure = len(gt_measures.difference(pred_measures))

        tp_dim = len(pred_dims.intersection(gt_dims))
        fp_dim = len(pred_dims.difference(gt_dims))
        fn_dim = len(gt_dims.difference(pred_dims))

        obs_tp = tp_measure + tp_dim
        obs_fp = fp_measure + fp_dim
        obs_fn = fn_measure + fn_dim

        precision = obs_tp / (obs_tp + obs_fp) if (obs_tp + obs_fp) > 0 else 0
        recall = obs_tp / (obs_tp + obs_fn) if (obs_tp + obs_fn) > 0 else 0

        observation_f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0

    return {
        'measure_f1': measure_f1,
        'dimension_f1': dimension_f1,
        'observation_f1': observation_f1,
    }, error_scores
