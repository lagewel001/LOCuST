"""
This module contains the code for the measure F1, dimension F1 and observation F1 metrics.
"""
import sqlglot
from sqlglot.expressions import EQ, In, Column, Literal, Pivot, Where
from typing import Set, Tuple, Union

from s_expression import Expression as SExpression
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


def extract_sql_components(query: str) -> Tuple[Set[str], Set[str]]:
    """Extracts measures and dimension values from a SQL query."""
    measures = set()
    dims = set()
    try:
        parsed = sqlglot.parse_one(query, read='duckdb')
        if not parsed:
            return measures, dims

        # 1. Find filters in WHERE clauses
        for where in parsed.find_all(Where):
            for condition in where.this.find_all(EQ, In):
                if isinstance(condition.this, Column):
                    col_name = condition.this.name
                    if isinstance(condition, EQ) and isinstance(condition.expression, Literal):
                        vals = {condition.expression.this}
                    elif isinstance(condition, In):
                        vals = {lit.this for lit in condition.expressions if isinstance(lit, Literal)}
                    else:
                        continue
                    
                    if col_name == 'Measure':
                        measures.update(vals)
                    else:
                        dims.update(vals)

        # 2. Find PIVOT and UNPIVOT clauses
        for pivot in parsed.find_all(Pivot):
            is_unpivot = pivot.args.get('unpivot', False)
            
            # The structure is Pivot(fields=[In(...)])
            for field in pivot.args.get('fields', []):
                if isinstance(field, In) and isinstance(field.this, Column):
                    col_name = field.this.name
                    vals = {lit.this for lit in field.expressions if isinstance(lit, Literal)}
                    
                    if is_unpivot:
                        if col_name == 'Measure':
                            measures.update(vals)
                    else: # It's a PIVOT
                        dims.update(vals)

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
            measures |= sub_exp.measures
            for dim_group, dim_codes in sub_exp.dimensions:
                dimensions |= dim_codes
            continue

        if hasattr(sub_exp, 'sub_expressions'):
            sub_exps.extend(sub_exp.sub_expressions)
        else:
            sub_exps.append(sub_exp.sub_expression)

    return {str(m) for m in measures}, {str(d) for d in dimensions}


def get_selection_metrics(predicted_query: str, ground_truth_query: str, query_type: str) -> dict:
    """Calculates all selection-based F1 metrics."""
    if query_type == 'sql':
        pred_measures, pred_dims = extract_sql_components(predicted_query)
        gt_measures, gt_dims = extract_sql_components(ground_truth_query)
    elif query_type == 'sexp':
        pred_measures, pred_dims = extract_sexp_components(predicted_query)
        gt_measures, gt_dims = extract_sexp_components(ground_truth_query)
    else:
        raise ValueError(f"Unknown query type: {query_type}")

    measure_f1 = calculate_f1(pred_measures, gt_measures)
    dimension_f1 = calculate_f1(pred_dims, gt_dims)

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
    }
