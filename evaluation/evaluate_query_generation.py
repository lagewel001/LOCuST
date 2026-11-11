import argparse
import duckdb
import json
import os
import pandas as pd
import sqlglot
import traceback
import warnings
from functools import lru_cache
from time import time
from tqdm import tqdm
from typing import get_args, Tuple, Dict, Union, Literal, Optional, FrozenSet

import config
from evaluation.metrics import (calculate_component_matching, lenient_execution_accuracy,
                                numeric_f1, record_accuracy, get_selection_metrics)
from models.generators.base_generator import BaseGenerator
from odata_graph import engine
from pipeline.db_executor import DBExecutor
from s_expression import Table, Expression, uri_to_code
from s_expression.parser import parse, eval
from utils.answer_comparator import is_equal_frame
from utils.custom_types import (QueryType, UnitCompatibilityError, PartitionCoherenceError, FormatWarning, LLMResponse,
                                LLMAnswerStatus)
from utils.global_functions import load_dataset, load_model_from_path, parse_for_table_id

logger = config.logging.getLogger(__name__)

if config.LANGUAGE == 'en':
    TEST_TABLES_PER_THEME = {
        'Education': ['84312ENG', '84732ENG', '81850ENG', '80393eng', '80509eng', '03753eng', '37931eng', '81491ENG', '80006eng'],
        'Energy': ['81154eng', '85799ENG', '81528ENG', '83376ENG', '83374ENG', '82117ENG', '82538ENG', '84918ENG', '84714ENG', '85899ENG', '82610ENG', '84917ENG', '85666ENG', '85592ENG', '80416ENG', '81567ENG', '82374ENG', '82375ENG', '82369ENG', '82371ENG', '83406ENG', '80101eng', '82379ENG', '71456eng', '70789eng', '71457eng', '83109ENG', '7516eng', '70802eng', '84672ENG', '37621eng', '83325ENG', '80099eng', '00377eng', '71840eng', '72002eng', '37281eng', '70846eng', '80324eng', '83141ENG', '81163eng', '00372eng', '83403ENG', '80100eng', '37215eng', '81606ENG'],
        'International trade': ['82659ENG', '82658ENG', '82616ENG'],
        'Manufacturing': ['7425eng', '81156eng', '81234eng', '85209ENG', '85806ENG', '85798ENG', '85770ENG', '83935ENG', '85771ENG', '83936ENG', '80274eng', '71835eng', '81238eng', '7055eng', '80092eng', '83838ENG', '81810ENG', '81984ENG', '81166eng', '81159eng', '83876ENG', '82444ENG', '37991eng', '37350ENG', '70696eng'],
        'Nature and environment': ['86242ENG', '86053ENG', '7477eng', '80408ENG', '82504ENG', '72002eng', '80370eng', '81010eng', '80447eng', '70946eng', '70947eng', '37221eng', '80448eng', '84735ENG', '83390ENG', '7063eng', '7467eng', '80138eng', '37687eng'],
        'Security and justice': ['82557ENG', '82558ENG', '82559ENG', '82522ENG', '82243ENG', '80045eng', '80426eng', '71482eng', '72007eng', '37957ENG', '37340eng', '37167eng', '37632eng', '37488eng', '37685ENG'],
    }
else:
    TEST_TABLES_PER_THEME = {
        'Energie': ['81156ned', '81154ned', '85799NED', '82538NED', '85677NED', '85337NED', '85080NED','84983NED', '84949NED', '84950NED', '86159NED', '85999NED', '85697NED', '85359NED', '85126NED', '84837NED', '84585NED', '84314NED', '83800NED', '83568NED', '83187NED', '83022NED', '83023NED', '83025NED', '83026NED', '81528NED', '85666NED', '85592NED', '80416ned', '84991NED', '81567NED', '85004NED', '70960ned', '86044NED', '85775NED', '85447NED', '85010NED', '84772NED', '84517NED', '84131NED', '85005NED', '82003NED', '70897NED', '81309NED', '37359', '81163ned', '70662ned', '71840ned', '70663ned', '70780ned', '72002ned', '70914ned', '84672NED', '7520', '7522', '37333', '37237', '37215', '7521', '7310SLEN', '07144', '07145', '37291', '7444', '83878NED', '83882NED', '7443', '37207', '7448', '82374NED', '7442', '7445', '82375NED', '7514', '7447', '82369NED', '7523', '82371NED', '7525', '7524', '7454', '70135ned', '80382ned', '83406NED', '80101ned', '82379NED', '70789ned', '83109NED', '7516', '71457ned', '71456ned', '82380NED', '70802ned', '84783NED', '84518NED', '84130NED', '70949ned', '71458ned', '37880klb', '71556ned'],
        'Industrie': ['81234ned', '85209NED', '80728ned', '81156ned', '85798NED', '7425ZUIV', '85806NED', '85363NED', '83115NED', '85770NED', '83935NED', '85771NED', '83936NED', '81238ned', '71835ned', '80324ned', '37215', '7055', '37759', '70661NED', '70035NED', '81166ned', '71847ned', '70036NED', '81984NED', '80273ned', '83876NED', '83838NED', '81810NED', '37350', '37991pvr', '80111ned', '37154', '37569', '37971lix', '82113NED', '81460NED', '81975NED', '81974NED', '71933ned', '70753ned', '71250NED', '70037PRC'],
        'Internationale handel': ['82659NED', '82658NED'],
        'Natuur en milieu': ['37687wat', '70695NED', '80182ned', '7219WEER', '37368', '7041BBOD'],
        'Onderwijs': ['86223NED', '86087NED', '84973NED', '83861NED', '84732NED', '84312NED', '85356NED', '85353NED', '86052NED', '85740NED', '85372NED', '85051NED', '84773NED', '84947NED', '85702NED', '85525NED', '85701NED', '85489NED', '85490NED', '71478ned', '84780NED', '84274NED', '85907NED', '83966NED', '80393ned', '80509ned', '85453NED', '85313NED', '84337NED', '85834NED', '80384ned', '37220', '03753', '71517ned', '70896ned', '71562ned', '83873NED', '85511NED', '85820NED', '85214NED', '84972NED', '84696NED', '84455NED', '83969NED', '83663NED', '82859NED', '82252NED', '71822ned', '71229ned', '71042ned', '80171ned', '71043NED', '71890ned', '70637ned', '81869NED', '84311NED', '7523', '71796ned', '71825ned', '80468ned', '80284ned', '71493ned', '70134ned', '71450ned', '83295NED', '83296NED', '7265ONDW', '71294ned', '83393NED', '81788NED', '37846sol', '37746sol', '71202ned', '71247ned', '71199ned', '71073ned', '71074ned', '71201ned', '71200ned', '71054ned', '70962ned', '71172ned', '71113ned', '83894NED', '83893NED', '37379ou', '71535ned', '70901ned', '70222ned', '81491ned', '70903ned', '70902ned', '80197ned', '82299NED', '82298NED', '85184NED', '82816NED', '82275NED', '83324NED', '82123NED', '83613NED', '70762ned', '70761ned', '81473NED', '70630ned', '37182'],
        'Veiligheid en recht': ['82558NED', '82557NED', '82559NED', '37900']
    }

def execute_query(query: str, query_type: QueryType) -> Tuple[Union[str, Expression], pd.DataFrame, float]:
    """Execute the given query and calculate its execution time"""
    if query_type == 'sexp':
        t0 = time()
        query, answer = eval(parse(query), offline=True)
        execution_time = time() - t0
    elif query_type in ['sql', 'pivot_sql']:
        db = DBExecutor(
            tables=[Table(t) for t in parse_for_table_id(query, query_type)],
            measures=set(),
            dims=set()
        )
        pivot = query_type == 'pivot_sql'
        answer, _, _, execution_time = db.query_db(query, friendly_labels=False, pivot=pivot)
    else:
        raise ValueError(f"Unknown query type: {query_type}")

    return query, answer, execution_time


def _pivot_collapses_axis(expression: sqlglot.exp.Expression, pivot: sqlglot.exp.Pivot) -> bool:
    """
        Whether a PIVOT aggregates the values instead of only reshaping them, i.e. whether it collapses an axis that is
        neither pivoted into columns nor grouped into rows: a dimension that is filtered on multiple codes or selected
        in the source of the pivot, or multiple (unpivoted) measures.
    """
    kept = {f.this.name for f in pivot.args.get('fields') or [] if isinstance(f, sqlglot.exp.In)}
    if pivot.args.get('group'):
        kept |= {c.name for c in pivot.args['group'].expressions}

    single_code, multi_code = set(), set()
    for where in expression.find_all(sqlglot.exp.Where):
        for cond in where.find_all(sqlglot.exp.EQ, sqlglot.exp.In, sqlglot.exp.Like, sqlglot.exp.RegexpLike):
            if not isinstance(cond.this, sqlglot.exp.Column):
                continue
            if isinstance(cond, sqlglot.exp.EQ) or (isinstance(cond, sqlglot.exp.In) and len(cond.expressions) == 1):
                single_code.add(cond.this.name)
            else:
                multi_code.add(cond.this.name)

    source = pivot.parent.this if isinstance(pivot.parent, sqlglot.exp.Subquery) else None
    selected = {s.alias_or_name for s in source.selects} if isinstance(source, sqlglot.exp.Select) else set()
    if not pivot.args.get('group'):
        # Without an explicit GROUP BY, DuckDB implicitly groups by all selected columns that are neither pivoted nor
        # aggregated (e.g. Measure in the ARG_MAX(Measure, Value) of a MAX over measures)
        aggregated = {c.name for e in pivot.args.get('expressions') or [] for c in e.find_all(sqlglot.exp.Column)}
        kept |= selected - aggregated

    # A dimension filtered on a single code can't be aggregated over, even if it isn't kept
    collapsed_dims = (multi_code | (selected - single_code)) - kept - {'Measure', 'Value', '*'}

    unpivoted_measures = [f for p in expression.find_all(sqlglot.exp.Pivot) if p.args.get('unpivot')
                          for f in p.args.get('fields') or [] if isinstance(f, sqlglot.exp.In)]
    collapsed_measures = 'Measure' not in kept and ('Measure' in multi_code or
                                                    any(len(f.expressions) > 1 for f in unpivoted_measures))
    return bool(collapsed_dims) or collapsed_measures


def get_question_type(query: str, query_type: QueryType) -> Optional[str]:
    """Determines the question type from an S-expression."""
    if query_type == 'sexp':
        # The order of bins matters for questions with multiple aggregations
        bins = ['PROP', 'SUM', 'AVG', 'MIN', 'MAX']

        has_join = 'JOIN' in query

        for agg in bins:
            if agg in query:
                if has_join:
                    return 'AGGJOIN'
                return agg

        if has_join:
            return 'JOIN'

        return 'VALUE'
    elif query_type in ['sql', 'pivot_sql']:
        try:
            expression = sqlglot.parse_one(query, read='duckdb')
        except Exception as e:
            return None

        # PROP by checking for a division.
        # A division inside an aggregation is no proportion, e.g. averaging over measures: AVG((A + B) / 2)
        if any(div.find_ancestor(sqlglot.exp.AggFunc) is None for div in expression.find_all(sqlglot.exp.Div)):
            return 'PROP'

        # JOIN / AGGJOIN
        is_join = bool(expression.find((
            sqlglot.exp.Join, sqlglot.exp.Union, sqlglot.exp.Intersect, sqlglot.exp.Except
        )))
        if is_join:
            is_aggjoin = any(
                isinstance(s.unalias(), (sqlglot.exp.Sum, sqlglot.exp.Avg, sqlglot.exp.Min, sqlglot.exp.Max))
                for s in expression.selects
            )
            if is_aggjoin:
                return 'AGGJOIN'
            return 'JOIN'

        # ARGMIN/ARGMAX by ranking the values, either with a window function (the non-pivoted MIN/MAX) or ORDER BY + LIMIT
        ranking_order = None
        for window in expression.find_all(sqlglot.exp.Window):
            is_rank = (isinstance(window.this, sqlglot.exp.RowNumber) or
                       (isinstance(window.this, sqlglot.exp.Anonymous) and window.this.name.upper() in ('RANK', 'DENSE_RANK')))
            if is_rank and window.args.get('order'):
                ranking_order = window.args['order'].expressions[0]
                break
        if ranking_order is None and expression.args.get('limit') and expression.args.get('order'):
            ranking_order = expression.args['order'].expressions[0]
        if ranking_order is not None:
            return 'MAX' if ranking_order.args.get('desc') else 'MIN'

        # Check aggregator in PIVOT to make distinction between VALUE and simple aggregators
        pivot = next((p for p in expression.find_all(sqlglot.exp.Pivot) if not p.args.get('unpivot')), None)
        if pivot:
            agg_expr = pivot.args.get('expressions')[0].unalias()
            if isinstance(agg_expr, (sqlglot.exp.Sum, sqlglot.exp.Avg, sqlglot.exp.Min, sqlglot.exp.Max)):
                if not _pivot_collapses_axis(expression, pivot):
                    return 'VALUE'  # every pivot cell holds a single value, so the query only selects values
                return {sqlglot.exp.Sum: 'SUM', sqlglot.exp.Avg: 'AVG',
                        sqlglot.exp.Min: 'MIN', sqlglot.exp.Max: 'MAX'}[type(agg_expr)]

        aggs = expression.find_all(sqlglot.exp.AggFunc)
        for agg_expr in aggs:
            if isinstance(agg_expr, sqlglot.exp.Sum):
                return 'SUM'
            if isinstance(agg_expr, sqlglot.exp.Avg):
                return 'AVG'
            if isinstance(agg_expr, sqlglot.exp.Min):
                return 'MIN'
            if isinstance(agg_expr, sqlglot.exp.Max):
                return 'MAX'

        return 'VALUE'
    else:
        raise ValueError(f"Unknown query type: {query_type}")


@lru_cache(maxsize=None)
def _execute_ground_truth_cached(query: str, query_type: QueryType) -> pd.DataFrame:
    """The gold query of a question is identical across runs, so its result is executed once and reused."""
    _, answer, _ = execute_query(query, query_type)
    return answer


def execute_ground_truth(query: str, query_type: QueryType) -> pd.DataFrame:
    """Cached gold-query result. A copy is returned so callers cannot mutate the cached frame."""
    return _execute_ground_truth_cached(query, query_type).copy()


@lru_cache(maxsize=None)
def non_observation_columns(table_id: str) -> FrozenSet[str]:
    """
        The columns of a table that hold no observations: its Id and dimension columns. Dimension codes can be numeric
        (e.g. SBI codes or ages are stored as integers), which the numeric metrics must not count as answer values.

        A dimension column is a dimension group of the table: a qb:dimension without a parent (skos:broader) or scheme
        (skos:inScheme), as opposed to the dimension codes, some of which are column names in pre-pivoted tables. Codes
        that are a measure of the table as well are no dimension column.
    """
    uri = Table(table_id).uri
    dimension_groups = {uri_to_code(binding['dim']['value']) for binding in engine.select(f"""
        PREFIX qb: <http://purl.org/linked-data/cube#>
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
        SELECT DISTINCT ?dim WHERE {{
            <{uri}> qb:dimension ?dim .
            FILTER NOT EXISTS {{ ?dim skos:broader ?parent }}
            FILTER NOT EXISTS {{ ?dim skos:inScheme ?scheme }}
        }}
    """)}
    measures = {uri_to_code(binding['measure']['value']) for binding in engine.select(f"""
        PREFIX qb: <http://purl.org/linked-data/cube#>
        SELECT ?measure WHERE {{ <{uri}> qb:measure ?measure }}
    """)}
    columns = {'Id'} | (dimension_groups - measures)
    # A JOIN suffixes a column that both tables provide with _B for the second table (s_expression/operators/join.py),
    # and MIN/MAX return the code of their selector as "MIN[<dimension>]" (s_expression/simple_aggregator.py)
    return frozenset(columns | {f'{column}_B' for column in columns} |
                     {f'{operator}[{column}]' for operator in ('MIN', 'MAX') for column in columns})


def column_aliases(query: str, columns: FrozenSet[str]) -> FrozenSet[str]:
    """
        The names a SQL query gives to the given columns, followed through CTEs and subqueries: a plain alias
        (SectorBranchesSIC2008 AS Sector) or the code an ARG_MIN/ARG_MAX returns (ARG_MIN(Regions, Value) AS "MIN[Region]").
    """
    try:
        expression = sqlglot.parse_one(query, read='duckdb')
    except sqlglot.errors.SqlglotError:
        return frozenset()

    names = set(columns)
    changed = True
    while changed:  # an alias can be aliased again, in any order of the traversal
        changed = False
        for alias in expression.find_all(sqlglot.exp.Alias):
            source = alias.this.this if isinstance(alias.this, (sqlglot.exp.ArgMin, sqlglot.exp.ArgMax)) else alias.this
            if isinstance(source, sqlglot.exp.Column) and source.name in names and alias.alias not in names:
                names.add(alias.alias)
                changed = True
    return frozenset(names - columns)


def theme_of(table_ids) -> Optional[str]:
    """The benchmark theme of the first table that belongs to one."""
    for table_id in table_ids:
        for theme, ids in TEST_TABLES_PER_THEME.items():
            if table_id in ids:
                return theme
    return None


def evaluate_query_generation(
        model_path: str,
        dataset_path: str,
        output_path: str,
        query_type: QueryType,
        task: Literal['end-to-end', 'query-only'],
        model_kwargs: Dict = None,
        per_question_path: Optional[str] = None,
        metrics_only: bool = False,
        limit: Optional[int] = None) -> Dict:
    """
        Evaluates query generation performance.

        :param model_path: path to the model
        :param dataset_path: path to the dataset file
        :param output_path: path to the generated queries from the model to. If a generated query is present for all
                            QA-paris in dataset_path, this can also be used for just re-calculating the metrics.
        :param query_type: type of query to evaluate ('sexp', 'sql' or 'pivot_sql')
        :param task: type of task to evaluate ('end-to-end' or 'query-only')
        :param model_kwargs: optional keyword arguments for model instantiation
        :param per_question_path: optional path to write one JSONL record per question to, holding every metric of
                                  that question. Used for metric analyses (scripts/metric_agreement_analysis.py).
        :param metrics_only: only recalculate the metrics of the queries already in output_path, without loading the
                             model. Fails when a question has no generated query yet.
        :param limit: only evaluate the first N questions of the dataset (for debugging)
        :return: dictionary containing the used model, dataset and metrics
     """
    if model_kwargs is None:
        model_kwargs = {}
    dataset = load_dataset(dataset_path)
    if limit:
        dataset = dataset[:limit]

    # Get previously generated results
    generated_answers = {}
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    if os.path.exists(output_path):
        with open(output_path, "r") as f:
            generated_answers = json.load(f)

    model: Optional[BaseGenerator] = None
    if metrics_only:
        missing = [item.question for item in dataset if not generated_answers.get(item.question, {}).get('query')]
        logger.info(f"Recalculating metrics for {len(dataset) - len(missing)}/{len(dataset)} cached queries "
                    f"({len(missing)} question(s) without a generated query).")
    else:
        model = load_model_from_path(model_path, **model_kwargs)

    per_question_records = []
    run_meta = {'model_path': model_path, 'task': task, 'query_type': query_type, 'language': config.LANGUAGE,
                'run': os.path.splitext(os.path.basename(output_path))[0]}

    QUESTION_TYPES = ['VALUE', 'SUM', 'AVG', 'MIN', 'MAX', 'JOIN', 'PROP', 'AGGJOIN']

    input_token_count = 0
    output_token_count = 0
    exact_match = 0
    rec_accuracy = 0
    num_f1 = 0
    execution_accuracy = {"strict": 0, "lenient": 0}
    component_scores = {'select_f1': 0.0, 'where_f1': 0.0, 'groupby_f1': 0.0, 'orderby_f1': 0.0, 'pivot_f1': 0.0}
    selection_scores = {
        "lenient": {'measure_f1': 0.0, 'dimension_f1': 0.0, 'observation_f1': 0.0},
        "strict": {'measure_f1': 0.0, 'dimension_f1': 0.0, 'observation_f1': 0.0}
    }
    error_scores = {
        'syntax_error': {
            'by_type': {q_type: 0 for q_type in QUESTION_TYPES},
            'by_theme': {theme: 0 for theme in TEST_TABLES_PER_THEME.keys()}
        },
        'formatting_error': {
            'by_type': {q_type: 0 for q_type in QUESTION_TYPES},
            'by_theme': {theme: 0 for theme in TEST_TABLES_PER_THEME.keys()}
        },
        'wrong_agg_func': {q_type: 0 for q_type in QUESTION_TYPES},
        'table_mismatch': 0,
        'unit_compatability_errors': 0,
        'coherence_errors': 0,
        'extra_measures': 0, 'missing_measures': 0,
        'extra_dimensions': 0, 'missing_dimensions': 0
    }
    if query_type == 'pivot_sql':
        error_scores['missing_pivot'] = 0
    # Questions without a generated query stay in the denominator and score 0 on every metric
    total = len(dataset)
    empty_queries = 0

    metrics_by_type = {
        q_type: {
            "count": 0,
            "empty_queries": 0,
            "exact_match": 0,
            "record_accuracy": 0,
            "numeric_f1": 0,
            "execution_accuracy": {"strict": 0, "lenient": 0},
            "selection_scores": {
                "lenient": {'measure_f1': 0.0, 'dimension_f1': 0.0, 'observation_f1': 0.0},
                "strict": {'measure_f1': 0.0, 'dimension_f1': 0.0, 'observation_f1': 0.0}
            }
        } for q_type in QUESTION_TYPES
    }

    for item in tqdm(dataset, desc=f"Evaluating query generation (Task: {task})", bar_format=config.TQDM_BAR_FMT):
        question = item.question
        ground_truth_query = item[query_type]
        golden_tables = {t_id: {} for t_id in parse_for_table_id(ground_truth_query, query_type)}

        response = None
        if question in generated_answers:
            response = LLMResponse(**generated_answers[question], status=LLMAnswerStatus.OK, remarks="")
            if response.query == "":
                response = None

        if response is None and metrics_only:
            # Nothing was generated for this question; count it as an empty answer instead of generating one now.
            # Keep the tokens of a failed attempt (e.g. a truncated response), so the token averages include them.
            cached = generated_answers.get(question, {})
            response = LLMResponse(query="", input_token_count=cached.get('input_token_count', 0),
                                   output_token_count=cached.get('output_token_count', 0),
                                   status=LLMAnswerStatus.OK, remarks="no cached query")
        elif response is None:
            response = model.generate_query(
                question,
                retrieved_tables=golden_tables if task == 'query-only' else None,
                query_type=query_type
            )

            generated_answers[question] = response.to_dict()
            with open(output_path, "w") as f:
                json.dump(generated_answers, f, indent=4)

        predicted_query = response.query
        input_token_count += response.input_token_count
        output_token_count += response.output_token_count

        # Determine question type from s-expression
        q_type = get_question_type(item.sexp, query_type='sexp')
        metrics_by_type[q_type]["count"] += 1

        if not predicted_query:
            empty_queries += 1
            metrics_by_type[q_type]["empty_queries"] += 1
            per_question_records.append({**run_meta, 'question': question, 'empty_query': True,
                                         'question_type': q_type,
                                         'theme': theme_of(golden_tables), 'golden_tables': list(golden_tables)})
            continue

        # Exact Match
        if query_type in ['sql', 'pivot_sql']:
            try:
                normalized_predicted = sqlglot.transpile(predicted_query, read='duckdb', pretty=False)[0]
                normalized_ground_truth = sqlglot.transpile(ground_truth_query, read='duckdb', pretty=False)[0]
            except Exception:
                normalized_predicted = ' '.join(predicted_query.split())
                normalized_ground_truth = ' '.join(ground_truth_query.split())
        else:  # sexp
            normalized_predicted = ' '.join(predicted_query.replace('(', ' ( ').replace(')', ' ) ').split())
            normalized_ground_truth = ' '.join(ground_truth_query.replace('(', ' ( ').replace(')', ' ) ').split())

        em_reward = 1 if normalized_predicted == normalized_ground_truth else 0
        exact_match += em_reward
        metrics_by_type[q_type]["exact_match"] += em_reward

        syntax_error = False
        format_error = False
        coherence_error = False
        rec_acc_reward = 0
        num_f1_reward = 0
        exec_acc_reward = 0
        lenient_exec_reward = 0
        try:
            # Check for query validity before execution
            if query_type == 'sexp':
                parse(predicted_query)
            elif query_type in ['sql', 'pivot_sql']:
                sqlglot.parse_one(predicted_query, read='duckdb')

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always", FormatWarning)
                _, predicted_result, _ = execute_query(predicted_query, query_type)

                if any(issubclass(warn.category, FormatWarning) for warn in w):
                    # Formatting from retrieved data is wrong (usually indicates no measure was returned)
                    format_error = True
                    error_scores['formatting_error']['by_type'][q_type] += 1
                    for table_id in golden_tables:
                        for theme, ids in TEST_TABLES_PER_THEME.items():
                            if table_id in ids:
                                error_scores['formatting_error']['by_theme'][theme] += 1
                                break

            ground_truth_result = execute_ground_truth(ground_truth_query, query_type)

            # Record Accuracy
            rec_acc_reward = record_accuracy(ground_truth_result, predicted_result)

            # Numeric F1, over the observations only
            ignore_columns = frozenset().union(*(non_observation_columns(t) for t in
                                                 set(golden_tables) | set(parse_for_table_id(predicted_query, query_type))))
            if query_type in ['sql', 'pivot_sql']:
                ignore_columns |= (column_aliases(ground_truth_query, ignore_columns) |
                                   column_aliases(predicted_query, ignore_columns))
            num_f1_reward = numeric_f1(ground_truth_result, predicted_result, ignore_columns=ignore_columns)

            # Execution Accuracy
            # Checking frame equality clogs the system memory for enormous tables. Trust me, the EX is 0 if you return more than 1 000 rows
            if len(predicted_result) < 1000 and is_equal_frame(ground_truth_result, predicted_result):
                exec_acc_reward = 1

            if len(predicted_result) < 1000:
                lenient_exec_reward = lenient_execution_accuracy(ground_truth_result, predicted_result)
        except UnitCompatibilityError:
            # Units are not compatible
            error_scores['unit_compatability_errors'] += 1
        except PartitionCoherenceError:
            # The S-expression aggregates over an incoherent code-set (mixed partitions/levels, or a total with its parts)
            coherence_error = True
            error_scores['coherence_errors'] += 1
        except duckdb.IOException:
            # Table not found, will be added to the table_mismatch later
            pass
        except (duckdb.BinderException, duckdb.ParserException, sqlglot.errors.SqlglotError, SyntaxError) as e:
            # Syntactical errors
            syntax_error = True
            error_scores['syntax_error']['by_type'][q_type] += 1
            for table_id in golden_tables:
                for theme, ids in TEST_TABLES_PER_THEME.items():
                    if table_id in ids:
                        error_scores['syntax_error']['by_theme'][theme] += 1
                        break
        except (RuntimeError, Exception) as e:
            with open('evaluation/errors.txt', 'a') as f:
                f.write(f"{predicted_query}\n"
                        f"{e}\n"
                        f"{traceback.format_exc()}\n")
            print(f"Caught unexpected error: {e}")

        rec_accuracy += rec_acc_reward
        metrics_by_type[q_type]['record_accuracy'] += rec_acc_reward

        num_f1 += num_f1_reward
        metrics_by_type[q_type]['numeric_f1'] += num_f1_reward

        execution_accuracy['strict'] += exec_acc_reward
        metrics_by_type[q_type]['execution_accuracy']['strict'] += exec_acc_reward

        execution_accuracy['lenient'] += lenient_exec_reward
        metrics_by_type[q_type]['execution_accuracy']['lenient'] += lenient_exec_reward

        # Component Matching (SQL only)
        if query_type in ['sql', 'pivot_sql']:
            comp_scores = calculate_component_matching(predicted_query, ground_truth_query)
            for key, value in comp_scores.items():
                component_scores[key] += value

        # Selection-based F1 scores (lenient: always calculated, strict: zero on syntax error)
        sel_scores, sel_errors = get_selection_metrics(predicted_query, ground_truth_query, query_type)
        for key, value in sel_scores.items():
            selection_scores['lenient'][key] += value
            metrics_by_type[q_type]['selection_scores']['lenient'][key] += value
            if not syntax_error:
                selection_scores['strict'][key] += value
                metrics_by_type[q_type]['selection_scores']['strict'][key] += value

        error_scores['extra_measures'] += 1 if sel_errors['extra_measures'] > 0 else 0
        error_scores['missing_measures'] += 1 if sel_errors['missing_measures'] > 0 else 0
        error_scores['extra_dimensions'] += 1 if sel_errors['extra_dimensions'] > 0 else 0
        error_scores['missing_dimensions'] += 1 if sel_errors['missing_dimensions'] > 0 else 0

        # Error analysis for queries with correct syntax
        predicted_tables = []
        table_mismatch = False
        if not syntax_error:
            predicted_tables = parse_for_table_id(predicted_query, query_type)
            table_mismatch = set(golden_tables) != set(predicted_tables)
            if table_mismatch:
                error_scores['table_mismatch'] += 1

            if query_type == 'pivot_sql' and 'PIVOT' in ground_truth_query and 'PIVOT' not in predicted_query:
                error_scores['missing_pivot'] += 1

            # Determine if the model attempted to answer the questions using the correct 'type' of query
            predicted_agg_func = get_question_type(predicted_query, query_type=query_type)
            if (exec_acc_reward != 1. and rec_acc_reward != 1. and sel_scores['observation_f1'] != 1. and
                    predicted_agg_func is not None and q_type != predicted_agg_func):
                error_scores['wrong_agg_func'][q_type] += 1

        per_question_records.append({
            **run_meta, 'question': question, 'empty_query': False, 'question_type': q_type,
            'theme': theme_of(golden_tables), 'golden_tables': list(golden_tables),
            'predicted_tables': predicted_tables, 'syntax_error': syntax_error, 'format_error': format_error,
            'coherence_error': coherence_error,
            'table_mismatch': table_mismatch, 'exact_match': em_reward, 'record_accuracy': rec_acc_reward,
            'numeric_f1': num_f1_reward, 'ex_strict': exec_acc_reward, 'ex_lenient': lenient_exec_reward,
            **{key: value for key, value in sel_scores.items()}, **sel_errors,
        })

    avg_input_token_count = input_token_count / total if total > 0 else 0
    avg_output_token_count = output_token_count / total if total > 0 else 0
    em_accuracy = exact_match / total if total > 0 else 0
    rec_accuracy = (rec_accuracy / total) if total > 0 else 0
    num_f1_accuracy = (num_f1 / total) if total > 0 else 0
    ex_accuracy = {
        "strict": (execution_accuracy['strict'] / total) if total > 0 else 0,
        "lenient": (execution_accuracy['lenient'] / total) if total > 0 else 0,
    }
    avg_selection_scores = {
        "strict": {key: value / total for key, value in selection_scores['strict'].items()},
        "lenient": {key: value / total for key, value in selection_scores['lenient'].items()}
    }

    # Calculate average scores for each question type
    avg_metrics_by_type = {}
    for q_type, metrics in metrics_by_type.items():
        count = metrics["count"]
        if count > 0:
            avg_metrics_by_type[q_type] = {
                "exact_match_accuracy": metrics["exact_match"] / count,
                "record_accuracy": metrics["record_accuracy"] / count,
                "numeric_f1": metrics["numeric_f1"] / count,
                "execution_accuracy": {
                    "strict": metrics['execution_accuracy']['strict'] / count,
                    "lenient": metrics['execution_accuracy']['lenient'] / count
                },
                "selection_metrics": {
                    "lenient": {key: value / count for key, value in metrics['selection_scores']['lenient'].items()},
                    "strict": {key: value / count for key, value in metrics['selection_scores']['strict'].items()}
                },
                "total_questions": count,
                "empty_queries": metrics["empty_queries"]
            }

    results = {
        "model_path": model_path,
        "dataset_path": dataset_path,
        "task": task,
        "query_type": query_type,
        "avg_input_token_count": avg_input_token_count,
        "avg_output_token_count": avg_output_token_count,
        "metrics": {
            "exact_match_accuracy": em_accuracy,
            "record_accuracy": rec_accuracy,
            "numeric_f1": num_f1_accuracy,
            "execution_accuracy": ex_accuracy,
            "selection_metrics": avg_selection_scores,
        },
        "error_analysis": error_scores,
        "metrics_by_question_type": avg_metrics_by_type,
        "total_questions": total,
        "empty_queries": empty_queries
    }

    if query_type in ['sql', 'pivot_sql']:
        avg_component_scores = {key: value / total for key, value in component_scores.items()}
        results["metrics"]["component_matching"] = avg_component_scores

    if per_question_path:
        os.makedirs(os.path.dirname(per_question_path), exist_ok=True)
        with open(per_question_path, "w") as f:
            f.write('\n'.join(json.dumps(record) for record in per_question_records) + '\n')

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Evaluate query generation performance.", allow_abbrev=False)
    parser.add_argument("--model_path", type=str, required=True, help="Path to the model.")
    parser.add_argument("--dataset_path", type=str, default=config.TEST_QA_FILE,
                        help="Path to the testing question-answer pairs set.")
    parser.add_argument("--query_type", type=str, choices=get_args(QueryType), default='sql',
                        help="Type of query to evaluate.")
    parser.add_argument("--task", type=str, choices=['end-to-end', 'query-only'], default='end-to-end',
                        help="Type task to evaluate. End-to-end included table retrieval. For query only the golden "
                             "tables will be provided to the model.")
    parser.add_argument("--output_path", type=str, default="evaluation/generated_queries.json",
                        help="Path to save the generated queries to.")
    parser.add_argument("--results_path", type=str, default="evaluation/query_generation_results.json",
                        help="Path to where to save the results JSON file with the metrics.")
    parser.add_argument("--per_question_path", type=str, default=None,
                        help="Optional path to write the per-question metrics to, as JSONL.")
    parser.add_argument("--metrics_only", action='store_true',
                        help="Only recalculate metrics over the queries in --output_path, without loading the model.")
    args, unknown = parser.parse_known_args()

    model_kwargs = {unknown[i].lstrip('-'): unknown[i + 1] for i in range(0, len(unknown), 2)}
    results = evaluate_query_generation(args.model_path, args.dataset_path, args.output_path,
                                        args.query_type, args.task,
                                        model_kwargs=model_kwargs,
                                        per_question_path=args.per_question_path,
                                        metrics_only=args.metrics_only)

    os.makedirs(os.path.dirname(args.results_path), exist_ok=True)
    with open(args.results_path, "w") as f:
        json.dump(results, f, indent=4)

    print(f"Queries saved to {args.output_path}.")
    print(f"Results saved to {args.results_path}")
    print(json.dumps(results, indent=4))
