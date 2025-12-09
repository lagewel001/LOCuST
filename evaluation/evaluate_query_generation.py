import argparse
import json
import os
import pandas as pd
import sqlglot
from time import time
from tqdm import tqdm
from typing import get_args, Tuple, Dict, Union, Literal

import config
from evaluation.component_match_metric import calculate_component_matching
from evaluation.evaluate_table_retrieval import parse_for_table_id
from evaluation.selection_metrics import get_selection_metrics
from models.generators.base_generator import BaseGenerator
from pipeline.db_executor import DBExecutor
from s_expression import Table, Expression
from s_expression.parser import parse, eval
from utils.answer_comparator import is_equal_frame
from utils.custom_types import QueryType
from utils.global_functions import load_dataset, load_model_from_path


def execute_query(query: str, query_type: QueryType) -> Tuple[Union[str, Expression], pd.DataFrame, float]:
    """Execute the given query and calculate its execution time"""
    if query_type == 'sexp':
        t0 = time()
        query, answer = eval(parse(query), offline=True)
        execution_time = time() - t0
    elif query_type == 'sql':
        db = DBExecutor(
            tables=[Table(t) for t in parse_for_table_id(query, query_type)],
            measures=set(),
            dims=set()
        )
        answer, _, execution_time = db.query_db(query, friendly_labels=False)
    else:
        raise ValueError(f"Unknown query type: {query_type}")

    return query, answer, execution_time


def get_question_type(sexp: str) -> str:
    """Determines the question type from an S-expression."""
    # The order of bins matters for questions with multiple aggregations
    bins = ['PROP', 'SUM', 'AVG', 'MIN', 'MAX']

    has_join = 'JOIN' in sexp

    for agg in bins:
        if agg in sexp:
            if has_join:
                return 'AGGJOIN'
            return agg

    if has_join:
        return 'JOIN'

    return 'VALUE'


def evaluate_query_generation(
        model_path: str,
        dataset_path: str,
        output_path: str,
        query_type: QueryType,
        task: Literal['end-to-end', 'query-only'],
        model_kwargs: Dict = None) -> Dict:
    """
        Evaluates query generation performance.

        :param model_path: path to the model
        :param dataset_path: path to the dataset file
        :param output_path: path to the generated queries from the model to. If a generated query is present for all
                            QA-paris in dataset_path, this can also be used for just re-calculating the metrics.
        :param query_type: type of query to evaluate ('sexp' or 'sql')
        :param task: type of task to evaluate ('end-to-end' or 'query-only')
        :param model_kwargs: optional keyword arguments for model instantiation
        :return: dictionary containing the used model, dataset and metrics
     """
    if model_kwargs is None:
        model_kwargs = {}
    dataset = load_dataset(dataset_path)
    model: BaseGenerator = load_model_from_path(model_path, **model_kwargs)

    # Get previously generated results
    generated_answers = {}
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    if os.path.exists(output_path):
        with open(output_path, "r") as f:
            generated_answers = json.load(f)

    exact_match = 0
    execution_accuracy = 0
    component_scores = {'select_f1': 0.0, 'where_f1': 0.0, 'groupby_f1': 0.0, 'orderby_f1': 0.0, 'pivot_f1': 0.0}
    selection_scores = {'measure_f1': 0.0, 'dimension_f1': 0.0, 'observation_f1': 0.0}
    total = len(dataset)

    question_types = ['VALUE', 'SUM', 'AVG', 'MIN', 'MAX', 'JOIN', 'PROP', 'AGGJOIN']
    metrics_by_type = {q_type: {
        "count": 0,
        "exact_match": 0,
        "execution_accuracy": 0,
        "selection_scores": {'measure_f1': 0.0, 'dimension_f1': 0.0, 'observation_f1': 0.0}
    } for q_type in question_types}

    for item in tqdm(dataset, desc=f"Evaluating query generation (Task: {task})", bar_format=config.TQDM_BAR_FMT):
        question = item.question
        ground_truth_query = item[query_type]

        if question in generated_answers:
            predicted_query = generated_answers[question]
        else:
            golden_tables = None
            if task == 'query-only':
                golden_tables = parse_for_table_id(ground_truth_query, query_type)
            predicted_query = model.generate_query(question, golden_tables=golden_tables)

            generated_answers[question] = predicted_query
            with open(output_path, "w") as f:
                json.dump(generated_answers, f, indent=4)

        # Determine question type from s-expression
        q_type = get_question_type(item.sexp)
        metrics_by_type[q_type]["count"] += 1

        # Exact Match
        em_reward = 1 if predicted_query == ground_truth_query else 0
        exact_match += em_reward
        metrics_by_type[q_type]["exact_match"] += em_reward

        # Execution Accuracy
        try:
            # Check for query validity before execution
            if query_type == 'sexp':
                parse(predicted_query)
            elif query_type == 'sql':
                sqlglot.parse_one(predicted_query, read='duckdb')

            _, predicted_result, _ = execute_query(predicted_query, query_type)
            _, ground_truth_result, _ = execute_query(ground_truth_query, query_type)
            
            if is_equal_frame(ground_truth_result, predicted_result):
                exec_acc_reward = 1
            else:
                exec_acc_reward = 0
        except Exception as e:
            print(e)
            exec_acc_reward = 0

        execution_accuracy += exec_acc_reward
        metrics_by_type[q_type]["execution_accuracy"] += exec_acc_reward

        # Selection-based F1 scores
        sel_scores = get_selection_metrics(predicted_query, ground_truth_query, query_type)
        for key, value in sel_scores.items():
            selection_scores[key] += value
            metrics_by_type[q_type]["selection_scores"][key] += value

        # Component Matching (SQL only)
        if query_type == 'sql':
            comp_scores = calculate_component_matching(predicted_query, ground_truth_query)
            for key, value in comp_scores.items():
                component_scores[key] += value

    em_accuracy = exact_match / total if total > 0 else 0
    ex_accuracy = (execution_accuracy / total) if total > 0 else 0
    avg_selection_scores = {key: value / total for key, value in selection_scores.items()}

    # Calculate average scores for each question type
    avg_metrics_by_type = {}
    for q_type, metrics in metrics_by_type.items():
        count = metrics["count"]
        if count > 0:
            avg_metrics_by_type[q_type] = {
                "exact_match_accuracy": metrics["exact_match"] / count,
                "execution_accuracy": metrics["execution_accuracy"] / count,
                "selection_metrics": {key: value / count for key, value in metrics["selection_scores"].items()},
                "total_questions": count
            }

    results = {
        "model_path": model_path,
        "dataset_path": dataset_path,
        "task": task,
        "query_type": query_type,
        "metrics": {
            "exact_match_accuracy": em_accuracy,
            "execution_accuracy": ex_accuracy,
            "selection_metrics": avg_selection_scores
        },
        "metrics_by_question_type": avg_metrics_by_type,
        "total_questions": total
    }

    if query_type == 'sql':
        avg_component_scores = {key: value / total for key, value in component_scores.items()}
        results["metrics"]["component_matching"] = avg_component_scores

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
    args, unknown = parser.parse_known_args()

    model_kwargs = {unknown[i].lstrip('-'): unknown[i + 1] for i in range(0, len(unknown), 2)}
    results = evaluate_query_generation(args.model_path, args.dataset_path, args.output_path,
                                        args.query_type, args.task,
                                        model_kwargs=model_kwargs)

    os.makedirs(os.path.dirname(args.results_path), exist_ok=True)
    with open(args.results_path, "w") as f:
        json.dump(results, f, indent=4)

    print(f"Queries saved to {args.output_path}.")
    print(f"Results saved to {args.results_path}")
    print(json.dumps(results, indent=4))
