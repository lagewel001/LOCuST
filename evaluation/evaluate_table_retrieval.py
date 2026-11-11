import argparse
import json
import numpy as np
import os
import pandas as pd
from matplotlib import pyplot as plt
from tqdm import tqdm
from time import perf_counter
from typing import get_args, List, Dict, Optional

import config
from models.retrievers.base_retriever import BaseRetriever
from utils.custom_types import QueryType
from utils.global_functions import load_dataset, load_model_from_path, parse_for_table_id


def recall_at_each_k(predicted_tables: List[str], ground_truth: List[str], k: int) -> np.ndarray:
    """
        Recall@1..k for a single question: the share of its ground-truth tables appearing in the
        first j predictions, for every j up to k. The question has perfect recall@j when this is 1.

        :param predicted_tables: ranked table predictions
        :param ground_truth: the question's ground-truth tables
        :param k: cut-off
        :return: array of length k, where index j-1 holds recall@j
    """
    hits = np.isin(predicted_tables[:k], list(ground_truth)).astype(float)
    found = np.cumsum(hits)
    if len(found) < k:  # the retriever returned fewer than k tables
        found = np.pad(found, (0, k - len(found)), constant_values=found[-1] if len(found) else 0.0)
    return found / len(set(ground_truth))  # a table used twice (e.g. a self-join) has to be retrieved once


def evaluate_table_retrieval(
        model_path: str,
        dataset_path: str,
        query_type: QueryType,
        k: int = 20,
        model_kwargs: Optional[Dict] = None) -> Dict:
    """
        Evaluates table retrieval performance of a given model on a given test dataset.

        :param model_path: path to the model
        :param dataset_path: path to the dataset file
        :param query_type: type of query to evaluate ('sexp', 'sql' or 'pivot_sql')
        :param k: The 'k' for recall@k calculation
        :param model_kwargs: optional keyword arguments for model instantiation
        :return: dictionary containing the used model (path and kwargs), dataset, query_type and
                 EM + the recall@1..k and perfect recall@1..k curves
    """
    if model_kwargs is None:
        model_kwargs = {}
    dataset = load_dataset(dataset_path)
    model: BaseRetriever = load_model_from_path(model_path, **model_kwargs)

    exact_match = 0
    recall_curve = np.zeros(k)
    perfect_recall_curve = np.zeros(k)
    total = len(dataset)
    perf_time = []

    for item in tqdm(dataset, desc="Evaluating table retrieval", bar_format=config.TQDM_BAR_FMT):
        question = item.question
        answer = item[query_type]
        ground_truth_table = parse_for_table_id(answer, query_type)

        if not ground_truth_table:
            total -= 1
            continue

        s_t = perf_counter()
        predicted_tables = list(model.retrieve_tables(question, k=k).keys())
        perf_time.append(perf_counter() - s_t)

        # Exact Match (EM)
        # Check if all the first N tables of the predicted tables match the number N tables in the ground truth
        if set(predicted_tables[:len(ground_truth_table)]) == set(ground_truth_table):
            exact_match += 1

        # Recall@1..K and perfect recall@1..K
        # The share of ground-truth tables found by each cut-off, and whether all of them were found (as a
        # JOIN question needs all its tables), so a model can be compared at the k that matters downstream
        # (a prompt sees a handful of tables) without re-evaluating.
        recall_at_k = recall_at_each_k(predicted_tables, ground_truth_table, k)

        # Test Marc Ponsen: problematic items
        if recall_at_k[-1] == 0.0:
            with open('./evaluation/table_retriever_errors.jsonl', 'a+', encoding='utf-8') as f:
                line = {"question": question, "ground_truth_table": ground_truth_table, "predicted_tables": predicted_tables}
                f.write(f"{json.dumps(line)}\n")

        recall_curve += recall_at_k
        perfect_recall_curve += (recall_at_k == 1.0)

    em_accuracy = exact_match / total if total > 0 else 0
    recall_curve = recall_curve / total if total > 0 else np.zeros(k)
    perfect_recall_curve = perfect_recall_curve / total if total > 0 else np.zeros(k)

    results = {
        "model_path": model_path,
        # Multiple checkpoints of one retriever share a model_path,
        # so the kwargs are the only record of which one was actually evaluated.
        "model_kwargs": model_kwargs,
        "dataset_path": dataset_path,
        "query_type": query_type,
        "metrics": {
            "exact_match_accuracy": em_accuracy,
            "recall_at_k": list(recall_curve),
            "perfect_recall_at_k": list(perfect_recall_curve),
            "mean_perf_time_in_seconds": float(np.mean(perf_time)),
            "median_perf_time_in_seconds": float(np.median(perf_time)),
        },
        "total_questions": total,
    }

    return results


def plot_graph(json_files: List[str], k: int):
    result_df = pd.DataFrame(index=range(k))
    perfect_df = pd.DataFrame(index=range(k))
    for file in json_files:
        with open(file) as f:
            results = json.load(f)
            metrics = results.get('metrics', {})

            curve = metrics.get('recall_at_k')
            if curve is None:
                print(f"SKIPPING {os.path.basename(file)}: no recall_at_k. Re-run the evaluation.")
                continue

            label = os.path.splitext(os.path.basename(file))[0]
            result_df[label] = pd.Series(curve[:k])
            if 'perfect_recall_at_k' in metrics:
                perfect_df[label] = pd.Series(metrics['perfect_recall_at_k'][:k])

    # Generate graphs: recall solid, perfect recall dotted in the same color
    result_df = result_df.reindex(sorted(result_df.columns), axis=1)
    range_x = len(result_df.index)
    x_axis = np.arange(1, range_x + 1, 1)
    plt.figure(figsize=(10, 7))
    for func in result_df:
        line, = plt.plot(x_axis, result_df[func], label=func)
        if func in perfect_df:
            plt.plot(x_axis, perfect_df[func], ':', color=line.get_color(), label=f"{func} (perfect recall)")

    max_tables = len(os.listdir(config.DB_ODATA3_FILES))

    plt.plot(x_axis, np.linspace(0, 1, max_tables)[:range_x], 'k--', label='Random')
    plt.xlim([min(x_axis), max(x_axis)])
    plt.ylim([0.0, 1.0])
    plt.xlabel("Top K retrieved tables")
    plt.ylabel("Recall")
    plt.title("recall@K and perfect recall@K for entity retrieval methods")
    plt.grid()
    plt.legend(bbox_to_anchor=(1.0, 1.0))
    plt.tight_layout()

    path = f"{os.path.dirname(args.output_path)}/er_results.png"
    plt.savefig(path)
    print(f"Saved graph to {path}")

    plt.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Evaluate table retrieval performance.", allow_abbrev=False)
    parser.add_argument("--model_path", type=str, required=True,
                        help="Path to the model.")
    parser.add_argument("--dataset_path", type=str, default=config.TEST_QA_FILE,
                        help="Path to the testing question-answer pairs set.")
    parser.add_argument("--query_type", type=str, choices=get_args(QueryType), default='sql',
                        help="Type of query to evaluate.")
    parser.add_argument("--k", "-k", type=int, default=20,
                        help="Value of K for recall@K and perfect recall@K.")
    parser.add_argument("--output_path", type=str,
                        default=f"evaluation/results/table_retrieval_{config.LANGUAGE}_results.json",
                        help="Path to save the results JSON file.")
    parser.add_argument("-p", "--plot-only", action="store_true",
                        help="Only plot the results of previous evaluations found in the directory of --output_path.")
    args, unknown = parser.parse_known_args()

    model_kwargs = {unknown[i].lstrip('-'): unknown[i+1] for i in range(0, len(unknown), 2)}

    if not args.plot_only:
        results = evaluate_table_retrieval(args.model_path, args.dataset_path, args.query_type, args.k,
                                           model_kwargs=model_kwargs)

        os.makedirs(os.path.dirname(args.output_path), exist_ok=True)
        with open(args.output_path, "w") as f:
            json.dump(results, f, indent=4)

        print(json.dumps(results, indent=4))
        print(f"Results saved to {args.output_path}")

    dir_name = os.path.dirname(args.output_path)
    print(f"Plotting results founds in {dir_name}")
    jsons = [f"{dir_name}/{file}" for file in os.listdir(dir_name) if '.json' in file]
    plot_graph(jsons, args.k)
