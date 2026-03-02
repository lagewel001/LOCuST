import argparse
import json
import os
from colbert import Searcher, Indexer
from colbert.infra import Run, RunConfig, ColBERTConfig
from typing import Literal, Dict

import config
from models.retrievers.base_retriever import BaseRetriever

BASE_PATH = f"{config.PATH_DIR_DATA}/colbert_retriever"


class ColBERTRetriever(BaseRetriever):
    pid_node_map: Dict[int, str]
    searcher: Searcher

    def __init__(self, checkpoint: str, mode: Literal['all', 'table', 'node'] = 'table'):
        """
            :param checkpoint: Directory containing model checkpoint, relative to {BASE_PATH}
            :param mode: (Sub)set of nodes to train model for (default): table
        """
        super().__init__()
        self.checkpoint = checkpoint
        self.mode = mode

        self.index_colbert()


    def retrieve_tables(self, query: str, k: int) -> Dict:
        ranking = self.searcher.search(query, k=k)
        sorted_tables = {self.pid_node_map[pid]: {
            'score': ranking[2][idx]
        } for idx, pid in enumerate(ranking[0])}
        return sorted_tables

    def index_colbert(self):
        """
            Create a ColBERT index. Requires a collection_<mode>.tsv and node_pid_map.json file for
            indexing the documents. This can be created using the create_train_set function in the
            colbert_training.py script.
        """
        model_path = f"{BASE_PATH}/{self.checkpoint}"
        collection_path = f"{model_path}/collection_{self.mode}.tsv"
        node_pid_map_path = f"{BASE_PATH}/{self.checkpoint}/collection_{self.mode}_node_pid_map.json"

        if not os.path.exists(collection_path):
            raise FileNotFoundError(f"A collection file must exist for indexing ColBERT! Missing {collection_path}")
        if not os.path.exists(node_pid_map_path):
            raise FileNotFoundError(f"A node-PID-mapping file must exists for running ColBERT! Missing {node_pid_map_path}")

        print("Creating ColBERTv2 index...")
        with Run().context(RunConfig(nranks=1, experiment=self.checkpoint)):
            colbert_config = ColBERTConfig(nbits=2, root=BASE_PATH)  # , index_bsize=8)
            indexer = Indexer(checkpoint=model_path, config=colbert_config)
            if os.path.exists(f"./experiments/{self.checkpoint}/indexes/{self.mode}.nbits=2/plan.json"):
                overwrite = 'reuse'
            else:
                overwrite = True
            indexer.index(name=f"{self.mode}.nbits=2", overwrite=overwrite, collection=collection_path)
            self.searcher = Searcher(index=f"{self.mode}.nbits=2", config=colbert_config)

        with open(node_pid_map_path, 'r') as f:
            node_pid_map = json.load(f)
            self.pid_node_map = {v: k for k, v in node_pid_map.items()}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='ColBERTv2 evaluation')
    parser.add_argument('--checkpoint', required=True, type=str,
                        help=f"Directory containing model checkpoint, relative to {BASE_PATH}")
    parser.add_argument('--mode', type=str, choices=['table', 'node', 'all'], default='all',
                        help='(Sub)set of nodes to that model was trained on (default: all)')

    args = parser.parse_args()

    retriever = ColBERTRetriever(checkpoint=args.checkpoint, mode=args.mode)

    q = "How many people went on vacation to Germany in 2020?"
    top_tables = retriever.retrieve_tables(query=q, k=10)
    print(top_tables)
