import argparse
import difflib
import json
import locale
import numpy as np
import os
import pandas as pd
import pickle
import regex as re
import shutil
from colbert import Trainer
from colbert.infra.run import Run
from colbert.infra.config import ColBERTConfig, RunConfig
from math import ceil
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.model_selection import train_test_split
from torch.utils.cpp_extension import verify_ninja_availability
from tqdm import tqdm
from typing import Literal

# Patch for ColBERT training issues on Windows and Mac
import colbert.infra.launcher
from models.retrievers.colbert.patch_colbert import patched_setup_new_process

colbert.infra.launcher.setup_new_process = patched_setup_new_process

import config
from evaluation.evaluate_table_retrieval import parse_for_table_id
from models.retrievers.bm25_retriever import get_graph_node_labels

verify_ninja_availability()

parser = argparse.ArgumentParser(prog='ColBERTv2 training script')
parser.add_argument('--skip_create_dataset', action='store_true',
                    help='Skip creating a new dataset (collection & triples) when already present.')
parser.add_argument('--model_name', type=str, default='sentence-transformers/LaBSE',
                    help='Name of the starting checkpoint (default: LaBSE)')
parser.add_argument('--embedding_model', type=str, default='sentence-transformers/LaBSE',
                    help='Name of embedding model (default: LaBSE)')
parser.add_argument('--azure', action='store_true',
                    help='Envoke the Azure AI/ML API on the given model_name for generating embeddings. '
                         'If False, the model will be retrieved from SentenceTransformers.')
parser.add_argument('--negative_n', type=int, default=10,
                    help='Negative N value. The sampler will alternatively pick close and far away matches, '
                         'thus an even N would be logical, but not required (default: 10)')
parser.add_argument('--output_name', type=str, default='LaBSE',
                    help='Output name and dir of the trained model (default: LaBSE)')
parser.add_argument('--mode', type=str, choices=['table', 'node', 'all'], default='all',
                    help='(Sub)set of nodes to train model for (default: all)')
parser.add_argument('--learning_rate', '-lr', type=float, dest='learning_rate', default=5e-6,
                    help='Learning rate for the model trainer (default: 5e-6)')
parser.add_argument('--batch_size', '-b', type=int, dest='batch_size', default=8,
                    help='Batch size for the model trainer (default: 8)')

args = parser.parse_args()

assert os.path.exists(config.TRAIN_QA_FILE)
with open(config.TRAIN_QA_FILE, 'r') as outfile:
    queries = [json.loads(l) for l in outfile.read().splitlines()]

# Create train and test set
RANDOMSTATE = 42
SPLIT = 0.1
train_df, test_df = train_test_split(queries, test_size=SPLIT, random_state=RANDOMSTATE, shuffle=True)

if args.azure:
    from openai import AzureOpenAI
    client = AzureOpenAI(
        api_version=config.AZURE_API_VERSION,
        azure_endpoint=config.AZURE_ENDPOINT,
        api_key=config.AZURE_KEY,
    )
else:
    embedding_model = SentenceTransformer(args.embedding_model)


def create_train_set(
        negative_n: int,
        triples_path: str,
        queries_path: str,
        collection_path: str,
        mode: Literal['all', 'table', 'node'],
        include_time_geo_dims: bool = False):
    """
        Create a training dataset for the ColBERTv2 training. The training set consists of three separate files:
            * collection | tsv: pid, passage
            * queries | tsv: pid, passage
            * triples | jsonl: query, positive passage, negative passage
        The triples file contains `negative_n` samples of a query pid, the positive node/table and a selected
        negative node/table, based on a sampling done using the cosine similarity of the embedded description.
    """
    pk_file = collection_path.replace('tsv', 'pk')
    if not os.path.exists(pk_file):
        # Get all nodes from the graph if not done so already
        nodes = get_graph_node_labels(include_time_geo_dims=include_time_geo_dims, preprocess_text=False)
        nodes = {k: v for k, v in nodes.items() if mode == 'all' or v['type'] == mode}
        collection = pd.DataFrame.from_dict({
            i: doc | {'embedding': None}
            for i, (id_, doc) in tqdm(enumerate(nodes.items()), total=len(nodes), desc='Fetching collection...')
        }, orient='index')
        collection['id'] = nodes.keys()

        collection['body'] = collection['body'].str.replace(r'(((\r|\\r)?(\n|\\n))|\s+)', ' ', regex=True)
        os.makedirs('/'.join(pk_file.split('/')[:-1]), exist_ok=True)
        with open(pk_file, 'wb') as f:
            pickle.dump(collection, f)

    assert os.path.exists(pk_file)
    collection = pd.read_pickle(pk_file)
    if collection['embedding'].isnull().any():
        # Embed the descriptions if not done yet
        embeddings = {}
        for i, doc in tqdm(collection.iterrows(), total=len(collection), desc='Encoding collection...'):
            if doc['body'] in embeddings:
                continue

            if args.azure:
                embeddings[doc['body']] = client.embeddings.create(
                    input=doc['body'], model=args.embedding_model
                ).data[0].embedding
            else:
                embeddings[doc['body']] = embedding_model.encode(doc['body'])

        collection['embedding'] = collection['body'].map(embeddings)
        with open(pk_file, 'wb') as f:
            pickle.dump(collection, f)

    if mode != 'all':
        collection = collection[collection['type'] == mode]
        collection.reset_index(drop=True, inplace=True)

    # Write TSV to disk => doc_pid \t body
    # Ensure no newline is added at the end of the TSV, as this will break ColBERT's training script
    collection.to_csv(collection_path,
                      columns=['body'], header=False, sep='\t',
                      encoding=locale.getpreferredencoding(), errors='ignore')
    node_pid_map = {v['id']: k for k, v in collection.iterrows()}
    with open(collection_path.replace('.tsv', '_node_pid_map.json'), 'w') as f:
        json.dump(node_pid_map, f)

    # Get golden table & nodes per query
    questions = {'train': {}, 'test': {}}
    for split, ds in [('train', train_df), ('test', test_df)]:
        for i, r in tqdm(enumerate(ds), desc=f"Getting positives ({split})..."):
            # Get golden measures and dimensions from S-expression
            golden_tables = parse_for_table_id(r['sexp'], 'sexp')
            golden_msrs = re.findall(r"(?:\(MSR\s\(\s*(?=[^)])|\G(?!^)\s+)(\w+)", r['sexp'])
            golden_dims = re.findall(r"(?:\(DIM\s\S+\s\(\s*(?=[^)])|\G(?!^)\s+)(\w+)", r['sexp'])

            q_nodes = [node_pid_map[t] for t in golden_tables if t in node_pid_map]
            for n in golden_msrs + golden_dims:
                for t in golden_tables:
                    node_id = f"{t}#{n}"
                    if node_id in node_pid_map:
                        q_nodes.append(node_pid_map[node_id])

            questions[split][i] = {
                'query': r['question'],
                'positives': q_nodes,
            }

        query_tsv = [(k, v['query']) for k, v in questions[split].items()]
        query_df = pd.DataFrame(query_tsv, columns=['i', 'q']).set_index('i', drop=True)

        # TSV => query_pid \t query
        path = queries_path.replace('.tsv', f"_{split}.tsv")
        query_df.to_csv(path, columns=['q'], header=False, sep='\t',
                        encoding=locale.getpreferredencoding(), errors='ignore')

    # JSON => [query_pid, [pos_doc_pid, neg_doc_pid], [pos_doc_pid, neg_doc_pid], ...]
    if os.path.exists(triples_path):
        os.remove(triples_path)

    for qpid, r in tqdm(questions['train'].items(), desc='Selecting negatives...'):
        if args.azure:
            query_embedding = np.array(client.embeddings.create(input=[r['query']], model=args.embedding_model).data[0].embedding)
        else:
            query_embedding = embedding_model.encode(r['query'], show_progress_bar=False)

        for positive in r['positives']:
            # Get n negative nodes per query partially based on cos sim of embeddings
            too_similar = collection[collection.body.isin(
                difflib.get_close_matches(collection.loc[positive]['body'], collection['body'], cutoff=0.95)
            )]

            negative_candidates = collection[~collection.index.isin(
                set(r['positives'] + list(too_similar.index.values))
            )]
            similarities = cosine_similarity(query_embedding.reshape(1, -1), np.vstack(negative_candidates['embedding'].values))[0]
            sorted_indices = np.argsort(similarities)

            for i in range(negative_n):
                negative = (sorted_indices[::-1] if i % 2 == 0 else sorted_indices)[ceil(i / 2)]
                with open(triples_path, 'a+', newline='', encoding=locale.getpreferredencoding(), errors='ignore') as triple_tsv:
                    triple_tsv.write(f"[{qpid}, {positive}, {negative}]\n")


def train(model_checkpoint: str, triples_path: str, queries_path: str, collection_path: str):
    with Run().context(RunConfig(nranks=1, experiment=BASE_PATH)):
        # Note: regardless of given root, the model seems always to be saved in `experiments/<model_name>/**`
        config = ColBERTConfig(bsize=args.batch_size, lr=args.learning_rate, accumsteps=8,
                               query_maxlen=128, doc_maxlen=512, dim=128, save_every=1_000)
        q_path = queries_path.replace('.tsv', '_train.tsv')
        trainer = Trainer(triples=triples_path, queries=q_path, collection=collection_path, config=config)

        trainer.train(checkpoint=model_checkpoint)
        checkpoint_path = trainer.best_checkpoint_path()

        # Move checkpoint path to sensible place
        print(f"Saved checkpoint to {checkpoint_path} and copying required files to {BASE_PATH}.")
        shutil.copytree(checkpoint_path, BASE_PATH, dirs_exist_ok=True)
        shutil.copyfile(collection_path, f"{BASE_PATH}/collection.tsv")
        shutil.copyfile(collection_path.replace('.tsv', '_node_pid_map.json'),
                        f"{BASE_PATH}/node_pid_map.json")

    print("=== DONE TRAINING ===")


if __name__ == "__main__":
    args.output_name = args.output_name.replace('/', '_')
    BASE_PATH = f"{config.PATH_DIR_DATA}/colbert_retriever/{args.output_name}"
    TRIPLES_PATH = f"{BASE_PATH}/triples_{args.mode}.jsonl"
    QUERIES_PATH = f"{BASE_PATH}/queries_{args.mode}.tsv"
    COLLECTION_PATH = f"{BASE_PATH}/collection_{args.mode}.tsv"

    if not args.skip_create_dataset:
        create_train_set(args.negative_n, TRIPLES_PATH, QUERIES_PATH, COLLECTION_PATH, args.mode)

    train(model_checkpoint=args.model_name,  # starting checkpoints to train
          triples_path=TRIPLES_PATH,
          queries_path=QUERIES_PATH,
          collection_path=COLLECTION_PATH)
