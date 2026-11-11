import argparse
import difflib
import itertools
import json
import locale
import numpy as np
import os
import pandas as pd
import math
import pickle
import random
import regex as re
import shutil
import textwrap
from colbert import Trainer, Indexer, Searcher
from datetime import datetime
from colbert.infra.run import Run
from colbert.infra.config import ColBERTConfig, RunConfig
from operator import itemgetter
from rdflib.namespace import QB
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.model_selection import GroupShuffleSplit
from torch.utils.cpp_extension import verify_ninja_availability
from tqdm import tqdm
from typing import Literal, Dict, List, Optional

# Patch for ColBERT training issues on Windows and Mac
import colbert.infra.launcher
from config import EN_TEST_TABLES, NL_TEST_TABLES
from odata_graph import engine
from models.retrievers.colbert.patch_colbert import patched_setup_new_process, set_checkpoint_steps

colbert.infra.launcher.setup_new_process = patched_setup_new_process

import config
from logs import logging
from utils.global_functions import parse_for_table_id

verify_ninja_availability()

logger = logging.getLogger(__name__)


class ColBERTTrainer(object):
    RANDOM_STATE = 42
    SPLIT = 0.1
    # Similarity above which a candidate counts as a duplicate of a positive and is not usable for hard-negative mining.
    DUPLICATE_CUTOFF = 0.95
    # Token embedding width for a fresh base model. ColBERT's paper own default is 128
    DEFAULT_DIM = 128

    def __init__(self,
                 output_name: str,
                 mode: Literal['all', 'table', 'node'],
                 model_name: str = 'BAAI/bge-m3',
                 embedding_model_name: str = 'sentence-transformers/all-MiniLM-L12-v2',
                 negative_n: int = 10,
                 learning_rate: float = 5e-6,
                 batch_size: int = 8,
                 azure: bool = False,
                 skip_create_dataset: bool = False,
                 num_rounds: int = 1,
                 hn_checkpoint: Optional[str] = None,
                 search_k: int = 100,
                 mix_ratio: float = 0.0,
                 nbits: int = 4,
                 epochs: int = 1,
                 accumsteps: int = 1,
                 doc_maxlen: Optional[int] = None,
                 dim: Optional[int] = None,
                 exclude_benchmark_tables: bool = True,
                 rebuild_only: bool = False):
        """
            Manages the full ColBERT training pipeline: dataset creation, negative sampling, and
            (optionally) iterative hard negative mining across multiple training rounds.

            On construction, loads the QA data from config.TRAIN_QA_FILE and creates a table-stratified
            train/test split to prevent data leakage. Also initializes the embedding model used for
            cosine-similarity-based negative sampling in round 1.

            Training modes:
                - Single-round (num_rounds=1, default): creates a training set with cosine-sim negatives
                  from the embedding model, then trains a ColBERT model. Equivalent to the legacy flow.
                - Iterative (num_rounds>1): round 1 uses cosine-sim negatives; subsequent rounds index
                  the previous round's checkpoint with ColBERT, retrieve each training query, and select
                  the highest-scoring false positives as hard negatives for retraining.
                - With hn_checkpoint: skips round 1's cosine-sim step entirely and mines hard negatives
                  from an existing checkpoint from the first round onwards.

            :param output_name: name for the output directory under data/colbert_retriever/
            :param mode: which document types to include in the collection
                         ('table' = tables only, 'node' = measures/dimensions only, 'all' = both)
            :param model_name: HuggingFace model name or local path for the base checkpoint
            :param embedding_model_name: SentenceTransformer model used for cosine-sim negative sampling
            :param negative_n: number of negatives to sample per (query, positive) pair
            :param learning_rate: learning rate for ColBERT fine-tuning
            :param batch_size: training batch size
            :param azure: if True, use Azure OpenAI API for embeddings instead of SentenceTransformers
            :param skip_create_dataset: skip dataset creation when collection/triples already exist on disk
            :param num_rounds: number of training rounds (1 = single-round, 2+ = iterative hard negative mining)
            :param hn_checkpoint: path to an existing ColBERT checkpoint to mine hard negatives from in round 1,
                                  skipping the initial cosine-sim training. Must contain collection, queries,
                                  and node_pid_map files.
            :param search_k: number of results to retrieve per query during hard negative mining
            :param mix_ratio: fraction of negatives to retain from the previous round's triples
                              (0.0 = all model-mined, 0.5 = half old / half new)
            :param nbits: quantization bits for the ColBERT index during hard negative mining (1, 2, 4, or 8)
            :param rebuild_only: if True, only initialize paths — skip loading QA data and embedding model.
                                 Used when the trainer is only needed for rebuild_collection().
        """
        self.mode = mode
        self.model_name = model_name
        self.negative_n = negative_n
        self.learning_rate = learning_rate
        self.batch_size = batch_size
        self.azure = azure
        self.skip_create_dataset = skip_create_dataset
        self.num_rounds = num_rounds
        self.hn_checkpoint = hn_checkpoint
        self.search_k = search_k
        self.mix_ratio = mix_ratio
        self.epochs = epochs
        self.accumsteps = accumsteps
        self.doc_maxlen = doc_maxlen
        self.dim = dim
        self.exclude_benchmark_tables = exclude_benchmark_tables
        self.nbits = nbits

        # Paths
        output_name = output_name.replace('/', '_')
        self.base_path = f"{config.PATH_DIR_DATA}/colbert_retriever/{output_name}"
        self.triples_path = f"{self.base_path}/triples_{mode}.jsonl"
        self.queries_path = f"{self.base_path}/queries_{mode}.tsv"
        self.collection_path = f"{self.base_path}/collection_{mode}.tsv"

        if rebuild_only:
            # Minimal init — only paths are needed for rebuild_collection()
            self.embedding_model = None
            self.azure_client = None
            self.embedding_model_name = embedding_model_name
            self.queries = None
            self.train_df = None
            self.test_df = None
            logger.info(
                f"=== ColBERT Trainer initialized (rebuild-only mode) ==="
                f"\n\tOutput directory:    {self.base_path}"
                f"\n\tMode:                {mode}"
            )
            return

        # Embedding model — only needed for cosine-sim negative sampling (round 1 without hn_checkpoint)
        self.embedding_model = None
        self.azure_client = None
        self.embedding_model_name = embedding_model_name
        needs_embedding_model = hn_checkpoint is None and not skip_create_dataset
        if needs_embedding_model:
            if azure:
                from openai import AzureOpenAI
                self.azure_client = AzureOpenAI(
                    api_version=config.AZURE_API_VERSION,
                    azure_endpoint=config.AZURE_ENDPOINT,
                    api_key=config.AZURE_KEY,
                )
            else:
                self.embedding_model = SentenceTransformer(embedding_model_name)

        # Load QA data and create train/test split
        assert os.path.exists(config.TRAIN_QA_FILE)
        with open(config.TRAIN_QA_FILE, 'r') as outfile:
            self.queries = [json.loads(l) for l in outfile.read().splitlines()]

        # Stratified split by primary table to prevent data leakage
        query_groups = [parse_for_table_id(q['sexp'], 'sexp')[0] for q in self.queries]
        gss = GroupShuffleSplit(n_splits=1, test_size=self.SPLIT, random_state=self.RANDOM_STATE)
        train_idx, test_idx = next(gss.split(self.queries, groups=query_groups))
        self.train_df = [self.queries[i] for i in train_idx]
        self.test_df = [self.queries[i] for i in test_idx]

        train_tables = {query_groups[i] for i in train_idx}
        test_tables = {query_groups[i] for i in test_idx}
        overlap = train_tables & test_tables
        self.split_summary = {'train_questions': len(self.train_df), 'test_questions': len(self.test_df),
                              'train_tables': len(train_tables), 'test_tables': len(test_tables),
                              'overlapping_tables': len(overlap)}

        # Log configuration summary
        if num_rounds > 1 or hn_checkpoint is not None:
            training_strategy = f"iterative hard negative mining ({num_rounds} round(s))"
            if hn_checkpoint:
                training_strategy += f", starting from checkpoint: {hn_checkpoint}"
        else:
            training_strategy = "single-round with cosine-sim negatives"

        embedding_source = "Azure OpenAI" if azure else embedding_model_name
        logger.info(
            f"=== ColBERT Trainer initialized ==="
            f"\n\tOutput directory:    {self.base_path}"
            f"\n\tBase model:          {model_name}"
            f"\n\tMode:                {mode}"
            f"\n\tTraining strategy:   {training_strategy}"
            f"\n\tNegative samples:    {negative_n} per (query, positive) pair"
            f"\n\tLearning rate:       {learning_rate}"
            f"\n\tBatch size:          {batch_size} (accumsteps {accumsteps}, "
            f"{batch_size // accumsteps} per forward pass)"
            f"\n\tEpochs:              {epochs}"
            f"\n\tnway:                {negative_n + 1} (1 positive + {negative_n} negatives per step)"
            f"\n\tBenchmark tables:    {'excluded from negatives' if exclude_benchmark_tables else 'INCLUDED (production mode)'}"
            f"\n\tEmbedding model:     {embedding_source}{' (skipped)' if not needs_embedding_model else ''}"
            f"\n\tSearch depth (k):    {search_k}"
            f"\n\tMix ratio:           {mix_ratio}"
            f"\n\tQuantization (nbits): {nbits}"
            f"\n\tData split:          {len(self.train_df)} train / {len(self.test_df)} test, "
            f"{len(train_tables)} train tables / {len(test_tables)} test tables, "
            f"{len(overlap)} overlapping (should be 0)"
        )

    def get_graph_node_labels(self, include_time_geo_dims: bool = False) -> Dict[str, Dict[str, str]]:
        """
            Return a dictionary with all the textual elements for every table and measure/dimension in the graph.
        """

        # Prevents the construction of multi level folder structure caused by the URL scheme
        prefix_node_labels_name = re.sub(r"[./]", "_", re.sub(r"^https?://", "", config.GRAPH_DB_REPO))
        node_labels_path = f"{self.base_path}/{prefix_node_labels_name}_node_labels{{}}.json"
        node_labels_path = node_labels_path.format('_including_time_geo' if include_time_geo_dims else '')
        if os.path.isfile(node_labels_path):
            logger.info(f"Loading node labels from {node_labels_path}")
            with open(node_labels_path) as f:
                node_labels = json.load(f)
                return node_labels

        tables = {}
        nodes = {}

        # Fetch all tables in the graph, including entries containing title and descriptions from the tables
        table_query = ("""
            PREFIX dcat: <http://www.w3.org/ns/dcat#>
            PREFIX dct: <http://purl.org/dc/terms/>

            SELECT DISTINCT ?id ?label WHERE {{
                ?s a dcat:Dataset .
                ?s dct:identifier ?id .
                OPTIONAL {{ ?s dct:title|dct:abstract|dct:description ?label }}
            }}
        """)

        try:
            fetch_tables = engine.select(table_query)
            props = [(r['id']['value'], (r.get('label', False) or {'value': ''})['value']) for r in fetch_tables]
            table_it = itertools.groupby(props, itemgetter(0))
        except Exception as e:
            raise RuntimeError(f"Failed to fetch table IDs: {e}")

        for table, table_props in tqdm(table_it, total=len(set(map(itemgetter(0), props))),
                                       desc="Fetching graph nodes for all tables", bar_format=config.TQDM_BAR_FMT):
            val = ' '.join(f"{prop[1].strip()}{'' if prop[1][-1] == '.' else '.'}" for prop in table_props)
            tables[table] = {'body': val, 'type': 'table'}

            # Get all measures and dimensions for a table
            # Codes carry specific gecko level classes, so time/geo exclusion matches the (cached)
            # subclass closure of gecko:TimeDimension/gecko:GeoDimension.
            # Not every code is typed correctly at the source, so the class check alone lets them
            # into the collection. There parents do cary the Time/Geo type, so a second check on the parent
            # catches what the first one misses.
            timegeo_values = ' '.join(f'<{c}>' for c in engine.timegeo_classes())
            timegeo_filter = textwrap.dedent(f"""
                FILTER NOT EXISTS {{
                    VALUES ?timegeo {{ {timegeo_values} }}
                    ?node a ?timegeo .
                }}
                FILTER NOT EXISTS {{
                    VALUES ?timegeo_broader {{ {timegeo_values} }}
                    ?node skos:broader ?parent .
                    ?parent a ?timegeo_broader .
                }}
            """) if not include_time_geo_dims else ''

            query = textwrap.dedent(f"""
                PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
                PREFIX dct: <http://purl.org/dc/terms/>
                PREFIX qb: <http://purl.org/linked-data/cube#>

                SELECT ?node_id ?type ?label WHERE {{
                    ?table ?type ?node .
                    FILTER (?type = qb:measure || ?type = qb:dimension) .
                    {timegeo_filter}
                    ?table dct:identifier ?table_id .
                    ?node qb:concept ?concept ;
                          dct:identifier ?node_id .
                    ?concept dct:isPartOf ?table .
                    OPTIONAL {{ ?concept skos:prefLabel|skos:altLabel|skos:definition|dct:description|dct:subject ?label }}
                    FILTER (?table_id = "{table}")
                }}
            """)

            try:
                result = engine.select(query)
                props = [(r['node_id']['value'], r['type']['value'], (r.get('label', False) or {'value': ''})['value'])
                         for r in result]
                node_it = itertools.groupby(props, itemgetter(0))
                for node_id, node_props in node_it:
                    node_props = list(node_props)
                    node_type = node_props[0][1]
                    if node_type == str(QB.measure):
                        type_ = 'measure'
                    elif node_type == str(QB.dimension):
                        type_ = 'dimension'
                    else:
                        raise ValueError(f"Unknown node type: {node_type}")

                    val = ' '.join(f"{prop[2].strip()}{'' if not prop[2] or prop[2][-1] == '.' else '.'}" for prop in node_props)
                    # Because nodes can have different concepts per table, store them using unique identifiers per table
                    nodes[f"{table}#{node_id}"] = {'body': val, 'type': type_, 'table': table}
            except Exception as e:
                logger.error(f"Failed to fetch table nodes: {e}")

        nodes.update(tables)
        os.makedirs(os.path.dirname(node_labels_path), exist_ok=True)
        with open(node_labels_path, 'w') as f:
            json.dump(nodes, f)

        logger.info(f"Successfully saved node properties to {node_labels_path}")
        return nodes

    @staticmethod
    def build_enriched_collection(
            nodes: Dict[str, Dict[str, str]],
            max_child_labels: int = 15) -> Dict[str, Dict[str, str]]:
        """
            Enrich document representations for the ColBERT collection by cross-pollinating
            information between tables and their child measures/dimensions.

            For tables: appends a structured summary of child measure and dimension labels to the body.
            For measures/dimensions: prepends the parent table's title to the body.

            :param nodes: raw output from get_graph_node_labels
            :param max_child_labels: max number of child labels to append per type to a table body
                                     (to stay within ColBERT's doc_maxlen)
            :return: A new nodes dict with enriched 'body' fields
        """
        tables = {k: v for k, v in nodes.items() if v['type'] == 'table'}
        children = {k: v for k, v in nodes.items() if v['type'] != 'table'}

        # Group children by parent table
        table_children: Dict[str, Dict[str, List[str]]] = {}
        for node_id, node_data in children.items():
            table_id = node_id.split('#')[0]
            if table_id not in table_children:
                table_children[table_id] = {'measure': [], 'dimension': []}
            table_children[table_id][node_data['type']].append(node_data['body'])

        # Extract table titles (first sentence of body, before the first period)
        table_titles = {}
        for table_id, table_data in tables.items():
            title = table_data['body'].split('.')[0].strip()
            table_titles[table_id] = title

        enriched = {}

        # Enrich table documents: append child measure and dimension labels
        for table_id, table_data in tables.items():
            body = table_data['body']
            tc = table_children.get(table_id, {'measure': [], 'dimension': []})

            msr_labels = [b.split('.')[0].strip() for b in tc['measure']][:max_child_labels]
            dim_labels = [b.split('.')[0].strip() for b in tc['dimension']][:max_child_labels]

            suffix_parts = []
            if msr_labels:
                suffix_parts.append(f"Measures: {', '.join(msr_labels)}")
            if dim_labels:
                suffix_parts.append(f"Dimensions: {', '.join(dim_labels)}")

            if suffix_parts:
                body = f"{body} {'. '.join(suffix_parts)}."

            enriched[table_id] = {**table_data, 'body': body}

        # Enrich child node documents: prepend parent table title
        for node_id, node_data in children.items():
            table_id = node_id.split('#')[0]
            title = table_titles.get(table_id, '')
            if title:
                body = f"[{title}] {node_data['body']}"
            else:
                body = node_data['body']

            enriched[node_id] = {**node_data, 'body': body}

        logger.info(f"Enriched {len(tables)} table(s) and {len(children)} node(s)")
        return enriched

    def _embed(self, text: str):
        """Embed a single text using the configured embedding model."""
        if self.azure:
            return np.array(self.azure_client.embeddings.create(
                input=[text], model=self.embedding_model_name
            ).data[0].embedding)
        else:
            return self.embedding_model.encode(text, show_progress_bar=False)

    def _build_positives(self, node_pid_map: dict, dataset: list = None) -> Dict[int, dict]:
        """
            Build a dict mapping query PIDs to their question text and positive document PIDs,
            using the given node-to-PID mapping.

            :param node_pid_map: mapping from node/table IDs to collection PIDs
            :param dataset: list of QA dicts to process (defaults to self.train_df)
        """
        if dataset is None:
            dataset = self.train_df

        positives = {}
        for i, r in enumerate(dataset):
            golden_tables = parse_for_table_id(r['sexp'], 'sexp')
            golden_msrs = re.findall(r"(?:\(MSR\s\(\s*(?=[^)])|\G(?!^)\s+)(\w+)", r['sexp'])
            golden_dims = re.findall(r"(?:\(DIM\s\S+\s\(\s*(?=[^)])|\G(?!^)\s+)(\w+)", r['sexp'])

            q_nodes = [node_pid_map[t] for t in golden_tables if t in node_pid_map]
            for n in golden_msrs + golden_dims:
                for t in golden_tables:
                    node_id = f"{t}#{n}"
                    if node_id in node_pid_map:
                        q_nodes.append(node_pid_map[node_id])

            positives[i] = {
                'query': r['question'],
                'positives': q_nodes,
            }
        return positives

    def _held_out_pids(self, node_pid_map: Dict[str, int]) -> set:
        """
            Collection PIDs belonging to tables reserved for the generalisation benchmark.

            These tables never appear as a positive, so without this they can only ever be sampled
            as negatives -- the model would be trained to rank down exactly the tables the benchmark
            measures. "Unseen" is a fair zero-shot test; "seen only as a negative" is not. Returns
            an empty set in production mode, where nothing is held out and suppressing them would
            only lose training signal.

            :param node_pid_map: mapping from node/table IDs to collection PIDs
            :return: set of PIDs to keep out of every negative pool
        """
        if not self.exclude_benchmark_tables:
            return set()

        benchmark = set(NL_TEST_TABLES if config.LANGUAGE == 'nl' else EN_TEST_TABLES)
        pids = {pid for node, pid in node_pid_map.items() if node.split('#')[0] in benchmark}
        logger.info(f"Excluding {len(pids)} document(s) from {len(benchmark)} benchmark table(s) "
                    f"from all negative pools.")
        return pids

    def _write_triples(self, triples: List[list], path: str):
        """
            Write triples as one line per (query, positive): [qpid, pid+, pid-, ...].

            One line carrying every negative lets ColBERT train an (nway)-way softmax over all of
            them at once, instead of the nway=2 binary contrast that one-negative-per-line forces.

            The list is shuffled, and extra epochs are expressed by repeating it: LazyBatcher slices
            batches straight off the file and raises StopIteration at EOF, so the file length is the
            training length. Shuffling is required rather than cosmetic -- unshuffled, consecutive
            lines are all positives of the same query, so a batch (and therefore its in-batch
            negatives) would be built from near-identical examples.

            :param triples: one [qpid, positive, *negatives] list per (query, positive) pair
            :param path: destination JSONL file
        """
        rng = random.Random(self.RANDOM_STATE)
        with open(path, 'w', newline='', encoding=locale.getpreferredencoding(), errors='ignore') as f:
            for _ in range(self.epochs):
                epoch = list(triples)
                rng.shuffle(epoch)
                for triple in epoch:
                    f.write(json.dumps(triple) + '\n')

        logger.info(f"Wrote {len(triples) * self.epochs:,} triples ({len(triples):,} unique "
                    f"x {self.epochs} epoch(s)) to {path}")

    def _mine_cosine_sim_negatives(self, train_questions: Dict[int, dict], collection: pd.DataFrame,
                                   held_out_pids: set = frozenset()):
        """
            Mine negative samples using cosine similarity from the embedding model.
            For each (query, positive) pair, selects negative_n negatives by alternating between the
            most and least similar candidates in the embedding space.

            :param train_questions: output of _build_positives() for the training split
            :param collection: DataFrame with 'body' and 'embedding' columns
            :param held_out_pids: PIDs that must never be sampled as a negative (see _held_out_pids)
        """
        triples = []
        for qpid, r in tqdm(train_questions.items(), desc='Selecting negatives...'):
            query_embedding = self._embed(r['query'])

            for positive in r['positives']:
                # Get n negative nodes per query partially based on cos sim of embeddings
                too_similar = collection[collection.body.isin(
                    difflib.get_close_matches(collection.loc[positive]['body'], collection['body'], cutoff=self.DUPLICATE_CUTOFF)
                )]

                negative_candidates = collection[~collection.index.isin(
                    set(r['positives']) | set(too_similar.index.values) | set(held_out_pids)
                )]
                candidate_pids = negative_candidates.index.to_numpy()
                similarities = cosine_similarity(query_embedding.reshape(1, -1), np.vstack(negative_candidates['embedding'].values))[0]
                sorted_indices = np.argsort(similarities)

                if len(candidate_pids) < self.negative_n:
                    logger.warning(f"Query {qpid}: only {len(candidate_pids)} candidate negatives "
                                   f"available, skipping positive {positive}.")
                    continue

                negatives = []
                for i in range(self.negative_n):
                    # argsort returns positions within `negative_candidates`, which has rows
                    # removed relative to `collection`; map back through the index to get a
                    # real collection pid before writing it as one.
                    rank = (sorted_indices[::-1] if i % 2 == 0 else sorted_indices)[i // 2]
                    negatives.append(int(candidate_pids[rank]))

                triples.append([qpid, positive, *negatives])

        self._write_triples(triples, self.triples_path)

    @staticmethod
    def _strip_table_prefix(body: str) -> str:
        """
            Drop the '[parent table title] ' prefix that build_enriched_collection
            prepends before doing duplicate detection.
        """
        return re.sub(r"^\[[^\]]*\]\s*", '', body).strip()

    @classmethod
    def _is_near_duplicate(cls, text: str, positive_texts: List[str], positive_text_set: set) -> bool:
        """
            Whether `text` is a (near-)duplicate of the query's positives, using the same
            similarity cutoff as the cosine-sim pass (first training round).
        """
        if text in positive_text_set:
            return True
        for positive_text in positive_texts:
            matcher = difflib.SequenceMatcher(None, positive_text, text)
            if (matcher.real_quick_ratio() >= cls.DUPLICATE_CUTOFF
                    and matcher.quick_ratio() >= cls.DUPLICATE_CUTOFF
                    and matcher.ratio() >= cls.DUPLICATE_CUTOFF):
                return True
        return False

    def _mine_hard_negatives_from_model(self, checkpoint_path: str):
        """
            Mine hard negatives using a trained ColBERT model's own retrieval errors.
            Indexes the collection with the given checkpoint, searches for each training query,
            and selects the highest-scoring non-positive results as hard negatives.

            :param checkpoint_path: path to the ColBERT checkpoint directory (must also contain
                                    collection, queries, and node_pid_map files)
        """
        collection_path = f"{checkpoint_path}/collection_{self.mode}.tsv"
        queries_path = f"{checkpoint_path}/queries_{self.mode}_train.tsv"
        triples_path = f"{checkpoint_path}/triples_{self.mode}.jsonl"
        node_pid_map_path = f"{checkpoint_path}/collection_{self.mode}_node_pid_map.json"

        for required_file in [collection_path, queries_path, node_pid_map_path]:
            if not os.path.exists(required_file):
                raise FileNotFoundError(f"Required file for hard negative mining not found: {required_file}")

        with open(node_pid_map_path, 'r') as f:
            node_pid_map = json.load(f)

        positives_per_query = {
            i: {**data, 'positives': set(data['positives'])}
            for i, data in self._build_positives(node_pid_map).items()
        }
        held_out_pids = self._held_out_pids(node_pid_map)

        # Read collection size for random negative fallback
        collection_df = pd.read_csv(collection_path, sep='\t', header=None, names=['body'],
                                    encoding=locale.getpreferredencoding())
        all_pids = set(range(len(collection_df)))

        # Sibling tables reuse measure/dimension labels near-verbatim, so a large share of the
        # collection is a (near-)duplicate of some other document. Such a duplicate of a positive is
        # a degenerate negative. The objective would push apart two all-but-identical token
        # sequences, and the model ranks it highly by construction, so it lands at the very top of
        # the mined candidates. Compare on the node's own text, with the '[parent table]' prefix
        # that build_enriched_collection prepends removed: the same label under two different tables
        # gets different prefixes, which would otherwise hide the duplicate, while for siblings of
        # one table the shared prefix inflates the score and would cost us the hard negatives that
        # are actually worth keeping.
        node_texts = {pid: self._strip_table_prefix(b) for pid, b in collection_df['body'].items()}
        pids_by_text: Dict[str, List[int]] = {}
        for pid, text in node_texts.items():
            pids_by_text.setdefault(text, []).append(pid)

        # Index the collection using the checkpoint
        logger.info(f"Indexing collection for hard negative mining with checkpoint: {checkpoint_path}")
        index_name = f"{self.mode}.nbits={self.nbits}.hn_mining"
        mining_experiment = f"{self.base_path}/hn_mining_{os.path.basename(checkpoint_path)}"
        index_dir = f"{mining_experiment}/indexes/{index_name}"

        with Run().context(RunConfig(nranks=1, experiment=mining_experiment)):
            colbert_config = ColBERTConfig(nbits=self.nbits, root=os.path.dirname(checkpoint_path), index_bsize=32)
            indexer = Indexer(checkpoint=checkpoint_path, config=colbert_config)
            # metadata.json is written last, so its presence means the index finished.
            overwrite = 'reuse' if os.path.exists(f"{index_dir}/metadata.json") else True
            indexer.index(name=index_name, overwrite=overwrite, collection=collection_path)
            # Pass the collection explicitly. Without it Searcher falls back to the path stored in
            # the index metadata, which is whatever was indexed under that name last.
            searcher = Searcher(index=index_name, collection=collection_path, config=colbert_config)

        # Load existing triples for mixing if needed
        old_triples = {}
        if self.mix_ratio > 0 and os.path.exists(triples_path):
            with open(triples_path, 'r', encoding=locale.getpreferredencoding()) as f:
                for line in f:
                    triple = json.loads(line)
                    key = (triple[0], triple[1])
                    if key not in old_triples:
                        old_triples[key] = []
                    # One line carries every negative for this (query, positive) pair.
                    old_triples[key].extend(triple[2:])

        # Mine hard negatives
        new_triples = []
        n_dupes_blocked = 0
        for qpid, q_data in tqdm(positives_per_query.items(), desc='Mining hard negatives...'):
            ranking = searcher.search(q_data['query'], k=self.search_k)
            ranked_pids = ranking[0]

            positive_texts = [node_texts[p] for p in q_data['positives'] if p in node_texts]
            positive_text_set = set(positive_texts)

            # Positives plus every exact duplicate of one, for the random-padding fallback below.
            # Benchmark documents join them: they are never a positive, so nothing else keeps them
            # out of the negatives.
            blocked = set(q_data['positives']) | held_out_pids
            for text in positive_text_set:
                blocked.update(pids_by_text.get(text, ()))

            # Filter out positives and near-duplicates of them to get hard negative candidates
            # (ordered by model confidence)
            hard_neg_candidates = []
            for pid in ranked_pids:
                if pid in blocked:
                    continue
                if self._is_near_duplicate(node_texts.get(pid, ''), positive_texts, positive_text_set):
                    n_dupes_blocked += 1
                    continue
                hard_neg_candidates.append(pid)

            for positive in q_data['positives']:
                n_old = int(np.floor(self.negative_n * self.mix_ratio)) if self.mix_ratio > 0 else 0
                n_new = self.negative_n - n_old

                # Select hard negatives from model
                selected_negatives = hard_neg_candidates[:n_new]

                # Pad with random negatives if not enough hard negatives found
                if len(selected_negatives) < n_new:
                    logger.warning(f"Query {qpid}: only found {len(selected_negatives)}/{n_new} hard negatives, "
                                   f"padding with random negatives")
                    available_random = list(all_pids - blocked - set(selected_negatives))
                    n_pad = n_new - len(selected_negatives)
                    selected_negatives.extend(np.random.choice(available_random, size=min(n_pad, len(available_random)),
                                                               replace=False).tolist())

                # Mix in old negatives if requested
                if n_old > 0:
                    key = (qpid, positive)
                    old_negs = old_triples.get(key, [])
                    selected_old = old_negs[:n_old]
                    # If not enough old negatives, fill with more model-mined ones
                    if len(selected_old) < n_old:
                        extra = n_old - len(selected_old)
                        selected_negatives = hard_neg_candidates[:n_new + extra]
                    selected_negatives = selected_old + selected_negatives

                # LazyBatcher asserts len(passages) == nway * bsize, so a short line would abort
                # training partway through rather than simply weakening one step.
                if len(selected_negatives) < self.negative_n:
                    logger.warning(f"Query {qpid}: only {len(selected_negatives)}/{self.negative_n} "
                                   f"negatives for positive {positive}, skipping it.")
                    continue

                new_triples.append([qpid, positive, *selected_negatives[:self.negative_n]])

        # Backup previous triples file
        if os.path.exists(triples_path):
            round_num = 1
            while os.path.exists(triples_path.replace('.jsonl', f'_round_{round_num}.jsonl')):
                round_num += 1
            backup_path = triples_path.replace('.jsonl', f'_round_{round_num}.jsonl')
            shutil.copy2(triples_path, backup_path)
            logger.info(f"Backed up previous triples to {backup_path}")

        # Write new triples
        self._write_triples(new_triples, triples_path)

        logger.info(f"Mined {len(new_triples)} hard negative triples and saved to {triples_path}. "
                    f"Skipped {n_dupes_blocked} candidate(s) whose text was >={self.DUPLICATE_CUTOFF:.0%} "
                    f"similar to a positive of the same query.")

    def rebuild_collection(self, include_time_geo_dims: bool = False):
        """
            Rebuild the collection TSV and node_pid_map from the current state of the SPARQL graph.
            This can be used independently of training to update the collection when new tables or
            nodes are added to the graph. After rebuilding, any existing ColBERT index should be
            re-created by re-instantiating the ColBERTRetriever.

            Invalidates the pickle cache and removes stale index directories to ensure a clean state.

            :param include_time_geo_dims: whether to include time and geo dimensions in the collection
            :return: tuple of (collection DataFrame, node_pid_map dict)
        """
        pk_file = self.collection_path.replace('.tsv', '.pk')

        # Invalidate pickle cache to force a fresh fetch from the graph
        if os.path.exists(pk_file):
            logger.info(f"Removing stale collection cache: {pk_file}")
            os.remove(pk_file)

        # Prevents the construction of multi level folder structure caused by the URL scheme
        prefix_node_labels_name = re.sub(r"[./]", "_", re.sub(r"^https?://", "", config.GRAPH_DB_REPO))
        # Also invalidate the cached node labels so they are re-fetched from the graph
        for suffix in ['', '_including_time_geo']:
            labels_path = f"{self.base_path}/{prefix_node_labels_name}_node_labels{suffix}.json"
            if os.path.exists(labels_path):
                logger.info(f"Removing stale node labels cache: {labels_path}")
                os.remove(labels_path)

        # Fetch all nodes from the graph and enrich
        nodes = self.get_graph_node_labels(include_time_geo_dims=include_time_geo_dims)
        nodes = self.build_enriched_collection(nodes)
        nodes = {k: v for k, v in nodes.items() if self.mode == 'all' or v['type'] == self.mode}

        collection = pd.DataFrame.from_dict({
            i: doc | {'embedding': None}
            for i, (id_, doc) in tqdm(enumerate(nodes.items()), total=len(nodes), desc='Building collection...')
        }, orient='index')
        collection['id'] = nodes.keys()
        collection['body'] = collection['body'].str.replace(r'(((\r|\\r)?(\n|\\n))|\s+)', ' ', regex=True)

        os.makedirs(os.path.dirname(self.collection_path), exist_ok=True)

        # Save pickle cache for subsequent use by create_train_set
        with open(pk_file, 'wb') as f:
            pickle.dump(collection, f)

        if self.mode != 'all':
            collection = collection[collection['type'] == self.mode]
            collection.reset_index(drop=True, inplace=True)

        # Write TSV to disk => doc_pid \t body
        collection.to_csv(self.collection_path,
                          columns=['body'], header=False, sep='\t',
                          encoding=locale.getpreferredencoding(), errors='ignore')
        node_pid_map = {v['id']: k for k, v in collection.iterrows()}
        with open(self.collection_path.replace('.tsv', '_node_pid_map.json'), 'w') as f:
            json.dump(node_pid_map, f)

        # Remove stale ColBERT indexes that were built against the old collection
        index_dir = f"./experiments/{os.path.basename(self.base_path)}/indexes"
        if os.path.isdir(index_dir):
            logger.info(f"Removing stale indexes: {index_dir}")
            shutil.rmtree(index_dir)

        logger.info(f"Rebuilt collection: {len(collection)} documents, "
                    f"saved to {self.collection_path}")
        return collection, node_pid_map

    def create_train_set(self, include_time_geo_dims: bool = False):
        """
            Create a training dataset for the ColBERTv2 training. The training set consists of three separate files:
                * collection | tsv: pid, passage
                * queries | tsv: pid, passage
                * triples | jsonl: query, positive passage, negative passage
            The triples file contains `negative_n` samples of a query pid, the positive node/table and a selected
            negative node/table, based on a sampling done using the cosine similarity of the embedded description.
        """
        pk_file = self.collection_path.replace('.tsv', '.pk')

        if not os.path.exists(pk_file):
            # No cached collection — build it from the graph
            self.rebuild_collection(include_time_geo_dims=include_time_geo_dims)

        collection = pd.read_pickle(pk_file)
        if collection['embedding'].isnull().any():
            # Embed the descriptions if not done yet
            embeddings = {}
            for i, doc in tqdm(collection.iterrows(), total=len(collection), desc='Encoding collection...'):
                if doc['body'] in embeddings:
                    continue
                embeddings[doc['body']] = self._embed(doc['body'])

            collection['embedding'] = collection['body'].map(embeddings)
            with open(pk_file, 'wb') as f:
                pickle.dump(collection, f)

        if self.mode != 'all':
            collection = collection[collection['type'] == self.mode]
            collection.reset_index(drop=True, inplace=True)

        # Load the node_pid_map written by rebuild_collection
        with open(self.collection_path.replace('.tsv', '_node_pid_map.json'), 'r') as f:
            node_pid_map = json.load(f)

        # Validate that all tables and nodes referenced by QA pairs are present in the collection.
        # Missing entries mean the model can never retrieve them, silently hurting evaluation scores.
        # This might mean that the graph is incomplete
        missing_tables = set()
        missing_nodes = set()
        for r in self.queries:  # queries contains ALL QA pairs (train + test)
            golden_tables = parse_for_table_id(r['sexp'], 'sexp')
            golden_msrs = re.findall(r"(?:\(MSR\s\(\s*(?=[^)])|\G(?!^)\s+)(\w+)", r['sexp'])
            golden_dims = re.findall(r"(?:\(DIM\s\S+\s\(\s*(?=[^)])|\G(?!^)\s+)(\w+)", r['sexp'])

            for t in golden_tables:
                if t not in node_pid_map:
                    missing_tables.add(t)
                for n in golden_msrs + golden_dims:
                    node_id = f"{t}#{n}"
                    if node_id not in node_pid_map:
                        missing_nodes.add(node_id)

        if missing_tables and self.mode != 'node':
            logger.warning(f"{len(missing_tables)} table(s) referenced in QA data are missing from the collection: "
                           f"{missing_tables}")
        if missing_nodes and self.mode != 'table':
            logger.warning(f"{len(missing_nodes)} node(s) referenced in QA data are missing from the collection. "
                           f"Examples: {list(missing_nodes)[:10]}")

        # Build positives for train and test splits, and write query TSVs
        for split, ds in [('train', self.train_df), ('test', self.test_df)]:
            questions = self._build_positives(node_pid_map, dataset=ds)
            query_tsv = [(k, v['query']) for k, v in questions.items()]
            query_df = pd.DataFrame(query_tsv, columns=['i', 'q']).set_index('i', drop=True)

            path = self.queries_path.replace('.tsv', f"_{split}.tsv")
            query_df.to_csv(path, columns=['q'], header=False, sep='\t',
                            encoding=locale.getpreferredencoding(), errors='ignore')

        validation_path = f"{self.base_path}/validation_{self.mode}.jsonl"
        with open(validation_path, 'w', encoding=locale.getpreferredencoding()) as f:
            for question in self.test_df:
                f.write(json.dumps(question, ensure_ascii=False) + '\n')
        logger.info(f"Wrote {len(self.test_df)} held-out validation questions to {validation_path}")

        # Mine negatives using cosine similarity of the embedding model
        train_questions = self._build_positives(node_pid_map)
        self._mine_cosine_sim_negatives(train_questions, collection,
                                        held_out_pids=self._held_out_pids(node_pid_map))

    @staticmethod
    def recommend_colbert_config(model_checkpoint: str, collection_path: str):
        """
            Analyze the base model and collection to recommend doc_maxlen and dim values.
            Tokenizes all documents in the collection using the base model's tokenizer to find the
            token length distribution.

            If the checkpoint is a previously trained ColBERT model (contains artifact.metadata),
            the dim from that checkpoint is used to avoid shape mismatches in the projection layer.

            :param model_checkpoint: HuggingFace model name or local path for the base model
            :param collection_path: path to the collection TSV file
            :return: dict with recommended 'doc_maxlen' and 'dim' values
        """
        from transformers import AutoTokenizer, AutoConfig

        tokenizer = AutoTokenizer.from_pretrained(model_checkpoint)

        # Read collection documents from TSV (format: pid \t body)
        docs = pd.read_csv(collection_path, sep='\t', header=None, names=['body'],
                           encoding=locale.getpreferredencoding())
        token_lengths = [len(tokenizer.encode(str(doc), add_special_tokens=True))
                         for doc in tqdm(docs['body'], desc='Tokenizing collection for length analysis')]
        token_lengths = np.array(token_lengths)

        p50 = int(np.percentile(token_lengths, 50))
        p95 = int(np.percentile(token_lengths, 95))
        p99 = int(np.percentile(token_lengths, 99))
        max_len = int(token_lengths.max())

        # Round p99 up to the nearest power of 2 for ColBERT efficiency
        recommended_doc_maxlen = int(2 ** np.ceil(np.log2(p99)))

        # Determine dim: a previously trained ColBERT checkpoint has to keep its own, or the projection layer will not load.
        model_config = AutoConfig.from_pretrained(model_checkpoint)
        hidden_size = model_config.hidden_size

        checkpoint_metadata = os.path.join(model_checkpoint, 'artifact.metadata')
        if os.path.exists(checkpoint_metadata):
            with open(checkpoint_metadata) as f:
                checkpoint_dim = json.load(f).get('dim', ColBERTTrainer.DEFAULT_DIM)
            recommended_dim = checkpoint_dim
            dim_source = f"fixed by checkpoint artifact.metadata (trained with dim={checkpoint_dim})"
        else:
            recommended_dim = ColBERTTrainer.DEFAULT_DIM
            dim_source = "default for a fresh base model; ColBERT's own default is 128"

        logger.info(f"=== ColBERT config recommendations for '{model_checkpoint}' ==="
                    f"\n\tCollection size: {len(docs)} documents"
                    f"\n\tToken length distribution: median={p50}, p95={p95}, p99={p99}, max={max_len}"
                    f"\n\tRecommended doc_maxlen: {recommended_doc_maxlen} (covers 99% of documents, "
                    f"truncates {(token_lengths > recommended_doc_maxlen).sum()}/{len(docs)})"
                    f"\n\tModel hidden size: {hidden_size}"
                    f"\n\tRecommended dim: {recommended_dim} ({dim_source})")

        return {
            'doc_maxlen': recommended_doc_maxlen,
            'dim': recommended_dim,
            'token_length_stats': {'p50': p50, 'p95': p95, 'p99': p99, 'max': max_len},
        }

    @staticmethod
    def validate_dim(dim: int, nbits: int):
        """
            Check dim against the only constraints ColBERT places on it, both in the residual codec
            (colbert/indexing/codecs/residual.py): dim % 8 and dim % (nbits * 8) must be 0. Checked
            here because otherwise it surfaces as an assertion deep inside indexing, after training
            has already finished.

            :param dim: token embedding width
            :param nbits: quantization bits per dimension
            :raises ValueError: when the index could not be built from this combination
        """
        if dim % 8 or dim % (nbits * 8):
            raise ValueError(f"dim={dim} cannot be indexed at nbits={nbits}: ColBERT's residual codec "
                             f"requires dim divisible by 8 and by nbits*8 ({nbits * 8}). "
                             f"Nearest valid values: {dim - dim % (nbits * 8)} or "
                             f"{dim - dim % (nbits * 8) + nbits * 8}.")

    def _training_stats_path(self, round_num: int) -> str:
        """Where one round records what it actually trained on, for the model card to read back."""
        directory = self.base_path if round_num == 0 else f"{self.base_path}/round_{round_num}"
        return f"{directory}/training_stats.json"

    def _collect_evaluations(self, name: str, results_dir: str) -> List[dict]:
        """
            Evaluation results belonging to this model, for the card's evaluation table.

            Matched on the checkpoint name so a model never reports another's numbers.

            :param name: this model's directory name
            :param results_dir: directory of evaluate_table_retrieval.py result JSONs
            :return: one record per matching result file
        """
        evaluations = []
        if not os.path.isdir(results_dir):
            return evaluations

        for result_file in sorted(os.listdir(results_dir)):
            if not result_file.endswith('.json'):
                continue
            with open(os.path.join(results_dir, result_file)) as f:
                result = json.load(f)
            checkpoint = str(result.get('model_kwargs', {}).get('checkpoint', ''))
            if not checkpoint.startswith(name):
                continue

            metrics = result.get('metrics', {})
            # recall_at_k is the full curve; its last point is the recall at the k the evaluation ran with,
            # and its length is that k.
            curve = metrics.get('recall_at_k')
            perfect_curve = metrics.get('perfect_recall_at_k')
            evaluations.append({'checkpoint': checkpoint,
                                'dataset': os.path.basename(result.get('dataset_path', '?')),
                                'em': metrics.get('exact_match_accuracy', 0.0),
                                'r': curve[-1] if curve else None,
                                'pr': perfect_curve[-1] if perfect_curve else None,
                                'k': len(curve) if curve else None})
        return evaluations

    def write_hf_metadata(self) -> List[str]:
        """
            Generate HuggingFace model card metadata, as a YAML block

            :return: the block's lines, fences included
        """
        lines = ["---",
                 "pipeline_tag: sentence-similarity",
                 "library_name: transformers",
                 "language:",
                 f"  - {config.LANGUAGE}",
                 "license: mit"]

        if self.model_name and re.fullmatch(r'[\w.-]+(/[\w.-]+)?', self.model_name):
            lines.append(f"base_model: {self.model_name}")

        lines += ["tags:",
                  "  - colbert",
                  "  - colbertv2",
                  "  - late-interaction",
                  "  - information-retrieval",
                  "  - table-retrieval",
                  "  - gecko"]

        return lines + ["---"]

    def write_model_card(self, results_dir: str = 'evaluation/results'):
        """
            Render MODEL_CARD.md next to the model from the configuration, what each round actually trained
            on, and any evaluation results found for it. Regenerating can be done using --write_model_card
            once the evaluations have finished, to replace the placeholders with real numbers.

            :param results_dir: directory of evaluate_table_retrieval.py result JSONs to scan
        """
        name = os.path.basename(self.base_path)
        strategy = (f"iterative hard negative mining ({self.num_rounds} round(s))"
                    if self.num_rounds > 1 or self.hn_checkpoint else "single-round with cosine-sim negatives")
        split = getattr(self, 'split_summary', {})

        lines = [f"# {name}", "",
                 f"ColBERT retriever for GECKOv2 ({config.LANGUAGE}), "
                 f"generated {datetime.now().strftime('%Y-%m-%d %H:%M')}.", "",
                 "## Training configuration", "",
                 "| Setting | Value |", "| --- | --- |",
                 f"| Base model | `{self.model_name}` |",
                 f"| Mode | {self.mode} |",
                 f"| Training strategy | {strategy} |",
                 f"| Negative samples | {self.negative_n} per (query, positive) pair |",
                 f"| nway | {self.negative_n + 1} (1 positive + {self.negative_n} negatives per step) |",
                 f"| Learning rate | {self.learning_rate} |",
                 f"| Batch size | {self.batch_size} (accumsteps {self.accumsteps}, "
                 f"{self.batch_size // self.accumsteps} per forward pass) |",
                 f"| Epochs | {self.epochs} |",
                 f"| Search depth (k) | {self.search_k} |",
                 f"| Mix ratio | {self.mix_ratio} |",
                 f"| Quantization (nbits) | {self.nbits} |",
                 f"| Embedding model | `{self.embedding_model_name}`"
                 f"{' (Azure)' if self.azure else ''} |",
                 f"| Benchmark tables | {'excluded from negatives' if self.exclude_benchmark_tables else 'included (production model)'} |", ""]

        if split:
            lines += ["## Data", "",
                      f"- Split: {split['train_questions']} train / {split['test_questions']} validation "
                      f"questions, {split['train_tables']} train tables / {split['test_tables']} validation "
                      f"tables, {split['overlapping_tables']} overlapping (should be 0)"]
            if os.path.exists(self.collection_path):
                with open(self.collection_path, encoding=locale.getpreferredencoding(), errors='ignore') as f:
                    lines.append(f"- Collection: {sum(1 for _ in f):,} documents")
            lines.append("")

        rounds = []
        for round_num in range(0, self.num_rounds + 1):
            stats_path = self._training_stats_path(round_num)
            if os.path.exists(stats_path):
                with open(stats_path) as f:
                    rounds.append(json.load(f))
        if rounds:
            lines += ["## Rounds", "",
                      "| Round | Negatives from | Triples | Steps | doc_maxlen | dim | Finished |",
                      "| --- | --- | --- | --- | --- | --- | --- |"]
            lines += [f"| {r['round']} | {r['negatives']} | {r['triples']:,} | {r['steps']:,} | "
                      f"{r['doc_maxlen']} | {r['dim']} | {r['finished']} |" for r in rounds]
            lines.append("")

        evaluations = self._collect_evaluations(name, results_dir)

        lines += ["## Evaluation", "",
                  "| Checkpoint | Dataset | EM | R@k | PR@k |", "| --- | --- | --- | --- | --- |"]
        rows = []
        for evaluation in evaluations:
            recall = f"{evaluation['r']:.3f} (@{evaluation['k']})" if evaluation['r'] is not None else '-'
            perfect_recall = f"{evaluation['pr']:.3f} (@{evaluation['k']})" if evaluation['pr'] is not None else '-'
            rows.append(f"| `{evaluation['checkpoint']}` | `{evaluation['dataset']}` | "
                        f"{evaluation['em']:.3f} | {recall} | {perfect_recall} |")
        lines += rows or ["| _TBD_ | _TBD_ | _TBD_ | _TBD_ | _TBD_ |"]
        if not evaluations:
            lines += ["", "> No evaluation results found yet. Run the evaluation, then regenerate this "
                      "card with `--write_model_card` to fill the table in."]
        lines.append("")

        lines = self.write_hf_metadata() + lines

        card_path = f"{self.base_path}/MODEL_CARD.md"
        with open(card_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines))

        logger.info(f"Wrote model card to {card_path}")
        return card_path

    def train(self, model_checkpoint: str, round_num: int = 0):
        """
            Train a ColBERT model.

            :param model_checkpoint: HuggingFace model name or local checkpoint path
            :param round_num: training round number (0 = single-round/legacy, 1+ = iterative round)
        """
        recommendations = self.recommend_colbert_config(model_checkpoint, self.collection_path)

        # LazyBatcher stops at EOF, so one pass over the file is the whole run: the step count
        # follows from its length. maxsteps also sets the horizon the LR schedule decays over.
        with open(self.triples_path, encoding=locale.getpreferredencoding()) as f:
            n_triples = sum(1 for _ in f)
        maxsteps = max(1, math.ceil(n_triples / self.batch_size))

        doc_maxlen = self.doc_maxlen or recommendations['doc_maxlen']
        dim = self.dim or recommendations['dim']
        self.validate_dim(dim, self.nbits)

        # ColBERT validates nothing and returns the last checkpoint, so without this the only
        # artefact of a multi-epoch run is its final step. Keeping one per epoch turns --epochs into
        # something selectable after the fact, by scoring each on the validation split.
        if self.epochs > 1:
            steps_per_epoch = max(1, maxsteps // self.epochs)
            kept = set_checkpoint_steps(steps_per_epoch * epoch for epoch in range(1, self.epochs))
            logger.info(f"Keeping an intermediate checkpoint every {steps_per_epoch:,} steps "
                        f"(1 epoch): {sorted(kept)}")

        experiment = self.base_path if round_num == 0 else f"{self.base_path}/checkpoints_round_{round_num}"

        with Run().context(RunConfig(nranks=1, experiment=experiment)):
            # Note: regardless of given root, the model seems always to be saved in `experiments/<model_name>/**`
            colbert_cfg = ColBERTConfig(bsize=self.batch_size,
                                        lr=self.learning_rate,
                                        accumsteps=self.accumsteps,
                                        query_maxlen=64,
                                        doc_maxlen=doc_maxlen,
                                        dim=dim,
                                        # One positive scored against every negative on the line, as a nway-way softmax.
                                        # ColBERT defaults to nway=2, which would silently discard all but the first negative.
                                        nway=self.negative_n + 1,
                                        # Off by default; the other passages in the batch are free extra negatives and are what makes a large bsize pay off.
                                        use_ib_negatives=True,
                                        maxsteps=maxsteps,
                                        # Without a warmup the scheduler is None and the LR stays flat for the entire run.
                                        warmup=max(1, int(0.1 * maxsteps)),
                                        save_every=1_000)
            logger.info(f"Training on {n_triples:,} triples: {maxsteps:,} steps of {self.batch_size} "
                        f"(nway={self.negative_n + 1}, in-batch negatives on, "
                        f"doc_maxlen={doc_maxlen}, dim={dim}, "
                        f"warmup={max(1, int(0.1 * maxsteps)):,})")
            q_path = self.queries_path.replace('.tsv', '_train.tsv')
            trainer = Trainer(triples=self.triples_path, queries=q_path,
                              collection=self.collection_path, config=colbert_cfg)

            trainer.train(checkpoint=model_checkpoint)
            checkpoint_path = trainer.best_checkpoint_path()

            if round_num > 0:
                # Save to round-specific directory
                round_dir = f"{self.base_path}/round_{round_num}"
                logger.info(f"Saved checkpoint to {checkpoint_path}, copying to round dir {round_dir}")
                shutil.copytree(checkpoint_path, round_dir, dirs_exist_ok=True)
                # Copy collection and node_pid_map into the round directory for subsequent mining
                shutil.copyfile(self.collection_path, f"{round_dir}/collection_{self.mode}.tsv")
                shutil.copyfile(self.collection_path.replace('.tsv', '_node_pid_map.json'),
                                f"{round_dir}/collection_{self.mode}_node_pid_map.json")
                # Copy queries into the round directory
                q_train_path = self.queries_path.replace('.tsv', '_train.tsv')
                q_test_path = self.queries_path.replace('.tsv', '_test.tsv')
                if os.path.exists(q_train_path):
                    shutil.copyfile(q_train_path, f"{round_dir}/queries_{self.mode}_train.tsv")
                if os.path.exists(q_test_path):
                    shutil.copyfile(q_test_path, f"{round_dir}/queries_{self.mode}_test.tsv")

            # Always copy to base_path as "latest" (preserves backward compat)
            logger.info(f"Copying checkpoint files to {self.base_path} as latest.")
            shutil.copytree(checkpoint_path, self.base_path, dirs_exist_ok=True)
            shutil.copyfile(self.collection_path, f"{self.base_path}/collection.tsv")
            shutil.copyfile(self.collection_path.replace('.tsv', '_node_pid_map.json'),
                            f"{self.base_path}/node_pid_map.json")

        # Record what this round actually trained on, for the model card to read back. Written per
        # round because the triple count, step count and negative source all differ between them.
        if round_num == 0:
            negatives = 'cosine similarity of embedded descriptions'
        elif round_num == 1 and self.hn_checkpoint:
            negatives = f"mined from `{os.path.basename(self.hn_checkpoint)}`"
        elif round_num == 1:
            negatives = 'cosine similarity of embedded descriptions'
        else:
            negatives = f"mined from round {round_num - 1}"

        stats_path = self._training_stats_path(round_num)
        os.makedirs(os.path.dirname(stats_path), exist_ok=True)
        with open(stats_path, 'w') as f:
            json.dump({'round': round_num or 1,
                       'negatives': negatives,
                       'triples': n_triples,
                       'steps': maxsteps,
                       'epochs': self.epochs,
                       'doc_maxlen': doc_maxlen,
                       'dim': dim,
                       'checkpoint': checkpoint_path,
                       'finished': datetime.now().strftime('%Y-%m-%d %H:%M')}, f, indent=4)

        self.write_model_card()

        logger.info(f"=== DONE TRAINING (round {round_num}) ===")

    def train_iterative(self):
        """
            Orchestrate multi-round iterative hard negative mining and training.
            Uses self.num_rounds, self.hn_checkpoint, self.search_k, self.negative_n,
            self.nbits, and self.mix_ratio for configuration.
        """
        os.makedirs(self.base_path, exist_ok=True)

        # When starting from an external checkpoint, copy shared dataset files into base_path
        # so that train() and recommend_colbert_config() can find them at the expected paths
        if self.hn_checkpoint is not None:
            hn_files = [
                (f"{self.hn_checkpoint}/collection_{self.mode}.tsv", self.collection_path),
                (f"{self.hn_checkpoint}/collection_{self.mode}_node_pid_map.json",
                 self.collection_path.replace('.tsv', '_node_pid_map.json')),
                (f"{self.hn_checkpoint}/queries_{self.mode}_train.tsv",
                 self.queries_path.replace('.tsv', '_train.tsv')),
                (f"{self.hn_checkpoint}/queries_{self.mode}_test.tsv",
                 self.queries_path.replace('.tsv', '_test.tsv')),
            ]
            for src, dst in hn_files:
                if os.path.exists(src) and src != dst:
                    shutil.copyfile(src, dst)
                    logger.info(f"Copied {src} -> {dst}")

        for round_num in range(1, self.num_rounds + 1):
            logger.info(f"=== Starting round {round_num}/{self.num_rounds} ===")

            if round_num == 1 and self.hn_checkpoint is not None:
                # Skip initial training — mine hard negatives from provided checkpoint
                logger.info(f"Mining hard negatives from provided checkpoint: {self.hn_checkpoint}")
                self._mine_hard_negatives_from_model(checkpoint_path=self.hn_checkpoint)
                # Copy triples from hn_checkpoint dir to base_path
                hn_triples = f"{self.hn_checkpoint}/triples_{self.mode}.jsonl"
                if hn_triples != self.triples_path:
                    shutil.copyfile(hn_triples, self.triples_path)
                self.train(model_checkpoint=self.hn_checkpoint, round_num=round_num)

            elif round_num == 1 and self.hn_checkpoint is None:
                # Round 1 without existing checkpoint: use cosine-sim negatives
                if not self.skip_create_dataset:
                    self.create_train_set()
                self.train(model_checkpoint=self.model_name, round_num=round_num)

            else:
                # Rounds 2+: mine hard negatives from previous round's checkpoint
                prev_round_dir = f"{self.base_path}/round_{round_num - 1}"
                logger.info(f"Mining hard negatives from round {round_num - 1} checkpoint: {prev_round_dir}")
                self._mine_hard_negatives_from_model(checkpoint_path=prev_round_dir)
                # Copy mined triples to base_path
                mined_triples = f"{prev_round_dir}/triples_{self.mode}.jsonl"
                if mined_triples != self.triples_path:
                    shutil.copyfile(mined_triples, self.triples_path)
                self.train(model_checkpoint=prev_round_dir, round_num=round_num)

        logger.info(f"=== Iterative training complete ({self.num_rounds} rounds). "
                    f"Latest checkpoint at {self.base_path} ===")

    def run(self):
        """Main entry point: dispatches to single-round or iterative training."""
        if self.num_rounds > 1 or self.hn_checkpoint is not None:
            self.train_iterative()
        else:
            if not self.skip_create_dataset:
                self.create_train_set()
            self.train(model_checkpoint=self.model_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='ColBERTv2 training script')
    parser.add_argument('--skip_create_dataset', action='store_true',
                        help='Skip creating a new dataset (collection & triples) when already present.')
    parser.add_argument('--model_name', type=str, default='BAAI/bge-m3',
                        help='Name/Huggingface handle of the starting checkpoint')
    parser.add_argument('--embedding_model', type=str, default='sentence-transformers/all-MiniLM-L12-v2',
                        help='Name/Huggingface handle of embedding model.')
    parser.add_argument('--azure', action='store_true',
                        help='Envoke the Azure AI/ML API on the given model_name for generating embeddings. '
                             'If False, the model will be retrieved from SentenceTransformers.')
    parser.add_argument('--negative_n', type=int, default=10,
                        help='Number of hard mined negatives per training same.')
    parser.add_argument('--output_name', type=str, required=True,
                        help='Output name and dir of the trained model')
    parser.add_argument('--mode', type=str, choices=['table', 'node', 'all'], required=True,
                        help='(Sub)set of nodes to train model for')
    parser.add_argument('--learning_rate', '-lr', type=float, dest='learning_rate', default=5e-6,
                        help='Learning rate for the model trainer')
    parser.add_argument('--batch_size', '-b', type=int, dest='batch_size', default=8,
                        help='Batch size for the model trainer')
    parser.add_argument('--num_rounds', type=int, default=1,
                        help='Number of training rounds. Round 1 uses cosine-sim negatives, '
                             'rounds >=2 mine from the trained model.')
    parser.add_argument('--hn_checkpoint', type=str, default=None,
                        help='Path to a pre-trained checkpoint to use for hard negative mining from round 1. '
                             'Skips initial cosine-sim training. The checkpoint directory must contain '
                             'the collection, queries, and node_pid_map files.')
    parser.add_argument('--search_k', type=int, default=100,
                        help='Search depth for hard negative mining.')
    parser.add_argument('--mix_ratio', type=float, default=0.0,
                        help='Fraction of negatives to keep from previous round (0.0 = all model-mined).')
    parser.add_argument('--nbits', type=int, choices=[1, 2, 4, 8], default=4,
                        help='Number of bits for quantization during hard negative mining indexing.')
    parser.add_argument('--epochs', type=int, default=1,
                        help='Passes over the triples. ColBERT stops at the end of the triples file, '
                             'so this is expressed by repeating the shuffled triples that many times.')
    parser.add_argument('--accumsteps', type=int, default=1,
                        help='Gradient accumulation steps. batch_size // accumsteps passages go '
                             'through each forward pass, so >1 shrinks the in-batch negative pool.')
    parser.add_argument('--doc_maxlen', type=int, default=None,
                        help='Override the document length recommended from the collection. Cost in '
                             'time, memory and index size is linear in this.')
    parser.add_argument('--dim', type=int, default=None,
                        help='Override the token embedding width. Index size and query latency are '
                             "linear in this; ColBERT's default is 128. Ignored when continuing from "
                             'a trained ColBERT checkpoint, which fixes its own dim.')
    parser.add_argument('--include_benchmark_tables', action='store_true',
                        help='Allow documents from the held-out benchmark tables to be sampled as '
                             'negatives. Off by default, which keeps the generalisation benchmark '
                             'honest; turn it on for a production model, where nothing is held out.')
    parser.add_argument('--write_model_card', action='store_true',
                        help='Regenerate MODEL_CARD.md from the recorded training stats and any '
                             'evaluation results found, then exit. Run this after evaluating to '
                             'replace the placeholders with real numbers.')
    parser.add_argument('--rebuild_collection', action='store_true',
                        help='Rebuild the collection TSV and node_pid_map from the current SPARQL graph, '
                             'then exit. Does not train or require QA data. Useful when new tables/nodes '
                             'have been added to the graph and the index needs updating.')

    args = parser.parse_args()

    if args.write_model_card:
        trainer = ColBERTTrainer(
            output_name=args.output_name,
            mode=args.mode,
            negative_n=args.negative_n,
            learning_rate=args.learning_rate,
            batch_size=args.batch_size,
            num_rounds=args.num_rounds,
            hn_checkpoint=args.hn_checkpoint,
            search_k=args.search_k,
            mix_ratio=args.mix_ratio,
            nbits=args.nbits,
            epochs=args.epochs,
            accumsteps=args.accumsteps,
            doc_maxlen=args.doc_maxlen,
            dim=args.dim,
            exclude_benchmark_tables=not args.include_benchmark_tables
        )
        trainer.write_model_card()
    elif args.rebuild_collection:
        trainer = ColBERTTrainer(
            output_name=args.output_name,
            mode=args.mode,
            rebuild_only=True,
        )
        trainer.rebuild_collection()
    else:
        trainer = ColBERTTrainer(
            output_name=args.output_name,
            mode=args.mode,
            model_name=args.model_name,
            embedding_model_name=args.embedding_model,
            negative_n=args.negative_n,
            learning_rate=args.learning_rate,
            batch_size=args.batch_size,
            azure=args.azure,
            skip_create_dataset=args.skip_create_dataset,
            num_rounds=args.num_rounds,
            hn_checkpoint=args.hn_checkpoint,
            search_k=args.search_k,
            mix_ratio=args.mix_ratio,
            nbits=args.nbits,
            epochs=args.epochs,
            accumsteps=args.accumsteps,
            doc_maxlen=args.doc_maxlen,
            dim=args.dim,
            exclude_benchmark_tables=not args.include_benchmark_tables
        )
        trainer.run()
