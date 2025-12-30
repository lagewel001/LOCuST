import os
from openai import AzureOpenAI
from itertools import groupby
from operator import itemgetter
from rdflib.namespace import SKOS, DCTERMS as DCT, QB
from typing import Tuple, Optional, List

import config
from models.generators.base_generator import BaseGenerator
from models.retrievers.colbert.colbert_retriever import ColBERTRetriever
from odata_graph import engine
from odata_graph.sparql_controller import QUDT
from s_expression import Table, Dimension, uri_to_code


class GPTBaselineSQLModel(BaseGenerator):
    """A baseline model for generating SQL queries using a GPT model."""
    retriever: ColBERTRetriever

    def __init__(self, checkpoint: str, reasoning: str = 'False'):
        super().__init__()
        self.retriever = ColBERTRetriever(checkpoint=checkpoint, mode='table')
        self.reasoning = True if reasoning == 'True' else False

        self.model_name = "gpt-5.1"
        self.ml_client = AzureOpenAI(
            api_version=config.AZURE_API_VERSION,
            azure_endpoint=config.AZURE_ENDPOINT,
            api_key=config.AZURE_KEY,
        )

    @staticmethod
    def _build_prompts(question: str, tables: dict) -> Tuple[str, str]:
        """
            Builds a prompt for the LLM based on the question and retrieved tables.

            :param question: question string
            :param tables: dictionary of ranked tables and their nodes
        """
        table_info = []
        for table_id, data in tables.items():
            # Get graph elements for tables
            table_node = Table(table_id)
            # FIXME: for some reason when running the local graph, time and geo dims are returned. Using GraphDB this does work correctly
            table_graph = engine.get_table_graph(table_node, include_time_geo_dims=False)

            units = dict(map(lambda x: (uri_to_code(x[0]), x[1].split('/')[-1]), table_graph.subject_objects(QUDT.unit)))
            concepts = set(map(lambda x: (x[1], x[0]), table_graph.subject_objects(QB.concept)))
            labels = set(table_graph.subject_objects(SKOS.prefLabel))

            # Inner join concepts with labels
            def inner_join(a, b):
                L = a + b
                L.sort(key=itemgetter(0))  # sort by the first column
                for _, group in groupby(L, itemgetter(0)):
                    row_a, row_b = next(group), next(group, None)
                    if row_b is not None:  # join
                        yield row_a[1:] + row_b[1:]  # cut 1st column from 2nd row

            concept_labels = dict(inner_join(list(concepts), list(labels)))

            labels = set(table_graph.objects(table_node.uri, DCT.title) or {''})
            if len(labels) == 0:
                labels = {''}
            table_info.append(f"Table: {table_id} "
                              f"(label: \"{str(labels.pop())}\") "
                              f"(score: {data.get('score', '-')})")

            nodes_info = []
            for code, label in concept_labels.items():
                node_type = 'dimensions' if Dimension.rdf_ns in code else 'measures'
                node_code = uri_to_code(code)
                nodes_info.append(f"- {node_type.capitalize()[:-1]}: {node_code} "
                                  f"(label: \"{label}\") "
                                  f"{' (unit: ' + units[node_code] + ')' if node_code in units else ''}")
                if len(nodes_info) == 50:  # Don't make list too long
                    break

            if nodes_info:
                table_info.append("Nodes:")
                table_info.extend(nodes_info)
            table_info.append("")

        table_info_str = chr(10).join(table_info)

        prompt_path = os.path.join(os.path.dirname(__file__), f'{config.LANGUAGE}_prompt.txt')
        with open(prompt_path, 'r', encoding='utf-8') as f:
            prompt_template = f.read()

        system_prompt = prompt_template.split('[SYSTEM]')[1].split('[USER]')[0].strip()
        user_prompt_template = prompt_template.split('[USER]')[1].strip()

        user_prompt = user_prompt_template.format(question=question, table_info=table_info_str)
        return system_prompt, user_prompt

    def _call_llm(self, system_prompt: str, user_prompt: str) -> str:
        """Calls the Azure ML endpoint and returns the generated SQL query."""
        try:
            response = self.ml_client.chat.completions.create(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                model=self.model_name,
                max_completion_tokens=10000,
                reasoning_effort="high" if self.reasoning else NOT_GIVEN
            )

            num_tokens = response.usage.completion_tokens
            response_data = response.choices[0].message.content
            response_data = response_data.replace('```sql\n', '').replace('\n```', '')
            return response_data, num_tokens
        except Exception as e:
            print(f"An error occurred while calling the LLM: {e}")
            return ""

    def generate_query(self, question: str, k: int = 5, golden_tables: Optional[List[str]] = None) -> Tuple[str, int]:
        """Generates a SQL query for the given question."""
        if golden_tables is None:
            retrieved_tables = self.retriever.retrieve_tables(question, k=k)
        else:
            retrieved_tables = {t: {} for t in golden_tables}

        if not retrieved_tables:
            return "", 0  # Cannot generate a query

        system_prompt, user_prompt = self._build_prompts(question, retrieved_tables)
        sql_query, num_tokens = self._call_llm(system_prompt, user_prompt)
        return sql_query, num_tokens
