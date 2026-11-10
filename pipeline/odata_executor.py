"""
    This module retrieves the data from an S-expression and returns a pivot table as a pandas DataFrame.
    This module is similar to the ` db_executor`, which does the same for SQL queries.
"""
import numpy as np
import pandas as pd
from rdflib import SKOS, QB, DCTERMS as DCT
from typing import Dict, List, Set, FrozenSet, Tuple

import config
from odata_graph import engine
from odata_graph.sparql_controller import QUDT, LevelSelection
from s_expression.mapper import CodeLabelMapper
from s_expression import Table, Measure, Dimension, uri_to_code
from utils.custom_types import ComparisonOperator, Connective, COMPARISON_OPERATORS, UnitCompatibilityError
from utils.global_functions import secure_request

class OData3QueryBuilder(object):
    """
        Helper class for generating a OData3 request/query to obtain
        observations of a table using given measure and dimension filters.
    """
    filters = False

    def __init__(self, table_id: Table):
        self.query = f"{config.ODATA3_BASE_URL}/{table_id}/TypedDataSet"

    def add_selects(self, measures: Set[Measure], dim_groups: Set[Dimension]):
        self.query += f'''&$select=ID, {", ".join([str(s) for s in measures | dim_groups])}'''

    def add_dim_filter(self, group: Dimension, codes: Set[Dimension]):
        if len(codes) > 0:
            self.query += '?$filter=' if not self.filters else ' and '
            self.query += f'''((substringof({')) or (substringof('.join(["'" + str(dim) + "'," + str(group) for dim in codes])})))'''
            self.filters = True

    def add_prefix_filter(self, group: Dimension, prefix: str):
        """Filter a dimension on a code prefix (e.g. all WK-codes) instead of enumerating codes."""
        self.query += '?$filter=' if not self.filters else ' and '
        self.query += f"startswith({group},'{prefix}')"
        self.filters = True

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return self.query


class OData4QueryBuilder(object):
    """
        Helper class for generating a OData4 request/query to obtain
        observations of a table using given measure and dimension filters.
    """
    filters = False

    def __init__(self, table_id: Table):
        self.query = f"{config.ODATA4_BASE_URL}/{table_id}/Observations/"

    def add_msr_filter(self, measures: Set[Measure]):
        self.query += '?$filter=' if not self.filters else ' and '
        self.query += f'''Measure in ('{"', '".join([str(m) for m in measures])}')'''
        self.filters = True

    def add_dim_filter(self, group: Dimension, codes: Set[Dimension]):
        if len(codes) > 0:
            self.query += '?$filter=' if not self.filters else ' and '
            # There's no distinction between codes of given dim group and all codes, but that shouldn't matter
            self.query += f'''{group} in ('{"', '".join([str(dim) for dim in codes])}')'''
            self.filters = True

    def add_prefix_filter(self, group: Dimension, prefix: str):
        """Filter a dimension on a code prefix (e.g. all WK-codes) instead of enumerating codes."""
        self.query += '?$filter=' if not self.filters else ' and '
        self.query += f"startswith({group},'{prefix}')"
        self.filters = True

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return self.query


class ODataExecutor(object):
    """
        Object for handling OData4 requests, following the execution of a VALUE expression.
        Requires a table identifier and a set of measure and dimension identifiers for filtering.

        :param table: URI and identifier of table to fetch
        :param measures: set of measure URIs to filter table on
        :param dims: set of dimension URIs to filter table on
    """
    def __init__(self,
                 table: Table,
                 measures: Set[Measure],
                 measure_filters: List[Tuple[Measure, ComparisonOperator, float]],
                 dims: Set[Tuple[Dimension, FrozenSet[Dimension]]],
                 filter_connective: Connective = 'AND',
                 level_filters: Dict[str, LevelSelection] = None):
        self.table = table
        self.measures = measures
        self.measure_filters = measure_filters
        self.filter_connective = filter_connective
        self.dims = dims
        self.level_filters = level_filters or {}

        if config.LANGUAGE == 'nl':
            self._odata4 = True
        else:
            self._odata4 = False

        self._odata_query = None
        # The label graph is fetched lazily in query_odata(), scoped to the codes the OData API actually returns.
        self._graph = None

    def _filter_observations(self, observations: List[Dict]) -> List[Dict]:
        """
            Apply the measure value condition(s) client-side. A single condition keeps only the
            observations of the filtered measure that satisfy it. Multiple conditions combined with
            AND/OR work on the dimension context level: a context (the unique combination of dimension
            values of an observation) survives when all (AND) or any (OR) of the conditioned measures
            satisfy their condition, after which all observations of the surviving contexts are kept.

            :param observations: OData4(-style) observations, one measure value per observation
            :returns: the observations that satisfy the measure value condition(s)
        """
        if len(self.measure_filters) == 1:
            msr, op, value = self.measure_filters[0]
            return [obs for obs in observations if obs['Measure'] == str(msr)
                    and COMPARISON_OPERATORS[op](obs['Value'], value)]

        non_context_keys = {'ID', 'Id', 'Measure', 'Value', 'ValueAttribute', 'StringValue'}
        context = lambda obs: tuple((k, obs[k]) for k in sorted(obs) if k not in non_context_keys)

        context_values: Dict[Tuple, Dict[str, float]] = {}
        for obs in observations:
            context_values.setdefault(context(obs), {})[obs['Measure']] = obs['Value']

        connective = all if self.filter_connective == 'AND' else any
        surviving = {ctx for ctx, msr_values in context_values.items()
                     if connective(str(msr) in msr_values and COMPARISON_OPERATORS[op](msr_values[str(msr)], value)
                                   for msr, op, value in self.measure_filters)}
        return [obs for obs in observations if context(obs) in surviving]

    def query_odata(self,
                    index_cols: FrozenSet = frozenset({'Measure', 'Unit'}),
                    friendly_labels: bool = True) -> Tuple[pd.DataFrame, CodeLabelMapper, CodeLabelMapper]:
        """
            Retrieve values from OData4 and create a pivot table using table, measure and dimension selectors.

            :param index_cols: the columns to use as the MultiIndex for the output pivot tables.
                               Defaults to ['Measure', 'Unit']
            :param friendly_labels: map the OData4 codes in the output table to their friendly names.
                                    Set to False when wanting to JOIN the pivot tables at a later stage.
            :returns: tuple containing a DataFrame pivot table, mapper for codes to labels and mapper for codes to definitions
        """
        table = self.table

        # For (LEVEL ...) selections with an exactness-proven code prefix, push a startswith filter into the OData
        # request instead of enumerating (potentially thousands of) codes, which would overflow the request URL.
        # Time levels match on an infix, which OData can't express positionally, so those keep the enumerated filter.
        def _add_dim_filter(builder, group: Dimension, codes: Set[Dimension]):
            selection = self.level_filters.get(str(group))
            if selection and selection.sql_pattern and selection.sql_pattern[0] == 'like':
                builder.add_prefix_filter(group, selection.sql_pattern[1].rstrip('%'))
            else:
                builder.add_dim_filter(group, codes)

        if self._odata4:  # Dutch
            self._odata_query = OData4QueryBuilder(table)
            self._odata_query.add_msr_filter(self.measures)
            for dim in self.dims:
                _add_dim_filter(self._odata_query, dim[0], set(dim[1]))
        else:  # English, OData3
            self._odata_query = OData3QueryBuilder(table)
            for dim in self.dims:
                _add_dim_filter(self._odata_query, dim[0], set(dim[1]))
            self._odata_query.add_selects(self.measures, {d[0] for d in self.dims})

        try:
            observations = secure_request(str(self._odata_query), json=True, max_retries=9, timeout=8)

            if observations is None:
                raise ValueError("Retrieved OData data is empty.")
            elif 'error' in observations:
                raise RuntimeError(observations['error']['message'])
            elif 'value' not in observations or not observations['value']:
                raise ValueError("Retrieved OData data is empty.")
            else:
                observations = observations['value']

            if not self._odata4:
                # Translate OData3 result to OData4
                msr_str = {str(m) for m in self.measures}
                odata4_observations = []

                for obs in observations:
                    for i, msr in enumerate(msr_str):
                        odata4_obs = {k: v for k, v in obs.items() if k not in msr_str}
                        odata4_obs['ID'] = f"{obs['ID']}_{i}"
                        odata4_obs['Measure'] = msr
                        odata4_obs['Value'] = float(obs[msr] if obs[msr] is not None else np.nan)
                        odata4_observations.append(odata4_obs)

                observations = odata4_observations

            # Fetch friendly labels scoped to the codes present in the response. This keeps the CONSTRUCT bounded.
            reserved = {'ID', 'Id', 'Measure', 'Value', 'ValueAttribute', 'StringValue'}
            present_codes = {str(v) for obs in observations for k, v in obs.items() if k not in reserved and v is not None}
            self._graph = engine.get_table_graph(table=table, geo_time_codes=present_codes)

            # Get friendly names of codes
            concept_labels = dict(self._graph.subject_objects(SKOS.prefLabel))
            concept_definitions = dict(self._graph.subject_objects(SKOS.definition))

            code_labels = CodeLabelMapper(
                # MSR/DIM-Concept mapping
                {str(obs).split(Measure.rdf_ns if obs in Measure.rdf_ns else Dimension.rdf_ns)[-1]: str(concept_labels[concept])
                 for obs, concept in self._graph.subject_objects(QB.concept)} |
                # Table title
                {str(table).split(Table.rdf_ns)[-1]: str(title)
                 for table, title in self._graph.subject_objects(DCT.title)}
            )

            code_definitions = CodeLabelMapper(
                # MSR/DIM-Concept mapping
                {str(obs).split(Measure.rdf_ns if obs in Measure.rdf_ns else Dimension.rdf_ns)[-1]: str(concept_definitions[concept])
                 for obs, concept in self._graph.subject_objects(QB.concept) if concept in concept_definitions}
            )

            # Filter values
            # CBS' API denies the Value filter in OData4 and Measure filters in OData3, so filtering needs
            #  to happen client-side
            if self.measure_filters:
                observations = self._filter_observations(observations)

            result_df = pd.DataFrame()
            if len(observations) > 0:
                obs_df = pd.DataFrame(observations)
                obs_df = obs_df.set_index('Id' if self._odata4 else 'ID', drop=True)
                if self._odata4:
                    obs_df = obs_df.drop(['ValueAttribute', 'StringValue'], axis=1)

                units = {uri_to_code(m): str(u) for m, u in self._graph.subject_objects(QUDT.unitOfSystem)}
                obs_df['Unit'] = obs_df['Measure'].map(units).replace({np.nan: ''})

                if friendly_labels:
                    obs_df = obs_df.rename(columns=code_labels)
                    for c in obs_df.columns:
                        obs_df[c] = obs_df[c].replace(code_labels)
                    index_cols = [code_labels.get(c, c) for c in index_cols]

                for i in index_cols:
                    if i not in obs_df.columns:
                        # Check if selector is one of the measures and drop all non-related
                        # Note: only ONE measure can be a selector. Secondary measures as selector will be ignored
                        obs_df = obs_df[obs_df['Measure'] == i]
                        if obs_df.empty:
                            raise ValueError(f"Selector column '{i}' not found in data table "
                                             f"{table} with columns {obs_df.columns.values}.")
                        else:
                            index_cols = {'Measure', 'Unit'}

                result_df = obs_df.pivot_table(index=index_cols,
                                               columns=[c for c in obs_df if c not in ['Value'] + list(index_cols)],
                                               values='Value')
        except (UnitCompatibilityError, SyntaxError) as e:
            raise e
        except Exception as e:
            raise RuntimeError(f"Failed to retrieve data from OData{'4' if self._odata4 else '3'} request {str(self._odata_query)}: {str(e)}")

        return result_df, code_labels, code_definitions
