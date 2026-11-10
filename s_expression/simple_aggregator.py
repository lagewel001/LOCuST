from __future__ import annotations

import pandas as pd
import sqlglot
from abc import ABC
from beartype import beartype
from typing import List, Tuple, Set, FrozenSet

from pipeline.db_executor import DBExecutor
from pipeline.odata_executor import CodeLabelMapper
from s_expression import Expression, ParsedExpression, Table, Measure, Dimension
from s_expression.operators import Value


class SimpleAggregator(Expression, ABC):
    """
        Shared logic for simple aggregator functions SUM, AVG, MIN, MAX

        :param sexp: the parsed S-expression, of which the evaluation instantiates this object
        :param selectors: axes to aggregate the intermediate table results over
        :param sub_expression: sub-expression that will be executed and used for aggregating over
    """
    @beartype
    def __init__(self, sexp: ParsedExpression, selectors: List[str], sub_expression: Expression):
        super().__init__(sexp)
        self.sub_expression = sub_expression
        self.selectors = selectors

        # Selectors of complex inner queries must match
        sub_exps = [self.sub_expression]
        for sub_exp in sub_exps:  # Get inner VALUE expression(s)
            if isinstance(sub_exp, Value):
                continue
            else:
                # TODO: implement aggregations over complex inner queries for non-measure aggregations
                if len(self.selectors):
                    raise NotImplementedError("Can't execute aggregations over complex inner queries "
                                              "with a dimension as the aggregation selector yet.")

                inner_selectors = [] if 'Measure' in sub_exp.selectors else sub_exp.selectors
                if inner_selectors != self.selectors:
                    raise ValueError(f"Selectors of all complex inner expressions must be identical.\n"
                                     f"Selector {self.selectors} in the {self._operator} expression\n"
                                     f"does not match {sub_exp.selectors} in the {sub_exp._operator} sub-expression")

            if hasattr(sub_exp, 'sub_expressions'):
                sub_exps.extend(sub_exp.sub_expressions)
            else:
                sub_exps.append(sub_exp.sub_expression)

    def _validate_partition_coherence(self):
        """
            Hard gate called by the concrete aggregation operators at the start of execution.
            For every selector dimension, the inner VALUE expressions' code-sets must be partition-
            coherent, i.e. sit in exactly one dimension group and not mix a total with siblings, otherwise
            the aggregation double-counts and a PartitionCoherenceError forces an explicit rewrite.

            An aggregation without selectors collapses the Measure axis instead, so there the
            selected measures must be coherent within each VALUE's table-scoped measure scheme
            (checked per VALUE: a JOIN can span multiple tables). Tables without a measure
            scheme (no MeasureGroups/TopicGroups) fail open.
        """
        from odata_graph import engine  # importing at module level results in a circular import

        value_exps, *_ = self._get_sub_exp_filters()
        if not self.selectors:
            for value in value_exps:
                engine.validate_measure_coherence(value.table, value.measures)
            return

        for value in value_exps:
            for dim, dim_codes in value.dimensions:
                if str(dim) in self.selectors:
                    if str(dim) in value.level_filters:
                        continue  # a (LEVEL ...) selection is same-level by construction
                    engine.validate_partition_coherence(value.table, dim, dim_codes)

    def _validate_unit_compatibility(self):
        """
            Hard gate for the SQL execution and the static Expression.validate() walk. Only an aggregation
            over the measures combines values of different measures, so only then must their units match
            (as in native mode). Aggregating over a dimension group keeps every measure separate. SQL cannot
            rescale measures with different conversion multipliers, so these are rejected as well
            (unlike native mode, which rescales them). Being a property of the S-expression, this applies
            to both SQL formats.

            :raises UnitCompatibilityError: when the aggregated measures have incompatible units
        """
        from odata_graph import engine  # importing at module level results in a circular import

        if not self.selectors:
            _, _, measures, _ = self._get_sub_exp_filters()
            engine.validate_msr_unit_compatibility(measures, allow_different_scaling=False)

    def _validate_coherence(self):
        """Node-local coherence and unit gates for the static Expression.validate() walk."""
        self._validate_partition_coherence()
        self._validate_unit_compatibility()

    def _get_sub_exp_filters(self) -> Tuple[List[Value], Set[Table], Set[Measure], Set[Tuple[Dimension, FrozenSet[Dimension]]]]:
        """
            Helper function for getting all tables, measures and dimensions from all
            inner VALUE expressions in this S-expression, regardless of their depth.

            :returns: Tuple containing a list of inner VALUE expressions, set with tables,
                      set with measures and set with dimensions
        """
        tables = set()
        measures = set()
        dimensions = set()
        value_exps = []
        sub_exps = [self.sub_expression]
        for sub_exp in sub_exps:  # Get inner VALUE expression(s)
            if isinstance(sub_exp, Value):
                tables |= { sub_exp.table }
                measures |= sub_exp.measures
                dimensions |= sub_exp.dimensions
                value_exps.append(sub_exp)
                continue

            if hasattr(sub_exp, 'sub_expressions'):
                sub_exps.extend(sub_exp.sub_expressions)
            else:
                sub_exps.append(sub_exp.sub_expression)

        return value_exps, tables, measures, dimensions

    @staticmethod
    def _ranked_cte_sql(ranked_select: str, rank_filter: str) -> str:
        """
            Wrap a SELECT containing a `RANK() ... AS rnk` window function into a CTE filtered
            on the rank. This is the shared pattern for the MIN/MAX type queries.

            :param ranked_select: the SELECT statement containing the rnk window column
            :param rank_filter: the WHERE condition on rnk (e.g. 'rnk = 1' or 'rnk <= 3')
            :returns: prettified SQL string
        """
        sql = f"""
            WITH RankedRows AS ({ranked_select})
            SELECT *
            FROM RankedRows
            WHERE {rank_filter}
        """
        return sqlglot.parse_one(sql).sql(pretty=True)

    def _non_pivot_value_parts(self) -> Tuple[List[str], List[str], str, str]:
        """
            Helper for the non-pivoted aggregation SQLs over a single inner VALUE expression.

            :returns: tuple containing the sorted measure codes, the sorted dimension groups that are NOT
                      aggregated over (i.e. all groups except the selectors), the FROM clause and the WHERE clause
        """
        value_sql = sqlglot.parse_one(self.sub_expression.odata3_sql)
        where = value_sql.find(sqlglot.exp.Where)
        measures = sorted(str(msr) for msr in self.sub_expression.measures)
        kept_dims = sorted(str(group) for group, _ in self.sub_expression.dimensions
                           if str(group) not in self.selectors)
        return measures, kept_dims, value_sql.find(sqlglot.exp.From).sql(), where.sql() if where else ''

    def _non_pivot_measure_combination(self, measures: List[str]) -> str:
        """Combine multiple measure columns into one value per row when aggregating over measures."""
        return ' + '.join(f'"{msr}"' for msr in measures)

    def _non_pivot_aggregation_sql(self) -> str:
        """
            Non-pivoted OData3 SQL for aggregating (SUM/AVG) over a single VALUE expression. Like the
            pivoted variant, the selector dimension groups are collapsed: they are only filtered on in the WHERE
            clause and must NOT be part of the GROUP BY, otherwise every selector code would end up in its own group.
            Each measure is aggregated separately when aggregating over a dimension group. Without selectors, the
            measure columns are combined per row instead (e.g. SUM(MeasureA + MeasureB)).

            === Example non-pivot SUM over a dimension group as OData3 SQL ===
            SELECT FarmAnimals, SUM("Livestock_1") AS "SUM"
            FROM '<parquet-file>'
            WHERE FarmAnimals = 'A044005'
            AND Periods IN ('2022MM03', '2022MM04')
            GROUP BY FarmAnimals;
        """
        measures, group_by, from_stmt, where_stmt = self._non_pivot_value_parts()
        if self.selectors:
            aggregations = [f'{self._operator}("{msr}") AS "{self._operator}{f"[{msr}]" if len(measures) > 1 else ""}"'
                            for msr in measures]
        else:
            aggregations = [f'{self._operator}({self._non_pivot_measure_combination(measures)}) AS "{self._operator}"']

        sql = f"""
            SELECT {', '.join(group_by + aggregations)}
            {from_stmt}
            {where_stmt}
            {f"GROUP BY {', '.join(group_by)}" if group_by else ''}
        """
        return sqlglot.parse_one(sql).sql(pretty=True)

    def _non_pivot_ranked_sql(self, order: str) -> str:
        """
            Non-pivoted OData3 SQL for the combined MIN/ARGMIN and MAX/ARGMAX queries over a single VALUE
            expression. Like the pivoted variant, the values are ranked separately for every combination of the dimension
            codes that are NOT aggregated over (and per measure when aggregating over a dimension group). A single
            measure is ranked directly on its column. Multiple measures, or aggregating over the measures themselves,
            require an UNPIVOT, for which the measure filters are applied before unpivoting.

            :param order: ranking order, i.e. 'ASC' for MIN and 'DESC' for MAX
        """
        measures, partition, from_stmt, where_stmt = self._non_pivot_value_parts()
        selector = self.selectors[0] if self.selectors else 'Measure'
        select_cols = [c for c in partition if c != selector] + [f'{selector} AS "{self._operator}[{selector}]"']

        if self.selectors and len(measures) == 1:
            ranked_value = f'"{measures[0]}"'
            select_cols.append(ranked_value)
            source = f"{from_stmt}\n{where_stmt}"
        else:
            if self.selectors:
                partition = ['Measure'] + partition
                select_cols.insert(0, 'Measure')
            ranked_value = 'Value'
            select_cols.append(f'Value AS "{self._operator}"')
            source = f"""
                FROM (SELECT * {from_stmt} {where_stmt})
                UNPIVOT(Value FOR Measure IN ({', '.join(f"'{msr}'" for msr in measures)}))
            """

        ranked = f"""
            SELECT
                RANK() OVER ({f"PARTITION BY {', '.join(partition)} " if partition else ''}ORDER BY {ranked_value} {order}) AS rnk,
                {', '.join(select_cols)}
            {source}
        """
        return self._ranked_cte_sql(ranked, rank_filter='rnk = 1')

    def _execute_sql(self, pivot: bool = False) -> Tuple[pd.DataFrame, CodeLabelMapper, CodeLabelMapper]:
        """
            Execute and return the result from the SQL query corresponding with this S-expression.

            :param pivot: bool to indicate to use the pivoted OData3 query
            :return: resulting DataFrame, code-to-label mapping dictionary and code-to-definition mapping dictionary
        """
        self._validate_unit_compatibility()
        _, tables, measures, dimensions = self._get_sub_exp_filters()

        if not pivot:
            query = self.odata3_sql
        else:
            query = self.odata3_pivot_sql

        db = DBExecutor(tables=list(tables), measures=measures, dims=dimensions, operator_name=self._operator)
        answer, code_labels, code_definitions, _ = db.query_db(query=query, pivot=pivot)
        return answer, code_labels, code_definitions

    @property
    def odata3_pivot_sql(self):
        """
            === Example SUM s-expression as OData3 SQL ===
            SELECT *
            FROM (
                SELECT Measure, Value, Perioden, BestemmingEnSeizoen, Marges, Vakantiekenmerken
                FROM '<parquet_file>'
                UNPIVOT (
                    Value FOR Measure IN (D004645)
                )
            PIVOT (
                SUM(Value)
                FOR BestemmingEnSeizoen IN ('L008691', 'L999996')
                    Marges IN ('MW00000')
                    Vakantiekenmerken IN ('T001460')
                    Perioden IN ('2021JJ00', '2022JJ00'))
                GROUP BY Measure
            );

            MIN/MAX additionally return the code holding the extreme value (the argmin/argmax), e.g. for
            (MIN (Regions) ...): PIVOT (MIN(Value) AS "MIN", ARG_MIN(Regions, Value) AS "MIN[Regions]" FOR ...).
        """
        # Get inner select from sub expression to fetch data to aggregate
        sub_sql = sqlglot.parse_one(self.sub_expression.odata3_pivot_sql)

        if not isinstance(self.sub_expression, Value):  # Aggregate over complex inner query (e.g. AGGJOIN)
            if len(self.selectors) == 0:
                sql = f"""
                    SELECT Measure, {self._operator}(Value) AS {self._operator}
                    FROM ({sub_sql.sql()})
                    UNPIVOT (Value FOR name IN (COLUMNS(c -> NOT contains(c, 'Measure'))))
                    GROUP BY Measure
                """
                return sqlglot.parse_one(sql).sql(pretty=True)
            else:
                raise NotImplementedError("Can't build SQL aggregations over complex inner queries "
                                          "with a dimension as the aggregation selector yet.")

        select = sub_sql.find(sqlglot.exp.Subquery).find(sqlglot.exp.Select)

        # Copy existing PIVOT over from sub-expression
        for_statements = {}
        for node in sub_sql.find_all(sqlglot.exp.In):
            if 'Measure' not in node.sql():
                for_statements |= dict([node.sql().split(' IN ')])

        # MIN/MAX also return which code (or which measure, when aggregating over the measures) holds the extreme
        # value, like the non-pivoted ranked SQL. Note: ARG_MIN/ARG_MAX return one code on ties, RANK() returns all
        aggregations = f"{self._operator}(Value)"
        arg_column = None
        if self._operator in ('MIN', 'MAX'):
            arg_column = self.selectors[0] if self.selectors else 'Measure'
            aggregations = (f'{self._operator}(Value) AS "{self._operator}", '
                            f'ARG_{self._operator}({arg_column}, Value) AS "{self._operator}[{arg_column}]"')

        # Remove measure from the inner select if aggregating on measure (unless it is the ARG_MIN/ARG_MAX argument;
        # a column used inside a PIVOT aggregate is not grouped by)
        if len(self.selectors) == 0 and arg_column != 'Measure':
            select.select(copy=False).set("expressions",
                                          [e for e in select.select().expressions if 'Measure' not in e.sql()])

        # Remove the selector from the existing FOR's in the PIVOT to add as WHERE. The selector is only filtered on
        # there, so the VALUE's own predicate applies: e.g. the compact `WijkenEnBuurten LIKE 'BU%'` of a LEVEL
        # selection instead of the enumerated codes that DuckDB only needs inside the PIVOT clause
        dimensions = {str(group): (group, codes) for group, codes in self.sub_expression.dimensions}
        for selector in self.selectors:
            if selector in for_statements:
                select.where(self.sub_expression._dim_predicate(*dimensions[selector]), append=True, copy=False)
            # If there is only ONE other FOR statement, PIVOT over the Measure
            if len(for_statements) <= 1:
                for_statements |= dict([node.sql().split(' IN ') for node in sub_sql.find_all(sqlglot.exp.In) if 'Measure' in node.sql()])
            for_statements.pop(selector, None)

        # Add a dummy column with the aggregated measures if applicable
        select_cols = '*'
        if not self.selectors:
            select_cols += ", '"
            # Sort the aggregated measure codes so the joined label is deterministic across runs
            # (schema items hash by URI, so set iteration order is otherwise unstable)
            select_cols += ';'.join(sorted(s.name for s in [e for e in select.find_all(sqlglot.exp.In)
                                                            if 'Measure' in e.sql()][0].expressions))
            select_cols += "' AS Measure"

        sql = f"""
            SELECT {select_cols}
            FROM ({select.sql()})
            PIVOT (
                {aggregations}
                FOR {'\n'.join('{} IN {}'.format(k, v) for k, v in for_statements.items())}
                {'GROUP BY Measure' if self.selectors else ''}
            )
        """
        return sqlglot.parse_one(sql).sql(pretty=True)
