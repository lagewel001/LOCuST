from __future__ import annotations

import pandas as pd
import sqlglot
from beartype import beartype
from typing import List, Tuple

from pipeline.db_executor import DBExecutor
from pipeline.odata_executor import CodeLabelMapper
from s_expression import Expression, ParsedExpression
from s_expression.operators import Value


class SimpleAggregator(Expression):
    """
        Shared logic for simple aggregator functions SUM, AVG, MIN, MAX

        :param sexp: the parsed S-expression, of which the evaluation instantiates this object
        :param selectors: axes to aggregate the intermediate pivot table results over
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

    def __call__(self, sql: bool = False, odata4: bool = False, offline: bool = False, verbose: bool = False) -> \
            Tuple[SimpleAggregator, pd.DataFrame]:
        raise NotImplementedError()

    def _execute_sql(self, odata4: bool = False) -> Tuple[pd.DataFrame, CodeLabelMapper]:
        """
            Execute and return the result from the SQL query corresponding with this S-expression.

            :param odata4: bool to indicate to use the OData4 parquet files
            :return: resulting DataFrame and code-to-label mapping dictionary
        """
        tables = []
        measures = set()
        dimensions = set()
        sub_exps = [self.sub_expression]
        for sub_exp in sub_exps: # Get inner VALUE expression(s)
            if isinstance(sub_exp, Value):
                tables.append(sub_exp.table)
                measures |= sub_exp.measures
                dimensions |= sub_exp.dimensions
                continue

            if hasattr(sub_exp, 'sub_expressions'):
                sub_exps.extend(sub_exp.sub_expressions)
            else:
                sub_exps.append(sub_exp.sub_expression)

        self._odata4 = odata4
        query = self._sql if not odata4 else self._odata4_sql
        db = DBExecutor(tables=tables, measures=measures, dims=dimensions, operator_name=self._operator)
        answer, code_labels, _ = db.query_db(query=query, index_cols=frozenset({'Measure', 'Unit'}))
        return answer, code_labels

    @property
    def _sql(self):
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
        """
        # Get inner select from sub expression to fetch data to aggregate
        sub_sql = sqlglot.parse_one(self.sub_expression._sql)
        return self._build_sql(sub_sql)

    @property
    def _odata4_sql(self):
        """
            === Example SUM s-expression as OData4 SQL ===
            SELECT *
            FROM (
                SELECT Measure, Value, Perioden, BestemmingEnSeizoen, Marges, Vakantiekenmerken
                FROM '<parquet_file>'
                WHERE Measure IN ('D004645') AND Perioden IN ('2021JJ00', '2022JJ00')
            )
            PIVOT (
                SUM(Value)
                FOR BestemmingEnSeizoen IN ('L008691', 'L999996')
                    Marges IN ('MW00000')
                    Vakantiekenmerken IN ('T001460')
                GROUP BY Measure
            );
        """
        # Get inner select from sub expression to fetch data to aggregate
        sub_sql = sqlglot.parse_one(self.sub_expression._odata4_sql)
        return self._build_sql(sub_sql)

    def _build_sql(self, sub_sql: sqlglot.exp.Expression) -> str:
        """
            Helper function for OData3 / OData4 SQL query building. In the current
            configuration, only the VALUE expressions differ between the two versions.
        """
        if not isinstance(self.sub_expression, Value):  # Aggregate over complex inner query
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

        # Remove measure from the inner select if aggregating on measure
        if len(self.selectors) == 0:
            select.select(copy=False).set("expressions",
                                          [e for e in select.select().expressions if 'Measure' not in e.sql()])

        # Remove the selector from the existing FOR's in the PIVOT to add as WHERE
        for selector in self.selectors:
            if selector in for_statements:
                select.where(f"{selector} IN {for_statements[selector]}", append=True, copy=False)
            # If there is only ONE other FOR statement, PIVOT over the Measure
            if len(for_statements) <= 1:
                for_statements |= dict([node.sql().split(' IN ') for node in sub_sql.find_all(sqlglot.exp.In) if 'Measure' in node.sql()])
            for_statements.pop(selector, None)

        # Add a dummy column with the aggregated measures if applicable
        select_cols = '*'
        if not self.selectors:
            select_cols += ", '"
            select_cols += ';'.join([s.name for s in [e for e in select.find_all(sqlglot.exp.In)
                                                      if 'Measure' in e.sql()][0].expressions])
            select_cols += "' AS Measure"

        sql = f"""
            SELECT {select_cols}
            FROM ({select.sql()})
            PIVOT (
                {self._operator}(Value)
                FOR {'\n'.join('{} IN {}'.format(k, v) for k, v in for_statements.items())}
                {'GROUP BY Measure' if self.selectors else ''}
            )
        """
        return sqlglot.parse_one(sql).sql(pretty=True)
