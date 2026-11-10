from __future__ import annotations

import numpy as np
import pandas as pd
from typing import Tuple

from odata_graph import engine
from s_expression.operators import Value
from s_expression.simple_aggregator import SimpleAggregator


class Min(SimpleAggregator):
    """
        Get the minimum values over given axes for the intermediate results of a given expression.
        When running as native S-expression, MIN works like a combined MIN and ARGMIN function.
        This means that always the context is returned from the initial tables. When running as
        SQL query, MIN works like a traditional MIN function over a PIVOT command.
    """
    def __call__(self,
                 sql: bool = False,
                 odata4: bool = False,
                 pivot: bool = False,
                 offline: bool = False,
                 verbose: bool = False) -> Tuple[Min, pd.DataFrame]:
        self._validate_partition_coherence()
        if sql:
            answer, code_labels, code_definitions = self._execute_sql(pivot=pivot)
            self.mapper = code_labels
            self.mapper_definitions = code_definitions
        else:
            measure_units = {}
            if isinstance(self.sub_expression, Value) and len(self.selectors) == 0:
                measure_units = engine.validate_msr_unit_compatibility(self.sub_expression.measures)
            sub_exp, table = self.sub_expression(offline=offline, odata4=odata4, verbose=verbose)

            self.mapper = sub_exp.mapper
            self.mapper_definitions = sub_exp.mapper_definitions
            measure_units = {self.mapper.get(k, k): v for k, v in measure_units.items()}

            min_values = self._aggregate(table, self.selectors, pd.DataFrame.min, measure_units)

            # TODO: single highest value assumption
            #  This method relies on that only ONE cell contains 1. If multiple columns have 1s
            #  (i.e. multiple equally highest values present in the table), it returns the first such column
            #  (by order in df.columns).
            if len(self.selectors) > 0:
                cols = [c if isinstance(table.columns, pd.MultiIndex) else (c,) for c in table.columns]
                selector_max = pd.DataFrame(  # Get the selector, being part of the corresponding column name, of each min value
                    np.array([
                        (pd.DataFrame(np.isclose(table, val), index=table.index, columns=cols) == 1).any().idxmax()
                        [table.columns.names.index(self.mapper.get(self.selectors[0], self.selectors[0]))]
                        for val in min_values.values.flatten()
                    ]).reshape(min_values.shape),
                    index=min_values.index,
                    columns=min_values.columns
                )

                # Combine the max dims from the selector with the corresponding values
                self.intermediate_results = [table] + sub_exp.intermediate_results
                answer = selector_max.compare(min_values).groupby(level=0, axis=1).apply(lambda dd: dd.agg(tuple, axis=1))
            else:
                answer = self._aggregate(table, self.selectors, pd.DataFrame.idxmin, measure_units)
                combiner = np.vectorize(lambda tup, nums: tup + (nums,), otypes=[object])
                answer[:] = combiner(answer.values, min_values.values)

        if verbose:
            self._print_answer(answer)

        return self, answer

    @property
    def odata3_sql(self) -> str:
        """
            === Example MIN expression over a dimension group as OData3 SQL ===
            WITH RankedRows AS (
                SELECT RANK() OVER (PARTITION BY Region ORDER BY "BuildingPermitsAdditions_11" ASC) AS rnk,
                       Region, Period AS "MIN[Period]", "BuildingPermitsAdditions_11"
                FROM '<parquet-file>'
                WHERE Period IN ('2024KW04', '2025KW01')
                AND Region IN ('NL01', 'PV20')
            )
            SELECT *
            FROM RankedRows
            WHERE rnk = 1;

            === Example MIN expression over measures as OData3 SQL ===
            WITH RankedRows AS (
                SELECT RANK() OVER (PARTITION BY Period, Region ORDER BY Value ASC) AS rnk,
                       Period, Region, Measure AS "MIN[Measure]", Value AS "MIN"
                FROM (SELECT * FROM '<parquet-file>' WHERE Period = '2025KW01' AND Region = 'NL01')
                UNPIVOT(Value FOR Measure IN ('BuildingPermitsAdditions_11', 'StockBalance_21'))
            )
            SELECT *
            FROM RankedRows
            WHERE rnk = 1;
        """
        if not isinstance(self.sub_expression, Value):
            return self.odata3_pivot_sql  # No non-pivot version for aggregations over complex inner queries

        return self._non_pivot_ranked_sql(order='ASC')


if __name__ == '__main__':
    from s_expression.parser import parse, eval

    # Both values equal zero should yield both values
    eval(parse("""
        (MIN (DoodsoorzakenUitgebreideLijst) (VALUE 7233 (MSR (M000134 = 1)) (DIM Perioden (2022JJ00)) (DIM Geslacht ()) (DIM Leeftijd ()) (DIM DoodsoorzakenUitgebreideLijst ())))
    """), verbose=True)

    # Single dimension table example
    eval(parse("""
        (MIN
        (Perioden)
        (VALUE 37789ksz
            (MSR (D000292))
            (DIM Perioden (2007JJ00 2016JJ00 2008JJ00 2013JJ00))
        )
    )
    """), verbose=True)

    # Multi dimension table example
    eval(parse("""(MIN
        (Perioden)
        (VALUE 85302NED
            (MSR (D004645))
            (DIM Perioden (2021JJ00 2022JJ00))
            (DIM BestemmingEnSeizoen (L008691 L999996))
            (DIM Vakantiekenmerken (T001460))
            (DIM Marges (MW00000))
        )
    )
    """), verbose=True)
