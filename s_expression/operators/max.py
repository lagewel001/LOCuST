from __future__ import annotations

import numpy as np
import pandas as pd
from typing import Tuple

from odata_graph import engine
from s_expression.operators import Value
from s_expression.simple_aggregator import SimpleAggregator


class Max(SimpleAggregator):
    """
        Get the max values over given axes for the intermediate results of a given expression.
        When running as native S-expression, MAX works like a combined MAX and ARGMAX function.
        This means that always the context is returned from the initial tables. When running as
        SQL query, MAX works like a traditional MAX function over a PIVOT command.
    """
    def __call__(self,
                 sql: bool = False,
                 odata4: bool = False,
                 pivot: bool = False,
                 offline: bool = False,
                 verbose: bool = False) -> Tuple[Max, pd.DataFrame]:
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

            max_values = self._aggregate(table, self.selectors, pd.DataFrame.max, measure_units)

            # TODO: single highest value assumption
            #  This method relies on that only ONE cell contains 1. If multiple columns have 1s
            #  (i.e. multiple equally highest values present in the table), it returns the first such column
            #  (by order in df.columns).
            if len(self.selectors) > 0:
                cols = [c if isinstance(table.columns, pd.MultiIndex) else (c,) for c in table.columns]
                selector_max = pd.DataFrame(  # Get the selector, being part of the corresponding column name, of each max value
                    np.array([
                        (pd.DataFrame(np.isclose(table, val), index=table.index, columns=cols) == 1).any().idxmax()
                        [table.columns.names.index(self.mapper.get(self.selectors[0], self.selectors[0]))]
                        for val in max_values.values.flatten()
                    ]).reshape(max_values.shape),
                    index=max_values.index,
                    columns=max_values.columns
                )

                # Combine the max dims from the selector with the corresponding values
                self.intermediate_results = [table] + sub_exp.intermediate_results
                answer = selector_max.compare(max_values).groupby(level=0, axis=1).apply(lambda dd: dd.agg(tuple, axis=1))
            else:
                answer = self._aggregate(table, self.selectors, pd.DataFrame.idxmax, measure_units)
                combiner = np.vectorize(lambda tup, nums: tup + (nums,), otypes=[object])
                answer[:] = combiner(answer.values, max_values.values)

        if verbose:
            self._print_answer(answer)

        return self, answer

    @property
    def odata3_sql(self) -> str:
        """
            === Example MAX expression over a dimension group as OData3 SQL ===
            WITH RankedRows AS (
                SELECT RANK() OVER (PARTITION BY Region ORDER BY "BuildingPermitsAdditions_11" DESC) AS rnk,
                       Region, Period AS "MAX[Period]", "BuildingPermitsAdditions_11"
                FROM '<parquet-file>'
                WHERE Period IN ('2024KW04', '2025KW01')
                AND Region IN ('NL01', 'PV20')
            )
            SELECT *
            FROM RankedRows
            WHERE rnk = 1;

            === Example MAX expression over measures as OData3 SQL ===
            WITH RankedRows AS (
                SELECT RANK() OVER (PARTITION BY Period, Region ORDER BY Value DESC) AS rnk,
                       Period, Region, Measure AS "MAX[Measure]", Value AS "MAX"
                FROM (SELECT * FROM '<parquet-file>' WHERE Period = '2025KW01' AND Region = 'NL01')
                UNPIVOT(Value FOR Measure IN ('BuildingPermitsAdditions_11', 'StockBalance_21'))
            )
            SELECT *
            FROM RankedRows
            WHERE rnk = 1;
        """
        if not isinstance(self.sub_expression, Value):
            return self.odata3_pivot_sql  # No non-pivot version for aggregations over complex inner queries

        return self._non_pivot_ranked_sql(order='DESC')


if __name__ == '__main__':
    from s_expression.parser import parse, eval

    eval(parse("""
        (MAX () (VALUE 85672NED (MSR (M000134)) (DIM BurgerlijkeStaat (1080)) (DIM CaribischNederland (GM9003 GM9001 GM9002)) (DIM Geslacht (T001038)) (DIM Leeftijd (10000)) (DIM Perioden (2021JJ00))))
    """), sql=True, verbose=True)

    eval(parse("""
        (MAX (BeroepsEnEigenVervoer) (VALUE 82836NED (MSR (M004375 D003778)) (DIM BeroepsEnEigenVervoer (100200 100100)) (DIM Vervoerstromen (A028222)) (DIM Perioden (2020JJ00))))
    """), verbose=True)

    # Max over dimension group and TWO measures
    eval(parse("""
        (MAX (Perioden) (VALUE 82836NED (MSR (M004375 D003778)) (DIM BeroepsEnEigenVervoer (100200 100100)) (DIM Vervoerstromen (A028222)) (DIM Perioden (2020JJ00 2021JJ00))))
    """), verbose=True)

    eval(parse("""(MAX
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
