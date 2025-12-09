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
                 offline: bool = False,
                 verbose: bool = False) -> Tuple[Max, pd.DataFrame]:
        if sql:
            answer, code_labels = self._execute_sql(odata4=odata4)
            self.mapper = code_labels
        else:
            measure_units = {}
            if isinstance(self.sub_expression, Value) and len(self.selectors) == 0:
                measure_units = engine.validate_msr_unit_compatibility(self.sub_expression.measures)
            sub_exp, table = self.sub_expression(offline=offline, odata4=odata4, verbose=verbose)

            self.mapper = sub_exp.mapper
            measure_units = {self.mapper.get(k, k): v for k, v in measure_units.items()}
            answer = self._aggregate(table, self.selectors, pd.DataFrame.max, measure_units)

            # TODO: single 1 Assumption
            #  This method relies on that only ONE cell contains 1. If multiple columns have 1s
            #  (i.e. multiple equally highest values present in the table), it returns the first such column
            #  (by order in df.columns).
            cols = [c if isinstance(table.columns, pd.MultiIndex) else (c,) for c in table.columns]
            selector_max = pd.DataFrame(  # Get the selector, being part of the corresponding column name, of each max value
                np.array([
                    (pd.DataFrame(np.isclose(table, val), index=table.index, columns=cols) == 1).any().idxmax()
                    [table.columns.names.index(self.mapper.get(self.selectors[0], self.selectors[0]))]
                    for val in answer.values.flatten()
                ]).reshape(answer.shape),
                index=answer.index,
                columns=answer.columns
            )

            # Combine the max dims from the selector with the corresponding values
            self.intermediate_results = [table] + sub_exp.intermediate_results
            answer = selector_max.compare(answer).groupby(level=0, axis=1).apply(lambda dd: dd.agg(tuple, axis=1))

        if verbose:
            self._print_answer(answer)

        return self, answer


if __name__ == '__main__':
    from s_expression.parser import parse, eval

    eval(parse("""
        (MAX (CaribischNederland) (VALUE 85672NED (MSR (M000134)) (DIM BurgerlijkeStaat (1080)) (DIM CaribischNederland (GM9003 GM9001 GM9002)) (DIM Geslacht (T001038)) (DIM Leeftijd (10000)) (DIM Perioden (2021JJ00))))
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
