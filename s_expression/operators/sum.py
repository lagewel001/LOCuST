from __future__ import annotations

import pandas as pd
from typing import Tuple

from odata_graph import engine
from s_expression.operators import Value
from s_expression.simple_aggregator import SimpleAggregator


class Sum(SimpleAggregator):
    """
        The SUM expression sums the resulting pivot tables from a sub-expression over a given column.
    """
    def __call__(self,
                 sql: bool = False,
                 odata4: bool = False,
                 offline: bool = False,
                 verbose: bool = False) -> Tuple[Sum, pd.DataFrame]:
        if sql:
             answer, code_labels = self._execute_sql(odata4=odata4)
             self.mapper = code_labels
        else:
            measure_units = {}
            if isinstance(self.sub_expression, Value) and len(self.selectors) == 0:
                measure_units = engine.validate_msr_unit_compatibility(self.sub_expression.measures)
            sub_exp, table = self.sub_expression(offline=offline, odata4=odata4, verbose=verbose)

            self.mapper = sub_exp.mapper
            self.intermediate_results = [table] + sub_exp.intermediate_results

            measure_units = {self.mapper.get(k, k): v for k, v in measure_units.items()}
            answer = self._aggregate(table, self.selectors, pd.DataFrame.sum, measure_units)

        if verbose:
            self._print_answer(answer)

        return self, answer


if __name__ == '__main__':
    from s_expression.parser import parse, eval

    # Measure sum
    eval(parse("""(SUM
        ()
        (VALUE 83180NED
            (MSR (D004585_1 D004560_1 D004550_1))
            (DIM Perioden (2015JJ00 2016JJ00 2017JJ00 2018JJ00 2019JJ00 2020JJ00 2021JJ00 2022JJ00))
        )
    )
    """), offline=False, verbose=True)

    # Dim sum --- not to be confused with much more fun Chinese dim sum
    eval(parse("""(SUM
         (Perioden)
         (VALUE 85302NED 
             (MSR (D004645 D006211_2))
             (DIM Perioden (2021JJ00 2022JJ00))
             (DIM BestemmingEnSeizoen (L008691 L999996))
             (DIM Vakantiekenmerken (T001460))
             (DIM Marges (MW00000))
         )
     """), sql=False, verbose=True)

    # Sum with join
    eval(parse("""
        (SUM ()
            (JOIN (M004032) (VALUE 85601NED (MSR (M004032)) (DIM ContainerGrootte (A052183 A052184)) (DIM Vervoerstroom (A045748)) (DIM Perioden (2023JJ00))) (VALUE 85598NED (MSR (M004032)) (DIM NederlandseZeehavens (T001293)) (DIM SoortLading (A041789)) (DIM Vervoerstroom (A045748)) (DIM Perioden (2023JJ00))))
        )
    """), sql=True, verbose=True)
