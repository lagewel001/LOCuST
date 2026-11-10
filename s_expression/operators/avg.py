from __future__ import annotations

import pandas as pd
import sqlglot
from typing import List, Tuple

from odata_graph import engine
from s_expression.operators import Value
from s_expression.simple_aggregator import SimpleAggregator


class Avg(SimpleAggregator):
    """
        The AVG expression averages the resulting (pivot) tables from a sub-expression over a given column.
    """
    def __call__(self,
                 sql: bool = False,
                 odata4: bool = False,
                 pivot: bool = False,
                 offline: bool = False,
                 verbose: bool = False) -> Tuple[Avg, pd.DataFrame]:
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
            self.intermediate_results = [table] + sub_exp.intermediate_results

            measure_units = {self.mapper.get(k, k): v for k, v in measure_units.items()}
            answer = self._aggregate(table, self.selectors, pd.DataFrame.mean, measure_units)

        if verbose:
            self._print_answer(answer)

        return self, answer

    @property
    def odata3_sql(self) -> str:
        """
            === Example AVG expression as OData3 SQL ===
            SELECT Periods, Region,
                   AVG("BuildingCostsAlteration_2") AS "AVG"
            FROM '<parquet-file>'
            WHERE Periods = '2019JJ00'
            AND Region = 'NL01'
            AND BuildingPhases IN ('A041324', 'A041322')
            GROUP BY Periods, Region;
        """
        if not isinstance(self.sub_expression, Value):  # Aggregate over complex inner query (e.g. AGGJOIN)
            if len(self.selectors) == 0:
                _, _, measures, _ = self._get_sub_exp_filters()
                sql = f"""
                    SELECT
                        '{str(list(measures)[0])}' AS Measure,
                        AVG(\"{str(list(measures)[0])}\") AS AVG
                    FROM ({self.sub_expression.odata3_sql})
                    GROUP BY Measure
                """
                return sqlglot.parse_one(sql).sql(pretty=True)
            else:
                raise NotImplementedError("Can't build SQL aggregations over complex inner queries "
                                          "with a dimension as the aggregation selector yet.")

        return self._non_pivot_aggregation_sql()

    def _non_pivot_measure_combination(self, measures: List[str]) -> str:
        """Averaging over measures takes the mean of the measure columns per row, i.e. (MeasureA + MeasureB) / 2."""
        if len(measures) == 1:
            return super()._non_pivot_measure_combination(measures)
        return f"({super()._non_pivot_measure_combination(measures)}) / {len(measures)}"


if __name__ == '__main__':
    from s_expression.parser import parse, eval

    # Single dimension table example
    eval(parse("""
        (AVG (Periods) (VALUE 03753eng (MSR (EnrolledPupilsStudents_2)) (DIM TypeOfEducation (A025279)) (DIM IdeologicalBasisOfTheSchool (A041917)) (DIM Schoolsize (A041906)) (DIM Periods (2004SJ00 2003SJ00))))
    """), sql=True,verbose=True)

    # Multi dimension table example
    eval(parse("""(AVG
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
