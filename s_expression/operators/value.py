from __future__ import annotations

import os
import pandas as pd
import sqlglot
from beartype import beartype
from beartype.door import is_bearable
from typing import Dict, List, Optional, Set, FrozenSet, Tuple, Union

import config
from odata_graph import engine
from odata_graph.sparql_controller import LevelSelection
from pipeline.odata_executor import ODataExecutor
from pipeline.db_executor import DBExecutor
from s_expression import Expression, ParsedExpression
from s_expression import Table, Measure, Dimension
from utils.custom_types import NonEmptyList, ComparisonOperator, Connective


class Value(Expression):
    """
        Basic value expression used for retrieving and filtering information from OData
        Syntax: (VALUE <table> (MSR (<code> ... <code>)) (DIM <dim> (<code> ... <code>)) (DIM ...))

        Measures can also be filtered on their values:
            (MSR (<code> <op> <num>))                                  single comparison
            (MSR (<num> <lop> <code> <lop> <num>))                     between-range, lop in {<, <=}
            (MSR (<code> <op> <num>) <AND|OR> (<code> <op> <num>))     combined conditions

        :param sexp: the parsed S-expression, of which the evaluation instantiates this object
        :param table: the table identifier to query
        :param measures: the measures identifier to filter the table on
        :param dimensions: the dimensions and their codes to filter the table on
    """
    table: Table
    measures: Set[Measure]
    measure_filters: List[Tuple[Measure, ComparisonOperator, float|int]]
    filter_connective: Connective
    dimensions: Set[Tuple[Dimension, FrozenSet[Dimension]]]
    level_filters: Dict[str, LevelSelection]

    @beartype
    def __init__(self,
                 sexp: ParsedExpression,
                 table: str,
                 measures: NonEmptyList[Union[str, List[str]]],
                 *dimensions: Tuple[str, List[str]]):
        super().__init__(sexp)
        self.table = Table(table)

        self.measures = set()
        self.measure_filters = []
        self.filter_connective = 'AND'
        if any(is_bearable(m, List[str]) for m in measures):
            self._parse_filter_clauses(measures)
        else:
            self._parse_measure_tokens(measures)

        # Reject unknown measure codes and MeasureGroups
        engine.validate_table_measures(self.table, self.measures)

        self.selectors = {}  # only relevant when VALUE is called from within a different operator
        self.intermediate_results = []

        # Ideally we would specify the type signature for dimensions as List[Tuple[str, Tuple[str, ...]]],
        #  following PEP646, but this isn't (yet) supported by beartype .
        #  (https://github.com/beartype/beartype/issues/428#issuecomment-2357650766)
        if not (all(map(lambda x: len(x) == 2, dimensions))  # every dimension must contain two items
                and all(map(lambda x: isinstance(x[0], str), dimensions))  # every first item must be a single string
                and all(map(lambda x: is_bearable(x[1], List[str]), dimensions))  # every second item must be a list of strings
        ):
            raise TypeError(f"Invalid dimension specification {dimensions}.\n"
                            f"Please make sure specify dimensions as `(DIM <dim> (<code> ... <code>))`.")

        # Intensional `(DIM <dim> (LEVEL <level> [<scope>]))` selections resolve to the level's concrete code-set via
        # the metadata graph, so downstream execution (SQL/OData, coherence checks, pivoting) works on plain codes.
        # The LevelSelection is kept so the SQL builders can emit its compact pattern instead of the enumerated IN-list.
        self.level_filters = {}
        self.dimensions = set()
        for d, codes in dimensions:
            if codes and codes[0] == 'LEVEL':
                if len(codes) not in (2, 3):
                    raise TypeError(f"Invalid level selection {codes} for dimension '{d}'.\n"
                                    f"Please specify level selections as "
                                    f"`(DIM <dim> (LEVEL <level>))` or `(DIM <dim> (LEVEL <level> <scope>))`.")
                selection = engine.resolve_level_codes(self.table, Dimension(d), codes[1],
                                                       scope=codes[2] if len(codes) == 3 else None)
                self.level_filters[d] = selection
                codes = selection.codes
            self.dimensions.add((Dimension(d), frozenset({Dimension(c) for c in codes})))

    def _parse_measure_tokens(self, measures: List[str]):
        """Parse a flat measure list: plain codes, a single comparison or a between-range."""
        comparison_idxs = [i for i, t in enumerate(measures) if is_bearable(t, ComparisonOperator)]
        if len(comparison_idxs) == 2:
            # Between-range: (MSR (<num> <lop> <code> <lop> <num>))
            if len(measures) != 5 or comparison_idxs != [1, 3]:
                raise TypeError(f"Invalid measure specification {measures}.\n"
                                f"Between filters must be specified as `(MSR (<num> <lop> <code> <lop> <num>))`.")
            low, low_op, code, high_op, high = measures
            if low_op not in ('<', '<=') or high_op not in ('<', '<='):
                raise ValueError(f"Invalid between filter `{' '.join(measures)}`: only < and <= are allowed.")
            try:
                low, high = float(low), float(high)
            except ValueError:
                raise ValueError(f"Invalid between filter `{' '.join(measures)}`: bounds must be numeric.")
            if low >= high:
                raise ValueError(f"Invalid between filter `{' '.join(measures)}`: "
                                 f"the lower bound must be smaller than the upper bound.")

            self.measures.add(Measure(code))
            self.measure_filters = [(Measure(code), '>' if low_op == '<' else '>=', low),
                                    (Measure(code), high_op, high)]
            return

        i = 0
        while i < len(measures):
            if i + 2 < len(measures) and is_bearable(measures[i + 1], ComparisonOperator):
                try:
                    code, op, value = measures[i], measures[i + 1], float(measures[i + 2])
                except ValueError:
                    raise ValueError(f"Invalid measure specification: comparison value '{measures[i + 2]}' must "
                                     f"be numeric in `{measures[i]} {measures[i + 1]} {measures[i + 2]}`")
                self.measures.add(Measure(code))
                self.measure_filters = [(Measure(code), op, value)]

                if len(measures) > 3:
                    raise TypeError(f"Invalid measure specification {measures}.\n"
                                    f"You can only select one measure when filtering by comparison operator.")
                break
            else:
                code = measures[i]
                self.measures.add(Measure(code))
                i += 1

    def _parse_filter_clauses(self, clauses: List[Union[str, List[str]]]):
        """Parse combined measure conditions: (MSR (<code> <op> <num>) <AND|OR> (<code> <op> <num>) ...)."""
        if len(clauses) % 2 == 0:
            raise TypeError(f"Invalid measure specification {clauses}.\n"
                            f"Combined conditions must alternate clauses and connectives, e.g. "
                            f"`(MSR (<code> <op> <num>) <AND|OR> (<code> <op> <num>))`.")

        connectives = set()
        for i, clause in enumerate(clauses):
            if i % 2 == 1:  # connective between two clauses
                if clause not in ('AND', 'OR'):
                    raise TypeError(f"Invalid connective '{clause}' in measure specification {clauses}.\n"
                                    f"Conditions can only be combined with AND or OR.")
                connectives.add(clause)
                continue

            if not (is_bearable(clause, List[str]) and len(clause) == 3
                    and is_bearable(clause[1], ComparisonOperator)):
                raise TypeError(f"Invalid measure filter clause {clause}.\n"
                                f"Each combined condition must be specified as `(<code> <op> <num>)`.")
            code, op, value = clause
            try:
                value = float(value)
            except ValueError:
                raise ValueError(f"Invalid measure specification: comparison value '{clause[2]}' must "
                                 f"be numeric in `{' '.join(clause)}`")
            self.measures.add(Measure(code))
            self.measure_filters.append((Measure(code), op, value))

        if len(connectives) > 1:
            raise ValueError(f"Invalid measure specification {clauses}.\n"
                             f"Mixing AND and OR connectives is not supported.")
        self.filter_connective = connectives.pop() if connectives else 'AND'

    def __call__(self,
                 index_cols: FrozenSet[str] = frozenset({'Measure', 'Unit'}),
                 friendly_labels: bool = True,
                 sql: bool = False,
                 odata4: bool = False,
                 pivot: bool = False,
                 offline: bool = False,
                 verbose: bool = False) -> Tuple[Value, pd.DataFrame]:
        """
            :param index_cols: column to index over when creating the answer pivot table
            :param friendly_labels: flag whether to translate measure/dimension codes in the answer to their labels
        """
        if sql or offline:
            self.selectors = index_cols or frozenset({'Measure', 'Unit'})
            db = DBExecutor(tables=[self.table], measures=self.measures, dims=self.dimensions,
                            operator_name=self._operator)

            # Offline native evaluation emulates the OData API, which returns pivot tables that the
            # native operators aggregate over, so it always uses the pivot SQL
            pivot = pivot or not sql
            if not pivot:
                query = self.odata3_sql
            else:
                query = self.odata3_pivot_sql

            answer, code_labels, code_definitions, _ = db.query_db(query=query, index_cols=index_cols,
                                                                   pivot=pivot, friendly_labels=friendly_labels)
        else:
            odata = ODataExecutor(self.table, self.measures, self.measure_filters, self.dimensions,
                                  filter_connective=self.filter_connective, level_filters=self.level_filters)
            answer, code_labels, code_definitions = odata.query_odata(index_cols=index_cols, friendly_labels=friendly_labels)

        self.mapper = code_labels
        self.mapper_definitions = code_definitions
        if verbose:
            self._print_answer(answer)

        return self, answer

    def _dim_predicate(self, group, codes) -> str:
        """
            WHERE-side filter for one dimension: the compact level pattern (`RegioS LIKE 'WK%'`)
            when the codes come from a `(LEVEL ...)` selection whose pattern was proven exact
            against the table's code list, else the enumerated IN-list. Note that PIVOT clauses
            always need the enumerated codes: DuckDB requires explicit pivot values.
        """
        selection = self.level_filters.get(str(group))
        if selection and selection.sql_pattern:
            kind, pattern = selection.sql_pattern
            if kind == 'like':
                return f"{group} LIKE '{pattern}'"
            return f"regexp_matches({group}, '{pattern}')"
        return f"{group} IN ('{"', '".join(sorted(str(c) for c in codes))}')"

    def _unpivot_filter_parts(self) -> Tuple[str, Optional[str]]:
        """
            FROM-source and WHERE-predicate implementing the comparison filters for the regular SQLs
            (the pivot SQL of this VALUE and the PROP SQL over it). A single condition filters the
            unpivoted Value column; combined/between conditions must filter in the wide column space
            before unpivoting, so AND/OR over different measures applies per table row.

            :returns: tuple of the FROM-source and the predicate on the unpivoted Value column (None if not needed)
        """
        table_source = f"'{os.path.relpath(config.DB_ODATA3_FILES)}/{self.table}.parquet'"
        if len(self.measure_filters) == 1:
            _, op, val = self.measure_filters[0]
            return table_source, f"Value {op} {val}"
        if len(self.measure_filters) > 1:
            predicate = f' {self.filter_connective} '.join(f'"{msr}" {op} {val}'
                                                           for msr, op, val in self.measure_filters)
            return f"(SELECT * FROM {table_source} WHERE ({predicate}))", None
        return table_source, None

    @property
    def odata3_pivot_sql(self):
        """
            === Example VALUE s-expression as OData3 SQL ===
            SELECT *
            FROM (
                SELECT Measure, Value, ArableCrops, Regions, Periods
                FROM '<parquet_file>'
                UNPIVOT (
                    Value FOR Measure IN (HarvestedArea_2, GrossYieldPerHa_3)
                )
            )
            PIVOT(MAX(Value) FOR
                ArableCrops IN ('A042362', 'A042180')
                Periods IN ('2013JJ00')
                Regions IN ('PV29')
            );
        """
        measure_selector = any(s in map(str, self.measures) for s in self.selectors)
        if measure_selector:
            where_cols = [('Measure', self.selectors)]
        else:
            where_cols = [(group, codes) for group, codes in self.dimensions if str(group) in self.selectors]

        where = 'WHERE '
        if where_cols:
            where += ' AND '.join(self._dim_predicate(group, codes) for group, codes in where_cols)

        table_source, value_filter = self._unpivot_filter_parts()
        if value_filter:
            if where != 'WHERE ':
                where += ' AND '
            where += value_filter

        sql = f"""
            SELECT *
            FROM (
                SELECT Measure, Value, {', '.join(str(g) for g, _ in self.dimensions)}
                FROM {table_source}
                UNPIVOT (
                    Value FOR Measure IN ('{"', '".join(str(msr) for msr in self.measures)}')
                )
                {where if where != "WHERE " else ""}
            )
            PIVOT (
                MAX(Value)
                FOR {'\n'.join('{} IN {}'.format(group, f"('{"', '".join({str(c) for c in codes})}')")
                               for group, codes in self.dimensions | ({('Measure', frozenset(self.measures))} if not measure_selector else set())
                               if str(group) not in self.selectors and len(codes) > 0)}
            )
        """
        return sqlglot.parse_one(sql).sql(pretty=True)

    @property
    def odata3_sql(self):
        """
            === Example VALUE expression as OData3 SQL ===
            SELECT Period, TypeOfBankruptcy, BankruptciesSessionDayCorrected_1
            FROM '<parquet_file>'
            WHERE TypeOfBankruptcy = 'A047597'
            AND Periods = '1993MM09';
        """
        select_cols = ""
        if self.dimensions:
            select_cols += ", ".join(map(lambda d: str(d[0]), self.dimensions)) + ', '
        select_cols += ', '.join(f'"{msr}"' for msr in sorted(map(str, self.measures)))

        sql = f"""
            SELECT {select_cols}
            FROM '{os.path.relpath(config.DB_ODATA3_FILES)}/{self.table}.parquet'
        """
        where_statements = []
        for group, codes in self.dimensions:
            if len(codes) > 1:
                where_statements.append(self._dim_predicate(group, codes))
            if len(codes) == 1:
                where_statements.append(f"{group} = '{list(codes)[0]}'")

        where = ""
        if len(where_statements) > 0:
            where += 'WHERE '
            where += '\nAND '.join(where_statements)

        # Comparison filters; the non-pivot format is wide, so conditions filter measure columns directly
        if len(self.measure_filters) == 1:
            where += ' AND ' if where else 'WHERE '
            msr, op, val = self.measure_filters[0]
            where += f"\"{msr}\" {op} {val}"
        elif len(self.measure_filters) > 1:
            predicate = f' {self.filter_connective} '.join(f'"{msr}" {op} {val}'
                                                           for msr, op, val in self.measure_filters)
            where += ' AND ' if where else 'WHERE '
            where += f"({predicate})"

        sql += where
        return sqlglot.parse_one(sql).sql(pretty=True)


if __name__ == '__main__':
    from s_expression.parser import parse, eval

    s_expression, answer = eval(parse("""(VALUE 85302NED
        (MSR (D004645 < 35000))
        (DIM BestemmingEnSeizoen (T001047))
        (DIM Vakantiekenmerken (T001460))
        (DIM Marges (MW00000))
    )
    """), verbose=True)

    eval(parse("""(VALUE 85302NED
        (MSR (D004645 < 35000))
        (DIM Perioden (2021JJ00 2022JJ00 2023JJ00 2024JJ00))
        (DIM BestemmingEnSeizoen (T001047))
        (DIM Vakantiekenmerken (T001460))
        (DIM Marges (MW00000))
    )
    """), verbose=True)
