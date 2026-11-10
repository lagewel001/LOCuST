from __future__ import annotations

import inspect
import pandas as pd
import sqlglot
from beartype import beartype
from copy import deepcopy
from itertools import product
from typing import List, Tuple, Set, FrozenSet

from odata_graph import engine
from pipeline.db_executor import DBExecutor
from pipeline.odata_executor import CodeLabelMapper
from s_expression import Expression, ParsedExpression, Table, Measure, Dimension
from s_expression.operators import Value, Sum, Avg, Min, Max
from utils.custom_types import NonEmptyList


class Join(Expression):
    """
        The JOIN expression does an outer join of two or multiple intermediate tables,
        resulting from a set of sub-expressions, over given axes. The axes will be used as the
        index of the pivot tables from the evaluated sub-expressions.

        :param sexp: the parsed S-expression, of which the evaluation instantiates this object
        :param selector: axes to join the intermediate (pivot) table results on
        :param sub_expressions: VALUE expressions that will be executed and used for joining
    """
    @beartype
    def __init__(self, sexp: ParsedExpression, selector: NonEmptyList[str], *sub_expressions: Value):
        super().__init__(sexp)
        self.sub_expressions = sub_expressions

        # Find out if the selector is a measure or a dim group
        self.measure_selector = any(s in map(str, set().union(*[sexp.measures for sexp in self.sub_expressions])) for s in selector)
        self.select_cols = selector
        self.selectors = selector if not self.measure_selector else {'Measure', 'Unit'}

    def __call__(self,
                 sql: bool = False,
                 odata4: bool = False,
                 pivot: bool = False,
                 offline: bool = False,
                 verbose: bool = False) -> Tuple[Join, pd.DataFrame]:
        if sql:
            tables = []
            measures = set()
            dims = set()
            for s in self.sub_expressions:
                # Create a deepcopy, for the DBExecutor will otherwise change the original sub_exp properties as well
                sub_exp = deepcopy(s)
                value_exp = sub_exp
                while 1:  # Get inner VALUE expression
                    if isinstance(value_exp, Value):
                        break
                    value_exp = sub_exp.sub_expression

                tables.append(sub_exp.table)
                measures |= sub_exp.measures
                dims |= sub_exp.dimensions

            db = DBExecutor(tables=tables, measures=measures, dims=dims, operator_name=self._operator)
            if not pivot:
                query = self.odata3_sql
            else:
                query = self.odata3_pivot_sql

            answer, code_labels, code_definitions, _ = db.query_db(query=query, index_cols=frozenset(self.selectors), pivot=pivot)
            self.mapper = code_labels
            self.mapper_definitions = code_definitions
        else:
            self.intermediate_results = []
            self.mapper = CodeLabelMapper({})
            self.mapper_definitions = CodeLabelMapper({})
            measures = set()
            for sexp in self.sub_expressions:
                sub_exp, table = sexp(index_cols=frozenset(self.selectors), friendly_labels=False,
                                      offline=offline, odata4=odata4, verbose=verbose)
                measures |= sub_exp.measures
                self.intermediate_results.append(table)
                self.mapper |= sub_exp.mapper
                self.mapper_definitions |= sub_exp.mapper_definitions

            answer = pd.concat(self.intermediate_results, axis=1)

            # Translate the (merged) code columns and indices to their friendly names
            answer.index = [tuple(self.mapper.get(i.strip(), i) for i in p_i) if not isinstance(p_i, str)
                            else self.mapper.get(p_i.strip(), p_i) for p_i in answer.index]
            answer.index.names = list({'Measure'} if self.measure_selector else self.selectors)
            answer.columns = [tuple(self.mapper.get(c.strip(), c) for c in p_col) if not isinstance(p_col, str)
                              else self.mapper.get(p_col.strip(), p_col) for p_col in answer.columns]

        # If JOIN was called from within an aggregator, check for measure compatibility
        called_from_aggregator = any(
            # Checking for SimpleAggregator here results in a circular import :(
            isinstance(f_info.frame.f_locals.get('self'), (Sum, Avg, Min, Max)) for f_info in inspect.stack())
        if called_from_aggregator and self.measure_selector:
            # TODO: report measure_units back for the aggregators to use when they need to multiply stuff
            _ = engine.validate_msr_unit_compatibility(measures)

        if verbose:
            self._print_answer(answer)

        return self, answer

    def _get_sub_exp_filters(self) -> Tuple[List[Value], Set[Table], Set[Measure], Set[Tuple[Dimension, FrozenSet[Dimension]]]]:
        """
            Helper function for getting all measures and dimensions from all
            inner VALUE expressions in this S-expression, regardless of their depth.

            :returns: Tuple containing a list of inner VALUE expressions, set with tables,
                      set with measures and set with dimensions
        """
        tables = set()
        measures = set()
        dimensions = set()
        value_exps = []
        for sub_exp in self.sub_expressions:
            inner_expressions: List[Expression] = [sub_exp]
            for inner_exp in inner_expressions:
                if isinstance(inner_exp, Value):
                    tables |= {inner_exp.table}
                    measures |= inner_exp.measures
                    dimensions |= inner_exp.dimensions
                    value_exps.append(inner_exp)
                    continue

                if hasattr(inner_exp, 'sub_expressions'):
                    inner_expressions.extend(inner_exp.sub_expressions)
                else:
                    inner_expressions.append(inner_exp.sub_expression)

        return value_exps, tables, measures, dimensions

    @property
    def odata3_pivot_sql(self):
        """
            === Example JOIN s-expression as SQL ===
            SELECT CONCAT_WS(', ', TableA.<selectorA>, TableA.<selectorB>) AS Dimension_Measure,
                   <measure-col1> <measure-col2>
            FROM (
                <sub-query>
            ) AS TableA
            INNER JOIN (
                <sub-query>
            ) AS TableB
            ON TableA.selector = TableB.selector;
        """
        inner_table_sqls = [sqlglot.parse_one(sub.odata3_pivot_sql) for sub in self.sub_expressions]

        # Get measures to select per table
        measures = []
        for inner_sql in inner_table_sqls:
            table_measures = []
            for col in inner_sql.find_all(sqlglot.exp.In):
                if 'Measure' in col.sql():
                    for msr in [s.name for s in col.expressions]:
                        table_measures.append(msr)
            measures.append(table_measures)

        # Remove selector from PIVOTs and add as WHERE clause and determine if the selector is a measure
        for_statements = []
        for i, table in enumerate(inner_table_sqls):
            pivot = table.find(sqlglot.exp.Pivot)
            dimensions = {str(group): (group, codes) for group, codes in self.sub_expressions[i].dimensions}
            table_fors = []
            for node in pivot.args['fields']:
                if isinstance(node, sqlglot.exp.In):
                    if not any(s in node.sql() for s in self.selectors):  # Non-selector, keep in PIVOT
                        table_fors.append(node)
                    else:  # JOIN selector, move to where of inner SQL for filtering
                        # (with the VALUE's own predicate, e.g. the compact LIKE of a LEVEL selection)
                        group = node.this.name
                        predicate = (self.sub_expressions[i]._dim_predicate(*dimensions[group])
                                     if group in dimensions else node.sql())
                        inner_select = table.find(sqlglot.exp.Subquery).find(sqlglot.exp.Select)
                        inner_select.where(predicate, append=True, copy=False)

            if len(table_fors) == 0:
                # Default to pivot on Measure if all table dimensions are used as join selectors
                table_fors.append(sqlglot.parse_one(f"Measure IN ('{"', '".join(measures[i])}')"))

            if len(table_fors) > 0:
                for_statements.append(table_fors)
            pivot.set('fields', table_fors)

        # Fill the SELECT and JOIN-ON statements with the proper columns
        if not self.measure_selector:
            if len(self.selectors) > 1:
                select = "CONCAT_WS(', ', TableA."
                select += ', TableA.'.join(self.selectors) + ') AS Dimension_Measure, '
                select += ', '.join([f'"{msr}"' for table_measures in measures for msr in table_measures])
            else:
                # Quote the pivoted columns, such that DuckDB can't read them as a number on accident
                select = f"TableA.{self.selectors[0]}"
                for i, fors in enumerate(for_statements):
                    combs = product(*[[s.name for s in f.expressions] for f in fors])
                    select += f", {', '.join(['"' + '_'.join(c) + '"' for c in combs])}"

            on_selectors = []
            for selector in self.selectors:
                on_selectors.append(f"TableA.{selector} = TableB.{selector}")
            join_on = ' AND '.join(on_selectors)
        else:
            select = "*"
            join_on = "TableA.Measure = TableB.Measure"

        sql = f"""
            SELECT {select}
            FROM (
                {inner_table_sqls[0]}
            ) AS TableA
            FULL JOIN (
                {inner_table_sqls[1]}
            ) AS TableB
            ON {join_on}
        """
        return sqlglot.parse_one(sql).sql(pretty=True)

    @property
    def odata3_sql(self):
        """
            === Example JOIN expressions as SQL (OData3) ===
            SELECT COALESCE(TableA.Periods, TableB.Periods) AS 'Periods',
                   BankruptciesSessionDayCorrected_1,
                   SoldHomes_4
            FROM (
                SELECT *
                FROM '<parquet-file>'
                WHERE Periods IN ('2007KW04')
                AND TypeOfBankruptcy IN ('A041718', 'A047597')
            ) AS TableA
            FULL JOIN (
                SELECT *
                FROM '<parquet-file>'
                WHERE Periods IN ('2007KW04')
            ) AS TableB
            ON TableA.Periods = TableB.Periods;

            === Example JOIN over a shared measure as SQL (OData3) ===
            (SELECT Periods, MunicipalTasks, Regions, "Expenditures_2"
             FROM '<parquet-file>'
             WHERE Periods = '2017JJ00' AND MunicipalTasks IN ('A041570', 'A041581') AND Regions = 'PV21')
            UNION ALL BY NAME
            (SELECT Periods, MunicipalTasks, Regions, "Expenditures_2"
             FROM '<other-parquet-file>'
             WHERE Periods = '2017JJ00' AND MunicipalTasks IN ('A041570', 'A041581') AND Regions = 'PV21');
        """
        inner_table_sqls = [sqlglot.parse_one(sub.odata3_sql) for sub in self.sub_expressions]

        if self.measure_selector:
            # Joining over a shared measure stacks the values of all tables. A FULL JOIN would have to match
            # the rows on their measure VALUES, silently merging identical values of two tables into a single
            # row (and dropping one of them from any aggregation over the join). UNION ALL BY NAME keeps every
            # value instead and fills the dimension groups that the other table lacks with NULLs.
            sql = '\nUNION ALL BY NAME\n'.join(f"({inner_sql.sql()})" for inner_sql in inner_table_sqls)
            return sqlglot.parse_one(sql).sql(pretty=True)

        for inner_sql in inner_table_sqls:
            inner_sql.find(sqlglot.exp.Select).args['expressions'] = [sqlglot.exp.Star()]

        def ident(name: str) -> str:
            return f'"{name}"' if name.replace('_', '').isnumeric() else name

        measures_per_inner = [sorted(map(str, sub_exp.measures)) for sub_exp in self.sub_expressions]
        dimensions_per_inner = [{str(group): frozenset(map(str, codes)) for group, codes in sub_exp.dimensions}
                                for sub_exp in self.sub_expressions]

        # Join keys: the selector, plus every other shared dimension group that both sides filter on exactly the
        # same codes (these describe the same rows, e.g. the same gender in both tables). A shared group filtered on
        # different codes (e.g. two different industries) is NOT a key: joining on it would never match and split
        # the answer over half-empty rows, like the pivot SQL (which only joins on the selector) never does. Keys are
        # compared as text, because a code column can be numeric in one table and textual in the other.
        shared_dims = [g for g in dimensions_per_inner[0] if g in dimensions_per_inner[1] and g not in self.select_cols]
        keys = list(self.select_cols) + sorted(g for g in shared_dims
                                               if dimensions_per_inner[0][g] == dimensions_per_inner[1][g])
        join_on = ' AND '.join(f"CAST(TableA.{ident(k)} AS VARCHAR) = CAST(TableB.{ident(k)} AS VARCHAR)"
                               for k in keys)

        # Fill the SELECT with the keys (present in either table), then the measures and remaining dimension groups
        # of both tables. A column name that TableA already provides gets a '_B' suffix for TableB.
        select_cols = [f"COALESCE(TableA.{ident(k)}, TableB.{ident(k)}) AS {ident(k)}" for k in keys]
        taken = set(keys)
        for columns_per_inner in (measures_per_inner, [sorted(dims) for dims in dimensions_per_inner]):
            for table, columns in zip(['TableA', 'TableB'], columns_per_inner):
                for col in columns:
                    if col in keys:
                        continue
                    if col in taken:
                        select_cols.append(f'{table}.{ident(col)} AS "{col}_B"')
                    else:
                        select_cols.append(f"{table}.{ident(col)}")
                        taken.add(col)
        select_cols = ',\n'.join(select_cols)

        sql = f"""
            SELECT {select_cols}
            FROM (
                {inner_table_sqls[0]}
            ) AS TableA
            FULL JOIN (
                {inner_table_sqls[1]}
            ) AS TableB
            ON {join_on}
        """
        return sqlglot.parse_one(sql).sql(pretty=True)


if __name__ == '__main__':
    from s_expression.parser import parse, eval

    eval(parse("""
        (JOIN (Regions) (VALUE 80783eng (MSR (CattleTotal_103)) (DIM FarmTypes (A009518 A009481 A009510)) (DIM Regions (LG12  )) (DIM Periods (2006JJ00))) (VALUE 80783eng (MSR (CattleTotal_103)) (DIM FarmTypes (A009518 A009481 A009510)) (DIM Regions (LG12  )) (DIM Periods (2007JJ00))) (VALUE 80784eng (MSR (RegularlyEmployedTotal_19)) (DIM Regions (LG12  )) (DIM Gender (3000  )) (DIM Periods (2014JJ00))))
    """), verbose=True)

    eval(parse("""
        (JOIN (M004032) (VALUE 85601NED (MSR (M004032)) (DIM ContainerGrootte (A052183 A052184)) (DIM Vervoerstroom (A045748)) (DIM Perioden (2023JJ00))) (VALUE 85598NED (MSR (M004032)) (DIM NederlandseZeehavens (T001293)) (DIM SoortLading (A041789)) (DIM Vervoerstroom (A045748)) (DIM Perioden (2023JJ00))))
    """), verbose=True)

    eval(parse("""
        (JOIN (Perioden) (VALUE 85601NED (MSR (M004032)) (DIM ContainerGrootte (A052183 A052184)) (DIM Vervoerstroom (A045748)) (DIM Perioden (2023JJ00))) (VALUE 85598NED (MSR (M004032)) (DIM NederlandseZeehavens (T001293)) (DIM SoortLading (A041789)) (DIM Vervoerstroom (A045748)) (DIM Perioden (2023JJ00))))
    """), verbose=True)

    eval(parse("""
        (JOIN (Regions) (VALUE 80783eng (MSR (CattleTotal_103)) (DIM FarmTypes (A009518 A009481 A009510)) (DIM Regions (LG12  )) (DIM Periods (2006JJ00))) (VALUE 80784eng (MSR (RegularlyEmployedTotal_19)) (DIM Regions (LG12  )) (DIM Gender (3000  )) (DIM Periods (2014JJ00))))
    """), verbose=True)

    eval(parse("""
        (JOIN (Perioden)
            (VALUE 84957NED
                (MSR (M004367))
                (DIM Perioden (2021JJ00 2022JJ00))
                (DIM Vervoerstromen (A045747))
            )
            (VALUE 85302NED
                (MSR (D006211_2))
                (DIM Perioden (2021JJ00 2022JJ00))
                (DIM BestemmingEnSeizoen (T001047))
                (DIM Vakantiekenmerken (T001460))
                (DIM Marges (MW00000))
            )
        )
    """), verbose=True)
