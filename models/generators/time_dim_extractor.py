"""
    This module contains code for rule-based extraction/pre-selection of TimeDimensions from a question.
"""
import datetime
import math
import re
from dateutil.relativedelta import relativedelta
from typing import Set

MONTHS = ['januari', 'january', 'februari', 'february', 'maart', 'march', 'april',
          'mei', 'may', 'juni', 'june', 'juli', 'july' 'augustus', 'august', 'september',
          'oktober', 'october', 'november', 'december']

YEAR_PARTS = {
    # Dutch
    'kwartaal': 'KW', 'kwartalen': 'KW', 'kwart': 'KW', 'seizoen': 'KW', 'seizoenen': 'KW',
    'eind': ['KW04', 'HJ02'], 'begin': ['KW01', 'HJ01'], 'helft': 'HJ', 'q1': 'KW01',
    'q2': 'KW02', 'q3': 'KW03', 'q4': 'KW04', 'lente': 'KW01', 'zomer': 'KW02', 'herfst': 'KW03',
    'winterseizoen': 'KW04', 'lenteseizoen': 'KW01', 'zomerseizoen': 'KW02', 'herfstseizoen': 'KW03',
    'winter': 'KW04',
    # English
    'quarter': 'KW', 'quarters': 'KW', 'season': 'KW', 'seasons': 'KW', 'end': ['KW04', 'HJ02'],
    'beginning': ['KW01', 'HJ01'], 'start': ['KW01', 'HJ01'], 'half': 'HJ', 'spring': 'KW01',
    'summer': 'KW02', 'autumn': 'KW03', 'fall': 'KW03', 'winter season': 'KW04', 'spring season': 'KW01',
    'summer season': 'KW02', 'autumn season': 'KW03'
}

CARDINALS = {
    # Dutch
    'eerste': '01',
    '1e': '01',
    '1': '01',
    'tweede': '02',
    '2e': '02',
    '2': '02',
    'derde': '03',
    '3e': '03',
    '3': '03',
    'vierde': '04',
    '4e': '04',
    '4': '04',
    'laatste': '04',
    # English
    'first': '01',
    '1st': '01',
    'second': '02',
    '2nd': '02',
    'third': '03',
    '3rd': '03',
    'fourth': '04',
    '4th': '04',
    'last': '04'
}

RELATIVE_TIME_CARDINALS = {
    'vorig': -1, 'vorige': -1, 'afgelopen': -1, 'verleden': -1,
    'dit': 0, 'deze': 0, 'huidig': 0, 'huidige': 0,
    'volgend': 1, 'volgende': 1, 'komend': 1, 'komende': 1,
    # English
    'previous': -1, 'last': -1, 'past': -1,
    'this': 0, 'current': 0,
    'next': 1, 'coming': 1
}

RELATIVE_TIME_KEYWORDS = {  # `KW` and `HJ` already in year_parts
    'jaar': 'JJ', 'maand': 'MM',
    # English
    'year': 'JJ', 'month': 'MM'
}

RANGE_KEYWORDS = [
    'vanaf', 'tot', 'tot en met', 't/m', 'tussen', 'gedurende',
    'since', 'starting', 'until', 'from', 'up to', 'to', 'between', 'within', 'through', 'during'
]

months_re = re.compile(fr'(?<!\w)({"|".join(MONTHS)})(?!\w)')
year_parts_re = re.compile(fr'(?<!\w)({"|".join(YEAR_PARTS)})(?!\w)')
cardinals_re = re.compile(fr'(?<!\w)({"|".join(CARDINALS)})(?!\w)')
years_re = re.compile(r'(?<!\d)\d{4}(?!\d)')
range_re = re.compile(fr'(?<!\w)({"|".join(RANGE_KEYWORDS)})(?!\w)')

relative_time_re = re.compile(rf'''(?mx)
    (?<!\w)
    (?P<cardinality>{"|".join(RELATIVE_TIME_CARDINALS)})\s
    (?P<year_part>{"|".join(list(RELATIVE_TIME_KEYWORDS) + list(YEAR_PARTS))})
    (?!\w)
''')


def month_to_code(month):
    index = MONTHS.index(month) + 1
    if index < 10:
        return f"0{index}"
    return index


def extract_tc(query: str, available_time_constraints: Set[str] = None) -> Set[str]:
    """
        Do a rule based extraction of time-constraints on the query. A query can either
        contain an absolute time constraint ('hoeveel faillissementen in 2020') or a relative
        time constraint ('hoeveel faillissementen afgelopen jaar').

        :param query: query to extract time-constraints from
        :param available_time_constraints: set of time dimension values available for the chosen table
        :return: set containing time-constraint identifiers as can be found in the graph
    """
    query = query.lower()
    available_time_constraints = available_time_constraints or []
    extracted_tc = set()

    def _extract_years() -> list:
        years_in_query = years_re.findall(query)
        max_year = datetime.datetime.now().year + 1000
        years_in_query = [year for year in years_in_query if int(year) <= max_year]
        return years_in_query

    years_in_query = list(set(_extract_years()))
    if re.search(range_re, query) and years_in_query:
        if len(years_in_query) > 1:
            years_as_num = sorted([int(y) for y in years_in_query])
        else: # i.e. len(years_in_query) == 1
            # This doesn't regard if you ask 'before year X' or 'after year X', but gives the most options
            years_as_num = sorted([int(years_in_query[0])] + [int(t[:4]) for t in available_time_constraints])
        years_in_query = list(map(str, range(years_as_num[0], years_as_num[-1] + 1)))

    months_in_query = list(set(months_re.findall(query)))

    # Check relative time mentions
    # TODO: no support yet for specific quarterly questions like 'Hoeveel x waren er afgelopen zomer?'
    relatives_in_query = [m.groupdict() for m in relative_time_re.finditer(query)]
    if len(relatives_in_query):
        t0 = datetime.datetime.now()
        cardinality = RELATIVE_TIME_CARDINALS[relatives_in_query[0]['cardinality']]
        year_part = (RELATIVE_TIME_KEYWORDS | YEAR_PARTS)[relatives_in_query[0]['year_part']]

        code = None
        if year_part == 'JJ':
            code = f"{t0.year + cardinality}{year_part}00"
        if year_part == 'MM':
            t = t0 + relativedelta(months=1 * cardinality)
            code = f"{t0.year}{year_part}{t.strftime('%m')}"
        if year_part == 'HJ':
            curr_h = math.ceil(t0.month / 6)
            new_h = curr_h + cardinality

            new_y = t0.year
            if new_h < 1:
                new_y = t0.year - 1
            elif new_h > 2:
                new_y = t0.year + 1
            code = f"{new_y}{year_part}0{range(1, 3)[3 % 2 - 1]}"
        if year_part == 'KW':
            curr_q = math.ceil(t0.month / 3)
            new_q = curr_q + cardinality

            new_y = t0.year
            if new_q < 1:
                new_y = t0.year - 1
            elif new_q > 4:
                new_y = t0.year + 1
            code = f"{new_y}{year_part}0{range(1, 5)[new_q % 4 - 1]}"

        if code in available_time_constraints:
            extracted_tc.add(code)

    if months_in_query:
        for month in months_in_query:
            for year in years_in_query:
                code = f"{year}MM{month}"
                if code in available_time_constraints:
                    extracted_tc.add(code)

    year_parts_in_query = list(set(year_parts_re.findall(query)))
    if year_parts_in_query:
        for year_part in year_parts_in_query:
            for year in years_in_query:
                yp = YEAR_PARTS[year_part]
                if isinstance(yp, list):
                    if not available_time_constraints or f"{year}{yp[0]}" in available_time_constraints:
                        extracted_tc.add(f"{year}{yp[0]}")
                    if f"{year}{yp[1]}" in available_time_constraints:
                        extracted_tc.add(f"{year}{yp[1]}")

                if len(yp) == 4:
                    code = f"{years_in_query[0]}{yp}"
                    if code in available_time_constraints:
                        extracted_tc.add(code)

        cardinals_in_query = cardinals_re.findall(query)
        if cardinals_in_query:
            cardinal = CARDINALS[cardinals_in_query[0]]
            for year in years_in_query:
                code = f"{year}{YEAR_PARTS[year_parts_in_query[0]]}{cardinal}"
                if code in available_time_constraints:
                    extracted_tc.add(code)

    for year in years_in_query:
        code = f"{year}JJ00"
        if code in available_time_constraints:
            extracted_tc.add(code)

    return extracted_tc
