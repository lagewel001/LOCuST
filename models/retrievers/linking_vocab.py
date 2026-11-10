"""
    Curated NL/EN vocabulary for rule-based geo/time entity-linking (see geo_time_linker.py).
"""
MONTHS = ['januari', 'january', 'februari', 'february', 'maart', 'march', 'april',
          'mei', 'may', 'juni', 'june', 'juli', 'july', 'augustus', 'august', 'september',
          'oktober', 'october', 'november', 'december']

# Month name -> two-digit month number
_MONTH_NUMBERS = {
    'januari': 1, 'january': 1,
    'februari': 2, 'february': 2,
    'maart': 3, 'march': 3,
    'april': 4,
    'mei': 5, 'may': 5,
    'juni': 6, 'june': 6,
    'juli': 7, 'july': 7,
    'augustus': 8, 'august': 8,
    'september': 9,
    'oktober': 10, 'october': 10,
    'november': 11,
    'december': 12,
}


def month_to_code(month: str) -> str:
    """Two-digit month code for a Dutch/English month name (always str, zero-padded)."""
    return f"{_MONTH_NUMBERS[month.lower()]:02d}"


# Year-part keyword -> period infix (or infix+number). List values are alternatives to offer
# ('eind 2023' can mean the last quarter or the last half year).
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

# Ordinal keyword -> two-digit part number ('eerste kwartaal' -> 01). Note: 'last'/'laatste'
# also appear as relative markers; the grammar treats them as cardinals only when an explicit
# year is present ('last quarter of 2023'), and as relative otherwise ('last quarter').
CARDINALS = {
    # Dutch
    'eerste': '01', '1e': '01', '1': '01',
    'tweede': '02', '2e': '02', '2': '02',
    'derde': '03', '3e': '03', '3': '03',
    'vierde': '04', '4e': '04', '4': '04',
    'laatste': '04',
    # English
    'first': '01', '1st': '01',
    'second': '02', '2nd': '02',
    'third': '03', '3rd': '03',
    'fourth': '04', '4th': '04',
    'last': '04',
}

# Relative-time marker -> offset from the reference period
RELATIVE_TIME_CARDINALS = {
    # Dutch
    'vorig': -1, 'vorige': -1, 'afgelopen': -1, 'verleden': -1,
    'dit': 0, 'deze': 0, 'huidig': 0, 'huidige': 0,
    'volgend': 1, 'volgende': 1, 'komend': 1, 'komende': 1,
    # English
    'previous': -1, 'last': -1, 'past': -1,
    'this': 0, 'current': 0,
    'next': 1, 'coming': 1
}

# Granularity keyword for relative expressions ('vorig jaar', 'afgelopen maand').
# KW and HJ are covered through YEAR_PARTS.
RELATIVE_TIME_KEYWORDS = {
    # Dutch
    'jaar': 'JJ', 'maand': 'MM',
    # English
    'year': 'JJ', 'month': 'MM'
}

RANGE_KEYWORDS = [
    'vanaf', 'tot', 'tot en met', 't/m', 'tussen', 'gedurende', 'sinds',
    'since', 'starting', 'until', 'from', 'up to', 'to', 'between', 'within', 'through', 'during'
]

# Geo level hint keyword -> canonical level token. Hints BOOST candidates of the hinted level
# during disambiguation ('provincie Utrecht' -> PV26 over GM0344) but never suppress the
# other candidates: the query generator should ultimately make the final selection from the surfaced options.
LEVEL_HINTS = {
    # Dutch
    'corop': 'CR', 'omgeving': 'CR', 'provincie': 'PV', 'gemeente': 'GM', 'stad': 'GM',
    'plaats': 'GM', 'wijk': 'WK', 'buurt': 'BU', 'landsdeel': 'LD',
    # English
    'area': 'CR', 'province': 'PV', 'municipality': 'GM', 'place': 'GM', 'city': 'GM',
    'town': 'GM', 'district': 'WK', 'neighbourhood': 'BU', 'neighborhood': 'BU', 'region': 'LD'
}
