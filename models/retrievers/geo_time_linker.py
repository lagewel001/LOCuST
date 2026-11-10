"""
    Rule-based geo/time entity-linking: recognize question-named regions ("Amsterdam") and
    periods ("eerste kwartaal 2023") and surface their codes as scored nodes for the query
    generators' prompts. This complements the semantic retrievers, which deliberately exclude
    time/geo codes from their index (region names are a closed vocabulary with exact-match
    semantics, ill-suited to embedding retrieval).

    The linker only surfaces candidates: on ambiguity ("Utrecht" is both the province PV26 and
    the municipality GM0344) all candidates are emitted, with the level hinted by the question
    ("provincie Utrecht") boosted, and the generator should ultimately make the final selection.

    The geo gazetteer is built from the metadata graph (labels + gecko level classes).
    Time expressions need no gazetteer: they are parsed by grammar and resolved against the wall
    clock: "afgelopen jaar" is the year before the one the question is asked in, which is what an
    end user means, rather than the year before the table's newest available period.
"""
import json
import os
import re
from datetime import datetime
from typing import Dict, FrozenSet, List, Optional, Set, Tuple

import config
from models.retrievers.linking_vocab import (CARDINALS, LEVEL_HINTS, MONTHS, RANGE_KEYWORDS,
                                             RELATIVE_TIME_CARDINALS, RELATIVE_TIME_KEYWORDS,
                                             YEAR_PARTS, month_to_code)
from odata_graph import engine
from odata_graph.sparql_controller import GECKO
from s_expression import Table

GAZETTEER_PATH = f"{config.PATH_DIR_DATA}/geo_time_linker/gazetteer_{config.GRAPH_DB_REPO}.json"

LINK_SCORE = 1.0        # linked codes (unambiguous, or of the question-hinted level)
AMBIGUOUS_SCORE = 0.8   # the other candidates of an ambiguous name (LLM disambiguates)

months_re = re.compile(fr'(?<!\w)({"|".join(MONTHS)})(?!\w)')
year_parts_re = re.compile(fr'(?<!\w)({"|".join(YEAR_PARTS)})(?!\w)')
cardinals_re = re.compile(fr'(?<!\w)({"|".join(CARDINALS)})(?!\w)')
years_re = re.compile(r'(?<!\d)(1[89]\d{2}|20\d{2})(?!\d)')  # sane year bounds: 1800-2099
range_re = re.compile(fr'(?<!\w)({"|".join(RANGE_KEYWORDS)})(?!\w)')
relative_time_re = re.compile(rf'''(?mx)
    (?<!\w)
    (?P<cardinality>{"|".join(RELATIVE_TIME_CARDINALS)})\s
    (?P<year_part>{"|".join(list(RELATIVE_TIME_KEYWORDS) + list(YEAR_PARTS))})
    (?!\w)
''')

# Periods per year for each relative-resolvable granularity
_PARTS_PER_YEAR = {'JJ': 1, 'HJ': 2, 'KW': 4, 'MM': 12}


def normalize_geo_text(text: str) -> str:
    """
        Shared normalization for gazetteer labels and questions: lowercase, strip
        parenthesized level suffixes ('Zuid-Holland (PV)'), strip CBS 'Wijk 00 ' label
        prefixes, and fold hyphens to spaces so 'Midden-Delfland' matches either spelling.
    """
    text = text.lower().strip()
    text = re.sub(r'\s*\([^)]*\)\s*$', '', text)
    text = re.sub(r'^wijk \d+\s*', '', text)
    text = text.replace('-', ' ')
    return re.sub(r'\s+', ' ', text).strip()


class GeoTimeLinker:
    def __init__(self):
        # {normalized_label: [(code, level_token), ...]} for all geo codes in the graph; built once and JSON-cached per graph repo
        self.gazetteer = self._load_gazetteer()

        # Index labels by their first word so a question only checks phrases it could contain.
        self._by_first_word: Dict[str, List[str]] = {}
        for label in self.gazetteer:
            if words := re.findall(r'\w+', label):
                self._by_first_word.setdefault(words[0], []).append(label)
        self._table_geo_cache: Dict[str, Dict[str, Tuple[str, str]]] = {}
        self._table_time_cache: Dict[str, FrozenSet[str]] = {}

    @staticmethod
    def _load_gazetteer() -> Dict[str, List[Tuple[str, str]]]:
        use_cache = not config.IS_UNIT_TESTING
        if use_cache and os.path.isfile(GAZETTEER_PATH):
            with open(GAZETTEER_PATH) as f:
                return {label: [tuple(c) for c in codes] for label, codes in json.load(f).items()}

        geo_classes = [c for c, meta in engine.level_classes().items() if meta['base'] == str(GECKO.GeoDimension)]
        rows = engine.select(f"""
            PREFIX qb: <http://purl.org/linked-data/cube#>
            PREFIX dct: <http://purl.org/dc/terms/>
            PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
            
            SELECT DISTINCT ?id ?label ?cls WHERE {{
                ?code a ?cls ;
                      dct:identifier ?id ;
                      qb:concept ?concept .
                ?concept skos:prefLabel ?label .
                FILTER (?cls IN ({', '.join(f'<{c}>' for c in geo_classes)}))
            }}
        """)

        gazetteer: Dict[str, List[Tuple[str, str]]] = {}
        for r in rows:
            label = normalize_geo_text(r['label']['value'])
            if not label:
                continue
            entry = (r['id']['value'], engine._level_token(r['cls']['value']))
            if entry not in gazetteer.setdefault(label, []):
                gazetteer[label].append(entry)

        if use_cache:
            os.makedirs(os.path.dirname(GAZETTEER_PATH), exist_ok=True)
            with open(GAZETTEER_PATH, 'w') as f:
                json.dump(gazetteer, f)
        return gazetteer

    def link_geo(self, question: str) -> Dict[str, Tuple[str, float]]:
        """
            Match region names in the question against the gazetteer.
            Longest-match-wins: a name whose span lies inside a longer matched
            name is dropped ('Alphen' inside 'Alphen aan den Rijn'). A LEVEL_HINTS keyword in
            the question ('provincie', 'stad', ...) boosts candidates of the hinted level(s).

            :param question: the natural language question
            :returns: {code: (level_token, score)}, table-independent (validated per table in enrich)
        """
        normalized = normalize_geo_text(question)
        words = set(re.findall(r'\w+', normalized))

        matches: List[Tuple[int, int, str]] = []  # (start, end, label)
        for word in words & set(self._by_first_word):
            for label in self._by_first_word[word]:
                for m in re.finditer(rf'(?<!\w){re.escape(label)}(?!\w)', normalized):
                    matches.append((m.start(), m.end(), label))

        # Longest-match-wins: drop matches strictly contained in another match's span
        surviving = [m for m in matches
                     if not any(o[0] <= m[0] and m[1] <= o[1] and (o[1] - o[0]) > (m[1] - m[0])
                     for o in matches)]

        hinted_levels = {LEVEL_HINTS[w] for w in words if w in LEVEL_HINTS}
        links: Dict[str, Tuple[str, float]] = {}
        for _, _, label in surviving:
            candidates = self.gazetteer[label]
            for code, token in candidates:
                ambiguous = len(candidates) > 1
                score = LINK_SCORE if (not ambiguous or token in hinted_levels) else AMBIGUOUS_SCORE
                if code not in links or score > links[code][1]:
                    links[code] = (token, score)
        return links

    def extract_time(self, question: str, available: Optional[Set[str]] = None) -> Set[str]:
        """
            Grammar-based extraction of period codes from the question. Deliberately
            over-generates ('eind 2023' offers both 2023KW04 and 2023HJ02; a bare year offers
            2023JJ00): the table's available codes filter first, the LLM selects last.

            :param question: the natural language question relative expressions ('afgelopen jaar')
                             resolve against the wall clock; if the table does not reach that period
                             the intersection below simply drops the candidate.

            :param available: the table's available period codes; candidates are intersected
                              with it (and single-endpoint ranges expand against it).
                              None (unit tests) skips filtering.
            :returns: set of period codes (e.g. {'2023KW01', '2023JJ00'})
        """
        query = question.lower()
        candidates: Set[str] = set()

        years = sorted(set(years_re.findall(query)))
        if len(years) > 1 and range_re.search(query):
            # Multiple years with a range marker span between them ('tussen 2019 en 2022')
            years = self._expand_year_range(years, available)
        elif len(years) == 1 and (m := re.search(rf'(?<!\w)({"|".join(RANGE_KEYWORDS)})\s+{years[0]}', query)):
            # A single endpoint expands against the table's available years, but only when the
            # range marker is adjacent to the year ('sinds 2015'): generic markers like the
            # English 'to' would otherwise fan a bare year mention out to every year
            years = self._expand_year_range(years, available, keyword=m.group(1))

        # Months x years -> monthly codes
        for month in set(months_re.findall(query)):
            for year in years:
                candidates.add(f"{year}MM{month_to_code(month)}")

        # Year parts (quarters/half years/seasons) x years, optionally with an ordinal
        year_parts = list(set(year_parts_re.findall(query)))
        cardinals = cardinals_re.findall(query)
        for keyword in year_parts:
            part = YEAR_PARTS[keyword]
            for year in years:
                if isinstance(part, list):        # 'eind'/'begin': offer every alternative
                    candidates.update(f"{year}{alt}" for alt in part)
                elif len(part) == 4:              # fully-specified part, e.g. 'q1' -> KW01
                    candidates.add(f"{year}{part}")
                elif cardinals:                   # infix + ordinal: 'eerste kwartaal' -> KW01
                    candidates.add(f"{year}{part}{CARDINALS[cardinals[0]]}")

        # Relative expressions ('vorig jaar', 'afgelopen kwartaal'), resolved against the wall clock
        for m in relative_time_re.finditer(query):
            cardinality_word = m.group('cardinality')
            if years and cardinality_word in CARDINALS:
                continue  # 'last quarter OF 2023' is an ordinal (handled above), not relative
            code = self._resolve_relative(RELATIVE_TIME_CARDINALS[cardinality_word],
                                          (RELATIVE_TIME_KEYWORDS | YEAR_PARTS)[m.group('year_part')])
            if code:
                candidates.add(code)

        # A bare year always offers its yearly total
        candidates.update(f"{year}JJ00" for year in years)

        return candidates & available if available is not None else candidates

    @staticmethod
    def _expand_year_range(years: List[str], available: Optional[Set[str]],
                           keyword: Optional[str] = None) -> List[str]:
        """
            Expand a range expression to the full year span: two mentioned years span between
            them ('tussen 2019 en 2022'); a single endpoint spans towards the table's available
            years, following the range marker's direction ('sinds 2015' runs forward to the
            newest year, 'tot 2015' backward to the oldest; ambiguous markers span both ways).
        """
        bounds = [int(y) for y in years]
        if len(bounds) == 1 and available:
            table_years = [int(c[:4]) for c in available if re.match(r'^\d{4}', c)]
            if not table_years:
                return years
            if keyword in {'vanaf', 'sinds', 'since', 'starting', 'from'}:
                bounds.append(max(table_years))
            elif keyword in {'tot', 'tot en met', 't/m', 'until', 'up to', 'to'}:
                bounds.append(min(table_years))
            else:
                bounds += [min(table_years), max(table_years)]
        return [str(y) for y in range(min(bounds), max(bounds) + 1)]

    @staticmethod
    def _resolve_relative(offset: int, granularity) -> Optional[str]:
        """
            Resolve a relative expression to a concrete period code, relative to the wall clock:
            'afgelopen jaar' is the year before the one the question is asked in, which is what
            an end user means, regardless of how far the table's data happens to run.
        """
        if not isinstance(granularity, str) or granularity not in _PARTS_PER_YEAR:
            return None  # e.g. 'eind'/'begin' alternatives carry no single granularity

        parts_per_year = _PARTS_PER_YEAR[granularity]
        now = datetime.now()
        year = now.year
        part = 0 if granularity == 'JJ' else -(-now.month * parts_per_year // 12)  # ceil

        if granularity == 'JJ':
            return f"{year + offset}JJ00"

        # Shift within the granularity's cycle, carrying over year boundaries
        total = year * parts_per_year + (part - 1) + offset
        return f"{total // parts_per_year}{granularity}{total % parts_per_year + 1:02d}"

    def _table_geo_codes(self, table_id: str) -> Dict[str, Tuple[str, str]]:
        if table_id not in self._table_geo_cache:
            self._table_geo_cache[table_id] = engine.get_table_geo_dims(Table(table_id), levels='all')
        return self._table_geo_cache[table_id]

    def _table_time_codes(self, table_id: str) -> FrozenSet[str]:
        if table_id not in self._table_time_cache:
            self._table_time_cache[table_id] = frozenset(engine.get_table_time_dims(Table(table_id)))
        return self._table_time_cache[table_id]

    def enrich(self, question: str, retrieved_tables: dict) -> dict:
        """
            Merge the question's linked geo/time codes into the retriever output, per table:
            only codes the table actually contains are added, as ordinary scored nodes, so
            they flow through prompt building (and its geo/time compaction) unchanged.

            :param question: the natural language question
            :param retrieved_tables: retriever output ({table_id: {'score': ..., 'nodes': ...}})
            :returns: the same dict, with linked codes merged into each table's 'nodes'
        """
        geo_links = self.link_geo(question)
        for table_id, data in retrieved_tables.items():
            nodes = data.setdefault('nodes', {})
            table_geo = self._table_geo_codes(table_id)
            for code, (_, score) in geo_links.items():
                if code in table_geo:
                    nodes[code] = {'score': max(score, nodes.get(code, {}).get('score') or 0)}
            for code in self.extract_time(question, available=self._table_time_codes(table_id)):
                nodes[code] = {'score': max(LINK_SCORE, nodes.get(code, {}).get('score') or 0)}
        return retrieved_tables
