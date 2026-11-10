"""
    Tests for the rule-based geo/time entity-linker: gazetteer building/matching, the temporal
    grammar (wall-clock-relative resolution), the retriever-output enrichment, and the engine/prompt
    plumbing it relies on (parameterized get_table_geo_dims, small-level prompt safety net).

    The unittest graph contains for table 80781ned (a.o.): Utrecht as PV26 + GM0344 + CR17,
    Venray GM0984, Ede GM0228, Groningen PV20 + GM0014, Midden-Delfland GM1842,
    Alphen aan den Rijn GM0484, and years 2000JJ00-2025JJ00.
"""
import pytest
from datetime import datetime

from models.generators.llm_baseline.base_llm_generator import BaseLLMGenerator
from models.retrievers.geo_time_linker import (AMBIGUOUS_SCORE, LINK_SCORE, GeoTimeLinker,
                                               normalize_geo_text)
from s_expression import Table

TABLE = Table('80781ned')


@pytest.fixture(scope='module')
def linker():
    return GeoTimeLinker()


# --- Gazetteer & geo matching ---

def test_normalization():
    assert normalize_geo_text('Zuid-Holland (PV)') == 'zuid holland'
    assert normalize_geo_text('Wijk 00 Binnenstad') == 'binnenstad'
    assert normalize_geo_text("  Alphen  aan den Rijn ") == 'alphen aan den rijn'


def test_gazetteer_contents(linker):
    assert ('GM0984', 'GM') in linker.gazetteer['venray']
    assert {token for _, token in linker.gazetteer['utrecht']} == {'PV', 'GM', 'CR'}
    # Suffixed labels are findable under their bare name
    assert ('PV28', 'PV') in linker.gazetteer['zuid holland']


def test_link_geo_simple(linker):
    links = linker.link_geo("Hoeveel leghennen waren er in Venray in 2020?")
    assert links == {'GM0984': ('GM', LINK_SCORE)}


def test_link_geo_multiword_and_hyphens(linker):
    assert 'GM0484' in linker.link_geo("Hoeveel varkens had Alphen aan den Rijn?")
    # Hyphenated names match in both spellings through hyphen->space folding
    assert 'GM1842' in linker.link_geo("Wat is de veestapel van Midden-Delfland?")
    assert 'GM1842' in linker.link_geo("Wat is de veestapel van Midden Delfland?")


def test_link_geo_word_boundaries(linker):
    """The old substring approach matched 'Ede' inside 'bestede'; boundaries prevent that."""
    assert 'GM0228' not in linker.link_geo("Hoeveel geld werd er besteed aan veevoer, bestede euro's?")
    assert 'GM0228' in linker.link_geo("Hoeveel kippen waren er in Ede?")


def test_link_geo_ambiguity_surfaces_all(linker):
    """Ambiguous names surface ALL candidates; the LLM disambiguates."""
    links = linker.link_geo("Hoeveel leghennen waren er in Utrecht?")
    assert set(links) == {'PV26', 'GM0344', 'CR17'}
    assert all(score == AMBIGUOUS_SCORE for _, score in links.values())


def test_link_geo_level_hint_boosts(linker):
    """A level keyword boosts the hinted candidates without suppressing the others."""
    links = linker.link_geo("Hoeveel leghennen waren er in de provincie Utrecht?")
    assert links['PV26'] == ('PV', LINK_SCORE)
    assert links['GM0344'] == ('GM', AMBIGUOUS_SCORE)  # still surfaced

    links = linker.link_geo("Hoeveel leghennen waren er in de stad Utrecht?")
    assert links['GM0344'] == ('GM', LINK_SCORE)


# --- Time grammar (unconstrained mode) ---

def test_time_bare_year(linker):
    assert linker.extract_time("Hoeveel varkens waren er in 2020?") == {'2020JJ00'}


def test_time_year_range(linker):
    codes = linker.extract_time("Hoeveel varkens waren er tussen 2019 en 2022?")
    assert codes == {'2019JJ00', '2020JJ00', '2021JJ00', '2022JJ00'}


def test_time_single_endpoint_range(linker):
    """'sinds <year>' expands against the table's available years; a bare year does not."""
    available = {'2014JJ00', '2015JJ00', '2016JJ00', '2017JJ00'}
    assert linker.extract_time("Hoeveel varkens waren er sinds 2015?", available) == \
           {'2015JJ00', '2016JJ00', '2017JJ00'}
    # Generic markers not adjacent to the year must not fan out ('...compared to the pigs in 2016')
    assert linker.extract_time("What happened to the pigs in 2016?", available) == {'2016JJ00'}


def test_time_quarters_and_ordinals(linker):
    assert '2023KW01' in linker.extract_time("eerste kwartaal van 2023")
    assert '2023KW04' in linker.extract_time("laatste kwartaal van 2023")  # ordinal, not relative
    assert '2021KW02' in linker.extract_time("de zomer van 2021")
    # 'eind' offers both the quarter and half-year readings
    assert {'2023KW04', '2023HJ02'} <= linker.extract_time("eind 2023")


def test_time_months(linker):
    assert linker.extract_time("Hoeveel faillissementen waren er in maart 2024?") >= {'2024MM03'}


def test_time_relative_wall_clock(linker):
    """Relative expressions resolve against the wall clock, not the table's newest period."""
    now = datetime.now()
    this_quarter = -(-now.month * 4 // 12)  # ceil
    prev_quarter_year, prev_quarter = (now.year, this_quarter - 1) if this_quarter > 1 else (now.year - 1, 4)

    # A table running well past the current year must not shift the reference period
    years = {f"{y}JJ00" for y in range(2000, now.year + 5)}
    assert linker.extract_time("Hoeveel varkens waren er afgelopen jaar?", years) == {f'{now.year - 1}JJ00'}
    assert linker.extract_time("Hoeveel varkens zijn er dit jaar?", years) == {f'{now.year}JJ00'}

    quarters = {f"{y}KW{q:02d}" for y in range(2000, now.year + 5) for q in range(1, 5)}
    assert linker.extract_time("Hoeveel faillissementen waren er vorig kwartaal?", quarters) == \
           {f'{prev_quarter_year}KW{prev_quarter:02d}'}

    # A table that lags behind the wall clock links nothing rather than the newest period it has
    assert linker.extract_time("Hoeveel varkens waren er afgelopen jaar?", {'2015JJ00', '2016JJ00'}) == set()


def test_time_available_filter(linker):
    """Candidates outside the table's available codes are dropped; surviving offers remain."""
    assert linker.extract_time("eerste kwartaal van 2023", available={'2023JJ00'}) == {'2023JJ00'}
    assert linker.extract_time("eerste kwartaal van 2023", available={'2022JJ00'}) == set()


# --- Engine plumbing ---

def test_get_table_geo_dims_levels(sparql_engine):
    default = sparql_engine.get_table_geo_dims(TABLE)
    assert default['GM0984'] == ('Venray', 'GM')
    assert not {token for _, token in default.values()} & {'WK', 'BU'}

    assert sparql_engine.get_table_geo_dims(TABLE, levels={'PV'}).keys() == \
           {f'PV{n}' for n in range(20, 32)}
    # 'all' ⊇ default (the fixture has no WK/BU codes to widen with, but the call path works)
    assert sparql_engine.get_table_geo_dims(TABLE, levels='all').keys() >= default.keys()


def test_dim_level_summary_small_level_codes(sparql_engine):
    summary = sparql_engine.get_dim_level_summary(TABLE, include_codes_upto=15)
    by_token = {token: (n, codes) for token, _, n, codes in summary['RegioS']}
    assert by_token['PV'] == (12, [f'PV{n}' for n in range(20, 32)])
    assert by_token['GM'] == (605, None)  # large levels return no code list


# --- Enrichment & prompt integration ---

class _Gen(BaseLLMGenerator):
    def __init__(self):
        pass

    def _call_llm(self, system_prompt: str, messages: list):
        raise NotImplementedError("Only the prompt building is tested")


def test_enrich_merges_table_valid_links(linker):
    tables = {'80781ned': {'score': 0.9, 'nodes': {'A028997_1': {'score': 0.8}}}}
    enriched = linker.enrich("Hoeveel leghennen waren er in Venray en Parijs in 2020?", tables)

    nodes = enriched['80781ned']['nodes']
    assert nodes['GM0984'] == {'score': LINK_SCORE}
    assert nodes['2020JJ00'] == {'score': LINK_SCORE}
    assert nodes['A028997_1'] == {'score': 0.8}  # existing scores untouched
    assert 'Parijs' not in str(nodes)  # unknown names link nothing


def test_prompt_small_level_net_and_linked_codes():
    gen = _Gen()
    gen.node_score_threshold = 0.1

    tables = {'80781ned': {'score': 0.9, 'nodes': {'A028997_1': {'score': 0.8}}}}
    linker = GeoTimeLinker()
    tables = linker.enrich("Hoeveel leghennen waren er in Venray in 2020?", tables)
    llm_prompt = gen._build_prompt("Hoeveel leghennen waren er in Venray in 2020?",
                                  tables, query_type='sql')
    prompt = llm_prompt.user_prompt

    assert '- Dimension: GM0984 (label: "Venray")' in prompt      # linked
    assert '- Dimension: 2020JJ00' in prompt                       # linked
    assert '- Dimension: PV26' in prompt                           # small-level safety net
    assert '- Dimension: GM0344' not in prompt                     # unlinked large-level code
    assert 'GM: Gemeente (605)' in prompt                          # levels annotation intact
