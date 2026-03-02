import math
from datetime import datetime
from models.generators.geo_dim_extractor import match_region
from models.generators.time_dim_extractor import extract_tc


def test_geo_match():
    assert match_region('Aantal inwoners amsterdam', {'GM0363'}) == {'GM0363'}
    assert (match_region('Hoeveel mensen wonen er in Fryslân boppe?', {'PV21'}) ==
            match_region('Hoeveel mensen wonen er in Friesland boppe?', {'PV21'}) == {'PV21'})
    assert (match_region('Inwonersaantal Hendrik-Ido-Ambacht', {'GM0531'}) ==
            match_region('Inwonersaantal Hendrik-Ido Ambacht', {'GM0531'}) ==
            match_region('Inwonersaantal Hendrik Ido Ambacht', {'GM0531'}) == {'GM0531'})
    assert match_region('Aantal tankstations in Papendrecht', {'GM0531'}) == set()
    assert match_region('How much electricity was supplied to facilities in Cranendonck, Coevorden, Cromstrijen, Culemborg, and Cuijk in 2010?',
                        {'BU01530000', 'BU02000206', 'BU07580006', 'BU18760200', 'GM0216', 'BU07840000', 'WK078401', 'GM1706', 'GM0109', 'GM0611', 'GM1684', 'WK168406', 'BU16840000', 'WK016001', 'WK198208', 'WK196307', 'GM0617', 'GM0286', 'BU16690404', 'BU07580000'},
                        match_largest_regions=True) == {'GM0286', 'GM0109', 'GM1706', 'GM0216', 'GM0617', 'GM0611', 'GM1684'}


def test_year_match():
    assert extract_tc('Aantal faillissementen in 2020 en 2022?', {'2020JJ00', '2021JJ00' ,'2022JJ00'}) == {'2020JJ00', '2022JJ00'}
    assert (extract_tc('Aantal faillissementen in het eerste kwartaal van 2020?', {'2020KW01'}) ==
            extract_tc('Aantal faillissementen in de lente van 2020?', {'2020KW01'}) == {'2020KW01'})
    assert extract_tc('Wat is de verwachte bevolkingsgroei de eerste kwartalen van 2020 tot 2024?',
                      {'2020KW01', '2021KW01', '2022KW01', '2022KW02', '2023KW01', '2024KW01', '2025KW01'}) == {'2020KW01', '2021KW01', '2022KW01', '2023KW01', '2024KW01'}
    assert extract_tc('Hoeveel bibliotheken in 2019', {'2020JJ00'}) == set()


def test_rel_time_match():
    t0 = datetime.now()
    assert extract_tc('Hoeveel faillissementen waren er in het afgelopen jaar?', {f"{t0.year - 1}JJ00"}) == {f"{t0.year - 1}JJ00"}
    assert extract_tc('Hoeveel faillissementen waren er deze maand?', {f"{t0.year}MM{t0.strftime('%m')}"}) == {f"{t0.year}MM{t0.strftime('%m')}"}
    assert extract_tc('Wat is de verwachte bevolkingsgroei dit kwartaal?', {f"{t0.year}KW0{math.ceil(t0.month / 3)}"}) == {f"{t0.year}KW0{math.ceil(t0.month / 3)}"}
    assert extract_tc('Hoeveel faillissementen waren er afgelopen?') == set()
