"""
    This module contains code for rule-based extraction/pre-selection of GeoDimensions from a question.
"""
import pandas as pd
from typing import Set

import config

GEOLOCATIONS = pd.read_csv(f'{config.PATH_DIR_DATA}/geolocations.csv')


def simple_code_disambiguation(query: str, candidates: Set[str], match_largest_regions: bool = False) -> Set[str]:
    """
        Disambiguate GeoDimension codes based on a couple of keyword hints.
        If relevant hints are found in the query, the most likely candidates are filtered and returned.
    """
    hints = {
        # Dutch
        'corop': 'CR', 'omgeving': 'CR', 'provincie': 'PV', 'gemeente': 'GM', 'stad': 'GM', 'plaats': 'GM',
        # English
        'area': 'CR', 'province': 'PV', 'municipality': 'GM', 'place': 'GM', 'city': 'GM', 'town': 'GM'
    }
    candidates_using_hints = set()
    for key in hints.keys():
        if key in query.split():
            for c in candidates:
                if hints[key] in c:
                    candidates_using_hints.add(c)

    if len(candidates_using_hints) == 0 and match_largest_regions:
        # Default to preferred ordering if no disambiguation could be made using the hints
        order = ['NL', 'PV', 'GM', 'CR', 'WK', 'BU']
        for key in order:
            for c in candidates:
                if key in c:
                    candidates_using_hints.add(c)

            if len(candidates_using_hints) > 0:
                return candidates_using_hints

    return candidates_using_hints or candidates


def match_region(query: str, available_geo_constraints: Set[str] = None, match_largest_regions: bool = False) -> Set[str]:
    """
        Rule-based matching of geo references in a query.

        :param query: natural language query containing possible geo references
        :param available_geo_constraints: set of available GeoDimension codes
        :param match_largest_regions: disambiguate regions based on larger regions, and only pick the 
                                      largest regions available in the candidates to return
        :return: List of available, most likely GeoDimension codes referenced by in the query
    """
    query = query.lower().strip().replace('-', ' ')
    mask = [l in query for l in GEOLOCATIONS.label.values]

    extracted_gc = set()
    region_matches = GEOLOCATIONS[mask]
    if not region_matches.empty:
        region_codes = set(region_matches.id.unique()) & available_geo_constraints
        if len(region_codes) == 1:
            return region_codes
        extracted_gc = simple_code_disambiguation(query, region_codes, match_largest_regions)

    return extracted_gc
