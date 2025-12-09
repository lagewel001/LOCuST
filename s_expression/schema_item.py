from __future__ import annotations

import re
from abc import ABC
from rdflib import Namespace, URIRef
from rdflib.term import Node
from typing import Union

import config


class SchemaItem(ABC):
    """SchemaItem acts as an Atom in the S-expressions"""
    def __init__(self, identifier: str, rdf_ns: Namespace):
        self.rdf_ns: Namespace = Namespace(rdf_ns)
        self.identifier: str = identifier

    @property
    def uri(self) -> URIRef:
        return self.rdf_ns.term(self.identifier)

    def __eq__(self, other: Union[SchemaItem, str]):
        """
            Compare URIs of schema items to determine equality,
            as different nodes can in theory have identical identifiers.
        """
        if isinstance(other, SchemaItem):
            return self.uri == other.uri
        elif isinstance(other, str):
            return self.uri == URIRef(other)

        raise ValueError(f"Cannot compare SchemaItem with an object of type {type(other)}.")

    def __ne__(self, other: Union[SchemaItem, str]):
        return self != other

    def __repr__(self):
        return str(self)

    def __str__(self):
        return self.identifier

    def __hash__(self):
        return id(self)


class Table(SchemaItem):
    rdf_ns: Namespace
    if config.GRAPH_DB_REPO == 'cbs-nl':
        rdf_ns = Namespace("https://datasets.cbs.nl/odata/v1/CBS/")
    elif config.GRAPH_DB_REPO == 'cbs-en':
        rdf_ns = Namespace("https://opendata.cbs.nl/ODataApi/OData/")
    else:
        raise ValueError(f"Invalid graph repository name: {config.GRAPH_DB_REPO}. "
                         f"Choose between ['cbs-nl', 'cbs-en']")

    def __init__(self, identifier: str):
        super().__init__(identifier, self.rdf_ns)


class Measure(SchemaItem):
    rdf_ns = Namespace("https://vocabs.cbs.nl/def/onderwerp/")

    def __init__(self, identifier: str):
        super().__init__(identifier, self.rdf_ns)


class Dimension(SchemaItem):
    rdf_ns = Namespace("https://vocabs.cbs.nl/def/dimensie/")

    def __init__(self, identifier: str):
        super().__init__(identifier, self.rdf_ns)


def uri_to_code(uri: Union[Node, URIRef, SchemaItem, str]) -> str:
    """Helper function to translate a schema URI to its base identifier."""
    return re.split(fr"{Table.rdf_ns}|{Measure.rdf_ns}|{Dimension.rdf_ns}", str(uri))[-1]
