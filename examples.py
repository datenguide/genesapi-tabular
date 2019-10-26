"""
render random examples from the elastic cache
"""


import csv
from flask import request

from cache import Cache
from schema import Schema
from settings import GENESAPI_TABULAR_STATIC


client = Cache.backend.client
index = Cache.backend.index


def serialize_example(id_, data):
    schema = Schema.get_filtered_for_query(data['definition']['data'])

    def get_rows():
        for i, row in enumerate(data['content'].split('\n')):
            yield row
            if i > 10:
                return

    rows = [r for r in get_rows()]
    delimiter = data['definition']['delimiter']

    return {
        'id': id_,
        'title': ' / '.join(s.title_de for s in schema),
        'subtitle': ', '.join(m.title_de for s in schema for m in s),
        'schema': schema,
        'url': '%s?%s' % (request.host_url, data['urlquery']),
        'static_url': '%s/?%s' % (GENESAPI_TABULAR_STATIC, data['urlquery']),
        'table': {
            'header': [r for r in csv.reader(rows[:1], delimiter=delimiter)][0],
            'rows': csv.reader(rows[1:], delimiter=delimiter)
        },
        'params': ((k, v) for k, v in data['definition'].items() if k != 'data' and v)
    }


def get_example(id_):
    example = client.get(index=index, id=id_)
    return [serialize_example(id_, example['_source'])]


def get_examples():
    # get a randomized list of 10 examples
    query = {
        'bool': {
            'filter': [
                {'term': {'kind': 'concrete'}},
                {'term': {'definition.format': 'csv'}}
            ]
        }
    }
    sort = {
        '_script': {
            'script': 'Math.random()',
            'type': 'number',
            'order': 'asc'
        }
    }
    examples = client.search(index=index, body={'query': query, 'sort': sort})
    for example in examples['hits']['hits']:
        yield serialize_example(example['_id'], example['_source'])
