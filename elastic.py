from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

from schema import Schema
from settings import ELASTIC_HOST, ELASTIC_INDEX, ELASTIC_AUTH
from util import cached_property


def get_term_filter(field, terms):
    if isinstance(terms, list):
        if len(terms) > 1:
            return {'terms': {field: terms}}
        terms = terms[0]
    return {'term': {field: terms}}


class ElasticQuery:
    def __init__(self, data):
        self.data = data
        self.client = Elasticsearch([ELASTIC_HOST], http_auth=ELASTIC_AUTH)

    def execute(self):
        return scan(self.client, index=[ELASTIC_INDEX], query=self.body)

    @cached_property
    def result(self):
        return self.execute()

    @cached_property
    def facts(self):
        for hit in self.result:
            yield hit['_source']

    @cached_property
    def body(self):
        return {
            'query': {
                'constant_score': {
                    'filter': {
                        'bool': {
                            'must': [f for f in self.get_filters() if f] + [{
                                'bool': {
                                    'should': [p for p in self.get_paths()]
                                }
                            }],
                            'must_not': [p for p in self.get_exclude_paths()]
                        }
                    }
                }
            }
        }

    def get_filters(self):
        return self.get_regions(), self.get_statistics(), self.get_time(), self.get_region_level(), self.get_parent()

    def get_regions(self):
        data = self.data['region']
        if data == 'all':
            return
        return get_term_filter('region_id', data)

    def get_region_level(self):
        if self.data['region'] == 'all':
            return get_term_filter('region_level', self.data['level'])

    def get_parent(self):
        data = self.data['parent']
        if data:
            return {'prefix': {'region_id': data}}

    def get_statistics(self):
        return get_term_filter('statistic', list(self.data['data'].keys()))

    def get_time(self):
        data = self.data['time']
        if data in ('all', 'last'):
            return
        if ':' in data:  # years range
            start, end = data.split(':')
            f = {}
            if start:
                f['gte'] = int(start)
            if end:
                f['lte'] = int(end)
            return {'range': {'year': f}}
        return get_term_filter('year', data)

    def get_paths(self):
        for stat in self.data['data'].values():
            for meas, dims in stat.items():
                if not dims:
                    yield {'exists': {'field': meas}}
                else:
                    for dim, vals in dims.items():
                        if not vals:
                            yield {'exists': {'field': 'path.%s.%s' % (meas, dim)}}
                        elif len(vals) == 1:
                            yield {'term': {'path.%s.%s.keyword' % (meas, dim): vals[0]}}
                        else:
                            yield {'terms': {'path.%s.%s.keyword' % (meas, dim): vals}}

    def get_exclude_paths(self):
        for stat_id, stat in self.data['data'].items():
            for meas, dims in stat.items():
                for dim in set(d.key for d in Schema[stat_id][meas]) - set(dims.keys()):
                    yield {'exists': {'field': 'path.%s.%s' % (meas, dim)}}
