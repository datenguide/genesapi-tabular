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
                            'must': [f for f in self.get_meta_filters() if f] + [{
                                'bool': {'should': [s for s in self.get_statistics()]}
                            }]
                        }
                    }
                }
            }
        }

    def get_meta_filters(self):
        return self.get_regions(), self.get_time(), self.get_region_level(), self.get_parent()

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

    def get_statistics(self):
        for statistic, measures in self.data['data'].items():
            yield {'bool': {'must': [
                {'term': {'statistic': statistic}},
                {'bool': {'should': [self.get_measure_filter(measure, dimensions, Schema[statistic])
                                     for measure, dimensions in measures.items()]}}
            ]}}

    def get_measure_filter(self, measure, dimensions, schema):
        other_dimensions = set(d.key for d in schema[measure]) - set(dimensions.keys())
        if not dimensions:
            return {'bool': {
                'must': {'exists': {'field': measure}},
                'must_not': [{'exists': {'field': d}} for d in other_dimensions]
            }}
        else:
            not_values = {}
            for dimension, values in dimensions.items():
                if values:
                    not_values['path.%s.%s.keyword' % (measure, dimension)] = list(
                        set(v.key for v in schema[measure][dimension]) - set(values))
            return {
                'bool': {
                    'should': [self.get_dimension_filter(measure, dimension, values)
                               for dimension, values in dimensions.items()],
                    # eclude other dimensions and values
                    'must_not': [{'exists': {'field': 'path.%s.%s' % (measure, other_dimension)}}
                                 for other_dimension in other_dimensions] + [
                                     {'terms': {k: v}} for k, v in not_values.items()]
                }
            }

    def get_dimension_filter(self, measure, dimension, values):
        field = 'path.%s.%s' % (measure, dimension)
        if not values:
            return {'exists': {'field': field}}
        return {'terms': {'%s.keyword' % field: values}}
