import json
import re
from hashlib import sha1
from urllib.parse import parse_qs

from schema import Schema
from util import cached_property, tree


class ValidationError(Exception):
    pass


def validate(condition, errmsg):
    if not condition:
        raise ValidationError(errmsg)
    return True


class Argument:
    def __init__(self, name, default, choices=[], regex=[], multi=True, single=True):
        self.name = name
        self.default = default
        self.choices = choices
        self.regex = regex
        self.multi = multi
        self.single = single

    def clean(self, data):
        if self.name not in data or data[self.name][0] == self.default:
            return self.default
        if self.single:
            validate(len(data[self.name]) == 1, 'param `%s` can only be used once in query string')
            if not self.multi:
                validate(',' not in data[self.name][0], 'param `%s` can not be comma-separated')
            for value in data[self.name][0].split(','):
                if self.choices and not self.regex:
                    validate(
                        value in self.choices, '`%s` is not in allowed choices for param `%s`' %
                        (value, self.name))
                if self.regex:
                    try:
                        validate(any(re.match(r, value) for r in self.regex),
                                 '`%s` is not valid for param `%s`' % (value, self.name))
                    except ValidationError as e:
                        if self.choices:
                            validate(value in self.choices,
                                     '`%s` is not in allowed choices for param `%s`' % (value, self.name))
                        else:
                            raise e
        val = data[self.name]
        if self.single and len(val) == 1:
            val = val[0]
        if self.multi:
            val = val.split(',')
            if len(val) == 1:
                val = val[0]
        return val


class DataArgument(Argument):
    def clean(self, data):
        try:  # FIXME
            data = super().clean(data)
        except ValidationError as e:
            if 'comma-separated' in str(e):
                pass
            else:
                raise e

        paths = tree()

        # FIXME implementation
        for statistic in data:
            statistic, measure = statistic.split(':', 1)
            if '(' in measure:
                dimensions = re.search(r'\(([A-Z0-9_,:|]+)\)', measure)
                measure = measure.split('(')[0]
                if dimensions:
                    for dimension in dimensions.group(1).split(','):
                        if ':' in dimension:
                            dimension, value = dimension.split(':')
                            paths[statistic][measure][dimension] = value.split('|')
                        else:
                            paths[statistic][measure][dimension]
            else:
                paths[statistic][measure]

        # sort stuff for unique identification
        paths = {skey: {mkey: {d: sorted(v) for d, v in sorted(m.items())}
                        for mkey, m in sorted(s.items())} for skey, s in sorted(paths.items())}
        return paths


NUM_RE = r'^\d+'


class Query:
    # arg_name: (default, choices / validation regex, multi comma-seperated, single [allowed only once in qs])
    region = Argument('region', 'all', regex=[NUM_RE], choices=['DG'])
    level = Argument('level', '1', choices=['0', '1', '2', '3', '4', 'all'])
    parent = Argument('parent', None, regex=[NUM_RE])
    time = Argument('time', 'latest', choices=['all'], regex=[
        r'^\d{4}$',         # 2000
        r'^\d{4}:\d{4}',    # 2000:2010
        r'^:\d{4}$',        # :2010
        r'^\d{4}:$'         # 2010:
    ])
    dformat = Argument('dformat', 'year', choices=['date'])
    labels = Argument('labels', 'id', choices=['name', 'both'])
    layout = Argument('layout', 'long', choices=['region', 'time'])
    format = Argument('format', 'csv', choices=['tsv', 'json'])
    delimiter = Argument('delimiter', ',', choices=[';'])
    sort = Argument('sort', 'time', choices=['region', 'value', 'measure'])  # data sorting
    # not implemented:
    # order = Argument('order', 'time,region,value,keys',
    #                  choices=['time', 'region', 'value', 'keys', 'meta'])  # column order
    data = DataArgument('data', {}, regex=[], multi=False, single=False)  # FIXME regex

    def __init__(self, data):
        if isinstance(data, str):  # urlquery
            self.urlquery = data  # FIXME create urlquery from dict when we support query via dict
            data = parse_qs(data)
            invalid = set(data.keys()) - set([a[0] for a in self.arguments])
            if len(invalid):
                raise ValidationError('unknown attributes: %s' % ', '.join(invalid))

        self._data = data

    def __getattr__(self, attr):
        return self.cleaned_data.get(attr, self.defaults.get(attr))

    def clean(self):
        # TODO more logic to check if query is valid against actual schema
        cleaned_arguments = {key: arg.clean(self._data) for key, arg in self.arguments}
        if Schema.validate_query(cleaned_arguments['data']):
            return cleaned_arguments

    @cached_property
    def cleaned_data(self):
        return dict(sorted(self.clean().items()))

    @cached_property
    def data_definition(self):
        return {k: v for k, v in self.cleaned_data.items()
                if k in ('region', 'level', 'parent', 'time', 'data', 'dformat')}

    @cached_property
    def key(self):
        """unique identifier for exactly this table with all given specs about format etc"""
        return sha1(json.dumps(self.cleaned_data).encode()).hexdigest()

    @cached_property
    def data_key(self):
        """unique identifier for the exact data used for this table regardless of format/transform options"""
        return sha1(json.dumps(self.data_definition).encode()).hexdigest()

    @cached_property
    def arguments(self):
        return [(key, arg) for key, arg in self.__class__.__dict__.items() if isinstance(arg, Argument)]

    @cached_property
    def schema(self):
        return Schema.get_filtered_for_query(self.cleaned_data['data'])
