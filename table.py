import json
import pandas as pd

from schema import NAMES
from util import cached_property


META_FIELDS = ['region_id', 'statistic']

dtypes = {
    'region_id': str,
    'year': str,
    'statistic': str
}


def typed(df):
    for col, t in dtypes.items():
        if col in df:
            df[col] = df[col].map(t)
    return df


class Table:
    def __init__(self, facts, query, from_base=False, cubes=[]):
        self._df = typed(pd.DataFrame(facts))
        self._from_base = from_base
        self.query = query
        self.measure_keys = [m.key for s in query.schema for m in s]
        self.dimension_keys = [d.key for s in query.schema for m in s for d in m]
        self.schema = query.schema
        self.cubes = cubes or list(self._df['cube'].unique())
        for k, v in query.cleaned_data.items():
            setattr(self, k, v)

    @classmethod
    def from_base(cls, base_data, query):
        return cls(json.loads(base_data['data']), query, True, cubes=base_data['cubes'])

    @cached_property
    def df(self):
        self.process()
        return self._df

    @cached_property
    def formats(self):
        return {
            'csv': self.to_csv(),
            'json': self.to_json(),
            'tsv': self.to_csv(delimiter='\t')
        }

    @cached_property
    def mimetype(self):
        if self.format == 'json':
            return 'application/json'
        return 'text/plain'

    def rendered(self):
        return self.formats[self.format]

    def to_json(self):
        return self.df.to_json(orient='table')

    def to_csv(self, delimiter=None):
        return self.df.fillna('').to_csv(index=not self.layout == 'long', sep=delimiter or self.delimiter)

    def process(self):
        if not self._from_base:
            self.clean_values()
            self.clean_columns()
            self.make_long()
        self.transform()
        self.sort_values()
        self.order_columns()
        self.labelize()
        self.clean_types()

    def make_long(self):
        """bring always into long format before other transformings"""
        df = self._df
        dfs = []
        for statistic in self.schema:
            df_s = df[df['statistic'] == statistic.key]
            df_s_ = []
            for measure in statistic:
                df_m = df_s[['region_id', self.dformat, 'statistic', measure.key]
                            + [dimension.key for dimension in measure]]
                df_m = df_m.dropna(subset=[measure.key])
                df_m = df_m.rename(columns={**{measure.key: 'value'},
                                            **{dimension.key: '%s.%s' % (measure.key, dimension.key)
                                               for dimension in measure}})
                df_m['measure'] = measure.key
                df_s_.append(df_m)
            dfs.append(pd.concat(df_s_))
        self._df = self._long_df = pd.concat(dfs)

    def transform(self):
        if self.layout == 'long':
            return  # already transformed via `self.make_long`
        df = self._df
        if len(self.measure_keys) == 1 and not len(self.dimension_keys):
            if self.layout == 'time':
                df = df.pivot(index=self.dformat, columns='region_id', values='value')
            if self.layout == 'region':
                df = df.pivot(index='region_id', columns=self.dformat, values='value')
            self._df = df
            return
        index_cols = sorted([c for c in self._df.columns if c not in self.meta_fields + ['value', 'measure']])
        if self.layout == 'time':
            index_cols = [self.dformat, 'region_id', 'measure'] + index_cols
        if self.layout == 'region':
            index_cols = ['region_id', self.dformat, 'measure'] + index_cols
        df.sort_values(index_cols, inplace=True)
        df.index = [df[c] for c in index_cols]
        df = df['value']
        for i in range(len(index_cols) - 1):
            df = df.unstack()
        df = df.dropna(axis=1, how='all')
        df.columns = df.columns.map(lambda x: '.'.join(reversed([i for i in x if not pd.isna(i)])))
        self._df = df

    def clean_values(self):
        for measure in self.measure_keys:
            if measure in self._df.columns:
                self._df[measure] = self._df[measure].map(lambda x: x['value'] if isinstance(x, dict) else x)

    def clean_columns(self):
        columns = [c for c in set(self.meta_fields + self.measure_keys + self.dimension_keys) if c in self._df]
        self._df = self._df[columns]

    def labelize(self):
        # FIXME internationalization
        if self.labels == 'name':
            if 'region_id' in self._df:
                self._df['region_id'] = self._df['region_id'].map(lambda x: NAMES.get(x, x))
            elif self._df.index.name == 'region_id':
                self._df.index = self._df.index.map(lambda x: NAMES.get(x))
            if 'statistic' in self._df:
                self._df['statistic'] = self._df['statistic'].map(lambda x: self.schema[x].title_de)
            if 'measure' in self._df:
                measure_names = {m.key: m.title_de for s in self.schema for m in s}
                self._df['measure'] = self._df['measure'].map(lambda x: measure_names[x])

            # FIXME implementation?
            columns = {c: c for c in self._df.columns if c.isupper()}
            for statistic in self.schema:
                for measure in statistic:
                    if measure.key in columns:
                        columns[measure.key] = measure.title_de
                    for dimension in measure:
                        col = '%s.%s' % (measure.key, dimension.key)
                        if col in columns:
                            self._df[col] = self._df[col].map(lambda x: dimension[x].title_de if not pd.isna(x) else x)
                            columns[col] = '%s: %s' % (measure.title_de, dimension.title_de)
            meta_fields = {
                'region_id': 'Region',
                'value': 'Wert',
                'statistic': 'Statistik',
                'measure': 'Merkmal',
                'year': 'Jahr',
                'date': 'Datum'
            }
            self._df.rename(columns={**columns, **meta_fields}, inplace=True)
            if self._df.index.name in meta_fields:
                self._df.index.name = meta_fields[self._df.index.name]

    def sort_values(self):
        # ?sort=
        main_col = {
            'time': self.dformat,
            'region': 'region_id',
            'measure': 'measure',
            'value': 'value'
        }[self.sort]

        # ?layout=
        column_order = {
            'long': ['region_id', self.dformat, 'measure'],
            'region': ['region_id', self.dformat, 'measure'],
            'time': [self.dformat, 'region_id', 'measure']
        }[self.layout]

        columns = [c for c in [main_col] + list(set(column_order) - set(main_col)) if c in self._df.columns]
        other_columns = sorted(set(self._df.columns) - set(columns))
        self._df.sort_values(columns + other_columns, inplace=True)
        if self._df.index.name in column_order + [main_col]:
            self._df.sort_index(inplace=True)

    def order_columns(self):
        layouts = {
            'long': ['region_id', self.dformat, 'measure', 'value'],
            'region': ['region_id', self.dformat, 'measure'],
            'time': [self.dformat, 'region_id', 'measure']
        }
        columns = [c for c in layouts[self.layout] if c in self._df.columns]
        other_columns = sorted(set(self._df.columns) - set(columns))
        self._df = self._df[columns + other_columns]

    def clean_types(self):
        def clean(value):
            if pd.isna(value):
                return value
            try:
                if int(value) == value:
                    return int(value)
                return value
            except ValueError:
                return value
        self._df = self._df.applymap(clean)

    @cached_property
    def meta_fields(self):
        return META_FIELDS + [self.dformat]

    def serialize(self):
        return {
            'content': self.rendered(),
            'mimetype': self.mimetype,
            'cubes': self.cubes,
            'definition': self.query.cleaned_data,
            'kind': 'concrete'
        }

    def serialize_base(self):
        return {
            'data': self._long_df.to_json(orient='records'),
            'cubes': self.cubes,
            'definition': self.query.data_definition,
            'kind': 'base'
        }
