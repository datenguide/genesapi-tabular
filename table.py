import base64
import pandas as pd
import pickle

from schema import NAMES
from util import cached_property


META_FIELDS = ['region_id', 'statistic']
FIELD_LABELS = {
    'region_id': 'ID_Region',
    'region_name': 'Region',
    'value': 'Wert',
    'statistic': 'Statistik',
    'measure': 'Merkmal',
    'year': 'Jahr',
    'date': 'Datum'
}

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
        if from_base:
            self._df = facts
            self._from_base = True
        else:
            self._df = typed(pd.DataFrame(facts))
            self._from_base = False
        self._from_base = from_base
        self._is_empty = not len(self._df)
        self.query = query
        self.measure_keys = [m.key for s in query.schema for m in s]
        self.dimension_keys = [d.key for s in query.schema for m in s for d in m]
        self.schema = query.schema
        if not self._is_empty:
            self.cubes = cubes or list(self._df['cube'].unique())
        for k, v in query.cleaned_data.items():
            setattr(self, k, v)

    @classmethod
    def from_base(cls, base_data, query):
        df = pickle.loads(base64.b64decode(base_data['blob']))
        return cls(df, query, True, cubes=base_data['cubes'])

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
        if self._is_empty:
            return
        if not self._from_base:
            self.clean_values()
            self.clean_columns()
            self.make_long()
        self.transform()
        self.clean_types()
        self.labelize()
        self.sort_values()
        self.order_columns()

    def make_long(self):
        """bring always into long format before other transformings"""
        df = self._df
        dfs = []
        for statistic in self.schema:
            df_s = df[df['statistic'] == statistic.key]
            df_s_ = []
            for measure in statistic:
                dimensions = list(set(d.key for d in measure) & set(df_s.columns))  # FIXME validate schema / levels
                df_m = df_s[['region_id', self.dformat, 'statistic', measure.key] + dimensions]
                df_m = df_m.dropna(subset=[measure.key])
                df_m = df_m.rename(columns={**{measure.key: 'value'},
                                            **{dimension: (statistic.key, measure.key, dimension)
                                               for dimension in dimensions}})
                df_m['measure'] = [(statistic.key, measure.key)] * len(df_m)
                df_s_.append(df_m)
            dfs.append(pd.concat(df_s_))
        self._df = self._long_df = pd.concat(dfs).dropna(axis=1, how='all')

    def transform(self):
        if self.layout == 'long':
            if not self._from_base:
                self._df['measure'] = self._df['measure'].map(lambda x: x[1])
            return  # already transformed via `self.make_long`
        dfs = []
        for measure in self._df['measure'].unique():
            df = self._df[self._df['measure'] == measure]
            df = df.dropna(axis=1, how='all')
            index_cols = sorted([c for c in df.columns if c not in self.meta_fields + ['value', 'measure']])
            if self.layout == 'time':
                index_cols = [self.dformat, 'region_id', 'measure'] + index_cols
            if self.layout == 'region':
                index_cols = ['region_id', self.dformat, 'measure'] + index_cols
            df.sort_values(index_cols, inplace=True)
            df.index = [df[c].map(lambda x: (c, x)) for c in index_cols]
            df = df['value']
            for i in range(len(index_cols) - 1):
                df = df.unstack()
            dfs.append(df)
        self._df = pd.concat(dfs, axis=1).dropna(axis=1, how='all')

    def clean_values(self):
        for measure in self.measure_keys:
            if measure in self._df.columns:
                self._df[measure] = self._df[measure].map(lambda x: x['value'] if isinstance(x, dict) else x)

    def clean_columns(self):
        columns = [c for c in set(self.meta_fields + self.measure_keys + self.dimension_keys) if c in self._df]
        self._df = self._df[columns]

    def labelize(self):
        # FIXME internationalization

        self._df.index = self._df.index.map(lambda x: x[1] if isinstance(x, tuple) else x)

        # always add `region_name`
        if 'region_id' in self._df:
            self._df['region_name'] = self._df['region_id'].map(lambda x: NAMES.get(x, x))
        elif self._df.index.name == 'region_id':
            self._df['region_name'] = self._df.index.map(lambda x: NAMES.get(x, x))

        # index names
        if self.layout == 'time':
            self._df.index.name = self._labels(self.dformat)[0]
        if self.layout == 'region':
            self._df.index.name = self._labels('region_id')[0]

        # labels inside df
        if self.labels == 'name':
            if 'statistic' in self._df:
                self._df['statistic'] = self._df['statistic'].map(lambda x: self.schema[x]).map(str)
            if 'measure' in self._df:
                measures = {m.key: m for s in self.schema for m in s}
                self._df['measure'] = self._df['measure'].map(lambda x: measures[x]).map(str)
            for column in self._df:
                if column[0] in self.schema:
                    dimension = self.schema[column]
                    self._df[column] = self._df[column].map(lambda x: str(dimension[x]) if not pd.isna(x) else x)
            # index name
            if self._df.index.name in FIELD_LABELS:
                self._df.index.name = FIELD_LABELS[self._df.index.name]

        # column labels
        if self.layout == 'long':
            def get_column_name(column):
                if isinstance(column, tuple):
                    statistic, measure, dimension = column
                    if self.labels == 'id':
                        return f'{statistic}:{measure}({dimension})'
                    measure = self.schema[statistic][measure]
                    if self.labels == 'name':
                        return f'{measure}: {measure[dimension]}'
                if self.labels == 'name':
                    return FIELD_LABELS[column]
                return column
            self._df.columns = self._df.columns.map(get_column_name)
            return

        not_layout_col = {
            'time': 'region_id',
            'region': self.dformat
        }[self.layout]

        def get_column_name(column):
            if len(column) == 2:
                if column[0] in FIELD_LABELS.keys():
                    if self.labels == 'name':
                        return FIELD_LABELS[column[0]]
                    return column[0]
                column = dict(column)
                if self.labels == 'id':
                    return f"{'.'.join(column['measure'])}-{not_layout_col}:{column[not_layout_col]}"
                if self.labels == 'name':
                    statistic, measure = column['measure']
                    s = column[not_layout_col]
                    suffix = NAMES.get(s, s) if not_layout_col == 'region_id' else s
                    return f"{self.schema[statistic, measure]} {suffix}"
            statistic = None
            measure = None
            dimensions = []
            suffix = None
            for keys, value in column:
                if isinstance(keys, tuple):
                    # it is not possible to have different statistics and measures in 1 column
                    statistic, measure, dimension = keys
                    if not pd.isna(value):
                        dimensions.append((dimension, value))
                elif keys != 'measure':
                    suffix = value  # it is not possible to have > 1 suffixes here
            if self.labels == 'id':
                return f"{statistic}:{measure}({','.join(':'.join(i) for i in dimensions)})-{not_layout_col}:{suffix}"
            if self.labels == 'name':
                measure = self.schema[statistic][measure]
                suffix = NAMES.get(suffix, suffix) if not_layout_col == 'region_id' else suffix
                return f"{measure}: {', '.join(str(measure[k][v]) for k, v in dimensions)}, {suffix}"

        self._df.columns = self._df.columns.map(get_column_name)

    def sort_values(self):
        # ?sort=
        main_col = self._labels({
            'time': self.dformat,
            'region': 'region_id',
            'measure': 'measure',
            'value': 'value'
        }[self.sort])

        # ?layout=
        column_order = self._labels(*{
            'long': ['region_id', self.dformat, 'measure'],
            'region': ['region_id', self.dformat, 'measure'],
            'time': [self.dformat, 'region_id', 'measure']
        }[self.layout])

        columns = [c for c in main_col + list(set(column_order) - set(main_col)) if c in self._df.columns]
        other_columns = sorted(set(self._df.columns) - set(columns))
        self._df.sort_values(columns + other_columns, inplace=True)
        if self._df.index.name in column_order + main_col:
            self._df.sort_index(inplace=True)

    def order_columns(self):
        layouts = {
            'long': ['region_id', 'region_name', self.dformat, 'measure', 'value'],
            'region': ['region_id', 'region_name', self.dformat, 'measure'],
            'time': [self.dformat, 'region_id', 'region_name', 'measure']
        }
        columns = [c for c in self._labels(*layouts[self.layout]) if c in self._df.columns]
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
            'urlquery': self.query.urlquery,
            'kind': 'concrete'
        }

    def serialize_base(self):
        return {
            'blob': base64.b64encode(pickle.dumps(self._long_df)).decode(),
            'cubes': self.cubes,
            'definition': self.query.data_definition,
            'kind': 'base'
        }

    def _labels(self, *fields):
        return [self.fields[f] for f in fields]

    @cached_property
    def fields(self):
        return {k: k if self.labels == 'id' else label for k, label in FIELD_LABELS.items()}
