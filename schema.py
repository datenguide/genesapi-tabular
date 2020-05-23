import requests

from settings import STORAGE_NAME, SCHEMA_URL, NAMES_URL
from exceptions import ValidationError
from util import cached_property


SCHEMA = requests.get(SCHEMA_URL).json()
NAMES = requests.get(NAMES_URL).json()


class Mixin:
    _child_class = None
    _child_accessor = None
    _child_accessor_func = lambda self: self._data[self._child_accessor].items()  # noqa
    _item_accessor = lambda self, attr: self._data[self._child_accessor][attr]  # noqa

    def __init__(self, data, parent=None):
        self._data = data
        for k, v in data.items():  # FIXME __getattr__ ?
            if not isinstance(v, dict):
                setattr(self, k, v)
        if not hasattr(self, 'name'):
            self.name = data['name']
        if not hasattr(self, 'key'):
            self.key = data.get('key', self.name)
        if parent:
            self.parent = parent
            if hasattr(parent, '_filter_data') and self._child_class:
                self._filter_data = self.parent._filter_data.get(self.key)

    def get_children(self):
        if self._child_class:
            return (self._child_class(v, self) for k, v in self._child_accessor_func() if self.contains(k))
        return iter(())  # empty generator

    def contains(self, item):
        if hasattr(self, '_filter_data') and self._filter_data:
            return item in self._filter_data.keys()  # noqa
        return True

    def __iter__(self):
        return self.get_children()

    def __contains__(self, key):
        for child in self:
            if key == child.key:
                return True
        return False

    def __getitem__(self, item):
        if isinstance(item, tuple):
            _self = self
            for i in item:
                _self = _self[i]
            return _self
        return self._child_class(self._item_accessor(item), self)

    def __repr__(self):
        return f'<{self.__class__.__name__}: {self.key}>'

    def __str__(self):
        # FIXME Internationalization
        return self.title_de


class Value(Mixin):
    pass


class Dimension(Mixin):
    _child_class = Value
    _child_accessor_func = lambda self: ((v['key'], v) for v in self._data['values'])  # noqa
    _item_accessor = lambda self, attr: [v for k, v in self._child_accessor_func() if k == attr][0]  # noqa

    def contains(self, item):
        return item in [v['key'] for v in self._data['values']]


class Measure(Mixin):
    _child_class = Dimension
    _child_accessor = 'dimensions'

    @cached_property
    def region_levels(self):
        return self._data['region_levels']


class Statistic(Mixin):
    _child_class = Measure
    _child_accessor = 'measures'


class Schema(Mixin):
    _child_class = Statistic
    _child_accessor_func = lambda self: self._data.items()  # noqa
    _item_accessor = lambda self, attr: self._data[attr]  # noqa

    def __init__(self, data, filter_data=None):
        self.name = STORAGE_NAME
        self.key = STORAGE_NAME
        if filter_data:
            self._filter_data = filter_data
        super().__init__(data)

    def get_filtered_for_query(self, filter_data):
        return self.__class__(SCHEMA, filter_data)

    def validate(self, cleaned_arguments):
        return all((
            self.validate_query(cleaned_arguments['data']),
            self.validate_levels(cleaned_arguments['data'], cleaned_arguments['level']),
            self.validate_parent(cleaned_arguments['parent']),
            self.validate_region(cleaned_arguments['region']),
        ))

    def validate_query(self, data_query):
        for statistic in data_query:
            if statistic not in self:
                raise ValidationError(f'Statistic `{statistic}` is not present in schema.')
            for measure in data_query[statistic]:
                if measure not in self[statistic]:
                    raise ValidationError(f'Measure `{measure}` is not present in statistic `{statistic}`.')
                for attribute in data_query[statistic][measure]:
                    if attribute not in self[statistic][measure]:
                        raise ValidationError(f'Attribute `{attribute}` is not present in measure `{measure}` of statistic `{statistic}`.')
                    for value in data_query[statistic][measure][attribute]:
                        if value not in self[statistic][measure][attribute]:
                            raise ValidationError(f'Value `{value}` is not present in attribute `{attribute}` of measure `{measure}` in statistic `{statistic}`.')
        return True

    def validate_levels(self, data_query, level):
        if level == 'all':
            return True
        levels = [int(l) for l in level.split(',')]
        for statistic in data_query:
            for measure in data_query[statistic]:
                if set(levels) - set(self[statistic][measure].region_levels):
                    raise ValidationError(f'Level `{level}` is not available in measure `{measure}` of statistic `{statistic}`.')
        return True

    def validate_parent(self, parent):
        if parent is None:
            return True
        if parent not in NAMES.keys():
            raise ValidationError(f'`{parent}` is not a valid parent region key.')
        return True

    def validate_region(self, region):
        if region == 'all':
            return True
        regions = [int(r) for r in region.split(',')]
        if set(regions) - set(NAMES.keys()):
            raise ValidationError(f'`{region}` is not a valid region key.')
        return True


Schema = Schema(SCHEMA)