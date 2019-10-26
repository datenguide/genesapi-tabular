import requests

from settings import STORAGE_NAME, SCHEMA_URL, NAMES_URL


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
                self._filter_data = self.parent._filter_data[self.key]

    def get_children(self):
        if self._child_class:
            return (self._child_class(v, self) for k, v in self._child_accessor_func() if self.contains(k))
        return iter(())  # empty generator

    def contains(self, item):
        if hasattr(self, '_filter_data'):
            return item in self._filter_data.keys()  # noqa
        return True

    def __iter__(self):
        return self.get_children()

    def __contains__(self, key):
        for child in self:
            if key == child.key:
                return True
        return False

    def __getitem__(self, attr):
        return self._child_class(self._item_accessor(attr), self)

    def __repr__(self):
        return '<%s: %s>' % (self.__class__.__name__, self.key)


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


Schema = Schema(SCHEMA)