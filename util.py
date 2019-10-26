import sys

from collections import defaultdict


def tree():
    return defaultdict(tree)


# https://docs.djangoproject.com/en/2.2/ref/utils/#module-django.utils.functional
class cached_property:
    """
    Decorator that converts a method with a single self argument into a
    property cached on the instance.

    A cached property can be made out of an existing method:
    (e.g. ``url = cached_property(get_absolute_url)``).
    On Python < 3.6, the optional ``name`` argument must be provided, e.g.
    ``url = cached_property(get_absolute_url, name='url')``.
    """
    name = None

    @staticmethod
    def func(instance):
        raise TypeError(
            'Cannot use cached_property instance without calling '
            '__set_name__() on it.'
        )

    @staticmethod
    def _is_mangled(name):
        return name.startswith('__') and not name.endswith('__')

    def __init__(self, func, name=None):
        if sys.version_info >= (3, 6):
            self.real_func = func
        else:
            func_name = func.__name__
            name = name or func_name
            if not (isinstance(name, str) and name.isidentifier()):
                raise ValueError(
                    "%r can't be used as the name of a cached_property." % name,
                )
            if self._is_mangled(name):
                raise ValueError(
                    'cached_property does not work with mangled methods on '
                    'Python < 3.6 without the appropriate `name` argument. See '
                    'https://docs.djangoproject.com/en/2.2/ref/utils/'
                    '#cached-property-mangled-name',
                )
            self.name = name
            self.func = func
        self.__doc__ = getattr(func, '__doc__')

    def __set_name__(self, owner, name):
        if self.name is None:
            self.name = name
            self.func = self.real_func
        elif name != self.name:
            raise TypeError(
                "Cannot assign the same cached_property to two different names "
                "(%r and %r)." % (self.name, name)
            )

    def __get__(self, instance, cls=None):
        """
        Call the function and put the return value in instance.__dict__ so that
        subsequent attribute access on the instance returns the cached value
        instead of calling cached_property.__get__().
        """
        if instance is None:
            return self
        res = instance.__dict__[self.name] = self.func(instance)
        return res
