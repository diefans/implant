"""Scope is the payload carrier."""

from collections.abc import MutableMapping
import os

import pytest


class Scope(MutableMapping):

    """A Scope is basically a linked py::obj::``dict`` with a lookup to a parent on missing keys."""

    def __init__(self, *args, **kwargs):
        self.parent = None
        self.data = dict(*args, **kwargs)
        super(Scope, self).__init__()

    def create_child(self):
        # XXX TODO should we copy the whole scope to avoid shared parent conflicts???
        child = Scope()
        child.parent = self
        return child

    def __getitem__(self, key):
        try:
            return self.data[key]

        except KeyError:
            if self.parent is not None:
                return self.parent[key]
            else:
                raise

    def __delitem__(self, key):
        """Deletions are only allowed for the current scope."""
        del self.data[key]

    def __setitem__(self, key, value):
        """This will override a parents item only for this scope."""
        self.data[key] = value

    def __len__(self):
        keys = self.data.keys()

        if self.parent is not None:
            keys = keys | set(self.parent.keys())

        return len(keys)

    def __iter__(self):
        keys = self.data.keys()

        if self.parent is not None:
            keys = keys | set(self.parent.keys())

        return iter(keys)

    def eval(self, expression):
        # TODO mean to plugin globals
        result = eval(expression, {
            'env': os.environ.get
        }, self)
        return result


@pytest.fixture
def parent():
    from debellator.evolve import scope
    s = scope.Scope()

    return s


@pytest.fixture
def child(parent):
    c = parent.create_child()
    return c


@pytest.fixture
def mocked_parent(parent):
    parent.update({
        'foo': 'bar',
        'bar': 'foo'
    })

    return parent


@pytest.fixture
def mocked_child(child):
    child.update({
        'bar': 'baz',
        'xyz': 123
    })

    return child


def test_scope_child():
    from debellator.evolve import scope

    s = scope.Scope()
    c = s.create_child()

    assert id(c.parent) == id(s)
    assert id(s) != id(c)


def test_scope_getitem(mocked_child, mocked_parent):
    values = [mocked_child[key] for key in ('foo', 'bar', 'xyz')]

    assert values == ['bar', 'baz', 123]


def test_scope_items(mocked_child, mocked_parent):
    items = set(mocked_child.items())

    assert items == {
        ('foo', 'bar'),
        ('bar', 'baz'),
        ('xyz', 123)
    }


def test_scope_keys(mocked_child, mocked_parent):
    keys = set(mocked_child.keys())
    assert keys == {'foo', 'bar', 'xyz'}


def test_scope_dict(mocked_child, mocked_parent):
    d = dict(mocked_child)

    assert d == {
        'foo': 'bar',
        'bar': 'baz',
        'xyz': 123
    }
