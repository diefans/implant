import pytest


def test_parameter(core):
    class Foo(core.Command):
        foo = core.Parameter(default='bar')
        bar = core.Parameter()

        def local(self, context):
            pass

        def remote(self, context):
            pass

    foo = Foo({'bar': 'baz'})

    assert foo.bar == 'baz'
    assert foo.foo == 'bar'
    assert dict(foo) == {'bar': 'baz', 'foo': 'bar'}


def test_parameter_attrib_error(core):
    class Foo(core.Command):
        bar = core.Parameter()

        def local(self, context):
            pass

        def remote(self, context):
            pass

    foo = Foo()
    with pytest.raises(AttributeError):
        foo.bar     # pylint: disable=W0104
