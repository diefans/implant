import pytest


@pytest.mark.asyncio
@pytest.mark.usefixtures('mocked_meta_path')
async def test_remote_descriptor():
    import importlib
    from debellator import core

    class Foo(core.Command):
        foobar = core.Parameter(description='foobar')
        remote = core.CommandRemote('foobar_module.Foobar')

    module = importlib.import_module('foobar_module')
    Foo.remote.set_remote_class(module)

    foo = Foo()
    foo.foobar = 'foobar'

    assert await foo.remote(None) == 'foobar'


def test_parameter():
    from debellator import core

    class Foo(core.Command):
        foo = core.Parameter(default='bar')
        bar = core.Parameter()

        def local(self, context):
            pass

        def remote(self, context):
            pass

    foo = Foo(bar='baz')

    assert foo.bar == 'baz'
    assert foo.foo == 'bar'
    assert dict(foo) == {'bar': 'baz', 'foo': 'bar'}


def test_parameter_attrib_error():
    from debellator import core

    class Foo(core.Command):
        bar = core.Parameter()

        def local(self, context):
            pass

        def remote(self, context):
            pass

    foo = Foo()
    with pytest.raises(AttributeError):
        foo.bar     # pylint: disable=W0104
