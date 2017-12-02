import pytest


@pytest.fixture
def foobar_module_source():
    return 'class Foobar:\n'\
        '    async def remote(self, context):\n'\
        '        return "foobar"'


@pytest.fixture
def namespace_spec():
    import importlib.machinery

    spec = importlib.machinery.ModuleSpec(
        name='foobar_namespace',
        loader=None,
        origin='namespace origin',
        is_package=True
    )
    return spec


@pytest.fixture
def module_spec():
    import importlib.abc
    import importlib.machinery

    class ModuleLoader(importlib.abc.ExecutionLoader):
        def is_package(self):
            return False

        def get_filename(self, fullname):
            return 'filename'

        def get_source(self, fullname):
            return 'class Foobar:\n'\
                '    async def remote(self, context):\n'\
                '        return "foobar"'

        @classmethod
        def module_repr(cls):
            return "<module>"

    spec = importlib.machinery.ModuleSpec(
        name='foobar_module',
        loader=ModuleLoader(),
        origin='module origin',
        is_package=False
    )
    return spec


@pytest.fixture
def fake_module_finder(namespace_spec, module_spec):
    import importlib.abc

    class FakeModuleFinder(importlib.abc.MetaPathFinder):
        # pylint: disable=R0201
        def find_spec(self, fullname, path, target=None):
            if fullname == 'foobar_namespace':
                spec = namespace_spec
            elif fullname == 'foobar_module':
                spec = module_spec
            else:
                spec = None
            return spec

    return FakeModuleFinder()


@pytest.fixture
def mocked_meta_path(request, fake_module_finder):
    import sys

    old_meta_path = sys.meta_path
    sys.meta_path = old_meta_path + [fake_module_finder]

    def teardown():
        sys.meta_path = old_meta_path
    request.addfinalizer(teardown)
    return sys.meta_path
