from unittest import mock

import pytest


@pytest.fixture
def namespace_spec():
    import importlib.machinery

    spec = importlib.machinery.ModuleSpec(
        name='foobar_namespace',
        loader=None,
        origin='namespace',
        is_package=True
    )
    return spec


@pytest.fixture
def fake_module_finder(namespace_spec):
    import importlib.abc

    class FakeModuleFinder(importlib.abc.MetaPathFinder):
        def find_spec(self, fullname, path, target=None):
            if fullname == 'foobar_namespace':
                return namespace_spec

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


@pytest.mark.asyncio
@pytest.mark.parametrize('fullname, result', [
    ('foobar_namespace', {
        'name': 'foobar_namespace',
        'namespace': True,
        'package': True,
        'origin': 'namespace',
        'source': None
    }),
    ('debellator.testing.foobar', {
        'name': 'debellator.testing.foobar',
        'namespace': False,
        'package': False,
        'origin': 'origin',
        'source': "source"
    })
])
@pytest.mark.usefixtures('mocked_meta_path')
async def test_find_spec_data(remote_task, fake_module_finder, fullname, result):
    from debellator import core

    with mock.patch('debellator.testing.foobar.__spec__.origin', 'origin'), \
            mock.patch('debellator.testing.foobar.__spec__.loader.get_source', return_value='source'):
        spec_data = await remote_task.execute(core.FindSpecData, fullname=fullname)

    assert spec_data == result


@pytest.mark.asyncio
async def _test_invoke_import(remote_task):
    from debellator import core
    import logging
    logging.basicConfig(level=logging.DEBUG)

    result = await remote_task.execute(core.InvokeImport, fullname='debellator.commands')
    assert result is not None
