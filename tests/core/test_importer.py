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


@pytest.mark.asyncio
@pytest.mark.parametrize('fullname, result', [
    ('foobar_namespace', {
        'name': 'foobar_namespace',
        'namespace': True,
        'origin': 'namespace',
        'source': None
    }),
])
async def test_find_spec_data_namespace(remote_task, fake_module_finder, fullname, result):
    from debellator import core
    import logging
    logging.basicConfig(level=logging.DEBUG)

    with mock.patch('sys.meta_path', [fake_module_finder]):
        spec_data = await remote_task.execute(core.FindSpecData, fullname=fullname)

    assert spec_data == result
