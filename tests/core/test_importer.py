import pytest


@pytest.mark.asyncio
@pytest.mark.parametrize('fullname, result', [
    ('foobar_namespace', {
        'name': 'foobar_namespace',
        'namespace': True,
        'package': True,
        'origin': 'namespace origin',
        'source': None
    }),
    ('foobar_module', {
        'name': 'foobar_module',
        'namespace': False,
        'package': False,
        'origin': 'module origin',
        'source': 'class Foobar:\n'
                  '    async def remote(self, context):\n'
                  '        return "foobar"'
    })
])
@pytest.mark.usefixtures('mocked_meta_path')
async def test_find_spec_data(remote_task, fake_module_finder, fullname,
                              result):
    from debellator import core

    spec_data = await remote_task.execute(core.FindSpecData,
                                          fullname=fullname)

    assert spec_data == result


@pytest.mark.asyncio
async def _test_invoke_import(remote_task):
    from debellator import core
    result = await remote_task.execute(core.InvokeImport,
                                       fullname='debellator.commands')
    assert result is not None
