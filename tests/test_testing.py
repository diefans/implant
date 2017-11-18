import pytest


@pytest.mark.asyncio
async def test_testing(remote_task):
    from debellator import core

    result = await remote_task.execute(core.InvokeImport, fullname='debellator.commands')
    result = await remote_task.execute('debellator.commands:Echo', data='foobar')

    assert result['remote_data'] == 'foobar'
