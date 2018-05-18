import pytest


@pytest.mark.asyncio
async def test_testing(remote_task):
    from implant import core

    result = await remote_task.execute(core.InvokeImport, fullname='implant.commands')
    result = await remote_task.execute('implant.commands:Echo', data='foobar')

    assert result['remote_data'] == 'foobar'
