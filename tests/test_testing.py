import pytest


@pytest.mark.asyncio
async def test_testing(implant_remote_task):
    from implant import core

    result = await implant_remote_task\
        .execute(core.InvokeImport, fullname='implant.commands')
    result = await implant_remote_task\
        .execute('implant.commands:Echo', data='foobar')

    assert result['remote_data'] == 'foobar'
