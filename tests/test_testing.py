import pytest


@pytest.yield_fixture
def event_loop():
    # try:
    #     import uvloop
    #     loop = uvloop.new_event_loop()

    # except ImportError:
    import asyncio
    loop = asyncio.new_event_loop()

    yield loop

    loop.close()


@pytest.mark.asyncio
async def test_testing(event_loop):
    import asyncio
    import logging
    from debellator import core, testing

    connector = testing.PipeConnector()#loop=event_loop)
    remote = await connector.launch()

    # setup launch specific tasks
    com_remote = asyncio.ensure_future(remote.communicate())
    try:
        result = await remote.execute(core.InvokeImport, fullname='debellator.commands')
        result = await remote.execute('debellator.commands:Echo', data='foobar')

        assert result['remote_data'] == 'foobar'
    finally:
        com_remote.cancel()
        await com_remote

        # pending = [task for task in asyncio.Task.all_tasks()]
        # assert pending == []
