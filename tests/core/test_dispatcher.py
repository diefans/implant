import pytest


@pytest.yield_fixture
def event_loop():
    try:
        import uvloop_
        loop = uvloop.new_event_loop()

    except ImportError:
        import asyncio
        loop = asyncio.new_event_loop()

    yield loop

    loop.close()


@pytest.mark.asyncio
async def test_echo(event_loop):
    import asyncio
    from debellator import core, connect

    connector = connect.Ssh()

    remote = await connector.launch()

    # setup launch specific tasks
    com_remote = asyncio.ensure_future(remote.communicate())
    try:
        com_import = core.InvokeImport(fullname='debellator.commands')
        result = await remote.execute(com_import)

        com_echo = core.Command['debellator.commands:Echo'](foo='bar')
        result = await remote.execute(com_echo)

        assert result['remote_self']['foo'] == 'bar'

    finally:
        com_remote.cancel()
        await com_remote
