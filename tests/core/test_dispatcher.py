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
        result = await remote.execute(core.InvokeImport, fullname='debellator.commands')
        result = await remote.execute('debellator.commands:Echo', data='foobar')

        assert result['remote_data'] == 'foobar'

    finally:
        com_remote.cancel()
        await com_remote
