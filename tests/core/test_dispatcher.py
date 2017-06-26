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
    from debellator import core, master, connect

    connector = connect.Ssh()

    # setup launch specific tasks
    dispatcher = core.Dispatcher()

    from pdb import set_trace; set_trace()       # XXX BREAKPOINT
    try:
        process = await connect.Remote(connector).launch()
        com_remote = asyncio.ensure_future(dispatcher.communicate(process.stdout, process.stdin))
        # remote_err = asyncio.ensure_future(log_remote_stderr(process))

        com_import = core.InvokeImport(fullname='debellator.plugins.core')
        result = await dispatcher.execute(com_import)

        com_echo = core.Command['debellator.core:Echo'](foo='bar')
        result = await dispatcher.execute(com_echo)

        assert result['remote_self']['foo'] == 'bar'

        # raises exception
        process.terminate()
        await process.wait()
    finally:
        com_remote.cancel()
        await com_remote
