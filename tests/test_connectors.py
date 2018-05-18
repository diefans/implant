import pytest


@pytest.yield_fixture
def event_loop():
    try:
        import uvloop
        loop = uvloop.new_event_loop()

    except ImportError:
        import asyncio
        loop = asyncio.new_event_loop()

    yield loop

    loop.close()


@pytest.mark.env('ssh')
@pytest.mark.asyncio
async def test_ssh(event_loop):
    import asyncio
    from implant import core, connect

    connector = connect.Ssh()

    remote = await connector.launch()

    # setup launch specific tasks
    com_remote = asyncio.ensure_future(remote.communicate())
    try:
        result = await remote.execute(core.InvokeImport,
                                      fullname='implant.commands')
        result = await remote.execute('implant.commands:Echo',
                                      data='foobar')

        assert result['remote_data'] == 'foobar'

    finally:
        com_remote.cancel()
        await com_remote


@pytest.mark.parametrize('con_str, result', [
    ('local://otheruser!@', ('local', 'otheruser', None, None, None)),
    ('local://!@', ('local', True, None, None, None)),
    ('local://@', ('local', False, None, None, None)),
    ('local://', ('local', False, None, None, None)),
    # we have no validation for e.g. hostnames:
    # see https://tools.ietf.org/html/rfc3986#section-3.2.2
    ('local://!', ('local', False, None, '!', None)),
    ('ssh://otheruser!user@host', ('ssh', 'otheruser', 'user', 'host', None)),
    ('ssh://!user@host', ('ssh', True, 'user', 'host', None)),
    ('ssh://user@host', ('ssh', False, 'user', 'host', None)),
    ('ssh://host', ('ssh', False, None, 'host', None)),
    ('lxd://otheruser!user@host/container', ('lxd', 'otheruser', 'user',
                                             'host', 'container')),
    ('lxd://!user@host/container', ('lxd', True, 'user', 'host', 'container')),
    ('lxd://user@host/container', ('lxd', False, 'user', 'host', 'container')),
    ('lxd://host/container', ('lxd', False, None, 'host', 'container')),
])
def test_parse_connection_string(con_str, result):
    from implant import connect

    r = connect.Connector.parse_connection_string(con_str)
    assert r == result
