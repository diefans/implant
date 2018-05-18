from unittest import mock

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
async def test_send(event_loop):
    import asyncio
    import os

    from implant import core

    uid = core.Uid()

    # set chunksize
    data = b'1234567890' * 10
    chunk_size, rest = divmod(len(data), 10)

    r_pipe, w_pipe = os.pipe()

    with mock.patch.object(core.Channels, 'chunk_size', chunk_size):
        with mock.patch.object(core.Uid, '__call__') as mock_uuid:
            async with core.Incomming(pipe=r_pipe) as reader:
                async with core.Outgoing(pipe=w_pipe) as writer:
                    mock_uuid.return_value = uid
                    channels = core.Channels(reader=reader, writer=writer)
                    fut_enqueue = asyncio.ensure_future(channels.enqueue())

                    c = channels.get_channel('foo')

                    try:
                        await c.send(data)

                        chunks = []

                        returned_data = await c

                        assert returned_data == data
                    finally:
                        fut_enqueue.cancel()
                        await fut_enqueue


@pytest.mark.asyncio
async def test_communicate(event_loop):
    import os
    import time
    import asyncio
    import uuid
    from implant import core

    r_pipe, w_pipe = os.pipe()

    # TODO mock Outgoing/os.kill to prevent shutdown of test run
    # alternativelly think about some other means to shutdown remote process if ssh closes
    with mock.patch.object(core.Channels, 'chunk_size', 0x800):
        async with core.Incomming(pipe=r_pipe) as reader:
            async with core.Outgoing(pipe=w_pipe) as writer:
                channels = core.Channels(reader=reader, writer=writer)
                com_future = asyncio.ensure_future(channels.enqueue())

                try:
                    # channel will receive its own messages
                    c = channels.get_channel('foo')

                    await c.send('bar')
                    msg = await c
                    assert msg == 'bar'

                    uid, duration = await c.send('baz', ack=True)
                    assert isinstance(uid, core.Uid)
                    assert isinstance(duration, float)

                    msg = await c
                    assert msg == 'baz'

                    # parallel send
                    t1 = time.time()
                    await asyncio.gather(
                        c.send('1'),
                        c.send('2'),
                        c.send('3'),
                        c.send('4'),
                        c.send('5'),
                    )

                    msgs = []

                    msgs.append(await c)
                    msgs.append(await c)
                    msgs.append(await c)
                    msgs.append(await c)
                    msgs.append(await c)

                    # just check for existence
                    short_msgs = [m[0] for m in msgs]
                    assert set(short_msgs) == {'1', '2', '3', '4', '5'}

                    t2 = time.time()

                    print("duration for parallel test: ", t2 - t1)

                finally:
                    # shutdown channel communications
                    com_future.cancel()
                    await com_future
