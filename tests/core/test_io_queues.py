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
async def test_send():
    import pickle

    from debellator import core

    uid = core.Uid()

    # set chunksize
    data = b'1234567890' * 10
    chunk_size, rest = divmod(len(pickle.dumps(data)), 10)

    with mock.patch.object(core.IoQueues, 'chunk_size', chunk_size):
        with mock.patch.object(core.Uid, '__call__') as mock_uuid:
            mock_uuid.return_value = uid
            queues = core.IoQueues()
            c = queues.get_channel('foo')
            queue = queues.outgoing

            await c.send(data)

            chunks = []

            while not queue.empty():
                chunks.append(await queue.get())
                queue.task_done()

            chunk_count = (chunk_size + (1 if rest else 0)) + 1
            assert len(chunks) == chunk_count


@pytest.mark.asyncio
async def test_communicate(event_loop):
    import os
    import time
    import asyncio
    import uuid
    from debellator import core

    r_pipe, w_pipe = os.pipe()

    # TODO mock Outgoing/os.kill to prevent shutdown of test run
    # alternativelly think about some other means to shutdown remote process if ssh closes
    with mock.patch.object(core.IoQueues, 'chunk_size', 0x800):
        async with core.Incomming(pipe=r_pipe) as reader:
            async with core.Outgoing(pipe=w_pipe) as writer:
                io_queues = core.IoQueues()
                com_future = asyncio.ensure_future(io_queues.communicate(reader, writer))

                try:
                    # channel will receive its own messages
                    c = io_queues.get_channel('foo')

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
                        c.send('1' * 100000),
                        c.send('2' * 100000),
                        c.send('3' * 100000),
                        c.send('4' * 100000),
                        c.send('5' * 100000),
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
