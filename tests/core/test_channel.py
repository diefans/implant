from unittest import mock
import pytest


@pytest.mark.parametrize('result, kwargs', [
    (0b00000000, {'eom': False, 'stop_iter': False, 'compression': False}),
    (0b00000001, {'eom': True, 'stop_iter': False, 'compression': False}),
    (0b00000010, {'eom': False, 'stop_iter': True, 'compression': False}),
    (0b00000011, {'eom': True, 'stop_iter': True, 'compression': False}),

    (0b00000000, {'eom': False, 'stop_iter': False, 'send_ack': False}),
    (0b00000001, {'eom': True, 'stop_iter': False, 'send_ack': False}),
    (0b00000010, {'eom': False, 'stop_iter': True, 'send_ack': False}),
    (0b00000011, {'eom': True, 'stop_iter': True, 'send_ack': False}),

    (0b00000100, {'eom': False, 'stop_iter': False, 'send_ack': True}),
    (0b00000101, {'eom': True, 'stop_iter': False, 'send_ack': True}),
    (0b00000110, {'eom': False, 'stop_iter': True, 'send_ack': True}),
    (0b00000111, {'eom': True, 'stop_iter': True, 'send_ack': True}),

    (0b00000000, {'eom': False, 'stop_iter': False, 'compression': 0}),
    (0b00010001, {'eom': True, 'stop_iter': False, 'compression': 1}),
    (0b00010001, {'eom': True, 'stop_iter': False, 'compression': True}),
])
def test_flags(kwargs, result):
    from dbltr import core

    flags = core.ChunkFlags(**kwargs)

    encoded = flags.encode()

    assert result == encoded

    decoded = core.ChunkFlags.decode(encoded)

    assert flags == decoded


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


class TestChannel:
    def test_init(self):
        pass

    @pytest.mark.asyncio
    async def test_send(self):
        import uuid
        import pickle

        from dbltr import core

        uid = uuid.uuid1()

        # set chunksize
        data = b'1234567890' * 10
        chunk_size, rest = divmod(len(pickle.dumps(data)), 10)

        with mock.patch.object(core.Channel, 'chunk_size', chunk_size):
            with mock.patch('uuid.uuid1.__call__') as mock_uuid:
                mock_uuid.return_value = uid
                c = core.Channel('foo')
                queue = c.io_queues.send

                await c.send(data)

                chunks = []

                while not queue.empty():
                    chunks.append(await queue.get())
                    queue.task_done()

                chunk_count = (chunk_size + (1 if rest else 0)) + 1
                assert len(chunks) == chunk_count

    @pytest.mark.asyncio
    async def test_communicate(self, event_loop):
        import os
        import time
        import asyncio
        import uuid
        from dbltr import core

        r_pipe, w_pipe = os.pipe()

        # TODO mock Outgoing/os.kill to prevent shutdown of test run
        # alternativelly think about some other means to shutdown remote process if ssh closes
        with mock.patch.object(core.Channel, 'chunk_size', 0x800):
            async with core.Incomming(pipe=r_pipe) as reader:
                async with core.Outgoing(pipe=w_pipe) as writer:
                    io_queues = core.IoQueues()
                    com_future = asyncio.ensure_future(core.Channel.communicate(io_queues, reader, writer))

                    try:
                        # channel will receive its own messages
                        c = core.Channel('foo', io_queues=io_queues)

                        await c.send('bar')
                        msg = await c
                        assert msg == 'bar'

                        uid, duration = await c.send('baz', ack=True)
                        assert isinstance(uid, uuid.UUID)
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
                        await com_future
