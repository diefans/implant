import pytest
from unittest import mock


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

    (0b00010000, {'eom': False, 'stop_iter': False, 'compression': 'gzip'}),
    (0b00010001, {'eom': True, 'stop_iter': False, 'compression': 'gzip'}),
    (0b00010010, {'eom': False, 'stop_iter': True, 'compression': 'gzip'}),
    (0b00010011, {'eom': True, 'stop_iter': True, 'compression': 'gzip'}),

    (0b00010000, {'eom': False, 'stop_iter': False, 'compression': True}),
    (0b00010001, {'eom': True, 'stop_iter': False, 'compression': True}),
    (0b00010010, {'eom': False, 'stop_iter': True, 'compression': True}),
    (0b00010011, {'eom': True, 'stop_iter': True, 'compression': True}),

    (0b00100000, {'eom': False, 'stop_iter': False, 'compression': 'lzma'}),
    (0b00100001, {'eom': True, 'stop_iter': False, 'compression': 'lzma'}),
    (0b00100010, {'eom': False, 'stop_iter': True, 'compression': 'lzma'}),
    (0b00100011, {'eom': True, 'stop_iter': True, 'compression': 'lzma'}),

    (0b00110000, {'eom': False, 'stop_iter': False, 'compression': 'zlib'}),
    (0b00110001, {'eom': True, 'stop_iter': False, 'compression': 'zlib'}),
    (0b00110010, {'eom': False, 'stop_iter': True, 'compression': 'zlib'}),
    (0b00110011, {'eom': True, 'stop_iter': True, 'compression': 'zlib'}),
])
def test_flags(kwargs, result):
    from dbltr import core

    flags = core.ChunkFlags(**kwargs)

    encoded = flags.encode()

    assert result == encoded

    decoded = core.ChunkFlags.decode(encoded)

    assert flags == decoded


class TestChannel:
    def test_init(self):
        pass

    @pytest.mark.asyncio
    async def test_send(self):
        import math
        import asyncio
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

                chunk_count = 3 * (chunk_size + (1 if rest else 0)) + 2
                assert len(chunks) == chunk_count

                for i, part in enumerate(chunks):
                    if i % 2 == 0:
                        id(part) == id(c.name)

    @pytest.mark.asyncio
    async def test_communicate(self, event_loop):
        import os
        import asyncio
        from dbltr import core

        r_pipe, w_pipe = os.pipe()
        io_queues = core.IoQueues()

        # TODO mock Outgoing/os.kill to prevent shutdown of test run
        # alternativelly think about some other means to shutdown remote process if ssh closes
        async with core.Incomming(pipe=r_pipe) as reader:
            async with core.Outgoing(pipe=w_pipe) as writer:
                com_future = asyncio.ensure_future(core.Channel.communicate(io_queues, reader, writer))

                try:
                    c = core.Channel('foo', io_queues=io_queues)

                    await c.send('bar')
                    msg = await c.receive()
                    assert msg == 'bar'

                    await c.send('baz', ack=True)
                    msg = await c.receive()
                    assert msg == 'baz'

                finally:
                    com_future.cancel()
                    await com_future
