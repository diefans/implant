import pytest
from unittest import mock


import asyncio
import uuid
import struct
import weakref
from dbltr import core


class ChunkFlags(dict):

    _masks = {
        'eom': (1, 0, int, bool),
        'stop_iter': (1, 1, int, bool),
        'ack': (1, 2, int, bool),
        'compression': (3, 3, {
            False: 0,
            True: 1,
            'gzip': 1,
            'lzma': 2,
            'zlib': 3,
        }.get, {
            0: False,
            1: 'gzip',
            2: 'lzma',
            3: 'zlib'
        }.get),
    }

    def __init__(self, *, ack=False, eom=False, stop_iter=False, compression=False):
        self.__dict__ = self
        super(ChunkFlags, self).__init__()

        self.eom = eom
        self.stop_iter = stop_iter
        self.ack = ack
        self.compression = compression is True and 'gzip' or compression

    def encode(self):
        def _mask_value(k, v):
            mask, shift, enc, dec = self._masks[k]
            return (enc(v) & mask) << shift

        return sum(_mask_value(k, v) for k, v in self.items())

    @classmethod
    def decode(cls, value):
        def _unmask_value(k, v):
            mask, shift, enc, dec = v
            return dec((value >> shift) & mask)

        return cls(**{k: _unmask_value(k, v) for k, v in cls._masks.items()})


@pytest.mark.parametrize('result, kwargs', [
    (0b00000000, {'eom': False, 'stop_iter': False, 'compression': False}),
    (0b00000001, {'eom': True, 'stop_iter': False, 'compression': False}),
    (0b00000010, {'eom': False, 'stop_iter': True, 'compression': False}),
    (0b00000011, {'eom': True, 'stop_iter': True, 'compression': False}),

    (0b00000000, {'eom': False, 'stop_iter': False, 'ack': False}),
    (0b00000001, {'eom': True, 'stop_iter': False, 'ack': False}),
    (0b00000010, {'eom': False, 'stop_iter': True, 'ack': False}),
    (0b00000011, {'eom': True, 'stop_iter': True, 'ack': False}),

    (0b00000100, {'eom': False, 'stop_iter': False, 'ack': True}),
    (0b00000101, {'eom': True, 'stop_iter': False, 'ack': True}),
    (0b00000110, {'eom': False, 'stop_iter': True, 'ack': True}),
    (0b00000111, {'eom': True, 'stop_iter': True, 'ack': True}),

    (0b00001000, {'eom': False, 'stop_iter': False, 'compression': 'gzip'}),
    (0b00001001, {'eom': True, 'stop_iter': False, 'compression': 'gzip'}),
    (0b00001010, {'eom': False, 'stop_iter': True, 'compression': 'gzip'}),
    (0b00001011, {'eom': True, 'stop_iter': True, 'compression': 'gzip'}),

    (0b00001000, {'eom': False, 'stop_iter': False, 'compression': True}),
    (0b00001001, {'eom': True, 'stop_iter': False, 'compression': True}),
    (0b00001010, {'eom': False, 'stop_iter': True, 'compression': True}),
    (0b00001011, {'eom': True, 'stop_iter': True, 'compression': True}),

    (0b00010000, {'eom': False, 'stop_iter': False, 'compression': 'lzma'}),
    (0b00010001, {'eom': True, 'stop_iter': False, 'compression': 'lzma'}),
    (0b00010010, {'eom': False, 'stop_iter': True, 'compression': 'lzma'}),
    (0b00010011, {'eom': True, 'stop_iter': True, 'compression': 'lzma'}),

    (0b00011000, {'eom': False, 'stop_iter': False, 'compression': 'zlib'}),
    (0b00011001, {'eom': True, 'stop_iter': False, 'compression': 'zlib'}),
    (0b00011010, {'eom': False, 'stop_iter': True, 'compression': 'zlib'}),
    (0b00011011, {'eom': True, 'stop_iter': True, 'compression': 'zlib'}),
])
def test_flags(kwargs, result):
    flags = ChunkFlags(**kwargs)

    encoded = flags.encode()

    assert result == encoded

    decoded = ChunkFlags.decode(encoded)

    assert flags == decoded


HEADER_FMT = '!16sQHI'


class Channel:

    _queue_out = asyncio.Queue()

    chunk_size = 0x8000

    acknowledgements = weakref.WeakValueDictionary()

    def __init__(self, name=None, *, io_queues=None):
        self.name = name

        if io_queues is None:
            io_queues = core.IoQueues(send=self._queue_out)

        self.io_queues = io_queues

    def _encode_header(self, uid, data=None, *, flags=None):
        assert isinstance(uid, uuid.UUID), "uid must be an UUID instance"

        if flags is None:
            flags = {}

        name = self.name.encode()

        channel_name_length = len(name)
        data_length = data and len(data) or 0
        chunk_flags = ChunkFlags(**flags)

        header = struct.pack(HEADER_FMT, uid.bytes, chunk_flags.encode(), channel_name_length, data_length)

        return header

    @classmethod
    def _decode_header(cls, header):
        flags_encoded, uid_bytes, channel_name_length, data_length = struct.unpack(HEADER_FMT, header)

        return uuid.UUID(bytes=uid_bytes), ChunkFlags.decode(flags_encoded), channel_name_length, data_length

    async def send(self, data, ack=False):
        compression = False
        uid = uuid.uuid1()
        name = self.name.encode()

        for part in core.split_data(data, self.chunk_size):

            header = self._encode_header(uid, part, flags={'eom': False, 'ack': False, 'compression': compression})

            await self.io_queues.send.put(header)
            await self.io_queues.send.put(name)
            await self.io_queues.send.put(part)

        header = self._encode_header(uid, None, flags={'eom': True, 'ack': ack, 'compression': False})
        await self.io_queues.send.put(header)
        await self.io_queues.send.put(name)

        # if acknowledgement is asked for
        # we return a future to let the caller wait for
        if ack:
            ack_future = asyncio.Future()
            self.acknowledgements[uid.int] = ack_future

            return ack_future

    @classmethod
    async def communicate(cls, io_queues, reader, writer):
        async def _send_writer():
            # send outgoing queue to writer
            pass


class TestChannel:
    def test_init(self):
        pass

    @pytest.mark.asyncio
    async def test_send(self):

        uid = uuid.uuid1()

        with mock.patch.object(Channel, 'chunk_size', 10):
            with mock.patch('uuid.uuid1.__call__') as mock_uuid:
                mock_uuid.return_value = uid
                c = Channel('foo')
                queue = c.io_queues.send

                await c.send(b'1234567890' * 10)

                chunks = []

                while not queue.empty():
                    chunks.append(await queue.get())
                    queue.task_done()

                assert len(chunks) == 32

                for i, part in enumerate(chunks):
                    if i % 2 == 0:
                        id(part) == id(c.name)
