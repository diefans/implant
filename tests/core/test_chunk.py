import pytest


@pytest.mark.parametrize("raw,chunk", [
    (b'', AssertionError),
    (b'||', (b'', b'', b'')),
    (b'||\n', (b'', b'', b'')),
    (b'123|123|Zm9v', (b'123', b'123', b'foo')),
    (b'123|123|Zm9v\n', (b'123', b'123', b'foo')),
])
def test_chunk_decode(raw, chunk):
    from dbltr import core

    if chunk == AssertionError:
        with pytest.raises(AssertionError):
            chunk_obj = core.Chunk.decode(raw)

    else:
        chunk_obj = core.Chunk.decode(raw)
        assert chunk == (chunk_obj.channel, chunk_obj.uid, chunk_obj.data)


@pytest.mark.parametrize("eol,raw,chunk", [
    (True, b'||\n', (b'', b'', b'')),
    (False, b'||', (b'', b'', b'')),
    (True, b'123|123|Zm9v\n', (b'123', b'123', b'foo')),
    (False, b'123|123|Zm9v', (b'123', b'123', b'foo')),
])
def test_chunk_encode(eol, raw, chunk):
    from dbltr import core

    channel, uid, data = chunk
    chunk_obj = core.Chunk(data=data, channel=channel, uid=uid)

    assert raw == chunk_obj.encode(eol)


def test_split_data():

    from dbltr import core

    data = b'0123456789'
    result = list(core.split_data(data, 3))

    assert result == [b'012', b'345', b'678', b'9']


def _compression_value(value):
    return {
        False: 0,
        True: 1,
        'gzip': 1,
        'lzma': 2,
        'zlib': 3,
        # 0: False,
        # 1: 'gzip',
        # 2: 'lzma',
        # 3: 'zlib',
    }.get(value, value)


class ChunkFlags(dict):

    _masks = {
        'eom': (1, 0, int, bool),
        'stop_iter': (1, 1, int, bool),
        'compression': (3, 2, {
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

    def __init__(self, *, eom=True, stop_iter=False, compression=False):
        self.__dict__ = self
        super(ChunkFlags, self).__init__()

        self.eom = eom
        self.stop_iter = stop_iter
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


@pytest.mark.parametrize('kwargs, result', [
    ({'eom': False, 'stop_iter': False, 'compression': False}, 0),
    ({'eom': True, 'stop_iter': False, 'compression': False}, 1),
    ({'eom': False, 'stop_iter': True, 'compression': False}, 2),
    ({'eom': True, 'stop_iter': True, 'compression': False}, 3),

    ({'eom': False, 'stop_iter': False, 'compression': 'gzip'}, 4),
    ({'eom': True, 'stop_iter': False, 'compression': 'gzip'}, 5),
    ({'eom': False, 'stop_iter': True, 'compression': 'gzip'}, 6),
    ({'eom': True, 'stop_iter': True, 'compression': 'gzip'}, 7),

    ({'eom': False, 'stop_iter': False, 'compression': True}, 4),
    ({'eom': True, 'stop_iter': False, 'compression': True}, 5),
    ({'eom': False, 'stop_iter': True, 'compression': True}, 6),
    ({'eom': True, 'stop_iter': True, 'compression': True}, 7),

    ({'eom': False, 'stop_iter': False, 'compression': 'lzma'}, 8),
    ({'eom': True, 'stop_iter': False, 'compression': 'lzma'}, 9),
    ({'eom': False, 'stop_iter': True, 'compression': 'lzma'}, 10),
    ({'eom': True, 'stop_iter': True, 'compression': 'lzma'}, 11),

    ({'eom': False, 'stop_iter': False, 'compression': 'zlib'}, 12),
    ({'eom': True, 'stop_iter': False, 'compression': 'zlib'}, 13),
    ({'eom': False, 'stop_iter': True, 'compression': 'zlib'}, 14),
    ({'eom': True, 'stop_iter': True, 'compression': 'zlib'}, 15),
])
def test_flags(kwargs, result):
    flags = ChunkFlags(**kwargs)

    encoded = flags.encode()

    assert result == encoded

    decoded = ChunkFlags.decode(encoded)

    assert flags == decoded


import struct
import uuid


class BinaryChunk:

    """
    structure:
    - channel_name length must be less than 1024 characters
    -

    [header length = 30 bytes]
    [!Q: 8 bytes]                     [!16s]     [!H: 2 bytes]        [!I: 4 bytes]
    {flags: compression|eom|stop_iter}{data uuid}{channel_name length}{data length}{channel_name}{data}


    """

    def __init__(self, data=None, *, channel_name=b'', uid=None):
        self.data = data and memoryview(data) or None
        self.channel_name = channel_name
        self.uid = uid

    @classmethod
    def view(cls, raw):
        raw_view = memoryview(raw)

        flags, uid, channel_name_length, data_length = cls.decode_header(raw_view[:30])

        channel_name_end = 30 + channel_name_length
        data_end = channel_name_end + data_length

        return flags, uid, memoryview(raw_view[30: channel_name_end]), memoryview(raw_view[channel_name_end: data_end])

    @classmethod
    def decode(cls, raw):
        pass

    def encode(self, eom=True):
        return b''.join((self.encode_header(eom=eom), self.channel_name, self.data))

    def encode_header(self, **flags):
        channel_name_length = len(self.channel_name)
        data_length = len(self.data)
        flags = ChunkFlags(**flags)
        header = struct.pack('!Q16sHI', flags.encode(), self.uid.bytes, channel_name_length, data_length)

        return header

    @classmethod
    def decode_header(cls, header):
        flags_encoded, uid_bytes, channel_name_length, data_length = struct.unpack('!Q16sHI', header)

        return ChunkFlags.decode(flags_encoded), uuid.UUID(bytes=uid_bytes), channel_name_length, data_length


def test_binary_chunk():
    uid = uuid.uuid1()

    c = BinaryChunk(b'1234567890', channel_name=b'foobar', uid=uid)

    header = c.encode_header()

    decoded_header = BinaryChunk.decode_header(header)

    assert decoded_header == (ChunkFlags(), uid, 6, 10)


def test_binary_chunk_view():
    uid = uuid.uuid1()

    c = BinaryChunk(b'1234567890', channel_name=b'foobar', uid=uid)
    raw = c.encode()

    view = c.view(raw)

    assert view == (ChunkFlags(), uid, memoryview(c.channel_name), memoryview(c.data))
