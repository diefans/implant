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
