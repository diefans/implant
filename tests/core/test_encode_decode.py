import pytest


@pytest.fixture
def core():
    from debellator import core
    return core


class Foo:
    def __init__(self, a, b):
        self.a = a
        self.b = b

    @classmethod
    def __msgpack_encode__(cls, data, data_type):
        return (data.a, data.b)

    @classmethod
    def __msgpack_decode__(cls, encoded_data, data_type):
        a, b = encoded_data
        return cls(a, b)


def test_encode_uid(core):
    uid = core.Uid()

    encoded = core.encode(uid)

    decoded = core.decode(encoded)

    assert uid == decoded
    assert isinstance(uid, core.Uid)


def test_default_encoder_exception(core):
    ex = Exception('foobar')

    encoded = core.encode(ex)

    decoded = core.decode(encoded)

    assert ex.args == decoded.args


def test_subclass(core):
    assert issubclass(Foo, core.Msgpack)


def test_encode_decode(core):
    data = (1, 'abc', 2.3, {1, 2, 3}, {1: 2})
    encoded_data = core.Msgpack.encode(data)
    decoded_data = core.Msgpack.decode(encoded_data)
    assert data == decoded_data


def test_custom_encoder(core):
    data = Foo(1, 2)
    encoded_data = core.Msgpack.encode(data)
    decoded_data = core.Msgpack.decode(encoded_data)
    assert (data.a, data.b) == (decoded_data.a, decoded_data.b)


def test_command(core):
    data = core.Command()
    data.a = 1
    data.b = 2
    encoded_data = core.encode(data)
    decoded_data = core.decode(encoded_data)

    assert decoded_data == {'a': 1, 'b': 2}
    assert isinstance(decoded_data, core.Command)


def test_dispacth_command(core):
    from debellator import commands
    cmd = commands.Echo(foo='bar')
    data = core.DispatchCommand('fqin', cmd)

    encoded_data = core.encode(data)
    decoded_data = core.decode(encoded_data)
    assert isinstance(decoded_data, core.DispatchCommand)
    assert (decoded_data.fqin, decoded_data.command) == (data.fqin, data.command)


def test_stop_async_iteration(core):
    data = StopAsyncIteration()
    encoded_data = core.encode(data)
    decoded_data = core.decode(encoded_data)
    assert isinstance(decoded_data, StopAsyncIteration)
