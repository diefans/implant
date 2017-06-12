import pytest


class Foo:
    def __init__(self, foo):
        self.foo = foo

    @classmethod
    def __msgpack_encode__(cls, data, data_type):
        return {
            'foo': data.foo
        }

    @classmethod
    def __msgpack_decode__(cls, encoded, data_type):
        foo = cls(encoded['foo'])
        return foo


def test_encode():
    from debellator import core

    obj = Foo(123)

    encoded = core.encode_msgpack(obj)

    decoded = core.decode_msgpack(encoded)

    assert obj.foo == decoded.foo
    assert type(obj) == type(decoded)


def test_encode_uid():
    from debellator import core

    uid = core.Uid()

    encoded = core.encode(uid)

    decoded = core.decode(encoded)

    assert uid == decoded
    assert isinstance(uid, core.Uid)


def test_default_encoder_exception():
    from debellator import core

    ex = Exception('foobar')

    encoded = core.encode(ex)

    decoded = core.decode(encoded)

    assert ex.args == decoded.args
