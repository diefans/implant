import pytest


class Foo:
    def __init__(self, foo):
        self.foo = foo

    def __msgpack_encode__(self):
        return {
            'foo': self.foo
        }

    @classmethod
    def __msgpack_decode__(cls, encoded):
        foo = cls(encoded['foo'])
        return foo


def test_encode():
    from dbltr import core

    obj = Foo(123)

    encoded = core.encode_msgpack(obj)

    decoded = core.decode_msgpack(encoded)

    assert obj.foo == decoded.foo
    assert type(obj) == type(decoded)


def test_encode_uid():
    from dbltr import core

    uid = core.Uid()

    encoded = core.encode(uid)

    decoded = core.decode(encoded)

    assert uid == decoded
    assert isinstance(uid, core.Uid)
