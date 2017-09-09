import pytest


def test_register(core):
    class Foo:
        @classmethod
        def __msgpack_encode__(cls, data, data_type):
            return None

        @classmethod
        def __msgpack_decode__(cls, encoded_data, data_type):
            return None

    core.Msgpack.register()(Foo)
    assert core.Msgpack.custom_encoders[Foo] == Foo


def test_register_wrong_type(core):
    class Foo:
        pass
    with pytest.raises(TypeError) as info:
        core.Msgpack.register()(Foo)

    assert info.value.args == (
        "Msgpack handler must be a subclass of abstract "
        "`Msgpack` class: <class 'test_msgpack.test_register_wrong_type.<locals>.Foo'>",)


@pytest.mark.parametrize("value", [
    None, True, False, -1, 2**64-1, 1.2345, 'text', b'binary', [1, 2, 3], (1, 2, 3), {'foo': 'bar'}
])
def test_default_types(value, core):
    encoded = core.encode(value)
    decoded = core.decode(encoded)
    assert decoded == value
