def test_encode_decode_options():
    from dbltr import core

    encoded = core.encode_options(foo='bar', bar=123)

    decoded = core.decode_options(encoded)

    assert decoded == {
        'foo': 'bar',
        'bar': 123
    }
