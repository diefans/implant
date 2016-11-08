def test_encode_decode_options():
    from debellator import core

    encoded = core.encode_options(foo='bar', bar=123)

    decoded = core.decode_options(encoded)

    assert decoded == {
        'foo': 'bar',
        'bar': 123
    }
