import pytest


@pytest.mark.parametrize('result, kwargs', [
    (0b00000000, {'eom': False, 'send_ack': False, 'recv_ack': False, 'compression': False}),
    (0b00000001, {'eom': True, 'send_ack': False, 'recv_ack': False, 'compression': False}),
    (0b00000010, {'eom': False, 'send_ack': True, 'recv_ack': False, 'compression': False}),
    (0b00000011, {'eom': True, 'send_ack': True, 'recv_ack': False, 'compression': False}),
    (0b00000100, {'eom': False, 'send_ack': False, 'recv_ack': True, 'compression': False}),
    (0b00000101, {'eom': True, 'send_ack': False, 'recv_ack': True, 'compression': False}),
    (0b00000110, {'eom': False, 'send_ack': True, 'recv_ack': True, 'compression': False}),
    (0b00000111, {'eom': True, 'send_ack': True, 'recv_ack': True, 'compression': False}),
    (0b00001000, {'eom': False, 'compression': True}),
    (0b00001001, {'eom': True, 'compression': True}),
    (0b00001010, {'eom': False, 'send_ack': True, 'compression': True}),
    (0b00001011, {'eom': True, 'send_ack': True, 'compression': True}),
    (0b00001100, {'eom': False, 'send_ack': False, 'recv_ack': True, 'compression': True}),
    (0b00001101, {'eom': True, 'send_ack': False, 'recv_ack': True, 'compression': True}),
    (0b00001110, {'eom': False, 'send_ack': True, 'recv_ack': True, 'compression': True}),
    (0b00001111, {'eom': True, 'send_ack': True, 'recv_ack': True, 'compression': True}),
])
def test_flags(kwargs, result):
    from debellator import core

    flags = core.ChunkFlags(**kwargs)

    encoded = flags.encode()

    assert result == encoded

    decoded = core.ChunkFlags.decode(encoded)

    assert flags == decoded
