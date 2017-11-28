def test_header(core):
    uid = core.Uid()
    h = core.Header(
        uid=uid,
        channel_name_len=10, data_len=100,
        eom=True, send_ack=True, recv_ack=True, compression=True
    )

    assert h.channel_name_len == 10
    assert h.data_len == 100
    assert h.uid == uid
    assert h.eom
    assert h.send_ack
    assert h.recv_ack
    assert h.compression

    h2 = core.Header(h)
    assert h2.channel_name_len == 10
    assert h2.data_len == 100
    assert h2.uid == uid
    assert h2.eom
    assert h2.send_ack
    assert h2.recv_ack
    assert h2.compression
