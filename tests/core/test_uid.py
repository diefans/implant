def test_uid_eq():
    from implant import core

    u1 = core.Uid()

    u2 = core.Uid(bytes=u1.bytes)

    assert u1 == u2
