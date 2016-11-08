def test_command_args():
    from debellator import master

    target = master.Target()

    args = target.command_args(code=b'foobar', options={'foo': 'bar'})

    assert \
        'import imp, base64, json; boot = imp.new_module("debellator.msgr");c = ' \
        'compile(base64.b64decode(b"Zm9vYmFy"), "<string>", "exec");exec(c, boot.__dict__); ' \
        'boot.main(**boot.decode_options(b"eyJmb28iOiAiYmFyIn0="));' \
        in args
