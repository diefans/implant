from unittest import mock
import pytest


@pytest.mark.parametrize('with_venv,venv_lines,options', [
    (False, [], '(\'gaR2ZW52wg==\',)'),
    (True, [
        'import os, sys, site, pkg_resources\n',
        'venv_path = os.path.expanduser("~/.implant")\n',
        'entry = site.getsitepackages([venv_path])[0]\n',
        'if not os.path.isdir(entry):\n',
        '    import venv\n',
        '    venv.create(venv_path, system_site_packages=False, clear=True,'
        ' symlinks=False, with_pip=True)\n',
        'sys.prefix = venv_path\n',
        'sys.path.insert(0, entry)\n',
        'site.addsitedir(entry)\n',
        'pkg_resources.working_set.add_entry(entry)\n',
        'try:\n',
        '    import umsgpack\n',
        'except ImportError:\n',
        '    import pip\n',
        '    pip.main(["install", "--prefix", venv_path, "-q", "u-msgpack-python"])\n'
    ], '(\'gaR2ZW52ww==\',)')
])
@mock.patch('implant.bootstrap.inspect')
def test_bootstrap_iter(inspect, with_venv, venv_lines, options):
    from implant import bootstrap
    import zlib
    import base64

    inspect.getsource.return_value = 'msgpack-code'
    inspect.getsourcefile.return_value = 'msgpack-source-file'
    msgpack_code = base64.b64encode(zlib.compress(b'msgpack-code', 9)).decode(),

    lines = [
        'import sys, imp, base64, zlib\n',
        'sys.modules["implant"] = implant = imp.new_module("implant")\n',
        'setattr(implant, "__path__", [])\n',
        'try:\n',
        '    from implant import msgpack\n',
        'except ImportError:\n',
        '    sys.modules["implant.msgpack"] = msgpack = imp.new_module("implant.msgpack")\n',
        '    c = compile(zlib.decompress(base64.b64decode(b"{msgpack_code}")),'
        ' "remote://msgpack-source-file", "exec")\n'.format(**locals()),
        '    exec(c, msgpack.__dict__)\n',
        'sys.modules["implant.core"] = core = imp.new_module("implant.core")\n',
        'implant.__dict__["core"] = core\n',
        'c = compile(zlib.decompress(base64.b64decode(b"eNpLLC5OLSpRCCkqTQUAGlIEUw==")),'
        ' "remote-string://", "exec", dont_inherit=True)\n',
        'exec(c, core.__dict__)\n',
        'core.main(**msgpack.decode(base64.b64decode(b"{options}")))\n'.format(
            msgpack_code=msgpack_code, options=options)
    ]

    bs = bootstrap.Bootstrap(b'assert True', options={'venv': with_venv})

    result = list(bs)

    assert result == venv_lines + lines
