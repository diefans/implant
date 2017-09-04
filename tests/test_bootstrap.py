from unittest import mock
import pytest


@pytest.mark.parametrize('with_venv,venv_lines,options', [
    (False, [], '(\'gaR2ZW52wg==\',)'),
    (True, [
        'import os, sys, site, pkg_resources\n',
        'venv_path = os.path.expanduser("~/.debellator")\n',
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
@mock.patch('debellator.bootstrap.inspect')
def test_bootstrap_iter(inspect, with_venv, venv_lines, options):
    from debellator import bootstrap
    import zlib
    import base64

    inspect.getsource.return_value = 'umsgpack-code'
    inspect.getsourcefile.return_value = 'umsgpack-source-file'
    msgpack_code = base64.b64encode(zlib.compress(b'umsgpack-code', 9)).decode(),

    lines = [
        'import sys, imp, base64, zlib\n',
        'try:\n',
        '    import umsgpack\n',
        'except ImportError:\n',
        '    sys.modules["umsgpack"] = umsgpack = imp.new_module("umsgpack")\n',
        '    c = compile(zlib.decompress(base64.b64decode(b"{msgpack_code}")),'
        ' "remote://umsgpack-source-file", "exec")\n'.format(**locals()),
        '    exec(c, umsgpack.__dict__)\n',
        'sys.modules["debellator"] = debellator = imp.new_module("debellator")\n',
        'setattr(debellator, "__path__", [])\n',
        'sys.modules["debellator.core"] = core = imp.new_module("debellator.core")\n',
        'debellator.__dict__["core"] = core\n',
        'c = compile(zlib.decompress(base64.b64decode(b"(\'eNpLLC5OLSpRCCkqTQUAGlIEUw==\',)")),'
        ' "remote-string://", "exec", dont_inherit=True)\n',
        'exec(c, core.__dict__)\n',
        'core.main(**core.decode(base64.b64decode(b"{options}")))\n'.format(
            msgpack_code=msgpack_code, options=options)
    ]

    bs = bootstrap.Bootstrap(b'assert True', options={'venv': with_venv})

    result = list(bs)

    assert result == venv_lines + lines
