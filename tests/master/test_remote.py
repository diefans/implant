import pytest


@pytest.mark.parametrize("venv,result", [
    (False, [
        'import sys, imp, base64, zlib;',
        'try:',
        '   import msgpack;',
        'except ImportError:',
        '   sys.modules["msgpack"] = msgpack = imp.new_module("msgpack");',
        '   c = compile(zlib.decompress(base64.b64decode(b"{msgpack_code}")), "{msgpack_code_path}", "exec");',
        '   exec(c, msgpack.__dict__);',
        'sys.modules["debellator"] = debellator = imp.new_module("debellator"); setattr(debellator, "__path__", []);',
        'sys.modules["debellator.core"] = core = imp.new_module("debellator.core");',
        'debellator.__dict__["core"] = core;',
        'c = compile(zlib.decompress(base64.b64decode(b"{code}")), "{code_path}", "exec", dont_inherit=True);',
        'exec(c, core.__dict__);',
        'core.main(**core.decode(base64.b64decode(b"{options}")));'
    ]),
    (True, [
        'import os, sys, site, pkg_resources;',
        'venv_path = os.path.expanduser("{venv}");entry = site.getsitepackages([venv_path])[0]',
        'if not os.path.isdir(entry):',
        '   import venv',
        '   venv.create(venv_path, system_site_packages=False, clear=True, symlinks=False, with_pip=True)',
        'sys.prefix = venv_path',
        'sys.path.insert(0, entry);',
        'site.addsitedir(entry);',
        'pkg_resources.working_set.add_entry(entry);',
        'try:',
        '   import msgpack',
        'except ImportError:',
        '   import pip',
        '   pip.main(["install", "--prefix", venv_path, "-q", "msgpack-python"])',
        'import sys, imp, base64, zlib;',
        'try:',
        '   import msgpack;',
        'except ImportError:',
        '   sys.modules["msgpack"] = msgpack = imp.new_module("msgpack");',
        '   c = compile(zlib.decompress(base64.b64decode(b"{msgpack_code}")), "{msgpack_code_path}", "exec");',
        '   exec(c, msgpack.__dict__);',
        'sys.modules["debellator"] = debellator = imp.new_module("debellator"); setattr(debellator, "__path__", []);',
        'sys.modules["debellator.core"] = core = imp.new_module("debellator.core");',
        'debellator.__dict__["core"] = core;',
        'c = compile(zlib.decompress(base64.b64decode(b"{code}")), "{code_path}", "exec", dont_inherit=True);',
        'exec(c, core.__dict__);',
        'core.main(**core.decode(base64.b64decode(b"{options}")));'])
])
def test_remote_bootstrap(venv, result):
    from debellator import master

    remote = master.Remote()

    args = list(remote._iter_bootstrap(venv))
    assert args == result
