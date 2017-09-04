"""Bootstrap of a remote python process."""
import base64
import inspect
import types
import zlib

import pkg_resources
import umsgpack

from .. import core


VENV_DEFAULT = '~/.debellator'


class Bootstrap(dict):
    def __init__(self, code, options=None):
        super(Bootstrap, self).__init__()
        self.__dict__ = self

        if options is None:
            options = {}

        self.options = base64.b64encode(core.encode(options)).decode(),

        venv = options.get('venv')
        self.venv = VENV_DEFAULT if venv is True\
            else None if venv is False\
            else venv

        if isinstance(code, types.ModuleType):
            code_source = inspect.getsource(code).encode()
            self.code_path = 'remote://{}'.format(inspect.getsourcefile(code))

        else:
            code_source = code
            self.code_path = 'remote-string://'

        self.code = base64.b64encode(zlib.compress(code_source, 9)).decode(),

        msgpack_code_source = inspect.getsource(umsgpack).encode()
        self.msgpack_code_path = 'remote://{}'.format(inspect.getsourcefile(umsgpack))
        self.msgpack_code = base64.b64encode(zlib.compress(msgpack_code_source, 9)).decode(),

    def formatsourcelines(self, lines):
        # lines, _ = inspect.getsourcelines(module)
        for line in map(lambda l: l.decode('utf-8', 'replace'), lines):
            stripped = line.strip()
            if stripped and not stripped.startswith('#'):
                yield line.format(**self)

    def __iter__(self):
        if self.venv:
            _with_venv_fmt = pkg_resources.resource_stream(__name__, '_with_venv.py.fmt')
            yield from self.formatsourcelines(_with_venv_fmt.readlines())

        _main_fmt = pkg_resources.resource_stream(__name__, '_main.py.fmt')
        yield from self.formatsourcelines(_main_fmt.readlines())

    def __str__(self):
        return ''.join(self)
