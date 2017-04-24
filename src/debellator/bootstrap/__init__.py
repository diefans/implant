import base64
import inspect
import types
import zlib

from .. import core
from . import message_pack, _main, _with_venv


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

        msgpack_code_source = inspect.getsource(message_pack).encode()
        self.msgpack_code_path = 'remote://{}'.format(inspect.getsourcefile(message_pack))
        self.msgpack_code = base64.b64encode(zlib.compress(msgpack_code_source, 9)).decode(),

    def formatsourcelines(self, module):
        lines, _ = inspect.getsourcelines(module)
        for line in lines:
            stripped = line.strip()
            if stripped and not stripped.startswith('#'):
                yield line.format(**self)

    def __iter__(self):
        if self.venv:
            yield from self.formatsourcelines(_with_venv)

        yield from self.formatsourcelines(_main)

    def __str__(self):
        return ''.join(self)