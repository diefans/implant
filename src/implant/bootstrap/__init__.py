# Copyright 2018 Oliver Berger
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Bootstrap of a remote python process."""
import base64
import inspect
import logging
import types
import zlib

import pkg_resources

from .. import core, msgpack


log = logging.getLogger(__name__)


VENV_DEFAULT = '~/.implant'


class Bootstrap(dict):

    """Provide an iterator over the bootstrap code."""

    def __init__(self, code, options=None):
        super(Bootstrap, self).__init__()
        self.__dict__ = self

        if options is None:
            options = {}

        self.options = base64.b64encode(msgpack.encode(options)).decode(),

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

        self.code = base64.b64encode(zlib.compress(code_source, 9)).decode()
        raw_len = len(code_source)
        comp_len = len(self.code)
        log.debug("Code compression ratio of %s -> %s: %.2f%%",
                  raw_len, comp_len, comp_len * 100 / raw_len)

        msgpack_code_source = inspect.getsource(msgpack).encode()
        self.msgpack_code_path = 'remote://{}'.format(
            inspect.getsourcefile(msgpack))
        self.msgpack_code = base64.b64encode(
            zlib.compress(msgpack_code_source, 9)).decode(),

    def formatsourcelines(self, lines):
        """Remove full line comments."""
        # TODO think about using pyminifier
        # lines, _ = inspect.getsourcelines(module)
        for line in map(lambda l: l.decode('utf-8', 'replace'), lines):
            stripped = line.strip()
            if stripped and not stripped.startswith('#'):
                yield line.format(**self)

    def __iter__(self):
        if self.venv:
            _with_venv_fmt = pkg_resources.resource_stream(
                __name__, '_with_venv.py.fmt')
            yield from self.formatsourcelines(_with_venv_fmt.readlines())

        _main_fmt = pkg_resources.resource_stream(__name__, '_main.py.fmt')
        yield from self.formatsourcelines(_main_fmt.readlines())

    def __str__(self):
        return ''.join(self)
