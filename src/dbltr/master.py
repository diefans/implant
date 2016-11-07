"""Controlles a bunch of remotes."""

import asyncio
import base64
import inspect
import logging
import os
import shlex
import sys
import traceback
import types
import zlib
from collections import namedtuple

from dbltr import core, task, mp

PLUGINS_ENTRY_POINT_GROUP = 'dbltr.plugins'

logger = logging.getLogger(__name__)


class Target(namedtuple('Target', ('host', 'user', 'sudo'))):

    """A unique representation of a Remote."""

    bootstrap = '\n'.join((
        'import sys, imp, base64, zlib;',
        'try:',
        '   import msgpack;',
        'except ImportError:',
        '   sys.modules["msgpack"] = msgpack = imp.new_module("msgpack");',
        '   c = compile(zlib.decompress(base64.b64decode(b"{msgpack_code}")), "{msgpack_code_path}", "exec");',
        '   exec(c, msgpack.__dict__);',

        'sys.modules["dbltr"] = dbltr = imp.new_module("dbltr"); setattr(dbltr, "__path__", []);',
        'sys.modules["dbltr.core"] = core = imp.new_module("dbltr.core");',
        'dbltr.__dict__["core"] = core;',

        'c = compile(zlib.decompress(base64.b64decode(b"{code}")), "{code_path}", "exec", dont_inherit=True);',
        'exec(c, core.__dict__);',

        'core.main(**core.decode(base64.b64decode(b"{options}")));',
    ))
    """Bootstrapping of core module on remote."""

    def __new__(cls, host=None, user=None, sudo=None):
        return super(Target, cls).__new__(cls, host, user, sudo)

    def command_args(self, code, *, options=None, python_bin=sys.executable):
        """Generate the command arguments to execute a python process."""
        if options is None:
            options = {}

        assert isinstance(options, dict), 'options must be a dict'

        if code is None:
            code = sys.modules[__name__]

        if isinstance(code, types.ModuleType):
            code_source = inspect.getsource(code).encode()
            code_path = 'remote://{}'.format(inspect.getsourcefile(code))

        else:
            code_source = code
            code_path = 'remote-string://'

        def _gen():
            # ssh
            if self.host is not None:
                yield 'ssh'
                yield '-T'
                # optionally with user
                if self.user is not None:
                    yield '-l'
                    yield self.user

                # remote port forwarding
                yield '-R'
                yield '10001:localhost:10000'

                yield self.host

            # sudo
            if self.sudo is not None:
                yield 'sudo'
                # optionally with user
                if self.sudo is not True:
                    yield '-u'
                    yield self.sudo

            # python exec
            # yield 'exec'
            yield from shlex.split(python_bin)

            yield '-c'

            if self.host is not None:
                bootstrap = ''.join(("'", self.bootstrap, "'"))
            else:
                bootstrap = self.bootstrap

            msgpack_code = inspect.getsource(mp).encode()
            msgpack_code_path = 'remote://{}'.format(inspect.getsourcefile(mp))

            yield bootstrap.format(
                code=base64.b64encode(zlib.compress(code_source, 9)).decode(),
                code_path=code_path,
                msgpack_code=base64.b64encode(zlib.compress(msgpack_code, 9)).decode(),
                msgpack_code_path=msgpack_code_path,
                options=base64.b64encode(core.encode(options)).decode(),
            )

            # yield ' 2> /tmp/core.log'

        command_args = list(_gen())

        return command_args


class Remote(asyncio.subprocess.SubprocessStreamProtocol):

    """
    Embodies a remote python process.
    """

    def __init__(self):
        super(Remote, self).__init__(
            limit=asyncio.streams._DEFAULT_LIMIT,       # noqa
            loop=asyncio.get_event_loop()
        )

        self.io_queues = core.IoQueues()

        self.pid = None
        self.returncode = None

        # a future indicating a receiving remote
        self.receiving = None

    def connection_made(self, transport):
        super(Remote, self).connection_made(transport)
        self.pid = transport.get_pid()

    def process_exited(self):
        self._transport.close()
        self.returncode = self._transport.get_returncode()

        # we keep transport
        # self._transport = None
        if self.receiving is not None:
            self.receiving.cancel()

    def add_finalizer(self, func):
        self.finalizer.append(func)

    async def process_launched(self):
        # setup all plugin commands for this remote
        await core.Command.local_setup(self.io_queues)

    async def wait(self):
        """Wait until the process exit and return the process return code.

        This method is a coroutine."""
        return await self._transport._wait()        # noqa

    def send_signal(self, signal):
        self._transport.send_signal(signal)

    def terminate(self):
        self._transport.terminate()

    def kill(self):
        self._transport.kill()

    @classmethod
    async def launch(cls, target=None, *, python_bin=sys.executable, code=None, options=None, **kwargs):
        """Create a remote process."""

        if target is None:
            target = Target()

        command_args = target.command_args(code, options=options, python_bin=python_bin)

        remote = cls()
        await asyncio.get_event_loop().subprocess_exec(
            lambda: remote,
            *command_args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            **kwargs
        )

        try:
            return remote

        finally:
            await remote.process_launched()

    async def _connect_stderr(self):
        """Collect error messages from remote in stderr queue."""

        async for line in self.stderr:
            logger.debug("\tRemote #%s: %s", self.pid, line[:-1].decode())

    async def communicate(self):
        """Start channel communication."""

        receiving = asyncio.gather(
            asyncio.ensure_future(core.Channel.communicate(self.io_queues, self.stdout, self.stdin)),
            asyncio.ensure_future(self._connect_stderr()),
        )

        self.receiving = receiving

        try:
            await self.receiving

        except asyncio.CancelledError:
            try:
                self.terminate()

            except ProcessLookupError:
                pass

        finally:
            self.receiving = None
            self._transport.close()


def parse_command(line):
    """Parse a command from line."""

    args = []
    kwargs = {}
    command, *parts = line.split(' ')

    for part in parts:
        if '=' in part:
            k, v = part.split('=')
            kwargs[k] = v

        else:
            args.append(part)

    return command, args, kwargs


async def _execute_command(io_queues, line):
    command_name, _, params = parse_command(line[:-1].decode())
    print("sending:", command_name, params)

    try:
        cmd = core.Command.create_command(io_queues, command_name, **params)
        result = await cmd

    except Exception as ex:     # noqa
        logger.error("Error:\n%s", traceback.format_exc())
        print("Error: {}\n > ".format(ex))

    else:
        return result


async def feed_stdin_to_remotes(**options):

    default_lines = {
        b'\n': b'dbltr.plugins.core:Echo foo=bar bar=123\n',
        b'i\n': b'dbltr.core:InvokeImport fullname=dbltr.plugins.core\n',
    }

    remote = await Remote.launch(
        code=core,
        target=Target(host='localhost'),
        python_bin=os.path.expanduser('~/.pyenv/versions/3.5.2/bin/python'),
        options=options
    )

    # start remote communication
    asyncio.ensure_future(remote.communicate())

    try:
        async with core.Incomming(pipe=sys.stdin) as reader:
            while True:
                line = await reader.readline()

                if line is b'':
                    break

                if line in default_lines:
                    line = default_lines[line]

                if remote.returncode is None:
                    result = await _execute_command(remote.io_queues, line)
                    # result = await asyncio.ensure_future(_execute_command(remote, line))
                    # result = await asyncio.gather(
                    #     _execute_command(remote.io_queues, line),
                    #     _execute_command(remote.io_queues, line),
                    # )

                    print("< {}\n > ".format(result), end='')

    except asyncio.CancelledError:
        pass

    if remote.returncode is None:
        remote.terminate()
        await remote.wait()


async def serve_tcp_10000(reader, writer):
    try:
        while True:
            writer.write(b"Hello World\n")
            await asyncio.sleep(1)

    except asyncio.CancelledError:
        writer.close()


async def print_debug(loop):
    while True:
        print(chr(27) + "[2J")  # clear screen
        loop.print_debug_info()
        await asyncio.sleep(0.5, loop=loop)


def main(debug=False, log_config=None):
    loop = asyncio.get_event_loop()

    options = {
        'debug': loop.get_debug(),
        'log_config': log_config,
    }

    if debug:
        logger.setLevel(logging.DEBUG)

    try:
        loop.run_until_complete(
            core.run(
                # print_debug(loop),
                # asyncio.start_server(serve_tcp_10000, 'localhost', 10000),
                feed_stdin_to_remotes(**options),
            )
        )

        core.cancel_pending_tasks(loop)

    finally:
        loop.close()
