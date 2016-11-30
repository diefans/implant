"""Controlles a bunch of remotes."""

import asyncio
import base64
import inspect
import logging
import shlex
import sys
import traceback
import types
import zlib

from debellator import core, mp

logger = logging.getLogger(__name__)
PLUGINS_ENTRY_POINT_GROUP = 'debellator.plugins'
VENV_DEFAULT = '~/.debellator'


class ProcessNotLaunchError(Exception):
    pass


class MetaRemote(type):
    processes = {}


class Remote(metaclass=MetaRemote):

    """A unique representation of a Remote."""

    def __init__(self, hostname=None, user=None, sudo=None):
        self.hostname = hostname
        self.user = user
        self.sudo = sudo

    def __hash__(self):
        return hash((self.hostname, self.user, self.sudo))

    def __eq__(self, other):
        assert isinstance(other, Remote)
        return hash(self) == hash(other)

    def _iter_bootstrap(self, venv):
        """Bootstrapping of core module on remote."""
        if self.hostname is not None:
            yield "'"

        if venv:
            yield from (
                'import os, sys, site, pkg_resources;',
                'venv_path = os.path.expanduser("{venv}");'
                'entry = site.getsitepackages([venv_path])[0]',

                # create venv if missing
                'if not os.path.isdir(entry):',
                '   import venv',
                '   venv.create(venv_path, system_site_packages=False, clear=True, symlinks=False, with_pip=True)',

                # insert venv at first position
                # pkg_resources is not adding site-packages if there is no distribution
                'sys.prefix = venv_path',
                'sys.path.insert(0, entry);',
                'site.addsitedir(entry);',
                'pkg_resources.working_set.add_entry(entry);',

                # pip should come from venv now
                'try:',
                '   import msgpack',
                'except ImportError:',
                # try to install msgpack
                '   import pip',
                # TODO use ssh port forwarding to install via master
                '   pip.main(["install", "--prefix", venv_path, "-q", "msgpack-python"])',
            )

        yield from (
            'import sys, imp, base64, zlib;',
            # just a msgpack fallback if no venv is used or msgpack somehow failed to install
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

            'core.main(**core.decode(base64.b64decode(b"{options}")));',
        )

        if self.hostname is not None:
            yield "'"

    def command_args(self, *, code=None, options=None, python_bin=sys.executable):
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

        msgpack_code = inspect.getsource(mp).encode()
        msgpack_code_path = 'remote://{}'.format(inspect.getsourcefile(mp))

        venv = options.get('venv')
        venv = VENV_DEFAULT if venv is True\
            else None if venv is False\
            else venv

        bootstrap_code = '\n'.join(self._iter_bootstrap(venv)).format(
            venv=venv,
            code=base64.b64encode(zlib.compress(code_source, 9)).decode(),
            code_path=code_path,
            msgpack_code=base64.b64encode(zlib.compress(msgpack_code, 9)).decode(),
            msgpack_code_path=msgpack_code_path,
            options=base64.b64encode(core.encode(options)).decode(),
        )

        def _gen():
            # ssh
            if self.hostname is not None:
                yield 'ssh'
                yield '-T'
                # optionally with user
                if self.user is not None:
                    yield '-l'
                    yield self.user

                # remote port forwarding
                yield '-R'
                yield '10001:localhost:10000'

                yield self.hostname

            # sudo
            if self.sudo:
                yield 'sudo'
                # optionally with user
                if self.sudo is not True:
                    yield '-u'
                    yield self.sudo

            yield from shlex.split(python_bin)
            yield '-c'
            yield bootstrap_code

            # yield ' 2> /tmp/core.log'

        command_args = list(_gen())

        return command_args

    async def launch(self, *, code=None, options=None, python_bin=sys.executable, **kwargs):
        """Launch a remote process.

        :param code: the python module to bootstrap
        :param options: options to send to remote
        :param python_bin: the path to the python binary to execute
        :param kwargs: further arguments to create the process

        """
        command_args = self.command_args(code=code, options=options, python_bin=python_bin)

        return await self._launch(*command_args, **kwargs)

    async def _launch(self, *args, **kwargs):
        process = await asyncio.create_subprocess_exec(
            *args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            **kwargs
        )

        # cache process
        self.__class__.processes[self] = process

        return process

    async def relaunch(self):
        """Wait until terminated and start a new process with the same args."""
        process = self.__class__.processes.get(self)
        if not process:
            raise ProcessNotLaunchedError()

        command_args = process._transport._proc.args

        process.terminate()
        await process.wait()

        return await self._launch(*command_args, **kwargs)


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
        logger.debug("execute loop: %s", id(asyncio.Task.current_task()._loop))
        result = await cmd.execute()

    except Exception as ex:     # noqa
        logger.error("Error:\n%s", traceback.format_exc())
        print("Error: {}\n > ".format(ex))

    else:
        return result


async def log_remote_stderr(remote):
    async for line in remote.stderr:
        logger.debug("\tRemote #%s: %s", remote.pid, line[:-1].decode())


async def feed_stdin_to_remotes(**options):

    default_lines = {
        b'\n': b'debellator.plugins.core:Echo foo=bar bar=123\n',
        b'i\n': b'debellator.core:InvokeImport fullname=debellator.plugins.core\n',
    }

    process = await Remote(hostname='localhost').launch(
        code=core,
        python_bin='~/.pyenv/versions/3.5.2/bin/python',
        options=options
    )

    try:
        # setup launch specific tasks
        io_queues = core.IoQueues()
        await core.Command.local_setup(io_queues)

        remote_com = asyncio.ensure_future(core.Channel.communicate(io_queues, process.stdout, process.stdin))
        remote_err = asyncio.ensure_future(log_remote_stderr(process))

        async with core.Incomming(pipe=sys.stdin) as reader:
            while True:
                line = await reader.readline()

                if line is b'':
                    break

                if line in default_lines:
                    line = default_lines[line]

                if process.returncode is None:
                    result = await _execute_command(io_queues, line)
                    # result = await asyncio.gather(
                    #     _execute_command(io_queues, line),
                    #     _execute_command(io_queues, line),
                    # )

                    print("< {}\n > ".format(result), end='')

    except asyncio.CancelledError:
        pass

    if process.returncode is None:
        process.terminate()
        await process.wait()


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
        'debug': debug,
        'log_config': log_config,
        # 'venv': False,
        'venv': True,
        # 'venv': '~/.debellator',
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
