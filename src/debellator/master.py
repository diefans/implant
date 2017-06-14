"""Controlles a bunch of remotes."""

import asyncio
import logging
import pathlib
import shlex
import sys
import traceback

from debellator import core, bootstrap

logger = logging.getLogger(__name__)
PLUGINS_ENTRY_POINT_GROUP = 'debellator.plugins'


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

    def command_args(self, *, code=None, options=None, python_bin=sys.executable):
        """Generate the command arguments to execute a python process."""

        bootstrap_code = str(bootstrap.Bootstrap(code, options))

        if self.hostname is not None:
            bootstrap_code = "'{}'".format(bootstrap_code)

        # from pdb import set_trace; set_trace()       # XXX BREAKPOINT

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

            yield str(python_bin)
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

    process = await Remote(
        # hostname='localhost'
    ).launch(
        code=core,
        python_bin=pathlib.Path('~/.pyenv/versions/3.5.2/bin/python').expanduser(),
        options=options
    )

    try:
        # setup launch specific tasks
        io_queues = core.IoQueues()
        await core.Command.local_setup(io_queues)

        remote_com = asyncio.ensure_future(io_queues.communicate(process.stdout, process.stdin))
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
