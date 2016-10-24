"""Controlles a bunch of remotes."""

import os
import sys
import asyncio
import shlex
import base64
import types
import traceback
import functools
import concurrent.futures
import inspect
import lzma

from collections import namedtuple
from logging import StreamHandler

from dbltr import core, utils


log = core.log


def get_python_source(obj):
    return inspect.getsource(obj)


class Target(namedtuple('Target', ('host', 'user', 'sudo'))):

    """A unique representation of a Remote."""

    bootstrap = (
        'import sys, imp, base64, json, lzma;'
        'sys.modules["dbltr"] = dbltr = imp.new_module("dbltr");'
        'sys.modules["dbltr.core"] = core = imp.new_module("dbltr.core");'
        'dbltr.__dict__["core"] = core;'
        'c = compile(lzma.decompress(base64.b64decode(b"{code}")), "<dbltr.core>", "exec");'
        'exec(c, core.__dict__); core.main(**core.decode_options(b"{options}"));'
    )
    """Bootstrapping of core module on remote."""

    def __new__(cls, host=None, user=None, sudo=None):
        return super(Target, cls).__new__(cls, host, user, sudo)

    def command_args(self, code, options=None, python_bin=sys.executable):
        """generates the command argsuments to execute a python process"""

        if options is None:
            options = {}

        assert isinstance(options, dict), 'options must be a dict'

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

            yield bootstrap.format(
                code=base64.b64encode(lzma.compress(code)).decode(),
                options=core.encode_options(**options),
            )

            # yield ' 2> /tmp/core.log'

        command_args = list(_gen())

        return command_args


class Remote(asyncio.subprocess.SubprocessStreamProtocol):

    """
    Embodies a remote python process.
    """

    def __init__(self, **options):
        super(Remote, self).__init__(limit=asyncio.streams._DEFAULT_LIMIT, loop=asyncio.get_event_loop())

        self.io_queues = core.IoQueues()

        self.queue_in = self.io_queues.send
        self.queue_out = self.io_queues.receive
        self.pid = None
        self.returncode = None

        # a future indicating a receiving remote
        self.receiving = None

    @utils.reify
    def execute(self):
        return self._commander.execute

    def connection_made(self, transport):
        super(Remote, self).connection_made(transport)
        self.pid = transport.get_pid()

        log('launched process', pid=self.pid)

    def process_exited(self):
        self._transport.close()
        self.returncode = self._transport.get_returncode()

        # we keep transport
        # self._transport = None
        if self.receiving is not None:
            self.receiving.cancel()

        # inform future
        core.logger.info("process exited: %s", self.returncode)

    def add_finalizer(self, func):
        self.finalizer.append(func)

    async def process_launched(self):
        # setup all plugin commands for this remote
        await core.Command.local_setup(self.io_queues)
        core.logger.info('\n%s', '\n'.join(['-' * 80] * 1))

    async def wait(self):
        """Wait until the process exit and return the process return code.

        This method is a coroutine."""
        return await self._transport._wait()

    def send_signal(self, signal):
        self._transport.send_signal(signal)

    def terminate(self):
        self._transport.terminate()

    def kill(self):
        self._transport.kill()

    @classmethod
    async def launch(cls,
                     target=None,
                     # host=None, user=None, sudo=None,
                     python_bin=sys.executable, code=None,
                     options=None, **kwargs):
        """Create a remote process."""
        core.logger.info('\n%s', '\n'.join(['+' * 80] * 1))

        if target is None:
            target = Target()

        if code is None:
            # our default receiver is myself
            code = sys.modules[__name__]

        if isinstance(code, types.ModuleType):
            code = get_python_source(code).encode()

        command_args = target.command_args(code, options=options, python_bin=python_bin)

        remote = cls(**options)
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

    async def send(self, data):
        """Send input to remote process."""

        self.stdin.write(data)
        await self.stdin.drain()

    async def _connect_stdin(self):
        """Send messesges from stdin queue to remote."""

        while True:
            data = await self.queue_in.get()
            await self.send(data)

            self.queue_in.task_done()

    async def _connect_stdout(self):
        """Collect messages from remote in stdout queue."""

        async for line in self.stdout:
            log("remote stdout", pid=self.pid, line=line)

            await self.channels.distribute(line)

    async def _connect_stderr(self):
        """Collect error messages from remote in stderr queue."""

        async for line in self.stderr:
            log("\t\tremote stderr {}: {}".format(self.pid, line[:-1].decode()))

    async def receive(self):

        receiving = asyncio.gather(
            asyncio.ensure_future(core.Channel.communicate(self.io_queues, self.stdout, self.stdin)),
        #     self.io_queues.send_to_writer(self.stdin),
        #     # asyncio.ensure_future(self._connect_stdin()),
        #     # asyncio.ensure_future(self._connect_stdout()),
            asyncio.ensure_future(self._connect_stderr()),
        #     # asyncio.ensure_future(self._commander.resolve_pending()),
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


async def _execute_command(remote, line):
    command_name, args, params = parse_command(line[:-1].decode())

    try:
        cmd = core.Command.create_command(remote.io_queues, command_name, **params)
        result = await cmd

    except Exception as ex:
        core.logger.error("Error:\n%s", traceback.format_exc())
        print("Error: {}\n > ".format(ex))

    else:
        return result


async def feed_stdin_to_remotes(**options):

    remote = await Remote.launch(code=core,
                                 target=Target(host='localhost'),
                                 python_bin=os.path.expanduser('~/.pyenv/versions/3.5.2/bin/python'),
                                 options=options)

    remote_task = asyncio.ensure_future(remote.receive())
    try:
        async with core.Incomming(pipe=sys.stdin) as reader:
            while True:
                line = await reader.readline()

                if line is b'':
                    break

                if not line[:-1]:
                    line = b'debellator#core:Echo foo=bar bar=123\n'

                if line == b'i\n':
                    line = b'dbltr.core:Import plugin_name=debellator#core\n'

                core.logger.debug("sending: %s", line)

                if remote.returncode is None:
                    result = await asyncio.gather(
                        _execute_command(remote, line),
                        _execute_command(remote, line),
                    )

                    print("< {}\n > ".format(result), end='')

    except asyncio.CancelledError:
        pass

    if remote.returncode is None:
        remote.terminate()
        await remote.wait()


class ExecutorConsoleHandler(StreamHandler):

    """Run logging in a separate executor, to not block on console output."""

    def __init__(self, executor):
        self.executor = executor

        super(ExecutorConsoleHandler, self).__init__()

    # TODO FIXME it still occurs...

    def emit(self, record):
        asyncio.get_event_loop().run_in_executor(
            self.executor, functools.partial(super(ExecutorConsoleHandler, self).emit, record)
        )


async def serve_tcp_10000(reader, writer):
    try:
        while True:
            writer.write(b"Hello World\n")
            await asyncio.sleep(1)

    except asyncio.CancelledError:
        writer.close()


def main():
    loop = core.create_loop(debug=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as logging_executor:
        logging_handler = ExecutorConsoleHandler(logging_executor)
        core.logger.propagate = False
        core.logger.addHandler(logging_handler)
        # core.logger.setLevel('INFO')

        try:
            loop.run_until_complete(
                core.run(
                    # asyncio.start_server(serve_tcp_10000, 'localhost', 10000),
                    feed_stdin_to_remotes(),
                )
            )

            core.cancel_pending_tasks()

            core.logger.info("* FINAL *".join('*' * 10))

        finally:
            loop.close()
