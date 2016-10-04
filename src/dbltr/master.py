"""Controlles a bunch of remotes."""

import os
import sys
import asyncio
import shlex
import base64
import types
import functools
import concurrent.futures
import inspect
import lzma

from collections import namedtuple

from dbltr import core, utils, plugins

from logging import StreamHandler

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
        'c = compile(lzma.decompress(base64.b64decode(b"{code}")), "<string>", "exec");'
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
                # optionally with user
                if self.user is not None:
                    yield '-l'
                    yield self.user
                yield self.host

            # sudo
            if self.sudo is not None:
                yield 'sudo'
                # optionally with user
                if self.sudo is not True:
                    yield '-u'
                    yield self.sudo

            # python exec
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

        command_args = list(_gen())

        return command_args


class Remote(asyncio.subprocess.SubprocessStreamProtocol):

    """
    Embodies a remote python process.
    """

    def __init__(self, *, loop=None, compressor=False, **options):
        if loop is None:
            loop = asyncio.get_event_loop()

        super(Remote, self).__init__(limit=asyncio.streams._DEFAULT_LIMIT, loop=loop)

        self.compressor = compressor

        self.queue_in = asyncio.Queue(loop=self._loop)
        self.queue_out = asyncio.Queue(loop=self._loop)
        self.queue_err = asyncio.Queue(loop=self._loop)
        self.pid = None
        self.returncode = None
        self.terminator = None
        self.teardown = None

        self.finalizer = []

        # a future indicating a receiving remote
        self.receiving = None

        # we distribute channels from stdout
        self.channels = core.Channels(loop=self._loop, compressor=self.compressor)

        # the channel to send everything
        self.channel_out = core.JsonChannel(queue=self.queue_in, loop=self._loop, compressor=self.compressor)
        self._commander = core.Commander(self.channel_out, self.channels.default, self._loop)

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
        self.terminator.set_result(self.returncode)
        core.logger.info("process exited: %s", self.returncode)

    def add_finalizer(self, func):
        self.finalizer.append(func)

    async def process_launched(self):
        self.terminator = asyncio.Future()

        async def teardown():
            exitcode = await self.terminator
            core.logger.debug("TTTTTTTTTT")

            for finalizer in self.finalizer:
                core.logger.info("\t\tteardown: %s", finalizer)
                await finalizer(self)

        self.teardown = asyncio.ensure_future(teardown())

        # setup all plugin commands
        await core.Cmd.local_setup(self, self._loop)
        # for plugin in set(core.Plugin.plugins.values()):
        #     for command in plugin.commands.values():
        #         if command.local_setup:

        #             core.logger.debug("\t\tSetup local plugin: %s, %s", plugin, command)

        #             asyncio.ensure_future(command.local_setup(command, self, self._loop))

        core.logger.info('\n%s', '\n'.join(['-' * 80] * 1))

    async def wait(self):
        """Wait until the process exit and return the process return code.

        This method is a coroutine."""
        await self.teardown
        return (await self._transport._wait())

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
                     loop=None, options=None, **kwargs):
        """Create a remote process."""
        core.logger.info('\n%s', '\n'.join(['+' * 80] * 1))

        if target is None:
            target = Target()

        if loop is None:
            loop = asyncio.get_event_loop()

        if code is None:
            # our default receiver is myself
            code = sys.modules[__name__]

        if isinstance(code, types.ModuleType):
            code = get_python_source(code).encode()

        command_args = target.command_args(code, options=options, python_bin=python_bin)

        remote = cls(loop=loop, **options)
        await loop.subprocess_exec(
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

    async def send(self, input):
        """Send input to remote process."""

        self.stdin.write(input)
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
            log("remote stderr {}: {}".format(self.pid, line[:-1].decode()))
            await self.queue_err.put(line)

    async def receive(self):
        receiving = asyncio.gather(
            asyncio.ensure_future(self._connect_stdin()),
            asyncio.ensure_future(self._connect_stdout()),
            asyncio.ensure_future(self._connect_stderr()),
            asyncio.ensure_future(self._commander.resolve_pending()),
            loop=self._loop
        )

        self.receiving = receiving

        try:
            await self.receiving

        except asyncio.CancelledError:

            log("remote cancel", pid=self.pid)
            self.terminate()
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


async def feed_stdin_to_remotes(**options):
    remote = await Remote.launch(code=core,
                                 python_bin=os.path.expanduser('~/.pyenv/versions/3.5.2/bin/python'),
                                 loop=core.loop, options=options)

    remote_task = asyncio.ensure_future(remote.receive(), loop=core.loop)
    core.logger.info("fooo")
    try:
        async with core.Incomming(pipe=sys.stdin) as reader:
            while True:
                line = await reader.readline()

                core.logger.debug("sending: %s", line)

                # line = await stdin_reader.readline()
                if line is b'':
                    break

                if remote.returncode is None:
                    command, args, kwargs = parse_command(line[:-1].decode())

                    try:
                        cmd = core.Plugin.get(command)
                        if isinstance(cmd, core.Command):
                            # new command execution
                            result = None

                        else:
                            result = await remote.execute(command, *args, **kwargs)

                    except (TypeError, KeyError) as ex:
                        print("< {}\n > ".format(ex), end='')

                    else:
                        print("< {}\n > ".format(result), end='')

                else:
                    await remote.wait()
                    break

            if not remote_task.done():
                remote_task.cancel()
                await remote.wait()

    except asyncio.CancelledError:
        core.logger.info('XXXXXXXXXXXXXXXXXXXXXXX')
        # wait for remote to complete
        remote_task.cancel()
        await remote_task
        if remote.returncode is None:
            await remote.wait()

        await remote_task
        # raise
        # remote.terminate()


class ExecutorConsoleHandler(StreamHandler):

    """Run logging in a separate executor, to not block on console output."""

    def __init__(self, executor):
        self.executor = executor

        super(ExecutorConsoleHandler, self).__init__()

    # TODO FIXME it still occurs...

    def _emit(self, record):
        core.loop.run_in_executor(self.executor, functools.partial(super(ExecutorConsoleHandler, self).emit, record))


def main():
    compressor = 'gzip'

    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as logging_executor:
        logging_handler = ExecutorConsoleHandler(logging_executor)
        core.logger.propagate = False
        core.logger.addHandler(logging_handler)
        # core.logger.setLevel('INFO')

        try:
            core.loop.run_until_complete(
                core.run(
                    feed_stdin_to_remotes(compressor=compressor),
                    loop=core.loop
                )
            )

        finally:
            core.loop.close()
