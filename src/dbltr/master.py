"""Controlles a bunch of remotes."""

import sys
import asyncio
import shlex
import base64
import types
import functools
import concurrent.futures
import inspect

from collections import namedtuple

from dbltr import core, utils

from logging import StreamHandler

log = core.log


def get_python_source(obj):
    return inspect.getsource(obj)


class Target(namedtuple('Target', ('host', 'user', 'sudo'))):

    """A unique representation of a Remote."""

    bootstrap = (
        'import imp, base64, json; boot = imp.new_module("dbltr.msgr");'
        'c = compile(base64.b64decode(b"{code}"), "<string>", "exec");'
        'exec(c, boot.__dict__); boot.main(**boot.decode_options(b"{options}"));'
    )

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
                code=base64.b64encode(code).decode(),
                options=core.encode_options(**options),
            )

        command_args = list(_gen())

        return command_args


class Remote(asyncio.subprocess.SubprocessStreamProtocol):

    """
    Embodies a remote python process.
    """

    targets = {}
    """caches all remotes."""

    def __init__(self, *, loop=None, compressor=False, **options):
        if loop is None:
            loop = asyncio.get_event_loop()

        super(Remote, self).__init__(limit=asyncio.streams._DEFAULT_LIMIT, loop=loop)

        self.compressor = core.get_compressor(compressor)

        self.queue_in = asyncio.Queue(loop=self._loop)
        self.queue_out = asyncio.Queue(loop=self._loop)
        self.queue_err = asyncio.Queue(loop=self._loop)
        self.pid = None
        self.returncode = None

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

    async def wait(self):
        """Wait until the process exit and return the process return code.

        This method is a coroutine."""
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

        return remote

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
            chunk = core.Chunk.decode(line, compressor=self.compressor)
            log("remote stdout", pid=self.pid, chunk=chunk, data=chunk.data)

            await self.channels.distribute(chunk)

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
            asyncio.ensure_future(self._commander()),
            loop=self._loop
        )

        self.receiving = receiving

        try:
            await self.receiving

        except asyncio.CancelledError:

            log("remote cancel", pid=self.pid)
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
    plugins = utils.load_plugins()

    remote = await Remote.launch(code=core, loop=core.loop, options=options)

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
        # wait for remote to complete
        remote_task.cancel()
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

        messenger = core.Messenger(core.loop)

        try:
            core.loop.run_until_complete(
                messenger.run(
                    feed_stdin_to_remotes(compressor=compressor)
                )
            )

        finally:
            core.loop.close()
