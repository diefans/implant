"""
unify messenger

resources:
- https://pymotw.com/3/asyncio/subprocesses.html
- http://stackoverflow.com/questions/24435987/how-to-stream-stdout-stderr-from-a-child-process-using-asyncio-and-obtain-its-e
- http://stackoverflow.com/questions/375427/non-blocking-read-on-a-subprocess-pipe-in-python/20697159#20697159
- https://github.com/python/asyncio/blob/master/examples/child_process.py



      ctrl         <---->               rcvr
? -> queue out      --->       -> pipe in -> queue in
                                               |
                                               +--> channeller
                                                      |
                                                      +--> channel 1 queue
                                                      |
                                                      +--> channel x queue

? <- queue in       <---       <- pipe out <- queue out
"""

import os
import atexit
import functools
import sys
import base64
import uuid
import asyncio
import shlex
import signal
import types
import logging

from collections import namedtuple


__all__ = ['loop', 'stdin', 'stdout']


logging.basicConfig()
LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)


LOG.debug('init')


class AsyncOut:
    def __init__(self):
        self._buffer = bytearray()
        self._new_data = None

    def _wakeup_new_data(self):
        future = self._new_data
        if future is not None:
            self._new_data = None
            if not future.cancelled():
                future.set_result(None)

    def write(self, data):
        if isinstance(data, bytes):
            self._buffer.extend(data)
        else:
            self._buffer.extend(data.encode())

        self._wakeup_new_data()



def log(_msg, **kwargs):
    colors = {
        'color': '\033[01;31m',
        'nocolor': '\033[0m'
    }
    if kwargs:
        error_msg = "{color}{msg} {kwargs}{nocolor}".format(msg=_msg, kwargs=kwargs, **colors)

    else:
        error_msg = "{color}{msg}{nocolor}".format(msg=_msg, **colors)

    # print(error_msg, file=sys.stderr, flush=True)
    LOG.debug(error_msg)


shutdown_tasks = set()


def create_loop(*, debug=False):
    """Create the appropriate loop."""

    if sys.platform == 'win32':
        loop = asyncio.ProactorEventLoop()  # for subprocess' pipes on Windows
        asyncio.set_event_loop(loop)

    else:
        loop = asyncio.get_event_loop()

    loop.set_debug(debug)

    # close loop on exit
    atexit.register(loop.close)
    return loop


# loop is global
loop = create_loop(debug=True)

# pipes are global so we have global queues
stdin = asyncio.Queue(loop=loop)
stdout = asyncio.Queue(loop=loop)
stderr = asyncio.Queue(loop=loop)

# we trigger exit by future
wants_exit = asyncio.Future(loop=loop)


# cancel tasks on exit
def cancel_tasks(future):
    # for task in asyncio.Task.all_tasks():
    for task in shutdown_tasks:
        task.cancel()
wants_exit.add_done_callback(cancel_tasks)

# exit on sigterm or sigint
for signame in ('SIGINT', 'SIGTERM'):
    loop.add_signal_handler(
        getattr(signal, signame),
        functools.partial(wants_exit.set_result, None)
    )


async def setup_stdio(loop):
    async def queue_read_pipe(queue, reader):
        try:
            while True:
                line = await reader.readline()
                log("<<< lin", line=line)
                if line is b'':
                    break
                await queue.put(line)

        except asyncio.CancelledError:
            reader._transport.close()

    async def queue_write_pipe(queue, writer):
        try:
            while True:
                data = await queue.get()
                writer.write(b"write..." + data)
                await writer.drain()
                queue.task_done()

        except asyncio.CancelledError:
            writer.close()

    # first connect writer
    stdout_transport, stdout_protocol = await loop.connect_write_pipe(
        asyncio.streams.FlowControlMixin, os.fdopen(sys.stdin.fileno(), 'wb')
    )
    stdout_writer = asyncio.streams.StreamWriter(stdout_transport, stdout_protocol, None, loop)

    # connect reader
    stdin_reader = asyncio.StreamReader(loop=loop)
    stdin_protocol = asyncio.StreamReaderProtocol(stdin_reader)

    stdin_transport, _ = await loop.connect_read_pipe(
        lambda: stdin_protocol, os.fdopen(sys.stdin.fileno(), 'r')
    )

    # stdio_tasks = [
    asyncio.ensure_future(queue_read_pipe(stdin, stdin_reader)),
    asyncio.ensure_future(queue_write_pipe(stdout, stdout_writer)),
    # ]

    # only make stderr async if no tty
    if not sys.stderr.isatty():
        stderr_transport, stderr_protocol = await loop.connect_write_pipe(
            asyncio.streams.FlowControlMixin, os.fdopen(sys.stderr.fileno(), 'wb')
        )
        stderr_writer = asyncio.streams.StreamWriter(stderr_transport, stderr_protocol, None, loop)

        # stdio_tasks.append(
        asyncio.ensure_future(queue_write_pipe(stderr, stderr_writer))
        # )

    # future = asyncio.gather(
    #     *stdio_tasks,
    #     loop=loop
    # )
    # shutdown_tasks.add(future)
    # await future


class Message(bytes):

    """
    A Message is a base64 encoded payload.
    """

    limit = 1024

    def __new__(cls, data, channel=None, uid=None, complete=True):
        return super(Message, cls).__new__(cls, data)

    def __init__(self, data, channel=None, uid=None, complete=True):
        super(Message, self).__init__()

        # enforce None for empty strings
        self.channel = channel or None
        self.uid = uid or uuid.uuid1()
        self.complete = complete

    def __hash__(self):
        return self.uid.int

    def __eq__(self, other):
        if isinstance(other, Message):
            return hash(self) == hash(other)

        return bytes(self) == other

    def __repr__(self):
        return b':'.join((
            self.channel or b'',
            self.uid.hex.encode(),
            bytes(self)
        )).decode()

    @classmethod
    def from_line(cls, line):
        """Create a message from a protocol line.

        incomplete message: <channel>:<uid>:<payload>\n
        complete message: <channel>:<uid>:<payload>:\n

        """

        try:
            channel, uid, payload, *complete = line[:-1].split(b':')
            uid = uuid.UUID(bytes(uid).decode())
            data = base64.b64decode(payload)

        except ValueError:
            # not enough colons
            # log("\nbroken message protocol!", line=line)
            uid = None
            channel = None
            data = line[:-1]
            complete = True

        else:
            complete = bool(complete)
            channel = bytes(channel)

        message = Message(
            data,
            channel=channel,
            uid=uid,
            complete=complete
        )

        return message

    def split_size(self, size=None):
        """Split a string into pieces."""

        size = size or self.limit

        if size < 1:
            raise ValueError('size must be greater than 0', size)

        length = len(self)
        remains = length % size

        def iterator():
            for i in (i * size for i in range(length // size)):
                yield self[i: i + size]

            if length < size:
                yield self

            elif remains:
                yield self[i + size: i + size + remains]

        return (length // size) + int(bool(remains)), iterator()

    def iter_lines(self):
        """Iterate over the splitted message if payload size is greater than limit."""

        channel = self.channel or b''
        uid = bytes(self.uid.hex, 'ascii')
        length, data = self.split_size()

        for i, chunk in enumerate(data):
            if i + 1 < length:
                yield b':'.join((channel, uid, base64.b64encode(chunk))) + b'\n'

            else:
                yield b':'.join((channel, uid, base64.b64encode(chunk), b'\n'))


def get_python_source(obj):
    import inspect
    return inspect.getsource(obj)


class Target(namedtuple('Target', ('host', 'user', 'sudo'))):

    """A unique representation of a Remote."""

    bootstrap = (
        'import imp, base64; boot = imp.new_module("dbltr.msgr");'
        'c = compile(base64.b64decode(b"{code}"), "<string>", "exec");'
        'exec(c, boot.__dict__); boot.main(False);'
    )

    def __new__(cls, host=None, user=None, sudo=None):
        return super(Target, cls).__new__(cls, host, user, sudo)

    def command_args(self, code, python_bin='/usr/bin/python3'):
        """generates the command argsuments to execute a python process"""

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
            # yield '-u'
            yield '-c'

            if self.host is not None:
                bootstrap = ''.join(("'", self.bootstrap, "'"))
            else:
                bootstrap = self.bootstrap

            yield bootstrap.format(code=base64.b64encode(code).decode())

        command_args = list(_gen())

        return command_args


class Remote(asyncio.subprocess.Process):

    """
    Embodies a remote python process.
    """

    targets = {}
    """caches all remotes."""

    @classmethod
    async def launch(cls,
                     target=None,
                     # host=None, user=None, sudo=None,
                     python_bin='/usr/bin/python3', code=None,
                     loop=None, **kwargs):
        """Create a remote process."""

        if loop is None:
            loop = asyncio.get_event_loop()

        # protocol = SubprocessMessageStreamProtocol(limit=asyncio.streams._DEFAULT_LIMIT, loop=loop)
        protocol = asyncio.subprocess.SubprocessStreamProtocol(limit=asyncio.streams._DEFAULT_LIMIT, loop=loop)

        # FIXME remove testing python bin
        # python_bin = python_bin or "/home/olli/.pyenv/versions/debellator3/bin/python"

        if code is None:
            # our default receiver is myself
            code = sys.modules[__name__]

        if isinstance(code, types.ModuleType):
            code = get_python_source(code).encode()

        command_args = target.command_args(code, python_bin=python_bin)

        transport, _ = await loop.subprocess_exec(
            lambda: protocol,
            *command_args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            **kwargs
        )
        process = cls(transport, protocol, loop)

        log('launched process', pid=process.pid
                   # , args=command_args
                   )

        # connect pipes
        asyncio.ensure_future(process.receive(), loop=loop)
        return process

    async def send(self, input):
        """Send input to remote process."""

        self.stdin.write(input)
        await self.stdin.drain()

    async def _connect_stdin(self):
        """Send messesges from tx queue to remote."""

        try:
            while True:
                data = await self.tx.get()

                if isinstance(data, Message):
                    for line in data.iter_lines():
                        await self.send(line)
                else:
                    await self.send(data)

                self.tx.task_done()
        except asyncio.CancelledError:
            self.stdin._transport.close()

    async def _connect_stdout(self):
        """Collect messages from remote in rx queue."""

        try:
            async for line in self.stdout:
                message = Message.from_line(line)
                log(">>> remote out", message=message, line=line)
                await self.rx.put(message)
                log('*** remote incomming queue size', size=self.rx.qsize())
        except asyncio.CancelledError:
            self.stdout._transport.close()

    async def _connect_stderr(self):
        """Collect error messages from remote in ex queue."""

        try:
            async for message in self.stderr:
                log(">>> remote error", message=message)
                await self.ex.put(message)
        except asyncio.CancelledError:
            self.stderr._transport.close()

    def receive(self):
        future = asyncio.gather(
            asyncio.ensure_future(self._connect_stdin()),
            asyncio.ensure_future(self._connect_stdout()),
            asyncio.ensure_future(self._connect_stderr()),
            loop=loop
        )
        shutdown_tasks.add(future)

        return future

    def __init__(self, *args, **kwargs):
        super(Remote, self).__init__(*args, **kwargs)

        self.rx = asyncio.Queue(loop=self._loop)
        self.tx = asyncio.Queue(loop=self._loop)
        self.ex = asyncio.Queue(loop=self._loop)


async def echo():
    try:
        while True:
            message = await stdin.get()
            await stdout.put(message)
            stdin.task_done()
    except asyncio.CancelledError:
        log('*** shutdown echo')


async def send_stdin_to_remote(loop, target):
    """Take a message and send it to a remote."""
    remotes = {}

    try:
        while True:
            if target in remotes:
                remote = remotes[target]

            else:
                remote = remotes[target] = await Remote.launch(
                    target, loop=loop, python_bin=sys.executable #'/home/olli/.pyenv/versions/debellator3/bin/python'
                )

            message = await stdin.get()

            log("*** processing remote", message=message)
            await stdout.put(message)
            # await log("size", size=stderr.qsize())
            await remote.tx.put(message)
            stdin.task_done()

    except asyncio.CancelledError:
        log('*** shutdown echo_remote')


async def run(master=True):
    stdio_task = asyncio.ensure_future(setup_stdio(loop), loop=loop)
    shutdown_tasks.add(stdio_task)

    if master:
        send_stdin_task = asyncio.ensure_future(send_stdin_to_remote(loop, Target()), loop=loop)
        shutdown_tasks.add(send_stdin_task)
    else:
        echo_task = asyncio.ensure_future(echo(), loop=loop)
        shutdown_tasks.add(echo_task)

    await wants_exit


def main(master=True):
    # TODO evaluate args
    # print(sys.argv)

    # loop.run_until_complete(setup_queues(loop))
    loop.run_until_complete(run(master))

    # see http://stackoverflow.com/questions/27796294/when-using-asyncio-how-do-you-allow-all-running-tasks-to-finish-before-shutting
    pending = asyncio.Task.all_tasks()
    loop.run_until_complete(asyncio.gather(*pending))
