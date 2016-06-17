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

import atexit
import sys
import base64
import uuid
import asyncio
import shlex
import signal
import types
from collections import namedtuple


def cancel_tasks():
    for task in asyncio.Task.all_tasks():
        task.cancel()


def create_loop(*, debug=False):
    """Create the appropriate loop."""

    if sys.platform == 'win32':
        loop = asyncio.ProactorEventLoop()  # for subprocess' pipes on Windows
        asyncio.set_event_loop(loop)

    else:
        loop = asyncio.get_event_loop()

    loop.set_debug(debug)

    # cancel tasks on exit
    for signame in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(
            getattr(signal, signame),
            cancel_tasks
        )

    # close loop on exit
    atexit.register(loop.close)
    return loop


# loop is global
loop = create_loop(debug=True)


def log(_msg, **kwargs):
    if kwargs:
        print("{msg} {kwargs}".format(msg=_msg, kwargs=kwargs), file=sys.stderr, flush=True)

    else:
        print("{msg}".format(msg=_msg), file=sys.stderr, flush=True)


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
                log("user ssh", host=self.host, user=self.user)
                yield 'ssh'
                # optionally with user
                if self.user is not None:
                    yield '-l'
                    yield self.user
                yield self.host

            # sudo
            if self.sudo is not None:
                log("user sudo", sudo=self.sudo)
                yield 'sudo'
                # optionally with user
                if self.sudo is not True:
                    yield '-u'
                    yield self.sudo

            # python exec
            log('user python', python=python_bin)
            yield from shlex.split(python_bin)
            yield '-u'
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

        transport, _ = await loop.subprocess_exec(
            lambda: protocol,
            *target.command_args(code, python_bin=python_bin),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            **kwargs
        )
        process = cls(transport, protocol, loop)

        log('launched process', pid=process.pid)

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
                log("out", message=message, line=line)
                await self.rx.put(message)
        except asyncio.CancelledError:
            self.stdout._transport.close()

    async def _connect_stderr(self):
        """Collect error messages from remote in ex queue."""

        try:
            async for message in self.stderr:
                log("err", message=message)
                await self.ex.put(message)
        except asyncio.CancelledError:
            self.stderr._transport.close()

    def receive(self):
        future = asyncio.gather(
            self._connect_stdin(),
            self._connect_stdout(),
            self._connect_stderr(),
        )

        return future

    def __init__(self, *args, **kwargs):
        super(Remote, self).__init__(*args, **kwargs)

        self.rx = asyncio.Queue(loop=self._loop)
        self.tx = asyncio.Queue(loop=self._loop)
        self.ex = asyncio.Queue(loop=self._loop)


class Messenger:
    def __init__(self, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self.rx = asyncio.Queue(loop=self._loop)
        self.tx = asyncio.Queue(loop=self._loop)
        self.ex = asyncio.Queue(loop=self._loop)

    async def _connect_stdin(self):
        """Transform each line in stdin into a `Message`"""

        reader = asyncio.StreamReader(loop=self._loop)
        protocol = asyncio.StreamReaderProtocol(reader)

        transport, _ = await self._loop.connect_read_pipe(lambda: protocol, sys.stdin)

        try:
            while True:
                line = await reader.readline()

                if line is b'':
                    # eof
                    break
                # put message into rx queue
                message = Message.from_line(line)

                await self.rx.put(message)
                log("in:", line=line, message=message, size=self.rx.qsize())

        except asyncio.CancelledError:
            transport.close()

    async def send(self, data):

        pass

    async def log(self, log):
        pass

    async def _connect_stdout(self):
        """Transform each `Message` in tx into one or more lines to stdout."""
        transport, _ = await self._loop.connect_write_pipe(lambda: asyncio.Protocol(), sys.stdout)

        try:
            while True:
                data = await self.tx.get()

                log("stdout:", msg=data)
                if isinstance(data, Message):
                    for line in data.iter_lines():
                        transport.write(line)
                        # sys.stdout.buffer.write(line)
                else:
                    transport.write(data)
                    # sys.stdout.buffer.write(data)

                self.tx.task_done()
        except asyncio.CancelledError:
            transport.close()

    async def _connect_stderr(self):
        """Stream ex queue to stderr."""

        try:
            while True:
                data = await self.ex.get()
                sys.stderr.buffer.write(data)
                self.ex.task_done()
        except asyncio.CancelledError:
            pass

    def receive(self):
        future = asyncio.gather(
            self._connect_stdin(),
            self._connect_stdout(),
            self._connect_stderr(),
        )

        return future


async def echo(messenger):
    try:
        while True:
            message = await messenger.rx.get()
            await messenger.tx.put(message)
            messenger.rx.task_done()
            # log("echo", tx=messenger.tx.qsize())
    except asyncio.CancelledError:
        log('shutdown echo')


async def echo_remote(loop, messenger, target):
    """Take a message and send it to a remote."""
    remotes = {}

    try:
        while True:
            if target in remotes:
                remote = remotes[target]

            else:
                remote = remotes[target] = await Remote.launch(
                    target, loop=loop, python_bin='/home/olli/.pyenv/versions/debellator3/bin/python'
                )

            message = await messenger.rx.get()

            log("processing remote", message=message)
            await remote.tx.put(message)
            messenger.rx.task_done()

            # log("echo", tx=messenger.tx.qsize())
    except asyncio.CancelledError:
        log('shutdown echo_remote')


def main(master=True):
    # TODO evaluate args
    # print(sys.argv)

    try:
        messenger = Messenger(loop)

        if master:
            asyncio.ensure_future(echo_remote(loop, messenger, Target()), loop=loop)
        else:

            asyncio.ensure_future(echo(messenger), loop=loop)

        loop.run_until_complete(messenger.receive())

    except asyncio.CancelledError as ex:
        pass

    finally:
        loop.close()
