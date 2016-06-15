"""
unify messenger
"""

import sys
import base64
import uuid
import functools
import asyncio
import signal


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


class Messenger:
    def __init__(self, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self.rx = asyncio.Queue(loop=self._loop)
        self.tx = asyncio.Queue(loop=self._loop)
        self.ex = asyncio.Queue(loop=self._loop)

    async def connect_stdin(self):
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

    async def connect_stdout(self):
        """Transform each `Message` in tx into one or more lines to stdout."""

        try:
            while True:
                data = await self.tx.get()

                log("stdout:", msg=data)
                if isinstance(data, Message):
                    for line in data.iter_lines():
                        sys.stdout.buffer.write(line)
                else:
                    sys.stdout.buffer.write(data)

                self.tx.task_done()
        except asyncio.CancelledError:
            pass

    async def connect_stderr(self):
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
            self.connect_stdin(),
            self.connect_stdout(),
            self.connect_stderr(),
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


def cancel_tasks():
    for task in asyncio.Task.all_tasks():
        task.cancel()


def create_loop():
    if sys.platform == 'win32':
        loop = asyncio.ProactorEventLoop()  # for subprocess' pipes on Windows
        asyncio.set_event_loop(loop)

    else:
        loop = asyncio.get_event_loop()

    loop.set_debug(True)

    for signame in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(
            getattr(signal, signame),
            cancel_tasks
        )

    return loop


def main(*args, **kwargs):
    # TODO evaluate args
    # print(sys.argv)

    loop = create_loop()

    try:
        messenger = Messenger(loop)

        loop.create_task(
            echo(messenger)
        )

        loop.run_until_complete(messenger.receive())

    except asyncio.CancelledError as ex:
        pass

    finally:
        loop.close()


if __name__ == '__main__':
    main()
