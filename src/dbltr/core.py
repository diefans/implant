"""
Remote process
"""
import sys
import asyncio
import signal
import functools
import os
import base64
import json
import logging


logging.basicConfig(level='DEBUG')
logger = logging.getLogger(__name__)


def log(_msg, **kwargs):
    if sys.stderr.isatty():
        colors = {
            'color': '\033[01;31m',
            'nocolor': '\033[0m'
        }

    else:
        colors = {
            'color': '',
            'nocolor': ''
        }

    if kwargs:
        error_msg = "{color}{msg} {kwargs}{nocolor}".format(msg=_msg, kwargs=kwargs, **colors)

    else:
        error_msg = "{color}{msg}{nocolor}".format(msg=_msg, **colors)

    logger.debug(error_msg)


def create_loop(*, debug=False):
    """Create the appropriate loop."""

    if sys.platform == 'win32':
        loop = asyncio.ProactorEventLoop()  # for subprocess' pipes on Windows
        asyncio.set_event_loop(loop)

    else:
        loop = asyncio.get_event_loop()

    loop.set_debug(debug)

    return loop


# loop is global
loop = create_loop(debug=True)


async def queue_read_pipe(queue, reader):
    """Queue lines from reader."""

    while True:
        line = await reader.readline()
        if line is b'':
            break

        await queue.put(line)


async def queue_write_pipe(queue, writer):
    """Write data from queue to writer."""

    while True:
        data = await queue.get()
        writer.write(data)
        await writer.drain()
        queue.task_done()


class Messenger:

    """Upstream communication."""

    def __init__(self, loop=None):
        self._loop = loop or asyncio.get_event_loop()

        self.queue_in = asyncio.Queue(loop=self._loop)
        self.queue_out = asyncio.Queue(loop=self._loop)
        self.queue_err = asyncio.Queue(loop=self._loop)

        # holds a future indicating that the Messenger is running
        self.running = None

    async def distribute_incomming_chunks(self):
        pass

    async def connect_stdio(self):
        """Connect stdio to queues.

        If stdout and stderr is a tty, it will not be connected.
        This keeps logging intact.

        https://www.mail-archive.com/python-tulip@googlegroups.com/msg00428.html
        """

        stdio_tasks = []
        stdio_cleanup = []

        # first connect writer
        if not sys.stdout.isatty():
            stdout_transport, stdout_protocol = await loop.connect_write_pipe(
                asyncio.streams.FlowControlMixin,
                # sys.stdout
                os.fdopen(sys.stdout.fileno(), 'wb')
            )
            stdout_writer = asyncio.streams.StreamWriter(stdout_transport, stdout_protocol, None, self._loop)

            stdio_tasks.append(
                asyncio.ensure_future(queue_write_pipe(self.queue_out, stdout_writer), loop=self._loop),
            )
            stdio_cleanup.append(lambda: stdout_transport.close())

            if os.fstat(sys.stderr.fileno()) != os.fstat(sys.stdout.fileno()):
                stderr_transport, stderr_protocol = await loop.connect_write_pipe(
                    asyncio.streams.FlowControlMixin,
                    # sys.stderr
                    os.fdopen(sys.stderr.fileno(), 'wb')
                )
                stderr_writer = asyncio.streams.StreamWriter(stderr_transport, stderr_protocol, None, self._loop)

                stdio_tasks.append(
                    asyncio.ensure_future(queue_write_pipe(self.queue_err, stderr_writer), loop=self._loop),
                )
                stdio_cleanup.append(lambda: stderr_transport.close())

            else:
                # redirect stderr to stdout
                stdio_tasks.append(
                    asyncio.ensure_future(queue_write_pipe(self.queue_err, stdout_writer), loop=self._loop),
                )

        # connect reader
        stdin_reader = asyncio.StreamReader(loop=self._loop)
        stdin_protocol = asyncio.StreamReaderProtocol(stdin_reader)

        stdin_transport, _ = await loop.connect_read_pipe(
            lambda: stdin_protocol,
            # sys.stdin
            os.fdopen(sys.stdin.fileno(), 'r')
        )
        stdio_tasks.append(
            asyncio.ensure_future(queue_read_pipe(self.queue_in, stdin_reader), loop=self._loop),
        )
        stdio_cleanup.append(lambda: stdin_transport.close())

        future = asyncio.gather(*stdio_tasks, loop=self._loop)

        try:
            await future

        except asyncio.CancelledError:
            for cleanup in stdio_cleanup:
                cleanup()

    def exit(self, result=None):
        if not self.running:
            raise RuntimeError('{} is not running!'.format(self))

        self.running.set_result(result)

    async def run(self, *tasks):
        """Schedules all tasks and wait for running is done or canceled"""

        # create indicator for running messenger
        running = asyncio.Future(loop=self._loop)

        shutdown_tasks = []

        # connect stdio
        shutdown_tasks.append(asyncio.ensure_future(self.connect_stdio(), loop=loop))

        for task in tasks:
            fut = asyncio.ensure_future(task, loop=self._loop)

            def _shutdown_on_error(future):
                try:
                    # retrieve result or exception
                    future.result()

                except Exception as ex:
                    # logger.error(traceback.format_exc())
                    running.set_exception(ex)

                    # raise

            fut.add_done_callback(_shutdown_on_error)
            shutdown_tasks.append(fut)

        # exit on sigterm or sigint
        for signame in ('SIGINT', 'SIGTERM'):
            sig = getattr(signal, signame)
            self._loop.add_signal_handler(sig, functools.partial(running.set_result, sig))

        self.running = running

        # wait for running completed
        result = await self.running

        # cleanup
        for task in shutdown_tasks:
            if task.done():
                continue

            log("cancel", task=task)
            task.cancel()
            await asyncio.wait_for(task, None)

        return result


DEFAULT_CHANNEL_NAME = b''


class Chunk:

    """We split messages into chunks to allow parallel communication of messages."""

    separator = b'|'

    def __init__(self, data=None, *, channel=None, uid=None):
        self.data = data
        self.channel = channel
        self.uid = uid

    @classmethod
    def decode(cls, raw):
        """Decodes a bytes string with or without ending \n into a Chunk."""

        raw_view = memoryview(raw)

        # split raw data at separators
        sep_ord = ord(cls.separator)

        def _gen_separator_index():
            count = 0
            for i, byte in enumerate(raw_view):
                if byte == sep_ord:
                    yield i
                    count += 1

            assert count == 2, 'A Chunk must be composed by two separators, e.g. `channel|uid|payload`!'
        channel_end, uid_end = _gen_separator_index()

        raw_len = len(raw_view)
        # 10 == \n: skip newline
        raw_end = raw_len - 1 if raw_view[-1] == 10 else raw_len

        channel_view = raw_view[0:channel_end]
        uid_view = raw_view[channel_end + 1:uid_end]
        data_view = raw_view[uid_end + 1:raw_end]

        return cls(base64.b64decode(data_view), channel=channel_view.tobytes(), uid=uid_view.tobytes())

    def encode(self, eol=True):
        """Encode the chunk into a bytes string."""

        def _gen_parts():
            yield self.channel or DEFAULT_CHANNEL_NAME
            yield self.separator
            yield self.uid or b''
            yield self.separator
            yield base64.b64encode(self.data or b'')
            if eol:
                yield b'\n'

        return b''.join(_gen_parts())

    def __repr__(self):
        def _gen():
            yield b'<Chunk'
            if self.uid or self.channel:
                yield b' '

            if self.uid:
                yield self.uid

            if self.channel:
                yield b'@'
                yield self.channel

            if self.data:
                yield b' len='
                yield str(len(self.data)).encode()

            else:
                yield b' EOM'

            yield b'>'

        return b''.join(_gen()).decode()


class ChunkChannel:

    """Iterator over a queues own chunks."""

    max_size = 10240

    def __init__(self, name=DEFAULT_CHANNEL_NAME, *, queue=None, loop=None):
        self.name = name
        self._loop = loop or asyncio.get_event_loop()
        self.queue = queue or asyncio.Queue(loop=loop)

    async def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.data_received()

    async def data_received(self):
        """:returns: a tuple (uid, chunk) from the queue of this channel."""

        line = await self.queue.get()

        try:
            chunk = Chunk.decode(line)
            logger.debug('Chunk received at %s: %s', self, chunk)
            return chunk.uid, chunk

        finally:
            self.queue.task_done()

    async def inject_chunk(self, chunk):
        """Allows forwarding of a chunk as is into the queue."""

        await self.queue.put(chunk.encode())

    async def send(self, data, *, uid=None):
        """Create a chunk from data and uid and send it to the queue of this channel."""

        if isinstance(data, Chunk):
            data = data.data

        for part in split_data(data, self.max_size):
            chunk = Chunk(part, channel=self.name, uid=uid)

            await self.inject_chunk(chunk)

    async def send_eom(self, *, uid=None):
        """Send EOM (end of message) to the queue."""

        chunk = Chunk(None, channel=self.name, uid=uid)

        await self.queue.put(chunk.encode())


def split_data(data, size=1024):
    """A generator to yield splitted data."""

    data_view = memoryview(data)
    data_len = len(data_view)
    start = 0

    while start < data_len:
        end = min(start + size, data_len)

        yield data_view[start:end]

        start = end


class JsonChannel(ChunkChannel):

    """Buffers chunks, combines them and tries to json-decode them."""

    def __init__(self, *args, **kwargs):
        super(JsonChannel, self).__init__(*args, **kwargs)

        # holds message parts until json is complete
        self.buffer = {}

    async def data_received(self):
        """Wait until a message is completed by sending an empty chunk."""

        while True:
            # wait for the next data
            uid, chunk = await super(JsonChannel, self).data_received()

            # create buffer if uid is new
            if uid not in self.buffer:
                self.buffer[uid] = bytearray()

            # message completed -> return json decoded chunks
            if not len(chunk.data):
                return uid, self._pop_json_from_buffer(uid)

            # append data to buffer
            self.buffer[uid].extend(chunk.data)

    def _pop_json_from_buffer(self, uid):
        try:
            data = json.loads(self.buffer[uid].decode())

            return data

        finally:
            del self.buffer[uid]

    async def send(self, data, *, uid=None):

        if isinstance(data, Chunk):
            data = data.data

        json_data = json.dumps(data).encode()

        await super(JsonChannel, self).send(json_data, uid=uid)

    def message(self, uid=None):
        class _context:
            def __init__(self, channel):
                self.channel = channel

            async def __aenter__(self):
                return functools.partial(self.channel.send, uid=uid)

            async def __aexit__(self, exc_type, exc, tb):
                # send EOM
                await self.channel.send_eom(uid=uid)

        return _context(self)


class Channels:

    """Distributes arbitrary chunks to channels.

    The empty channel is used to send Commands."""

    def __init__(self, loop=None):
        self._loop = loop or asyncio.get_event_loop()

        self._channels = {}
        self.add_channel(JsonChannel(loop=self._loop))

        self.default = self[DEFAULT_CHANNEL_NAME]

    async def __call__(self, queue):
        logger.info("Starting channel distribution...")
        channel = ChunkChannel(queue=queue, loop=self._loop)

        async for uid, chunk in channel:
            await self.distribute(chunk)

    async def distribute(self, chunk):
        channel = chunk.channel
        if channel not in self._channels:
            logger.error('Channel `%s` not found for Chunk %s', channel, chunk)

        await self._channels[channel].inject_chunk(chunk)
        logger.info("Chunk %s distributed to Channel `%s`", chunk, channel)

    def __getitem__(self, channel_name):
        try:
            return self._channels[channel_name]

        except KeyError:
            raise KeyError("Channel `{}` not found!".format(channel_name))

    def __setitem__(self, channel_name, channel):
        if channel_name in self._channels:
            logger.warning('Overriding existing channel `%s`', channel_name)

        self._channels[channel_name] = channel

    def __delitem__(self, channel_name):
        try:
            logger.info('Removing channel `%s`', channel_name)
            del self._channels[channel_name]

        except KeyError:
            raise KeyError("Channel `{}` not found!".format(channel_name))

    def add_channel(self, channel):
        self[channel.name] = channel


class Commander:

    """
    Execute specific messages/commands

    and return a result by replying in the same channel and with the same uid.
    """

    # TODO command registration

    def __init__(self, loop=None):
        self.loop = loop or asyncio.get_event_loop()

    async def execute_messages(self, channel_in, queue):
        logger.info("Retrieving commands...")

        channel_out = JsonChannel(channel_in.name, queue=queue)

        async for uid, message in channel_in:
            logger.info("retrieved command: %s", message)

            # do something and return result
            result = {
                'foo': message
            }

            async with channel_out.message(uid) as send:
                await send(result)


def main(*args, **kwargs):
    messenger = Messenger(loop)

    channels = Channels(loop)

    commander = Commander(loop)

    try:
        loop.run_until_complete(
            messenger.run(
                channels(messenger.queue_in),
                commander.execute_messages(channels.default, messenger.queue_out)
            )
        )

    finally:
        loop.close()


if __name__ == '__main__':
    main()
