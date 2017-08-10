"""The core module is transfered to the remote process and will bootstrap pipe communication.

It creates default io queues and dispatches commands accordingly.

"""  # pylint: disable=C0302
import abc
import asyncio
import collections
import concurrent
import contextlib
import functools
import hashlib
import importlib.abc
import importlib.machinery
import importlib._bootstrap_external
import inspect
import logging
import logging.config
import os
import signal
import struct
import sys
import threading
import time
import traceback
import weakref
import zlib

import msgpack

log = logging.getLogger(__name__)


class MsgpackEncoder(metaclass=abc.ABCMeta):

    """Add msgpack en/decoding to a type."""

    @abc.abstractclassmethod
    def __msgpack_encode__(cls, data, data_type):   # noqa
        return None

    @abc.abstractclassmethod
    def __msgpack_decode__(cls, data, data_type):
        return None

    @classmethod
    def __subclasshook__(cls, C):
        if cls is MsgpackEncoder:
            if any("__msgpack_encode__" in B.__dict__ for B in C.__mro__) \
                    and any("__msgpack_decode__" in B.__dict__ for B in C.__mro__):
                return True
        return NotImplemented


class MsgpackDefaultEncoder(dict):

    """Encode or decode custom objects."""

    def encode(self, data):
        data_type = type(data)
        data_module = data_type.__module__

        encoder = data if isinstance(data, MsgpackEncoder) else self._get_encoder(data_type)
        if encoder:
            return {
                '__custom_object__': True,
                '__module__': data_module,
                '__type__': data_type.__name__,
                '__data__': encoder.__msgpack_encode__(data, data_type=data_type)
            }

        return data

    def decode(self, encoded):
        if encoded.get('__custom_object__', False):
            # we have to search the class
            module = sys.modules.get(encoded['__module__'])

            if not module:
                # TODO import missing module
                raise TypeError("The module of the encoded data type is not loaded: {}".format(encoded))

            data_type = getattr(module, encoded['__type__'])

            decoder = data_type if issubclass(data_type, MsgpackEncoder) else self._get_encoder(data_type)
            decoded = decoder.__msgpack_decode__(encoded['__data__'], data_type=data_type)

            return decoded

        return encoded

    def _get_encoder(self, data_type):
        # lookup data types for registered encoders
        for cls in data_type.__mro__:
            if cls in self:
                encoder = self[cls]
                return encoder

        return None

    def register_encoder(self, data_type):
        def decorator(func):
            self[data_type] = func

            return func

        return decorator


msgpack_default_encoder = MsgpackDefaultEncoder()


@msgpack_default_encoder.register_encoder(Exception)
class MsgpackExceptionEncoder(MsgpackEncoder):

    """Encode and decode Exception arguments.

    Traceback and other internals will be lost.
    """

    @classmethod
    def __msgpack_encode__(cls, data, data_type):
        return data.args

    @classmethod
    def __msgpack_decode__(cls, encoded, data_type):
        return data_type(*encoded)


# TODO FIXME XXX msgpack is not able to preserve tuple type and
# is also not able to call default hook for tuple
# so someone has to know in advance that this is a tuple and use use_list=False for that
def encode_msgpack(data):
    try:
        return msgpack.packb(data, default=msgpack_default_encoder.encode, use_bin_type=True, encoding="utf-8")

    except:     # noqa
        log.error("Error packing:\n%s", traceback.format_exc())
        raise


def decode_msgpack(data):
    try:
        return msgpack.unpackb(data, object_hook=msgpack_default_encoder.decode, encoding="utf-8")

    except:     # noqa
        log.error("Error unpacking:\n%s", traceback.format_exc())
        raise


encode = encode_msgpack
decode = decode_msgpack


class reify:

    """Create a property and cache the result."""

    def __init__(self, wrapped):
        self.wrapped = wrapped
        functools.update_wrapper(self, wrapped)

    def __get__(self, inst, objtype=None):
        if inst is None:
            return self

        val = self.wrapped(inst)
        setattr(inst, self.wrapped.__name__, val)

        return val


class Uid:

    """Represent a unique id."""

    _uid = threading.local()

    def __init__(self, time=None, id=None, tid=None, seq=None, bytes=None):     # noqa
        fmt = "!dQQQ"
        all_args = [time, id, tid, seq].count(None) == 0

        if not all_args:
            if bytes:
                time, id, tid, seq = struct.unpack(fmt, bytes)
            else:
                time, id, tid, seq = self._create_uid()

        self.bytes = bytes or struct.pack(fmt, time, id, tid, seq)
        self.time = time
        self.id = id
        self.tid = tid
        self.seq = seq

    @classmethod
    def _create_uid(cls):
        if getattr(cls._uid, "uid", None) is None:
            thread = threading.current_thread()
            cls._uid.tid = thread.ident
            cls._uid.id = id(thread)
            cls._uid.uid = 0

        cls._uid.uid += 1
        uid = (time.time(), cls._uid.id, cls._uid.tid, cls._uid.uid)

        return uid

    def __hash__(self):
        return hash(self.bytes)

    def __eq__(self, other):
        if isinstance(other, Uid):
            return (self.time, self.id, self.tid, self.seq) == (other.time, other.id, other.tid, other.seq)

        return False

    def __str__(self):
        return '-'.join(map(str, (self.time, self.id, self.tid, self.seq)))

    def __repr__(self):
        return str(self)

    @classmethod
    def __msgpack_encode__(cls, data, data_type):
        return data.bytes

    @classmethod
    def __msgpack_decode__(cls, encoded, data_type):
        return cls(bytes=encoded)


class Incomming(asyncio.StreamReader):

    """A context for an incomming pipe."""

    def __init__(self, *, pipe=sys.stdin):
        super(Incomming, self).__init__()

        self.pipe = os.fdopen(pipe) if isinstance(pipe, int) else pipe

    async def __aenter__(self):
        protocol = asyncio.StreamReaderProtocol(self)

        await asyncio.get_event_loop().connect_read_pipe(
            lambda: protocol,
            self.pipe,
        )

        return self

    async def __aexit__(self, exc_type, value, tb):
        self._transport.close()

    async def readexactly(self, n):
        """Read exactly n bytes from the stream.

        This is a short and faster implementation then the original one
        (see of https://github.com/python/asyncio/issues/394).

        """
        buffer, missing = bytearray(), n

        while missing:
            if not self._buffer:
                await self._wait_for_data('readexactly')

            if self._eof or not self._buffer:
                raise asyncio.IncompleteReadError(bytes(buffer), n)

            length = min(len(self._buffer), missing)
            buffer.extend(self._buffer[:length])

            del self._buffer[:length]
            missing -= length

            self._maybe_resume_transport()

        return buffer


class ShutdownOnConnectionLost(asyncio.streams.FlowControlMixin):

    """Send SIGHUP when connection is lost."""

    def connection_lost(self, exc):
        """Shutdown process."""
        super(ShutdownOnConnectionLost, self).connection_lost(exc)

        if exc is not None:
            log.warning("Connection lost by %s", exc)
        # log.info("Pending tasks: %s", [task for task in asyncio.Task.all_tasks()])
        # os.kill(os.getpid(), signal.SIGHUP)


class Outgoing:

    """A context for an outgoing pipe."""

    def __init__(self, *, pipe=sys.stdout, reader=None, shutdown=False):
        self.pipe = os.fdopen(pipe) if isinstance(pipe, int) else pipe
        self.transport = None
        self.reader = reader
        self.shutdown = shutdown

    async def __aenter__(self):
        self.transport, protocol = await asyncio.get_event_loop().connect_write_pipe(
            ShutdownOnConnectionLost if self.shutdown else asyncio.streams.FlowControlMixin,
            self.pipe
        )

        writer = asyncio.streams.StreamWriter(
            self.transport,
            protocol,
            self.reader,
            asyncio.get_event_loop()
        )
        return writer

    async def __aexit__(self, exc_type, value, tb):
        pass
        # XXX why is this commented?
        # self.transport.close()


def split_data(data, size=1024):
    """Create a generator to split data into chunks."""
    data_view, data_len, start = memoryview(data), len(data), 0
    while start < data_len:
        end = min(start + size, data_len)
        yield data_view[start:end]
        start = end


class ChunkFlags(dict):

    """Store flags for a chunk."""

    _masks = {
        'eom': (1, 0, int, bool),
        'send_ack': (1, 1, int, bool),
        'recv_ack': (1, 2, int, bool),
        'compression': (1, 3, int, bool),
    }

    def __init__(self, *, send_ack=False, recv_ack=False, eom=False, compression=False):
        self.__dict__ = self
        super(ChunkFlags, self).__init__()

        self.eom = eom
        self.send_ack = send_ack
        self.recv_ack = recv_ack
        self.compression = compression

    def encode(self):
        def _mask_value(k, v):
            mask, shift, enc, _ = self._masks[k]
            return (enc(v) & mask) << shift

        return sum(_mask_value(k, v) for k, v in self.items())

    @classmethod
    def decode(cls, value):
        def _unmask_value(k, v):
            mask, shift, _, dec = v
            return dec((value >> shift) & mask)

        return cls(**{k: _unmask_value(k, v) for k, v in cls._masks.items()})


HEADER_FMT = '!32sQHI'
HEADER_SIZE = struct.calcsize(HEADER_FMT)


def _encode_header(uid, channel_name=None, data=None, *, flags=None):
    """Create chunk header.

    [header length = 30 bytes]
    [2 byte] [!16s]     [!Q: 8 bytes]                     [!H: 2 bytes]        [!I: 4 bytes]
    {type}{data uuid}{flags: compression|eom|stop_iter}{channel_name length}{data length}{channel_name}{data}

    """
    assert isinstance(uid, Uid), "uid must be an Uid instance"

    if flags is None:
        flags = {}

    if channel_name:
        encoded_channel_name = channel_name.encode()
        channel_name_length = len(encoded_channel_name)
    else:
        channel_name_length = 0

    data_length = len(data) if data else 0
    chunk_flags = ChunkFlags(**flags)

    header = struct.pack(
        HEADER_FMT,
        uid.bytes, chunk_flags.encode(), channel_name_length, data_length
    )
    check = hashlib.md5(header).digest()

    return b''.join((header, check))


def _decode_header(header):
    assert hashlib.md5(header[:-16]).digest() == header[-16:], "Header checksum mismatch!"
    (
        uid_bytes,
        flags_encoded,
        channel_name_length,
        data_length
    ) = struct.unpack(HEADER_FMT, header[:-16])

    uid = Uid(bytes=uid_bytes)
    return uid, ChunkFlags.decode(flags_encoded), channel_name_length, data_length


Chunk = collections.namedtuple('Chunk', ('uid', 'flags', 'channel_name', 'data'))


class Channels:

    """Hold references to all channel queues."""

    chunk_size = 0x8000

    acknowledgements = {}
    """Global acknowledgment futures distinctive by uid."""

    def __init__(self, *, reader, writer):
        self.incomming = weakref.WeakValueDictionary()

        self.reader = reader
        self.writer = writer

        self._lock_communicate = asyncio.Lock()
        self._evt_shutdown = asyncio.Event()

    def get_channel(self, channel_name):
        """Create a channel and weakly register its queue.

        :param channel_name: the name of the channel to create

        """
        queue = asyncio.Queue()
        try:
            channel = Channel(
                channel_name,
                send=functools.partial(self.send, channel_name),
                queue=queue
            )
            return channel

        finally:
            self.incomming[channel_name] = queue

    def shutdown(self):
        """Shutdown enqueueing of messages."""
        self._evt_shutdown.set()

    async def enqueue(self):
        """Schedule receive tasks.

        Incomming chunks are collected and stored in the appropriate channel queue.
        """
        async with self._lock_communicate:
            # start receiving
            self._evt_shutdown.clear()
            _fut_receive_reader = asyncio.ensure_future(self._receive_reader())

            await self._evt_shutdown.wait()

            # stop receiving new messages
            _fut_receive_reader.cancel()
            await _fut_receive_reader

    async def _read_chunk(self):
        # read header
        raw_header = await self.reader.readexactly(HEADER_SIZE + 16)
        uid, flags, channel_name_length, data_length = _decode_header(raw_header)

        log.debug("%s: flags %s", uid, flags)

        # read channel name
        channel_name = (await self.reader.readexactly(channel_name_length)).decode() \
            if channel_name_length else None

        # read data
        if data_length:
            data = await self.reader.readexactly(data_length)
            if flags.compression:
                data = zlib.decompress(data)
        else:
            data = None

        chunk = Chunk(uid, flags, channel_name, data)
        return chunk

    async def _finalize_message(self, buffer, chunk):
        if chunk.flags.send_ack:
            # we have to acknowledge the reception
            await self._send_ack(chunk.uid)

        if chunk.flags.eom:
            # put message into channel queue
            if chunk.uid in buffer and chunk.channel_name:
                msg = decode(buffer[chunk.uid])
                try:
                    # try to store message in channel
                    await self.incomming[chunk.channel_name].put(msg)
                finally:
                    del buffer[chunk.uid]

            # acknowledge reception
            ack_future = self.acknowledgements.get(chunk.uid)
            if ack_future and chunk.flags.recv_ack:
                try:
                    log.debug("%s: acknowledge", chunk.uid)
                    duration = time.time() - chunk.uid.time
                    ack_future.set_result((chunk.uid, duration))
                finally:
                    del self.acknowledgements[chunk.uid]

    def _feed_data(self, buffer, chunk):
        if chunk.data:
            if chunk.uid not in buffer:
                buffer[chunk.uid] = bytearray()

            buffer[chunk.uid].extend(chunk.data)

        # debug
        if chunk.channel_name:
            log.debug("%s: channel `%s` receives: %s bytes",
                      chunk.uid, chunk.channel_name, len(chunk.data) if chunk.data else 0)
        else:
            log.debug("%s: no channel received: %s", chunk.uid, chunk.flags)

    async def _receive_single_message(self, buffer):
        chunk = await self._read_chunk()

        self._feed_data(buffer, chunk)

        await self._finalize_message(buffer, chunk)

    async def _receive_reader(self):
        # receive incomming data into queues
        log.info("Start receiving from %s...", self.reader)
        buffer = {}
        try:
            while True:
                await self._receive_single_message(buffer)

        except (asyncio.CancelledError, GeneratorExit):
            if buffer:
                log.warning("Receive buffer was not empty when canceled!")

            log.warning("Receiving canceled")

        except EOFError:
            log.info("While waiting for data, we received EOF.")

        except:     # noqa
            log.error("Error while receiving:\n%s", traceback.format_exc())
            raise

    async def _send_ack(self, uid):
        # no channel_name, no data
        header = _encode_header(uid, None, None, flags={
            'eom': True, 'recv_ack': True
        })

        log.debug("%s: send acknowledgement", uid)
        await self._send_raw(header)

    async def _send_raw(self, *data):
        for part in data:
            self.writer.write(part)

        await self.writer.drain()

    async def send(self, channel_name, data, ack=False, compress=6):
        """Send data in a encoded form to the channel.

        :param data: the python object to send
        :param ack: request acknowledgement of the reception of that message
        :param compress: compress the data with zlib

        Messages are split into chunks and put into the outgoing queue.

        """
        uid = Uid()
        encoded_channel_name = channel_name.encode()
        loop = asyncio.get_event_loop()

        with concurrent.futures.ThreadPoolExecutor() as executor:
            encoded_data = await loop.run_in_executor(executor, encode, data)

        log.debug("%s: channel `%s` sends: %s bytes", uid, channel_name, len(encoded_data))

        for part in split_data(encoded_data, self.chunk_size):
            if compress:
                raw_len = len(part)
                part = zlib.compress(part, compress)
                comp_len = len(part)

                log.debug("%s: compression ratio of %s -> %s: %.2f%%", uid, raw_len, comp_len, comp_len * 100 / raw_len)

            header = _encode_header(uid, channel_name, part, flags={
                'eom': False, 'send_ack': False, 'compression': bool(compress)
            })

            await self._send_raw(header, encoded_channel_name, part)

        header = _encode_header(uid, channel_name, None, flags={
            'eom': True, 'send_ack': ack, 'compression': False
        })

        # if acknowledgement is asked for, we await this future and return its result
        # see _receive_reader for resolution of future
        if ack:
            ack_future = asyncio.Future()
            self.acknowledgements[uid] = ack_future

        await self._send_raw(header, encoded_channel_name)

        if ack:
            log.debug("%s: wait for acknowledgement...", uid)
            acknowledgement = await ack_future
            log.debug("%s: acknowldeged: %s", uid, acknowledgement)

            return acknowledgement


class Channel:

    """Channel provides means to send and receive messages bound to a specific channel name."""

    def __init__(self, name=None, *, queue, send):
        """Initialize the channel.

        :param name: the channel name
        :param queue: the incomming queue
        :param send: the partial send method of Channels

        """
        self.name = name
        self.queue = queue
        self.send = send

    def __repr__(self):
        return '<{0.name} {in_size}>'.format(
            self,
            in_size=self.queue.qsize(),
        )

    def __await__(self):
        """Receive the next message in this channel."""
        async def coro():
            msg = await self.queue.get()

            try:
                return msg
            finally:
                self.queue.task_done()

        return coro().__await__()

    async def __aiter__(self):
        return self

    async def __anext__(self):
        data = await self
        if isinstance(data, StopAsyncIteration):
            raise data

        return data

    def stop_iteration(self):       # noqa
        class context:
            async def __aenter__(ctx):      # noqa
                return self

            async def __aexit__(ctx, *args):        # noqa
                await self.send(StopAsyncIteration())

        return context()


def exclusive(fun):
    """Make an async function call exclusive."""
    lock = asyncio.Lock()
    log.debug("Locking function: %s -> %s", lock, fun)

    async def locked_fun(*args, **kwargs):
        log.debug("Wait for lock releasing: %s -> %s", lock, fun)
        async with lock:
            log.debug("Executing locked function: %s -> %s", lock, fun)
            return await fun(*args, **kwargs)

    return locked_fun


DispatchContext = collections.namedtuple('DispatchContext', ('channel', 'execute'))
DISPATCHER_CHANNEL_NAME = 'Dispatcher'


class Dispatcher:

    """Enables execution of `Command`s.

    A `Command` is split into local and remote part, where a context with
    a dedicated `Channel` is provided to enable streaming of arbitrary data.
    The local part also gets a remote future passed, which resolves to the
    result of the remote part of the `Command`.

    """

    def __init__(self, io_queues):
        self.io_queues = io_queues
        self.channel = self.io_queues.get_channel(DISPATCHER_CHANNEL_NAME)
        self.pending_commands = collections.defaultdict(asyncio.Future)
        self.pending_remote_tasks = set()

        self._fut_execute_io_queues = None
        self._lock_dispatch = asyncio.Lock()
        self._evt_shutdown = asyncio.Event()

    def shutdown(self):
        self._evt_shutdown.set()

    async def dispatch(self):
        """Start sending and receiving messages and executing them."""
        async with self._lock_dispatch:
            self._evt_shutdown.clear()

            self._fut_execute_io_queues = asyncio.ensure_future(self._execute_io_queues())

            await self._evt_shutdown.wait()

            self._fut_execute_io_queues.cancel()
            await self._fut_execute_io_queues

            # wait for pending tasks to finalize
            for task in self.pending_remote_tasks:
                log.info("Cancelling pending remote task: %s", task)
                task.cancel()
                await task

    @contextlib.contextmanager
    def context(self, fqin):
        """Create a context to pass to a `Command`s local and remote part.

        The `Channel` is built via a fully qualified instance name (fqin).

        """
        channel = self.io_queues.get_channel(fqin)
        context = DispatchContext(channel=channel, execute=self.execute)
        yield context

    async def _execute_io_queues(self):
        """Execute messages sent via our `Dispatcher.channel`."""
        log.info("Listening on channel %s for command dispatch...", self.channel)

        try:
            async for message in self.channel:
                log.debug("Received dispatch message: %s", message)
                await message(self)

        except asyncio.CancelledError:
            log.info("Command execution stopped.")

        finally:
            # teardown here
            for fqin, fut in self.pending_commands.items():
                log.warning("Teardown pending command: %s, %s", fqin, fut)
                fut.cancel()
                await fut
                del self.pending_commands[fqin]

    async def execute(self, command):
        """Execute a command.

        First creating the remote side and its future
        and second executing its local part.

        """
        fqin = command.__class__.create_fqin()

        with self.context(fqin) as context:
            async with self.remote_future(fqin, command) as future:
                try:
                    log.debug("Excute command: %s", command)
                    # execute local side of command
                    result = await command.local(context, remote_future=future)
                    future.result()
                    return result

                except:     # noqa
                    log.error("Error while executing command: %s\n%s", command, traceback.format_exc())
                    raise

    def remote_future(self, fqin, command):        # noqa
        """Create a context for remote command future by sending
        `DispatchCommand` and returning its pending future.

        """
        class _context:
            async def __aenter__(ctx):      # noqa
                # send execution request to remote
                await self.channel.send(DispatchCommand(fqin, command), ack=True)
                future = self.pending_commands[fqin]

                return future

            async def __aexit__(ctx, *args):        # noqa
                del self.pending_commands[fqin]

        return _context()

    async def execute_remote(self, fqin, command):
        """Execute the remote part of a `Command`.

        This method is called by a `DispatchCommand` message.

        The result is send via `Dispatcher.channel` to resolve the pending command future.

        """
        current_task = asyncio.Task.current_task()
        self.pending_remote_tasks.add(current_task)
        try:
            with self.context(fqin) as context:
                try:
                    # execute remote side of command
                    result = await command.remote(context)
                    await self.channel.send(DispatchResult(fqin, result=result))

                    return result

                except asyncio.CancelledError:
                    log.info("Remote execution canceled")

                except Exception as ex:
                    tb = traceback.format_exc()
                    log.error("traceback:\n%s", tb)
                    await self.channel.send(DispatchException(fqin, exception=ex, tb=tb))

                    raise
        finally:
            self.pending_remote_tasks.remove(current_task)


class _CommandMeta(type):
    base = None
    commands = {}

    def __new__(mcs, name, bases, dct):
        """Create Command class.

        Add command_name as __module__:__name__
        """
        dct['command_name'] = ':'.join((dct['__module__'], dct['__qualname__']))
        cls = type.__new__(mcs, name, bases, dct)

        if mcs.base is None:
            mcs.base = cls
        else:
            # only register classes except base class
            mcs.commands[cls.command_name] = cls

        return cls

    def create_fqin(cls):
        uid = Uid()

        fqin = '/'.join(map(str, (cls.command_name, uid)))
        return fqin

    def __getitem__(cls, command_name):
        command_class = cls.commands[command_name]
        return command_class


class Command(dict, metaclass=_CommandMeta):

    """Common ancestor of all Commands."""

    def __init__(self, *args, **kwargs):
        self.__dict__ = self
        super().__init__(*args, **kwargs)

    def __repr__(self):
        _repr = super().__repr__()
        return "<{self.__class__.command_name} {_repr}>".format(**locals())


class DispatchMessage:

    """Base class for command dispatch communication."""

    def __init__(self, fqin):
        self.fqin = fqin

    def __repr__(self):
        return "<{self.__class__.__name__} {self.fqin}>".format(**locals())


class DispatchCommand(DispatchMessage, MsgpackEncoder):

    """Arguments for a command dispatch."""

    def __init__(self, fqin, command):
        super().__init__(fqin)
        self.command = command
        log.info("Dispatch created: %s %s", self.fqin, self.command)

    async def __call__(self, dispatcher):
        # schedule remote execution
        asyncio.ensure_future(dispatcher.execute_remote(self.fqin, self.command))

    @classmethod
    def __msgpack_encode__(cls, data, data_type):
        return (data.fqin, data.command.__class__.command_name, data.command)

    @classmethod
    def __msgpack_decode__(cls, encoded, data_type):
        fqin, command_name, params = encoded
        command = Command[command_name]()
        command.update(params)
        return cls(fqin, command)


class DispatchException(DispatchMessage, MsgpackEncoder):

    """Remote execution ended in an exception."""

    def __init__(self, fqin, exception, tb=None):
        super().__init__(fqin)
        self.exception = exception
        self.tb = tb or traceback.format_exc()

    async def __call__(self, dispatcher):
        future = dispatcher.pending_commands[self.fqin]
        log.error("Dispatch exception for %s:\n%s", self.fqin, self.tb)
        future.set_exception(self.exception)

    @classmethod
    def __msgpack_encode__(cls, data, data_type):
        return (data.fqin, data.exception, data.tb)

    @classmethod
    def __msgpack_decode__(cls, encoded, data_type):
        fqin, exc, tb = encoded
        return cls(fqin, exc, tb)


class DispatchResult(DispatchMessage, MsgpackEncoder):

    """The result of a remote execution."""

    def __init__(self, fqin, result=None):        # noqa
        super().__init__(fqin)
        self.result = result

    async def __call__(self, dispatcher):
        future = dispatcher.pending_commands[self.fqin]
        log.debug("Dispatch result of %s", self.fqin)
        future.set_result(self.result)

    @classmethod
    def __msgpack_encode__(cls, data, data_type):
        return (data.fqin, data.result)

    @classmethod
    def __msgpack_decode__(cls, encoded, data_type):
        return cls(*encoded)


# events are taken from https://github.com/zopefoundation/zope.event
# function names are modified and adopted to asyncio
event_subscribers = []
event_registry = {}


async def notify_event(event):
    """ Notify all subscribers of ``event``.
    """
    for subscriber in event_subscribers:
        await subscriber(event)


def event_handler(event_class, handler_=None, decorator=False):
    """Define an event handler for a (new-style) class.
    This can be called with a class and a handler, or with just a
    class and the result used as a handler decorator.
    """
    if handler_ is None:
        return lambda func: event_handler(event_class, func, True)

    if not event_registry:
        event_subscribers.append(event_dispatch)

    if event_class not in event_registry:
        event_registry[event_class] = [handler_]
    else:
        event_registry[event_class].append(handler_)

    if decorator:
        return event_handler


async def event_dispatch(event):
    for event_class in event.__class__.__mro__:
        for handler in event_registry.get(event_class, ()):
            await handler(event)


class NotifyEvent(Command):

    """Notify about an event.

    If the remote side registers for this event, it gets notified.

    """

    def __init__(self, event=None, local=False):
        """Initialize NotifyEvent command.

        :param event: the event instance, which has to be de/encodable via message pack
        :param local: if True, the local side will also be notified
        """
        super().__init__()
        self.dispatch_local = local
        self.event = event

    async def local(self, context, remote_future):
        # we wait for remote events to be dispatched first
        await remote_future

        if self.dispatch_local:
            # local event submission
            log.info("Notify local %s", self.event)
            await notify_event(self.event)

    async def remote(self, context):
        # local event submission
        log.info("Notify remote %s", self.event)
        await notify_event(self.event)


class Echo(Command):

    """Demonstrate the basic command API."""

    async def local(self, context, remote_future):
        # custom protocol
        # first: receive
        async with context.channel.stop_iteration():
            for x in "send to remote":
                await context.channel.send(x)

        # second: send
        # py 3.6
        # from_remote = ''.join([x async for x in context.channel])
        from_remote = []
        async for x in context.channel:
            from_remote.append(x)

        # third: wait for remote to finish
        remote_result = await remote_future

        result = {
            'from_remote': ''.join(from_remote),
        }
        result.update(remote_result)
        return result

    async def remote(self, context):
        # first: send
        # py 3.6
        # from_local = ''.join([x async for x in context.channel])
        from_local = []
        async for x in context.channel:
            from_local.append(x)

        # second: receive
        async with context.channel.stop_iteration():
            for x in "send to local":
                await context.channel.send(x)

        # third: return result
        return {
            'from_local': ''.join(from_local),
            'remote_self': self,
            'pid': os.getpid()
        }


class InvokeImport(Command):

    """Invoke an import of a module on the remote side.

    The local side will import the module first.
    The remote side will trigger the remote import hook, which in turn
    will receive all missing modules from the local side.

    The import is executed in a separate executor thread, to have a separate event loop available.

    """

    async def local(self, context, remote_future):
        importlib.import_module(self.fullname)
        result = await remote_future
        return result

    @exclusive
    async def remote(self, context):
        loop = asyncio.get_event_loop()

        def import_stuff():
            thread_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(thread_loop)

            try:
                importlib.import_module(self.fullname)

            except ImportError:
                log.debug("Error when importing %s:\n%s", self.fullname, traceback.format_exc())
                raise

            finally:
                thread_loop.close()

        with concurrent.futures.ThreadPoolExecutor() as executor:
            await loop.run_in_executor(executor, import_stuff)


class FindModule(Command):

    """Find a module on the remote side."""

    async def local(self, context, remote_future):
        module_loaded = await remote_future

        return module_loaded

    @staticmethod
    def _is_namespace(module):
        # see https://www.python.org/dev/peps/pep-0451/#how-loading-will-work
        spec = module.__spec__
        is_namespace = spec.loader is None and spec.submodule_search_locations is not None
        return is_namespace

    @staticmethod
    def _is_package(module):
        is_package = bool(getattr(module, '__path__', None) is not None)
        return is_package

    @staticmethod
    def _get_source(module):
        spec = module.__spec__
        if isinstance(spec.loader, importlib.abc.InspectLoader):
            source = spec.loader.get_source(module.__name__)
        else:
            try:
                source = inspect.getsource(module)
            except OSError:
                # when source is empty
                source = ''

        return source

    @staticmethod
    def _get_source_filename(module):
        spec = module.__spec__
        if isinstance(spec.loader, importlib.abc.ExecutionLoader):
            filename = spec.loader.get_filename(module.__name__)
        else:
            filename = inspect.getsourcefile(module)

        return filename

    async def remote(self, context):
        module = sys.modules.get(self.module_name)

        if module:
            is_namespace = self._is_namespace(module)
            is_package = self._is_package(module)

            log.debug("module found: %s", module)
            remote_module_data = {
                'name': self.module_name,
                'is_namespace': is_namespace,
                'is_package': is_package
            }

            if not is_namespace:
                remote_module_data.update({
                    'source': self._get_source(module),
                    'source_filename': self._get_source_filename(module)
                })

            return remote_module_data
        else:
            log.warning('Module not loaded: %s', self.module_name)
            return None


class RemoteModuleFinder(importlib.abc.MetaPathFinder):

    """Import hook that schedules a `FindModule` coroutine in the main loop.

    The import itself is run in a separate executor thread to keep things async.

    http://stackoverflow.com/questions/32059732/send-asyncio-tasks-to-loop-running-in-other-thread
    https://www.python.org/dev/peps/pep-0302/
    https://www.python.org/dev/peps/pep-0420/
    https://www.python.org/dev/peps/pep-0451/
    """

    def __init__(self, dispatcher, loop):
        self.dispatcher = dispatcher
        self.main_loop = loop

    def _find_remote_module(self, module_name):
        # ask master for this module
        log.debug("Module lookup: %s", module_name)

        future = asyncio.run_coroutine_threadsafe(
            self.dispatcher.execute(FindModule(module_name=module_name)),
            loop=self.main_loop
        )
        module_data = future.result()

        return module_data

    def find_spec(self, fullname, path, target=None):
        log.debug('Path for module %s: %s', fullname, path)

        remote_module_data = self._find_remote_module(fullname)

        if remote_module_data:
            # FIXME logging blocks
            # log.debug("Module found for %s: %s", fullname, remote_module_data)

            if remote_module_data['is_namespace']:
                log.debug("Namespace package found for %s", fullname)
                spec = importlib.machinery.ModuleSpec(
                    name=fullname,
                    loader=None,
                    origin='remote namespace',
                    is_package=remote_module_data['is_package']
                )

            else:
                log.debug("Module found for %s", fullname)
                origin = 'remote://{}'.format(remote_module_data.get('source_filename', fullname))
                is_package = remote_module_data['is_package']

                loader = RemoteModuleLoader(
                    remote_module_data.get('source', ''),
                    filename=origin,
                    is_package=is_package
                )

                spec = importlib.machinery.ModuleSpec(
                    name=fullname,
                    loader=loader,
                    origin=origin,
                    is_package=is_package
                )

            return spec

        else:
            log.debug("No module found for %s", fullname)


class RemoteModuleLoader(importlib.abc.ExecutionLoader):    # pylint: disable=W0223

    """Load the found module spec."""

    def __init__(self, source, filename=None, is_package=False):
        self.source = source
        self.filename = filename
        self._is_package = is_package

    def is_package(self):
        return self._is_package

    def get_filename(self, fullname):
        if not self.filename:
            raise ImportError

        return self.filename

    def get_source(self, fullname):
        return self.source

    @classmethod
    def module_repr(cls, module):
        return "<module '{}' (namespace)>".format(module.__name__)


async def cancel_pending_tasks(loop):
    for task in asyncio.Task.all_tasks():
        if task.done() or task.cancelled():
            continue

        try:
            task.cancel()
            await task

        # try:
        #     loop.run_until_complete(task)

        except asyncio.CancelledError:
            pass


async def log_tcp_10001():
    try:
        reader, writer = await asyncio.open_connection('localhost', 10001)

        while True:
            msg = await reader.readline()

            log.info("TCP: %s", msg)

    except asyncio.CancelledError:
        log.info("close tcp log")
        writer.close()


class ShutdownRemoteEvent(MsgpackEncoder):

    """A Shutdown event."""

    @classmethod
    def __msgpack_encode__(cls, data, data_type):
        return None

    @classmethod
    def __msgpack_decode__(cls, encoded, data_type):
        return data_type()


async def communicate(loop, *, stdin=sys.stdin, stdout=sys.stdout, echo=None):
    try:
        async with Incomming(pipe=stdin) as reader:
            async with Outgoing(pipe=stdout, shutdown=True) as writer:
                io_queues = Channels(reader=reader, writer=writer)
                dispatcher = Dispatcher(io_queues)

                @event_handler(ShutdownRemoteEvent)
                async def _shutdown(event):
                    dispatcher.shutdown()
                    io_queues.shutdown()

                _setup_import_hook(dispatcher, loop)
                # TODO think about generic handshake
                # send echo to master to prove behavior
                if echo is not None:
                    writer.write(echo)
                    await writer.drain()

                fut_communicate = asyncio.ensure_future(io_queues.enqueue())
                fut_dispatch = asyncio.ensure_future(dispatcher.dispatch())

                await asyncio.gather(fut_communicate, fut_dispatch)

    except asyncio.CancelledError:
        log.info("Cancelled communicate??")

    log.info('Communication end.')


class ExecutorConsoleHandler(logging.StreamHandler):

    # FIXME TODO Executor seems to disturb uvloop so that it hangs randomly

    """Run logging in a separate executor, to not block on console output."""

    def __init__(self, *args, **kwargs):
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=150)

        super(ExecutorConsoleHandler, self).__init__(*args, **kwargs)

    def emit(self, record):
        # FIXME is not really working on sys.stdout
        # logging in thread breaks weakref in Channels

        def _emit():
            thread_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(thread_loop)

            try:
                super(ExecutorConsoleHandler, self).emit(record)
            finally:
                thread_loop.close()

        # with concurrent.futures.ThreadPoolExecutor() as executor:
        #     await loop.run_in_executor(executor, import_stuff)

        asyncio.get_event_loop().run_in_executor(
            self.executor, _emit
        )

    def __del__(self):
        self.executor.shutdown(wait=True)


def _setup_import_hook(dispatcher, loop):
    remote_module_finder = RemoteModuleFinder(dispatcher, loop)
    sys.meta_path.append(remote_module_finder)

    log.debug("meta path: %s", sys.meta_path)


def _setup_logging(loop, debug=False, log_config=None):
    if log_config is None:
        log_config = {
            'version': 1,
            'formatters': {
                'simple': {
                    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                }
            },
            'handlers': {
                'console': {
                    'class': 'logging.StreamHandler',
                    'formatter': 'simple',
                    'level': 'DEBUG',
                    'stream': 'ext://sys.stderr'
                }
            },
            'logs': {
                'debellator': {
                    'handlers': ['console'],
                    'level': 'DEBUG',
                    'propagate': False
                }
            },
            'root': {
                'handlers': ['console'],
                'level': 'WARNING'
            },
        }

    logging.config.dictConfig(log_config)
    loop = asyncio.get_event_loop()

    if debug:
        log.setLevel(logging.DEBUG)
        loop.set_debug(debug)


def main(debug=False, log_config=None, echo=None, **kwargs):
    loop = asyncio.get_event_loop()

    _setup_logging(loop, debug, log_config)
    log.info(' *** ' * 5)
    log.info("Starting process %s: pid=%s of ppid=%s", __name__, os.getpid(), os.getppid())
    log.debug("msgpack used: %s", msgpack)


    try:
        loop.run_until_complete(
            # run(
                communicate(loop, echo=echo)
                # log_tcp_10001()
            # )
        )
        # loop.run_until_complete(cancel_pending_tasks(loop))
        log.info("Finally.")

    except asyncio.CancelledError as ex:
        log.info("Canceled: %s", ex)

    except Exception as ex:
        log.info("Error: %s", ex)

    finally:
        loop.close()

    log.info("foobar")
