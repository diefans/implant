"""The core module is transfered to the remote process and will bootstrap pipe communication.

It creates default channels and dispatches commands accordingly.

"""  # pylint: disable=C0302
import abc
import asyncio
import collections
import concurrent
import functools
import hashlib
import importlib.abc
import importlib.machinery
import importlib.util
import inspect
import io
import logging
import logging.config
import os
import struct
import sys
import threading
import time
import traceback
import weakref
import zlib

import umsgpack

log = logging.getLogger(__name__)


class MsgpackMeta(abc.ABCMeta):
    ext_handlers_encode = {}
    ext_handlers_decode = {}
    custom_encoders = {}

    def register(cls, data_type=None, ext_code=None):
        def decorator(handler):
            if not issubclass(handler, Msgpack):
                raise TypeError("Msgpack handler must be a subclass"
                                " of abstract `Msgpack` class: {}".format(handler))
            if data_type is None:
                _data_type = handler
            else:
                _data_type = data_type

            if ext_code is not None:
                cls.ext_handlers_encode[_data_type] = \
                    lambda data: umsgpack.Ext(ext_code, handler.__msgpack_encode__(data, _data_type))
                cls.ext_handlers_decode[ext_code] = \
                    lambda ext: handler.__msgpack_decode__(ext.data, _data_type)
            else:
                cls.custom_encoders[_data_type] = handler
            return handler
        return decorator

    def _pack(cls, obj, fp, **options):
        # pylint: disable=W0212
        ext_handlers = options.get("ext_handlers")
        # lookup mro except object for matching handler
        ext_handler_match = next((
            obj_cls for obj_cls in obj.__class__.__mro__[:-1]
            if obj_cls in ext_handlers
        ), None) if ext_handlers else None

        if obj is None:
            umsgpack._pack_nil(obj, fp, options)
        elif ext_handler_match:
            umsgpack._pack_ext(ext_handlers[ext_handler_match](obj), fp, options)
        elif isinstance(obj, bool):
            umsgpack._pack_boolean(obj, fp, options)
        elif isinstance(obj, int):
            umsgpack._pack_integer(obj, fp, options)
        elif isinstance(obj, float):
            umsgpack._pack_float(obj, fp, options)
        elif umsgpack.compatibility and isinstance(obj, str):
            umsgpack._pack_oldspec_raw(obj.encode('utf-8'), fp, options)
        elif umsgpack.compatibility and isinstance(obj, bytes):
            umsgpack._pack_oldspec_raw(obj, fp, options)
        elif isinstance(obj, str):
            umsgpack._pack_string(obj, fp, options)
        elif isinstance(obj, bytes):
            umsgpack._pack_binary(obj, fp, options)
        elif isinstance(obj, (tuple, list)):
            umsgpack._pack_array(obj, fp, options)
        elif isinstance(obj, dict):
            umsgpack._pack_map(obj, fp, options)
        elif isinstance(obj, umsgpack.Ext):
            umsgpack._pack_ext(obj, fp, options)
        # default fallback
        elif ext_handlers and object in ext_handlers:
            umsgpack._pack_ext(ext_handlers[object](obj), fp, options)
        else:
            raise umsgpack.UnsupportedTypeException(
                "unsupported type: %s" % str(type(obj)))

    def _packb(cls, obj, **options):
        fp = io.BytesIO()
        cls._pack(obj, fp, **options)
        return fp.getvalue()

    def encode(cls, data):
        encoded_data = cls._packb(data, ext_handlers=cls.ext_handlers_encode)
        return encoded_data

    def decode(cls, encoded_data):
        data = umsgpack.unpackb(encoded_data, ext_handlers=cls.ext_handlers_decode)
        return data

    def get_custom_encoder(cls, data_type):
        if issubclass(data_type, Msgpack):
            return data_type

        # lookup data types for registered encoders
        for subclass in data_type.__mro__:
            try:
                return cls.custom_encoders[subclass]
            except KeyError:
                continue
        return None


class Msgpack(metaclass=MsgpackMeta):

    """Add msgpack en/decoding to a type."""

    @abc.abstractclassmethod
    def __msgpack_encode__(cls, data, data_type):
        return None

    @abc.abstractclassmethod
    def __msgpack_decode__(cls, encoded_data, data_type):
        return None

    @classmethod
    def __subclasshook__(cls, C):
        if cls is Msgpack:
            if any("__msgpack_encode__" in B.__dict__ for B in C.__mro__) \
                    and any("__msgpack_decode__" in B.__dict__ for B in C.__mro__):
                return True
        return NotImplemented


encode = Msgpack.encode
decode = Msgpack.decode


@Msgpack.register(object, 0x01)
class CustomEncoder:
    @classmethod
    def __msgpack_encode__(cls, data, data_type):
        data_type = type(data)
        encoder = Msgpack.get_custom_encoder(data_type)
        if encoder is None:
            raise TypeError("There is no custom encoder for this type registered: {}".format(data_type))

        wrapped = {
            'type': data_type.__name__,
            'module': data_type.__module__,
            'data': encoder.__msgpack_encode__(data, data_type)
        }
        return encode(wrapped)

    @classmethod
    def __msgpack_decode__(cls, encoded_data, data_type):
        wrapped = decode(encoded_data)

        try:
            module = sys.modules[wrapped['module']]
        except KeyError:
            raise
            # XXX should we import the module?

        data_type = getattr(module, wrapped['type'])
        encoder = Msgpack.get_custom_encoder(data_type)
        if encoder is None:
            raise TypeError("There is no custom encoder for this type registered: {}".format(data_type))

        data = encoder.__msgpack_decode__(wrapped['data'], data_type)
        return data


@Msgpack.register(tuple, 0x02)
class TupleEncoder:
    @classmethod
    def __msgpack_encode__(cls, data, data_type):
        return encode(list(data))

    @classmethod
    def __msgpack_decode__(cls, encoded_data, data_type):
        return tuple(decode(encoded_data))


@Msgpack.register(set, 0x03)
class SetEncoder:
    @classmethod
    def __msgpack_encode__(cls, data, data_type):
        return encode(list(data))

    @classmethod
    def __msgpack_decode__(cls, encoded_data, data_type):
        return set(decode(encoded_data))


@Msgpack.register(Exception, 0x04)
class ExceptionEncoder:
    @classmethod
    def __msgpack_encode__(cls, data, data_type):
        return encode(data.args)

    @classmethod
    def __msgpack_decode__(cls, encoded_data, data_type):
        return data_type(*decode(encoded_data))


@Msgpack.register(StopAsyncIteration, 0x05)
class StopAsyncIterationEncoder:
    @classmethod
    def __msgpack_encode__(cls, data, data_type):
        return encode(data.args)

    @classmethod
    def __msgpack_decode__(cls, encoded_data, data_type):
        return StopAsyncIteration(*decode(encoded_data))


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

    def __init__(self, *, pipe=sys.stdin, loop=None):
        super(Incomming, self).__init__(loop=loop)
        self.pipe = os.fdopen(pipe) if isinstance(pipe, int) else pipe

    async def __aenter__(self):
        await self.connect()
        return self

    async def connect(self):
        protocol = asyncio.StreamReaderProtocol(self)

        transport, protocol = await self._loop.connect_read_pipe(
            lambda: protocol,
            self.pipe,
        )

        return transport, protocol

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

    def __init__(self, *, pipe=sys.stdout, reader=None, shutdown=False, loop=None):
        self.loop = loop if loop is not None else asyncio.get_event_loop()
        self.pipe = os.fdopen(pipe) if isinstance(pipe, int) else pipe
        self.transport = None
        self.reader = reader
        self.shutdown = shutdown

    async def __aenter__(self):
        writer = await self.connect()
        return writer

    async def connect(self):
        self.transport, protocol = await self.loop.connect_write_pipe(
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
        self.transport.close()


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

    def __init__(self, reader, writer, *, loop=None):
        self.loop = loop if loop is not None else asyncio.get_event_loop()
        self.incomming = weakref.WeakValueDictionary()

        self.reader = reader
        self.writer = writer

        self._lock_communicate = asyncio.Lock(loop=self.loop)

    def get_channel(self, channel_name):
        """Create a channel and weakly register its queue.

        :param channel_name: the name of the channel to create

        """
        queue = asyncio.Queue(loop=self.loop)
        try:
            channel = Channel(
                channel_name,
                send=functools.partial(self.send, channel_name),
                queue=queue
            )
            return channel

        finally:
            self.incomming[channel_name] = queue

    async def enqueue(self):
        """Schedule receive tasks.

        Incomming chunks are collected and stored in the appropriate channel queue.
        """
        async with self._lock_communicate:
            # start receiving
            fut_receive_reader = asyncio.ensure_future(self._receive_reader(), loop=self.loop)
            try:
                # never ending
                await asyncio.Future(loop=self.loop)
            except asyncio.CancelledError:
                log.info("Shutdown of message enqueueing")

            # stop receiving new messages
            fut_receive_reader.cancel()
            await fut_receive_reader

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

    @staticmethod
    def _feed_data(buffer, chunk):
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
        encoded_data = encode(data)

        log.debug("%s: channel `%s` sends: %s bytes", uid, channel_name, len(encoded_data))

        for part in split_data(encoded_data, self.chunk_size):
            if compress:
                raw_len = len(part)
                part = zlib.compress(part, compress)
                comp_len = len(part)

                log.debug("%s: compression ratio of %s -> %s: %.2f%%",
                          uid, raw_len, comp_len, comp_len * 100 / raw_len)

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
            ack_future = asyncio.Future(loop=self.loop)
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

    async def send_iteration(self, iterable):
        if isinstance(iterable, collections.abc.AsyncIterable):
            log.debug("Channel %s sends async iterable: %s", self, iterable)
            async for value in iterable:
                await self.send(value)
        else:
            log.debug("Channel %s sends iterable: %s", self, iterable)
            for value in iterable:
                await self.send(value)
        await self.send(StopAsyncIteration())


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


DispatchLocalContext = collections.namedtuple(
    'DispatchContext',
    ('channel', 'execute', 'fqin', 'remote_future')
)
DispatchRemoteContext = collections.namedtuple(
    'DispatchContext',
    ('channel', 'execute', 'fqin', 'pending_remote_task')
)
DISPATCHER_CHANNEL_NAME = 'Dispatcher'


class Dispatcher:

    """Enables execution of `Command`s.

    A `Command` is split into local and remote part, where a context with
    a dedicated `Channel` is provided to enable streaming of arbitrary data.
    The local part also gets a remote future passed, which resolves to the
    result of the remote part of the `Command`.

    """

    def __init__(self, io_queues, *, loop=None):
        self.loop = loop if loop is not None else asyncio.get_event_loop()
        self.io_queues = io_queues
        self.channel = self.io_queues.get_channel(DISPATCHER_CHANNEL_NAME)
        self.pending_commands = collections.defaultdict(functools.partial(asyncio.Future, loop=self.loop))
        self.pending_dispatches = collections.defaultdict(functools.partial(asyncio.Event, loop=self.loop))
        self.pending_remote_tasks = set()

        self._lock_dispatch = asyncio.Lock(loop=self.loop)

    async def dispatch(self):
        """Start sending and receiving messages and executing them."""
        async with self._lock_dispatch:
            fut_execute_io_queues = asyncio.ensure_future(self._execute_io_queues(), loop=self.loop)
            try:
                # never ending
                await asyncio.Future(loop=self.loop)
            except asyncio.CancelledError:
                log.info("Shutdown of dispatcher")

            for task in self.pending_remote_tasks:
                log.info("Waiting for task to finalize: %s", task)
                await task

            fut_execute_io_queues.cancel()
            await fut_execute_io_queues

    async def _execute_io_queues(self):
        """Execute messages sent via our `Dispatcher.channel`."""
        log.info("Listening on channel %s for command dispatch...", self.channel)

        try:
            async for message in self.channel:
                log.debug("Received dispatch message: %s", message)
                await message(self)

        except asyncio.CancelledError:
            pass

        finally:
            # teardown here
            for fqin, fut in list(self.pending_commands.items()):
                log.warning("Teardown pending command: %s, %s", fqin, fut)
                await fut
                del self.pending_commands[fqin]

    def create_command(self, command_name, params):
        command_class = Command[command_name] if isinstance(command_name, str) else command_name
        command = command_class(params, loop=self.loop)
        return command

    async def execute(self, command_name, **params):
        """Execute a command.

        First creating the remote side and its future
        and second executing its local part.

        """
        command = self.create_command(command_name, params)
        fqin = command.__class__.create_fqin()

        await self.channel.send(DispatchCommand(fqin, *command.dispatch_data), ack=True)

        async with self.remote_future(fqin, command) as future:
            context = self.local_context(fqin, future)
            try:
                evt = self.pending_dispatches[fqin]
                log.debug("%s - waiting for dispatch to be ready", fqin)
                await evt.wait()
                log.debug("%s - execute command: %s", fqin, command)
                # execute local side of command
                result = await command.local(context)
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
                future = self.pending_commands[fqin]

                return future

            async def __aexit__(ctx, *args):        # noqa
                del self.pending_commands[fqin]

        return _context()

    def local_context(self, fqin, remote_future):
        """Create a local context to pass to a `Command`s local part.

        The `Channel` is built via a fully qualified instance name (fqin).

        """
        channel = self.io_queues.get_channel(fqin)
        context = DispatchLocalContext(
            channel=channel,
            execute=self.execute,
            fqin=fqin,
            remote_future=remote_future
        )
        return context

    def remote_context(self, fqin, pending_remote_task):
        """Create a remote context to pass to a `Command`s remote part.

        The `Channel` is built via a fully qualified instance name (fqin).

        """
        channel = self.io_queues.get_channel(fqin)
        context = DispatchRemoteContext(
            channel=channel,
            execute=self.execute,
            fqin=fqin,
            pending_remote_task=pending_remote_task
        )
        return context

    async def execute_remote(self, fqin, command):
        """Execute the remote part of a `Command`.

        This method is called by a `DispatchCommand` message.

        The result is send via `Dispatcher.channel` to resolve the pending command future.

        """
        # TODO current_task is not a stable API see PyO3/tokio#54
        current_task = asyncio.Task.current_task()
        self.pending_remote_tasks.add(current_task)
        log.debug("Starting remote task: %s", fqin)
        context = self.remote_context(fqin, current_task)
        await self.channel.send(DispatchReady(fqin))
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
            log.debug("Finalizing remote task: %s", fqin)
            self.pending_remote_tasks.remove(current_task)


class _CommandMeta(type):
    base = None
    commands = {}

    def __new__(mcs, name, bases, dct):
        """Create Command class.

        Add command_name as __module__:__name__
        Collect parameters
        """
        dct['command_name'] = ':'.join((dct['__module__'], dct['__qualname__']))
        dct['parameters'] = {name: attr for name, attr in dct.items() if isinstance(attr, Parameter)}

        # check for remote outsourcing
        if mcs.base is not None:
            try:
                remote = dct['remote']
            except KeyError:
                raise TypeError("Command must define a remote,"
                                " either as a method or as a dotted name pointing to another class.")

            if isinstance(remote, str):
                dct['_remote_module'], dct['_remote_class'] = remote.rsplit('.', 1)

            else:
                dct['_remote_module'], dct['_remote_class'] = dct['__module__'], dct['__qualname__']

        cls = type.__new__(mcs, name, bases, dct)

        # set parameter names in python < 3.6
        if sys.version_info < (3, 6):
            for attr_name, parameter in cls.parameters.items():
                parameter.__set_name__(cls, attr_name)

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


class NoDefault:
    pass


class Parameter:

    """Define a `Command` parameter."""

    def __init__(self, *, default=NoDefault, description=None):
        self.name = None
        self.default = default
        self.description = description

    def __get__(self, instance, owner):
        if instance is None:
            return self
        try:
            return instance.__dict__[self.name]
        except KeyError:
            if self.default is NoDefault:
                raise AttributeError("The Parameter has no default value "
                                     "and another value was not assigned yet: {}".format(self.name))
            return self.default

    def __set__(self, instance, value):
        instance.__dict__[self.name] = value

    def __set_name__(self, owner, name):
        self.name = name


class Command(metaclass=_CommandMeta):

    """Common ancestor of all Commands."""

    def __init__(self, parameters=None, *, loop=None):
        self._loop = loop if loop is not None else asyncio.get_event_loop()
        if parameters is not None:
            for name, value in parameters.items():
                setattr(self, name, value)

    def __iter__(self):
        return iter((name, getattr(self, name)) for name in self.__class__.parameters)

    def __repr__(self):
        _repr = super().__repr__()
        command_name = self.__class__.command_name
        return "<{command_name} {_repr}>".format(command_name=command_name, _repr=_repr)

    @property
    def dispatch_data(self):
        return (
            self.__class__.command_name,
            self.__class__.__name__,
            self.__class__.__module__,
            dict(self.__iter__())
        )


class DispatchMessage:

    """Base class for command dispatch communication."""

    def __init__(self, fqin):
        self.fqin = fqin

    def __repr__(self):
        return "<{self.__class__.__name__} {self.fqin}>".format(**locals())


class DispatchCommand(DispatchMessage):

    """Arguments for a command dispatch."""

    def __init__(self, fqin, command_name, command_class, command_module, params):
        super().__init__(fqin)
        self.command_name = command_name
        self.command_class = command_class
        self.command_module = command_module
        self.params = params
        log.info("Dispatch created: %s %s", self.fqin, self.params)

    async def __call__(self, dispatcher):
        async def execute():
            if self.command_module not in sys.modules:
                await async_import(self.command_module)

            command = dispatcher.create_command(self.command_name, self.params)
            await dispatcher.execute_remote(self.fqin, command)
        # schedule remote execution
        fut = asyncio.ensure_future(execute())
        fut.add_done_callback(self._send_command_dispatch_errors)

    def _send_command_dispatch_errors(self, fut):
        try:
            fut.result()
        except Exception as ex:     # pylint: disable=W0703
            tb = traceback.format_exc()
            log.error("traceback:\n%s", tb)
            asyncio.ensure_future(self.channel.send(DispatchException(self.fqin, exception=ex, tb=tb)))

    @classmethod
    def __msgpack_encode__(cls, data, data_type):
        return encode((
            data.fqin,
            data.command_name,
            data.command_class,
            data.command_module,
            data.params,
        ))

    @classmethod
    def __msgpack_decode__(cls, encoded, data_type):
        return cls(*decode(encoded))


class DispatchReady(DispatchMessage):

    async def __call__(self, dispatcher):
        log.debug("%s - set dispatch ready", self.fqin)
        evt = dispatcher.pending_dispatches[self.fqin]
        evt.set()

    @classmethod
    def __msgpack_encode__(cls, data, data_type):
        return encode(data.fqin)

    @classmethod
    def __msgpack_decode__(cls, encoded, data_type):
        fqin = decode(encoded)
        return cls(fqin)


class DispatchException(DispatchMessage):

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
        return encode((data.fqin, data.exception, data.tb))

    @classmethod
    def __msgpack_decode__(cls, encoded, data_type):
        fqin, exc, tb = decode(encoded)
        return cls(fqin, exc, tb)


class DispatchResult(DispatchMessage):

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
        return encode((data.fqin, data.result))

    @classmethod
    def __msgpack_decode__(cls, encoded, data_type):
        return cls(*decode(encoded))


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

    event = Parameter(default=None, description='the event instance, which has to be de/encodable via message pack')
    dispatch_local = Parameter(default=False, description='if True, the local side will also be notified')

    async def local(self, context):
        # we wait for remote events to be dispatched first
        await context.remote_future

        if self.dispatch_local:
            log.info("Notify local %s", self.event)
            await notify_event(self.event)

    async def remote(self, context):
        async def notify_after_pending_command_finalized():
            log.debug("Waiting for finalization of remote task: %s", context.fqin)
            await context.pending_remote_task
            log.debug("Notify remote %s", self.event)
            await notify_event(self.event)
        asyncio.ensure_future(notify_after_pending_command_finalized(), loop=self._loop)


class InvokeImport(Command):

    """Invoke an import of a module on the remote side.

    The local side will import the module first.
    The remote side will trigger the remote import hook, which in turn
    will receive all missing modules from the local side.

    The import is executed in a separate executor thread, to have a separate event loop available.

    """

    fullname = Parameter(description='The full module name to be imported')

    async def local(self, context):
        module = importlib.import_module(self.fullname)
        log.debug("Local module: %s", module)
        result = await context.remote_future
        return result

    @exclusive
    async def remote(self, context):
        await async_import(self.fullname)


class FindModule(Command):

    """Find a module on the remote side."""

    fullname = Parameter(description='The full module name to find.')

    async def local(self, context):
        module_loaded = await context.remote_future

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
        module = sys.modules.get(self.fullname)

        if module:
            is_namespace = self._is_namespace(module)
            is_package = self._is_package(module)

            log.debug("module found: %s", module)
            remote_module_data = {
                'name': self.fullname,
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
            log.warning('Module not loaded: %s', self.fullname)
            return None


class FindSpecData(Command):

    """Find spec data for a module to import at the peer side."""

    fullname = Parameter(description='The full module name to find.')

    async def local(self, context):
        pass

    async def remote(self, context):
        spec = importlib.util.find_spec(self.fullname)

        spec_data = {
            'name': spec.name,
            'origin': spec.origin,
            'submodule_search_locations': spec.submodule_search_locations,
            'loader': self._extract_loader_data(spec)
        }

        return spec_data

    def _extract_loader_data(self, spec):
        return {}


class RemoteModuleFinder(importlib.abc.MetaPathFinder):

    """Import hook that schedules a `FindModule` coroutine in the main loop.

    The import itself is run in a separate executor thread to keep things async.

    http://stackoverflow.com/questions/32059732/send-asyncio-tasks-to-loop-running-in-other-thread
    https://www.python.org/dev/peps/pep-0302/
    https://www.python.org/dev/peps/pep-0420/
    https://www.python.org/dev/peps/pep-0451/
    """

    def __init__(self, loop, dispatcher):
        self.main_loop = loop
        self.dispatcher = dispatcher

    def _find_remote_module(self, module_name):
        # ask master for this module
        log.debug("Module lookup: %s", module_name)

        future = asyncio.run_coroutine_threadsafe(
            self.dispatcher.execute(FindModule, fullname=module_name),
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


async def async_import(fullname):
    def import_stuff():
        thread_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(thread_loop)

        try:
            module = importlib.import_module(fullname)
            log.debug("Remotelly imported module: %s", module)

        except ImportError:
            log.debug("Error when importing %s:\n%s", fullname, traceback.format_exc())
            raise

        finally:
            thread_loop.close()

    log.debug("Importing module: %s", fullname)
    loop = asyncio.get_event_loop()
    thread_pool_executor = concurrent.futures.ThreadPoolExecutor()
    await loop.run_in_executor(thread_pool_executor, import_stuff)


class ShutdownRemoteEvent:

    """A Shutdown event."""

    @classmethod
    def __msgpack_encode__(cls, data, data_type):
        return None

    @classmethod
    def __msgpack_decode__(cls, encoded, data_type):
        return data_type()


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


class Core:
    def __init__(self, loop, *, echo=None, **kwargs):
        self.loop = loop
        self.echo = echo

    async def communicate(self, reader, writer):
        try:
            channels = Channels(reader=reader, writer=writer)
            dispatcher = Dispatcher(channels)

            fut_enqueue = asyncio.ensure_future(channels.enqueue())
            fut_dispatch = asyncio.ensure_future(dispatcher.dispatch())

            remote_module_finder = RemoteModuleFinder(self.loop, dispatcher)

            # shutdown is done via event
            @event_handler(ShutdownRemoteEvent)
            async def _shutdown(event):
                log.info("Shutdown remote event")

                self.teardown_import_hook(remote_module_finder)

                fut_dispatch.cancel()
                await fut_dispatch

                fut_enqueue.cancel()
                await fut_enqueue

            self.setup_import_hook(remote_module_finder)

            await asyncio.gather(fut_enqueue, fut_dispatch)
        except asyncio.CancelledError:
            log.info("Cancelled communicate??")
        log.info('Communication end.')

    @staticmethod
    def setup_import_hook(module_finder):
        sys.meta_path.append(module_finder)

    @staticmethod
    def teardown_import_hook(module_finder):
        if module_finder in sys.meta_path:
            sys.meta_path.remove(module_finder)

    def setup_logging(self, debug=False, log_config=None):
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

        if debug:
            log.setLevel(logging.DEBUG)
            self.loop.set_debug(debug)

    async def connect_sysio(self):
        return await self.connect(stdin=sys.stdin, stdout=sys.stdout)

    async def connect(self, *, stdin, stdout, stderr=None):
        log.info(' *** ' * 5)
        log.info("Starting process %s: pid=%s of ppid=%s", __name__, os.getpid(), os.getppid())
        async with Incomming(pipe=stdin) as reader:
            async with Outgoing(pipe=stdout, shutdown=True) as writer:
                # TODO think about generic handshake
                # send echo to master to prove behavior
                if self.echo is not None:
                    writer.write(self.echo)
                    await writer.drain()

                await self.communicate(reader, writer)

    @classmethod
    def main(cls, debug=False, log_config=None, *, loop=None, **kwargs):
        loop = loop if loop is not None else asyncio.get_event_loop()

        # thread_pool_executor = concurrent.futures.ThreadPoolExecutor()
        # loop.set_default_executor(thread_pool_executor)

        core = cls(loop, **kwargs)
        core.setup_logging(debug, log_config)

        try:
            loop.run_until_complete(core.connect_sysio())
        finally:
            loop.close()
            thread_pool_executor.shutdown()
        log.info("exit")


main = Core.main
