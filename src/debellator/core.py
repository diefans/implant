"""The core module is transfered to the remote process and will bootstrap pipe communication.

It creates default io queues and distributes commands accordingly.

"""  # pylint: disable=C0302
import abc
import asyncio
import concurrent
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
import types
import weakref
import zlib
from collections import defaultdict

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


def encode_msgpack(data):
    try:
        return msgpack.packb(data, default=msgpack_default_encoder.encode, use_bin_type=True, encoding="utf-8")

    except:
        log.error("Error packing:\n%s", traceback.format_exc())
        raise


def decode_msgpack(data):
    try:
        return msgpack.unpackb(data, object_hook=msgpack_default_encoder.decode, encoding="utf-8")

    except:
        log.error("Error unpacking:\n%s", traceback.format_exc())
        raise


encode = encode_msgpack
decode = decode_msgpack


class aenumerate:

    """Enumerate an async iterator."""

    def __init__(self, aiterable, start=0):
        self._ait = aiterable
        self._i = start

    async def __aiter__(self):
        return self

    async def __anext__(self):
        val = await self._ait.__anext__()
        try:
            return self._i, val
        finally:
            self._i += 1


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

    """Represent a uinique id for the current thread."""

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

    @classmethod
    def __msgpack_encode__(cls, data, data_type):
        return data.bytes

    @classmethod
    def __msgpack_decode__(cls, encoded, data_type):
        return cls(bytes=encoded)


def create_module(module_name, is_package=False):
    """Create an empty module and all its parent packages."""
    if module_name not in sys.modules:
        package_name, _, module_attr = module_name.rpartition('.')
        module = types.ModuleType(module_name)
        module.__file__ = '<memory>'

        if package_name:
            package = create_module(package_name, is_package=True)
            # see https://docs.python.org/3/reference/import.html#submodules
            setattr(package, module_attr, module)

        if is_package:
            module.__package__ = module_name
            module.__path__ = []

        else:
            module.__package__ = package_name

        sys.modules[module_name] = module

    return sys.modules[module_name]


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

        This is a short and faster implementation the original one
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

        log.warning("Connection lost! Shutting down...")
        os.kill(os.getpid(), signal.SIGHUP)


class Outgoing:

    """A context for an outgoing pipe."""

    def __init__(self, *, pipe=sys.stdout, shutdown=False):
        self.pipe = os.fdopen(pipe) if isinstance(pipe, int) else pipe
        self.transport = None
        self.shutdown = shutdown

    async def __aenter__(self):
        self.transport, protocol = await asyncio.get_event_loop().connect_write_pipe(
            ShutdownOnConnectionLost if self.shutdown else asyncio.streams.FlowControlMixin,
            self.pipe
        )

        writer = asyncio.streams.StreamWriter(self.transport, protocol, None, asyncio.get_event_loop())
        return writer

    async def __aexit__(self, exc_type, value, tb):
        self.transport.close()


def split_data(data, size=1024):
    """A generator to yield splitted data."""
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
    [!16s]     [!Q: 8 bytes]                     [!H: 2 bytes]        [!I: 4 bytes]
    {data uuid}{flags: compression|eom|stop_iter}{channel_name length}{data length}{channel_name}{data}

    """
    assert isinstance(uid, Uid), "uid must be an Uid instance"

    if flags is None:
        flags = {}

    if channel_name:
        encoded_channel_name = channel_name.encode()
        channel_name_length = len(encoded_channel_name)
    else:
        channel_name_length = 0

    data_length = data and len(data) or 0
    chunk_flags = ChunkFlags(**flags)

    header = struct.pack(HEADER_FMT, uid.bytes, chunk_flags.encode(), channel_name_length, data_length)
    check = hashlib.md5(header).digest()

    return b''.join((header, check))


def _decode_header(header):
    assert hashlib.md5(header[:-16]).digest() == header[-16:], "Header checksum mismatch!"
    uid_bytes, flags_encoded, channel_name_length, data_length = struct.unpack(HEADER_FMT, header[:-16])

    uid = Uid(bytes=uid_bytes)
    return uid, ChunkFlags.decode(flags_encoded), channel_name_length, data_length


class IoQueues:

    """Just to keep send and receive queues together."""

    chunk_size = 0x8000

    acknowledgements = weakref.WeakValueDictionary()
    """Global acknowledgment futures distinctive by uid."""

    def __init__(self, send=None):
        self.outgoing = asyncio.Queue() if send is None else send
        self.incomming = defaultdict(asyncio.Queue)

    def __getitem__(self, channel_name):
        return self.incomming[channel_name]

    def __delitem__(self, channel_name):
        try:
            del self.incomming[channel_name]

        except KeyError:
            # we ignore missing queues, since not all commands use it
            pass

    async def communicate(self, reader, writer):
        """Schedule send and receive tasks.

        :param reader: the `StreamReader` instance
        :param writer: the `StreamWriter` instance

        """
        fut_send_recv = asyncio.gather(
            self._send_writer(writer),
            self._receive_reader(reader)
        )

        await fut_send_recv

    async def _send_writer(self, writer):
        # send outgoing queue to writer
        queue = self.outgoing

        try:
            while True:
                data = await queue.get()
                if isinstance(data, tuple):
                    for part in data:
                        writer.write(part)
                else:
                    writer.write(data)

                queue.task_done()
                await writer.drain()

        except asyncio.CancelledError:
            if queue.qsize():
                log.warning("Send queue was not empty when canceled!")

        except:
            log.error("Error while sending:\n%s", traceback.format_exc())
            raise

    async def _receive_single_message(self, reader, buffer):
        # read header
        raw_header = await reader.readexactly(HEADER_SIZE + 16)
        uid, flags, channel_name_length, data_length = _decode_header(raw_header)

        if channel_name_length:
            channel_name = (await reader.readexactly(channel_name_length)).decode()

        if data_length:
            if uid not in buffer:
                buffer[uid] = bytearray()

            part = await reader.readexactly(data_length)
            if flags.compression:
                part = zlib.decompress(part)

            buffer[uid].extend(part)

        if channel_name_length:
            log.debug("Channel %s receives: %s bytes", channel_name, data_length)
        else:
            log.debug("Message %s, received: %s", uid, flags)

        if flags.send_ack:
            # we have to acknowledge the reception
            await self._send_ack(uid)

        if flags.eom:
            # put message into channel queue
            if uid in buffer and channel_name_length:
                msg = decode(buffer[uid])
                await self[channel_name].put(msg)
                del buffer[uid]

            # acknowledge reception
            ack_future = self.acknowledgements.get(uid)
            if ack_future and flags.recv_ack:
                duration = time.time() - uid.time
                ack_future.set_result((uid, duration))

    async def _receive_reader(self, reader):
        # receive incomming data into queues
        buffer = {}
        try:
            while True:
                await self._receive_single_message(reader, buffer)

        except asyncio.IncompleteReadError:
            # incomplete is always a cancellation
            log.warning("While waiting for data, we received EOF!")

        except asyncio.CancelledError:
            if buffer:
                log.warning("Receive buffer was not empty when canceled!")

        except:
            log.error("Error while receiving:\n%s", traceback.format_exc())
            raise

    async def _send_ack(self, uid):
        # no channel_name, no data
        header = _encode_header(uid, None, None, flags={
            'eom': True, 'recv_ack': True
        })

        await self.outgoing.put(header)

    async def send(self, channel_name, data, ack=False, compress=6):
        """Send data in a encoded form to the channel.

        :param data: the python object to send
        :param ack: request acknowledgement of the reception of that message
        :param compress: compress the data with zlib

        """
        uid = Uid()
        encoded_channel_name = channel_name.encode()
        loop = asyncio.get_event_loop()

        with concurrent.futures.ThreadPoolExecutor() as executor:
            encoded_data = await loop.run_in_executor(executor, encode, data)

        log.debug("Channel %s sends: %s bytes", channel_name, len(encoded_data))

        for part in split_data(encoded_data, self.chunk_size):
            if compress:
                raw_len = len(part)
                part = zlib.compress(part, compress)
                comp_len = len(part)

                log.debug("Compression ratio of %s -> %s: %.2f%%", raw_len, comp_len, comp_len * 100 / raw_len)

            header = _encode_header(uid, channel_name, part, flags={
                'eom': False, 'send_ack': False, 'compression': bool(compress)
            })

            await self.outgoing.put((header, encoded_channel_name, part))

        header = _encode_header(uid, channel_name, None, flags={
            'eom': True, 'send_ack': ack, 'compression': False
        })

        # if acknowledgement is asked for, we await this future and return its result
        # see _receive_reader for resolution of future
        if ack:
            ack_future = asyncio.Future()
            self.acknowledgements[uid] = ack_future

        await self.outgoing.put((header, encoded_channel_name))

        if ack:
            return await ack_future


class Channel:

    """Channel provides means to send and receive messages."""

    def __init__(self, name=None, *, io_queues=None):
        """Initialize the channel.

        :param name: the channel name
        :param io_queues: the queues to send and receive with

        """
        self.name = name
        self.io_queues = io_queues or IoQueues()
        self.io_outgoing = self.io_queues.outgoing
        self.io_incomming = self.io_queues[self.name]

    def __repr__(self):
        return '<{0.name} {in_size} / {out_size}>'.format(
            self,
            in_size=self.io_incomming.qsize(),
            out_size=self.io_outgoing.qsize(),
        )

    def __await__(self):
        """Receive the next message in this channel."""
        async def coro():
            msg = await self.io_incomming.get()

            try:
                return msg
            finally:
                self.io_incomming.task_done()

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

    async def send(self, data, ack=False, compress=6):
        """Send data in a encoded form to the channel.

        :param data: the python object to send
        :param ack: request acknowledgement of the reception of that message
        :param compress: compress the data with zlib

        """
        return await self.io_queues.send(self.name, data, ack=ack, compress=compress)


# FIXME at the moment the exclusive lock is global for all calls
# we should bind it somehow to the used io_queues/remote instance
def exclusive(fun):
    """Make an async function call exclusive."""
    lock = asyncio.Lock()

    async def locked_fun(*args, **kwargs):
        async with lock:
            log.debug("Executing locked function: %s -> %s", lock, fun)
            return await fun(*args, **kwargs)

    return locked_fun


class _CommandMeta(type):

    base = None
    commands = {}
    # commands = defaultdict(dict)

    command_instances = weakref.WeakValueDictionary()
    """Holds references to all active Instances of Commands, to forward to their queue"""

    def __new__(mcs, name, bases, dct):
        """Register command at plugin vice versa."""
        module_name = dct['__module__']
        command_name = ':'.join((module_name, name))
        dct['command_name'] = command_name

        cls = type.__new__(mcs, name, bases, dct)

        if mcs.base is None:
            mcs.base = cls
        else:
            # only register classes except base class
            mcs.commands[command_name] = cls

        return cls

    @classmethod
    def _register_command(mcs, cls):
        cls.plugin.commands[cls.__name__] = cls

        for plugin_name in set((cls.plugin.module_name, cls.plugin.name)):
            name = ':'.join((plugin_name, cls.__name__))
            mcs.commands[name] = cls

    def __getitem__(cls, value):
        return cls.command_instances[value]

    @classmethod
    def _lookup_command_classmethods(mcs, *names):
        valid_names = set(['local_setup', 'remote_setup'])
        names = set(names) & valid_names

        for command in mcs.commands.values():
            for name, attr in inspect.getmembers(command, inspect.ismethod):
                if name in names:
                    yield command, name, attr

    @classmethod
    async def local_setup(mcs, *args, **kwargs):
        for command, name, func in mcs._lookup_command_classmethods('local_setup'):
            log.info('Setup %s.%s.%s', command.__module__, command.__name__, name)
            await func(*args, **kwargs)

    @classmethod
    async def remote_setup(mcs, *args, **kwargs):
        for command, name, func in mcs._lookup_command_classmethods('remote_setup'):
            log.info('Setup %s.%s.%s', command.__module__, command.__name__, name)
            await func(*args, **kwargs)

    def create_reference(cls, uid, inst):
        fqin = (cls.command_name, uid)
        cls.command_instances[fqin] = inst


class Command(metaclass=_CommandMeta):

    """Base command class, which has no other use than provide the common ancestor to all Commands."""

    def __init__(self, io_queues, command_uid=None, **params):
        super(Command, self).__init__()
        self.uid = command_uid or Uid()
        self.io_queues = io_queues
        self.params = params
        self.__class__.create_reference(self.uid, self)

    def __getattr__(self, name):
        """Forward attribute lookup to params dict, if not found"""
        try:
            return super(Command, self).__getattr__(name)

        except AttributeError:
            try:
                return self.params[name]

            except KeyError:
                raise AttributeError(
                    "'{}' has neither an attribute nor a parameter '{}'".format(self, name)
                )

    def __contains__(self, name):
        return self.params.__contains__(name)

    def __getitem__(self, name):
        return self.params.__getitem__(name)

    @classmethod
    def create_command(cls, io_queues, command_name, command_uid=None, **params):
        """Create a new Command instance and assign a uid to it."""
        command_class = cls.commands.get(command_name)

        if inspect.isclass(command_class) and issubclass(command_class, Command):
            # do something and return result
            command = command_class(io_queues, command_uid=command_uid, **params)

            return command

        raise KeyError('The command `{}` does not exist!'.format(command_name))

    async def execute(self):
        """Execute the command by delegating to Execute command."""
        execute = Execute(self.io_queues, self)
        result = await execute.local()

        return result

    async def local(self, remote_future):
        raise NotImplementedError(
            'You have to implement a `local` method'
            'for your Command to work: {}'.format(self.__class__)
        )

    async def remote(self):
        """An empty remote part."""

    @reify
    def fqin(self):
        """The fully qualified instance name."""
        return (self.command_name, self.uid)

    @reify
    def channel_name(self):
        """Channel name is used in header."""
        return '/'.join(map(str, self.fqin))

    @reify
    def channel(self):
        return Channel(self.channel_name, io_queues=self.io_queues)

    def __repr__(self):
        return self.channel_name


class ExecuteException(MsgpackEncoder):

    """Remote execution ended in an exception."""

    def __init__(self, fqin, exception, tb=None):
        self.fqin = fqin
        self.exception = exception
        self.tb = tb or traceback.format_exc()

    async def __call__(self, io_queues):
        future = Execute.pending_commands[self.fqin]
        log.error("Remote exception for %s:\n%s", self.fqin, self.tb)
        future.set_exception(self.exception)

    @classmethod
    def __msgpack_encode__(cls, data, data_type):
        return (data.fqin, data.exception, data.tb)

    @classmethod
    def __msgpack_decode__(cls, encoded, data_type):
        fqin, exc, tb = encoded
        return cls(fqin, exc, tb)


class ExecuteResult(MsgpackEncoder):

    """The result of a remote execution."""

    def __init__(self, fqin, result=None):        # noqa
        self.fqin = fqin
        self.result = result

    async def __call__(self, io_queues):
        future = Execute.pending_commands[self.fqin]
        future.set_result(self.result)

    @classmethod
    def __msgpack_encode__(cls, data, data_type):
        return (data.fqin, data.result)

    @classmethod
    def __msgpack_decode__(cls, encoded, data_type):
        return cls(*encoded)


class ExecuteArgs(MsgpackEncoder):

    """Arguments for an execution."""

    def __init__(self, fqin, params=None):
        self.fqin = fqin
        self.params = params

    async def __call__(self, io_queues):
        command = Execute.create_command(io_queues, *self.fqin, **self.params)
        execute = Execute(io_queues, command)
        asyncio.ensure_future(execute.remote())

    @classmethod
    def __msgpack_encode__(cls, data, data_type):
        return (data.fqin, data.params)

    @classmethod
    def __msgpack_decode__(cls, encoded, data_type):
        return cls(*encoded)


class Execute(Command):

    """The executor of all commands.

    This class should not be called directly!
    """

    pending_commands = defaultdict(asyncio.Future)

    def __init__(self, io_queues, command):
        super(Execute, self).__init__(io_queues)

        self.command = command

    async def execute(self):
        # forbid using Execute directly
        raise RuntimeError("Do not invoke `await Execute()` directly, instead use `await Command(**params)`)")

    @reify
    def channel_name(self):
        """Execution is always run on the class channel."""
        return self.__class__.command_name

    @classmethod
    async def execute_io_queues(cls, io_queues):
        # listen to the global execute channel
        channel = Channel(cls.command_name, io_queues=io_queues)

        try:
            async for message in channel:
                log.debug("*** Received execution message: %s", message)
                await message(io_queues)

        except asyncio.CancelledError:
            pass

        # teardown here
        for fqin, fut in cls.pending_commands.items():
            log.warning("Teardown pending command: %s, %s", fqin, fut)
            fut.cancel()
            del cls.pending_commands[fqin]

    @classmethod
    async def local_setup(cls, io_queues):
        """Wait for ExecuteResult messages and resolves waiting futures."""
        asyncio.ensure_future(cls.execute_io_queues(io_queues))

    @classmethod
    async def remote_setup(cls, io_queues):
        asyncio.ensure_future(cls.execute_io_queues(io_queues))

    def remote_future(self):        # noqa
        """Create remote command and yield its future."""
        class _context:
            async def __aenter__(ctx):      # noqa
                await self.channel.send(ExecuteArgs(self.command.fqin, self.command.params), ack=True)
                future = self.pending_commands[self.command.channel_name]

                return future

            async def __aexit__(ctx, *args):        # noqa
                del self.pending_commands[self.command.channel_name]

        return _context()

    async def local(self):
        async with self.remote_future() as future:
            try:
                log.debug("Excute command: %s", self.command)
                # execute local side of command
                result = await self.command.local(remote_future=future)
                future.result()
                return result

            except:     # noqa
                log.error("Error while executing command: %s\n%s", self.command, traceback.format_exc())
                raise

            finally:
                # cleanup channel
                del self.io_queues[self.command.channel_name]

    async def remote(self):
        fqin = self.command.channel_name

        try:
            # execute remote side of command
            result = await self.command.remote()
            await self.channel.send(ExecuteResult(fqin, result=result))

            return result

        except Exception as ex:
            log.error("traceback:\n%s", traceback.format_exc())
            await self.channel.send(ExecuteException(fqin, exception=ex))

            raise

        finally:
            # cleanup channel
            del self.io_queues[self.command.channel_name]


class InvokeImport(Command):

    """Invoke an import of a module on the remote side.

    The local side will import the module first.
    The remote side will trigger the remote import hook, which in turn
    will receive all missing modules from the local side.

    The import is executed in a separate executor thread, to have a separate event loop available.

    """

    @exclusive
    async def execute(self):
        return await super(InvokeImport, self).execute()

    async def local(self, remote_future):
        importlib.import_module(self.fullname)
        result = await remote_future
        return result

    async def remote(self):
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

    async def local(self, remote_future):
        module_loaded = await remote_future

        return module_loaded

    def _is_namespace(self, module):
        # see https://www.python.org/dev/peps/pep-0451/#how-loading-will-work
        spec = module.__spec__
        is_namespace = spec.loader is None and spec.submodule_search_locations is not None
        return is_namespace

    def _is_package(self, module):
        is_package = bool(getattr(module, '__path__', None) is not None)
        return is_package

    def _get_source(self, module):
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

    def _get_source_filename(self, module):
        spec = module.__spec__
        if isinstance(spec.loader, importlib.abc.ExecutionLoader):
            filename = spec.loader.get_filename(module.__name__)
        else:
            filename = inspect.getsourcefile(module)

        return filename

    async def remote(self):
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
            log.error('Module not loaded: %s', self.module_name)
            return None


class RemoteModuleFinder(importlib.abc.MetaPathFinder):

    """Import hook that schedules a `FindModule` coroutine in the main loop.

    The import itself is run in a separate executor thread to keep things async.

    http://stackoverflow.com/questions/32059732/send-asyncio-tasks-to-loop-running-in-other-thread
    https://www.python.org/dev/peps/pep-0302/
    https://www.python.org/dev/peps/pep-0420/
    https://www.python.org/dev/peps/pep-0451/
    """

    def __init__(self, io_queues, loop):
        self.io_queues = io_queues
        self.main_loop = loop

    def _find_remote_module(self, module_name):
        # ask master for this module
        log.debug("Module lookup: %s", module_name)

        future = asyncio.run_coroutine_threadsafe(
            FindModule(self.io_queues, module_name=module_name).execute(),
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

class RemoteNamespaceFinder(importlib.abc.MetaPathFinder):
    def _find_remote_namespace(self, fullname):
        pass

    def find_loader(self, fullname):
        pass


async def run(*tasks):
    """Schedule all tasks and wait for running is done or canceled."""
    # create indicator for running messenger
    running = asyncio.ensure_future(asyncio.gather(*tasks))

    # exit on sigterm or sigint
    for signame in ('SIGINT', 'SIGTERM', 'SIGHUP', 'SIGQUIT'):
        sig = getattr(signal, signame)

        def exit_with_signal(sig):
            try:
                running.cancel()

            except asyncio.InvalidStateError:
                log.warning("running already done!")

        asyncio.get_event_loop().add_signal_handler(sig, functools.partial(exit_with_signal, sig))

    # wait for running completed
    try:
        result = await running
        return result

    except asyncio.CancelledError:
        pass


def cancel_pending_tasks(loop):
    for task in asyncio.Task.all_tasks():
        if task.done() or task.cancelled():
            continue

        task.cancel()
        try:
            loop.run_until_complete(task)

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


async def communicate(io_queues):
    async with Incomming(pipe=sys.stdin) as reader:
        async with Outgoing(pipe=sys.stdout, shutdown=True) as writer:
            await Channel.communicate(io_queues, reader, writer)


class ExecutorConsoleHandler(logging.StreamHandler):

    # FIXME TODO Executor seems to disturb uvloop so that it hangs randomly

    """Run logging in a separate executor, to not block on console output."""

    def __init__(self, *args, **kwargs):
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=150)

        super(ExecutorConsoleHandler, self).__init__(*args, **kwargs)

    def emit(self, record):
        # FIXME is not really working on sys.stdout
        asyncio.get_event_loop().run_in_executor(
            self.executor, functools.partial(super(ExecutorConsoleHandler, self).emit, record)
        )

    def __del__(self):
        self.executor.shutdown(wait=True)


def main(debug=False, log_config=None, **kwargs):
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
                    'level': 'INFO',
                    'propagate': False
                }
            },
            'root': {
                'handlers': ['console'],
                'level': 'DEBUG'
            },
        }

    logging.config.dictConfig(log_config)
    loop = asyncio.get_event_loop()

    if debug:
        log.setLevel(logging.DEBUG)
        loop.set_debug(debug)

    io_queues = IoQueues()

    # setup all plugin commands
    loop.run_until_complete(Command.remote_setup(io_queues))

    # install import hook
    remote_module_finder = RemoteModuleFinder(io_queues, loop)
    sys.meta_path.append(remote_module_finder)

    log.debug("meta path: %s", sys.meta_path)
    log.debug("msgpack used: %s", msgpack)

    try:
        loop.run_until_complete(
            run(
                communicate(io_queues)
                # log_tcp_10001()
            )
        )
        cancel_pending_tasks(loop)

    finally:
        loop.close()
