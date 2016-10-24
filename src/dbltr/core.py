"""
Remote process
"""
import inspect
import sys
import asyncio
import signal
import functools
import os
import base64
import json
import hashlib
import uuid
import types
import gzip
import lzma
import logging
import weakref
import traceback
import pickle
import struct
import time
import contextlib
from collections import defaultdict, namedtuple
import pkg_resources


pkg_environment = pkg_resources.Environment()


logging.basicConfig(level='DEBUG')
logger = logging.getLogger(__name__)
logger.info('\n%s', '\n'.join(['*' * 80] * 3))


class aenumerate:
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
    def __init__(self, wrapped):
        self.wrapped = wrapped
        functools.update_wrapper(self, wrapped)

    def __get__(self, inst, objtype=None):
        if inst is None:
            return self
        val = self.wrapped(inst)
        setattr(inst, self.wrapped.__name__, val)
        return val


def create_module(module_name, is_package=False):
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


def uuid1_time(uid):
    return (uid.time - 0x01b21dd213814000) * 100 / 1e9


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
        loop_ = asyncio.ProactorEventLoop()  # for subprocess' pipes on Windows
        asyncio.set_event_loop(loop_)

    else:
        loop_ = asyncio.get_event_loop()

    loop_.set_debug(debug)

    return loop_


class IoQueues:
    """Just to keep send and receive queues together."""

    def __init__(self, send=None, error=None):
        if send is None:
            send = asyncio.Queue()

        self.send = send

        self.receive = defaultdict(asyncio.Queue)
        # self.receive = weakref.WeakValueDictionary()

    def __getitem__(self, channel_name):
        try:
            return self.receive[channel_name]

        except KeyError:
            queue = self.receive[channel_name] = asyncio.Queue()
            logger.info("CREATING: %s, %s", channel_name, id(queue))

            return queue

    async def send_to_writer(self, writer):
        """A continuos task to send all data in the send queue to a stream writer."""

        while True:
            data = await self.send.get()
            try:
                writer.write(data)
                await writer.drain()

            finally:
                self.send.task_done()

    def _log(self):
        logger.debug("\tIoQueues: %s", self)
        for name, q in self.receive.items():
            logger.debug("\t\tqueue: %s, %s, %s", name, id(q), q.qsize())


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
        # see https://github.com/python/asyncio/issues/394
        buffer = bytearray()

        missing = n

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
    def connection_lost(self, exc):
        """Shutdown process"""
        super(ShutdownOnConnectionLost, self).connection_lost(exc)

        # XXX FIXME is not called
        logger.warning("Connection lost! Shutting down...")
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


async def send_outgoing_queue(queue, pipe=sys.stdout):
    """Write data from queue to stdout."""

    async with Outgoing(pipe=pipe, shutdown=True) as writer:
        while True:
            data = await queue.get()
            writer.write(data)
            await writer.drain()
            queue.task_done()


def split_data(data, size=1024):
    """A generator to yield splitted data."""

    data_view = memoryview(data)
    data_len = len(data_view)
    start = 0

    while start < data_len:
        end = min(start + size, data_len)

        yield data_view[start:end]

        start = end


class ChunkFlags(dict):

    _masks = {
        'eom': (1, 0, int, bool),
        'stop_iter': (1, 1, int, bool),
        'send_ack': (1, 2, int, bool),
        'recv_ack': (1, 3, int, bool),
        'compression': (3, 4, {
            False: 0,
            True: 1,
            'gzip': 1,
            'lzma': 2,
            'zlib': 3,
        }.get, {
            0: False,
            1: 'gzip',
            2: 'lzma',
            3: 'zlib'
        }.get),
    }

    def __init__(self, *, send_ack=False, recv_ack=False, eom=False, stop_iter=False, compression=False):
        self.__dict__ = self
        super(ChunkFlags, self).__init__()

        self.eom = eom
        self.stop_iter = stop_iter
        self.send_ack = send_ack
        self.recv_ack = recv_ack
        self.compression = compression is True and 'gzip' or compression

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


HEADER_FMT = '!16sQHI'
HEADER_SIZE = struct.calcsize(HEADER_FMT)


class Channel:

    chunk_size = 0x8000

    acknowledgements = weakref.WeakValueDictionary()
    """Global acknowledgment futures distinctive by uuid."""

    def __init__(self, name=None, *, io_queues=None):
        self.name = name
        self.io_queues = io_queues or IoQueues()
        self.io_outgoing = self.io_queues.send

        if self.name in self.io_queues.receive:
            self.io_incomming = self.io_queues.receive[self.name]

        else:
            queue = asyncio.Queue()
            self.io_incomming = queue
            self.io_queues.receive[self.name] = queue

        # self.io_incomming = self.io_queues[self.name]
        logger.info("Channel incomming: %s", id(self.io_incomming))

    def __repr__(self):
        return '<{0.name} {in_size} / {out_size}>'.format(
            self,
            in_size=self.io_incomming.qsize(),
            out_size=self.io_outgoing.qsize(),
        )

    async def receive(self):
        """Receive the next message in this channel."""

        msg = await self.io_incomming.get()
        try:
            return msg
        finally:
            self.io_incomming.task_done()

    def __await__(self):
        """Receive the next message in this channel."""

        msg = yield from self.io_incomming.get()

        try:
            return msg
        finally:
            self.io_incomming.task_done()

    async def __aiter__(self):
        return self

    async def __anext__(self):
        # TODO use iterator and stop_iter header to raise AsyncStopIteration
        data = await self

        return data

    @staticmethod
    def _encode_header(uid, channel_name=None, data=None, *, flags=None):
        """
        [header length = 30 bytes]
        [!16s]     [!Q: 8 bytes]                     [!H: 2 bytes]        [!I: 4 bytes]
        {data uuid}{flags: compression|eom|stop_iter}{channel_name length}{data length}{channel_name}{data}
        """
        assert isinstance(uid, uuid.UUID), "uid must be an UUID instance"

        if flags is None:
            flags = {}

        if channel_name:
            name = channel_name.encode()
            channel_name_length = len(name)
        else:
            channel_name_length = 0

        data_length = data and len(data) or 0
        chunk_flags = ChunkFlags(**flags)

        header = struct.pack(HEADER_FMT, uid.bytes, chunk_flags.encode(), channel_name_length, data_length)
        check = hashlib.md5(header).digest()

        return b''.join((header, check))

    @staticmethod
    def _decode_header(header):
        assert hashlib.md5(header[:-16]).digest() == header[-16:], "Header checksum mismatch!"
        uid_bytes, flags_encoded, channel_name_length, data_length = struct.unpack(HEADER_FMT, header[:-16])

        return uuid.UUID(bytes=uid_bytes), ChunkFlags.decode(flags_encoded), channel_name_length, data_length

    async def send(self, data, ack=False):
        compression = False
        uid = uuid.uuid1()
        name = self.name.encode()

        pickled_data = pickle.dumps(data)

        for part in split_data(pickled_data, self.chunk_size):
            header = self._encode_header(uid, self.name, part, flags={
                'eom': False, 'send_ack': False, 'compression': compression
            })

            await self.io_outgoing.put((header, name, part))

        header = self._encode_header(uid, self.name, None, flags={
            'eom': True, 'send_ack': ack, 'compression': False
        })
        logger.debug("\n\n--> send to %s: %s", self.name, uid)
        await self.io_outgoing.put((header, name))

        # if acknowledgement is asked for
        # we await this future and return its result
        # see _receive_reader for resolution of future
        if ack:
            ack_future = asyncio.Future()
            self.acknowledgements[uid] = ack_future

            return await ack_future

    @classmethod
    async def _send_ack(cls, io_queues, uid):
        # no channel_name, no data
        header = cls._encode_header(uid, None, None, flags={
            'eom': True, 'recv_ack': True
        })

        await io_queues.send.put(header)

    @classmethod
    async def communicate(cls, io_queues, reader, writer):
        fut_send_recv = asyncio.gather(
            cls._send_writer(io_queues, writer),
            cls._receive_reader(io_queues, reader)
        )

        await fut_send_recv

    @classmethod
    async def _send_writer(cls, io_queues, writer):
        # send outgoing queue to writer
        queue = io_queues.send

        try:
            logger.debug("sending queue to writer")
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
                logger.warning("Send queue was not empty when canceld!")

    @classmethod
    async def _receive_single_message(cls, io_queues, reader, buffer):
        # read header
        try:
            logger.debug("waiting for header...")

            try:
                raw_header = await reader.readexactly(HEADER_SIZE + 16)

            except asyncio.IncompleteReadError as ex:
                # if 0 bytes are read, we assume a Cancellation
                if len(ex.partial) == 0:
                    raise asyncio.CancelledError("While waiting for header, we received EOF!")

                raise

            uid, flags, channel_name_length, data_length = cls._decode_header(raw_header)
            logger.debug("\t<-- header: %s, sizes: %s/%s, %s", uid, channel_name_length, data_length, flags)

            if channel_name_length:
                channel_name = (await reader.readexactly(channel_name_length)).decode()
                logger.debug("\t<-- channel_name: %s", channel_name)

            if data_length:
                if uid not in buffer:
                    buffer[uid] = bytearray()

                buffer[uid].extend(await reader.readexactly(data_length))

            if flags.send_ack:
                # we have to acknowledge the reception
                await cls._send_ack(io_queues, uid)

            if flags.eom:
                # put message into channel queue
                if uid in buffer and channel_name_length:
                    msg = pickle.loads(buffer[uid])
                    await io_queues[channel_name].put(msg)
                    del buffer[uid]

                # acknowledge reception
                ack_future = cls.acknowledgements.get(uid)
                if ack_future and flags.recv_ack:
                    duration = time.time() - uuid1_time(uid)
                    ack_future.set_result((uid, duration))

        except asyncio.CancelledError:
            raise

        except:
            logger.error("Error while receiving:\n%s", traceback.format_exc())
            raise

    @classmethod
    async def _receive_reader(cls, io_queues, reader):
        # receive incomming data into queues

        buffer = {}

        try:
            logger.debug("receiving reader into queues")
            while True:
                await cls._receive_single_message(io_queues, reader, buffer)
                io_queues._log()

        except asyncio.CancelledError:
            if buffer:
                log.warn("Receive buffer was not empty when canceled!")



######################################################################################
PLUGINS_ENTRY_POINT_GROUP = 'dbltr.plugins'


class MetaPlugin(type):

    plugins = {}

    def __new__(mcs, name, bases, dct):
        cls = type.__new__(mcs, name, bases, dct)

        cls.scan_entry_points()

        # add this module as default
        cls.add(cls(__name__))

        return cls

    def scan_entry_points(cls):
        # scan for available plugins
        for entry_point in pkg_resources.iter_entry_points(group=PLUGINS_ENTRY_POINT_GROUP):
            plugin = cls.create_from_entry_point(entry_point)
            # we index entry_points and module names
            cls.add(plugin)
            # mcs.plugins[plugin.module_name] = mcs.plugins[plugin.name] = plugin

    def add(cls, plugin):
        cls.plugins[plugin.module_name] = cls.plugins[plugin.name] = plugin

    def __getitem__(cls, name):
        plugin_name, _, command_name = name.partition(':')
        plugin = cls.plugins[plugin_name]

        if not command_name:
            return plugin

        command = plugin.commands[command_name]
        return command

    def __contains__(cls, name):
        try:
            cls[name]
            return True

        except KeyError:
            return False

    def get(cls, name, default=None):
        try:
            return cls[name]

        except KeyError:
            return default


class Plugin(metaclass=MetaPlugin):

    """A plugin module introduced via entry point."""

    def __init__(self, module_name, name=None):
        self.module_name = module_name
        self.name = name or module_name

        self.commands = {}
        """A map of commands the plugin provides."""

    @reify
    def module(self):
        """On demand module loading."""

        return __import__(self.module_name, fromlist=[''])
        # return self.entry_point.load()

    @classmethod
    def create_from_entry_point(cls, entry_point):
        plugin = cls(entry_point.module_name, '#'.join((entry_point.dist.key, entry_point.name)))
        cls.add(plugin)

        return plugin

    @classmethod
    def create_from_code(cls, code, project_name, entry_point_name, module_name, version='0.0.0'):
        dist = pkg_resources.Distribution(project_name=project_name, version=version)
        dist.activate()
        pkg_environment.add(dist)
        entry_point = pkg_resources.EntryPoint(entry_point_name, module_name, dist=dist)

        module = create_module(module_name)
        plugin = cls.create_from_entry_point(entry_point)

        c = compile(code, "<{}>".format(module_name), "exec", optimize=2)
        exec(c, module.__dict__)

        return plugin

    def __repr__(self):
        return "<Plugin {}>".format(self.module_name)


def exclusive(fun):
    """Makes an async function call exclusive."""

    lock = asyncio.Lock()

    async def locked_fun(*args, **kwargs):
        logger.debug("\n\n---\n\nlocking %s: %s", lock, fun)
        async with lock:
            return await fun(*args, **kwargs)
        logger.debug("\n\n---\n\nreleasing %s: %s", lock, fun)

    return locked_fun


class CommandMeta(type):

    base = None
    commands = {}
    typed_commands = weakref.WeakSet()

    command_instances = weakref.WeakValueDictionary()
    """Holds references to all active Instances of Commands, to forward to their queue"""

    command_instance_names = weakref.WeakKeyDictionary()
    """Holds all instances mapping to their FQIN."""

    def __new__(mcs, name, bases, dct):
        """Register command at plugin vice versa"""

        module = dct['__module__']
        dct['plugin'] = Plugin[module]

        cls = type.__new__(mcs, name, bases, dct)
        if mcs.base is None:
            mcs.base = cls

        else:
            # only register classes except base class
            mcs._register_command(cls)

        return cls

    @classmethod
    def _register_command(mcs, cls):
        cls.plugin.commands[cls.__name__] = cls

        for plugin_name in set((cls.plugin.module_name, cls.plugin.name)):
            name = ':'.join((plugin_name, cls.__name__))
            mcs.commands[name] = cls

    def __getitem__(cls, value):
        @functools.singledispatch
        def _get(value):
            return None

        @_get.register(tuple)
        def _tuple(value):
            return cls.command_instances[value]

        @_get.register(str)
        def _str(value):
            return cls.command_instances[tuple(value.split('/', 1))]

        @_get.register(bytes)
        def _bytes(value):
            return cls.command_instances[tuple(value.split(b'/', 1))]

        @_get.register(memoryview)
        def _memoryview(value):
            return cls.command_instances[tuple(value.tobytes().split(b'/', 1))]

        return _get(value)

    @property
    def fqn(cls):
        return ':'.join((cls.plugin.module_name, cls.__name__))

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
        for _, _, func in mcs._lookup_command_classmethods('local_setup'):
            await func(*args, **kwargs)

    @classmethod
    async def remote_setup(mcs, *args, **kwargs):
        for _, _, func in mcs._lookup_command_classmethods('remote_setup'):
            await func(*args, **kwargs)

    def _create_reference(cls, uid, inst):
        fqin = (cls.fqn, uid)
        cls.command_instance_names[inst] = fqin
        cls.command_instances[fqin] = inst

    def __call__(cls, *args, command_uid=None, **kwargs):
        inst = super(CommandMeta, cls).__call__(*args, **kwargs)

        if command_uid is None:
            command_uid = uuid.uuid1()

        cls._create_reference(command_uid, inst)

        return inst


class Command(metaclass=CommandMeta):

    """Base command class, which has no other use than provide the common ancestor to all Commands."""

    def __init__(self, io_queues, **params):
        super(Command, self).__init__()

        self.io_queues = io_queues
        self.params = params

    def __getattr__(self, name):
        try:
            return super(Command, self).__getattr__(name)

        except AttributeError:
            try:
                return self.params[name]

            except KeyError:
                raise AttributeError(
                    "'{}' has neither an attribute nor a parameter '{}'".format(self, name)
                )

    @classmethod
    def create_command(cls, io_queues, command_name, command_uid=None, **params):
        """Create a new Command instance and assign a uid to it."""

        command_class = cls.commands.get(command_name)

        if inspect.isclass(command_class) and issubclass(command_class, Command):
            # do something and return result
            command = command_class(io_queues, command_uid=command_uid, **params)

            return command

        raise KeyError('The command `{}` does not exist!'.format(command_name))

    async def __await__(self):
        """Executes the command by delegating to execution command."""

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
    def uid(self):
        _, uid = self.fqin

        return uid

    @reify
    def fqin(self):
        """The fully qualified instance name."""

        return self.__class__.command_instance_names[self]

    @reify
    def channel_name(self):
        return '/'.join(map(str, self.fqin))

    @reify
    def command_name(self):
        return self.__class__.fqn

    @reify
    def channel(self):
        return Channel(self.channel_name, io_queues=self.io_queues)

    def __repr__(self):
        return self.channel_name

    def __del___(self):
        # cleanup io_queues
        if self.channel_name in self.io_queues.receive:
            del self.io_queues.receive[self.channel_name]



class RemoteResult(namedtuple('RemoteResult', ('fqin', 'result', 'exception', 'traceback'))):
    def __new__(cls, fqin, result=None, exception=None, traceback=None):
        tb = traceback.format_exc() if exception else traceback

        return super(RemoteResult, cls).__new__(cls, fqin, result, exception, tb)


class Execute(Command):

    """The executor of all commands.

    This class should not be invoked directly!
    """

    pending_commands = defaultdict(asyncio.Future)

    def __init__(self, io_queues, command):
        super(Execute, self).__init__(io_queues)

        self.command = command

    async def __await__(self):
        # forbid using Execute directly
        raise RuntimeError("Do not invoke `await Execute()` directly, instead use `await Command(**params)`)")

    @reify
    def channel_name(self):
        """Execution is always run on the class channel."""

        return self.__class__.fqn

    @classmethod
    async def local_setup(cls, io_queues):
        """Waits for RemoteResult messages and resolves waiting futures."""

        async def resolve_pending_commands():
            # listen to the global execute channel
            channel = Channel(cls.fqn, io_queues=io_queues)
            # logger.info("more channel incomming: %s", id(channel.io_incomming))

            try:
                async for fqin, result, exception, traceback in channel:
                    future = cls.pending_commands[fqin]

                    if not exception:
                        future.set_result(result)

                    else:
                        logger.error("Remote exception for %s:\n%s", fqin, traceback)
                        future.set_exception(exception)

            except asyncio.CancelledError:
                pass

            # teardown here
            for fqin, fut in cls.pending_commands.items():
                logger.warn("Teardown pending command: %s, %s", fqin, fut)
                fut.cancel()
                del cls.pending_commands[fqin]

        pending = asyncio.ensure_future(resolve_pending_commands())
        def log_pending_result(fut):
            try:
                logger.debug(fut.result)

            except:
                logger.error("Error:\n%s", traceback.format_exc())

        pending.add_done_callback(log_pending_result)

    @classmethod
    async def remote_setup(cls, io_queues):
        async def execute_commands():
            # our incomming command queue is global
            channel = Channel(cls.fqn, io_queues=io_queues)

            try:
                async for args, kwargs in channel:
                    command = cls.create_command(io_queues, *args, **kwargs)
                    execute = cls(io_queues, command)
                    asyncio.ensure_future(execute.remote())

            except asyncio.CancelledError:
                pass

        asyncio.ensure_future(execute_commands())

    def remote_future(self):
        """Create remote command and yield its future."""

        class _context:
            async def __aenter__(ctx):
                await self.channel.send((self.command.fqin, self.command.params), ack=True)
                future = self.pending_commands[self.command.channel_name]

                return future

            async def __aexit__(ctx, *args):
                del self.pending_commands[self.command.channel_name]

        return _context()

    async def local(self):
        # create remote command

        async with self.remote_future() as future:
            try:
                # execute local side of command
                result = await self.command.local(remote_future=future)
                future.result()
                return result

            except:
                logger.error("Error while executing command: %s\n%s", self.command, traceback.format_exc())

    async def remote(self):

        fqin = self.command.channel_name

        try:
            # execute remote side of command
            result = await self.command.remote()
            await self.channel.send(RemoteResult(fqin, result=result))

            return result

        except Exception as ex:
            logger.error("traceback:\n%s", traceback.format_exc())
            await self.channel.send(RemoteResult(fqin, exception=ex))

            raise


class Import(Command):

    def __init__(self, io_queues, **params):
        super(Import, self).__init__(io_queues, **params)

        logger.debug("\n\nImporting %s", self.plugin_name)

    @exclusive
    async def __await__(self):
        return await super(Import, self).__await__()

    async def local(self, remote_future):
        plugin = Plugin[self.plugin_name]

        code = inspect.getsource(plugin.module)
        module_name = plugin.module.__name__

        project_name, entry_point_name = plugin.name.split('#')

        plugin_data = {
            'code': code,
            'project_name': project_name,
            'entry_point_name': entry_point_name,
            'module_name': module_name,
        }

        await self.channel.send(plugin_data)

        result = await remote_future

        return result

    async def remote(self):
        plugin_data = await self.channel
        plugin = Plugin.create_from_code(**plugin_data)

        return {
            'commands': list(Command.commands.keys()),
            'plugins': list(Plugin.plugins.keys())
        }


async def run(*tasks):
    """Schedules all tasks and wait for running is done or canceled"""

    # create indicator for running messenger
    running = asyncio.Future()

    for task in tasks:
        fut = asyncio.ensure_future(task)

    # exit on sigterm or sigint
    for signame in ('SIGINT', 'SIGTERM', 'SIGHUP', 'SIGQUIT'):
        sig = getattr(signal, signame)

        def exit_with_signal(sig):
            logger.info("SIGNAL: %s", sig)
            try:
                running.set_result(sig)

            except asyncio.InvalidStateError:
                logger.warning("running already done!")

        asyncio.get_event_loop().add_signal_handler(sig, functools.partial(exit_with_signal, sig))

    # wait for running completed
    result = await running

    return result


def cancel_pending_tasks():
    loop = asyncio.get_event_loop()

    for task in asyncio.Task.all_tasks():
        if task.done() or task.cancelled():
            continue

        task.cancel()
        try:
            loop.run_until_complete(task)

        except asyncio.CancelledError:
            pass

        logger.debug("*** Task shutdown: %s", task)


async def log_tcp_10001():
    try:
        reader, writer = await asyncio.open_connection('localhost', 10001)

        while True:
            msg = await reader.readline()

            logger.info("TCP: %s", msg)

    except asyncio.CancelledError:
        logger.info("close tcp logger")
        writer.close()


async def communicate(io_queues):
    async with Incomming(pipe=sys.stdin) as reader:
        async with Outgoing(pipe=sys.stdout, shutdown=True) as writer:
            await Channel.communicate(io_queues, reader, writer)


def main(**kwargs):

    loop = create_loop(debug=True)

    io_queues = IoQueues()

    # setup all plugin commands
    loop.run_until_complete(Command.remote_setup(io_queues))
    try:
        loop.run_until_complete(
            run(
                communicate(io_queues)
                # send_outgoing_queue(queue_out, sys.stdout),
                # log_tcp_10001()
            )
        )
        cancel_pending_tasks()

    finally:
        loop.close()


def decode_options(b64):
    return json.loads(base64.b64decode(b64).decode())


def encode_options(**options):
    return base64.b64encode(json.dumps(options).encode()).decode()


if __name__ == '__main__':
    main()
