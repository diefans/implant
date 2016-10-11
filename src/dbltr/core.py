"""
Remote process
"""
import inspect
import sys
import asyncio
import signal
import functools
import itertools
import os
import base64
import json
import uuid
import types
import gzip
import lzma
import contextlib
import logging
import weakref
import traceback
import pickle
from collections import defaultdict
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
        asyncio.set_event_loop(loop)

    else:
        loop_ = asyncio.get_event_loop()

    loop_.set_debug(debug)

    return loop_


# loop is global
loop = create_loop(debug=True)


class Incomming:

    """A context for an incomming pipe."""

    def __init__(self, *, pipe=sys.stdin):
        self.pipe = pipe
        self.transport = None

    async def __aenter__(self):
        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)

        self.transport, _ = await asyncio.get_event_loop().connect_read_pipe(
            lambda: protocol,
            # sys.stdin
            os.fdopen(self.pipe.fileno(), 'r')
        )
        return reader

    async def __aexit__(self, exc_type, value, tb):
        self.transport.close()


class Outgoing:

    """A context for an outgoing pipe."""

    def __init__(self, *, pipe=sys.stdout):
        self.pipe = pipe
        self.transport = None

    async def __aenter__(self):
        self.transport, protocol = await asyncio.get_event_loop().connect_write_pipe(
            asyncio.streams.FlowControlMixin,
            # sys.stdout
            os.fdopen(self.pipe.fileno(), 'wb')
        )
        writer = asyncio.streams.StreamWriter(self.transport, protocol, None, asyncio.get_event_loop())

        return writer

    async def __aexit__(self, exc_type, value, tb):
        self.transport.close()


async def send_outgoing_queue(queue, pipe=sys.stdout):
    """Write data from queue to stdout."""

    async with Outgoing(pipe=pipe) as writer:
        while True:
            data = await queue.get()
            writer.write(data)
            await writer.drain()
            queue.task_done()


async def distribute_incomming_chunks(channels, pipe=sys.stdin):
    """Distribute all chunks from stdin to channels."""

    async with Incomming(pipe=pipe) as reader:
        while True:
            line = await reader.readline()
            if line is b'':
                break

            await channels.distribute(line)


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

        return plugin

    @classmethod
    def create_from_code(cls, code, project_name, entry_point_name, module_name, version='0.0.0'):
        dist = pkg_resources.Distribution(project_name=project_name, version=version)
        dist.activate()
        pkg_environment.add(dist)
        entry_point = pkg_resources.EntryPoint(entry_point_name, module_name, dist=dist)

        module = create_module(module_name)
        c = compile(code, "<string>", "exec", optimize=2)
        exec(c, module.__dict__)

        plugin = cls.create_from_entry_point(entry_point)

        return plugin

    def __repr__(self):
        return "<Plugin {}>".format(self.module_name)


class CommandMeta(type):

    base = None
    commands = {}

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

        @_get.register(bytes)
        def _bytes(value):
            return cls.command_instances[tuple(value.split(b'/', 1))]

        @_get.register(memoryview)
        def _memoryview(value):
            return cls.command_instances[tuple(value.tobytes().split(b'/', 1))]

        return _get(value)

    @property
    def fqn(cls):
        return ':'.join((cls.plugin.module_name, cls.__name__)).encode()

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
            command_uid = uuid.uuid1().hex.encode()

        cls._create_reference(command_uid, inst)

        return inst


class Command(metaclass=CommandMeta):

    """Base command class, which has no other use than provide the common ancestor to all Commands."""

    channels = defaultdict(asyncio.Queue)

    def __init__(self):
        self.queue = asyncio.Queue()

    def local(self, *args, **kwargs):
        raise NotImplementedError(
            'You have to implement at least a `local` method'
            'for your Command to work: {}'.format(self.__class__)
        )

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
        return b'/'.join(self.fqin)

    @functools.lru_cache(maxsize=10, typed=True)
    def channel(self, channel_type, *, queue=None):
        return channel_type(self.channel_name, queue=queue or self.queue)

    def __repr__(self):
        return self.channel_name.decode()


class RemoteException(Exception):

    def __init__(self, *, fqin, traceback, type):
        super(RemoteException, self).__init__()

        self.fqin = fqin
        self.traceback = traceback
        self.type = type


class Execute(Command):

    """The executor of all commands."""

    pending_futures = defaultdict(asyncio.Future)

    def __init__(self, command_name, *args, **kwargs):
        super(Execute, self).__init__()

        self.command_name = command_name
        self.args = args
        self.kwargs = kwargs

    @property
    def params(self):
        params = {
            'command_name': self.command_name,
            'args': self.args,
            'kwargs': self.kwargs,
        }
        return params

    def create_command(self):
        """Create a new Command instance and assign a uid to it."""

        command_class = Command.commands.get(self.command_name)

        if inspect.isclass(command_class) and issubclass(command_class, Command):

            # do something and return result
            command = command_class(*self.args, command_uid=self.uid, **self.kwargs)

            return command

        raise KeyError('The command `{}` does not exist!'.format(self.command_name))

    async def local(self, remote):
        command = self.create_command()

        await self._create_remote_command(remote)

        # future_create = self.pending_futures[self.fqin]
        # await future_create

        # XXX TODO create a context and remove future when done
        future = self.pending_futures[command.channel_name]
        result = await command.local(queue_out=remote.queue_in, future=future)
        future.result()
        del self.pending_futures[command.channel_name]

        try:
            return result

        finally:
            del command

    async def _create_remote_command(self, remote):
        channel_out = JsonChannel(
            self.__class__.fqn,
            queue=remote.queue_in
        )

        # create remote command
        async with channel_out.message(self.uid) as send:
            await send(self.params)

        # wait for sync
        async for msg in self.channel(JsonChannel):
            logger.info("sync: %s", msg)
            break

    @classmethod
    async def local_setup(cls, remote):
        # distibute to local instances
        logger.info("create local instance distributor")

        channel_in = JsonChannel()

        async def resolve_pending_futures():
            try:
                async for uid, message in channel_in:
                    fqin = message['fqin'].encode()
                    future = cls.pending_futures[fqin]

                    if 'result' in message:
                        future.set_result(message['result'])

                    elif 'exception' in message:
                        ex = RemoteException(
                            fqin=fqin,
                            type=message['exception']['type'],
                            traceback=message['exception']['traceback'],
                        )
                        ex_unpickled = pickle.loads(base64.b64decode(message['exception']['pickle']))

                        future.set_exception(ex_unpickled)
            except asyncio.CancelledError:
                pass

            # teardown here
            for fut in cls.pending_futures:
                fut.cancel()

        async def distribute_commands():
            try:
                async for line in remote.stdout:
                    if line is b'':
                        logger.warning('Remote send EOF')
                        break

                    channel_name, _, _, _ = Chunk.view(line)

                    logger.debug("\t\t<-- incomming: %s", line)

                    # split command channel from other plugin channels
                    # we want to execute a command
                    if channel_name == cls.fqn:
                        await channel_in.forward(line)

                    else:
                        # forward arbitrary data to their instances
                        try:
                            await cls[channel_name].queue.put(line)

                        except KeyError:
                            from pdb import set_trace; set_trace()       # XXX BREAKPOINT
                            logger.error("Channel instance not found: %s", channel_name.tobytes())

            except asyncio.CancelledError:
                pass

            # teardown here

        asyncio.ensure_future(distribute_commands())
        asyncio.ensure_future(resolve_pending_futures())

    async def remote(self, queue_out):

        command = self.create_command()
        logger.info("remote executing %s for %s", command, self.params)

        result_message = {
            'fqin': command.channel_name.decode(),
        }

        # trigger sync for awaiting local method
        channel = self.channel(JsonChannel, queue=queue_out)
        async with channel.message() as send:
            await send(None)

        try:
            result = await command.remote(
                queue_out=queue_out,
            )

            result_message['result'] = result

        except Exception as ex:
            result_message['exception'] = {
                'traceback': traceback.format_exc(),
                'type': ex.__class__.__name__,
                'pickle': base64.b64encode(pickle.dumps(ex)).decode()
            }

        return result_message

    @classmethod
    async def remote_setup(cls, queue_out):
        channel_out = JsonChannel(cls.fqn, queue=queue_out)

        # our incomming command queue
        queue_in = asyncio.Queue()
        channel_in = JsonChannel(queue=queue_in)

        async def execute_commands():
            try:
                async for uid, message in channel_in:
                    logger.info("retrieved command: %s", message)

                    execute = cls(message['command_name'], *message['args'], command_uid=uid, **message['kwargs'])

                    result = await execute.remote(queue_out=queue_out)

                    logger.debug("result: %s", result)

                    async with channel_out.message(uid) as send:
                        await send(result, compress='gzip')
            except asyncio.CancelledError:
                pass

            # teardown here

        async def distribute_commands():
            try:
                async with Incomming(pipe=sys.stdin) as reader:
                    while True:
                        line = await reader.readline()
                        if line is b'':
                            break

                        channel_name, _, _, _ = Chunk.view(line)

                        logger.debug("\t\t--> incomming: %s", line)

                        # split command channel from other plugin channels
                        # we want to execute a command
                        if channel_name == cls.fqn:
                            await channel_in.forward(line)

                        else:
                            # forward arbitrary data to their instances
                            await cls[channel_name].queue.put(line)
            except asyncio.CancelledError:
                pass

            # teardown here

        # create a teardown background task to be called when this process finishes
        asyncio.ensure_future(execute_commands())
        asyncio.ensure_future(distribute_commands())


class Import(Command):
    def __init__(self, plugin_name):
        super(Import, self).__init__()

        self.plugin_name = plugin_name

    async def local(self, queue_out, future):
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

        channel_out = self.channel(JsonChannel, queue=queue_out)
        async with channel_out.message() as send:
            await send(plugin_data)

        result = await cmd(code, project_name, entry_point_name, module_name)

        return result

    async def remote(self, queue_out):
        channel_in = self.channel(JsonChannel)

        async for uid, module_data in channel_in: break

        plugin = Plugin.create_from_code(code, project_name, entry_point_name, module_name)
        Plugin.add(plugin)

        # collect commands
        logger.debug("--> %s, commands: %s", id(Commander), Commander.commands)
        commands = list(Commander.commands.keys())

        return {
            'commands': commands,
            'plugins': list(Plugin.plugins.keys())
        }


class Echo(Command):
    def __init__(self, *args, **kwargs):
        super(Echo, self).__init__()

        self.args = args
        self.kwargs = kwargs

    async def local(self, queue_out, future):
        logger.info("local running %s", self)
        channel_in = self.channel(JsonChannel)
        channel_out = self.channel(JsonChannel, queue=queue_out)

        incomming = []

        # custom protocol
        # first receive
        async for i, (uid, msg) in aenumerate(channel_in):
            incomming.append(msg)
            if i == 9:
                break

        # second send
        for i in range(10):
            async with channel_out.message() as send:
                await send({'i': i})

        result = await future
        logger.info("local future finished: %s", result)
        return [result, incomming]

    async def remote(self, queue_out):
        logger.info("remote running %s", self)

        channel_in = self.channel(JsonChannel)
        channel_out = self.channel(JsonChannel, queue=queue_out)

        data = []

        # first send
        for i in range(10):
            async with channel_out.message() as send:
                await send({'i': str(i ** 2)})

        # second receive
        async for uid, msg in channel_in:
            logger.info("\t\t--> data incomming: %s", msg)
            data.append(msg)

            if msg['i'] == 9:
                break

        # raise Exception("foo")
        return {
            'args': self.args,
            'kwargs': self.kwargs,
            'data': data
        }


DEFAULT_CHANNEL_NAME = b''


class Chunk:

    """
    Abstracts the format of the wire data.

    We split messages into chunks to allow parallel communication of messages.
    """

    separator = b'|'

    compressor = {
        True: gzip,
        'gzip': gzip,
        'lzma': lzma
    }

    def __init__(self, data=None, *, channel=None, uid=None):
        self.data = data
        self.channel = channel
        self.uid = uid

    @classmethod
    @functools.lru_cache(typed=True)
    def view(cls, raw):
        raw_view = memoryview(raw)

        # split raw data at separators
        sep_ord = ord(cls.separator)

        def _gen_separator_index():
            count = 0
            for i, byte in enumerate(raw_view):
                if byte == sep_ord:
                    yield i
                    count += 1

            assert count == 3, 'A Chunk must be composed by three separators, '\
                'e.g. `channel|uid|payload`!'
        channel_end, uid_end, compressor_end = _gen_separator_index()

        raw_len = len(raw_view)
        # 10 == \n: skip newline
        raw_end = raw_len - 1 if raw_view[-1] == 10 else raw_len

        channel_view = raw_view[0:channel_end]
        uid_view = raw_view[channel_end + 1:uid_end]
        compressor_view = raw_view[uid_end + 1:compressor_end]
        data_view = raw_view[compressor_end + 1:raw_end]

        return channel_view, uid_view, compressor_view, data_view

    @classmethod
    def decode(cls, raw):
        """Decodes a bytes string with or without ending \n into a Chunk."""

        channel_view, uid_view, compressor_view, data_view = cls.view(raw)

        data = base64.b64decode(data_view)
        if compressor_view.tobytes().decode() in cls.compressor:
            data = cls.compressor[compressor_view.tobytes().decode()].decompress(data)

        return cls(data, channel=channel_view.tobytes(), uid=uid_view.tobytes())

    def encode(self, eol=True, *, compress=False):
        """Encode the chunk into a bytes string."""

        def _gen_parts():
            yield self.channel or DEFAULT_CHANNEL_NAME
            yield self.separator
            yield self.uid or b''
            yield self.separator
            yield compress and compress.encode() or b''
            yield self.separator

            data = self.data or b''
            if compress in self.compressor:
                data = self.compressor[compress].compress(data)
            yield base64.b64encode(data)

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

    @property
    def is_eom(self):
        return self.data == b''


class ChunkChannel:

    """Iterator over a queues own chunks."""

    max_size = 10240

    def __init__(self, name=DEFAULT_CHANNEL_NAME, *, queue=None):
        self.name = name
        self.queue = queue or asyncio.Queue()

        self.futures = {}
        """Register a message uid to be a resolvable future.
        If that uid is received the futures result will be set.

        For this to work a task must iterate the channel.
        """

    async def __aiter__(self):
        return self

    async def __anext__(self):
        uid, data = await self.data_received()

        # trigger waiting futures
        if uid in self.futures:
            self.futures[uid].set_result(data)

        return uid, data

    @contextlib.contextmanager
    def waiting_for(self, uid):
        """Create a context with a future to wait for."""
        # TODO XXX does this make only sense for complete (json) messages?

        self.futures[uid] = future = asyncio.Future()

        yield future

        del self.futures[uid]

    async def data_received(self):
        """:returns: a tuple (uid, chunk) from the queue of this channel."""

        line = await self.queue.get()

        try:
            chunk = Chunk.decode(line)
            # logger.debug('Chunk received at %s: %s', self, chunk)

            return chunk.uid, chunk

        finally:
            self.queue.task_done()

    async def forward(self, line):
        await self.queue.put(line)

    async def inject_chunk(self, chunk, compress=False):
        """Allows forwarding of a chunk as is into the queue."""

        await self.queue.put(chunk.encode(compress=compress))

    async def send(self, data, *, uid=None, compress=False):
        """Create a chunk from data and uid and send it to the queue of this channel."""

        if isinstance(data, Chunk):
            data = data.data

        for part in split_data(data, self.max_size):
            chunk = Chunk(part, channel=self.name, uid=uid)

            await self.inject_chunk(chunk, compress=compress)

    async def send_eom(self, *, uid=None):
        """Send EOM (end of message) to the queue."""

        chunk = Chunk(None, channel=self.name, uid=uid)

        await self.queue.put(chunk.encode(compress=False))

    def message(self, uid=None):

        if uid is None:
            uid = uuid.uuid1().hex.encode()

        class _context:
            def __init__(self, channel):
                self.channel = channel

            async def __aenter__(self):
                return functools.partial(self.channel.send, uid=uid)

            async def __aexit__(self, exc_type, exc, tb):
                # send EOM
                await self.channel.send_eom(uid=uid)

        return _context(self)


def split_data(data, size=1024):
    """A generator to yield splitted data."""

    data_view = memoryview(data)
    data_len = len(data_view)
    start = 0

    while start < data_len:
        end = min(start + size, data_len)

        yield data_view[start:end]

        start = end


class EncodedBufferChannel(ChunkChannel):

    """Buffers chunks, combines them and tries to decode them."""

    def __init__(self, *args, **kwargs):
        super(EncodedBufferChannel, self).__init__(*args, **kwargs)

        # holds message parts until decoding is complete
        self.buffer = {}

    async def data_received(self):
        """Wait until a message is completed by sending an empty chunk."""

        while True:
            # wait for the next data
            uid, chunk = await super(EncodedBufferChannel, self).data_received()

            # create buffer if uid is new
            if uid not in self.buffer:
                self.buffer[uid] = bytearray()

            # message completed -> return decoded chunks
            if chunk.is_eom:
                return uid, self._pop_from_buffer(uid)

            # append data to buffer
            self.buffer[uid].extend(chunk.data)

    def _pop_from_buffer(self, uid):
        try:
            data = self.decode(self.buffer[uid].decode())
            return data

        finally:
            del self.buffer[uid]

    def decode(self, data):
        return data

    def encode(self, data):
        return data

    async def send(self, data, *, uid=None, compress=False):

        if isinstance(data, Chunk):
            data = data.data

        encoded_data = self.encode(data).encode()

        await super(EncodedBufferChannel, self).send(
            encoded_data, uid=uid, compress=compress
        )


class JsonChannel(EncodedBufferChannel):

    def decode(self, data):
        data = json.loads(data)
        return data

    def encode(self, data):
        data = json.dumps(data)
        return data


class Channels:

    """Distributes arbitrary chunks to channels.

    The empty channel is used to send Commands."""

    def __init__(self):
        self._channels = {}
        self.add_channel(JsonChannel())

        self.default = self[DEFAULT_CHANNEL_NAME]

    async def distribute(self, line):
        channel, *_ = Chunk.view(line)

        if channel not in self._channels:
            logger.error('Channel `%s` not found for Chunk', channel)

        await self._channels[channel].forward(line)
        # logger.info("Chunk %s distributed to Channel `%s`", chunk, channel)

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
            # logger.info('Removing channel `%s`', channel_name)
            del self._channels[channel_name]

        except KeyError:
            raise KeyError("Channel `{}` not found!".format(channel_name))

    def add_channel(self, channel):
        self[channel.name] = channel


async def import_plugin(cmd, plugin_name):
    plugin = Plugin[plugin_name]

    code = inspect.getsource(plugin.module)
    module_name = plugin.module.__name__

    project_name, entry_point_name = plugin.name.split('#')

    result = await cmd(code, project_name, entry_point_name, module_name)

    return result


async def import_plugin(code, project_name, entry_point_name, module_name):
    plugin = Plugin.create_from_code(code, project_name, entry_point_name, module_name)
    Plugin.add(plugin)

    # collect commands
    logger.debug("--> %s, commands: %s", id(Commander), Commander.commands)
    commands = list(Commander.commands.keys())

    return {
        'commands': commands,
        'plugins': list(Plugin.plugins.keys())
    }


async def run(*tasks):
    """Schedules all tasks and wait for running is done or canceled"""

    # create indicator for running messenger
    running = asyncio.Future()

    shutdown_tasks = []

    for task in tasks:
        fut = asyncio.ensure_future(task)

        def _shutdown_on_error(future):
            try:
                # retrieve result or exception
                future.result()

            except Exception as ex:
                # delegate tsk error to running future to signal exit
                # logger.error(traceback.format_exc())
                running.set_exception(ex)

                # raise

        fut.add_done_callback(_shutdown_on_error)
        shutdown_tasks.append(fut)

    # exit on sigterm or sigint
    for signame in ('SIGINT', 'SIGTERM'):
        sig = getattr(signal, signame)
        asyncio.get_event_loop().add_signal_handler(sig, functools.partial(running.set_result, sig))

    # wait for running completed
    result = await running
    logger.info("shutdown".join([' - '] * 10))

    # cleanup
    for task in itertools.chain(shutdown_tasks):
        if task.done() or task.cancelled():
            continue

        log("cancel", task=task)
        task.cancel()
        await asyncio.wait_for(task, None)


    return result


def cancel_pending_tasks():
    logger.info("Shutdown pending tasks")
    loop = asyncio.get_event_loop()

    for task in asyncio.Task.all_tasks():
        if task.done() or task.cancelled():
            continue

        task.cancel()
        loop.run_until_complete(task)

        logger.info("*** Task shutdown: %s", task)


def main(**kwargs):

    # TODO channels must somehow connected with commands
    channels = Channels()

    queue_out = asyncio.Queue()
    channel_out = JsonChannel(channels.default.name, queue=queue_out)

    # setup all plugin commands
    loop.run_until_complete(Command.remote_setup(queue_out))
    try:
        loop.run_until_complete(
            run(
                send_outgoing_queue(queue_out, sys.stdout),
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
