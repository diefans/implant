#!/usr/bin/env python3
"""
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

import sys
import shlex
import types
import signal
import functools
import asyncio
import base64
import uuid
from subprocess import Popen, PIPE
from collections import namedtuple

from dbltr import receive, msgr

log = receive.log


def get_python_source(obj):
    import inspect
    return inspect.getsource(obj)


class SubprocessMessageStreamProtocol(asyncio.subprocess.SubprocessStreamProtocol):
    def __init__(self, limit=None, loop=None):
        super(SubprocessMessageStreamProtocol, self).__init__(
            limit or asyncio.streams._DEFAULT_LIMIT,
            loop or asyncio.get_event_loop()
        )

    def connection_made(self, transport):
        log('connection made')
        self._transport = transport

        stdout_transport = transport.get_pipe_transport(1)
        if stdout_transport is not None:
            self.stdout = receive.MessageStreamReader(limit=self._limit,
                                                      loop=self._loop)
            self.stdout.set_transport(stdout_transport)

        stderr_transport = transport.get_pipe_transport(2)
        if stderr_transport is not None:
            self.stderr = receive.MessageStreamReader(limit=self._limit,
                                                      loop=self._loop)
            self.stderr.set_transport(stderr_transport)

        stdin_transport = transport.get_pipe_transport(0)
        if stdin_transport is not None:
            self.stdin = asyncio.StreamWriter(stdin_transport,
                                              protocol=self,
                                              reader=None,
                                              loop=self._loop)

    def pipe_data_received(self, fd, data):
        log('<<< {} {}'.format(fd, data.decode()))

        super(SubprocessMessageStreamProtocol, self).pipe_data_received(fd, data)

    def send(self, data, channel=None):
        channel = channel or b''
        uid = bytes(uuid.uuid1().hex, 'ascii')
        log('>>> Sending: {}'.format(b':'.join((channel, uid, data)).decode()))

        data = list(receive.split_size(data, 2))
        length = len(data)

        for i, chunk in enumerate(data):
            if i + 1 < length:
                self.stdin.write(b':'.join((channel, uid, base64.b64encode(chunk))) + b'\n')

            else:
                self.stdin.write(b':'.join((channel, uid, base64.b64encode(chunk), b'\n')))


class Remote(asyncio.subprocess.Process):

    """
    Embodies a remote python process.
    """

    bootstrap = (
        'import imp, base64; boot = imp.new_module("dbltr.boot");'
        'c = compile(base64.b64decode(b"{code}"), "<string>", "exec");'
        'exec(c, boot.__dict__); boot.main(None);'
    )

    @classmethod
    async def launch(cls,
                     host=None, user=None, sudo=None,
                     python_bin='/usr/bin/python3', code=None,
                     loop=None, **kwargs):
        """Create a remote process."""

        if loop is None:
            loop = asyncio.get_event_loop()

        protocol = SubprocessMessageStreamProtocol(limit=asyncio.streams._DEFAULT_LIMIT, loop=loop)

        # FIXME remove testing python bin
        # python_bin = python_bin or "/home/olli/.pyenv/versions/debellator3/bin/python"

        if code is None:
            # our default receiver
            code = msgr

        if isinstance(code, types.ModuleType):
            code = get_python_source(code).encode()

        def _command_args():
            """generates the command args."""

            # ssh
            if host is not None:
                log("user ssh", host=host, user=user)
                yield 'ssh'
                # optionally with user
                if user is not None:
                    yield '-l'
                    yield user
                yield host

            # sudo
            if sudo is not None:
                log("user sudo", sudo=sudo)
                yield 'sudo'
                # optionally with user
                if sudo is not True:
                    yield '-u'
                    yield sudo

            # python exec
            log('user python', python=python_bin)
            yield from shlex.split(python_bin)
            yield '-u'
            yield '-c'

            if host is not None:
                bootstrap = ''.join(("'", cls.bootstrap, "'"))
            else:
                bootstrap = cls.bootstrap

            yield bootstrap.format(code=base64.b64encode(code).decode())

        transport, _ = await loop.subprocess_exec(
            lambda: protocol,
            *list(_command_args()),
            stdin=PIPE, stdout=PIPE, stderr=PIPE,
            **kwargs
        )

        process = cls(transport, protocol, loop)

        log('launched process', pid=process.pid)

        return process

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, value, traceback):
        print('terminate process')

        try:
            self.terminate()

        except PermissionError:
            # seems we are not able to terminate
            log('Unable to terminate process', pid=self.pid)

    async def work(self, queue):
        """Process the queue."""

        while True:
            task = await queue.get()

            # TODO prepare dependencies

            result = await task(self)

            # TODO evaluate result

            queue.task_done()
            print('done', result, queue, queue.qsize())


async def ping(remote):
    async with remote as process:
        msg = receive.Message(b'foo')

        process._protocol.send(msg)
        # stream._stream_writer.write(msg)

        async for msg in process.stdout:
            if msg is None:
                break

            if msg == b'foo':
                return msg


class Target(namedtuple('Target', ('host', 'user', 'sudo'))):

    """A unique representation of a Remote."""

    def __new__(cls, host=None, user=None, sudo=None):
        return super(Target, cls).__new__(cls, host, user, sudo)


class Strategy:

    """A `Strategy` is the rolling plan to fulfill debellation of remote targets."""

    def __init__(self, parallel=1, loop=None):
        self._loop = loop or asyncio.get_event_loop()

        self.parallel = parallel

        self._scheduled_queue = asyncio.Queue(maxsize=parallel)
        """A queue for all remotes."""

    async def add_target(self, target):
        await self._scheduled_queue.put(target)

    async def __aiter__(self):
        return self

    async def __anext__(self):
        msg = await self._stream_reader.readline()

        if msg is None:
            # eof
            raise StopAsyncIteration

        return msg




class Debellator:
    remotes = {
        ('localhost', None, '/home/olli/.pyenv/versions/debellator3/bin/python', msgr): asyncio.Queue(),
        # ('ms', 'root', None, receive): asyncio.Queue()
    }

    tasks = [
        ping
    ]

    def __init__(self, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._queue = asyncio.Queue()

    async def be(self):
        """Schedules a strategy for each target remote."""

        for access, queue in self.remotes.items():

            # enqueue tasks
            for task in self.tasks:
                await queue.put(task)

            # create remote
            host, user, python_bin, code = access
            remote = await Remote.launch(host, user=user, code=code, python_bin=python_bin, loop=self._loop)

            asyncio.Task(remote.work(queue))

            # queue remote queue
            await self.expand(await queue.join())

        # wait for master queue to finish
        print('wait for master queue', self._queue.qsize())
        await self._queue.join()
        print('finished master queue')

    async def expand(self, queue):
        async def _wait_for_queue():
            await queue.join()
            print('queue finished')

        await self._queue.put(queue)


def main():

    if sys.platform == 'win32':
        loop = asyncio.ProactorEventLoop()  # for subprocess' pipes on Windows
        asyncio.set_event_loop(loop)
    else:
        loop = asyncio.get_event_loop()

    loop.set_debug(True)

    done = asyncio.Future(loop=loop)

    def ask_exit(signame):
        print('got signal %s: exit' % signame)
        loop.stop()
        done.cancelled()

    # register shutdowen
    for signame in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(
            getattr(signal, signame),
            functools.partial(ask_exit, signame)
        )

    try:
        debellator = Debellator(loop)
        loop.run_until_complete(debellator.be())
    finally:
        loop.close()


if __name__ == '__main__':
    main()
