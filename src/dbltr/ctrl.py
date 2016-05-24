#!/usr/bin/env python3
"""
resources:
- https://pymotw.com/3/asyncio/subprocesses.html
- http://stackoverflow.com/questions/24435987/how-to-stream-stdout-stderr-from-a-child-process-using-asyncio-and-obtain-its-e
- http://stackoverflow.com/questions/375427/non-blocking-read-on-a-subprocess-pipe-in-python/20697159#20697159
- https://github.com/python/asyncio/blob/master/examples/child_process.py


How it should work


class Project:
    pass


class Task:
    pass


class Slave:
    pass



async for task in project.tasks:

    await result = slave.do(task)

    await task.complete(result)



"""

import sys
import types
import signal
import functools
import asyncio
import base64
import uuid
from subprocess import Popen, PIPE

from dbltr import receive


def log(msg):
    print(msg, file=sys.stderr, flush=True)


class PingProtocol(asyncio.SubprocessProtocol):
    FD_NAMES = ['stdin', 'stdout', 'stderr']

    def __init__(self, loop, done):
        self.loop = loop
        self.done = done
        self.buffer = bytearray()
        self.transport = None
        self.stdin = None
        self.stdout = None
        self.stderr = None
        super().__init__()

    def connection_made(self, transport):
        log('process started {}'.format(transport.get_pid()))
        self.transport = transport

        self.stdin = transport.get_pipe_transport(0)
        self.stdout = transport.get_pipe_transport(1)
        self.stderr = transport.get_pipe_transport(2)

        # initial start
        self.send(b'foo')
        # self.stdin.write(self._create_message(b'foo'))
        # self.stdin.write(b'foo\n')

    def pipe_connection_lost(self, fd, exc):
        print("connection lost", fd, exc)

    def send(self, data):
        uid = bytes(uuid.uuid1().hex, 'ascii')
        data = list(receive.split_size(data, 2))
        length = len(data)

        for i, chunk in enumerate(data):
            if i + 1 < length:
                self.stdin.write(b':'.join((uid, base64.b64encode(chunk))) + b'\n')

            else:
                self.stdin.write(b':'.join((uid, base64.b64encode(chunk), b'\n')))

    def pipe_data_received(self, fd, data):
        print('read {} bytes from {}'.format(len(data),
                                             self.FD_NAMES[fd]))
        print("<", fd, data.decode())
        if fd == 1:

            self.buffer.extend(data)

            if data == b'foo':
                self.send(b'bar')

            elif data == b'bar':
                self.send(b'terminate')

            elif data == b'terminate':
                self.send(b'exit')

    def process_exited(self):
        print('process exited')
        return_code = self.transport.get_returncode()
        print('return code {}'.format(return_code))
        if not return_code:
            cmd_output = bytes(self.buffer).decode()
            # results = self._parse_results(cmd_output)
        else:
            results = []
        self.done.set_result((return_code, 'foo'))
        # self.loop.stop()


processes = set()


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
        log("connection made")
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
        log("<<< {} {}".format(fd, data.decode()))

        super(SubprocessMessageStreamProtocol, self).pipe_data_received(fd, data)

    def send(self, data):
        log(">>> Sending: {}".format(data))
        uid = bytes(uuid.uuid1().hex, 'ascii')
        data = list(receive.split_size(data, 2))
        length = len(data)

        for i, chunk in enumerate(data):
            if i + 1 < length:
                self.stdin.write(b':'.join((uid, base64.b64encode(chunk))) + b'\n')

            else:
                self.stdin.write(b':'.join((uid, base64.b64encode(chunk), b'\n')))



async def create_subprocess_exec(program, *args, stdin=None, stdout=None,
                                 stderr=None, loop=None,
                                 limit=asyncio.streams._DEFAULT_LIMIT, **kwds):
    if loop is None:
        loop = asyncio.get_event_loop()
    protocol = SubprocessMessageStreamProtocol(limit=limit,
                                               loop=loop)

    transport, protocol = await loop.subprocess_exec(
        lambda: protocol,
        program, *args,
        stdin=stdin, stdout=stdout,
        stderr=stderr, **kwds
    )
    return asyncio.subprocess.Process(transport, protocol, loop)


class Remote(asyncio.subprocess.Process):

    """
    Embodies a remote python process.
    """

    @classmethod
    async def launch(cls, host, python_bin=None, code=None, loop=None, **kwargs):
        """Create a remote process."""

        if loop is None:
            loop = asyncio.get_event_loop()

        protocol = SubprocessMessageStreamProtocol(limit=asyncio.streams._DEFAULT_LIMIT, loop=loop)

        # FIXME remove testing python bin
        python_bin = python_bin or "/home/olli/.pyenv/versions/debellator3/bin/python"

        if code is None:
            # our default receiver
            code = receive

        if isinstance(code, types.ModuleType):
            code = get_python_source(receive).encode()

        command = (
            "'"
            'import imp, base64; boot = imp.new_module("dbltr.boot");'
            'c = compile(base64.b64decode(b"{code}"), "<string>", "exec");'
            'exec(c, boot.__dict__); boot.main(None);'
            "'"
        ).format(code=base64.b64encode(code).decode())

        # asyncio.create_subprocess_exec
        print('launching process')

        transport, protocol = await loop.subprocess_exec(
            lambda: protocol,
            'ssh', host, python_bin, '-u', '-c', command,
            stdin=PIPE, stdout=PIPE, stderr=PIPE,
            **kwargs
        )

        return cls(transport, protocol, loop)

    async def _launch_process(self):
        command = (
            "'"
            'import imp, base64; boot = imp.new_module("dbltr.boot");'
            'c = compile(base64.b64decode(b"{}"), "<string>", "exec");'
            'exec(c, boot.__dict__); boot.main(None);'
            "'"
        ).format(base64.b64encode(self._code).decode())

        # asyncio.create_subprocess_exec
        print('launching process')

        process = await create_subprocess_exec(
            'ssh', self._host, self._python_bin, '-u', '-c', command,
            stdin=PIPE,
            stdout=PIPE,
            stderr=PIPE
        )

        return process

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, value, traceback):
        self.terminate()
        print('terminate process')

    async def work(self, queue):
        """Process the queue."""

        while True:
            task = await queue.get()

            # TODO prepare dependencies

            result = await task(self)

            # TODO evaluate result

            queue.task_done()


async def ping(remote):
    async with remote as process:
        msg = receive.Message(b'foo')

        process._protocol.send(msg)
        # stream._stream_writer.write(msg)

        async for msg in process.stdout:
            if msg is None:
                break


class Debellator:
    remotes = {
        ('localhost', 'olli', receive): asyncio.Queue()
    }

    tasks = [
        ping
    ]

    def __init__(self, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._queue = asyncio.Queue()

    async def be(self):

        for access, queue in self.remotes.items():

            # enqueue tasks
            for task in self.tasks:
                await queue.put(task)

            # create remote
            host, user, code = access
            remote = await Remote.launch(host, code=code, loop=self._loop)

            asyncio.Task(remote.work(queue))

            # queue remote queue
            await self.expand(queue.join())

        # wait for master queue to finish
        print("wait for master queue", self._queue.qsize())
        await self._queue.join()
        print("finished master queue")

    async def expand(self, queue):
        async def _wait_for_queue():
            await queue.join()

        await self._queue.put(_wait_for_queue())


def main():

    if sys.platform == "win32":
        loop = asyncio.ProactorEventLoop()  # for subprocess' pipes on Windows
        asyncio.set_event_loop(loop)
    else:
        loop = asyncio.get_event_loop()


    done = asyncio.Future(loop=loop)

    def ask_exit(signame):
        print("got signal %s: exit" % signame)
        for proc in processes:
            proc.send_signal(signal.SIGHUP)
            proc.terminate()
            print("Killing: ", proc)
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

    # asyncio.StreamReader
    # asyncio.StreamReaderProtocol
