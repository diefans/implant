#!/usr/bin/env python3
"""
resources:
- https://pymotw.com/3/asyncio/subprocesses.html
- http://stackoverflow.com/questions/24435987/how-to-stream-stdout-stderr-from-a-child-process-using-asyncio-and-obtain-its-e
- http://stackoverflow.com/questions/375427/non-blocking-read-on-a-subprocess-pipe-in-python/20697159#20697159


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
import signal
import functools
import asyncio
from asyncio.subprocess import PIPE
import shlex
import base64
import uuid

from dbltr import receive


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
        print('process started {}'.format(transport.get_pid()))
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


async def run_client(loop, done, *command):
    factory = functools.partial(PingProtocol, loop, done)

    # asyncio.create_subprocess_exec
    proc = loop.subprocess_exec(
        factory,
        *command,
        stdout=PIPE,
        stderr=PIPE,
        stdin=PIPE
    )

    try:
        print('launching process')
        transport, protocol = await proc

        print("register process")
        processes.add(asyncio.subprocess.Process(transport, protocol, loop))

        print('waiting for process to complete', transport, protocol)
        await done

    finally:
        transport.close()


def get_python_source(obj):
    import inspect
    return inspect.getsource(obj)


def main():

    receive_source = get_python_source(receive).encode('utf-8')
    receive_source_b64 = base64.b64encode(receive_source).decode('utf-8')

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

        ssh_command = [
            "ssh", "localhost",
            "/home/olli/.pyenv/versions/debellator3/bin/python",
            "-u",
            "-c",
            "'"
            'import imp, base64; boot = imp.new_module("dbltr.boot");'
            'c = compile(base64.b64decode(b"{}"), "<string>", "exec");'
            'exec(c, boot.__dict__); boot.main(None);'
            "'".format(receive_source_b64)
        ]

        returncode = loop.create_task(
            run_client(
                loop,
                done,
                *ssh_command
            )
        )
        loop.run_until_complete(done)
    finally:
        loop.close()


if __name__ == '__main__':
    main()

    # asyncio.StreamReader
    # asyncio.StreamReaderProtocol
