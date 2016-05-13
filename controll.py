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
import functools
import asyncio
from asyncio.subprocess import PIPE
import shlex


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
        self.stdin.write(b'foo')

    def pipe_data_received(self, fd, data):
        print('read {} bytes from {}'.format(len(data),
                                             self.FD_NAMES[fd]))
        if fd == 1:
            print("<", data)

            self.buffer.extend(data)

            if data == b'foo':
                self.stdin.write(b'bar')

            elif data == b'bar':
                self.stdin.write(b'terminate')

            elif data == b'terminate':
                self.stdin.write(b'exit')
        else:
            print(fd, data)

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


async def run_client(loop, done, command):
    factory = functools.partial(PingProtocol, loop, done)

    proc = loop.subprocess_exec(
        factory,
        *shlex.split(command),
        stdout=PIPE,
        stdin=PIPE
    )

    try:
        print('launching process')
        transport, protocol = await proc
        print('waiting for process to complete')
        await done

    finally:
        transport.close()


def get_python_source(obj):
    import inspect
    return inspect.getsource(obj)


def main():
    import receive

    receive_source = get_python_source(receive).replace('"', '\\"').replace("'", "\\'")

    if sys.platform == "win32":
        loop = asyncio.ProactorEventLoop()  # for subprocess' pipes on Windows
        asyncio.set_event_loop(loop)
    else:
        loop = asyncio.get_event_loop()

    try:
        done = asyncio.Future(loop=loop)

        command = """ssh localhost '/home/olli/.pyenv/versions/debellator3/bin/python -u -c "{}"'""".format(receive_source)
        print(command)

        returncode = loop.create_task(
            run_client(
                loop,
                done,
                """ssh localhost '/home/olli/.pyenv/versions/debellator3/bin/python -u /home/olli/code/da/debellator3/receive.py'"""
                # """ssh localhost 'python -c "{}"'""".format(receive_source)
                # command
            )
        )
        loop.run_until_complete(done)
    finally:
        loop.close()


if __name__ == '__main__':
    main()

    # asyncio.StreamReader
    # asyncio.StreamReaderProtocol
