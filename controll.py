#!/usr/bin/env python3

import sys
import functools
import asyncio
from asyncio.subprocess import PIPE
import shlex


class PingProtocol(asyncio.SubprocessProtocol):
    FD_NAMES = ['stdin', 'stdout', 'stderr']

    def __init__(self, done):
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


async def run_client(loop, command):
    cmd_done = asyncio.Future(loop=loop)
    factory = functools.partial(PingProtocol, cmd_done)

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
        await cmd_done

    finally:
        transport.close()


def main():

    if sys.platform == "win32":
        loop = asyncio.ProactorEventLoop()  # for subprocess' pipes on Windows
        asyncio.set_event_loop(loop)
    else:
        loop = asyncio.get_event_loop()

    try:
        returncode = loop.run_until_complete(
            run_client(
                loop,
                """ssh localhost 'python -u /home/olli/code/da/debellator/receive.py'"""
            )
        )
    finally:
        loop.close()


if __name__ == '__main__':
    main()
