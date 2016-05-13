"""
Receiver for debellator.

http://stackoverflow.com/questions/23709916/iterating-over-asyncio-coroutine
http://sahandsaba.com/understanding-asyncio-node-js-python-3-4.html

http://sametmax.com/open-bar-sur-asyncio/


see https://github.com/python/asyncio/issues/314 for Exception on exit

"""

import sys
import asyncio


class DataStreamReader(asyncio.StreamReader):
    async def read_data(self):
        # wait for data
        if not self._buffer and not self._eof:
            await self._wait_for_data('read_data')

        data = bytes(self._buffer)
        self._buffer.clear()

        self._maybe_resume_transport()
        return data

    def close(self):
        self._transport.close()


class AioStdinPipe:

    def __init__(self, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self.reader = DataStreamReader()

    def get_reader(self):
        return asyncio.StreamReaderProtocol(self.reader)

    async def __aiter__(self):
        await self.loop.connect_read_pipe(self.get_reader, sys.stdin)
        return self

    async def __anext__(self):
        while True:
            val = await self.reader.read_data()
            if val == b'':
                continue
                # raise StopAsyncIteration
            return val


class Receiver(object):

    """
    Remote control.
    """
    def __init__(self, loop, done=None):
        self.loop = loop
        self.done = done

    async def recv(self):
        reader = DataStreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        await self.loop.connect_read_pipe(lambda: protocol, sys.stdin)

        try:
            while True:
                data = await reader.read_data()
                if data == b'':
                    continue

                sys.stdout.buffer.write(data)
                if data == b'terminate':
                    if self.done:
                        self.done.set_result(None)
                    return
        finally:
            # important for clean exit
            reader.close()

    async def receive(self):
        async for line in AioStdinPipe():
            sys.stdout.buffer.write(line)
            if line == b'terminate':
                if self.done:
                    self.done.set_result(None)
                self.loop.stop()
                return
                # break

        if self.done:
            await self.done


async def workaround():
    await asyncio.sleep(0)


def main(args):
    # create loop
    if sys.platform == "win32":
        loop = asyncio.ProactorEventLoop()  # for subprocess' pipes on Windows
        asyncio.set_event_loop(loop)
    else:
        loop = asyncio.get_event_loop()

    try:
        done = asyncio.Future(loop=loop)

        receiver = Receiver(loop)

        # returncode = loop.create_task(receiver.receive())

        # loop.run_until_complete(done)
        # done.set_result(None)
        loop.run_until_complete(receiver.recv())

    finally:
        loop.close()


if __name__ == '__main__':
    main(sys.argv)
