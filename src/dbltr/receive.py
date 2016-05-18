"""
Receiver for debellator.

http://stackoverflow.com/questions/23709916/iterating-over-asyncio-coroutine
http://sahandsaba.com/understanding-asyncio-node-js-python-3-4.html

http://sametmax.com/open-bar-sur-asyncio/


see https://github.com/python/asyncio/issues/314 for Exception on exit

"""

import sys
import uuid
import json
import functools
import asyncio
import signal
from collections import defaultdict


def split_size(s, size=1024):
    """Split a string into pieces."""

    if size < 1:
        raise ValueError('size must be greater than 0', size)

    length = len(s)
    remains = length % size

    for i in (i * size for i in range(length // size)):
        yield s[i: i + size]

    if length < size:
        yield s

    elif remains:
        yield s[i + size: i + size + remains]


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


class Message:

    """
    A Message is a base64 encoded payload.
    """

    limit = 1024

    def __init__(self, payload, uuid=None):
        self.payload = payload
        self.uuid = uuid or uuid.uuid1()

    @property
    def jsonified(self):
        return json.dumps(self.payload)

    def encode(self):
        """Create a serialized version of that message.

        If the payload is greater than self.limit it is split at that boundary.
        """
        b64_jsonified = base64.b64encode(self.jsonified)

        # for split_msg in split_size(b64_jsonified, self.limit):






class MessageBuffer:

    """
    Buffers incomming data until complete and yields a Message
    """

    def __init__(self, loop):
        self._loop = loop
        self._waiter = None
        self._channels = defaultdict(bytearray)

    def feed_data(self, data):
        """Append new data to the buffer.

        Every data has following format: <uuid>:<base64><:?>\n
        """

        uuid, payload, *cont = data.split(b':')

        self._channels[uuid].extend(payload)

        # if we not have an ending colon, we are finished
        if not cont:
            # resolve future
            self._resolve_message(Message(uuid, b''.join(self._channels[uuid])))

    def _resolve_message(self, message):
        """Finalize the waiter by providing a message."""

        self._waiter.set_result(message)

    async def _wait_for_data(self):
        """Create a future and wait for its fruition."""

        if self._waiter is not None:
            raise RuntimeError("There is already a corroutine waiting for data on this transport")

        self._waiter = asyncio.futures.Future(loop=self._loop)
        try:
            return await self._waiter
        finally:
            self._waiter = None

    async def __aiter__(self):
        return self

    async def __anext__(self):
        while True:
            value = await self._wait_for_data()

            if value == b'':
                continue
                # raise StopAsyncIteration
            return value


class MessageProtocol(asyncio.Protocol):

    """
    Just feed data to the buffer.
    """

    def __init__(self, buffer):
        self._transport = None
        self.buffer = buffer

    def connection_made(self, transport):
        self._transport = transport

    def connection_lost(self, exc):
        raise exc

    def data_received(self, data):
        try:
            self.buffer.feed_data(data)
            sys.stderr.buffer.write(b"Incomming message: " + data)

        except ValueError:
            sys.stderr.buffer.write(b"Bad message: " + data)

    def eof_received(self):
        pass


class Receiver(object):

    """
    Remote control.
    """
    def __init__(self, loop, done=None):
        self.loop = loop
        self.done = done

    async def messages(self):
        buffer = MessageBuffer(self.loop)
        protocol = MessageProtocol(buffer)
        await self.loop.connect_read_pipe(lambda: protocol, sys.stdin)

        async for message in buffer:
            sys.stdout.buffer.write(message.payload)


    async def recv(self):
        reader = DataStreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        await self.loop.connect_read_pipe(lambda: protocol, sys.stdin)

        try:
            while True:
                data = await reader.read_data()
                if data == b'':
                    continue

                await asyncio.sleep(1)
                sys.stdout.buffer.write(data)
                if data == b'terminate':
                    if self.done:
                        self.done.set_result(None)
                    return
        except:
            import traceback

            with open("receive.log", "w") as log:
                log.write(traceback.format_exc())

            raise

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
        loop = asyncio.ProactorEventLoop()  # for subprocess pipes on Windows
        asyncio.set_event_loop(loop)
    else:
        loop = asyncio.get_event_loop()

    done = asyncio.Future(loop=loop)
    def ask_exit(signame):
        print("got signal %s: exit" % signame)
        done.set_result(None)
        loop.stop()

    # register shutdowen
    for signame in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(
            getattr(signal, signame),
            functools.partial(ask_exit, signame)
        )
    try:

        receiver = Receiver(loop)

        # returncode = loop.create_task(receiver.receive())

        # loop.run_until_complete(done)
        # done.set_result(None)
        loop.run_until_complete(receiver.messages())

    finally:
        loop.close()


if __name__ == '__main__':
    main(sys.argv)
