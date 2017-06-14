"""
Core features
"""

import asyncio
import concurrent
import threading
import traceback

from debellator import core
from zope import interface, component


class Echo(core.Command):
    async def local(self, remote_future):
        incomming = []

        # # custom protocol
        # # first receive
        # async for i, msg in core.aenumerate(core.JsonChannelIterator(channel_in)):
        #     incomming.append(msg)

        # # second send
        # await core.JsonChannelIterator(channel_out).send({'i': i} for i in range(10))

        result = await remote_future
        return [result, incomming]

    async def remote(self):

        data = []

        # # first send
        # await core.JsonChannelIterator(channel_out).send({'i': str(i ** 2) for i in range(10)})

        # # second receive
        # async for msg in core.JsonChannelIterator(channel_in):
        #     data.append(msg)

        # raise Exception("foo")
        return {
            'params': self.params,
            'data': data
        }


class Copy(core.Command):
    def __init__(self, *args, **kwargs):
        super(Copy, self).__init__(*args, **kwargs)

        assert self.src
        assert self.dest

        self.executor = concurrent.futures.ThreadPoolExecutor()
        self.loop = asyncio.get_event_loop()

    def __del__(self):
        self.executor.shutdown(wait=True)

    async def local(self, remote_future):
        with open(self.src, "rb") as f:
            async with self.channel.stop_iteration():
                while True:
                    data = await self.loop.run_in_executor(self.executor, f.read, 0x8000)
                    if not data:
                        break

                    await self.channel.send(data)

        result = await remote_future

        return result

    async def remote(self):
        with open(self.dest, "wb") as f:
            async for data in self.channel:
                await self.loop.run_in_executor(self.executor, f.write, data)





# provide all strategies


# provide all control structures
# sequence, choice, if, loop
core.log.info("loaded module: %s", __name__)
