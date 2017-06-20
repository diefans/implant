"""
Core features
"""

import asyncio
import concurrent
import threading
import traceback

from debellator import core
from zope import interface, component


class Copy(core.Command):
    def __init__(self, *args, **kwargs):
        super(Copy, self).__init__(*args, **kwargs)

        assert self.src
        assert self.dest

        self.executor = concurrent.futures.ThreadPoolExecutor()
        self.loop = asyncio.get_event_loop()

    def __del__(self):
        self.executor.shutdown(wait=True)

    async def local(self, io_queues, remote_future):
        with open(self.src, "rb") as f:
            async with self.channel.stop_iteration():
                while True:
                    data = await self.loop.run_in_executor(self.executor, f.read, 0x8000)
                    if not data:
                        break

                    await self.channel.send(data)

        result = await remote_future

        return result

    async def remote(self, io_queues):
        with open(self.dest, "wb") as f:
            async for data in self.channel:
                await self.loop.run_in_executor(self.executor, f.write, data)





# provide all strategies


# provide all control structures
# sequence, choice, if, loop
core.log.info("loaded module: %s", __name__)
