"""
Core features
"""

import asyncio
import concurrent
import logging
import os
import time

from debellator import core


log = logging.getLogger(__name__)


class Echo(core.Command):

    """Demonstrate the basic command API."""

    async def local(self, context):
        # custom protocol
        # first: send
        await context.channel.send_iteration("send to remote")

        # second: receive
        from_remote = []
        async for x in context.channel:
            from_remote.append(x)
        log.debug("************ receiving from remote: %s", from_remote)

        # third: wait for remote to finish and return result
        remote_result = await context.remote_future

        result = {
            'from_remote': ''.join(from_remote),
        }
        result.update(remote_result)
        return result

    async def remote(self, context):
        # first: receive
        from_local = []
        async for x in context.channel:
            from_local.append(x)
        log.debug("************ receiving from local: %s", from_local)

        # second: send
        await context.channel.send_iteration("send to local")

        # third: return result
        return {
            'from_local': ''.join(from_local),
            'remote_self': self,
            'pid': os.getpid()
        }


class SystemLoad(core.Command):
    async def local(self, context):
        t, load = await context.remote_future
        return t, load

    async def remote(self, context):
        t, load = time.time(), os.getloadavg()
        return t, load


class Copy(core.Command):
    def __init__(self, src, dest):
        super(Copy, self).__init__(src=src, dest=dest)

        assert self.src
        assert self.dest

        self.executor = concurrent.futures.ThreadPoolExecutor()
        self.loop = asyncio.get_event_loop()

    def __del__(self):
        self.executor.shutdown(wait=True)

    async def local(self, context):
        with open(self.src, "rb") as f:
            while True:
                data = await self.loop.run_in_executor(self.executor, f.read, 0x8000)
                if not data:
                    context.channel.send(StopAsyncIteration())
                    break
                await context.channel.send(data)
        result = await context.remote_future
        return result

    async def remote(self, context):
        with open(self.dest, "wb") as f:
            async for data in context.channel:
                await self.loop.run_in_executor(self.executor, f.write, data)
