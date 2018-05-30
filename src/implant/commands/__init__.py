# Copyright 2018 Oliver Berger
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Core features
"""

import asyncio
import concurrent
import logging
import os
import time

from implant import core


log = logging.getLogger(__name__)


class Echo(core.Command):

    """Demonstrate the basic command API."""

    data = core.Parameter(default='ping', description='Meaningful data.')

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
            'local_data': self.data,
            'from_remote': ''.join(from_remote),
        }
        result.update(remote_result)
        return result

    remote = core.CommandRemote('implant.commands.remotes.Echo')


class SystemLoad(core.Command):
    async def local(self, context):
        t, load = await context.remote_future
        return t, load

    async def remote(self, context):
        t, load = time.time(), os.getloadavg()
        return t, load


class Copy(core.Command):

    src = core.Parameter(description='Source file at local side.')
    dest = core.Parameter(description='Desatination file at remote side.')

    def __init__(self, *args, **kwargs):
        super(Copy, self).__init__(*args, **kwargs)

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
