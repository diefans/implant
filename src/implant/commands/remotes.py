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

import logging
import os

from implant import commands, core

log = logging.getLogger(__name__)


class Echo(commands.Echo):
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
            'remote_data': self.data,
            'remote_pid': os.getpid()
        }
