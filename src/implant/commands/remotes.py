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
