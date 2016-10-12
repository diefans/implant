"""
Core features
"""

from dbltr import core


class Echo(core.Command):
    def __init__(self, *args, **kwargs):
        super(Echo, self).__init__()

        self.args = args
        self.kwargs = kwargs

    async def local(self, queue_out, future):
        core.logger.info("local running %s", self)
        channel_in = self.channel(core.JsonChannel)
        channel_out = self.channel(core.JsonChannel, queue=queue_out)

        incomming = []

        # custom protocol
        # first receive
        async for i, (uid, msg) in core.aenumerate(channel_in):
            incomming.append(msg)
            if i == 9:
                break

        # second send
        for i in range(10):
            async with channel_out.message() as send:
                await send({'i': i})

        result = await future
        core.logger.info("local future finished: %s", result)
        return [result, incomming]

    async def remote(self, queue_out):
        core.logger.info("remote running %s", self)

        channel_in = self.channel(core.JsonChannel)
        channel_out = self.channel(core.JsonChannel, queue=queue_out)

        data = []

        # first send
        for i in range(10):
            async with channel_out.message() as send:
                await send({'i': str(i ** 2)})

        # second receive
        async for uid, msg in channel_in:
            core.logger.info("\t\t--> data incomming: %s", msg)
            data.append(msg)

            if msg['i'] == 9:
                break

        # raise Exception("foo")
        return {
            'args': self.args,
            'kwargs': self.kwargs,
            'data': data
        }


# provide all strategies


# provide all control structures
# sequence, choice, if, loop
core.logger.debug("core: %s, commands: %s", id(core.Command), core.Command.commands)
core.logger.info('...core plugins loaded')
