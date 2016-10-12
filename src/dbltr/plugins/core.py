"""
Core features
"""

from dbltr import core


class Echo(core.Command):
    def __init__(self, *args, **kwargs):
        super(Echo, self).__init__()

        self.args = args
        self.kwargs = kwargs

    async def local(self, queue_out, remote_future):
        channel_in = self.channel(core.JsonChannel)
        channel_out = self.channel(core.JsonChannel, queue=queue_out)

        incomming = []

        # custom protocol
        # first receive
        async for i, msg in core.aenumerate(core.JsonChannelIterator(channel_in)):
            incomming.append(msg)

        # second send
        await core.JsonChannelIterator(channel_out).send({'i': i} for i in range(10))

        result = await remote_future
        return [result, incomming]

    async def remote(self, queue_out):
        channel_in = self.channel(core.JsonChannel)
        channel_out = self.channel(core.JsonChannel, queue=queue_out)

        data = []

        # first send
        await core.JsonChannelIterator(channel_out).send({'i': str(i ** 2) for i in range(10)})

        # second receive
        async for msg in core.JsonChannelIterator(channel_in):
            data.append(msg)

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
