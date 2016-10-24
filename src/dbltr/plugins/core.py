"""
Core features
"""

from dbltr import core


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


# provide all strategies


# provide all control structures
# sequence, choice, if, loop
