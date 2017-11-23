from debellator import core


class Foobar(core.Command):

    foobar = core.Parameter(description='foorbar')

    async def local(self, context):
        pass

    async def remote(self, context):
        return 'foobar'


foo = 'bar'
