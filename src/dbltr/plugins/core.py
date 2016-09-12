"""
Core features
"""

from dbltr import core


core.logger.debug("core: %s, commands: %s", id(core.Commander), core.Commander.commands)


# some misc commands
@core.Commander.command()
async def echo(cmd, *args, **kwargs):
    return await cmd(*args, **kwargs)


@echo.remote
async def echo(*args, **kwargs):
    return {
        'args': args,
        'kwargs': kwargs
    }


@core.Commander.command()
async def foo(cmd, *args, **kwargs):
    return await cmd(*args, **kwargs)

# provide all strategies


# provide all control structures
# sequence, choice, if, loop
core.logger.debug("core: %s, commands: %s", id(core.Commander), core.Commander.commands)
core.logger.info('...core plugins loaded')
