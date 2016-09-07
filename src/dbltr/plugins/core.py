"""
Core features
"""

from dbltr import core


# some misc commands
@core.Commander.command()
async def echo(*args, **kwargs):
    return args, kwargs


@echo.remote
async def echo(*args, **kwargs):
    return {
        'args': args,
        'kwargs': kwargs
    }

# provide all strategies


# provide all control structures
# sequence, choice, if, loop

core.logger.info('...core plugins loaded')
