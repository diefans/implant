import asyncio
import click
import logging

from dbltr import master

logger = logging.getLogger(__name__)


@click.group(invoke_without_command=True)
@click.option('use_uvloop', '--uvloop/--no-uvloop', default=False, help='Use uvloop policy.')
@click.option('--debug/--no-debug', default=False, help='Enable or disable debug.')
@click.pass_context
def cli(ctx, use_uvloop, debug):
    if use_uvloop:
        try:
            import uvloop
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            logger.info("Using uvloop policy")

        except ImportError:
            logger.warning("uvloop policy is not available.")
            pass

    if debug:
        logger.info("Enable asyncio debug")
        master.core.logger.setLevel(logging.DEBUG)
        asyncio.get_event_loop().set_debug(debug)

    if ctx.invoked_subcommand is None:
        master.main()


# @cli.command()
# @click.pass_context
# def cmd(ctx):
#     pass
