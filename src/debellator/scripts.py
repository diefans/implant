import asyncio
import click
import logging
import logging.config
import pkg_resources

import yaml


logger = logging.getLogger(__name__)


@click.group(invoke_without_command=True)
@click.option('use_uvloop', '--uvloop/--no-uvloop', default=False, help='Use uvloop policy.')
@click.option('--debug/--no-debug', default=False, help='Enable or disable debug.')
@click.option('--log-config',
              type=click.File('r'),
              default=None,
              help='Logging configuration in yaml format.')
@click.pass_context
def cli(ctx, use_uvloop, debug, log_config):
    if use_uvloop:
        try:
            import uvloop
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            # logger.info("Using uvloop policy")

        except ImportError:
            # logger.warning("uvloop policy is not available.")
            pass

    if log_config is None:
        log_config = pkg_resources.resource_string('debellator', 'logging.yaml')

    log_config = yaml.load(log_config)
    logging.config.dictConfig(log_config)


    if ctx.invoked_subcommand is None:
        # we need to import master lazy because master imports core,
        # which may use exclusive decorator, which tries to get the actual loop,
        # which is only set after running set_event_loop_policy
        from debellator import master

        if debug:
            logger.info("Enable asyncio debug")
            master.core.logger.setLevel(logging.DEBUG)
            asyncio.get_event_loop().set_debug(debug)

        master.main(log_config=log_config, debug=debug)


# @cli.command()
# @click.pass_context
# def cmd(ctx):
#     pass
