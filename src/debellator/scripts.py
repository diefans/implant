import asyncio
import logging
import logging.config

import click
import pkg_resources
import yaml

log = logging.getLogger(__name__)


def run():
    return cli(auto_envvar_prefix='DEBELLATOR')


@click.group(invoke_without_command=True)
@click.option('event_loop', '--loop', default='asyncio',
              type=click.Choice(('asyncio', 'uvloop', 'tokio')),
              help='Use uvloop policy.')
@click.option('--debug/--no-debug', default=False, help='Enable or disable debug.')
@click.option('--log-config',
              type=click.File('r'),
              default=None,
              help='Logging configuration in yaml format.')
@click.option('--remote-log-config',
              type=click.File('r'),
              default=None,
              help='Logging configuration in yaml format.')
# @click.option('--config', '-c', default=None, help='General configuration.')
@click.pass_context
def cli(ctx, event_loop, debug, log_config, remote_log_config):
    if log_config is None:
        log_config = pkg_resources.resource_string('debellator', 'logging.yaml')

    if remote_log_config is None:
        remote_log_config = pkg_resources.resource_string('debellator', 'remote-logging.yaml')

    log_config = yaml.load(log_config)
    remote_log_config = yaml.load(remote_log_config)
    logging.config.dictConfig(log_config)

    if event_loop == 'uvloop':
        try:
            import uvloop
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            log.info("Using uvloop event loop policy")

        except ImportError:
            log.warning("uvloop is not available.")

    elif event_loop == 'tokio':
        try:
            import tokio
            asyncio.set_event_loop_policy(tokio.EventLoopPolicy())
            log.info("Using tokio event loop policy")

        except ImportError:
            log.warning("tokio is not available.")

    if ctx.invoked_subcommand is None:
        # we need to import master lazy because master imports core,
        # which may use exclusive decorator, which tries to get the actual loop,
        # which is only set after running set_event_loop_policy
        from debellator import master

        if debug:
            log.info("Enable asyncio debug")
            master.core.log.setLevel(logging.DEBUG)
            asyncio.get_event_loop().set_debug(debug)

        master.main(log_config=remote_log_config, debug=debug)

    else:
        if debug:
            asyncio.get_event_loop().set_debug(debug)
            log.setLevel(logging.DEBUG)
