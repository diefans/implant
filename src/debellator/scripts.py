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
@click.option('use_uvloop', '--uvloop/--no-uvloop', default=False, help='Use uvloop policy.')
@click.option('--debug/--no-debug', default=False, help='Enable or disable debug.')
@click.option('--log-config',
              type=click.File('r'),
              default=None,
              help='Logging configuration in yaml format.')
# @click.option('--config', '-c', default=None, help='General configuration.')
@click.pass_context
def cli(ctx, use_uvloop, debug, log_config):
    if log_config is None:
        log_config = pkg_resources.resource_string('debellator', 'logging.yaml')

    log_config = yaml.load(log_config)
    logging.config.dictConfig(log_config)

    if use_uvloop:
        try:
            import uvloop
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
            log.info("Using uvloop policy")

        except ImportError:
            log.warning("uvloop policy is not available.")

    if ctx.invoked_subcommand is None:
        # we need to import master lazy because master imports core,
        # which may use exclusive decorator, which tries to get the actual loop,
        # which is only set after running set_event_loop_policy
        from debellator import master

        if debug:
            log.info("Enable asyncio debug")
            master.core.logger.setLevel(logging.DEBUG)
            asyncio.get_event_loop().set_debug(debug)

        master.main(log_config=log_config, debug=debug)

    else:
        if debug:
            asyncio.get_event_loop().set_debug(debug)
            log.setLevel(logging.DEBUG)



@cli.command()
@click.option('--root', '-r',
              default=None,
              type=click.Path(exists=True, file_okay=False, resolve_path=True),
              help='Lookup specs in this directory.')
@click.option('--settings', '-s',
              default=None,
              type=click.File(),
              envvar='SETTINGS'
)
@click.argument('definitions', nargs=-1, required=True)
@click.pass_context
def evolve(ctx, root, settings, definitions):
    import yaml
    from debellator import evolve

    evolve.main(
        definitions=definitions,
        settings=yaml.load(settings) if settings is not None else {},
        root=root,
    )
