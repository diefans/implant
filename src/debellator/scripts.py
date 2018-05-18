"""CLI scripts."""
import asyncio
import importlib
import logging
import logging.config

import click
from ruamel import yaml

from debellator import connect, core, pool

log = logging.getLogger(__name__)

LOGGING_LEVEL_NAMES = map(logging.getLevelName, sorted((
    logging.NOTSET, logging.DEBUG, logging.INFO,
    logging.WARN, logging.ERROR, logging.CRITICAL,
    )))
DEFAULT_LOGGING_LEVEL = logging.getLevelName(logging.WARNING)
_LOG_SIMPLE_FMT = {
    'format': ('{asctime} - {process}/{thread} - '
               '{levelname} - {name} - {message}'),
    'style': '{'}
_LOG_COLORED_FMT = {
    '()': 'colorlog.TTYColoredFormatter',
    'format': ('{asctime} - {process}/{thread} - '
               '{log_color}{levelname}{reset} - {name} - {message}'),
    'style': '{'}


def setup_logging(debug, log_config, remote_log_config,
                  log_level=DEFAULT_LOGGING_LEVEL):
    default_log_config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'simple': _LOG_SIMPLE_FMT,
            'colored': _LOG_COLORED_FMT,
        },
        'handlers': {'console': {'class': 'logging.StreamHandler',
                                 'formatter': 'colored',
                                 'level': logging.NOTSET,
                                 'stream': 'ext://sys.stderr'},
                     'logfile': {'class': 'logging.FileHandler',
                                 'filename': '/tmp/debellator.log',
                                 'formatter': 'simple',
                                 'level': logging.NOTSET}},
        'root': {'handlers': ['console', 'logfile'], 'level': log_level},
    }

    default_remote_log_config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {'simple': _LOG_SIMPLE_FMT},
        'handlers': {'console': {'class': 'logging.StreamHandler',
                                 'formatter': 'simple',
                                 'level': logging.NOTSET,
                                 'stream': 'ext://sys.stderr'},
                     'logfile': {'class': 'logging.FileHandler',
                                 'filename': '/tmp/debellator.log',
                                 'formatter': 'simple',
                                 'level': logging.NOTSET}},
        'root': {'handlers': ['console'], 'level': log_level},
    }

    log_config = yaml.load(log_config)\
        if log_config else default_log_config
    remote_log_config = yaml.load(remote_log_config)\
        if remote_log_config else default_remote_log_config

    logging.config.dictConfig(log_config)

    return log_config, remote_log_config


def find_loop_specs():
    """Just find specs for common loops."""
    loop_specs = {
        module_name: importlib.util.find_spec(module_name)
        for module_name in ('asyncio', 'uvloop', 'tokio')
    }
    return loop_specs


def run():
    """Main entry point."""
    return cli(obj={}, auto_envvar_prefix='DEBELLATOR')     # noqa


@click.group(invoke_without_command=True)
@click.option('event_loop', '--loop', default='asyncio',
              type=click.Choice(find_loop_specs().keys()),
              help='Use a different loop policy.')
@click.option('--debug/--no-debug', default=False,
              help='Enable or disable debug.')
@click.option('--log-config',
              type=click.File('r'),
              default=None,
              help='Logging configuration in yaml format.')
@click.option('--remote-log-config',
              type=click.File('r'),
              default=None,
              help='Logging configuration in yaml format.')
@click.option('--log-level', default=DEFAULT_LOGGING_LEVEL,
              type=click.Choice(LOGGING_LEVEL_NAMES),
              help=f'The logging level, defaults to `{DEFAULT_LOGGING_LEVEL}`')
# @click.option('--config', '-c', default=None, help='General configuration.')
@click.pass_context
def cli(ctx, event_loop, debug, log_config, remote_log_config, log_level):
    """Main CLI entry point."""
    log_config, remote_log_config = setup_logging(
        debug=debug, log_config=log_config,
        remote_log_config=remote_log_config,
        log_level=log_level)

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
        # which may use exclusive decorator,
        # which tries to get the actual loop,
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

    ctx.obj['log_config'] = log_config
    ctx.obj['remote_log_config'] = remote_log_config
    ctx.obj['debug'] = debug


def resolve_command_class(dotted_command_name):
    """Resolve the command class for the given string.

    :param dotted_command_name: <module path>:<command cass name>
    """
    module_name, command_name = dotted_command_name.split(':')
    module = importlib.import_module(module_name)
    command_cls = getattr(module, command_name)
    assert issubclass(command_cls, core.Command), \
        '{} is not a subclass of {}'.format(dotted_command_name, core.Command)
    return command_cls


async def run_command_on_remotes(*connectors,
                                 options, command_cls, command_params,
                                 loop=None):
    """Connect remotes and run the command."""
    remotes_pool = pool.RemotesPool(options, loop=loop)
    # connect
    for connector in connectors:
        await remotes_pool.connect(connector)

    # execute
    results = {connector: await remote.execute(command_cls, **command_params)
               for connector, (remote, *_) in remotes_pool.items()}

    await remotes_pool.stop()
    return results


@cli.command('cmd')
@click.option('dotted_command_name', '-c', '--command', required=True,
              help='the path to a debellator command')
@click.option('command_params_file', '-p', '--params', type=click.File('rb'))
@click.option('remote_uri', '-r', '--remote', help='The remote connection',
              default='local://')
@click.pass_context
def cli_cmd(ctx, dotted_command_name, command_params_file, remote_uri):
    """Execute a :py:obj:`debellator.core.Command` in a remote process."""
    loop = asyncio.get_event_loop()

    # lookup command
    command_cls = resolve_command_class(dotted_command_name)
    command_params = yaml.load(command_params_file.read(), Loader=yaml.Loader)
    options = {
        'log_config': ctx.obj['remote_log_config'],
        'debug': ctx.obj['debug']
    }
    connector = connect.Connector.create_connector(remote_uri)
    task = run_command_on_remotes(
        connector,
        options=options,
        command_cls=command_cls,
        command_params=command_params,
        loop=loop
    )

    results = loop.run_until_complete(task)
    print(yaml.dump(results))
    loop.stop()
    loop.close()
