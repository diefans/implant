"""We introduce an environment marker to skip integration tests by default.

Environments are: `all`, `ssh`, `lxd` or `docker`,
where `all` will execute all such marked tests.
"""
import pytest


def pytest_addoption(parser):
    parser.addoption('-E', '--env', action='store', metavar='ENV',
                     help='Run tests for for that environment.')


def pytest_configure(config):
    # register an additional marker
    config.addinivalue_line('markers',
                            'env(name): mark test to run only on named environment')


def pytest_runtest_setup(item):
    # setup integration mark
    env_marker = item.get_marker('env')
    if env_marker is not None:
        env_name = env_marker.args[0]
        target_env = item.config.getoption('-E')
        if target_env != 'all' and env_name != target_env:
            pytest.skip('test requires env %r' % env_name)
