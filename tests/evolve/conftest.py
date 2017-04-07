import pytest


@pytest.fixture(scope='session')
def dist():
    import pkg_resources
    return pkg_resources.get_distribution('debellator')
