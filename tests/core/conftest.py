import pytest


@pytest.fixture
def core():
    from debellator import core
    return core
