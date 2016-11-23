import pytest


@pytest.fixture(scope='session')
def dist():
    import pkg_resources
    return pkg_resources.get_distribution('debellator')


@pytest.fixture
def specs_entry_point(dist):
    import pkg_resources

    ep = pkg_resources.EntryPoint('default', module_name='debellator.test_specs.default', dist=dist)

    return ep


@pytest.fixture
def specs_entry_point_duplicates(dist):
    import pkg_resources

    ep = pkg_resources.EntryPoint('default', module_name='debellator.test_specs.duplicate', dist=dist)

    return ep


def test_collect_specs(specs_entry_point):
    from debellator import parsing

    specs = parsing.Specs(specs_entry_point)

    assert specs == {
        parsing.EntryPointNamespace(specs_entry_point): {
            'test.yaml': parsing.Spec('test.yaml', namespace=parsing.EntryPointNamespace(specs_entry_point))
        }
    }


def test_definitions(specs_entry_point):
    from debellator import parsing

    specs = parsing.Specs(specs_entry_point)
    namespace = parsing.EntryPointNamespace(specs_entry_point)

    namespace_specs = specs[namespace]

    assert list(namespace_specs.keys()) == ['test.yaml']
    assert list(namespace_specs['test.yaml'].keys()) == ['localhost', 'test', 'foo']
