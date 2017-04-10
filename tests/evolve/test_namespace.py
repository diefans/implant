import pytest


@pytest.fixture
def definitions():
    from debellator.evolve import definitions

    return definitions


@pytest.mark.parametrize('ref_string,groups', [
    ('foo#bar:specs.yaml:def', ('foo#bar', 'specs.yaml', 'def')),
    (':specs/module.yml:xyz', ('', 'specs/module.yml', 'xyz')),
    ('specs.yaml:foo', (None, 'specs.yaml', 'foo')),
    # an empty spec is not allowed
    (':foo', None),
    ('foo', (None, None, 'foo')),
])
def test_reference_string(definitions, ref_string, groups):
    match = definitions.re_reference_string.match(ref_string)

    if groups is None:
        assert match is None

    else:
        assert match.groups() == groups


@pytest.fixture
def test_dir():
    import os
    return os.path.realpath(os.path.dirname(__file__))


@pytest.fixture
def bootstrap(test_dir):
    from debellator.evolve import config
    config.bootstrap(root=test_dir)


@pytest.mark.usefixtures('bootstrap')
def test_lookup_namespace(definitions, test_dir):
    from debellator.evolve import specs
    # from zope import component

    ns = definitions._lookup_namespace('')
    assert isinstance(ns, specs.DirectoryNamespace)
    assert str(ns.root) == test_dir


@pytest.mark.usefixtures('bootstrap')
def test_lookup_reference(definitions):
    ref = definitions._lookup_reference(':test_specs.yaml:bar1')

    assert ref.ref_string == 'foo'
    assert ref.target == 'bar'


@pytest.mark.parametrize('df,target', [
    ('bar1', 'bar'),
    ('bar2', 'bar'),
    ('bar3', 'bar'),
])
@pytest.mark.usefixtures('bootstrap')
def test_spec_references(definitions, df, target):
    ns = definitions._lookup_namespace('')
    spec = ns['test_specs.yaml']

    assert spec[df].target == target


@pytest.mark.usefixtures('bootstrap')
def test_specs(definitions):
    ns = definitions._lookup_namespace('')
    spec = ns['test_specs.yaml']

    keys = list(spec.keys())

    assert keys == ['foo', 'bar1', 'bar2', 'bar3']
