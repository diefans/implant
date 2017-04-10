def test_construction():
    import collections
    from zope import component
    import io

    from debellator.evolve import specs, config, definitions

    component.provideUtility(config.Dependencies())

    # this will provide adapters
    config.load_registry_entry_points()

    yaml = """
    foo:
        - null
        - abs
        - 123
    bar:
        - 12.34
        - true
        - false

    baz: !debug
        foo: bar
        bar: baz

    """

    loader = specs.ComponentLoader(io.StringIO(yaml))
    data = loader.get_single_data()

    result = collections.OrderedDict([
        ('foo', [None, 'abs', 123]),
        ('bar', [12.34, True, False]),
        ('baz', definitions.DebugMapping([
            ('foo', 'bar'),
            ('bar', 'baz')
        ]))
    ])
    assert data == result
