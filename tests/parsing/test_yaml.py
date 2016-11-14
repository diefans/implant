def test_collect_specs():
    import pkg_resources
    import os
    from debellator import parsing

    specs = parsing.Specs(*pkg_resources.iter_entry_points(group='debellator.specs', name='default'))

    assert specs == {
        parsing.Spec('default', os.path.join(pkg_resources.resource_filename('debellator.specs', ''),
                                             'test.yaml'))
    }


def test_load_definitions():
    import pkg_resources
    from debellator import parsing

    specs = parsing.Specs(*pkg_resources.iter_entry_points(group='debellator.specs', name='default'))

    for spec in specs:
        defs = spec.load()

        assert defs == {}
