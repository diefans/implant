def test_collect_specs():
    import pkg_resources
    import os
    from debellator import parsing

    ep, *_ = pkg_resources.iter_entry_points(group='debellator.test.specs', name='default')
    specs = parsing.Specs(ep)

    assert specs == {
        parsing.EntryPointSpec(ep, 'test.yaml')
    }


def test_load_definitions():
    import pkg_resources
    from debellator import parsing

    specs = parsing.Specs(*pkg_resources.iter_entry_points(group='debellator.test.specs', name='default'))

    for spec in specs:
        defs = spec.load()

        assert defs == {}
