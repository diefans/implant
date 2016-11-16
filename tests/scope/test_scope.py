import pytest


@pytest.mark.parametrize("levels, result", [
    ([{'foo': 'bar'}, {'bar': 'bar'}, {'foo': 'foo'}], 'foo'),
    ([{'foo': 'bar'}, {'bar': 'foo'}, {'bar': 'bar'}], 'bar'),
    ([{'bar': 'foo'}], KeyError),
    ([], KeyError),
])
def test_stacking(levels, result):
    import inspect
    from debellator import scope

    s = scope.Scope(None)

    for level in levels:
        s.push(level)

    if inspect.isclass(result) and issubclass(result, Exception):
        with pytest.raises(result):
            _ = s['foo']
    else:
        assert s['foo'] == result


@pytest.mark.parametrize("levels, name, dotted, top", [
    (
        [
            {'foo': 'bar'},
            {'bar': {
                'foo': {
                    'baz': 'foobar'
                }
            }},
            {'foo': 'foo'},
            {'baz': 123},
        ],
        'bim', 'bar.foo', {
            'foo': 'foo',
            'bim': {
                'baz': 'foobar'
            }
        }
    ),
])
def test_export(levels, name, dotted, top):
    from debellator import scope

    s = scope.Scope(None)

    for level in levels:
        s.push(level)

    variables = s.pop({name: dotted})

    assert s.levels[-1] == top
