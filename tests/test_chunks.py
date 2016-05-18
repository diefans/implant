import pytest


@pytest.mark.parametrize('data,size,result', [
    ('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ', 10, ['abcdefghij', 'klmnopqrst', 'uvwxyzABCD',
                                                                  'EFGHIJKLMN', 'OPQRSTUVWX', 'YZ']),
    ('abcdefghij', 2, ['ab', 'cd', 'ef', 'gh', 'ij']),
    ('abcdefghij', 0, ValueError),
    ('j', 2, ['j']),

])
def test_split(data, size, result):
    from dbltr import receive

    if type(result) is type and issubclass(result, Exception):
        with pytest.raises(result):
            splitted = list(receive.split_size(data, size))
    else:
        splitted = list(receive.split_size(data, size))
        assert splitted == result
