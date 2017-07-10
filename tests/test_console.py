import pytest


@pytest.fixture
def console():
    from debellator import console
    return console


@pytest.mark.parametrize('seq, groups', [
    # c0
    ('\x07', {
        'c0': '\x07',
        'c1': None,
        'tail': '',
        'cseq': None, 'cseq_final': None, 'cseq_params': None, 'cseq_inter': None,
        'cstr': None, 'cstr_final': None, 'cstr_chars': None,
        'indep': None,
    }),
    # c1
    ('\x9b0A', {
        'c0': None,
        'c1': '\x9b',
        'tail': '',
        'cseq': '0A', 'cseq_final': 'A', 'cseq_params': '0', 'cseq_inter': None,
        'cstr': None, 'cstr_final': None, 'cstr_chars': None,
        'indep': None,
    }),
    ('\x1b[0A', {
        'c0': '\x1b',
        'c1': '[',
        'tail': '',
        'cseq': '0A', 'cseq_final': 'A', 'cseq_params': '0', 'cseq_inter': None,
        'cstr': None, 'cstr_final': None, 'cstr_chars': None,
        'indep': None,
    }),
    ('\x1b]0;this is the window title\u0007', {
        'c0': '\x1b',
        'c1': ']',
        'tail': '',
        'cseq': None, 'cseq_final': None, 'cseq_params': None, 'cseq_inter': None,
        'cstr': '0;this is the window title\u0007', 'cstr_final': '\x07',
        'cstr_chars': '0;this is the window title',
        'indep': None,
    }),
    # incomplete
    ('\x1b', {
        'c0': '\x1b',
        'c1': '',
        'tail': '',
        'cseq': None, 'cseq_final': None, 'cseq_params': None, 'cseq_inter': None,
        'cstr': None, 'cstr_final': None, 'cstr_chars': None,
        'indep': None,
    }),
    ('\x1b[', {
        'c0': '\x1b',
        'c1': '[',
        'tail': '',
        'cseq': '', 'cseq_final': None, 'cseq_params': None, 'cseq_inter': None,
        'cstr': None, 'cstr_final': None, 'cstr_chars': None,
        'indep': None,
    }),
    ('\x1b[0', {
        'c0': '\x1b',
        'c1': '[',
        'tail': '',
        'cseq': '0', 'cseq_final': None, 'cseq_params': '0', 'cseq_inter': None,
        'cstr': None, 'cstr_final': None, 'cstr_chars': None,
        'indep': None,
    }),
    ('\x1b]0;this is the ', {
        'c0': '\x1b',
        'c1': ']',
        'tail': '',
        'cseq': None, 'cseq_final': None, 'cseq_params': None, 'cseq_inter': None,
        'cstr': '0;this is the ', 'cstr_final': None, 'cstr_chars': '0;this is the ',
        'indep': None,
    }),
    # overcomplete
    ('\x1b[0Aabcd', {
        'c0': '\x1b',
        'c1': '[',
        'tail': 'abcd',
        'cseq': '0A', 'cseq_final': 'A', 'cseq_params': '0', 'cseq_inter': None,
        'cstr': None, 'cstr_chars': None, 'cstr_final': None,
        'indep': None,
    }),
    ('\x1b]0;this is the window title\u0007foobar', {
        'c0': '\x1b',
        'c1': ']',
        'tail': 'foobar',
        'cseq': None, 'cseq_final': None, 'cseq_params': None, 'cseq_inter': None,
        'cstr': '0;this is the window title\u0007', 'cstr_final': '\x07', 'cstr_chars': '0;this is the window title',
        'indep': None,
    }),
    # tail
    ('\x1b[\x30\x20\x30Aabcd', {
        'c0': '\x1b',
        'c1': '[',
        'tail': '\x30Aabcd',
        'cseq': '\x30\x20', 'cseq_final': None, 'cseq_params': '0', 'cseq_inter': '\x20',
        'cstr': None, 'cstr_final': None, 'cstr_chars': None,
        'indep': None,
    }),
])
def test_re_ansi(seq, groups, console):
    m = console.AnsiMatch(seq)
    assert m == groups


@pytest.mark.parametrize('seq, bad', [
    ('\x1bOH', True),
    ('\x1b[D', False),
])
def test_ansi_match(seq, bad, console):
    m = console.AnsiMatch(seq)

    assert m.is_bad is bad
