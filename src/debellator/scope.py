"""Scope is the basic means for information transport between tasks and subtasks."""

import itertools


class Scope:

    """A Scope is a dict like stacked structure which is bound to a certain key."""

    def __init__(self, key):
        self.key = key
        self.root = {}
        self.levels = []

    def push(self, variables, defaults=None):
        self.levels.append(dict(defaults, **variables))

    def pop(self, *exports):
        """Remove a scope level.

        :param exports: a list of names which should be exported to the next scope level
        """
        if not self.levels:
            raise RuntimeError("The scope has no more levels to pop off: {}".format(self))

        exported = {name: value for name, value in self.levels.pop().items() if name in exports}
        level = self.levels[-1] if self.levels else self.root
        level.update(exported)

    def __getitem__(self, name):
        for variables in reversed((self.root,), self.levels):
            try:
                return variables[name]

            except KeyError:
                pass

        raise KeyError('`{}` is undefined in this scope: {}'.format(name, self))
