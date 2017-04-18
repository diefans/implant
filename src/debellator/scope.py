"""Scope is the basic means for information transport between tasks and subtasks."""

import itertools

from zope.interface.registry import Components


class Registry(Components, dict):
    def __init__(self, name, *args, **kwargs):
        Components.__init__(self, name, *args, **kwargs)
        dict.__init__(self)



class Scope:

    """A Scope is a dict like stacked structure which is bound to a certain key."""

    def __init__(self, key):
        self.key = key
        self.root = {}
        self.levels = []

    def push(self, variables, defaults=None):
        self.levels.append(dict(defaults or {}, **variables))

    def pop(self, exports=None):
        """Remove a scope level.

        :param exports: a mapping of names and their value branch which should be exported to the next scope level
        """
        if not self.levels:
            raise RuntimeError("The scope has no more levels to pop off: {}".format(self))

        exported = {name: self[dotted_name] for name, dotted_name in exports.items()}

        variables = self.levels.pop()

        level = self.levels[-1] if self.levels else self.root
        level.update(exported)

        return variables

    def __iter__(self):
        """Create a reversed iterator over the stacked levels."""
        def _gen():
            yield from reversed(self.levels)
            yield self.root

        return iter(_gen())

    def __getitem__(self, dotted_name):
        """Resolve a dotted name to a value."""
        name, _, dotted_name = dotted_name.partition('.')

        for variables in self:
            try:
                variable = variables[name]
                break

            except KeyError:
                pass
        else:
            # if we did not found a variable
            raise KeyError('`{}` is undefined in this scope: {}'.format(name, self))

        # descend into variable to return only a branch
        while dotted_name:
            name, _, dotted_name = dotted_name.partition('.')
            if name in variable:
                variable = variable[name]
            else:
                raise KeyError('`{}` is not defined: {}'.format(name, variable))

        return variable
