"""Task definition and dependency injection.

task_name: !debellator#core:copy
    src: !path foo.bar
    dest: !path /tmp


"""


class Task:

    """A Task is the unit of work.

    It has name and certain dependencies.

    """

    def __init__(self):
        pass


class copy(Task):
