"""package setup"""

__version__ = "0.0.0"

import sys, os

from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand


class PyTest(TestCommand):
    """Our test runner."""

    user_options = [('pytest-args=', 'a', "Arguments to pass to py.test")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = ["tests/unit"]

    def finalize_options(self):
        # pylint: disable=W0201
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        # import here, cause outside the eggs aren't loaded
        import pytest
        errno = pytest.main(self.pytest_args)
        sys.exit(errno)


class Tox(TestCommand):
    user_options = [('tox-args=', 'a', "Arguments to pass to tox")]
    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.tox_args = None
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True
    def run_tests(self):
        #import here, cause outside the eggs aren't loaded
        import tox
        import shlex
        args = self.tox_args
        if args:
            args = shlex.split(self.tox_args)
        errno = tox.cmdline(args=args)
        sys.exit(errno)


def read(*paths):
    """Build a file path from *paths* and return the contents."""
    with open(os.path.join(*paths), 'r') as f:
        return f.read()


setup(
    name="Debellator",
    author="Oliver Berger",
    author_email="diefans@gmail.com",
    url="https://github.com/diefans/debellator",
    description='Remote execution via stdin/stdout messaging.',
    long_description=read('README.rst'),
    version=__version__,
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2.7',
        'Topic :: Database',
    ],
    license='Apache License Version 2.0',

    keywords="asyncio ssh RPC Remote execution dependency injection stdin stdout messaging",

    package_dir={'': 'src'},
    namespace_packages=['debellator'],
    packages=find_packages(
        'src',
        exclude=["tests*"]
    ),
    entry_points={
        'console_scripts': [
            'debellator=debellator.scripts:cli'
        ],
        'debellator.plugins': [
            '_=debellator.core',
            'core=debellator.plugins.core'
        ],
        'debellator.specs': [
            'default=debellator.specs'
        ],
        'debellator.assets': [
            'default=debellator.assets'
        ],
    },

    install_requires=[
        'click',
        'pyyaml'
    ],

    tests_require=['tox'],
    cmdclass = {'test': Tox},
    # tests_require=[
    #     # tests
    #     'pytest',
    #     'pytest-pep8',
    #     'pytest-cache',
    #     'pytest-random',
    #     'mock',
    #     'docker-py',
    # ],
    # cmdclass={'test': PyTest},
)
