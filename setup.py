"""package setup"""

import os

from setuptools import find_packages, setup

__version__ = "0.0.0"


def read(*paths):
    """Build a file path from *paths* and return the contents."""
    with open(os.path.join(*paths), 'r') as f:
        return f.read()


setup(
    name="implant",
    author="Oliver Berger",
    author_email="diefans@gmail.com",
    url="https://github.com/diefans/implant",
    description='Remote execution via stdin/stdout messaging.',
    long_description=read('README.rst'),
    version=__version__,
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: Apache Software License',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Topic :: Internet :: WWW/HTTP',
        'Framework :: AsyncIO',
    ],
    license='Apache License Version 2.0',

    keywords="asyncio ssh RPC Remote execution dependency"
    " injection stdin stdout messaging",

    package_dir={'': 'src'},
    # namespace_packages=['implant'],
    packages=find_packages(
        'src',
        exclude=["tests*"]
    ),
    include_package_data=True,
    entry_points={
        'console_scripts': [
            'implant=implant.scripts:run'
        ],
        'pytest11': ['implant = implant.testing'],
    },

    install_requires=read('requirements.txt').split('\n'),
    extras_require={
        'dev': read('requirements-dev.txt').split('\n'),
        'uvloop': read('requirements-uvloop.txt').split('\n'),
        'tokio': read('requirements-tokio.txt').split('\n'),
    },
    dependency_links=[
        'git+https://github.com/PyO3/tokio#egg=tokio-0.99.0'
    ],
)
