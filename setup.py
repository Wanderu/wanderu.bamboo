# coding: utf-8
from setuptools import setup, find_packages
from os.path import join as pathjoin, dirname

def read(*rnames):
    return open(pathjoin(dirname(__file__), *rnames)).read()

setup(
    # about meta
    name = 'wanderu.bamboo',
    version = '1.0.1',
    author = "Wanderu Dev Team",
    author_email = "ckirkos@wanderu.com",
    url = "www.wanderu.com",
    license="Apache License 2.0",
    description = read('README.md'),
    namespace_packages = ['wanderu'],  # setuptools specific feature
    packages = find_packages(),   # Find packages in the 'src' folder
    package_data = {
        'wanderu.bamboo': [
            'scripts/*.lua'
        ]
    },
    install_requires = [
        'setuptools',
        'redis',
        'hiredis'
    ],
    extras_require = {
        # pip install -e .[tx]
        # pip install wanderu.bamboo[tx]
        'tx': ['txredisapi']
    },
    # setup_requires = ['nose'],  # for the `nosetests` setuptools command
    tests_require = ['nose', 'coverage', 'txredisapi'],  # to run the tests themselves
    test_suite = 'nose.collector'
)
