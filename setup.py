# coding: utf-8
from setuptools import setup, find_packages
from os.path import join as pathjoin, dirname


def read(*rnames):
    return open(pathjoin(dirname(__file__), *rnames)).read()

setup(
    # about meta
    name = 'wanderu.bamboo',
    version = '1.0.0',
    author = "Wanderu Dev Team",
    author_email = "dev@wanderu.com",
    url = "www.wanderu.com",
    # description = read('README.rst'),
    description = "Reliable job processing library backed by redis.",
    # package info
    namespace_packages = ['wanderu'],  # setuptools specific feature
    packages = find_packages(),   # Find packages in the 'src' folder
    # package_dir = {'': 'src'},  # Make the 'src' folder the root
                                  # package folder
    package_data = {
        'wanderu.bamboo': [
            'scripts/*.lua',
            'tests/*.json'
        ]
    },
    install_requires = [
        'setuptools',
        'redis',
        'hiredis',
    ],
    setup_requires = ['nose'],  # for the `nosetests` setuptools command
    tests_require = ['nose', 'coverage'],  # to run the tests themselves
    test_suite = 'nose.collector'
)
