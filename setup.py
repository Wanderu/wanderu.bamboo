# coding: utf-8
from os.path import join as pathjoin, dirname
from setuptools import setup, find_packages

def read(*rnames):
    return open(pathjoin(dirname(__file__), *rnames)).read()

setup(
    # about meta
    name = 'wanderu.bamboo',
    version = '1.1.1',
    author = "Wanderu Dev Team",
    author_email = "ckirkos@wanderu.com",
    url = "www.wanderu.com",
    license="Apache License 2.0",
    keywords = ['Redis', 'Queue'],
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
        'six',
        'redis',
        'hiredis'
    ],
    extras_require = {
        # pip install -e .[tx]
        # pip install wanderu.bamboo[tx]
        'tx': ['txredisapi']
    },
    tests_require = ['nose', 'coverage', 'txredisapi'],  # to run the tests themselves
    test_suite = 'nose.collector',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: Apache License 2.0',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
)
