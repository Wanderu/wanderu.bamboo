# coding: utf-8
from os import rename
from os.path import join as pathjoin, dirname
import subprocess
from setuptools import setup, find_packages
from setuptools.command.install import install
from setuptools.command.develop import develop
from setuptools.command.egg_info import egg_info

def read(*rnames):
    """ Reads file """
    return open(pathjoin(dirname(__file__), *rnames)).read()

class GitCloneScripts():
    """ Clones Lua Scripts and puts in expected directory """
    subprocess.check_call(['git', 'clone', 'https://github.com/wanderu/bamboo-scripts'])
    rename('bamboo-scripts', 'wanderu/bamboo/scripts')

class GitCloneScriptsInstall(install):
    """ Git Clone on install """
    def run(self):
        install.run(self)
        GitCloneScripts()

class GitCloneScriptsDevelop(develop):
    """ Git Clone on develop """
    def run(self):
        develop.run(self)
        GitCloneScripts()

class GitCloneScriptsEggInfo(egg_info):
    """ Git Clone on egg_info """
    def run(self):
        egg_info.run(self)
        GitCloneScripts()

setup(
    # about meta
    name = 'wanderu.bamboo',
    version = '1.1.2',
    author = "Wanderu Dev Team",
    author_email = "ckirkos@wanderu.com",
    url = "www.wanderu.com",
    license="Apache License 2.0",
    keywords = ['Redis', 'Queue'],
    description = read('README.md'),
    namespace_packages = ['wanderu'],  # setuptools specific feature
    packages = find_packages(),   # Find packages in the 'src' folder
    cmdclass = {
        'install': GitCloneScriptsInstall,
        'develop': GitCloneScriptsDevelop,
        'egg_info': GitCloneScriptsEggInfo,
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
