from setuptools import setup
import os.path

setup(
    name='remoteexecution',
    version='1.0',
    packages=['remoteexecution'],
    url='https://github.com/emillynge/python-remoteexecution',
    license='GNU General Public License v3.0',
    author='Emil Sauer Lynge',
    author_email='',
    zip_safe = False,
    description='Execute remotely using a manager to start submits',
    install_requires=['jsoncodecs', 'Pyro4', 'boltons'],
    dependency_links=['https://github.com/emillynge/python-jsoncodecs/tarball/master#egg=jsoncodecs-0.1'],
    entry_points={
    'console_scripts': [
        'remote-exec-cli = remoteexecution.__main__:main',
    ]})
