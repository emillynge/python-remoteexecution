from __future__ import (absolute_import, print_function, unicode_literals, division)
from .Environments import EnvironmentFactory
from .Utils import Commandline
__author__ = 'emil'

def main():
    Commandline().__call__()

if __name__ == '__main__':
    main()
