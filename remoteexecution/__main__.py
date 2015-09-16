from __future__ import (absolute_import, print_function, unicode_literals, division)
from .Environments import set_default
from .Utils import Commandline

__author__ = 'emil'


def main():
    set_default()
    Commandline().__call__()


if __name__ == '__main__':
    main()
