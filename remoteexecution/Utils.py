from __future__ import (absolute_import, print_function, unicode_literals, division)
from collections import defaultdict, namedtuple
import threading
import sys
import re
from importlib import import_module
from argparse import ArgumentParser
import datetime
import logging
from time import (sleep, time)

from boltons import tbutils
import Pyro4

Pyro4.config.COMMTIMEOUT = 5.0  # without this daemon.close() hangs
import os
import json
import Pyro4
from Pyro4 import errors as pyro_errors
from Pyro4 import util as pyro_util
from functools import partial
from .Environments import (EnvironmentFactory, communication_environment, serializing_environment)
from .Environments import set_default as default_environment

__author__ = 'emil'
from sshtunnel import SSHTunnelForwarder, address_to_str
from pxssh import pxssh
from pexpect import spawn
from StringIO import StringIO
from hashlib import md5
from random import random


# class ShellOverwrite(object):
#     """ Methods that must be overridden by topmost class """
#     __metaclass__ = abc.ABCMeta
#
#     @abc.abstractmethod
#     def __init__(self, work_dir=None, settings=None, **kwargs):
#         """ init method should completely inititalize instance such that sendline works immediately
#         :param work_dir:
#         :param settings: settings needed for init
#         :param kwargs: sent directly to super init
#         :return:
#         """
#         super(ShellOverwrite, self).__init__(**kwargs)
#
#     @abc.abstractmethod
#     def close(self, force=True):
#         """ shutdown shell instance """
#         super(ShellOverwrite, self).close(force=force)
#         pass
#
#
# class ShellRequired(object):
#     """ Methods that is required but can be provided by mixed in classes """
#     __metaclass__ = abc.ABCMeta
#
#     @abc.abstractmethod
#     def start_orphaned_process(self, command=""):
#         """ take command in string or list format, return pid of any spawned process """
#         pid = int()
#         return pid
#
#     @abc.abstractmethod
#     def prompt(self, timeout=-1):
#         """ wait for prompt to be ready to receive new command. return bool indicating whether prompt is reached """
#         return_code = bool()
#         return return_code
#
#     @abc.abstractmethod
#     def expect(self, pattern, timeout=-1, searchwindowsize=-1):
#         """ Wait for shell to output something that matches patterns.
#         return idx of the matched pattern and set current position to this line"""
#         idx = int()
#         return idx
#
#     @abc.abstractmethod
#     def readline(self):
#         """ read the line at current position """
#         line = str()
#         return line

class SSHOutPipe(StringIO):
    def __init__(self, ssh_session, io_file):
        assert isinstance(ssh_session, SSHPrompt)
        self.ssh_session = ssh_session
        self.io_file = io_file
        self.io_file_open = True
        # noinspection PyTypeChecker,PyTypeChecker
        StringIO.__init__(self)

    def fill_buffer(self):
        if not self.io_file_open:
            return None
        start_tag = md5(str(random())).hexdigest()  # len=32
        end_tag = md5(str(random())).hexdigest()  # len=32
        self.ssh_session.send_python_script(["fp = open('{0}', 'r')".format(self.io_file),
                                             "fp.seek({0})".format(self.len),
                                             "end = '{0}' + '{1}'".format(end_tag[:16], end_tag[16:]),
                                             "start = '{0}' + '{1}'".format(start_tag[:16], start_tag[16:]),
                                             'print(start + fp.read() + end)',
                                             'fp.close()'])
        self.ssh_session.expect('{0}.*{1}'.format(start_tag, end_tag))
        self.write(self.ssh_session.after[32:-32])
        self.ssh_session.prompt()

    def read(self, **kwargs):
        pos = self.pos
        self.fill_buffer()
        self.pos = pos
        return StringIO.read(self, **kwargs)

    def readline(self, **kwargs):
        pos = self.pos
        if self.len == self.pos and not self.buflist:
            self.fill_buffer()
        self.pos = pos
        return StringIO.readline(self, **kwargs)


class SSHPopen(object):
    # noinspection PyUnusedLocal
    def __init__(self, commands, work_dir='.', ssh_prompt=None, ssh_settings=None, stdout=None, stderr=None,
                 stdin=None):
        self.ssh_session = ssh_prompt
        if not ssh_prompt:
            self.ssh_session = SSHPrompt()
            self.ssh_session.login(**ssh_settings)
            self.ssh_session.set_lock(self)
        else:
            assert isinstance(ssh_prompt, (SSHPrompt, BashPrompt))
            if self.ssh_session.is_locked():
                raise Exception('Cannot use propmt, locked by another object')
            self.ssh_session.set_lock(self)

        self.ssh_session.sendline('cd ' + work_dir)
        self.ssh_session.prompt()
        python_lines = ['import tempfile', 'import os', "print tempfile.mkdtemp(prefix='s082768-', suffix='-sshIO')",
                        "print os.sep"]
        self.ssh_session.sendline('python -c "' + '; '.join(python_lines) + '"')
        self.ssh_session.expect('[^\n\r]+\w+-sshIO')
        self.io_dir = self.ssh_session.after
        self.ssh_session.expect('[^\n\r]+')
        self.sep = self.ssh_session.after
        self.io_in = self.io_dir + self.sep + 'nohup.in'
        self.io_out = self.io_dir + self.sep + 'nohup.out'
        self.io_err = self.io_dir + self.sep + 'nohup.err'
        self.ssh_session.prompt()
        self.ssh_session.sendline("""python -c "open('{0}', 'w').close()" """.format(self.io_in))
        self.ssh_session.prompt()
        self.ssh_session.sendline("nohup {0} > {1} 2>{2} < {3} &".format(' '.join(commands), self.io_out, self.io_err,
                                                                         self.io_in))
        self.ssh_session.expect('\[\d+\] \d+')
        self.pid = self.ssh_session.after.split(' ')[-1]
        if stdout:
            self.stdout = SSHOutPipe(self.ssh_session, self.io_out)
        else:
            self.stdout = None
        if stderr:
            self.stderr = SSHOutPipe(self.ssh_session, self.io_err)
        else:
            self.stderr = None

        self.stdin = None

    def poll(self):
        self.ssh_session.require_lock(self)
        self.ssh_session.sendline('ps -p {0}; echo "END"'.format(self.pid))
        if self.ssh_session.expect(['(\d\d:\d\d:\d\d) (.+?)((<defunct>)|(\r))', 'END\r']):
            self.ssh_session.prompt()
            return 0
        self.ssh_session.prompt()

    def terminate(self):
        self.ssh_session.sendline('kill -9 {0}'.format(self.pid))
        self.ssh_session.prompt()
        self.ssh_session.release_lock(self)

    def communicate(self):
        while self.poll() is None:
            sleep(1)
        self.ssh_session.release_lock(self)

        stdout = self.stdout
        if stdout is not None:
            stdout = stdout.read()

        stderr = self.stderr
        if stdout is not None:
            stderr = stderr.read()

        return stdout, stderr

    def close_fd(self):
        if self.stdout:
            self.stdout.io_file_open = False
        if self.stderr:
            self.stderr.io_file_open = False
        if self.stdin:
            self.stdin.io_file_open = False
        self.ssh_session.send_python_script(["import shutil",
                                             "shutil.rmtree('{0}')".format(self.io_dir)])

    def __del__(self):
        self.terminate()
        self.close_fd()

class LockMixin(object):
    def __init__(self, *args, **kwargs):
        self._owner = None
        super(LockMixin, self).__init__(*args, **kwargs)

    def set_lock(self, obj):
        if self._owner is not None:
            raise Exception('Cannot lock, already locked!')
        self._owner = id(obj)

    def release_lock(self, obj):
        if self._owner is None:
            return
        if id(obj) != self._owner:
            raise Exception('Cannot release. not locked by this object')
        self._owner = None

    def has_lock(self, obj):
        return id(obj) == self._owner

    def is_locked(self):
        return self._owner is not None

    def require_lock(self, obj):
        InvalidUserInput.compare('obj', self._owner, id(obj), 'Not locked by this object')

class SSHPrompt(LockMixin, pxssh):

    def login(self, **kwargs):
        """
        Adapter class that converts TunnelForwarder SSH credentials to pxssh type
        """
        server, username = (None, None)
        for key, val in kwargs.items():
            if key == 'ssh_address_or_host':
                if isinstance(val, tuple):
                    server = val[0]
                    kwargs['port'] = val[1]
                else:
                    server = val
            elif key == 'ssh_port':
                kwargs['port'] = val
            elif key == 'ssh_host_key':
                pass
            elif key == 'ssh_username':
                username = val
            elif key == 'ssh_password':
                kwargs['password'] = val
            elif key == 'ssh_private_key':
                kwargs['ssh_key'] = val
            else:
                continue
            del (kwargs[key])

        return super(SSHPrompt, self).login(server, username, **kwargs)

    def send_python_script(self, python_lines):
        self.sendline('python -c "' + '; '.join(python_lines) + '"')

    def __del__(self):
        self.logout()
        sleep(1)
        self.close()


class BashPrompt(LockMixin, spawn):
    def __init__(self):
        self.PROMPT = "[\$\#] "
        super(BashPrompt, self).__init__('bash -i')

    def prompt(self, timeout=-1):
        """Match the next shell prompt.

        This is little more than a short-cut to the :meth:`~pexpect.spawn.expect`
        method. Note that if you called :meth:`login` with
        ``auto_prompt_reset=False``, then before calling :meth:`prompt` you must
        set the :attr:`PROMPT` attribute to a regex that it will use for
        matching the prompt.

        Calling :meth:`prompt` will erase the contents of the :attr:`before`
        attribute even if no prompt is ever matched. If timeout is not given or
        it is set to -1 then self.timeout is used.

        :return: True if the shell prompt was matched, False if the timeout was
                 reached.
        """
        from pexpect import TIMEOUT

        if timeout == -1:
            timeout = self.timeout
        i = self.expect([self.PROMPT, TIMEOUT], timeout=timeout)
        if i == 1:
            return False
        return True

    def logout(self):
        pass


# noinspection PyClassHasNoInit
class EnvironmentCallMixin:
    @staticmethod
    def env_call(environment, method_name, *args, **kwargs):
        return getattr(EnvironmentFactory.get_environment(environment), method_name).__call__(*args, **kwargs)


class TunnelForwarder(SSHTunnelForwarder):
    def __init__(self, *args, **kwargs):
        super(TunnelForwarder, self).__init__(*args, **kwargs)
        for srv in self._server_list:
            self.suppress_server_errors(srv)

    @staticmethod
    def suppress_server_errors(srv):
        def suppres_error(*args):
            pass
        setattr(srv, 'handle_error', suppres_error)

    def on_the_fly_tunnel(self, local_bind_address=None, remote_bind_address=None):
        # Check if the requested tunnels already has a valid server
        for _srv in self._server_list:
            if remote_bind_address:
                if _srv.remote_host != remote_bind_address[0] and remote_bind_address[0] != '0.0.0.0':
                    continue  # remote host is provided, is not default and does not match
                if _srv.remote_port != remote_bind_address[1] and remote_bind_address[1] != 0:
                    continue  # remote port is provided, is not default and does not match
            if local_bind_address:
                if _srv.local_host != local_bind_address[0] and local_bind_address[0] != '0.0.0.0':
                    continue  # local host is provided, is not default and does not match
                if _srv.local_port != local_bind_address[1] and local_bind_address[1] != 0:
                    continue  # local port is provided, is not default and does not match

            # If this code is reached the current server is valid for the requested tunnel
            return _srv.local_host, _srv.local_port

        # If this code is reached, we have found no valid server among the existing tunnels
        if local_bind_address is None:
            local_bind_address = ('0.0.0.0', 0)  # default adress and port
        elif isinstance(local_bind_address, (str, unicode)):
            local_bind_address = (local_bind_address, 0)  # default port
        elif isinstance(local_bind_address, int):
            local_bind_address = ('0.0.0.0', local_bind_address)  # default address

        if not self._is_started:
            self.start()

        srv = self.make_ssh_forward_server(remote_bind_address, local_bind_address)
        self.suppress_server_errors(srv)

        self._server_list.append(srv)
        thread = threading.Thread(
            target=self.serve_forever_wrapper, args=(srv,),
            name="Srv-" + address_to_str(srv.local_address))
        thread.daemon = self.daemon_forward_servers
        thread.start()
        self._threads.append(thread)
        self.tunnel_is_up[srv.local_address] = self.local_is_up(srv.local_address)
        if self.tunnel_is_up[srv.local_address]:
            self.logger.error("An error occurred while opening tunnel.")
        return srv.local_host, srv.local_port

def import_obj_from(module_name, obj_name):
    return import_module(module_name).__dict__[obj_name]


def get_external_ip():
    return Pyro4.socketutil.getIpAddress('localhost', workaround127=True)


def make_path(path, ignore_last=False):
    paths = path.split('/')
    if ignore_last:
        paths = paths[:-1]

    abs_path = paths[0]
    exists = True
    for p in paths[1:] + ['']:
        if abs_path and (not exists or not os.path.exists(abs_path)):
            os.mkdir(abs_path)
            exists = False
        abs_path += '/' + p
        if not p:
            break


class RemoteExecutionLogger(object):
    def __init__(self, **settings):
        self.settings = settings
        self.logger = self.create_logger(**settings)
        for item in dir(self.logger):
            if item[:2] != '__':
                setattr(self, item, getattr(self.logger, item))

    def duplicate(self, append_name=None, **overwrites):
        if append_name:
            overwrites['logger_name'] = self.settings.get('logger_name', '?') + ' - ' + append_name
        _settings = dict()
        _settings.update(self.settings)
        _settings.update(overwrites)
        return RemoteExecutionLogger(**_settings)

    def write(self, s):
        self.logger.debug(s)

    def flush(self):
        pass

    @staticmethod
    def create_logger(logger_name="RemoteExecution", log_to_file=None, log_to_stream=True, log_level='DEBUG',
                      format_str=None):
        if not log_to_file and not log_to_stream:  # neither stream nor logfile specified. no logger wanted.
            return DummyLogger()
        # create logger with 'spam_application'
        _logger = logging.getLogger(logger_name)
        _logger.setLevel(log_level)
        # create formatter and add it to the handlers
        if not format_str:
            format_str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        formatter = logging.Formatter(format_str)

        if log_to_file is None:
            log_to_file = ['logs/default.log']
        elif not isinstance(log_to_file, (list, tuple)):
            log_to_file = [log_to_file] if log_to_file else list()

        if log_to_file:
            for logfile in log_to_file:
                make_path(logfile, ignore_last=True)
                # create file handler which logs even debug messages
                fh = logging.FileHandler(logfile)
                fh.setLevel(log_level)
                fh.setFormatter(formatter)
                _logger.addHandler(fh)

        if log_to_stream:
            # create console handler with a higher log level
            ch = logging.StreamHandler()
            ch.setLevel(log_level)
            ch.setFormatter(formatter)
            _logger.addHandler(ch)

        return _logger


class DummyLogger(object):
    def __init__(self):
        self.settings = dict()

    def debug(self, *args, **kwargs):
        pass

    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass

    # noinspection PyUnusedLocal
    def duplicate(self, **kwargs):
        return self


class Commandline(object):
    def __init__(self):
        self.args2method = {'manager': {'start': self.start_manager,
                                        'stop': self.stop_manager,
                                        'isup': self.isup_manager},
                            'executor': {'start': self.start_executor,
                                         'stop': self.stop_executor}}
        self.logger = DummyLogger()
        self.stdout_logger = RemoteExecutionLogger.create_logger('CLI/stdout', log_to_file="", log_to_stream=True,
                                                                 format_str='%(message)s')
        self.argv = tuple()
        self.data = dict()
        self.commands = None

    def __call__(self, *commands):
        if commands:
            if len(commands) > 1:
                self.argv = commands
            else:
                self.argv = commands[0].split(' ')
            self.args = self.parse_args()
        else:
            self.argv = sys.argv[1:]
            self.args = self.parse_args()
            self.set_environment()
            self.logger = self.create_logger()

        if self.args.remote:
            self.remote_call(self.argv)
        else:
            self.pre_execute()
            self.execute()
            self.post_execute()

    def set_environment(self):
        if self.args.env_info:
            EnvironmentFactory.set_from_env_info(self.args.env_info[0])
        else:
            default_environment()

    def get(self, *args):
        return tuple(self.data[key] for key in args)

    # noinspection PyMethodMayBeStatic
    def remote_call(self, commands):
        cli = communication_environment().client2manager_side_cli
        cli(' '.join([command for command in commands if command not in ['-r', '--remote']]))

    def parse_args(self):
        args = self.get_argument_parser().parse_args(self.argv)

        self.data['kwargs'] = dict()
        if args.kwargs:
            for kwarg in args.kwargs:
                if '=' not in kwarg:
                    raise self.CommandLineException(
                        'Invalid input "{0}": key-word arguments must contain a "="'.format(kwarg))
                self.data['kwargs'].update(dict([tuple(kwarg.split('='))]))
        return args

    def get_exec_func(self):
        if self.args.module in self.args2method and self.args.action in self.args2method[self.args.module]:
            return self.args2method[self.args.module][self.args.action]
        return self.method_not_implemented_func

    def method_not_implemented_func(self):
        self.stdout("error", "action {0} not implemented for module {1}".format(self.args.action, self.args.module))

    def pre_execute(self):
        self.data['ip'] = get_external_ip()
        if self.args.ip:
            self.stdout('ip', self.data['ip'])

        self.data['pid'] = os.getpid()
        if self.args.pid:
            self.stdout('pid', self.data['pid'])

    def execute(self):
        try:
            self.get_exec_func().__call__()
        except Exception as e:
            self.logger.error("Exception occurred during execution of {0} {1}".format(self.args.action,
                                                                                      self.args.module),
                              exc_info=True)
            sleep(.5)
            self.stdout('error', e.__class__.__name__ + ': ' + e.message)

    def post_execute(self):
        pass

    def get_logger_args(self):
        kwargs = dict()
        kwargs["log_to_stream"] = self.args.stream
        if self.args.logfiles is False:
            kwargs['log_to_file'] = list()
        else:
            if self.args.logfiles:
                logfiles = list()
                for lf in self.args.logfiles[0]:
                    if lf[0] == '/':
                        logfiles.append(lf)
                    else:
                        logfiles.append(os.path.abspath('.') + os.sep + 'logs' + os.sep + lf)
                kwargs['log_to_file'] = logfiles
        kwargs["logger_name"] = 'CLI/{0} {1}'.format(self.args.action, self.args.module)
        if self.args.log_level:
            kwargs['log_level'] = self.args.log_level
        return kwargs

    def create_logger(self):
        kwargs = self.get_logger_args()
        print(kwargs)
        return RemoteExecutionLogger(**kwargs)

    def stdout(self, tag, obj):
        self.stdout_logger.debug("{0}: {1}\n\r".format(tag, json.dumps(obj)))

    def execute_return(self, result):
        self.stdout('return', result)

    def get_manager_from_manager_side(self):
        comm_env = communication_environment()
        comm_env.my_location = 'manager'

        return WrappedProxy('remote_execution.manager', comm_env.manager_host, comm_env.manager_port,
                            logger=self.logger)

    def get_kwargs(self, *kwargs_fields):
        int_casts = ['sub_id', 'port']
        kwargs = namedtuple('kwargs', kwargs_fields)
        values = list()
        error_message = ""
        for kwarg_field in kwargs_fields:
            if kwarg_field not in self.data['kwargs']:
                error_message += '\n\t{0} is missing.'.format(kwarg_field)
            elif kwarg_field in int_casts:
                values.append(int(self.data['kwargs'][kwarg_field]))
            else:
                values.append(self.data['kwargs'][kwarg_field])
        if error_message:
            raise self.CommandLineException('{0} {1} command requires key-word arguments.{2}'.format(self.args.action,
                                                                                                     self.args.module,
                                                                                                     error_message))
        return kwargs(*tuple(values))

    def start_executor(self):
        from .ServerSide import ExecutionController

        ExecutionController(*self.get_kwargs('sub_id'), logger=self.logger.duplicate(logger_name='Executor'))

    def stop_executor(self):
        # Assume we are on manager side

        manager = self.get_manager_from_manager_side()
        if not manager.sub_shut(*self.get_kwargs('sub_id')):
            manager.sub_del(*self.get_kwargs('sub_id'))
        self.execute_return(0)

    def start_manager(self):
        from .ServerSide import Manager

        comm_env = communication_environment()
        comm_env.my_location = 'manager'
        self.logger.debug("Initializing manager")
        daemon = WrappedDaemon(port=comm_env.manager_port, host=comm_env.manager_host)
        self.logger.debug("Init Manager")
        manager = Manager(logger=self.logger)
        wrapped_manager = WrappedObject(manager, logger=self.logger)
        daemon.register(wrapped_manager, "remote_execution.manager")
        self.logger.info("putting manager in request loop")
        self.stdout('blocking', datetime.datetime.now().isoformat())
        daemon.requestLoop(loopCondition=manager.is_alive)
        sleep(1)
        daemon.close()

    def stop_manager(self):
        try:
            manager = self.get_manager_from_manager_side()
            manager.shutdown()
            while True:
                manager.is_alive()
        except pyro_errors.CommunicationError:
            self.execute_return(0)

    def isup_manager(self):
        try:

            manager = self.get_manager_from_manager_side()
            if manager.is_alive():
                self.execute_return(True)
        except pyro_errors.CommunicationError:
            self.execute_return(False)

    @staticmethod
    def get_argument_parser():
        parser = ArgumentParser('Command line interface to QsubTools')
        parser.add_argument('-i', '--get-ip',
                            action='store_true',
                            help='output ip to stdout before executing action',
                            dest='ip')

        parser.add_argument('-p', '--get-pid',
                            action='store_true',
                            help='output pid to stdout before executing action',
                            dest='pid')

        parser.add_argument('-r', '--remote',
                            action='store_true',
                            help='execute this command on remote server',
                            dest='remote')

        parser.add_argument('-E', '--env-info',
                            help='specify the environment info in string format *without* spaces',
                            nargs=1,
                            dest='env_info')

        logging_group = parser.add_argument_group("logging")
        logging_group.add_argument('-s', '--stream', action='store_true',
                                   help='activate logging to stdout (default False)',
                                   dest='stream')

        logging_group.add_argument('-L', '--log-level',
                                   choices=['ERROR', 'WARNING', 'INFO', 'DEBUG'],
                                   help='log level',
                                   dest='log_level')

        # Logfile specification
        logfile_group = logging_group.add_mutually_exclusive_group()
        logfile_group.add_argument('-f', '--file',
                                   action='append',
                                   help='specify logging to specific file(s).',
                                   dest='logfiles', nargs='+')

        logfile_group.add_argument('-F', '--disable-file',
                                   action='store_false',
                                   help='disable logging to file',
                                   dest='logfiles')

        # Required arguments
        parser.add_argument('action',
                            choices=['start', 'stop', 'isup'],
                            help="action to send to module")

        parser.add_argument('module',
                            choices=['manager', 'executor'],
                            help="module to send action to")

        parser.add_argument('kwargs',
                            help="arguments to send to module",
                            nargs='*',
                            metavar='key=value')
        return parser

    @staticmethod
    class CommandLineException(Exception):
        pass


class RemoteCommandline(Commandline):
    def __init__(self, make_ssh_prompt, interpreter_args, target):
        self.ssh = None
        self.make_ssh_prompt = make_ssh_prompt
        self.logger = DummyLogger()
        self.interpreter = interpreter_args
        self.target = target
        super(RemoteCommandline, self).__init__()

    def __call__(self, commands):
        if self.ssh is None:
            self.ssh = self.make_ssh_prompt()
        super(RemoteCommandline, self).__call__(commands)

    def blocking(self):
        if self.args.action == 'start':
            return True
        return False

    def pre_execute(self):
        if self.args.stream:
            self.ssh.logfile = sys.stdout

        blocking = self.blocking()
        full_command = ""
        if blocking:
            full_command += 'nohup '

        full_command += "{1} ".format(self.interpreter, self.target)
        full_command += "-E '{0}' ".format(EnvironmentFactory.cls_repr())
        full_command += ' '.join(self.argv)

        if blocking:
            full_command += ' > nohup.out 2>&1 & tail -f nohup.out'.format(os.devnull)
            self.commands = full_command
        else:
            self.commands = full_command

    def execute(self):
        self.ssh.sendline(self.commands)

    def ssh_expect(self, pattern):
        patterns = ['[eE]rror:', pattern]
        idx = self.ssh.expect(patterns)
        line = self.ssh.readline().strip('[\n\r :]')
        if idx == 0:
            self.data['error'] = line
            sleep(.5)
            self.ssh.terminate()
            self.ssh = None

            # noinspection PyUnresolvedReferences
            raise self.CommandLineException(
                'in {0}\n\t{1}'.format(self.get_exec_func().im_func.func_name, self.data['error']))
        else:
            self.data[patterns[idx].strip(':')] = json.loads(line)

    def post_execute(self):
        if self.args.ip:
            self.ssh_expect('ip:')

        if self.args.pid:
            self.ssh_expect('pid:')

        if not self.blocking():
            self.ssh_expect('return:')
        else:
            self.ssh_expect('blocking')
            self.ssh.terminate()
            self.ssh = None

    def __del__(self):
        self.ssh.close()


class InvalidUserInput(Exception):
    # noinspection PyProtectedMember,PyProtectedMember
    def __init__(self, message, argname="", argnames=tuple(), expected=None, found=None, but_requires=None, indent=3,
                 declaration_frame="method",**kwargs):
        if argname:
            argnames = (argname,)
        calling_frame = sys._getframe(indent)
        method_frame = sys._getframe(indent - 1)
        if declaration_frame == 'method':
            declaration, input_args, method_name = self.get_method_declaration(method_frame)
        else:
            declaration, input_args, method_name = self.get_call_declaration(calling_frame)

        tb = tbutils.TracebackInfo.from_frame(frame=calling_frame, limit=1)
        tb_str = tb.get_formatted()


        self.data = {'expected': expected,
                     'found': found,
                     'argnames': argnames,
                     'method_name': method_name,
                     'pre_message': tb_str[34:] if tb_str else "",
                     'message': message,
                     'declaration': declaration,
                     'input_args': input_args,
                     'arg_no': [input_args.index(argname) + 1 for argname in argnames if argname in input_args] or "",
                     'should': but_requires}
        super(InvalidUserInput, self).__init__(self.message, **kwargs)

    def get_call_declaration(self, frame):
        lno_start = frame.f_code.co_firstlineno
        lno_end = frame.f_lineno

        with open(frame.f_code.co_filename) as fp:
            for k in range(lno_start - 1):
                fp.readline()
            search_space = [fp.readline() for  k in range(lno_end - lno_start + 1)]
        search_space.reverse()

        declaration = list()
        paran_counts = 0
        for line in search_space:
            for char in line:
                if char == ')':
                    paran_counts += 1
                    continue
                if char == '(':
                    paran_counts -= 1
                    continue
            declaration.insert(0, line)
            if paran_counts == 0:
                break

        declaration = ''.join(declaration)
        input_str = re.findall('^\W+\w[^\(]*\((.+)\)'.format(frame.f_code.co_name),
                                   declaration, re.DOTALL)[0]
        declaration_str = re.findall('^\W+([^\(]+\(.+\))'.format(frame.f_code.co_name), declaration, re.DOTALL)[0]

        return declaration_str, self.parse_input_str(input_str), frame.f_code.co_name

    def get_method_declaration(self, frame):
        lno = frame.f_code.co_firstlineno
        with open(frame.f_code.co_filename) as fp:
            for k in range(lno - 1):
                fp.readline()

            declaration_buffer = fp.readline()
            while '):' not in declaration_buffer:
                declaration_buffer += fp.readline()
            input_str = re.findall('def\W+{0}\((.+)\):'.format(frame.f_code.co_name),
                                   declaration_buffer, re.DOTALL)[0]
            declaration_str = re.findall('def\W+({0}\(.+?\)):'.format(frame.f_code.co_name), declaration_buffer)[0]

        return declaration_str, self.parse_input_str(input_str), frame.f_code.co_name

    @property
    def super_message(self):
        return super(InvalidUserInput, self).message

    @property
    def message(self):
        message = "{pre_message} Invalid input to:\n\t {declaration}\t".format(**self.data)
        if self.data['message']:
            message += self.data['message'] + '\n\t'

        if self.data['argnames']:
            if isinstance(self.data['argnames'], tuple) and len(self.data['argnames']) == 1:
                self.data['argnames'] = self.data['argnames'][0]
            message += 'argument {arg_no} "{argnames}"'.format(**self.data)

        if self.data['found']:
            message += ' was "{found}"'.format(**self.data)

        if self.data['expected']:
            message += ' but {method_name} requires {should} "{expected}"'.format(**self.data)
        return message

    @staticmethod
    def compare(argname, expect, found, message="", equal=True):
        if equal and expect != found:
            raise InvalidUserInput(message, argname, expect, found, but_requires='it to be')
        elif not equal and expect == found:
            raise InvalidUserInput(message, argname, expect, found, but_requires='it not to be')

    @staticmethod
    def isinstance(argname, expect, found, message="", equal=True, **kwargs):

        if not isinstance(found, expect) and equal:
            raise InvalidUserInput(message, argname=argname, expected=tuple(cls.__name__ for cls in expect),
                                   found=found.__class__.__name__, but_requires='it to be this class', **kwargs)
        elif isinstance(found, expect) and not equal:
            raise InvalidUserInput(message, argname=argname, expected=tuple(cls.__name__ for cls in expect),
                                   found=found.__class__.__name__, but_requires='it to be this class', **kwargs)

    @staticmethod
    def parse_input_str(input_str):
        quotes = defaultdict(int)
        pairs = {'(': ')', '[': ']', '{': '}'}
        input_args = list()
        curr_arg = ""
        kwarg = False
        for char in input_str:

            if char in '([{':
                quotes[pairs[char]] += 1
                continue

            if char in ')]}':
                quotes[char] -= 1
                if quotes[char] == 0:
                    del (quotes[char])
                continue

            if char in '\n\r\t ':
                continue

            if char == ',' and not quotes:
                input_args.append(curr_arg)
                curr_arg = ''
                kwarg = False
                continue

            if char == '=':
                kwarg = True

            if kwarg:
                continue
            curr_arg += char
        input_args.append(curr_arg)
        input_args = [a.strip(' ') for a in input_args]
        if 'self' in input_args:
            input_args.remove('self')
        return input_args


class WrappedDaemon(Pyro4.Daemon):
    def __init__(self, *args, **kwargs):
        super(WrappedDaemon, self).__init__(*args, **kwargs)

        # noinspection PyPep8Naming
        def get_metadata(objectId):
            obj = self.objectsById.get(objectId)
            if obj is not None:
                if hasattr(obj, 'QSUB_metadata'):
                    return getattr(obj, 'QSUB_metadata')
                return pyro_util.get_exposed_members(obj, only_exposed=Pyro4.config.REQUIRE_EXPOSE)
            else:
                Pyro4.core.log.debug("unknown object requested: %s", objectId)
                raise pyro_errors.DaemonError("unknown object")

        setattr(self.objectsById['Pyro.Daemon'], 'get_metadata', get_metadata)


class WrappedObject(object):
    def __init__(self, obj, logger=DummyLogger()):
        methods = set(m for m in dir(obj) if m[0] != '_' and hasattr(getattr(obj, m), '__call__'))
        logger.debug(methods)
        attrs = set()
        oneway = set()
        props = set(m for m in dir(obj) if m[0] != '_' and m not in methods)

        # exposing methods of wrapped object
        for method_name in methods:
            setattr(self, method_name, getattr(obj, method_name))

        # exposing properties of wrapped object
        for prop_name in props:
            setattr(self, 'QSUB_fget_' + prop_name, partial(getattr, obj, prop_name))
            methods.add('QSUB_fget_' + prop_name)
            setattr(self, 'QSUB_fset_' + prop_name, partial(setattr, obj, prop_name))
            methods.add('QSUB_fset_' + prop_name)
            setattr(self, 'QSUB_fdel_' + prop_name, partial(delattr, obj, prop_name))
            methods.add('QSUB_fdel_' + prop_name)

        self.QSUB_metadata = {"methods": methods,
                              "oneway": oneway,
                              "attrs": attrs}

        self.logger = logger.duplicate(append_name='OWrap')
        self.logger.debug('Logger created')
        self._tickets = defaultdict(dict)

    def __getattribute__(self, item):
        if item in super(WrappedObject, self).__getattribute__('QSUB_metadata')['methods']:
            ser_env = serializing_environment()

            def call(*args, **kwargs):
                ticket = args[0][:6]
                faf = args[1]
                state = self._tickets[ticket].get('state', 'first')
                if state != 'first':
                    self.logger.debug('{0} - already called. state: {1}\t Waiting...'.format(ticket, state))
                    while self._tickets[ticket]['state'] == 'running':
                        sleep(1)
                    if self._tickets[ticket]['state'] == 'error':
                        self.logger.warning('Encountered exception during wait')
                        raise self._tickets[ticket]['error']
                    self.logger.debug('{0} - Returning saved result'.format(ticket))
                    return self._tickets[ticket]['result']

                args = args[2:]
                self.logger.debug('{3} - Calling local method {2} with : {0} , {1}'.format(truncate_object(args),
                                                                                     truncate_object(kwargs), item, ticket))
                if ser_env.serialize_wrapper:
                    decoded = ser_env.decoder(*args)
                    args = tuple(decoded['args'])
                    kwargs = dict((bytes(k), v) for k, v in decoded['kwargs'].iteritems())
                try:
                    self._tickets[ticket]['state'] = 'running'
                    result = super(WrappedObject, self).__getattribute__(item).__call__(*args, **kwargs)
                    self.logger.debug('{2} - Recieved {1} from local method {0}'.format(item, truncate_object(result), ticket))
                    if ser_env.serialize_wrapper:
                        result = ser_env.encoder(result)
                    self._tickets[ticket]['result'] = result
                    self._tickets[ticket]['state'] = 'done'
                    return result
                except Exception as e:
                    self._tickets['state'] = 'error'
                    self._tickets['error'] = e
                    self.logger.error('{0} - Exception during function call'.format(ticket), exc_info=True)
                    raise e

            return call
        else:
            return super(WrappedObject, self).__getattribute__(item)


# noinspection PyPep8Naming
def WrappedProxy(uri, host="", port="", logger=DummyLogger()):
    if host and port:
        uri += '@{0}:{1}'.format(host, port)
    proxy = Pyro4.Proxy('PYRO:' + uri)
    proxy._pyroTimeout = 5
    return wrap_proxy(proxy, logger=logger)


# noinspection PyProtectedMember
def wrap_proxy(pyro_proxy, logger=DummyLogger()):
    _logger = logger.duplicate(append_name='PWrap')
    assert isinstance(pyro_proxy, Pyro4.Proxy)
    pyro_proxy._pyroGetMetadata()
    props = defaultdict(dict)
    in_props = defaultdict(dict)
    methods = dict()

    def manipulate_prop(action, propname, self, *args):
        return self._props[action][propname].__call__(*args)

    for method_name in pyro_proxy._pyroMethods:
        assert isinstance(pyro_proxy, Pyro4.Proxy)
        regexp = re.findall('^QSUB_((fget)|(fset)|(fdel))_(.+$)', method_name)
        if regexp:
            props[regexp[0][-1]][regexp[0][0]] = pyro_proxy.__getattr__(method_name)
            in_props[regexp[0][-1]][regexp[0][0]] = partial(manipulate_prop, regexp[0][-1], regexp[0][0])
        else:
            methods[method_name] = pyro_proxy.__getattr__(method_name)

    # noinspection PyShadowingNames
    class WrappedProxy(object):
        def __init__(self, _props):
            self._props = _props
            self._faf = False

        # noinspection PyMethodMayBeStatic
        def set_timeout(self, timeout):
            pyro_proxy._pyroTimeout = timeout

        def set_fire_and_forget(self, faf):
            self._faf = faf

        def release_socket(self):
            pyro_proxy._pyroRelease()

        # noinspection PyUnboundLocalVariable
        def __getattribute__(self, item):
            if item in methods:
                ser_env = serializing_environment()

                # noinspection PyShadowingNames
                def call(*args, **kwargs):
                    ticket = md5(str(random())).hexdigest()
                    _logger.debug('Calling remote method {0} with {1}, {2}'.format(item, truncate_object(args),
                                                                                   truncate_object(kwargs)))
                    if ser_env.serialize_wrapper:
                        args = (ser_env.encoder({'args': args, 'kwargs': kwargs}),)
                        kwargs = dict()
                    retries = 0
                    while retries < 5:
                        try:
                            result = super(WrappedProxy, self).__getattribute__(item).__call__(ticket, self._faf, *args,
                                                                                               **kwargs)
                            _logger.debug('Recieved {1} from remote method {0}'.format(item, truncate_object(result)))
                            if ser_env.serialize_wrapper:
                                result = ser_env.decoder(result)
                            return result
                        except pyro_errors.CommunicationError as e:
                            _logger.warning('Retrying: {0}'.format(e.message))
                        retries += 1
                        sleep(.1)
                    # noinspection PyUnboundLocalVariable
                    raise e

                return call

            elif item in props:
                retries = 0
                while retries < 5:
                    try:
                        return super(WrappedProxy, self).__getattribute__(item)
                    except pyro_errors.CommunicationError as e:
                        pass
                    retries += 1
                    sleep(.1)
                raise e
            else:
                return super(WrappedProxy, self).__getattribute__(item)

    for m_name, m in methods.items():
        setattr(WrappedProxy, m_name, m)

    for (prop_name, _methods) in in_props.iteritems():
        setattr(WrappedProxy, prop_name, property(**_methods))

    return WrappedProxy(props)


class Timer(object):
    def __init__(self, timeout=-1, interval=.2):
        self.start = time()
        self.timeout = timeout
        self.interval = interval
        self._elapsed = 0

    @property
    def elapsed(self):
        self._elapsed = self.start - time()
        return self._elapsed

    def sleep(self):
        i = int(self.elapsed / self.interval)
        i += 1
        next_tick = i * self.interval
        while self.elapsed < next_tick and not self._timed_out():
            sleep(self.interval / 10)
        return self._timed_out()

    def _timed_out(self):
        return self._elapsed > self.timeout > 0

    def timed_out(self):
        return self.elapsed > self.timeout > 0


def truncate_object(obj):
    if isinstance(obj, dict):
        return dict((key, truncate_object(val)) for key, val in obj.iteritems())

    if isinstance(obj, list):
        return list(truncate_object(e) for e in obj)

    if isinstance(obj, tuple):
        return tuple(truncate_object(e) for e in obj)

    if isinstance(obj, (basestring, bytearray)):
        if len(obj) > 70:
            return obj[:30] + b'...' + obj[-30:]
        return obj
    return obj
