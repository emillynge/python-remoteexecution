from __future__ import (absolute_import, print_function, unicode_literals, division)
__author__ = 'emil'
import abc
from pexpect import pxssh
import Pyro4
import os
from copy import deepcopy
import json
from subprocess import (Popen, PIPE)
from jsoncodecs import (build_codec, HANDLERS)
import re
from collections import (namedtuple, defaultdict)
from functools import partial
FNULL = open(os.devnull, 'w')

def set_default():
    EnvironmentFactory.set_environments(communication=CommAllOnSameMachine, execution=ExecTest,
                                        serializer=SerializingEnvironment)
    EnvironmentFactory.set_settings(manager_work_dir=os.path.abspath('.'), manager_port=5000,
                                    manager_interpreter='python2', manager_target='remote-exec-cli',
                                    pyro_serializer='serpent')

def serializing_environment():
    env_obj = EnvironmentFactory.get_environment('serializer')
    assert isinstance(env_obj, (SerializingEnvironment))
    return env_obj

def communication_environment():
    env_obj = EnvironmentFactory.get_environment('communication')
    assert isinstance(env_obj, (CommunicationEnvironment, DTUHPCCommunication, CommAllOnSameMachine,
                                Manager2ExecutorThroughSSH, Client2ManagerThroughSSH))
    return env_obj

def execution_environment():
    env_obj = EnvironmentFactory.get_environment('execution')
    assert isinstance(env_obj, (ExecutionEnvironment))
    return env_obj


class EnvironmentFactory(object):
    def __init__(self):
        self._settings = self._get_set_settings()
        self._environments = self._get_set_environments()
        self._set_get_factory(new_factory=self)

    @classmethod
    def set_from_env_info(cls, env_info_string):
        env_info = json.loads(env_info_string)

        for key, val in env_info.iteritems():
            if key == 'settings':
                cls.set_settings(**val)
            else:
                cls.set_environments(**{key: val})

    @classmethod
    def set_settings(cls, **factory_settings):
        cls._get_set_settings(**factory_settings)

    @staticmethod
    def _get_set_settings(_settings=dict(),  **factory_settings):
        if 'pyro_serializer' in factory_settings:
            Pyro4.config.SERIALIZER = factory_settings['pyro_serializer']
            Pyro4.config.SERIALIZERS_ACCEPTED.add(factory_settings['pyro_serializer'])

        if factory_settings:
            _settings.update(factory_settings)
        return _settings

    @classmethod
    def set_environments(cls, **environments):
        cls._get_set_environments(**environments)

    @classmethod
    def _get_set_environments(cls, _environments=dict(),  **environments):
        for env_name, env_cls in environments.iteritems():
            env_obj = cls.load_environment_obj(env_name, env_cls)
            _environments[env_name] = env_obj
        return _environments

    @staticmethod
    def _set_get_factory(_factory=list(), new_factory=None):
        if new_factory is not None:
            _factory.append(new_factory)
        elif not _factory:
            EnvironmentFactory()
        return _factory[0]

    @staticmethod
    def load_environment_obj(env_name, cls):
        if isinstance(cls, (str, unicode)):
            return EnvironmentFactory.load_environment_obj(env_name, import_obj_from(*tuple(cls.split(':'))))
        elif isinstance(cls, dict):
            return EnvironmentFactory.load_environment_obj(env_name, import_obj_from(*cls.popitem()))
        elif issubclass(cls, Environment):
            return cls()
        raise InvalidUserInput('Trying to set invalid environment', argname=env_name,
                               expected='Subclass of Environment or string', found=cls)

    @classmethod
    def get_environment(cls, environment_name):
        factory = cls._set_get_factory()
        if not factory:
            factory = cls()

        if environment_name not in factory._environments:
            raise InvalidUserInput('Requested environment has not been set', argname='environment_name',
                                   expected=factory._environments, found=environment_name)
        env_obj = factory._environments[environment_name]
        assert issubclass(env_obj.__class__, Environment)
        env_obj.set_settings(**factory._settings)
        return env_obj

    def __repr__(self):
        env_info = dict()
        for env_name, env_obj in self._environments.iteritems():
            env_info[env_name] = {env_obj.__class__.__module__: env_obj.__class__.__name__}
        env_info['settings'] = self._settings
        env_info_string = json.dumps(env_info)
        return env_info_string.replace(' ', '')

    @classmethod
    def cls_repr(cls):
        factory = cls._set_get_factory()
        return factory.__repr__()


from .Utils import (WrappedProxy, import_obj_from, InvalidUserInput, DummyLogger, TunnelForwarder, RemoteCommandline, SSHPrompt, \
                    get_external_ip, BashPrompt, SSHPopen)
from .ClientSide import (BaseScriptGenerator, SimpleScriptGenerator, HPCScriptGenerator)


class Environment(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        self.settings = dict()

    def set_attribute_if_in_settings(self, *attributes, **settings):
        _settings = deepcopy(settings)
        for attr in attributes:
            if attr in _settings:
                setattr(self, attr, _settings.pop(attr))
        return _settings

    def set_settings(self, **settings):
        self.settings = settings

    def is_attrs_set(self, **attr_expect_pairs):
        for attr_name, expect_type in attr_expect_pairs.iteritems():
            self.is_attr_set(attr_name, expect_type)

    def is_attr_set(self, attr_name, expected_type):
        attr = getattr(self, attr_name)
        InvalidUserInput.isinstance(attr_name, expected_type, attr, message='missing environment settings')

    def __repr__(self):
        return '{"' + self.__class__.__module__ + '":"' + self.__class__.__name__ + '"}'



class _CommunicationRequired(Environment):
    __metaclass__ = abc.ABCMeta

    # Remote commandlines always needed to get from client to manager side.
    # in some cases it may be necessary to get to the executor side and from executor to manager
    @abc.abstractmethod
    def make_remote_cli_client2manager(self):
        return RemoteCommandline(None)

    # optional
    @abc.abstractmethod
    def make_remote_cli_manager2executor(self):
        return RemoteCommandline(None)

    # optional
    @abc.abstractmethod
    def make_remote_cli_executor2manager(self):
        return RemoteCommandline(None)

    # Tunnels are needed from client and executor to manager to obtain manager_proxy.
    # Manager tunnels should only take optional address arguments.
    # Furthermore a proxy is needed from client and manager to the executor, these should take address arguments
    # Optionally proxies from manager and executor can be implemented with address arguments.
    #  All tunnels return the *local* bindings for host and port

    # *Manager tunnels*
    @abc.abstractmethod
    def client2manager_tunnel(self, manager_host=None, manager_port=None):
        host = str()
        port = int()
        return host, port

    @abc.abstractmethod
    def executor2manager_tunnel(self, manager_host=None, manager_port=None):
        host = str()
        port = int()
        return host, port

    # *Executor tunnels*
    @abc.abstractmethod
    def manager2executor_tunnel(self, executor_host, executor_port):
        host = str()
        port = int()
        return host, port

    # *Client tunnels* (optional)
    @abc.abstractmethod
    def manager2client_tunnel(self, client_host, client_port):
        host = str()
        port = int()
        return host, port

class _CommunicationOptionals(object):
    @property
    def executor_popen(self):
        raise NotImplementedError('Method is marked as non-available for this configuration')

    def manager2client_tunnel(self, client_host, client_port):
        raise NotImplementedError('Method is marked as non-available for this configuration')

    def make_remote_cli_manager2executor(self):
        raise NotImplementedError('Method is marked as non-available for this configuration')

    def make_remote_cli_executor2manager(self):
        raise NotImplementedError('Method is marked as non-available for this configuration')


class CommunicationEnvironment(_CommunicationOptionals, _CommunicationRequired):
    def __init__(self):
        super(Environment, self).__init__()
        self.my_location = None
        self._my_ip = None

        self.manager_port = None
        self._manager_ip = None
        self._manager_proxy = None

        self._manager_side_cli = None
        self._client_side_cli = None
        self._executor_side_cli = None

    def set_settings(self, manager_port=None, manager_ip=None,
                     manager_work_dir=None, client_work_dir=None,  executor_work_dir=None,
                     **settings):

        self.manager_port = manager_port or self.manager_port
        self._manager_ip = manager_ip or self.manager_port
        self.is_attr_set('manager_port', int)
        super(CommunicationEnvironment, self).set_settings(**settings)

    @property
    def manager_side_cli(self):
        if self._manager_side_cli:
            return self._manager_side_cli
        if self.my_location == 'client':
            self._manager_side_cli = self.make_remote_cli_client2manager()
            return self._manager_side_cli
        if self.my_location == 'executor':
            self._manager_side_cli = self.make_remote_cli_executor2manager()
            return self._manager_side_cli
        raise Exception('Cannot request a manager command line when my_location is unknown')

    @property
    def client2manager_side_cli(self):
        self.my_location = 'client'
        return self.manager_side_cli

    @property
    def executor2manager_side_cli(self):
        self.my_location = 'executor'
        return self.manager_side_cli

    @property
    def my_ip(self):
        if not self._my_ip:
            self._my_ip = get_external_ip()
        return self._my_ip

    @property
    def manager_ip(self):
        if self._manager_ip:
            return self._manager_ip

        self.manager_side_cli('-i isup manager')
        ip = self.manager_side_cli.get('ip')[0]
        self._manager_ip = ip
        EnvironmentFactory.set_settings(manager_ip=ip)
        return ip

    @property
    def manager_host(self):
        """ default is that the manager is registered on the external ip
        :return: the hostname used to connect to manager.
        """
        return self.manager_ip

    # This one is inferred and should not be overridden
    def client2executor_tunnel(self, executor_host, executor_port):
        manager_host_binding, manager_port_binding = self.client2manager_proxy.env_call('communication',
                                                                                        'manager2executor_tunnel',
                                                                                             executor_host,
                                                                                             executor_port)
        host, port = self.client2manager_tunnel(manager_host=manager_host_binding, manager_port=manager_port_binding)
        return host, port


    # This one is inferred and should not be overridden
    def executor2client_tunnel(self, client_host, client_port):
        manager_host_binding, manager_port_binding = self.executor2manager_proxy.env_call('communication',
                                                                                           'manager2client_tunnel',
                                                                                               client_host,
                                                                                               client_port)
        host, port = self.executor2manager_tunnel(manager_host=manager_host_binding, manager_port=manager_port_binding)
        return host, port

    # *Manager proxies*
    @property
    def client2manager_proxy(self, manager_host=None, manager_port=None):
        if not self._manager_proxy:
            local_host, local_port = self.client2manager_tunnel()
            self._manager_proxy = WrappedProxy('remote_execution.manager@{0}:{1}'.format(local_host, local_port))
        return self._manager_proxy

    @property
    def executor2manager_proxy(self, manager_host=None, manager_port=None):
        if not self._manager_proxy:
            local_host, local_port = self.executor2manager_tunnel()
            self._manager_proxy = WrappedProxy('remote_execution.manager@{0}:{1}'.format(local_host, local_port))
        return self._manager_proxy


class BashMixin(object):
    @staticmethod
    def bash_prompt(work_dir):
        s = BashPrompt()
        s.sendline('cd {0}'.format(work_dir))
        s.prompt()
        return s

    def spoof_manager_remote_cli(self):
        ex_env = execution_environment()

        def ssh_instance_generator():
            return self.bash_prompt(ex_env.manager_work_dir)

        cli = RemoteCommandline(ssh_instance_generator, ex_env.manager_interpreter, ex_env.manager_target)
        return cli

    def spoof_executor_remote_cli(self):
        ex_env = execution_environment()

        def ssh_instance_generator():
            self.bash_prompt(ex_env.executor_work_dir)

        cli = RemoteCommandline(ssh_instance_generator, ex_env.executor_interpreter, ex_env.executor_target)
        return cli


class SSHMixin(object):
    def __init__(self):
        self.tunnel_managers = dict()
        super(SSHMixin, self).__init__()

    def make_tunnel(self, ssh_settings, local_bind_address=None, remote_bind_address=None):
        """
        make a ssh tunnel from local_bind_address to remote_bind_address on a remote host. login with ssh_settings
        :param local_bind_address: tuple with (host, port) if None a free port is found on localhost
        :param remote_bind_address: tuple with (host, port)
        :param ssh_settings: dict with the settings:
            ssh_address_or_host
            ssh_port
            ssh_host_key
            ssh_username
            ssh_password
            ssh_private_key
        :return: local address bindings (local_host, local_port)
        """
        assert isinstance(ssh_settings, dict)
        connection = str(ssh_settings['ssh_address_or_host'])
        if 'ssh_username' in ssh_settings:
            connection += '@' + ssh_settings['ssh_username']
        if connection in self.tunnel_managers:
            return self.tunnel_managers[connection].on_the_fly_tunnel(local_bind_address=None, remote_bind_address=None)
        server = TunnelForwarder(
            local_bind_address=local_bind_address,
            remote_bind_address=remote_bind_address,
            logger=DummyLogger(),
            raise_exception_if_any_forwarder_have_a_problem=False,
            **ssh_settings)
        server.start()
        self.tunnel_managers[connection] = server
        return server.local_bind_host, server.local_bind_port

    @staticmethod
    def ssh_prompt(ssh_settings, work_dir):
        s = SSHPrompt()
        if not s.login(**ssh_settings):
            raise pxssh.ExceptionPxssh('Login failed')

        s.sendline('cd {0}'.format(work_dir))
        s.prompt()
        return s

class Client2ManagerThroughSSH(SSHMixin, CommunicationEnvironment):
    def __init__(self):
        self.client2manager_ssh_settings = None
        super(Client2ManagerThroughSSH, self).__init__()

    def set_settings(self, ssh_client2manager=None, **settings):
        self.client2manager_ssh_settings = ssh_client2manager
        self.is_attr_set('client2manager_ssh_settings', dict)
        super(Client2ManagerThroughSSH, self).set_settings(**settings)

    def client2manager_tunnel(self, manager_host=None, manager_port=None):
        self.my_location = 'client'
        manager_host = manager_host or self.manager_host
        manager_port = manager_port or self.manager_port
        return self.make_tunnel(self.client2manager_ssh_settings, remote_bind_address=(manager_host, manager_port))

    def make_remote_cli_client2manager(self):
        ex_env = execution_environment()

        def ssh_instance_generator():
            self.ssh_prompt(self.client2manager_ssh_settings, ex_env.manager_work_dir)
        cli = RemoteCommandline(ssh_instance_generator, ex_env.manager_interpreter, ex_env.manager_target)
        return cli

class Executor2ManagerThroughSSH(SSHMixin, CommunicationEnvironment):
    def __init__(self):
        self.executor2manager_ssh_settings = None
        super(Executor2ManagerThroughSSH, self).__init__()

    def set_settings(self, ssh_executor2manager=None, **settings):
        self.executor2manager_ssh_settings = ssh_executor2manager
        self.is_attr_set('client2manager_ssh_settings', dict)
        super(Executor2ManagerThroughSSH, self).set_settings(**settings)

    def executor2manager_tunnel(self, manager_host=None, manager_port=None):
        self.my_location = 'client'
        manager_host = manager_host or self.manager_host
        manager_port = manager_port or self.manager_port
        return self.make_tunnel(self.executor2manager_ssh_settings, remote_bind_address=(manager_host, manager_port))

    def make_remote_cli_executor2manager(self):
        ex_env = execution_environment()

        def ssh_instance_generator():
            self.ssh_prompt(self.executor2manager_ssh_settings, ex_env.manager_work_dir)
        cli = RemoteCommandline(ssh_instance_generator, ex_env.manager_interpreter, ex_env.manager_target)
        return cli


class Manager2ExecutorThroughSSH(SSHMixin, CommunicationEnvironment):
    def __init__(self):
        self.manager2executor_ssh_settings = None
        self.executor_popen_ssh = None
        super(Manager2ExecutorThroughSSH, self).__init__()

    def set_settings(self, ssh_manager2executor=None, **settings):
        self.manager2executor_ssh_settings = ssh_manager2executor
        self.is_attr_set('manager2executor_ssh_settings', dict)
        super(Manager2ExecutorThroughSSH, self).set_settings(**settings)

    def manager2executor_tunnel(self, executor_host, executor_port):
        self.my_location = 'manager'
        return self.make_tunnel(self.manager2executor_ssh_settings, remote_bind_address=(executor_host, executor_port))

    def make_remote_cli_manager2executor(self):
        ex_env = execution_environment()

        def ssh_instance_generator():
            self.ssh_prompt(self.manager2executor_ssh_settings, ex_env.executor_work_dir)
        cli = RemoteCommandline(ssh_instance_generator, ex_env.executor_interpreter, ex_env.executor_target)
        return cli

    def ssh_instance_manager2executor(self):
        ex_env = execution_environment()
        return self.ssh_prompt(self.manager2executor_ssh_settings, ex_env.executor_work_dir)

    @property
    def executor_popen(self):
        if not self.executor_popen_ssh:
            self.executor_popen_ssh = self.ssh_instance_manager2executor()
        ex_env = execution_environment()
        return partial(SSHPopen, work_dir=ex_env.executor_work_dir, ssh_prompt=self.executor_popen_ssh)

class ManagerAndExecutorOnLAN(CommunicationEnvironment):
    def manager2executor_tunnel(self, executor_host, executor_port):
        return executor_host, executor_port

    def executor2manager_tunnel(self, manager_host=None, manager_port=None):
        if manager_host is None and self:
            raise NotImplementedError('Cannot determine manager_host automatically over LAN')
        manager_port = manager_port or self.manager_port
        return manager_host, manager_port


class DTUHPCCommunication(Client2ManagerThroughSSH, ManagerAndExecutorOnLAN):
    pass


class CommManagerAndExecutorOnSameMachine(BashMixin, CommunicationEnvironment):
    @property
    def executor_popen(self):
        return Popen

    def manager2executor_tunnel(self, executor_host, executor_port):
        return executor_host, executor_port

    def make_remote_cli_executor2manager(self):
        return self.spoof_manager_remote_cli()

    def executor2manager_tunnel(self, manager_host=None, manager_port=None):
        manager_host = manager_host or self.manager_host
        manager_port = manager_port or self.manager_port
        return manager_host, manager_port


class CommClientAndManagerOnSameMachine(BashMixin, CommunicationEnvironment):
    def manager2client_tunnel(self, client_host, client_port):
        return client_host, client_port

    def client2manager_tunnel(self, manager_host=None, manager_port=None):
        manager_host = manager_host or self.manager_host
        manager_port = manager_port or self.manager_port
        return manager_host, manager_port

    def make_remote_cli_client2manager(self):
        return self.spoof_manager_remote_cli()

    def manager2executor_tunnel(self, executor_host, executor_port):
        return executor_host, executor_port


class CommAllOnSameMachine(CommManagerAndExecutorOnSameMachine, CommClientAndManagerOnSameMachine):
    @property
    def manager_host(self):
        return 'localhost'

    @property
    def my_ip(self):
        return 'localhost'


class ExecutionEnvironment(Environment):
    def __init__(self):
        self.client_interpreter = None
        self.client_target = None
        self.client_work_dir = None
        self.manager_interpreter = None
        self.manager_target = None
        self.manager_work_dir = None
        self.executor_interpreter = None
        self.executor_target = None
        self.executor_work_dir = None
        self.output_cls = namedtuple('Output', ['stdout', 'stderr'])
        super(ExecutionEnvironment, self).__init__()
        
    def set_settings(self, **settings):
        _settings = self.set_attribute_if_in_settings('client_interpreter',
                                                      'client_target',
                                                      'client_work_dir',
                                                      'manager_interpreter',
                                                      'manager_target',
                                                      'manager_work_dir',
                                                      'executor_interpreter',
                                                      'executor_target',
                                                      'executor_work_dir', **settings)
        self.is_attrs_set(client_interpreter=(str, unicode),
                          client_target=(str, unicode),
                          client_work_dir=(str, unicode),
                          manager_interpreter=(str, unicode),
                          manager_target=(str, unicode),
                          manager_work_dir=(str, unicode),
                          executor_interpreter=(str, unicode),
                          executor_target=(str, unicode),
                          executor_work_dir=(str, unicode))
        super(ExecutionEnvironment, self).set_settings(**_settings)

    @property
    def client_command_line_prefix(self):
        return '{1} -E \'{2}\''.format(self.client_interpreter, self.client_target, EnvironmentFactory.cls_repr())

    @property
    def manager_command_line_prefix(self):
        return '{1} -E \'{2}\''.format(self.manager_interpreter, self.manager_target, EnvironmentFactory.cls_repr())

    @property
    def executor_command_line_prefix(self):
        return '{1} -E \'{2}\''.format(self.executor_interpreter, self.executor_target, EnvironmentFactory.cls_repr())

    @abc.abstractmethod
    def job_start(self, execution_script_location):
        job_id = str()
        return job_id

    @abc.abstractmethod
    def job_stat(self, job_id):
        state = dict()
        return state

    @abc.abstractmethod
    def job_del(self, job_id):
        output = self.output_cls()
        return output

    @abc.abstractproperty
    def script_generator(self):
        return BaseScriptGenerator()


class ExecManagerAndExecutorOnSameMachine(ExecutionEnvironment):
    def set_settings(self,  **settings):
        _settings = self.set_attribute_if_in_settings('manager_work_dir', 'manager_interpreter', 'manager_target',
                                                      **settings)
        self.executor_work_dir = self.manager_work_dir
        self.executor_interpreter = self.manager_interpreter
        self.executor_target = self.manager_target
        super(ExecManagerAndExecutorOnSameMachine, self).set_settings(**_settings)


class ExecClientAndManagerOnSameMachine(ExecutionEnvironment):
    def set_settings(self,  **settings):
        _settings = self.set_attribute_if_in_settings('manager_work_dir', 'manager_interpreter', 'manager_target',
                                                      **settings)
        self.client_work_dir = self.manager_work_dir
        self.client_interpreter = self.manager_interpreter
        self.client_target = self.manager_target
        super(ExecClientAndManagerOnSameMachine, self).set_settings(**_settings)


class ExecAllOnSameMachine(ExecManagerAndExecutorOnSameMachine, ExecClientAndManagerOnSameMachine):
    def set_settings(self,  work_dir=None, interpreter=None, target=None, **settings):
        _settings = self.set_attribute_if_in_settings('manager_work_dir','manager_target', 'manager_interpreter', **settings)
        self.manager_work_dir = work_dir or self.manager_work_dir
        self.manager_target = target or self.manager_target
        self.manager_interpreter = interpreter or self.manager_interpreter
        super(ExecAllOnSameMachine, self).set_settings(**_settings)


class PopenExecution(ExecutionEnvironment):
    def job_start(self, execution_script_location):
        comm_env = communication_environment()
        _POpen = comm_env.executor_popen
        p1 = _POpen(['sh', execution_script_location])
        p2 = _POpen(['ps', '--ppid', str(p1.pid)], stdout=PIPE)
        p2.stdout.readline()
        line = p2.stdout.readline()
        job_id = re.findall('\d+', line)[0]
        return job_id

    def job_stat(self, job_id):
        comm_env = communication_environment()
        _POpen = comm_env.executor_popen
        p_stat = _POpen(['ps', '-p', job_id], stdout=PIPE)
        p_stat.stdout.readline()
        line = p_stat.stdout.readline()
        rexp = re.findall('(\d\d:\d\d:\d\d) (.+?)((<defunct>)|($))', line)
        if rexp:
            time = rexp[0][0]
            if rexp[0][2]:
                state = 'C'
            else:
                state = 'R'
        else:
            time = '00:00:00'
            state = 'C'

        return state, time

    @property
    def script_generator(self):
        comm_env = communication_environment()
        return SimpleScriptGenerator(self.executor_work_dir)

    def job_del(self, job_id, fire_and_forget=False):
        comm_env = communication_environment()
        _POpen = comm_env.executor_popen
        if fire_and_forget:
            Popen(['kill', '-9', job_id])
            return None

        p = Popen(['kill', '-9', job_id], stderr=PIPE, stdout=PIPE)
        return p.communicate()


class QsubExecution(ExecutionEnvironment):
    def __init__(self):
        self._available_modules = None
        self.base_modules = None
        super(QsubExecution, self).__init__()

    def set_settings(self, **settings):
        _settings = self.set_attribute_if_in_settings('base_modules', **settings)
        super(QsubExecution, self).set_settings(**_settings)

    @property
    def available_modules(self):
        if self._available_modules:
            return self._available_modules
        p = Popen("module avail", stdout=PIPE, stderr=PIPE, shell=True)
        (o, e) = p.communicate()
        if e:
            lines = re.findall('/apps/dcc/etc/Modules/modulefiles\W+(.+)',
                               e, re.DOTALL)
        else:
            lines = list()

        if lines:
            lines = lines[0]
        else:
            return dict()

        modules = re.split('[ \t\n]+', lines)[:-1]
        module_ver_list = [m.strip('(default)').split('/') for m in modules]

        module_dict = defaultdict(list)
        for mod_ver in module_ver_list:
            if len(mod_ver) < 2:
                mod_ver.append('default')

            module_dict[mod_ver[0]].append(mod_ver[1])
        self._available_modules = module_dict
        return module_dict

    def job_stat(self, job_id):
        p_stat = Popen(['qstat', job_id], stdout=PIPE, stderr=FNULL)
        vals = re.split('[ ]+', re.findall(job_id + '.+', p_stat.stdout.read())[0])
        keys = ['Job ID', 'Name', 'User', 'Time Use', 'S', 'Queue']
        info = dict(zip(keys, vals[:-1]))
        if info['S'] == 'Q':
            p_start = Popen(['showstart', job_id], stdout=PIPE)
            time_str = re.findall('Estimated Rsv based start in\W+(\d+:\d+:\d+)', p_start.stdout.read()) or ['00:00:00']
            time_str = time_str[0]
            return 'Q', time_str
        return info['S'], info['Time Use']

    def job_start(self, execution_script_location):
        p_sub = Popen(['qsub', execution_script_location], stdout=PIPE, stderr=PIPE)
        stdout = p_sub.stdout.read()
        job_id = re.findall('(\d+)\.\w+', stdout)[0]
        return job_id

    def job_del(self, job_id, fire_and_forget=False):
        if fire_and_forget:
            Popen(['qdel', job_id], stderr=FNULL, stdout=FNULL)
            return None
        p = Popen(['qdel', job_id], stderr=PIPE, stdout=PIPE)
        return p.communicate()

    def script_generator(self):
        comm_env = communication_environment()
        return HPCScriptGenerator(self.base_modules, self.executor_work_dir,
                           comm_env._manager_proxy)

class ExecTest(ExecAllOnSameMachine, PopenExecution):
    pass


class SerializingEnvironment(Environment):
    def __init__(self):
        self.serialize_wrapper = True
        self.codec_handlers = HANDLERS
        self.key_typecasts = list()
        self._decoder = None
        self._encoder = None
        self.codec = None
        super(SerializingEnvironment, self).__init__()

    def build_codec(self):
        (enc, dec) = build_codec('RemoteExec', *tuple(self.codec_handlers))

        def encoder(obj):
            return json.dumps(obj, cls=enc)
        self._encoder = encoder

        def decoder(obj):
            return json.loads(obj, cls=dec, key_typecasts=self.key_typecasts)
        self._decoder = decoder

    @property
    def encoder(self):
        if not self._encoder:
            self.build_codec()
        return self._encoder

    @property
    def decoder(self):
        if not self._decoder:
            self.build_codec()
        return self._decoder


def set_settings(self, **settings):
    _settings = self.set_attribute_if_in_settings('codec_handlers',
                                                  'key_typercasts',
                                                  **settings)

    super(SerializingEnvironment, self).set_settings(**_settings)