from __future__ import (absolute_import, print_function, unicode_literals, division)

__author__ = 'emil'
from .ServerSide import (Manager)
from .Utils import (InvalidUserInput, DummyLogger, RemoteExecutionLogger, WrappedProxy, Commandline)

from .Environments import (communication_environment, execution_environment, EnvironmentFactory)
from Pyro4 import errors as pyro_errors
from time import sleep
import Pyro4
from collections import namedtuple
from subprocess import Popen
import abc

HPC_Time = namedtuple("HPC_Time", ['h', 'm', 's'])
HPC_Time.__new__.__defaults__ = (0, 0, 0)
HPC_resources = namedtuple("HPC_resources", ['nodes', 'ppn', 'gpus', 'pvmem', 'vmem'])
HPC_resources.__new__.__defaults__ = (1, 1, 0, None, None)
ClassInfo = namedtuple('ClassInfo', ['module', 'class_name'])


class Client(object):
    def __init__(self, logger=None):
        self.logger = logger or RemoteExecutionLogger(logger_name='Client')
        InvalidUserInput.isinstance('logger', RemoteExecutionLogger, self.logger)
        self.remote_commandline = communication_environment().client2manager_side_cli
        self.remote_commandline.logger = self.logger.duplicate(logger_name='CLI/Client')
        self.manager_proxy = None
        self.start()

    def start(self):

        if not self.isup_manager():
            self.start_manager()
        else:
            self.get_proxy()

        if not self.manager_proxy.is_alive():
            self.logger.error('could not start manager')
            raise Exception("Could not start manager")
        comm_env = communication_environment()
        host, port = comm_env.client2manager_tunnel()
        self.logger.info("Successfully connected to Manager on {0}".format(port))

    def instance_generator(self, object_descriptor=None, rel_dir=".", **requested_resources):
        script_generator = execution_environment().script_generator
        script_generator.logger = self.logger.duplicate(logger_name='Script')
        assert isinstance(script_generator, (HPCScriptGenerator, SimpleScriptGenerator))
        script_generator.execution_settings(rel_dir=rel_dir, **requested_resources)
        instance = RemoteInstance(self.manager_proxy, self, logger=self.logger,
                                  object_descriptor=object_descriptor, script_generator=script_generator)
        return instance

    def get_proxy(self):
        comm_env = communication_environment()
        host, port = comm_env.client2manager_tunnel()
        self.manager_proxy = WrappedProxy('remote_execution.manager', host, port,
                                      logger=self.logger.duplicate(append_name='Manager'))

    def isup_manager(self):
        self.remote_commandline('-i -f mylog -s isup manager')
        EnvironmentFactory.set_settings(manager_ip=self.remote_commandline.get('ip')[0])
        return self.remote_commandline.get('return')[0]

    def start_manager(self):
        self.remote_commandline('-i -f mylog -s start manager')
        self.get_proxy()

    @staticmethod
    def get_manager(self):
        raise Manager()

    def stop_manager(self):
        self.manager_proxy.shutdown()
        try:
            while True:
                self.manager_proxy.is_alive()
                sleep(1)
        except pyro_errors.CommunicationError:
            pass
        self.manager_proxy.release_socket()
        del self.manager_proxy

    def restart_manager(self):
        self.stop_manager()
        sleep(3)
        for _ in range(5):
            try:
                self.start_manager()
                e = None
                break
            except Commandline.CommandLineException as e:
                if 'Errno 98' in e.message:
                    sleep(5)
                else:
                    raise e
        if e:
            raise e

        self.logger.info("Manager restarted")


class BaseScriptGenerator(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, base_dir):
        self.req_resources = dict()
        self.base_dir = base_dir
        self.rel_dir = '.'
        self.logger = DummyLogger()
        self._lines = list()

    def execution_settings(self, rel_dir='.', **requested_resources):
        """
        called by user or parent
        :param rel_dir: where to execute script relative to basedir set in assemble
        :param requested_resources: key_value pairs that describes the needed ressources for execution
        :return: None
        """
        self.rel_dir = rel_dir
        self.req_resources = requested_resources
        self.check_ressources(**requested_resources)

    @abc.abstractmethod
    def check_ressources(self, **requested_resources):
        """
        input check on requested ressources to be implemented by subclassing.
        should raise InvalidInputError if invalid ressources are requested
        :param requested_resources:
        :return: None
        """

    @property
    def work_dir(self):
        return self.base_dir + '/' + self.rel_dir

    def generate_submit_script(self, execute_commands, log_file, sub_id):
        """ generate a script that sets up execution on the server side
        """
        self._lines = list()
        self._write_lines(execute_commands, log_file, sub_id)
        return self.get_script()

    @abc.abstractmethod
    def _write_lines(self, execute_command, log_file, sub_id):
        """ Dummy method, must be overridden by subclassing. writes lines for the submission script """
        pass

    def get_script(self):
        if not self._lines:
            raise Exception('No script generated yet')
        return '\n'.join(self._lines)


class SimpleScriptGenerator(BaseScriptGenerator):
    def _write_lines(self, execute_command, log_file, sub_id):
        #self._lines.append('#!/bin/sh')
        #self._lines.append('cd ' + self.work_dir)
        self._lines.append(execute_command)

    def check_ressources(self, **requested_resources):
        pass


class HPCScriptGenerator(BaseScriptGenerator):
    """
    Subclass that writes a execution script tailored to the DTU HPC qsub system.
    """

    def __init__(self, base_modules, base_dir, manager):
        self.base_modules = base_modules
        self.manager = manager
        self.mod2script = {'cuda': """if [ -n "$PBS_GPUFILE" ] ; then
        export CUDA_DEVICE=`cat $PBS_GPUFILE | rev | cut -d"-" -f1 | rev | tr -cd [:digit:]` ; fi"""}
        super(HPCScriptGenerator, self).__init__(base_dir)

    def check_ressources(self, wc=HPC_Time(), hw_ressources=HPC_resources(), add_modules=None, username=None):
        assert isinstance(wc, HPC_Time)
        assert isinstance(hw_ressources, HPC_resources)

        if not self.manager.env_call('execution', 'path_exists', self.work_dir):
            raise InvalidUserInput("Work directory {0} doesn't exist.".format(self.work_dir))

        try:
            if hw_ressources.nodes < 1 or hw_ressources.ppn < 1:
                raise InvalidUserInput('A job must have at least 1 node and 1 processor', argname='resources',
                                       found=hw_ressources)
            if not any(wc):
                raise InvalidUserInput('No wall clock time assigned to job', argname='wallclock', found=wc)

            self.check_modules()

            InvalidUserInput.compare(argname='username', expect=None, found=username, equal=False)

        except InvalidUserInput as e:
            self.logger.error('Invalid parameters passed to Qsub', exc_info=True)
            raise e

    @property
    def modules(self):
        for item in self.req_resources['add_modules'].iteritems():
            yield item

        for module_name, version in self.base_modules.iteritems():
            if module_name in self.req_resources['add_modules']:
                continue
            yield (module_name, version)
        raise StopIteration()

    def check_modules(self):
        avail_modules = self.manager.env_call('execution', 'available_modules')
        for (module, version) in self.modules:
            if module not in avail_modules:
                raise InvalidUserInput("Required module {0} is not available".format(module))
            if version and version not in avail_modules[module]:
                raise InvalidUserInput("Required module version {0} is not available for module {1}".format(version,
                                                                                                            module))
            self.logger.debug("module {0}, version {1} is available".format(module, version if version else "default"))

    def _write_lines(self, execute_commands, log_file, sub_id):
        self._lines.append('#!/bin/sh')
        # noinspection PyTypeChecker
        self.write_resources(self.req_resources['hw_ressources'])
        # noinspection PyTypeChecker
        self.write_wallclock(self.req_resources['wc'])
        self.write_name('Remote execution {0}'.format(sub_id))
        self.write_mail(self.req_resources['username'] + '@student.dtu.dk')
        self.append_pbs_pragma('e', log_file + ".e")
        self.append_pbs_pragma('o', log_file + ".o")

        for module_name, version in self.req_resources['modules'].iteritems():
            self._lines.append('module load ' + module_name)
            if version:
                self._lines[-1] += '/' + version
            if module_name in self.mod2script:
                self._lines.append(self.mod2script[module_name])
        self._lines.append("export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:{0}/lib".format(self.base_dir))
        self._lines.append('cd {0}'.format(self.work_dir))

        if isinstance(execute_commands, list):
            self._lines.append('\n'.join(execute_commands))
        else:
            self._lines.append(execute_commands)

    @staticmethod
    def make_pbs_pragma(flag, line):
        return "#PBS -" + flag.strip(' ') + ' ' + line

    def append_pbs_pragma(self, flag, line):
        self._lines.append(self.make_pbs_pragma(flag, line))

    def write_name(self, name):
        self.append_pbs_pragma('N ', name)

    def write_mail(self, mail_address):
        self.append_pbs_pragma('M', mail_address)
        self.append_pbs_pragma('m', 'a')

    def write_resources(self, resources):
        assert isinstance(resources, HPC_resources)
        self.append_pbs_pragma('l', 'nodes={1}:ppn={0}'.format(resources.ppn, resources.nodes))

        if resources.gpus:
            self.append_pbs_pragma('l', 'gpus={0}'.format(resources.gpus))

        if resources.pvmem:
            self.append_pbs_pragma('l', 'pvmem={0}'.format(resources.pvmem))

        if resources.vmem:
            self.append_pbs_pragma('l', 'vmem={0}'.format(resources.vmem))

    def write_wallclock(self, wallclock):
        assert isinstance(wallclock, HPC_Time)
        self.append_pbs_pragma("l", "walltime={0}:{1}:{2}".format(wallclock.h, wallclock.m, wallclock.s))


class RemoteInstance(object):
    def __init__(self, manager_proxy, client, script_generator, logger=DummyLogger(), object_descriptor=None):
        assert isinstance(client, Client)

        self.args = tuple()
        self.kwargs = dict()
        self.obj_descriptor = object_descriptor
        self.manager_proxy = manager_proxy
        self.client = client
        self.script_generator = script_generator
        (self.sub_id, self.logfile) = self.manager_proxy.sub_id_request()
        self.logger = logger.duplicate(logger_name='Instance {0}'.format(self.sub_id))
        self.logger.info("sub_id {0} received".format(self.sub_id))
        self.remote_obj = None
        self.executor_local_host = None
        self.executor_local_port = None
        self.proxy_info = None
        self.orphan = False
        self.submitted = False
        self.execution_controller = None
        self.stage_submission()

    def __call__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        return self

    def stage_submission(self):
        kwargs = {'manager_ip': communication_environment().manager_host,
                  'sub_id': self.sub_id}
        ex_env = execution_environment()
        exe = "{1} -f {0}.log -L DEBUG start executor ".format(self.logfile, ex_env.executor_command_line_prefix)
        exe += "manager_ip={manager_ip} sub_id={sub_id}".format(**kwargs)
        script = self.script_generator.generate_submit_script(exe, self.logfile, self.sub_id)
        self.manager_proxy.sub_stage(self.sub_id, script)

    def make_tunnel(self):
        if self.proxy_info:
            comm_env = communication_environment()
            self.executor_local_host, self.executor_local_port = comm_env.client2executor_tunnel(
                self.proxy_info['host'],
                self.proxy_info['port'])
        else:
            raise Exception('Cannot make tunnel without a ready executor. Have you submitted?')

    def get_execution_controller(self):
        if self.manager_proxy.in_state(self.sub_id, 'ready') and not self.execution_controller:
            self.proxy_info = self.manager_proxy.get_proxy_info(self.sub_id)
            self.make_tunnel()
            self._get_execution_controller()

    def _get_execution_controller(self):
        self.execution_controller = WrappedProxy('remote_execution.executor.controller', self.executor_local_host,
                                                 self.executor_local_port,
                                                 logger=self.logger.duplicate(append_name='Exec'))

    def make_obj(self, obj_descriptor):
        if not all([self.manager_proxy.in_state(self.sub_id, 'ready'), self.execution_controller]):
            raise Exception('Execution controller not ready')

        prototype_set = False
        if isinstance(obj_descriptor, ClassInfo):
            self.execution_controller.set_prototype(cls=obj_descriptor.class_name,
                                                    module=obj_descriptor.module)
            prototype_set = True
        elif hasattr(obj_descriptor, '__name__') and hasattr(obj_descriptor, '__module__'):
            self.execution_controller.set_prototype(cls=obj_descriptor.__name__,
                                                    module=obj_descriptor.__module__)
            prototype_set = True

        if prototype_set:
            obj_info = self.execution_controller.register_new_object(*self.args, **self.kwargs)
            return self._get_obj(obj_info)
        else:
            raise InvalidUserInput('Descriptor matches no valid ways of setting the prototype',
                                   argname='obj_descriptor',
                                   found=obj_descriptor)

    def _get_obj(self, obj_info):
        return WrappedProxy(obj_info['object_id'], self.executor_local_host, self.executor_local_port,
                            logger=self.logger.duplicate(append_name='RemoteObj'))

    def wait_for_state(self, target_state, iter_limit=100):
        state, t = self.manager_proxy.sub_stat(self.sub_id)
        i = 0
        while not self.manager_proxy.has_reached_state(self.sub_id, target_state):
            # raise KeyboardInterrupt()
            if t > 0:
                self.client.logger.debug(
                    'Waiting for remote object to get to {2}.\n\t Current state: {0}\n\t Seconds left: {1}'.format(
                        state, t, target_state))
                sleep(min([t, 30]))
                i = 0
            elif i > iter_limit:
                raise Exception('iter limit reached. no progression.')
            i += 1
            state, t = self.manager_proxy.sub_stat(self.sub_id)
        return state

    def submit(self, no_wait=False):
        self.manager_proxy.sub_start(self.sub_id)
        self.submitted = True
        if not no_wait:
            self.wait_for_state('ready')

    def __enter__(self):
        try:
            self.submit()
            self.get_execution_controller()
            return self.make_obj(self.obj_descriptor)
        except Exception:
            self.close()
            raise

    # noinspection PyBroadException
    def close(self):
        if not self.orphan and self.submitted:
            try:
                self.manager_proxy.sub_shut(self.sub_id)
            except Exception as e:
                self.logger.warning('Error during sub shut: {0}'.format(e.message))
                ex_env = execution_environment()
                Popen(ex_env.client_command_line_prefix.split(' ') + ['-r', 'stop', 'executor',
                                                                      'sub_id={0}'.format(self.sub_id)])

    # noinspection PyUnusedLocal
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        self.__exit__(1, 2, 3)
