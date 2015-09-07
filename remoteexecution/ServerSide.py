from __future__ import (absolute_import, print_function, unicode_literals, division)
import os

__author__ = 'emil'
from .Environments import (communication_environment, execution_environment )
from .Utils import (DummyLogger, RemoteExecutionLogger, WrappedDaemon, WrappedProxy, WrappedObject,
                    InvalidUserInput, import_obj_from, EnvironmentCallMixin, Timer)
import re
from collections import namedtuple, defaultdict
from time import sleep
from subprocess import Popen, PIPE

class Manager(EnvironmentCallMixin, object):
    def __init__(self, logger=None):
        self._state2num = {'requested': 0,
                           'staged': 1,
                           'submitted': 1.5,
                           'queued': 2,
                           'running': 3,
                           'init': 3.5,
                           'ready': 4,
                           'shutdown': 4.5,
                           'completed': 5,
                           'orphaned': 6}
        self.logger = logger.duplicate(logger_name='Manager') if logger else DummyLogger()
        self.running = False
        self.latest_sub_id = -1
        self.subs = defaultdict(dict)
        comm_env = communication_environment()
        ex_env = execution_environment()
        self.ip = comm_env.my_ip
        self.log_dir = ex_env.manager_work_dir + '/manlogs/'
        self.work_dir = ex_env.manager_work_dir
        if not os.path.isdir(self.work_dir + os.sep + 'subs'):
            os.mkdir(self.work_dir + os.sep + 'subs')

        self.running = True
        self.pid = os.getpid()
        self.logger.info("Manager started on {0} with pid {1}".format( self.ip, self.pid))

    def read_file(self, file_name):
        with open(file_name, 'r') as fp:
            return fp.read()

    def get_controller(self, sub_id):
        if not self.in_state(sub_id, 'ready'):
            Exception('This submission is not in "ready" state. No controller available')
        return WrappedProxy('PYRO:qsub.execution.controller@{ip}:{port}'.format(**self.subs[sub_id]['proxy_info']))

    def sub_shut(self, sub_id):
        """
        Shutdown a submission
        :param sub_id: int - submission id
        :return: int - 0 for successful, 1 for forced
        """
        if not self.has_reached_state(sub_id, 'submitted'):
            self.subs['state'] = 'completed'
            self.logger.info('Trashing staged submit {0}'.format(sub_id))
            return 0

        if self.in_state(sub_id, 'queued'):
            self.logger.info('dequeueing submit {0}'.format(sub_id))
            self.sub_del(sub_id)
            return 0    # Killed because it was still in queue

        if not self.has_reached_state(sub_id, 'shutdown'):  # past queue, but before shutdown
            self.logger.debug('Trying to shutdown submit {0} gracefully'.format(sub_id))

            if not self.wait_for_state(sub_id, 'ready', 2.0):    # if past queue executor must either be ready or becoming ready
                self.logger.info('submit {0} stuck in {1}'.format(sub_id, self.get_state(sub_id)))
                self.sub_del(sub_id)
                return 1

            controller = WrappedProxy('remote_execution.executor.controller',
                                      self.subs[sub_id]['proxy_info']['host'],
                                      self.subs[sub_id]['proxy_info']['port'],
                                      logger=self.logger.duplicate(append_name='Exec'))

            controller.shutdown()   #   putting submit into shutdown state
            self.subs[sub_id]['state'] = 'shutdown'

        if not self.wait_for_state(sub_id, 'completed', 5.5):   #   after shutdown. Wait for complete
            self.logger.info('submit {0} stuck in {1}'.format(sub_id, self.get_state(sub_id)))
            self.sub_del(sub_id)
            return 1

        self.logger.info('graceful shutdown of submit {0} successful'.format(sub_id))
        return 0

    def sub_del(self, sub_id):
        """
        Force shutdown of submission. Usually it is preferred to call sub_shut instead
        :param sub_id: int - submission id
        :return: None
        """
        self.logger.info('Trying to remove submit {0}'.format(sub_id))
        if sub_id in self.subs:
            if 'job_id' in self.subs[sub_id]:
                ex_env = execution_environment()
                (stdout, stderr) = ex_env.job_del(self.subs[sub_id]['job_id'])
                self.logger.debug('Removing job {0} from queue: {1}'.format(sub_id, (stdout, stderr)))
                self.subs[sub_id]['state'] = 'completed'
                return
            self.logger.debug('job_id {0} not in subs'.format(self.subs[sub_id]))
            return
        self.logger.debug('sub_id not in qsubs')
        return

    def sub_id_request(self):
        """
        Request a new submission id.
        :return:
            sub_id - int
            logfile - str
        """
        self.latest_sub_id += 1
        self.subs[self.latest_sub_id]['state'] = 'requested'
        self.logger.info("Submission id: {0} granted".format(self.latest_sub_id))
        self.sub_log_clean(self.latest_sub_id)
        return self.latest_sub_id, self.logfile(self.latest_sub_id)

    def sub_log_clean(self, sub_id):
        """
        Remove old submission logs and scripts with sub_id
        :param sub_id: int - submission id
        :return: None
        """
        cmd = 'rm -f ' + self.logfile(sub_id) + '.*'
        p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
        self.logger.debug('removing logfiles with command {1}:\n\t{0}'.format(p.communicate(),
                                                                              cmd))
        Popen(['rm', '-f', self.subid2sh(sub_id)])

    def sub_stage(self, sub_id, script):
        """
        Prepare submission for execution
        :param sub_id:
        :param script:
        :return:
        """
        with open(self.subid2sh(sub_id), 'w') as fp:
            fp.write(script)
        self.subs[sub_id]['state'] = 'staged'

    def sub_start(self, sub_id):
        if self.has_reached_state(sub_id, 'submitted'):
            raise Exception('Cannot submit twice')
        ex_env = execution_environment()
        job_id = str(ex_env.job_start(self.subid2sh(sub_id)))
        self.logger.debug('Started job'.format(job_id))
        self.subs[sub_id]['job_id'] = job_id
        self.subs[sub_id]['state'] = 'submitted'
        self.logger.info('starting submit {0}'.format(sub_id))
        return self.sub_stat(sub_id)

    def set_proxy_info(self, sub_id, daemon_ip, daemon_port):
        self.subs[sub_id]['proxy_info'] = {'host': daemon_ip, 'port': int(daemon_port)}
        self.subs[sub_id]['state'] = 'ready'
        self.logger.info('proxy info on submit {0} recieved: {host}:{port}'.format(sub_id,
                                                                                    **self.subs[sub_id]['proxy_info']))

    def get_proxy_info(self, sub_id):
        if self.has_reached_state(sub_id, 'ready'):
            return self.subs[sub_id]['proxy_info']
        else:
            return None

    def state_num(self, sub_id):
        if sub_id not in self.subs:
            return -1
        return self._state2num[self.subs[sub_id]['state']]

    def has_reached_state(self, sub_id, state):
        if state not in self._state2num:
            InvalidUserInput('', 'state', expected=self._state2num.keys(), found=state, should='must be one of')
        return self.state_num(sub_id) >= self._state2num[state]

    def in_state(self, sub_id, state):
        return self.state_num(sub_id) == self._state2num[state]

    def get_state(self, sub_id):
        if self.has_reached_state(sub_id, 'requested'):
            return self.subs[sub_id]['state']

    def wait_for_state(self, sub_id, state, timeout, sleep_interval=.2):
        timer = Timer(timeout, sleep_interval)
        while True:
            self.sub_stat(sub_id)
            if self.has_reached_state(sub_id, state):
                return True
            if not timer.sleep():   # evaluates to true if timer is timed out
                return False

    @staticmethod
    def time_str2time_sec(t_str):
        time_tup = re.findall('(\d+):(\d+):(\d+)', t_str)
        if time_tup:
            return int(time_tup[0][0]) * 60 * 60 + int(time_tup[0][1]) * 60 + int(time_tup[0][0])
        return -1

    def sub_stat(self, sub_id):
        time_sec = -1
        if not self.has_reached_state(sub_id, 'submitted'):
            return None, time_sec

        ex_env = execution_environment()

        state, time_str = ex_env.job_stat(self.subs[sub_id]['job_id'])
        time_sec = self.time_str2time_sec(time_str)

        if state == 'Q':
            self.subs[sub_id]['state'] = 'queued'

        elif state == 'R':
            if not self.has_reached_state(sub_id, 'running'):
                self.subs[sub_id]['state'] = 'running'
        elif state == 'C':
            self.subs[sub_id]['state'] = 'completed'

        return self.subs[sub_id]['state'], time_sec

    @staticmethod
    def subid2sh(sub_id):
        return 'subs/{0}.sh'.format(sub_id)

    def logfile(self, sub_id):
        return '{0}/{1}'.format(self.log_dir, sub_id)

    def error_log(self, sub_id):
        return self.logfile(sub_id) + '.e'

    def out_log(self, sub_id):
        return self.logfile(sub_id) + '.o'

    def is_alive(self):
        return self.running

    def shutdown(self):
        try:
            self.logger.info('shutting down manager')
            self.shutdown_all_subs()
        finally:
            self.running = False

    def shutdown_all_subs(self):
        self.logger.info('cleaning up submissions')
        for sub_id, sub_info in self.subs.items():
            if not self.has_reached_state(sub_id, 'shutdown'):
                self.sub_shut(sub_id)
        self.logger.info('cleanup done')

    def __del__(self):
        self.shutdown_all_subs()

class DummyPrototype(object):
    def __init__(self, *args, **kwargs):
        pass


class ExecutionController(EnvironmentCallMixin, object):
    def __init__(self, sub_id, logger=None):
        comm_env = communication_environment()
        self.local_ip = comm_env.my_ip
        self.sub_id = None
        self.manager = comm_env.executor2manager_proxy
        if self.manager.is_alive():
            logger.info("manager found!")
        else:
            logger.error('no manager found')
            raise Exception('no manager found')

        self.prototype = DummyPrototype
        self.n_obj = 0
        self.running = False
        self.wrapped_objects = list()
        self.sub_id = int(sub_id)
        self.logger = logger or RemoteExecutionLogger(logger_name="Executor", log_to_file=[])
        self.running = True
        self.daemon = WrappedDaemon(host=self.local_ip)
        self.port = self.daemon.locationStr.split(':')[-1]
        self.daemon.register(WrappedObject(self, logger=self.logger), 'remote_execution.executor.controller')
        self.manager.set_proxy_info(sub_id, self.local_ip, self.port)
        self.daemon.requestLoop(self.is_alive)

    def register_new_object(self, *args, **kwargs):
        wrapped_obj = self.get_wrapped_object(*args, **kwargs)
        object_id = "qsub.execution.{0}".format(self.n_obj)
        proxy_name = "PYRO:{0}@{1}:{2}".format(object_id, self.local_ip, self.port)
        self.wrapped_objects.append((args, kwargs, wrapped_obj, object_id, proxy_name))
        self.n_obj += 1
        self.daemon.register(wrapped_obj, objectId=object_id)
        return {'object_id': object_id}

    def get_wrapped_object(self, *args, **kwargs):
        return WrappedObject(self.prototype(*args, **kwargs), logger=self.logger)

    def set_prototype(self, cls=None, module=None):
        if cls or module:
            if not all([cls, module]):
                InvalidUserInput('module and cls must be provided together', argnames=('cls', 'module'),
                                 found=(cls, module))
            self.prototype = import_obj_from(module, cls)

    def is_alive(self):
        if self.running:
            self.logger.debug('alive')
            return True
        else:
            self.logger.debug('dead')
            return False

    def shutdown(self):
        self.logger.info("shutdown signal received")
        self.running = False