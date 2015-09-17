__author__ = 'emil'
__author__ = 'emil'
from remoteexecution.Environments import (EnvironmentFactory, TethysCommunication,
                                          PopenExecution, SerializingEnvironment, set_default, communication_environment)
from remoteexecution.ClientSide import Client
from remoteexecution.Utils import SSHPopen

import numpy as np


set_default()
EnvironmentFactory.set_environments(communication=TethysCommunication, execution=PopenExecution)
EnvironmentFactory.set_settings(ssh_client2manager={'ssh_address_or_host': 'thinlinc.compute.dtu.dk',
                                                    'ssh_port': 22,
                                                    'ssh_username': 's082768',
                                                    'ssh_private_key': '/home/emil/.ssh/id_rsa'},
                                ssh_manager2executor={'ssh_address_or_host': 'tethys',
                                                      'ssh_port': 22,
                                                      'ssh_username': 's082768',
                                                      'ssh_private_key': '/home/s082768/.ssh/id_dsa'},
                                ssh_executor2manager={'ssh_address_or_host': 'linuxterm1',
                                                      'ssh_port': 22,
                                                      'ssh_username': 's082768',
                                                      'ssh_private_key': '/home/s082768/.ssh/id_rsa'},
                                manager_port=60000,
                                manager_work_dir='/home/s082768/remoteexec',
                                executor_work_dir='/home/s082768/remoteexec')
try:
    if 0:
        comm_env = communication_environment()
        p = SSHPopen(['echo', 'hi'], ssh_settings=comm_env.client2manager_ssh_settings, stdout=True)
        print p.stdout.read()

    if 1:
        client = Client()
        #client.restart_manager()
        inst_gen = client.instance_generator(object_descriptor=np.ones)
        with inst_gen([10,10]) as arr:
            print arr
finally:
    client.stop_manager()