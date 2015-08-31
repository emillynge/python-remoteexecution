__author__ = 'emil'
from .Builders import Director, HPCClientBuilder, HPCManagerBuilder
from .Utils import create_logger

def build():
    WORKDIR = "/zhome/25/2/51526/Speciale/rnn-speciale"
    LOGDIR = "/zhome/25/2/51526/Speciale/rnn-speciale/qsubs/logs"

    director = Director("RemoteExecution.DefaultBuild:build")
    client_builder = HPCClientBuilder('s082768', 'hpc-fe1.gbar.dtu.dk', "/home/emil/.ssh/id_rsa")
    manager_builder = HPCManagerBuilder(WORKDIR, LOGDIR)
    director.set_logger(create_logger(logger_name='Default'))
    director.set_manager_builder(manager_builder)
    director.set_client_builder(client_builder)
    return director

