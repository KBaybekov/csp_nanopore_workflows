import pyslurm
import os
from src.utils.common import run_shell_cmd

def submit_slurm_job(command:str, working_dir:str, job_name:str, partition:str='', nodes:int=1,
                     ntasks:int=1, dependency:list=None, dependency_type:str='all',
                     exclude_nodes:list=[]) ->str :
    """Отправка задачи в SLURM
    :param command: команда для CLI
    :param job_name: наименование задачи
    :param partition: на какой части кластера выполняется
    :param nodes: количество машин для задачи
    :param ntasks: количество задач на задание
    :param dependency: задачи, по успешному завершению которых будет запущено задание
    :param dependency_type: тип зависимости от задач - должны быть успешно выполнены все либо любая из задач ('all','any')
    :return: id задачи Slurm
    """
    if not command:
        raise ValueError('Empty CMD for sbatch script')
    elif not working_dir:
        raise ValueError('Work dir not specified')
    elif not job_name:
        raise ValueError('Job name not specified')

    slurm_script = ['#!/bin/bash\n']
    slurm_script_file = os.path.join(working_dir, f'{job_name}.sh')
    option_str = '#SBATCH --{}={}'

    opts = {'job-name':job_name,
            'partition':partition,
            'nodes':nodes,
            'ntasks':ntasks,
            'dependency':dependency,
            'exclude':exclude_nodes,
            'chdir':working_dir,
            'command':command}    

    for opt,val in opts.items():
        if opt != 'command':
            if val:
                if opt == 'dependency':
                    if dependency_type == 'all':
                        delimiter = ':'
                    elif dependency_type == 'any':
                        delimiter = '?'
                    val = f"afterok:{f'{delimiter}'.join(dependency)}"
                if opt == 'exclude':
                    val = ','.join(exclude_nodes)
                
                slurm_script.append(option_str.format(opt, val))
        else:
            slurm_script.extend(['\n', val])

    with open(slurm_script_file, 'w') as s:
        s.write('\n'.join(slurm_script))

    os.system(f'sbatch {slurm_script_file}')
    job_id, stderr = run_shell_cmd(cmd=f"squeue -n {job_name} | tail -1| awk '{{print $1}}'")
    return job_id.removesuffix('\n')

def get_slurm_job_status(job_id:str):
    """Проверка статуса задачи через pyslurm"""
    job_info = pyslurm.job().find_id(job_id)
    if job_info:
        state = job_info.get('job_state', 'UNKNOWN')
        #print(f"Job {job_id} state: {state}")
        return state
    return 'JOB NOT FOUND'

def get_idle_nodes(partition_name:str) -> list:
    """Получение списка простаивающих узлов"""
    nodes = pyslurm.node().get()
    idle_nodes = [node for node, data in nodes.items() if data['state'] == 'IDLE' and partition_name in data['partitions']]
    return idle_nodes