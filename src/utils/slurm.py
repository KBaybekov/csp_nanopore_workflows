import pyslurm

def submit_slurm_job(command:str, job_name:str, partition:str, nodes:int=1,
                     ntasks:int=1, dependency:list=None, dependency_type:str='all',
                     exclude_nodes:list=[]):
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
    job = {
        "job_name": job_name,
        "partition": partition,
        "command": command,
        "ntasks": ntasks,
        "nodes": nodes,
        "output": f"/tmp/{job_name}_%j.out"
    }
    if exclude_nodes:
        job["exclude"] = f"{','.join(exclude_nodes)}"
    if dependency:
        if dependency_type == 'all':
            delimiter = ':'
        elif dependency_type == 'any':
            delimiter = '?'
        job["dependency"] = f"afterok:{f'{delimiter}'.join(map(str, dependency))}"

    job_id = pyslurm.job().submit_batch_job(job)
    print(f"Job {job_name} submitted with ID {job_id}")
    return job_id

def is_slurm_job_running(job_id:str):
    """Проверка статуса задачи через pyslurm"""
    job_info = pyslurm.job().find_id(job_id)
    if job_info:
        state = job_info.get('job_state', 'UNKNOWN')
        print(f"Job {job_id} state: {state}")
        return state in ['RUNNING', 'PENDING']
    return False

def get_idle_nodes(partition_name:str) -> list:
    """Получение списка простаивающих узлов"""
    nodes = pyslurm.node().get()
    idle_nodes = [node for node, data in nodes.items() if data['state'] == 'IDLE' and partition_name in data['partitions']]
    return idle_nodes