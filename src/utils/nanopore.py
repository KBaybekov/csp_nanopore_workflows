import os
from slurm import submit_slurm_job

def get_fast5_dirs(dir:str) -> list:
    """
    Генерирует список подпапок, содержащих fast5 и именованных 'fast5_pass'.
    Выдаёт ошибку, если итоговый список пустой.

    :param dir: Директория, где искать файлы.
    :return: Список папок.
    """
    subdirs = [os.path.join(dir, s) for s in os.listdir(dir) if os.path.isdir(os.path.join(dir, s, os.sep))]
    fast5s = []
    for subdir in subdirs:
        
        for root, _ds, fs in os.walk(subdir):
            for f in fs:
                if f.endswith('.fast5') and os.path.basename(root) == 'fast5_pass':
                    fast5s.append(os.path.join(root))

    if not fast5s:
        raise FileNotFoundError('FAST5 файлы не найдены!')
    return list(set(fast5s))

def convert_fast5_to_pod5(fast5_dirs:list, sample:str, out_dir:str, threads:str, ntasks:int):
    """
    Запуск задачи конвертации fast5 -> pod5 на CPU. Задача выполняется на одной ЦПУ ноде
    :param fast5_dirs: папки с файлами для конвертации
    :param sample: наименование образца
    :param out_dir: папка для результатов
    :param threads: количество потоков на задачу
    :param ntasks: количество задач на машину
    :return: список id задач Slurm для образца
    """
    job_ids = []
    for idx, fast5_dir in enumerate(fast5_dirs):
        pod5_name = f"{idx}"
        command = f"pod5 convert fast5 {fast5_dir}*.fast5 --output {out_dir}{sample}/{pod5_name}.pod5 --threads {threads}"
        job_id = submit_slurm_job(command, partition="cpu_nodes",
                                  job_name=f"pod5_convert_{sample}_{pod5_name}",
                                  nodes=1, ntasks=ntasks)
        job_ids.append(job_id)
    return job_ids

def basecalling(sample:str, pod5_dir:str, ubam_dir:str, mod_bases:str, model:str, dependency:list):
    """Запуск бейсколлинга на GPU"""

    sample_pod5_dir = f'{os.path.join(pod5_dir,sample)}{os.sep}'

    command = f"dorado basecaller --modified-bases {mod_bases} {model} {sample_pod5_dir}*.pod5 > {sample_ubam_dir}{sample}_{mod_bases.replace('_', '-')}.ubam"
    return submit_slurm_job(command, partition="gpu_nodes", nodes=1, job_name=f"basecall_{sample}", dependency=dependency)

def aligning(sample:str, pod5_dir:str, ubam_dir:str, mod_bases:str, model:str, dependency:list):
    """Запуск выравнивания на CPU нодах"""


    command = f"nextflow run epi2me-labs/wf-alignment --bam /common_share/source_files/ubam/mod/5mCG/770720000101_5mCG.ubam --out_dir /common_share/demo/full/ --prefix 16_ --references /common_share/nanopore_service_files/ref_files/GCA_000001405.15_GRCh38_no_alt_analysis_set.fna --threads 16"
    return submit_slurm_job(command, partition="gpu_nodes", nodes=1, job_name=f"basecall_{sample}", dependency=dependency)