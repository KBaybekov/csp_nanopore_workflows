import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.slurm import submit_slurm_job

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
                    fast5s.append(f'{os.path.join(root)}{os.sep}')

    if not fast5s:
        raise FileNotFoundError('FAST5 файлы не найдены!')
    return list(set(fast5s))

def convert_fast5_to_pod5(fast5_dirs:list, sample:str, out_dir:str, threads:str, ntasks:int, exclude_nodes:list=[], working_dir:str=''):
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
    pod5_dir = f'{os.path.join(out_dir, sample)}{os.sep}'
    #print('pod5_dir',pod5_dir)
    for idx, fast5_dir in enumerate(fast5_dirs):
        pod5_name = f"{idx}"
        command = f"pod5 convert fast5 {fast5_dir}*.fast5 --output {pod5_dir}/{pod5_name}.pod5 --threads {threads}"
        job_id = submit_slurm_job(command, partition="cpu_nodes",
                                  job_name=f"pod5_convert_{sample}_{pod5_name}",
                                  nodes=1, ntasks=ntasks, exclude_nodes=exclude_nodes, working_dir=working_dir)
        job_ids.append(job_id)
    return job_ids

def basecalling(sample:str, in_dir:str, out_dir:str, mod_type:str, model:str, dependency:list, working_dir:str='') -> tuple:
    """Запуск бейсколлинга на GPU"""

    pod5_dir = f'{os.path.join(in_dir,sample)}{os.sep}'
    ubam_dir = f'{os.path.join(out_dir,sample)}{os.sep}'
    ubam = f"{ubam_dir}{sample}_{mod_type.replace('_', '-')}.ubam"

    command = f"dorado basecaller --modified-bases {mod_type} {model} {pod5_dir}*.pod5 > {ubam}"
    return (submit_slurm_job(command, partition="gpu_nodes", nodes=1, job_name=f"basecall_{sample}_{mod_type}", dependency=dependency, working_dir=working_dir),
             ubam)

def aligning(sample:str, ubam:str, out_dir:str, mod_type:str, ref:str, threads:str, dependency:list, exclude_nodes:list=[], working_dir:str=''):
    """Запуск выравнивания на CPU нодах"""
    bam_dir = f'{os.path.join(out_dir,sample)}{os.sep}'
    bam = ubam.replace(os.path.dirname(ubam), bam_dir).replace('.ubam', '.bam')
    command = f"nextflow run epi2me-labs/wf-alignment --bam {ubam} --out_dir {bam_dir} --references {ref} --threads {threads}"
    return (submit_slurm_job(command, partition="cpu_nodes", nodes=1,
                            job_name=f"align_{sample}_{mod_type}",
                            dependency=dependency, exclude_nodes=exclude_nodes, working_dir=working_dir),
                             bam)

def modifications_lookup(sample:str, bam:str, out_dir:str, mod_type:str, model:str, ref:str, threads:str, dependency:list, exclude_nodes:list=[], working_dir:str=''):
    """Запуск выравнивания на CPU нодах"""
    
    command = f"nextflow run epi2me-labs/wf-human-variation --bam {bam} --ref {ref} --mod --threads {threads} --out_dir {out_dir} --sample_name {sample} --override_basecaller_cfg {model} --force_strand"
    return submit_slurm_job(command, partition="cpu_nodes", nodes=1,
                            job_name=f"modkit_{sample}_{mod_type}",
                            dependency=dependency, exclude_nodes=exclude_nodes, working_dir=working_dir)

def sv_lookup(sample:str, bam:str, out_dir:str, mod_type:str, tr_bed:str, model:str, ref:str,
              threads:str, dependency:list, dependency_type:str, exclude_nodes:list=[], working_dir:str=''):
    """Запуск выравнивания на CPU нодах"""
    
    command = f"nextflow run epi2me-labs/wf-human-variation --bam {bam} --ref {ref} --snp --cnv --str --sv --phased --tr_bed {tr_bed} --threads {threads} --out_dir {out_dir} --sample_name {sample} --override_basecaller_cfg {model} --force_strand"
    return submit_slurm_job(command, partition="cpu_nodes", nodes=1,
                            job_name=f"sv_{sample}_{mod_type}",
                            dependency=dependency, dependency_type=dependency_type, exclude_nodes=exclude_nodes, working_dir=working_dir)