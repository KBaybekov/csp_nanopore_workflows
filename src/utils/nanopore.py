import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.slurm import submit_slurm_job

dorado_bin = '/home/PAK-CSPMZ/kbajbekov/programms/dorado-0.8.3-linux-x64/bin/dorado'


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

    return list(set(fast5s))

def convert_fast5_to_pod5(fast5_dirs:list, sample:str, out_dir:str, threads:str, mem:int, exclude_nodes:list=[], working_dir:str='') ->list :
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
    
    for fast5_dir in fast5_dirs:
        #print('fast5_dir',fast5_dir)
        # pod5 will be named as parent dir for fast5 files
        pod5_name = f'{sample}_{os.path.basename(os.path.dirname(os.path.normpath(fast5_dir)))}'

        command = f"pod5 convert fast5 {fast5_dir}*.fast5 --output {pod5_dir}/{pod5_name}.pod5 --threads {threads}"
        job_id = submit_slurm_job(command, partition="cpu_nodes",
                                  job_name=f"pod5_convert_{sample}_{pod5_name}",
                                  nodes=1, cpus_per_task=threads, mem=mem, exclude_nodes=exclude_nodes, working_dir=working_dir)
        job_ids.append(job_id)
    return job_ids

def basecalling(sample:str, in_dir:str, out_dir:str, mod_type:str, model:str, mem:int, threads:int, dependency:list, working_dir:str='') -> tuple:
    """Запуск бейсколлинга на GPU"""

    pod5_dir = f'{os.path.join(in_dir,sample)}{os.sep}'
    ubam_dir = f'{os.path.join(out_dir,sample)}{os.sep}'
    os.makedirs(name=ubam_dir, exist_ok=True)
    ubam = f"{ubam_dir}{sample}_{mod_type.replace('_', '-')}.ubam"

    command = f"{dorado_bin} basecaller {model} {pod5_dir} --modified-bases {mod_type} > {ubam}"
    return (submit_slurm_job(command, partition="gpu_nodes", nodes=1, job_name=f"basecall_{sample}_{mod_type}", mem=mem, cpus_per_task=threads, dependency=dependency, working_dir=working_dir),
             ubam)

def aligning(sample:str, ubam:str, out_dir:str, mod_type:str, ref:str, threads:str, mem:int, dependency:list, exclude_nodes:list=[], working_dir:str=''):
    """Запуск выравнивания на CPU нодах"""
    bam_dir = f'{os.path.join(out_dir,sample,mod_type)}{os.sep}'
    bam = ubam.replace(os.path.dirname(ubam), bam_dir).replace('.ubam', '.sorted.aligned.bam')
    command = f"nextflow run epi2me-labs/wf-alignment --bam {ubam} --out_dir {bam_dir} --references {ref} --threads {threads}"
    return (submit_slurm_job(command, partition="cpu_nodes", nodes=1, cpus_per_task=threads,
                            job_name=f"align_{sample}_{mod_type}", mem=mem,
                            dependency=dependency, exclude_nodes=exclude_nodes, working_dir=working_dir),
                             bam)

def modifications_lookup(sample:str, bam:str, out_dir:str, mod_type:str, model:str, ref:str, threads:str, mem:int, dependency:list, exclude_nodes:list=[], working_dir:str=''):
    """Запуск выравнивания на CPU нодах"""
    
    command = f"nextflow run epi2me-labs/wf-human-variation --bam {bam} --ref {ref} --mod --threads {threads} --out_dir {out_dir} --sample_name {sample}_ --override_basecaller_cfg {model} --force_strand"
    return submit_slurm_job(command, partition="cpu_nodes", nodes=1, cpus_per_task=threads,
                            job_name=f"modkit_{sample}_{mod_type}", mem=mem,
                            dependency=dependency, exclude_nodes=exclude_nodes, working_dir=working_dir)

def sv_lookup(sample:str, bam:str, out_dir:str, mod_type:str, tr_bed:str, model:str, ref:str, mem:int,
              threads:str, dependency:list, exclude_nodes:list=[], working_dir:str=''):
    """Запуск выравнивания на CPU нодах"""
    
    command = f"nextflow run epi2me-labs/wf-human-variation --bam {bam} --ref {ref} --snp --cnv --str --sv --phased --tr_bed {tr_bed} --threads {threads} --out_dir {out_dir} --sample_name {sample}_ --override_basecaller_cfg {model} --force_strand"
    return submit_slurm_job(command, partition="cpu_nodes", nodes=1, cpus_per_task=threads,
                            job_name=f"sv_{sample}_{mod_type}", mem=mem,
                            dependency=dependency, exclude_nodes=exclude_nodes, working_dir=working_dir)