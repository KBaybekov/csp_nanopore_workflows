#!/usr/bin/env python3

"""
Script searches for sample folders in in_dir, then checks for .fast5 files in fast5_pass subdirectories of sample.
Task queue is created.
After converting all sample's .fast5 files to .pod5 on CPU nodes, basecalling starts on GPU.

Usage: Usage: nanopore_preprocessing.py in_dir pod5_dir out_dir dorado_model threads
"""
import sys
import os
import time
import datetime
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import argparse
from utils.common import get_dirs_in_dir, load_yaml, split_list_in_chunks
from utils.nanopore import aligning, basecalling, modifications_lookup, sv_lookup, convert_fast5_to_pod5, get_fast5_dirs
from utils.slurm import get_slurm_job_status, cancel_slurm_job


def ch_d(d):
    print(d)
    exit()


def parse_cli_args() -> dict:
    """
    Функция для обработки аргументов командной строки
    """
           
    parser = argparse.ArgumentParser(
        description = 'Генерация и загрузка в очередь Slurm заданий по обработке данных Oxford Nanopore от .fast5 до репортов', 
        epilog = '©Kirill Baybekov'
    )
    
    # Основные аргументы с описаниями из YAML
    parser.add_argument('-i', '--input_dir', required=True, type=str, help='директория с папками, содержащими данные Oxford Nanopore')
    parser.add_argument('-o', '--output_dir', required=True, type=str, help='выходная директория')
    parser.add_argument('-t', '--threads_per_machine', required=True, default='', type=str, help='количество потоков на машину')
    parser.add_argument('-m', '--dorado_model', required=True, default='', type=str, help='папка модели dorado')
    parser.add_argument('-mp', '--dorado_models_path', default='/common_share/reference_files/dorado_models/', type=str, help='папка с моделями dorado')
    parser.add_argument('-tmp', '--tmp_dir', required=True, default='', type=str, help='папка для временных файлов')


    # Парсим аргументы
    args = parser.parse_args()
    # Преобразуем Namespace в словарь
    args = vars(args)  # Преобразуем объект Namespace в словарь

    return args


def create_sample_sections_in_dict(target_dict:dict, sample:str, sections:list, val) -> dict:
    target_dict.update({sample:{section:val.copy() for section in sections}})
    return target_dict


def store_job_ids(pending_jobs:dict,job_results:dict, sample:str, stage:str, job_ids:list) -> None:
    #print('pending_jobs', pending_jobs)
    #print('job_results', job_results)
    pending_jobs[sample][stage].extend(job_ids)
    job_results[sample][stage].update({id:'' for id in job_ids})


def generate_job_status_report(pending_jobs:dict, job_results:dict, timestamp:str) -> tuple:
    RED = "\033[31m"
    YELLOW = "\033[33m"
    GREEN = "\033[32m"
    BLUE = "\033[34m"
    WHITE ="\033[0m"
    PURPLE = "\033[35m"
    status_coloring = {'PENDING':YELLOW, 'RUNNING':BLUE, 'COMPLETED':GREEN, 'FAILED':RED, 'REMOVED':PURPLE}

    jobs_data = get_slurm_job_status()
    # check if there is still any pending job
    no_pending_jobs = True
    # check every sample in pending_jobs
    for sample, stages in pending_jobs.items():
        
        # check every  stage in sample
        for stage, jobs in stages.items():
            if jobs:
                no_pending_jobs = False
            for job in jobs:
                # check for job in slurmd
                job_status = jobs_data.get(int(job), 'JOB NOT FOUND')
                # if job is found, check for its status
                if isinstance(job_status, dict):
                    job_state = job_status.get('job_state', 'UNKNOWN_STATE')
                elif isinstance(job_status, str):
                    job_state = job_status
                if job_state == 'RUNNING':
                    node = f", {job_status.get('nodes', 'UNKNOWN_NODE')}"
                elif job_state == 'UNKNOWN_STATE':
                    pending_jobs, job_results = remove_job_from_processing(pending_jobs=pending_jobs, job_results=job_results,
                                                                      sample=sample, stage=stage, job=job, job_state='JOB NOT FOUND')

                # we need only one sv_lookup per sample, so other will be cancelled if job started
                if stage == 'sv_lookup' and job_state == 'RUNNING' and len(jobs) > 1:
                    job_to_cancel = jobs[1] if job == jobs[0] else jobs[0]
                    cancel_slurm_job(job_to_cancel=int(job_to_cancel))
                    pending_jobs, job_results = remove_job_from_processing(pending_jobs=pending_jobs, job_results=job_results,
                                                                      sample=sample, stage=stage, job=job_to_cancel, job_state='REMOVED')

                job_results[sample][stage][job] = job_state
                
                
                if job_state == 'COMPLETED':
                    pending_jobs, job_results = remove_job_from_processing(pending_jobs=pending_jobs, job_results=job_results,
                                                                      sample=sample, stage=stage, job=job, job_state='COMPLETED')

    data2print = [timestamp]
    #check if all jobs are completed (or removed, or unknown)
    no_more_jobs = True
    for sample, stages in job_results.items():
        data2print.append(f'{sample}:')
        for stage, jobs in stages.items():
            stage_data = []
            stage_data.append(f'\t{stage.upper()}: ')
            for job in jobs:
                node = ''
                job_state = job_results[sample][stage][job]

                if job_state in ['RUNNING', 'PENDING']:
                    no_more_jobs = False

                if job_state == 'RUNNING':
                    node = f", {jobs_data[int(job)].get('nodes', 'UNKNOWN_NODE')}"
                status_color = status_coloring.get(job_state, WHITE)
                stage_data.append(f'{job} ({status_color}{job_state}{WHITE}{node})\t')

            data2print.append(''.join(stage_data))
    data2print = f'\n'.join(data2print)

    #remove color marks from data going to txt file and save it
    data2txt = data2print
    for color in status_coloring.values():
        data2txt.replace(color, '')
    os.system(f'echo "{data2txt}" >> {log_file}')

    #print job data
    os.system('clear')
    print(data2print)

    # we stop to print slurm data
    if (no_pending_jobs & no_more_jobs):
        stop_slurm_monitoring = True
    else:
        stop_slurm_monitoring = False

    return (pending_jobs, job_results, stop_slurm_monitoring)

def remove_job_from_processing(pending_jobs:dict, job_results:dict, sample:str, stage:str, job:int, job_state:str) -> tuple:
    pending_jobs[sample][stage].remove(job)
    job_results[sample][stage][job] = job_state

    return (pending_jobs, job_results)


def update_pending_jobs(pending_jobs, job_results):
    # check every sample in pending_jobs
    for sample, stages in pending_jobs.items():
        # check every  stage in sample
        for stage, jobs in stages.items():
            # new list for remaining job_ids
            updated_jobs = []

            # Для каждого job_id в stage проверяем его статус в job_results
            for job_id in jobs:
                # Получаем статус из job_results
                status = job_results.get(sample, {}).get(stage, {}).get(job_id, 'Unknown')

                # Если статус False, пропускаем задачу (удаляем её из pending_jobs)
                if status == False:
                    continue
                
                # Если статус не False, оставляем задачу
                updated_jobs.append(job_id)

            # Обновляем список задач для текущего stage
            stages[stage] = updated_jobs

    return pending_jobs


def main():
    pending_jobs = {}
    job_results = {}

    # create subdirs in dir
    for dir_data in directories.values():
        os.makedirs(dir_data['path'], exist_ok=True) 

    sample_dirs = get_dirs_in_dir(dir=in_dir)
    # Create dict with sample_name:[sample_fast5s_dirs] as key:val
    sample_data = {}
    for s in sample_dirs:
        f5d = get_fast5_dirs(dir=s)
        if f5d:
            sample_data.update({os.path.basename(os.path.normpath(s)):f5d})
    found_samples = "\n\t".join(sample_data.keys())
    print(f'FAST5 data found for samples:\n\t{found_samples}')
    time.sleep(5)
    #print(sample_data)
    # Create list of samples for iteration
    samples = list(sample_data.keys())
    samples.sort()
    #!!for s in ['770720000101', '770720030104']:
    #    samples.remove(s)
    # We will split sample list in 4 chunks for using 4 concurrent gpu processes
    samples_chunks = list(split_list_in_chunks(lst=samples, chunks=concurrent_gpu_processes))
    cudas_idxs = {}
    for idx, lst in enumerate(samples_chunks):
        cudas = f'{6-idx*2},{7-idx*2}'
        for sample in lst:
            cudas_idxs.update({sample:cudas})
    ch_d(cudas_idxs)
    #print(samples)
    # Loop will proceed until we're out of jobs for submitting or samples to process
    while samples or pending_jobs:
        # Choose sample
        if samples:
            sample_job_ids = {}
            for stage in stages:
                sample_job_ids[stage] = []

            #SHIT IN STRING BELOW CREATES [] JUST ONCE, NEXT VALS WILL BE JUST LINKS. IF U CHANGE 1 VAL, U CHANGE ALL   
            #sample_job_ids = dict.fromkeys(stages, [])
            
            #print('sample_job_ids', sample_job_ids)
            # pop sample from initial sample list
            sample = samples.pop(0)
            #print('sample', sample)
            pending_jobs = create_sample_sections_in_dict(target_dict=pending_jobs, sample=sample,
                                                          sections=stages, val=[])
            job_results = create_sample_sections_in_dict(target_dict=job_results, sample=sample,
                                                          sections=stages, val={})
            fast5_dirs = sample_data[sample]
            #print('pending_jobs', pending_jobs, 'job_results', job_results)
            #exit()
            # Pulling converting task, one per job
            #print(sample_job_ids)
            sample_job_ids['converting'] = convert_fast5_to_pod5(fast5_dirs=fast5_dirs, sample=sample,
                                                                      out_dir=directories['pod5_dir']['path'],
                                                                      threads=threads_per_converting,
                                                                      mem=mem_per_converting,
                                                                      exclude_nodes=exclude_node_cpu,
                                                                      working_dir=working_dir)
            
            
            #print("sample_job_ids['converting']", sample_job_ids['converting'])
            # Basecalling, aligning and mod lookup will be performed for each modification type in list          
            for mod_type in mod_bases:
                # basecalling results will be stored in ubam dir of sample.
                #GPU
                #print(sample_job_ids['basecalling'])
                job_id_basecalling, ubam = basecalling(sample=sample,
                                                 in_dir=directories['pod5_dir']['path'],
                                                 out_dir=directories['ubam_dir']['path'],
                                                mod_type=mod_type, model=dorado_model,
                                                mem=mem_per_basecalling, threads=threads_per_basecalling, cudas=cudas_idxs[sample],
                                                working_dir=working_dir,
                                                dependency=sample_job_ids['converting'])
                sample_job_ids['basecalling'].append(job_id_basecalling)
                #print('job_id_basecalling', job_id_basecalling)
                #print(job_id_basecalling, ubam, sample_job_ids['basecalling'])
                
                # Alignment results will be stored in bam dir of sample.
                #CPU
                job_id_aligning, bam = aligning(sample=sample, ubam=ubam, out_dir=directories['other_dir']['path'],
                                           mod_type=mod_type, ref=ref_fasta, threads=threads_per_align, mem=mem_per_align,
                                           dependency=[job_id_basecalling], working_dir=working_dir, exclude_nodes=exclude_node_cpu)
                sample_job_ids['aligning'].append(job_id_aligning)
                #print('job_id_aligning', job_id_aligning)

                # mod lookup results will be stored in common dir of sample.
                #CPU
                sample_job_ids['mod_lookup'].append(modifications_lookup(sample=sample, bam=bam, out_dir=directories['other_dir']['path'],
                                                     mod_type=mod_type, model=dorado_model, ref=ref_fasta, mem=mem_per_calling_mod,
                                                     threads=threads_per_calling_mod, dependency=[job_id_aligning], working_dir=working_dir, exclude_nodes=exclude_node_cpu))

                # SV calling will be performed just once with using of the first ready BAM 
                # SV lookup results will be stored in common dir of sample.
                #CPU
                sample_job_ids['sv_lookup'].append(sv_lookup(sample=sample, bam=bam, out_dir=directories['other_dir']['path'],
                                                        mod_type=mod_type, model=dorado_model, ref=ref_fasta, mem=mem_per_calling_sv,
                                                        tr_bed=ref_tr_bed, threads=threads_per_calling_sv, dependency=[job_id_aligning],
                                                        working_dir=working_dir, exclude_nodes=exclude_node_cpu))
            
            # Sample related job ids will be stored in logging dict
            #print(sample_job_ids)
            #print('job_results', job_results)
            for stage, job_ids in sample_job_ids.items():
                store_job_ids(pending_jobs=pending_jobs, job_results=job_results,
                              sample=sample, stage=stage, job_ids=job_ids)            
            
            #print(job_results)
            #os.system('scancel -u kbajbekov && rm -rf /common_share/tmp/slurm/*')
            #exit()
        # Check pending jobs
        elif pending_jobs:
            now = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
            pending_jobs, job_results, stop_slurm_monitoring = generate_job_status_report(pending_jobs=pending_jobs, job_results=job_results, timestamp=now)

            if stop_slurm_monitoring:
                print('Slurm stage finished. Goodbye!')
                exit()
            else:
                # pause before next check
                time.sleep(27)
        # pause before next check
        time.sleep(3)

    #move sample's files if all tasks are completed
    """    for sample, stages in job_results.items():
            for stage in stages.keys():
                if all(job_state == 'COMPLETED' for job_state in stage.values()):
                    for dir in directories['out_dir'].keys():
                        move_sample_files_2_res_dir(mask=sample, src_dir=directories['out_dir'][dir]['path'], dst_dir=directories['res_dir'][dir]['path'])"""

    print("All samples processed!")
    """dir2search_in = directories['other_dir']['path']
    for d in directories.keys():
        extensions = tuple(directories[d]['extensions'])
        d_path = directories[d]['path']
        files2move = get_samples_in_dir_tree(dir=dir2search_in, extensions=extensions, empty_ok=True)
        for f in files2move:
            shutil.move(src=f, dst=d_path)"""

args = parse_cli_args()

in_dir = f'{os.path.normpath(os.path.join(args["input_dir"]))}{os.sep}'
out_dir = f'{os.path.normpath(os.path.join(args["output_dir"]))}{os.sep}'
log_file = f'{out_dir}log.txt'
#dorado_model = f'{os.path.normpath(os.path.join(args["dorado_model"]))}{os.sep}'
dorado_model = args["dorado_model"]
threads_per_machine = args["threads_per_machine"]
working_dir = f'{os.path.normpath(os.path.join(args["tmp_dir"]))}{os.sep}'
if not os.path.exists(working_dir):
    os.makedirs(working_dir, exist_ok=True)

#copy model to work dir
os.system(f'cp -r {args["dorado_models_path"]}{dorado_model} {working_dir}')

ref_fasta = '/common_share/nanopore_service_files/ref_files/GCA_000001405.15_GRCh38_no_alt_analysis_set.fna'
ref_tr_bed = '/common_share/nanopore_service_files/ref_files/human_GRCh38_no_alt_analysis_set.trf.bed'
mod_bases = ['5mCG_5hmCG', '5mCG']

# we don't want to use dgx10 for this time as CPU node
exclude_node_cpu = ['dgx10']
exclude_node_gpu = []

configs = f"{os.path.dirname(os.path.realpath(__file__).replace('src', 'configs'))}/"

directories = load_yaml(file_path=f'{configs}dir_structure.yaml')
stages = ['converting', 'basecalling', 'aligning', 'sv_lookup', 'mod_lookup']

# generate paths strings for subdirs in out_dir
for d in directories.keys():
    directories[d]['path'] = f'{os.path.join(out_dir, directories[d]["name"])}{os.sep}'

# How many tasks should be run on one machine concurrently 
tasks_per_machine_converting = '16'
tasks_per_machine_aligning = '6'
tasks_per_machine_calling_sv = '8'
tasks_per_machine_calling_mod = '16'

threads_per_basecalling = 64 #T
threads_per_converting = str(min((int(threads_per_machine)//int(tasks_per_machine_converting)), 16)) #T
threads_per_align = str(min((int(threads_per_machine)//int(tasks_per_machine_aligning)), 40)) #T
threads_per_calling_sv = str(min((int(threads_per_machine)//int(tasks_per_machine_calling_sv)), 32))
threads_per_calling_mod = str(min((int(threads_per_machine)//int(tasks_per_machine_calling_mod)), 16))

# How many RAM per task we need
mem_per_converting = 128
mem_per_basecalling = 512
mem_per_align = 32
mem_per_calling_sv = 128
mem_per_calling_mod = 64

#how many concurrent gpu processes we need
concurrent_gpu_processes = 4
"""На один образец (~1,3 Тб) 8 GPU A100 тратят 104 минуты.
   Соответственно, если мы будем обрабатывать сразу 4 образца,
   среднее затраченное время будет ~416 минут 
   (увеличение времени кратно уменьшению количества видеокарт на задачу)"""

# unfinished jobs will be stored there.
# Structure: {sample:{stage1:[job_id_0, job_id_1], stage2:[job_id_2]}} 
pending_jobs = {}
    
# finished jobs will be stored there (logging purposes)
# Structure: {sample:{stage1:{job_id_0 : exit_code, job_id_1 : exit_code}, stage2:{job_id_2 : exit_code}}} 
job_results = {}

if __name__ == "__main__":
    main()