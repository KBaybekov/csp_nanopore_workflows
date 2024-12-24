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
from utils.common import get_dirs_in_dir, load_yaml, get_samples_in_dir_tree
from utils.nanopore import aligning, basecalling, modifications_lookup, sv_lookup, convert_fast5_to_pod5, get_fast5_dirs
from utils.slurm import get_slurm_job_status


def ch_d(d):
    print(d)
    exit()

def create_sample_sections_in_dict(target_dict:dict, sample:str, sections:list, dict_type:str) -> dict:
    target_dict.update({sample:{}})
    if dict_type == 'job_listing':
        target_dict[sample] = target_dict[sample].fromkeys(sections, [])
    elif dict_type == 'job_logging':
        target_dict[sample] = target_dict[sample].fromkeys(sections, {})
    #print()
    return target_dict

def store_job_ids(sample:str, stage:str, job_ids:list) -> None:
    pending_jobs[sample][stage].extend(job_ids)
    job_results[sample][stage] = dict.fromkeys(job_ids, '')

def generate_job_status_report(pending_jobs:dict, job_results:dict, timestamp:str) -> tuple:
    print(timestamp)
    # check every sample in pending_jobs
    for sample, stages in pending_jobs.items():
        data2print = []
        # check every  stage in sample
        for stage, jobs in stages.items():
            for job in jobs:
                job_status = get_slurm_job_status(job_id=job)
                job_results[sample][stage][job] = job_status
                data2print.append(f'{job} ({job_status})')
                if job_status == 'COMPLETED':
                    pending_jobs[sample][stage].remove(job)
            print(f'{stage.upper()}[{", ".join(data2print)}]', end='\t')
        print()

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
    sample_data = {os.path.basename(os.path.normpath(s)):get_fast5_dirs(dir=s) for s in sample_dirs}
    #print(sample_data)
    # Create list of samples for iteration
    samples = list(sample_data.keys())
    samples.sort()
    #print(samples)
    # Loop will proceed until we're out of jobs for submitting or samples to process
    while samples or pending_jobs:
        # Choose sample
        if samples:
            sample_job_ids = dict.fromkeys(stages, [])
            #print('sample_job_ids', sample_job_ids)
            # pop sample from initial sample list
            sample = samples.pop(0)
            #print('sample', sample)
            pending_jobs = create_sample_sections_in_dict(target_dict=pending_jobs, sample=sample,
                                                          sections=stages, dict_type='job_listing')
            job_results = create_sample_sections_in_dict(target_dict=job_results, sample=sample,
                                                          sections=stages, dict_type='job_logging')
            fast5_dirs = sample_data[sample]
            #print('pending_jobs', pending_jobs, 'job_results', job_results, 'fast5_dirs', fast5_dirs, )
            #exit()
            # Pulling converting task, one per job
            sample_job_ids['converting'].extend(convert_fast5_to_pod5(fast5_dirs=fast5_dirs, sample=sample,
                                                                      out_dir=directories['pod5_dir']['path'],
                                                                      threads=threads_per_converting,
                                                                      ntasks=tasks_per_machine_converting,
                                                                      exclude_nodes=exclude_node_cpu,
                                                                      working_dir=working_dir))
            #print("sample_job_ids['converting']", sample_job_ids['converting'])
            # Basecalling, aligning and mod lookup will be performed for each modification type in list          
            for mod_type in mod_bases:
                # basecalling results will be stored in ubam dir of sample.
                #GPU
                print(sample_job_ids['basecalling'])
                job_id_basecalling, ubam = basecalling(sample=sample,
                                                 in_dir=directories['pod5_dir']['path'],
                                                 out_dir=directories['ubam_dir']['path'],
                                                mod_type=mod_type, model=dorado_model,
                                                working_dir=working_dir,
                                                dependency=sample_job_ids['converting'])
                sample_job_ids['basecalling'].append(job_id_basecalling)
                print(job_id_basecalling, ubam, sample_job_ids['basecalling'])
                
                # Alignment results will be stored in bam dir of sample.
                #CPU
                job_id_aligning, bam = aligning(sample=sample, ubam=ubam, out_dir=directories['bam_dir']['path'],
                                           mod_type=mod_type, ref=dorado_model, threads=threads_per_align,
                                           dependency=[job_id_basecalling], working_dir=working_dir, exclude_nodes=exclude_node_cpu)
                sample_job_ids['aligning'].append(job_id_aligning)

                # mod lookup results will be stored in common dir of sample.
                #CPU
                sample_job_ids['mod_lookup'].append(modifications_lookup(sample=sample, bam=bam, out_dir=directories['other_dir']['path'],
                                                     mod_type=mod_type, model=os.path.basename(dorado_model), ref=ref_fasta,
                                                     threads=threads_per_calling_mod, dependency=[job_id_aligning], working_dir=working_dir,
                                                     exclude_nodes=exclude_node_cpu))
            
            # SV calling will be performed just once with using of the first ready BAM 
            # SV lookup results will be stored in common dir of sample.
            #CPU
            sample_job_ids['sv_lookup'].append(sv_lookup(sample=sample, bam=bam, out_dir=directories['other_dir']['path'],
                                                     mod_type=mod_type, model=os.path.basename(dorado_model), ref=ref_fasta,
                                                     tr_bed=ref_tr_bed, threads=threads_per_calling_sv, dependency=sample_job_ids['aligning'],
                                                     dependency_type='any', working_dir=working_dir, exclude_nodes=exclude_node_cpu))
            
            # Sample related job ids will be stored in logging dict
            print(sample_job_ids)
            exit()
            for stage, job_ids in sample_job_ids.items():
                store_job_ids(sample=sample, stage=stage, job_ids=job_ids)            
        
        # Check pending jobs
        elif pending_jobs:
            now = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
            pending_jobs, job_results = generate_job_status_report(pending_jobs=pending_jobs, job_results=job_results, timestamp=now)

        # pause before next check
        time.sleep(10)

    print("All samples processed. Moving files to their folders...")
    dir2search_in = directories['other_dir']['path']
    for d in directories.keys():
        extensions = tuple(directories[d]['extensions'])
        d_path = directories[d]['path']
        files2move = get_samples_in_dir_tree(dir=dir2search_in, extensions=extensions, empty_ok=True)
        for f in files2move:
            os.system(f'mv {f} {d_path}')

if len(sys.argv) < 5:
    raise SyntaxError(print('Usage: human_variation.py in_dir out_dir dorado_model threads'))

in_dir = f'{os.path.normpath(os.path.join(sys.argv[1]))}{os.sep}'
out_dir = f'{os.path.normpath(os.path.join(sys.argv[2]))}{os.sep}'
dorado_model = f'{os.path.normpath(os.path.join(sys.argv[3]))}{os.sep}'
threads_per_machine = sys.argv[4]

ref_fasta = '/common_share/nanopore_service_files/ref_files/GCA_000001405.15_GRCh38_no_alt_analysis_set.fna'
ref_tr_bed = '/common_share/nanopore_service_files/ref_files/human_GRCh38_no_alt_analysis_set.trf.bed'
mod_bases = ['5mCG_5hmCG', '5mCG']

# we don't want to use dgx10 for this time as CPU node
exclude_node_cpu = ['dgx10']
exclude_node_gpu = []

# directory for running slurm jobs
working_dir = '/common_share/tmp/slurm/'

configs = f"{os.path.dirname(os.path.realpath(__file__).replace('src', 'configs'))}/"

directories = load_yaml(file_path=f'{configs}dir_structure.yaml')
stages = ['converting', 'basecalling', 'aligning', 'sv_lookup', 'mod_lookup']

# generate paths strings for subdirs in out_dir
for d in directories.keys():
    directories[d]['path'] = f'{os.path.join(out_dir, directories[d]["name"])}{os.sep}'

# How many tasks should be run on one machine concurrently 
tasks_per_machine_converting = '16'
tasks_per_machine_aligning = '4'
tasks_per_machine_calling_sv = '8'
tasks_per_machine_calling_mod = '32'

# How many threads per task we need
threads_per_converting = str(min((int(threads_per_machine)//int(tasks_per_machine_converting)), 16))
threads_per_align = str(min((int(threads_per_machine)//int(tasks_per_machine_aligning)), 64))
threads_per_calling_sv = str(min((int(threads_per_machine)//int(tasks_per_machine_calling_sv)), 32))
threads_per_calling_mod = str(min((int(threads_per_machine)//int(tasks_per_machine_calling_mod)), 8))

# unfinished jobs will be stored there.
# Structure: {sample:{stage1:[job_id_0, job_id_1], stage2:[job_id_2]}} 
pending_jobs = {}
    
# finished jobs will be stored there (logging purposes)
# Structure: {sample:{stage1:{job_id_0 : exit_code, job_id_1 : exit_code}, stage2:{job_id_2 : exit_code}}} 
job_results = {}

if __name__ == "__main__":
    main()