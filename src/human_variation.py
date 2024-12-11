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
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.common import get_dirs_in_dir, load_yaml
from utils.nanopore import aligning, basecalling, convert_fast5_to_pod5, get_fast5_dirs
from utils.slurm import is_slurm_job_running


def ch_d(d):
    print(d)
    exit()

def main(in_dir:str, model:str):
    # create subdirs in dir
    for dir_data in directories_data.values():
        os.makedirs(dir_data['path'], exist_ok=True) 

    sample_dirs = get_dirs_in_dir(dir=in_dir)
    # Create dict with sample_name:[sample_fast5s_dirs] as key:val
    sample_data = {os.path.basename(os.path.normpath(s)):get_fast5_dirs(dir=s) for s in sample_dirs}
    # Create list of samples for iteration
    samples = list(sample_data.keys())
    samples.sort()
        
    # Create list for slurm jobs (each for one type of jobs)
    pending_jobs = {}
    pending_basecalling_jobs = {}
    
    # create dict for saving of job results
    job_results = {}

    # Loop will proceed until we're out of jobs for submitting or samples to process
    while samples or pending_jobs or pending_basecalling_jobs:
        # Choose sample
        if samples:
            sample = samples.pop(0)
            pending_jobs[sample] = []
            # 
            job_results[sample] = {}
            fast5_dirs = sample_data[sample]
            
            # Start converting to pod5
            job_results[sample]['converting'] = {}
            job_ids_converting = convert_fast5_to_pod5(fast5_dirs=fast5_dirs, sample=sample,
                                                       out_dir=out_dir, threads=threads_per_converting, ntasks=tasks_per_machine_converting)
            pending_jobs[sample].extend(job_ids_converting)
# НАДО РЕАЛИЗОВАТЬ ДЛЯ pending_basecalling_jobs ПРОВЕРКУ, ЧТОБЫ ПЕРВАЯ ТАСКА, ЗАВЕРШЕННАЯ ДЛЯ ОБРАЗЦА, ПОШЛА В РАБОТУ НА ДАУНСТРИМ
            for job in job_ids_converting:
                job_results[sample]['converting'][job] = ''

            # Pulling basecalling task, which will start after all converting jobs finish succesfully
            job_results[sample]['basecalling'] = {}
            job_ids_basecalling = []
            for mod_bases in ['5mCG_5hmCG', '5mCG']:
                job_results[sample]['basecalling'][mod_bases] = {}
                job_id_basecalling = basecalling(sample=sample, pod5_dir=pod5_dir, ubam_dir=out_dir,
                                                mod_bases=mod_bases, model=model, dependency=job_ids)
                job_results[sample]['basecalling'][mod_bases][job_id_basecalling] = ''
                job_ids_basecalling.append(job_id_basecalling)

            pending_jobs[sample].extend(job_ids_basecalling)
                
            
            # Pulling alignment task, which will start after basecalling jobs finish succesfully
            job_id_aligning = aligning(sample=sample, pod5_dir=pod5_dir, ubam_dir=out_dir,
                                             mod_bases=mod_bases, model=model, dependency=[job_id_basecalling])
            
        
        # Check conversion jobs
        for sample, jobs in pending_conversion_jobs:
            job_ids = jobs.copy()
            for job_id in job_ids:
                if not is_slurm_job_running(job_id):
                    pending_conversion_jobs[sample].remove((job_id, sample))
            # if no conversion jobs remaining for sample, basecalling starts
            if not pending_conversion_jobs[sample]:
                del pending_conversion_jobs[sample]
                # create task for every modification that chemistry of ONT kit allows
                for mod_bases in ['5mCG_5hmCG', '5mCG']:
                    job_id = basecalling(sample=sample, mod_bases=mod_bases, model=model)(sample:str, pod5_dir:str, ubam_dir:str, mod_bases:str, model:str, dependency:list):
                pending_basecalling_jobs.update({sample:job_id})

        # Check basecalling jobs
        for sample, job_id in pending_basecalling_jobs.items():
            if not is_slurm_job_running(job_id):
                del pending_basecalling_jobs[sample]
        # print summary check every 30 sec
        #print_running_jobs()

        # pause before next check
        time.sleep(10)

    print("All samples processed.")

if len(sys.argv) < 6:
    raise SyntaxError(print('Usage: nanopore_preprocessing.py in_dir pod5_dir tmp_dir out_dir dorado_model threads'))

in_dir = f'{os.path.normpath(os.path.join(sys.argv[1]))}{os.sep}'
pod5_dir = f'{os.path.normpath(os.path.join(sys.argv[2]))}{os.sep}'
out_dir = f'{os.path.normpath(os.path.join(sys.argv[4]))}{os.sep}'
dorado_model = f'{os.path.normpath(os.path.join(sys.argv[5]))}{os.sep}'
threads_per_machine = sys.argv[6]

configs = f"{os.path.dirname(os.path.realpath(__file__).replace('src', 'configs'))}/"

directories_data = load_yaml(file_path=f'{configs}dir_structure.yaml')

# generate paths strings for subdirs in out_dir
for d in directories_data.keys():
    dir_name = directories_data[d]['name']
    directories_data[d]['path'] = f'{os.path.join(out_dir, dir_name)}{os.sep}'

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

if __name__ == "__main__":
    main()