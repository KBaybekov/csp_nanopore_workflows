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
from utils.nanopore import aligning, basecalling, modifications_lookup, sv_lookup, convert_fast5_to_pod5, get_fast5_dirs
from utils.slurm import is_slurm_job_running


def ch_d(d):
    print(d)
    exit()

def create_sample_sections_in_dict(target_dict:dict, sample:str, sections:list, dict_type:str) -> dict:
    target_dict[sample] = {}
    if dict_type == 'job_listing':
        target_dict[sample].fromkeys(sections, [])
    elif dict_type == 'job_logging':
        target_dict[sample].fromkeys(sections, {})
    return target_dict

def store_job_ids(sample:str, stage:str, job_ids:list) -> None:
    pending_jobs[sample][stage].extend(job_ids)
    job_results[sample][stage] = dict.fromkeys(job_ids, '')

def main():
    # create subdirs in dir
    for dir_data in directories.values():
        os.makedirs(dir_data['path'], exist_ok=True) 

    sample_dirs = get_dirs_in_dir(dir=in_dir)
    # Create dict with sample_name:[sample_fast5s_dirs] as key:val
    sample_data = {os.path.basename(os.path.normpath(s)):get_fast5_dirs(dir=s) for s in sample_dirs}
    # Create list of samples for iteration
    samples = list(sample_data.keys())
    samples.sort()
        
    # Loop will proceed until we're out of jobs for submitting or samples to process
    while samples or pending_jobs:
        # Choose sample
        if samples:
            sample_job_ids = dict.fromkeys(stages, [])
            # pop sample from initial sample list
            sample = samples.pop(0)
            pending_jobs = create_sample_sections_in_dict(target_dict=pending_jobs, sample=sample,
                                                          sections=stages, dict_type='job_listing')
            job_results = create_sample_sections_in_dict(target_dict=job_results, sample=sample,
                                                          sections=stages, dict_type='job_logging')
            fast5_dirs = sample_data[sample]
            
            # Pulling converting task, one per job
            sample_job_ids['converting'].extend(convert_fast5_to_pod5(fast5_dirs=fast5_dirs, sample=sample,
                                                                      out_dir=directories['pod5_dir']['path'],
                                                                      threads=threads_per_converting,
                                                                      ntasks=tasks_per_machine_converting))

            # Basecalling, aligning and mod lookup will be performed for each modification type in list          
            for mod_type in mod_bases:
                # basecalling results will be stored in ubam dir of sample.
                #GPU
                job_id_basecalling, ubam = basecalling(sample=sample,
                                                 in_dir=directories['pod5_dir']['path'],
                                                 out_dir=directories['ubam_dir']['path'],
                                                mod_type=mod_type, model=dorado_model,
                                                dependency=sample_job_ids['converting'])
                sample_job_ids['basecalling'].append(job_id_basecalling)
                
                # Alignment results will be stored in bam dir of sample.
                #CPU
                job_id_aligning, bam = aligning(sample=sample, ubam=ubam, out_dir=directories['bam_dir']['path'],
                                           mod_type=mod_type, ref=dorado_model, threads=threads_per_align,
                                           dependency=[job_id_basecalling], exclude_nodes=exclude_node_cpu)
                sample_job_ids['aligning'].append(job_id_aligning)

                # mod lookup results will be stored in common dir of sample.
                #CPU
                sample_job_ids['mod_lookup'].append(modifications_lookup(sample=sample, bam=bam, out_dir=directories['other_dir']['path'],
                                                     mod_type=mod_type, model=os.path.basename(dorado_model), ref=ref_fasta,
                                                     threads=threads_per_calling_mod, dependency=[job_id_aligning],
                                                     exclude_nodes=exclude_node_cpu))
            
            # SV calling will be performed just once with using of the first ready BAM 
            # SV lookup results will be stored in common dir of sample.
            #CPU
            sample_job_ids['sv_lookup'].append(sv_lookup(sample=sample, bam=bam, out_dir=directories['other_dir']['path'],
                                                     mod_type=mod_type, model=os.path.basename(dorado_model), ref=ref_fasta,
                                                     tr_bed=ref_tr_bed, threads=threads_per_calling_sv, dependency=sample_job_ids['aligning'],
                                                     dependency_type='any', exclude_nodes=exclude_node_cpu))
            
            # Sample related job ids will be stored in logging dict
            for stage, job_ids in sample_job_ids.items():
                store_job_ids(sample=sample, stage=stage, job_ids=job_ids)            
        
        # Check pending jobs
        elif pending_jobs:
            job_ids = jobs.copy()
            for job_id in job_ids:
                if not is_slurm_job_running(job_id):
                    pending_conversion_jobs[sample].remove((job_id, sample))
            # if no conversion jobs remaining for sample, basecalling starts
            if not pending_conversion_jobs[sample]:
                del pending_conversion_jobs[sample]
                # create task for every modification that chemistry of ONT kit allows
                for mod_base in ['5mCG_5hmCG', '5mCG']:
                    job_id = basecalling(sample=sample, mod_base=mod_base, model=model)(sample:str, pod5_dir:str, ubam_dir:str, mod_base:str, model:str, dependency:list):
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
    directories[d]['path'] = f'{os.path.join(out_dir, directories[d]['name'])}{os.sep}'

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