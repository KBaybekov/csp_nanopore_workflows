import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.common import run_shell_cmd

command = 'echo "$PWD, $(hostname) started" && sleep 120 && echo "$(hostname) finished!"'
working_dir = sys.argv[1]
job_name = sys.argv[2]
partition = sys.argv[3]
nodes = sys.argv[4]
#dependency = sys.argv[5]
ntasks = sys.argv[6]
#exclude_nodes = sys.argv[7]
dependency = '120'
exclude_nodes = ''
dependency_type = 'all'


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
                val = val.split(',')
                if dependency_type == 'all':
                    delimiter = ':'
                elif dependency_type == 'any':
                    delimiter = '?'
                val = f"afterok:{f'{delimiter}'.join(map(str, val))}"
            
            slurm_script.append(option_str.format(opt, val))
    else:
        slurm_script.extend(['\n', val])


with open(slurm_script_file, 'w') as s:
    s.write('\n'.join(slurm_script))


os.system(f'sbatch {slurm_script_file}')
cmd = f"squeue -n {job_name} | tail -1| awk '{{print $1}}'"
#job_id = subprocess.run(, shell=True,
#                        capture_output=True, text=True).stderr
job_id, stderr = run_shell_cmd(cmd=f"squeue -n {job_name} | tail -1| awk '{{print $1}}'")
job_id = job_id.removesuffix('\n')

#job_id = os.system(f"squeue -n {job_name} | tail -1| awk '{{print $1}}'")
print(job_id, job_id, job_id)