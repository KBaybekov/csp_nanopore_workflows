#!/bin/bash
#SBATCH --job-name=myarrayjob
#SBATCH --ntasks=5
#SBATCH --nodes=5
#SBATCH --partition=cpu_nodes
#SBATCH --cpus-per-task=1
#SBATCH --array=1-10

# Specify the path to the config file
config=/common_share/github/csp_nanopore_workflows/configs/t.txt

# Extract the sample name for the current $SLURM_ARRAY_TASK_ID
sample=$(awk -v ArrayTaskID=$SLURM_ARRAY_TASK_ID '$1==ArrayTaskID {print $2}' $config)

# Extract the sex for the current $SLURM_ARRAY_TASK_ID
sex=$(awk -v ArrayTaskID=$SLURM_ARRAY_TASK_ID '$1==ArrayTaskID {print $3}' $config)

# Print to a file a message that includes the current $SLURM_ARRAY_TASK_ID, the same name, and the sex of the sample
echo "This is array task ${SLURM_ARRAY_TASK_ID}, the sample name is ${sample} and the sex is ${sex}." >> output.txt