#!/bin/sh

#SBATCH --partition=general-compute
#SBATCH --time=8:00:00
#SBATCH --nodes=1
#SBATCH --mem=128000
#SBATCH --ntasks-per-node=1
#SBATCH --job-name=merge_splice_tab_files
#SBATCH --output=merge_splice_tab_files.log
#SBATCH --mail-user=paulspur@buffalo.edu
#SBATCH --mail-type=END
#SBATCH --qos=supporters
#SBATCH --account=big

echo "SLURM_JOBID="$SLURM_JOBID
echo "SLURM_JOB_NODELIST"=$SLURM_JOB_NODELIST
echo "SLURM_NNODES"=$SLURM_NNODES
echo "SLURMTMPDIR="$SLURMTMPDIR
echo "working directory = "$SLURM_SUBMIT_DIR

ulimit -s unlimited
module load python/anaconda-5.0.1
module list

echo "Launch job"

source activate py36
python merge_tab_files_into_tsv.py

echo "All Done!"
