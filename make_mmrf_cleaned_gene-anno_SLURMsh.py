import os

def getsamples():
    samples = []
    with open('oddcolumns.txt','r') as fin:
        for line in fin:
            samplenum = line.strip().split('.')[0].split('_')[-1]
            samples.append(samplenum)
    return(samples)

def create_slurm_scripts(samples):
    for s in samples:
        #samplenum = f.split('_')[1]
        with open("slurm_gene-anno_{0}.sh".format(s),"w") as fout:
            fout.write('#!/bin/sh\n\n')
            fout.write('#SBATCH --partition=general-compute\n')
            fout.write('#SBATCH --time=2:00:00\n')
            fout.write('#SBATCH --nodes=1\n')
            fout.write('#SBATCH --mem=16000\n')
            fout.write('#SBATCH --ntasks-per-node=1\n')
            fout.write('#SBATCH --job-name=splice-junction_gene-anno_{0}\n'.format(s))
            fout.write('#SBATCH --output=splice-junction_gene-anno_{0}.log\n'.format(s))
            fout.write('#SBATCH --mail-user=paulspur@buffalo.edu\n')
            fout.write('#SBATCH --mail-type=END\n')
            fout.write('#SBATCH --qos=supporters\n')
            fout.write('#SBATCH --account=big\n\n')
            fout.write('echo "SLURM_JOBID="$SLURM_JOBID\n')
            fout.write('echo "SLURM_JOB_NODELIST"=$SLURM_JOB_NODELIST\n')
            fout.write('echo "SLURM_NNODES"=$SLURM_NNODES\n')
            fout.write('echo "SLURMTMPDIR="$SLURMTMPDIR\n')
            fout.write('echo "working directory = "$SLURM_SUBMIT_DIR\n\n')
            fout.write('ulimit -s unlimited\n')
            fout.write('module load R\n')
            #fout.write('module load VEP\n')
            fout.write('module list\n\n')
            fout.write('echo "Launch job"\n\n')
            fout.write('Rscript assign_gene-info_to_cleaned_splice-junctions_command-line.R MMRF_{0}_T_p2SJ.cleaned.tab\n\n'.format(s))
            fout.write('echo "All Done!"')


def main():
    samples = getsamples()
    create_slurm_scripts(samples)


if __name__ == '__main__':
    main()
