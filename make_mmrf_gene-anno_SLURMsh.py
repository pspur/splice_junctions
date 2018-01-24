import os

def getsamples():
    samples = [file for file in os.listdir('/gpfs/projects/academic/big2/dbGaP/MM/RNA-Seq/STAR/SplicJunctions/hg38') if file.endswith('.tab')]
    return(samples)

def create_slurm_scripts(samples):
    for f in samples:
        samplenum = f.split('_')[1]
        with open("slurm_gene-anno_{0}.sh".format(samplenum),"w") as fout:
            fout.write('#!/bin/sh\n\n')
            fout.write('#SBATCH --partition=general-compute\n')
            fout.write('#SBATCH --time=2:00:00\n')
            fout.write('#SBATCH --nodes=1\n')
            fout.write('#SBATCH --mem=16000\n')
            fout.write('#SBATCH --ntasks-per-node=1\n')
            fout.write('#SBATCH --job-name=splice-junction_gene-anno_{0}\n'.format(samplenum))
            fout.write('#SBATCH --output=splice-junction_gene-anno_{0}.log\n'.format(samplenum))
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
            fout.write('Rscript assign_gene-info_to_splice-junctions_command-line.R /gpfs/projects/academic/big2/dbGaP/MM/RNA-Seq/STAR/SplicJunctions/hg38/{0}\n\n'.format(f))
            fout.write('echo "All Done!"')


def main():
    samples = getsamples()
    create_slurm_scripts(samples)


if __name__ == '__main__':
    main()
