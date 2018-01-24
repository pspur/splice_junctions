directory = '/gpfs/projects/academic/big2/dbGaP/MM/RNA-Seq/STAR/SplicJunctions/hg38'

samples = []
with open('oddcolumns.txt','r') as fin:
    for line in fin:
        samplenum = line.strip().split('.')[0].split('_')[-1]
        samples.append(samplenum)

for sample in samples:
    with open('{0}/MMRF_{1}_T_p2SJ.out.tab'.format(directory,sample),'r') as fin,open('MMRF_{0}_T_p2SJ.cleaned.tab'.format(sample),'w') as fout:
        for line in fin:
            line = line.strip()
            if len(line.split('\t')) == 9:
                fout.write(line + '\n')