# This script has a memory requirement of at least 96gb due to the formation of 
# a single dict containing every splice junction across all sample files. I 
# execute it on the cluster using slurm_merge_tab_files.sh

import os
import json
import hashlib
from collections import defaultdict
from timeit import default_timer as timer

def get_es_id(CHROM, SPLICESTART, SPLICEEND, index_name, type_name):
    es_id = '%s-%s-%s-%s-%s' %(CHROM,SPLICESTART,SPLICEEND,index_name,type_name)
    es_id = es_id.encode('utf-8')
    es_id = hashlib.sha224(es_id).hexdigest()
    return(es_id)


# Generate an elasticsearch document body that conforms to our mapping.
def create_body(data):
    body = defaultdict(dict)
    needs_mapping = {"annotated": {"0": "unannotated",
                                   "1": "annotated"},
                     "motif" : {"0": "non-canonical",
                                "1": "GT/AG",
                                "2": "CT/AC",
                                "3": "GC/AG",
                                "4": "CT/GC",
                                "5": "AT/AC",
                                "6": "GT/AT"},
                     "strand" : {"0": "undefined",
                                 "1": "+",
                                 "2": "-"},
                    }

    nested_fields = ('unique_reads_cross',
                     'multimap_reads_cross',
                     'max_overhang',
                     'sample_ID')
    
    for key in data.keys():
        if key not in nested_fields:
            if key not in needs_mapping.keys():
                body[key] = data[key]
            else:
                mapped_value = needs_mapping[key][data.get(key)]
                body[key] = mapped_value
    body['sample'] = [{key:data[key] for key in nested_fields}]
    return(json.loads(json.dumps(body)))


# Build dict from input files. There is significant overlap of splice junctions
# between samples, and we're interesting in nesting the sample-specific data.
def merge_splices(input_files,index_name,type_name):
    header = ('chrom',
              'splice_start',
              'splice_end',
              'strand',
              'motif',
              'annotated',
              'unique_reads_cross',
              'multimap_reads_cross',
              'max_overhang',
              'gene_refGene',
              'RSID_refGene')

    output_dict = {}
    entire_start = timer()
    for i,f in enumerate(input_files,start=1):
        with open(f,'r') as fin:
            sample_ID = os.path.basename(f).split('.')[0][:11]
            print('{0} out of {1}'.format(i,len(input_files)))
            for line in fin:
                line = line.strip()
                data = dict(zip(header, line.split('\t')))
                data['sample_ID'] = sample_ID
                if data['gene_refGene'] == 'NA':
                    data['gene_refGene'] = 'intergenic'
                data = {key:value for key,value in data.items() if value != 'NA'}
                es_id = get_es_id(data['chrom'],
                                  data['splice_start'],
                                  data['splice_end'],
                                  index_name,
                                  type_name)
                if es_id not in output_dict.keys():
                    output_dict[es_id] = create_body(data)
                else:
                    output_dict[es_id]['sample'].append(
                        {'unique_reads_cross' :  data['unique_reads_cross'],
                         'multimap_reads_cross': data['multimap_reads_cross'],
                         'max_overhang' :        data['max_overhang'],
                         'sample_ID' :           data['sample_ID']
                        })
    
    return(output_dict)


# convert dict, which can only be loaded as a massive json, into tsv, which can
# be read line-by-line with minimal memory overhead into elasticsearch.
def dict_to_tsv(data,input_files,tsv_filename):
    sample_IDs = sorted([os.path.basename(file).split('.')[0][:11] for file in input_files])
    with open(tsv_filename,'w') as fout:
        header = ('chrom\tsplice_start\tsplice_end\tstrand\tmotif\tannotated\tgene_refGene\tRSID_refGene\t{0}\n'.format('\t'.join([sample_ID for sample_ID in sample_IDs])))
        fout.write(header)
        for entry in data.values():
            sample_fields_dict = {sample_ID:'.:.:.' for sample_ID in sample_IDs}
            for sample in entry['sample']:
                sample_field = '{0}:{1}:{2}'.format(sample['unique_reads_cross'],
                                                    sample['multimap_reads_cross'],
                                                    sample['max_overhang'])
                sample_fields_dict[sample['sample_ID']] = sample_field
            
            sample_fields = [sample_fields_dict[sID] for sID in sample_IDs]
            sample_fields_string = '\t'.join(sample_fields)
            line = '{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}\n'.format(
                                entry['chrom'],
                                entry['splice_start'],
                                entry['splice_end'],
                                entry['strand'],
                                entry['motif'],
                                entry['annotated'],
                                entry.get('gene_refGene','.'),
                                entry.get('RSID_refGene','.'),
                                sample_fields_string
                                )
            fout.write(line)
    return


def main():
    index_name = 'empiregen_splice'
    type_name = 'empiregen_splice'
    tsv_filename = 'splices.tsv'
    input_files = [file for file in os.listdir('./') if file.endswith('.gene-anno.tab')]
    splice_dict = merge_splices(input_files,index_name,type_name)
    dict_to_tsv(splice_dict,input_files,tsv_filename)


if __name__=='__main__':
    main()