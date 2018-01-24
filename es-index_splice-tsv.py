import elasticsearch
import hashlib
from elasticsearch import helpers
from timeit import default_timer as timer

def get_es_id(CHROM, SPLICESTART, SPLICEEND, index_name, type_name):
    es_id = '%s-%s-%s-%s-%s' %(CHROM,SPLICESTART,SPLICEEND,index_name,type_name)
    es_id = es_id.encode('utf-8')
    es_id = hashlib.sha224(es_id).hexdigest()
    return(es_id)


def set_data(index_name,type_name,input_tsv):

    with open(input_tsv,'r') as fin:
        for line in fin:
            line = line.strip()
            if line.startswith('#'):
                cols = line.split('\t')
                headers = cols[:9]
                sample_IDs = cols[9:]
                continue
            cols = line.split('\t')
            
            nested = cols[9:]
            nested = dict(zip(sample_IDs, nested))
            nested = {key:value for key,value in nested.items() if value != '.:.:.'}
            samples = []
            for sID,triplet in nested.items():
                unique_reads_cross,multimap_reads_cross,max_overhang = triplet.split(':')
                samples.append({'unique_reads_cross' :  unique_reads_cross,
                                'multimap_reads_cross': multimap_reads_cross,
                                'max_overhang' :        max_overhang,
                                'sample_ID' :           sID
                                })
            
            non_nested = cols[:9]
            non_nested = dict(zip(headers, non_nested))
            _id = get_es_id(non_nested['chrom'],
                            non_nested['splice_start'],
                            non_nested['splice_end'],
                            index_name,
                            type_name)
            source = {key:value for key,value in non_nested.items() if value != '.'}
            source['sample'] = samples
            
            action = {"_index":  index_name,
                      "_type":   type_name,
                      "_source": source,
                      "_id":     _id
                     }
            yield action


def main():
    hostname = '199.109.195.2'
    port = '9200'
    index_name = 'empiregen_splice'
    type_name = 'empiregen_splice'
    input_tsv = 'splices.tsv'

    total_start = timer()
    es = elasticsearch.Elasticsearch(host=hostname, port=port)
    no_variants_processed, errors = helpers.bulk(es, set_data(index_name,
                                                type_name,
                                                input_tsv,
                                                ),
                                                chunk_size=500,
                                                request_timeout=600,
                                                stats_only=True)

    print(no_variants_processed,errors)
    total_end = timer()
    total_time = total_end-total_start
    print('total runtime: ' + str(total_time))


if __name__ == '__main__':
    main()