library(dplyr)

assign_genes <- function(fin) {

    get.gene <- function(x,rg) {
        ref.gene <- rg
        chr <- as.character(x[1])
        strand <- as.numeric(x[4])
        intron.start <- as.numeric(x[2])
        intron.end <- as.numeric(x[3])
        #browser()
        match <- filter(ref.gene, V3==chr, V4==strand, V5 <= intron.start, V6 >= intron.end)
        
        if (nrow(match)==0) {
            return(list('gene'=NA,'refseq_id'=NA))
        
        # only one match, return it regardless of which refseq category it is
        } else if (nrow(match) == 1) {
            return(list('gene'=match$V13,'refseq_id'=match$V2))
        
        # if at least one mRNA transcript match, use that
        } else if (length(grep("NM_",match$V2)) > 0) {
            match <- match[grepl("NM_",match$V2), ]
            best_gene_idx <- !duplicated(match$V13)
            gene <- paste(match$V13[best_gene_idx],collapse=';')
            refseq_id <- paste(match$V2[best_gene_idx],collapse=';')
            return(list('gene'=gene,'refseq_id'=refseq_id))
        
        # otherwise, fall through to at least one non-mRNA transcript match
        } else {
            best_gene_idx <- !duplicated(match$V13)
            gene <- paste(match$V13[best_gene_idx],collapse=';')
            refseq_id <- paste(match$V2[best_gene_idx],collapse=';')
            return(list('gene'=gene,'refseq_id'=refseq_id))
        }
    }

    ref.gene <- read.table('/util/ccr/data/UCSC/hg38/refGene.txt.gz', sep = '\t', header=F, stringsAsFactors=F)
    #ref.gene <- read.table('../Downloads/refGene_hg38.txt.gz', sep = '\t', header=F, stringsAsFactors=F)
    ref.gene <- ref.gene[grep('_', ref.gene$V3, invert=T), ]
    ref.gene$V4 <- ifelse(ref.gene$V4 == '+',1,2) # match up with mmrf file strand notation
    
    mmrf <- read.table(fin, sep= '\t', header=F, stringsAsFactors = F)
    
    #mmrf.sample <- mmrf[1:10000, ]
    #mmrf <- mmrf.sample
    system.time(result <- apply(mmrf,1,get.gene, rg=ref.gene))[3]
    result <- unlist(result, recursive=T, use.names=F)
    result <- matrix(result,ncol=2,nrow=nrow(mmrf),byrow=T)
    mmrf$gene <- result[,1]
    mmrf$refseq_id <- result[,2]
    filename <- sub('cleaned','gene-anno',basename(fin))
    write.table(mmrf, file=filename, row.names=F, col.names=F, quote=F, sep='\t')
    return(mmrf)
}

args <- commandArgs(trailingOnly = T)
infile <- args[1]
output <- assign_genes(infile)
