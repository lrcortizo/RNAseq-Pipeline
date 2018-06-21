#!/bin/bash

#PATH necesario
export PATH=$PATH:/home/luisrana/BioInf/assembly/bowtie-0.12.8:/home/luisrana/BioInf/assembly/RSEM-1.2.25

#Directorio donde se encuentra el archivo Trinity.fasta
srrdir="/home/user/trinity"

#Directorio donde se encuentra Trinity
bindir="/home/user/trinityrnaseq-2.1.1"

#Archivos pair-end reducidos
file1=rfile1.fq
file2=rfile2.fq

#Ejecucion de RSEM
$bindir/util/align_and_estimate_abundance.pl --transcripts $srrdir/Trinity.fasta --seqType fq --left $file1 --right $file2 --est_method RSEM --aln_method bowtie --trinity_mode --prep_reference --output_dir $srrdir/RSEM
