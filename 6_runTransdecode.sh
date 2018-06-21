#!/bin/bash

#PATH necesario
export PATH=$PATH:/home/luisrana/BioInf/assembly/bowtie-0.12.8

#Directorio donde se encuentra TransDecoder
trdir="/home/user/TransDecoder-2.0"

#Ejecución de TransDecoder con el archivo Trinity.fasta
$trdir/TransDecoder.LongOrfs -t Trinity.fasta
$trdir/TransDecoder.Predict  -t Trinity.fasta



