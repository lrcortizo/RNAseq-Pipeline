#!/bin/bash

#Se quitan los espacios en la descripcion de las secuencias de los archivos fastq para evitar errores
mv rfile1.fq rfile1_t$1.fq
mv rfile2.fq rfile2_t$1.fq

cat rfile1_t$1.fq | sed -e 's/\(^@SRR[^ ].*\)[ \]\([^ ].*\)/\1_\2/' > tmp1.fq
cat rfile2_t$1.fq | sed -e 's/\(^@SRR[^ ].*\)[ \]\([^ ].*\)/\1_\2/' > tmp2.fq
mv tmp1.fq rfile1.fq
mv tmp2.fq rfile2.fq

#PATH necesario
export PATH=$PATH:/home/luisrana/BioInf/assembly/bowtie-0.12.8:/home/luisrana/BioInf/assembly/trinityrnaseq-2.1.1/trinity-plugins/jellyfish-2.1.4

#Directorio de salida
srrdir="/home/user/trinity"

#Directorio donde se encuentra Trinity
bindir="/home/user/trinityrnaseq-2.1.1"

#Archivos pair-end reducidos
file1="rfile1.fq"
file2="rfile2.fq"

#Ejecucion de Trinity
$bindir/Trinity --seqType fq --max_memory 12G --left $file1 --right $file2 --CPU 4 --no_cleanup --output $srrdir

