#!/usr/bin/env python

import collections
import numpy as np
import time
import os, fnmatch
import sys
import itertools
from operator import itemgetter, attrgetter
import math
from Bio import SeqIO
from Bio import Seq
from Bio.SeqRecord import SeqRecord
from Bio.Blast.Applications import NcbitblastnCommandline as Tblastn
from Bio import AlignIO
from Bio.Align.Applications import ClustalOmegaCommandline

from scipy import *
import struct
import re
import json
import cPickle as pickle
from collections import defaultdict
import multiprocessing
from copy import deepcopy

import timeit
import operator

#Funcion encargada de guardar archivos mediante pickle
def save_object(obj, filename):
    with open(filename, 'wb') as output:
        pickle.dump(obj, output, pickle.HIGHEST_PROTOCOL)

#Busca archivos e un directorio a partir de una expresion regular
def find_files(directory, pattern):
    for root, dirs, files in os.walk(directory):
        for basename in files:
            if fnmatch.fnmatch(basename, pattern):
                filename = os.path.join(root, basename)
                yield filename
                
#Implementacion sencilla del framework MapReduce
class SimpleMapReduce(object):
    def __init__(self, map_func, partition_func, reduce_func, num_workers=None):
        self.map_func       = map_func
        self.partition_func = partition_func
        self.reduce_func    = reduce_func
        self.pool = multiprocessing.Pool(4)

    def partition(self, mapped_values):
        partitioned_data  = self.partition_func( list(mapped_values))
        return partitioned_data
    
    def __call__(self, inputs, chunksize=1):
        map_responses    = self.pool.map(self.map_func, inputs,chunksize=chunksize)
        partitioned_data = self.partition( itertools.chain(*map_responses) )
        reduced_values   = self.pool.map(self.reduce_func, partitioned_data)
        return reduced_values



def batch_iterator(iterator, batch_size) :
    """Returns lists of length batch_size.
 
    This can be used on any iterator, for example to batch up
    SeqRecord objects from Bio.SeqIO.parse(...), or to batch
    Alignment objects from Bio.AlignIO.parse(...), or simply
    lines from a file handle.
 
    This is a generator function, and it returns lists of the
    entries from the supplied iterator.  Each list will have
    batch_size entries, although the final list may be shorter.
    """
    entry = True #Make sure we loop once
    while entry :
        batch = []
        while len(batch) < batch_size :
            try :
                entry = iterator.next()
            except StopIteration :
                entry = None
            if entry is None :
                #End of file
                break
            batch.append(entry)
        if batch :
            yield batch

			
#funcion de mapeo
def process_blockNN(d):
    print "in blockNN"
	
    #INDICAR directorio de trabajo
    srrdir="/home/luis/Escritorio/TFG/SRR1550983"
	
    #Se comprueba si existe el directorio
    if os.path.exists(srr):
        print multiprocessing.current_process().name,multiprocessing.current_process().pid, d[0].name
    
        #Se comprueba si existe el directorio /tmp en el directorio indicado y de lo contrario se crea
        if not os.path.exists(srr+"/tmp"):
            os.makedirs(srr+"/tmp")

        #Se escribe el archivo temporal que va a contener las secuencias de ejecucion por partes de Blast
        filename = srrdir+"/tmp/group1_%i.fasta" % int(d[0].name.split(".")[1])
        handle = open(filename, "w")
        for i, batch in enumerate(d):
            print i, batch
            count = SeqIO.write(batch, handle, "fasta")
        handle.close()
	
        tmpOutFile= filename.replace(".fasta",".blast_out")

        in_chain=filename

        #INDICAR el archivo contra el que se va a ejecutar blast
        in_query="./homo_sapiens_IMGT.fasta"
		
    #Se compruba que existe el archivo in_query
        if os.path.isfile(in_query):
            #Ejecucion de blast
            blastp_cmdline = Tblastn( query = in_query,
                                    subject = in_chain, 
                                      evalue  = 7.5e-5,
                                      outfmt=6,       
                                      #num_alignments=1000,
                                      max_target_seqs=int(1e6),
                                      out=tmpOutFile)
    
            print blastp_cmdline
            stdout, stderr = blastp_cmdline()
            fp = open(tmpOutFile,"r")
            pseqs=[]
            for lk in fp:
                pseqs.append(lk.split()[1])
            fp.close()

            outfile = filename.replace(".fasta",".pkl")

            #Se almacena el resultado final pkl
            save_object(pseqs, outfile)
    
            #Se eliminan los archivos temporales
            os.remove(filename)
            os.remove(tmpOutFile)

            #Se escribe el archivo fastq final
            filename = srrdir+"/tmp/group1_%i.fq" % int(d[0].name.split(".")[1])
            handle = open(filename, "w")
            for i, batch in enumerate(d):
                count = SeqIO.write(batch, handle, "fastq")
            handle.close()

            print pseqs
            pseqs=[]

            return pseqs
        else:
            sys.exit("ERROR: " + in_query + " doesn't exists")
    else:
        sys.exit("ERROR: " + srr + " doesn't exists")

		
#funcion de particion, si procede
def partition_func(s):
    print "inside partition", multiprocessing.current_process().name
    print s
    return s

	
#funcion de reduccion, si procede
def reduce_func(s):
    return s
    
	
class RunMapper:
    def __init__(self, srr):
        #Se crea el objeto MapReduce indicando sus funciones
        self.srr = srr
        self.mapper = SimpleMapReduce(process_blockNN, partition_func, reduce_func)
		
    def run(self):
        #Se comprueba si existe el archivo
        if os.path.exists(self.srr):
		
            #INDICAR la expresion regular que busca los archivos(left pair-end) deseados para reducir
            iter1= list(find_files(self.srr, "SRR*_1.fastq_*") )
            iter1.sort()
			
            #Para cada trozo dividido
            for i in range(len(iter1)):
                fileb = iter1[i]
                print i, iter1[i], "----------------------------------------"
                record_iter = SeqIO.parse(open(fileb),"fastq")
                try:
                    #ejecucion SimpleMapReduce
                    res= self.mapper( batch_iterator(record_iter, 20000) )
                except:
                    print "done"
        else:
            print("ERROR: directory doesn't exists " + self.srr)


#--------------------------------------
if __name__ == '__main__':

    start_time = timeit.default_timer()

            
    #INDICAR aqui el directorio de trabajo
    srr="/home/luis/Escritorio/TFG/SRR1550983"
	
    R = RunMapper(srr)
    R.run()

    elapsed = timeit.default_timer() - start_time
    print "ELAPSED TIME =", elapsed



