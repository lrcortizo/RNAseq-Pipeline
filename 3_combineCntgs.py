#!/usr/bin/env python

import collections
import numpy as np
import matplotlib.pyplot as plt
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


#funcion de mapeo
def process_blockNN(d):
    print multiprocessing.current_process().name,multiprocessing.current_process().pid, d
    
    #archivos pair-end
    infile1 = d 
    infile2 = d.replace("group1", "group2")

    q=open(infile1, 'rb')
    p1=pickle.load(q)
    q.close()
        
    q=open(infile2, 'rb')
    p2=pickle.load(q)
    q.close()

    #Se crean dos conjuntos a partir de los archivos y se hace una union, para formar una lista con los comunes validos
    s1 = set(p1)
    s2 = set(p2)
    mlist = list(s1.union(s2))
    print infile1, infile2,len(p1), len(p2), len(mlist)
    print

    #Funcion que se encarga de escribir los archivos finales resultantes
    def write_output(file_in, all_pseqs):
        #INDICAR aqui el directorio de trabajo
        srr="/home/user/directoriotrabajo"

        if os.path.exists(srr):
            #Se comprueba si existe el directorio /fq_out en el directorio indicado y de lo contrario se crea
            if not os.path.exists(srr+"/fq_out"):
                os.makedirs(srr+"/fq_out")
	
            in_file = os.path.splitext(file_in)[0] + ".fq"
            #Se escribe el archivo de salida reducido
            reduced_file =  srr+"fq_out/" + os.path.basename(file_in).split(".")[0] + "_r.fastq"
            ofile = open(reduced_file,"w")
            for rec in SeqIO.parse(in_file, "fastq"):
                if rec.id in all_pseqs:
                    SeqIO.write(rec, ofile, "fastq")
            ofile.close()
        else:
            sys.exit("ERROR " + srr + " doesn't exists")

    fqfiles=[ infile1.replace("pkl","fq"), infile2.replace("pkl","fq") ]
    print ".... writing reduced:", fqfiles[0]
    write_output(fqfiles[0], mlist)
    print ".... writing reduced:", fqfiles[1]        
    write_output(fqfiles[1], mlist)

    s=[]
    return s


#funcion de particion, si procede
def partition_func(s):
    #print "inside partition", multiprocessing.current_process().name
    #print s
    return s

	
#funcion de reducion
def reduce_func(s):
    return s

	
class RunMapper:
    def __init__(self, srr):
        self.srr = srr
        self.mapper = SimpleMapReduce(process_blockNN, partition_func, reduce_func)

    def run(self): 
        try: 
            #INDICAR la expresion regular que busca todos los archivos deseados
	        #Ejecucion SimpleMapReduce
            res= self.mapper( find_files(self.srr+"/tmp", "group1*.pkl")  )
        except:
            print "done"

			
# --------------------------------------
if __name__ == '__main__':
    #INDICAR aqui el directorio de trabajo
    srr="/home/user/directoriotrabajo"
    #Comprobar si el directorio indicado contiene los archivos necesarios
    if os.path.exists(srr+"/tmp"):
        start_time = timeit.default_timer()
        C=RunMapper(srr)
        C.run()
        elapsed = timeit.default_timer() - start_time
        print "ELAPSED TIME =", elapsed
    else:
        print("ERROR: +"+ srr +" doesn't contain /tmp directory with input files")
                                                   


 
