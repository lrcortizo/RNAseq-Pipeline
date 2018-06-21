#!/usr/bin/env python

import shutil
import subprocess
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

#Busca archivos e un directorio a partir de una expresion regular
def find_files(directory, pattern):
    for root, dirs, files in os.walk(directory):
        for basename in files:
            if fnmatch.fnmatch(basename, pattern):
                filename = os.path.join(root, basename)
                yield filename

				
#Clase encargada de combinar los archivos fastq
class CombineFastas: 
    def __init__(self,srr):
        self.srr = srr

    def run(self):
        #INDICAR la expresion regular que busca todos los archivos deseados
        iter1= list(find_files(self.srr + "fq_out", "group1*.fastq") )
        iter2= list(find_files(self.srr + "fq_out", "group2*.fastq") )
	
        #Funcion que se encarga de escribir los archivos finales resultantes
        def write_final_output(indx, iter_file):
            #Se recorre cada uno de los archivos y se vuelcan en un solo archivo final
            x = [ (int(os.path.basename(s).split(".")[0].split("_")[1]), s) for s in iter_file]
            x.sort()
            mlist = [ s[1] for s in x]
            print mlist
            final_rfile= self.srr+"fq_out/rfile%s.fq" % (indx)
            outfile = open(final_rfile,"w")
            for infile in mlist:
                shutil.copyfileobj(open(infile), outfile)

        write_final_output(1, iter1)
        write_final_output(2, iter2)
        

# --------------------------------------
if __name__ == '__main__':
    #INDICAR aqui el directorio de trabajo
    srr="/home/user/directoriotrabajo"
	
    #Se comprueba si el directorio indicado contiene los archivos necesarios
    if os.path.exists(srr+"/fq_out"):
        start_time = timeit.default_timer()
    
        C = CombineFastas(srr)
        C.run()

        elapsed = timeit.default_timer() - start_time
        print "ELAPSED TIME =", elapsed
    else:
        print("ERROR: " + srr + " doesn't contain /fq_out directory with input files")

                                                   
