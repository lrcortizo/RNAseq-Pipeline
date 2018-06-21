#!/usr/bin/env python

import os

class DivideFiles:

	def __init__(self, nb, lines, name1, name2):
		self.num_blocks = nb
		self.last_rec = int(lines/4/float(num_blocks))
		self.num_lines = lines
		self.fname1 = name1
		self.fname2 = name2
		
		#Se crea el script sed_divide.sh, opcion 'w' para sobreescribir por si ya existe uno creado anteriormente
		outfile = open('sed_divide.sh', 'w')
		outfile.write('#!/bin/bash\n')
		outfile.close()

	
	#Funcion que calcula y genera un script con las lineas por donde se va a trocear el archivo con el comando sed
	def generate_sedlines(self):
		fname = self.fname1
		#Opcion 'a' para ir agregando lineas a continuacion
		outfile = open('sed_divide.sh', 'a')

		#Se generan las lineas sed para los dos archivos
		for j in range(2):
			cnt=1
			outfile.write('\n')
			for i in range(self.num_blocks-1):

				#Se calcula la linea final de cada trozo
				cnt_next = cnt + self.last_rec*4  - 1

				#Se genera el sed correspondiente y se agrega al script
				line = "echo %d" % (i)
				outfile.write(line+"\n")
				line = "sed -e '%s,%s!d' %s > %s_%d" % (cnt,cnt_next, fname, fname,i)
				outfile.write(line+"\n")

				#Siguiente trozo: linea inicial = linea final+1
				cnt =  cnt_next +1

			#El ultimo archivo dividido hasta la linea final
			line = "echo %d" % (self.num_blocks-1)
			outfile.write(line+"\n")
			line = "sed -e '%s,%s!d' %s > %s_%d" % (cnt,self.num_lines, fname, fname, self.num_blocks-1)
			outfile.write(line+"\n")

			fname = self.fname2
		outfile.close()


	#Funcion que ejecut el script creado
	def sed_divide(self):
		#Antes de ejecutar, se dan permisos de ejecucion para evitar errores
		os.system('chmod 777 sed_divide.sh')
		os.system('./sed_divide.sh')

			
# --------------------------------------
if __name__ == '__main__':

#CONFIGURAR LOS PARAMETROS SIGUIENTES:
	#INDICAR el numero de archivos en los que se van a dividir, por ejemplo 24
	num_blocks=24
	#INDICAR el archivo(parte izquierda pair-end) que se va a dividir, por ejemplo SRR1550981_1.fastq
	fname1="SRR1550983_1.fastq"
	#INDICAR el archivo(parte derecha pair-end) que se va a dividir, por ejemplo SRR1550981_2.fastq
	fname2="SRR1550983_2.fastq"

	#Si existen los archivos indicados, se procede a su division
	if os.path.isfile(fname1) and os.path.isfile(fname1):
		#Se calcula el numero de lineas de los archivos de entrada
		lines=sum(1 for line in open(fname1))
		#Se crea la clase con los parametros indicados y se llama a las funciones que realizan la division
		D = DivideFiles(num_blocks, lines, fname1, fname2)
		D.generate_sedlines()
		D.sed_divide()
	else:
		print "ERROR input files don't found"


