#import pydoop.hdfs as hdfs
from collections import OrderedDict
from subprocess import *
import sys
import threading
from threading import Thread
import csv
import os
from time import *
from random import *
#import threading
#from threading import Thread
#result output
#run_cmd execvp
#args_list args
#
global cont
cont=1

def space(x):
	for i in range(0,x):
		print()

def change(t):
	global cont
	if t==1:
		cont=1
	else:
		cont=0



def entry():
	print("select option")
	print("1. Load a new database")
	print("2. Query ")
	space(2)
	

def ask():
	print("Enter Y if u want to continue")
	print("Else enter N")
	x=input()
	x.lower()
	if(x=='y'):
		change(1)
		space(4)
	else:
		change(0)
		
	

def ggg():
	pass
	
	
def intro():
	for i in range(0,10):
		print("*",end="")

	print("SQL Engine",end="")


	for i in range(0,10):
		print("*",end="")
	space(3)





def execvp(args):
	pid=Popen(args,stdout=PIPE,stderr=PIPE)
	(result,err)=pid.communicate()
	if not err:
		return (err)

def y():
	os.system('python3 /home/hadoop/selectmapper.py | python3 /home/hadoop/selectreducer.py')




intro()
l=list(range(1000,49,-1))
while(cont):
	entry()
	choice=int(input())
	if choice==1:
		
		
		#th1=Thread(target=gg)
		#th1.start()
		print("Enter load command")
		command=input().split()
		start=time()
		com=command[1].split('/')
		schema=command[3]   #schema
		dbname=com[0]
		csvname=com[1]

		createdb=['hdfs','dfs','-mkdir']
		dbstring='/'+str(dbname)
		createdb.append(dbstring)
		execvp(createdb)
		print("dir created")

		
		args=['hdfs','dfs','-put','/home/hadoop/bigdata/']
		args.append(csvname)
		s='/'+dbname+'/'
		args.append(s)
		execvp(args)
			
		print("Done")

		
	
		schema=schema[1:-1]
		#schemalist=[]
		#schemalist.append(schema)
		print(schema)
		fp=open(str(csvname)+'.txt','w')
		fp.write(schema)
		fp.write("\n")
		#print("CSVNAME",csvname)
		with open(csvname,'r') as f:
			data=csv.reader(f)
			for r in data:
				x=str(r)
				x=x[1:-1]
				fp.write(x)
				fp.write("\n")
				
		fp.close()
		args_schema = ['hdfs','dfs','-copyFromLocal']
		args_schema.append('/home/hadoop/'+ str(csvname)+'.txt')
		args_schema.append('/'+str(dbname))
		execvp(args_schema)
		print("Schema Created")

		args1=['hdfs','dfs','-put','/home/hadoop/bigdata/']
		args1.append(str(csvname)+'.txt')
		s1='/'+dbname+'/'
		args1.append(s1)
		execvp(args1)
		
		#print("final hadoop")
		end=time()
		tofex=end-start
		print("Successfully executed in: %s seconds " % (tofex) )
		
	
		
	
		
				


	elif choice==2:
		print("Enter the database name")
		dbname=input() #bigdata
		#print("Enter table name")
		#csvname=input()
		print("Enter query")
		query=input().split()
		start=time()
		csvtablename=[str(dbname)+'.csv.txt']
		with open("/home/hadoop/selectmapper.py") as f:
			lines=f.readlines()
		#print(lines)
		lines[1]='query_list = '+ str(query) + '\n'
		lines[2]='dbtextname= '+str(csvtablename)+'\n'
		#dbtxtname=str(csvname)+'.txt'
		#lines[2]='dbtextname=' +' +'\n'
		
		with open("/home/hadoop/selectmapper.py",'w') as f:
			f.writelines(lines)
		

				
		#args2=['python3','/home/hadoop/selectmapper.py']
		#execvp(args2)
		
		#os.system('python3 /home/hadoop/selectmapper.py | python3 /home/hadoop/selectreducer.py')

		inp='/bigdata/'+str(csvtablename[0])
		#print("inp::::::::::::::",inp)




		'''cmd = ['hadoop', 'jar', '/home/hadoop/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.0.jar'
        , '-file' ,'/home/hadoop/selectmapper.py' ,'-mapper' ,'\"python3 selectmapper.py\"',
         '-file','/home/hadoop/selectreducer.py','-reducer' ,'\"python3 selectreducer.py\"', '-output',
          '/bigdata/2', '-input']'''
		

		
		r=l[-1]
		l.pop()
		print("Generated file: ",r)
		cmd = ['hadoop', 'jar', '/home/hadoop/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.0.jar'
        , '-file' ,'/home/hadoop/selectmapper.py' ,'-mapper' ,'\"python3 selectmapper.py\"',
         '-file','/home/hadoop/selectreducer.py','-reducer' ,'\"python3 selectreducer.py\"', '-output',
          '/bigdata/'+str(r), '-input']



		cmd.append(inp)
		#print(cmd)
#		execvp(cmd
		print(' '.join(cmd))
		os.system(' '.join(cmd))
		end=time()
		#os.system('python3 /home/hadoop/selectmapper.py | python3 /home/hadoop/selectreducer.py')
		y()
		#print("Successfully executed query")
		tofex=end-start
		print("Successfully executed in: %s seconds " % (tofex) )


	else:
		print("Wrong Option")
	ask()
		

	
	



	



