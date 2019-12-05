#!/usr/bin/python
query_list = ['select', 'pw', 'from', 'Iris', 'max', 'pw']
dbtextname= ['Iris.csv.txt']
#dbtextname='dodo.csv.txt'
dbtextname=dbtextname[0]
import os
from collections import OrderedDict

#####################
maxflag=1
minflag=2
sumflag=3


#############

if query_list[3] in dbtextname:


	fp1=open('/home/hadoop/'+dbtextname,'r')
	listcolumn=fp1.readlines()[0]
	fp1.close()

	columndict=OrderedDict()
	listcolumn=listcolumn.split(',')

	index=0
	for i in listcolumn:
		x=i.split(':')
	
		columndict[x[0]]=[x[1],index]
		index+=1
	
	#print(columndict)

	def execvp(args):
		pid=Popen(args,stdout=PIPE,stderr=PIPE)
		(result,err)=pid.communicate()
		if not err:
			return (err)


	for i in range(0,len(query_list)):
		x=query_list[i]
		x.lower()
		query_list[i]=x





	fp=open('/home/hadoop/'+dbtextname,'r')




	if 'sum' in query_list or 'max' in query_list or 'min' in query_list:
		#print("pp")
		if query_list[1] in columndict and query_list[-1] in columndict:	
		
			flag=-1
			
			if 'sum' in query_list:
				flag=3
			elif 'max' in query_list:
				flag=1
			else:
				flag=2

			colcheck=columndict[query_list[1]]
			if colcheck[0]=='int' or colcheck[0]=='float':
				cond=query_list[-3]
				#print(cond)
				if '=' in cond:
					p=cond.split('=')
					#if(len(p)==3):
						
					info1=columndict[p[0]]	
					for i in fp:
						x=i.split(',')
						for j in range(0,len(x)):
							temp=x[j]
							temp=temp.strip()
							temp=temp[1:-1]
							x[j]=temp
						if p[1]== x[info1[1]] :

							print(x[info1[1]],flag)
			
						
			
					

				else: #no condition
					info1=columndict[query_list[-1]]
					ccheck=0
					for i in fp:
						x=i.split(',')
						for j in range(0,len(x)):
							temp=x[j]
							temp=temp.strip()
							temp=temp[1:-1]
							x[j]=temp
						if ccheck!=0:
							print(x[info1[1]],flag)
						ccheck+=1
			
			else:
				print("Operation not supported on this data type",0)
					
									
		



		else:
			print("INVALID COLUMN",0)
	
	











	else:
		flag=0
		#print("in else")
		if (query_list[1] in columndict or query_list[1]=='*') and 'where' in query_list:	
	
			#print("found")
			if '=' in query_list[-2]:
				z=query_list[-1]
				query_list.pop()
				z=query_list[-1]+' '+z
				query_list[-1]=z
			cond=query_list[-1]
		
			p=cond.split('=')
	
		
			if query_list[1]=='*':
				info1=columndict[p[0]]
				for i in fp:
					x=i.split(',')	
				
					for j in range(0,len(x)):
						temp=x[j]
						temp=temp.strip()
						temp=temp[1:-1]
						x[j]=temp
					if(x[info1[1]]==str(p[1])):
						print(x,flag)

			else:
				info1=columndict[p[0]]
				colcheck=columndict[query_list[1]]
				for i in fp:
					x=i.split(',')	
				
					for j in range(0,len(x)):
						temp=x[j]
						temp=temp.strip()
						temp=temp[1:-1]
						x[j]=temp

					if(x[info1[1]]==str(p[1])):
						print(x[colcheck[1]],flag)
			
						
				
				
		
		
		
		
		
		else:
			#print("uuuu")
			if query_list[1]=='*':
				dodo=0
				for i in fp:
					x=i.split(',')
					for j in range(0,len(x)):
						temp=x[j]
						temp=temp.strip()
						temp=temp[1:-1]
						x[j]=temp
					#y=''.join(x)
					if dodo!=0:
						print(x,flag)
					dodo+=1
			else:
				z=query_list[1]
				#print(z,columndict[z])
			
				if z in columndict:
					info=columndict[z]
					dodo=0
					for i in fp:
						x=i.split(',')	

						for j in range(0,len(x)):
							temp=x[j]
							temp=temp.strip()
							temp=temp[1:-1]
							x[j]=temp					

					
						if dodo!=0:
							print(x[info[1]],flag)
						dodo+=1

				else:
					print("Invalid column",flag)
					
		
else:
	print("Invalid table",0)
						
				
				
			
	

