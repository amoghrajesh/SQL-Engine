#!/usr/bin/python


settoprint=[]
import sys
c=0
for line in sys.stdin:
	
	settoprint.append(line.split())


flag=settoprint[0][-1]
#print(flag)

if flag=='0':

	for i in settoprint:
		x=''.join(i)
		x=x[:-1]
		#print(x)
		if x[0]=='[':
			print(x[1:-1])
		else:
			print(x)


elif flag=='1':
	datatype=settoprint[-1][0]
	maxprint=-(10**9)
	if '.' in datatype:
		print("i m here")
		for i in settoprint:
			maxprint=max(maxprint,float(i[0]))

	else:
		for i in settoprint:
			maxprint=max(maxprint,float(i[0]))
	print("\t Max")
	print("\t",maxprint)



elif flag=='2':
	datatype=settoprint[-1][0]
	minprint=10**9
	if '.' in datatype:
		for i in settoprint:
			minprint=min(minprint,float(i[0]))

	else:
		for i in settoprint:
			minprint=min(minprint,float(i[0]))
	print("\t Min ")
	print("\t",minprint)

else:
	datatype=settoprint[-1][0]
	sumtoprint=0
	if '.' in datatype:
		for i in settoprint:
			sumtoprint+=float(i[0])

	else:
		for i in settoprint:
			sumtoprint+=float(i[0])
	print("\t Sum")
	print("\t",sumtoprint)

