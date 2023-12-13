import Pyro4
import subprocess
import os
import sys
from resourceReportClient import *


def mapperClient (ip, inputName, offset, size, outputName, mapperName):
	try :
		HBobj = Pyro4.Proxy("PYRONAME:mapReduceRunner"+ip)
	except:
		print ('Something went wrong:(')

	print ("Running Mapper Task...")

	status = HBobj.runMapReduce(offset, size, inputName, outputName, mapperName)
	print ("Mapper Task Completed")
	return status


def reducerClient (ip, outputName, reducerName, count):
	try :
		HBobj = Pyro4.Proxy("PYRONAME:mapReduceRunner"+ip)
	except:
		print ('Something went wrong:(')

	print ("Running Reducer Task...")
	
	status = HBobj.runReduceTask(outputName, "reducer_out.txt", reducerName, count)
	print ("Reducer Task Completed")
	return status


# mapperClient ('192.168.56.103', 'input.txt', '0', '10000', 'mapper_out0.txt', 'Mapper')
# reducerClient ('192.168.56.103', 'mapper_out', 'Reducer', '1')


# print(getResourceStatusOfDataNodes())

# if __name__=="__main__":
# 	offset = 0
#
# 	count = len(final)
#
# 	for i in final:
# 		print("Running Mapper", i,"...")
# 		while (mapperClient(i[1], inputName, str(offset), i[2], output+i[0]+".txt", mapperName) != 0):
# 			pass
#
# 		offset = offset + int(i[2])
# 		print("Mapper", i, "completed")
#
# 	while (reducerClient(i[1], output, reducerName, str(count)) != 0) :
# 		pass
#
# 	print ("Job completed! :)")
