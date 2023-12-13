import Pyro4
import subprocess
import os
import socket

def getIP():
	s = s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	s.connect(('8.8.8.8', 0))
	return s.getsockname()[0]

@Pyro4.expose
class mapReduceServer():
	
	def runMapReduce(self, offset, size, inputName, outputName, mapperName): #runnerName.jar runnerName.class
		cmd = "hadoop jar " + mapperName + ".jar " + mapperName + " " + offset + " " + size + " " + inputName + " " + outputName
		returnStatus = os.system(cmd)
		return returnStatus

	def initJob(self, fileName):
		cmd = "hdfs dfs -get " + fileName + ".jar"
		os.system(cmd)
		cmd = "hdfs dfs -get " + fileName + ".class"
		returnStatus = os.system(cmd)
		return returnStatus

	def runReduceTask(self, inputName, outputName, reducerName, noOfMappers):
		cmd = "hadoop jar " + reducerName + ".jar " + reducerName + " " + noOfMappers + " " + inputName + " " + outputName
		returnStatus = os.system(cmd)
		return returnStatus
		
host = '192.168.56.104'
port = 4999
daemon = Pyro4.Daemon(host=host, port=port)


# ip = getIP()
ip = "192.168.56.104"
ns = Pyro4.locateNS()
uri = daemon.register(mapReduceServer)
ns.register("mapReduceRunner"+ip, uri)
print("Listening . . .")
daemon.requestLoop()		
