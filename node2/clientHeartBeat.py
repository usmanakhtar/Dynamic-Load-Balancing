import Pyro4
import socket
import time


def getIP():
	s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	s.connect(('8.8.8.8', 0))
	return s.getsockname()[0]


def getHostName():
	return socket.gethostname()


try :
	uri = Pyro4.resolve("PYRONAME:heartbeats")
	HBobj = Pyro4.Proxy(uri)

except:
	print ('Something went wrong')



report = {
	'host-name' : getHostName(),
	'ip' : '192.168.56.104'
}

while (1) :
	print(HBobj.updateStatus(report))
	time.sleep(5)
