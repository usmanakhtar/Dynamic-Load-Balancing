import Pyro4
import socket


def getResourceStatusOfDataNodes() :
	result = {}

	try :
		HBobj = Pyro4.Proxy("PYRONAME:heartbeats")

	except:
		print ('Something went wrong')
		return result

	datanodeInfo = HBobj.returnLiveNodes()

	for dataNode in datanodeInfo.keys():
		HBobj = Pyro4.Proxy("PYRONAME:monitorModule" +datanodeInfo[dataNode]['ip'])
		resource = {}
		resource['cpu'] = HBobj.getCPUinfo()
		resource['ram'] = HBobj.getRAMinfo()
		resource['disk'] = HBobj.getDISKinfo()
		result[datanodeInfo[dataNode]['ip']] = resource

	return result




