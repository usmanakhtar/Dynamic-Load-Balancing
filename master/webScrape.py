#!/usr/bin/python3i
# ret stores a dictionary {index:[length,noOfLiveNode,listOfIp]}
# listOfIp [local ip, dest ip 1,dest ip 2...]
import urllib.request
import re

class webScraping(object):

    def __init__(self):
        self.data = ""

    ''' resources{} is a map of IP's of node with the block it contains'''
    def readData(self, url):
        ret ={}
        req = urllib.request.Request(url)
        resp = urllib.request.urlopen(req)
        self.data = resp.read().decode("utf-8")
        regex = r"([0-9])\..*len=([0-9]+).*Live_repl=([0-9]+).*\]\]"
        matches = re.finditer(regex,self.data)
        for matchNum , match in enumerate(matches):
            x = match.groups()
            regex =  r"([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)"
            matches_ip = re.finditer(regex,match.group())
            ip_list = []
            for ip_c,ip_s in enumerate(matches_ip):
                y = ip_s.groups()
                ip_list.append(y[0])
            ret[x[0]]=[x[1],x[2],ip_list]

        # print(ret)
        resources = {}
        count = 0
        for i in ret.keys():
            count+=1
            for ip in ret[i][2]:
                if ip not in resources and ip != '127.0.1.1' and ip!= '127.0.0.1':
                    resources[ip] = []
                if ip != '127.0.1.1' and ip!= '127.0.0.1':
                    l = []
                    l.append(i)
                    l.append(ret[i][0])
                    resources[ip].append(l)


        # print(resources)

        return resources, count
