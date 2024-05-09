#!/usr/bin/python3.8
currentDirectory = "../conf"

def readAllCrawlerIP():
	crawlerList = ['baidu.dat', 'bing.dat', 'facebook.dat', 'google.dat', 'sogou.dat', 'other.dat']
	crawlerIP = []
	for crawler in crawlerList:
		with open(currentDirectory  + '/' + crawler) as input_data:	
			for ipset in input_data.readlines():
				ipset = ipset.strip()
				if (ipset.startswith('#') == True):
					continue
				crawlerIP.append(ipset)
	return crawlerIP


if __name__ == "__main__":
	if len(currentDirectory) == 0 :
		currentDirectory = "."
	print(readAllCrawlerIP())
