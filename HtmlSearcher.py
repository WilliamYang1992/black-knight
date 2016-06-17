#encoding:GBK

import os
import re
import time
import math
import urllib
import  random
import html5lib
import requests
from bs4 import BeautifulSoup
from threading import Thread
from threading import Event 



########################################################################
class SeedHolder():
    """holding the seeds for initial search"""

    #----------------------------------------------------------------------
    def __init__(self, seedList):
        """Constructor"""
        self.seeds = seedList
        
    #----------------------------------------------------------------------
    def addSeed(self, seed):
        """add seed into seedList"""
        self.seeds.append(seed)
        
    #----------------------------------------------------------------------
    def removeSeed(self):
        """remove seed from seedList"""
        self.seeds.pop()
        
    #----------------------------------------------------------------------
    def divideSeeds(self):
        """divide seed from seeds into several groups"""
        seedsPerGroup = math.floor(len(self.seeds) / Configure.THREAD_LIMIT)
        if seedsPerGroup == 0:seedsPerGroup = 1
        seedsGroups = []
        while not self.seeds == []:
            tmp = []
            for i in range(seedsPerGroup):
                tmp.append(self.seeds[0])
                self.seeds.pop(0)
            seedsGroups.append(tmp)
        return seedsGroups
            
        
        
        
        
        
########################################################################
class ThreadController(Thread):
    """base thread"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        Thread.__init__(self)
        
    
        
########################################################################
class Searcher(ThreadController):
    """a single thread for doing searching work"""

    #----------------------------------------------------------------------
    def __init__(self, seeds):
        """Constructor"""
        ThreadController.__init__(self)
        self.seeds = seeds
        self.thread_stop = False
        
    #----------------------------------------------------------------------
    def run(self):
        """overwrite run() method"""
        seeds_sum = 0      #total seed index
        seed_current = 0  #current seed index
        seed_set = set()   #a set to store unique urls
        seed_infos = {}
        for i in range(len(self.seeds)):          
            seed_set.add(self.seeds[i])
            
        validator = UrlValidator()
        
        while not self.thread_stop:
            while seeds_sum < Configure.TOTAL_URL_LIMIT:
                if seed_current < len(self.seeds):
                    Event().wait(3)
                    try:
                        if seed_current == 0:
                            req = requests.get(self.seeds[seed_current], timeout = 120)
                        else:
                            req = requests.get(self.seeds[seed_current], timeout = 10)
                        soup = BeautifulSoup(req.content, "html5lib")
                    except(Exception) as e:
                        print(e)
                        seed_current += 1
                        continue
                    
                    seed_title = "None"
                    seed_url = "None"
                        
                    try:
                        seed_url = self.seeds[seed_current]
                        seed_title = soup.head.title.string.encode('GBK', 'ignore').decode('GBK', 'ignore')
                        print("try: " + seed_url)
                        print('标题: ' + seed_title)
                        print('url: ' +  seed_url+ '\n')
                    except(AttributeError):
                        print("Error_title_url: " + seed_url)
                    try:
                        urls = soup.find_all("a", limit= Configure.SINGLE_URL_RECURSION_LIMIT)
                    except(Exception) as e:
                        print(e)
                        seed_current += 1
                        continue
                    seed_infos.setdefault(seed_title, seed_url)
                    
                    new_urls = validator.CheckUrls(self.seeds[seed_current], urls)

                    for seed in new_urls:
                        if seed not in seed_set:
                            self.seeds.append(seed)
                            seed_set.add(seed)
                            
                    seed_current += 1
                    seeds_sum += 1
                    
                    try:
                        if len(seed_infos) >= Configure.OUTPUT_FREQUENCY:
                            data_txt = open(r'data.txt', mode='a+', buffering= -1)
                            for seed_info in seed_infos.items():
                                data_txt.write("标题: " + seed_info[0])
                                data_txt.write("Url: " + seed_info[1] + '\n')
                            data_txt.flush()
                            data_txt.close()
                            seed_infos.clear()
                    except(Exception) as e:
                        print(e)                     
                    
                else:
                    self.thread_stop = True
                    break
            self.thread_stop = True
            
        
    #----------------------------------------------------------------------
    def stop(self):
        """stop current thread"""
        self.thread_stop = True
        
            
        
        
        
########################################################################
class Configure: 
    """restore the initial parameters"""
    SINGLE_URL_RECURSION_LIMIT = 50
    TOTAL_URL_LIMIT = 1000
    THREAD_LIMIT = 22
    OUTPUT_FREQUENCY = 100

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        
        
########################################################################
class UrlValidator:
    """Validate and alter urls"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        
    #----------------------------------------------------------------------
    def CheckUrls(self, url, a_tags):
        """Check each url in urls, if url has invalid form or not a valid link,
        if will be altered. Return a list of valid urls"""
        urls = []
        match = re.search("(http|ftp)://([^/\r\n]+)?", url)
        if match:
            url = match.group()
        for a_tag in a_tags:
            if a_tag.has_attr('href'):
                if not re.search('^.*(?!java.*?)', a_tag.attrs['href']):
                    continue
                match = re.search('^[http].*[^(doc)|^(pdf)|^(jpg)|^:|^;]$', a_tag.attrs['href'])
                if match:
                    urls.append(a_tag.attrs['href'])
                else:
                    match = re.search('[/]?[a-z].*[^(doc)|^(pdf)|^(jpg)|^:|^;]$', a_tag.attrs['href'])
                    if match:
                        href = match.group(0)
                        if re.search('.*[.]+.*', a_tag.attrs['href']):
                            continue
                        if href.startswith('/'):
                            new_href = url + href
                        else:
                            new_href = url + '/' + href
                        urls.append(new_href)
        if len(urls) > 30:
            return sorted(urls)[18:28]
        elif len(urls) > 20:
            return sorted(urls)[8:18]
        else:
            return sorted(urls)        
        
        
        
        
        
        
########################################################################
class UrlCollector:
    """collect the urls which from url validator"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        
        
########################################################################
class DataExporter:
    """Export url datas to database"""

    #----------------------------------------------------------------------
    def __init__(self, databaseName):
        """Constructor"""
        
    #----------------------------------------------------------------------
    def export(self, urls):
        """export urls"""
        
        
if __name__ == '__main__':
    seedList = ["http://www.hao123.com",
                "http://www.sina.com",
                "http://www.qq.com",
                "http://www.tom.com",
                "http://www.dgut.edu.cn",
                "http://www.7dong.cc",
                "http://www.163.com",
                "http://bbs.tianya.cn",
                "http://www.sohu.com", 
                "http://www.jb51.net",
                "http://tieba.baidu.com",
                "http://www.mtime.com",
                "http://www.piaohua.com",
                "http://www.gamersky.com",
                "http://www.python.org",
                "http://www.taobao.com",
                "http://www.pc6.com",
                "http://www.yinyuetai.com",
                "http://www.jd.com",
                "http://news.qq.com",
                "http://lol.qq.com",
                "http://lvyou.baidu.com"]
    seedList1 = ["http://www.qq.com"]
    seedHolder =  SeedHolder(seedList)
    print("搜索开始" + "'\n")
    for group in seedHolder.divideSeeds():
        print(group)
        Searcher(group).start()
    print('\n')
    
        
        
           
   
    
    
        
    
    
        
    
    
        
        
        
    
    
        
        
        
    
    
        
        
        
        
    
    
        
        
    
    


#if __name__ == '__main__':
    #while sum_seeds < total_limits:
        #if sum_seeds < len(seeds):
            #time.sleep(0.5)
            #try: 
                #req = requests.get(seeds[sum_seeds])
            #except(Exception) as e:
                #print(e)         
            #soup = BeautifulSoup(req.content, "html5lib")
            #try:      
                #title = soup.head.title.string
                #print('标题: ' + title.encode('GBK', 'ignore').decode('GBK', 'ignore'))
                #print('url: ' + seeds[sum_seeds] + '\n')
                #urls = soup.find_all("a", limit= single_seed_limits)
            #except(Exception) as e:
                #print(e)
            #for url in urls:
                #if url.has_attr('href'):        
                    #seed = url.attrs['href']
                #else:
                    #continue
                #if seed.startswith('http'):
                    #seeds.append(seed)
            #sum_seeds += 1
        #else:
            #print(seeds)
            #break
    
    
    