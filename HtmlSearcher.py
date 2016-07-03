#encoding:GBK

#VERSION = 0.81
#AUTHOR: William Yang
#EMAIL: 505741310@qq.com
#WEIBO: weibo.com/yyb1105

import os
import re
import csv
import time
import math
import uuid
import lxml
import html
import codecs
import random
import urllib
import pickle
import pymysql
import sqlite3
import logging
import html5lib
import requests
import threading
from threading import Event
from threading import Thread
from bs4 import BeautifulSoup
from urllib.parse import urlparse



########################################################################
class Configure: 
    """restore the initial parameters"""
    #OUTPUT_STYLE
    TXT_MODE = 0
    PICKLE_MODE = 1
    SQLITE_MODE = 2
    CSV_MODE = 3
    MYSQL_MODE = 4
    POSTGRESQL_MODE = 5
    JSON_MODE = 6
    MONGODB_MODE = 7
    HTML_MODE = 8
    OUTPUT_PATH = r'.\\'
    ###############################
    OUTPUT_STYLE = MYSQL_MODE
    ###############################
    
    #OUTPUT_NAME_STYLE
    USE_UUID = 0
    USE_TIMESTAMP = 1
    USE_DATETIME = 2
    USE_DOMAIN_NAME = 3
    ###############################
    OUTPUT_NAME_STYLE = USE_DOMAIN_NAME
    ###############################
    
    #OBTAIN_URL_STYLE
    BY_ORDER = 0
    BY_SORTED = 1
    BY_RANDOM = 2
    ###############################
    OBTAIN_URL_STYLE = BY_RANDOM
    ###############################
    
    #START_MODE
    START_IN_CMD = 0
    START_IN_GUI = 1
    START_IN_WEB = 2
    ###############################
    START_MODE = START_IN_CMD
    ###############################
    
    #WORK_MODE
    WORK_IN_SINGLETON = 0
    WORK_IN_DISTRIBUTION = 1
    ###############################
    WORK_MODE = WORK_IN_SINGLETON
    ###############################
    
    #THREAD Setting
    SINGLE_URL_SEARCH_LIMIT = 200
    TOTAL_URL_LIMIT = 1000
    THREAD_LIMIT = 22
    OUTPUT_FREQUENCY = 20
    THREAD_WAIT = 0
    
    SEEDLIST = ["http://www.hao123.com",
                "http://www.sina.com",
                "http://www.qq.com",
                "http://www.tom.com",
                "http://www.dgut.edu.cn",
                "http://www.7dong.cc",
                "http://www.163.com",
                "http://bbs.tianya.cn",
                "http://www.sohu.com", 
                "http://www.jb51.net",
                "http://www.kadang.com",
                "http://www.mtime.com",
                "http://www.piaohua.com",
                "http://www.ithome.com",
                "http://www.python.org",
                "http://www.taobao.com",
                "http://www.pc6.com",
                "http://www.yinyuetai.com",
                "http://www.youku.com",
                "http://www.pythontab.com/",
                "http://lol.qq.com/main.shtml",
                "http://lvyou.baidu.com"]
    SEEDLIST1 = ["http://www.gamersky.com"]
    SEEDLIST2 = ["http://lol.qq.com/main.shtml"]
    SEEDLIST3 = ["http://lol.qq.com/main.shtml",
                 "http://bbs.tianya.cn",
                 "http://www.gamersky.com"]
    SEEDLIST5 = ["http://news.qq.com"]
    
    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""


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
        #if number of seeds less than THREAD_LIMIT
        if seedsPerGroup == 0:seedsPerGroup = 1
        seedsGroups = []
        while not self.seeds == []:
            tmp = []
            if len(self.seeds) < seedsPerGroup:
                for i in range(len(self.seeds)):
                    seedsGroups[i].append(self.seeds[i])
                self.seeds.clear()
            else:
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
        
    #----------------------------------------------------------------------
    def __init__(self, seedList= None):
        """Constructor"""
        Thread.__init__(self)
        self.seedList = seedList
        
        
    def generateThreads(self):
        threading.stack_size(8388608)
        seedHolder =  SeedHolder(self.seedList)
        print("Start searching" + "\n")
        print("Threads: " + str(Configure.THREAD_LIMIT) + "\n")
        for group in seedHolder.divideSeeds():
            print(group)
            Searcher(group).start()
        print('\n')        
        
        
        
    
        
########################################################################
class Searcher(ThreadController):
    """a simple thread for doing searching work"""

    #----------------------------------------------------------------------
    def __init__(self, seeds):
        """Constructor"""
        ThreadController.__init__(self)
        self.seeds = seeds
        self.thread_stop = False
        self.total_url_limit = Configure.TOTAL_URL_LIMIT
        self.thread_wait = Configure.THREAD_WAIT
        self.single_url_search_limit = Configure.SINGLE_URL_SEARCH_LIMIT
        
    #----------------------------------------------------------------------
    def run(self):
        """overwrite run() method"""
        global url_count
        seeds_sum = 0     #total valid seed index
        seed_current = 0  #current seed index
        seed_set = set()  #a set to store unique urls
      
        validator = UrlValidator()
        exporter = DataExporter(self.seeds)
        exporter.initial_info()
        threadEvent = Event()
           
        #add initial seeds to seed_set
        for i in range(len(self.seeds)):          
            seed_set.add(self.seeds[i])        
        
        while not self.thread_stop:
            while seeds_sum < self.total_url_limit:
                threadEvent.wait(Configure.THREAD_WAIT)
                if seed_current < len(self.seeds):
                    threadEvent.wait(self.thread_wait)
                    try:
                        if seed_current == 0:
                            req = requests.get(self.seeds[seed_current],
                                               timeout = 120)
                        else:
                            req = requests.get(self.seeds[seed_current],
                                               timeout = 10)
                        if req.status_code is 200 :
                            #soup = BeautifulSoup(req.content, "html5lib")
                            #soup = BeautifulSoup(req.content, "lxml")
                            soup = BeautifulSoup(req.content, "html.parser")
                        else:
                            seed_current += 1
                            continue
                    except(Exception) as e:
                        print(e)
                        seed_current += 1
                        continue
                    
                    #predefine seed_title and seed_url in case of exception
                    seed_title = "None"
                    seed_url = "None"
                        
                    try:
                        seed_title = soup.title.string
                        seed_url= self.seeds[seed_current]
                        print("TRY: " + seed_url)
                        print('TITLE: ' + seed_title)
                        print('URL: ' +  seed_url+ '\n')
                    except(AttributeError):
                        print("AttributeError")
                        seed_current += 1
                        continue
                    except(UnicodeEncodeError):
                        print("UnicodeError")
                        seed_current += 1
                        continue
                    #some url doesn't has title
                    except(TypeError):
                        print("TypeError")
                        seed_current += 1
                        continue                    
                        
                    try:
                        urls = soup.find_all("a", limit= self.
                                             single_url_search_limit)
                    except(Exception) as e:
                        print(e)
                        seed_current += 1
                        continue
                    
                    #add data to dict
                    try:
                        exporter.addData(seed_url, seed_title)
                    except(Exception) as e:
                        error_info = open(r'.\error_info.txt', 'w+', -1)
                        error_info.write(e)
                        error_info.close()
                    #export url data, "seeds_sum" for recording order
                    exporter.export(seeds_sum)                      
                                         
                    
                    new_urls = validator.CheckUrls(self.seeds[seed_current], urls)
                    
                    #check duplicate url. if it is unique then added to seed_set
                    # or abandoned
                    if not len(new_urls) == 0:
                        for seed in new_urls:
                            if seed not in seed_set:
                                self.seeds.append(seed)
                                seed_set.add(seed)
                                
                    url_count += 1        
                    os.system("Title " + str(url_count))
                    seed_current += 1
                    seeds_sum += 1
                                       
                else:
                    exporter.isFinished = True
                    exporter.export(seeds_sum, isFinished= True)
                    exporter.finish_info(exporter.NO_MORE_SEEDS)
                    seed_set.clear()
                    self.thread_stop = True
                    break
            if not exporter.isFinished: 
                exporter.finish_info(exporter.EXCEED_LIMIT)
                seed_set.clear()
                self.thread_stop = True
            break
            
            
        
    #----------------------------------------------------------------------
    def stop(self):
        """stop current thread"""
        self.thread_stop = True
        
            
        
        
        
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
        if Configure.OBTAIN_URL_STYLE == Configure.BY_SORTED:
            count = len(urls)
            try:
                return sorted(urls)[count // 2:count // 2 + count // 10 + 10]
            except(IndexError):
                return sorted(urls)
        elif Configure.OBTAIN_URL_STYLE == Configure.BY_RANDOM:
            count = len(urls)
            try:
                return random.sample(urls, count // 10 + 1)
            except(ValueError):
                return urls
        elif Configure.OBTAIN_URL_STYLE == Configure.BY_ORDER:
            count = len(urls)
            try:    
                return urls[0:count // 10 + 1]
            except(IndexError):
                return urls
        
        
        
########################################################################
class UrlFilter:
    """filter urls which in specific conditions"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        
        
########################################################################
class DataExporter:
    """Export url datas to database"""
    
    NO_MORE_SEEDS = 0
    EXCEED_LIMIT = 1

    #----------------------------------------------------------------------
    def __init__(self, initial_seeds):
        """Constructor"""
        self.seed_infos = {}   #a dict to store tempory titles and urls
        self.initial_seeds = initial_seeds
        self.isFinished = False
        self.output_position = 0
        self.output_style = Configure.OUTPUT_STYLE
        self.txt_mode = Configure.TXT_MODE
        self.output_frequency = Configure.OUTPUT_FREQUENCY
        self.total_url_limit = Configure.TOTAL_URL_LIMIT
        
        if Configure.OUTPUT_NAME_STYLE == Configure.USE_UUID:
            self.urldata_file_name = "URLDATA-" + str(uuid.uuid1())
        elif Configure.OUTPUT_NAME_STYLE == Configure.USE_TIMESTAMP:
            self.urldata_file_name = "URLDATA-" + str(int(time.time()))
        elif Configure.OUTPUT_NAME_STYLE == Configure.USE_DATETIME:
            now = time.strftime("%Y-%m-%d_%H-%M-%S")
            self.urldata_file_name = "URLDATA-" + now
        elif Configure.OUTPUT_NAME_STYLE == Configure.USE_DOMAIN_NAME:     
            self.urldata_file_name = "URLDATA-" + str(urlparse(initial_seeds[0])[1])
        self.urldata_file_name = os.path.join(Configure.OUTPUT_PATH, self.urldata_file_name) 
            
        
    #----------------------------------------------------------------------
    def addData(self, title, url):
        """add data to tempory dict"""
        self.seed_infos.setdefault(title, url)
        
    #----------------------------------------------------------------------
    def initial_info(self):
        """output the initial seeds info which for searching"""
        if self.output_style == Configure.TXT_MODE:
            with open(self.urldata_file_name, "w", -1) as urldata_file:
                urldata_file.write("Initial seeds to search: \n")
                for i in range(len(self.initial_seeds)):
                    urldata_file.write(str(i + 1) + ". " + self.initial_seeds[i] + '\n')
                urldata_file.write('\n')
                urldata_file.write(time.strftime("Start time: %x %X\n\n"))
        elif self.output_style == Configure.CSV_MODE:
            with open(self.urldata_file_name + '.csv', 'a', buffering= 1) as data_csv:
                writer = csv.writer(data_csv)
                writer.writerow(("INDEX", "TITLE", "URL" ))
            
            
    #----------------------------------------------------------------------
    def finish_info(self, result_code):
        """output finished information to the tail of urldata file"""
        if self.output_style == Configure.TXT_MODE:
            if result_code == self.NO_MORE_SEEDS:
                with open(self.urldata_file_name, "a+", -1) as urldata_file:
                    urldata_file.write("\nNo more urls can be found in given seeds.\n")
                    urldata_file.write(time.strftime("End time: %x %X\n"))
            else:
                with open(self.urldata_file_name, "a+", -1) as urldata_file:
                    urldata_file.write("\nThe number of urls exceeds limit of TOTAL_URL_LIMIT({0}).\n".format(Configure.TOTAL_URL_LIMIT))
                    urldata_file.write(time.strftime("\nEnd time: %x %X\n"))
        elif self.output_style == Configure.CSV_MODE:
            pass
        
    #----------------------------------------------------------------------
    def export(self, seeds_sum, isFinished = False):
        """export urls"""
        if len(self.seed_infos) >= self.output_frequency or seeds_sum + 1 == self.total_url_limit or \
        isFinished == True :
            if self.output_style == self.txt_mode:
                self._toTXT(seeds_sum)
            elif Configure.OUTPUT_STYLE == Configure.SQLITE_MODE:
                pass
            elif Configure.OUTPUT_STYLE == Configure.MYSQL_MODE:
                self._toMySQL()
            elif Configure.OUTPUT_STYLE == Configure.CSV_MODE:
                self._toCSV(seeds_sum)
            elif Configure.OUTPUT_STYLE == Configure.JSON_MODE:
                pass
            elif Configure.OUTPUT_STYLE == Configure.MONGODB_MODE:
                pass
            elif Configure.OUTPUT_STYLE == Configure.HTML_MODE:
                pass
            
                    
                    
    #----------------------------------------------------------------------
    def _toTXT(self, seeds_sum):
        """export data to txt format"""
        try:
            data_txt = open(self.urldata_file_name + '.txt', mode='a', buffering= 1)
            for seed_info in self.seed_infos.items():
                data_txt.write(str(self.output_position  + 1) + ". ""TITLE: ")
                #delete space and CRLF in seed_title
                data_txt.write((''.join(seed_info[1].strip().split('\n')).replace(' ', '')))
                data_txt.write(" ")
                data_txt.write("URL: ")
                data_txt.write(seed_info[0])
                data_txt.write("\n")
                self.output_position += 1
        except(IOError):
            print("Can not open or create urldata file!")
        except(UnicodeEncodeError) as e:
            print(e)
        except(Exception) as e:
            print(e)
        finally:
            self.output_position = seeds_sum + 1
            data_txt.flush()
            data_txt.close()
            self.seed_infos.clear()
            
    #----------------------------------------------------------------------
    def _toCSV(self, seeds_sum):
        """export data to csv format"""
        try:
            data_csv = open(self.urldata_file_name + '.csv', 'a', buffering= 1)
        except(IOError):
            print("Can't not open or create data.csv")
        try:
            writer = csv.writer(data_csv)
            for seed_info in self.seed_infos.items():
                csvRow = []
                csvRow.append(str(self.output_position  + 1))
                #delete space and CRLF in seed_title
                csvRow.append((''.join(seed_info[1].strip().split('\n')).replace(' ', '')))
                csvRow.append(seed_info[0])
                writer.writerow(csvRow)
                self.output_position += 1
        except(Exception) as e:
            print(e)
        finally:
            self.output_position = seeds_sum + 1
            data_csv.flush()
            data_csv.close()
            self.seed_infos.clear()
            
    #----------------------------------------------------------------------
    def _toMySQL(self):
        """export data to local or remote MySQL database"""
        tableName = 'urldata'
        databaseName = 'scraping'
        conn = pymysql.connect(host = '127.0.0.1', user = 'root', passwd = '3911965',
            db = 'mysql', charset = 'utf8')
        cur = conn.cursor()
        cur.execute("USE " + databaseName)
        try:
            for seed_info in self.seed_infos.items(): 
                cur.execute("INSERT INTO " + tableName+ " (title,url) VALUES (\"%s\",\"%s\")",
                    (seed_info[1], seed_info[0]))
                cur.connection.commit()           
        except(pymysql.err.InternalError) as e:
            print(e)
        except(pymysql.err.IntegrityError) as e:
            print(e)
        except(pymysql.err.DataError) as e:
            print(e)
        finally:
            cur.close()
            conn.close()
            self.seed_infos.clear()

            
            
        
########################################################################
class Launcher:
    """launch this app in CMD, GUI or WEB according to START_MODE"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.start_mode = Configure.START_MODE
        
    #----------------------------------------------------------------------
    def start(self):
        """start this app"""
        seedList = []
        if self.start_mode == Configure.START_IN_CMD:
            url = input("Please input a valid url, "
                        "leave it blank by default:\n")
            if not url == "":
                if url.startswith('http://'):
                    seedList.append(url)
                else:
                    seedList.append("http://" + url)
            else:
                seedList = Configure.SEEDLIST
            ThreadController(seedList).generateThreads()
                  
   
        
if __name__ == '__main__':
    url_count = 0
    Launcher().start()
    
