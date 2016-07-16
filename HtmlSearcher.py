#encoding:GBK

##############################
## VERSION = 0.878          ##
## AUTHOR: William Yang     ##
## EMAIL: 505741310@qq.com  ##
## WEIBO: weibo.com/yyb1105 ##
##############################

import os
import re
import csv
import time
import math
import uuid
import lxml
import html
import random
import urllib
import pickle
import pymysql
import sqlite3
import pymongo
import html5lib
import requests
import threading
from threading import Event
from threading import Thread
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from colorama import init, Fore, Back


########################################################################
class Configure: 
    """Store the initial parameters"""
    
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
    BASE_DIRECTORY = 'urldata'
    #####################################
    OUTPUT_STYLE = MYSQL_MODE
    #####################################
    
    #OUTPUT_NAME_STYLE
    USE_UUID = 0
    USE_TIMESTAMP = 1
    USE_DATETIME = 2
    USE_DOMAIN_NAME = 3
    #####################################
    OUTPUT_NAME_STYLE = USE_DOMAIN_NAME
    #####################################
    
    #OBTAIN_URL_STYLE
    BY_ORDER = 0
    BY_SORTED = 1
    BY_RANDOM = 2
    #####################################
    OBTAIN_URL_STYLE = BY_RANDOM
    #####################################
    
    #COLLECT_MODE
    TITLE_DATA = 0
    JPG_DATA = 1
    GIF_DATA = 2
    IMAGE_DATA = 3
    CONTENT_DATA = 4
    #####################################
    COLLECT_MODE = TITLE_DATA
    #####################################
    
    #URL_FILTER_MODE
    NO_RESTRICTION = 0
    DOMAIN_RESTRICTION = 1
    #####################################
    URL_FILTER_MODE = DOMAIN_RESTRICTION
    #####################################    
    
    #URL_FILTER_ATTRIBUTES
    UNLIMITED = 0
    ############################### 
    URL_LENGTH_LIMIT = UNLIMITED
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
    
    #WRITE_MODE
    INDEPENDENCE = 0
    COOPERATION = 1
    COMMON_URLDATA_FILENAME = 'URLDATA'
    ###############################
    WRITE_MODE = COOPERATION
    ###############################
    
    #THREAD Setting
    SINGLE_URL_SEARCH_LIMIT = UNLIMITED
    TOTAL_URL_LIMIT = 5000
    THREAD_LIMIT = 10
    OUTPUT_FREQUENCY = 50
    THREAD_WAIT = 0
    USE_PHANTOMJS = False
    SCRAP_AT_ALL = True
    SCRAP_AT_ALL_RECURSION = 500
    
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
                "http://www.pythontab.com",
                "http://lol.qq.com/main.shtml",
                "http://lvyou.baidu.com"]
    SEEDLIST1 = ["http://www.gamersky.com"]
    SEEDLIST2 = ["http://lol.qq.com/main.shtml"]
    SEEDLIST3 = ["http://lol.qq.com/main.shtml",
                 "http://bbs.tianya.cn",
                 "http://www.gamersky.com"]
    SEEDLIST5 = ["http://news.qq.com"]
    SEEDLIST6 = ["http://people.mtime.com/1256028",
                 "http://www.qq.com",
                 "http://www.pc6.com"]
    SEEDLIST7 = ["http://www.qq.com"]
    SEEDLIST8 = ["www.gamersky.com",
                 "www.gamersky.com",
                 "www.gamersky.com",
                 "www.gamersky.com",
                 "www.gamersky.com",
                 "www.gamersky.com",
                 "www.gamersky.com",
                 "www.gamersky.com",
                 "www.gamersky.com",
                 "www.gamersky.com",
                 "www.gamersky.com",
                 "www.gamersky.com",
                 "www.gamersky.com",
                 "www.gamersky.com",
                 "www.gamersky.com",
                 "www.gamersky.com",
                 "www.gamersky.com",
                 "www.gamersky.com",
                 "www.gamersky.com",
                 "www.gamersky.com"]
    
    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""


########################################################################
class SeedsHolder():
    """Store and hold seeds which for initial search"""

    #----------------------------------------------------------------------
    def __init__(self, seedList):
        """Constructor"""
        self.seeds = seedList
        
    #----------------------------------------------------------------------
    def addSeed(self, seed):
        """Add seed into seedList"""
        self.seeds.append(seed)
        
    #----------------------------------------------------------------------
    def removeSeed(self):
        """Remove seed from seedList"""
        self.seeds.pop()
        
    #----------------------------------------------------------------------
    def divideSeeds(self, mode):
        """Divide seed from seeds into several groups"""
        if mode == Configure.INDEPENDENCE:
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
        elif mode == Configure.COOPERATION:
            return self.seeds
            
        
       
########################################################################
class ThreadController(Thread):
    """Base class of threads, controll how a thread runs and quantities of threads"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        Thread.__init__(self)
        
    #----------------------------------------------------------------------
    def __init__(self, seedList= None):
        """Constructor"""
        Thread.__init__(self)
        self.seedList = seedList
        
    #----------------------------------------------------------------------    
    def generateThreads(self):
        """Generate some threads which depend on THREAD_LIMIT and the number of initial seeds"""
        global all_url_seeds
        #threading.stack_size(8388608)  #Ajust thread stack size
        seedsHolder =  SeedsHolder(self.seedList[:])
        print(Fore.LIGHTYELLOW_EX + "Start searching" + "\n")
        print(Fore.LIGHTYELLOW_EX + "Threads: " + str(Configure.THREAD_LIMIT) + "\n")
        if Configure.WRITE_MODE == Configure.INDEPENDENCE:
            for group in seedsHolder.divideSeeds(Configure.INDEPENDENCE):
                print(Fore.LIGHTYELLOW_EX + repr(group))
                searcher = Searcher(group)
                searcher.start()
                #searcher.join()
            print('\n')
        elif Configure.WRITE_MODE == Configure.COOPERATION:
            all_url_seeds = seedsHolder.divideSeeds(Configure.COOPERATION)
            DataExporter.exportRunningLog(self.seedList, 'start', None)
            for i in all_url_seeds:
                searcher = Searcher([])
                searcher.start()
            for i in self.seedList:
                print(i)
            print('\n')
        
        
        
    
        
########################################################################
class Searcher(ThreadController):
    """Simple thread that doing specific searching work"""

    #----------------------------------------------------------------------
    def __init__(self, seeds):
        """Constructor"""
        ThreadController.__init__(self)
        self.seeds = seeds
        self.thread_stop = False
        self.total_url_limit = Configure.TOTAL_URL_LIMIT
        self.thread_wait = Configure.THREAD_WAIT
        self.single_url_search_limit = Configure.SINGLE_URL_SEARCH_LIMIT
        self.write_mode = Configure.WRITE_MODE
        self.output_style = Configure.OUTPUT_STYLE
        self.tableName = 'urldata'
        self.databaseName = 'scraping'        
        if self.output_style == Configure.MYSQL_MODE:
            self.conn = pymysql.connect(host = '127.0.0.1', user = 'root', passwd = '3911965',
                db = 'mysql', charset = 'utf8mb4')
            self.cur = self.conn.cursor()
            self.cur.execute("USE " + self.databaseName)        
        
    #----------------------------------------------------------------------
    def run(self):
        """Overwrite run() method. It is main flow os each threads"""
        global all_url_count
        global all_url_set
        global all_url_seeds
        
        seeds_sum = 0     #total valid seed index
        seed_current = 0  #current seed index
        seed_set = set()  #a set to store unique urls
      
        validator = UrlValidator()
        exporter = DataExporter(self.seeds)
        exporter.initial_action()
        threadEvent = Event()
        mutex = threading.Lock()
        
        if self.write_mode == Configure.INDEPENDENCE:
            urlfilter = UrlFilter(self.seeds[0])
            #add initial seeds to seed_set
            for i in range(len(self.seeds)):          
                seed_set.add(self.seeds[i])             
        elif self.write_mode == Configure.COOPERATION:
            urlfilter = UrlFilter(all_url_seeds[0])
           
 
        while not self.thread_stop:
            while seeds_sum < self.total_url_limit:
                threadEvent.wait(Configure.THREAD_WAIT)
                if seed_current < len(self.seeds) or len(all_url_seeds) != 0:
                    threadEvent.wait(self.thread_wait)
                    #tag_A
                    try:
                        if self.write_mode == Configure.INDEPENDENCE:
                            if (seed_current == 0):
                                req = requests.get(self.seeds[seed_current],
                                    timeout = 120)
                            else:
                                req = requests.get(self.seeds[seed_current],
                                    timeout = 10)
                        elif self.write_mode == Configure.COOPERATION:
                            try:
                                mutex.acquire()
                                seed_from_all_url_list = all_url_seeds.pop(0)
                                mutex.release()
                                req = requests.get(seed_from_all_url_list)
                            except(Exception) as e:
                                with open("error_info.txt", 'a', buffering= 1) as err:
                                    err.write("Error: run() requests\n" + repr(e) + '\n\n')
                                continue
                        if req.status_code is 200 :
                            #lowest speed
                            #soup = BeautifulSoup(req.content, "html5lib")
                            #fastet speed but has some bug
                            #soup = BeautifulSoup(req.content, "lxml")
                            #midium speed
                            soup = BeautifulSoup(req.content, "html.parser")
                        else:
                            if self.write_mode == Configure.INDEPENDENCE:
                                seed_current += 1
                            continue
                    except(ConnectionError):
                        if self.write_mode == Configure.INDEPENDENCE:
                            seed_current += 1
                            #when a connection error occurs, append current seed to tail of seeds list
                            self.seeds.append(self.seeds[seed_current])
                        elif self.write_mode == Configure.COOPERATION:
                            all_url_seeds.append(seed_from_all_url_list)
                        continue
                    except(Exception) as e:
                        print(Fore.LIGHTRED_EX + repr(e))
                        with open("error_info.txt", 'a', buffering= 1) as err:
                            err.write("Error: run() tag_A\n" + repr(e) + '\n\n')
                        if self.write_mode == Configure.INDEPENDENCE:
                            seed_current += 1
                        continue
                    
                    #predefine seed_title and seed_url in case of exception
                    seed_title = "None"
                    seed_url = "None"
                        
                    try:
                        
                        if self.write_mode == Configure.INDEPENDENCE:  
                            seed_url= self.seeds[seed_current]
                        else:
                            seed_url = seed_from_all_url_list
                        seed_title = soup.title.string
                        #soup.title.string.encode('GBK', 'ignore').decode('GBK', 'ignore')
                        #seed_title = soup.title.string.encode('GB2312', 'ignore').decode('GB18030', 'ignore')
                        #seed_title = bytes(soup.title.string, 'utf-8').decode('utf-8', 'ignore')
                        #print( + "TRY: " + seed_url)
                        print(Fore.LIGHTCYAN_EX+ 'TITLE: ' +
                              seed_title.encode('GBK', 'ignore').decode('GBK', 'ignore'))
                        #print('TITLE: ' + seed_title)
                        print(Fore.LIGHTGREEN_EX + 'URL: ' +  seed_url+ '\n')
                        
                        #seed_url = self.seeds[seed_current]
                        #seed_title = soup.head.title.string.encode('GBK', 'ignore').decode('GBK', 'ignore')
                        #print("Try: " + seed_url)
                        #print('Title: ' + seed_title)
                        #print('Url: ' +  seed_url+ '\n')
                    except(AttributeError) as e:
                        print(Fore.LIGHTRED_EX + "AttributeError")
                        print(Fore.LIGHTRED_EX + seed_url + '\n')
                        if self.write_mode == Configure.INDEPENDENCE:
                            seed_current += 1
                        with open("error_info.txt", 'a', buffering= 1, encoding= 'GB18030') as err:
                            err.write("AttributeError: run()\n" + repr(e) + "\n" + "Url: "
                                + seed_url + '\n\n')
                        continue
                    except(UnicodeEncodeError) as e:
                        print(Fore.LIGHTRED_EX + "UnicodeError\n")
                        if self.write_mode == Configure.INDEPENDENCE:
                            seed_current += 1
                        #seed_title = soup.title.string.decode('GB2312', 'ignore')
                        #seed_title = soup.title.string.decode('GB2312', 'replace').encode('GBK', 'replace')
                        with open("error_info.txt", 'a', buffering= 1, encoding= 'GB18030') as err:
                            err.write("UnicodeError: run()\n" + repr(e) + "\n" + "Url: "
                                + seed_url + '\n\n')
                        continue
                    #some url doesn't has title
                    except(TypeError) as e:
                        print(Fore.LIGHTRED_EX + "TypeError\n")
                        if self.write_mode == Configure.INDEPENDENCE:
                            seed_current += 1
                        with open("error_info.txt", 'a', buffering= 1, encoding= 'GB18030') as err:
                            err.write("TypeError: run()\n" + repr(e) + "\n" + "Url: "
                                + seed_url + '\n\n')
                        continue
                                                                      
                        
                    #Find all links in current url
                    try:
                        if Configure.SINGLE_URL_SEARCH_LIMIT == Configure.UNLIMITED:
                            urls = soup.find_all("a")
                        else:
                            urls = soup.find_all("a", limit= self.single_url_search_limit)
                    except(Exception) as e:
                        with open("error_info.txt", 'a', buffering= 1) as err:
                            err.write("Error: run()\n" + repr(e) + '\n\n')
                        if self.write_mode == Configure.INDEPENDENCE:
                            seed_current += 1
                        continue
                    
                                 
                    #Add data to dict
                    exporter.addData(seed_url, seed_title)
                    
                    if Configure.COLLECT_MODE == Configure.TITLE_DATA:
                        #Export title-url data to specific path, "seeds_sum" for recording order
                        exporter.export_titledata(seeds_sum)
                    elif Configure.COLLECT_MODE == Configure.JPG_DATA:
                        #Collect jpgs from all of links and exports to specific path
                        exporter.export_jpgdata()
                        
                    #Validate and reform url which is incorrect format
                    checked_urls = validator.CheckUrls(seed_url, urls)
                    #Filter url with specific conditions
                    filtered_urls = urlfilter.filters(checked_urls)
                    
                    #Check duplicate url. if it is unique then added to seed_set
                    # or abandoned
                    if not self.output_style == Configure.MYSQL_MODE and not \
                       self.output_style == Configure.MONGODB_MODE:
                        if not len(filtered_urls) == 0:
                            if self.write_mode == Configure.INDEPENDENCE:
                                for seed in filtered_urls:
                                    if seed not in seed_set:
                                        self.seeds.append(seed)
                                        seed_set.add(seed)
                            else:
                                for seed in filtered_urls:
                                    if seed not in all_url_set:
                                        all_url_seeds.append(seed)
                                        all_url_set.add(seed)
                    else:  #Use database to filtered duplicated urls
                        if not len(filtered_urls) == 0:
                            if self.write_mode == Configure.INDEPENDENCE:
                                pass
                            elif self.write_mode == Configure.COOPERATION:
                                for seed in filtered_urls:
                                    select_stmt = "SELECT * from " + self.tableName + \
                                        " WHERE url = \"'" + seed + "'\""
                                    if self.cur.execute(select_stmt):
                                        #print(Fore.LIGHTWHITE_EX + 'Url already exist: ' + seed + '\n')
                                        continue
                                    if seed not in all_url_set:
                                        all_url_seeds.append(seed)
                                        all_url_set.add(seed)
                                    
                    #Output the number of urls that have been found by all threads                    
                    all_url_count += 1
                    os.system("Title Count:" + str(all_url_count) + '   Recursion:' + str(Launcher.recursion))
                    if self.write_mode == Configure.INDEPENDENCE:
                        seed_current += 1                      
                    seeds_sum += 1
                                       
                else:
                    if Configure.SCRAP_AT_ALL:
                        if not Launcher.re_collect and \
                           Launcher.recursion <= Configure.SCRAP_AT_ALL_RECURSION:
                            Launcher.re_collect = True
                            all_url_seeds = Launcher.randomSeeds()
                        continue
                    exporter.isFinished = True
                    exporter.export_titledata(seeds_sum, isFinished= True)
                    if self.write_mode == Configure.INDEPENDENCE:
                        exporter.finish_action(exporter.NO_MORE_SEEDS)
                    else:
                        DataExporter.exportRunningLog(None, 'end',
                            DataExporter.NO_MORE_SEEDS)
                    seed_set.clear()
                    self.thread_stop = True
                    break
            #If in case of exceeds limit of urls rather not found more urls
            if not exporter.isFinished:  
                
                if self.write_mode == Configure.INDEPENDENCE:
                    exporter.finish_action(exporter.EXCEED_LIMIT)
                elif self.write_mode == Configure.COOPERATION:
                    DataExporter.exportRunningLog(None, 'end',
                        DataExporter.EXCEED_LIMIT)
                seed_set.clear()
                self.thread_stop = True
            break
            
            
        
    #----------------------------------------------------------------------
    def stop(self):
        """Stop current thread"""
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
                    match = re.search('[/]?[a-z].*[^(doc)|^(pdf)|^(jpg)|^:|^;]$',
                        a_tag.attrs['href'])
                    if match:
                        href = match.group(0)
                        if re.search('.*[.]+.*', a_tag.attrs['href']):
                            continue
                        if href.startswith('/'):
                            new_href = url + href
                        else:
                            new_href = url + '/' + href
                        urls.append(new_href)

        #with open('urls.txt', 'w') as url_txt:
            #url_txt.write(repr(urls))
        if Configure.OBTAIN_URL_STYLE == Configure.BY_SORTED:
            count = len(urls)
            try:
                return sorted(urls)[count // 2:count // 2 + count // 10 + 10]
            except(IndexError) as e:
                with open("error_info.txt", 'a', buffering= 1) as err:
                    err.write("IndexError: \n" + repr(e) + '\n\n')
                return sorted(urls)
        elif Configure.OBTAIN_URL_STYLE == Configure.BY_RANDOM:
            count = len(urls)
            try:
                return random.sample(urls, count // 10 + 1)
            except(ValueError) as e:
                with open("error_info.txt", 'a', buffering= 1) as err:
                    err.write("ValueError: \n" + repr(e) + '\n\n')
                return urls
        elif Configure.OBTAIN_URL_STYLE == Configure.BY_ORDER:
            count = len(urls)
            try:
                return urls[0:count // 10 + 1]
            except(IndexError) as e:
                with open("error_info.txt", 'a', buffering= 1) as err:
                    err.write("IndexError: \n" + repr(e) + '\n\n')
                return urls
        
        
        
########################################################################
class UrlFilter:
    """Filter urls which in specific conditions"""

    #----------------------------------------------------------------------
    def __init__(self, url):
        """Constructor"""
        self.url_filter_mode = Configure.URL_FILTER_MODE
        self.url_length_limit = Configure.URL_LENGTH_LIMIT
        self.domainName = self._getDomainName(url)
        
    
    #----------------------------------------------------------------------
    def _getDomainName(self, url):
        """Get domain name"""
        match = re.search("http://(.*)\.com", url)
        if match:
            domainName = match.group(1).split('.')[1]
            return domainName
        return ""
        
    
    #----------------------------------------------------------------------
    def filters(self, urls):
        """Filter urls and return them"""
        filtered_urls = []
        for url in urls:
            if self._checkUrl(url):
                filtered_urls.append(url)
        return filtered_urls
                
                
    #----------------------------------------------------------------------
    def _checkUrl(self, url):
        """Check url whether is meets the requirement of Configure"""
        if self.url_filter_mode == Configure.DOMAIN_RESTRICTION:
            if self.domainName + '.com/' in repr(url) and repr(url).count("http") == 1:
                conditionA = True
            else:conditionA = False
                
        if self.url_length_limit == Configure.UNLIMITED:
            conditionB = True
        else:
            conditionB = (len(repr(url)) < self.url_length_limit and True or False)
            
        #if meets with all conditions will retrun True
        return conditionA and conditionB
        
        
        
        
        
########################################################################
class DataExporter:
    """Export url datas to database"""
    
    NO_MORE_SEEDS = 0
    EXCEED_LIMIT = 1
    all_finished = False

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
        self.write_mode = Configure.WRITE_MODE
        self.tableName = 'urldata'
        self.databaseName = 'scraping'
        if self.write_mode == Configure.COOPERATION and self.output_style == Configure.MYSQL_MODE:
            self.conn, self.cur = self._getMySQLConnection()
        
        if self.write_mode == Configure.INDEPENDENCE:  
            if Configure.OUTPUT_NAME_STYLE == Configure.USE_UUID:
                self.urldata_file_name = "URLDATA-" + str(uuid.uuid1())
            elif Configure.OUTPUT_NAME_STYLE == Configure.USE_TIMESTAMP:
                self.urldata_file_name = "URLDATA-" + str(int(time.time()))
            elif Configure.OUTPUT_NAME_STYLE == Configure.USE_DATETIME:
                now = time.strftime("%Y-%m-%d_%H-%M-%S")
                self.urldata_file_name = "URLDATA-" + now
            elif Configure.OUTPUT_NAME_STYLE == Configure.USE_DOMAIN_NAME:
                self.urldata_file_name = "URLDATA-" + str(urlparse(initial_seeds[0])[1])
        else:
            self.urldata_file_name = Configure.COMMON_URLDATA_FILENAME
                 
        self.urldata_file_name = os.path.join(Configure.OUTPUT_PATH, Configure.BASE_DIRECTORY,
            self.urldata_file_name)
    
    
    #----------------------------------------------------------------------
    def _getMySQLConnection(self, host = '127.0.0.1', user = 'root', password = '3911965',
            database = 'mysql', port = 3306, charset = 'utf8mb4'):
        """Get MySQL database connection"""
        try:
            conn = pymysql.connect(host=host, user = user, password = password,
                database = database, port = port, charset = charset)
            cur = conn.cursor()
            cur.execute("USE " + self.databaseName) 
        except(Exception) as e:
            with open("error_info.txt", 'a', buffering= 1) as err:
                err.write("Error: _getMySQLConnection()\n" + repr(e) + '\n\n')
            conn, cur = None, None
        return conn, cur
        
        
    #----------------------------------------------------------------------
    def addData(self, key, value):
        """Add data to tempory dict"""
        self.seed_infos.setdefault(key, value)
        
        
    #----------------------------------------------------------------------
    @staticmethod
    def exportRunningLog(initial_seedList, status, result_code):
        """When write mode is coopertaion, export infomation to running log"""
        running_log_path = os.path.join(Configure.OUTPUT_PATH, Configure.BASE_DIRECTORY,
            'running_log.txt')
        if status == 'start' or status == 1:
            with open(running_log_path, 'w') as running_log:
                running_log.write("Initial seeds to search: \n\n")
                for i in range(len(initial_seedList)):
                    running_log.write(repr(i + 1) + ". " + initial_seedList[i] + '\n')
                running_log.write('\n')
                running_log.write(time.strftime("Start time: %x %X\n\n"))
        elif status == 'end' or status == 0:
            if not DataExporter.all_finished:
                DataExporter.all_finished = True
                if result_code == DataExporter.NO_MORE_SEEDS:
                    with open(running_log_path, "a+") as running_log:
                        running_log.write("\nNo more urls can be found in given seeds.\n")
                        running_log.write(time.strftime("\nEnd time: %x %X\n\n"))
                elif result_code == DataExporter.EXCEED_LIMIT:
                    with open(running_log_path, "a+") as running_log:
                        running_log.write("\nThe number of urls exceeds limit of \
                        TOTAL_URL_LIMIT({0}).\n".format(Configure.TOTAL_URL_LIMIT))
                        running_log.write(time.strftime("\nEnd time: %x %X\n\n"))

        
    #----------------------------------------------------------------------
    def initial_action(self):
        """Perform initial actions and  output the initial seeds info which for searching"""
        if not self.write_mode == Configure.COOPERATION: 
            if self.output_style == Configure.TXT_MODE:
                with open(self.urldata_file_name + '.txt', "w", 1, 'GB18030') as urldata_file:
                    urldata_file.write("Initial seeds to search: \n")
                    for i in range(len(self.initial_seeds)):
                        urldata_file.write(str(i + 1) + ". " + self.initial_seeds[i] + '\n')
                    urldata_file.write('\n')
                    urldata_file.write(time.strftime("Start time: %x %X\n"))
            elif self.output_style == Configure.CSV_MODE:
                with open(self.urldata_file_name + '.csv', 'w', 1, 'GB18030') as data_csv:
                    writer = csv.writer(data_csv)
                    writer.writerow(("Initial seeds to search: ", "", ""))
                    for i in range(len(self.initial_seeds)):
                        writer.writerow((str(i + 1) + ". " + self.initial_seeds[i], "", ""))
                    writer.writerow((time.strftime("Start time: %x %X"), "", ""))
                    writer.writerow(("INDEX", "TITLE", "URL" ))
        else:
            pass
            
            
            
    #----------------------------------------------------------------------
    def finish_action(self, result_code):
        """Perform finishing actions and output information to the tail of urldata file"""
        if not self.write_mode == Configure.COOPERATION: 
            if self.output_style == Configure.TXT_MODE:
                if result_code == self.NO_MORE_SEEDS:
                    with open(self.urldata_file_name + '.txt', "a+", 1, 'GB18030') as urldata_file:
                        urldata_file.write("\nNo more urls can be found in given seeds.\n")
                        urldata_file.write(time.strftime("\nEnd time: %x %X\n"))
                else:
                    with open(self.urldata_file_name + '.txt', "a+", 1, 'GB18030') as urldata_file:
                        urldata_file.write("\nThe number of urls exceeds limit of \
                        TOTAL_URL_LIMIT({0}).\n".format(Configure.TOTAL_URL_LIMIT))
                        urldata_file.write(time.strftime("\nEnd time: %x %X\n"))
            elif self.output_style == Configure.CSV_MODE:
                if result_code == DataExporter.NO_MORE_SEEDS:
                    with open(self.urldata_file_name + '.csv', 'a+', 1, 'GB18030') as data_csv:
                        writer = csv.writer(data_csv)
                        writer.writerow(("No more urls can be found in given seeds.", "", ""))
                        writer.writerow((time.strftime("End time: %x %X"), "", ""))
                elif result_code == DataExporter.EXCEED_LIMIT:
                    with open(self.urldata_file_name + '.csv', 'a+', 1, 'GB18030') as data_csv:
                        writer = csv.writer(data_csv)
                        writer.writerow(("The number of urls exceeds limit of TOTAL_URL_LIMIT({0}).\
                        ".format(Configure.TOTAL_URL_LIMIT), "", ""))
                        writer.writerow((time.strftime("End time: %x %X"), "", ""))
            elif self.output_style == Configure.MYSQL_MODE:
                self.cur.close()  #close cursor
                self.conn.close() #close MySQL connection
        else:
            pass
                    
        
    #----------------------------------------------------------------------
    def export_titledata(self, seeds_sum, isFinished = False):
        """Export title-url data"""
        if len(self.seed_infos) >= self.output_frequency or seeds_sum + 1 == self.total_url_limit \
           or isFinished == True :
            if self.output_style == self.txt_mode:
                if self.write_mode == Configure.INDEPENDENCE:
                    self._toTXT_independence(seeds_sum)
                else:
                    self._toTXT_cooperation()
            elif Configure.OUTPUT_STYLE == Configure.SQLITE_MODE:
                pass
            elif Configure.OUTPUT_STYLE == Configure.MYSQL_MODE:
                if self.write_mode == Configure.INDEPENDENCE:
                    pass
                else:
                    self._toMySQL_cooperation()
            elif Configure.OUTPUT_STYLE == Configure.CSV_MODE:
                self._toCSV(seeds_sum)
            elif Configure.OUTPUT_STYLE == Configure.JSON_MODE:
                pass
            elif Configure.OUTPUT_STYLE == Configure.MONGODB_MODE:
                self._toMongoDB()
            elif Configure.OUTPUT_STYLE == Configure.HTML_MODE:
                pass
            
                    
    #----------------------------------------------------------------------
    def export_jpgdata(self):
        """Collect jpgs from all of links and exports to specific path"""
        pass
        
                 
    #----------------------------------------------------------------------
    def _toTXT_independence(self, seeds_sum):
        """Export data in txt format to independent file"""
        try:
            data_txt = open(self.urldata_file_name + '.txt', mode='a', buffering= 1,
                encoding= 'GB18030')
            for seed_info in self.seed_infos.items():
                data_txt.write(str(self.output_position  + 1) + ". " + "TITLE: ")
                #delete space and CRLF in seed_title
                data_txt.write((''.join(seed_info[1].strip().split('\n')).replace(' ', '')))
                data_txt.write(" ")
                data_txt.write("URL: ")
                data_txt.write(seed_info[0])
                data_txt.write("\n")
                self.output_position += 1
        except(IOError) as e:
            print(Fore.LIGHTRED_EX + "Can not open or create a urldata file!")
            with open("error_info.txt", 'a', buffering= 1) as err:
                err.write("IOError: _toTXT()\n" + repr(e) + '\n\n')
        except(UnicodeEncodeError) as e:
            with open("error_info.txt", 'a', buffering= 1) as err:
                err.write("UnicodeEncodeError: _toTXT()\n" + repr(e) + '\n\n')
        else:
            data_txt.flush()
            data_txt.close()
        finally:
            self.output_position = seeds_sum + 1
            self.seed_infos.clear()
            
            
    
    #----------------------------------------------------------------------
    def _toTXT_cooperation(self):
        """Export data in txt format to the same file"""
        global url_written_position
        try:
            data_txt = open(self.urldata_file_name + '.txt', mode='a', buffering= 1,
                encoding= 'GB18030')
            for seed_info in self.seed_infos.items():
                data_txt.write(str(url_written_position) + ". " + "TITLE: ")
                url_written_position += 1
                #delete space and CRLF in seed_title
                data_txt.write((''.join(seed_info[1].strip().split('\n')).replace(' ', '')))
                data_txt.write(" ")
                data_txt.write("URL: ")
                data_txt.write(seed_info[0])
                data_txt.write("\n")
        except(IOError) as e:
            print(Fore.LIGHTRED_EX + "Can not open or create urldata file!")
            with open("error_info.txt", 'a', buffering= 1) as err:
                err.write("IOError: _toTXT()\n" + repr(e) + '\n\n')
        except(UnicodeEncodeError) as e:
            with open("error_info.txt", 'a', buffering= 1) as err:
                err.write("UnicodeEncodeError: _toTXT()\n" + repr(e) + '\n\n')
        else:
            data_txt.flush()
            data_txt.close()
        finally:
            self.seed_infos.clear()
    
    
            
    #----------------------------------------------------------------------
    def _toCSV(self, seeds_sum):
        """Export data in csv format to independent file"""
        try:
            data_csv = open(self.urldata_file_name + '.csv', 'a', buffering= 1,encoding= 'GB18030')
        except(IOError) as e:
            with open("error_info.txt", 'a', buffering= 1, encoding= 'GB18030') as err:
                err.write("IOError: _toCSV()\n" + repr(e) + '\n\n')
            print(Fore.LIGHTRED_EX + "Can't not open or create data.csv")
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
            with open("error_info.txt", 'a', buffering= 1, encoding= 'GB18030') as err:
                err.write("Error: _toCSV()\n" + repr(e) + '\n\n')
        else:
            data_csv.flush()
            data_csv.close()
        finally:
            self.output_position = seeds_sum + 1
            self.seed_infos.clear()
    
    
    #----------------------------------------------------------------------
    def _toMySQL_cooperation(self):
        """Export data to the same local or remote MySQL database"""
        try:
            for seed_info in self.seed_infos.items():
                select_stmt = "SELECT * from " + self.tableName + \
                    " WHERE url = \"'" + seed_info[0] + "'\""
                self.cur.execute(select_stmt)
                result = self.cur.fetchone()
                if result:
                    continue
                self.cur.execute("INSERT INTO " + self.tableName + \
                    "(title,url) VALUES (\"%s\",\"%s\")",(seed_info[1], seed_info[0]))
                self.cur.connection.commit()
        except(pymysql.err.InternalError) as e:
            with open("error_info.txt", 'a', buffering= 1, encoding= 'GB18030') as err:
                err.write("pymysql.err.InternalError: _toMySQL()\n" + repr(e) + '\n\n')
        except(pymysql.err.IntegrityError) as e:
            with open("error_info.txt", 'a', buffering= 1, encoding= 'GB18030') as err:
                err.write("pymysql.err.IntegrityError: _toMySQL()\n" + repr(e) + '\n\n')
        except(pymysql.err.DataError) as e:
            with open("error_info.txt", 'a', buffering= 1, encoding= 'GB18030') as err:
                err.write("pymysql.err.DataError: _toMySQL()\n" + repr(e) + '\n\n')
        finally:
            self.seed_infos.clear()
    
            
    #----------------------------------------------------------------------
    def _toMongoDB(self):
        """Export data to local or remote MongoDB database"""
        pass
        

            
            
        
########################################################################
class Launcher:
    """Launch this app in CMD, GUI or WEB according to START_MODE"""
    re_collect = False  #A sign to decide whether re-collect initial seeds
    recursion = 0

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.start_mode = Configure.START_MODE
        self.output_style = Configure.OUTPUT_STYLE
        
    #----------------------------------------------------------------------
    def start(self):
        """start this app"""
        seedList = []
        if self.start_mode == Configure.START_IN_CMD:
            if self._createExportPath():
                url = input("Please input a valid url, "
                    "leave it blank with default value:\n")
                if not url == "":
                    if url == "random" or url == "RANDOM":
                        seedList = Launcher.randomSeeds('start')
                    elif url.startswith('http://'):
                        seedList.append(url)
                    else:
                        seedList.append("http://" + url)
                    seedList = self._copySeed(seedList)
                else:
                    seedList = self._supplementUrls(Configure.SEEDLIST8)
                ThreadController(seedList).generateThreads()
                
                
    #----------------------------------------------------------------------
    def _createExportPath(self):
        """Create a path for outputting data if it does not exist"""
        export_path = os.path.join(Configure.OUTPUT_PATH, Configure.BASE_DIRECTORY)
        try:
            if not os.path.exists(export_path):os.makedirs(export_path)
            return True
        except(OSError):
            print(Fore.LIGHTRED_EX + "Can't create directory used for exporting urldata")
            return False
        
    
    #----------------------------------------------------------------------
    def _supplementUrls(self, urls):
        """Supplement url which has no schema"""
        supplemented_urls = []
        for url in urls:
            if not url.startswith('http://'):
                url = "http://" + url
            supplemented_urls.append(url)
        return supplemented_urls
    
    
    #----------------------------------------------------------------------
    def _copySeed(self, seedList):
        """Copy a seed and make the same number with THREAD_LIMIT"""
        tmp = seedList[:]
        if not len(tmp) >= Configure.THREAD_LIMIT:
            while len(tmp) < Configure.THREAD_LIMIT:
                tmp = tmp + seedList[:]
            if not len(tmp) == Configure.THREAD_LIMIT:
                for i in range(len(tmp) - Configure.THREAD_LIMIT):tmp.pop()
        return tmp
    
    
    #----------------------------------------------------------------------
    @staticmethod
    def randomSeeds(mode = None):
        seedList = []
        try:
            conn = pymysql.connect(host= '127.0.0.1', user = 'root', password = '3911965',
                database = 'mysql', port = 3306, charset = 'utf8mb4')
            cur = conn.cursor()
            cur.execute("USE scraping")
            select_stmt = "SELECT MAX(ID) FROM urldata"
            cur.execute(select_stmt)
            max_id = int(cur.fetchone()[0])
            for i in range(Configure.THREAD_LIMIT):
                seed_id = random.randint(1, max_id)
                print(seed_id)
                select_stmt = "SELECT * FROM urldata WHERE id = " + str(seed_id)
                cur.execute(select_stmt)
                seed = str(cur.fetchone()[2].strip('\''))
                seedList.append(seed)
            #for i in range(Configure.THREAD_LIMIT):
                #select_stmt = "SELECT * FROM urldata WHERE scraped=0"
                #cur.execute(select_stmt)
                #result = cur.fetchone()
                #print(result[0])
                #seed = str(result[2].strip('\''))
                #seedList.append(seed)
                #update_stmt = "UPDATE urldata SET scraped = 1 WHERE id = " + \
                    #result[0]
                #print(update_stmt)
                #print('######################################')
                #cur.execute(update_stmt)
                #conn.commit()
        except(Exception) as e:
            with open("error_info.txt", 'a', buffering= 1) as err:
                err.write("Error: _randomSeeds()\n" + repr(e) + '\n\n')
        finally:
            cur.close()
            conn.close()
        if not mode == "start":   
            Launcher.re_collect = False
            Launcher.recursion += 1
        return seedList
        
        

   
   
        
if __name__ == '__main__':
    init()  #Colorama init()
    all_url_count = 0  #The number of urls which collected by all threads and
    # will show in the title of CMD
    all_url_seeds = []  #The list that store all seeds for each thread to get and scrap it
    url_written_position = 1  #Record position for export url data when WRITE_MODE is COOPERATION
    all_url_set = set()  #Store all urls when WRITE_MODE is COOPERATION
    Launcher().start()
    
