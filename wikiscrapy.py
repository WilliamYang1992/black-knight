#encoding:utf-8

from urllib.request import urlopen
from bs4 import BeautifulSoup
import re
import time
import lxml
import datetime
import random
import pymysql

conn = pymysql.connect(host = '127.0.0.1', user = 'root', passwd = '3911965',
    db = 'mysql', charset = 'utf8')
cur = conn.cursor()
cur.execute("USE scraping")

random.seed(datetime.datetime.now())

def store(title, content):
    try:
        cur.execute("INSERT INTO pages (title,content) VALUES (\"%s\",\"%s\")",
            (title, content))
        cur.connection.commit()
    except(pymysql.err.InternalError):
        pass
    
def getLinks(articleUrl):
    try:     
        html = urlopen("http://en.wikipedia.org" + articleUrl)
    except(Exception) as e:
        print(e)
        time.sleep(300)
        html = urlopen("http://en.wikipedia.org" + "/wiki/Dongguan")
    bsObj = BeautifulSoup(html, 'lxml')
    try: 
        title = bsObj.find('h1').get_text()
    except(AttributeError):
        pass
    content = bsObj.find('div', {"id": "mw-content-text",}).find('p').get_text()
    store(title, content)
    return bsObj.find('div', {"id": "bodyContent",}).findAll('a',
        href = re.compile("^(/wiki/)((?!:).)*$"))

links = getLinks("/wiki/The_Man_in_the_Moone")
try:
    while len(links) > 0:
        newArticle = links[random.randint(0, len(links)-1)].attrs['href']
        print(newArticle)
        links = getLinks(newArticle)
finally:
    cur.close()
    conn.close()