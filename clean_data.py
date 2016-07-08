import re
import lxml
from bs4 import BeautifulSoup
from urllib.request import urlopen

def ngrams(inputContent, n):
    output = []
    inputContent = re.sub('\n+', ' ', inputContent)
    a = re.sub('\n+', ' ', inputContent)
    print(a)
    inputContent = re.sub(' +', ' ', inputContent)
    inputContent = bytes(content, "utf-8")
    inputContent = inputContent.decode("ascii", "ignore")
    inputContent = inputContent.split(" ")
    for i in range(len(inputContent) - n + 1):
        output.append(inputContent[i:i+n])
    return output

html = urlopen("http://en.wikipedia.org/wiki/Python_(programming_language)")
bsobj = BeautifulSoup(html, 'lxml')
content = bsobj.find("div", {"id": "mw-content-text",}).get_text()
ngrams = ngrams(content, 2)
#print(ngrams)
print("2-grams count is: " + str(len(ngrams)))
    