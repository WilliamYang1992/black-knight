import requests

params = {'firstname': 'William', 'lastname': 'Yang',}
req = requests.post("http://pythonscraping.com/files/processing.php", data= params)
print(req.text)