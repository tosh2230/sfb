import json
import urllib.request

URL = 'https://www.gaitameonline.com/rateaj/getrate'

class Fx():
    def __init__(self):
        pass

    def get_mid_rate(self, pair):
        rq = urllib.request.Request(URL)
        with urllib.request.urlopen(rq) as response:
            body = json.loads(response.read())

        for dic in body['quotes']:
            if ('currencyPairCode', pair) in dic.items():
                mid = (float(dic['ask']) + float(dic['bid'])) / 2
                break

        return mid