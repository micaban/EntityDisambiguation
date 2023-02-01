import requests, json, random, time, os
from datetime import datetime as dt

class Processing:
    def __init__(self, path="../config/SYSTEM.json"):
        path = os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

        with open(path, "r") as jsonFile:
            sysFile = json.load(jsonFile)
            self.processing = sysFile['common']['api']['microservices']['processing']

    def getTranslationEndpoint(self):
        return random.choice(self.processing['translation'])

    # USED IN ADVERSE E TRACED
    def v5_google_translate_api(self, tags, to="en"):

        try:
            time.sleep(20)
            payload = json.dumps({'tags': tags, 'to': to}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getTranslationEndpoint()+"v5/google/translate/api", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v5_google_translate_api request")
            print(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])
