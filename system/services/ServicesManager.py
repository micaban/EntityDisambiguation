import requests, json, random, time, os
from datetime import datetime as dt

class ServicesManager:
    def __init__(self, path="../config/SYSTEM.json"):
        path = os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

        with open(path, "r") as jsonFile:
            sysFile = json.load(jsonFile)
            self.endpoints = sysFile['common']['api']['services'] # FLASK SERVICES

            self.osint_services_endpoints = ""
            if "osint_services_endpoints" in sysFile['common']['api'].keys():
                self.osint_services_endpoints = sysFile['common']['api']['osint_services_endpoints']

            self.java_endpoints = sysFile['common']['api']['java_services']
            self.wikipedia_miner_endpoints = sysFile['common']['api']['wikipedia_miner']
            self.db_spotlight_endpoints = sysFile['common']['api']['db_spotlight']
            self.wayback_endpoint = sysFile['common']['api']['wayback']

    def getOSINTEndpoint(self):
        return random.choice(self.osint_services_endpoints)

    def getEndpoint(self):
        return random.choice(self.endpoints)

    def getJavaEndpoint(self):
        return random.choice(self.java_endpoints)

    def getWaybackEndpoint(self):
        return random.choice(self.wayback_endpoint)

    def getWikipediaMinerEndpoint(self):
        return random.choice(self.wikipedia_miner_endpoints)

    def getDBPediaEndpoint(self):
        return random.choice(self.db_spotlight_endpoints)

    ##### OSINT SERVICES ######
    def collection_tineye_v6(self, url=None, limit=200, image_upload=None):
        
        try:
            payload = json.dumps({'url': url, 'limit':limit, 'image_upload':image_upload}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getOSINTEndpoint()+"collection/tineye/v6", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on collection_tineye_v6 request")
            print(str(e))

        result = json.loads(response.json())

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])

    def collection_tineye_enabled_v6(self):
        
        try:
            payload = json.dumps({}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getOSINTEndpoint()+"collection/tineye/enabled/v6", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on collection_tineye_enabled_v6 request")
            print(str(e))

        result = json.loads(response.json())

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])



    def processing_images_facedetection_v6(self, url, username=None, password=None):
    
        try:
            payload = json.dumps({'url': url, 'username':username, 'password':password}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getOSINTEndpoint()+"processing/images/facedetection/v6", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on processing_images_facedetection_v6 request")
            print(str(e))

        result = json.loads(response.json())

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])
        
    def processing_translate_v6(self, src, dest="en", detectSrcLang=False):
        
        try:
            payload = json.dumps({'src': src, 'dest': dest, 'detectSrcLang': detectSrcLang}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getOSINTEndpoint()+"processing/translate/v6", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on processing_translate_v6 request")
            print(str(e))

        result = json.loads(response.json())

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])

    def processing_images_ocr_v6(self, url, username=None, password=None):
        
        try:
            payload = json.dumps({'url': url, 'username': username, 'password': password}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getOSINTEndpoint()+"processing/images/ocr/v6", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on processing_images_ocr_v6 request")
            print(str(e))

        result = json.loads(response.json())

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])

    def processing_images_objects_v6(self, url, threshold=0.9, username=None, password=None):
        
        try:
            payload = json.dumps({'url': url, 'threshold': threshold, 'username': username, 'password': password}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getOSINTEndpoint()+"processing/images/ocr/v6", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on processing_images_objects_v6 request")
            print(str(e))

        result = json.loads(response.json())

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])

    """
    def processing_entities_v6(self, src, sentiment=False):
        
        try:
            payload = json.dumps({'src': src, 'sentiment':sentiment}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getOSINTEndpoint()+"processing/entities/v6", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on processing_entities_v6 request")
            print(str(e))

        result = json.loads(response.json())

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])
    """

    #################

    ##### SERVICES MANAGER ######
    def v3_detect_language(self, text):
        try:
            payload = json.dumps({'text': text}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getJavaEndpoint()+"v3/LanguageDetector", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v3_detect_language request")
            print(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']['language']
        else:
            raise Exception(result['data']['message'])

    def v5_translate_api(self, fromLanguage, toLanguage, text):
        try:
            payload = json.dumps({'text': text, 'fromLanguage':fromLanguage, 'toLanguage':toLanguage}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getEndpoint()+"v5/translate/api", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v5_translate_api request")
            print(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])

    def v5_translate_api_enabled(self):
        try:
            payload = json.dumps({}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getEndpoint()+"v5/translate/api/enabled", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v5_translate_api_enabled request")
            print(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])

    def v5_check_availability(self, url):
        try:
            payload = json.dumps({"url": url}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getEndpoint()+"v5/check/availability", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v5_check_availability request")
            print(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])

    def v5_translate_api_reached_limit(self):
        try:
            payload = json.dumps({}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getEndpoint()+"v5/translate/api/reached_limit", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v5_translate_api_reached_limit request")
            print(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])
    # USED IN ADVERSE
    def v4_google_search(self, language, key, fixed_keys_list, page, number_of_result, fixed_keys_operator):
        
        try:
            payload = json.dumps({'language': language, 'key': key, 'fixed_keys_list': fixed_keys_list, 
                        'page': page, 'number_of_result': number_of_result, 'fixed_keys_operator':fixed_keys_operator}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getEndpoint()+"v4/google/search", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v4_google_search request")
            print(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])

    # USED IN traced3 FOR THE GRAPH
    def v6_google_search(self, language, key, fixed_keys_list, page, number_of_result, fixed_keys_operator):
        
        try:
            payload = json.dumps({'language': language, 'key': key, 'fixed_keys_list': fixed_keys_list, 
                        'page': page, 'number_of_result': number_of_result, 'fixed_keys_operator':fixed_keys_operator}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getEndpoint()+"v6/google/search", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v6_google_search request")
            print(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])

    # USED IN traced3
    def v6_google_search_news(self, language, key, fixed_keys_list, page, number_of_result, fixed_keys_operator):
        
        try:
            payload = json.dumps({'language': language, 'key': key, 'fixed_keys_list': fixed_keys_list, 
                        'page': page, 'number_of_result': number_of_result, 'fixed_keys_operator':fixed_keys_operator}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getEndpoint()+"v6/google/search/news", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v6_google_search_news request")
            print(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])
        
    # USED IN ADVERSE
    def v4_google_search_news(self, language, key, fixed_keys_list, page, number_of_result, fixed_keys_operator):
        
        try:
            payload = json.dumps({'language': language, 'key': key, 'fixed_keys_list': fixed_keys_list, 
                    'page': page, 'number_of_result': number_of_result, "fixed_keys_operator": fixed_keys_operator}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getEndpoint()+"v4/google/search/news", data=payload)
            session.close()

        except Exception as e:
            print("Error on v4_google_search_news request")
            print(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])

    # USED IN ADVERSE
    def v4_translate_batch_adverse(self, tags):

        try:
            payload = json.dumps({'tags': tags}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getEndpoint()+"v4/translate/batch/adverse", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v4_translate_batch_adverse request")
            print(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])

    # USED IN TRACED
    def v4_translate_batch_traced(self, tags):

        try:
            payload = json.dumps({'tags': tags}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getEndpoint()+"v4/translate/batch/traced", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v4_translate_batch_traced request")
            print(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])
    
    # USED IN CRISTINA TWEETS
    def v4_translate_batch_cristina(self, tags):

        try:
            payload = json.dumps({'tags': tags}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getEndpoint()+"v4/translate/batch/cristina", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v4_translate_batch_cristina request")
            print(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])

    # USED IN ADVERSE E TRACED
    def v5_google_translate_api(self, tags, to="en"):

        try:
            time.sleep(10)
            payload = json.dumps({'tags': tags, 'to': to}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getEndpoint()+"v5/google/translate/api", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v5_google_translate_api request")
            print(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])

    # USED IN TRACED
    def v5_google_search(self, language, key, fixed_key, page, number_of_result):
        
        try:
            payload = json.dumps({'language': language, 'key': key,
                                'fixed_key': fixed_key, 'page': page, 'number_of_result': number_of_result}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getEndpoint()+"v5/google/search", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v5_google_search request")
            print(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])

    # USED IN TRACED
    def v5_image_efix(self, url):
        
        try:
            payload = json.dumps({'url': url}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getEndpoint()+"v5/image/efix", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v5_image_efix request")
            print(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])

    # USED IN traced3
    def v6_google_search_by_keyword_and_image(self, fixed_key, image_url, language):
        try:
            payload = json.dumps({'fixed_key': fixed_key, 'image_url': image_url, "language":language}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getEndpoint()+"v6/google/search/by-keyword-and-image", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v6_google_search_by_keyword_and_image request")
            raise Exception(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])

    # USED IN traced3
    def v6_google_search_images_bykeyowrd_related(self, fixed_key, secondary_key, find_related):
        try:
            payload = json.dumps({'fixed_key': fixed_key, 'secondary_key': secondary_key, "find_related":find_related}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getEndpoint()+"v6/google/search/images/bykeyword-related", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v6_google_search_images_bykeyowrd_related request")
            raise Exception(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])

    # USED IN ADVERSE
    def v3_google_search_site(self, key, site, page, number_of_result):
        
        try:
            payload = json.dumps({'key': key, 'site': site, 'page': page, 'number_of_result': number_of_result}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getEndpoint()+"v3/google/search/site", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v3_google_search_site request")
            print(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])

    # USED IN TRACED (?) AND traced3
    def v5_google_search_site(self, key, site, page, number_of_result):
        
        try:
            payload = json.dumps({'key': key, 'site': site, 'page': page, 'number_of_result': number_of_result}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getEndpoint()+"v5/google/search/site", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v5_google_search_site request")
            print(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])
    
    # USED IN traced3
    def v6_facebook_search_profile(self, key, ):
        
        try:
            payload = json.dumps({'key': key}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getEndpoint()+"v6/facebook/search/profile", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v6_facebook_search_profile request")
            print(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])

    # USED IN TRACED
    def v4_google_search_referrals_domain(self, url):
        
        try:
            payload = json.dumps({'url': url}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getEndpoint()+"v4/google/search/referrals/domain", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v4_google_search_referrals_domain request")
            print(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])

    # USED IN TRACED
    def v4_google_search_referrals_url(self, url):
        
        try:
            payload = json.dumps({'url': url}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getEndpoint()+"v4/google/search/referrals/url", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v4_google_search_referrals_url request")
            print(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])

    # USED IN TRACED
    def v4_whois(self, domain):
        
        try:
            payload = json.dumps({'domain': domain}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getEndpoint()+"v4/whois", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v4_whois request")
            print(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])

    # USED IN ADVERSE
    def v3_url_extract(self, url):
        try:
            payload = json.dumps({'url': url}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getEndpoint()+"v3/url/extract", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v3_url_extract request")
            raise Exception(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])

    # USED IN TRACED
    def v4_url_download_screen_platform_extract(self, full_url, schema, download, screen, platform, filename, scroll):
        try:
            payload = json.dumps({'full_url': full_url, 'schema': schema, 'platform': platform,
                                    'download': download, 'screen': screen, 'scroll': scroll,
                                    'filename': filename}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getEndpoint()+"v4/url/download/screen/platform/extract", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v4_url_download_screen_platform_extract request")
            raise Exception(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])

    def v5_url_download_screen_platform_extract_focus(self, full_url, schema, download, screen, platform, filename, scroll, verify_presence_of_image=[]):
        try:
            payload = json.dumps({'full_url': full_url, 'schema': schema, 'platform': platform,
                                    'download': download, 'screen': screen, 'scroll': scroll,
                                    'filename': filename, 'verify_presence_of_image':verify_presence_of_image}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getEndpoint()+"v5/url/download/screen/platform/extract/focus", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v5_url_download_screen_platform_extract_focus request")
            raise Exception(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])    

    # USED IN TRACED
    def v5_tineye_search(self, image_url, seed_image_url, find_next_results=True):
        try:
            payload = json.dumps({'image_url': image_url, 'seed_image_url': seed_image_url, 'find_next_results':find_next_results}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getEndpoint()+"v5/tineye/search", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v5_tineye_search request")
            raise Exception(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])

    # USED IN traced3
    def v6_google_search_image(self, fixed_key, secondary_key):
        try:
            payload = json.dumps({'fixed_key': fixed_key, 'secondary_key': secondary_key}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getEndpoint()+"v6/google/search/image", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v6_google_search_image request")
            raise Exception(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])

    def v6_google_search_image_url(self, fixed_key, secondary_key, results=3):
        try:
            payload = json.dumps({'fixed_key': fixed_key, 'secondary_key': secondary_key, 'results':results}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getEndpoint()+"v6/google/search/image/url", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v6_google_search_image_url request")
            raise Exception(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])


    # USED IN TRACED
    def v5_tineye_search_getNextResults(self, image_url):
        try:
            payload = json.dumps({'image_url': image_url}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getEndpoint()+"v5/tineye/search/getNextResults", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v5_tineye_search_getNextResults request")
            raise Exception(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])

    # USED IN ADVERSE
    def v3_twitter_search_key(self, key, fromDate, toDate, language):
        try:
            payload = json.dumps({'key': key, 'from': fromDate, 'to': toDate, 'language': language}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getEndpoint()+"v3/twitter/search/key", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v3_twitter_search_key request")
            print(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])

    # USED IN ADVERSE
    def v3_twitter_search_domain(self, domain, fromDate, toDate):
        try:
            payload = json.dumps({'domain': domain, 'from': fromDate, 'to': toDate}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getEndpoint()+"v3/twitter/search/domain", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v3_twitter_search_domain request")
            print(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])

    # USED IN ADVERSE
    """
    def v3_twitter_search_key(self, key, fromDate, toDate, language):
        try:
            payload = json.dumps({'key': key, 'from': fromDate, 'to': toDate, 'language': language}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getEndpoint()+"v3/twitter/search/key", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v3_twitter_search_key request")
            print(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])
    """

    # USED IN TRACED
    def v4_twitter_search_tweets(self, url, date):
        try:
            payload = json.dumps({'url': url, "until": date}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getEndpoint()+"v4/twitter/search/tweets", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v4_twitter_search_tweets request")
            print(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])

    # NOT USED
    def v4_twitter_hashtag_tweets(self, url, date):
        try:
            payload = json.dumps({'url': url, "until": date}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getEndpoint()+"v4/twitter/hashtag/tweets", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v4_twitter_hashtag_tweets request")
            print(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])

    # NOT USED
    def v4_twitter_profile_tweets(self, url, date):
        try:
            payload = json.dumps({'url': url, "until": date}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getEndpoint()+"v4/twitter/profile/tweets", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v4_twitter_profile_tweets request")
            print(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])

    # USED IN TRACED
    def v4_wikipedia_miner(self, source, language):
        try:
            session = requests.session()
            #source = ' '.join(e for e in source.split(" ") if e.isalnum())
            #url = self.getWikipediaMinerEndpoint()+"wikify?responseFormat=json&minProbability=0.8&wikipedia="+language+"&source=%2F"+source+"%2F"
            
            url = self.getWikipediaMinerEndpoint()+"wikify?responseFormat=json&minProbability=0.8&wikipedia="+language+"&source="+source
            #print(url)
            response = session.get(url)
            session.close()
            
            result = response.json()
            return (result['detectedTopics'], result['wikifiedDocument'])
        except Exception as e:
            print("Error on v4_wikipedia_miner request")
            print(str(e))
            raise Exception(str(e))

    # USED IN TRACED
    def v4_hostIO_domain(self, url):
        try:
            payload = json.dumps({'url': url}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.getEndpoint()+"v4/hostIO/domain", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v4_hostIO_domain request")
            raise Exception(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])

    ### https://archive.org/help/wayback_api.php ## 20060101
    # USED IN TRACED
    def v5_wayback(self, url, fromDate=None):
        try:
            session = requests.session()
            url = self.getWaybackEndpoint()+url
            
            if fromDate is not None:
                url += "&timestamp="+fromDate
            
            response = session.get(url)
            session.close()

            resp = response.json()
            
            if len(resp['archived_snapshots']) != 0:
                return dt.strptime(resp['archived_snapshots']['closest']['timestamp'], "%Y%m%d%H%M%S").isoformat()
            else:
                return None
        except Exception as e: 
            print("Error on v5_wayback request")
            print(str(e))
    