import requests, json, random, os
from datetime import datetime as dt

class StorageManager:
    def __init__(self, path="../config/SYSTEM.json"):
        path = os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

        with open(path, "r") as jsonFile:
            sysFile = json.load(jsonFile)
            self.storageEndpoint = sysFile['common']['api']['storage'][0]

    # EXTRACT TEXT, URL AND IMAGES FROM A PDF
    def v5_pdf_meta_images_links(self, url, meta_b, images_b, links_b, elaborate_pointers_b, 
                                referenceID, referenceDomain, referenceDomainProtocol):
        
        try:
            payload = json.dumps({'url': url, 'meta_b': meta_b,
                                'images_b': images_b, 'links_b': links_b, "elaborate_pointers_b": elaborate_pointers_b,
                                'referenceID': referenceID, 'referenceDomain': referenceDomain, 'referenceDomainProtocol': referenceDomainProtocol}, 
                                ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.storageEndpoint+"v5/pdf/meta/images/links", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v5_pdf_meta_images_links request")
            print(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])
 
    # STORE THE SCREENSHOTS OF A PAGE
    def v5_store_screen(self, source, referenceID):
        try:
            payload = json.dumps({'source': source, 'referenceID': referenceID}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.storageEndpoint+"v5/store/screen", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v5_store_screen request")
            raise Exception(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])

    # STORE THE SOURCE OF A PAGE
    def v5_store_source(self, source, filename, extension):
        try:
            payload = json.dumps({'source': source, 'filename':filename, 'extension':extension}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.storageEndpoint+"v5/store/source", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v5_store_source request")
            raise Exception(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])
    
    # DOWNLOAD PDF AND OTHER MEDIA
    def v5_media_download(self, full_url, extension, referenceID):
        try:
            payload = json.dumps({'full_url': full_url, 'extension': extension, 'referenceID': referenceID}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.storageEndpoint+"v5/media/download", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v5_media_download request")
            raise Exception(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])

    # DOWNLOAD VIDEO m3u8 (ex. twitter videos)
    def v5_media_download_m3u8(self, full_url, base_url, referenceID):
        try:
            payload = json.dumps({'full_url': full_url, 'base_url': base_url, 'referenceID': referenceID}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.storageEndpoint+"v5/media/download/m3u8", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v5_media_download_m3u8 request")
            raise Exception(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])
  
    # DOWNLOAD PDF AND OTHER MEDIA FOR NEXTCLOUD
    def v4_media_download(self, full_url, extension, referenceID):
        try:
            payload = json.dumps({'full_url': full_url, 'extension': extension, 'referenceID': referenceID}, ensure_ascii=False).encode("utf-8")
            
            session = requests.session()
            response = session.post(self.storageEndpoint+"v4/media/download", data=payload)
            session.close()
            
        except Exception as e:
            print("Error on v4_media_download request")
            raise Exception(str(e))

        result = response.json()

        if(result['status'] == "success"):
            return result['data']
        else:
            raise Exception(result['data']['message'])