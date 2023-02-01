# coding=utf-8
# 12-10-2020
#   Class that download and/or update a doc of type page

import json, base64, requests, time
from datetime import datetime as dt

import logging
logger = logging.getLogger('downloader')
logger.setLevel(logging.INFO)
logging.basicConfig()

import multiprocessing
from functools import partial

class DownloaderManager(object):

    def __init__(self, urlManager, services, data_core, storage):
        self.urlManager = urlManager
        self.services = services
        self.data_core = data_core
        self.config = {
            "schemas": {
                "default": {
                    "schema": "GenericCrawling",
                    "platform": "extractByGenericCrawlingSchema"
                },
                "kmu.files.cnow.at": {
                    "schema": "owncloud",
                    "platform": "owncloud"
                },
                "obedientsupporters.com": {
                    "schema": "owncloud",
                    "platform": "owncloud"
                }
            }
        }
        self.storage = storage
        self.forcedSeeds = ['telegra.ph', 'elokab.pm', 'elokab.site', 'pastethis.to', 'justpaste.it', 'elokab.ro'] #'mstdn.jp', 
    def explodeLinks(self, investigation, present_doc, fields=["internal_link", "external_link"]):

        for field_to_explode in fields:
            if field_to_explode in present_doc:
                logger.info("[DOWNLOADER] Attach the investigation ID to "+str(len(present_doc[field_to_explode]))+" docs")
                for link in present_doc[field_to_explode]:

                    first_level_linkID = self.data_core.getID(link)
                    first_level_doc = self.data_core.getByID(first_level_linkID, "id, found_with_crawlerID")

                    if first_level_doc != None:
                        if 'found_with_crawlerID' not in first_level_doc.keys():
                            first_level_doc['found_with_crawlerID'] = []

                        if investigation['id'] not in first_level_doc['found_with_crawlerID']:
                            first_level_doc['found_with_crawlerID'].append(investigation['id'])
                            self.data_core.update(first_level_doc, {"found_with_crawlerID": "set"})     
    

    def explodePointers(self, result, content, investigationID):

        if result is not None and 'point_to' in result:
            logger.info("[DOWNLOADER] ELABORATE "+str(len(result['point_to']))+" POINTERS")

            for page in result['point_to']:
                try:
                        
                    page['id'] = self.data_core.getID(page['url'])
                    domainToCheck = self.urlManager.getDomain(page['url'])
                    
                    existent_doc = self.data_core.getByID(page['id'])
                    if existent_doc == None:
                        page["found_with_crawlerID"] = content["found_with_crawlerID"]
                        page["instance_of_crawlerID"] = content["instance_of_crawlerID"]

                        ##### FORCE THE SEED
                        if domainToCheck in self.forcedSeeds:
                            page["sys_seed_b"] = True

                        self.data_core.save([page])
                    else:
                        if 'referenced_as_tag' not in existent_doc:
                            existent_doc['referenced_as_tag'] = []

                        existent_doc['referenced_as_tag'] += page['referenced_as_tag']
                        existent_doc['referenced_as_tag'] = list(set(existent_doc['referenced_as_tag']))

                        if 'found_with_crawlerID' not in existent_doc:
                            existent_doc['found_with_crawlerID'] = []

                        existent_doc['found_with_crawlerID'].append(investigationID)
                        existent_doc['found_with_crawlerID'] = list(set(existent_doc['found_with_crawlerID']))

                        fieldToUpdate = {'referenced_as_tag':'set', "found_with_crawlerID":"set"}

                        ##### FORCE THE SEED
                        if domainToCheck in self.forcedSeeds:
                            existent_doc["sys_seed_b"] = True
                            fieldToUpdate["sys_seed_b"] = "set"

                        self.data_core.update(existent_doc, fieldToUpdate)
                except Exception as e:
                    logger.info("[DOWNLOADER] "+str(e))


    def tryAddInvestigationID(self, investigation, present_doc, fieldUpdates):
        
        if 'found_with_crawlerID' not in present_doc:
            present_doc['found_with_crawlerID'] = []

        if investigation['id'] not in present_doc['found_with_crawlerID']:
            logger.info("[DOWNLOADER] Assign the investigation ID")
            present_doc['found_with_crawlerID'].append(investigation['id'])
            present_doc['found_with_crawlerID'] = list(set(present_doc['found_with_crawlerID']))
            fieldUpdates["found_with_crawlerID"] = "set"
        else:
            logger.info("[DOWNLOADER] DO NOT Assign the investigation ID, already present")
        
        return (present_doc, fieldUpdates)

    def isDownloadablePage(self, url, isSeed, id, investigationID):

        extension_s, type_s = self.urlManager.getExtensionAndPageType(url)
        if type_s != "page":
            logger.info("CANNOT DOWNLOAD, IS NOT A WEB PAGE, STORE/UPDATE THE EXTENSION")
            
            content = {}
            content['id'] = id
            content['extension_s'] = extension_s
            content['type_s'] = type_s
            
            if self.data_core.getByID(id) != None:
                self.data_core.update(content, fieldUpdates={"extension_s":"set", "type_s":"set"})
            else:
                content["found_with_crawlerID"] = []
                content["found_with_crawlerID"].append(investigationID)
                content["instance_of_crawlerID"] = investigationID
                
                content["sys_seed_b"] = isSeed
                content["full_url_s"] = url
                content["url"] = self.urlManager.getLinkWithoutProtocol(content["full_url_s"])
                content["domain"], https = self.urlManager.getDomainAndProtocol(content["full_url_s"])
                if https:
                    content["https"] = True
                else:
                    content["https"] = False

                self.data_core.save([content])
            return False
        else:
            return True

    def tryAddCycleNumber(self, investigation, present_doc, fieldUpdates, cycleField, cycleFieldValue):

        if cycleField not in present_doc:
            logger.info("[DOWNLOADER] Assign the cycle number")
            present_doc[cycleField] = str(cycleFieldValue)
            fieldUpdates[cycleField] = "set"
        else:
            logger.info("[DOWNLOADER] DO NOT Assign the cycle number, already assigned the following cycle: "+str(present_doc[cycleField]))

        return (present_doc, fieldUpdates)

    def download(self, investigation, url, id, investigationID, isSeed, cycleField, cycleFieldValue, verify_presence_of_image=[]):
        
        if 'http' not in url: 
            print("NON HO RICEVUTO IL FULL URL S")
        
        ################## CHECK URL SEED
        domain = self.urlManager.getDomain(url)
        if domain in self.forcedSeeds:
            logger.info("\n[DOWNLOADER] [SEED] è un seed: "+url)
            isSeed = True
        ##################

        result = None
        if self.isDownloadablePage(url, isSeed, id, investigationID):

            ################## GET THE PLATFORM
            domain = self.urlManager.getDomain(url)
            schema = self.config['schemas']["default"]["schema"]
            platform = self.config['schemas']["default"]["platform"]
            if domain in self.config['schemas'].keys():
                schema = self.config['schemas'][domain]["schema"]
                platform = self.config['schemas'][domain]["platform"]
            ##################

            ################## TRY DOWNLOAD
            try:
                logger.info("\n[DOWNLOADER] "+id+" "+schema+" "+platform)
                screen = True
                if domain == "facebook.com":
                    screen = False
                
                if schema == "GenericCrawling" and platform == "extractByGenericCrawlingSchema" and domain != "elokab.ro":
                    print("Download with focus: "+url)
                
                    result = self.services.v5_url_download_screen_platform_extract_focus(url, schema, 
                                                                                        download=True, screen=screen, 
                                                                                        platform=platform, filename=id, scroll=True, verify_presence_of_image=verify_presence_of_image)
                else:
                    logger.info("Download withou focus: "+url)
                    result = self.services.v4_url_download_screen_platform_extract(url, schema, 
                                                                            download=True, screen=screen, 
                                                                            platform=platform, filename=id, scroll=True)

                # STORE THE SCREEN
                if 'screen' in result:
                    self.storage.v5_store_screen(result['screen'], referenceID=id)

                # STORE THE SOURCE
                if 'source' in result:
                    self.storage.v5_store_source(result['source'], filename=id, extension="html")

                # MAIN PAGE
                content = result['content']
                content['download_date'] = dt.today().strftime("%Y-%m-%dT%H:%M:%SZ")
                content['completion_f'] = 0.2
                content['failed_download'] = False
                
                fieldUpdates = {}
                for key in content.keys():
                    fieldUpdates[key] = "set"
                    
                content['id'] = id  # AFTER fieldUpdates

            except Exception as e:
                if 'Image not present error' in str(e):
                    logger.info("[DOWNLOADER] Skip document because image")
                    raise ValueError(str(e))

                logger.info("[DOWNLOADER] error during the download: "+str(e))
                content = {}
                fieldUpdates = {}

                content['failed_download'] = True

                for key in content.keys():
                    fieldUpdates[key] = "set"
                    
                content['id'] = id   # AFTER fieldUpdates
            ################## 

            ####### SAVE OR UPDATE THE DOC
            if self.data_core.existByID(id):
                logger.info("[DOWNLOADER] UPDATE: "+id)
                if cycleField is not None:
                    stored_content = self.data_core.getByID(id, fl="id, found_with_crawlerID, sys_seed_b, "+cycleField)
                else:
                    stored_content = self.data_core.getByID(id, fl="id, found_with_crawlerID, sys_seed_b")

                if (isSeed) and ('sys_seed_b' not in stored_content or not stored_content['sys_seed_b']):
                    content["sys_seed_b"] = isSeed
                    fieldUpdates["sys_seed_b"] = "set"

                if cycleField is not None and cycleFieldValue is not None and cycleField not in stored_content:
                    content, fieldUpdates = self.tryAddCycleNumber(investigation, content, fieldUpdates, cycleField, cycleFieldValue)
                
                if investigationID not in stored_content['found_with_crawlerID']:
                    content['found_with_crawlerID'] = stored_content['found_with_crawlerID']
                    content, fieldUpdates = self.tryAddInvestigationID(investigation, content, fieldUpdates)
                    
                self.data_core.update(content, fieldUpdates, softCommit=False)
                content = self.data_core.getByID(id)
            else:
                content, fieldUpdates = self.tryAddInvestigationID(investigation, content, fieldUpdates)
                content["instance_of_crawlerID"] = investigationID
                
                content["sys_seed_b"] = isSeed
                content["full_url_s"] = url
                content["url"] = self.urlManager.getLinkWithoutProtocol(content["full_url_s"])
                content["domain"], https = self.urlManager.getDomainAndProtocol(content["full_url_s"])

                if https:
                    content["https"] = True
                else:
                    content["https"] = False

                content['extension_s'], content['type_s'] = self.urlManager.getExtensionAndPageType(url)

                if cycleField is not None and cycleFieldValue is not None:
                    content, fieldUpdates = self.tryAddCycleNumber(investigation, content, fieldUpdates, cycleField, cycleFieldValue)

                logger.info("[DOWNLOADER] SAVE: "+id)
                self.data_core.save([content])
            
            ####### ELABORATE DOC POINTERS
            self.explodePointers(result, content, investigationID)
