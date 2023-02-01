import requests, json, random, time, os
from datetime import datetime
from system.classes.solr import SolrManager
from system.classes.neo4j import Neo4jManager

class InvestigationManager:
    def __init__(self, path="../config/SYSTEM.json"):
        path = os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

        with open(path, "r") as jsonFile:
            data = json.load(jsonFile)
            self.cores = data['traced']['solr']['cores']

    def getActiveInvestigations(self, extraQuery="", orderBy="restart_date_dt desc", fl="*", sleep=True):

        investigations = []
        if extraQuery != "":
            extraQuery = " AND ("+extraQuery+")"

        while len(investigations) == 0:
            investigations_core = SolrManager()
            investigations_core.configureAndConnect('connection', "AcheCrawlers")

            investigations = investigations_core.search('enabled_b:true AND -pause_b:true AND creation_date:[2020-01-01T00:00:00Z TO *] '+extraQuery, 100000, 0, orderBy, fl)
            investigations_core.close()

            if len(investigations) == 0:
                if not sleep:
                    break
                else:
                    #print("SLEEP...")
                    time.sleep(10)
                
        return list(investigations)

    def isInvestigationActive(self, id):

        investigations_core = SolrManager()
        investigations_core.configureAndConnect('connection', "AcheCrawlers")

        investigation = investigations_core.search("id:"+id+" AND enabled_b:true AND -pause_b:true", rows=1, start=0, sort="id desc", fl="*")
        investigations_core.close()

        if len(investigation) != 0:
            return True, list(investigation)[0]
        else:
            return False, None

    def getInvestigationsNodes(self, orderBy="last_edit", fl="*", sleep=True):

        investigations = []

        while len(investigations) == 0:
            neo4j = Neo4jManager()
            neo4j.configureAndConnect('connection1')
            
            investigations = neo4j.getInvestigationRoots(orderBy)

            if len(investigations) == 0:
                if not sleep:
                    break
                else:
                    #print("SLEEP...")
                    time.sleep(10)
                
        return investigations

    def getAllInvestigationPaths(self):

        neo4j = Neo4jManager()
        neo4j.configureAndConnect('connection1')
                
        return list(neo4j.getAllInvestigationPaths())

    def investigationGenerator(self, workers, extraQuery="", orderBy="restart_date_dt desc", fl="*"):
        
        investigations = []
        if extraQuery != "":
            extraQuery = " AND ("+extraQuery+")"

        ## ACQUIRE INVESTIGATIONS FROM THE SYSTEM
        investigations_core = SolrManager()
        investigations_core.configureAndConnect('connection', "AcheCrawlers")

        investigations = list(investigations_core.search('enabled_b:true AND -pause_b:true AND creation_date:[2020-01-01T00:00:00Z TO *] '+extraQuery, 100000, 0, orderBy, fl))
        investigations_core.close()
        investigations += [None for i in range(0, workers - len(investigations))]
        #######

        while True:
            for value in investigations:
                
                ## RECHECK INVESTIGATIONS INTO THE SYSTEM
                investigations_core = SolrManager()
                investigations_core.configureAndConnect('connection', "AcheCrawlers")
                refreshedInvestigations = list(investigations_core.search('enabled_b:true AND -pause_b:true AND creation_date:[2020-01-01T00:00:00Z TO *] '+extraQuery, 100000, 0, orderBy, fl))
                investigations_core.close()
                refreshedInvestigations += [None for i in range(0, workers - len(refreshedInvestigations))]
                #######

                ## IF THE INVESTIGATIONS LIST HAS CHANGED
                if json.dumps(investigations) != json.dumps(refreshedInvestigations):
                    investigations = refreshedInvestigations
                    print("System state changed")
                    break

                yield value

    def sleep(self, nInvest, cylces=6, extraQuery="", orderBy="restart_date_dt desc", fl="*"):
        print("Half SLEEP...")

        i = 1
        while i <= cylces:
            time.sleep(10)
            i += 1
            if len(self.getActiveInvestigations(extraQuery, orderBy, fl, sleep=False)) != nInvest:
                break
        
        print("END Half SLEEP...")

    def sleep2(self, nInvest1, nInvest2, cylces=6, extraQuery1="", extraQuery2="", orderBy="restart_date_dt desc", fl="*"):
        print("Half SLEEP...")

        i = 1
        while i <= cylces:
            time.sleep(10)
            i += 1
            if len(self.getActiveInvestigations(extraQuery1, orderBy, fl, sleep=False)) != nInvest1:
                break

            if len(self.getActiveInvestigations(extraQuery2, orderBy, fl, sleep=False)) != nInvest2:
                break
        
        print("END Half SLEEP...")

    def getInvestigationCore(self):
        
        investigations_core = SolrManager()
        investigations_core.configureAndConnect('connection', self.cores['investigations_core'])

        return investigations_core

    def getLocalFileCore(self):
        
        local_file_core = SolrManager()
        local_file_core.configureAndConnect('connection', self.cores['local_file_core'])

        return local_file_core

    def getSeedsCore(self):
        
        seeds_core = SolrManager()
        seeds_core.configureAndConnect('connection', self.cores['seed_images_logger'])

        return seeds_core

    def getMainCore(self):
        
        docs_core = SolrManager()
        docs_core.configureAndConnect('connection', self.cores['docs_core'])

        return docs_core

    def getSecondaryCore(self):
        
        domain_core = SolrManager()
        domain_core.configureAndConnect('connection', self.cores['secondary_core'])

        return domain_core

    def getSocialCore(self):
        
        social_core = SolrManager()
        social_core.configureAndConnect('connection', self.cores['social_core'])

        return social_core

    def getCore(self, section):

        core = SolrManager()
        core.configureAndConnect('connection', self.cores[section])

        return core