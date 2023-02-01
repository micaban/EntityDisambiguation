# coding=utf-8
# 09-03-2018
#   Solr class

import pysolr, json, hashlib, os, time
from random import randint
class SolrManager(object):

    def __init__(self):
        with open(os.path.join(os.path.dirname(os.path.realpath(__file__)),"../config/SYSTEM.json"), "r") as jsonFile:
            self.config = json.load(jsonFile)['common']['solr']
 
    def configureAndConnect(self, sectionName, nameOfCore):
        self.address = self.config[sectionName]['ip']+':'+self.config[sectionName]['port']+'/'+self.config[sectionName]['directory']+'/'+nameOfCore+'/'
 
        if self.config[sectionName]['type'] == "auth":
            url = self.config[sectionName]['connection']+'://'+self.config[sectionName]['user']+':'+self.config[sectionName]['password']+'@'+self.address
        elif self.config[sectionName]['type'] == "available":
            url = self.config[sectionName]['connection']+'://'+self.address
        
        if self.config[sectionName]['connection'] == "https":
            self.conn = pysolr.Solr(url, timeout=100, verify=self.config['solr']['https_pem_cert'])
        else:
            self.conn = pysolr.Solr(url, timeout=100)
    
    def getID(self, value):
        return hashlib.md5(value.encode("UTF-8")).hexdigest()    

    def get(self, docID, fl="*"):
        return self.conn.search(q='id:"'+docID+'"', rows='1', fl=fl)

    def getByID(self, docID, fl="*"):
        l = list(self.conn.search(q='id:"'+docID+'"', rows='1', fl=fl))
        if len(l) > 0:
            return l[0]
        else:
            return None

    def existByID(self, docID):
        l = list(self.conn.search(q='id:"'+docID+'"', rows='1', fl="id"))
        if len(l) > 0:
            return True
        else:
            return False

    def save(self, objects):
        if len(objects) > 0:
            self.conn.add(objects, commit=True)

    def exist(self, q):
        res = self.conn.search(q=q)
        if len(res) == 0:
            return False
        else:
            return True

    def search(self, q, rows, start, sort, fl):
        while True:
            try:
                return self.conn.search(q=q, rows=rows, start=start, sort=sort, fl=fl)
            except Exception as e:
                print(str(e))
                seconds = randint(10, 30)
                print("Solr Connection error on search query: retry after sleeping for "+str(seconds))
                time.sleep(seconds)

    def facet(self, q, facetField, mincount=1):
        results = self.conn.search(q, **{
            'facet': 'on',
            'rows': 0,
            'facet.field': facetField,
            'facet.mincount': mincount,
            'facet.limit': 10000000
        })
        
        if facetField in results.facets['facet_fields']:
            return results.facets['facet_fields'][facetField]
        else:
            return []

    def facet_pivot(self, q, pivotValues, mincount=1):
        results = self.conn.search(q, **{
            'facet': 'on',
            'rows': 0,
            'facet.pivot': ','.join(pivotValues),
            'facet.mincount': mincount,
            'facet.limit': 10000000
        })
        
        if ','.join(pivotValues) in results.facets['facet_pivot'].keys():
            return results.facets['facet_pivot'][','.join(pivotValues)]
        else:
            return []

    def update(self, doc, fieldUpdates,  _version_="1", softCommit=True):
        if isinstance(doc, pysolr.Results):
            raise Exception("Cannot update on Result Object")

        if type(doc) != list:
            doc = [doc]

        for obj_idx in range(0, len(doc)):
            obj = doc[obj_idx]

            obj_id = obj['id']
            obj = {k: obj[k] for k in fieldUpdates.keys() if k in obj}
            obj["_version_"] = _version_
            obj["id"] = obj_id

            doc[obj_idx] = obj

        if softCommit:
            self.conn.add(doc, fieldUpdates=fieldUpdates, softCommit=True)
        else:
            self.conn.add(doc, fieldUpdates=fieldUpdates, commit=True)

    def emptyDB(self):
        self.conn.delete(q='*:*')

    def close(self):
        try:
            self.conn.get_session().close()
        except:
            print("Cannot close solr connection")
        
