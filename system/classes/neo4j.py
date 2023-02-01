# coding=utf-8
# 09-03-2018
#   Neo4j class

import re, json, math, os
from py2neo import Graph, Path, NodeSelector
from py2neo import Node, Relationship
from py2neo import ogm
from py2neo.ogm import GraphObject, Property

class Neo4jManager(object):

    def __init__(self):
        with open(os.path.join(os.path.dirname(os.path.realpath(__file__)),"../config/SYSTEM.json"), "r") as jsonFile:
            self.config = json.load(jsonFile)['common']['neo4j']
 
    def configureAndConnect(self, sectionName):
        
        self.graph = Graph(bolt=True, 
                            host=self.config[sectionName]['ip'], 
                            bolt_port=int(self.config[sectionName]['bolt_port']), 
                            http_port=int(self.config[sectionName]['http_port']),
                            user=self.config[sectionName]['user'], 
                            password=self.config[sectionName]['password'])
        
        self.defineUniquness('Root', 'value')
        self.defineUniquness('location_NE', 'value')
        self.defineUniquness('other_NE', 'value')
        self.defineUniquness('organization_NE', 'value')
        self.defineUniquness('person_NE', 'value')

    def update(self, nodeOrRelation, propList):
        for p in propList:
            nodeOrRelation[p['name']] = p['value']

        self.graph.push(nodeOrRelation)
        return nodeOrRelation
    
    def defineUniquness(self, label, nameOfField):
        try:
            self.graph.schema.create_uniqueness_constraint(label, nameOfField)
        except:
            pass

    def configure_instance_session(self, label, value):
        return self.run('MATCH (n:'+label+'{value:"'+value+'"})-[r:rel]-(m) SET r.session_explored=False, r.session_social_explored=False')
    
    def search(self, label, property_key=None, property_value=None, limit=None):
        if limit is not None:
            limit = int(limit)

        return self.graph.find(label, property_key, property_value, limit)

    def match(self, label, property_key, property_value, limit):
        return self.graph.run("MATCH (n) WHERE n.total IS NULL RETURN n LIMIT "+limit)
    
    def exist(self, label, value, returnNode=False):                  
        
        nodes = list(self.graph.run('MATCH (n:'+label+' {value:"'+value.replace('"', '')+'"}) RETURN n'))    
        exist = False
        for _ in nodes:
            exist = True
        
        if returnNode:
            if exist:
                for node in nodes:
                    return node[0]
            else:
                return None
        else:
            return exist

    def existValue(self, value):
        
        nodes = self.graph.run('MATCH (n {value:"'+value+'"}) RETURN n')
        
        exist = False
        for _ in nodes:
            exist = True
        
        return exist
    
    def run(self, q):
        return self.graph.run(q)

    def getNodeWithoutRelations(self, valueOfRoot, limit):
        return self.graph.run("MATCH (u:Root {value:\""+valueOfRoot+"\"}), (n) WHERE (u)-[:found]->(n) AND NOT (u)-[:rel]->(n) AND n.value <> \""+valueOfRoot+"\" RETURN n LIMIT "+limit)
           
    def getRelationBetweenNodes(self, valueOfRoot, value, typeRel="rel"):
        return self.graph.run("MATCH (u:Root {value:\""+valueOfRoot+"\"}), (u)-[r:"+typeRel+"]-> (n) WHERE n.value=\""+value+"\" RETURN r LIMIT 1")
        
    def getRelationBetweenSpecificValuesOfNodes(self, fromNodeLabel, fromNodeValue, toNodeLabel, toNodeValue, typeRel="rel"):
        return self.graph.run("MATCH (from:"+fromNodeLabel+" {value:\""+fromNodeValue+"\"})-[r:"+typeRel+"]->(to:"+toNodeLabel+" {value:\""+toNodeValue+"\"}) RETURN r LIMIT 1")
    
    def getAllInvestigationPaths(self):
        return self.graph.run("MATCH (u:Root), (u)-[r:investigation_path]->(n) WHERE r.active=true RETURN u,r,n")
    
    def getRelationAndNodesByType(self, valueOfRoot, typeOfNode, typeRel="rel", limit="10"):
        return self.graph.run("MATCH (u:Root {value:\""+valueOfRoot+"\"}), (u)-[r:"+typeRel+"]->(n:"+typeOfNode+") RETURN r,n ORDER BY r.distance LIMIT "+str(limit))

    def getNodeByRelationNotExplored(self, valueOfRoot, limit=1):
        return self.graph.run("MATCH (u:Root {value:\""+valueOfRoot+"\"}), (u)-[r:rel]-> (n) WHERE (r.explored = 'False' OR r.session_explored = 'False') RETURN n LIMIT "+str(limit))

    def getNodeByRelationNotExplored2(self, fromLabel, fromValue, limit=1):
        return self.graph.run("MATCH (u:"+fromLabel+" {value:\""+fromValue+"\"})-[r:rel]->(n) WHERE (r.explored = false OR r.session_explored = false) RETURN n LIMIT "+str(limit))

    def getNodeBySocialRelationNotExplored(self, valueOfRoot, limit, threshold):
        return self.graph.run("MATCH (u:Root {value:\""+valueOfRoot+"\"}), (u)-[r:rel]-> (n) WHERE (r.social_explored = 'False' OR r.session_social_explored = 'False') AND (r.distance <= \""+str(threshold)+"\" OR r.investigation_path = 'True' ) RETURN n LIMIT "+str(limit))
    
    def getNodesByActiveInvestigationPaths(self, valueOfRoot):
        return self.graph.run("MATCH (u:Root {value:\""+valueOfRoot+"\"}), (u)-[r:investigation_path]-> (n) WHERE (r.active = true) RETURN n ORDER BY r.insertion_date DESC LIMIT 1000000000") # !! 1000000000??!!! NOOOO

    def getNode(self, label, valueOfRoot):
        return self.graph.run("MATCH (u:"+label+" {value:\""+valueOfRoot+"\"}) RETURN u LIMIT 1")

    def getRoot(self, label, value):
        rootNodes = self.graph.run("MATCH (u:"+label+" {value:\""+value+"\"}) RETURN u LIMIT 1")
        for node in rootNodes:
            return node[0]

        return None

    def getRoots(self):
        rootNodes = self.graph.run("MATCH (u:Root) RETURN u")

        nodes = {}
        i = -1
        for node in rootNodes:
            i = i+1
            nodes[i] = node[0]

        return nodes

    def getInvestigationRoots(self, orderBy="last_edit"):
        rootNodes = self.graph.run("MATCH (u:Root)  RETURN u ORDER BY u."+orderBy)
        return list(rootNodes)
    
    def elaborateNode(self, label, value):
        return Node(label, value=value)

    def elaborateRelationship(self, node1, node2, typeRel="rel"):
        return Relationship(node1, typeRel, node2)

    def setProperties(self, nodeOrRelation, propList):
        for p in propList:
            nodeOrRelation[p['name']] = p['value']
        return nodeOrRelation

    def save(self, nodeOrRelation):
        try:
            self.graph.create(nodeOrRelation)
        except Exception as e:
            pass

    def emptyDB(self):
        self.graph.run("MATCH (n) DETACH DELETE n")
