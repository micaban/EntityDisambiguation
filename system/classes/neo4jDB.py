# coding=utf-8
# 28-04-2022
#   Neo4j class

from neo4j import GraphDatabase
import re, json, math, os
import pandas as pd

class Neo4jManager(object):

    def __init__(self):
        with open(os.path.join(os.path.dirname(os.path.realpath(__file__)),"../config/SYSTEM.json"), "r") as jsonFile:
            self.config = json.load(jsonFile)['common']['neo4j']
    
    def configureAndConnect(self, sectionName):
        try:
            self.driver = GraphDatabase.driver('bolt://'+self.config[sectionName]['ip']+':'+self.config[sectionName]['bolt_port'], auth=(self.config[sectionName]['user'], self.config[sectionName]['password']))
        except Exception as e:
            print("Failed to create the driver:", e)

    def mergeInvestigationToResource(self, investigationId):
        with self.driver.session() as session:
            session.run("MERGE (n:Investigation {id: $id})"
                  "SET n.id = $id", {"id":investigationId})
   
    def mergeDocumentToResource(self, documentId):
        with self.driver.session() as session:
            session.run("MERGE (n:Document {id: $id})"
                  "SET n.id = $id", {"id":documentId})
        
    def connectEntityToDocument(self, typeEntity, nameDoc, nameEntity):
        with self.driver.session() as session:
            session.run("MATCH (a:Document {id: $name_a}) "
               "MATCH (b:"+typeEntity+" {name: $name_b}) "
               "MERGE (a)-[:HAS_ENTITY]-(b)",
               name_a=nameDoc, name_b=nameEntity)
               
    def connectDocumentToInvestigation(self, nameInv, nameDoc):
        with self.driver.session() as session:
            session.run("MATCH (a:Investigation {id: $name_a}) "
               "MATCH (b:Document {id: $name_b}) "
               "MERGE (a)-[:HAS_DOCUMENT]-(b)",
               name_a=nameInv, name_b=nameDoc)

    def mergeNode(self,typeEntity, name, indagine, count_docs):
    #create/update a node with properties: name, count of docs and a vector of the investigations where is present
        with self.driver.session() as session:
            session.run("MERGE (n:"+typeEntity+" {name: $name})"
            " SET n.name = $name, n.count = $count"
            , {"name":name, "count":count_docs, "indagine": indagine})
    
    def createUndirectedGraph (self, name, properties):
        with self.driver.session() as session:
            session.run("CALL gds.graph.create($name,$properties,{undirected:{type:'*', orientation:'UNDIRECTED'}})", 
                        {"name":name, "properties":properties}) 
        
    def deleteGraph (self, name):
        with self.driver.session() as session:
            session.run("CALL gds.graph.drop($name)", {"name":name})
        
    def writeEmbedding (self, name, walkLength, iterations, embeddingDimension, propertyName):
        with self.driver.session() as session:
            session.run("CALL gds.beta.node2vec.write($name, {walkLength:$walkLength,iterations: $iterations,embeddingDimension: $embeddingDimension,writeProperty: $propertyName})", 
            {"name":name, "walkLength":walkLength, "iterations":iterations, "embeddingDimension":embeddingDimension, "propertyName":propertyName})
        
    def getDfPersonsMore5Mentions (self, indagine):
        with self.driver.session() as session:
            all_persons = session.run("MATCH (a:Investigation)-[r:HAS_DOCUMENT]-(b:Document)-[s:HAS_ENTITY]-(p:Person)"
                " WHERE a.id='"+indagine+"' AND p.count>=5"
                " RETURN distinct p.name AS name, p.embeddingNode2vec AS embedding, p.count as count, p.flag as flag, p.flag_timestap as flag_timestap "
                " ORDER BY count ASC")
            df_analysis = pd.DataFrame([dict(record) for record in all_persons])
            return df_analysis
    
    def setFlagAndTime (self, typeEntity, name):
        with self.driver.session() as session:
            session.run("MERGE (n:"+typeEntity+" {name: $name}) SET n.flag = $state, n.flag_timestap=datetime({timezone: 'Europe/Rome'})", {"name":name, "state":True})
    
    def getDifferenceTimeStamp (self, time):
        with self.driver.session() as session:
            result = session.run( "RETURN duration.inSeconds(datetime({timezone: 'Europe/Rome'}),datetime({timezone: 'Europe/Rome'}))", {"originalTime":time})
            diff = session.run("UNWIND [duration.inSeconds(datetime($originalTime),datetime({timezone: 'Europe/Rome'})) ] AS aDuration RETURN aDuration", {"originalTime":time})
            
  
            return diff.single()[0]
    
    
    def close(self):
        self.driver.close()
    