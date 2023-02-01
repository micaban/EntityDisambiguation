# coding=utf-8

import os, sys, time, json, logging
from datetime import datetime as dt
from datetime import timedelta
import re, math
from collections import Counter
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from difflib import SequenceMatcher

from itertools import repeat, islice, combinations, permutations
import multiprocessing
import unidecode


from system.services.ServicesManager import ServicesManager
services = ServicesManager()

from system.services.InvestigationManager import InvestigationManager
investigationManager = InvestigationManager()

from system.classes.neo4jDB import Neo4jManager
from system.classes.solr import SolrManager

logger = logging.getLogger('disambiguation_NE')
logger.setLevel(logging.INFO)
logging.basicConfig()

def get_cosine_similarity(feature_vec_1, feature_vec_2):    
    return cosine_similarity(feature_vec_1.reshape(1, -1), feature_vec_2.reshape(1, -1))[0][0]

def get_cosine(vec1, vec2):
    intersection = set(vec1.keys()) & set(vec2.keys())
    numerator = sum([vec1[x] * vec2[x] for x in intersection])

    sum1 = sum([vec1[x]**2 for x in vec1.keys()])
    sum2 = sum([vec2[x]**2 for x in vec2.keys()])
    denominator = math.sqrt(sum1) * math.sqrt(sum2)

    if not denominator:
        return 0.0
    else:
        return float(numerator) / denominator

def text_to_vector(text):
    WORD = re.compile(r'\w+')
    return Counter(WORD.findall(text))

def get_similarity(a, b):
    a = text_to_vector(a.strip().lower())
    b = text_to_vector(b.strip().lower())

    return get_cosine(a, b)

def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()

def searchSolrToProcess(indagine,data_core,docs_num):

    coreDNA = data_core
    # get all documents for that investigation that don't have the flags in true
    result = coreDNA.search(
        'found_with_crawlerID:' + indagine + ' AND (person_NE:[* TO *] OR organization_NE:[* TO *] OR location_NE:[* TO *]) AND -sys_disambiguation_' + indagine + '_b:True AND -sys_disambiguation_error_' + indagine + '_b:True',
         docs_num, 0, 'id asc', configuration["dna_selector_fl"])

    for field_to_parse in configuration['facet_fields_to_parse']:
        logger.info("evaluateRelevantNE: facet with query: " + configuration['facet_query'] + "and field" + field_to_parse)
        globals()[f"count_{field_to_parse}"] = data_core.facet(configuration['facet_query'], field_to_parse)

    return result, count_person_NE, count_location_NE, count_organization_NE

def compareEmbedding(df_analysis, th_str, th_emb, th_min,neo):
    dfRes = pd.DataFrame(columns=['Type', 'Entity1', 'Entity2', 'Embedding Sim', 'String Sim'])
    similarity = cosine_similarity(df_analysis['embedding'].values.tolist())
    #FOR EACH VECTOR OF SIMILARITY TAKE THE ONES GREATER THAN THE TH AND THE DIFFERENCE WITH ITSELF (=1)
    for i in range(0, len(similarity)):
        each = similarity[i]
        passed = np.where(each > th_emb)[0] #ALL INDEXES THAT PASSED THE TH
        index = np.argwhere(passed == i)
        final = np.delete(passed, index)
        if len(final) != 0:
            for j in range(0, len(final)):
                name1 = df_analysis['name'][i]
                name2 = df_analysis['name'][final[j]]
                dfRes = dfRes.append({'Type': 'Person',
                                      'Entity1': name1,
                                      'Entity2': name2,
                                      'Embedding Sim': similarity[i][final[j]],
                                      'String Sim': get_similarity(name1, name2)}, ignore_index=True)
        neo.setFlagAndTime('Person', df_analysis['name'].values[i])

    dfResult = dfRes[(dfRes["String Sim"] > th_str)]
    return dfResult, dfRes

def updateCoreEntities(dfResult, indagine, count_person):
    coreEntities = SolrManager()
    coreEntities.configureAndConnect('connection', "entities")

    for i in range(0, len(dfResult)):

        c1 = len(dfResult.iloc[[i]]['Entity1'].values[0].split())
        c2 = len(dfResult.iloc[[i]]['Entity2'].values[0].split())

        #THE FIRST NAME WILL BE THE ONE WITH MORE WORDS, IF LENGTH IS EQUAL WE GET THE MOST MENTIONED ONE
        if c1 > c2:
            name = dfResult.iloc[[i]]['Entity1'].values[0]
            alias = dfResult.iloc[[i]]['Entity2'].values[0]
            actualmax = c1
        elif c1 < c2:
            name = dfResult.iloc[[i]]['Entity2'].values[0]
            alias = dfResult.iloc[[i]]['Entity1'].values[0]
            actualmax = c2
        else: #THEY HAVE THE SAME LENGTH WE TAKE THE MOST MENTIONED
            c11 = count_person[count_person.index(dfResult.iloc[[i]]['Entity1'].values[0]) + 1]
            c21 = count_person[count_person.index(dfResult.iloc[[i]]['Entity2'].values[0]) + 1]

            if c11 > c21:
                name = dfResult.iloc[[i]]['Entity1'].values[0]
                alias = dfResult.iloc[[i]]['Entity2'].values[0]
                actualmax = c1
            else:
                name = dfResult.iloc[[i]]['Entity2'].values[0]
                alias = dfResult.iloc[[i]]['Entity1'].values[0]
                actualmax = c2

        ##all the posible cases for existance of alias/first_name
        # name already exists FOR THAT INVESTIGATION add new alias
        if coreEntities.exist('first_name:"' + name + '" AND id_investigation:"'+indagine+'"'):
            doc = coreEntities.search('first_name:"' + name + '" AND id_investigation:"'+indagine+'"', '1', 0, 'id asc', configuration["entities_selector_fl"]).docs[0]
            if alias not in doc['name']:
                doc['name'].append(alias)
                coreEntities.update(doc, {'name': "set"})

        # the actual alias is a first_name in a existent doc for that investigation, delete that doc and create a new one with existing info
        elif coreEntities.exist('first_name:"' + alias + '" AND id_investigation:"'+indagine+'"'):
            doc = coreEntities.search('first_name:"' + alias + '" AND id_investigation:"'+indagine+'"', '1', 0, 'id asc', configuration["entities_selector_fl"]).docs[0]
            doc['name'].append(doc['first_name'])
            newdoc = {'name': doc['name'],
                   'first_name': name,
                   'type': 'Person',
                   'id_investigation': indagine,
                   'id': coreEntities.getID(indagine + '_' + name)}
            coreEntities.save(newdoc)
            coreEntities.deleteByID(doc['id'])

        # the alias already exists as alias of an existing doc, compare the name with the actual name and set name the greater one and alias the other one
        elif coreEntities.exist('name:' + alias + ' AND id_investigation:"'+indagine+'"'):
            ###VERR!!! HACE UN MATCH EXACTO DENTRO DEL ARRAY DE NOMBRES??
            doc = coreEntities.search('name:' + alias + ' AND id_investigation:"'+indagine+'"', '1', 0, 'id asc',configuration["entities_selector_fl"]).docs[0]
            doc['name'].append(doc['first_name'])
            newdoc = {'name': doc['name'],
                      'first_name': name,
                      'type': 'Person',
                      'id_investigation': indagine,
                      'id': coreEntities.getID(indagine + '_' + name)}
            len_existent = len(doc['first_name'].split())
            if actualmax < len_existent:
                doc['name'].append(name)
                coreEntities.update(doc, {'name': "set"})
            elif actualmax > len_existent:
                coreEntities.save(newdoc)
                coreEntities.deleteByID(doc['id'])
            else: #WHEN THE LENGTH IS THE SAME WE COMPARE THE COUNT OF MENTIONS
                count_existent = count_person[count_person.index(doc['first_name']) + 1]
                count_new = count_person[count_person.index(name + 1)]
                if count_new>=count_existent:
                    coreEntities.save(newdoc)
                    coreEntities.deleteByID(doc['id'])
                else:
                    doc['name'].append(name)
                    coreEntities.update(doc, {'name': "set"})

        else: # the alias is not part of an existing name or alias and the name does not exist in a document, we create the doc
            doc = {'name': [alias],
                   'first_name': name,
                   'type': 'Person',
                   'id_investigation': indagine,
                   'id': coreEntities.getID(indagine +'_'+ name)}
            coreEntities.save(doc)
    coreEntities.close()

def addDoctoGraph(doc, investigation,count_person,count_organization, count_location,neo):
    document = doc.get('id')
    entities_persons = doc.get('person_NE')
    entities_organizations = doc.get('organization_NE')
    entities_locations = doc.get('location_NE')

    ### CREATION OF NODES AND RELATIONSHIPS OF NEW DOCS
    neo.mergeDocumentToResource(document)
    neo.connectDocumentToInvestigation(investigation, document)

    if entities_persons is not None:
        for j in entities_persons:
            count_docs = -1
            if j in count_person:
                count_docs = count_person[count_person.index(j) + 1]
            neo.mergeNode('Person', j, investigation, count_docs)
            neo.connectEntityToDocument('Person', document, j)
    if entities_organizations is not None:
        for k in entities_organizations:
            count_docs = -1
            if k in count_organization:
                count_docs = count_organization[count_organization.index(k) + 1]
            neo.mergeNode('Organization', k, investigation, count_docs)
            neo.connectEntityToDocument('Organization', document, k)
    if entities_locations is not None:
        for l in entities_locations:
            count_docs = -1
            if l in count_location:
                count_docs = count_location[count_location.index(l) + 1]
            neo.mergeNode('Location', l, investigation, count_docs)
            neo.connectEntityToDocument('Location', document, l)

def evaluateDisambPersonsNE(record):
    
    investigation, configuration = record
    worked = False

    # CHECK EMPTY SLOT
    if investigation is None:
        logger.warning("Empty Slot")
        return worked

    # IF IT IS TOO EARLY
    '''
    logger.info(str(dt.now()) +" - "+str(dt.strptime(investigation['last_disambiguation_dt'], "%Y-%m-%dT%H:%M:%SZ")))
    if ((dt.now() - dt.strptime(investigation['last_disambiguation_dt'], "%Y-%m-%dT%H:%M:%SZ")).total_seconds() < 300):
        logger.warning("Too early...")
        return worked
    '''
    try:
        id_investigation = investigation["id"]
        ### START: CONFIGURATION PARAMETERS
        data_core = investigationManager.getCore(configuration['core']) # CONNECT TO THE CORE
        th_embedding = configuration['th_embedding']
        th_names = configuration['th_names']
        th_minutes = configuration['th_minutes']

        ### START: GETTING INFORMATION FROM DNA CORE
        result, count_person, count_location, count_organization = searchSolrToProcess(id_investigation,data_core,configuration['number_docs'])

        ### START: MAIN SECTION TO REACH THE OBJECTIVE
        if len(result)>0:
           neo = Neo4jManager()
           neo.configureAndConnect('connectionDisambiguation')
           neo.mergeInvestigationToResource(id_investigation)

           for i in range(0,len(result.docs)):
               try:
                   doc = result.docs[i]
                   addDoctoGraph(doc, id_investigation,count_person,count_organization, count_location, neo)

                   # update doc flag of processed/added to graph in solr DNA core
                   doc['sys_disambiguation_' + id_investigation + '_b'] = True
                   data_core.update(doc, {'sys_disambiguation_' + id_investigation + '_b': "set"})

               except Exception as e:
                   logger.error(str(e))
                   doc = result.docs[i]
                   doc['sys_disambiguation_error_' + id_investigation + '_b'] = True
                   data_core.update(doc, {'sys_disambiguation_' + id_investigation + '_b': "set"})

           # creating graph in memory, calculate the embedding of the nodes present in the graph in memory and drop the graph in memory
           neo.createUndirectedGraph( configuration['memory_graph'], configuration['memory_graph_nodes'])
           neo.writeEmbedding(configuration['memory_graph'], configuration['embedding_walkLength'], configuration['embedding_iterations'], configuration['embedding_dimension'], configuration['embedding_propertyName'])
           neo.deleteGraph(configuration['memory_graph'])

           # get the nodes and embedding for calculation of similarity
           df_analysis = neo.getDfPersonsMore5Mentions(id_investigation)
           now = dt.now()
           new_persons = len(df_analysis[(df_analysis["flag"] != True) | (df_analysis["flag_timestap"] <= (now - timedelta(minutes=configuration['th_minutes'])))])
           if new_persons > 0:
               dfCompared, dfAll = compareEmbedding(df_analysis, th_names, th_embedding, th_minutes, neo)
               if len(dfCompared) > 0:
                  updateCoreEntities(dfCompared, id_investigation,count_person)
           neo.close()
           return True
        else:
            return worked

    except Exception as e:
        logger.error(str(e))
    finally:
        if 'data_core' in locals():
            data_core.close()

    return worked

if __name__ == "__main__":

    while True:
        try:      
            #### CONFIGURATION OF THE EXECUTION
            with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "config/disambiguation_NE.json")) as confFile:
                conf = json.load(confFile)

            configuration = conf['executions'][conf['execute']]

            #### NUMBER OF WORKERS
            if configuration['workers'] == "CPU_COUNT":
                workers = multiprocessing.cpu_count()
            else:
                workers = configuration['workers']

            pool = multiprocessing.Pool(processes=workers)
            generator = investigationManager.investigationGenerator(workers)
            
            while True:
                logger.info("Restart...v1")

                worked = pool.map(evaluateDisambPersonsNE, iterable=zip(islice(generator, workers), repeat(configuration)))

                if True not in worked:
                    time.sleep(10) # EMPY SLOTS 

        except Exception as e:
            logger.error(str(e))  