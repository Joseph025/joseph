#file_name    : schema.py
#file_function: data in memeory
#author       : liumengemeng 13195
#date         : 20200417

import sys
#sys.path.append(r"/home/l13195/issueSearch/KG/src")
from src.utils import common
from enum import Enum
'''
golbal param
key：property
value: property datatype
'''
g_schema_property_data_type_Dict = {
    "name"        :"STRING",
    "alias"       :"STRING",
    "describe"    :"STRING",
    "describekey" :"LIST",
    "detail"      :"STRING",
    "detailkey"   :"LIST",
    "searchkey"   :"LIST",
    "level"       :"STRING",
    "pl"          :"STRING",
    "pdt"         :"STRING",
    "pdtseries"   :"LIST",
    "board"       :"LIST",
    "bugmodule"   :"STRING",
    "class"       :"LIST",
    "pdtform"     :"STRING",
    "state"       :"STRING",
    "subfeature"  :"LIST",
    "feature"     :"LIST",
    "isverpatch"  :"BOOLEAN",
    "isverhead"   :"BOOLEAN",
    "isverweak"   :"BOOLEAN",
    "Domain"      :"STRING",
    "Range"       :"STRING",
    "url"         :"STRING"
}

g_schema_property_data_type_e = Enum('g_schema_property_data_type_e', \
                                     'STRING,\
                                      CHARACTER,\
                                      BOOLEAN,\
                                      BYTE,\
                                      SHORT,\
                                      LONG,\
                                      FLOAT,\
                                      DOUBLE,\
                                      GEOSHAPE,\
                                      DATE,\
                                      UUID')

g_schema_property_cardinality = Enum('g_schema_property_cardinality', \
                                     'SINGLE, \
                                      SET, \
                                      LIST')
                                      

g_edge_multiplicity_e = ("g_edge_multiplicity_e", \
                         "MULTI, \
                          SIMPLE, \
                          MANY2ONE, \
                          ONE2MANY, \
                          ONE2ONE")



'''
describe:used to property key manage
input   :propertyKeyName is necessary,others is not necessary
         Cardinality---enum g_schema_property_cardinality type,default is SINGLE
         datatype -----enum g_schema_property_data_type_e

'''                          
class propertyKey():
    def __init__(self, \
                 propertyKeyName, \
                 dataType, \
                 cardinality, \
                 signatures, \
                 ttlTime):
        self.propertyKeyName = propertyKeyName
        self.dataType        = dataType
        self.cardinality     = cardinality
        self.signatures      = signatures
        self.ttlTime         = ttlTime

'''
describe:used to vertex label manage
input   :properties---associated properties key
'''                          
class vertexLabel():
    def __init__(self, \
                 label = None, \
                 isStatic = False, \
                 isPartition = False, \
                 properties = None, \
                 ttl = 0):
        self.label = label
        self.isStatic = isStatic
        self.isPartition = isPartition
        self.properties = properties
        self.ttl = ttl
 
'''
describe:used to edge label manage
input   :connections---datatype:edgeLabel.connect()
''' 
class edgeConnect():
    def __init__(self,startVertexLabel,endVertexLabel):
        self.startVertexLabel = startVertexLabel
        self.endVertexLabel   = endVertexLabel

class edgeLabel():
    def __init__(self, \
                 edgeLabelName, \
                 directed, \
                 signaturePropertyKey, \
                 multiplicity, \
                 properties, \
                 connectObj):
        self.edgeLabelName = edgeLabelName
        self.directed = directed
        self.signaturePropertyKey = signaturePropertyKey
        self.multiplicity = multiplicity
        self.properties = properties
        self.connections = {}
        self.connections.update(connectObj.__dict__)
 
'''
describe:used to vertex  manage
input   :propertyObj---define in this file,such as:properityIssue
'''  
class vertex():
    def __init__(self,label, propertyDict):
        self.label      = label
        self.properties = propertyDict


'''
describe:used to edge manage
input   :propertyObj---properityEdge,define in this file
'''        
class edge():
    def __init__(self,label,outVertexObj,inVertexObj, propertyDict):
        self.label      = label
        self.outVertex  = outVertexObj.__dict__
        self.inVertex   = inVertexObj.__dict__
        self.properties = propertyDict


        
'''
describe:if you want get nodes or labels,this datatype can help you filter target  
input   :property.predicate---and,or,not
'''     
class Filter():
    class property():
        def __init__(self, \
                     key, \
                     predicate, \
                     values):
            self.key = key
            self.predicate = predicate
            self.values = values
             
    def __init__(self, \
                 operator, \
                 filterList):
        self.operator = operator
        self.filterList = []
        if not common.data_is_NULL(filterList):
            for filter in filterList:
                self.filterList.append(filter.__dict__)
                

class properityEdge():
    def __init__(self,Domain,Range):
        self.Domain = Domain
        self.Range  = Range


class properityIssue(): 
    def __init__(self, \
                 name, \
                 describe, \
                 level, \
                 issues, \
                 customer, \
                 customerType, \
                 introduceVer, \
                 resoveVer, \
                 bugmodule, \
                 pdt):
        self.name         = name
        self.describe     = describe
        self.level        = level
        self.issues        = issues
        self.customer     = customer
        self.customerType = customerType
        self.introduceVer = introduceVer
        self.resoveVer    = resoveVer   
        self.config       = config
        self.bugmodule    = bugmodule
        self.pdt          = pdt    
   
        
                



class QuerySemantics():
    def __init__(self,score,NerList,intention = None):
        self.score     = score
        self.NerList   = NerList
        self.intention = intention
        



        

        