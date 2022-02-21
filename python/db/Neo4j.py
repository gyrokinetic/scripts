from tqdm import tqdm
from neo4j import GraphDatabase
import sys, json, re, os, time
import pandas as pd

class Neo4j(object):

   def __init__(self, uri, username='neo4j', password='test123'):
      self.uri = uri
      self.username = username
      self.password = password
      self.Driver = None
      try:
         self.Driver = GraphDatabase.driver(self.uri, auth=(self.username, self.password))
      except Exception as e:
         print("Failed to connect ", e)

   def Close(self):
      if(self.Driver is not None):
         self.Driver.close()
         self.Driver = None

   def Query(self, query, parameters=None, db='neo4j'):
      assert self.Driver is not None, "Driver is not initiated"
      session = None
      response = None

      try:
         session = self.Driver.session(database=db) if db is not None else self.Driver.session()
         response = list(session.run(query, parameters))

      except Exception as e:
         print("Query failed", e)
      finally:
         if session is not None:
            session.close()
      return response

   def insert_data(self, query, rows, batch_size = 10000):
        # Function to handle the updating the Neo4j database in batch mode.
            
        total = batch = 0
        t1 = time.time()
        result = None

        num = len(rows)
        while batch * batch_size < len(rows):

           res = self.Query(query, parameters = {'rows': rows[batch*batch_size:(batch+1)*batch_size].to_dict('records')})
           total += res[0]['total']
           batch += 1
           result = {"total":total, "batches":batch, "num":num, "time":time.time()-t1}
           print(result)
        return result

   def TestAddEdgeAndNodeFromDF(self, rows, num=5000):
     #('EdgeName', 'EdgeAttr', 'FNode', 'TNode', 'FLabel', 'TLabel')
      query = """
         UNWIND $rows as spec
         CALL apoc.merge.node([spec.FLabel], spec.FNode) YIELD node
         with node as FNode, spec
         CALL apoc.merge.node([spec.TLabel], spec.TNode) YIELD node
         with FNode, node as TNode, spec
         call apoc.create.relationship(FNode, spec.EdgeName, spec.EdgeAttr, TNode) YIELD rel
         RETURN count(*) as total
      """
      self.insert_data(query, rows, num)
      #self.Query(query, parameters = nodeSpec)



   def TestAddEdgeAndNode(self, nodeSpec):
     #{"EdgeName":"Derive", "FromNode":{"NodeName":"Data", "ID":"2"}, "ToNode":{"NodeName":"Data", "ID":"1"}}
     #{"EName":"Derive", "FNode":{"NodeName":"Data", "ID":2}, "TNode":{"NodeName":"Data", "ID":1}, "FLabel":"DESIGN", 'TLabel':"DESIGN"}
      query = """
         UNWIND $nodes as spec
         CALL apoc.merge.node([spec.FLabel], spec.FNode) YIELD node
         with node as FNode, spec
         CALL apoc.merge.node([spec.TLabel], spec.TNode) YIELD node
         with FNode, node as TNode, spec
         call apoc.create.relationship(FNode, spec.EName, spec.EEdge, TNode) YIELD rel
         RETURN FNode AS x ORDER BY x
      """
      self.Query(query, parameters = nodeSpec)


   def AddEdgeAndNode(self, nodeSpec):
     #{"EdgeName":"Derive", "FromNode":{"NodeName":"Data", "ID":"2"}, "ToNode":{"NodeName":"Data", "ID":"1"}}
      query = """
         UNWIND $nodes as spec
         CALL apoc.merge.node([spec.FromNode.Label], {ID: spec.FromNode.ID, NAME: spec.FromNode.NodeName}) YIELD node
         with node as FromNode, spec
         CALL apoc.merge.node([spec.ToNode.Label], {ID: spec.ToNode.ID, NAME: spec.ToNode.NodeName}) YIELD node
         with FromNode, node as ToNode, spec
         call apoc.create.relationship(FromNode, spec.EName, NULL, ToNode) YIELD rel
         RETURN FromNode AS x ORDER BY x
      """
      self.Query(query, parameters = nodeSpec)

   def AddNode(self, nodeSpec):
      query = """
         UNWIND $nodes as spec
         MERGE (nd1:""" + spec.label_1 + spec.json_1 + """)
         with nd1, spec
         MERGE (nd2:""" + spec.label_2 + spec.json_2 + """)
         with nd1, nd2, spec
         call apoc.create.relationship(nd1, spec.edgename, spec.edgejson, nd2) YIELD rel
         RETURN nd1.""" + spec.label_1 + " AS x ORDER BY x"
      return self.Query(query, parameters = nodeSpec)

   def AddNodeTest(self, nodeSpec):
      query = """
         UNWIND $nodes as spec
         MERGE (nd1:TEST)
         with nd1, spec
         MERGE (nd2:TEST)
         with nd1, nd2, spec
         call apoc.create.relationship(nd1, spec.edgename, NULL, nd2) YIELD rel
         RETURN nd1.TEST AS x ORDER BY x
      """
      self.Query(query, parameters = nodeSpec)

   def Clean(self, nodeLabel= '', num=5000):
      keepon  = True
      while(keepon):
         try:
            query = 'MATCH (n' + nodeLabel + ') OPTIONAL MATCH (n' + nodeLabel + ')-[r]-() with n,r limit ' + str(num) + ' DETACH DELETE n,r RETURN count(n) as rec'
            res = self.Query(query)
            n = res[0]['rec']
            print(n)
            if(n == 0):
               keepon = False
         except Exception as e:
            print("Error in Clean ", e)
      return res
   
nodeSpec = {"nodes" : [
    {"edgename":"Derive", "nd1":{"Name":"Data", "MID":"1"}, "nd2":{"Name":"Data", "MID":"2"}},
    {"edgename":"Contain", "nd1":{"Name":"Data", "MID":"1"}, "nd2":{"Name":"Data", "MID":"2"}},
    {"edgename":"OWN", "nd1":{"Name":"Data", "MID":"1"}, "nd2":{"Name":"Agent", "MID":"3"}}
    ]}

EdgeNodeSpec = {"nodes" : [
    {"EdgeName":"Derive", "FromNode":{"NodeName":"Data", "ID":"2"}, "ToNode":{"NodeName":"Data", "ID":"1"}}
    ,{"EdgeName":"Contain", "FromNode":{"NodeName":"Data", "ID":"2"}, "ToNode":{"NodeName":"Data", "ID":"1"}}
    ,{"EdgeName":"OWN", "FromNode":{"NodeName":"Data", "ID":"1"}, "ToNode":{"NodeName":"Agent", "ID":"3"}}
    ,{"EdgeName":"Approve", "FromNode":{"NodeName":"Data", "ID":"1"}, "ToNode":{"NodeName":"Agent", "ID":"3"}}
    ,{"EdgeName":"Trigger", "FromNode":{"NodeName":"Event", "ID":"4"}, "ToNode":{"NodeName":"Data", "ID":"1"}}
    ,{"EdgeName":"Belong", "FromNode":{"NodeName":"Data", "ID":"1"}, "ToNode":{"NodeName":"Universe", "ID":"5"}}
    ,{"EdgeName":"Locate", "FromNode":{"NodeName":"Data", "ID":"1"}, "ToNode":{"NodeName":"Location", "ID":"6"}}
    ,{"EdgeName":"In", "FromNode":{"NodeName":"Data", "ID":"1"}, "ToNode":{"NodeName":"Status", "ID":"7"}}
    ,{"EdgeName":"Assert", "FromNode":{"NodeName":"Assertion", "ID":"8"}, "ToNode":{"NodeName":"Data", "ID":"1"}}
    ]}

TestEdgeNodeSpec = {"nodes" : [
    {"EName":"Derive", "EEdge":{}, "FNode":{"Name":"Data", "ID":2}, "TNode":{"Name":"Data", "ID":1}, "FLabel":"DESIGN", 'TLabel':"DESIGN"}
    ,{"EName":"Contain", "EEdge":{}, "FNode":{"Name":"Data", "ID":2}, "TNode":{"Name":"Data", "ID":1}, "FLabel":"DESIGN", 'TLabel':"DESIGN"}
    ,{"EName":"OWN", "EEdge":{}, "FNode":{"Name":"Data", "ID":1}, "TNode":{"Name":"Agent", "ID":3}, "FLabel":"DESIGN", 'TLabel':"DESIGN"}
    ,{"EName":"Approve", "EEdge":{}, "FNode":{"Name":"Data", "ID":1}, "TNode":{"Name":"Agent", "ID":3}, "FLabel":"DESIGN", 'TLabel':"DESIGN"}
    ,{"EName":"Trigger", "EEdge":{}, "FNode":{"Name":"Event", "ID":4}, "TNode":{"Name":"Data", "ID":1}, "FLabel":"DESIGN", 'TLabel':"DESIGN"}
    ,{"EName":"Belong", "EEdge":{}, "FNode":{"Name":"Data", "ID":1}, "TNode":{"Name":"Universe", "ID":5}, "FLabel":"DESIGN", 'TLabel':"DESIGN"}
    ,{"EName":"Locate", "EEdge":{}, "FNode":{"Name":"Data", "ID":1}, "TNode":{"Name":"Location", "ID":6}, "FLabel":"DESIGN", 'TLabel':"DESIGN"}
    ,{"EName":"In", "EEdge":{}, "FNode":{"Name":"Data", "ID":1}, "TNode":{"Name":"Status", "ID":7}, "FLabel":"DESIGN", 'TLabel':"DESIGN"}
    ,{"EName":"Assert", "EEdge":{"BY":"Name", "IN":"Datetime"}, "FNode":{"Name":"Assertion", "ID":8}, "TNode":{"Name":"Data", "ID":1}, "FLabel":"DESIGN", 'TLabel':"DESIGN"}
    ]}

df = pd.DataFrame(columns=('EdgeName', 'EdgeAttr', 'FNode', 'TNode', 'FLabel', 'TLabel'))
df.loc[0]=['Derive', '{}', '{"Name":"Data","ID":2}', '{"Name":"Data","ID":1}', 'DESIGN', 'DESIGN']
df.loc[1]=['Contain', '{}', '{"Name":"Data","ID":2}', '{"Name":"Data","ID":1}', 'DESIGN', 'DESIGN']
df.loc[2]=['OWN', '{}', '{"Name":"Data","ID":1}', '{"Name":"Agent","ID":3}', 'DESIGN', 'DESIGN']
df.loc[3]=['Approve', '{}', '{"Name":"Data","ID":1}', '{"Name":"Agent","ID":3}', 'DESIGN', 'DESIGN']
df.loc[4]=['Trigger', '{}', '{"Name":"Trigger","ID":4}', '{"Name":"Data","ID":1}', 'DESIGN', 'DESIGN']
df.loc[5]=['Belong', '{}', '{"Name":"Data","ID":1}', '{"Name":"Universe","ID":5}', 'DESIGN', 'DESIGN']
df.loc[6]=['Locate', '{}', '{"Name":"Data","ID":1}', '{"Name":"Location","ID":6}', 'DESIGN', 'DESIGN']
df.loc[7]=['In', '{}', '{"Name":"Data","ID":1}', '{"Name":"Status","ID":7}', 'DESIGN', 'DESIGN']
df.loc[8]=['Assert', '{"BY":"Name", "AT":"Datetime"}', '{"Name":"Assertion","ID":8}', '{"Name":"Data","ID":1}', 'DESIGN', 'DESIGN']

df['EdgeAttr']=df.EdgeAttr.apply(json.loads)
df['FNode']=df.FNode.apply(json.loads)
df['TNode']=df.TNode.apply(json.loads)

print(df)

obj = Neo4j('bolt://localhost:7687')
print(obj)
#obj.AddNodeTest(nodeSpec)
obj.Clean(nodeLabel= ':DESIGN', num=5000)
obj.TestAddEdgeAndNode(TestEdgeNodeSpec)
#obj.TestAddEdgeAndNodeFromDF(df, num=3)
