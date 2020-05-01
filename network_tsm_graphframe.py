from pyspark import SparkContext,SparkConf,SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from graphframes import *
from itertools import chain
import math
import os
import time

start_time = time.time()

#initalize SparkContext, SparkSession and SQLContext
sc = SparkContext()
spark = SparkSession(sc)
sqlContext = SQLContext(sc)
sqlContext.setConf('spark.sql.shuffle.partitions', '10')

inv = 1
iter_count = 1
delimiter = "\t"
normChoice = 1

#read file
edges= spark.read.format("csv").option("delimiter","\t").load(os.path.dirname(os.path.abspath(__file__)) + "/test_network.txt").withColumnRenamed("_c0","src").withColumnRenamed("_c1","dst")

if len(edges.columns) == 3:
   edges = edges.withColumnRenamed("_c2","weight")
else:
   edges = edges.withColumn("weight", lit(1))

#Step 1: Create a graph using given file where each line represents an edge

temp1 = edges.select("src").distinct().withColumnRenamed("src","id")
temp2 = edges.select("dst").distinct().withColumnRenamed("dst","id")
vertices = temp1.unionByName(temp2).distinct()
#print(type(vertices))
# temp1.show(temp1.count(), False)

#number of vertices
num_vertices = vertices.count()
print('Number of vertices: ',str(num_vertices))

# Create a GraphFrame
g = GraphFrame(vertices, edges)
# g.vertices.show()
# g.edges.show()

#Step 2: Get number of vertices and intialize the score for each node

#trustingness score
hti = {}
#trustworthiness score
htw = {}

intial_score = 1 /float(num_vertices)
vertices.cache()
v = vertices.select("id").collect()
for node in v:
   hti[node.id] = intial_score
   htw[node.id] = intial_score

# print(type(hti))
# print(hti)

#Step 3: Calculate scores

def calcScores(vs, s, n, other_sc, flag):
   if flag == 'ti':
      for dst, weight in vs:
         # print(src,dst)
         # print(dst)
         s += inver(other_sc.get(dst)) * weight
   elif flag == 'tw':
      for src, weight in vs:
         # print(src,dst)
         s += inver(other_sc.get(src)) * weight
   return s


def inver(a):
 return 1/float(1+a**inv)

i = 0
out_edges_map = {}
in_edges_map = {}

for node in v:
   out_edges = g.edges.filter("src = "+node.id)
   # out_edges_x = g.find("(a) - [e] -> (b)").filter("a.id = " + node.id).select("e")
   # out_edges.show()
   # print("-----------------------")
   # out_edges_x.show()
   # print("-----------------------")
   out_edges_list = []
   out_edges.cache()
   for src, dst, weight in out_edges.collect():
         out_edges_list.append([dst,weight])
   out_edges_map[node.id] = out_edges_list
   in_edges = g.edges.filter("dst = "+node.id)
   in_edges.cache()
   in_edges_list = []
   for src, dst, weight in in_edges.collect():
         in_edges_list.append([src,weight])
   in_edges_map[node.id] = in_edges_list

# print(in_edges_map)
# print(out_edges_map)


while(i < iter_count):
   for node in v:
         #"""Calculate Scores for Trustingness"""
         #vsti = g.neighbors(node, mode=OUT)
         # print('trustingness')
         # out_edges = g.edges.filter("src = "+node.id)
         # out_edges.show(False)
         sc = calcScores(out_edges_map.get(node.id), hti.get(node.id), node.id, htw, 'ti')
         hti[node.id] = sc

   for node in v:
         #"""Calculate Scores for Trustworthiness"""
         # vstw = g.neighbors(node, mode=IN)
         # print('trustworthiness')
        # in_edges = g.edges.filter("dst = "+node.id)
         sc = calcScores(in_edges_map.get(node.id), htw.get(node.id), node.id, hti, 'tw')
         htw[node.id] = sc

   i += 1


#Step 4: Normalize
def normalize(userScores, choice):
   values = userScores.values()
   columns = "Double"
   df = spark.createDataFrame(values, columns)
   # df.show()
   min_val = array_min(df["value"])
   max_val = array_max(df["value"])
   if choice == 0:  # min-max
      for user in userScores:
         userScores[user] = (userScores[user] - min_val)/float(max_val - min_val)

   elif choice == 1: # sum-of-squares
      df = df.withColumn("sq_val", df.value * df.value)
      norm_den = df.select(sum("sq_val").alias("sum")).first().sum
      norm_den = math.sqrt(norm_den)
      for user in userScores:
         # print('in user scores')
         userScores[user] = userScores[user]/float(norm_den)

   return userScores

norm_hti = normalize(hti, normChoice)
norm_htw = normalize(htw, normChoice)

# print(norm_hti)
# print(norm_htw)

print("--- %s seconds ---" % (time.time() - start_time))