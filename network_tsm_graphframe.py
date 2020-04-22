from pyspark import SparkContext,SparkConf,SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from graphframes import *

#initalize SparkContext, SparkSession and SQLContext
sc = SparkContext()
spark = SparkSession(sc)
sqlContext = SQLContext(sc)

inv = 1
iter_count = 10
delimiter = "\t"
normChoice = 1

#read file
edges= spark.read.format("csv").option("delimiter","\t").load("test_network.txt").withColumnRenamed("_c0","src").withColumnRenamed("_c1","dst")
#edges = edges_rdd.map(lambda x: (x, )).toDF().withColumnRenamed("_1","dst")
#edges.show()

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
g.vertices.show()
g.edges.show()


#Step 2: Get number of vertices and intialize the score for each node

#trustingness score
hti = {}
#trustworthiness score
htw = {}

intial_score = 1 /float(num_vertices)

v = vertices.select("id").collect()
for node in v:
	hti[node.id] = intial_score
	htw[node.id] = intial_score

print(type(hti))
print(hti)

#Step 3: Calculate scores
i = 0
while(i < iter_count):
 for node in vertices:
         #"""Calculate Scores for Trustingness"""
         # vsti = g.neighbors(node, mode=OUT)
         vsti = [ind[1] for ind in G.out_edges(node)]
         sc = calcScores(vsti, hti.get(node), node, htw, 'ti')
         hti[node] = sc

 for node in vertices:
         #"""Calculate Scores for Trustworthiness"""
         # vstw = g.neighbors(node, mode=IN)
         vstw = [ind[0] for ind in G.in_edges(node)]
         sc = calcScores(vstw, htw.get(node), node, hti, 'tw')
         htw[node] = sc

 i += 1