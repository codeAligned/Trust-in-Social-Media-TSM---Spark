from pyspark import SparkContext,SparkConf,SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os
import time
import networkx as nx
import re
import math

start_time = time.time()

sc = SparkContext()
sqlContext = SQLContext(sc)
sqlContext.setConf('spark.sql.shuffle.partitions', '50')

inv = 1
iter_count = 10
delimiter = "\t"
normChoice = 1

# ````Read graph as RDD````

# edges= sc.textFile(os.path.dirname(os.path.abspath(__file__)) + "/network2.txt")
# v1 = edges.map(lambda e : e.split(delimiter)[0])
# v2 = edges.map(lambda e : e.split(delimiter)[1])
# vertices = v1.union(v2).distinct()

# print(type(edges))

# out_edges_map = []

# def getOutLinks(v):
# 	out_links = v1.filter(lambda e : e.split(delimiter)[0] == v)
# 	print("nainy")
# 	print(out_links)

# vertices.foreach(lambda v : getOutLinks(v))

# outlinks_rdd = sc.parallelize(out_links_map)

# ````Read graph as DiGraph````
G = nx.DiGraph()

#Step 1: Create a graph using given file where each line represents an edge
with open('network2.txt') as infile:
    #print('infile')
    for line in infile:
        #print (line)
        l_spl = re.split('\t', line.rstrip())
        if len(l_spl) == 3:
            # G.add_weighted_edges_from(l_spl[0], l_spl[1], float(l_spl[2]))
            G.add_edge(int(l_spl[0]), int(l_spl[1]), weight = float(l_spl[2]))
        elif len(l_spl) == 2:
            # G.add_weighted_edges_from(l_spl[0], l_spl[1], 1)
            G.add_edge(int(l_spl[0]), int(l_spl[1]), weight=1)

#Step 2: Get number of vertices and intialize the score for each node
vertices = G.nodes()

#trustingness score
hti = {}
#trustworthiness score
htw = {}

initial_score = 1 /float(len(vertices))

# print(initial_score)

outlinks_map = []
inlinks_map = []

for v in vertices:
	outlinks_map.append((v, [ind[1] for ind in G.out_edges(v)]))
	inlinks_map.append((v, [ind[0] for ind in G.in_edges(v)]))
	hti[v] = initial_score
	htw[v] = initial_score

# print(outlinks_map)


outlinks_rdd = sc.parallelize(outlinks_map)
inlinks_rdd = sc.parallelize(inlinks_map)

hti_score = sc.broadcast(hti)
htw_score = sc.broadcast(htw)

# print(hti_score.value)
# print(htw_score.value)

def inver(a):
	return 1/float(1+a**inv)

def update_hti_score(x):
	node = x[0]
	outlinks = x[1]
	#add weight !!!
	# print(len(x))
	new_score = hti_score.value[node]
	
	# Get values from broadcast variable
	htw_lookup = htw_score.value
	for i in outlinks:
		try:
			new_score += inver(htw_lookup[i])
		except KeyError:
			pass
	return (node, outlinks, new_score)

def update_htw_score(x):
	node = x[0]
	inlinks = x[1]
	#add weight !!!
	new_score = htw_score.value[node]
	# Get values from broadcast variable
	hti_lookup = hti_score.value
	for i in inlinks:
		try:
			new_score += inver(hti_lookup[i])
		except KeyError:
			pass
	return (node, inlinks, new_score)

it = 0

while(it < iter_count):
	# print(hti_score.value)
	outlinks_rdd = outlinks_rdd.map(lambda x: update_hti_score(x))
	inlinks_rdd = inlinks_rdd.map(lambda x: update_htw_score(x))

	# hti
	hti_score_collect = outlinks_rdd.map(lambda x: [x[0], x[2]]).collect()
	temp_hti = hti_score.value
	for i in hti_score_collect:
		try:
			temp_hti[i[0]] = i[1]
		except KeyError:
			pass
	hti_score.unpersist()
	del hti_score
	hti_score = sc.broadcast(temp_hti)

	# htw
	htw_score_collect = inlinks_rdd.map(lambda x: [x[0], x[2]]).collect()
	temp_htw= htw_score.value
	for i in htw_score_collect:
		try:
			temp_htw[i[0]] = i[1]
		except KeyError:
			pass
	htw_score.unpersist()
	del htw_score
	htw_score = sc.broadcast(temp_htw)

	it = it+1

print(hti_score.value)
# print('----')
print(htw_score.value)

print("--- %s seconds ---" % (time.time() - start_time))
