from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
from graphframes import *

sc = SparkContext()
sqlContext = SQLContext(sc)

def loadEdges(filepath):
	rdd = sc.textFile(filepath)
	pattern = Row('src', 'dst')
	rdd = rdd.map(lambda r: pattern(r.split()[0], r.split()[1]))
	df = sqlContext.createDataFrame(rdd)
	df.collect()
	return df

def loadVertices(filepath):
	rdd = sc.textFile(filepath)
	pattern = Row('id', 'communities')
	rdd = rdd.map(lambda r: pattern(r.split()[0], r.split()[1:]))
	df = sqlContext.createDataFrame(rdd)
	df.collect()
	return df

def loadSuperNodeInfo(filepath):
	rdd = sc.textFile(filepath)
	pattern = Row('id', 'isSuperNode')
	rdd = rdd.map(lambda r: pattern(r.split()[0], r.split()[1]))
	df = sqlContext.createDataFrame(rdd)
	df.collect()
	return df

edges_df = loadEdges('code/SampleGraph.txt')
vertices_df = loadVertices('code/SampleUtoC.txt')
isSuperNode_df = loadSuperNodeInfo('code/SampleIsSuperNode.txt')
vertices_df.join(isSuperNode_df, isSuperNode_df.id == vertices_df.id, 'inner').drop(isSuperNode_df.id).collect()

g = GraphFrame(vertices_df, edges_df)
print(g)