from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import FloatType, ArrayType, IntegerType
from graphframes import *
import numpy as np
from numpy.linalg import *


top_num = 5000
mem_num = 92000
epsilon = 0.0001

sc = SparkContext()
sqlContext = SQLContext(sc)

sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3a.us-west-2.amazonaws.com")
sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
sc._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId", "AKIAIBR7N2DK6M3WI5XQ")
sc._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", "W7wcnq77f6rY3aBULN/hYmkg0+0KVs3prw7/s42k")

''' s3_vertices_filepath = "s3a://sdurgam/Spark/Sample/SampleUtoC.txt/"
s3_supernodes_filepath = "s3a://sdurgam/Spark/Sample/SampleIsSuperNode.txt/"
s3_edges_filepath = "s3a://sdurgam/Spark/Sample/SampleGraph.txt/" '''

s3_vertices_filepath = "SampleUtoC.txt/"
s3_supernodes_filepath = "SampleIsSuperNode.txt/"
s3_edges_filepath = "SampleGraph.txt/"

def testSaveToS3(local_filepath, dst_filepath):
	v = sc.textFile(local_filepath)
	record = Row('id', 'memberships')
	df1 = v.map(lambda r: record(r.split()[0], ','.join(r.split()[1:])))
	df2 = sqlContext.createDataFrame(df1)
	df2.collect()
	#df2.coalesce(1).write.format("com.databricks.spark.csv").save(dst_filepath)
	df2.coalesce(1).write.json(dst_filepath)
	df2.describe().show()
	df2.show()
	
def top5000Membership(list_memberships):
	vector = [0] * 5000
	for id in list_memberships:
		if (int(id) < 5000):
			vector[int(id)] = 1
	return vector

def membershipVector(list_memberships):
	vector = [0] * 92000
	for id in list_memberships:
		if (int(id) % 97000 >= 5000):
			vector[(int(id) % 97000) - 5000] = 1
	print("In function ", type(vector))
	return vector

def isSuperNode(supernode_rdd,vertex_id):
	match_rdd = supernodes_rdd.filter(lambda row: row.split()[0] == vertex_id).collect()
	if(match_rdd.count != 0):
		return True
	else:
		return False

def similarityMeasure(top5000,top5000_src):
	src_vector = np.array(top5000_src)
	dst_vector = np.array(top5000)
	cosineSimilarity = float(np.dot(src_vector, dst_vector) / (norm(src_vector, ord = 2) * norm(src_vector, ord = 2))) + epsilon
	return cosineSimilarity

def memVectorCalculate(isSuperNode,memVector):
	vector = np.array(memVector)
	newVector = np.dot(int(isSuperNode),memVector)
	return newVector

if __name__ == "__main__":
	Edge = Row('src', 'dst')
	Vertex = Row('id','top5000','memVector')
	SuperNode = Row('id', 'isSuperNode')

	edge_rdd_raw = sc.textFile(s3_edges_filepath)
	vertex_rdd_raw = sc.textFile(s3_vertices_filepath)
	supernodes_rdd_raw = sc.textFile(s3_supernodes_filepath)

    #loading the superNode data
	supernode_rdd = supernodes_rdd_raw.map(lambda r: SuperNode(r.split()[0], r.split()[1]))
	supernode_df = sqlContext.createDataFrame(supernode_rdd)

	#loading the vertex data
	headers = vertex_rdd_raw.take(1)
	vertex_rdd_raw = vertex_rdd_raw.filter(lambda x: x != headers)
	vertex_rdd = vertex_rdd_raw.map(lambda r: Vertex(r.split()[0],top5000Membership(r.split()[1:]),membershipVector(r.split()[1:])))
	vertex_df = sqlContext.createDataFrame(vertex_rdd)
	vertex_df = vertex_df.join(supernode_df, supernode_df.id == vertex_df.id, 'inner').drop(supernode_df.id)
	
	#loading the edge data
	edge_rdd = edge_rdd_raw.map(lambda r: Edge(r.split()[0],r.split()[1]))
	edge_df = sqlContext.createDataFrame(edge_rdd)
	edge_df = edge_df.join(vertex_df, vertex_df.id == edge_df.src, 'left').drop(vertex_df.id).drop(vertex_df.memVector).drop(vertex_df.isSuperNode)
	edge_df.show()
	edge_df = edge_df.withColumnRenamed('top5000', 'top5000_src')
	edge_df = edge_df.join(vertex_df, vertex_df.id == edge_df.dst, 'left').drop(vertex_df.id).drop(vertex_df.memVector).drop(vertex_df.isSuperNode)
	

	#calculating edge weights
	udf = UserDefinedFunction(similarityMeasure, FloatType())
	edge_df = edge_df.withColumn('weights',udf('top5000','top5000_src'))
	edge_df = edge_df.drop('top5000_src').drop('top5000')
	vertex_df = vertex_df.drop('top5000')
	edge_df.show()

	#adding vector to be used for label propagation
	udf1 = UserDefinedFunction(memVectorCalculate, ArrayType(IntegerType()))
	vertex_df = vertex_df.withColumn('newMemVector',udf1('isSuperNode','memVector'))

	#saving the graph
	graph = GraphFrame(vertex_df, edge_df)
	print(graph.vertices.count(),graph.edges.count())
	graph.vertices.write.parquet("code/SampleGraphVer")
	graph.edges.write.parquet("code/SampleGraphEdge")

