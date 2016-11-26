from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
from graphframes import *

sc = SparkContext()
sqlContext = SQLContext(sc)

sc._jsc.hadoopConfiguration().set("fs.s3.endpoint", "s3.us-west-2.amazonaws.com")
sc._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3.S3FileSystem")
sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", "AKIAIBR7N2DK6M3WI5XQ")
sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", "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")

local_vertices_filepath = "/Users/sdurgam/Documents/Academia/Cloud\ computing\ and\ storage/Final\ Project/graphlab/firstUtoC.txt"
s3_vertices_filepath = "s3://sdurgam/Vertices/"


def saveToS3(local_filepath, dst_filepath):
	v = sc.textFile(local_filepath)
	record = Row('id', 'memberships')
	df1 = v.map(lambda r: record(r.split()[0], ','.join(r.split()[1:])))
	df2 = sqlContext.createDataFrame(df1)
	df2.collect()
	#df2.coalesce(1).write.format("com.databricks.spark.csv").save(dst_filepath)
	df2.coalesce(1).write.json(dst_filepath)
	df2.describe().show()
	df2.show()

saveToS3(local_vertices_filepath, s3_vertices_filepath)
