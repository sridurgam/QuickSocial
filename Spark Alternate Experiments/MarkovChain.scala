import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.graphframes._
import org.graphframes.lib.AggregateMessages
import org.apache.spark.sql.functions.udf

val sqlContext = new SQLContext(sc)

val edges = spark.read.parquet("code/SampleGraphEdge")
val vertices = spark.read.parquet("code/SampleGraphVer")

val toInteger: String => Int = _.toInt
val toIntegerUDF = udf(toInteger)
val newEdges = edges.withColumn("intWeights", toIntegerUDF('weights)).drop("weights")

val graph = GraphFrexame(vertices, newEdges)

val AM = AggregateMessages

val msgToSrc = AM.dst("MemVector")
val msgToDst = AM.src("MemVector")
val msgFromEdge = AM.edge("floatWeights")

def aggfunc(msg: org.apache.spark.sql.Column) = sum(msg.getField("weights") * AM.msg.getField("memVector"))

val agg = graph.aggregateMessages.sendToSrc(msgToSrc).sendToDst(msgToDst).sendToSrc(sendFromEdge).sendToDst(sendFromEdge).agg(aggfunc(AM.msg).as("UpdatedVector"))