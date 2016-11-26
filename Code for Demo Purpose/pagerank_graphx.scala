import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession
object pagerank_graphx {
  def main(args: Array[String]): Unit = {
  
    
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext

    val t0 = System.nanoTime()
    val graph = GraphLoader.edgeListFile(sc, "/Users/sank/Desktop/CloudComputing/presentation/pagerank/edges_graphx")
    // Run PageRank
    val ranks = graph.pageRank(0.0001).vertices
    // Join the ranks with the usernames
    val users = sc.textFile("/Users/sank/Desktop/CloudComputing/presentation/pagerank/vertices_graphx").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }

    val t1 = System.nanoTime()
    // Print the result
    println(ranksByUsername.collect().mkString("\n"))
    // $example off$
    spark.stop()
    
    println("Elapsed time: " + (t1 - t0)/1000000000 + "s")
  }
}