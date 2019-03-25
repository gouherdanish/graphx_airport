import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import collection.mutable.HashMap

object airport {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Social Network Analysis")
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    val vertices = ArrayBuffer[(Long, (String, String, String))]()
    val vertice_map = new HashMap[String,Long]()  { override def default(key:String) = 0 }
    val edges = ArrayBuffer[Edge[String]]()

    // Reading airports.csv file
    val airport_data = scala.io.Source.fromFile("./airports.csv")

    // Checking if airport code contains \N or ""
    // Creating HashMap or Dictionary of airport code and coounter (BLR -> 1)
    // Creating vertex array (1L, (BLR, Bangalore, India))
    var counter = 1
    for (line <- airport_data.getLines()) {
      val cols = line.split(",")
      if (cols(4) != "\\N" && cols(4) != "") {
        vertice_map += (cols(4) -> counter)
        vertices += ((counter, (cols(4), cols(1), cols(3))))
        counter += 1
      }
    }

    // Printing 5 Airports
    println("Printing 5 Airports")
    for (line <- vertices.take(5)) {
      println(line)
    }

    // Creating Vertex RDD
    val plane_vertexRDD: RDD[(Long, (String, String, String))] = sc.parallelize(vertices)

    // Reading routes.csv file
    val route_data = scala.io.Source.fromFile("./routes.csv")

    // Checking if airport src and dest codes are present in HashMap - if any one out of src and dst is present ("or" condition), then make edges
    var count = 1
    for (line <- route_data.getLines()) {
      val cols = line.split(",")
      if (vertice_map(cols(2)) != 0 || vertice_map(cols(4)) != 0) {
        edges += (Edge(vertice_map(cols(2)), vertice_map(cols(4)), cols(0)))
        count += 1
      }
    }

    // Printing 5 elements in HashMap
    println('\n'+"Printing 5 HashMap Elements")
    vertice_map.take(5).foreach(println)

    // Printing 5 Routes
    println('\n'+"Printing 5 Routes")
    for (line <- edges.take(5)) {
      println(line)
    }

    // Creating Edge RDD
    val plane_edgeRDD: RDD[Edge[String]] = sc.parallelize(edges)

    // Defining Default Vertex
    val default_plane_vertex = ("Location", "Currently", "Unknown")

    // Creating Graph
    val plane_graph = Graph(plane_vertexRDD, plane_edgeRDD, default_plane_vertex)

    // Graph View
    println('\n'+"Graph View - One Liner")
    plane_graph.vertices.collect.take(5).foreach(println)
    plane_graph.edges.collect.take(5).foreach(println)
    plane_graph.triplets.collect.take(5).foreach(triplet=> println(triplet.srcAttr._1 +" -> "+triplet.dstAttr._1))

    println('\n'+"Graph View - For Loop")
    for (triplet <- plane_graph.triplets.collect.take(5)) {
      print(triplet.srcAttr._1)
      print(" to ")
      print(triplet.dstAttr._1)
      print(" with Airline ")
      println(triplet.attr)
    }

    // Removing Default Vertex
    val plane_graph_fixed = plane_graph.subgraph(vpred = (id,prop)=> prop._1!="Location")

    println('\n'+"Graph View 2 - Removing Missing Airport")
    plane_graph_fixed.vertices.collect.take(5).foreach(println)
    plane_graph_fixed.edges.collect.take(5).foreach(println)
    plane_graph_fixed.triplets.collect.take(5).foreach(triplet=> println(triplet.srcAttr._1 +" -> "+triplet.dstAttr._1))

    // Finding Busiest Airport
    val busy_airport = plane_graph_fixed.degrees
      .toDF("Airport","Degree")
      .join(plane_graph_fixed.vertices.toDF("Airport","Airport Name"),"Airport")
      .orderBy(desc("Degree"))
    busy_airport.show(5,false)

    val busy_airport_1 = plane_graph_fixed
      .outerJoinVertices(plane_graph_fixed.degrees)((vid,x,deg)=>(x,deg.getOrElse(0)))
      .vertices.top(5)(Ordering.by(_._2))
    busy_airport_1.foreach(println)
  }
  /*def get_airport_name_udf = (id:Long,vertice_map:HashMap[String,Long])=>{
    vertice_map(id)
  }
  val get_airport_name_udf = udf()*/

}
